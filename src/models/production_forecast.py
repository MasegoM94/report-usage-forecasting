"""Production forecast generation — explicitly separated from backtest evaluation.

The forecasting lifecycle is divided into two phases that must never be mixed:

Phase 1 — Evaluation  (backtest_evaluation → model_summary → selection)
    Candidate models are fitted on rolling training windows.  Error metrics
    are computed against held-out test windows.  One model is selected per
    report.  No model state from this phase carries forward.

Phase 2 — Production  (this module)
    A *fresh* model is fitted on ALL available canonical history.
    ``model.update`` is never called on an evaluation model.
    The production forecast starts on the day after the final observed date.

This separation means:
* Evaluation windows do not become the training set for the production model.
* The production model always uses the maximum available information.
* Lineage is explicit: every production row records which selection run chose
  the model and why.

Public API
----------
refit_and_forecast(model_name, full_series, horizon) -> ModelResult
    Fit a clean, fresh model on *full_series* and return *horizon*-day
    predictions.  All series history is used as the training window.

build_production_forecast(selection, series_dict, run_id, generated_at,
                          horizon, selection_run_id) -> pd.DataFrame
    For every report in *selection* that has a selected model, call
    ``refit_and_forecast`` and assemble the production output DataFrame.
    Reports without a reliable model produce a single NaN-placeholder row
    that preserves failure status.

Output columns
--------------
run_id, generated_at, training_start, training_cutoff, report_id,
forecast_date, horizon_step, selected_model, forecast, lower_bound,
upper_bound, model_order, seasonal_order, selection_reason, selection_run_id
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

import numpy as np
import pandas as pd

from src.config.forecasting import FORECAST_HORIZON_DAYS

if TYPE_CHECKING:
    from src.models.candidates import ModelResult

# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

PRODUCTION_FORECAST_COLS = [
    "run_id",
    "generated_at",
    "training_start",
    "training_cutoff",
    "report_id",
    "forecast_date",
    "horizon_step",
    "selected_model",
    "forecast",
    "lower_bound",
    "upper_bound",
    "model_order",
    "seasonal_order",
    "selection_reason",
    "selection_run_id",
]

# ---------------------------------------------------------------------------
# Model registry — same functions used in evaluation; fresh fit each time
# ---------------------------------------------------------------------------

def _build_model_registry() -> dict[str, Callable]:
    """Import candidate functions lazily to avoid hard dependency on pmdarima at
    import time.  Returns the registry on every call — callers may cache it."""
    from src.models.candidates import (
        forecast_auto_arima,
        forecast_ets,
        forecast_moving_average,
        forecast_naive,
        forecast_seasonal_naive,
    )
    return {
        "naive": forecast_naive,
        "seasonal_naive": forecast_seasonal_naive,
        "moving_average": forecast_moving_average,
        "ets": forecast_ets,
        "auto_arima": forecast_auto_arima,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _extract_order(meta: dict, key: str) -> Optional[str]:
    """Return a metadata order value as a string, or None when absent."""
    val = meta.get(key)
    return str(val) if val is not None else None


def _failure_row(
    run_id: str,
    generated_at: pd.Timestamp,
    report_id: str,
    selected_model: Optional[str],
    selection_reason: str,
    selection_run_id: str,
    training_start: Optional[pd.Timestamp] = None,
    training_cutoff: Optional[pd.Timestamp] = None,
) -> dict:
    """Return a single NaN row preserving failure status for a report."""
    return {
        "run_id": run_id,
        "generated_at": generated_at,
        "training_start": training_start,
        "training_cutoff": training_cutoff,
        "report_id": report_id,
        "forecast_date": pd.NaT,
        "horizon_step": None,
        "selected_model": selected_model,
        "forecast": np.nan,
        "lower_bound": np.nan,
        "upper_bound": np.nan,
        "model_order": None,
        "seasonal_order": None,
        "selection_reason": selection_reason,
        "selection_run_id": selection_run_id,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def refit_and_forecast(
    model_name: str,
    full_series: pd.Series,
    horizon: int = FORECAST_HORIZON_DAYS,
) -> "ModelResult":
    """Fit a fresh production model on *full_series* and return *horizon*-day forecasts.

    This is the production fitting step — every call starts from scratch.
    No evaluation model state is reused.  No ``model.update`` is performed.
    The training window is the entirety of *full_series*.

    Parameters
    ----------
    model_name:
        One of ``"naive"``, ``"seasonal_naive"``, ``"moving_average"``,
        ``"ets"``, ``"auto_arima"``.  Unknown names return a failed
        ``ModelResult`` rather than raising.
    full_series:
        Complete canonical daily series for one report, date-indexed.
        Must include ALL available history — not a train/test split.
    horizon:
        Number of future days to forecast.  Defaults to
        ``FORECAST_HORIZON_DAYS`` (28).

    Returns
    -------
    ModelResult
        ``fit_status`` is ``"ok"`` / ``"converged"`` on success,
        ``"failed"`` when any exception prevented forecasting.
        The ``forecast`` Series index starts on the day after
        ``full_series.index[-1]`` — it never overlaps with observed dates.
    """
    registry = _build_model_registry()
    fn = registry.get(model_name)
    if fn is None:
        # Return a documented failure rather than raising so callers can
        # preserve the failure row in the output rather than crashing.
        from src.models.candidates import _failed
        return _failed(
            model_name,
            ValueError(f"Unknown model name {model_name!r}; not in registry."),
        )
    return fn(full_series, horizon)


def build_production_forecast(
    selection: pd.DataFrame,
    series_dict: dict[str, pd.Series],
    run_id: str,
    generated_at: pd.Timestamp,
    horizon: int = FORECAST_HORIZON_DAYS,
    selection_run_id: Optional[str] = None,
) -> pd.DataFrame:
    """Refit selected models on full history and assemble production forecast rows.

    For each report in *selection*:
    * If ``selection_status != "selected"`` or ``selected_model`` is null,
      one NaN-placeholder row is emitted to preserve failure status in the
      output (requirement 6).
    * Otherwise, ``refit_and_forecast`` is called with the report's full
      canonical series.  The forecast is guaranteed to start on the day
      after the final observed date — evaluation rows are never mixed in
      (requirement 1 / 4).

    Parameters
    ----------
    selection:
        Output of ``select_models`` — one row per report_id.  Required
        columns: ``report_id``, ``selection_status``, ``selected_model``,
        ``selection_reason``.
    series_dict:
        Full canonical daily series per report_id (all available history).
        The fitting step uses every observation present here.
    run_id:
        Unique identifier for this production run, stamped on every row.
    generated_at:
        Wall-clock timestamp when forecasting was initiated.
    horizon:
        Days to forecast ahead.  Every successful report produces exactly
        *horizon* rows.  Defaults to ``FORECAST_HORIZON_DAYS`` (28).
    selection_run_id:
        Run ID from the model-selection step.  Captures selection lineage
        when selection and production forecasting occur in separate runs.
        Defaults to *run_id* when both phases share the same run.

    Returns
    -------
    pd.DataFrame
        One row per (report_id, horizon_step) for successful fits.
        One NaN row per report when no model was selected or fitting failed.
        Columns follow ``PRODUCTION_FORECAST_COLS`` exactly.
        Sorted by (report_id, horizon_step) with NaN steps last.

    Notes
    -----
    ``training_start`` and ``training_cutoff`` are the first and last dates
    of the full_series actually used for fitting — always ``series.index.min()``
    and ``series.index.max()`` respectively (requirement 3 / proof of full
    history use).

    ``model_order`` and ``seasonal_order`` are serialised as strings from
    ``model_metadata`` (e.g. ``"(2, 1, 1)"``) and are ``None`` for models
    without an order concept (naive, seasonal_naive, moving_average).
    """
    if selection_run_id is None:
        selection_run_id = run_id

    if selection.empty:
        return pd.DataFrame(columns=PRODUCTION_FORECAST_COLS)

    rows: list[dict] = []

    for _, sel in selection.iterrows():
        report_id: str = str(sel["report_id"])
        selection_reason: str = str(sel.get("selection_reason", ""))
        selected_model = sel.get("selected_model")
        status = sel.get("selection_status", "no_reliable_model")

        # --- No reliable model — preserve failure status ---
        if status != "selected" or pd.isna(selected_model):
            rows.append(
                _failure_row(
                    run_id=run_id,
                    generated_at=generated_at,
                    report_id=report_id,
                    selected_model=None,
                    selection_reason=selection_reason,
                    selection_run_id=selection_run_id,
                )
            )
            continue

        model_name: str = str(selected_model)
        series = series_dict.get(report_id)

        if series is None or series.empty:
            rows.append(
                _failure_row(
                    run_id=run_id,
                    generated_at=generated_at,
                    report_id=report_id,
                    selected_model=model_name,
                    selection_reason=selection_reason,
                    selection_run_id=selection_run_id,
                )
            )
            continue

        # --- Refit on ALL available history — no split, no model.update ---
        training_start: pd.Timestamp = series.index.min()
        training_cutoff: pd.Timestamp = series.index.max()

        result = refit_and_forecast(model_name, series, horizon)

        if result.fit_status == "failed" or result.forecast is None:
            rows.append(
                _failure_row(
                    run_id=run_id,
                    generated_at=generated_at,
                    report_id=report_id,
                    selected_model=model_name,
                    selection_reason=selection_reason,
                    selection_run_id=selection_run_id,
                    training_start=training_start,
                    training_cutoff=training_cutoff,
                )
            )
            continue

        meta = result.model_metadata or {}
        lo_series = result.lower_bound
        hi_series = result.upper_bound

        for step, date in enumerate(result.forecast.index, start=1):
            lo = float(lo_series.iloc[step - 1]) if lo_series is not None else np.nan
            hi = float(hi_series.iloc[step - 1]) if hi_series is not None else np.nan
            rows.append({
                "run_id": run_id,
                "generated_at": generated_at,
                "training_start": training_start,
                "training_cutoff": training_cutoff,
                "report_id": report_id,
                "forecast_date": date,
                "horizon_step": step,
                "selected_model": model_name,
                "forecast": float(result.forecast.iloc[step - 1]),
                "lower_bound": lo,
                "upper_bound": hi,
                "model_order": _extract_order(meta, "order"),
                "seasonal_order": _extract_order(meta, "seasonal_order"),
                "selection_reason": selection_reason,
                "selection_run_id": selection_run_id,
            })

    if not rows:
        return pd.DataFrame(columns=PRODUCTION_FORECAST_COLS)

    out = pd.DataFrame(rows)[PRODUCTION_FORECAST_COLS]
    return out.sort_values(
        ["report_id", "horizon_step"],
        ascending=[True, True],
        na_position="last",
        ignore_index=True,
    )
