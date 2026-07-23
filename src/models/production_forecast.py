"""Production forecast generation — explicitly separated from backtest evaluation.

The forecasting lifecycle is divided into two phases that must never be mixed:

Phase 1 — Evaluation  (backtest_evaluation → model_summary → selection)
    Candidate models are fitted on rolling training windows.  Error metrics
    are computed against held-out test windows.  One (model_family, candidate_m)
    pair is selected per report.  No model state from this phase carries forward.

Phase 2 — Production  (this module)
    A *fresh* model is fitted on ALL available canonical history using exactly
    the seasonal period chosen in Phase 1.  ``model.update`` is never called on
    an evaluation model.  ``profile_seasonality`` is never called again — the
    period is read directly from the selection output.

Seasonal period preservation
-----------------------------
The central requirement of this module is that ``selected_m`` from the
cross-fold model selection is passed *explicitly* to the candidate fitting
function during production refitting:

* SARIMA (``auto_arima`` family, m > 1)  →  ``forecast_auto_arima(..., seasonal_period=m)``
* Non-seasonal ARIMA (``auto_arima`` family, m = 1)  →  ``seasonal_period=1`` (no seasonal terms)
* Seasonal ETS (``ets`` family, m > 1)  →  ``forecast_ets(..., seasonal_period=m)``
* Seasonal naive  →  ``forecast_seasonal_naive(..., seasonal_period=m)``
* Naive / moving average  →  no period parameter (always non-seasonal)

The default ``seasonal_period`` in every candidate function is deliberately
*not* used here — relying on a default would silently override the selection.

Fallback policy
---------------
If the selected model fails to fit on the full series, ``build_production_forecast``
attempts the next eligible candidate from the ranked ``candidate_summary``
(output of ``summarise_candidate_performance``).  Selection lineage is always
preserved: ``selected_model_*`` columns reflect the original selection; the
``fallback_*`` columns record what was actually used when fallback occurred.

Public API
----------
refit_and_forecast(model_name, full_series, horizon, seasonal_period) -> ModelResult
    Fit a fresh model named *model_name* on *full_series*.  When
    *seasonal_period* is provided it overrides any default embedded in the
    model function.  M-encoded names (``"auto_arima_m30"``) are also
    accepted — the period is parsed from the suffix.

build_production_forecast(selection, series_dict, run_id, generated_at,
                          horizon, selection_run_id, candidate_summary) -> pd.DataFrame
    For every report in *selection*, refit the selected model on the full
    series and return production forecast rows.  Attempts fallback candidates
    from *candidate_summary* when the primary fit fails.

Output columns (``PRODUCTION_FORECAST_COLS``)
---------------------------------------------
run_id, selection_run_id, generated_at, report_id,
training_start, training_cutoff, forecast_date, horizon_step,
selected_model_family, selected_model_name, selected_m,
forecast, lower_bound, upper_bound, model_order, seasonal_order,
selection_reason, valid_backtest_folds, median_backtest_mase,
mean_backtest_wape, mean_backtest_bias,
production_fit_status, fallback_used, fallback_reason
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

import numpy as np
import pandas as pd

from src.config.forecasting import FORECAST_HORIZON_DAYS, NON_SEASONAL_PERIOD

if TYPE_CHECKING:
    from src.models.candidates import ModelResult

# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

PRODUCTION_FORECAST_COLS = [
    "run_id",
    "selection_run_id",
    "generated_at",
    "report_id",
    "training_start",
    "training_cutoff",
    "forecast_date",
    "horizon_step",
    "selected_model_family",
    "selected_model_name",
    "selected_m",
    "forecast",
    "lower_bound",
    "upper_bound",
    "model_order",
    "seasonal_order",
    "selection_reason",
    "valid_backtest_folds",
    "median_backtest_mase",
    "mean_backtest_wape",
    "mean_backtest_bias",
    "production_fit_status",
    "fallback_used",
    "fallback_reason",
]

# ---------------------------------------------------------------------------
# Model registry — same functions used in evaluation; fresh fit each time
# ---------------------------------------------------------------------------

def _build_model_registry() -> dict[str, Callable]:
    """Import candidate functions lazily to avoid hard dependency on pmdarima at
    import time.  Returns a fresh dict on every call."""
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
# Period-preserving dispatch
# ---------------------------------------------------------------------------

def _refit_with_period(
    model_family: str,
    selected_m: int,
    full_series: "pd.Series",
    horizon: int,
) -> "ModelResult":
    """Fit *model_family* with exactly *selected_m* as the seasonal period.

    This is the core production-refitting function.  It never calls
    ``profile_seasonality`` and never falls back to a global default period.

    Dispatch rules
    --------------
    * ``naive``          — no period parameter; always non-seasonal.
    * ``moving_average`` — no period parameter; the window is independent of
                           the seasonal period (always non-seasonal).
    * ``seasonal_naive`` — ``seasonal_period=selected_m`` passed explicitly.
    * ``ets``            — ``seasonal_period=selected_m`` (1 → non-seasonal ETS).
    * ``auto_arima``     — ``seasonal_period=selected_m`` (1 → non-seasonal ARIMA,
                           > 1 → SARIMA with that exact period).

    Parameters
    ----------
    model_family:
        One of the keys in the model registry (``"naive"``, ``"ets"``, etc.).
    selected_m:
        The seasonal period selected during cross-fold evaluation.  Passed
        directly to the candidate function — never substituted.
    full_series:
        Complete canonical daily series; all observations are used as training.
    horizon:
        Number of forecast days.

    Returns
    -------
    ModelResult
        ``fit_status == "failed"`` when the family is unknown or fitting raises.
    """
    registry = _build_model_registry()
    fn = registry.get(model_family)

    if fn is None:
        from src.models.candidates import _failed
        label = f"{model_family}_m{selected_m}"
        return _failed(label, ValueError(
            f"Unknown model family {model_family!r}; not in registry."
        ))

    # Non-seasonal baselines — seasonal period is not a meaningful parameter
    if model_family in ("naive", "moving_average"):
        return fn(full_series, horizon)

    # All seasonal-aware models receive the period explicitly
    return fn(full_series, horizon, seasonal_period=selected_m)


def _parse_m_from_name(model_name: str, registry_keys: set[str]) -> tuple[str, Optional[int]]:
    """Parse a potentially m-encoded model name into (family, m).

    Examples
    --------
    ``"auto_arima_m7"``  → ``("auto_arima", 7)``
    ``"seasonal_naive"`` → ``("seasonal_naive", None)``
    ``"naive"``          → ``("naive", None)``
    """
    if model_name in registry_keys:
        return model_name, None
    for family in sorted(registry_keys, key=len, reverse=True):
        prefix = f"{family}_m"
        if model_name.startswith(prefix):
            suffix = model_name[len(prefix):]
            try:
                return family, int(suffix)
            except ValueError:
                pass
    return model_name, None


# ---------------------------------------------------------------------------
# Internal row builders
# ---------------------------------------------------------------------------

def _extract_order(meta: dict, key: str) -> Optional[str]:
    val = meta.get(key)
    return str(val) if val is not None else None


def _failure_row(
    run_id: str,
    selection_run_id: str,
    generated_at: "pd.Timestamp",
    report_id: str,
    selected_model_family: Optional[str],
    selected_model_name: Optional[str],
    selected_m: Optional[int],
    selection_reason: str,
    valid_backtest_folds,
    median_backtest_mase,
    mean_backtest_wape,
    mean_backtest_bias,
    training_start: Optional["pd.Timestamp"] = None,
    training_cutoff: Optional["pd.Timestamp"] = None,
    production_fit_status: str = "failed",
    fallback_used: bool = False,
    fallback_reason: Optional[str] = None,
) -> dict:
    return {
        "run_id": run_id,
        "selection_run_id": selection_run_id,
        "generated_at": generated_at,
        "report_id": report_id,
        "training_start": training_start,
        "training_cutoff": training_cutoff,
        "forecast_date": pd.NaT,
        "horizon_step": None,
        "selected_model_family": selected_model_family,
        "selected_model_name": selected_model_name,
        "selected_m": selected_m,
        "forecast": np.nan,
        "lower_bound": np.nan,
        "upper_bound": np.nan,
        "model_order": None,
        "seasonal_order": None,
        "selection_reason": selection_reason,
        "valid_backtest_folds": valid_backtest_folds,
        "median_backtest_mase": median_backtest_mase,
        "mean_backtest_wape": mean_backtest_wape,
        "mean_backtest_bias": mean_backtest_bias,
        "production_fit_status": production_fit_status,
        "fallback_used": fallback_used,
        "fallback_reason": fallback_reason,
    }


def _forecast_rows(
    result: "ModelResult",
    horizon: int,
    run_id: str,
    selection_run_id: str,
    generated_at: "pd.Timestamp",
    report_id: str,
    training_start: "pd.Timestamp",
    training_cutoff: "pd.Timestamp",
    selected_model_family: Optional[str],
    selected_model_name: Optional[str],
    selected_m: Optional[int],
    selection_reason: str,
    valid_backtest_folds,
    median_backtest_mase,
    mean_backtest_wape,
    mean_backtest_bias,
    production_fit_status: str,
    fallback_used: bool,
    fallback_reason: Optional[str],
) -> list[dict]:
    meta = result.model_metadata or {}
    lo_series = result.lower_bound
    hi_series = result.upper_bound
    rows: list[dict] = []
    for step, date in enumerate(result.forecast.index, start=1):
        lo = float(lo_series.iloc[step - 1]) if lo_series is not None else np.nan
        hi = float(hi_series.iloc[step - 1]) if hi_series is not None else np.nan
        rows.append({
            "run_id": run_id,
            "selection_run_id": selection_run_id,
            "generated_at": generated_at,
            "report_id": report_id,
            "training_start": training_start,
            "training_cutoff": training_cutoff,
            "forecast_date": date,
            "horizon_step": step,
            "selected_model_family": selected_model_family,
            "selected_model_name": selected_model_name,
            "selected_m": selected_m,
            "forecast": float(result.forecast.iloc[step - 1]),
            "lower_bound": lo,
            "upper_bound": hi,
            "model_order": _extract_order(meta, "order"),
            "seasonal_order": _extract_order(meta, "seasonal_order"),
            "selection_reason": selection_reason,
            "valid_backtest_folds": valid_backtest_folds,
            "median_backtest_mase": median_backtest_mase,
            "mean_backtest_wape": mean_backtest_wape,
            "mean_backtest_bias": mean_backtest_bias,
            "production_fit_status": production_fit_status,
            "fallback_used": fallback_used,
            "fallback_reason": fallback_reason,
        })
    return rows


# ---------------------------------------------------------------------------
# Fallback candidate ranking
# ---------------------------------------------------------------------------

def _fallback_candidates(
    report_id: str,
    exclude_family: str,
    exclude_m: int,
    candidate_summary: "pd.DataFrame",
    max_bias_ratio: float,
) -> "pd.DataFrame":
    """Return eligible fallback candidates for *report_id*, sorted by median MASE.

    Excludes the originally selected (family, m) pair and applies the same
    guardrails used during selection (sufficient folds, bias ratio).
    """
    report_df = candidate_summary[candidate_summary["report_id"] == report_id].copy()

    # Exclude the primary that just failed
    is_primary = (
        (report_df["model_family"] == exclude_family) &
        (report_df["candidate_m"] == exclude_m)
    )
    report_df = report_df[~is_primary]

    # Require sufficient folds
    if "has_sufficient_folds" in report_df.columns:
        report_df = report_df[report_df["has_sufficient_folds"]]

    # Bias guardrail
    if (
        max_bias_ratio is not None
        and "absolute_mean_bias" in report_df.columns
        and "mean_mae" in report_df.columns
    ):
        with np.errstate(divide="ignore", invalid="ignore"):
            ratio = report_df["absolute_mean_bias"] / report_df["mean_mae"]
        report_df = report_df[ratio.fillna(0.0) <= max_bias_ratio]

    # Require a valid median MASE for ranking
    if "median_mase" in report_df.columns:
        report_df = report_df.dropna(subset=["median_mase"]).sort_values("median_mase")

    return report_df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def refit_and_forecast(
    model_name: str,
    full_series: "pd.Series",
    horizon: int = FORECAST_HORIZON_DAYS,
    seasonal_period: Optional[int] = None,
) -> "ModelResult":
    """Fit a fresh production model on *full_series* and return *horizon*-day forecasts.

    This is the production fitting step — every call starts from scratch.
    No evaluation model state is reused.  No ``model.update`` is performed.
    The training window is the entirety of *full_series*.

    Parameters
    ----------
    model_name:
        Bare family name (``"naive"``, ``"auto_arima"``, …) or m-encoded name
        (``"auto_arima_m7"``, ``"seasonal_naive_m28"``, …).  When the name
        encodes a period, that period is used for refitting unless overridden
        by *seasonal_period*.
    full_series:
        Complete canonical daily series.  Must include ALL available history.
    horizon:
        Days to forecast.  Defaults to ``FORECAST_HORIZON_DAYS`` (28).
    seasonal_period:
        When provided, used as the seasonal period regardless of any period
        encoded in *model_name*.  Pass ``NON_SEASONAL_PERIOD`` (= 1) to
        explicitly request a non-seasonal fit.

    Returns
    -------
    ModelResult
        ``fit_status == "failed"`` when the model name is unknown or any
        exception prevented forecasting.
    """
    registry = _build_model_registry()

    # Resolve family and effective period
    family, encoded_m = _parse_m_from_name(model_name, set(registry.keys()))
    effective_m = seasonal_period if seasonal_period is not None else encoded_m

    if effective_m is not None:
        return _refit_with_period(family, effective_m, full_series, horizon)

    # Legacy path — bare name, no period: call registry function with its defaults
    fn = registry.get(family)
    if fn is None:
        from src.models.candidates import _failed
        return _failed(
            model_name,
            ValueError(f"Unknown model name {model_name!r}; not in registry."),
        )
    return fn(full_series, horizon)


def build_production_forecast(
    selection: "pd.DataFrame",
    series_dict: dict[str, "pd.Series"],
    run_id: str,
    generated_at: "pd.Timestamp",
    horizon: int = FORECAST_HORIZON_DAYS,
    selection_run_id: Optional[str] = None,
    candidate_summary: Optional["pd.DataFrame"] = None,
    max_bias_ratio: float = 0.5,
) -> "pd.DataFrame":
    """Refit selected models on full history and assemble production forecast rows.

    Uses ``_refit_with_period`` to guarantee that the seasonal period chosen
    during cross-fold evaluation is preserved exactly during production refitting.
    ``profile_seasonality`` is never called from this function.

    Parameters
    ----------
    selection:
        Output of ``select_candidate_models`` — one row per report_id.
        Required columns: ``report_id``, ``selection_status``,
        ``selected_model_family``, ``selected_model_name``, ``selected_m``,
        ``selection_reason``.
        Optional backtest-metric columns carried into the output:
        ``valid_folds``, ``median_mase``, ``mean_wape``, ``mean_bias``.
    series_dict:
        Full canonical daily series per report_id (all available history).
        Every observation present is used as the production training window.
    run_id:
        Unique identifier for this production run, stamped on every row.
    generated_at:
        Wall-clock timestamp when forecasting was initiated.
    horizon:
        Days to forecast ahead.  Every successful report produces exactly
        *horizon* rows.  Defaults to ``FORECAST_HORIZON_DAYS`` (28).
    selection_run_id:
        Run ID from the model-selection step.  Defaults to *run_id*.
    candidate_summary:
        Optional output of ``summarise_candidate_performance``.  When
        provided, used to rank fallback candidates when the primary model
        fails to fit on the full series.
    max_bias_ratio:
        Bias guardrail applied to fallback candidates:
        ``|mean_bias| / mean_mae ≤ max_bias_ratio``.

    Returns
    -------
    pd.DataFrame
        One row per (report_id, horizon_step) for successful fits.
        One placeholder row per report when no model was selected or all
        fits failed.  Columns follow ``PRODUCTION_FORECAST_COLS`` exactly.
        Sorted by (report_id, horizon_step) with NaN steps last.

    Notes
    -----
    ``selected_model_family``, ``selected_model_name``, and ``selected_m``
    in the output always reflect the **original selection** — they are never
    overwritten by a fallback model.  When fallback is used, ``fallback_used``
    is ``True`` and ``fallback_reason`` records the originally selected model,
    its m value, the error that caused the failure, and the fallback model
    actually used to generate the forecast.

    ``training_start`` and ``training_cutoff`` are the first and last dates
    of the full_series used for fitting — proof of full-history use.
    """
    if selection_run_id is None:
        selection_run_id = run_id

    if selection.empty:
        return pd.DataFrame(columns=PRODUCTION_FORECAST_COLS)

    rows: list[dict] = []

    for _, sel in selection.iterrows():
        report_id: str = str(sel["report_id"])
        selection_reason: str = str(sel.get("selection_reason", ""))
        status = sel.get("selection_status", "no_reliable_model")

        # Original selection — may be None for no_reliable_model reports
        selected_family = sel.get("selected_model_family")
        selected_name = sel.get("selected_model_name")
        _raw_m = sel.get("selected_m")
        selected_m: Optional[int] = None if pd.isna(_raw_m) else int(_raw_m)  # type: ignore[arg-type]

        # Backtest metrics propagated into the output schema
        valid_backtest_folds = sel.get("valid_folds", np.nan)
        median_backtest_mase = sel.get("median_mase", np.nan)
        mean_backtest_wape = sel.get("mean_wape", np.nan)
        mean_backtest_bias = sel.get("mean_bias", np.nan)

        # Shared kwargs for helper functions
        _common = dict(
            run_id=run_id,
            selection_run_id=selection_run_id,
            generated_at=generated_at,
            report_id=report_id,
            selected_model_family=selected_family,
            selected_model_name=selected_name,
            selected_m=selected_m,
            selection_reason=selection_reason,
            valid_backtest_folds=valid_backtest_folds,
            median_backtest_mase=median_backtest_mase,
            mean_backtest_wape=mean_backtest_wape,
            mean_backtest_bias=mean_backtest_bias,
        )

        # --- No reliable model: emit placeholder ---
        if status != "selected" or selected_family is None or pd.isna(selected_family):
            rows.append(_failure_row(**_common))
            continue

        series = series_dict.get(report_id)
        if series is None or series.empty:
            rows.append(_failure_row(**_common))
            continue

        training_start: pd.Timestamp = series.index.min()
        training_cutoff: pd.Timestamp = series.index.max()
        _with_training = dict(_common, training_start=training_start, training_cutoff=training_cutoff)

        # --- Primary fit: refit with exactly selected_m ---
        result = _refit_with_period(str(selected_family), int(selected_m), series, horizon)

        if result.fit_status != "failed" and result.forecast is not None:
            rows.extend(_forecast_rows(
                result=result,
                horizon=horizon,
                production_fit_status=result.fit_status,
                fallback_used=False,
                fallback_reason=None,
                **_with_training,
            ))
            continue

        # --- Primary failed: attempt fallback from candidate_summary ---
        primary_error = result.error_message or "unknown error"

        if candidate_summary is not None and not candidate_summary.empty:
            fallback_df = _fallback_candidates(
                report_id,
                str(selected_family),
                int(selected_m),
                candidate_summary,
                max_bias_ratio,
            )
            for _, fb in fallback_df.iterrows():
                fb_family: str = str(fb["model_family"])
                fb_m: int = int(fb["candidate_m"])
                fb_name: str = str(fb.get("model_name", f"{fb_family}_m{fb_m}"))

                fb_result = _refit_with_period(fb_family, fb_m, series, horizon)
                if fb_result.fit_status != "failed" and fb_result.forecast is not None:
                    fb_reason = (
                        f"Originally selected {selected_name} (m={selected_m}) failed: "
                        f"{primary_error}. "
                        f"Fell back to {fb_name} (m={fb_m})."
                    )
                    rows.extend(_forecast_rows(
                        result=fb_result,
                        horizon=horizon,
                        production_fit_status=fb_result.fit_status,
                        fallback_used=True,
                        fallback_reason=fb_reason,
                        **_with_training,
                    ))
                    break
            else:
                # Every fallback candidate also failed
                rows.append(_failure_row(
                    **_with_training,
                    production_fit_status="failed",
                    fallback_used=True,
                    fallback_reason=(
                        f"Originally selected {selected_name} (m={selected_m}) failed: "
                        f"{primary_error}. All fallback candidates also failed."
                    ),
                ))
        else:
            # No candidate_summary: no fallback available
            rows.append(_failure_row(
                **_with_training,
                production_fit_status="failed",
                fallback_used=False,
                fallback_reason=None,
            ))

    if not rows:
        return pd.DataFrame(columns=PRODUCTION_FORECAST_COLS)

    out = pd.DataFrame(rows)[PRODUCTION_FORECAST_COLS]
    return out.sort_values(
        ["report_id", "horizon_step"],
        ascending=[True, True],
        na_position="last",
        ignore_index=True,
    )
