"""Fold-level model evaluation using rolling-origin backtesting.

Public API
----------
BacktestConfig
    Dataclass holding the parameters that control split generation and metric
    computation.  Defaults wire to ``src/config/forecasting.py`` so callers
    that accept portfolio defaults need zero configuration.

evaluate_models_across_folds(report_id, series, model_registry, backtest_config)
    Evaluate every model in *model_registry* on every fold produced by
    ``generate_rolling_splits`` and return two tidy DataFrames:

    * **predictions** — one row per (fold, model, forecast_date)
    * **fold_metrics** — one row per (fold, model)

Design rules
------------
* Every model is run on the same folds.
* MASE uses only the fold's own training series as its denominator baseline.
* Failed model-folds are preserved as explicit rows (never silently dropped).
* A failure in one (model, fold) pair does not skip any other pair.
* Forecast and actual indices are verified to align exactly; a misalignment
  is recorded as ``fit_status='misaligned'`` rather than silently dropping rows.
* Outputs are sorted deterministically before returning.
* No model selection is performed here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional

import numpy as np
import pandas as pd

from src.config.forecasting import (
    BACKTEST_FOLDS,
    BACKTEST_STEP_DAYS,
    FORECAST_HORIZON_DAYS,
    MIN_TRAIN_DAYS,
    SEASONAL_PERIOD,
)
from src.models.backtesting import ForecastFold, generate_rolling_splits
from src.models.candidates import ModelResult
from src.models.metrics import calculate_interval_metrics, calculate_point_metrics


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class BacktestConfig:
    """Parameters controlling fold generation and metric computation.

    Attributes
    ----------
    horizon:
        Number of test observations per fold.  Matches ``FORECAST_HORIZON_DAYS``
        by default (28 days).
    n_folds:
        Requested number of rolling-origin folds.  Actual folds produced may
        be fewer if the series is too short.
    step:
        Days between successive cutoff dates (stride of the rolling window).
    min_train_size:
        Minimum training-window size required for the first fold.
    seasonal_period:
        Lag used for the MASE denominator.  Must match the seasonality
        assumption used when training the candidate models.
    """
    horizon: int = FORECAST_HORIZON_DAYS
    n_folds: int = BACKTEST_FOLDS
    step: int = BACKTEST_STEP_DAYS
    min_train_size: int = MIN_TRAIN_DAYS
    seasonal_period: int = SEASONAL_PERIOD


# ---------------------------------------------------------------------------
# Column name constants
# ---------------------------------------------------------------------------

# Prediction-level output columns (in output order)
_PRED_COLS = [
    "report_id", "fold_number", "cutoff_date",
    "train_start", "train_end",
    "forecast_date", "horizon_step",
    "model_name", "actual", "forecast", "lower_bound", "upper_bound",
    "fit_status",
]

# Fold-level metric output columns (in output order)
_METRIC_COLS = [
    "report_id", "fold_number", "cutoff_date",
    "model_name",
    "mae", "rmse", "wape", "mase", "bias",
    "interval_coverage", "mean_interval_width",
    "fit_status", "error_message",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_failed_result(model_name: str, exc: Exception) -> ModelResult:
    """Wrap an unexpected exception from a non-candidates model function."""
    return ModelResult(
        model_name=model_name,
        forecast=None,
        lower_bound=None,
        upper_bound=None,
        forecast_raw=None,
        lower_bound_raw=None,
        upper_bound_raw=None,
        model_metadata={},
        fit_status="failed",
        error_message=f"{type(exc).__name__}: {exc}",
    )


def _build_prediction_rows(
    report_id: str,
    fold: ForecastFold,
    result: ModelResult,
) -> list[dict]:
    """Return one prediction-level dict per horizon step.

    For successful models the forecast and actual values are date-aligned.
    For failed models, actuals are taken directly from the test fold and
    forecast/bound fields are NaN.
    """
    test = fold.test_series
    horizon = len(test)
    horizon_steps = list(range(1, horizon + 1))
    base = {
        "report_id": report_id,
        "fold_number": fold.fold_number,
        "cutoff_date": fold.cutoff_date,
        "train_start": fold.train_start,
        "train_end": fold.train_end,
    }

    if result.fit_status == "failed":
        return [
            {
                **base,
                "forecast_date": date,
                "horizon_step": step,
                "model_name": result.model_name,
                "actual": float(test.iloc[i]),
                "forecast": np.nan,
                "lower_bound": np.nan,
                "upper_bound": np.nan,
                "fit_status": "failed",
            }
            for i, (date, step) in enumerate(zip(test.index, horizon_steps))
        ]

    # Verify date alignment between forecast and test series
    if not result.forecast.index.equals(test.index):
        # Attempt reindex to test dates; missing positions become NaN
        fc_aligned = result.forecast.reindex(test.index)
        lo_aligned = result.lower_bound.reindex(test.index) if result.lower_bound is not None else None
        hi_aligned = result.upper_bound.reindex(test.index) if result.upper_bound is not None else None
        fit_status = "misaligned"
    else:
        fc_aligned = result.forecast
        lo_aligned = result.lower_bound
        hi_aligned = result.upper_bound
        fit_status = result.fit_status

    rows = []
    for i, (date, step) in enumerate(zip(test.index, horizon_steps)):
        rows.append({
            **base,
            "forecast_date": date,
            "horizon_step": step,
            "model_name": result.model_name,
            "actual": float(test.iloc[i]),
            "forecast": float(fc_aligned.iloc[i]) if pd.notna(fc_aligned.iloc[i]) else np.nan,
            "lower_bound": float(lo_aligned.iloc[i]) if lo_aligned is not None and pd.notna(lo_aligned.iloc[i]) else np.nan,
            "upper_bound": float(hi_aligned.iloc[i]) if hi_aligned is not None and pd.notna(hi_aligned.iloc[i]) else np.nan,
            "fit_status": fit_status,
        })
    return rows


def _build_metric_row(
    report_id: str,
    fold: ForecastFold,
    result: ModelResult,
    predictions: list[dict],
    seasonal_period: int,
) -> dict:
    """Return one fold-level metric dict for a single (fold, model) pair.

    MASE denominator is computed exclusively from the fold's training series.
    Interval metrics are only computed when both bounds are present and the
    model did not fail.
    """
    base = {
        "report_id": report_id,
        "fold_number": fold.fold_number,
        "cutoff_date": fold.cutoff_date,
        "model_name": result.model_name,
        "error_message": result.error_message,
    }

    if result.fit_status == "failed":
        return {
            **base,
            "mae": np.nan, "rmse": np.nan, "wape": np.nan,
            "mase": np.nan, "bias": np.nan,
            "interval_coverage": np.nan, "mean_interval_width": np.nan,
            "fit_status": "failed",
        }

    # Extract aligned actual and forecast arrays from the prediction rows
    # (guarantees metric computation uses the same aligned values shown to users)
    actual_arr = np.array([r["actual"] for r in predictions])
    forecast_arr = np.array([r["forecast"] for r in predictions])

    pt = calculate_point_metrics(
        actual_arr,
        forecast_arr,
        training_series=fold.train_series.to_numpy(),
        seasonal_period=seasonal_period,
    )

    # Interval metrics only when the model produced bounds
    has_bounds = (
        result.lower_bound is not None
        and result.upper_bound is not None
        and result.fit_status != "failed"
    )
    if has_bounds:
        lo_arr = np.array([r["lower_bound"] for r in predictions])
        hi_arr = np.array([r["upper_bound"] for r in predictions])
        iv = calculate_interval_metrics(actual_arr, lo_arr, hi_arr)
        interval_coverage = iv["interval_coverage"]
        mean_interval_width = iv["mean_interval_width"]
    else:
        interval_coverage = np.nan
        mean_interval_width = np.nan

    return {
        **base,
        "mae": pt["mae"],
        "rmse": pt["rmse"],
        "wape": pt["wape"],
        "mase": pt["mase"],
        "bias": pt["bias"],
        "interval_coverage": interval_coverage,
        "mean_interval_width": mean_interval_width,
        "fit_status": result.fit_status,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def evaluate_models_across_folds(
    report_id: str,
    series: pd.Series,
    model_registry: dict[str, Callable],
    backtest_config: Optional[BacktestConfig] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Evaluate every model in *model_registry* on every rolling-origin fold.

    Parameters
    ----------
    report_id:
        Identifier carried through to both output DataFrames so results from
        different reports can be concatenated and filtered.
    series:
        Continuous daily ``pd.Series`` with a sorted ``DatetimeIndex``.
        Passed directly to ``generate_rolling_splits``; all continuity and
        sort constraints from that function apply.
    model_registry:
        Mapping of ``model_name → callable``.  Each callable must accept
        ``(training_series: pd.Series, horizon: int)`` and return a
        ``ModelResult``.  Non-``ModelResult`` returns and any raised exceptions
        are caught and recorded as failed rows without aborting other models.
    backtest_config:
        Controls fold generation and metric computation.  Defaults to
        ``BacktestConfig()`` which uses portfolio-wide constants from
        ``src/config/forecasting.py``.

    Returns
    -------
    predictions : pd.DataFrame
        One row per (fold, model, forecast_date).  Columns: ``report_id``,
        ``fold_number``, ``cutoff_date``, ``train_start``, ``train_end``,
        ``forecast_date``, ``horizon_step``, ``model_name``, ``actual``,
        ``forecast``, ``lower_bound``, ``upper_bound``, ``fit_status``.
        Sorted by (report_id, fold_number, model_name, horizon_step).

    fold_metrics : pd.DataFrame
        One row per (fold, model).  Columns: ``report_id``, ``fold_number``,
        ``cutoff_date``, ``model_name``, ``mae``, ``rmse``, ``wape``,
        ``mase``, ``bias``, ``interval_coverage``, ``mean_interval_width``,
        ``fit_status``, ``error_message``.
        Sorted by (report_id, fold_number, model_name).

    Raises
    ------
    ValueError
        Propagated from ``generate_rolling_splits`` when the series is too
        short to produce even one fold.
    """
    if backtest_config is None:
        backtest_config = BacktestConfig()

    cfg = backtest_config

    # Generate folds — raises ValueError if series too short for even one fold
    folds, _split_status = generate_rolling_splits(
        series,
        horizon=cfg.horizon,
        n_folds=cfg.n_folds,
        step=cfg.step,
        min_train_size=cfg.min_train_size,
    )

    all_pred_rows: list[dict] = []
    all_metric_rows: list[dict] = []

    for fold in folds:
        for model_name, model_fn in model_registry.items():
            # Run the model — catch anything that escapes from non-candidates callables
            try:
                result = model_fn(fold.train_series, cfg.horizon)
                if not isinstance(result, ModelResult):
                    raise TypeError(
                        f"model '{model_name}' returned {type(result).__name__}, "
                        "expected ModelResult"
                    )
            except Exception as exc:
                result = _make_failed_result(model_name, exc)

            pred_rows = _build_prediction_rows(report_id, fold, result)
            metric_row = _build_metric_row(
                report_id, fold, result, pred_rows, cfg.seasonal_period
            )

            all_pred_rows.extend(pred_rows)
            all_metric_rows.append(metric_row)

    # Build DataFrames with guaranteed column order
    if all_pred_rows:
        predictions = pd.DataFrame(all_pred_rows)[_PRED_COLS]
    else:
        predictions = pd.DataFrame(columns=_PRED_COLS)

    if all_metric_rows:
        fold_metrics = pd.DataFrame(all_metric_rows)[_METRIC_COLS]
    else:
        fold_metrics = pd.DataFrame(columns=_METRIC_COLS)

    # Deterministic sort
    predictions = predictions.sort_values(
        ["report_id", "fold_number", "model_name", "horizon_step"],
        ignore_index=True,
    )
    fold_metrics = fold_metrics.sort_values(
        ["report_id", "fold_number", "model_name"],
        ignore_index=True,
    )

    return predictions, fold_metrics
