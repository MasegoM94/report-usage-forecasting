"""Horizon-bucket evaluation of rolling-backtest predictions.

Partitions prediction-level backtest results into fixed horizon windows and
computes evaluation metrics for each bucket.

Buckets
-------
days_1_7    — horizon_step  1 –  7  (near-term, within one seasonal cycle)
days_8_14   — horizon_step  8 – 14  (mid-term, second seasonal cycle)
days_15_28  — horizon_step 15 – 28  (long-term, weeks 3-4)
full_horizon — horizon_step  1 – 28  (summary across the whole horizon)

Day boundaries are inclusive.  The bucket names are fixed; only the upper
bound of the last bucket changes when ``FORECAST_HORIZON_DAYS`` is not 28.

MASE denominator
----------------
MASE is defined as MAE / (seasonal-naive MAE in the training window).  For
bucket evaluation the *numerator* (MAE) is computed from the bucket subset
only, but the *denominator* is always the seasonal-naive error computed from
the full fold training series — identical to the denominator used in the
fold-level MASE produced by ``evaluate_models_across_folds``.

This requires the original series for each report so the training window can
be reconstructed from the ``train_start`` / ``train_end`` timestamps stored in
the prediction rows.  Callers provide this via the ``series_lookup`` argument.

Public API
----------
HORIZON_BUCKETS : list[tuple[str, int, int]]
    Ordered bucket definitions: (name, step_lo_inclusive, step_hi_inclusive).

calculate_horizon_bucket_metrics(predictions, series_lookup, ...)
    Returns a tidy DataFrame with one row per
    (report_id, fold_number, model_name, horizon_bucket).
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from src.config.forecasting import FORECAST_HORIZON_DAYS
from src.models.metrics import calculate_interval_metrics, calculate_point_metrics


# ---------------------------------------------------------------------------
# Bucket definitions
# ---------------------------------------------------------------------------

HORIZON_BUCKETS: list[tuple[str, int, int]] = [
    ("days_1_7",     1,  7),
    ("days_8_14",    8,  14),
    ("days_15_28",   15, FORECAST_HORIZON_DAYS),
    ("full_horizon", 1,  FORECAST_HORIZON_DAYS),
]
"""Ordered list of (bucket_name, step_lo, step_hi) tuples.

``step_lo`` and ``step_hi`` are inclusive horizon-step bounds.  Both day 7
and day 14 fall into ``days_1_7`` and ``days_8_14`` respectively; day 28 falls
into ``days_15_28`` and ``full_horizon``.
"""

# Output column order
_OUTPUT_COLS = [
    "report_id",
    "model_name",
    "fold_number",
    "cutoff_date",
    "horizon_bucket",
    "mae",
    "wape",
    "mase_lag1",
    "bias",
    "interval_coverage",
    "observation_count",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reconstruct_training_series(
    report_id: str,
    train_start: pd.Timestamp,
    train_end: pd.Timestamp,
    series_lookup: dict[str, pd.Series],
) -> pd.Series:
    """Slice the original series to the fold training window [train_start, train_end]."""
    if report_id not in series_lookup:
        raise KeyError(
            f"report_id '{report_id}' not found in series_lookup. "
            f"Available keys: {sorted(series_lookup.keys())}"
        )
    s = series_lookup[report_id]
    # Boolean mask so the slice works with any DatetimeIndex (no .loc KeyError on boundaries)
    mask = (s.index >= train_start) & (s.index <= train_end)
    return s.loc[mask]


def _bucket_metrics_row(
    report_id: str,
    fold_number: int,
    cutoff_date: pd.Timestamp,
    model_name: str,
    bucket_name: str,
    bucket_preds: pd.DataFrame,
    fold_train_series: pd.Series,
) -> dict:
    """Compute metrics for one (fold, model, bucket) group.

    Parameters
    ----------
    bucket_preds:
        Subset of the predictions DataFrame filtered to this bucket's
        horizon_step range.  May contain NaN forecasts (failed model-folds).
    fold_train_series:
        Full training series for this fold — used as the mase_lag1 denominator
        (lag-1 random-walk error) regardless of which bucket is being evaluated.
        The denominator is shared across all buckets in the same fold so that
        mase_lag1 values are comparable.
    """
    base = {
        "report_id": report_id,
        "model_name": model_name,
        "fold_number": fold_number,
        "cutoff_date": cutoff_date,
        "horizon_bucket": bucket_name,
    }

    # Only evaluate rows where a forecast was produced (excludes failed model-folds)
    valid = bucket_preds.dropna(subset=["forecast"])

    if valid.empty:
        return {
            **base,
            "mae": np.nan,
            "wape": np.nan,
            "mase_lag1": np.nan,
            "bias": np.nan,
            "interval_coverage": np.nan,
            "observation_count": 0,
        }

    actual_arr = valid["actual"].to_numpy(dtype=float)
    forecast_arr = valid["forecast"].to_numpy(dtype=float)

    # mase_lag1 uses a lag-1 denominator from the fold training series.
    # Only the MAE numerator changes between buckets; the denominator is constant
    # per fold so mase_lag1 values are proportional to MAE and remain comparable.
    pt = calculate_point_metrics(
        actual_arr,
        forecast_arr,
        training_series=fold_train_series.to_numpy(),
    )

    # Interval coverage — only when both bounds are present and non-NaN
    has_bounds = (
        "lower_bound" in valid.columns
        and "upper_bound" in valid.columns
        and valid["lower_bound"].notna().any()
        and valid["upper_bound"].notna().any()
    )
    if has_bounds:
        lo = valid["lower_bound"].to_numpy(dtype=float)
        hi = valid["upper_bound"].to_numpy(dtype=float)
        iv = calculate_interval_metrics(actual_arr, lo, hi)
        interval_coverage = iv["interval_coverage"]
    else:
        interval_coverage = np.nan

    return {
        **base,
        "mae": pt["mae"],
        "wape": pt["wape"],
        "mase_lag1": pt["mase_lag1"],
        "bias": pt["bias"],
        "interval_coverage": interval_coverage,
        "observation_count": len(valid),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def calculate_horizon_bucket_metrics(
    predictions: pd.DataFrame,
    series_lookup: dict[str, pd.Series],
    buckets: Optional[list[tuple[str, int, int]]] = None,
) -> pd.DataFrame:
    """Compute per-bucket metrics from prediction-level backtest results.

    Parameters
    ----------
    predictions:
        Prediction-level DataFrame produced by ``evaluate_models_across_folds``.
        Required columns: ``report_id``, ``fold_number``, ``cutoff_date``,
        ``train_start``, ``train_end``, ``horizon_step``, ``model_name``,
        ``actual``, ``forecast``.  Optional columns: ``lower_bound``,
        ``upper_bound``, ``fit_status``.
    series_lookup:
        Mapping of ``report_id → pd.Series`` (the original daily series with a
        DatetimeIndex) used to reconstruct each fold's training window for the
        mase_lag1 denominator.  Must cover every ``report_id`` present in
        *predictions*.
    buckets:
        Override the default ``HORIZON_BUCKETS`` list.  Each element is a
        ``(name, step_lo, step_hi)`` tuple with inclusive step bounds.  Pass
        ``None`` (default) to use the module-level ``HORIZON_BUCKETS``.

    Returns
    -------
    pd.DataFrame
        One row per (report_id, fold_number, model_name, horizon_bucket).
        Columns: ``report_id``, ``model_name``, ``fold_number``,
        ``cutoff_date``, ``horizon_bucket``, ``mae``, ``wape``, ``mase_lag1``,
        ``bias``, ``interval_coverage``, ``observation_count``.
        Sorted by (report_id, model_name, fold_number, horizon_bucket).

    Raises
    ------
    ValueError
        If any required column is missing from *predictions*.
    KeyError
        If *series_lookup* does not contain a key for every report_id in
        *predictions*.
    """
    required_cols = {
        "report_id", "fold_number", "cutoff_date",
        "train_start", "train_end",
        "horizon_step", "model_name", "actual", "forecast",
    }
    missing = required_cols - set(predictions.columns)
    if missing:
        raise ValueError(
            f"predictions DataFrame is missing required columns: {sorted(missing)}"
        )

    if buckets is None:
        buckets = HORIZON_BUCKETS

    # Ensure timestamps are proper Timestamps (not strings)
    preds = predictions.copy()
    for col in ("cutoff_date", "train_start", "train_end", "forecast_date"):
        if col in preds.columns:
            preds[col] = pd.to_datetime(preds[col])

    # Pre-compute MASE denominators per (report_id, fold_number).
    # The denominator is independent of the model and bucket — only the
    # training window determines it.
    _denom_cache: dict[tuple[str, int], pd.Series] = {}

    def _get_training_series(report_id: str, fold_number: int) -> pd.Series:
        key = (report_id, fold_number)
        if key not in _denom_cache:
            # All rows for this (report_id, fold_number) share the same
            # train_start / train_end — take the first occurrence
            mask = (preds["report_id"] == report_id) & (preds["fold_number"] == fold_number)
            row = preds.loc[mask].iloc[0]
            _denom_cache[key] = _reconstruct_training_series(
                report_id,
                row["train_start"],
                row["train_end"],
                series_lookup,
            )
        return _denom_cache[key]

    rows: list[dict] = []

    # Group by (report_id, fold_number, model_name) — the natural evaluation unit
    group_keys = ["report_id", "fold_number", "model_name"]
    for (report_id, fold_number, model_name), group in preds.groupby(group_keys, sort=False):
        cutoff_date = group["cutoff_date"].iloc[0]
        fold_train = _get_training_series(report_id, fold_number)

        for bucket_name, step_lo, step_hi in buckets:
            bucket_mask = (
                (group["horizon_step"] >= step_lo)
                & (group["horizon_step"] <= step_hi)
            )
            bucket_preds = group.loc[bucket_mask]

            if bucket_preds.empty:
                # Bucket has no rows for this horizon (e.g. horizon < step_lo)
                rows.append({
                    "report_id": report_id,
                    "model_name": model_name,
                    "fold_number": fold_number,
                    "cutoff_date": cutoff_date,
                    "horizon_bucket": bucket_name,
                    "mae": np.nan,
                    "wape": np.nan,
                    "mase_lag1": np.nan,
                    "bias": np.nan,
                    "interval_coverage": np.nan,
                    "observation_count": 0,
                })
                continue

            rows.append(
                _bucket_metrics_row(
                    report_id=report_id,
                    fold_number=fold_number,
                    cutoff_date=cutoff_date,
                    model_name=model_name,
                    bucket_name=bucket_name,
                    bucket_preds=bucket_preds,
                    fold_train_series=fold_train,
                )
            )

    if not rows:
        return pd.DataFrame(columns=_OUTPUT_COLS)

    result = pd.DataFrame(rows)[_OUTPUT_COLS]
    result = result.sort_values(
        ["report_id", "model_name", "fold_number", "horizon_bucket"],
        ignore_index=True,
    )
    return result
