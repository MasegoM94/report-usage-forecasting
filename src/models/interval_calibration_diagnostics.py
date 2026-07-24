"""Prediction-interval calibration and scoring diagnostics.

Determines whether prediction intervals:
1. Achieve approximately the intended nominal coverage.
2. Are sufficiently narrow to be useful.
3. Become less reliable at longer forecast horizons.
4. Penalise interval misses according to their magnitude (Winkler score).
5. Have enough valid observations for a defensible conclusion.

Evidence sources
----------------
Backtest forecast errors: rolling-origin fold-level and cross-fold.
Production forecast errors: all-record and deduplicated-date views.

Training residuals do not contain forecast intervals and are excluded.

Nominal interval policy
-----------------------
All candidate forecast functions use ``alpha=0.05`` (95% central PI).
``nominal_coverage = 1 - alpha = 0.95`` is stored explicitly with every
backtest prediction row and every production forecast row.  Legacy rows
where the interval level is unknown retain ``nominal_coverage = null``
and are excluded from Winkler scoring (which requires alpha).

Do not infer that every historical interval was 95%.

Winkler interval score
----------------------
For a central PI with ``alpha = 1 - nominal_coverage``::

    if actual in [lower, upper]:
        score = upper - lower

    if actual < lower:
        score = (upper - lower) + (2 / alpha) * (lower - actual)

    if actual > upper:
        score = (upper - lower) + (2 / alpha) * (actual - upper)

Lower score is better.  Inside-interval rows receive score = width.

Width normalisation
-------------------
Group-level normalisation scale::

    scale = mean(actual) over valid interval rows   (when > 0)
    fallback: mean(forecast) over valid interval rows  (when > 0)
    otherwise: null (do not divide by epsilon)

Normalised width and Winkler score use this group scale consistently.

Horizon buckets (inclusive)
---------------------------
days_1_7   : horizon_step  1 –  7
days_8_14  : horizon_step  8 – 14
days_15_28 : horizon_step 15 – 28

Valid interval observation
--------------------------
A row is a valid interval observation only when:
- actual is finite
- lower_bound is finite
- upper_bound is finite
- lower_bound <= upper_bound
- nominal_coverage is a finite value in (0, 1)

Rows without bounds are not counted as interval misses.

Coverage convention
-------------------
    coverage_gap = observed_coverage - nominal_coverage

Negative gap → undercoverage (actual is outside the interval more than expected).
Positive gap → overcoverage (intervals are conservative).

Public API
----------
calculate_interval_row_metrics(actual, lower, upper, nominal_coverage) -> dict
calculate_winkler_score(actual, lower, upper, alpha) -> float | None
calculate_interval_calibration_metrics(df, cfg, norm_scale) -> dict
calculate_horizon_interval_metrics(df, cfg) -> dict
build_backtest_interval_diagnostics_by_fold(df, cfg) -> pd.DataFrame
build_backtest_interval_diagnostics_summary(fold_df, bt_df, cfg) -> pd.DataFrame
build_production_interval_diagnostics(df, cfg) -> pd.DataFrame
classify_interval_calibration(metrics, cfg) -> tuple[str, list[str]]
validate_interval_calibration_diagnostics(df, dataset_name) -> None
persist_interval_calibration_diagnostics(fold_df, summary_df, prod_df, project_root) -> dict
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

_DEFAULT_NOMINAL_COVERAGE = 0.95


@dataclass
class IntervalCalibrationConfig:
    """Thresholds for interval calibration classification."""

    NOMINAL_INTERVAL_COVERAGE: float = _DEFAULT_NOMINAL_COVERAGE

    # Minimum valid interval observations to compute calibration metrics.
    MIN_INTERVAL_OBSERVATIONS: int = 10
    MIN_INTERVAL_OBSERVATIONS_PER_FOLD: int = 5
    MIN_INTERVAL_OBSERVATIONS_PER_HORIZON_BUCKET: int = 5

    # Coverage tolerance (|coverage_gap| <= this → well_calibrated).
    COVERAGE_TOLERANCE: float = 0.03          # ±3 pp

    # Undercoverage thresholds (coverage_gap = obs - nominal, so negative = under).
    UNDERCOVERAGE_WARNING_GAP: float = -0.05  # −5 pp
    UNDERCOVERAGE_POOR_GAP: float = -0.10     # −10 pp
    UNDERCOVERAGE_SEVERE_GAP: float = -0.15   # −15 pp

    # Overcoverage threshold.
    OVERCOVERAGE_WARNING_GAP: float = 0.10    # +10 pp

    # Normalised width thresholds.
    NORMALIZED_WIDTH_WARNING_THRESHOLD: float = 2.0
    NORMALIZED_WIDTH_POOR_THRESHOLD: float = 4.0

    # Normalised Winkler score thresholds.
    NORMALIZED_WINKLER_WARNING_THRESHOLD: float = 2.5
    NORMALIZED_WINKLER_POOR_THRESHOLD: float = 5.0

    # Horizon deterioration.
    HORIZON_COVERAGE_DROP_WARNING: float = 0.05   # late vs early coverage drop
    HORIZON_COVERAGE_DROP_POOR: float = 0.10
    HORIZON_WIDTH_GROWTH_WARNING_RATIO: float = 2.0
    HORIZON_WIDTH_GROWTH_POOR_RATIO: float = 4.0

    # Minimum valid folds before cross-fold interval summary is credible.
    MIN_VALID_FOLDS_FOR_INTERVAL_SUMMARY: int = 2


_DEFAULT_CFG = IntervalCalibrationConfig()

# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------

_EVIDENCE_COLS = [
    "interval_observation_count",
    "total_forecast_observation_count",
    "missing_interval_count",
    "invalid_interval_count",
    "interval_availability_rate",
    "first_interval_date",
    "last_interval_date",
    "evidence_status",
]

_COVERAGE_COLS = [
    "nominal_coverage",
    "observed_coverage",
    "coverage_gap",
    "absolute_coverage_gap",
    "interval_hit_count",
    "interval_miss_count",
    "lower_miss_count",
    "upper_miss_count",
    "lower_miss_rate",
    "upper_miss_rate",
]

_WIDTH_COLS = [
    "mean_interval_width",
    "median_interval_width",
    "p90_interval_width",
    "min_interval_width",
    "max_interval_width",
    "interval_normalization_scale",
    "interval_normalization_method",
    "interval_normalization_status",
    "normalized_mean_interval_width",
    "normalized_median_interval_width",
]

_MISS_SEVERITY_COLS = [
    "mean_miss_distance",
    "median_miss_distance",
    "max_miss_distance",
    "mean_lower_miss_distance",
    "mean_upper_miss_distance",
]

_WINKLER_COLS = [
    "mean_winkler_score",
    "median_winkler_score",
    "p90_winkler_score",
    "normalized_mean_winkler_score",
    "normalized_median_winkler_score",
]

_HORIZON_COLS = [
    "early_horizon_coverage",
    "middle_horizon_coverage",
    "late_horizon_coverage",
    "early_horizon_interval_count",
    "middle_horizon_interval_count",
    "late_horizon_interval_count",
    "horizon_coverage_range",
    "early_horizon_normalized_width",
    "middle_horizon_normalized_width",
    "late_horizon_normalized_width",
    "horizon_width_growth_ratio",
    "early_horizon_winkler_score",
    "middle_horizon_winkler_score",
    "late_horizon_winkler_score",
    "horizon_calibration_deterioration_flag",
]

_CLASSIFICATION_COLS = [
    "calibration_status",
    "interval_usefulness_status",
    "calibration_reasons",
    "calibration_evidence_status",
]

BACKTEST_FOLD_INTERVAL_COLS: list[str] = (
    [
        "evaluation_run_id",
        "report_id",
        "report_name",
        "model_family",
        "model_name",
        "candidate_m",
        "fold_number",
        "cutoff_date",
        "train_start",
        "train_end",
    ]
    + _EVIDENCE_COLS
    + _COVERAGE_COLS
    + _WIDTH_COLS
    + _MISS_SEVERITY_COLS
    + _WINKLER_COLS
    + _HORIZON_COLS
    + _CLASSIFICATION_COLS
)

BACKTEST_SUMMARY_INTERVAL_COLS: list[str] = [
    "evaluation_run_id",
    "report_id",
    "report_name",
    "model_family",
    "model_name",
    "candidate_m",
    "nominal_coverage",
    # fold counts
    "total_fold_count",
    "valid_fold_count",
    "sufficient_interval_fold_count",
    "undercoverage_fold_count",
    "acceptable_fold_count",
    "overwide_fold_count",
    "poor_fold_count",
    # pooled and median coverage
    "median_observed_coverage",
    "pooled_observed_coverage",
    "median_coverage_gap",
    "mean_absolute_coverage_gap",
    # width
    "median_normalized_interval_width",
    "mean_normalized_interval_width",
    # Winkler
    "median_normalized_winkler_score",
    "mean_normalized_winkler_score",
    # miss direction
    "lower_miss_fold_count",
    "upper_miss_fold_count",
    # classification
    "cross_fold_calibration_status",
    "cross_fold_interval_usefulness_status",
    "cross_fold_calibration_reasons",
]

PRODUCTION_INTERVAL_COLS: list[str] = (
    [
        "evaluation_run_id",
        "report_id",
        "report_name",
        "selected_model_family",
        "selected_model_name",
        "selected_m",
        "lineage_complete",
        # production overlap counts
        "original_prediction_count",
        "deduplicated_date_count",
        "excluded_overlap_count",
    ]
    + _EVIDENCE_COLS
    + _COVERAGE_COLS
    + _WIDTH_COLS
    + _MISS_SEVERITY_COLS
    + _WINKLER_COLS
    + [
        # all-records vs deduplicated explicit labels
        "all_record_observed_coverage",
        "all_record_mean_winkler_score",
        "deduplicated_observed_coverage",
        "deduplicated_mean_winkler_score",
    ]
    + _HORIZON_COLS
    + _CLASSIFICATION_COLS
)

# ---------------------------------------------------------------------------
# Valid status sets
# ---------------------------------------------------------------------------

_VALID_CALIBRATION_STATUSES = frozenset({
    "well_calibrated", "slight_undercoverage", "undercoverage",
    "severe_undercoverage", "overcoverage", "insufficient_evidence",
    "calculation_failed",
})

_VALID_USEFULNESS_STATUSES = frozenset({
    "useful", "wide_but_usable", "overwide", "poor",
    "insufficient_evidence", "unavailable",
})

# ---------------------------------------------------------------------------
# Row-level Winkler score
# ---------------------------------------------------------------------------


def calculate_winkler_score(
    actual: float,
    lower: float,
    upper: float,
    alpha: float,
) -> float | None:
    """Compute the Winkler interval score for one observation.

    Parameters
    ----------
    actual, lower, upper:
        Scalar floats; must all be finite.
    alpha:
        Must satisfy 0 < alpha < 1.  Use ``alpha = 1 - nominal_coverage``.

    Returns
    -------
    Non-negative Winkler score, or None when inputs are invalid.

    Notes
    -----
    * Inside interval:  score = upper - lower
    * Below lower:     score = (upper - lower) + (2/alpha) * (lower - actual)
    * Above upper:     score = (upper - lower) + (2/alpha) * (actual - upper)
    """
    if not (0 < alpha < 1):
        return None
    if not (np.isfinite(actual) and np.isfinite(lower) and np.isfinite(upper)):
        return None
    if lower > upper:
        return None
    width = upper - lower
    if actual < lower:
        return float(width + (2.0 / alpha) * (lower - actual))
    if actual > upper:
        return float(width + (2.0 / alpha) * (actual - upper))
    return float(width)


def calculate_interval_row_metrics(
    actual: float,
    lower: float,
    upper: float,
    nominal_coverage: float | None,
) -> dict:
    """Compute row-level interval metrics.

    Returns a dict with keys: inside_interval, interval_width,
    lower_miss_distance, upper_miss_distance, miss_distance,
    winkler_score, interval_observation_valid, interval_availability_status.
    """
    out: dict = {
        "inside_interval": None,
        "interval_width": None,
        "lower_miss_distance": None,
        "upper_miss_distance": None,
        "miss_distance": None,
        "winkler_score": None,
        "interval_observation_valid": False,
        "interval_availability_status": "unavailable",
    }

    if not (np.isfinite(lower) and np.isfinite(upper)):
        out["interval_availability_status"] = "missing_bounds"
        return out

    if lower > upper:
        out["interval_availability_status"] = "invalid_bounds"
        return out

    if not np.isfinite(actual):
        out["interval_availability_status"] = "missing_actual"
        return out

    if nominal_coverage is None or not np.isfinite(nominal_coverage) or not (0 < nominal_coverage < 1):
        out["interval_availability_status"] = "unknown_nominal_coverage"
        # still compute coverage and width even without Winkler
        out["inside_interval"] = bool(lower <= actual <= upper)
        out["interval_width"] = float(upper - lower)
        lo_miss = max(0.0, lower - actual)
        up_miss = max(0.0, actual - upper)
        out["lower_miss_distance"] = lo_miss
        out["upper_miss_distance"] = up_miss
        out["miss_distance"] = lo_miss + up_miss
        out["interval_observation_valid"] = True
        return out

    alpha = 1.0 - nominal_coverage
    out["inside_interval"] = bool(lower <= actual <= upper)
    out["interval_width"] = float(upper - lower)
    lo_miss = max(0.0, lower - actual)
    up_miss = max(0.0, actual - upper)
    out["lower_miss_distance"] = lo_miss
    out["upper_miss_distance"] = up_miss
    out["miss_distance"] = lo_miss + up_miss
    out["winkler_score"] = calculate_winkler_score(actual, lower, upper, alpha)
    out["interval_observation_valid"] = True
    out["interval_availability_status"] = "available"
    return out


# ---------------------------------------------------------------------------
# Normalisation scale
# ---------------------------------------------------------------------------


def _compute_norm_scale(
    actuals: np.ndarray,
    forecasts: np.ndarray,
) -> tuple[float | None, str, str]:
    """Return (scale, method, status).

    Primary: mean actual when > 0.
    Fallback: mean forecast when > 0.
    Otherwise: (None, 'none', 'no_valid_scale').
    """
    if len(actuals):
        mean_act = float(np.nanmean(actuals))
        if mean_act > 0:
            return mean_act, "mean_actual", "ok"
    if len(forecasts):
        mean_fc = float(np.nanmean(forecasts))
        if mean_fc > 0:
            return mean_fc, "mean_forecast", "fallback"
    return None, "none", "no_valid_scale"


# ---------------------------------------------------------------------------
# Group-level calibration metrics
# ---------------------------------------------------------------------------


def calculate_interval_calibration_metrics(
    df: pd.DataFrame,
    cfg: IntervalCalibrationConfig = _DEFAULT_CFG,
    norm_scale: float | None = None,
    norm_method: str = "none",
    norm_status: str = "no_valid_scale",
    date_col: str = "forecast_date",
) -> dict:
    """Aggregate interval calibration metrics from a pre-filtered group.

    Parameters
    ----------
    df:
        DataFrame with valid interval observations; must have columns:
        actual, lower_bound, upper_bound, nominal_coverage, forecast,
        inside_interval, interval_width, horizon_step (optional).
    cfg:
        Configuration thresholds.
    norm_scale, norm_method, norm_status:
        Group-level normalisation scale (pre-computed by caller).
    date_col:
        Column to use for first/last date.

    Returns
    -------
    Flat dict covering _EVIDENCE_COLS + _COVERAGE_COLS + _WIDTH_COLS +
    _MISS_SEVERITY_COLS + _WINKLER_COLS.
    """
    out: dict = {}

    total_count = len(df)
    out["total_forecast_observation_count"] = total_count

    # Valid interval rows: finite actual, lower, upper; lower <= upper; valid nominal_coverage
    def _is_valid_interval(row):
        a = row.get("actual", np.nan)
        lo = row.get("lower_bound", np.nan)
        hi = row.get("upper_bound", np.nan)
        nc = row.get("nominal_coverage", np.nan)
        if not (np.isfinite(a) and np.isfinite(lo) and np.isfinite(hi)):
            return False
        if lo > hi:
            return False
        return True

    a_arr = pd.to_numeric(df["actual"] if "actual" in df.columns else pd.Series([], dtype=float), errors="coerce").values
    lo_arr = pd.to_numeric(df["lower_bound"] if "lower_bound" in df.columns else pd.Series([], dtype=float), errors="coerce").values
    hi_arr = pd.to_numeric(df["upper_bound"] if "upper_bound" in df.columns else pd.Series([], dtype=float), errors="coerce").values
    nc_arr = pd.to_numeric(df["nominal_coverage"] if "nominal_coverage" in df.columns else pd.Series([], dtype=float), errors="coerce").values

    # Missing bounds (either lo or hi is NaN)
    missing_bounds = ~(np.isfinite(lo_arr) & np.isfinite(hi_arr))
    # Invalid reversed intervals
    valid_bounds = np.isfinite(lo_arr) & np.isfinite(hi_arr)
    reversed_bounds = valid_bounds & (lo_arr > hi_arr)
    # Valid interval observations
    valid_mask = (
        np.isfinite(a_arr)
        & np.isfinite(lo_arr)
        & np.isfinite(hi_arr)
        & (lo_arr <= hi_arr)
    )

    missing_count = int(missing_bounds.sum())
    invalid_count = int(reversed_bounds.sum())
    valid_count = int(valid_mask.sum())

    out["interval_observation_count"] = valid_count
    out["missing_interval_count"] = missing_count
    out["invalid_interval_count"] = invalid_count
    out["interval_availability_rate"] = (
        valid_count / total_count if total_count > 0 else None
    )

    # Dates
    if date_col in df.columns and valid_count > 0:
        valid_dates = df.loc[valid_mask, date_col].dropna() if valid_mask.any() else pd.Series([], dtype="object")
        out["first_interval_date"] = valid_dates.min() if len(valid_dates) else None
        out["last_interval_date"] = valid_dates.max() if len(valid_dates) else None
    else:
        out["first_interval_date"] = None
        out["last_interval_date"] = None

    if valid_count < cfg.MIN_INTERVAL_OBSERVATIONS:
        out["evidence_status"] = "insufficient"
        # fill remaining with None
        for col in _COVERAGE_COLS + _WIDTH_COLS + _MISS_SEVERITY_COLS + _WINKLER_COLS:
            if col not in out:
                out[col] = None
        return out

    out["evidence_status"] = "ok"

    # Subset to valid rows
    valid_df = df[valid_mask].reset_index(drop=True)
    va = a_arr[valid_mask]
    vlo = lo_arr[valid_mask]
    vhi = hi_arr[valid_mask]
    vnc = nc_arr[valid_mask]
    fc_arr = pd.to_numeric(valid_df["forecast"] if "forecast" in valid_df.columns else pd.Series([np.nan] * valid_count), errors="coerce").values

    # Determine nominal_coverage for this group (use mode of valid rows)
    valid_nc = vnc[np.isfinite(vnc) & (vnc > 0) & (vnc < 1)]
    nc_group = float(np.median(valid_nc)) if len(valid_nc) else None
    out["nominal_coverage"] = nc_group

    # Coverage
    inside = (va >= vlo) & (va <= vhi)
    hit_count = int(inside.sum())
    miss_count = valid_count - hit_count
    observed_cov = hit_count / valid_count

    # Lower/upper misses
    lo_miss_mask = va < vlo
    up_miss_mask = va > vhi
    lo_miss_count = int(lo_miss_mask.sum())
    up_miss_count = int(up_miss_mask.sum())

    out["observed_coverage"] = observed_cov
    out["coverage_gap"] = observed_cov - nc_group if nc_group is not None else None
    out["absolute_coverage_gap"] = abs(observed_cov - nc_group) if nc_group is not None else None
    out["interval_hit_count"] = hit_count
    out["interval_miss_count"] = miss_count
    out["lower_miss_count"] = lo_miss_count
    out["upper_miss_count"] = up_miss_count
    out["lower_miss_rate"] = lo_miss_count / valid_count
    out["upper_miss_rate"] = up_miss_count / valid_count

    # Width
    widths = vhi - vlo
    out["mean_interval_width"] = float(np.mean(widths))
    out["median_interval_width"] = float(np.median(widths))
    out["p90_interval_width"] = float(np.percentile(widths, 90))
    out["min_interval_width"] = float(np.min(widths))
    out["max_interval_width"] = float(np.max(widths))

    # Normalisation
    if norm_scale is None:
        ns, nm, nst = _compute_norm_scale(va, fc_arr)
    else:
        ns, nm, nst = norm_scale, norm_method, norm_status

    out["interval_normalization_scale"] = ns
    out["interval_normalization_method"] = nm
    out["interval_normalization_status"] = nst

    if ns and ns > 0:
        out["normalized_mean_interval_width"] = float(np.mean(widths)) / ns
        out["normalized_median_interval_width"] = float(np.median(widths)) / ns
    else:
        out["normalized_mean_interval_width"] = None
        out["normalized_median_interval_width"] = None

    # Miss severity
    miss_dists = np.maximum(vlo - va, 0.0) + np.maximum(va - vhi, 0.0)
    lo_dists = np.maximum(vlo - va, 0.0)
    up_dists = np.maximum(va - vhi, 0.0)
    all_miss_dists = miss_dists[miss_dists > 0]

    out["mean_miss_distance"] = float(np.mean(all_miss_dists)) if len(all_miss_dists) else 0.0
    out["median_miss_distance"] = float(np.median(all_miss_dists)) if len(all_miss_dists) else 0.0
    out["max_miss_distance"] = float(np.max(miss_dists))
    out["mean_lower_miss_distance"] = float(np.mean(lo_dists[lo_dists > 0])) if lo_dists.any() else 0.0
    out["mean_upper_miss_distance"] = float(np.mean(up_dists[up_dists > 0])) if up_dists.any() else 0.0

    # Winkler scores
    winkler_scores = []
    for i in range(valid_count):
        nc_i = float(vnc[i]) if np.isfinite(vnc[i]) else None
        if nc_i and 0 < nc_i < 1:
            alpha_i = 1.0 - nc_i
            ws = calculate_winkler_score(float(va[i]), float(vlo[i]), float(vhi[i]), alpha_i)
            if ws is not None:
                winkler_scores.append(ws)

    if winkler_scores:
        ws_arr = np.array(winkler_scores)
        out["mean_winkler_score"] = float(np.mean(ws_arr))
        out["median_winkler_score"] = float(np.median(ws_arr))
        out["p90_winkler_score"] = float(np.percentile(ws_arr, 90))
        if ns and ns > 0:
            out["normalized_mean_winkler_score"] = float(np.mean(ws_arr)) / ns
            out["normalized_median_winkler_score"] = float(np.median(ws_arr)) / ns
        else:
            out["normalized_mean_winkler_score"] = None
            out["normalized_median_winkler_score"] = None
    else:
        out["mean_winkler_score"] = None
        out["median_winkler_score"] = None
        out["p90_winkler_score"] = None
        out["normalized_mean_winkler_score"] = None
        out["normalized_median_winkler_score"] = None

    return out


# ---------------------------------------------------------------------------
# Horizon calibration
# ---------------------------------------------------------------------------


def calculate_horizon_interval_metrics(
    df: pd.DataFrame,
    cfg: IntervalCalibrationConfig = _DEFAULT_CFG,
    norm_scale: float | None = None,
) -> dict:
    """Calculate interval metrics per horizon bucket.

    Buckets use actual horizon_step values, not row order.
    Buckets with fewer than MIN_INTERVAL_OBSERVATIONS_PER_HORIZON_BUCKET
    valid rows are excluded from comparison.

    Returns
    -------
    Flat dict with all _HORIZON_COLS keys.
    """
    base: dict = {k: None for k in _HORIZON_COLS}
    base["horizon_calibration_deterioration_flag"] = False

    if "horizon_step" not in df.columns:
        return base

    a_arr = pd.to_numeric(df["actual"], errors="coerce").values
    lo_arr = pd.to_numeric(df["lower_bound"], errors="coerce").values
    hi_arr = pd.to_numeric(df["upper_bound"], errors="coerce").values
    nc_arr = pd.to_numeric(df["nominal_coverage"], errors="coerce").values
    h_arr = pd.to_numeric(df["horizon_step"], errors="coerce").values
    fc_arr = pd.to_numeric(df["forecast"] if "forecast" in df.columns else pd.Series([np.nan] * len(df)), errors="coerce").values

    valid_mask = (
        np.isfinite(a_arr) & np.isfinite(lo_arr) & np.isfinite(hi_arr)
        & (lo_arr <= hi_arr) & np.isfinite(h_arr)
    )

    def _bucket(lo_h: int, hi_h: int) -> dict:
        bmask = valid_mask & (h_arr >= lo_h) & (h_arr <= hi_h)
        n = int(bmask.sum())
        if n < cfg.MIN_INTERVAL_OBSERVATIONS_PER_HORIZON_BUCKET:
            return {"count": n, "coverage": None, "norm_width": None, "winkler": None}
        bva = a_arr[bmask]
        blo = lo_arr[bmask]
        bhi = hi_arr[bmask]
        bnc = nc_arr[bmask]
        bfc = fc_arr[bmask]
        cov = float(np.mean((bva >= blo) & (bva <= bhi)))
        widths = bhi - blo
        # normalisation
        if norm_scale and norm_scale > 0:
            ns = norm_scale
        else:
            mean_a = float(np.nanmean(bva))
            ns = mean_a if mean_a > 0 else (float(np.nanmean(bfc)) if np.nanmean(bfc) > 0 else None)
        norm_w = float(np.mean(widths)) / ns if ns else None
        # Winkler
        ws_list = []
        for i in range(n):
            nc_i = float(bnc[i]) if np.isfinite(bnc[i]) else None
            if nc_i and 0 < nc_i < 1:
                ws = calculate_winkler_score(float(bva[i]), float(blo[i]), float(bhi[i]), 1.0 - nc_i)
                if ws is not None:
                    ws_list.append(ws)
        mean_winkler = float(np.mean(ws_list)) / ns if (ws_list and ns) else (float(np.mean(ws_list)) if ws_list else None)
        return {"count": n, "coverage": cov, "norm_width": norm_w, "winkler": mean_winkler}

    early = _bucket(1, 7)
    middle = _bucket(8, 14)
    late = _bucket(15, 28)

    base["early_horizon_coverage"] = early["coverage"]
    base["middle_horizon_coverage"] = middle["coverage"]
    base["late_horizon_coverage"] = late["coverage"]
    base["early_horizon_interval_count"] = early["count"]
    base["middle_horizon_interval_count"] = middle["count"]
    base["late_horizon_interval_count"] = late["count"]
    base["early_horizon_normalized_width"] = early["norm_width"]
    base["middle_horizon_normalized_width"] = middle["norm_width"]
    base["late_horizon_normalized_width"] = late["norm_width"]
    base["early_horizon_winkler_score"] = early["winkler"]
    base["middle_horizon_winkler_score"] = middle["winkler"]
    base["late_horizon_winkler_score"] = late["winkler"]

    # Coverage range and width growth
    covs = [v for v in [early["coverage"], middle["coverage"], late["coverage"]] if v is not None]
    if len(covs) >= 2:
        base["horizon_coverage_range"] = float(max(covs) - min(covs))

    # Width growth ratio: late / early
    if early["norm_width"] and late["norm_width"] and early["norm_width"] > 0:
        base["horizon_width_growth_ratio"] = float(late["norm_width"] / early["norm_width"])

    # Deterioration flag: practical coverage drop OR excessive width growth
    deteriorate = False
    if early["coverage"] is not None and late["coverage"] is not None:
        drop = early["coverage"] - late["coverage"]
        if drop >= cfg.HORIZON_COVERAGE_DROP_WARNING:
            deteriorate = True
    ratio = base.get("horizon_width_growth_ratio")
    if ratio is not None and ratio >= cfg.HORIZON_WIDTH_GROWTH_POOR_RATIO:
        deteriorate = True
    base["horizon_calibration_deterioration_flag"] = deteriorate

    return base


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def classify_interval_calibration(
    metrics: dict,
    cfg: IntervalCalibrationConfig = _DEFAULT_CFG,
) -> tuple[str, str, list[str]]:
    """Classify calibration status, usefulness status, and reasons.

    Parameters
    ----------
    metrics:
        Dict from calculate_interval_calibration_metrics + horizon metrics.

    Returns
    -------
    (calibration_status, usefulness_status, reasons)
    """
    evidence = metrics.get("evidence_status", "insufficient")
    reasons: list[str] = []

    if evidence in ("insufficient", "empty"):
        n = metrics.get("interval_observation_count", 0) or 0
        reasons.append(
            f"Insufficient evidence: only {n} valid interval observations are available."
        )
        return "insufficient_evidence", "insufficient_evidence", reasons

    avail = metrics.get("interval_availability_rate")
    if avail is not None and avail == 0.0:
        reasons.append("Prediction intervals are unavailable for this model.")
        return "insufficient_evidence", "unavailable", reasons

    nc = metrics.get("nominal_coverage")
    obs_cov = metrics.get("observed_coverage")
    gap = metrics.get("coverage_gap")

    if obs_cov is None or nc is None:
        reasons.append("Coverage could not be computed.")
        return "calculation_failed", "insufficient_evidence", reasons

    # Coverage classification
    cal_status: str
    if gap is None:
        cal_status = "calculation_failed"
        reasons.append("Coverage gap is unavailable.")
    elif gap <= cfg.UNDERCOVERAGE_SEVERE_GAP:
        cal_status = "severe_undercoverage"
        reasons.append(
            f"Observed coverage is {obs_cov:.1%}, materially below the nominal "
            f"{nc:.0%} level (gap {gap:+.1%})."
        )
    elif gap <= cfg.UNDERCOVERAGE_POOR_GAP:
        cal_status = "undercoverage"
        reasons.append(
            f"Observed coverage is {obs_cov:.1%}, below the nominal {nc:.0%} level "
            f"(gap {gap:+.1%})."
        )
    elif gap <= cfg.UNDERCOVERAGE_WARNING_GAP:
        cal_status = "slight_undercoverage"
        reasons.append(
            f"Observed coverage is {obs_cov:.1%}, slightly below nominal {nc:.0%} "
            f"(gap {gap:+.1%})."
        )
    elif gap >= cfg.OVERCOVERAGE_WARNING_GAP:
        cal_status = "overcoverage"
        reasons.append(
            f"Observed coverage is {obs_cov:.1%}, above the nominal {nc:.0%} level; "
            "intervals may be conservative."
        )
    elif abs(gap) <= cfg.COVERAGE_TOLERANCE:
        cal_status = "well_calibrated"
        reasons.append(
            f"Prediction interval coverage is close to the nominal {nc:.0%} level "
            f"({obs_cov:.1%} observed, gap {gap:+.1%})."
        )
    elif gap > 0:
        cal_status = "well_calibrated"
        reasons.append(
            f"Prediction interval coverage is slightly above the nominal {nc:.0%} "
            f"level ({obs_cov:.1%} observed, gap {gap:+.1%})."
        )
    else:
        cal_status = "slight_undercoverage"
        reasons.append(
            f"Observed coverage {obs_cov:.1%} is near but below nominal {nc:.0%}."
        )

    # Miss direction
    lo_rate = metrics.get("lower_miss_rate", 0) or 0
    up_rate = metrics.get("upper_miss_rate", 0) or 0
    if lo_rate > 0.05:
        reasons.append(
            f"Most interval misses occur below the lower bound ({lo_rate:.1%}), "
            "indicating unusually low actual usage."
        )
    if up_rate > 0.05:
        reasons.append(
            f"Most interval misses occur above the upper bound ({up_rate:.1%}), "
            "indicating unusually high actual usage."
        )

    # Width / usefulness classification
    norm_width = metrics.get("normalized_mean_interval_width")
    norm_winkler = metrics.get("normalized_mean_winkler_score")
    use_status: str

    if norm_width is None and norm_winkler is None:
        use_status = "insufficient_evidence"
    elif norm_width is not None and norm_width >= cfg.NORMALIZED_WIDTH_POOR_THRESHOLD:
        use_status = "overwide"
        reasons.append(
            f"Intervals achieve high coverage but are wide relative to typical "
            f"report usage (normalised width {norm_width:.2f})."
        )
    elif norm_width is not None and norm_width >= cfg.NORMALIZED_WIDTH_WARNING_THRESHOLD:
        use_status = "wide_but_usable"
        reasons.append(
            f"Intervals are somewhat wide relative to report usage "
            f"(normalised width {norm_width:.2f})."
        )
    else:
        use_status = "useful"
        if norm_width is not None:
            reasons.append(
                f"Intervals are appropriately sized (normalised width {norm_width:.2f})."
            )

    if norm_winkler is not None and norm_winkler >= cfg.NORMALIZED_WINKLER_WARNING_THRESHOLD:
        reasons.append(
            f"The mean normalised Winkler score is {norm_winkler:.2f}, reflecting "
            "wide intervals and/or several large misses."
        )
        if use_status == "useful":
            use_status = "wide_but_usable"

    # Combined poor check
    if cal_status in ("severe_undercoverage",) and use_status in ("overwide",):
        cal_status = "undercoverage"  # not additionally degraded; handled separately

    # Horizon
    deteriorate = metrics.get("horizon_calibration_deterioration_flag", False)
    if deteriorate:
        reasons.append("Coverage deteriorates at longer forecast horizons.")

    return cal_status, use_status, reasons


# ---------------------------------------------------------------------------
# Shared filter helper
# ---------------------------------------------------------------------------


def _filter_valid_interval_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows where inside_interval can be meaningfully computed."""
    a = pd.to_numeric(df.get("actual", pd.Series([], dtype=float)), errors="coerce")
    lo = pd.to_numeric(df.get("lower_bound", pd.Series([], dtype=float)), errors="coerce")
    hi = pd.to_numeric(df.get("upper_bound", pd.Series([], dtype=float)), errors="coerce")
    valid = df["residual_observation_valid"].fillna(False).astype(bool) if "residual_observation_valid" in df.columns else pd.Series(True, index=df.index)
    mask = valid & a.notna() & lo.notna() & hi.notna()
    return df[mask].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Backtest fold-level diagnostics
# ---------------------------------------------------------------------------


def build_backtest_interval_diagnostics_by_fold(
    backtest_df: pd.DataFrame,
    cfg: IntervalCalibrationConfig = _DEFAULT_CFG,
    evaluation_run_id: str = "",
) -> pd.DataFrame:
    """Build per-fold backtest interval calibration diagnostics.

    Groups by (report_id, model_family, model_name, candidate_m, fold_number).
    Overlapping folds are never pooled at this level.

    Parameters
    ----------
    backtest_df:
        DataFrame matching BACKTEST_FORECAST_ERRORS_COLS schema.
    cfg, evaluation_run_id:
        As above.

    Returns
    -------
    DataFrame with BACKTEST_FOLD_INTERVAL_COLS columns.
    """
    if backtest_df.empty:
        return pd.DataFrame(columns=BACKTEST_FOLD_INTERVAL_COLS)

    group_keys = ["report_id", "model_family", "model_name", "candidate_m", "fold_number"]
    rows: list[dict] = []

    for group_vals, grp in backtest_df.groupby(group_keys, dropna=False):
        g = dict(zip(group_keys, group_vals if isinstance(group_vals, tuple) else (group_vals,)))
        first_row = grp.iloc[0]
        try:
            ns, nm, nst = _compute_norm_scale(
                pd.to_numeric(grp.get("actual", pd.Series([], dtype=float)), errors="coerce").dropna().values,
                pd.to_numeric(grp.get("forecast", pd.Series([], dtype=float)), errors="coerce").dropna().values,
            )
            cal = calculate_interval_calibration_metrics(grp, cfg, ns, nm, nst, date_col="forecast_date")
            horiz = calculate_horizon_interval_metrics(grp, cfg, ns)
            cal_status, use_status, reasons = classify_interval_calibration(
                {**cal, **horiz}, cfg
            )
            row = {
                "evaluation_run_id": evaluation_run_id,
                "report_id": g["report_id"],
                "report_name": first_row.get("report_name"),
                "model_family": g["model_family"],
                "model_name": g["model_name"],
                "candidate_m": g["candidate_m"],
                "fold_number": g["fold_number"],
                "cutoff_date": first_row.get("cutoff_date"),
                "train_start": first_row.get("train_start"),
                "train_end": first_row.get("train_end"),
                **cal,
                **horiz,
                "calibration_status": cal_status,
                "interval_usefulness_status": use_status,
                "calibration_reasons": "; ".join(reasons),
                "calibration_evidence_status": cal.get("evidence_status", "insufficient"),
            }
        except Exception as exc:
            _log.warning("Backtest interval fold failed for %s: %s", g, exc)
            row = {
                "evaluation_run_id": evaluation_run_id,
                "report_id": g.get("report_id"),
                "fold_number": g.get("fold_number"),
                "calibration_status": "calculation_failed",
                "calibration_reasons": f"Calculation failed: {exc}",
                "calibration_evidence_status": "calculation_failed",
            }
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=BACKTEST_FOLD_INTERVAL_COLS)

    result = pd.DataFrame(rows)
    for col in BACKTEST_FOLD_INTERVAL_COLS:
        if col not in result.columns:
            result[col] = None
    return result[BACKTEST_FOLD_INTERVAL_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Backtest cross-fold summary
# ---------------------------------------------------------------------------


def build_backtest_interval_diagnostics_summary(
    fold_df: pd.DataFrame,
    backtest_df: pd.DataFrame | None = None,
    cfg: IntervalCalibrationConfig = _DEFAULT_CFG,
    evaluation_run_id: str = "",
) -> pd.DataFrame:
    """Build cross-fold interval calibration summary.

    Pooled coverage = total hits / total valid observations (not mean of fold
    coverages).  Median coverage is also calculated.

    Parameters
    ----------
    fold_df:
        Output of build_backtest_interval_diagnostics_by_fold.
    backtest_df:
        Original backtest DataFrame for pooled computations (optional).
    cfg, evaluation_run_id:
        As above.

    Returns
    -------
    DataFrame with BACKTEST_SUMMARY_INTERVAL_COLS columns.
    """
    if fold_df.empty:
        return pd.DataFrame(columns=BACKTEST_SUMMARY_INTERVAL_COLS)

    summary_keys = ["report_id", "model_family", "model_name", "candidate_m"]
    rows: list[dict] = []

    for group_vals, grp in fold_df.groupby(summary_keys, dropna=False):
        g = dict(zip(summary_keys, group_vals if isinstance(group_vals, tuple) else (group_vals,)))
        first_row = grp.iloc[0]

        total_folds = len(grp)
        nc_mode = grp["nominal_coverage"].dropna().median() if "nominal_coverage" in grp.columns else None

        # Valid folds = those with evidence_status == "ok" or sufficient observations
        ev_ok = (
            (grp.get("calibration_evidence_status", grp.get("evidence_status", pd.Series())) == "ok")
            | (grp.get("calibration_status", pd.Series()) != "calculation_failed")
        )
        valid_folds = int(ev_ok.sum())
        sufficient = int(
            (grp.get("interval_observation_count", pd.Series(0)).fillna(0) >= cfg.MIN_INTERVAL_OBSERVATIONS_PER_FOLD).sum()
        )

        cal_statuses = grp.get("calibration_status", pd.Series())
        use_statuses = grp.get("interval_usefulness_status", pd.Series())

        undercoverage_folds = int(cal_statuses.isin(["undercoverage", "severe_undercoverage", "slight_undercoverage"]).sum())
        acceptable_folds = int(cal_statuses.isin(["well_calibrated", "overcoverage"]).sum())
        overwide_folds = int(use_statuses.isin(["overwide"]).sum())
        poor_folds = int(cal_statuses.isin(["severe_undercoverage"]).sum())

        obs_covs = pd.to_numeric(grp.get("observed_coverage", pd.Series()), errors="coerce").dropna()
        gaps = pd.to_numeric(grp.get("coverage_gap", pd.Series()), errors="coerce").dropna()
        med_cov = float(obs_covs.median()) if len(obs_covs) else None
        med_gap = float(gaps.median()) if len(gaps) else None
        mean_abs_gap = float(gaps.abs().mean()) if len(gaps) else None

        # Pooled coverage from original backtest_df
        pooled_cov: float | None = None
        if backtest_df is not None:
            mask = pd.Series(True, index=backtest_df.index)
            for k, v in g.items():
                if k in backtest_df.columns:
                    mask &= (backtest_df[k] == v) if not pd.isna(v) else backtest_df[k].isna()
            sub = backtest_df[mask]
            a_p = pd.to_numeric(sub.get("actual", pd.Series([], dtype=float)), errors="coerce").values
            lo_p = pd.to_numeric(sub.get("lower_bound", pd.Series([], dtype=float)), errors="coerce").values
            hi_p = pd.to_numeric(sub.get("upper_bound", pd.Series([], dtype=float)), errors="coerce").values
            valid_p = np.isfinite(a_p) & np.isfinite(lo_p) & np.isfinite(hi_p) & (lo_p <= hi_p)
            if valid_p.sum() >= cfg.MIN_INTERVAL_OBSERVATIONS:
                hits = int(((a_p[valid_p] >= lo_p[valid_p]) & (a_p[valid_p] <= hi_p[valid_p])).sum())
                pooled_cov = hits / int(valid_p.sum())

        norm_widths = pd.to_numeric(grp.get("normalized_mean_interval_width", pd.Series()), errors="coerce").dropna()
        norm_winklers = pd.to_numeric(grp.get("normalized_mean_winkler_score", pd.Series()), errors="coerce").dropna()

        lower_miss_folds = int((pd.to_numeric(grp.get("lower_miss_rate", pd.Series(0.0)), errors="coerce").fillna(0) > 0.05).sum())
        upper_miss_folds = int((pd.to_numeric(grp.get("upper_miss_rate", pd.Series(0.0)), errors="coerce").fillna(0) > 0.05).sum())

        # Cross-fold classification
        if sufficient < cfg.MIN_VALID_FOLDS_FOR_INTERVAL_SUMMARY:
            x_cal = "insufficient_evidence"
            x_use = "insufficient_evidence"
            x_reasons = ["Insufficient valid folds for interval calibration summary."]
        else:
            # Determine cross-fold calibration from individual fold statuses
            if any(s == "severe_undercoverage" for s in cal_statuses):
                x_cal = "severe_undercoverage"
                x_reasons = [f"Severe undercoverage in {poor_folds} fold(s)."]
            elif undercoverage_folds > total_folds / 2:
                x_cal = "undercoverage"
                x_reasons = [f"Undercoverage in {undercoverage_folds} of {total_folds} folds."]
            elif acceptable_folds == total_folds:
                x_cal = "well_calibrated"
                x_reasons = ["Prediction interval coverage is close to nominal across all folds."]
            else:
                x_cal = "slight_undercoverage"
                x_reasons = [f"Coverage is mixed across {total_folds} folds."]

            if overwide_folds > total_folds / 2:
                x_use = "overwide"
                x_reasons.append(f"Intervals are overwide in {overwide_folds} of {total_folds} folds.")
            elif len(norm_widths) and norm_widths.mean() >= cfg.NORMALIZED_WIDTH_WARNING_THRESHOLD:
                x_use = "wide_but_usable"
            else:
                x_use = "useful"

        row = {
            "evaluation_run_id": evaluation_run_id,
            "report_id": g["report_id"],
            "report_name": first_row.get("report_name"),
            "model_family": g["model_family"],
            "model_name": g["model_name"],
            "candidate_m": g["candidate_m"],
            "nominal_coverage": nc_mode,
            "total_fold_count": total_folds,
            "valid_fold_count": valid_folds,
            "sufficient_interval_fold_count": sufficient,
            "undercoverage_fold_count": undercoverage_folds,
            "acceptable_fold_count": acceptable_folds,
            "overwide_fold_count": overwide_folds,
            "poor_fold_count": poor_folds,
            "median_observed_coverage": med_cov,
            "pooled_observed_coverage": pooled_cov,
            "median_coverage_gap": med_gap,
            "mean_absolute_coverage_gap": mean_abs_gap,
            "median_normalized_interval_width": float(norm_widths.median()) if len(norm_widths) else None,
            "mean_normalized_interval_width": float(norm_widths.mean()) if len(norm_widths) else None,
            "median_normalized_winkler_score": float(norm_winklers.median()) if len(norm_winklers) else None,
            "mean_normalized_winkler_score": float(norm_winklers.mean()) if len(norm_winklers) else None,
            "lower_miss_fold_count": lower_miss_folds,
            "upper_miss_fold_count": upper_miss_folds,
            "cross_fold_calibration_status": x_cal,
            "cross_fold_interval_usefulness_status": x_use,
            "cross_fold_calibration_reasons": "; ".join(x_reasons),
        }
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=BACKTEST_SUMMARY_INTERVAL_COLS)

    result = pd.DataFrame(rows)
    for col in BACKTEST_SUMMARY_INTERVAL_COLS:
        if col not in result.columns:
            result[col] = None
    return result[BACKTEST_SUMMARY_INTERVAL_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Production interval diagnostics
# ---------------------------------------------------------------------------


def _deduplicate_production(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Shortest horizon_step per (report_id, forecast_date), tie-break by latest generated_at."""
    if df.empty or "horizon_step" not in df.columns:
        return df, 0
    sort_cols = ["report_id", "forecast_date", "horizon_step"]
    asc = [True, True, True]
    if "generated_at" in df.columns:
        sort_cols.append("generated_at")
        asc.append(False)
    sorted_df = df.sort_values(sort_cols, ascending=asc)
    deduped = sorted_df.drop_duplicates(subset=["report_id", "forecast_date"], keep="first")
    return deduped.reset_index(drop=True), len(df) - len(deduped)


def build_production_interval_diagnostics(
    production_df: pd.DataFrame,
    cfg: IntervalCalibrationConfig = _DEFAULT_CFG,
    evaluation_run_id: str = "",
) -> pd.DataFrame:
    """Build production interval calibration diagnostics.

    Calculates:
    - All-record metrics: every valid forecast row.
    - Deduplicated-date metrics: shortest-horizon per (report, forecast_date).

    Primary calibration status uses all-record evidence.  The deduplicated
    view is labelled separately for chronological interpretation.

    Incomplete lineage rows are preserved without inference.

    Parameters
    ----------
    production_df:
        DataFrame matching PRODUCTION_FORECAST_ERRORS_COLS schema.
    cfg, evaluation_run_id:
        As above.

    Returns
    -------
    DataFrame with PRODUCTION_INTERVAL_COLS columns.
    """
    if production_df.empty:
        return pd.DataFrame(columns=PRODUCTION_INTERVAL_COLS)

    group_keys = [
        "report_id", "selected_model_family", "selected_model_name", "selected_m",
    ]
    missing = [k for k in group_keys if k not in production_df.columns]
    if missing:
        _log.warning("Production interval: missing group columns %s", missing)
        return pd.DataFrame(columns=PRODUCTION_INTERVAL_COLS)

    rows: list[dict] = []

    for group_vals, grp in production_df.groupby(group_keys, dropna=False):
        g = dict(zip(group_keys, group_vals if isinstance(group_vals, tuple) else (group_vals,)))
        first_row = grp.iloc[0]

        try:
            orig_count = len(grp)
            deduped_grp, excluded_overlap = _deduplicate_production(grp)
            deduped_count = len(deduped_grp)

            # Normalisation scale from all-records actuals
            a_all = pd.to_numeric(grp.get("actual", pd.Series([], dtype=float)), errors="coerce").dropna().values
            fc_all = pd.to_numeric(grp.get("forecast", pd.Series([], dtype=float)), errors="coerce").dropna().values
            ns, nm, nst = _compute_norm_scale(a_all, fc_all)

            # All-record calibration
            cal_all = calculate_interval_calibration_metrics(grp, cfg, ns, nm, nst, date_col="forecast_date")

            # Deduplicated calibration (for comparison labels)
            a_ded = pd.to_numeric(deduped_grp.get("actual", pd.Series([], dtype=float)), errors="coerce").dropna().values
            lo_ded = pd.to_numeric(deduped_grp.get("lower_bound", pd.Series([], dtype=float)), errors="coerce").values
            hi_ded = pd.to_numeric(deduped_grp.get("upper_bound", pd.Series([], dtype=float)), errors="coerce").values
            nc_ded = pd.to_numeric(deduped_grp.get("nominal_coverage", pd.Series([], dtype=float)), errors="coerce").values
            valid_ded = np.isfinite(a_ded) & np.isfinite(lo_ded) & np.isfinite(hi_ded) & (lo_ded <= hi_ded)
            ded_cov: float | None = None
            ded_winkler: float | None = None
            if valid_ded.sum() >= cfg.MIN_INTERVAL_OBSERVATIONS:
                va_d = a_ded[valid_ded]
                vlo_d = lo_ded[valid_ded]
                vhi_d = hi_ded[valid_ded]
                vnc_d = nc_ded[valid_ded]
                ded_cov = float(np.mean((va_d >= vlo_d) & (va_d <= vhi_d)))
                ws_d = []
                for i in range(len(va_d)):
                    nc_i = float(vnc_d[i]) if np.isfinite(vnc_d[i]) else None
                    if nc_i and 0 < nc_i < 1:
                        ws = calculate_winkler_score(float(va_d[i]), float(vlo_d[i]), float(vhi_d[i]), 1.0 - nc_i)
                        if ws is not None:
                            ws_d.append(ws / ns if ns else ws)
                ded_winkler = float(np.mean(ws_d)) if ws_d else None

            # Horizon
            horiz = calculate_horizon_interval_metrics(grp, cfg, ns)

            cal_status, use_status, reasons = classify_interval_calibration(
                {**cal_all, **horiz}, cfg
            )
            reasons.append(
                "The production summary evaluates all forecasts issued; "
                "the deduplicated date view retains the shortest-horizon "
                "forecast for each actual date."
            )

            row = {
                "evaluation_run_id": evaluation_run_id,
                "report_id": g["report_id"],
                "report_name": first_row.get("report_name"),
                "selected_model_family": g["selected_model_family"],
                "selected_model_name": g["selected_model_name"],
                "selected_m": g["selected_m"],
                "lineage_complete": first_row.get("lineage_complete"),
                "nominal_coverage": cal_all.get("nominal_coverage"),
                "original_prediction_count": orig_count,
                "deduplicated_date_count": deduped_count,
                "excluded_overlap_count": excluded_overlap,
                **cal_all,
                **horiz,
                "all_record_observed_coverage": cal_all.get("observed_coverage"),
                "all_record_mean_winkler_score": cal_all.get("mean_winkler_score"),
                "deduplicated_observed_coverage": ded_cov,
                "deduplicated_mean_winkler_score": ded_winkler,
                "calibration_status": cal_status,
                "interval_usefulness_status": use_status,
                "calibration_reasons": "; ".join(reasons),
                "calibration_evidence_status": cal_all.get("evidence_status", "insufficient"),
            }
        except Exception as exc:
            _log.warning("Production interval failed for %s: %s", g, exc)
            row = {
                "evaluation_run_id": evaluation_run_id,
                "report_id": g.get("report_id"),
                "calibration_status": "calculation_failed",
                "calibration_reasons": f"Calculation failed: {exc}",
                "calibration_evidence_status": "calculation_failed",
            }
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=PRODUCTION_INTERVAL_COLS)

    result = pd.DataFrame(rows)
    for col in PRODUCTION_INTERVAL_COLS:
        if col not in result.columns:
            result[col] = None
    return result[PRODUCTION_INTERVAL_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

_VALID_CAL_STATUSES = frozenset({
    "well_calibrated", "slight_undercoverage", "undercoverage",
    "severe_undercoverage", "overcoverage", "insufficient_evidence",
    "calculation_failed",
})
_VALID_USE_STATUSES = frozenset({
    "useful", "wide_but_usable", "overwide", "poor",
    "insufficient_evidence", "unavailable",
})


def validate_interval_calibration_diagnostics(
    df: pd.DataFrame,
    dataset_name: str,
) -> None:
    """Validate schema and value constraints.  Raises ValueError on failure."""
    schema_map = {
        "backtest_fold": BACKTEST_FOLD_INTERVAL_COLS,
        "backtest_summary": BACKTEST_SUMMARY_INTERVAL_COLS,
        "production": PRODUCTION_INTERVAL_COLS,
    }
    expected = schema_map.get(dataset_name)
    if expected is None:
        raise ValueError(f"Unknown dataset_name: {dataset_name!r}")

    if df.empty:
        return

    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(
            f"interval_calibration[{dataset_name}]: missing columns {missing}"
        )

    if "calibration_status" in df.columns:
        bad = df["calibration_status"].dropna()
        inv = bad[~bad.isin(_VALID_CAL_STATUSES)]
        if not inv.empty:
            raise ValueError(
                f"interval_calibration[{dataset_name}]: invalid calibration_status "
                f"values {inv.unique().tolist()}"
            )

    if "interval_usefulness_status" in df.columns:
        bad = df["interval_usefulness_status"].dropna()
        inv = bad[~bad.isin(_VALID_USE_STATUSES)]
        if not inv.empty:
            raise ValueError(
                f"interval_calibration[{dataset_name}]: invalid interval_usefulness_status "
                f"values {inv.unique().tolist()}"
            )

    # Coverage rates in [0, 1]
    for col in ("observed_coverage", "nominal_coverage",
                "lower_miss_rate", "upper_miss_rate",
                "interval_availability_rate"):
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            if ((vals < 0) | (vals > 1)).any():
                raise ValueError(
                    f"interval_calibration[{dataset_name}]: {col} has values outside [0,1]"
                )

    # Winkler scores non-negative
    for col in ("mean_winkler_score", "median_winkler_score", "p90_winkler_score"):
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            if (vals < 0).any():
                raise ValueError(
                    f"interval_calibration[{dataset_name}]: {col} has negative values"
                )

    # Interval widths non-negative
    for col in ("mean_interval_width", "median_interval_width", "min_interval_width"):
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            if (vals < 0).any():
                raise ValueError(
                    f"interval_calibration[{dataset_name}]: {col} has negative values"
                )

    # insufficient_evidence may not be well_calibrated
    if "calibration_evidence_status" in df.columns and "calibration_status" in df.columns:
        bad_mask = (
            (df["calibration_evidence_status"] == "insufficient")
            & (df["calibration_status"] == "well_calibrated")
        )
        if bad_mask.any():
            raise ValueError(
                f"interval_calibration[{dataset_name}]: insufficient_evidence rows "
                "have calibration_status='well_calibrated'"
            )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

_DIAG_DIR = Path("outputs") / "diagnostics"


def persist_interval_calibration_diagnostics(
    backtest_fold_df: pd.DataFrame,
    backtest_summary_df: pd.DataFrame,
    production_df: pd.DataFrame,
    project_root: Path,
) -> dict[str, Path | None]:
    """Validate and write all three interval calibration files.

    Files are written to ``<project_root>/outputs/diagnostics/``.
    Existing files are overwritten.  Empty DataFrames write header-only CSV.

    Returns
    -------
    Dict mapping dataset name → absolute Path or None on failure.
    """
    diag_dir = project_root / _DIAG_DIR
    diag_dir.mkdir(parents=True, exist_ok=True)

    datasets = {
        "backtest_fold": (
            backtest_fold_df,
            "backtest_interval_calibration_by_fold_latest.csv",
            BACKTEST_FOLD_INTERVAL_COLS,
        ),
        "backtest_summary": (
            backtest_summary_df,
            "backtest_interval_calibration_summary_latest.csv",
            BACKTEST_SUMMARY_INTERVAL_COLS,
        ),
        "production": (
            production_df,
            "production_interval_calibration_latest.csv",
            PRODUCTION_INTERVAL_COLS,
        ),
    }

    paths: dict[str, Path | None] = {}
    for name, (df, filename, cols) in datasets.items():
        path = diag_dir / filename
        try:
            validate_interval_calibration_diagnostics(df, name)
            if df.empty:
                pd.DataFrame(columns=cols).to_csv(path, index=False)
            else:
                df.to_csv(path, index=False)
            paths[name] = path
        except Exception as exc:
            _log.error("Failed to persist interval_calibration[%s]: %s", name, exc)
            paths[name] = None

    return paths
