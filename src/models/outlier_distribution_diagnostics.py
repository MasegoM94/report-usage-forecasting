"""Outlier and distribution diagnostics for forecast residuals.

Determines whether a forecasting model has:
1. An unusually high rate of large outlier residuals.
2. Residuals that deviate substantially from normality.
3. Tail asymmetry suggesting directional extreme-event bias.

Three separate evidence sources are analysed independently:
- Training residuals  (in-sample, may be optimistic)
- Backtest forecast errors  (out-of-sample rolling-origin)
- Production forecast errors  (realized operational evidence)

Residual sign convention
------------------------
All three source datasets use the diagnostic residual convention::

    residual = actual - forecast_or_fitted

Positive residual  Rightarrow model UNDERFORECASTED.
Negative residual  Rightarrow model OVERFORECASTED.

Robust scale (MAD)
------------------
Outlier detection uses a robust z-score based on the median absolute deviation::

    residual_mad = median(|residuals - median(residuals)|)
    scaled_mad   = 1.4826 * residual_mad
    robust_z     = (residual - residual_median) / scaled_mad

When scaled_mad == 0, a fallback hierarchy is applied:
    1. IQR  (iqr/1.3490)
    2. std
    3. exact-deviation rule  (any nonzero deviation from median = outlier)

Public API
----------
calculate_robust_outlier_metrics(residuals, actuals, ...) -> dict
calculate_distribution_metrics(residuals, cfg) -> dict
classify_outlier_status(metrics, cfg) -> tuple[str, list[str]]
classify_distribution_status(metrics, cfg) -> tuple[str, list[str]]
build_training_outlier_distribution_diagnostics(df, cfg, run_id) -> pd.DataFrame
build_backtest_outlier_distribution_by_fold(df, cfg, run_id) -> pd.DataFrame
build_backtest_outlier_distribution_summary(fold_df, cfg, run_id) -> pd.DataFrame
build_production_outlier_distribution_diagnostics(df, cfg, run_id) -> pd.DataFrame
validate_outlier_distribution_diagnostics(df, dataset_name) -> None
persist_outlier_distribution_diagnostics(..., project_root) -> dict[str, Path|None]
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional dependencies
# ---------------------------------------------------------------------------

try:
    from scipy.stats import jarque_bera as _scipy_jb, shapiro as _scipy_shapiro
    from scipy.stats import kurtosis as _scipy_kurtosis, skew as _scipy_skew
    _SCIPY_AVAILABLE = True
except ImportError:
    _SCIPY_AVAILABLE = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class OutlierDistributionConfig:
    """Thresholds for outlier and distribution classification."""

    MIN_RESIDUALS_FOR_OUTLIER_DIAGNOSTICS: int = 7
    MIN_RESIDUALS_FOR_DISTRIBUTION_DIAGNOSTICS: int = 10
    ROBUST_Z_OUTLIER_THRESHOLD: float = 3.5
    OUTLIER_RATE_WARNING_THRESHOLD: float = 0.05
    OUTLIER_RATE_POOR_THRESHOLD: float = 0.15
    LARGE_MISS_WARNING_SCALE: float = 3.0
    LARGE_MISS_POOR_SCALE: float = 5.0
    TAIL_LOWER_QUANTILE: float = 0.10
    TAIL_UPPER_QUANTILE: float = 0.90
    TAIL_ASYMMETRY_WARNING_THRESHOLD: float = 0.30
    SKEWNESS_WARNING_THRESHOLD: float = 0.5
    SKEWNESS_POOR_THRESHOLD: float = 1.5
    EXCESS_KURTOSIS_WARNING_THRESHOLD: float = 1.0
    EXCESS_KURTOSIS_POOR_THRESHOLD: float = 3.0
    NORMALITY_ALPHA: float = 0.05
    SHAPIRO_MIN_SAMPLE_SIZE: int = 8
    SHAPIRO_MAX_SAMPLE_SIZE: int = 5000
    MIN_VALID_FOLDS_FOR_DISTRIBUTION_SUMMARY: int = 2


_DEFAULT_CFG = OutlierDistributionConfig()

_DIAG_DIR = "outputs/diagnostics"

# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------

_EVIDENCE_COLS = [
    "residual_source",
    "residual_count",
    "valid_residual_count",
    "excluded_invalid_count",
    "first_residual_date",
    "last_residual_date",
    "evidence_status",
]

_ROBUST_SCALE_COLS = [
    "residual_median",
    "residual_mad",
    "scaled_mad",
    "robust_residual_scale",
    "residual_iqr",
    "q1_residual",
    "q3_residual",
    "scale_method_used",
    "scale_fallback_used",
    "scale_status",
]

_OUTLIER_METRIC_COLS = [
    "outlier_count",
    "outlier_rate",
    "positive_outlier_count",
    "negative_outlier_count",
    "positive_outlier_rate",
    "negative_outlier_rate",
    "largest_positive_residual",
    "largest_negative_residual",
    "largest_absolute_residual",
    "largest_positive_residual_date",
    "largest_negative_residual_date",
    "largest_absolute_residual_date",
    "largest_absolute_residual_horizon_step",
    "largest_absolute_robust_z",
    "mean_absolute_outlier_residual",
    "median_absolute_outlier_residual",
    "outlier_status",
    "outlier_reasons",
    "outlier_evidence_status",
]

_TAIL_COLS = [
    "lower_tail_quantile",
    "upper_tail_quantile",
    "lower_tail_mean",
    "upper_tail_mean",
    "lower_tail_count",
    "upper_tail_count",
    "tail_asymmetry",
    "tail_direction",
    "extreme_underforecast_share",
    "extreme_overforecast_share",
]

_DISTRIBUTION_COLS = [
    "residual_mean",
    "residual_std",
    "residual_skewness",
    "residual_kurtosis",
    "excess_kurtosis",
    "jarque_bera_statistic",
    "jarque_bera_pvalue",
    "jarque_bera_significant",
    "shapiro_statistic",
    "shapiro_pvalue",
    "shapiro_significant",
    "distribution_shape",
    "normality_status",
    "distribution_reasons",
    "distribution_evidence_status",
]

TRAINING_OUTLIER_COLS: list[str] = (
    [
        "diagnostic_run_id",
        "report_id",
        "report_name",
        "model_family",
        "model_name",
        "candidate_m",
        "fit_scope",
        "fold_number",
        "training_start",
        "training_cutoff",
    ]
    + _EVIDENCE_COLS
    + _ROBUST_SCALE_COLS
    + _OUTLIER_METRIC_COLS
    + _TAIL_COLS
    + _DISTRIBUTION_COLS
)

BACKTEST_FOLD_OUTLIER_COLS: list[str] = (
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
    + _ROBUST_SCALE_COLS
    + _OUTLIER_METRIC_COLS
    + _TAIL_COLS
    + _DISTRIBUTION_COLS
)

BACKTEST_SUMMARY_OUTLIER_COLS: list[str] = [
    "evaluation_run_id",
    "report_id",
    "report_name",
    "model_family",
    "model_name",
    "candidate_m",
    "total_fold_count",
    "valid_fold_count",
    "acceptable_outlier_fold_count",
    "warning_outlier_fold_count",
    "poor_outlier_fold_count",
    "high_outlier_rate_fold_count",
    "positive_tail_fold_count",
    "negative_tail_fold_count",
    "median_outlier_rate",
    "mean_outlier_rate",
    "max_fold_outlier_rate",
    "median_abs_largest_residual",
    "max_absolute_residual_across_folds",
    "positively_skewed_fold_count",
    "negatively_skewed_fold_count",
    "heavy_tailed_fold_count",
    "jarque_bera_significant_fold_count",
    "practical_distribution_issue_fold_count",
    "cross_fold_outlier_status",
    "cross_fold_distribution_status",
    "cross_fold_reasons",
]

PRODUCTION_OUTLIER_COLS: list[str] = (
    [
        "evaluation_run_id",
        "report_id",
        "report_name",
        "selected_model_family",
        "selected_model_name",
        "selected_m",
        "lineage_complete",
    ]
    + _EVIDENCE_COLS
    + [
        "original_prediction_count",
        "deduplicated_date_count",
        "excluded_overlap_count",
    ]
    + _ROBUST_SCALE_COLS
    + _OUTLIER_METRIC_COLS
    + _TAIL_COLS
    + _DISTRIBUTION_COLS
)

# ---------------------------------------------------------------------------
# Valid status values
# ---------------------------------------------------------------------------

_VALID_OUTLIER_STATUSES = frozenset({
    "acceptable", "warning", "poor", "insufficient_evidence", "calculation_failed",
})

_VALID_NORMALITY_STATUSES = frozenset({
    "no_material_concern", "caution", "poor_for_analytic_intervals",
    "insufficient_evidence", "calculation_failed",
})

_VALID_TAIL_DIRECTIONS = frozenset({
    "underforecast_heavy", "overforecast_heavy", "approximately_balanced",
    "insufficient_evidence",
})

_VALID_DISTRIBUTION_SHAPES = frozenset({
    "approximately_symmetric", "positively_skewed", "negatively_skewed",
    "heavy_tailed", "light_tailed", "mixed_non_normal", "constant",
    "insufficient_evidence",
})

# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def _filter_valid(
    df: pd.DataFrame,
    residual_col: str = "residual",
    actual_col: str = "actual",
    valid_col: str = "residual_observation_valid",
) -> tuple[np.ndarray, np.ndarray, int, int]:
    """Return (residuals, actuals, excluded_invalid, total_count)."""
    total = len(df)
    valid_mask = (
        df[valid_col].fillna(False).astype(bool)
        if valid_col in df.columns
        else pd.Series(True, index=df.index)
    )
    sub = df[valid_mask]
    r = np.asarray(
        sub[residual_col] if residual_col in sub.columns else [],
        dtype=float,
    )
    a = np.asarray(
        sub[actual_col] if actual_col in sub.columns else np.zeros(len(sub)),
        dtype=float,
    )
    finite_mask = np.isfinite(r)
    excluded = int((~finite_mask).sum())
    return r[finite_mask], a[finite_mask], excluded, total


def _filter_valid_with_meta(
    df: pd.DataFrame,
    residual_col: str = "residual",
    actual_col: str = "actual",
    valid_col: str = "residual_observation_valid",
    date_col: Optional[str] = None,
    horizon_col: Optional[str] = None,
) -> tuple[np.ndarray, np.ndarray, Optional[np.ndarray], Optional[np.ndarray], int, int]:
    """Like _filter_valid but also returns dates and horizon arrays."""
    total = len(df)
    valid_mask = (
        df[valid_col].fillna(False).astype(bool)
        if valid_col in df.columns
        else pd.Series(True, index=df.index)
    )
    sub = df[valid_mask]
    r = np.asarray(
        sub[residual_col] if residual_col in sub.columns else [],
        dtype=float,
    )
    a = np.asarray(
        sub[actual_col] if actual_col in sub.columns else np.zeros(len(sub)),
        dtype=float,
    )
    finite_mask = np.isfinite(r)
    excluded = int((~finite_mask).sum())

    dates = None
    if date_col and date_col in sub.columns:
        dates = sub[date_col].values[finite_mask]

    horizons = None
    if horizon_col and horizon_col in sub.columns:
        horizons = np.asarray(sub[horizon_col].values[finite_mask])

    return r[finite_mask], a[finite_mask], dates, horizons, excluded, total


def _evidence_block(
    df: pd.DataFrame,
    residuals: np.ndarray,
    total_count: int,
    excluded_invalid: int,
    source: str,
    date_col: str = "residual_date",
    cfg: OutlierDistributionConfig = _DEFAULT_CFG,
) -> dict:
    valid_count = len(residuals)
    first_date = None
    last_date = None
    if date_col in df.columns:
        dates = df[date_col].dropna()
        if len(dates):
            first_date = dates.min()
            last_date = dates.max()
    evidence_status = (
        "ok" if valid_count >= cfg.MIN_RESIDUALS_FOR_OUTLIER_DIAGNOSTICS else "insufficient"
    )
    return {
        "residual_source": source,
        "residual_count": total_count,
        "valid_residual_count": valid_count,
        "excluded_invalid_count": excluded_invalid,
        "first_residual_date": first_date,
        "last_residual_date": last_date,
        "evidence_status": evidence_status,
    }


# ---------------------------------------------------------------------------
# Robust scale
# ---------------------------------------------------------------------------


def _compute_robust_scale(
    residuals: np.ndarray,
    cfg: OutlierDistributionConfig = _DEFAULT_CFG,
) -> dict:
    """Compute robust scale metrics (MAD-based) with fallbacks."""
    n = len(residuals)
    if n == 0:
        return {
            "residual_median": None,
            "residual_mad": None,
            "scaled_mad": None,
            "robust_residual_scale": None,
            "residual_iqr": None,
            "q1_residual": None,
            "q3_residual": None,
            "scale_method_used": None,
            "scale_fallback_used": None,
            "scale_status": None,
        }

    med = float(np.median(residuals))
    deviations = np.abs(residuals - med)
    mad = float(np.median(deviations))
    scaled = mad * 1.4826

    q1 = float(np.percentile(residuals, 25))
    q3 = float(np.percentile(residuals, 75))
    iqr = q3 - q1

    if scaled > 0:
        scale_method = "mad"
        fallback_used = False
        scale_status = "ok"
        robust_scale = scaled
    elif iqr > 0:
        # IQR fallback: scale IQR to be comparable to std
        robust_scale = iqr / 1.3490
        scaled = robust_scale
        scale_method = "iqr"
        fallback_used = True
        scale_status = "fallback_iqr"
    else:
        std_val = float(np.std(residuals, ddof=1)) if n >= 2 else 0.0
        if std_val > 0:
            robust_scale = std_val
            scaled = std_val
            scale_method = "std"
            fallback_used = True
            scale_status = "fallback_std"
        else:
            # All residuals identical
            robust_scale = 0.0
            scaled = 0.0
            scale_method = "exact_deviation"
            fallback_used = True
            scale_status = "constant_residuals"

    return {
        "residual_median": med,
        "residual_mad": mad,
        "scaled_mad": mad * 1.4826,  # always 1.4826*mad regardless of fallback
        "robust_residual_scale": robust_scale,
        "residual_iqr": iqr,
        "q1_residual": q1,
        "q3_residual": q3,
        "scale_method_used": scale_method,
        "scale_fallback_used": fallback_used,
        "scale_status": scale_status,
    }


# ---------------------------------------------------------------------------
# Core outlier metrics
# ---------------------------------------------------------------------------


def calculate_robust_outlier_metrics(
    residuals: np.ndarray,
    actuals: np.ndarray,
    dates: Optional[np.ndarray] = None,
    horizon_steps: Optional[np.ndarray] = None,
    cfg: OutlierDistributionConfig = _DEFAULT_CFG,
) -> dict:
    """Calculate robust outlier metrics for one group."""
    n = len(residuals)
    null_scale = {k: None for k in _ROBUST_SCALE_COLS}
    null_metrics = {k: None for k in _OUTLIER_METRIC_COLS}

    if n < cfg.MIN_RESIDUALS_FOR_OUTLIER_DIAGNOSTICS:
        null_metrics["outlier_status"] = "insufficient_evidence"
        null_metrics["outlier_reasons"] = "Insufficient residuals for outlier diagnostics"
        null_metrics["outlier_evidence_status"] = "insufficient"
        return {**null_scale, **null_metrics}

    scale_info = _compute_robust_scale(residuals, cfg)
    scale_status = scale_info["scale_status"]
    robust_scale = scale_info["robust_residual_scale"]

    # Compute robust z-scores
    med = scale_info["residual_median"]

    if scale_status == "constant_residuals":
        # All residuals identical
        robust_z = np.zeros(n)
        is_outlier = np.zeros(n, dtype=bool)
    elif robust_scale is not None and robust_scale > 0:
        robust_z = (residuals - med) / robust_scale
        is_outlier = np.abs(robust_z) >= cfg.ROBUST_Z_OUTLIER_THRESHOLD
    else:
        # exact_deviation fallback
        robust_z = np.where(np.abs(residuals - med) > 0, np.inf, 0.0)
        is_outlier = np.abs(residuals - med) > 0

    outlier_count = int(np.sum(is_outlier))
    outlier_rate = outlier_count / n
    pos_out_mask = is_outlier & (residuals > 0)
    neg_out_mask = is_outlier & (residuals < 0)
    pos_outlier_count = int(np.sum(pos_out_mask))
    neg_outlier_count = int(np.sum(neg_out_mask))

    # Largest misses
    pos_residuals = residuals[residuals > 0]
    neg_residuals = residuals[residuals < 0]

    largest_pos = float(np.max(pos_residuals)) if len(pos_residuals) else None
    largest_neg = float(np.min(neg_residuals)) if len(neg_residuals) else None
    largest_abs = float(np.max(np.abs(residuals)))
    largest_abs_idx = int(np.argmax(np.abs(residuals)))

    # Dates for largest residuals
    def _get_date(idx_mask):
        if dates is None:
            return None
        idxs = np.where(idx_mask)[0]
        if len(idxs) == 0:
            return None
        return dates[idxs[np.argmax(np.abs(residuals[idxs]))]]

    largest_pos_date = None
    if largest_pos is not None and dates is not None:
        pos_idx = np.where(residuals > 0)[0]
        if len(pos_idx):
            largest_pos_date = dates[pos_idx[np.argmax(residuals[pos_idx])]]

    largest_neg_date = None
    if largest_neg is not None and dates is not None:
        neg_idx = np.where(residuals < 0)[0]
        if len(neg_idx):
            largest_neg_date = dates[neg_idx[np.argmin(residuals[neg_idx])]]

    largest_abs_date = dates[largest_abs_idx] if dates is not None else None
    largest_abs_horizon = (
        horizon_steps[largest_abs_idx] if horizon_steps is not None else None
    )
    largest_abs_robust_z = float(np.max(np.abs(robust_z[np.isfinite(robust_z)]))) if np.any(np.isfinite(robust_z)) else None

    outlier_abs_residuals = np.abs(residuals[is_outlier])
    mean_abs_out = float(np.mean(outlier_abs_residuals)) if outlier_count > 0 else None
    median_abs_out = float(np.median(outlier_abs_residuals)) if outlier_count > 0 else None

    # Status and reasons
    outlier_status, outlier_reasons = classify_outlier_status(
        {
            "valid_residual_count": n,
            "outlier_rate": outlier_rate,
            "scale_status": scale_status,
            "scale_method_used": scale_info["scale_method_used"],
            "scale_fallback_used": scale_info["scale_fallback_used"],
            "largest_positive_residual": largest_pos,
            "largest_negative_residual": largest_neg,
            "tail_direction": None,  # computed separately
        },
        cfg,
    )

    return {
        **scale_info,
        "outlier_count": outlier_count,
        "outlier_rate": outlier_rate,
        "positive_outlier_count": pos_outlier_count,
        "negative_outlier_count": neg_outlier_count,
        "positive_outlier_rate": pos_outlier_count / n,
        "negative_outlier_rate": neg_outlier_count / n,
        "largest_positive_residual": largest_pos,
        "largest_negative_residual": largest_neg,
        "largest_absolute_residual": largest_abs,
        "largest_positive_residual_date": largest_pos_date,
        "largest_negative_residual_date": largest_neg_date,
        "largest_absolute_residual_date": largest_abs_date,
        "largest_absolute_residual_horizon_step": largest_abs_horizon,
        "largest_absolute_robust_z": largest_abs_robust_z,
        "mean_absolute_outlier_residual": mean_abs_out,
        "median_absolute_outlier_residual": median_abs_out,
        "outlier_status": outlier_status,
        "outlier_reasons": "; ".join(outlier_reasons),
        "outlier_evidence_status": "ok" if n >= cfg.MIN_RESIDUALS_FOR_OUTLIER_DIAGNOSTICS else "insufficient",
    }


# ---------------------------------------------------------------------------
# Tail metrics
# ---------------------------------------------------------------------------


def _compute_tail_metrics(
    residuals: np.ndarray,
    cfg: OutlierDistributionConfig = _DEFAULT_CFG,
) -> dict:
    """Compute tail quantile and asymmetry metrics."""
    n = len(residuals)
    null = {k: None for k in _TAIL_COLS}

    if n < cfg.MIN_RESIDUALS_FOR_OUTLIER_DIAGNOSTICS:
        null["tail_direction"] = "insufficient_evidence"
        return null

    lower_q_val = float(np.percentile(residuals, cfg.TAIL_LOWER_QUANTILE * 100))
    upper_q_val = float(np.percentile(residuals, cfg.TAIL_UPPER_QUANTILE * 100))

    lower_mask = residuals <= lower_q_val
    upper_mask = residuals >= upper_q_val

    lower_count = int(np.sum(lower_mask))
    upper_count = int(np.sum(upper_mask))

    lower_mean = float(np.mean(residuals[lower_mask])) if lower_count > 0 else 0.0
    upper_mean = float(np.mean(residuals[upper_mask])) if upper_count > 0 else 0.0

    # tail_asymmetry = upper_tail_mean - abs(lower_tail_mean)
    # positive = underforecast heavy (upper tail dominates)
    tail_asymmetry = upper_mean - abs(lower_mean)

    if tail_asymmetry > cfg.TAIL_ASYMMETRY_WARNING_THRESHOLD:
        tail_direction = "underforecast_heavy"
    elif tail_asymmetry < -cfg.TAIL_ASYMMETRY_WARNING_THRESHOLD:
        tail_direction = "overforecast_heavy"
    else:
        tail_direction = "approximately_balanced"

    return {
        "lower_tail_quantile": lower_q_val,
        "upper_tail_quantile": upper_q_val,
        "lower_tail_mean": lower_mean,
        "upper_tail_mean": upper_mean,
        "lower_tail_count": lower_count,
        "upper_tail_count": upper_count,
        "tail_asymmetry": tail_asymmetry,
        "tail_direction": tail_direction,
        "extreme_underforecast_share": upper_count / n,
        "extreme_overforecast_share": lower_count / n,
    }


# ---------------------------------------------------------------------------
# Distribution metrics
# ---------------------------------------------------------------------------


def _classify_distribution_shape(
    skewness: Optional[float],
    excess_kurtosis: Optional[float],
    cfg: OutlierDistributionConfig,
) -> str:
    if skewness is None and excess_kurtosis is None:
        return "insufficient_evidence"

    shapes = []

    if skewness is not None:
        if skewness > cfg.SKEWNESS_WARNING_THRESHOLD:
            shapes.append("positively_skewed")
        elif skewness < -cfg.SKEWNESS_WARNING_THRESHOLD:
            shapes.append("negatively_skewed")
        else:
            shapes.append("approximately_symmetric")

    if excess_kurtosis is not None:
        if excess_kurtosis > cfg.EXCESS_KURTOSIS_WARNING_THRESHOLD:
            shapes.append("heavy_tailed")
        elif excess_kurtosis < -cfg.EXCESS_KURTOSIS_WARNING_THRESHOLD:
            shapes.append("light_tailed")

    if len(shapes) == 0:
        return "approximately_symmetric"

    # Check if we have conflicting shape indicators
    skew_shapes = [s for s in shapes if s in ("positively_skewed", "negatively_skewed", "approximately_symmetric")]
    kurt_shapes = [s for s in shapes if s in ("heavy_tailed", "light_tailed")]

    # If we have a skew issue AND a kurtosis issue at the same time → mixed
    if skew_shapes and kurt_shapes:
        skew_issue = skew_shapes[0] != "approximately_symmetric"
        kurt_issue = len(kurt_shapes) > 0
        if skew_issue and kurt_issue:
            return "mixed_non_normal"

    return shapes[0]


def calculate_distribution_metrics(
    residuals: np.ndarray,
    cfg: OutlierDistributionConfig = _DEFAULT_CFG,
) -> dict:
    """Calculate skewness, kurtosis, and normality test metrics."""
    n = len(residuals)
    null = {k: None for k in _DISTRIBUTION_COLS}
    null["normality_status"] = "insufficient_evidence"
    null["distribution_evidence_status"] = "insufficient"
    null["distribution_reasons"] = "Insufficient residuals for distribution diagnostics"
    null["distribution_shape"] = "insufficient_evidence"

    if n < cfg.MIN_RESIDUALS_FOR_DISTRIBUTION_DIAGNOSTICS:
        return null

    res_mean = float(np.mean(residuals))
    res_std = float(np.std(residuals, ddof=1)) if n >= 2 else None

    skewness = None
    excess_kurt = None
    raw_kurt = None

    if _SCIPY_AVAILABLE:
        try:
            skewness = float(_scipy_skew(residuals))
            excess_kurt = float(_scipy_kurtosis(residuals, fisher=True))
            raw_kurt = excess_kurt + 3.0
        except Exception:
            pass
    else:
        # Manual computation
        if n >= 3 and res_std and res_std > 0:
            z = (residuals - np.mean(residuals)) / res_std
            skewness = float(np.mean(z ** 3))
            excess_kurt = float(np.mean(z ** 4)) - 3.0
            raw_kurt = excess_kurt + 3.0

    # Jarque-Bera test
    jb_stat = None
    jb_pval = None
    jb_sig = None

    if _SCIPY_AVAILABLE and n >= 8:
        try:
            jb_result = _scipy_jb(residuals)
            jb_stat = float(jb_result.statistic)
            jb_pval = float(jb_result.pvalue)
            jb_sig = jb_pval < cfg.NORMALITY_ALPHA
        except Exception:
            pass

    # Shapiro-Wilk test
    sw_stat = None
    sw_pval = None
    sw_sig = None

    if (
        _SCIPY_AVAILABLE
        and cfg.SHAPIRO_MIN_SAMPLE_SIZE <= n <= cfg.SHAPIRO_MAX_SAMPLE_SIZE
    ):
        try:
            sw_result = _scipy_shapiro(residuals)
            sw_stat = float(sw_result.statistic)
            sw_pval = float(sw_result.pvalue)
            sw_sig = sw_pval < cfg.NORMALITY_ALPHA
        except Exception:
            pass

    dist_shape = _classify_distribution_shape(skewness, excess_kurt, cfg)

    normality_status, dist_reasons = classify_distribution_status(
        {
            "valid_residual_count": n,
            "residual_skewness": skewness,
            "excess_kurtosis": excess_kurt,
            "jarque_bera_significant": jb_sig,
            "shapiro_significant": sw_sig,
            "distribution_shape": dist_shape,
        },
        cfg,
    )

    return {
        "residual_mean": res_mean,
        "residual_std": res_std,
        "residual_skewness": skewness,
        "residual_kurtosis": raw_kurt,
        "excess_kurtosis": excess_kurt,
        "jarque_bera_statistic": jb_stat,
        "jarque_bera_pvalue": jb_pval,
        "jarque_bera_significant": jb_sig,
        "shapiro_statistic": sw_stat,
        "shapiro_pvalue": sw_pval,
        "shapiro_significant": sw_sig,
        "distribution_shape": dist_shape,
        "normality_status": normality_status,
        "distribution_reasons": "; ".join(dist_reasons),
        "distribution_evidence_status": "ok",
    }


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def classify_outlier_status(
    metrics: dict,
    cfg: OutlierDistributionConfig = _DEFAULT_CFG,
) -> tuple[str, list[str]]:
    """Classify outlier status and return (status, reasons)."""
    reasons: list[str] = []
    n = metrics.get("valid_residual_count", 0) or 0

    if n < cfg.MIN_RESIDUALS_FOR_OUTLIER_DIAGNOSTICS:
        reasons.append("Insufficient residuals for outlier diagnostics")
        return "insufficient_evidence", reasons

    scale_status = metrics.get("scale_status")
    scale_fallback = metrics.get("scale_fallback_used")

    if scale_fallback:
        reasons.append(f"Scale fallback used: {scale_status}")

    outlier_rate = metrics.get("outlier_rate")
    status = "acceptable"

    if outlier_rate is not None:
        if outlier_rate >= cfg.OUTLIER_RATE_POOR_THRESHOLD:
            status = "poor"
            reasons.append(
                f"High outlier rate: {outlier_rate:.1%} >= {cfg.OUTLIER_RATE_POOR_THRESHOLD:.1%}"
            )
        elif outlier_rate >= cfg.OUTLIER_RATE_WARNING_THRESHOLD:
            if status == "acceptable":
                status = "warning"
            reasons.append(
                f"Elevated outlier rate: {outlier_rate:.1%} >= {cfg.OUTLIER_RATE_WARNING_THRESHOLD:.1%}"
            )

    return status, reasons


def classify_distribution_status(
    metrics: dict,
    cfg: OutlierDistributionConfig = _DEFAULT_CFG,
) -> tuple[str, list[str]]:
    """Classify normality/distribution status and return (status, reasons)."""
    reasons: list[str] = []
    n = metrics.get("valid_residual_count", 0) or 0

    if n < cfg.MIN_RESIDUALS_FOR_DISTRIBUTION_DIAGNOSTICS:
        reasons.append("Insufficient residuals for distribution diagnostics")
        return "insufficient_evidence", reasons

    status = "no_material_concern"
    skewness = metrics.get("residual_skewness")
    excess_kurt = metrics.get("excess_kurtosis")
    jb_sig = metrics.get("jarque_bera_significant")
    sw_sig = metrics.get("shapiro_significant")
    dist_shape = metrics.get("distribution_shape")

    if skewness is not None:
        abs_skew = abs(skewness)
        if abs_skew >= cfg.SKEWNESS_POOR_THRESHOLD:
            if status in ("no_material_concern", "caution"):
                status = "poor_for_analytic_intervals"
            reasons.append(
                f"High skewness: {skewness:.2f} (threshold {cfg.SKEWNESS_POOR_THRESHOLD})"
            )
        elif abs_skew >= cfg.SKEWNESS_WARNING_THRESHOLD:
            if status == "no_material_concern":
                status = "caution"
            reasons.append(
                f"Moderate skewness: {skewness:.2f} (threshold {cfg.SKEWNESS_WARNING_THRESHOLD})"
            )

    if excess_kurt is not None:
        if excess_kurt >= cfg.EXCESS_KURTOSIS_POOR_THRESHOLD:
            if status in ("no_material_concern", "caution"):
                status = "poor_for_analytic_intervals"
            reasons.append(
                f"Heavy tails: excess kurtosis {excess_kurt:.2f} >= {cfg.EXCESS_KURTOSIS_POOR_THRESHOLD}"
            )
        elif excess_kurt >= cfg.EXCESS_KURTOSIS_WARNING_THRESHOLD:
            if status == "no_material_concern":
                status = "caution"
            reasons.append(
                f"Elevated kurtosis: excess kurtosis {excess_kurt:.2f} >= {cfg.EXCESS_KURTOSIS_WARNING_THRESHOLD}"
            )

    if jb_sig:
        if status == "no_material_concern":
            status = "caution"
        reasons.append("Jarque-Bera test significant: residuals not normally distributed")

    if sw_sig:
        if status == "no_material_concern":
            status = "caution"
        reasons.append("Shapiro-Wilk test significant: residuals not normally distributed")

    return status, reasons


# ---------------------------------------------------------------------------
# Production dedup helper
# ---------------------------------------------------------------------------


def _deduplicate_production(grp: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Deduplicate production data: keep shortest horizon_step per (report_id, forecast_date).
    Tie-break: latest generated_at."""
    if "forecast_date" not in grp.columns or "horizon_step" not in grp.columns:
        return grp, 0

    original_count = len(grp)
    sort_cols = ["horizon_step"]
    ascending = [True]
    if "generated_at" in grp.columns:
        sort_cols.append("generated_at")
        ascending.append(False)

    deduped = (
        grp.sort_values(sort_cols, ascending=ascending)
        .groupby(["forecast_date"], dropna=False)
        .first()
        .reset_index()
    )
    excluded_overlap = original_count - len(deduped)
    return deduped, excluded_overlap


# ---------------------------------------------------------------------------
# Build training diagnostics
# ---------------------------------------------------------------------------


def build_training_outlier_distribution_diagnostics(
    training_df: pd.DataFrame,
    cfg: OutlierDistributionConfig = _DEFAULT_CFG,
    diagnostic_run_id: str = "",
) -> pd.DataFrame:
    """Build training outlier and distribution diagnostics.

    Groups by (report_id, model_family, model_name, candidate_m, fit_scope, fold_number).
    """
    if training_df.empty:
        return pd.DataFrame(columns=TRAINING_OUTLIER_COLS)

    group_keys = [
        "report_id", "model_family", "model_name", "candidate_m",
        "fit_scope", "fold_number",
    ]

    rows: list[dict] = []
    for group_vals, grp in training_df.groupby(group_keys, dropna=False):
        g = dict(
            zip(
                group_keys,
                group_vals if isinstance(group_vals, tuple) else (group_vals,),
            )
        )
        try:
            has_horizon = "horizon_step" in grp.columns
            r, a, dates, horizons, excl, total = _filter_valid_with_meta(
                grp,
                date_col="residual_date",
                horizon_col="horizon_step" if has_horizon else None,
            )
            first_row = grp.iloc[0]
            ev = _evidence_block(
                grp, r, total, excl, "training",
                date_col="residual_date", cfg=cfg,
            )
            outlier_m = calculate_robust_outlier_metrics(r, a, dates, horizons, cfg)
            tail_m = _compute_tail_metrics(r, cfg)
            dist_m = calculate_distribution_metrics(r, cfg)

            row = {
                "diagnostic_run_id": diagnostic_run_id,
                "report_id": g["report_id"],
                "report_name": first_row.get("report_name"),
                "model_family": g["model_family"],
                "model_name": g["model_name"],
                "candidate_m": g["candidate_m"],
                "fit_scope": g["fit_scope"],
                "fold_number": g["fold_number"],
                "training_start": first_row.get("training_start"),
                "training_cutoff": first_row.get("training_cutoff"),
                **ev,
                **outlier_m,
                **tail_m,
                **dist_m,
            }
        except Exception as exc:
            _log.warning("Training outlier diagnostics failed for group %s: %s", g, exc)
            row = {
                "diagnostic_run_id": diagnostic_run_id,
                "report_id": g.get("report_id"),
                "outlier_status": "calculation_failed",
                "outlier_reasons": f"Calculation failed: {exc}",
                "normality_status": "calculation_failed",
                "distribution_reasons": f"Calculation failed: {exc}",
            }
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=TRAINING_OUTLIER_COLS)

    result = pd.DataFrame(rows)
    for col in TRAINING_OUTLIER_COLS:
        if col not in result.columns:
            result[col] = None
    return result[TRAINING_OUTLIER_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Build backtest fold diagnostics
# ---------------------------------------------------------------------------


def build_backtest_outlier_distribution_by_fold(
    backtest_df: pd.DataFrame,
    cfg: OutlierDistributionConfig = _DEFAULT_CFG,
    evaluation_run_id: str = "",
) -> pd.DataFrame:
    """Build backtest outlier and distribution diagnostics per fold."""
    if backtest_df.empty:
        return pd.DataFrame(columns=BACKTEST_FOLD_OUTLIER_COLS)

    group_keys = [
        "report_id", "model_family", "model_name", "candidate_m", "fold_number",
    ]

    rows: list[dict] = []
    for group_vals, grp in backtest_df.groupby(group_keys, dropna=False):
        g = dict(
            zip(
                group_keys,
                group_vals if isinstance(group_vals, tuple) else (group_vals,),
            )
        )
        try:
            has_horizon = "horizon_step" in grp.columns
            date_col = "residual_date" if "residual_date" in grp.columns else None
            r, a, dates, horizons, excl, total = _filter_valid_with_meta(
                grp,
                date_col=date_col,
                horizon_col="horizon_step" if has_horizon else None,
            )
            first_row = grp.iloc[0]
            ev = _evidence_block(
                grp, r, total, excl, "backtest",
                date_col=date_col or "residual_date", cfg=cfg,
            )
            outlier_m = calculate_robust_outlier_metrics(r, a, dates, horizons, cfg)
            tail_m = _compute_tail_metrics(r, cfg)
            dist_m = calculate_distribution_metrics(r, cfg)

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
                **ev,
                **outlier_m,
                **tail_m,
                **dist_m,
            }
        except Exception as exc:
            _log.warning("Backtest outlier diagnostics failed for group %s: %s", g, exc)
            row = {
                "evaluation_run_id": evaluation_run_id,
                "report_id": g.get("report_id"),
                "outlier_status": "calculation_failed",
                "outlier_reasons": f"Calculation failed: {exc}",
                "normality_status": "calculation_failed",
                "distribution_reasons": f"Calculation failed: {exc}",
            }
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=BACKTEST_FOLD_OUTLIER_COLS)

    result = pd.DataFrame(rows)
    for col in BACKTEST_FOLD_OUTLIER_COLS:
        if col not in result.columns:
            result[col] = None
    return result[BACKTEST_FOLD_OUTLIER_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Build backtest summary diagnostics
# ---------------------------------------------------------------------------


def build_backtest_outlier_distribution_summary(
    fold_df: pd.DataFrame,
    cfg: OutlierDistributionConfig = _DEFAULT_CFG,
    evaluation_run_id: str = "",
) -> pd.DataFrame:
    """Cross-fold summary of outlier and distribution diagnostics."""
    if fold_df.empty:
        return pd.DataFrame(columns=BACKTEST_SUMMARY_OUTLIER_COLS)

    group_keys = ["report_id", "model_family", "model_name", "candidate_m"]

    rows: list[dict] = []
    for group_vals, grp in fold_df.groupby(group_keys, dropna=False):
        g = dict(
            zip(
                group_keys,
                group_vals if isinstance(group_vals, tuple) else (group_vals,),
            )
        )
        first_row = grp.iloc[0]
        total_folds = len(grp)

        # Valid folds are those with calculable outlier metrics
        valid_mask = grp["outlier_status"].notna() & ~grp["outlier_status"].isin(
            {"calculation_failed", "insufficient_evidence"}
        )
        valid_grp = grp[valid_mask]
        valid_fold_count = int(valid_mask.sum())

        # Fold status counts
        def _count(col, val):
            if col not in grp.columns:
                return 0
            return int((grp[col] == val).sum())

        acc_count = _count("outlier_status", "acceptable")
        warn_count = _count("outlier_status", "warning")
        poor_count = _count("outlier_status", "poor")

        # High outlier rate folds
        high_rate_count = 0
        if "outlier_rate" in grp.columns:
            high_rate_count = int(
                (grp["outlier_rate"].fillna(0) >= cfg.OUTLIER_RATE_WARNING_THRESHOLD).sum()
            )

        positive_tail_count = _count("tail_direction", "underforecast_heavy")
        negative_tail_count = _count("tail_direction", "overforecast_heavy")

        # Aggregate outlier rates
        outlier_rates = grp["outlier_rate"].dropna().values if "outlier_rate" in grp.columns else np.array([])
        median_outlier_rate = float(np.median(outlier_rates)) if len(outlier_rates) else None
        mean_outlier_rate = float(np.mean(outlier_rates)) if len(outlier_rates) else None
        max_outlier_rate = float(np.max(outlier_rates)) if len(outlier_rates) else None

        # Largest residuals across folds
        abs_largest = grp["largest_absolute_residual"].dropna().values if "largest_absolute_residual" in grp.columns else np.array([])
        median_abs_largest = float(np.median(abs_largest)) if len(abs_largest) else None
        max_abs_residual = float(np.max(abs_largest)) if len(abs_largest) else None

        # Distribution shape counts
        pos_skew_count = _count("distribution_shape", "positively_skewed")
        neg_skew_count = _count("distribution_shape", "negatively_skewed")
        heavy_tailed_count = _count("distribution_shape", "heavy_tailed")

        jb_sig_count = 0
        if "jarque_bera_significant" in grp.columns:
            jb_sig_count = int(grp["jarque_bera_significant"].fillna(False).sum())

        practical_dist_count = int(
            grp["normality_status"].isin({"poor_for_analytic_intervals"}).sum()
            if "normality_status" in grp.columns
            else 0
        )

        # Cross-fold statuses
        if valid_fold_count == 0:
            cross_outlier_status = "insufficient_evidence"
            cross_dist_status = "insufficient_evidence"
        else:
            if poor_count > 0:
                cross_outlier_status = "poor"
            elif warn_count > 0:
                cross_outlier_status = "warning"
            else:
                cross_outlier_status = "acceptable"

            if practical_dist_count > 0:
                cross_dist_status = "poor_for_analytic_intervals"
            elif heavy_tailed_count > 0 or jb_sig_count > 0:
                cross_dist_status = "caution"
            else:
                cross_dist_status = "no_material_concern"

        # Cross-fold reasons
        cross_reasons = []
        if valid_fold_count < cfg.MIN_VALID_FOLDS_FOR_DISTRIBUTION_SUMMARY:
            cross_reasons.append(
                f"Few valid folds: {valid_fold_count} of {total_folds}"
            )
        if poor_count > 0:
            cross_reasons.append(f"{poor_count} fold(s) with poor outlier status")
        if warn_count > 0:
            cross_reasons.append(f"{warn_count} fold(s) with warning outlier status")
        if high_rate_count > 0:
            cross_reasons.append(f"{high_rate_count} fold(s) with high outlier rate")
        if practical_dist_count > 0:
            cross_reasons.append(f"{practical_dist_count} fold(s) with poor distribution")
        if jb_sig_count > 0:
            cross_reasons.append(f"{jb_sig_count} fold(s) with significant Jarque-Bera test")

        row = {
            "evaluation_run_id": evaluation_run_id,
            "report_id": g["report_id"],
            "report_name": first_row.get("report_name"),
            "model_family": g["model_family"],
            "model_name": g["model_name"],
            "candidate_m": g["candidate_m"],
            "total_fold_count": total_folds,
            "valid_fold_count": valid_fold_count,
            "acceptable_outlier_fold_count": acc_count,
            "warning_outlier_fold_count": warn_count,
            "poor_outlier_fold_count": poor_count,
            "high_outlier_rate_fold_count": high_rate_count,
            "positive_tail_fold_count": positive_tail_count,
            "negative_tail_fold_count": negative_tail_count,
            "median_outlier_rate": median_outlier_rate,
            "mean_outlier_rate": mean_outlier_rate,
            "max_fold_outlier_rate": max_outlier_rate,
            "median_abs_largest_residual": median_abs_largest,
            "max_absolute_residual_across_folds": max_abs_residual,
            "positively_skewed_fold_count": pos_skew_count,
            "negatively_skewed_fold_count": neg_skew_count,
            "heavy_tailed_fold_count": heavy_tailed_count,
            "jarque_bera_significant_fold_count": jb_sig_count,
            "practical_distribution_issue_fold_count": practical_dist_count,
            "cross_fold_outlier_status": cross_outlier_status,
            "cross_fold_distribution_status": cross_dist_status,
            "cross_fold_reasons": "; ".join(cross_reasons),
        }
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=BACKTEST_SUMMARY_OUTLIER_COLS)

    result = pd.DataFrame(rows)
    for col in BACKTEST_SUMMARY_OUTLIER_COLS:
        if col not in result.columns:
            result[col] = None
    return result[BACKTEST_SUMMARY_OUTLIER_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Build production diagnostics
# ---------------------------------------------------------------------------


def build_production_outlier_distribution_diagnostics(
    production_df: pd.DataFrame,
    cfg: OutlierDistributionConfig = _DEFAULT_CFG,
    evaluation_run_id: str = "",
) -> pd.DataFrame:
    """Build production outlier and distribution diagnostics."""
    if production_df.empty:
        return pd.DataFrame(columns=PRODUCTION_OUTLIER_COLS)

    group_keys = [
        "report_id", "selected_model_family", "selected_model_name", "selected_m",
    ]
    missing = [k for k in group_keys if k not in production_df.columns]
    if missing:
        _log.warning("Production outlier: missing group columns %s", missing)
        return pd.DataFrame(columns=PRODUCTION_OUTLIER_COLS)

    rows: list[dict] = []
    for group_vals, grp in production_df.groupby(group_keys, dropna=False):
        g = dict(
            zip(
                group_keys,
                group_vals if isinstance(group_vals, tuple) else (group_vals,),
            )
        )
        first_row = grp.iloc[0]

        try:
            original_count = len(grp)
            # All-records outlier metrics
            r_all, a_all, dates_all, horizons_all, excl_all, total_all = _filter_valid_with_meta(
                grp,
                date_col="forecast_date" if "forecast_date" in grp.columns else None,
                horizon_col="horizon_step" if "horizon_step" in grp.columns else None,
            )
            ev = _evidence_block(
                grp, r_all, total_all, excl_all, "production",
                date_col="forecast_date" if "forecast_date" in grp.columns else "residual_date",
                cfg=cfg,
            )
            outlier_m = calculate_robust_outlier_metrics(
                r_all, a_all, dates_all, horizons_all, cfg
            )

            # Deduplicated tail metrics
            deduped_grp, excluded_overlap = _deduplicate_production(grp)
            deduped_count = len(deduped_grp)
            r_deduped, a_deduped, _, _, _, _ = _filter_valid_with_meta(deduped_grp)
            tail_m = _compute_tail_metrics(r_deduped, cfg)
            dist_m = calculate_distribution_metrics(r_deduped, cfg)

            row = {
                "evaluation_run_id": evaluation_run_id,
                "report_id": g["report_id"],
                "report_name": first_row.get("report_name"),
                "selected_model_family": g["selected_model_family"],
                "selected_model_name": g["selected_model_name"],
                "selected_m": g["selected_m"],
                "lineage_complete": first_row.get("lineage_complete"),
                **ev,
                "original_prediction_count": original_count,
                "deduplicated_date_count": deduped_count,
                "excluded_overlap_count": excluded_overlap,
                **outlier_m,
                **tail_m,
                **dist_m,
            }
        except Exception as exc:
            _log.warning(
                "Production outlier diagnostics failed for group %s: %s", g, exc
            )
            row = {
                "evaluation_run_id": evaluation_run_id,
                "report_id": g.get("report_id"),
                "outlier_status": "calculation_failed",
                "outlier_reasons": f"Calculation failed: {exc}",
                "normality_status": "calculation_failed",
                "distribution_reasons": f"Calculation failed: {exc}",
            }
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=PRODUCTION_OUTLIER_COLS)

    result = pd.DataFrame(rows)
    for col in PRODUCTION_OUTLIER_COLS:
        if col not in result.columns:
            result[col] = None
    return result[PRODUCTION_OUTLIER_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_outlier_distribution_diagnostics(
    df: pd.DataFrame,
    dataset_name: str,
) -> None:
    """Validate schema and value constraints. Raises ValueError on failure."""
    schema_map = {
        "training": TRAINING_OUTLIER_COLS,
        "backtest_fold": BACKTEST_FOLD_OUTLIER_COLS,
        "backtest_summary": BACKTEST_SUMMARY_OUTLIER_COLS,
        "production": PRODUCTION_OUTLIER_COLS,
    }
    expected_cols = schema_map.get(dataset_name)
    if expected_cols is None:
        raise ValueError(f"Unknown dataset_name: {dataset_name!r}")

    if df.empty:
        return

    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"outlier_distribution_diagnostics[{dataset_name}]: missing columns {missing}"
        )

    tol = 1e-9

    # scaled_mad == 1.4826 * residual_mad
    if "scaled_mad" in df.columns and "residual_mad" in df.columns:
        mask = df["residual_mad"].notna() & df["scaled_mad"].notna()
        if mask.any():
            expected_scaled = df.loc[mask, "residual_mad"] * 1.4826
            actual_scaled = df.loc[mask, "scaled_mad"]
            diff = (expected_scaled - actual_scaled).abs()
            if (diff > tol).any():
                raise ValueError(
                    "scaled_mad must equal 1.4826 * residual_mad (within tolerance)"
                )

    # outlier_rate = outlier_count / valid_residual_count
    if all(c in df.columns for c in ("outlier_rate", "outlier_count", "valid_residual_count")):
        mask = df["valid_residual_count"].notna() & (df["valid_residual_count"] > 0) & df["outlier_rate"].notna()
        if mask.any():
            expected_rate = df.loc[mask, "outlier_count"] / df.loc[mask, "valid_residual_count"]
            actual_rate = df.loc[mask, "outlier_rate"]
            diff = (expected_rate - actual_rate).abs()
            if (diff > tol).any():
                raise ValueError("outlier_rate must equal outlier_count / valid_residual_count")

    # positive + negative outlier counts == outlier_count
    if all(
        c in df.columns
        for c in ("positive_outlier_count", "negative_outlier_count", "outlier_count")
    ):
        mask = df["outlier_count"].notna()
        if mask.any():
            total = (
                df.loc[mask, "positive_outlier_count"].fillna(0)
                + df.loc[mask, "negative_outlier_count"].fillna(0)
            )
            if not (total == df.loc[mask, "outlier_count"]).all():
                raise ValueError(
                    "positive_outlier_count + negative_outlier_count must equal outlier_count"
                )

    # rates between 0 and 1
    for rate_col in ("outlier_rate", "positive_outlier_rate", "negative_outlier_rate",
                     "extreme_underforecast_share", "extreme_overforecast_share"):
        if rate_col in df.columns:
            vals = df[rate_col].dropna()
            if len(vals) and ((vals < 0).any() or (vals > 1).any()):
                raise ValueError(f"{rate_col} must be between 0 and 1")

    # p-values between 0 and 1
    for pval_col in ("jarque_bera_pvalue", "shapiro_pvalue"):
        if pval_col in df.columns:
            vals = df[pval_col].dropna()
            if len(vals) and ((vals < 0).any() or (vals > 1).any()):
                raise ValueError(f"{pval_col} must be between 0 and 1")

    # lower_tail_quantile <= upper_tail_quantile
    if "lower_tail_quantile" in df.columns and "upper_tail_quantile" in df.columns:
        mask = df["lower_tail_quantile"].notna() & df["upper_tail_quantile"].notna()
        if mask.any():
            bad = df.loc[mask, "lower_tail_quantile"] > df.loc[mask, "upper_tail_quantile"]
            if bad.any():
                raise ValueError("lower_tail_quantile must be <= upper_tail_quantile")

    # Valid status values
    if "outlier_status" in df.columns:
        bad = df["outlier_status"].dropna()
        invalid = bad[~bad.isin(_VALID_OUTLIER_STATUSES)]
        if len(invalid):
            raise ValueError(
                f"Invalid outlier_status values: {invalid.unique().tolist()}"
            )

    if "normality_status" in df.columns:
        bad = df["normality_status"].dropna()
        invalid = bad[~bad.isin(_VALID_NORMALITY_STATUSES)]
        if len(invalid):
            raise ValueError(
                f"Invalid normality_status values: {invalid.unique().tolist()}"
            )

    if "tail_direction" in df.columns:
        bad = df["tail_direction"].dropna()
        invalid = bad[~bad.isin(_VALID_TAIL_DIRECTIONS)]
        if len(invalid):
            raise ValueError(
                f"Invalid tail_direction values: {invalid.unique().tolist()}"
            )

    # insufficient_evidence rows don't have acceptable outlier_status
    if "evidence_status" in df.columns and "outlier_status" in df.columns:
        insuff_mask = df["evidence_status"] == "insufficient"
        if insuff_mask.any():
            bad = df.loc[insuff_mask, "outlier_status"] == "acceptable"
            if bad.any():
                raise ValueError(
                    "Rows with insufficient evidence must not have outlier_status='acceptable'"
                )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def persist_outlier_distribution_diagnostics(
    training_df: pd.DataFrame,
    backtest_fold_df: pd.DataFrame,
    backtest_summary_df: pd.DataFrame,
    production_df: pd.DataFrame,
    project_root: Path,
) -> dict[str, Path | None]:
    """Validate and write all four outlier distribution diagnostic files."""
    diag_dir = project_root / _DIAG_DIR
    diag_dir.mkdir(parents=True, exist_ok=True)

    datasets = {
        "training": (
            training_df,
            "training_outlier_distribution_diagnostics_latest.csv",
            TRAINING_OUTLIER_COLS,
        ),
        "backtest_fold": (
            backtest_fold_df,
            "backtest_outlier_distribution_by_fold_latest.csv",
            BACKTEST_FOLD_OUTLIER_COLS,
        ),
        "backtest_summary": (
            backtest_summary_df,
            "backtest_outlier_distribution_summary_latest.csv",
            BACKTEST_SUMMARY_OUTLIER_COLS,
        ),
        "production": (
            production_df,
            "production_outlier_distribution_diagnostics_latest.csv",
            PRODUCTION_OUTLIER_COLS,
        ),
    }

    paths: dict[str, Path | None] = {}
    for name, (df, filename, cols) in datasets.items():
        path = diag_dir / filename
        try:
            validate_outlier_distribution_diagnostics(df, name)
            if df.empty:
                pd.DataFrame(columns=cols).to_csv(path, index=False)
            else:
                df.to_csv(path, index=False)
            paths[name] = path
        except Exception as exc:
            _log.error(
                "Failed to persist outlier_distribution_diagnostics[%s]: %s", name, exc
            )
            paths[name] = None

    return paths
