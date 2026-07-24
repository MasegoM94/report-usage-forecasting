"""Forecast bias and residual-stability diagnostics.

Determines whether a forecasting model:
1. Persistently underforecasts or overforecasts.
2. Has unstable bias across rolling folds.
3. Develops larger bias at longer forecast horizons.
4. Has residual variance that changes materially across folds, time windows,
   or horizons.
5. Has sufficient evidence for a stable conclusion.

Three separate evidence sources are analysed independently:
- Training residuals  (in-sample, may be optimistic)
- Backtest forecast errors  (out-of-sample rolling-origin)
- Production forecast errors  (realized operational evidence)

Residual sign convention
------------------------
All three source datasets use the diagnostic residual convention::

    residual = actual - forecast_or_fitted

Positive residual → model UNDERFORECASTED.
Negative residual → model OVERFORECASTED.

The canonical production monitoring history uses::

    signed_error = forecast - actual   (positive = over-forecast)

We validate:   residual = -signed_error

Normalized bias
---------------
Primary normalized bias::

    normalized_bias = sum(residuals) / sum(actuals)

Positive → underforecasting; negative → overforecasting.
When total actual equals zero, normalized_bias is null.

Variance comparison
-------------------
Training/backtest: fold-level variance dispersion (never pool overlapping folds).
Production: equal-length recent vs previous windows from the deduplicated
shortest-horizon time series.

Horizon buckets (inclusive)
---------------------------
days_1_7   : horizon_step  1 –  7
days_8_14  : horizon_step  8 – 14
days_15_28 : horizon_step 15 – 28

Statistical tests
-----------------
Brown–Forsythe (median-centred Levene) for fold variance equality requires
scipy.stats.  When scipy is not installed the test is skipped gracefully and
``variance_test_available = False``.

Public API
----------
calculate_bias_metrics(residuals, actuals) -> dict
calculate_fold_bias_stability(fold_records) -> dict
calculate_horizon_bias(residuals, actuals, horizon_steps, cfg) -> dict
calculate_variance_stability(residuals_by_group, cfg) -> dict
build_training_bias_diagnostics(df, cfg) -> pd.DataFrame
build_backtest_bias_diagnostics_by_fold(df, cfg) -> pd.DataFrame
build_backtest_bias_summary(fold_df, cfg) -> pd.DataFrame
build_production_bias_diagnostics(df, cfg) -> pd.DataFrame
classify_bias_status(metrics, cfg) -> tuple[str, list[str]]
classify_variance_stability(metrics, cfg) -> tuple[str, list[str]]
validate_bias_stability_diagnostics(df, dataset_name) -> None
persist_bias_stability_diagnostics(...) -> dict[str, Path | None]
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
# Optional dependency: scipy for Brown–Forsythe test
# ---------------------------------------------------------------------------

try:
    from scipy.stats import levene as _scipy_levene
    _SCIPY_AVAILABLE = True
except ImportError:
    _SCIPY_AVAILABLE = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class BiasStabilityConfig:
    """Thresholds for bias and variance classification.

    All threshold values are on the normalized_bias scale
    (sum(residuals) / sum(actuals)) unless otherwise noted.
    """

    # Minimum valid residuals to compute any bias.
    MIN_RESIDUALS_FOR_BIAS: int = 7

    # |normalized_bias| thresholds for bias classification.
    BIAS_WARNING_THRESHOLD: float = 0.05   # 5 %
    BIAS_POOR_THRESHOLD: float = 0.15       # 15 %

    # |median_residual / mean_actual| threshold for secondary median signal.
    MEDIAN_BIAS_WARNING_THRESHOLD: float = 0.05

    # Minimum valid folds before cross-fold stability can be assessed.
    MIN_VALID_FOLDS_FOR_BIAS_STABILITY: int = 2

    # Cross-fold bias std (on normalized scale) thresholds.
    FOLD_BIAS_STD_WARNING_THRESHOLD: float = 0.10
    FOLD_BIAS_STD_POOR_THRESHOLD: float = 0.25

    # Share of folds with directional bias (underforecasting or overforecasting)
    # thresholds.
    DIRECTIONAL_BIAS_FOLD_SHARE_WARNING: float = 0.60
    DIRECTIONAL_BIAS_FOLD_SHARE_POOR: float = 0.80

    # Minimum observations per horizon bucket to include in horizon analysis.
    MIN_OBSERVATIONS_PER_HORIZON_BUCKET: int = 5

    # |normalized_bias| range across horizon buckets.
    HORIZON_BIAS_RANGE_WARNING_THRESHOLD: float = 0.05
    HORIZON_BIAS_RANGE_POOR_THRESHOLD: float = 0.15

    # Minimum residuals to compute variance.
    MIN_RESIDUALS_FOR_VARIANCE: int = 5

    # Minimum residuals per variance comparison window.
    MIN_RESIDUALS_PER_VARIANCE_WINDOW: int = 14

    # Number of observations per production variance window.
    PRODUCTION_VARIANCE_WINDOW_SIZE: int = 28

    # variance_change_ratio thresholds (recent / previous).
    VARIANCE_CHANGE_WARNING_RATIO: float = 2.0
    VARIANCE_CHANGE_POOR_RATIO: float = 4.0

    # Fold variance coefficient-of-variation thresholds.
    FOLD_VARIANCE_CV_WARNING_THRESHOLD: float = 0.50
    FOLD_VARIANCE_CV_POOR_THRESHOLD: float = 1.00

    # Alpha for Brown–Forsythe test (supporting evidence only).
    VARIANCE_TEST_ALPHA: float = 0.05

    # Fraction trimmed from each tail for trimmed mean.
    TRIMMED_MEAN_PROPORTION: float = 0.10


_DEFAULT_CFG = BiasStabilityConfig()

# ---------------------------------------------------------------------------
# Horizon bucket definitions (mirrors horizon_evaluation.py)
# ---------------------------------------------------------------------------

_HORIZON_BUCKETS: list[tuple[str, int, int]] = [
    ("early",  1,  7),
    ("middle", 8,  14),
    ("late",   15, 28),
]

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

_BIAS_COLS = [
    # central
    "mean_residual",
    "median_residual",
    "residual_sum",
    "mean_absolute_residual",
    "median_absolute_residual",
    "mean_actual",
    "total_actual",
    "normalized_bias",
    "absolute_normalized_bias",
    "bias_direction",
    # robust
    "trimmed_mean_residual",
    "robust_normalized_bias",
    "mean_minus_median_residual",
]

_VARIANCE_COLS = [
    "residual_variance",
    "residual_std",
    "robust_residual_scale",
    "recent_residual_variance",
    "previous_residual_variance",
    "variance_change_ratio",
    "variance_change_absolute",
    "variance_difference",
    "recent_residual_std",
    "previous_residual_std",
    "recent_window_start",
    "recent_window_end",
    "previous_window_start",
    "previous_window_end",
    "variance_stability_status",
    "variance_stability_reasons",
    "variance_evidence_status",
]

_FOLD_VARIANCE_COLS = [
    "fold_variance_mean",
    "fold_variance_std",
    "fold_variance_coefficient_of_variation",
    "variance_test_statistic",
    "variance_test_pvalue",
    "variance_test_available",
    "variance_test_groups",
]

_HORIZON_BIAS_COLS = [
    "early_horizon_residual_count",
    "middle_horizon_residual_count",
    "late_horizon_residual_count",
    "early_horizon_bias",
    "middle_horizon_bias",
    "late_horizon_bias",
    "early_horizon_normalized_bias",
    "middle_horizon_normalized_bias",
    "late_horizon_normalized_bias",
    "early_horizon_variance",
    "middle_horizon_variance",
    "late_horizon_variance",
    "horizon_bias_range",
    "horizon_absolute_bias_range",
    "horizon_bias_direction_change",
    "horizon_variance_range",
    "horizon_bias_worsening_flag",
    "horizon_bias_status",
    "horizon_bias_reasons",
]

_FOLD_STABILITY_COLS = [
    "valid_fold_count",
    "total_fold_count",
    "mean_fold_bias",
    "median_fold_bias",
    "fold_bias_std",
    "fold_bias_min",
    "fold_bias_max",
    "fold_absolute_bias_mean",
    "fold_bias_sign_change_count",
    "underforecast_fold_count",
    "overforecast_fold_count",
    "approximately_unbiased_fold_count",
    "directional_bias_fold_share",
    "bias_consistency_status",
    "aggregate_normalized_bias",
    "median_fold_normalized_bias",
]

_CLASSIFICATION_COLS = [
    "bias_status",
    "bias_reasons",
    "practical_bias_flag",
]

TRAINING_BIAS_COLS: list[str] = (
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
    + _BIAS_COLS
    + _HORIZON_BIAS_COLS
    + _VARIANCE_COLS
    + _CLASSIFICATION_COLS
)

BACKTEST_FOLD_BIAS_COLS: list[str] = (
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
    + _BIAS_COLS
    + _HORIZON_BIAS_COLS
    + _VARIANCE_COLS
    + _CLASSIFICATION_COLS
)

BACKTEST_SUMMARY_BIAS_COLS: list[str] = (
    [
        "evaluation_run_id",
        "report_id",
        "report_name",
        "model_family",
        "model_name",
        "candidate_m",
    ]
    + _FOLD_STABILITY_COLS
    + _FOLD_VARIANCE_COLS
    + _BIAS_COLS          # pooled across all valid fold residuals
    + _CLASSIFICATION_COLS
)

PRODUCTION_BIAS_COLS: list[str] = (
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
    + _BIAS_COLS
    + [
        # all-records vs deduplicated distinction
        "all_records_mean_residual",
        "all_records_normalized_bias",
        "all_records_valid_count",
        "deduped_mean_residual",
        "deduped_normalized_bias",
        "deduped_valid_count",
    ]
    + _HORIZON_BIAS_COLS
    + _VARIANCE_COLS
    + _CLASSIFICATION_COLS
)

# ---------------------------------------------------------------------------
# Valid status sets
# ---------------------------------------------------------------------------

_VALID_BIAS_STATUSES = frozenset({
    "acceptable",
    "underforecasting",
    "overforecasting",
    "unstable",
    "poor",
    "insufficient_evidence",
    "calculation_failed",
})

_VALID_BIAS_DIRECTION = frozenset({
    "underforecasting",
    "overforecasting",
    "approximately_unbiased",
    "mixed",
    "insufficient_evidence",
})

_VALID_CONSISTENCY_STATUSES = frozenset({
    "consistent",
    "mixed",
    "unstable",
    "insufficient_evidence",
})

_VALID_HORIZON_STATUSES = frozenset({
    "stable",
    "worsening_with_horizon",
    "improving_with_horizon",
    "directionally_unstable",
    "insufficient_evidence",
})

_VALID_VARIANCE_STATUSES = frozenset({
    "stable",
    "warning",
    "unstable",
    "insufficient_evidence",
    "calculation_failed",
})

# ---------------------------------------------------------------------------
# Low-level arithmetic helpers
# ---------------------------------------------------------------------------


def _trimmed_mean(arr: np.ndarray, proportion: float) -> float | None:
    """Return trimmed mean excluding `proportion` from each tail."""
    n = len(arr)
    if n == 0:
        return None
    k = int(np.floor(proportion * n))
    if 2 * k >= n:
        return float(np.mean(arr))
    trimmed = np.sort(arr)[k: n - k]
    return float(np.mean(trimmed))


def _robust_scale(arr: np.ndarray) -> float | None:
    """Median Absolute Deviation (MAD) scaled to be comparable to std."""
    if len(arr) == 0:
        return None
    mad = float(np.median(np.abs(arr - np.median(arr))))
    return mad * 1.4826  # consistency factor for normal distribution


def _safe_variance(arr: np.ndarray) -> float | None:
    if len(arr) < 2:
        return None
    return float(np.var(arr, ddof=1))


def _bias_direction_label(
    normalized_bias: float | None,
    cfg: BiasStabilityConfig,
) -> str:
    if normalized_bias is None:
        return "insufficient_evidence"
    if abs(normalized_bias) < cfg.BIAS_WARNING_THRESHOLD:
        return "approximately_unbiased"
    if normalized_bias > 0:
        return "underforecasting"
    return "overforecasting"


# ---------------------------------------------------------------------------
# Core bias metrics
# ---------------------------------------------------------------------------


def calculate_bias_metrics(
    residuals: np.ndarray,
    actuals: np.ndarray,
    cfg: BiasStabilityConfig = _DEFAULT_CFG,
) -> dict:
    """Calculate all central and robust bias metrics for one group.

    Parameters
    ----------
    residuals:
        1-D finite residual values (residual = actual - fitted/forecast).
    actuals:
        1-D corresponding actual values (must match residuals length).
    cfg:
        Configuration thresholds.

    Returns
    -------
    Flat dict with all _BIAS_COLS keys.
    """
    n = len(residuals)
    if n == 0 or n < cfg.MIN_RESIDUALS_FOR_BIAS:
        return {k: None for k in _BIAS_COLS}

    mean_res = float(np.mean(residuals))
    median_res = float(np.median(residuals))
    res_sum = float(np.sum(residuals))
    mean_abs = float(np.mean(np.abs(residuals)))
    median_abs = float(np.median(np.abs(residuals)))
    mean_act = float(np.mean(actuals)) if len(actuals) else None
    total_act = float(np.sum(actuals)) if len(actuals) else None

    norm_bias: float | None = None
    abs_norm_bias: float | None = None
    robust_norm_bias: float | None = None

    if total_act is not None and total_act > 0:
        norm_bias = res_sum / total_act
        abs_norm_bias = abs(norm_bias)

    trimmed = _trimmed_mean(residuals, cfg.TRIMMED_MEAN_PROPORTION)
    if total_act and total_act > 0 and trimmed is not None:
        trimmed_sum = trimmed * n
        robust_norm_bias = trimmed_sum / total_act

    direction = _bias_direction_label(norm_bias, cfg)

    return {
        "mean_residual": mean_res,
        "median_residual": median_res,
        "residual_sum": res_sum,
        "mean_absolute_residual": mean_abs,
        "median_absolute_residual": median_abs,
        "mean_actual": mean_act,
        "total_actual": total_act,
        "normalized_bias": norm_bias,
        "absolute_normalized_bias": abs_norm_bias,
        "bias_direction": direction,
        "trimmed_mean_residual": trimmed,
        "robust_normalized_bias": robust_norm_bias,
        "mean_minus_median_residual": mean_res - median_res,
    }


# ---------------------------------------------------------------------------
# Horizon bias
# ---------------------------------------------------------------------------


def calculate_horizon_bias(
    residuals: np.ndarray,
    actuals: np.ndarray,
    horizon_steps: np.ndarray,
    cfg: BiasStabilityConfig = _DEFAULT_CFG,
) -> dict:
    """Calculate bias and variance per horizon bucket.

    Buckets are defined by actual horizon_step values (not row order).
    Buckets with fewer than MIN_OBSERVATIONS_PER_HORIZON_BUCKET observations
    are marked as having insufficient evidence.

    Parameters
    ----------
    residuals, actuals, horizon_steps:
        Parallel arrays of equal length.

    Returns
    -------
    Flat dict with all _HORIZON_BIAS_COLS keys.
    """
    base = {k: None for k in _HORIZON_BIAS_COLS}

    def _bucket_stats(lo: int, hi: int) -> dict:
        mask = (horizon_steps >= lo) & (horizon_steps <= hi)
        r = residuals[mask]
        a = actuals[mask]
        n = len(r)
        if n < cfg.MIN_OBSERVATIONS_PER_HORIZON_BUCKET:
            return {"count": n, "bias": None, "norm_bias": None, "variance": None}
        mean_b = float(np.mean(r))
        tot_a = float(np.sum(a))
        norm_b = float(np.sum(r) / tot_a) if tot_a > 0 else None
        var_b = _safe_variance(r)
        return {"count": n, "bias": mean_b, "norm_bias": norm_b, "variance": var_b}

    early = _bucket_stats(1, 7)
    middle = _bucket_stats(8, 14)
    late = _bucket_stats(15, 28)

    base["early_horizon_residual_count"] = early["count"]
    base["middle_horizon_residual_count"] = middle["count"]
    base["late_horizon_residual_count"] = late["count"]
    base["early_horizon_bias"] = early["bias"]
    base["middle_horizon_bias"] = middle["bias"]
    base["late_horizon_bias"] = late["bias"]
    base["early_horizon_normalized_bias"] = early["norm_bias"]
    base["middle_horizon_normalized_bias"] = middle["norm_bias"]
    base["late_horizon_normalized_bias"] = late["norm_bias"]
    base["early_horizon_variance"] = early["variance"]
    base["middle_horizon_variance"] = middle["variance"]
    base["late_horizon_variance"] = late["variance"]

    # Range and direction analysis using available buckets
    norm_vals = [
        v for v in [early["norm_bias"], middle["norm_bias"], late["norm_bias"]]
        if v is not None
    ]
    bias_vals = [
        v for v in [early["bias"], middle["bias"], late["bias"]]
        if v is not None
    ]
    var_vals = [
        v for v in [early["variance"], middle["variance"], late["variance"]]
        if v is not None
    ]

    if len(norm_vals) >= 2:
        base["horizon_bias_range"] = float(max(norm_vals) - min(norm_vals))
        base["horizon_absolute_bias_range"] = float(
            max(abs(v) for v in norm_vals) - min(abs(v) for v in norm_vals)
        )
    if len(var_vals) >= 2:
        base["horizon_variance_range"] = float(max(var_vals) - min(var_vals))

    # Direction-change: sign flips among available buckets
    if len(bias_vals) >= 2:
        signs = [1 if v > 0 else (-1 if v < 0 else 0) for v in bias_vals]
        nonzero_signs = [s for s in signs if s != 0]
        changes = sum(
            1 for i in range(1, len(nonzero_signs))
            if nonzero_signs[i] != nonzero_signs[i - 1]
        )
        base["horizon_bias_direction_change"] = changes
    else:
        base["horizon_bias_direction_change"] = 0

    # Horizon bias status and worsening flag
    reasons: list[str] = []
    status = "insufficient_evidence"

    sufficient_buckets = sum(
        1 for v in [early["norm_bias"], middle["norm_bias"], late["norm_bias"]]
        if v is not None
    )

    if sufficient_buckets < 2:
        reasons.append(
            "Insufficient evidence: fewer than two horizon buckets meet the "
            "minimum observation requirement."
        )
        base["horizon_bias_worsening_flag"] = False
        base["horizon_bias_status"] = status
        base["horizon_bias_reasons"] = "; ".join(reasons)
        return base

    # Worsening: compare available early → late trend
    worsening = False
    if early["norm_bias"] is not None and late["norm_bias"] is not None:
        rng = abs(late["norm_bias"] - early["norm_bias"])
        if rng >= cfg.HORIZON_BIAS_RANGE_WARNING_THRESHOLD:
            # direction of worsening
            if abs(late["norm_bias"]) > abs(early["norm_bias"]):
                worsening = True

    base["horizon_bias_worsening_flag"] = worsening

    # Direction changes
    dir_changes = base["horizon_bias_direction_change"] or 0
    bias_range = base["horizon_bias_range"] or 0.0
    abs_bias_range = base["horizon_absolute_bias_range"] or 0.0

    if dir_changes >= 1:
        status = "directionally_unstable"
        reasons.append("Horizon bias changes direction across buckets.")
    elif worsening:
        # determine direction
        if late["norm_bias"] is not None and late["norm_bias"] < 0:
            reasons.append(
                "Overforecasting becomes more severe at longer horizons "
                f"(early {early['norm_bias']:.1%}, late {late['norm_bias']:.1%})."
            )
        else:
            reasons.append(
                "Underforecasting becomes more severe at longer horizons "
                f"(early {early['norm_bias']:.1%}, late {late['norm_bias']:.1%})."
            )
        status = "worsening_with_horizon"
    elif abs_bias_range < cfg.HORIZON_BIAS_RANGE_WARNING_THRESHOLD:
        status = "stable"
        reasons.append("Forecast bias is stable across horizon buckets.")
    else:
        status = "improving_with_horizon"
        reasons.append("Forecast bias decreases at longer horizons.")

    base["horizon_bias_status"] = status
    base["horizon_bias_reasons"] = "; ".join(reasons)
    return base


# ---------------------------------------------------------------------------
# Variance stability
# ---------------------------------------------------------------------------


def calculate_variance_stability(
    residuals_by_group: list[np.ndarray],
    cfg: BiasStabilityConfig = _DEFAULT_CFG,
    recent_window: np.ndarray | None = None,
    previous_window: np.ndarray | None = None,
    recent_dates: tuple | None = None,
    previous_dates: tuple | None = None,
) -> dict:
    """Calculate variance stability metrics for fold-level or time-window analysis.

    Parameters
    ----------
    residuals_by_group:
        List of finite residual arrays (one per fold or time window).
        Used for fold-level variance dispersion and Brown–Forsythe test.
    cfg:
        Configuration thresholds.
    recent_window, previous_window:
        Equal-length finite residual arrays for recent vs previous comparison.
        When provided, window comparison is performed.  Both must be supplied
        or neither.
    recent_dates, previous_dates:
        (start_date, end_date) tuples for the respective windows.

    Returns
    -------
    Flat dict with all _VARIANCE_COLS + _FOLD_VARIANCE_COLS keys.
    """
    out: dict = {k: None for k in _VARIANCE_COLS + _FOLD_VARIANCE_COLS}
    out["variance_test_available"] = _SCIPY_AVAILABLE

    # Overall variance from all residuals pooled
    all_res = np.concatenate(residuals_by_group) if residuals_by_group else np.array([])
    if len(all_res) >= cfg.MIN_RESIDUALS_FOR_VARIANCE:
        out["residual_variance"] = _safe_variance(all_res)
        out["residual_std"] = (
            float(np.sqrt(out["residual_variance"]))
            if out["residual_variance"] is not None
            else None
        )
        out["robust_residual_scale"] = _robust_scale(all_res)

    # Fold-level variance
    valid_groups = [
        g for g in residuals_by_group
        if len(g) >= cfg.MIN_RESIDUALS_FOR_VARIANCE
    ]
    if valid_groups:
        fold_vars = [_safe_variance(g) for g in valid_groups]
        fold_vars_finite = [v for v in fold_vars if v is not None and np.isfinite(v)]
        if fold_vars_finite:
            fv_mean = float(np.mean(fold_vars_finite))
            fv_std = float(np.std(fold_vars_finite, ddof=1)) if len(fold_vars_finite) >= 2 else 0.0
            fv_cv = (fv_std / fv_mean) if fv_mean > 0 else None
            out["fold_variance_mean"] = fv_mean
            out["fold_variance_std"] = fv_std
            out["fold_variance_coefficient_of_variation"] = fv_cv

        # Brown–Forsythe test (median-centred Levene)
        if _SCIPY_AVAILABLE and len(valid_groups) >= 2:
            try:
                stat, pval = _scipy_levene(*valid_groups, center="median")
                out["variance_test_statistic"] = float(stat)
                out["variance_test_pvalue"] = float(pval)
                out["variance_test_groups"] = len(valid_groups)
            except Exception as exc:
                _log.debug("Brown–Forsythe test failed: %s", exc)
        elif len(valid_groups) >= 2:
            out["variance_test_groups"] = len(valid_groups)

    # Window comparison
    variance_evidence_status = "insufficient"
    reasons: list[str] = []
    var_status = "insufficient_evidence"

    if recent_window is not None and previous_window is not None:
        rw = np.asarray(recent_window, dtype=float)
        pw = np.asarray(previous_window, dtype=float)
        rw = rw[np.isfinite(rw)]
        pw = pw[np.isfinite(pw)]

        if recent_dates:
            out["recent_window_start"] = recent_dates[0]
            out["recent_window_end"] = recent_dates[1]
        if previous_dates:
            out["previous_window_start"] = previous_dates[0]
            out["previous_window_end"] = previous_dates[1]

        if (
            len(rw) >= cfg.MIN_RESIDUALS_PER_VARIANCE_WINDOW
            and len(pw) >= cfg.MIN_RESIDUALS_PER_VARIANCE_WINDOW
        ):
            if len(rw) != len(pw):
                # Unequal windows — not allowed
                variance_evidence_status = "unequal_windows"
                reasons.append(
                    f"Unequal comparison windows ({len(rw)} vs {len(pw)}) — "
                    "variance comparison skipped."
                )
                var_status = "insufficient_evidence"
            else:
                r_var = _safe_variance(rw)
                p_var = _safe_variance(pw)
                out["recent_residual_variance"] = r_var
                out["previous_residual_variance"] = p_var
                out["recent_residual_std"] = float(np.sqrt(r_var)) if r_var is not None else None
                out["previous_residual_std"] = float(np.sqrt(p_var)) if p_var is not None else None

                if r_var is not None and p_var is not None:
                    out["variance_difference"] = r_var - p_var
                    out["variance_change_absolute"] = abs(r_var - p_var)

                    if p_var == 0.0 and r_var == 0.0:
                        ratio = 1.0
                    elif p_var == 0.0:
                        ratio = None  # undefined — infinite
                        out["variance_evidence_status"] = "previous_variance_zero"
                    else:
                        ratio = r_var / p_var
                    out["variance_change_ratio"] = ratio

                    variance_evidence_status = "ok"

                    if ratio is None:
                        var_status = "unstable"
                        reasons.append(
                            "Previous window variance is zero but recent window "
                            "variance is positive — ratio is undefined (infinite)."
                        )
                    elif ratio >= cfg.VARIANCE_CHANGE_POOR_RATIO:
                        var_status = "unstable"
                        reasons.append(
                            f"Residual variance increased from {p_var:.2f} to "
                            f"{r_var:.2f} "
                            f"(ratio {ratio:.2f} ≥ poor threshold {cfg.VARIANCE_CHANGE_POOR_RATIO})."
                        )
                    elif ratio >= cfg.VARIANCE_CHANGE_WARNING_RATIO:
                        var_status = "warning"
                        reasons.append(
                            f"Residual variance increased from {p_var:.2f} to "
                            f"{r_var:.2f} "
                            f"(ratio {ratio:.2f} ≥ warning threshold {cfg.VARIANCE_CHANGE_WARNING_RATIO})."
                        )
                    elif ratio > 0 and ratio <= 1.0 / cfg.VARIANCE_CHANGE_WARNING_RATIO:
                        var_status = "warning"
                        reasons.append(
                            f"Residual variance decreased materially from {p_var:.2f} to {r_var:.2f}."
                        )
                    else:
                        var_status = "stable"
                        reasons.append("Residual variance is stable across comparable windows.")
        else:
            reasons.append(
                "Insufficient observations for equal-length variance window comparison."
            )
    else:
        # No window comparison — use fold dispersion only
        fv_cv = out.get("fold_variance_coefficient_of_variation")
        if fv_cv is not None:
            variance_evidence_status = "ok"
            if fv_cv >= cfg.FOLD_VARIANCE_CV_POOR_THRESHOLD:
                var_status = "unstable"
                reasons.append(
                    f"Fold-level residual variance is unstable "
                    f"(CV={fv_cv:.2f} ≥ poor threshold {cfg.FOLD_VARIANCE_CV_POOR_THRESHOLD})."
                )
            elif fv_cv >= cfg.FOLD_VARIANCE_CV_WARNING_THRESHOLD:
                var_status = "warning"
                reasons.append(
                    f"Fold-level residual variance dispersion is elevated "
                    f"(CV={fv_cv:.2f})."
                )
            else:
                var_status = "stable"
                reasons.append("Fold-level residual variance is stable.")
        else:
            reasons.append("Insufficient evidence for variance stability assessment.")

    if not out.get("variance_evidence_status"):
        out["variance_evidence_status"] = variance_evidence_status
    out["variance_stability_status"] = var_status
    out["variance_stability_reasons"] = "; ".join(reasons) if reasons else ""
    return out


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def classify_bias_status(
    metrics: dict,
    cfg: BiasStabilityConfig = _DEFAULT_CFG,
) -> tuple[str, list[str]]:
    """Classify overall bias status from computed metrics.

    Parameters
    ----------
    metrics:
        Dict containing at minimum: evidence_status, normalized_bias,
        absolute_normalized_bias, bias_direction, mean_residual,
        median_residual.

    Returns
    -------
    (bias_status, reasons)  where bias_status in _VALID_BIAS_STATUSES.
    """
    evidence = metrics.get("evidence_status", "insufficient")
    if evidence in ("insufficient", "empty"):
        return "insufficient_evidence", ["Insufficient residuals for bias assessment."]
    if evidence == "calculation_failed":
        return "calculation_failed", ["Bias calculation failed."]

    reasons: list[str] = []
    norm_bias = metrics.get("normalized_bias")
    abs_norm = metrics.get("absolute_normalized_bias")
    direction = metrics.get("bias_direction", "insufficient_evidence")
    mean_res = metrics.get("mean_residual")
    median_res = metrics.get("median_residual")

    practical_flag = False

    if norm_bias is None:
        reasons.append(
            "Total actual usage is zero, so normalized bias is unavailable; "
            "raw error metrics are retained."
        )
        # Use mean residual magnitude for classification
        if mean_res is not None and abs(mean_res) > 0:
            return "insufficient_evidence", reasons
        return "insufficient_evidence", reasons

    if abs_norm is None:
        return "insufficient_evidence", reasons

    # Primary signal: normalized bias magnitude
    if abs_norm >= cfg.BIAS_POOR_THRESHOLD:
        practical_flag = True
        if norm_bias > 0:
            status = "poor"
            reasons.append(
                f"The model persistently underforecasts usage, with normalized bias of "
                f"+{norm_bias:.1%}."
            )
        else:
            status = "poor"
            reasons.append(
                f"The model persistently overforecasts usage, with normalized bias of "
                f"{norm_bias:.1%}."
            )
    elif abs_norm >= cfg.BIAS_WARNING_THRESHOLD:
        practical_flag = True
        if norm_bias > 0:
            status = "underforecasting"
            reasons.append(
                f"The model underforecasts usage (normalized bias +{norm_bias:.1%})."
            )
        else:
            status = "overforecasting"
            reasons.append(
                f"The model overforecasts usage (normalized bias {norm_bias:.1%})."
            )
    else:
        status = "acceptable"
        reasons.append("Forecast bias is within the configured practical tolerance.")

    # Supporting: mean vs median divergence
    if mean_res is not None and median_res is not None:
        m_diff = abs(mean_res - median_res)
        if mean_res != 0:
            rel_diff = m_diff / (abs(mean_res) + 1e-12)
            if rel_diff > 0.5:
                reasons.append(
                    f"Mean bias ({mean_res:.2f}) diverges from median bias "
                    f"({median_res:.2f}), suggesting outlier influence."
                )

    return status, reasons


def calculate_fold_bias_stability(
    fold_records: list[dict],
    cfg: BiasStabilityConfig = _DEFAULT_CFG,
) -> dict:
    """Summarise bias consistency across folds.

    Parameters
    ----------
    fold_records:
        List of per-fold bias metric dicts (from calculate_bias_metrics).
        Each must have: normalized_bias, residual_sum, total_actual,
        bias_direction.

    Returns
    -------
    Flat dict with all _FOLD_STABILITY_COLS keys.
    """
    out: dict = {k: None for k in _FOLD_STABILITY_COLS}
    out["total_fold_count"] = len(fold_records)

    valid = [
        f for f in fold_records
        if f.get("normalized_bias") is not None
        and f.get("total_actual") is not None
    ]
    n_valid = len(valid)
    out["valid_fold_count"] = n_valid

    if n_valid < cfg.MIN_VALID_FOLDS_FOR_BIAS_STABILITY:
        out["bias_consistency_status"] = "insufficient_evidence"
        return out

    norm_biases = [f["normalized_bias"] for f in valid]
    abs_biases = [abs(b) for b in norm_biases]

    out["mean_fold_bias"] = float(np.mean(norm_biases))
    out["median_fold_bias"] = float(np.median(norm_biases))
    out["fold_bias_std"] = float(np.std(norm_biases, ddof=1)) if n_valid >= 2 else 0.0
    out["fold_bias_min"] = float(min(norm_biases))
    out["fold_bias_max"] = float(max(norm_biases))
    out["fold_absolute_bias_mean"] = float(np.mean(abs_biases))
    out["median_fold_normalized_bias"] = float(np.median(norm_biases))

    # Sign changes
    sign_changes = sum(
        1 for i in range(1, n_valid)
        if np.sign(norm_biases[i]) != np.sign(norm_biases[i - 1])
        and np.sign(norm_biases[i]) != 0
        and np.sign(norm_biases[i - 1]) != 0
    )
    out["fold_bias_sign_change_count"] = sign_changes

    # Directional counts
    under_count = sum(1 for b in norm_biases if b >= cfg.BIAS_WARNING_THRESHOLD)
    over_count = sum(1 for b in norm_biases if b <= -cfg.BIAS_WARNING_THRESHOLD)
    unbiased_count = n_valid - under_count - over_count
    directional_count = under_count + over_count

    out["underforecast_fold_count"] = under_count
    out["overforecast_fold_count"] = over_count
    out["approximately_unbiased_fold_count"] = unbiased_count
    out["directional_bias_fold_share"] = directional_count / n_valid

    # Aggregate normalized bias from pooled fold totals
    total_res_sum = sum(f.get("residual_sum") or 0 for f in valid)
    total_act_sum = sum(f.get("total_actual") or 0 for f in valid)
    if total_act_sum > 0:
        out["aggregate_normalized_bias"] = total_res_sum / total_act_sum
    else:
        out["aggregate_normalized_bias"] = None

    # Consistency classification
    fbs = out["fold_bias_std"] or 0.0
    dir_share = out["directional_bias_fold_share"] or 0.0
    sc = sign_changes

    if fbs >= cfg.FOLD_BIAS_STD_POOR_THRESHOLD or sc >= max(2, n_valid // 2):
        consistency = "unstable"
    elif fbs >= cfg.FOLD_BIAS_STD_WARNING_THRESHOLD or sc >= 1:
        consistency = "mixed"
    elif dir_share >= cfg.DIRECTIONAL_BIAS_FOLD_SHARE_WARNING:
        consistency = "consistent"
    else:
        consistency = "consistent"

    out["bias_consistency_status"] = consistency
    return out


def classify_variance_stability(
    metrics: dict,
    cfg: BiasStabilityConfig = _DEFAULT_CFG,
) -> tuple[str, list[str]]:
    """Extract variance status from the metrics dict (already computed)."""
    status = metrics.get("variance_stability_status", "insufficient_evidence")
    reasons_str = metrics.get("variance_stability_reasons", "")
    reasons = [r.strip() for r in reasons_str.split(";") if r.strip()] if reasons_str else []
    return status, reasons


# ---------------------------------------------------------------------------
# Internal: filter valid residuals
# ---------------------------------------------------------------------------


def _filter_valid(
    df: pd.DataFrame,
    residual_col: str = "residual",
    actual_col: str = "actual",
    valid_col: str = "residual_observation_valid",
) -> tuple[np.ndarray, np.ndarray, int, int]:
    """Return (residuals, actuals, excluded_invalid, total_count)."""
    total = len(df)
    valid_mask = df[valid_col].fillna(False).astype(bool) if valid_col in df.columns else pd.Series(True, index=df.index)
    sub = df[valid_mask]
    r = np.asarray(sub[residual_col] if residual_col in sub.columns else [], dtype=float)
    a = np.asarray(sub[actual_col] if actual_col in sub.columns else np.zeros(len(sub)), dtype=float)
    finite_mask = np.isfinite(r)
    excluded = int((~finite_mask).sum())
    return r[finite_mask], a[finite_mask], excluded, total


def _evidence_block(
    df: pd.DataFrame,
    residuals: np.ndarray,
    total_count: int,
    excluded_invalid: int,
    source: str,
    date_col: str = "residual_date",
    cfg: BiasStabilityConfig = _DEFAULT_CFG,
) -> dict:
    valid_count = len(residuals)
    first_date = None
    last_date = None
    if date_col in df.columns:
        dates = df[date_col].dropna()
        if len(dates):
            first_date = dates.min()
            last_date = dates.max()
    evidence_status = "ok" if valid_count >= cfg.MIN_RESIDUALS_FOR_BIAS else "insufficient"
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
# Training bias diagnostics
# ---------------------------------------------------------------------------


def build_training_bias_diagnostics(
    training_df: pd.DataFrame,
    cfg: BiasStabilityConfig = _DEFAULT_CFG,
    diagnostic_run_id: str = "",
) -> pd.DataFrame:
    """Build training bias and stability diagnostics.

    Groups by (report_id, model_family, model_name, candidate_m, fit_scope,
    fold_number).  Uses only residual_observation_valid == True rows.

    Note: Training residuals are in-sample evidence; conclusions may be
    optimistic relative to out-of-sample performance.

    Parameters
    ----------
    training_df:
        DataFrame matching TRAINING_RESIDUALS_COLS schema.
    cfg:
        Configuration thresholds.
    diagnostic_run_id:
        Run identifier.

    Returns
    -------
    DataFrame with TRAINING_BIAS_COLS columns.
    """
    if training_df.empty:
        return pd.DataFrame(columns=TRAINING_BIAS_COLS)

    group_keys = [
        "report_id", "model_family", "model_name", "candidate_m",
        "fit_scope", "fold_number",
    ]

    rows: list[dict] = []
    for group_vals, grp in training_df.groupby(group_keys, dropna=False):
        g = dict(zip(group_keys, group_vals if isinstance(group_vals, tuple) else (group_vals,)))

        try:
            residuals, actuals, excl_invalid, total_count = _filter_valid(
                grp, residual_col="residual", actual_col="actual",
            )
            first_row = grp.iloc[0]
            ev = _evidence_block(
                grp, residuals, total_count, excl_invalid, "training",
                date_col="residual_date", cfg=cfg,
            )
            bias = calculate_bias_metrics(residuals, actuals, cfg)
            has_horizon = "horizon_step" in grp.columns
            if has_horizon:
                valid_mask = grp["residual_observation_valid"].fillna(False).astype(bool)
                valid_grp = grp[valid_mask]
                all_r = np.asarray(valid_grp["residual"], dtype=float)
                all_a = np.asarray(valid_grp["actual"], dtype=float)
                all_h = np.asarray(valid_grp["horizon_step"], dtype=float)
                fin = np.isfinite(all_r)
                horiz = calculate_horizon_bias(all_r[fin], all_a[fin], all_h[fin], cfg)
            else:
                horiz = {k: None for k in _HORIZON_BIAS_COLS}

            var_metrics = calculate_variance_stability([residuals], cfg)

            bias_status, bias_reasons = classify_bias_status(
                {**ev, **bias}, cfg
            )

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
                **bias,
                **horiz,
                **var_metrics,
                "bias_status": bias_status,
                "bias_reasons": "; ".join(bias_reasons),
                "practical_bias_flag": bias_status in ("underforecasting", "overforecasting", "unstable", "poor"),
            }
        except Exception as exc:
            _log.warning("Training bias failed for group %s: %s", g, exc)
            row = {
                "diagnostic_run_id": diagnostic_run_id,
                "report_id": g.get("report_id"),
                "bias_status": "calculation_failed",
                "bias_reasons": f"Calculation failed: {exc}",
                "evidence_status": "calculation_failed",
            }

        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=TRAINING_BIAS_COLS)

    result = pd.DataFrame(rows)
    for col in TRAINING_BIAS_COLS:
        if col not in result.columns:
            result[col] = None
    return result[TRAINING_BIAS_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Backtest fold-level diagnostics
# ---------------------------------------------------------------------------


def build_backtest_bias_diagnostics_by_fold(
    backtest_df: pd.DataFrame,
    cfg: BiasStabilityConfig = _DEFAULT_CFG,
    evaluation_run_id: str = "",
) -> pd.DataFrame:
    """Build per-fold backtest bias diagnostics.

    ACF/bias is calculated within each fold independently — overlapping
    fold windows are never concatenated.

    Parameters
    ----------
    backtest_df:
        DataFrame matching BACKTEST_FORECAST_ERRORS_COLS schema.
    cfg, evaluation_run_id:
        As above.

    Returns
    -------
    DataFrame with BACKTEST_FOLD_BIAS_COLS columns.
    """
    if backtest_df.empty:
        return pd.DataFrame(columns=BACKTEST_FOLD_BIAS_COLS)

    group_keys = [
        "report_id", "model_family", "model_name", "candidate_m", "fold_number",
    ]

    rows: list[dict] = []
    for group_vals, grp in backtest_df.groupby(group_keys, dropna=False):
        g = dict(zip(group_keys, group_vals if isinstance(group_vals, tuple) else (group_vals,)))
        try:
            residuals, actuals, excl_invalid, total_count = _filter_valid(
                grp, residual_col="residual", actual_col="actual",
            )
            first_row = grp.iloc[0]
            ev = _evidence_block(
                grp, residuals, total_count, excl_invalid, "backtest",
                date_col="forecast_date", cfg=cfg,
            )
            bias = calculate_bias_metrics(residuals, actuals, cfg)

            # Horizon bias
            valid_mask = grp["residual_observation_valid"].fillna(False).astype(bool)
            valid_grp = grp[valid_mask]
            if "horizon_step" in valid_grp.columns:
                all_r = np.asarray(valid_grp["residual"], dtype=float)
                all_a = np.asarray(valid_grp["actual"], dtype=float)
                all_h = np.asarray(valid_grp["horizon_step"], dtype=float)
                fin = np.isfinite(all_r)
                horiz = calculate_horizon_bias(all_r[fin], all_a[fin], all_h[fin], cfg)
            else:
                horiz = {k: None for k in _HORIZON_BIAS_COLS}

            var_metrics = calculate_variance_stability([residuals], cfg)
            bias_status, bias_reasons = classify_bias_status({**ev, **bias}, cfg)

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
                **bias,
                **horiz,
                **var_metrics,
                "bias_status": bias_status,
                "bias_reasons": "; ".join(bias_reasons),
                "practical_bias_flag": bias_status in ("underforecasting", "overforecasting", "unstable", "poor"),
            }
        except Exception as exc:
            _log.warning("Backtest fold bias failed for group %s: %s", g, exc)
            row = {
                "evaluation_run_id": evaluation_run_id,
                "report_id": g.get("report_id"),
                "fold_number": g.get("fold_number"),
                "bias_status": "calculation_failed",
                "bias_reasons": f"Calculation failed: {exc}",
                "evidence_status": "calculation_failed",
            }
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=BACKTEST_FOLD_BIAS_COLS)

    result = pd.DataFrame(rows)
    for col in BACKTEST_FOLD_BIAS_COLS:
        if col not in result.columns:
            result[col] = None
    return result[BACKTEST_FOLD_BIAS_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Backtest cross-fold summary
# ---------------------------------------------------------------------------


def build_backtest_bias_summary(
    fold_df: pd.DataFrame,
    backtest_df: pd.DataFrame | None = None,
    cfg: BiasStabilityConfig = _DEFAULT_CFG,
    evaluation_run_id: str = "",
) -> pd.DataFrame:
    """Build cross-fold summary bias diagnostics.

    Aggregated bias is computed from pooled fold totals (not mean of fold
    biases) so that volume differences between folds are handled correctly.

    Parameters
    ----------
    fold_df:
        Output of build_backtest_bias_diagnostics_by_fold.
    backtest_df:
        Original backtest DataFrame (used for pooled residual and fold
        variance calculations).  Optional.
    cfg, evaluation_run_id:
        As above.

    Returns
    -------
    DataFrame with BACKTEST_SUMMARY_BIAS_COLS columns.
    """
    if fold_df.empty:
        return pd.DataFrame(columns=BACKTEST_SUMMARY_BIAS_COLS)

    summary_group_keys = ["report_id", "model_family", "model_name", "candidate_m"]

    rows: list[dict] = []
    for group_vals, grp in fold_df.groupby(summary_group_keys, dropna=False):
        g = dict(zip(summary_group_keys, group_vals if isinstance(group_vals, tuple) else (group_vals,)))
        first_row = grp.iloc[0]

        # Gather per-fold bias records for stability
        fold_records = []
        for _, frow in grp.iterrows():
            fold_records.append({
                "normalized_bias": frow.get("normalized_bias"),
                "residual_sum": frow.get("residual_sum"),
                "total_actual": frow.get("total_actual"),
                "bias_direction": frow.get("bias_direction"),
            })

        stability = calculate_fold_bias_stability(fold_records, cfg)

        # Pooled residuals for overall bias
        all_residuals = np.array([])
        all_actuals = np.array([])
        fold_residual_groups: list[np.ndarray] = []

        if backtest_df is not None:
            mask = np.ones(len(backtest_df), dtype=bool)
            for k, v in g.items():
                if k in backtest_df.columns:
                    if pd.isna(v):
                        mask &= backtest_df[k].isna()
                    else:
                        mask &= backtest_df[k] == v
            sub = backtest_df[mask]
            # Pooled across folds
            r, a, excl, total = _filter_valid(
                sub, residual_col="residual", actual_col="actual",
            )
            all_residuals = r
            all_actuals = a

            # Per-fold groups for variance
            if "fold_number" in sub.columns:
                for fn, fsub in sub.groupby("fold_number"):
                    fr, _, _, _ = _filter_valid(fsub, residual_col="residual", actual_col="actual")
                    if len(fr) >= cfg.MIN_RESIDUALS_FOR_VARIANCE:
                        fold_residual_groups.append(fr)

        pooled_bias = calculate_bias_metrics(all_residuals, all_actuals, cfg)
        var_metrics = calculate_variance_stability(fold_residual_groups, cfg)

        ev_status = "ok" if len(all_residuals) >= cfg.MIN_RESIDUALS_FOR_BIAS else "insufficient"
        bias_status, bias_reasons = classify_bias_status(
            {"evidence_status": ev_status, **pooled_bias}, cfg
        )

        # Fold variance dispersion cols live in _FOLD_VARIANCE_COLS
        row = {
            "evaluation_run_id": evaluation_run_id,
            "report_id": g["report_id"],
            "report_name": first_row.get("report_name"),
            "model_family": g["model_family"],
            "model_name": g["model_name"],
            "candidate_m": g["candidate_m"],
            **stability,
            **{k: var_metrics.get(k) for k in _FOLD_VARIANCE_COLS},
            **pooled_bias,
            "bias_status": bias_status,
            "bias_reasons": "; ".join(bias_reasons),
            "practical_bias_flag": bias_status in ("underforecasting", "overforecasting", "unstable", "poor"),
        }
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=BACKTEST_SUMMARY_BIAS_COLS)

    result = pd.DataFrame(rows)
    for col in BACKTEST_SUMMARY_BIAS_COLS:
        if col not in result.columns:
            result[col] = None
    return result[BACKTEST_SUMMARY_BIAS_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Production bias diagnostics
# ---------------------------------------------------------------------------


def _deduplicate_production(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Shortest-horizon per (report_id, forecast_date); ties by latest generated_at."""
    if df.empty:
        return df, 0
    if "horizon_step" not in df.columns:
        return df, 0
    sort_cols = ["report_id", "forecast_date", "horizon_step"]
    ascending = [True, True, True]
    if "generated_at" in df.columns:
        sort_cols.append("generated_at")
        ascending.append(False)
    sorted_df = df.sort_values(sort_cols, ascending=ascending)
    deduped = sorted_df.drop_duplicates(subset=["report_id", "forecast_date"], keep="first")
    excluded = len(df) - len(deduped)
    return deduped.reset_index(drop=True), excluded


def build_production_bias_diagnostics(
    production_df: pd.DataFrame,
    cfg: BiasStabilityConfig = _DEFAULT_CFG,
    evaluation_run_id: str = "",
) -> pd.DataFrame:
    """Build production bias and stability diagnostics.

    Calculates two distinct bias measures:
    - all_records: uses every valid realized forecast row (evaluates every
      forecast made, including repeat forecasts for the same target date).
    - deduped (shortest-horizon per forecast_date): used for temporal variance
      analysis and as the primary time-series evidence.

    Production variance window comparison uses the deduplicated series only,
    comparing equal-length recent vs previous windows sorted by forecast_date.

    Parameters
    ----------
    production_df:
        DataFrame matching PRODUCTION_FORECAST_ERRORS_COLS schema.
    cfg, evaluation_run_id:
        As above.

    Returns
    -------
    DataFrame with PRODUCTION_BIAS_COLS columns.
    """
    if production_df.empty:
        return pd.DataFrame(columns=PRODUCTION_BIAS_COLS)

    group_keys = [
        "report_id", "selected_model_family", "selected_model_name", "selected_m",
    ]
    missing = [k for k in group_keys if k not in production_df.columns]
    if missing:
        _log.warning("Production bias: missing group columns %s", missing)
        return pd.DataFrame(columns=PRODUCTION_BIAS_COLS)

    rows: list[dict] = []
    for group_vals, grp in production_df.groupby(group_keys, dropna=False):
        g = dict(zip(group_keys, group_vals if isinstance(group_vals, tuple) else (group_vals,)))
        first_row = grp.iloc[0]

        try:
            # All-records bias
            r_all, a_all, excl_all, total_all = _filter_valid(
                grp, residual_col="residual", actual_col="actual",
            )
            all_bias = calculate_bias_metrics(r_all, a_all, cfg)
            all_records_norm = all_bias.get("normalized_bias")
            all_records_mean = all_bias.get("mean_residual")

            # Deduplicated series for temporal / variance analysis
            deduped_grp, excluded_overlap = _deduplicate_production(grp)
            r_deduped, a_deduped, excl_deduped, total_deduped = _filter_valid(
                deduped_grp, residual_col="residual", actual_col="actual",
            )
            deduped_bias = calculate_bias_metrics(r_deduped, a_deduped, cfg)

            # Use all-records residuals as primary evidence source
            ev = _evidence_block(
                grp, r_all, total_all, excl_all, "production",
                date_col="forecast_date", cfg=cfg,
            )

            # Horizon bias (all records, to cover all horizon steps)
            if "horizon_step" in grp.columns:
                valid_mask_all = grp["residual_observation_valid"].fillna(False).astype(bool)
                v_grp = grp[valid_mask_all]
                all_r2 = np.asarray(v_grp["residual"], dtype=float)
                all_a2 = np.asarray(v_grp["actual"], dtype=float)
                all_h2 = np.asarray(v_grp["horizon_step"], dtype=float)
                fin2 = np.isfinite(all_r2)
                horiz = calculate_horizon_bias(all_r2[fin2], all_a2[fin2], all_h2[fin2], cfg)
            else:
                horiz = {k: None for k in _HORIZON_BIAS_COLS}

            # Variance: deduplicated sorted series → recent vs previous windows
            recent_window = None
            previous_window = None
            recent_dates_tup = None
            previous_dates_tup = None

            if len(r_deduped) >= 2 * cfg.MIN_RESIDUALS_PER_VARIANCE_WINDOW:
                # Sort by forecast_date
                if "forecast_date" in deduped_grp.columns:
                    sort_idx = deduped_grp["forecast_date"].argsort().values
                else:
                    sort_idx = np.arange(len(deduped_grp))
                sorted_deduped = deduped_grp.iloc[sort_idx]
                valid_deduped_mask = sorted_deduped["residual_observation_valid"].fillna(False).astype(bool)
                sorted_valid = sorted_deduped[valid_deduped_mask].reset_index(drop=True)
                r_sorted = np.asarray(sorted_valid["residual"], dtype=float)
                r_sorted = r_sorted[np.isfinite(r_sorted)]
                n_sorted = len(r_sorted)
                wsize = cfg.PRODUCTION_VARIANCE_WINDOW_SIZE

                if n_sorted >= 2 * wsize:
                    recent_window = r_sorted[-wsize:]
                    previous_window = r_sorted[-(2 * wsize):-wsize]
                    if "forecast_date" in sorted_valid.columns:
                        dates_col = pd.to_datetime(sorted_valid["forecast_date"])
                        valid_dates = dates_col[np.isfinite(np.asarray(sorted_valid["residual"], dtype=float))]
                        valid_dates = valid_dates.reset_index(drop=True)
                        if len(valid_dates) >= 2 * wsize:
                            recent_dates_tup = (
                                valid_dates.iloc[-(wsize)],
                                valid_dates.iloc[-1],
                            )
                            previous_dates_tup = (
                                valid_dates.iloc[-(2 * wsize)],
                                valid_dates.iloc[-(wsize + 1)],
                            )

            var_metrics = calculate_variance_stability(
                [r_deduped] if len(r_deduped) >= cfg.MIN_RESIDUALS_FOR_VARIANCE else [],
                cfg,
                recent_window=recent_window,
                previous_window=previous_window,
                recent_dates=recent_dates_tup,
                previous_dates=previous_dates_tup,
            )

            bias_status, bias_reasons = classify_bias_status({**ev, **all_bias}, cfg)

            row = {
                "evaluation_run_id": evaluation_run_id,
                "report_id": g["report_id"],
                "report_name": first_row.get("report_name"),
                "selected_model_family": g["selected_model_family"],
                "selected_model_name": g["selected_model_name"],
                "selected_m": g["selected_m"],
                "lineage_complete": first_row.get("lineage_complete"),
                **ev,
                **all_bias,
                "all_records_mean_residual": all_records_mean,
                "all_records_normalized_bias": all_records_norm,
                "all_records_valid_count": len(r_all),
                "deduped_mean_residual": deduped_bias.get("mean_residual"),
                "deduped_normalized_bias": deduped_bias.get("normalized_bias"),
                "deduped_valid_count": len(r_deduped),
                **horiz,
                **var_metrics,
                "bias_status": bias_status,
                "bias_reasons": "; ".join(bias_reasons),
                "practical_bias_flag": bias_status in ("underforecasting", "overforecasting", "unstable", "poor"),
            }
        except Exception as exc:
            _log.warning("Production bias failed for group %s: %s", g, exc)
            row = {
                "evaluation_run_id": evaluation_run_id,
                "report_id": g.get("report_id"),
                "bias_status": "calculation_failed",
                "bias_reasons": f"Calculation failed: {exc}",
                "evidence_status": "calculation_failed",
            }
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=PRODUCTION_BIAS_COLS)

    result = pd.DataFrame(rows)
    for col in PRODUCTION_BIAS_COLS:
        if col not in result.columns:
            result[col] = None
    return result[PRODUCTION_BIAS_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_bias_stability_diagnostics(
    df: pd.DataFrame,
    dataset_name: str,
) -> None:
    """Validate schema and value constraints.  Raises ValueError on failure."""
    schema_map = {
        "training": TRAINING_BIAS_COLS,
        "backtest_fold": BACKTEST_FOLD_BIAS_COLS,
        "backtest_summary": BACKTEST_SUMMARY_BIAS_COLS,
        "production": PRODUCTION_BIAS_COLS,
    }
    expected_cols = schema_map.get(dataset_name)
    if expected_cols is None:
        raise ValueError(f"Unknown dataset_name: {dataset_name!r}")

    if df.empty:
        return

    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"bias_stability_diagnostics[{dataset_name}]: missing columns {missing}"
        )

    if "bias_status" in df.columns:
        bad = df["bias_status"].dropna()
        invalid = bad[~bad.isin(_VALID_BIAS_STATUSES)]
        if not invalid.empty:
            raise ValueError(
                f"bias_stability_diagnostics[{dataset_name}]: invalid bias_status "
                f"values {invalid.unique().tolist()}"
            )

    if "variance_stability_status" in df.columns:
        bad = df["variance_stability_status"].dropna()
        invalid = bad[~bad.isin(_VALID_VARIANCE_STATUSES)]
        if not invalid.empty:
            raise ValueError(
                f"bias_stability_diagnostics[{dataset_name}]: invalid "
                f"variance_stability_status values {invalid.unique().tolist()}"
            )

    # Arithmetic: positive normalized bias cannot be labelled overforecasting
    if "normalized_bias" in df.columns and "bias_status" in df.columns:
        pos_mask = df["normalized_bias"].fillna(0) > 0.01
        neg_mask = df["normalized_bias"].fillna(0) < -0.01
        if (pos_mask & (df["bias_status"] == "overforecasting")).any():
            raise ValueError(
                f"bias_stability_diagnostics[{dataset_name}]: positive normalized_bias "
                "labelled as 'overforecasting'"
            )
        if (neg_mask & (df["bias_status"] == "underforecasting")).any():
            raise ValueError(
                f"bias_stability_diagnostics[{dataset_name}]: negative normalized_bias "
                "labelled as 'underforecasting'"
            )

    # mean_absolute_residual must be non-negative
    if "mean_absolute_residual" in df.columns:
        neg = df["mean_absolute_residual"].dropna()
        if (neg < 0).any():
            raise ValueError(
                f"bias_stability_diagnostics[{dataset_name}]: "
                "mean_absolute_residual has negative values"
            )

    # absolute_normalized_bias must equal abs(normalized_bias)
    if "normalized_bias" in df.columns and "absolute_normalized_bias" in df.columns:
        nb = df["normalized_bias"].dropna()
        anb = df["absolute_normalized_bias"].dropna()
        common = nb.index.intersection(anb.index)
        if len(common):
            if not np.allclose(
                nb.loc[common].abs().values, anb.loc[common].values, atol=1e-9
            ):
                raise ValueError(
                    f"bias_stability_diagnostics[{dataset_name}]: "
                    "absolute_normalized_bias does not equal abs(normalized_bias)"
                )

    # insufficient_evidence rows should not have acceptable status
    if "evidence_status" in df.columns and "bias_status" in df.columns:
        bad_mask = (df["evidence_status"] == "insufficient") & (
            df["bias_status"] == "acceptable"
        )
        if bad_mask.any():
            raise ValueError(
                f"bias_stability_diagnostics[{dataset_name}]: rows with "
                "evidence_status='insufficient' have bias_status='acceptable'"
            )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

_DIAG_DIR = Path("outputs") / "diagnostics"


def persist_bias_stability_diagnostics(
    training_df: pd.DataFrame,
    backtest_fold_df: pd.DataFrame,
    backtest_summary_df: pd.DataFrame,
    production_df: pd.DataFrame,
    project_root: Path,
) -> dict[str, Path | None]:
    """Validate and write all four bias stability diagnostic files.

    Files are written to ``<project_root>/outputs/diagnostics/``.
    Existing files are overwritten (derived/cached datasets; not append-only).
    Empty DataFrames are written as header-only CSV files.

    Returns
    -------
    Dict mapping dataset name → absolute Path or None on failure.
    """
    diag_dir = project_root / _DIAG_DIR
    diag_dir.mkdir(parents=True, exist_ok=True)

    datasets = {
        "training": (
            training_df,
            "training_bias_stability_diagnostics_latest.csv",
            TRAINING_BIAS_COLS,
        ),
        "backtest_fold": (
            backtest_fold_df,
            "backtest_bias_stability_by_fold_latest.csv",
            BACKTEST_FOLD_BIAS_COLS,
        ),
        "backtest_summary": (
            backtest_summary_df,
            "backtest_bias_stability_summary_latest.csv",
            BACKTEST_SUMMARY_BIAS_COLS,
        ),
        "production": (
            production_df,
            "production_bias_stability_diagnostics_latest.csv",
            PRODUCTION_BIAS_COLS,
        ),
    }

    paths: dict[str, Path | None] = {}
    for name, (df, filename, cols) in datasets.items():
        path = diag_dir / filename
        try:
            validate_bias_stability_diagnostics(df, name)
            if df.empty:
                pd.DataFrame(columns=cols).to_csv(path, index=False)
            else:
                df.to_csv(path, index=False)
            paths[name] = path
        except Exception as exc:
            _log.error(
                "Failed to persist bias_stability_diagnostics[%s]: %s", name, exc
            )
            paths[name] = None

    return paths
