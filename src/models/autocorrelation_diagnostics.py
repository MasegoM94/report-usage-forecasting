"""Residual autocorrelation diagnostics for model health monitoring.

Determines whether model residuals or out-of-sample forecast errors still
contain meaningful temporal structure, which would indicate the model has
not fully captured the signal.

Three diagnostic datasets are produced (one per residual source):

1. Training ACF diagnostics
   outputs/diagnostics/training_autocorrelation_diagnostics_latest.csv
   One row per (report, model_family, model_name, candidate_m, fold_number)
   group.

2. Backtest ACF diagnostics by fold
   outputs/diagnostics/backtest_autocorrelation_by_fold_latest.csv
   One row per (report, model, fold) — fold-level evidence only; never
   concatenates overlapping windows.

3. Backtest ACF summary (cross-fold)
   outputs/diagnostics/backtest_autocorrelation_summary_latest.csv
   One row per (report, model) — aggregated across folds.

4. Production ACF diagnostics
   outputs/diagnostics/production_autocorrelation_diagnostics_latest.csv
   One row per (report, selected_model_family, selected_model_name, selected_m).

Sign convention
---------------
All three source datasets use::

    residual = actual - forecast_or_fitted   (positive = under-forecast)

ACF is computed on residuals directly; positive lag-1 ACF indicates persistence
(under-forecasting or over-forecasting alternation).

Optional dependencies
---------------------
ACF is computed with pure NumPy and always available.

Ljung–Box and Durbin–Watson require statsmodels.  When statsmodels is not
installed, those fields are set to None / NaN and
``ljung_box_available = False``.

Public API
----------
calculate_residual_autocorrelations(residuals, lags) -> dict[int, float]
calculate_ljung_box_diagnostics(residuals, lag) -> dict
calculate_durbin_watson(residuals) -> float | None
build_training_autocorrelation_diagnostics(df, cfg) -> pd.DataFrame
build_backtest_autocorrelation_diagnostics(df, cfg) -> tuple[pd.DataFrame, pd.DataFrame]
build_production_autocorrelation_diagnostics(df, cfg) -> pd.DataFrame
classify_autocorrelation_status(metrics, cfg) -> tuple[str, list[str]]
validate_autocorrelation_diagnostics(df, dataset_name) -> None
persist_autocorrelation_diagnostics(training_df, backtest_fold_df,
                                    backtest_summary_df, production_df,
                                    project_root) -> dict[str, Path | None]
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
# Optional dependency detection
# ---------------------------------------------------------------------------

try:
    from statsmodels.stats.diagnostic import acorr_ljungbox as _sm_ljungbox
    from statsmodels.stats.stattools import durbin_watson as _sm_dw
    _STATSMODELS_AVAILABLE = True
except ImportError:
    _STATSMODELS_AVAILABLE = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class AutocorrelationConfig:
    """Thresholds and hyper-parameters for residual ACF diagnostics."""

    # Minimum valid residuals required to compute any ACF at all.
    min_residuals_for_acf: int = 14

    # Default lags to evaluate (plus candidate_m / selected_m when > 1).
    default_lags: list[int] = field(
        default_factory=lambda: [1, 2, 3, 7, 14, 28]
    )

    # Lag eligibility: residual_count >= min_residuals_for_acf AND
    # residual_count >= lag_eligibility_multiplier × lag.
    lag_eligibility_multiplier: int = 3

    # Ljung–Box lag used for the omnibus test (when statsmodels is available).
    # Fallback: largest eligible default lag.
    ljung_box_lag: Optional[int] = None  # None → auto-select

    # Classification thresholds
    # Warn when |ACF at any lag| >= this.
    acf_warning_threshold: float = 0.2
    # Flag "poor" when |ACF at any lag| >= this.
    acf_poor_threshold: float = 0.4
    # Flag statistical significance when Ljung–Box p-value < this.
    ljung_box_alpha: float = 0.05
    # Durbin–Watson bounds: 2 is ideal; flag outside [lb, ub].
    dw_warning_lower: float = 1.5
    dw_warning_upper: float = 2.5
    dw_poor_lower: float = 1.0
    dw_poor_upper: float = 3.0


_DEFAULT_CFG = AutocorrelationConfig()

# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------

_EVIDENCE_COLS = [
    "residual_count",
    "valid_residual_count",
    "excluded_invalid_count",
    "excluded_overlap_count",
    "first_residual_date",
    "last_residual_date",
    "evidence_status",
]

_ACF_COLS = [
    "lag1_autocorrelation",
    "selected_m_autocorrelation",
    "max_abs_autocorrelation",
    "max_abs_autocorrelation_lag",
    "evaluated_lags",
    "valid_lag_count",
]

_LB_COLS = [
    "ljung_box_lag",
    "ljung_box_statistic",
    "ljung_box_pvalue",
    "ljung_box_significant",
    "ljung_box_available",
]

_DW_COLS = [
    "durbin_watson",
    "durbin_watson_available",
]

_CLASSIFICATION_COLS = [
    "autocorrelation_status",
    "autocorrelation_reasons",
    "practical_autocorrelation_flag",
    "statistical_dependence_flag",
]

TRAINING_ACF_COLS: list[str] = [
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
] + _EVIDENCE_COLS + _ACF_COLS + _LB_COLS + _DW_COLS + _CLASSIFICATION_COLS

BACKTEST_FOLD_ACF_COLS: list[str] = [
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
] + _EVIDENCE_COLS + _ACF_COLS + _LB_COLS + _DW_COLS + _CLASSIFICATION_COLS

BACKTEST_SUMMARY_ACF_COLS: list[str] = [
    "evaluation_run_id",
    "report_id",
    "report_name",
    "model_family",
    "model_name",
    "candidate_m",
    # fold aggregate
    "fold_count",
    "folds_with_sufficient_evidence",
    "folds_with_autocorrelation_warning",
    "folds_with_autocorrelation_poor",
    "folds_with_acceptable_status",
    "folds_with_insufficient_evidence",
    "folds_with_calculation_failed",
    # pooled evidence (sum across folds)
    "total_residual_count",
    "total_valid_residual_count",
    # summary ACF (median across folds that had sufficient evidence)
    "median_lag1_autocorrelation",
    "median_max_abs_autocorrelation",
    "max_lag1_autocorrelation_abs",
    "max_max_abs_autocorrelation",
    # classification
    "autocorrelation_status",
    "autocorrelation_reasons",
    "practical_autocorrelation_flag",
    "statistical_dependence_flag",
]

PRODUCTION_ACF_COLS: list[str] = [
    "evaluation_run_id",
    "report_id",
    "report_name",
    "selected_model_family",
    "selected_model_name",
    "selected_m",
] + _EVIDENCE_COLS + _ACF_COLS + _LB_COLS + _DW_COLS + _CLASSIFICATION_COLS

# ---------------------------------------------------------------------------
# Core ACF computation (pure NumPy — no statsmodels)
# ---------------------------------------------------------------------------

def calculate_residual_autocorrelations(
    residuals: np.ndarray,
    lags: list[int],
) -> dict[int, float]:
    """Compute sample autocorrelation at each requested lag.

    Uses the biased estimator (denominator = n, not n-lag) for numerical
    stability with short series.  Lag-0 ACF is always 1.0 by definition
    and is never returned.

    Parameters
    ----------
    residuals:
        1-D array of residuals.  Must already be finite (caller is
        responsible for filtering).
    lags:
        Positive integer lag values to evaluate.  Lags where
        ``n < 3 * lag`` are skipped and excluded from the result dict.

    Returns
    -------
    dict mapping lag → ACF value for eligible lags only.
    """
    residuals = np.asarray(residuals, dtype=float)
    n = len(residuals)
    if n < 2:
        return {}

    mean = residuals.mean()
    centered = residuals - mean
    var = (centered ** 2).mean()
    if var == 0.0:
        return {lag: 0.0 for lag in lags if n >= max(3 * lag, 2)}

    result: dict[int, float] = {}
    for lag in lags:
        if lag <= 0:
            continue
        if n < max(3 * lag, 2):
            continue
        cov = (centered[:-lag] * centered[lag:]).mean()
        result[lag] = float(cov / var)
    return result


# ---------------------------------------------------------------------------
# Ljung–Box (optional statsmodels)
# ---------------------------------------------------------------------------

def calculate_ljung_box_diagnostics(
    residuals: np.ndarray,
    lag: int,
) -> dict:
    """Run Ljung–Box omnibus test at a single lag.

    Returns a dict with keys: ljung_box_lag, ljung_box_statistic,
    ljung_box_pvalue, ljung_box_significant, ljung_box_available.

    When statsmodels is not installed all numeric fields are None and
    ljung_box_available is False.
    """
    base: dict = {
        "ljung_box_lag": lag,
        "ljung_box_statistic": None,
        "ljung_box_pvalue": None,
        "ljung_box_significant": None,
        "ljung_box_available": _STATSMODELS_AVAILABLE,
    }
    if not _STATSMODELS_AVAILABLE:
        return base

    residuals = np.asarray(residuals, dtype=float)
    if len(residuals) < lag + 2:
        return base

    try:
        result = _sm_ljungbox(residuals, lags=[lag], return_df=True)
        stat = float(result["lb_stat"].iloc[0])
        pval = float(result["lb_pvalue"].iloc[0])
        base.update({
            "ljung_box_statistic": stat,
            "ljung_box_pvalue": pval,
            "ljung_box_significant": pval < _DEFAULT_CFG.ljung_box_alpha,
        })
    except Exception as exc:
        _log.debug("Ljung-Box failed: %s", exc)
    return base


# ---------------------------------------------------------------------------
# Durbin–Watson (optional statsmodels)
# ---------------------------------------------------------------------------

def calculate_durbin_watson(residuals: np.ndarray) -> float | None:
    """Return Durbin–Watson statistic or None when unavailable."""
    if not _STATSMODELS_AVAILABLE:
        return None
    residuals = np.asarray(residuals, dtype=float)
    if len(residuals) < 3:
        return None
    try:
        return float(_sm_dw(residuals))
    except Exception as exc:
        _log.debug("Durbin-Watson failed: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify_autocorrelation_status(
    metrics: dict,
    cfg: AutocorrelationConfig = _DEFAULT_CFG,
) -> tuple[str, list[str]]:
    """Classify autocorrelation severity from computed metrics.

    Parameters
    ----------
    metrics:
        Dict with keys: evidence_status, max_abs_autocorrelation,
        lag1_autocorrelation, ljung_box_significant,
        ljung_box_available, durbin_watson, durbin_watson_available.

    Returns
    -------
    (status, reasons)  where status in
        {"acceptable", "warning", "poor", "insufficient_evidence",
         "calculation_failed"}
    """
    evidence_status = metrics.get("evidence_status", "insufficient")

    if evidence_status in ("insufficient", "empty"):
        return "insufficient_evidence", ["insufficient residuals for ACF"]

    if evidence_status == "calculation_failed":
        return "calculation_failed", ["ACF calculation failed"]

    reasons: list[str] = []
    practical_flag = False
    statistical_flag = False

    max_acf = metrics.get("max_abs_autocorrelation")
    lag1 = metrics.get("lag1_autocorrelation")

    if max_acf is not None and np.isfinite(max_acf):
        if max_acf >= cfg.acf_poor_threshold:
            practical_flag = True
            reasons.append(
                f"max |ACF|={max_acf:.3f} ≥ poor threshold {cfg.acf_poor_threshold}"
            )
        elif max_acf >= cfg.acf_warning_threshold:
            practical_flag = True
            reasons.append(
                f"max |ACF|={max_acf:.3f} ≥ warning threshold {cfg.acf_warning_threshold}"
            )

    dw = metrics.get("durbin_watson")
    dw_avail = metrics.get("durbin_watson_available", False)
    if dw_avail and dw is not None and np.isfinite(dw):
        if dw < cfg.dw_poor_lower or dw > cfg.dw_poor_upper:
            practical_flag = True
            reasons.append(f"Durbin-Watson={dw:.3f} outside poor bounds [{cfg.dw_poor_lower}, {cfg.dw_poor_upper}]")
        elif dw < cfg.dw_warning_lower or dw > cfg.dw_warning_upper:
            practical_flag = True
            reasons.append(f"Durbin-Watson={dw:.3f} outside warning bounds [{cfg.dw_warning_lower}, {cfg.dw_warning_upper}]")

    lb_sig = metrics.get("ljung_box_significant")
    lb_avail = metrics.get("ljung_box_available", False)
    if lb_avail and lb_sig is True:
        statistical_flag = True
        pval = metrics.get("ljung_box_pvalue")
        reasons.append(f"Ljung-Box p={pval:.4f} < α={cfg.ljung_box_alpha}")

    if max_acf is not None and np.isfinite(max_acf) and max_acf >= cfg.acf_poor_threshold:
        status = "poor"
    elif dw_avail and dw is not None and np.isfinite(dw) and (
        dw < cfg.dw_poor_lower or dw > cfg.dw_poor_upper
    ):
        status = "poor"
    elif practical_flag or statistical_flag:
        status = "warning"
    else:
        status = "acceptable"

    return status, reasons


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _select_lags(candidate_m: int | None, cfg: AutocorrelationConfig) -> list[int]:
    lags = list(cfg.default_lags)
    if candidate_m and candidate_m > 1 and candidate_m not in lags:
        lags.append(candidate_m)
    return sorted(set(lags))


def _auto_lb_lag(
    eligible_lags: list[int],
    cfg: AutocorrelationConfig,
) -> int:
    """Pick Ljung-Box lag: cfg override, else largest eligible default lag."""
    if cfg.ljung_box_lag is not None:
        return cfg.ljung_box_lag
    defaults = sorted(set(cfg.default_lags) & set(eligible_lags), reverse=True)
    return defaults[0] if defaults else (eligible_lags[-1] if eligible_lags else 1)


def _filter_residuals(
    residual_series: pd.Series,
    date_series: pd.Series | None = None,
) -> tuple[np.ndarray, pd.Series | None, int, int]:
    """Return finite residuals, dates, excluded_invalid, excluded_overlap."""
    arr = np.asarray(residual_series, dtype=float)
    finite_mask = np.isfinite(arr)
    excluded_invalid = int((~finite_mask).sum())
    arr_clean = arr[finite_mask]
    dates_clean = None
    if date_series is not None:
        dates_arr = np.asarray(date_series)
        dates_clean = pd.Series(dates_arr[finite_mask])
    return arr_clean, dates_clean, excluded_invalid, 0


def _compute_acf_block(
    residuals: np.ndarray,
    lags: list[int],
    candidate_m: int | None,
    cfg: AutocorrelationConfig,
    total_count: int,
    valid_count: int,
    excluded_invalid: int,
    excluded_overlap: int,
    first_date,
    last_date,
) -> dict:
    """Compute full ACF metrics block and return as flat dict."""
    n = len(residuals)

    if n < cfg.min_residuals_for_acf:
        evidence_status = "insufficient"
        acf_map: dict[int, float] = {}
    else:
        evidence_status = "ok"
        eligible_lags = [
            lag for lag in lags
            if n >= max(cfg.min_residuals_for_acf, cfg.lag_eligibility_multiplier * lag)
        ]
        try:
            acf_map = calculate_residual_autocorrelations(residuals, eligible_lags)
        except Exception as exc:
            _log.warning("ACF calculation error: %s", exc)
            acf_map = {}
            evidence_status = "calculation_failed"

    # ACF summary fields
    lag1_acf = acf_map.get(1)
    m_acf = acf_map.get(candidate_m) if candidate_m and candidate_m > 1 else None
    if acf_map:
        max_abs_lag = max(acf_map, key=lambda k: abs(acf_map[k]))
        max_abs_acf = abs(acf_map[max_abs_lag])
    else:
        max_abs_lag = None
        max_abs_acf = None

    evaluated_lags_str = ",".join(str(k) for k in sorted(acf_map.keys()))
    valid_lag_count = len(acf_map)

    # Ljung-Box
    lb_lag: int | None = None
    lb_result: dict = {
        "ljung_box_lag": None,
        "ljung_box_statistic": None,
        "ljung_box_pvalue": None,
        "ljung_box_significant": None,
        "ljung_box_available": _STATSMODELS_AVAILABLE,
    }
    if evidence_status == "ok" and acf_map:
        eligible_lags_computed = sorted(acf_map.keys())
        lb_lag = _auto_lb_lag(eligible_lags_computed, cfg)
        lb_result = calculate_ljung_box_diagnostics(residuals, lb_lag)

    # Durbin-Watson
    dw = calculate_durbin_watson(residuals) if evidence_status == "ok" else None

    metrics_for_classify = {
        "evidence_status": evidence_status,
        "max_abs_autocorrelation": max_abs_acf,
        "lag1_autocorrelation": lag1_acf,
        "ljung_box_significant": lb_result.get("ljung_box_significant"),
        "ljung_box_pvalue": lb_result.get("ljung_box_pvalue"),
        "ljung_box_available": lb_result.get("ljung_box_available", False),
        "durbin_watson": dw,
        "durbin_watson_available": _STATSMODELS_AVAILABLE,
    }
    status, reasons = classify_autocorrelation_status(metrics_for_classify, cfg)

    return {
        # evidence
        "residual_count": total_count,
        "valid_residual_count": valid_count,
        "excluded_invalid_count": excluded_invalid,
        "excluded_overlap_count": excluded_overlap,
        "first_residual_date": first_date,
        "last_residual_date": last_date,
        "evidence_status": evidence_status,
        # ACF
        "lag1_autocorrelation": lag1_acf,
        "selected_m_autocorrelation": m_acf,
        "max_abs_autocorrelation": max_abs_acf,
        "max_abs_autocorrelation_lag": max_abs_lag,
        "evaluated_lags": evaluated_lags_str,
        "valid_lag_count": valid_lag_count,
        # Ljung-Box
        "ljung_box_lag": lb_result.get("ljung_box_lag"),
        "ljung_box_statistic": lb_result.get("ljung_box_statistic"),
        "ljung_box_pvalue": lb_result.get("ljung_box_pvalue"),
        "ljung_box_significant": lb_result.get("ljung_box_significant"),
        "ljung_box_available": lb_result.get("ljung_box_available", _STATSMODELS_AVAILABLE),
        # DW
        "durbin_watson": dw,
        "durbin_watson_available": _STATSMODELS_AVAILABLE,
        # Classification
        "autocorrelation_status": status,
        "autocorrelation_reasons": "; ".join(reasons) if reasons else "",
        "practical_autocorrelation_flag": (
            status in ("warning", "poor")
        ),
        "statistical_dependence_flag": bool(
            lb_result.get("ljung_box_significant") is True
        ),
    }


# ---------------------------------------------------------------------------
# Training ACF diagnostics
# ---------------------------------------------------------------------------

def build_training_autocorrelation_diagnostics(
    training_df: pd.DataFrame,
    cfg: AutocorrelationConfig = _DEFAULT_CFG,
    diagnostic_run_id: str = "",
) -> pd.DataFrame:
    """Build training ACF diagnostics from canonical training residuals dataset.

    Groups by (report_id, model_family, model_name, candidate_m, fit_scope,
    fold_number).  Keeps only rows where ``residual_observation_valid == True``.

    Parameters
    ----------
    training_df:
        DataFrame matching TRAINING_RESIDUALS_COLS schema (from
        residual_datasets.build_training_residual_dataset).
    cfg:
        Configuration thresholds.
    diagnostic_run_id:
        Identifier for this diagnostics run.

    Returns
    -------
    DataFrame with TRAINING_ACF_COLS columns.
    """
    if training_df.empty:
        return pd.DataFrame(columns=TRAINING_ACF_COLS)

    group_keys = [
        "report_id", "model_family", "model_name", "candidate_m",
        "fit_scope", "fold_number",
    ]

    rows: list[dict] = []
    for group_vals, grp in training_df.groupby(group_keys, dropna=False):
        g = dict(zip(group_keys, group_vals if isinstance(group_vals, tuple) else (group_vals,)))

        candidate_m = g.get("candidate_m")
        try:
            candidate_m_int = int(candidate_m) if pd.notna(candidate_m) else None
        except (TypeError, ValueError):
            candidate_m_int = None

        lags = _select_lags(candidate_m_int, cfg)

        valid_rows = grp[grp["residual_observation_valid"].fillna(False).astype(bool)]
        total_count = len(grp)
        valid_count = len(valid_rows)

        residuals_raw = valid_rows["residual"] if "residual" in valid_rows.columns else pd.Series([], dtype=float)
        dates_raw = valid_rows["residual_date"] if "residual_date" in valid_rows.columns else None

        arr, dates, excl_invalid, excl_overlap = _filter_residuals(residuals_raw, dates_raw)

        first_date = dates.min() if (dates is not None and len(dates)) else None
        last_date = dates.max() if (dates is not None and len(dates)) else None

        # Pull lineage metadata from first row
        first_row = grp.iloc[0]

        acf_block = _compute_acf_block(
            residuals=arr,
            lags=lags,
            candidate_m=candidate_m_int,
            cfg=cfg,
            total_count=total_count,
            valid_count=valid_count,
            excluded_invalid=excl_invalid,
            excluded_overlap=excl_overlap,
            first_date=first_date,
            last_date=last_date,
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
            **acf_block,
        }
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=TRAINING_ACF_COLS)

    result = pd.DataFrame(rows)
    for col in TRAINING_ACF_COLS:
        if col not in result.columns:
            result[col] = None
    return result[TRAINING_ACF_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Backtest ACF diagnostics (by-fold + cross-fold summary)
# ---------------------------------------------------------------------------

def build_backtest_autocorrelation_diagnostics(
    backtest_df: pd.DataFrame,
    cfg: AutocorrelationConfig = _DEFAULT_CFG,
    evaluation_run_id: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build per-fold and cross-fold summary backtest ACF diagnostics.

    ACF is calculated within each fold separately to avoid concatenating
    overlapping windows.  Cross-fold summary aggregates fold-level evidence.

    Parameters
    ----------
    backtest_df:
        DataFrame matching BACKTEST_FORECAST_ERRORS_COLS schema.
    cfg:
        Configuration thresholds.
    evaluation_run_id:
        Identifier for this run.

    Returns
    -------
    (fold_df, summary_df) — one row per (report, model, fold) and one row
    per (report, model) respectively.
    """
    if backtest_df.empty:
        return (
            pd.DataFrame(columns=BACKTEST_FOLD_ACF_COLS),
            pd.DataFrame(columns=BACKTEST_SUMMARY_ACF_COLS),
        )

    fold_group_keys = [
        "report_id", "model_family", "model_name", "candidate_m", "fold_number",
    ]

    fold_rows: list[dict] = []
    for group_vals, grp in backtest_df.groupby(fold_group_keys, dropna=False):
        g = dict(zip(fold_group_keys, group_vals if isinstance(group_vals, tuple) else (group_vals,)))

        candidate_m = g.get("candidate_m")
        try:
            candidate_m_int = int(candidate_m) if pd.notna(candidate_m) else None
        except (TypeError, ValueError):
            candidate_m_int = None

        lags = _select_lags(candidate_m_int, cfg)

        valid_rows = grp[grp["residual_observation_valid"].fillna(False).astype(bool)]
        total_count = len(grp)
        valid_count = len(valid_rows)

        residuals_raw = valid_rows["residual"] if "residual" in valid_rows.columns else pd.Series([], dtype=float)
        dates_raw = valid_rows["forecast_date"] if "forecast_date" in valid_rows.columns else None

        arr, dates, excl_invalid, excl_overlap = _filter_residuals(residuals_raw, dates_raw)
        first_date = dates.min() if (dates is not None and len(dates)) else None
        last_date = dates.max() if (dates is not None and len(dates)) else None

        first_row = grp.iloc[0]

        acf_block = _compute_acf_block(
            residuals=arr,
            lags=lags,
            candidate_m=candidate_m_int,
            cfg=cfg,
            total_count=total_count,
            valid_count=valid_count,
            excluded_invalid=excl_invalid,
            excluded_overlap=excl_overlap,
            first_date=first_date,
            last_date=last_date,
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
            **acf_block,
        }
        fold_rows.append(row)

    fold_df: pd.DataFrame
    if not fold_rows:
        fold_df = pd.DataFrame(columns=BACKTEST_FOLD_ACF_COLS)
    else:
        fold_df = pd.DataFrame(fold_rows)
        for col in BACKTEST_FOLD_ACF_COLS:
            if col not in fold_df.columns:
                fold_df[col] = None
        fold_df = fold_df[BACKTEST_FOLD_ACF_COLS].reset_index(drop=True)

    # Cross-fold summary
    summary_group_keys = ["report_id", "model_family", "model_name", "candidate_m"]
    summary_rows: list[dict] = []

    if not fold_df.empty:
        for group_vals, grp in fold_df.groupby(summary_group_keys, dropna=False):
            g = dict(zip(summary_group_keys, group_vals if isinstance(group_vals, tuple) else (group_vals,)))

            sufficient = grp["evidence_status"] == "ok"
            fold_count = len(grp)
            folds_sufficient = int(sufficient.sum())
            statuses = grp["autocorrelation_status"]
            folds_warn = int((statuses == "warning").sum())
            folds_poor = int((statuses == "poor").sum())
            folds_acceptable = int((statuses == "acceptable").sum())
            folds_insuff = int((statuses == "insufficient_evidence").sum())
            folds_failed = int((statuses == "calculation_failed").sum())

            total_residuals = int(grp["residual_count"].sum())
            total_valid = int(grp["valid_residual_count"].sum())

            sufficient_grp = grp[sufficient]
            if len(sufficient_grp):
                med_lag1 = float(sufficient_grp["lag1_autocorrelation"].dropna().median()) if sufficient_grp["lag1_autocorrelation"].notna().any() else None
                med_max = float(sufficient_grp["max_abs_autocorrelation"].dropna().median()) if sufficient_grp["max_abs_autocorrelation"].notna().any() else None
                max_lag1_abs = float(sufficient_grp["lag1_autocorrelation"].abs().max()) if sufficient_grp["lag1_autocorrelation"].notna().any() else None
                max_max = float(sufficient_grp["max_abs_autocorrelation"].max()) if sufficient_grp["max_abs_autocorrelation"].notna().any() else None
            else:
                med_lag1 = med_max = max_lag1_abs = max_max = None

            # Classify summary
            if folds_sufficient == 0:
                sum_status = "insufficient_evidence"
                sum_reasons = ["no folds had sufficient evidence"]
                practical = False
                statistical = False
            else:
                practical = bool(max_max is not None and max_max >= cfg.acf_warning_threshold)
                statistical = bool(grp["statistical_dependence_flag"].any())
                if max_max is not None and max_max >= cfg.acf_poor_threshold:
                    sum_status = "poor"
                    sum_reasons = [f"cross-fold max |ACF|={max_max:.3f} ≥ poor threshold"]
                elif practical or statistical:
                    sum_status = "warning"
                    sum_reasons = []
                    if practical:
                        sum_reasons.append(f"cross-fold max |ACF|={max_max:.3f} ≥ warning threshold")
                    if statistical:
                        sum_reasons.append("Ljung-Box significant in at least one fold")
                else:
                    sum_status = "acceptable"
                    sum_reasons = []

            first_row = grp.iloc[0]
            summary_rows.append({
                "evaluation_run_id": evaluation_run_id,
                "report_id": g["report_id"],
                "report_name": first_row.get("report_name"),
                "model_family": g["model_family"],
                "model_name": g["model_name"],
                "candidate_m": g["candidate_m"],
                "fold_count": fold_count,
                "folds_with_sufficient_evidence": folds_sufficient,
                "folds_with_autocorrelation_warning": folds_warn,
                "folds_with_autocorrelation_poor": folds_poor,
                "folds_with_acceptable_status": folds_acceptable,
                "folds_with_insufficient_evidence": folds_insuff,
                "folds_with_calculation_failed": folds_failed,
                "total_residual_count": total_residuals,
                "total_valid_residual_count": total_valid,
                "median_lag1_autocorrelation": med_lag1,
                "median_max_abs_autocorrelation": med_max,
                "max_lag1_autocorrelation_abs": max_lag1_abs,
                "max_max_abs_autocorrelation": max_max,
                "autocorrelation_status": sum_status,
                "autocorrelation_reasons": "; ".join(sum_reasons),
                "practical_autocorrelation_flag": practical,
                "statistical_dependence_flag": statistical,
            })

    summary_df: pd.DataFrame
    if not summary_rows:
        summary_df = pd.DataFrame(columns=BACKTEST_SUMMARY_ACF_COLS)
    else:
        summary_df = pd.DataFrame(summary_rows)
        for col in BACKTEST_SUMMARY_ACF_COLS:
            if col not in summary_df.columns:
                summary_df[col] = None
        summary_df = summary_df[BACKTEST_SUMMARY_ACF_COLS].reset_index(drop=True)

    return fold_df, summary_df


# ---------------------------------------------------------------------------
# Production ACF diagnostics
# ---------------------------------------------------------------------------

def _deduplicate_production_residuals(
    prod_df: pd.DataFrame,
) -> tuple[pd.DataFrame, int]:
    """Select shortest-horizon forecast per (report_id, forecast_date).

    Ties broken by latest generated_at.  Returns (deduped_df, excluded_count).
    """
    if prod_df.empty:
        return prod_df, 0

    cols_needed = {"report_id", "forecast_date"}
    if not cols_needed.issubset(prod_df.columns):
        return prod_df, 0

    if "horizon_step" in prod_df.columns:
        sort_cols = ["report_id", "forecast_date", "horizon_step"]
        ascending = [True, True, True]
    else:
        sort_cols = ["report_id", "forecast_date"]
        ascending = [True, True]

    if "generated_at" in prod_df.columns:
        sort_cols.append("generated_at")
        ascending.append(False)

    sorted_df = prod_df.sort_values(sort_cols, ascending=ascending)
    deduped = sorted_df.drop_duplicates(subset=["report_id", "forecast_date"], keep="first")
    excluded = len(prod_df) - len(deduped)
    return deduped.reset_index(drop=True), excluded


def build_production_autocorrelation_diagnostics(
    production_df: pd.DataFrame,
    cfg: AutocorrelationConfig = _DEFAULT_CFG,
    evaluation_run_id: str = "",
) -> pd.DataFrame:
    """Build production ACF diagnostics from canonical production forecast errors.

    Deduplicates to shortest-horizon forecast per (report, forecast_date)
    before computing ACF.  Groups by (report_id, selected_model_family,
    selected_model_name, selected_m).

    Parameters
    ----------
    production_df:
        DataFrame matching PRODUCTION_FORECAST_ERRORS_COLS schema.
    cfg:
        Configuration thresholds.
    evaluation_run_id:
        Identifier for this run.

    Returns
    -------
    DataFrame with PRODUCTION_ACF_COLS columns.
    """
    if production_df.empty:
        return pd.DataFrame(columns=PRODUCTION_ACF_COLS)

    group_keys = [
        "report_id", "selected_model_family", "selected_model_name", "selected_m",
    ]
    missing = [k for k in group_keys if k not in production_df.columns]
    if missing:
        _log.warning("Production ACF: missing group columns %s", missing)
        return pd.DataFrame(columns=PRODUCTION_ACF_COLS)

    rows: list[dict] = []
    for group_vals, grp in production_df.groupby(group_keys, dropna=False):
        g = dict(zip(group_keys, group_vals if isinstance(group_vals, tuple) else (group_vals,)))

        selected_m = g.get("selected_m")
        try:
            m_int = int(selected_m) if pd.notna(selected_m) else None
        except (TypeError, ValueError):
            m_int = None

        lags = _select_lags(m_int, cfg)

        deduped_grp, excluded_overlap = _deduplicate_production_residuals(grp)
        valid_rows = deduped_grp[
            deduped_grp["residual_observation_valid"].fillna(False).astype(bool)
        ] if "residual_observation_valid" in deduped_grp.columns else deduped_grp

        total_count = len(deduped_grp)
        valid_count = len(valid_rows)

        residuals_raw = valid_rows["residual"] if "residual" in valid_rows.columns else pd.Series([], dtype=float)
        dates_raw = valid_rows["forecast_date"] if "forecast_date" in valid_rows.columns else None

        arr, dates, excl_invalid, _ = _filter_residuals(residuals_raw, dates_raw)
        first_date = dates.min() if (dates is not None and len(dates)) else None
        last_date = dates.max() if (dates is not None and len(dates)) else None

        first_row = grp.iloc[0]

        acf_block = _compute_acf_block(
            residuals=arr,
            lags=lags,
            candidate_m=m_int,
            cfg=cfg,
            total_count=total_count,
            valid_count=valid_count,
            excluded_invalid=excl_invalid,
            excluded_overlap=excluded_overlap,
            first_date=first_date,
            last_date=last_date,
        )

        row = {
            "evaluation_run_id": evaluation_run_id,
            "report_id": g["report_id"],
            "report_name": first_row.get("report_name"),
            "selected_model_family": g["selected_model_family"],
            "selected_model_name": g["selected_model_name"],
            "selected_m": g["selected_m"],
            **acf_block,
        }
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=PRODUCTION_ACF_COLS)

    result = pd.DataFrame(rows)
    for col in PRODUCTION_ACF_COLS:
        if col not in result.columns:
            result[col] = None
    return result[PRODUCTION_ACF_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

_VALID_STATUSES = frozenset({
    "acceptable", "warning", "poor",
    "insufficient_evidence", "calculation_failed",
})


def validate_autocorrelation_diagnostics(
    df: pd.DataFrame,
    dataset_name: str,
) -> None:
    """Validate schema and value constraints.  Raises ValueError on failure."""
    schema_map = {
        "training": TRAINING_ACF_COLS,
        "backtest_fold": BACKTEST_FOLD_ACF_COLS,
        "backtest_summary": BACKTEST_SUMMARY_ACF_COLS,
        "production": PRODUCTION_ACF_COLS,
    }

    expected_cols = schema_map.get(dataset_name)
    if expected_cols is None:
        raise ValueError(f"Unknown dataset_name: {dataset_name!r}")

    if df.empty:
        return

    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"autocorrelation_diagnostics[{dataset_name}]: missing columns {missing}"
        )

    if "autocorrelation_status" in df.columns:
        bad = df["autocorrelation_status"].dropna()
        invalid = bad[~bad.isin(_VALID_STATUSES)]
        if not invalid.empty:
            raise ValueError(
                f"autocorrelation_diagnostics[{dataset_name}]: invalid statuses "
                f"{invalid.unique().tolist()}"
            )

    for col in ("lag1_autocorrelation", "max_abs_autocorrelation",
                "selected_m_autocorrelation"):
        if col in df.columns:
            finite_vals = df[col].dropna()
            if (finite_vals.abs() > 1.0 + 1e-6).any():
                raise ValueError(
                    f"autocorrelation_diagnostics[{dataset_name}]: {col} "
                    f"contains values outside [-1, 1]"
                )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

_DIAG_DIR = Path("outputs") / "diagnostics"


def persist_autocorrelation_diagnostics(
    training_df: pd.DataFrame,
    backtest_fold_df: pd.DataFrame,
    backtest_summary_df: pd.DataFrame,
    production_df: pd.DataFrame,
    project_root: Path,
) -> dict[str, Path | None]:
    """Validate and write all four autocorrelation diagnostic files.

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
            "training_autocorrelation_diagnostics_latest.csv",
            TRAINING_ACF_COLS,
        ),
        "backtest_fold": (
            backtest_fold_df,
            "backtest_autocorrelation_by_fold_latest.csv",
            BACKTEST_FOLD_ACF_COLS,
        ),
        "backtest_summary": (
            backtest_summary_df,
            "backtest_autocorrelation_summary_latest.csv",
            BACKTEST_SUMMARY_ACF_COLS,
        ),
        "production": (
            production_df,
            "production_autocorrelation_diagnostics_latest.csv",
            PRODUCTION_ACF_COLS,
        ),
    }

    paths: dict[str, Path | None] = {}
    for name, (df, filename, cols) in datasets.items():
        path = diag_dir / filename
        try:
            validate_autocorrelation_diagnostics(df, name)
            if df.empty:
                pd.DataFrame(columns=cols).to_csv(path, index=False)
            else:
                df.to_csv(path, index=False)
            paths[name] = path
        except Exception as exc:
            _log.error("Failed to persist autocorrelation diagnostics[%s]: %s", name, exc)
            paths[name] = None

    return paths
