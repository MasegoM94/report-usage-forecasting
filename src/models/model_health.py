"""Report-level model-health summary.

Purpose
-------
Consolidates the five Sprint-5 diagnostic components into one validated
per-report summary row, answering nine operational questions:

1. Is the selected model leaving important temporal structure unexplained?
2. Is it systematically overforecasting or underforecasting?
3. Are residual variance and bias stable?
4. Are large misses frequent or severe?
5. Are residuals strongly skewed or heavy-tailed?
6. Are prediction intervals calibrated and useful?
7. Is production accuracy deteriorating?
8. Is there enough evidence to assess model health?
9. What model-review action is warranted?

Evidence sources and precedence
--------------------------------
Backtest diagnostics are the primary model-health source because they reflect
out-of-sample forecasting performance.  Training diagnostics capture in-sample
adequacy.  Production diagnostics provide current operational evidence but are
used only when sufficient realized observations exist; otherwise the backtest
is the primary basis and the production fields are preserved as-is.

Component diagnostic priorities (highest to lowest):

1. Severe production deterioration with sufficient evidence
2. Poor backtest residual autocorrelation
3. Poor practical backtest bias
4. Severe interval undercoverage or unusable intervals
5. Severe residual variance instability
6. Repeated outlier-heavy backtest folds
7. Production bias or calibration problems
8. Training residual warnings
9. Distribution non-normality alone

Non-normality alone does not make a model ``poor``.  One significant p-value
from a Ljung–Box, Jarque–Bera, or Shapiro–Wilk test does not override
materially stronger practical evidence.

Classification rules
--------------------
healthy
    Backtest evidence is sufficient, no component has poor status,
    warning count is below the configured limit, no material production
    deterioration exists, interval calibration is acceptable (or intervals
    are unavailable but not required for that model), and lineage is sufficient.
    A model with insufficient evidence must not be called healthy.

watch
    One or more warning-level issues exist, production evidence is limited
    but backtest evidence is acceptable, or modest practical issues remain.

poor
    At least one critical or poor issue exists; or multiple poor/warning
    signals combine; or severe production deterioration is confirmed.

insufficient_evidence
    Backtest evidence is below the minimum, no production evidence is
    available, or lineage is insufficient for selected-model attribution.

calculation_failed
    Diagnostic calculation failed despite otherwise valid source inputs.

Automatic retraining
--------------------
``automatic_retraining_triggered`` is always ``False`` in this sprint.
The recommended action ``consider_retraining`` is a signal for human review,
not an automatic pipeline action.

Output file
-----------
``outputs/diagnostics/report_model_diagnostics_latest.csv``

Grain
-----
One row per ``(diagnostic_run_id, report_id)``.

Public API
----------
MODEL_HEALTH_COLS : list[str]
    Canonical output column order.

build_report_model_diagnostics(
    production_forecast_df, training_acf_df, backtest_acf_summary_df,
    production_acf_df, training_bias_df, backtest_bias_summary_df,
    production_bias_df, training_outlier_df, backtest_outlier_summary_df,
    production_outlier_df, backtest_interval_summary_df,
    production_interval_df, deterioration_df, cfg, diagnostic_run_id
) -> pd.DataFrame

classify_model_diagnostic_status(issues, cfg) -> str

determine_recommended_model_action(issues, status, cfg) -> tuple[str, str, bool]

build_model_diagnostic_reasons(issues, sources, status, action) -> list[str]

validate_report_model_diagnostics(df) -> None

persist_report_model_diagnostics(df, project_root) -> Path | None

Data dictionary
---------------
diagnostic_run_id         Pipeline run ID for this diagnostic pass.
generated_at              UTC timestamp when this summary row was created.
report_id                 Unique report identifier.
report_name               Human-readable report name.
selected_model_family     Model family of the currently deployed model (e.g. SARIMA).
selected_model_name       Full model name (e.g. sarima).
selected_m                Seasonal period used by the selected model.
training_cutoff           Last date of the training window.
selection_run_id          Run ID of the selection pass that chose this model.
lineage_complete          True when all lineage fields are populated.
lineage_missing_fields    JSON list of missing lineage fields; [] when complete.

training_residual_count           Total training residuals available.
backtest_error_count              Total backtest forecast errors available.
backtest_valid_fold_count         Number of folds with sufficient evidence.
production_error_count            Total production realized errors available.
production_completed_run_count    Number of completed production runs.
interval_observation_count        Number of valid interval observations.
diagnostic_evidence_status        Overall evidence quality label.
missing_evidence_categories       JSON list of missing diagnostic categories.
evidence_completeness_score       0.0–1.0 fraction of evidence categories present.

training_autocorrelation_status   ACF status from training residuals.
backtest_autocorrelation_status   ACF status from backtest cross-fold summary.
production_autocorrelation_status ACF status from production errors.
lag1_autocorrelation              Lag-1 ACF from production or backtest.
selected_m_autocorrelation        ACF at the selected seasonal period.
max_abs_autocorrelation           Maximum absolute ACF across evaluated lags.
ljung_box_pvalue                  Ljung–Box p-value from production diagnostics.
significant_ljung_box_fold_count  Number of backtest folds with significant LB test.
practical_autocorrelation_fold_count Number of folds with practical autocorrelation flag.
autocorrelation_reasons           Free-text reason string.

training_bias_status         Bias status from training residuals.
backtest_bias_status         Bias status from backtest cross-fold summary.
production_bias_status       Bias status from production errors.
normalized_bias              Normalized bias from the primary evidence source.
median_residual              Median residual from the primary evidence source.
fold_bias_std                Standard deviation of fold-level normalized biases.
directional_bias_fold_share  Share of folds with consistent bias direction.
horizon_bias_status          Horizon-level bias degradation status.
late_horizon_normalized_bias Normalized bias at the late horizon bucket (days 15–28).
variance_stability_status    Residual variance stability status.
variance_change_ratio        Ratio of recent to previous residual variance.
fold_variance_coefficient_of_variation CV of fold-level residual variances.
bias_stability_reasons       Free-text reason string.

training_outlier_status      Outlier status from training residuals.
backtest_outlier_status      Cross-fold outlier status from backtest.
production_outlier_status    Outlier status from production errors.
outlier_rate                 Proportion of residuals classified as outliers.
largest_absolute_residual    Magnitude of the largest single forecast error.
largest_absolute_residual_date Date of the largest error.
tail_direction               Direction of residual tail asymmetry.
residual_skewness            Third standardized moment of residual distribution.
excess_kurtosis              Excess kurtosis (kurtosis − 3).
normality_status             Normality assessment from distribution diagnostics.
outlier_distribution_reasons Free-text reason string.

backtest_calibration_status   Interval calibration status from cross-fold summary.
production_calibration_status Interval calibration status from production diagnostics.
interval_usefulness_status    Width/Winkler usefulness classification.
nominal_coverage              Stated nominal coverage level (e.g. 0.95).
observed_coverage             Empirical interval hit rate.
coverage_gap                  observed_coverage − nominal_coverage.
normalized_mean_interval_width Mean interval width divided by the normalization scale.
normalized_mean_winkler_score  Mean Winkler score divided by the normalization scale.
horizon_calibration_deterioration_flag True when coverage drops at longer horizons.
interval_calibration_reasons  Free-text reason string.

accuracy_deterioration_flag  True when recent WAPE materially exceeds previous WAPE.
deterioration_severity       Severity label for the production deterioration signal.
deterioration_reasons        Free-text reason string from deterioration module.
recent_wape                  WAPE for the most recent completed production run.
previous_wape                WAPE for the immediately preceding run.
recent_bias                  Bias for the most recent run.
previous_bias                Bias for the immediately preceding run.
recent_interval_coverage     Interval hit rate for the most recent run.
previous_interval_coverage   Interval hit rate for the preceding run.

model_diagnostic_status      healthy | watch | poor | insufficient_evidence | calculation_failed
diagnostic_issue_count       Total number of non-none component issues.
warning_issue_count          Count of warning-severity issues.
poor_issue_count             Count of poor-severity issues.
primary_model_issue          Primary issue label (deterministic priority order).
model_diagnostic_reasons     Concatenated free-text reason strings.
recommended_model_action     Action label from the allowed set.
action_priority              low | medium | high | urgent | insufficient_evidence
review_required              True when model_diagnostic_status is not healthy.
automatic_retraining_triggered Always False in this sprint.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class ModelHealthConfig:
    """Thresholds governing model-health classification."""

    # Minimum valid backtest folds required to call a model healthy.
    MIN_VALID_BACKTEST_FOLDS: int = 2
    # Minimum backtest forecast errors required.
    MIN_BACKTEST_ERROR_COUNT: int = 20
    # Minimum production errors for production evidence to influence final status.
    MIN_PRODUCTION_ERROR_COUNT: int = 10

    # Maximum warning-level issues before upgrading to watch (not poor).
    MAX_WARNINGS_FOR_HEALTHY: int = 0

    # Statuses that are treated as poor-severity in component columns.
    POOR_AUTOCORRELATION_STATUSES: frozenset = field(
        default_factory=lambda: frozenset({"poor"})
    )
    POOR_BIAS_STATUSES: frozenset = field(
        default_factory=lambda: frozenset({"poor", "severe"})
    )
    POOR_VARIANCE_STATUSES: frozenset = field(
        default_factory=lambda: frozenset({"poor", "severe"})
    )
    POOR_OUTLIER_STATUSES: frozenset = field(
        default_factory=lambda: frozenset({"poor"})
    )
    POOR_INTERVAL_STATUSES: frozenset = field(
        default_factory=lambda: frozenset(
            {"undercoverage", "severe_undercoverage", "overwide", "poor"}
        )
    )
    CRITICAL_INTERVAL_STATUSES: frozenset = field(
        default_factory=lambda: frozenset({"severe_undercoverage"})
    )

    WARNING_AUTOCORRELATION_STATUSES: frozenset = field(
        default_factory=lambda: frozenset({"warning"})
    )
    WARNING_BIAS_STATUSES: frozenset = field(
        default_factory=lambda: frozenset({"warning"})
    )
    WARNING_VARIANCE_STATUSES: frozenset = field(
        default_factory=lambda: frozenset({"warning"})
    )
    WARNING_OUTLIER_STATUSES: frozenset = field(
        default_factory=lambda: frozenset({"warning"})
    )
    WARNING_INTERVAL_STATUSES: frozenset = field(
        default_factory=lambda: frozenset(
            {"slight_undercoverage", "wide_but_usable"}
        )
    )

    # Fraction of backtest folds with poor autocorrelation that triggers poor.
    AUTOCORRELATION_POOR_FOLD_FRACTION: float = 0.5

    # consider_retraining threshold — minimum poor-severity issues.
    MIN_POOR_ISSUES_FOR_RETRAINING: int = 2

    # When multiple poor issues exist, use urgent priority.
    URGENT_POOR_ISSUE_THRESHOLD: int = 3


_DEFAULT_CFG = ModelHealthConfig()

# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

MODEL_HEALTH_COLS: list[str] = [
    # Identity / lineage
    "diagnostic_run_id",
    "generated_at",
    "report_id",
    "report_name",
    "selected_model_family",
    "selected_model_name",
    "selected_m",
    "training_cutoff",
    "selection_run_id",
    "lineage_complete",
    "lineage_missing_fields",
    # Evidence summary
    "training_residual_count",
    "backtest_error_count",
    "backtest_valid_fold_count",
    "production_error_count",
    "production_completed_run_count",
    "interval_observation_count",
    "diagnostic_evidence_status",
    "missing_evidence_categories",
    "evidence_completeness_score",
    # Source availability flags
    "training_diagnostics_available",
    "backtest_diagnostics_available",
    "production_diagnostics_available",
    "interval_diagnostics_available",
    "deterioration_evidence_available",
    # Autocorrelation
    "training_autocorrelation_status",
    "backtest_autocorrelation_status",
    "production_autocorrelation_status",
    "lag1_autocorrelation",
    "selected_m_autocorrelation",
    "max_abs_autocorrelation",
    "ljung_box_pvalue",
    "significant_ljung_box_fold_count",
    "practical_autocorrelation_fold_count",
    "autocorrelation_reasons",
    # Bias and stability
    "training_bias_status",
    "backtest_bias_status",
    "production_bias_status",
    "normalized_bias",
    "median_residual",
    "fold_bias_std",
    "directional_bias_fold_share",
    "horizon_bias_status",
    "late_horizon_normalized_bias",
    "variance_stability_status",
    "variance_change_ratio",
    "fold_variance_coefficient_of_variation",
    "bias_stability_reasons",
    # Outliers and distribution
    "training_outlier_status",
    "backtest_outlier_status",
    "production_outlier_status",
    "outlier_rate",
    "largest_absolute_residual",
    "largest_absolute_residual_date",
    "tail_direction",
    "residual_skewness",
    "excess_kurtosis",
    "normality_status",
    "outlier_distribution_reasons",
    # Prediction intervals
    "backtest_calibration_status",
    "production_calibration_status",
    "interval_usefulness_status",
    "nominal_coverage",
    "observed_coverage",
    "coverage_gap",
    "normalized_mean_interval_width",
    "normalized_mean_winkler_score",
    "horizon_calibration_deterioration_flag",
    "interval_calibration_reasons",
    # Production monitoring
    "accuracy_deterioration_flag",
    "deterioration_severity",
    "deterioration_reasons",
    "recent_wape",
    "previous_wape",
    "recent_bias",
    "previous_bias",
    "recent_interval_coverage",
    "previous_interval_coverage",
    # Final summary
    "model_diagnostic_status",
    "diagnostic_issue_count",
    "warning_issue_count",
    "poor_issue_count",
    "primary_model_issue",
    "model_diagnostic_reasons",
    "recommended_model_action",
    "action_priority",
    "review_required",
    "automatic_retraining_triggered",
]

# ---------------------------------------------------------------------------
# Allowed categorical values
# ---------------------------------------------------------------------------

_EVIDENCE_STATUSES = frozenset({
    "complete",
    "strong_backtest_limited_production",
    "partial",
    "insufficient_evidence",
    "incomplete_lineage",
    "calculation_failed",
})

_HEALTH_STATUSES = frozenset({
    "healthy",
    "watch",
    "poor",
    "insufficient_evidence",
    "calculation_failed",
})

_ALLOWED_ACTIONS = frozenset({
    "continue_monitoring",
    "review_model_specification",
    "review_seasonality",
    "investigate_bias",
    "investigate_variance_instability",
    "investigate_outliers",
    "review_interval_calibration",
    "review_production_deterioration",
    "consider_retraining",
    "insufficient_evidence",
})

_ACTION_PRIORITIES = frozenset({"low", "medium", "high", "urgent", "insufficient_evidence"})

# Priority order for primary_model_issue
_PRIMARY_ISSUE_ORDER = [
    "production_deterioration",
    "residual_autocorrelation",
    "persistent_bias",
    "interval_undercoverage",
    "variance_instability",
    "repeated_outliers",
    "interval_overwidth",
    "distribution_caution",
    "insufficient_evidence",
    "none",
]

# ---------------------------------------------------------------------------
# Issue dataclass
# ---------------------------------------------------------------------------


@dataclass
class ComponentIssue:
    """Single component diagnostic issue."""

    name: str           # e.g. "residual_autocorrelation"
    severity: str       # none | informational | warning | poor | critical
    source: str         # training | backtest | production | combined
    reason: str         # human-readable sentence


_SEVERITY_RANK = {
    "none": 0,
    "informational": 1,
    "warning": 2,
    "poor": 3,
    "critical": 4,
}


def _max_severity(issues: list[ComponentIssue]) -> str:
    if not issues:
        return "none"
    return max(issues, key=lambda i: _SEVERITY_RANK.get(i.severity, 0)).severity


# ---------------------------------------------------------------------------
# Safe scalar extraction helpers
# ---------------------------------------------------------------------------


def _scalar(val, default=None):
    """Extract a scalar from a pandas Series cell, returning default on NaN."""
    if val is None:
        return default
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    return val


def _str_scalar(val, default=None):
    v = _scalar(val)
    if v is None:
        return default
    return str(v)


def _float_scalar(val, default=None):
    v = _scalar(val)
    if v is None:
        return default
    try:
        f = float(v)
        return None if not np.isfinite(f) else f
    except (TypeError, ValueError):
        return default


def _bool_scalar(val, default=False):
    v = _scalar(val)
    if v is None:
        return default
    try:
        return bool(v)
    except (TypeError, ValueError):
        return default


def _int_scalar(val, default=0):
    v = _scalar(val)
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _first_row(df: pd.DataFrame, report_id: str,
               join_cols: list[str], join_vals: list) -> Optional[pd.Series]:
    """Return the first matching row, or None."""
    if df is None or df.empty:
        return None
    mask = pd.Series(True, index=df.index)
    for col, val in zip(join_cols, join_vals):
        if col not in df.columns:
            return None
        if val is None or (isinstance(val, float) and np.isnan(val)):
            mask &= df[col].isna()
        else:
            mask &= df[col] == val
    sub = df[mask]
    return sub.iloc[0] if len(sub) > 0 else None


# ---------------------------------------------------------------------------
# Evidence assessment
# ---------------------------------------------------------------------------


def _assess_evidence(
    report_id: str,
    spine_row: pd.Series,
    sources: dict[str, Optional[pd.Series]],
    cfg: ModelHealthConfig,
) -> tuple[str, list[str], float]:
    """Return (evidence_status, missing_categories, completeness_score)."""
    categories = [
        "training_diagnostics",
        "backtest_diagnostics",
        "production_diagnostics",
        "interval_diagnostics",
        "deterioration_evidence",
    ]
    available = {
        "training_diagnostics": sources.get("training_acf") is not None
            or sources.get("training_bias") is not None
            or sources.get("training_outlier") is not None,
        "backtest_diagnostics": sources.get("backtest_acf") is not None
            or sources.get("backtest_bias") is not None
            or sources.get("backtest_outlier") is not None,
        "production_diagnostics": sources.get("production_acf") is not None
            or sources.get("production_bias") is not None
            or sources.get("production_outlier") is not None,
        "interval_diagnostics": sources.get("backtest_interval") is not None
            or sources.get("production_interval") is not None,
        "deterioration_evidence": sources.get("deterioration") is not None,
    }
    missing = sorted([c for c, ok in available.items() if not ok])
    score = sum(available.values()) / len(categories)

    lineage_ok = _bool_scalar(spine_row.get("lineage_complete"), True)
    if not lineage_ok:
        return "incomplete_lineage", missing, score

    bt_bias = sources.get("backtest_bias")
    bt_folds = _int_scalar(bt_bias.get("valid_fold_count") if bt_bias is not None else None, 0)
    bt_count = _int_scalar(bt_bias.get("total_fold_count") if bt_bias is not None else None, 0)

    if bt_folds < cfg.MIN_VALID_BACKTEST_FOLDS:
        return "insufficient_evidence", missing, score

    prod_bias = sources.get("production_bias")
    prod_count = _int_scalar(
        prod_bias.get("valid_residual_count") if prod_bias is not None else None, 0
    )

    if not available["backtest_diagnostics"]:
        return "insufficient_evidence", missing, score

    if not available["production_diagnostics"] or prod_count < cfg.MIN_PRODUCTION_ERROR_COUNT:
        return "strong_backtest_limited_production", missing, score

    if missing:
        return "partial", missing, score

    return "complete", missing, score


# ---------------------------------------------------------------------------
# Component issue detectors
# ---------------------------------------------------------------------------


def _detect_autocorrelation_issues(
    sources: dict,
    cfg: ModelHealthConfig,
) -> list[ComponentIssue]:
    issues: list[ComponentIssue] = []

    bt = sources.get("backtest_acf")
    prod = sources.get("production_acf")
    tr = sources.get("training_acf")

    # Training (informational only)
    if tr is not None:
        tr_status = _str_scalar(tr.get("autocorrelation_status"))
        if tr_status in cfg.POOR_AUTOCORRELATION_STATUSES:
            issues.append(ComponentIssue(
                "residual_autocorrelation", "informational", "training",
                "Training residuals show autocorrelation (in-sample only).",
            ))
        elif tr_status in cfg.WARNING_AUTOCORRELATION_STATUSES:
            issues.append(ComponentIssue(
                "residual_autocorrelation", "informational", "training",
                "Training residuals show mild autocorrelation (in-sample only).",
            ))

    # Backtest — primary source
    if bt is not None:
        n_poor = _int_scalar(bt.get("folds_with_autocorrelation_poor"), 0)
        n_warn = _int_scalar(bt.get("folds_with_autocorrelation_warning"), 0)
        n_folds = _int_scalar(bt.get("fold_count"), 0)
        n_practical = _int_scalar(bt.get("practical_autocorrelation_flag"), 0)
        bt_status = _str_scalar(bt.get("autocorrelation_status"))

        # Practical flag check: if the column is boolean, treat as count of folds flagged
        # (In the summary schema it may be aggregated as count or median)
        if bt_status in cfg.POOR_AUTOCORRELATION_STATUSES:
            poor_fraction = n_poor / n_folds if n_folds > 0 else 0.0
            severity = "poor" if poor_fraction >= cfg.AUTOCORRELATION_POOR_FOLD_FRACTION else "warning"
            issues.append(ComponentIssue(
                "residual_autocorrelation", severity, "backtest",
                f"Backtest residuals show autocorrelation in {n_poor} of {n_folds} folds "
                f"(poor-status folds: {poor_fraction:.0%}).",
            ))
        elif bt_status in cfg.WARNING_AUTOCORRELATION_STATUSES or n_warn > 0:
            issues.append(ComponentIssue(
                "residual_autocorrelation", "warning", "backtest",
                f"Mild autocorrelation observed in {n_warn} backtest fold(s).",
            ))

    # Production — supplementary
    if prod is not None:
        prod_status = _str_scalar(prod.get("autocorrelation_status"))
        if prod_status in cfg.POOR_AUTOCORRELATION_STATUSES:
            issues.append(ComponentIssue(
                "residual_autocorrelation", "poor", "production",
                "Production forecast errors show autocorrelation.",
            ))
        elif prod_status in cfg.WARNING_AUTOCORRELATION_STATUSES:
            issues.append(ComponentIssue(
                "residual_autocorrelation", "warning", "production",
                "Mild autocorrelation observed in production forecast errors.",
            ))

    return issues


def _detect_bias_issues(
    sources: dict,
    cfg: ModelHealthConfig,
) -> list[ComponentIssue]:
    issues: list[ComponentIssue] = []

    bt = sources.get("backtest_bias")
    prod = sources.get("production_bias")
    tr = sources.get("training_bias")

    # Training (informational)
    if tr is not None:
        tr_status = _str_scalar(tr.get("bias_status"))
        if tr_status in cfg.POOR_BIAS_STATUSES | cfg.WARNING_BIAS_STATUSES:
            issues.append(ComponentIssue(
                "persistent_bias", "informational", "training",
                f"Training residuals indicate {tr_status} bias (in-sample).",
            ))

    # Backtest — primary
    if bt is not None:
        bt_status = _str_scalar(bt.get("bias_status"))
        nb = _float_scalar(bt.get("normalized_bias"))
        dir_share = _float_scalar(bt.get("directional_bias_fold_share"))
        bias_dir = _str_scalar(bt.get("bias_direction"), "unknown")

        if bt_status in cfg.POOR_BIAS_STATUSES:
            nb_str = f" ({nb:+.1%})" if nb is not None else ""
            issues.append(ComponentIssue(
                "persistent_bias", "poor", "backtest",
                f"The model persistently {bias_dir}s across backtest folds"
                f"{nb_str}.",
            ))
        elif bt_status in cfg.WARNING_BIAS_STATUSES:
            issues.append(ComponentIssue(
                "persistent_bias", "warning", "backtest",
                f"Modest systematic {bias_dir} detected in backtest folds.",
            ))

        # Variance instability
        var_status = _str_scalar(bt.get("variance_stability_status") or bt.get("bias_consistency_status"))
        fvcv = _float_scalar(bt.get("fold_variance_coefficient_of_variation"))
        if var_status in cfg.POOR_VARIANCE_STATUSES:
            issues.append(ComponentIssue(
                "variance_instability", "poor", "backtest",
                f"Residual variance is materially unstable across folds"
                f"{f' (CV={fvcv:.2f})' if fvcv else ''}.",
            ))
        elif var_status in cfg.WARNING_VARIANCE_STATUSES:
            issues.append(ComponentIssue(
                "variance_instability", "warning", "backtest",
                "Moderate residual variance instability observed across folds.",
            ))

    # Production bias — supplementary
    if prod is not None:
        prod_status = _str_scalar(prod.get("bias_status"))
        var_status = _str_scalar(prod.get("variance_stability_status"))
        vcr = _float_scalar(prod.get("variance_change_ratio"))

        if prod_status in cfg.POOR_BIAS_STATUSES:
            nb = _float_scalar(prod.get("normalized_bias"))
            nb_str = f" ({nb:+.1%})" if nb is not None else ""
            issues.append(ComponentIssue(
                "persistent_bias", "poor", "production",
                f"Production errors show persistent bias{nb_str}.",
            ))
        elif prod_status in cfg.WARNING_BIAS_STATUSES:
            issues.append(ComponentIssue(
                "persistent_bias", "warning", "production",
                "Mild bias observed in production forecast errors.",
            ))

        if var_status in cfg.POOR_VARIANCE_STATUSES:
            issues.append(ComponentIssue(
                "variance_instability", "poor", "production",
                f"Production residual variance changed materially"
                f"{f' (ratio={vcr:.1f})' if vcr else ''}.",
            ))

    return issues


def _detect_outlier_issues(
    sources: dict,
    cfg: ModelHealthConfig,
) -> list[ComponentIssue]:
    issues: list[ComponentIssue] = []

    bt = sources.get("backtest_outlier")
    prod = sources.get("production_outlier")
    tr = sources.get("training_outlier")

    # Training (informational)
    if tr is not None:
        tr_status = _str_scalar(tr.get("outlier_status"))
        dist_status = _str_scalar(tr.get("normality_status"))
        if tr_status in cfg.POOR_OUTLIER_STATUSES | cfg.WARNING_OUTLIER_STATUSES:
            issues.append(ComponentIssue(
                "repeated_outliers", "informational", "training",
                f"Training residuals show {tr_status} outlier behaviour (in-sample).",
            ))
        if dist_status not in (None, "no_material_concern", "insufficient_evidence"):
            issues.append(ComponentIssue(
                "distribution_caution", "informational", "training",
                f"Training distribution: {dist_status} (in-sample).",
            ))

    # Backtest — primary
    if bt is not None:
        bt_status = _str_scalar(bt.get("cross_fold_outlier_status"))
        dist_status = _str_scalar(bt.get("cross_fold_distribution_status"))
        poor_folds = _int_scalar(bt.get("poor_outlier_fold_count"), 0)
        warn_folds = _int_scalar(bt.get("warning_outlier_fold_count"), 0)
        heavy_folds = _int_scalar(bt.get("heavy_tailed_fold_count"), 0)

        if bt_status in cfg.POOR_OUTLIER_STATUSES:
            issues.append(ComponentIssue(
                "repeated_outliers", "poor", "backtest",
                f"Repeated large forecast errors across {poor_folds} backtest fold(s).",
            ))
        elif bt_status in cfg.WARNING_OUTLIER_STATUSES or warn_folds > 0:
            issues.append(ComponentIssue(
                "repeated_outliers", "warning", "backtest",
                f"Large forecast errors observed in {warn_folds} backtest fold(s).",
            ))

        if heavy_folds > 0 and dist_status not in (None, "no_material_concern"):
            issues.append(ComponentIssue(
                "distribution_caution", "warning", "backtest",
                f"Heavy-tailed residual distribution observed in {heavy_folds} fold(s); "
                "analytic prediction intervals may be unreliable.",
            ))

    # Production — supplementary
    if prod is not None:
        prod_status = _str_scalar(prod.get("outlier_status"))
        norm_status = _str_scalar(prod.get("normality_status"))
        if prod_status in cfg.POOR_OUTLIER_STATUSES:
            issues.append(ComponentIssue(
                "repeated_outliers", "poor", "production",
                "Production forecast errors contain a high rate of large misses.",
            ))
        elif prod_status in cfg.WARNING_OUTLIER_STATUSES:
            issues.append(ComponentIssue(
                "repeated_outliers", "warning", "production",
                "Production forecast errors contain isolated large misses.",
            ))
        if norm_status not in (None, "no_material_concern", "insufficient_evidence"):
            issues.append(ComponentIssue(
                "distribution_caution", "informational", "production",
                f"Production residual distribution: {norm_status}.",
            ))

    return issues


def _detect_interval_issues(
    sources: dict,
    cfg: ModelHealthConfig,
) -> list[ComponentIssue]:
    issues: list[ComponentIssue] = []

    bt = sources.get("backtest_interval")
    prod = sources.get("production_interval")

    # Backtest — primary
    if bt is not None:
        cal_status = _str_scalar(bt.get("cross_fold_calibration_status"))
        use_status = _str_scalar(bt.get("cross_fold_interval_usefulness_status"))

        if cal_status in cfg.CRITICAL_INTERVAL_STATUSES:
            issues.append(ComponentIssue(
                "interval_undercoverage", "critical", "backtest",
                f"Severe interval undercoverage across backtest folds ({cal_status}).",
            ))
        elif cal_status in cfg.POOR_INTERVAL_STATUSES:
            issues.append(ComponentIssue(
                "interval_undercoverage", "poor", "backtest",
                f"Interval calibration is {cal_status} across backtest folds.",
            ))
        elif cal_status in cfg.WARNING_INTERVAL_STATUSES:
            issues.append(ComponentIssue(
                "interval_undercoverage", "warning", "backtest",
                f"Mild interval undercoverage in backtest ({cal_status}).",
            ))

        if use_status in ("overwide",):
            issues.append(ComponentIssue(
                "interval_overwidth", "poor", "backtest",
                "Prediction intervals are excessively wide across backtest folds.",
            ))
        elif use_status in ("wide_but_usable",):
            issues.append(ComponentIssue(
                "interval_overwidth", "warning", "backtest",
                "Prediction intervals are wider than expected relative to usage levels.",
            ))

    # Production — supplementary
    if prod is not None:
        prod_cal = _str_scalar(prod.get("calibration_status"))
        prod_use = _str_scalar(prod.get("interval_usefulness_status"))

        if prod_cal in cfg.CRITICAL_INTERVAL_STATUSES:
            issues.append(ComponentIssue(
                "interval_undercoverage", "critical", "production",
                f"Severe interval undercoverage in production ({prod_cal}).",
            ))
        elif prod_cal in cfg.POOR_INTERVAL_STATUSES:
            issues.append(ComponentIssue(
                "interval_undercoverage", "poor", "production",
                f"Interval calibration is {prod_cal} in production.",
            ))
        elif prod_cal in cfg.WARNING_INTERVAL_STATUSES:
            issues.append(ComponentIssue(
                "interval_undercoverage", "warning", "production",
                f"Mild interval undercoverage in production ({prod_cal}).",
            ))

    return issues


def _detect_deterioration_issues(
    sources: dict,
    cfg: ModelHealthConfig,
) -> list[ComponentIssue]:
    issues: list[ComponentIssue] = []

    det = sources.get("deterioration")
    if det is None:
        return issues

    flag = _bool_scalar(det.get("accuracy_deterioration_flag"), False)
    ev = _str_scalar(det.get("evidence_status"), "unknown")
    reasons = _str_scalar(det.get("deterioration_reasons"), "")

    if not flag or ev in ("insufficient", "unknown"):
        return issues

    issues.append(ComponentIssue(
        "production_deterioration", "critical", "production",
        f"Production accuracy has deteriorated. {reasons}".strip(),
    ))
    return issues


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def classify_model_diagnostic_status(
    issues: list[ComponentIssue],
    evidence_status: str,
    cfg: ModelHealthConfig = _DEFAULT_CFG,
) -> str:
    """Classify overall model health from component issues and evidence status.

    Rules
    -----
    * insufficient_evidence evidence → insufficient_evidence
    * calculation_failed evidence → calculation_failed
    * critical severity → poor
    * >= 2 poor-severity issues → poor
    * 1 poor + 2 warnings → poor
    * 1 poor alone → poor
    * warning only (above threshold) → watch
    * no issues with sufficient evidence → healthy
    """
    if evidence_status in ("insufficient_evidence", "incomplete_lineage"):
        return "insufficient_evidence"
    if evidence_status == "calculation_failed":
        return "calculation_failed"

    n_critical = sum(1 for i in issues if i.severity == "critical")
    n_poor = sum(1 for i in issues if i.severity == "poor")
    n_warning = sum(1 for i in issues if i.severity == "warning")
    n_info = sum(1 for i in issues if i.severity == "informational")

    if n_critical >= 1:
        return "poor"
    if n_poor >= 2:
        return "poor"
    if n_poor == 1 and n_warning >= 2:
        return "poor"
    if n_poor == 1:
        return "poor"
    if n_warning > cfg.MAX_WARNINGS_FOR_HEALTHY:
        return "watch"
    return "healthy"


def determine_recommended_model_action(
    issues: list[ComponentIssue],
    status: str,
    cfg: ModelHealthConfig = _DEFAULT_CFG,
) -> tuple[str, str, bool]:
    """Determine recommended action, priority, and review_required.

    Returns
    -------
    (action, priority, review_required)
    """
    if status == "insufficient_evidence":
        return "insufficient_evidence", "insufficient_evidence", False
    if status == "calculation_failed":
        return "insufficient_evidence", "insufficient_evidence", True

    n_poor = sum(1 for i in issues if i.severity in ("poor", "critical"))
    n_critical = sum(1 for i in issues if i.severity == "critical")
    n_warning = sum(1 for i in issues if i.severity == "warning")

    # Primary issue drives the action
    primary = _select_primary_issue(issues)

    _action_map = {
        "production_deterioration": "review_production_deterioration",
        "residual_autocorrelation": "review_model_specification",
        "persistent_bias": "investigate_bias",
        "interval_undercoverage": "review_interval_calibration",
        "variance_instability": "investigate_variance_instability",
        "repeated_outliers": "investigate_outliers",
        "interval_overwidth": "review_interval_calibration",
        "distribution_caution": "review_model_specification",
        "insufficient_evidence": "insufficient_evidence",
        "none": "continue_monitoring",
    }

    # Check for seasonality-specific autocorrelation
    m_acf_issues = [
        i for i in issues
        if i.name == "residual_autocorrelation" and "m=" in (i.reason or "")
    ]
    if m_acf_issues:
        base_action = "review_seasonality"
    else:
        base_action = _action_map.get(primary, "continue_monitoring")

    # consider_retraining when multiple severe issues
    consider_retrain = (
        status == "poor"
        and n_poor >= cfg.MIN_POOR_ISSUES_FOR_RETRAINING
    )
    if consider_retrain:
        action = "consider_retraining"
    else:
        action = base_action

    # Priority
    review = status != "healthy"
    if status == "healthy":
        priority = "low"
    elif n_critical >= 1 or (n_poor + n_critical) >= cfg.URGENT_POOR_ISSUE_THRESHOLD:
        priority = "urgent"
    elif n_poor >= 2:
        priority = "high"
    elif n_poor == 1:
        priority = "high"
    elif n_warning >= 2:
        priority = "medium"
    else:
        priority = "low"

    return action, priority, review


def _select_primary_issue(issues: list[ComponentIssue]) -> str:
    """Select primary issue using deterministic priority order."""
    if not issues:
        return "none"

    # Build set of active issue names, weighted by severity
    severe = {i.name for i in issues if i.severity in ("poor", "critical")}
    any_issue = {i.name for i in issues}

    for candidate in _PRIMARY_ISSUE_ORDER:
        if candidate in severe:
            return candidate
    for candidate in _PRIMARY_ISSUE_ORDER:
        if candidate in any_issue:
            return candidate
    return "none"


# ---------------------------------------------------------------------------
# Reason generation
# ---------------------------------------------------------------------------


def build_model_diagnostic_reasons(
    issues: list[ComponentIssue],
    sources: dict,
    evidence_status: str,
    status: str,
    action: str,
) -> list[str]:
    """Build ordered, deterministic reason strings.

    Order: evidence → deterioration → autocorrelation → bias → variance →
    intervals → outliers → distribution → training → action.
    """
    reasons: list[str] = []

    # 1. Evidence and lineage
    if evidence_status == "insufficient_evidence":
        reasons.append(
            "Insufficient evidence: fewer than the required valid backtest folds "
            "are available."
        )
    elif evidence_status == "incomplete_lineage":
        reasons.append(
            "Incomplete model lineage: selected-model attribution cannot be "
            "confirmed for all fields."
        )
    elif evidence_status == "strong_backtest_limited_production":
        prod_bias = sources.get("production_bias")
        n = _int_scalar(prod_bias.get("valid_residual_count") if prod_bias is not None else None, 0)
        reasons.append(
            f"Production evidence is limited to {n} realized "
            "predictions; model health is based primarily on rolling backtests."
        )
    elif evidence_status == "partial":
        missing = [c for c in [
            "training_diagnostics", "backtest_diagnostics",
            "production_diagnostics", "interval_diagnostics",
            "deterioration_evidence",
        ] if sources.get(c.replace("_diagnostics", "_acf").replace("_evidence", "")) is None]
        if missing:
            reasons.append(
                f"Partial evidence: the following diagnostic categories are "
                f"missing: {', '.join(missing)}."
            )

    # 2. Production deterioration
    for i in issues:
        if i.name == "production_deterioration":
            reasons.append(i.reason)

    # 3. Autocorrelation
    for i in issues:
        if i.name == "residual_autocorrelation" and i.source != "training":
            reasons.append(i.reason)

    # 4. Bias
    for i in issues:
        if i.name == "persistent_bias" and i.source != "training":
            reasons.append(i.reason)

    # 5. Variance stability
    for i in issues:
        if i.name == "variance_instability" and i.source != "training":
            reasons.append(i.reason)

    # 6. Interval calibration
    for i in issues:
        if i.name in ("interval_undercoverage", "interval_overwidth") and i.source != "training":
            reasons.append(i.reason)

    # 7. Outlier behaviour
    for i in issues:
        if i.name == "repeated_outliers" and i.source != "training":
            reasons.append(i.reason)

    # 8. Distribution caution
    for i in issues:
        if i.name == "distribution_caution":
            reasons.append(
                "Residuals are heavy-tailed or non-normal, but point-forecast "
                "diagnostics remain acceptable; analytic interval interpretation "
                "warrants caution."
                if i.severity in ("warning", "informational")
                else i.reason
            )
            break  # one distribution reason is enough

    # 9. Training-only evidence (informational)
    for i in issues:
        if i.source == "training" and i.name not in {r.split(":")[0] for r in reasons}:
            reasons.append(f"Training evidence (in-sample): {i.reason}")
            break

    # 10. Healthy summary
    if status == "healthy" and not reasons:
        reasons.append(
            "Model health is acceptable: out-of-sample residual diagnostics "
            "show no material autocorrelation, practical bias, or "
            "interval-calibration issue."
        )

    # 11. Action
    _action_reasons = {
        "review_seasonality": (
            "Recommended action: review the seasonal period to address residual "
            "autocorrelation at the selected cycle."
        ),
        "review_model_specification": (
            "Recommended action: review the model specification to reduce "
            "remaining serial dependence in residuals."
        ),
        "investigate_bias": (
            "Recommended action: investigate persistent forecast bias."
        ),
        "investigate_variance_instability": (
            "Recommended action: investigate residual variance instability."
        ),
        "investigate_outliers": (
            "Recommended action: investigate repeated large forecast errors."
        ),
        "review_interval_calibration": (
            "Recommended action: review interval calibration before relying "
            "on the forecast range."
        ),
        "review_production_deterioration": (
            "Recommended action: review recent production performance, which "
            "shows signs of accuracy deterioration."
        ),
        "consider_retraining": (
            "Recommended action: consider retraining the model; multiple "
            "diagnostic components show poor performance."
        ),
    }
    if action in _action_reasons:
        reasons.append(_action_reasons[action])

    return [r for r in reasons if r]


# ---------------------------------------------------------------------------
# Per-report row builder
# ---------------------------------------------------------------------------


def _build_health_row(
    spine_row: pd.Series,
    sources: dict[str, Optional[pd.Series]],
    cfg: ModelHealthConfig,
    diagnostic_run_id: str,
    generated_at: str,
) -> dict:
    """Build a single model-health summary row dict."""
    report_id = _str_scalar(spine_row.get("report_id"))

    # Identity
    row: dict = {
        "diagnostic_run_id": diagnostic_run_id,
        "generated_at": generated_at,
        "report_id": report_id,
        "report_name": _str_scalar(spine_row.get("report_name")),
        "selected_model_family": _str_scalar(spine_row.get("selected_model_family")),
        "selected_model_name": _str_scalar(spine_row.get("selected_model_name")),
        "selected_m": _scalar(spine_row.get("selected_m")),
        "training_cutoff": _str_scalar(spine_row.get("training_cutoff")),
        "selection_run_id": _str_scalar(spine_row.get("selection_run_id")),
        "lineage_complete": _bool_scalar(spine_row.get("lineage_complete"), True),
        "lineage_missing_fields": _str_scalar(spine_row.get("lineage_missing_fields"), "[]"),
    }

    # --- Source availability flags ---
    row["training_diagnostics_available"] = (
        sources.get("training_acf") is not None
        or sources.get("training_bias") is not None
        or sources.get("training_outlier") is not None
    )
    row["backtest_diagnostics_available"] = (
        sources.get("backtest_acf") is not None
        or sources.get("backtest_bias") is not None
        or sources.get("backtest_outlier") is not None
    )
    row["production_diagnostics_available"] = (
        sources.get("production_acf") is not None
        or sources.get("production_bias") is not None
        or sources.get("production_outlier") is not None
    )
    row["interval_diagnostics_available"] = (
        sources.get("backtest_interval") is not None
        or sources.get("production_interval") is not None
    )
    row["deterioration_evidence_available"] = sources.get("deterioration") is not None

    # --- Evidence summary ---
    tr_acf = sources.get("training_acf")
    bt_acf = sources.get("backtest_acf")
    bt_bias = sources.get("backtest_bias")
    bt_outlier = sources.get("backtest_outlier")
    prod_bias = sources.get("production_bias")
    bt_interval = sources.get("backtest_interval")
    det = sources.get("deterioration")

    row["training_residual_count"] = _int_scalar(
        tr_acf.get("valid_residual_count") if tr_acf is not None else None, 0
    )
    row["backtest_error_count"] = _int_scalar(
        bt_bias.get("total_actual") if bt_bias is not None else None, 0
    )
    row["backtest_valid_fold_count"] = _int_scalar(
        bt_bias.get("valid_fold_count") if bt_bias is not None else None, 0
    )
    row["production_error_count"] = _int_scalar(
        prod_bias.get("valid_residual_count") if prod_bias is not None else None, 0
    )
    row["production_completed_run_count"] = 0  # not available in current schemas
    row["interval_observation_count"] = _int_scalar(
        bt_interval.get("sufficient_interval_fold_count") if bt_interval is not None else None, 0
    )

    evidence_status, missing_cats, score = _assess_evidence(
        report_id, spine_row, sources, cfg
    )
    row["diagnostic_evidence_status"] = evidence_status
    row["missing_evidence_categories"] = json.dumps(sorted(missing_cats))
    row["evidence_completeness_score"] = round(score, 3)

    # --- Detect issues ---
    issues: list[ComponentIssue] = []
    try:
        issues += _detect_deterioration_issues(sources, cfg)
        issues += _detect_autocorrelation_issues(sources, cfg)
        issues += _detect_bias_issues(sources, cfg)
        issues += _detect_outlier_issues(sources, cfg)
        issues += _detect_interval_issues(sources, cfg)
    except Exception as exc:
        _log.error("Issue detection failed for report %s: %s", report_id, exc)
        row["model_diagnostic_status"] = "calculation_failed"
        row["model_diagnostic_reasons"] = f"Issue detection failed: {exc}"
        for col in MODEL_HEALTH_COLS:
            if col not in row:
                row[col] = None
        row["automatic_retraining_triggered"] = False
        row["review_required"] = True
        row["recommended_model_action"] = "insufficient_evidence"
        row["action_priority"] = "insufficient_evidence"
        return row

    # --- Classification ---
    status = classify_model_diagnostic_status(issues, evidence_status, cfg)
    action, priority, review = determine_recommended_model_action(issues, status, cfg)
    reasons = build_model_diagnostic_reasons(issues, sources, evidence_status, status, action)

    primary = _select_primary_issue(issues)
    n_warn = sum(1 for i in issues if i.severity == "warning")
    n_poor = sum(1 for i in issues if i.severity in ("poor", "critical"))

    row["model_diagnostic_status"] = status
    row["diagnostic_issue_count"] = len([i for i in issues if i.severity != "none"])
    row["warning_issue_count"] = n_warn
    row["poor_issue_count"] = n_poor
    row["primary_model_issue"] = primary
    row["model_diagnostic_reasons"] = " ".join(reasons)
    row["recommended_model_action"] = action
    row["action_priority"] = priority
    row["review_required"] = review
    row["automatic_retraining_triggered"] = False

    # --- Autocorrelation fields ---
    prod_acf = sources.get("production_acf")
    row["training_autocorrelation_status"] = _str_scalar(tr_acf.get("autocorrelation_status") if tr_acf is not None else None)
    row["backtest_autocorrelation_status"] = _str_scalar(bt_acf.get("autocorrelation_status") if bt_acf is not None else None)
    row["production_autocorrelation_status"] = _str_scalar(prod_acf.get("autocorrelation_status") if prod_acf is not None else None)
    # Prefer production, fall back to backtest summary
    row["lag1_autocorrelation"] = _float_scalar(
        prod_acf.get("lag1_autocorrelation") if prod_acf is not None else
        bt_acf.get("median_lag1_autocorrelation") if bt_acf is not None else None
    )
    row["selected_m_autocorrelation"] = _float_scalar(
        prod_acf.get("selected_m_autocorrelation") if prod_acf is not None else None
    )
    row["max_abs_autocorrelation"] = _float_scalar(
        prod_acf.get("max_abs_autocorrelation") if prod_acf is not None else
        bt_acf.get("median_max_abs_autocorrelation") if bt_acf is not None else None
    )
    row["ljung_box_pvalue"] = _float_scalar(prod_acf.get("ljung_box_pvalue") if prod_acf is not None else None)
    row["significant_ljung_box_fold_count"] = 0  # not in backtest ACF summary schema
    row["practical_autocorrelation_fold_count"] = _int_scalar(
        bt_acf.get("folds_with_autocorrelation_poor") if bt_acf is not None else None, 0
    )
    acf_reasons = []
    for i in issues:
        if i.name == "residual_autocorrelation":
            acf_reasons.append(i.reason)
    row["autocorrelation_reasons"] = " ".join(acf_reasons) if acf_reasons else None

    # --- Bias fields ---
    tr_bias = sources.get("training_bias")
    prod_b = sources.get("production_bias")
    row["training_bias_status"] = _str_scalar(tr_bias.get("bias_status") if tr_bias is not None else None)
    row["backtest_bias_status"] = _str_scalar(bt_bias.get("bias_status") if bt_bias is not None else None)
    row["production_bias_status"] = _str_scalar(prod_b.get("bias_status") if prod_b is not None else None)
    # Primary evidence: backtest
    row["normalized_bias"] = _float_scalar(
        bt_bias.get("normalized_bias") if bt_bias is not None else
        prod_b.get("normalized_bias") if prod_b is not None else None
    )
    row["median_residual"] = _float_scalar(
        bt_bias.get("median_residual") if bt_bias is not None else
        prod_b.get("median_residual") if prod_b is not None else None
    )
    row["fold_bias_std"] = _float_scalar(bt_bias.get("fold_bias_std") if bt_bias is not None else None)
    row["directional_bias_fold_share"] = _float_scalar(
        bt_bias.get("directional_bias_fold_share") if bt_bias is not None else None
    )
    row["horizon_bias_status"] = _str_scalar(
        prod_b.get("horizon_bias_status") if prod_b is not None else
        tr_bias.get("horizon_bias_status") if tr_bias is not None else None
    )
    row["late_horizon_normalized_bias"] = _float_scalar(
        prod_b.get("late_horizon_normalized_bias") if prod_b is not None else
        tr_bias.get("late_horizon_normalized_bias") if tr_bias is not None else None
    )
    row["variance_stability_status"] = _str_scalar(
        bt_bias.get("variance_stability_status") if bt_bias is not None else
        prod_b.get("variance_stability_status") if prod_b is not None else None
    )
    row["variance_change_ratio"] = _float_scalar(
        prod_b.get("variance_change_ratio") if prod_b is not None else
        tr_bias.get("variance_change_ratio") if tr_bias is not None else None
    )
    row["fold_variance_coefficient_of_variation"] = _float_scalar(
        bt_bias.get("fold_variance_coefficient_of_variation") if bt_bias is not None else None
    )
    bias_reasons = []
    for i in issues:
        if i.name in ("persistent_bias", "variance_instability"):
            bias_reasons.append(i.reason)
    row["bias_stability_reasons"] = " ".join(bias_reasons) if bias_reasons else None

    # --- Outlier fields ---
    tr_outlier = sources.get("training_outlier")
    bt_out = sources.get("backtest_outlier")
    prod_out = sources.get("production_outlier")
    row["training_outlier_status"] = _str_scalar(tr_outlier.get("outlier_status") if tr_outlier is not None else None)
    row["backtest_outlier_status"] = _str_scalar(bt_out.get("cross_fold_outlier_status") if bt_out is not None else None)
    row["production_outlier_status"] = _str_scalar(prod_out.get("outlier_status") if prod_out is not None else None)
    row["outlier_rate"] = _float_scalar(
        bt_out.get("median_outlier_rate") if bt_out is not None else
        prod_out.get("outlier_rate") if prod_out is not None else None
    )
    row["largest_absolute_residual"] = _float_scalar(
        prod_out.get("largest_absolute_residual") if prod_out is not None else
        bt_out.get("max_absolute_residual_across_folds") if bt_out is not None else None
    )
    row["largest_absolute_residual_date"] = _str_scalar(
        prod_out.get("largest_absolute_residual_date") if prod_out is not None else None
    )
    row["tail_direction"] = _str_scalar(prod_out.get("tail_direction") if prod_out is not None else None)
    row["residual_skewness"] = _float_scalar(prod_out.get("residual_skewness") if prod_out is not None else None)
    row["excess_kurtosis"] = _float_scalar(prod_out.get("excess_kurtosis") if prod_out is not None else None)
    row["normality_status"] = _str_scalar(
        prod_out.get("normality_status") if prod_out is not None else
        bt_out.get("cross_fold_distribution_status") if bt_out is not None else None
    )
    out_reasons = []
    for i in issues:
        if i.name in ("repeated_outliers", "distribution_caution"):
            out_reasons.append(i.reason)
    row["outlier_distribution_reasons"] = " ".join(out_reasons) if out_reasons else None

    # --- Interval fields ---
    prod_int = sources.get("production_interval")
    row["backtest_calibration_status"] = _str_scalar(
        bt_interval.get("cross_fold_calibration_status") if bt_interval is not None else None
    )
    row["production_calibration_status"] = _str_scalar(
        prod_int.get("calibration_status") if prod_int is not None else None
    )
    row["interval_usefulness_status"] = _str_scalar(
        bt_interval.get("cross_fold_interval_usefulness_status") if bt_interval is not None else
        prod_int.get("interval_usefulness_status") if prod_int is not None else None
    )
    row["nominal_coverage"] = _float_scalar(
        bt_interval.get("nominal_coverage") if bt_interval is not None else
        prod_int.get("nominal_coverage") if prod_int is not None else None
    )
    row["observed_coverage"] = _float_scalar(
        prod_int.get("observed_coverage") if prod_int is not None else
        bt_interval.get("pooled_observed_coverage") if bt_interval is not None else None
    )
    row["coverage_gap"] = _float_scalar(
        prod_int.get("coverage_gap") if prod_int is not None else
        bt_interval.get("median_coverage_gap") if bt_interval is not None else None
    )
    row["normalized_mean_interval_width"] = _float_scalar(
        prod_int.get("normalized_mean_interval_width") if prod_int is not None else
        bt_interval.get("median_normalized_interval_width") if bt_interval is not None else None
    )
    row["normalized_mean_winkler_score"] = _float_scalar(
        prod_int.get("normalized_mean_winkler_score") if prod_int is not None else
        bt_interval.get("median_normalized_winkler_score") if bt_interval is not None else None
    )
    row["horizon_calibration_deterioration_flag"] = _bool_scalar(
        prod_int.get("horizon_calibration_deterioration_flag") if prod_int is not None else False
    )
    int_reasons = []
    for i in issues:
        if i.name in ("interval_undercoverage", "interval_overwidth"):
            int_reasons.append(i.reason)
    row["interval_calibration_reasons"] = " ".join(int_reasons) if int_reasons else None

    # --- Deterioration fields ---
    row["accuracy_deterioration_flag"] = _bool_scalar(
        det.get("accuracy_deterioration_flag") if det is not None else None, False
    )
    row["deterioration_reasons"] = _str_scalar(
        det.get("deterioration_reasons") if det is not None else None
    )
    row["recent_wape"] = _float_scalar(det.get("recent_wape") if det is not None else None)
    row["previous_wape"] = _float_scalar(det.get("previous_wape") if det is not None else None)
    row["recent_bias"] = _float_scalar(det.get("recent_bias") if det is not None else None)
    row["previous_bias"] = _float_scalar(det.get("previous_bias") if det is not None else None)
    row["recent_interval_coverage"] = _float_scalar(det.get("recent_interval_coverage") if det is not None else None)
    row["previous_interval_coverage"] = _float_scalar(det.get("previous_interval_coverage") if det is not None else None)

    # Deterioration severity
    if row["accuracy_deterioration_flag"]:
        ev_det = _str_scalar(det.get("evidence_status") if det is not None else None, "unknown")
        if ev_det in ("ok", "sufficient"):
            row["deterioration_severity"] = "confirmed"
        elif ev_det == "insufficient":
            row["deterioration_severity"] = "unconfirmed_limited_evidence"
            row["accuracy_deterioration_flag"] = False
        else:
            row["deterioration_severity"] = "unknown"
    else:
        row["deterioration_severity"] = "none"

    return row


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def build_report_model_diagnostics(
    production_forecast_df: pd.DataFrame,
    training_acf_df: Optional[pd.DataFrame] = None,
    backtest_acf_summary_df: Optional[pd.DataFrame] = None,
    production_acf_df: Optional[pd.DataFrame] = None,
    training_bias_df: Optional[pd.DataFrame] = None,
    backtest_bias_summary_df: Optional[pd.DataFrame] = None,
    production_bias_df: Optional[pd.DataFrame] = None,
    training_outlier_df: Optional[pd.DataFrame] = None,
    backtest_outlier_summary_df: Optional[pd.DataFrame] = None,
    production_outlier_df: Optional[pd.DataFrame] = None,
    backtest_interval_summary_df: Optional[pd.DataFrame] = None,
    production_interval_df: Optional[pd.DataFrame] = None,
    deterioration_df: Optional[pd.DataFrame] = None,
    cfg: ModelHealthConfig = _DEFAULT_CFG,
    diagnostic_run_id: str = "",
) -> pd.DataFrame:
    """Build one model-health row per active report.

    Parameters
    ----------
    production_forecast_df:
        Report spine — one or more rows per report_id, used to identify the
        active selected model specification.  Must contain report_id,
        selected_model_family, selected_model_name, selected_m,
        training_cutoff, selection_run_id.
    *_df:
        Diagnostic source DataFrames (all optional — missing sources yield
        partial evidence).
    cfg:
        Classification thresholds.
    diagnostic_run_id:
        Pipeline run ID stamped into every output row.

    Returns
    -------
    DataFrame with MODEL_HEALTH_COLS columns, one row per report.
    """
    if production_forecast_df is None or production_forecast_df.empty:
        return pd.DataFrame(columns=MODEL_HEALTH_COLS)

    generated_at = datetime.now(timezone.utc).isoformat()

    # Build report spine: one row per report (latest run_id / most recent row)
    spine_key = "report_id"
    if "run_id" in production_forecast_df.columns:
        spine = (
            production_forecast_df
            .sort_values("run_id", ascending=False)
            .drop_duplicates(subset=[spine_key], keep="first")
            .reset_index(drop=True)
        )
    else:
        spine = production_forecast_df.drop_duplicates(subset=[spine_key], keep="first").reset_index(drop=True)

    def _safe_df(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        return None if (df is None or df.empty) else df

    results = []
    for _, spine_row in spine.iterrows():
        rid = _str_scalar(spine_row.get("report_id"))
        mname = _str_scalar(spine_row.get("selected_model_name"))
        m_val = _scalar(spine_row.get("selected_m"))

        def _match(df, src_name_col, src_m_col):
            return _first_row(df, rid, ["report_id", src_name_col, src_m_col], [rid, mname, m_val])

        def _match_report_only(df):
            return _first_row(df, rid, ["report_id"], [rid])

        def _match_bt_summary(df, model_col="model_name", m_col="candidate_m"):
            return _first_row(df, rid, ["report_id", model_col, m_col], [rid, mname, m_val])

        sources: dict[str, Optional[pd.Series]] = {
            "training_acf": _first_row(
                _safe_df(training_acf_df), rid,
                ["report_id", "model_name", "candidate_m"], [rid, mname, m_val]
            ),
            "backtest_acf": _match_bt_summary(_safe_df(backtest_acf_summary_df)),
            "production_acf": _match(
                _safe_df(production_acf_df), "selected_model_name", "selected_m"
            ),
            "training_bias": _first_row(
                _safe_df(training_bias_df), rid,
                ["report_id", "model_name", "candidate_m"], [rid, mname, m_val]
            ),
            "backtest_bias": _match_bt_summary(_safe_df(backtest_bias_summary_df)),
            "production_bias": _match(
                _safe_df(production_bias_df), "selected_model_name", "selected_m"
            ),
            "training_outlier": _first_row(
                _safe_df(training_outlier_df), rid,
                ["report_id", "model_name", "candidate_m"], [rid, mname, m_val]
            ),
            "backtest_outlier": _match_bt_summary(_safe_df(backtest_outlier_summary_df)),
            "production_outlier": _match(
                _safe_df(production_outlier_df), "selected_model_name", "selected_m"
            ),
            "backtest_interval": _match_bt_summary(_safe_df(backtest_interval_summary_df)),
            "production_interval": _match(
                _safe_df(production_interval_df), "selected_model_name", "selected_m"
            ),
            "deterioration": _match_report_only(_safe_df(deterioration_df)),
        }

        try:
            row = _build_health_row(spine_row, sources, cfg, diagnostic_run_id, generated_at)
        except Exception as exc:
            _log.error("Model health row failed for report %s: %s", rid, exc)
            row = {c: None for c in MODEL_HEALTH_COLS}
            row["report_id"] = rid
            row["diagnostic_run_id"] = diagnostic_run_id
            row["generated_at"] = generated_at
            row["model_diagnostic_status"] = "calculation_failed"
            row["model_diagnostic_reasons"] = f"Row build failed: {exc}"
            row["automatic_retraining_triggered"] = False
            row["review_required"] = True
            row["recommended_model_action"] = "insufficient_evidence"
            row["action_priority"] = "insufficient_evidence"

        results.append(row)

    if not results:
        return pd.DataFrame(columns=MODEL_HEALTH_COLS)

    out = pd.DataFrame(results)
    for col in MODEL_HEALTH_COLS:
        if col not in out.columns:
            out[col] = None

    # Deterministic sort
    out = out.sort_values(["report_id"], ignore_index=True)
    return out[MODEL_HEALTH_COLS]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_report_model_diagnostics(df: pd.DataFrame) -> None:
    """Validate schema, grain, and logic constraints.  Raises ValueError."""
    # Schema
    missing = [c for c in MODEL_HEALTH_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"report_model_diagnostics: missing columns {missing}")

    if df.empty:
        return

    # Grain: one row per diagnostic_run_id + report_id
    dupes = df.duplicated(subset=["diagnostic_run_id", "report_id"], keep=False)
    if dupes.any():
        n = int(dupes.sum())
        raise ValueError(
            f"report_model_diagnostics: {n} duplicate row(s) on "
            "(diagnostic_run_id, report_id)."
        )

    # Allowed statuses
    bad_status = df["model_diagnostic_status"].dropna()
    inv = bad_status[~bad_status.isin(_HEALTH_STATUSES)]
    if not inv.empty:
        raise ValueError(
            f"report_model_diagnostics: invalid model_diagnostic_status "
            f"values {inv.unique().tolist()}"
        )

    bad_ev = df["diagnostic_evidence_status"].dropna()
    inv_ev = bad_ev[~bad_ev.isin(_EVIDENCE_STATUSES)]
    if not inv_ev.empty:
        raise ValueError(
            f"report_model_diagnostics: invalid diagnostic_evidence_status "
            f"values {inv_ev.unique().tolist()}"
        )

    bad_action = df["recommended_model_action"].dropna()
    inv_act = bad_action[~bad_action.isin(_ALLOWED_ACTIONS)]
    if not inv_act.empty:
        raise ValueError(
            f"report_model_diagnostics: invalid recommended_model_action "
            f"values {inv_act.unique().tolist()}"
        )

    bad_prio = df["action_priority"].dropna()
    inv_prio = bad_prio[~bad_prio.isin(_ACTION_PRIORITIES)]
    if not inv_prio.empty:
        raise ValueError(
            f"report_model_diagnostics: invalid action_priority "
            f"values {inv_prio.unique().tolist()}"
        )

    # automatic_retraining_triggered must always be False
    if "automatic_retraining_triggered" in df.columns:
        bad_retrain = df["automatic_retraining_triggered"].dropna()
        if bad_retrain.any():
            raise ValueError(
                "report_model_diagnostics: automatic_retraining_triggered "
                "must be False for all rows."
            )

    # healthy requires sufficient evidence
    healthy_mask = df["model_diagnostic_status"] == "healthy"
    insuff_mask = df["diagnostic_evidence_status"].isin(
        {"insufficient_evidence", "incomplete_lineage"}
    )
    if (healthy_mask & insuff_mask).any():
        raise ValueError(
            "report_model_diagnostics: rows with insufficient evidence "
            "cannot have model_diagnostic_status='healthy'."
        )

    # poor status should have at least one poor/critical issue count
    poor_mask = df["model_diagnostic_status"] == "poor"
    if poor_mask.any():
        poor_rows = df[poor_mask]
        no_poor_issue = poor_rows["poor_issue_count"].fillna(0) == 0
        if no_poor_issue.any():
            raise ValueError(
                "report_model_diagnostics: rows with status='poor' must have "
                "at least one poor-severity component issue."
            )

    # continue_monitoring not allowed for poor status
    poor_continue = (
        (df["model_diagnostic_status"] == "poor")
        & (df["recommended_model_action"] == "continue_monitoring")
    )
    if poor_continue.any():
        raise ValueError(
            "report_model_diagnostics: 'continue_monitoring' action is not "
            "allowed for status='poor'."
        )

    # counts are non-negative
    for col in (
        "training_residual_count", "backtest_error_count",
        "backtest_valid_fold_count", "production_error_count",
        "diagnostic_issue_count", "warning_issue_count", "poor_issue_count",
    ):
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            if (vals < 0).any():
                raise ValueError(
                    f"report_model_diagnostics: {col} has negative values."
                )

    # coverage in [0, 1]
    for col in ("observed_coverage", "nominal_coverage"):
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce").dropna()
            if ((vals < 0) | (vals > 1)).any():
                raise ValueError(
                    f"report_model_diagnostics: {col} has values outside [0, 1]."
                )

    # primary_model_issue in valid set
    if "primary_model_issue" in df.columns:
        valid_issues = set(_PRIMARY_ISSUE_ORDER)
        bad_pmi = df["primary_model_issue"].dropna()
        inv_pmi = bad_pmi[~bad_pmi.isin(valid_issues)]
        if not inv_pmi.empty:
            raise ValueError(
                f"report_model_diagnostics: invalid primary_model_issue "
                f"values {inv_pmi.unique().tolist()}"
            )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

_DIAG_DIR = Path("outputs") / "diagnostics"
_FILENAME = "report_model_diagnostics_latest.csv"


def persist_report_model_diagnostics(
    df: pd.DataFrame,
    project_root: Path,
) -> Optional[Path]:
    """Validate and write model health summary to disk.

    Writes ``<project_root>/outputs/diagnostics/report_model_diagnostics_latest.csv``.
    Returns path or None on failure.
    """
    diag_dir = project_root / _DIAG_DIR
    diag_dir.mkdir(parents=True, exist_ok=True)
    path = diag_dir / _FILENAME
    try:
        validate_report_model_diagnostics(df)
        if df.empty:
            pd.DataFrame(columns=MODEL_HEALTH_COLS).to_csv(path, index=False)
        else:
            df.to_csv(path, index=False)
        return path
    except Exception as exc:
        _log.error("Failed to persist report_model_diagnostics: %s", exc)
        return None
