"""
Evidence-aware deterministic report diagnostics layer.

Consumes five canonical Sprint 7 context outputs. Does not recalculate
source metrics. All risk flags are evidence-gated.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Schema constant
# ---------------------------------------------------------------------------

REPORT_DIAGNOSTICS_COLS = [
    # Identity and evidence
    "analytics_run_id", "generated_at", "analytics_as_of_date",
    "report_id", "report_name",
    "diagnostic_evidence_status", "missing_diagnostic_evidence",
    "privacy_suppression_status",
    # Historical usage risks
    "usage_decline_risk", "inactivity_risk", "volatility_risk", "anomaly_risk",
    "historical_usage_risk_severity",
    # Forecast risks
    "forecast_decline_risk", "forecast_low_usage_risk",
    "forecast_inactivity_risk", "forecast_uncertainty_risk",
    "forecast_risk_severity",
    # Model-health risks
    "model_health_risk", "model_bias_risk", "residual_autocorrelation_risk",
    "interval_calibration_risk", "production_deterioration_risk",
    "model_risk_severity",
    # Engagement risks
    "active_user_decline_risk", "low_repeat_engagement_risk",
    "elevated_lapse_risk", "declining_frequency_risk",
    "engagement_risk_severity",
    # Dependency risks
    "concentrated_dependency_risk", "increasing_dependency_risk",
    "dependency_risk_severity",
    # Lifecycle / metadata risks
    "immature_report_flag", "missing_metadata_risk", "ownership_gap_risk",
    "cadence_context_missing", "criticality_context_missing",
    "lifecycle_risk_severity",
    # Final diagnostics
    "primary_diagnostic", "primary_diagnostic_category",
    "diagnostic_issue_count", "diagnostic_warning_count",
    "overall_diagnostic_severity", "diagnostic_summary",
    "diagnostic_reasons", "recommended_diagnostic_action",
    "diagnostic_review_required",
]

SEVERITY_ORDER = ["none", "informational", "warning", "poor", "insufficient_evidence"]
ALLOWED_SEVERITIES = frozenset(SEVERITY_ORDER)

ALLOWED_PRIMARY_DIAGNOSTICS = frozenset({
    "no_valid_data", "prolonged_inactivity", "severe_historical_decline",
    "expected_inactivity", "severe_model_health_issue", "elevated_lapse",
    "active_user_decline", "concentrated_dependency", "high_forecast_uncertainty",
    "declining_frequency", "low_repeat_engagement", "metadata_limitation",
    "newly_launched_or_immature", "none",
})

ALLOWED_PRIMARY_CATEGORIES = frozenset({
    "data_quality", "historical_usage", "forecast_outlook", "model_health",
    "engagement", "dependency", "lifecycle", "metadata", "none",
    "insufficient_evidence",
})

ALLOWED_RECOMMENDED_ACTIONS = frozenset({
    "continue_monitoring", "investigate_usage_decline", "review_inactivity",
    "review_forecast_decline", "review_forecast_uncertainty", "review_model_health",
    "investigate_user_decline", "improve_repeat_engagement", "investigate_user_lapse",
    "review_concentrated_dependency", "validate_report_audience",
    "complete_report_metadata", "investigate_data_quality", "monitor_new_report",
    "insufficient_evidence",
})

PROHIBITED_ACTIONS = frozenset({
    "retire_report", "delete_report", "automatically_retrain", "change_selected_model",
})

PROHIBITED_DIAGNOSTIC_COLS = frozenset({
    "user_id", "email", "email_address", "user_name", "username",
    "display_name", "unique_user", "principal_name",
    "repeat_rate", "latest_views", "prior_views", "top_user_concentration",
    "usage_change_pct",
})

PRIMARY_DIAGNOSTIC_TO_ACTION = {
    "no_valid_data": "investigate_data_quality",
    "prolonged_inactivity": "review_inactivity",
    "severe_historical_decline": "investigate_usage_decline",
    "expected_inactivity": "review_inactivity",
    "severe_model_health_issue": "review_model_health",
    "elevated_lapse": "investigate_user_lapse",
    "active_user_decline": "investigate_user_decline",
    "concentrated_dependency": "review_concentrated_dependency",
    "high_forecast_uncertainty": "review_forecast_uncertainty",
    "declining_frequency": "improve_repeat_engagement",
    "low_repeat_engagement": "improve_repeat_engagement",
    "metadata_limitation": "complete_report_metadata",
    "newly_launched_or_immature": "monitor_new_report",
    "none": "continue_monitoring",
}

# ---------------------------------------------------------------------------
# Actual column name mappings discovered from source data
# ---------------------------------------------------------------------------
# report_features: anomaly is captured via 'latest_usage_anomaly_status' and
# 'usage_anomaly_count_28d'. There is no 'anomaly_detected' boolean column.
# Inactivity is in 'inactivity_status'; historical classification in
# 'historical_usage_status'.
FEAT_ANOMALY_STATUS_COL = "latest_usage_anomaly_status"
FEAT_ANOMALY_COUNT_COL = "usage_anomaly_count_28d"
FEAT_HISTORICAL_STATUS_COL = "historical_usage_status"
FEAT_MATURITY_COL = "adoption_maturity_status"

# report_engagement_context: privacy suppression uses "not_suppressed" (not "none")
ENG_PRIVACY_STATUS_COL = "privacy_suppression_status"
ENG_PRIVACY_COUNT_COL = "privacy_suppressed_field_count"
ENG_ACTIVE_USER_DIR_COL = "active_user_direction_28d"
ENG_ACTIVE_USER_CHG_COL = "active_user_change_28d_pct"
ENG_REPEAT_STATUS_COL = "repeat_engagement_status"
ENG_LAPSE_RATE_COL = "lapse_rate_28d"
ENG_FREQ_DIR_COL = "frequency_direction"
ENG_DEP_STATUS_COL = "dependency_status"
ENG_CONC_DIR_COL = "concentration_direction"
ENG_EVIDENCE_COL = "engagement_evidence_status"

# report_forecast_outlook
FC_OUTLOOK_STATUS_COL = "forecast_outlook_status"
FC_UNCERTAINTY_STATUS_COL = "forecast_uncertainty_status"
FC_EVIDENCE_COL = "forecast_evidence_status"

# report_model_health_context
MH_DIAG_STATUS_COL = "model_diagnostic_status"
MH_BIAS_COL = "bias_status"
MH_AUTOCORR_COL = "residual_autocorrelation_status"
MH_INTERVAL_COL = "interval_calibration_status"
MH_DETERIORATION_COL = "production_deterioration_status"
MH_EVIDENCE_COL = "model_evidence_status"

# report_metadata_context
META_COMPLETENESS_COL = "metadata_completeness_score"
META_OWNERSHIP_COL = "ownership_status"
META_OWNER_TEAM_COL = "report_owner_team"
META_CADENCE_COL = "expected_usage_cadence"
META_CRITICALITY_COL = "criticality_level"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ReportDiagnosticsConfig:
    # Historical usage
    DECLINE_STATUSES: frozenset = frozenset({"declining_usage"})
    INACTIVITY_STATUSES: frozenset = frozenset({"recently_inactive", "prolonged_inactivity"})
    PROLONGED_INACTIVITY_STATUS: str = "prolonged_inactivity"
    VOLATILITY_STATUSES: frozenset = frozenset({"bursty_usage"})
    INSUFFICIENT_HISTORY_STATUSES: frozenset = frozenset({
        "insufficient_history", "no_valid_usage_data", "calculation_failed"
    })
    # Lifecycle
    IMMATURE_STATUSES: frozenset = frozenset({"newly_launched", "maturing"})
    NEWLY_LAUNCHED_STATUS: str = "newly_launched"
    # Forecast
    FORECAST_DECLINE_STATUSES: frozenset = frozenset({"decline_expected"})
    FORECAST_INACTIVITY_STATUSES: frozenset = frozenset({"inactivity_expected"})
    FORECAST_LOW_USAGE_STATUSES: frozenset = frozenset({"low_usage_expected"})
    FORECAST_HIGH_UNCERTAINTY: str = "high_uncertainty"
    FORECAST_VERY_HIGH_UNCERTAINTY: str = "very_high_uncertainty"
    INVALID_FORECAST_STATUSES: frozenset = frozenset({
        "invalid_forecast", "insufficient_evidence", "calculation_failed"
    })
    # Model health
    POOR_MODEL_STATUSES: frozenset = frozenset({"poor"})
    WATCH_MODEL_STATUSES: frozenset = frozenset({"watch", "warning"})
    INVALID_MODEL_STATUSES: frozenset = frozenset({
        "calculation_failed", "invalid", "missing", "insufficient_evidence", "insufficient"
    })
    # Engagement
    DECLINING_BREADTH_STATUSES: frozenset = frozenset({"declining_breadth"})
    DECLINING_FREQUENCY_STATUSES: frozenset = frozenset({"declining_frequency"})
    LOW_REPEAT_STATUSES: frozenset = frozenset({"low_repeat", "one_time_dominated"})
    HIGH_LAPSE_STATUSES: frozenset = frozenset({"high_lapse"})
    INSUFFICIENT_ENGAGEMENT_STATUSES: frozenset = frozenset({
        "insufficient_engagement_evidence", "no_valid_user_data"
    })
    # Dependency
    HIGH_DEPENDENCY_STATUSES: frozenset = frozenset({
        "high_dependency", "single_user_dependency"
    })
    INCREASING_DEPENDENCY_STATUSES: frozenset = frozenset({"increasing_concentration"})
    # Engagement suppression
    PRIVACY_SUPPRESSED_STATUSES: frozenset = frozenset({
        "privacy_suppressed", "partial_suppression"
    })
    # Metadata
    LIMITED_METADATA_STATUSES: frozenset = frozenset({
        "limited_metadata", "missing_metadata"
    })
    # Thresholds
    ACTIVE_USER_DECLINE_THRESHOLD_PCT: float = -0.10
    RETURNING_USER_LOW_THRESHOLD: float = 0.25
    LAPSE_RATE_HIGH_THRESHOLD: float = 0.40
    METADATA_COMPLETENESS_LOW: float = 0.50


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _safe_get(row: pd.Series, col: str, default=None):
    """Get column value with fallback; returns default if col absent or NaN/None."""
    if col not in row.index:
        return default
    val = row[col]
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    return val


def _is_truthy(val) -> bool:
    """True if val is True, 'True', 'true', 1, '1'."""
    if val is True or val == 1:
        return True
    if isinstance(val, str) and val.lower() in {"true", "1"}:
        return True
    return False


def _classify_severity(issues: list[bool], warnings: list[bool]) -> str:
    if any(issues):
        return "poor"
    if any(warnings):
        return "warning"
    return "none"


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _build_historical_usage_risks(feat_row: pd.Series, cfg: ReportDiagnosticsConfig) -> dict:
    status = _safe_get(feat_row, FEAT_HISTORICAL_STATUS_COL, "")
    if status in cfg.INSUFFICIENT_HISTORY_STATUSES:
        return {
            "usage_decline_risk": False,
            "inactivity_risk": False,
            "volatility_risk": False,
            "anomaly_risk": False,
            "historical_usage_risk_severity": "insufficient_evidence",
        }

    decline = status in cfg.DECLINE_STATUSES
    inactivity = status in cfg.INACTIVITY_STATUSES
    prolonged = status == cfg.PROLONGED_INACTIVITY_STATUS
    volatility = status in cfg.VOLATILITY_STATUSES

    # Anomaly: use latest_usage_anomaly_status or anomaly_count
    anomaly_status = _safe_get(feat_row, FEAT_ANOMALY_STATUS_COL, "normal")
    anomaly_count = _safe_get(feat_row, FEAT_ANOMALY_COUNT_COL, 0)
    anomaly = (
        (isinstance(anomaly_status, str) and anomaly_status not in {"normal", "none", ""})
        or (anomaly_count is not None and anomaly_count > 0)
    )

    if prolonged:
        sev = "poor"
    elif decline or inactivity:
        sev = "warning"
    elif volatility or anomaly:
        sev = "informational"
    else:
        sev = "none"

    return {
        "usage_decline_risk": decline,
        "inactivity_risk": inactivity,
        "volatility_risk": volatility,
        "anomaly_risk": anomaly,
        "historical_usage_risk_severity": sev,
    }


def _build_forecast_risks(outlook_row: Optional[pd.Series], cfg: ReportDiagnosticsConfig) -> dict:
    _insufficient = {
        "forecast_decline_risk": False,
        "forecast_low_usage_risk": False,
        "forecast_inactivity_risk": False,
        "forecast_uncertainty_risk": False,
        "forecast_risk_severity": "insufficient_evidence",
    }

    if outlook_row is None:
        return _insufficient

    status = _safe_get(outlook_row, FC_OUTLOOK_STATUS_COL, "")
    if status in cfg.INVALID_FORECAST_STATUSES:
        return _insufficient

    # Also check evidence
    evidence = _safe_get(outlook_row, FC_EVIDENCE_COL, "")
    if evidence in {"missing", "insufficient"}:
        return _insufficient

    decline = status in cfg.FORECAST_DECLINE_STATUSES
    low_usage = status in cfg.FORECAST_LOW_USAGE_STATUSES
    inactivity = status in cfg.FORECAST_INACTIVITY_STATUSES

    uncertainty_status = _safe_get(outlook_row, FC_UNCERTAINTY_STATUS_COL, "")
    uncertainty = uncertainty_status in {
        cfg.FORECAST_HIGH_UNCERTAINTY, cfg.FORECAST_VERY_HIGH_UNCERTAINTY
    }
    very_high_unc = uncertainty_status == cfg.FORECAST_VERY_HIGH_UNCERTAINTY

    if inactivity:
        sev = "poor"
    elif decline or very_high_unc:
        sev = "warning"
    elif low_usage or uncertainty:
        sev = "informational"
    else:
        sev = "none"

    return {
        "forecast_decline_risk": decline,
        "forecast_low_usage_risk": low_usage,
        "forecast_inactivity_risk": inactivity,
        "forecast_uncertainty_risk": uncertainty,
        "forecast_risk_severity": sev,
    }


def _build_model_health_risks(health_row: Optional[pd.Series], cfg: ReportDiagnosticsConfig) -> dict:
    _insufficient = {
        "model_health_risk": False,
        "model_bias_risk": False,
        "residual_autocorrelation_risk": False,
        "interval_calibration_risk": False,
        "production_deterioration_risk": False,
        "model_risk_severity": "insufficient_evidence",
    }

    if health_row is None:
        return _insufficient

    diag_status = _safe_get(health_row, MH_DIAG_STATUS_COL, "")
    if diag_status in cfg.INVALID_MODEL_STATUSES:
        return _insufficient

    model_health = diag_status in (cfg.POOR_MODEL_STATUSES | cfg.WATCH_MODEL_STATUSES)

    bias_val = _safe_get(health_row, MH_BIAS_COL, "")
    bias_risk = isinstance(bias_val, str) and bias_val in {"detected", "high", "present"}

    autocorr_val = _safe_get(health_row, MH_AUTOCORR_COL, "")
    autocorr_risk = isinstance(autocorr_val, str) and autocorr_val in {"detected", "high", "present"}

    interval_val = _safe_get(health_row, MH_INTERVAL_COL, "")
    interval_risk = isinstance(interval_val, str) and interval_val in {"poor", "failed"}

    det_val = _safe_get(health_row, MH_DETERIORATION_COL, "")
    det_risk = isinstance(det_val, str) and det_val in {"detected", "deteriorating"}

    if diag_status in cfg.POOR_MODEL_STATUSES:
        sev = "poor"
    elif model_health or bias_risk or autocorr_risk or interval_risk or det_risk:
        sev = "warning"
    else:
        sev = "none"

    return {
        "model_health_risk": model_health,
        "model_bias_risk": bias_risk,
        "residual_autocorrelation_risk": autocorr_risk,
        "interval_calibration_risk": interval_risk,
        "production_deterioration_risk": det_risk,
        "model_risk_severity": sev,
    }


def _build_engagement_risks(eng_row: Optional[pd.Series], cfg: ReportDiagnosticsConfig) -> dict:
    _insufficient = {
        "active_user_decline_risk": False,
        "low_repeat_engagement_risk": False,
        "elevated_lapse_risk": False,
        "declining_frequency_risk": False,
        "engagement_risk_severity": "insufficient_evidence",
    }

    if eng_row is None:
        return _insufficient

    evidence = _safe_get(eng_row, ENG_EVIDENCE_COL, "")
    if evidence in cfg.INSUFFICIENT_ENGAGEMENT_STATUSES:
        return _insufficient

    # Active user decline
    direction = _safe_get(eng_row, ENG_ACTIVE_USER_DIR_COL, "")
    chg_pct = _safe_get(eng_row, ENG_ACTIVE_USER_CHG_COL)
    active_decline = False
    if direction == "declining":
        try:
            if chg_pct is not None and float(chg_pct) < cfg.ACTIVE_USER_DECLINE_THRESHOLD_PCT:
                active_decline = True
        except (TypeError, ValueError):
            pass

    # Low repeat
    repeat_status = _safe_get(eng_row, ENG_REPEAT_STATUS_COL, "")
    low_repeat = isinstance(repeat_status, str) and repeat_status in cfg.LOW_REPEAT_STATUSES

    # Elevated lapse
    lapse_rate = _safe_get(eng_row, ENG_LAPSE_RATE_COL)
    elevated_lapse = False
    try:
        if lapse_rate is not None and float(lapse_rate) > cfg.LAPSE_RATE_HIGH_THRESHOLD:
            elevated_lapse = True
    except (TypeError, ValueError):
        pass

    # Declining frequency
    freq_dir = _safe_get(eng_row, ENG_FREQ_DIR_COL, "")
    declining_freq = isinstance(freq_dir, str) and freq_dir == "declining"

    sev = _classify_severity(
        issues=[elevated_lapse and (lapse_rate is not None and float(lapse_rate) > 0.60)],
        warnings=[active_decline, low_repeat, elevated_lapse, declining_freq],
    )

    return {
        "active_user_decline_risk": active_decline,
        "low_repeat_engagement_risk": low_repeat,
        "elevated_lapse_risk": elevated_lapse,
        "declining_frequency_risk": declining_freq,
        "engagement_risk_severity": sev,
    }


def _build_dependency_risks(eng_row: Optional[pd.Series], cfg: ReportDiagnosticsConfig) -> dict:
    _insufficient = {
        "concentrated_dependency_risk": False,
        "increasing_dependency_risk": False,
        "dependency_risk_severity": "insufficient_evidence",
    }
    _no_risk = {
        "concentrated_dependency_risk": False,
        "increasing_dependency_risk": False,
        "dependency_risk_severity": "none",
    }

    if eng_row is None:
        return _insufficient

    # Privacy gate
    privacy_status = _safe_get(eng_row, ENG_PRIVACY_STATUS_COL, "")
    suppressed_count = _safe_get(eng_row, ENG_PRIVACY_COUNT_COL, 0)
    try:
        suppressed_count = int(suppressed_count) if suppressed_count is not None else 0
    except (TypeError, ValueError):
        suppressed_count = 0

    if privacy_status in cfg.PRIVACY_SUPPRESSED_STATUSES or suppressed_count > 0:
        return _no_risk

    dep_status = _safe_get(eng_row, ENG_DEP_STATUS_COL, "")
    concentrated = isinstance(dep_status, str) and dep_status in cfg.HIGH_DEPENDENCY_STATUSES

    conc_dir = _safe_get(eng_row, ENG_CONC_DIR_COL, "")
    increasing = isinstance(conc_dir, str) and conc_dir == "increasing"

    if concentrated:
        sev = "poor"
    elif increasing:
        sev = "warning"
    else:
        sev = "none"

    return {
        "concentrated_dependency_risk": concentrated,
        "increasing_dependency_risk": increasing,
        "dependency_risk_severity": sev,
    }


def _build_lifecycle_risks(
    meta_row: Optional[pd.Series],
    feat_row: pd.Series,
    cfg: ReportDiagnosticsConfig,
) -> dict:
    maturity = _safe_get(feat_row, FEAT_MATURITY_COL, "")
    immature = isinstance(maturity, str) and maturity in cfg.IMMATURE_STATUSES

    if meta_row is None:
        return {
            "immature_report_flag": immature,
            "missing_metadata_risk": True,
            "ownership_gap_risk": True,
            "cadence_context_missing": True,
            "criticality_context_missing": True,
            "lifecycle_risk_severity": "warning",
        }

    completeness = _safe_get(meta_row, META_COMPLETENESS_COL)
    missing_meta = True
    try:
        if completeness is not None:
            missing_meta = float(completeness) < cfg.METADATA_COMPLETENESS_LOW
    except (TypeError, ValueError):
        pass

    ownership = _safe_get(meta_row, META_OWNERSHIP_COL, "")
    owner_team = _safe_get(meta_row, META_OWNER_TEAM_COL)
    ownership_gap = (
        ownership in {"unknown", ""} or owner_team is None
    )

    cadence = _safe_get(meta_row, META_CADENCE_COL, "")
    cadence_missing = cadence in {"unknown", "", None}

    criticality = _safe_get(meta_row, META_CRITICALITY_COL, "")
    criticality_missing = criticality in {"unknown", "", None}

    # Severity: "warning" for metadata/ownership gaps; "informational" for context gaps only
    if missing_meta or ownership_gap:
        sev = "warning"
    elif immature or cadence_missing or criticality_missing:
        sev = "informational"
    else:
        sev = "none"

    return {
        "immature_report_flag": immature,
        "missing_metadata_risk": missing_meta,
        "ownership_gap_risk": ownership_gap,
        "cadence_context_missing": cadence_missing,
        "criticality_context_missing": criticality_missing,
        "lifecycle_risk_severity": sev,
    }


# ---------------------------------------------------------------------------
# Evidence assessment
# ---------------------------------------------------------------------------

def _assess_evidence(
    feat_row,
    outlook_row,
    health_row,
    eng_row,
    meta_row,
) -> tuple[str, list[str]]:
    sources = {
        "features": feat_row is not None,
        "forecast_outlook": outlook_row is not None,
        "model_health": health_row is not None,
        "engagement": eng_row is not None,
        "metadata": meta_row is not None,
    }
    available = sum(sources.values())
    missing = sorted(k for k, v in sources.items() if not v)

    if available == 5:
        status = "complete"
    elif available == 4:
        status = "mostly_complete"
    elif available == 3:
        status = "partial"
    elif available == 2:
        status = "evidence_limited"
    else:
        status = "insufficient"

    return status, missing


# ---------------------------------------------------------------------------
# Primary diagnostic precedence
# ---------------------------------------------------------------------------

def _determine_primary_diagnostic(
    hist: dict,
    forecast: dict,
    model: dict,
    engagement: dict,
    dep: dict,
    lifecycle: dict,
    cfg: ReportDiagnosticsConfig,
    feat_row: pd.Series,
) -> tuple[str, str]:
    """Returns (primary_diagnostic, primary_diagnostic_category)."""

    # 1. No valid data
    status = _safe_get(feat_row, FEAT_HISTORICAL_STATUS_COL, "")
    if status in {"no_valid_usage_data", "calculation_failed"}:
        return "no_valid_data", "data_quality"

    # 2. Prolonged inactivity (evidence-supported)
    if hist.get("inactivity_risk") and status == cfg.PROLONGED_INACTIVITY_STATUS:
        return "prolonged_inactivity", "historical_usage"

    # 3. Severe historical decline
    if hist.get("usage_decline_risk") and hist.get("historical_usage_risk_severity") in {"poor", "warning"}:
        return "severe_historical_decline", "historical_usage"

    # 4. Expected inactivity (forecast)
    if forecast.get("forecast_inactivity_risk"):
        return "expected_inactivity", "forecast_outlook"

    # 5. Severe model health issue
    if model.get("model_health_risk") and model.get("model_risk_severity") == "poor":
        return "severe_model_health_issue", "model_health"

    # 6. Elevated lapse
    if engagement.get("elevated_lapse_risk"):
        return "elevated_lapse", "engagement"

    # 7. Active user decline
    if engagement.get("active_user_decline_risk"):
        return "active_user_decline", "engagement"

    # 8. Concentrated dependency
    if dep.get("concentrated_dependency_risk"):
        return "concentrated_dependency", "dependency"

    # 9. High forecast uncertainty
    if forecast.get("forecast_uncertainty_risk"):
        return "high_forecast_uncertainty", "forecast_outlook"

    # 10. Declining frequency
    if engagement.get("declining_frequency_risk"):
        return "declining_frequency", "engagement"

    # 11. Low repeat engagement
    if engagement.get("low_repeat_engagement_risk"):
        return "low_repeat_engagement", "engagement"

    # 12. Metadata limitation
    if lifecycle.get("missing_metadata_risk") or lifecycle.get("ownership_gap_risk"):
        return "metadata_limitation", "metadata"

    # 13. Newly launched or immature
    if lifecycle.get("immature_report_flag"):
        return "newly_launched_or_immature", "lifecycle"

    # 14. None
    return "none", "none"


# ---------------------------------------------------------------------------
# Issue/warning counts
# ---------------------------------------------------------------------------

def _count_issues_and_warnings(
    hist: dict, forecast: dict, model: dict, engagement: dict,
    dep: dict, lifecycle: dict,
) -> tuple[int, int]:
    issues = []
    warnings = []

    for section, key in [
        (hist, "historical_usage_risk_severity"),
        (forecast, "forecast_risk_severity"),
        (model, "model_risk_severity"),
        (engagement, "engagement_risk_severity"),
        (dep, "dependency_risk_severity"),
    ]:
        sev = section.get(key, "none")
        if sev == "poor":
            issues.append(key)
        elif sev == "warning":
            warnings.append(key)

    # Lifecycle: warnings only
    sev = lifecycle.get("lifecycle_risk_severity", "none")
    if sev in {"warning", "informational"}:
        warnings.append("lifecycle_risk_severity")

    return len(issues), len(warnings)


# ---------------------------------------------------------------------------
# Overall severity
# ---------------------------------------------------------------------------

def _overall_severity(issue_count: int, warning_count: int, evidence_status: str) -> str:
    if evidence_status == "insufficient":
        return "insufficient_evidence"
    if issue_count > 0:
        return "poor"
    if warning_count > 0:
        return "warning"
    return "none"


# ---------------------------------------------------------------------------
# Reason builder
# ---------------------------------------------------------------------------

def _build_diagnostic_reasons(
    hist: dict, forecast: dict, model: dict, engagement: dict,
    dep: dict, lifecycle: dict, evidence_status: str,
    primary_diagnostic: str, action: str,
) -> str:
    parts = []

    if evidence_status not in {"complete", "mostly_complete"}:
        parts.append(f"evidence_status:{evidence_status}")

    if hist.get("historical_usage_risk_severity") not in {"none", "insufficient_evidence"}:
        flags = sorted(
            k for k in ["usage_decline_risk", "inactivity_risk", "volatility_risk", "anomaly_risk"]
            if hist.get(k)
        )
        if flags:
            parts.append(f"historical_risks:{','.join(flags)}")

    if forecast.get("forecast_risk_severity") not in {"none", "insufficient_evidence"}:
        flags = sorted(
            k for k in ["forecast_decline_risk", "forecast_low_usage_risk",
                         "forecast_inactivity_risk", "forecast_uncertainty_risk"]
            if forecast.get(k)
        )
        if flags:
            parts.append(f"forecast_risks:{','.join(flags)}")

    if model.get("model_risk_severity") not in {"none", "insufficient_evidence"}:
        flags = sorted(
            k for k in ["model_health_risk", "model_bias_risk", "residual_autocorrelation_risk",
                         "interval_calibration_risk", "production_deterioration_risk"]
            if model.get(k)
        )
        if flags:
            parts.append(f"model_risks:{','.join(flags)}")

    if engagement.get("engagement_risk_severity") not in {"none", "insufficient_evidence"}:
        flags = sorted(
            k for k in ["active_user_decline_risk", "low_repeat_engagement_risk",
                         "elevated_lapse_risk", "declining_frequency_risk"]
            if engagement.get(k)
        )
        if flags:
            parts.append(f"engagement_risks:{','.join(flags)}")

    if dep.get("dependency_risk_severity") not in {"none", "insufficient_evidence"}:
        flags = sorted(
            k for k in ["concentrated_dependency_risk", "increasing_dependency_risk"]
            if dep.get(k)
        )
        if flags:
            parts.append(f"dependency_risks:{','.join(flags)}")

    lc_flags = sorted(
        k for k in ["immature_report_flag", "missing_metadata_risk", "ownership_gap_risk",
                     "cadence_context_missing", "criticality_context_missing"]
        if lifecycle.get(k)
    )
    if lc_flags:
        parts.append(f"lifecycle_flags:{','.join(lc_flags)}")

    parts.append(f"primary:{primary_diagnostic}")
    parts.append(f"action:{action}")

    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Main build function
# ---------------------------------------------------------------------------

def build_report_diagnostics(
    features_df: pd.DataFrame,
    forecast_df: Optional[pd.DataFrame],
    model_health_df: Optional[pd.DataFrame],
    engagement_df: Optional[pd.DataFrame],
    metadata_df: Optional[pd.DataFrame],
    analytics_run_id: str,
    cfg: Optional[ReportDiagnosticsConfig] = None,
) -> pd.DataFrame:
    """
    Grain: one row per report_id.
    Spine: features_df (all reports must appear here).
    All other DataFrames are left-joined on report_id.
    """
    if cfg is None:
        cfg = ReportDiagnosticsConfig()

    generated_at = datetime.utcnow().isoformat()

    def _index_by_report(df):
        if df is None or df.empty:
            return {}
        return {str(row["report_id"]): row for _, row in df.iterrows()}

    outlook_idx = _index_by_report(forecast_df)
    health_idx = _index_by_report(model_health_df)
    eng_idx = _index_by_report(engagement_df)
    meta_idx = _index_by_report(metadata_df)

    rows = []
    for _, feat_row in features_df.iterrows():
        rid = str(feat_row["report_id"])
        outlook_row = outlook_idx.get(rid)
        health_row = health_idx.get(rid)
        eng_row = eng_idx.get(rid)
        meta_row = meta_idx.get(rid)

        # Evidence
        evidence_status, missing_sources = _assess_evidence(
            feat_row,
            outlook_row,
            health_row,
            eng_row,
            meta_row,
        )

        # Risk sections
        hist = _build_historical_usage_risks(feat_row, cfg)
        forecast = _build_forecast_risks(
            pd.Series(outlook_row) if outlook_row is not None else None, cfg
        )
        model = _build_model_health_risks(
            pd.Series(health_row) if health_row is not None else None, cfg
        )
        engagement = _build_engagement_risks(
            pd.Series(eng_row) if eng_row is not None else None, cfg
        )
        dep = _build_dependency_risks(
            pd.Series(eng_row) if eng_row is not None else None, cfg
        )
        lifecycle = _build_lifecycle_risks(
            pd.Series(meta_row) if meta_row is not None else None, feat_row, cfg
        )

        # Primary diagnostic
        primary, category = _determine_primary_diagnostic(
            hist, forecast, model, engagement, dep, lifecycle, cfg, feat_row
        )

        # Action
        action = PRIMARY_DIAGNOSTIC_TO_ACTION.get(primary, "continue_monitoring")

        # Counts and severity
        issue_count, warning_count = _count_issues_and_warnings(
            hist, forecast, model, engagement, dep, lifecycle
        )
        overall_sev = _overall_severity(issue_count, warning_count, evidence_status)

        # Reasons
        reasons = _build_diagnostic_reasons(
            hist, forecast, model, engagement, dep, lifecycle,
            evidence_status, primary, action
        )

        # Privacy suppression
        _eng_series = pd.Series(eng_row) if eng_row is not None else pd.Series(dtype=object)
        privacy_status = _safe_get(_eng_series, ENG_PRIVACY_STATUS_COL, "unknown")

        row = {
            "analytics_run_id": analytics_run_id,
            "generated_at": generated_at,
            "analytics_as_of_date": _safe_get(feat_row, "analytics_as_of_date"),
            "report_id": rid,
            "report_name": _safe_get(feat_row, "report_name"),
            "diagnostic_evidence_status": evidence_status,
            "missing_diagnostic_evidence": ",".join(missing_sources) if missing_sources else None,
            "privacy_suppression_status": privacy_status,
            **hist,
            **forecast,
            **model,
            **engagement,
            **dep,
            **lifecycle,
            "primary_diagnostic": primary,
            "primary_diagnostic_category": category,
            "diagnostic_issue_count": issue_count,
            "diagnostic_warning_count": warning_count,
            "overall_diagnostic_severity": overall_sev,
            "diagnostic_summary": f"Report {rid}: {primary} — {action}",
            "diagnostic_reasons": reasons,
            "recommended_diagnostic_action": action,
            "diagnostic_review_required": overall_sev in {"poor", "warning"},
        }
        rows.append(row)

    df = pd.DataFrame(rows)[REPORT_DIAGNOSTICS_COLS]
    return df.sort_values("report_id").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_report_diagnostics(df: pd.DataFrame) -> None:
    missing = [c for c in REPORT_DIAGNOSTICS_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if df.duplicated(subset=["analytics_run_id", "report_id"]).any():
        raise ValueError("Duplicate (analytics_run_id, report_id) grain")

    bad_cols = set(df.columns) & PROHIBITED_DIAGNOSTIC_COLS
    if bad_cols:
        raise ValueError(f"Prohibited columns: {bad_cols}")

    bad_actions = set(df["recommended_diagnostic_action"].dropna()) & PROHIBITED_ACTIONS
    if bad_actions:
        raise ValueError(f"Prohibited actions: {bad_actions}")

    invalid_actions = set(df["recommended_diagnostic_action"].dropna()) - ALLOWED_RECOMMENDED_ACTIONS
    if invalid_actions:
        raise ValueError(f"Invalid actions: {invalid_actions}")

    invalid_primaries = set(df["primary_diagnostic"].dropna()) - ALLOWED_PRIMARY_DIAGNOSTICS
    if invalid_primaries:
        raise ValueError(f"Invalid primary diagnostics: {invalid_primaries}")

    invalid_cats = set(df["primary_diagnostic_category"].dropna()) - ALLOWED_PRIMARY_CATEGORIES
    if invalid_cats:
        raise ValueError(f"Invalid categories: {invalid_cats}")

    invalid_sevs = set(df["overall_diagnostic_severity"].dropna()) - ALLOWED_SEVERITIES
    if invalid_sevs:
        raise ValueError(f"Invalid severities: {invalid_sevs}")

    # No personal identifiers
    email_pat = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
    for col in df.select_dtypes(include="object").columns:
        for val in df[col].dropna().astype(str):
            if email_pat.search(val):
                raise ValueError(f"Email-like value found in column {col}")

    # Concentration risk must not be raised from suppressed metrics
    suppressed_mask = df["privacy_suppression_status"].isin(["privacy_suppressed", "partial_suppression"])
    if (df.loc[suppressed_mask, "concentrated_dependency_risk"] == True).any():
        raise ValueError("concentrated_dependency_risk raised despite privacy suppression")

    # No retirement recommendations
    if "retire_report" in df["recommended_diagnostic_action"].values:
        raise ValueError("retire_report is a prohibited action")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_report_diagnostics(df: pd.DataFrame, project_root: Path) -> Path:
    validate_report_diagnostics(df)
    out_dir = project_root / "outputs" / "analytics"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "report_diagnostics.csv"
    df.sort_values(["analytics_run_id", "report_id"]).to_csv(out_path, index=False)
    return out_path
