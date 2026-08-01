"""Pure-logic helpers for the report-level explorer.

These functions contain no Streamlit calls and can be tested independently.
All inputs and outputs are plain Python types (dicts, DataFrames, pd.Series).
"""

from __future__ import annotations

from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Report-level GenAI data contract (Sprint 8 schema)
# ---------------------------------------------------------------------------

REPORT_GENAI_FIELDS: tuple[str, ...] = (
    "executive_summary",
    "usage_insight",
    "engagement_insight",
    "forecast_insight",
    "model_confidence_note",
    "recommended_action",
    "evidence_limitations",
)

# Older aliases still present in the JSON — consulted only when the Sprint-8
# field is absent or null.
_GENAI_LEGACY_ALIASES: dict[str, list[str]] = {
    "executive_summary":    ["forecast_summary"],
    "recommended_action":   ["recommended_actions"],
    "evidence_limitations": ["hypotheses"],
}

GENAI_STATE_LABELS: dict[str, str] = {
    "valid":      "LLM-generated (validated)",
    "reused":     "Reused (validated result)",
    "rule_based": "Deterministic summary (rule-based)",
    "fallback":   "Fallback summary",
    "invalid":    "Validation failed — narrative not displayed",
    "missing":    "Not available",
}


# ---------------------------------------------------------------------------
# Report-detail data contract
# ---------------------------------------------------------------------------

# Required fields: their absence degrades the *whole* summary header.
REPORT_DETAIL_REQUIRED: tuple[str, ...] = (
    "report_id",
    "analytics_run_id",
    "analytics_as_of_date",
    "overall_report_status",
    "overall_review_priority",
    "recommended_report_action",
)

# Optional fields: their absence degrades only the section that uses them.
REPORT_DETAIL_OPTIONAL: tuple[str, ...] = (
    # Identity
    "report_name", "workspace_name", "criticality_level", "expected_usage_cadence",
    # Historical usage
    "recent_28d_views", "previous_28d_views", "usage_change_28d_pct",
    "historical_usage_status", "days_since_last_use", "current_zero_usage_streak_days",
    "usage_volatility_status", "latest_usage_anomaly_status", "history_sufficient_28d",
    # Forecast
    "forecast_total_28d", "forecast_change_vs_actual_28d_pct",
    "forecast_outlook_status", "forecast_uncertainty_status", "forecast_interpretation_status",
    "selected_model_name", "available_forecast_horizon_days",
    "forecast_lower_total_28d", "forecast_upper_total_28d",
    "forecast_as_of_date", "training_cutoff",
    # Model health
    "model_diagnostic_status", "primary_model_issue", "bias_status",
    "residual_autocorrelation_status", "interval_calibration_status",
    "production_evidence_maturity", "production_deterioration_status",
    "model_evidence_status",
    # Engagement
    "unique_users_28d", "active_user_direction_28d", "returning_user_share_28d",
    "lapse_rate_28d", "retained_user_rate_28d", "views_per_active_user_28d",
    "top_1_user_view_share_28d", "overall_engagement_status",
    "engagement_evidence_status", "privacy_suppression_status", "privacy_suppressed_fields",
    # Decision
    "overall_evidence_status", "primary_diagnostic", "primary_diagnostic_category",
    "report_reasons",
)


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def report_display_name(mart_row: pd.Series) -> str:
    """Return the report display name, falling back to report_id."""
    name = mart_row.get("report_name")
    if pd.notna(name) and str(name).strip():
        return str(name).strip()
    rid = mart_row.get("report_id")
    if pd.notna(rid) and str(rid).strip():
        return str(rid).strip()
    return "Unknown report"


def fmt_pct_change(value: Any) -> str:
    """Safe ratio-to-percentage formatter.

    Expects a ratio (0.23 → '+23.0%').  Returns '—' when value is null.
    """
    if pd.isna(value):
        return "—"
    try:
        pct = float(value) * 100
        sign = "+" if pct > 0 else ""
        return f"{sign}{pct:.1f}%"
    except (TypeError, ValueError):
        return "—"


def suppression_aware_metric(
    value: Any,
    *,
    suppressed: bool = False,
    insufficient: bool = False,
    fmt_fn: Any = None,
) -> str:
    """Return a display string that respects suppression and insufficiency.

    Priority order:
    1. suppressed → 'Suppressed (privacy)'
    2. insufficient → 'Insufficient history'
    3. pd.isna(value) → '—'
    4. fmt_fn(value) if provided, else str(value)
    """
    if suppressed:
        return "Suppressed (privacy)"
    if insufficient:
        return "Insufficient history"
    if pd.isna(value):
        return "—"
    if fmt_fn is not None:
        return fmt_fn(value)
    return str(value)


def parse_report_reasons(raw: Any) -> list[str]:
    """Parse the pipeline 'key:value | key:value' reasons format.

    Returns an empty list for null or blank inputs.
    Each returned element is a trimmed part string (e.g. 'status:growing').
    """
    if pd.isna(raw) or not str(raw).strip():
        return []
    return [p.strip() for p in str(raw).split("|") if p.strip()]


def is_field_suppressed(eng_row: pd.Series, field_keyword: str) -> bool:
    """Return True when the engagement mart marks a field group as suppressed.

    Checks the boolean flag columns (*_privacy_suppressed) and the
    free-text privacy_suppressed_fields string.
    """
    if eng_row.empty:
        return False
    # Boolean flag columns use {activity,cohort,frequency,concentration}_privacy_suppressed
    flag_col = f"{field_keyword}_privacy_suppressed"
    if flag_col in eng_row.index:
        v = eng_row[flag_col]
        if isinstance(v, (bool, int)):
            if v:
                return True
        elif isinstance(v, str) and v.strip().lower() in ("true", "1", "yes"):
            return True
    # Free-text field list
    fields_str = str(eng_row.get("privacy_suppressed_fields", "") or "")
    return field_keyword in fields_str


# ---------------------------------------------------------------------------
# GenAI state classification
# ---------------------------------------------------------------------------

def classify_genai_state(
    insight_row: pd.Series,
) -> str:
    """Classify the GenAI generation state from a report insight row.

    Returns one of:
        "valid"      — LLM-generated and validation_status == 'valid'
        "reused"     — hash-matched reuse (api_attempts == 0 or generation_mode contains 'reused')
        "rule_based" — deterministic fallback
        "fallback"   — error-path fallback
        "invalid"    — validation_status != 'valid'
        "missing"    — no row available
    """
    if insight_row.empty:
        return "missing"

    validation_status = str(insight_row.get("validation_status", "")).strip().lower()
    generation_status = str(insight_row.get("generation_status", "")).strip().lower()
    generation_mode   = str(insight_row.get("generation_mode", "")).strip().lower()

    if validation_status not in ("valid", ""):
        return "invalid"

    if generation_status == "rule_based" or "rule_based" in generation_mode:
        return "rule_based"

    if "fallback" in generation_status or "fallback" in generation_mode:
        return "fallback"

    # Hash-reuse: api_attempts == 0 means no live call was made
    try:
        attempts = int(insight_row.get("api_attempts", -1))
        if attempts == 0:
            return "reused"
    except (TypeError, ValueError):
        pass

    if "reused" in generation_mode or "cached" in generation_mode:
        return "reused"

    return "valid"


def get_genai_field(insight_row: pd.Series, field: str) -> Any:
    """Return a Sprint-8 field with legacy alias fallback.

    Returns None when neither the primary field nor any alias is populated.
    """
    if insight_row.empty:
        return None
    val = insight_row.get(field)
    if val is not None and pd.notna(val) and str(val).strip():
        return val
    for alias in _GENAI_LEGACY_ALIASES.get(field, []):
        val = insight_row.get(alias)
        if val is not None and pd.notna(val) and str(val).strip():
            return val
    return None


# ---------------------------------------------------------------------------
# Report-detail payload assembly
# ---------------------------------------------------------------------------

def _safe_get(row: pd.Series, field: str) -> Any:
    """Return field value or None when absent/NaN."""
    val = row.get(field)
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return val


def build_report_detail(
    mart_row: pd.Series,
    eng_row: pd.Series,
    insight_row: pd.Series,
) -> dict[str, Any]:
    """Assemble a structured report-detail payload from validated mart fields.

    Each section is a sub-dict so callers can render sections independently
    and verify section separation in tests.  Missing values are None — never
    fabricated.
    """
    g = _safe_get  # alias

    # Engagement suppression flags
    activity_sup     = is_field_suppressed(eng_row, "activity")
    cohort_sup       = is_field_suppressed(eng_row, "cohort")
    frequency_sup    = is_field_suppressed(eng_row, "frequency")
    concentration_sup = is_field_suppressed(eng_row, "concentration")
    any_sup          = any([activity_sup, cohort_sup, frequency_sup, concentration_sup])

    # Prefer mart_analytics engagement fields; fall back to eng_row for extras
    def _eng(field: str) -> Any:
        v = g(mart_row, field)
        if v is None and not eng_row.empty:
            v = g(eng_row, field)
        return v

    genai_state = classify_genai_state(insight_row)

    return {
        "identity": {
            "report_id":            g(mart_row, "report_id"),
            "report_name":          g(mart_row, "report_name"),
            "workspace_name":       g(mart_row, "workspace_name"),
            "criticality_level":    g(mart_row, "criticality_level"),
            "expected_usage_cadence": g(mart_row, "expected_usage_cadence"),
            "analytics_run_id":     g(mart_row, "analytics_run_id"),
            "analytics_as_of_date": g(mart_row, "analytics_as_of_date"),
        },
        "historical_usage": {
            "recent_28d_views":             g(mart_row, "recent_28d_views"),
            "previous_28d_views":           g(mart_row, "previous_28d_views"),
            "usage_change_28d_pct":         g(mart_row, "usage_change_28d_pct"),
            "historical_usage_status":      g(mart_row, "historical_usage_status"),
            "days_since_last_use":          g(mart_row, "days_since_last_use"),
            "current_zero_usage_streak_days": g(mart_row, "current_zero_usage_streak_days"),
            "usage_volatility_status":      g(mart_row, "usage_volatility_status"),
            "latest_usage_anomaly_status":  g(mart_row, "latest_usage_anomaly_status"),
            "history_sufficient_28d":       g(mart_row, "history_sufficient_28d"),
        },
        "forecast": {
            "forecast_total_28d":               g(mart_row, "forecast_total_28d"),
            "forecast_change_vs_actual_28d_pct": g(mart_row, "forecast_change_vs_actual_28d_pct"),
            "forecast_outlook_status":          g(mart_row, "forecast_outlook_status"),
            "forecast_uncertainty_status":      g(mart_row, "forecast_uncertainty_status"),
            "forecast_interpretation_status":   g(mart_row, "forecast_interpretation_status"),
            "selected_model_name":              g(mart_row, "selected_model_name"),
            "available_forecast_horizon_days":  g(mart_row, "available_forecast_horizon_days"),
            "forecast_lower_total_28d":         g(mart_row, "forecast_lower_total_28d"),
            "forecast_upper_total_28d":         g(mart_row, "forecast_upper_total_28d"),
            "forecast_as_of_date":              g(mart_row, "forecast_as_of_date"),
            "training_cutoff":                  g(mart_row, "training_cutoff"),
        },
        "model_health": {
            "model_diagnostic_status":          g(mart_row, "model_diagnostic_status"),
            "primary_model_issue":              g(mart_row, "primary_model_issue"),
            "bias_status":                      g(mart_row, "bias_status"),
            "residual_autocorrelation_status":  g(mart_row, "residual_autocorrelation_status"),
            "interval_calibration_status":      g(mart_row, "interval_calibration_status"),
            "production_evidence_maturity":     g(mart_row, "production_evidence_maturity"),
            "production_deterioration_status":  g(mart_row, "production_deterioration_status"),
            "model_evidence_status":            g(mart_row, "model_evidence_status"),
        },
        "engagement": {
            "unique_users_28d":             _eng("unique_users_28d"),
            "active_user_direction_28d":    _eng("active_user_direction_28d"),
            "returning_user_share_28d":     _eng("returning_user_share_28d"),
            "lapse_rate_28d":               _eng("lapse_rate_28d"),
            "retained_user_rate_28d":       _eng("retained_user_rate_28d"),
            "views_per_active_user_28d":    _eng("views_per_active_user_28d"),
            "top_1_user_view_share_28d":    _eng("top_1_user_view_share_28d"),
            "overall_engagement_status":    _eng("overall_engagement_status"),
            "engagement_evidence_status":   _eng("engagement_evidence_status"),
            "privacy_suppression_status":   _eng("privacy_suppression_status"),
            "privacy_suppressed_fields":    _eng("privacy_suppressed_fields"),
            "_activity_suppressed":         activity_sup,
            "_cohort_suppressed":           cohort_sup,
            "_frequency_suppressed":        frequency_sup,
            "_concentration_suppressed":    concentration_sup,
            "_any_suppressed":              any_sup,
        },
        "decision": {
            "overall_report_status":        g(mart_row, "overall_report_status"),
            "overall_evidence_status":      g(mart_row, "overall_evidence_status"),
            "overall_review_priority":      g(mart_row, "overall_review_priority"),
            "primary_diagnostic":           g(mart_row, "primary_diagnostic"),
            "primary_diagnostic_category":  g(mart_row, "primary_diagnostic_category"),
            "recommended_report_action":    g(mart_row, "recommended_report_action"),
            "report_reasons":               g(mart_row, "report_reasons"),
        },
        "genai": {
            "state":     genai_state,
            "analytics_as_of_date": g(insight_row, "analytics_as_of_date") if not insight_row.empty else None,
            "generation_status":    g(insight_row, "generation_status") if not insight_row.empty else None,
            "validation_status":    g(insight_row, "validation_status") if not insight_row.empty else None,
            "prompt_version":       g(insight_row, "prompt_version") if not insight_row.empty else None,
            "model_name":           g(insight_row, "model_name") if not insight_row.empty else None,
            "generated_at":         g(insight_row, "generated_at") if not insight_row.empty else None,
            "genai_run_id":         g(insight_row, "genai_run_id") if not insight_row.empty else None,
            # Sprint-8 narrative fields (with legacy alias fallback)
            **{
                field: get_genai_field(insight_row, field)
                for field in REPORT_GENAI_FIELDS
            },
        },
    }
