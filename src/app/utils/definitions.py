"""Centralised definitions and label mappings for the Streamlit reviewer app.

This module is the authoritative source for:
  - DEFINITIONS: human-readable explanations of pipeline concepts
  - STATUS_LABELS: internal pipeline code → display string
  - status_label(): safe lookup with graceful unknown handling

Other utility modules (portfolio_helpers, report_helpers) may keep their own
self-contained copies for backward compatibility; this module provides the
comprehensive superset used by filter_helpers and the sidebar.
"""

from __future__ import annotations

from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Concept definitions
# ---------------------------------------------------------------------------

DEFINITIONS: dict[str, str] = {
    "active_report": (
        "A report that has received at least one view in the most recent 28-day window."
    ),
    "recent_usage": (
        "The total number of views in the most recent 28-day window "
        "(recent_28d_views in the analytics mart)."
    ),
    "historical_usage_status": (
        "A classification of the report's usage pattern over the recent historical window. "
        "Determined by the analytics pipeline from trend slope, volatility, and inactivity signals. "
        "Possible values: growing usage, stable (regular), stable (intermittent), bursty, "
        "declining usage, inactive."
    ),
    "forecast_outlook": (
        "The expected direction of usage over the forecast horizon based on the selected "
        "time-series model. Possible values: growth expected, stable outlook, "
        "reactivation expected, uncertain, decline expected. "
        "A forecast outlook is an estimate — it is not a commitment or a guarantee."
    ),
    "prediction_interval": (
        "The shaded range in the forecast chart. It describes the spread of plausible "
        "individual future view counts based on the model's uncertainty. "
        "It is not a confidence interval for a statistical parameter — "
        "it is a range of possible outcomes for a single future observation."
    ),
    "forecast_interpretation_status": (
        "The pipeline's assessment of how much weight to place on the numerical forecast, "
        "given the current model health and evidence. "
        "'Insufficient model evidence' means the model has not yet accumulated enough "
        "production history to validate its outputs — it does not mean the forecast is wrong."
    ),
    "model_diagnostic_status": (
        "The overall health classification of the forecasting model for this report. "
        "Derived from backtest performance and production monitoring outputs. "
        "Possible values: healthy, sufficient evidence, insufficient evidence, degraded, failing."
    ),
    "insufficient_evidence": (
        "The model has not yet accumulated enough production run history to assess its health. "
        "This is an evidence-maturity status — it does not mean the model is producing "
        "poor forecasts or that the report is experiencing usage problems."
    ),
    "engagement_status": (
        "A classification of how broadly and regularly people use the report. "
        "Possible values: healthy (broad), healthy (niche), growing, elevated lapse, "
        "declining, inactive. "
        "Low engagement does not imply low business value — a niche but loyal audience "
        "may reflect a specialist report with high relevance to a small team."
    ),
    "retention": (
        "The share of users who were active in the previous 28-day window and also "
        "returned in the current window (retained_user_rate_28d)."
    ),
    "lapse": (
        "The share of previously active users who did not return in the current 28-day "
        "window (lapse_rate_28d). A higher lapse rate may indicate declining relevance, "
        "but it may also reflect intermittent usage patterns."
    ),
    "concentration": (
        "The degree to which views are attributable to a small number of users, "
        "measured by the top-user share (top_1_user_view_share_28d) and HHI. "
        "High concentration is a dependency risk — it does not indicate misuse."
    ),
    "privacy_suppression": (
        "Some engagement metrics cannot be displayed because the user population for "
        "this report is too small to share safely. Suppressed fields are shown as "
        "'Suppressed (privacy)' — they never appear as zero."
    ),
    "review_priority": (
        "How urgently the report should be reviewed. Assigned deterministically by the "
        "analytics pipeline based on the combination of status, diagnostic, and evidence fields. "
        "Possible values: low, medium, high, critical."
    ),
    "recommended_action": (
        "The specific review action suggested by the analytics pipeline. "
        "This is a recommendation — it has not been executed and does not trigger "
        "any automated workflow. Possible values: continue monitoring, "
        "investigate usage decline, review planned deprecation, "
        "review forecast uncertainty, review model health."
    ),
    "evidence_status": (
        "How complete the analytical evidence is for this report across all signal types "
        "(usage, forecast, model health, engagement). "
        "'Complete' means all major evidence sources were available for this analytics run."
    ),
    "deterministic_shortlist": (
        "The attention shortlist is produced by a deterministic sort of the canonical mart: "
        "reports sorted by review priority (critical → high → medium → low) then "
        "alphabetically by report ID. It is capped at 5 and excludes 'continue monitoring' "
        "reports. It is not re-ranked by any score computed in Streamlit."
    ),
    "genai_summary": (
        "A structured narrative summary produced by a language model (or the rule-based "
        "fallback) after the analytics pipeline has completed. The LLM explains validated "
        "analytical conclusions — it does not calculate or decide them. "
        "The analytics layer calculates and decides; the GenAI layer explains."
    ),
    "rule_based_fallback": (
        "When the language model API is unavailable or produces invalid output, "
        "the pipeline generates a deterministic summary directly from validated mart fields. "
        "This fallback is clearly labelled and carries the same analytical grounding "
        "as an LLM-generated summary — the difference is in phrasing, not in the "
        "underlying evidence."
    ),
}


# ---------------------------------------------------------------------------
# Status label mappings (authoritative master copy)
# ---------------------------------------------------------------------------

STATUS_LABELS: dict[str, str] = {
    # historical_usage_status
    "growing_usage":              "Growing usage",
    "stable_regular_usage":       "Stable (regular)",
    "stable_intermittent_usage":  "Stable (intermittent)",
    "bursty_usage":               "Bursty",
    "declining_usage":            "Declining usage",
    "prolonged_inactivity":       "Inactive",
    # forecast_outlook_status
    "growth_expected":            "Growth expected",
    "stable_outlook":             "Stable outlook",
    "reactivation_expected":      "Reactivation expected",
    "uncertain_outlook":          "Uncertain",
    "decline_expected":           "Decline expected",
    # overall_engagement_status
    "healthy_broad_adoption":     "Healthy – broad",
    "healthy_niche_adoption":     "Healthy – niche",
    "growing_adoption":           "Growing adoption",
    "declining_adoption":         "Declining adoption",
    "elevated_lapse":             "Elevated lapse",
    "inactive":                   "Inactive",
    # model_diagnostic_status / model_evidence_status
    "healthy":                    "Healthy",
    "sufficient_evidence":        "Sufficient evidence",
    "insufficient_evidence":      "Insufficient evidence",
    "degraded":                   "Degraded",
    "failing":                    "Failing",
    # overall_review_priority
    "low":                        "Low",
    "medium":                     "Medium",
    "high":                       "High",
    "critical":                   "Critical",
    # recommended_report_action
    "continue_monitoring":        "Continue monitoring",
    "investigate_usage_decline":  "Investigate usage decline",
    "review_planned_deprecation": "Review planned deprecation",
    "review_forecast_uncertainty":"Review forecast uncertainty",
    "review_model_health":        "Review model health",
    # overall_report_status
    "growing":                    "Growing",
    "declining":                  "Declining",
    "planned_deprecation":        "Planned deprecation",
    "forecast_uncertain":         "Forecast uncertain",
    # overall_evidence_status / evidence-related
    "complete":                   "Complete",
    "partial":                    "Partial",
    "insufficient":               "Insufficient",
    "incomplete":                 "Incomplete",
    # forecast_interpretation_status
    "normal":                     "Normal",
    "insufficient_model_evidence":"Insufficient model evidence",
    "low_model_confidence":       "Low model confidence",
    # privacy_suppression_status
    "not_suppressed":             "Not suppressed",
    "suppressed":                 "Suppressed (privacy)",
    # report_category
    "Dashboard":                  "Dashboard",
    "Report":                     "Report",
    "Paginated":                  "Paginated",
    # concentration / dependency
    "no_deterioration":           "No deterioration",
    "deteriorating":              "Deteriorating",
    # direction
    "increasing":                 "Increasing",
    "decreasing":                 "Decreasing",
    "stable":                     "Stable",
    # generic / catch-all
    "none":                       "None",
    "unknown":                    "Unknown",
    "ok":                         "OK",
}


def status_label(code: Any) -> str:
    """Return a human-readable display label for a pipeline status code.

    Falls back to title-casing the code with underscores replaced by spaces
    so unknown future codes are still readable.  Never returns None.
    """
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return "—"
    raw = str(code).strip()
    if not raw or raw.lower() in ("nan", "none", ""):
        return "—"
    return STATUS_LABELS.get(raw, raw.replace("_", " ").replace("-", " ").title())
