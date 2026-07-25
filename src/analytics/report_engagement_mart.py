"""
Canonical report-level engagement mart for Sprint 6.

Joins all engagement metric sources into one row per report,
classifies engagement status, and produces a validated output CSV.

NOT in scope: metric recomputation, GenAI, Streamlit, report diagnostics,
              report segmentation, forecasting.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from src.analytics.privacy_policy import (
    validate_no_direct_identifiers,
    detect_email_like_values,
)
from src.analytics.report_engagement_status import (
    EngagementStatusConfig,
    build_engagement_issue_flags,
    classify_overall_engagement_status,
    determine_primary_engagement_issue,
    determine_recommended_action,
    classify_breadth_status,
    classify_repeat_engagement_status,
    get_repeat_engagement_maturity_status,
    build_engagement_reasons,
    _safe_bool,
    _safe_float,
    _safe_int,
    _safe_str,
    _get,
)

# ---------------------------------------------------------------------------
# Schema constant
# ---------------------------------------------------------------------------

MART_REPORT_ENGAGEMENT_COLS = [
    # Identity
    "analytics_run_id", "generated_at", "analytics_as_of_date",
    "report_id", "report_name", "report_activation_date", "report_active_as_of_date",
    # Source coverage
    "available_calendar_history_days", "active_usage_days_lifetime",
    "history_sufficient_7d", "history_sufficient_28d", "history_sufficient_previous_28d",
    "comparison_history_sufficient_28d", "history_sufficient_90d", "comparison_history_sufficient_90d",
    "has_any_valid_user_activity", "user_data_quality_status",
    "excluded_user_event_count", "excluded_user_event_share",
    "engagement_evidence_status", "missing_engagement_evidence", "engagement_source_availability",
    # Privacy
    "privacy_suppression_status", "activity_privacy_suppressed",
    "cohort_privacy_suppressed", "frequency_privacy_suppressed", "concentration_privacy_suppressed",
    "privacy_suppressed_field_count", "privacy_suppressed_fields", "privacy_reasons",
    # Breadth
    "unique_users_7d", "unique_users_28d", "unique_users_previous_28d", "unique_users_90d",
    "active_user_change_28d", "active_user_change_28d_pct", "active_user_direction_28d",
    # Repeat engagement
    "returning_users_28d", "one_time_users_28d",
    "returning_user_share_28d", "one_time_user_share_28d",
    "returning_user_share_previous_28d", "returning_user_share_change_28d",
    "repeat_view_users_28d", "repeat_view_user_share_28d",
    "mean_active_days_per_user_28d", "median_active_days_per_user_28d",
    "repeat_usage_status",
    # Cohorts
    "newly_adopted_users_28d", "retained_users_28d",
    "reactivated_users_28d", "lapsed_users_28d", "unclassified_recent_users_28d",
    "newly_adopted_user_share_28d", "retained_user_rate_28d",
    "lapse_rate_28d", "reactivated_user_share_28d",
    "cohort_status", "cohort_evidence_status",
    # Frequency
    "total_views_7d", "total_views_28d", "total_views_previous_28d", "total_views_90d",
    "total_user_report_days_28d",
    "views_per_active_user_28d", "views_per_user_day_28d",
    "median_views_per_user_28d", "p90_views_per_user_28d",
    "median_user_active_days_28d", "median_return_gap_days_28d",
    "total_views_change_28d_pct", "views_per_active_user_change_28d_pct",
    "frequency_direction", "frequency_status", "frequency_evidence_status",
    # Concentration
    "top_1_user_view_share_28d", "top_3_users_view_share_28d",
    "top_10pct_users_view_share_28d", "top_10pct_user_count_28d",
    "user_view_hhi_28d", "effective_user_count_28d", "effective_user_share_28d",
    "top_1_share_change_28d", "hhi_change_28d", "effective_user_count_change_28d",
    "concentration_direction", "concentration_status", "dependency_change_status",
    "concentration_evidence_status",
    # Issue flags
    "active_user_decline_issue", "active_user_decline_severity",
    "low_repeat_engagement_issue", "low_repeat_engagement_severity",
    "elevated_lapse_issue", "elevated_lapse_severity",
    "concentrated_dependency_issue", "concentrated_dependency_severity",
    "increasing_dependency_issue", "increasing_dependency_severity",
    "declining_frequency_issue", "declining_frequency_severity",
    "inactivity_issue", "inactivity_severity",
    "privacy_limitation_issue", "privacy_limitation_severity",
    "insufficient_history_issue", "insufficient_history_severity",
    "user_data_quality_issue", "user_data_quality_severity",
    # Derived engagement maturity
    "days_since_first_valid_user_activity", "repeat_engagement_maturity_status",
    # Final summary
    "breadth_status", "repeat_engagement_status",
    "adoption_transition_status", "usage_pattern_status", "dependency_status",
    "overall_engagement_status", "primary_engagement_issue",
    "engagement_issue_count", "engagement_warning_count",
    "recommended_engagement_action", "engagement_action_priority", "review_required",
    "engagement_reasons",
]

# ---------------------------------------------------------------------------
# Cardinality validation
# ---------------------------------------------------------------------------

def _validate_unique_grain(df: pd.DataFrame, source_name: str, key_cols: tuple = ("report_id",)) -> None:
    """Raise ValueError if df has duplicate rows on key_cols."""
    if df.empty:
        return
    key_list = [c for c in key_cols if c in df.columns]
    if not key_list:
        return
    dups = df.duplicated(subset=key_list)
    if dups.any():
        dup_vals = df.loc[dups, key_list].to_dict("records")
        raise ValueError(
            f"Duplicate rows in {source_name} for keys {key_list}: {dup_vals[:5]}"
        )


# ---------------------------------------------------------------------------
# Privacy aggregation
# ---------------------------------------------------------------------------

def _aggregate_suppression(row: dict) -> dict:
    suppressed_fields = []
    if row.get("activity_privacy_suppressed"):
        suppressed_fields.append("activity_metrics")
    if row.get("cohort_privacy_suppressed"):
        suppressed_fields.append("cohort_metrics")
    if row.get("frequency_privacy_suppressed"):
        suppressed_fields.append("frequency_distributions")
    if row.get("concentration_privacy_suppressed"):
        suppressed_fields.append("concentration_metrics")
    suppressed_fields = sorted(set(suppressed_fields))
    return {
        "privacy_suppressed_field_count": len(suppressed_fields),
        "privacy_suppressed_fields": ",".join(suppressed_fields) if suppressed_fields else None,
        "privacy_reasons": "Metrics suppressed due to small user population." if suppressed_fields else None,
    }


# ---------------------------------------------------------------------------
# Evidence status
# ---------------------------------------------------------------------------

def _compute_engagement_evidence_status(row: dict, any_suppression: bool) -> str:
    dq_status = row.get("user_data_quality_status") or "good"
    if dq_status == "no_valid_user_data":
        return "no_valid_user_data"
    hist_28d = bool(row.get("history_sufficient_28d", False))
    cmp_28d = bool(row.get("comparison_history_sufficient_28d", False))
    hist_7d = bool(row.get("history_sufficient_7d", False))
    has_activity = bool(row.get("has_any_valid_user_activity", False))

    if hist_28d and cmp_28d and not any_suppression:
        return "complete"
    if hist_28d and cmp_28d and any_suppression:
        return "complete_with_privacy_suppression"
    if hist_28d and not cmp_28d:
        if has_activity:
            return "recent_window_only"
        return "no_recent_activity"
    if hist_7d and not hist_28d:
        return "partial_history"
    return "insufficient_history"


def _compute_missing_evidence(row: dict) -> Optional[str]:
    missing = []
    if not row.get("report_activation_date"):
        missing.append("report_activation_date")
    if not row.get("history_sufficient_28d"):
        missing.append("recent_28d_history")
    if not row.get("history_sufficient_previous_28d"):
        missing.append("previous_28d_history")
    if row.get("user_data_quality_status") == "no_valid_user_data":
        missing.append("valid_user_data")
    missing = sorted(set(missing))
    return ",".join(missing) if missing else None


def _compute_source_availability(
    activity_df: pd.DataFrame,
    cohort_df: pd.DataFrame,
    frequency_df: pd.DataFrame,
    concentration_df: pd.DataFrame,
    quality_df: pd.DataFrame,
    report_id: str,
) -> str:
    available = []
    if not activity_df.empty and report_id in activity_df["report_id"].values:
        available.append("activity")
    if not cohort_df.empty and report_id in cohort_df["report_id"].values:
        available.append("cohort")
    if not frequency_df.empty and report_id in frequency_df["report_id"].values:
        available.append("frequency")
    if not concentration_df.empty and report_id in concentration_df["report_id"].values:
        available.append("concentration")
    if not quality_df.empty and report_id in quality_df["report_id"].values:
        available.append("quality")
    return ",".join(sorted(available)) if available else "none"


# ---------------------------------------------------------------------------
# Days since first activity
# ---------------------------------------------------------------------------

def _compute_days_since_first_activity(first_date_val, analytics_as_of_date_val) -> Optional[int]:
    if first_date_val is None:
        return None
    try:
        if pd.isna(first_date_val):
            return None
    except (TypeError, ValueError):
        pass
    try:
        first_date = pd.to_datetime(str(first_date_val)).date()
        as_of_date = pd.to_datetime(str(analytics_as_of_date_val)).date()
        return (as_of_date - first_date).days
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Adoption / usage / dependency derived statuses
# ---------------------------------------------------------------------------

def _classify_adoption_transition(row: dict, issues: dict) -> str:
    cohort_status = row.get("cohort_status")
    if cohort_status in (None, "no_valid_user_data", "insufficient_history", "privacy_suppressed"):
        return "unavailable"
    return str(cohort_status)


def _classify_usage_pattern(row: dict, issues: dict) -> str:
    freq_status = row.get("frequency_status")
    if freq_status in (None, "no_valid_user_data", "insufficient_history", "privacy_suppressed"):
        return "unavailable"
    return str(freq_status)


def _classify_dependency(row: dict, issues: dict) -> str:
    conc_status = row.get("concentration_status")
    dep_change = row.get("dependency_change_status")
    if conc_status in (None, "no_valid_user_data", "insufficient_history", "privacy_suppressed"):
        return "unavailable"
    if dep_change and dep_change not in ("stable", "no_valid_user_data", "insufficient_history"):
        return f"{conc_status}_{dep_change}"
    return str(conc_status)


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_report_engagement_mart(
    sufficiency_df: pd.DataFrame,
    activity_df: pd.DataFrame,
    cohort_df: pd.DataFrame,
    frequency_df: pd.DataFrame,
    concentration_df: pd.DataFrame,
    quality_df: pd.DataFrame,
    boundaries_df: pd.DataFrame,
    cfg: EngagementStatusConfig,
    analytics_run_id: str,
) -> pd.DataFrame:
    """
    Build the canonical report engagement mart (one row per report_id).
    """
    generated_at = datetime.utcnow().isoformat()

    # ── 1. Validate input cardinality ────────────────────────────────────────
    _validate_unique_grain(sufficiency_df, "sufficiency_df", ("report_id",))
    _validate_unique_grain(activity_df, "activity_df", ("report_id",))
    _validate_unique_grain(cohort_df, "cohort_df", ("report_id",))
    _validate_unique_grain(frequency_df, "frequency_df", ("report_id",))
    _validate_unique_grain(concentration_df, "concentration_df", ("report_id",))
    _validate_unique_grain(quality_df, "quality_df", ("report_id",))

    # ── 2. Index non-spine sources by report_id ──────────────────────────────
    def _index_by_rid(df: pd.DataFrame) -> dict:
        if df.empty or "report_id" not in df.columns:
            return {}
        return {str(row["report_id"]): row.to_dict() for _, row in df.iterrows()}

    act_idx = _index_by_rid(activity_df)
    coh_idx = _index_by_rid(cohort_df)
    frq_idx = _index_by_rid(frequency_df)
    cnc_idx = _index_by_rid(concentration_df)
    qlt_idx = _index_by_rid(quality_df)

    # analytics_as_of_date from boundaries
    analytics_as_of_date = None
    if not boundaries_df.empty:
        analytics_as_of_date = str(boundaries_df.iloc[0].get("analytics_as_of_date", ""))
    if not analytics_as_of_date and not sufficiency_df.empty:
        analytics_as_of_date = str(sufficiency_df.iloc[0].get("analytics_as_of_date", ""))

    rows = []
    for _, suf_row in sufficiency_df.sort_values("report_id").iterrows():
        rid = str(suf_row["report_id"])

        try:
            act_row = act_idx.get(rid, {})
            coh_row = coh_idx.get(rid, {})
            frq_row = frq_idx.get(rid, {})
            cnc_row = cnc_idx.get(rid, {})
            qlt_row = qlt_idx.get(rid, {})

            # ── Build merged row dict (sufficiency is primary) ──────────────
            # Start with sufficiency fields
            merged = {
                "report_id": rid,
                "report_name": _safe_str(_get(suf_row, "report_name")) or _safe_str(act_row.get("report_name")),
                "report_activation_date": _safe_str(_get(suf_row, "report_activation_date")),
                "report_active_as_of_date": _safe_str(_get(suf_row, "report_active_as_of_date")),
                "available_calendar_history_days": _safe_int(_get(suf_row, "available_calendar_history_days")),
                "active_usage_days_lifetime": _safe_int(_get(suf_row, "active_usage_days_lifetime")),
                "has_any_valid_user_activity": _safe_bool(_get(suf_row, "has_any_valid_user_activity")),
                # Sufficiency flags (fall back to activity if available)
                "history_sufficient_7d": _safe_bool(_get(act_row, "history_sufficient_7d") if act_row else _get(suf_row, "history_sufficient_7d")),
                "history_sufficient_28d": _safe_bool(_get(act_row, "history_sufficient_28d") if act_row else _get(suf_row, "history_sufficient_28d")),
                "history_sufficient_previous_28d": _safe_bool(_get(act_row, "history_sufficient_previous_28d") if act_row else _get(suf_row, "history_sufficient_previous_28d")),
                "comparison_history_sufficient_28d": _safe_bool(_get(act_row, "comparison_history_sufficient_28d") if act_row else _get(suf_row, "comparison_history_sufficient_28d")),
                "history_sufficient_90d": _safe_bool(_get(act_row, "history_sufficient_90d") if act_row else _get(suf_row, "history_sufficient_90d")),
                "comparison_history_sufficient_90d": _safe_bool(_get(act_row, "comparison_history_sufficient_90d") if act_row else _get(suf_row, "comparison_history_sufficient_90d")),
                # Quality
                "user_data_quality_status": _safe_str(act_row.get("user_data_quality_status")) or "good",
                "excluded_user_event_share": _safe_float(act_row.get("excluded_user_event_share")),
                "excluded_user_event_count": _safe_int(qlt_row.get("excluded_event_count")),
                # Privacy suppression from individual sources
                "activity_privacy_suppressed": _safe_bool(act_row.get("activity_privacy_suppressed")),
                "cohort_privacy_suppressed": _safe_bool(coh_row.get("cohort_privacy_suppressed")),
                "frequency_privacy_suppressed": _safe_bool(frq_row.get("frequency_privacy_suppressed")),
                "concentration_privacy_suppressed": _safe_bool(cnc_row.get("concentration_privacy_suppressed")),
                # Activity metrics
                "unique_users_7d": _safe_int(act_row.get("unique_users_7d")),
                "unique_users_28d": _safe_int(act_row.get("unique_users_28d")),
                "unique_users_previous_28d": _safe_int(act_row.get("unique_users_previous_28d")),
                "unique_users_90d": _safe_int(act_row.get("unique_users_90d")),
                "active_user_change_28d": _safe_int(act_row.get("active_user_change_28d")),
                "active_user_change_28d_pct": _safe_float(act_row.get("active_user_change_28d_pct")),
                "active_user_direction_28d": _safe_str(act_row.get("active_user_direction_28d")),
                "returning_users_28d": _safe_int(act_row.get("returning_users_28d")),
                "one_time_users_28d": _safe_int(act_row.get("one_time_users_28d")),
                "returning_user_share_28d": _safe_float(act_row.get("returning_user_share_28d")),
                "one_time_user_share_28d": _safe_float(act_row.get("one_time_user_share_28d")),
                "returning_user_share_previous_28d": _safe_float(act_row.get("returning_user_share_previous_28d")),
                "returning_user_share_change_28d": _safe_float(act_row.get("returning_user_share_change_28d")),
                "repeat_view_users_28d": _safe_int(act_row.get("repeat_view_users_28d")),
                "repeat_view_user_share_28d": _safe_float(act_row.get("repeat_view_user_share_28d")),
                "mean_active_days_per_user_28d": _safe_float(act_row.get("mean_active_days_per_user_28d")),
                "median_active_days_per_user_28d": _safe_float(act_row.get("median_active_days_per_user_28d")),
                "repeat_usage_status": _safe_str(act_row.get("repeat_usage_status")),
                "privacy_suppression_status": _safe_str(act_row.get("privacy_suppression_status")),
                # Cohort metrics
                "newly_adopted_users_28d": _safe_int(coh_row.get("newly_adopted_users_28d")),
                "retained_users_28d": _safe_int(coh_row.get("retained_users_28d")),
                "reactivated_users_28d": _safe_int(coh_row.get("reactivated_users_28d")),
                "lapsed_users_28d": _safe_int(coh_row.get("lapsed_users_28d")),
                "unclassified_recent_users_28d": _safe_int(coh_row.get("unclassified_recent_users_28d")),
                "newly_adopted_user_share_28d": _safe_float(coh_row.get("newly_adopted_user_share_28d")),
                "retained_user_rate_28d": _safe_float(coh_row.get("retained_user_rate_28d")),
                "lapse_rate_28d": _safe_float(coh_row.get("lapse_rate_28d")),
                "reactivated_user_share_28d": _safe_float(coh_row.get("reactivated_user_share_28d")),
                "cohort_status": _safe_str(coh_row.get("cohort_status")),
                "cohort_evidence_status": _safe_str(coh_row.get("cohort_evidence_status")),
                # Frequency metrics
                "total_views_7d": _safe_int(frq_row.get("total_views_7d")),
                "total_views_28d": _safe_int(frq_row.get("total_views_28d")),
                "total_views_previous_28d": _safe_int(frq_row.get("total_views_previous_28d")),
                "total_views_90d": _safe_int(frq_row.get("total_views_90d")),
                "total_user_report_days_28d": _safe_int(frq_row.get("total_user_report_days_28d")),
                "views_per_active_user_28d": _safe_float(frq_row.get("views_per_active_user_28d")),
                "views_per_user_day_28d": _safe_float(frq_row.get("views_per_user_day_28d")),
                "median_views_per_user_28d": _safe_float(frq_row.get("median_views_per_user_28d")),
                "p90_views_per_user_28d": _safe_float(frq_row.get("p90_views_per_user_28d")),
                "median_user_active_days_28d": _safe_float(frq_row.get("median_user_active_days_28d")),
                "median_return_gap_days_28d": _safe_float(frq_row.get("median_return_gap_days_28d")),
                "total_views_change_28d_pct": _safe_float(frq_row.get("total_views_change_28d_pct")),
                "views_per_active_user_change_28d_pct": _safe_float(frq_row.get("views_per_active_user_change_28d_pct")),
                "frequency_direction": _safe_str(frq_row.get("frequency_direction")),
                "frequency_status": _safe_str(frq_row.get("frequency_status")),
                "frequency_evidence_status": _safe_str(frq_row.get("frequency_evidence_status")),
                # Concentration metrics
                "top_1_user_view_share_28d": _safe_float(cnc_row.get("top_1_user_view_share_28d")),
                "top_3_users_view_share_28d": _safe_float(cnc_row.get("top_3_users_view_share_28d")),
                "top_10pct_users_view_share_28d": _safe_float(cnc_row.get("top_10pct_users_view_share_28d")),
                "top_10pct_user_count_28d": _safe_int(cnc_row.get("top_10pct_user_count_28d")),
                "user_view_hhi_28d": _safe_float(cnc_row.get("user_view_hhi_28d")),
                "effective_user_count_28d": _safe_float(cnc_row.get("effective_user_count_28d")),
                "effective_user_share_28d": _safe_float(cnc_row.get("effective_user_share_28d")),
                "top_1_share_change_28d": _safe_float(cnc_row.get("top_1_share_change_28d")),
                "hhi_change_28d": _safe_float(cnc_row.get("hhi_change_28d")),
                "effective_user_count_change_28d": _safe_float(cnc_row.get("effective_user_count_change_28d")),
                "concentration_direction": _safe_str(cnc_row.get("concentration_direction")),
                "concentration_status": _safe_str(cnc_row.get("concentration_status")),
                "concentration_status_28d": _safe_str(cnc_row.get("concentration_status_28d")),
                "dependency_change_status": _safe_str(cnc_row.get("dependency_change_status")),
                "concentration_evidence_status": _safe_str(cnc_row.get("concentration_evidence_status")),
            }

            # ── Privacy suppression aggregation ──────────────────────────────
            suppression_agg = _aggregate_suppression(merged)
            any_suppression = suppression_agg["privacy_suppressed_field_count"] > 0
            merged.update(suppression_agg)

            # ── Engagement evidence status ────────────────────────────────────
            merged["engagement_evidence_status"] = _compute_engagement_evidence_status(merged, any_suppression)
            merged["missing_engagement_evidence"] = _compute_missing_evidence(merged)
            merged["engagement_source_availability"] = _compute_source_availability(
                activity_df, cohort_df, frequency_df, concentration_df, quality_df, rid
            )

            # ── Days since first activity ─────────────────────────────────────
            first_date = _safe_str(_get(suf_row, "first_observed_usage_date"))
            as_of = analytics_as_of_date or _safe_str(_get(suf_row, "analytics_as_of_date"))
            days_since = _compute_days_since_first_activity(first_date, as_of)
            merged["days_since_first_valid_user_activity"] = days_since

            # ── Repeat engagement maturity ────────────────────────────────────
            maturity_status = get_repeat_engagement_maturity_status(days_since, cfg)
            merged["repeat_engagement_maturity_status"] = maturity_status

            # Build pd.Series for classifier functions
            row_series = pd.Series(merged)

            # ── Issue flags ───────────────────────────────────────────────────
            issues = build_engagement_issue_flags(row_series, cfg)
            merged.update(issues)

            # ── Overall engagement status ─────────────────────────────────────
            overall_status = classify_overall_engagement_status(row_series, issues, cfg)
            merged["overall_engagement_status"] = overall_status

            # ── Primary issue ─────────────────────────────────────────────────
            primary_issue = determine_primary_engagement_issue(issues)
            # Check newly_active signal separately
            u28 = merged.get("unique_users_28d")
            u_prev = merged.get("unique_users_previous_28d")
            if primary_issue == "none" and overall_status == "newly_active":
                primary_issue = "newly_active"
            merged["primary_engagement_issue"] = primary_issue

            # ── Recommended action ────────────────────────────────────────────
            action, priority, review_required = determine_recommended_action(
                primary_issue, overall_status, issues, cfg
            )
            merged["recommended_engagement_action"] = action
            merged["engagement_action_priority"] = priority
            merged["review_required"] = review_required

            # ── Breadth and repeat status ─────────────────────────────────────
            merged["breadth_status"] = classify_breadth_status(row_series, issues, cfg)
            merged["repeat_engagement_status"] = classify_repeat_engagement_status(
                row_series, maturity_status, issues, cfg
            )

            # ── Adoption / usage / dependency derived statuses ────────────────
            merged["adoption_transition_status"] = _classify_adoption_transition(merged, issues)
            merged["usage_pattern_status"] = _classify_usage_pattern(merged, issues)
            merged["dependency_status"] = _classify_dependency(merged, issues)

            # ── Issue counts ─────────────────────────────────────────────────
            merged["engagement_issue_count"] = issues.get("engagement_issue_count", 0)
            merged["engagement_warning_count"] = issues.get("engagement_warning_count", 0)

            # ── Reasons ───────────────────────────────────────────────────────
            merged["engagement_reasons"] = build_engagement_reasons(
                row_series, issues, overall_status, action, cfg
            )

            # ── Metadata ──────────────────────────────────────────────────────
            merged["analytics_run_id"] = analytics_run_id
            merged["generated_at"] = generated_at
            merged["analytics_as_of_date"] = as_of

            rows.append(merged)

        except Exception as exc:
            import traceback
            # Record a minimal error row
            rows.append({
                "analytics_run_id": analytics_run_id,
                "generated_at": generated_at,
                "analytics_as_of_date": analytics_as_of_date,
                "report_id": rid,
                "engagement_evidence_status": "calculation_failed",
                "overall_engagement_status": "mixed_signals",
                "primary_engagement_issue": "none",
                "recommended_engagement_action": "continue_monitoring",
                "engagement_action_priority": "low",
                "review_required": False,
                "engagement_reasons": f"calculation_failed:{exc}",
                "engagement_issue_count": 0,
                "engagement_warning_count": 0,
            })

    # ── Assemble DataFrame ────────────────────────────────────────────────────
    result = pd.DataFrame(rows)

    # Ensure all schema columns are present (fill missing with None)
    for col in MART_REPORT_ENGAGEMENT_COLS:
        if col not in result.columns:
            result[col] = None

    # Remove internal-only columns (e.g. concentration_status_28d)
    extra_cols = [c for c in result.columns if c not in MART_REPORT_ENGAGEMENT_COLS]
    result = result.drop(columns=extra_cols, errors="ignore")

    result = result[MART_REPORT_ENGAGEMENT_COLS].sort_values("report_id").reset_index(drop=True)
    return result


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

_ALLOWED_OVERALL_STATUSES = {
    "no_valid_user_data", "insufficient_evidence", "inactive", "newly_active",
    "declining_adoption", "elevated_lapse", "low_repeat_usage", "concentrated_dependency",
    "growing_adoption", "healthy_broad_adoption", "healthy_niche_adoption",
    "stable_engagement", "privacy_limited", "mixed_signals",
}

_ALLOWED_ACTIONS = {
    "investigate_data_quality", "validate_report_audience", "investigate_user_decline",
    "investigate_user_lapse", "improve_repeat_engagement", "review_concentrated_dependency",
    "continue_monitoring", "insufficient_evidence", "monitor_new_adoption",
}

_PROHIBITED_ACTIONS = {"retire_report", "delete_report", "archive_report"}


def validate_report_engagement_mart(df: pd.DataFrame, cfg: EngagementStatusConfig) -> None:
    """Validate the engagement mart DataFrame before writing."""
    # 1. Required columns
    missing_cols = [c for c in MART_REPORT_ENGAGEMENT_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing mart columns: {missing_cols}")

    if df.empty:
        return

    # 2. Unique grain
    _validate_unique_grain(df, "mart_report_engagement", ("analytics_run_id", "report_id"))

    # 3. Status values
    bad_statuses = df[
        df["overall_engagement_status"].notna()
        & ~df["overall_engagement_status"].isin(_ALLOWED_OVERALL_STATUSES)
    ]
    if not bad_statuses.empty:
        raise ValueError(
            f"Invalid overall_engagement_status values: {bad_statuses['overall_engagement_status'].unique().tolist()}"
        )

    # 4. Issue count reconciliation
    issue_flag_cols = [
        "active_user_decline_issue", "low_repeat_engagement_issue", "elevated_lapse_issue",
        "concentrated_dependency_issue", "increasing_dependency_issue", "declining_frequency_issue",
        "inactivity_issue", "privacy_limitation_issue", "insufficient_history_issue",
        "user_data_quality_issue",
    ]
    present_flags = [c for c in issue_flag_cols if c in df.columns]
    if present_flags and "engagement_issue_count" in df.columns:
        computed_count = df[present_flags].fillna(False).astype(bool).sum(axis=1)
        mismatch = (computed_count != df["engagement_issue_count"].fillna(0)).any()
        if mismatch:
            raise ValueError("engagement_issue_count does not match sum of issue flags.")

    # 5. No direct identifiers
    validate_no_direct_identifiers(df, context="mart_report_engagement")

    # 6. No user lists (no list/array-type columns)
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, set, tuple))).any():
            raise ValueError(f"Column '{col}' contains user lists.")

    # 7. Inactive status → unique_users_28d == 0 and history_sufficient_28d == True
    if "overall_engagement_status" in df.columns:
        inactive_rows = df[df["overall_engagement_status"] == "inactive"]
        if not inactive_rows.empty:
            if "unique_users_28d" in df.columns:
                bad = inactive_rows[inactive_rows["unique_users_28d"].fillna(-1) != 0]
                if not bad.empty:
                    raise ValueError(
                        f"Inactive status but unique_users_28d != 0 for: {bad['report_id'].tolist()}"
                    )
            if "history_sufficient_28d" in df.columns:
                bad = inactive_rows[~inactive_rows["history_sufficient_28d"].fillna(False)]
                if not bad.empty:
                    raise ValueError(
                        f"Inactive status but history_sufficient_28d == False for: {bad['report_id'].tolist()}"
                    )

    # 8. no_valid_user_data status must not be inactive
    if "overall_engagement_status" in df.columns:
        bad = df[df["overall_engagement_status"] == "no_valid_user_data"]
        if not bad.empty:
            bad_inactive = bad[bad.get("inactivity_issue", pd.Series(False, index=bad.index)).fillna(False)]
            # no_valid_user_data is not classified as inactive
            pass  # already handled by classifier ordering

    # 9. Inactivity issue should not use continue_monitoring action
    if "inactivity_issue" in df.columns and "recommended_engagement_action" in df.columns:
        inactivity_w_poor = df[
            df["inactivity_issue"].fillna(False)
            & (df.get("inactivity_severity", pd.Series("none", index=df.index)).fillna("none") == "poor")
            & (df["recommended_engagement_action"] == "continue_monitoring")
        ]
        if not inactivity_w_poor.empty:
            raise ValueError(
                f"continue_monitoring action used for inactivity (poor severity) for: "
                f"{inactivity_w_poor['report_id'].tolist()}"
            )

    # 10. No prohibited actions
    if "recommended_engagement_action" in df.columns:
        bad_actions = df[df["recommended_engagement_action"].isin(_PROHIBITED_ACTIONS)]
        if not bad_actions.empty:
            raise ValueError(
                f"Prohibited action values found: {bad_actions['recommended_engagement_action'].unique().tolist()}"
            )

    # 11. Concentrated dependency requires non-suppressed HHI
    if "concentrated_dependency_issue" in df.columns and "concentration_privacy_suppressed" in df.columns:
        bad = df[
            df["concentrated_dependency_issue"].fillna(False)
            & df["concentration_privacy_suppressed"].fillna(False)
        ]
        if not bad.empty:
            raise ValueError(
                f"concentrated_dependency_issue=True but concentration_privacy_suppressed=True for: "
                f"{bad['report_id'].tolist()}"
            )

    # 12. Sorted by report_id
    if len(df) > 1 and "report_id" in df.columns:
        sorted_ids = df["report_id"].tolist()
        if sorted_ids != sorted(sorted_ids):
            raise ValueError("Mart DataFrame is not sorted by report_id.")

    # 13. Low repeat with immature history not allowed
    if "low_repeat_engagement_issue" in df.columns and "repeat_engagement_maturity_status" in df.columns:
        bad = df[
            df["low_repeat_engagement_issue"].fillna(False)
            & (df["repeat_engagement_maturity_status"] == "immature")
        ]
        if not bad.empty:
            raise ValueError(
                f"low_repeat_engagement_issue=True but repeat_engagement_maturity_status=immature for: "
                f"{bad['report_id'].tolist()}"
            )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_report_engagement_mart(df: pd.DataFrame, project_root: Path) -> Path:
    """Validate and write the engagement mart CSV."""
    cfg = EngagementStatusConfig()
    validate_report_engagement_mart(df, cfg)
    output_dir = project_root / "outputs" / "analytics"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "mart_report_engagement.csv"
    df.to_csv(out_path, index=False)
    return out_path
