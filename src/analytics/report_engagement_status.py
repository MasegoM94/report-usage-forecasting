"""
Deterministic engagement status classifier for Sprint 6.

Scope: issue flag builder, overall engagement status classifier,
       breadth/repeat engagement status classifiers, reason builder.

NOT in scope: metric computation, GenAI, Streamlit, report diagnostics,
              report segmentation, forecasting.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EngagementStatusConfig:
    # Niche classification
    NICHE_ACTIVE_USER_MAX: int = 10
    NICHE_RETURNING_SHARE_MIN: float = 0.25
    NICHE_MEDIAN_ACTIVE_DAYS_MIN: float = 1.5
    # Repeat engagement thresholds
    LOW_REPEAT_SHARE_THRESHOLD: float = 0.25   # returning_share < 25% → low
    MODERATE_REPEAT_SHARE_THRESHOLD: float = 0.50  # >= 50% → strong
    # Lapse thresholds
    ELEVATED_LAPSE_THRESHOLD: float = 0.40     # lapse_rate >= 40% → elevated
    STRONG_RETENTION_THRESHOLD: float = 0.70   # retained_rate >= 70% → strong
    # Active-user change thresholds
    DECLINE_MATERIAL_PCT: float = 0.20         # >= 20% drop → material decline
    GROWTH_MATERIAL_PCT: float = 0.20          # >= 20% gain → material growth
    MIN_ABSOLUTE_CHANGE: int = 2               # minimum absolute user change
    # Frequency thresholds
    HIGH_FREQUENCY_MEDIAN_ACTIVE_DAYS: float = 5.0
    MODERATE_FREQUENCY_MEDIAN_ACTIVE_DAYS: float = 2.0
    # Maturity thresholds
    IMMATURE_DAYS_THRESHOLD: int = 14
    MATURING_DAYS_THRESHOLD: int = 28
    # Concentration
    CONCENTRATION_ISSUE_HHI: float = 0.35
    INCREASING_DEPENDENCY_TOP1_CHANGE: float = 0.05
    # Action mapping thresholds (derived from issue priority)
    MIN_POOR_ISSUES_FOR_HIGH_PRIORITY: int = 2
    MIN_WARNING_ISSUES_FOR_MEDIUM_PRIORITY: int = 2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _safe_bool(val, default: bool = False) -> bool:
    if val is None:
        return default
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    return bool(val)


def _safe_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _safe_str(val) -> Optional[str]:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return str(val)


def _get(row, key, default=None):
    """Get a value from a row (dict or pd.Series), returning default if missing or null."""
    try:
        val = row[key]
    except (KeyError, IndexError):
        return default
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    return val


# ---------------------------------------------------------------------------
# Maturity status
# ---------------------------------------------------------------------------

def get_repeat_engagement_maturity_status(
    days_since_first_activity: Optional[int],
    cfg: EngagementStatusConfig,
) -> str:
    """
    Returns "immature" (<14d), "maturing" (14-27d), "mature" (>=28d), "unavailable" (None).
    """
    if days_since_first_activity is None:
        return "unavailable"
    d = int(days_since_first_activity)
    if d < cfg.IMMATURE_DAYS_THRESHOLD:
        return "immature"
    if d < cfg.MATURING_DAYS_THRESHOLD:
        return "maturing"
    return "mature"


# ---------------------------------------------------------------------------
# Issue flag builder
# ---------------------------------------------------------------------------

def build_engagement_issue_flags(
    row: pd.Series,
    cfg: EngagementStatusConfig,
) -> dict:
    """
    Evaluate all engagement issue flags for a single report row.

    Input: combined row from the merged mart input DataFrame.
    Returns: dict of issue flags, severities, reasons, and counts.
    """
    # Pull required fields
    comparison_sufficient = _safe_bool(_get(row, "comparison_history_sufficient_28d"))
    history_sufficient_28d = _safe_bool(_get(row, "history_sufficient_28d"))
    has_any_activity = _safe_bool(_get(row, "has_any_valid_user_activity"))
    dq_status = _safe_str(_get(row, "user_data_quality_status")) or "good"

    unique_users_28d = _safe_int(_get(row, "unique_users_28d"))
    unique_users_prev = _safe_int(_get(row, "unique_users_previous_28d"))
    active_user_direction = _safe_str(_get(row, "active_user_direction_28d"))
    active_user_change = _safe_float(_get(row, "active_user_change_28d"))
    active_user_change_pct = _safe_float(_get(row, "active_user_change_28d_pct"))

    returning_share = _safe_float(_get(row, "returning_user_share_28d"))
    repeat_usage_status = _safe_str(_get(row, "repeat_usage_status"))
    repeat_engagement_maturity_status = _safe_str(_get(row, "repeat_engagement_maturity_status")) or "unavailable"

    lapse_rate = _safe_float(_get(row, "lapse_rate_28d"))
    retained_rate = _safe_float(_get(row, "retained_user_rate_28d"))

    concentration_status_28d = _safe_str(_get(row, "concentration_status_28d"))
    concentration_privacy_suppressed = _safe_bool(_get(row, "concentration_privacy_suppressed"))
    concentration_direction = _safe_str(_get(row, "concentration_direction"))
    top_1_share_change = _safe_float(_get(row, "top_1_share_change_28d"))
    hhi_28d = _safe_float(_get(row, "user_view_hhi_28d"))

    frequency_direction = _safe_str(_get(row, "frequency_direction"))

    activity_privacy_suppressed = _safe_bool(_get(row, "activity_privacy_suppressed"))
    cohort_privacy_suppressed = _safe_bool(_get(row, "cohort_privacy_suppressed"))
    frequency_privacy_suppressed = _safe_bool(_get(row, "frequency_privacy_suppressed"))

    # ── 1. Active-user decline issue ─────────────────────────────────────────
    decline_issue = False
    decline_severity = "none"
    decline_reason = "No active-user decline detected."
    if comparison_sufficient and active_user_direction == "declining":
        abs_change = abs(active_user_change) if active_user_change is not None else 0
        abs_pct = abs(active_user_change_pct) if active_user_change_pct is not None else 0
        if abs_change >= cfg.MIN_ABSOLUTE_CHANGE and abs_pct >= cfg.DECLINE_MATERIAL_PCT:
            decline_issue = True
            if abs_pct >= 2 * cfg.DECLINE_MATERIAL_PCT:
                decline_severity = "poor"
                decline_reason = (
                    f"Active users declined materially by {abs_pct:.0%} "
                    f"({int(abs_change)} users) over the last 28 days."
                )
            else:
                decline_severity = "warning"
                decline_reason = (
                    f"Active users declined by {abs_pct:.0%} "
                    f"({int(abs_change)} users) over the last 28 days."
                )

    # ── 2. Low repeat engagement issue ───────────────────────────────────────
    low_repeat_issue = False
    low_repeat_severity = "none"
    low_repeat_reason = "No low repeat engagement detected."
    n_users = unique_users_28d if unique_users_28d is not None else 0
    if (
        repeat_usage_status not in ("insufficient_history", "no_valid_user_data", "privacy_suppressed", "no_recent_activity")
        and repeat_engagement_maturity_status in ("maturing", "mature")
        and n_users >= 3
        and returning_share is not None
        and returning_share < cfg.LOW_REPEAT_SHARE_THRESHOLD
        and active_user_direction not in ("newly_active",)  # newly_active reports can't have repeat history
    ):
        low_repeat_issue = True
        low_repeat_severity = "warning"
        low_repeat_reason = (
            f"Only {returning_share:.0%} of users returned on multiple dates in 28 days; "
            f"below the {cfg.LOW_REPEAT_SHARE_THRESHOLD:.0%} threshold."
        )

    # ── 3. Elevated lapse issue ──────────────────────────────────────────────
    lapse_issue = False
    lapse_severity = "none"
    lapse_reason = "No elevated lapse rate detected."
    if comparison_sufficient and lapse_rate is not None and lapse_rate >= cfg.ELEVATED_LAPSE_THRESHOLD:
        lapse_issue = True
        if lapse_rate >= 0.70:
            lapse_severity = "poor"
            lapse_reason = (
                f"Lapse rate is {lapse_rate:.0%}, indicating severe user attrition."
            )
        else:
            lapse_severity = "warning"
            lapse_reason = (
                f"Lapse rate is {lapse_rate:.0%}, above the {cfg.ELEVATED_LAPSE_THRESHOLD:.0%} threshold."
            )

    # ── 4. Concentrated dependency issue ────────────────────────────────────
    conc_issue = False
    conc_severity = "none"
    conc_reason = "No concentrated dependency detected."
    if (
        not concentration_privacy_suppressed
        and concentration_status_28d == "highly_concentrated"
    ):
        conc_issue = True
        conc_severity = "warning"
        conc_reason = (
            "Report usage is highly concentrated among a small number of users, "
            "creating dependency risk."
        )

    # ── 5. Increasing dependency issue ───────────────────────────────────────
    incr_dep_issue = False
    incr_dep_severity = "none"
    incr_dep_reason = "No increasing dependency detected."
    if (
        concentration_direction == "concentrating"
        and top_1_share_change is not None
        and top_1_share_change >= cfg.INCREASING_DEPENDENCY_TOP1_CHANGE
    ):
        incr_dep_issue = True
        incr_dep_severity = "informational"
        incr_dep_reason = (
            f"Top user share increased by {top_1_share_change:.0%} over 28 days, "
            "indicating growing concentration."
        )

    # ── 6. Declining frequency issue ─────────────────────────────────────────
    freq_decline_issue = False
    freq_decline_severity = "none"
    freq_decline_reason = "No declining frequency detected."
    if comparison_sufficient and frequency_direction == "decreasing":
        freq_decline_issue = True
        freq_decline_severity = "informational"
        freq_decline_reason = "Usage frequency per active user declined over the last 28 days."

    # ── 7. Inactivity issue ──────────────────────────────────────────────────
    inactivity_issue = False
    inactivity_severity = "none"
    inactivity_reason = "Report has active users in the recent period."
    if (
        history_sufficient_28d
        and dq_status != "no_valid_user_data"
        and (unique_users_28d is not None and unique_users_28d == 0)
    ):
        inactivity_issue = True
        inactivity_severity = "poor"
        inactivity_reason = (
            "No users accessed this report in the last 28 days despite sufficient history."
        )

    # ── 8. Privacy limitation issue ──────────────────────────────────────────
    privacy_issue = (
        activity_privacy_suppressed
        or cohort_privacy_suppressed
        or frequency_privacy_suppressed
        or concentration_privacy_suppressed
    )
    privacy_severity = "informational" if privacy_issue else "none"
    privacy_reason = (
        "Some engagement metrics are suppressed due to small user population."
        if privacy_issue
        else "No privacy suppression applied."
    )

    # ── 9. Insufficient history issue ────────────────────────────────────────
    insuf_hist_issue = (
        not history_sufficient_28d
        and dq_status != "no_valid_user_data"
    )
    insuf_hist_severity = "informational" if insuf_hist_issue else "none"
    insuf_hist_reason = (
        "Insufficient usage history to compute full 28-day engagement metrics."
        if insuf_hist_issue
        else "Sufficient history available for 28-day analysis."
    )

    # ── 10. Data quality issue ───────────────────────────────────────────────
    dq_issue = dq_status in ("poor", "no_valid_user_data")
    if dq_status == "no_valid_user_data":
        dq_severity = "poor"
        dq_reason = "No valid user data available for this report."
    elif dq_status == "poor":
        dq_severity = "warning"
        dq_reason = "User data quality is poor; engagement metrics may be unreliable."
    else:
        dq_severity = "none"
        dq_reason = "User data quality is acceptable."

    # ── Counts ──────────────────────────────────────────────────────────────
    all_issues = [
        ("active_user_decline", decline_issue, decline_severity),
        ("low_repeat_engagement", low_repeat_issue, low_repeat_severity),
        ("elevated_lapse", lapse_issue, lapse_severity),
        ("concentrated_dependency", conc_issue, conc_severity),
        ("increasing_dependency", incr_dep_issue, incr_dep_severity),
        ("declining_frequency", freq_decline_issue, freq_decline_severity),
        ("inactivity", inactivity_issue, inactivity_severity),
        ("privacy_limitation", privacy_issue, privacy_severity),
        ("insufficient_history", insuf_hist_issue, insuf_hist_severity),
        ("user_data_quality", dq_issue, dq_severity),
    ]

    issue_count = sum(1 for _, flag, _ in all_issues if flag)
    warning_count = sum(1 for _, flag, sev in all_issues if flag and sev in ("warning", "poor"))
    poor_count = sum(1 for _, flag, sev in all_issues if flag and sev == "poor")

    return {
        "active_user_decline_issue": decline_issue,
        "active_user_decline_severity": decline_severity,
        "active_user_decline_reason": decline_reason,
        "low_repeat_engagement_issue": low_repeat_issue,
        "low_repeat_engagement_severity": low_repeat_severity,
        "low_repeat_engagement_reason": low_repeat_reason,
        "elevated_lapse_issue": lapse_issue,
        "elevated_lapse_severity": lapse_severity,
        "elevated_lapse_reason": lapse_reason,
        "concentrated_dependency_issue": conc_issue,
        "concentrated_dependency_severity": conc_severity,
        "concentrated_dependency_reason": conc_reason,
        "increasing_dependency_issue": incr_dep_issue,
        "increasing_dependency_severity": incr_dep_severity,
        "increasing_dependency_reason": incr_dep_reason,
        "declining_frequency_issue": freq_decline_issue,
        "declining_frequency_severity": freq_decline_severity,
        "declining_frequency_reason": freq_decline_reason,
        "inactivity_issue": inactivity_issue,
        "inactivity_severity": inactivity_severity,
        "inactivity_reason": inactivity_reason,
        "privacy_limitation_issue": privacy_issue,
        "privacy_limitation_severity": privacy_severity,
        "privacy_limitation_reason": privacy_reason,
        "insufficient_history_issue": insuf_hist_issue,
        "insufficient_history_severity": insuf_hist_severity,
        "insufficient_history_reason": insuf_hist_reason,
        "user_data_quality_issue": dq_issue,
        "user_data_quality_severity": dq_severity,
        "user_data_quality_reason": dq_reason,
        "engagement_issue_count": issue_count,
        "engagement_warning_count": warning_count,
        "engagement_poor_count": poor_count,
    }


# ---------------------------------------------------------------------------
# Overall engagement status classifier
# ---------------------------------------------------------------------------

def classify_overall_engagement_status(
    row: pd.Series,
    issues: dict,
    cfg: EngagementStatusConfig,
) -> str:
    """
    Classify one of 15 overall_engagement_status values.
    Evaluated in priority order.
    """
    dq_status = _safe_str(_get(row, "user_data_quality_status")) or "good"
    history_sufficient_28d = _safe_bool(_get(row, "history_sufficient_28d"))
    comparison_sufficient = _safe_bool(_get(row, "comparison_history_sufficient_28d"))
    unique_users_28d = _safe_int(_get(row, "unique_users_28d"))
    unique_users_prev = _safe_int(_get(row, "unique_users_previous_28d"))
    active_user_direction = _safe_str(_get(row, "active_user_direction_28d"))
    returning_share = _safe_float(_get(row, "returning_user_share_28d"))

    poor_count = issues.get("engagement_poor_count", 0)

    # 1. no_valid_user_data
    if dq_status == "no_valid_user_data":
        return "no_valid_user_data"

    # 2. insufficient_evidence
    if not history_sufficient_28d:
        # Check if newly active (previous=0, recent>0)
        if (
            comparison_sufficient
            and unique_users_prev == 0
            and unique_users_28d is not None
            and unique_users_28d > 0
        ):
            return "newly_active"
        return "insufficient_evidence"

    # 3. inactive
    if issues.get("inactivity_issue"):
        return "inactive"

    # 4. newly_active
    if (
        comparison_sufficient
        and unique_users_prev == 0
        and unique_users_28d is not None
        and unique_users_28d > 0
    ):
        return "newly_active"

    # 5. declining_adoption
    decline_issue = issues.get("active_user_decline_issue", False)
    decline_severity = issues.get("active_user_decline_severity", "none")
    lapse_issue = issues.get("elevated_lapse_issue", False)

    if decline_issue and (
        decline_severity == "poor"
        or (decline_issue and lapse_issue)
    ):
        return "declining_adoption"

    # 6. elevated_lapse (without decline)
    if lapse_issue and not decline_issue:
        return "elevated_lapse"

    # 7. low_repeat_usage
    low_repeat = issues.get("low_repeat_engagement_issue", False)
    if low_repeat and not decline_issue and not lapse_issue:
        return "low_repeat_usage"

    # 8. concentrated_dependency
    conc_issue = issues.get("concentrated_dependency_issue", False)
    if conc_issue and not decline_issue:
        return "concentrated_dependency"

    # 9. growing_adoption
    if active_user_direction == "growing" and poor_count == 0:
        return "growing_adoption"

    # 10. healthy_broad_adoption
    n_users = unique_users_28d if unique_users_28d is not None else 0
    if (
        n_users > cfg.NICHE_ACTIVE_USER_MAX
        and poor_count == 0
        and returning_share is not None
        and returning_share >= cfg.LOW_REPEAT_SHARE_THRESHOLD
    ):
        return "healthy_broad_adoption"

    # 11. healthy_niche_adoption
    if (
        n_users > 0
        and n_users <= cfg.NICHE_ACTIVE_USER_MAX
        and returning_share is not None
        and returning_share >= cfg.NICHE_RETURNING_SHARE_MIN
        and not lapse_issue
        and not (decline_issue and decline_severity == "poor")
    ):
        return "healthy_niche_adoption"

    # 12. stable_engagement — no poor issues and sufficient evidence
    if poor_count == 0 and history_sufficient_28d and n_users > 0:
        warning_count = issues.get("engagement_warning_count", 0)
        if warning_count == 0:
            return "stable_engagement"

    # 13. privacy_limited
    privacy_issue = issues.get("privacy_limitation_issue", False)
    if privacy_issue and not decline_issue and not lapse_issue and not low_repeat and not conc_issue:
        return "privacy_limited"

    # 14. growing_adoption (without poor issues but with warnings)
    if active_user_direction == "growing":
        return "growing_adoption"

    # 15. mixed_signals (fallback)
    return "mixed_signals"


# ---------------------------------------------------------------------------
# Primary issue determiner
# ---------------------------------------------------------------------------

def determine_primary_engagement_issue(issues: dict) -> str:
    """
    Return the single highest-priority engagement issue label.
    """
    if issues.get("user_data_quality_issue") and issues.get("user_data_quality_severity") == "poor":
        return "no_valid_user_data"
    if issues.get("inactivity_issue"):
        return "inactivity"
    if issues.get("active_user_decline_issue") and issues.get("active_user_decline_severity") in ("warning", "poor"):
        return "active_user_decline"
    if issues.get("elevated_lapse_issue"):
        return "elevated_lapse"
    if issues.get("low_repeat_engagement_issue"):
        return "low_repeat_engagement"
    if issues.get("concentrated_dependency_issue"):
        return "concentrated_dependency"
    if issues.get("declining_frequency_issue"):
        return "declining_frequency"
    if issues.get("privacy_limitation_issue"):
        return "privacy_limitation"
    if issues.get("insufficient_history_issue"):
        return "insufficient_history"
    # Check newly_active signal (stored via issues or row)
    return "none"


# ---------------------------------------------------------------------------
# Recommended action
# ---------------------------------------------------------------------------

def determine_recommended_action(
    primary_issue: str,
    overall_status: str,
    issues: dict,
    cfg: EngagementStatusConfig,
) -> tuple:
    """
    Returns (action, priority, review_required).
    """
    poor_count = issues.get("engagement_poor_count", 0)
    warning_count = issues.get("engagement_warning_count", 0)

    # Priority override from issue counts
    def _priority_from_counts(base_priority: str) -> str:
        if poor_count >= cfg.MIN_POOR_ISSUES_FOR_HIGH_PRIORITY:
            return "high"
        if warning_count >= cfg.MIN_WARNING_ISSUES_FOR_MEDIUM_PRIORITY:
            return "medium"
        return base_priority

    action_map = {
        "no_valid_user_data": ("investigate_data_quality", "high", True),
        "inactivity": ("validate_report_audience", "medium", True),
        "active_user_decline": (
            "investigate_user_decline",
            "high" if issues.get("active_user_decline_severity") == "poor" else "medium",
            issues.get("active_user_decline_severity") == "poor",
        ),
        "elevated_lapse": ("investigate_user_lapse", "medium", False),
        "low_repeat_engagement": ("improve_repeat_engagement", "low", False),
        "concentrated_dependency": ("review_concentrated_dependency", "low", False),
        "declining_frequency": ("continue_monitoring", "low", False),
        "privacy_limitation": ("continue_monitoring", "low", False),
        "insufficient_history": ("insufficient_evidence", "insufficient_evidence", False),
        "none": ("continue_monitoring", "low", False),
    }

    if primary_issue in action_map:
        action, priority, review_required = action_map[primary_issue]
    elif primary_issue == "newly_active" or overall_status == "newly_active":
        action, priority, review_required = "monitor_new_adoption", "low", False
    else:
        action, priority, review_required = "continue_monitoring", "low", False

    # Apply priority override based on issue severity counts (except special cases)
    # Override: newly_active status always gets monitor action
    if overall_status == "newly_active" and action not in ("investigate_data_quality", "validate_report_audience"):
        action, priority, review_required = "monitor_new_adoption", "low", False
        return (action, priority, review_required)

    if priority not in ("high", "insufficient_evidence"):
        priority = _priority_from_counts(priority)

    return (action, priority, review_required)


# ---------------------------------------------------------------------------
# Breadth status classifier
# ---------------------------------------------------------------------------

def classify_breadth_status(
    row: pd.Series,
    issues: dict,
    cfg: EngagementStatusConfig,
) -> str:
    """
    Classify the breadth of active users.
    """
    dq_status = _safe_str(_get(row, "user_data_quality_status")) or "good"
    if dq_status == "no_valid_user_data":
        return "no_valid_user_data"

    history_sufficient_28d = _safe_bool(_get(row, "history_sufficient_28d"))
    if not history_sufficient_28d:
        return "insufficient_history"

    unique_users_28d = _safe_int(_get(row, "unique_users_28d"))
    if unique_users_28d is None:
        return "insufficient_history"
    if unique_users_28d == 0:
        return "no_recent_activity"

    active_user_direction = _safe_str(_get(row, "active_user_direction_28d"))
    if active_user_direction == "growing":
        return "growing_breadth"
    if active_user_direction == "declining" and issues.get("active_user_decline_issue"):
        return "declining_breadth"

    if unique_users_28d > cfg.NICHE_ACTIVE_USER_MAX:
        return "broad_adoption"
    return "niche_adoption"


# ---------------------------------------------------------------------------
# Repeat engagement status classifier
# ---------------------------------------------------------------------------

def classify_repeat_engagement_status(
    row: pd.Series,
    maturity_status: str,
    issues: dict,
    cfg: EngagementStatusConfig,
) -> str:
    """
    Classify repeat engagement pattern for the report.
    """
    dq_status = _safe_str(_get(row, "user_data_quality_status")) or "good"
    if dq_status == "no_valid_user_data":
        return "no_valid_user_data"

    history_sufficient_28d = _safe_bool(_get(row, "history_sufficient_28d"))
    if not history_sufficient_28d:
        return "insufficient_history"

    if maturity_status == "immature":
        return "too_early_to_assess"

    unique_users_28d = _safe_int(_get(row, "unique_users_28d"))
    if unique_users_28d is None or unique_users_28d == 0:
        return "no_recent_activity"

    privacy_suppressed = _safe_bool(_get(row, "activity_privacy_suppressed"))
    if privacy_suppressed:
        return "privacy_suppressed"

    returning_share = _safe_float(_get(row, "returning_user_share_28d"))
    if returning_share is None:
        return "insufficient_data"

    if returning_share >= cfg.MODERATE_REPEAT_SHARE_THRESHOLD:
        return "strong_repeat_engagement"
    if returning_share >= cfg.LOW_REPEAT_SHARE_THRESHOLD:
        return "moderate_repeat_engagement"
    return "low_repeat_engagement"


# ---------------------------------------------------------------------------
# Reason builder
# ---------------------------------------------------------------------------

def build_engagement_reasons(
    row: pd.Series,
    issues: dict,
    overall_status: str,
    action: str,
    cfg: EngagementStatusConfig,
) -> str:
    """
    Build a pipe-separated deterministic reason string.
    10-item ordered structure; items with no meaningful statement are skipped.
    """
    reasons = []

    # 1. Data quality
    dq_issue = issues.get("user_data_quality_issue", False)
    dq_sev = issues.get("user_data_quality_severity", "none")
    dq_status = _safe_str(_get(row, "user_data_quality_status")) or "good"
    if dq_issue:
        reasons.append(f"data_quality:{issues.get('user_data_quality_reason', dq_status)}")

    # 2. Evidence and source coverage
    hist_suf = _safe_bool(_get(row, "history_sufficient_28d"))
    cmp_suf = _safe_bool(_get(row, "comparison_history_sufficient_28d"))
    avail_days = _safe_int(_get(row, "available_calendar_history_days"))
    if hist_suf and cmp_suf:
        evidence_stmt = "Full 28-day comparison window available."
    elif hist_suf:
        evidence_stmt = "Recent 28-day window available; no comparison window."
    else:
        evidence_stmt = "Insufficient history for 28-day analysis."
    if avail_days is not None:
        evidence_stmt += f" ({avail_days} calendar days of history available.)"
    reasons.append(f"evidence:{evidence_stmt}")

    # 3. Privacy limitations
    if issues.get("privacy_limitation_issue"):
        reasons.append(f"privacy:{issues.get('privacy_limitation_reason', 'Metrics suppressed.')}")

    # 4. Active-user breadth
    unique_users = _safe_int(_get(row, "unique_users_28d"))
    direction = _safe_str(_get(row, "active_user_direction_28d"))
    if unique_users is not None:
        breadth_stmt = f"{unique_users} active users in last 28 days."
        if direction and direction not in ("insufficient_history", "no_valid_user_data"):
            breadth_stmt += f" Trend: {direction}."
        if issues.get("active_user_decline_issue"):
            breadth_stmt += f" {issues.get('active_user_decline_reason', '')}"
        reasons.append(f"breadth:{breadth_stmt}")

    # 5. Returning-user behaviour
    ret_share = _safe_float(_get(row, "returning_user_share_28d"))
    repeat_status = _safe_str(_get(row, "repeat_usage_status"))
    if ret_share is not None:
        ret_stmt = f"Returning user share: {ret_share:.0%}."
        if issues.get("low_repeat_engagement_issue"):
            ret_stmt += f" {issues.get('low_repeat_engagement_reason', '')}"
        reasons.append(f"repeat_engagement:{ret_stmt}")
    elif repeat_status and repeat_status not in ("insufficient_history", "no_valid_user_data"):
        reasons.append(f"repeat_engagement:{repeat_status}")

    # 6. Cohort transitions
    lapse_rate = _safe_float(_get(row, "lapse_rate_28d"))
    retained_rate = _safe_float(_get(row, "retained_user_rate_28d"))
    if lapse_rate is not None:
        cohort_stmt = f"Lapse rate: {lapse_rate:.0%}."
        if retained_rate is not None:
            cohort_stmt += f" Retained rate: {retained_rate:.0%}."
        if issues.get("elevated_lapse_issue"):
            cohort_stmt += f" {issues.get('elevated_lapse_reason', '')}"
        reasons.append(f"cohort:{cohort_stmt}")

    # 7. Frequency and intensity
    freq_dir = _safe_str(_get(row, "frequency_direction"))
    median_active_days = _safe_float(_get(row, "median_user_active_days_28d"))
    if freq_dir and freq_dir not in ("insufficient_history", "no_valid_user_data", "privacy_suppressed"):
        freq_stmt = f"Frequency direction: {freq_dir}."
        if median_active_days is not None:
            freq_stmt += f" Median active days per user: {median_active_days:.1f}."
        if issues.get("declining_frequency_issue"):
            freq_stmt += f" {issues.get('declining_frequency_reason', '')}"
        reasons.append(f"frequency:{freq_stmt}")

    # 8. Concentration
    hhi = _safe_float(_get(row, "user_view_hhi_28d"))
    conc_dir = _safe_str(_get(row, "concentration_direction"))
    if hhi is not None:
        conc_stmt = f"view_hhi:{hhi:.3f}."
        if conc_dir:
            conc_stmt += f" Concentration direction: {conc_dir}."
        if issues.get("concentrated_dependency_issue"):
            conc_stmt += f" {issues.get('concentrated_dependency_reason', '')}"
        if issues.get("increasing_dependency_issue"):
            conc_stmt += f" {issues.get('increasing_dependency_reason', '')}"
        reasons.append(f"concentration:{conc_stmt}")

    # 9. Overall classification
    reasons.append(f"status:{overall_status}")

    # 10. Recommended action
    reasons.append(f"action:{action}")

    return "|".join(reasons)
