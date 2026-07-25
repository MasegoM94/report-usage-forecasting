"""
Privacy-safe report engagement cohorts.

Sprint 6 — User Analytics
Scope: newly adopted, retained, reactivated, lapsed user cohort counts, rates,
       privacy suppression, output persistence.

NOT in scope: frequency metrics, concentration/HHI, overall engagement status,
              GenAI, Streamlit.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from src.analytics.privacy_policy import (
    PROHIBITED_OUTPUT_COLUMNS,
    detect_email_like_values,
    validate_no_direct_identifiers,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CohortConfig:
    MIN_USERS_FOR_COHORT_BREAKDOWN: int = 5
    STRONG_RETENTION_RATE_THRESHOLD: float = 0.70
    LAPSE_WARNING_RATE_THRESHOLD: float = 0.40
    LAPSE_POOR_RATE_THRESHOLD: float = 0.70
    DOMINANT_COHORT_SHARE_THRESHOLD: float = 0.50
    REQUIRE_PREVIOUS_HISTORY_FOR_REACTIVATION: bool = True
    ALLOW_PARTIAL_COHORT_CLASSIFICATION: bool = True


# ---------------------------------------------------------------------------
# Schema constant
# ---------------------------------------------------------------------------

REPORT_ENGAGEMENT_COHORTS_COLS = [
    # Identity and evidence
    "analytics_run_id", "generated_at", "analytics_as_of_date",
    "report_id", "report_name", "report_activation_date",
    "comparison_history_sufficient_28d", "cohort_history_sufficient",
    "pre_previous_history_available", "has_any_valid_user_activity",
    "user_data_quality_status", "excluded_user_event_share",
    # Window population counts
    "recent_users_28d", "previous_users_28d", "pre_previous_users_lifetime",
    "recent_only_users_28d", "previous_only_users_28d", "users_active_both_windows",
    # Cohort counts
    "newly_adopted_users_28d", "retained_users_28d",
    "reactivated_users_28d", "lapsed_users_28d",
    "unclassified_recent_users_28d",
    # Cohort rates
    "newly_adopted_user_share_28d", "retained_user_rate_28d",
    "lapse_rate_28d", "reactivated_user_share_28d",
    "unclassified_recent_user_share_28d",
    # Supporting fields
    "net_user_movement_28d", "cohort_balance",
    "recent_user_retention_share", "previous_user_continuation_rate",
    # Privacy suppression
    "cohort_privacy_suppressed", "cohort_privacy_suppression_reason",
    "suppressed_cohort_fields",
    # Status
    "cohort_status", "cohort_evidence_status", "cohort_reasons",
]

# Fields that are suppressed when privacy threshold is triggered
_SUPPRESSIBLE_COHORT_COUNT_FIELDS = [
    "newly_adopted_users_28d",
    "retained_users_28d",
    "reactivated_users_28d",
    "lapsed_users_28d",
    "unclassified_recent_users_28d",
]
_SUPPRESSIBLE_COHORT_RATE_FIELDS = [
    "newly_adopted_user_share_28d",
    "retained_user_rate_28d",
    "lapse_rate_28d",
    "reactivated_user_share_28d",
    "unclassified_recent_user_share_28d",
    "cohort_balance",
    "recent_user_retention_share",
    "previous_user_continuation_rate",
    "net_user_movement_28d",
]

_VALID_COHORT_STATUSES = {
    "no_valid_user_data",
    "insufficient_history",
    "privacy_suppressed",
    "no_recent_or_previous_activity",
    "complete_lapse",
    "newly_active_no_prior_population",
    "partial_history",
    "strong_retention",
    "elevated_lapse",
    "growth_driven_by_new_adoption",
    "growth_driven_by_reactivation",
    "mixed_transition",
}

_VALID_EVIDENCE_STATUSES = {
    "sufficient",
    "partial",
    "insufficient_history",
    "no_valid_user_data",
    "calculation_failed",
}


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _safe_date(val) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, date):
        return val
    try:
        return pd.to_datetime(str(val)).date()
    except Exception:
        return None


def _to_float_or_none(val) -> Optional[float]:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return float(val)


# ---------------------------------------------------------------------------
# 1. Build window user sets (in-memory only, never returned in output)
# ---------------------------------------------------------------------------

def build_report_user_window_sets(
    mart_df: pd.DataFrame,
    report_id: str,
    window_28d_start: date,
    window_28d_end: date,
    previous_28d_start: date,
    previous_28d_end: date,
    pre_previous_28d_end: date,
) -> dict:
    """
    Build frozensets of user_key values for each observation window.

    Returns a dict with:
        recent_users, previous_users, pre_previous_users (frozensets of user_key)
        first_use_by_user (dict: user_key -> first_report_use_date as date or None)

    The frozensets are NEVER persisted to outputs — they stay in memory only.
    """
    # Filter to this report
    report_df = mart_df[mart_df["report_id"] == report_id].copy()

    # Parse usage_date to date objects
    report_df["_usage_date"] = pd.to_datetime(report_df["usage_date"], errors="coerce").dt.date

    # Parse first_report_use_date
    if "first_report_use_date" in report_df.columns:
        report_df["_first_use"] = pd.to_datetime(
            report_df["first_report_use_date"], errors="coerce"
        ).dt.date
    else:
        # Derive from min usage_date per user
        report_df["_first_use"] = None

    # Recent users: window_28d_start <= usage_date <= window_28d_end
    recent_mask = (
        report_df["_usage_date"].notna()
        & (report_df["_usage_date"] >= window_28d_start)
        & (report_df["_usage_date"] <= window_28d_end)
    )
    recent_users = frozenset(report_df.loc[recent_mask, "user_key"].dropna().unique())

    # Previous users: previous_28d_start <= usage_date <= previous_28d_end
    prev_mask = (
        report_df["_usage_date"].notna()
        & (report_df["_usage_date"] >= previous_28d_start)
        & (report_df["_usage_date"] <= previous_28d_end)
    )
    previous_users = frozenset(report_df.loc[prev_mask, "user_key"].dropna().unique())

    # Pre-previous users: usage_date < previous_28d_start AND <= pre_previous_28d_end
    pre_prev_mask = (
        report_df["_usage_date"].notna()
        & (report_df["_usage_date"] < previous_28d_start)
        & (report_df["_usage_date"] <= pre_previous_28d_end)
    )
    pre_previous_users = frozenset(report_df.loc[pre_prev_mask, "user_key"].dropna().unique())

    # first_use_by_user: per (report_id, user_key)
    first_use_by_user: dict = {}
    if "first_report_use_date" in report_df.columns:
        # Use the pre-computed column; take first non-null per user_key.
        # If the column exists but all values for a user are null, leave first_use as None
        # (do NOT fall back to min usage_date — null means unknown, not absent).
        for uk, grp in report_df.groupby("user_key"):
            raw = grp["_first_use"].dropna()
            if not raw.empty:
                first_use_by_user[uk] = raw.iloc[0]
            else:
                first_use_by_user[uk] = None
    else:
        # Column absent: derive from min usage_date as documented fallback
        for uk, grp in report_df.groupby("user_key"):
            dates = grp["_usage_date"].dropna()
            if not dates.empty:
                first_use_by_user[uk] = dates.min()
            else:
                first_use_by_user[uk] = None

    return {
        "recent_users": recent_users,
        "previous_users": previous_users,
        "pre_previous_users": pre_previous_users,
        "first_use_by_user": first_use_by_user,
    }


# ---------------------------------------------------------------------------
# 2. Classify cohorts
# ---------------------------------------------------------------------------

def classify_report_user_cohorts(
    window_sets: dict,
    window_28d_start: date,
    pre_previous_28d_end: date,
    cfg: CohortConfig,
) -> dict:
    """
    Classify users into mutually exclusive cohorts.

    Returns dict with frozensets:
        newly_adopted, retained, reactivated, unclassified_recent, lapsed
    """
    recent_users: frozenset = window_sets["recent_users"]
    previous_users: frozenset = window_sets["previous_users"]
    pre_previous_users: frozenset = window_sets["pre_previous_users"]
    first_use_by_user: dict = window_sets["first_use_by_user"]

    # Retained: in both windows
    retained = recent_users & previous_users

    # Newly adopted: in recent, not in previous, first_use >= window_28d_start
    candidate_new = recent_users - previous_users
    newly_adopted = frozenset(
        u for u in candidate_new
        if first_use_by_user.get(u) is not None
        and first_use_by_user[u] >= window_28d_start
    )

    # Reactivated: in recent, not in previous, not newly_adopted, in pre_previous
    if cfg.REQUIRE_PREVIOUS_HISTORY_FOR_REACTIVATION:
        candidate_remaining = candidate_new - newly_adopted
        reactivated = candidate_remaining & pre_previous_users
    else:
        reactivated = frozenset()

    # Unclassified recent: in recent, not previous, not newly_adopted, not reactivated
    unclassified_recent = (recent_users - previous_users) - newly_adopted - reactivated

    # Lapsed: in previous, not in recent
    lapsed = previous_users - recent_users

    # Sanity assertions (defensive)
    classified_recent = newly_adopted | retained | reactivated | unclassified_recent
    assert classified_recent == recent_users, (
        f"Cohort classification incomplete: {len(recent_users)} recent users but "
        f"{len(classified_recent)} classified"
    )
    assert (retained | lapsed) == previous_users, (
        f"Previous user reconciliation failed: {len(previous_users)} previous but "
        f"{len(retained | lapsed)} accounted for"
    )

    return {
        "newly_adopted": newly_adopted,
        "retained": retained,
        "reactivated": reactivated,
        "unclassified_recent": unclassified_recent,
        "lapsed": lapsed,
    }


# ---------------------------------------------------------------------------
# 3. Aggregate counts
# ---------------------------------------------------------------------------

def aggregate_report_cohort_metrics(cohorts: dict, window_sets: dict) -> dict:
    """Convert frozensets to counts. Returns population and cohort counts."""
    recent_users: frozenset = window_sets["recent_users"]
    previous_users: frozenset = window_sets["previous_users"]
    pre_previous_users: frozenset = window_sets["pre_previous_users"]

    newly_adopted: frozenset = cohorts["newly_adopted"]
    retained: frozenset = cohorts["retained"]
    reactivated: frozenset = cohorts["reactivated"]
    unclassified_recent: frozenset = cohorts["unclassified_recent"]
    lapsed: frozenset = cohorts["lapsed"]

    recent_count = len(recent_users)
    previous_count = len(previous_users)

    return {
        "recent_users_28d": recent_count,
        "previous_users_28d": previous_count,
        "pre_previous_users_lifetime": len(pre_previous_users),
        "users_active_both_windows": len(retained),
        "recent_only_users_28d": len(recent_users - previous_users),
        "previous_only_users_28d": len(previous_users - recent_users),
        "newly_adopted_users_28d": len(newly_adopted),
        "retained_users_28d": len(retained),
        "reactivated_users_28d": len(reactivated),
        "lapsed_users_28d": len(lapsed),
        "unclassified_recent_users_28d": len(unclassified_recent),
        "net_user_movement_28d": recent_count - previous_count,
    }


# ---------------------------------------------------------------------------
# 4. Calculate rates
# ---------------------------------------------------------------------------

def calculate_cohort_rates(counts: dict) -> dict:
    """Compute cohort rates and shares. Uses pd.NA when denominator is 0."""
    recent = counts["recent_users_28d"]
    previous = counts["previous_users_28d"]
    newly_adopted = counts["newly_adopted_users_28d"]
    retained = counts["retained_users_28d"]
    reactivated = counts["reactivated_users_28d"]
    lapsed = counts["lapsed_users_28d"]
    unclassified = counts["unclassified_recent_users_28d"]

    def safe_rate(numerator: int, denominator: int):
        if denominator == 0:
            return None
        return numerator / denominator

    newly_adopted_share = safe_rate(newly_adopted, recent)
    retained_rate = safe_rate(retained, previous)
    lapse_rate = safe_rate(lapsed, previous)
    reactivated_share = safe_rate(reactivated, recent)
    unclassified_share = safe_rate(unclassified, recent)
    recent_user_retention_share = safe_rate(retained, recent)
    # previous_user_continuation_rate is the same concept as retained_rate
    previous_user_continuation_rate = retained_rate

    # Cohort balance: (newly_adopted + reactivated) - lapsed
    cohort_balance = (newly_adopted + reactivated) - lapsed

    # Validation: retained_rate + lapse_rate must sum to 1.0 when previous > 0
    if previous > 0 and retained_rate is not None and lapse_rate is not None:
        total = retained_rate + lapse_rate
        if abs(total - 1.0) > 1e-9:
            raise ValueError(
                f"retained_rate ({retained_rate:.6f}) + lapse_rate ({lapse_rate:.6f}) "
                f"= {total:.6f}, expected 1.0 (tolerance 1e-9). "
                f"retained={retained}, lapsed={lapsed}, previous={previous}"
            )

    return {
        "newly_adopted_user_share_28d": newly_adopted_share,
        "retained_user_rate_28d": retained_rate,
        "lapse_rate_28d": lapse_rate,
        "reactivated_user_share_28d": reactivated_share,
        "unclassified_recent_user_share_28d": unclassified_share,
        "cohort_balance": cohort_balance,
        "recent_user_retention_share": recent_user_retention_share,
        "previous_user_continuation_rate": previous_user_continuation_rate,
    }


# ---------------------------------------------------------------------------
# 5. Privacy suppression
# ---------------------------------------------------------------------------

def apply_cohort_privacy_suppression(
    counts: dict,
    rates: dict,
    cfg: CohortConfig,
) -> dict:
    """
    Suppress cohort breakdown fields when user counts fall below threshold.

    Suppressed when:
        recent_users_28d < MIN_USERS_FOR_COHORT_BREAKDOWN
        OR (previous_users_28d > 0 AND previous_users_28d < MIN_USERS_FOR_COHORT_BREAKDOWN)

    Net movement and total population counts are NOT suppressed.
    Returns merged dict including suppression metadata.
    """
    threshold = cfg.MIN_USERS_FOR_COHORT_BREAKDOWN
    recent = counts["recent_users_28d"]
    previous = counts["previous_users_28d"]

    suppress = False
    suppression_reason: Optional[str] = None

    if recent < threshold:
        suppress = True
        suppression_reason = f"recent_users_{recent}_below_minimum_{threshold}"
    elif previous > 0 and previous < threshold:
        suppress = True
        suppression_reason = f"previous_users_{previous}_below_minimum_{threshold}"

    merged = {**counts, **rates}

    if suppress:
        suppressed_fields = sorted(
            _SUPPRESSIBLE_COHORT_COUNT_FIELDS + _SUPPRESSIBLE_COHORT_RATE_FIELDS
        )
        for field in suppressed_fields:
            merged[field] = None
        merged["cohort_privacy_suppressed"] = True
        merged["cohort_privacy_suppression_reason"] = suppression_reason
        merged["suppressed_cohort_fields"] = ",".join(suppressed_fields)
    else:
        merged["cohort_privacy_suppressed"] = False
        merged["cohort_privacy_suppression_reason"] = None
        merged["suppressed_cohort_fields"] = None

    return merged


# ---------------------------------------------------------------------------
# 6. Status classification
# ---------------------------------------------------------------------------

def classify_cohort_status(
    counts: dict,
    rates: dict,
    is_suppressed: bool,
    comparison_sufficient: bool,
    has_valid_data: bool,
    cfg: CohortConfig,
    pre_previous_history_available: bool = True,
) -> tuple:
    """
    Determine cohort_status, cohort_evidence_status, and reasons list.

    Returns (cohort_status, cohort_evidence_status, reasons_list).
    """
    recent = counts.get("recent_users_28d", 0) or 0
    previous = counts.get("previous_users_28d", 0) or 0
    newly_adopted = counts.get("newly_adopted_users_28d", 0) or 0
    retained = counts.get("retained_users_28d", 0) or 0
    reactivated = counts.get("reactivated_users_28d", 0) or 0
    lapsed = counts.get("lapsed_users_28d", 0) or 0
    unclassified = counts.get("unclassified_recent_users_28d", 0) or 0
    net_movement = counts.get("net_user_movement_28d", 0) or 0

    retained_rate = rates.get("retained_user_rate_28d")
    lapse_rate = rates.get("lapse_rate_28d")

    # --- Status determination (checked in order) ---
    if not has_valid_data:
        cohort_status = "no_valid_user_data"
    elif not comparison_sufficient:
        cohort_status = "insufficient_history"
    elif is_suppressed:
        cohort_status = "privacy_suppressed"
    elif recent == 0 and previous == 0:
        cohort_status = "no_recent_or_previous_activity"
    elif recent == 0 and previous > 0 and lapsed == previous:
        cohort_status = "complete_lapse"
    elif previous == 0 and recent > 0:
        cohort_status = "newly_active_no_prior_population"
    elif (
        cfg.ALLOW_PARTIAL_COHORT_CLASSIFICATION
        and unclassified > 0
        and recent > 0
        and unclassified / recent > 0.5
    ):
        cohort_status = "partial_history"
    elif (
        retained_rate is not None
        and lapse_rate is not None
        and retained_rate >= cfg.STRONG_RETENTION_RATE_THRESHOLD
        and lapse_rate < cfg.LAPSE_WARNING_RATE_THRESHOLD
    ):
        cohort_status = "strong_retention"
    elif lapse_rate is not None and lapse_rate >= cfg.LAPSE_WARNING_RATE_THRESHOLD:
        cohort_status = "elevated_lapse"
    elif (
        net_movement > 0
        and newly_adopted > 0
        and newly_adopted / max(1, recent - previous) > cfg.DOMINANT_COHORT_SHARE_THRESHOLD
    ):
        cohort_status = "growth_driven_by_new_adoption"
    elif (
        net_movement > 0
        and reactivated > 0
        and reactivated / max(1, recent - previous) > cfg.DOMINANT_COHORT_SHARE_THRESHOLD
    ):
        cohort_status = "growth_driven_by_reactivation"
    else:
        cohort_status = "mixed_transition"

    # --- Evidence status ---
    if not has_valid_data:
        cohort_evidence_status = "no_valid_user_data"
    elif not comparison_sufficient:
        cohort_evidence_status = "insufficient_history"
    elif not pre_previous_history_available and unclassified > 0:
        cohort_evidence_status = "partial"
    else:
        cohort_evidence_status = "sufficient"

    # --- Build deterministic reasons list ---
    reasons: list = []

    # 1. Evidence limitation
    if not has_valid_data:
        reasons.append("No valid user data available for this report.")
    elif not comparison_sufficient:
        reasons.append("Comparison window history is insufficient for cohort analysis.")
    elif not pre_previous_history_available:
        reasons.append("Pre-previous window history unavailable; reactivation classification is limited.")

    # 2. Privacy suppression
    if is_suppressed:
        reasons.append("Cohort breakdown suppressed: user counts below privacy threshold.")

    # 3. Population sizes
    reasons.append(f"Recent 28d users: {recent}. Previous 28d users: {previous}.")

    # 4-9. Cohort counts/rates (if known and not suppressed)
    if not is_suppressed and comparison_sufficient and has_valid_data:
        reasons.append(f"Newly adopted: {newly_adopted}.")
        if retained_rate is not None:
            reasons.append(f"Retained: {retained} ({retained_rate:.1%} retention rate).")
        else:
            reasons.append(f"Retained: {retained}.")
        reasons.append(f"Reactivated: {reactivated}.")
        if lapse_rate is not None:
            reasons.append(f"Lapsed: {lapsed} ({lapse_rate:.1%} lapse rate).")
        else:
            reasons.append(f"Lapsed: {lapsed}.")
        if unclassified > 0:
            reasons.append(f"Unclassified recent users: {unclassified}.")

    # 9. Net movement
    reasons.append(f"Net user movement: {net_movement:+d}.")

    # 10. Status conclusion
    reasons.append(f"Cohort status: {cohort_status}.")

    return (cohort_status, cohort_evidence_status, reasons)


# ---------------------------------------------------------------------------
# 7. Main builder
# ---------------------------------------------------------------------------

def build_report_engagement_cohorts(
    sufficiency_df: pd.DataFrame,
    mart_df: pd.DataFrame,
    quality_df: pd.DataFrame,
    boundaries_df: pd.DataFrame,
    cfg: CohortConfig,
    analytics_run_id: str,
) -> pd.DataFrame:
    """
    Build the report engagement cohorts table — one row per report.

    Uses sufficiency_df as the spine. All reports in sufficiency_df appear
    in the output. Never exposes user-level data.
    """
    generated_at = datetime.utcnow().isoformat()

    # --- Parse window dates from boundaries_df ---
    if boundaries_df.empty:
        raise ValueError("boundaries_df is empty — run engagement windows step first")

    b = boundaries_df.iloc[0]
    analytics_as_of_date = str(b.get("analytics_as_of_date", ""))

    window_28d_start = _safe_date(b.get("window_28d_start"))
    window_28d_end = _safe_date(b.get("window_28d_end"))
    previous_28d_start = _safe_date(b.get("previous_28d_start"))
    previous_28d_end = _safe_date(b.get("previous_28d_end"))
    pre_previous_28d_end = _safe_date(b.get("pre_previous_28d_end"))

    # --- Pre-parse mart dates once ---
    if not mart_df.empty:
        mart_parsed = mart_df.copy()
        mart_parsed["usage_date"] = pd.to_datetime(
            mart_parsed["usage_date"], errors="coerce"
        ).dt.date
    else:
        mart_parsed = mart_df.copy()

    # --- Index quality by report_id ---
    quality_by_report: dict = {}
    if not quality_df.empty and "report_id" in quality_df.columns:
        for _, row in quality_df.iterrows():
            quality_by_report[str(row["report_id"])] = row

    rows = []
    for _, suf_row in sufficiency_df.sort_values("report_id").iterrows():
        rid = str(suf_row["report_id"])
        report_name = suf_row.get("report_name", "")
        report_activation_date = suf_row.get("report_activation_date", None)

        comparison_sufficient = bool(suf_row.get("comparison_history_sufficient_28d", False))
        has_any_activity = bool(suf_row.get("has_any_valid_user_activity", False))

        # Quality info
        q_row = quality_by_report.get(rid)
        if q_row is not None:
            dq_status = str(q_row.get("data_quality_status", "good"))
            excluded_share = _to_float_or_none(q_row.get("excluded_user_event_share", 0.0))
        else:
            dq_status = "good"
            excluded_share = 0.0

        has_valid_data = dq_status != "no_valid_user_data"

        # cohort_history_sufficient definition
        cohort_history_sufficient = (
            comparison_sufficient
            and has_any_activity
            and dq_status != "no_valid_user_data"
        )

        # Null template for insufficient/bad data
        null_counts: dict = {
            "recent_users_28d": None,
            "previous_users_28d": None,
            "pre_previous_users_lifetime": None,
            "users_active_both_windows": None,
            "recent_only_users_28d": None,
            "previous_only_users_28d": None,
            "newly_adopted_users_28d": None,
            "retained_users_28d": None,
            "reactivated_users_28d": None,
            "lapsed_users_28d": None,
            "unclassified_recent_users_28d": None,
            "net_user_movement_28d": None,
        }
        null_rates: dict = {
            "newly_adopted_user_share_28d": None,
            "retained_user_rate_28d": None,
            "lapse_rate_28d": None,
            "reactivated_user_share_28d": None,
            "unclassified_recent_user_share_28d": None,
            "cohort_balance": None,
            "recent_user_retention_share": None,
            "previous_user_continuation_rate": None,
        }

        pre_previous_history_available = False

        try:
            if (
                not comparison_sufficient
                or not has_valid_data
                or window_28d_start is None
                or previous_28d_start is None
            ):
                # Cannot compute cohorts
                counts = null_counts.copy()
                rates = null_rates.copy()

                merged = {**counts, **rates}
                merged["cohort_privacy_suppressed"] = False
                merged["cohort_privacy_suppression_reason"] = None
                merged["suppressed_cohort_fields"] = None
                is_suppressed = False

                cohort_status, cohort_evidence_status, cohort_reasons = classify_cohort_status(
                    counts={k: 0 for k in null_counts},
                    rates=null_rates,
                    is_suppressed=False,
                    comparison_sufficient=comparison_sufficient,
                    has_valid_data=has_valid_data,
                    cfg=cfg,
                    pre_previous_history_available=False,
                )
            else:
                # Build window sets
                window_sets = build_report_user_window_sets(
                    mart_df=mart_parsed,
                    report_id=rid,
                    window_28d_start=window_28d_start,
                    window_28d_end=window_28d_end,
                    previous_28d_start=previous_28d_start,
                    previous_28d_end=previous_28d_end,
                    pre_previous_28d_end=pre_previous_28d_end,
                )

                pre_previous_history_available = len(window_sets["pre_previous_users"]) > 0

                # Classify cohorts
                cohorts = classify_report_user_cohorts(
                    window_sets=window_sets,
                    window_28d_start=window_28d_start,
                    pre_previous_28d_end=pre_previous_28d_end,
                    cfg=cfg,
                )

                # Aggregate counts
                counts = aggregate_report_cohort_metrics(cohorts, window_sets)

                # Calculate rates
                rates = calculate_cohort_rates(counts)

                # Privacy suppression
                merged = apply_cohort_privacy_suppression(counts, rates, cfg)
                is_suppressed = merged["cohort_privacy_suppressed"]

                # Rebuild counts/rates from merged for status classification
                # (some may have been nulled by suppression)
                status_counts = {k: merged.get(k, 0) or 0 for k in null_counts}
                status_counts["net_user_movement_28d"] = counts.get("net_user_movement_28d", 0)
                status_rates = {k: merged.get(k) for k in null_rates}

                cohort_status, cohort_evidence_status, cohort_reasons = classify_cohort_status(
                    counts=status_counts,
                    rates=status_rates,
                    is_suppressed=is_suppressed,
                    comparison_sufficient=comparison_sufficient,
                    has_valid_data=has_valid_data,
                    cfg=cfg,
                    pre_previous_history_available=pre_previous_history_available,
                )

        except Exception as exc:
            merged = {**null_counts, **null_rates}
            merged["cohort_privacy_suppressed"] = False
            merged["cohort_privacy_suppression_reason"] = None
            merged["suppressed_cohort_fields"] = None
            is_suppressed = False
            cohort_status = "insufficient_history"
            cohort_evidence_status = "calculation_failed"
            cohort_reasons = [f"Calculation failed: {exc}"]

        row = {
            "analytics_run_id": analytics_run_id,
            "generated_at": generated_at,
            "analytics_as_of_date": analytics_as_of_date,
            "report_id": rid,
            "report_name": report_name,
            "report_activation_date": report_activation_date,
            "comparison_history_sufficient_28d": comparison_sufficient,
            "cohort_history_sufficient": cohort_history_sufficient,
            "pre_previous_history_available": pre_previous_history_available,
            "has_any_valid_user_activity": has_any_activity,
            "user_data_quality_status": dq_status,
            "excluded_user_event_share": excluded_share,
            # Window population counts
            "recent_users_28d": merged.get("recent_users_28d"),
            "previous_users_28d": merged.get("previous_users_28d"),
            "pre_previous_users_lifetime": merged.get("pre_previous_users_lifetime"),
            "recent_only_users_28d": merged.get("recent_only_users_28d"),
            "previous_only_users_28d": merged.get("previous_only_users_28d"),
            "users_active_both_windows": merged.get("users_active_both_windows"),
            # Cohort counts
            "newly_adopted_users_28d": merged.get("newly_adopted_users_28d"),
            "retained_users_28d": merged.get("retained_users_28d"),
            "reactivated_users_28d": merged.get("reactivated_users_28d"),
            "lapsed_users_28d": merged.get("lapsed_users_28d"),
            "unclassified_recent_users_28d": merged.get("unclassified_recent_users_28d"),
            # Cohort rates
            "newly_adopted_user_share_28d": merged.get("newly_adopted_user_share_28d"),
            "retained_user_rate_28d": merged.get("retained_user_rate_28d"),
            "lapse_rate_28d": merged.get("lapse_rate_28d"),
            "reactivated_user_share_28d": merged.get("reactivated_user_share_28d"),
            "unclassified_recent_user_share_28d": merged.get("unclassified_recent_user_share_28d"),
            # Supporting
            "net_user_movement_28d": merged.get("net_user_movement_28d"),
            "cohort_balance": merged.get("cohort_balance"),
            "recent_user_retention_share": merged.get("recent_user_retention_share"),
            "previous_user_continuation_rate": merged.get("previous_user_continuation_rate"),
            # Privacy suppression
            "cohort_privacy_suppressed": merged.get("cohort_privacy_suppressed"),
            "cohort_privacy_suppression_reason": merged.get("cohort_privacy_suppression_reason"),
            "suppressed_cohort_fields": merged.get("suppressed_cohort_fields"),
            # Status
            "cohort_status": cohort_status,
            "cohort_evidence_status": cohort_evidence_status,
            "cohort_reasons": " | ".join(cohort_reasons),
        }
        rows.append(row)

    result = pd.DataFrame(rows, columns=REPORT_ENGAGEMENT_COHORTS_COLS)
    return result


# ---------------------------------------------------------------------------
# 8. Validation
# ---------------------------------------------------------------------------

def validate_report_engagement_cohorts(
    df: pd.DataFrame,
    cfg: CohortConfig,
) -> None:
    """Validate the report engagement cohorts DataFrame. Raises ValueError on failure."""

    # 1. Required columns
    missing = [c for c in REPORT_ENGAGEMENT_COHORTS_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"validate_report_engagement_cohorts: missing columns: {missing}")

    if df.empty:
        return

    # 2. Unique grain
    dupes = df.duplicated(subset=["analytics_run_id", "report_id"])
    if dupes.any():
        raise ValueError(
            f"Duplicate (analytics_run_id, report_id) rows: "
            f"{df.loc[dupes, 'report_id'].tolist()}"
        )

    # 3. No prohibited identifiers
    validate_no_direct_identifiers(df, context="report_engagement_cohorts")

    # 4. Deterministic sort by report_id
    sorted_ids = df["report_id"].tolist()
    if sorted_ids != sorted(sorted_ids):
        raise ValueError("Output must be sorted by report_id ascending")

    # 5. Allowed status values
    bad_statuses = set(df["cohort_status"].dropna()) - _VALID_COHORT_STATUSES
    if bad_statuses:
        raise ValueError(f"Invalid cohort_status values: {bad_statuses}")

    bad_evidence = set(df["cohort_evidence_status"].dropna()) - _VALID_EVIDENCE_STATUSES
    if bad_evidence:
        raise ValueError(f"Invalid cohort_evidence_status values: {bad_evidence}")

    # 6. When not suppressed and comparison sufficient and recent>0 and previous>0
    sufficient_rows = df[
        (df["cohort_privacy_suppressed"] == False)
        & (df["comparison_history_sufficient_28d"] == True)
        & (df["recent_users_28d"].notna())
        & (df["previous_users_28d"].notna())
        & (df["recent_users_28d"] > 0)
        & (df["previous_users_28d"] > 0)
    ]

    if not sufficient_rows.empty:
        # users_active_both_windows == retained_users_28d
        bad = sufficient_rows[
            sufficient_rows["users_active_both_windows"] != sufficient_rows["retained_users_28d"]
        ]
        if not bad.empty:
            raise ValueError(
                f"users_active_both_windows != retained_users_28d for: {bad['report_id'].tolist()}"
            )

        # previous_only_users_28d == lapsed_users_28d
        bad = sufficient_rows[
            sufficient_rows["previous_only_users_28d"] != sufficient_rows["lapsed_users_28d"]
        ]
        if not bad.empty:
            raise ValueError(
                f"previous_only_users_28d != lapsed_users_28d for: {bad['report_id'].tolist()}"
            )

        # recent_only == newly_adopted + reactivated + unclassified
        recent_only_check = (
            sufficient_rows["newly_adopted_users_28d"].fillna(0)
            + sufficient_rows["reactivated_users_28d"].fillna(0)
            + sufficient_rows["unclassified_recent_users_28d"].fillna(0)
        )
        bad = sufficient_rows[sufficient_rows["recent_only_users_28d"] != recent_only_check]
        if not bad.empty:
            raise ValueError(
                f"recent_only_users_28d != newly_adopted + reactivated + unclassified for: "
                f"{bad['report_id'].tolist()}"
            )

        # recent_users == retained + newly_adopted + reactivated + unclassified
        recent_total_check = (
            sufficient_rows["retained_users_28d"].fillna(0)
            + sufficient_rows["newly_adopted_users_28d"].fillna(0)
            + sufficient_rows["reactivated_users_28d"].fillna(0)
            + sufficient_rows["unclassified_recent_users_28d"].fillna(0)
        )
        bad = sufficient_rows[sufficient_rows["recent_users_28d"] != recent_total_check]
        if not bad.empty:
            raise ValueError(
                f"recent_users_28d != cohort sum for: {bad['report_id'].tolist()}"
            )

        # previous_users == retained + lapsed
        previous_total_check = (
            sufficient_rows["retained_users_28d"].fillna(0)
            + sufficient_rows["lapsed_users_28d"].fillna(0)
        )
        bad = sufficient_rows[sufficient_rows["previous_users_28d"] != previous_total_check]
        if not bad.empty:
            raise ValueError(
                f"previous_users_28d != retained + lapsed for: {bad['report_id'].tolist()}"
            )

        # retained_rate + lapse_rate == 1.0 (±0.001)
        both_rates = sufficient_rows[
            sufficient_rows["retained_user_rate_28d"].notna()
            & sufficient_rows["lapse_rate_28d"].notna()
        ]
        if not both_rates.empty:
            rate_sum = both_rates["retained_user_rate_28d"] + both_rates["lapse_rate_28d"]
            bad = both_rates[(rate_sum - 1.0).abs() > 0.001]
            if not bad.empty:
                raise ValueError(
                    f"retained_rate + lapse_rate != 1.0 (±0.001) for: {bad['report_id'].tolist()}"
                )

    # 7. Rates in [0, 1]
    rate_cols = [
        "newly_adopted_user_share_28d", "retained_user_rate_28d",
        "lapse_rate_28d", "reactivated_user_share_28d",
        "unclassified_recent_user_share_28d", "recent_user_retention_share",
        "previous_user_continuation_rate",
    ]
    for col in rate_cols:
        if col in df.columns:
            numeric = pd.to_numeric(df[col], errors="coerce")
            bad = df[numeric.notna() & ((numeric < 0) | (numeric > 1))]
            if not bad.empty:
                raise ValueError(f"{col} out of [0,1] for: {bad['report_id'].tolist()}")

    # 8. Rates null when denominator is 0
    # recent-denominator rates null when recent=0
    recent_zero = df[df["recent_users_28d"].notna() & (df["recent_users_28d"] == 0)]
    for col in ["newly_adopted_user_share_28d", "reactivated_user_share_28d",
                "unclassified_recent_user_share_28d", "recent_user_retention_share"]:
        if col in df.columns:
            bad = recent_zero[recent_zero[col].notna()]
            if not bad.empty:
                raise ValueError(f"{col} must be null when recent_users_28d=0 for: {bad['report_id'].tolist()}")

    # previous-denominator rates null when previous=0
    prev_zero = df[df["previous_users_28d"].notna() & (df["previous_users_28d"] == 0)]
    for col in ["retained_user_rate_28d", "lapse_rate_28d", "previous_user_continuation_rate"]:
        if col in df.columns:
            bad = prev_zero[prev_zero[col].notna()]
            if not bad.empty:
                raise ValueError(f"{col} must be null when previous_users_28d=0 for: {bad['report_id'].tolist()}")

    # 9. Suppressed cohort fields must be null when suppressed=True
    suppressed_rows = df[df["cohort_privacy_suppressed"] == True]
    if not suppressed_rows.empty:
        all_suppressible = _SUPPRESSIBLE_COHORT_COUNT_FIELDS + _SUPPRESSIBLE_COHORT_RATE_FIELDS
        for col in all_suppressible:
            if col in df.columns:
                bad = suppressed_rows[suppressed_rows[col].notna()]
                if not bad.empty:
                    raise ValueError(
                        f"Suppressed rows have non-null {col} for: {bad['report_id'].tolist()}"
                    )

    # 10. complete_lapse requires recent=0 and previous>0
    complete_lapse = df[df["cohort_status"] == "complete_lapse"]
    if not complete_lapse.empty:
        bad = complete_lapse[
            (complete_lapse["recent_users_28d"].notna() & complete_lapse["recent_users_28d"] > 0)
        ]
        if not bad.empty:
            raise ValueError(
                f"complete_lapse status requires recent_users=0 but got >0 for: {bad['report_id'].tolist()}"
            )

    # 11. insufficient_history and no_valid_user_data: counts/rates should be null
    for status in ["insufficient_history", "no_valid_user_data"]:
        status_rows = df[df["cohort_status"] == status]
        if not status_rows.empty:
            for col in (_SUPPRESSIBLE_COHORT_COUNT_FIELDS + _SUPPRESSIBLE_COHORT_RATE_FIELDS):
                if col in df.columns:
                    bad = status_rows[status_rows[col].notna()]
                    if not bad.empty:
                        raise ValueError(
                            f"cohort_status={status} rows should have null {col} for: "
                            f"{bad['report_id'].tolist()}"
                        )

    # 12. No user lists in any column
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, set, tuple, frozenset))).any():
            raise ValueError(f"Column '{col}' contains user lists — privacy violation.")


# ---------------------------------------------------------------------------
# 9. Persistence
# ---------------------------------------------------------------------------

def persist_report_engagement_cohorts(
    df: pd.DataFrame,
    project_root: Path,
) -> Path:
    """Validate and write the cohorts DataFrame to outputs/analytics/."""
    df_sorted = df.sort_values("report_id").reset_index(drop=True)
    validate_report_engagement_cohorts(df_sorted, CohortConfig())
    output_dir = project_root / "outputs" / "analytics"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "report_engagement_cohorts.csv"
    df_sorted.to_csv(out_path, index=False)
    return out_path
