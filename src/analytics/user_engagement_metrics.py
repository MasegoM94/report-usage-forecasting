"""
Windowed active-user, returning-user, and one-time-user metrics.

Sprint 6 — User Analytics
Scope: unique_users, returning_users, one_time_users, repeat_view_users,
       per-user active-day distribution, active-user change / direction,
       privacy suppression, output persistence.

NOT in scope: cohorts, HHI, concentration, GenAI, Streamlit.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.analytics.privacy_policy import (
    PROHIBITED_OUTPUT_COLUMNS,
    validate_no_direct_identifiers,
)

# ---------------------------------------------------------------------------
# Schema constant
# ---------------------------------------------------------------------------

REPORT_USER_ACTIVITY_METRICS_COLS = [
    # Identity and evidence
    "analytics_run_id", "generated_at", "analytics_as_of_date",
    "report_id", "report_name",
    "history_sufficient_7d", "history_sufficient_28d",
    "history_sufficient_previous_28d", "comparison_history_sufficient_28d",
    "history_sufficient_90d", "history_sufficient_previous_90d",
    "comparison_history_sufficient_90d",
    "has_any_valid_user_activity", "user_data_quality_status",
    "excluded_user_event_share", "privacy_suppression_status",
    # Active-user breadth
    "unique_users_7d", "unique_users_28d", "unique_users_previous_28d",
    "unique_users_90d", "unique_users_previous_90d",
    "active_user_change_28d", "active_user_change_28d_pct",
    "active_user_change_90d", "active_user_change_90d_pct",
    "active_user_direction_28d", "active_user_direction_90d",
    # Returning and one-time users (28d)
    "returning_users_28d", "one_time_users_28d",
    "returning_user_share_28d", "one_time_user_share_28d",
    "returning_users_previous_28d", "one_time_users_previous_28d",
    "returning_user_share_previous_28d", "one_time_user_share_previous_28d",
    "returning_user_share_change_28d",
    # Returning and one-time users (90d)
    "returning_users_90d", "one_time_users_90d",
    "returning_user_share_90d", "one_time_user_share_90d",
    # Per-user active-day distribution (28d)
    "mean_active_days_per_user_28d", "median_active_days_per_user_28d",
    "p90_active_days_per_user_28d", "max_active_days_per_user_28d",
    # Repeat-view users
    "repeat_view_users_28d", "repeat_view_user_share_28d",
    "repeat_view_users_previous_28d", "repeat_view_user_share_previous_28d",
    # Statuses
    "repeat_usage_status", "activity_evidence_status", "activity_metric_reasons",
    # Privacy suppression metadata
    "activity_privacy_suppressed", "activity_suppressed_fields",
    "activity_privacy_suppression_reason",
]

_SUPPRESSIBLE_SHARE_FIELDS = [
    "returning_user_share_28d",
    "one_time_user_share_28d",
    "repeat_view_user_share_28d",
    "mean_active_days_per_user_28d",
    "median_active_days_per_user_28d",
    "p90_active_days_per_user_28d",
    "max_active_days_per_user_28d",
    "returning_user_share_previous_28d",
    "one_time_user_share_previous_28d",
    "repeat_view_user_share_previous_28d",
    "returning_user_share_90d",
    "one_time_user_share_90d",
    "repeat_view_user_share_90d",
    "returning_user_share_change_28d",
]

_VALID_DIRECTIONS = {
    "growing", "declining", "stable", "newly_active", "inactive",
    "insufficient_history", "no_valid_user_data",
}

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class UserEngagementMetricsConfig:
    MIN_ABSOLUTE_USER_CHANGE_FOR_DIRECTION: int = 2
    USER_GROWTH_MATERIAL_PCT: float = 0.20
    USER_DECLINE_MATERIAL_PCT: float = 0.20
    MIN_BASE_USERS_FOR_PCT_CLASSIFICATION: int = 3
    RETURNING_SHARE_STRONG_THRESHOLD: float = 0.50
    RETURNING_SHARE_MODERATE_THRESHOLD: float = 0.25
    MIN_USERS_FOR_REPEAT_STATUS: int = 3
    MIN_USERS_FOR_DISTRIBUTION_METRICS: int = 5
    MIN_USERS_FOR_SHARE_METRICS: int = 5
    PERCENTILE_INTERPOLATION: str = "linear"


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def _parse_date(val) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return pd.to_datetime(str(val)).date()


def _parse_mart_dates(mart_df: pd.DataFrame) -> pd.DataFrame:
    """Return mart_df with usage_date as datetime.date."""
    df = mart_df.copy()
    df["usage_date"] = pd.to_datetime(df["usage_date"], errors="coerce").dt.date
    return df


# ---------------------------------------------------------------------------
# Window user activity
# ---------------------------------------------------------------------------

_WINDOW_ACTIVITY_COLS = [
    "user_key", "active_date_count", "window_views",
    "returning_user_flag", "one_time_user_flag", "repeat_view_user_flag",
]


def calculate_window_user_activity(
    mart_df: pd.DataFrame,
    report_id: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Filter mart_df to report_id and inclusive date range.
    Return user-level aggregate (one row per user_key).
    """
    if mart_df.empty:
        return pd.DataFrame(columns=_WINDOW_ACTIVITY_COLS)

    df = _parse_mart_dates(mart_df)
    mask = (
        (df["report_id"] == report_id)
        & (df["usage_date"] >= start_date)
        & (df["usage_date"] <= end_date)
    )
    filtered = df[mask]

    if filtered.empty:
        return pd.DataFrame(columns=_WINDOW_ACTIVITY_COLS)

    grouped = (
        filtered.groupby("user_key", sort=False)
        .agg(
            active_date_count=("usage_date", "nunique"),
            window_views=("daily_views", "sum"),
        )
        .reset_index()
    )
    grouped["returning_user_flag"] = grouped["active_date_count"] >= 2
    grouped["one_time_user_flag"] = grouped["active_date_count"] == 1
    grouped["repeat_view_user_flag"] = grouped["window_views"] > 1

    return grouped[_WINDOW_ACTIVITY_COLS]


# ---------------------------------------------------------------------------
# Sub-metric calculators
# ---------------------------------------------------------------------------

def calculate_active_user_metrics(user_activity_df: pd.DataFrame) -> dict:
    if user_activity_df.empty:
        return {"unique_users": 0, "active_user_days": 0}
    return {
        "unique_users": len(user_activity_df),
        "active_user_days": int(user_activity_df["active_date_count"].sum()),
    }


def calculate_returning_user_metrics(
    user_activity_df: pd.DataFrame,
    cfg: UserEngagementMetricsConfig,
) -> dict:
    if user_activity_df.empty:
        return {
            "returning_users": 0,
            "one_time_users": 0,
            "returning_user_share": pd.NA,
            "one_time_user_share": pd.NA,
        }
    n = len(user_activity_df)
    returning = int(user_activity_df["returning_user_flag"].sum())
    one_time = int(user_activity_df["one_time_user_flag"].sum())
    if n == 0:
        ret_share = pd.NA
        ot_share = pd.NA
    else:
        ret_share = returning / n
        ot_share = one_time / n
    return {
        "returning_users": returning,
        "one_time_users": one_time,
        "returning_user_share": ret_share,
        "one_time_user_share": ot_share,
    }


def calculate_repeat_view_metrics(
    user_activity_df: pd.DataFrame,
    cfg: UserEngagementMetricsConfig,
) -> dict:
    if user_activity_df.empty:
        return {"repeat_view_users": 0, "repeat_view_user_share": pd.NA}
    n = len(user_activity_df)
    rv = int(user_activity_df["repeat_view_user_flag"].sum())
    share = rv / n if n > 0 else pd.NA
    return {"repeat_view_users": rv, "repeat_view_user_share": share}


def calculate_active_day_distribution(
    user_activity_df: pd.DataFrame,
    cfg: UserEngagementMetricsConfig,
) -> dict:
    null_result = {
        "mean_active_days": None,
        "median_active_days": None,
        "p90_active_days": None,
        "max_active_days": None,
    }
    if user_activity_df.empty:
        return null_result
    counts = user_activity_df["active_date_count"]
    return {
        "mean_active_days": float(counts.mean()),
        "median_active_days": float(counts.median()),
        "p90_active_days": float(np.percentile(counts, 90)),
        "max_active_days": float(counts.max()),
    }


# ---------------------------------------------------------------------------
# Active-user change / direction
# ---------------------------------------------------------------------------

def calculate_active_user_change(
    recent_count: Optional[int],
    previous_count: Optional[int],
    comparison_sufficient: bool,
    has_valid_data: bool,
    cfg: Optional[UserEngagementMetricsConfig] = None,
) -> dict:
    if cfg is None:
        cfg = UserEngagementMetricsConfig()

    if not has_valid_data:
        return {"change": None, "change_pct": None, "direction": "no_valid_user_data"}

    if not comparison_sufficient:
        return {"change": None, "change_pct": None, "direction": "insufficient_history"}

    if recent_count is None or previous_count is None:
        return {"change": None, "change_pct": None, "direction": "insufficient_history"}

    if recent_count == 0 and previous_count == 0:
        return {"change": 0, "change_pct": None, "direction": "inactive"}

    if previous_count == 0 and recent_count > 0:
        return {"change": recent_count, "change_pct": None, "direction": "newly_active"}

    if recent_count == 0 and previous_count > 0:
        change = recent_count - previous_count
        change_pct = change / previous_count
        return {"change": change, "change_pct": change_pct, "direction": "declining"}

    change = recent_count - previous_count
    change_pct = change / previous_count

    abs_change = abs(change)
    if (
        change >= cfg.MIN_ABSOLUTE_USER_CHANGE_FOR_DIRECTION
        and change_pct >= cfg.USER_GROWTH_MATERIAL_PCT
    ):
        direction = "growing"
    elif (
        change <= -cfg.MIN_ABSOLUTE_USER_CHANGE_FOR_DIRECTION
        and change_pct <= -cfg.USER_DECLINE_MATERIAL_PCT
    ):
        direction = "declining"
    else:
        direction = "stable"

    return {"change": change, "change_pct": change_pct, "direction": direction}


# ---------------------------------------------------------------------------
# Status classifiers
# ---------------------------------------------------------------------------

def classify_repeat_usage_status(
    returning_share,
    unique_users: Optional[int],
    history_sufficient: bool,
    has_valid_data: bool,
    cfg: UserEngagementMetricsConfig,
) -> str:
    if not has_valid_data:
        return "no_valid_user_data"
    if not history_sufficient:
        return "insufficient_history"
    if unique_users == 0:
        return "no_recent_activity"
    if unique_users is None or unique_users < cfg.MIN_USERS_FOR_REPEAT_STATUS:
        return "privacy_suppressed"
    if returning_share is None or (isinstance(returning_share, float) and np.isnan(returning_share)):
        return "privacy_suppressed"
    try:
        if pd.isna(returning_share):
            return "privacy_suppressed"
    except (TypeError, ValueError):
        pass
    rs = float(returning_share)
    if rs >= cfg.RETURNING_SHARE_STRONG_THRESHOLD:
        return "strong_repeat_engagement"
    if rs >= cfg.RETURNING_SHARE_MODERATE_THRESHOLD:
        return "moderate_repeat_engagement"
    return "low_repeat_engagement"


# ---------------------------------------------------------------------------
# Privacy suppression
# ---------------------------------------------------------------------------

def apply_engagement_privacy_suppression(
    metrics: dict,
    unique_users: Optional[int],
    cfg: UserEngagementMetricsConfig,
) -> dict:
    metrics = dict(metrics)
    if unique_users is not None and unique_users < cfg.MIN_USERS_FOR_DISTRIBUTION_METRICS:
        suppressed = []
        for field in _SUPPRESSIBLE_SHARE_FIELDS:
            if field in metrics and metrics[field] is not None:
                try:
                    if not pd.isna(metrics[field]):
                        suppressed.append(field)
                except (TypeError, ValueError):
                    suppressed.append(field)
                metrics[field] = None
            elif field in metrics:
                metrics[field] = None
        metrics["activity_privacy_suppressed"] = True
        metrics["activity_suppressed_fields"] = ",".join(suppressed) if suppressed else None
        metrics["activity_privacy_suppression_reason"] = "unique_users_below_minimum"
    else:
        metrics["activity_privacy_suppressed"] = False
        metrics["activity_suppressed_fields"] = None
        metrics["activity_privacy_suppression_reason"] = None
    return metrics


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_report_user_activity_metrics(
    sufficiency_df: pd.DataFrame,
    mart_df: pd.DataFrame,
    quality_df: pd.DataFrame,
    boundaries_df: pd.DataFrame,
    cfg: UserEngagementMetricsConfig,
    analytics_run_id: str,
) -> pd.DataFrame:
    """
    Build one row per report_id in sufficiency_df.
    """
    generated_at = datetime.utcnow().isoformat()

    # ── Parse window boundaries ────────────────────────────────────────────
    if boundaries_df.empty:
        raise ValueError("boundaries_df is empty — run engagement windows step first")

    b = boundaries_df.iloc[0]
    analytics_as_of_date = str(b.get("analytics_as_of_date", ""))

    w7s = _parse_date(b.get("window_7d_start"))
    w7e = _parse_date(b.get("window_7d_end"))
    w28s = _parse_date(b.get("window_28d_start"))
    w28e = _parse_date(b.get("window_28d_end"))
    pw28s = _parse_date(b.get("previous_28d_start"))
    pw28e = _parse_date(b.get("previous_28d_end"))
    w90s = _parse_date(b.get("window_90d_start"))
    w90e = _parse_date(b.get("window_90d_end"))
    pw90s = _parse_date(b.get("previous_90d_start"))
    pw90e = _parse_date(b.get("previous_90d_end"))

    # ── Pre-parse mart dates once ──────────────────────────────────────────
    if not mart_df.empty:
        mart_parsed = _parse_mart_dates(mart_df)
    else:
        mart_parsed = mart_df.copy()

    # ── Index quality by report_id ─────────────────────────────────────────
    quality_by_report: dict = {}
    if not quality_df.empty and "report_id" in quality_df.columns:
        for _, row in quality_df.iterrows():
            quality_by_report[str(row["report_id"])] = row

    rows = []
    for _, suf_row in sufficiency_df.sort_values("report_id").iterrows():
        rid = str(suf_row["report_id"])
        report_name = suf_row.get("report_name", "")

        # ── Sufficiency flags ──────────────────────────────────────────────
        suf_7d = bool(suf_row.get("history_sufficient_7d", False))
        suf_28d = bool(suf_row.get("history_sufficient_28d", False))
        suf_prev28d = bool(suf_row.get("history_sufficient_previous_28d", False))
        cmp_28d = bool(suf_row.get("comparison_history_sufficient_28d", False))
        suf_90d = bool(suf_row.get("history_sufficient_90d", False))
        suf_prev90d = bool(suf_row.get("history_sufficient_previous_90d", False))
        cmp_90d = bool(suf_row.get("comparison_history_sufficient_90d", False))
        has_any_activity = bool(suf_row.get("has_any_valid_user_activity", False))

        # ── Quality info ───────────────────────────────────────────────────
        q_row = quality_by_report.get(rid)
        if q_row is not None:
            dq_status = str(q_row.get("data_quality_status", "good"))
            excluded_share = q_row.get("excluded_user_event_share", 0.0)
        else:
            dq_status = "good"
            excluded_share = 0.0

        has_valid_data = dq_status != "no_valid_user_data"

        # ── Active users 7d ────────────────────────────────────────────────
        if suf_7d and has_valid_data and w7s and w7e:
            ua7 = calculate_window_user_activity(mart_parsed, rid, w7s, w7e)
            unique_users_7d = calculate_active_user_metrics(ua7)["unique_users"]
        else:
            unique_users_7d = None

        # ── Active users 28d (current) ─────────────────────────────────────
        if suf_28d and has_valid_data and w28s and w28e:
            ua28 = calculate_window_user_activity(mart_parsed, rid, w28s, w28e)
            am28 = calculate_active_user_metrics(ua28)
            unique_users_28d = am28["unique_users"]
            rm28 = calculate_returning_user_metrics(ua28, cfg)
            rvm28 = calculate_repeat_view_metrics(ua28, cfg)
            dist28 = calculate_active_day_distribution(ua28, cfg)
            returning_users_28d = rm28["returning_users"]
            one_time_users_28d = rm28["one_time_users"]
            returning_user_share_28d = _to_float_or_none(rm28["returning_user_share"])
            one_time_user_share_28d = _to_float_or_none(rm28["one_time_user_share"])
            repeat_view_users_28d = rvm28["repeat_view_users"]
            repeat_view_user_share_28d = _to_float_or_none(rvm28["repeat_view_user_share"])
            mean_active_days_28d = dist28["mean_active_days"]
            median_active_days_28d = dist28["median_active_days"]
            p90_active_days_28d = dist28["p90_active_days"]
            max_active_days_28d = dist28["max_active_days"]
            # Zero-user suppression of shares
            if unique_users_28d == 0:
                returning_user_share_28d = None
                one_time_user_share_28d = None
                repeat_view_user_share_28d = None
                mean_active_days_28d = None
                median_active_days_28d = None
                p90_active_days_28d = None
                max_active_days_28d = None
        else:
            unique_users_28d = None
            returning_users_28d = None
            one_time_users_28d = None
            returning_user_share_28d = None
            one_time_user_share_28d = None
            repeat_view_users_28d = None
            repeat_view_user_share_28d = None
            mean_active_days_28d = None
            median_active_days_28d = None
            p90_active_days_28d = None
            max_active_days_28d = None

        # ── Active users previous 28d ──────────────────────────────────────
        if suf_prev28d and has_valid_data and pw28s and pw28e:
            uaprev28 = calculate_window_user_activity(mart_parsed, rid, pw28s, pw28e)
            unique_users_prev28d = calculate_active_user_metrics(uaprev28)["unique_users"]
            rmprev28 = calculate_returning_user_metrics(uaprev28, cfg)
            rvmprev28 = calculate_repeat_view_metrics(uaprev28, cfg)
            returning_users_prev28d = rmprev28["returning_users"]
            one_time_users_prev28d = rmprev28["one_time_users"]
            returning_user_share_prev28d = _to_float_or_none(rmprev28["returning_user_share"])
            one_time_user_share_prev28d = _to_float_or_none(rmprev28["one_time_user_share"])
            repeat_view_users_prev28d = rvmprev28["repeat_view_users"]
            repeat_view_user_share_prev28d = _to_float_or_none(rvmprev28["repeat_view_user_share"])
            if unique_users_prev28d == 0:
                returning_user_share_prev28d = None
                one_time_user_share_prev28d = None
                repeat_view_user_share_prev28d = None
        else:
            unique_users_prev28d = None
            returning_users_prev28d = None
            one_time_users_prev28d = None
            returning_user_share_prev28d = None
            one_time_user_share_prev28d = None
            repeat_view_users_prev28d = None
            repeat_view_user_share_prev28d = None

        # ── 28d change / direction ─────────────────────────────────────────
        chg28 = calculate_active_user_change(
            unique_users_28d, unique_users_prev28d, cmp_28d, has_valid_data, cfg
        )
        active_user_change_28d = chg28["change"]
        active_user_change_28d_pct = chg28["change_pct"]
        active_user_direction_28d = chg28["direction"]

        # ── Returning share change 28d ─────────────────────────────────────
        if (
            returning_user_share_28d is not None
            and returning_user_share_prev28d is not None
        ):
            returning_user_share_change_28d = returning_user_share_28d - returning_user_share_prev28d
        else:
            returning_user_share_change_28d = None

        # ── Active users 90d ───────────────────────────────────────────────
        if suf_90d and has_valid_data and w90s and w90e:
            ua90 = calculate_window_user_activity(mart_parsed, rid, w90s, w90e)
            unique_users_90d = calculate_active_user_metrics(ua90)["unique_users"]
            rm90 = calculate_returning_user_metrics(ua90, cfg)
            rvm90 = calculate_repeat_view_metrics(ua90, cfg)
            returning_users_90d = rm90["returning_users"]
            one_time_users_90d = rm90["one_time_users"]
            returning_user_share_90d = _to_float_or_none(rm90["returning_user_share"])
            one_time_user_share_90d = _to_float_or_none(rm90["one_time_user_share"])
            repeat_view_user_share_90d = _to_float_or_none(rvm90["repeat_view_user_share"])
            if unique_users_90d == 0:
                returning_user_share_90d = None
                one_time_user_share_90d = None
                repeat_view_user_share_90d = None
        else:
            unique_users_90d = None
            returning_users_90d = None
            one_time_users_90d = None
            returning_user_share_90d = None
            one_time_user_share_90d = None
            repeat_view_user_share_90d = None

        # ── Active users previous 90d ──────────────────────────────────────
        if suf_prev90d and has_valid_data and pw90s and pw90e:
            uaprev90 = calculate_window_user_activity(mart_parsed, rid, pw90s, pw90e)
            unique_users_prev90d = calculate_active_user_metrics(uaprev90)["unique_users"]
        else:
            unique_users_prev90d = None

        # ── 90d change / direction ─────────────────────────────────────────
        chg90 = calculate_active_user_change(
            unique_users_90d, unique_users_prev90d, cmp_90d, has_valid_data, cfg
        )
        active_user_change_90d = chg90["change"]
        active_user_change_90d_pct = chg90["change_pct"]
        active_user_direction_90d = chg90["direction"]

        # ── Build metrics dict for suppression ────────────────────────────
        metrics = {
            "returning_user_share_28d": returning_user_share_28d,
            "one_time_user_share_28d": one_time_user_share_28d,
            "repeat_view_user_share_28d": repeat_view_user_share_28d,
            "mean_active_days_per_user_28d": mean_active_days_28d,
            "median_active_days_per_user_28d": median_active_days_28d,
            "p90_active_days_per_user_28d": p90_active_days_28d,
            "max_active_days_per_user_28d": max_active_days_28d,
            "returning_user_share_previous_28d": returning_user_share_prev28d,
            "one_time_user_share_previous_28d": one_time_user_share_prev28d,
            "repeat_view_user_share_previous_28d": repeat_view_user_share_prev28d,
            "returning_user_share_90d": returning_user_share_90d,
            "one_time_user_share_90d": one_time_user_share_90d,
            "repeat_view_user_share_90d": repeat_view_user_share_90d,
            "returning_user_share_change_28d": returning_user_share_change_28d,
        }
        metrics = apply_engagement_privacy_suppression(metrics, unique_users_28d, cfg)

        # ── Repeat-usage status ────────────────────────────────────────────
        repeat_usage_status = classify_repeat_usage_status(
            returning_share=metrics.get("returning_user_share_28d"),
            unique_users=unique_users_28d,
            history_sufficient=suf_28d,
            has_valid_data=has_valid_data,
            cfg=cfg,
        )

        # ── Activity evidence status ───────────────────────────────────────
        if not has_valid_data:
            activity_evidence_status = "no_valid_user_data"
        elif suf_28d or suf_7d or suf_90d:
            if suf_28d and suf_prev28d:
                activity_evidence_status = "sufficient"
            elif suf_28d or suf_7d:
                activity_evidence_status = "partial_history"
            else:
                activity_evidence_status = "partial_history"
        else:
            activity_evidence_status = "insufficient_history"

        # ── Privacy suppression status summary ────────────────────────────
        if metrics.get("activity_privacy_suppressed"):
            privacy_suppression_status = "suppressed"
        else:
            privacy_suppression_status = "not_suppressed"

        # ── Activity metric reasons ────────────────────────────────────────
        reasons = []
        if activity_evidence_status != "sufficient":
            reasons.append(f"evidence:{activity_evidence_status}")
        if metrics.get("activity_privacy_suppressed"):
            reasons.append("privacy:suppressed")
        if unique_users_28d is not None:
            reasons.append(f"active_users_28d:{unique_users_28d}")
        if active_user_direction_28d:
            reasons.append(f"direction_28d:{active_user_direction_28d}")
        if metrics.get("returning_user_share_28d") is not None:
            reasons.append(f"returning_share_28d:{metrics['returning_user_share_28d']:.3f}")
        if metrics.get("one_time_user_share_28d") is not None:
            reasons.append(f"one_time_share_28d:{metrics['one_time_user_share_28d']:.3f}")
        if unique_users_28d == 0 or (unique_users_28d is None and suf_28d):
            reasons.append("no_recent_activity_28d")
        activity_metric_reasons = "|".join(reasons) if reasons else "none"

        row = {
            "analytics_run_id": analytics_run_id,
            "generated_at": generated_at,
            "analytics_as_of_date": analytics_as_of_date,
            "report_id": rid,
            "report_name": report_name,
            "history_sufficient_7d": suf_7d,
            "history_sufficient_28d": suf_28d,
            "history_sufficient_previous_28d": suf_prev28d,
            "comparison_history_sufficient_28d": cmp_28d,
            "history_sufficient_90d": suf_90d,
            "history_sufficient_previous_90d": suf_prev90d,
            "comparison_history_sufficient_90d": cmp_90d,
            "has_any_valid_user_activity": has_any_activity,
            "user_data_quality_status": dq_status,
            "excluded_user_event_share": excluded_share,
            "privacy_suppression_status": privacy_suppression_status,
            # Active-user breadth
            "unique_users_7d": unique_users_7d,
            "unique_users_28d": unique_users_28d,
            "unique_users_previous_28d": unique_users_prev28d,
            "unique_users_90d": unique_users_90d,
            "unique_users_previous_90d": unique_users_prev90d,
            "active_user_change_28d": active_user_change_28d,
            "active_user_change_28d_pct": active_user_change_28d_pct,
            "active_user_change_90d": active_user_change_90d,
            "active_user_change_90d_pct": active_user_change_90d_pct,
            "active_user_direction_28d": active_user_direction_28d,
            "active_user_direction_90d": active_user_direction_90d,
            # Returning / one-time (28d)
            "returning_users_28d": returning_users_28d,
            "one_time_users_28d": one_time_users_28d,
            "returning_user_share_28d": metrics.get("returning_user_share_28d"),
            "one_time_user_share_28d": metrics.get("one_time_user_share_28d"),
            "returning_users_previous_28d": returning_users_prev28d,
            "one_time_users_previous_28d": one_time_users_prev28d,
            "returning_user_share_previous_28d": metrics.get("returning_user_share_previous_28d"),
            "one_time_user_share_previous_28d": metrics.get("one_time_user_share_previous_28d"),
            "returning_user_share_change_28d": metrics.get("returning_user_share_change_28d"),
            # Returning / one-time (90d)
            "returning_users_90d": returning_users_90d,
            "one_time_users_90d": one_time_users_90d,
            "returning_user_share_90d": metrics.get("returning_user_share_90d"),
            "one_time_user_share_90d": metrics.get("one_time_user_share_90d"),
            # Distribution (28d)
            "mean_active_days_per_user_28d": metrics.get("mean_active_days_per_user_28d"),
            "median_active_days_per_user_28d": metrics.get("median_active_days_per_user_28d"),
            "p90_active_days_per_user_28d": metrics.get("p90_active_days_per_user_28d"),
            "max_active_days_per_user_28d": metrics.get("max_active_days_per_user_28d"),
            # Repeat-view
            "repeat_view_users_28d": repeat_view_users_28d,
            "repeat_view_user_share_28d": metrics.get("repeat_view_user_share_28d"),
            "repeat_view_users_previous_28d": repeat_view_users_prev28d,
            "repeat_view_user_share_previous_28d": metrics.get("repeat_view_user_share_previous_28d"),
            # Statuses
            "repeat_usage_status": repeat_usage_status,
            "activity_evidence_status": activity_evidence_status,
            "activity_metric_reasons": activity_metric_reasons,
            # Privacy metadata
            "activity_privacy_suppressed": metrics.get("activity_privacy_suppressed"),
            "activity_suppressed_fields": metrics.get("activity_suppressed_fields"),
            "activity_privacy_suppression_reason": metrics.get("activity_privacy_suppression_reason"),
        }
        rows.append(row)

    result = pd.DataFrame(rows, columns=REPORT_USER_ACTIVITY_METRICS_COLS)
    return result


def _to_float_or_none(val) -> Optional[float]:
    """Convert pd.NA / np.nan / None to None; otherwise float."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return float(val)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

_VALID_ACTIVITY_EVIDENCE = {
    "sufficient", "no_valid_user_data", "insufficient_history",
    "partial_history", "calculation_failed",
}

_VALID_REPEAT_STATUS = {
    "strong_repeat_engagement", "moderate_repeat_engagement", "low_repeat_engagement",
    "no_recent_activity", "privacy_suppressed", "insufficient_history", "no_valid_user_data",
}


def validate_report_user_activity_metrics(
    df: pd.DataFrame,
    cfg: Optional[UserEngagementMetricsConfig] = None,
) -> None:
    if cfg is None:
        cfg = UserEngagementMetricsConfig()

    # 1. Required columns
    missing = [c for c in REPORT_USER_ACTIVITY_METRICS_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if df.empty:
        return

    # 2. Unique grain
    dupes = df.duplicated(subset=["analytics_run_id", "report_id"])
    if dupes.any():
        raise ValueError(f"Duplicate (analytics_run_id, report_id) rows: {df.loc[dupes, 'report_id'].tolist()}")

    # 3. Counts non-negative
    for col in ["unique_users_7d", "unique_users_28d", "unique_users_previous_28d",
                "unique_users_90d", "unique_users_previous_90d",
                "returning_users_28d", "one_time_users_28d",
                "returning_users_previous_28d", "one_time_users_previous_28d",
                "returning_users_90d", "one_time_users_90d",
                "repeat_view_users_28d", "repeat_view_users_previous_28d"]:
        if col in df.columns:
            bad = df[df[col].notna() & (df[col] < 0)]
            if not bad.empty:
                raise ValueError(f"Negative values in {col}: {bad['report_id'].tolist()}")

    # 4. returning <= unique
    for window, ret_col, uniq_col in [
        ("28d", "returning_users_28d", "unique_users_28d"),
        ("prev28d", "returning_users_previous_28d", "unique_users_previous_28d"),
        ("90d", "returning_users_90d", "unique_users_90d"),
    ]:
        if ret_col in df.columns and uniq_col in df.columns:
            both = df[df[ret_col].notna() & df[uniq_col].notna()]
            bad = both[both[ret_col] > both[uniq_col]]
            if not bad.empty:
                raise ValueError(f"{ret_col} > {uniq_col} for: {bad['report_id'].tolist()}")

    # 5. one_time <= unique
    for window, ot_col, uniq_col in [
        ("28d", "one_time_users_28d", "unique_users_28d"),
        ("prev28d", "one_time_users_previous_28d", "unique_users_previous_28d"),
        ("90d", "one_time_users_90d", "unique_users_90d"),
    ]:
        if ot_col in df.columns and uniq_col in df.columns:
            both = df[df[ot_col].notna() & df[uniq_col].notna()]
            bad = both[both[ot_col] > both[uniq_col]]
            if not bad.empty:
                raise ValueError(f"{ot_col} > {uniq_col} for: {bad['report_id'].tolist()}")

    # 6. returning + one_time = unique (where sufficient and activity > 0)
    for ret_col, ot_col, uniq_col, suf_col in [
        ("returning_users_28d", "one_time_users_28d", "unique_users_28d", "history_sufficient_28d"),
        ("returning_users_90d", "one_time_users_90d", "unique_users_90d", "history_sufficient_90d"),
    ]:
        if all(c in df.columns for c in [ret_col, ot_col, uniq_col, suf_col]):
            mask = (
                df[suf_col].fillna(False)
                & df[ret_col].notna()
                & df[ot_col].notna()
                & df[uniq_col].notna()
                & (df[uniq_col] > 0)
            )
            sub = df[mask]
            bad = sub[(sub[ret_col] + sub[ot_col] - sub[uniq_col]).abs() > 1]
            if not bad.empty:
                raise ValueError(
                    f"{ret_col} + {ot_col} != {uniq_col} (±1 tolerance) for: "
                    f"{bad['report_id'].tolist()}"
                )

    # 7. repeat_view_users <= unique
    for rv_col, uniq_col in [
        ("repeat_view_users_28d", "unique_users_28d"),
        ("repeat_view_users_previous_28d", "unique_users_previous_28d"),
    ]:
        if rv_col in df.columns and uniq_col in df.columns:
            both = df[df[rv_col].notna() & df[uniq_col].notna()]
            bad = both[both[rv_col] > both[uniq_col]]
            if not bad.empty:
                raise ValueError(f"{rv_col} > {uniq_col} for: {bad['report_id'].tolist()}")

    # 8. Shares in [0, 1]
    share_cols = [
        "returning_user_share_28d", "one_time_user_share_28d",
        "returning_user_share_previous_28d", "one_time_user_share_previous_28d",
        "returning_user_share_90d", "one_time_user_share_90d",
        "repeat_view_user_share_28d", "repeat_view_user_share_previous_28d",
        "repeat_view_user_share_90d",
    ]
    for col in share_cols:
        if col in df.columns:
            bad = df[df[col].notna() & ((df[col] < 0) | (df[col] > 1))]
            if not bad.empty:
                raise ValueError(f"{col} out of [0,1] for: {bad['report_id'].tolist()}")

    # 9. returning + one_time shares sum to 1.0 (within 0.001)
    for ret_s, ot_s in [
        ("returning_user_share_28d", "one_time_user_share_28d"),
        ("returning_user_share_previous_28d", "one_time_user_share_previous_28d"),
        ("returning_user_share_90d", "one_time_user_share_90d"),
    ]:
        if ret_s in df.columns and ot_s in df.columns:
            both = df[df[ret_s].notna() & df[ot_s].notna()]
            bad = both[((both[ret_s] + both[ot_s]) - 1.0).abs() > 0.001]
            if not bad.empty:
                raise ValueError(
                    f"{ret_s} + {ot_s} don't sum to 1.0 for: {bad['report_id'].tolist()}"
                )

    # 10. Shares null when unique_users = 0
    for uniq_col, share_cols_for_window in [
        ("unique_users_28d", ["returning_user_share_28d", "one_time_user_share_28d", "repeat_view_user_share_28d"]),
        ("unique_users_90d", ["returning_user_share_90d", "one_time_user_share_90d"]),
    ]:
        if uniq_col in df.columns:
            zero_rows = df[df[uniq_col].notna() & (df[uniq_col] == 0)]
            for sc in share_cols_for_window:
                if sc in df.columns:
                    bad = zero_rows[zero_rows[sc].notna()]
                    if not bad.empty:
                        raise ValueError(
                            f"{sc} must be null when {uniq_col}=0 for: {bad['report_id'].tolist()}"
                        )

    # 11. Suppressed shares are null
    if "activity_privacy_suppressed" in df.columns:
        suppressed = df[df["activity_privacy_suppressed"] == True]
        for col in _SUPPRESSIBLE_SHARE_FIELDS:
            if col in df.columns:
                bad = suppressed[suppressed[col].notna()]
                if not bad.empty:
                    raise ValueError(
                        f"Suppressed rows have non-null {col} for: {bad['report_id'].tolist()}"
                    )

    # 12. Direction values in allowed set
    for dir_col in ["active_user_direction_28d", "active_user_direction_90d"]:
        if dir_col in df.columns:
            bad = df[df[dir_col].notna() & ~df[dir_col].isin(_VALID_DIRECTIONS)]
            if not bad.empty:
                raise ValueError(f"Invalid direction values in {dir_col}: {bad[dir_col].unique().tolist()}")

    # 13. newly_active requires previous=0 and recent>0
    for dir_col, recent_col, prev_col, suf_col in [
        ("active_user_direction_28d", "unique_users_28d", "unique_users_previous_28d", "comparison_history_sufficient_28d"),
        ("active_user_direction_90d", "unique_users_90d", "unique_users_previous_90d", "comparison_history_sufficient_90d"),
    ]:
        if dir_col in df.columns:
            newly_active = df[df[dir_col] == "newly_active"]
            if not newly_active.empty and prev_col in df.columns:
                bad = newly_active[newly_active[prev_col].notna() & (newly_active[prev_col] != 0)]
                if not bad.empty:
                    raise ValueError(f"newly_active but previous_count != 0 for: {bad['report_id'].tolist()}")

    # 14. inactive requires both=0 AND comparison_sufficient=True
    for dir_col, recent_col, prev_col, suf_col in [
        ("active_user_direction_28d", "unique_users_28d", "unique_users_previous_28d", "comparison_history_sufficient_28d"),
    ]:
        if dir_col in df.columns and suf_col in df.columns:
            inactive = df[df[dir_col] == "inactive"]
            if not inactive.empty:
                bad = inactive[inactive[suf_col].fillna(False) == False]
                if not bad.empty:
                    raise ValueError(
                        f"inactive direction but comparison_sufficient=False for: {bad['report_id'].tolist()}"
                    )

    # 15. insufficient_history not when comparison_sufficient=True
    for dir_col, suf_col in [
        ("active_user_direction_28d", "comparison_history_sufficient_28d"),
        ("active_user_direction_90d", "comparison_history_sufficient_90d"),
    ]:
        if dir_col in df.columns and suf_col in df.columns:
            bad = df[
                (df[dir_col] == "insufficient_history")
                & (df[suf_col].fillna(False) == True)
            ]
            if not bad.empty:
                raise ValueError(
                    f"insufficient_history direction when comparison_sufficient=True for: {bad['report_id'].tolist()}"
                )

    # 16. no_valid_user_data direction not when has_valid_data=True
    for dir_col in ["active_user_direction_28d", "active_user_direction_90d"]:
        if dir_col in df.columns and "user_data_quality_status" in df.columns:
            bad = df[
                (df[dir_col] == "no_valid_user_data")
                & (df["user_data_quality_status"] != "no_valid_user_data")
            ]
            if not bad.empty:
                raise ValueError(
                    f"no_valid_user_data direction when data is valid for: {bad['report_id'].tolist()}"
                )

    # 17. p90 <= max
    if "p90_active_days_per_user_28d" in df.columns and "max_active_days_per_user_28d" in df.columns:
        both = df[df["p90_active_days_per_user_28d"].notna() & df["max_active_days_per_user_28d"].notna()]
        bad = both[both["p90_active_days_per_user_28d"] > both["max_active_days_per_user_28d"]]
        if not bad.empty:
            raise ValueError(f"p90_active_days > max_active_days for: {bad['report_id'].tolist()}")

    # 18. Median/mean/p90 null when suppressed
    if "activity_privacy_suppressed" in df.columns:
        sup = df[df["activity_privacy_suppressed"] == True]
        for col in ["mean_active_days_per_user_28d", "median_active_days_per_user_28d",
                    "p90_active_days_per_user_28d", "max_active_days_per_user_28d"]:
            if col in df.columns:
                bad = sup[sup[col].notna()]
                if not bad.empty:
                    raise ValueError(f"Suppressed {col} not null for: {bad['report_id'].tolist()}")

    # 19. No prohibited identifier columns
    validate_no_direct_identifiers(df, context="report_user_activity_metrics")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_report_user_activity_metrics(
    df: pd.DataFrame,
    project_root: Path,
) -> Path:
    validate_report_user_activity_metrics(df)
    output_dir = project_root / "outputs" / "analytics"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "report_user_activity_metrics.csv"
    df_sorted = df.sort_values("report_id").reset_index(drop=True)
    df_sorted.to_csv(out_path, index=False)
    return out_path
