"""
Report-level usage-frequency and engagement-intensity metrics.

Sprint 6 — User Analytics
Scope: views per user, active days per user, return gaps, frequency direction,
       frequency classification, privacy suppression, output persistence.

NOT in scope: concentration/HHI, overall engagement status, GenAI, Streamlit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.analytics.privacy_policy import (
    PROHIBITED_OUTPUT_COLUMNS,
    validate_no_direct_identifiers,
    detect_email_like_values,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FrequencyMetricsConfig:
    # Distribution threshold
    MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS: int = 5
    MIN_RETURNING_USERS_FOR_GAP_METRICS: int = 2  # need >= 2 users with 2+ dates
    # Classification thresholds
    HIGH_FREQUENCY_MEDIAN_ACTIVE_DAYS: float = 5.0    # median >= 5 active days in 28d
    MODERATE_FREQUENCY_MEDIAN_ACTIVE_DAYS: float = 2.0 # median >= 2
    LOW_FREQUENCY_MEDIAN_ACTIVE_DAYS: float = 1.0      # median == 1 (one-time window)
    BURSTY_VIEWS_PER_USER_DAY_THRESHOLD: float = 3.0   # median views/day >= 3
    BURSTY_MAX_MEDIAN_ACTIVE_DAYS: float = 2.0          # AND median active days <= 2
    FREQUENCY_CHANGE_MATERIAL_PCT: float = 0.20         # 20% change is material
    MIN_ABSOLUTE_CHANGE_FOR_FREQUENCY_DIRECTION: int = 2
    PERCENTILE_INTERPOLATION_METHOD: str = "linear"


# ---------------------------------------------------------------------------
# Schema constant
# ---------------------------------------------------------------------------

REPORT_USER_FREQUENCY_METRICS_COLS = [
    # Identity and evidence
    "analytics_run_id", "generated_at", "analytics_as_of_date",
    "report_id", "report_name",
    "history_sufficient_7d", "history_sufficient_28d",
    "history_sufficient_previous_28d", "comparison_history_sufficient_28d",
    "history_sufficient_90d", "history_sufficient_previous_90d",
    "comparison_history_sufficient_90d",
    "has_any_valid_user_activity", "user_data_quality_status",
    "privacy_suppression_status",
    # Recent 7-day
    "total_views_7d", "total_user_report_days_7d", "unique_users_7d",
    "views_per_active_user_7d", "views_per_user_day_7d",
    "mean_user_active_days_7d", "median_user_active_days_7d",
    # Recent 28-day
    "total_views_28d", "total_user_report_days_28d", "unique_users_28d",
    "views_per_active_user_28d", "views_per_user_day_28d",
    "mean_views_per_user_28d", "median_views_per_user_28d",
    "p75_views_per_user_28d", "p90_views_per_user_28d", "max_views_per_user_28d",
    "mean_user_active_days_28d", "median_user_active_days_28d",
    "p75_user_active_days_28d", "p90_user_active_days_28d", "max_user_active_days_28d",
    "mean_views_per_user_day_28d", "median_views_per_user_day_28d", "p90_views_per_user_day_28d",
    "mean_return_gap_days_28d", "median_return_gap_days_28d",
    "returning_user_gap_observation_count_28d",
    # Previous 28-day
    "total_views_previous_28d", "total_user_report_days_previous_28d", "unique_users_previous_28d",
    "views_per_active_user_previous_28d", "views_per_user_day_previous_28d",
    "median_views_per_user_previous_28d", "median_user_active_days_previous_28d",
    "median_return_gap_days_previous_28d",
    # Recent 90-day
    "total_views_90d", "total_user_report_days_90d", "unique_users_90d",
    "views_per_active_user_90d", "views_per_user_day_90d",
    "median_views_per_user_90d", "median_user_active_days_90d",
    # Comparison 28d
    "total_views_change_28d", "total_views_change_28d_pct",
    "user_report_days_change_28d", "user_report_days_change_28d_pct",
    "views_per_active_user_change_28d", "views_per_active_user_change_28d_pct",
    "views_per_user_day_change_28d", "views_per_user_day_change_28d_pct",
    "median_views_per_user_change_28d", "median_user_active_days_change_28d",
    "median_return_gap_change_days_28d",
    # Status
    "frequency_direction", "frequency_status",
    "frequency_evidence_status", "frequency_reasons",
    # Privacy
    "frequency_privacy_suppressed", "frequency_privacy_suppression_reason",
    "suppressed_frequency_fields",
]

# Fields suppressed for recent 28d when unique_users < threshold
_SUPPRESSIBLE_28D_FIELDS = [
    "mean_views_per_user_28d", "median_views_per_user_28d",
    "p75_views_per_user_28d", "p90_views_per_user_28d", "max_views_per_user_28d",
    "mean_user_active_days_28d", "median_user_active_days_28d",
    "p75_user_active_days_28d", "p90_user_active_days_28d", "max_user_active_days_28d",
    "mean_views_per_user_day_28d", "median_views_per_user_day_28d", "p90_views_per_user_day_28d",
    "mean_return_gap_days_28d", "median_return_gap_days_28d",
    "returning_user_gap_observation_count_28d",
]

# Fields suppressed for previous 28d when unique_users_prev < threshold
_SUPPRESSIBLE_PREV28D_FIELDS = [
    "median_views_per_user_previous_28d",
    "median_user_active_days_previous_28d",
    "median_return_gap_days_previous_28d",
]

# Comparison fields suppressed when either window distribution was suppressed
_SUPPRESSIBLE_COMPARISON_FIELDS = [
    "median_views_per_user_change_28d",
    "median_user_active_days_change_28d",
    "median_return_gap_change_days_28d",
]

_VALID_FREQUENCY_STATUSES = {
    "no_valid_user_data",
    "insufficient_history",
    "privacy_suppressed",
    "no_recent_activity",
    "bursty_usage",
    "declining_frequency",
    "increasing_frequency",
    "high_frequency",
    "moderate_frequency",
    "occasional_but_consistent",
    "low_frequency",
    "calculation_failed",
}

_VALID_FREQUENCY_DIRECTIONS = {
    "increasing", "stable", "decreasing",
    "increasing_from_zero", "inactive",
    "insufficient_history", "privacy_suppressed", "no_valid_user_data",
}


# ---------------------------------------------------------------------------
# Date parsing helper
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
    if isinstance(val, date):
        return val
    return pd.to_datetime(str(val)).date()


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
# User-level window aggregation
# ---------------------------------------------------------------------------

_USER_WINDOW_FREQ_COLS = [
    "user_key",
    "user_total_views",
    "user_active_days",
    "user_views_per_active_day",
    "user_first_window_use_date",
    "user_last_window_use_date",
    "user_span_days",
]


def aggregate_user_window_frequency(
    mart_df: pd.DataFrame,
    report_id: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Filter mart_df to report_id and inclusive [start_date, end_date].
    Return user-level aggregate — one row per user_key.

    Returns empty DataFrame with correct columns if no rows.
    """
    if mart_df.empty:
        return pd.DataFrame(columns=_USER_WINDOW_FREQ_COLS)

    df = mart_df.copy()
    df["_usage_date"] = pd.to_datetime(df["usage_date"], errors="coerce").dt.date

    mask = (
        (df["report_id"] == report_id)
        & (df["_usage_date"] >= start_date)
        & (df["_usage_date"] <= end_date)
    )
    filtered = df[mask]

    if filtered.empty:
        return pd.DataFrame(columns=_USER_WINDOW_FREQ_COLS)

    grouped = (
        filtered.groupby("user_key", sort=False)
        .agg(
            user_total_views=("daily_views", "sum"),
            user_active_days=("_usage_date", "nunique"),
            user_first_window_use_date=("_usage_date", "min"),
            user_last_window_use_date=("_usage_date", "max"),
        )
        .reset_index()
    )

    grouped["user_views_per_active_day"] = (
        grouped["user_total_views"] / grouped["user_active_days"]
    )
    grouped["user_span_days"] = (
        (grouped["user_last_window_use_date"] - grouped["user_first_window_use_date"])
        .apply(lambda d: d.days + 1)
    )

    return grouped[_USER_WINDOW_FREQ_COLS]


# ---------------------------------------------------------------------------
# Return gap calculation
# ---------------------------------------------------------------------------

def calculate_return_gaps(
    user_df: pd.DataFrame,
    mart_window_df: pd.DataFrame,
    report_id: str,
    start_date: date,
    end_date: date,
    cfg: FrequencyMetricsConfig,
) -> dict:
    """
    For users with user_active_days >= 2 in user_df, compute pooled consecutive
    calendar-day gaps between their sorted distinct usage dates in the window.

    Gap policy: consecutive date gaps are pooled across all qualifying users
    before computing mean and median.

    Returns dict with keys:
        mean_return_gap_days, median_return_gap_days,
        returning_user_gap_observation_count
    All None if fewer than MIN_RETURNING_USERS_FOR_GAP_METRICS qualify.
    """
    null_result = {
        "mean_return_gap_days": None,
        "median_return_gap_days": None,
        "returning_user_gap_observation_count": None,
    }

    if user_df.empty or mart_window_df.empty:
        return null_result

    # Users with >= 2 active dates
    qualifying = user_df[user_df["user_active_days"] >= 2]["user_key"].tolist()

    if len(qualifying) < cfg.MIN_RETURNING_USERS_FOR_GAP_METRICS:
        return null_result

    # Parse dates from mart_window_df (already filtered to report+window)
    mdf = mart_window_df.copy()
    mdf["_usage_date"] = pd.to_datetime(mdf["usage_date"], errors="coerce").dt.date

    # Pool gaps across all qualifying users
    all_gaps: list[int] = []
    for uk in qualifying:
        user_dates = sorted(
            mdf.loc[mdf["user_key"] == uk, "_usage_date"].dropna().unique()
        )
        if len(user_dates) < 2:
            continue
        for i in range(1, len(user_dates)):
            gap = (user_dates[i] - user_dates[i - 1]).days
            all_gaps.append(gap)

    if not all_gaps:
        return null_result

    return {
        "mean_return_gap_days": float(np.mean(all_gaps)),
        "median_return_gap_days": float(np.median(all_gaps)),
        "returning_user_gap_observation_count": len(all_gaps),
    }


# ---------------------------------------------------------------------------
# Frequency metric calculation from user-level aggregate
# ---------------------------------------------------------------------------

def calculate_report_frequency_metrics(
    user_df: pd.DataFrame,
    total_views: int,
    total_user_report_days: int,
    cfg: FrequencyMetricsConfig,
) -> dict:
    """
    Compute report-level frequency metrics from user-level aggregate.

    Input: user-level aggregate from aggregate_user_window_frequency,
    plus pre-computed totals (total_views, total_user_report_days).

    Returns a dict of metric name → value. All distribution fields are None if
    user_df is empty. Privacy suppression is NOT applied here.
    """
    if user_df.empty:
        return {
            "unique_users": 0,
            "views_per_active_user": None,
            "views_per_user_day": None,
            "mean_views_per_user": None,
            "median_views_per_user": None,
            "p75_views_per_user": None,
            "p90_views_per_user": None,
            "max_views_per_user": None,
            "mean_user_active_days": None,
            "median_user_active_days": None,
            "p75_user_active_days": None,
            "p90_user_active_days": None,
            "max_user_active_days": None,
            "mean_views_per_user_day": None,
            "median_views_per_user_day": None,
            "p90_views_per_user_day": None,
        }

    unique_users = len(user_df)
    views_per_active_user = total_views / unique_users if unique_users > 0 else None
    views_per_user_day = (
        total_views / total_user_report_days if total_user_report_days > 0 else None
    )

    interp = cfg.PERCENTILE_INTERPOLATION_METHOD

    views_series = user_df["user_total_views"]
    active_days_series = user_df["user_active_days"]
    vpd_series = user_df["user_views_per_active_day"]

    return {
        "unique_users": unique_users,
        "views_per_active_user": views_per_active_user,
        "views_per_user_day": views_per_user_day,
        # Views per user distributions
        "mean_views_per_user": float(views_series.mean()),
        "median_views_per_user": float(views_series.quantile(0.5, interpolation=interp)),
        "p75_views_per_user": float(views_series.quantile(0.75, interpolation=interp)),
        "p90_views_per_user": float(views_series.quantile(0.90, interpolation=interp)),
        "max_views_per_user": float(views_series.max()),
        # Active days distributions
        "mean_user_active_days": float(active_days_series.mean()),
        "median_user_active_days": float(active_days_series.quantile(0.5, interpolation=interp)),
        "p75_user_active_days": float(active_days_series.quantile(0.75, interpolation=interp)),
        "p90_user_active_days": float(active_days_series.quantile(0.90, interpolation=interp)),
        "max_user_active_days": float(active_days_series.max()),
        # Views per active day distributions
        "mean_views_per_user_day": float(vpd_series.mean()),
        "median_views_per_user_day": float(vpd_series.quantile(0.5, interpolation=interp)),
        "p90_views_per_user_day": float(vpd_series.quantile(0.90, interpolation=interp)),
    }


# ---------------------------------------------------------------------------
# Change metrics
# ---------------------------------------------------------------------------

def calculate_frequency_change_metrics(
    recent: dict,
    previous: dict,
    comparison_sufficient: bool,
) -> dict:
    """
    Compute change metrics comparing recent vs previous window values.

    If not comparison_sufficient, all changes are None.
    change_pct is None when previous value is 0 or None.
    For median_return_gap_change_days: positive = gaps getting longer = less frequent returns.
    """
    null_result = {
        "total_views_change_28d": None,
        "total_views_change_28d_pct": None,
        "user_report_days_change_28d": None,
        "user_report_days_change_28d_pct": None,
        "views_per_active_user_change_28d": None,
        "views_per_active_user_change_28d_pct": None,
        "views_per_user_day_change_28d": None,
        "views_per_user_day_change_28d_pct": None,
        "median_views_per_user_change_28d": None,
        "median_user_active_days_change_28d": None,
        "median_return_gap_change_days_28d": None,
    }

    if not comparison_sufficient:
        return null_result

    def _change(r_val, p_val):
        if r_val is None or p_val is None:
            return None, None
        change = r_val - p_val
        pct = change / p_val if p_val != 0 else None
        return change, pct

    tv_c, tv_pct = _change(recent.get("total_views"), previous.get("total_views"))
    urd_c, urd_pct = _change(recent.get("total_user_report_days"), previous.get("total_user_report_days"))
    vpau_c, vpau_pct = _change(recent.get("views_per_active_user"), previous.get("views_per_active_user"))
    vpud_c, vpud_pct = _change(recent.get("views_per_user_day"), previous.get("views_per_user_day"))

    mvpu_c, _ = _change(recent.get("median_views_per_user"), previous.get("median_views_per_user"))
    muad_c, _ = _change(recent.get("median_user_active_days"), previous.get("median_user_active_days"))
    mrgd_c, _ = _change(recent.get("median_return_gap_days"), previous.get("median_return_gap_days"))

    return {
        "total_views_change_28d": tv_c,
        "total_views_change_28d_pct": tv_pct,
        "user_report_days_change_28d": urd_c,
        "user_report_days_change_28d_pct": urd_pct,
        "views_per_active_user_change_28d": vpau_c,
        "views_per_active_user_change_28d_pct": vpau_pct,
        "views_per_user_day_change_28d": vpud_c,
        "views_per_user_day_change_28d_pct": vpud_pct,
        "median_views_per_user_change_28d": mvpu_c,
        "median_user_active_days_change_28d": muad_c,
        "median_return_gap_change_days_28d": mrgd_c,
    }


# ---------------------------------------------------------------------------
# Classification: direction
# ---------------------------------------------------------------------------

def classify_frequency_direction(
    recent_total_views: Optional[int],
    previous_total_views: Optional[int],
    comparison_sufficient: bool,
    has_valid_data: bool,
    cfg: FrequencyMetricsConfig,
) -> str:
    """
    Returns one of: increasing, stable, decreasing, increasing_from_zero,
    inactive, insufficient_history, privacy_suppressed, no_valid_user_data
    """
    if not has_valid_data:
        return "no_valid_user_data"
    if not comparison_sufficient:
        return "insufficient_history"
    if recent_total_views is None or previous_total_views is None:
        return "insufficient_history"
    if recent_total_views == 0 and previous_total_views == 0:
        return "inactive"
    if previous_total_views == 0 and recent_total_views > 0:
        return "increasing_from_zero"
    if recent_total_views == 0 and previous_total_views > 0:
        return "decreasing"

    change = recent_total_views - previous_total_views
    pct = change / previous_total_views

    if (
        abs(change) >= cfg.MIN_ABSOLUTE_CHANGE_FOR_FREQUENCY_DIRECTION
        and pct >= cfg.FREQUENCY_CHANGE_MATERIAL_PCT
    ):
        return "increasing"
    elif (
        abs(change) >= cfg.MIN_ABSOLUTE_CHANGE_FOR_FREQUENCY_DIRECTION
        and pct <= -cfg.FREQUENCY_CHANGE_MATERIAL_PCT
    ):
        return "decreasing"
    return "stable"


# ---------------------------------------------------------------------------
# Classification: status
# ---------------------------------------------------------------------------

def classify_frequency_status(
    metrics_28d: dict,
    comparison_metrics: dict,
    direction: str,
    history_sufficient: bool,
    has_valid_data: bool,
    is_suppressed: bool,
    cfg: FrequencyMetricsConfig,
) -> tuple[str, str, list[str]]:
    """
    Returns (frequency_status, frequency_evidence_status, reasons_list).
    """
    try:
        unique_users_28d = metrics_28d.get("unique_users", 0) or 0
        median_user_active_days = metrics_28d.get("median_user_active_days")
        median_views_per_user_day = metrics_28d.get("median_views_per_user_day")
        median_user_active_days_change = comparison_metrics.get("median_user_active_days_change_28d")

        # --- Status determination (priority order) ---
        if not has_valid_data:
            status = "no_valid_user_data"
        elif not history_sufficient:
            status = "insufficient_history"
        elif is_suppressed:
            status = "privacy_suppressed"
        elif unique_users_28d == 0:
            status = "no_recent_activity"
        elif (
            median_views_per_user_day is not None
            and median_user_active_days is not None
            and median_views_per_user_day >= cfg.BURSTY_VIEWS_PER_USER_DAY_THRESHOLD
            and median_user_active_days <= cfg.BURSTY_MAX_MEDIAN_ACTIVE_DAYS
        ):
            status = "bursty_usage"
        elif (
            direction == "decreasing"
            and median_user_active_days_change is not None
            and median_user_active_days_change < 0
        ):
            status = "declining_frequency"
        elif direction in ("increasing", "increasing_from_zero"):
            status = "increasing_frequency"
        elif (
            median_user_active_days is not None
            and median_user_active_days >= cfg.HIGH_FREQUENCY_MEDIAN_ACTIVE_DAYS
        ):
            status = "high_frequency"
        elif (
            median_user_active_days is not None
            and median_user_active_days >= cfg.MODERATE_FREQUENCY_MEDIAN_ACTIVE_DAYS
        ):
            status = "moderate_frequency"
        elif (
            unique_users_28d > 0
            and median_user_active_days is not None
            and median_user_active_days >= 1.5
            and median_user_active_days < cfg.MODERATE_FREQUENCY_MEDIAN_ACTIVE_DAYS
        ):
            status = "occasional_but_consistent"
        elif unique_users_28d > 0:
            status = "low_frequency"
        else:
            status = "low_frequency"

        # --- Evidence status ---
        history_sufficient_28d = history_sufficient
        history_sufficient_7d = metrics_28d.get("_history_sufficient_7d", False)

        if not has_valid_data:
            evidence_status = "no_valid_user_data"
        elif history_sufficient_28d:
            evidence_status = "sufficient"
        elif history_sufficient_7d:
            evidence_status = "partial"
        else:
            evidence_status = "insufficient_history"

        # --- Reasons (deterministic ordered list) ---
        reasons: list[str] = []

        # 1. data validity
        if not has_valid_data:
            reasons.append("no_valid_user_data")

        # 2. history sufficiency
        if not history_sufficient:
            reasons.append("history_insufficient")
        else:
            reasons.append("history_sufficient_28d")

        # 3. privacy suppression
        if is_suppressed:
            reasons.append("privacy_suppressed")

        # 4. user count
        reasons.append(f"unique_users_28d:{unique_users_28d}")

        # 5. direction
        reasons.append(f"frequency_direction:{direction}")

        # 6. median active days
        if median_user_active_days is not None:
            reasons.append(f"median_active_days_28d:{median_user_active_days:.2f}")
        else:
            reasons.append("median_active_days_28d:null")

        # 7. median views per user day
        if median_views_per_user_day is not None:
            reasons.append(f"median_views_per_user_day:{median_views_per_user_day:.2f}")
        else:
            reasons.append("median_views_per_user_day:null")

        # 8. active days change
        if median_user_active_days_change is not None:
            reasons.append(f"median_active_days_change:{median_user_active_days_change:.2f}")
        else:
            reasons.append("median_active_days_change:null")

        # 9. frequency status
        reasons.append(f"frequency_status:{status}")

        # 10. evidence status
        reasons.append(f"evidence_status:{evidence_status}")

        return status, evidence_status, reasons

    except Exception as exc:
        return "calculation_failed", "calculation_failed", [f"error:{exc}"]


# ---------------------------------------------------------------------------
# Privacy suppression
# ---------------------------------------------------------------------------

def apply_frequency_privacy_suppression(
    freq_28d: dict,
    freq_prev_28d: dict,
    comparison: dict,
    unique_users_28d: Optional[int],
    unique_users_prev_28d: Optional[int],
    cfg: FrequencyMetricsConfig,
) -> tuple[dict, dict, dict, bool, Optional[str], Optional[str]]:
    """
    Apply privacy suppression to distribution fields.

    Suppress condition: unique_users_28d < MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS (and not None).
    Also suppress previous distributions if unique_users_prev_28d < threshold.

    Returns:
        (freq_28d_updated, freq_prev_28d_updated, comparison_updated,
         suppressed_bool, suppression_reason, suppressed_fields_str)
    """
    freq_28d = dict(freq_28d)
    freq_prev_28d = dict(freq_prev_28d)
    comparison = dict(comparison)

    threshold = cfg.MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS

    suppress_28d = (
        unique_users_28d is not None
        and unique_users_28d < threshold
    )
    suppress_prev_28d = (
        unique_users_prev_28d is not None
        and unique_users_prev_28d < threshold
    )

    suppress_comparison_medians = suppress_28d or suppress_prev_28d

    suppressed_field_names: list[str] = []

    if suppress_28d:
        for field in _SUPPRESSIBLE_28D_FIELDS:
            if field in freq_28d:
                freq_28d[field] = None
                suppressed_field_names.append(field)

    if suppress_prev_28d:
        for field in _SUPPRESSIBLE_PREV28D_FIELDS:
            if field in freq_prev_28d:
                freq_prev_28d[field] = None
                if field not in suppressed_field_names:
                    suppressed_field_names.append(field)

    if suppress_comparison_medians:
        for field in _SUPPRESSIBLE_COMPARISON_FIELDS:
            if field in comparison:
                comparison[field] = None
                if field not in suppressed_field_names:
                    suppressed_field_names.append(field)

    # is_suppressed reflects the recent 28d window (primary suppression).
    # Previous-window suppression is applied independently; the row-level flag
    # tracks whether the primary (recent 28d) distributions were suppressed.
    is_suppressed = suppress_28d
    suppression_reason = "unique_users_below_minimum" if (suppress_28d or suppress_prev_28d) else None
    suppressed_fields_str = (
        ",".join(suppressed_field_names) if suppressed_field_names else None
    )

    return (
        freq_28d,
        freq_prev_28d,
        comparison,
        is_suppressed,
        suppression_reason,
        suppressed_fields_str,
    )


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_report_frequency_metrics(
    sufficiency_df: pd.DataFrame,
    mart_df: pd.DataFrame,
    quality_df: pd.DataFrame,
    boundaries_df: pd.DataFrame,
    cfg: FrequencyMetricsConfig,
    analytics_run_id: str,
) -> pd.DataFrame:
    """
    Build one row per report in sufficiency_df with report-level frequency metrics.

    Returns DataFrame sorted by report_id with REPORT_USER_FREQUENCY_METRICS_COLS columns.
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
    mart_parsed = mart_df.copy()
    if not mart_parsed.empty:
        mart_parsed["usage_date"] = pd.to_datetime(
            mart_parsed["usage_date"], errors="coerce"
        ).dt.date

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
        else:
            dq_status = "good"
        has_valid_data = dq_status != "no_valid_user_data"

        # Helper: compute window totals and frequency metrics
        def _compute_window(suf_flag, start, end):
            """Returns (total_views, total_urd, user_df, freq_dict, window_mart_df)."""
            if not suf_flag or not has_valid_data or start is None or end is None:
                return 0, 0, pd.DataFrame(columns=_USER_WINDOW_FREQ_COLS), {
                    "unique_users": None,
                    "views_per_active_user": None,
                    "views_per_user_day": None,
                    "mean_views_per_user": None,
                    "median_views_per_user": None,
                    "p75_views_per_user": None,
                    "p90_views_per_user": None,
                    "max_views_per_user": None,
                    "mean_user_active_days": None,
                    "median_user_active_days": None,
                    "p75_user_active_days": None,
                    "p90_user_active_days": None,
                    "max_user_active_days": None,
                    "mean_views_per_user_day": None,
                    "median_views_per_user_day": None,
                    "p90_views_per_user_day": None,
                }, pd.DataFrame()

            # Filter mart to this report + window
            if mart_parsed.empty:
                mw = pd.DataFrame()
            else:
                mask = (
                    (mart_parsed["report_id"] == rid)
                    & (mart_parsed["usage_date"] >= start)
                    & (mart_parsed["usage_date"] <= end)
                )
                mw = mart_parsed[mask]

            total_views = int(mw["daily_views"].sum()) if not mw.empty else 0
            total_urd = len(mw)  # one row per user-report-day

            user_df = aggregate_user_window_frequency(mart_parsed, rid, start, end)
            freq = calculate_report_frequency_metrics(user_df, total_views, total_urd, cfg)
            return total_views, total_urd, user_df, freq, mw

        # ── 7-day window ──────────────────────────────────────────────────
        tv7, urd7, udf7, freq7, mw7 = _compute_window(suf_7d, w7s, w7e)
        unique_7d = freq7.get("unique_users")
        if unique_7d is None:
            total_views_7d = None
            total_urd_7d = None
            unique_users_7d = None
            views_per_au_7d = None
            views_per_ud_7d = None
            mean_uad_7d = None
            median_uad_7d = None
        else:
            total_views_7d = tv7
            total_urd_7d = urd7
            unique_users_7d = unique_7d
            views_per_au_7d = freq7.get("views_per_active_user")
            views_per_ud_7d = freq7.get("views_per_user_day")
            mean_uad_7d = freq7.get("mean_user_active_days")
            median_uad_7d = freq7.get("median_user_active_days")

        # ── 28-day window ─────────────────────────────────────────────────
        tv28, urd28, udf28, freq28, mw28 = _compute_window(suf_28d, w28s, w28e)
        unique_28d = freq28.get("unique_users")
        if unique_28d is None:
            total_views_28d = None
            total_urd_28d = None
            unique_users_28d = None
            views_per_au_28d = None
            views_per_ud_28d = None
        else:
            total_views_28d = tv28
            total_urd_28d = urd28
            unique_users_28d = unique_28d
            views_per_au_28d = freq28.get("views_per_active_user")
            views_per_ud_28d = freq28.get("views_per_user_day")

        # Return gaps for 28d
        gap28 = calculate_return_gaps(udf28, mw28, rid, w28s or date.min, w28e or date.min, cfg)

        # ── Previous 28-day window ────────────────────────────────────────
        tv_p28, urd_p28, udf_p28, freq_p28, mw_p28 = _compute_window(suf_prev28d, pw28s, pw28e)
        unique_p28d = freq_p28.get("unique_users")
        if unique_p28d is None:
            total_views_p28d = None
            total_urd_p28d = None
            unique_users_p28d = None
            views_per_au_p28d = None
            views_per_ud_p28d = None
        else:
            total_views_p28d = tv_p28
            total_urd_p28d = urd_p28
            unique_users_p28d = unique_p28d
            views_per_au_p28d = freq_p28.get("views_per_active_user")
            views_per_ud_p28d = freq_p28.get("views_per_user_day")

        gap_p28 = calculate_return_gaps(udf_p28, mw_p28, rid, pw28s or date.min, pw28e or date.min, cfg)

        # ── 90-day window ─────────────────────────────────────────────────
        tv90, urd90, udf90, freq90, mw90 = _compute_window(suf_90d, w90s, w90e)
        unique_90d = freq90.get("unique_users")
        if unique_90d is None:
            total_views_90d = None
            total_urd_90d = None
            unique_users_90d = None
            views_per_au_90d = None
            views_per_ud_90d = None
        else:
            total_views_90d = tv90
            total_urd_90d = urd90
            unique_users_90d = unique_90d
            views_per_au_90d = freq90.get("views_per_active_user")
            views_per_ud_90d = freq90.get("views_per_user_day")

        # ── Change metrics ────────────────────────────────────────────────
        recent_dict = {
            "total_views": total_views_28d,
            "total_user_report_days": total_urd_28d,
            "views_per_active_user": views_per_au_28d,
            "views_per_user_day": views_per_ud_28d,
            "median_views_per_user": freq28.get("median_views_per_user"),
            "median_user_active_days": freq28.get("median_user_active_days"),
            "median_return_gap_days": gap28.get("median_return_gap_days"),
        }
        prev_dict = {
            "total_views": total_views_p28d,
            "total_user_report_days": total_urd_p28d,
            "views_per_active_user": views_per_au_p28d,
            "views_per_user_day": views_per_ud_p28d,
            "median_views_per_user": freq_p28.get("median_views_per_user"),
            "median_user_active_days": freq_p28.get("median_user_active_days"),
            "median_return_gap_days": gap_p28.get("median_return_gap_days"),
        }
        change_metrics = calculate_frequency_change_metrics(recent_dict, prev_dict, cmp_28d)

        # ── Privacy suppression ───────────────────────────────────────────
        freq28_for_suppression = {k: freq28.get(k) for k in _SUPPRESSIBLE_28D_FIELDS
                                   if k.replace("_28d", "") in freq28 or k in freq28}
        # Build dict with the actual field names used in suppression
        freq28_supp_input = {
            "mean_views_per_user_28d": freq28.get("mean_views_per_user"),
            "median_views_per_user_28d": freq28.get("median_views_per_user"),
            "p75_views_per_user_28d": freq28.get("p75_views_per_user"),
            "p90_views_per_user_28d": freq28.get("p90_views_per_user"),
            "max_views_per_user_28d": freq28.get("max_views_per_user"),
            "mean_user_active_days_28d": freq28.get("mean_user_active_days"),
            "median_user_active_days_28d": freq28.get("median_user_active_days"),
            "p75_user_active_days_28d": freq28.get("p75_user_active_days"),
            "p90_user_active_days_28d": freq28.get("p90_user_active_days"),
            "max_user_active_days_28d": freq28.get("max_user_active_days"),
            "mean_views_per_user_day_28d": freq28.get("mean_views_per_user_day"),
            "median_views_per_user_day_28d": freq28.get("median_views_per_user_day"),
            "p90_views_per_user_day_28d": freq28.get("p90_views_per_user_day"),
            "mean_return_gap_days_28d": gap28.get("mean_return_gap_days"),
            "median_return_gap_days_28d": gap28.get("median_return_gap_days"),
            "returning_user_gap_observation_count_28d": gap28.get("returning_user_gap_observation_count"),
        }
        freq_prev28_supp_input = {
            "median_views_per_user_previous_28d": freq_p28.get("median_views_per_user"),
            "median_user_active_days_previous_28d": freq_p28.get("median_user_active_days"),
            "median_return_gap_days_previous_28d": gap_p28.get("median_return_gap_days"),
        }
        comparison_supp_input = {
            "median_views_per_user_change_28d": change_metrics.get("median_views_per_user_change_28d"),
            "median_user_active_days_change_28d": change_metrics.get("median_user_active_days_change_28d"),
            "median_return_gap_change_days_28d": change_metrics.get("median_return_gap_change_days_28d"),
        }

        (
            freq28_suppressed,
            freq_prev28_suppressed,
            comparison_suppressed,
            is_suppressed,
            suppression_reason,
            suppressed_fields_str,
        ) = apply_frequency_privacy_suppression(
            freq28_supp_input,
            freq_prev28_supp_input,
            comparison_supp_input,
            unique_users_28d,
            unique_users_p28d,
            cfg,
        )

        # ── Direction and status ──────────────────────────────────────────
        direction = classify_frequency_direction(
            recent_total_views=total_views_28d,
            previous_total_views=total_views_p28d,
            comparison_sufficient=cmp_28d,
            has_valid_data=has_valid_data,
            cfg=cfg,
        )

        # Pass _history_sufficient_7d through metrics dict for evidence status
        freq28_for_status = dict(freq28)
        freq28_for_status["_history_sufficient_7d"] = suf_7d
        freq28_for_status["unique_users"] = unique_users_28d or 0
        freq28_for_status["median_user_active_days"] = freq28_suppressed.get("median_user_active_days_28d")
        freq28_for_status["median_views_per_user_day"] = freq28_suppressed.get("median_views_per_user_day_28d")

        status, evidence_status, reasons = classify_frequency_status(
            metrics_28d=freq28_for_status,
            comparison_metrics={
                "median_user_active_days_change_28d": comparison_suppressed.get("median_user_active_days_change_28d"),
            },
            direction=direction,
            history_sufficient=suf_28d,
            has_valid_data=has_valid_data,
            is_suppressed=is_suppressed,
            cfg=cfg,
        )

        privacy_suppression_status = "suppressed" if is_suppressed else "not_suppressed"

        # ── Build output row ──────────────────────────────────────────────
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
            "privacy_suppression_status": privacy_suppression_status,
            # 7d
            "total_views_7d": total_views_7d,
            "total_user_report_days_7d": total_urd_7d,
            "unique_users_7d": unique_users_7d,
            "views_per_active_user_7d": views_per_au_7d,
            "views_per_user_day_7d": views_per_ud_7d,
            "mean_user_active_days_7d": mean_uad_7d,
            "median_user_active_days_7d": median_uad_7d,
            # 28d counts and ratios
            "total_views_28d": total_views_28d,
            "total_user_report_days_28d": total_urd_28d,
            "unique_users_28d": unique_users_28d,
            "views_per_active_user_28d": views_per_au_28d,
            "views_per_user_day_28d": views_per_ud_28d,
            # 28d distributions (may be suppressed)
            "mean_views_per_user_28d": freq28_suppressed.get("mean_views_per_user_28d"),
            "median_views_per_user_28d": freq28_suppressed.get("median_views_per_user_28d"),
            "p75_views_per_user_28d": freq28_suppressed.get("p75_views_per_user_28d"),
            "p90_views_per_user_28d": freq28_suppressed.get("p90_views_per_user_28d"),
            "max_views_per_user_28d": freq28_suppressed.get("max_views_per_user_28d"),
            "mean_user_active_days_28d": freq28_suppressed.get("mean_user_active_days_28d"),
            "median_user_active_days_28d": freq28_suppressed.get("median_user_active_days_28d"),
            "p75_user_active_days_28d": freq28_suppressed.get("p75_user_active_days_28d"),
            "p90_user_active_days_28d": freq28_suppressed.get("p90_user_active_days_28d"),
            "max_user_active_days_28d": freq28_suppressed.get("max_user_active_days_28d"),
            "mean_views_per_user_day_28d": freq28_suppressed.get("mean_views_per_user_day_28d"),
            "median_views_per_user_day_28d": freq28_suppressed.get("median_views_per_user_day_28d"),
            "p90_views_per_user_day_28d": freq28_suppressed.get("p90_views_per_user_day_28d"),
            "mean_return_gap_days_28d": freq28_suppressed.get("mean_return_gap_days_28d"),
            "median_return_gap_days_28d": freq28_suppressed.get("median_return_gap_days_28d"),
            "returning_user_gap_observation_count_28d": freq28_suppressed.get("returning_user_gap_observation_count_28d"),
            # Previous 28d
            "total_views_previous_28d": total_views_p28d,
            "total_user_report_days_previous_28d": total_urd_p28d,
            "unique_users_previous_28d": unique_users_p28d,
            "views_per_active_user_previous_28d": views_per_au_p28d,
            "views_per_user_day_previous_28d": views_per_ud_p28d,
            "median_views_per_user_previous_28d": freq_prev28_suppressed.get("median_views_per_user_previous_28d"),
            "median_user_active_days_previous_28d": freq_prev28_suppressed.get("median_user_active_days_previous_28d"),
            "median_return_gap_days_previous_28d": freq_prev28_suppressed.get("median_return_gap_days_previous_28d"),
            # 90d
            "total_views_90d": total_views_90d,
            "total_user_report_days_90d": total_urd_90d,
            "unique_users_90d": unique_users_90d,
            "views_per_active_user_90d": views_per_au_90d,
            "views_per_user_day_90d": views_per_ud_90d,
            "median_views_per_user_90d": freq90.get("median_views_per_user"),
            "median_user_active_days_90d": freq90.get("median_user_active_days"),
            # Comparison 28d
            "total_views_change_28d": change_metrics.get("total_views_change_28d"),
            "total_views_change_28d_pct": change_metrics.get("total_views_change_28d_pct"),
            "user_report_days_change_28d": change_metrics.get("user_report_days_change_28d"),
            "user_report_days_change_28d_pct": change_metrics.get("user_report_days_change_28d_pct"),
            "views_per_active_user_change_28d": change_metrics.get("views_per_active_user_change_28d"),
            "views_per_active_user_change_28d_pct": change_metrics.get("views_per_active_user_change_28d_pct"),
            "views_per_user_day_change_28d": change_metrics.get("views_per_user_day_change_28d"),
            "views_per_user_day_change_28d_pct": change_metrics.get("views_per_user_day_change_28d_pct"),
            "median_views_per_user_change_28d": comparison_suppressed.get("median_views_per_user_change_28d"),
            "median_user_active_days_change_28d": comparison_suppressed.get("median_user_active_days_change_28d"),
            "median_return_gap_change_days_28d": comparison_suppressed.get("median_return_gap_change_days_28d"),
            # Status
            "frequency_direction": direction,
            "frequency_status": status,
            "frequency_evidence_status": evidence_status,
            "frequency_reasons": "|".join(reasons) if reasons else "none",
            # Privacy
            "frequency_privacy_suppressed": is_suppressed,
            "frequency_privacy_suppression_reason": suppression_reason,
            "suppressed_frequency_fields": suppressed_fields_str,
        }
        rows.append(row)

    result = pd.DataFrame(rows, columns=REPORT_USER_FREQUENCY_METRICS_COLS)
    return result


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_report_frequency_metrics(
    df: pd.DataFrame,
    boundaries_df: Optional[pd.DataFrame] = None,
    cfg: FrequencyMetricsConfig = FrequencyMetricsConfig(),
) -> None:
    """Validate the report frequency metrics DataFrame. Raises ValueError on failure."""

    # 1. Required columns present
    missing = [c for c in REPORT_USER_FREQUENCY_METRICS_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in frequency metrics output: {missing}")

    if df.empty:
        return

    # 2. Unique (analytics_run_id, report_id) grain
    dupes = df.duplicated(subset=["analytics_run_id", "report_id"])
    if dupes.any():
        raise ValueError(
            f"Duplicate (analytics_run_id, report_id) rows: {df.loc[dupes, 'report_id'].tolist()}"
        )

    # 3. total_views >= 0 where not null
    for col in ["total_views_7d", "total_views_28d", "total_views_previous_28d", "total_views_90d"]:
        if col in df.columns:
            bad = df[df[col].notna() & (df[col] < 0)]
            if not bad.empty:
                raise ValueError(f"Negative values in {col}: {bad['report_id'].tolist()}")

    # 4. total_user_report_days >= unique_users when unique > 0
    for urd_col, uniq_col in [
        ("total_user_report_days_28d", "unique_users_28d"),
        ("total_user_report_days_previous_28d", "unique_users_previous_28d"),
        ("total_user_report_days_90d", "unique_users_90d"),
    ]:
        if urd_col in df.columns and uniq_col in df.columns:
            both = df[df[urd_col].notna() & df[uniq_col].notna() & (df[uniq_col] > 0)]
            bad = both[both[urd_col] < both[uniq_col]]
            if not bad.empty:
                raise ValueError(
                    f"{urd_col} < {uniq_col} for: {bad['report_id'].tolist()}"
                )

    # 5. views_per_active_user = total_views / unique_users (approximately ±0.01)
    for tv_col, uniq_col, vpau_col in [
        ("total_views_28d", "unique_users_28d", "views_per_active_user_28d"),
    ]:
        if all(c in df.columns for c in [tv_col, uniq_col, vpau_col]):
            mask = df[tv_col].notna() & df[uniq_col].notna() & df[vpau_col].notna() & (df[uniq_col] > 0)
            sub = df[mask]
            expected = sub[tv_col] / sub[uniq_col]
            bad = sub[(sub[vpau_col] - expected).abs() > 0.01]
            if not bad.empty:
                raise ValueError(
                    f"{vpau_col} does not match {tv_col}/{uniq_col} for: {bad['report_id'].tolist()}"
                )

    # 6. views_per_user_day = total_views / total_user_report_days (approximately)
    for tv_col, urd_col, vpud_col in [
        ("total_views_28d", "total_user_report_days_28d", "views_per_user_day_28d"),
    ]:
        if all(c in df.columns for c in [tv_col, urd_col, vpud_col]):
            mask = df[tv_col].notna() & df[urd_col].notna() & df[vpud_col].notna() & (df[urd_col] > 0)
            sub = df[mask]
            expected = sub[tv_col] / sub[urd_col]
            bad = sub[(sub[vpud_col] - expected).abs() > 0.01]
            if not bad.empty:
                raise ValueError(
                    f"{vpud_col} does not match {tv_col}/{urd_col} for: {bad['report_id'].tolist()}"
                )

    # 7. Percentile ordering: median <= p75 <= p90 <= max for views distributions
    for med, p75, p90, mx in [
        ("median_views_per_user_28d", "p75_views_per_user_28d", "p90_views_per_user_28d", "max_views_per_user_28d"),
        ("median_user_active_days_28d", "p75_user_active_days_28d", "p90_user_active_days_28d", "max_user_active_days_28d"),
    ]:
        if all(c in df.columns for c in [med, p75, p90, mx]):
            mask = df[[med, p75, p90, mx]].notna().all(axis=1)
            sub = df[mask]
            bad = sub[(sub[med] > sub[p75]) | (sub[p75] > sub[p90]) | (sub[p90] > sub[mx])]
            if not bad.empty:
                raise ValueError(
                    f"Percentile ordering violated ({med} <= {p75} <= {p90} <= {mx}) "
                    f"for: {bad['report_id'].tolist()}"
                )

    # 8. p90_user_active_days <= 28 (window length)
    if "p90_user_active_days_28d" in df.columns:
        bad = df[df["p90_user_active_days_28d"].notna() & (df["p90_user_active_days_28d"] > 28)]
        if not bad.empty:
            raise ValueError(
                f"p90_user_active_days_28d > 28 for: {bad['report_id'].tolist()}"
            )

    # 9. change_pct null when previous denominator = 0
    for prev_col, pct_col in [
        ("total_views_previous_28d", "total_views_change_28d_pct"),
        ("total_user_report_days_previous_28d", "user_report_days_change_28d_pct"),
        ("views_per_active_user_previous_28d", "views_per_active_user_change_28d_pct"),
        ("views_per_user_day_previous_28d", "views_per_user_day_change_28d_pct"),
    ]:
        if prev_col in df.columns and pct_col in df.columns:
            zero_prev = df[df[prev_col].notna() & (df[prev_col] == 0)]
            bad = zero_prev[zero_prev[pct_col].notna()]
            if not bad.empty:
                raise ValueError(
                    f"{pct_col} must be null when {prev_col}=0 for: {bad['report_id'].tolist()}"
                )

    # 10. Suppressed distribution fields are null when suppressed
    if "frequency_privacy_suppressed" in df.columns:
        suppressed = df[df["frequency_privacy_suppressed"] == True]
        if not suppressed.empty:
            for col in _SUPPRESSIBLE_28D_FIELDS:
                if col in df.columns:
                    bad = suppressed[suppressed[col].notna()]
                    if not bad.empty:
                        raise ValueError(
                            f"Suppressed field {col} must be null for: {bad['report_id'].tolist()}"
                        )

    # 11. Status in allowed set
    if "frequency_status" in df.columns:
        bad = df[df["frequency_status"].notna() & ~df["frequency_status"].isin(_VALID_FREQUENCY_STATUSES)]
        if not bad.empty:
            raise ValueError(f"Invalid frequency_status values: {bad['frequency_status'].unique().tolist()}")

    # 12. Direction in allowed set
    if "frequency_direction" in df.columns:
        bad = df[df["frequency_direction"].notna() & ~df["frequency_direction"].isin(_VALID_FREQUENCY_DIRECTIONS)]
        if not bad.empty:
            raise ValueError(f"Invalid frequency_direction values: {bad['frequency_direction'].unique().tolist()}")

    # 13. No direct identifiers
    validate_no_direct_identifiers(df, context="report_user_frequency_metrics")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_report_frequency_metrics(
    df: pd.DataFrame,
    project_root: Path,
) -> Path:
    """Validate and write to outputs/analytics/report_user_frequency_metrics.csv."""
    validate_report_frequency_metrics(df)
    output_dir = project_root / "outputs" / "analytics"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "report_user_frequency_metrics.csv"
    df_sorted = df.sort_values("report_id").reset_index(drop=True)
    df_sorted.to_csv(out_path, index=False)
    return out_path
