"""Evidence-aware historical performance and lifecycle feature builder.

Schema version 2.0.0 — replaces the legacy window-only v1 schema.

Key design principles
---------------------
* analytics_as_of_date is always source-derived (never datetime.now()).
* Every metric carries an evidence-sufficiency flag or status so downstream
  consumers never need to guess whether a null means "zero" or "unknown".
* User-level columns (user_key, user_id, etc.) are never in the output.
* All window boundaries follow the engagement_windows convention:
    7d  : [as_of - 6d, as_of]          (7 days inclusive)
    28d : [as_of - 27d, as_of]         (28 days inclusive)
    prev28d: [as_of - 55d, as_of - 28d] (28 days inclusive, no overlap)
    90d : [as_of - 89d, as_of]         (90 days inclusive)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from src.analytics.privacy_policy import PROHIBITED_OUTPUT_COLUMNS

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

REPORT_FEATURES_SCHEMA_VERSION = "2.0.0"

REPORT_FEATURES_COLS = [
    # Identity and lineage
    "analytics_run_id", "generated_at", "analytics_as_of_date",
    "source_max_usage_date", "analytics_timezone", "as_of_date_policy",
    "schema_version", "report_id", "report_name", "workspace_id",
    # Lifecycle
    "report_activation_date", "report_age_days", "first_observed_usage_date",
    "latest_observed_usage_date", "days_since_first_observed_use",
    "days_since_last_use", "adoption_maturity_status", "report_lifecycle_status",
    "activation_date_status",
    # Evidence
    "available_calendar_history_days", "observed_calendar_days",
    "active_usage_days_lifetime", "history_sufficient_7d",
    "history_sufficient_28d", "history_sufficient_previous_28d",
    "comparison_history_sufficient_28d", "history_sufficient_90d",
    "usage_evidence_status", "usage_evidence_reasons",
    # Usage totals
    "lifetime_views", "recent_7d_views", "recent_28d_views",
    "previous_28d_views", "recent_90d_views", "average_daily_views_28d",
    "average_daily_views_previous_28d", "active_usage_days_28d",
    "active_usage_days_previous_28d", "usage_day_rate_28d",
    # Change
    "usage_change_28d", "usage_change_28d_pct", "average_daily_usage_change_28d",
    "average_daily_usage_change_28d_pct", "active_usage_days_change_28d",
    "usage_direction_28d", "usage_change_materiality",
    # Trend
    "usage_trend_slope_28d", "usage_trend_slope_90d",
    "usage_trend_strength_28d", "usage_trend_status", "trend_evidence_status",
    # Volatility
    "usage_mean_daily_28d", "usage_std_daily_28d", "usage_cv_28d",
    "usage_median_daily_28d", "usage_p90_daily_28d", "usage_max_daily_28d",
    "usage_volatility_status", "usage_consistency_status",
    # Inactivity
    "current_zero_usage_streak_days", "longest_zero_usage_streak_90d",
    "zero_usage_days_28d", "zero_usage_share_28d",
    "inactive_7d", "inactive_28d", "inactivity_status",
    # Peaks and anomalies
    "peak_usage_date_90d", "peak_daily_views_90d",
    "recent_peak_usage_date_28d", "recent_peak_daily_views_28d",
    "usage_anomaly_count_28d", "latest_usage_anomaly_status",
    "anomaly_evidence_status",
    # Summary
    "historical_usage_status", "primary_historical_usage_issue",
    "historical_usage_issue_count", "historical_usage_reasons",
    "historical_usage_review_required",
]

# Deprecated columns that must not appear in v2 outputs
_DEPRECATED_COLS = frozenset({
    "latest_views", "prior_views", "usage_change_pct", "top_user_concentration",
    "repeat_rate", "avg_views", "total_views", "days_active", "unique_users",
    "trend_history_sufficient", "usage_trend_12w_slope", "newly_active_flag",
    "usage_change_28d_pct_legacy",
})


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ReportFeaturesConfig:
    LATEST_SOURCE_DATE_IS_COMPLETE: bool = True
    ANALYTICS_TIMEZONE: str = "UTC"
    # Lifecycle thresholds
    NEWLY_LAUNCHED_MAX_DAYS: int = 13        # < 14 observable days
    MATURING_MAX_DAYS: int = 27              # 14–27
    # mature: >= 28 days
    # Usage direction thresholds
    MIN_ABSOLUTE_VIEW_CHANGE: int = 5
    USAGE_GROWTH_PCT_THRESHOLD: float = 0.10
    USAGE_DECLINE_PCT_THRESHOLD: float = 0.10
    MIN_PREVIOUS_VIEWS_FOR_PCT_DIRECTION: int = 5
    # Trend thresholds
    TREND_STRONG_THRESHOLD: float = 2.0
    TREND_MODERATE_THRESHOLD: float = 0.5
    MIN_NON_ZERO_DAYS_FOR_TREND: int = 5
    # Volatility
    CV_HIGH_THRESHOLD: float = 1.5
    CV_MODERATE_THRESHOLD: float = 0.75
    BURSTY_ZERO_SHARE_MIN: float = 0.50
    BURSTY_PEAK_TO_MEDIAN_MIN: float = 5.0
    # Anomaly
    ANOMALY_IQR_MULTIPLIER: float = 3.0
    MIN_DAYS_FOR_ANOMALY_DETECTION: int = 14
    # Inactivity
    PROLONGED_INACTIVITY_DAYS: int = 28
    RECENT_INACTIVITY_DAYS: int = 7
    # Evidence sufficiency
    HISTORY_SUFFICIENT_7D_MIN_DAYS: int = 7
    HISTORY_SUFFICIENT_28D_MIN_DAYS: int = 28
    HISTORY_SUFFICIENT_90D_MIN_DAYS: int = 90
    SCHEMA_VERSION: str = "2.0.0"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_date(val) -> Optional[date]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, date):
        return val
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def _build_report_day_spine(
    report_id: str,
    start_date: date,
    end_date: date,
    daily_views_series: pd.Series,  # index is date, values are view counts
) -> pd.Series:
    """Return a complete daily series from start_date to end_date, zeros filled."""
    if start_date > end_date:
        return pd.Series(dtype=float)
    idx = pd.date_range(start=start_date, end=end_date, freq="D").date
    spine = pd.Series(0.0, index=idx)
    if not daily_views_series.empty:
        # Align: only dates that fall inside [start_date, end_date]
        mask = (daily_views_series.index >= start_date) & (daily_views_series.index <= end_date)
        valid = daily_views_series[mask]
        for d, v in valid.items():
            if d in spine.index:
                spine[d] = float(v)
    return spine


def _resolve_report_window_start(
    activation_date: Optional[date],
    source_coverage_start: date,
    window_start: date,
) -> date:
    """Return the effective start for a report's window."""
    candidates = [window_start, source_coverage_start]
    if activation_date is not None:
        candidates.append(activation_date)
    return max(candidates)


def _compute_lifecycle_fields(
    launch_date_raw,
    first_usage_date: Optional[date],
    latest_usage_date: Optional[date],
    analytics_as_of_date: date,
    cfg: ReportFeaturesConfig,
    inactive_28d: bool,
    history_sufficient_28d: bool,
    days_since_last_use: Optional[int],
) -> dict:
    activation_date = _safe_date(launch_date_raw)

    # activation_date_status
    if activation_date is not None:
        activation_date_status = "known"
    elif first_usage_date is not None:
        activation_date_status = "inferred_from_source_coverage"
        activation_date = first_usage_date
    else:
        activation_date_status = "unavailable"

    # report_age_days
    if activation_date is not None:
        report_age_days = max(0, (analytics_as_of_date - activation_date).days)
    else:
        report_age_days = None

    # days_since_first_observed_use
    days_since_first_observed_use = None
    if first_usage_date is not None:
        days_since_first_observed_use = max(0, (analytics_as_of_date - first_usage_date).days)

    # available_calendar_history_days (based on activation)
    available_calendar_history_days = 0
    if activation_date is not None and activation_date <= analytics_as_of_date:
        available_calendar_history_days = max(0, (analytics_as_of_date - activation_date).days + 1)

    # adoption_maturity_status
    if available_calendar_history_days < 14:
        adoption_maturity_status = "newly_launched"
    elif available_calendar_history_days < 28:
        adoption_maturity_status = "maturing"
    else:
        adoption_maturity_status = "mature"

    # report_lifecycle_status
    if activation_date is not None and activation_date > analytics_as_of_date:
        report_lifecycle_status = "pre_activation"
    elif report_age_days is not None and report_age_days <= 13:
        report_lifecycle_status = "newly_launched"
    elif report_age_days is not None and report_age_days <= 27:
        report_lifecycle_status = "maturing"
    elif report_age_days is not None and report_age_days >= 28:
        # check dormant
        if (
            inactive_28d
            and history_sufficient_28d
            and days_since_last_use is not None
            and days_since_last_use >= 28
        ):
            report_lifecycle_status = "dormant"
        else:
            report_lifecycle_status = "established"
    elif activation_date is None and first_usage_date is None:
        report_lifecycle_status = "unknown"
    else:
        report_lifecycle_status = "unknown"

    return {
        "report_activation_date": str(activation_date) if (activation_date is not None and _safe_date(launch_date_raw) is not None) else None,
        "report_age_days": report_age_days,
        "first_observed_usage_date": str(first_usage_date) if first_usage_date else None,
        "latest_observed_usage_date": str(latest_usage_date) if latest_usage_date else None,
        "days_since_first_observed_use": days_since_first_observed_use,
        "adoption_maturity_status": adoption_maturity_status,
        "report_lifecycle_status": report_lifecycle_status,
        "activation_date_status": activation_date_status,
        "available_calendar_history_days": available_calendar_history_days,
    }


def _compute_history_sufficiency(
    available_days: int,
) -> dict:
    suf_7d = available_days >= 7
    suf_28d = available_days >= 28
    suf_prev28d = available_days >= 56
    suf_90d = available_days >= 90
    comparison_28d = suf_28d and suf_prev28d

    return {
        "history_sufficient_7d": suf_7d,
        "history_sufficient_28d": suf_28d,
        "history_sufficient_previous_28d": suf_prev28d,
        "comparison_history_sufficient_28d": comparison_28d,
        "history_sufficient_90d": suf_90d,
    }


def _compute_usage_totals(
    spine_28d: pd.Series,
    spine_prev28d: pd.Series,
    spine_7d: pd.Series,
    spine_90d: pd.Series,
    spine_lifetime: pd.Series,
    history_sufficient_28d: bool,
    comparison_sufficient: bool,
    history_sufficient_90d: bool,
    history_sufficient_7d: bool,
) -> dict:
    lifetime_views = float(spine_lifetime.sum()) if not spine_lifetime.empty else 0.0

    recent_7d_views = float(spine_7d.sum()) if (history_sufficient_7d and not spine_7d.empty) else (float(spine_7d.sum()) if not spine_7d.empty else 0.0)
    recent_28d_views = float(spine_28d.sum()) if not spine_28d.empty else 0.0
    recent_90d_views = float(spine_90d.sum()) if (history_sufficient_90d and not spine_90d.empty) else (np.nan if not history_sufficient_90d else 0.0)
    previous_28d_views = float(spine_prev28d.sum()) if (comparison_sufficient and not spine_prev28d.empty) else np.nan

    average_daily_views_28d = (recent_28d_views / 28.0) if history_sufficient_28d else np.nan
    average_daily_views_previous_28d = (float(spine_prev28d.sum()) / 28.0) if comparison_sufficient else np.nan
    active_usage_days_28d = int((spine_28d > 0).sum()) if history_sufficient_28d else None
    active_usage_days_previous_28d = int((spine_prev28d > 0).sum()) if comparison_sufficient else None
    usage_day_rate_28d = (active_usage_days_28d / 28.0) if history_sufficient_28d and active_usage_days_28d is not None else np.nan

    return {
        "lifetime_views": lifetime_views,
        "recent_7d_views": recent_7d_views,
        "recent_28d_views": recent_28d_views,
        "previous_28d_views": previous_28d_views,
        "recent_90d_views": recent_90d_views,
        "average_daily_views_28d": average_daily_views_28d,
        "average_daily_views_previous_28d": average_daily_views_previous_28d,
        "active_usage_days_28d": active_usage_days_28d,
        "active_usage_days_previous_28d": active_usage_days_previous_28d,
        "usage_day_rate_28d": usage_day_rate_28d,
    }


def _compute_usage_change(
    recent_views: float,
    previous_views,  # may be nan
    active_days_recent,  # may be None
    active_days_prev,  # may be None
    comparison_sufficient: bool,
    cfg: ReportFeaturesConfig,
) -> dict:
    if not comparison_sufficient or pd.isna(previous_views):
        return {
            "usage_change_28d": np.nan,
            "usage_change_28d_pct": np.nan,
            "average_daily_usage_change_28d": np.nan,
            "average_daily_usage_change_28d_pct": np.nan,
            "active_usage_days_change_28d": np.nan,
            "usage_direction_28d": "insufficient_history",
            "usage_change_materiality": "insufficient_history",
        }

    change = recent_views - previous_views
    pct_change = (change / previous_views) if previous_views > 0 else np.nan
    avg_daily_change = change / 28.0
    avg_daily_pct_change = (pct_change / 28.0) if not pd.isna(pct_change) else np.nan
    active_days_change = (
        (active_days_recent - active_days_prev)
        if (active_days_recent is not None and active_days_prev is not None)
        else np.nan
    )

    # Direction
    if recent_views == 0 and previous_views == 0:
        direction = "inactive"
    elif previous_views == 0 and recent_views > 0:
        direction = "newly_active"
    elif recent_views == 0 and previous_views > 0:
        direction = "declining"
    elif abs(change) < cfg.MIN_ABSOLUTE_VIEW_CHANGE:
        direction = "stable"
    elif not pd.isna(pct_change) and pct_change >= cfg.USAGE_GROWTH_PCT_THRESHOLD:
        direction = "growing"
    elif not pd.isna(pct_change) and pct_change <= -cfg.USAGE_DECLINE_PCT_THRESHOLD:
        direction = "declining"
    else:
        direction = "stable"

    # Materiality
    if direction in ("inactive", "newly_active"):
        materiality = "no_previous_data" if previous_views == 0 else "material"
    elif abs(change) < cfg.MIN_ABSOLUTE_VIEW_CHANGE:
        materiality = "immaterial"
    elif direction in ("growing", "declining"):
        materiality = "material"
    else:
        materiality = "immaterial"

    return {
        "usage_change_28d": change,
        "usage_change_28d_pct": pct_change,
        "average_daily_usage_change_28d": avg_daily_change,
        "average_daily_usage_change_28d_pct": avg_daily_pct_change,
        "active_usage_days_change_28d": active_days_change,
        "usage_direction_28d": direction,
        "usage_change_materiality": materiality,
    }


def _compute_trend(
    spine: pd.Series,
    cfg: ReportFeaturesConfig,
) -> Tuple[float, float, str, str]:
    """Returns (slope, strength, trend_status, evidence_status)."""
    if spine.empty or len(spine) == 0:
        return (np.nan, np.nan, "insufficient_history", "insufficient")

    n = len(spine)
    non_zero_days = int((spine > 0).sum())
    max_val = float(spine.max())

    # Inactive
    if max_val == 0:
        return (0.0, 0.0, "inactive", "sufficient")

    # Insufficient activity check
    if non_zero_days < cfg.MIN_NON_ZERO_DAYS_FOR_TREND and max_val < 2:
        return (np.nan, np.nan, "insufficient_history", "insufficient")

    if non_zero_days < cfg.MIN_NON_ZERO_DAYS_FOR_TREND:
        return (np.nan, np.nan, "insufficient_activity", "partial")

    # OLS
    x = np.arange(n, dtype=float)
    y = spine.values.astype(float)
    try:
        coeffs = np.polyfit(x, y, 1)
        slope = float(coeffs[0])
    except Exception:
        return (np.nan, np.nan, "insufficient_history", "insufficient")

    mean_val = float(np.mean(y))
    if mean_val > 0:
        strength = slope * n / mean_val
    else:
        strength = 0.0

    # Status
    if strength >= cfg.TREND_STRONG_THRESHOLD:
        status = "strongly_increasing"
    elif strength >= cfg.TREND_MODERATE_THRESHOLD:
        status = "increasing"
    elif strength <= -cfg.TREND_STRONG_THRESHOLD:
        status = "strongly_decreasing"
    elif strength <= -cfg.TREND_MODERATE_THRESHOLD:
        status = "decreasing"
    else:
        status = "stable"

    evidence_status = "sufficient" if non_zero_days >= cfg.MIN_NON_ZERO_DAYS_FOR_TREND * 2 else "partial"

    return (slope, strength, status, evidence_status)


def _compute_volatility(
    spine_28d: pd.Series,
    history_sufficient: bool,
    usage_day_rate_28d: float,
    cfg: ReportFeaturesConfig,
) -> dict:
    if not history_sufficient or spine_28d.empty:
        return {
            "usage_mean_daily_28d": np.nan,
            "usage_std_daily_28d": np.nan,
            "usage_cv_28d": np.nan,
            "usage_median_daily_28d": np.nan,
            "usage_p90_daily_28d": np.nan,
            "usage_max_daily_28d": np.nan,
            "usage_volatility_status": "insufficient_history",
            "usage_consistency_status": "insufficient_history",
        }

    vals = spine_28d.values.astype(float)
    mean_val = float(np.mean(vals))
    std_val = float(np.std(vals, ddof=0))
    median_val = float(np.median(vals))
    p90_val = float(np.percentile(vals, 90))
    max_val = float(np.max(vals))
    cv = (std_val / mean_val) if mean_val > 0 else np.nan
    zero_share = float((vals == 0).sum()) / len(vals)

    # Volatility status
    if max_val == 0:
        volatility_status = "inactive"
        consistency_status = "inactive"
    else:
        # bursty: high zero share AND extreme peak vs median
        is_bursty = (
            zero_share >= cfg.BURSTY_ZERO_SHARE_MIN
            and (median_val == 0 or (median_val > 0 and p90_val / median_val >= cfg.BURSTY_PEAK_TO_MEDIAN_MIN))
        )
        if is_bursty:
            volatility_status = "bursty"
        elif not pd.isna(cv) and cv >= cfg.CV_HIGH_THRESHOLD:
            volatility_status = "high_volatility"
        elif not pd.isna(cv) and cv >= cfg.CV_MODERATE_THRESHOLD:
            volatility_status = "moderate_volatility"
        else:
            volatility_status = "low_volatility"

        # Consistency
        if is_bursty:
            consistency_status = "bursty"
        elif usage_day_rate_28d >= 0.7 and (pd.isna(cv) or cv < cfg.CV_MODERATE_THRESHOLD):
            consistency_status = "regular"
        elif usage_day_rate_28d >= 0.1:
            consistency_status = "intermittent"
        else:
            consistency_status = "inactive"

    return {
        "usage_mean_daily_28d": mean_val,
        "usage_std_daily_28d": std_val,
        "usage_cv_28d": cv,
        "usage_median_daily_28d": median_val,
        "usage_p90_daily_28d": p90_val,
        "usage_max_daily_28d": max_val,
        "usage_volatility_status": volatility_status,
        "usage_consistency_status": consistency_status,
    }


def _compute_zero_streak(spine_series: pd.Series) -> Tuple[int, Optional[int]]:
    """Returns (current_zero_streak, longest_zero_streak)."""
    if spine_series.empty:
        return (0, None)

    vals = spine_series.values.astype(float)
    n = len(vals)

    # Current zero streak from end
    current = 0
    for i in range(n - 1, -1, -1):
        if vals[i] == 0:
            current += 1
        else:
            break

    # Longest zero streak anywhere
    longest = 0
    run = 0
    for v in vals:
        if v == 0:
            run += 1
            longest = max(longest, run)
        else:
            run = 0

    return (current, longest)


def _compute_peaks_and_anomalies(
    spine_28d: pd.Series,
    spine_90d: pd.Series,
    history_28d: bool,
    history_90d: bool,
    analytics_as_of_date: date,
    cfg: ReportFeaturesConfig,
) -> dict:
    result = {
        "peak_usage_date_90d": None,
        "peak_daily_views_90d": np.nan,
        "recent_peak_usage_date_28d": None,
        "recent_peak_daily_views_28d": np.nan,
        "usage_anomaly_count_28d": 0,
        "latest_usage_anomaly_status": "insufficient_history",
        "anomaly_evidence_status": "insufficient",
    }

    # 90d peak
    if history_90d and not spine_90d.empty and float(spine_90d.max()) > 0:
        peak_val_90d = float(spine_90d.max())
        # Use earliest date on ties
        peak_dates_90d = [d for d, v in spine_90d.items() if v == peak_val_90d]
        peak_date_90d = min(peak_dates_90d)
        result["peak_usage_date_90d"] = str(peak_date_90d)
        result["peak_daily_views_90d"] = peak_val_90d

    # 28d peak
    if history_28d and not spine_28d.empty and float(spine_28d.max()) > 0:
        peak_val_28d = float(spine_28d.max())
        peak_dates_28d = [d for d, v in spine_28d.items() if v == peak_val_28d]
        peak_date_28d = min(peak_dates_28d)
        result["recent_peak_usage_date_28d"] = str(peak_date_28d)
        result["recent_peak_daily_views_28d"] = peak_val_28d

    # Anomalies
    if not history_28d or spine_28d.empty or len(spine_28d) < cfg.MIN_DAYS_FOR_ANOMALY_DETECTION:
        return result

    result["anomaly_evidence_status"] = "sufficient"

    vals = spine_28d.values.astype(float)
    q1 = float(np.percentile(vals, 25))
    q3 = float(np.percentile(vals, 75))
    iqr = q3 - q1
    high_threshold = q3 + cfg.ANOMALY_IQR_MULTIPLIER * iqr

    anomaly_dates = []
    for d, v in spine_28d.items():
        if v > high_threshold and v > 0:
            anomaly_dates.append(d)

    result["usage_anomaly_count_28d"] = len(anomaly_dates)

    # Check last 7 days for anomaly
    window_7d_start = analytics_as_of_date - timedelta(days=6)
    recent_anomalies = [d for d in anomaly_dates if d >= window_7d_start]

    max_val = float(spine_28d.max())
    if max_val == 0:
        result["latest_usage_anomaly_status"] = "inactive"
    elif recent_anomalies:
        result["latest_usage_anomaly_status"] = "high_usage_anomaly"
    else:
        result["latest_usage_anomaly_status"] = "normal"

    return result


def _determine_historical_status(row_dict: dict, cfg: ReportFeaturesConfig) -> Tuple[str, str, int, str]:
    """Returns (historical_usage_status, primary_issue, issue_count, reasons)."""
    reasons = []

    usage_evidence_status = row_dict.get("usage_evidence_status", "no_valid_data")
    history_28d = row_dict.get("history_sufficient_28d", False)
    inactive_28d = row_dict.get("inactive_28d", False)
    direction = row_dict.get("usage_direction_28d", "insufficient_history")
    materiality = row_dict.get("usage_change_materiality", "insufficient_history")
    volatility = row_dict.get("usage_volatility_status", "insufficient_history")
    consistency = row_dict.get("usage_consistency_status", "insufficient_history")
    anomaly_count = row_dict.get("usage_anomaly_count_28d", 0) or 0
    current_streak = row_dict.get("current_zero_usage_streak_days", 0) or 0

    # Build reason sentences
    if usage_evidence_status == "no_valid_data":
        reasons.append("No valid usage data found for this report.")
        return (
            "no_valid_usage_data",
            "invalid_usage_data",
            1,
            "|".join(reasons),
        )

    if not history_28d:
        reasons.append("Insufficient history for 28-day window analysis.")
        return (
            "insufficient_history",
            "insufficient_history",
            1,
            "|".join(reasons),
        )

    # Collect issues
    issues = []

    if current_streak >= cfg.PROLONGED_INACTIVITY_DAYS:
        issues.append("prolonged_inactivity")
        reasons.append(f"No usage for {current_streak} consecutive days (prolonged inactivity).")

    if inactive_28d:
        if "prolonged_inactivity" not in issues:
            issues.append("recent_inactivity")
            reasons.append("No usage observed in the most recent 28-day window.")

    if direction == "declining" and materiality == "material":
        issues.append("usage_decline")
        reasons.append("Usage is declining materially over the 28-day comparison window.")

    if anomaly_count >= 2:
        issues.append("anomaly_detected")
        reasons.append(f"{anomaly_count} usage anomalies detected in the 28-day window.")

    if volatility == "high_volatility":
        issues.append("high_volatility")
        reasons.append("Usage is highly volatile (CV >= threshold).")

    if volatility == "bursty":
        issues.append("bursty_usage")
        reasons.append("Usage pattern is bursty (sparse spikes with many zero days).")

    # Determine primary issue (precedence order)
    precedence = [
        "prolonged_inactivity",
        "recent_inactivity",
        "usage_decline",
        "anomaly_detected",
        "high_volatility",
        "bursty_usage",
    ]
    primary = "none"
    for p in precedence:
        if p in issues:
            primary = p
            break

    issue_count = len(issues)

    # Determine overall status
    if primary == "prolonged_inactivity":
        overall = "prolonged_inactivity"
    elif primary == "recent_inactivity":
        overall = "recently_inactive"
    elif primary == "usage_decline":
        overall = "declining_usage"
    elif volatility == "bursty":
        overall = "bursty_usage"
    elif direction == "growing" and not issues:
        overall = "growing_usage"
        reasons.append("Usage is growing materially over the 28-day comparison window.")
    elif direction == "growing":
        overall = "growing_usage"
        reasons.append("Usage is growing.")
    elif consistency == "regular" and not issues:
        overall = "stable_regular_usage"
        reasons.append("Usage is stable and regular.")
    elif consistency == "intermittent" and not issues:
        overall = "stable_intermittent_usage"
        reasons.append("Usage is stable but intermittent.")
    elif direction == "newly_active":
        overall = "newly_active"
        reasons.append("Report newly active with no prior usage in comparison window.")
    elif direction == "inactive" or inactive_28d:
        overall = "recently_inactive"
        if not reasons:
            reasons.append("No usage in 28-day window.")
    else:
        overall = "stable_regular_usage" if not issues else "declining_usage"
        if not reasons:
            reasons.append("Usage is stable.")

    if not reasons:
        reasons.append("No significant usage issues detected.")

    return (overall, primary, issue_count, "|".join(reasons))


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------

def _build_report_features_row(
    report_id: str,
    report_row: pd.Series,
    daily_views: pd.Series,  # index = date objects, values = float view counts
    analytics_as_of_date: date,
    source_coverage_start: date,
    analytics_run_id: str,
    generated_at: str,
    cfg: ReportFeaturesConfig,
) -> dict:
    try:
        aod = analytics_as_of_date

        # Window boundaries
        w7_start = aod - timedelta(days=6)
        w28_start = aod - timedelta(days=27)
        wprev28_end = w28_start - timedelta(days=1)
        wprev28_start = wprev28_end - timedelta(days=27)
        w90_start = aod - timedelta(days=89)

        # Lifetime spine (all available data up to aod)
        lifetime_start = source_coverage_start
        activation_date = _safe_date(report_row.get("launch_date"))
        eff_start = max(
            lifetime_start,
            activation_date if activation_date is not None else lifetime_start,
        )
        # Only consider data up to aod
        valid_views = daily_views[daily_views.index <= aod] if not daily_views.empty else pd.Series(dtype=float)

        # Observed dates (positive usage only)
        has_mart_records = not valid_views.empty
        has_positive_usage = has_mart_records and (valid_views > 0).any()

        first_usage_date = valid_views[valid_views > 0].index.min() if has_positive_usage else None
        latest_usage_date = valid_views[valid_views > 0].index.max() if has_positive_usage else None
        # First date with ANY record in mart (including zeros)
        first_date_in_mart = valid_views.index.min() if has_mart_records else None

        observed_calendar_days = int((valid_views > 0).sum()) if has_mart_records else 0
        active_usage_days_lifetime = observed_calendar_days

        # days_since_last_use
        days_since_last_use = None
        if latest_usage_date is not None:
            days_since_last_use = max(0, (aod - latest_usage_date).days)

        # Inactivity flags (preliminary)
        inactive_7d = True
        inactive_28d = False

        # History sufficiency: use first mart date (including zeros) when no known activation
        if activation_date is not None and activation_date <= aod:
            avail_days = max(0, (aod - max(activation_date, source_coverage_start)).days + 1)
        elif first_date_in_mart is not None:
            avail_days = max(0, (aod - max(first_date_in_mart, source_coverage_start)).days + 1)
        else:
            avail_days = 0

        suf = _compute_history_sufficiency(avail_days)

        # Spines
        spine_7d = _build_report_day_spine(report_id, w7_start, aod, valid_views)
        spine_28d = _build_report_day_spine(report_id, w28_start, aod, valid_views)
        spine_prev28d = _build_report_day_spine(report_id, wprev28_start, wprev28_end, valid_views)
        spine_90d = _build_report_day_spine(report_id, w90_start, aod, valid_views)
        spine_lifetime = _build_report_day_spine(report_id, eff_start, aod, valid_views)

        # Inactivity
        inactive_7d = bool((spine_7d == 0).all()) if not spine_7d.empty else True
        inactive_28d = bool((spine_28d == 0).all() and suf["history_sufficient_28d"]) if not spine_28d.empty else False

        # Lifecycle fields
        lifecycle = _compute_lifecycle_fields(
            launch_date_raw=report_row.get("launch_date"),
            first_usage_date=first_usage_date,
            latest_usage_date=latest_usage_date,
            analytics_as_of_date=aod,
            cfg=cfg,
            inactive_28d=inactive_28d,
            history_sufficient_28d=suf["history_sufficient_28d"],
            days_since_last_use=days_since_last_use,
        )

        # Re-derive available_calendar_history_days from lifecycle output
        available_calendar_history_days = lifecycle["available_calendar_history_days"]

        # Usage evidence
        # "no_valid_data" = no mart records at all; zero-view records ARE data
        if not has_mart_records:
            usage_evidence_status = "no_valid_data"
            usage_evidence_reasons = "No usage events found for this report."
        elif not suf["history_sufficient_28d"]:
            usage_evidence_status = "insufficient_history"
            usage_evidence_reasons = "Less than 28 calendar days of history available."
        else:
            usage_evidence_status = "sufficient"
            usage_evidence_reasons = "Sufficient history for analysis."

        # Usage totals
        totals = _compute_usage_totals(
            spine_28d=spine_28d,
            spine_prev28d=spine_prev28d,
            spine_7d=spine_7d,
            spine_90d=spine_90d,
            spine_lifetime=spine_lifetime,
            history_sufficient_28d=suf["history_sufficient_28d"],
            comparison_sufficient=suf["comparison_history_sufficient_28d"],
            history_sufficient_90d=suf["history_sufficient_90d"],
            history_sufficient_7d=suf["history_sufficient_7d"],
        )

        # Usage change
        change = _compute_usage_change(
            recent_views=totals["recent_28d_views"],
            previous_views=totals["previous_28d_views"],
            active_days_recent=totals["active_usage_days_28d"],
            active_days_prev=totals["active_usage_days_previous_28d"],
            comparison_sufficient=suf["comparison_history_sufficient_28d"],
            cfg=cfg,
        )

        # Trend (use 28d for status, 90d for slope)
        slope_28d, strength_28d, trend_status_28d, _ = _compute_trend(spine_28d, cfg)
        slope_90d, _, trend_status_90d, trend_evidence = _compute_trend(spine_90d, cfg)
        # Pick best trend status
        trend_status = trend_status_90d if suf["history_sufficient_90d"] else trend_status_28d
        trend_evidence_status = trend_evidence if suf["history_sufficient_90d"] else "partial"
        if usage_evidence_status == "no_valid_data":
            trend_status = "inactive"
            trend_evidence_status = "insufficient"

        # Volatility
        udr28 = totals.get("usage_day_rate_28d", np.nan)
        if pd.isna(udr28):
            udr28 = 0.0
        vol = _compute_volatility(spine_28d, suf["history_sufficient_28d"], udr28, cfg)

        # Inactivity metrics
        zero_days_28d = int((spine_28d == 0).sum()) if suf["history_sufficient_28d"] else None
        zero_share_28d = (zero_days_28d / 28.0) if zero_days_28d is not None else np.nan

        # Zero streaks: use 90d spine for longest streak
        spine_for_streak = spine_90d if suf["history_sufficient_90d"] else spine_lifetime
        current_streak, longest_streak = _compute_zero_streak(spine_for_streak)

        # Inactivity status
        if vol["usage_volatility_status"] == "inactive" or (not has_mart_records):
            inactivity_status = "permanently_inactive"
        elif inactive_28d:
            inactivity_status = "recently_inactive"
        elif inactive_7d:
            inactivity_status = "short_term_inactive"
        else:
            inactivity_status = "active"

        # Peaks and anomalies
        peaks = _compute_peaks_and_anomalies(
            spine_28d=spine_28d,
            spine_90d=spine_90d,
            history_28d=suf["history_sufficient_28d"],
            history_90d=suf["history_sufficient_90d"],
            analytics_as_of_date=aod,
            cfg=cfg,
        )

        # Assemble partial row for status determination
        row_for_status = {
            "usage_evidence_status": usage_evidence_status,
            "history_sufficient_28d": suf["history_sufficient_28d"],
            "inactive_28d": inactive_28d,
            "usage_direction_28d": change["usage_direction_28d"],
            "usage_change_materiality": change["usage_change_materiality"],
            "usage_volatility_status": vol["usage_volatility_status"],
            "usage_consistency_status": vol["usage_consistency_status"],
            "usage_anomaly_count_28d": peaks["usage_anomaly_count_28d"],
            "current_zero_usage_streak_days": current_streak,
        }
        hist_status, primary_issue, issue_count, hist_reasons = _determine_historical_status(
            row_for_status, cfg
        )

        review_required = primary_issue not in ("none", "insufficient_history")

        # Dim fields
        report_name = report_row.get("report_name", None)
        workspace_id = report_row.get("workspace_id", None)

        return {
            # Lineage
            "analytics_run_id": analytics_run_id,
            "generated_at": generated_at,
            "analytics_as_of_date": str(aod),
            "source_max_usage_date": str(aod),
            "analytics_timezone": cfg.ANALYTICS_TIMEZONE,
            "as_of_date_policy": "source_max_date",
            "schema_version": REPORT_FEATURES_SCHEMA_VERSION,
            "report_id": report_id,
            "report_name": report_name,
            "workspace_id": workspace_id,
            # Lifecycle
            "report_activation_date": lifecycle["report_activation_date"],
            "report_age_days": lifecycle["report_age_days"],
            "first_observed_usage_date": lifecycle["first_observed_usage_date"],
            "latest_observed_usage_date": lifecycle["latest_observed_usage_date"],
            "days_since_first_observed_use": lifecycle["days_since_first_observed_use"],
            "days_since_last_use": days_since_last_use,
            "adoption_maturity_status": lifecycle["adoption_maturity_status"],
            "report_lifecycle_status": lifecycle["report_lifecycle_status"],
            "activation_date_status": lifecycle["activation_date_status"],
            # Evidence
            "available_calendar_history_days": available_calendar_history_days,
            "observed_calendar_days": observed_calendar_days,
            "active_usage_days_lifetime": active_usage_days_lifetime,
            "history_sufficient_7d": suf["history_sufficient_7d"],
            "history_sufficient_28d": suf["history_sufficient_28d"],
            "history_sufficient_previous_28d": suf["history_sufficient_previous_28d"],
            "comparison_history_sufficient_28d": suf["comparison_history_sufficient_28d"],
            "history_sufficient_90d": suf["history_sufficient_90d"],
            "usage_evidence_status": usage_evidence_status,
            "usage_evidence_reasons": usage_evidence_reasons,
            # Totals
            "lifetime_views": totals["lifetime_views"],
            "recent_7d_views": totals["recent_7d_views"],
            "recent_28d_views": totals["recent_28d_views"],
            "previous_28d_views": totals["previous_28d_views"],
            "recent_90d_views": totals["recent_90d_views"],
            "average_daily_views_28d": totals["average_daily_views_28d"],
            "average_daily_views_previous_28d": totals["average_daily_views_previous_28d"],
            "active_usage_days_28d": totals["active_usage_days_28d"],
            "active_usage_days_previous_28d": totals["active_usage_days_previous_28d"],
            "usage_day_rate_28d": totals["usage_day_rate_28d"],
            # Change
            "usage_change_28d": change["usage_change_28d"],
            "usage_change_28d_pct": change["usage_change_28d_pct"],
            "average_daily_usage_change_28d": change["average_daily_usage_change_28d"],
            "average_daily_usage_change_28d_pct": change["average_daily_usage_change_28d_pct"],
            "active_usage_days_change_28d": change["active_usage_days_change_28d"],
            "usage_direction_28d": change["usage_direction_28d"],
            "usage_change_materiality": change["usage_change_materiality"],
            # Trend
            "usage_trend_slope_28d": slope_28d,
            "usage_trend_slope_90d": slope_90d,
            "usage_trend_strength_28d": strength_28d,
            "usage_trend_status": trend_status,
            "trend_evidence_status": trend_evidence_status,
            # Volatility
            "usage_mean_daily_28d": vol["usage_mean_daily_28d"],
            "usage_std_daily_28d": vol["usage_std_daily_28d"],
            "usage_cv_28d": vol["usage_cv_28d"],
            "usage_median_daily_28d": vol["usage_median_daily_28d"],
            "usage_p90_daily_28d": vol["usage_p90_daily_28d"],
            "usage_max_daily_28d": vol["usage_max_daily_28d"],
            "usage_volatility_status": vol["usage_volatility_status"],
            "usage_consistency_status": vol["usage_consistency_status"],
            # Inactivity
            "current_zero_usage_streak_days": current_streak,
            "longest_zero_usage_streak_90d": longest_streak,
            "zero_usage_days_28d": zero_days_28d,
            "zero_usage_share_28d": zero_share_28d,
            "inactive_7d": inactive_7d,
            "inactive_28d": inactive_28d,
            "inactivity_status": inactivity_status,
            # Peaks and anomalies
            "peak_usage_date_90d": peaks["peak_usage_date_90d"],
            "peak_daily_views_90d": peaks["peak_daily_views_90d"],
            "recent_peak_usage_date_28d": peaks["recent_peak_usage_date_28d"],
            "recent_peak_daily_views_28d": peaks["recent_peak_daily_views_28d"],
            "usage_anomaly_count_28d": peaks["usage_anomaly_count_28d"],
            "latest_usage_anomaly_status": peaks["latest_usage_anomaly_status"],
            "anomaly_evidence_status": peaks["anomaly_evidence_status"],
            # Summary
            "historical_usage_status": hist_status,
            "primary_historical_usage_issue": primary_issue,
            "historical_usage_issue_count": issue_count,
            "historical_usage_reasons": hist_reasons,
            "historical_usage_review_required": review_required,
        }

    except Exception as exc:
        # Fail gracefully — return a minimal error row
        row = {col: None for col in REPORT_FEATURES_COLS}
        row["analytics_run_id"] = analytics_run_id
        row["generated_at"] = generated_at
        row["analytics_as_of_date"] = str(analytics_as_of_date)
        row["schema_version"] = REPORT_FEATURES_SCHEMA_VERSION
        row["report_id"] = report_id
        row["historical_usage_status"] = "calculation_failed"
        row["primary_historical_usage_issue"] = "calculation_error"
        row["historical_usage_reasons"] = f"calculation_failure:{exc}"
        row["historical_usage_review_required"] = True
        return row


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_report_features(
    mart_df: pd.DataFrame,
    dim_report_df: pd.DataFrame,
    analytics_as_of_date: date,
    analytics_run_id: str,
    cfg: ReportFeaturesConfig = None,
) -> pd.DataFrame:
    """Build one evidence-aware feature row per report.

    Parameters
    ----------
    mart_df:
        Either mart_report_user_daily (has user_key column — will be aggregated
        to daily totals) or a pre-aggregated daily frame with report_id,
        usage_date/date, daily_views columns.
    dim_report_df:
        Report dimension table (report_id, report_name, workspace_id, launch_date, …).
    analytics_as_of_date:
        Source-derived as-of date. Must NOT be date.today().
    analytics_run_id:
        UUID string identifying this analytics run.
    cfg:
        Optional ReportFeaturesConfig. Defaults to ReportFeaturesConfig().

    Returns
    -------
    pd.DataFrame with columns in REPORT_FEATURES_COLS.
    """
    if cfg is None:
        cfg = ReportFeaturesConfig()

    generated_at = datetime.utcnow().isoformat()

    # ── Aggregate mart to daily totals ──────────────────────────────────────
    if mart_df is None or mart_df.empty:
        daily_agg = pd.DataFrame(columns=["report_id", "usage_date", "daily_views"])
    else:
        df = mart_df.copy()
        # Detect date column
        if "usage_date" in df.columns:
            date_col = "usage_date"
        elif "date" in df.columns:
            date_col = "date"
        else:
            daily_agg = pd.DataFrame(columns=["report_id", "usage_date", "daily_views"])
            date_col = None

        if date_col is not None:
            # Detect view column
            if "daily_views" in df.columns:
                view_col = "daily_views"
            elif "view_count" in df.columns:
                view_col = "view_count"
            elif "views" in df.columns:
                view_col = "views"
            else:
                view_col = None

            df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
            df = df.dropna(subset=["report_id", date_col])

            if view_col:
                df[view_col] = pd.to_numeric(df[view_col], errors="coerce").fillna(0)
                daily_agg = (
                    df.groupby(["report_id", date_col], as_index=False)[view_col]
                    .sum()
                    .rename(columns={date_col: "usage_date", view_col: "daily_views"})
                )
            else:
                # Count rows as 1 view each
                daily_agg = (
                    df.groupby(["report_id", date_col])
                    .size()
                    .reset_index(name="daily_views")
                    .rename(columns={date_col: "usage_date"})
                )

    # ── Source coverage start ────────────────────────────────────────────────
    if daily_agg.empty:
        source_coverage_start = analytics_as_of_date
    else:
        source_coverage_start = pd.to_datetime(daily_agg["usage_date"]).dt.date.min()

    # ── Build daily views lookup: {report_id: pd.Series(index=date, values=views)} ──
    views_by_report: dict[str, pd.Series] = {}
    if not daily_agg.empty:
        for rid, grp in daily_agg.groupby("report_id"):
            grp_sorted = grp.sort_values("usage_date")
            s = pd.Series(
                grp_sorted["daily_views"].values.astype(float),
                index=grp_sorted["usage_date"].values,
                name=rid,
            )
            views_by_report[str(rid)] = s

    # ── Build one row per report in dim_report_df ───────────────────────────
    if dim_report_df is None or dim_report_df.empty:
        # Derive spine from daily_agg
        if daily_agg.empty:
            return pd.DataFrame(columns=REPORT_FEATURES_COLS)
        report_ids = daily_agg["report_id"].unique()
        dim_spine = pd.DataFrame({"report_id": report_ids})
    else:
        dim_spine = dim_report_df.copy()

    rows = []
    for _, dim_row in dim_spine.sort_values("report_id").iterrows():
        report_id = str(dim_row["report_id"])
        daily_views = views_by_report.get(report_id, pd.Series(dtype=float))

        row = _build_report_features_row(
            report_id=report_id,
            report_row=dim_row,
            daily_views=daily_views,
            analytics_as_of_date=analytics_as_of_date,
            source_coverage_start=source_coverage_start,
            analytics_run_id=analytics_run_id,
            generated_at=generated_at,
            cfg=cfg,
        )
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=REPORT_FEATURES_COLS)

    result = pd.DataFrame(rows, columns=REPORT_FEATURES_COLS)
    return result.reset_index(drop=True)


# ---------------------------------------------------------------------------
# Validation and persistence
# ---------------------------------------------------------------------------

def validate_report_features(df: pd.DataFrame, analytics_as_of_date: date = None) -> None:
    """Validate a report_features DataFrame. Raises ValueError on failure."""

    # 1. Deprecated fields absent
    deprecated_present = [c for c in _DEPRECATED_COLS if c in df.columns]
    if deprecated_present:
        raise ValueError(f"Deprecated columns present in report_features: {deprecated_present}")

    # 2. Required columns present
    missing = [c for c in REPORT_FEATURES_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    if df.empty:
        return

    # 3. Schema version
    versions = df["schema_version"].dropna().unique()
    if len(versions) > 0 and not all(v == REPORT_FEATURES_SCHEMA_VERSION for v in versions):
        raise ValueError(f"Unexpected schema_version values: {versions}")

    # 4. Unique (analytics_run_id, report_id)
    dupes = df.duplicated(subset=["analytics_run_id", "report_id"])
    if dupes.any():
        bad = df.loc[dupes, "report_id"].tolist()
        raise ValueError(f"Duplicate (analytics_run_id, report_id): {bad}")

    # 5. No direct user identifiers
    bad_cols = set(df.columns) & PROHIBITED_OUTPUT_COLUMNS
    if bad_cols:
        raise ValueError(f"Prohibited user identifier columns found: {bad_cols}")

    # 6. Non-negative numeric fields
    for col in ("days_since_last_use", "report_age_days", "recent_28d_views", "usage_cv_28d"):
        if col in df.columns:
            neg = df[df[col].notna() & (pd.to_numeric(df[col], errors="coerce") < 0)]
            if not neg.empty:
                raise ValueError(f"Negative values in {col}: {neg['report_id'].tolist()}")

    # 7. Status values in allowed sets
    valid_maturity = {"newly_launched", "maturing", "mature"}
    if "adoption_maturity_status" in df.columns:
        bad = df[~df["adoption_maturity_status"].isin(valid_maturity)]["adoption_maturity_status"].dropna()
        if not bad.empty:
            raise ValueError(f"Invalid adoption_maturity_status: {bad.unique()}")

    valid_lifecycle = {
        "pre_activation", "newly_launched", "maturing", "established",
        "dormant", "unknown", "calculation_failed",
    }
    if "report_lifecycle_status" in df.columns:
        bad = df[~df["report_lifecycle_status"].isin(valid_lifecycle)]["report_lifecycle_status"].dropna()
        if not bad.empty:
            raise ValueError(f"Invalid report_lifecycle_status: {bad.unique()}")


def persist_report_features(df: pd.DataFrame, project_root: Path) -> Path:
    """Validate and write report_features to CSV. Returns the output path."""
    validate_report_features(df)
    out_path = project_root / "outputs" / "metrics" / "report_features.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.sort_values(["analytics_run_id", "report_id"]).to_csv(out_path, index=False)
    return out_path
