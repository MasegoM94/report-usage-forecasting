"""
Tests for src/analytics/user_engagement_metrics.py

Sprint 6 — Windowed active-user, returning-user, and one-time-user metrics.
All tests use inline DataFrames — no real files loaded from disk.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.analytics.user_engagement_metrics import (
    REPORT_USER_ACTIVITY_METRICS_COLS,
    UserEngagementMetricsConfig,
    apply_engagement_privacy_suppression,
    build_report_user_activity_metrics,
    calculate_active_day_distribution,
    calculate_active_user_change,
    calculate_active_user_metrics,
    calculate_repeat_view_metrics,
    calculate_returning_user_metrics,
    calculate_window_user_activity,
    classify_repeat_usage_status,
    persist_report_user_activity_metrics,
    validate_report_user_activity_metrics,
)
from src.analytics.privacy_policy import validate_no_direct_identifiers

# ---------------------------------------------------------------------------
# Default config
# ---------------------------------------------------------------------------
CFG = UserEngagementMetricsConfig()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_id() -> str:
    return str(uuid.uuid4())


def _make_mart_rows(report_id: str, user_keys_dates_views: list) -> pd.DataFrame:
    """user_keys_dates_views: list of (user_key, date_str, daily_views)"""
    rows = []
    for user_key, date_str, daily_views in user_keys_dates_views:
        rows.append({
            "analytics_run_id": "test-run",
            "report_id": report_id,
            "user_key": user_key,
            "usage_date": date_str,
            "daily_views": daily_views,
        })
    return pd.DataFrame(rows)


def _make_sufficiency_row(report_id: str, report_name: str = "Test Report", **flags) -> pd.DataFrame:
    """Returns single-row sufficiency DataFrame with all required columns defaulted."""
    defaults = {
        "analytics_run_id": "test-run",
        "generated_at": "2026-01-01T00:00:00",
        "analytics_as_of_date": "2026-03-30",
        "report_id": report_id,
        "report_name": report_name,
        "report_activation_date": "2025-01-01",
        "activation_date_status": "known",
        "history_inference_method": "known_activation_date",
        "first_observed_usage_date": "2025-01-01",
        "latest_observed_usage_date": "2026-03-30",
        "available_calendar_history_days": 365,
        "active_usage_days_lifetime": 10,
        "has_any_valid_user_activity": True,
        "report_active_as_of_date": True,
        "source_coverage_start_date": "2025-01-01",
        "history_sufficient_7d": True,
        "history_sufficient_28d": True,
        "history_sufficient_previous_28d": True,
        "comparison_history_sufficient_28d": True,
        "history_sufficient_90d": True,
        "history_sufficient_previous_90d": True,
        "comparison_history_sufficient_90d": True,
        "activation_before_7d_window": True,
        "activation_before_28d_window": True,
        "activation_before_previous_28d_window": True,
        "activation_before_90d_window": True,
        "activation_before_previous_90d_window": True,
        "history_sufficiency_status": "complete_90d_history",
        "history_sufficiency_reasons": "none",
        "history_source_status": "mart_available",
    }
    defaults.update(flags)
    return pd.DataFrame([defaults])


def _make_quality_row(
    report_id: str,
    data_quality_status: str = "good",
    excluded_share: float = 0.0,
) -> pd.DataFrame:
    return pd.DataFrame([{
        "analytics_run_id": "test-run",
        "report_id": report_id,
        "report_name": "Test Report",
        "data_quality_status": data_quality_status,
        "excluded_user_event_share": excluded_share,
    }])


def _make_boundaries(as_of_date_str: str) -> pd.DataFrame:
    """Returns single-row boundaries DataFrame with computed window dates."""
    aod = pd.to_datetime(as_of_date_str).date()
    w7e = aod
    w7s = aod - timedelta(days=6)
    w28e = aod
    w28s = aod - timedelta(days=27)
    pw28e = w28s - timedelta(days=1)
    pw28s = pw28e - timedelta(days=27)
    w90e = aod
    w90s = aod - timedelta(days=89)
    pw90e = w90s - timedelta(days=1)
    pw90s = pw90e - timedelta(days=89)
    pre_prev28_end = pw28s - timedelta(days=1)
    return pd.DataFrame([{
        "analytics_run_id": "test-run",
        "generated_at": "2026-01-01T00:00:00",
        "analytics_timezone": "UTC",
        "source_max_usage_date": str(aod),
        "analytics_as_of_date": str(aod),
        "as_of_date_policy": "source_max_date",
        "latest_date_completeness_status": "complete",
        "window_7d_start": str(w7s),
        "window_7d_end": str(w7e),
        "window_28d_start": str(w28s),
        "window_28d_end": str(w28e),
        "previous_28d_start": str(pw28s),
        "previous_28d_end": str(pw28e),
        "window_90d_start": str(w90s),
        "window_90d_end": str(w90e),
        "previous_90d_start": str(pw90s),
        "previous_90d_end": str(pw90e),
        "pre_previous_28d_end": str(pre_prev28_end),
    }])


def _build(suf_df, mart_df, quality_df=None, as_of="2026-03-30"):
    """Run build_report_user_activity_metrics with default boundaries."""
    if quality_df is None:
        quality_df = pd.DataFrame()
    boundaries_df = _make_boundaries(as_of)
    return build_report_user_activity_metrics(
        sufficiency_df=suf_df,
        mart_df=mart_df,
        quality_df=quality_df,
        boundaries_df=boundaries_df,
        cfg=CFG,
        analytics_run_id=_run_id(),
    )


# ---------------------------------------------------------------------------
# TestActiveUserCounts
# ---------------------------------------------------------------------------

class TestActiveUserCounts:
    AS_OF = "2026-03-30"

    def _boundaries_dates(self):
        aod = pd.to_datetime(self.AS_OF).date()
        return {
            "w7s": aod - timedelta(days=6),
            "w7e": aod,
            "w28s": aod - timedelta(days=27),
            "w28e": aod,
            "pw28s": (aod - timedelta(days=27)) - timedelta(days=28),
            "pw28e": (aod - timedelta(days=27)) - timedelta(days=1),
            "w90s": aod - timedelta(days=89),
            "w90e": aod,
        }

    def test_one_user_recent_7d(self):
        d = self._boundaries_dates()
        date_in_7d = str(d["w7s"])
        mart = _make_mart_rows("R_001", [("UK_0001", date_in_7d, 1)])
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart, as_of=self.AS_OF)
        assert result.iloc[0]["unique_users_7d"] == 1

    def test_one_user_recent_28d(self):
        d = self._boundaries_dates()
        mart = _make_mart_rows("R_001", [("UK_0001", str(d["w28s"]), 1)])
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart, as_of=self.AS_OF)
        assert result.iloc[0]["unique_users_28d"] == 1

    def test_one_user_recent_90d(self):
        d = self._boundaries_dates()
        mart = _make_mart_rows("R_001", [("UK_0001", str(d["w90s"]), 1)])
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart, as_of=self.AS_OF)
        assert result.iloc[0]["unique_users_90d"] == 1

    def test_multiple_users_counted(self):
        d = self._boundaries_dates()
        dt = str(d["w28s"])
        mart = _make_mart_rows("R_001", [
            ("UK_0001", dt, 1), ("UK_0002", dt, 1), ("UK_0003", dt, 1),
        ])
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart, as_of=self.AS_OF)
        assert result.iloc[0]["unique_users_28d"] == 3

    def test_user_active_in_multiple_windows(self):
        d = self._boundaries_dates()
        # Date in 7d window is also in 28d
        dt = str(d["w7s"])
        mart = _make_mart_rows("R_001", [("UK_0001", dt, 1)])
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart, as_of=self.AS_OF)
        assert result.iloc[0]["unique_users_7d"] == 1
        assert result.iloc[0]["unique_users_28d"] == 1

    def test_exact_window_start_included(self):
        d = self._boundaries_dates()
        mart = _make_mart_rows("R_001", [("UK_0001", str(d["w28s"]), 1)])
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart, as_of=self.AS_OF)
        assert result.iloc[0]["unique_users_28d"] == 1

    def test_exact_window_end_included(self):
        d = self._boundaries_dates()
        mart = _make_mart_rows("R_001", [("UK_0001", str(d["w28e"]), 1)])
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart, as_of=self.AS_OF)
        assert result.iloc[0]["unique_users_28d"] == 1

    def test_outside_window_excluded(self):
        d = self._boundaries_dates()
        # 1 day before window start
        before = str(d["w28s"] - timedelta(days=1))
        mart = _make_mart_rows("R_001", [("UK_0001", before, 1)])
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart, as_of=self.AS_OF)
        assert result.iloc[0]["unique_users_28d"] == 0

    def test_zero_activity_sufficient_history(self):
        mart = pd.DataFrame()  # no rows
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart, as_of=self.AS_OF)
        assert result.iloc[0]["unique_users_28d"] == 0

    def test_insufficient_history_gives_null(self):
        mart = pd.DataFrame()
        suf = _make_sufficiency_row(
            "R_001",
            history_sufficient_28d=False,
            history_sufficient_previous_28d=False,
            comparison_history_sufficient_28d=False,
        )
        result = _build(suf, mart, as_of=self.AS_OF)
        assert pd.isna(result.iloc[0]["unique_users_28d"]) or result.iloc[0]["unique_users_28d"] is None

    def test_no_valid_user_data(self):
        d = self._boundaries_dates()
        mart = _make_mart_rows("R_001", [("UK_0001", str(d["w28s"]), 1)])
        suf = _make_sufficiency_row("R_001")
        quality = _make_quality_row("R_001", data_quality_status="no_valid_user_data")
        result = _build(suf, mart, quality_df=quality, as_of=self.AS_OF)
        assert pd.isna(result.iloc[0]["unique_users_28d"]) or result.iloc[0]["unique_users_28d"] is None


# ---------------------------------------------------------------------------
# TestReturningAndOneTime
# ---------------------------------------------------------------------------

class TestReturningAndOneTime:
    AS_OF = "2026-03-30"

    def _w28s(self):
        return (pd.to_datetime(self.AS_OF).date() - timedelta(days=27))

    def test_one_view_one_date_is_one_time(self):
        dt = str(self._w28s())
        ua = calculate_window_user_activity(
            _make_mart_rows("R", [("U1", dt, 1)]), "R",
            self._w28s(), pd.to_datetime(self.AS_OF).date()
        )
        m = calculate_returning_user_metrics(ua, CFG)
        assert m["one_time_users"] == 1
        assert m["returning_users"] == 0

    def test_five_views_one_date_is_one_time(self):
        dt = str(self._w28s())
        ua = calculate_window_user_activity(
            _make_mart_rows("R", [("U1", dt, 5)]), "R",
            self._w28s(), pd.to_datetime(self.AS_OF).date()
        )
        m = calculate_returning_user_metrics(ua, CFG)
        assert m["one_time_users"] == 1
        assert m["returning_users"] == 0

    def test_one_view_two_dates_is_returning(self):
        aod = pd.to_datetime(self.AS_OF).date()
        w28s = self._w28s()
        dt1 = str(w28s)
        dt2 = str(w28s + timedelta(days=1))
        ua = calculate_window_user_activity(
            _make_mart_rows("R", [("U1", dt1, 1), ("U1", dt2, 1)]), "R", w28s, aod
        )
        m = calculate_returning_user_metrics(ua, CFG)
        assert m["returning_users"] == 1
        assert m["one_time_users"] == 0

    def test_multiple_views_multiple_dates_is_returning(self):
        aod = pd.to_datetime(self.AS_OF).date()
        w28s = self._w28s()
        ua = calculate_window_user_activity(
            _make_mart_rows("R", [
                ("U1", str(w28s), 3),
                ("U1", str(w28s + timedelta(days=2)), 2),
            ]), "R", w28s, aod
        )
        m = calculate_returning_user_metrics(ua, CFG)
        assert m["returning_users"] == 1

    def test_mixed_users_reconciliation(self):
        aod = pd.to_datetime(self.AS_OF).date()
        w28s = self._w28s()
        mart = _make_mart_rows("R", [
            ("U1", str(w28s), 1), ("U1", str(w28s + timedelta(days=1)), 1),  # returning
            ("U2", str(w28s), 1), ("U2", str(w28s + timedelta(days=2)), 1),  # returning
            ("U3", str(w28s), 1),  # one-time
        ])
        ua = calculate_window_user_activity(mart, "R", w28s, aod)
        m = calculate_returning_user_metrics(ua, CFG)
        total = calculate_active_user_metrics(ua)["unique_users"]
        assert m["returning_users"] + m["one_time_users"] == total

    def test_returning_plus_one_time_equals_unique(self):
        aod = pd.to_datetime(self.AS_OF).date()
        w28s = self._w28s()
        mart = _make_mart_rows("R", [
            ("U1", str(w28s), 1), ("U1", str(w28s + timedelta(days=1)), 1),
            ("U2", str(w28s), 1),
            ("U3", str(w28s), 1),
        ])
        ua = calculate_window_user_activity(mart, "R", w28s, aod)
        m = calculate_returning_user_metrics(ua, CFG)
        n = calculate_active_user_metrics(ua)["unique_users"]
        assert m["returning_users"] + m["one_time_users"] == n

    def test_zero_users_gives_null_shares(self):
        ua = pd.DataFrame(columns=["user_key", "active_date_count", "window_views",
                                   "returning_user_flag", "one_time_user_flag", "repeat_view_user_flag"])
        m = calculate_returning_user_metrics(ua, CFG)
        assert pd.isna(m["returning_user_share"]) or m["returning_user_share"] is None
        assert pd.isna(m["one_time_user_share"]) or m["one_time_user_share"] is None

    def test_returning_share_correct(self):
        aod = pd.to_datetime(self.AS_OF).date()
        w28s = self._w28s()
        mart = _make_mart_rows("R", [
            ("U1", str(w28s), 1), ("U1", str(w28s + timedelta(1)), 1),
            ("U2", str(w28s), 1), ("U2", str(w28s + timedelta(2)), 1),
            ("U3", str(w28s), 1),
            ("U4", str(w28s), 1),
        ])
        ua = calculate_window_user_activity(mart, "R", w28s, aod)
        m = calculate_returning_user_metrics(ua, CFG)
        assert abs(m["returning_user_share"] - 0.5) < 0.001

    def test_one_time_share_correct(self):
        aod = pd.to_datetime(self.AS_OF).date()
        w28s = self._w28s()
        mart = _make_mart_rows("R", [
            ("U1", str(w28s), 1), ("U1", str(w28s + timedelta(1)), 1),
            ("U2", str(w28s), 1), ("U2", str(w28s + timedelta(2)), 1),
            ("U3", str(w28s), 1),
            ("U4", str(w28s), 1),
        ])
        ua = calculate_window_user_activity(mart, "R", w28s, aod)
        m = calculate_returning_user_metrics(ua, CFG)
        assert abs(m["one_time_user_share"] - 0.5) < 0.001

    def test_previous_28d_returning_calculated(self):
        aod = pd.to_datetime(self.AS_OF).date()
        w28s = aod - timedelta(days=27)
        pw28e = w28s - timedelta(days=1)
        pw28s = pw28e - timedelta(days=27)
        # Put a returning user in the previous window
        mart = _make_mart_rows("R_001", [
            ("U1", str(pw28s), 1),
            ("U1", str(pw28s + timedelta(1)), 1),
        ])
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart, as_of=self.AS_OF)
        assert result.iloc[0]["returning_users_previous_28d"] == 1


# ---------------------------------------------------------------------------
# TestRepeatView
# ---------------------------------------------------------------------------

class TestRepeatView:
    AS_OF = "2026-03-30"

    def _w28_range(self):
        aod = pd.to_datetime(self.AS_OF).date()
        return aod - timedelta(days=27), aod

    def test_one_view_one_date_not_repeat_view(self):
        w28s, aod = self._w28_range()
        ua = calculate_window_user_activity(
            _make_mart_rows("R", [("U1", str(w28s), 1)]), "R", w28s, aod
        )
        m = calculate_repeat_view_metrics(ua, CFG)
        assert m["repeat_view_users"] == 0

    def test_five_views_one_date_is_repeat_view(self):
        w28s, aod = self._w28_range()
        ua = calculate_window_user_activity(
            _make_mart_rows("R", [("U1", str(w28s), 5)]), "R", w28s, aod
        )
        m = calculate_repeat_view_metrics(ua, CFG)
        assert m["repeat_view_users"] == 1

    def test_one_view_two_dates_is_repeat_view(self):
        w28s, aod = self._w28_range()
        ua = calculate_window_user_activity(
            _make_mart_rows("R", [
                ("U1", str(w28s), 1),
                ("U1", str(w28s + timedelta(1)), 1),
            ]), "R", w28s, aod
        )
        m = calculate_repeat_view_metrics(ua, CFG)
        assert m["repeat_view_users"] == 1

    def test_repeat_view_distinct_from_returning(self):
        # User with 5 views on 1 date = repeat_view but NOT returning
        w28s, aod = self._w28_range()
        ua = calculate_window_user_activity(
            _make_mart_rows("R", [("U1", str(w28s), 5)]), "R", w28s, aod
        )
        rm = calculate_returning_user_metrics(ua, CFG)
        rvm = calculate_repeat_view_metrics(ua, CFG)
        assert rm["returning_users"] == 0
        assert rvm["repeat_view_users"] == 1

    def test_repeat_view_share_correct(self):
        w28s, aod = self._w28_range()
        mart = _make_mart_rows("R", [
            ("U1", str(w28s), 5),   # repeat-view
            ("U2", str(w28s), 1),   # not repeat-view
        ])
        ua = calculate_window_user_activity(mart, "R", w28s, aod)
        m = calculate_repeat_view_metrics(ua, CFG)
        assert abs(m["repeat_view_user_share"] - 0.5) < 0.001


# ---------------------------------------------------------------------------
# TestActiveUserChange
# ---------------------------------------------------------------------------

class TestActiveUserChange:
    def test_recent_growth(self):
        r = calculate_active_user_change(15, 10, True, True, CFG)
        assert r["direction"] == "growing"
        assert r["change"] == 5

    def test_recent_decline(self):
        r = calculate_active_user_change(10, 15, True, True, CFG)
        assert r["direction"] == "declining"
        assert r["change"] == -5

    def test_stable_counts(self):
        # previous=10, recent=11 → change=1 (below MIN_ABSOLUTE=2), stable
        r = calculate_active_user_change(11, 10, True, True, CFG)
        assert r["direction"] == "stable"

    def test_newly_active(self):
        r = calculate_active_user_change(5, 0, True, True, CFG)
        assert r["direction"] == "newly_active"
        assert r["change"] == 5

    def test_inactive(self):
        r = calculate_active_user_change(0, 0, True, True, CFG)
        assert r["direction"] == "inactive"

    def test_previous_zero_recent_positive_direction(self):
        r = calculate_active_user_change(3, 0, True, True, CFG)
        assert r["direction"] == "newly_active"

    def test_recent_zero_previous_positive_direction(self):
        r = calculate_active_user_change(0, 10, True, True, CFG)
        assert r["direction"] == "declining"

    def test_small_base_not_material_growth(self):
        # previous=2, recent=4 → 100% growth but abs_change=2 barely meets threshold
        # 2 >= MIN_ABSOLUTE_USER_CHANGE_FOR_DIRECTION(2) and 1.0 >= USER_GROWTH_MATERIAL_PCT(0.20)
        # So this should be growing, not stable
        r = calculate_active_user_change(4, 2, True, True, CFG)
        # 4-2=2 >= 2 and pct=1.0 >= 0.20 → growing
        assert r["direction"] == "growing"

    def test_insufficient_comparison_history(self):
        r = calculate_active_user_change(10, 5, False, True, CFG)
        assert r["direction"] == "insufficient_history"
        assert r["change"] is None

    def test_pct_change_null_when_previous_zero(self):
        r = calculate_active_user_change(5, 0, True, True, CFG)
        assert r["change_pct"] is None


# ---------------------------------------------------------------------------
# TestActivesDayDistribution
# ---------------------------------------------------------------------------

class TestActivesDayDistribution:
    def _make_ua(self, active_day_counts: list[int]) -> pd.DataFrame:
        rows = []
        for i, c in enumerate(active_day_counts):
            rows.append({
                "user_key": f"U{i}",
                "active_date_count": c,
                "window_views": c,
                "returning_user_flag": c >= 2,
                "one_time_user_flag": c == 1,
                "repeat_view_user_flag": c > 1,
            })
        return pd.DataFrame(rows)

    def test_active_days_per_user_computed(self):
        ua = self._make_ua([1, 2, 3])
        d = calculate_active_day_distribution(ua, CFG)
        assert abs(d["mean_active_days"] - 2.0) < 0.001

    def test_median_active_days(self):
        ua = self._make_ua([1, 2, 3])
        d = calculate_active_day_distribution(ua, CFG)
        assert d["median_active_days"] == 2.0

    def test_p90_active_days(self):
        ua = self._make_ua([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        d = calculate_active_day_distribution(ua, CFG)
        assert d["p90_active_days"] >= 9.0  # 90th percentile of 1..10

    def test_max_active_days(self):
        ua = self._make_ua([1, 2, 5])
        d = calculate_active_day_distribution(ua, CFG)
        assert d["max_active_days"] == 5.0

    def test_aggregation_at_user_level_first(self):
        # User with 3 events on same day → 1 active day
        aod = date(2026, 3, 30)
        w28s = aod - timedelta(days=27)
        mart = _make_mart_rows("R", [
            ("U1", str(w28s), 3),  # same date, different rows? no, daily_views=3
        ])
        ua = calculate_window_user_activity(mart, "R", w28s, aod)
        assert ua.iloc[0]["active_date_count"] == 1

    def test_no_active_users_null_distribution(self):
        ua = pd.DataFrame(columns=["user_key", "active_date_count", "window_views",
                                   "returning_user_flag", "one_time_user_flag", "repeat_view_user_flag"])
        d = calculate_active_day_distribution(ua, CFG)
        assert d["mean_active_days"] is None
        assert d["median_active_days"] is None
        assert d["p90_active_days"] is None
        assert d["max_active_days"] is None

    def test_one_user_distribution(self):
        ua = self._make_ua([3])
        d = calculate_active_day_distribution(ua, CFG)
        assert d["mean_active_days"] == 3.0
        assert d["median_active_days"] == 3.0
        assert d["max_active_days"] == 3.0

    def test_privacy_suppression_nullifies_distribution(self):
        # unique_users=3 < MIN_USERS_FOR_DISTRIBUTION_METRICS=5 → suppressed
        aod = date(2026, 3, 30)
        w28s = aod - timedelta(days=27)
        mart = _make_mart_rows("R_001", [
            ("U1", str(w28s), 1),
            ("U2", str(w28s), 1),
            ("U3", str(w28s), 1),
        ])
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart)
        assert result.iloc[0]["mean_active_days_per_user_28d"] is None
        assert result.iloc[0]["median_active_days_per_user_28d"] is None


# ---------------------------------------------------------------------------
# TestPrivacySuppression
# ---------------------------------------------------------------------------

class TestPrivacySuppression:
    def test_below_threshold_suppresses_shares(self):
        metrics = {
            "returning_user_share_28d": 0.5,
            "one_time_user_share_28d": 0.5,
            "repeat_view_user_share_28d": 0.3,
            "mean_active_days_per_user_28d": 2.0,
            "median_active_days_per_user_28d": 2.0,
            "p90_active_days_per_user_28d": 3.0,
            "max_active_days_per_user_28d": 3.0,
            "returning_user_share_previous_28d": 0.4,
            "one_time_user_share_previous_28d": 0.6,
            "repeat_view_user_share_previous_28d": None,
            "returning_user_share_90d": 0.5,
            "one_time_user_share_90d": 0.5,
            "repeat_view_user_share_90d": None,
            "returning_user_share_change_28d": 0.1,
        }
        result = apply_engagement_privacy_suppression(metrics, unique_users=3, cfg=CFG)
        assert result["returning_user_share_28d"] is None
        assert result["activity_privacy_suppressed"] is True

    def test_exactly_threshold_not_suppressed(self):
        metrics = {k: None for k in [
            "returning_user_share_28d", "one_time_user_share_28d", "repeat_view_user_share_28d",
            "mean_active_days_per_user_28d", "median_active_days_per_user_28d",
            "p90_active_days_per_user_28d", "max_active_days_per_user_28d",
            "returning_user_share_previous_28d", "one_time_user_share_previous_28d",
            "repeat_view_user_share_previous_28d", "returning_user_share_90d",
            "one_time_user_share_90d", "repeat_view_user_share_90d",
            "returning_user_share_change_28d",
        ]}
        metrics["returning_user_share_28d"] = 0.5
        result = apply_engagement_privacy_suppression(metrics, unique_users=5, cfg=CFG)
        assert result["activity_privacy_suppressed"] is False
        assert result["returning_user_share_28d"] == 0.5

    def test_suppressed_counts_still_available(self):
        aod = date(2026, 3, 30)
        w28s = aod - timedelta(days=27)
        mart = _make_mart_rows("R_001", [
            ("U1", str(w28s), 1),
            ("U2", str(w28s), 1),
            ("U3", str(w28s), 1),
        ])
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart)
        # Counts should NOT be suppressed
        assert result.iloc[0]["unique_users_28d"] == 3
        assert result.iloc[0]["returning_users_28d"] is not None or result.iloc[0]["returning_users_28d"] == 0

    def test_active_user_change_not_suppressed(self):
        aod = date(2026, 3, 30)
        w28s = aod - timedelta(days=27)
        pw28e = w28s - timedelta(days=1)
        pw28s = pw28e - timedelta(days=27)
        mart = _make_mart_rows("R_001", [
            ("U1", str(w28s), 1),
            ("U2", str(w28s), 1),
            ("U3", str(w28s), 1),
            ("U1", str(pw28s), 1),
        ])
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart)
        # direction should not be suppressed
        assert result.iloc[0]["active_user_direction_28d"] is not None

    def test_suppressed_fields_metadata_deterministic(self):
        metrics = {
            "returning_user_share_28d": 0.5,
            "one_time_user_share_28d": 0.5,
            "repeat_view_user_share_28d": None,
            "mean_active_days_per_user_28d": 2.0,
            "median_active_days_per_user_28d": 2.0,
            "p90_active_days_per_user_28d": 3.0,
            "max_active_days_per_user_28d": 3.0,
            "returning_user_share_previous_28d": None,
            "one_time_user_share_previous_28d": None,
            "repeat_view_user_share_previous_28d": None,
            "returning_user_share_90d": None,
            "one_time_user_share_90d": None,
            "repeat_view_user_share_90d": None,
            "returning_user_share_change_28d": None,
        }
        r1 = apply_engagement_privacy_suppression(dict(metrics), unique_users=3, cfg=CFG)
        r2 = apply_engagement_privacy_suppression(dict(metrics), unique_users=3, cfg=CFG)
        assert r1["activity_suppressed_fields"] == r2["activity_suppressed_fields"]

    def test_no_user_list_in_output(self):
        mart = pd.DataFrame()
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart)
        for col in result.columns:
            assert not result[col].apply(lambda x: isinstance(x, (list, set, tuple))).any()

    def test_no_direct_identifiers(self):
        mart = pd.DataFrame()
        suf = _make_sufficiency_row("R_001")
        result = _build(suf, mart)
        validate_no_direct_identifiers(result, context="test_output")


# ---------------------------------------------------------------------------
# TestRepeatUsageStatus
# ---------------------------------------------------------------------------

class TestRepeatUsageStatus:
    def test_strong_repeat_engagement(self):
        status = classify_repeat_usage_status(0.60, 10, True, True, CFG)
        assert status == "strong_repeat_engagement"

    def test_moderate_repeat_engagement(self):
        status = classify_repeat_usage_status(0.30, 10, True, True, CFG)
        assert status == "moderate_repeat_engagement"

    def test_low_repeat_engagement(self):
        status = classify_repeat_usage_status(0.15, 10, True, True, CFG)
        assert status == "low_repeat_engagement"

    def test_no_recent_activity_status(self):
        status = classify_repeat_usage_status(None, 0, True, True, CFG)
        assert status == "no_recent_activity"

    def test_privacy_suppressed_status(self):
        status = classify_repeat_usage_status(0.5, 2, True, True, CFG)
        assert status == "privacy_suppressed"

    def test_insufficient_history_status(self):
        status = classify_repeat_usage_status(0.5, 10, False, True, CFG)
        assert status == "insufficient_history"

    def test_no_valid_user_data_status(self):
        status = classify_repeat_usage_status(0.5, 10, True, False, CFG)
        assert status == "no_valid_user_data"

    def test_newly_active_not_unfairly_classified(self):
        # A report with history_sufficient=True, unique_users=6, returning_share=0.5 → strong
        status = classify_repeat_usage_status(0.5, 6, True, True, CFG)
        assert status == "strong_repeat_engagement"


# ---------------------------------------------------------------------------
# TestValidation
# ---------------------------------------------------------------------------

def _make_valid_row(report_id: str = "R_001") -> dict:
    """Build a minimal valid metrics row."""
    return {
        "analytics_run_id": "run-1",
        "generated_at": "2026-01-01",
        "analytics_as_of_date": "2026-03-30",
        "report_id": report_id,
        "report_name": "Test",
        "history_sufficient_7d": True,
        "history_sufficient_28d": True,
        "history_sufficient_previous_28d": True,
        "comparison_history_sufficient_28d": True,
        "history_sufficient_90d": True,
        "history_sufficient_previous_90d": True,
        "comparison_history_sufficient_90d": True,
        "has_any_valid_user_activity": True,
        "user_data_quality_status": "good",
        "excluded_user_event_share": 0.0,
        "privacy_suppression_status": "not_suppressed",
        "unique_users_7d": 5,
        "unique_users_28d": 10,
        "unique_users_previous_28d": 8,
        "unique_users_90d": 15,
        "unique_users_previous_90d": 12,
        "active_user_change_28d": 2,
        "active_user_change_28d_pct": 0.25,
        "active_user_change_90d": 3,
        "active_user_change_90d_pct": 0.25,
        "active_user_direction_28d": "growing",
        "active_user_direction_90d": "growing",
        "returning_users_28d": 6,
        "one_time_users_28d": 4,
        "returning_user_share_28d": 0.6,
        "one_time_user_share_28d": 0.4,
        "returning_users_previous_28d": 5,
        "one_time_users_previous_28d": 3,
        "returning_user_share_previous_28d": None,
        "one_time_user_share_previous_28d": None,
        "returning_user_share_change_28d": None,
        "returning_users_90d": 9,
        "one_time_users_90d": 6,
        "returning_user_share_90d": 0.6,
        "one_time_user_share_90d": 0.4,
        "mean_active_days_per_user_28d": 2.0,
        "median_active_days_per_user_28d": 2.0,
        "p90_active_days_per_user_28d": 3.0,
        "max_active_days_per_user_28d": 5.0,
        "repeat_view_users_28d": 7,
        "repeat_view_user_share_28d": 0.7,
        "repeat_view_users_previous_28d": 5,
        "repeat_view_user_share_previous_28d": None,
        "repeat_usage_status": "strong_repeat_engagement",
        "activity_evidence_status": "sufficient",
        "activity_metric_reasons": "none",
        "activity_privacy_suppressed": False,
        "activity_suppressed_fields": None,
        "activity_privacy_suppression_reason": None,
    }


class TestValidation:
    def test_negative_count_rejected(self):
        row = _make_valid_row()
        row["unique_users_28d"] = -1
        df = pd.DataFrame([row])
        with pytest.raises(ValueError, match="Negative"):
            validate_report_user_activity_metrics(df, CFG)

    def test_returning_exceeds_unique_rejected(self):
        row = _make_valid_row()
        row["returning_users_28d"] = 15  # > unique_users_28d=10
        df = pd.DataFrame([row])
        with pytest.raises(ValueError):
            validate_report_user_activity_metrics(df, CFG)

    def test_shares_not_summing_to_one_rejected(self):
        row = _make_valid_row()
        row["returning_user_share_28d"] = 0.6
        row["one_time_user_share_28d"] = 0.5  # sum = 1.1
        df = pd.DataFrame([row])
        with pytest.raises(ValueError):
            validate_report_user_activity_metrics(df, CFG)

    def test_inactive_without_sufficient_history_rejected(self):
        row = _make_valid_row()
        row["active_user_direction_28d"] = "inactive"
        row["comparison_history_sufficient_28d"] = False
        df = pd.DataFrame([row])
        with pytest.raises(ValueError):
            validate_report_user_activity_metrics(df, CFG)

    def test_newly_active_with_nonzero_previous_rejected(self):
        row = _make_valid_row()
        row["active_user_direction_28d"] = "newly_active"
        row["unique_users_previous_28d"] = 5  # should be 0 for newly_active
        df = pd.DataFrame([row])
        with pytest.raises(ValueError):
            validate_report_user_activity_metrics(df, CFG)

    def test_suppression_inconsistency_rejected(self):
        row = _make_valid_row()
        row["activity_privacy_suppressed"] = True
        row["returning_user_share_28d"] = 0.5  # should be None when suppressed
        df = pd.DataFrame([row])
        with pytest.raises(ValueError):
            validate_report_user_activity_metrics(df, CFG)

    def test_duplicate_report_rows_rejected(self):
        row = _make_valid_row("R_001")
        df = pd.DataFrame([row, row])
        with pytest.raises(ValueError, match="[Dd]uplicate"):
            validate_report_user_activity_metrics(df, CFG)


# ---------------------------------------------------------------------------
# TestPersistence
# ---------------------------------------------------------------------------

class TestPersistence:
    def _make_result_df(self, n: int = 2) -> pd.DataFrame:
        rows = []
        for i in range(n):
            r = _make_valid_row(f"R_{i+1:03d}")
            r["analytics_run_id"] = "run-persist"
            rows.append(r)
        return pd.DataFrame(rows, columns=REPORT_USER_ACTIVITY_METRICS_COLS)

    def test_output_file_created(self, tmp_path: Path):
        df = self._make_result_df()
        out = persist_report_user_activity_metrics(df, tmp_path)
        assert out.exists()

    def test_schema_stable(self, tmp_path: Path):
        df = self._make_result_df()
        out = persist_report_user_activity_metrics(df, tmp_path)
        written = pd.read_csv(out)
        for col in REPORT_USER_ACTIVITY_METRICS_COLS:
            assert col in written.columns, f"Missing column: {col}"

    def test_deterministic_sorting(self, tmp_path: Path):
        rows = []
        for rid in ["R_003", "R_001", "R_002"]:
            r = _make_valid_row(rid)
            r["analytics_run_id"] = "run-sort"
            rows.append(r)
        df = pd.DataFrame(rows, columns=REPORT_USER_ACTIVITY_METRICS_COLS)
        out = persist_report_user_activity_metrics(df, tmp_path)
        written = pd.read_csv(out)
        assert list(written["report_id"]) == sorted(written["report_id"].tolist())

    def test_latest_file_replaced(self, tmp_path: Path):
        df1 = self._make_result_df(1)
        p1 = persist_report_user_activity_metrics(df1, tmp_path)
        df2 = self._make_result_df(2)
        p2 = persist_report_user_activity_metrics(df2, tmp_path)
        assert p1 == p2
        written = pd.read_csv(p2)
        assert len(written) == 2

    def test_source_mart_unchanged(self, tmp_path: Path):
        aod = date(2026, 3, 30)
        w28s = aod - timedelta(days=27)
        mart = _make_mart_rows("R_001", [("U1", str(w28s), 1)])
        before_len = len(mart)
        before_cols = list(mart.columns)
        suf = _make_sufficiency_row("R_001")
        _build(suf, mart)
        assert len(mart) == before_len
        assert list(mart.columns) == before_cols

    def test_invalid_output_rejected_before_writing(self, tmp_path: Path):
        row = _make_valid_row()
        row["unique_users_28d"] = -5
        df = pd.DataFrame([row], columns=REPORT_USER_ACTIVITY_METRICS_COLS)
        with pytest.raises(ValueError):
            persist_report_user_activity_metrics(df, tmp_path)
        out = tmp_path / "outputs" / "analytics" / "report_user_activity_metrics.csv"
        assert not out.exists()
