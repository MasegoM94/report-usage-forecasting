"""
Tests for src/analytics/user_frequency_metrics.py

Sprint 6 — Report-level usage-frequency and engagement-intensity metrics.
All tests use inline DataFrames — no real files loaded from disk.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.analytics.user_frequency_metrics import (
    REPORT_USER_FREQUENCY_METRICS_COLS,
    FrequencyMetricsConfig,
    aggregate_user_window_frequency,
    apply_frequency_privacy_suppression,
    build_report_frequency_metrics,
    calculate_frequency_change_metrics,
    calculate_report_frequency_metrics,
    calculate_return_gaps,
    classify_frequency_direction,
    classify_frequency_status,
    persist_report_frequency_metrics,
    validate_report_frequency_metrics,
)
from src.analytics.privacy_policy import validate_no_direct_identifiers

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_id() -> str:
    return str(uuid.uuid4())


def _make_mart(rows) -> pd.DataFrame:
    """rows: list of (report_id, user_key, usage_date_str, daily_views)"""
    records = []
    for report_id, user_key, usage_date_str, daily_views in rows:
        records.append({
            "analytics_run_id": "test-run",
            "report_id": report_id,
            "user_key": user_key,
            "usage_date": usage_date_str,
            "daily_views": daily_views,
        })
    df = pd.DataFrame(records)
    # Add first_report_use_date as min(usage_date) per (report_id, user_key)
    if not df.empty:
        first_use = (
            df.groupby(["report_id", "user_key"])["usage_date"]
            .min()
            .reset_index()
            .rename(columns={"usage_date": "first_report_use_date"})
        )
        df = df.merge(first_use, on=["report_id", "user_key"], how="left")
    else:
        df["first_report_use_date"] = pd.Series(dtype=object)
    return df


def _make_sufficiency(rows) -> pd.DataFrame:
    """rows: list of dicts with at least report_id; rest defaulted."""
    defaults = {
        "analytics_run_id": "test-run",
        "generated_at": "2024-04-01T00:00:00",
        "analytics_as_of_date": "2024-03-31",
        "report_name": "Test Report",
        "report_activation_date": "2023-01-01",
        "activation_date_status": "known",
        "history_inference_method": "known_activation_date",
        "first_observed_usage_date": "2023-01-01",
        "latest_observed_usage_date": "2024-03-31",
        "available_calendar_history_days": 456,
        "active_usage_days_lifetime": 30,
        "has_any_valid_user_activity": True,
        "report_active_as_of_date": True,
        "source_coverage_start_date": "2023-01-01",
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
    records = []
    for row in rows:
        r = dict(defaults)
        r.update(row)
        records.append(r)
    return pd.DataFrame(records)


def _make_quality(rows) -> pd.DataFrame:
    """rows: list of dicts with report_id, data_quality_status, excluded_user_event_share"""
    records = []
    for row in rows:
        r = {
            "report_id": row["report_id"],
            "data_quality_status": row.get("data_quality_status", "good"),
            "excluded_user_event_share": row.get("excluded_user_event_share", 0.0),
        }
        records.append(r)
    return pd.DataFrame(records)


def _make_boundaries(as_of_str: str = "2024-03-31") -> pd.DataFrame:
    """Compute window dates from as_of_date."""
    aod = date.fromisoformat(as_of_str)
    return pd.DataFrame([{
        "analytics_run_id": "test-run",
        "generated_at": "2024-04-01T00:00:00",
        "analytics_timezone": "UTC",
        "source_max_usage_date": as_of_str,
        "analytics_as_of_date": as_of_str,
        "as_of_date_policy": "source_max_date",
        "latest_date_completeness_status": "complete",
        "window_7d_start": str(aod - timedelta(days=6)),
        "window_7d_end": as_of_str,
        "window_28d_start": str(aod - timedelta(days=27)),
        "window_28d_end": as_of_str,
        "previous_28d_start": str(aod - timedelta(days=27) - timedelta(days=28)),
        "previous_28d_end": str(aod - timedelta(days=28)),
        "window_90d_start": str(aod - timedelta(days=89)),
        "window_90d_end": as_of_str,
        "previous_90d_start": str(aod - timedelta(days=89) - timedelta(days=90)),
        "previous_90d_end": str(aod - timedelta(days=90)),
        "pre_previous_28d_end": str(aod - timedelta(days=28) - timedelta(days=28) - timedelta(days=1)),
    }])


def _make_cfg() -> FrequencyMetricsConfig:
    return FrequencyMetricsConfig()


# ---------------------------------------------------------------------------
# TestBasicAggregation
# ---------------------------------------------------------------------------

class TestBasicAggregation:
    def test_single_user_single_view(self):
        mart = _make_mart([("R1", "U1", "2024-03-15", 1)])
        result = aggregate_user_window_frequency(
            mart, "R1", date(2024, 3, 1), date(2024, 3, 31)
        )
        assert len(result) == 1
        assert result.iloc[0]["user_total_views"] == 1
        assert result.iloc[0]["user_active_days"] == 1

    def test_single_user_multiple_views_same_day(self):
        mart = _make_mart([("R1", "U1", "2024-03-15", 5)])
        result = aggregate_user_window_frequency(
            mart, "R1", date(2024, 3, 1), date(2024, 3, 31)
        )
        assert result.iloc[0]["user_total_views"] == 5
        assert result.iloc[0]["user_active_days"] == 1

    def test_single_user_multiple_days(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),
            ("R1", "U1", "2024-03-15", 1),
            ("R1", "U1", "2024-03-20", 1),
        ])
        result = aggregate_user_window_frequency(
            mart, "R1", date(2024, 3, 1), date(2024, 3, 31)
        )
        assert result.iloc[0]["user_active_days"] == 3

    def test_multiple_users(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-15", 1),
            ("R1", "U2", "2024-03-16", 2),
        ])
        result = aggregate_user_window_frequency(
            mart, "R1", date(2024, 3, 1), date(2024, 3, 31)
        )
        assert len(result) == 2

    def test_multiple_reports_isolated(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-15", 3),
            ("R2", "U1", "2024-03-15", 7),
        ])
        r1 = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        r2 = aggregate_user_window_frequency(mart, "R2", date(2024, 3, 1), date(2024, 3, 31))
        assert r1.iloc[0]["user_total_views"] == 3
        assert r2.iloc[0]["user_total_views"] == 7

    def test_window_start_date_included(self):
        start = date(2024, 3, 4)  # window_28d_start for aod=2024-03-31
        mart = _make_mart([("R1", "U1", str(start), 1)])
        result = aggregate_user_window_frequency(mart, "R1", start, date(2024, 3, 31))
        assert len(result) == 1

    def test_window_end_date_included(self):
        end = date(2024, 3, 31)
        mart = _make_mart([("R1", "U1", str(end), 1)])
        result = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), end)
        assert len(result) == 1

    def test_outside_window_excluded(self):
        mart = _make_mart([("R1", "U1", "2024-02-28", 1)])  # 1 day before March window
        result = aggregate_user_window_frequency(
            mart, "R1", date(2024, 3, 1), date(2024, 3, 31)
        )
        assert len(result) == 0


# ---------------------------------------------------------------------------
# TestTotalMetrics
# ---------------------------------------------------------------------------

class TestTotalMetrics:
    def test_total_views_is_sum_of_daily_views(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 3),
            ("R1", "U1", "2024-03-02", 5),
            ("R1", "U2", "2024-03-03", 2),
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        total_views = int(user_df["user_total_views"].sum())
        assert total_views == 10

    def test_total_user_report_days_equals_row_count(self):
        # mart has 1 row per user-date; total_user_report_days = count of rows in window
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),
            ("R1", "U1", "2024-03-02", 2),
            ("R1", "U2", "2024-03-01", 1),
        ])
        start, end = date(2024, 3, 1), date(2024, 3, 31)
        # Filter mart to window
        mdf = mart.copy()
        mdf["_date"] = pd.to_datetime(mdf["usage_date"]).dt.date
        window_rows = mdf[(mdf["report_id"] == "R1") & (mdf["_date"] >= start) & (mdf["_date"] <= end)]
        assert len(window_rows) == 3  # 3 rows = 3 user-report-days

    def test_unique_users_count(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),
            ("R1", "U1", "2024-03-02", 1),
            ("R1", "U2", "2024-03-03", 1),
            ("R1", "U3", "2024-03-04", 1),
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        assert len(user_df) == 3

    def test_views_per_active_user_formula(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 4),
            ("R1", "U2", "2024-03-02", 6),
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        freq = calculate_report_frequency_metrics(user_df, 10, 2, _make_cfg())
        assert abs(freq["views_per_active_user"] - 5.0) < 0.01

    def test_views_per_user_day_formula(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 6),
            ("R1", "U1", "2024-03-02", 4),
            ("R1", "U2", "2024-03-03", 5),
        ])
        # total_views=15, total_user_report_days=3 → 5.0
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        freq = calculate_report_frequency_metrics(user_df, 15, 3, _make_cfg())
        assert abs(freq["views_per_user_day"] - 5.0) < 0.01

    def test_zero_activity_gives_zero_counts_null_ratios(self):
        empty_df = pd.DataFrame(columns=[
            "user_key", "user_total_views", "user_active_days",
            "user_views_per_active_day", "user_first_window_use_date",
            "user_last_window_use_date", "user_span_days",
        ])
        freq = calculate_report_frequency_metrics(empty_df, 0, 0, _make_cfg())
        assert freq["unique_users"] == 0
        assert freq["views_per_active_user"] is None
        assert freq["views_per_user_day"] is None


# ---------------------------------------------------------------------------
# TestDistributions
# ---------------------------------------------------------------------------

class TestDistributions:
    def test_user_level_aggregation_first(self):
        """User with 3 events on 1 day: user_total_views=3, user_active_days=1"""
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 3),
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        assert user_df.iloc[0]["user_total_views"] == 3
        assert user_df.iloc[0]["user_active_days"] == 1

    def test_median_views_per_user(self):
        """3 users with 1, 3, 5 views → median=3"""
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),
            ("R1", "U2", "2024-03-02", 3),
            ("R1", "U3", "2024-03-03", 5),
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        freq = calculate_report_frequency_metrics(user_df, 9, 3, _make_cfg())
        assert abs(freq["median_views_per_user"] - 3.0) < 0.01

    def test_p75_views_per_user(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),
            ("R1", "U2", "2024-03-02", 2),
            ("R1", "U3", "2024-03-03", 3),
            ("R1", "U4", "2024-03-04", 4),
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        freq = calculate_report_frequency_metrics(user_df, 10, 4, _make_cfg())
        # p75 of [1,2,3,4] = 3.25 with linear interpolation
        assert freq["p75_views_per_user"] >= 3.0

    def test_p90_views_per_user(self):
        mart = _make_mart([
            ("R1", f"U{i}", f"2024-03-{i:02d}", i) for i in range(1, 11)
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        freq = calculate_report_frequency_metrics(user_df, 55, 10, _make_cfg())
        assert freq["p90_views_per_user"] >= freq["median_views_per_user"]

    def test_max_views_per_user(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 2),
            ("R1", "U2", "2024-03-02", 10),
            ("R1", "U3", "2024-03-03", 5),
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        freq = calculate_report_frequency_metrics(user_df, 17, 3, _make_cfg())
        assert freq["max_views_per_user"] == 10.0

    def test_median_user_active_days(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),
            ("R1", "U2", "2024-03-01", 1),
            ("R1", "U2", "2024-03-02", 1),
            ("R1", "U3", "2024-03-01", 1),
            ("R1", "U3", "2024-03-02", 1),
            ("R1", "U3", "2024-03-03", 1),
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        freq = calculate_report_frequency_metrics(user_df, 6, 6, _make_cfg())
        # active days: [1, 2, 3] → median = 2
        assert abs(freq["median_user_active_days"] - 2.0) < 0.01

    def test_p90_user_active_days(self):
        mart = _make_mart([
            ("R1", f"U{i}", f"2024-03-{j:02d}", 1)
            for i, cnt in enumerate([1, 1, 2, 2, 3], start=1)
            for j in range(1, cnt + 1)
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        freq = calculate_report_frequency_metrics(user_df, len(mart), len(mart), _make_cfg())
        assert freq["p90_user_active_days"] is not None

    def test_percentile_ordering(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),
            ("R1", "U2", "2024-03-02", 3),
            ("R1", "U3", "2024-03-03", 5),
            ("R1", "U4", "2024-03-04", 10),
            ("R1", "U5", "2024-03-05", 20),
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        freq = calculate_report_frequency_metrics(user_df, 39, 5, _make_cfg())
        assert freq["median_views_per_user"] <= freq["p75_views_per_user"]
        assert freq["p75_views_per_user"] <= freq["p90_views_per_user"]
        assert freq["p90_views_per_user"] <= freq["max_views_per_user"]

    def test_single_user_distribution(self):
        """1 user: mean=median=p75=p90=max"""
        mart = _make_mart([("R1", "U1", "2024-03-15", 5)])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        freq = calculate_report_frequency_metrics(user_df, 5, 1, _make_cfg())
        assert freq["mean_views_per_user"] == 5.0
        assert freq["median_views_per_user"] == 5.0
        assert freq["p75_views_per_user"] == 5.0
        assert freq["p90_views_per_user"] == 5.0
        assert freq["max_views_per_user"] == 5.0

    def test_suppressed_distribution_is_null(self):
        cfg = FrequencyMetricsConfig(MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS=5)
        freq28 = {
            "mean_views_per_user_28d": 3.0,
            "median_views_per_user_28d": 2.0,
            "p75_views_per_user_28d": 4.0,
            "p90_views_per_user_28d": 5.0,
            "max_views_per_user_28d": 6.0,
            "mean_user_active_days_28d": 2.0,
            "median_user_active_days_28d": 1.5,
            "p75_user_active_days_28d": 3.0,
            "p90_user_active_days_28d": 4.0,
            "max_user_active_days_28d": 5.0,
            "mean_views_per_user_day_28d": 2.0,
            "median_views_per_user_day_28d": 1.5,
            "p90_views_per_user_day_28d": 3.0,
            "mean_return_gap_days_28d": 5.0,
            "median_return_gap_days_28d": 4.0,
            "returning_user_gap_observation_count_28d": 3,
        }
        freq_prev = {"median_views_per_user_previous_28d": 2.0,
                     "median_user_active_days_previous_28d": 1.5,
                     "median_return_gap_days_previous_28d": 4.0}
        comparison = {"median_views_per_user_change_28d": 0.5,
                      "median_user_active_days_change_28d": 0.1,
                      "median_return_gap_change_days_28d": -1.0}
        f28, fp28, comp, suppressed, reason, fields = apply_frequency_privacy_suppression(
            freq28, freq_prev, comparison, unique_users_28d=3, unique_users_prev_28d=3, cfg=cfg
        )
        assert suppressed is True
        assert f28["median_views_per_user_28d"] is None
        assert f28["median_user_active_days_28d"] is None


# ---------------------------------------------------------------------------
# TestReturnGaps
# ---------------------------------------------------------------------------

class TestReturnGaps:
    def _make_window_mart(self, rows):
        return _make_mart(rows)

    def test_user_with_consecutive_days(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),
            ("R1", "U1", "2024-03-02", 1),
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        mw = mart.copy()
        result = calculate_return_gaps(
            user_df, mw, "R1", date(2024, 3, 1), date(2024, 3, 31),
            FrequencyMetricsConfig(MIN_RETURNING_USERS_FOR_GAP_METRICS=1)
        )
        assert result["median_return_gap_days"] == 1.0

    def test_user_with_multi_day_gap(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),
            ("R1", "U1", "2024-03-05", 1),
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        result = calculate_return_gaps(
            user_df, mart, "R1", date(2024, 3, 1), date(2024, 3, 31),
            FrequencyMetricsConfig(MIN_RETURNING_USERS_FOR_GAP_METRICS=1)
        )
        assert result["median_return_gap_days"] == 4.0

    def test_multiple_qualifying_users_pooled(self):
        """2 users: [gap=1], [gap=3,5] → pool=[1,3,5], median=3"""
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),
            ("R1", "U1", "2024-03-02", 1),  # gap=1
            ("R1", "U2", "2024-03-01", 1),
            ("R1", "U2", "2024-03-04", 1),  # gap=3
            ("R1", "U2", "2024-03-09", 1),  # gap=5
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        result = calculate_return_gaps(
            user_df, mart, "R1", date(2024, 3, 1), date(2024, 3, 31),
            FrequencyMetricsConfig(MIN_RETURNING_USERS_FOR_GAP_METRICS=2)
        )
        # pool = [1, 3, 5] → median=3
        assert result["median_return_gap_days"] == 3.0
        assert result["returning_user_gap_observation_count"] == 3

    def test_one_time_users_excluded_from_gaps(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),  # one-time user
            ("R1", "U2", "2024-03-01", 1),  # one-time user
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        result = calculate_return_gaps(
            user_df, mart, "R1", date(2024, 3, 1), date(2024, 3, 31),
            FrequencyMetricsConfig(MIN_RETURNING_USERS_FOR_GAP_METRICS=1)
        )
        assert result["mean_return_gap_days"] is None

    def test_gaps_not_calculated_across_users(self):
        """User A ends 03-01, User B starts 03-02 — no cross-user gap."""
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),
            ("R1", "U1", "2024-03-05", 1),  # U1 gap=4
            ("R1", "U2", "2024-03-02", 1),
            ("R1", "U2", "2024-03-10", 1),  # U2 gap=8
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        result = calculate_return_gaps(
            user_df, mart, "R1", date(2024, 3, 1), date(2024, 3, 31),
            FrequencyMetricsConfig(MIN_RETURNING_USERS_FOR_GAP_METRICS=2)
        )
        # pool = [4, 8] → no cross-user gaps
        assert result["returning_user_gap_observation_count"] == 2
        assert result["mean_return_gap_days"] == 6.0

    def test_insufficient_returning_users(self):
        """Only 1 user with 2+ dates but cfg requires 2 → gaps null"""
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),
            ("R1", "U1", "2024-03-05", 1),
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        result = calculate_return_gaps(
            user_df, mart, "R1", date(2024, 3, 1), date(2024, 3, 31),
            FrequencyMetricsConfig(MIN_RETURNING_USERS_FOR_GAP_METRICS=2)
        )
        assert result["mean_return_gap_days"] is None
        assert result["median_return_gap_days"] is None

    def test_gap_observation_count_correct(self):
        """2 users: 3 dates + 2 dates → pool size = 2+1 = 3"""
        mart = _make_mart([
            ("R1", "U1", "2024-03-01", 1),
            ("R1", "U1", "2024-03-03", 1),
            ("R1", "U1", "2024-03-07", 1),
            ("R1", "U2", "2024-03-02", 1),
            ("R1", "U2", "2024-03-06", 1),
        ])
        user_df = aggregate_user_window_frequency(mart, "R1", date(2024, 3, 1), date(2024, 3, 31))
        result = calculate_return_gaps(
            user_df, mart, "R1", date(2024, 3, 1), date(2024, 3, 31),
            FrequencyMetricsConfig(MIN_RETURNING_USERS_FOR_GAP_METRICS=2)
        )
        assert result["returning_user_gap_observation_count"] == 3


# ---------------------------------------------------------------------------
# TestComparison
# ---------------------------------------------------------------------------

class TestComparison:
    def test_total_views_increase(self):
        recent = {"total_views": 100, "total_user_report_days": 50, "views_per_active_user": 10,
                  "views_per_user_day": 2, "median_views_per_user": 8, "median_user_active_days": 3,
                  "median_return_gap_days": 5}
        prev = {"total_views": 80, "total_user_report_days": 40, "views_per_active_user": 8,
                "views_per_user_day": 2, "median_views_per_user": 6, "median_user_active_days": 2,
                "median_return_gap_days": 6}
        result = calculate_frequency_change_metrics(recent, prev, comparison_sufficient=True)
        assert result["total_views_change_28d"] == 20

    def test_total_views_decrease(self):
        recent = {"total_views": 50, "total_user_report_days": 25, "views_per_active_user": 5,
                  "views_per_user_day": 2, "median_views_per_user": 4, "median_user_active_days": 1,
                  "median_return_gap_days": 7}
        prev = {"total_views": 100, "total_user_report_days": 50, "views_per_active_user": 10,
                "views_per_user_day": 2, "median_views_per_user": 8, "median_user_active_days": 3,
                "median_return_gap_days": 5}
        result = calculate_frequency_change_metrics(recent, prev, comparison_sufficient=True)
        assert result["total_views_change_28d"] == -50

    def test_stable_views(self):
        recent = {"total_views": 100, "total_user_report_days": 50, "views_per_active_user": 10,
                  "views_per_user_day": 2, "median_views_per_user": 8, "median_user_active_days": 3,
                  "median_return_gap_days": 5}
        prev = {"total_views": 100, "total_user_report_days": 50, "views_per_active_user": 10,
                "views_per_user_day": 2, "median_views_per_user": 8, "median_user_active_days": 3,
                "median_return_gap_days": 5}
        result = calculate_frequency_change_metrics(recent, prev, comparison_sufficient=True)
        assert result["total_views_change_28d"] == 0
        assert result["total_views_change_28d_pct"] == 0.0

    def test_increasing_from_zero_direction(self):
        cfg = _make_cfg()
        direction = classify_frequency_direction(
            recent_total_views=10,
            previous_total_views=0,
            comparison_sufficient=True,
            has_valid_data=True,
            cfg=cfg,
        )
        assert direction == "increasing_from_zero"

    def test_both_zero_inactive(self):
        cfg = _make_cfg()
        direction = classify_frequency_direction(
            recent_total_views=0,
            previous_total_views=0,
            comparison_sufficient=True,
            has_valid_data=True,
            cfg=cfg,
        )
        assert direction == "inactive"

    def test_previous_zero_pct_null(self):
        recent = {"total_views": 10, "total_user_report_days": 5, "views_per_active_user": 5,
                  "views_per_user_day": 2, "median_views_per_user": 4, "median_user_active_days": 1,
                  "median_return_gap_days": None}
        prev = {"total_views": 0, "total_user_report_days": 0, "views_per_active_user": 0,
                "views_per_user_day": 0, "median_views_per_user": None, "median_user_active_days": None,
                "median_return_gap_days": None}
        result = calculate_frequency_change_metrics(recent, prev, comparison_sufficient=True)
        assert result["total_views_change_28d_pct"] is None

    def test_median_active_days_decline(self):
        recent = {"total_views": 50, "total_user_report_days": 25, "views_per_active_user": 5,
                  "views_per_user_day": 2, "median_views_per_user": 4, "median_user_active_days": 1.0,
                  "median_return_gap_days": 7}
        prev = {"total_views": 80, "total_user_report_days": 40, "views_per_active_user": 8,
                "views_per_user_day": 2, "median_views_per_user": 6, "median_user_active_days": 3.0,
                "median_return_gap_days": 5}
        result = calculate_frequency_change_metrics(recent, prev, comparison_sufficient=True)
        assert result["median_user_active_days_change_28d"] == -2.0

    def test_views_per_user_increase(self):
        recent = {"total_views": 50, "total_user_report_days": 10, "views_per_active_user": 10.0,
                  "views_per_user_day": 5.0, "median_views_per_user": 8, "median_user_active_days": 2,
                  "median_return_gap_days": 5}
        prev = {"total_views": 30, "total_user_report_days": 10, "views_per_active_user": 6.0,
                "views_per_user_day": 3.0, "median_views_per_user": 5, "median_user_active_days": 2,
                "median_return_gap_days": 5}
        result = calculate_frequency_change_metrics(recent, prev, comparison_sufficient=True)
        assert result["views_per_active_user_change_28d"] == pytest.approx(4.0)

    def test_insufficient_comparison_history(self):
        recent = {"total_views": 100, "total_user_report_days": 50, "views_per_active_user": 10,
                  "views_per_user_day": 2, "median_views_per_user": 8, "median_user_active_days": 3,
                  "median_return_gap_days": 5}
        prev = {"total_views": 80, "total_user_report_days": 40, "views_per_active_user": 8,
                "views_per_user_day": 2, "median_views_per_user": 6, "median_user_active_days": 2,
                "median_return_gap_days": 6}
        result = calculate_frequency_change_metrics(recent, prev, comparison_sufficient=False)
        assert result["total_views_change_28d"] is None
        assert result["total_views_change_28d_pct"] is None
        assert result["median_views_per_user_change_28d"] is None


# ---------------------------------------------------------------------------
# TestClassification
# ---------------------------------------------------------------------------

class TestClassification:
    def _base_metrics_28d(self, **overrides):
        m = {
            "unique_users": 10,
            "median_user_active_days": 6.0,
            "median_views_per_user_day": 1.0,
            "_history_sufficient_7d": True,
        }
        m.update(overrides)
        return m

    def test_high_frequency(self):
        cfg = _make_cfg()
        metrics = self._base_metrics_28d(median_user_active_days=6.0)
        status, evidence, _ = classify_frequency_status(
            metrics_28d=metrics, comparison_metrics={},
            direction="stable", history_sufficient=True, has_valid_data=True,
            is_suppressed=False, cfg=cfg
        )
        assert status == "high_frequency"

    def test_moderate_frequency(self):
        cfg = _make_cfg()
        metrics = self._base_metrics_28d(median_user_active_days=3.0)
        status, _, _ = classify_frequency_status(
            metrics_28d=metrics, comparison_metrics={},
            direction="stable", history_sufficient=True, has_valid_data=True,
            is_suppressed=False, cfg=cfg
        )
        assert status == "moderate_frequency"

    def test_low_frequency(self):
        cfg = _make_cfg()
        metrics = self._base_metrics_28d(median_user_active_days=1.0)
        status, _, _ = classify_frequency_status(
            metrics_28d=metrics, comparison_metrics={},
            direction="stable", history_sufficient=True, has_valid_data=True,
            is_suppressed=False, cfg=cfg
        )
        assert status == "low_frequency"

    def test_occasional_but_consistent(self):
        cfg = _make_cfg()
        metrics = self._base_metrics_28d(median_user_active_days=1.7)
        status, _, _ = classify_frequency_status(
            metrics_28d=metrics, comparison_metrics={},
            direction="stable", history_sufficient=True, has_valid_data=True,
            is_suppressed=False, cfg=cfg
        )
        assert status == "occasional_but_consistent"

    def test_bursty_usage(self):
        cfg = _make_cfg()
        metrics = self._base_metrics_28d(
            median_views_per_user_day=5.0,
            median_user_active_days=1.5
        )
        status, _, _ = classify_frequency_status(
            metrics_28d=metrics, comparison_metrics={},
            direction="stable", history_sufficient=True, has_valid_data=True,
            is_suppressed=False, cfg=cfg
        )
        assert status == "bursty_usage"

    def test_declining_frequency(self):
        cfg = _make_cfg()
        metrics = self._base_metrics_28d(median_user_active_days=1.0)
        status, _, _ = classify_frequency_status(
            metrics_28d=metrics,
            comparison_metrics={"median_user_active_days_change_28d": -2.0},
            direction="decreasing", history_sufficient=True, has_valid_data=True,
            is_suppressed=False, cfg=cfg
        )
        assert status == "declining_frequency"

    def test_increasing_frequency(self):
        cfg = _make_cfg()
        metrics = self._base_metrics_28d(median_user_active_days=2.0)
        status, _, _ = classify_frequency_status(
            metrics_28d=metrics, comparison_metrics={},
            direction="increasing", history_sufficient=True, has_valid_data=True,
            is_suppressed=False, cfg=cfg
        )
        assert status == "increasing_frequency"

    def test_no_recent_activity(self):
        cfg = _make_cfg()
        metrics = self._base_metrics_28d(unique_users=0, median_user_active_days=None)
        status, _, _ = classify_frequency_status(
            metrics_28d=metrics, comparison_metrics={},
            direction="inactive", history_sufficient=True, has_valid_data=True,
            is_suppressed=False, cfg=cfg
        )
        assert status == "no_recent_activity"

    def test_privacy_suppressed_status(self):
        cfg = _make_cfg()
        metrics = self._base_metrics_28d(median_user_active_days=None)
        status, _, _ = classify_frequency_status(
            metrics_28d=metrics, comparison_metrics={},
            direction="stable", history_sufficient=True, has_valid_data=True,
            is_suppressed=True, cfg=cfg
        )
        assert status == "privacy_suppressed"

    def test_insufficient_history_status(self):
        cfg = _make_cfg()
        metrics = self._base_metrics_28d()
        status, evidence, _ = classify_frequency_status(
            metrics_28d=metrics, comparison_metrics={},
            direction="insufficient_history", history_sufficient=False, has_valid_data=True,
            is_suppressed=False, cfg=cfg
        )
        assert status == "insufficient_history"

    def test_no_valid_user_data_status(self):
        cfg = _make_cfg()
        metrics = self._base_metrics_28d()
        status, _, _ = classify_frequency_status(
            metrics_28d=metrics, comparison_metrics={},
            direction="no_valid_user_data", history_sufficient=True, has_valid_data=False,
            is_suppressed=False, cfg=cfg
        )
        assert status == "no_valid_user_data"

    def test_deterministic_classification(self):
        cfg = _make_cfg()
        metrics = self._base_metrics_28d(median_user_active_days=3.0)
        r1 = classify_frequency_status(
            metrics, {}, "stable", True, True, False, cfg
        )
        r2 = classify_frequency_status(
            metrics, {}, "stable", True, True, False, cfg
        )
        assert r1[0] == r2[0]
        assert r1[1] == r2[1]

    def test_deterministic_reason_order(self):
        cfg = _make_cfg()
        metrics = self._base_metrics_28d(median_user_active_days=3.0)
        _, _, reasons1 = classify_frequency_status(
            metrics, {}, "stable", True, True, False, cfg
        )
        _, _, reasons2 = classify_frequency_status(
            metrics, {}, "stable", True, True, False, cfg
        )
        assert reasons1 == reasons2


# ---------------------------------------------------------------------------
# TestPrivacySuppression
# ---------------------------------------------------------------------------

class TestPrivacySuppression:
    def _make_freq28(self, **overrides):
        base = {
            "mean_views_per_user_28d": 3.0,
            "median_views_per_user_28d": 2.0,
            "p75_views_per_user_28d": 4.0,
            "p90_views_per_user_28d": 5.0,
            "max_views_per_user_28d": 6.0,
            "mean_user_active_days_28d": 2.0,
            "median_user_active_days_28d": 1.5,
            "p75_user_active_days_28d": 3.0,
            "p90_user_active_days_28d": 4.0,
            "max_user_active_days_28d": 5.0,
            "mean_views_per_user_day_28d": 2.0,
            "median_views_per_user_day_28d": 1.5,
            "p90_views_per_user_day_28d": 3.0,
            "mean_return_gap_days_28d": 5.0,
            "median_return_gap_days_28d": 4.0,
            "returning_user_gap_observation_count_28d": 3,
        }
        base.update(overrides)
        return base

    def _make_freq_prev28(self):
        return {
            "median_views_per_user_previous_28d": 2.0,
            "median_user_active_days_previous_28d": 1.5,
            "median_return_gap_days_previous_28d": 4.0,
        }

    def _make_comparison(self):
        return {
            "median_views_per_user_change_28d": 0.5,
            "median_user_active_days_change_28d": 0.1,
            "median_return_gap_change_days_28d": -1.0,
        }

    def test_below_threshold_suppresses_distributions(self):
        cfg = FrequencyMetricsConfig(MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS=5)
        f28, _, _, suppressed, _, _ = apply_frequency_privacy_suppression(
            self._make_freq28(), self._make_freq_prev28(), self._make_comparison(),
            unique_users_28d=3, unique_users_prev_28d=3, cfg=cfg
        )
        assert suppressed is True
        assert f28["median_views_per_user_28d"] is None

    def test_exactly_threshold_not_suppressed(self):
        cfg = FrequencyMetricsConfig(MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS=5)
        f28, _, _, suppressed, _, _ = apply_frequency_privacy_suppression(
            self._make_freq28(), self._make_freq_prev28(), self._make_comparison(),
            unique_users_28d=5, unique_users_prev_28d=5, cfg=cfg
        )
        assert suppressed is False
        assert f28["median_views_per_user_28d"] == 2.0

    def test_total_views_not_suppressed(self):
        """total_views_28d is NOT in the suppression list — remains accessible."""
        cfg = FrequencyMetricsConfig(MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS=5)
        freq28 = dict(self._make_freq28())
        freq28["total_views_28d"] = 100
        f28, _, _, _, _, _ = apply_frequency_privacy_suppression(
            freq28, self._make_freq_prev28(), self._make_comparison(),
            unique_users_28d=2, unique_users_prev_28d=2, cfg=cfg
        )
        assert f28.get("total_views_28d") == 100

    def test_views_per_active_user_not_suppressed(self):
        cfg = FrequencyMetricsConfig(MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS=5)
        freq28 = dict(self._make_freq28())
        freq28["views_per_active_user_28d"] = 5.0
        f28, _, _, _, _, _ = apply_frequency_privacy_suppression(
            freq28, self._make_freq_prev28(), self._make_comparison(),
            unique_users_28d=2, unique_users_prev_28d=2, cfg=cfg
        )
        assert f28.get("views_per_active_user_28d") == 5.0

    def test_previous_distribution_suppressed_when_prev_below_threshold(self):
        cfg = FrequencyMetricsConfig(MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS=5)
        _, fp28, _, suppressed, _, _ = apply_frequency_privacy_suppression(
            self._make_freq28(), self._make_freq_prev28(), self._make_comparison(),
            unique_users_28d=5, unique_users_prev_28d=3, cfg=cfg  # prev below threshold
        )
        assert fp28["median_views_per_user_previous_28d"] is None

    def test_comparison_median_suppressed_when_distributions_suppressed(self):
        cfg = FrequencyMetricsConfig(MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS=5)
        _, _, comp, _, _, _ = apply_frequency_privacy_suppression(
            self._make_freq28(), self._make_freq_prev28(), self._make_comparison(),
            unique_users_28d=2, unique_users_prev_28d=2, cfg=cfg
        )
        assert comp["median_views_per_user_change_28d"] is None
        assert comp["median_user_active_days_change_28d"] is None
        assert comp["median_return_gap_change_days_28d"] is None

    def test_suppressed_fields_metadata_deterministic(self):
        cfg = FrequencyMetricsConfig(MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS=5)
        _, _, _, _, _, fields1 = apply_frequency_privacy_suppression(
            self._make_freq28(), self._make_freq_prev28(), self._make_comparison(),
            unique_users_28d=2, unique_users_prev_28d=2, cfg=cfg
        )
        _, _, _, _, _, fields2 = apply_frequency_privacy_suppression(
            self._make_freq28(), self._make_freq_prev28(), self._make_comparison(),
            unique_users_28d=2, unique_users_prev_28d=2, cfg=cfg
        )
        assert fields1 == fields2

    def test_no_user_list_in_output(self):
        """Output DataFrame must not contain list-type columns."""
        mart = _make_mart([("R1", "U1", "2024-03-15", 1), ("R1", "U2", "2024-03-15", 2)])
        suf = _make_sufficiency([{"report_id": "R1"}])
        qual = _make_quality([{"report_id": "R1"}])
        bounds = _make_boundaries()
        result = build_report_frequency_metrics(suf, mart, qual, bounds, _make_cfg(), _run_id())
        for col in result.columns:
            assert not result[col].apply(lambda x: isinstance(x, (list, set, tuple))).any()

    def test_no_direct_identifiers(self):
        mart = _make_mart([("R1", "U1", "2024-03-15", 1)])
        suf = _make_sufficiency([{"report_id": "R1"}])
        qual = _make_quality([{"report_id": "R1"}])
        bounds = _make_boundaries()
        result = build_report_frequency_metrics(suf, mart, qual, bounds, _make_cfg(), _run_id())
        validate_no_direct_identifiers(result)


# ---------------------------------------------------------------------------
# TestValidation
# ---------------------------------------------------------------------------

class TestValidation:
    def _make_valid_row(self, report_id="R1", run_id="test-run"):
        row = {col: None for col in REPORT_USER_FREQUENCY_METRICS_COLS}
        row.update({
            "analytics_run_id": run_id,
            "generated_at": "2024-04-01T00:00:00",
            "analytics_as_of_date": "2024-03-31",
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
            "privacy_suppression_status": "not_suppressed",
            "total_views_28d": 10,
            "total_user_report_days_28d": 5,
            "unique_users_28d": 5,
            "views_per_active_user_28d": 2.0,
            "views_per_user_day_28d": 2.0,
            "median_views_per_user_28d": 2.0,
            "p75_views_per_user_28d": 3.0,
            "p90_views_per_user_28d": 4.0,
            "max_views_per_user_28d": 5.0,
            "median_user_active_days_28d": 1.0,
            "p75_user_active_days_28d": 1.0,
            "p90_user_active_days_28d": 1.0,
            "max_user_active_days_28d": 1.0,
            "frequency_direction": "stable",
            "frequency_status": "low_frequency",
            "frequency_evidence_status": "sufficient",
            "frequency_reasons": "none",
            "frequency_privacy_suppressed": False,
            "frequency_privacy_suppression_reason": None,
            "suppressed_frequency_fields": None,
        })
        return row

    def _make_valid_df(self, report_id="R1"):
        return pd.DataFrame([self._make_valid_row(report_id)])

    def test_negative_views_rejected(self):
        df = self._make_valid_df()
        df["total_views_28d"] = -1
        with pytest.raises(ValueError, match="Negative"):
            validate_report_frequency_metrics(df)

    def test_user_report_days_below_unique_users_rejected(self):
        df = self._make_valid_df()
        df["total_user_report_days_28d"] = 2
        df["unique_users_28d"] = 5
        with pytest.raises(ValueError):
            validate_report_frequency_metrics(df)

    def test_invalid_percentile_order_rejected(self):
        df = self._make_valid_df()
        df["p75_views_per_user_28d"] = 10.0
        df["p90_views_per_user_28d"] = 5.0  # p90 < p75
        df["max_views_per_user_28d"] = 20.0
        with pytest.raises(ValueError):
            validate_report_frequency_metrics(df)

    def test_active_days_exceed_window_length_rejected(self):
        df = self._make_valid_df()
        df["p90_user_active_days_28d"] = 29.0  # > 28
        with pytest.raises(ValueError):
            validate_report_frequency_metrics(df)

    def test_nonzero_ratio_with_zero_denominator_rejected(self):
        df = self._make_valid_df()
        df["total_views_previous_28d"] = 0
        df["total_views_change_28d_pct"] = 0.5  # pct should be null when prev=0
        with pytest.raises(ValueError):
            validate_report_frequency_metrics(df)

    def test_invalid_status_rejected(self):
        df = self._make_valid_df()
        df["frequency_status"] = "not_a_valid_status"
        with pytest.raises(ValueError):
            validate_report_frequency_metrics(df)

    def test_duplicate_report_rows_rejected(self):
        row = self._make_valid_row()
        df = pd.DataFrame([row, row])
        with pytest.raises(ValueError, match="Duplicate"):
            validate_report_frequency_metrics(df)

    def test_suppression_inconsistency_rejected(self):
        df = self._make_valid_df()
        df["frequency_privacy_suppressed"] = True
        df["median_views_per_user_28d"] = 3.0  # should be null when suppressed
        with pytest.raises(ValueError):
            validate_report_frequency_metrics(df)


# ---------------------------------------------------------------------------
# TestPersistence
# ---------------------------------------------------------------------------

class TestPersistence:
    def _build_df(self):
        mart = _make_mart([
            ("R1", "U1", "2024-03-15", 2),
            ("R1", "U2", "2024-03-16", 3),
            ("R1", "U3", "2024-03-17", 1),
            ("R1", "U4", "2024-03-18", 4),
            ("R1", "U5", "2024-03-19", 2),
        ])
        suf = _make_sufficiency([{"report_id": "R1"}])
        qual = _make_quality([{"report_id": "R1"}])
        bounds = _make_boundaries()
        return build_report_frequency_metrics(suf, mart, qual, bounds, _make_cfg(), _run_id())

    def test_output_file_created(self, tmp_path):
        df = self._build_df()
        out = persist_report_frequency_metrics(df, tmp_path)
        assert out.exists()

    def test_schema_stable(self, tmp_path):
        df = self._build_df()
        out = persist_report_frequency_metrics(df, tmp_path)
        loaded = pd.read_csv(out)
        assert list(loaded.columns) == REPORT_USER_FREQUENCY_METRICS_COLS

    def test_deterministic_sorting(self, tmp_path):
        mart = _make_mart([
            ("R3", "U1", "2024-03-15", 1),
            ("R1", "U2", "2024-03-15", 1),
            ("R2", "U3", "2024-03-15", 1),
        ])
        suf = _make_sufficiency([
            {"report_id": "R3"},
            {"report_id": "R1"},
            {"report_id": "R2"},
        ])
        qual = _make_quality([{"report_id": "R1"}, {"report_id": "R2"}, {"report_id": "R3"}])
        bounds = _make_boundaries()
        df = build_report_frequency_metrics(suf, mart, qual, bounds, _make_cfg(), _run_id())
        out = persist_report_frequency_metrics(df, tmp_path)
        loaded = pd.read_csv(out)
        assert list(loaded["report_id"]) == ["R1", "R2", "R3"]

    def test_latest_file_replaced(self, tmp_path):
        df = self._build_df()
        out1 = persist_report_frequency_metrics(df, tmp_path)
        out2 = persist_report_frequency_metrics(df, tmp_path)
        assert out1 == out2
        assert out2.exists()

    def test_source_mart_unchanged(self, tmp_path):
        mart = _make_mart([("R1", "U1", "2024-03-15", 2)])
        original_shape = mart.shape
        suf = _make_sufficiency([{"report_id": "R1"}])
        qual = _make_quality([{"report_id": "R1"}])
        bounds = _make_boundaries()
        build_report_frequency_metrics(suf, mart, qual, bounds, _make_cfg(), _run_id())
        assert mart.shape == original_shape

    def test_invalid_output_rejected_before_writing(self, tmp_path):
        df = self._build_df()
        df["total_views_28d"] = -1  # invalid
        with pytest.raises(ValueError):
            persist_report_frequency_metrics(df, tmp_path)
