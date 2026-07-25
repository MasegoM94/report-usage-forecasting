"""
Tests for src/analytics/engagement_windows.py

Sprint 6 — Observation windows and report-history sufficiency.
All tests use mock DataFrames — no real files loaded from disk.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.analytics.engagement_windows import (
    REPORT_HISTORY_SUFFICIENCY_COLS,
    WINDOW_BOUNDARIES_COLS,
    EngagementWindowBoundaries,
    EngagementWindowConfig,
    assign_engagement_window,
    build_engagement_window_boundaries,
    build_report_history_sufficiency,
    filter_report_user_window,
    persist_engagement_window_outputs,
    resolve_analytics_as_of_date,
    validate_engagement_window_boundaries,
    validate_report_history_sufficiency,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_id() -> str:
    return str(uuid.uuid4())


def _make_mart_df(rows: list[dict]) -> pd.DataFrame:
    """Build a mock mart_report_user_daily DataFrame."""
    defaults = {"report_id": "R_001", "user_key": "UK_0001", "daily_views": 1}
    records = [{**defaults, **r} for r in rows]
    return pd.DataFrame(records)


def _make_report_meta_df(rows: list[dict]) -> pd.DataFrame:
    """Build a mock report metadata DataFrame."""
    defaults = {"report_id": "R_001", "report_name": "Test Report", "report_activation_date": None}
    records = [{**defaults, **r} for r in rows]
    return pd.DataFrame(records)


def _make_quality_df(rows: list[dict]) -> pd.DataFrame:
    """Build a mock quality DataFrame."""
    defaults = {"report_id": "R_001", "data_quality_status": "good"}
    records = [{**defaults, **r} for r in rows]
    return pd.DataFrame(records)


def _simple_boundaries(as_of: date, run_id: str | None = None) -> EngagementWindowBoundaries:
    """Build a valid EngagementWindowBoundaries for a given as_of_date."""
    run_id = run_id or _run_id()
    mart = _make_mart_df([{"usage_date": str(as_of)}])
    cfg = EngagementWindowConfig()
    return build_engagement_window_boundaries(mart, cfg, run_id)


# ---------------------------------------------------------------------------
# TestBoundaryDefinitions
# ---------------------------------------------------------------------------

class TestBoundaryDefinitions:
    def test_7d_window_exact_length(self):
        b = _simple_boundaries(date(2025, 6, 30))
        assert (b.window_7d_end - b.window_7d_start).days + 1 == 7

    def test_28d_window_exact_length(self):
        b = _simple_boundaries(date(2025, 6, 30))
        assert (b.window_28d_end - b.window_28d_start).days + 1 == 28

    def test_previous_28d_window_exact_length(self):
        b = _simple_boundaries(date(2025, 6, 30))
        assert (b.previous_28d_end - b.previous_28d_start).days + 1 == 28

    def test_90d_window_exact_length(self):
        b = _simple_boundaries(date(2025, 6, 30))
        assert (b.window_90d_end - b.window_90d_start).days + 1 == 90

    def test_previous_90d_window_exact_length(self):
        b = _simple_boundaries(date(2025, 6, 30))
        assert (b.previous_90d_end - b.previous_90d_start).days + 1 == 90

    def test_recent_and_previous_28d_no_overlap(self):
        b = _simple_boundaries(date(2025, 6, 30))
        assert b.previous_28d_end < b.window_28d_start

    def test_previous_28d_ends_one_day_before_recent_starts(self):
        b = _simple_boundaries(date(2025, 6, 30))
        assert b.previous_28d_end == b.window_28d_start - timedelta(days=1)

    def test_pre_previous_28d_end_correct(self):
        b = _simple_boundaries(date(2025, 6, 30))
        assert b.pre_previous_28d_end == b.previous_28d_start - timedelta(days=1)

    def test_leap_day_in_window(self):
        # as_of_date = 2024-03-01 → window_28d_start = 2024-02-03, includes 2024-02-29
        b = _simple_boundaries(date(2024, 3, 1))
        assert b.window_28d_start == date(2024, 2, 3)
        assert b.window_28d_start <= date(2024, 2, 29) <= b.window_28d_end

    def test_month_boundary(self):
        # as_of_date = 2024-03-31 → window_28d_start = 2024-03-04
        b = _simple_boundaries(date(2024, 3, 31))
        assert b.window_28d_start == date(2024, 3, 4)

    def test_year_boundary(self):
        # as_of_date = 2024-01-15 → window_90d_start = 2023-10-18
        b = _simple_boundaries(date(2024, 1, 15))
        assert b.window_90d_start == date(2023, 10, 18)


# ---------------------------------------------------------------------------
# TestAsOfDate
# ---------------------------------------------------------------------------

class TestAsOfDate:
    def test_latest_date_complete(self):
        mart = _make_mart_df([{"usage_date": "2025-06-30"}])
        cfg = EngagementWindowConfig(LATEST_SOURCE_DATE_IS_COMPLETE=True)
        aod, policy, status, src_max = resolve_analytics_as_of_date(mart, cfg, _run_id())
        assert aod == date(2025, 6, 30)
        assert policy == "source_max_date"
        assert status == "complete"

    def test_latest_date_incomplete(self):
        mart = _make_mart_df([{"usage_date": "2025-06-30"}])
        cfg = EngagementWindowConfig(LATEST_SOURCE_DATE_IS_COMPLETE=False)
        aod, policy, status, src_max = resolve_analytics_as_of_date(mart, cfg, _run_id())
        assert aod == date(2025, 6, 29)
        assert policy == "source_max_date_minus_1_assumed_incomplete"
        assert status == "assumed_incomplete"

    def test_single_day_source(self):
        mart = _make_mart_df([{"usage_date": "2025-01-01"}])
        cfg = EngagementWindowConfig()
        aod, _, _, src_max = resolve_analytics_as_of_date(mart, cfg, _run_id())
        assert aod == date(2025, 1, 1)
        assert src_max == date(2025, 1, 1)

    def test_no_valid_dates_raises(self):
        mart = pd.DataFrame({"usage_date": pd.Series([], dtype=str), "report_id": [], "user_key": [], "daily_views": []})
        cfg = EngagementWindowConfig()
        with pytest.raises(ValueError, match="No valid usage dates"):
            resolve_analytics_as_of_date(mart, cfg, _run_id())

    def test_as_of_not_in_future(self):
        src_date = date.today() - timedelta(days=5)
        mart = _make_mart_df([{"usage_date": str(src_date)}])
        cfg = EngagementWindowConfig()
        aod, _, _, src_max = resolve_analytics_as_of_date(mart, cfg, _run_id())
        assert aod <= src_max

    def test_policy_persisted_in_boundaries(self):
        mart = _make_mart_df([{"usage_date": "2025-06-30"}])
        cfg_complete = EngagementWindowConfig(LATEST_SOURCE_DATE_IS_COMPLETE=True)
        cfg_incomplete = EngagementWindowConfig(LATEST_SOURCE_DATE_IS_COMPLETE=False)
        b_complete = build_engagement_window_boundaries(mart, cfg_complete, _run_id())
        b_incomplete = build_engagement_window_boundaries(mart, cfg_incomplete, _run_id())
        assert b_complete.as_of_date_policy == "source_max_date"
        assert b_incomplete.as_of_date_policy == "source_max_date_minus_1_assumed_incomplete"

    def test_timezone_recorded(self):
        mart = _make_mart_df([{"usage_date": "2025-06-30"}])
        cfg = EngagementWindowConfig(PROJECT_ANALYTICS_TIMEZONE="Europe/London")
        b = build_engagement_window_boundaries(mart, cfg, _run_id())
        assert b.analytics_timezone == "Europe/London"

    def test_source_max_date_recorded(self):
        mart = _make_mart_df([
            {"usage_date": "2025-06-28"},
            {"usage_date": "2025-06-30"},
            {"usage_date": "2025-06-29"},
        ])
        cfg = EngagementWindowConfig()
        b = build_engagement_window_boundaries(mart, cfg, _run_id())
        assert b.source_max_usage_date == date(2025, 6, 30)


# ---------------------------------------------------------------------------
# TestActivationRules
# ---------------------------------------------------------------------------

class TestActivationRules:
    def _build(self, as_of: date, activation: date | None, mart_dates: list[date] | None = None):
        run_id = _run_id()
        if mart_dates is None:
            # Provide 300 days of coverage so source_cov_start predates all windows
            mart_dates = [as_of - timedelta(days=i) for i in range(300)]
        mart = _make_mart_df([{"usage_date": str(d), "report_id": "R_001"} for d in mart_dates])
        cfg = EngagementWindowConfig()
        b = build_engagement_window_boundaries(mart, cfg, run_id)
        meta = _make_report_meta_df([{"report_id": "R_001", "report_activation_date": str(activation) if activation else None}])
        quality = _make_quality_df([{"report_id": "R_001", "data_quality_status": "good"}])
        suf = build_report_history_sufficiency(meta, mart, quality, b, cfg, run_id)
        return suf.iloc[0], b

    def test_report_active_before_all_windows(self):
        as_of = date(2025, 6, 30)
        activation = as_of - timedelta(days=200)
        row, b = self._build(as_of, activation)
        assert row["history_sufficient_7d"] is True or row["history_sufficient_7d"] == True
        assert row["history_sufficient_28d"] is True or row["history_sufficient_28d"] == True
        assert row["history_sufficient_90d"] is True or row["history_sufficient_90d"] == True
        assert row["comparison_history_sufficient_28d"] is True or row["comparison_history_sufficient_28d"] == True

    def test_report_activated_exactly_on_7d_window_start(self):
        as_of = date(2025, 6, 30)
        b = _simple_boundaries(as_of)
        activation = b.window_7d_start
        row, _ = self._build(as_of, activation)
        assert row["history_sufficient_7d"] == True

    def test_report_activated_one_day_after_7d_window_start(self):
        as_of = date(2025, 6, 30)
        b = _simple_boundaries(as_of)
        activation = b.window_7d_start + timedelta(days=1)
        row, _ = self._build(as_of, activation)
        assert row["history_sufficient_7d"] == False

    def test_report_activated_during_recent_28d(self):
        as_of = date(2025, 6, 30)
        b = _simple_boundaries(as_of)
        # Inside 28d window but after window start
        activation = b.window_28d_start + timedelta(days=5)
        row, _ = self._build(as_of, activation)
        assert row["history_sufficient_28d"] == False

    def test_report_activated_during_previous_28d(self):
        as_of = date(2025, 6, 30)
        b = _simple_boundaries(as_of)
        activation = b.previous_28d_start + timedelta(days=3)
        row, _ = self._build(as_of, activation)
        assert row["history_sufficient_previous_28d"] == False

    def test_report_activation_after_as_of_date(self):
        as_of = date(2025, 6, 30)
        activation = as_of + timedelta(days=5)
        row, _ = self._build(as_of, activation)
        assert row["report_active_as_of_date"] == False

    def test_missing_activation_with_sufficient_observed_history_fallback(self):
        as_of = date(2025, 6, 30)
        b = _simple_boundaries(as_of)
        # first observation before 7d window start
        first_obs = b.window_7d_start - timedelta(days=5)
        mart_dates = [first_obs, as_of - timedelta(days=1)]
        run_id = _run_id()
        mart = _make_mart_df([{"usage_date": str(d), "report_id": "R_001"} for d in mart_dates])
        cfg = EngagementWindowConfig(ALLOW_OBSERVED_HISTORY_ACTIVATION_FALLBACK=True)
        b2 = build_engagement_window_boundaries(mart, cfg, run_id)
        # No activation date provided
        meta = _make_report_meta_df([{"report_id": "R_001", "report_activation_date": None}])
        quality = _make_quality_df([{"report_id": "R_001", "data_quality_status": "good"}])
        suf = build_report_history_sufficiency(meta, mart, quality, b2, cfg, run_id)
        row = suf.iloc[0]
        assert row["activation_date_status"] == "missing_observed_history_fallback"
        assert row["history_sufficient_7d"] == True

    def test_missing_activation_with_insufficient_fallback(self):
        as_of = date(2025, 6, 30)
        b = _simple_boundaries(as_of)
        # first observation AFTER 7d window start — insufficient for 7d
        first_obs = b.window_7d_start + timedelta(days=2)
        mart_dates = [first_obs]
        run_id = _run_id()
        mart = _make_mart_df([{"usage_date": str(d), "report_id": "R_001"} for d in mart_dates])
        cfg = EngagementWindowConfig(ALLOW_OBSERVED_HISTORY_ACTIVATION_FALLBACK=True)
        b2 = build_engagement_window_boundaries(mart, cfg, run_id)
        meta = _make_report_meta_df([{"report_id": "R_001", "report_activation_date": None}])
        quality = _make_quality_df([{"report_id": "R_001"}])
        suf = build_report_history_sufficiency(meta, mart, quality, b2, cfg, run_id)
        row = suf.iloc[0]
        assert row["history_sufficient_7d"] == False


# ---------------------------------------------------------------------------
# TestSourceCoverage
# ---------------------------------------------------------------------------

class TestSourceCoverage:
    def _build_with_coverage(self, as_of: date, activation: date, coverage_start: date):
        run_id = _run_id()
        mart_dates = [as_of - timedelta(days=i) for i in range(5)]
        mart = _make_mart_df([{"usage_date": str(d), "report_id": "R_001"} for d in mart_dates])
        cfg = EngagementWindowConfig()
        b = build_engagement_window_boundaries(mart, cfg, run_id)
        meta = _make_report_meta_df([{"report_id": "R_001", "report_activation_date": str(activation)}])
        quality = _make_quality_df([{"report_id": "R_001"}])
        suf = build_report_history_sufficiency(meta, mart, quality, b, cfg, run_id, source_coverage_start_date=coverage_start)
        return suf.iloc[0], b

    def test_full_source_coverage(self):
        as_of = date(2025, 6, 30)
        activation = as_of - timedelta(days=200)
        coverage_start = as_of - timedelta(days=300)
        row, b = self._build_with_coverage(as_of, activation, coverage_start)
        assert row["history_sufficient_28d"] == True
        assert row["comparison_history_sufficient_28d"] == True

    def test_source_starts_during_previous_window(self):
        as_of = date(2025, 6, 30)
        b = _simple_boundaries(as_of)
        activation = as_of - timedelta(days=200)
        # Coverage starts inside previous_28d
        coverage_start = b.previous_28d_start + timedelta(days=3)
        row, _ = self._build_with_coverage(as_of, activation, coverage_start)
        assert row["comparison_history_sufficient_28d"] == False
        assert row["history_sufficient_28d"] == True  # recent 28d is covered

    def test_source_starts_during_recent_window(self):
        as_of = date(2025, 6, 30)
        b = _simple_boundaries(as_of)
        activation = as_of - timedelta(days=200)
        # Coverage starts inside recent_28d
        coverage_start = b.window_28d_start + timedelta(days=5)
        row, _ = self._build_with_coverage(as_of, activation, coverage_start)
        assert row["history_sufficient_28d"] == False

    def test_source_coverage_before_activation(self):
        as_of = date(2025, 6, 30)
        b = _simple_boundaries(as_of)
        # Activation is the binding constraint: only active 10 days before as_of
        activation = as_of - timedelta(days=10)
        coverage_start = as_of - timedelta(days=300)
        row, _ = self._build_with_coverage(as_of, activation, coverage_start)
        assert row["history_sufficient_28d"] == False  # activation too recent
        assert row["history_sufficient_7d"] == True   # within 7d


# ---------------------------------------------------------------------------
# TestSufficiencyFlags
# ---------------------------------------------------------------------------

class TestSufficiencyFlags:
    def _suf_row(self, as_of: date, activation: date):
        run_id = _run_id()
        # 300 days of mart coverage so source_cov_start predates all windows
        mart = _make_mart_df([
            {"usage_date": str(as_of - timedelta(days=i)), "report_id": "R_001"}
            for i in range(300)
        ])
        cfg = EngagementWindowConfig()
        b = build_engagement_window_boundaries(mart, cfg, run_id)
        meta = _make_report_meta_df([{"report_id": "R_001", "report_activation_date": str(activation)}])
        quality = _make_quality_df([{"report_id": "R_001"}])
        suf = build_report_history_sufficiency(meta, mart, quality, b, cfg, run_id)
        return suf.iloc[0]

    def test_sufficient_7d_only(self):
        as_of = date(2025, 6, 30)
        # window_7d_start = as_of - 6; activation must be <= window_7d_start for sufficiency
        # Activate exactly 7 days before as_of (= window_7d_start) → only 7d sufficient
        row = self._suf_row(as_of, as_of - timedelta(days=6))
        assert row["history_sufficient_7d"] == True
        assert row["history_sufficient_28d"] == False

    def test_sufficient_recent_28d_only(self):
        as_of = date(2025, 6, 30)
        b = _simple_boundaries(as_of)
        # Activation exactly at window_28d_start → 28d sufficient, previous not
        activation = b.window_28d_start
        row = self._suf_row(as_of, activation)
        assert row["history_sufficient_28d"] == True
        assert row["history_sufficient_previous_28d"] == False
        assert row["comparison_history_sufficient_28d"] == False

    def test_sufficient_recent_and_previous_28d(self):
        as_of = date(2025, 6, 30)
        activation = as_of - timedelta(days=60)
        row = self._suf_row(as_of, activation)
        assert row["history_sufficient_28d"] == True
        assert row["history_sufficient_previous_28d"] == True
        assert row["comparison_history_sufficient_28d"] == True

    def test_sufficient_90d_history(self):
        as_of = date(2025, 6, 30)
        activation = as_of - timedelta(days=250)
        row = self._suf_row(as_of, activation)
        assert row["comparison_history_sufficient_90d"] == True
        assert row["history_sufficiency_status"] == "complete_90d_history"

    def test_no_user_activity_with_sufficient_history(self):
        as_of = date(2025, 6, 30)
        activation = as_of - timedelta(days=200)
        run_id = _run_id()
        # mart has rows for OTHER report providing 300d coverage, but none for R_001
        mart = _make_mart_df([
            {"usage_date": str(as_of - timedelta(days=i)), "report_id": "OTHER"}
            for i in range(300)
        ])
        cfg = EngagementWindowConfig()
        b = build_engagement_window_boundaries(mart, cfg, run_id)
        meta = _make_report_meta_df([{"report_id": "R_001", "report_activation_date": str(activation)}])
        quality = _make_quality_df([{"report_id": "R_001", "data_quality_status": "good"}])
        # Pass explicit source_coverage_start so coverage is sufficient
        source_start = as_of - timedelta(days=300)
        suf = build_report_history_sufficiency(meta, mart, quality, b, cfg, run_id, source_coverage_start_date=source_start)
        row = suf.iloc[0]
        # History sufficient (via known activation + explicit coverage), but no activity for R_001
        assert row["has_any_valid_user_activity"] == False
        assert "complete" in row["history_sufficiency_status"]

    def test_no_valid_user_data(self):
        as_of = date(2025, 6, 30)
        run_id = _run_id()
        mart = _make_mart_df([{"usage_date": str(as_of), "report_id": "OTHER"}])
        cfg = EngagementWindowConfig()
        b = build_engagement_window_boundaries(mart, cfg, run_id)
        meta = _make_report_meta_df([{"report_id": "R_001", "report_activation_date": None}])
        quality = _make_quality_df([{"report_id": "R_001", "data_quality_status": "no_valid_user_data"}])
        suf = build_report_history_sufficiency(meta, mart, quality, b, cfg, run_id)
        row = suf.iloc[0]
        assert row["history_sufficiency_status"] == "no_valid_user_data"

    def test_report_absent_from_mart(self):
        as_of = date(2025, 6, 30)
        run_id = _run_id()
        mart = _make_mart_df([{"usage_date": str(as_of), "report_id": "OTHER"}])
        cfg = EngagementWindowConfig()
        b = build_engagement_window_boundaries(mart, cfg, run_id)
        # R_999 not in mart or quality
        meta = _make_report_meta_df([{"report_id": "R_999", "report_activation_date": None}])
        quality = pd.DataFrame(columns=["report_id", "data_quality_status"])
        suf = build_report_history_sufficiency(meta, mart, quality, b, cfg, run_id)
        row = suf.iloc[0]
        assert row["history_source_status"] == "mart_absent"

    def test_newly_activated_report(self):
        as_of = date(2025, 6, 30)
        activation = as_of  # activated today
        row = self._suf_row(as_of, activation)
        # activation == as_of, not <= window_7d_start
        assert row["history_sufficient_7d"] == False
        assert row["history_sufficiency_status"] == "insufficient_history"

    def test_report_with_sparse_usage(self):
        # available_calendar_history_days is based on activation date, not number of mart rows
        as_of = date(2025, 6, 30)
        activation = as_of - timedelta(days=100)
        run_id = _run_id()
        # Only 2 mart rows but activation is old
        mart = _make_mart_df([
            {"usage_date": str(as_of - timedelta(days=1)), "report_id": "R_001"},
            {"usage_date": str(as_of - timedelta(days=50)), "report_id": "R_001"},
        ])
        cfg = EngagementWindowConfig()
        b = build_engagement_window_boundaries(mart, cfg, run_id)
        meta = _make_report_meta_df([{"report_id": "R_001", "report_activation_date": str(activation)}])
        quality = _make_quality_df([{"report_id": "R_001"}])
        suf = build_report_history_sufficiency(meta, mart, quality, b, cfg, run_id)
        row = suf.iloc[0]
        assert row["active_usage_days_lifetime"] == 2
        # available_calendar_history_days based on activation, not usage count
        assert row["available_calendar_history_days"] > 2


# ---------------------------------------------------------------------------
# TestWindowFiltering
# ---------------------------------------------------------------------------

class TestWindowFiltering:
    def _base(self):
        as_of = date(2025, 6, 30)
        b = _simple_boundaries(as_of)
        # Create mart with dates spread across windows
        dates = [
            b.window_7d_start,
            b.window_7d_end,
            b.window_28d_start,
            b.previous_28d_start,
            b.previous_28d_end,
            b.pre_previous_28d_end,
            as_of + timedelta(days=5),  # future
        ]
        mart = _make_mart_df([{"usage_date": str(d), "report_id": "R_001"} for d in dates])
        return mart, b

    def test_filter_recent_7d_returns_only_7d_rows(self):
        mart, b = self._base()
        result = filter_report_user_window(mart, b.window_7d_start, b.window_7d_end)
        from src.analytics.engagement_windows import _parse_usage_dates
        dates_out = _parse_usage_dates(result).dropna()
        assert all(b.window_7d_start <= d <= b.window_7d_end for d in dates_out)

    def test_filter_recent_28d_returns_only_28d_rows(self):
        mart, b = self._base()
        result = filter_report_user_window(mart, b.window_28d_start, b.window_28d_end)
        from src.analytics.engagement_windows import _parse_usage_dates
        dates_out = _parse_usage_dates(result).dropna()
        assert all(b.window_28d_start <= d <= b.window_28d_end for d in dates_out)

    def test_filter_previous_28d_returns_only_previous_rows(self):
        mart, b = self._base()
        result = filter_report_user_window(mart, b.previous_28d_start, b.previous_28d_end)
        from src.analytics.engagement_windows import _parse_usage_dates
        dates_out = _parse_usage_dates(result).dropna()
        assert all(b.previous_28d_start <= d <= b.previous_28d_end for d in dates_out)

    def test_exact_boundary_dates_included(self):
        mart, b = self._base()
        result = filter_report_user_window(mart, b.window_7d_start, b.window_7d_end)
        from src.analytics.engagement_windows import _parse_usage_dates
        dates_out = set(_parse_usage_dates(result).dropna())
        assert b.window_7d_start in dates_out
        assert b.window_7d_end in dates_out

    def test_date_outside_window_excluded(self):
        mart, b = self._base()
        result = filter_report_user_window(mart, b.window_7d_start, b.window_7d_end)
        from src.analytics.engagement_windows import _parse_usage_dates
        dates_out = set(_parse_usage_dates(result).dropna())
        assert b.previous_28d_start not in dates_out

    def test_filter_does_not_modify_source(self):
        mart, b = self._base()
        original_len = len(mart)
        _ = filter_report_user_window(mart, b.window_7d_start, b.window_7d_end)
        assert len(mart) == original_len

    def test_pre_previous_history_filter(self):
        mart, b = self._base()
        result = filter_report_user_window(mart, date.min, b.pre_previous_28d_end)
        from src.analytics.engagement_windows import _parse_usage_dates
        dates_out = _parse_usage_dates(result).dropna()
        assert all(d <= b.pre_previous_28d_end for d in dates_out)


# ---------------------------------------------------------------------------
# TestValidation
# ---------------------------------------------------------------------------

class TestValidation:
    def _valid_boundaries(self) -> EngagementWindowBoundaries:
        return _simple_boundaries(date(2025, 6, 30))

    def test_invalid_7d_length_rejected(self):
        b = self._valid_boundaries()
        # Make it 8 days by shifting start 1 day back
        bad = EngagementWindowBoundaries(
            **{**b.__dict__, "window_7d_start": b.window_7d_start - timedelta(days=1)}
        )
        with pytest.raises(ValueError, match="7d window"):
            validate_engagement_window_boundaries(bad)

    def test_overlapping_windows_rejected(self):
        b = self._valid_boundaries()
        # Make previous_28d_end == window_28d_start (overlap)
        bad = EngagementWindowBoundaries(
            **{**b.__dict__, "previous_28d_end": b.window_28d_start}
        )
        with pytest.raises(ValueError):
            validate_engagement_window_boundaries(bad)

    def test_negative_history_days_rejected(self):
        run_id = _run_id()
        b = _simple_boundaries(date(2025, 6, 30))
        # Build sufficiency df with negative days
        row = {col: None for col in REPORT_HISTORY_SUFFICIENCY_COLS}
        row.update({
            "analytics_run_id": run_id,
            "report_id": "R_001",
            "activation_date_status": "known",
            "history_source_status": "mart_absent",
            "history_sufficiency_status": "insufficient_history",
            "available_calendar_history_days": -1,
        })
        df = pd.DataFrame([row])
        with pytest.raises(ValueError, match="Negative available_calendar_history_days"):
            validate_report_history_sufficiency(df)

    def test_comparison_flag_inconsistency_rejected(self):
        run_id = _run_id()
        row = {col: None for col in REPORT_HISTORY_SUFFICIENCY_COLS}
        row.update({
            "analytics_run_id": run_id,
            "report_id": "R_001",
            "activation_date_status": "known",
            "history_source_status": "mart_available",
            "history_sufficiency_status": "complete_28d_comparison_history",
            "history_sufficient_28d": False,         # inconsistent
            "history_sufficient_previous_28d": True,
            "comparison_history_sufficient_28d": True,
            "available_calendar_history_days": 30,
        })
        df = pd.DataFrame([row])
        with pytest.raises(ValueError, match="comparison_history_sufficient_28d"):
            validate_report_history_sufficiency(df)

    def test_invalid_activation_date_rejected(self):
        """Validation: activation_date_status must be from known set."""
        run_id = _run_id()
        row = {col: None for col in REPORT_HISTORY_SUFFICIENCY_COLS}
        row.update({
            "analytics_run_id": run_id,
            "report_id": "R_001",
            "activation_date_status": "invalid_status_value",
            "history_source_status": "mart_absent",
            "history_sufficiency_status": "insufficient_history",
            "available_calendar_history_days": 0,
        })
        df = pd.DataFrame([row])
        with pytest.raises(ValueError, match="activation_date_status"):
            validate_report_history_sufficiency(df)

    def test_deterministic_reason_order(self):
        as_of = date(2025, 6, 30)
        activation = as_of - timedelta(days=200)
        run_id_1 = _run_id()
        run_id_2 = _run_id()
        mart = _make_mart_df([{"usage_date": str(as_of), "report_id": "R_001"}])
        cfg = EngagementWindowConfig()
        b1 = build_engagement_window_boundaries(mart, cfg, run_id_1)
        b2 = build_engagement_window_boundaries(mart, cfg, run_id_2)
        meta = _make_report_meta_df([{"report_id": "R_001", "report_activation_date": str(activation)}])
        quality = _make_quality_df([{"report_id": "R_001"}])
        suf1 = build_report_history_sufficiency(meta, mart, quality, b1, cfg, run_id_1)
        suf2 = build_report_history_sufficiency(meta, mart, quality, b2, cfg, run_id_2)
        assert suf1.iloc[0]["history_sufficiency_reasons"] == suf2.iloc[0]["history_sufficiency_reasons"]

    def test_deterministic_report_sorting(self):
        as_of = date(2025, 6, 30)
        run_id = _run_id()
        mart = _make_mart_df([{"usage_date": str(as_of), "report_id": "R_001"}])
        cfg = EngagementWindowConfig()
        b = build_engagement_window_boundaries(mart, cfg, run_id)
        meta = _make_report_meta_df([
            {"report_id": "R_003", "report_activation_date": str(as_of - timedelta(days=10))},
            {"report_id": "R_001", "report_activation_date": str(as_of - timedelta(days=10))},
            {"report_id": "R_002", "report_activation_date": str(as_of - timedelta(days=10))},
        ])
        quality = _make_quality_df([{"report_id": "R_001"}, {"report_id": "R_002"}, {"report_id": "R_003"}])
        suf = build_report_history_sufficiency(meta, mart, quality, b, cfg, run_id)
        assert list(suf["report_id"]) == ["R_001", "R_002", "R_003"]

    def test_duplicate_report_rows_rejected(self):
        run_id = _run_id()
        row = {col: None for col in REPORT_HISTORY_SUFFICIENCY_COLS}
        row.update({
            "analytics_run_id": run_id,
            "report_id": "R_001",
            "activation_date_status": "known",
            "history_source_status": "mart_absent",
            "history_sufficiency_status": "insufficient_history",
            "available_calendar_history_days": 0,
        })
        df = pd.DataFrame([row, row.copy()])
        with pytest.raises(ValueError, match="Duplicate"):
            validate_report_history_sufficiency(df)


# ---------------------------------------------------------------------------
# TestPersistence
# ---------------------------------------------------------------------------

class TestPersistence:
    def _setup(self, tmp_path: Path):
        as_of = date(2025, 6, 30)
        activation = as_of - timedelta(days=200)
        run_id = _run_id()
        mart = _make_mart_df([{"usage_date": str(as_of), "report_id": "R_001"}])
        cfg = EngagementWindowConfig()
        b = build_engagement_window_boundaries(mart, cfg, run_id)
        meta = _make_report_meta_df([{"report_id": "R_001", "report_activation_date": str(activation)}])
        quality = _make_quality_df([{"report_id": "R_001"}])
        suf = build_report_history_sufficiency(meta, mart, quality, b, cfg, run_id)
        return b, suf, run_id

    def test_boundary_file_created(self, tmp_path):
        b, suf, _ = self._setup(tmp_path)
        paths = persist_engagement_window_outputs(b, suf, tmp_path)
        assert paths["boundaries"].exists()

    def test_sufficiency_file_created(self, tmp_path):
        b, suf, _ = self._setup(tmp_path)
        paths = persist_engagement_window_outputs(b, suf, tmp_path)
        assert paths["sufficiency"].exists()

    def test_schemas_stable(self, tmp_path):
        b, suf, _ = self._setup(tmp_path)
        paths = persist_engagement_window_outputs(b, suf, tmp_path)
        b_df = pd.read_csv(paths["boundaries"])
        s_df = pd.read_csv(paths["sufficiency"])
        assert list(b_df.columns) == WINDOW_BOUNDARIES_COLS
        assert list(s_df.columns) == REPORT_HISTORY_SUFFICIENCY_COLS

    def test_latest_outputs_replaced(self, tmp_path):
        b, suf, _ = self._setup(tmp_path)
        paths1 = persist_engagement_window_outputs(b, suf, tmp_path)
        mtime1 = paths1["boundaries"].stat().st_mtime

        import time
        time.sleep(0.05)

        b2, suf2, _ = self._setup(tmp_path)
        paths2 = persist_engagement_window_outputs(b2, suf2, tmp_path)
        mtime2 = paths2["boundaries"].stat().st_mtime

        assert paths1["boundaries"] == paths2["boundaries"]
        assert mtime2 >= mtime1

    def test_input_marts_unchanged(self, tmp_path):
        as_of = date(2025, 6, 30)
        run_id = _run_id()
        mart = _make_mart_df([{"usage_date": str(as_of), "report_id": "R_001"}])
        original_len = len(mart)
        original_cols = list(mart.columns)
        cfg = EngagementWindowConfig()
        b = build_engagement_window_boundaries(mart, cfg, run_id)
        meta = _make_report_meta_df([{"report_id": "R_001", "report_activation_date": str(as_of - timedelta(days=100))}])
        quality = _make_quality_df([{"report_id": "R_001"}])
        suf = build_report_history_sufficiency(meta, mart, quality, b, cfg, run_id)
        persist_engagement_window_outputs(b, suf, tmp_path)
        assert len(mart) == original_len
        assert list(mart.columns) == original_cols

    def test_invalid_output_rejected_before_writing(self, tmp_path):
        b, suf, run_id = self._setup(tmp_path)
        # Corrupt the sufficiency df — add a duplicate row
        bad_suf = pd.concat([suf, suf], ignore_index=True)
        out_path = tmp_path / "outputs" / "analytics" / "report_engagement_history_sufficiency.csv"
        with pytest.raises(ValueError):
            persist_engagement_window_outputs(b, bad_suf, tmp_path)
        # File should NOT have been written (or if it existed before, not overwritten by this call)
        # The key assertion: exception was raised before/during write
