"""
Tests for src/analytics/user_concentration_metrics.py

Sprint 6 — Report-level user-concentration and dependency metrics.
All tests use inline DataFrames — no real files loaded from disk.
"""

from __future__ import annotations

import math
import uuid
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.analytics.user_concentration_metrics import (
    REPORT_USER_CONCENTRATION_METRICS_COLS,
    ConcentrationMetricsConfig,
    aggregate_user_window_views,
    apply_concentration_privacy_suppression,
    build_report_concentration_metrics,
    calculate_concentration_change,
    calculate_top_user_shares,
    calculate_user_view_hhi,
    classify_concentration_direction,
    classify_concentration_status,
    classify_concentration_status_single_window,
    persist_report_concentration_metrics,
    validate_report_concentration_metrics,
)
from src.analytics.privacy_policy import validate_no_direct_identifiers


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_id() -> str:
    return str(uuid.uuid4())


def _cfg(**kwargs) -> ConcentrationMetricsConfig:
    if not kwargs:
        return ConcentrationMetricsConfig()
    # Build by overriding defaults via dict tricks — dataclass is frozen so recreate
    defaults = {
        "MIN_USERS_FOR_CONCENTRATION_METRICS": 5,
        "MIN_USERS_FOR_TOP_1_SHARE": 5,
        "MIN_USERS_FOR_TOP_3_SHARE": 5,
        "MIN_USERS_FOR_HHI": 5,
        "ALLOW_SINGLE_USER_DEPENDENCY_STATUS": False,
        "BROAD_HHI_MAX": 0.15,
        "MODERATE_HHI_MAX": 0.35,
        "HIGH_CONCENTRATION_HHI_MIN": 0.35,
        "TOP1_WARNING_SHARE": 0.40,
        "TOP1_POOR_SHARE": 0.70,
        "TOP3_WARNING_SHARE": 0.60,
        "TOP3_POOR_SHARE": 0.85,
        "EFFECTIVE_USER_SHARE_WARNING": 0.40,
        "EFFECTIVE_USER_SHARE_POOR": 0.20,
        "CONCENTRATION_CHANGE_WARNING": 0.05,
        "CONCENTRATION_CHANGE_POOR": 0.10,
        "MIN_USERS_FOR_CONCENTRATION_STATUS": 3,
        "PERCENTILE_INTERPOLATION_METHOD": "linear",
    }
    defaults.update(kwargs)
    return ConcentrationMetricsConfig(**defaults)


def _make_user_views(rows) -> pd.DataFrame:
    """rows: list of (user_key, user_window_views)"""
    df = pd.DataFrame(rows, columns=["user_key", "user_window_views"])
    df = df.sort_values(
        ["user_window_views", "user_key"], ascending=[False, True]
    ).reset_index(drop=True)
    df["rank"] = range(1, len(df) + 1)
    return df[["user_key", "user_window_views", "rank"]]


def _make_mart(rows) -> pd.DataFrame:
    """rows: list of (report_id, user_key, date_str, daily_views)"""
    records = []
    for report_id, user_key, usage_date_str, daily_views in rows:
        records.append({
            "analytics_run_id": "test-run",
            "report_id": report_id,
            "user_key": user_key,
            "usage_date": usage_date_str,
            "daily_views": daily_views,
        })
    return pd.DataFrame(records)


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
    final_rows = []
    for r in rows:
        merged = dict(defaults)
        merged.update(r)
        final_rows.append(merged)
    return pd.DataFrame(final_rows)


def _make_quality(rows=None) -> pd.DataFrame:
    """rows: list of dicts with report_id and data_quality_status."""
    if rows is None:
        return pd.DataFrame(columns=["report_id", "data_quality_status"])
    return pd.DataFrame(rows)


def _make_boundaries(as_of_str="2024-03-31") -> pd.DataFrame:
    aod = date.fromisoformat(as_of_str)
    w28e = aod
    w28s = aod - timedelta(days=27)
    pw28e = w28s - timedelta(days=1)
    pw28s = pw28e - timedelta(days=27)
    w90e = aod
    w90s = aod - timedelta(days=89)
    return pd.DataFrame([{
        "analytics_run_id": "test-run",
        "generated_at": "2024-04-01T00:00:00",
        "analytics_as_of_date": str(aod),
        "window_7d_start": str(aod - timedelta(days=6)),
        "window_7d_end": str(aod),
        "window_28d_start": str(w28s),
        "window_28d_end": str(w28e),
        "previous_28d_start": str(pw28s),
        "previous_28d_end": str(pw28e),
        "window_90d_start": str(w90s),
        "window_90d_end": str(w90e),
        "previous_90d_start": str(w90s - timedelta(days=90)),
        "previous_90d_end": str(w90s - timedelta(days=1)),
        "pre_previous_28d_end": str(pw28s - timedelta(days=1)),
    }])


# ---------------------------------------------------------------------------
# TestTopUserShares
# ---------------------------------------------------------------------------

class TestTopUserShares:
    def test_one_user_all_shares_one(self):
        udf = _make_user_views([("UK_001", 10)])
        result = calculate_top_user_shares(udf, 10)
        assert result["top_1_user_view_share"] == pytest.approx(1.0)
        assert result["top_3_users_view_share"] == pytest.approx(1.0)
        assert result["top_10pct_users_view_share"] == pytest.approx(1.0)

    def test_two_equal_users_top1_share(self):
        udf = _make_user_views([("UK_001", 5), ("UK_002", 5)])
        result = calculate_top_user_shares(udf, 10)
        assert result["top_1_user_view_share"] == pytest.approx(0.5)

    def test_three_equal_users(self):
        udf = _make_user_views([("UK_001", 10), ("UK_002", 10), ("UK_003", 10)])
        result = calculate_top_user_shares(udf, 30)
        assert result["top_1_user_view_share"] == pytest.approx(1/3, rel=1e-6)
        assert result["top_3_users_view_share"] == pytest.approx(1.0)

    def test_ten_equal_users(self):
        rows = [(f"UK_{i:03d}", 10) for i in range(10)]
        udf = _make_user_views(rows)
        result = calculate_top_user_shares(udf, 100)
        assert result["top_1_user_view_share"] == pytest.approx(0.1)
        assert result["top_3_users_view_share"] == pytest.approx(0.3)
        assert result["top_10pct_user_count"] == 1
        assert result["top_10pct_users_view_share"] == pytest.approx(0.1)

    def test_one_dominant_user(self):
        udf = _make_user_views([("UK_001", 9), ("UK_002", 1)])
        result = calculate_top_user_shares(udf, 10)
        assert result["top_1_user_view_share"] == pytest.approx(0.9)

    def test_top_10pct_ceiling_rule_fewer_than_10_users(self):
        # 7 users → ceil(0.7) = 1
        rows = [(f"UK_{i:03d}", 10) for i in range(7)]
        udf = _make_user_views(rows)
        result = calculate_top_user_shares(udf, 70)
        assert result["top_10pct_user_count"] == 1

    def test_top_10pct_ceiling_rule_exactly_10_users(self):
        # 10 users → ceil(1.0) = 1
        rows = [(f"UK_{i:03d}", 10) for i in range(10)]
        udf = _make_user_views(rows)
        result = calculate_top_user_shares(udf, 100)
        assert result["top_10pct_user_count"] == 1

    def test_top_10pct_ceiling_rule_more_than_10_users(self):
        # 11 users → ceil(1.1) = 2
        rows = [(f"UK_{i:03d}", 10) for i in range(11)]
        udf = _make_user_views(rows)
        result = calculate_top_user_shares(udf, 110)
        assert result["top_10pct_user_count"] == 2

    def test_top_3_gte_top_1(self):
        udf = _make_user_views([("UK_001", 5), ("UK_002", 3), ("UK_003", 2)])
        result = calculate_top_user_shares(udf, 10)
        assert result["top_3_users_view_share"] >= result["top_1_user_view_share"] - 1e-9

    def test_top_10pct_gte_top_1(self):
        udf = _make_user_views([("UK_001", 9), ("UK_002", 1)])
        result = calculate_top_user_shares(udf, 10)
        assert result["top_10pct_users_view_share"] >= result["top_1_user_view_share"] - 1e-9

    def test_top_3_vs_top_10pct_conditional(self):
        # 5 users → top_10pct_count = ceil(0.5) = 1 → top10% == top1 < top3
        rows = [(f"UK_{i:03d}", 10) for i in range(5)]
        udf = _make_user_views(rows)
        result = calculate_top_user_shares(udf, 50)
        assert result["top_10pct_user_count"] == 1
        # top10pct = top1 = 0.2, top3 = 0.6 → top10 <= top3 is fine but top3 > top10
        assert result["top_3_users_view_share"] >= result["top_10pct_users_view_share"] - 1e-9

    def test_deterministic_tie_breaking(self):
        # Two users with equal views → should sort by user_key ascending
        udf = _make_user_views([("UK_B", 10), ("UK_A", 10)])
        # After sorting, UK_A should be rank 1
        assert udf.iloc[0]["user_key"] == "UK_A"
        assert udf.iloc[1]["user_key"] == "UK_B"


# ---------------------------------------------------------------------------
# TestHHI
# ---------------------------------------------------------------------------

class TestHHI:
    def test_one_user_hhi_is_one(self):
        udf = _make_user_views([("UK_001", 10)])
        result = calculate_user_view_hhi(udf, 10)
        assert result["user_view_hhi"] == pytest.approx(1.0)

    def test_two_equal_users_hhi(self):
        udf = _make_user_views([("UK_001", 5), ("UK_002", 5)])
        result = calculate_user_view_hhi(udf, 10)
        assert result["user_view_hhi"] == pytest.approx(0.5)

    def test_four_equal_users_hhi(self):
        rows = [(f"UK_{i:03d}", 5) for i in range(4)]
        udf = _make_user_views(rows)
        result = calculate_user_view_hhi(udf, 20)
        assert result["user_view_hhi"] == pytest.approx(0.25)

    def test_one_dominant_hhi(self):
        udf = _make_user_views([("UK_001", 9), ("UK_002", 1)])
        result = calculate_user_view_hhi(udf, 10)
        # HHI = 0.81 + 0.01 = 0.82
        assert result["user_view_hhi"] == pytest.approx(0.82, rel=1e-6)

    def test_effective_user_count(self):
        rows = [(f"UK_{i:03d}", 5) for i in range(4)]
        udf = _make_user_views(rows)
        result = calculate_user_view_hhi(udf, 20)
        # HHI=0.25 → effective=4.0
        assert result["effective_user_count"] == pytest.approx(4.0, rel=1e-6)

    def test_effective_user_share(self):
        rows = [(f"UK_{i:03d}", 5) for i in range(4)]
        udf = _make_user_views(rows)
        result = calculate_user_view_hhi(udf, 20)
        # effective=4.0, active=4 → share=1.0
        assert result["effective_user_share"] == pytest.approx(1.0, rel=1e-6)

    def test_hhi_lower_bound(self):
        # For N equal users, HHI = 1/N (lower bound)
        for n in [2, 5, 10, 20]:
            rows = [(f"UK_{i:03d}", 10) for i in range(n)]
            udf = _make_user_views(rows)
            result = calculate_user_view_hhi(udf, 10 * n)
            assert result["user_view_hhi"] >= 1.0/n - 1e-9

    def test_hhi_upper_bound(self):
        udf = _make_user_views([("UK_001", 100)])
        result = calculate_user_view_hhi(udf, 100)
        assert result["user_view_hhi"] <= 1.0 + 1e-9

    def test_zero_total_views_null_hhi(self):
        udf = _make_user_views([("UK_001", 0)])
        result = calculate_user_view_hhi(udf, 0)
        assert result["user_view_hhi"] is None

    def test_floating_point_tolerance(self):
        # Three users: 1/3 each — HHI should be 1/3 within 1e-6
        rows = [("UK_001", 1), ("UK_002", 1), ("UK_003", 1)]
        udf = _make_user_views(rows)
        result = calculate_user_view_hhi(udf, 3)
        assert abs(result["user_view_hhi"] - 1/3) < 1e-6


# ---------------------------------------------------------------------------
# TestRelationships
# ---------------------------------------------------------------------------

class TestRelationships:
    def test_top1_lte_top3(self):
        udf = _make_user_views([("UK_001", 9), ("UK_002", 1)])
        r = calculate_top_user_shares(udf, 10)
        assert r["top_1_user_view_share"] <= r["top_3_users_view_share"] + 1e-9

    def test_top1_lte_top10pct(self):
        udf = _make_user_views([("UK_001", 9), ("UK_002", 1)])
        r = calculate_top_user_shares(udf, 10)
        assert r["top_1_user_view_share"] <= r["top_10pct_users_view_share"] + 1e-9

    def test_top3_lte_top10pct_when_count_gte_3(self):
        # With 30 equal users, top_10pct_count=3 → top3 == top10pct
        rows = [(f"UK_{i:03d}", 10) for i in range(30)]
        udf = _make_user_views(rows)
        r = calculate_top_user_shares(udf, 300)
        assert r["top_10pct_user_count"] == 3
        assert r["top_3_users_view_share"] <= r["top_10pct_users_view_share"] + 1e-9

    def test_effective_user_count_lte_active_users(self):
        # Unequal users → effective < active
        udf = _make_user_views([("UK_001", 8), ("UK_002", 2)])
        r = calculate_user_view_hhi(udf, 10)
        assert r["effective_user_count"] <= 2 + 1e-9

    def test_top10pct_count_at_least_one_when_users_exist(self):
        for n in [1, 3, 7, 10, 15]:
            rows = [(f"UK_{i:03d}", 10) for i in range(n)]
            udf = _make_user_views(rows)
            r = calculate_top_user_shares(udf, 10 * n)
            assert r["top_10pct_user_count"] >= 1


# ---------------------------------------------------------------------------
# TestWindowMetrics
# ---------------------------------------------------------------------------

class TestWindowMetrics:
    _AOD = "2024-03-31"

    def _suf(self, report_id="R_001"):
        return _make_sufficiency([{"report_id": report_id}])

    def _bounds(self):
        return _make_boundaries(self._AOD)

    def test_recent_28d_concentration(self):
        # Users in the 28d window
        aod = date.fromisoformat(self._AOD)
        w28s = aod - timedelta(days=27)
        mart = _make_mart([
            ("R_001", f"UK_{i:03d}", str(w28s + timedelta(days=i % 5)), 2)
            for i in range(10)
        ])
        suf = self._suf()
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, self._bounds(), _cfg(), "run1")
        row = df.iloc[0]
        assert row["active_user_count_28d"] is not None
        assert row["active_user_count_28d"] > 0

    def test_previous_28d_concentration(self):
        aod = date.fromisoformat(self._AOD)
        w28s = aod - timedelta(days=27)
        pw28e = w28s - timedelta(days=1)
        pw28s = pw28e - timedelta(days=27)
        mart = _make_mart([
            ("R_001", f"UK_{i:03d}", str(pw28s + timedelta(days=i % 5)), 1)
            for i in range(10)
        ])
        suf = self._suf()
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, self._bounds(), _cfg(), "run1")
        row = df.iloc[0]
        assert row["active_user_count_previous_28d"] is not None

    def test_recent_90d_concentration(self):
        aod = date.fromisoformat(self._AOD)
        w90s = aod - timedelta(days=89)
        mart = _make_mart([
            ("R_001", f"UK_{i:03d}", str(w90s + timedelta(days=i % 10)), 1)
            for i in range(10)
        ])
        suf = self._suf()
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, self._bounds(), _cfg(), "run1")
        row = df.iloc[0]
        assert row["active_user_count_90d"] is not None

    def test_exact_boundary_dates_included(self):
        aod = date.fromisoformat(self._AOD)
        w28s = aod - timedelta(days=27)
        # Place users exactly on boundary dates
        mart = _make_mart([
            ("R_001", f"UK_{i:03d}", str(w28s), 5) for i in range(6)
        ])
        suf = self._suf()
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, self._bounds(), _cfg(), "run1")
        assert df.iloc[0]["active_user_count_28d"] == 6

    def test_outside_window_excluded(self):
        aod = date.fromisoformat(self._AOD)
        w28s = aod - timedelta(days=27)
        outside_date = str(w28s - timedelta(days=1))
        mart = _make_mart([
            ("R_001", f"UK_{i:03d}", outside_date, 5) for i in range(6)
        ])
        suf = self._suf()
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, self._bounds(), _cfg(), "run1")
        # Users are outside window, so active_user_count_28d should be 0 or None
        row = df.iloc[0]
        assert row["active_user_count_28d"] is None or row["active_user_count_28d"] == 0

    def test_insufficient_history_null_metrics(self):
        suf = _make_sufficiency([{
            "report_id": "R_001",
            "history_sufficient_28d": False,
            "history_sufficient_previous_28d": False,
            "comparison_history_sufficient_28d": False,
            "history_sufficient_90d": False,
            "history_sufficient_previous_90d": False,
            "comparison_history_sufficient_90d": False,
        }])
        mart = _make_mart([("R_001", "UK_001", "2024-03-31", 5)])
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, self._bounds(), _cfg(), "run1")
        row = df.iloc[0]
        assert row["user_view_hhi_28d"] is None
        assert row["top_1_user_view_share_28d"] is None

    def test_no_activity_zero_counts_null_shares(self):
        aod = date.fromisoformat(self._AOD)
        w28s = aod - timedelta(days=27)
        # No activity in window
        mart = pd.DataFrame(columns=["analytics_run_id", "report_id", "user_key", "usage_date", "daily_views"])
        suf = self._suf()
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, self._bounds(), _cfg(), "run1")
        row = df.iloc[0]
        assert row["top_1_user_view_share_28d"] is None
        assert row["user_view_hhi_28d"] is None

    def test_no_valid_user_data_null_metrics(self):
        suf = _make_sufficiency([{"report_id": "R_001"}])
        mart = _make_mart([("R_001", "UK_001", "2024-03-31", 5)])
        q = _make_quality([{"report_id": "R_001", "data_quality_status": "no_valid_user_data"}])
        df = build_report_concentration_metrics(suf, mart, q, self._bounds(), _cfg(), "run1")
        row = df.iloc[0]
        assert row["concentration_status"] == "no_valid_user_data"


# ---------------------------------------------------------------------------
# TestChangeMetrics
# ---------------------------------------------------------------------------

class TestChangeMetrics:
    def test_hhi_increase(self):
        recent = {"user_view_hhi": 0.4, "top_1_user_view_share": 0.6,
                  "top_3_users_view_share": 0.9, "top_10pct_users_view_share": 0.9,
                  "effective_user_count": 2.5, "effective_user_share": 0.5}
        prev = {"user_view_hhi": 0.2, "top_1_user_view_share": 0.4,
                "top_3_users_view_share": 0.6, "top_10pct_users_view_share": 0.6,
                "effective_user_count": 5.0, "effective_user_share": 1.0}
        result = calculate_concentration_change(recent, prev, True)
        assert result["hhi_change_28d"] == pytest.approx(0.2)

    def test_hhi_decrease(self):
        recent = {"user_view_hhi": 0.1, "top_1_user_view_share": 0.3,
                  "top_3_users_view_share": 0.5, "top_10pct_users_view_share": 0.5,
                  "effective_user_count": 10.0, "effective_user_share": 1.0}
        prev = {"user_view_hhi": 0.3, "top_1_user_view_share": 0.5,
                "top_3_users_view_share": 0.8, "top_10pct_users_view_share": 0.8,
                "effective_user_count": 3.3, "effective_user_share": 0.5}
        result = calculate_concentration_change(recent, prev, True)
        assert result["hhi_change_28d"] == pytest.approx(-0.2)

    def test_hhi_stable(self):
        recent = {"user_view_hhi": 0.25, "top_1_user_view_share": 0.5,
                  "top_3_users_view_share": 0.75, "top_10pct_users_view_share": 0.75,
                  "effective_user_count": 4.0, "effective_user_share": 1.0}
        prev = {"user_view_hhi": 0.25, "top_1_user_view_share": 0.5,
                "top_3_users_view_share": 0.75, "top_10pct_users_view_share": 0.75,
                "effective_user_count": 4.0, "effective_user_share": 1.0}
        result = calculate_concentration_change(recent, prev, True)
        assert result["hhi_change_28d"] == pytest.approx(0.0)

    def test_newly_active_direction(self):
        direction, _ = classify_concentration_direction(
            recent_hhi=0.25, previous_hhi=None, hhi_change=None,
            recent_users=5, previous_users=0,
            comparison_sufficient=True, has_valid_data=True, is_suppressed=False,
            cfg=_cfg(),
        )
        assert direction == "newly_active"

    def test_inactive_direction(self):
        direction, _ = classify_concentration_direction(
            recent_hhi=None, previous_hhi=None, hhi_change=None,
            recent_users=0, previous_users=0,
            comparison_sufficient=True, has_valid_data=True, is_suppressed=False,
            cfg=_cfg(),
        )
        assert direction == "inactive"

    def test_top1_share_change_calculation(self):
        recent = {"top_1_user_view_share": 0.6, "top_3_users_view_share": 0.8,
                  "top_10pct_users_view_share": 0.8, "user_view_hhi": 0.4,
                  "effective_user_count": 2.5, "effective_user_share": 0.5}
        prev = {"top_1_user_view_share": 0.4, "top_3_users_view_share": 0.7,
                "top_10pct_users_view_share": 0.7, "user_view_hhi": 0.2,
                "effective_user_count": 5.0, "effective_user_share": 1.0}
        result = calculate_concentration_change(recent, prev, True)
        assert result["top_1_share_change_28d"] == pytest.approx(0.2)

    def test_top3_share_change_calculation(self):
        recent = {"top_1_user_view_share": 0.3, "top_3_users_view_share": 0.8,
                  "top_10pct_users_view_share": 0.8, "user_view_hhi": 0.1,
                  "effective_user_count": 10.0, "effective_user_share": 1.0}
        prev = {"top_1_user_view_share": 0.3, "top_3_users_view_share": 0.5,
                "top_10pct_users_view_share": 0.5, "user_view_hhi": 0.1,
                "effective_user_count": 10.0, "effective_user_share": 1.0}
        result = calculate_concentration_change(recent, prev, True)
        assert result["top_3_share_change_28d"] == pytest.approx(0.3)

    def test_effective_user_count_change(self):
        recent = {"top_1_user_view_share": 0.25, "top_3_users_view_share": 0.75,
                  "top_10pct_users_view_share": 0.75, "user_view_hhi": 0.25,
                  "effective_user_count": 4.0, "effective_user_share": 0.8}
        prev = {"top_1_user_view_share": 0.5, "top_3_users_view_share": 0.9,
                "top_10pct_users_view_share": 0.9, "user_view_hhi": 0.5,
                "effective_user_count": 2.0, "effective_user_share": 0.4}
        result = calculate_concentration_change(recent, prev, True)
        assert result["effective_user_count_change_28d"] == pytest.approx(2.0)

    def test_effective_share_change(self):
        recent = {"top_1_user_view_share": 0.25, "top_3_users_view_share": 0.75,
                  "top_10pct_users_view_share": 0.75, "user_view_hhi": 0.25,
                  "effective_user_count": 4.0, "effective_user_share": 0.8}
        prev = {"top_1_user_view_share": 0.5, "top_3_users_view_share": 0.9,
                "top_10pct_users_view_share": 0.9, "user_view_hhi": 0.5,
                "effective_user_count": 2.0, "effective_user_share": 0.4}
        result = calculate_concentration_change(recent, prev, True)
        assert result["effective_user_share_change_28d"] == pytest.approx(0.4)

    def test_insufficient_comparison_history_null(self):
        recent = {"top_1_user_view_share": 0.5, "user_view_hhi": 0.3,
                  "top_3_users_view_share": 0.8, "top_10pct_users_view_share": 0.8,
                  "effective_user_count": 3.3, "effective_user_share": 0.5}
        prev = {"top_1_user_view_share": 0.4, "user_view_hhi": 0.2,
                "top_3_users_view_share": 0.7, "top_10pct_users_view_share": 0.7,
                "effective_user_count": 5.0, "effective_user_share": 1.0}
        result = calculate_concentration_change(recent, prev, False)
        assert result["hhi_change_28d"] is None
        assert result["top_1_share_change_28d"] is None


# ---------------------------------------------------------------------------
# TestClassification
# ---------------------------------------------------------------------------

class TestClassification:
    def test_broadly_distributed(self):
        # Low HHI = 0.1 → broadly_distributed
        udf = _make_user_views([(f"UK_{i:03d}", 10) for i in range(10)])
        status = classify_concentration_status_single_window(
            user_views_df=udf, hhi=0.1, effective_user_share=1.0,
            top_1_share=0.1, top_3_share=0.3,
            active_user_count=10, history_sufficient=True,
            has_valid_data=True, is_suppressed=False, cfg=_cfg(),
        )
        assert status == "broadly_distributed"

    def test_moderately_concentrated(self):
        # HHI = 0.25 → moderately_concentrated
        udf = _make_user_views([(f"UK_{i:03d}", 10) for i in range(4)])
        status = classify_concentration_status_single_window(
            user_views_df=udf, hhi=0.25, effective_user_share=1.0,
            top_1_share=0.25, top_3_share=0.75,
            active_user_count=4, history_sufficient=True,
            has_valid_data=True, is_suppressed=False, cfg=_cfg(),
        )
        assert status == "moderately_concentrated"

    def test_highly_concentrated(self):
        # HHI = 0.5 → highly_concentrated
        udf = _make_user_views([("UK_001", 7), ("UK_002", 3)])
        status = classify_concentration_status_single_window(
            user_views_df=udf, hhi=0.58, effective_user_share=0.3,
            top_1_share=0.7, top_3_share=1.0,
            active_user_count=2, history_sufficient=True,
            has_valid_data=True, is_suppressed=False, cfg=_cfg(),
        )
        assert status == "highly_concentrated"

    def test_single_user_dependent_blocked_by_default(self):
        # ALLOW_SINGLE_USER_DEPENDENCY_STATUS=False → won't return single_user_dependent
        udf = _make_user_views([("UK_001", 9), ("UK_002", 1)])
        status = classify_concentration_status_single_window(
            user_views_df=udf, hhi=0.82, effective_user_share=0.15,
            top_1_share=0.9, top_3_share=1.0,
            active_user_count=2, history_sufficient=True,
            has_valid_data=True, is_suppressed=False,
            cfg=_cfg(ALLOW_SINGLE_USER_DEPENDENCY_STATUS=False),
        )
        assert status != "single_user_dependent"

    def test_single_user_dependent_when_allowed(self):
        # ALLOW_SINGLE_USER_DEPENDENCY_STATUS=True AND top1 >= 0.70
        udf = _make_user_views([("UK_001", 9), ("UK_002", 1)])
        status = classify_concentration_status_single_window(
            user_views_df=udf, hhi=0.82, effective_user_share=0.15,
            top_1_share=0.9, top_3_share=1.0,
            active_user_count=2, history_sufficient=True,
            has_valid_data=True, is_suppressed=False,
            cfg=_cfg(ALLOW_SINGLE_USER_DEPENDENCY_STATUS=True),
        )
        assert status == "single_user_dependent"

    def test_no_recent_activity_status(self):
        udf = pd.DataFrame(columns=["user_key", "user_window_views", "rank"])
        status = classify_concentration_status_single_window(
            user_views_df=udf, hhi=None, effective_user_share=None,
            top_1_share=None, top_3_share=None,
            active_user_count=0, history_sufficient=True,
            has_valid_data=True, is_suppressed=False, cfg=_cfg(),
        )
        assert status == "no_recent_activity"

    def test_privacy_suppressed_status(self):
        udf = _make_user_views([("UK_001", 5), ("UK_002", 3)])
        status = classify_concentration_status_single_window(
            user_views_df=udf, hhi=None, effective_user_share=None,
            top_1_share=None, top_3_share=None,
            active_user_count=2, history_sufficient=True,
            has_valid_data=True, is_suppressed=True, cfg=_cfg(),
        )
        assert status == "privacy_suppressed"

    def test_insufficient_history_status(self):
        udf = pd.DataFrame(columns=["user_key", "user_window_views", "rank"])
        status = classify_concentration_status_single_window(
            user_views_df=udf, hhi=None, effective_user_share=None,
            top_1_share=None, top_3_share=None,
            active_user_count=0, history_sufficient=False,
            has_valid_data=True, is_suppressed=False, cfg=_cfg(),
        )
        assert status == "insufficient_history"

    def test_no_valid_user_data_status(self):
        udf = pd.DataFrame(columns=["user_key", "user_window_views", "rank"])
        status = classify_concentration_status_single_window(
            user_views_df=udf, hhi=None, effective_user_share=None,
            top_1_share=None, top_3_share=None,
            active_user_count=0, history_sufficient=True,
            has_valid_data=False, is_suppressed=False, cfg=_cfg(),
        )
        assert status == "no_valid_user_data"

    def test_deterministic_classification(self):
        # Same inputs should always produce same output
        udf = _make_user_views([(f"UK_{i:03d}", 10) for i in range(6)])
        results = [
            classify_concentration_status_single_window(
                user_views_df=udf, hhi=1/6, effective_user_share=1.0,
                top_1_share=1/6, top_3_share=0.5,
                active_user_count=6, history_sufficient=True,
                has_valid_data=True, is_suppressed=False, cfg=_cfg(),
            )
            for _ in range(3)
        ]
        assert len(set(results)) == 1

    def test_deterministic_reason_order(self):
        recent = {
            "active_user_count": 10,
            "user_view_hhi": 0.1,
            "effective_user_share": 1.0,
            "top_1_user_view_share": 0.1,
            "concentration_status_28d": "broadly_distributed",
        }
        results = [
            classify_concentration_status(
                recent_28d=recent, previous_28d={},
                concentration_direction="stable",
                comparison_sufficient=True,
                history_sufficient_28d=True,
                has_valid_data=True, is_suppressed=False, cfg=_cfg(),
            )
            for _ in range(3)
        ]
        # Reasons should be same each time
        reasons_list = [r[2] for r in results]
        assert reasons_list[0] == reasons_list[1] == reasons_list[2]
        assert len(reasons_list[0]) == 10


# ---------------------------------------------------------------------------
# TestPrivacySuppression
# ---------------------------------------------------------------------------

class TestPrivacySuppression:
    _AOD = "2024-03-31"

    def _make_few_users_mart(self, n=3):
        aod = date.fromisoformat(self._AOD)
        w28s = aod - timedelta(days=27)
        return _make_mart([
            ("R_001", f"UK_{i:03d}", str(w28s + timedelta(days=i)), 5)
            for i in range(n)
        ])

    def test_below_threshold_suppresses_hhi(self):
        mart = self._make_few_users_mart(3)
        suf = _make_sufficiency([{"report_id": "R_001"}])
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")
        # 3 < 5 → suppressed
        assert df.iloc[0]["user_view_hhi_28d"] is None

    def test_exactly_threshold_not_suppressed(self):
        mart = self._make_few_users_mart(5)
        suf = _make_sufficiency([{"report_id": "R_001"}])
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")
        # 5 >= 5 → not suppressed
        assert df.iloc[0]["user_view_hhi_28d"] is not None

    def test_total_views_not_suppressed(self):
        mart = self._make_few_users_mart(3)
        suf = _make_sufficiency([{"report_id": "R_001"}])
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")
        # total_views should not be suppressed
        assert df.iloc[0]["total_views_28d"] is not None

    def test_active_user_count_not_suppressed(self):
        mart = self._make_few_users_mart(3)
        suf = _make_sufficiency([{"report_id": "R_001"}])
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")
        assert df.iloc[0]["active_user_count_28d"] is not None

    def test_top_1_suppressed(self):
        mart = self._make_few_users_mart(3)
        suf = _make_sufficiency([{"report_id": "R_001"}])
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")
        assert df.iloc[0]["top_1_user_view_share_28d"] is None

    def test_effective_count_suppressed(self):
        mart = self._make_few_users_mart(3)
        suf = _make_sufficiency([{"report_id": "R_001"}])
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")
        assert df.iloc[0]["effective_user_count_28d"] is None

    def test_no_user_key_in_output(self):
        mart = self._make_few_users_mart(5)
        suf = _make_sufficiency([{"report_id": "R_001"}])
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")
        assert "user_key" not in df.columns

    def test_no_ranked_user_list(self):
        mart = self._make_few_users_mart(5)
        suf = _make_sufficiency([{"report_id": "R_001"}])
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")
        for col in df.columns:
            assert df[col].apply(lambda x: isinstance(x, (list, set, tuple))).sum() == 0

    def test_suppression_metadata_deterministic(self):
        mart = self._make_few_users_mart(3)
        suf = _make_sufficiency([{"report_id": "R_001"}])
        q = _make_quality()
        df1 = build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")
        df2 = build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")
        assert df1.iloc[0]["concentration_privacy_suppressed"] == df2.iloc[0]["concentration_privacy_suppressed"]
        assert df1.iloc[0]["suppressed_concentration_fields"] == df2.iloc[0]["suppressed_concentration_fields"]


# ---------------------------------------------------------------------------
# TestValidation
# ---------------------------------------------------------------------------

class TestValidation:
    _AOD = "2024-03-31"

    def _good_df(self):
        aod = date.fromisoformat(self._AOD)
        w28s = aod - timedelta(days=27)
        mart = _make_mart([
            ("R_001", f"UK_{i:03d}", str(w28s + timedelta(days=i % 10)), 5)
            for i in range(10)
        ])
        suf = _make_sufficiency([{"report_id": "R_001"}])
        q = _make_quality()
        return build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")

    def test_negative_active_count_rejected(self):
        df = self._good_df().copy()
        df.loc[0, "active_user_count_28d"] = -1
        with pytest.raises(ValueError, match="Negative"):
            validate_report_concentration_metrics(df)

    def test_top1_above_top3_rejected(self):
        df = self._good_df().copy()
        df.loc[0, "top_1_user_view_share_28d"] = 0.9
        df.loc[0, "top_3_users_view_share_28d"] = 0.5
        with pytest.raises(ValueError):
            validate_report_concentration_metrics(df)

    def test_invalid_hhi_rejected(self):
        df = self._good_df().copy()
        df.loc[0, "user_view_hhi_28d"] = 1.5
        with pytest.raises(ValueError):
            validate_report_concentration_metrics(df)

    def test_invalid_status_rejected(self):
        df = self._good_df().copy()
        df.loc[0, "concentration_status"] = "invalid_status_xyz"
        with pytest.raises(ValueError):
            validate_report_concentration_metrics(df)

    def test_duplicate_report_rows_rejected(self):
        df = self._good_df()
        df2 = pd.concat([df, df], ignore_index=True)
        with pytest.raises(ValueError, match="Duplicate"):
            validate_report_concentration_metrics(df2)

    def test_nonull_concentration_with_zero_views_rejected(self):
        df = self._good_df().copy()
        # Force total_views_28d = 0 but keep top_1_share not null
        df.loc[0, "total_views_28d"] = 0
        df.loc[0, "top_1_user_view_share_28d"] = 0.5
        with pytest.raises(ValueError):
            validate_report_concentration_metrics(df)

    def test_suppression_inconsistency_rejected(self):
        df = self._good_df().copy()
        # Mark as suppressed but leave sensitive field non-null
        df.loc[0, "concentration_privacy_suppressed"] = True
        df.loc[0, "top_1_user_view_share_28d"] = 0.5
        with pytest.raises(ValueError):
            validate_report_concentration_metrics(df)

    def test_effective_count_above_active_rejected(self):
        df = self._good_df().copy()
        df.loc[0, "effective_user_count_28d"] = 1000.0
        df.loc[0, "active_user_count_28d"] = 10
        with pytest.raises(ValueError):
            validate_report_concentration_metrics(df)


# ---------------------------------------------------------------------------
# TestPersistence
# ---------------------------------------------------------------------------

class TestPersistence:
    _AOD = "2024-03-31"

    def _build(self):
        aod = date.fromisoformat(self._AOD)
        w28s = aod - timedelta(days=27)
        mart = _make_mart([
            ("R_001", f"UK_{i:03d}", str(w28s + timedelta(days=i % 10)), 5)
            for i in range(10)
        ])
        suf = _make_sufficiency([{"report_id": "R_001"}])
        q = _make_quality()
        return build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")

    def test_output_file_created(self, tmp_path):
        df = self._build()
        path = persist_report_concentration_metrics(df, tmp_path)
        assert path.exists()
        assert path.name == "report_user_concentration_metrics.csv"

    def test_schema_stable(self, tmp_path):
        df = self._build()
        path = persist_report_concentration_metrics(df, tmp_path)
        loaded = pd.read_csv(path)
        for col in REPORT_USER_CONCENTRATION_METRICS_COLS:
            assert col in loaded.columns, f"Missing column: {col}"

    def test_deterministic_sorting(self, tmp_path):
        aod = date.fromisoformat(self._AOD)
        w28s = aod - timedelta(days=27)
        mart = _make_mart([
            ("R_002", f"UK_{i:03d}", str(w28s + timedelta(days=i % 5)), 3) for i in range(6)
        ] + [
            ("R_001", f"UK_{i:03d}", str(w28s + timedelta(days=i % 5)), 5) for i in range(6)
        ])
        suf = _make_sufficiency([{"report_id": "R_001"}, {"report_id": "R_002"}])
        q = _make_quality()
        df = build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")
        path = persist_report_concentration_metrics(df, tmp_path)
        loaded = pd.read_csv(path)
        assert list(loaded["report_id"]) == sorted(loaded["report_id"].tolist())

    def test_latest_file_replaced(self, tmp_path):
        df = self._build()
        path1 = persist_report_concentration_metrics(df, tmp_path)
        path2 = persist_report_concentration_metrics(df, tmp_path)
        assert path1 == path2
        assert path2.exists()

    def test_source_mart_unchanged(self, tmp_path):
        aod = date.fromisoformat(self._AOD)
        w28s = aod - timedelta(days=27)
        mart = _make_mart([
            ("R_001", f"UK_{i:03d}", str(w28s + timedelta(days=i % 10)), 5)
            for i in range(10)
        ])
        original_shape = mart.shape
        suf = _make_sufficiency([{"report_id": "R_001"}])
        q = _make_quality()
        build_report_concentration_metrics(suf, mart, q, _make_boundaries(self._AOD), _cfg(), "run1")
        assert mart.shape == original_shape

    def test_invalid_output_rejected_before_writing(self, tmp_path):
        df = self._build().copy()
        df.loc[0, "user_view_hhi_28d"] = 2.0  # invalid
        with pytest.raises(ValueError):
            persist_report_concentration_metrics(df, tmp_path)
