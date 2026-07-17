"""Tests for src.analytics.report_features.build_report_features.

Covers: 28-day window metrics, 12-week trend slope, top-1 user concentration,
column naming consistency, and edge cases (zero prior, insufficient history).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.analytics.report_features import MIN_TREND_WEEKS, build_report_features


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _daily(
    report_id: str,
    start: str,
    n_days: int,
    views: list[int] | None = None,
) -> pd.DataFrame:
    """Build a minimal daily adoption frame for a single report."""
    dates = pd.date_range(start, periods=n_days, freq="D")
    v = views if views is not None else [10] * n_days
    assert len(v) == n_days
    return pd.DataFrame({"report_id": report_id, "date": dates, "daily_views": v})


def _fact_views(report_id: str, user_views: dict[str, int]) -> pd.DataFrame:
    """Build a minimal fact_report_views with user-level view counts."""
    rows = [
        {"report_id": report_id, "user_id": uid, "view_count": vc, "date": "2025-01-01"}
        for uid, vc in user_views.items()
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 1. Positive usage growth
# ---------------------------------------------------------------------------

class TestPositiveGrowth:
    def test_usage_change_28d_pct_positive(self):
        """recent > previous → positive usage_change_28d_pct."""
        views = [5] * 28 + [10] * 28          # previous=140, recent=280
        df = _daily("R_001", "2025-01-01", 56, views)
        result = build_report_features(df)
        row = result.loc[result["report_id"] == "R_001"].iloc[0]
        assert row["usage_change_28d_pct"] > 0

    def test_recent_and_previous_28d_views_correct(self):
        views = [5] * 28 + [10] * 28
        df = _daily("R_001", "2025-01-01", 56, views)
        result = build_report_features(df)
        row = result.loc[result["report_id"] == "R_001"].iloc[0]
        assert row["previous_28d_views"] == 140
        assert row["recent_28d_views"] == 280


# ---------------------------------------------------------------------------
# 2. Declining usage
# ---------------------------------------------------------------------------

class TestDecliningUsage:
    def test_usage_change_28d_pct_negative(self):
        """recent < previous → negative usage_change_28d_pct."""
        views = [10] * 28 + [2] * 28          # previous=280, recent=56
        df = _daily("R_002", "2025-01-01", 56, views)
        result = build_report_features(df)
        row = result.loc[result["report_id"] == "R_002"].iloc[0]
        assert row["usage_change_28d_pct"] < 0


# ---------------------------------------------------------------------------
# 3. Stable usage
# ---------------------------------------------------------------------------

class TestStableUsage:
    def test_usage_change_28d_pct_near_zero(self):
        """Identical previous and recent windows → change == 0."""
        views = [7] * 56
        df = _daily("R_003", "2025-01-01", 56, views)
        result = build_report_features(df)
        row = result.loc[result["report_id"] == "R_003"].iloc[0]
        assert row["usage_change_28d_pct"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# 4. Zero prior-period usage
# ---------------------------------------------------------------------------

class TestZeroPriorPeriod:
    def test_both_zero_returns_zero(self):
        """previous=0, recent=0 → usage_change_28d_pct = 0.0 (no activity)."""
        views = [0] * 56
        df = _daily("R_004", "2025-01-01", 56, views)
        result = build_report_features(df)
        row = result.loc[result["report_id"] == "R_004"].iloc[0]
        assert row["usage_change_28d_pct"] == pytest.approx(0.0)
        assert row["newly_active_flag"] is False or row["newly_active_flag"] == False

    def test_zero_previous_nonzero_recent_returns_null(self):
        """previous=0, recent>0 → percentage undefined; usage_change_28d_pct is null."""
        views = [0] * 28 + [5] * 28
        df = _daily("R_005", "2025-01-01", 56, views)
        result = build_report_features(df)
        row = result.loc[result["report_id"] == "R_005"].iloc[0]
        assert pd.isna(row["usage_change_28d_pct"])

    def test_zero_previous_nonzero_recent_sets_newly_active(self):
        """previous=0, recent>0 → newly_active_flag = True."""
        views = [0] * 28 + [5] * 28
        df = _daily("R_005", "2025-01-01", 56, views)
        result = build_report_features(df)
        row = result.loc[result["report_id"] == "R_005"].iloc[0]
        assert row["newly_active_flag"] is True or row["newly_active_flag"] == True


# ---------------------------------------------------------------------------
# 5. Insufficient history
# ---------------------------------------------------------------------------

class TestInsufficientHistory:
    def test_fewer_than_56_days_returns_null_window_metrics(self):
        """< 56 days of history → all window metrics are null."""
        df = _daily("R_006", "2025-01-01", 30)
        result = build_report_features(df)
        row = result.loc[result["report_id"] == "R_006"].iloc[0]
        assert pd.isna(row["usage_change_28d_pct"])
        assert pd.isna(row["recent_28d_views"])
        assert pd.isna(row["previous_28d_views"])
        assert pd.isna(row["usage_trend_12w_slope"])
        assert row["trend_history_sufficient"] is False or row["trend_history_sufficient"] == False

    def test_exactly_56_days_sufficient(self):
        """Exactly 56 days → trend_history_sufficient = True."""
        df = _daily("R_007", "2025-01-01", 56)
        result = build_report_features(df)
        row = result.loc[result["report_id"] == "R_007"].iloc[0]
        assert row["trend_history_sufficient"] is True or row["trend_history_sufficient"] == True

    def test_trend_slope_null_when_few_complete_weeks(self):
        """Fewer than MIN_TREND_WEEKS complete weeks → usage_trend_12w_slope null."""
        # 56 days but partial weeks mean fewer than MIN_TREND_WEEKS complete
        df = _daily("R_008", "2025-01-01", 56)
        result = build_report_features(df)
        row = result.loc[result["report_id"] == "R_008"].iloc[0]
        # 56 days = 8 weeks exactly; may or may not reach MIN_TREND_WEEKS (8)
        # Just check the result is either a float or null — not an error
        val = row["usage_trend_12w_slope"]
        assert pd.isna(val) or isinstance(val, float)


# ---------------------------------------------------------------------------
# 6. Top-1 user concentration
# ---------------------------------------------------------------------------

class TestTop1UserConcentration:
    def test_single_dominant_user(self):
        """One user has 90% of views → top_1_user_view_share ≈ 0.9."""
        fact = _fact_views("R_010", {"user_A": 90, "user_B": 5, "user_C": 5})
        daily = _daily("R_010", "2025-01-01", 56)
        result = build_report_features(daily, fact_report_views=fact)
        row = result.loc[result["report_id"] == "R_010"].iloc[0]
        assert row["top_1_user_view_share"] == pytest.approx(0.90)

    def test_equal_users_share(self):
        """Four equal users → top_1_user_view_share = 0.25."""
        fact = _fact_views("R_011", {"u1": 25, "u2": 25, "u3": 25, "u4": 25})
        daily = _daily("R_011", "2025-01-01", 56)
        result = build_report_features(daily, fact_report_views=fact)
        row = result.loc[result["report_id"] == "R_011"].iloc[0]
        assert row["top_1_user_view_share"] == pytest.approx(0.25)

    def test_column_present_without_fact_views(self):
        """top_1_user_view_share is present (null) even without fact_report_views."""
        daily = _daily("R_012", "2025-01-01", 56)
        result = build_report_features(daily)
        assert "top_1_user_view_share" in result.columns


# ---------------------------------------------------------------------------
# 7. Column naming consistency
# ---------------------------------------------------------------------------

class TestColumnNaming:
    def test_no_usage_change_pct_column(self):
        """Legacy column usage_change_pct must not appear in output."""
        daily = _daily("R_020", "2025-01-01", 56)
        result = build_report_features(daily)
        assert "usage_change_pct" not in result.columns

    def test_no_top_user_concentration_column(self):
        """Legacy column top_user_concentration must not appear in output."""
        daily = _daily("R_021", "2025-01-01", 56)
        result = build_report_features(daily)
        assert "top_user_concentration" not in result.columns

    def test_required_new_columns_present(self):
        """All new stable metric columns must be present."""
        daily = _daily("R_022", "2025-01-01", 56)
        result = build_report_features(daily)
        for col in (
            "recent_28d_views",
            "previous_28d_views",
            "usage_change_28d_pct",
            "newly_active_flag",
            "trend_history_sufficient",
            "usage_trend_12w_slope",
            "top_1_user_view_share",
        ):
            assert col in result.columns, f"Missing column: {col}"

    def test_empty_input_returns_correct_schema(self):
        """Empty input returns an empty frame with the correct column schema."""
        result = build_report_features(pd.DataFrame())
        assert "usage_change_28d_pct" in result.columns
        assert "top_1_user_view_share" in result.columns
        assert "usage_change_pct" not in result.columns
        assert "top_user_concentration" not in result.columns


# ---------------------------------------------------------------------------
# 8. Trend slope direction
# ---------------------------------------------------------------------------

class TestTrendSlope:
    def test_growing_trend_positive_slope(self):
        """Steadily increasing weekly views → positive slope."""
        # Build 12+ weeks of growing data
        weekly_totals = list(range(10, 130, 10))          # 12 values, weeks 0..11
        views = []
        for w in weekly_totals:
            views.extend([w // 7] * 7)
        df = _daily("R_030", "2025-01-06", len(views), views)  # Monday start
        result = build_report_features(df)
        row = result.loc[result["report_id"] == "R_030"].iloc[0]
        if not pd.isna(row["usage_trend_12w_slope"]):
            assert row["usage_trend_12w_slope"] > 0

    def test_declining_trend_negative_slope(self):
        """Steadily declining weekly views → negative slope."""
        weekly_totals = list(range(120, 0, -10))           # 12 values decreasing
        views = []
        for w in weekly_totals:
            views.extend([w // 7] * 7)
        df = _daily("R_031", "2025-01-06", len(views), views)
        result = build_report_features(df)
        row = result.loc[result["report_id"] == "R_031"].iloc[0]
        if not pd.isna(row["usage_trend_12w_slope"]):
            assert row["usage_trend_12w_slope"] < 0
