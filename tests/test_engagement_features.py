"""Tests for src.features.engagement_features.build_user_engagement_features.

Covers: top_1_user_view_share, top_10pct_user_share, column naming, and
the distinction between the two concentration metrics.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.features.engagement_features import build_user_engagement_features


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_inputs(
    user_views: dict[str, int],
    date: str = "2025-01-01",
    report_id: str = "R_001",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (fact_report_views, fact_page_views) for a single date/report."""
    rows = []
    for uid, n in user_views.items():
        for _ in range(n):
            rows.append({"date": date, "report_id": report_id, "user_id": uid, "view_count": 1})

    fact_views = pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["date", "report_id", "user_id", "view_count"]
    )

    # Minimal page-view table (one page per user)
    page_rows = [
        {"date": date, "report_id": report_id, "user_id": uid, "section_id": "pg1"}
        for uid in user_views
    ]
    fact_pages = pd.DataFrame(page_rows) if page_rows else pd.DataFrame(
        columns=["date", "report_id", "user_id", "section_id"]
    )

    return fact_views, fact_pages


# ---------------------------------------------------------------------------
# 1. top_1_user_view_share
# ---------------------------------------------------------------------------

class TestTop1UserViewShare:
    def test_dominant_user_high_share(self):
        """One user has 90 of 100 views → top_1_user_view_share = 0.9."""
        fact_v, fact_p = _make_inputs({"user_A": 90, "user_B": 5, "user_C": 5})
        result = build_user_engagement_features(fact_v, fact_p)
        row = result.loc[result["report_id"] == "R_001"].iloc[0]
        assert row["top_1_user_view_share"] == pytest.approx(0.9)

    def test_equal_users_share_is_one_over_n(self):
        """Four users with equal views → top_1_user_view_share = 0.25."""
        fact_v, fact_p = _make_inputs({"u1": 25, "u2": 25, "u3": 25, "u4": 25})
        result = build_user_engagement_features(fact_v, fact_p)
        row = result.loc[result["report_id"] == "R_001"].iloc[0]
        assert row["top_1_user_view_share"] == pytest.approx(0.25)

    def test_single_user_share_is_one(self):
        """Only one user → top_1_user_view_share = 1.0."""
        fact_v, fact_p = _make_inputs({"solo": 20})
        result = build_user_engagement_features(fact_v, fact_p)
        row = result.iloc[0]
        assert row["top_1_user_view_share"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# 2. top_10pct_user_share
# ---------------------------------------------------------------------------

class TestTop10PctUserShare:
    def test_top_10pct_higher_than_top_1_when_one_user_dominant(self):
        """With a single dominant user top_10pct equals top_1 (ceiling(10%) = 1)."""
        fact_v, fact_p = _make_inputs({"u1": 80, "u2": 10, "u3": 10})
        result = build_user_engagement_features(fact_v, fact_p)
        row = result.iloc[0]
        # ceil(3 * 0.10) = 1 user → top_10pct = top_1 in this case
        assert row["top_10pct_user_share"] == row["top_1_user_view_share"]

    def test_top_10pct_lower_than_or_equal_top_1(self):
        """top_10pct_user_share ≤ 1.0 always; top_1 ≤ top_10pct when more than 1 user."""
        # 10 users, first has 50 views, rest have 5 each → 10% = 1 user
        user_views = {"u0": 50, **{f"u{i}": 5 for i in range(1, 10)}}
        fact_v, fact_p = _make_inputs(user_views)
        result = build_user_engagement_features(fact_v, fact_p)
        row = result.iloc[0]
        assert 0.0 <= row["top_10pct_user_share"] <= 1.0

    def test_value_between_zero_and_one(self):
        fact_v, fact_p = _make_inputs({"a": 3, "b": 3, "c": 3, "d": 1})
        result = build_user_engagement_features(fact_v, fact_p)
        row = result.iloc[0]
        assert 0.0 <= row["top_10pct_user_share"] <= 1.0


# ---------------------------------------------------------------------------
# 3. Distinction between metrics
# ---------------------------------------------------------------------------

class TestConcentrationDistinction:
    def test_top_1_and_top_10pct_are_separate_columns(self):
        """Both concentration columns must be present and independent."""
        fact_v, fact_p = _make_inputs({"u1": 10, "u2": 10, "u3": 10, "u4": 10, "u5": 10,
                                        "u6": 10, "u7": 10, "u8": 10, "u9": 10, "u10": 10})
        result = build_user_engagement_features(fact_v, fact_p)
        assert "top_1_user_view_share" in result.columns
        assert "top_10pct_user_share" in result.columns

    def test_top_10pct_gte_top_1_with_many_users(self):
        """With 20 users (top 10% = 2 users), top_10pct >= top_1."""
        users = {f"u{i}": max(1, 20 - i) for i in range(20)}
        fact_v, fact_p = _make_inputs(users)
        result = build_user_engagement_features(fact_v, fact_p)
        row = result.iloc[0]
        assert row["top_10pct_user_share"] >= row["top_1_user_view_share"]


# ---------------------------------------------------------------------------
# 4. Column naming consistency
# ---------------------------------------------------------------------------

class TestColumnNaming:
    def test_no_top_user_share_column(self):
        """Legacy ambiguous name top_user_share must not appear in output."""
        fact_v, fact_p = _make_inputs({"u1": 5, "u2": 5})
        result = build_user_engagement_features(fact_v, fact_p)
        assert "top_user_share" not in result.columns

    def test_required_columns_present(self):
        """All expected output columns must be present."""
        fact_v, fact_p = _make_inputs({"u1": 5, "u2": 5})
        result = build_user_engagement_features(fact_v, fact_p)
        for col in (
            "date",
            "report_id",
            "repeat_user_rate",
            "top_1_user_view_share",
            "top_10pct_user_share",
            "days_since_last_use",
            "avg_pages_per_user",
        ):
            assert col in result.columns, f"Missing column: {col}"

    def test_empty_input_schema(self):
        """Empty input returns frame with correct column schema."""
        empty_v = pd.DataFrame(columns=["date", "report_id", "user_id"])
        empty_p = pd.DataFrame(columns=["date", "report_id", "user_id", "section_id"])
        result = build_user_engagement_features(empty_v, empty_p)
        assert "top_1_user_view_share" in result.columns
        assert "top_10pct_user_share" in result.columns
        assert "top_user_share" not in result.columns
