"""Tests for engagement mart loading and field mapping in the Streamlit app."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.app.utils.load_data import row_for_report


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_eng_row(**overrides) -> pd.DataFrame:
    """Return a minimal valid engagement mart row."""
    row = {
        "report_id":                     "R001",
        "report_name":                   "Test Report",
        "analytics_as_of_date":          "2026-03-31",
        "engagement_evidence_status":    "complete",
        "privacy_suppression_status":    "not_suppressed",
        "activity_privacy_suppressed":   False,
        "cohort_privacy_suppressed":     False,
        "concentration_privacy_suppressed": False,
        "privacy_suppressed_field_count": 0,
        "privacy_suppressed_fields":     "",
        "active_usage_days_28d":         12.0,
        "returning_user_share_28d":      0.74,
        "top_1_user_view_share_28d":     0.05,
        "unique_users_28d":              55,
        "retained_user_rate_28d":        0.88,
        "lapse_rate_28d":                0.12,
        "overall_engagement_status":     "healthy_broad_adoption",
    }
    row.update(overrides)
    return pd.DataFrame([row])


class TestValidEngagementRow:
    def test_active_days_readable(self):
        df = _make_eng_row()
        row = row_for_report(df, "R001")
        assert row["active_usage_days_28d"] == 12.0

    def test_returning_share_readable(self):
        df = _make_eng_row()
        row = row_for_report(df, "R001")
        assert row["returning_user_share_28d"] == pytest.approx(0.74)

    def test_top_user_share_readable(self):
        df = _make_eng_row()
        row = row_for_report(df, "R001")
        assert row["top_1_user_view_share_28d"] == pytest.approx(0.05)

    def test_missing_report_returns_empty_series(self):
        df = _make_eng_row()
        row = row_for_report(df, "NONEXISTENT")
        assert row.empty


class TestPrivacySuppression:
    def test_concentration_suppression_flag_readable(self):
        df = _make_eng_row(
            concentration_privacy_suppressed=True,
            privacy_suppression_status="suppressed",
            privacy_suppressed_fields="concentration",
            privacy_suppressed_field_count=1,
        )
        row = row_for_report(df, "R001")
        assert row["concentration_privacy_suppressed"] is True or row["concentration_privacy_suppressed"] == True  # noqa

    def test_concentration_suppressed_field_in_privacy_fields_string(self):
        df = _make_eng_row(
            concentration_privacy_suppressed=True,
            privacy_suppressed_fields="concentration",
        )
        row = row_for_report(df, "R001")
        assert "concentration" in str(row.get("privacy_suppressed_fields", ""))

    def test_activity_suppression_flag_readable(self):
        df = _make_eng_row(
            activity_privacy_suppressed=True,
            privacy_suppression_status="suppressed",
            privacy_suppressed_fields="activity",
            privacy_suppressed_field_count=1,
        )
        row = row_for_report(df, "R001")
        assert row["activity_privacy_suppressed"] is True or row["activity_privacy_suppressed"] == True  # noqa

    def test_cohort_suppression_flag_readable(self):
        df = _make_eng_row(
            cohort_privacy_suppressed=True,
            privacy_suppression_status="suppressed",
            privacy_suppressed_fields="cohort",
        )
        row = row_for_report(df, "R001")
        assert "cohort" in str(row.get("privacy_suppressed_fields", ""))


class TestInsufficientEngagementHistory:
    def test_incomplete_evidence_status_readable(self):
        df = _make_eng_row(
            engagement_evidence_status="incomplete",
            missing_engagement_evidence="cohort,frequency",
        )
        row = row_for_report(df, "R001")
        assert "incomplete" in str(row.get("engagement_evidence_status", ""))

    def test_missing_evidence_field_readable(self):
        df = _make_eng_row(
            engagement_evidence_status="incomplete",
            missing_engagement_evidence="cohort",
        )
        row = row_for_report(df, "R001")
        assert "cohort" in str(row.get("missing_engagement_evidence", ""))


class TestNullMetricDistinction:
    def test_null_active_days_is_nan_not_zero(self):
        df = _make_eng_row(active_usage_days_28d=np.nan)
        row = row_for_report(df, "R001")
        val = row.get("active_usage_days_28d")
        assert pd.isna(val), "A null active-days value must be NaN, not zero"

    def test_null_returning_share_is_nan_not_zero(self):
        df = _make_eng_row(returning_user_share_28d=np.nan)
        row = row_for_report(df, "R001")
        val = row.get("returning_user_share_28d")
        assert pd.isna(val), "A null returning-share value must be NaN, not zero"

    def test_zero_top_user_share_preserved(self):
        """A genuine zero concentration (completely distributed) must not be confused with null."""
        df = _make_eng_row(top_1_user_view_share_28d=0.0)
        row = row_for_report(df, "R001")
        assert row["top_1_user_view_share_28d"] == pytest.approx(0.0)
