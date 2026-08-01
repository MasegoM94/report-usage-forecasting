"""Tests for src/app/utils/load_data.py.

Focuses on the concentration-field contract introduced to prevent a repeat of
the top_user_concentration → top_1_user_view_share mismatch that caused the
Streamlit app to display NaN silently.
"""

from __future__ import annotations

import pytest
import pandas as pd

from src.app.utils.load_data import (
    row_for_report,
    validate_report_features_schema,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_report_features(**extra_cols) -> pd.DataFrame:
    """Return a minimal valid report_features DataFrame."""
    row = {
        "report_id":            "R1",
        "top_1_user_view_share": 0.42,
        **extra_cols,
    }
    return pd.DataFrame([row])


# ---------------------------------------------------------------------------
# TestConcentrationFieldContract
# ---------------------------------------------------------------------------

class TestConcentrationFieldContract:
    """Prove that row_for_report returns top_1_user_view_share, not the legacy name."""

    def test_top_1_user_view_share_returned(self):
        """row_for_report exposes top_1_user_view_share with its correct value."""
        df = _make_report_features()
        row = row_for_report(df, "R1")
        assert "top_1_user_view_share" in row.index
        assert row["top_1_user_view_share"] == pytest.approx(0.42)

    def test_legacy_field_absent(self):
        """top_user_concentration must not appear in the prepared row."""
        df = _make_report_features()
        row = row_for_report(df, "R1")
        assert "top_user_concentration" not in row.index, (
            "Legacy field top_user_concentration must not be present. "
            "The Streamlit app reads top_1_user_view_share directly."
        )

    def test_concentration_value_is_fraction(self):
        """top_1_user_view_share must be a fraction in [0, 1]."""
        df = _make_report_features(top_1_user_view_share=0.75)
        row = row_for_report(df, "R1")
        val = row["top_1_user_view_share"]
        assert 0.0 <= float(val) <= 1.0

    def test_missing_report_returns_empty_series(self):
        """row_for_report returns an empty Series when report_id is not found."""
        df = _make_report_features()
        row = row_for_report(df, "NONEXISTENT")
        assert row.empty

    def test_high_concentration_value_preserved(self):
        """A 90 % concentration (one dominant user) is preserved without rounding."""
        df = _make_report_features(top_1_user_view_share=0.90)
        row = row_for_report(df, "R1")
        assert row["top_1_user_view_share"] == pytest.approx(0.90)

    def test_zero_concentration_preserved(self):
        """A 0 % value (perfectly distributed viewership) is preserved."""
        df = _make_report_features(top_1_user_view_share=0.0)
        row = row_for_report(df, "R1")
        assert row["top_1_user_view_share"] == pytest.approx(0.0)

    def test_nan_concentration_preserved(self):
        """NaN (no user data available) must pass through without error."""
        import numpy as np
        df = _make_report_features(top_1_user_view_share=np.nan)
        row = row_for_report(df, "R1")
        assert pd.isna(row["top_1_user_view_share"])


# ---------------------------------------------------------------------------
# TestValidateReportFeaturesSchema
# ---------------------------------------------------------------------------

class TestValidateReportFeaturesSchema:
    def _make_v2(self, **overrides):
        row = {
            "report_id": "R001",
            "analytics_as_of_date": "2026-03-31",
            "schema_version": "2.0.0",
            "historical_usage_status": "stable_regular_usage",
        }
        row.update(overrides)
        return pd.DataFrame([row])

    def test_valid_df_passes(self):
        """A v2 DataFrame with all required columns passes without error."""
        df = self._make_v2()
        validate_report_features_schema(df)  # must not raise

    def test_extra_single_column_allowed(self):
        """A single extra column beyond the required set is permitted."""
        df = self._make_v2(extra_col="x")
        validate_report_features_schema(df)  # must not raise

    def test_empty_df_passes(self):
        """An empty DataFrame (file missing or unreadable) is always valid."""
        validate_report_features_schema(pd.DataFrame())  # must not raise

    def test_missing_v2_field_raises(self):
        """A DataFrame missing v2 required fields raises ValueError."""
        df = pd.DataFrame([{"report_id": "R1", "some_other_col": 1}])
        with pytest.raises(ValueError):
            validate_report_features_schema(df)

    def test_error_message_names_the_column(self):
        """The error message names the missing column so a developer knows what to fix."""
        df = pd.DataFrame([{"report_id": "R1"}])
        with pytest.raises(ValueError) as exc_info:
            validate_report_features_schema(df)
        # Should mention one of the missing v2 required cols
        assert any(col in str(exc_info.value) for col in ["analytics_as_of_date", "schema_version", "historical_usage_status"])

    def test_error_message_suggests_fix(self):
        """The error message points to streamlit_app.py so the fix location is obvious."""
        df = pd.DataFrame([{"report_id": "R1"}])
        with pytest.raises(ValueError) as exc_info:
            validate_report_features_schema(df)
        assert "streamlit_app.py" in str(exc_info.value)

    def test_missing_report_id_raises(self):
        """report_id is also required; its absence must be flagged."""
        df = pd.DataFrame([{"analytics_as_of_date": "2026-03-31", "schema_version": "2.0.0", "historical_usage_status": "x"}])
        with pytest.raises(ValueError, match="report_id"):
            validate_report_features_schema(df)

    def test_extra_columns_allowed(self):
        """Additional columns beyond the required set are permitted."""
        df = self._make_v2(extra_col="x", another_col=42)
        validate_report_features_schema(df)  # must not raise


# ---------------------------------------------------------------------------
# TestFieldNameDistinction
# ---------------------------------------------------------------------------

class TestFieldNameDistinction:
    """Guard against the two concentration metrics being confused with each other."""

    def test_top_1_and_top_10pct_are_distinct_columns(self):
        """When both fields are present they must have different names."""
        df = _make_report_features(top_10pct_user_share=0.65)
        row = row_for_report(df, "R1")
        assert "top_1_user_view_share" in row.index
        assert "top_10pct_user_share" in row.index
        assert row.index.tolist().count("top_1_user_view_share") == 1
        assert row.index.tolist().count("top_10pct_user_share") == 1

    def test_top_1_le_top_10pct_when_both_present(self):
        """top_1 ≤ top_10pct always; if they're equal it means only one user exists."""
        df = _make_report_features(
            top_1_user_view_share=0.30,
            top_10pct_user_share=0.55,
        )
        row = row_for_report(df, "R1")
        assert row["top_1_user_view_share"] <= row["top_10pct_user_share"]
