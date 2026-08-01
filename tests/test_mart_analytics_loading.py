"""Tests for canonical mart analytics loading and validation."""

from __future__ import annotations

import pandas as pd
import pytest

from src.app.utils.load_data import validate_mart_analytics_schema


def _make_mart(**overrides) -> pd.DataFrame:
    row = {
        "report_id":           "R001",
        "analytics_run_id":    "abc-123",
        "analytics_as_of_date": "2026-03-31",
        "report_name":         "Test Report",
        "overall_report_status": "healthy",
        "overall_review_priority": "low",
        "recommended_report_action": "continue_monitoring",
    }
    row.update(overrides)
    return pd.DataFrame([row])


class TestValidateMartAnalyticsSchema:
    def test_valid_mart_passes(self):
        validate_mart_analytics_schema(_make_mart())

    def test_empty_mart_passes(self):
        validate_mart_analytics_schema(pd.DataFrame())

    def test_missing_report_id_raises(self):
        df = pd.DataFrame([{"analytics_run_id": "x", "analytics_as_of_date": "2026-03-31"}])
        with pytest.raises(ValueError, match="report_id"):
            validate_mart_analytics_schema(df)

    def test_missing_run_id_raises(self):
        df = pd.DataFrame([{"report_id": "R001", "analytics_as_of_date": "2026-03-31"}])
        with pytest.raises(ValueError, match="analytics_run_id"):
            validate_mart_analytics_schema(df)

    def test_missing_as_of_date_raises(self):
        df = pd.DataFrame([{"report_id": "R001", "analytics_run_id": "x"}])
        with pytest.raises(ValueError, match="analytics_as_of_date"):
            validate_mart_analytics_schema(df)

    def test_duplicate_report_ids_raise(self):
        df = pd.concat([_make_mart(), _make_mart()], ignore_index=True)
        with pytest.raises(ValueError, match="duplicate"):
            validate_mart_analytics_schema(df)

    def test_optional_columns_allowed(self):
        df = _make_mart(some_extra_column="value")
        validate_mart_analytics_schema(df)  # must not raise

    def test_error_names_missing_column(self):
        df = pd.DataFrame([{"report_id": "R001"}])
        with pytest.raises(ValueError) as exc_info:
            validate_mart_analytics_schema(df)
        assert "analytics_run_id" in str(exc_info.value) or "analytics_as_of_date" in str(exc_info.value)
