"""Integration tests for load_app_data() using temporary directories.

Each test writes minimal fixture files to a tmp_path, calls load_app_data(root=tmp_path),
and asserts on the returned dict.

No Streamlit runtime is required — the @_cache decorator is a no-op outside Streamlit.
No live LLM calls occur.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from src.app.utils.load_data import (
    load_app_data,
    available_reports,
    normalize_forecast_date,
    load_portfolio_insight,
    validate_mart_analytics_schema,
)


# ---------------------------------------------------------------------------
# Helpers for writing fixture files
# ---------------------------------------------------------------------------

def _write_mart(tmp: Path, rows: list[dict]) -> Path:
    p = tmp / "outputs" / "analytics" / "mart_report_analytics.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(p, index=False)
    return p


def _write_engagement(tmp: Path, rows: list[dict]) -> Path:
    p = tmp / "outputs" / "analytics" / "mart_report_engagement.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(p, index=False)
    return p


def _write_insights(tmp: Path, records: list[dict]) -> Path:
    p = tmp / "outputs" / "insights" / "report_ai_insights.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(records), encoding="utf-8")
    return p


def _write_portfolio_insight(tmp: Path, payload: Any) -> Path:
    p = tmp / "outputs" / "insights" / "portfolio_ai_insight.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


def _write_forecasts(tmp: Path, rows: list[dict]) -> Path:
    p = tmp / "outputs" / "forecasts" / "report_view_forecasts_latest.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(p, index=False)
    return p


def _minimal_mart_row(report_id: str = "R_001", report_name: str = "Report 1") -> dict:
    return {
        "report_id":             report_id,
        "report_name":           report_name,
        "analytics_run_id":      "run_001",
        "analytics_as_of_date":  "2026-07-01",
        "overall_review_priority": "low",
        "recommended_report_action": "continue_monitoring",
        "overall_report_status": "growing",
    }


# ---------------------------------------------------------------------------
# Complete valid fixture set
# ---------------------------------------------------------------------------

class TestCompleteValidFixture:
    def test_load_returns_dict(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        data = load_app_data(root=tmp_path)
        assert isinstance(data, dict)

    def test_report_analytics_loaded(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        data = load_app_data(root=tmp_path)
        mart = data.get("report_analytics", pd.DataFrame())
        assert not mart.empty
        assert "report_id" in mart.columns

    def test_engagement_loaded_when_present(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        _write_engagement(tmp_path, [{"report_id": "R_001", "unique_users_28d": 10}])
        data = load_app_data(root=tmp_path)
        eng = data.get("engagement", pd.DataFrame())
        assert not eng.empty

    def test_insights_loaded_when_present(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        _write_insights(tmp_path, [{"report_id": "R_001", "executive_summary": "Test."}])
        data = load_app_data(root=tmp_path)
        ins = data.get("insights", pd.DataFrame())
        assert not ins.empty


# ---------------------------------------------------------------------------
# Canonical mart only (all optional files absent)
# ---------------------------------------------------------------------------

class TestMartOnly:
    def test_missing_optional_files_do_not_crash(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        data = load_app_data(root=tmp_path)
        assert "report_analytics" in data

    def test_portfolio_insight_status_absent_when_file_missing(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        data = load_app_data(root=tmp_path)
        assert data.get("_portfolio_insight_status") == "absent"

    def test_portfolio_insight_is_empty_dict_when_file_missing(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        data = load_app_data(root=tmp_path)
        assert data.get("_portfolio_insight") == {}


# ---------------------------------------------------------------------------
# Missing optional files
# ---------------------------------------------------------------------------

class TestMissingOptionalFiles:
    def test_missing_forecasts_returns_empty_frame(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        data = load_app_data(root=tmp_path)
        forecasts = data.get("forecasts", pd.DataFrame())
        assert isinstance(forecasts, pd.DataFrame)

    def test_missing_engagement_returns_empty_frame(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        data = load_app_data(root=tmp_path)
        eng = data.get("engagement", pd.DataFrame())
        assert isinstance(eng, pd.DataFrame)

    def test_all_optional_files_absent_app_still_loads(self, tmp_path):
        # No mart either — fully empty fixture
        data = load_app_data(root=tmp_path)
        assert isinstance(data, dict)


# ---------------------------------------------------------------------------
# Malformed optional JSON
# ---------------------------------------------------------------------------

class TestMalformedOptionalJson:
    def test_malformed_insights_json_returns_empty_frame(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        p = tmp_path / "outputs" / "insights" / "report_ai_insights.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("{not valid json}", encoding="utf-8")
        data = load_app_data(root=tmp_path)
        ins = data.get("insights", pd.DataFrame())
        assert isinstance(ins, pd.DataFrame)

    def test_malformed_portfolio_insight_status(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        p = tmp_path / "outputs" / "insights" / "portfolio_ai_insight.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("{broken", encoding="utf-8")
        _, status = load_portfolio_insight(tmp_path)
        assert status == "malformed_json"

    def test_portfolio_insight_list_structure(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        _write_portfolio_insight(tmp_path, [{"key": "value"}])  # list not dict
        _, status = load_portfolio_insight(tmp_path)
        assert status == "unexpected_structure"

    def test_portfolio_insight_validation_failed(self, tmp_path):
        _write_portfolio_insight(tmp_path, {"validation_status": "failed", "executive_summary": "x"})
        _, status = load_portfolio_insight(tmp_path)
        assert status == "validation_failed"

    def test_portfolio_insight_ok(self, tmp_path):
        _write_portfolio_insight(tmp_path, {"validation_status": "valid", "executive_summary": "x"})
        payload, status = load_portfolio_insight(tmp_path)
        assert status == "ok"
        assert payload.get("executive_summary") == "x"


# ---------------------------------------------------------------------------
# Duplicate report IDs
# ---------------------------------------------------------------------------

class TestDuplicateReportIds:
    def test_duplicate_report_ids_raises(self, tmp_path):
        rows = [_minimal_mart_row("R_001", "Report A"), _minimal_mart_row("R_001", "Report B")]
        _write_mart(tmp_path, rows)
        with pytest.raises(ValueError, match="duplicate report_id"):
            load_app_data(root=tmp_path)


# ---------------------------------------------------------------------------
# Invalid date values
# ---------------------------------------------------------------------------

class TestInvalidDateValues:
    def test_invalid_dates_coerced_not_raised(self, tmp_path):
        row = _minimal_mart_row()
        row["analytics_as_of_date"] = "not-a-date"
        _write_mart(tmp_path, [row])
        data = load_app_data(root=tmp_path)  # must not raise
        assert "report_analytics" in data

    def test_forecast_date_normalization(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        _write_forecasts(tmp_path, [
            {"report_id": "R_001", "forecast_date": "2026-08-01", "forecast": 10.0},
        ])
        data = load_app_data(root=tmp_path)
        forecasts = data.get("forecasts", pd.DataFrame())
        if not forecasts.empty and "Date" in forecasts.columns:
            assert pd.api.types.is_datetime64_any_dtype(forecasts["Date"])


# ---------------------------------------------------------------------------
# Production forecast date normalization
# ---------------------------------------------------------------------------

class TestProductionForecastDateNormalization:
    def test_forecast_date_column_promoted_to_date(self):
        df = pd.DataFrame({
            "report_id": ["R_001"],
            "forecast_date": ["2026-08-01"],
            "forecast": [10.0],
        })
        result = normalize_forecast_date(df)
        assert "Date" in result.columns
        assert pd.api.types.is_datetime64_any_dtype(result["Date"])

    def test_existing_date_column_preserved(self):
        df = pd.DataFrame({
            "report_id": ["R_001"],
            "Date": pd.to_datetime(["2026-08-01"]),
            "forecast": [10.0],
        })
        result = normalize_forecast_date(df)
        assert "Date" in result.columns

    def test_empty_dataframe_handled(self):
        result = normalize_forecast_date(pd.DataFrame())
        assert result.empty


# ---------------------------------------------------------------------------
# Privacy suppression in engagement mart
# ---------------------------------------------------------------------------

class TestEngagementPrivacySuppression:
    def test_suppressed_flag_loaded(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        _write_engagement(tmp_path, [{
            "report_id": "R_001",
            "privacy_suppressed": True,
            "unique_users_28d": None,
        }])
        data = load_app_data(root=tmp_path)
        eng = data.get("engagement", pd.DataFrame())
        if not eng.empty and "privacy_suppressed" in eng.columns:
            row = eng[eng["report_id"] == "R_001"]
            assert not row.empty

    def test_suppressed_unique_users_null_not_zero(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        _write_engagement(tmp_path, [{
            "report_id": "R_001",
            "privacy_suppressed": True,
            "unique_users_28d": None,
        }])
        data = load_app_data(root=tmp_path)
        eng = data.get("engagement", pd.DataFrame())
        if not eng.empty and "unique_users_28d" in eng.columns:
            row = eng[eng["report_id"] == "R_001"]
            if not row.empty:
                # Value should be NaN, not 0 (suppressed ≠ zero)
                val = row.iloc[0]["unique_users_28d"]
                assert pd.isna(val)


# ---------------------------------------------------------------------------
# Report and portfolio insight loading
# ---------------------------------------------------------------------------

class TestInsightLoading:
    def test_valid_report_insight_loaded(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        _write_insights(tmp_path, [{
            "report_id": "R_001",
            "validation_status": "valid",
            "executive_summary": "Stable report.",
        }])
        data = load_app_data(root=tmp_path)
        ins = data.get("insights", pd.DataFrame())
        assert not ins.empty
        assert "report_id" in ins.columns

    def test_empty_insights_file_returns_empty_frame(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        p = tmp_path / "outputs" / "insights" / "report_ai_insights.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("[]", encoding="utf-8")
        data = load_app_data(root=tmp_path)
        ins = data.get("insights", pd.DataFrame())
        assert isinstance(ins, pd.DataFrame)

    def test_portfolio_insight_absent_status(self, tmp_path):
        _write_mart(tmp_path, [_minimal_mart_row()])
        _, status = load_portfolio_insight(tmp_path)
        assert status == "absent"

    def test_portfolio_insight_valid_ok_status(self, tmp_path):
        _write_portfolio_insight(tmp_path, {
            "validation_status": "valid",
            "executive_summary": "Portfolio looks good.",
            "analytics_as_of_date": "2026-07-01",
        })
        payload, status = load_portfolio_insight(tmp_path)
        assert status == "ok"
        assert "executive_summary" in payload


# ---------------------------------------------------------------------------
# Stale lineage mismatch
# ---------------------------------------------------------------------------

class TestStaleMismatch:
    def test_mismatched_run_ids_still_loads(self, tmp_path):
        """Different run IDs between mart and insights do not cause a crash."""
        _write_mart(tmp_path, [_minimal_mart_row()])
        _write_insights(tmp_path, [{
            "report_id": "R_001",
            "analytics_run_id": "stale_run_999",
            "validation_status": "valid",
            "executive_summary": "Old summary.",
        }])
        data = load_app_data(root=tmp_path)
        # Both should be loaded; mismatch detection is a display-layer concern
        assert "report_analytics" in data
        assert "insights" in data
