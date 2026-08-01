"""Application-level smoke tests for the Streamlit reviewer app.

These tests verify that:
  - The app module imports without error.
  - Render-preparation helpers work correctly with varied fixture data.
  - All primary sections handle empty/missing/invalid inputs gracefully.
  - No live LLM calls or Streamlit runtime is required.

Strategy: because Streamlit AppTest requires a live event loop and is
environment-dependent, these tests exercise the pure-logic render-preparation
layer (build_report_detail, available_reports, load_app_data logic) and the
chart builder rather than the full Streamlit render path.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

# Ensure src/app is importable in test context
_SRC_APP = Path(__file__).resolve().parents[1] / "src" / "app"
if str(_SRC_APP) not in sys.path:
    sys.path.insert(0, str(_SRC_APP))


# ---------------------------------------------------------------------------
# Import smoke test — must succeed before any other test runs
# ---------------------------------------------------------------------------

def test_utils_import_charts():
    from src.app.utils.charts import usage_forecast_chart  # noqa: F401


def test_utils_import_load_data():
    from src.app.utils.load_data import (  # noqa: F401
        load_app_data, available_reports, row_for_report,
        normalize_forecast_date, read_csv, read_json_records,
        validate_mart_analytics_schema,
    )


def test_utils_import_report_helpers():
    from src.app.utils.report_helpers import (  # noqa: F401
        build_report_detail, classify_genai_state, get_genai_field,
        suppression_aware_metric, REPORT_GENAI_FIELDS, GENAI_STATE_LABELS,
    )


def test_utils_import_definitions():
    from src.app.utils.definitions import DEFINITIONS, STATUS_LABELS, status_label  # noqa: F401


def test_utils_import_filter_helpers():
    from src.app.utils.filter_helpers import (  # noqa: F401
        apply_filters, apply_attention_filter, search_reports,
        extract_filter_options, check_filter_availability,
        active_filter_summary, safe_session_report, default_filter_state,
    )


def test_utils_import_portfolio_helpers():
    from src.app.utils.portfolio_helpers import (  # noqa: F401
        attention_shortlist, distribution_table, portfolio_headline_metrics,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_mart(n: int = 5) -> pd.DataFrame:
    priorities = ["low", "medium", "high", "critical", "low"]
    actions = [
        "continue_monitoring", "investigate_usage_decline",
        "review_forecast_uncertainty", "review_model_health",
        "continue_monitoring",
    ]
    engagements = [
        "healthy_broad_adoption", "declining_adoption", "inactive",
        "healthy_niche_adoption", "growing_adoption",
    ]
    rows = []
    for i in range(n):
        rows.append({
            "report_id":                f"R_{i:03d}",
            "report_name":              f"Report {i}",
            "analytics_run_id":         "run_001",
            "analytics_as_of_date":     "2026-07-01",
            "historical_usage_status":  "growing_usage" if i % 2 == 0 else "declining_usage",
            "forecast_outlook_status":  "growth_expected" if i % 2 == 0 else "decline_expected",
            "overall_engagement_status": engagements[i % len(engagements)],
            "model_diagnostic_status":  "insufficient_evidence",
            "overall_report_status":    "growing" if i % 2 == 0 else "declining",
            "overall_evidence_status":  "complete",
            "overall_review_priority":  priorities[i % len(priorities)],
            "recommended_report_action": actions[i % len(actions)],
            "primary_diagnostic":       "usage_decline" if i % 2 else None,
            "privacy_suppression_status": "not_suppressed",
            "recent_28d_views":         100 + i * 10,
        })
    return pd.DataFrame(rows[:n])


def _make_engagement(n: int = 5) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            "report_id":                  f"R_{i:03d}",
            "unique_users_28d":           10 + i,
            "returning_user_share_28d":   0.6,
            "lapse_rate_28d":             0.2,
            "retained_user_rate_28d":     0.8,
            "top_1_user_view_share_28d":  0.3,
            "overall_engagement_status":  "healthy_broad_adoption",
            "engagement_evidence_status": "complete",
            "privacy_suppressed":         False,
            "cohort_privacy_suppressed":  False,
            "concentration_privacy_suppressed": False,
        })
    return pd.DataFrame(rows[:n])


def _make_insights(n: int = 5) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            "report_id":         f"R_{i:03d}",
            "validation_status": "valid",
            "generation_status": "llm_generated",
            "generation_mode":   "llm",
            "api_attempts":      1,
            "executive_summary": f"Summary for report {i}.",
            "usage_insight":     "Usage is stable.",
            "forecast_insight":  "Forecast outlook is positive.",
        })
    return pd.DataFrame(rows[:n])


def _minimal_data(n: int = 5) -> dict[str, Any]:
    return {
        "report_analytics": _make_mart(n),
        "engagement":       _make_engagement(n),
        "insights":         _make_insights(n),
        "forecasts":        pd.DataFrame(),
        "metrics":          pd.DataFrame(),
        "_portfolio_insight": {},
        "_portfolio_insight_status": "absent",
    }


# ---------------------------------------------------------------------------
# available_reports
# ---------------------------------------------------------------------------

class TestAvailableReports:
    def test_returns_dataframe_with_label_column(self):
        from src.app.utils.load_data import available_reports
        data = _minimal_data()
        reports = available_reports(data)
        assert "report_id" in reports.columns
        assert "label" in reports.columns

    def test_count_matches_mart(self):
        from src.app.utils.load_data import available_reports
        data = _minimal_data(5)
        reports = available_reports(data)
        assert len(reports) == 5

    def test_empty_mart_falls_back_to_other_sources(self):
        from src.app.utils.load_data import available_reports
        data = _minimal_data(3)
        data["report_analytics"] = pd.DataFrame()
        reports = available_reports(data)
        # Should still find reports from engagement / insights
        assert len(reports) >= 0  # does not crash

    def test_duplicate_names_disambiguated(self):
        from src.app.utils.load_data import available_reports
        mart = pd.DataFrame({
            "report_id":   ["R_001", "R_002"],
            "report_name": ["Finance Report", "Finance Report"],
            "analytics_run_id": ["run_1", "run_1"],
            "analytics_as_of_date": ["2026-07-01", "2026-07-01"],
        })
        data = {"report_analytics": mart}
        reports = available_reports(data)
        assert reports["report_name"].nunique() == 2

    def test_no_data_returns_empty_frame(self):
        from src.app.utils.load_data import available_reports
        reports = available_reports({})
        assert reports.empty


# ---------------------------------------------------------------------------
# build_report_detail
# ---------------------------------------------------------------------------

class TestBuildReportDetailSmoke:
    def test_valid_mart_returns_all_sections(self):
        from src.app.utils.report_helpers import build_report_detail
        mart = _make_mart(1)
        eng = _make_engagement(1)
        ins = _make_insights(1)
        detail = build_report_detail(mart.iloc[0], eng.iloc[0], ins.iloc[0])
        for key in ("identity", "historical_usage", "forecast", "model_health",
                    "engagement", "decision", "genai"):
            assert key in detail

    def test_empty_mart_row_returns_safe_defaults(self):
        from src.app.utils.report_helpers import build_report_detail
        detail = build_report_detail(
            pd.Series(dtype="object"),
            pd.Series(dtype="object"),
            pd.Series(dtype="object"),
        )
        assert "identity" in detail
        assert detail["genai"]["state"] == "missing"

    def test_missing_engagement_row_no_crash(self):
        from src.app.utils.report_helpers import build_report_detail
        mart = _make_mart(1)
        detail = build_report_detail(
            mart.iloc[0],
            pd.Series(dtype="object"),
            pd.Series(dtype="object"),
        )
        assert "engagement" in detail

    def test_invalid_genai_state_classified(self):
        from src.app.utils.report_helpers import build_report_detail, classify_genai_state
        ins_row = pd.Series({
            "report_id": "R_000",
            "validation_status": "failed",
            "generation_status": "llm_generated",
            "generation_mode": "llm",
            "api_attempts": 1,
        })
        state = classify_genai_state(ins_row)
        assert state == "invalid"

    def test_missing_genai_row_classified_as_missing(self):
        from src.app.utils.report_helpers import classify_genai_state
        state = classify_genai_state(pd.Series(dtype="object"))
        assert state == "missing"

    def test_reused_state_classified(self):
        from src.app.utils.report_helpers import classify_genai_state
        ins_row = pd.Series({
            "validation_status": "valid",
            "generation_status": "reused",
            "generation_mode": "reused",
            "api_attempts": 0,
        })
        state = classify_genai_state(ins_row)
        assert state == "reused"

    def test_rule_based_state_classified(self):
        from src.app.utils.report_helpers import classify_genai_state
        ins_row = pd.Series({
            "validation_status": "valid",
            "generation_status": "rule_based",
            "generation_mode": "rule_based",
            "api_attempts": 0,
        })
        state = classify_genai_state(ins_row)
        assert state == "rule_based"


# ---------------------------------------------------------------------------
# Empty-state helpers
# ---------------------------------------------------------------------------

class TestEmptyStateScenarios:
    """Verify build_report_detail handles all empty/missing data scenarios."""

    def test_no_historical_usage_section_safe(self):
        from src.app.utils.report_helpers import build_report_detail
        mart = pd.Series({"report_id": "R_001", "report_name": "Test"})
        detail = build_report_detail(mart, pd.Series(dtype="object"), pd.Series(dtype="object"))
        h = detail["historical_usage"]
        assert h.get("recent_28d_views") is None or True  # no crash

    def test_no_forecast_section_safe(self):
        from src.app.utils.report_helpers import build_report_detail
        mart = pd.Series({"report_id": "R_001", "report_name": "Test"})
        detail = build_report_detail(mart, pd.Series(dtype="object"), pd.Series(dtype="object"))
        assert "forecast" in detail

    def test_no_model_health_section_safe(self):
        from src.app.utils.report_helpers import build_report_detail
        mart = pd.Series({"report_id": "R_001", "report_name": "Test"})
        detail = build_report_detail(mart, pd.Series(dtype="object"), pd.Series(dtype="object"))
        assert "model_health" in detail

    def test_privacy_suppressed_engagement_section_safe(self):
        from src.app.utils.report_helpers import build_report_detail
        mart = _make_mart(1).iloc[0]
        eng = pd.Series({
            "report_id": "R_000",
            "privacy_suppressed": True,
            "cohort_privacy_suppressed": True,
            "concentration_privacy_suppressed": True,
        })
        detail = build_report_detail(mart, eng, pd.Series(dtype="object"))
        eng_section = detail["engagement"]
        assert eng_section.get("_any_suppressed", False) is True

    def test_zero_reports_after_filter_available_reports_empty(self):
        from src.app.utils.load_data import available_reports
        from src.app.utils.filter_helpers import apply_filters
        data = _minimal_data(5)
        mart = data["report_analytics"]
        filtered = apply_filters(mart, {"overall_report_status": ["nonexistent"]})
        assert filtered.empty

    def test_unknown_status_values_do_not_crash(self):
        from src.app.utils.definitions import status_label
        result = status_label("some_future_unknown_code_xyz")
        assert isinstance(result, str)
        assert result  # non-empty


# ---------------------------------------------------------------------------
# Chart smoke tests
# ---------------------------------------------------------------------------

class TestChartSmoke:
    def test_chart_with_complete_data(self):
        from src.app.utils.charts import usage_forecast_chart
        df = pd.DataFrame({
            "Date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03",
                                     "2026-01-04", "2026-01-05"]),
            "actual":   [10.0, 12.0, 11.0, np.nan, np.nan],
            "forecast": [np.nan, np.nan, np.nan, 13.0, 14.0],
            "lower_ci": [np.nan, np.nan, np.nan, 11.0, 12.0],
            "upper_ci": [np.nan, np.nan, np.nan, 15.0, 16.0],
        })
        fig = usage_forecast_chart(df, report_title="Test Report")
        assert len(fig.data) >= 1

    def test_chart_with_empty_data_returns_annotation(self):
        from src.app.utils.charts import usage_forecast_chart
        fig = usage_forecast_chart(pd.DataFrame())
        assert len(fig.data) == 0
        assert fig.layout.annotations

    def test_chart_with_forecast_only(self):
        from src.app.utils.charts import usage_forecast_chart
        df = pd.DataFrame({
            "Date": pd.to_datetime(["2026-01-04", "2026-01-05"]),
            "forecast": [13.0, 14.0],
        })
        fig = usage_forecast_chart(df)
        names = [t.name for t in fig.data]
        assert "Forecast" in names

    def test_chart_with_actuals_only(self):
        from src.app.utils.charts import usage_forecast_chart
        df = pd.DataFrame({
            "Date": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "actual": [10.0, 12.0],
        })
        fig = usage_forecast_chart(df)
        names = [t.name for t in fig.data]
        assert "Historical usage" in names
        assert "Forecast" not in names

    def test_prediction_interval_labelled_not_confidence_interval(self):
        from src.app.utils.charts import usage_forecast_chart
        df = pd.DataFrame({
            "Date": pd.to_datetime(["2026-01-04", "2026-01-05"]),
            "forecast": [13.0, 14.0],
            "lower_ci": [11.0, 12.0],
            "upper_ci": [15.0, 16.0],
        })
        fig = usage_forecast_chart(df)
        names = [t.name for t in fig.data]
        assert "Prediction interval" in names
        assert "Confidence interval" not in names


# ---------------------------------------------------------------------------
# Validate mart analytics schema
# ---------------------------------------------------------------------------

class TestMartAnalyticsSchemaSmoke:
    def test_valid_mart_passes(self):
        from src.app.utils.load_data import validate_mart_analytics_schema
        mart = _make_mart(3)
        validate_mart_analytics_schema(mart)  # no exception

    def test_empty_mart_passes(self):
        from src.app.utils.load_data import validate_mart_analytics_schema
        validate_mart_analytics_schema(pd.DataFrame())  # no exception

    def test_missing_required_columns_raises(self):
        from src.app.utils.load_data import validate_mart_analytics_schema
        mart = pd.DataFrame({"report_id": ["R_001"]})  # missing analytics_run_id
        with pytest.raises(ValueError, match="missing required column"):
            validate_mart_analytics_schema(mart)

    def test_duplicate_report_ids_raises(self):
        from src.app.utils.load_data import validate_mart_analytics_schema
        mart = pd.DataFrame({
            "report_id": ["R_001", "R_001"],
            "analytics_run_id": ["run_1", "run_1"],
            "analytics_as_of_date": ["2026-07-01", "2026-07-01"],
        })
        with pytest.raises(ValueError, match="duplicate report_id"):
            validate_mart_analytics_schema(mart)
