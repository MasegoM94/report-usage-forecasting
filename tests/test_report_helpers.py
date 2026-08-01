"""Tests for report-level explorer helpers.

Covers:
  - report_display_name: name extraction and fallback
  - fmt_pct_change: ratio formatting, sign, null
  - suppression_aware_metric: suppressed / insufficient / value / null
  - parse_report_reasons: pipe-separated pipeline reasons
  - is_field_suppressed: boolean flag columns and free-text field list
  - classify_genai_state: all six state codes
  - get_genai_field: Sprint-8 field and legacy alias fallback
  - build_report_detail: payload structure and section separation
  - available_reports ordering via load_data (mart preference)

No live LLM calls. No Streamlit imports.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.app.utils.report_helpers import (
    REPORT_GENAI_FIELDS,
    build_report_detail,
    classify_genai_state,
    fmt_pct_change,
    get_genai_field,
    is_field_suppressed,
    parse_report_reasons,
    report_display_name,
    suppression_aware_metric,
)
from src.app.utils.load_data import available_reports


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _mart_row(**overrides) -> pd.Series:
    base = {
        "report_id":                  "R_001",
        "report_name":                "Finance Dashboard",
        "workspace_name":             None,
        "criticality_level":          "high",
        "expected_usage_cadence":     "daily",
        "analytics_run_id":           "run-abc",
        "analytics_as_of_date":       "2026-03-31",
        "recent_28d_views":           1000.0,
        "previous_28d_views":         800.0,
        "usage_change_28d_pct":       0.25,
        "historical_usage_status":    "growing_usage",
        "days_since_last_use":        0.0,
        "current_zero_usage_streak_days": 0.0,
        "usage_volatility_status":    "low_volatility",
        "latest_usage_anomaly_status":"normal",
        "history_sufficient_28d":     True,
        "forecast_total_28d":         1050.0,
        "forecast_change_vs_actual_28d_pct": 0.05,
        "forecast_outlook_status":    "growth_expected",
        "forecast_uncertainty_status":"low_uncertainty",
        "forecast_interpretation_status": "sufficient_evidence",
        "selected_model_name":        "sarima",
        "available_forecast_horizon_days": 28,
        "forecast_lower_total_28d":   900.0,
        "forecast_upper_total_28d":   1200.0,
        "model_diagnostic_status":    "insufficient_evidence",
        "primary_model_issue":        "none",
        "bias_status":                None,
        "residual_autocorrelation_status": None,
        "interval_calibration_status": None,
        "production_evidence_maturity": "none",
        "production_deterioration_status": "no_deterioration",
        "model_evidence_status":      "insufficient",
        "unique_users_28d":           12.0,
        "active_user_direction_28d":  "increasing",
        "returning_user_share_28d":   0.6,
        "lapse_rate_28d":             0.1,
        "retained_user_rate_28d":     0.8,
        "views_per_active_user_28d":  84.0,
        "top_1_user_view_share_28d":  0.2,
        "overall_engagement_status":  "healthy_broad_adoption",
        "engagement_evidence_status": "complete",
        "privacy_suppression_status": "not_suppressed",
        "privacy_suppressed_fields":  "",
        "overall_report_status":      "growing",
        "overall_evidence_status":    "complete",
        "overall_review_priority":    "medium",
        "primary_diagnostic":         "high_forecast_uncertainty",
        "primary_diagnostic_category": "forecast",
        "recommended_report_action":  "continue_monitoring",
        "report_reasons":             "status:growing | issue:none | priority:medium",
    }
    base.update(overrides)
    return pd.Series(base)


def _eng_row(**overrides) -> pd.Series:
    base = {
        "report_id":                "R_001",
        "unique_users_28d":         12.0,
        "returning_user_share_28d": 0.6,
        "lapse_rate_28d":           0.1,
        "retained_user_rate_28d":   0.8,
        "top_1_user_view_share_28d":0.2,
        "views_per_active_user_28d":84.0,
        "active_user_direction_28d":"increasing",
        "overall_engagement_status":"healthy_broad_adoption",
        "engagement_evidence_status":"complete",
        "privacy_suppression_status":"not_suppressed",
        "activity_privacy_suppressed":  False,
        "cohort_privacy_suppressed":    False,
        "frequency_privacy_suppressed": False,
        "concentration_privacy_suppressed": False,
        "privacy_suppressed_fields": "",
    }
    base.update(overrides)
    return pd.Series(base)


def _insight_row(**overrides) -> pd.Series:
    base = {
        "report_id":          "R_001",
        "analytics_run_id":   "run-abc",
        "analytics_as_of_date": "2026-03-31",
        "prompt_version":     "report_insight_v1",
        "model_name":         "gpt-4.1-mini",
        "generated_at":       "2026-07-31T03:00:00Z",
        "genai_run_id":       "genai-xyz",
        "generation_status":  "success",
        "generation_mode":    "live_api",
        "validation_status":  "valid",
        "api_attempts":       1,
        "executive_summary":  "This report is growing steadily.",
        "usage_insight":      "28-day views increased 25%.",
        "engagement_insight": "Broad adoption with high return rate.",
        "forecast_insight":   "Growth expected to continue.",
        "model_confidence_note": "Insufficient backtest evidence; treat forecast directionally.",
        "recommended_action": "Continue monitoring.",
        "evidence_limitations": ["Insufficient model evidence for health assessment."],
        # Legacy aliases — present but should only be fallbacks
        "forecast_summary":   "Legacy summary text.",
        "recommended_actions":"Legacy action text.",
        "hypotheses":         "Legacy hypothesis text.",
        "confidence":         "high",
    }
    base.update(overrides)
    return pd.Series(base)


# ---------------------------------------------------------------------------
# TestReportDisplayName
# ---------------------------------------------------------------------------

class TestReportDisplayName:
    def test_uses_report_name_when_present(self):
        row = pd.Series({"report_id": "R_001", "report_name": "Finance Dashboard"})
        assert report_display_name(row) == "Finance Dashboard"

    def test_strips_whitespace(self):
        row = pd.Series({"report_id": "R_001", "report_name": "  Finance  "})
        assert report_display_name(row) == "Finance"

    def test_falls_back_to_report_id_when_name_is_null(self):
        row = pd.Series({"report_id": "R_001", "report_name": None})
        assert report_display_name(row) == "R_001"

    def test_falls_back_to_report_id_when_name_is_blank(self):
        row = pd.Series({"report_id": "R_001", "report_name": "   "})
        assert report_display_name(row) == "R_001"

    def test_returns_unknown_when_both_absent(self):
        row = pd.Series({"report_id": None, "report_name": None})
        assert report_display_name(row) == "Unknown report"

    def test_uses_report_id_when_name_key_missing(self):
        row = pd.Series({"report_id": "R_002"})
        assert report_display_name(row) == "R_002"


# ---------------------------------------------------------------------------
# TestFmtPctChange
# ---------------------------------------------------------------------------

class TestFmtPctChange:
    def test_none_returns_dash(self):
        assert fmt_pct_change(None) == "—"

    def test_nan_returns_dash(self):
        assert fmt_pct_change(float("nan")) == "—"

    def test_positive_ratio(self):
        assert fmt_pct_change(0.23) == "+23.0%"

    def test_negative_ratio(self):
        assert fmt_pct_change(-0.15) == "-15.0%"

    def test_zero(self):
        assert fmt_pct_change(0.0) == "0.0%"

    def test_string_input_raises_gracefully(self):
        result = fmt_pct_change("not_a_number")
        assert result == "—"

    def test_small_positive(self):
        result = fmt_pct_change(0.001)
        assert result == "+0.1%"


# ---------------------------------------------------------------------------
# TestSuppressionAwareMetric
# ---------------------------------------------------------------------------

class TestSuppressionAwareMetric:
    def test_suppressed_overrides_value(self):
        assert suppression_aware_metric(0.5, suppressed=True) == "Suppressed (privacy)"

    def test_suppressed_overrides_null(self):
        assert suppression_aware_metric(None, suppressed=True) == "Suppressed (privacy)"

    def test_insufficient_when_not_suppressed(self):
        assert suppression_aware_metric(0.5, insufficient=True) == "Insufficient history"

    def test_suppressed_takes_priority_over_insufficient(self):
        result = suppression_aware_metric(0.5, suppressed=True, insufficient=True)
        assert result == "Suppressed (privacy)"

    def test_null_value_returns_dash(self):
        assert suppression_aware_metric(None) == "—"

    def test_nan_value_returns_dash(self):
        assert suppression_aware_metric(float("nan")) == "—"

    def test_fmt_fn_applied_when_not_suppressed(self):
        result = suppression_aware_metric(0.6, fmt_fn=lambda v: f"{v * 100:.0f}%")
        assert result == "60%"

    def test_plain_value_returned_as_string_when_no_fmt(self):
        result = suppression_aware_metric(42)
        assert result == "42"


# ---------------------------------------------------------------------------
# TestParseReportReasons
# ---------------------------------------------------------------------------

class TestParseReportReasons:
    def test_parses_pipe_separated_string(self):
        raw = "status:growing | issue:none | priority:medium"
        result = parse_report_reasons(raw)
        assert result == ["status:growing", "issue:none", "priority:medium"]

    def test_handles_null(self):
        assert parse_report_reasons(None) == []

    def test_handles_blank_string(self):
        assert parse_report_reasons("   ") == []

    def test_handles_single_reason(self):
        assert parse_report_reasons("status:growing") == ["status:growing"]

    def test_handles_nan(self):
        assert parse_report_reasons(float("nan")) == []

    def test_trims_whitespace_from_parts(self):
        raw = "  key:value  |  other:thing  "
        result = parse_report_reasons(raw)
        assert result == ["key:value", "other:thing"]


# ---------------------------------------------------------------------------
# TestIsFieldSuppressed
# ---------------------------------------------------------------------------

class TestIsFieldSuppressed:
    def test_boolean_flag_true(self):
        row = pd.Series({"cohort_privacy_suppressed": True, "privacy_suppressed_fields": ""})
        assert is_field_suppressed(row, "cohort") is True

    def test_boolean_flag_false(self):
        row = pd.Series({"cohort_privacy_suppressed": False, "privacy_suppressed_fields": ""})
        assert is_field_suppressed(row, "cohort") is False

    def test_string_true_flag(self):
        row = pd.Series({"activity_privacy_suppressed": "true", "privacy_suppressed_fields": ""})
        assert is_field_suppressed(row, "activity") is True

    def test_free_text_field_list(self):
        row = pd.Series({"privacy_suppressed_fields": "concentration,cohort"})
        assert is_field_suppressed(row, "concentration") is True

    def test_free_text_field_not_in_list(self):
        row = pd.Series({"privacy_suppressed_fields": "cohort"})
        assert is_field_suppressed(row, "activity") is False

    def test_empty_row_returns_false(self):
        assert is_field_suppressed(pd.Series(dtype="object"), "cohort") is False


# ---------------------------------------------------------------------------
# TestClassifyGenaiState
# ---------------------------------------------------------------------------

class TestClassifyGenaiState:
    def test_valid_state(self):
        row = pd.Series({"generation_status": "success", "generation_mode": "live_api",
                         "validation_status": "valid", "api_attempts": 1})
        assert classify_genai_state(row) == "valid"

    def test_rule_based_via_generation_status(self):
        row = pd.Series({"generation_status": "rule_based", "generation_mode": "rule_based_fallback",
                         "validation_status": "valid", "api_attempts": 0})
        assert classify_genai_state(row) == "rule_based"

    def test_rule_based_via_generation_mode(self):
        row = pd.Series({"generation_status": "success", "generation_mode": "rule_based",
                         "validation_status": "valid"})
        assert classify_genai_state(row) == "rule_based"

    def test_fallback_via_generation_status(self):
        row = pd.Series({"generation_status": "fallback_api_error", "generation_mode": "",
                         "validation_status": "valid", "api_attempts": 3})
        assert classify_genai_state(row) == "fallback"

    def test_invalid_via_validation_status(self):
        row = pd.Series({"generation_status": "success", "generation_mode": "live_api",
                         "validation_status": "invalid"})
        assert classify_genai_state(row) == "invalid"

    def test_missing_empty_row(self):
        assert classify_genai_state(pd.Series(dtype="object")) == "missing"

    def test_reused_via_api_attempts_zero(self):
        row = pd.Series({"generation_status": "success", "generation_mode": "live_api",
                         "validation_status": "valid", "api_attempts": 0})
        assert classify_genai_state(row) == "reused"

    def test_reused_via_generation_mode(self):
        row = pd.Series({"generation_status": "success", "generation_mode": "reused_hash",
                         "validation_status": "valid", "api_attempts": 1})
        assert classify_genai_state(row) == "reused"


# ---------------------------------------------------------------------------
# TestGetGenaiField
# ---------------------------------------------------------------------------

class TestGetGenaiField:
    def test_returns_sprint8_field_when_present(self):
        row = pd.Series({"executive_summary": "A sprint-8 summary.", "forecast_summary": "Legacy."})
        assert get_genai_field(row, "executive_summary") == "A sprint-8 summary."

    def test_falls_back_to_alias_when_primary_absent(self):
        row = pd.Series({"forecast_summary": "Legacy summary.", "executive_summary": None})
        result = get_genai_field(row, "executive_summary")
        assert result == "Legacy summary."

    def test_returns_none_when_both_absent(self):
        row = pd.Series({"other_field": "value"})
        result = get_genai_field(row, "executive_summary")
        assert result is None

    def test_returns_none_for_empty_row(self):
        assert get_genai_field(pd.Series(dtype="object"), "executive_summary") is None

    def test_no_aliases_for_usage_insight(self):
        row = pd.Series({"usage_insight": "Usage went up."})
        assert get_genai_field(row, "usage_insight") == "Usage went up."

    def test_blank_primary_falls_to_alias(self):
        row = pd.Series({"executive_summary": "   ", "forecast_summary": "Legacy."})
        assert get_genai_field(row, "executive_summary") == "Legacy."


# ---------------------------------------------------------------------------
# TestBuildReportDetail
# ---------------------------------------------------------------------------

class TestBuildReportDetail:
    def test_returns_all_section_keys(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        assert set(detail.keys()) == {"identity", "historical_usage", "forecast",
                                       "model_health", "engagement", "decision", "genai"}

    def test_identity_section_populated(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        assert detail["identity"]["report_id"] == "R_001"
        assert detail["identity"]["report_name"] == "Finance Dashboard"

    def test_historical_usage_section_populated(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        hu = detail["historical_usage"]
        assert hu["recent_28d_views"] == 1000.0
        assert hu["historical_usage_status"] == "growing_usage"

    def test_forecast_section_populated(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        fc = detail["forecast"]
        assert fc["forecast_outlook_status"] == "growth_expected"
        assert fc["selected_model_name"] == "sarima"

    def test_model_health_section_populated(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        mh = detail["model_health"]
        assert mh["model_diagnostic_status"] == "insufficient_evidence"

    def test_engagement_section_populated(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        eng = detail["engagement"]
        assert eng["returning_user_share_28d"] == 0.6
        assert eng["_any_suppressed"] is False

    def test_engagement_suppression_detected(self):
        mart = _mart_row(privacy_suppression_status="suppressed")
        eng  = _eng_row(cohort_privacy_suppressed=True, activity_privacy_suppressed=True)
        detail = build_report_detail(mart, eng, _insight_row())
        assert detail["engagement"]["_cohort_suppressed"] is True
        assert detail["engagement"]["_any_suppressed"] is True

    def test_decision_section_populated(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        dec = detail["decision"]
        assert dec["recommended_report_action"] == "continue_monitoring"
        assert dec["overall_review_priority"] == "medium"

    def test_genai_section_state_valid(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        assert detail["genai"]["state"] == "valid"

    def test_genai_section_uses_sprint8_fields(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        assert detail["genai"]["executive_summary"] == "This report is growing steadily."
        # Legacy alias should NOT override when Sprint-8 field is present
        assert detail["genai"]["executive_summary"] != "Legacy summary text."

    def test_missing_optional_metadata_does_not_crash(self):
        mart = pd.Series({
            "report_id":              "R_999",
            "analytics_run_id":       "run-x",
            "analytics_as_of_date":   "2026-03-31",
            "overall_report_status":  "stable",
            "overall_review_priority":"low",
            "recommended_report_action": "continue_monitoring",
        })
        detail = build_report_detail(mart, pd.Series(dtype="object"), pd.Series(dtype="object"))
        assert detail["identity"]["report_id"] == "R_999"
        assert detail["identity"]["report_name"] is None
        assert detail["genai"]["state"] == "missing"

    def test_no_insight_returns_missing_genai_state(self):
        detail = build_report_detail(_mart_row(), _eng_row(), pd.Series(dtype="object"))
        assert detail["genai"]["state"] == "missing"

    def test_rule_based_insight_classified_correctly(self):
        insight = _insight_row(generation_status="rule_based", generation_mode="rule_based_fallback")
        detail = build_report_detail(_mart_row(), _eng_row(), insight)
        assert detail["genai"]["state"] == "rule_based"


# ---------------------------------------------------------------------------
# TestAnalyticalSeparation
# ---------------------------------------------------------------------------

class TestAnalyticalSeparation:
    """Verify that build_report_detail keeps analytical domains in separate sections."""

    def test_usage_not_in_engagement(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        # historical_usage_status must not appear in the engagement section
        assert "historical_usage_status" not in detail["engagement"]

    def test_engagement_not_in_forecast(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        assert "returning_user_share_28d" not in detail["forecast"]

    def test_model_health_not_in_engagement(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        assert "model_diagnostic_status" not in detail["engagement"]

    def test_decision_section_present_and_separate(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        dec = detail["decision"]
        # Decision fields must live in 'decision', not in other sections
        assert "recommended_report_action" in dec
        assert "recommended_report_action" not in detail["historical_usage"]
        assert "recommended_report_action" not in detail["engagement"]


# ---------------------------------------------------------------------------
# TestAvailableReportsMartPreference
# ---------------------------------------------------------------------------

class TestAvailableReportsMartPreference:
    """available_reports should prefer mart and guarantee one row per report_id."""

    def _make_data(self, mart_ids=None, metrics_ids=None):
        data = {}
        if mart_ids:
            data["report_analytics"] = pd.DataFrame({
                "report_id":   mart_ids,
                "report_name": [f"Report {i}" for i in mart_ids],
            })
        if metrics_ids:
            data["metrics"] = pd.DataFrame({
                "report_id":   metrics_ids,
                "report_name": [f"Metrics Report {i}" for i in metrics_ids],
            })
        return data

    def test_one_row_per_report_id(self):
        data = self._make_data(mart_ids=["R_001", "R_002", "R_003"])
        reports = available_reports(data)
        assert reports["report_id"].nunique() == len(reports)

    def test_ordering_is_by_report_id(self):
        data = self._make_data(mart_ids=["R_003", "R_001", "R_002"])
        reports = available_reports(data)
        ids = reports["report_id"].tolist()
        assert ids == sorted(ids)

    def test_mart_name_wins_over_legacy_source(self):
        """When both mart and metrics have the same report_id, mart name should win."""
        data = {
            "report_analytics": pd.DataFrame({
                "report_id":   ["R_001"],
                "report_name": ["Canonical Name"],
            }),
            "metrics": pd.DataFrame({
                "report_id":   ["R_001"],
                "report_name": ["Legacy Name"],
            }),
        }
        reports = available_reports(data)
        row = reports[reports["report_id"] == "R_001"]
        assert row.iloc[0]["report_name"] == "Canonical Name"

    def test_fallback_when_mart_absent(self):
        data = self._make_data(metrics_ids=["R_001"])
        reports = available_reports(data)
        assert len(reports) == 1
        assert reports.iloc[0]["report_id"] == "R_001"

    def test_label_column_present(self):
        data = self._make_data(mart_ids=["R_001"])
        reports = available_reports(data)
        assert "label" in reports.columns

    def test_null_report_name_falls_back_to_id(self):
        data = {
            "report_analytics": pd.DataFrame({
                "report_id":   ["R_001"],
                "report_name": [None],
            })
        }
        reports = available_reports(data)
        assert reports.iloc[0]["report_name"] == "R_001"

    def test_duplicate_display_names_disambiguated(self):
        data = {
            "report_analytics": pd.DataFrame({
                "report_id":   ["R_001", "R_002"],
                "report_name": ["Finance", "Finance"],
            })
        }
        reports = available_reports(data)
        names = reports["report_name"].tolist()
        assert len(set(names)) == 2   # both disambiguated


# ---------------------------------------------------------------------------
# TestForecastPresentation
# ---------------------------------------------------------------------------

class TestForecastPresentation:
    def test_prediction_interval_fields_in_forecast_section(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        fc = detail["forecast"]
        assert "forecast_lower_total_28d" in fc
        assert "forecast_upper_total_28d" in fc

    def test_forecast_only_data_no_crash(self):
        mart = _mart_row(forecast_total_28d=500.0, recent_28d_views=None,
                         previous_28d_views=None)
        detail = build_report_detail(mart, pd.Series(dtype="object"), pd.Series(dtype="object"))
        assert detail["forecast"]["forecast_total_28d"] == 500.0
        assert detail["historical_usage"]["recent_28d_views"] is None

    def test_selected_model_preserved(self):
        detail = build_report_detail(_mart_row(), _eng_row(), _insight_row())
        assert detail["forecast"]["selected_model_name"] == "sarima"

    def test_missing_metrics_does_not_crash(self):
        detail = build_report_detail(_mart_row(), pd.Series(dtype="object"), pd.Series(dtype="object"))
        assert detail is not None


# ---------------------------------------------------------------------------
# TestGenaiRenderingState
# ---------------------------------------------------------------------------

class TestGenaiRenderingState:
    def test_valid_success(self):
        row = _insight_row(generation_status="success", generation_mode="live_api",
                           validation_status="valid", api_attempts=1)
        assert classify_genai_state(row) == "valid"

    def test_reused_result(self):
        row = _insight_row(generation_status="success", generation_mode="live_api",
                           validation_status="valid", api_attempts=0)
        assert classify_genai_state(row) == "reused"

    def test_rule_based_result(self):
        row = _insight_row(generation_status="rule_based", generation_mode="rule_based_fallback",
                           validation_status="valid", api_attempts=0)
        assert classify_genai_state(row) == "rule_based"

    def test_fallback_result(self):
        row = _insight_row(generation_status="fallback_schema_invalid",
                           generation_mode="", validation_status="valid", api_attempts=3)
        assert classify_genai_state(row) == "fallback"

    def test_invalid_validation_status(self):
        row = _insight_row(validation_status="invalid")
        assert classify_genai_state(row) == "invalid"

    def test_missing_no_row(self):
        assert classify_genai_state(pd.Series(dtype="object")) == "missing"
