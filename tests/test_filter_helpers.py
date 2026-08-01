"""Tests for the sidebar filter helpers.

Covers:
  - apply_filters: single, multiple, multi-select, null, zero rows, unknown field
  - apply_attention_filter: attention logic, empty mart
  - search_reports: case-insensitive, name, ID, workspace, whitespace, no results
  - extract_filter_options: options present, excludes null, sorted
  - check_filter_availability: present, missing, insufficient distinct values
  - active_filter_summary: descriptions, combinations
  - safe_session_report: current preserved, fallback, empty
  - default_filter_state: structure

No Streamlit imports. No live LLM calls.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.app.utils.filter_helpers import (
    apply_attention_filter,
    apply_filters,
    active_filter_summary,
    check_filter_availability,
    default_filter_state,
    extract_filter_options,
    safe_session_report,
    search_reports,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_mart(n: int = 6) -> pd.DataFrame:
    rows = []
    statuses = ["growing_usage", "declining_usage", "stable_regular_usage"] * 4
    priorities = ["high", "medium", "low"] * 4
    actions = ["investigate_usage_decline", "continue_monitoring", "review_forecast_uncertainty"] * 4
    engagements = ["healthy_broad_adoption", "declining_adoption", "inactive"] * 4
    models = ["insufficient_evidence"] * 12
    privacies = ["not_suppressed", "suppressed", "not_suppressed"] * 4
    categories = ["Dashboard", "Report", "Paginated"] * 4
    evidences = ["complete"] * 12
    outlooks = ["growth_expected", "decline_expected", "stable_outlook"] * 4
    for i in range(n):
        rows.append({
            "report_id":                  f"R_{i:03d}",
            "report_name":                f"Report {i}",
            "workspace_name":             None,
            "report_category":            categories[i],
            "historical_usage_status":    statuses[i],
            "forecast_outlook_status":    outlooks[i],
            "overall_engagement_status":  engagements[i],
            "model_diagnostic_status":    models[i],
            "overall_report_status":      "growing" if i % 2 == 0 else "declining",
            "overall_evidence_status":    evidences[i],
            "overall_review_priority":    priorities[i],
            "recommended_report_action":  actions[i],
            "privacy_suppression_status": privacies[i],
        })
    return pd.DataFrame(rows[:n])


def _make_reports(n: int = 6) -> pd.DataFrame:
    mart = _make_mart(n)
    df = mart[["report_id", "report_name"]].copy()
    df["workspace_name"] = None
    df["label"] = df["report_name"] + " (" + df["report_id"] + ")"
    return df


# ---------------------------------------------------------------------------
# TestApplyFilters
# ---------------------------------------------------------------------------

class TestApplyFilters:
    def test_single_filter_one_value(self):
        mart = _make_mart(6)
        result = apply_filters(mart, {"overall_report_status": ["growing"]})
        assert all(result["overall_report_status"] == "growing")
        assert len(result) == 3

    def test_single_filter_multiple_values(self):
        mart = _make_mart(6)
        result = apply_filters(mart, {"overall_report_status": ["growing", "declining"]})
        assert len(result) == 6

    def test_two_filters_and_logic(self):
        mart = _make_mart(6)
        result = apply_filters(mart, {
            "overall_report_status": ["growing"],
            "overall_review_priority": ["high"],
        })
        # Only rows where both conditions hold
        assert all(result["overall_report_status"] == "growing")
        assert all(result["overall_review_priority"] == "high")

    def test_empty_filter_dict_returns_all(self):
        mart = _make_mart(6)
        result = apply_filters(mart, {})
        assert len(result) == len(mart)

    def test_empty_value_list_skipped(self):
        mart = _make_mart(6)
        result = apply_filters(mart, {"overall_report_status": []})
        assert len(result) == len(mart)

    def test_zero_matching_rows(self):
        mart = _make_mart(6)
        result = apply_filters(mart, {"overall_report_status": ["nonexistent"]})
        assert result.empty

    def test_unknown_filter_field_ignored(self):
        mart = _make_mart(6)
        result = apply_filters(mart, {"nonexistent_field": ["value"]})
        assert len(result) == len(mart)

    def test_empty_mart_returns_empty(self):
        result = apply_filters(pd.DataFrame(), {"overall_report_status": ["growing"]})
        assert result.empty

    def test_attention_filter_high_priority(self):
        mart = _make_mart(6)
        result = apply_filters(mart, {"overall_review_priority": ["high", "critical"]})
        assert all(result["overall_review_priority"].isin(["high", "critical"]))

    def test_evidence_status_filter(self):
        mart = _make_mart(6)
        result = apply_filters(mart, {"overall_evidence_status": ["complete"]})
        assert len(result) == 6  # all rows have "complete"

    def test_workspace_filter_skipped_when_all_null(self):
        mart = _make_mart(6)
        # workspace_name is all None — filter has no values to match
        result = apply_filters(mart, {"workspace_name": ["Finance"]})
        assert result.empty

    def test_multiselect_engagement_filter(self):
        mart = _make_mart(6)
        result = apply_filters(mart, {
            "overall_engagement_status": ["healthy_broad_adoption", "inactive"]
        })
        assert all(result["overall_engagement_status"].isin(
            ["healthy_broad_adoption", "inactive"]
        ))


# ---------------------------------------------------------------------------
# TestApplyAttentionFilter
# ---------------------------------------------------------------------------

class TestApplyAttentionFilter:
    def test_high_priority_included(self):
        mart = _make_mart(6)
        result = apply_attention_filter(mart)
        assert all(
            (result["overall_review_priority"].isin({"high", "critical"}))
            | (result["recommended_report_action"] != "continue_monitoring")
        )

    def test_critical_priority_included(self):
        mart = pd.DataFrame([{
            "report_id": "R_001",
            "overall_review_priority": "critical",
            "recommended_report_action": "continue_monitoring",
        }])
        result = apply_attention_filter(mart)
        assert len(result) == 1

    def test_continue_monitoring_medium_excluded(self):
        mart = pd.DataFrame([{
            "report_id": "R_001",
            "overall_review_priority": "medium",
            "recommended_report_action": "continue_monitoring",
        }])
        result = apply_attention_filter(mart)
        assert result.empty

    def test_low_priority_non_monitoring_action_included(self):
        mart = pd.DataFrame([{
            "report_id": "R_001",
            "overall_review_priority": "low",
            "recommended_report_action": "investigate_usage_decline",
        }])
        result = apply_attention_filter(mart)
        assert len(result) == 1

    def test_empty_mart_returns_empty(self):
        result = apply_attention_filter(pd.DataFrame())
        assert result.empty

    def test_missing_priority_column_uses_action(self):
        mart = pd.DataFrame([{
            "report_id": "R_001",
            "recommended_report_action": "investigate_usage_decline",
        }])
        result = apply_attention_filter(mart)
        assert len(result) == 1

    def test_missing_action_column_uses_priority(self):
        mart = pd.DataFrame([{
            "report_id": "R_001",
            "overall_review_priority": "high",
        }])
        result = apply_attention_filter(mart)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# TestSearchReports
# ---------------------------------------------------------------------------

class TestSearchReports:
    def test_case_insensitive_name_match(self):
        reports = _make_reports(6)
        result = search_reports(reports, "REPORT 0")
        assert len(result) >= 1
        assert all("report 0" in n.lower() for n in result["report_name"].str.lower())

    def test_report_id_match(self):
        reports = _make_reports(6)
        result = search_reports(reports, "R_001")
        assert any(result["report_id"] == "R_001")

    def test_partial_id_match(self):
        reports = _make_reports(6)
        result = search_reports(reports, "001")
        assert any(result["report_id"] == "R_001")

    def test_workspace_match_when_present(self):
        reports = _make_reports(3)
        reports["workspace_name"] = ["Finance Team", None, "Sales"]
        result = search_reports(reports, "finance")
        assert len(result) == 1
        assert result.iloc[0]["workspace_name"] == "Finance Team"

    def test_whitespace_stripped(self):
        reports = _make_reports(6)
        result_plain = search_reports(reports, "R_001")
        result_spaced = search_reports(reports, "  R_001  ")
        assert len(result_plain) == len(result_spaced)

    def test_no_results_returns_empty(self):
        reports = _make_reports(6)
        result = search_reports(reports, "zzz_no_match_xyz")
        assert result.empty

    def test_blank_query_returns_all(self):
        reports = _make_reports(6)
        result = search_reports(reports, "")
        assert len(result) == len(reports)

    def test_whitespace_only_query_returns_all(self):
        reports = _make_reports(6)
        result = search_reports(reports, "   ")
        assert len(result) == len(reports)

    def test_empty_reports_returns_empty(self):
        result = search_reports(pd.DataFrame(columns=["report_id", "report_name"]), "test")
        assert result.empty

    def test_duplicate_names_both_returned(self):
        reports = pd.DataFrame({
            "report_id":   ["R_001", "R_002"],
            "report_name": ["Finance Report", "Finance Report"],
            "label":       ["Finance Report (R_001)", "Finance Report (R_002)"],
        })
        result = search_reports(reports, "finance")
        assert len(result) == 2


# ---------------------------------------------------------------------------
# TestExtractFilterOptions
# ---------------------------------------------------------------------------

class TestExtractFilterOptions:
    def test_returns_non_null_values(self):
        mart = _make_mart(6)
        options = extract_filter_options(mart, "historical_usage_status")
        codes = [v for _, v in options]
        assert None not in codes
        assert "nan" not in codes

    def test_returns_sorted_options(self):
        mart = _make_mart(6)
        options = extract_filter_options(mart, "historical_usage_status")
        codes = [v for _, v in options]
        assert codes == sorted(codes)

    def test_includes_display_label(self):
        mart = _make_mart(6)
        options = extract_filter_options(mart, "historical_usage_status")
        labels = [lbl for lbl, _ in options]
        # All labels should be non-empty strings
        assert all(isinstance(l, str) and l.strip() for l in labels)

    def test_excludes_null_values(self):
        mart = _make_mart(6)
        mart.loc[0, "historical_usage_status"] = None
        options = extract_filter_options(mart, "historical_usage_status")
        codes = [v for _, v in options]
        assert None not in codes

    def test_empty_mart_returns_empty(self):
        options = extract_filter_options(pd.DataFrame(), "historical_usage_status")
        assert options == []

    def test_missing_field_returns_empty(self):
        mart = _make_mart(6)
        options = extract_filter_options(mart, "nonexistent_field")
        assert options == []

    def test_all_null_field_returns_empty(self):
        mart = _make_mart(6)
        mart["workspace_name"] = None
        options = extract_filter_options(mart, "workspace_name")
        assert options == []


# ---------------------------------------------------------------------------
# TestCheckFilterAvailability
# ---------------------------------------------------------------------------

class TestCheckFilterAvailability:
    def test_available_field_with_two_distinct_values(self):
        mart = _make_mart(6)
        availability = check_filter_availability(mart)
        assert availability.get("historical_usage_status") is True

    def test_field_with_only_one_value_unavailable(self):
        mart = _make_mart(6)
        mart["overall_evidence_status"] = "complete"  # only one distinct value
        availability = check_filter_availability(mart)
        assert availability.get("overall_evidence_status") is False

    def test_all_null_workspace_unavailable(self):
        mart = _make_mart(6)
        availability = check_filter_availability(mart)
        assert availability.get("workspace_name") is False

    def test_empty_mart_all_false(self):
        availability = check_filter_availability(pd.DataFrame())
        assert all(v is False for v in availability.values())

    def test_returns_entry_for_all_filterable_fields(self):
        from src.app.utils.filter_helpers import FILTERABLE_FIELDS
        mart = _make_mart(6)
        availability = check_filter_availability(mart)
        for field, _ in FILTERABLE_FIELDS:
            assert field in availability


# ---------------------------------------------------------------------------
# TestActiveFilterSummary
# ---------------------------------------------------------------------------

class TestActiveFilterSummary:
    def test_empty_returns_empty_list(self):
        assert active_filter_summary({}, "", False) == []

    def test_search_query_appears(self):
        parts = active_filter_summary({}, "finance", False)
        assert any("finance" in p.lower() for p in parts)

    def test_attention_only_appears(self):
        parts = active_filter_summary({}, "", True)
        assert any("attention" in p.lower() for p in parts)

    def test_single_filter_appears(self):
        parts = active_filter_summary(
            {"overall_review_priority": ["high"]}, "", False
        )
        assert any("review priority" in p.lower() or "priority" in p.lower() for p in parts)

    def test_multiple_filters_each_listed(self):
        parts = active_filter_summary(
            {"overall_review_priority": ["high"],
             "historical_usage_status": ["declining_usage"]},
            "", False,
        )
        assert len(parts) == 2

    def test_empty_value_list_not_shown(self):
        parts = active_filter_summary({"overall_review_priority": []}, "", False)
        assert parts == []

    def test_combined_search_filter_attention(self):
        parts = active_filter_summary(
            {"overall_review_priority": ["high"]},
            "finance",
            True,
        )
        assert len(parts) == 3


# ---------------------------------------------------------------------------
# TestSafeSessionReport
# ---------------------------------------------------------------------------

class TestSafeSessionReport:
    def test_current_id_preserved_when_in_list(self):
        ids = ["R_001", "R_002", "R_003"]
        assert safe_session_report(ids, "R_002") == "R_002"

    def test_fallback_to_first_when_current_missing(self):
        ids = ["R_001", "R_002", "R_003"]
        assert safe_session_report(ids, "R_999") == "R_001"

    def test_returns_none_when_list_empty(self):
        assert safe_session_report([], "R_001") is None

    def test_returns_none_when_list_empty_no_current(self):
        assert safe_session_report([], None) is None

    def test_first_available_when_no_current(self):
        ids = ["R_001", "R_002"]
        assert safe_session_report(ids, None) == "R_001"


# ---------------------------------------------------------------------------
# TestDefaultFilterState
# ---------------------------------------------------------------------------

class TestDefaultFilterState:
    def test_returns_expected_keys(self):
        state = default_filter_state()
        assert "search_query" in state
        assert "active_filters" in state
        assert "attention_only" in state

    def test_search_query_is_empty_string(self):
        assert default_filter_state()["search_query"] == ""

    def test_active_filters_is_empty_dict(self):
        assert default_filter_state()["active_filters"] == {}

    def test_attention_only_is_false(self):
        assert default_filter_state()["attention_only"] is False


# ---------------------------------------------------------------------------
# TestFilterAwareMetrics (filter counts and denominators)
# ---------------------------------------------------------------------------

class TestFilterAwareMetrics:
    def test_filtered_count_less_than_total(self):
        mart = _make_mart(6)
        filtered = apply_filters(mart, {"overall_report_status": ["growing"]})
        assert len(filtered) < len(mart)

    def test_filtered_count_correct(self):
        mart = _make_mart(6)
        growing = (mart["overall_report_status"] == "growing").sum()
        filtered = apply_filters(mart, {"overall_report_status": ["growing"]})
        assert len(filtered) == growing

    def test_zero_filtered_rows_no_crash(self):
        mart = _make_mart(6)
        filtered = apply_filters(mart, {"overall_report_status": ["nonexistent"]})
        assert filtered.empty

    def test_no_filter_returns_all(self):
        mart = _make_mart(6)
        filtered = apply_filters(mart, {})
        assert len(filtered) == len(mart)
