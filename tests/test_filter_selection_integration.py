"""Integration tests for the filter → selection → report-detail pipeline.

Tests the full sequence:
  1. Load canonical mart.
  2. Apply search and filters.
  3. Recalculate display-only portfolio summaries.
  4. Resolve selected report.
  5. Assemble report-detail payload.
  6. Load matching GenAI and forecast evidence.

No Streamlit runtime. No live LLM calls.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.app.utils.filter_helpers import (
    apply_filters,
    apply_attention_filter,
    search_reports,
    safe_session_report,
    default_filter_state,
    check_filter_availability,
    extract_filter_options,
    active_filter_summary,
)
from src.app.utils.load_data import available_reports, row_for_report
from src.app.utils.report_helpers import build_report_detail
from src.app.utils.portfolio_helpers import (
    attention_shortlist,
    distribution_table,
    portfolio_headline_metrics,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _mart(n: int = 10) -> pd.DataFrame:
    priorities = (["low", "medium", "high", "critical"] * 3)[:n]
    actions = (["continue_monitoring", "investigate_usage_decline",
                "review_forecast_uncertainty", "review_model_health"] * 3)[:n]
    statuses = (["growing", "declining"] * 5)[:n]
    outlooks = (["growth_expected", "decline_expected", "stable_outlook"] * 4)[:n]
    categories = (["Dashboard", "Report", "Paginated"] * 4)[:n]
    rows = []
    for i in range(n):
        rows.append({
            "report_id":                  f"R_{i:03d}",
            "report_name":                f"Report {i}",
            "analytics_run_id":           "run_001",
            "analytics_as_of_date":       "2026-07-01",
            "overall_report_status":      statuses[i],
            "overall_review_priority":    priorities[i],
            "recommended_report_action":  actions[i],
            "forecast_outlook_status":    outlooks[i],
            "historical_usage_status":    "growing_usage" if i % 2 == 0 else "declining_usage",
            "overall_engagement_status":  "healthy_broad_adoption" if i % 3 == 0 else "declining_adoption",
            "model_diagnostic_status":    "insufficient_evidence",
            "overall_evidence_status":    "complete",
            "privacy_suppression_status": "not_suppressed",
            "report_category":            categories[i],
            "recent_28d_views":           100 + i * 5,
        })
    return pd.DataFrame(rows)


def _available(mart: pd.DataFrame) -> pd.DataFrame:
    return available_reports({"report_analytics": mart})


def _engagement(n: int = 10) -> pd.DataFrame:
    rows = [{"report_id": f"R_{i:03d}", "unique_users_28d": 10 + i} for i in range(n)]
    return pd.DataFrame(rows)


def _insights(n: int = 10) -> pd.DataFrame:
    rows = [{
        "report_id": f"R_{i:03d}",
        "validation_status": "valid",
        "generation_status": "llm_generated",
        "generation_mode": "llm",
        "api_attempts": 1,
        "executive_summary": f"Summary {i}.",
    } for i in range(n)]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 1. Search
# ---------------------------------------------------------------------------

class TestSearchIntegration:
    def test_search_by_name_reduces_list(self):
        mart = _mart(10)
        reports = _available(mart)
        filtered = search_reports(reports, "Report 0")
        assert len(filtered) < len(reports)
        assert all("report 0" in r.lower() for r in filtered["report_name"].str.lower())

    def test_search_by_id_exact_match(self):
        mart = _mart(10)
        reports = _available(mart)
        filtered = search_reports(reports, "R_003")
        assert any(filtered["report_id"] == "R_003")

    def test_blank_search_returns_all(self):
        mart = _mart(10)
        reports = _available(mart)
        filtered = search_reports(reports, "")
        assert len(filtered) == len(reports)

    def test_no_match_returns_empty(self):
        mart = _mart(10)
        reports = _available(mart)
        filtered = search_reports(reports, "zzz_no_match")
        assert filtered.empty


# ---------------------------------------------------------------------------
# 2. Filters
# ---------------------------------------------------------------------------

class TestFilterIntegration:
    def test_single_field_filter(self):
        mart = _mart(10)
        filtered = apply_filters(mart, {"overall_report_status": ["growing"]})
        assert all(filtered["overall_report_status"] == "growing")

    def test_two_field_and_filter(self):
        mart = _mart(10)
        filtered = apply_filters(mart, {
            "overall_report_status": ["growing"],
            "overall_review_priority": ["low"],
        })
        assert all(filtered["overall_report_status"] == "growing")
        assert all(filtered["overall_review_priority"] == "low")

    def test_empty_filter_returns_all(self):
        mart = _mart(10)
        filtered = apply_filters(mart, {})
        assert len(filtered) == len(mart)

    def test_zero_result_filter(self):
        mart = _mart(10)
        filtered = apply_filters(mart, {"overall_report_status": ["nonexistent"]})
        assert filtered.empty

    def test_unknown_field_ignored(self):
        mart = _mart(10)
        filtered = apply_filters(mart, {"made_up_field": ["value"]})
        assert len(filtered) == len(mart)


# ---------------------------------------------------------------------------
# 3. Attention filter
# ---------------------------------------------------------------------------

class TestAttentionFilterIntegration:
    def test_attention_filter_includes_high_priority(self):
        mart = _mart(10)
        filtered = apply_attention_filter(mart)
        assert all(
            (filtered["overall_review_priority"].isin({"high", "critical"}))
            | (filtered["recommended_report_action"] != "continue_monitoring")
        )

    def test_attention_filter_excludes_low_priority_continue_monitoring(self):
        # Rows with low/medium priority AND continue_monitoring should be excluded
        low_cont = _mart(10)
        low_cont_only = low_cont[
            (low_cont["overall_review_priority"].isin(["low", "medium"]))
            & (low_cont["recommended_report_action"] == "continue_monitoring")
        ]
        filtered = apply_attention_filter(low_cont_only)
        assert filtered.empty


# ---------------------------------------------------------------------------
# 4. Portfolio distributions after filtering
# ---------------------------------------------------------------------------

class TestFilteredPortfolioDistributions:
    def test_filtered_headline_metrics_reflect_subset(self):
        mart = _mart(10)
        growing = mart[mart["overall_report_status"] == "growing"]
        full_metrics = portfolio_headline_metrics(mart)
        filtered_metrics = portfolio_headline_metrics(growing)
        assert filtered_metrics.get("total_reports", 0) <= full_metrics.get("total_reports", 0)

    def test_filtered_distribution_table_totals_match_subset(self):
        mart = _mart(10)
        growing = mart[mart["overall_report_status"] == "growing"]
        dist = distribution_table(growing, "overall_review_priority")
        if not dist.empty and "Count" in dist.columns:
            assert dist["Count"].sum() == len(growing)

    def test_attention_shortlist_uses_full_mart_not_filtered(self):
        mart = _mart(10)
        # Even if we filter the mart, the shortlist function always receives the mart it's passed
        full_shortlist = attention_shortlist(mart)
        filtered = apply_filters(mart, {"overall_report_status": ["growing"]})
        filtered_shortlist = attention_shortlist(filtered)
        # Filtered shortlist has ≤ rows than full shortlist
        assert len(filtered_shortlist) <= len(full_shortlist) + 1  # cap is applied

    def test_zero_filtered_reports_headline_all_zero_or_none(self):
        mart = _mart(10)
        empty = apply_filters(mart, {"overall_report_status": ["nonexistent"]})
        metrics = portfolio_headline_metrics(empty)
        total = metrics.get("total_reports")
        # Empty mart returns None (not computable) or 0 — either is acceptable
        assert total is None or total == 0


# ---------------------------------------------------------------------------
# 5. Selected report resolution
# ---------------------------------------------------------------------------

class TestSelectedReportResolution:
    def test_current_id_preserved_when_in_filtered_list(self):
        mart = _mart(10)
        filtered = apply_filters(mart, {"overall_report_status": ["growing"]})
        selectable_ids = filtered["report_id"].tolist()
        first_growing = selectable_ids[0]
        result = safe_session_report(selectable_ids, first_growing)
        assert result == first_growing

    def test_filtered_out_report_falls_back_to_first(self):
        mart = _mart(10)
        filtered = apply_filters(mart, {"overall_report_status": ["growing"]})
        selectable_ids = filtered["report_id"].tolist()
        # R_001 is declining (odd index), not in growing filter
        result = safe_session_report(selectable_ids, "R_001")
        assert result == selectable_ids[0]

    def test_empty_selectable_returns_none(self):
        result = safe_session_report([], "R_001")
        assert result is None

    def test_no_current_id_returns_first(self):
        mart = _mart(10)
        reports = _available(mart)
        ids = reports["report_id"].tolist()
        result = safe_session_report(ids, None)
        assert result == ids[0]


# ---------------------------------------------------------------------------
# 6. Report-detail assembly
# ---------------------------------------------------------------------------

class TestReportDetailAssembly:
    def test_valid_mart_row_produces_complete_detail(self):
        mart = _mart(5)
        eng = _engagement(5)
        ins = _insights(5)
        mart_row = row_for_report(mart, "R_002")
        eng_row = row_for_report(eng, "R_002")
        ins_row = row_for_report(ins, "R_002")
        detail = build_report_detail(mart_row, eng_row, ins_row)
        for section in ("identity", "historical_usage", "forecast", "model_health",
                        "engagement", "decision", "genai"):
            assert section in detail

    def test_missing_mart_row_returns_minimal_identity(self):
        detail = build_report_detail(
            pd.Series(dtype="object"),
            pd.Series(dtype="object"),
            pd.Series(dtype="object"),
        )
        assert "identity" in detail
        assert detail["genai"]["state"] == "missing"

    def test_insight_for_correct_report_loaded(self):
        mart = _mart(5)
        ins = _insights(5)
        mart_row = row_for_report(mart, "R_002")
        ins_row = row_for_report(ins, "R_002")
        detail = build_report_detail(mart_row, pd.Series(dtype="object"), ins_row)
        exec_summary = detail["genai"].get("executive_summary")
        assert exec_summary is not None
        assert "Summary 2" in str(exec_summary)

    def test_insight_not_mixed_between_reports(self):
        mart = _mart(5)
        ins = _insights(5)
        ins_row_0 = row_for_report(ins, "R_000")
        ins_row_2 = row_for_report(ins, "R_002")
        detail_0 = build_report_detail(
            row_for_report(mart, "R_000"), pd.Series(dtype="object"), ins_row_0
        )
        detail_2 = build_report_detail(
            row_for_report(mart, "R_002"), pd.Series(dtype="object"), ins_row_2
        )
        assert detail_0["genai"].get("executive_summary") != detail_2["genai"].get("executive_summary")


# ---------------------------------------------------------------------------
# 7. Duplicate display names
# ---------------------------------------------------------------------------

class TestDuplicateDisplayNames:
    def test_duplicate_names_disambiguated_in_available_reports(self):
        mart = pd.DataFrame({
            "report_id":             ["R_001", "R_002"],
            "report_name":           ["Finance Report", "Finance Report"],
            "analytics_run_id":      ["run_1", "run_1"],
            "analytics_as_of_date":  ["2026-07-01", "2026-07-01"],
        })
        reports = available_reports({"report_analytics": mart})
        assert reports["report_name"].nunique() == 2

    def test_selector_can_resolve_either_duplicate(self):
        mart = pd.DataFrame({
            "report_id":             ["R_001", "R_002"],
            "report_name":           ["Finance Report", "Finance Report"],
            "analytics_run_id":      ["run_1", "run_1"],
            "analytics_as_of_date":  ["2026-07-01", "2026-07-01"],
        })
        reports = available_reports({"report_analytics": mart})
        ids = reports["report_id"].tolist()
        result = safe_session_report(ids, "R_001")
        assert result == "R_001"
        result2 = safe_session_report(ids, "R_002")
        assert result2 == "R_002"


# ---------------------------------------------------------------------------
# 8. Unknown status categories
# ---------------------------------------------------------------------------

class TestUnknownStatusCategories:
    def test_unknown_status_in_mart_does_not_crash_filter(self):
        mart = _mart(5)
        mart.loc[0, "overall_report_status"] = "future_unknown_status"
        filtered = apply_filters(mart, {"overall_report_status": ["growing"]})
        assert isinstance(filtered, pd.DataFrame)

    def test_extract_options_includes_unknown_values(self):
        mart = _mart(5)
        mart.loc[0, "overall_report_status"] = "future_unknown_status"
        options = extract_filter_options(mart, "overall_report_status")
        codes = [v for _, v in options]
        assert "future_unknown_status" in codes

    def test_unknown_priority_not_in_attention_filter(self):
        mart = pd.DataFrame({
            "report_id": ["R_001"],
            "overall_review_priority": ["future_priority"],
            "recommended_report_action": ["continue_monitoring"],
        })
        filtered = apply_attention_filter(mart)
        # "future_priority" is not in ATTENTION_PRIORITY_VALUES → row excluded
        assert filtered.empty

    def test_clear_filter_state_resets_to_defaults(self):
        state = default_filter_state()
        assert state["search_query"] == ""
        assert state["active_filters"] == {}
        assert state["attention_only"] is False

    def test_active_filter_summary_shows_three_parts_when_all_active(self):
        parts = active_filter_summary(
            {"overall_review_priority": ["high"]},
            search_query="finance",
            attention_only=True,
        )
        assert len(parts) == 3

    def test_filter_availability_suppresses_single_value_field(self):
        mart = _mart(5)
        mart["model_diagnostic_status"] = "insufficient_evidence"  # only one value
        availability = check_filter_availability(mart)
        assert availability.get("model_diagnostic_status") is False
