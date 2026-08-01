"""Tests for portfolio overview loading, metrics, and attention shortlist.

Covers:
  - load_portfolio_insight: all status codes
  - _portfolio_headline_metrics: counts from mart
  - _attention_shortlist: ordering, cap, empty cases
  - _distribution_table: structure and ordering
  - validate_portfolio_mart_fields: field presence detection
  - No live LLM calls anywhere in this file.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.app.utils.load_data import (
    load_portfolio_insight,
    validate_portfolio_mart_fields,
    PORTFOLIO_NARRATIVE_FIELDS,
    PORTFOLIO_LINEAGE_FIELDS,
)
from src.app.utils.portfolio_helpers import (
    portfolio_headline_metrics as _portfolio_headline_metrics,
    attention_shortlist as _attention_shortlist,
    distribution_table as _distribution_table,
    status_label as _status_label,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _valid_insight_dict() -> dict:
    return {
        "analytics_run_id":          "abc-123",
        "analytics_as_of_date":      "2026-03-31",
        "report_count":              30,
        "prompt_version":            "portfolio_insight_v1",
        "model_name":                "gpt-4.1-mini",
        "generation_status":         "rule_based",
        "generation_mode":           "rule_based_fallback",
        "validation_status":         "valid",
        "executive_summary":         "Portfolio contains 30 reports.",
        "portfolio_usage_summary":   "11 growing, 9 declining.",
        "portfolio_engagement_summary": "Mostly healthy broad adoption.",
        "portfolio_forecast_summary":   "20 reports expected to grow.",
        "portfolio_model_health_summary": "All 30 have insufficient evidence.",
        "priority_actions":          ["Investigate 7 declining reports"],
        "positive_signals":          ["11 reports growing"],
        "evidence_limitations":      ["Model evidence insufficient for all reports"],
        "genai_run_id":              "ggg-999",
        "generated_at":              "2026-07-31T03:23:01Z",
    }


def _make_mart(n: int = 5, **overrides) -> pd.DataFrame:
    """Build a minimal mart DataFrame with n rows."""
    rows = []
    for i in range(n):
        rid = f"R_{i:03d}"
        rows.append({
            "report_id":               rid,
            "report_name":             f"Report {i}",
            "analytics_run_id":        "run-abc",
            "analytics_as_of_date":    "2026-03-31",
            "historical_usage_status": "growing_usage" if i % 2 == 0 else "declining_usage",
            "forecast_outlook_status": "growth_expected",
            "overall_engagement_status": "healthy_broad_adoption",
            "model_diagnostic_status": "insufficient_evidence",
            "overall_report_status":   "growing" if i % 2 == 0 else "declining",
            "overall_evidence_status": "complete",
            "overall_review_priority": "high" if i < 2 else "medium",
            "recommended_report_action": (
                "investigate_usage_decline" if i < 3 else "continue_monitoring"
            ),
            "primary_diagnostic":      "severe_historical_decline" if i < 2 else "none",
            "privacy_suppression_status": "suppressed" if i == 0 else "not_suppressed",
            "recent_28d_views":        100 if i > 0 else 0,
        })
    df = pd.DataFrame(rows)
    for k, v in overrides.items():
        df[k] = v
    return df


# ---------------------------------------------------------------------------
# load_portfolio_insight
# ---------------------------------------------------------------------------

class TestLoadPortfolioInsight:
    def test_valid_file_returns_ok(self, tmp_path):
        insight = _valid_insight_dict()
        (tmp_path / "outputs" / "insights").mkdir(parents=True)
        (tmp_path / "outputs" / "insights" / "portfolio_ai_insight.json").write_text(
            json.dumps(insight)
        )
        payload, status = load_portfolio_insight(root=tmp_path)
        assert status == "ok"
        assert payload["report_count"] == 30

    def test_missing_file_returns_absent(self, tmp_path):
        _, status = load_portfolio_insight(root=tmp_path)
        assert status == "absent"

    def test_empty_file_returns_empty(self, tmp_path):
        (tmp_path / "outputs" / "insights").mkdir(parents=True)
        (tmp_path / "outputs" / "insights" / "portfolio_ai_insight.json").write_text("   ")
        _, status = load_portfolio_insight(root=tmp_path)
        assert status == "empty"

    def test_malformed_json_returns_malformed(self, tmp_path):
        (tmp_path / "outputs" / "insights").mkdir(parents=True)
        (tmp_path / "outputs" / "insights" / "portfolio_ai_insight.json").write_text(
            "{not valid json"
        )
        payload, status = load_portfolio_insight(root=tmp_path)
        assert status == "malformed_json"
        assert payload == {}

    def test_json_list_returns_unexpected_structure(self, tmp_path):
        (tmp_path / "outputs" / "insights").mkdir(parents=True)
        (tmp_path / "outputs" / "insights" / "portfolio_ai_insight.json").write_text(
            json.dumps([{"report_id": "R001"}])
        )
        payload, status = load_portfolio_insight(root=tmp_path)
        assert status == "unexpected_structure"

    def test_invalid_validation_status_returns_validation_failed(self, tmp_path):
        insight = _valid_insight_dict()
        insight["validation_status"] = "invalid"
        (tmp_path / "outputs" / "insights").mkdir(parents=True)
        (tmp_path / "outputs" / "insights" / "portfolio_ai_insight.json").write_text(
            json.dumps(insight)
        )
        payload, status = load_portfolio_insight(root=tmp_path)
        assert status == "validation_failed"
        # Payload is still returned so the caller can display a warning + the data
        assert payload["report_count"] == 30

    def test_fallback_generation_status_still_ok(self, tmp_path):
        insight = _valid_insight_dict()
        insight["generation_status"] = "rule_based"
        (tmp_path / "outputs" / "insights").mkdir(parents=True)
        (tmp_path / "outputs" / "insights" / "portfolio_ai_insight.json").write_text(
            json.dumps(insight)
        )
        _, status = load_portfolio_insight(root=tmp_path)
        # A rule_based generation is still valid if validation_status == "valid"
        assert status == "ok"

    def test_missing_narrative_fields_do_not_crash(self, tmp_path):
        # Minimal dict — no narrative fields
        minimal = {
            "analytics_run_id": "x",
            "analytics_as_of_date": "2026-03-31",
            "report_count": 5,
            "validation_status": "valid",
        }
        (tmp_path / "outputs" / "insights").mkdir(parents=True)
        (tmp_path / "outputs" / "insights" / "portfolio_ai_insight.json").write_text(
            json.dumps(minimal)
        )
        payload, status = load_portfolio_insight(root=tmp_path)
        assert status == "ok"
        # All narrative fields return None gracefully
        for field in PORTFOLIO_NARRATIVE_FIELDS:
            assert payload.get(field) is None


# ---------------------------------------------------------------------------
# _portfolio_headline_metrics
# ---------------------------------------------------------------------------

class TestPortfolioHeadlineMetrics:
    def test_total_count_matches_mart(self):
        mart = _make_mart(n=5)
        m = _portfolio_headline_metrics(mart)
        assert m["total_reports"] == 5

    def test_with_recent_usage_counts_nonzero(self):
        mart = _make_mart(n=5)
        # Row 0 has recent_28d_views=0, rows 1-4 have 100
        m = _portfolio_headline_metrics(mart)
        assert m["with_recent_usage"] == 4

    def test_requiring_review_counts_non_monitoring(self):
        mart = _make_mart(n=5)
        # Rows 0, 1, 2 have investigate_usage_decline; rows 3, 4 have continue_monitoring
        m = _portfolio_headline_metrics(mart)
        assert m["requiring_review"] == 3

    def test_high_priority_counts_high_and_critical(self):
        mart = _make_mart(n=5)
        # Rows 0, 1 have priority=high; rows 2-4 have medium
        m = _portfolio_headline_metrics(mart)
        assert m["high_priority"] == 2

    def test_privacy_suppressed_counts_suppressed(self):
        mart = _make_mart(n=5)
        # Only row 0 has suppressed
        m = _portfolio_headline_metrics(mart)
        assert m["privacy_suppressed"] == 1

    def test_empty_mart_returns_none_values(self):
        m = _portfolio_headline_metrics(pd.DataFrame())
        assert m["total_reports"] is None
        assert m["with_recent_usage"] is None

    def test_analytics_as_of_date_extracted(self):
        mart = _make_mart(n=3)
        m = _portfolio_headline_metrics(mart)
        assert m["analytics_as_of_date"] == "2026-03-31"

    def test_missing_optional_field_returns_none_not_crash(self):
        mart = _make_mart(n=3)
        mart = mart.drop(columns=["recent_28d_views"])
        m = _portfolio_headline_metrics(mart)
        assert m["with_recent_usage"] is None  # graceful degradation

    def test_zero_division_safe_when_all_null(self):
        mart = _make_mart(n=3)
        mart["recent_28d_views"] = np.nan
        m = _portfolio_headline_metrics(mart)
        assert m["with_recent_usage"] == 0


# ---------------------------------------------------------------------------
# _attention_shortlist
# ---------------------------------------------------------------------------

class TestAttentionShortlist:
    def test_shortlist_capped_at_five(self):
        mart = _make_mart(n=10)
        mart["recommended_report_action"] = "investigate_usage_decline"
        mart["overall_review_priority"] = "high"
        sl = _attention_shortlist(mart, cap=5)
        assert len(sl) <= 5

    def test_continue_monitoring_excluded(self):
        mart = _make_mart(n=5)
        # All continue_monitoring
        mart["recommended_report_action"] = "continue_monitoring"
        sl = _attention_shortlist(mart)
        assert sl.empty

    def test_high_priority_before_medium(self):
        mart = _make_mart(n=4)
        mart.loc[2, "overall_review_priority"] = "high"
        mart.loc[2, "recommended_report_action"] = "investigate_usage_decline"
        mart.loc[3, "overall_review_priority"] = "medium"
        mart.loc[3, "recommended_report_action"] = "investigate_usage_decline"
        mart.loc[0, "recommended_report_action"] = "continue_monitoring"
        mart.loc[1, "recommended_report_action"] = "continue_monitoring"
        sl = _attention_shortlist(mart)
        # First row must be the high-priority one
        assert sl.iloc[0]["overall_review_priority"] == "high"

    def test_stored_order_not_reranked_by_score(self):
        """Shortlist uses deterministic sort on existing fields, not a computed score."""
        mart = _make_mart(n=6)
        mart["recommended_report_action"] = "investigate_usage_decline"
        mart["overall_review_priority"] = "medium"
        sl = _attention_shortlist(mart, cap=5)
        # Order should be alphabetical by report_id within same priority tier
        ids = sl["report_id"].tolist()
        assert ids == sorted(ids)

    def test_empty_mart_returns_empty(self):
        sl = _attention_shortlist(pd.DataFrame())
        assert sl.empty

    def test_shortlist_includes_required_columns(self):
        mart = _make_mart(n=3)
        mart["recommended_report_action"] = "investigate_usage_decline"
        sl = _attention_shortlist(mart)
        expected = {"report_id", "overall_review_priority", "recommended_report_action"}
        assert expected.issubset(sl.columns)

    def test_total_actionable_exceeds_cap_message_possible(self):
        """When there are more actionable reports than cap, the cap applies."""
        mart = _make_mart(n=10)
        mart["recommended_report_action"] = "investigate_usage_decline"
        mart["overall_review_priority"] = "high"
        sl = _attention_shortlist(mart, cap=3)
        assert len(sl) == 3  # capped at 3, not all 10


# ---------------------------------------------------------------------------
# _distribution_table
# ---------------------------------------------------------------------------

class TestDistributionTable:
    def test_returns_three_columns(self):
        mart = _make_mart(n=4)
        df = _distribution_table(mart, "historical_usage_status")
        assert list(df.columns) == ["Status", "Count", "Share %"]

    def test_count_sum_equals_mart_rows(self):
        mart = _make_mart(n=6)
        df = _distribution_table(mart, "historical_usage_status")
        assert df["Count"].sum() == 6

    def test_missing_column_returns_empty(self):
        df = _distribution_table(_make_mart(n=3), "nonexistent_column")
        assert df.empty

    def test_empty_mart_returns_empty(self):
        df = _distribution_table(pd.DataFrame(), "historical_usage_status")
        assert df.empty

    def test_order_respected(self):
        mart = _make_mart(n=4)
        order = ["declining_usage", "growing_usage"]
        df = _distribution_table(mart, "historical_usage_status", order=order)
        # First row should be declining_usage (first in order, if present)
        assert df.iloc[0]["Status"] == _status_label("declining_usage")


# ---------------------------------------------------------------------------
# validate_portfolio_mart_fields
# ---------------------------------------------------------------------------

class TestValidatePortfolioMartFields:
    def test_all_fields_present_in_full_mart(self):
        mart = _make_mart(n=3)
        presence = validate_portfolio_mart_fields(mart)
        # All fields defined in the contract should be True
        for field, present in presence.items():
            assert present, f"Expected field '{field}' to be present in mart"

    def test_missing_optional_field_detected(self):
        mart = _make_mart(n=3).drop(columns=["privacy_suppression_status"])
        presence = validate_portfolio_mart_fields(mart)
        assert presence["privacy_suppression_status"] is False

    def test_empty_mart_all_false(self):
        presence = validate_portfolio_mart_fields(pd.DataFrame())
        assert all(not v for v in presence.values())
