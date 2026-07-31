"""Tests for portfolio-level GenAI insight generation (Sprint 8 Step 8).

All tests use a small synthetic DataFrame fixture.
No live API calls are made in any test.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.genai.portfolio_insights import (
    PORTFOLIO_INSIGHT_SHORTLIST_MAX,
    REQUIRED_PORTFOLIO_FIELDS,
    _build_attention_shortlist,
    _compute_portfolio_hash,
    _extract_context_numbers,
    _load_existing_portfolio,
    _validate_portfolio_schema,
    build_portfolio_context,
    generate_portfolio_insight,
    generate_rule_based_portfolio_insight,
    save_portfolio_insight,
)
from src.genai.prompts import (
    PORTFOLIO_INSIGHT_PROMPT_VERSION,
    build_portfolio_insight_prompt,
)

# ── Synthetic fixture ──────────────────────────────────────────────────────────

def _mart(n: int = 10, **overrides) -> pd.DataFrame:
    """
    Build a synthetic mart DataFrame with n rows covering typical status distributions.
    Overrides replace the default column values for all rows.
    """
    analytics_run_id = overrides.pop("analytics_run_id", "run-001")
    analytics_as_of_date = overrides.pop("analytics_as_of_date", "2025-01-15")

    rows = []
    for i in range(n):
        row: dict = {
            "report_id":                  f"RPT{i+1:03d}",
            "report_name":                f"Report {i+1}",
            "analytics_run_id":           analytics_run_id,
            "analytics_as_of_date":       analytics_as_of_date,
            # Historical usage — distribute across statuses
            "historical_usage_status":    ["growing_usage", "growing_usage", "growing_usage",
                                           "stable_regular_usage", "stable_regular_usage",
                                           "declining_usage", "declining_usage", "declining_usage",
                                           "prolonged_inactivity", "bursty_usage"][i % 10],
            # Forecast
            "forecast_outlook_status":    ["growth_expected", "growth_expected", "growth_expected",
                                           "growth_expected", "stable_outlook",
                                           "decline_expected", "decline_expected",
                                           "uncertain_outlook", "reactivation_expected",
                                           "growth_expected"][i % 10],
            "forecast_uncertainty_status": ["high_uncertainty", "high_uncertainty",
                                            "moderate_uncertainty", "intervals_unavailable",
                                            "high_uncertainty", "very_high_uncertainty",
                                            "high_uncertainty", "high_uncertainty",
                                            "moderate_uncertainty", "intervals_unavailable"][i % 10],
            # Model health
            "model_diagnostic_status":    "insufficient_evidence",
            "primary_model_issue":        "insufficient_evidence",
            "recommended_model_action":   "insufficient_evidence",
            # Engagement
            "overall_engagement_status":  ["healthy_broad_adoption", "healthy_broad_adoption",
                                           "healthy_broad_adoption", "growing_adoption",
                                           "growing_adoption", "declining_adoption",
                                           "elevated_lapse", "inactive",
                                           "inactive", "healthy_broad_adoption"][i % 10],
            "active_user_direction_28d":  ["stable", "growing", "stable", "growing",
                                           "stable", "declining", "stable", "inactive",
                                           "inactive", "stable"][i % 10],
            "adoption_transition_status": ["strong_retention", "strong_retention",
                                           "strong_retention", "strong_retention",
                                           "mixed_transition", "elevated_lapse",
                                           "elevated_lapse", "unavailable",
                                           "unavailable", "strong_retention"][i % 10],
            "dependency_status":          "broadly_distributed_stable_dependency",
            # Evidence / metadata
            "overall_evidence_status":    "complete",
            "overall_report_status":      ["growing", "growing", "growing",
                                           "healthy", "healthy",
                                           "declining", "declining",
                                           "planned_deprecation", "planned_deprecation",
                                           "insufficient_evidence"][i % 10],
            "overall_review_priority":    ["medium", "medium", "medium",
                                           "low", "low",
                                           "high", "high",
                                           "medium", "medium",
                                           "insufficient_evidence"][i % 10],
            "recommended_report_action":  ["continue_monitoring", "continue_monitoring",
                                           "continue_monitoring", "continue_monitoring",
                                           "review_forecast_uncertainty",
                                           "investigate_usage_decline", "investigate_usage_decline",
                                           "review_planned_deprecation", "review_planned_deprecation",
                                           "insufficient_evidence"][i % 10],
            "primary_diagnostic":         "usage_analysis",
            "privacy_suppression_status": "not_suppressed" if i < 8 else "suppressed",
            "metadata_evidence_status":   "minimal",
            "current_zero_usage_streak_days": 0,
        }
        row.update(overrides)
        rows.append(row)
    return pd.DataFrame(rows)


def _ctx(n: int = 10, **overrides) -> dict:
    """Return a portfolio context built from the synthetic mart."""
    return build_portfolio_context(_mart(n, **overrides))


# ── 1. Aggregate calculation ──────────────────────────────────────────────────

class TestBuildPortfolioContext:
    def test_total_count_matches_mart(self):
        ctx = _ctx(10)
        assert ctx["total_report_count"] == 10

    def test_growing_count_correct(self):
        ctx = _ctx(10)
        # Rows 0,1,2 → growing_usage  (3 out of 10)
        assert ctx["historical_usage"]["growing"] == 3

    def test_declining_count_correct(self):
        ctx = _ctx(10)
        # Rows 5,6,7 → declining_usage  (3 out of 10)
        assert ctx["historical_usage"]["declining"] == 3

    def test_inactive_count_correct(self):
        ctx = _ctx(10)
        # Row 8 → prolonged_inactivity  (1 out of 10)
        assert ctx["historical_usage"]["inactive"] == 1

    def test_shares_sum_to_100_approx(self):
        ctx = _ctx(10)
        hist = ctx["historical_usage"]
        total_share = (
            hist["growing_share_pct"] + hist["stable_share_pct"]
            + hist["declining_share_pct"] + hist["inactive_share_pct"]
            + _pct(hist["other"], 10)
        )
        assert abs(total_share - 100.0) < 1.0

    def test_privacy_suppressed_count(self):
        ctx = _ctx(10)
        # Rows 8,9 are suppressed → 2
        assert ctx["portfolio_evidence"]["reports_with_privacy_suppression"] == 2

    def test_forecast_growth_expected_count(self):
        ctx = _ctx(10)
        # Rows 0,1,2,3,9 → growth_expected (5 out of 10)
        assert ctx["forecast_outlook"]["growth_expected"] == 5

    def test_forecast_decline_expected_count(self):
        ctx = _ctx(10)
        # Rows 5,6 → decline_expected (2 out of 10)
        assert ctx["forecast_outlook"]["decline_expected"] == 2

    def test_high_uncertainty_count(self):
        ctx = _ctx(10)
        # high_uncertainty: rows 0,1,4,6,7; very_high: row 5 → 6
        assert ctx["forecast_outlook"]["high_or_very_high_uncertainty"] == 6

    def test_model_health_counts_present(self):
        ctx = _ctx(10)
        # All rows have insufficient_evidence for model_diagnostic_status
        assert ctx["model_health"]["status_counts"].get("insufficient_evidence", 0) == 10

    def test_declining_active_user_breadth(self):
        ctx = _ctx(10)
        # Row 5 → active_user_direction_28d == 'declining'
        assert ctx["engagement"]["declining_active_user_breadth"] == 1

    def test_elevated_lapse_count(self):
        ctx = _ctx(10)
        # Rows 5,6 → elevated_lapse in adoption_transition_status
        assert ctx["engagement"]["elevated_lapse"] == 2

    def test_review_priority_counts_present(self):
        ctx = _ctx(10)
        priority = ctx["decision_support"]["review_priority_counts"]
        assert "high" in priority
        assert priority["high"] == 2  # rows 5,6

    def test_recommended_action_counts_present(self):
        ctx = _ctx(10)
        actions = ctx["decision_support"]["recommended_action_counts"]
        assert actions.get("continue_monitoring", 0) == 4

    def test_analytics_run_id_propagated(self):
        ctx = _ctx(10)
        assert ctx["analytics_run_id"] == "run-001"

    def test_analytics_as_of_date_propagated(self):
        ctx = _ctx(10)
        assert ctx["analytics_as_of_date"] == "2025-01-15"

    def test_empty_mart_raises(self):
        with pytest.raises(ValueError, match="empty"):
            build_portfolio_context(pd.DataFrame())

    def test_top_risks_populated_when_declining(self):
        ctx = _ctx(10)
        assert any("declining" in r for r in ctx["top_risks"])

    def test_top_positive_signals_populated_when_growing(self):
        ctx = _ctx(10)
        assert any("growing" in s for s in ctx["top_positive_signals"])

    def test_missing_metadata_count(self):
        ctx = _ctx(10)
        # All rows have metadata_evidence_status == 'minimal'
        assert ctx["portfolio_evidence"]["reports_with_missing_metadata"] == 10

    def test_long_zero_streak_counted(self):
        df = _mart(10)
        df.loc[0, "current_zero_usage_streak_days"] = 30
        df.loc[1, "current_zero_usage_streak_days"] = 20
        ctx = build_portfolio_context(df)
        assert ctx["historical_usage"]["long_zero_usage_streak_count"] == 2


def _pct(count: int, total: int) -> float:
    return round(count / total * 100, 1) if total > 0 else 0.0


# ── 2. Privacy-safe context ───────────────────────────────────────────────────

class TestPrivacySafeContext:
    def test_no_user_level_data_in_context(self):
        ctx = _ctx(10)
        prohibited = {"user_id", "user_key", "email", "email_address",
                      "display_name", "user_name", "username"}
        flat_keys = set(_flatten_keys(ctx))
        assert flat_keys.isdisjoint(prohibited)

    def test_no_raw_event_records(self):
        ctx = _ctx(10)
        # Context should not include per-report-per-day event data
        assert "events" not in ctx
        assert "raw_views" not in ctx


def _flatten_keys(d: dict, prefix: str = "") -> list[str]:
    keys = []
    for k, v in d.items():
        full = f"{prefix}.{k}" if prefix else k
        keys.append(full)
        if isinstance(v, dict):
            keys.extend(_flatten_keys(v, full))
    return keys


# ── 3. Deterministic ordering ─────────────────────────────────────────────────

class TestDeterministicOrdering:
    def test_status_counts_are_sorted_deterministically(self):
        ctx1 = _ctx(10)
        ctx2 = _ctx(10)
        assert ctx1["historical_usage"]["status_counts"] == ctx2["historical_usage"]["status_counts"]

    def test_action_counts_deterministic(self):
        ctx1 = _ctx(10)
        ctx2 = _ctx(10)
        assert (
            ctx1["decision_support"]["recommended_action_counts"]
            == ctx2["decision_support"]["recommended_action_counts"]
        )

    def test_top_risks_sorted(self):
        ctx = _ctx(10)
        risks = ctx["top_risks"]
        assert risks == sorted(risks)


# ── 4. Prompt ─────────────────────────────────────────────────────────────────

class TestPortfolioPrompt:
    def test_prompt_version_constant_present(self):
        assert PORTFOLIO_INSIGHT_PROMPT_VERSION == "portfolio_insight_v1"

    def test_prompt_contains_report_count(self):
        ctx = _ctx(10)
        prompt = build_portfolio_insight_prompt(ctx)
        assert "10" in prompt

    def test_prompt_contains_context_json(self):
        ctx = _ctx(10)
        prompt = build_portfolio_insight_prompt(ctx)
        assert "total_report_count" in prompt

    def test_shortlist_names_appear_in_prompt(self):
        ctx = _ctx(10)
        if ctx["attention_shortlist"]:
            shortlist_name = ctx["attention_shortlist"][0]["report_name"]
            prompt = build_portfolio_insight_prompt(ctx)
            assert shortlist_name in prompt

    def test_no_individual_user_data_in_prompt(self):
        ctx = _ctx(10)
        prompt = build_portfolio_insight_prompt(ctx)
        assert "user_id" not in prompt.lower()


# ── 5. Attention shortlist ────────────────────────────────────────────────────

class TestAttentionShortlist:
    def test_shortlist_max_five_reports(self):
        ctx = _ctx(10)
        assert len(ctx["attention_shortlist"]) <= 5

    def test_shortlist_only_attention_actions(self):
        ctx = _ctx(10)
        allowed = {
            "investigate_usage_decline", "review_planned_deprecation",
            "review_forecast_uncertainty", "investigate_user_decline",
            "investigate_user_lapse", "validate_report_audience",
            "complete_report_metadata", "review_inactivity",
        }
        for report in ctx["attention_shortlist"]:
            assert report["recommended_report_action"] in allowed

    def test_shortlist_fields_present(self):
        ctx = _ctx(10)
        required_keys = {
            "report_id", "report_name", "overall_review_priority",
            "overall_report_status", "primary_diagnostic",
            "recommended_report_action", "overall_evidence_status",
        }
        for report in ctx["attention_shortlist"]:
            assert required_keys.issubset(report.keys())

    def test_high_priority_appears_before_medium(self):
        ctx = _ctx(10)
        sl = ctx["attention_shortlist"]
        if len(sl) >= 2:
            priorities = [r["overall_review_priority"] for r in sl]
            priority_rank = {"high": 0, "medium": 1, "low": 2}
            for i in range(len(priorities) - 1):
                r1 = priority_rank.get(priorities[i], 99)
                r2 = priority_rank.get(priorities[i + 1], 99)
                assert r1 <= r2

    def test_no_continue_monitoring_in_shortlist(self):
        ctx = _ctx(10)
        for report in ctx["attention_shortlist"]:
            assert report["recommended_report_action"] != "continue_monitoring"

    def test_empty_mart_shortlist_is_empty(self):
        df = _mart(3, recommended_report_action="continue_monitoring")
        result = _build_attention_shortlist(df)
        assert result == []


# ── 6. Structured output validation ──────────────────────────────────────────

def _valid_portfolio_insight() -> dict:
    """A minimal valid portfolio insight matching the schema."""
    return {
        "executive_summary": "The portfolio contains 10 reports. 3 are growing and 3 are declining.",
        "portfolio_usage_summary": "Of 10 reports: 3 growing, 2 stable, 3 declining, 1 inactive.",
        "portfolio_engagement_summary": "Healthy broad adoption is the most common status. 1 report shows declining active-user breadth.",
        "portfolio_forecast_summary": "5 reports are expected to grow. 2 are expected to decline. 6 reports have high forecast uncertainty.",
        "portfolio_model_health_summary": "Model diagnostic evidence is insufficient for 10 of 10 reports.",
        "priority_actions": ["Investigate usage decline for 3 reports", "Review planned deprecation for 2 reports"],
        "positive_signals": ["3 reports show growing historical usage", "5 reports are expected to grow"],
        "evidence_limitations": ["Model diagnostic evidence is insufficient for 10 reports."],
    }


class TestValidatePortfolioSchema:
    def _ctx(self) -> dict:
        return _ctx(10)

    def test_valid_insight_produces_no_hard_errors(self):
        errors = _validate_portfolio_schema(_valid_portfolio_insight(), self._ctx())
        hard = [e for e in errors if not e.startswith("potential_identifier")]
        assert not hard

    def test_missing_required_field_flagged(self):
        insight = {k: v for k, v in _valid_portfolio_insight().items()
                   if k != "executive_summary"}
        errors = _validate_portfolio_schema(insight, self._ctx())
        assert any("missing_field:executive_summary" in e for e in errors)

    def test_empty_priority_actions_flagged(self):
        insight = {**_valid_portfolio_insight(), "priority_actions": []}
        errors = _validate_portfolio_schema(insight, self._ctx())
        assert any("empty_field:priority_actions" in e for e in errors)

    def test_retirement_recommendation_rejected(self):
        insight = {**_valid_portfolio_insight(),
                   "portfolio_usage_summary": "We should retire these reports."}
        errors = _validate_portfolio_schema(insight, self._ctx())
        assert any("prohibited_phrase" in e for e in errors)

    def test_deletion_recommendation_rejected(self):
        insight = {**_valid_portfolio_insight(),
                   "priority_actions": ["Delete the declining reports."]}
        errors = _validate_portfolio_schema(insight, self._ctx())
        assert any("prohibited_phrase" in e for e in errors)

    def test_retraining_recommendation_rejected(self):
        insight = {**_valid_portfolio_insight(),
                   "portfolio_model_health_summary": "We should retrain all models immediately."}
        errors = _validate_portfolio_schema(insight, self._ctx())
        assert any("prohibited_phrase" in e for e in errors)

    def test_unsupported_number_rejected(self):
        # 99 is not in the context aggregates for a 10-report portfolio
        ctx = self._ctx()
        insight = {**_valid_portfolio_insight(),
                   "executive_summary": "There are 99 reports with severe issues."}
        errors = _validate_portfolio_schema(insight, ctx)
        assert any("ungrounded" in e for e in errors)

    def test_grounded_count_accepted(self):
        # total_report_count=10 is in context; "10 reports" should not be flagged
        errors = _validate_portfolio_schema(_valid_portfolio_insight(), self._ctx())
        ungrounded = [e for e in errors if "ungrounded" in e]
        assert not any("10" in e for e in ungrounded)

    def test_invalid_action_category_rejected(self):
        insight = {**_valid_portfolio_insight(),
                   "priority_actions": ["Automatically remove underperforming reports"]}
        errors = _validate_portfolio_schema(insight, self._ctx())
        assert any("invalid_action_category" in e for e in errors)

    def test_valid_action_category_accepted(self):
        insight = {**_valid_portfolio_insight(),
                   "priority_actions": ["Investigate usage decline for 3 reports"]}
        errors = _validate_portfolio_schema(insight, self._ctx())
        action_errors = [e for e in errors if "invalid_action_category" in e]
        assert not action_errors

    def test_identifier_pattern_produces_warning_only(self):
        # "user id" triggers potential_identifier warning but not a prohibited_phrase error
        insight = {**_valid_portfolio_insight(),
                   "evidence_limitations": ["The user id field was excluded from analytics."]}
        errors = _validate_portfolio_schema(insight, self._ctx())
        hard = [e for e in errors if not e.startswith("potential_identifier")]
        # Only warning — not a hard error
        assert not any("prohibited_phrase" in e for e in hard)
        assert any("potential_identifier" in e for e in errors)

    def test_email_address_produces_identifier_warning(self):
        insight = {**_valid_portfolio_insight(),
                   "evidence_limitations": ["Contact owner@example.com for metadata."]}
        errors = _validate_portfolio_schema(insight, self._ctx())
        assert any("potential_identifier:email_addr" in e for e in errors)


# ── 7. Fallback schema ────────────────────────────────────────────────────────

class TestRuleBasedFallback:
    def test_fallback_has_all_required_fields(self):
        ctx = _ctx(10)
        result = generate_rule_based_portfolio_insight(ctx)
        for field in REQUIRED_PORTFOLIO_FIELDS:
            assert field in result

    def test_fallback_executive_summary_contains_count(self):
        ctx = _ctx(10)
        result = generate_rule_based_portfolio_insight(ctx)
        assert "10" in result["executive_summary"]

    def test_fallback_priority_actions_non_empty(self):
        ctx = _ctx(10)
        result = generate_rule_based_portfolio_insight(ctx)
        assert isinstance(result["priority_actions"], list)
        assert len(result["priority_actions"]) > 0

    def test_fallback_evidence_limitations_non_empty(self):
        ctx = _ctx(10)
        result = generate_rule_based_portfolio_insight(ctx)
        assert isinstance(result["evidence_limitations"], list)
        assert len(result["evidence_limitations"]) > 0

    def test_fallback_no_prohibited_phrases(self):
        ctx = _ctx(10)
        result = generate_rule_based_portfolio_insight(ctx)
        errors = _validate_portfolio_schema(result, ctx)
        hard = [e for e in errors
                if e.startswith("prohibited_phrase") or e.startswith("invalid_action")]
        assert not hard

    def test_fallback_mentions_model_health_limitation(self):
        ctx = _ctx(10)
        result = generate_rule_based_portfolio_insight(ctx)
        # Model health is insufficient for all 10 rows
        lim_text = " ".join(str(x) for x in result["evidence_limitations"]).lower()
        assert "model" in lim_text or "insufficient" in lim_text


# ── 8. Hash reuse ─────────────────────────────────────────────────────────────

class TestHashReuse:
    def _make_lineage(self, ctx: dict, status: str = "success") -> dict:
        h = _compute_portfolio_hash(ctx, PORTFOLIO_INSIGHT_PROMPT_VERSION, "gpt-4.1-mini")
        return {
            **_valid_portfolio_insight(),
            "input_hash": h,
            "generation_status": status,
            "prompt_version": PORTFOLIO_INSIGHT_PROMPT_VERSION,
            "model_name": "gpt-4.1-mini",
        }

    def test_unchanged_context_is_reused(self):
        ctx = _ctx(10)
        existing = self._make_lineage(ctx, "success")
        result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key=None,
                                            existing_insight=existing)
        assert result["generation_status"] == "reused"

    def test_changed_aggregate_triggers_regeneration(self):
        ctx1 = _ctx(10)
        ctx2 = _ctx(12)   # different total_report_count
        existing = self._make_lineage(ctx1, "success")
        result = generate_portfolio_insight(ctx2, model="gpt-4.1-mini", api_key=None,
                                            existing_insight=existing)
        assert result["generation_status"] != "reused"

    def test_failed_prior_output_not_reused(self):
        ctx = _ctx(10)
        existing = self._make_lineage(ctx, "fallback_api_error")
        result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key=None,
                                            existing_insight=existing)
        assert result["generation_status"] != "reused"

    def test_different_model_triggers_regeneration(self):
        ctx = _ctx(10)
        # Hash computed with model A
        existing = {
            **_valid_portfolio_insight(),
            "input_hash": _compute_portfolio_hash(ctx, PORTFOLIO_INSIGHT_PROMPT_VERSION, "model-A"),
            "generation_status": "success",
        }
        # Generate with model B
        result = generate_portfolio_insight(ctx, model="model-B", api_key=None,
                                            existing_insight=existing)
        assert result["generation_status"] != "reused"

    def test_different_prompt_version_triggers_regeneration(self):
        ctx = _ctx(10)
        existing = {
            **_valid_portfolio_insight(),
            "input_hash": _compute_portfolio_hash(ctx, "portfolio_insight_v0", "gpt-4.1-mini"),
            "generation_status": "success",
        }
        result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key=None,
                                            existing_insight=existing,
                                            prompt_version=PORTFOLIO_INSIGHT_PROMPT_VERSION)
        assert result["generation_status"] != "reused"


# ── 9. No API key → rule-based ───────────────────────────────────────────────

class TestNoApiKey:
    def test_no_key_produces_rule_based_status(self):
        ctx = _ctx(10)
        result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key=None)
        assert result["generation_status"] == "rule_based"

    def test_no_key_output_has_all_required_fields(self):
        ctx = _ctx(10)
        result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key=None)
        for field in REQUIRED_PORTFOLIO_FIELDS:
            assert field in result

    def test_no_key_lineage_fields_present(self):
        ctx = _ctx(10)
        result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key=None)
        for field in ("input_hash", "prompt_version", "model_name", "generated_at",
                      "report_count", "generation_status", "validation_status"):
            assert field in result


# ── 10. LLM API path (mocked) ────────────────────────────────────────────────

class TestMockedApiCall:
    def _mock_response(self, insight: dict) -> MagicMock:
        return MagicMock(
            status_code=200,
            raise_for_status=MagicMock(),
            json=MagicMock(return_value={
                "output": [{"content": [{"text": json.dumps(insight)}]}]
            }),
        )

    def test_valid_response_produces_success_status(self):
        ctx = _ctx(10)
        good_insight = _valid_portfolio_insight()
        with patch("requests.post", return_value=self._mock_response(good_insight)):
            result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key="fake-key")
        assert result["generation_status"] == "success"
        assert result["validation_status"] in ("valid", "warnings")

    def test_invalid_response_triggers_fallback(self):
        ctx = _ctx(10)
        bad_insight = {"executive_summary": "We should retire these reports."}
        with patch("requests.post", return_value=self._mock_response(bad_insight)):
            result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key="fake-key")
        assert "fallback" in result["generation_status"]

    def test_api_error_triggers_fallback(self):
        ctx = _ctx(10)
        with patch("requests.post", side_effect=Exception("connection refused")):
            result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key="fake-key")
        assert "fallback" in result["generation_status"]

    def test_unsupported_number_in_response_triggers_fallback(self):
        ctx = _ctx(10)
        bad_insight = {
            **_valid_portfolio_insight(),
            "executive_summary": "There are 999 reports with critical issues.",
        }
        with patch("requests.post", return_value=self._mock_response(bad_insight)):
            result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key="fake-key")
        assert "fallback" in result["generation_status"]

    def test_ungrounded_action_category_triggers_fallback(self):
        ctx = _ctx(10)
        bad_insight = {
            **_valid_portfolio_insight(),
            "priority_actions": ["Automatically remove underperforming reports"],
        }
        with patch("requests.post", return_value=self._mock_response(bad_insight)):
            result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key="fake-key")
        assert "fallback" in result["generation_status"]


# ── 11. Lineage fields ────────────────────────────────────────────────────────

class TestLineage:
    def test_lineage_fields_present_in_rule_based_output(self):
        ctx = _ctx(10)
        result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key=None)
        assert result["report_count"] == 10
        assert result["prompt_version"] == PORTFOLIO_INSIGHT_PROMPT_VERSION
        assert result["model_name"] == "gpt-4.1-mini"
        assert result["input_hash"] is not None
        assert result["generated_at"] is not None

    def test_analytics_run_id_in_lineage(self):
        ctx = _ctx(10)
        result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key=None)
        assert result["analytics_run_id"] == "run-001"

    def test_analytics_as_of_date_in_lineage(self):
        ctx = _ctx(10)
        result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key=None)
        assert result["analytics_as_of_date"] == "2025-01-15"


# ── 12. Save output ───────────────────────────────────────────────────────────

class TestSavePortfolioInsight:
    def test_json_written(self, tmp_path):
        ctx = _ctx(10)
        result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key=None)
        # create expected directory structure
        (tmp_path / "outputs" / "insights").mkdir(parents=True)
        paths = save_portfolio_insight(result, project_root=tmp_path)
        assert paths["json"].exists()
        loaded = json.loads(paths["json"].read_text())
        assert loaded["report_count"] == 10

    def test_markdown_written(self, tmp_path):
        ctx = _ctx(10)
        result = generate_portfolio_insight(ctx, model="gpt-4.1-mini", api_key=None)
        (tmp_path / "outputs" / "insights").mkdir(parents=True)
        paths = save_portfolio_insight(result, project_root=tmp_path)
        assert paths["markdown"].exists()
        md = paths["markdown"].read_text()
        assert "Portfolio AI Insight" in md

    def test_load_existing_returns_none_when_missing(self, tmp_path):
        result = _load_existing_portfolio(tmp_path)
        assert result is None

    def test_load_existing_ignores_failed_outputs(self, tmp_path):
        (tmp_path / "outputs" / "insights").mkdir(parents=True)
        bad = {"generation_status": "fallback_api_error", "input_hash": "abc"}
        (tmp_path / "outputs" / "insights" / "portfolio_ai_insight.json").write_text(
            json.dumps(bad)
        )
        result = _load_existing_portfolio(tmp_path)
        assert result is None


# ── 13. Context number extraction helper ─────────────────────────────────────

class TestExtractContextNumbers:
    def test_extracts_integer_counts(self):
        ctx = {"total_report_count": 30}
        nums = _extract_context_numbers(ctx)
        assert 30.0 in nums

    def test_extracts_nested_values(self):
        ctx = {"historical_usage": {"growing": 11, "declining_share_pct": 36.7}}
        nums = _extract_context_numbers(ctx)
        assert 11.0 in nums
        assert 36.7 in nums

    def test_ignores_string_values(self):
        ctx = {"analytics_run_id": "run-001"}
        nums = _extract_context_numbers(ctx)
        assert not nums
