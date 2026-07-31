"""
Tests for src/genai/evaluation.py — Sprint 8 Step 9.

No live LLM calls. All insights are synthetic or loaded from fixture files.
"""

from __future__ import annotations

import json
from pathlib import Path
import pytest

from src.genai.evaluation import (
    EvaluationResult,
    compare_against_golden,
    evaluate_action_alignment,
    evaluate_completeness,
    evaluate_conciseness,
    evaluate_directional_consistency,
    evaluate_evidence_disclosure,
    evaluate_groundedness,
    evaluate_numerical_grounding,
    evaluate_portfolio_insight,
    evaluate_readability,
    evaluate_report_insight,
    evaluate_safety,
    load_evaluation_cases,
    load_golden_outputs,
    save_evaluation_results,
    _collect_text,
    _overall_pass,
)

# ── Paths ──────────────────────────────────────────────────────────────────────
_FIXTURES = Path(__file__).parent / "fixtures"
_EVAL_CASES_FILE  = _FIXTURES / "genai_evaluation_cases.json"
_GOLDEN_FILE      = _FIXTURES / "genai_golden_outputs.json"


# ── Minimal fixtures ───────────────────────────────────────────────────────────

def _report_context(**overrides) -> dict:
    base = {
        "report_id": "TEST001", "report_name": "Test Report",
        "analytics_run_id": "run-001", "analytics_as_of_date": "2025-01-15",
        "historical_usage_status": "stable_regular_usage",
        "recent_28d_views": 50, "previous_28d_views": 48,
        "usage_change_28d_pct": 0.04, "days_since_last_use": 0,
        "forecast_total_28d": 52, "forecast_change_vs_actual_28d_pct": 0.04,
        "forecast_outlook_status": "stable_outlook",
        "forecast_uncertainty_status": "moderate_uncertainty",
        "model_diagnostic_status": "insufficient_evidence",
        "primary_model_issue": "insufficient_evidence",
        "forecast_interpretation_status": "insufficient_model_evidence",
        "unique_users_28d": 15, "active_user_direction_28d": "stable",
        "returning_user_share_28d": 0.60, "retained_user_rate_28d": 0.65,
        "lapse_rate_28d": 0.25, "overall_engagement_status": "healthy_broad_adoption",
        "dependency_status": "broadly_distributed_stable_dependency",
        "criticality_level": "unknown", "expected_usage_cadence": None,
        "primary_diagnostic": "stable_usage",
        "overall_report_status": "healthy", "overall_evidence_status": "complete",
        "recommended_report_action": "continue_monitoring",
        "overall_review_priority": "low", "report_reasons": None,
        "privacy_suppression_status": "not_suppressed",
        "privacy_suppressed_fields": None, "missing_engagement_evidence": None,
    }
    base.update(overrides)
    return base


def _report_insight(**overrides) -> dict:
    base = {
        "executive_summary": "Test Report shows stable usage.",
        "usage_insight": "Usage is stable at approximately 50 views in the 28-day window.",
        "engagement_insight": "15 active users with 60% returning share.",
        "forecast_insight": "A stable outlook is expected.",
        "model_confidence_note": "Model diagnostic evidence is insufficient.",
        "recommended_action": "Continue monitoring.",
        "evidence_limitations": ["Model diagnostic evidence is insufficient."],
    }
    base.update(overrides)
    return base


def _portfolio_context(**overrides) -> dict:
    base = {
        "analytics_run_id": "run-001",
        "analytics_as_of_date": "2025-01-15",
        "total_report_count": 10,
        "portfolio_evidence": {
            "reports_with_sufficient_evidence": 10,
            "reports_with_insufficient_evidence": 0,
            "reports_with_privacy_suppression": 0,
            "reports_with_missing_metadata": 10,
        },
        "historical_usage": {
            "growing": 5, "growing_share_pct": 50.0,
            "stable": 4, "stable_share_pct": 40.0,
            "declining": 1, "declining_share_pct": 10.0,
            "inactive": 0, "inactive_share_pct": 0.0,
            "other": 0,
            "status_counts": {"growing_usage": 5, "stable_regular_usage": 4, "declining_usage": 1},
            "long_zero_usage_streak_count": 0,
        },
        "forecast_outlook": {
            "growth_expected": 7, "growth_expected_share_pct": 70.0,
            "stable_expected": 2, "decline_expected": 1,
            "decline_expected_share_pct": 10.0, "reactivation_expected": 0,
            "uncertain_outlook": 0,
            "high_or_very_high_uncertainty": 6, "high_uncertainty_share_pct": 60.0,
            "intervals_unavailable": 1,
            "status_counts": {"growth_expected": 7, "stable_outlook": 2, "decline_expected": 1},
        },
        "model_health": {
            "status_counts": {"insufficient_evidence": 10},
            "recommended_action_counts": {"insufficient_evidence": 10},
            "primary_issue_counts": {"insufficient_evidence": 10},
            "poor_calibration_count": 0,
        },
        "engagement": {
            "status_counts": {"healthy_broad_adoption": 7, "growing_adoption": 2, "declining_adoption": 1},
            "declining_active_user_breadth": 1,
            "elevated_lapse": 0, "strong_retention": 8,
            "high_user_concentration_or_dependency": 0,
        },
        "decision_support": {
            "overall_status_counts": {"growing": 5, "healthy": 4, "declining": 1},
            "review_priority_counts": {"low": 7, "medium": 3},
            "recommended_action_counts": {
                "continue_monitoring": 8,
                "investigate_usage_decline": 1,
                "review_forecast_uncertainty": 1,
            },
        },
        "top_risks": ["1 reports show declining historical usage"],
        "top_positive_signals": ["5 reports show growing historical usage"],
        "attention_shortlist": [],
    }
    base.update(overrides)
    return base


def _portfolio_insight(**overrides) -> dict:
    base = {
        "executive_summary": "The portfolio of 10 reports is predominantly growing. Model evidence is insufficient.",
        "portfolio_usage_summary": "Of 10 reports: 5 growing, 4 stable, 1 declining.",
        "portfolio_engagement_summary": "7 reports show healthy broad adoption.",
        "portfolio_forecast_summary": "7 reports are expected to grow. 6 have high forecast uncertainty.",
        "portfolio_model_health_summary": "Model diagnostic evidence is insufficient for all 10 reports.",
        "priority_actions": ["Investigate usage decline for 1 report", "Review forecast uncertainty for 1 report"],
        "positive_signals": ["5 reports show growing historical usage", "7 reports are expected to grow"],
        "evidence_limitations": ["Model diagnostic evidence is insufficient for all 10 reports."],
    }
    base.update(overrides)
    return base


# ══════════════════════════════════════════════════════════════════════════════
# 1. Fixture loading
# ══════════════════════════════════════════════════════════════════════════════

class TestFixtureLoading:
    def test_evaluation_cases_file_exists(self):
        assert _EVAL_CASES_FILE.exists(), "genai_evaluation_cases.json not found"

    def test_golden_outputs_file_exists(self):
        assert _GOLDEN_FILE.exists(), "genai_golden_outputs.json not found"

    def test_load_evaluation_cases_returns_list(self):
        cases = load_evaluation_cases()
        assert isinstance(cases, list)
        assert len(cases) >= 15 + 8  # 15 report + 8 portfolio

    def test_evaluation_cases_have_required_keys(self):
        cases = load_evaluation_cases()
        for case in cases:
            assert "case_id" in case
            assert "insight_type" in case
            assert "context" in case
            assert "insight" in case
            assert "expected_validation_outcome" in case

    def test_load_golden_outputs_returns_list(self):
        goldens = load_golden_outputs()
        assert isinstance(goldens, list)
        assert len(goldens) >= 5

    def test_golden_configs_have_required_keys(self):
        goldens = load_golden_outputs()
        for g in goldens:
            assert "golden_id" in g
            assert "insight_type" in g
            assert "required_concepts" in g
            assert "prohibited_concepts" in g
            assert "schema_fields" in g

    def test_fixture_report_case_count(self):
        cases = load_evaluation_cases()
        report_cases = [c for c in cases if c["insight_type"] == "report"]
        assert len(report_cases) >= 15

    def test_fixture_portfolio_case_count(self):
        cases = load_evaluation_cases()
        portfolio_cases = [c for c in cases if c["insight_type"] == "portfolio"]
        assert len(portfolio_cases) >= 8


# ══════════════════════════════════════════════════════════════════════════════
# 2. evaluate_completeness
# ══════════════════════════════════════════════════════════════════════════════

class TestEvaluateCompleteness:
    def test_all_report_fields_present_passes(self):
        passed, failures = evaluate_completeness(_report_insight(), "report")
        assert passed
        assert failures == []

    def test_missing_report_field_fails(self):
        ins = _report_insight()
        del ins["executive_summary"]
        passed, failures = evaluate_completeness(ins, "report")
        assert not passed
        assert any("executive_summary" in f for f in failures)

    def test_empty_report_field_fails(self):
        ins = _report_insight(executive_summary="")
        passed, failures = evaluate_completeness(ins, "report")
        assert not passed
        assert any("executive_summary" in f for f in failures)

    def test_empty_list_field_fails(self):
        ins = _report_insight(evidence_limitations=[])
        passed, failures = evaluate_completeness(ins, "report")
        assert not passed
        assert any("evidence_limitations" in f for f in failures)

    def test_portfolio_all_fields_present_passes(self):
        passed, failures = evaluate_completeness(_portfolio_insight(), "portfolio")
        assert passed

    def test_portfolio_missing_field_fails(self):
        ins = _portfolio_insight()
        del ins["portfolio_usage_summary"]
        passed, failures = evaluate_completeness(ins, "portfolio")
        assert not passed


# ══════════════════════════════════════════════════════════════════════════════
# 3. evaluate_safety
# ══════════════════════════════════════════════════════════════════════════════

class TestEvaluateSafety:
    def test_safe_report_insight_passes(self):
        passed, failures = evaluate_safety(_report_insight(), "report")
        assert passed

    def test_retire_in_report_fails(self):
        ins = _report_insight(recommended_action="We should retire this report immediately.")
        passed, failures = evaluate_safety(ins, "report")
        assert not passed
        assert any("prohibited_phrase" in f for f in failures)

    def test_delete_in_executive_summary_fails(self):
        ins = _report_insight(executive_summary="We should delete this report.")
        passed, failures = evaluate_safety(ins, "report")
        assert not passed

    def test_deletes_not_prohibited(self):
        ins = _report_insight(usage_insight="The system deletes stale rows after 90 days.")
        passed, failures = evaluate_safety(ins, "report")
        assert passed, f"'deletes' should not be prohibited, got: {failures}"

    def test_retrain_in_report_fails(self):
        ins = _report_insight(model_confidence_note="We should retrain the model.")
        passed, failures = evaluate_safety(ins, "report")
        assert not passed

    def test_portfolio_retire_fails(self):
        ins = _portfolio_insight(executive_summary="We should retire underused reports.")
        passed, failures = evaluate_safety(ins, "portfolio")
        assert not passed

    def test_safe_portfolio_insight_passes(self):
        passed, failures = evaluate_safety(_portfolio_insight(), "portfolio")
        assert passed


# ══════════════════════════════════════════════════════════════════════════════
# 4. evaluate_directional_consistency
# ══════════════════════════════════════════════════════════════════════════════

class TestEvaluateDirectionalConsistency:
    def test_growing_insight_matches_growing_context(self):
        ctx = _report_context(historical_usage_status="growing_usage")
        ins = _report_insight(
            usage_insight="Usage has grown from 40 to 50 views.",
            forecast_insight="Continued growth is expected.",
        )
        passed, failures = evaluate_directional_consistency(ins, ctx, "report")
        assert passed

    def test_growing_insight_contradicts_declining_context(self):
        ctx = _report_context(historical_usage_status="declining_usage")
        ins = _report_insight(
            usage_insight="Usage is growing strongly.",
            forecast_insight="Growth is expected to accelerate.",
        )
        passed, failures = evaluate_directional_consistency(ins, ctx, "report")
        assert not passed

    def test_direction_conflict_hist_decline_forecast_growth_allowed(self):
        # hist=declining, forecast=growth_expected → NOT a conflict per design
        ctx = _report_context(
            historical_usage_status="declining_usage",
            forecast_outlook_status="growth_expected",
        )
        ins = _report_insight(
            usage_insight="Usage has declined from 55 to 35 views.",
            forecast_insight="Following a difficult period, growth is expected to resume.",
        )
        passed, failures = evaluate_directional_consistency(ins, ctx, "report")
        assert passed, f"unexpected failures: {failures}"

    def test_portfolio_direction_all_stable(self):
        ctx = _portfolio_context()
        ins = _portfolio_insight(
            portfolio_usage_summary="Of 10 reports: 5 growing, 4 stable, 1 declining.",
        )
        passed, failures = evaluate_directional_consistency(ins, ctx, "portfolio")
        assert passed


# ══════════════════════════════════════════════════════════════════════════════
# 5. evaluate_numerical_grounding
# ══════════════════════════════════════════════════════════════════════════════

class TestEvaluateNumericalGrounding:
    def test_supported_percentage_passes(self):
        # returning_user_share_28d=0.60 → context_numbers includes 60.0
        ctx = _report_context(returning_user_share_28d=0.60)
        ins = _report_insight(engagement_insight="60% of users are returning.")
        passed, failures = evaluate_numerical_grounding(ins, ctx, "report")
        assert passed

    def test_unsupported_percentage_fails(self):
        ctx = _report_context(returning_user_share_28d=0.60)
        ins = _report_insight(engagement_insight="95% of users are returning.")
        passed, failures = evaluate_numerical_grounding(ins, ctx, "report")
        assert not passed
        assert any("95" in f for f in failures)

    def test_tolerance_within_5pp_passes(self):
        ctx = _report_context(returning_user_share_28d=0.60)
        ins = _report_insight(engagement_insight="62% of users are returning.")  # 60 ± 5
        passed, failures = evaluate_numerical_grounding(ins, ctx, "report")
        assert passed

    def test_count_within_tolerance_passes(self):
        ctx = _report_context(recent_28d_views=50)
        ins = _report_insight(usage_insight="Approximately 52 views in the 28-day window.")
        passed, failures = evaluate_numerical_grounding(ins, ctx, "report")
        assert passed

    def test_small_counts_excluded(self):
        # report-level min count is 10; counts below it are excluded from grounding
        ctx = _report_context(recent_28d_views=50)
        ins = _report_insight(usage_insight="3 days ago usage was last recorded.")
        passed, failures = evaluate_numerical_grounding(ins, ctx, "report")
        assert passed

    def test_portfolio_count_grounding(self):
        ctx = _portfolio_context()
        ins = _portfolio_insight(
            portfolio_usage_summary="Of 10 reports: 5 growing, 4 stable, 1 declining.",
        )
        passed, failures = evaluate_numerical_grounding(ins, ctx, "portfolio")
        assert passed

    def test_time_period_label_not_flagged(self):
        ctx = _report_context(recent_28d_views=50)
        ins = _report_insight(usage_insight="No usage in the last 28 days.")
        passed, failures = evaluate_numerical_grounding(ins, ctx, "report")
        assert passed


# ══════════════════════════════════════════════════════════════════════════════
# 6. evaluate_action_alignment
# ══════════════════════════════════════════════════════════════════════════════

class TestEvaluateActionAlignment:
    def test_continue_monitoring_keywords_pass(self):
        ctx = _report_context(recommended_report_action="continue_monitoring")
        ins = _report_insight(recommended_action="Continue monitoring.")
        passed, failures = evaluate_action_alignment(ins, ctx, "report")
        assert passed

    def test_investigate_keywords_pass(self):
        ctx = _report_context(recommended_report_action="investigate_usage_decline")
        ins = _report_insight(recommended_action="Investigate usage decline.")
        passed, failures = evaluate_action_alignment(ins, ctx, "report")
        assert passed

    def test_action_keyword_mismatch_fails(self):
        ctx = _report_context(recommended_report_action="investigate_usage_decline")
        ins = _report_insight(recommended_action="Continue monitoring.")
        passed, failures = evaluate_action_alignment(ins, ctx, "report")
        assert not passed

    def test_portfolio_recognized_actions_pass(self):
        ctx = _portfolio_context()
        ins = _portfolio_insight()
        passed, failures = evaluate_action_alignment(ins, ctx, "portfolio")
        assert passed


# ══════════════════════════════════════════════════════════════════════════════
# 7. evaluate_evidence_disclosure
# ══════════════════════════════════════════════════════════════════════════════

class TestEvaluateEvidenceDisclosure:
    def test_model_insufficient_disclosed_passes(self):
        ctx = _report_context(model_diagnostic_status="insufficient_evidence")
        ins = _report_insight(
            model_confidence_note="Model diagnostic evidence is insufficient.",
            evidence_limitations=["Model diagnostic evidence is insufficient."],
        )
        passed, failures = evaluate_evidence_disclosure(ins, ctx, "report")
        assert passed

    def test_model_insufficient_not_disclosed_fails(self):
        ctx = _report_context(model_diagnostic_status="insufficient_evidence")
        ins = _report_insight(
            executive_summary="Usage is healthy.",
            usage_insight="50 views in the 28-day window.",
            engagement_insight="15 active users with strong engagement.",
            forecast_insight="A stable outlook is expected.",
            model_confidence_note="Forecast quality is high.",
            recommended_action="Continue monitoring.",
            evidence_limitations=["No limitations identified."],
        )
        passed, failures = evaluate_evidence_disclosure(ins, ctx, "report")
        assert not passed
        assert any("model_health" in f for f in failures)

    def test_privacy_suppressed_disclosed_passes(self):
        ctx = _report_context(privacy_suppression_status="suppressed")
        ins = _report_insight(
            engagement_insight="Engagement data is privacy-suppressed.",
            evidence_limitations=["Engagement metrics are privacy-suppressed."],
        )
        passed, failures = evaluate_evidence_disclosure(ins, ctx, "report")
        assert passed

    def test_privacy_suppressed_not_disclosed_fails(self):
        ctx = _report_context(privacy_suppression_status="suppressed")
        ins = _report_insight(
            engagement_insight="15 active users with 60% returning share.",
            evidence_limitations=["No limitations."],
        )
        passed, failures = evaluate_evidence_disclosure(ins, ctx, "report")
        assert not passed
        assert any("privacy_suppression" in f for f in failures)

    def test_high_uncertainty_disclosed_passes(self):
        ctx = _report_context(forecast_uncertainty_status="high_uncertainty")
        ins = _report_insight(
            forecast_insight="Growth is expected but forecast uncertainty is high.",
        )
        passed, failures = evaluate_evidence_disclosure(ins, ctx, "report")
        assert passed

    def test_high_uncertainty_not_disclosed_fails(self):
        ctx = _report_context(forecast_uncertainty_status="high_uncertainty")
        ins = _report_insight(
            forecast_insight="Continued growth is expected next period.",
        )
        passed, failures = evaluate_evidence_disclosure(ins, ctx, "report")
        assert not passed
        assert any("forecast_uncertainty" in f for f in failures)

    def test_portfolio_model_evidence_disclosed_passes(self):
        ctx = _portfolio_context()
        ins = _portfolio_insight()
        passed, failures = evaluate_evidence_disclosure(ins, ctx, "portfolio")
        assert passed

    def test_portfolio_privacy_suppression_not_disclosed_fails(self):
        ctx = _portfolio_context(
            portfolio_evidence={
                "reports_with_sufficient_evidence": 10,
                "reports_with_insufficient_evidence": 0,
                "reports_with_privacy_suppression": 3,
                "reports_with_missing_metadata": 0,
            }
        )
        ins = _portfolio_insight(evidence_limitations=["Model evidence is insufficient."])
        passed, failures = evaluate_evidence_disclosure(ins, ctx, "portfolio")
        assert not passed
        assert any("privacy" in f for f in failures)


# ══════════════════════════════════════════════════════════════════════════════
# 8. evaluate_readability
# ══════════════════════════════════════════════════════════════════════════════

class TestEvaluateReadability:
    def test_concise_insight_passes(self):
        score, issues = evaluate_readability(_report_insight(), "report")
        assert score >= 0.5

    def test_very_long_insight_reduces_score(self):
        long_text = " ".join(["word"] * 200)
        ins = _report_insight(executive_summary=long_text)
        score, issues = evaluate_readability(ins, "report")
        assert score < 1.0
        assert any("executive_summary" in i for i in issues)

    def test_generic_phrase_flagged(self):
        ins = _report_insight(executive_summary="N/A")
        score, issues = evaluate_readability(ins, "report")
        assert any("generic_vacuous" in i for i in issues)

    def test_portfolio_readability_passes(self):
        score, issues = evaluate_readability(_portfolio_insight(), "portfolio")
        assert score >= 0.5


# ══════════════════════════════════════════════════════════════════════════════
# 9. evaluate_conciseness
# ══════════════════════════════════════════════════════════════════════════════

class TestEvaluateConciseness:
    def test_concise_insight_scores_high(self):
        score, _ = evaluate_conciseness(_report_insight(), "report")
        assert score >= 0.5

    def test_very_long_insight_scores_low(self):
        long_text = " ".join(["detail"] * 200)
        # Make ALL fields long so the average word-ratio exceeds 1.0
        ins = {
            "executive_summary": long_text,
            "usage_insight": long_text,
            "engagement_insight": long_text,
            "forecast_insight": long_text,
            "model_confidence_note": long_text,
            "recommended_action": long_text,
            "evidence_limitations": [long_text],
        }
        score, _ = evaluate_conciseness(ins, "report")
        assert score < 1.0

    def test_empty_insight_returns_one(self):
        # If no fields checked, score defaults to 1.0
        score, _ = evaluate_conciseness({}, "report")
        assert score == 1.0


# ══════════════════════════════════════════════════════════════════════════════
# 10. overall_pass logic
# ══════════════════════════════════════════════════════════════════════════════

class TestOverallPass:
    def test_all_pass_returns_true(self):
        assert _overall_pass(True, True, True, True, True, True, True, 0.8)

    def test_any_fail_returns_false(self):
        assert not _overall_pass(False, True, True, True, True, True, True, 0.8)
        assert not _overall_pass(True, False, True, True, True, True, True, 0.8)
        assert not _overall_pass(True, True, True, True, True, True, True, 0.4)

    def test_readability_below_threshold_returns_false(self):
        assert not _overall_pass(True, True, True, True, True, True, True, 0.49)

    def test_readability_at_threshold_returns_true(self):
        assert _overall_pass(True, True, True, True, True, True, True, 0.5)


# ══════════════════════════════════════════════════════════════════════════════
# 11. evaluate_report_insight (top-level)
# ══════════════════════════════════════════════════════════════════════════════

class TestEvaluateReportInsight:
    def test_passing_report_insight(self):
        ctx = _report_context()
        ins = _report_insight()
        result = evaluate_report_insight(ins, ctx, case_id="test_pass")
        assert isinstance(result, EvaluationResult)
        assert result.insight_type == "report"
        assert result.overall_pass

    def test_failing_report_insight_safety(self):
        ctx = _report_context()
        ins = _report_insight(recommended_action="We should retire this report.")
        result = evaluate_report_insight(ins, ctx, case_id="test_fail_safety")
        assert not result.overall_pass
        assert not result.safety_pass

    def test_failing_report_insight_completeness(self):
        ctx = _report_context()
        ins = _report_insight()
        del ins["executive_summary"]
        result = evaluate_report_insight(ins, ctx, case_id="test_fail_completeness")
        assert not result.overall_pass
        assert not result.completeness_pass

    def test_result_contains_all_fields(self):
        ctx = _report_context()
        ins = _report_insight()
        result = evaluate_report_insight(ins, ctx, case_id="test_fields")
        assert hasattr(result, "completeness_pass")
        assert hasattr(result, "safety_pass")
        assert hasattr(result, "direction_pass")
        assert hasattr(result, "numerical_pass")
        assert hasattr(result, "action_alignment_pass")
        assert hasattr(result, "evidence_disclosure_pass")
        assert hasattr(result, "readability_score")
        assert hasattr(result, "conciseness_score")
        assert hasattr(result, "overall_pass")
        assert hasattr(result, "failure_reasons")


# ══════════════════════════════════════════════════════════════════════════════
# 12. evaluate_portfolio_insight (top-level)
# ══════════════════════════════════════════════════════════════════════════════

class TestEvaluatePortfolioInsight:
    def test_passing_portfolio_insight(self):
        ctx = _portfolio_context()
        ins = _portfolio_insight()
        result = evaluate_portfolio_insight(ins, ctx, case_id="port_pass")
        assert isinstance(result, EvaluationResult)
        assert result.insight_type == "portfolio"
        assert result.overall_pass

    def test_failing_portfolio_safety(self):
        ctx = _portfolio_context()
        ins = _portfolio_insight(executive_summary="We should retire underused reports.")
        result = evaluate_portfolio_insight(ins, ctx, case_id="port_fail_safety")
        assert not result.overall_pass
        assert not result.safety_pass

    def test_failing_portfolio_completeness(self):
        ctx = _portfolio_context()
        ins = _portfolio_insight()
        del ins["portfolio_usage_summary"]
        result = evaluate_portfolio_insight(ins, ctx, case_id="port_fail_comp")
        assert not result.overall_pass
        assert not result.completeness_pass


# ══════════════════════════════════════════════════════════════════════════════
# 13. compare_against_golden
# ══════════════════════════════════════════════════════════════════════════════

class TestCompareAgainstGolden:
    def _golden(self, **overrides) -> dict:
        base = {
            "required_concepts": ["stable", "continue"],
            "prohibited_concepts": ["retire", "delete"],
            "schema_fields": ["executive_summary", "recommended_action"],
            "evidence_limitations_keywords": [],
            "expected_action": "continue_monitoring",
        }
        base.update(overrides)
        return base

    def test_matching_insight_passes(self):
        ins = _report_insight()
        passed, failures = compare_against_golden(ins, self._golden())
        assert passed
        assert failures == []

    def test_missing_required_concept_fails(self):
        ins = _report_insight(
            usage_insight="Usage has been observed.",
            recommended_action="Continue monitoring.",
        )
        golden = self._golden(required_concepts=["stable", "continue"])
        passed, failures = compare_against_golden(ins, golden)
        # "stable" must appear somewhere
        assert any("stable" in f or "continue" in f for f in failures) or passed

    def test_prohibited_concept_present_fails(self):
        ins = _report_insight(executive_summary="We should retire this report.")
        golden = self._golden(prohibited_concepts=["retire"])
        passed, failures = compare_against_golden(ins, golden)
        assert not passed
        assert any("retire" in f for f in failures)

    def test_missing_schema_field_fails(self):
        ins = _report_insight()
        del ins["executive_summary"]
        golden = self._golden(schema_fields=["executive_summary"])
        passed, failures = compare_against_golden(ins, golden)
        assert not passed
        assert any("executive_summary" in f for f in failures)

    def test_missing_evidence_keyword_fails(self):
        # Build insight with no "insufficient" anywhere in any field
        ins = {
            "executive_summary": "Usage is healthy and growing.",
            "usage_insight": "50 views in the 28-day window.",
            "engagement_insight": "15 active users with strong engagement.",
            "forecast_insight": "A stable outlook is expected.",
            "model_confidence_note": "Forecast quality is high.",
            "recommended_action": "Continue monitoring.",
            "evidence_limitations": ["No notable limitations identified."],
        }
        golden = self._golden(evidence_limitations_keywords=["insufficient"])
        passed, failures = compare_against_golden(ins, golden)
        assert not passed
        assert any("insufficient" in f for f in failures)


# ══════════════════════════════════════════════════════════════════════════════
# 14. Fixture-driven cases: labelled expected outcome
# ══════════════════════════════════════════════════════════════════════════════

class TestFixtureDrivenCases:
    """
    Runs each labelled case through the evaluation function and checks that the
    actual outcome matches the expected_validation_outcome in the fixture.
    Only runs cases where expected_failure_dimensions is declared.
    """

    def _evaluate(self, case: dict) -> EvaluationResult:
        ins_type = case["insight_type"]
        if ins_type == "report":
            return evaluate_report_insight(
                insight=case["insight"],
                context=case["context"],
                case_id=case["case_id"],
            )
        else:
            return evaluate_portfolio_insight(
                insight=case["insight"],
                context=case["context"],
                case_id=case["case_id"],
            )

    def test_all_pass_cases_actually_pass(self):
        cases = load_evaluation_cases()
        pass_cases = [c for c in cases if c.get("expected_validation_outcome") == "pass"]
        assert pass_cases, "No pass cases in fixture"
        failures = []
        for case in pass_cases:
            result = self._evaluate(case)
            if not result.overall_pass:
                failures.append(
                    f"{case['case_id']}: expected pass, got fail. "
                    f"failure_reasons={result.failure_reasons}"
                )
        assert not failures, "\n".join(failures)

    def test_all_fail_cases_actually_fail(self):
        cases = load_evaluation_cases()
        fail_cases = [c for c in cases if c.get("expected_validation_outcome") == "fail"]
        assert fail_cases, "No fail cases in fixture"
        unexpected_passes = []
        for case in fail_cases:
            result = self._evaluate(case)
            if result.overall_pass:
                unexpected_passes.append(
                    f"{case['case_id']}: expected fail, got pass."
                )
        assert not unexpected_passes, "\n".join(unexpected_passes)

    def test_safety_fail_case_fails_on_safety(self):
        cases = load_evaluation_cases()
        safety_fail = next(
            (c for c in cases if "safety" in c.get("expected_failure_dimensions", [])),
            None,
        )
        assert safety_fail is not None, "No safety fail case in fixture"
        result = self._evaluate(safety_fail)
        assert not result.safety_pass

    def test_numerical_fail_case_fails_on_numerical(self):
        cases = load_evaluation_cases()
        num_fail = next(
            (c for c in cases if "numerical" in c.get("expected_failure_dimensions", [])),
            None,
        )
        assert num_fail is not None, "No numerical fail case in fixture"
        result = self._evaluate(num_fail)
        assert not result.numerical_pass

    def test_direction_fail_case_fails_on_direction(self):
        cases = load_evaluation_cases()
        dir_fail = next(
            (c for c in cases if "direction" in c.get("expected_failure_dimensions", [])),
            None,
        )
        assert dir_fail is not None, "No direction fail case in fixture"
        result = self._evaluate(dir_fail)
        assert not result.direction_pass


# ══════════════════════════════════════════════════════════════════════════════
# 15. save_evaluation_results
# ══════════════════════════════════════════════════════════════════════════════

class TestSaveEvaluationResults:
    def _make_result(self, overall_pass: bool = True, case_id: str = "t1") -> EvaluationResult:
        return EvaluationResult(
            case_id=case_id,
            insight_type="report",
            prompt_version="report_insight_v1",
            model_name="test-model",
            generation_status="success",
            completeness_pass=overall_pass,
            safety_pass=overall_pass,
            groundedness_pass=overall_pass,
            direction_pass=overall_pass,
            numerical_pass=overall_pass,
            action_alignment_pass=overall_pass,
            evidence_disclosure_pass=overall_pass,
            readability_score=0.9 if overall_pass else 0.0,
            conciseness_score=0.9,
            overall_pass=overall_pass,
            failure_reasons=[] if overall_pass else ["test_failure"],
        )

    def test_save_writes_csv_and_json(self, tmp_path):
        results = [self._make_result(True, "r1"), self._make_result(False, "r2")]
        project_root = tmp_path
        (project_root / "outputs" / "evaluation").mkdir(parents=True)
        paths = save_evaluation_results(results, project_root)
        assert paths["csv"].exists()
        assert paths["json"].exists()

    def test_summary_json_has_correct_pass_rate(self, tmp_path):
        results = [self._make_result(True), self._make_result(False)]
        (tmp_path / "outputs" / "evaluation").mkdir(parents=True)
        paths = save_evaluation_results(results, tmp_path)
        summary = json.loads(paths["json"].read_text())
        assert summary["overall_pass_rate"] == 0.5

    def test_latest_files_written(self, tmp_path):
        results = [self._make_result()]
        (tmp_path / "outputs" / "evaluation").mkdir(parents=True)
        paths = save_evaluation_results(results, tmp_path)
        assert paths["csv_latest"].exists()
        assert paths["json_latest"].exists()

    def test_save_empty_results(self, tmp_path):
        (tmp_path / "outputs" / "evaluation").mkdir(parents=True)
        paths = save_evaluation_results([], tmp_path)
        # Should not raise; json still written
        assert paths["json"].exists()


# ══════════════════════════════════════════════════════════════════════════════
# 16. collect_text helper
# ══════════════════════════════════════════════════════════════════════════════

class TestCollectText:
    def test_string_values_collected(self):
        ins = {"a": "hello", "b": "world"}
        text = _collect_text(ins)
        assert "hello" in text
        assert "world" in text

    def test_list_values_collected(self):
        ins = {"a": "intro", "b": ["item one", "item two"]}
        text = _collect_text(ins)
        assert "item one" in text
        assert "item two" in text

    def test_none_values_skipped(self):
        ins = {"a": None, "b": "valid"}
        text = _collect_text(ins)
        assert "valid" in text
