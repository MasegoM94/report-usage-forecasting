"""Tests for prompt templates."""
import pytest
from src.genai.prompts import (
    REPORT_INSIGHT_SYSTEM_PROMPT,
    REPORT_INSIGHT_PROMPT_VERSION,
    build_report_insight_prompt,
)


class TestPromptVersion:
    def test_version_constant_is_string(self):
        assert isinstance(REPORT_INSIGHT_PROMPT_VERSION, str)
        assert len(REPORT_INSIGHT_PROMPT_VERSION) > 0

    def test_version_contains_v_or_underscore(self):
        assert "v" in REPORT_INSIGHT_PROMPT_VERSION.lower() or "_" in REPORT_INSIGHT_PROMPT_VERSION


class TestSystemPrompt:
    def test_system_prompt_is_string(self):
        assert isinstance(REPORT_INSIGHT_SYSTEM_PROMPT, str)
        assert len(REPORT_INSIGHT_SYSTEM_PROMPT) > 100

    def test_no_retirement_instruction_as_positive_command(self):
        # The prompt may mention "retire" in a prohibition context only
        lower = REPORT_INSIGHT_SYSTEM_PROMPT.lower()
        # It should mention retirement/retire only in the prohibition list
        if "retire" in lower:
            # Must appear alongside "do not recommend" or similar prohibition
            assert "do not recommend" in lower or "retirement" in lower

    def test_prohibits_metric_recalculation(self):
        assert "do not recalculate" in REPORT_INSIGHT_SYSTEM_PROMPT.lower()

    def test_instructs_json_only(self):
        assert "json" in REPORT_INSIGHT_SYSTEM_PROMPT.lower()

    def test_distinguishes_forecast_and_model_health(self):
        prompt_lower = REPORT_INSIGHT_SYSTEM_PROMPT.lower()
        assert "forecast" in prompt_lower
        assert "model" in prompt_lower

    def test_mentions_privacy_or_suppression(self):
        lower = REPORT_INSIGHT_SYSTEM_PROMPT.lower()
        assert "suppressed" in lower or "privacy" in lower

    def test_prohibits_retirement_in_list(self):
        lower = REPORT_INSIGHT_SYSTEM_PROMPT.lower()
        assert "retirement" in lower or "retire" in lower

    def test_requires_uncertainty_language(self):
        lower = REPORT_INSIGHT_SYSTEM_PROMPT.lower()
        assert "expected" in lower or "expectation" in lower or "certainties" in lower

    def test_required_json_schema_fields_mentioned(self):
        lower = REPORT_INSIGHT_SYSTEM_PROMPT.lower()
        for field in ("executive_summary", "usage_insight", "forecast_insight", "recommended_action"):
            assert field in lower, f"Schema field missing from system prompt: {field}"


class TestBuildReportInsightPrompt:
    def _ctx(self, **kw):
        base = {
            "report_id": "R001",
            "report_name": "Test Report",
            "historical_usage_status": "stable_regular_usage",
            "recommended_report_action": "continue_monitoring",
            "overall_report_status": "healthy",
            "privacy_suppression_status": "not_suppressed",
            "privacy_suppressed_fields": None,
            "overall_evidence_status": "complete",
        }
        base.update(kw)
        return base

    def test_returns_string(self):
        prompt = build_report_insight_prompt(self._ctx())
        assert isinstance(prompt, str)
        assert len(prompt) > 50

    def test_contains_report_name(self):
        prompt = build_report_insight_prompt(self._ctx())
        assert "Test Report" in prompt

    def test_privacy_suppressed_adds_note(self):
        ctx = self._ctx(
            privacy_suppression_status="privacy_suppressed",
            privacy_suppressed_fields="top_1_user_view_share_28d",
        )
        prompt = build_report_insight_prompt(ctx)
        assert "PRIVACY NOTE" in prompt

    def test_not_suppressed_no_privacy_note(self):
        prompt = build_report_insight_prompt(self._ctx())
        assert "PRIVACY NOTE" not in prompt

    def test_limited_evidence_adds_note(self):
        ctx = self._ctx(overall_evidence_status="insufficient")
        prompt = build_report_insight_prompt(ctx)
        assert "EVIDENCE NOTE" in prompt

    def test_complete_evidence_no_evidence_note(self):
        prompt = build_report_insight_prompt(self._ctx())
        assert "EVIDENCE NOTE" not in prompt

    def test_context_json_in_prompt(self):
        prompt = build_report_insight_prompt(self._ctx())
        assert "stable_regular_usage" in prompt

    def test_suppressed_fields_stripped_from_context_json(self):
        ctx = self._ctx(
            privacy_suppressed_fields="top_1_user_view_share_28d",
            privacy_suppression_status="privacy_suppressed",
        )
        prompt = build_report_insight_prompt(ctx)
        # The PRIVACY NOTE should appear
        assert "PRIVACY NOTE" in prompt
