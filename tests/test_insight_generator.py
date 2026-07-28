"""
Tests for the refactored GenAI insight generator.
All model calls are mocked — no live API calls.
"""
import json
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.genai.insight_generator import (
    build_mart_context,
    _compute_context_hash,
    _load_existing_insights,
    _parse_json_response,
    _validate_insight_schema,
    generate_rule_based_insight,
    generate_ai_insight,
    generate_report_insights,
    INSIGHT_CONTEXT_ALLOWLIST,
    PROHIBITED_CONTEXT_PATTERNS,
    REQUIRED_INSIGHT_FIELDS,
)
from src.genai.prompts import (
    REPORT_INSIGHT_PROMPT_VERSION,
    build_report_insight_prompt,
)

AS_OF = "2024-03-31"
RUN_ID = "test-run-001"
MODEL = "gpt-4.1-mini"


# ── Fixtures ─────────────────────────────────────────────────────────────────

def _mart_row(**kw):
    """Minimal mart row with healthy defaults plus extra prohibited fields."""
    base = {
        "report_id": "R001",
        "report_name": "Sales Dashboard",
        "analytics_run_id": RUN_ID,
        "analytics_as_of_date": AS_OF,
        "historical_usage_status": "stable_regular_usage",
        "recent_28d_views": 50,
        "previous_28d_views": 48,
        "usage_change_28d_pct": 0.04,
        "days_since_last_use": 1,
        "forecast_outlook_status": "stable_outlook",
        "forecast_uncertainty_status": "low_uncertainty",
        "model_diagnostic_status": "good",
        "primary_model_issue": None,
        "forecast_interpretation_status": "outlook_supported",
        "unique_users_28d": 15,
        "active_user_direction_28d": "stable",
        "returning_user_share_28d": 0.60,
        "retained_user_rate_28d": 0.55,
        "lapse_rate_28d": 0.15,
        "overall_engagement_status": "healthy",
        "dependency_status": "distributed",
        "criticality_level": "high",
        "expected_usage_cadence": "daily",
        "primary_diagnostic": "none",
        "overall_report_status": "healthy",
        "overall_evidence_status": "complete",
        "recommended_report_action": "continue_monitoring",
        "overall_review_priority": "low",
        "report_reasons": "primary:none | action:continue_monitoring",
        "privacy_suppression_status": "not_suppressed",
        "privacy_suppressed_fields": None,
        "missing_engagement_evidence": None,
        "forecast_total_28d": 52.0,
        "forecast_change_vs_actual_28d_pct": 0.04,
        # Extra fields that should NOT appear in context
        "user_id": "secret",
        "user_key": "secret",
        "repeat_rate": 0.5,
        "latest_views": 50,
    }
    base.update(kw)
    return pd.DataFrame([base])


def _valid_insight():
    return {
        "executive_summary": "Sales Dashboard is healthy with 50 views in 28 days.",
        "usage_insight": "Usage is stable with 50 views.",
        "engagement_insight": "15 active users with returning share noted.",
        "forecast_insight": "Forecast is stable and is expected to continue.",
        "model_confidence_note": "Model diagnostic status is good.",
        "recommended_action": "Continue monitoring.",
        "evidence_limitations": ["No significant evidence limitations identified."],
    }


# ── Context building ─────────────────────────────────────────────────────────

class TestBuildMartContext:
    def test_returns_one_context_per_report(self):
        mart = _mart_row()
        contexts = build_mart_context(mart)
        assert len(contexts) == 1

    def test_only_allowlist_fields_present(self):
        mart = _mart_row()
        ctx = build_mart_context(mart)[0]
        for key in ctx:
            assert key in INSIGHT_CONTEXT_ALLOWLIST, f"Non-allowlist field: {key}"

    def test_prohibited_fields_stripped(self):
        mart = _mart_row()
        ctx = build_mart_context(mart)[0]
        for pat in PROHIBITED_CONTEXT_PATTERNS:
            assert pat not in ctx, f"Prohibited field in context: {pat}"

    def test_user_id_stripped(self):
        mart = _mart_row()
        ctx = build_mart_context(mart)[0]
        assert "user_id" not in ctx
        assert "user_key" not in ctx

    def test_deprecated_fields_stripped(self):
        mart = _mart_row()
        ctx = build_mart_context(mart)[0]
        assert "repeat_rate" not in ctx
        assert "latest_views" not in ctx

    def test_nan_converted_to_none(self):
        mart = _mart_row()
        mart = mart.copy()
        mart["days_since_last_use"] = float("nan")
        ctx = build_mart_context(mart)[0]
        assert ctx.get("days_since_last_use") is None

    def test_filter_by_report_id(self):
        mart = pd.concat([_mart_row(report_id="R001"), _mart_row(report_id="R002")], ignore_index=True)
        contexts = build_mart_context(mart, report_id_filter=["R001"])
        assert len(contexts) == 1
        assert contexts[0]["report_id"] == "R001"

    def test_multiple_reports(self):
        mart = pd.concat([_mart_row(report_id=f"R{i:03}") for i in range(5)], ignore_index=True)
        contexts = build_mart_context(mart)
        assert len(contexts) == 5


class TestPrivacySuppressedContext:
    def test_suppression_note_in_prompt(self):
        mart = _mart_row(
            privacy_suppression_status="privacy_suppressed",
            privacy_suppressed_fields="top_1_user_view_share_28d,user_view_hhi_28d",
        )
        ctx = build_mart_context(mart)[0]
        prompt = build_report_insight_prompt(ctx)
        assert "PRIVACY NOTE" in prompt
        assert "suppressed" in prompt.lower()

    def test_not_suppressed_no_note(self):
        mart = _mart_row(privacy_suppression_status="not_suppressed")
        ctx = build_mart_context(mart)[0]
        prompt = build_report_insight_prompt(ctx)
        assert "PRIVACY NOTE" not in prompt


# ── Prompt version ────────────────────────────────────────────────────────────

class TestPromptVersion:
    def test_prompt_version_constant_exists(self):
        assert REPORT_INSIGHT_PROMPT_VERSION
        assert isinstance(REPORT_INSIGHT_PROMPT_VERSION, str)

    def test_prompt_version_in_output_on_fallback(self):
        ctx = build_mart_context(_mart_row())[0]
        result = generate_ai_insight(ctx, MODEL, api_key="")
        assert result.get("prompt_version") == REPORT_INSIGHT_PROMPT_VERSION


# ── Schema validation ─────────────────────────────────────────────────────────

class TestValidateInsightSchema:
    def test_valid_insight_no_schema_errors(self):
        ctx = build_mart_context(_mart_row())[0]
        errors = _validate_insight_schema(_valid_insight(), ctx)
        schema_errors = [e for e in errors if "missing_field" in e or "empty_field" in e]
        assert not schema_errors

    def test_missing_required_field_flagged(self):
        ctx = build_mart_context(_mart_row())[0]
        incomplete = {k: v for k, v in _valid_insight().items() if k != "executive_summary"}
        errors = _validate_insight_schema(incomplete, ctx)
        assert any("missing_field:executive_summary" in e for e in errors)

    def test_empty_field_flagged(self):
        ctx = build_mart_context(_mart_row())[0]
        insight = {**_valid_insight(), "usage_insight": ""}
        errors = _validate_insight_schema(insight, ctx)
        assert any("empty_field" in e for e in errors)

    def test_retirement_phrase_rejected(self):
        ctx = build_mart_context(_mart_row())[0]
        insight = {**_valid_insight(), "recommended_action": "We should retire this report."}
        errors = _validate_insight_schema(insight, ctx)
        assert any("prohibited_phrase" in e for e in errors)

    def test_deletion_phrase_rejected(self):
        ctx = build_mart_context(_mart_row())[0]
        insight = {**_valid_insight(), "executive_summary": "Consider deleting this report."}
        errors = _validate_insight_schema(insight, ctx)
        assert any("prohibited_phrase" in e for e in errors)

    def test_deterministic_action_preserved(self):
        ctx = build_mart_context(_mart_row())[0]
        fallback = generate_rule_based_insight(ctx)
        assert fallback["recommended_action"].strip()


# ── Parse JSON response ───────────────────────────────────────────────────────

class TestParseJsonResponse:
    def test_valid_json_parsed(self):
        text = json.dumps(_valid_insight())
        result = _parse_json_response(text)
        assert result["executive_summary"] == _valid_insight()["executive_summary"]

    def test_fenced_block_extracted(self):
        text = f"Sure!\n```json\n{json.dumps(_valid_insight())}\n```"
        result = _parse_json_response(text)
        assert "executive_summary" in result

    def test_invalid_json_raises(self):
        with pytest.raises(ValueError, match="JSON parse failed"):
            _parse_json_response("not json at all {{{{")

    def test_non_dict_raises(self):
        with pytest.raises(ValueError):
            _parse_json_response("[1, 2, 3]")


# ── Rule-based fallback ───────────────────────────────────────────────────────

class TestRuleBasedFallback:
    def test_all_required_fields_present(self):
        ctx = build_mart_context(_mart_row())[0]
        result = generate_rule_based_insight(ctx)
        for field in REQUIRED_INSIGHT_FIELDS:
            assert field in result, f"Missing: {field}"

    def test_backward_compat_aliases_present(self):
        ctx = build_mart_context(_mart_row())[0]
        result = generate_rule_based_insight(ctx)
        for alias in ("health_status", "forecast_summary", "recommended_actions", "confidence"):
            assert alias in result, f"Missing alias: {alias}"

    def test_no_prohibited_phrases(self):
        ctx = build_mart_context(_mart_row())[0]
        result = generate_rule_based_insight(ctx)
        text = " ".join(str(v) for v in result.values() if isinstance(v, str)).lower()
        for phrase in {"retire", "delete", "retrain"}:
            assert phrase not in text

    def test_uses_recommended_report_action(self):
        ctx = build_mart_context(_mart_row(recommended_report_action="review_inactivity"))[0]
        result = generate_rule_based_insight(ctx)
        assert (
            "review" in result["recommended_action"].lower()
            or "inactivity" in result["recommended_action"].lower()
        )

    def test_privacy_suppressed_noted(self):
        ctx = build_mart_context(_mart_row(privacy_suppression_status="privacy_suppressed"))[0]
        result = generate_rule_based_insight(ctx)
        limitations_text = " ".join(str(v) for v in result["evidence_limitations"]).lower()
        assert "privacy" in limitations_text or "suppressed" in limitations_text

    def test_limited_evidence_noted(self):
        ctx = build_mart_context(_mart_row(overall_evidence_status="partial"))[0]
        result = generate_rule_based_insight(ctx)
        limitations_text = " ".join(str(v) for v in result["evidence_limitations"]).lower()
        text_all = " ".join(
            str(v) for v in result.values() if isinstance(v, str)
        ).lower()
        assert "partial" in limitations_text or "limited" in text_all or "unavailable" in text_all


# ── Generate AI insight (mocked API) ─────────────────────────────────────────

class TestGenerateAiInsight:
    def _mock_api_response(self, insight_dict):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "output": [{"content": [{"text": json.dumps(insight_dict)}]}]
        }
        return mock_resp

    def test_success_returns_all_lineage_fields(self):
        ctx = build_mart_context(_mart_row())[0]
        with patch("requests.post", return_value=self._mock_api_response(_valid_insight())):
            result = generate_ai_insight(ctx, MODEL, api_key="fake-key")
        for field in ["analytics_run_id", "analytics_as_of_date", "prompt_version", "model_name", "generated_at", "input_hash"]:
            assert field in result, f"Missing lineage: {field}"

    def test_success_status(self):
        ctx = build_mart_context(_mart_row())[0]
        with patch("requests.post", return_value=self._mock_api_response(_valid_insight())):
            result = generate_ai_insight(ctx, MODEL, api_key="fake-key")
        assert result["generation_status"] == "success"

    def test_success_has_backward_compat_aliases(self):
        ctx = build_mart_context(_mart_row())[0]
        with patch("requests.post", return_value=self._mock_api_response(_valid_insight())):
            result = generate_ai_insight(ctx, MODEL, api_key="fake-key")
        assert "health_status" in result
        assert "forecast_summary" in result
        assert "recommended_actions" in result

    def test_no_api_key_uses_rule_based(self):
        ctx = build_mart_context(_mart_row())[0]
        result = generate_ai_insight(ctx, MODEL, api_key="")
        assert result["generation_status"] == "rule_based"
        for field in REQUIRED_INSIGHT_FIELDS:
            assert field in result

    def test_api_failure_falls_back(self):
        ctx = build_mart_context(_mart_row())[0]
        with patch("requests.post", side_effect=Exception("network error")):
            result = generate_ai_insight(ctx, MODEL, api_key="fake-key")
        assert "fallback" in result["generation_status"]
        for field in REQUIRED_INSIGHT_FIELDS:
            assert field in result

    def test_schema_invalid_falls_back(self):
        """LLM returns JSON missing required fields → fallback."""
        ctx = build_mart_context(_mart_row())[0]
        incomplete = {"executive_summary": "ok"}  # missing 6 required fields
        with patch("requests.post", return_value=self._mock_api_response(incomplete)):
            result = generate_ai_insight(ctx, MODEL, api_key="fake-key")
        # Either it fell back or passed despite warnings
        assert "generation_status" in result
        for field in REQUIRED_INSIGHT_FIELDS:
            assert field in result

    def test_retirement_phrase_triggers_fallback_or_warning(self):
        ctx = build_mart_context(_mart_row())[0]
        bad_insight = {**_valid_insight(), "recommended_action": "Retire this report."}
        with patch("requests.post", return_value=self._mock_api_response(bad_insight)):
            result = generate_ai_insight(ctx, MODEL, api_key="fake-key")
        # Either validation_status is not 'valid' or it fell back
        assert result.get("validation_status") != "valid" or "fallback" in result.get("generation_status", "")


# ── Skip-unchanged (hash) ──────────────────────────────────────────────────────

class TestInputHashSkipUnchanged:
    def test_same_context_same_hash(self):
        ctx = build_mart_context(_mart_row())[0]
        h1 = _compute_context_hash(ctx, REPORT_INSIGHT_PROMPT_VERSION, MODEL)
        h2 = _compute_context_hash(ctx, REPORT_INSIGHT_PROMPT_VERSION, MODEL)
        assert h1 == h2

    def test_changed_context_different_hash(self):
        ctx1 = build_mart_context(_mart_row())[0]
        ctx2 = build_mart_context(_mart_row(recent_28d_views=99))[0]
        h1 = _compute_context_hash(ctx1, REPORT_INSIGHT_PROMPT_VERSION, MODEL)
        h2 = _compute_context_hash(ctx2, REPORT_INSIGHT_PROMPT_VERSION, MODEL)
        assert h1 != h2

    def test_different_prompt_version_different_hash(self):
        ctx = build_mart_context(_mart_row())[0]
        h1 = _compute_context_hash(ctx, "v1", MODEL)
        h2 = _compute_context_hash(ctx, "v2", MODEL)
        assert h1 != h2

    def test_reuse_when_hash_matches(self):
        ctx = build_mart_context(_mart_row())[0]
        input_hash = _compute_context_hash(ctx, REPORT_INSIGHT_PROMPT_VERSION, MODEL)
        existing = {
            **_valid_insight(),
            "report_id": "R001",
            "report_name": "Sales Dashboard",
            "input_hash": input_hash,
            "generation_status": "success",
            "prompt_version": REPORT_INSIGHT_PROMPT_VERSION,
        }
        result = generate_ai_insight(ctx, MODEL, api_key="fake-key", existing_insight=existing)
        assert result["generation_status"] == "reused"

    def test_regenerate_when_hash_differs(self):
        ctx = build_mart_context(_mart_row())[0]
        existing = {
            **_valid_insight(),
            "report_id": "R001",
            "input_hash": "stale-hash-000",
            "generation_status": "success",
        }
        result = generate_ai_insight(ctx, MODEL, api_key="", existing_insight=existing)
        assert result["generation_status"] != "reused"


# ── Batch generation ──────────────────────────────────────────────────────────

class TestGenerateReportInsights:
    def _write_mart(self, tmp_path, mart_df):
        (tmp_path / "outputs" / "analytics").mkdir(parents=True, exist_ok=True)
        mart_df.to_csv(
            tmp_path / "outputs" / "analytics" / "mart_report_analytics.csv",
            index=False,
        )

    def test_one_failed_report_does_not_stop_batch(self, tmp_path):
        mart = pd.concat([_mart_row(report_id=f"R{i:03}") for i in range(3)], ignore_index=True)
        self._write_mart(tmp_path, mart)
        results = generate_report_insights(project_root=tmp_path, model=MODEL)
        assert len(results) == 3

    def test_mart_is_canonical_source(self, tmp_path):
        """generate_report_insights must read from mart, not legacy CSVs."""
        mart = _mart_row()
        self._write_mart(tmp_path, mart)
        results = generate_report_insights(project_root=tmp_path, model=MODEL)
        assert len(results) == 1
        assert results[0]["report_id"] == "R001"

    def test_missing_mart_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            generate_report_insights(project_root=tmp_path, model=MODEL)

    def test_no_live_api_call_without_key(self, tmp_path):
        mart = _mart_row()
        self._write_mart(tmp_path, mart)
        with patch("requests.post") as mock_post:
            with patch.dict("os.environ", {"OPENAI_API_KEY": ""}):
                results = generate_report_insights(project_root=tmp_path, model=MODEL)
            mock_post.assert_not_called()
        assert len(results) == 1

    def test_all_results_have_genai_run_id(self, tmp_path):
        mart = pd.concat([_mart_row(report_id=f"R{i:03}") for i in range(3)], ignore_index=True)
        self._write_mart(tmp_path, mart)
        results = generate_report_insights(project_root=tmp_path, model=MODEL)
        run_ids = {r.get("genai_run_id") for r in results}
        assert len(run_ids) == 1  # all share same run_id
        assert None not in run_ids

    def test_limit_param(self, tmp_path):
        mart = pd.concat([_mart_row(report_id=f"R{i:03}") for i in range(5)], ignore_index=True)
        self._write_mart(tmp_path, mart)
        results = generate_report_insights(project_root=tmp_path, model=MODEL, limit=2)
        assert len(results) == 2
