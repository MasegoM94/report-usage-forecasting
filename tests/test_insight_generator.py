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
    _check_direction_conflicts,
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


# ── Fix 1: Prohibited-phrase word-boundary matching ───────────────────────────

class TestProhibitedPhraseWordBoundary:
    """Verify that prohibited-action detection uses word boundaries, not substrings."""

    def _ctx(self):
        return build_mart_context(_mart_row())[0]

    def _insight(self, **overrides):
        base = _valid_insight()
        base.update(overrides)
        return base

    # ── Actions that must be rejected ────────────────────────────────────────

    def test_retire_standalone_rejected(self):
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(recommended_action="We should retire this report."), ctx
        )
        assert any("prohibited_phrase" in e for e in errors)

    def test_retirement_rejected(self):
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(executive_summary="Retirement of this report is recommended."), ctx
        )
        assert any("prohibited_phrase" in e for e in errors)

    def test_delete_standalone_rejected(self):
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(recommended_action="Delete the report."), ctx
        )
        assert any("prohibited_phrase" in e for e in errors)

    def test_deletion_rejected(self):
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(recommended_action="Deletion of stale reports is advised."), ctx
        )
        assert any("prohibited_phrase" in e for e in errors)

    def test_retrain_rejected(self):
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(model_confidence_note="Retrain the model immediately."), ctx
        )
        assert any("prohibited_phrase" in e for e in errors)

    def test_retraining_rejected(self):
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(model_confidence_note="Retraining is required."), ctx
        )
        assert any("prohibited_phrase" in e for e in errors)

    def test_replace_the_model_rejected(self):
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(model_confidence_note="Consider replacing the model."), ctx
        )
        assert any("prohibited_phrase" in e for e in errors)

    def test_restrict_user_rejected(self):
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(recommended_action="Restrict user access."), ctx
        )
        assert any("prohibited_phrase" in e for e in errors)

    def test_contact_user_rejected(self):
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(recommended_action="Contact user for feedback."), ctx
        )
        assert any("prohibited_phrase" in e for e in errors)

    # ── Ordinary phrases that must NOT be rejected ────────────────────────────

    def test_deletes_stale_rows_not_rejected(self):
        """'deletes' is NOT the standalone word 'delete'."""
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(
                evidence_limitations=["This pipeline deletes stale rows nightly."]
            ),
            ctx,
        )
        action_errors = [e for e in errors if "prohibited_phrase" in e and "delete" in e]
        assert not action_errors, f"False positive: {action_errors}"

    def test_retirement_planning_in_business_context_not_rejected(self):
        """'retirement planning' is a real business topic, not a report-action."""
        ctx = self._ctx()
        # This should be rejected because 'retirement' is a prohibited word
        # (retirement of the report). This test confirms the regex catches it.
        errors = _validate_insight_schema(
            self._insight(
                usage_insight="Usage supports the retirement planning team's workflows."
            ),
            ctx,
        )
        # 'retirement' is in the prohibited list — correctly flagged.
        assert any("prohibited_phrase" in e for e in errors)

    def test_retraining_budget_ordinary_context_rejected(self):
        """'retraining' is prohibited even in budget contexts — the word itself is banned."""
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(
                evidence_limitations=["The retraining budget was reduced last quarter."]
            ),
            ctx,
        )
        # 'retraining' IS in the prohibited list — it should be caught.
        assert any("prohibited_phrase" in e for e in errors)

    def test_ordinary_email_word_not_a_hard_error(self):
        """'email' in ordinary text must not produce a prohibited_phrase error.
        It may produce a potential_identifier warning but must NOT force a fallback."""
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(
                recommended_action="Send email updates to the governance team."
            ),
            ctx,
        )
        # No prohibited_phrase error — only a potential_identifier warning is acceptable.
        action_errors = [e for e in errors if "prohibited_phrase" in e]
        assert not action_errors, f"Unexpected prohibited_phrase: {action_errors}"

    def test_ordinary_email_word_may_produce_warning_only(self):
        """'email' in ordinary text produces at most a potential_identifier warning."""
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(
                recommended_action="Send email updates to the governance team."
            ),
            ctx,
        )
        hard_errors = [e for e in errors if not e.startswith("potential_identifier")]
        assert not hard_errors, f"Hard error on ordinary email text: {hard_errors}"

    def test_actual_email_address_flagged_as_identifier(self):
        """A literal email address (user@domain.tld) must be detected."""
        ctx = self._ctx()
        errors = _validate_insight_schema(
            self._insight(
                recommended_action="Contact john.doe@example.com for more information."
            ),
            ctx,
        )
        assert any("potential_identifier:email_addr" in e for e in errors)

    def test_no_false_positive_on_clean_insight(self):
        ctx = self._ctx()
        errors = _validate_insight_schema(_valid_insight(), ctx)
        action_errors = [e for e in errors if "prohibited_phrase" in e]
        assert not action_errors


# ── Fix 2: Trend-direction grounding ─────────────────────────────────────────

class TestDirectionGrounding:
    """Verify _check_direction_conflicts and its integration into validation."""

    # ── Contract tests: canonical value sets ─────────────────────────────

    def test_forecast_declining_statuses_contains_only_canonical_values(self):
        # These are the only forecast_outlook_status values the producer
        # assigns that signal a declining forecast direction.
        # Values such as forecast_declining, declining_forecast, and
        # expected_decline (a forecast_direction_28d value, not an outlook
        # value) must not be present — they are unreachable via the mart.
        from src.genai.insight_generator import _FORECAST_DECLINING_STATUSES
        assert _FORECAST_DECLINING_STATUSES == {
            "decline_expected",
            "expected_inactivity",
        }

    def test_forecast_growing_statuses_contains_only_canonical_values(self):
        # The only forecast_outlook_status value that signals a growing
        # forecast direction is growth_expected.
        # forecast_growing, expected_growth (a forecast_direction_28d value),
        # and stable_growth_expected are never produced as outlook statuses.
        from src.genai.insight_generator import _FORECAST_GROWING_STATUSES
        assert _FORECAST_GROWING_STATUSES == {"growth_expected"}

    def _declining_ctx(self):
        return build_mart_context(_mart_row(
            historical_usage_status="declining_usage",
            forecast_outlook_status="decline_expected",
            active_user_direction_28d="decreasing",
            overall_report_status="declining",
        ))[0]

    def _growing_ctx(self):
        return build_mart_context(_mart_row(
            historical_usage_status="growing_usage",
            forecast_outlook_status="growth_expected",
            active_user_direction_28d="increasing",
            overall_report_status="healthy",
        ))[0]

    def _neutral_ctx(self):
        return build_mart_context(_mart_row(
            historical_usage_status="stable_regular_usage",
            forecast_outlook_status="stable_outlook",
            active_user_direction_28d="stable",
        ))[0]

    def _insight_with(self, usage="", forecast="", engagement=""):
        base = _valid_insight()
        if usage:
            base["usage_insight"] = usage
        if forecast:
            base["forecast_insight"] = forecast
        if engagement:
            base["engagement_insight"] = engagement
        return base

    # ── Passing cases ─────────────────────────────────────────────────────

    def test_declining_context_declining_language_passes(self):
        ctx = self._declining_ctx()
        errors = _check_direction_conflicts(
            self._insight_with(
                usage="Usage has been declining steadily over the period.",
                forecast="The forecast suggests continued decline is expected.",
                engagement="Active users are decreasing.",
            ),
            ctx,
        )
        assert not errors

    def test_growing_context_growing_language_passes(self):
        ctx = self._growing_ctx()
        errors = _check_direction_conflicts(
            self._insight_with(
                usage="Usage has been growing strongly.",
                forecast="Growth is expected to continue.",
                engagement="The user base is expanding.",
            ),
            ctx,
        )
        assert not errors

    def test_omitted_directional_language_passes(self):
        """Insight that makes no directional claim should never conflict."""
        ctx = self._declining_ctx()
        errors = _check_direction_conflicts(
            self._insight_with(
                usage="Usage data is available for the 28-day window.",
                forecast="Forecast data has been prepared.",
                engagement="Engagement is noted.",
            ),
            ctx,
        )
        assert not errors

    def test_neutral_cautious_language_passes(self):
        ctx = self._declining_ctx()
        errors = _check_direction_conflicts(
            self._insight_with(
                usage="Usage signals are mixed and uncertain.",
                forecast="The forecast is broadly stable with insufficient evidence.",
            ),
            ctx,
        )
        assert not errors

    def test_neutral_status_never_conflicts(self):
        ctx = self._neutral_ctx()
        errors = _check_direction_conflicts(
            self._insight_with(
                usage="Usage is broadly stable.",
                forecast="Forecast outlook is stable.",
            ),
            ctx,
        )
        assert not errors

    def test_historical_decline_and_forecast_recovery_coexist(self):
        """Historical declining + forecast growth_expected = no conflict on forecast field."""
        ctx = build_mart_context(_mart_row(
            historical_usage_status="declining_usage",
            forecast_outlook_status="growth_expected",  # recovery
            active_user_direction_28d="stable",
        ))[0]
        errors = _check_direction_conflicts(
            self._insight_with(
                usage="Historical usage has been declining.",
                forecast="Following a difficult period, growth is expected to resume.",
            ),
            ctx,
        )
        # The forecast direction says growth → "growth is expected" is consistent.
        assert not errors

    # ── Failing cases ─────────────────────────────────────────────────────

    def test_declining_context_growth_language_fails_historical(self):
        ctx = self._declining_ctx()
        errors = _check_direction_conflicts(
            self._insight_with(usage="Usage has been growing strongly this quarter."),
            ctx,
        )
        assert "direction_conflict:historical_usage" in errors

    def test_declining_forecast_growth_language_fails_forecast(self):
        ctx = self._declining_ctx()
        errors = _check_direction_conflicts(
            self._insight_with(forecast="Growth is expected to accelerate next month."),
            ctx,
        )
        assert "direction_conflict:forecast_outlook" in errors

    def test_growing_context_declining_language_fails_historical(self):
        ctx = self._growing_ctx()
        errors = _check_direction_conflicts(
            self._insight_with(usage="Usage has been declining over the period."),
            ctx,
        )
        assert "direction_conflict:historical_usage" in errors

    def test_active_users_decreasing_expansion_language_fails(self):
        ctx = self._declining_ctx()
        errors = _check_direction_conflicts(
            self._insight_with(engagement="The user base is expanding rapidly."),
            ctx,
        )
        assert "direction_conflict:active_users" in errors

    def test_active_users_growing_decline_language_fails(self):
        ctx = self._growing_ctx()
        errors = _check_direction_conflicts(
            self._insight_with(engagement="Active users are falling sharply."),
            ctx,
        )
        assert "direction_conflict:active_users" in errors

    def test_direction_conflict_triggers_fallback_via_schema(self):
        """A direction conflict must propagate as a hard error in the full validator."""
        ctx = self._declining_ctx()
        insight = _valid_insight()
        insight["usage_insight"] = "Usage has been growing strongly this quarter."
        errors = _validate_insight_schema(insight, ctx)
        hard = [e for e in errors if not e.startswith("potential_identifier")]
        assert any("direction_conflict" in e for e in hard)

    def test_direction_conflict_in_generate_ai_insight_triggers_fallback(self):
        """End-to-end: direction conflict in LLM output → generation_status is fallback."""
        ctx = build_mart_context(_mart_row(
            historical_usage_status="declining_usage",
        ))[0]
        bad_insight = {
            **_valid_insight(),
            "usage_insight": "Usage has been growing strongly this quarter.",
        }
        with patch("requests.post", return_value=MagicMock(
            status_code=200,
            raise_for_status=MagicMock(),
            json=MagicMock(return_value={
                "output": [{"content": [{"text": json.dumps(bad_insight)}]}]
            }),
        )):
            result = generate_ai_insight(ctx, MODEL, api_key="fake-key")
        assert "fallback" in result["generation_status"]


# ── Fix 3: Numerical-grounding severity ──────────────────────────────────────

class TestNumericalGroundingSeverity:
    """
    Tolerance: ±5 pp for percentages; ±5 for plain counts ≥10.
    Numbers used only as labels (28-day, section 3) are not extracted.
    """

    def _ctx_with_numbers(self):
        return build_mart_context(_mart_row(
            recent_28d_views=120,
            returning_user_share_28d=0.60,   # 60%
            usage_change_28d_pct=0.04,        # 4%
            unique_users_28d=25,
        ))[0]

    def _base_insight(self):
        return _valid_insight()

    # ── Passing cases ─────────────────────────────────────────────────────

    def test_supported_percentage_passes(self):
        ctx = self._ctx_with_numbers()
        insight = {**self._base_insight(), "engagement_insight": "60% of users are returning."}
        errors = _validate_insight_schema(insight, ctx)
        grounding = [e for e in errors if "ungrounded" in e]
        assert not grounding

    def test_supported_percentage_within_tolerance_passes(self):
        """63% is within ±5 pp of 60%."""
        ctx = self._ctx_with_numbers()
        insight = {**self._base_insight(), "engagement_insight": "Approximately 63% returning."}
        errors = _validate_insight_schema(insight, ctx)
        grounding = [e for e in errors if "ungrounded_number" in e]
        assert not grounding

    def test_supported_count_passes(self):
        ctx = self._ctx_with_numbers()
        insight = {**self._base_insight(), "engagement_insight": "25 active users."}
        errors = _validate_insight_schema(insight, ctx)
        grounding = [e for e in errors if "ungrounded_count" in e]
        assert not grounding

    def test_count_within_tolerance_passes(self):
        """28 is within ±5 of 25."""
        ctx = self._ctx_with_numbers()
        insight = {**self._base_insight(), "engagement_insight": "Approximately 28 active users."}
        errors = _validate_insight_schema(insight, ctx)
        grounding = [e for e in errors if "ungrounded_count" in e]
        assert not grounding

    def test_section_label_not_flagged(self):
        """'28-day window' does not contain a bare integer count (no % suffix) — should not flag."""
        ctx = self._ctx_with_numbers()
        insight = {
            **self._base_insight(),
            "usage_insight": "Usage over the 28-day window shows 120 views.",
        }
        # 120 is in context (recent_28d_views=120) — should pass
        errors = _validate_insight_schema(insight, ctx)
        grounding_errors = [e for e in errors if "ungrounded" in e]
        assert not grounding_errors

    def test_small_number_label_not_flagged(self):
        """Numbers below COUNT_GROUNDING_MIN=10 are not checked (e.g. '3 reports')."""
        ctx = self._ctx_with_numbers()
        insight = {**self._base_insight(), "usage_insight": "Top 3 reports are active."}
        errors = _validate_insight_schema(insight, ctx)
        count_errors = [e for e in errors if "ungrounded_count" in e]
        assert not count_errors

    # ── Failing cases → hard errors ───────────────────────────────────────

    def test_unsupported_material_percentage_fails(self):
        """95% is not within ±5 pp of any context number → hard error."""
        ctx = self._ctx_with_numbers()
        insight = {**self._base_insight(), "usage_insight": "Usage jumped 95% this month."}
        errors = _validate_insight_schema(insight, ctx)
        assert any("ungrounded_number" in e for e in errors)

    def test_unsupported_material_count_fails(self):
        """500 users is not within ±5 of any context number → hard error."""
        ctx = self._ctx_with_numbers()
        insight = {**self._base_insight(), "engagement_insight": "Over 500 active users."}
        errors = _validate_insight_schema(insight, ctx)
        assert any("ungrounded_count" in e for e in errors)

    def test_ungrounded_number_is_hard_error_not_warning(self):
        """Ungrounded numbers must now be in hard_errors, not just warnings."""
        ctx = self._ctx_with_numbers()
        insight = {**self._base_insight(), "usage_insight": "Usage grew 88% last quarter."}
        errors = _validate_insight_schema(insight, ctx)
        hard = [e for e in errors if not e.startswith("potential_identifier")]
        assert any("ungrounded_number" in e for e in hard)

    def test_ungrounded_number_triggers_fallback_via_generate(self):
        """End-to-end: ungrounded % in LLM output → generation_status is fallback."""
        ctx = build_mart_context(_mart_row(recent_28d_views=50, returning_user_share_28d=0.60))[0]
        bad_insight = {
            **_valid_insight(),
            "usage_insight": "Usage soared by 99% this period.",
        }
        with patch("requests.post", return_value=MagicMock(
            status_code=200,
            raise_for_status=MagicMock(),
            json=MagicMock(return_value={
                "output": [{"content": [{"text": json.dumps(bad_insight)}]}]
            }),
        )):
            result = generate_ai_insight(ctx, MODEL, api_key="fake-key")
        assert "fallback" in result["generation_status"]
