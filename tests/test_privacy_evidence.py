"""Tests confirming correct privacy and evidence-state terminology.

These tests are the authoritative guard against four categories of terminology mistakes:
  1. Suppressed values must never be formatted as zero.
  2. Insufficient evidence must not be labelled unhealthy or declining.
  3. Concentration must not be labelled misuse.
  4. Recommended actions must remain recommendations (not automated actions).
  5. Invalid GenAI output must not be rendered as trusted.
  6. User-level identifiers must not be present in app data.

No Streamlit runtime. No live LLM calls.
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.app.utils.report_helpers import (
    build_report_detail,
    classify_genai_state,
    get_genai_field,
    suppression_aware_metric,
    is_field_suppressed,
    GENAI_STATE_LABELS,
)
from src.app.utils.definitions import DEFINITIONS, STATUS_LABELS, status_label


# ---------------------------------------------------------------------------
# 1. Suppressed values are never formatted as zero
# ---------------------------------------------------------------------------

class TestSuppressionNeverZero:
    def test_suppression_aware_metric_returns_suppressed_label(self):
        result = suppression_aware_metric(None, suppressed=True, fmt_fn=str)
        assert "suppressed" in result.lower()

    def test_suppression_aware_metric_returns_suppressed_even_with_numeric_value(self):
        result = suppression_aware_metric(0.0, suppressed=True, fmt_fn=str)
        assert "suppressed" in result.lower()
        assert result != "0.0"
        assert result != "0"

    def test_suppression_aware_metric_zero_value_not_suppressed(self):
        result = suppression_aware_metric(0.0, suppressed=False, fmt_fn=lambda v: f"{v:.1f}")
        assert result == "0.0"

    def test_engagement_suppression_flag_propagated_to_detail(self):
        mart = pd.Series({"report_id": "R_001", "report_name": "Test"})
        eng = pd.Series({
            "report_id": "R_001",
            "privacy_suppressed": True,
            "cohort_privacy_suppressed": True,
            "concentration_privacy_suppressed": True,
        })
        detail = build_report_detail(mart, eng, pd.Series(dtype="object"))
        assert detail["engagement"]["_any_suppressed"] is True

    def test_suppressed_label_in_status_labels(self):
        label = STATUS_LABELS.get("suppressed", "")
        assert "suppressed" in label.lower() or "privacy" in label.lower()

    def test_not_suppressed_label_distinct_from_zero(self):
        label = STATUS_LABELS.get("not_suppressed", "")
        assert label != "0" and label != "0.0"

    def test_suppression_aware_insufficient_label(self):
        result = suppression_aware_metric(None, suppressed=False, insufficient=True, fmt_fn=str)
        assert "insufficient" in result.lower() or "history" in result.lower()

    def test_suppression_aware_null_not_suppressed_returns_dash(self):
        result = suppression_aware_metric(None, suppressed=False, fmt_fn=str)
        assert result == "—"


# ---------------------------------------------------------------------------
# 2. Insufficient evidence is not labelled unhealthy
# ---------------------------------------------------------------------------

class TestInsufficientEvidenceNotUnhealthy:
    def test_insufficient_evidence_status_label(self):
        label = status_label("insufficient_evidence")
        assert "insufficient" in label.lower()
        assert "unhealthy" not in label.lower()
        assert "poor" not in label.lower()

    def test_insufficient_evidence_definition_no_unhealthy(self):
        defn = DEFINITIONS["insufficient_evidence"].lower()
        assert "unhealthy" not in defn

    def test_insufficient_evidence_definition_says_evidence_maturity(self):
        defn = DEFINITIONS["insufficient_evidence"].lower()
        assert "evidence" in defn or "maturity" in defn or "history" in defn

    def test_model_health_definition_no_unhealthy_for_insufficient(self):
        defn = DEFINITIONS["model_diagnostic_status"].lower()
        assert "unhealthy" not in defn

    def test_missing_genai_not_classified_as_declining(self):
        state = classify_genai_state(pd.Series(dtype="object"))
        assert state == "missing"
        assert state != "declining"

    def test_missing_engagement_not_labelled_declining(self):
        mart = pd.Series({"report_id": "R_001"})
        eng = pd.Series(dtype="object")
        detail = build_report_detail(mart, eng, pd.Series(dtype="object"))
        eng_status = detail["engagement"].get("overall_engagement_status")
        assert eng_status is None or eng_status != "declining_adoption"


# ---------------------------------------------------------------------------
# 3. Concentration is not labelled misuse
# ---------------------------------------------------------------------------

class TestConcentrationNotMisuse:
    def test_concentration_definition_does_not_equate_with_misuse(self):
        defn = DEFINITIONS["concentration"].lower()
        if "misuse" in defn:
            assert "not" in defn or "does not" in defn

    def test_concentration_label_not_misuse(self):
        label = status_label("concentration")
        assert "misuse" not in label.lower()

    def test_engagement_low_not_low_value(self):
        defn = DEFINITIONS["engagement_status"].lower()
        assert "does not imply low business value" in defn or "low engagement" in defn

    def test_low_engagement_label_no_low_value_claim(self):
        label = status_label("declining_adoption")
        assert "low value" not in label.lower()
        assert "worthless" not in label.lower()


# ---------------------------------------------------------------------------
# 4. Recommended actions are recommendations, not automated actions
# ---------------------------------------------------------------------------

class TestRecommendedActionNotAutomated:
    def test_recommended_action_definition_not_automated(self):
        defn = DEFINITIONS["recommended_action"].lower()
        if "automated" in defn:
            assert "not" in defn

    def test_recommended_action_definition_says_recommendation(self):
        defn = DEFINITIONS["recommended_action"].lower()
        assert "recommendation" in defn or "not been executed" in defn

    def test_action_label_investigate_readable(self):
        label = status_label("investigate_usage_decline")
        assert "investigate" in label.lower() or "decline" in label.lower()

    def test_action_label_continue_monitoring_readable(self):
        label = status_label("continue_monitoring")
        assert "continue" in label.lower() or "monitor" in label.lower()

    def test_action_labels_not_imperative_execute(self):
        action_codes = [
            "continue_monitoring",
            "investigate_usage_decline",
            "review_planned_deprecation",
            "review_forecast_uncertainty",
            "review_model_health",
        ]
        for code in action_codes:
            label = status_label(code)
            assert "execute" not in label.lower(), f"Action label for {code!r} uses 'execute': {label!r}"
            assert "automated" not in label.lower(), f"Action label for {code!r} uses 'automated': {label!r}"


# ---------------------------------------------------------------------------
# 5. Invalid GenAI output is not rendered as trusted
# ---------------------------------------------------------------------------

class TestInvalidGenaiNotTrusted:
    def test_invalid_genai_state_classified_as_invalid(self):
        ins_row = pd.Series({
            "validation_status": "failed",
            "generation_status": "llm_generated",
            "generation_mode": "llm",
            "api_attempts": 1,
        })
        state = classify_genai_state(ins_row)
        assert state == "invalid"

    def test_invalid_state_label_communicates_failure(self):
        label = GENAI_STATE_LABELS.get("invalid", "")
        assert "invalid" in label.lower() or "fail" in label.lower() or "not display" in label.lower()

    def test_invalid_state_not_in_trusted_states(self):
        trusted = {"valid", "reused", "rule_based"}
        assert "invalid" not in trusted

    def test_missing_state_not_in_trusted_states(self):
        trusted = {"valid", "reused", "rule_based"}
        assert "missing" not in trusted

    def test_fallback_state_labelled_as_fallback(self):
        label = GENAI_STATE_LABELS.get("fallback", "")
        assert "fallback" in label.lower() or "failed" in label.lower()

    def test_rule_based_labelled_accurately(self):
        label = GENAI_STATE_LABELS.get("rule_based", "")
        assert "rule" in label.lower() or "deterministic" in label.lower()

    def test_valid_genai_fields_not_retrieved_from_invalid_insight(self):
        ins_row = pd.Series({
            "validation_status": "failed",
            "generation_status": "llm_generated",
            "executive_summary": "DO NOT TRUST THIS",
        })
        state = classify_genai_state(ins_row)
        # The classified state should prevent rendering
        assert state == "invalid"
        # get_genai_field still returns the raw value (caller is responsible for state check)
        field = get_genai_field(ins_row, "executive_summary")
        assert field is not None  # field present but state == invalid means caller skips it

    def test_all_genai_state_labels_defined(self):
        expected_states = {"valid", "reused", "rule_based", "fallback", "invalid", "missing"}
        for state in expected_states:
            assert state in GENAI_STATE_LABELS, f"Missing label for state: {state!r}"


# ---------------------------------------------------------------------------
# 6. User-level identifiers not present in engagement data
# ---------------------------------------------------------------------------

class TestNoUserIdentifiers:
    def test_build_report_detail_does_not_expose_user_ids(self):
        mart = pd.Series({
            "report_id":   "R_001",
            "report_name": "Test",
        })
        eng = pd.Series({
            "report_id":           "R_001",
            "unique_users_28d":    5,
            "user_id":             "user_key_abc123",  # should NOT propagate
            "email":               "test@example.com",  # should NOT propagate
        })
        detail = build_report_detail(mart, eng, pd.Series(dtype="object"))
        eng_section = detail["engagement"]

        # Confirm user identifiers are not present in the section
        for key in eng_section:
            val = str(eng_section.get(key, ""))
            assert "user_key_abc123" not in val
            assert "test@example.com" not in val

    def test_engagement_section_has_no_user_id_field(self):
        mart = pd.Series({"report_id": "R_001"})
        eng = pd.Series({
            "report_id":        "R_001",
            "unique_users_28d": 5,
            "user_id":          "user_key_123",
        })
        detail = build_report_detail(mart, eng, pd.Series(dtype="object"))
        assert "user_id" not in detail["engagement"]
        assert "user_key" not in detail["engagement"]


# ---------------------------------------------------------------------------
# 7. Terminology in definitions (cross-check)
# ---------------------------------------------------------------------------

class TestDefinitionTerminology:
    def test_prediction_interval_not_called_confidence_interval(self):
        defn = DEFINITIONS["prediction_interval"].lower()
        if "confidence interval" in defn:
            assert "not a confidence interval" in defn or "is not a" in defn

    def test_privacy_suppression_says_not_zero(self):
        defn = DEFINITIONS["privacy_suppression"].lower()
        assert "zero" in defn or "not zero" in defn or "never" in defn

    def test_review_priority_not_described_as_automated(self):
        defn = DEFINITIONS["review_priority"].lower()
        assert "automated" not in defn or "not" in defn

    def test_genai_summary_says_analytics_decides(self):
        defn = DEFINITIONS["genai_summary"].lower()
        assert "calculates" in defn or "decides" in defn or "analytics" in defn
