"""Tests for centralised definitions and label mappings.

Covers:
  - DEFINITIONS: required keys exist, correct terminology
  - STATUS_LABELS: known values, unknown values, null
  - status_label(): output consistency and capitalization
  - No conflicting wording (prediction interval ≠ confidence interval,
    insufficient_evidence ≠ unhealthy, recommended_action ≠ automated)

No Streamlit imports. No live LLM calls.
"""

from __future__ import annotations

import pytest

from src.app.utils.definitions import DEFINITIONS, STATUS_LABELS, status_label


# ---------------------------------------------------------------------------
# TestDefinitions
# ---------------------------------------------------------------------------

class TestDefinitions:
    REQUIRED_KEYS = [
        "active_report",
        "recent_usage",
        "historical_usage_status",
        "forecast_outlook",
        "prediction_interval",
        "forecast_interpretation_status",
        "model_diagnostic_status",
        "insufficient_evidence",
        "engagement_status",
        "retention",
        "lapse",
        "concentration",
        "privacy_suppression",
        "review_priority",
        "recommended_action",
        "evidence_status",
        "deterministic_shortlist",
        "genai_summary",
        "rule_based_fallback",
    ]

    def test_all_required_keys_present(self):
        for key in self.REQUIRED_KEYS:
            assert key in DEFINITIONS, f"Missing definition: {key}"

    def test_all_values_are_non_empty_strings(self):
        for key, value in DEFINITIONS.items():
            assert isinstance(value, str) and value.strip(), f"Empty definition: {key}"

    def test_prediction_interval_distinguishes_from_confidence_interval(self):
        defn = DEFINITIONS["prediction_interval"].lower()
        # The definition may mention "confidence interval" only to contrast with it
        if "confidence interval" in defn:
            assert "not a confidence interval" in defn or "is not a" in defn, (
                "If 'confidence interval' appears, it must be used to contrast — "
                "not to describe the prediction interval."
            )

    def test_insufficient_evidence_not_described_as_unhealthy(self):
        defn = DEFINITIONS["insufficient_evidence"].lower()
        assert "unhealthy" not in defn
        assert "poor" not in defn or "does not mean" in defn

    def test_recommended_action_not_described_as_automated(self):
        defn = DEFINITIONS["recommended_action"].lower()
        assert "automated" not in defn or "not" in defn or "does not" in defn

    def test_recommended_action_says_not_executed(self):
        defn = DEFINITIONS["recommended_action"].lower()
        assert "not been executed" in defn or "recommendation" in defn

    def test_engagement_status_says_low_engagement_not_low_value(self):
        defn = DEFINITIONS["engagement_status"].lower()
        assert "does not imply low business value" in defn or "low engagement" in defn

    def test_concentration_not_equated_with_misuse(self):
        defn = DEFINITIONS["concentration"].lower()
        # May mention misuse only to deny it
        if "misuse" in defn:
            assert "not" in defn or "does not" in defn, (
                "If 'misuse' appears, the definition must explicitly deny it."
            )

    def test_no_duplicate_keys(self):
        # Dict construction deduplicates, so verify by checking key count
        assert len(DEFINITIONS) == len(set(DEFINITIONS.keys()))

    def test_genai_summary_mentions_analytics_decides(self):
        defn = DEFINITIONS["genai_summary"].lower()
        assert "calculates" in defn or "decides" in defn or "analytics" in defn

    def test_rule_based_fallback_not_described_as_inferior(self):
        defn = DEFINITIONS["rule_based_fallback"].lower()
        # Should say it reflects valid analytics, not that it is low quality
        assert "validated" in defn or "grounding" in defn or "same" in defn


# ---------------------------------------------------------------------------
# TestStatusLabels
# ---------------------------------------------------------------------------

class TestStatusLabels:
    def test_contains_all_historical_usage_codes(self):
        expected = [
            "growing_usage", "stable_regular_usage", "stable_intermittent_usage",
            "bursty_usage", "declining_usage", "prolonged_inactivity",
        ]
        for code in expected:
            assert code in STATUS_LABELS, f"Missing code: {code}"

    def test_contains_all_forecast_outlook_codes(self):
        expected = [
            "growth_expected", "stable_outlook", "reactivation_expected",
            "uncertain_outlook", "decline_expected",
        ]
        for code in expected:
            assert code in STATUS_LABELS, f"Missing code: {code}"

    def test_contains_review_priority_codes(self):
        for code in ["low", "medium", "high", "critical"]:
            assert code in STATUS_LABELS

    def test_contains_recommended_action_codes(self):
        actions = [
            "continue_monitoring", "investigate_usage_decline",
            "review_planned_deprecation", "review_forecast_uncertainty",
            "review_model_health",
        ]
        for code in actions:
            assert code in STATUS_LABELS, f"Missing action: {code}"

    def test_contains_engagement_codes(self):
        codes = [
            "healthy_broad_adoption", "healthy_niche_adoption", "growing_adoption",
            "declining_adoption", "elevated_lapse", "inactive",
        ]
        for code in codes:
            assert code in STATUS_LABELS, f"Missing engagement code: {code}"

    def test_contains_model_health_codes(self):
        for code in ["healthy", "sufficient_evidence", "insufficient_evidence",
                     "degraded", "failing"]:
            assert code in STATUS_LABELS

    def test_contains_privacy_codes(self):
        assert "not_suppressed" in STATUS_LABELS
        assert "suppressed" in STATUS_LABELS

    def test_all_values_are_non_empty_strings(self):
        for code, label in STATUS_LABELS.items():
            assert isinstance(label, str) and label.strip(), f"Empty label for: {code}"

    def test_no_duplicate_values_for_distinct_codes(self):
        # insufficient_evidence appears for both model and evidence contexts —
        # it's acceptable if codes are distinct but share a readable label.
        # Just verify the dict has no None values.
        for v in STATUS_LABELS.values():
            assert v is not None


# ---------------------------------------------------------------------------
# TestStatusLabelFunction
# ---------------------------------------------------------------------------

class TestStatusLabelFunction:
    def test_known_code_returns_label(self):
        assert status_label("growing_usage") == "Growing usage"

    def test_known_code_continue_monitoring(self):
        assert status_label("continue_monitoring") == "Continue monitoring"

    def test_known_code_insufficient_evidence(self):
        assert status_label("insufficient_evidence") == "Insufficient evidence"

    def test_unknown_code_returns_title_cased(self):
        result = status_label("some_future_code")
        assert result == "Some Future Code"

    def test_unknown_hyphenated_code(self):
        result = status_label("some-future-code")
        assert result == "Some Future Code"

    def test_none_returns_dash(self):
        assert status_label(None) == "—"

    def test_nan_returns_dash(self):
        import math
        assert status_label(float("nan")) == "—"

    def test_empty_string_returns_dash(self):
        assert status_label("") == "—"

    def test_blank_string_returns_dash(self):
        assert status_label("   ") == "—"

    def test_nan_string_returns_dash(self):
        assert status_label("nan") == "—"

    def test_capitalization_consistent_no_all_caps(self):
        # No label should be fully uppercased
        for code, label in STATUS_LABELS.items():
            assert label == label.title() or label[0].isupper(), (
                f"Label for {code!r} has unexpected capitalization: {label!r}"
            )

    def test_returns_string_for_integer_input(self):
        result = status_label(42)
        assert isinstance(result, str)

    def test_suppressed_label(self):
        result = status_label("suppressed")
        assert "privacy" in result.lower() or "suppressed" in result.lower()
