"""Tests for the report segmentation layer."""
import pytest
import pandas as pd
from pathlib import Path
from src.analytics.report_segmentation import (
    build_report_segments, validate_report_segments,
    persist_report_segments, REPORT_SEGMENTS_COLS,
    ALLOWED_PRIMARY_SEGMENTS, PROHIBITED_SEGMENT_COLS,
)

RUN_ID = "test-seg-001"
AS_OF = "2024-03-31"


def _feat(**kw):
    base = {
        "analytics_run_id": RUN_ID, "analytics_as_of_date": AS_OF,
        "report_id": "R001", "report_name": "Sales Dashboard",
        "historical_usage_status": "stable_regular_usage",
        "adoption_maturity_status": "mature",
        "report_lifecycle_status": "established",
        "recent_28d_views": 50,
    }
    base.update(kw)
    return pd.DataFrame([base])


def _outlook(**kw):
    base = {
        "report_id": "R001",
        "forecast_outlook_status": "stable_outlook",
        "forecast_uncertainty_status": "low_uncertainty",
    }
    base.update(kw)
    return pd.DataFrame([base])


def _health(**kw):
    base = {
        "report_id": "R001",
        "model_diagnostic_status": "good",
    }
    base.update(kw)
    return pd.DataFrame([base])


def _engagement(**kw):
    base = {
        "report_id": "R001",
        "engagement_evidence_status": "complete",
        "privacy_suppression_status": "not_suppressed",
        "privacy_suppressed_field_count": 0,
        "overall_engagement_status": "healthy_broad_adoption",
        "active_user_direction_28d": "stable",
        "active_user_change_28d_pct": 0.02,
        "repeat_engagement_status": "strong_repeat_engagement",
        "lapse_rate_28d": 0.15,
        "frequency_direction": "stable",
        "dependency_status": "broadly_distributed_stable_dependency",
        "concentration_direction": "stable",
        "unique_users_28d": 25,
        "returning_user_share_28d": 0.60,
        "breadth_status": "broad_adoption",
    }
    base.update(kw)
    return pd.DataFrame([base])


def _metadata(**kw):
    base = {
        "report_id": "R001",
        "metadata_completeness_score": 1.0,
        "metadata_interpretation_status": "metadata_supported",
        "report_lifecycle_status": "established",
        "deprecation_status": None,
        "ownership_status": "known",
    }
    base.update(kw)
    return pd.DataFrame([base])


def _diagnostics(**kw):
    base = {
        "report_id": "R001",
        "primary_diagnostic": "none",
        "overall_diagnostic_severity": "none",
        "recommended_diagnostic_action": "continue_monitoring",
    }
    base.update(kw)
    return pd.DataFrame([base])


def _build(feat_kw=None, outlook_kw=None, health_kw=None, eng_kw=None,
           meta_kw=None, diag_kw=None):
    return build_report_segments(
        _feat(**(feat_kw or {})),
        _outlook(**(outlook_kw or {})),
        _health(**(health_kw or {})),
        _engagement(**(eng_kw or {})),
        _metadata(**(meta_kw or {})),
        _diagnostics(**(diag_kw or {})),
        RUN_ID,
    )


class TestSchema:
    def test_all_cols_present(self):
        df = _build()
        missing = [c for c in REPORT_SEGMENTS_COLS if c not in df.columns]
        assert not missing

    def test_no_prohibited_cols(self):
        df = _build()
        bad = set(df.columns) & PROHIBITED_SEGMENT_COLS
        assert not bad

    def test_no_deprecated_niche_col(self):
        df = _build()
        assert "niche" not in df.columns

    def test_unique_grain(self):
        df = _build()
        assert not df.duplicated(subset=["analytics_run_id", "report_id"]).any()

    def test_primary_in_allowed_set(self):
        df = _build()
        assert df["primary_report_segment"].iloc[0] in ALLOWED_PRIMARY_SEGMENTS


class TestHealthyBroadAdoption:
    def test_healthy_stable_broad(self):
        df = _build()
        assert df["primary_report_segment"].iloc[0] == "healthy_broad_adoption"

    def test_usage_segment_stable(self):
        df = _build()
        assert df["usage_segment"].iloc[0] == "stable_regular_usage"

    def test_engagement_segment_broad(self):
        df = _build()
        assert df["engagement_segment"].iloc[0] == "broad_healthy_engagement"


class TestHealthyNicheAdoption:
    def test_small_stable_userbase_niche(self):
        df = _build(eng_kw={
            "unique_users_28d": 4,
            "returning_user_share_28d": 0.50,
            "active_user_direction_28d": "stable",
            "active_user_change_28d_pct": 0.0,
            "lapse_rate_28d": 0.10,
            "repeat_engagement_status": "strong_repeat_engagement",
            "dependency_status": "broadly_distributed_stable_dependency",
            "privacy_suppression_status": "not_suppressed",
            "privacy_suppressed_field_count": 0,
        })
        assert df["engagement_segment"].iloc[0] == "niche_healthy_engagement"
        assert df["primary_report_segment"].iloc[0] == "healthy_niche_adoption"

    def test_high_concentration_alone_not_niche(self):
        # High concentration with many users should NOT be niche_healthy_engagement
        df = _build(eng_kw={
            "unique_users_28d": 50,
            "returning_user_share_28d": 0.10,
            "dependency_status": "high_dependency_single_user",
            "privacy_suppressed_field_count": 0,
            "privacy_suppression_status": "not_suppressed",
        })
        assert df["primary_report_segment"].iloc[0] != "healthy_niche_adoption"


class TestGrowingReport:
    def test_growing_usage_segment(self):
        df = _build(feat_kw={"historical_usage_status": "growing_usage"})
        assert df["primary_report_segment"].iloc[0] == "growing_report"

    def test_growth_expected_forecast(self):
        df = _build(outlook_kw={"forecast_outlook_status": "growth_expected"})
        primary = df["primary_report_segment"].iloc[0]
        assert primary in {"growing_report", "healthy_broad_adoption"}


class TestDecliningReport:
    def test_declining_usage_raises_segment(self):
        df = _build(feat_kw={"historical_usage_status": "declining_usage"})
        assert df["primary_report_segment"].iloc[0] == "declining_report"

    def test_declining_engagement_raises_segment(self):
        df = _build(eng_kw={
            "active_user_direction_28d": "declining",
            "active_user_change_28d_pct": -0.25,
            "lapse_rate_28d": 0.10,
            "repeat_engagement_status": "strong_repeat_engagement",
        })
        assert df["primary_report_segment"].iloc[0] == "declining_report"


class TestInactiveReport:
    def test_prolonged_inactivity_inactive(self):
        df = _build(feat_kw={"historical_usage_status": "prolonged_inactivity"})
        assert df["primary_report_segment"].iloc[0] == "inactive_report"


class TestElevatedLapse:
    def test_high_lapse_raises_segment(self):
        df = _build(eng_kw={
            "lapse_rate_28d": 0.65,
            "active_user_direction_28d": "stable",
        })
        assert df["engagement_segment"].iloc[0] == "elevated_lapse"
        assert df["primary_report_segment"].iloc[0] == "elevated_lapse"


class TestLowRepeatUsage:
    def test_low_repeat_status_raises_segment(self):
        df = _build(eng_kw={
            "repeat_engagement_status": "low_repeat",
            "lapse_rate_28d": 0.20,
            "active_user_direction_28d": "stable",
        })
        assert df["primary_report_segment"].iloc[0] == "low_repeat_usage"


class TestConcentratedDependency:
    def test_high_dependency_raises_segment(self):
        df = _build(eng_kw={
            "dependency_status": "high_dependency",
            "privacy_suppressed_field_count": 0,
            "privacy_suppression_status": "not_suppressed",
            "active_user_direction_28d": "stable",
            "lapse_rate_28d": 0.10,
            "repeat_engagement_status": "strong_repeat_engagement",
        })
        assert df["dependency_segment"].iloc[0] == "highly_concentrated"
        assert df["primary_report_segment"].iloc[0] == "concentrated_dependency"

    def test_suppressed_no_concentration_segment(self):
        df = _build(eng_kw={
            "dependency_status": "high_dependency",
            "privacy_suppression_status": "privacy_suppressed",
            "privacy_suppressed_field_count": 3,
        })
        assert df["dependency_segment"].iloc[0] == "privacy_limited"
        assert df["primary_report_segment"].iloc[0] != "concentrated_dependency"


class TestUncertainForecast:
    def test_uncertain_forecast_segment(self):
        df = _build(outlook_kw={"forecast_outlook_status": "uncertain_outlook"})
        assert df["primary_report_segment"].iloc[0] in {
            "uncertain_forecast", "model_review_needed",
        }


class TestModelReviewNeeded:
    def test_poor_model_with_poor_severity(self):
        df = _build(
            health_kw={"model_diagnostic_status": "poor"},
            diag_kw={"overall_diagnostic_severity": "poor", "primary_diagnostic": "severe_model_health_issue"},
        )
        assert df["model_health_segment"].iloc[0] == "poor_model"
        assert df["primary_report_segment"].iloc[0] == "model_review_needed"

    def test_model_review_does_not_imply_usage_decline(self):
        df = _build(
            feat_kw={"historical_usage_status": "stable_regular_usage"},
            health_kw={"model_diagnostic_status": "poor"},
            diag_kw={"overall_diagnostic_severity": "poor"},
        )
        assert df["usage_segment"].iloc[0] == "stable_regular_usage"


class TestNewlyLaunched:
    def test_newly_launched_lifecycle(self):
        df = _build(feat_kw={
            "historical_usage_status": "newly_active",
            "report_lifecycle_status": "newly_launched",
            "adoption_maturity_status": "newly_launched",
        })
        primary = df["primary_report_segment"].iloc[0]
        assert primary == "newly_launched"


class TestPlannedDeprecation:
    def test_deprecated_status_raises_segment(self):
        df = _build(meta_kw={
            "metadata_completeness_score": 0.8,
            "deprecation_status": "deprecated",
            "metadata_interpretation_status": "metadata_supported",
        })
        assert df["lifecycle_segment"].iloc[0] == "planned_deprecation"
        assert df["primary_report_segment"].iloc[0] == "planned_deprecation"


class TestMixedSignals:
    def test_intermittent_usage_no_clear_segment(self):
        df = _build(feat_kw={"historical_usage_status": "stable_intermittent_usage"})
        primary = df["primary_report_segment"].iloc[0]
        assert primary in {"mixed_signals", "healthy_broad_adoption", "healthy_niche_adoption"}


class TestInsufficientEvidence:
    def test_no_valid_data_insufficient(self):
        df = _build(feat_kw={"historical_usage_status": "no_valid_usage_data"})
        primary = df["primary_report_segment"].iloc[0]
        assert primary in {"data_quality_issue", "insufficient_evidence"}

    def test_missing_all_sources_insufficient(self):
        feat = pd.DataFrame([{
            "analytics_run_id": RUN_ID, "analytics_as_of_date": AS_OF,
            "report_id": "R001", "report_name": "Test",
            "historical_usage_status": "no_valid_usage_data",
            "adoption_maturity_status": "unknown",
            "report_lifecycle_status": "unknown",
        }])
        df = build_report_segments(feat, None, None, None, None, None, RUN_ID)
        assert len(df) == 1


class TestNoPrecedenceConflation:
    def test_prolonged_inactivity_beats_model_review(self):
        df = _build(
            feat_kw={"historical_usage_status": "prolonged_inactivity"},
            health_kw={"model_diagnostic_status": "poor"},
            diag_kw={"overall_diagnostic_severity": "poor"},
        )
        assert df["primary_report_segment"].iloc[0] == "inactive_report"

    def test_lapse_beats_concentration(self):
        df = _build(
            eng_kw={
                "lapse_rate_28d": 0.65,
                "dependency_status": "high_dependency",
                "privacy_suppressed_field_count": 0,
                "privacy_suppression_status": "not_suppressed",
                "active_user_direction_28d": "stable",
            },
        )
        assert df["primary_report_segment"].iloc[0] == "elevated_lapse"


class TestDeterministicReasons:
    def test_forecast_outlook_status_never_calculation_failed(self):
        # calculation_failed is a sentinel used by other fields (model_diagnostic_status,
        # bias_status, etc.) but is never produced by classify_forecast_outlook_status()
        # or the missing-forecast branch. FORECAST_INSUFFICIENT_STATUSES must not
        # imply it is a valid forecast_outlook_status value.
        from src.analytics.report_forecast_outlook import _ALLOWED_OUTLOOK_STATUSES
        assert "calculation_failed" not in _ALLOWED_OUTLOOK_STATUSES
        df = _build(outlook_kw={"forecast_outlook_status": "insufficient_evidence"})
        assert df["forecast_segment"].iloc[0] == "insufficient_evidence"
        df2 = _build(outlook_kw={"forecast_outlook_status": "invalid_forecast"})
        assert df2["forecast_segment"].iloc[0] == "insufficient_evidence"

    def test_reasons_are_deterministic(self):
        df1 = _build()
        df2 = _build()
        assert df1["segment_reasons"].iloc[0] == df2["segment_reasons"].iloc[0]

    def test_reasons_contain_all_segments(self):
        df = _build()
        reasons = df["segment_reasons"].iloc[0]
        for seg in ["usage:", "engagement:", "forecast:", "model:", "dependency:", "lifecycle:", "metadata:", "primary:"]:
            assert seg in reasons


class TestPersistence:
    def test_file_created(self, tmp_path):
        df = _build()
        path = persist_report_segments(df, tmp_path)
        assert path.exists()

    def test_schema_stable(self, tmp_path):
        df = _build()
        path = persist_report_segments(df, tmp_path)
        loaded = pd.read_csv(path)
        assert list(loaded.columns) == REPORT_SEGMENTS_COLS

    def test_sorted_by_report_id(self, tmp_path):
        feat = pd.concat([
            _feat(report_id="R003"), _feat(report_id="R001"), _feat(report_id="R002"),
        ])
        o = pd.concat([_outlook(report_id=f"R00{i}") for i in [3, 1, 2]])
        h = pd.concat([_health(report_id=f"R00{i}") for i in [3, 1, 2]])
        e = pd.concat([_engagement(report_id=f"R00{i}") for i in [3, 1, 2]])
        m = pd.concat([_metadata(report_id=f"R00{i}") for i in [3, 1, 2]])
        d = pd.concat([_diagnostics(report_id=f"R00{i}") for i in [3, 1, 2]])
        df = build_report_segments(feat, o, h, e, m, d, RUN_ID)
        path = persist_report_segments(df, tmp_path)
        loaded = pd.read_csv(path)
        assert list(loaded["report_id"]) == sorted(loaded["report_id"].tolist())
