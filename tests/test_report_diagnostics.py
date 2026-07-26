"""Tests for the report diagnostics layer."""
import re
import pytest
import pandas as pd
from pathlib import Path
from src.analytics.report_diagnostics import (
    build_report_diagnostics, validate_report_diagnostics,
    persist_report_diagnostics, REPORT_DIAGNOSTICS_COLS,
    PROHIBITED_DIAGNOSTIC_COLS, PROHIBITED_ACTIONS,
    ALLOWED_RECOMMENDED_ACTIONS, ALLOWED_PRIMARY_DIAGNOSTICS,
    PRIMARY_DIAGNOSTIC_TO_ACTION,
)

RUN_ID = "test-diag-001"
AS_OF = "2024-03-31"


# ---- Fixture helpers ----

def _feat(**kw):
    """Minimal report_features row — healthy defaults using actual column names."""
    base = {
        "analytics_run_id": RUN_ID,
        "analytics_as_of_date": AS_OF,
        "report_id": "R001",
        "report_name": "Sales Dashboard",
        "historical_usage_status": "stable_regular_usage",
        # Actual anomaly columns (no 'anomaly_detected' boolean in real data)
        "latest_usage_anomaly_status": "normal",
        "usage_anomaly_count_28d": 0,
        "adoption_maturity_status": "mature",
        "report_lifecycle_status": "established",
        "activation_date_status": "known",
        "recent_28d_views": 50,
        "previous_28d_views": 48,
        "returning_user_share_28d": 0.60,
    }
    base.update(kw)
    return pd.DataFrame([base])


def _outlook(**kw):
    base = {
        "report_id": "R001",
        "forecast_outlook_status": "stable_outlook",
        "forecast_uncertainty_status": "low_uncertainty",
        "forecast_direction_28d": "expected_stability",
        "forecast_evidence_status": "sufficient",
    }
    base.update(kw)
    return pd.DataFrame([base])


def _health(**kw):
    base = {
        "report_id": "R001",
        "model_diagnostic_status": "good",
        "model_evidence_status": "complete",
        "bias_status": "none",
        "residual_autocorrelation_status": "none",
        "interval_calibration_status": "good",
        "production_deterioration_status": "none",
    }
    base.update(kw)
    return pd.DataFrame([base])


def _engagement(**kw):
    base = {
        "report_id": "R001",
        "engagement_evidence_status": "complete",
        "privacy_suppression_status": "not_suppressed",
        "privacy_suppressed_field_count": 0,
        "active_user_direction_28d": "stable",
        "active_user_change_28d_pct": 0.02,
        "repeat_engagement_status": "healthy_repeat",
        "lapse_rate_28d": 0.15,
        "frequency_direction": "stable",
        "dependency_status": "distributed",
        "concentration_direction": "stable",
    }
    base.update(kw)
    return pd.DataFrame([base])


def _metadata(**kw):
    base = {
        "report_id": "R001",
        "metadata_completeness_score": 1.0,
        "ownership_status": "known",
        "report_owner_team": "Sales Ops",
        "expected_usage_cadence": "daily",
        "criticality_level": "high",
        "metadata_interpretation_status": "metadata_supported",
    }
    base.update(kw)
    return pd.DataFrame([base])


def _build(feat_kw=None, outlook_kw=None, health_kw=None, eng_kw=None, meta_kw=None,
           forecast_df=None, health_df=None, engagement_df=None, metadata_df=None):
    f = _feat(**(feat_kw or {}))
    o = forecast_df if forecast_df is not None else _outlook(**(outlook_kw or {}))
    h = health_df if health_df is not None else _health(**(health_kw or {}))
    e = engagement_df if engagement_df is not None else _engagement(**(eng_kw or {}))
    m = metadata_df if metadata_df is not None else _metadata(**(meta_kw or {}))
    return build_report_diagnostics(f, o, h, e, m, RUN_ID)


class TestSchema:
    def test_all_required_cols_present(self):
        df = _build()
        missing = [c for c in REPORT_DIAGNOSTICS_COLS if c not in df.columns]
        assert not missing, f"Missing columns: {missing}"

    def test_no_prohibited_cols(self):
        df = _build()
        bad = set(df.columns) & PROHIBITED_DIAGNOSTIC_COLS
        assert not bad, f"Prohibited columns present: {bad}"

    def test_no_deprecated_cols(self):
        df = _build()
        deprecated = {"repeat_rate", "latest_views", "prior_views", "top_user_concentration", "usage_change_pct"}
        assert not (set(df.columns) & deprecated)

    def test_unique_grain(self):
        df = _build()
        assert not df.duplicated(subset=["analytics_run_id", "report_id"]).any()

    def test_column_order_matches_schema(self):
        df = _build()
        assert list(df.columns) == REPORT_DIAGNOSTICS_COLS


class TestHealthyReport:
    def test_no_risks_raised(self):
        df = _build()
        for col in ["usage_decline_risk", "inactivity_risk", "volatility_risk", "anomaly_risk",
                    "forecast_decline_risk", "model_health_risk", "active_user_decline_risk",
                    "elevated_lapse_risk", "concentrated_dependency_risk"]:
            assert df[col].iloc[0] == False, f"{col} should be False for healthy report"

    def test_primary_diagnostic_none(self):
        assert _build()["primary_diagnostic"].iloc[0] == "none"

    def test_action_continue_monitoring(self):
        assert _build()["recommended_diagnostic_action"].iloc[0] == "continue_monitoring"

    def test_overall_severity_none(self):
        assert _build()["overall_diagnostic_severity"].iloc[0] == "none"

    def test_review_not_required(self):
        assert _build()["diagnostic_review_required"].iloc[0] == False


class TestUsageDeclineRisk:
    def test_decline_status_raises_risk(self):
        df = _build(feat_kw={"historical_usage_status": "declining_usage"})
        assert df["usage_decline_risk"].iloc[0] == True

    def test_primary_is_severe_decline(self):
        df = _build(feat_kw={"historical_usage_status": "declining_usage"})
        assert df["primary_diagnostic"].iloc[0] == "severe_historical_decline"

    def test_action_is_investigate_decline(self):
        df = _build(feat_kw={"historical_usage_status": "declining_usage"})
        assert df["recommended_diagnostic_action"].iloc[0] == "investigate_usage_decline"


class TestProlongedInactivity:
    def test_inactivity_raises_risk(self):
        df = _build(feat_kw={"historical_usage_status": "prolonged_inactivity"})
        assert df["inactivity_risk"].iloc[0] == True

    def test_primary_is_prolonged_inactivity(self):
        df = _build(feat_kw={"historical_usage_status": "prolonged_inactivity"})
        assert df["primary_diagnostic"].iloc[0] == "prolonged_inactivity"

    def test_action_is_review_inactivity(self):
        df = _build(feat_kw={"historical_usage_status": "prolonged_inactivity"})
        assert df["recommended_diagnostic_action"].iloc[0] == "review_inactivity"

    def test_severity_poor(self):
        df = _build(feat_kw={"historical_usage_status": "prolonged_inactivity"})
        assert df["historical_usage_risk_severity"].iloc[0] == "poor"


class TestForecastDecline:
    def test_forecast_decline_raises_risk(self):
        df = _build(outlook_kw={"forecast_outlook_status": "decline_expected"})
        assert df["forecast_decline_risk"].iloc[0] == True

    def test_action_for_forecast_decline_is_allowed(self):
        # forecast_decline_risk does not have its own primary diagnostic in the
        # precedence chain (only forecast_inactivity does). The action returned
        # must still be an allowed value — not a prohibited one.
        df = _build(outlook_kw={"forecast_outlook_status": "decline_expected"})
        assert df["recommended_diagnostic_action"].iloc[0] in ALLOWED_RECOMMENDED_ACTIONS
        assert df["recommended_diagnostic_action"].iloc[0] not in PROHIBITED_ACTIONS


class TestForecastUncertainty:
    def test_high_uncertainty_raises_risk(self):
        df = _build(outlook_kw={"forecast_uncertainty_status": "high_uncertainty"})
        assert df["forecast_uncertainty_risk"].iloc[0] == True

    def test_very_high_uncertainty_raises_risk(self):
        df = _build(outlook_kw={"forecast_uncertainty_status": "very_high_uncertainty"})
        assert df["forecast_uncertainty_risk"].iloc[0] == True

    def test_action_review_uncertainty(self):
        df = _build(
            outlook_kw={
                "forecast_uncertainty_status": "high_uncertainty",
                "forecast_outlook_status": "stable_outlook",
            },
        )
        assert df["recommended_diagnostic_action"].iloc[0] == "review_forecast_uncertainty"


class TestModelHealthRisk:
    def test_poor_model_raises_risk(self):
        df = _build(health_kw={"model_diagnostic_status": "poor"})
        assert df["model_health_risk"].iloc[0] == True

    def test_model_risk_does_not_imply_usage_decline(self):
        df = _build(
            feat_kw={"historical_usage_status": "stable_regular_usage"},
            health_kw={"model_diagnostic_status": "poor"},
        )
        assert df["usage_decline_risk"].iloc[0] == False

    def test_action_review_model_health(self):
        df = _build(health_kw={"model_diagnostic_status": "poor"})
        assert df["recommended_diagnostic_action"].iloc[0] == "review_model_health"

    def test_severity_poor(self):
        df = _build(health_kw={"model_diagnostic_status": "poor"})
        assert df["model_risk_severity"].iloc[0] == "poor"

    def test_insufficient_evidence_model_gated(self):
        # When model_diagnostic_status == 'insufficient_evidence', no model risk
        df = _build(health_kw={"model_diagnostic_status": "insufficient_evidence"})
        assert df["model_health_risk"].iloc[0] == False
        assert df["model_risk_severity"].iloc[0] == "insufficient_evidence"


class TestActiveUserDecline:
    def test_declining_direction_raises_risk(self):
        df = _build(eng_kw={
            "active_user_direction_28d": "declining",
            "active_user_change_28d_pct": -0.20,
        })
        assert df["active_user_decline_risk"].iloc[0] == True

    def test_stable_direction_no_risk(self):
        df = _build(eng_kw={"active_user_direction_28d": "stable"})
        assert df["active_user_decline_risk"].iloc[0] == False

    def test_decline_within_threshold_no_risk(self):
        # -5% is within -10% threshold, should not trigger
        df = _build(eng_kw={
            "active_user_direction_28d": "declining",
            "active_user_change_28d_pct": -0.05,
        })
        assert df["active_user_decline_risk"].iloc[0] == False


class TestLowRepeatEngagement:
    def test_low_repeat_status_raises_risk(self):
        df = _build(eng_kw={"repeat_engagement_status": "low_repeat"})
        assert df["low_repeat_engagement_risk"].iloc[0] == True

    def test_action_improve_repeat(self):
        df = _build(eng_kw={
            "repeat_engagement_status": "low_repeat",
            "active_user_direction_28d": "stable",
            "active_user_change_28d_pct": 0.0,
        })
        assert df["recommended_diagnostic_action"].iloc[0] == "improve_repeat_engagement"


class TestElevatedLapse:
    def test_high_lapse_rate_raises_risk(self):
        df = _build(eng_kw={"lapse_rate_28d": 0.55})
        assert df["elevated_lapse_risk"].iloc[0] == True

    def test_low_lapse_no_risk(self):
        df = _build(eng_kw={"lapse_rate_28d": 0.10})
        assert df["elevated_lapse_risk"].iloc[0] == False

    def test_action_investigate_lapse(self):
        df = _build(eng_kw={"lapse_rate_28d": 0.55})
        assert df["recommended_diagnostic_action"].iloc[0] == "investigate_user_lapse"


class TestDecliningFrequency:
    def test_declining_frequency_raises_risk(self):
        df = _build(eng_kw={"frequency_direction": "declining"})
        assert df["declining_frequency_risk"].iloc[0] == True

    def test_stable_frequency_no_risk(self):
        df = _build(eng_kw={"frequency_direction": "stable"})
        assert df["declining_frequency_risk"].iloc[0] == False


class TestConcentratedDependency:
    def test_high_dependency_raises_risk(self):
        df = _build(eng_kw={
            "dependency_status": "high_dependency",
            "privacy_suppressed_field_count": 0,
            "privacy_suppression_status": "not_suppressed",
        })
        assert df["concentrated_dependency_risk"].iloc[0] == True

    def test_action_review_dependency(self):
        df = _build(eng_kw={
            "dependency_status": "high_dependency",
            "privacy_suppressed_field_count": 0,
            "privacy_suppression_status": "not_suppressed",
        })
        assert df["recommended_diagnostic_action"].iloc[0] == "review_concentrated_dependency"


class TestPrivacySuppressedConcentration:
    def test_suppressed_metrics_no_dependency_risk(self):
        df = _build(eng_kw={
            "dependency_status": "high_dependency",
            "privacy_suppression_status": "privacy_suppressed",
            "privacy_suppressed_field_count": 3,
        })
        assert df["concentrated_dependency_risk"].iloc[0] == False

    def test_partial_suppression_no_dependency_risk(self):
        df = _build(eng_kw={
            "dependency_status": "high_dependency",
            "privacy_suppression_status": "partial_suppression",
            "privacy_suppressed_field_count": 1,
        })
        assert df["concentrated_dependency_risk"].iloc[0] == False

    def test_suppressed_passes_validation(self):
        df = _build(eng_kw={
            "dependency_status": "high_dependency",
            "privacy_suppression_status": "privacy_suppressed",
            "privacy_suppressed_field_count": 3,
        })
        validate_report_diagnostics(df)


class TestNewlyLaunchedReport:
    def test_immature_flag_set(self):
        df = _build(feat_kw={"adoption_maturity_status": "newly_launched"})
        assert df["immature_report_flag"].iloc[0] == True

    def test_action_monitor_new_report(self):
        # Need to ensure no higher-precedence flag fires
        df = _build(
            feat_kw={"adoption_maturity_status": "newly_launched"},
            meta_kw={"metadata_completeness_score": 1.0, "ownership_status": "known",
                      "report_owner_team": "Team A", "expected_usage_cadence": "daily",
                      "criticality_level": "high"},
        )
        assert df["recommended_diagnostic_action"].iloc[0] == "monitor_new_report"


class TestMissingMetadata:
    def test_low_completeness_raises_risk(self):
        df = _build(meta_kw={"metadata_completeness_score": 0.20})
        assert df["missing_metadata_risk"].iloc[0] == True

    def test_missing_metadata_does_not_imply_low_value(self):
        df = _build(meta_kw={"metadata_completeness_score": 0.20})
        assert df["primary_diagnostic_category"].iloc[0] in {"metadata", "lifecycle", "none"}
        assert df["recommended_diagnostic_action"].iloc[0] != "retire_report"

    def test_criticality_missing_no_low_value_inference(self):
        df = _build(meta_kw={
            "criticality_level": "unknown",
            "metadata_completeness_score": 1.0,
            "ownership_status": "known",
            "report_owner_team": "Team A",
            "expected_usage_cadence": "daily",
        })
        assert df["recommended_diagnostic_action"].iloc[0] not in PROHIBITED_ACTIONS


class TestNoValidData:
    def test_no_valid_usage_primary_is_no_valid_data(self):
        df = _build(feat_kw={"historical_usage_status": "no_valid_usage_data"})
        assert df["primary_diagnostic"].iloc[0] == "no_valid_data"

    def test_no_valid_data_action(self):
        df = _build(feat_kw={"historical_usage_status": "no_valid_usage_data"})
        assert df["recommended_diagnostic_action"].iloc[0] == "investigate_data_quality"


class TestInsufficientEvidence:
    def test_no_context_sources_returns_row(self):
        feat = _feat()
        df = build_report_diagnostics(feat, None, None, None, None, RUN_ID)
        assert len(df) == 1

    def test_missing_sources_recorded(self):
        feat = _feat()
        df = build_report_diagnostics(feat, None, None, None, None, RUN_ID)
        missing = df["missing_diagnostic_evidence"].iloc[0]
        assert missing and "forecast_outlook" in missing

    def test_evidence_status_reflects_gap(self):
        feat = _feat()
        df = build_report_diagnostics(feat, None, None, None, None, RUN_ID)
        assert df["diagnostic_evidence_status"].iloc[0] not in {"complete", "mostly_complete"}


class TestConflictingRisksPrecedence:
    def test_prolonged_inactivity_beats_model_health(self):
        df = _build(
            feat_kw={"historical_usage_status": "prolonged_inactivity"},
            health_kw={"model_diagnostic_status": "poor"},
        )
        assert df["primary_diagnostic"].iloc[0] == "prolonged_inactivity"

    def test_lapse_beats_concentrated_dependency(self):
        df = _build(
            eng_kw={
                "lapse_rate_28d": 0.55,
                "dependency_status": "high_dependency",
                "privacy_suppressed_field_count": 0,
                "privacy_suppression_status": "not_suppressed",
                "active_user_direction_28d": "stable",
                "active_user_change_28d_pct": 0.0,
            }
        )
        primary = df["primary_diagnostic"].iloc[0]
        assert primary == "elevated_lapse"

    def test_no_valid_data_beats_all(self):
        df = _build(
            feat_kw={"historical_usage_status": "no_valid_usage_data"},
            health_kw={"model_diagnostic_status": "poor"},
            eng_kw={"lapse_rate_28d": 0.75},
        )
        assert df["primary_diagnostic"].iloc[0] == "no_valid_data"


class TestProhibitedActions:
    def test_no_retire_report(self):
        for kw in [
            {"historical_usage_status": "prolonged_inactivity"},
            {"historical_usage_status": "no_valid_usage_data"},
        ]:
            df = _build(feat_kw=kw)
            assert "retire_report" not in df["recommended_diagnostic_action"].values

    def test_no_delete_report(self):
        df = _build(feat_kw={"historical_usage_status": "prolonged_inactivity"})
        assert "delete_report" not in df["recommended_diagnostic_action"].values


class TestDeterministicOutput:
    def test_reasons_are_deterministic(self):
        df1 = _build()
        df2 = _build()
        assert df1["diagnostic_reasons"].iloc[0] == df2["diagnostic_reasons"].iloc[0]

    def test_action_matches_primary_diagnostic_mapping(self):
        df = _build()
        primary = df["primary_diagnostic"].iloc[0]
        action = df["recommended_diagnostic_action"].iloc[0]
        expected = PRIMARY_DIAGNOSTIC_TO_ACTION.get(primary, "continue_monitoring")
        assert action == expected


class TestPersistence:
    def test_file_created(self, tmp_path):
        df = _build()
        path = persist_report_diagnostics(df, tmp_path)
        assert path.exists()

    def test_schema_stable_after_roundtrip(self, tmp_path):
        df = _build()
        path = persist_report_diagnostics(df, tmp_path)
        loaded = pd.read_csv(path)
        assert list(loaded.columns) == REPORT_DIAGNOSTICS_COLS

    def test_sorted_by_report_id(self, tmp_path):
        ids = ["R003", "R001", "R002"]
        feat = pd.concat([_feat(report_id=rid) for rid in ids])
        o = pd.concat([_outlook(report_id=rid) for rid in ids])
        h = pd.concat([_health(report_id=rid) for rid in ids])
        e = pd.concat([_engagement(report_id=rid) for rid in ids])
        m = pd.concat([_metadata(report_id=rid) for rid in ids])
        df = build_report_diagnostics(feat, o, h, e, m, RUN_ID)
        path = persist_report_diagnostics(df, tmp_path)
        loaded = pd.read_csv(path)
        assert list(loaded["report_id"]) == sorted(loaded["report_id"].tolist())


class TestAnomalyDetection:
    def test_anomaly_count_raises_risk(self):
        df = _build(feat_kw={"usage_anomaly_count_28d": 2, "latest_usage_anomaly_status": "spike"})
        assert df["anomaly_risk"].iloc[0] == True

    def test_normal_anomaly_status_no_risk(self):
        df = _build(feat_kw={"usage_anomaly_count_28d": 0, "latest_usage_anomaly_status": "normal"})
        assert df["anomaly_risk"].iloc[0] == False

    def test_anomaly_status_non_normal_raises_risk(self):
        df = _build(feat_kw={"latest_usage_anomaly_status": "spike", "usage_anomaly_count_28d": 0})
        assert df["anomaly_risk"].iloc[0] == True
