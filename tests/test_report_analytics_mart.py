"""Tests for the canonical report analytics mart."""
import pytest
import pandas as pd
from pathlib import Path
from src.analytics.report_analytics_mart import (
    build_mart_report_analytics, validate_mart_report_analytics,
    persist_mart_report_analytics, PROHIBITED_MART_COLS,
    PROHIBITED_MART_ACTIONS, ALLOWED_OVERALL_STATUS,
    ALLOWED_MART_ACTIONS,
)

RUN_ID = "test-mart-001"
AS_OF = "2024-03-31"


def _feat(**kw):
    base = {
        "analytics_run_id": RUN_ID, "analytics_as_of_date": AS_OF,
        "report_id": "R001", "report_name": "Test", "workspace_id": "WS001",
        "historical_usage_status": "stable_regular_usage",
        "recent_28d_views": 50, "previous_28d_views": 48,
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
    base = {"report_id": "R001", "model_diagnostic_status": "good"}
    base.update(kw)
    return pd.DataFrame([base])


def _engagement(**kw):
    base = {
        "report_id": "R001",
        "engagement_evidence_status": "complete",
        "privacy_suppression_status": "not_suppressed",
        "privacy_suppressed_field_count": 0,
        "overall_engagement_status": "healthy_broad_adoption",
    }
    base.update(kw)
    return pd.DataFrame([base])


def _metadata(**kw):
    base = {
        "report_id": "R001",
        "metadata_completeness_score": 1.0,
        "metadata_interpretation_status": "metadata_supported",
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


def _segments(**kw):
    base = {
        "report_id": "R001",
        "primary_report_segment": "healthy_broad_adoption",
        "segment_evidence_status": "complete",
    }
    base.update(kw)
    return pd.DataFrame([base])


def _build(**kw):
    return build_mart_report_analytics(
        kw.get("features_df", _feat()),
        kw.get("forecast_df", _outlook()),
        kw.get("model_health_df", _health()),
        kw.get("engagement_df", _engagement()),
        kw.get("metadata_df", _metadata()),
        kw.get("diagnostics_df", _diagnostics()),
        kw.get("segments_df", _segments()),
        RUN_ID,
    )


class TestMartSchema:
    def test_no_prohibited_cols(self):
        df = _build()
        bad = set(df.columns) & PROHIBITED_MART_COLS
        assert not bad

    def test_summary_cols_present(self):
        df = _build()
        for col in [
            "overall_report_status", "primary_report_issue", "overall_evidence_status",
            "overall_review_priority", "recommended_report_action",
            "report_reasons", "report_review_required",
        ]:
            assert col in df.columns

    def test_schema_version_present(self):
        df = _build()
        assert "schema_version" in df.columns

    def test_unique_report_id(self):
        df = _build()
        assert not df.duplicated(subset=["report_id"]).any()


class TestAllSourcesPresent:
    def test_healthy_overall_status(self):
        df = _build()
        assert df["overall_report_status"].iloc[0] in ALLOWED_OVERALL_STATUS

    def test_action_continue_monitoring(self):
        df = _build()
        assert df["recommended_report_action"].iloc[0] == "continue_monitoring"

    def test_review_priority_low(self):
        df = _build()
        assert df["overall_review_priority"].iloc[0] == "low"


class TestOneSourceMissing:
    def test_missing_forecast_still_produces_row(self):
        df = _build(forecast_df=None)
        assert len(df) == 1

    def test_missing_engagement_still_produces_row(self):
        df = _build(engagement_df=None)
        assert len(df) == 1

    def test_evidence_partial_when_missing_source(self):
        df = _build(forecast_df=None, model_health_df=None)
        assert df["overall_evidence_status"].iloc[0] in {"partial", "insufficient"}


class TestDuplicateSourceRejected:
    def test_duplicate_report_id_in_source_raises(self):
        bad_forecast = pd.concat([_outlook(), _outlook()])
        with pytest.raises(ValueError):
            _build(forecast_df=bad_forecast)


class TestSpinePreservation:
    def test_all_spine_reports_in_mart(self):
        feat = pd.concat([_feat(report_id="R001"), _feat(report_id="R002")])
        o = pd.concat([_outlook(report_id="R001"), _outlook(report_id="R002")])
        h = pd.concat([_health(report_id="R001"), _health(report_id="R002")])
        e = pd.concat([_engagement(report_id="R001"), _engagement(report_id="R002")])
        m = pd.concat([_metadata(report_id="R001"), _metadata(report_id="R002")])
        d = pd.concat([_diagnostics(report_id="R001"), _diagnostics(report_id="R002")])
        s = pd.concat([_segments(report_id="R001"), _segments(report_id="R002")])
        df = build_mart_report_analytics(feat, o, h, e, m, d, s, RUN_ID)
        assert set(df["report_id"]) == {"R001", "R002"}


class TestStatusPrecedence:
    def test_inactive_report_status(self):
        df = _build(
            features_df=_feat(historical_usage_status="prolonged_inactivity"),
            diagnostics_df=_diagnostics(primary_diagnostic="prolonged_inactivity", overall_diagnostic_severity="poor"),
            segments_df=_segments(primary_report_segment="inactive_report"),
        )
        assert df["overall_report_status"].iloc[0] == "inactive"

    def test_declining_status(self):
        df = _build(
            features_df=_feat(historical_usage_status="declining_usage"),
            diagnostics_df=_diagnostics(primary_diagnostic="severe_historical_decline", overall_diagnostic_severity="poor"),
            segments_df=_segments(primary_report_segment="declining_report"),
        )
        assert df["overall_report_status"].iloc[0] == "declining"

    def test_growing_status(self):
        df = _build(
            features_df=_feat(historical_usage_status="growing_usage"),
            segments_df=_segments(primary_report_segment="growing_report"),
        )
        assert df["overall_report_status"].iloc[0] == "growing"

    def test_model_limited_status(self):
        df = _build(
            model_health_df=_health(model_diagnostic_status="poor"),
            diagnostics_df=_diagnostics(primary_diagnostic="severe_model_health_issue", overall_diagnostic_severity="poor"),
            segments_df=_segments(primary_report_segment="model_review_needed"),
        )
        assert df["overall_report_status"].iloc[0] == "model_limited"


class TestPrivacySuppression:
    def test_suppressed_privacy_status_preserved(self):
        df = _build(engagement_df=_engagement(
            privacy_suppression_status="privacy_suppressed",
            privacy_suppressed_field_count=3,
        ))
        evidence = df["overall_evidence_status"].iloc[0]
        assert "privacy" in evidence or evidence == "complete_with_privacy_limits"

    def test_no_user_identifiers_in_mart(self):
        df = _build()
        bad = set(df.columns) & PROHIBITED_MART_COLS
        assert not bad


class TestProhibitedActions:
    def test_no_retire_action(self):
        for feat_kw in [
            {"historical_usage_status": "prolonged_inactivity"},
            {"historical_usage_status": "no_valid_usage_data"},
        ]:
            df = _build(features_df=_feat(**feat_kw))
            assert "retire_report" not in df["recommended_report_action"].values

    def test_no_delete_action(self):
        df = _build(features_df=_feat(historical_usage_status="prolonged_inactivity"))
        assert "delete_report" not in df["recommended_report_action"].values

    def test_no_retrain_action(self):
        df = _build(model_health_df=_health(model_diagnostic_status="poor"))
        assert "automatically_retrain" not in df["recommended_report_action"].values

    def test_all_actions_allowed(self):
        df = _build()
        action = df["recommended_report_action"].iloc[0]
        assert action in ALLOWED_MART_ACTIONS


class TestNoDeprecatedFields:
    def test_no_repeat_rate_col(self):
        df = _build()
        assert "repeat_rate" not in df.columns

    def test_no_latest_views_col(self):
        df = _build()
        assert "latest_views" not in df.columns

    def test_no_top_user_concentration_col(self):
        df = _build()
        assert "top_user_concentration" not in df.columns


class TestPersistence:
    def test_file_created(self, tmp_path):
        df = _build()
        path = persist_mart_report_analytics(df, tmp_path)
        assert path.exists()

    def test_sorted_by_report_id(self, tmp_path):
        feat = pd.concat([_feat(report_id="R003"), _feat(report_id="R001"), _feat(report_id="R002")])
        o = pd.concat([_outlook(report_id=f"R00{i}") for i in [3, 1, 2]])
        h = pd.concat([_health(report_id=f"R00{i}") for i in [3, 1, 2]])
        e = pd.concat([_engagement(report_id=f"R00{i}") for i in [3, 1, 2]])
        m = pd.concat([_metadata(report_id=f"R00{i}") for i in [3, 1, 2]])
        d = pd.concat([_diagnostics(report_id=f"R00{i}") for i in [3, 1, 2]])
        s = pd.concat([_segments(report_id=f"R00{i}") for i in [3, 1, 2]])
        df = build_mart_report_analytics(feat, o, h, e, m, d, s, RUN_ID)
        path = persist_mart_report_analytics(df, tmp_path)
        loaded = pd.read_csv(path)
        assert list(loaded["report_id"]) == sorted(loaded["report_id"].tolist())

    def test_no_deprecated_cols_after_roundtrip(self, tmp_path):
        df = _build()
        path = persist_mart_report_analytics(df, tmp_path)
        loaded = pd.read_csv(path)
        for col in ["repeat_rate", "latest_views", "prior_views", "top_user_concentration"]:
            assert col not in loaded.columns
