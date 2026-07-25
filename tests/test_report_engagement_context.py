"""Tests for the report engagement context layer."""
import os
import time
import pytest
import pandas as pd
from pathlib import Path
from src.analytics.report_engagement_context import (
    build_report_engagement_context, validate_report_engagement_context,
    persist_report_engagement_context, ENGAGEMENT_CONTEXT_COLS,
    ENGAGEMENT_CONTEXT_SCHEMA_VERSION, PROHIBITED_COLUMNS,
)

AS_OF = "2024-03-31"
RUN_ID = "test-ec-001"


def _mart_row(**overrides):
    """Default healthy engagement mart row."""
    d = {
        "analytics_run_id": RUN_ID,
        "report_id": "R001",
        "report_name": "Report R001",
        "workspace_id": "WS001",
        "analytics_as_of_date": AS_OF,
        "generated_at": "2024-04-01T00:00:00",
        # Evidence
        "engagement_evidence_status": "complete",
        "user_data_quality_status": "good",
        "missing_engagement_evidence": None,
        "privacy_suppression_status": "not_suppressed",
        "privacy_suppressed_field_count": 0,
        "privacy_suppressed_fields": None,
        # Breadth
        "unique_users_7d": 15,
        "unique_users_28d": 25,
        "unique_users_previous_28d": 22,
        "active_user_change_28d": 3,
        "active_user_change_28d_pct": 0.136,
        "active_user_direction_28d": "growing",
        "breadth_status": "growing",
        # Repeat
        "returning_user_share_28d": 0.60,
        "one_time_user_share_28d": 0.40,
        "returning_user_share_change_28d": 0.05,
        "median_active_days_per_user_28d": 3.0,
        "repeat_engagement_status": "strong",
        # Cohorts
        "newly_adopted_users_28d": 5,
        "retained_users_28d": 18,
        "reactivated_users_28d": 2,
        "lapsed_users_28d": 4,
        "retained_user_rate_28d": 0.818,
        "lapse_rate_28d": 0.182,
        "adoption_transition_status": "stable",
        # Frequency
        "views_per_active_user_28d": 8.4,
        "views_per_user_day_28d": 2.8,
        "median_views_per_user_28d": 6.0,
        "median_return_gap_days_28d": 4.0,
        "frequency_direction": "stable",
        "usage_pattern_status": "normal",
        # Concentration
        "top_1_user_view_share_28d": 0.12,
        "top_3_users_view_share_28d": 0.30,
        "user_view_hhi_28d": 0.05,
        "effective_user_count_28d": 20.0,
        "effective_user_share_28d": 0.80,
        "concentration_direction": "stable",
        "dependency_status": "low",
        # Summary
        "overall_engagement_status": "healthy_broad_adoption",
        "primary_engagement_issue": "none",
        "engagement_issue_count": 0,
        "recommended_engagement_action": "continue_monitoring",
        "engagement_action_priority": "low",
        "engagement_reasons": "Report has broad adoption with good repeat engagement.",
        "review_required": False,
    }
    d.update(overrides)
    return pd.DataFrame([d])


def _features(as_of=AS_OF):
    return pd.DataFrame([{"report_id": "R001", "analytics_as_of_date": as_of}])


def _build(mart_df, features_df=None, run_id=RUN_ID):
    return build_report_engagement_context(mart_df, features_df, run_id)


class TestEngagementContextSchema:
    def test_all_required_cols_present(self):
        df = _build(_mart_row(), _features())
        missing = [c for c in ENGAGEMENT_CONTEXT_COLS if c not in df.columns]
        assert not missing, f"Missing cols: {missing}"

    def test_no_prohibited_columns(self):
        df = _build(_mart_row(), _features())
        present = set(df.columns) & PROHIBITED_COLUMNS
        assert not present, f"Prohibited columns: {present}"

    def test_repeat_rate_absent(self):
        df = _build(_mart_row(), _features())
        assert "repeat_rate" not in df.columns

    def test_unique_grain(self):
        df = _build(pd.concat([_mart_row(report_id="R001"), _mart_row(report_id="R002")]), _features())
        assert not df.duplicated(subset=["report_id"]).any()


class TestTemporalAlignment:
    def test_aligned_dates(self):
        df = _build(_mart_row(), _features(as_of=AS_OF))
        assert df["temporal_alignment_status"].iloc[0] == "aligned"
        assert df["engagement_interpretation_status"].iloc[0] != "invalid_temporal_alignment"

    def test_mismatched_dates(self):
        df = _build(_mart_row(analytics_as_of_date="2024-03-15"), _features(as_of=AS_OF))
        assert df["temporal_alignment_status"].iloc[0] == "mismatched"
        assert df["engagement_interpretation_status"].iloc[0] == "invalid_temporal_alignment"

    def test_missing_features(self):
        df = _build(_mart_row(), features_df=None)
        assert df["temporal_alignment_status"].iloc[0] in ("missing_features", "aligned")

    def test_mismatch_recorded_in_reasons(self):
        df = _build(_mart_row(analytics_as_of_date="2024-02-01"), _features())
        reasons = df["temporal_alignment_reasons"].iloc[0]
        assert reasons is not None


class TestInterpretationStatus:
    def test_healthy_supported(self):
        df = _build(_mart_row(), _features())
        assert df["engagement_interpretation_status"].iloc[0] == "engagement_supported"

    def test_privacy_limited(self):
        df = _build(_mart_row(privacy_suppressed_field_count=3), _features())
        assert df["engagement_interpretation_status"].iloc[0] == "engagement_supported_with_privacy_limits"

    def test_insufficient_history(self):
        df = _build(_mart_row(
            engagement_evidence_status="insufficient_history",
            overall_engagement_status="insufficient_evidence",
        ), _features())
        assert df["engagement_interpretation_status"].iloc[0] == "insufficient_engagement_evidence"

    def test_no_valid_user_data(self):
        df = _build(_mart_row(
            user_data_quality_status="no_valid_user_data",
            overall_engagement_status="no_valid_user_data",
        ), _features())
        assert df["engagement_interpretation_status"].iloc[0] == "no_valid_user_data"

    def test_partial_evidence(self):
        df = _build(_mart_row(engagement_evidence_status="recent_window_only"), _features())
        assert df["engagement_interpretation_status"].iloc[0] == "partial_engagement_evidence"

    def test_temporal_mismatch_overrides_all(self):
        # Even with good engagement, mismatch overrides
        df = _build(_mart_row(analytics_as_of_date="2020-01-01"), _features())
        assert df["engagement_interpretation_status"].iloc[0] == "invalid_temporal_alignment"


class TestStatusScenarios:
    def test_healthy_broad_adoption(self):
        df = _build(_mart_row(overall_engagement_status="healthy_broad_adoption"), _features())
        assert df["overall_engagement_status"].iloc[0] == "healthy_broad_adoption"

    def test_declining_adoption(self):
        df = _build(_mart_row(
            overall_engagement_status="declining_adoption",
            active_user_direction_28d="declining",
            primary_engagement_issue="active_user_decline",
            recommended_engagement_action="investigate_user_decline",
        ), _features())
        assert df["recommended_engagement_action"].iloc[0] == "investigate_user_decline"

    def test_privacy_suppressed_row(self):
        df = _build(_mart_row(
            overall_engagement_status="privacy_limited",
            privacy_suppressed_field_count=8,
            top_1_user_view_share_28d=None,
            user_view_hhi_28d=None,
        ), _features())
        assert pd.isna(df["top_1_user_view_share_28d"].iloc[0])
        assert pd.isna(df["user_view_hhi_28d"].iloc[0])

    def test_no_valid_user_data_not_inactive(self):
        df = _build(_mart_row(
            overall_engagement_status="no_valid_user_data",
            user_data_quality_status="no_valid_user_data",
        ), _features())
        assert df["overall_engagement_status"].iloc[0] != "inactive"

    def test_concentrated_dependency_unsuppressed(self):
        df = _build(_mart_row(
            overall_engagement_status="concentrated_dependency",
            top_1_user_view_share_28d=0.75,
            privacy_suppressed_field_count=0,
        ), _features())
        assert df["top_1_user_view_share_28d"].iloc[0] == 0.75

    def test_low_repeat_engagement(self):
        df = _build(_mart_row(
            overall_engagement_status="low_repeat_usage",
            returning_user_share_28d=0.10,
            primary_engagement_issue="low_repeat_engagement",
            recommended_engagement_action="improve_repeat_engagement",
        ), _features())
        assert df["recommended_engagement_action"].iloc[0] == "improve_repeat_engagement"


class TestPrivacyPreservation:
    def test_suppressed_values_remain_null(self):
        df = _build(_mart_row(
            privacy_suppressed_field_count=5,
            top_1_user_view_share_28d=None,
            user_view_hhi_28d=None,
        ), _features())
        assert pd.isna(df["user_view_hhi_28d"].iloc[0])

    def test_no_user_keys(self):
        df = _build(_mart_row(), _features())
        assert "user_key" not in df.columns

    def test_no_direct_identifiers(self):
        df = _build(_mart_row(), _features())
        bad = set(df.columns) & {"user_id", "email", "email_address", "display_name"}
        assert not bad


class TestValidation:
    def test_valid_output_passes(self):
        df = _build(_mart_row(), _features())
        validate_report_engagement_context(df)  # should not raise

    def test_prohibited_action_rejected(self):
        df = _build(_mart_row(recommended_engagement_action="continue_monitoring"), _features())
        # Inject a prohibited action directly
        df.loc[0, "recommended_engagement_action"] = "retire_report"
        with pytest.raises(ValueError):
            validate_report_engagement_context(df)

    def test_invalid_interpretation_rejected(self):
        df = _build(_mart_row(), _features())
        df.loc[0, "engagement_interpretation_status"] = "made_up_value"
        with pytest.raises(ValueError):
            validate_report_engagement_context(df)


class TestPersistence:
    def test_file_created(self, tmp_path):
        df = _build(_mart_row(), _features())
        path = persist_report_engagement_context(df, tmp_path)
        assert path.exists()

    def test_deterministic_sort(self, tmp_path):
        rows = pd.concat([_mart_row(report_id="R003"), _mart_row(report_id="R001"), _mart_row(report_id="R002")])
        df = _build(rows, pd.DataFrame([
            {"report_id": "R001", "analytics_as_of_date": AS_OF},
            {"report_id": "R002", "analytics_as_of_date": AS_OF},
            {"report_id": "R003", "analytics_as_of_date": AS_OF},
        ]))
        path = persist_report_engagement_context(df, tmp_path)
        loaded = pd.read_csv(path)
        assert list(loaded["report_id"]) == sorted(loaded["report_id"].tolist())

    def test_schema_stable(self, tmp_path):
        df = _build(_mart_row(), _features())
        path = persist_report_engagement_context(df, tmp_path)
        loaded = pd.read_csv(path)
        assert list(loaded.columns) == ENGAGEMENT_CONTEXT_COLS

    def test_source_mart_unchanged(self, tmp_path):
        mart_path = Path("outputs/analytics/mart_report_engagement.csv")
        if not mart_path.exists():
            pytest.skip("engagement mart not present")
        mtime_before = os.path.getmtime(mart_path)
        time.sleep(0.01)
        df = _build(_mart_row(), _features())
        persist_report_engagement_context(df, tmp_path)
        assert os.path.getmtime(mart_path) == mtime_before


class TestDeterminism:
    def test_same_input_same_output(self):
        df1 = _build(_mart_row(), _features())
        df2 = _build(_mart_row(), _features())
        # All non-generated_at columns should match
        compare_cols = [c for c in ENGAGEMENT_CONTEXT_COLS if c != "generated_at"]
        pd.testing.assert_frame_equal(
            df1[compare_cols].reset_index(drop=True),
            df2[compare_cols].reset_index(drop=True),
        )
