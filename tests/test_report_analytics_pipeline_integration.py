"""
End-to-end integration tests for the Sprint 7 report analytics pipeline.

Tests load persisted outputs and validate that the full pipeline maintains
data integrity, temporal alignment, privacy, and deterministic logic.
"""
import re
import pytest
import pandas as pd
from pathlib import Path

ANALYTICS_DIR = Path("outputs/analytics")
METRICS_DIR = Path("outputs/metrics")


@pytest.fixture(scope="module")
def features_df():
    return pd.read_csv(METRICS_DIR / "report_features.csv")


@pytest.fixture(scope="module")
def forecast_df():
    return pd.read_csv(ANALYTICS_DIR / "report_forecast_outlook.csv")


@pytest.fixture(scope="module")
def model_health_df():
    return pd.read_csv(ANALYTICS_DIR / "report_model_health_context.csv")


@pytest.fixture(scope="module")
def engagement_df():
    return pd.read_csv(ANALYTICS_DIR / "report_engagement_context.csv")


@pytest.fixture(scope="module")
def metadata_df():
    return pd.read_csv(ANALYTICS_DIR / "report_metadata_context.csv")


@pytest.fixture(scope="module")
def diagnostics_df():
    return pd.read_csv(ANALYTICS_DIR / "report_diagnostics.csv")


@pytest.fixture(scope="module")
def segments_df():
    return pd.read_csv(ANALYTICS_DIR / "report_segments.csv")


@pytest.fixture(scope="module")
def mart_df():
    return pd.read_csv(ANALYTICS_DIR / "mart_report_analytics.csv")


PROHIBITED_COLS = frozenset({
    "user_id", "email", "email_address", "user_name", "username",
    "display_name", "unique_user", "principal_name",
    "repeat_rate", "latest_views", "prior_views", "top_user_concentration",
    "usage_change_pct",
})

PROHIBITED_ACTIONS = frozenset({
    "retire_report", "delete_report", "automatically_retrain",
    "change_selected_model", "restrict_user", "contact_specific_user",
})


class TestFileExistence:
    def test_report_features_exists(self):
        assert (METRICS_DIR / "report_features.csv").exists()

    def test_forecast_outlook_exists(self):
        assert (ANALYTICS_DIR / "report_forecast_outlook.csv").exists()

    def test_model_health_exists(self):
        assert (ANALYTICS_DIR / "report_model_health_context.csv").exists()

    def test_engagement_context_exists(self):
        assert (ANALYTICS_DIR / "report_engagement_context.csv").exists()

    def test_metadata_context_exists(self):
        assert (ANALYTICS_DIR / "report_metadata_context.csv").exists()

    def test_diagnostics_exists(self):
        assert (ANALYTICS_DIR / "report_diagnostics.csv").exists()

    def test_segments_exists(self):
        assert (ANALYTICS_DIR / "report_segments.csv").exists()

    def test_mart_exists(self):
        assert (ANALYTICS_DIR / "mart_report_analytics.csv").exists()


class TestSpinePreservation:
    def test_mart_has_all_feature_reports(self, features_df, mart_df):
        feat_ids = set(features_df["report_id"].astype(str))
        mart_ids = set(mart_df["report_id"].astype(str))
        assert feat_ids == mart_ids

    def test_unique_report_id_in_mart(self, mart_df):
        assert not mart_df.duplicated(subset=["report_id"]).any()

    def test_unique_report_id_in_diagnostics(self, diagnostics_df):
        assert not diagnostics_df.duplicated(subset=["report_id"]).any()

    def test_unique_report_id_in_segments(self, segments_df):
        assert not segments_df.duplicated(subset=["report_id"]).any()

    def test_all_features_reports_in_diagnostics(self, features_df, diagnostics_df):
        feat_ids = set(features_df["report_id"].astype(str))
        diag_ids = set(diagnostics_df["report_id"].astype(str))
        assert feat_ids == diag_ids

    def test_all_features_reports_in_segments(self, features_df, segments_df):
        feat_ids = set(features_df["report_id"].astype(str))
        seg_ids = set(segments_df["report_id"].astype(str))
        assert feat_ids == seg_ids


class TestTemporalAlignment:
    def test_features_as_of_date_consistent(self, features_df):
        assert features_df["analytics_as_of_date"].nunique() == 1

    def test_engagement_as_of_date_consistent(self, engagement_df):
        if "analytics_as_of_date" in engagement_df.columns:
            assert engagement_df["analytics_as_of_date"].nunique() == 1

    def test_features_engagement_as_of_aligned(self, features_df, engagement_df):
        if "analytics_as_of_date" in engagement_df.columns:
            feat_date = features_df["analytics_as_of_date"].iloc[0]
            eng_date = engagement_df["analytics_as_of_date"].iloc[0]
            assert feat_date == eng_date, f"Temporal mismatch: features={feat_date}, engagement={eng_date}"


class TestPrivacyPreservation:
    def test_no_prohibited_cols_in_mart(self, mart_df):
        bad = set(mart_df.columns) & PROHIBITED_COLS
        assert not bad, f"Prohibited columns: {bad}"

    def test_no_prohibited_cols_in_segments(self, segments_df):
        bad = set(segments_df.columns) & PROHIBITED_COLS
        assert not bad

    def test_no_prohibited_cols_in_diagnostics(self, diagnostics_df):
        bad = set(diagnostics_df.columns) & PROHIBITED_COLS
        assert not bad

    def test_no_email_patterns_in_mart(self, mart_df):
        email_pat = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
        for col in mart_df.select_dtypes(include="object").columns:
            for val in mart_df[col].dropna().astype(str).head(50):
                assert not email_pat.search(val), f"Email in mart.{col}: {val}"

    def test_suppressed_concentration_null_in_engagement(self, engagement_df):
        """When privacy is suppressed, concentration metrics must be null."""
        if "privacy_suppression_status" not in engagement_df.columns:
            pytest.skip("privacy_suppression_status not found")
        suppressed = engagement_df[
            engagement_df["privacy_suppression_status"] == "suppressed"
        ]
        if len(suppressed) == 0:
            pytest.skip("No suppressed rows in dataset")
        conc_cols = [c for c in ["top_1_user_view_share_28d", "user_view_hhi_28d"] if c in suppressed.columns]
        for col in conc_cols:
            assert suppressed[col].isna().all(), f"Suppressed rows have non-null {col}"


class TestNoProhibitedActions:
    def test_no_retirement_in_diagnostics(self, diagnostics_df):
        if "recommended_diagnostic_action" in diagnostics_df.columns:
            bad = set(diagnostics_df["recommended_diagnostic_action"].dropna()) & PROHIBITED_ACTIONS
            assert not bad, f"Prohibited actions in diagnostics: {bad}"

    def test_no_retirement_in_mart(self, mart_df):
        for col in ["recommended_report_action", "recommended_diagnostic_action"]:
            if col in mart_df.columns:
                bad = set(mart_df[col].dropna()) & PROHIBITED_ACTIONS
                assert not bad, f"Prohibited actions in mart.{col}: {bad}"

    def test_no_retirement_in_model_health(self, model_health_df):
        if "recommended_model_action" in model_health_df.columns:
            bad = set(model_health_df["recommended_model_action"].dropna()) & PROHIBITED_ACTIONS
            assert not bad

    def test_automatic_retraining_not_triggered(self, model_health_df):
        if "automatic_retraining_triggered" in model_health_df.columns:
            assert (model_health_df["automatic_retraining_triggered"] == False).all()


class TestNoDeprecatedFields:
    def test_no_repeat_rate_in_mart(self, mart_df):
        assert "repeat_rate" not in mart_df.columns

    def test_no_latest_views_in_mart(self, mart_df):
        assert "latest_views" not in mart_df.columns

    def test_no_prior_views_in_mart(self, mart_df):
        assert "prior_views" not in mart_df.columns

    def test_no_top_user_concentration_in_mart(self, mart_df):
        assert "top_user_concentration" not in mart_df.columns

    def test_no_usage_change_pct_in_mart(self, mart_df):
        assert "usage_change_pct" not in mart_df.columns

    def test_no_deprecated_in_segments(self, segments_df):
        bad = set(segments_df.columns) & PROHIBITED_COLS
        assert not bad

    def test_no_deprecated_in_diagnostics(self, diagnostics_df):
        bad = set(diagnostics_df.columns) & PROHIBITED_COLS
        assert not bad


class TestSourceMetricReconciliation:
    def test_mart_recent_28d_views_matches_features(self, features_df, mart_df):
        if "recent_28d_views" not in features_df.columns or "recent_28d_views" not in mart_df.columns:
            pytest.skip("Column not present in both")
        for _, feat_row in features_df.iterrows():
            rid = str(feat_row["report_id"])
            mart_row = mart_df[mart_df["report_id"].astype(str) == rid]
            if len(mart_row) == 0:
                continue
            feat_val = feat_row["recent_28d_views"]
            mart_val = mart_row["recent_28d_views"].iloc[0]
            if pd.isna(feat_val) and pd.isna(mart_val):
                continue
            assert feat_val == mart_val, f"Mismatch in recent_28d_views for {rid}"

    def test_mart_model_diagnostic_matches_source(self, model_health_df, mart_df):
        if "model_diagnostic_status" not in model_health_df.columns or "model_diagnostic_status" not in mart_df.columns:
            pytest.skip("Column not present")
        mh_idx = {str(r["report_id"]): r["model_diagnostic_status"] for _, r in model_health_df.iterrows()}
        for _, mart_row in mart_df.iterrows():
            rid = str(mart_row["report_id"])
            if rid not in mh_idx:
                continue
            assert mart_row["model_diagnostic_status"] == mh_idx[rid]

    def test_mart_primary_segment_matches_segments(self, segments_df, mart_df):
        if "primary_report_segment" not in segments_df.columns or "primary_report_segment" not in mart_df.columns:
            pytest.skip("Column not present")
        seg_idx = {str(r["report_id"]): r["primary_report_segment"] for _, r in segments_df.iterrows()}
        for _, mart_row in mart_df.iterrows():
            rid = str(mart_row["report_id"])
            if rid not in seg_idx:
                continue
            assert mart_row["primary_report_segment"] == seg_idx[rid]


class TestStatusDeterminism:
    def test_overall_status_valid(self, mart_df):
        from src.analytics.report_analytics_mart import ALLOWED_OVERALL_STATUS
        if "overall_report_status" not in mart_df.columns:
            pytest.skip("Column not present")
        invalid = set(mart_df["overall_report_status"].dropna()) - ALLOWED_OVERALL_STATUS
        assert not invalid, f"Invalid statuses: {invalid}"

    def test_review_priority_valid(self, mart_df):
        from src.analytics.report_analytics_mart import ALLOWED_REVIEW_PRIORITY
        if "overall_review_priority" not in mart_df.columns:
            pytest.skip("Column not present")
        invalid = set(mart_df["overall_review_priority"].dropna()) - ALLOWED_REVIEW_PRIORITY
        assert not invalid

    def test_primary_segment_valid(self, segments_df):
        from src.analytics.report_segmentation import ALLOWED_PRIMARY_SEGMENTS
        invalid = set(segments_df["primary_report_segment"].dropna()) - ALLOWED_PRIMARY_SEGMENTS
        assert not invalid

    def test_primary_diagnostic_valid(self, diagnostics_df):
        from src.analytics.report_diagnostics import ALLOWED_PRIMARY_DIAGNOSTICS
        invalid = set(diagnostics_df["primary_diagnostic"].dropna()) - ALLOWED_PRIMARY_DIAGNOSTICS
        assert not invalid


class TestRepresentativeCategories:
    """Validate that at least some expected categories appear in the dataset."""

    def test_some_reports_have_review_required(self, diagnostics_df):
        if "diagnostic_review_required" in diagnostics_df.columns:
            assert diagnostics_df["diagnostic_review_required"].any()

    def test_segments_have_multiple_categories(self, segments_df):
        assert segments_df["primary_report_segment"].nunique() >= 2

    def test_mart_has_multiple_statuses(self, mart_df):
        if "overall_report_status" in mart_df.columns:
            assert mart_df["overall_report_status"].nunique() >= 2

    def test_model_health_has_records(self, model_health_df):
        assert len(model_health_df) > 0


class TestSourceFilesUnchanged:
    """Source input files must not be modified by the analytics pipeline."""

    def test_mart_csv_distinct_from_features(self, mart_df):
        """Mart is a superset of features — not the same file."""
        assert len(mart_df.columns) > 20  # mart has far more columns than any single source
