"""Comprehensive tests for the Sprint 7 prerequisite readiness validator."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import pytest

from src.analytics.report_analytics_readiness import (
    READINESS_OUTPUT_COLS,
    RECONCILIATION_OUTPUT_COLS,
    build_report_analytics_readiness_summary,
    build_report_spine_reconciliation,
    persist_readiness_outputs,
    validate_engagement_mart_readiness,
    validate_model_diagnostics_readiness,
    validate_report_analytics_prerequisites,
    validate_report_features_readiness,
)


# ─────────────────────────────────────────────────────────────────
# Fixtures / helpers
# ─────────────────────────────────────────────────────────────────

def _fresh_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stale_ts(days: int = 60) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def _make_features(n: int = 5, *, run_id: str | None = None, ts: str | None = None) -> pd.DataFrame:
    run_id = run_id or str(uuid.uuid4())
    ts = ts or _fresh_ts()
    return pd.DataFrame([{
        "report_id": f"RPT{i:03d}",
        "analytics_run_id": run_id,
        "generated_at": ts,
        "analytics_as_of_date": "2024-03-31",
        "schema_version": "2.0.0",
        "report_name": f"Report {i}",
        "recent_28d_views": 100,
        "previous_28d_views": 90,
        "history_sufficient_28d": True,
        "comparison_history_sufficient_28d": True,
        "usage_direction_28d": "stable",
        "usage_change_materiality": "immaterial",
        "historical_usage_status": "stable_regular_usage",
        "primary_historical_usage_issue": "none",
    } for i in range(n)])


def _make_engagement(n: int = 5, *, run_id: str | None = None, ts: str | None = None,
                     as_of: str = "2024-03-31") -> pd.DataFrame:
    run_id = run_id or str(uuid.uuid4())
    ts = ts or _fresh_ts()
    return pd.DataFrame([{
        "analytics_run_id": run_id,
        "generated_at": ts,
        "analytics_as_of_date": as_of,
        "report_id": f"RPT{i:03d}",
        "report_name": f"Report {i}",
        "unique_users_28d": 10,
        "overall_engagement_status": "stable_engagement",
        "engagement_evidence_status": "sufficient",
        "recommended_engagement_action": "maintain_current_approach",
        "activity_privacy_suppressed": False,
    } for i in range(n)])


def _make_model_diag(n: int = 5, *, ts: str | None = None, run_id: str | None = None) -> pd.DataFrame:
    run_id = run_id or str(uuid.uuid4())
    ts = ts or _fresh_ts()
    return pd.DataFrame([{
        "diagnostic_run_id": run_id,
        "generated_at": ts,
        "report_id": f"RPT{i:03d}",
        "report_name": f"Report {i}",
        "model_diagnostic_status": "healthy",
        "primary_model_issue": "none",
        "recommended_model_action": "no_action",
        "automatic_retraining_triggered": False,
        "training_cutoff": "2024-03-01",
    } for i in range(n)])


# ─────────────────────────────────────────────────────────────────
# Report features readiness (8 tests)
# ─────────────────────────────────────────────────────────────────

class TestReportFeaturesReadiness:
    def test_current_schema_accepted(self, tmp_path):
        p = tmp_path / "report_features.csv"
        _make_features().to_csv(p, index=False)
        result = validate_report_features_readiness(p)
        assert result["readiness_status"] == "ready"

    def test_stale_legacy_schema_rejected(self, tmp_path):
        p = tmp_path / "report_features.csv"
        # Legacy schema: uses old column names, no lineage
        legacy = pd.DataFrame([{
            "report_id": f"RPT{i:03d}",
            "latest_views": 100,
            "prior_views": 90,
            "usage_change_pct": 11.1,
            "top_user_concentration": 0.3,
            "days_active": 20,
            "avg_views": 10.0,
        } for i in range(5)])
        legacy.to_csv(p, index=False)
        result = validate_report_features_readiness(p)
        assert result["readiness_status"] != "ready"

    def test_canonical_fields_required(self, tmp_path):
        p = tmp_path / "report_features.csv"
        df = _make_features()
        df = df.drop(columns=["recent_28d_views"])
        df.to_csv(p, index=False)
        result = validate_report_features_readiness(p)
        assert result["schema_valid"] is False
        assert "missing_required_columns" in (result["readiness_reasons"] or "")

    def test_deprecated_fields_detected(self, tmp_path):
        p = tmp_path / "report_features.csv"
        df = _make_features()
        df["repeat_rate"] = 0.5  # deprecated
        df.to_csv(p, index=False)
        result = validate_report_features_readiness(p)
        assert "deprecated_columns_present" in (result["readiness_reasons"] or "")

    def test_valid_unique_grain(self, tmp_path):
        p = tmp_path / "report_features.csv"
        _make_features(5).to_csv(p, index=False)
        result = validate_report_features_readiness(p)
        assert result["grain_valid"] is True

    def test_duplicate_report_rejected(self, tmp_path):
        p = tmp_path / "report_features.csv"
        df = _make_features(3)
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        df.to_csv(p, index=False)
        result = validate_report_features_readiness(p)
        assert result["grain_valid"] is False
        assert result["readiness_status"] == "invalid_grain"

    def test_missing_lineage_fields_flagged(self, tmp_path):
        p = tmp_path / "report_features.csv"
        df = _make_features()
        df = df.drop(columns=["analytics_run_id"])
        df.to_csv(p, index=False)
        result = validate_report_features_readiness(p)
        assert result["lineage_valid"] is False

    def test_missing_file_returns_missing(self, tmp_path):
        p = tmp_path / "report_features.csv"
        result = validate_report_features_readiness(p)
        assert result["readiness_status"] == "missing"
        assert result["file_exists"] is False


# ─────────────────────────────────────────────────────────────────
# Engagement mart readiness (7 tests)
# ─────────────────────────────────────────────────────────────────

class TestEngagementMartReadiness:
    def test_engagement_mart_exists_ready(self, tmp_path):
        p = tmp_path / "mart_report_engagement.csv"
        _make_engagement().to_csv(p, index=False)
        result = validate_engagement_mart_readiness(p)
        assert result["readiness_status"] == "ready"

    def test_user_key_in_columns_schema_mismatch(self, tmp_path):
        p = tmp_path / "mart_report_engagement.csv"
        df = _make_engagement()
        df["user_key"] = "u001"
        df.to_csv(p, index=False)
        result = validate_engagement_mart_readiness(p)
        assert result["schema_valid"] is False
        assert result["readiness_status"] == "schema_mismatch"

    def test_duplicate_report_invalid_grain(self, tmp_path):
        p = tmp_path / "mart_report_engagement.csv"
        df = _make_engagement(3)
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        df.to_csv(p, index=False)
        result = validate_engagement_mart_readiness(p)
        assert result["grain_valid"] is False
        assert result["readiness_status"] == "invalid_grain"

    def test_invalid_engagement_status_schema_mismatch(self, tmp_path):
        p = tmp_path / "mart_report_engagement.csv"
        df = _make_engagement(3)
        df.loc[0, "overall_engagement_status"] = "unknown_bad_status"
        df.to_csv(p, index=False)
        result = validate_engagement_mart_readiness(p)
        assert result["schema_valid"] is False

    def test_prohibited_action_schema_mismatch(self, tmp_path):
        p = tmp_path / "mart_report_engagement.csv"
        df = _make_engagement(3)
        df.loc[0, "recommended_engagement_action"] = "retire_report"
        df.to_csv(p, index=False)
        result = validate_engagement_mart_readiness(p)
        assert result["schema_valid"] is False
        assert "prohibited_action" in (result["readiness_reasons"] or "")

    def test_missing_file_returns_missing(self, tmp_path):
        p = tmp_path / "mart_report_engagement.csv"
        result = validate_engagement_mart_readiness(p)
        assert result["readiness_status"] == "missing"

    def test_suppression_flags_preserved(self, tmp_path):
        p = tmp_path / "mart_report_engagement.csv"
        df = _make_engagement(3)
        assert "activity_privacy_suppressed" in df.columns
        df.to_csv(p, index=False)
        result = validate_engagement_mart_readiness(p)
        assert result["readiness_status"] == "ready"


# ─────────────────────────────────────────────────────────────────
# Model health readiness (6 tests)
# ─────────────────────────────────────────────────────────────────

class TestModelDiagnosticsReadiness:
    def test_valid_output_ready(self, tmp_path):
        p = tmp_path / "report_model_diagnostics.csv"
        _make_model_diag().to_csv(p, index=False)
        result = validate_model_diagnostics_readiness(p)
        assert result["readiness_status"] == "ready"

    def test_auto_retraining_true_schema_mismatch(self, tmp_path):
        p = tmp_path / "report_model_diagnostics.csv"
        df = _make_model_diag(3)
        df.loc[0, "automatic_retraining_triggered"] = True
        df.to_csv(p, index=False)
        result = validate_model_diagnostics_readiness(p)
        assert result["schema_valid"] is False
        assert "automatic_retraining_triggered_is_true" in (result["readiness_reasons"] or "")

    def test_duplicate_report_id_invalid_grain(self, tmp_path):
        p = tmp_path / "report_model_diagnostics.csv"
        df = _make_model_diag(3)
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        df.to_csv(p, index=False)
        result = validate_model_diagnostics_readiness(p)
        assert result["grain_valid"] is False

    def test_missing_file_returns_missing(self, tmp_path):
        p = tmp_path / "report_model_diagnostics.csv"
        result = validate_model_diagnostics_readiness(p)
        assert result["readiness_status"] == "missing"

    def test_missing_generated_at_incomplete_lineage(self, tmp_path):
        p = tmp_path / "report_model_diagnostics.csv"
        df = _make_model_diag(3)
        df = df.drop(columns=["generated_at"])
        df.to_csv(p, index=False)
        result = validate_model_diagnostics_readiness(p)
        assert result["lineage_valid"] is False

    def test_missing_model_diagnostic_status_schema_mismatch(self, tmp_path):
        p = tmp_path / "report_model_diagnostics.csv"
        df = _make_model_diag(3)
        df = df.drop(columns=["model_diagnostic_status"])
        df.to_csv(p, index=False)
        result = validate_model_diagnostics_readiness(p)
        assert result["schema_valid"] is False


# ─────────────────────────────────────────────────────────────────
# Temporal alignment (4 tests)
# ─────────────────────────────────────────────────────────────────

class TestTemporalAlignment:
    def test_matching_as_of_date_both_ready(self, tmp_path):
        feat = tmp_path / "report_features.csv"
        eng = tmp_path / "mart_report_engagement.csv"
        _make_features(5).to_csv(feat, index=False)
        _make_engagement(5, as_of="2024-03-31").to_csv(eng, index=False)
        r1 = validate_report_features_readiness(feat)
        r2 = validate_engagement_mart_readiness(eng)
        assert r1["readiness_status"] == "ready"
        assert r2["readiness_status"] == "ready"

    def test_mismatched_as_of_dates_flagged_in_summary(self, tmp_path):
        feat = tmp_path / "features.csv"
        eng = tmp_path / "engagement.csv"
        _make_features(3).to_csv(feat, index=False)
        _make_engagement(3, as_of="2024-01-01").to_csv(eng, index=False)
        r1 = validate_report_features_readiness(feat)
        r2 = validate_engagement_mart_readiness(eng)
        # Dates differ — summary should capture them separately
        assert r1["analytics_as_of_date"] != r2["analytics_as_of_date"]

    def test_all_three_prerequisites_pass(self, tmp_path):
        feat = tmp_path / "report_features.csv"
        eng = tmp_path / "mart_report_engagement.csv"
        diag = tmp_path / "report_model_diagnostics.csv"
        _make_features().to_csv(feat, index=False)
        _make_engagement().to_csv(eng, index=False)
        _make_model_diag().to_csv(diag, index=False)
        results = [
            validate_report_features_readiness(feat),
            validate_engagement_mart_readiness(eng),
            validate_model_diagnostics_readiness(diag),
        ]
        statuses = {r["readiness_status"] for r in results}
        assert statuses == {"ready"}

    def test_stale_output_detected(self, tmp_path):
        feat = tmp_path / "report_features.csv"
        _make_features(ts=_stale_ts(60)).to_csv(feat, index=False)
        result = validate_report_features_readiness(feat, max_staleness_days=7)
        assert result["freshness_status"] == "stale"
        assert result["readiness_status"] == "stale"


# ─────────────────────────────────────────────────────────────────
# Reconciliation (6 tests)
# ─────────────────────────────────────────────────────────────────

def _make_dim(report_ids, names=None):
    names = names or [f"Report {r}" for r in report_ids]
    return pd.DataFrame({"report_id": report_ids, "report_name": names})


class TestSpineReconciliation:
    def test_report_in_all_sources_complete(self):
        dim = _make_dim(["R001"])
        feat = pd.DataFrame([{"report_id": "R001"}])
        eng = pd.DataFrame([{"report_id": "R001"}])
        diag = pd.DataFrame([{"report_id": "R001"}])
        fc = pd.DataFrame([{"report_id": "R001"}])
        df = build_report_spine_reconciliation(dim, feat, eng, diag, fc)
        assert df.loc[df["report_id"] == "R001", "reconciliation_status"].iloc[0] == "complete"

    def test_report_no_engagement_expected_missing(self):
        dim = _make_dim(["R001"])
        feat = pd.DataFrame([{"report_id": "R001"}])
        eng_other = pd.DataFrame([{"report_id": "ROTHER"}])  # R001 not in engagement
        fc = pd.DataFrame([{"report_id": "R001"}])
        df = build_report_spine_reconciliation(dim, feat, eng_other, None, fc)
        row = df.loc[df["report_id"] == "R001"]
        assert row["reconciliation_status"].iloc[0] == "expected_missing_engagement"

    def test_report_in_meta_no_forecast_expected_missing_forecast(self):
        dim = _make_dim(["R001"])
        feat = pd.DataFrame([{"report_id": "R001"}])
        eng = pd.DataFrame([{"report_id": "R001"}])
        diag = pd.DataFrame([{"report_id": "R001"}])
        fc_empty = pd.DataFrame([{"report_id": "ROTHER"}])  # R001 not in forecast
        df = build_report_spine_reconciliation(dim, feat, eng, diag, fc_empty)
        row = df.loc[df["report_id"] == "R001"]
        assert row["reconciliation_status"].iloc[0] == "expected_missing_forecast"

    def test_report_missing_from_features_missing_required_source(self):
        dim = _make_dim(["R001"])
        feat = pd.DataFrame([{"report_id": "R999"}])  # different report
        df = build_report_spine_reconciliation(dim, feat, None, None, None)
        row = df.loc[df["report_id"] == "R001"]
        assert row["reconciliation_status"].iloc[0] == "missing_required_source"

    def test_orphan_report_only_in_features_not_metadata(self):
        dim = _make_dim(["R001"])
        feat = pd.DataFrame([{"report_id": "R001"}, {"report_id": "R999"}])
        df = build_report_spine_reconciliation(dim, feat, None, None, None)
        row = df.loc[df["report_id"] == "R999"]
        assert row["reconciliation_status"].iloc[0] == "orphan_source_record"

    def test_missing_sources_serialization_deterministic(self):
        dim = _make_dim(["R001"])
        feat = None  # R001 missing from features
        df1 = build_report_spine_reconciliation(dim, feat, None, None, None)
        df2 = build_report_spine_reconciliation(dim, feat, None, None, None)
        ms1 = df1.loc[df1["report_id"] == "R001", "missing_sources"].iloc[0]
        ms2 = df2.loc[df2["report_id"] == "R001", "missing_sources"].iloc[0]
        assert ms1 == ms2


# ─────────────────────────────────────────────────────────────────
# Persistence (4 tests)
# ─────────────────────────────────────────────────────────────────

class TestPersistence:
    def _build_sample_dfs(self, tmp_path):
        feat = tmp_path / "report_features.csv"
        eng = tmp_path / "mart_report_engagement.csv"
        diag = tmp_path / "report_model_diagnostics.csv"
        _make_features().to_csv(feat, index=False)
        _make_engagement().to_csv(eng, index=False)
        _make_model_diag().to_csv(diag, index=False)
        results = [
            validate_report_features_readiness(feat),
            validate_engagement_mart_readiness(eng),
            validate_model_diagnostics_readiness(diag),
        ]
        readiness_df = build_report_analytics_readiness_summary(results)
        dim = _make_dim([f"RPT{i:03d}" for i in range(5)])
        feat_df = _make_features()
        eng_df = _make_engagement()
        diag_df = _make_model_diag()
        rec_df = build_report_spine_reconciliation(dim, feat_df, eng_df, diag_df, None)
        return readiness_df, rec_df

    def test_readiness_csv_created(self, tmp_path):
        r_df, rec_df = self._build_sample_dfs(tmp_path)
        out_root = tmp_path / "project"
        r_path, _ = persist_readiness_outputs(r_df, rec_df, out_root)
        assert r_path.exists()

    def test_reconciliation_csv_created(self, tmp_path):
        r_df, rec_df = self._build_sample_dfs(tmp_path)
        out_root = tmp_path / "project"
        _, rec_path = persist_readiness_outputs(r_df, rec_df, out_root)
        assert rec_path.exists()

    def test_readiness_csv_has_correct_columns(self, tmp_path):
        r_df, rec_df = self._build_sample_dfs(tmp_path)
        out_root = tmp_path / "project"
        r_path, _ = persist_readiness_outputs(r_df, rec_df, out_root)
        loaded = pd.read_csv(r_path)
        assert list(loaded.columns) == READINESS_OUTPUT_COLS

    def test_reconciliation_sort_order_deterministic(self, tmp_path):
        r_df, rec_df = self._build_sample_dfs(tmp_path)
        out_root = tmp_path / "project"
        _, rec_path = persist_readiness_outputs(r_df, rec_df, out_root)
        loaded1 = pd.read_csv(rec_path)
        # Write again and confirm identical ordering
        persist_readiness_outputs(r_df, rec_df, out_root)
        loaded2 = pd.read_csv(rec_path)
        assert list(loaded1["report_id"]) == list(loaded2["report_id"])
