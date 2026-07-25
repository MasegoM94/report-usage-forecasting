"""Tests for Sprint 7 Step 5: report model-health context layer."""
import pytest
import pandas as pd
import tempfile
from pathlib import Path

from src.analytics.report_model_health_context import (
    ModelHealthContextConfig,
    build_report_model_health_context,
    validate_report_model_health_context,
    persist_report_model_health_context,
    MODEL_HEALTH_CONTEXT_COLS,
    MODEL_HEALTH_CONTEXT_SCHEMA_VERSION,
    ALLOWED_INTERPRETATION_STATUSES,
    ALLOWED_FORECAST_REVIEW_ACTIONS,
    PROHIBITED_ACTIONS,
    _classify_forecast_interpretation,
    _validate_lineage_agreement,
    _build_interpretation_reasons,
    _count_model_issues,
)

CFG = ModelHealthContextConfig()
RUN_ID = "test-mhc-001"


def _diag_row(**overrides):
    """Default healthy diagnostic row."""
    d = {
        "report_id": "R001",
        "report_name": "Report R001",
        "selected_model_name": "SARIMA",
        "selected_m": 7,
        "training_cutoff": "2024-03-31",
        "selection_run_id": "sel-001",
        "evaluation_run_id": "eval-001",
        "diagnostic_run_id": "diag-001",
        "generated_at": "2024-04-01T00:00:00",
        "model_diagnostic_status": "healthy",
        "primary_model_issue": "none",
        "recommended_model_action": "continue_monitoring",
        "model_review_required": False,
        "backtest_evidence_status": "complete",
        "production_evidence_status": "partial",
        "production_evidence_maturity": "immature",
        "model_evidence_status": "complete",
        "residual_autocorrelation_status": "normal",
        "bias_status": "no_significant_bias",
        "variance_stability_status": "stable",
        "outlier_status": "low",
        "interval_calibration_status": "well_calibrated",
        "automatic_retraining_triggered": False,
        # Real diag columns for graceful fallback
        "diagnostic_issue_count": 0,
        "warning_issue_count": 0,
        "poor_issue_count": 0,
        "diagnostic_evidence_status": "complete",
        "missing_evidence_categories": "[]",
        "backtest_diagnostics_available": True,
        "production_diagnostics_available": True,
        "review_required": False,
        "model_diagnostic_reasons": "Model health is acceptable.",
    }
    d.update(overrides)
    return pd.DataFrame([d])


def _outlook_row(**overrides):
    """Default stable forecast outlook row."""
    d = {
        "report_id": "R001",
        "forecast_run_id": "fc-001",
        "selected_model_name": "SARIMA",
        "selected_m": 7,
        "training_cutoff": "2024-03-31",
        "forecast_outlook_status": "stable_outlook",
        "forecast_uncertainty_status": "moderate_uncertainty",
    }
    d.update(overrides)
    return pd.DataFrame([d])


# ---------------------------------------------------------------------------
# TestInterpretationClassification
# ---------------------------------------------------------------------------

class TestInterpretationClassification:
    def test_healthy_stable_is_outlook_supported(self):
        diag = _diag_row().iloc[0]
        outlook = _outlook_row().iloc[0]
        result = _classify_forecast_interpretation(diag, outlook, "valid", CFG)
        assert result == "outlook_supported"

    def test_watch_model_is_caution(self):
        diag = _diag_row(
            model_diagnostic_status="watch",
            primary_model_issue="persistent_bias",
            model_evidence_status="complete",
            backtest_evidence_status="complete",
        ).iloc[0]
        outlook = _outlook_row().iloc[0]
        result = _classify_forecast_interpretation(diag, outlook, "valid", CFG)
        assert result == "outlook_supported_with_caution"

    def test_poor_model_is_health_limited(self):
        diag = _diag_row(
            model_diagnostic_status="poor",
            primary_model_issue="residual_autocorrelation",
            model_evidence_status="complete",
            backtest_evidence_status="complete",
        ).iloc[0]
        outlook = _outlook_row().iloc[0]
        result = _classify_forecast_interpretation(diag, outlook, "valid", CFG)
        assert result == "model_health_limited"

    def test_very_high_uncertainty_is_uncertainty_limited(self):
        diag = _diag_row(
            model_diagnostic_status="healthy",
            model_evidence_status="complete",
            backtest_evidence_status="complete",
        ).iloc[0]
        outlook = _outlook_row(forecast_uncertainty_status="very_high_uncertainty").iloc[0]
        result = _classify_forecast_interpretation(diag, outlook, "valid", CFG)
        assert result == "uncertainty_limited"

    def test_insufficient_backtest_evidence(self):
        diag = _diag_row(
            model_diagnostic_status="insufficient_evidence",
            model_evidence_status="insufficient",
            backtest_evidence_status="insufficient",
        ).iloc[0]
        outlook = _outlook_row().iloc[0]
        result = _classify_forecast_interpretation(diag, outlook, "valid", CFG)
        assert result == "insufficient_model_evidence"

    def test_calculation_failed_is_invalid(self):
        diag = _diag_row(
            model_diagnostic_status="calculation_failed",
            model_evidence_status="calculation_failed",
        ).iloc[0]
        outlook = _outlook_row().iloc[0]
        result = _classify_forecast_interpretation(diag, outlook, "valid", CFG)
        assert result == "invalid_model_evidence"


# ---------------------------------------------------------------------------
# TestLineageValidation
# ---------------------------------------------------------------------------

class TestLineageValidation:
    def test_matching_fields_is_valid(self):
        diag = _diag_row().iloc[0]
        outlook = _outlook_row().iloc[0]
        status, fields = _validate_lineage_agreement(diag, outlook, CFG)
        assert status == "valid"
        assert fields is None

    def test_model_name_mismatch(self):
        diag = _diag_row(selected_model_name="SARIMA").iloc[0]
        outlook = _outlook_row(selected_model_name="ETS").iloc[0]
        status, fields = _validate_lineage_agreement(diag, outlook, CFG)
        assert status == "mismatch"
        assert "selected_model_name" in fields

    def test_selected_m_mismatch(self):
        diag = _diag_row(selected_m=7).iloc[0]
        outlook = _outlook_row(selected_m=30).iloc[0]
        status, fields = _validate_lineage_agreement(diag, outlook, CFG)
        assert status == "mismatch"
        assert "selected_m" in fields

    def test_training_cutoff_mismatch(self):
        diag = _diag_row(training_cutoff="2024-03-31").iloc[0]
        outlook = _outlook_row(training_cutoff="2024-01-31").iloc[0]
        status, fields = _validate_lineage_agreement(diag, outlook, CFG)
        assert status == "mismatch"
        assert "training_cutoff" in fields

    def test_missing_forecast_outlook(self):
        diag = _diag_row().iloc[0]
        status, fields = _validate_lineage_agreement(diag, None, CFG)
        assert status == "missing_forecast_outlook"
        assert fields is None


# ---------------------------------------------------------------------------
# TestOutputSchema
# ---------------------------------------------------------------------------

class TestOutputSchema:
    def _build_context(self):
        return build_report_model_health_context(
            _diag_row(), _outlook_row(), CFG, RUN_ID
        )

    def test_all_required_columns_present(self):
        df = self._build_context()
        for col in MODEL_HEALTH_CONTEXT_COLS:
            assert col in df.columns, f"Missing column: {col}"

    def test_unique_grain(self):
        df = self._build_context()
        assert not df.duplicated(subset=["diagnostic_run_id", "report_id"]).any()

    def test_no_prohibited_actions(self):
        df = self._build_context()
        actions = set(df["recommended_model_action"].dropna())
        assert actions.isdisjoint(PROHIBITED_ACTIONS)

    def test_no_user_identifier_columns(self):
        df = self._build_context()
        _user_cols = {"user_id", "email", "email_address", "display_name",
                      "unique_user", "principal_name", "user_key"}
        assert set(df.columns).isdisjoint(_user_cols)

    def test_no_automatic_retraining_true(self):
        df = self._build_context()
        if "automatic_retraining_triggered" in df.columns:
            assert not df["automatic_retraining_triggered"].any()


# ---------------------------------------------------------------------------
# TestEvidenceStatuses
# ---------------------------------------------------------------------------

class TestEvidenceStatuses:
    def test_complete_backtest_evidence_preserved(self):
        df = build_report_model_health_context(
            _diag_row(backtest_evidence_status="complete"),
            _outlook_row(), CFG, RUN_ID
        )
        assert df.iloc[0]["backtest_evidence_status"] == "complete"

    def test_immature_production_not_failure(self):
        df = build_report_model_health_context(
            _diag_row(production_evidence_maturity="immature"),
            _outlook_row(), CFG, RUN_ID
        )
        assert df.iloc[0]["forecast_interpretation_status"] != "invalid_model_evidence"

    def test_insufficient_evidence_yields_insufficient_interpretation(self):
        df = build_report_model_health_context(
            _diag_row(
                model_diagnostic_status="insufficient_evidence",
                model_evidence_status="insufficient",
                backtest_evidence_status="insufficient",
            ),
            _outlook_row(), CFG, RUN_ID
        )
        assert df.iloc[0]["forecast_interpretation_status"] == "insufficient_model_evidence"

    def test_missing_diag_fields_handled_gracefully(self):
        # Minimal row with only required fields
        minimal = pd.DataFrame([{
            "report_id": "R001",
            "diagnostic_run_id": "diag-001",
            "model_diagnostic_status": "healthy",
        }])
        df = build_report_model_health_context(minimal, None, CFG, RUN_ID)
        assert len(df) == 1
        assert df.iloc[0]["report_id"] == "R001"


# ---------------------------------------------------------------------------
# TestReasons
# ---------------------------------------------------------------------------

class TestReasons:
    def test_reasons_are_non_empty_string(self):
        df = build_report_model_health_context(
            _diag_row(), _outlook_row(), CFG, RUN_ID
        )
        reasons = df.iloc[0]["forecast_interpretation_reasons"]
        assert isinstance(reasons, str) and len(reasons) > 0

    def test_reasons_are_deterministic(self):
        df1 = build_report_model_health_context(
            _diag_row(), _outlook_row(), CFG, "run-a"
        )
        df2 = build_report_model_health_context(
            _diag_row(), _outlook_row(), CFG, "run-b"
        )
        assert (
            df1.iloc[0]["forecast_interpretation_reasons"]
            == df2.iloc[0]["forecast_interpretation_reasons"]
        )

    def test_no_retire_or_business_value_language(self):
        df = build_report_model_health_context(
            _diag_row(), _outlook_row(), CFG, RUN_ID
        )
        for col in df.select_dtypes(include="object").columns:
            vals = df[col].dropna().astype(str)
            for v in vals:
                assert "retire" not in v.lower(), f"'retire' found in {col}: {v}"


# ---------------------------------------------------------------------------
# TestMissingOutlook
# ---------------------------------------------------------------------------

class TestMissingOutlook:
    def test_missing_outlook_lineage_status(self):
        df = build_report_model_health_context(
            _diag_row(), None, CFG, RUN_ID
        )
        assert df.iloc[0]["lineage_validation_status"] == "missing_forecast_outlook"

    def test_interpretation_works_without_outlook(self):
        df = build_report_model_health_context(
            _diag_row(), None, CFG, RUN_ID
        )
        assert df.iloc[0]["forecast_interpretation_status"] in ALLOWED_INTERPRETATION_STATUSES

    def test_forecast_fields_null_but_row_preserved(self):
        df = build_report_model_health_context(
            _diag_row(), None, CFG, RUN_ID
        )
        assert len(df) == 1
        assert df.iloc[0]["forecast_outlook_status"] is None or pd.isna(df.iloc[0]["forecast_outlook_status"])
        assert df.iloc[0]["forecast_run_id"] is None or pd.isna(df.iloc[0]["forecast_run_id"])


# ---------------------------------------------------------------------------
# TestPersistence
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_file_created_at_correct_path(self, tmp_path):
        df = build_report_model_health_context(
            _diag_row(), _outlook_row(), CFG, RUN_ID
        )
        out = persist_report_model_health_context(df, tmp_path)
        expected = tmp_path / "outputs" / "analytics" / "report_model_health_context.csv"
        assert out == expected
        assert expected.exists()

    def test_validate_called_before_persist(self, tmp_path):
        # Inject an invalid interpretation status
        df = build_report_model_health_context(
            _diag_row(), _outlook_row(), CFG, RUN_ID
        )
        df["forecast_interpretation_status"] = "not_a_valid_status"
        with pytest.raises(ValueError):
            persist_report_model_health_context(df, tmp_path)

    def test_deterministic_sort_order(self, tmp_path):
        diag = pd.concat([
            _diag_row(report_id="R002"),
            _diag_row(report_id="R001"),
        ], ignore_index=True)
        outlook = pd.concat([
            _outlook_row(report_id="R002"),
            _outlook_row(report_id="R001"),
        ], ignore_index=True)
        out = persist_report_model_health_context(
            build_report_model_health_context(diag, outlook, CFG, RUN_ID),
            tmp_path,
        )
        loaded = pd.read_csv(out)
        assert list(loaded["report_id"]) == sorted(loaded["report_id"].tolist())

    def test_source_files_unchanged_after_persist(self, tmp_path):
        # Create source files
        diag_dir = tmp_path / "outputs" / "diagnostics"
        diag_dir.mkdir(parents=True)
        diag_path = diag_dir / "report_model_diagnostics.csv"
        _diag_row().to_csv(diag_path, index=False)
        original_size = diag_path.stat().st_size

        df = build_report_model_health_context(
            _diag_row(), _outlook_row(), CFG, RUN_ID
        )
        persist_report_model_health_context(df, tmp_path)

        assert diag_path.stat().st_size == original_size


# ---------------------------------------------------------------------------
# TestIssueCount
# ---------------------------------------------------------------------------

class TestIssueCount:
    def test_zero_issues_for_healthy(self):
        diag = _diag_row(diagnostic_issue_count=0, warning_issue_count=0).iloc[0]
        issue_count, warning_count = _count_model_issues(diag)
        assert issue_count == 0
        assert warning_count == 0

    def test_warning_increments_warning_count(self):
        diag = _diag_row(
            diagnostic_issue_count=1,
            warning_issue_count=1,
            poor_issue_count=0,
        ).iloc[0]
        issue_count, warning_count = _count_model_issues(diag)
        assert warning_count == 1

    def test_poor_issue_increments_issue_count(self):
        diag = _diag_row(
            diagnostic_issue_count=2,
            warning_issue_count=0,
            poor_issue_count=2,
        ).iloc[0]
        issue_count, warning_count = _count_model_issues(diag)
        assert issue_count >= 1


# ---------------------------------------------------------------------------
# Integration: multi-report
# ---------------------------------------------------------------------------

class TestMultiReport:
    def test_multiple_reports_all_rows_preserved(self):
        diag = pd.concat([
            _diag_row(report_id="R001"),
            _diag_row(report_id="R002", model_diagnostic_status="watch"),
            _diag_row(report_id="R003",
                      model_diagnostic_status="insufficient_evidence",
                      model_evidence_status="insufficient",
                      backtest_evidence_status="insufficient"),
        ], ignore_index=True)
        outlook = pd.concat([
            _outlook_row(report_id="R001"),
            _outlook_row(report_id="R002"),
            _outlook_row(report_id="R003"),
        ], ignore_index=True)
        df = build_report_model_health_context(diag, outlook, CFG, RUN_ID)
        assert len(df) == 3
        statuses = set(df["forecast_interpretation_status"])
        assert statuses.issubset(ALLOWED_INTERPRETATION_STATUSES)

    def test_schema_version_constant(self):
        assert MODEL_HEALTH_CONTEXT_SCHEMA_VERSION == "1.0.0"

    def test_validate_passes_on_good_output(self):
        df = build_report_model_health_context(
            _diag_row(), _outlook_row(), CFG, RUN_ID
        )
        # Should not raise
        validate_report_model_health_context(df)

    def test_validate_rejects_prohibited_action(self):
        df = build_report_model_health_context(
            _diag_row(), _outlook_row(), CFG, RUN_ID
        )
        df["recommended_model_action"] = "retire_report"
        with pytest.raises(ValueError, match="retire"):
            validate_report_model_health_context(df)
