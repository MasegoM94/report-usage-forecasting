"""Tests for the report metadata context layer."""
import re
import pytest
import pandas as pd
from pathlib import Path
from src.analytics.report_metadata_context import (
    build_report_metadata_context, validate_report_metadata_context,
    persist_report_metadata_context, METADATA_CONTEXT_COLS,
    METADATA_CONTEXT_SCHEMA_VERSION, COMPLETENESS_REQUIRED_FIELDS,
)

AS_OF = "2024-03-31"
RUN_ID = "test-mc-001"


def _dim_row(**overrides):
    """Fully populated dim_report row using actual dim_report column conventions."""
    d = {
        "report_id": "R001",
        "report_name": "Sales Dashboard",
        "workspace_id": "WS001",
        "workspace_name": "Sales Analytics",
        "report_category": "Dashboard",
        "owner_team": "Sales Ops",
        "department": "Revenue",
        "business_area": "Commercial",
        "report_status": "active",
        "criticality_level": "high",
        "certification_status": "certified",
        "endorsement_status": "endorsed",
        "expected_usage_cadence": "daily",
        "service_level_tier": "tier_1",
        "source_system": "Salesforce",
        "replacement_report_id": None,
        "successor_report_id": None,
        "launch_date": "2023-01-15",
    }
    d.update(overrides)
    return pd.DataFrame([d])


def _features_row(**overrides):
    d = {
        "report_id": "R001",
        "analytics_as_of_date": AS_OF,
        "report_activation_date": "2023-01-15",
        "report_age_days": 442,
        "first_observed_usage_date": "2023-01-16",
        "latest_observed_usage_date": "2024-03-30",
        "days_since_last_use": 1,
        "adoption_maturity_status": "mature",
        "report_lifecycle_status": "established",
        "activation_date_status": "known",
    }
    d.update(overrides)
    return pd.DataFrame([d])


def _build(dim_df, features_df=None, as_of=AS_OF, run_id=RUN_ID):
    return build_report_metadata_context(dim_df, features_df, as_of, run_id)


class TestSchema:
    def test_all_required_cols_present(self):
        df = _build(_dim_row(), _features_row())
        missing = [c for c in METADATA_CONTEXT_COLS if c not in df.columns]
        assert not missing, f"Missing: {missing}"

    def test_no_prohibited_columns(self):
        df = _build(_dim_row(), _features_row())
        from src.analytics.report_metadata_context import PROHIBITED_METADATA_COLS
        bad = set(df.columns) & PROHIBITED_METADATA_COLS
        assert not bad

    def test_unique_grain(self):
        dim = pd.concat([_dim_row(report_id="R001"), _dim_row(report_id="R002")])
        feat = pd.concat([_features_row(report_id="R001"), _features_row(report_id="R002")])
        df = _build(dim, feat)
        assert not df.duplicated(subset=["report_id"]).any()


class TestCompletenessScore:
    def test_complete_metadata_score_one(self):
        df = _build(_dim_row(), _features_row())
        score = df["metadata_completeness_score"].iloc[0]
        assert score == 1.0

    def test_missing_activation_date_reduces_score(self):
        dim = _dim_row(launch_date=None)
        feat = _features_row(report_activation_date=None, activation_date_status="unavailable")
        df = _build(dim, feat)
        score = df["metadata_completeness_score"].iloc[0]
        assert score < 1.0

    def test_missing_owner_reduces_score(self):
        df = _build(_dim_row(owner_team=None), _features_row())
        score = df["metadata_completeness_score"].iloc[0]
        assert score < 1.0

    def test_score_between_0_and_1(self):
        df = _build(_dim_row(), _features_row())
        s = df["metadata_completeness_score"].iloc[0]
        assert 0.0 <= s <= 1.0

    def test_all_missing_score_zero(self):
        dim = _dim_row(
            owner_team=None, report_category=None, expected_usage_cadence=None,
            criticality_level=None, report_status=None, launch_date=None,
        )
        feat = _features_row(report_activation_date=None, activation_date_status="unavailable")
        df = _build(dim, feat)
        score = df["metadata_completeness_score"].iloc[0]
        assert score == 0.0


class TestInterpretationStatus:
    def test_complete_metadata_supported(self):
        df = _build(_dim_row(), _features_row())
        assert df["metadata_interpretation_status"].iloc[0] == "metadata_supported"

    def test_partial_metadata_with_gaps(self):
        df = _build(_dim_row(criticality_level=None, expected_usage_cadence=None), _features_row())
        status = df["metadata_interpretation_status"].iloc[0]
        assert status in ("metadata_supported", "metadata_supported_with_gaps")

    def test_minimal_metadata_limited(self):
        dim = _dim_row(
            owner_team=None, report_category=None, expected_usage_cadence=None,
            criticality_level=None, launch_date=None,
        )
        feat = _features_row(report_activation_date=None, activation_date_status="unavailable")
        df = _build(dim, feat)
        status = df["metadata_interpretation_status"].iloc[0]
        assert status in ("limited_metadata", "missing_metadata")


class TestCadenceNormalization:
    def test_valid_cadence_preserved(self):
        df = _build(_dim_row(expected_usage_cadence="daily"), _features_row())
        assert df["expected_usage_cadence"].iloc[0] == "daily"

    def test_null_cadence_becomes_unknown(self):
        df = _build(_dim_row(expected_usage_cadence=None), _features_row())
        val = df["expected_usage_cadence"].iloc[0]
        assert val == "unknown" or pd.isna(val)

    def test_invalid_cadence_becomes_unknown(self):
        df = _build(_dim_row(expected_usage_cadence="whenever"), _features_row())
        val = df["expected_usage_cadence"].iloc[0]
        assert val in ("unknown", None) or pd.isna(val)


class TestNoInferenceFromUsage:
    def test_cadence_not_inferred_from_usage(self):
        df = _build(_dim_row(expected_usage_cadence=None), _features_row())
        val = df["expected_usage_cadence"].iloc[0]
        assert val == "unknown" or pd.isna(val)

    def test_criticality_not_inferred(self):
        df = _build(_dim_row(criticality_level=None), _features_row())
        val = df["criticality_level"].iloc[0]
        assert val == "unknown" or pd.isna(val)


class TestPrivacySafety:
    def test_email_owner_redacted(self):
        df = _build(_dim_row(owner_team="john.smith@company.com"), _features_row())
        val = str(df["report_owner_team"].iloc[0]) if not pd.isna(df["report_owner_team"].iloc[0]) else ""
        assert "@" not in val or val == "" or pd.isna(df["report_owner_team"].iloc[0])

    def test_no_email_in_any_column(self):
        df = _build(_dim_row(), _features_row())
        email_pat = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
        for col in df.select_dtypes(include="object").columns:
            for val in df[col].dropna():
                assert not email_pat.search(str(val)), f"Email in {col}: {val}"


class TestSelfReferenceRejected:
    def test_self_replacement_rejected(self):
        dim = _dim_row(replacement_report_id="R001")
        with pytest.raises(ValueError):
            _build(dim, _features_row())

    def test_self_successor_rejected(self):
        dim = _dim_row(successor_report_id="R001")
        with pytest.raises(ValueError):
            _build(dim, _features_row())


class TestLifecycleReconciliation:
    def test_lifecycle_from_features(self):
        df = _build(_dim_row(), _features_row(adoption_maturity_status="mature"))
        assert df["adoption_maturity_status"].iloc[0] == "mature"

    def test_missing_features_graceful(self):
        df = _build(_dim_row(), features_df=None)
        assert len(df) == 1

    def test_activation_date_from_features_preferred(self):
        feat = _features_row(report_activation_date="2023-01-20")
        df = _build(_dim_row(launch_date="2023-01-15"), feat)
        assert df["report_activation_date"].iloc[0] == "2023-01-20"


class TestCertification:
    def test_certified_preserved(self):
        df = _build(_dim_row(certification_status="certified"), _features_row())
        assert df["certification_status"].iloc[0] == "certified"

    def test_uncertified_not_penalized(self):
        df = _build(_dim_row(certification_status="uncertified"), _features_row())
        status = df["metadata_evidence_status"].iloc[0]
        assert status != "invalid_metadata"


class TestMissingFields:
    def test_missing_fields_deterministic(self):
        dim = _dim_row(owner_team=None, criticality_level=None, expected_usage_cadence=None)
        feat = _features_row(report_activation_date=None)
        df1 = _build(dim, feat)
        df2 = _build(dim, feat)
        assert df1["missing_metadata_fields"].iloc[0] == df2["missing_metadata_fields"].iloc[0]

    def test_missing_fields_sorted(self):
        dim = _dim_row(owner_team=None, criticality_level=None, expected_usage_cadence=None)
        feat = _features_row(report_activation_date=None)
        df = _build(dim, feat)
        fields = df["missing_metadata_fields"].iloc[0]
        if fields and not pd.isna(fields):
            parts = fields.split(",")
            assert parts == sorted(parts)

    def test_no_missing_fields_null(self):
        df = _build(_dim_row(), _features_row())
        assert pd.isna(df["missing_metadata_fields"].iloc[0]) or df["missing_metadata_fields"].iloc[0] is None


class TestValidation:
    def test_valid_output_passes(self):
        df = _build(_dim_row(), _features_row())
        validate_report_metadata_context(df)

    def test_invalid_cadence_rejected(self):
        df = _build(_dim_row(), _features_row())
        df.loc[0, "expected_usage_cadence"] = "whenever_i_feel_like_it"
        with pytest.raises(ValueError):
            validate_report_metadata_context(df)

    def test_score_outside_range_rejected(self):
        df = _build(_dim_row(), _features_row())
        df.loc[0, "metadata_completeness_score"] = 1.5
        with pytest.raises(ValueError):
            validate_report_metadata_context(df)


class TestPersistence:
    def test_file_created(self, tmp_path):
        df = _build(_dim_row(), _features_row())
        path = persist_report_metadata_context(df, tmp_path)
        assert path.exists()

    def test_schema_stable(self, tmp_path):
        df = _build(_dim_row(), _features_row())
        path = persist_report_metadata_context(df, tmp_path)
        loaded = pd.read_csv(path)
        assert list(loaded.columns) == METADATA_CONTEXT_COLS

    def test_deterministic_sort(self, tmp_path):
        dim = pd.concat([_dim_row(report_id="R003"), _dim_row(report_id="R001"), _dim_row(report_id="R002")])
        feat = pd.concat([
            _features_row(report_id="R001"),
            _features_row(report_id="R002"),
            _features_row(report_id="R003"),
        ])
        df = _build(dim, feat)
        path = persist_report_metadata_context(df, tmp_path)
        loaded = pd.read_csv(path)
        assert list(loaded["report_id"]) == sorted(loaded["report_id"].tolist())
