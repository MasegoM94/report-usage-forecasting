"""Regression tests: current report features pass Streamlit loader; legacy schema fails."""
import pandas as pd
import pytest
from pathlib import Path

try:
    from src.app.utils.load_data import validate_report_features_schema
    HAS_VALIDATOR = True
except ImportError:
    HAS_VALIDATOR = False

# v2.0.0 canonical columns replacing the v1 schema
CURRENT_CANONICAL_COLS = [
    "report_id", "analytics_as_of_date", "schema_version",
    "recent_28d_views", "previous_28d_views",
    "history_sufficient_28d", "usage_direction_28d",
    "historical_usage_status", "primary_historical_usage_issue",
]
LEGACY_ONLY_COLS = [
    "latest_views", "prior_views", "usage_change_pct", "top_user_concentration",
    "avg_views", "days_active", "top_1_user_view_share",
    "trend_history_sufficient", "usage_trend_12w_slope",
]

def _make_current(n=5):
    return pd.DataFrame([{
        "report_id": f"R{i:03d}",
        "analytics_as_of_date": "2026-03-31",
        "schema_version": "2.0.0",
        "recent_28d_views": 100,
        "previous_28d_views": 90,
        "history_sufficient_28d": True,
        "comparison_history_sufficient_28d": True,
        "usage_direction_28d": "stable",
        "usage_change_materiality": "immaterial",
        "historical_usage_status": "stable_regular_usage",
        "primary_historical_usage_issue": "none",
        "analytics_run_id": "run-001",
        "generated_at": "2026-07-25T00:00:00",
    } for i in range(n)])

def _make_legacy(n=5):
    return pd.DataFrame([{
        "report_id": f"R{i:03d}",
        "latest_views": 100,
        "prior_views": 90,
        "usage_change_pct": 11.1,
        "top_user_concentration": 0.3,
        "days_active": 20,
        "avg_views": 10.0,
    } for i in range(n)])

class TestCurrentSchemaAccepted:
    def test_current_has_required_cols(self):
        df = _make_current()
        for col in CURRENT_CANONICAL_COLS:
            assert col in df.columns

    @pytest.mark.skipif(not HAS_VALIDATOR, reason="validator not importable")
    def test_current_passes_validator(self):
        df = _make_current()
        validate_report_features_schema(df)  # must not raise

    def test_current_file_on_disk_uses_current_schema(self):
        path = Path("outputs/metrics/report_features.csv")
        if not path.exists():
            pytest.skip("report_features.csv not yet generated")
        df = pd.read_csv(path)
        missing = [c for c in CURRENT_CANONICAL_COLS if c not in df.columns]
        assert not missing, f"Missing canonical cols in on-disk file: {missing}"

    def test_current_file_has_no_legacy_cols(self):
        path = Path("outputs/metrics/report_features.csv")
        if not path.exists():
            pytest.skip("report_features.csv not yet generated")
        df = pd.read_csv(path)
        present_legacy = [c for c in LEGACY_ONLY_COLS if c in df.columns]
        assert not present_legacy, f"Legacy cols still in file: {present_legacy}"

class TestLegacySchemaRejected:
    @pytest.mark.skipif(not HAS_VALIDATOR, reason="validator not importable")
    def test_legacy_fails_validator(self):
        df = _make_legacy()
        with pytest.raises((ValueError, KeyError)):
            validate_report_features_schema(df)

    def test_legacy_missing_canonical_cols(self):
        df = _make_legacy()
        missing = [c for c in CURRENT_CANONICAL_COLS if c not in df.columns]
        assert len(missing) > 0, "Legacy DataFrame unexpectedly has all canonical cols"
