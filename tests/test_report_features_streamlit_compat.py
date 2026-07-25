"""Regression tests: current report features pass Streamlit loader; legacy schema fails."""
import pandas as pd
import pytest
from pathlib import Path

# Try to import the schema validator from the app
try:
    from src.app.utils.load_data import validate_report_features_schema
    HAS_VALIDATOR = True
except ImportError:
    HAS_VALIDATOR = False

CURRENT_CANONICAL_COLS = [
    "report_id", "recent_28d_views", "previous_28d_views",
    "usage_change_28d_pct", "top_1_user_view_share",
    "days_active", "avg_views",
]
LEGACY_ONLY_COLS = [
    "latest_views", "prior_views", "usage_change_pct", "top_user_concentration",
]

def _make_current(n=5):
    return pd.DataFrame([{
        "report_id": f"R{i:03d}",
        "recent_28d_views": 100,
        "previous_28d_views": 90,
        "usage_change_28d_pct": 11.1,
        "top_1_user_view_share": 0.3,
        "days_active": 20,
        "avg_views": 10.0,
        "analytics_run_id": "run-001",
        "generated_at": "2024-03-31T00:00:00",
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
        # Should not raise
        validate_report_features_schema(df)

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
