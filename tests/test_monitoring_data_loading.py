"""Tests for monitoring data loading in src/app/utils/load_data.py.

Covers:
* All monitoring files present and valid
* No realized data yet (all monitoring files absent)
* Partial monitoring outputs (some files present, some absent)
* Invalid schema detection (file present but missing required columns)
* Zero-view actual records preserved
* selected_m and model lineage columns loaded correctly
* Production forecast file preference over legacy
* Silent fallback when production file is absent
* Raise when production file is present with invalid schema
"""

from __future__ import annotations

import csv
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.app.utils.load_data import (
    MONITORING_PATHS,
    REQUIRED_COLS_PERF_BY_HORIZON,
    REQUIRED_COLS_PERF_BY_MODEL,
    REQUIRED_COLS_PERF_BY_REPORT,
    REQUIRED_COLS_PERF_BY_RUN,
    REQUIRED_COLS_PRODUCTION_FORECAST,
    REQUIRED_COLS_REALIZED_ERRORS,
    check_monitoring_file,
    load_app_data,
    load_monitoring_data,
)


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------

def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _prod_forecast_row(**overrides) -> dict:
    base = {
        "run_id":                "run_001",
        "selection_run_id":      "run_001",
        "generated_at":          "2024-01-28",
        "report_id":             "R1",
        "training_start":        "2023-01-01",
        "training_cutoff":       "2023-12-31",
        "forecast_date":         "2024-01-29",
        "horizon_step":          1,
        "selected_model_family": "seasonal_naive",
        "selected_model_name":   "seasonal_naive_m7",
        "selected_m":            7,
        "forecast":              42.0,
        "lower_bound":           35.0,
        "upper_bound":           49.0,
        "production_fit_status": "ok",
    }
    base.update(overrides)
    return base


def _perf_by_run_row(**overrides) -> dict:
    base = {
        "run_id":                          "run_001",
        "generated_at":                    "2024-01-28",
        "training_cutoff":                 "2023-12-31",
        "realized_prediction_count":       28,
        "expected_prediction_count":       28,
        "realization_rate":                1.0,
        "fully_realized_report_count":     2,
        "partially_realized_report_count": 0,
        "mae":                             3.1,
        "rmse":                            4.2,
        "wape":                            0.12,
        "bias":                            0.5,
        "absolute_bias":                   0.5,
        "interval_coverage":               0.90,
        "mean_interval_width":             14.0,
    }
    base.update(overrides)
    return base


def _perf_by_report_row(**overrides) -> dict:
    base = {
        "report_id":               "R1",
        "production_run_count":    1,
        "realized_prediction_count": 28,
        "mae":                     3.1,
        "rmse":                    4.2,
        "wape":                    0.12,
        "bias":                    0.5,
        "absolute_bias":           0.5,
        "interval_coverage":       0.90,
        "mean_interval_width":     14.0,
        "recent_wape":             0.12,
        "previous_wape":           np.nan,
        "accuracy_change":         np.nan,
        "monitoring_status":       "insufficient_data",
    }
    base.update(overrides)
    return base


def _perf_by_horizon_row(**overrides) -> dict:
    base = {
        "report_id":        "R1",
        "horizon_bucket":   "days_1_7",
        "observation_count": 7,
        "mae":              2.5,
        "rmse":             3.1,
        "wape":             0.10,
        "bias":             0.2,
        "interval_coverage": 0.92,
        "mean_interval_width": 12.0,
    }
    base.update(overrides)
    return base


def _perf_by_model_row(**overrides) -> dict:
    base = {
        "selected_model_family":   "seasonal_naive",
        "selected_m":              7,
        "report_count":            2,
        "production_run_count":    1,
        "realized_prediction_count": 56,
        "mae":                     3.1,
        "rmse":                    4.2,
        "wape":                    0.12,
        "bias":                    0.5,
        "interval_coverage":       0.90,
        "mean_interval_width":     14.0,
    }
    base.update(overrides)
    return base


def _realized_errors_row(**overrides) -> dict:
    base = {
        "report_id":    "R1",
        "forecast_date": "2024-01-29",
        "actual":        40.0,
        "forecast":      42.0,
    }
    base.update(overrides)
    return base


def _write_all_monitoring_files(base: Path) -> None:
    """Write a minimal but valid set of all seven monitoring files."""
    _write_csv(
        base / MONITORING_PATHS["production_forecasts_latest"],
        [_prod_forecast_row()],
    )
    _write_csv(
        base / MONITORING_PATHS["production_forecasts_history"],
        [_prod_forecast_row()],
    )
    _write_csv(
        base / MONITORING_PATHS["realized_errors_history"],
        [_realized_errors_row()],
    )
    _write_csv(
        base / MONITORING_PATHS["perf_by_run"],
        [_perf_by_run_row()],
    )
    _write_csv(
        base / MONITORING_PATHS["perf_by_report"],
        [_perf_by_report_row()],
    )
    _write_csv(
        base / MONITORING_PATHS["perf_by_horizon"],
        [_perf_by_horizon_row()],
    )
    _write_csv(
        base / MONITORING_PATHS["perf_by_model"],
        [_perf_by_model_row()],
    )


# ---------------------------------------------------------------------------
# TestCheckMonitoringFile
# ---------------------------------------------------------------------------

class TestCheckMonitoringFile:
    def test_absent_file_returns_absent(self, tmp_path):
        path = tmp_path / "nonexistent.csv"
        df, status = check_monitoring_file(path, {"run_id"})
        assert status == "absent"
        assert df.empty

    def test_empty_file_returns_empty(self, tmp_path):
        path = tmp_path / "empty.csv"
        path.write_text("")
        df, status = check_monitoring_file(path, {"run_id"})
        assert status == "empty"
        assert df.empty

    def test_valid_file_returns_ok(self, tmp_path):
        path = tmp_path / "valid.csv"
        _write_csv(path, [{"run_id": "r1", "wape": 0.1}])
        df, status = check_monitoring_file(path, {"run_id", "wape"})
        assert status == "ok"
        assert len(df) == 1

    def test_missing_col_returns_invalid_schema(self, tmp_path):
        path = tmp_path / "bad.csv"
        _write_csv(path, [{"run_id": "r1"}])  # missing "wape"
        df, status = check_monitoring_file(path, {"run_id", "wape"})
        assert status == "invalid_schema"
        assert not df.empty  # raw rows are still returned

    def test_zero_actual_rows_preserved(self, tmp_path):
        path = tmp_path / "zeros.csv"
        _write_csv(path, [
            {"report_id": "R1", "forecast_date": "2024-01-01", "actual": 0.0, "forecast": 5.0},
            {"report_id": "R1", "forecast_date": "2024-01-02", "actual": 0.0, "forecast": 3.0},
        ])
        df, status = check_monitoring_file(path, REQUIRED_COLS_REALIZED_ERRORS)
        assert status == "ok"
        assert len(df) == 2
        assert (df["actual"] == 0.0).all()


# ---------------------------------------------------------------------------
# TestLoadMonitoringData — all files present
# ---------------------------------------------------------------------------

class TestLoadMonitoringDataAllPresent:
    def test_all_keys_in_result(self, tmp_path):
        _write_all_monitoring_files(tmp_path)
        result = load_monitoring_data(tmp_path)
        assert set(result.keys()) == set(MONITORING_PATHS.keys())

    def test_all_statuses_ok(self, tmp_path):
        _write_all_monitoring_files(tmp_path)
        result = load_monitoring_data(tmp_path)
        for key, entry in result.items():
            assert entry["status"] == "ok", (
                f"Expected status 'ok' for '{key}', got '{entry['status']}'"
            )

    def test_data_frames_non_empty(self, tmp_path):
        _write_all_monitoring_files(tmp_path)
        result = load_monitoring_data(tmp_path)
        for key, entry in result.items():
            assert not entry["data"].empty, f"Expected non-empty DataFrame for '{key}'"

    def test_no_missing_cols_reported(self, tmp_path):
        _write_all_monitoring_files(tmp_path)
        result = load_monitoring_data(tmp_path)
        for key, entry in result.items():
            assert entry["missing_cols"] == [], (
                f"'{key}' reported missing columns: {entry['missing_cols']}"
            )

    def test_path_reported_per_entry(self, tmp_path):
        _write_all_monitoring_files(tmp_path)
        result = load_monitoring_data(tmp_path)
        for key, entry in result.items():
            assert "path" in entry
            assert isinstance(entry["path"], Path)


# ---------------------------------------------------------------------------
# TestLoadMonitoringData — no realized data (all absent)
# ---------------------------------------------------------------------------

class TestLoadMonitoringDataNoRealizedData:
    def test_all_statuses_absent(self, tmp_path):
        result = load_monitoring_data(tmp_path)
        for key, entry in result.items():
            assert entry["status"] == "absent", (
                f"Expected 'absent' for '{key}', got '{entry['status']}'"
            )

    def test_all_data_frames_empty(self, tmp_path):
        result = load_monitoring_data(tmp_path)
        for key, entry in result.items():
            assert entry["data"].empty

    def test_no_missing_cols_when_absent(self, tmp_path):
        result = load_monitoring_data(tmp_path)
        for key, entry in result.items():
            assert entry["missing_cols"] == []


# ---------------------------------------------------------------------------
# TestLoadMonitoringData — partial monitoring outputs
# ---------------------------------------------------------------------------

class TestPartialMonitoringOutputs:
    def test_perf_by_run_absent_others_ok(self, tmp_path):
        """Write all files except perf_by_run; that one must be 'absent'."""
        _write_all_monitoring_files(tmp_path)
        (tmp_path / MONITORING_PATHS["perf_by_run"]).unlink()

        result = load_monitoring_data(tmp_path)
        assert result["perf_by_run"]["status"] == "absent"
        assert result["production_forecasts_latest"]["status"] == "ok"
        assert result["perf_by_report"]["status"] == "ok"

    def test_only_production_forecast_present(self, tmp_path):
        _write_csv(
            tmp_path / MONITORING_PATHS["production_forecasts_latest"],
            [_prod_forecast_row()],
        )
        result = load_monitoring_data(tmp_path)
        assert result["production_forecasts_latest"]["status"] == "ok"
        for key in ["perf_by_run", "perf_by_report", "perf_by_horizon", "perf_by_model"]:
            assert result[key]["status"] == "absent"

    def test_empty_perf_by_horizon(self, tmp_path):
        """An empty file (pipeline ran but wrote nothing) must be status 'empty'."""
        path = tmp_path / MONITORING_PATHS["perf_by_horizon"]
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("")
        result = load_monitoring_data(tmp_path)
        assert result["perf_by_horizon"]["status"] == "empty"


# ---------------------------------------------------------------------------
# TestInvalidSchema
# ---------------------------------------------------------------------------

class TestInvalidSchema:
    def test_perf_by_run_missing_wape(self, tmp_path):
        path = tmp_path / MONITORING_PATHS["perf_by_run"]
        _write_csv(path, [{"run_id": "r1", "mae": 3.0}])  # missing wape, bias, realization_rate, realized_prediction_count
        result = load_monitoring_data(tmp_path)
        assert result["perf_by_run"]["status"] == "invalid_schema"
        assert "wape" in result["perf_by_run"]["missing_cols"]

    def test_perf_by_report_missing_monitoring_status(self, tmp_path):
        path = tmp_path / MONITORING_PATHS["perf_by_report"]
        _write_csv(path, [{"report_id": "R1", "wape": 0.1}])
        result = load_monitoring_data(tmp_path)
        assert result["perf_by_report"]["status"] == "invalid_schema"
        assert "monitoring_status" in result["perf_by_report"]["missing_cols"]

    def test_perf_by_horizon_missing_horizon_bucket(self, tmp_path):
        path = tmp_path / MONITORING_PATHS["perf_by_horizon"]
        _write_csv(path, [{"report_id": "R1", "wape": 0.1, "observation_count": 7}])
        result = load_monitoring_data(tmp_path)
        assert result["perf_by_horizon"]["status"] == "invalid_schema"
        assert "horizon_bucket" in result["perf_by_horizon"]["missing_cols"]

    def test_perf_by_model_missing_selected_m(self, tmp_path):
        path = tmp_path / MONITORING_PATHS["perf_by_model"]
        _write_csv(path, [{"selected_model_family": "naive", "wape": 0.1}])
        result = load_monitoring_data(tmp_path)
        assert result["perf_by_model"]["status"] == "invalid_schema"
        assert "selected_m" in result["perf_by_model"]["missing_cols"]

    def test_raw_df_still_returned_for_invalid_schema(self, tmp_path):
        """The raw DataFrame is returned even on invalid schema so the UI can diagnose."""
        path = tmp_path / MONITORING_PATHS["perf_by_run"]
        _write_csv(path, [{"run_id": "r1", "mae": 3.0}])
        result = load_monitoring_data(tmp_path)
        assert not result["perf_by_run"]["data"].empty
        assert "run_id" in result["perf_by_run"]["data"].columns


# ---------------------------------------------------------------------------
# TestZeroViewActualsPreserved
# ---------------------------------------------------------------------------

class TestZeroViewActualsPreserved:
    def test_zero_actual_in_realized_errors(self, tmp_path):
        path = tmp_path / MONITORING_PATHS["realized_errors_history"]
        _write_csv(path, [
            _realized_errors_row(actual=0.0, forecast=3.0),
            _realized_errors_row(actual=10.0, forecast=9.0),
        ])
        result = load_monitoring_data(tmp_path)
        df = result["realized_errors_history"]["data"]
        assert result["realized_errors_history"]["status"] == "ok"
        assert len(df) == 2
        assert (df["actual"] == 0.0).any()

    def test_zero_actual_in_production_forecast(self, tmp_path):
        path = tmp_path / MONITORING_PATHS["production_forecasts_latest"]
        _write_csv(path, [
            _prod_forecast_row(forecast=0.0),
            _prod_forecast_row(report_id="R2", forecast=5.0),
        ])
        result = load_monitoring_data(tmp_path)
        df = result["production_forecasts_latest"]["data"]
        assert len(df) == 2
        assert (df["forecast"] == 0.0).any()


# ---------------------------------------------------------------------------
# TestModelLineageLoaded
# ---------------------------------------------------------------------------

class TestModelLineageLoaded:
    def test_selected_m_in_production_forecast(self, tmp_path):
        _write_csv(
            tmp_path / MONITORING_PATHS["production_forecasts_latest"],
            [
                _prod_forecast_row(selected_model_family="seasonal_naive", selected_m=7),
                _prod_forecast_row(report_id="R2", selected_model_family="auto_arima", selected_m=14),
            ],
        )
        result = load_monitoring_data(tmp_path)
        df = result["production_forecasts_latest"]["data"]
        assert "selected_m" in df.columns
        assert set(df["selected_m"].astype(int)) == {7, 14}

    def test_selected_model_family_in_perf_by_model(self, tmp_path):
        _write_csv(
            tmp_path / MONITORING_PATHS["perf_by_model"],
            [
                _perf_by_model_row(selected_model_family="seasonal_naive", selected_m=7),
                _perf_by_model_row(selected_model_family="auto_arima",    selected_m=1),
            ],
        )
        result = load_monitoring_data(tmp_path)
        df = result["perf_by_model"]["data"]
        assert "selected_model_family" in df.columns
        assert "selected_m" in df.columns
        assert set(df["selected_model_family"]) == {"seasonal_naive", "auto_arima"}

    def test_model_lineage_in_prod_forecast_history(self, tmp_path):
        _write_csv(
            tmp_path / MONITORING_PATHS["production_forecasts_history"],
            [
                _prod_forecast_row(run_id="run_001", selected_model_name="seasonal_naive_m7"),
                _prod_forecast_row(run_id="run_002", selected_model_name="auto_arima_m14"),
            ],
        )
        result = load_monitoring_data(tmp_path)
        df = result["production_forecasts_history"]["data"]
        assert "selected_model_name" in df.columns
        assert set(df["selected_model_name"]) == {"seasonal_naive_m7", "auto_arima_m14"}


# ---------------------------------------------------------------------------
# TestLoadAppDataIntegration
# ---------------------------------------------------------------------------

class TestLoadAppDataIntegration:
    def test_monitoring_keys_in_data(self, tmp_path):
        """load_app_data includes all monitoring keys in the returned dict."""
        _write_all_monitoring_files(tmp_path)
        data = load_app_data(tmp_path)
        for key in MONITORING_PATHS:
            assert key in data, f"Expected '{key}' in data"

    def test_monitoring_meta_key_present(self, tmp_path):
        """data['_monitoring'] holds the structured monitoring result."""
        data = load_app_data(tmp_path)
        assert "_monitoring" in data
        assert isinstance(data["_monitoring"], dict)

    def test_production_forecast_preferred_over_legacy(self, tmp_path):
        """When production_forecasts_latest is valid, data['forecasts'] uses it."""
        _write_csv(
            tmp_path / MONITORING_PATHS["production_forecasts_latest"],
            [_prod_forecast_row(run_id="prod_run_001")],
        )
        # Also write legacy file so fall-through would be possible
        legacy_path = tmp_path / "outputs" / "forecasts" / "report_view_forecasts_latest.csv"
        _write_csv(legacy_path, [{"Date": "2024-01-01", "ReportId": "R1", "forecast": 9.9}])

        data = load_app_data(tmp_path)
        # The production run_id only appears in the production file
        assert "run_id" in data["forecasts"].columns
        assert "prod_run_001" in data["forecasts"]["run_id"].values

    def test_legacy_used_when_production_absent(self, tmp_path):
        """When production file is absent, legacy data['forecasts'] is used."""
        legacy_path = tmp_path / "outputs" / "forecasts" / "report_view_forecasts_latest.csv"
        _write_csv(legacy_path, [{"Date": "2024-01-01", "ReportId": "R1", "forecast": 9.9}])
        data = load_app_data(tmp_path)
        assert not data["forecasts"].empty

    def test_raises_when_production_forecast_has_invalid_schema(self, tmp_path):
        """If production file exists but has bad schema, ValueError is raised immediately."""
        bad_path = tmp_path / MONITORING_PATHS["production_forecasts_latest"]
        _write_csv(bad_path, [{"run_id": "r1", "bad_col": 1}])
        with pytest.raises(ValueError, match="production_forecasts_latest"):
            load_app_data(tmp_path)

    def test_error_message_names_missing_cols(self, tmp_path):
        bad_path = tmp_path / MONITORING_PATHS["production_forecasts_latest"]
        _write_csv(bad_path, [{"run_id": "r1"}])
        with pytest.raises(ValueError) as exc_info:
            load_app_data(tmp_path)
        # At least one of the missing required columns must be named in the error
        msg = str(exc_info.value)
        assert any(col in msg for col in REQUIRED_COLS_PRODUCTION_FORECAST - {"run_id"})

    def test_empty_production_forecast_falls_back_to_legacy(self, tmp_path):
        """An empty production file does not block the app (status 'empty')."""
        prod_path = tmp_path / MONITORING_PATHS["production_forecasts_latest"]
        prod_path.parent.mkdir(parents=True, exist_ok=True)
        prod_path.write_text("")

        legacy_path = tmp_path / "outputs" / "forecasts" / "report_view_forecasts_latest.csv"
        _write_csv(legacy_path, [{"Date": "2024-01-01", "ReportId": "R1", "forecast": 9.9}])

        data = load_app_data(tmp_path)
        # No exception, legacy data still available
        assert not data["forecasts"].empty

    def test_all_monitoring_absent_does_not_raise(self, tmp_path):
        """When no monitoring files exist, load_app_data must not raise."""
        data = load_app_data(tmp_path)  # only tmp_path, no monitoring files
        monitoring = data["_monitoring"]
        for key in MONITORING_PATHS:
            assert monitoring[key]["status"] == "absent"
