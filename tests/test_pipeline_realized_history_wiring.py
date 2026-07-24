"""Tests proving run_production_pipeline calls update_realized_forecast_history.

These tests use monkeypatching and lightweight stubs to avoid running the full
pipeline.  They verify:
  - update_realized_forecast_history is called (not update_realized_errors)
  - realized_forecast_history.csv is created on disk
  - repeated calls do not duplicate rows (dedup)
  - future forecast dates remain unrealized after a run
  - zero-view actuals are written, not dropped
  - the pipeline return dict exposes realized_rows and n_realized_skipped
"""

from __future__ import annotations

import importlib
import sys
import types
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_PROD_COLS = [
    "run_id", "selection_run_id", "generated_at", "report_id",
    "training_start", "training_cutoff", "forecast_date", "horizon_step",
    "selected_model_family", "selected_model_name", "selected_m",
    "forecast", "lower_bound", "upper_bound", "model_order",
    "seasonal_order", "selection_reason", "valid_backtest_folds",
    "median_backtest_mase", "mean_backtest_wape", "mean_backtest_bias",
    "production_fit_status", "fallback_used", "fallback_reason",
]


def _prod_row(
    *,
    run_id: str = "run_A",
    report_id: str = "r1",
    forecast_date: str | pd.Timestamp = "2024-02-01",
    horizon_step: int = 1,
    forecast: float = 100.0,
    training_cutoff: str | pd.Timestamp = "2024-01-31",
    generated_at: pd.Timestamp | None = None,
    report_name: str = "Report One",
) -> dict:
    generated_at = generated_at or pd.Timestamp("2024-02-01")
    return {
        "run_id": run_id,
        "selection_run_id": run_id,
        "generated_at": generated_at,
        "report_id": report_id,
        "report_name": report_name,
        "training_start": pd.Timestamp("2023-01-01"),
        "training_cutoff": pd.Timestamp(training_cutoff),
        "forecast_date": pd.Timestamp(forecast_date),
        "horizon_step": horizon_step,
        "selected_model_family": "seasonal_naive",
        "selected_model_name": "seasonal_naive_m7",
        "selected_m": 7,
        "forecast": forecast,
        "lower_bound": np.nan,
        "upper_bound": np.nan,
        "model_order": None,
        "seasonal_order": None,
        "selection_reason": "best_mase",
        "valid_backtest_folds": 4,
        "median_backtest_mase": 0.9,
        "mean_backtest_wape": 0.1,
        "mean_backtest_bias": 0.0,
        "production_fit_status": "ok",
        "fallback_used": False,
        "fallback_reason": None,
    }


def _actuals_df(
    report_id: str,
    dates: list[str],
    views: list[float],
) -> pd.DataFrame:
    return pd.DataFrame(
        {"date": pd.to_datetime(dates), "report_id": report_id, "daily_views": views}
    )


def _write_prod_history(project_root: Path, rows: list[dict]) -> Path:
    out_dir = project_root / "outputs" / "forecasts"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "production_forecasts_history.csv"
    df = pd.DataFrame(rows)
    for col in _PROD_COLS:
        if col not in df.columns:
            df[col] = None
    df.to_csv(path, index=False)
    return path


def _realized_history_path(project_root: Path) -> Path:
    return project_root / "outputs" / "metrics" / "realized_forecast_history.csv"


def _ensure_output_dirs(project_root: Path) -> None:
    (project_root / "outputs" / "forecasts").mkdir(parents=True, exist_ok=True)
    (project_root / "outputs" / "metrics").mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Stub for run_production_pipeline's heavy dependencies
# ---------------------------------------------------------------------------

def _make_minimal_pipeline_stubs(
    tmp_path: Path,
    daily_series: pd.DataFrame,
    production_df: pd.DataFrame,
    *,
    run_id: str = "run_test",
) -> dict:
    """Return a dict of keyword arguments for patch() calls."""
    generated_at = pd.Timestamp("2024-02-01")

    selection = pd.DataFrame(
        [{"report_id": "r1", "selection_status": "selected"}]
    )
    data_diag: list = []

    return {
        "generated_at": generated_at,
        "run_id": run_id,
        "daily_series": daily_series,
        "production_df": production_df,
        "selection": selection,
        "data_diag": data_diag,
    }


# ---------------------------------------------------------------------------
# TestWiringCallsUpdateRealizedForecastHistory
# ---------------------------------------------------------------------------


class TestWiringCallsUpdateRealizedForecastHistory:
    """Prove that run_production_pipeline uses update_realized_forecast_history."""

    def _import_module(self):
        mod_name = "src.pipelines.run_forecasting_pipeline"
        if mod_name in sys.modules:
            return importlib.reload(sys.modules[mod_name])
        return importlib.import_module(mod_name)

    def test_update_realized_forecast_history_is_called(self, tmp_path):
        """update_realized_forecast_history must be invoked during pipeline run."""
        _ensure_output_dirs(tmp_path)
        daily_series = _actuals_df("r1", ["2024-02-01"], [50.0])
        prod_df = pd.DataFrame([_prod_row()])
        _write_prod_history(tmp_path, [_prod_row()])

        rfh_mod = importlib.import_module("src.models.realized_forecast_history")
        called_with = {}

        def fake_update(raw_actuals_df, project_root, *, realized_at=None):
            called_with["raw_actuals_df"] = raw_actuals_df
            called_with["project_root"] = project_root
            called_with["realized_at"] = realized_at
            return pd.DataFrame(), 0

        import src.pipelines.run_forecasting_pipeline as pipe_mod

        with patch.object(rfh_mod, "update_realized_forecast_history", fake_update), \
             patch("src.pipelines.run_forecasting_pipeline.get_project_root", return_value=tmp_path), \
             patch("src.pipelines.run_forecasting_pipeline.load_forecast_feature_input",
                   return_value=(daily_series, daily_series, tmp_path / "dummy.csv")), \
             patch("src.pipelines.run_forecasting_pipeline.run_data_quality_checks",
                   return_value=pd.DataFrame()), \
             patch("src.pipelines.run_forecasting_pipeline.build_daily_series_for_all_reports",
                   return_value=({}, {}, {})), \
             patch("src.pipelines.run_forecasting_pipeline.filter_by_data_criteria",
                   return_value=([], [])), \
             patch("src.pipelines.run_forecasting_pipeline.run_candidate_backtest_stage",
                   return_value=(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
                                 pd.DataFrame(columns=["report_id", "selection_status"]))), \
             patch("src.models.production_forecast.build_production_forecast",
                   return_value=prod_df), \
             patch("src.pipelines.run_forecasting_pipeline.save_production_outputs",
                   return_value={}):
            pipe_mod.run_production_pipeline()

        assert called_with, "update_realized_forecast_history was never called"
        assert "raw_actuals_df" in called_with

    def test_update_realized_errors_not_called_in_production_path(self, tmp_path):
        """update_realized_errors must NOT be called by run_production_pipeline."""
        _ensure_output_dirs(tmp_path)
        daily_series = _actuals_df("r1", ["2024-02-01"], [50.0])
        prod_df = pd.DataFrame([_prod_row()])

        update_realized_errors_calls = []

        import src.pipelines.run_forecasting_pipeline as pipe_mod
        original_ure = pipe_mod.update_realized_errors

        def spy_ure(*args, **kwargs):
            update_realized_errors_calls.append((args, kwargs))
            return original_ure(*args, **kwargs)

        rfh_mod = importlib.import_module("src.models.realized_forecast_history")

        with patch.object(rfh_mod, "update_realized_forecast_history",
                          return_value=(pd.DataFrame(), 0)), \
             patch.object(pipe_mod, "update_realized_errors", spy_ure), \
             patch("src.pipelines.run_forecasting_pipeline.get_project_root", return_value=tmp_path), \
             patch("src.pipelines.run_forecasting_pipeline.load_forecast_feature_input",
                   return_value=(daily_series, daily_series, tmp_path / "dummy.csv")), \
             patch("src.pipelines.run_forecasting_pipeline.run_data_quality_checks",
                   return_value=pd.DataFrame()), \
             patch("src.pipelines.run_forecasting_pipeline.build_daily_series_for_all_reports",
                   return_value=({}, {}, {})), \
             patch("src.pipelines.run_forecasting_pipeline.filter_by_data_criteria",
                   return_value=([], [])), \
             patch("src.pipelines.run_forecasting_pipeline.run_candidate_backtest_stage",
                   return_value=(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
                                 pd.DataFrame(columns=["report_id", "selection_status"]))), \
             patch("src.models.production_forecast.build_production_forecast",
                   return_value=prod_df), \
             patch("src.pipelines.run_forecasting_pipeline.save_production_outputs",
                   return_value={}):
            pipe_mod.run_production_pipeline()

        assert update_realized_errors_calls == [], (
            "update_realized_errors should NOT be called from run_production_pipeline"
        )


# ---------------------------------------------------------------------------
# TestRealizedHistoryFileCreation
# ---------------------------------------------------------------------------


class TestRealizedHistoryFileCreation:
    """Integration-style: write real files via update_realized_forecast_history."""

    def test_realized_forecast_history_csv_created(self, tmp_path):
        """After pipeline run, realized_forecast_history.csv must exist."""
        _ensure_output_dirs(tmp_path)
        t_cutoff = pd.Timestamp("2024-01-31")
        t_forecast = pd.Timestamp("2024-02-01")
        row = _prod_row(
            training_cutoff=str(t_cutoff.date()),
            forecast_date=str(t_forecast.date()),
        )
        _write_prod_history(tmp_path, [row])

        actuals = _actuals_df("r1", ["2024-02-01"], [55.0])

        from src.models.realized_forecast_history import update_realized_forecast_history
        realized_rows, _ = update_realized_forecast_history(
            raw_actuals_df=actuals,
            project_root=tmp_path,
            realized_at=pd.Timestamp("2024-02-02"),
        )

        out_path = _realized_history_path(tmp_path)
        assert out_path.exists(), "realized_forecast_history.csv was not created"
        written = pd.read_csv(out_path)
        assert len(written) == 1
        assert written["actual"].iloc[0] == pytest.approx(55.0)

    def test_repeated_runs_do_not_duplicate_rows(self, tmp_path):
        """Calling update_realized_forecast_history twice must not duplicate rows."""
        _ensure_output_dirs(tmp_path)
        row = _prod_row(forecast_date="2024-02-01", training_cutoff="2024-01-31")
        _write_prod_history(tmp_path, [row])
        actuals = _actuals_df("r1", ["2024-02-01"], [55.0])

        from src.models.realized_forecast_history import update_realized_forecast_history

        update_realized_forecast_history(actuals, tmp_path,
                                         realized_at=pd.Timestamp("2024-02-02"))
        update_realized_forecast_history(actuals, tmp_path,
                                         realized_at=pd.Timestamp("2024-02-02"))

        written = pd.read_csv(_realized_history_path(tmp_path))
        assert len(written) == 1, (
            f"Expected 1 row after 2 runs, got {len(written)}"
        )

    def test_future_forecast_dates_remain_unrealized(self, tmp_path):
        """Forecast dates without matching actuals must not appear in history."""
        _ensure_output_dirs(tmp_path)
        past_row = _prod_row(forecast_date="2024-02-01", training_cutoff="2024-01-31")
        future_row = _prod_row(
            forecast_date="2025-12-31",
            training_cutoff="2024-01-31",
            horizon_step=2,
        )
        _write_prod_history(tmp_path, [past_row, future_row])
        actuals = _actuals_df("r1", ["2024-02-01"], [55.0])

        from src.models.realized_forecast_history import update_realized_forecast_history

        realized_rows, n_skipped = update_realized_forecast_history(
            actuals, tmp_path, realized_at=pd.Timestamp("2024-02-02")
        )

        written = pd.read_csv(_realized_history_path(tmp_path))
        assert len(written) == 1, (
            "Only the past row should be realized; future row must not appear"
        )
        assert pd.to_datetime(written["forecast_date"].iloc[0]).date() == pd.Timestamp(
            "2024-02-01"
        ).date()

    def test_zero_view_actuals_preserved(self, tmp_path):
        """Zero actual views must be written to history, not treated as missing."""
        _ensure_output_dirs(tmp_path)
        row = _prod_row(forecast_date="2024-02-01", training_cutoff="2024-01-31")
        _write_prod_history(tmp_path, [row])
        actuals = _actuals_df("r1", ["2024-02-01"], [0.0])

        from src.models.realized_forecast_history import update_realized_forecast_history

        realized_rows, _ = update_realized_forecast_history(
            actuals, tmp_path, realized_at=pd.Timestamp("2024-02-02")
        )

        written = pd.read_csv(_realized_history_path(tmp_path))
        assert len(written) == 1
        assert written["actual"].iloc[0] == pytest.approx(0.0)
        # signed_error = forecast - actual = 100 - 0 = 100
        assert written["signed_error"].iloc[0] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# TestReturnDictKeys
# ---------------------------------------------------------------------------


class TestReturnDictKeys:
    """run_production_pipeline return dict must expose realized history keys."""

    def test_return_dict_exposes_realized_rows_and_n_skipped(self, tmp_path):
        """Return dict must have realized_rows (DataFrame) and n_realized_skipped (int)."""
        _ensure_output_dirs(tmp_path)
        daily_series = _actuals_df("r1", ["2024-02-01"], [50.0])
        prod_df = pd.DataFrame([_prod_row()])
        realized_df = pd.DataFrame({"run_id": ["run_test"]})
        n_skipped = 3

        import src.pipelines.run_forecasting_pipeline as pipe_mod
        rfh_mod = importlib.import_module("src.models.realized_forecast_history")

        with patch.object(rfh_mod, "update_realized_forecast_history",
                          return_value=(realized_df, n_skipped)), \
             patch("src.pipelines.run_forecasting_pipeline.get_project_root", return_value=tmp_path), \
             patch("src.pipelines.run_forecasting_pipeline.load_forecast_feature_input",
                   return_value=(daily_series, daily_series, tmp_path / "dummy.csv")), \
             patch("src.pipelines.run_forecasting_pipeline.run_data_quality_checks",
                   return_value=pd.DataFrame()), \
             patch("src.pipelines.run_forecasting_pipeline.build_daily_series_for_all_reports",
                   return_value=({}, {}, {})), \
             patch("src.pipelines.run_forecasting_pipeline.filter_by_data_criteria",
                   return_value=([], [])), \
             patch("src.pipelines.run_forecasting_pipeline.run_candidate_backtest_stage",
                   return_value=(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
                                 pd.DataFrame(columns=["report_id", "selection_status"]))), \
             patch("src.models.production_forecast.build_production_forecast",
                   return_value=prod_df), \
             patch("src.pipelines.run_forecasting_pipeline.save_production_outputs",
                   return_value={}):
            result = pipe_mod.run_production_pipeline()

        assert "realized_rows" in result, "return dict must have 'realized_rows'"
        assert "n_realized_skipped" in result, "return dict must have 'n_realized_skipped'"
        assert isinstance(result["realized_rows"], pd.DataFrame)
        assert result["n_realized_skipped"] == n_skipped


# ---------------------------------------------------------------------------
# TestDeduplicationAcrossRuns
# ---------------------------------------------------------------------------


class TestDeduplicationAcrossRuns:
    """Multiple pipeline runs for the same report/date must not duplicate rows."""

    def test_two_runs_same_report_date_deduped(self, tmp_path):
        """Two pipeline runs producing forecasts for the same (report_id, forecast_date)
        result in exactly 2 rows (different run_ids)."""
        _ensure_output_dirs(tmp_path)
        actuals = _actuals_df("r1", ["2024-02-01"], [60.0])

        row_a = _prod_row(run_id="run_A", forecast_date="2024-02-01",
                          training_cutoff="2024-01-31")
        row_b = _prod_row(run_id="run_B", forecast_date="2024-02-01",
                          training_cutoff="2024-01-31")

        from src.models.realized_forecast_history import update_realized_forecast_history

        # Run A: writes prod history with only run_A's row
        _write_prod_history(tmp_path, [row_a])
        update_realized_forecast_history(actuals, tmp_path,
                                         realized_at=pd.Timestamp("2024-02-02"))

        # Run B: appends run_B row to prod history, calls again
        _write_prod_history(tmp_path, [row_a, row_b])
        update_realized_forecast_history(actuals, tmp_path,
                                         realized_at=pd.Timestamp("2024-02-03"))

        written = pd.read_csv(_realized_history_path(tmp_path))
        # Unique key is (run_id, report_id, forecast_date) so expect 2 rows
        assert len(written) == 2, (
            f"Expected 2 rows (one per run_id), got {len(written)}"
        )
        assert set(written["run_id"]) == {"run_A", "run_B"}
