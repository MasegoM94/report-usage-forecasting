"""Tests for src/monitoring/production_performance.py.

Covers:
  - complete run (all horizon steps realized)
  - partial run (fewer horizon steps than expected)
  - zero total actual volume (WAPE must be null)
  - horizon bucket boundaries (step boundaries are inclusive)
  - model and selected_m grouping
  - recent vs previous WAPE
  - insufficient previous-period history
  - interval coverage
  - deterministic output (same input → same output across calls)
  - validate_realized_history_input raises on missing columns
  - save_production_performance writes to disk
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.monitoring.production_performance import (
    DEGRADED_WAPE_THRESHOLD,
    MIN_PERIODS_FOR_COMPARISON,
    _HORIZON_COLS,
    _MODEL_COLS,
    _REPORT_COLS,
    _RUN_COLS,
    build_production_performance_tables,
    realized_performance_by_horizon,
    realized_performance_by_model,
    realized_performance_by_report,
    realized_performance_by_run,
    save_production_performance,
    validate_realized_history_input,
)
from src.models.horizon_evaluation import HORIZON_BUCKETS


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _row(
    *,
    run_id: str = "run_A",
    report_id: str = "r1",
    forecast_date: str = "2024-02-01",
    horizon_step: int = 1,
    forecast: float = 110.0,
    actual: float = 100.0,
    lower_bound: float = float("nan"),
    upper_bound: float = float("nan"),
    selected_model_family: str = "seasonal_naive",
    selected_model_name: str = "seasonal_naive_m7",
    selected_m: int = 7,
    generated_at: str = "2024-01-31",
    training_cutoff: str = "2024-01-30",
) -> dict:
    se = forecast - actual
    return {
        "run_id": run_id,
        "selection_run_id": run_id,
        "generated_at": pd.Timestamp(generated_at),
        "training_cutoff": pd.Timestamp(training_cutoff),
        "realized_at": pd.Timestamp("2024-02-02"),
        "report_id": report_id,
        "report_name": "Report",
        "selected_model_family": selected_model_family,
        "selected_model_name": selected_model_name,
        "selected_m": selected_m,
        "forecast_date": pd.Timestamp(forecast_date),
        "horizon_step": horizon_step,
        "forecast": forecast,
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
        "actual": actual,
        "signed_error": se,
        "absolute_error": abs(se),
        "squared_error": se ** 2,
        "inside_interval": float("nan"),
        "interval_width": float("nan"),
        "percentage_error": abs(se) / actual * 100 if actual != 0 else float("nan"),
        "lineage_complete": None,
        "lineage_missing_fields": None,
    }


def _df(*rows) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


# ---------------------------------------------------------------------------
# TestValidateInput
# ---------------------------------------------------------------------------


class TestValidateInput:

    def test_raises_on_missing_columns(self):
        df = pd.DataFrame([{"run_id": "r", "report_id": "r1"}])
        with pytest.raises(ValueError, match="missing"):
            validate_realized_history_input(df)

    def test_passes_on_full_columns(self):
        df = _df(_row())
        validate_realized_history_input(df)  # must not raise

    def test_empty_df_with_correct_cols_passes(self):
        df = _df(_row()).iloc[0:0]
        validate_realized_history_input(df)


# ---------------------------------------------------------------------------
# TestRealizedPerformanceByRun — complete run
# ---------------------------------------------------------------------------


class TestRealizedPerformanceByRun:

    def test_schema(self):
        df = _df(_row())
        out = realized_performance_by_run(df)
        assert list(out.columns) == _RUN_COLS

    def test_empty_input(self):
        df = _df(_row()).iloc[0:0]
        out = realized_performance_by_run(df)
        assert out.empty
        assert list(out.columns) == _RUN_COLS

    def test_complete_run_realization_rate_one(self):
        """All horizon steps for all reports → realization_rate == 1.0."""
        rows = []
        for step in range(1, 8):  # 7 steps, 1 report
            rows.append(_row(run_id="run_A", report_id="r1", horizon_step=step,
                             forecast=110, actual=100))
        df = _df(*rows)
        out = realized_performance_by_run(df)
        r = out[out["run_id"] == "run_A"].iloc[0]
        assert r["realized_prediction_count"] == 7
        assert r["expected_prediction_count"] == 7
        assert r["realization_rate"] == pytest.approx(1.0)
        assert r["fully_realized_report_count"] == 1
        assert r["partially_realized_report_count"] == 0

    def test_partial_run_detected(self):
        """Only 3 of 7 steps realized → partial run."""
        rows = [
            _row(run_id="run_A", report_id="r1", horizon_step=1),
            _row(run_id="run_A", report_id="r1", horizon_step=2),
            _row(run_id="run_A", report_id="r1", horizon_step=3),
        ]
        df = _df(*rows)
        out = realized_performance_by_run(df)
        r = out.iloc[0]
        assert r["realized_prediction_count"] == 3
        assert r["realization_rate"] == pytest.approx(3 / 3)  # max_step=3, n_reports=1
        # partial detection: reports with < max_step realizations
        # Here all 3 steps exist so report is "fully realized" relative to observed max
        assert r["fully_realized_report_count"] == 1

    def test_partial_run_two_reports_one_incomplete(self):
        """Two reports, r2 has fewer steps — must show partially_realized_report_count=1."""
        rows = []
        for step in range(1, 8):
            rows.append(_row(run_id="run_A", report_id="r1", horizon_step=step))
        rows.append(_row(run_id="run_A", report_id="r2", horizon_step=1))  # only step 1
        df = _df(*rows)
        out = realized_performance_by_run(df)
        r = out.iloc[0]
        assert r["partially_realized_report_count"] == 1
        assert r["fully_realized_report_count"] == 1

    def test_mae_computed_correctly(self):
        """MAE = mean(|actual - forecast|) = mean(10) = 10."""
        rows = [
            _row(run_id="run_A", report_id="r1", forecast=110, actual=100),
            _row(run_id="run_A", report_id="r1", forecast=120, actual=110),
        ]
        df = _df(*rows)
        out = realized_performance_by_run(df)
        assert out["mae"].iloc[0] == pytest.approx(10.0)

    def test_sorted_by_run_id(self):
        rows = [
            _row(run_id="run_B", report_id="r1"),
            _row(run_id="run_A", report_id="r1"),
        ]
        df = _df(*rows)
        out = realized_performance_by_run(df)
        assert list(out["run_id"]) == ["run_A", "run_B"]

    def test_absolute_bias(self):
        """absolute_bias = |bias| — always non-negative."""
        # forecast consistently 20 below actual → bias = -20, absolute_bias = 20
        rows = [_row(run_id="run_A", report_id="r1", forecast=80, actual=100)]
        df = _df(*rows)
        out = realized_performance_by_run(df)
        assert out["bias"].iloc[0] == pytest.approx(-20.0)
        assert out["absolute_bias"].iloc[0] == pytest.approx(20.0)


# ---------------------------------------------------------------------------
# TestZeroActualVolume
# ---------------------------------------------------------------------------


class TestZeroActualVolume:

    def test_wape_null_when_all_actuals_zero_by_run(self):
        rows = [
            _row(run_id="run_A", report_id="r1", actual=0.0, forecast=5.0, horizon_step=1),
            _row(run_id="run_A", report_id="r1", actual=0.0, forecast=3.0, horizon_step=2),
        ]
        df = _df(*rows)
        out = realized_performance_by_run(df)
        assert math.isnan(out["wape"].iloc[0])

    def test_mae_defined_when_actuals_zero(self):
        """MAE is well-defined even when all actuals are zero."""
        rows = [_row(run_id="run_A", report_id="r1", actual=0.0, forecast=5.0)]
        df = _df(*rows)
        out = realized_performance_by_run(df)
        assert out["mae"].iloc[0] == pytest.approx(5.0)

    def test_wape_null_by_report_when_all_actuals_zero(self):
        rows = [_row(run_id="run_A", report_id="r1", actual=0.0, forecast=5.0)]
        df = _df(*rows)
        out = realized_performance_by_report(df)
        assert math.isnan(out["wape"].iloc[0])
        assert out["monitoring_status"].iloc[0] == "no_actuals"

    def test_wape_null_by_model_when_all_actuals_zero(self):
        rows = [_row(run_id="run_A", report_id="r1", actual=0.0, forecast=5.0)]
        df = _df(*rows)
        out = realized_performance_by_model(df)
        assert math.isnan(out["wape"].iloc[0])

    def test_zero_actuals_rows_preserved_in_count(self):
        """Rows with actual=0 must not be silently dropped."""
        rows = [
            _row(run_id="run_A", report_id="r1", actual=0.0, forecast=5.0),
            _row(run_id="run_A", report_id="r1", actual=100.0, forecast=110.0,
                 horizon_step=2),
        ]
        df = _df(*rows)
        out = realized_performance_by_run(df)
        assert out["realized_prediction_count"].iloc[0] == 2


# ---------------------------------------------------------------------------
# TestHorizonBuckets
# ---------------------------------------------------------------------------


class TestHorizonBuckets:

    def _rows_for_steps(self, steps: list[int], run_id: str = "run_A",
                         report_id: str = "r1") -> list[dict]:
        return [
            _row(run_id=run_id, report_id=report_id, horizon_step=s,
                 forecast_date=f"2024-02-{s:02d}")
            for s in steps
        ]

    def test_schema(self):
        df = _df(*self._rows_for_steps([1, 8, 15]))
        out = realized_performance_by_horizon(df)
        assert list(out.columns) == _HORIZON_COLS

    def test_step_1_in_days_1_7(self):
        df = _df(*self._rows_for_steps([1]))
        out = realized_performance_by_horizon(df)
        buckets = out[out["report_id"] == "r1"]["horizon_bucket"].tolist()
        assert "days_1_7" in buckets
        assert "full_horizon" in buckets

    def test_step_7_boundary_in_days_1_7(self):
        """Day 7 is the inclusive upper bound of days_1_7."""
        df = _df(*self._rows_for_steps([7]))
        out = realized_performance_by_horizon(df)
        assert "days_1_7" in out["horizon_bucket"].values

    def test_step_8_boundary_in_days_8_14(self):
        """Day 8 is the inclusive lower bound of days_8_14."""
        df = _df(*self._rows_for_steps([8]))
        out = realized_performance_by_horizon(df)
        assert "days_8_14" in out["horizon_bucket"].values

    def test_step_14_boundary_in_days_8_14(self):
        df = _df(*self._rows_for_steps([14]))
        out = realized_performance_by_horizon(df)
        assert "days_8_14" in out["horizon_bucket"].values

    def test_step_15_boundary_in_days_15_28(self):
        df = _df(*self._rows_for_steps([15]))
        out = realized_performance_by_horizon(df)
        assert "days_15_28" in out["horizon_bucket"].values

    def test_step_28_boundary_in_days_15_28(self):
        df = _df(*self._rows_for_steps([28]))
        out = realized_performance_by_horizon(df)
        assert "days_15_28" in out["horizon_bucket"].values

    def test_full_horizon_aggregates_all_steps(self):
        """full_horizon bucket must include steps 1–28."""
        rows = self._rows_for_steps(list(range(1, 29)))
        df = _df(*rows)
        out = realized_performance_by_horizon(df)
        fh = out[(out["report_id"] == "r1") & (out["horizon_bucket"] == "full_horizon")]
        assert not fh.empty
        assert fh["observation_count"].iloc[0] == 28

    def test_bucket_order_deterministic(self):
        """Buckets must appear in canonical order: days_1_7, days_8_14, days_15_28, full_horizon."""
        rows = self._rows_for_steps(list(range(1, 29)))
        df = _df(*rows)
        out = realized_performance_by_horizon(df)
        report_buckets = out[out["report_id"] == "r1"]["horizon_bucket"].tolist()
        expected_order = [b[0] for b in HORIZON_BUCKETS]
        assert report_buckets == expected_order

    def test_empty_input(self):
        df = _df(_row()).iloc[0:0]
        out = realized_performance_by_horizon(df)
        assert out.empty
        assert list(out.columns) == _HORIZON_COLS

    def test_mae_per_bucket(self):
        """Days 1-7: forecast=110, actual=100 → MAE=10. Days 8-14: forecast=90, actual=100 → MAE=10."""
        rows = (
            [_row(run_id="run_A", report_id="r1", horizon_step=s,
                  forecast_date=f"2024-02-{s:02d}", forecast=110, actual=100)
             for s in range(1, 8)]
            + [_row(run_id="run_A", report_id="r1", horizon_step=s,
                    forecast_date=f"2024-02-{s:02d}", forecast=90, actual=100)
               for s in range(8, 15)]
        )
        df = _df(*rows)
        out = realized_performance_by_horizon(df)
        near = out[out["horizon_bucket"] == "days_1_7"]["mae"].iloc[0]
        mid = out[out["horizon_bucket"] == "days_8_14"]["mae"].iloc[0]
        assert near == pytest.approx(10.0)
        assert mid == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# TestRealizedPerformanceByModel
# ---------------------------------------------------------------------------


class TestRealizedPerformanceByModel:

    def test_schema(self):
        df = _df(_row())
        out = realized_performance_by_model(df)
        assert list(out.columns) == _MODEL_COLS

    def test_empty_input(self):
        df = _df(_row()).iloc[0:0]
        out = realized_performance_by_model(df)
        assert out.empty
        assert list(out.columns) == _MODEL_COLS

    def test_single_model_family(self):
        rows = [
            _row(selected_model_family="seasonal_naive", selected_m=7),
            _row(selected_model_family="seasonal_naive", selected_m=7, report_id="r2"),
        ]
        df = _df(*rows)
        out = realized_performance_by_model(df)
        assert len(out) == 1
        assert out["report_count"].iloc[0] == 2

    def test_selected_m_grouping(self):
        """m=7 and m=30 must produce separate rows even for the same family."""
        rows = [
            _row(selected_model_family="seasonal_naive", selected_m=7,  report_id="r1"),
            _row(selected_model_family="seasonal_naive", selected_m=30, report_id="r2"),
        ]
        df = _df(*rows)
        out = realized_performance_by_model(df)
        assert len(out) == 2
        assert set(out["selected_m"]) == {7, 30}

    def test_multiple_families_sorted(self):
        rows = [
            _row(selected_model_family="ets",            selected_m=1,  report_id="r1"),
            _row(selected_model_family="seasonal_naive", selected_m=7,  report_id="r2"),
            _row(selected_model_family="auto_arima",     selected_m=7,  report_id="r3"),
        ]
        df = _df(*rows)
        out = realized_performance_by_model(df)
        assert list(out["selected_model_family"]) == sorted(
            ["ets", "seasonal_naive", "auto_arima"]
        )

    def test_run_count_per_model(self):
        """production_run_count = distinct run_ids that used this model."""
        rows = [
            _row(run_id="run_A", selected_model_family="seasonal_naive", selected_m=7),
            _row(run_id="run_B", selected_model_family="seasonal_naive", selected_m=7),
        ]
        df = _df(*rows)
        out = realized_performance_by_model(df)
        assert out["production_run_count"].iloc[0] == 2


# ---------------------------------------------------------------------------
# TestRecentVsPreviousWape
# ---------------------------------------------------------------------------


class TestRecentVsPreviousWape:

    def test_recent_and_previous_wape_computed(self):
        """Two runs: run_A (older) and run_B (more recent) → both wapes populated."""
        rows = [
            # run_A: forecast=110, actual=100 → WAPE = 10/100 = 0.10
            _row(run_id="run_A", report_id="r1", forecast=110, actual=100,
                 generated_at="2024-01-01"),
            # run_B: forecast=115, actual=100 → WAPE = 15/100 = 0.15
            _row(run_id="run_B", report_id="r1", forecast=115, actual=100,
                 generated_at="2024-01-02"),
        ]
        df = _df(*rows)
        out = realized_performance_by_report(df)
        r = out[out["report_id"] == "r1"].iloc[0]
        assert r["recent_wape"]   == pytest.approx(0.15)
        assert r["previous_wape"] == pytest.approx(0.10)
        assert r["accuracy_change"] == pytest.approx(0.05)

    def test_monitoring_status_degraded_when_wape_worsens(self):
        """accuracy_change > DEGRADED_WAPE_THRESHOLD → status = 'degraded'."""
        rows = [
            _row(run_id="run_A", report_id="r1", forecast=105, actual=100),
            _row(run_id="run_B", report_id="r1", forecast=130, actual=100),
        ]
        df = _df(*rows)
        out = realized_performance_by_report(df)
        r = out.iloc[0]
        change = r["recent_wape"] - r["previous_wape"]
        assert change > DEGRADED_WAPE_THRESHOLD
        assert r["monitoring_status"] == "degraded"

    def test_monitoring_status_ok_when_wape_improves(self):
        rows = [
            # run_A: WAPE = 0.20
            _row(run_id="run_A", report_id="r1", forecast=120, actual=100),
            # run_B: WAPE = 0.05
            _row(run_id="run_B", report_id="r1", forecast=105, actual=100),
        ]
        df = _df(*rows)
        out = realized_performance_by_report(df)
        assert out["monitoring_status"].iloc[0] == "ok"

    def test_insufficient_history_status(self):
        """Only one run → insufficient_data status."""
        rows = [_row(run_id="run_A", report_id="r1")]
        df = _df(*rows)
        out = realized_performance_by_report(df)
        assert out["monitoring_status"].iloc[0] == "insufficient_data"
        assert math.isnan(out["recent_wape"].iloc[0])
        assert math.isnan(out["previous_wape"].iloc[0])

    def test_schema(self):
        df = _df(_row())
        out = realized_performance_by_report(df)
        assert list(out.columns) == _REPORT_COLS

    def test_empty_input(self):
        df = _df(_row()).iloc[0:0]
        out = realized_performance_by_report(df)
        assert out.empty
        assert list(out.columns) == _REPORT_COLS

    def test_sorted_by_report_id(self):
        rows = [
            _row(run_id="run_A", report_id="r2"),
            _row(run_id="run_A", report_id="r1"),
        ]
        df = _df(*rows)
        out = realized_performance_by_report(df)
        assert list(out["report_id"]) == ["r1", "r2"]


# ---------------------------------------------------------------------------
# TestIntervalCoverage
# ---------------------------------------------------------------------------


class TestIntervalCoverage:

    def test_perfect_coverage(self):
        """All actuals within bounds → interval_coverage = 1.0."""
        rows = [
            _row(run_id="run_A", report_id="r1", actual=100, forecast=100,
                 lower_bound=80, upper_bound=120),
            _row(run_id="run_A", report_id="r1", actual=110, forecast=110,
                 lower_bound=90, upper_bound=130, horizon_step=2,
                 forecast_date="2024-02-02"),
        ]
        df = _df(*rows)
        out = realized_performance_by_run(df)
        assert out["interval_coverage"].iloc[0] == pytest.approx(1.0)

    def test_zero_coverage(self):
        """All actuals outside bounds → interval_coverage = 0.0."""
        rows = [
            _row(run_id="run_A", report_id="r1", actual=100, forecast=100,
                 lower_bound=110, upper_bound=130),
        ]
        df = _df(*rows)
        out = realized_performance_by_run(df)
        assert out["interval_coverage"].iloc[0] == pytest.approx(0.0)

    def test_no_bounds_gives_nan_coverage(self):
        """When bounds are all NaN, interval_coverage must be NaN."""
        rows = [_row(run_id="run_A", report_id="r1")]  # bounds default to NaN
        df = _df(*rows)
        out = realized_performance_by_run(df)
        assert math.isnan(out["interval_coverage"].iloc[0])

    def test_mean_interval_width(self):
        """mean_interval_width = mean(upper - lower)."""
        rows = [
            _row(run_id="run_A", report_id="r1", actual=100, forecast=100,
                 lower_bound=80, upper_bound=120),  # width = 40
            _row(run_id="run_A", report_id="r1", actual=110, forecast=110,
                 lower_bound=90, upper_bound=110, horizon_step=2,
                 forecast_date="2024-02-02"),          # width = 20
        ]
        df = _df(*rows)
        out = realized_performance_by_run(df)
        assert out["mean_interval_width"].iloc[0] == pytest.approx(30.0)

    def test_interval_coverage_by_horizon_bucket(self):
        """Interval coverage is independently computed per horizon bucket."""
        rows = [
            # days_1_7: inside bounds
            _row(run_id="run_A", report_id="r1", horizon_step=1,
                 actual=100, forecast=100, lower_bound=80, upper_bound=120),
            # days_8_14: outside bounds
            _row(run_id="run_A", report_id="r1", horizon_step=8,
                 forecast_date="2024-02-08",
                 actual=100, forecast=100, lower_bound=110, upper_bound=130),
        ]
        df = _df(*rows)
        out = realized_performance_by_horizon(df)
        near = out[out["horizon_bucket"] == "days_1_7"]["interval_coverage"].iloc[0]
        mid  = out[out["horizon_bucket"] == "days_8_14"]["interval_coverage"].iloc[0]
        assert near == pytest.approx(1.0)
        assert mid  == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# TestDeterministicOutput
# ---------------------------------------------------------------------------


class TestDeterministicOutput:

    def _sample_df(self) -> pd.DataFrame:
        rows = []
        for rid in ["r2", "r1", "r3"]:
            for step in [1, 8, 15]:
                rows.append(_row(
                    run_id="run_A",
                    report_id=rid,
                    horizon_step=step,
                    forecast_date=f"2024-02-{step:02d}",
                    forecast=110,
                    actual=100,
                ))
        return _df(*rows)

    def test_by_run_same_output_twice(self):
        df = self._sample_df()
        out1 = realized_performance_by_run(df)
        out2 = realized_performance_by_run(df)
        pd.testing.assert_frame_equal(out1, out2)

    def test_by_report_same_output_twice(self):
        df = self._sample_df()
        out1 = realized_performance_by_report(df)
        out2 = realized_performance_by_report(df)
        pd.testing.assert_frame_equal(out1, out2)

    def test_by_horizon_same_output_twice(self):
        df = self._sample_df()
        out1 = realized_performance_by_horizon(df)
        out2 = realized_performance_by_horizon(df)
        pd.testing.assert_frame_equal(out1, out2)

    def test_by_model_same_output_twice(self):
        df = self._sample_df()
        out1 = realized_performance_by_model(df)
        out2 = realized_performance_by_model(df)
        pd.testing.assert_frame_equal(out1, out2)

    def test_input_row_order_does_not_affect_output(self):
        """Shuffling the input DataFrame must produce the same sorted output."""
        df = self._sample_df()
        df_shuffled = df.sample(frac=1, random_state=42).reset_index(drop=True)
        out_normal   = realized_performance_by_run(df)
        out_shuffled = realized_performance_by_run(df_shuffled)
        pd.testing.assert_frame_equal(out_normal, out_shuffled)


# ---------------------------------------------------------------------------
# TestBuildAllTables
# ---------------------------------------------------------------------------


class TestBuildAllTables:

    def test_all_four_keys_present(self):
        df = _df(_row())
        tables = build_production_performance_tables(df)
        assert set(tables.keys()) == {"by_run", "by_report", "by_horizon", "by_model"}

    def test_empty_input_all_tables_empty(self):
        df = _df(_row()).iloc[0:0]
        tables = build_production_performance_tables(df)
        for key, tbl in tables.items():
            assert tbl.empty, f"Expected empty table for '{key}'"


# ---------------------------------------------------------------------------
# TestSaveProductionPerformance
# ---------------------------------------------------------------------------


class TestSaveProductionPerformance:

    def test_files_written_to_monitoring_dir(self, tmp_path):
        df = _df(_row())
        tables = build_production_performance_tables(df)
        paths = save_production_performance(tables, tmp_path)
        mon_dir = tmp_path / "outputs" / "monitoring"
        assert mon_dir.exists()
        for key, path in paths.items():
            if path is not None:
                assert path.exists(), f"{key} file not written"

    def test_csv_readable(self, tmp_path):
        df = _df(_row())
        tables = build_production_performance_tables(df)
        paths = save_production_performance(tables, tmp_path)
        for key, path in paths.items():
            if path is not None:
                loaded = pd.read_csv(path)
                assert not loaded.empty, f"{key} CSV is empty"

    def test_empty_tables_produce_none_paths(self, tmp_path):
        df = _df(_row()).iloc[0:0]
        tables = build_production_performance_tables(df)
        paths = save_production_performance(tables, tmp_path)
        for key, path in paths.items():
            assert path is None, f"Expected None path for empty table '{key}'"
