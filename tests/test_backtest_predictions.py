"""Tests for src/models/backtest_predictions.py.

Covers all required scenarios:
  - output file created at the correct path
  - stable schema and column order
  - residual sign: residual = actual - forecast
  - signed-error sign: signed_error = forecast - actual
  - residual + signed_error == 0 (sign pair consistency)
  - interval hit (inside_interval = True)
  - interval miss (inside_interval = False)
  - missing intervals (inside_interval = NaN)
  - failed model rows preserved with NaN error fields
  - SARIMA m=7 and m=30 remain distinct (candidate_m lineage)
  - fold and cutoff lineage carried through
  - deterministic sorting
  - repeated run safely replaces (overwrites) latest output
  - horizon_step outside [1, 28] fails validation
  - duplicate key fails validation
  - sign-inconsistent row fails validation
"""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.backtest_predictions import (
    BACKTEST_PREDICTIONS_COLS,
    add_diagnostic_fields,
    save_backtest_predictions,
    validate_backtest_predictions,
)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _pred_row(
    *,
    report_id: str = "R1",
    fold_number: int = 1,
    cutoff_date: str = "2024-01-07",
    train_start: str = "2023-01-01",
    train_end: str = "2024-01-07",
    forecast_date: str = "2024-01-08",
    horizon_step: int = 1,
    model_name: str = "seasonal_naive_m7",
    model_family: str = "seasonal_naive",
    candidate_m: int = 7,
    seasonal_candidate_rank: int = 1,
    cycles_available: int = 52,
    autocorrelation_at_m: float = 0.65,
    spectral_power_at_m: float = 0.42,
    seasonality_status: str = "seasonal",
    candidate_source: str = "seasonality_profiler",
    actual: float = 100.0,
    forecast: float = 110.0,
    lower_bound: float = float("nan"),
    upper_bound: float = float("nan"),
    fit_status: str = "ok",
) -> dict:
    """Return a single prediction row matching _PRED_COLS_EXT schema."""
    return {
        "report_id":               report_id,
        "fold_number":             fold_number,
        "cutoff_date":             cutoff_date,
        "train_start":             train_start,
        "train_end":               train_end,
        "forecast_date":           forecast_date,
        "horizon_step":            horizon_step,
        "model_name":              model_name,
        "model_family":            model_family,
        "candidate_m":             candidate_m,
        "seasonal_candidate_rank": seasonal_candidate_rank,
        "cycles_available":        cycles_available,
        "autocorrelation_at_m":    autocorrelation_at_m,
        "spectral_power_at_m":     spectral_power_at_m,
        "seasonality_status":      seasonality_status,
        "candidate_source":        candidate_source,
        "actual":                  actual,
        "forecast":                forecast,
        "lower_bound":             lower_bound,
        "upper_bound":             upper_bound,
        "fit_status":              fit_status,
    }


def _df(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


# ---------------------------------------------------------------------------
# TestOutputFile
# ---------------------------------------------------------------------------

class TestOutputFile:
    def test_file_created_at_correct_path(self, tmp_path):
        df = _df(_pred_row())
        path = save_backtest_predictions(df, tmp_path, evaluation_run_id="run_001")
        assert path is not None
        expected = tmp_path / "outputs" / "metrics" / "backtest_predictions_latest.csv"
        assert path == expected
        assert path.exists()

    def test_csv_is_readable(self, tmp_path):
        df = _df(_pred_row())
        path = save_backtest_predictions(df, tmp_path, evaluation_run_id="run_001")
        loaded = pd.read_csv(path)
        assert not loaded.empty

    def test_returns_none_for_empty_dataframe(self, tmp_path):
        empty = pd.DataFrame(columns=list(_pred_row().keys()))
        result = save_backtest_predictions(empty, tmp_path)
        assert result is None

    def test_returns_none_for_none_input(self, tmp_path):
        result = save_backtest_predictions(None, tmp_path)
        assert result is None

    def test_metrics_dir_created_if_absent(self, tmp_path):
        df = _df(_pred_row())
        save_backtest_predictions(df, tmp_path)
        assert (tmp_path / "outputs" / "metrics").exists()


# ---------------------------------------------------------------------------
# TestSchema
# ---------------------------------------------------------------------------

class TestSchema:
    def test_column_order_matches_canonical(self, tmp_path):
        df = _df(_pred_row())
        path = save_backtest_predictions(df, tmp_path)
        loaded = pd.read_csv(path)
        assert list(loaded.columns) == BACKTEST_PREDICTIONS_COLS

    def test_add_diagnostic_fields_output_columns(self):
        df = _df(_pred_row())
        out = add_diagnostic_fields(df)
        assert list(out.columns) == BACKTEST_PREDICTIONS_COLS

    def test_evaluation_run_id_column_present_when_supplied(self):
        df = _df(_pred_row())
        out = add_diagnostic_fields(df, evaluation_run_id="run_xyz")
        assert out["evaluation_run_id"].iloc[0] == "run_xyz"

    def test_evaluation_run_id_null_when_not_supplied(self):
        df = _df(_pred_row())
        out = add_diagnostic_fields(df)
        assert pd.isna(out["evaluation_run_id"].iloc[0])

    def test_identity_columns_unchanged(self):
        """Lineage columns must pass through without modification."""
        row = _pred_row(
            report_id="MY_REPORT",
            fold_number=3,
            model_name="auto_arima_m7",
            candidate_m=7,
            cutoff_date="2024-03-01",
        )
        out = add_diagnostic_fields(_df(row))
        r = out.iloc[0]
        assert r["report_id"] == "MY_REPORT"
        assert r["fold_number"] == 3
        assert r["model_name"] == "auto_arima_m7"
        assert r["candidate_m"] == 7
        assert r["cutoff_date"] == "2024-03-01"


# ---------------------------------------------------------------------------
# TestResidualSign
# ---------------------------------------------------------------------------

class TestResidualSign:
    """residual = actual - forecast  (positive when model under-forecasts)."""

    def test_residual_positive_when_under_forecast(self):
        # actual=100, forecast=80 → model predicted less than reality → residual > 0
        out = add_diagnostic_fields(_df(_pred_row(actual=100.0, forecast=80.0)))
        assert out["residual"].iloc[0] == pytest.approx(20.0)

    def test_residual_negative_when_over_forecast(self):
        # actual=100, forecast=120 → model over-predicted → residual < 0
        out = add_diagnostic_fields(_df(_pred_row(actual=100.0, forecast=120.0)))
        assert out["residual"].iloc[0] == pytest.approx(-20.0)

    def test_residual_zero_when_perfect(self):
        out = add_diagnostic_fields(_df(_pred_row(actual=100.0, forecast=100.0)))
        assert out["residual"].iloc[0] == pytest.approx(0.0)

    def test_residual_formula_actual_minus_forecast(self):
        actual, forecast = 150.0, 130.0
        out = add_diagnostic_fields(_df(_pred_row(actual=actual, forecast=forecast)))
        assert out["residual"].iloc[0] == pytest.approx(actual - forecast)


# ---------------------------------------------------------------------------
# TestSignedErrorSign
# ---------------------------------------------------------------------------

class TestSignedErrorSign:
    """signed_error = forecast - actual  (positive when model over-forecasts).

    Consistent with the sign convention in realized_forecast_history.signed_error.
    """

    def test_signed_error_positive_when_over_forecast(self):
        out = add_diagnostic_fields(_df(_pred_row(actual=100.0, forecast=120.0)))
        assert out["signed_error"].iloc[0] == pytest.approx(20.0)

    def test_signed_error_negative_when_under_forecast(self):
        out = add_diagnostic_fields(_df(_pred_row(actual=100.0, forecast=80.0)))
        assert out["signed_error"].iloc[0] == pytest.approx(-20.0)

    def test_signed_error_zero_when_perfect(self):
        out = add_diagnostic_fields(_df(_pred_row(actual=100.0, forecast=100.0)))
        assert out["signed_error"].iloc[0] == pytest.approx(0.0)

    def test_residual_and_signed_error_are_opposites(self):
        """residual + signed_error must always equal zero for non-NaN rows."""
        rows = [
            _pred_row(actual=100.0, forecast=110.0),
            _pred_row(actual=200.0, forecast=180.0, horizon_step=2, forecast_date="2024-01-09"),
            _pred_row(actual=50.0,  forecast=50.0,  horizon_step=3, forecast_date="2024-01-10"),
        ]
        out = add_diagnostic_fields(_df(*rows))
        # For all successful rows: residual + signed_error == 0
        both_valid = out["residual"].notna() & out["signed_error"].notna()
        total = (out.loc[both_valid, "residual"] + out.loc[both_valid, "signed_error"]).abs()
        assert (total < 1e-9).all()

    def test_absolute_error_is_abs_signed_error(self):
        """absolute_error == |signed_error| always."""
        rows = [
            _pred_row(actual=100.0, forecast=120.0),
            _pred_row(actual=100.0, forecast=80.0,  horizon_step=2, forecast_date="2024-01-09"),
        ]
        out = add_diagnostic_fields(_df(*rows))
        for _, row in out.iterrows():
            if pd.notna(row["absolute_error"]) and pd.notna(row["signed_error"]):
                assert abs(row["signed_error"]) == pytest.approx(row["absolute_error"])


# ---------------------------------------------------------------------------
# TestIntervalHitMiss
# ---------------------------------------------------------------------------

class TestIntervalHitMiss:
    def test_inside_interval_true_when_actual_within_bounds(self):
        row = _pred_row(actual=100.0, forecast=100.0, lower_bound=80.0, upper_bound=120.0)
        out = add_diagnostic_fields(_df(row))
        assert out["inside_interval"].iloc[0] == True

    def test_inside_interval_true_at_lower_bound(self):
        """Boundary is inclusive: actual == lower_bound → inside."""
        row = _pred_row(actual=80.0, forecast=100.0, lower_bound=80.0, upper_bound=120.0)
        out = add_diagnostic_fields(_df(row))
        assert out["inside_interval"].iloc[0] == True

    def test_inside_interval_true_at_upper_bound(self):
        """Boundary is inclusive: actual == upper_bound → inside."""
        row = _pred_row(actual=120.0, forecast=100.0, lower_bound=80.0, upper_bound=120.0)
        out = add_diagnostic_fields(_df(row))
        assert out["inside_interval"].iloc[0] == True

    def test_inside_interval_false_when_actual_below_lower(self):
        row = _pred_row(actual=70.0, forecast=100.0, lower_bound=80.0, upper_bound=120.0)
        out = add_diagnostic_fields(_df(row))
        assert out["inside_interval"].iloc[0] == False

    def test_inside_interval_false_when_actual_above_upper(self):
        row = _pred_row(actual=130.0, forecast=100.0, lower_bound=80.0, upper_bound=120.0)
        out = add_diagnostic_fields(_df(row))
        assert out["inside_interval"].iloc[0] == False

    def test_interval_width_computed_correctly(self):
        row = _pred_row(actual=100.0, forecast=100.0, lower_bound=80.0, upper_bound=120.0)
        out = add_diagnostic_fields(_df(row))
        assert out["interval_width"].iloc[0] == pytest.approx(40.0)


# ---------------------------------------------------------------------------
# TestMissingIntervals
# ---------------------------------------------------------------------------

class TestMissingIntervals:
    def test_inside_interval_nan_when_no_bounds(self):
        row = _pred_row(actual=100.0, forecast=100.0)  # bounds default to NaN
        out = add_diagnostic_fields(_df(row))
        assert pd.isna(out["inside_interval"].iloc[0])

    def test_interval_width_nan_when_no_bounds(self):
        row = _pred_row(actual=100.0, forecast=100.0)
        out = add_diagnostic_fields(_df(row))
        assert pd.isna(out["interval_width"].iloc[0])

    def test_inside_interval_nan_when_only_lower_bound(self):
        row = _pred_row(actual=100.0, forecast=100.0, lower_bound=80.0)
        out = add_diagnostic_fields(_df(row))
        assert pd.isna(out["inside_interval"].iloc[0])

    def test_inside_interval_nan_when_only_upper_bound(self):
        row = _pred_row(actual=100.0, forecast=100.0, upper_bound=120.0)
        out = add_diagnostic_fields(_df(row))
        assert pd.isna(out["inside_interval"].iloc[0])


# ---------------------------------------------------------------------------
# TestFailedModelRows
# ---------------------------------------------------------------------------

class TestFailedModelRows:
    """Failed predictions must be preserved but must not contribute to error fields."""

    def _failed_row(self, **kwargs) -> dict:
        return _pred_row(
            forecast=float("nan"),
            lower_bound=float("nan"),
            upper_bound=float("nan"),
            fit_status="failed",
            **kwargs,
        )

    def test_failed_row_preserved_in_output(self):
        rows = [
            _pred_row(actual=100.0, forecast=110.0),
            self._failed_row(actual=90.0, horizon_step=2, forecast_date="2024-01-09"),
        ]
        out = add_diagnostic_fields(_df(*rows))
        failed = out[out["fit_status"] == "failed"]
        assert len(failed) == 1
        assert failed["actual"].iloc[0] == pytest.approx(90.0)

    def test_failed_row_residual_is_nan(self):
        row = self._failed_row(actual=100.0)
        out = add_diagnostic_fields(_df(row))
        assert pd.isna(out["residual"].iloc[0])

    def test_failed_row_signed_error_is_nan(self):
        row = self._failed_row(actual=100.0)
        out = add_diagnostic_fields(_df(row))
        assert pd.isna(out["signed_error"].iloc[0])

    def test_failed_row_absolute_error_is_nan(self):
        row = self._failed_row(actual=100.0)
        out = add_diagnostic_fields(_df(row))
        assert pd.isna(out["absolute_error"].iloc[0])

    def test_failed_row_squared_error_is_nan(self):
        row = self._failed_row(actual=100.0)
        out = add_diagnostic_fields(_df(row))
        assert pd.isna(out["squared_error"].iloc[0])

    def test_failed_row_inside_interval_is_nan(self):
        row = self._failed_row(actual=100.0)
        out = add_diagnostic_fields(_df(row))
        assert pd.isna(out["inside_interval"].iloc[0])

    def test_failed_and_successful_rows_coexist(self):
        """Successful rows still get valid error fields even when failed rows are present."""
        rows = [
            _pred_row(actual=100.0, forecast=110.0),
            self._failed_row(actual=80.0, horizon_step=2, forecast_date="2024-01-09"),
        ]
        out = add_diagnostic_fields(_df(*rows))
        success = out[out["fit_status"] == "ok"]
        assert len(success) == 1
        assert success["residual"].iloc[0] == pytest.approx(-10.0)
        assert success["signed_error"].iloc[0] == pytest.approx(10.0)

    def test_actual_preserved_in_failed_row(self):
        """actual must not be NaN in a failed row — it is the observed ground truth."""
        row = self._failed_row(actual=55.0)
        out = add_diagnostic_fields(_df(row))
        assert out["actual"].iloc[0] == pytest.approx(55.0)


# ---------------------------------------------------------------------------
# TestCandidateMLineage
# ---------------------------------------------------------------------------

class TestCandidateMLineage:
    """m=7 and m=30 SARIMA candidates must remain distinct rows with correct candidate_m."""

    def _two_candidate_rows(self):
        return [
            _pred_row(
                model_name="auto_arima_m7",
                model_family="auto_arima",
                candidate_m=7,
                seasonal_candidate_rank=1,
                actual=100.0,
                forecast=105.0,
            ),
            _pred_row(
                model_name="auto_arima_m30",
                model_family="auto_arima",
                candidate_m=30,
                seasonal_candidate_rank=2,
                actual=100.0,
                forecast=115.0,
                horizon_step=1,  # same step to test lineage distinction
                forecast_date="2024-01-08",
            ),
        ]

    def test_two_candidate_rows_remain_distinct(self):
        out = add_diagnostic_fields(_df(*self._two_candidate_rows()))
        assert len(out) == 2
        candidate_ms = set(out["candidate_m"].tolist())
        assert candidate_ms == {7, 30}

    def test_candidate_m7_has_correct_residual(self):
        out = add_diagnostic_fields(_df(*self._two_candidate_rows()))
        m7 = out[out["candidate_m"] == 7].iloc[0]
        assert m7["residual"] == pytest.approx(100.0 - 105.0)

    def test_candidate_m30_has_correct_residual(self):
        out = add_diagnostic_fields(_df(*self._two_candidate_rows()))
        m30 = out[out["candidate_m"] == 30].iloc[0]
        assert m30["residual"] == pytest.approx(100.0 - 115.0)

    def test_model_family_preserved_per_candidate(self):
        out = add_diagnostic_fields(_df(*self._two_candidate_rows()))
        assert (out["model_family"] == "auto_arima").all()

    def test_seasonal_candidate_rank_preserved(self):
        out = add_diagnostic_fields(_df(*self._two_candidate_rows()))
        ranks = dict(zip(out["candidate_m"], out["seasonal_candidate_rank"]))
        assert ranks[7]  == 1
        assert ranks[30] == 2


# ---------------------------------------------------------------------------
# TestFoldLineage
# ---------------------------------------------------------------------------

class TestFoldLineage:
    """fold_number and cutoff_date must be preserved exactly."""

    def test_fold_number_carried_through(self):
        row = _pred_row(fold_number=4, cutoff_date="2024-03-01")
        out = add_diagnostic_fields(_df(row))
        assert out["fold_number"].iloc[0] == 4

    def test_cutoff_date_carried_through(self):
        row = _pred_row(fold_number=2, cutoff_date="2024-02-15")
        out = add_diagnostic_fields(_df(row))
        assert out["cutoff_date"].iloc[0] == "2024-02-15"

    def test_train_start_and_train_end_carried_through(self):
        row = _pred_row(train_start="2023-06-01", train_end="2024-02-14")
        out = add_diagnostic_fields(_df(row))
        assert out["train_start"].iloc[0] == "2023-06-01"
        assert out["train_end"].iloc[0] == "2024-02-14"

    def test_multiple_folds_each_retain_lineage(self):
        rows = [
            _pred_row(fold_number=1, cutoff_date="2024-01-07", horizon_step=1, forecast_date="2024-01-08"),
            _pred_row(fold_number=2, cutoff_date="2024-02-04", horizon_step=1, forecast_date="2024-02-05"),
            _pred_row(fold_number=3, cutoff_date="2024-03-03", horizon_step=1, forecast_date="2024-03-04"),
        ]
        out = add_diagnostic_fields(_df(*rows))
        assert set(out["fold_number"]) == {1, 2, 3}
        assert set(out["cutoff_date"]) == {"2024-01-07", "2024-02-04", "2024-03-03"}


# ---------------------------------------------------------------------------
# TestDeterministicSorting
# ---------------------------------------------------------------------------

class TestDeterministicSorting:
    def _sample_df(self):
        rows = []
        for rid in ["R2", "R1"]:
            for fold in [2, 1]:
                for model in ["seasonal_naive_m7", "naive"]:
                    for step in [3, 1, 2]:
                        rows.append(_pred_row(
                            report_id=rid,
                            fold_number=fold,
                            model_name=model,
                            model_family="seasonal_naive" if "naive_m" in model else "naive",
                            candidate_m=7 if "m7" in model else 1,
                            horizon_step=step,
                            forecast_date=f"2024-01-0{step + 7}",
                        ))
        return _df(*rows)

    def test_sorted_by_report_fold_model_step(self, tmp_path):
        df = self._sample_df()
        path = save_backtest_predictions(df, tmp_path)
        loaded = pd.read_csv(path)
        assert list(loaded["report_id"])   == sorted(loaded["report_id"].tolist())
        # Within each report, fold_number should be ascending
        for rid in loaded["report_id"].unique():
            sub = loaded[loaded["report_id"] == rid]
            assert list(sub["fold_number"]) == sorted(sub["fold_number"].tolist())

    def test_same_input_different_order_same_output(self, tmp_path):
        df = self._sample_df()
        df_shuffled = df.sample(frac=1, random_state=99).reset_index(drop=True)

        path1 = save_backtest_predictions(df, tmp_path / "a")
        path2 = save_backtest_predictions(df_shuffled, tmp_path / "b")

        loaded1 = pd.read_csv(path1)
        loaded2 = pd.read_csv(path2)
        pd.testing.assert_frame_equal(loaded1, loaded2)


# ---------------------------------------------------------------------------
# TestRepeatedRunOverwrites
# ---------------------------------------------------------------------------

class TestRepeatedRunOverwrites:
    def test_second_run_replaces_first(self, tmp_path):
        """Writing twice must leave exactly one file with second-run content."""
        df1 = _df(_pred_row(actual=100.0, forecast=110.0))
        df2 = _df(_pred_row(actual=200.0, forecast=190.0))

        path = save_backtest_predictions(df1, tmp_path, evaluation_run_id="run_001")
        save_backtest_predictions(df2, tmp_path, evaluation_run_id="run_002")

        loaded = pd.read_csv(path)
        assert len(loaded) == 1
        assert loaded["actual"].iloc[0] == pytest.approx(200.0)
        assert loaded["evaluation_run_id"].iloc[0] == "run_002"

    def test_only_one_file_exists_after_two_runs(self, tmp_path):
        df = _df(_pred_row())
        save_backtest_predictions(df, tmp_path, evaluation_run_id="run_001")
        save_backtest_predictions(df, tmp_path, evaluation_run_id="run_002")
        csv_files = list((tmp_path / "outputs" / "metrics").glob("backtest_predictions*.csv"))
        assert len(csv_files) == 1

    def test_no_history_file_created(self, tmp_path):
        """No append-only history file should be created for backtest predictions."""
        df = _df(_pred_row())
        save_backtest_predictions(df, tmp_path, evaluation_run_id="run_001")
        save_backtest_predictions(df, tmp_path, evaluation_run_id="run_002")
        metrics_dir = tmp_path / "outputs" / "metrics"
        history_files = list(metrics_dir.glob("backtest_predictions_history*"))
        assert len(history_files) == 0


# ---------------------------------------------------------------------------
# TestValidation
# ---------------------------------------------------------------------------

class TestValidation:
    def test_valid_df_passes(self):
        df = _df(_pred_row())
        out = add_diagnostic_fields(df)
        validate_backtest_predictions(out)  # must not raise

    def test_missing_required_column_raises(self):
        out = add_diagnostic_fields(_df(_pred_row())).drop(columns=["report_id"])
        with pytest.raises(ValueError, match="missing required column"):
            validate_backtest_predictions(out)

    def test_horizon_step_out_of_range_raises(self):
        row = _pred_row(horizon_step=29)
        out = add_diagnostic_fields(_df(row))
        out["horizon_step"] = 29  # force the invalid value through
        with pytest.raises(ValueError, match="horizon_step"):
            validate_backtest_predictions(out)

    def test_horizon_step_zero_raises(self):
        out = add_diagnostic_fields(_df(_pred_row()))
        out["horizon_step"] = 0
        with pytest.raises(ValueError, match="horizon_step"):
            validate_backtest_predictions(out)

    def test_duplicate_key_raises(self):
        row = _pred_row()
        out = add_diagnostic_fields(pd.concat([_df(row), _df(row)], ignore_index=True))
        with pytest.raises(ValueError, match="duplicate"):
            validate_backtest_predictions(out)

    def test_sign_inconsistency_raises(self):
        """Manually corrupt the sign so residual + signed_error ≠ 0."""
        out = add_diagnostic_fields(_df(_pred_row(actual=100.0, forecast=110.0)))
        out = out.copy()
        out.loc[0, "residual"] = 999.0  # break the invariant
        with pytest.raises(ValueError, match="sign"):
            validate_backtest_predictions(out)

    def test_horizon_step_28_is_valid(self):
        """Boundary value: horizon_step=28 must be accepted."""
        row = _pred_row(horizon_step=28, forecast_date="2024-02-04")
        out = add_diagnostic_fields(_df(row))
        validate_backtest_predictions(out)  # must not raise

    def test_horizon_step_1_is_valid(self):
        """Boundary value: horizon_step=1 must be accepted."""
        row = _pred_row(horizon_step=1)
        out = add_diagnostic_fields(_df(row))
        validate_backtest_predictions(out)  # must not raise
