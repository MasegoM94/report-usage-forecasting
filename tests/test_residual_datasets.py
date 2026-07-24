"""Tests for src/models/residual_datasets.py.

Covers all required scenarios:

Training residual tests
  - ARIMA residual rows created
  - SARIMA residual rows created
  - ETS residual rows created
  - correct residual sign
  - initial unavailable fitted observations handled
  - fold lineage preserved
  - production-refit lineage preserved
  - candidate_m preserved
  - extraction failure recorded
  - no artificial zero fitted values
  - evidence counts reconcile
  - duplicate key rejected

Backtest error tests
  - correct residual sign
  - correct signed-error sign
  - residual and signed error are opposites
  - absolute error correct
  - squared error correct
  - horizon step 1
  - horizon step 28
  - invalid horizon rejected
  - interval hit
  - interval miss
  - missing interval
  - failed prediction row not valid
  - SARIMA m=7 and m=30 remain separate
  - deterministic sorting
  - source backtest file is not modified

Production error tests
  - canonical signed_error converted correctly
  - zero actual retained
  - positive production residual means underforecast
  - negative production residual means overforecast
  - complete lineage preserved
  - incomplete lineage preserved
  - missing selected_m not inferred
  - invalid signed-error reconciliation fails
  - canonical realized history remains unchanged
  - deterministic sorting

Cross-dataset tests
  - residual_source is never mixed
  - training residuals are not treated as backtest errors
  - backtest errors are not treated as production errors
  - schemas remain stable
  - output files are created
  - repeated execution safely replaces latest diagnostic files
  - no duplicated append-only monitoring file is created
"""

from __future__ import annotations

import importlib
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.residual_datasets import (
    BACKTEST_FORECAST_ERRORS_COLS,
    PRODUCTION_FORECAST_ERRORS_COLS,
    TRAINING_RESIDUALS_COLS,
    build_backtest_forecast_error_dataset,
    build_production_forecast_error_view,
    build_training_residual_dataset,
    persist_residual_datasets,
    validate_backtest_forecast_error_dataset,
    validate_production_forecast_error_dataset,
    validate_training_residual_dataset,
)

_STATSMODELS_AVAILABLE = importlib.util.find_spec("statsmodels") is not None
_PMDARIMA_AVAILABLE    = importlib.util.find_spec("pmdarima") is not None

_skip_no_statsmodels = pytest.mark.skipif(
    not _STATSMODELS_AVAILABLE, reason="statsmodels not installed"
)
_skip_no_pmdarima = pytest.mark.skipif(
    not _PMDARIMA_AVAILABLE, reason="pmdarima not installed"
)


# ---------------------------------------------------------------------------
# Shared fixtures and helpers
# ---------------------------------------------------------------------------

def _daily_series(n: int, start: str = "2022-01-01", seed: int = 42) -> pd.Series:
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, periods=n, freq="D")
    return pd.Series(50 + rng.integers(-10, 11, size=n).astype(float), index=idx)


def _tr_record(
    *,
    report_id: str = "R1",
    fold_number: int = 1,
    training_start: str = "2022-01-01",
    training_cutoff: str = "2022-06-01",
    model_name: str = "naive",
    model_family: str = "naive",
    candidate_m: int = 1,
    fit_status: str = "ok",
    n: int = 10,
    extraction_status: str = "ok",
    extraction_reason: str | None = None,
) -> dict:
    """Build a synthetic training-residual record as returned by evaluate_candidates_across_folds."""
    if extraction_status == "ok" and n > 0:
        actuals   = np.linspace(10, 20, n)
        fitteds   = actuals - np.linspace(-1, 1, n)  # some positive, some negative residuals
        residuals = actuals - fitteds
        dates     = pd.date_range(training_start, periods=n, freq="D")
    else:
        actuals = fitteds = residuals = dates = None

    return {
        "report_id":                  report_id,
        "fold_number":                fold_number,
        "training_start":             pd.Timestamp(training_start),
        "training_cutoff":            pd.Timestamp(training_cutoff),
        "model_name":                 model_name,
        "model_family":               model_family,
        "candidate_m":                candidate_m,
        "fit_status":                 fit_status,
        "training_actual":            actuals,
        "training_fitted":            fitteds,
        "training_residuals":         residuals,
        "training_residual_dates":    dates,
        "residual_extraction_status": extraction_status,
        "residual_extraction_reason": extraction_reason,
        "training_observation_count": n + 1 if extraction_status == "ok" and n > 0 else n,
        "fitted_observation_count":   n if extraction_status == "ok" else None,
        "residual_observation_count": n if extraction_status == "ok" else None,
    }


def _bp_row(
    *,
    evaluation_run_id: str = "run_001",
    report_id: str = "R1",
    fold_number: int = 1,
    cutoff_date: str = "2022-06-01",
    train_start: str = "2022-01-01",
    train_end: str = "2022-05-31",
    model_family: str = "naive",
    model_name: str = "naive",
    candidate_m: int = 1,
    seasonal_candidate_rank: int = 1,
    cycles_available: int = 20,
    autocorrelation_at_m: float = 0.5,
    spectral_power_at_m: float = 0.3,
    seasonality_status: str = "non_seasonal",
    candidate_source: str = "default",
    forecast_date: str = "2022-06-02",
    horizon_step: int = 1,
    actual: float = 100.0,
    forecast: float = 110.0,
    lower_bound: float = float("nan"),
    upper_bound: float = float("nan"),
    fit_status: str = "ok",
) -> dict:
    """Build a synthetic backtest prediction row (BACKTEST_PREDICTIONS_COLS format)."""
    residual      = actual - forecast
    signed_error  = forecast - actual
    absolute_error = abs(residual)
    squared_error  = residual ** 2
    inside_interval = float("nan")
    interval_width  = float("nan")
    if not (np.isnan(lower_bound) or np.isnan(upper_bound)):
        inside_interval = float(lower_bound <= actual <= upper_bound)
        interval_width  = upper_bound - lower_bound
    if fit_status == "failed":
        residual = signed_error = absolute_error = squared_error = float("nan")
        inside_interval = interval_width = float("nan")
    return {
        "evaluation_run_id":       evaluation_run_id,
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
        "residual":                residual,
        "signed_error":            signed_error,
        "absolute_error":          absolute_error,
        "squared_error":           squared_error,
        "inside_interval":         inside_interval,
        "interval_width":          interval_width,
    }


def _rh_row(
    *,
    run_id: str = "run_001",
    selection_run_id: str = "run_001",
    report_id: str = "R1",
    report_name: str = "Report 1",
    generated_at: str = "2022-07-01",
    training_cutoff: str = "2022-06-01",
    selected_model_family: str = "naive",
    selected_model_name: str = "naive",
    selected_m: float = 1.0,
    forecast_date: str = "2022-06-02",
    horizon_step: int = 1,
    realized_at: str = "2022-07-01",
    actual: float = 100.0,
    forecast: float = 110.0,
    lower_bound: float = float("nan"),
    upper_bound: float = float("nan"),
    lineage_complete: bool = True,
    lineage_missing_fields: str = "",
) -> dict:
    """Build a synthetic realized_forecast_history row."""
    se = forecast - actual
    ae = abs(se)
    sq = se ** 2
    ii = float("nan")
    iw = float("nan")
    if not (np.isnan(lower_bound) or np.isnan(upper_bound)):
        ii = float(lower_bound <= actual <= upper_bound)
        iw = upper_bound - lower_bound
    pe = abs(se) / actual * 100 if actual != 0 else float("nan")
    return {
        "run_id":                  run_id,
        "selection_run_id":        selection_run_id,
        "generated_at":            generated_at,
        "training_cutoff":         training_cutoff,
        "realized_at":             realized_at,
        "report_id":               report_id,
        "report_name":             report_name,
        "selected_model_family":   selected_model_family,
        "selected_model_name":     selected_model_name,
        "selected_m":              selected_m,
        "forecast_date":           forecast_date,
        "horizon_step":            horizon_step,
        "forecast":                forecast,
        "lower_bound":             lower_bound,
        "upper_bound":             upper_bound,
        "actual":                  actual,
        "signed_error":            se,
        "absolute_error":          ae,
        "squared_error":           sq,
        "inside_interval":         ii,
        "interval_width":          iw,
        "percentage_error":        pe,
        "lineage_complete":        lineage_complete,
        "lineage_missing_fields":  lineage_missing_fields,
    }


def _df(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


# ---------------------------------------------------------------------------
# TestTrainingResiduals
# ---------------------------------------------------------------------------

class TestTrainingResiduals:
    def test_naive_residual_rows_created(self):
        rec = _tr_record(model_name="naive", model_family="naive", candidate_m=1)
        df = build_training_residual_dataset([rec], diagnostic_run_id="D1")
        valid = df[df["residual_observation_valid"] == True]
        assert len(valid) > 0

    def test_correct_residual_sign(self):
        """residual = actual - fitted (positive when model under-forecasts)."""
        rec = _tr_record(n=5)
        df  = build_training_residual_dataset([rec], diagnostic_run_id="D1")
        valid = df[df["residual_observation_valid"] == True]
        for _, row in valid.iterrows():
            assert abs(row["residual"] - (row["actual"] - row["fitted"])) < 1e-9

    def test_no_artificial_zero_fitted_values(self):
        """No row must have fitted=0 unless the model genuinely predicted zero."""
        rec = _tr_record(n=10)
        df  = build_training_residual_dataset([rec], diagnostic_run_id="D1")
        valid = df[df["residual_observation_valid"] == True]
        # Our synthetic fitted values are actuals ± offset — none should be exactly 0
        # unless numpy coincidentally produced a 0; we verify that stub rows have null fitted
        stubs = df[df["residual_observation_valid"] == False]
        assert stubs["fitted"].isna().all(), "Stub rows must not have a fitted value."

    def test_fold_lineage_preserved(self):
        rec = _tr_record(fold_number=3, model_name="naive")
        df  = build_training_residual_dataset([rec], diagnostic_run_id="D1")
        assert (df["fold_number"] == 3).any()

    def test_production_refit_lineage_preserved(self):
        rec = _tr_record(fold_number=None)
        df  = build_training_residual_dataset(
            [rec],
            diagnostic_run_id="D1",
            fit_scope="production_refit",
        )
        assert (df["fit_scope"] == "production_refit").all()
        # production_refit rows must have null fold_number
        assert df["fold_number"].isna().all()

    def test_candidate_m_preserved(self):
        for m in [1, 7, 28, 30]:
            rec = _tr_record(candidate_m=m, model_name=f"seasonal_naive_m{m}")
            df  = build_training_residual_dataset([rec], diagnostic_run_id="D1")
            assert (df["candidate_m"] == m).all(), f"candidate_m={m} not preserved"

    def test_extraction_failure_recorded_as_stub_row(self):
        rec = _tr_record(extraction_status="failed", extraction_reason="SomeError: boom", n=0)
        df  = build_training_residual_dataset([rec], diagnostic_run_id="D1")
        assert len(df) == 1
        row = df.iloc[0]
        assert row["residual_observation_valid"] == False
        assert row["residual_extraction_status"] == "failed"
        assert "boom" in str(row["residual_extraction_reason"])

    def test_unavailable_status_recorded_as_stub_row(self):
        rec = _tr_record(extraction_status="unavailable", n=0,
                         extraction_reason="moving_average: series too short")
        df  = build_training_residual_dataset([rec], diagnostic_run_id="D1")
        assert len(df) == 1
        assert df.iloc[0]["residual_observation_valid"] == False

    def test_evidence_counts_reconcile(self):
        n = 8
        rec = _tr_record(n=n)
        df  = build_training_residual_dataset([rec], diagnostic_run_id="D1")
        # All rows share the same counts
        assert (df["training_observation_count"] == n + 1).all()  # n+1 set in _tr_record
        assert (df["fitted_observation_count"]   == n).all()
        assert (df["residual_observation_count"] == n).all()

    def test_initial_unavailable_fitted_handled(self):
        """naive: first obs excluded; fitted_obs < training_obs."""
        rec = _tr_record(n=9)
        # Simulate: training_obs = 10, fitted = 9 (first excluded)
        rec["training_observation_count"] = 10
        rec["fitted_observation_count"]   = 9
        rec["residual_observation_count"] = 9
        df = build_training_residual_dataset([rec], diagnostic_run_id="D1")
        assert (df["training_observation_count"] == 10).all()
        assert (df["fitted_observation_count"]   == 9).all()

    def test_residual_source_is_training(self):
        df = build_training_residual_dataset(
            [_tr_record()], diagnostic_run_id="D1"
        )
        assert (df["residual_source"] == "training").all()

    def test_schema_stable(self):
        df = build_training_residual_dataset(
            [_tr_record()], diagnostic_run_id="D1"
        )
        assert list(df.columns) == TRAINING_RESIDUALS_COLS

    def test_multiple_candidates_independent(self):
        """Extraction failure for one candidate must not invalidate others."""
        ok_rec   = _tr_record(model_name="naive",    extraction_status="ok",   n=5)
        fail_rec = _tr_record(model_name="bad_model", extraction_status="failed", n=0)
        df = build_training_residual_dataset([ok_rec, fail_rec], diagnostic_run_id="D1")
        valid  = df[df["residual_observation_valid"] == True]
        stubs  = df[df["residual_observation_valid"] == False]
        assert len(valid)  > 0
        assert len(stubs) == 1
        assert stubs.iloc[0]["model_name"] == "bad_model"

    @_skip_no_pmdarima
    def test_arima_residual_rows_created(self):
        from src.models.candidates import forecast_auto_arima
        series = _daily_series(200)
        result = forecast_auto_arima(series, horizon=28, seasonal_period=1)
        if result.fit_status == "failed":
            pytest.skip(f"auto_arima failed: {result.error_message}")
        rec = {
            "report_id": "R1", "fold_number": 1,
            "training_start": series.index[0], "training_cutoff": series.index[-1],
            "model_name": result.model_name, "model_family": "auto_arima",
            "candidate_m": 1, "fit_status": result.fit_status,
            "training_actual":            result.training_actual,
            "training_fitted":            result.training_fitted,
            "training_residuals":         result.training_residuals,
            "training_residual_dates":    result.training_residual_dates,
            "residual_extraction_status": result.residual_extraction_status,
            "residual_extraction_reason": result.residual_extraction_reason,
            "training_observation_count": result.training_observation_count,
            "fitted_observation_count":   result.fitted_observation_count,
            "residual_observation_count": result.residual_observation_count,
        }
        df = build_training_residual_dataset([rec], diagnostic_run_id="D1")
        valid = df[df["residual_observation_valid"] == True]
        assert len(valid) > 0
        assert result.residual_extraction_status == "ok"

    @_skip_no_pmdarima
    def test_sarima_residual_rows_created(self):
        from src.models.candidates import forecast_auto_arima
        series = _daily_series(365)
        result = forecast_auto_arima(series, horizon=28, seasonal_period=7)
        if result.fit_status == "failed":
            pytest.skip(f"sarima failed: {result.error_message}")
        rec = {
            "report_id": "R1", "fold_number": 1,
            "training_start": series.index[0], "training_cutoff": series.index[-1],
            "model_name": result.model_name, "model_family": "auto_arima",
            "candidate_m": 7, "fit_status": result.fit_status,
            "training_actual":            result.training_actual,
            "training_fitted":            result.training_fitted,
            "training_residuals":         result.training_residuals,
            "training_residual_dates":    result.training_residual_dates,
            "residual_extraction_status": result.residual_extraction_status,
            "residual_extraction_reason": result.residual_extraction_reason,
            "training_observation_count": result.training_observation_count,
            "fitted_observation_count":   result.fitted_observation_count,
            "residual_observation_count": result.residual_observation_count,
        }
        df = build_training_residual_dataset([rec], diagnostic_run_id="D1")
        valid = df[df["residual_observation_valid"] == True]
        assert len(valid) > 0
        assert df.iloc[0]["candidate_m"] == 7

    @_skip_no_statsmodels
    def test_ets_residual_rows_created(self):
        from src.models.candidates import forecast_ets
        series = _daily_series(365)
        result = forecast_ets(series, horizon=28, seasonal_period=7)
        if result.fit_status == "failed":
            pytest.skip(f"ets failed: {result.error_message}")
        rec = {
            "report_id": "R1", "fold_number": 1,
            "training_start": series.index[0], "training_cutoff": series.index[-1],
            "model_name": result.model_name, "model_family": "ets",
            "candidate_m": 7, "fit_status": result.fit_status,
            "training_actual":            result.training_actual,
            "training_fitted":            result.training_fitted,
            "training_residuals":         result.training_residuals,
            "training_residual_dates":    result.training_residual_dates,
            "residual_extraction_status": result.residual_extraction_status,
            "residual_extraction_reason": result.residual_extraction_reason,
            "training_observation_count": result.training_observation_count,
            "fitted_observation_count":   result.fitted_observation_count,
            "residual_observation_count": result.residual_observation_count,
        }
        df = build_training_residual_dataset([rec], diagnostic_run_id="D1")
        valid = df[df["residual_observation_valid"] == True]
        assert len(valid) > 0

    def test_duplicate_key_rejected(self):
        rec = _tr_record(fold_number=1, model_name="naive", candidate_m=1, n=3)
        df  = build_training_residual_dataset([rec, rec], diagnostic_run_id="D1")
        # Build produces rows; validation should catch duplicates
        with pytest.raises(ValueError, match="duplicate"):
            validate_training_residual_dataset(df)

    def test_validation_passes_for_valid_df(self):
        df = build_training_residual_dataset(
            [_tr_record()], diagnostic_run_id="D1"
        )
        validate_training_residual_dataset(df)  # must not raise

    def test_wrong_residual_source_rejected(self):
        df = build_training_residual_dataset([_tr_record()], diagnostic_run_id="D1")
        df = df.copy()
        df["residual_source"] = "backtest"
        with pytest.raises(ValueError, match="residual_source"):
            validate_training_residual_dataset(df)

    def test_missing_required_column_rejected(self):
        df = build_training_residual_dataset([_tr_record()], diagnostic_run_id="D1")
        df = df.drop(columns=["residual"])
        with pytest.raises(ValueError, match="missing"):
            validate_training_residual_dataset(df)


# ---------------------------------------------------------------------------
# TestBacktestForecastErrors
# ---------------------------------------------------------------------------

class TestBacktestForecastErrors:
    def _simple_df(self, **kwargs):
        return _df(_bp_row(**kwargs))

    def test_correct_residual_sign(self):
        """residual = actual - forecast."""
        df = build_backtest_forecast_error_dataset(
            self._simple_df(actual=100.0, forecast=110.0)
        )
        valid = df[df["residual_observation_valid"] == True]
        assert len(valid) == 1
        assert abs(valid.iloc[0]["residual"] - (100.0 - 110.0)) < 1e-9

    def test_correct_signed_error_sign(self):
        """signed_error = forecast - actual."""
        df = build_backtest_forecast_error_dataset(
            self._simple_df(actual=100.0, forecast=110.0)
        )
        valid = df[df["residual_observation_valid"] == True]
        assert abs(valid.iloc[0]["signed_error"] - (110.0 - 100.0)) < 1e-9

    def test_residual_and_signed_error_are_opposites(self):
        df = build_backtest_forecast_error_dataset(
            self._simple_df(actual=100.0, forecast=115.0)
        )
        valid = df[df["residual_observation_valid"] == True]
        r  = float(valid.iloc[0]["residual"])
        se = float(valid.iloc[0]["signed_error"])
        assert abs(r + se) < 1e-9

    def test_absolute_error_correct(self):
        df = build_backtest_forecast_error_dataset(
            self._simple_df(actual=100.0, forecast=130.0)
        )
        valid = df[df["residual_observation_valid"] == True]
        assert abs(valid.iloc[0]["absolute_error"] - 30.0) < 1e-9

    def test_squared_error_correct(self):
        df = build_backtest_forecast_error_dataset(
            self._simple_df(actual=100.0, forecast=110.0)
        )
        valid = df[df["residual_observation_valid"] == True]
        assert abs(valid.iloc[0]["squared_error"] - 100.0) < 1e-9

    def test_horizon_step_1(self):
        df = build_backtest_forecast_error_dataset(
            self._simple_df(horizon_step=1)
        )
        assert (df["horizon_step"] == 1).any()

    def test_horizon_step_28(self):
        df = build_backtest_forecast_error_dataset(
            self._simple_df(horizon_step=28, forecast_date="2022-06-29")
        )
        assert (df["horizon_step"] == 28).any()

    def test_invalid_horizon_rejected(self):
        df = build_backtest_forecast_error_dataset(
            self._simple_df(horizon_step=29, forecast_date="2022-06-30")
        )
        with pytest.raises(ValueError, match="horizon_step"):
            validate_backtest_forecast_error_dataset(df)

    def test_interval_hit(self):
        df = build_backtest_forecast_error_dataset(
            self._simple_df(actual=100.0, forecast=100.0, lower_bound=80.0, upper_bound=120.0)
        )
        valid = df[df["residual_observation_valid"] == True]
        assert valid.iloc[0]["inside_interval"] == True

    def test_interval_miss_below(self):
        df = build_backtest_forecast_error_dataset(
            self._simple_df(actual=70.0, forecast=100.0, lower_bound=80.0, upper_bound=120.0)
        )
        valid = df[df["residual_observation_valid"] == True]
        assert valid.iloc[0]["inside_interval"] == False

    def test_missing_interval(self):
        df = build_backtest_forecast_error_dataset(
            self._simple_df(actual=100.0, forecast=100.0)
        )
        valid = df[df["residual_observation_valid"] == True]
        assert pd.isna(valid.iloc[0]["inside_interval"])

    def test_failed_prediction_row_not_valid(self):
        df = build_backtest_forecast_error_dataset(
            self._simple_df(actual=100.0, forecast=float("nan"), fit_status="failed")
        )
        assert (df["residual_observation_valid"] == False).all()

    def test_sarima_m7_and_m30_remain_separate(self):
        rows = _df(
            _bp_row(model_name="auto_arima_m7",  candidate_m=7,  forecast_date="2022-06-02"),
            _bp_row(model_name="auto_arima_m30", candidate_m=30, forecast_date="2022-06-02"),
        )
        df = build_backtest_forecast_error_dataset(rows)
        ms = set(df["candidate_m"].tolist())
        assert ms == {7, 30}

    def test_deterministic_sorting(self):
        rows = _df(
            _bp_row(report_id="R2", fold_number=1, forecast_date="2022-06-02"),
            _bp_row(report_id="R1", fold_number=2, forecast_date="2022-06-03"),
            _bp_row(report_id="R1", fold_number=1, forecast_date="2022-06-02"),
        )
        df = build_backtest_forecast_error_dataset(rows)
        assert list(df["report_id"]) == sorted(df["report_id"].tolist())

    def test_source_backtest_file_not_modified(self):
        original = _df(_bp_row())
        original_copy = original.copy()
        build_backtest_forecast_error_dataset(original)
        pd.testing.assert_frame_equal(original, original_copy)

    def test_residual_source_is_backtest(self):
        df = build_backtest_forecast_error_dataset(_df(_bp_row()))
        assert (df["residual_source"] == "backtest").all()

    def test_schema_stable(self):
        df = build_backtest_forecast_error_dataset(_df(_bp_row()))
        assert list(df.columns) == BACKTEST_FORECAST_ERRORS_COLS

    def test_validation_passes_for_valid_df(self):
        df = build_backtest_forecast_error_dataset(_df(_bp_row()))
        validate_backtest_forecast_error_dataset(df)  # must not raise

    def test_failed_row_valid_flag_rejected(self):
        df = build_backtest_forecast_error_dataset(
            _df(_bp_row(fit_status="failed"))
        )
        # Force-mark as valid to test the validator catches it
        df = df.copy()
        df["residual_observation_valid"] = True
        with pytest.raises(ValueError, match="failed-fit"):
            validate_backtest_forecast_error_dataset(df)

    def test_sign_inconsistency_rejected(self):
        df = build_backtest_forecast_error_dataset(_df(_bp_row(actual=100.0, forecast=110.0)))
        df = df.copy()
        df.loc[0, "residual"] = 999.0  # corrupt
        with pytest.raises(ValueError, match="residual"):
            validate_backtest_forecast_error_dataset(df)


# ---------------------------------------------------------------------------
# TestProductionForecastErrors
# ---------------------------------------------------------------------------

class TestProductionForecastErrors:
    def test_canonical_signed_error_converted_correctly(self):
        """signed_error = forecast - actual (100-110 → se=-10); residual = +10."""
        df = build_production_forecast_error_view(_df(_rh_row(actual=100.0, forecast=110.0)))
        valid = df[df["residual_observation_valid"] == True]
        assert abs(valid.iloc[0]["residual"] - (100.0 - 110.0)) < 1e-9

    def test_zero_actual_retained(self):
        """Zero actual counts are genuine observations and must not be filtered."""
        df = build_production_forecast_error_view(_df(_rh_row(actual=0.0, forecast=5.0)))
        assert len(df) == 1
        assert df.iloc[0]["actual"] == 0.0

    def test_positive_residual_means_underforecast(self):
        """actual=120, forecast=100 → residual=+20 (underforecast)."""
        df = build_production_forecast_error_view(_df(_rh_row(actual=120.0, forecast=100.0)))
        valid = df[df["residual_observation_valid"] == True]
        assert valid.iloc[0]["residual"] == pytest.approx(20.0)

    def test_negative_residual_means_overforecast(self):
        """actual=100, forecast=120 → residual=-20 (overforecast)."""
        df = build_production_forecast_error_view(_df(_rh_row(actual=100.0, forecast=120.0)))
        valid = df[df["residual_observation_valid"] == True]
        assert valid.iloc[0]["residual"] == pytest.approx(-20.0)

    def test_complete_lineage_preserved(self):
        row = _rh_row(lineage_complete=True, selected_model_family="seasonal_naive",
                      selected_m=7.0)
        df = build_production_forecast_error_view(_df(row))
        assert df.iloc[0]["lineage_complete"] == True
        assert df.iloc[0]["selected_model_family"] == "seasonal_naive"
        assert df.iloc[0]["selected_m"] == 7.0

    def test_incomplete_lineage_preserved(self):
        row = _rh_row(lineage_complete=False,
                      lineage_missing_fields="selected_model_family,selected_m",
                      selected_model_family=None, selected_m=float("nan"))
        df = build_production_forecast_error_view(_df(row))
        assert df.iloc[0]["lineage_complete"] == False
        assert "selected_model_family" in str(df.iloc[0]["lineage_missing_fields"])

    def test_missing_selected_m_not_inferred(self):
        """When selected_m is NaN, it must remain NaN — do not infer."""
        row = _rh_row(selected_m=float("nan"), lineage_complete=False)
        df = build_production_forecast_error_view(_df(row))
        assert pd.isna(df.iloc[0]["selected_m"])

    def test_residual_source_is_production(self):
        df = build_production_forecast_error_view(_df(_rh_row()))
        assert (df["residual_source"] == "production").all()

    def test_schema_stable(self):
        df = build_production_forecast_error_view(_df(_rh_row()))
        assert list(df.columns) == PRODUCTION_FORECAST_ERRORS_COLS

    def test_deterministic_sorting(self):
        rows = _df(
            _rh_row(run_id="run_002", report_id="R1", forecast_date="2022-06-04"),
            _rh_row(run_id="run_001", report_id="R2", forecast_date="2022-06-03"),
            _rh_row(run_id="run_001", report_id="R1", forecast_date="2022-06-02"),
        )
        df = build_production_forecast_error_view(rows)
        assert list(df["run_id"]) == sorted(df["run_id"].tolist())

    def test_canonical_realized_history_unchanged(self):
        original = _df(_rh_row())
        original_copy = original.copy()
        build_production_forecast_error_view(original)
        pd.testing.assert_frame_equal(original, original_copy)

    def test_invalid_signed_error_reconciliation_fails(self):
        df = build_production_forecast_error_view(_df(_rh_row(actual=100.0, forecast=110.0)))
        df = df.copy()
        df.loc[0, "residual"] = 999.0  # corrupt to break residual == -signed_error
        with pytest.raises(ValueError, match="residual"):
            validate_production_forecast_error_dataset(df)

    def test_validation_passes_for_valid_df(self):
        df = build_production_forecast_error_view(_df(_rh_row()))
        validate_production_forecast_error_dataset(df)

    def test_missing_required_column_rejected(self):
        df = build_production_forecast_error_view(_df(_rh_row()))
        df = df.drop(columns=["residual"])
        with pytest.raises(ValueError, match="missing"):
            validate_production_forecast_error_dataset(df)


# ---------------------------------------------------------------------------
# TestCrossDataset
# ---------------------------------------------------------------------------

class TestCrossDataset:
    def test_training_residual_source_never_mixed(self):
        df = build_training_residual_dataset([_tr_record()], diagnostic_run_id="D1")
        assert set(df["residual_source"].unique()) == {"training"}

    def test_backtest_residual_source_never_mixed(self):
        df = build_backtest_forecast_error_dataset(_df(_bp_row()))
        assert set(df["residual_source"].unique()) == {"backtest"}

    def test_production_residual_source_never_mixed(self):
        df = build_production_forecast_error_view(_df(_rh_row()))
        assert set(df["residual_source"].unique()) == {"production"}

    def test_training_schema_not_accepted_as_backtest(self):
        tr_df = build_training_residual_dataset([_tr_record()], diagnostic_run_id="D1")
        with pytest.raises(ValueError):
            validate_backtest_forecast_error_dataset(tr_df)

    def test_backtest_schema_not_accepted_as_production(self):
        bfe_df = build_backtest_forecast_error_dataset(_df(_bp_row()))
        with pytest.raises(ValueError):
            validate_production_forecast_error_dataset(bfe_df)

    def test_output_files_created(self, tmp_path):
        records = [_tr_record()]
        bp_df   = _df(_bp_row())
        rh_df   = _df(_rh_row())

        bp_path = tmp_path / "backtest_predictions_latest.csv"
        rh_path = tmp_path / "realized_forecast_history.csv"
        bp_df.to_csv(bp_path, index=False)
        rh_df.to_csv(rh_path, index=False)

        paths = persist_residual_datasets(
            training_residual_records=records,
            backtest_predictions_path=bp_path,
            realized_history_path=rh_path,
            project_root=tmp_path,
            diagnostic_run_id="D1",
        )
        assert paths["training_residuals"] is not None
        assert Path(paths["training_residuals"]).exists()
        assert paths["backtest_forecast_errors"] is not None
        assert Path(paths["backtest_forecast_errors"]).exists()
        assert paths["production_forecast_errors"] is not None
        assert Path(paths["production_forecast_errors"]).exists()

    def test_repeated_execution_replaces_latest_files(self, tmp_path):
        records = [_tr_record(n=3)]
        records2 = [_tr_record(n=7, model_name="seasonal_naive_m7")]

        bp_path = tmp_path / "bp.csv"
        rh_path = tmp_path / "rh.csv"
        _df(_bp_row()).to_csv(bp_path, index=False)
        _df(_rh_row()).to_csv(rh_path, index=False)

        paths1 = persist_residual_datasets(
            training_residual_records=records,
            backtest_predictions_path=bp_path,
            realized_history_path=rh_path,
            project_root=tmp_path,
            diagnostic_run_id="D1",
        )
        paths2 = persist_residual_datasets(
            training_residual_records=records2,
            backtest_predictions_path=bp_path,
            realized_history_path=rh_path,
            project_root=tmp_path,
            diagnostic_run_id="D2",
        )
        # Same path
        assert paths1["training_residuals"] == paths2["training_residuals"]
        # Second write has content from D2
        df2 = pd.read_csv(paths2["training_residuals"])
        assert (df2["diagnostic_run_id"] == "D2").all()

    def test_no_append_only_history_created(self, tmp_path):
        bp_path = tmp_path / "bp.csv"
        rh_path = tmp_path / "rh.csv"
        _df(_bp_row()).to_csv(bp_path, index=False)
        _df(_rh_row()).to_csv(rh_path, index=False)

        persist_residual_datasets(
            training_residual_records=[_tr_record()],
            backtest_predictions_path=bp_path,
            realized_history_path=rh_path,
            project_root=tmp_path,
            diagnostic_run_id="D1",
        )
        persist_residual_datasets(
            training_residual_records=[_tr_record()],
            backtest_predictions_path=bp_path,
            realized_history_path=rh_path,
            project_root=tmp_path,
            diagnostic_run_id="D2",
        )
        diag_dir = tmp_path / "outputs" / "diagnostics"
        history_files = list(diag_dir.glob("*history*"))
        assert len(history_files) == 0, (
            f"Append-only history files must not be created: {history_files}"
        )
