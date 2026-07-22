"""Tests for src.models.production_forecast — the production forecasting phase.

These tests prove the six required guarantees:
1. production fit uses all available history
2. production forecast starts after the final observed date
3. horizon equals 28 days
4. evaluation forecasts are not published as future forecasts
5. selected model metadata is preserved

All tests use only naive / seasonal_naive models — no pmdarima / statsmodels
dependency so the suite runs without optional packages.

Fixture conventions
-------------------
_make_series(n_days, start)  → pd.Series with a clean DatetimeIndex
_make_selection(rows)        → pd.DataFrame matching select_models output schema
_run_id / _generated_at      → stable test values for run lineage fields
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.config.forecasting import FORECAST_HORIZON_DAYS
from src.models.production_forecast import (
    PRODUCTION_FORECAST_COLS,
    build_production_forecast,
    refit_and_forecast,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_RUN_ID = "TEST_RUN_001"
_SEL_RUN_ID = "TEST_SEL_RUN_001"
_GENERATED_AT = pd.Timestamp("2024-06-01 09:00:00")
_SERIES_START = pd.Timestamp("2023-01-01")


def _make_series(n_days: int = 200, start: pd.Timestamp = _SERIES_START) -> pd.Series:
    """Return a clean daily series with a DatetimeIndex — no gaps, non-negative."""
    idx = pd.date_range(start=start, periods=n_days, freq="D")
    rng = np.random.default_rng(42)
    values = (rng.integers(5, 20, size=n_days)).astype(float)
    return pd.Series(values, index=idx, name="daily_views")


def _make_selection(
    report_id: str = "R_001",
    selected_model: str = "naive",
    selection_status: str = "selected",
    selection_reason: str = "Naive selected: test fixture.",
    valid_folds: int = 4,
    median_mase: float = 0.80,
    mean_wape: float = 0.30,
    mean_bias: float = 0.1,
    fold_win_rate: float = 0.5,
    seasonal_naive_median_mase: float = 1.00,
    improvement_vs_seasonal_naive_pct: float = 20.0,
) -> pd.DataFrame:
    """Return a minimal selection DataFrame (output of select_models)."""
    return pd.DataFrame([{
        "report_id": report_id,
        "selected_model": selected_model if selection_status == "selected" else None,
        "selection_status": selection_status,
        "selection_reason": selection_reason,
        "valid_folds": valid_folds,
        "median_mase": median_mase,
        "mean_wape": mean_wape,
        "mean_bias": mean_bias,
        "fold_win_rate": fold_win_rate,
        "seasonal_naive_median_mase": seasonal_naive_median_mase,
        "improvement_vs_seasonal_naive_pct": improvement_vs_seasonal_naive_pct,
    }])


# ---------------------------------------------------------------------------
# 1. Production fit uses all available history
# ---------------------------------------------------------------------------

class TestProductionFitUsesFullHistory:
    """training_start and training_cutoff must reflect the complete input series."""

    def test_training_start_equals_series_first_date(self):
        series = _make_series(n_days=300)
        sel = _make_selection()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        fc_rows = result[result["horizon_step"].notna()]
        assert fc_rows.iloc[0]["training_start"] == series.index.min()

    def test_training_cutoff_equals_series_last_date(self):
        series = _make_series(n_days=300)
        sel = _make_selection()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        fc_rows = result[result["horizon_step"].notna()]
        assert fc_rows.iloc[0]["training_cutoff"] == series.index.max()

    def test_different_length_series_reflected_in_cutoff(self):
        """Two series of different lengths must each report their own cutoff."""
        series_short = _make_series(n_days=200)
        series_long = _make_series(n_days=400)

        sel = pd.concat([
            _make_selection(report_id="R_SHORT"),
            _make_selection(report_id="R_LONG"),
        ], ignore_index=True)

        result = build_production_forecast(
            sel,
            {"R_SHORT": series_short, "R_LONG": series_long},
            _RUN_ID, _GENERATED_AT,
        )

        short_cutoff = result[result["report_id"] == "R_SHORT"]["training_cutoff"].iloc[0]
        long_cutoff = result[result["report_id"] == "R_LONG"]["training_cutoff"].iloc[0]
        assert short_cutoff == series_short.index.max()
        assert long_cutoff == series_long.index.max()
        assert long_cutoff > short_cutoff

    def test_full_history_means_no_train_test_split(self):
        """refit_and_forecast must use all observations, not an 80/20 split."""
        series = _make_series(n_days=250)
        result = refit_and_forecast("seasonal_naive", series, horizon=28)
        # The forecast starts one day after the last observed date.
        # If a train/test split were used (80 %) the cutoff would be around
        # day 200, and the forecast index would start there — not at day 251.
        expected_start = series.index.max() + pd.Timedelta(days=1)
        assert result.forecast.index[0] == expected_start


# ---------------------------------------------------------------------------
# 2. Production forecast starts after the final observed date
# ---------------------------------------------------------------------------

class TestForecastStartsAfterObservedData:
    """Every forecast_date must be strictly after training_cutoff."""

    def test_all_forecast_dates_after_training_cutoff(self):
        series = _make_series()
        sel = _make_selection()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        fc = result[result["horizon_step"].notna()].copy()
        fc["forecast_date"] = pd.to_datetime(fc["forecast_date"])
        fc["training_cutoff"] = pd.to_datetime(fc["training_cutoff"])
        assert (fc["forecast_date"] > fc["training_cutoff"]).all()

    def test_first_forecast_date_is_day_after_cutoff(self):
        series = _make_series()
        sel = _make_selection()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        step1 = result[result["horizon_step"] == 1].iloc[0]
        expected = pd.Timestamp(step1["training_cutoff"]) + pd.Timedelta(days=1)
        assert pd.Timestamp(step1["forecast_date"]) == expected

    def test_no_forecast_date_in_training_window(self):
        series = _make_series(n_days=365)
        sel = _make_selection()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        training_dates = set(series.index.normalize())
        fc_dates = set(pd.to_datetime(result["forecast_date"].dropna()).dt.normalize())
        overlap = training_dates & fc_dates
        assert not overlap, f"Forecast dates overlap with training: {overlap}"

    def test_refit_forecast_index_starts_after_series(self):
        """refit_and_forecast contract: index[0] > series.index[-1]."""
        series = _make_series(n_days=180)
        result = refit_and_forecast("naive", series, horizon=28)
        assert result.forecast is not None
        assert result.forecast.index[0] > series.index[-1]


# ---------------------------------------------------------------------------
# 3. Horizon equals 28 days
# ---------------------------------------------------------------------------

class TestHorizonIs28Days:
    """Successful production forecasts must produce exactly FORECAST_HORIZON_DAYS rows."""

    def test_exactly_28_rows_per_successful_report(self):
        series = _make_series()
        sel = _make_selection()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        fc = result[result["horizon_step"].notna()]
        assert len(fc) == FORECAST_HORIZON_DAYS

    def test_horizon_steps_are_1_through_28(self):
        series = _make_series()
        sel = _make_selection()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        steps = sorted(result["horizon_step"].dropna().astype(int).tolist())
        assert steps == list(range(1, FORECAST_HORIZON_DAYS + 1))

    def test_custom_horizon_respected(self):
        series = _make_series()
        sel = _make_selection()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT, horizon=7
        )
        fc = result[result["horizon_step"].notna()]
        assert len(fc) == 7
        assert sorted(fc["horizon_step"].tolist()) == list(range(1, 8))

    def test_multi_report_each_gets_28_rows(self):
        series_a = _make_series(n_days=200)
        series_b = _make_series(n_days=250, start=pd.Timestamp("2022-01-01"))

        sel = pd.concat([
            _make_selection(report_id="R_A", selected_model="naive"),
            _make_selection(report_id="R_B", selected_model="seasonal_naive"),
        ], ignore_index=True)

        result = build_production_forecast(
            sel,
            {"R_A": series_a, "R_B": series_b},
            _RUN_ID, _GENERATED_AT,
        )
        for rid in ["R_A", "R_B"]:
            n = len(result[(result["report_id"] == rid) & result["horizon_step"].notna()])
            assert n == FORECAST_HORIZON_DAYS, f"{rid}: expected 28 rows, got {n}"

    def test_refit_returns_28_forecast_values(self):
        series = _make_series()
        result = refit_and_forecast("naive", series)
        assert len(result.forecast) == FORECAST_HORIZON_DAYS


# ---------------------------------------------------------------------------
# 4. Evaluation forecasts are not published as future forecasts
# ---------------------------------------------------------------------------

class TestEvaluationForecastsNotPublished:
    """Production rows must not contain any historical backtest evaluation dates."""

    def test_production_output_has_no_historical_dates(self):
        """Production rows must be in the future relative to training_cutoff."""
        series = _make_series(n_days=365)
        sel = _make_selection()

        # Simulate what the evaluation phase would produce: it makes predictions
        # for held-out windows *within* the observed history.  We confirm that
        # none of those historical dates end up in the production output.
        from src.models.backtesting import generate_rolling_splits
        from src.models.candidates import forecast_naive

        folds, _ = generate_rolling_splits(series, horizon=28, n_folds=2, step=28, min_train_size=90)
        eval_test_dates: set[pd.Timestamp] = set()
        for fold in folds:
            eval_test_dates.update(fold.test_series.index)

        production_df = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        prod_dates = set(
            pd.to_datetime(production_df["forecast_date"].dropna()).dt.normalize()
        )
        overlap = eval_test_dates & prod_dates
        assert not overlap, (
            f"Production rows overlap with backtest evaluation dates: {sorted(overlap)[:3]}"
        )

    def test_build_production_forecast_never_calls_model_update(self, monkeypatch):
        """Confirm no pmdarima model.update is invoked in the production path.

        We monkeypatch the pmdarima ARIMA.update to raise if called, then
        verify naive/seasonal_naive production forecasting runs cleanly.
        """
        try:
            import pmdarima
            original_update = pmdarima.arima.ARIMA.update

            def _fail_update(self, *args, **kwargs):
                raise AssertionError(
                    "model.update was called in the production path — "
                    "this violates requirement 1."
                )

            monkeypatch.setattr(pmdarima.arima.ARIMA, "update", _fail_update)
        except ImportError:
            pass  # pmdarima not installed — update cannot be called anyway

        series = _make_series()
        sel = _make_selection(selected_model="naive")
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        assert len(result[result["horizon_step"].notna()]) == 28

    def test_no_actual_values_column_in_production_output(self):
        """Production rows should have no 'actual' column — future dates are unknown."""
        series = _make_series()
        sel = _make_selection()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        assert "actual" not in result.columns, (
            "Production forecast must not contain 'actual' — it would imply"
            " evaluation rows were mixed into the output."
        )

    def test_output_columns_match_production_schema_not_evaluation_schema(self):
        """The production output must follow PRODUCTION_FORECAST_COLS, not
        the evaluation backtest schema (which has fold_number, cutoff_date, etc.)."""
        series = _make_series()
        sel = _make_selection()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        assert list(result.columns) == PRODUCTION_FORECAST_COLS
        evaluation_only_cols = {"fold_number", "cutoff_date", "horizon_step_in_fold", "actual"}
        leaked = evaluation_only_cols & set(result.columns)
        assert not leaked, f"Evaluation-only columns leaked into production output: {leaked}"


# ---------------------------------------------------------------------------
# 5. Selected model metadata is preserved
# ---------------------------------------------------------------------------

class TestSelectedModelMetadataPreserved:
    """selection_reason, selected_model, and selection lineage must appear in output."""

    def test_selected_model_propagated(self):
        sel = _make_selection(selected_model="seasonal_naive")
        series = _make_series()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        assert (result["selected_model"] == "seasonal_naive").all()

    def test_selection_reason_propagated(self):
        reason = "Seasonal naive selected: lowest median MASE (0.750)."
        sel = _make_selection(selection_reason=reason)
        series = _make_series()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        assert (result["selection_reason"] == reason).all()

    def test_run_id_stamped_on_every_row(self):
        sel = _make_selection()
        series = _make_series()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        assert (result["run_id"] == _RUN_ID).all()

    def test_selection_run_id_distinct_from_run_id(self):
        """When selection happened in a different run, selection_run_id differs."""
        sel = _make_selection()
        series = _make_series()
        result = build_production_forecast(
            sel, {"R_001": series},
            run_id="PROD_RUN_002",
            generated_at=_GENERATED_AT,
            selection_run_id="SEL_RUN_001",
        )
        assert (result["run_id"] == "PROD_RUN_002").all()
        assert (result["selection_run_id"] == "SEL_RUN_001").all()

    def test_selection_run_id_defaults_to_run_id(self):
        """When selection_run_id is not supplied, it should equal run_id."""
        sel = _make_selection()
        series = _make_series()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        assert (result["selection_run_id"] == _RUN_ID).all()

    def test_generated_at_stamped_on_every_row(self):
        sel = _make_selection()
        series = _make_series()
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT
        )
        assert (pd.to_datetime(result["generated_at"]) == _GENERATED_AT).all()

    def test_report_id_preserved(self):
        sel = pd.concat([
            _make_selection(report_id="REPORT_ALPHA"),
            _make_selection(report_id="REPORT_BETA"),
        ], ignore_index=True)
        series = {
            "REPORT_ALPHA": _make_series(n_days=200),
            "REPORT_BETA": _make_series(n_days=250),
        }
        result = build_production_forecast(sel, series, _RUN_ID, _GENERATED_AT)
        assert set(result["report_id"]) == {"REPORT_ALPHA", "REPORT_BETA"}


# ---------------------------------------------------------------------------
# 6. Model failure handling
# ---------------------------------------------------------------------------

class TestFailureHandling:
    """Failed and no-model reports produce placeholder rows, not silent drops."""

    def test_no_reliable_model_produces_placeholder_row(self):
        sel = _make_selection(
            selection_status="no_reliable_model",
            selection_reason="No reliable model: fewer than 3 valid folds (best was 1).",
        )
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT
        )
        assert len(result) == 1
        row = result.iloc[0]
        assert pd.isna(row["forecast"])
        assert pd.isna(row["horizon_step"])
        assert row["selected_model"] is None

    def test_missing_series_produces_placeholder_row(self):
        """When a selected report has no series in series_dict, preserve as failure."""
        sel = _make_selection()
        result = build_production_forecast(
            sel, {}, _RUN_ID, _GENERATED_AT   # empty series_dict
        )
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["forecast"])

    def test_unknown_model_name_produces_placeholder_row(self):
        sel = _make_selection(selected_model="nonexistent_model_xyz")
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT
        )
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["forecast"])

    def test_empty_selection_returns_empty_dataframe(self):
        result = build_production_forecast(
            pd.DataFrame(), {}, _RUN_ID, _GENERATED_AT
        )
        assert result.empty
        assert list(result.columns) == PRODUCTION_FORECAST_COLS

    def test_failure_row_preserves_selection_reason(self):
        reason = "No reliable model: all candidates excluded by bias guardrail."
        sel = _make_selection(
            selection_status="no_reliable_model",
            selection_reason=reason,
        )
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT
        )
        assert result.iloc[0]["selection_reason"] == reason

    def test_mixed_success_and_failure(self):
        """Reports with and without a model should both appear in the output."""
        sel = pd.concat([
            _make_selection(report_id="OK", selection_status="selected", selected_model="naive"),
            _make_selection(report_id="FAIL", selection_status="no_reliable_model"),
        ], ignore_index=True)
        series = {
            "OK": _make_series(n_days=200),
            "FAIL": _make_series(n_days=200),
        }
        result = build_production_forecast(sel, series, _RUN_ID, _GENERATED_AT)
        assert set(result["report_id"]) == {"OK", "FAIL"}
        ok_rows = result[result["report_id"] == "OK"]
        fail_rows = result[result["report_id"] == "FAIL"]
        assert len(ok_rows) == FORECAST_HORIZON_DAYS
        assert len(fail_rows) == 1
        assert pd.isna(fail_rows.iloc[0]["forecast"])


# ---------------------------------------------------------------------------
# 7. refit_and_forecast contract
# ---------------------------------------------------------------------------

class TestRefitAndForecastContract:
    """Unit-level checks on the core refitting function."""

    def test_returns_model_result(self):
        from src.models.candidates import ModelResult
        series = _make_series()
        result = refit_and_forecast("naive", series)
        assert isinstance(result, ModelResult)

    def test_naive_fit_status_ok(self):
        result = refit_and_forecast("naive", _make_series())
        assert result.fit_status == "ok"

    def test_seasonal_naive_fit_status_ok(self):
        result = refit_and_forecast("seasonal_naive", _make_series())
        assert result.fit_status == "ok"

    def test_unknown_model_returns_failed_status(self):
        result = refit_and_forecast("does_not_exist", _make_series())
        assert result.fit_status == "failed"
        assert result.forecast is None

    def test_forecast_values_non_negative(self):
        """All candidate models clip forecast to ≥ 0."""
        for model_name in ["naive", "seasonal_naive"]:
            result = refit_and_forecast(model_name, _make_series())
            assert (result.forecast >= 0).all(), f"{model_name}: negative forecast values"

    def test_forecast_length_equals_horizon(self):
        for model_name in ["naive", "seasonal_naive"]:
            result = refit_and_forecast(model_name, _make_series(), horizon=14)
            assert len(result.forecast) == 14

    def test_model_order_none_for_baseline_models(self):
        """Naive and seasonal_naive have no ARIMA order — model_order should be absent."""
        series = _make_series()
        sel = _make_selection(selected_model="naive")
        result = build_production_forecast(sel, {"R_001": series}, _RUN_ID, _GENERATED_AT)
        assert result.iloc[0]["model_order"] is None
        assert result.iloc[0]["seasonal_order"] is None


# ---------------------------------------------------------------------------
# 8. Output schema
# ---------------------------------------------------------------------------

class TestOutputSchema:
    def test_production_forecast_cols_present(self):
        sel = _make_selection()
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT
        )
        assert list(result.columns) == PRODUCTION_FORECAST_COLS

    def test_sorted_by_report_id_then_horizon_step(self):
        sel = pd.concat([
            _make_selection(report_id="Z_REPORT"),
            _make_selection(report_id="A_REPORT"),
        ], ignore_index=True)
        series = {
            "Z_REPORT": _make_series(n_days=200),
            "A_REPORT": _make_series(n_days=200),
        }
        result = build_production_forecast(sel, series, _RUN_ID, _GENERATED_AT)
        report_ids = result[result["horizon_step"].notna()]["report_id"].tolist()
        assert report_ids[:28] == ["A_REPORT"] * 28
        assert report_ids[28:] == ["Z_REPORT"] * 28

        steps = result[result["report_id"] == "A_REPORT"]["horizon_step"].dropna().astype(int).tolist()
        assert steps == list(range(1, 29))
