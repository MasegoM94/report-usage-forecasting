"""Tests for src.models.production_forecast — the production forecasting phase.

Guarantees verified
-------------------
1.  The seasonal period selected in backtesting is used exactly during production refitting.
2.  Production fit uses all available history.
3.  Production forecast starts after the final observed date.
4.  Horizon equals 28 days.
5.  Evaluation forecasts are not published as future forecasts.
6.  Selected model metadata is preserved in the output.
7.  Fallback logic uses the next ranked eligible candidate.
8.  Fallback lineage is fully recorded in the output.
9.  Previous production forecast history is never overwritten.
10. Seasonality profiling is not called again during production refitting.

All tests use only naive / seasonal_naive models unless monkeypatching is
explicitly required (e.g. SARIMA period-preservation tests), so the suite
runs without optional packages installed.

Fixture conventions
-------------------
_make_series(n_days, start)   → pd.Series with a clean DatetimeIndex
_make_selection(...)          → pd.DataFrame matching select_candidate_models output
_make_candidate_summary(...)  → pd.DataFrame matching summarise_candidate_performance output
_run_id / _generated_at       → stable test values for run lineage fields
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

import src.models.candidates as _candidates_module
from src.config.forecasting import FORECAST_HORIZON_DAYS, NON_SEASONAL_PERIOD
from src.models.production_forecast import (
    PRODUCTION_FORECAST_COLS,
    _refit_with_period,
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
    values = rng.integers(5, 20, size=n_days).astype(float)
    return pd.Series(values, index=idx, name="daily_views")


def _make_selection(
    report_id: str = "R_001",
    selected_model_family: str = "naive",
    selected_model_name: str = "naive",
    selected_m: int = NON_SEASONAL_PERIOD,
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
    """Return a minimal selection DataFrame (output of select_candidate_models)."""
    is_sel = selection_status == "selected"
    return pd.DataFrame([{
        "report_id": report_id,
        "selected_model_family": selected_model_family if is_sel else None,
        "selected_model_name": selected_model_name if is_sel else None,
        "selected_m": selected_m if is_sel else None,
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


def _make_candidate_summary(
    report_id: str = "R_001",
    model_family: str = "naive",
    model_name: str = "naive",
    candidate_m: int = NON_SEASONAL_PERIOD,
    median_mase: float = 1.00,
    mean_mae: float = 10.0,
    mean_bias: float = 0.5,
    absolute_mean_bias: float = 0.5,
    valid_folds: int = 4,
    has_sufficient_folds: bool = True,
) -> dict:
    """Return one candidate summary row dict."""
    return {
        "report_id": report_id,
        "model_family": model_family,
        "model_name": model_name,
        "candidate_m": candidate_m,
        "candidate_fold_count": valid_folds,
        "valid_folds": valid_folds,
        "failed_folds": 0,
        "has_sufficient_folds": has_sufficient_folds,
        "median_mase": median_mase,
        "mean_mase": median_mase,
        "mase_std": 0.05,
        "mean_wape": 0.30,
        "mean_mae": mean_mae,
        "mean_rmse": mean_mae * 1.2,
        "mean_bias": mean_bias,
        "absolute_mean_bias": absolute_mean_bias,
        "fold_win_count": 2,
        "fold_win_rate": 0.5,
        "mean_interval_coverage": np.nan,
        "mean_interval_width": np.nan,
    }


# ===========================================================================
# 1. Seasonal period preservation during production refitting
# ===========================================================================

class TestSeasonalPeriodPreserved:
    """The selected_m must be passed explicitly to the candidate function — never
    substituted with a global default, a re-detected period, or an Auto-ARIMA default."""

    def _capture_period(self, monkeypatch, family_attr: str):
        """Monkeypatch the named candidate function and return a list that will
        accumulate every seasonal_period argument it receives."""
        captured: list[int] = []

        original_fn = getattr(_candidates_module, family_attr)

        def _fake(series, horizon, seasonal_period=7, **kwargs):
            captured.append(seasonal_period)
            # Return a valid (non-failed) ModelResult using a real naive forecast
            naive = _candidates_module.forecast_naive(series, horizon)
            return _candidates_module.ModelResult(
                model_name=f"{family_attr}_m{seasonal_period}",
                forecast=naive.forecast,
                lower_bound=None,
                upper_bound=None,
                forecast_raw=naive.forecast_raw,
                lower_bound_raw=None,
                upper_bound_raw=None,
                model_metadata={"seasonal_period": seasonal_period},
                fit_status="ok",
            )

        monkeypatch.setattr(_candidates_module, family_attr, _fake)
        return captured

    def test_sarima_m7_uses_period_7(self, monkeypatch):
        """auto_arima family with selected_m=7 must call the fitting function with seasonal_period=7."""
        captured = self._capture_period(monkeypatch, "forecast_auto_arima")
        series = _make_series()
        _refit_with_period("auto_arima", 7, series, FORECAST_HORIZON_DAYS)
        assert captured == [7], f"Expected seasonal_period=7, got {captured}"

    def test_sarima_m30_uses_period_30(self, monkeypatch):
        """auto_arima family with selected_m=30 must call the fitting function with seasonal_period=30."""
        captured = self._capture_period(monkeypatch, "forecast_auto_arima")
        series = _make_series()
        _refit_with_period("auto_arima", 30, series, FORECAST_HORIZON_DAYS)
        assert captured == [30], f"Expected seasonal_period=30, got {captured}"

    def test_seasonal_naive_m28_uses_period_28(self, monkeypatch):
        """seasonal_naive with selected_m=28 must call the fitting function with seasonal_period=28."""
        captured = self._capture_period(monkeypatch, "forecast_seasonal_naive")
        series = _make_series()
        _refit_with_period("seasonal_naive", 28, series, FORECAST_HORIZON_DAYS)
        assert captured == [28], f"Expected seasonal_period=28, got {captured}"

    def test_arima_m1_uses_non_seasonal(self, monkeypatch):
        """auto_arima with selected_m=1 must call with seasonal_period=1 (non-seasonal ARIMA)."""
        captured = self._capture_period(monkeypatch, "forecast_auto_arima")
        series = _make_series()
        _refit_with_period("auto_arima", NON_SEASONAL_PERIOD, series, FORECAST_HORIZON_DAYS)
        assert captured == [NON_SEASONAL_PERIOD], f"Expected seasonal_period=1, got {captured}"

    def test_ets_m7_uses_period_7(self, monkeypatch):
        captured = self._capture_period(monkeypatch, "forecast_ets")
        _refit_with_period("ets", 7, _make_series(), FORECAST_HORIZON_DAYS)
        assert captured == [7]

    def test_naive_has_no_period_parameter(self):
        """Naive model must not receive any period argument (no seasonal component)."""
        # We verify by calling _refit_with_period and checking it doesn't fail
        # with an unexpected keyword argument — naive ignores the period entirely.
        result = _refit_with_period("naive", NON_SEASONAL_PERIOD, _make_series(), 28)
        assert result.fit_status == "ok"
        assert result.forecast is not None

    def test_refit_and_forecast_uses_encoded_period_from_name(self, monkeypatch):
        """refit_and_forecast('auto_arima_m30', series) must use period=30."""
        captured = self._capture_period(monkeypatch, "forecast_auto_arima")
        series = _make_series()
        refit_and_forecast("auto_arima_m30", series, horizon=28)
        assert captured == [30]

    def test_refit_and_forecast_explicit_seasonal_period_overrides_name(self, monkeypatch):
        """Explicit seasonal_period parameter overrides the period encoded in the name."""
        captured = self._capture_period(monkeypatch, "forecast_auto_arima")
        series = _make_series()
        refit_and_forecast("auto_arima_m7", series, horizon=28, seasonal_period=14)
        assert captured == [14]

    def test_selected_m_propagated_into_production_rows(self, monkeypatch):
        """selected_m from the selection output appears in every production forecast row."""
        # Use seasonal_naive so no pmdarima needed
        sel = _make_selection(
            selected_model_family="seasonal_naive",
            selected_model_name="seasonal_naive_m14",
            selected_m=14,
        )
        series = _make_series(n_days=300)
        result = build_production_forecast(sel, {"R_001": series}, _RUN_ID, _GENERATED_AT)
        fc = result[result["horizon_step"].notna()]
        assert (fc["selected_m"] == 14).all()

    def test_selected_model_family_propagated_into_production_rows(self):
        sel = _make_selection(
            selected_model_family="seasonal_naive",
            selected_model_name="seasonal_naive_m7",
            selected_m=7,
        )
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT
        )
        fc = result[result["horizon_step"].notna()]
        assert (fc["selected_model_family"] == "seasonal_naive").all()


# ===========================================================================
# 2. Production fit uses all available history
# ===========================================================================

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
        expected_start = series.index.max() + pd.Timedelta(days=1)
        assert result.forecast.index[0] == expected_start

    def test_production_fitting_uses_all_history(self):
        """The training_cutoff must equal series.index.max() — no hold-out split."""
        series = _make_series(n_days=365)
        sel = _make_selection(
            selected_model_family="seasonal_naive",
            selected_model_name="seasonal_naive_m7",
            selected_m=7,
        )
        result = build_production_forecast(sel, {"R_001": series}, _RUN_ID, _GENERATED_AT)
        fc = result[result["horizon_step"].notna()]
        assert fc.iloc[0]["training_cutoff"] == series.index.max()


# ===========================================================================
# 3. Production forecast starts after the final observed date
# ===========================================================================

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
        series = _make_series(n_days=180)
        result = refit_and_forecast("naive", series, horizon=28)
        assert result.forecast is not None
        assert result.forecast.index[0] > series.index[-1]


# ===========================================================================
# 4. Horizon equals 28 days
# ===========================================================================

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

    def test_production_horizon_always_28(self):
        """The default horizon must equal FORECAST_HORIZON_DAYS (28)."""
        assert FORECAST_HORIZON_DAYS == 28

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
            _make_selection(report_id="R_A", selected_model_family="naive", selected_model_name="naive", selected_m=1),
            _make_selection(report_id="R_B", selected_model_family="seasonal_naive", selected_model_name="seasonal_naive_m7", selected_m=7),
        ], ignore_index=True)
        result = build_production_forecast(
            sel, {"R_A": series_a, "R_B": series_b}, _RUN_ID, _GENERATED_AT,
        )
        for rid in ["R_A", "R_B"]:
            n = len(result[(result["report_id"] == rid) & result["horizon_step"].notna()])
            assert n == FORECAST_HORIZON_DAYS, f"{rid}: expected 28, got {n}"

    def test_refit_returns_28_forecast_values(self):
        result = refit_and_forecast("naive", _make_series())
        assert len(result.forecast) == FORECAST_HORIZON_DAYS


# ===========================================================================
# 5. Evaluation forecasts are not published as future forecasts
# ===========================================================================

class TestEvaluationForecastsNotPublished:

    def test_production_output_has_no_historical_dates(self):
        series = _make_series(n_days=365)
        sel = _make_selection()
        from src.models.backtesting import generate_rolling_splits
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
        assert not overlap, f"Production rows overlap with backtest evaluation dates: {sorted(overlap)[:3]}"

    def test_build_production_forecast_never_calls_model_update(self, monkeypatch):
        try:
            import pmdarima
            def _fail_update(self, *args, **kwargs):
                raise AssertionError("model.update was called in the production path.")
            monkeypatch.setattr(pmdarima.arima.ARIMA, "update", _fail_update)
        except ImportError:
            pass
        series = _make_series()
        sel = _make_selection(selected_model_family="naive", selected_model_name="naive", selected_m=1)
        result = build_production_forecast(sel, {"R_001": series}, _RUN_ID, _GENERATED_AT)
        assert len(result[result["horizon_step"].notna()]) == 28

    def test_no_actual_values_column_in_production_output(self):
        series = _make_series()
        sel = _make_selection()
        result = build_production_forecast(sel, {"R_001": series}, _RUN_ID, _GENERATED_AT)
        assert "actual" not in result.columns

    def test_output_columns_match_production_schema_not_evaluation_schema(self):
        series = _make_series()
        sel = _make_selection()
        result = build_production_forecast(sel, {"R_001": series}, _RUN_ID, _GENERATED_AT)
        assert list(result.columns) == PRODUCTION_FORECAST_COLS
        evaluation_only = {"fold_number", "cutoff_date", "horizon_step_in_fold", "actual"}
        leaked = evaluation_only & set(result.columns)
        assert not leaked

    def test_seasonality_profiling_not_called_during_refitting(self, monkeypatch):
        """profile_seasonality must NOT be invoked in the production refitting path."""
        def _fail_if_called(*args, **kwargs):
            raise AssertionError(
                "profile_seasonality was called during production refitting — "
                "this violates the separation requirement: the selected period "
                "must be read from the selection output, not re-detected."
            )
        import src.models.production_forecast as _pf_mod
        # profile_seasonality must not be importable from the production module
        assert not hasattr(_pf_mod, "profile_seasonality"), (
            "production_forecast module must not import profile_seasonality"
        )
        # Confirm it is not called anywhere in the AST (excludes docstrings/comments)
        import ast, inspect, textwrap
        raw_src = inspect.getsource(_pf_mod)
        tree = ast.parse(textwrap.dedent(raw_src))
        call_names = [
            (node.func.id if isinstance(node.func, ast.Name) else
             node.func.attr if isinstance(node.func, ast.Attribute) else "")
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, (ast.Name, ast.Attribute))
        ]
        assert "profile_seasonality" not in call_names, (
            "production_forecast.py must not call profile_seasonality"
        )


# ===========================================================================
# 6. Selected model metadata is preserved
# ===========================================================================

class TestSelectedModelMetadataPreserved:

    def test_selected_model_family_propagated(self):
        sel = _make_selection(selected_model_family="seasonal_naive", selected_model_name="seasonal_naive_m7", selected_m=7)
        series = _make_series()
        result = build_production_forecast(sel, {"R_001": series}, _RUN_ID, _GENERATED_AT)
        fc = result[result["horizon_step"].notna()]
        assert (fc["selected_model_family"] == "seasonal_naive").all()

    def test_selected_model_name_propagated(self):
        sel = _make_selection(selected_model_family="seasonal_naive", selected_model_name="seasonal_naive_m7", selected_m=7)
        series = _make_series()
        result = build_production_forecast(sel, {"R_001": series}, _RUN_ID, _GENERATED_AT)
        fc = result[result["horizon_step"].notna()]
        assert (fc["selected_model_name"] == "seasonal_naive_m7").all()

    def test_selection_reason_propagated(self):
        reason = "Seasonal-naive m=7 selected: lowest median MASE (0.750)."
        sel = _make_selection(selection_reason=reason)
        result = build_production_forecast(sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT)
        assert (result["selection_reason"] == reason).all()

    def test_run_id_stamped_on_every_row(self):
        result = build_production_forecast(
            _make_selection(), {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT
        )
        assert (result["run_id"] == _RUN_ID).all()

    def test_selection_run_id_distinct_from_run_id(self):
        result = build_production_forecast(
            _make_selection(), {"R_001": _make_series()},
            run_id="PROD_RUN_002", generated_at=_GENERATED_AT,
            selection_run_id="SEL_RUN_001",
        )
        assert (result["run_id"] == "PROD_RUN_002").all()
        assert (result["selection_run_id"] == "SEL_RUN_001").all()

    def test_selection_run_id_defaults_to_run_id(self):
        result = build_production_forecast(
            _make_selection(), {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT
        )
        assert (result["selection_run_id"] == _RUN_ID).all()

    def test_valid_backtest_folds_propagated(self):
        sel = _make_selection(valid_folds=3)
        result = build_production_forecast(sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT)
        fc = result[result["horizon_step"].notna()]
        assert (fc["valid_backtest_folds"] == 3).all()

    def test_median_backtest_mase_propagated(self):
        sel = _make_selection(median_mase=0.63)
        result = build_production_forecast(sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT)
        fc = result[result["horizon_step"].notna()]
        assert (fc["median_backtest_mase"].round(4) == 0.63).all()

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


# ===========================================================================
# 7. Fallback logic
# ===========================================================================

class TestFallbackUsesNextRankedCandidate:
    """When the primary selected model fails to refit, the next eligible candidate
    from candidate_summary must be attempted in ranked order."""

    def _failing_fn(self, series, horizon, seasonal_period=7, **kwargs):
        from src.models.candidates import _failed
        return _failed(f"auto_arima_m{seasonal_period}", RuntimeError("mock failure"))

    def test_fallback_to_second_ranked_candidate_when_primary_fails(self, monkeypatch):
        monkeypatch.setattr(_candidates_module, "forecast_auto_arima", self._failing_fn)

        sel = _make_selection(
            selected_model_family="auto_arima",
            selected_model_name="auto_arima_m7",
            selected_m=7,
        )
        series = _make_series(n_days=300)
        candidate_summary = pd.DataFrame([
            _make_candidate_summary(
                model_family="auto_arima", model_name="auto_arima_m7",
                candidate_m=7, median_mase=0.65,
            ),
            _make_candidate_summary(
                model_family="seasonal_naive", model_name="seasonal_naive_m7",
                candidate_m=7, median_mase=0.85,
            ),
        ])
        result = build_production_forecast(
            sel, {"R_001": series}, _RUN_ID, _GENERATED_AT,
            candidate_summary=candidate_summary,
        )
        fc = result[result["horizon_step"].notna()]
        assert len(fc) == FORECAST_HORIZON_DAYS, "Fallback should produce 28 forecast rows"

    def test_fallback_skips_primary_candidate(self, monkeypatch):
        """The fallback list must exclude the originally selected (family, m) pair."""
        called_with: list[int] = []
        original_sn = _candidates_module.forecast_seasonal_naive

        def tracking_sn(series, horizon, seasonal_period=7, **kwargs):
            called_with.append(seasonal_period)
            return original_sn(series, horizon, seasonal_period=seasonal_period)

        monkeypatch.setattr(_candidates_module, "forecast_auto_arima", self._failing_fn)
        monkeypatch.setattr(_candidates_module, "forecast_seasonal_naive", tracking_sn)

        sel = _make_selection(
            selected_model_family="auto_arima", selected_model_name="auto_arima_m7", selected_m=7,
        )
        candidate_summary = pd.DataFrame([
            _make_candidate_summary(model_family="auto_arima", model_name="auto_arima_m7", candidate_m=7, median_mase=0.65),
            _make_candidate_summary(model_family="seasonal_naive", model_name="seasonal_naive_m7", candidate_m=7, median_mase=0.85),
        ])
        build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT,
            candidate_summary=candidate_summary,
        )
        # Only seasonal_naive should be tried as fallback (auto_arima m=7 excluded)
        assert called_with == [7]

    def test_no_fallback_without_candidate_summary(self, monkeypatch):
        """When no candidate_summary is provided, a failed primary produces a placeholder."""
        monkeypatch.setattr(_candidates_module, "forecast_auto_arima", self._failing_fn)
        sel = _make_selection(
            selected_model_family="auto_arima", selected_model_name="auto_arima_m7", selected_m=7,
        )
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT,
            candidate_summary=None,
        )
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["forecast"])

    def test_fallback_candidates_filtered_by_sufficient_folds(self, monkeypatch):
        """Candidates with has_sufficient_folds=False must be excluded from fallback."""
        monkeypatch.setattr(_candidates_module, "forecast_auto_arima", self._failing_fn)
        sel = _make_selection(
            selected_model_family="auto_arima", selected_model_name="auto_arima_m7", selected_m=7,
        )
        candidate_summary = pd.DataFrame([
            _make_candidate_summary(model_family="auto_arima", model_name="auto_arima_m7", candidate_m=7, median_mase=0.65, has_sufficient_folds=True),
            _make_candidate_summary(model_family="naive", model_name="naive", candidate_m=1, median_mase=0.90, has_sufficient_folds=False),
        ])
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT,
            candidate_summary=candidate_summary,
        )
        # naive is excluded (insufficient folds) → no valid fallback → failure row
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["forecast"])


# ===========================================================================
# 8. Fallback lineage
# ===========================================================================

class TestFallbackLineageRecorded:
    """Fallback events must be fully documented in the output."""

    def _failing_auto_arima(self, series, horizon, seasonal_period=7, **kwargs):
        from src.models.candidates import _failed
        return _failed(f"auto_arima_m{seasonal_period}", RuntimeError("deliberate test failure"))

    def test_fallback_used_flag_set_when_fallback_occurs(self, monkeypatch):
        monkeypatch.setattr(_candidates_module, "forecast_auto_arima", self._failing_auto_arima)
        sel = _make_selection(
            selected_model_family="auto_arima", selected_model_name="auto_arima_m7", selected_m=7,
        )
        candidate_summary = pd.DataFrame([
            _make_candidate_summary(model_family="auto_arima", model_name="auto_arima_m7", candidate_m=7, median_mase=0.65),
            _make_candidate_summary(model_family="seasonal_naive", model_name="seasonal_naive_m7", candidate_m=7, median_mase=0.85),
        ])
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT,
            candidate_summary=candidate_summary,
        )
        fc = result[result["horizon_step"].notna()]
        assert (fc["fallback_used"] == True).all()

    def test_fallback_used_false_for_successful_primary_fit(self):
        sel = _make_selection(selected_model_family="naive", selected_model_name="naive", selected_m=1)
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT
        )
        fc = result[result["horizon_step"].notna()]
        assert (fc["fallback_used"] == False).all()

    def test_fallback_reason_encodes_original_model(self, monkeypatch):
        monkeypatch.setattr(_candidates_module, "forecast_auto_arima", self._failing_auto_arima)
        sel = _make_selection(
            selected_model_family="auto_arima", selected_model_name="auto_arima_m7", selected_m=7,
        )
        candidate_summary = pd.DataFrame([
            _make_candidate_summary(model_family="auto_arima", model_name="auto_arima_m7", candidate_m=7, median_mase=0.65),
            _make_candidate_summary(model_family="seasonal_naive", model_name="seasonal_naive_m7", candidate_m=7, median_mase=0.85),
        ])
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT,
            candidate_summary=candidate_summary,
        )
        fc = result[result["horizon_step"].notna()]
        reason = fc["fallback_reason"].iloc[0]
        assert "auto_arima_m7" in reason, f"original model missing from reason: {reason}"
        assert "m=7" in reason, f"original m missing from reason: {reason}"

    def test_fallback_reason_encodes_fallback_model(self, monkeypatch):
        monkeypatch.setattr(_candidates_module, "forecast_auto_arima", self._failing_auto_arima)
        sel = _make_selection(
            selected_model_family="auto_arima", selected_model_name="auto_arima_m7", selected_m=7,
        )
        candidate_summary = pd.DataFrame([
            _make_candidate_summary(model_family="auto_arima", model_name="auto_arima_m7", candidate_m=7, median_mase=0.65),
            _make_candidate_summary(model_family="seasonal_naive", model_name="seasonal_naive_m7", candidate_m=7, median_mase=0.85),
        ])
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT,
            candidate_summary=candidate_summary,
        )
        fc = result[result["horizon_step"].notna()]
        reason = fc["fallback_reason"].iloc[0]
        assert "seasonal_naive" in reason.lower() or "naive" in reason.lower(), \
            f"fallback model missing from reason: {reason}"

    def test_selected_model_family_preserves_original_when_fallback_used(self, monkeypatch):
        """selected_model_family must always reflect the original selection, not the fallback."""
        monkeypatch.setattr(_candidates_module, "forecast_auto_arima", self._failing_auto_arima)
        sel = _make_selection(
            selected_model_family="auto_arima", selected_model_name="auto_arima_m7", selected_m=7,
        )
        candidate_summary = pd.DataFrame([
            _make_candidate_summary(model_family="auto_arima", model_name="auto_arima_m7", candidate_m=7, median_mase=0.65),
            _make_candidate_summary(model_family="seasonal_naive", model_name="seasonal_naive_m7", candidate_m=7, median_mase=0.85),
        ])
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT,
            candidate_summary=candidate_summary,
        )
        fc = result[result["horizon_step"].notna()]
        # Original selection preserved even when fallback was used for the actual forecast
        assert (fc["selected_model_family"] == "auto_arima").all()
        assert (fc["selected_m"] == 7).all()


# ===========================================================================
# 9. Previous forecast history not overwritten
# ===========================================================================

class TestForecastHistoryNotOverwritten:
    """save_production_outputs must append to the history file, never overwrite it."""

    def test_history_file_accumulates_across_runs(self, tmp_path):
        from src.pipelines.run_forecasting_pipeline import save_production_outputs

        series = _make_series()
        sel = _make_selection()
        prod_df = build_production_forecast(sel, {"R_001": series}, "RUN_001", _GENERATED_AT)

        # Minimal summary/selection DataFrames with required structure
        empty_summary = pd.DataFrame()
        empty_sel = pd.DataFrame()

        # First run
        save_production_outputs(
            production_df=prod_df,
            model_summary=empty_summary,
            selection=empty_sel,
            project_root=tmp_path,
            run_id="RUN_001",
            generated_at=_GENERATED_AT,
        )

        history_path = tmp_path / "outputs" / "forecasts" / "production_forecasts_history.csv"
        assert history_path.exists()
        rows_after_first = pd.read_csv(history_path)
        n_first = len(rows_after_first)
        assert n_first > 0

        # Second run — same data but different run_id
        prod_df2 = build_production_forecast(
            sel, {"R_001": series}, "RUN_002",
            _GENERATED_AT + pd.Timedelta(hours=1),
        )
        save_production_outputs(
            production_df=prod_df2,
            model_summary=empty_summary,
            selection=empty_sel,
            project_root=tmp_path,
            run_id="RUN_002",
            generated_at=_GENERATED_AT + pd.Timedelta(hours=1),
        )

        rows_after_second = pd.read_csv(history_path)
        n_second = len(rows_after_second)

        assert n_second == n_first * 2, (
            f"History file must have 2× rows after two runs, "
            f"got {n_second} (expected {n_first * 2})"
        )

    def test_latest_file_is_overwritten_but_history_is_not(self, tmp_path):
        from src.pipelines.run_forecasting_pipeline import save_production_outputs

        series = _make_series()
        empty = pd.DataFrame()

        for i, run_id in enumerate(("RUN_A", "RUN_B"), start=1):
            sel = _make_selection()
            prod_df = build_production_forecast(
                sel, {"R_001": series}, run_id,
                _GENERATED_AT + pd.Timedelta(hours=i),
            )
            save_production_outputs(
                production_df=prod_df,
                model_summary=empty, selection=empty,
                project_root=tmp_path, run_id=run_id,
                generated_at=_GENERATED_AT + pd.Timedelta(hours=i),
            )

        latest_path = tmp_path / "outputs" / "forecasts" / "production_forecasts_latest.csv"
        history_path = tmp_path / "outputs" / "forecasts" / "production_forecasts_history.csv"

        latest_df = pd.read_csv(latest_path)
        history_df = pd.read_csv(history_path)

        # Latest reflects the most recent run only
        assert "RUN_A" not in latest_df["run_id"].values, "Latest file must not contain older runs"
        # History contains both runs
        assert set(history_df["run_id"].unique()) == {"RUN_A", "RUN_B"}


# ===========================================================================
# 10. Failure handling
# ===========================================================================

class TestFailureHandling:
    """Failed and no-model reports produce placeholder rows, not silent drops."""

    def test_no_reliable_model_produces_placeholder_row(self):
        sel = _make_selection(
            selection_status="no_reliable_model",
            selection_reason="No reliable model: fewer than 3 valid folds.",
        )
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT
        )
        assert len(result) == 1
        row = result.iloc[0]
        assert pd.isna(row["forecast"])
        assert pd.isna(row["horizon_step"])
        assert row["selected_model_family"] is None

    def test_missing_series_produces_placeholder_row(self):
        sel = _make_selection()
        result = build_production_forecast(sel, {}, _RUN_ID, _GENERATED_AT)
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["forecast"])

    def test_unknown_model_family_produces_placeholder_row(self):
        sel = _make_selection(
            selected_model_family="nonexistent_family",
            selected_model_name="nonexistent_family_m7",
            selected_m=7,
        )
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT
        )
        assert len(result) == 1
        assert pd.isna(result.iloc[0]["forecast"])

    def test_empty_selection_returns_empty_dataframe(self):
        result = build_production_forecast(pd.DataFrame(), {}, _RUN_ID, _GENERATED_AT)
        assert result.empty
        assert list(result.columns) == PRODUCTION_FORECAST_COLS

    def test_failure_row_preserves_selection_reason(self):
        reason = "No reliable model: all candidates excluded by bias guardrail."
        sel = _make_selection(selection_status="no_reliable_model", selection_reason=reason)
        result = build_production_forecast(
            sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT
        )
        assert result.iloc[0]["selection_reason"] == reason

    def test_mixed_success_and_failure(self):
        sel = pd.concat([
            _make_selection(report_id="OK", selected_model_family="naive", selected_model_name="naive", selected_m=1),
            _make_selection(report_id="FAIL", selection_status="no_reliable_model"),
        ], ignore_index=True)
        series = {"OK": _make_series(n_days=200), "FAIL": _make_series(n_days=200)}
        result = build_production_forecast(sel, series, _RUN_ID, _GENERATED_AT)
        assert set(result["report_id"]) == {"OK", "FAIL"}
        ok_rows = result[result["report_id"] == "OK"]
        fail_rows = result[result["report_id"] == "FAIL"]
        assert len(ok_rows) == FORECAST_HORIZON_DAYS
        assert len(fail_rows) == 1
        assert pd.isna(fail_rows.iloc[0]["forecast"])


# ===========================================================================
# 11. refit_and_forecast contract
# ===========================================================================

class TestRefitAndForecastContract:

    def test_returns_model_result(self):
        from src.models.candidates import ModelResult
        result = refit_and_forecast("naive", _make_series())
        assert isinstance(result, ModelResult)

    def test_naive_fit_status_ok(self):
        assert refit_and_forecast("naive", _make_series()).fit_status == "ok"

    def test_seasonal_naive_fit_status_ok(self):
        assert refit_and_forecast("seasonal_naive", _make_series()).fit_status == "ok"

    def test_unknown_model_returns_failed_status(self):
        result = refit_and_forecast("does_not_exist", _make_series())
        assert result.fit_status == "failed"
        assert result.forecast is None

    def test_forecast_values_non_negative(self):
        for name in ["naive", "seasonal_naive"]:
            result = refit_and_forecast(name, _make_series())
            assert (result.forecast >= 0).all(), f"{name}: negative forecast values"

    def test_forecast_length_equals_horizon(self):
        for name in ["naive", "seasonal_naive"]:
            result = refit_and_forecast(name, _make_series(), horizon=14)
            assert len(result.forecast) == 14

    def test_model_order_none_for_baseline_models(self):
        sel = _make_selection(selected_model_family="naive", selected_model_name="naive", selected_m=1)
        result = build_production_forecast(sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT)
        assert result.iloc[0]["model_order"] is None
        assert result.iloc[0]["seasonal_order"] is None

    def test_m_encoded_model_name_resolves_period(self, monkeypatch):
        captured: list[int] = []

        def _fake_sn(series, horizon, seasonal_period=7, **kwargs):
            captured.append(seasonal_period)
            return _candidates_module.forecast_naive(series, horizon)

        monkeypatch.setattr(_candidates_module, "forecast_seasonal_naive", _fake_sn)
        refit_and_forecast("seasonal_naive_m14", _make_series(), horizon=28)
        assert captured == [14]


# ===========================================================================
# 12. Output schema
# ===========================================================================

class TestOutputSchema:

    def test_production_forecast_cols_present(self):
        sel = _make_selection()
        result = build_production_forecast(sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT)
        assert list(result.columns) == PRODUCTION_FORECAST_COLS

    def test_sorted_by_report_id_then_horizon_step(self):
        sel = pd.concat([
            _make_selection(report_id="Z_REPORT"),
            _make_selection(report_id="A_REPORT"),
        ], ignore_index=True)
        series = {"Z_REPORT": _make_series(n_days=200), "A_REPORT": _make_series(n_days=200)}
        result = build_production_forecast(sel, series, _RUN_ID, _GENERATED_AT)
        report_ids = result[result["horizon_step"].notna()]["report_id"].tolist()
        assert report_ids[:28] == ["A_REPORT"] * 28
        assert report_ids[28:] == ["Z_REPORT"] * 28
        steps = result[result["report_id"] == "A_REPORT"]["horizon_step"].dropna().astype(int).tolist()
        assert steps == list(range(1, 29))

    def test_production_fit_status_in_successful_rows(self):
        sel = _make_selection(selected_model_family="naive", selected_model_name="naive", selected_m=1)
        result = build_production_forecast(sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT)
        fc = result[result["horizon_step"].notna()]
        # Naive returns "ok"
        assert fc["production_fit_status"].iloc[0] == "ok"

    def test_fallback_reason_null_for_successful_primary(self):
        sel = _make_selection(selected_model_family="naive", selected_model_name="naive", selected_m=1)
        result = build_production_forecast(sel, {"R_001": _make_series()}, _RUN_ID, _GENERATED_AT)
        fc = result[result["horizon_step"].notna()]
        assert fc["fallback_reason"].isna().all()
