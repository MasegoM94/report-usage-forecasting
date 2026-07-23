"""Tests for src.models.candidates — the uniform model-result contract.

Optional-dependency handling
----------------------------
``forecast_ets`` requires statsmodels; ``forecast_auto_arima`` requires
pmdarima.  Tests that assert successful fits are skipped when the library is
absent — this mirrors the runtime behaviour (a missing library causes a
``failed`` ModelResult rather than an import error).  Tests for the graceful-
failure contract itself run regardless, because the ``_failed`` path is
exercised as long as any exception is raised.
"""
from __future__ import annotations

import importlib

import numpy as np
import pandas as pd
import pytest

from src.config.forecasting import FORECAST_HORIZON_DAYS, SEASONAL_CANDIDATES
from src.models.candidates import (
    ModelResult,
    forecast_auto_arima,
    forecast_ets,
    forecast_moving_average,
    forecast_naive,
    forecast_seasonal_naive,
)

# ---------------------------------------------------------------------------
# Optional-dependency availability flags (checked once at collection time)
# ---------------------------------------------------------------------------

_STATSMODELS_AVAILABLE = importlib.util.find_spec("statsmodels") is not None
_PMDARIMA_AVAILABLE = importlib.util.find_spec("pmdarima") is not None

_skip_no_statsmodels = pytest.mark.skipif(
    not _STATSMODELS_AVAILABLE, reason="statsmodels not installed"
)
_skip_no_pmdarima = pytest.mark.skipif(
    not _PMDARIMA_AVAILABLE, reason="pmdarima not installed"
)


def _require_fit_success(result: "ModelResult") -> None:
    """Skip the test if a model failed because its library is not installed;
    fail the test for any other failure reason."""
    if result.fit_status == "failed":
        msg = result.error_message or ""
        if "No module named" in msg:
            pytest.skip(f"Optional dependency not installed: {msg}")
        pytest.fail(f"Model '{result.model_name}' failed unexpectedly: {msg}")

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

HORIZON = FORECAST_HORIZON_DAYS  # 28


def _daily_series(n: int, start: str = "2022-01-01", seed: int = 0) -> pd.Series:
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, periods=n, freq="D")
    values = 10 + rng.integers(-3, 4, size=n).astype(float)
    values = np.clip(values, 0, None)
    return pd.Series(values, index=idx)


# Enough history for all models (covers 2+ seasonal cycles + horizon)
_TRAIN = _daily_series(365)

# Minimal series: exactly min needed for one ETS seasonal fit + horizon
# Uses the shortest candidate (weekly, m=7) as the reference period.
_SHORT = _daily_series(2 * SEASONAL_CANDIDATES[0] + HORIZON + 1)

# All model functions collected for parametric tests.
# Statistical models carry skip marks so tests are skipped (not failed) when
# the optional library is absent.
_ALL_MODELS = [
    pytest.param(forecast_naive, id="naive"),
    pytest.param(forecast_seasonal_naive, id="seasonal_naive"),
    pytest.param(forecast_moving_average, id="moving_average"),
    pytest.param(forecast_ets, id="ets", marks=_skip_no_statsmodels),
    pytest.param(forecast_auto_arima, id="auto_arima", marks=_skip_no_pmdarima),
]


# ---------------------------------------------------------------------------
# 1. Output length — every forecast has exactly horizon observations
# ---------------------------------------------------------------------------

class TestOutputLength:
    @pytest.mark.parametrize("fn", _ALL_MODELS)
    def test_forecast_length_equals_horizon(self, fn):
        result = fn(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        assert len(result.forecast) == HORIZON

    @pytest.mark.parametrize("fn", _ALL_MODELS)
    def test_lower_bound_length_when_present(self, fn):
        result = fn(_TRAIN, horizon=HORIZON)
        if result.lower_bound is not None:
            assert len(result.lower_bound) == HORIZON

    @pytest.mark.parametrize("fn", _ALL_MODELS)
    def test_upper_bound_length_when_present(self, fn):
        result = fn(_TRAIN, horizon=HORIZON)
        if result.upper_bound is not None:
            assert len(result.upper_bound) == HORIZON

    def test_non_default_horizon_respected(self):
        for fn in [forecast_naive, forecast_seasonal_naive, forecast_moving_average]:
            result = fn(_TRAIN, horizon=14)
            assert len(result.forecast) == 14


# ---------------------------------------------------------------------------
# 2. Forecast index — same DatetimeIndex starting the day after training ends
# ---------------------------------------------------------------------------

class TestForecastIndex:
    def _expected_index(self, training: pd.Series, horizon: int) -> pd.DatetimeIndex:
        return pd.date_range(
            start=training.index[-1] + pd.Timedelta(days=1),
            periods=horizon,
            freq="D",
        )

    @pytest.mark.parametrize("fn", _ALL_MODELS)
    def test_forecast_index_is_datetimeindex(self, fn):
        result = fn(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        assert isinstance(result.forecast.index, pd.DatetimeIndex)

    @pytest.mark.parametrize("fn", _ALL_MODELS)
    def test_forecast_index_starts_day_after_training(self, fn):
        result = fn(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        expected_start = _TRAIN.index[-1] + pd.Timedelta(days=1)
        assert result.forecast.index[0] == expected_start

    @pytest.mark.parametrize("fn", _ALL_MODELS)
    def test_forecast_index_is_daily(self, fn):
        result = fn(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        diffs = result.forecast.index[1:] - result.forecast.index[:-1]
        assert (diffs == pd.Timedelta(days=1)).all()

    @pytest.mark.parametrize("fn", _ALL_MODELS)
    def test_bounds_share_forecast_index_when_present(self, fn):
        result = fn(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        if result.lower_bound is not None:
            pd.testing.assert_index_equal(result.lower_bound.index, result.forecast.index)
        if result.upper_bound is not None:
            pd.testing.assert_index_equal(result.upper_bound.index, result.forecast.index)


# ---------------------------------------------------------------------------
# 3. Metadata — required keys present per model
# ---------------------------------------------------------------------------

class TestMetadata:
    def test_naive_metadata_has_lag(self):
        result = forecast_naive(_TRAIN, horizon=HORIZON)
        assert "lag" in result.model_metadata
        assert result.model_metadata["lag"] == 1

    def test_naive_metadata_has_last_observed(self):
        result = forecast_naive(_TRAIN, horizon=HORIZON)
        assert "last_observed" in result.model_metadata
        assert result.model_metadata["last_observed"] == float(_TRAIN.iloc[-1])

    def test_seasonal_naive_metadata_has_seasonal_period(self):
        # Default seasonal_period is SEASONAL_CANDIDATES[0] (7 — weekly).
        result = forecast_seasonal_naive(_TRAIN, horizon=HORIZON)
        assert result.model_metadata.get("seasonal_period") == SEASONAL_CANDIDATES[0]

    def test_seasonal_naive_metadata_has_fallback_flag(self):
        result = forecast_seasonal_naive(_TRAIN, horizon=HORIZON)
        assert "fallback_to_naive" in result.model_metadata

    def test_moving_average_metadata_has_window(self):
        result = forecast_moving_average(_TRAIN, horizon=HORIZON, window=14)
        assert result.model_metadata.get("window") == 14

    def test_moving_average_metadata_has_effective_window(self):
        result = forecast_moving_average(_TRAIN, horizon=HORIZON, window=14)
        assert "effective_window" in result.model_metadata

    def test_moving_average_metadata_has_avg_value(self):
        result = forecast_moving_average(_TRAIN, horizon=HORIZON, window=7)
        assert "moving_average_value" in result.model_metadata

    @_skip_no_statsmodels
    def test_ets_metadata_has_aic_and_bic(self):
        result = forecast_ets(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        assert "aic" in result.model_metadata
        assert "bic" in result.model_metadata
        assert np.isfinite(result.model_metadata["aic"])

    @_skip_no_statsmodels
    def test_ets_metadata_has_converged_flag(self):
        result = forecast_ets(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        assert "converged" in result.model_metadata

    @_skip_no_statsmodels
    def test_ets_metadata_has_seasonal_info(self):
        result = forecast_ets(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        assert "seasonal" in result.model_metadata
        assert "seasonal_periods" in result.model_metadata

    @_skip_no_pmdarima
    def test_auto_arima_metadata_has_order(self):
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        assert "order" in result.model_metadata
        p, d, q = result.model_metadata["order"]
        assert all(isinstance(x, int) for x in (p, d, q))

    @_skip_no_pmdarima
    def test_auto_arima_metadata_has_seasonal_order(self):
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        assert "seasonal_order" in result.model_metadata
        assert len(result.model_metadata["seasonal_order"]) == 4

    @_skip_no_pmdarima
    def test_auto_arima_metadata_has_seasonal_period(self):
        # Default seasonal_period is SEASONAL_CANDIDATES[0] (7 — weekly).
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        assert result.model_metadata.get("seasonal_period") == SEASONAL_CANDIDATES[0]

    @_skip_no_pmdarima
    def test_auto_arima_metadata_has_aic_and_aicc(self):
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        assert "aic" in result.model_metadata
        assert "aicc" in result.model_metadata

    @_skip_no_pmdarima
    def test_auto_arima_metadata_has_model_str(self):
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        assert "model_str" in result.model_metadata
        assert "ARIMA" in result.model_metadata["model_str"]


# ---------------------------------------------------------------------------
# 4. Non-negative published values
# ---------------------------------------------------------------------------

class TestNonNegativePublishedValues:
    def _series_with_negative_raw(self) -> pd.Series:
        """Series whose naive/seasonal-naive raw forecast will be negative."""
        idx = pd.date_range("2022-01-01", periods=50, freq="D")
        vals = np.full(50, -5.0)  # all negative
        return pd.Series(vals, index=idx)

    @pytest.mark.parametrize("fn", _ALL_MODELS)
    def test_published_forecast_is_non_negative(self, fn):
        result = fn(_TRAIN, horizon=HORIZON)
        if result.fit_status == "failed":
            pytest.skip(f"{result.model_name} failed: {result.error_message}")
        assert (result.forecast >= 0).all(), (
            f"{result.model_name}: forecast has negative published values"
        )

    def test_naive_clips_negative_raw_to_zero(self):
        s = self._series_with_negative_raw()
        result = forecast_naive(s, horizon=HORIZON)
        assert result.fit_status != "failed"
        assert (result.forecast >= 0).all()
        # raw should be negative
        assert (result.forecast_raw < 0).all()

    def test_seasonal_naive_clips_negative_raw_to_zero(self):
        s = self._series_with_negative_raw()
        result = forecast_seasonal_naive(s, horizon=HORIZON)
        assert result.fit_status != "failed"
        assert (result.forecast >= 0).all()

    @pytest.mark.parametrize(
        "fn",
        [
            pytest.param(forecast_ets, marks=_skip_no_statsmodels),
            pytest.param(forecast_auto_arima, marks=_skip_no_pmdarima),
        ],
    )
    def test_statistical_model_lower_bound_non_negative(self, fn):
        result = fn(_TRAIN, horizon=HORIZON)
        if result.fit_status == "failed":
            pytest.skip(f"{result.model_name} failed: {result.error_message}")
        if result.lower_bound is not None:
            assert (result.lower_bound >= 0).all(), (
                f"{result.model_name}: lower_bound has negative published values"
            )

    def test_raw_and_published_differ_when_raw_has_negatives(self):
        """Published forecast clips; raw preserves original model output."""
        s = self._series_with_negative_raw()
        result = forecast_naive(s, horizon=HORIZON)
        assert result.forecast_raw is not None
        # At least some raw values should differ from the published (clipped) values
        assert not (result.forecast == result.forecast_raw).all()


# ---------------------------------------------------------------------------
# 5. Graceful failure — no exception propagates to caller
# ---------------------------------------------------------------------------

class TestGracefulFailure:
    def _bad_series(self) -> pd.Series:
        """A pd.Series with an integer (non-datetime) index — invalid input."""
        return pd.Series([1.0, 2.0, 3.0], index=[0, 1, 2])

    @pytest.mark.parametrize("fn", _ALL_MODELS)
    def test_invalid_index_returns_failed_result_not_exception(self, fn):
        """An invalid training series must return fit_status='failed', never raise."""
        result = fn(self._bad_series(), horizon=HORIZON)
        assert result.fit_status == "failed"
        assert result.error_message is not None
        assert result.forecast is None

    @pytest.mark.parametrize("fn", _ALL_MODELS)
    def test_failed_result_has_correct_model_name(self, fn):
        result = fn(self._bad_series(), horizon=HORIZON)
        assert result.fit_status == "failed"
        # model_name must still identify the model
        assert isinstance(result.model_name, str) and result.model_name

    def test_failed_result_bounds_are_none(self):
        bad = self._bad_series()
        fns = [forecast_naive, forecast_seasonal_naive, forecast_moving_average,
               forecast_ets, forecast_auto_arima]
        for fn in fns:
            result = fn(bad, horizon=HORIZON)
            if result.fit_status == "failed":
                assert result.lower_bound is None
                assert result.upper_bound is None

    def test_non_series_input_returns_failed_result(self):
        fns = [forecast_naive, forecast_seasonal_naive, forecast_moving_average,
               forecast_ets, forecast_auto_arima]
        for fn in fns:
            result = fn([1, 2, 3, 4, 5], horizon=HORIZON)  # list, not Series
            assert result.fit_status == "failed"

    def test_multiple_model_failures_are_independent(self):
        """Failing one model must not affect the others — simulate by checking
        that a valid model still produces a result after an invalid one."""
        bad = self._bad_series()
        _ = forecast_naive(bad, horizon=HORIZON)          # fails
        good = forecast_naive(_TRAIN, horizon=HORIZON)    # must still succeed
        assert good.fit_status == "ok"
        assert good.forecast is not None

    def test_moving_average_invalid_window_returns_failed(self):
        result = forecast_moving_average(_TRAIN, horizon=HORIZON, window=0)
        assert result.fit_status == "failed"
        assert result.error_message is not None


# ---------------------------------------------------------------------------
# 6. Model-name contract
# ---------------------------------------------------------------------------

class TestModelNames:
    def test_baseline_model_names_are_stable(self):
        # naive and moving_average are not period-specific; their names are fixed.
        # seasonal_naive encodes m in its name.
        assert forecast_naive(_TRAIN, horizon=HORIZON).model_name == "naive"
        assert forecast_moving_average(_TRAIN, horizon=HORIZON).model_name == "moving_average"
        default_m = SEASONAL_CANDIDATES[0]  # 7
        assert forecast_seasonal_naive(_TRAIN, horizon=HORIZON).model_name == f"seasonal_naive_m{default_m}"

    @_skip_no_statsmodels
    def test_ets_model_name_includes_m(self):
        default_m = SEASONAL_CANDIDATES[0]
        result = forecast_ets(_TRAIN, horizon=HORIZON)
        assert result.model_name == f"ets_m{default_m}"

    @_skip_no_pmdarima
    def test_auto_arima_model_name_includes_m(self):
        default_m = SEASONAL_CANDIDATES[0]
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON)
        assert result.model_name == f"auto_arima_m{default_m}"

    def test_failed_result_still_carries_model_name(self):
        bad = pd.Series([1.0], index=[0])  # integer index → fails validation
        default_m = SEASONAL_CANDIDATES[0]
        for fn, name in [
            (forecast_naive, "naive"),
            (forecast_seasonal_naive, f"seasonal_naive_m{default_m}"),
            (forecast_moving_average, "moving_average"),
            (forecast_ets, f"ets_m{default_m}"),
            (forecast_auto_arima, f"auto_arima_m{default_m}"),
        ]:
            result = fn(bad, horizon=HORIZON)
            assert result.model_name == name


# ---------------------------------------------------------------------------
# 7. Baseline-specific behaviour
# ---------------------------------------------------------------------------

class TestBaselineBehaviours:
    def test_naive_forecast_is_constant(self):
        result = forecast_naive(_TRAIN, horizon=HORIZON)
        assert result.forecast.nunique() == 1

    def test_naive_forecast_equals_last_training_value(self):
        result = forecast_naive(_TRAIN, horizon=HORIZON)
        expected = max(0.0, float(_TRAIN.iloc[-1]))
        assert (result.forecast == expected).all()

    def test_seasonal_naive_repeats_last_season(self):
        idx = pd.date_range("2022-01-01", periods=14, freq="D")
        s = pd.Series(np.arange(14, dtype=float), index=idx)
        result = forecast_seasonal_naive(s, horizon=7, seasonal_period=7)
        # Last 7 values: [7,8,9,10,11,12,13]
        expected = [7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0]
        assert list(result.forecast.values) == expected

    def test_seasonal_naive_falls_back_when_series_too_short(self):
        short = _daily_series(3)
        result = forecast_seasonal_naive(short, horizon=HORIZON, seasonal_period=7)
        assert result.model_metadata["fallback_to_naive"] is True
        assert result.forecast.nunique() == 1  # constant after fallback

    def test_moving_average_forecast_is_constant(self):
        result = forecast_moving_average(_TRAIN, horizon=HORIZON, window=7)
        assert result.forecast.nunique() == 1

    def test_moving_average_value_matches_manual_calculation(self):
        result = forecast_moving_average(_TRAIN, horizon=7, window=7)
        expected_avg = float(_TRAIN.iloc[-7:].mean())
        assert result.forecast.iloc[0] == pytest.approx(max(0.0, expected_avg))

    def test_moving_average_effective_window_capped_at_series_length(self):
        tiny = _daily_series(3)
        result = forecast_moving_average(tiny, horizon=HORIZON, window=14)
        assert result.model_metadata["effective_window"] == 3

    def test_baselines_have_no_bounds(self):
        for fn in [forecast_naive, forecast_seasonal_naive, forecast_moving_average]:
            result = fn(_TRAIN, horizon=HORIZON)
            assert result.lower_bound is None
            assert result.upper_bound is None


# ---------------------------------------------------------------------------
# 8. Statistical model interval contract
# ---------------------------------------------------------------------------

class TestStatisticalModelIntervals:
    @_skip_no_pmdarima
    def test_auto_arima_returns_lower_and_upper_bounds(self):
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        assert result.lower_bound is not None
        assert result.upper_bound is not None

    @_skip_no_pmdarima
    def test_auto_arima_upper_bound_gte_lower_bound(self):
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        assert (result.upper_bound >= result.lower_bound).all()

    @_skip_no_pmdarima
    def test_auto_arima_forecast_within_or_touching_bounds(self):
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        # Point forecast should lie within published (clipped) bounds
        lo = result.lower_bound
        hi = result.upper_bound
        assert (result.forecast >= lo - 1e-8).all()
        assert (result.forecast <= hi + 1e-8).all()

    @_skip_no_pmdarima
    def test_raw_bounds_preserved_separately_from_published(self):
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON)
        _require_fit_success(result)
        assert result.lower_bound_raw is not None
        assert result.upper_bound_raw is not None
        # Raw and published series exist as separate objects
        assert result.lower_bound is not result.lower_bound_raw


# ---------------------------------------------------------------------------
# 9. Multi-period candidates — requirements 12-14
# ---------------------------------------------------------------------------

class TestMultiPeriodCandidates:
    """Verify correct behaviour for each candidate period and horizon variant.

    Tests cover:
    * m = 1 (non-seasonal ARIMA / ETS)
    * m = 7  (weekly — default)
    * m = 28 (four-week)
    * m = 30 (approximate monthly)
    * m = 90 (approximate quarterly, sufficient & insufficient history)
    * horizon < m  (partial-cycle tile)
    * horizon > m  (multi-cycle tile)
    * insufficient history → failed ModelResult, not an exception
    * independent failure — sibling models unaffected
    """

    # ------------------------------------------------------------------
    # Seasonal naïve: model_name and output length for each m
    # ------------------------------------------------------------------

    @pytest.mark.parametrize("m", [1, 7, 28, 30, 90])
    def test_seasonal_naive_model_name_includes_m(self, m):
        result = forecast_seasonal_naive(_TRAIN, horizon=HORIZON, seasonal_period=m)
        assert result.model_name == f"seasonal_naive_m{m}"

    @pytest.mark.parametrize("m", [1, 7, 28, 30, 90])
    def test_seasonal_naive_output_length_is_horizon(self, m):
        result = forecast_seasonal_naive(_TRAIN, horizon=HORIZON, seasonal_period=m)
        assert len(result.forecast) == HORIZON

    def test_seasonal_naive_horizon_shorter_than_m(self):
        # horizon=14 < m=28: tile is cut after 14 values
        result = forecast_seasonal_naive(_TRAIN, horizon=14, seasonal_period=28)
        assert result.fit_status == "ok"
        assert len(result.forecast) == 14

    def test_seasonal_naive_horizon_longer_than_m(self):
        # horizon=56 > m=28: two full cycles
        result = forecast_seasonal_naive(_TRAIN, horizon=56, seasonal_period=28)
        assert result.fit_status == "ok"
        assert len(result.forecast) == 56

    def test_seasonal_naive_m90_horizon_shorter_than_period(self):
        # horizon=28 < m=90: tile produces first 28 of the 90-day pattern
        result = forecast_seasonal_naive(_TRAIN, horizon=HORIZON, seasonal_period=90)
        assert len(result.forecast) == HORIZON

    def test_seasonal_naive_metadata_preserves_m(self):
        for m in [7, 28, 30, 90]:
            result = forecast_seasonal_naive(_TRAIN, horizon=HORIZON, seasonal_period=m)
            assert result.model_metadata["seasonal_period"] == m

    # ------------------------------------------------------------------
    # ETS: model_name, non-seasonal path (m=1), fallback on short series
    # ------------------------------------------------------------------

    @_skip_no_statsmodels
    @pytest.mark.parametrize("m", [1, 7, 28])
    def test_ets_model_name_includes_m(self, m):
        result = forecast_ets(_TRAIN, horizon=HORIZON, seasonal_period=m)
        _require_fit_success(result)
        assert result.model_name == f"ets_m{m}"

    @_skip_no_statsmodels
    def test_ets_m1_is_non_seasonal(self):
        result = forecast_ets(_TRAIN, horizon=HORIZON, seasonal_period=1)
        _require_fit_success(result)
        assert result.model_metadata["seasonal"] is None
        assert result.model_metadata["fallback_no_seasonal"] is True

    @_skip_no_statsmodels
    def test_ets_requested_seasonal_period_preserved_on_fallback(self):
        # Short series: 100 obs < 3*90 = 270 → fallback, but requested period recorded
        short = _daily_series(100)
        result = forecast_ets(short, horizon=HORIZON, seasonal_period=90)
        _require_fit_success(result)
        assert result.model_metadata["fallback_no_seasonal"] is True
        assert result.model_metadata["requested_seasonal_period"] == 90

    @_skip_no_statsmodels
    def test_ets_m7_output_length_is_horizon(self):
        result = forecast_ets(_TRAIN, horizon=HORIZON, seasonal_period=7)
        _require_fit_success(result)
        assert len(result.forecast) == HORIZON

    @_skip_no_statsmodels
    def test_ets_m28_with_sufficient_history(self):
        result = forecast_ets(_TRAIN, horizon=HORIZON, seasonal_period=28)
        _require_fit_success(result)
        assert len(result.forecast) == HORIZON
        assert result.model_name == "ets_m28"

    # ------------------------------------------------------------------
    # Auto-ARIMA: m=1 non-seasonal, m=7, m=28, m=30, m=90
    # ------------------------------------------------------------------

    @_skip_no_pmdarima
    def test_auto_arima_m1_is_non_seasonal(self):
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON, seasonal_period=1)
        _require_fit_success(result)
        assert result.model_name == "auto_arima_m1"
        assert len(result.forecast) == HORIZON
        # pmdarima reports seasonal_order as (0,0,0,0) when seasonal=False
        sp, sd, sq, m = result.model_metadata["seasonal_order"]
        assert m == 0, f"Expected non-seasonal m=0 in seasonal_order, got {m}"

    @_skip_no_pmdarima
    @pytest.mark.parametrize("m", [7, 28])
    def test_auto_arima_seasonal_model_name(self, m):
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON, seasonal_period=m)
        _require_fit_success(result)
        assert result.model_name == f"auto_arima_m{m}"
        assert len(result.forecast) == HORIZON

    @_skip_no_pmdarima
    def test_auto_arima_m30_sufficient_history(self):
        # 365 obs >= 3*30=90 → should fit
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON, seasonal_period=30)
        _require_fit_success(result)
        assert result.model_name == "auto_arima_m30"
        assert result.model_metadata["seasonal_period"] == 30

    @_skip_no_pmdarima
    def test_auto_arima_m90_sufficient_history(self):
        # 365 obs >= 3*90=270 → should fit
        result = forecast_auto_arima(_TRAIN, horizon=HORIZON, seasonal_period=90)
        _require_fit_success(result)
        assert result.model_name == "auto_arima_m90"
        assert len(result.forecast) == HORIZON

    @_skip_no_pmdarima
    def test_auto_arima_m90_insufficient_history_returns_failed(self):
        # 100 obs < 3*90=270 → must return failed, not raise
        short = _daily_series(100)
        result = forecast_auto_arima(short, horizon=HORIZON, seasonal_period=90)
        assert result.fit_status == "failed"
        assert result.forecast is None
        assert result.model_name == "auto_arima_m90"
        assert "insufficient" in (result.error_message or "").lower()

    @_skip_no_pmdarima
    def test_auto_arima_m28_insufficient_history_returns_failed(self):
        # 50 obs < 3*28=84 → failed
        short = _daily_series(50)
        result = forecast_auto_arima(short, horizon=HORIZON, seasonal_period=28)
        assert result.fit_status == "failed"
        assert result.forecast is None

    # ------------------------------------------------------------------
    # Graceful failure isolation: one period failing does not affect others
    # ------------------------------------------------------------------

    def test_m90_failure_does_not_terminate_sibling_naive(self):
        short = _daily_series(50)
        failed = forecast_auto_arima(short, horizon=HORIZON, seasonal_period=90) if _PMDARIMA_AVAILABLE \
            else forecast_seasonal_naive(short, horizon=HORIZON, seasonal_period=90)
        # Regardless of that result, naive on same series still succeeds
        good = forecast_naive(short, horizon=HORIZON)
        assert good.fit_status == "ok"
        assert good.forecast is not None

    def test_m90_failure_does_not_terminate_m7_arima(self):
        if not _PMDARIMA_AVAILABLE:
            pytest.skip("pmdarima not installed")
        short = _daily_series(50)
        _ = forecast_auto_arima(short, horizon=HORIZON, seasonal_period=90)  # fails
        # m=7 on a long series must still succeed
        good = forecast_auto_arima(_TRAIN, horizon=HORIZON, seasonal_period=7)
        _require_fit_success(good)
        assert good.forecast is not None

    # ------------------------------------------------------------------
    # Output length is always exactly horizon regardless of m
    # ------------------------------------------------------------------

    @pytest.mark.parametrize("m,h", [
        (7, 3),    # horizon much shorter than m
        (7, 28),   # horizon = 4m
        (7, 50),   # horizon > 4m
        (28, 14),  # horizon < m
        (28, 28),  # horizon == m
        (28, 56),  # horizon = 2m
        (90, 28),  # horizon << m
    ])
    def test_seasonal_naive_output_always_equals_horizon(self, m, h):
        result = forecast_seasonal_naive(_TRAIN, horizon=h, seasonal_period=m)
        assert len(result.forecast) == h
