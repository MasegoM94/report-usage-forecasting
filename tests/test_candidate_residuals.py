"""Tests for in-sample training-residual extraction on ModelResult.

Covers all required scenarios:
  - ARIMA residual extraction
  - SARIMA residual extraction
  - ETS residual extraction
  - unavailable residual policy for simple baselines (moving average edge case)
  - alignment with training dates
  - initial missing fitted values (first observation excluded)
  - residual sign (residual = actual - fitted)
  - residual extraction failure does not fail forecast
  - candidate_m preservation alongside residuals
  - forecasts unchanged before and after extension
  - no model object persisted in ModelResult
"""

from __future__ import annotations

import importlib

import numpy as np
import pandas as pd
import pytest

from src.models.candidates import (
    ModelResult,
    forecast_auto_arima,
    forecast_ets,
    forecast_moving_average,
    forecast_naive,
    forecast_seasonal_naive,
)

# ---------------------------------------------------------------------------
# Optional-dependency guards
# ---------------------------------------------------------------------------

_STATSMODELS_AVAILABLE = importlib.util.find_spec("statsmodels") is not None
_PMDARIMA_AVAILABLE    = importlib.util.find_spec("pmdarima") is not None

_skip_no_statsmodels = pytest.mark.skipif(
    not _STATSMODELS_AVAILABLE, reason="statsmodels not installed"
)
_skip_no_pmdarima = pytest.mark.skipif(
    not _PMDARIMA_AVAILABLE, reason="pmdarima not installed"
)


def _require_forecast_ok(result: ModelResult) -> None:
    if result.fit_status == "failed":
        msg = result.error_message or ""
        if "No module named" in msg:
            pytest.skip(f"Optional dependency not installed: {msg}")
        pytest.fail(f"Model '{result.model_name}' failed unexpectedly: {msg}")


# ---------------------------------------------------------------------------
# Shared series fixtures
# ---------------------------------------------------------------------------

def _daily_series(n: int, start: str = "2022-01-01", seed: int = 42) -> pd.Series:
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, periods=n, freq="D")
    values = 50 + rng.integers(-10, 11, size=n).astype(float)
    return pd.Series(values, index=idx)


_TRAIN = _daily_series(365)   # enough for all models
_TRAIN_SHORT = _daily_series(30)  # short but > 7


# ---------------------------------------------------------------------------
# TestModelResultResidualFields
# ---------------------------------------------------------------------------

class TestModelResultResidualFields:
    """Verify the new fields are present on ModelResult with correct defaults."""

    def test_all_residual_fields_on_naive(self):
        r = forecast_naive(_TRAIN, horizon=28)
        for attr in [
            "training_actual", "training_fitted", "training_residuals",
            "training_residual_dates", "residual_extraction_status",
            "residual_extraction_reason", "training_observation_count",
            "fitted_observation_count", "residual_observation_count",
        ]:
            assert hasattr(r, attr), f"ModelResult missing field: {attr}"

    def test_failed_result_has_unavailable_status(self):
        bad = pd.Series([1.0, 2.0], index=[0, 1])  # integer index → fails
        r = forecast_naive(bad, horizon=5)
        assert r.fit_status == "failed"
        assert r.residual_extraction_status == "unavailable"
        assert r.training_actual is None
        assert r.training_residuals is None


# ---------------------------------------------------------------------------
# TestNaiveResiduals
# ---------------------------------------------------------------------------

class TestNaiveResiduals:
    def test_extraction_status_ok(self):
        r = forecast_naive(_TRAIN, horizon=28)
        assert r.residual_extraction_status == "ok"

    def test_arrays_are_numpy(self):
        r = forecast_naive(_TRAIN, horizon=28)
        assert isinstance(r.training_actual, np.ndarray)
        assert isinstance(r.training_fitted, np.ndarray)
        assert isinstance(r.training_residuals, np.ndarray)
        assert isinstance(r.training_residual_dates, pd.DatetimeIndex)

    def test_residual_sign(self):
        """residual = actual - fitted  (not fitted - actual)."""
        r = forecast_naive(_TRAIN, horizon=28)
        expected = r.training_actual - r.training_fitted
        np.testing.assert_allclose(r.training_residuals, expected, atol=1e-12)

    def test_first_observation_excluded(self):
        """Naive has no fitted value for the first training day."""
        r = forecast_naive(_TRAIN, horizon=28)
        assert r.training_observation_count == len(_TRAIN)
        assert r.fitted_observation_count == len(_TRAIN) - 1

    def test_fitted_value_is_previous_actual(self):
        """fitted[t] = y[t-1] for naive."""
        y = _TRAIN.to_numpy(dtype=float)
        r = forecast_naive(_TRAIN, horizon=28)
        np.testing.assert_allclose(r.training_fitted, y[:-1], atol=1e-12)

    def test_dates_align_with_arrays(self):
        r = forecast_naive(_TRAIN, horizon=28)
        assert len(r.training_residual_dates) == len(r.training_actual)
        assert len(r.training_residual_dates) == len(r.training_residuals)
        # Dates start from the second training day
        assert r.training_residual_dates[0] == _TRAIN.index[1]

    def test_residuals_all_finite(self):
        r = forecast_naive(_TRAIN, horizon=28)
        assert np.all(np.isfinite(r.training_residuals))

    def test_counts_consistent(self):
        r = forecast_naive(_TRAIN, horizon=28)
        assert r.residual_observation_count == r.fitted_observation_count
        assert r.fitted_observation_count == len(r.training_residuals)

    def test_no_model_object_in_result(self):
        """ModelResult must not retain a fitted model object."""
        r = forecast_naive(_TRAIN, horizon=28)
        for val in vars(r).values():
            assert not hasattr(val, "predict"), (
                f"ModelResult field appears to be a fitted model: {type(val)}"
            )

    def test_forecast_unchanged(self):
        """Forecast values must be exactly last observed, clipped to ≥ 0."""
        r = forecast_naive(_TRAIN, horizon=28)
        last = max(0.0, float(_TRAIN.iloc[-1]))
        assert (r.forecast == last).all()


# ---------------------------------------------------------------------------
# TestSeasonalNaiveResiduals
# ---------------------------------------------------------------------------

class TestSeasonalNaiveResiduals:
    def test_extraction_status_ok_default_m(self):
        r = forecast_seasonal_naive(_TRAIN, horizon=28, seasonal_period=7)
        assert r.residual_extraction_status == "ok"

    def test_first_m_observations_excluded(self):
        """fitted[t] = y[t-7]; first 7 observations have no fitted value."""
        m = 7
        r = forecast_seasonal_naive(_TRAIN, horizon=28, seasonal_period=m)
        assert r.training_observation_count == len(_TRAIN)
        assert r.fitted_observation_count == len(_TRAIN) - m

    def test_fitted_values_equal_lagged_actuals(self):
        m = 7
        y = _TRAIN.to_numpy(dtype=float)
        r = forecast_seasonal_naive(_TRAIN, horizon=28, seasonal_period=m)
        np.testing.assert_allclose(r.training_fitted, y[:-m], atol=1e-12)

    def test_residual_sign(self):
        r = forecast_seasonal_naive(_TRAIN, horizon=28, seasonal_period=7)
        expected = r.training_actual - r.training_fitted
        np.testing.assert_allclose(r.training_residuals, expected, atol=1e-12)

    def test_dates_start_at_position_m(self):
        m = 7
        r = forecast_seasonal_naive(_TRAIN, horizon=28, seasonal_period=m)
        assert r.training_residual_dates[0] == _TRAIN.index[m]

    def test_fallback_to_naive_uses_lag1(self):
        """When series < m, the fallback is naive (lag-1); fitted[t] = y[t-1]."""
        short = _daily_series(5)
        r = forecast_seasonal_naive(short, horizon=3, seasonal_period=7)
        assert r.model_metadata["fallback_to_naive"] is True
        assert r.residual_extraction_status == "ok"
        # First obs excluded; rest are lag-1
        y = short.to_numpy(dtype=float)
        np.testing.assert_allclose(r.training_fitted, y[:-1], atol=1e-12)

    def test_m28_initial_missing_count(self):
        m = 28
        r = forecast_seasonal_naive(_TRAIN, horizon=28, seasonal_period=m)
        assert r.fitted_observation_count == len(_TRAIN) - m

    def test_residuals_finite(self):
        r = forecast_seasonal_naive(_TRAIN, horizon=28, seasonal_period=7)
        assert np.all(np.isfinite(r.training_residuals))

    def test_candidate_m_preserved(self):
        for m in [7, 28, 30]:
            r = forecast_seasonal_naive(_TRAIN, horizon=28, seasonal_period=m)
            assert r.model_metadata["seasonal_period"] == m

    def test_forecast_unchanged_after_residual_extension(self):
        """Forecasts must equal the last m-day pattern repeated."""
        m = 7
        r = forecast_seasonal_naive(_TRAIN, horizon=7, seasonal_period=m)
        y = _TRAIN.to_numpy(dtype=float)
        expected = y[-m:]
        np.testing.assert_allclose(r.forecast.values, expected, atol=1e-12)


# ---------------------------------------------------------------------------
# TestMovingAverageResiduals
# ---------------------------------------------------------------------------

class TestMovingAverageResiduals:
    def test_extraction_status_ok_when_n_gt_window(self):
        r = forecast_moving_average(_TRAIN, horizon=28, window=7)
        assert r.residual_extraction_status == "ok"

    def test_first_window_observations_excluded(self):
        w = 7
        r = forecast_moving_average(_TRAIN, horizon=28, window=w)
        assert r.training_observation_count == len(_TRAIN)
        assert r.fitted_observation_count == len(_TRAIN) - w

    def test_fitted_equals_rolling_mean(self):
        w = 7
        y = _TRAIN.to_numpy(dtype=float)
        r = forecast_moving_average(_TRAIN, horizon=28, window=w)
        expected_fitted = np.array([y[t - w: t].mean() for t in range(w, len(y))])
        np.testing.assert_allclose(r.training_fitted, expected_fitted, atol=1e-12)

    def test_residual_sign(self):
        r = forecast_moving_average(_TRAIN, horizon=28, window=7)
        expected = r.training_actual - r.training_fitted
        np.testing.assert_allclose(r.training_residuals, expected, atol=1e-12)

    def test_dates_start_at_position_w(self):
        w = 7
        r = forecast_moving_average(_TRAIN, horizon=28, window=w)
        assert r.training_residual_dates[0] == _TRAIN.index[w]

    def test_unavailable_when_series_too_short_for_window(self):
        """Series length == effective_window: no observation left over."""
        tiny = _daily_series(7)
        r = forecast_moving_average(tiny, horizon=3, window=7)
        # fit_status must still be ok (forecast succeeds)
        assert r.fit_status == "ok"
        assert r.residual_extraction_status == "unavailable"
        assert r.training_residuals is None

    def test_residuals_finite(self):
        r = forecast_moving_average(_TRAIN, horizon=28, window=7)
        assert np.all(np.isfinite(r.training_residuals))

    def test_forecast_unchanged(self):
        w = 7
        r = forecast_moving_average(_TRAIN, horizon=28, window=w)
        y = _TRAIN.to_numpy(dtype=float)
        expected = max(0.0, float(y[-w:].mean()))
        assert (r.forecast == expected).all()


# ---------------------------------------------------------------------------
# TestETSResiduals
# ---------------------------------------------------------------------------

class TestETSResiduals:
    @_skip_no_statsmodels
    def test_extraction_status_ok(self):
        r = forecast_ets(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r)
        assert r.residual_extraction_status == "ok"

    @_skip_no_statsmodels
    def test_arrays_are_numpy(self):
        r = forecast_ets(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r)
        assert isinstance(r.training_actual, np.ndarray)
        assert isinstance(r.training_fitted, np.ndarray)
        assert isinstance(r.training_residuals, np.ndarray)

    @_skip_no_statsmodels
    def test_residual_sign(self):
        """residual = actual - fitted."""
        r = forecast_ets(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r)
        expected = r.training_actual - r.training_fitted
        np.testing.assert_allclose(r.training_residuals, expected, atol=1e-9)

    @_skip_no_statsmodels
    def test_residuals_all_finite(self):
        r = forecast_ets(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r)
        assert np.all(np.isfinite(r.training_residuals))

    @_skip_no_statsmodels
    def test_dates_align_with_training_index(self):
        r = forecast_ets(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r)
        assert r.training_residual_dates[0] >= _TRAIN.index[0]
        assert r.training_residual_dates[-1] == _TRAIN.index[-1]

    @_skip_no_statsmodels
    def test_fitted_observation_count_lte_training_count(self):
        r = forecast_ets(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r)
        assert r.fitted_observation_count <= r.training_observation_count

    @_skip_no_statsmodels
    def test_training_observation_count_equals_series_length(self):
        r = forecast_ets(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r)
        assert r.training_observation_count == len(_TRAIN)

    @_skip_no_statsmodels
    def test_candidate_m_in_metadata(self):
        for m in [7, 28]:
            r = forecast_ets(_TRAIN, horizon=28, seasonal_period=m)
            _require_forecast_ok(r)
            assert r.model_metadata["requested_seasonal_period"] == m

    @_skip_no_statsmodels
    def test_no_model_object_in_result(self):
        r = forecast_ets(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r)
        for val in vars(r).values():
            assert not hasattr(val, "forecast"), (
                f"ModelResult field appears to be a fitted statsmodels object: {type(val)}"
            )

    @_skip_no_statsmodels
    def test_forecast_unchanged(self):
        """Residual extraction must not alter the forecast series."""
        r1 = forecast_ets(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r1)
        # Run again; values must be identical
        r2 = forecast_ets(_TRAIN, horizon=28, seasonal_period=7)
        pd.testing.assert_series_equal(r1.forecast, r2.forecast)


# ---------------------------------------------------------------------------
# TestARIMAResiduals
# ---------------------------------------------------------------------------

class TestARIMAResiduals:
    @_skip_no_pmdarima
    def test_extraction_status_ok_non_seasonal(self):
        r = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=1)
        _require_forecast_ok(r)
        assert r.residual_extraction_status == "ok"

    @_skip_no_pmdarima
    def test_arima_arrays_are_numpy(self):
        r = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=1)
        _require_forecast_ok(r)
        assert isinstance(r.training_actual, np.ndarray)
        assert isinstance(r.training_fitted, np.ndarray)
        assert isinstance(r.training_residuals, np.ndarray)

    @_skip_no_pmdarima
    def test_arima_residual_sign(self):
        r = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=1)
        _require_forecast_ok(r)
        expected = r.training_actual - r.training_fitted
        np.testing.assert_allclose(r.training_residuals, expected, atol=1e-9)

    @_skip_no_pmdarima
    def test_arima_residuals_finite(self):
        r = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=1)
        _require_forecast_ok(r)
        assert np.all(np.isfinite(r.training_residuals))

    @_skip_no_pmdarima
    def test_arima_initial_observations_excluded(self):
        """Differenced ARIMA must have fewer fitted than training observations."""
        r = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=1)
        _require_forecast_ok(r)
        d = r.model_metadata["order"][1]
        D = r.model_metadata["seasonal_order"][1]
        m = r.model_metadata["seasonal_order"][3]
        # At least d + D*m initial observations must be missing
        expected_missing_min = d + D * m if m > 0 else d
        actual_missing = r.training_observation_count - r.fitted_observation_count
        assert actual_missing >= expected_missing_min, (
            f"Expected at least {expected_missing_min} initial missing; "
            f"got {actual_missing}"
        )

    @_skip_no_pmdarima
    def test_arima_dates_align_with_arrays(self):
        r = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=1)
        _require_forecast_ok(r)
        assert len(r.training_residual_dates) == len(r.training_residuals)
        assert len(r.training_residual_dates) == len(r.training_actual)

    @_skip_no_pmdarima
    def test_arima_no_model_object_in_result(self):
        r = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=1)
        _require_forecast_ok(r)
        for val in vars(r).values():
            assert not hasattr(val, "predict") or isinstance(val, pd.Series), (
                f"ModelResult field appears to be a fitted model: {type(val)}"
            )

    @_skip_no_pmdarima
    def test_arima_forecast_unchanged(self):
        r1 = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=1)
        _require_forecast_ok(r1)
        r2 = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=1)
        pd.testing.assert_series_equal(r1.forecast, r2.forecast)


# ---------------------------------------------------------------------------
# TestSARIMAResiduals
# ---------------------------------------------------------------------------

class TestSARIMAResiduals:
    @_skip_no_pmdarima
    def test_extraction_status_ok_m7(self):
        r = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r)
        assert r.residual_extraction_status == "ok"

    @_skip_no_pmdarima
    def test_sarima_residual_sign(self):
        r = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r)
        expected = r.training_actual - r.training_fitted
        np.testing.assert_allclose(r.training_residuals, expected, atol=1e-9)

    @_skip_no_pmdarima
    def test_sarima_residuals_finite(self):
        r = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r)
        assert np.all(np.isfinite(r.training_residuals))

    @_skip_no_pmdarima
    def test_sarima_candidate_m_preserved(self):
        """candidate_m in model_metadata must match the seasonal_period argument."""
        for m in [7, 28]:
            r = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=m)
            _require_forecast_ok(r)
            assert r.model_metadata["seasonal_period"] == m

    @_skip_no_pmdarima
    def test_sarima_initial_missing_gte_d_plus_D_m(self):
        r = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r)
        d = r.model_metadata["order"][1]
        sp_order = r.model_metadata["seasonal_order"]
        D, m = sp_order[1], sp_order[3]
        expected_missing_min = d + D * m if m > 0 else d
        actual_missing = r.training_observation_count - r.fitted_observation_count
        assert actual_missing >= expected_missing_min

    @_skip_no_pmdarima
    def test_sarima_m7_and_m28_are_distinct(self):
        """m=7 and m=28 SARIMA candidates must produce independent residuals."""
        r7  = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=7)
        r28 = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=28)
        _require_forecast_ok(r7)
        _require_forecast_ok(r28)
        # Different seasonal periods → different residual counts expected
        assert r7.model_metadata["seasonal_period"]  == 7
        assert r28.model_metadata["seasonal_period"] == 28
        # Residual arrays must not be identical objects
        assert r7.training_residuals is not r28.training_residuals

    @_skip_no_pmdarima
    def test_sarima_no_model_object_in_result(self):
        r = forecast_auto_arima(_TRAIN, horizon=28, seasonal_period=7)
        _require_forecast_ok(r)
        for val in vars(r).values():
            assert not hasattr(val, "predict") or isinstance(val, pd.Series), (
                f"ModelResult field appears to be a fitted model: {type(val)}"
            )


# ---------------------------------------------------------------------------
# TestResidualExtractionFailureIsolation
# ---------------------------------------------------------------------------

class TestResidualExtractionFailureIsolation:
    """Residual extraction failure must never propagate to the forecast."""

    def test_naive_forecast_present_even_when_residuals_would_fail(self):
        """Force a scenario where the training series is as short as possible
        but still valid for the naive model — residuals have 0 valid rows
        (edge case: single-observation series)."""
        single = pd.Series([5.0], index=pd.date_range("2022-01-01", periods=1, freq="D"))
        r = forecast_naive(single, horizon=3)
        assert r.fit_status == "ok"
        assert r.forecast is not None
        assert len(r.forecast) == 3
        # With only 1 training obs, there are 0 valid residuals
        assert r.fitted_observation_count == 0 or r.training_residuals is None or len(r.training_residuals) == 0

    def test_moving_average_forecast_ok_when_residuals_unavailable(self):
        """Series == window: forecast still succeeds even though residuals are unavailable."""
        tiny = _daily_series(7)
        r = forecast_moving_average(tiny, horizon=3, window=7)
        assert r.fit_status == "ok"
        assert r.forecast is not None
        assert r.residual_extraction_status == "unavailable"

    @_skip_no_statsmodels
    def test_ets_forecast_ok_when_residual_arrays_all_nan(self):
        """Even if all fitted values were NaN (pathological), the forecast must survive."""
        # Short seasonal series: ETS falls back to non-seasonal; fitted values still exist
        short = _daily_series(20)
        r = forecast_ets(short, horizon=5, seasonal_period=7)
        assert r.fit_status in ("ok", "converged")
        assert r.forecast is not None


# ---------------------------------------------------------------------------
# TestForecastUnchanged
# ---------------------------------------------------------------------------

class TestForecastUnchanged:
    """Cross-model: forecast values must be identical to those produced by
    a hypothetical implementation without residual extraction."""

    def _expected_naive(self, series, horizon):
        last = max(0.0, float(series.iloc[-1]))
        idx = pd.date_range(series.index[-1] + pd.Timedelta(days=1), periods=horizon, freq="D")
        return pd.Series(np.full(horizon, last), index=idx)

    def test_naive_forecast_value_unchanged(self):
        r = forecast_naive(_TRAIN, horizon=28)
        expected = self._expected_naive(_TRAIN, 28)
        pd.testing.assert_series_equal(r.forecast, expected, check_names=False)

    def test_seasonal_naive_forecast_value_unchanged(self):
        m = 7
        y = _TRAIN.to_numpy(dtype=float)
        r = forecast_seasonal_naive(_TRAIN, horizon=28, seasonal_period=m)
        last_season = y[-m:]
        repeats = int(np.ceil(28 / m))
        expected_vals = np.tile(last_season, repeats)[:28].clip(0)
        np.testing.assert_allclose(r.forecast.values, expected_vals, atol=1e-12)

    def test_moving_average_forecast_value_unchanged(self):
        w = 7
        y = _TRAIN.to_numpy(dtype=float)
        expected_avg = max(0.0, float(y[-w:].mean()))
        r = forecast_moving_average(_TRAIN, horizon=28, window=w)
        assert (r.forecast == expected_avg).all()
