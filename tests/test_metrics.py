"""Tests for src.models.metrics — the authoritative forecast evaluation module."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.config.forecasting import SEASONAL_CANDIDATES
from src.models.metrics import calculate_interval_metrics, calculate_point_metrics


# ---------------------------------------------------------------------------
# 1. Perfect forecast → all error metrics are zero
# ---------------------------------------------------------------------------

class TestPerfectForecast:
    def test_zero_errors(self):
        actual = [10.0, 5.0, 0.0, 8.0, 3.0]
        m = calculate_point_metrics(actual, actual)
        assert m["mae"] == pytest.approx(0.0)
        assert m["rmse"] == pytest.approx(0.0)
        assert m["wape"] == pytest.approx(0.0)
        assert m["bias"] == pytest.approx(0.0)

    def test_mase_lag1_zero_when_training_provided(self):
        actual = [4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0]
        training = [3.0, 5.0, 3.0, 5.0, 3.0, 5.0, 3.0, 5.0]
        m = calculate_point_metrics(actual, actual, training_series=training)
        assert m["mase_lag1"] == pytest.approx(0.0)
        assert m["mase_lag1_status"] == "ok"


# ---------------------------------------------------------------------------
# 2. Positive bias (over-forecast)
# ---------------------------------------------------------------------------

class TestPositiveBias:
    def test_forecast_exceeds_actual(self):
        actual = [5.0, 5.0, 5.0]
        forecast = [8.0, 8.0, 8.0]
        m = calculate_point_metrics(actual, forecast)
        assert m["bias"] == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# 3. Negative bias (under-forecast)
# ---------------------------------------------------------------------------

class TestNegativeBias:
    def test_forecast_below_actual(self):
        actual = [10.0, 10.0, 10.0]
        forecast = [6.0, 6.0, 6.0]
        m = calculate_point_metrics(actual, forecast)
        assert m["bias"] == pytest.approx(-4.0)


# ---------------------------------------------------------------------------
# 4. Zero actual values are preserved (not excluded) in MAE / RMSE / WAPE
# ---------------------------------------------------------------------------

class TestZeroActualsPreserved:
    def test_zeros_included_in_mae(self):
        actual = [0.0, 0.0, 10.0]
        forecast = [5.0, 5.0, 10.0]
        m = calculate_point_metrics(actual, forecast)
        # MAE = mean(5, 5, 0) = 10/3
        assert m["mae"] == pytest.approx(10.0 / 3.0)

    def test_zeros_included_in_rmse(self):
        actual = [0.0, 10.0]
        forecast = [4.0, 10.0]
        m = calculate_point_metrics(actual, forecast)
        # RMSE = sqrt(mean(16, 0)) = sqrt(8)
        assert m["rmse"] == pytest.approx(np.sqrt(8.0))

    def test_zeros_included_in_wape(self):
        actual = [0.0, 0.0, 10.0]
        forecast = [5.0, 5.0, 10.0]
        m = calculate_point_metrics(actual, forecast)
        # WAPE = (5+5+0) / (0+0+10) = 1.0
        assert m["wape"] == pytest.approx(1.0)
        assert m["wape_status"] == "ok"


# ---------------------------------------------------------------------------
# 5. All-zero actual series → WAPE is NaN with clear status
# ---------------------------------------------------------------------------

class TestAllZeroActuals:
    def test_wape_nan_on_all_zero_actuals(self):
        actual = [0.0, 0.0, 0.0]
        forecast = [1.0, 2.0, 3.0]
        m = calculate_point_metrics(actual, forecast)
        assert np.isnan(m["wape"])
        assert "all actuals are zero" in m["wape_status"]

    def test_mae_and_rmse_still_computable(self):
        actual = [0.0, 0.0]
        forecast = [3.0, 1.0]
        m = calculate_point_metrics(actual, forecast)
        assert m["mae"] == pytest.approx(2.0)
        assert m["rmse"] == pytest.approx(np.sqrt(5.0))


# ---------------------------------------------------------------------------
# 6. Valid mase_lag1 when training series has ≥ 2 observations
# ---------------------------------------------------------------------------

class TestValidMASELag1:
    def test_mase_lag1_computed_and_status_ok(self):
        rng = np.random.default_rng(42)
        training = rng.integers(1, 20, size=30).astype(float)
        actual = rng.integers(1, 20, size=10).astype(float)
        forecast = actual + rng.uniform(-1, 1, size=10)
        m = calculate_point_metrics(actual, forecast, training_series=training)
        assert not np.isnan(m["mase_lag1"])
        assert m["mase_lag1_status"] == "ok"

    def test_mase_lag1_formula_uses_lag1_denominator(self):
        # training = [1, 3, 5, 7, 9]; lag-1 diffs = [2, 2, 2, 2]; denom = 2.0
        training = np.array([1.0, 3.0, 5.0, 7.0, 9.0])
        actual = np.array([5.0, 5.0])
        forecast = np.array([4.0, 6.0])
        m = calculate_point_metrics(actual, forecast, training_series=training)
        # mae = mean(|5-4|, |5-6|) = 1.0; mase_lag1 = 1.0 / 2.0 = 0.5
        assert m["mae"] == pytest.approx(1.0)
        assert m["mase_lag1"] == pytest.approx(0.5)

    def test_mase_lag1_same_regardless_of_candidate_period(self):
        """Two calls with different candidate_seasonal_period must produce identical mase_lag1."""
        training = np.arange(1, 31, dtype=float)
        actual = np.array([15.0, 16.0])
        forecast = np.array([14.0, 17.0])
        m7 = calculate_point_metrics(
            actual, forecast, training_series=training,
            candidate_seasonal_period=7,
        )
        m30 = calculate_point_metrics(
            actual, forecast, training_series=training,
            candidate_seasonal_period=30,
        )
        assert m7["mase_lag1"] == pytest.approx(m30["mase_lag1"])


# ---------------------------------------------------------------------------
# 7. mase_m — per-candidate diagnostic using candidate's own period
# ---------------------------------------------------------------------------

class TestMaseMDiagnostic:
    def test_mase_m_nan_when_no_candidate_period(self):
        training = np.arange(1.0, 20.0)
        actual = [5.0, 6.0]
        forecast = [4.0, 7.0]
        m = calculate_point_metrics(actual, forecast, training_series=training)
        assert np.isnan(m["mase_m"])
        assert "no candidate seasonal period" in m["mase_m_status"]

    def test_mase_m_nan_when_candidate_period_is_1(self):
        training = np.arange(1.0, 20.0)
        actual = [5.0, 6.0]
        forecast = [4.0, 7.0]
        m = calculate_point_metrics(
            actual, forecast, training_series=training,
            candidate_seasonal_period=1,
        )
        assert np.isnan(m["mase_m"])

    def test_mase_m_uses_candidate_period_denominator(self):
        # training=[1,2,3,4,5,6,7,8]; lag-7 error: |8-1|=7 → denom=7; mae=1.0
        training = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
        actual = np.array([5.0, 5.0])
        forecast = np.array([4.0, 6.0])
        m = calculate_point_metrics(
            actual, forecast, training_series=training,
            candidate_seasonal_period=7,
        )
        assert m["mae"] == pytest.approx(1.0)
        assert m["mase_m"] == pytest.approx(1.0 / 7.0)

    def test_mase_m_differs_from_mase_lag1_when_lag_differs(self):
        training = np.arange(1.0, 31.0)
        actual = [15.0, 16.0]
        forecast = [14.0, 17.0]
        m = calculate_point_metrics(
            actual, forecast, training_series=training,
            candidate_seasonal_period=7,
        )
        # lag-1 and lag-7 denominators are different for a trending series
        assert not np.isnan(m["mase_lag1"])
        assert not np.isnan(m["mase_m"])
        assert m["mase_lag1"] != pytest.approx(m["mase_m"])

    def test_sarima_m7_and_m30_share_mase_lag1_denominator(self):
        """Simulates evaluating SARIMA-m7 and SARIMA-m30 on the same fold.

        Both should produce the same mase_lag1 because the lag-1 denominator is
        computed from the same training series and is independent of candidate_m.
        This is the key property that makes cross-candidate ranking fair.
        """
        training = np.arange(1.0, 91.0)    # 90 obs — enough for both m=7 and m=30
        actual = np.array([50.0, 51.0, 52.0, 53.0])
        # SARIMA-m7: slightly better forecast
        forecast_m7 = np.array([49.0, 51.0, 52.0, 54.0])
        # SARIMA-m30: slightly worse forecast
        forecast_m30 = np.array([48.0, 52.0, 53.0, 55.0])

        result_m7 = calculate_point_metrics(
            actual, forecast_m7, training_series=training,
            candidate_seasonal_period=7,
        )
        result_m30 = calculate_point_metrics(
            actual, forecast_m30, training_series=training,
            candidate_seasonal_period=30,
        )

        # Both use the same training series → same lag-1 denominator
        # mase_lag1 values differ only because MAE differs
        lag1_denom_m7 = result_m7["mae"] / result_m7["mase_lag1"]
        lag1_denom_m30 = result_m30["mae"] / result_m30["mase_lag1"]
        assert lag1_denom_m7 == pytest.approx(lag1_denom_m30), (
            "SARIMA-m7 and SARIMA-m30 must use the same lag-1 denominator "
            "so their mase_lag1 scores are directly comparable."
        )

        # mase_m values DIFFER because they use different denominators
        assert result_m7["mase_m"] != pytest.approx(result_m30["mase_m"])


# ---------------------------------------------------------------------------
# 8. Insufficient history → mase_lag1 is NaN with clear status
# ---------------------------------------------------------------------------

class TestInsufficientHistoryMASELag1:
    def test_no_training_series(self):
        m = calculate_point_metrics([5.0, 6.0], [5.0, 6.0])
        assert np.isnan(m["mase_lag1"])
        assert "no training series provided" in m["mase_lag1_status"]

    def test_single_obs_training_too_short_for_lag1(self):
        # lag-1 needs ≥ 2 observations; a single observation is insufficient
        m = calculate_point_metrics(
            [5.0, 6.0], [5.0, 6.0],
            training_series=[10.0],
        )
        assert np.isnan(m["mase_lag1"])
        assert "too short" in m["mase_lag1_status"]

    def test_two_obs_is_sufficient_for_lag1(self):
        m = calculate_point_metrics(
            [5.0], [5.0],
            training_series=[3.0, 6.0],
        )
        # lag-1 error = |6-3| = 3; mae = 0 → mase_lag1 = 0
        assert m["mase_lag1"] == pytest.approx(0.0)
        assert m["mase_lag1_status"] == "ok"


# ---------------------------------------------------------------------------
# 9. Interval coverage — fraction within bounds
# ---------------------------------------------------------------------------

class TestIntervalCoverage:
    def test_all_covered(self):
        actual = [5.0, 6.0, 7.0]
        lo = [4.0, 5.0, 6.0]
        hi = [6.0, 7.0, 8.0]
        m = calculate_interval_metrics(actual, lo, hi)
        assert m["interval_coverage"] == pytest.approx(1.0)
        assert m["coverage_status"] == "ok"

    def test_none_covered(self):
        actual = [0.0, 0.0, 0.0]
        lo = [1.0, 1.0, 1.0]
        hi = [2.0, 2.0, 2.0]
        m = calculate_interval_metrics(actual, lo, hi)
        assert m["interval_coverage"] == pytest.approx(0.0)

    def test_partial_coverage(self):
        actual = [1.0, 5.0, 10.0, 20.0]
        lo = [0.0, 4.0, 0.0, 0.0]
        hi = [2.0, 6.0, 5.0, 5.0]
        # rows 0 and 1 are inside bounds, rows 2 and 3 are not → 0.5 coverage
        m = calculate_interval_metrics(actual, lo, hi)
        assert m["interval_coverage"] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# 10. Interval width — mean of (upper - lower)
# ---------------------------------------------------------------------------

class TestIntervalWidth:
    def test_mean_width(self):
        lo = [1.0, 2.0, 3.0]
        hi = [3.0, 6.0, 9.0]  # widths: 2, 4, 6
        m = calculate_interval_metrics([2.0, 4.0, 6.0], lo, hi)
        assert m["mean_interval_width"] == pytest.approx(4.0)

    def test_negative_width_warns_in_status(self):
        lo = [5.0, 1.0]
        hi = [2.0, 3.0]  # first interval has negative width
        m = calculate_interval_metrics([3.0, 2.0], lo, hi)
        assert "negative width" in m["width_status"]


# ---------------------------------------------------------------------------
# 11. Mismatched lengths → ValueError
# ---------------------------------------------------------------------------

class TestMismatchedLengths:
    def test_point_metrics_mismatched(self):
        with pytest.raises(ValueError, match="same length"):
            calculate_point_metrics([1.0, 2.0], [1.0])

    def test_interval_metrics_mismatched(self):
        with pytest.raises(ValueError, match="same length"):
            calculate_interval_metrics([1.0, 2.0], [0.5, 0.5, 0.5], [1.5, 1.5, 1.5])


# ---------------------------------------------------------------------------
# 12. Null / non-numeric input → clear error or NaN handling
# ---------------------------------------------------------------------------

class TestNullAndNonNumericInputs:
    def test_non_numeric_string_raises(self):
        with pytest.raises((ValueError, TypeError)):
            calculate_point_metrics(["a", "b"], [1.0, 2.0])

    def test_nan_in_actual_propagates(self):
        actual = [1.0, np.nan, 3.0]
        forecast = [1.0, 2.0, 3.0]
        m = calculate_point_metrics(actual, forecast)
        # nan propagates through mean — result is nan
        assert np.isnan(m["mae"])

    def test_interval_nan_rows_excluded(self):
        actual = [5.0, np.nan, 5.0]
        lo = [4.0, 4.0, 4.0]
        hi = [6.0, 6.0, 6.0]
        m = calculate_interval_metrics(actual, lo, hi)
        # nan row excluded → 2 valid rows, both covered
        assert m["n_eval"] == 2
        assert m["interval_coverage"] == pytest.approx(1.0)

    def test_pandas_series_accepted(self):
        actual = pd.Series([1.0, 2.0, 3.0])
        forecast = pd.Series([1.0, 2.0, 3.0])
        m = calculate_point_metrics(actual, forecast)
        assert m["mae"] == pytest.approx(0.0)

    def test_empty_arrays_return_nan(self):
        m = calculate_point_metrics([], [])
        assert np.isnan(m["mae"])
        assert m["n_eval"] == 0
