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

    def test_mase_zero_when_training_provided(self):
        actual = [4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0]
        training = [3.0, 5.0, 3.0, 5.0, 3.0, 5.0, 3.0, 5.0]
        m = calculate_point_metrics(actual, actual, training_series=training)
        assert m["mase"] == pytest.approx(0.0)
        assert m["mase_status"] == "ok"


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
# 6. Valid MASE when training series is long enough
# ---------------------------------------------------------------------------

class TestValidMASE:
    def test_mase_computed_and_status_ok(self):
        rng = np.random.default_rng(42)
        training = rng.integers(1, 20, size=30).astype(float)
        actual = rng.integers(1, 20, size=10).astype(float)
        forecast = actual + rng.uniform(-1, 1, size=10)
        m = calculate_point_metrics(actual, forecast, training_series=training, seasonal_period=SEASONAL_CANDIDATES[0])
        assert not np.isnan(m["mase"])
        assert m["mase_status"] == "ok"

    def test_mase_formula_matches_manual(self):
        training = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
        actual = np.array([5.0, 5.0])
        forecast = np.array([4.0, 6.0])
        m = calculate_point_metrics(actual, forecast, training_series=training, seasonal_period=SEASONAL_CANDIDATES[0])
        # seasonal naive in-sample error: |y[7]-y[0]| = |8-1| = 7 → denom = 7
        # MAE = mean(|5-4|, |5-6|) = 1.0
        assert m["mae"] == pytest.approx(1.0)
        assert m["mase"] == pytest.approx(1.0 / 7.0)


# ---------------------------------------------------------------------------
# 7. Insufficient history → mase is NaN with clear status
# ---------------------------------------------------------------------------

class TestInsufficientHistoryMASE:
    def test_no_training_series(self):
        m = calculate_point_metrics([5.0, 6.0], [5.0, 6.0])
        assert np.isnan(m["mase"])
        assert "no training series provided" in m["mase_status"]

    def test_training_too_short(self):
        m = calculate_point_metrics(
            [5.0, 6.0], [5.0, 6.0],
            training_series=[1.0, 2.0, 3.0],  # only 3 obs, need ≥8 for period=7 (SEASONAL_CANDIDATES[0])
            seasonal_period=SEASONAL_CANDIDATES[0],
        )
        assert np.isnan(m["mase"])
        assert "too short" in m["mase_status"]

    def test_exactly_at_boundary_too_short(self):
        # Need > seasonal_period obs; exactly seasonal_period is still too short
        m = calculate_point_metrics(
            [1.0], [1.0],
            training_series=list(range(7)),  # 7 obs, need ≥8
            seasonal_period=SEASONAL_CANDIDATES[0],
        )
        assert np.isnan(m["mase"])


# ---------------------------------------------------------------------------
# 8. Interval coverage — fraction within bounds
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
# 9. Interval width — mean of (upper - lower)
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
# 10. Mismatched lengths → ValueError
# ---------------------------------------------------------------------------

class TestMismatchedLengths:
    def test_point_metrics_mismatched(self):
        with pytest.raises(ValueError, match="same length"):
            calculate_point_metrics([1.0, 2.0], [1.0])

    def test_interval_metrics_mismatched(self):
        with pytest.raises(ValueError, match="same length"):
            calculate_interval_metrics([1.0, 2.0], [0.5, 0.5, 0.5], [1.5, 1.5, 1.5])


# ---------------------------------------------------------------------------
# 11. Null / non-numeric input → clear error or NaN handling
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
