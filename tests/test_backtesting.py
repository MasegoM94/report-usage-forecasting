"""Tests for src.models.backtesting.generate_rolling_splits."""
from __future__ import annotations

import pandas as pd
import pytest

from src.config.forecasting import (
    BACKTEST_FOLDS,
    BACKTEST_STEP_DAYS,
    FORECAST_HORIZON_DAYS,
    MIN_TRAIN_DAYS,
)
from src.models.backtesting import ForecastFold, generate_rolling_splits


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_series(n_days: int, start: str = "2022-01-01") -> pd.Series:
    """Return a daily integer series of length *n_days* with a DatetimeIndex."""
    idx = pd.date_range(start, periods=n_days, freq="D")
    return pd.Series(range(n_days), index=idx, dtype=float)


# Minimum size that comfortably yields 4 folds:
# min_train + (n_folds-1)*step + horizon = 180 + 3*28 + 28 = 292
_FULL_SERIES = _make_series(400)

HORIZON = FORECAST_HORIZON_DAYS    # 28
STEP = BACKTEST_STEP_DAYS          # 28
FOLDS = BACKTEST_FOLDS             # 4
MIN_TRAIN = MIN_TRAIN_DAYS         # 180


# ---------------------------------------------------------------------------
# 1. Correct number of folds
# ---------------------------------------------------------------------------

class TestFoldCount:
    def test_produces_requested_folds(self):
        folds, status = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        assert len(folds) == FOLDS
        assert status["ok"] is True
        assert status["n_produced"] == FOLDS

    def test_fold_numbers_are_one_based_and_sequential(self):
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        assert [f.fold_number for f in folds] == list(range(1, FOLDS + 1))

    def test_single_fold(self):
        folds, status = generate_rolling_splits(
            _make_series(MIN_TRAIN + HORIZON), horizon=HORIZON, n_folds=1,
            step=STEP, min_train_size=MIN_TRAIN
        )
        assert len(folds) == 1
        assert status["ok"] is True


# ---------------------------------------------------------------------------
# 2. Exact test horizon
# ---------------------------------------------------------------------------

class TestExactTestHorizon:
    def test_every_test_series_has_horizon_length(self):
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        for fold in folds:
            assert len(fold.test_series) == HORIZON, (
                f"Fold {fold.fold_number}: test length {len(fold.test_series)} != {HORIZON}"
            )

    def test_test_start_equals_cutoff_plus_one_day(self):
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        for fold in folds:
            expected = fold.cutoff_date + pd.Timedelta(days=1)
            assert fold.test_start == expected, (
                f"Fold {fold.fold_number}: test_start {fold.test_start} != {expected}"
            )

    def test_test_end_equals_test_start_plus_horizon_minus_one(self):
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        for fold in folds:
            expected = fold.test_start + pd.Timedelta(days=HORIZON - 1)
            assert fold.test_end == expected, (
                f"Fold {fold.fold_number}: test_end {fold.test_end} != {expected}"
            )

    def test_non_default_horizon(self):
        folds, _ = generate_rolling_splits(
            _make_series(300), horizon=14, n_folds=3, step=14, min_train_size=100
        )
        for fold in folds:
            assert len(fold.test_series) == 14


# ---------------------------------------------------------------------------
# 3. Correct expanding training windows
# ---------------------------------------------------------------------------

class TestExpandingTrainingWindows:
    def test_train_start_is_always_first_series_date(self):
        """Expanding window: every fold starts training from the series origin."""
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        first_date = _FULL_SERIES.index[0]
        for fold in folds:
            assert fold.train_start == first_date, (
                f"Fold {fold.fold_number}: train_start {fold.train_start} != series origin {first_date}"
            )

    def test_training_window_grows_by_step_each_fold(self):
        """Each successive fold must add exactly STEP observations to the training set."""
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        for i in range(1, len(folds)):
            diff = len(folds[i].train_series) - len(folds[i - 1].train_series)
            assert diff == STEP, (
                f"Folds {i}/{i+1}: training window grew by {diff} days, expected {STEP}"
            )

    def test_train_end_equals_cutoff_date(self):
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        for fold in folds:
            assert fold.train_end == fold.cutoff_date
            assert fold.train_series.index[-1] == fold.cutoff_date

    def test_first_fold_train_size_meets_minimum(self):
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        assert len(folds[0].train_series) >= MIN_TRAIN


# ---------------------------------------------------------------------------
# 4. No train-test overlap
# ---------------------------------------------------------------------------

class TestNoTrainTestOverlap:
    def test_train_and_test_dates_are_disjoint(self):
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        for fold in folds:
            train_dates = set(fold.train_series.index)
            test_dates = set(fold.test_series.index)
            overlap = train_dates & test_dates
            assert not overlap, (
                f"Fold {fold.fold_number}: {len(overlap)} overlapping dates between train and test."
            )

    def test_test_immediately_follows_train(self):
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        for fold in folds:
            gap = (fold.test_start - fold.train_end).days
            assert gap == 1, (
                f"Fold {fold.fold_number}: gap between train end and test start is {gap} days."
            )


# ---------------------------------------------------------------------------
# 5. Correct cutoff dates
# ---------------------------------------------------------------------------

class TestCutoffDates:
    def test_cutoffs_are_strictly_increasing(self):
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        for i in range(1, len(folds)):
            assert folds[i].cutoff_date > folds[i - 1].cutoff_date, (
                f"Cutoffs are not strictly increasing between folds {i} and {i+1}."
            )

    def test_cutoff_step_matches_step_parameter(self):
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        for i in range(1, len(folds)):
            delta = (folds[i].cutoff_date - folds[i - 1].cutoff_date).days
            assert delta == STEP, (
                f"Cutoff step between folds {i} and {i+1} is {delta} days, expected {STEP}."
            )

    def test_latest_fold_uses_tail_of_series(self):
        """The last fold's test window must end at the last date in the series."""
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        assert folds[-1].test_end == _FULL_SERIES.index[-1]

    def test_cutoff_dates_are_in_series_index(self):
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        series_dates = set(_FULL_SERIES.index)
        for fold in folds:
            assert fold.cutoff_date in series_dates


# ---------------------------------------------------------------------------
# 6. Insufficient total history
# ---------------------------------------------------------------------------

class TestInsufficientHistory:
    def test_raises_when_series_shorter_than_min_train_plus_horizon(self):
        too_short = _make_series(MIN_TRAIN + HORIZON - 1)
        with pytest.raises(ValueError, match="at least"):
            generate_rolling_splits(too_short, horizon=HORIZON, n_folds=1,
                                    step=STEP, min_train_size=MIN_TRAIN)

    def test_raises_on_exactly_zero_length_series(self):
        empty = pd.Series([], index=pd.DatetimeIndex([]), dtype=float)
        with pytest.raises(ValueError):
            generate_rolling_splits(empty, horizon=HORIZON, n_folds=1,
                                    step=STEP, min_train_size=MIN_TRAIN)

    def test_partial_folds_returned_with_ok_false(self):
        """Series long enough for 2 folds but not 4 → returns 2, status.ok=False."""
        # Need: min_train + (k-1)*step + horizon for k folds
        # k=2: 180 + 28 + 28 = 236 → use 236 (exactly 2 folds)
        n = MIN_TRAIN + STEP + HORIZON
        folds, status = generate_rolling_splits(
            _make_series(n), horizon=HORIZON, n_folds=4, step=STEP, min_train_size=MIN_TRAIN
        )
        assert status["ok"] is False
        assert status["n_produced"] < status["n_requested"]
        assert len(folds) == status["n_produced"]
        assert "Returning" in status["message"]

    def test_minimum_exactly_sufficient_for_one_fold(self):
        exact = _make_series(MIN_TRAIN + HORIZON)
        folds, status = generate_rolling_splits(
            exact, horizon=HORIZON, n_folds=1, step=STEP, min_train_size=MIN_TRAIN
        )
        assert len(folds) == 1
        assert status["ok"] is True


# ---------------------------------------------------------------------------
# 7. Discontinuous date index
# ---------------------------------------------------------------------------

class TestDiscontinuousIndex:
    def test_gap_in_index_raises_value_error(self):
        dates = pd.date_range("2022-01-01", periods=10, freq="D").tolist()
        dates.pop(5)  # introduce a gap
        s = pd.Series(range(9), index=pd.DatetimeIndex(dates), dtype=float)
        with pytest.raises(ValueError, match="gaps"):
            generate_rolling_splits(s, horizon=3, n_folds=1, step=3, min_train_size=5)

    def test_non_datetime_index_raises_value_error(self):
        s = pd.Series(range(300), index=range(300), dtype=float)
        with pytest.raises(ValueError, match="DatetimeIndex"):
            generate_rolling_splits(s, horizon=HORIZON, n_folds=1,
                                    step=STEP, min_train_size=MIN_TRAIN)

    def test_non_series_raises_type_error(self):
        import numpy as np
        arr = np.arange(300, dtype=float)
        with pytest.raises(TypeError, match="pd.Series"):
            generate_rolling_splits(arr, horizon=HORIZON, n_folds=1,
                                    step=STEP, min_train_size=MIN_TRAIN)


# ---------------------------------------------------------------------------
# 8. Unsorted date index
# ---------------------------------------------------------------------------

class TestUnsortedIndex:
    def test_reversed_index_raises_value_error(self):
        s = _make_series(300)
        s_rev = s.iloc[::-1]
        with pytest.raises(ValueError, match="sorted"):
            generate_rolling_splits(s_rev, horizon=HORIZON, n_folds=FOLDS,
                                    step=STEP, min_train_size=MIN_TRAIN)

    def test_single_out_of_order_date_raises(self):
        s = _make_series(300)
        # Swap two dates in the middle to break sort order
        vals = s.values.copy()
        idx = s.index.tolist()
        idx[100], idx[101] = idx[101], idx[100]
        s_bad = pd.Series(vals, index=pd.DatetimeIndex(idx))
        with pytest.raises(ValueError, match="sorted"):
            generate_rolling_splits(s_bad, horizon=HORIZON, n_folds=FOLDS,
                                    step=STEP, min_train_size=MIN_TRAIN)


# ---------------------------------------------------------------------------
# 9. Horizon larger than available data
# ---------------------------------------------------------------------------

class TestHorizonTooLarge:
    def test_horizon_equals_series_length_raises(self):
        s = _make_series(50)
        with pytest.raises(ValueError):
            generate_rolling_splits(s, horizon=50, n_folds=1,
                                    step=10, min_train_size=10)

    def test_horizon_larger_than_series_raises(self):
        s = _make_series(30)
        with pytest.raises(ValueError):
            generate_rolling_splits(s, horizon=100, n_folds=1,
                                    step=10, min_train_size=10)


# ---------------------------------------------------------------------------
# 10. Fold step behaviour
# ---------------------------------------------------------------------------

class TestFoldStepBehaviour:
    def test_step_equals_horizon_produces_non_overlapping_test_windows(self):
        """step == horizon → consecutive test windows share no dates."""
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=HORIZON, min_train_size=MIN_TRAIN
        )
        for i in range(1, len(folds)):
            prev_test = set(folds[i - 1].test_series.index)
            curr_test = set(folds[i].test_series.index)
            assert not prev_test & curr_test, (
                f"Folds {i}/{i+1} have overlapping test windows when step==horizon."
            )

    def test_step_less_than_horizon_produces_overlapping_test_windows(self):
        """step < horizon → consecutive test windows share dates."""
        small_step = HORIZON // 2
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=3, step=small_step, min_train_size=MIN_TRAIN
        )
        for i in range(1, len(folds)):
            prev_test = set(folds[i - 1].test_series.index)
            curr_test = set(folds[i].test_series.index)
            assert prev_test & curr_test, (
                f"Folds {i}/{i+1} have no overlapping test windows when step < horizon."
            )

    def test_invalid_step_zero_raises(self):
        with pytest.raises(ValueError, match="positive integer"):
            generate_rolling_splits(_FULL_SERIES, horizon=HORIZON, n_folds=FOLDS,
                                    step=0, min_train_size=MIN_TRAIN)

    def test_invalid_horizon_zero_raises(self):
        with pytest.raises(ValueError, match="positive integer"):
            generate_rolling_splits(_FULL_SERIES, horizon=0, n_folds=FOLDS,
                                    step=STEP, min_train_size=MIN_TRAIN)

    def test_original_datetime_index_preserved_in_train(self):
        """train_series must carry the original DatetimeIndex, not a reset integer index."""
        folds, _ = generate_rolling_splits(
            _FULL_SERIES, horizon=HORIZON, n_folds=FOLDS, step=STEP, min_train_size=MIN_TRAIN
        )
        for fold in folds:
            assert isinstance(fold.train_series.index, pd.DatetimeIndex)
            assert isinstance(fold.test_series.index, pd.DatetimeIndex)
