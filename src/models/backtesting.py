"""Rolling-origin time-series cross-validation splits.

Provides ``generate_rolling_splits`` — a pure data-splitting function that
partitions a continuous daily ``pd.Series`` into expanding-window train/test
folds suitable for fixed-horizon evaluation.

No model fitting occurs in this module.  The caller is responsible for
training and scoring each fold.

Terminology
-----------
cutoff date
    The last day of a training window.  The test window immediately follows,
    starting at ``cutoff_date + 1 day``.

expanding window
    Every fold adds one ``step`` of observations to the training set.  The
    earliest (fold 1) has the smallest training set; the latest fold has the
    largest.  This matches production behaviour where the model sees all
    available history up to the forecast origin.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from src.config.forecasting import (
    BACKTEST_FOLDS,
    BACKTEST_STEP_DAYS,
    FORECAST_HORIZON_DAYS,
    MIN_TRAIN_DAYS,
)


# ---------------------------------------------------------------------------
# Return type
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ForecastFold:
    """A single train/test split for rolling-origin evaluation.

    Attributes
    ----------
    fold_number:    1-based index, ordered oldest to newest.
    cutoff_date:    Last day of the training window (inclusive).
    train_start:    First day of the training window (inclusive).
    train_end:      Same as ``cutoff_date``.
    test_start:     First day of the test window (= ``cutoff_date + 1 day``).
    test_end:       Last day of the test window (inclusive).
    train_series:   Observations in [train_start, train_end].
    test_series:    Observations in [test_start, test_end].  Exactly
                    ``horizon`` elements long.
    """
    fold_number: int
    cutoff_date: pd.Timestamp
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp
    train_series: pd.Series = field(repr=False)
    test_series: pd.Series = field(repr=False)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_series(series: pd.Series) -> pd.Series:
    """Return *series* after asserting it is a valid continuous daily series.

    Raises
    ------
    TypeError
        If *series* is not a ``pd.Series``.
    ValueError
        If the index is not a DatetimeIndex, is not sorted, contains
        duplicates, or has gaps (non-daily steps).
    """
    if not isinstance(series, pd.Series):
        raise TypeError(f"'series' must be a pd.Series, got {type(series).__name__}.")

    idx = series.index
    if not isinstance(idx, pd.DatetimeIndex):
        raise ValueError(
            f"'series' index must be a DatetimeIndex, got {type(idx).__name__}."
        )

    if not idx.is_monotonic_increasing:
        raise ValueError(
            "'series' index must be sorted in ascending order. "
            f"First out-of-order date: {_first_unsorted_date(idx)}."
        )

    if idx.duplicated().any():
        dupes = idx[idx.duplicated()].unique()
        raise ValueError(
            f"'series' index contains duplicate dates: {list(dupes[:3])}{'...' if len(dupes) > 3 else ''}."
        )

    if len(idx) > 1:
        gaps = idx[1:][idx[1:] - idx[:-1] != pd.Timedelta(days=1)]
        if len(gaps):
            raise ValueError(
                f"'series' index has gaps (non-daily steps). "
                f"First gap ends at {gaps[0].date()} "
                f"(expected {(gaps[0] - pd.Timedelta(days=1)).date()} + 1 day)."
            )

    return series


def _first_unsorted_date(idx: pd.DatetimeIndex) -> str:
    for i in range(1, len(idx)):
        if idx[i] <= idx[i - 1]:
            return str(idx[i].date())
    return "unknown"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_rolling_splits(
    series: pd.Series,
    horizon: int = FORECAST_HORIZON_DAYS,
    n_folds: int = BACKTEST_FOLDS,
    step: int = BACKTEST_STEP_DAYS,
    min_train_size: int = MIN_TRAIN_DAYS,
) -> tuple[list[ForecastFold], dict]:
    """Partition *series* into expanding-window rolling-origin folds.

    Each fold covers a fixed ``horizon``-day test window immediately following
    a training window.  Folds are ordered oldest to newest; training windows
    expand by ``step`` days with each fold (expanding-window, not sliding).

    Parameters
    ----------
    series:
        A continuous daily ``pd.Series`` with a sorted ``DatetimeIndex`` and no
        gaps.  Must not contain missing dates; the function raises rather than
        filling gaps silently.
    horizon:
        Number of test observations per fold.  Every test window contains
        exactly this many days.
    n_folds:
        Requested number of folds.  If the series is too short to produce all
        *n_folds*, the function returns as many valid folds as possible and
        records the shortfall in the returned status dict (see below).
    step:
        Days between successive cutoff dates.  Setting ``step == horizon``
        means test windows are non-overlapping; ``step < horizon`` produces
        overlapping windows.
    min_train_size:
        Minimum number of observations required in the training window of the
        *first* (earliest) fold.  Folds whose training window would be shorter
        than this are skipped.

    Returns
    -------
    folds : list[ForecastFold]
        Ordered list of folds (fold 1 is oldest).  May be shorter than
        *n_folds* if the series is too short.
    status : dict
        ``{"ok": bool, "message": str, "n_produced": int, "n_requested": int}``

        ``ok`` is ``True`` when ``n_produced == n_requested``.  When ``False``,
        ``message`` explains the shortfall in plain English.

    Raises
    ------
    TypeError
        If *series* is not a ``pd.Series``.
    ValueError
        If the series index is not a sorted, contiguous ``DatetimeIndex``; or
        if *horizon*, *n_folds*, *step*, or *min_train_size* are not positive
        integers; or if the series has fewer observations than ``min_train_size
        + horizon`` (the minimum required to produce even one fold).

    Examples
    --------
    >>> import pandas as pd
    >>> idx = pd.date_range("2023-01-01", periods=365, freq="D")
    >>> s = pd.Series(range(365), index=idx)
    >>> folds, status = generate_rolling_splits(s, horizon=28, n_folds=4, step=28, min_train_size=180)
    >>> len(folds)
    4
    >>> len(folds[0].test_series)
    28
    """
    # --- Parameter validation -----------------------------------------------
    for name, val in [("horizon", horizon), ("n_folds", n_folds),
                      ("step", step), ("min_train_size", min_train_size)]:
        if not isinstance(val, int) or val < 1:
            raise ValueError(f"'{name}' must be a positive integer, got {val!r}.")

    series = _validate_series(series)
    n = len(series)

    # Absolute minimum to produce a single fold
    min_total = min_train_size + horizon
    if n < min_total:
        raise ValueError(
            f"Series has {n} observations but needs at least "
            f"min_train_size ({min_train_size}) + horizon ({horizon}) = {min_total} "
            f"to produce even one fold."
        )

    # --- Compute cutoff positions -------------------------------------------
    # The last fold's cutoff is at index (n - horizon - 1) so the test window
    # fits exactly at the tail of the series.
    # The first fold's cutoff is as early as possible given min_train_size:
    # cutoff_index = min_train_size - 1  (0-based; training = [0..cutoff_index])
    #
    # We generate candidate cutoffs backwards from the latest possible, stepping
    # by -step, then reverse so fold 1 is oldest.

    last_cutoff_idx = n - horizon - 1   # last possible cutoff (0-based)
    first_cutoff_idx = min_train_size - 1  # earliest valid cutoff

    if last_cutoff_idx < first_cutoff_idx:
        raise ValueError(
            f"Series has {n} observations.  With min_train_size={min_train_size} "
            f"and horizon={horizon}, the required window ({min_train_size + horizon}) "
            f"exceeds available data."
        )

    # Generate up to n_folds cutoff indices, working backwards from the latest.
    # This guarantees the newest fold always uses the most recent data, and
    # expanding windows grow naturally as we step forward.
    candidate_cutoff_indices = []
    cutoff_idx = last_cutoff_idx
    while cutoff_idx >= first_cutoff_idx and len(candidate_cutoff_indices) < n_folds:
        candidate_cutoff_indices.append(cutoff_idx)
        cutoff_idx -= step

    # Oldest first
    candidate_cutoff_indices.reverse()

    # --- Build folds --------------------------------------------------------
    folds: list[ForecastFold] = []
    idx = series.index

    for fold_num, ci in enumerate(candidate_cutoff_indices, start=1):
        train = series.iloc[: ci + 1]          # [0 .. cutoff_idx] inclusive
        test = series.iloc[ci + 1: ci + 1 + horizon]

        folds.append(
            ForecastFold(
                fold_number=fold_num,
                cutoff_date=idx[ci],
                train_start=idx[0],
                train_end=idx[ci],
                test_start=idx[ci + 1],
                test_end=idx[ci + horizon],
                train_series=train,
                test_series=test,
            )
        )

    # --- Status -------------------------------------------------------------
    n_produced = len(folds)
    n_requested = n_folds

    if n_produced == n_requested:
        status = {
            "ok": True,
            "message": f"Produced all {n_produced} requested folds.",
            "n_produced": n_produced,
            "n_requested": n_requested,
        }
    else:
        needed = min_train_size + (n_requested - 1) * step + horizon
        status = {
            "ok": False,
            "message": (
                f"Requested {n_requested} folds but series has only {n} observations. "
                f"To produce {n_requested} folds with min_train_size={min_train_size}, "
                f"step={step}, horizon={horizon}, at least {needed} observations are needed. "
                f"Returning {n_produced} valid fold(s)."
            ),
            "n_produced": n_produced,
            "n_requested": n_requested,
        }

    return folds, status
