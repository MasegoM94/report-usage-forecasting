"""Authoritative forecast evaluation metrics.

All metric calculations for the forecasting pipeline live here.
Callers must not reimplement WAPE, MAE, RMSE, MASE, bias, or interval
coverage — import from this module instead.

Public API
----------
calculate_point_metrics(actual, forecast, training_series, candidate_seasonal_period)
    MAE, RMSE, WAPE, mase_lag1, mase_m, bias for a single evaluation window.

calculate_interval_metrics(actual, lower_bound, upper_bound)
    Empirical coverage and mean width of prediction intervals.

Design rules
------------
* Zero actual values are preserved throughout — no row is silently dropped
  because actual == 0.
* Where a metric is mathematically undefined (e.g. WAPE when all actuals are
  zero, MASE when training history is too short), the returned value is
  ``np.nan`` and a plain-English ``*_status`` key explains why.
* All inputs are coerced to ``float64`` numpy arrays; mismatched lengths raise
  ``ValueError`` before any computation.
* MAPE is intentionally absent from the public API.  It is undefined when
  actuals are zero (common in daily report-usage data) and produces
  asymmetrically large values when actuals are small.  WAPE is the
  recommended scale-free alternative.
* ``mase_lag1`` always uses a lag-1 random-walk in-sample error as the
  denominator, making it directly comparable across all candidates regardless
  of their seasonal period.  A common denominator is required: if SARIMA-m7
  used a lag-7 denominator and SARIMA-m30 used a lag-30 denominator, their
  MASE scores would measure performance relative to different baselines and
  cannot be fairly ranked against each other.
* ``mase_m`` uses the candidate's own seasonal period as the denominator and
  is provided for diagnostic purposes only — never use it for cross-candidate
  ranking.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Input helpers
# ---------------------------------------------------------------------------

def _to_float_array(x: "np.ndarray | pd.Series | list", name: str) -> np.ndarray:
    """Coerce *x* to a 1-D float64 numpy array, raising on non-numeric input."""
    if isinstance(x, pd.Series):
        arr = pd.to_numeric(x, errors="coerce").to_numpy(dtype=float)
    else:
        try:
            arr = np.asarray(x, dtype=float)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"'{name}' could not be converted to float array: {exc}") from exc
    if arr.ndim != 1:
        raise ValueError(f"'{name}' must be 1-D, got shape {arr.shape}.")
    return arr


def _validate_equal_lengths(**arrays: np.ndarray) -> int:
    """Raise ``ValueError`` if any arrays differ in length; return the common length."""
    lengths = {name: len(arr) for name, arr in arrays.items()}
    unique = set(lengths.values())
    if len(unique) != 1:
        detail = ", ".join(f"{n}={l}" for n, l in lengths.items())
        raise ValueError(f"All arrays must have the same length. Got: {detail}.")
    return unique.pop()


# ---------------------------------------------------------------------------
# Point metrics
# ---------------------------------------------------------------------------

def calculate_point_metrics(
    actual: "np.ndarray | pd.Series | list",
    forecast: "np.ndarray | pd.Series | list",
    training_series: "np.ndarray | pd.Series | list | None" = None,
    candidate_seasonal_period: Optional[int] = None,
) -> dict[str, float]:
    """Compute MAE, RMSE, WAPE, mase_lag1, mase_m, and bias for a single evaluation window.

    Parameters
    ----------
    actual:
        Observed values in the evaluation window.  Zero values are retained;
        they are not filtered before computing any metric.
    forecast:
        Predicted values aligned with *actual*.
    training_series:
        In-sample observations used to compute the MASE denominators.  When
        ``None``, ``mase_lag1`` and ``mase_m`` are ``np.nan``.
    candidate_seasonal_period:
        When provided and greater than 1, ``mase_m`` is computed using this
        lag as the seasonal-naive denominator.  This is a per-candidate
        diagnostic metric only — it is not used for cross-candidate ranking.
        When ``None`` or ``<= 1``, ``mase_m`` is ``np.nan``.

    Returns
    -------
    dict with keys:
        mae             — Mean Absolute Error: mean(|actual - forecast|)
        rmse            — Root Mean Squared Error: sqrt(mean((actual - forecast)²))
        wape            — Weighted Absolute Percentage Error:
                          sum(|actual - forecast|) / sum(|actual|).
                          ``np.nan`` when all actuals are zero.
        bias            — Mean signed error: mean(forecast - actual).
        mase_lag1       — Mean Absolute Scaled Error using a lag-1 random-walk
                          in-sample error as the denominator.  This is the
                          primary MASE used for cross-candidate ranking because
                          every candidate uses the same denominator within a fold.
                          ``np.nan`` when training data is absent or too short
                          (needs ≥ 2 observations).
        mase_m          — MASE using the candidate's own ``candidate_seasonal_period``
                          lag as the denominator.  Diagnostic only.  ``np.nan``
                          when ``candidate_seasonal_period`` is not provided or
                          when training data is insufficient.
        wape_status     — "ok" | "undefined: all actuals are zero"
        mase_lag1_status — "ok" | "undefined: ..."
        mase_m_status   — "ok" | "undefined: ..."
        n_eval          — Number of evaluation observations used.

    Raises
    ------
    ValueError
        When *actual* and *forecast* have different lengths, or when either
        cannot be converted to a numeric array.
    """
    a = _to_float_array(actual, "actual")
    f = _to_float_array(forecast, "forecast")
    _validate_equal_lengths(actual=a, forecast=f)

    if len(a) == 0:
        return {
            "mae": np.nan,
            "rmse": np.nan,
            "wape": np.nan,
            "bias": np.nan,
            "mase_lag1": np.nan,
            "mase_m": np.nan,
            "wape_status": "undefined: empty evaluation window",
            "mase_lag1_status": "undefined: empty evaluation window",
            "mase_m_status": "undefined: empty evaluation window",
            "n_eval": 0,
        }

    errors = a - f
    abs_errors = np.abs(errors)

    mae = float(np.mean(abs_errors))
    rmse = float(np.sqrt(np.mean(errors ** 2)))
    bias = float(np.mean(f - a))

    # WAPE — safe for zero actuals (the denominator is sum not mean)
    abs_actual_sum = float(np.sum(np.abs(a)))
    if abs_actual_sum == 0:
        wape_val = np.nan
        wape_status = "undefined: all actuals are zero"
    else:
        wape_val = float(np.sum(abs_errors) / abs_actual_sum)
        wape_status = "ok"

    # mase_lag1 — always uses lag-1 random-walk denominator.
    # This is the primary ranking metric: every candidate in a fold uses the
    # same denominator, so scores are directly comparable regardless of m.
    mase_lag1_val, mase_lag1_status = _compute_mase(mae, training_series, 1)

    # mase_m — per-candidate diagnostic using the candidate's own seasonal lag.
    if candidate_seasonal_period is not None and candidate_seasonal_period > 1:
        mase_m_val, mase_m_status = _compute_mase(
            mae, training_series, candidate_seasonal_period
        )
    else:
        mase_m_val = np.nan
        mase_m_status = "undefined: no candidate seasonal period provided"

    return {
        "mae": mae,
        "rmse": rmse,
        "wape": wape_val,
        "bias": bias,
        "mase_lag1": mase_lag1_val,
        "mase_m": mase_m_val,
        "wape_status": wape_status,
        "mase_lag1_status": mase_lag1_status,
        "mase_m_status": mase_m_status,
        "n_eval": len(a),
    }


def _compute_mase(
    mae: float,
    training_series: "np.ndarray | pd.Series | list | None",
    seasonal_period: int,
) -> tuple[float, str]:
    """Return ``(mase_value, mase_status)`` pair.

    MASE = MAE_test / MAE_seasonal_naive_train

    The seasonal-naive MAE in sample is::

        mean(|y[t] - y[t - m]|)  for t = m, m+1, …, T_train

    where *m* is ``seasonal_period``.  When ``seasonal_period=1`` this
    reduces to the random-walk (lag-1) in-sample error.
    """
    if training_series is None:
        return np.nan, "undefined: no training series provided"

    tr = _to_float_array(training_series, "training_series")
    min_required = seasonal_period + 1
    if len(tr) < min_required:
        return (
            np.nan,
            f"undefined: training series too short for seasonal_period={seasonal_period} "
            f"(need ≥{min_required} obs, got {len(tr)})",
        )

    # Seasonal naive in-sample errors: |y[t] - y[t-m]|
    in_sample_errors = np.abs(tr[seasonal_period:] - tr[:-seasonal_period])
    denom = float(np.mean(in_sample_errors))

    if denom == 0:
        return np.nan, "undefined: seasonal naive denominator is zero"

    return float(mae / denom), "ok"


# ---------------------------------------------------------------------------
# Interval metrics
# ---------------------------------------------------------------------------

def calculate_interval_metrics(
    actual: "np.ndarray | pd.Series | list",
    lower_bound: "np.ndarray | pd.Series | list",
    upper_bound: "np.ndarray | pd.Series | list",
) -> dict[str, float]:
    """Compute empirical coverage and mean width of prediction intervals.

    Parameters
    ----------
    actual:
        Observed values in the evaluation window.
    lower_bound:
        Lower bound of the prediction interval for each observation.
    upper_bound:
        Upper bound of the prediction interval for each observation.

    Returns
    -------
    dict with keys:
        interval_coverage   — Fraction of actuals falling within
                              [lower_bound, upper_bound].  Ideal value for a
                              nominal 95 % interval is 0.95.
        mean_interval_width — Mean of (upper_bound - lower_bound).  Narrower
                              is better, given adequate coverage.
        n_eval              — Number of observations used (NaN rows excluded).
        coverage_status     — "ok" | "undefined: no valid observations"
        width_status        — "ok" | "undefined: no valid observations" |
                              "warn: some intervals have negative width"

    Raises
    ------
    ValueError
        When *actual*, *lower_bound*, and *upper_bound* have different lengths,
        or when any array cannot be converted to numeric.
    """
    a = _to_float_array(actual, "actual")
    lo = _to_float_array(lower_bound, "lower_bound")
    hi = _to_float_array(upper_bound, "upper_bound")
    _validate_equal_lengths(actual=a, lower_bound=lo, upper_bound=hi)

    # Only evaluate rows where all three values are finite
    valid = np.isfinite(a) & np.isfinite(lo) & np.isfinite(hi)
    n_eval = int(valid.sum())

    if n_eval == 0:
        return {
            "interval_coverage": np.nan,
            "mean_interval_width": np.nan,
            "n_eval": 0,
            "coverage_status": "undefined: no valid observations",
            "width_status": "undefined: no valid observations",
        }

    a_v, lo_v, hi_v = a[valid], lo[valid], hi[valid]
    covered = (a_v >= lo_v) & (a_v <= hi_v)
    coverage = float(covered.mean())

    widths = hi_v - lo_v
    mean_width = float(np.mean(widths))

    width_status = "warn: some intervals have negative width" if (widths < 0).any() else "ok"

    return {
        "interval_coverage": coverage,
        "mean_interval_width": mean_width,
        "n_eval": n_eval,
        "coverage_status": "ok",
        "width_status": width_status,
    }
