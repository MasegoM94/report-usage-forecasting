"""Candidate forecasting model functions with a consistent result contract.

Every public function in this module follows the same call signature::

    forecast_<name>(
        training_series: pd.Series,
        horizon: int,
        **model_specific_kwargs,
    ) -> ModelResult

and returns a single ``ModelResult`` dataclass.

Contract guarantees
-------------------
* ``forecast``, ``lower_bound``, ``upper_bound`` carry the published values —
  raw model output clipped to [0, ∞) because report-view counts cannot be
  negative.
* ``forecast_raw``, ``lower_bound_raw``, ``upper_bound_raw`` carry the
  unclipped model output for diagnostics.  For models without native
  prediction intervals, the ``*_bound*`` fields are ``None``.
* ``fit_status`` is ``"ok"`` for deterministic baselines, ``"converged"`` /
  ``"did_not_converge"`` for statistical models, and ``"failed"`` when an
  exception prevented the model from producing a forecast.
* When ``fit_status == "failed"``, ``forecast`` / ``*_bound*`` fields are
  ``None`` and ``error_message`` contains the exception text.  Callers must
  check ``fit_status`` before accessing forecast arrays.
* No evaluation metrics are computed inside these functions.
* A failure in one model function never raises to the caller; the failed
  ``ModelResult`` is returned instead so evaluation of sibling models
  continues uninterrupted.

Forecast index
--------------
All returned series share the same ``pd.DatetimeIndex`` starting one day
after ``training_series.index[-1]`` and running for exactly ``horizon`` days
at daily frequency.  Callers can rely on index alignment across models.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from src.config.forecasting import (
    FORECAST_HORIZON_DAYS,
    MIN_SEASONAL_CYCLES,
    NON_SEASONAL_PERIOD,
    SEASONAL_CANDIDATES,
)


# ---------------------------------------------------------------------------
# Result contract
# ---------------------------------------------------------------------------

@dataclass
class ModelResult:
    """Uniform return type for every candidate model function.

    Attributes
    ----------
    model_name:
        Short stable identifier (e.g. ``"naive"``, ``"auto_arima"``).
    forecast:
        Published point forecast — raw output clipped to ≥ 0.
        ``None`` when ``fit_status == "failed"``.
    lower_bound:
        Published lower prediction-interval bound clipped to ≥ 0.
        ``None`` for models without native intervals or on failure.
    upper_bound:
        Published upper prediction-interval bound clipped to ≥ 0.
        ``None`` for models without native intervals or on failure.
    forecast_raw:
        Unclipped point forecast direct from the model.  ``None`` on failure.
    lower_bound_raw:
        Unclipped lower bound.  ``None`` when unavailable or on failure.
    upper_bound_raw:
        Unclipped upper bound.  ``None`` when unavailable or on failure.
    model_metadata:
        Model-specific fit information (order, window, AIC, etc.).
        Always a dict; empty on failure.
    fit_status:
        ``"ok"`` for deterministic baselines.
        ``"converged"`` / ``"did_not_converge"`` for statistical models.
        ``"failed"`` when an exception prevented forecasting.
    error_message:
        Exception text when ``fit_status == "failed"``; ``None`` otherwise.
    """
    model_name: str
    forecast: Optional[pd.Series]
    lower_bound: Optional[pd.Series]
    upper_bound: Optional[pd.Series]
    forecast_raw: Optional[pd.Series]
    lower_bound_raw: Optional[pd.Series]
    upper_bound_raw: Optional[pd.Series]
    model_metadata: dict = field(default_factory=dict)
    fit_status: str = "ok"
    error_message: Optional[str] = None


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _forecast_index(training_series: pd.Series, horizon: int) -> pd.DatetimeIndex:
    """Return the daily DatetimeIndex for the forecast window."""
    return pd.date_range(
        start=training_series.index[-1] + pd.Timedelta(days=1),
        periods=horizon,
        freq="D",
    )


def _clip(s: pd.Series) -> pd.Series:
    """Clip a forecast series to [0, ∞) — published post-processing step."""
    return s.clip(lower=0)


def _as_series(arr: np.ndarray, index: pd.DatetimeIndex, name: str) -> pd.Series:
    return pd.Series(arr.astype(float), index=index, name=name)


def _validate_training_series(series: pd.Series) -> None:
    if not isinstance(series, pd.Series):
        raise TypeError(f"training_series must be a pd.Series, got {type(series).__name__}.")
    if not isinstance(series.index, pd.DatetimeIndex):
        raise ValueError("training_series must have a DatetimeIndex.")
    if series.empty:
        raise ValueError("training_series is empty.")


def _failed(model_name: str, exc: Exception) -> ModelResult:
    """Return a ModelResult representing a model that raised during fitting."""
    return ModelResult(
        model_name=model_name,
        forecast=None,
        lower_bound=None,
        upper_bound=None,
        forecast_raw=None,
        lower_bound_raw=None,
        upper_bound_raw=None,
        model_metadata={},
        fit_status="failed",
        error_message=f"{type(exc).__name__}: {exc}",
    )


# ---------------------------------------------------------------------------
# 1. Naïve (last-value)
# ---------------------------------------------------------------------------

def forecast_naive(
    training_series: pd.Series,
    horizon: int = FORECAST_HORIZON_DAYS,
) -> ModelResult:
    """Forecast every future day as the most recently observed value.

    Metadata keys
    -------------
    lag : int
        Always 1 — the naive model repeats the observation one period back.
    last_observed : float
        The training value used as the forecast.
    """
    try:
        _validate_training_series(training_series)
        idx = _forecast_index(training_series, horizon)
        last_val = float(training_series.iloc[-1])
        raw = _as_series(np.full(horizon, last_val), idx, "forecast")
        return ModelResult(
            model_name="naive",
            forecast=_clip(raw),
            lower_bound=None,
            upper_bound=None,
            forecast_raw=raw,
            lower_bound_raw=None,
            upper_bound_raw=None,
            model_metadata={"lag": 1, "last_observed": last_val},
            fit_status="ok",
        )
    except Exception as exc:
        return _failed("naive", exc)


# ---------------------------------------------------------------------------
# 2. Seasonal naïve
# ---------------------------------------------------------------------------

def forecast_seasonal_naive(
    training_series: pd.Series,
    horizon: int = FORECAST_HORIZON_DAYS,
    seasonal_period: int = SEASONAL_CANDIDATES[0],
) -> ModelResult:
    """Forecast by repeating the most recent full seasonal cycle.

    Falls back to the last observed value when the training series is shorter
    than one seasonal period.

    Metadata keys
    -------------
    seasonal_period : int
        The lag used for pattern repetition.
    fallback_to_naive : bool
        ``True`` when the training series was too short for a seasonal pattern
        and the naïve last-value was used instead.
    """
    _name = f"seasonal_naive_m{seasonal_period}"
    try:
        _validate_training_series(training_series)
        idx = _forecast_index(training_series, horizon)
        y = training_series.to_numpy(dtype=float)

        if len(y) < seasonal_period:
            values = np.full(horizon, y[-1])
            fallback = True
        else:
            last_season = y[-seasonal_period:]
            repeats = int(np.ceil(horizon / seasonal_period))
            values = np.tile(last_season, repeats)[:horizon]
            fallback = False

        raw = _as_series(values, idx, "forecast")
        return ModelResult(
            model_name=_name,
            forecast=_clip(raw),
            lower_bound=None,
            upper_bound=None,
            forecast_raw=raw,
            lower_bound_raw=None,
            upper_bound_raw=None,
            model_metadata={"seasonal_period": seasonal_period, "fallback_to_naive": fallback},
            fit_status="ok",
        )
    except Exception as exc:
        return _failed(_name, exc)


# ---------------------------------------------------------------------------
# 3. Moving average
# ---------------------------------------------------------------------------

def forecast_moving_average(
    training_series: pd.Series,
    horizon: int = FORECAST_HORIZON_DAYS,
    window: int = SEASONAL_CANDIDATES[0],
) -> ModelResult:
    """Forecast every future day as the mean of the last *window* observations.

    Metadata keys
    -------------
    window : int
        Number of most-recent training observations averaged.
    effective_window : int
        Actual window used — may be smaller than *window* when fewer training
        observations are available.
    moving_average_value : float
        The constant value used for all forecast periods.
    """
    try:
        if not isinstance(window, int) or window < 1:
            raise ValueError(f"'window' must be a positive integer, got {window!r}.")
        _validate_training_series(training_series)
        idx = _forecast_index(training_series, horizon)
        effective_window = min(window, len(training_series))
        avg = float(training_series.iloc[-effective_window:].mean())
        raw = _as_series(np.full(horizon, avg), idx, "forecast")
        return ModelResult(
            model_name="moving_average",
            forecast=_clip(raw),
            lower_bound=None,
            upper_bound=None,
            forecast_raw=raw,
            lower_bound_raw=None,
            upper_bound_raw=None,
            model_metadata={
                "window": window,
                "effective_window": effective_window,
                "moving_average_value": avg,
            },
            fit_status="ok",
        )
    except Exception as exc:
        return _failed("moving_average", exc)


# ---------------------------------------------------------------------------
# 4. ETS (Holt-Winters Exponential Smoothing)
# ---------------------------------------------------------------------------

def forecast_ets(
    training_series: pd.Series,
    horizon: int = FORECAST_HORIZON_DAYS,
    seasonal_period: int = SEASONAL_CANDIDATES[0],
    alpha: float = 0.05,
) -> ModelResult:
    """Fit a Holt-Winters ETS model and forecast *horizon* days ahead.

    Uses additive error, additive trend, and additive seasonal components
    (ETS(A,A,A)).  When the series is too short for seasonal decomposition
    (fewer than ``MIN_SEASONAL_CYCLES`` full seasonal cycles), trend-only
    smoothing is used (ETS(A,A,N)) so a result is always returned rather
    than failing.  Pass ``seasonal_period=NON_SEASONAL_PERIOD`` (= 1) to
    request a non-seasonal ETS(A,A,N) explicitly.

    Metadata keys
    -------------
    error_type : str       Always ``"add"`` (additive errors).
    trend : str            ``"add"`` or ``None``.
    seasonal : str         ``"add"`` or ``None``.
    seasonal_periods : int Lag used for the seasonal component; ``None`` when
                           non-seasonal fallback was applied.
    requested_seasonal_period : int
        The ``seasonal_period`` argument supplied by the caller — preserved
        even when the non-seasonal fallback is used.
    aic : float            Akaike Information Criterion from the fitted model.
    bic : float            Bayesian Information Criterion.
    converged : bool       Whether the optimiser reported convergence.
    fallback_no_seasonal : bool
        ``True`` when seasonal fitting was skipped due to insufficient history
        or because ``seasonal_period == NON_SEASONAL_PERIOD``.
    prediction_interval_alpha : float
        The alpha level used for prediction intervals (default 0.05 → 95 %).
    """
    _name = f"ets_m{seasonal_period}"
    try:
        from statsmodels.tsa.holtwinters import ExponentialSmoothing

        _validate_training_series(training_series)
        idx = _forecast_index(training_series, horizon)
        y = training_series.astype(float)

        # Seasonal ETS requires at least MIN_SEASONAL_CYCLES full cycles.
        # seasonal_period=1 (NON_SEASONAL_PERIOD) always uses the non-seasonal spec.
        use_seasonal = (
            seasonal_period > NON_SEASONAL_PERIOD
            and len(y) >= MIN_SEASONAL_CYCLES * seasonal_period
        )
        model = ExponentialSmoothing(
            y,
            trend="add",
            seasonal="add" if use_seasonal else None,
            seasonal_periods=seasonal_period if use_seasonal else None,
            initialization_method="estimated",
        )
        fit = model.fit(optimized=True, remove_bias=False)

        fc_raw_arr = fit.forecast(horizon)
        raw = _as_series(fc_raw_arr.values, idx, "forecast")

        # Prediction intervals via simulation (statsmodels >= 0.12)
        try:
            pred = fit.get_prediction(
                start=len(y),
                end=len(y) + horizon - 1,
            )
            summary = pred.summary_frame(alpha=alpha)
            lo_raw = _as_series(summary["mean_ci_lower"].values, idx, "lower_bound")
            hi_raw = _as_series(summary["mean_ci_upper"].values, idx, "upper_bound")
        except Exception:
            lo_raw = None
            hi_raw = None

        return ModelResult(
            model_name=_name,
            forecast=_clip(raw),
            lower_bound=_clip(lo_raw) if lo_raw is not None else None,
            upper_bound=_clip(hi_raw) if hi_raw is not None else None,
            forecast_raw=raw,
            lower_bound_raw=lo_raw,
            upper_bound_raw=hi_raw,
            model_metadata={
                "error_type": "add",
                "trend": "add",
                "seasonal": "add" if use_seasonal else None,
                "seasonal_periods": seasonal_period if use_seasonal else None,
                "requested_seasonal_period": seasonal_period,
                "aic": float(fit.aic),
                "bic": float(fit.bic),
                "converged": bool(fit.mle_retvals.get("converged", True)
                                  if hasattr(fit, "mle_retvals") and fit.mle_retvals
                                  else True),
                "fallback_no_seasonal": not use_seasonal,
                "prediction_interval_alpha": alpha,
            },
            fit_status="converged",
        )
    except Exception as exc:
        return _failed(_name, exc)


# ---------------------------------------------------------------------------
# 5. Auto-ARIMA (SARIMA via pmdarima)
# ---------------------------------------------------------------------------

def forecast_auto_arima(
    training_series: pd.Series,
    horizon: int = FORECAST_HORIZON_DAYS,
    seasonal_period: int = SEASONAL_CANDIDATES[0],
    alpha: float = 0.05,
) -> ModelResult:
    """Fit an Auto-ARIMA / SARIMA model and forecast *horizon* days ahead.

    Uses ``pmdarima.auto_arima`` with stepwise search.  When
    ``seasonal_period == NON_SEASONAL_PERIOD`` (= 1), the search is
    restricted to non-seasonal ARIMA (no seasonal terms).  For all other
    values of ``seasonal_period``, a SARIMA search is run with ``m`` set
    to that period.

    The function validates that the training series contains at least
    ``MIN_SEASONAL_CYCLES × seasonal_period`` observations before fitting
    a seasonal model.  Shorter series return a ``failed`` ModelResult so
    that the rest of the evaluation continues uninterrupted.

    Metadata keys
    -------------
    order : tuple[int, int, int]          ARIMA (p, d, q).
    seasonal_order : tuple[int, int, int, int]  Seasonal (P, D, Q, m);
                                          (0, 0, 0, 0) for non-seasonal fits.
    seasonal_period : int                 The ``seasonal_period`` argument.
    aic : float                           Akaike Information Criterion.
    aicc : float                          Corrected AIC (penalises small samples).
    bic : float                           Bayesian Information Criterion.
    model_str : str                       Human-readable order string.
    n_train : int                         Number of training observations.
    """
    _name = f"auto_arima_m{seasonal_period}"
    try:
        from pmdarima import auto_arima

        _validate_training_series(training_series)
        idx = _forecast_index(training_series, horizon)
        y = training_series.astype(float)

        use_seasonal = seasonal_period > NON_SEASONAL_PERIOD
        if use_seasonal and len(y) < MIN_SEASONAL_CYCLES * seasonal_period:
            raise ValueError(
                f"Insufficient training history for seasonal_period={seasonal_period}: "
                f"need at least {MIN_SEASONAL_CYCLES} × {seasonal_period} = "
                f"{MIN_SEASONAL_CYCLES * seasonal_period} observations, "
                f"got {len(y)}."
            )

        arima_kwargs: dict = dict(
            stepwise=True,
            suppress_warnings=True,
            error_action="ignore",
            trace=False,
        )
        if use_seasonal:
            arima_kwargs["seasonal"] = True
            arima_kwargs["m"] = seasonal_period
        else:
            arima_kwargs["seasonal"] = False

        model = auto_arima(y, **arima_kwargs)

        fc_raw_arr, conf_int = model.predict(n_periods=horizon, return_conf_int=True, alpha=alpha)
        raw = _as_series(fc_raw_arr, idx, "forecast")

        conf = np.asarray(conf_int, dtype=float)
        lo_raw = _as_series(conf[:, 0], idx, "lower_bound")
        hi_raw = _as_series(conf[:, 1], idx, "upper_bound")

        p, d, q = model.order
        sp, sd, sq, m = model.seasonal_order
        model_str = f"ARIMA({p},{d},{q})x({sp},{sd},{sq},{m})"

        try:
            aic_val = float(model.aic())
        except Exception:
            aic_val = float("nan")
        try:
            aicc_val = float(model.aicc())
        except Exception:
            aicc_val = float("nan")
        try:
            bic_val = float(model.bic())
        except Exception:
            bic_val = float("nan")

        return ModelResult(
            model_name=_name,
            forecast=_clip(raw),
            lower_bound=_clip(lo_raw),
            upper_bound=_clip(hi_raw),
            forecast_raw=raw,
            lower_bound_raw=lo_raw,
            upper_bound_raw=hi_raw,
            model_metadata={
                "order": (p, d, q),
                "seasonal_order": (sp, sd, sq, m),
                "seasonal_period": seasonal_period,
                "aic": aic_val,
                "aicc": aicc_val,
                "bic": bic_val,
                "model_str": model_str,
                "n_train": len(y),
            },
            fit_status="converged",
        )
    except Exception as exc:
        return _failed(_name, exc)
