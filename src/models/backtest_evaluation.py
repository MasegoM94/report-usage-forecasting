"""Fold-level model evaluation using rolling-origin backtesting.

Public API
----------
BacktestConfig
    Dataclass holding the parameters that control split generation and metric
    computation.  Defaults wire to ``src/config/forecasting.py`` so callers
    that accept portfolio defaults need zero configuration.

evaluate_models_across_folds(report_id, series, model_registry, backtest_config)
    Evaluate every model in *model_registry* on every fold produced by
    ``generate_rolling_splits`` and return two tidy DataFrames:

    * **predictions** — one row per (fold, model, forecast_date)
    * **fold_metrics** — one row per (fold, model)

evaluate_candidates_across_folds(report_id, series, backtest_config, ...)
    Fold-aware variant: for each fold, calls ``profile_seasonality`` on the
    training series to determine which seasonal periods to evaluate, then
    builds a per-fold candidate list that includes all non-seasonal baselines
    plus the shortlisted seasonal models.  Returns the same pair of
    DataFrames as ``evaluate_models_across_folds`` but with additional
    seasonality-metadata columns.

Design rules
------------
* Every model is run on the same folds.
* MASE uses only the fold's own training series as its denominator baseline.
* Failed model-folds are preserved as explicit rows (never silently dropped).
* A failure in one (model, fold) pair does not skip any other pair.
* Forecast and actual indices are verified to align exactly; a misalignment
  is recorded as ``fit_status='misaligned'`` rather than silently dropping rows.
* Outputs are sorted deterministically before returning.
* No model selection is performed here.
* Seasonal-period candidates are derived exclusively from the fold training
  series; test-fold observations are never read during candidate profiling.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass, field
from typing import Callable, Optional

import numpy as np
import pandas as pd

from src.config.forecasting import (
    BACKTEST_FOLDS,
    BACKTEST_STEP_DAYS,
    FORECAST_HORIZON_DAYS,
    MIN_TRAIN_DAYS,
    NON_SEASONAL_PERIOD,
    SEASONAL_CANDIDATES,
)
from src.models.backtesting import ForecastFold, generate_rolling_splits
from src.models.candidates import (
    ModelResult,
    forecast_auto_arima,
    forecast_ets,
    forecast_moving_average,
    forecast_naive,
    forecast_seasonal_naive,
)
from src.models.metrics import calculate_interval_metrics, calculate_point_metrics
from src.models.seasonality import profile_seasonality


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class BacktestConfig:
    """Parameters controlling fold generation and metric computation.

    Attributes
    ----------
    horizon:
        Number of test observations per fold.  Matches ``FORECAST_HORIZON_DAYS``
        by default (28 days).
    n_folds:
        Requested number of rolling-origin folds.  Actual folds produced may
        be fewer if the series is too short.
    step:
        Days between successive cutoff dates (stride of the rolling window).
    min_train_size:
        Minimum training-window size required for the first fold.
    seasonal_period:
        Lag used for the MASE denominator.  Must match the seasonality
        assumption used when training the candidate models.
    """
    horizon: int = FORECAST_HORIZON_DAYS
    n_folds: int = BACKTEST_FOLDS
    step: int = BACKTEST_STEP_DAYS
    min_train_size: int = MIN_TRAIN_DAYS
    seasonal_period: int = SEASONAL_CANDIDATES[0]


# ---------------------------------------------------------------------------
# Column name constants
# ---------------------------------------------------------------------------

# Prediction-level output columns (in output order)
_PRED_COLS = [
    "report_id", "fold_number", "cutoff_date",
    "train_start", "train_end",
    "forecast_date", "horizon_step",
    "model_name", "actual", "forecast", "lower_bound", "upper_bound",
    "fit_status",
]

# Fold-level metric output columns (in output order)
_METRIC_COLS = [
    "report_id", "fold_number", "cutoff_date",
    "model_name",
    "mae", "rmse", "wape", "mase_lag1", "mase_m", "bias",
    "interval_coverage", "mean_interval_width",
    "fit_status", "error_message",
]

# ---------------------------------------------------------------------------
# Extended column lists for evaluate_candidates_across_folds
# ---------------------------------------------------------------------------

# Columns added by fold-specific seasonal profiling (same in both output tables)
_CANDIDATE_EXTRA_COLS = [
    "model_family",
    "candidate_m",
    "seasonal_candidate_rank",
    "cycles_available",
    "autocorrelation_at_m",
    "spectral_power_at_m",
    "seasonality_status",
    "candidate_source",
]

_PRED_COLS_EXT = [
    "report_id", "fold_number", "cutoff_date", "train_start", "train_end",
    "forecast_date", "horizon_step",
    "model_name", "model_family", "candidate_m",
    "seasonal_candidate_rank", "cycles_available",
    "autocorrelation_at_m", "spectral_power_at_m",
    "seasonality_status", "candidate_source",
    "actual", "forecast", "lower_bound", "upper_bound", "fit_status",
    "nominal_coverage",   # 0.95 for all candidates (alpha=0.05 hardcoded in forecast functions)
]

_METRIC_COLS_EXT = [
    "report_id", "fold_number", "cutoff_date",
    "model_name", "model_family", "candidate_m",
    "seasonal_candidate_rank", "cycles_available",
    "autocorrelation_at_m", "spectral_power_at_m",
    "seasonality_status", "candidate_source",
    "mae", "rmse", "wape", "mase_lag1", "mase_m", "bias",
    "interval_coverage", "mean_interval_width",
    "fit_status", "error_message",
]


# ---------------------------------------------------------------------------
# Candidate specification (internal to evaluate_candidates_across_folds)
# ---------------------------------------------------------------------------

@dataclass
class _CandidateSpec:
    """One model-period pair ready to be evaluated on a fold.

    Attributes
    ----------
    model_name:
        Stable identifier carried into the output DataFrames, e.g.
        ``"seasonal_naive_m7"`` or ``"auto_arima_m1"``.
    model_fn:
        Callable with signature ``(training_series, horizon) -> ModelResult``.
        The seasonal period (if any) is already bound via ``functools.partial``.
    model_family:
        Coarse model family label: ``"naive"``, ``"moving_average"``,
        ``"seasonal_naive"``, ``"auto_arima"``, or ``"ets"``.
    candidate_m:
        Seasonal period for this candidate.  Always ``NON_SEASONAL_PERIOD``
        (= 1) for non-seasonal models.
    seasonal_candidate_rank:
        Position in the profiler's ranked list of seasonal candidates
        (1 = highest-scoring, 2 = second, …).  0 for non-seasonal candidates.
    cycles_available:
        ``floor(n_train / candidate_m)`` as reported by the profiler.
        For non-seasonal candidates (m=1) this equals ``n_train``.
    autocorrelation_at_m:
        Lag-m autocorrelation reported by the profiler, or NaN for m=1.
    spectral_power_at_m:
        Spectral power at frequency 1/m reported by the profiler, or NaN
        for m=1.
    seasonality_status:
        ``profile.seasonality_status`` for this fold's training series.
    candidate_source:
        ``"baseline"`` for models always evaluated (naive, moving_average,
        arima_m1, ets_m1); ``"seasonality_profiler"`` for models whose m
        was shortlisted by the profiler.
    """
    model_name: str
    model_fn: Callable
    model_family: str
    candidate_m: int
    seasonal_candidate_rank: int
    cycles_available: int
    autocorrelation_at_m: float
    spectral_power_at_m: float
    seasonality_status: str
    candidate_source: str


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_failed_result(model_name: str, exc: Exception) -> ModelResult:
    """Wrap an unexpected exception from a non-candidates model function."""
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


def _build_prediction_rows(
    report_id: str,
    fold: ForecastFold,
    result: ModelResult,
) -> list[dict]:
    """Return one prediction-level dict per horizon step.

    For successful models the forecast and actual values are date-aligned.
    For failed models, actuals are taken directly from the test fold and
    forecast/bound fields are NaN.
    """
    test = fold.test_series
    horizon = len(test)
    horizon_steps = list(range(1, horizon + 1))
    base = {
        "report_id": report_id,
        "fold_number": fold.fold_number,
        "cutoff_date": fold.cutoff_date,
        "train_start": fold.train_start,
        "train_end": fold.train_end,
    }

    if result.fit_status == "failed":
        return [
            {
                **base,
                "forecast_date": date,
                "horizon_step": step,
                "model_name": result.model_name,
                "actual": float(test.iloc[i]),
                "forecast": np.nan,
                "lower_bound": np.nan,
                "upper_bound": np.nan,
                "fit_status": "failed",
                "nominal_coverage": 0.95,
            }
            for i, (date, step) in enumerate(zip(test.index, horizon_steps))
        ]

    # Verify date alignment between forecast and test series
    if not result.forecast.index.equals(test.index):
        # Attempt reindex to test dates; missing positions become NaN
        fc_aligned = result.forecast.reindex(test.index)
        lo_aligned = result.lower_bound.reindex(test.index) if result.lower_bound is not None else None
        hi_aligned = result.upper_bound.reindex(test.index) if result.upper_bound is not None else None
        fit_status = "misaligned"
    else:
        fc_aligned = result.forecast
        lo_aligned = result.lower_bound
        hi_aligned = result.upper_bound
        fit_status = result.fit_status

    rows = []
    for i, (date, step) in enumerate(zip(test.index, horizon_steps)):
        rows.append({
            **base,
            "forecast_date": date,
            "horizon_step": step,
            "model_name": result.model_name,
            "actual": float(test.iloc[i]),
            "forecast": float(fc_aligned.iloc[i]) if pd.notna(fc_aligned.iloc[i]) else np.nan,
            "lower_bound": float(lo_aligned.iloc[i]) if lo_aligned is not None and pd.notna(lo_aligned.iloc[i]) else np.nan,
            "upper_bound": float(hi_aligned.iloc[i]) if hi_aligned is not None and pd.notna(hi_aligned.iloc[i]) else np.nan,
            "fit_status": fit_status,
            "nominal_coverage": 0.95,
        })
    return rows


def _build_metric_row(
    report_id: str,
    fold: ForecastFold,
    result: ModelResult,
    predictions: list[dict],
    candidate_seasonal_period: Optional[int] = None,
) -> dict:
    """Return one fold-level metric dict for a single (fold, model) pair.

    ``mase_lag1`` uses a lag-1 random-walk denominator computed exclusively
    from the fold's training series.  All candidates within the same fold share
    this denominator, so their scores are directly comparable.

    ``mase_m`` uses ``candidate_seasonal_period`` as the denominator and is
    a per-candidate diagnostic; pass ``None`` to omit it.

    Interval metrics are only computed when both bounds are present and the
    model did not fail.
    """
    base = {
        "report_id": report_id,
        "fold_number": fold.fold_number,
        "cutoff_date": fold.cutoff_date,
        "model_name": result.model_name,
        "error_message": result.error_message,
    }

    if result.fit_status == "failed":
        return {
            **base,
            "mae": np.nan, "rmse": np.nan, "wape": np.nan,
            "mase_lag1": np.nan, "mase_m": np.nan, "bias": np.nan,
            "interval_coverage": np.nan, "mean_interval_width": np.nan,
            "fit_status": "failed",
        }

    # Extract aligned actual and forecast arrays from the prediction rows
    # (guarantees metric computation uses the same aligned values shown to users)
    actual_arr = np.array([r["actual"] for r in predictions])
    forecast_arr = np.array([r["forecast"] for r in predictions])

    pt = calculate_point_metrics(
        actual_arr,
        forecast_arr,
        training_series=fold.train_series.to_numpy(),
        candidate_seasonal_period=candidate_seasonal_period,
    )

    # Interval metrics only when the model produced bounds
    has_bounds = (
        result.lower_bound is not None
        and result.upper_bound is not None
        and result.fit_status != "failed"
    )
    if has_bounds:
        lo_arr = np.array([r["lower_bound"] for r in predictions])
        hi_arr = np.array([r["upper_bound"] for r in predictions])
        iv = calculate_interval_metrics(actual_arr, lo_arr, hi_arr)
        interval_coverage = iv["interval_coverage"]
        mean_interval_width = iv["mean_interval_width"]
    else:
        interval_coverage = np.nan
        mean_interval_width = np.nan

    return {
        **base,
        "mae": pt["mae"],
        "rmse": pt["rmse"],
        "wape": pt["wape"],
        "mase_lag1": pt["mase_lag1"],
        "mase_m": pt["mase_m"],
        "bias": pt["bias"],
        "interval_coverage": interval_coverage,
        "mean_interval_width": mean_interval_width,
        "fit_status": result.fit_status,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def evaluate_models_across_folds(
    report_id: str,
    series: pd.Series,
    model_registry: dict[str, Callable],
    backtest_config: Optional[BacktestConfig] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Evaluate every model in *model_registry* on every rolling-origin fold.

    Parameters
    ----------
    report_id:
        Identifier carried through to both output DataFrames so results from
        different reports can be concatenated and filtered.
    series:
        Continuous daily ``pd.Series`` with a sorted ``DatetimeIndex``.
        Passed directly to ``generate_rolling_splits``; all continuity and
        sort constraints from that function apply.
    model_registry:
        Mapping of ``model_name → callable``.  Each callable must accept
        ``(training_series: pd.Series, horizon: int)`` and return a
        ``ModelResult``.  Non-``ModelResult`` returns and any raised exceptions
        are caught and recorded as failed rows without aborting other models.
    backtest_config:
        Controls fold generation and metric computation.  Defaults to
        ``BacktestConfig()`` which uses portfolio-wide constants from
        ``src/config/forecasting.py``.

    Returns
    -------
    predictions : pd.DataFrame
        One row per (fold, model, forecast_date).  Columns: ``report_id``,
        ``fold_number``, ``cutoff_date``, ``train_start``, ``train_end``,
        ``forecast_date``, ``horizon_step``, ``model_name``, ``actual``,
        ``forecast``, ``lower_bound``, ``upper_bound``, ``fit_status``.
        Sorted by (report_id, fold_number, model_name, horizon_step).

    fold_metrics : pd.DataFrame
        One row per (fold, model).  Columns: ``report_id``, ``fold_number``,
        ``cutoff_date``, ``model_name``, ``mae``, ``rmse``, ``wape``,
        ``mase``, ``bias``, ``interval_coverage``, ``mean_interval_width``,
        ``fit_status``, ``error_message``.
        Sorted by (report_id, fold_number, model_name).

    Raises
    ------
    ValueError
        Propagated from ``generate_rolling_splits`` when the series is too
        short to produce even one fold.
    """
    if backtest_config is None:
        backtest_config = BacktestConfig()

    cfg = backtest_config

    # Generate folds — raises ValueError if series too short for even one fold
    folds, _split_status = generate_rolling_splits(
        series,
        horizon=cfg.horizon,
        n_folds=cfg.n_folds,
        step=cfg.step,
        min_train_size=cfg.min_train_size,
    )

    all_pred_rows: list[dict] = []
    all_metric_rows: list[dict] = []

    for fold in folds:
        for model_name, model_fn in model_registry.items():
            # Run the model — catch anything that escapes from non-candidates callables
            try:
                result = model_fn(fold.train_series, cfg.horizon)
                if not isinstance(result, ModelResult):
                    raise TypeError(
                        f"model '{model_name}' returned {type(result).__name__}, "
                        "expected ModelResult"
                    )
            except Exception as exc:
                result = _make_failed_result(model_name, exc)

            pred_rows = _build_prediction_rows(report_id, fold, result)
            metric_row = _build_metric_row(
                report_id, fold, result, pred_rows
            )

            all_pred_rows.extend(pred_rows)
            all_metric_rows.append(metric_row)

    # Build DataFrames with guaranteed column order
    if all_pred_rows:
        predictions = pd.DataFrame(all_pred_rows)[_PRED_COLS]
    else:
        predictions = pd.DataFrame(columns=_PRED_COLS)

    if all_metric_rows:
        fold_metrics = pd.DataFrame(all_metric_rows)[_METRIC_COLS]
    else:
        fold_metrics = pd.DataFrame(columns=_METRIC_COLS)

    # Deterministic sort
    predictions = predictions.sort_values(
        ["report_id", "fold_number", "model_name", "horizon_step"],
        ignore_index=True,
    )
    fold_metrics = fold_metrics.sort_values(
        ["report_id", "fold_number", "model_name"],
        ignore_index=True,
    )

    return predictions, fold_metrics


# ---------------------------------------------------------------------------
# Fold-specific candidate builder
# ---------------------------------------------------------------------------

def _build_fold_candidates(
    fold: ForecastFold,
    candidate_periods: tuple[int, ...],
    include_ets: bool,
    include_arima: bool,
) -> list[_CandidateSpec]:
    """Profile *fold.train_series* and return an ordered candidate list.

    The training series is the only input to ``profile_seasonality``; the
    fold's test series is never read.  This guarantees that test-fold
    observations cannot influence which seasonal periods are evaluated.

    The returned list always contains the non-seasonal baselines (naive,
    moving_average, and — when the optional libraries are requested — arima_m1
    and ets_m1), followed by the seasonal candidates shortlisted by the
    profiler in score order.

    Duplicate (model_family, candidate_m) pairs are silently dropped so that
    the same model-period combination is never fitted twice.
    """
    profile = profile_seasonality(fold.train_series, candidate_periods=candidate_periods)
    n_train = len(fold.train_series)
    status = profile.seasonality_status

    specs: list[_CandidateSpec] = []
    seen: set[tuple[str, int]] = set()

    def _add(spec: _CandidateSpec) -> None:
        key = (spec.model_family, spec.candidate_m)
        if key not in seen:
            seen.add(key)
            specs.append(spec)

    def _ns_meta(family: str) -> dict:
        """Metadata for a non-seasonal (m=1) candidate."""
        return dict(
            model_family=family,
            candidate_m=NON_SEASONAL_PERIOD,
            seasonal_candidate_rank=0,
            cycles_available=n_train,
            autocorrelation_at_m=float("nan"),
            spectral_power_at_m=float("nan"),
            seasonality_status=status,
            candidate_source="baseline",
        )

    # ------------------------------------------------------------------
    # Non-seasonal baselines — always evaluated (requirements 2, 4, 6)
    # ------------------------------------------------------------------
    _add(_CandidateSpec(
        model_name="naive",
        model_fn=forecast_naive,
        **_ns_meta("naive"),
    ))
    _add(_CandidateSpec(
        model_name="moving_average",
        model_fn=forecast_moving_average,
        **_ns_meta("moving_average"),
    ))
    if include_arima:
        _add(_CandidateSpec(
            model_name=f"auto_arima_m{NON_SEASONAL_PERIOD}",
            model_fn=functools.partial(
                forecast_auto_arima, seasonal_period=NON_SEASONAL_PERIOD
            ),
            **_ns_meta("auto_arima"),
        ))
    if include_ets:
        _add(_CandidateSpec(
            model_name=f"ets_m{NON_SEASONAL_PERIOD}",
            model_fn=functools.partial(
                forecast_ets, seasonal_period=NON_SEASONAL_PERIOD
            ),
            **_ns_meta("ets"),
        ))

    # ------------------------------------------------------------------
    # Seasonal candidates from the profiler (requirements 3, 4, 5)
    # selected_candidate_periods[0] is always NON_SEASONAL_PERIOD; skip it.
    # ------------------------------------------------------------------
    seasonal_ms = [
        m for m in profile.selected_candidate_periods
        if m > NON_SEASONAL_PERIOD
    ]
    for rank, m in enumerate(seasonal_ms, start=1):
        s_meta = dict(
            candidate_m=m,
            seasonal_candidate_rank=rank,
            cycles_available=profile.cycles_available_by_period.get(m, 0),
            autocorrelation_at_m=profile.autocorrelation_by_period.get(
                m, float("nan")
            ),
            spectral_power_at_m=profile.spectral_power_by_period.get(
                m, float("nan")
            ),
            seasonality_status=status,
            candidate_source="seasonality_profiler",
        )

        # Seasonal naïve (requirement 3)
        _add(_CandidateSpec(
            model_name=f"seasonal_naive_m{m}",
            model_fn=functools.partial(forecast_seasonal_naive, seasonal_period=m),
            model_family="seasonal_naive",
            **s_meta,
        ))
        # SARIMA (requirement 4)
        if include_arima:
            _add(_CandidateSpec(
                model_name=f"auto_arima_m{m}",
                model_fn=functools.partial(forecast_auto_arima, seasonal_period=m),
                model_family="auto_arima",
                **s_meta,
            ))
        # Seasonal ETS (requirement 5)
        if include_ets:
            _add(_CandidateSpec(
                model_name=f"ets_m{m}",
                model_fn=functools.partial(forecast_ets, seasonal_period=m),
                model_family="ets",
                **s_meta,
            ))

    return specs


# ---------------------------------------------------------------------------
# evaluate_candidates_across_folds — public API
# ---------------------------------------------------------------------------

def evaluate_candidates_across_folds(
    report_id: str,
    series: pd.Series,
    backtest_config: Optional[BacktestConfig] = None,
    candidate_periods: tuple[int, ...] = SEASONAL_CANDIDATES,
    include_ets: bool = True,
    include_arima: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Evaluate fold-specific seasonal candidates on every rolling-origin fold.

    For each fold this function:

    1. Calls ``profile_seasonality(fold.train_series, ...)`` — test
       observations are never read during profiling.
    2. Builds a per-fold candidate list: non-seasonal baselines always; seasonal
       models only for the periods shortlisted by the profiler.
    3. Runs each candidate and records prediction rows and fold-level metrics.
    4. Annotates every row with seasonality-metadata columns (``model_family``,
       ``candidate_m``, ``seasonal_candidate_rank``, etc.).

    ``mase_lag1`` uses a lag-1 random-walk denominator shared by all
    candidates in the same fold, ensuring cross-candidate comparability.
    ``mase_m`` uses each candidate's own period as the denominator and
    is carried as a diagnostic column only.

    Parameters
    ----------
    report_id:
        Identifier carried into both output DataFrames.
    series:
        Continuous daily ``pd.Series`` with a sorted ``DatetimeIndex``.
    backtest_config:
        Controls fold generation.  Defaults to ``BacktestConfig()``.
        ``BacktestConfig.seasonal_period`` is not used here; each candidate
        supplies its own m.
    candidate_periods:
        Tuple of candidate periods passed to the profiler on every fold.
        Default: ``SEASONAL_CANDIDATES``.
    include_ets:
        When ``True`` (default), ETS candidates (ets_m1 and ets_m{m} for
        each seasonal m) are added to the evaluation.  Set ``False`` to skip
        ETS when statsmodels is unavailable.
    include_arima:
        When ``True`` (default), Auto-ARIMA candidates (auto_arima_m1 and
        auto_arima_m{m}) are added.  Set ``False`` to skip when pmdarima is
        unavailable.

    Returns
    -------
    predictions : pd.DataFrame
        One row per (fold, candidate, forecast_date).
        Columns: all columns in ``evaluate_models_across_folds`` plus
        ``model_family``, ``candidate_m``, ``seasonal_candidate_rank``,
        ``cycles_available``, ``autocorrelation_at_m``,
        ``spectral_power_at_m``, ``seasonality_status``,
        ``candidate_source``.
        Sorted by (report_id, fold_number, model_name, horizon_step).

    fold_metrics : pd.DataFrame
        One row per (fold, candidate).
        Same extended columns as *predictions* plus all metric columns from
        ``evaluate_models_across_folds``.
        Sorted by (report_id, fold_number, model_name).

    training_residual_records : list[dict]
        One dict per (fold, candidate) containing the training-residual
        arrays extracted from ``ModelResult``.  Suitable for passing to
        ``build_training_residual_dataset`` in
        ``src.models.residual_datasets``.  The dicts carry:
        ``report_id``, ``fold_number``, ``training_start``,
        ``training_cutoff``, ``model_name``, ``model_family``,
        ``candidate_m``, ``fit_status``, and all residual-extraction
        fields from ``ModelResult`` (arrays already computed; no fitted
        model object is retained).

    Raises
    ------
    ValueError
        Propagated from ``generate_rolling_splits`` when the series is too
        short to produce even one fold.
    """
    if backtest_config is None:
        backtest_config = BacktestConfig()
    cfg = backtest_config

    folds, _ = generate_rolling_splits(
        series,
        horizon=cfg.horizon,
        n_folds=cfg.n_folds,
        step=cfg.step,
        min_train_size=cfg.min_train_size,
    )

    all_pred_rows: list[dict] = []
    all_metric_rows: list[dict] = []
    all_tr_records: list[dict] = []

    for fold in folds:
        candidates = _build_fold_candidates(
            fold, candidate_periods, include_ets, include_arima
        )

        for spec in candidates:
            extra = {
                "model_family": spec.model_family,
                "candidate_m": spec.candidate_m,
                "seasonal_candidate_rank": spec.seasonal_candidate_rank,
                "cycles_available": spec.cycles_available,
                "autocorrelation_at_m": spec.autocorrelation_at_m,
                "spectral_power_at_m": spec.spectral_power_at_m,
                "seasonality_status": spec.seasonality_status,
                "candidate_source": spec.candidate_source,
            }

            try:
                result = spec.model_fn(fold.train_series, cfg.horizon)
                if not isinstance(result, ModelResult):
                    raise TypeError(
                        f"model '{spec.model_name}' returned "
                        f"{type(result).__name__}, expected ModelResult"
                    )
            except Exception as exc:
                result = _make_failed_result(spec.model_name, exc)

            # Pass candidate_m so mase_m (diagnostic) is computed for each
            # candidate.  mase_lag1 always uses lag-1 regardless of this value.
            pred_rows = _build_prediction_rows(report_id, fold, result)
            metric_row = _build_metric_row(
                report_id, fold, result, pred_rows,
                candidate_seasonal_period=spec.candidate_m,
            )

            for row in pred_rows:
                row.update(extra)
            metric_row.update(extra)

            all_pred_rows.extend(pred_rows)
            all_metric_rows.append(metric_row)

            # Collect training-residual fields from ModelResult for diagnostic use.
            # No model object is retained — arrays were already extracted in the
            # candidate function before the model was released.
            train_start = (
                fold.train_series.index[0]
                if not fold.train_series.empty
                else None
            )
            all_tr_records.append({
                "report_id": report_id,
                "fold_number": fold.fold_number,
                "training_start": train_start,
                "training_cutoff": fold.cutoff_date,
                "model_name": result.model_name,
                "model_family": spec.model_family,
                "candidate_m": spec.candidate_m,
                "fit_status": result.fit_status,
                "training_actual": result.training_actual,
                "training_fitted": result.training_fitted,
                "training_residuals": result.training_residuals,
                "training_residual_dates": result.training_residual_dates,
                "residual_extraction_status": result.residual_extraction_status,
                "residual_extraction_reason": result.residual_extraction_reason,
                "training_observation_count": result.training_observation_count,
                "fitted_observation_count": result.fitted_observation_count,
                "residual_observation_count": result.residual_observation_count,
            })

    if all_pred_rows:
        predictions = pd.DataFrame(all_pred_rows)[_PRED_COLS_EXT]
    else:
        predictions = pd.DataFrame(columns=_PRED_COLS_EXT)

    if all_metric_rows:
        fold_metrics = pd.DataFrame(all_metric_rows)[_METRIC_COLS_EXT]
    else:
        fold_metrics = pd.DataFrame(columns=_METRIC_COLS_EXT)

    predictions = predictions.sort_values(
        ["report_id", "fold_number", "model_name", "horizon_step"],
        ignore_index=True,
    )
    fold_metrics = fold_metrics.sort_values(
        ["report_id", "fold_number", "model_name"],
        ignore_index=True,
    )

    return predictions, fold_metrics, all_tr_records
