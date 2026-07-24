"""Persist prediction-level rolling-backtest outputs.

Purpose
-------
``outputs/metrics/backtest_predictions_latest.csv`` is the canonical
prediction-level record of every (report, fold, candidate, horizon_step)
evaluation attempt.  It serves as:

* a diagnostic and audit dataset — each row can be inspected without
  re-running the expensive backtest
* the source for residual analysis, outlier detection, and distribution
  diagnostics (future work — not computed here)
* an audit trail for model-lineage questions ("which candidates were tried
  for report R on fold 3?")

This module adds six derived fields to the predictions DataFrame produced by
``evaluate_candidates_across_folds`` and writes the result to disk.  It does
not re-run any model evaluation.

Schema
------
The output extends ``_PRED_COLS_EXT`` from ``backtest_evaluation.py`` with an
optional ``evaluation_run_id`` column (when the caller supplies one) and six
derived diagnostic columns.

Identity / lineage (carried through unchanged)
    evaluation_run_id       — pipeline run ID (null when not supplied)
    report_id
    fold_number
    cutoff_date
    train_start
    train_end
    forecast_date
    horizon_step
    model_name
    model_family
    candidate_m
    seasonal_candidate_rank
    cycles_available
    autocorrelation_at_m
    spectral_power_at_m
    seasonality_status
    candidate_source

Raw prediction values
    actual
    forecast
    lower_bound
    upper_bound
    fit_status

Derived diagnostic fields (added here)
    residual        = actual - forecast   (actual minus forecast; NaN for failed rows)
    signed_error    = forecast - actual   (forecast minus actual; consistent with
                                           realized_forecast_history.signed_error)
    absolute_error  = |forecast - actual| (NaN for failed rows)
    squared_error   = (forecast - actual)²(NaN for failed rows)
    inside_interval = True when lower_bound ≤ actual ≤ upper_bound; NaN when
                      bounds are absent; always NaN for failed rows
    interval_width  = upper_bound - lower_bound; NaN when bounds are absent;
                      always NaN for failed rows

Sign conventions
----------------
``residual = actual - forecast``        (positive = under-forecast)
``signed_error = forecast - actual``    (positive = over-forecast)

These two are always opposite in sign.  Both are included so callers can use
whichever convention is natural for their analysis without silent sign errors.
Only rows with ``fit_status != "failed"`` have non-NaN error values; failed
rows preserve actuals but leave all error fields NaN to prevent failed
predictions from contaminating residual distributions.

Uniqueness key
--------------
(report_id, fold_number, model_name, horizon_step) must be unique.  The
function validates this before writing.

History convention
------------------
No append-only history file is created.  The existing repository pattern
reserves append-only CSVs for realized production outputs (``realized_errors_history.csv``,
``realized_forecast_history.csv``, ``production_forecasts_history.csv``).
Backtest predictions are recomputed from scratch on every evaluation run
and should be treated as a cache that is always overwritten, not an audit log.

Public API
----------
BACKTEST_PREDICTIONS_COLS : list[str]
    Canonical output column order.

add_diagnostic_fields(df) -> pd.DataFrame
    Adds the six derived fields to a copy of the predictions DataFrame.
    Accepts the raw ``_PRED_COLS_EXT`` output of
    ``evaluate_candidates_across_folds``; also accepts rows that already carry
    ``evaluation_run_id``.

validate_backtest_predictions(df) -> None
    Raises ``ValueError`` on schema or uniqueness violations.

save_backtest_predictions(df, project_root, evaluation_run_id=None) -> Path | None
    Full write path: add fields → validate → write CSV → return path.
    Returns ``None`` when *df* is empty.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

# Required identity / lineage columns that must already exist in the input
_IDENTITY_COLS: list[str] = [
    "report_id",
    "fold_number",
    "cutoff_date",
    "train_start",
    "train_end",
    "forecast_date",
    "horizon_step",
    "model_name",
    "model_family",
    "candidate_m",
    "seasonal_candidate_rank",
    "cycles_available",
    "autocorrelation_at_m",
    "spectral_power_at_m",
    "seasonality_status",
    "candidate_source",
    "actual",
    "forecast",
    "lower_bound",
    "upper_bound",
    "fit_status",
]

# Derived fields appended by this module
_DERIVED_COLS: list[str] = [
    "residual",
    "signed_error",
    "absolute_error",
    "squared_error",
    "inside_interval",
    "interval_width",
]

# Full output column order
BACKTEST_PREDICTIONS_COLS: list[str] = (
    ["evaluation_run_id"] + _IDENTITY_COLS + _DERIVED_COLS
)

# Minimum required input columns (evaluation_run_id is optional at input time)
_REQUIRED_INPUT_COLS: set[str] = set(_IDENTITY_COLS)

# Unique key
_UNIQUE_KEY: list[str] = [
    "report_id", "fold_number", "model_name", "horizon_step",
]

# Valid horizon_step range
_MIN_HORIZON_STEP = 1
_MAX_HORIZON_STEP = 28


# ---------------------------------------------------------------------------
# Field derivation
# ---------------------------------------------------------------------------

def add_diagnostic_fields(
    df: pd.DataFrame,
    evaluation_run_id: Optional[str] = None,
) -> pd.DataFrame:
    """Add derived diagnostic columns to the backtest predictions DataFrame.

    Parameters
    ----------
    df:
        Predictions DataFrame from ``evaluate_candidates_across_folds``.
        Must contain all columns in ``_REQUIRED_INPUT_COLS``.
    evaluation_run_id:
        Pipeline run ID to stamp into every row.  When ``None``, the column
        is set to ``None`` (null) rather than being omitted, so the schema
        remains stable across calls.

    Returns
    -------
    A copy of *df* with ``evaluation_run_id`` prepended and six derived
    columns appended, in ``BACKTEST_PREDICTIONS_COLS`` order.

    Error-field treatment
    ---------------------
    All six derived fields are set to ``NaN`` when ``fit_status == "failed"``.
    This prevents failed rows from polluting residual distributions while
    still preserving the row's identity and actual value for audit purposes.

    Interval fields (``inside_interval``, ``interval_width``) are additionally
    set to ``NaN`` when either ``lower_bound`` or ``upper_bound`` is ``NaN``,
    regardless of ``fit_status``.
    """
    _validate_input_cols(df)

    out = df.copy()

    # Stamp run ID (always present in the output, null when not supplied)
    if "evaluation_run_id" not in out.columns:
        out.insert(0, "evaluation_run_id", evaluation_run_id)
    elif evaluation_run_id is not None:
        out["evaluation_run_id"] = evaluation_run_id

    actual   = pd.to_numeric(out["actual"],   errors="coerce")
    forecast = pd.to_numeric(out["forecast"], errors="coerce")
    lo       = pd.to_numeric(out["lower_bound"], errors="coerce")
    hi       = pd.to_numeric(out["upper_bound"], errors="coerce")

    failed_mask = out["fit_status"] == "failed"

    # residual = actual - forecast  (NaN for failed)
    residual = actual - forecast
    residual[failed_mask] = np.nan
    out["residual"] = residual

    # signed_error = forecast - actual  (NaN for failed)
    signed_error = forecast - actual
    signed_error[failed_mask] = np.nan
    out["signed_error"] = signed_error

    # absolute_error = |forecast - actual|  (NaN for failed)
    absolute_error = (forecast - actual).abs()
    absolute_error[failed_mask] = np.nan
    out["absolute_error"] = absolute_error

    # squared_error = (forecast - actual)²  (NaN for failed)
    squared_error = (forecast - actual) ** 2
    squared_error[failed_mask] = np.nan
    out["squared_error"] = squared_error

    # inside_interval = lower ≤ actual ≤ upper  (NaN when bounds absent or failed)
    has_bounds = lo.notna() & hi.notna()
    inside = pd.Series(np.where(
        has_bounds & ~failed_mask,
        (actual >= lo) & (actual <= hi),
        np.nan,
    ), index=out.index)
    out["inside_interval"] = inside

    # interval_width = upper - lower  (NaN when bounds absent or failed)
    width = hi - lo
    width[~has_bounds | failed_mask] = np.nan
    out["interval_width"] = width

    # Project to canonical column order (add missing optional columns as null)
    for col in BACKTEST_PREDICTIONS_COLS:
        if col not in out.columns:
            out[col] = np.nan

    return out[BACKTEST_PREDICTIONS_COLS]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_backtest_predictions(df: pd.DataFrame) -> None:
    """Raise ``ValueError`` when the DataFrame violates schema invariants.

    Checks
    ------
    1. All required output columns are present.
    2. ``horizon_step`` values are in [1, 28].
    3. Unique key (report_id, fold_number, model_name, horizon_step).
    4. Sign consistency: ``residual == -(signed_error)`` for non-NaN rows.
    5. ``absolute_error >= 0`` for all non-NaN rows.
    """
    # 1. Required columns
    missing = set(BACKTEST_PREDICTIONS_COLS) - set(df.columns)
    if missing:
        raise ValueError(
            f"backtest_predictions is missing required column(s): {sorted(missing)}"
        )

    if df.empty:
        return

    # 2. Horizon step range
    steps = pd.to_numeric(df["horizon_step"], errors="coerce").dropna()
    if steps.empty:
        raise ValueError("backtest_predictions has no valid horizon_step values.")
    out_of_range = steps[(steps < _MIN_HORIZON_STEP) | (steps > _MAX_HORIZON_STEP)]
    if not out_of_range.empty:
        raise ValueError(
            f"backtest_predictions has {len(out_of_range)} row(s) with "
            f"horizon_step outside [{_MIN_HORIZON_STEP}, {_MAX_HORIZON_STEP}]: "
            f"{sorted(out_of_range.unique().tolist())[:5]}"
        )

    # 3. Uniqueness
    dupes = df.duplicated(subset=_UNIQUE_KEY, keep=False)
    if dupes.any():
        n = int(dupes.sum())
        raise ValueError(
            f"backtest_predictions has {n} duplicate row(s) on key {_UNIQUE_KEY}."
        )

    # 4. Sign consistency: residual + signed_error ≈ 0 for non-NaN rows
    res    = pd.to_numeric(df["residual"],     errors="coerce")
    se     = pd.to_numeric(df["signed_error"], errors="coerce")
    both   = res.notna() & se.notna()
    if both.any():
        discrepancy = (res[both] + se[both]).abs()
        if (discrepancy > 1e-9).any():
            n_bad = int((discrepancy > 1e-9).sum())
            raise ValueError(
                f"backtest_predictions has {n_bad} row(s) where "
                f"residual + signed_error ≠ 0 (sign inconsistency)."
            )

    # 5. Absolute error non-negative
    ae = pd.to_numeric(df["absolute_error"], errors="coerce").dropna()
    if (ae < 0).any():
        raise ValueError(
            "backtest_predictions has row(s) with negative absolute_error."
        )


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def save_backtest_predictions(
    df: pd.DataFrame,
    project_root: Path,
    evaluation_run_id: Optional[str] = None,
) -> Optional[Path]:
    """Add diagnostic fields, validate, and write to disk.

    Writes ``outputs/metrics/backtest_predictions_latest.csv`` (overwrite).
    No append-only history file is created — see module docstring.

    Parameters
    ----------
    df:
        Predictions DataFrame from ``evaluate_candidates_across_folds``.
    project_root:
        Repository root.
    evaluation_run_id:
        Pipeline run ID stamped into every row.  Pass the same ``run_id``
        used elsewhere in the pipeline so predictions can be cross-referenced
        with production forecasts and monitoring tables.

    Returns
    -------
    Absolute path to the written file, or ``None`` when *df* is empty.
    """
    if df is None or df.empty:
        return None

    out = add_diagnostic_fields(df, evaluation_run_id=evaluation_run_id)

    # Deterministic sort: (report_id, fold_number, model_name, horizon_step)
    out = out.sort_values(
        ["report_id", "fold_number", "model_name", "horizon_step"],
        ignore_index=True,
    )

    validate_backtest_predictions(out)

    metrics_dir = project_root / "outputs" / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    path = metrics_dir / "backtest_predictions_latest.csv"
    out.to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _validate_input_cols(df: pd.DataFrame) -> None:
    missing = _REQUIRED_INPUT_COLS - set(df.columns)
    if missing:
        raise ValueError(
            f"backtest predictions input is missing required column(s): "
            f"{sorted(missing)}"
        )
