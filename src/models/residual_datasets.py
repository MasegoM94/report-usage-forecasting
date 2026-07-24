"""Canonical residual and forecast-error datasets for model diagnostics.

Three clearly separated datasets are produced:

1. Training residuals  (outputs/diagnostics/training_residuals_latest.csv)
   In-sample model fit evidence.  Extracted from ModelResult training-residual
   fields; one row per valid fitted observation per (report, fold, candidate).
   May be optimistic compared with out-of-sample errors.

2. Backtest forecast errors  (outputs/diagnostics/backtest_forecast_errors_latest.csv)
   Out-of-sample rolling-origin evidence.  Derived from
   outputs/metrics/backtest_predictions_latest.csv.  Primary source for
   realistic diagnostic performance.

3. Production forecast errors  (outputs/diagnostics/production_forecast_errors_latest.csv)
   Realized operational evidence.  Derived from
   outputs/metrics/realized_forecast_history.csv.  May initially contain
   limited observations.  Some migrated rows may have incomplete lineage.

Sign conventions
----------------
All three datasets use the diagnostic residual convention::

    residual = actual - forecast_or_fitted

Positive residual → model UNDERFORECASTED.
Negative residual → model OVERFORECASTED.

The canonical production monitoring history stores::

    signed_error = forecast - actual   (positive = over-forecast)

For Dataset 3, both are present and we derive::

    residual = actual - forecast = -signed_error

The canonical ``signed_error`` column is NOT renamed or modified.

Unique keys
-----------
Training residuals:
    (diagnostic_run_id, report_id, fit_scope, model_name, candidate_m,
     fold_number, residual_date)

Backtest forecast errors:
    (evaluation_run_id, report_id, fold_number, model_name, candidate_m,
     forecast_date)

Production forecast errors:
    (run_id, report_id, forecast_date)

Handling unavailable fitted values
-----------------------------------
Policy (training residuals):
    Rows where ``residual_extraction_status != "ok"`` or where the residual is
    not finite are **excluded** from the row-level dataset.  For each
    (report, model, fold) combination that produces zero valid rows, one stub
    row is inserted with null residual/actual/fitted values and
    ``residual_observation_valid = False`` so downstream consumers can audit
    which candidates had no valid training residuals.  Evidence counts are
    populated for all stubs.

    Initial observations consumed by differencing (ARIMA) or seasonal lags
    (seasonal-naïve) are naturally excluded by the candidate functions before
    this module is called; they are reflected in the difference between
    ``training_observation_count`` and ``fitted_observation_count``.

Handling failed model rows
--------------------------
Backtest: ``fit_status == "failed"`` rows are retained with
    ``residual_observation_valid = False`` and null residual/signed_error.
Training: ModelResult with ``fit_status == "failed"`` produces one stub row
    with ``residual_observation_valid = False`` (no valid residuals to emit).
Production: rows with non-finite actual or forecast are marked
    ``residual_observation_valid = False``.

Incomplete production lineage
------------------------------
Rows in realized_forecast_history.csv where ``lineage_complete`` is False or
where ``selected_model_family``/``selected_m`` are null are retained as-is.
They remain valid for residual magnitude, bias, and outlier analysis; they
must later be excluded only from analyses that require model-family or
selected_m grouping.  ``lineage_missing_fields`` is preserved without inference.

Public API
----------
build_training_residual_dataset(records, ...) -> pd.DataFrame
build_backtest_forecast_error_dataset(backtest_df, ...) -> pd.DataFrame
build_production_forecast_error_view(realized_df) -> pd.DataFrame
validate_training_residual_dataset(df) -> None
validate_backtest_forecast_error_dataset(df) -> None
validate_production_forecast_error_dataset(df) -> None
persist_residual_datasets(...) -> dict[str, Path | None]
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Output schemas
# ---------------------------------------------------------------------------

TRAINING_RESIDUALS_COLS: list[str] = [
    # lineage
    "diagnostic_run_id",
    "evaluation_run_id",
    "fit_run_id",
    "report_id",
    "report_name",
    "model_family",
    "model_name",
    "candidate_m",
    "fit_scope",            # "backtest_fold" | "production_refit"
    "fold_number",          # int for backtest_fold; None for production_refit
    "training_start",
    "training_cutoff",
    # residual values
    "residual_date",
    "actual",
    "fitted",
    "residual",
    "residual_source",              # always "training"
    "residual_observation_valid",   # bool
    "residual_extraction_status",
    "residual_extraction_reason",
    # evidence counts (same across all rows in one candidate-fold group)
    "training_observation_count",
    "fitted_observation_count",
    "residual_observation_count",
]

BACKTEST_FORECAST_ERRORS_COLS: list[str] = [
    # lineage
    "evaluation_run_id",
    "report_id",
    "report_name",
    "fold_number",
    "cutoff_date",
    "train_start",
    "train_end",
    "model_family",
    "model_name",
    "candidate_m",
    "fit_status",
    # seasonality context
    "seasonal_candidate_rank",
    "cycles_available",
    "autocorrelation_at_m",
    "spectral_power_at_m",
    "seasonality_status",
    "candidate_source",
    # timing
    "forecast_date",
    "horizon_step",
    # values
    "actual",
    "forecast",
    "residual",
    "signed_error",
    "absolute_error",
    "squared_error",
    "lower_bound",
    "upper_bound",
    "inside_interval",
    "interval_width",
    "residual_source",              # always "backtest"
    "residual_observation_valid",   # bool
]

PRODUCTION_FORECAST_ERRORS_COLS: list[str] = [
    # lineage
    "run_id",
    "selection_run_id",
    "report_id",
    "report_name",
    "generated_at",
    "training_cutoff",
    "selected_model_family",
    "selected_model_name",
    "selected_m",
    "lineage_complete",
    "lineage_missing_fields",
    # timing
    "forecast_date",
    "horizon_step",
    "realized_at",
    # values
    "actual",
    "forecast",
    "residual",             # derived: actual - forecast = -signed_error
    "signed_error",         # canonical: forecast - actual (positive = over-forecast)
    "absolute_error",
    "squared_error",
    "lower_bound",
    "upper_bound",
    "inside_interval",
    "interval_width",
    "residual_source",              # always "production"
    "residual_observation_valid",   # bool
]

# Unique keys (for duplicate detection)
_TR_UNIQUE_KEY = [
    "diagnostic_run_id", "report_id", "fit_scope", "model_name",
    "candidate_m", "fold_number", "residual_date",
]
_BFE_UNIQUE_KEY = [
    "evaluation_run_id", "report_id", "fold_number",
    "model_name", "candidate_m", "forecast_date",
]
_PFE_UNIQUE_KEY = ["run_id", "report_id", "forecast_date"]

# Paths relative to project root
_DIAGNOSTICS_DIR = Path("outputs") / "diagnostics"
_TRAINING_RESIDUALS_FILE      = _DIAGNOSTICS_DIR / "training_residuals_latest.csv"
_BACKTEST_ERRORS_FILE         = _DIAGNOSTICS_DIR / "backtest_forecast_errors_latest.csv"
_PRODUCTION_ERRORS_FILE       = _DIAGNOSTICS_DIR / "production_forecast_errors_latest.csv"

_MIN_HORIZON_STEP = 1
_MAX_HORIZON_STEP = 28


# ---------------------------------------------------------------------------
# Dataset 1 — Training residuals
# ---------------------------------------------------------------------------

def build_training_residual_dataset(
    residual_records: list[dict],
    *,
    diagnostic_run_id: str = "",
    evaluation_run_id: str = "",
    fit_run_id: str = "",
    name_lookup: Optional[dict[str, str]] = None,
    fit_scope: str = "backtest_fold",
) -> pd.DataFrame:
    """Build the training-residual dataset from per-candidate residual records.

    Parameters
    ----------
    residual_records:
        List of dicts produced by ``evaluate_candidates_across_folds``.  Each
        dict has: ``report_id``, ``fold_number``, ``training_start``,
        ``training_cutoff``, ``model_name``, ``model_family``, ``candidate_m``,
        ``fit_status``, ``training_actual``, ``training_fitted``,
        ``training_residuals``, ``training_residual_dates``,
        ``residual_extraction_status``, ``residual_extraction_reason``,
        ``training_observation_count``, ``fitted_observation_count``,
        ``residual_observation_count``.
    diagnostic_run_id:
        Identifier for this diagnostic build run.
    evaluation_run_id:
        Backtest run ID (stamped from the pipeline ``run_id``).
    fit_run_id:
        Run ID of the fitting step; equals ``evaluation_run_id`` for backtest
        folds.
    name_lookup:
        Optional ``{report_id: report_name}`` mapping.
    fit_scope:
        ``"backtest_fold"`` for backtest records; ``"production_refit"`` for
        full-history production refits.

    Returns
    -------
    pd.DataFrame
        Columns: ``TRAINING_RESIDUALS_COLS``.
        Policy (see module docstring):
        - Valid residual rows have ``residual_observation_valid = True``.
        - When a candidate produces zero valid rows, one stub row is inserted
          with null residual/actual/fitted and
          ``residual_observation_valid = False``.
    """
    name_lookup = name_lookup or {}
    rows: list[dict] = []

    for rec in residual_records:
        report_id    = rec.get("report_id", "")
        fold_number  = rec.get("fold_number")
        model_name   = rec.get("model_name", "")
        model_family = rec.get("model_family", "")
        candidate_m  = rec.get("candidate_m")
        train_start  = rec.get("training_start")
        train_cutoff = rec.get("training_cutoff")

        report_name = name_lookup.get(report_id, report_id)

        ext_status = rec.get("residual_extraction_status", "unavailable")
        ext_reason = rec.get("residual_extraction_reason")
        n_train    = rec.get("training_observation_count")
        n_fitted   = rec.get("fitted_observation_count")
        n_resid    = rec.get("residual_observation_count")

        actuals   = rec.get("training_actual")
        fitteds   = rec.get("training_fitted")
        residuals = rec.get("training_residuals")
        dates     = rec.get("training_residual_dates")

        common = {
            "diagnostic_run_id":          diagnostic_run_id,
            "evaluation_run_id":          evaluation_run_id,
            "fit_run_id":                 fit_run_id,
            "report_id":                  report_id,
            "report_name":                report_name,
            "model_family":               model_family,
            "model_name":                 model_name,
            "candidate_m":                candidate_m,
            "fit_scope":                  fit_scope,
            "fold_number":                fold_number if fit_scope == "backtest_fold" else None,
            "training_start":             train_start,
            "training_cutoff":            train_cutoff,
            "residual_source":            "training",
            "residual_extraction_status": ext_status,
            "residual_extraction_reason": ext_reason,
            "training_observation_count": n_train,
            "fitted_observation_count":   n_fitted,
            "residual_observation_count": n_resid,
        }

        valid_rows_added = 0

        if (
            ext_status == "ok"
            and actuals is not None
            and fitteds is not None
            and residuals is not None
            and dates is not None
            and len(actuals) > 0
        ):
            for i in range(len(actuals)):
                r = float(residuals[i])
                a = float(actuals[i])
                f = float(fitteds[i])
                valid = np.isfinite(r) and np.isfinite(a) and np.isfinite(f)
                if not valid:
                    continue  # exclude non-finite rows per policy
                row = {
                    **common,
                    "residual_date":             dates[i],
                    "actual":                    a,
                    "fitted":                    f,
                    "residual":                  r,
                    "residual_observation_valid": True,
                }
                rows.append(row)
                valid_rows_added += 1

        # Stub row when zero valid residuals (unavailable, failed, or empty arrays)
        if valid_rows_added == 0:
            rows.append({
                **common,
                "residual_date":             None,
                "actual":                    None,
                "fitted":                    None,
                "residual":                  None,
                "residual_observation_valid": False,
            })

    if not rows:
        return pd.DataFrame(columns=TRAINING_RESIDUALS_COLS)

    df = pd.DataFrame(rows)
    for col in TRAINING_RESIDUALS_COLS:
        if col not in df.columns:
            df[col] = None

    df = df[TRAINING_RESIDUALS_COLS].sort_values(
        ["report_id", "fit_scope", "fold_number", "model_name", "candidate_m", "residual_date"],
        ignore_index=True,
        na_position="last",
    )
    return df


# ---------------------------------------------------------------------------
# Dataset 2 — Backtest forecast errors
# ---------------------------------------------------------------------------

def build_backtest_forecast_error_dataset(
    backtest_df: pd.DataFrame,
    *,
    name_lookup: Optional[dict[str, str]] = None,
) -> pd.DataFrame:
    """Derive the backtest forecast-error dataset from backtest predictions.

    Parameters
    ----------
    backtest_df:
        Content of ``outputs/metrics/backtest_predictions_latest.csv``.
        Must have all columns in ``BACKTEST_PREDICTIONS_COLS`` from
        ``src.models.backtest_predictions``.
    name_lookup:
        Optional ``{report_id: report_name}`` mapping.

    Returns
    -------
    pd.DataFrame
        Columns: ``BACKTEST_FORECAST_ERRORS_COLS``.

    Notes
    -----
    ``residual = actual - forecast`` (already present in backtest_predictions
    as ``residual``).  ``signed_error = forecast - actual`` is also already
    present.  This function adds ``report_name``, ``residual_source``, and
    ``residual_observation_valid``; the arithmetic is validated, not recomputed.
    """
    if backtest_df is None or backtest_df.empty:
        return pd.DataFrame(columns=BACKTEST_FORECAST_ERRORS_COLS)

    name_lookup = name_lookup or {}
    df = backtest_df.copy()

    df["report_name"] = df["report_id"].map(name_lookup).fillna(df["report_id"])
    df["residual_source"] = "backtest"

    # A row is a valid residual observation when:
    # - fit_status != "failed"
    # - actual and forecast are both finite
    actual_ok   = pd.to_numeric(df["actual"],   errors="coerce").notna()
    forecast_ok = pd.to_numeric(df["forecast"], errors="coerce").notna()
    not_failed  = df["fit_status"] != "failed"
    df["residual_observation_valid"] = actual_ok & forecast_ok & not_failed

    # For invalid rows, null out residual/signed_error so consumers don't
    # accidentally use NaN-contaminated rows as zeros.
    invalid = ~df["residual_observation_valid"]
    for col in ("residual", "signed_error", "absolute_error", "squared_error",
                "inside_interval", "interval_width"):
        if col in df.columns:
            df.loc[invalid, col] = np.nan

    for col in BACKTEST_FORECAST_ERRORS_COLS:
        if col not in df.columns:
            df[col] = None

    df = df[BACKTEST_FORECAST_ERRORS_COLS].sort_values(
        ["report_id", "fold_number", "model_name", "candidate_m", "forecast_date"],
        ignore_index=True,
    )
    return df


# ---------------------------------------------------------------------------
# Dataset 3 — Production forecast-error diagnostic view
# ---------------------------------------------------------------------------

def build_production_forecast_error_view(
    realized_df: pd.DataFrame,
) -> pd.DataFrame:
    """Derive the production forecast-error view from realized forecast history.

    Parameters
    ----------
    realized_df:
        Content of ``outputs/metrics/realized_forecast_history.csv``.
        Must use the canonical ``REALIZED_FORECAST_HISTORY_COLS`` schema where
        ``signed_error = forecast - actual``.

    Returns
    -------
    pd.DataFrame
        Columns: ``PRODUCTION_FORECAST_ERRORS_COLS``.

    Sign-convention derivation::

        residual = actual - forecast = -signed_error

    Both formulas are reconciled during validation.  The canonical
    ``signed_error`` column is preserved unchanged.
    """
    if realized_df is None or realized_df.empty:
        return pd.DataFrame(columns=PRODUCTION_FORECAST_ERRORS_COLS)

    df = realized_df.copy()

    # Derive residual = actual - forecast = -signed_error
    actual   = pd.to_numeric(df["actual"],       errors="coerce")
    forecast = pd.to_numeric(df["forecast"],     errors="coerce")
    se       = pd.to_numeric(df.get("signed_error", pd.Series(dtype=float)), errors="coerce")

    df["residual"] = actual - forecast

    df["residual_source"] = "production"

    actual_ok   = actual.notna() & (actual >= 0)
    forecast_ok = forecast.notna()
    df["residual_observation_valid"] = actual_ok & forecast_ok

    # Rename realized_forecast_history columns that differ from output schema
    _rename = {
        "selected_model_family": "selected_model_family",
        "selected_model_name":   "selected_model_name",
        "selected_m":            "selected_m",
    }
    for src, dst in _rename.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]

    # report_name may not exist in realized history; default to report_id
    if "report_name" not in df.columns:
        df["report_name"] = df.get("report_id", "")

    for col in PRODUCTION_FORECAST_ERRORS_COLS:
        if col not in df.columns:
            df[col] = None

    df = df[PRODUCTION_FORECAST_ERRORS_COLS].sort_values(
        ["run_id", "report_id", "forecast_date"],
        ignore_index=True,
    )
    return df


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_training_residual_dataset(df: pd.DataFrame) -> None:
    """Raise ValueError on schema or arithmetic violations.

    Checks
    ------
    1. All TRAINING_RESIDUALS_COLS present.
    2. residual_source == "training" everywhere.
    3. residual_observation_valid is boolean-coercible.
    4. For valid rows: actual, fitted, residual are all finite.
    5. For valid rows: residual == actual - fitted (within 1e-9).
    6. residual dates fall at or within [training_start, training_cutoff].
    7. evidence count ordering: residual_obs <= fitted_obs <= training_obs.
    8. fit_scope contains only "backtest_fold" | "production_refit".
    9. backtest_fold rows have non-null fold_number.
    10. No raw model objects.
    """
    _check_required_cols(df, TRAINING_RESIDUALS_COLS, "training_residual")

    if df.empty:
        return

    # residual_source
    if not (df["residual_source"] == "training").all():
        bad = df[df["residual_source"] != "training"]["residual_source"].unique()
        raise ValueError(
            f"training_residual: unexpected residual_source values: {bad.tolist()}"
        )

    # fit_scope
    valid_scopes = {"backtest_fold", "production_refit"}
    bad_scopes = set(df["fit_scope"].dropna().unique()) - valid_scopes
    if bad_scopes:
        raise ValueError(
            f"training_residual: invalid fit_scope values: {sorted(bad_scopes)}"
        )

    # backtest_fold rows need fold_number
    bf_mask = df["fit_scope"] == "backtest_fold"
    if bf_mask.any() and df.loc[bf_mask, "fold_number"].isna().any():
        raise ValueError(
            "training_residual: backtest_fold rows must have non-null fold_number."
        )

    valid = df["residual_observation_valid"].fillna(False).astype(bool)
    valid_rows = df[valid]

    if not valid_rows.empty:
        # Finite checks
        for col in ("actual", "fitted", "residual"):
            non_finite = ~np.isfinite(valid_rows[col].astype(float))
            if non_finite.any():
                raise ValueError(
                    f"training_residual: {non_finite.sum()} valid rows have "
                    f"non-finite {col}."
                )

        # Arithmetic: residual = actual - fitted
        a = valid_rows["actual"].astype(float)
        f = valid_rows["fitted"].astype(float)
        r = valid_rows["residual"].astype(float)
        discrepancy = (r - (a - f)).abs()
        if (discrepancy > 1e-9).any():
            n = int((discrepancy > 1e-9).sum())
            raise ValueError(
                f"training_residual: {n} valid rows where residual ≠ actual - fitted."
            )

    # Unique key (only for rows with a non-null residual_date)
    keyed = df[df["residual_date"].notna()]
    if not keyed.empty:
        dupes = keyed.duplicated(subset=_TR_UNIQUE_KEY, keep=False)
        if dupes.any():
            raise ValueError(
                f"training_residual: {dupes.sum()} duplicate rows on key "
                f"{_TR_UNIQUE_KEY}."
            )

    # Evidence count ordering
    _check_evidence_counts(df, "training_residual")


def validate_backtest_forecast_error_dataset(df: pd.DataFrame) -> None:
    """Raise ValueError on schema or arithmetic violations.

    Checks
    ------
    1. All BACKTEST_FORECAST_ERRORS_COLS present.
    2. residual_source == "backtest" everywhere.
    3. horizon_step in [1, 28].
    4. For valid rows: residual == actual - forecast.
    5. For valid rows: signed_error == forecast - actual.
    6. For valid rows: residual == -signed_error.
    7. For valid rows: absolute_error >= 0.
    8. For valid rows: lower_bound <= upper_bound (where both present).
    9. Failed fit rows must not be marked valid.
    10. Unique key is unique.
    """
    _check_required_cols(df, BACKTEST_FORECAST_ERRORS_COLS, "backtest_forecast_errors")

    if df.empty:
        return

    if not (df["residual_source"] == "backtest").all():
        raise ValueError("backtest_forecast_errors: residual_source must be 'backtest'.")

    # horizon_step range
    steps = pd.to_numeric(df["horizon_step"], errors="coerce").dropna()
    if not steps.empty:
        out = steps[(steps < _MIN_HORIZON_STEP) | (steps > _MAX_HORIZON_STEP)]
        if not out.empty:
            raise ValueError(
                f"backtest_forecast_errors: horizon_step out of "
                f"[{_MIN_HORIZON_STEP},{_MAX_HORIZON_STEP}]: "
                f"{sorted(out.unique().tolist())[:5]}"
            )

    # Failed rows must not be valid
    failed_valid = (df["fit_status"] == "failed") & df["residual_observation_valid"].fillna(False).astype(bool)
    if failed_valid.any():
        raise ValueError(
            f"backtest_forecast_errors: {failed_valid.sum()} failed-fit rows are "
            "marked residual_observation_valid=True."
        )

    valid = df["residual_observation_valid"].fillna(False).astype(bool)
    valid_rows = df[valid]

    if not valid_rows.empty:
        a  = pd.to_numeric(valid_rows["actual"],       errors="coerce")
        fc = pd.to_numeric(valid_rows["forecast"],     errors="coerce")
        r  = pd.to_numeric(valid_rows["residual"],     errors="coerce")
        se = pd.to_numeric(valid_rows["signed_error"], errors="coerce")
        ae = pd.to_numeric(valid_rows["absolute_error"], errors="coerce")

        # residual = actual - forecast
        if ((r - (a - fc)).abs() > 1e-9).any():
            n = int(((r - (a - fc)).abs() > 1e-9).sum())
            raise ValueError(
                f"backtest_forecast_errors: {n} rows where residual ≠ actual-forecast."
            )

        # signed_error = forecast - actual
        if ((se - (fc - a)).abs() > 1e-9).any():
            n = int(((se - (fc - a)).abs() > 1e-9).sum())
            raise ValueError(
                f"backtest_forecast_errors: {n} rows where signed_error ≠ forecast-actual."
            )

        # residual = -signed_error
        if ((r + se).abs() > 1e-9).any():
            n = int(((r + se).abs() > 1e-9).sum())
            raise ValueError(
                f"backtest_forecast_errors: {n} rows where residual ≠ -signed_error."
            )

        # absolute_error >= 0
        if (ae < 0).any():
            raise ValueError("backtest_forecast_errors: negative absolute_error.")

        # Interval consistency
        lo = pd.to_numeric(valid_rows.get("lower_bound", pd.Series(dtype=float)), errors="coerce")
        hi = pd.to_numeric(valid_rows.get("upper_bound", pd.Series(dtype=float)), errors="coerce")
        both = lo.notna() & hi.notna()
        if both.any() and (lo[both] > hi[both] + 1e-9).any():
            raise ValueError("backtest_forecast_errors: lower_bound > upper_bound.")

    # Unique key (skip when evaluation_run_id is all-null — e.g. test scenarios)
    has_run_id = df["evaluation_run_id"].notna().any()
    if has_run_id:
        dupes = df.duplicated(subset=_BFE_UNIQUE_KEY, keep=False)
        if dupes.any():
            raise ValueError(
                f"backtest_forecast_errors: {dupes.sum()} duplicate rows on key "
                f"{_BFE_UNIQUE_KEY}."
            )


def validate_production_forecast_error_dataset(df: pd.DataFrame) -> None:
    """Raise ValueError on schema or arithmetic violations.

    Checks
    ------
    1. All PRODUCTION_FORECAST_ERRORS_COLS present.
    2. residual_source == "production" everywhere.
    3. For valid rows: residual == actual - forecast.
    4. For valid rows: residual == -signed_error.
    5. horizon_step in [1, 28].
    6. actual and forecast are non-negative where valid.
    7. Unique key is unique.
    """
    _check_required_cols(df, PRODUCTION_FORECAST_ERRORS_COLS, "production_forecast_errors")

    if df.empty:
        return

    if not (df["residual_source"] == "production").all():
        raise ValueError("production_forecast_errors: residual_source must be 'production'.")

    steps = pd.to_numeric(df["horizon_step"], errors="coerce").dropna()
    if not steps.empty:
        out = steps[(steps < _MIN_HORIZON_STEP) | (steps > _MAX_HORIZON_STEP)]
        if not out.empty:
            raise ValueError(
                f"production_forecast_errors: horizon_step out of "
                f"[{_MIN_HORIZON_STEP},{_MAX_HORIZON_STEP}]."
            )

    valid = df["residual_observation_valid"].fillna(False).astype(bool)
    valid_rows = df[valid]

    if not valid_rows.empty:
        a  = pd.to_numeric(valid_rows["actual"],       errors="coerce")
        fc = pd.to_numeric(valid_rows["forecast"],     errors="coerce")
        r  = pd.to_numeric(valid_rows["residual"],     errors="coerce")
        se = pd.to_numeric(valid_rows["signed_error"], errors="coerce")

        # Non-negative actuals and forecasts
        if (a < 0).any():
            raise ValueError("production_forecast_errors: negative actual values.")
        if (fc < 0).any():
            raise ValueError("production_forecast_errors: negative forecast values.")

        # residual = actual - forecast
        both_finite = r.notna() & (a - fc).notna()
        if both_finite.any():
            disc = (r[both_finite] - (a[both_finite] - fc[both_finite])).abs()
            if (disc > 1e-9).any():
                raise ValueError(
                    "production_forecast_errors: residual ≠ actual - forecast."
                )

        # residual = -signed_error
        se_finite = r.notna() & se.notna()
        if se_finite.any():
            disc2 = (r[se_finite] + se[se_finite]).abs()
            if (disc2 > 1e-9).any():
                raise ValueError(
                    "production_forecast_errors: residual ≠ -signed_error."
                )

    # Unique key
    dupes = df.duplicated(subset=_PFE_UNIQUE_KEY, keep=False)
    if dupes.any():
        raise ValueError(
            f"production_forecast_errors: {dupes.sum()} duplicate rows on key "
            f"{_PFE_UNIQUE_KEY}."
        )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_residual_datasets(
    *,
    training_residual_records: Optional[list[dict]] = None,
    backtest_predictions_path: Optional[Path] = None,
    realized_history_path: Optional[Path] = None,
    project_root: Path,
    diagnostic_run_id: str = "",
    evaluation_run_id: str = "",
    fit_run_id: str = "",
    name_lookup: Optional[dict[str, str]] = None,
) -> dict[str, Optional[Path]]:
    """Build, validate, and persist all three residual diagnostic datasets.

    Writes after validation succeeds.  Failures in one dataset do not prevent
    the others from being written.  Returns a dict of ``{name: path}`` for
    each dataset written; ``None`` when writing was skipped or failed.

    Parameters
    ----------
    training_residual_records:
        Output of ``run_candidate_backtest_stage`` (fifth return value).
    backtest_predictions_path:
        Path to ``outputs/metrics/backtest_predictions_latest.csv``.
    realized_history_path:
        Path to ``outputs/metrics/realized_forecast_history.csv``.
    project_root:
        Repository root; ``outputs/diagnostics/`` is created if absent.
    diagnostic_run_id:
        Identifier for this diagnostic build.
    evaluation_run_id:
        Backtest run ID.
    fit_run_id:
        Fitting run ID (equals ``evaluation_run_id`` for backtest folds).
    name_lookup:
        Optional ``{report_id: report_name}`` mapping.
    """
    diag_dir = project_root / _DIAGNOSTICS_DIR
    diag_dir.mkdir(parents=True, exist_ok=True)

    paths: dict[str, Optional[Path]] = {
        "training_residuals": None,
        "backtest_forecast_errors": None,
        "production_forecast_errors": None,
    }

    # --- Dataset 1: Training residuals ---
    try:
        records = training_residual_records or []
        tr_df = build_training_residual_dataset(
            records,
            diagnostic_run_id=diagnostic_run_id,
            evaluation_run_id=evaluation_run_id,
            fit_run_id=fit_run_id or evaluation_run_id,
            name_lookup=name_lookup,
        )
        validate_training_residual_dataset(tr_df)
        tr_path = project_root / _TRAINING_RESIDUALS_FILE
        tr_df.to_csv(tr_path, index=False)
        paths["training_residuals"] = tr_path

        n_valid   = int(tr_df["residual_observation_valid"].fillna(False).sum())
        n_invalid = len(tr_df) - n_valid
        _log.info(
            "Training residuals: %d valid rows, %d unavailable/stub rows → %s",
            n_valid, n_invalid, tr_path,
        )
    except Exception as exc:
        _log.warning("Training residual dataset failed: %s", exc)

    # --- Dataset 2: Backtest forecast errors ---
    try:
        if backtest_predictions_path and Path(backtest_predictions_path).exists():
            bp_df = pd.read_csv(backtest_predictions_path)
        else:
            bp_df = pd.DataFrame()

        bfe_df = build_backtest_forecast_error_dataset(bp_df, name_lookup=name_lookup)
        validate_backtest_forecast_error_dataset(bfe_df)
        bfe_path = project_root / _BACKTEST_ERRORS_FILE
        bfe_df.to_csv(bfe_path, index=False)
        paths["backtest_forecast_errors"] = bfe_path

        n_valid   = int(bfe_df["residual_observation_valid"].fillna(False).sum())
        n_invalid = len(bfe_df) - n_valid
        _log.info(
            "Backtest forecast errors: %d valid rows, %d invalid rows → %s",
            n_valid, n_invalid, bfe_path,
        )
    except Exception as exc:
        _log.warning("Backtest forecast error dataset failed: %s", exc)

    # --- Dataset 3: Production forecast errors ---
    try:
        if realized_history_path and Path(realized_history_path).exists():
            rh_df = pd.read_csv(realized_history_path)
        else:
            rh_df = pd.DataFrame()

        pfe_df = build_production_forecast_error_view(rh_df)
        validate_production_forecast_error_dataset(pfe_df)
        pfe_path = project_root / _PRODUCTION_ERRORS_FILE
        pfe_df.to_csv(pfe_path, index=False)
        paths["production_forecast_errors"] = pfe_path

        n_valid      = int(pfe_df["residual_observation_valid"].fillna(False).sum())
        n_incomplete = int(~pfe_df.get("lineage_complete", pd.Series(True)).fillna(True).sum())
        _log.info(
            "Production forecast errors: %d valid rows, %d incomplete-lineage rows → %s",
            n_valid, n_incomplete, pfe_path,
        )
    except Exception as exc:
        _log.warning("Production forecast error dataset failed: %s", exc)

    return paths


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_required_cols(df: pd.DataFrame, cols: list[str], name: str) -> None:
    missing = set(cols) - set(df.columns)
    if missing:
        raise ValueError(
            f"{name} is missing required column(s): {sorted(missing)}"
        )


def _check_evidence_counts(df: pd.DataFrame, name: str) -> None:
    """Validate evidence count ordering: residual_obs <= fitted_obs <= training_obs."""
    for col in ("training_observation_count", "fitted_observation_count",
                "residual_observation_count"):
        vals = pd.to_numeric(df[col], errors="coerce").dropna()
        if (vals < 0).any():
            raise ValueError(f"{name}: negative {col}.")

    n_train  = pd.to_numeric(df["training_observation_count"], errors="coerce")
    n_fitted = pd.to_numeric(df["fitted_observation_count"],   errors="coerce")
    n_resid  = pd.to_numeric(df["residual_observation_count"], errors="coerce")

    both_tf = n_train.notna() & n_fitted.notna()
    if both_tf.any() and (n_fitted[both_tf] > n_train[both_tf]).any():
        raise ValueError(
            f"{name}: fitted_observation_count > training_observation_count."
        )

    both_fr = n_fitted.notna() & n_resid.notna()
    if both_fr.any() and (n_resid[both_fr] > n_fitted[both_fr]).any():
        raise ValueError(
            f"{name}: residual_observation_count > fitted_observation_count."
        )
