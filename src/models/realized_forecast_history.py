"""Normalized realized production forecast history.

This module owns the canonical schema for realized production forecasts and
provides three pure-function layers that keep I/O separate from transformation:

    normalize_forecast_history_schema(raw_df, source)
        Coerce either the current production_forecasts_history schema or the
        legacy forecasts_history schema into the canonical column set.

    build_realized_forecast_rows(forecast_history, actual_daily_series, realized_at)
        Join production forecasts to actuals and compute all error/interval
        columns.  Pure: no filesystem reads or writes.

    validate_realized_forecast_history(df)
        Raise ValueError with a clear message when any invariant is violated.

File-level helpers (write_realized_forecast_history, load_realized_forecast_history)
handle CSV I/O with append-only, dedup-safe semantics.

Canonical schema
----------------
Identity and lineage:
    run_id              – str    pipeline run identifier
    selection_run_id    – str    run_id of the backtest/selection that chose the model
    generated_at        – ts     when the forecast was generated
    training_cutoff     – ts     last date of training data used for this forecast
    realized_at         – ts     when this row was written (actual became available)
    report_id           – str
    report_name         – str

Forecast specification:
    selected_model_family  – str   e.g. "auto_arima", "seasonal_naive"
    selected_model_name    – str   e.g. "auto_arima_m7"
    selected_m             – int   1 for non-seasonal

Forecast timing:
    forecast_date  – date  the calendar date being forecasted
    horizon_step   – int   1-indexed days since training_cutoff  (1 .. 28)

Forecast values:
    forecast      – float  point forecast
    lower_bound   – float  lower prediction-interval bound (may be NaN)
    upper_bound   – float  upper prediction-interval bound (may be NaN)

Actual and errors:
    actual           – float  realized daily views
    signed_error     – float  forecast − actual  (positive = over-forecast)
    absolute_error   – float  |signed_error|
    squared_error    – float  signed_error²
    inside_interval  – bool   lower_bound ≤ actual ≤ upper_bound (NaN when bounds absent)
    interval_width   – float  upper_bound − lower_bound (NaN when bounds absent)

Optional:
    percentage_error – float  |signed_error| / actual × 100; NaN when actual = 0

Single-writer assumption
------------------------
CSV history persistence assumes a single active pipeline writer.  Concurrent
processes may create duplicate or lost updates because the read-check-append-
write sequence is not transactional.  An in-process threading lock (from
``src.persistence._csv_lock``) prevents two threads within the same Python
process from writing the history file simultaneously, but separate OS processes
are NOT coordinated and this implementation makes no claim of cross-process
safety.

For production deployments with concurrent writers use transactional database
storage, Delta or Iceberg tables, database upserts, orchestrator-enforced
mutual exclusion, or an explicit cross-process file-locking mechanism.
"""

from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime as _dt
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.persistence._csv_lock import csv_write_lock, log_write_plan

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Canonical schema
# ---------------------------------------------------------------------------

REALIZED_FORECAST_HISTORY_COLS: list[str] = [
    # identity / lineage
    "run_id",
    "selection_run_id",
    "generated_at",
    "training_cutoff",
    "realized_at",
    "report_id",
    "report_name",
    # forecast specification
    "selected_model_family",
    "selected_model_name",
    "selected_m",
    # timing
    "forecast_date",
    "horizon_step",
    # values
    "forecast",
    "lower_bound",
    "upper_bound",
    # actuals and errors
    "actual",
    "signed_error",
    "absolute_error",
    "squared_error",
    "inside_interval",
    "interval_width",
    # optional
    "percentage_error",
    # lineage completeness flags (populated during migration; null for live rows)
    "lineage_complete",
    "lineage_missing_fields",
]

# Lineage columns that must be non-null for a row to be considered fully complete
_LINEAGE_COLS = [
    "run_id", "selection_run_id", "generated_at", "training_cutoff",
    "selected_model_family", "selected_model_name", "selected_m",
]

# Unique key — no duplicates allowed across the history file
_UNIQUE_KEY = ["run_id", "report_id", "forecast_date"]

# File location (relative to project root)
_REALIZED_HISTORY_RELPATH = Path("outputs") / "metrics" / "realized_forecast_history.csv"

# Legacy file produced by the old update_realized_errors path
_LEGACY_RELPATH = Path("outputs") / "metrics" / "realized_errors_history.csv"

# Archive directory for migrated / retired files
_ARCHIVE_RELPATH = Path("outputs") / "archive"

# ---------------------------------------------------------------------------
# Source schema constants (for normalization)
# ---------------------------------------------------------------------------

# Columns that must exist in the current production forecast history
_PRODUCTION_REQUIRED_COLS = {
    "run_id", "report_id", "forecast_date", "forecast",
    "selected_model_family", "selected_model_name", "selected_m",
    "training_cutoff",
}

# Old column names in the legacy forecasts_history.csv → canonical names
_LEGACY_RENAME = {
    "target_date":    "forecast_date",
    "forecast_views": "forecast",
    "actual_views":   "actual",
    "error":          "signed_error",
    "abs_error":      "absolute_error",
    "lower_ci":       "lower_bound",
    "upper_ci":       "upper_bound",
}

# ---------------------------------------------------------------------------
# Schema normalization
# ---------------------------------------------------------------------------

def normalize_forecast_history_schema(
    raw_df: pd.DataFrame,
    source: str,
) -> pd.DataFrame:
    """Coerce *raw_df* into the canonical pre-join column set.

    Parameters
    ----------
    raw_df:
        DataFrame as read from disk.  Must match one of the known schemas.
    source:
        ``"production"`` — current production_forecasts_history schema
            (PRODUCTION_FORECAST_COLS from src.models.production_forecast).
        ``"legacy"`` — old forecasts_history schema written by append_forecasts_history.

    Returns
    -------
    DataFrame with canonical column names; columns not present in the source
    are added as NaN.  Error columns (signed_error, absolute_error, etc.) are
    NOT computed here; ``build_realized_forecast_rows`` does that after joining
    to actuals.

    Raises
    ------
    ValueError
        For an unknown source tag or when required columns are missing.
    """
    if source not in ("production", "legacy"):
        raise ValueError(
            f"Unknown source {source!r}. Expected 'production' or 'legacy'."
        )

    df = raw_df.copy()

    if source == "legacy":
        missing = {"run_id", "report_id", "target_date", "forecast_views"} - set(df.columns)
        if missing:
            raise ValueError(
                f"Legacy forecast history is missing required columns: {sorted(missing)}. "
                "Cannot normalize an incompatible schema — pass source='legacy' only for "
                "files produced by append_forecasts_history."
            )
        df = df.rename(columns=_LEGACY_RENAME)

        # Legacy schema carries model_name but not the split family/name/m columns
        if "model_name" in df.columns and "selected_model_name" not in df.columns:
            df["selected_model_name"] = df["model_name"]
        for col in ("selected_model_family", "selected_m", "selection_run_id",
                    "generated_at", "training_cutoff"):
            if col not in df.columns:
                df[col] = np.nan

        # Derive horizon_step from horizon_days when present
        if "horizon_days" in df.columns and "horizon_step" not in df.columns:
            df["horizon_step"] = pd.to_numeric(df["horizon_days"], errors="coerce")

    elif source == "production":
        missing = _PRODUCTION_REQUIRED_COLS - set(df.columns)
        if missing:
            raise ValueError(
                f"Production forecast history is missing required columns: {sorted(missing)}."
            )
        # Production schema already uses canonical names for most columns
        # Add report_name if absent (possible in minimal test DataFrames)
        if "report_name" not in df.columns:
            df["report_name"] = np.nan

    # Ensure all canonical pre-join columns exist (fill missing with NaN)
    for col in REALIZED_FORECAST_HISTORY_COLS:
        if col not in df.columns:
            df[col] = np.nan

    # Parse dates robustly
    for date_col in ("generated_at", "training_cutoff", "forecast_date"):
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    return df


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def build_realized_forecast_rows(
    forecast_history: pd.DataFrame,
    actual_daily_series: pd.DataFrame,
    realized_at: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    """Join production forecasts to actuals and compute all error columns.

    Parameters
    ----------
    forecast_history:
        Normalized production forecast rows (output of
        ``normalize_forecast_history_schema``).  Must contain at minimum:
        run_id, report_id, forecast_date, forecast.
    actual_daily_series:
        One row per (report_id, date) with a ``daily_views`` column (the
        canonical mart_report_daily_series schema after standardisation).
        Zero-view days must be present as rows with daily_views = 0.
    realized_at:
        Timestamp to stamp on newly created rows.  Defaults to now.

    Returns
    -------
    DataFrame in the canonical ``REALIZED_FORECAST_HISTORY_COLS`` column order.
    Contains only rows where an actual is available (future dates excluded).
    Empty DataFrame if nothing can be joined.

    Raises
    ------
    ValueError
        If required columns are absent from either input.
    """
    if realized_at is None:
        realized_at = pd.Timestamp.now()

    _check_required(forecast_history, {"run_id", "report_id", "forecast_date", "forecast"},
                    "forecast_history")
    _check_required(actual_daily_series, {"report_id", "date", "daily_views"},
                    "actual_daily_series")

    # Columns computed here from the join — drop any NaN placeholders that
    # normalize_forecast_history_schema may have added so they don't collide.
    _POST_JOIN = {
        "actual", "signed_error", "absolute_error", "squared_error",
        "inside_interval", "interval_width", "percentage_error", "realized_at",
    }
    fh = forecast_history.drop(
        columns=[c for c in _POST_JOIN if c in forecast_history.columns],
    ).copy()
    fh["forecast_date"] = pd.to_datetime(fh["forecast_date"], errors="coerce")

    act = actual_daily_series.copy()
    act["date"] = pd.to_datetime(act["date"], errors="coerce")
    # Aggregate per (report_id, date) in case the input has multiple rows
    act = (
        act.groupby(["report_id", "date"])["daily_views"]
        .sum()
        .reset_index(name="actual")
    )

    merged = fh.merge(
        act,
        left_on=["report_id", "forecast_date"],
        right_on=["report_id", "date"],
        how="left",
    )
    # Exclude future dates: keep only rows where actual is present (including 0)
    realized = merged[merged["actual"].notna()].copy()

    if realized.empty:
        return pd.DataFrame(columns=REALIZED_FORECAST_HISTORY_COLS)

    # --- Error columns ---
    realized["forecast"] = pd.to_numeric(realized["forecast"], errors="coerce")
    realized["actual"]   = pd.to_numeric(realized["actual"], errors="coerce")

    realized["signed_error"]   = realized["forecast"] - realized["actual"]
    realized["absolute_error"] = realized["signed_error"].abs()
    realized["squared_error"]  = realized["signed_error"] ** 2

    # percentage_error: null when actual = 0
    pct = realized["absolute_error"] / realized["actual"].replace(0, np.nan) * 100
    realized["percentage_error"] = pct

    # --- Interval columns ---
    has_lower = realized["lower_bound"].notna()
    has_upper = realized["upper_bound"].notna()
    has_both  = has_lower & has_upper

    realized["interval_width"] = np.where(
        has_both,
        realized["upper_bound"] - realized["lower_bound"],
        np.nan,
    )
    realized["inside_interval"] = np.where(
        has_both,
        (realized["actual"] >= realized["lower_bound"]) &
        (realized["actual"] <= realized["upper_bound"]),
        np.nan,
    )
    # Cast to nullable boolean so NaN is preserved correctly
    realized["inside_interval"] = realized["inside_interval"].astype("object")

    realized["realized_at"] = realized_at

    # Drop the join-artifact 'date' column from the actuals side
    if "date" in realized.columns:
        realized = realized.drop(columns=["date"])

    # Return in canonical column order; fill any still-missing columns with NaN
    for col in REALIZED_FORECAST_HISTORY_COLS:
        if col not in realized.columns:
            realized[col] = np.nan

    return realized[REALIZED_FORECAST_HISTORY_COLS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_realized_forecast_history(df: pd.DataFrame) -> None:
    """Validate a realized forecast history DataFrame against all invariants.

    Raises
    ------
    ValueError
        On the first violated invariant with a clear, actionable message.
    """
    # 1. Required columns
    missing_cols = set(REALIZED_FORECAST_HISTORY_COLS) - set(df.columns)
    if missing_cols:
        raise ValueError(
            f"Realized forecast history is missing {len(missing_cols)} required "
            f"column(s): {sorted(missing_cols)}."
        )

    if df.empty:
        return  # an empty validated DataFrame is fine

    # 2. Unique key — no nulls (migrated rows with lineage_complete=False are exempt
    #    because they may legitimately lack run_id from the legacy schema)
    lineage_complete = df.get("lineage_complete", pd.Series(True, index=df.index))
    non_migrated = lineage_complete.isna() | (lineage_complete == True)  # noqa: E712
    for col in _UNIQUE_KEY:
        n_null = df.loc[non_migrated, col].isna().sum()
        if n_null:
            raise ValueError(
                f"Unique key column '{col}' contains {n_null} null value(s) "
                "in fully-lineaged rows. History file may be corrupted."
            )

    # 3. Unique key — no duplicates
    dup_mask = df.duplicated(subset=_UNIQUE_KEY, keep=False)
    n_dup = int(dup_mask.sum())
    if n_dup:
        sample = df.loc[dup_mask, _UNIQUE_KEY].head(3).to_dict(orient="records")
        raise ValueError(
            f"Realized forecast history contains {n_dup} row(s) with duplicate "
            f"({', '.join(_UNIQUE_KEY)}) keys. Sample: {sample}."
        )

    # 4. Date columns parse successfully (errors="raise" already applied upstream;
    #    here we check that no NaT slipped through for key date columns)
    for date_col in ("forecast_date",):
        bad = df[date_col].isna().sum()
        if bad:
            raise ValueError(
                f"Date column '{date_col}' contains {bad} unparseable / null value(s)."
            )

    # 5. horizon_step: integer, 1..28 (when present and non-null)
    if "horizon_step" in df.columns:
        hs = pd.to_numeric(df["horizon_step"], errors="coerce")
        bad_range = ((hs < 1) | (hs > 28)) & hs.notna()
        n_bad = int(bad_range.sum())
        if n_bad:
            raise ValueError(
                f"horizon_step has {n_bad} value(s) outside [1, 28]: "
                f"{hs[bad_range].unique().tolist()[:5]}."
            )

    # 6–7. actual and forecast non-negative
    for col in ("actual", "forecast"):
        neg = (pd.to_numeric(df[col], errors="coerce") < 0).sum()
        if neg:
            raise ValueError(
                f"Column '{col}' contains {neg} negative value(s). "
                "Daily view counts must be non-negative."
            )

    # 8. selected_m: positive integer when present
    if "selected_m" in df.columns:
        sm = pd.to_numeric(df["selected_m"], errors="coerce")
        bad_m = (sm < 1) & sm.notna()
        if bad_m.sum():
            raise ValueError(
                f"selected_m contains {int(bad_m.sum())} value(s) < 1. "
                "Valid periods are positive integers (1 = non-seasonal)."
            )

    # 9. lower_bound <= upper_bound when both present
    lo = pd.to_numeric(df["lower_bound"], errors="coerce")
    hi = pd.to_numeric(df["upper_bound"], errors="coerce")
    both_present = lo.notna() & hi.notna()
    if both_present.any():
        inverted = both_present & (lo > hi)
        n_inv = int(inverted.sum())
        if n_inv:
            raise ValueError(
                f"{n_inv} row(s) have lower_bound > upper_bound."
            )

    # 10. interval_width non-negative when present
    iw = pd.to_numeric(df["interval_width"], errors="coerce")
    neg_width = (iw < 0) & iw.notna()
    if neg_width.sum():
        raise ValueError(
            f"interval_width contains {int(neg_width.sum())} negative value(s)."
        )

    # 11. inside_interval consistent with bounds and actual
    if "inside_interval" in df.columns:
        both = lo.notna() & hi.notna()
        act = pd.to_numeric(df["actual"], errors="coerce")
        expected_inside = (act >= lo) & (act <= hi)
        recorded = df["inside_interval"].map(
            lambda x: None if (x is None or (isinstance(x, float) and np.isnan(x)))
            else bool(x)
        )
        mismatch = both & (recorded != expected_inside)
        n_mis = int(mismatch.sum())
        if n_mis:
            raise ValueError(
                f"{n_mis} row(s) have inside_interval inconsistent with actual "
                "and bounds."
            )

    # 12–14. Error arithmetic consistency (allow small floating-point tolerance)
    fc = pd.to_numeric(df["forecast"], errors="coerce")
    act = pd.to_numeric(df["actual"], errors="coerce")
    se = pd.to_numeric(df["signed_error"], errors="coerce")
    ae = pd.to_numeric(df["absolute_error"], errors="coerce")
    sqe = pd.to_numeric(df["squared_error"], errors="coerce")

    expected_se  = fc - act
    expected_ae  = expected_se.abs()
    expected_sqe = expected_se ** 2

    tol = 1e-6
    if se.notna().any():
        bad_se = ((se - expected_se).abs() > tol) & se.notna()
        if bad_se.sum():
            raise ValueError(
                f"signed_error is inconsistent with forecast − actual in "
                f"{int(bad_se.sum())} row(s)."
            )
    if ae.notna().any():
        bad_ae = ((ae - expected_ae).abs() > tol) & ae.notna()
        if bad_ae.sum():
            raise ValueError(
                f"absolute_error is inconsistent with |signed_error| in "
                f"{int(bad_ae.sum())} row(s)."
            )
    if sqe.notna().any():
        bad_sqe = ((sqe - expected_sqe).abs() > tol) & sqe.notna()
        if bad_sqe.sum():
            raise ValueError(
                f"squared_error is inconsistent with signed_error² in "
                f"{int(bad_sqe.sum())} row(s)."
            )

    # 15. percentage_error null when actual = 0
    if "percentage_error" in df.columns:
        pct = pd.to_numeric(df["percentage_error"], errors="coerce")
        zero_actual = act == 0
        bad_pct = zero_actual & pct.notna()
        if bad_pct.sum():
            raise ValueError(
                f"percentage_error must be null when actual = 0, but "
                f"{int(bad_pct.sum())} row(s) have a non-null value."
            )


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def _validate_existing_on_disk(existing: pd.DataFrame) -> None:
    """Validate the key columns in an existing on-disk file before dedup.

    Separate from ``validate_realized_forecast_history`` so we can give a more
    targeted error message about file corruption vs. new-row validation issues.
    """
    for col in _UNIQUE_KEY:
        n_null = existing[col].isna().sum()
        if n_null:
            raise ValueError(
                f"Existing realized_forecast_history has {n_null} null(s) in "
                f"key column '{col}'. The file may be corrupted. "
                "Inspect outputs/metrics/realized_forecast_history.csv before continuing."
            )

    try:
        existing["forecast_date"] = pd.to_datetime(
            existing["forecast_date"], errors="raise"
        )
    except Exception as exc:
        raise ValueError(
            "Existing realized_forecast_history contains an unparseable "
            f"'forecast_date'. Original error: {exc}"
        ) from exc

    dup_mask = existing.duplicated(subset=_UNIQUE_KEY, keep=False)
    n_dup = int(dup_mask.sum())
    if n_dup:
        sample = existing.loc[dup_mask, _UNIQUE_KEY].head(3).to_dict(orient="records")
        raise ValueError(
            f"Existing realized_forecast_history contains {n_dup} duplicate "
            f"key(s). Sample: {sample}. The file may be corrupted."
        )


def load_realized_forecast_history(project_root: Path) -> pd.DataFrame:
    """Load the canonical realized forecast history CSV, or return an empty frame.

    Returns
    -------
    DataFrame with ``forecast_date`` parsed as datetime, or an empty DataFrame
    if the file does not yet exist.

    Raises
    ------
    ValueError
        If the file exists but contains corrupted key columns.
    """
    path = project_root / _REALIZED_HISTORY_RELPATH
    if not path.exists():
        return pd.DataFrame(columns=REALIZED_FORECAST_HISTORY_COLS)
    existing = pd.read_csv(path)
    if existing.empty:
        return existing
    _validate_existing_on_disk(existing)
    existing["forecast_date"] = pd.to_datetime(existing["forecast_date"])
    return existing


def write_realized_forecast_history(
    new_rows: pd.DataFrame,
    project_root: Path,
    *,
    skip_count_out: Optional[list] = None,
) -> tuple[Path, int]:
    """Append *new_rows* to the realized forecast history; skip already-realized keys.

    Writes to ``outputs/metrics/realized_forecast_history.csv``.

    Single-writer assumption
    ------------------------
    CSV history persistence assumes a single active pipeline writer.  Concurrent
    processes may create duplicate or lost updates because the read-check-append-
    write sequence is not transactional.  An in-process threading lock prevents
    two threads within the same Python process from writing this file
    simultaneously, but separate OS processes are NOT coordinated.

    For production deployments with concurrent writers use transactional
    database storage, Delta or Iceberg tables, database upserts, orchestrator-
    enforced mutual exclusion, or an explicit cross-process file-locking
    mechanism.

    Parameters
    ----------
    new_rows:
        Validated rows to potentially append.
    project_root:
        Repository root; the file is written to
        ``outputs/metrics/realized_forecast_history.csv``.
    skip_count_out:
        If provided, a single-element list; ``[N]`` is set to the number of
        rows skipped because their key already existed.

    Returns
    -------
    (path, n_appended)  — path of the history file; number of rows written.

    Raises
    ------
    ValueError
        If the existing file is corrupted.
    """
    validate_realized_forecast_history(new_rows)

    path = project_root / _REALIZED_HISTORY_RELPATH
    path.parent.mkdir(parents=True, exist_ok=True)

    with csv_write_lock(path):
        existing = load_realized_forecast_history(project_root)
        existing_rows = len(existing)

        n_skip = 0
        rows_to_write = new_rows
        if not existing.empty:
            existing_keys = set(
                zip(
                    existing["run_id"],
                    existing["report_id"],
                    existing["forecast_date"],
                )
            )
            rows_to_write = rows_to_write.copy()
            rows_to_write["_fc_date_ts"] = pd.to_datetime(rows_to_write["forecast_date"])
            candidate_keys = list(
                zip(rows_to_write["run_id"], rows_to_write["report_id"],
                    rows_to_write["_fc_date_ts"])
            )
            keep_mask = [k not in existing_keys for k in candidate_keys]
            n_skip = len(keep_mask) - sum(keep_mask)
            rows_to_write = rows_to_write[keep_mask].drop(columns=["_fc_date_ts"]).copy()
        else:
            if "_fc_date_ts" in rows_to_write.columns:
                rows_to_write = rows_to_write.drop(columns=["_fc_date_ts"])

        if skip_count_out is not None and len(skip_count_out) >= 1:
            skip_count_out[0] = n_skip

        log_write_plan(
            target=path,
            candidate_rows=len(new_rows),
            existing_rows=existing_rows,
            new_rows=len(rows_to_write),
            skipped_rows=n_skip,
        )

        if rows_to_write.empty:
            return path, 0

        # Ensure all canonical columns exist; lineage flags default to null for live rows
        for col in REALIZED_FORECAST_HISTORY_COLS:
            if col not in rows_to_write.columns:
                rows_to_write = rows_to_write.copy()
                rows_to_write[col] = np.nan

        write_header = not path.exists() or path.stat().st_size == 0
        rows_to_write[REALIZED_FORECAST_HISTORY_COLS].to_csv(
            path, mode="a", index=False, header=write_header
        )
        return path, len(rows_to_write)


# ---------------------------------------------------------------------------
# Migration helper
# ---------------------------------------------------------------------------

def _compute_lineage_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Populate lineage_complete and lineage_missing_fields for each row in *df*.

    A row is lineage-complete when all ``_LINEAGE_COLS`` are non-null.
    ``lineage_missing_fields`` is a JSON-encoded list of missing column names,
    or null when the row is complete.
    """
    df = df.copy()
    missing_per_row = df[_LINEAGE_COLS].isnull()
    df["lineage_complete"] = (~missing_per_row.any(axis=1)).astype(object)
    df["lineage_missing_fields"] = missing_per_row.apply(
        lambda row: json.dumps(list(row.index[row])) if row.any() else None,
        axis=1,
    )
    return df


def migrate_legacy_realized_errors(
    project_root: Path,
    *,
    backup: bool = True,
) -> dict:
    """Migrate ``outputs/metrics/realized_errors_history.csv`` to the canonical schema.

    This function is **idempotent**: safe to call repeatedly.  On each call it:

    1. Returns early with ``{"status": "no_legacy_file"}`` when the legacy file
       is absent (already archived or never created).
    2. Reads the legacy file, normalizes column names via ``_LEGACY_RENAME``.
    3. Flips ``signed_error`` (legacy stores ``actual − forecast``; canonical
       stores ``forecast − actual``) and recomputes ``absolute_error`` /
       ``squared_error``.
    4. Fills missing canonical columns with NaN.
    5. Stamps ``lineage_complete`` / ``lineage_missing_fields`` on every row so
       callers can filter out rows that lack model lineage without data loss.
    6. Deduplicates against any rows already in
       ``outputs/metrics/realized_forecast_history.csv`` (key:
       ``run_id + report_id + forecast_date``).
    7. Appends only the non-duplicate rows to the canonical file.
    8. Creates a timestamped backup of the legacy file in
       ``outputs/metrics/`` before touching anything (when ``backup=True``).
    9. Moves the legacy file to
       ``outputs/archive/realized_errors_history_<timestamp>.csv``.

    Missing lineage
    ---------------
    Legacy rows that lack ``selected_model_family``, ``run_id``, etc. are
    preserved exactly as-is (NaN in those columns).  ``lineage_complete=False``
    and ``lineage_missing_fields`` lists the absent columns so downstream
    consumers can treat them appropriately without inventing data.

    Single-writer assumption
    ------------------------
    CSV history persistence assumes a single active pipeline writer.  Concurrent
    processes may create duplicate or lost updates because the read-check-append-
    write sequence is not transactional.  An in-process threading lock prevents
    two threads within the same Python process from writing the canonical history
    file simultaneously, but separate OS processes are NOT coordinated and this
    implementation makes no claim of cross-process safety.

    For production deployments with concurrent writers use transactional
    database storage, Delta or Iceberg tables, database upserts, orchestrator-
    enforced mutual exclusion, or an explicit cross-process file-locking
    mechanism.

    Parameters
    ----------
    project_root:
        Repository root.
    backup:
        When True (default), write a timestamped backup of the legacy file
        alongside it before any modification.

    Returns
    -------
    dict with keys:
        ``status``          – one of ``"no_legacy_file"``, ``"already_archived"``,
                              ``"migrated"``, ``"no_new_rows"``
        ``rows_migrated``   – int, rows appended to the canonical file
        ``rows_skipped``    – int, duplicate rows not appended
        ``backup_path``     – Path or None
        ``archive_path``    – Path or None
    """
    legacy_path    = project_root / _LEGACY_RELPATH
    canonical_path = project_root / _REALIZED_HISTORY_RELPATH
    archive_dir    = project_root / _ARCHIVE_RELPATH

    # Already archived in a previous run
    if not legacy_path.exists():
        return {
            "status": "no_legacy_file",
            "rows_migrated": 0,
            "rows_skipped": 0,
            "backup_path": None,
            "archive_path": None,
        }

    legacy_df = pd.read_csv(legacy_path)

    ts = _dt.now().strftime("%Y%m%d_%H%M%S")

    # ------------------------------------------------------------------
    # Backup before any write
    # ------------------------------------------------------------------
    backup_path: Optional[Path] = None
    if backup:
        backup_path = legacy_path.with_name(
            f"realized_errors_history_{ts}.csv.bak"
        )
        shutil.copy2(legacy_path, backup_path)

    if legacy_df.empty:
        # Archive the empty file then return
        archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = archive_dir / f"realized_errors_history_{ts}.csv"
        shutil.move(str(legacy_path), str(archive_path))
        return {
            "status": "no_new_rows",
            "rows_migrated": 0,
            "rows_skipped": 0,
            "backup_path": backup_path,
            "archive_path": archive_path,
        }

    # ------------------------------------------------------------------
    # Normalize column names
    # ------------------------------------------------------------------
    legacy_df = legacy_df.rename(columns=_LEGACY_RENAME)

    # The legacy signed_error stored actual − forecast; canonical is forecast − actual
    if "signed_error" in legacy_df.columns:
        legacy_df["signed_error"]   = -pd.to_numeric(legacy_df["signed_error"], errors="coerce")
        legacy_df["absolute_error"] = legacy_df["signed_error"].abs()
        legacy_df["squared_error"]  = legacy_df["signed_error"] ** 2

    # Parse key date column
    if "forecast_date" in legacy_df.columns:
        legacy_df["forecast_date"] = pd.to_datetime(
            legacy_df["forecast_date"], errors="coerce"
        )

    # Fill all canonical columns (preserves NaN for missing lineage — never invented)
    for col in REALIZED_FORECAST_HISTORY_COLS:
        if col not in legacy_df.columns:
            legacy_df[col] = np.nan

    # Stamp lineage flags
    legacy_df = _compute_lineage_flags(legacy_df)

    # ------------------------------------------------------------------
    # Dedup and append under the in-process write lock
    # ------------------------------------------------------------------
    canonical_path.parent.mkdir(parents=True, exist_ok=True)
    n_skip = 0
    rows_migrated = 0

    with csv_write_lock(canonical_path):
        existing_rows = 0
        if canonical_path.exists():
            existing = pd.read_csv(canonical_path)
            existing_rows = len(existing)
            if not existing.empty:
                existing["forecast_date"] = pd.to_datetime(
                    existing["forecast_date"], errors="coerce"
                )
                existing_keys = set(
                    zip(existing["run_id"], existing["report_id"],
                        existing["forecast_date"])
                )
                legacy_df["_fc_ts"] = pd.to_datetime(
                    legacy_df["forecast_date"], errors="coerce"
                )
                candidate_keys = list(
                    zip(legacy_df["run_id"], legacy_df["report_id"],
                        legacy_df["_fc_ts"])
                )
                keep_mask = [k not in existing_keys for k in candidate_keys]
                n_skip = len(keep_mask) - sum(keep_mask)
                legacy_df = legacy_df[keep_mask].drop(columns=["_fc_ts"]).copy()

        if "_fc_ts" in legacy_df.columns:
            legacy_df = legacy_df.drop(columns=["_fc_ts"])

        log_write_plan(
            target=canonical_path,
            candidate_rows=len(legacy_df) + n_skip,
            existing_rows=existing_rows,
            new_rows=len(legacy_df),
            skipped_rows=n_skip,
        )

        if not legacy_df.empty:
            write_header = (
                not canonical_path.exists() or canonical_path.stat().st_size == 0
            )
            legacy_df[REALIZED_FORECAST_HISTORY_COLS].to_csv(
                canonical_path, mode="a", index=False, header=write_header
            )
            rows_migrated = len(legacy_df)

    # ------------------------------------------------------------------
    # Archive the legacy file
    # ------------------------------------------------------------------
    archive_dir.mkdir(parents=True, exist_ok=True)
    archive_path = archive_dir / f"realized_errors_history_{ts}.csv"
    shutil.move(str(legacy_path), str(archive_path))

    return {
        "status": "migrated" if rows_migrated > 0 else "no_new_rows",
        "rows_migrated": rows_migrated,
        "rows_skipped": n_skip,
        "backup_path": backup_path,
        "archive_path": archive_path,
    }


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

def _check_required(df: pd.DataFrame, required: set[str], name: str) -> None:
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"'{name}' is missing required columns: {sorted(missing)}."
        )


def update_realized_forecast_history(
    raw_actuals_df: pd.DataFrame,
    project_root: Path,
    *,
    realized_at: Optional[pd.Timestamp] = None,
) -> tuple[pd.DataFrame, int]:
    """Top-level entry point: join production forecasts to actuals, then append.

    Selection rule for the forecast source
    ---------------------------------------
    1. If ``outputs/forecasts/production_forecasts_history.csv`` exists, use it
       (current schema, full lineage).
    2. Otherwise fall back to ``outputs/forecasts/forecasts_history.csv`` via the
       legacy normalizer.
    3. If both exist the production file takes precedence and the legacy file is
       ignored.  The caller is responsible for ensuring no double-counting across
       the two sources.

    Parameters
    ----------
    raw_actuals_df:
        mart_report_daily_series canonical DataFrame with columns
        [report_id, date, daily_views].  Must be the only source of actuals.
    project_root:
        Repository root.
    realized_at:
        Timestamp for the realized_at column. Defaults to now.

    Returns
    -------
    (new_rows_df, n_skipped)
        new_rows_df — the rows that were appended (empty if nothing new).
        n_skipped   — number of candidate rows already in the history.
    """
    _check_required(raw_actuals_df, {"report_id", "date", "daily_views"}, "raw_actuals_df")

    prod_path   = project_root / "outputs" / "forecasts" / "production_forecasts_history.csv"
    legacy_path = project_root / "outputs" / "forecasts" / "forecasts_history.csv"

    if prod_path.exists():
        raw = pd.read_csv(prod_path)
        forecast_history = normalize_forecast_history_schema(raw, source="production")
    elif legacy_path.exists():
        raw = pd.read_csv(legacy_path)
        forecast_history = normalize_forecast_history_schema(raw, source="legacy")
    else:
        return pd.DataFrame(columns=REALIZED_FORECAST_HISTORY_COLS), 0

    if forecast_history.empty:
        return pd.DataFrame(columns=REALIZED_FORECAST_HISTORY_COLS), 0

    new_rows = build_realized_forecast_rows(
        forecast_history=forecast_history,
        actual_daily_series=raw_actuals_df,
        realized_at=realized_at or pd.Timestamp.now(),
    )

    if new_rows.empty:
        return new_rows, 0

    skip_count = [0]
    write_realized_forecast_history(new_rows, project_root, skip_count_out=skip_count)
    return new_rows, skip_count[0]
