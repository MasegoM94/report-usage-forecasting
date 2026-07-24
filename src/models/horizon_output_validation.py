"""Schema validation for persisted horizon-bucket output files.

Validates the two canonical horizon-bucket CSVs:

backtest_horizon_performance_latest.csv
    From ``calculate_horizon_bucket_metrics`` enriched with selection lineage.
    One row per (report_id, model_name, fold_number, horizon_bucket).
    Required dimensions: report_id, model_name, fold_number, horizon_bucket,
    selected_model_name, selected_m.

realized_performance_by_horizon.csv
    From ``realized_performance_by_horizon`` in the monitoring pipeline.
    One row per (report_id, horizon_bucket).
    Required dimensions: report_id, horizon_bucket.

Both files must only contain the canonical horizon_bucket values defined in
``HORIZON_BUCKETS``.

Public API
----------
validate_backtest_horizon_output(df) → None   (raises ValueError on violations)
validate_realized_horizon_output(df)  → None  (raises ValueError on violations)
"""

from __future__ import annotations

import pandas as pd

from src.models.horizon_evaluation import HORIZON_BUCKETS

# Canonical set of horizon bucket names — derived from the single source of truth
_VALID_BUCKETS: frozenset[str] = frozenset(name for name, _, _ in HORIZON_BUCKETS)

# Required columns for each output schema
_BACKTEST_REQUIRED_COLS: set[str] = {
    "report_id",
    "model_name",
    "fold_number",
    "horizon_bucket",
    "mae",
    "wape",
    "bias",
    "observation_count",
    # lineage columns added during save
    "selected_model_name",
    "selected_m",
}

_REALIZED_REQUIRED_COLS: set[str] = {
    "report_id",
    "horizon_bucket",
    "mae",
    "wape",
    "observation_count",
}


def validate_backtest_horizon_output(df: pd.DataFrame) -> None:
    """Raise ValueError if *df* does not satisfy the backtest horizon schema.

    Checks
    ------
    * All required columns are present.
    * ``horizon_bucket`` values are a subset of the canonical bucket names.
    * No duplicate (report_id, model_name, fold_number, horizon_bucket) rows.
    """
    _check_required_cols(df, _BACKTEST_REQUIRED_COLS, label="backtest horizon")
    _check_bucket_values(df, label="backtest horizon")

    dupe_key = ["report_id", "model_name", "fold_number", "horizon_bucket"]
    _check_no_duplicates(df, dupe_key, label="backtest horizon")


def validate_realized_horizon_output(df: pd.DataFrame) -> None:
    """Raise ValueError if *df* does not satisfy the realized horizon schema.

    Checks
    ------
    * All required columns are present.
    * ``horizon_bucket`` values are a subset of the canonical bucket names.
    * No duplicate (report_id, horizon_bucket) rows.
    """
    _check_required_cols(df, _REALIZED_REQUIRED_COLS, label="realized horizon")
    _check_bucket_values(df, label="realized horizon")

    dupe_key = ["report_id", "horizon_bucket"]
    _check_no_duplicates(df, dupe_key, label="realized horizon")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_required_cols(df: pd.DataFrame, required: set[str], label: str) -> None:
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"{label} output is missing required column(s): {sorted(missing)}"
        )


def _check_bucket_values(df: pd.DataFrame, label: str) -> None:
    if "horizon_bucket" not in df.columns:
        return  # already caught by required-cols check
    unknown = set(df["horizon_bucket"].dropna().unique()) - _VALID_BUCKETS
    if unknown:
        raise ValueError(
            f"{label} output contains unknown horizon_bucket value(s): "
            f"{sorted(unknown)}. Valid values: {sorted(_VALID_BUCKETS)}"
        )


def _check_no_duplicates(df: pd.DataFrame, key_cols: list[str], label: str) -> None:
    present_keys = [c for c in key_cols if c in df.columns]
    if not present_keys:
        return
    dupes = df.duplicated(subset=present_keys, keep=False)
    if dupes.any():
        n = int(dupes.sum())
        raise ValueError(
            f"{label} output has {n} duplicate row(s) "
            f"on key {present_keys}."
        )
