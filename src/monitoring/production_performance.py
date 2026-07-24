"""Aggregate production forecast monitoring from realized prediction history.

Inputs
------
The normalized realized forecast history DataFrame produced by
``src.models.realized_forecast_history`` (``REALIZED_FORECAST_HISTORY_COLS``).

Outputs
-------
Four aggregate monitoring tables, each returned as a tidy DataFrame:

realized_performance_by_run
    One row per run_id.  Identifies partial vs fully-realized runs.

realized_performance_by_report
    One row per report_id.  Includes recent/previous WAPE delta and a
    monitoring_status flag.

realized_performance_by_horizon
    One row per (report_id, horizon_bucket).  Buckets follow the same
    definitions as ``src.models.horizon_evaluation.HORIZON_BUCKETS``:
        days_1_7     — horizon_step  1 –  7
        days_8_14    — horizon_step  8 – 14
        days_15_28   — horizon_step 15 – 28
        full_horizon — horizon_step  1 – 28

realized_performance_by_model
    One row per (selected_model_family, selected_m).

Design rules
------------
* Metric computation delegates to ``src.models.metrics`` — WAPE, MAE, RMSE,
  and interval metrics are never reimplemented here.
* WAPE is null when total actual volume is zero for a group.
* Zero actual values are preserved; rows are never dropped because actual == 0.
* Row-level percentage error is not used for aggregation.
* Partial runs (realized_prediction_count < expected_prediction_count) are
  identified and NOT presented as fully complete.
* All output DataFrames are deterministically sorted.
* monitoring_status in the report-level table is one of:
    "ok"               — WAPE improved or stayed within threshold
    "degraded"         — WAPE worsened by > DEGRADED_WAPE_THRESHOLD
    "insufficient_data" — fewer than MIN_PERIODS_FOR_COMPARISON realized periods
    "no_actuals"       — all actual values are zero (WAPE undefined)
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.models.horizon_evaluation import HORIZON_BUCKETS
from src.models.metrics import calculate_interval_metrics, calculate_point_metrics
from src.models.realized_forecast_history import (
    REALIZED_FORECAST_HISTORY_COLS,
    load_realized_forecast_history,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Fraction of WAPE degradation that triggers "degraded" status
DEGRADED_WAPE_THRESHOLD: float = 0.05

# Minimum number of realized horizon windows (run-level rows with full
# realization) before recent/previous comparison is valid.
MIN_PERIODS_FOR_COMPARISON: int = 2

# Output column orders — used for schema validation and deterministic reindex
_RUN_COLS = [
    "run_id",
    "generated_at",
    "training_cutoff",
    "realized_prediction_count",
    "expected_prediction_count",
    "realization_rate",
    "fully_realized_report_count",
    "partially_realized_report_count",
    "mae",
    "rmse",
    "wape",
    "bias",
    "absolute_bias",
    "interval_coverage",
    "mean_interval_width",
]

_REPORT_COLS = [
    "report_id",
    "production_run_count",
    "realized_prediction_count",
    "mae",
    "rmse",
    "wape",
    "bias",
    "absolute_bias",
    "interval_coverage",
    "mean_interval_width",
    "recent_wape",
    "previous_wape",
    "accuracy_change",
    "monitoring_status",
]

_HORIZON_COLS = [
    "report_id",
    "horizon_bucket",
    "observation_count",
    "mae",
    "rmse",
    "wape",
    "bias",
    "interval_coverage",
    "mean_interval_width",
]

_MODEL_COLS = [
    "selected_model_family",
    "selected_m",
    "report_count",
    "production_run_count",
    "realized_prediction_count",
    "mae",
    "rmse",
    "wape",
    "bias",
    "interval_coverage",
    "mean_interval_width",
]

# Canonical bucket order for deterministic sorting
_BUCKET_ORDER = [b[0] for b in HORIZON_BUCKETS]


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

_REQUIRED_INPUT_COLS = {
    "run_id", "report_id", "forecast_date", "horizon_step",
    "forecast", "actual", "signed_error", "absolute_error",
    "lower_bound", "upper_bound",
    "generated_at", "training_cutoff",
    "selected_model_family", "selected_model_name", "selected_m",
}


def validate_realized_history_input(df: pd.DataFrame) -> None:
    """Raise ValueError when required columns are missing from the input.

    Does not validate row-level values — that is the responsibility of
    ``validate_realized_forecast_history`` in the realized_history module.
    """
    missing = _REQUIRED_INPUT_COLS - set(df.columns)
    if missing:
        raise ValueError(
            f"realized_forecast_history input is missing {len(missing)} required "
            f"column(s): {sorted(missing)}."
        )


# ---------------------------------------------------------------------------
# Internal aggregation helpers
# ---------------------------------------------------------------------------

def _agg_metrics(group: pd.DataFrame) -> dict:
    """Compute point and interval metrics for a group of realized rows.

    Uses ``calculate_point_metrics`` and ``calculate_interval_metrics`` from
    ``src.models.metrics``.  Training series is not available at monitoring
    time, so MASE columns are not produced here.

    Zero actuals are preserved.  WAPE is null when all actuals are zero.
    """
    actual   = pd.to_numeric(group["actual"],   errors="coerce").to_numpy(float)
    forecast = pd.to_numeric(group["forecast"], errors="coerce").to_numpy(float)

    # Drop rows where either actual or forecast is NaN (no valid pair)
    valid = np.isfinite(actual) & np.isfinite(forecast)
    actual_v   = actual[valid]
    forecast_v = forecast[valid]

    if len(actual_v) == 0:
        pt = {"mae": np.nan, "rmse": np.nan, "wape": np.nan, "bias": np.nan}
    else:
        pt = calculate_point_metrics(actual_v, forecast_v)

    lo = pd.to_numeric(group["lower_bound"], errors="coerce").to_numpy(float)
    hi = pd.to_numeric(group["upper_bound"], errors="coerce").to_numpy(float)
    iv = calculate_interval_metrics(actual, lo, hi)

    return {
        "mae":                pt["mae"],
        "rmse":               pt["rmse"],
        "wape":               pt["wape"],
        "bias":               pt["bias"],
        "absolute_bias":      abs(pt["bias"]) if not np.isnan(pt["bias"]) else np.nan,
        "interval_coverage":  iv["interval_coverage"],
        "mean_interval_width": iv["mean_interval_width"],
    }


def _explode_horizon_buckets(df: pd.DataFrame) -> pd.DataFrame:
    """Replicate rows so each row appears once per matching horizon bucket.

    Uses the same ``HORIZON_BUCKETS`` definitions as
    ``src.models.horizon_evaluation`` — a step may match multiple buckets
    (e.g. step=1 matches both ``days_1_7`` and ``full_horizon``).  Each
    matching (row, bucket) pair becomes its own row in the output, which is
    then grouped by (report_id, horizon_bucket) for aggregation.

    Rows whose horizon_step matches no bucket are dropped.
    """
    hs = pd.to_numeric(df["horizon_step"], errors="coerce")
    pieces = []
    for name, lo, hi in HORIZON_BUCKETS:
        mask = (hs >= lo) & (hs <= hi)
        if mask.any():
            sub = df[mask].copy()
            sub["horizon_bucket"] = name
            pieces.append(sub)
    if not pieces:
        return df.iloc[0:0].copy().assign(horizon_bucket=pd.Series(dtype=str))
    return pd.concat(pieces, ignore_index=True)


# ---------------------------------------------------------------------------
# Per-run: expected prediction count
# ---------------------------------------------------------------------------

def _expected_count_per_run(df: pd.DataFrame) -> pd.Series:
    """Return a Series indexed by run_id with the expected prediction count.

    Expected = reports in this run × horizon steps in this run.
    This is derived from the observed data (max horizon_step per run ×
    distinct report_ids), so partial runs are detected by comparing
    realized vs expected.
    """
    run_stats = df.groupby("run_id").agg(
        _n_reports=("report_id", "nunique"),
        _max_step=("horizon_step", "max"),
    )
    return (run_stats["_n_reports"] * run_stats["_max_step"]).rename("expected")


# ---------------------------------------------------------------------------
# Table 1: realized_performance_by_run
# ---------------------------------------------------------------------------

def realized_performance_by_run(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate realized forecast performance by pipeline run.

    Parameters
    ----------
    df:
        Normalized realized forecast history.  Must contain the columns in
        ``_REQUIRED_INPUT_COLS``.

    Returns
    -------
    DataFrame with columns ``_RUN_COLS``, sorted by ``run_id``.
    Partial runs are identified (realization_rate < 1.0).

    Raises
    ------
    ValueError
        If required columns are missing.
    """
    validate_realized_history_input(df)
    if df.empty:
        return pd.DataFrame(columns=_RUN_COLS)

    df = df.copy()
    df["horizon_step"] = pd.to_numeric(df["horizon_step"], errors="coerce")
    df["actual"]   = pd.to_numeric(df["actual"],   errors="coerce")
    df["forecast"] = pd.to_numeric(df["forecast"], errors="coerce")

    # Per-run aggregate metrics
    rows = []
    for run_id, grp in df.groupby("run_id", sort=True):
        metrics = _agg_metrics(grp)

        # Realized and expected counts
        realized_count = len(grp)
        max_step = int(grp["horizon_step"].max()) if grp["horizon_step"].notna().any() else 0
        n_reports = grp["report_id"].nunique()
        expected_count = n_reports * max_step if max_step > 0 else realized_count
        realization_rate = realized_count / expected_count if expected_count > 0 else np.nan

        # Partial vs fully realized reports within this run
        per_report_counts = grp.groupby("report_id")["horizon_step"].count()
        fully_realized = int((per_report_counts == max_step).sum())
        partially_realized = int((per_report_counts < max_step).sum())

        # Lineage columns (take first non-null value per run)
        generated_at    = grp["generated_at"].dropna().iloc[0] if grp["generated_at"].notna().any() else pd.NaT
        training_cutoff = grp["training_cutoff"].dropna().iloc[0] if grp["training_cutoff"].notna().any() else pd.NaT

        rows.append({
            "run_id":                        run_id,
            "generated_at":                  generated_at,
            "training_cutoff":               training_cutoff,
            "realized_prediction_count":     realized_count,
            "expected_prediction_count":     expected_count,
            "realization_rate":              realization_rate,
            "fully_realized_report_count":   fully_realized,
            "partially_realized_report_count": partially_realized,
            **metrics,
        })

    out = pd.DataFrame(rows)
    # Deterministic sort
    out = out.sort_values("run_id", ignore_index=True)
    return out[_RUN_COLS]


# ---------------------------------------------------------------------------
# Table 2: realized_performance_by_report
# ---------------------------------------------------------------------------

def realized_performance_by_report(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate realized forecast performance by report.

    The report-level view accumulates all realized rows across all runs.
    recent_wape is computed from the most recent run; previous_wape from
    the run immediately before.  accuracy_change = recent_wape - previous_wape
    (positive means worsening).

    monitoring_status:
        "ok"                — recent_wape ≤ previous_wape + DEGRADED_WAPE_THRESHOLD
        "degraded"          — recent_wape > previous_wape + DEGRADED_WAPE_THRESHOLD
        "insufficient_data" — fewer than MIN_PERIODS_FOR_COMPARISON runs with actuals
        "no_actuals"        — all actuals are zero across all runs

    Parameters
    ----------
    df:
        Normalized realized forecast history.

    Returns
    -------
    DataFrame with columns ``_REPORT_COLS``, sorted by ``report_id``.
    """
    validate_realized_history_input(df)
    if df.empty:
        return pd.DataFrame(columns=_REPORT_COLS)

    df = df.copy()
    df["actual"]   = pd.to_numeric(df["actual"],   errors="coerce")
    df["forecast"] = pd.to_numeric(df["forecast"], errors="coerce")

    rows = []
    for report_id, grp in df.groupby("report_id", sort=True):
        metrics = _agg_metrics(grp)
        production_run_count = grp["run_id"].nunique()
        realized_count = len(grp)

        # Recent vs previous WAPE — compare the two most recent run_ids
        # (sorted lexicographically, which matches the timestamp-prefix convention)
        sorted_runs = sorted(grp["run_id"].unique())
        recent_wape   = np.nan
        previous_wape = np.nan
        accuracy_change = np.nan
        monitoring_status: str

        if metrics["wape"] is None or np.isnan(
            grp["actual"].sum()
        ) or grp["actual"].sum() == 0:
            monitoring_status = "no_actuals"
        elif len(sorted_runs) < MIN_PERIODS_FOR_COMPARISON:
            monitoring_status = "insufficient_data"
        else:
            recent_run   = grp[grp["run_id"] == sorted_runs[-1]]
            previous_run = grp[grp["run_id"] == sorted_runs[-2]]

            recent_a  = pd.to_numeric(recent_run["actual"],   errors="coerce").to_numpy(float)
            recent_f  = pd.to_numeric(recent_run["forecast"], errors="coerce").to_numpy(float)
            prev_a    = pd.to_numeric(previous_run["actual"],  errors="coerce").to_numpy(float)
            prev_f    = pd.to_numeric(previous_run["forecast"],errors="coerce").to_numpy(float)

            rv = np.isfinite(recent_a) & np.isfinite(recent_f)
            pv = np.isfinite(prev_a)   & np.isfinite(prev_f)

            r_wape = calculate_point_metrics(recent_a[rv], recent_f[rv])["wape"] if rv.any() else np.nan
            p_wape = calculate_point_metrics(prev_a[pv],   prev_f[pv])["wape"]   if pv.any() else np.nan

            recent_wape   = r_wape
            previous_wape = p_wape

            if np.isnan(r_wape) or np.isnan(p_wape):
                monitoring_status = "insufficient_data"
                accuracy_change = np.nan
            else:
                accuracy_change = r_wape - p_wape
                if accuracy_change > DEGRADED_WAPE_THRESHOLD:
                    monitoring_status = "degraded"
                else:
                    monitoring_status = "ok"

        rows.append({
            "report_id":                 report_id,
            "production_run_count":      production_run_count,
            "realized_prediction_count": realized_count,
            **metrics,
            "recent_wape":       recent_wape,
            "previous_wape":     previous_wape,
            "accuracy_change":   accuracy_change,
            "monitoring_status": monitoring_status,
        })

    out = pd.DataFrame(rows)
    out = out.sort_values("report_id", ignore_index=True)
    return out[_REPORT_COLS]


# ---------------------------------------------------------------------------
# Table 3: realized_performance_by_horizon
# ---------------------------------------------------------------------------

def realized_performance_by_horizon(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate realized forecast performance by (report_id, horizon_bucket).

    Horizon buckets use the same boundaries as
    ``src.models.horizon_evaluation.HORIZON_BUCKETS``:
        days_1_7     — horizon_step  1 –  7
        days_8_14    — horizon_step  8 – 14
        days_15_28   — horizon_step 15 – 28
        full_horizon — horizon_step  1 – 28

    Parameters
    ----------
    df:
        Normalized realized forecast history.

    Returns
    -------
    DataFrame with columns ``_HORIZON_COLS``, sorted by
    (report_id, horizon_bucket) with bucket order matching ``HORIZON_BUCKETS``.
    """
    validate_realized_history_input(df)
    if df.empty:
        return pd.DataFrame(columns=_HORIZON_COLS)

    df = df.copy()
    df["horizon_step"] = pd.to_numeric(df["horizon_step"], errors="coerce")
    df["actual"]   = pd.to_numeric(df["actual"],   errors="coerce")
    df["forecast"] = pd.to_numeric(df["forecast"], errors="coerce")

    # Explode rows into all matching buckets (a step may match multiple buckets,
    # e.g. step=1 falls into both days_1_7 and full_horizon)
    df = _explode_horizon_buckets(df)

    rows = []
    for (report_id, bucket), grp in df.groupby(
        ["report_id", "horizon_bucket"], sort=True
    ):
        metrics = _agg_metrics(grp)
        rows.append({
            "report_id":        report_id,
            "horizon_bucket":   bucket,
            "observation_count": len(grp),
            "mae":              metrics["mae"],
            "rmse":             metrics["rmse"],
            "wape":             metrics["wape"],
            "bias":             metrics["bias"],
            "interval_coverage":   metrics["interval_coverage"],
            "mean_interval_width": metrics["mean_interval_width"],
        })

    if not rows:
        return pd.DataFrame(columns=_HORIZON_COLS)

    out = pd.DataFrame(rows)

    # Deterministic sort: report_id alphabetically, then bucket in canonical order
    bucket_cat = pd.CategoricalDtype(categories=_BUCKET_ORDER, ordered=True)
    out["horizon_bucket"] = out["horizon_bucket"].astype(bucket_cat)
    out = out.sort_values(["report_id", "horizon_bucket"], ignore_index=True)
    out["horizon_bucket"] = out["horizon_bucket"].astype(str)

    return out[_HORIZON_COLS]


# ---------------------------------------------------------------------------
# Table 4: realized_performance_by_model
# ---------------------------------------------------------------------------

def realized_performance_by_model(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate realized forecast performance by (selected_model_family, selected_m).

    Parameters
    ----------
    df:
        Normalized realized forecast history.

    Returns
    -------
    DataFrame with columns ``_MODEL_COLS``, sorted by
    (selected_model_family, selected_m).
    """
    validate_realized_history_input(df)
    if df.empty:
        return pd.DataFrame(columns=_MODEL_COLS)

    df = df.copy()
    df["selected_m"] = pd.to_numeric(df["selected_m"], errors="coerce")
    df["actual"]   = pd.to_numeric(df["actual"],   errors="coerce")
    df["forecast"] = pd.to_numeric(df["forecast"], errors="coerce")

    rows = []
    for (family, m), grp in df.groupby(
        ["selected_model_family", "selected_m"], sort=True
    ):
        metrics = _agg_metrics(grp)
        rows.append({
            "selected_model_family":     family,
            "selected_m":                int(m) if not np.isnan(m) else np.nan,
            "report_count":              grp["report_id"].nunique(),
            "production_run_count":      grp["run_id"].nunique(),
            "realized_prediction_count": len(grp),
            "mae":              metrics["mae"],
            "rmse":             metrics["rmse"],
            "wape":             metrics["wape"],
            "bias":             metrics["bias"],
            "interval_coverage":   metrics["interval_coverage"],
            "mean_interval_width": metrics["mean_interval_width"],
        })

    out = pd.DataFrame(rows)
    out = out.sort_values(
        ["selected_model_family", "selected_m"], ignore_index=True
    )
    return out[_MODEL_COLS]


# ---------------------------------------------------------------------------
# Combined builder
# ---------------------------------------------------------------------------

def build_production_performance_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build all monitoring tables from the realized forecast history.

    Parameters
    ----------
    df:
        Normalized realized forecast history (``REALIZED_FORECAST_HISTORY_COLS``).

    Returns
    -------
    dict with keys:
        ``by_run``        → realized_performance_by_run DataFrame
        ``by_report``     → realized_performance_by_report DataFrame
        ``by_horizon``    → realized_performance_by_horizon DataFrame
        ``by_model``      → realized_performance_by_model DataFrame
        ``deterioration`` → cross-run deterioration report DataFrame
    """
    from src.monitoring.deterioration import compute_deterioration_report

    by_run    = realized_performance_by_run(df)
    by_report = realized_performance_by_report(df)
    return {
        "by_run":        by_run,
        "by_report":     by_report,
        "by_horizon":    realized_performance_by_horizon(df),
        "by_model":      realized_performance_by_model(df),
        "deterioration": compute_deterioration_report(by_run, by_report),
    }


# ---------------------------------------------------------------------------
# Pipeline I/O helper
# ---------------------------------------------------------------------------

def save_production_performance(
    tables: dict[str, pd.DataFrame],
    project_root: Path,
) -> dict[str, Optional[Path]]:
    """Write monitoring tables to ``outputs/monitoring/`` and metrics copies.

    Called by the pipeline after realized history has been updated.
    Each table is written as a CSV overwrite (these are derived views, not
    append-only history; they are always recomputed from the full history).

    The ``by_horizon`` table is additionally written to
    ``outputs/metrics/realized_performance_by_horizon.csv`` so that backtest
    and production horizon results live in the same directory and can be
    compared without navigating between ``monitoring/`` and ``metrics/``.

    Parameters
    ----------
    tables:
        Output of ``build_production_performance_tables``.
    project_root:
        Repository root.

    Returns
    -------
    dict mapping table name → output Path (or None if table is empty).
    Includes an extra ``"realized_horizon_metrics"`` key for the metrics-dir copy.
    """
    from src.models.horizon_output_validation import validate_realized_horizon_output

    out_dir = project_root / "outputs" / "monitoring"
    out_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir = project_root / "outputs" / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    name_map = {
        "by_run":        "realized_performance_by_run.csv",
        "by_report":     "realized_performance_by_report.csv",
        "by_horizon":    "realized_performance_by_horizon.csv",
        "by_model":      "realized_performance_by_model.csv",
        "deterioration": "deterioration_report.csv",
    }

    paths: dict[str, Optional[Path]] = {}
    for key, filename in name_map.items():
        tbl = tables.get(key, pd.DataFrame())
        if tbl is not None and not tbl.empty:
            path = out_dir / filename
            tbl.to_csv(path, index=False)
            paths[key] = path
        else:
            paths[key] = None

    # Mirror the realized horizon table to outputs/metrics/ so production and
    # backtest horizon outputs share one directory for easy comparison.
    horizon_tbl = tables.get("by_horizon", pd.DataFrame())
    if horizon_tbl is not None and not horizon_tbl.empty:
        try:
            validate_realized_horizon_output(horizon_tbl)
        except ValueError:
            pass  # validation errors are non-fatal for the write
        metrics_path = metrics_dir / "realized_performance_by_horizon.csv"
        horizon_tbl.to_csv(metrics_path, index=False)
        paths["realized_horizon_metrics"] = metrics_path
    else:
        paths["realized_horizon_metrics"] = None

    return paths


def update_production_performance(project_root: Path) -> dict[str, object]:
    """Load realized history, build tables, write to disk.

    This is the single entry point called by the pipeline after
    ``update_realized_forecast_history`` completes.

    Parameters
    ----------
    project_root:
        Repository root.

    Returns
    -------
    dict with keys ``tables`` (dict of DataFrames) and ``paths``
    (dict of output Paths).
    """
    df = load_realized_forecast_history(project_root)
    tables = build_production_performance_tables(df)
    paths = save_production_performance(tables, project_root)
    return {"tables": tables, "paths": paths}
