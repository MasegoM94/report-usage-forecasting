"""Build and persist the per-report monitoring context mart.

Output
------
outputs/monitoring/mart_report_monitoring_context.csv

Grain: one row per (monitoring_run_id, report_id).

The table is a wide denormalised view suitable for Streamlit displays and
GenAI prompt construction.  It joins eight source tables, all of which are
optional — absent sources produce null columns rather than dropped rows.

Source tables and their join keys
----------------------------------
production_forecast_df       run_id, report_id      (anchor — always required)
perf_by_report_df            report_id
deterioration_df             report_id
report_features_df           report_id
feature_context_df           report_id              (mart_report_daily_context)
segments_df                  report_id
diagnostics_df               report_id

Join cardinality rules
----------------------
* The anchor table (production forecast) is reduced to one row per report_id
  by taking the most-recent run_id and summing forecast values.
* All other joins are LEFT on report_id (m:1 or 1:1).  Cardinality violations
  are logged and deduplicated rather than silently hidden.

Null semantics
--------------
The ``context_status`` column records per-row data coverage:

    "ok"                    — all major sections are populated
    "no_actuals_yet"        — perf_by_report is absent (monitoring never ran)
    "zero_actual_usage"     — realized actuals are all zero for this report
    "insufficient_evidence" — deterioration cannot be assessed (too few runs)
    "missing_context"       — report_features or segment/diagnostic data absent

Nulls are NOT back-filled with zero.  Callers must check ``context_status``
before treating a null production_wape as meaning "0% error".

Public API
----------
OUTPUT_COLS: list[str]
    Canonical column order for the output file.

build_report_monitoring_context(production_forecast_df, ...) -> pd.DataFrame
    Pure builder — no file I/O.

validate_report_monitoring_context(df) -> None
    Raises ValueError when one-row-per-(run_id, report_id) invariant is violated
    or required identity columns are missing.

save_report_monitoring_context(df, project_root) -> Path
    Write to outputs/monitoring/mart_report_monitoring_context.csv.

update_report_monitoring_context(project_root) -> dict
    Pipeline entry point: load sources, build, validate, save.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Output schema
# ---------------------------------------------------------------------------

OUTPUT_COLS: list[str] = [
    # Identity
    "monitoring_run_id",
    "generated_at",
    "report_id",
    "report_name",
    # Forecast summary
    "selected_model_family",
    "selected_model_name",
    "selected_m",
    "forecast_28d_views",
    "lower_28d_total",
    "upper_28d_total",
    # Backtest performance
    "median_backtest_mase",
    "mean_backtest_wape",
    "mean_backtest_bias",
    "valid_backtest_folds",
    "fold_win_rate",
    # Production monitoring
    "production_wape",
    "production_bias",
    "production_interval_coverage",
    "realized_prediction_count",
    "recent_wape",
    "previous_wape",
    "accuracy_deterioration_flag",
    "deterioration_reasons",
    "monitoring_status",
    # Report context
    "usage_change_28d_pct",
    "top_1_user_view_share",
    "top_10pct_user_share",
    "report_segment",
    "main_diagnostic",
    "diagnostic_summary",
    # Coverage flag
    "context_status",
]

# Identity columns that must always be non-null
_REQUIRED_IDENTITY_COLS: set[str] = {"monitoring_run_id", "report_id"}

# Unique key for cardinality validation
_UNIQUE_KEY: list[str] = ["monitoring_run_id", "report_id"]


# ---------------------------------------------------------------------------
# Public builder
# ---------------------------------------------------------------------------

def build_report_monitoring_context(
    production_forecast_df: pd.DataFrame,
    perf_by_report_df: Optional[pd.DataFrame] = None,
    deterioration_df: Optional[pd.DataFrame] = None,
    report_features_df: Optional[pd.DataFrame] = None,
    feature_context_df: Optional[pd.DataFrame] = None,
    segments_df: Optional[pd.DataFrame] = None,
    diagnostics_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Build the per-report monitoring context DataFrame.

    Parameters
    ----------
    production_forecast_df:
        Row-per-(run_id, report_id, horizon_step) forecast output from
        ``build_production_forecast`` / ``save_production_outputs``.
        Required columns: ``run_id``, ``report_id``, ``generated_at``,
        ``selected_model_family``, ``selected_model_name``, ``selected_m``,
        ``forecast``, ``lower_bound``, ``upper_bound``,
        ``median_backtest_mase``, ``mean_backtest_wape``, ``mean_backtest_bias``,
        ``valid_backtest_folds``.
        Rows for all reports in the most-recent run are included; reports from
        older runs that are not in the latest run are excluded.
    perf_by_report_df:
        Output of ``realized_performance_by_report`` (monitoring table).
        Optional; when absent all production monitoring fields are null.
    deterioration_df:
        Output of ``compute_deterioration_report`` (one row per report_id).
        Optional; when absent accuracy_deterioration_flag and
        deterioration_reasons are null.
    report_features_df:
        Flat report-level feature file (``outputs/metrics/report_features.csv``
        or equivalent).  Provides ``usage_change_28d_pct``.
    feature_context_df:
        Row-per-(report_id, date) context mart
        (``data/processed/mart_report_daily_context.csv``).
        The most-recent row per report is used to extract
        ``top_1_user_view_share`` and ``top_10pct_user_share``.
    segments_df:
        Report segment table.  Provides ``report_segment``.
    diagnostics_df:
        Report diagnostics table.  Provides ``main_diagnostic`` and
        ``diagnostic_summary``.

    Returns
    -------
    pd.DataFrame in ``OUTPUT_COLS`` column order, sorted by
    (monitoring_run_id, report_id).  One row per (monitoring_run_id, report_id).

    Raises
    ------
    ValueError
        When ``production_forecast_df`` is empty or missing required columns.
    """
    if production_forecast_df is None or production_forecast_df.empty:
        raise ValueError(
            "production_forecast_df is required and must not be empty. "
            "The monitoring context cannot be built without at least one "
            "production forecast row."
        )

    _check_required_forecast_cols(production_forecast_df)

    # ------------------------------------------------------------------
    # Step 1: Anchor — one row per report_id from the most-recent run.
    # ------------------------------------------------------------------
    anchor = _build_anchor(production_forecast_df)
    logger.info(
        "monitoring_context | anchor=%d reports | run_id(s)=%s",
        len(anchor),
        anchor["monitoring_run_id"].unique().tolist(),
    )

    # ------------------------------------------------------------------
    # Step 2: LEFT-join production monitoring (by_report).
    # ------------------------------------------------------------------
    anchor = _left_join_deduped(
        anchor,
        _prepare_perf_by_report(perf_by_report_df),
        on="report_id",
        source_label="perf_by_report",
    )

    # ------------------------------------------------------------------
    # Step 3: LEFT-join deterioration indicators.
    # ------------------------------------------------------------------
    anchor = _left_join_deduped(
        anchor,
        _prepare_deterioration(deterioration_df),
        on="report_id",
        source_label="deterioration",
    )

    # ------------------------------------------------------------------
    # Step 4: LEFT-join report features (usage_change_28d_pct).
    # ------------------------------------------------------------------
    anchor = _left_join_deduped(
        anchor,
        _prepare_report_features(report_features_df),
        on="report_id",
        source_label="report_features",
    )

    # ------------------------------------------------------------------
    # Step 5: LEFT-join concentration fields from the context mart.
    # ------------------------------------------------------------------
    anchor = _left_join_deduped(
        anchor,
        _prepare_feature_context(feature_context_df),
        on="report_id",
        source_label="feature_context",
    )

    # ------------------------------------------------------------------
    # Step 6: LEFT-join segment.
    # ------------------------------------------------------------------
    anchor = _left_join_deduped(
        anchor,
        _prepare_segments(segments_df),
        on="report_id",
        source_label="segments",
    )

    # ------------------------------------------------------------------
    # Step 7: LEFT-join diagnostics.
    # ------------------------------------------------------------------
    anchor = _left_join_deduped(
        anchor,
        _prepare_diagnostics(diagnostics_df),
        on="report_id",
        source_label="diagnostics",
    )

    # ------------------------------------------------------------------
    # Step 8: Stamp context_status per row.
    # ------------------------------------------------------------------
    anchor["context_status"] = anchor.apply(_classify_context_status, axis=1)

    # ------------------------------------------------------------------
    # Step 9: Project to output schema, sort.
    # ------------------------------------------------------------------
    out = _project(anchor)
    return out.sort_values(_UNIQUE_KEY, ignore_index=True)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_report_monitoring_context(df: pd.DataFrame) -> None:
    """Raise ValueError when the output DataFrame violates schema invariants.

    Checks
    ------
    * Required identity columns are present and non-null.
    * Exactly one row per (monitoring_run_id, report_id).
    """
    missing_id = _REQUIRED_IDENTITY_COLS - set(df.columns)
    if missing_id:
        raise ValueError(
            f"mart_report_monitoring_context is missing identity column(s): "
            f"{sorted(missing_id)}"
        )

    for col in _REQUIRED_IDENTITY_COLS:
        n_null = int(df[col].isna().sum())
        if n_null:
            raise ValueError(
                f"mart_report_monitoring_context has {n_null} null value(s) in "
                f"identity column '{col}'. Every row must have a non-null "
                f"monitoring_run_id and report_id."
            )

    dupes = df.duplicated(subset=_UNIQUE_KEY, keep=False)
    if dupes.any():
        n = int(dupes.sum())
        raise ValueError(
            f"mart_report_monitoring_context has {n} duplicate row(s) on "
            f"key {_UNIQUE_KEY}. Expected exactly one row per "
            "(monitoring_run_id, report_id)."
        )


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def save_report_monitoring_context(
    df: pd.DataFrame,
    project_root: Path,
) -> Path:
    """Write the monitoring context to outputs/monitoring/.

    The file is always overwritten (it is a derived view recomputed each run).

    Parameters
    ----------
    df:
        Output of ``build_report_monitoring_context``.
    project_root:
        Repository root.

    Returns
    -------
    Absolute path to the written file.
    """
    out_dir = project_root / "outputs" / "monitoring"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "mart_report_monitoring_context.csv"
    df.to_csv(path, index=False)
    logger.info(
        "monitoring_context | saved %d rows to %s",
        len(df),
        path.relative_to(project_root),
    )
    return path


def update_report_monitoring_context(project_root: Path) -> dict:
    """Load source files, build, validate, and save the monitoring context.

    This is the pipeline entry point.  It reads all source files from disk
    and delegates to ``build_report_monitoring_context``.

    Returns
    -------
    dict with keys:
        ``"df"``   — the built DataFrame (or empty DataFrame on error)
        ``"path"`` — output path (or None on error)
        ``"status"`` — "ok", "no_production_forecast", or "error"
        ``"n_reports"`` — number of report rows written
    """
    from src.monitoring.report_monitoring_context_loader import load_monitoring_sources

    sources = load_monitoring_sources(project_root)

    prod_df = sources.get("production_forecast_df")
    if prod_df is None or prod_df.empty:
        logger.warning(
            "monitoring_context | no production forecast found; skipping context build"
        )
        return {
            "df": pd.DataFrame(columns=OUTPUT_COLS),
            "path": None,
            "status": "no_production_forecast",
            "n_reports": 0,
        }

    try:
        df = build_report_monitoring_context(**sources)
        validate_report_monitoring_context(df)
        path = save_report_monitoring_context(df, project_root)
        return {
            "df": df,
            "path": path,
            "status": "ok",
            "n_reports": len(df),
        }
    except Exception as exc:
        logger.error("monitoring_context | build failed: %s", exc)
        return {
            "df": pd.DataFrame(columns=OUTPUT_COLS),
            "path": None,
            "status": "error",
            "n_reports": 0,
        }


# ---------------------------------------------------------------------------
# Step 1 helpers — anchor construction
# ---------------------------------------------------------------------------

_REQUIRED_FORECAST_COLS: set[str] = {
    "run_id", "report_id", "generated_at",
    "selected_model_family", "selected_model_name", "selected_m",
    "forecast",
}


def _check_required_forecast_cols(df: pd.DataFrame) -> None:
    missing = _REQUIRED_FORECAST_COLS - set(df.columns)
    if missing:
        raise ValueError(
            f"production_forecast_df is missing required column(s): {sorted(missing)}"
        )


def _build_anchor(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse production forecast rows to one per report_id from the latest run.

    Selects the most-recent ``run_id`` (lexicographic max — run_ids are
    timestamp-prefixed).  Sums forecast/bound values across horizon steps.
    Carries forward report-level lineage columns from the first row per report.
    """
    df = df.copy()

    # Most-recent run per report (a report may appear in multiple runs)
    latest_run = df.groupby("report_id")["run_id"].max().rename("monitoring_run_id")
    df = df.merge(latest_run, on="report_id")
    df = df[df["run_id"] == df["monitoring_run_id"]]

    # Report-level scalars: take first row per report_id
    lineage_cols = [
        "report_id", "monitoring_run_id",
        "generated_at",
        "selected_model_family", "selected_model_name", "selected_m",
    ]
    # Add optional lineage columns when present
    optional_lineage = [
        "report_name",
        "median_backtest_mase", "mean_backtest_wape", "mean_backtest_bias",
        "valid_backtest_folds",
    ]
    for col in optional_lineage:
        if col in df.columns:
            lineage_cols.append(col)

    first_rows = (
        df.sort_values(["report_id", "horizon_step"] if "horizon_step" in df.columns else ["report_id"])
        .groupby("report_id", sort=False)
        .first()
        .reset_index()[lineage_cols]
    )

    # Aggregate forecast totals across horizon steps
    agg_cols: dict = {}
    if "forecast" in df.columns:
        agg_cols["forecast_28d_views"] = ("forecast", "sum")
    if "lower_bound" in df.columns:
        agg_cols["lower_28d_total"] = ("lower_bound", "sum")
    if "upper_bound" in df.columns:
        agg_cols["upper_28d_total"] = ("upper_bound", "sum")

    # fold_win_rate: report-level scalar, not aggregated — take first row value
    if "fold_win_rate" in df.columns:
        agg_cols["fold_win_rate"] = ("fold_win_rate", "first")

    if agg_cols:
        agg = df.groupby("report_id").agg(**agg_cols).reset_index()
        anchor = first_rows.merge(agg, on="report_id", how="left")
    else:
        anchor = first_rows

    return anchor


# ---------------------------------------------------------------------------
# Step 2–7 helpers — source preparation
# ---------------------------------------------------------------------------

def _safe_dedup(df: pd.DataFrame, key: str, source_label: str) -> pd.DataFrame:
    """Deduplicate on key, logging if any rows were dropped."""
    if df.empty or key not in df.columns:
        return df
    n_before = len(df)
    df = df.drop_duplicates(subset=[key], keep="last")
    n_dropped = n_before - len(df)
    if n_dropped:
        logger.warning(
            "monitoring_context | %s: dropped %d duplicate(s) on '%s'",
            source_label, n_dropped, key,
        )
    return df


def _left_join_deduped(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str,
    source_label: str,
) -> pd.DataFrame:
    """Deduplicate right on join key then left-join, with suffix handling."""
    if right.empty:
        return left

    right = _safe_dedup(right, on, source_label)

    # Avoid column collisions: only bring in columns not already in left
    new_cols = [c for c in right.columns if c == on or c not in left.columns]
    right = right[new_cols]

    return left.merge(right, on=on, how="left")


def _prepare_perf_by_report(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    cols = {
        "report_id":                 "report_id",
        "wape":                      "production_wape",
        "bias":                      "production_bias",
        "interval_coverage":         "production_interval_coverage",
        "realized_prediction_count": "realized_prediction_count",
        "recent_wape":               "recent_wape",
        "previous_wape":             "previous_wape",
        "monitoring_status":         "monitoring_status",
    }
    present = {src: dst for src, dst in cols.items() if src in df.columns}
    out = df[list(present.keys())].rename(columns=present)
    return out


def _prepare_deterioration(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    cols = {
        "report_id":               "report_id",
        "accuracy_deterioration_flag": "accuracy_deterioration_flag",
        "deterioration_reasons":   "deterioration_reasons",
    }
    present = {src: dst for src, dst in cols.items() if src in df.columns}
    out = df[list(present.keys())].rename(columns=present)
    return out


def _prepare_report_features(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df[["report_id"]].copy()

    # usage_change_28d_pct: use the explicit 28-day field if available, otherwise
    # fall back to the generic usage_change_pct (computed over the analysis window).
    if "usage_change_28d_pct" in df.columns:
        out["usage_change_28d_pct"] = df["usage_change_28d_pct"].values
    elif "usage_change_pct" in df.columns:
        out["usage_change_28d_pct"] = df["usage_change_pct"].values
    else:
        out["usage_change_28d_pct"] = np.nan

    # report_name falls back from features if not already in anchor
    if "report_name" in df.columns:
        out["report_name"] = df["report_name"].values

    return out


def _prepare_feature_context(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Take the most-recent row per report from the rolling context mart."""
    if df is None or df.empty:
        return pd.DataFrame()

    # Identify the date column
    date_col = None
    for candidate in ("date", "Date", "target_date"):
        if candidate in df.columns:
            date_col = candidate
            break

    if date_col is not None:
        df = (
            df.assign(**{date_col: pd.to_datetime(df[date_col], errors="coerce")})
            .sort_values(date_col)
            .groupby("report_id", sort=False)
            .last()
            .reset_index()
        )
    else:
        df = df.drop_duplicates(subset=["report_id"], keep="last")

    wanted = ["report_id"]
    for col in ("top_1_user_view_share", "top_10pct_user_share"):
        if col in df.columns:
            wanted.append(col)

    return df[wanted]


def _prepare_segments(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    cols = {"report_id": "report_id", "report_segment": "report_segment"}
    present = {src: dst for src, dst in cols.items() if src in df.columns}
    return df[list(present.keys())].rename(columns=present)


def _prepare_diagnostics(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    cols = {
        "report_id":          "report_id",
        "main_diagnostic":    "main_diagnostic",
        "diagnostic_summary": "diagnostic_summary",
    }
    present = {src: dst for src, dst in cols.items() if src in df.columns}
    return df[list(present.keys())].rename(columns=present)


# ---------------------------------------------------------------------------
# Step 8 — context_status classification
# ---------------------------------------------------------------------------

def _classify_context_status(row: pd.Series) -> str:
    """Return a single context_status string for a merged row.

    Priority (first match wins):
    1. no_actuals_yet     — production monitoring has never run for this report
    2. zero_actual_usage  — realized actuals exist but are all zero
    3. insufficient_evidence — deterioration cannot be assessed
    4. missing_context    — segment/diagnostic/feature data absent
    5. ok
    """
    monitoring_status = row.get("monitoring_status")
    if pd.isna(monitoring_status):
        # perf_by_report was either absent or had no row for this report
        return "no_actuals_yet"

    if str(monitoring_status) == "no_actuals":
        return "zero_actual_usage"

    evidence = row.get("accuracy_deterioration_flag")
    if pd.isna(evidence):
        det_reasons = row.get("deterioration_reasons")
        # deterioration df was absent entirely
        if pd.isna(det_reasons) or det_reasons == [] or det_reasons == "[]":
            evidence_status_field = row.get("evidence_status")
            if pd.isna(evidence_status_field):
                # deterioration table not joined at all
                pass
            elif str(evidence_status_field) == "insufficient_evidence":
                return "insufficient_evidence"

    has_segment = not pd.isna(row.get("report_segment"))
    has_diagnostic = not pd.isna(row.get("main_diagnostic"))
    has_features = (
        not pd.isna(row.get("usage_change_28d_pct"))
        or not pd.isna(row.get("top_1_user_view_share"))
    )

    if not (has_segment or has_diagnostic or has_features):
        return "missing_context"

    return "ok"


# ---------------------------------------------------------------------------
# Step 9 — schema projection
# ---------------------------------------------------------------------------

def _project(df: pd.DataFrame) -> pd.DataFrame:
    """Reindex to OUTPUT_COLS, adding null columns for any that are absent."""
    for col in OUTPUT_COLS:
        if col not in df.columns:
            df[col] = np.nan
    return df[OUTPUT_COLS]
