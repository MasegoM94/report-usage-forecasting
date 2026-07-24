"""Run the report usage forecasting baseline pipeline.

This script mirrors the production-facing flow in
``notebooks/05_forecasting_baseline.ipynb``: load the processed forecasting
feature mart, run per-report data sufficiency checks, compare SARIMA against
simple baselines, and publish latest plus history outputs.

Usage:
    python -m src.pipelines.run_forecasting_pipeline
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from src.config.forecasting import FORECAST_HORIZON_DAYS, SEASONAL_CANDIDATES
from src.models.metrics import calculate_point_metrics


DATE_COL = "Date"
REPORT_ID_COL = "Report Guid"
REPORT_NAME_COL = "Report Name"
USER_COL = "User Id"
VIEWS_COL = "Occurrences"

STANDARD_DATE_COL = "date"
STANDARD_REPORT_ID_COL = "report_id"
STANDARD_TARGET_COL = "daily_views"
REQUIRED_FEATURE_COLUMNS = {
    STANDARD_DATE_COL,
    STANDARD_REPORT_ID_COL,
    STANDARD_TARGET_COL,
}

MIN_DAYS = 90
MIN_NONZERO_DAYS = 35
MIN_NONZERO_RATIO = 0.25
MIN_TOTAL_VIEWS = 120

TRAIN_TEST_FRACTION = 0.8
MAX_MAE_RATIO_TO_MEAN = 0.6
MAX_SELECTED_WAPE = 0.75
MAX_ZERO_SHARE = 0.75
MAE_TIE_TOLERANCE = 0.01

MODEL_NAME = "AutoARIMA_m7_daily_report_views"

EXPECTED_FORECAST_COLUMNS = [
    DATE_COL,
    "forecast",
    "lower_ci",
    "upper_ci",
    "actual",
    "error",
    "abs_error",
    "pct_error",
    "ReportId",
    "ReportName",
    "IsForecast",
    "ModelRunTimestamp",
    "ModelName",
    "run_id",
    "run_timestamp",
    "IsPlaceholderRow",
]

EXPECTED_MODEL_COMPARISON_COLUMNS = [
    "report_id",
    "report_name",
    "model_name",
    "mae",
    "rmse",
    "wape",
    "selected_model_flag",
    "forecast_reliable",
]


def get_project_root() -> Path:
    """Return the repository root based on this pipeline file location."""
    return Path(__file__).resolve().parents[2]


def list_processed_csvs(processed_dir: Path) -> list[Path]:
    """Return available processed CSV files for transparent fallback selection."""
    return sorted(processed_dir.glob("*.csv"))


def load_canonical_daily_series(processed_dir: Path) -> pd.DataFrame:
    """Load ``mart_report_daily_series.csv`` — the only accepted forecasting input.

    Raises ``FileNotFoundError`` if the canonical file is absent.  Does not
    fall back to any other table.  Does not guess column aliases.  Does not
    aggregate rows.

    Parameters
    ----------
    processed_dir:
        ``data/processed/`` directory that must contain
        ``mart_report_daily_series.csv``.

    Returns
    -------
    pd.DataFrame
        Raw contents of ``mart_report_daily_series.csv`` with ``date`` parsed
        as ``datetime64``.  Columns: at minimum ``report_id``, ``date``,
        ``daily_views`` (plus optional indicator columns).

    Raises
    ------
    FileNotFoundError
        When ``mart_report_daily_series.csv`` is not present.
    ValueError
        When the file is present but is missing one or more of
        ``report_id``, ``date``, ``daily_views``.
    """
    path = processed_dir / "mart_report_daily_series.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Canonical forecasting input not found: {path}\n"
            "Run notebooks/04_feature_engineering.ipynb (or "
            "src/features/report_features.build_report_daily_series) to produce "
            "mart_report_daily_series.csv before running the forecasting pipeline."
        )
    df = pd.read_csv(path)
    missing = [
        c for c in [STANDARD_DATE_COL, STANDARD_REPORT_ID_COL, STANDARD_TARGET_COL]
        if c not in df.columns
    ]
    if missing:
        raise ValueError(
            f"mart_report_daily_series.csv is missing required columns: {missing}. "
            f"Present columns: {list(df.columns)}. "
            "Re-run the feature-engineering step to regenerate the file."
        )
    df[STANDARD_DATE_COL] = pd.to_datetime(df[STANDARD_DATE_COL])
    return df


def validate_forecasting_series_input(df: pd.DataFrame) -> None:
    """Validate the standardised ``[date, report_id, daily_views]`` frame.

    Called immediately after ``standardise_forecasting_columns`` and before any
    model fitting.  Every check raises ``ValueError`` on failure — there are no
    silent warnings.  ``mart_report_daily_series`` is treated as authoritative:
    the forecasting layer must never manufacture or fill missing dates.

    Checks
    ------
    1. Required columns present.
    2. No null ``daily_views``.
    3. No duplicate ``(report_id, date)`` pairs.
    4. Per-report dates are sorted.
    5. Per-report dates are contiguous (no calendar-day gaps).
    6. ``daily_views`` is non-negative.
    7. ``daily_views`` is integer-valued.

    Parameters
    ----------
    df:
        Standardised DataFrame with columns ``date``, ``report_id``,
        ``daily_views``.  The ``date`` column must already be parsed as
        ``datetime64``.

    Raises
    ------
    ValueError
        On any structural or value violation.  Gap errors identify the
        report_id, the missing date, and the two neighbouring dates.
    """
    missing = [c for c in [STANDARD_DATE_COL, STANDARD_REPORT_ID_COL, STANDARD_TARGET_COL]
               if c not in df.columns]
    if missing:
        raise ValueError(
            f"Forecasting input is missing required columns: {missing}"
        )

    null_target = int(df[STANDARD_TARGET_COL].isna().sum())
    if null_target:
        raise ValueError(
            f"Forecasting input has {null_target} null {STANDARD_TARGET_COL} values. "
            "Run the feature-engineering step to produce a zero-filled series."
        )

    dup_count = int(
        df.duplicated([STANDARD_DATE_COL, STANDARD_REPORT_ID_COL]).sum()
    )
    if dup_count:
        raise ValueError(
            f"Forecasting input has {dup_count} duplicate (report_id, date) pairs. "
            "Aggregate or deduplicate before fitting."
        )

    for rid, grp in df.groupby(STANDARD_REPORT_ID_COL, sort=False):
        dates = grp[STANDARD_DATE_COL].reset_index(drop=True)

        # Check 4 — sorted
        if not dates.is_monotonic_increasing:
            raise ValueError(
                f"Report {rid!r}: dates are not sorted in ascending order. "
                "Sort the series before passing it to the forecasting pipeline."
            )

        # Check 5 — contiguous (evaluated on already-sorted dates)
        diffs = dates.diff().dropna()
        bad = diffs[diffs != pd.Timedelta("1D")]
        if not bad.empty:
            gap_pos = bad.index[0]
            before = dates.iloc[gap_pos - 1]
            after = dates.iloc[gap_pos]
            expected = before + pd.Timedelta("1D")
            raise ValueError(
                f"Report {rid!r} has a missing date: expected {expected:%Y-%m-%d} "
                f"but the series jumps from {before:%Y-%m-%d} to {after:%Y-%m-%d}. "
                "Re-run the feature-engineering step (build_report_daily_series) to "
                "produce a complete daily series — the forecasting layer does not fill gaps."
            )

        # Check 6 — non-negative
        neg_count = int((grp[STANDARD_TARGET_COL] < 0).sum())
        if neg_count:
            raise ValueError(
                f"Report {rid!r}: {neg_count} row(s) have negative {STANDARD_TARGET_COL}."
            )

        # Check 7 — integer-valued
        if not pd.api.types.is_integer_dtype(grp[STANDARD_TARGET_COL]):
            non_whole = grp[STANDARD_TARGET_COL].dropna()
            non_whole = non_whole[non_whole != non_whole.round()]
            if not non_whole.empty:
                raise ValueError(
                    f"Report {rid!r}: {STANDARD_TARGET_COL} contains "
                    f"{len(non_whole)} non-integer value(s)."
                )


def standardise_forecasting_columns(
    df: pd.DataFrame,
    source_path: Path,
    date_dim_path: Path,
) -> pd.DataFrame:
    """Return a ``[date, report_id, daily_views]`` frame from any supported input.

    Strips all engagement and performance columns that are not valid SARIMA
    inputs.  The caller must pass the result to
    ``validate_forecasting_series_input`` before fitting any model.

    Supported inputs
    ----------------
    * ``mart_report_daily_series.csv`` — canonical; already has the right columns.
    * Wide context marts — extra columns are silently dropped.
    * Raw fact tables with legacy column names — renamed and aggregated.
    * Tables with integer ``date_key`` — joined to ``dim_date`` for a parsed date.
    """
    standardised = df.copy()
    rename_map = {}

    if "Date" in standardised.columns and STANDARD_DATE_COL not in standardised.columns:
        rename_map["Date"] = STANDARD_DATE_COL
    if "Report Guid" in standardised.columns and STANDARD_REPORT_ID_COL not in standardised.columns:
        rename_map["Report Guid"] = STANDARD_REPORT_ID_COL
    if "ReportId" in standardised.columns and STANDARD_REPORT_ID_COL not in standardised.columns:
        rename_map["ReportId"] = STANDARD_REPORT_ID_COL
    if "Occurrences" in standardised.columns and STANDARD_TARGET_COL not in standardised.columns:
        rename_map["Occurrences"] = STANDARD_TARGET_COL
    if "view_count" in standardised.columns and STANDARD_TARGET_COL not in standardised.columns:
        rename_map["view_count"] = STANDARD_TARGET_COL
    if "views" in standardised.columns and STANDARD_TARGET_COL not in standardised.columns:
        rename_map["views"] = STANDARD_TARGET_COL

    if rename_map:
        standardised = standardised.rename(columns=rename_map)

    if STANDARD_DATE_COL not in standardised.columns and "date_key" in standardised.columns:
        if not date_dim_path.exists():
            raise FileNotFoundError(
                f"{source_path.name} uses date_key, but {date_dim_path} was not found."
            )
        dim_date = pd.read_csv(date_dim_path, usecols=["date_key", "date"])
        standardised = standardised.merge(dim_date, on="date_key", how="left")

    missing_columns = REQUIRED_FEATURE_COLUMNS - set(standardised.columns)
    if missing_columns:
        raise ValueError(
            f"{source_path.name} is missing required forecasting columns after "
            f"standardisation: {sorted(missing_columns)}"
        )

    standardised = standardised[
        [STANDARD_DATE_COL, STANDARD_REPORT_ID_COL, STANDARD_TARGET_COL]
    ].copy()
    standardised[STANDARD_DATE_COL] = pd.to_datetime(
        standardised[STANDARD_DATE_COL],
        errors="coerce",
    )
    standardised[STANDARD_TARGET_COL] = pd.to_numeric(
        standardised[STANDARD_TARGET_COL],
        errors="coerce",
    ).fillna(0)
    standardised = standardised.dropna(subset=[STANDARD_DATE_COL, STANDARD_REPORT_ID_COL])

    return (
        standardised.groupby(
            [STANDARD_DATE_COL, STANDARD_REPORT_ID_COL],
            as_index=False,
        )[STANDARD_TARGET_COL]
        .sum()
        .sort_values([STANDARD_DATE_COL, STANDARD_REPORT_ID_COL])
    )


def adapt_to_forecasting_schema(features: pd.DataFrame, report_dim_path: Path) -> pd.DataFrame:
    """Translate ``[date, report_id, daily_views]`` to the internal SARIMA schema.

    The internal forecasting functions in this module use legacy column names
    (``Date``, ``Report Guid``, ``Occurrences``) inherited from the original raw
    Power BI export.  This adapter renames the standardised columns and adds a
    dummy ``User Id`` / ``Report Name`` so the rest of the pipeline does not need
    to be changed.

    This is a purely internal translation step.  No model features are added here
    — the only column that reaches pmdarima is ``Occurrences`` (= ``daily_views``).
    """
    model_input = features.copy()

    if report_dim_path.exists():
        report_names = pd.read_csv(report_dim_path, usecols=["report_id", "report_name"])
        model_input = model_input.merge(report_names, on=STANDARD_REPORT_ID_COL, how="left")
    else:
        model_input["report_name"] = model_input[STANDARD_REPORT_ID_COL]

    model_input["report_name"] = model_input["report_name"].fillna(
        model_input[STANDARD_REPORT_ID_COL]
    )
    model_input["synthetic_user_id"] = "PROCESSED_DAILY_TOTAL"

    model_input = model_input.rename(
        columns={
            STANDARD_DATE_COL: DATE_COL,
            STANDARD_REPORT_ID_COL: REPORT_ID_COL,
            "report_name": REPORT_NAME_COL,
            STANDARD_TARGET_COL: VIEWS_COL,
            "synthetic_user_id": USER_COL,
        }
    )
    model_input[VIEWS_COL] = pd.to_numeric(model_input[VIEWS_COL], errors="coerce").fillna(0)
    model_input[DATE_COL] = pd.to_datetime(model_input[DATE_COL])

    return model_input[
        [DATE_COL, REPORT_ID_COL, REPORT_NAME_COL, USER_COL, VIEWS_COL]
    ].sort_values([DATE_COL, REPORT_ID_COL])


def load_forecast_feature_input(project_root: Path) -> tuple[pd.DataFrame, pd.DataFrame, Path]:
    """Load ``mart_report_daily_series.csv`` and validate for SARIMA input.

    Returns
    -------
    daily_series : pd.DataFrame
        ``[date, report_id, daily_views]`` frame (plus optional indicator
        columns) loaded directly from the canonical mart.  No alias guessing
        or aggregation is performed.
    model_input : pd.DataFrame
        Same data translated to the internal legacy schema used by
        ``run_forecasts_for_reports``.
    input_path : Path
        Path of the source CSV that was loaded.

    Raises
    ------
    FileNotFoundError
        When ``mart_report_daily_series.csv`` is absent.
    ValueError
        When the loaded data fails ``validate_forecasting_series_input``.
    """
    processed_dir = project_root / "data" / "processed"
    input_path = processed_dir / "mart_report_daily_series.csv"
    daily_series = load_canonical_daily_series(processed_dir)
    validate_forecasting_series_input(daily_series)
    model_input = adapt_to_forecasting_schema(
        daily_series[[STANDARD_DATE_COL, STANDARD_REPORT_ID_COL, STANDARD_TARGET_COL]],
        processed_dir / "dim_report.csv",
    )
    return daily_series, model_input.reset_index(drop=True), input_path


def run_data_quality_checks(df: pd.DataFrame) -> pd.DataFrame:
    """Return per-check quality summary for the standardised daily series.

    Operates on the ``[date, report_id, daily_views]`` frame returned by
    ``standardise_forecasting_columns``, not on the legacy internal schema.

    Parameters
    ----------
    df:
        Standardised daily series with columns ``date``, ``report_id``,
        ``daily_views``.

    Returns
    -------
    pd.DataFrame
        One row per check with columns ``check``, ``value``, ``status``.
    """
    checks = [{"check": "row_count", "value": len(df), "status": "ok" if len(df) else "fail"}]
    checks.append({"check": "distinct_reports", "value": int(df[STANDARD_REPORT_ID_COL].nunique()), "status": "ok"})

    for col in [STANDARD_DATE_COL, STANDARD_REPORT_ID_COL, STANDARD_TARGET_COL]:
        if col not in df.columns:
            checks.append({"check": f"nulls_{col}", "value": "MISSING", "status": "fail"})
            continue
        null_count = int(df[col].isna().sum())
        checks.append(
            {
                "check": f"nulls_{col}",
                "value": null_count,
                "status": "ok" if null_count == 0 else ("fail" if col == STANDARD_TARGET_COL else "warn"),
            }
        )

    if STANDARD_DATE_COL in df.columns and STANDARD_REPORT_ID_COL in df.columns:
        dup_count = int(df.duplicated([STANDARD_DATE_COL, STANDARD_REPORT_ID_COL]).sum())
        checks.append(
            {
                "check": "duplicate_report_date_pairs",
                "value": dup_count,
                "status": "ok" if dup_count == 0 else "fail",
            }
        )

    if STANDARD_DATE_COL in df.columns:
        checks.append({"check": "min_date", "value": str(df[STANDARD_DATE_COL].min().date()), "status": "ok"})
        checks.append({"check": "max_date", "value": str(df[STANDARD_DATE_COL].max().date()), "status": "ok"})

    if STANDARD_TARGET_COL in df.columns:
        negative = int((df[STANDARD_TARGET_COL] < 0).sum())
        checks.append(
            {
                "check": "negative_daily_views",
                "value": negative,
                "status": "ok" if negative == 0 else "fail",
            }
        )
        zero_rows = int((df[STANDARD_TARGET_COL] == 0).sum())
        checks.append({"check": "zero_view_rows_preserved", "value": zero_rows, "status": "ok"})

    return pd.DataFrame(checks)


def build_daily_series_for_all_reports(
    df: pd.DataFrame,
) -> tuple[dict[str, pd.Series], dict[str, str], dict[str, dict]]:
    """Convert the validated internal-schema frame into per-report Series objects.

    ``mart_report_daily_series`` is treated as authoritative.  The input must
    already be continuous (validated by ``validate_forecasting_series_input``
    upstream).  This function **does not** fill gaps, manufacture rows, or
    reindex to a synthetic date range — the series it receives is the series
    pmdarima fits.

    Zero-view days that exist in the source are preserved exactly as-is.
    Sparse reports (high zero share) are gated out by ``filter_by_data_criteria``,
    not by dropping zeros here.

    Parameters
    ----------
    df:
        Internal-schema DataFrame with columns ``Date``, ``Report Guid``,
        ``Report Name``, ``Occurrences`` (output of ``adapt_to_forecasting_schema``).
        Must have already passed ``validate_forecasting_series_input``.

    Returns
    -------
    series_dict : dict[str, pd.Series]
        Per-report daily view series, date-indexed.  Exactly the rows present
        in the source — no synthetic rows added.
    name_lookup : dict[str, str]
        report_id → report_name mapping.
    provenance : dict[str, dict]
        Per-report metadata: ``date_min``, ``date_max``, ``n_obs``.
    """
    d = df.copy()
    d[DATE_COL] = pd.to_datetime(d[DATE_COL])
    d[VIEWS_COL] = pd.to_numeric(d[VIEWS_COL], errors="coerce").fillna(0)

    series_dict: dict[str, pd.Series] = {}
    name_lookup: dict[str, str] = {}
    provenance: dict[str, dict] = {}

    for rid, group in d.groupby(REPORT_ID_COL):
        rid_str = str(rid)
        name_lookup[rid_str] = str(group[REPORT_NAME_COL].iloc[0])
        # Aggregate to one row per date (no-op for canonical input; handles any
        # residual duplicates that slipped through adapt_to_forecasting_schema).
        daily = group.groupby(DATE_COL)[VIEWS_COL].sum().rename("Views").sort_index()
        daily.index.name = DATE_COL
        series_dict[rid_str] = daily
        provenance[rid_str] = {
            "date_min": str(daily.index.min().date()),
            "date_max": str(daily.index.max().date()),
            "n_obs": int(len(daily)),
        }

    return series_dict, name_lookup, provenance


def filter_by_data_criteria(
    series_dict: dict[str, pd.Series],
    provenance: dict[str, dict] | None = None,
) -> tuple[list[str], pd.DataFrame]:
    """Gate reports by data sufficiency and record per-report diagnostics.

    Gating is applied *after* the complete zero-filled daily series is built.
    Zeros are never removed from a passing series — sparse reports that fail
    ``MIN_NONZERO_RATIO`` are excluded in full, not trimmed.

    Parameters
    ----------
    series_dict:
        Per-report daily view series from ``build_daily_series_for_all_reports``.
    provenance:
        Optional per-report metadata dict (date_min, date_max, n_obs) returned
        by ``build_daily_series_for_all_reports``.  When supplied, these fields
        are included in the diagnostics table (requirement 9).

    Returns
    -------
    passing : list[str]
        Report IDs that pass all sufficiency thresholds.
    diagnostics : pd.DataFrame
        One row per report with sufficiency counts and gate outcomes.
    """
    records = []
    passing = []
    prov = provenance or {}

    for rid, series in series_dict.items():
        total_days = int(series.size)
        nonzero_days = int((series > 0).sum())
        nonzero_ratio = nonzero_days / total_days if total_days else 0.0
        total_views = int(series.sum())
        passes = (
            total_days >= MIN_DAYS
            and nonzero_days >= MIN_NONZERO_DAYS
            and nonzero_ratio >= MIN_NONZERO_RATIO
            and total_views >= MIN_TOTAL_VIEWS
        )

        row: dict = {
            "report_id": rid,
            "date_min": prov.get(rid, {}).get("date_min"),
            "date_max": prov.get(rid, {}).get("date_max"),
            "n_obs": prov.get(rid, {}).get("n_obs", total_days),
            "total_days": total_days,
            "nonzero_days": nonzero_days,
            "nonzero_ratio": nonzero_ratio,
            "zero_share": 1 - nonzero_ratio,
            "total_views": total_views,
            "passes_min_days": total_days >= MIN_DAYS,
            "passes_min_nonzero_days": nonzero_days >= MIN_NONZERO_DAYS,
            "passes_nonzero_ratio": nonzero_ratio >= MIN_NONZERO_RATIO,
            "passes_total_views": total_views >= MIN_TOTAL_VIEWS,
            "passes_data_criteria": passes,
        }
        records.append(row)
        if passes:
            passing.append(rid)

    return passing, pd.DataFrame.from_records(records)


def naive_forecast_last_value(y_train: pd.Series, steps: int) -> np.ndarray:
    """Forecast future values as the latest observed value."""
    return np.full(shape=steps, fill_value=float(y_train.iloc[-1]), dtype=float)


def seasonal_naive_forecast(y_train: pd.Series, steps: int) -> np.ndarray:
    """Forecast future values by repeating the latest weekly (m=7) pattern.

    This is a legacy helper used only by the old ``train_and_evaluate_arima``
    single-split pipeline.  It hard-codes m=7 (``SEASONAL_CANDIDATES[0]``)
    because the old pipeline did not support per-report seasonal periods.
    New code should call ``forecast_seasonal_naive`` from ``candidates.py``
    and pass ``seasonal_period`` explicitly.
    """
    _m = SEASONAL_CANDIDATES[0]  # 7 — weekly; legacy pipeline assumption
    if len(y_train) < _m:
        return naive_forecast_last_value(y_train, steps)

    last_season = y_train.iloc[-_m:].to_numpy(dtype=float)
    repeats = int(np.ceil(steps / _m))
    return np.tile(last_season, repeats)[:steps]


def build_forecast_frame(
    index: pd.DatetimeIndex,
    forecast_values: np.ndarray,
    actual_values: Optional[np.ndarray] = None,
    lower_ci: Optional[np.ndarray] = None,
    upper_ci: Optional[np.ndarray] = None,
) -> pd.DataFrame:
    """Build a forecast frame with error columns."""
    forecast_values = np.clip(np.asarray(forecast_values, dtype=float), 0, None)
    if actual_values is None:
        actual_values = np.full(len(forecast_values), np.nan)
    if lower_ci is None:
        lower_ci = np.full(len(forecast_values), np.nan)
    if upper_ci is None:
        upper_ci = np.full(len(forecast_values), np.nan)

    df = pd.DataFrame(
        {
            "forecast": forecast_values,
            "lower_ci": lower_ci,
            "upper_ci": upper_ci,
            "actual": actual_values,
        },
        index=index,
    )
    df["error"] = df["actual"] - df["forecast"]
    df["abs_error"] = df["error"].abs()
    df["pct_error"] = (df["abs_error"] / df["actual"].replace(0, np.nan)) * 100
    return df


def build_model_comparison(metrics: dict) -> pd.DataFrame:
    """Build per-model metric rows for selected-model inspection."""
    comparison = pd.DataFrame.from_records(
        [
            {
                "model_name": "naive",
                "mae": metrics.get("mae_naive"),
                "rmse": metrics.get("rmse_naive"),
                "wape": metrics.get("wape_naive"),
            },
            {
                "model_name": "seasonal_naive",
                "mae": metrics.get("mae_seasonal_naive"),
                "rmse": metrics.get("rmse_seasonal_naive"),
                "wape": metrics.get("wape_seasonal_naive"),
            },
            {
                "model_name": "sarima",
                "mae": metrics.get("mae_arima"),
                "rmse": metrics.get("rmse_arima"),
                "wape": metrics.get("wape_arima"),
            },
        ]
    )
    return comparison.dropna(subset=["mae", "rmse"], how="any").copy()


def select_best_model(comparison: pd.DataFrame) -> Optional[pd.Series]:
    """Select best model by MAE, then RMSE for near ties."""
    if comparison.empty:
        return None

    best_mae = comparison["mae"].min()
    ranked = comparison[comparison["mae"] <= best_mae + MAE_TIE_TOLERANCE]
    ranked = ranked.sort_values(["rmse", "mae", "model_name"], ascending=[True, True, True])
    return ranked.iloc[0]


def apply_reliability_checks(metrics: dict) -> dict:
    """Apply notebook reliability gates to selected forecasts."""
    selected_mae = metrics.get("selected_mae")
    selected_wape = metrics.get("selected_wape")
    avg_level = metrics.get("avg_level_test")
    zero_share = metrics.get("zero_share")

    passes_wape = pd.notna(selected_wape) and selected_wape <= MAX_SELECTED_WAPE
    passes_mae_ratio = (
        pd.isna(avg_level)
        or avg_level == 0
        or selected_mae <= MAX_MAE_RATIO_TO_MEAN * avg_level
    )
    passes_zero_share = pd.isna(zero_share) or zero_share <= MAX_ZERO_SHARE
    has_selected_model = pd.notna(metrics.get("selected_model"))

    metrics["passes_wape_reliability"] = bool(passes_wape)
    metrics["passes_mae_ratio"] = bool(passes_mae_ratio)
    metrics["passes_zero_share"] = bool(passes_zero_share)
    metrics["passes_model_criteria"] = bool(has_selected_model and passes_mae_ratio)
    metrics["forecast_reliable"] = bool(
        has_selected_model
        and metrics.get("passes_data_criteria", False)
        and passes_mae_ratio
        and passes_wape
        and passes_zero_share
    )
    return metrics


def train_and_evaluate_arima(
    y: pd.Series,
    horizon_days: int,
) -> tuple[dict[str, pd.DataFrame], dict, pd.DataFrame]:
    """Train SARIMA, compare baselines, and return forecast frames plus metrics."""
    from pmdarima import auto_arima

    y = y.astype(float)
    train_size = int(len(y) * TRAIN_TEST_FRACTION)
    if train_size < 30 or len(y) - train_size < 7:
        raise ValueError("Series too short for robust backtest split")

    y_train = y.iloc[:train_size]
    y_test = y.iloc[train_size:]
    n_test = len(y_test)

    model = auto_arima(
        y_train,
        seasonal=True,
        m=SEASONAL_CANDIDATES[0],  # 7 — weekly; legacy pipeline assumption
        stepwise=True,
        suppress_warnings=True,
        error_action="ignore",
        trace=False,
    )

    fc_test, conf_test = model.predict(n_periods=n_test, return_conf_int=True)
    fc_test = np.clip(fc_test, 0, None)
    conf_test = np.clip(np.asarray(conf_test, dtype=float), 0, None)

    naive_fc = naive_forecast_last_value(y_train, n_test)
    seasonal_naive_fc = seasonal_naive_forecast(y_train, n_test)

    model.update(y_test)
    fc_future, conf_future = model.predict(n_periods=horizon_days, return_conf_int=True)
    fc_future = np.clip(fc_future, 0, None)
    conf_future = np.clip(np.asarray(conf_future, dtype=float), 0, None)

    future_dates = pd.date_range(
        start=y.index[-1] + pd.Timedelta(days=1),
        periods=horizon_days,
        freq="D",
    )

    model_forecasts = {
        "sarima": pd.concat(
            [
                build_forecast_frame(
                    y_test.index,
                    fc_test,
                    actual_values=y_test.values,
                    lower_ci=conf_test[:, 0],
                    upper_ci=conf_test[:, 1],
                ),
                build_forecast_frame(
                    future_dates,
                    fc_future,
                    lower_ci=conf_future[:, 0],
                    upper_ci=conf_future[:, 1],
                ),
            ]
        ),
        "naive": pd.concat(
            [
                build_forecast_frame(y_test.index, naive_fc, actual_values=y_test.values),
                build_forecast_frame(future_dates, naive_forecast_last_value(y, horizon_days)),
            ]
        ),
        "seasonal_naive": pd.concat(
            [
                build_forecast_frame(
                    y_test.index,
                    seasonal_naive_fc,
                    actual_values=y_test.values,
                ),
                build_forecast_frame(future_dates, seasonal_naive_forecast(y, horizon_days)),
            ]
        ),
    }
    for forecast_df in model_forecasts.values():
        forecast_df.index.name = DATE_COL

    p, d, q = model.order
    seasonal_p, seasonal_d, seasonal_q, m = model.seasonal_order

    _m_arima = calculate_point_metrics(y_test.to_numpy(), fc_test, training_series=y_train.to_numpy())
    _m_naive = calculate_point_metrics(y_test.to_numpy(), naive_fc, training_series=y_train.to_numpy())
    _m_snv   = calculate_point_metrics(y_test.to_numpy(), seasonal_naive_fc, training_series=y_train.to_numpy())

    metrics = {
        "n_obs": len(y),
        "n_train": len(y_train),
        "n_test": len(y_test),
        "mae_arima": _m_arima["mae"],
        "rmse_arima": _m_arima["rmse"],
        "wape_arima": _m_arima["wape"],
        "bias_arima": _m_arima["bias"],
        "mase_arima": _m_arima["mase"],
        "mae_naive": _m_naive["mae"],
        "rmse_naive": _m_naive["rmse"],
        "wape_naive": _m_naive["wape"],
        "mae_seasonal_naive": _m_snv["mae"],
        "rmse_seasonal_naive": _m_snv["rmse"],
        "wape_seasonal_naive": _m_snv["wape"],
        "avg_level_test": y_test.mean(),
        "p": p,
        "d": d,
        "q": q,
        "P": seasonal_p,
        "D": seasonal_d,
        "Q": seasonal_q,
        "m": m,
        "model_str": f"ARIMA({p},{d},{q})x({seasonal_p},{seasonal_d},{seasonal_q},{m})",
    }
    metrics["mae_improvement_vs_naive"] = (
        1 - metrics["mae_arima"] / metrics["mae_naive"]
        if metrics["mae_naive"] > 0
        else np.nan
    )
    metrics["mae_improvement_vs_seasonal_naive"] = (
        1 - metrics["mae_arima"] / metrics["mae_seasonal_naive"]
        if metrics["mae_seasonal_naive"] > 0
        else np.nan
    )

    model_comparison = build_model_comparison(metrics)
    selected = select_best_model(model_comparison)
    if selected is not None:
        metrics.update(
            {
                "selected_model": selected["model_name"],
                "selected_mae": selected["mae"],
                "selected_rmse": selected["rmse"],
                "selected_wape": selected["wape"],
            }
        )
        model_comparison["selected_model_flag"] = model_comparison["model_name"].eq(
            selected["model_name"]
        )
    else:
        metrics.update(
            {
                "selected_model": pd.NA,
                "selected_mae": np.nan,
                "selected_rmse": np.nan,
                "selected_wape": np.nan,
            }
        )
        model_comparison["selected_model_flag"] = False

    return model_forecasts, metrics, model_comparison


def run_forecasts_for_reports(
    raw_df: pd.DataFrame,
    run_id: str,
    run_timestamp: pd.Timestamp,
    horizon_days: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run forecasting for every report that passes data checks."""
    series_dict, name_lookup, provenance = build_daily_series_for_all_reports(raw_df)
    passing_data_ids, data_diag = filter_by_data_criteria(series_dict, provenance)

    all_forecasts = []
    all_metrics = []
    all_model_comparisons = []

    for rid in passing_data_ids:
        model_forecasts = {}
        model_comparison = pd.DataFrame()
        name = name_lookup.get(rid, "")

        try:
            model_forecasts, metrics, model_comparison = train_and_evaluate_arima(
                series_dict[rid],
                horizon_days=horizon_days,
            )
        except Exception as exc:
            metrics = {
                "n_obs": len(series_dict[rid]),
                "n_train": np.nan,
                "n_test": np.nan,
                "mae_arima": np.nan,
                "rmse_arima": np.nan,
                "wape_arima": np.nan,
                "mae_naive": np.nan,
                "rmse_naive": np.nan,
                "wape_naive": np.nan,
                "mae_seasonal_naive": np.nan,
                "rmse_seasonal_naive": np.nan,
                "wape_seasonal_naive": np.nan,
                "avg_level_test": np.nan,
                "selected_model": pd.NA,
                "selected_mae": np.nan,
                "selected_rmse": np.nan,
                "selected_wape": np.nan,
                "passes_model_criteria": False,
                "forecast_reliable": False,
                "error": str(exc),
            }

        metrics["report_id"] = rid
        metrics["report_name"] = name
        diag_row = data_diag.loc[data_diag["report_id"] == rid].iloc[0]
        metrics.update(diag_row.to_dict())
        metrics = apply_reliability_checks(metrics)
        all_metrics.append(metrics)

        if not model_comparison.empty:
            comparison_out = model_comparison.copy()
            comparison_out.insert(0, "report_id", rid)
            comparison_out.insert(1, "report_name", name)
            comparison_out["forecast_reliable"] = metrics["forecast_reliable"]
            all_model_comparisons.append(comparison_out)

        selected_model = metrics.get("selected_model")
        if (
            isinstance(selected_model, str)
            and selected_model in model_forecasts
            and metrics["forecast_reliable"]
        ):
            forecast_out = model_forecasts[selected_model].copy().reset_index()
            forecast_out["ReportId"] = rid
            forecast_out["ReportName"] = name
            forecast_out["IsForecast"] = forecast_out["actual"].isna().astype(int)
            forecast_out["ModelRunTimestamp"] = run_timestamp
            forecast_out["ModelName"] = selected_model
            forecast_out["run_id"] = run_id
            forecast_out["run_timestamp"] = run_timestamp
            forecast_out["IsPlaceholderRow"] = 0
            all_forecasts.append(forecast_out)

    forecast_table = (
        pd.concat(all_forecasts, ignore_index=True)
        if all_forecasts
        else pd.DataFrame()
    )
    metrics_table = pd.DataFrame(all_metrics)
    model_comparison_table = (
        pd.concat(all_model_comparisons, ignore_index=True)
        if all_model_comparisons
        else pd.DataFrame()
    )
    return forecast_table, metrics_table, data_diag, model_comparison_table


def ensure_forecast_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Return forecast output in the notebook's stable CSV schema."""
    for col in EXPECTED_FORECAST_COLUMNS:
        if col not in df.columns:
            if col == DATE_COL:
                df[col] = pd.NaT
            elif col in {"IsForecast", "IsPlaceholderRow"}:
                df[col] = 0
            elif col in {"ReportId", "ReportName", "ModelName", "run_id"}:
                df[col] = pd.NA
            elif col in {"ModelRunTimestamp", "run_timestamp"}:
                df[col] = pd.NaT
            else:
                df[col] = pd.NA
    return df[EXPECTED_FORECAST_COLUMNS]


def make_placeholder_forecast_row(
    run_id: str,
    run_timestamp: pd.Timestamp,
) -> pd.DataFrame:
    """Create a schema-safe latest forecast file when no report is forecastable."""
    row = {
        DATE_COL: pd.Timestamp("1900-01-01"),
        "forecast": pd.NA,
        "lower_ci": pd.NA,
        "upper_ci": pd.NA,
        "actual": pd.NA,
        "error": pd.NA,
        "abs_error": pd.NA,
        "pct_error": pd.NA,
        "ReportId": pd.NA,
        "ReportName": "NO_FORECAST",
        "IsForecast": 1,
        "ModelRunTimestamp": run_timestamp,
        "ModelName": MODEL_NAME,
        "run_id": run_id,
        "run_timestamp": run_timestamp,
        "IsPlaceholderRow": 1,
    }
    return pd.DataFrame([row])[EXPECTED_FORECAST_COLUMNS]


def ensure_model_comparison_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Return model comparison output in a stable CSV schema."""
    if df is None or df.empty:
        return pd.DataFrame(columns=EXPECTED_MODEL_COMPARISON_COLUMNS)
    for col in EXPECTED_MODEL_COMPARISON_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[EXPECTED_MODEL_COMPARISON_COLUMNS]


def save_latest_outputs(
    forecast_table: pd.DataFrame,
    metrics_table: pd.DataFrame,
    model_comparison_table: pd.DataFrame,
    project_root: Path,
    run_id: str,
    run_timestamp: pd.Timestamp,
) -> tuple[Path, Path, Path]:
    """Save latest forecasts, metrics, and model comparison files."""
    forecast_output_dir = project_root / "outputs" / "forecasts"
    metrics_output_dir = project_root / "outputs" / "metrics"
    forecast_output_dir.mkdir(parents=True, exist_ok=True)
    metrics_output_dir.mkdir(parents=True, exist_ok=True)

    if forecast_table is None or forecast_table.empty:
        forecast_table = make_placeholder_forecast_row(run_id, run_timestamp)
    forecast_table = ensure_forecast_schema(forecast_table)
    model_comparison_table = ensure_model_comparison_schema(model_comparison_table)

    latest_forecast_file = forecast_output_dir / "report_view_forecasts_latest.csv"
    latest_metrics_file = metrics_output_dir / "report_view_metrics_latest.csv"
    latest_model_comparison_file = metrics_output_dir / "report_model_comparison_latest.csv"

    forecast_table.to_csv(latest_forecast_file, index=False)
    metrics_table.to_csv(latest_metrics_file, index=False)
    model_comparison_table.to_csv(latest_model_comparison_file, index=False)
    return latest_forecast_file, latest_metrics_file, latest_model_comparison_file


def append_forecasts_history(
    forecast_table: pd.DataFrame,
    project_root: Path,
) -> Optional[Path]:
    """Append future forecast rows to the long-running forecast history.

    Writes to ``outputs/forecasts/forecasts_history.csv``.

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

    Returns
    -------
    Path written, or None when there are no forecast rows to append.
    """
    import logging
    from src.persistence._csv_lock import csv_write_lock, log_write_plan

    _log = logging.getLogger(__name__)

    if forecast_table is None or forecast_table.empty:
        return None

    df_future = forecast_table[forecast_table["IsForecast"] == 1].copy()
    if df_future.empty:
        return None

    df_future[DATE_COL] = pd.to_datetime(df_future[DATE_COL], errors="coerce")
    df_future["ModelRunTimestamp"] = pd.to_datetime(
        df_future["ModelRunTimestamp"],
        errors="coerce",
    )
    df_future["target_date"] = df_future[DATE_COL]
    df_future["horizon_days"] = (
        df_future["target_date"].dt.normalize()
        - df_future["ModelRunTimestamp"].dt.normalize()
    ).dt.days

    df_out = df_future[
        [
            "run_id",
            "run_timestamp",
            "ReportId",
            "ReportName",
            "target_date",
            "horizon_days",
            "forecast",
            "lower_ci",
            "upper_ci",
            "ModelName",
        ]
    ].rename(
        columns={
            "ReportId": "report_id",
            "ReportName": "report_name",
            "forecast": "forecast_views",
            "ModelName": "model_name",
        }
    )

    history_file = project_root / "outputs" / "forecasts" / "forecasts_history.csv"

    with csv_write_lock(history_file):
        existing_rows = 0
        if history_file.exists():
            try:
                existing_rows = sum(1 for _ in open(history_file)) - 1  # subtract header
                existing_rows = max(existing_rows, 0)
            except OSError:
                existing_rows = 0

        log_write_plan(
            target=history_file,
            candidate_rows=len(df_out),
            existing_rows=existing_rows,
            new_rows=len(df_out),
            skipped_rows=0,
        )
        write_header = not history_file.exists()
        df_out.to_csv(history_file, mode="a", index=False, header=write_header)

    return history_file


def append_metrics_history(
    metrics_table: pd.DataFrame,
    project_root: Path,
    run_id: str,
    run_timestamp: pd.Timestamp,
) -> Optional[Path]:
    """Append this run's per-report metrics to the long-running metrics history.

    Writes to ``outputs/metrics/metrics_history.csv``.

    Grain
    -----
    One row per ``(run_id, report_id)``.  Each pipeline run produces exactly one
    metrics row per evaluated report, so the composite key is unique within a run
    and must remain unique across the history file.

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

    Raises
    ------
    ValueError
        If ``run_id`` is blank, if ``metrics_table`` lacks a ``report_id``
        column, or if any ``(run_id, report_id)`` key in the incoming batch
        already exists in the history file.
    """
    import logging
    from src.persistence._csv_lock import csv_write_lock, log_write_plan

    if metrics_table.empty:
        return None

    if not run_id or not str(run_id).strip():
        raise ValueError("run_id must be a non-empty string.")

    if "report_id" not in metrics_table.columns:
        raise ValueError(
            "metrics_table must contain a 'report_id' column "
            "(grain: one row per report per run)."
        )

    df = metrics_table.copy()
    df["run_id"] = run_id
    df["run_timestamp"] = run_timestamp

    history_file = project_root / "outputs" / "metrics" / "metrics_history.csv"

    with csv_write_lock(history_file):
        existing_rows = 0
        if history_file.exists():
            existing = pd.read_csv(history_file, usecols=["run_id", "report_id"])
            existing_rows = len(existing)
            if not existing.empty:
                existing_keys = set(zip(existing["run_id"], existing["report_id"]))
                incoming_keys = list(zip(df["run_id"], df["report_id"]))
                duplicates = [k for k in incoming_keys if k in existing_keys]
                if duplicates:
                    dup_str = ", ".join(
                        f"(run_id={r}, report_id={p})" for r, p in duplicates[:5]
                    )
                    raise ValueError(
                        f"metrics_history already contains {len(duplicates)} duplicate "
                        f"key(s). First duplicates: {dup_str}. "
                        "Pass a new run_id for each pipeline execution."
                    )

        log_write_plan(
            target=history_file,
            candidate_rows=len(df),
            existing_rows=existing_rows,
            new_rows=len(df),
            skipped_rows=0,
        )
        write_header = not history_file.exists()
        df.to_csv(history_file, mode="a", index=False, header=write_header)

    return history_file


def compute_daily_actuals(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Compute daily report actuals for realised-error backfill."""
    d = raw_df.copy()
    d[DATE_COL] = pd.to_datetime(d[DATE_COL])
    d[VIEWS_COL] = pd.to_numeric(d[VIEWS_COL], errors="coerce").fillna(0)
    grouped = (
        d.groupby([REPORT_ID_COL, REPORT_NAME_COL, DATE_COL])[VIEWS_COL]
        .sum()
        .reset_index(name="actual_views")
    )
    return grouped.rename(
        columns={
            REPORT_ID_COL: "report_id",
            REPORT_NAME_COL: "report_name",
            DATE_COL: "date",
        }
    )


def _validate_realized_errors_history(existing: pd.DataFrame) -> None:
    """Validate the existing realized_errors_history DataFrame before dedup.

    Raises
    ------
    ValueError
        If any deduplication key column contains nulls, if ``target_date``
        cannot be parsed as a valid date, or if the file already contains
        duplicate ``(run_id, report_id, target_date)`` keys.
    """
    key_cols = ["run_id", "report_id", "target_date"]

    for col in key_cols:
        null_count = existing[col].isna().sum()
        if null_count:
            raise ValueError(
                f"realized_errors_history contains {null_count} null value(s) "
                f"in key column '{col}'. The history file may be corrupted."
            )

    # target_date must already be datetime after parse_dates; check for NaT
    if not pd.api.types.is_datetime64_any_dtype(existing["target_date"]):
        raise ValueError(
            "realized_errors_history 'target_date' column could not be parsed "
            "as datetime. The history file may be corrupted."
        )

    dup_mask = existing.duplicated(subset=key_cols, keep=False)
    n_dups = dup_mask.sum()
    if n_dups:
        sample = existing.loc[dup_mask, key_cols].head(3).to_dict(orient="records")
        raise ValueError(
            f"realized_errors_history contains {n_dups} row(s) with duplicate "
            f"(run_id, report_id, target_date) keys. "
            f"Sample duplicates: {sample}. "
            "The history file may be corrupted."
        )


def update_realized_errors(raw_df: pd.DataFrame, project_root: Path) -> pd.DataFrame:
    """Backfill actual errors for historical forecasts whose dates have arrived.

    .. deprecated::
        ``update_realized_errors`` writes to the legacy
        ``realized_errors_history.csv`` schema and is retained only for the
        legacy ``run_pipeline`` path.  Production orchestration
        (``run_production_pipeline``) uses
        ``src.models.realized_forecast_history.update_realized_forecast_history``
        instead.  Do not call both functions in the same pipeline execution.

    Single-writer assumption
    ------------------------
    Uses append-mode CSV writes with no file locking.  Safe only when a single
    process writes at a time.  Concurrent writers will produce duplicates.

    Raises
    ------
    ValueError
        If the existing realized_errors_history file contains invalid dates,
        null deduplication keys, or pre-existing duplicate keys.
    """
    forecasts_history_file = project_root / "outputs" / "forecasts" / "forecasts_history.csv"
    realized_errors_file = project_root / "outputs" / "metrics" / "realized_errors_history.csv"
    if not forecasts_history_file.exists():
        return pd.DataFrame()

    forecasts_hist = pd.read_csv(
        forecasts_history_file,
        parse_dates=["run_timestamp", "target_date"],
    )
    if forecasts_hist.empty:
        return pd.DataFrame()

    actuals = compute_daily_actuals(raw_df)
    actuals["date"] = pd.to_datetime(actuals["date"])
    actuals = actuals[["report_id", "date", "actual_views"]]

    merged = forecasts_hist.merge(
        actuals,
        left_on=["report_id", "target_date"],
        right_on=["report_id", "date"],
        how="left",
    )
    merged = merged[~merged["actual_views"].isna()].copy()
    if merged.empty:
        return pd.DataFrame()

    if realized_errors_file.exists():
        existing = pd.read_csv(realized_errors_file)
        # Re-parse target_date with errors="raise" so bad values surface immediately
        # rather than silently becoming NaT (which would corrupt dedup logic).
        try:
            existing["target_date"] = pd.to_datetime(
                existing["target_date"], errors="raise"
            )
        except Exception as exc:
            raise ValueError(
                "realized_errors_history contains an unparseable 'target_date' value. "
                f"Original error: {exc}"
            ) from exc

        if not existing.empty:
            _validate_realized_errors_history(existing)
            existing_keys = set(
                zip(existing["run_id"], existing["report_id"], existing["target_date"])
            )
            merged["key"] = list(
                zip(merged["run_id"], merged["report_id"], merged["target_date"])
            )
            merged = merged[~merged["key"].isin(existing_keys)].copy()
            merged = merged.drop(columns=["key"])

    if merged.empty:
        return pd.DataFrame()

    merged["error"] = merged["actual_views"] - merged["forecast_views"]
    merged["abs_error"] = merged["error"].abs()
    merged["pct_error"] = (
        merged["abs_error"] / merged["actual_views"].replace(0, np.nan)
    ) * 100
    merged["realized_at"] = pd.Timestamp.now()

    df_out = merged[
        [
            "run_id",
            "report_id",
            "report_name",
            "target_date",
            "horizon_days",
            "forecast_views",
            "actual_views",
            "error",
            "abs_error",
            "pct_error",
            "run_timestamp",
            "realized_at",
            "model_name",
        ]
    ]

    write_header = not realized_errors_file.exists()
    df_out.to_csv(realized_errors_file, mode="a", index=False, header=write_header)
    return df_out


def run_pipeline(horizon: int = FORECAST_HORIZON_DAYS) -> dict[str, object]:
    """Run the full forecasting baseline and save outputs.

    Data flow
    ---------
    1. ``load_forecast_feature_input`` selects ``mart_report_daily_series`` (or
       best available fallback), strips to ``[date, report_id, daily_views]``,
       and validates the series before any model sees it.
    2. ``run_data_quality_checks`` runs on the clean standardised frame.
    3. ``run_forecasts_for_reports`` gates reports by data sufficiency, then fits
       pmdarima on each passing series.  No engagement or performance columns
       enter the model.
    """
    project_root = get_project_root()
    run_timestamp = pd.Timestamp.now()
    run_id = run_timestamp.strftime("%Y%m%d_%H%M%S") + "_" + str(uuid.uuid4())[:8]

    daily_series, model_input, input_path = load_forecast_feature_input(project_root)
    dq_table = run_data_quality_checks(daily_series)
    forecast_table, metrics_table, data_diag, model_comparison_table = run_forecasts_for_reports(
        model_input,
        run_id=run_id,
        run_timestamp=run_timestamp,
        horizon_days=horizon,
    )

    latest_paths = save_latest_outputs(
        forecast_table,
        metrics_table,
        model_comparison_table,
        project_root,
        run_id,
        run_timestamp,
    )
    forecast_history_path = append_forecasts_history(forecast_table, project_root)
    metrics_history_path = append_metrics_history(
        metrics_table,
        project_root,
        run_id,
        run_timestamp,
    )
    realized_errors = update_realized_errors(model_input, project_root)

    print(f"RUN_ID: {run_id}")
    print(f"Input CSV used: {input_path.relative_to(project_root)}")
    print(f"Forecast rows published: {len(forecast_table)}")
    print(f"Metrics rows: {len(metrics_table)}")
    print(f"Data quality checks: {len(dq_table)}")
    print(f"Model comparison rows: {len(model_comparison_table)}")
    print(f"Reports passing data criteria: {int(data_diag['passes_data_criteria'].sum())}/{len(data_diag)}")
    for path in latest_paths:
        print(f"Saved: {path.relative_to(project_root)}")
    if forecast_history_path:
        print(f"Appended: {forecast_history_path.relative_to(project_root)}")
    if metrics_history_path:
        print(f"Appended: {metrics_history_path.relative_to(project_root)}")
    if not realized_errors.empty:
        print(f"Realized error rows added: {len(realized_errors)}")

    return {
        "run_id": run_id,
        "input_path": input_path,
        "forecast_table": forecast_table,
        "metrics_table": metrics_table,
        "data_diagnostics": data_diag,
        "model_comparison_table": model_comparison_table,
        "latest_paths": latest_paths,
    }


# ---------------------------------------------------------------------------
# Stage helpers for the new production pipeline
# ---------------------------------------------------------------------------

def run_backtest_stage(
    eligible_series: dict[str, pd.Series],
    model_registry: Optional[dict],
    backtest_config=None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run rolling backtest evaluation for all eligible reports.

    This is Stage 1 + 2 + 3 of the production pipeline:
    backtest → fold metrics → model summary → model selection.

    Parameters
    ----------
    eligible_series:
        Per-report daily series that have passed data-sufficiency checks.
    model_registry:
        Mapping of model_name → callable.  When ``None``, the full five-
        model registry from ``candidates.py`` is used.
    backtest_config:
        ``BacktestConfig`` instance; defaults to the central config values.

    Returns
    -------
    predictions : pd.DataFrame
        Row-per-(report, fold, model, forecast_date) backtest predictions.
    fold_metrics : pd.DataFrame
        Row-per-(report, fold, model) fold-level evaluation metrics.
    model_summary : pd.DataFrame
        Row-per-(report, model) cross-fold summary.
    selection : pd.DataFrame
        Row-per-report model selection decisions.
    """
    from src.models.backtest_evaluation import BacktestConfig, evaluate_models_across_folds
    from src.models.candidates import (
        forecast_auto_arima,
        forecast_ets,
        forecast_moving_average,
        forecast_naive,
        forecast_seasonal_naive,
    )
    from src.models.model_summary import summarise_model_performance
    from src.models.selection import select_models

    if model_registry is None:
        model_registry = {
            "naive": forecast_naive,
            "seasonal_naive": forecast_seasonal_naive,
            "moving_average": forecast_moving_average,
            "ets": forecast_ets,
            "auto_arima": forecast_auto_arima,
        }

    if backtest_config is None:
        backtest_config = BacktestConfig()

    all_predictions: list[pd.DataFrame] = []
    all_fold_metrics: list[pd.DataFrame] = []

    for rid, series in eligible_series.items():
        try:
            preds, fmetrics = evaluate_models_across_folds(
                rid, series, model_registry, backtest_config
            )
            all_predictions.append(preds)
            all_fold_metrics.append(fmetrics)
        except Exception:
            pass

    if not all_fold_metrics:
        empty_preds = pd.DataFrame()
        empty_fm = pd.DataFrame()
        empty_summary = pd.DataFrame()
        empty_sel = pd.DataFrame()
        return empty_preds, empty_fm, empty_summary, empty_sel

    predictions = pd.concat(all_predictions, ignore_index=True)
    fold_metrics = pd.concat(all_fold_metrics, ignore_index=True)
    model_summary = summarise_model_performance(fold_metrics)
    selection = select_models(model_summary)

    return predictions, fold_metrics, model_summary, selection


def run_candidate_backtest_stage(
    eligible_series: dict[str, pd.Series],
    backtest_config=None,
    candidate_periods: tuple = None,
    include_ets: bool = True,
    include_arima: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list]:
    """Run candidate-aware rolling backtest evaluation for all eligible reports.

    Uses ``evaluate_candidates_across_folds`` (per-fold seasonality profiling)
    instead of the fixed five-model registry.  Returns one selection row per
    report with ``selected_model_family``, ``selected_model_name``, and
    ``selected_m`` as separate columns, enabling period-preserving refitting.

    Parameters
    ----------
    eligible_series:
        Per-report daily series that have passed data-sufficiency checks.
    backtest_config:
        ``BacktestConfig`` instance; defaults to central config values.
    candidate_periods:
        Tuple of candidate seasonal periods.  Defaults to ``SEASONAL_CANDIDATES``.
    include_ets, include_arima:
        Whether to include ETS / Auto-ARIMA candidates.

    Returns
    -------
    predictions, fold_metrics, candidate_summary, selection : pd.DataFrame
        ``candidate_summary`` has one row per (report_id, model_family, candidate_m).
        ``selection`` has ``selected_model_family``, ``selected_m`` etc. as separate
        columns — ready for ``build_production_forecast``.
    training_residual_records : list[dict]
        Per-(report, fold, candidate) training-residual records from
        ``evaluate_candidates_across_folds``.  Pass to
        ``build_training_residual_dataset`` in ``src.models.residual_datasets``.
    """
    from src.models.backtest_evaluation import BacktestConfig, evaluate_candidates_across_folds
    from src.models.model_summary import summarise_candidate_performance
    from src.models.selection import select_candidate_models

    if candidate_periods is None:
        candidate_periods = SEASONAL_CANDIDATES

    if backtest_config is None:
        backtest_config = BacktestConfig()

    all_predictions: list[pd.DataFrame] = []
    all_fold_metrics: list[pd.DataFrame] = []
    all_tr_records: list[dict] = []

    for rid, series in eligible_series.items():
        try:
            preds, fmetrics, tr_records = evaluate_candidates_across_folds(
                rid, series, backtest_config,
                candidate_periods=candidate_periods,
                include_ets=include_ets,
                include_arima=include_arima,
            )
            all_predictions.append(preds)
            all_fold_metrics.append(fmetrics)
            all_tr_records.extend(tr_records)
        except Exception:
            pass

    if not all_fold_metrics:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), []

    predictions = pd.concat(all_predictions, ignore_index=True)
    fold_metrics = pd.concat(all_fold_metrics, ignore_index=True)
    candidate_summary = summarise_candidate_performance(fold_metrics)
    selection = select_candidate_models(candidate_summary)

    return predictions, fold_metrics, candidate_summary, selection, all_tr_records


def save_production_outputs(
    production_df: pd.DataFrame,
    model_summary: pd.DataFrame,
    selection: pd.DataFrame,
    project_root: Path,
    run_id: str,
    generated_at: pd.Timestamp,
    series_dict: Optional[dict] = None,
    fold_metrics: Optional[pd.DataFrame] = None,
    candidate_summary: Optional[pd.DataFrame] = None,
    predictions: Optional[pd.DataFrame] = None,
) -> dict[str, Optional[Path]]:
    """Save production forecast, model summary, and selection files.

    Production forecast rows are written to ``latest`` (overwrite) and
    appended to ``production_forecasts_history.csv`` (never overwritten).
    The history file accumulates every run's forecasts in append mode so
    that past production forecasts are never lost (requirement 5).

    Parameters
    ----------
    production_df:
        Output of ``build_production_forecast``.
    model_summary:
        Cross-fold model summary from ``summarise_model_performance``.
    selection:
        Per-report model selection from ``select_models``.
    project_root:
        Repository root; outputs are written under ``outputs/``.
    run_id:
        Stamped into the history file for each batch of rows.
    generated_at:
        Forecast generation timestamp.
    series_dict:
        Optional per-report daily series dict; when supplied, seasonality
        diagnostic outputs are generated and saved alongside forecasts.
    fold_metrics:
        Optional fold-level metrics (output of ``evaluate_candidates_across_folds``);
        required for seasonality diagnostics.
    candidate_summary:
        Optional candidate summary (output of ``summarise_candidate_performance``);
        used for baseline improvement computations in diagnostics.
    predictions:
        Optional row-per-(report, fold, model, forecast_date) backtest predictions
        (output of ``run_candidate_backtest_stage``).  When supplied together with
        ``series_dict``, horizon-bucket metrics are computed and saved to
        ``outputs/metrics/backtest_horizon_performance_latest.csv``.  Selection
        lineage (``selected_model_name``, ``selected_m``) is joined from
        ``selection`` so each row can be linked to the winning candidate.

    Returns
    -------
    dict mapping logical name → ``Path`` (or ``None`` when nothing saved).
    """
    forecast_dir = project_root / "outputs" / "forecasts"
    metrics_dir = project_root / "outputs" / "metrics"
    forecast_dir.mkdir(parents=True, exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    paths: dict[str, Optional[Path]] = {}

    # Latest production forecast (overwrite — always reflects the current run)
    latest_fc_path = forecast_dir / "production_forecasts_latest.csv"
    production_df.to_csv(latest_fc_path, index=False)
    paths["production_latest"] = latest_fc_path

    # History (append-only — past runs are never overwritten)
    history_path = forecast_dir / "production_forecasts_history.csv"
    write_header = not history_path.exists()
    if not production_df.empty:
        production_df.to_csv(history_path, mode="a", index=False, header=write_header)
        paths["production_history"] = history_path
    else:
        paths["production_history"] = None

    # Model summary
    if not model_summary.empty:
        summary_path = metrics_dir / "evaluation_model_summary_latest.csv"
        model_summary.to_csv(summary_path, index=False)
        paths["model_summary"] = summary_path
    else:
        paths["model_summary"] = None

    # Selection decisions
    if not selection.empty:
        sel_path = metrics_dir / "model_selection_latest.csv"
        selection.to_csv(sel_path, index=False)
        paths["model_selection"] = sel_path
    else:
        paths["model_selection"] = None

    # Prediction-level backtest output (diagnostic and audit dataset)
    if predictions is not None and not predictions.empty:
        try:
            from src.models.backtest_predictions import save_backtest_predictions
            bp_path = save_backtest_predictions(
                predictions, project_root, evaluation_run_id=run_id
            )
            paths["backtest_predictions"] = bp_path
        except Exception:
            paths["backtest_predictions"] = None
    else:
        paths["backtest_predictions"] = None

    # Backtest horizon-bucket performance output
    if predictions is not None and series_dict and not predictions.empty:
        try:
            from src.models.horizon_evaluation import calculate_horizon_bucket_metrics
            from src.models.horizon_output_validation import (
                validate_backtest_horizon_output,
            )

            horizon_df = calculate_horizon_bucket_metrics(predictions, series_dict)

            # Join selection lineage so each row is traceable to the winning candidate
            if not selection.empty and "report_id" in selection.columns:
                lineage = selection[
                    ["report_id", "selected_model_name", "selected_m"]
                ].drop_duplicates("report_id")
                horizon_df = horizon_df.merge(lineage, on="report_id", how="left")
            else:
                horizon_df["selected_model_name"] = None
                horizon_df["selected_m"] = None

            validate_backtest_horizon_output(horizon_df)

            bh_path = metrics_dir / "backtest_horizon_performance_latest.csv"
            horizon_df.to_csv(bh_path, index=False)
            paths["backtest_horizon"] = bh_path
        except Exception:
            paths["backtest_horizon"] = None
    else:
        paths["backtest_horizon"] = None

    # Seasonality diagnostic outputs
    if series_dict and fold_metrics is not None and not fold_metrics.empty:
        try:
            from src.models.seasonality_diagnostics import (
                build_seasonality_candidates,
                build_seasonality_summary,
                save_seasonality_diagnostics,
            )
            cs = candidate_summary if candidate_summary is not None else pd.DataFrame()
            diag_summary = build_seasonality_summary(
                series_dict=series_dict,
                selection=selection if not selection.empty else pd.DataFrame(),
                fold_metrics=fold_metrics,
                production_df=production_df,
                candidate_summary=cs,
            )
            diag_candidates = build_seasonality_candidates(fold_metrics)
            diag_paths = save_seasonality_diagnostics(diag_summary, diag_candidates, project_root)
            paths["seasonality_summary"] = diag_paths["summary"]
            paths["seasonality_candidates"] = diag_paths["candidates"]
        except Exception:
            paths["seasonality_summary"] = None
            paths["seasonality_candidates"] = None
    else:
        paths["seasonality_summary"] = None
        paths["seasonality_candidates"] = None

    return paths


def run_production_pipeline(
    horizon: int = FORECAST_HORIZON_DAYS,
    model_registry: Optional[dict] = None,
    backtest_config=None,
) -> dict[str, object]:
    """Run the explicit six-step production forecasting lifecycle.

    The evaluation and future forecasting phases are kept strictly separate:

    1. Generate rolling backtest results
       ``evaluate_models_across_folds`` fits candidates on expanding windows;
       no evaluation state survives this step.

    2. Summarize model performance
       ``summarise_model_performance`` aggregates fold-level metrics per
       (report, model).

    3. Select one model per report
       ``select_models`` applies the policy from ``selection.py``.

    4. Refit the selected model on complete available history
       ``refit_and_forecast`` fits a *fresh* model on all observations.
       ``model.update`` is never called on an evaluation model.

    5. Generate the next *horizon*-day production forecast
       The forecast index starts the day after the final observed date.
       No backtest evaluation rows are mixed into the output.

    6. Save forecast metadata and selection lineage
       ``save_production_outputs`` writes latest files (overwrite) and
       appends to history (never overwrite prior runs).

    Parameters
    ----------
    horizon:
        Number of future days to forecast.  Defaults to
        ``FORECAST_HORIZON_DAYS`` (28).
    model_registry:
        Override the default five-model registry.  Useful for tests.
    backtest_config:
        Override the default ``BacktestConfig``.  Useful for tests.

    Returns
    -------
    dict
        Keys: ``run_id``, ``input_path``, ``production_forecast``,
        ``model_summary``, ``selection``, ``data_diagnostics``,
        ``output_paths``.
    """
    from src.models.production_forecast import build_production_forecast

    project_root = get_project_root()
    generated_at = pd.Timestamp.now()
    run_id = generated_at.strftime("%Y%m%d_%H%M%S") + "_" + str(uuid.uuid4())[:8]

    # Load and validate canonical input
    daily_series, model_input, input_path = load_forecast_feature_input(project_root)
    dq_table = run_data_quality_checks(daily_series)

    # Build per-report series objects and gate by data sufficiency
    series_dict, name_lookup, provenance = build_daily_series_for_all_reports(model_input)
    passing_ids, data_diag = filter_by_data_criteria(series_dict, provenance)
    eligible_series = {rid: series_dict[rid] for rid in passing_ids}

    # Steps 1–3: Backtest → summarize → select
    # Use candidate-aware stage: evaluates each (model_family, candidate_m)
    # combination per fold and selects jointly, so selected_m is always
    # available for period-preserving production refitting.
    predictions, fold_metrics, model_summary, selection, training_residual_records = (
        run_candidate_backtest_stage(
            eligible_series,
            backtest_config=backtest_config,
        )
    )

    # Steps 4–5: Refit on full history → generate production forecasts.
    # Pass model_summary (candidate_summary) so fallback candidates are
    # available when the primary selected model fails to refit.
    production_df = build_production_forecast(
        selection=selection,
        series_dict=eligible_series,
        run_id=run_id,
        generated_at=generated_at,
        horizon=horizon,
        selection_run_id=run_id,
        candidate_summary=model_summary,
    )

    # Step 6: Save with lineage + seasonality diagnostics + backtest horizon
    output_paths = save_production_outputs(
        production_df=production_df,
        model_summary=model_summary,
        selection=selection,
        project_root=project_root,
        run_id=run_id,
        generated_at=generated_at,
        series_dict=eligible_series,
        fold_metrics=fold_metrics,
        candidate_summary=model_summary,
        predictions=predictions,
    )

    # Step 7: Update normalized realized forecast history.
    # Runs only after production_forecasts_history.csv has been written by
    # save_production_outputs (Step 6) so the canonical source is available.
    # Uses daily_series (the mart_report_daily_series frame) as the only
    # source of actuals.  update_realized_errors is NOT called here.
    from src.models.realized_forecast_history import (
        update_realized_forecast_history as _update_rfh,
    )
    realized_rows, n_skipped = _update_rfh(
        raw_actuals_df=daily_series,
        project_root=project_root,
        realized_at=generated_at,
    )

    n_selected = int(selection["selection_status"].eq("selected").sum()) if not selection.empty else 0
    n_eligible = len(passing_ids)
    n_reports_total = len(data_diag)
    n_candidates = len(production_df[production_df["horizon_step"].notna()])

    print(f"RUN_ID: {run_id}")
    print(f"Input: {input_path.relative_to(project_root)}")
    print(f"Reports: {n_eligible}/{n_reports_total} passed data criteria")
    print(f"Models selected: {n_selected}/{n_eligible} eligible reports")
    print(f"Production forecast rows: {n_candidates}")
    for name, path in output_paths.items():
        if path:
            print(f"Saved [{name}]: {path.relative_to(project_root)}")
    print(
        f"Realized forecast history: "
        f"{len(realized_rows)} new row(s) appended, "
        f"{n_skipped} already-realized row(s) skipped "
        f"(of {len(realized_rows) + n_skipped} candidates)"
    )

    # Step 8: Build canonical residual and forecast-error diagnostic datasets.
    # Runs after backtest predictions and realized history have been written.
    # Failures are isolated — they do not abort the production run.
    try:
        from src.models.residual_datasets import persist_residual_datasets
        name_lookup = {
            rid: series_dict[rid].name if hasattr(series_dict.get(rid, None), "name") else rid
            for rid in (series_dict or {})
        }
        _diag_paths = persist_residual_datasets(
            training_residual_records=training_residual_records,
            backtest_predictions_path=output_paths.get("backtest_predictions"),
            realized_history_path=(
                project_root / "outputs" / "metrics" / "realized_forecast_history.csv"
            ),
            project_root=project_root,
            diagnostic_run_id=run_id,
            evaluation_run_id=run_id,
            name_lookup=name_lookup,
        )
        for name, path in _diag_paths.items():
            if path:
                print(f"Saved [diagnostics_{name}]: {path.relative_to(project_root)}")
    except Exception as _exc:
        print(f"Warning: residual dataset generation failed: {_exc}")
        _diag_paths = {}

    # Step 9 (pre): Residual autocorrelation diagnostics.
    # Runs after residual datasets are built.  Failures are isolated.
    try:
        from src.models.autocorrelation_diagnostics import (
            build_training_autocorrelation_diagnostics,
            build_backtest_autocorrelation_diagnostics,
            build_production_autocorrelation_diagnostics,
            persist_autocorrelation_diagnostics,
        )
        from src.models.residual_datasets import (
            build_training_residual_dataset,
            build_backtest_forecast_error_dataset,
            build_production_forecast_error_view,
        )
        _tr_res_df = build_training_residual_dataset(
            training_residual_records,
            diagnostic_run_id=run_id,
            evaluation_run_id=run_id,
            name_lookup=name_lookup,
        )
        _bt_preds_path = output_paths.get("backtest_predictions")
        _bt_preds_df = pd.read_csv(_bt_preds_path) if (_bt_preds_path and Path(_bt_preds_path).exists()) else pd.DataFrame()
        _bt_res_df = build_backtest_forecast_error_dataset(
            _bt_preds_df, evaluation_run_id=run_id, name_lookup=name_lookup
        ) if not _bt_preds_df.empty else pd.DataFrame()
        _realized_path = project_root / "outputs" / "metrics" / "realized_forecast_history.csv"
        _real_df = pd.read_csv(_realized_path) if _realized_path.exists() else pd.DataFrame()
        _prod_res_df = build_production_forecast_error_view(_real_df) if not _real_df.empty else pd.DataFrame()

        _tr_acf = build_training_autocorrelation_diagnostics(_tr_res_df, diagnostic_run_id=run_id)
        _bt_fold_acf, _bt_sum_acf = build_backtest_autocorrelation_diagnostics(_bt_res_df, evaluation_run_id=run_id)
        _prod_acf = build_production_autocorrelation_diagnostics(_prod_res_df, evaluation_run_id=run_id)

        _acf_paths = persist_autocorrelation_diagnostics(
            _tr_acf, _bt_fold_acf, _bt_sum_acf, _prod_acf, project_root
        )
        for name, path in _acf_paths.items():
            if path:
                print(f"Saved [acf_{name}]: {path.relative_to(project_root)}")
    except Exception as _acf_exc:
        print(f"Warning: autocorrelation diagnostics failed: {_acf_exc}")
        _acf_paths = {}

    # Step 9 (pre-2): Bias and residual-stability diagnostics.
    # Runs after autocorrelation diagnostics.  Failures are isolated.
    try:
        from src.models.bias_stability_diagnostics import (
            build_training_bias_diagnostics,
            build_backtest_bias_diagnostics_by_fold,
            build_backtest_bias_summary,
            build_production_bias_diagnostics,
            persist_bias_stability_diagnostics,
        )
        _tr_bias = build_training_bias_diagnostics(_tr_res_df, diagnostic_run_id=run_id)
        _bt_fold_bias = build_backtest_bias_diagnostics_by_fold(
            _bt_res_df, evaluation_run_id=run_id
        ) if not _bt_res_df.empty else pd.DataFrame()
        _bt_sum_bias = build_backtest_bias_summary(
            _bt_fold_bias, _bt_res_df if not _bt_res_df.empty else None,
            evaluation_run_id=run_id,
        )
        _prod_bias = build_production_bias_diagnostics(
            _prod_res_df, evaluation_run_id=run_id
        ) if not _prod_res_df.empty else pd.DataFrame()
        _bias_paths = persist_bias_stability_diagnostics(
            _tr_bias, _bt_fold_bias, _bt_sum_bias, _prod_bias, project_root
        )
        for name, path in _bias_paths.items():
            if path:
                print(f"Saved [bias_{name}]: {path.relative_to(project_root)}")
    except Exception as _bias_exc:
        print(f"Warning: bias stability diagnostics failed: {_bias_exc}")
        _bias_paths = {}

    # Step 9: Aggregate production performance monitoring tables.
    # Runs after realized history is updated so the tables always reflect the
    # latest realized rows.  Written to outputs/monitoring/ as CSV overwrites
    # (derived views, not append-only history).
    from src.monitoring.production_performance import update_production_performance
    perf_result = update_production_performance(project_root)
    monitoring_paths = perf_result["paths"]
    for name, path in monitoring_paths.items():
        if path:
            print(f"Saved [monitoring_{name}]: {path.relative_to(project_root)}")

    return {
        "run_id": run_id,
        "input_path": input_path,
        "production_forecast": production_df,
        "predictions": predictions,
        "fold_metrics": fold_metrics,
        "model_summary": model_summary,
        "selection": selection,
        "data_diagnostics": data_diag,
        "data_quality": dq_table,
        "output_paths": output_paths,
        "realized_rows": realized_rows,
        "n_realized_skipped": n_skipped,
        "monitoring_tables": perf_result["tables"],
        "monitoring_paths": monitoring_paths,
    }


if __name__ == "__main__":
    run_pipeline()
