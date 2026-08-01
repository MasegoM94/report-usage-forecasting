"""Data loading helpers for the Streamlit reviewer app.

The app is intentionally read-only and tolerant of missing artifacts so that it
can be opened before every pipeline output has been generated.

Forecast file preference
------------------------
When ``outputs/forecasts/production_forecasts_latest.csv`` is present,
``load_app_data`` sets ``data["forecasts"]`` from that file and ignores the
legacy ``report_view_forecasts_latest.csv``.  If the production file is present
but fails schema validation a ``ValueError`` is raised immediately — there is no
silent fallback, because silently mixing production and legacy data would corrupt
downstream displays.  When the production file is absent the legacy file is used
without complaint (monitoring has simply not yet run).

Monitoring output files
-----------------------
Seven production-pipeline monitoring files are loaded alongside the standard app
inputs.  Each file is accessed under a stable key in ``data`` and in the
structured dict returned by ``load_monitoring_data``.  Each monitoring entry
carries a ``status`` field:

    "absent"         — file does not exist (monitoring has not yet run)
    "empty"          — file exists but contains no data rows
    "invalid_schema" — file exists and has rows, but required columns are missing
    "ok"             — file exists, has rows, and passes schema validation

Keys added to ``data`` by monitoring:
    production_forecasts_latest    outputs/forecasts/production_forecasts_latest.csv
    production_forecasts_history   outputs/forecasts/production_forecasts_history.csv
    realized_errors_history        outputs/metrics/realized_errors_history.csv
    perf_by_run                    outputs/monitoring/realized_performance_by_run.csv
    perf_by_report                 outputs/monitoring/realized_performance_by_report.csv
    perf_by_horizon                outputs/monitoring/realized_performance_by_horizon.csv
    perf_by_model                  outputs/monitoring/realized_performance_by_model.csv
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

import pandas as pd

try:
    import streamlit as st

    def _cache(fn):  # type: ignore[return]
        return st.cache_data(fn)
except ImportError:
    # Running outside a Streamlit session (tests, scripts).
    # Apply a no-op decorator so the module remains importable.
    def _cache(fn):
        return fn


OUTPUT_PATHS = {
    "forecast_features": Path("data/processed/mart_report_daily_context.csv"),
    # Legacy forecast file — used only when production_forecasts_latest is absent
    "forecasts": Path("outputs/forecasts/report_view_forecasts_latest.csv"),
    "metrics": Path("outputs/metrics/report_view_metrics_latest.csv"),
    "report_features": Path("outputs/metrics/report_features.csv"),
    # PRIVACY: user_features.csv and user_segments.csv are NOT loaded into the app.
    # These files contain user_key (pseudonymous) but must not flow into app memory
    # because the app also has access to dim_user which would allow re-identification.
    # user_features: Path("outputs/metrics/user_features.csv"),   # excluded — privacy
    # user_segments: Path("outputs/segments/user_segments.csv"),  # excluded — privacy
    "segments": Path("outputs/segments/report_segments.csv"),
    "diagnostics": Path("outputs/diagnostics/report_diagnostics.csv"),
    "insights": Path("outputs/insights/report_ai_insights.json"),
    # PRIVACY: dim_user.csv maps user_key -> email (direct identifier).
    # It is RESTRICTED IDENTITY DATA and must NOT be loaded into the app.
    # dim_user: Path("data/processed/dim_user.csv"),  # excluded — restricted identity data
    "dim_report": Path("data/processed/dim_report.csv"),
    "fact_report_views": Path("data/processed/fact_report_views.csv"),
    # Canonical report-level analytics mart (Sprint 8+).  One row per report.
    "report_analytics": Path("outputs/analytics/mart_report_analytics.csv"),
    # Engagement mart — report-level aggregates only, no user identifiers.
    "engagement": Path("outputs/analytics/mart_report_engagement.csv"),
}

# Monitoring files written by the production pipeline.  Keyed by the logical
# name used in data["..."] and the structured monitoring dict.
MONITORING_PATHS: dict[str, Path] = {
    "production_forecasts_latest":  Path("outputs/forecasts/production_forecasts_latest.csv"),
    "production_forecasts_history": Path("outputs/forecasts/production_forecasts_history.csv"),
    "realized_errors_history":      Path("outputs/metrics/realized_errors_history.csv"),
    "perf_by_run":                  Path("outputs/monitoring/realized_performance_by_run.csv"),
    "perf_by_report":               Path("outputs/monitoring/realized_performance_by_report.csv"),
    "perf_by_horizon":              Path("outputs/monitoring/realized_performance_by_horizon.csv"),
    "perf_by_model":                Path("outputs/monitoring/realized_performance_by_model.csv"),
}

# Status literals returned in the structured monitoring dict
MonitoringStatus = Literal["absent", "empty", "invalid_schema", "ok"]

# ---------------------------------------------------------------------------
# Required column sets — one per monitoring file type
# ---------------------------------------------------------------------------

# Minimum columns required to consider each monitoring file structurally valid.
# These cover identity and the most commonly displayed fields; additional columns
# may exist without error.

REQUIRED_COLS_PRODUCTION_FORECAST: set[str] = {
    "run_id",
    "report_id",
    "forecast_date",
    "horizon_step",
    "selected_model_family",
    "selected_model_name",
    "selected_m",
    "forecast",
}

REQUIRED_COLS_REALIZED_ERRORS: set[str] = {
    "report_id",
    "forecast_date",
    "actual",
    "forecast",
}

REQUIRED_COLS_PERF_BY_RUN: set[str] = {
    "run_id",
    "realized_prediction_count",
    "realization_rate",
    "wape",
    "bias",
}

REQUIRED_COLS_PERF_BY_REPORT: set[str] = {
    "report_id",
    "wape",
    "monitoring_status",
}

REQUIRED_COLS_PERF_BY_HORIZON: set[str] = {
    "report_id",
    "horizon_bucket",
    "wape",
    "observation_count",
}

REQUIRED_COLS_PERF_BY_MODEL: set[str] = {
    "selected_model_family",
    "selected_m",
    "wape",
}

# Map each monitoring key to its required column set
_MONITORING_REQUIRED_COLS: dict[str, set[str]] = {
    "production_forecasts_latest":  REQUIRED_COLS_PRODUCTION_FORECAST,
    "production_forecasts_history": REQUIRED_COLS_PRODUCTION_FORECAST,
    "realized_errors_history":      REQUIRED_COLS_REALIZED_ERRORS,
    "perf_by_run":                  REQUIRED_COLS_PERF_BY_RUN,
    "perf_by_report":               REQUIRED_COLS_PERF_BY_REPORT,
    "perf_by_horizon":              REQUIRED_COLS_PERF_BY_HORIZON,
    "perf_by_model":                REQUIRED_COLS_PERF_BY_MODEL,
}

REPORT_ID_COLUMNS = ["report_id", "ReportId", "report_key", "ReportKey"]
REPORT_NAME_COLUMNS = ["report_name", "ReportName", "name", "Report"]
USER_ID_COLUMNS = ["user_id", "UserId", "user_key", "UserKey", "unique_user"]
VIEW_COLUMNS = ["views", "total_views", "view_count", "daily_views", "forecast_views", "actual"]
RELIABILITY_COLUMNS = [
    "forecast_reliable",
    "forecast_reliability",
    "reliability",
    "reliable_forecast",
    "publish_flag",
]
SEGMENT_COLUMNS = ["segment", "report_segment", "health_status"]
DIAGNOSTIC_COLUMNS = [
    "diagnostic_flag",
    "diagnostic_summary",
    "primary_diagnostic",
    "main_diagnostic",
]
DATE_COLUMNS = ["date", "Date", "creation_time", "CreationTime", "timestamp", "Timestamp"]


def first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first matching column name from a flexible candidate list."""
    for column in candidates:
        if column in df.columns:
            return column
    return None


def truthy(value: Any) -> bool:
    """Interpret common boolean-like CSV values consistently."""
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y", "reliable"}
    if pd.isna(value):
        return False
    return bool(value)


def has_positive_usage(df: pd.DataFrame) -> pd.Series:
    """Return a row-level usage mask from the best available usage column."""
    view_col = first_existing_column(df, VIEW_COLUMNS)
    if view_col is None:
        return pd.Series(True, index=df.index)
    return pd.to_numeric(df[view_col], errors="coerce").fillna(0).gt(0)


def get_analysis_date_range(df: pd.DataFrame) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    """Return the observed start and end dates from the first available date column."""
    date_col = first_existing_column(df, DATE_COLUMNS)
    if df.empty or date_col is None:
        return None, None

    dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().normalize(), dates.max().normalize()


def project_root() -> Path:
    """Return the repository root when running from Streamlit's script path."""
    return Path(__file__).resolve().parents[3]


def read_csv(path: Path) -> pd.DataFrame:
    """Read a CSV file, returning an empty frame if it is missing or invalid."""
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def read_json_records(path: Path) -> pd.DataFrame:
    """Read JSON list/dict records into a DataFrame with graceful fallback."""
    if not path.exists():
        return pd.DataFrame()
    try:
        with path.open("r", encoding="utf-8") as file:
            payload: Any = json.load(file)
    except Exception:
        return pd.DataFrame()

    if isinstance(payload, list):
        return pd.DataFrame(payload)
    if isinstance(payload, dict):
        records = payload.get("reports") or payload.get("insights")
        if isinstance(records, list):
            return pd.DataFrame(records)
        return pd.DataFrame([payload])
    return pd.DataFrame()


def normalize_forecast_date(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure forecast DataFrames expose a canonical ``Date`` column.

    Production forecast outputs use ``forecast_date``; legacy outputs use ``Date``.
    This function promotes ``forecast_date`` → ``Date`` when ``Date`` is absent,
    then converts the result to datetime.  Existing valid ``Date`` columns are
    left untouched (only converted to datetime if not already).

    Invalid date values are coerced to NaT rather than raising.
    """
    if df.empty:
        return df
    result = df.copy()
    if "Date" not in result.columns and "forecast_date" in result.columns:
        result["Date"] = result["forecast_date"]
    if "Date" in result.columns:
        result["Date"] = pd.to_datetime(result["Date"], errors="coerce")
    return result


def normalize_report_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add standard id/name columns when files use alternate source names."""
    if df.empty:
        return df

    renamed = df.copy()
    report_id_col = first_existing_column(renamed, REPORT_ID_COLUMNS)
    report_name_col = first_existing_column(renamed, REPORT_NAME_COLUMNS)
    user_id_col = first_existing_column(renamed, USER_ID_COLUMNS)

    if report_id_col and "report_id" not in renamed.columns:
        renamed["report_id"] = renamed[report_id_col]
    if report_name_col and "report_name" not in renamed.columns:
        renamed["report_name"] = renamed[report_name_col]
    if user_id_col and "user_id" not in renamed.columns:
        renamed["user_id"] = renamed[user_id_col]

    for date_col in DATE_COLUMNS:
        if date_col in renamed.columns:
            renamed[date_col] = pd.to_datetime(renamed[date_col], errors="coerce")
    if "target_date" in renamed.columns:
        renamed["target_date"] = pd.to_datetime(renamed["target_date"], errors="coerce")
    return renamed


# ---------------------------------------------------------------------------
# Monitoring file loading helpers
# ---------------------------------------------------------------------------

def check_monitoring_file(
    path: Path,
    required_cols: set[str],
) -> tuple[pd.DataFrame, "MonitoringStatus"]:
    """Read one monitoring CSV and return (DataFrame, status).

    The four possible statuses reflect distinct operational states:

    ``"absent"``         — the file does not exist; monitoring has not run.
    ``"empty"``          — the file exists but has no data rows (pipeline ran
                           but produced nothing to write, e.g. no realized data).
    ``"invalid_schema"`` — the file has rows but is missing required columns;
                           likely a schema drift or partial write.
    ``"ok"``             — file has rows and passes column validation.

    The returned DataFrame is always safe to pass to pandas operations:
    - For "absent" and "empty" it is an empty DataFrame.
    - For "invalid_schema" the raw DataFrame is returned so callers can inspect
      which columns are present, but status signals that required columns are absent.
    - For "ok" the DataFrame has at minimum the required columns.

    Zero-actual rows are never dropped — callers receive all rows from the file.
    """
    if not path.exists():
        return pd.DataFrame(), "absent"

    df = read_csv(path)

    if df.empty:
        return df, "empty"

    missing = required_cols - set(df.columns)
    if missing:
        return df, "invalid_schema"

    return df, "ok"


def load_monitoring_data(
    root: Path | None = None,
) -> dict[str, dict[str, Any]]:
    """Load all production-pipeline monitoring outputs.

    Returns a dict keyed by logical monitoring name.  Each value is a dict:

        {
            "data":    pd.DataFrame,       # empty when absent/empty/invalid
            "status":  MonitoringStatus,   # "absent" | "empty" | "invalid_schema" | "ok"
            "missing_cols": list[str],     # non-empty only when status == "invalid_schema"
            "path":    Path,               # absolute path that was attempted
        }

    The ``data`` value for ``"invalid_schema"`` files contains the raw (but
    column-incomplete) DataFrame so callers can display a diagnostic message.
    """
    base = root or project_root()
    result: dict[str, dict[str, Any]] = {}

    for key, rel_path in MONITORING_PATHS.items():
        abs_path = base / rel_path
        required = _MONITORING_REQUIRED_COLS.get(key, set())
        df, status = check_monitoring_file(abs_path, required)

        missing_cols: list[str] = []
        if status == "invalid_schema":
            missing_cols = sorted(required - set(df.columns))

        result[key] = {
            "data":         df if status == "ok" else (df if status == "invalid_schema" else pd.DataFrame()),
            "status":       status,
            "missing_cols": missing_cols,
            "path":         abs_path,
        }

    return result


# ---------------------------------------------------------------------------
# report_features schema guard
# ---------------------------------------------------------------------------

# Columns that must be present in report_features when the file is non-empty.
# Raising here instead of displaying NaN surfaces a rename early and clearly.
_REPORT_FEATURES_REQUIRED_COLS: set[str] = {
    # v2.0.0 schema — top_1_user_view_share moved to mart_report_engagement
    "report_id",
    "analytics_as_of_date",
    "schema_version",
    "historical_usage_status",
}


def validate_report_features_schema(df: pd.DataFrame) -> None:
    """Raise ValueError if *df* is missing required report_features columns.

    Called by ``load_app_data`` whenever ``report_features`` is non-empty so
    that a future rename of ``top_1_user_view_share`` fails clearly rather than
    silently displaying NaN in the Streamlit app.

    Does nothing when *df* is empty (file absent or unreadable).
    """
    if df.empty:
        return
    missing = _REPORT_FEATURES_REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(
            "report_features is missing required column(s): "
            f"{sorted(missing)}. "
            "If the field was renamed, update src/app/streamlit_app.py and "
            "this validation list together."
        )


# ---------------------------------------------------------------------------
# Canonical report analytics mart validation
# ---------------------------------------------------------------------------

# Columns that must be present when mart_report_analytics.csv is non-empty.
# A missing required column raises ValueError immediately — it signals schema
# drift that must be fixed rather than silently degraded.
_MART_ANALYTICS_REQUIRED_COLS: set[str] = {
    "report_id",
    "analytics_run_id",
    "analytics_as_of_date",
}

# Columns that are expected in the mart but whose absence is tolerated.
# The app degrades gracefully (shows "N/A" or omits the section) rather than
# crashing.  This list is informational only — it is not enforced at load time.
_MART_ANALYTICS_EXPECTED_COLS: set[str] = {
    "report_name",
    "overall_report_status",
    "overall_review_priority",
    "recommended_report_action",
    "forecast_outlook_status",
    "model_diagnostic_status",
    "historical_usage_status",
}


def validate_mart_analytics_schema(df: pd.DataFrame) -> None:
    """Raise ValueError if *df* is non-empty and missing required mart columns.

    Empty DataFrames (file absent or unreadable) are silently accepted — the
    mart is optional from the app's perspective; its absence degrades gracefully.
    """
    if df.empty:
        return
    missing = _MART_ANALYTICS_REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(
            "mart_report_analytics.csv is missing required column(s): "
            f"{sorted(missing)}. "
            "If the mart schema changed, update _MART_ANALYTICS_REQUIRED_COLS "
            "in src/app/utils/load_data.py."
        )
    duplicates = df[df["report_id"].duplicated(keep=False)]
    if not duplicates.empty:
        dup_ids = duplicates["report_id"].unique().tolist()
        raise ValueError(
            f"mart_report_analytics.csv contains duplicate report_id values: {dup_ids}. "
            "The mart must have exactly one row per report."
        )


@_cache
def load_app_data(root: Path | None = None) -> dict[str, pd.DataFrame]:
    """Load all app inputs from outputs/* into a keyed dictionary.

    Caching
    -------
    When called from a running Streamlit session this function is cached via
    ``@st.cache_data``.  The cache is keyed on the ``root`` argument (defaults
    to the project root derived from the file path).  To force a reload during
    development press the **⟳ Rerun** button while holding Shift, or call
    ``st.cache_data.clear()`` from a Python console.

    Outside Streamlit (tests, scripts) the decorator is a no-op so the function
    behaves as a plain function.

    Monitoring data
    ---------------
    All seven monitoring files are loaded and available under their logical keys
    (e.g. ``data["perf_by_run"]``, ``data["perf_by_report"]``).

    The full structured monitoring result (including per-file status) is stored
    under ``data["_monitoring"]`` for callers that need to distinguish absent
    from invalid.  The value is a dict produced by ``load_monitoring_data``.

    Forecast file preference
    ------------------------
    If ``production_forecasts_latest.csv`` loaded successfully (status "ok"),
    it replaces the legacy ``data["forecasts"]``.  If the production file is
    present but has an invalid schema a ``ValueError`` is raised immediately —
    there is no silent fallback to legacy data.
    """
    base = root or project_root()
    data: dict[str, pd.DataFrame] = {}

    for key, relative_path in OUTPUT_PATHS.items():
        path = base / relative_path
        if path.suffix == ".json":
            data[key] = normalize_report_columns(read_json_records(path))
        else:
            data[key] = normalize_report_columns(read_csv(path))

    validate_report_features_schema(data.get("report_features", pd.DataFrame()))
    validate_mart_analytics_schema(data.get("report_analytics", pd.DataFrame()))

    # Load monitoring outputs and promote to top-level keys
    monitoring = load_monitoring_data(base)
    data["_monitoring"] = monitoring  # type: ignore[assignment]

    for key, entry in monitoring.items():
        data[key] = normalize_report_columns(entry["data"])

    # Prefer production forecast over legacy.
    prod_entry = monitoring["production_forecasts_latest"]
    if prod_entry["status"] == "ok":
        # Production file is present and valid — use it as the canonical forecast source.
        data["forecasts"] = normalize_report_columns(prod_entry["data"])
    elif prod_entry["status"] == "invalid_schema":
        raise ValueError(
            "production_forecasts_latest.csv is present but missing required "
            f"column(s): {prod_entry['missing_cols']}. "
            "Fix the pipeline output or remove the file before opening the app. "
            "The app will not silently fall back to legacy forecast data when a "
            "production file exists with an invalid schema."
        )
    # status "absent" → keep data["forecasts"] from legacy path above (no complaint)
    # status "empty"  → production file exists but has no rows; keep legacy for display
    #                    but do NOT raise — an empty production file is not invalid

    # Ensure all forecast DataFrames expose the canonical "Date" column.
    # Production files use "forecast_date"; legacy files use "Date".
    data["forecasts"] = normalize_forecast_date(data.get("forecasts", pd.DataFrame()))

    return data


def available_reports(data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Build the sidebar report list from every artifact that identifies reports."""
    frames = []
    for df in data.values():
        if {"report_id", "report_name"}.issubset(df.columns):
            frames.append(df[["report_id", "report_name"]])

    if not frames:
        return pd.DataFrame(columns=["report_id", "report_name", "label"])

    reports = (
        pd.concat(frames, ignore_index=True)
        .dropna(subset=["report_id"])
        .drop_duplicates(subset=["report_id"])
        .sort_values("report_id")
    )
    reports["report_name"] = reports["report_name"].fillna(reports["report_id"])
    reports["label"] = reports["report_name"] + " (" + reports["report_id"] + ")"
    return reports.reset_index(drop=True)


def row_for_report(df: pd.DataFrame, report_id: str) -> pd.Series:
    """Return the first matching row for a report, or an empty Series."""
    if df.empty or "report_id" not in df.columns:
        return pd.Series(dtype="object")
    match = df[df["report_id"] == report_id]
    if match.empty:
        return pd.Series(dtype="object")
    return match.iloc[0]


def _historical_forecast_rows(forecasts: pd.DataFrame) -> pd.DataFrame:
    """Limit forecast output rows to dates with observed activity."""
    if forecasts.empty:
        return forecasts
    if "IsForecast" in forecasts.columns:
        forecast_mask = forecasts["IsForecast"].astype(str).str.strip().str.lower().isin(
            {"true", "1", "yes", "y"}
        )
        return forecasts.loc[~forecast_mask]
    if "actual" in forecasts.columns:
        return forecasts.loc[forecasts["actual"].notna()]
    return forecasts


def get_dashboard_analysis_period(data: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Choose the clearest available source for the dashboard analysis date range."""
    sources = [
        ("forecast input feature mart", data.get("forecast_features", pd.DataFrame())),
        (
            "historical forecast output rows",
            _historical_forecast_rows(data.get("forecasts", pd.DataFrame())),
        ),
        ("report usage data", data.get("fact_report_views", pd.DataFrame())),
        ("report-level feature data", data.get("report_features", pd.DataFrame())),
    ]

    for source, df in sources:
        start_date, end_date = get_analysis_date_range(df)
        if start_date is not None and end_date is not None:
            return {
                "start_date": start_date,
                "end_date": end_date,
                "source": source,
                "warnings": [],
            }

    return {
        "start_date": None,
        "end_date": None,
        "source": None,
        "warnings": ["No analysis date column was found in the loaded dashboard data."],
    }


def _distinct_count(df: pd.DataFrame, candidates: list[str]) -> int | None:
    """Count distinct identifiers from the first available candidate column."""
    column = first_existing_column(df, candidates)
    if df.empty or column is None:
        return None
    return int(df[column].dropna().nunique())


def _largest_distinct_count(
    frames: list[tuple[str, pd.DataFrame]], candidates: list[str]
) -> tuple[int | None, str | None]:
    """Find the most complete available source for an entity count."""
    best_count: int | None = None
    best_source: str | None = None
    for source, df in frames:
        count = _distinct_count(df, candidates)
        if count is not None and (best_count is None or count > best_count):
            best_count = count
            best_source = source
    return best_count, best_source


def calculate_user_adoption_metrics(data: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Summarise total and active users using the best available app inputs."""
    warnings: list[str] = []
    frames = [
        ("user catalogue", data.get("dim_user", pd.DataFrame())),
        ("user summaries", data.get("user_features", pd.DataFrame())),
        ("user groups", data.get("user_segments", pd.DataFrame())),
        ("view history", data.get("fact_report_views", pd.DataFrame())),
    ]

    total_users = _distinct_count(data.get("dim_user", pd.DataFrame()), USER_ID_COLUMNS)
    total_source = "user catalogue"
    assumption = "Total users were counted from the user catalogue."
    if total_users is None:
        total_users, total_source = _largest_distinct_count(frames[1:], USER_ID_COLUMNS)
        if total_source:
            assumption = f"Total users were estimated from the broadest available {total_source}."
            warnings.append(
                "The user catalogue was unavailable, so total users were estimated from summary data."
            )
        else:
            assumption = "Total users could not be calculated from the available files."
            warnings.append("No user catalogue or user summary was available.")

    events = data.get("fact_report_views", pd.DataFrame())
    user_features = data.get("user_features", pd.DataFrame())
    active_users = None
    active_source = None
    if not events.empty and first_existing_column(events, USER_ID_COLUMNS):
        active_users = _distinct_count(events.loc[has_positive_usage(events)], USER_ID_COLUMNS)
        active_source = "view history"
    elif not user_features.empty and first_existing_column(user_features, USER_ID_COLUMNS):
        active_users = _distinct_count(
            user_features.loc[has_positive_usage(user_features)], USER_ID_COLUMNS
        )
        active_source = "user summaries"
        warnings.append("Active users were estimated from user summary data.")
    elif total_users is not None:
        active_users = total_users
        active_source = total_source
        warnings.append("Detailed user view history was unavailable, so active users equal total users.")

    active_pct = active_users / total_users if total_users and active_users is not None else None
    return {
        "total_users": total_users,
        "active_users": active_users,
        "active_user_rate": active_pct,
        "total": total_users,
        "active": active_users,
        "active_pct": active_pct,
        "source": total_source,
        "active_source": active_source,
        "assumption": assumption,
        "warnings": warnings,
    }


def calculate_report_adoption_metrics(data: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Summarise total and used reports using dimensions, events, or outputs."""
    warnings: list[str] = []
    report_frames = [
        ("report catalogue", data.get("dim_report", pd.DataFrame())),
        ("report summaries", data.get("report_features", pd.DataFrame())),
        ("report groups", data.get("segments", pd.DataFrame())),
        ("forecast summaries", data.get("metrics", pd.DataFrame())),
        ("diagnostics", data.get("diagnostics", pd.DataFrame())),
        ("forecast results", data.get("forecasts", pd.DataFrame())),
    ]

    total_reports = _distinct_count(data.get("dim_report", pd.DataFrame()), REPORT_ID_COLUMNS)
    total_source = "report catalogue"
    assumption = "Total reports were counted from the report catalogue."
    if total_reports is None:
        total_reports, total_source = _largest_distinct_count(report_frames[1:], REPORT_ID_COLUMNS)
        if total_source:
            assumption = f"Total reports were estimated from the broadest available {total_source}."
            warnings.append(
                "The report catalogue was unavailable, so total reports were estimated from summary data."
            )
        else:
            assumption = "Total reports could not be calculated from the available files."
            warnings.append("No report catalogue or report summary was available.")

    events = data.get("fact_report_views", pd.DataFrame())
    features = data.get("report_features", pd.DataFrame())
    active_reports = None
    active_source = None
    if not events.empty and first_existing_column(events, REPORT_ID_COLUMNS):
        active_reports = _distinct_count(events.loc[has_positive_usage(events)], REPORT_ID_COLUMNS)
        active_source = "view history"
    elif not features.empty and first_existing_column(features, REPORT_ID_COLUMNS):
        active_reports = _distinct_count(features.loc[has_positive_usage(features)], REPORT_ID_COLUMNS)
        active_source = "report summaries"
        warnings.append("Used reports were estimated from report summary data.")
    elif total_reports is not None:
        active_reports = total_reports
        active_source = total_source
        warnings.append("Detailed report view history was unavailable, so used reports equal total reports.")

    active_pct = active_reports / total_reports if total_reports and active_reports is not None else None
    return {
        "total_reports": total_reports,
        "used_reports": active_reports,
        "report_usage_rate": active_pct,
        "total": total_reports,
        "active": active_reports,
        "active_pct": active_pct,
        "source": total_source,
        "active_source": active_source,
        "assumption": assumption,
        "warnings": warnings,
    }


def calculate_forecast_reliability_summary(data: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Return the share of reports classified as forecast reliable."""
    metrics = data.get("metrics", pd.DataFrame())
    if metrics.empty:
        return {
            "total": None,
            "reliable": None,
            "reliable_pct": None,
            "assumption": "Forecast metrics were unavailable.",
            "warnings": ["Forecast reliability could not be calculated because metrics are missing."],
        }

    reliability_col = first_existing_column(metrics, RELIABILITY_COLUMNS)
    if reliability_col:
        total = int(metrics[reliability_col].notna().sum())
        reliable = int(metrics[reliability_col].map(truthy).sum())
        pct = reliable / total if total else None
        return {
            "total": total,
            "reliable": reliable,
            "reliable_pct": pct,
            "assumption": f"Reliability is counted from {reliability_col}.",
            "warnings": [],
        }

    if "selected_wape" in metrics.columns:
        reliable_mask = pd.to_numeric(metrics["selected_wape"], errors="coerce").le(0.30)
        total = int(metrics["selected_wape"].notna().sum())
        reliable = int(reliable_mask.sum())
        pct = reliable / total if total else None
        return {
            "total": total,
            "reliable": reliable,
            "reliable_pct": pct,
            "assumption": "Reliability is approximated from selected_wape <= 30%.",
            "warnings": ["Reliability flag was unavailable, so selected WAPE was used as a fallback."],
        }

    return {
        "total": None,
        "reliable": None,
        "reliable_pct": None,
        "assumption": "No reliability flag or WAPE field was available.",
        "warnings": ["Forecast reliability could not be calculated from the available columns."],
    }


def _report_lookup(data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Create a report id/name lookup from all report-aware app inputs."""
    reports = available_reports(data)
    if reports.empty:
        return pd.DataFrame(columns=["report_id", "report_name"])
    return reports[["report_id", "report_name"]]


def get_at_risk_reports(data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Build a compact at-risk report list from diagnostics and reliability flags."""
    reports = _report_lookup(data)
    if reports.empty:
        return pd.DataFrame(
            columns=[
                "report_id",
                "report_name",
                "segment_or_health_status",
                "diagnostic",
                "forecast_reliability",
            ]
        )

    risk = reports.copy()
    risk["_risk"] = False

    diagnostics = data.get("diagnostics", pd.DataFrame())
    if not diagnostics.empty and "report_id" in diagnostics.columns:
        diag_cols = ["report_id"] + [
            col
            for col in [
                "report_name",
                "report_segment",
                "health_status",
                "diagnostic_flag",
                "diagnostic_summary",
                "primary_diagnostic",
                "main_diagnostic",
                "performance_issue",
                "engagement_issue",
                "dependency_risk",
                "inactive_risk",
            ]
            if col in diagnostics.columns
        ]
        diag = diagnostics[diag_cols].drop_duplicates(subset=["report_id"])
        risk = risk.merge(diag, on="report_id", how="left", suffixes=("", "_diagnostic"))
        risk_flag_cols = [
            col for col in diagnostics.columns if col.endswith(("issue", "risk"))
        ]
        if risk_flag_cols:
            flagged = diagnostics.set_index("report_id")[risk_flag_cols].apply(
                lambda row: any(truthy(value) for value in row), axis=1
            )
            risk["_risk"] = risk["_risk"] | risk["report_id"].map(flagged).fillna(False)

    segments = data.get("segments", pd.DataFrame())
    if not segments.empty and "report_id" in segments.columns:
        segment_col = first_existing_column(segments, SEGMENT_COLUMNS)
        if segment_col and segment_col not in risk.columns:
            risk = risk.merge(
                segments[["report_id", segment_col]].drop_duplicates(subset=["report_id"]),
                on="report_id",
                how="left",
            )
        if segment_col:
            risky_segments = {"at_risk", "inactive", "low_adoption", "declining", "unhealthy"}
            segment_flags = segments.set_index("report_id")[segment_col].astype(str).str.lower()
            risk["_risk"] = risk["_risk"] | risk["report_id"].map(
                segment_flags.isin(risky_segments)
            ).fillna(False)

    metrics = data.get("metrics", pd.DataFrame())
    reliability_col = first_existing_column(metrics, RELIABILITY_COLUMNS) if not metrics.empty else None
    if reliability_col and "report_id" in metrics.columns:
        rel = metrics[["report_id", reliability_col]].drop_duplicates(subset=["report_id"])
        rel["forecast_reliability"] = rel[reliability_col].map(
            lambda value: "Reliable" if truthy(value) else "At risk"
        )
        risk = risk.merge(rel[["report_id", "forecast_reliability"]], on="report_id", how="left")
        reliability_flags = metrics.set_index("report_id")[reliability_col].map(
            lambda value: not truthy(value)
        )
        risk["_risk"] = risk["_risk"] | risk["report_id"].map(reliability_flags).fillna(False)

    segment_col = first_existing_column(risk, SEGMENT_COLUMNS)
    diagnostic_col = first_existing_column(risk, DIAGNOSTIC_COLUMNS)
    output = risk.loc[risk["_risk"]].copy()
    output["segment_or_health_status"] = output[segment_col] if segment_col else pd.NA
    output["diagnostic"] = output[diagnostic_col] if diagnostic_col else pd.NA
    if "forecast_reliability" not in output.columns:
        output["forecast_reliability"] = pd.NA

    columns = [
        "report_id",
        "report_name",
        "segment_or_health_status",
        "diagnostic",
        "forecast_reliability",
    ]
    return output[columns].sort_values("report_id").reset_index(drop=True)
