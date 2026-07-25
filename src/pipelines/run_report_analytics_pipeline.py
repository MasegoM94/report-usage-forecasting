"""Run the report-level behavioural analytics pipeline.

Usage:
    python -m src.pipelines.run_report_analytics_pipeline
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone, date
from pathlib import Path

import pandas as pd

from src.analytics.report_diagnostics import build_report_diagnostics
from src.analytics.report_features import build_report_features
from src.analytics.report_segmentation import build_report_segments


def get_project_root() -> Path:
    """Return the repository root based on this pipeline file location."""
    return Path(__file__).resolve().parents[2]


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    """Read a CSV when it exists, otherwise return an empty DataFrame."""
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def choose_daily_usage_table(processed_dir: Path) -> Path:
    """Choose the richest available processed daily report usage table.

    Preference order:
    1. mart_report_daily_context.csv — canonical wide diagnostic context mart
    2. mart_forecast_features.csv — legacy wide mart (backward compat)
    3. mart_report_daily_adoption.csv
    4. mart_report_daily_adoption_ts_features.csv
    """
    candidates = [
        processed_dir / "mart_report_daily_context.csv",
        processed_dir / "mart_forecast_features.csv",
        processed_dir / "mart_report_daily_adoption.csv",
        processed_dir / "mart_report_daily_adoption_ts_features.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate

    available = "\n".join(str(path) for path in sorted(processed_dir.glob("*.csv")))
    raise FileNotFoundError(
        "No suitable processed daily report usage table was found.\n\n"
        f"Available processed CSVs:\n{available or 'No processed CSV files found.'}"
    )


def run_pipeline(project_root: Path | None = None) -> dict[str, Path]:
    """Build report features, segments, and diagnostics, then save CSV outputs."""
    root = project_root or get_project_root()
    processed_dir = root / "data" / "processed"
    output_paths = {
        "features": root / "outputs" / "metrics" / "report_features.csv",
        "segments": root / "outputs" / "segments" / "report_segments.csv",
        "diagnostics": root / "outputs" / "diagnostics" / "report_diagnostics.csv",
    }

    for output_path in output_paths.values():
        output_path.parent.mkdir(parents=True, exist_ok=True)

    daily_usage_path = choose_daily_usage_table(processed_dir)
    daily_usage = pd.read_csv(daily_usage_path)
    fact_report_views = read_csv_if_exists(processed_dir / "fact_report_views.csv")
    report_performance = read_csv_if_exists(processed_dir / "mart_report_performance.csv")
    dim_report = read_csv_if_exists(processed_dir / "dim_report.csv")
    dim_date = read_csv_if_exists(processed_dir / "dim_date.csv")

    analytics_run_id = str(uuid.uuid4())
    generated_at = datetime.now(timezone.utc).isoformat()
    analytics_as_of_date = str(date.today())

    report_features = build_report_features(
        daily_adoption=daily_usage,
        fact_report_views=fact_report_views,
        report_performance=report_performance,
        dim_report=dim_report,
        dim_date=dim_date,
    )
    # Attach lineage fields at the persistence layer (keeps builder pure).
    report_features.insert(1, "analytics_run_id", analytics_run_id)
    report_features.insert(2, "generated_at", generated_at)
    report_features.insert(3, "analytics_as_of_date", analytics_as_of_date)

    # Build downstream outputs before dropping deprecated columns
    # (report_segmentation uses repeat_rate internally).
    report_segments = build_report_segments(report_features)
    report_diagnostics = build_report_diagnostics(report_features, report_segments)

    # Drop deprecated columns at the persistence layer only.
    # repeat_rate is superseded by returning_user_share_28d in the engagement mart.
    _DEPRECATED_COLS = {"repeat_rate", "usage_change_pct", "latest_views",
                        "prior_views", "top_user_concentration"}
    report_features_out = report_features.drop(
        columns=[c for c in _DEPRECATED_COLS if c in report_features.columns]
    )

    report_features_out.to_csv(output_paths["features"], index=False)
    report_segments.to_csv(output_paths["segments"], index=False)
    report_diagnostics.to_csv(output_paths["diagnostics"], index=False)

    print("Report analytics pipeline complete.")
    print(f"Input daily usage table: {daily_usage_path}")
    for label, output_path in output_paths.items():
        print(f"{label}: {output_path}")

    return output_paths


if __name__ == "__main__":
    run_pipeline()
