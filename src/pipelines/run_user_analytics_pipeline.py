"""Run the user-level behavioural analytics pipeline.

Usage:
    python -m src.pipelines.run_user_analytics_pipeline
"""

from __future__ import annotations

import uuid
from datetime import date
from pathlib import Path

import pandas as pd

from src.analytics.user_features import build_user_features
from src.analytics.user_segmentation import build_user_segments
from src.analytics.report_user_daily import (
    persist_report_user_daily_outputs,
    run_report_user_daily_pipeline,
)


def get_project_root() -> Path:
    """Return the repository root based on this pipeline file location."""
    return Path(__file__).resolve().parents[2]


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    """Read a CSV when it exists, otherwise return an empty DataFrame."""
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def choose_user_usage_table(processed_dir: Path) -> Path:
    """Choose the best available processed user activity table."""
    candidates = [
        processed_dir / "fact_report_views.csv",
        processed_dir / "fact_page_views.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate

    available = "\n".join(str(path) for path in sorted(processed_dir.glob("*.csv")))
    raise FileNotFoundError(
        "No suitable processed user activity table was found.\n\n"
        f"Available processed CSVs:\n{available or 'No processed CSV files found.'}"
    )


def run_pipeline(project_root: Path | None = None) -> dict[str, Path]:
    """Build user features and segments, then save CSV outputs."""
    root = project_root or get_project_root()
    processed_dir = root / "data" / "processed"
    output_paths = {
        "features": root / "outputs" / "metrics" / "user_features.csv",
        "segments": root / "outputs" / "segments" / "user_segments.csv",
    }

    for output_path in output_paths.values():
        output_path.parent.mkdir(parents=True, exist_ok=True)

    usage_path = choose_user_usage_table(processed_dir)
    usage_events = pd.read_csv(usage_path)
    dim_user = read_csv_if_exists(processed_dir / "dim_user.csv")
    dim_date = read_csv_if_exists(processed_dir / "dim_date.csv")

    user_features = build_user_features(
        usage_events=usage_events,
        dim_user=dim_user,
        dim_date=dim_date,
    )
    user_segments = build_user_segments(user_features)

    user_features.to_csv(output_paths["features"], index=False)
    user_segments.to_csv(output_paths["segments"], index=False)

    print("User analytics pipeline complete.")
    print(f"Input user activity table: {usage_path}")
    for label, output_path in output_paths.items():
        print(f"{label}: {output_path}")

    # ── Step: Report-user-day mart ──────────────────────────────────────────
    try:
        _run_report_user_daily_step(root, processed_dir, usage_path)
    except Exception as exc:  # noqa: BLE001
        print(f"[WARNING] report_user_daily step failed: {exc}")

    return output_paths


def _run_report_user_daily_step(
    root: Path,
    processed_dir: Path,
    usage_path: Path,
) -> None:
    """Build and persist the report-user-day mart and quality output."""
    fact_df = pd.read_csv(usage_path)

    # Load report metadata if available (dim_report.csv)
    report_meta_path = processed_dir / "dim_report.csv"
    report_meta_df: pd.DataFrame | None = None
    if report_meta_path.exists():
        meta = pd.read_csv(report_meta_path)
        if "report_id" in meta.columns and "report_name" in meta.columns:
            report_meta_df = meta[["report_id", "report_name"]].drop_duplicates("report_id")

    analytics_run_id = str(uuid.uuid4())

    mart_df, quality_df = run_report_user_daily_pipeline(
        fact_df=fact_df,
        analytics_run_id=analytics_run_id,
        as_of_date=date.today(),
        report_meta_df=report_meta_df,
        source_file=str(usage_path),
    )

    paths = persist_report_user_daily_outputs(mart_df, quality_df, root)

    print(
        f"report_user_daily mart: {len(mart_df):,} rows → {paths['mart']}\n"
        f"report_user_data_quality: {len(quality_df):,} rows → {paths['quality']}"
    )


if __name__ == "__main__":
    run_pipeline()
