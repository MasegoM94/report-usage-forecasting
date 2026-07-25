"""Run the report-level behavioural analytics pipeline.

Usage:
    python -m src.pipelines.run_report_analytics_pipeline
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.analytics.engagement_windows import (
    EngagementWindowConfig,
    resolve_analytics_as_of_date,
)
from src.analytics.report_features import (
    ReportFeaturesConfig,
    build_report_features,
    persist_report_features,
)


def get_project_root() -> Path:
    """Return the repository root based on this pipeline file location."""
    return Path(__file__).resolve().parents[2]


def read_csv_if_exists(path: Path) -> pd.DataFrame:
    """Read a CSV when it exists, otherwise return an empty DataFrame."""
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def run_pipeline(project_root: Path | None = None) -> dict[str, Path]:
    """Build report features and save to CSV output."""
    root = project_root or get_project_root()
    processed_dir = root / "data" / "processed"
    analytics_dir = root / "outputs" / "analytics"
    output_paths: dict[str, Path] = {}

    analytics_run_id = str(uuid.uuid4())

    # ── Load source: prefer mart_report_user_daily (has usage_date + daily_views) ──
    mart_path = analytics_dir / "mart_report_user_daily.csv"
    if mart_path.exists():
        mart_df = pd.read_csv(mart_path, parse_dates=["usage_date"])
        print(f"Loaded mart_report_user_daily: {len(mart_df):,} rows")
    else:
        # Fallback: aggregate fact_report_views with dim_date for date resolution
        frv_path = processed_dir / "fact_report_views.csv"
        if not frv_path.exists():
            raise FileNotFoundError(
                "Neither mart_report_user_daily.csv (outputs/analytics/) nor "
                "fact_report_views.csv (data/processed/) was found."
            )
        mart_df = pd.read_csv(frv_path)
        # fact_report_views uses date_key (YYYYMMDD int) — convert to date
        if "date_key" in mart_df.columns and "usage_date" not in mart_df.columns:
            mart_df["usage_date"] = pd.to_datetime(
                mart_df["date_key"].astype(str), format="%Y%m%d", errors="coerce"
            )
        print(f"Loaded fact_report_views as fallback: {len(mart_df):,} rows")

    # ── Resolve analytics_as_of_date from source (never date.today()) ────────
    _win_cfg = EngagementWindowConfig()
    analytics_as_of_date, as_of_date_policy, _, source_max_usage_date = (
        resolve_analytics_as_of_date(mart_df, _win_cfg, analytics_run_id)
    )
    print(f"analytics_as_of_date: {analytics_as_of_date}  (policy={as_of_date_policy})")

    # ── Dim report ────────────────────────────────────────────────────────────
    dim_report = read_csv_if_exists(processed_dir / "dim_report.csv")

    # ── Build report features ─────────────────────────────────────────────────
    rf_cfg = ReportFeaturesConfig()
    report_features = build_report_features(
        mart_df=mart_df,
        dim_report_df=dim_report,
        analytics_as_of_date=analytics_as_of_date,
        analytics_run_id=analytics_run_id,
        cfg=rf_cfg,
    )

    # ── Persist ──────────────────────────────────────────────────────────────
    features_path = persist_report_features(report_features, root)
    output_paths["features"] = features_path
    print(f"Report features: {len(report_features)} rows → {features_path}")

    # ── Legacy downstream outputs (best-effort; fail gracefully) ─────────────
    try:
        from src.analytics.report_segmentation import build_report_segments
        from src.analytics.report_diagnostics import build_report_diagnostics

        seg_path = root / "outputs" / "segments" / "report_segments.csv"
        diag_path = root / "outputs" / "diagnostics" / "report_diagnostics.csv"
        seg_path.parent.mkdir(parents=True, exist_ok=True)
        diag_path.parent.mkdir(parents=True, exist_ok=True)

        report_segments = build_report_segments(report_features)
        report_diagnostics = build_report_diagnostics(report_features, report_segments)
        report_segments.to_csv(seg_path, index=False)
        report_diagnostics.to_csv(diag_path, index=False)
        output_paths["segments"] = seg_path
        output_paths["diagnostics"] = diag_path
        print(f"Segments: {len(report_segments)} rows → {seg_path}")
        print(f"Diagnostics: {len(report_diagnostics)} rows → {diag_path}")
    except Exception as _exc:
        print(f"[WARNING] Legacy segments/diagnostics step skipped: {_exc}")

    print("Report analytics pipeline complete.")
    for label, path in output_paths.items():
        print(f"  {label}: {path}")

    return output_paths


if __name__ == "__main__":
    run_pipeline()
