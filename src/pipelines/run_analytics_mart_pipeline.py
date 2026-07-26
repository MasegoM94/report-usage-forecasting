"""Build the canonical report analytics mart."""
import uuid
from pathlib import Path
import pandas as pd
from src.analytics.report_analytics_mart import (
    build_mart_report_analytics, persist_mart_report_analytics,
)


def run_pipeline(project_root: Path = None):
    project_root = project_root or Path.cwd()
    run_id = str(uuid.uuid4())

    def _load(path):
        p = project_root / path
        if p.exists():
            df = pd.read_csv(p)
            print(f"Loaded {path}: {len(df)} rows")
            return df
        print(f"MISSING: {path}")
        return None

    features_df = _load("outputs/metrics/report_features.csv")
    forecast_df = _load("outputs/analytics/report_forecast_outlook.csv")
    health_df = _load("outputs/analytics/report_model_health_context.csv")
    engagement_df = _load("outputs/analytics/report_engagement_context.csv")
    metadata_df = _load("outputs/analytics/report_metadata_context.csv")
    diagnostics_df = _load("outputs/analytics/report_diagnostics.csv")
    segments_df = _load("outputs/analytics/report_segments.csv")

    mart_df = build_mart_report_analytics(
        features_df, forecast_df, health_df, engagement_df,
        metadata_df, diagnostics_df, segments_df, run_id
    )
    out_path = persist_mart_report_analytics(mart_df, project_root)
    print(f"Mart: {len(mart_df)} reports, {len(mart_df.columns)} columns -> {out_path}")
    print(mart_df[["report_id", "overall_report_status", "overall_review_priority", "recommended_report_action"]].to_string())


if __name__ == "__main__":
    run_pipeline()
