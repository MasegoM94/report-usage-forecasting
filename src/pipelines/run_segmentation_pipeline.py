"""Build report segments from canonical Sprint 7 context outputs."""
import uuid
from pathlib import Path
import pandas as pd
from src.analytics.report_segmentation import (
    build_report_segments, persist_report_segments,
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

    seg_df = build_report_segments(
        features_df, forecast_df, health_df, engagement_df, metadata_df, diagnostics_df, run_id
    )
    out_path = persist_report_segments(seg_df, project_root)
    print(f"Segments: {len(seg_df)} reports -> {out_path}")
    print(seg_df[["report_id", "primary_report_segment", "segment_evidence_status"]].to_string())


if __name__ == "__main__":
    run_pipeline()
