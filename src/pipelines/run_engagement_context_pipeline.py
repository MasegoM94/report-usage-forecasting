"""Build the report engagement context from the engagement mart."""
import uuid
from pathlib import Path
import pandas as pd
from src.analytics.report_engagement_context import (
    build_report_engagement_context, persist_report_engagement_context,
)


def run_pipeline(project_root: Path = None):
    project_root = project_root or Path.cwd()
    run_id = str(uuid.uuid4())

    eng_path = project_root / "outputs" / "analytics" / "mart_report_engagement.csv"
    features_path = project_root / "outputs" / "metrics" / "report_features.csv"

    eng_df = pd.read_csv(eng_path)
    print(f"Loaded engagement mart: {len(eng_df)} rows")

    features_df = None
    if features_path.exists():
        features_df = pd.read_csv(features_path)
        print(f"Loaded report features: {len(features_df)} rows")

    ctx_df = build_report_engagement_context(eng_df, features_df, run_id)
    out_path = persist_report_engagement_context(ctx_df, project_root)
    print(f"Engagement context: {len(ctx_df)} reports -> {out_path}")
    print(ctx_df[["report_id", "overall_engagement_status", "engagement_interpretation_status"]].to_string())


if __name__ == "__main__":
    run_pipeline()
