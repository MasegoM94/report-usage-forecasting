"""Build the report metadata context from dim_report and report_features."""
import uuid
from pathlib import Path
import pandas as pd
from src.analytics.report_metadata_context import (
    build_report_metadata_context, persist_report_metadata_context,
    METADATA_CONTEXT_SCHEMA_VERSION,
)


def run_pipeline(project_root: Path = None):
    project_root = project_root or Path.cwd()
    run_id = str(uuid.uuid4())

    dim_path = project_root / "data" / "processed" / "dim_report.csv"
    features_path = project_root / "outputs" / "metrics" / "report_features.csv"

    dim_df = pd.read_csv(dim_path)
    print(f"Loaded dim_report: {len(dim_df)} rows")

    features_df = None
    if features_path.exists():
        features_df = pd.read_csv(features_path)
        as_of = features_df["analytics_as_of_date"].iloc[0] if "analytics_as_of_date" in features_df.columns else str(pd.Timestamp.now().date())
        print(f"Loaded report_features: {len(features_df)} rows, as_of={as_of}")
    else:
        as_of = str(pd.Timestamp.now().date())

    ctx_df = build_report_metadata_context(dim_df, features_df, as_of, run_id)
    out_path = persist_report_metadata_context(ctx_df, project_root)
    print(f"Metadata context: {len(ctx_df)} reports → {out_path}")
    print(ctx_df[["report_id", "report_name", "metadata_completeness_score", "metadata_interpretation_status"]].to_string())


if __name__ == "__main__":
    run_pipeline()
