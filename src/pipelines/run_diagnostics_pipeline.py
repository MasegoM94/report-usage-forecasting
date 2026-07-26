"""Build the report diagnostics layer from canonical Sprint 7 context outputs."""
import uuid
from pathlib import Path
import pandas as pd
from src.analytics.report_diagnostics import (
    build_report_diagnostics, persist_report_diagnostics,
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
    if features_df is None:
        raise FileNotFoundError("report_features.csv is required")

    forecast_df = _load("outputs/analytics/report_forecast_outlook.csv")
    health_df = _load("outputs/analytics/report_model_health_context.csv")
    engagement_df = _load("outputs/analytics/report_engagement_context.csv")
    metadata_df = _load("outputs/analytics/report_metadata_context.csv")

    diag_df = build_report_diagnostics(
        features_df, forecast_df, health_df, engagement_df, metadata_df, run_id
    )
    out_path = persist_report_diagnostics(diag_df, project_root)
    print(f"Diagnostics: {len(diag_df)} reports -> {out_path}")
    print(
        diag_df[
            ["report_id", "overall_diagnostic_severity", "primary_diagnostic", "recommended_diagnostic_action"]
        ].to_string()
    )


if __name__ == "__main__":
    run_pipeline()
