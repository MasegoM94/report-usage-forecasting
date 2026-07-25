"""Build the report forecast outlook from production forecasts and report features."""
import uuid
from pathlib import Path

import pandas as pd

from src.analytics.report_forecast_outlook import (
    ForecastOutlookConfig,
    build_report_forecast_outlook,
    persist_report_forecast_outlook,
)


def run_pipeline(project_root: Path = None):
    project_root = project_root or Path.cwd()
    run_id = str(uuid.uuid4())
    cfg = ForecastOutlookConfig()

    # Load inputs
    fc_path = project_root / "outputs" / "forecasts" / "report_view_forecasts_latest.csv"
    features_path = project_root / "outputs" / "metrics" / "report_features.csv"
    dim_report_path = project_root / "data" / "processed" / "dim_report.csv"

    fc_df = pd.read_csv(fc_path)
    features_df = pd.read_csv(features_path)
    dim_df = pd.read_csv(dim_report_path)

    outlook_df = build_report_forecast_outlook(fc_df, features_df, dim_df, cfg, run_id)
    out_path = persist_report_forecast_outlook(outlook_df, project_root)
    print(f"Forecast outlook: {len(outlook_df)} reports → {out_path}")
    return out_path


if __name__ == "__main__":
    run_pipeline()
