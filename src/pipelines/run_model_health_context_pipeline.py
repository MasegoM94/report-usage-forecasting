"""Build report model-health context from diagnostics and forecast outlook."""
import uuid
from pathlib import Path
import pandas as pd
from src.analytics.report_model_health_context import (
    ModelHealthContextConfig, build_report_model_health_context,
    persist_report_model_health_context,
)


def run_pipeline(project_root: Path = None):
    project_root = project_root or Path.cwd()
    run_id = str(uuid.uuid4())
    cfg = ModelHealthContextConfig()

    # Load model diagnostics (required)
    diag_path = project_root / "outputs" / "diagnostics" / "report_model_diagnostics.csv"
    diag_df = pd.read_csv(diag_path)
    print(f"Loaded model diagnostics: {len(diag_df)} rows")

    # Load forecast outlook (optional)
    outlook_path = project_root / "outputs" / "analytics" / "report_forecast_outlook.csv"
    outlook_df = None
    if outlook_path.exists():
        outlook_df = pd.read_csv(outlook_path)
        print(f"Loaded forecast outlook: {len(outlook_df)} rows")
    else:
        print("Warning: forecast outlook not found; interpretation will use model diagnostics only")

    ctx_df = build_report_model_health_context(diag_df, outlook_df, cfg, run_id)
    out_path = persist_report_model_health_context(ctx_df, project_root)
    print(f"Model health context: {len(ctx_df)} reports → {out_path}")
    print(ctx_df[["report_id", "model_diagnostic_status", "forecast_interpretation_status"]].to_string())


if __name__ == "__main__":
    run_pipeline()
