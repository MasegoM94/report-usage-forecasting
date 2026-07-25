"""Run the Sprint 7 prerequisite readiness check.

Usage:
    python -m src.pipelines.run_readiness_check
"""
from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import pandas as pd

from src.analytics.report_analytics_readiness import (
    build_report_analytics_readiness_summary,
    build_report_spine_reconciliation,
    persist_readiness_outputs,
    validate_report_analytics_prerequisites,
)


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _safe_read(path: Path) -> pd.DataFrame | None:
    if path.exists():
        try:
            return pd.read_csv(path)
        except Exception:
            return None
    return None


def _ensure_canonical_model_diagnostics(root: Path) -> None:
    """Copy _latest file to canonical name if canonical is absent or older."""
    canonical = root / "outputs" / "diagnostics" / "report_model_diagnostics.csv"
    latest = root / "outputs" / "diagnostics" / "report_model_diagnostics_latest.csv"
    if not latest.exists():
        return
    if canonical.exists():
        # Overwrite only if latest is newer
        if latest.stat().st_mtime > canonical.stat().st_mtime:
            shutil.copy2(latest, canonical)
    else:
        shutil.copy2(latest, canonical)


def run_readiness_check(project_root: Path | None = None) -> dict[str, Path]:
    root = project_root or get_project_root()

    # Ensure canonical model diagnostics file exists
    _ensure_canonical_model_diagnostics(root)

    run_id = str(uuid.uuid4())

    # Validate prerequisites
    results = validate_report_analytics_prerequisites(root)
    readiness_df = build_report_analytics_readiness_summary(results, run_id)

    # Temporal alignment check
    features_result = next((r for r in results if r["prerequisite_name"] == "report_features"), None)
    engagement_result = next((r for r in results if r["prerequisite_name"] == "engagement_mart"), None)
    if features_result and engagement_result:
        feat_date = features_result.get("analytics_as_of_date")
        eng_date = engagement_result.get("analytics_as_of_date")
        if feat_date and eng_date and feat_date != eng_date:
            print(
                f"[WARNING] Temporal misalignment: report_features as_of={feat_date}, "
                f"engagement_mart as_of={eng_date}"
            )
        elif feat_date and eng_date:
            print(f"[OK] Temporal alignment: both sources share as_of_date={feat_date}")

    # Spine reconciliation
    dim_report_df = _safe_read(root / "data" / "processed" / "dim_report.csv")
    if dim_report_df is None:
        dim_report_df = pd.DataFrame(columns=["report_id", "report_name"])

    report_features_df = _safe_read(root / "outputs" / "metrics" / "report_features.csv")
    engagement_mart_df = _safe_read(root / "outputs" / "analytics" / "mart_report_engagement.csv")
    model_diagnostics_df = _safe_read(root / "outputs" / "diagnostics" / "report_model_diagnostics.csv")
    _fc_raw = _safe_read(root / "outputs" / "forecasts" / "report_view_forecasts_latest.csv")
    production_forecast_df = _fc_raw if (_fc_raw is not None and "report_id" in _fc_raw.columns) else None

    reconciliation_df = build_report_spine_reconciliation(
        dim_report_df=dim_report_df,
        report_features_df=report_features_df,
        engagement_mart_df=engagement_mart_df,
        model_diagnostics_df=model_diagnostics_df,
        production_forecast_df=production_forecast_df,
    )

    r_path, rec_path = persist_readiness_outputs(readiness_df, reconciliation_df, root)

    print("\n=== Sprint 7 Prerequisite Readiness ===")
    for _, row in readiness_df.iterrows():
        print(
            f"  {row['prerequisite_name']:30s}  status={row['readiness_status']:20s}  "
            f"rows={row['row_count']}  reasons={row['readiness_reasons']}"
        )

    print(f"\nReadiness CSV:       {r_path}")
    print(f"Reconciliation CSV:  {rec_path}")

    statuses = set(readiness_df["readiness_status"])
    if statuses == {"ready"}:
        print("\n[PASS] All prerequisites ready for Sprint 7.")
    else:
        print(f"\n[WARN] Some prerequisites not ready: {statuses}")

    return {"readiness": r_path, "reconciliation": rec_path}


if __name__ == "__main__":
    run_readiness_check()
