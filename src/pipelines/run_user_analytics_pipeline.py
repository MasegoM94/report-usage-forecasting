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

    # ── Step: Engagement window boundaries and report history sufficiency ───
    try:
        from src.analytics.engagement_windows import (
            EngagementWindowConfig,
            build_engagement_window_boundaries,
            build_report_history_sufficiency,
            persist_engagement_window_outputs,
        )

        _ew_cfg = EngagementWindowConfig()
        _boundaries = build_engagement_window_boundaries(
            mart_df=mart_df,
            cfg=_ew_cfg,
            analytics_run_id=analytics_run_id,
        )

        # Load report metadata spine
        _report_meta_path = processed_dir / "dim_report.csv"
        _report_meta_df = pd.read_csv(_report_meta_path) if _report_meta_path.exists() else pd.DataFrame()
        if _report_meta_df.empty:
            # Fallback: derive report spine from mart
            _report_meta_df = (
                mart_df[["report_id"]].drop_duplicates()
                .assign(report_name=None, report_activation_date=None)
            )

        _sufficiency_df = build_report_history_sufficiency(
            report_meta_df=_report_meta_df,
            mart_df=mart_df,
            quality_df=quality_df,
            boundaries=_boundaries,
            cfg=_ew_cfg,
            analytics_run_id=analytics_run_id,
        )
        _window_paths = persist_engagement_window_outputs(_boundaries, _sufficiency_df, root)
        print(f"  Engagement windows: {_boundaries.analytics_as_of_date}")
        print(f"  History sufficiency: {len(_sufficiency_df)} reports → {_window_paths['sufficiency']}")
    except Exception as _ew_exc:
        import traceback
        print(f"Warning: engagement windows step failed: {_ew_exc}")
        traceback.print_exc()
        return  # Cannot proceed without window boundaries

    # ── Step: Windowed user engagement metrics ──────────────────────────────
    try:
        from src.analytics.user_engagement_metrics import (
            UserEngagementMetricsConfig,
            build_report_user_activity_metrics,
            persist_report_user_activity_metrics,
        )
        _uem_cfg = UserEngagementMetricsConfig()
        _sufficiency_path = root / "outputs" / "analytics" / "report_engagement_history_sufficiency.csv"
        _boundaries_path = root / "outputs" / "analytics" / "engagement_window_boundaries.csv"
        _sufficiency_input = pd.read_csv(_sufficiency_path) if _sufficiency_path.exists() else pd.DataFrame()
        _boundaries_input = pd.read_csv(_boundaries_path) if _boundaries_path.exists() else pd.DataFrame()
        _activity_df = build_report_user_activity_metrics(
            sufficiency_df=_sufficiency_input,
            mart_df=mart_df,
            quality_df=quality_df,
            boundaries_df=_boundaries_input,
            cfg=_uem_cfg,
            analytics_run_id=analytics_run_id,
        )
        _activity_path = persist_report_user_activity_metrics(_activity_df, root)
        print(f"  User activity metrics: {len(_activity_df)} reports → {_activity_path}")
    except Exception as _uem_exc:
        import traceback
        print(f"Warning: user engagement metrics step failed: {_uem_exc}")
        traceback.print_exc()

    # ── Step: Report engagement cohorts ────────────────────────────────────
    try:
        from src.analytics.user_engagement_cohorts import (
            build_report_engagement_cohorts,
            persist_report_engagement_cohorts,
            CohortConfig,
        )
        _cohort_cfg = CohortConfig()
        _cohort_df = build_report_engagement_cohorts(
            sufficiency_df=_sufficiency_input,
            mart_df=mart_df,
            quality_df=quality_df,
            boundaries_df=_boundaries_input,
            cfg=_cohort_cfg,
            analytics_run_id=analytics_run_id,
        )
        _cohort_path = persist_report_engagement_cohorts(_cohort_df, root)
        print(f"  Engagement cohorts: {len(_cohort_df)} reports → {_cohort_path}")
    except Exception as _cohort_exc:
        import traceback
        print(f"Warning: engagement cohorts step failed: {_cohort_exc}")
        traceback.print_exc()

    # ── Step: Report usage frequency and intensity metrics ─────────────────
    try:
        from src.analytics.user_frequency_metrics import (
            build_report_frequency_metrics,
            persist_report_frequency_metrics,
            FrequencyMetricsConfig,
        )
        _freq_cfg = FrequencyMetricsConfig()
        _freq_df = build_report_frequency_metrics(
            sufficiency_df=_sufficiency_input,
            mart_df=mart_df,
            quality_df=quality_df,
            boundaries_df=_boundaries_input,
            cfg=_freq_cfg,
            analytics_run_id=analytics_run_id,
        )
        _freq_path = persist_report_frequency_metrics(_freq_df, root)
        print(f"  Frequency metrics: {len(_freq_df)} reports → {_freq_path}")
    except Exception as _freq_exc:
        import traceback
        print(f"Warning: frequency metrics step failed: {_freq_exc}")
        traceback.print_exc()


    # ── Step: Report user concentration and dependency metrics ─────────────
    try:
        from src.analytics.user_concentration_metrics import (
            build_report_concentration_metrics,
            persist_report_concentration_metrics,
            ConcentrationMetricsConfig,
        )
        _conc_cfg = ConcentrationMetricsConfig()
        _conc_df = build_report_concentration_metrics(
            sufficiency_df=_sufficiency_input,
            mart_df=mart_df,
            quality_df=quality_df,
            boundaries_df=_boundaries_input,
            cfg=_conc_cfg,
            analytics_run_id=analytics_run_id,
        )
        _conc_path = persist_report_concentration_metrics(_conc_df, root)
        print(f"  Concentration metrics: {len(_conc_df)} reports → {_conc_path}")
    except Exception as _conc_exc:
        import traceback
        print(f"Warning: concentration metrics step failed: {_conc_exc}")
        traceback.print_exc()


if __name__ == "__main__":
    run_pipeline()
