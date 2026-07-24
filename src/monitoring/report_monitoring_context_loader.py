"""Load source DataFrames needed to build the report monitoring context mart.

Separated from ``report_monitoring_context.py`` so the builder stays testable
without touching the filesystem.  Called only by
``update_report_monitoring_context`` in the pipeline entry point.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def load_monitoring_sources(project_root: Path) -> dict[str, Any]:
    """Read all source files and return keyword-argument dict for build function.

    Missing files are returned as None (the builder handles None gracefully).

    Parameters
    ----------
    project_root:
        Repository root (absolute path).

    Returns
    -------
    dict with keys matching the parameters of ``build_report_monitoring_context``:
        production_forecast_df, perf_by_report_df, deterioration_df,
        report_features_df, feature_context_df, segments_df, diagnostics_df
    """

    def _read(rel: str) -> pd.DataFrame | None:
        path = project_root / rel
        if not path.exists():
            logger.debug("monitoring_context_loader | absent: %s", rel)
            return None
        try:
            df = pd.read_csv(path)
            if df.empty:
                logger.debug("monitoring_context_loader | empty: %s", rel)
                return None
            return df
        except Exception as exc:
            logger.warning("monitoring_context_loader | failed to read %s: %s", rel, exc)
            return None

    return {
        "production_forecast_df": _read(
            "outputs/forecasts/production_forecasts_latest.csv"
        ),
        "perf_by_report_df": _read(
            "outputs/monitoring/realized_performance_by_report.csv"
        ),
        "deterioration_df": _read(
            "outputs/monitoring/deterioration_report.csv"
        ),
        "report_features_df": _read(
            "outputs/metrics/report_features.csv"
        ),
        "feature_context_df": _read(
            "data/processed/mart_report_daily_context.csv"
        ),
        "segments_df": _read(
            "outputs/segments/report_segments.csv"
        ),
        "diagnostics_df": _read(
            "outputs/diagnostics/report_diagnostics.csv"
        ),
    }
