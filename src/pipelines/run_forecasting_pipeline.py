"""Run the Day 1 baseline forecasting pipeline.

Usage:
    python -m src.pipelines.run_forecasting_pipeline
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.models.baselines import (
    moving_average_forecast,
    naive_forecast,
    seasonal_naive_forecast,
)


DATE_COL = "date"
REPORT_ID_COL = "report_id"
TARGET_COL = "daily_views"
REQUIRED_COLUMNS = {DATE_COL, REPORT_ID_COL, TARGET_COL}


def get_project_root() -> Path:
    """Return the repository root based on this pipeline file location."""
    return Path(__file__).resolve().parents[2]


def load_feature_mart(feature_path: Path) -> pd.DataFrame:
    """Load and validate the processed forecasting feature mart."""
    if not feature_path.exists():
        raise FileNotFoundError(
            f"Feature mart not found at {feature_path}. "
            "Run notebooks/04_feature_engineering.ipynb before this pipeline."
        )

    df = pd.read_csv(feature_path, parse_dates=[DATE_COL])
    missing_columns = REQUIRED_COLUMNS - set(df.columns)
    if missing_columns:
        raise ValueError(
            f"Feature mart is missing required columns: {sorted(missing_columns)}"
        )

    return df


def select_sample_report(df: pd.DataFrame) -> str:
    """Select one report with enough non-null history for a smoke-test forecast."""
    valid_rows = df.dropna(subset=[DATE_COL, REPORT_ID_COL, TARGET_COL]).copy()
    if valid_rows.empty:
        raise ValueError("No valid rows found in the feature mart.")

    report_counts = valid_rows.groupby(REPORT_ID_COL).size().sort_values(ascending=False)
    if report_counts.empty:
        raise ValueError("No valid report_id values found in the feature mart.")

    return str(report_counts.index[0])


def run_pipeline(horizon: int = 30) -> Path:
    """Run baseline forecasts for one sample report and save the combined output."""
    project_root = get_project_root()
    feature_path = project_root / "data" / "processed" / "mart_forecast_features.csv"
    output_path = project_root / "outputs" / "forecasts" / "sample_baseline_forecasts.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    feature_mart = load_feature_mart(feature_path)
    report_id = select_sample_report(feature_mart)

    report_history = (
        feature_mart.loc[feature_mart[REPORT_ID_COL].astype(str) == report_id]
        .sort_values(DATE_COL)
        .copy()
    )

    forecasts = pd.concat(
        [
            naive_forecast(report_history, horizon=horizon),
            moving_average_forecast(report_history, horizon=horizon, window=7),
            seasonal_naive_forecast(report_history, horizon=horizon, season_length=7),
        ],
        ignore_index=True,
    )
    forecasts.insert(0, REPORT_ID_COL, report_id)

    forecasts.to_csv(output_path, index=False)
    print(f"Saved sample baseline forecasts to: {output_path}")
    return output_path


if __name__ == "__main__":
    run_pipeline()
