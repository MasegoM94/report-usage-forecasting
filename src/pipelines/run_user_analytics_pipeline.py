"""Run the user-level behavioural analytics pipeline.

Usage:
    python -m src.pipelines.run_user_analytics_pipeline
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.analytics.user_features import build_user_features
from src.analytics.user_segmentation import build_user_segments


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

    return output_paths


if __name__ == "__main__":
    run_pipeline()
