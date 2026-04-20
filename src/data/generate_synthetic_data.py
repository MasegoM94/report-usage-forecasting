"""Generate raw synthetic Power BI-style usage telemetry tables.

This script mirrors the implemented logic in
`notebooks/02_generate_raw_tables.ipynb` and writes CSV files to `data/raw/`.
"""

from pathlib import Path

import numpy as np
import pandas as pd


RANDOM_SEED = 42
N_REPORTS = 30
N_USERS = 200
START_DATE = "2025-01-01"
END_DATE = "2026-03-31"


def get_project_root() -> Path:
    """Return the repository root based on this script location."""
    return Path(__file__).resolve().parents[2]


def get_paths() -> dict[str, Path]:
    """Return project paths used by the raw-data generation step."""
    project_root = get_project_root()
    raw_path = project_root / "data" / "raw"
    raw_path.mkdir(parents=True, exist_ok=True)
    return {"project_root": project_root, "raw_path": raw_path}


def generate_reports(n_reports: int) -> pd.DataFrame:
    """Generate report-level metadata."""
    report_types = ["Report", "Paginated", "Dashboard"]
    workspace_ids = [f"WS_{i:03d}" for i in range(1, 6)]

    return pd.DataFrame(
        {
            "report_id": [f"R_{i:03d}" for i in range(1, n_reports + 1)],
            "report_name": [f"Report_{i:03d}" for i in range(1, n_reports + 1)],
            "workspace_id": np.random.choice(workspace_ids, n_reports),
            "report_type": np.random.choice(
                report_types, n_reports, p=[0.7, 0.2, 0.1]
            ),
            "is_usage_metrics_report": np.random.choice(
                [True, False], n_reports, p=[0.1, 0.9]
            ),
        }
    )


def generate_users(n_users: int) -> pd.DataFrame:
    """Generate user reference data."""
    return pd.DataFrame(
        {
            "user_key": [f"UK_{i:04d}" for i in range(1, n_users + 1)],
            "user_id": [f"user{i:03d}@masegoinc.com" for i in range(1, n_users + 1)],
            "unique_user": [f"User {i:03d}" for i in range(1, n_users + 1)],
        }
    )


def generate_report_pages(reports: pd.DataFrame) -> pd.DataFrame:
    """Generate page metadata for reports and dashboards only."""
    page_rows = []

    for _, row in reports.iterrows():
        if row["report_type"] not in ["Report", "Dashboard"]:
            continue

        n_pages = np.random.randint(3, 9)

        for page_number in range(1, n_pages + 1):
            page_rows.append(
                {
                    "report_id": row["report_id"],
                    "section_id": f"{row['report_id']}_P{page_number}",
                    "section_name": f"Page {page_number}",
                }
            )

    return pd.DataFrame(page_rows)


def generate_dates(start_date: str, end_date: str) -> pd.DataFrame:
    """Generate the calendar table."""
    dates_range = pd.date_range(start_date, end_date, freq="D")
    dates = pd.DataFrame({"date": dates_range})
    dates["day_of_week"] = dates["date"].dt.day_name()
    dates["week_start_date"] = dates["date"] - pd.to_timedelta(
        dates["date"].dt.weekday, unit="D"
    )
    dates["month"] = dates["date"].dt.to_period("M").astype(str)
    dates["is_weekend"] = dates["date"].dt.weekday >= 5
    return dates


def generate_hidden_drivers(
    reports: pd.DataFrame, users: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Generate hidden simulation drivers used by usage and load telemetry."""
    report_popularity = pd.DataFrame(
        {
            "report_id": reports["report_id"],
            "base_popularity": np.random.gamma(
                shape=2.0, scale=2.0, size=len(reports)
            ),
        }
    )

    user_activity = pd.DataFrame(
        {
            "user_key": users["user_key"],
            "activity_score": np.random.gamma(
                shape=2.0, scale=1.5, size=len(users)
            ),
        }
    )

    report_performance = pd.DataFrame(
        {
            "report_id": reports["report_id"],
            "base_load_time_ms": np.random.normal(
                loc=3500, scale=1200, size=len(reports)
            ).clip(800, 9000),
        }
    )

    return report_popularity, user_activity, report_performance


def generate_report_views(
    dates_range: pd.DatetimeIndex,
    reports: pd.DataFrame,
    users: pd.DataFrame,
    report_popularity: pd.DataFrame,
    user_activity: pd.DataFrame,
) -> pd.DataFrame:
    """Generate report-level usage rows."""
    view_rows = []

    report_lookup = report_popularity.set_index("report_id")["base_popularity"].to_dict()
    user_lookup = user_activity.set_index("user_key")["activity_score"].to_dict()

    consumption_methods = ["Web", "Mobile"]
    distribution_methods = ["Direct", "App", "SharedLink", "Embedded"]

    for date in dates_range:
        weekend_factor = 0.55 if date.weekday() >= 5 else 1.0

        for _, report in reports.iterrows():
            report_id = report["report_id"]
            popularity = report_lookup[report_id]

            for _, user in users.iterrows():
                user_key = user["user_key"]
                user_id = user["user_id"]
                activity = user_lookup[user_key]

                p_view = min(0.015 * popularity * activity * weekend_factor, 0.65)

                if np.random.rand() < p_view:
                    view_count = np.random.choice(
                        [1, 2, 3, 4], p=[0.65, 0.2, 0.1, 0.05]
                    )

                    view_rows.append(
                        {
                            "date": date,
                            "report_id": report_id,
                            "user_key": user_key,
                            "user_id": user_id,
                            "consumption_method": np.random.choice(
                                consumption_methods, p=[0.85, 0.15]
                            ),
                            "distribution_method": np.random.choice(
                                distribution_methods, p=[0.5, 0.3, 0.1, 0.1]
                            ),
                            "user_agent": np.random.choice(
                                ["Chrome", "Edge", "Safari", "Firefox"],
                                p=[0.45, 0.3, 0.15, 0.1],
                            ),
                            "view_count": view_count,
                        }
                    )

    return pd.DataFrame(view_rows)


def generate_report_page_views(
    report_views: pd.DataFrame, report_pages: pd.DataFrame
) -> pd.DataFrame:
    """Generate page-level usage rows derived from report views."""
    pages_by_report = report_pages.groupby("report_id")["section_id"].apply(list).to_dict()
    page_view_rows = []

    for _, row in report_views.iterrows():
        if row["report_id"] not in pages_by_report:
            continue

        possible_pages = pages_by_report[row["report_id"]]
        n_page_events = np.random.randint(
            1, min(len(possible_pages), row["view_count"] + 2) + 1
        )
        viewed_pages = np.random.choice(
            possible_pages, size=n_page_events, replace=False
        )

        for section_id in viewed_pages:
            ts = pd.Timestamp(row["date"]) + pd.Timedelta(
                minutes=np.random.randint(0, 1440)
            )

            page_view_rows.append(
                {
                    "timestamp": ts,
                    "date": row["date"],
                    "report_id": row["report_id"],
                    "section_id": section_id,
                    "user_key": row["user_key"],
                    "client": np.random.choice(["Browser", "MobileApp"], p=[0.85, 0.15]),
                    "session_source": np.random.choice(
                        ["Direct", "App", "SharedLink"], p=[0.6, 0.3, 0.1]
                    ),
                    "page_view_count": 1,
                }
            )

    return pd.DataFrame(page_view_rows)


def generate_report_load_times(
    report_views: pd.DataFrame, report_performance: pd.DataFrame
) -> pd.DataFrame:
    """Generate report-load telemetry rows derived from report views."""
    load_rows = []
    load_lookup = report_performance.set_index("report_id")["base_load_time_ms"].to_dict()

    for _, row in report_views.iterrows():
        base_load = load_lookup[row["report_id"]]
        adjusted_load = np.random.normal(loc=base_load, scale=500)
        adjusted_load = max(500, adjusted_load)

        ts = pd.Timestamp(row["date"]) + pd.Timedelta(
            minutes=np.random.randint(0, 1440)
        )

        load_rows.append(
            {
                "timestamp": ts,
                "date": row["date"],
                "report_id": row["report_id"],
                "user_key": row["user_key"],
                "user_id": row["user_id"],
                "browser": row["user_agent"],
                "client": "Browser"
                if row["consumption_method"] == "Web"
                else "MobileApp",
                "country": np.random.choice(
                    ["Canada", "UK", "South Africa"], p=[0.7, 0.2, 0.1]
                ),
                "load_time_ms": round(adjusted_load, 0),
            }
        )

    return pd.DataFrame(load_rows)


def print_shapes(tables: dict[str, pd.DataFrame]) -> None:
    """Print table shapes."""
    for table_name, df in tables.items():
        print(f"{table_name}: {df.shape}")


def run_basic_validation(tables: dict[str, pd.DataFrame]) -> None:
    """Run the same lightweight key existence checks as the notebook."""
    reports = tables["reports"]
    users = tables["users"]
    report_pages = tables["report_pages"]
    report_views = tables["report_views"]
    report_page_views = tables["report_page_views"]
    report_load_times = tables["report_load_times"]

    print(
        "All report_ids in report_views exist in reports:",
        report_views["report_id"].isin(reports["report_id"]).all(),
    )
    print(
        "All user_keys in report_views exist in users:",
        report_views["user_key"].isin(users["user_key"]).all(),
    )
    print(
        "All section_ids in report_page_views exist in report_pages:",
        report_page_views["section_id"].isin(report_pages["section_id"]).all(),
    )
    print(
        "All report_ids in report_load_times exist in reports:",
        report_load_times["report_id"].isin(reports["report_id"]).all(),
    )


def save_raw_tables(tables: dict[str, pd.DataFrame], raw_path: Path) -> None:
    """Save raw tables as CSV files."""
    for table_name, df in tables.items():
        output_path = raw_path / f"{table_name}.csv"
        df.to_csv(output_path, index=False)
        print(f"Saved {output_path}")


def main() -> None:
    """Generate and save all raw synthetic tables."""
    np.random.seed(RANDOM_SEED)
    paths = get_paths()
    raw_path = paths["raw_path"]

    dates_range = pd.date_range(START_DATE, END_DATE, freq="D")
    print(f"Saving raw tables to: {raw_path.resolve()}")
    print(f"Reports: {N_REPORTS}")
    print(f"Users: {N_USERS}")
    print(f"Date range: {START_DATE} to {END_DATE} ({len(dates_range)} days)")

    reports = generate_reports(N_REPORTS)
    users = generate_users(N_USERS)
    report_pages = generate_report_pages(reports)
    dates = generate_dates(START_DATE, END_DATE)
    report_popularity, user_activity, report_performance = generate_hidden_drivers(
        reports, users
    )
    report_views = generate_report_views(
        dates_range, reports, users, report_popularity, user_activity
    )
    report_page_views = generate_report_page_views(report_views, report_pages)
    report_load_times = generate_report_load_times(report_views, report_performance)

    tables = {
        "reports": reports,
        "users": users,
        "report_pages": report_pages,
        "dates": dates,
        "report_views": report_views,
        "report_page_views": report_page_views,
        "report_load_times": report_load_times,
    }

    print_shapes(tables)
    run_basic_validation(tables)
    save_raw_tables(tables, raw_path)


if __name__ == "__main__":
    main()
