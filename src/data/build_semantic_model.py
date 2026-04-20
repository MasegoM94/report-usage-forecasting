"""Build the processed semantic model from raw CSV tables.

This script mirrors the implemented logic in
`notebooks/03_build_semantic_model_csv.ipynb` and writes CSV files to
`data/processed/`.
"""

from pathlib import Path

import pandas as pd


def get_project_root() -> Path:
    """Return the repository root based on this script location."""
    return Path(__file__).resolve().parents[2]


def get_paths() -> dict[str, Path]:
    """Return project paths used by the semantic-model build step."""
    project_root = get_project_root()
    raw_path = project_root / "data" / "raw"
    processed_path = project_root / "data" / "processed"
    processed_path.mkdir(parents=True, exist_ok=True)
    return {
        "project_root": project_root,
        "raw_path": raw_path,
        "processed_path": processed_path,
    }


def load_raw_tables(raw_path: Path) -> dict[str, pd.DataFrame]:
    """Load raw CSV tables created by the synthetic-data step."""
    tables = {
        "reports": pd.read_csv(raw_path / "reports.csv"),
        "users": pd.read_csv(raw_path / "users.csv"),
        "report_pages": pd.read_csv(raw_path / "report_pages.csv"),
        "dates": pd.read_csv(
            raw_path / "dates.csv", parse_dates=["date", "week_start_date"]
        ),
        "report_views": pd.read_csv(raw_path / "report_views.csv", parse_dates=["date"]),
        "report_page_views": pd.read_csv(
            raw_path / "report_page_views.csv", parse_dates=["timestamp", "date"]
        ),
        "report_load_times": pd.read_csv(
            raw_path / "report_load_times.csv", parse_dates=["timestamp", "date"]
        ),
    }

    print("Loaded raw tables successfully.")
    print_shapes(tables)
    return tables


def build_dim_date(dates: pd.DataFrame) -> pd.DataFrame:
    """Build the date dimension."""
    dim_date = dates.copy()
    dim_date["date_key"] = dim_date["date"].dt.strftime("%Y%m%d").astype(int)

    return (
        dim_date[
            [
                "date_key",
                "date",
                "day_of_week",
                "week_start_date",
                "month",
                "is_weekend",
            ]
        ]
        .drop_duplicates()
        .sort_values("date")
        .reset_index(drop=True)
    )


def build_dim_user(users: pd.DataFrame) -> pd.DataFrame:
    """Build the user dimension."""
    return users.copy().drop_duplicates(subset=["user_key"]).reset_index(drop=True)


def build_dim_report(reports: pd.DataFrame) -> pd.DataFrame:
    """Build the report dimension."""
    return reports.copy().drop_duplicates(subset=["report_id"]).reset_index(drop=True)


def build_dim_page(report_pages: pd.DataFrame) -> pd.DataFrame:
    """Build the page dimension with a generated surrogate page key."""
    dim_page = report_pages.copy()
    dim_page = dim_page.drop_duplicates(subset=["report_id", "section_id"]).reset_index(
        drop=True
    )
    dim_page["page_key"] = range(1, len(dim_page) + 1)

    return dim_page[["page_key", "report_id", "section_id", "section_name"]]


def build_fact_report_views(
    report_views: pd.DataFrame, dim_date: pd.DataFrame
) -> pd.DataFrame:
    """Build the report views fact table."""
    fact_report_views = report_views.copy()
    fact_report_views = fact_report_views.merge(
        dim_date[["date", "date_key"]],
        on="date",
        how="left",
    )

    return fact_report_views[
        [
            "date_key",
            "report_id",
            "user_key",
            "consumption_method",
            "distribution_method",
            "view_count",
        ]
    ].reset_index(drop=True)


def build_fact_page_views(
    report_page_views: pd.DataFrame, dim_date: pd.DataFrame, dim_page: pd.DataFrame
) -> pd.DataFrame:
    """Build the page views fact table."""
    fact_page_views = report_page_views.copy()
    fact_page_views = fact_page_views.merge(
        dim_date[["date", "date_key"]],
        on="date",
        how="left",
    )

    # The notebook joins page keys by section_id only. section_id currently embeds
    # report_id, so this preserves the implemented behaviour.
    fact_page_views = fact_page_views.merge(
        dim_page[["page_key", "section_id"]],
        on="section_id",
        how="left",
    )

    return fact_page_views[
        [
            "date_key",
            "report_id",
            "page_key",
            "user_key",
            "client",
            "session_source",
            "page_view_count",
        ]
    ].reset_index(drop=True)


def build_fact_report_loads(
    report_load_times: pd.DataFrame, dim_date: pd.DataFrame
) -> pd.DataFrame:
    """Build the report load performance fact table."""
    fact_report_loads = report_load_times.copy()
    fact_report_loads = fact_report_loads.merge(
        dim_date[["date", "date_key"]],
        on="date",
        how="left",
    )

    return fact_report_loads[
        [
            "date_key",
            "report_id",
            "user_key",
            "browser",
            "client",
            "country",
            "load_time_ms",
        ]
    ].reset_index(drop=True)


def print_shapes(tables: dict[str, pd.DataFrame]) -> None:
    """Print table shapes."""
    for table_name, df in tables.items():
        print(f"{table_name}: {df.shape}")


def run_lightweight_checks(tables: dict[str, pd.DataFrame]) -> None:
    """Run notebook-aligned uniqueness, referential integrity, and null checks."""
    dim_date = tables["dim_date"]
    dim_user = tables["dim_user"]
    dim_report = tables["dim_report"]
    dim_page = tables["dim_page"]
    fact_report_views = tables["fact_report_views"]
    fact_page_views = tables["fact_page_views"]
    fact_report_loads = tables["fact_report_loads"]

    print("Dimension key uniqueness")
    print("------------------------")
    print("dim_date date_key unique:", dim_date["date_key"].is_unique)
    print("dim_user user_key unique:", dim_user["user_key"].is_unique)
    print("dim_report report_id unique:", dim_report["report_id"].is_unique)
    print("dim_page page_key unique:", dim_page["page_key"].is_unique)

    print("Referential integrity checks")
    print("----------------------------")
    print(
        "fact_report_views.report_id in dim_report:",
        fact_report_views["report_id"].isin(dim_report["report_id"]).all(),
    )
    print(
        "fact_report_views.user_key in dim_user:",
        fact_report_views["user_key"].isin(dim_user["user_key"]).all(),
    )
    print(
        "fact_page_views.report_id in dim_report:",
        fact_page_views["report_id"].isin(dim_report["report_id"]).all(),
    )
    print(
        "fact_page_views.page_key in dim_page:",
        fact_page_views["page_key"].isin(dim_page["page_key"]).all(),
    )
    print(
        "fact_page_views.user_key in dim_user:",
        fact_page_views["user_key"].isin(dim_user["user_key"]).all(),
    )
    print(
        "fact_report_loads.report_id in dim_report:",
        fact_report_loads["report_id"].isin(dim_report["report_id"]).all(),
    )
    print(
        "fact_report_loads.user_key in dim_user:",
        fact_report_loads["user_key"].isin(dim_user["user_key"]).all(),
    )

    print("Null checks in fact keys")
    print("------------------------")
    print("fact_report_views")
    print(fact_report_views[["date_key", "report_id", "user_key"]].isnull().sum())
    print("\nfact_page_views")
    print(
        fact_page_views[["date_key", "report_id", "page_key", "user_key"]]
        .isnull()
        .sum()
    )
    print("\nfact_report_loads")
    print(fact_report_loads[["date_key", "report_id", "user_key"]].isnull().sum())


def save_processed_tables(tables: dict[str, pd.DataFrame], processed_path: Path) -> None:
    """Save semantic model tables as CSV files."""
    for table_name, df in tables.items():
        output_path = processed_path / f"{table_name}.csv"
        df.to_csv(output_path, index=False)
        print(f"Saved {output_path}")


def main() -> None:
    """Build and save all semantic model tables."""
    paths = get_paths()
    print("Project root:", paths["project_root"])
    print("Raw path:", paths["raw_path"])
    print("Processed path:", paths["processed_path"])

    raw_tables = load_raw_tables(paths["raw_path"])

    dim_date = build_dim_date(raw_tables["dates"])
    dim_user = build_dim_user(raw_tables["users"])
    dim_report = build_dim_report(raw_tables["reports"])
    dim_page = build_dim_page(raw_tables["report_pages"])
    fact_report_views = build_fact_report_views(raw_tables["report_views"], dim_date)
    fact_page_views = build_fact_page_views(
        raw_tables["report_page_views"], dim_date, dim_page
    )
    fact_report_loads = build_fact_report_loads(
        raw_tables["report_load_times"], dim_date
    )

    processed_tables = {
        "dim_date": dim_date,
        "dim_user": dim_user,
        "dim_report": dim_report,
        "dim_page": dim_page,
        "fact_report_views": fact_report_views,
        "fact_page_views": fact_page_views,
        "fact_report_loads": fact_report_loads,
    }

    print("Processed table shapes")
    print("----------------------")
    print_shapes(processed_tables)
    run_lightweight_checks(processed_tables)
    save_processed_tables(processed_tables, paths["processed_path"])


if __name__ == "__main__":
    main()
