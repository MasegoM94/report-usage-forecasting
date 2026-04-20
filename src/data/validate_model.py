"""Validate the processed semantic model with Great Expectations and pandas.

This script mirrors the implemented logic in
`notebooks/04_validate_semantic_model_hybrid_gx_csv.ipynb` and writes CSV
validation outputs to `outputs/validation/`.
"""

from pathlib import Path
from typing import Any

import pandas as pd


def get_project_root() -> Path:
    """Return the repository root based on this script location."""
    return Path(__file__).resolve().parents[2]


def get_paths() -> dict[str, Path]:
    """Return project paths used by validation."""
    project_root = get_project_root()
    processed_path = project_root / "data" / "processed"
    raw_path = project_root / "data" / "raw"
    validation_path = project_root / "outputs" / "validation"
    validation_path.mkdir(parents=True, exist_ok=True)
    return {
        "project_root": project_root,
        "processed_path": processed_path,
        "raw_path": raw_path,
        "validation_path": validation_path,
    }


def import_great_expectations() -> Any:
    """Import Great Expectations with a helpful error message if unavailable."""
    try:
        import great_expectations as gx
    except ImportError as exc:
        raise SystemExit(
            "Great Expectations is required for validation. "
            "Install it with `pip install great_expectations` or install the "
            "project requirements."
        ) from exc
    return gx


def load_processed_tables(processed_path: Path) -> dict[str, pd.DataFrame]:
    """Load processed semantic model CSV tables."""
    tables = {
        "dim_date": pd.read_csv(
            processed_path / "dim_date.csv", parse_dates=["date", "week_start_date"]
        ),
        "dim_user": pd.read_csv(processed_path / "dim_user.csv"),
        "dim_report": pd.read_csv(processed_path / "dim_report.csv"),
        "dim_page": pd.read_csv(processed_path / "dim_page.csv"),
        "fact_report_views": pd.read_csv(processed_path / "fact_report_views.csv"),
        "fact_page_views": pd.read_csv(processed_path / "fact_page_views.csv"),
        "fact_report_loads": pd.read_csv(processed_path / "fact_report_loads.csv"),
    }

    print("Loaded processed tables successfully.")
    print_shapes(tables)
    return tables


def load_raw_tables(raw_path: Path) -> dict[str, pd.DataFrame]:
    """Load raw CSV tables needed for row-count reconciliation."""
    return {
        "reports": pd.read_csv(raw_path / "reports.csv"),
        "users": pd.read_csv(raw_path / "users.csv"),
        "report_pages": pd.read_csv(raw_path / "report_pages.csv"),
        "dates": pd.read_csv(raw_path / "dates.csv"),
        "report_views": pd.read_csv(raw_path / "report_views.csv"),
        "report_page_views": pd.read_csv(raw_path / "report_page_views.csv"),
        "report_load_times": pd.read_csv(raw_path / "report_load_times.csv"),
    }


def initialize_gx_context(gx: Any) -> Any:
    """Initialize the Great Expectations context."""
    print("GX version:", gx.__version__)
    context = gx.get_context()
    print("GX context initialized.")
    return context


def get_or_create_pandas_datasource(
    context: Any, datasource_name: str = "pandas_validation_source"
) -> Any:
    """Get or create the pandas datasource used by notebook validation."""
    try:
        return context.data_sources.get(datasource_name)
    except Exception:
        return context.data_sources.add_pandas(datasource_name)


def get_or_create_dataframe_asset(data_source: Any, asset_name: str) -> Any:
    """Get or create a DataFrame asset."""
    try:
        return data_source.get_asset(asset_name)
    except Exception:
        return data_source.add_dataframe_asset(name=asset_name)


def get_or_create_batch_definition(
    data_asset: Any, batch_definition_name: str = "whole_dataframe"
) -> Any:
    """Get or create a whole-DataFrame batch definition."""
    try:
        return data_asset.get_batch_definition(batch_definition_name)
    except Exception:
        return data_asset.add_batch_definition_whole_dataframe(batch_definition_name)


def get_batch(context: Any, df: pd.DataFrame, asset_name: str) -> Any:
    """Create or retrieve a Great Expectations batch for a DataFrame."""
    data_source = get_or_create_pandas_datasource(context)
    data_asset = get_or_create_dataframe_asset(data_source, asset_name)
    batch_definition = get_or_create_batch_definition(data_asset)
    return batch_definition.get_batch(batch_parameters={"dataframe": df})


def parse_gx_result(
    table_name: str, check_group: str, expectation_name: str, result_obj: Any
) -> dict[str, Any]:
    """Flatten a GX validation result into one tabular record."""
    success = result_obj.get("success", None)
    expectation_config = result_obj.get("expectation_config", {})
    result = result_obj.get("result", {})

    return {
        "table_name": table_name,
        "check_group": check_group,
        "expectation_name": expectation_name,
        "success": success,
        "unexpected_count": result.get("unexpected_count"),
        "unexpected_percent": result.get("unexpected_percent"),
        "missing_count": result.get("missing_count"),
        "missing_percent": result.get("missing_percent"),
        "element_count": result.get("element_count"),
        "details": str(expectation_config.get("kwargs", {})),
    }


def run_gx_expectations(
    context: Any,
    df: pd.DataFrame,
    table_name: str,
    expectations: list[Any],
    check_group: str,
) -> pd.DataFrame:
    """Run a list of GX expectations against one DataFrame."""
    batch = get_batch(context, df, asset_name=f"{table_name}_asset")
    records = []

    for expectation in expectations:
        result = batch.validate(expectation)
        expectation_name = expectation.__class__.__name__
        records.append(
            parse_gx_result(
                table_name=table_name,
                check_group=check_group,
                expectation_name=expectation_name,
                result_obj=result,
            )
        )

    return pd.DataFrame(records)


def run_gx_uniqueness_checks(
    gx: Any, context: Any, tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Run GX uniqueness checks for dimension keys."""
    uniqueness_specs = {
        "dim_date": (
            tables["dim_date"],
            [gx.expectations.ExpectColumnValuesToBeUnique(column="date_key")],
        ),
        "dim_user": (
            tables["dim_user"],
            [gx.expectations.ExpectColumnValuesToBeUnique(column="user_key")],
        ),
        "dim_report": (
            tables["dim_report"],
            [gx.expectations.ExpectColumnValuesToBeUnique(column="report_id")],
        ),
        "dim_page": (
            tables["dim_page"],
            [gx.expectations.ExpectColumnValuesToBeUnique(column="page_key")],
        ),
    }

    results = [
        run_gx_expectations(context, df, table_name, expectations, "uniqueness")
        for table_name, (df, expectations) in uniqueness_specs.items()
    ]
    return pd.concat(results, ignore_index=True)


def run_gx_null_checks(
    gx: Any, context: Any, tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Run GX not-null checks for model keys and measures."""
    null_specs = {
        "dim_date": (
            tables["dim_date"],
            [gx.expectations.ExpectColumnValuesToNotBeNull(column="date_key")],
        ),
        "dim_user": (
            tables["dim_user"],
            [gx.expectations.ExpectColumnValuesToNotBeNull(column="user_key")],
        ),
        "dim_report": (
            tables["dim_report"],
            [gx.expectations.ExpectColumnValuesToNotBeNull(column="report_id")],
        ),
        "dim_page": (
            tables["dim_page"],
            [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="page_key"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="report_id"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="section_id"),
            ],
        ),
        "fact_report_views": (
            tables["fact_report_views"],
            [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="date_key"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="report_id"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="user_key"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="view_count"),
            ],
        ),
        "fact_page_views": (
            tables["fact_page_views"],
            [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="date_key"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="report_id"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="page_key"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="user_key"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="page_view_count"),
            ],
        ),
        "fact_report_loads": (
            tables["fact_report_loads"],
            [
                gx.expectations.ExpectColumnValuesToNotBeNull(column="date_key"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="report_id"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="user_key"),
                gx.expectations.ExpectColumnValuesToNotBeNull(column="load_time_ms"),
            ],
        ),
    }

    results = [
        run_gx_expectations(context, df, table_name, expectations, "nulls")
        for table_name, (df, expectations) in null_specs.items()
    ]
    return pd.concat(results, ignore_index=True)


def run_gx_rowcount_checks(
    gx: Any, context: Any, tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Run GX row-count checks requiring each table to contain at least one row."""
    rowcount_specs = {
        "dim_date": tables["dim_date"],
        "dim_user": tables["dim_user"],
        "dim_report": tables["dim_report"],
        "dim_page": tables["dim_page"],
        "fact_report_views": tables["fact_report_views"],
        "fact_page_views": tables["fact_page_views"],
        "fact_report_loads": tables["fact_report_loads"],
    }

    results = []
    for table_name, df in rowcount_specs.items():
        expectations = [gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)]
        results.append(
            run_gx_expectations(context, df, table_name, expectations, "row_count")
        )

    return pd.concat(results, ignore_index=True)


def run_referential_integrity_checks(
    tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Run pandas referential integrity checks between facts and dimensions."""
    dim_date = tables["dim_date"]
    dim_user = tables["dim_user"]
    dim_report = tables["dim_report"]
    dim_page = tables["dim_page"]
    fact_report_views = tables["fact_report_views"]
    fact_page_views = tables["fact_page_views"]
    fact_report_loads = tables["fact_report_loads"]
    ri_checks = []

    def add_ri_check(
        check_name: str,
        fact_df: pd.DataFrame,
        fact_key: str,
        dim_df: pd.DataFrame,
        dim_key: str,
    ) -> None:
        invalid_mask = ~fact_df[fact_key].isin(dim_df[dim_key])
        invalid_count = int(invalid_mask.sum())
        total_rows = int(len(fact_df))

        ri_checks.append(
            {
                "check_name": check_name,
                "fact_key": fact_key,
                "dimension_key": dim_key,
                "invalid_count": invalid_count,
                "total_rows": total_rows,
                "success": invalid_count == 0,
            }
        )

    add_ri_check(
        "fact_report_views.report_id -> dim_report.report_id",
        fact_report_views,
        "report_id",
        dim_report,
        "report_id",
    )
    add_ri_check(
        "fact_report_views.user_key -> dim_user.user_key",
        fact_report_views,
        "user_key",
        dim_user,
        "user_key",
    )
    add_ri_check(
        "fact_report_views.date_key -> dim_date.date_key",
        fact_report_views,
        "date_key",
        dim_date,
        "date_key",
    )
    add_ri_check(
        "fact_page_views.report_id -> dim_report.report_id",
        fact_page_views,
        "report_id",
        dim_report,
        "report_id",
    )
    add_ri_check(
        "fact_page_views.page_key -> dim_page.page_key",
        fact_page_views,
        "page_key",
        dim_page,
        "page_key",
    )
    add_ri_check(
        "fact_page_views.user_key -> dim_user.user_key",
        fact_page_views,
        "user_key",
        dim_user,
        "user_key",
    )
    add_ri_check(
        "fact_page_views.date_key -> dim_date.date_key",
        fact_page_views,
        "date_key",
        dim_date,
        "date_key",
    )
    add_ri_check(
        "fact_report_loads.report_id -> dim_report.report_id",
        fact_report_loads,
        "report_id",
        dim_report,
        "report_id",
    )
    add_ri_check(
        "fact_report_loads.user_key -> dim_user.user_key",
        fact_report_loads,
        "user_key",
        dim_user,
        "user_key",
    )
    add_ri_check(
        "fact_report_loads.date_key -> dim_date.date_key",
        fact_report_loads,
        "date_key",
        dim_date,
        "date_key",
    )

    return pd.DataFrame(ri_checks)


def run_row_count_reconciliation(
    raw_tables: dict[str, pd.DataFrame], processed_tables: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Compare raw and processed row counts."""
    row_count_reconciliation = pd.DataFrame(
        [
            {
                "raw_table": "reports",
                "processed_table": "dim_report",
                "raw_row_count": len(raw_tables["reports"]),
                "processed_row_count": len(processed_tables["dim_report"]),
            },
            {
                "raw_table": "users",
                "processed_table": "dim_user",
                "raw_row_count": len(raw_tables["users"]),
                "processed_row_count": len(processed_tables["dim_user"]),
            },
            {
                "raw_table": "report_pages",
                "processed_table": "dim_page",
                "raw_row_count": len(raw_tables["report_pages"]),
                "processed_row_count": len(processed_tables["dim_page"]),
            },
            {
                "raw_table": "dates",
                "processed_table": "dim_date",
                "raw_row_count": len(raw_tables["dates"]),
                "processed_row_count": len(processed_tables["dim_date"]),
            },
            {
                "raw_table": "report_views",
                "processed_table": "fact_report_views",
                "raw_row_count": len(raw_tables["report_views"]),
                "processed_row_count": len(processed_tables["fact_report_views"]),
            },
            {
                "raw_table": "report_page_views",
                "processed_table": "fact_page_views",
                "raw_row_count": len(raw_tables["report_page_views"]),
                "processed_row_count": len(processed_tables["fact_page_views"]),
            },
            {
                "raw_table": "report_load_times",
                "processed_table": "fact_report_loads",
                "raw_row_count": len(raw_tables["report_load_times"]),
                "processed_row_count": len(processed_tables["fact_report_loads"]),
            },
        ]
    )

    row_count_reconciliation["row_count_difference"] = (
        row_count_reconciliation["processed_row_count"]
        - row_count_reconciliation["raw_row_count"]
    )
    row_count_reconciliation["exact_match"] = (
        row_count_reconciliation["processed_row_count"]
        == row_count_reconciliation["raw_row_count"]
    )
    return row_count_reconciliation


def build_validation_summary(
    gx_validation_results: pd.DataFrame,
    referential_integrity_checks: pd.DataFrame,
    row_count_reconciliation: pd.DataFrame,
) -> pd.DataFrame:
    """Build the combined validation summary table."""
    gx_summary = (
        gx_validation_results.groupby("check_group", as_index=False).agg(
            checks_run=("success", "count"),
            checks_passed=("success", "sum"),
        )
    )
    gx_summary["checks_failed"] = (
        gx_summary["checks_run"] - gx_summary["checks_passed"]
    )

    ri_summary = pd.DataFrame(
        [
            {
                "check_group": "referential_integrity",
                "checks_run": len(referential_integrity_checks),
                "checks_passed": int(referential_integrity_checks["success"].sum()),
                "checks_failed": int((~referential_integrity_checks["success"]).sum()),
            }
        ]
    )

    reconciliation_summary = pd.DataFrame(
        [
            {
                "check_group": "row_count_reconciliation",
                "checks_run": len(row_count_reconciliation),
                "checks_passed": int(row_count_reconciliation["exact_match"].sum()),
                "checks_failed": int((~row_count_reconciliation["exact_match"]).sum()),
            }
        ]
    )

    return pd.concat(
        [gx_summary, ri_summary, reconciliation_summary],
        ignore_index=True,
    )


def save_validation_outputs(
    validation_outputs: dict[str, pd.DataFrame], validation_path: Path
) -> None:
    """Save validation result tables as CSV files."""
    for table_name, df in validation_outputs.items():
        output_path = validation_path / f"{table_name}.csv"
        df.to_csv(output_path, index=False)
        print(f"Saved {output_path}")


def print_shapes(tables: dict[str, pd.DataFrame]) -> None:
    """Print table shapes."""
    for table_name, df in tables.items():
        print(f"{table_name}: {df.shape}")


def main() -> None:
    """Run model validation and save validation outputs."""
    gx = import_great_expectations()
    paths = get_paths()
    print("Project root:", paths["project_root"])
    print("Processed path:", paths["processed_path"])
    print("Validation path:", paths["validation_path"])

    processed_tables = load_processed_tables(paths["processed_path"])
    context = initialize_gx_context(gx)

    gx_validation_results = pd.concat(
        [
            run_gx_uniqueness_checks(gx, context, processed_tables),
            run_gx_null_checks(gx, context, processed_tables),
            run_gx_rowcount_checks(gx, context, processed_tables),
        ],
        ignore_index=True,
    )
    referential_integrity_checks = run_referential_integrity_checks(processed_tables)
    raw_tables = load_raw_tables(paths["raw_path"])
    row_count_reconciliation = run_row_count_reconciliation(
        raw_tables, processed_tables
    )
    validation_summary = build_validation_summary(
        gx_validation_results,
        referential_integrity_checks,
        row_count_reconciliation,
    )

    validation_outputs = {
        "gx_validation_results": gx_validation_results,
        "referential_integrity_checks": referential_integrity_checks,
        "row_count_reconciliation": row_count_reconciliation,
        "validation_summary": validation_summary,
    }
    save_validation_outputs(validation_outputs, paths["validation_path"])
    print("Validation outputs saved successfully.")


if __name__ == "__main__":
    main()
