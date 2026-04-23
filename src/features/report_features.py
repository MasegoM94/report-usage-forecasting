"""Feature engineering helpers for report-level adoption marts."""

from __future__ import annotations

import pandas as pd


def _validate_input_columns(
    fact_report_views: pd.DataFrame,
    required_columns: list[str],
) -> None:
    """Raise a helpful error if one or more required columns are missing."""
    missing_columns = [column for column in required_columns if column not in fact_report_views]
    if missing_columns:
        missing_list = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing required columns: {missing_list}")


def _coerce_to_datetime(date_series: pd.Series) -> pd.Series:
    """Convert a date-like series to pandas datetime values."""
    if pd.api.types.is_datetime64_any_dtype(date_series):
        return pd.to_datetime(date_series, errors="coerce")

    if pd.api.types.is_numeric_dtype(date_series):
        parsed_dates = pd.to_datetime(date_series.astype("Int64").astype(str), format="%Y%m%d", errors="coerce")
        if parsed_dates.notna().any():
            return parsed_dates

    return pd.to_datetime(date_series, errors="coerce")


def build_report_daily_adoption(
    fact_report_views: pd.DataFrame,
    date_col: str = "date",
    report_col: str = "report_id",
    user_col: str = "user_id",
    views_col: str | None = None,
) -> pd.DataFrame:
    """Build a daily report-level adoption mart from report view events.

    Parameters
    ----------
    fact_report_views:
        Source fact table at a report-view event grain.
    date_col:
        Name of the source date column.
    report_col:
        Name of the source report identifier column.
    user_col:
        Name of the source user identifier column.
    views_col:
        Optional numeric column to sum for daily view totals. If omitted,
        `daily_views` is calculated from the row count.

    Returns
    -------
    pd.DataFrame
        A DataFrame at the `date` and `report_id` grain with:
        `daily_views`, `unique_viewers`, and `views_per_user`.
    """
    if not isinstance(fact_report_views, pd.DataFrame):
        raise TypeError("fact_report_views must be a pandas DataFrame.")

    required_columns = [date_col, report_col, user_col]
    if views_col is not None:
        required_columns.append(views_col)
    _validate_input_columns(fact_report_views, required_columns)

    if fact_report_views.empty:
        return pd.DataFrame(
            columns=["date", "report_id", "daily_views", "unique_viewers", "views_per_user"]
        )

    working_df = fact_report_views.copy()
    working_df["date"] = _coerce_to_datetime(working_df[date_col])

    if working_df["date"].isna().any():
        raise ValueError(
            f"Unable to parse all values in '{date_col}' into valid datetimes."
        )

    working_df["date"] = working_df["date"].dt.normalize()
    working_df["report_id"] = working_df[report_col]
    working_df["_user_id"] = working_df[user_col]

    if views_col is None:
        grouped = (
            working_df.groupby(["date", "report_id"], as_index=False)
            .agg(
                daily_views=("report_id", "size"),
                unique_viewers=("_user_id", "nunique"),
            )
        )
    else:
        if not pd.api.types.is_numeric_dtype(working_df[views_col]):
            raise TypeError(f"'{views_col}' must be numeric when provided.")

        grouped = (
            working_df.groupby(["date", "report_id"], as_index=False)
            .agg(
                daily_views=(views_col, "sum"),
                unique_viewers=("_user_id", "nunique"),
            )
        )

    grouped["views_per_user"] = grouped["daily_views"].div(
        grouped["unique_viewers"].replace(0, pd.NA)
    )
    grouped["views_per_user"] = grouped["views_per_user"].fillna(0.0)

    result = (
        grouped[["date", "report_id", "daily_views", "unique_viewers", "views_per_user"]]
        .sort_values(["report_id", "date"])
        .reset_index(drop=True)
    )

    return result


def add_time_series_usage_features(
    df: pd.DataFrame,
    date_col: str = "date",
    report_col: str = "report_id",
) -> pd.DataFrame:
    """Add rolling usage features and week-over-week change by report.

    Parameters
    ----------
    df:
        Daily report-level feature table. The input is expected to already
        contain one row per date and report, including zero-usage days.
    date_col:
        Name of the date column.
    report_col:
        Name of the report identifier column.

    Returns
    -------
    pd.DataFrame
        The input dataset enriched with:
        `views_7d`, `views_28d`, `wow_change_views`,
        `viewers_7d`, and `viewers_28d`.
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame.")

    required_columns = [date_col, report_col, "daily_views", "unique_viewers"]
    _validate_input_columns(df, required_columns)

    if df.empty:
        return df.copy()

    enriched_df = df.copy()
    enriched_df[date_col] = _coerce_to_datetime(enriched_df[date_col])

    if enriched_df[date_col].isna().any():
        raise ValueError(
            f"Unable to parse all values in '{date_col}' into valid datetimes."
        )

    for measure_col in ["daily_views", "unique_viewers"]:
        if not pd.api.types.is_numeric_dtype(enriched_df[measure_col]):
            raise TypeError(f"'{measure_col}' must be numeric.")

    enriched_df = enriched_df.sort_values([report_col, date_col]).reset_index(drop=True)

    report_groups = enriched_df.groupby(report_col, group_keys=False)

    enriched_df["views_7d"] = report_groups["daily_views"].transform(
        lambda series: series.rolling(window=7, min_periods=1).sum()
    )
    enriched_df["views_28d"] = report_groups["daily_views"].transform(
        lambda series: series.rolling(window=28, min_periods=1).sum()
    )
    enriched_df["viewers_7d"] = report_groups["unique_viewers"].transform(
        lambda series: series.rolling(window=7, min_periods=1).sum()
    )
    enriched_df["viewers_28d"] = report_groups["unique_viewers"].transform(
        lambda series: series.rolling(window=28, min_periods=1).sum()
    )

    lag_7d = report_groups["daily_views"].shift(7)
    enriched_df["wow_change_views"] = (
        enriched_df["daily_views"] - lag_7d
    ).div(lag_7d.where(lag_7d.ne(0)))
    enriched_df["wow_change_views"] = enriched_df["wow_change_views"].replace(
        [float("inf"), float("-inf")], pd.NA
    )

    return enriched_df
