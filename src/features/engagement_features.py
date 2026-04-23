"""Behavioural feature engineering helpers for report engagement marts."""

from __future__ import annotations

import math

import pandas as pd


def _validate_input_columns(df: pd.DataFrame, required_columns: list[str]) -> None:
    """Raise a helpful error when required columns are missing."""
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        missing_list = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing required columns: {missing_list}")


def _coerce_to_datetime(date_series: pd.Series) -> pd.Series:
    """Convert a date-like series to pandas datetime values."""
    if pd.api.types.is_datetime64_any_dtype(date_series):
        return pd.to_datetime(date_series, errors="coerce")

    if pd.api.types.is_numeric_dtype(date_series):
        parsed_dates = pd.to_datetime(
            date_series.astype("Int64").astype(str),
            format="%Y%m%d",
            errors="coerce",
        )
        if parsed_dates.notna().any():
            return parsed_dates

    return pd.to_datetime(date_series, errors="coerce")


def build_user_engagement_features(
    fact_report_views: pd.DataFrame,
    fact_page_views: pd.DataFrame,
    date_col: str = "date",
    report_col: str = "report_id",
    user_col: str = "user_id",
) -> pd.DataFrame:
    """Build behavioural engagement features at the daily report grain.

    Parameters
    ----------
    fact_report_views:
        Report-view fact table containing one or more records per user, report,
        and date. If a `views` or `view_count` column is present, it is used as
        the view measure; otherwise row counts are used.
    fact_page_views:
        Page-view fact table used to derive a session-depth proxy. If a
        `page_view_count` column is present, it is used as the page-view
        measure; otherwise row counts are used.
    date_col:
        Name of the date column in both inputs.
    report_col:
        Name of the report identifier column in both inputs.
    user_col:
        Name of the user identifier column in both inputs.

    Returns
    -------
    pd.DataFrame
        Behavioural feature mart at the `date` and `report_id` grain with:
        `repeat_user_rate`, `top_10pct_user_share`, `days_since_last_use`,
        and `avg_pages_per_user`.
    """
    if not isinstance(fact_report_views, pd.DataFrame):
        raise TypeError("fact_report_views must be a pandas DataFrame.")
    if not isinstance(fact_page_views, pd.DataFrame):
        raise TypeError("fact_page_views must be a pandas DataFrame.")

    _validate_input_columns(fact_report_views, [date_col, report_col, user_col])
    _validate_input_columns(fact_page_views, [date_col, report_col, user_col])
    if "section_id" not in fact_page_views.columns and "page_key" not in fact_page_views.columns:
        raise ValueError("fact_page_views must include either 'section_id' or 'page_key'.")

    if fact_report_views.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "report_id",
                "repeat_user_rate",
                "top_10pct_user_share",
                "days_since_last_use",
                "avg_pages_per_user",
            ]
        )

    report_views_df = fact_report_views.copy()
    page_views_df = fact_page_views.copy()

    report_views_df["date"] = _coerce_to_datetime(report_views_df[date_col]).dt.normalize()
    page_views_df["date"] = _coerce_to_datetime(page_views_df[date_col]).dt.normalize()

    if report_views_df["date"].isna().any():
        raise ValueError(
            f"Unable to parse all values in '{date_col}' from fact_report_views."
        )
    if page_views_df["date"].isna().any():
        raise ValueError(
            f"Unable to parse all values in '{date_col}' from fact_page_views."
        )

    report_views_df["report_id"] = report_views_df[report_col]
    report_views_df["user_id"] = report_views_df[user_col]
    page_views_df["report_id"] = page_views_df[report_col]
    page_views_df["user_id"] = page_views_df[user_col]

    report_views_measure = (
        "view_count"
        if "view_count" in report_views_df.columns
        else "views"
        if "views" in report_views_df.columns
        else None
    )
    page_views_measure = (
        "page_view_count" if "page_view_count" in page_views_df.columns else None
    )

    if report_views_measure is not None and not pd.api.types.is_numeric_dtype(
        report_views_df[report_views_measure]
    ):
        raise TypeError(f"'{report_views_measure}' must be numeric when provided.")
    if page_views_measure is not None and not pd.api.types.is_numeric_dtype(
        page_views_df[page_views_measure]
    ):
        raise TypeError(f"'{page_views_measure}' must be numeric when provided.")

    user_daily_views = (
        report_views_df.groupby(["date", "report_id", "user_id"], as_index=False).agg(
            user_views=(
                report_views_measure,
                "sum",
            )
            if report_views_measure is not None
            else ("report_id", "size")
        )
    )

    first_seen = (
        user_daily_views.groupby(["report_id", "user_id"], as_index=False)["date"]
        .min()
        .rename(columns={"date": "first_view_date"})
    )
    user_daily_views = user_daily_views.merge(
        first_seen,
        on=["report_id", "user_id"],
        how="left",
    )
    user_daily_views["is_repeat_user"] = (
        user_daily_views["date"] > user_daily_views["first_view_date"]
    )

    repeat_users_daily = (
        user_daily_views.groupby(["date", "report_id"], as_index=False)
        .agg(
            unique_viewers=("user_id", "nunique"),
            repeat_users=("is_repeat_user", "sum"),
            total_views=("user_views", "sum"),
        )
    )
    repeat_users_daily["repeat_user_rate"] = repeat_users_daily["repeat_users"].div(
        repeat_users_daily["unique_viewers"].replace(0, pd.NA)
    )
    repeat_users_daily["repeat_user_rate"] = repeat_users_daily[
        "repeat_user_rate"
    ].fillna(0.0)

    def _top_user_share(group: pd.DataFrame) -> float:
        """Calculate the share of views from the top 10 percent of users."""
        total_views = group["user_views"].sum()
        if total_views == 0 or group.empty:
            return 0.0

        top_n = max(1, math.ceil(len(group) * 0.10))
        top_views = group["user_views"].nlargest(top_n).sum()
        return float(top_views / total_views)

    concentration_daily = (
        user_daily_views.groupby(["date", "report_id"])
        .apply(_top_user_share, include_groups=False)
        .reset_index(name="top_10pct_user_share")
    )

    daily_usage = (
        user_daily_views.groupby(["date", "report_id"], as_index=False)
        .agg(daily_views=("user_views", "sum"))
        .sort_values(["report_id", "date"])
        .reset_index(drop=True)
    )
    active_dates = daily_usage.loc[daily_usage["daily_views"] > 0].copy()
    active_dates["previous_active_date"] = active_dates.groupby("report_id")["date"].shift(1)
    daily_usage = daily_usage.merge(
        active_dates[["date", "report_id", "previous_active_date"]],
        on=["date", "report_id"],
        how="left",
    )
    daily_usage["days_since_last_use"] = (
        daily_usage["date"] - daily_usage["previous_active_date"]
    ).dt.days
    daily_usage["days_since_last_use"] = daily_usage["days_since_last_use"].fillna(0).astype(int)

    user_daily_pages = (
        page_views_df.groupby(["date", "report_id", "user_id"], as_index=False).agg(
            page_views=(
                page_views_measure,
                "sum",
            )
            if page_views_measure is not None
            else ("report_id", "size")
        )
    )
    session_depth_daily = (
        user_daily_pages.groupby(["date", "report_id"], as_index=False)
        .agg(avg_pages_per_user=("page_views", "mean"))
    )

    engagement_mart = (
        repeat_users_daily[
            ["date", "report_id", "repeat_user_rate", "unique_viewers", "total_views"]
        ]
        .merge(
            concentration_daily,
            on=["date", "report_id"],
            how="left",
        )
        .merge(
            daily_usage[["date", "report_id", "days_since_last_use"]],
            on=["date", "report_id"],
            how="left",
        )
        .merge(
            session_depth_daily,
            on=["date", "report_id"],
            how="left",
        )
    )

    engagement_mart["top_10pct_user_share"] = engagement_mart[
        "top_10pct_user_share"
    ].fillna(0.0)
    engagement_mart["avg_pages_per_user"] = engagement_mart[
        "avg_pages_per_user"
    ].fillna(0.0)

    result = (
        engagement_mart[
            [
                "date",
                "report_id",
                "repeat_user_rate",
                "top_10pct_user_share",
                "days_since_last_use",
                "avg_pages_per_user",
            ]
        ]
        .sort_values(["report_id", "date"])
        .reset_index(drop=True)
    )

    return result
