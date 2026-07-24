"""Behavioural feature engineering helpers for report engagement marts."""

from __future__ import annotations

import math

import pandas as pd

from src.features._common import _normalize_date_column, _validate_input_columns


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
        Behavioural feature mart at the ``date`` and ``report_id`` grain with:

        repeat_user_rate
            Fraction of active users on a given day who had previously viewed
            the report on an earlier date (range 0–1).
        top_1_user_view_share
            Fraction of total views on a given day attributable to the single
            highest-consuming user (range 0–1). Measures single-user
            dependency risk.
        top_10pct_user_share
            Fraction of total views from the top 10 percent of users by view
            volume on a given day (range 0–1). Measures broad concentration
            risk. Distinct from ``top_1_user_view_share``.
        days_since_last_use
            Calendar days elapsed since the report last had at least one view
            event, measured from each row's date. Zero on the first active day.
        avg_pages_per_user
            Mean number of page views per user on a given day, derived from
            ``fact_page_views``.
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
                "top_1_user_view_share",
                "top_10pct_user_share",
                "days_since_last_use",
                "avg_pages_per_user",
            ]
        )

    report_views_df = fact_report_views.copy()
    page_views_df = fact_page_views.copy()

    report_views_df["date"] = _normalize_date_column(report_views_df, date_col)
    page_views_df["date"] = _normalize_date_column(page_views_df, date_col)

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
    # DEPRECATED: is_repeat_user uses lifetime_returned semantics (date > first_view_date).
    # Canonical replacement: lifetime_returned_flag.
    # For windowed returning-user metrics use returning_user_count_28d (Sprint 6).
    # Renamed to lifetime_returned_flag internally; is_repeat_user kept as alias below.
    user_daily_views["lifetime_returned_flag"] = (
        user_daily_views["date"] > user_daily_views["first_view_date"]
    )
    # DEPRECATED alias — retained so internal aggregation below continues to work.
    user_daily_views["is_repeat_user"] = user_daily_views["lifetime_returned_flag"]

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

    def _top_1_share(group: pd.DataFrame) -> float:
        """Fraction of views from the single highest-consuming user."""
        total_views = group["user_views"].sum()
        if total_views == 0 or group.empty:
            return 0.0
        return float(group["user_views"].max() / total_views)

    def _top_10pct_share(group: pd.DataFrame) -> float:
        """Fraction of views from the top 10 percent of users by volume."""
        total_views = group["user_views"].sum()
        if total_views == 0 or group.empty:
            return 0.0
        top_n = max(1, math.ceil(len(group) * 0.10))
        top_views = group["user_views"].nlargest(top_n).sum()
        return float(top_views / total_views)

    top_1_daily = (
        user_daily_views.groupby(["date", "report_id"])
        .apply(_top_1_share, include_groups=False)
        .reset_index(name="top_1_user_view_share")
    )

    concentration_daily = (
        user_daily_views.groupby(["date", "report_id"])
        .apply(_top_10pct_share, include_groups=False)
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
        .merge(top_1_daily, on=["date", "report_id"], how="left")
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

    engagement_mart["top_1_user_view_share"] = engagement_mart[
        "top_1_user_view_share"
    ].fillna(0.0)
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
                "top_1_user_view_share",
                "top_10pct_user_share",
                "days_since_last_use",
                "avg_pages_per_user",
            ]
        ]
        .sort_values(["report_id", "date"])
        .reset_index(drop=True)
    )

    return result
