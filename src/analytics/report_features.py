"""Report-level behavioural feature helpers.

These functions aggregate existing processed Power BI usage tables into one
simple row per report. They do not regenerate source data or alter the
forecasting pipeline.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _empty_features() -> pd.DataFrame:
    """Return an empty report feature frame with the expected columns."""
    return pd.DataFrame(
        columns=[
            "report_id",
            "report_name",
            "avg_views",
            "repeat_rate",
            "top_user_concentration",
            "days_active",
            "total_views",
            "unique_users",
            "avg_load_time",
            "p90_load_time",
            "latest_views",
            "prior_views",
            "usage_change_pct",
        ]
    )


def _with_date(df: pd.DataFrame, dim_date: pd.DataFrame | None = None) -> pd.DataFrame:
    """Return a copy of df with a parsed date column when possible."""
    result = df.copy()
    if "date" in result.columns:
        result["date"] = pd.to_datetime(result["date"], errors="coerce")
        return result

    if "date_key" in result.columns and dim_date is not None and not dim_date.empty:
        date_lookup = dim_date[["date_key", "date"]].copy()
        date_lookup["date"] = pd.to_datetime(date_lookup["date"], errors="coerce")
        result = result.merge(date_lookup, on="date_key", how="left")

    return result


def _view_measure(df: pd.DataFrame) -> pd.Series:
    """Return the best available report view measure."""
    if "view_count" in df.columns:
        return pd.to_numeric(df["view_count"], errors="coerce").fillna(0)
    if "daily_views" in df.columns:
        return pd.to_numeric(df["daily_views"], errors="coerce").fillna(0)
    if "views" in df.columns:
        return pd.to_numeric(df["views"], errors="coerce").fillna(0)
    return pd.Series(1, index=df.index, dtype="float64")


def build_report_features(
    daily_adoption: pd.DataFrame,
    fact_report_views: pd.DataFrame | None = None,
    report_performance: pd.DataFrame | None = None,
    dim_report: pd.DataFrame | None = None,
    dim_date: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build one explainable behavioural feature row per report.

    Parameters
    ----------
    daily_adoption:
        Processed daily report adoption table, preferably
        ``mart_forecast_features.csv`` or ``mart_report_daily_adoption.csv``.
    fact_report_views:
        Optional user-level report view fact table. Used for repeat rate,
        unique users, and top-user concentration when available.
    report_performance:
        Optional processed report performance table. Used for average and p90
        load time summaries.
    dim_report:
        Optional report dimension used to attach report names.
    dim_date:
        Optional date dimension used when facts contain ``date_key`` instead of
        ``date``.
    """
    if daily_adoption is None or daily_adoption.empty:
        return _empty_features()

    usage = _with_date(daily_adoption, dim_date)
    if "report_id" not in usage.columns or "daily_views" not in usage.columns:
        return _empty_features()

    usage["daily_views"] = pd.to_numeric(usage["daily_views"], errors="coerce").fillna(0)
    usage = usage.dropna(subset=["report_id"])

    feature_df = (
        usage.groupby("report_id", as_index=False)
        .agg(
            avg_views=("daily_views", "mean"),
            total_views=("daily_views", "sum"),
            days_active=("daily_views", lambda series: int((series > 0).sum())),
        )
    )

    if "date" in usage.columns:
        latest_dates = usage.groupby("report_id")["date"].transform("max")
        prior_dates = usage.groupby("report_id")["date"].transform(
            lambda series: series[series < series.max()].max()
        )
        latest = (
            usage.loc[usage["date"].eq(latest_dates)]
            .groupby("report_id", as_index=False)["daily_views"]
            .sum()
            .rename(columns={"daily_views": "latest_views"})
        )
        prior = (
            usage.loc[usage["date"].eq(prior_dates)]
            .groupby("report_id", as_index=False)["daily_views"]
            .sum()
            .rename(columns={"daily_views": "prior_views"})
        )
        feature_df = feature_df.merge(latest, on="report_id", how="left")
        feature_df = feature_df.merge(prior, on="report_id", how="left")
    else:
        feature_df["latest_views"] = np.nan
        feature_df["prior_views"] = np.nan

    feature_df["usage_change_pct"] = (
        feature_df["latest_views"] - feature_df["prior_views"]
    ).div(feature_df["prior_views"].where(feature_df["prior_views"].ne(0)))
    feature_df["usage_change_pct"] = feature_df["usage_change_pct"].replace(
        [np.inf, -np.inf],
        np.nan,
    )

    if dim_report is not None and {"report_id", "report_name"}.issubset(dim_report.columns):
        feature_df = feature_df.merge(
            dim_report[["report_id", "report_name"]].drop_duplicates(),
            on="report_id",
            how="left",
        )
    else:
        feature_df["report_name"] = np.nan

    if fact_report_views is not None and not fact_report_views.empty:
        views = _with_date(fact_report_views, dim_date)
        user_col = "user_id" if "user_id" in views.columns else "user_key" if "user_key" in views.columns else None

        if "report_id" in views.columns and user_col is not None:
            views = views.dropna(subset=["report_id", user_col]).copy()
            views["view_count"] = _view_measure(views)

            user_report_views = (
                views.groupby(["report_id", user_col], as_index=False)["view_count"].sum()
            )
            repeat = (
                user_report_views.assign(is_repeat_user=lambda df: df["view_count"] > 1)
                .groupby("report_id", as_index=False)
                .agg(
                    unique_users=(user_col, "nunique"),
                    repeat_users=("is_repeat_user", "sum"),
                    top_user_views=("view_count", "max"),
                    user_level_views=("view_count", "sum"),
                )
            )
            repeat["repeat_rate"] = repeat["repeat_users"].div(
                repeat["unique_users"].replace(0, np.nan)
            )
            repeat["top_user_concentration"] = repeat["top_user_views"].div(
                repeat["user_level_views"].replace(0, np.nan)
            )
            feature_df = feature_df.merge(
                repeat[
                    [
                        "report_id",
                        "unique_users",
                        "repeat_rate",
                        "top_user_concentration",
                    ]
                ],
                on="report_id",
                how="left",
            )
        else:
            feature_df["unique_users"] = np.nan
            feature_df["repeat_rate"] = np.nan
            feature_df["top_user_concentration"] = np.nan
    else:
        feature_df["unique_users"] = np.nan
        feature_df["repeat_rate"] = np.nan
        feature_df["top_user_concentration"] = np.nan

    if report_performance is not None and not report_performance.empty:
        perf = report_performance.copy()
        if "report_id" in perf.columns:
            agg_map = {}
            if "avg_load_time" in perf.columns:
                perf["avg_load_time"] = pd.to_numeric(perf["avg_load_time"], errors="coerce")
                agg_map["avg_load_time"] = ("avg_load_time", "mean")
            if "p90_load_time" in perf.columns:
                perf["p90_load_time"] = pd.to_numeric(perf["p90_load_time"], errors="coerce")
                agg_map["p90_load_time"] = ("p90_load_time", "mean")

            if agg_map:
                perf_features = perf.groupby("report_id", as_index=False).agg(**agg_map)
                feature_df = feature_df.merge(perf_features, on="report_id", how="left")

    for col in ["avg_load_time", "p90_load_time"]:
        if col not in feature_df.columns:
            feature_df[col] = np.nan

    output_columns = _empty_features().columns.tolist()
    feature_df = feature_df.reindex(columns=output_columns)

    return feature_df.sort_values("report_id").reset_index(drop=True)
