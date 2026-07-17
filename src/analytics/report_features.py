"""Report-level behavioural feature helpers.

These functions aggregate existing processed Power BI usage tables into one
simple row per report. They do not regenerate source data or alter the
forecasting pipeline.

Output column glossary
----------------------
report_id               : Natural key for the report.
report_name             : Human-readable label from dim_report (may be null).
avg_views               : Mean daily_views across the report's full history.
total_views             : Sum of daily_views across the report's full history.
days_active             : Number of calendar days on which daily_views > 0.
unique_users            : Distinct user count over the full history window.
repeat_rate             : Fraction of users who viewed the report on more than
                          one distinct day (range 0–1).
top_1_user_view_share   : Fraction of total views attributable to the single
                          highest-consuming user (range 0–1). Measures
                          single-user dependency risk; distinct from
                          top_10pct_users_view_share.
avg_load_time           : Mean of avg_load_time from the performance mart.
p90_load_time           : Mean of p90_load_time from the performance mart.
recent_28d_views        : Total daily_views over the most recent 28 calendar
                          days in the series. Null when history < 56 days.
previous_28d_views      : Total daily_views over the 28 days immediately
                          preceding the recent window. Null when history < 56
                          days.
usage_change_28d_pct    : (recent_28d_views - previous_28d_views) /
                          previous_28d_views.
                          · previous=0, recent=0  → 0.0 ("no activity")
                          · previous=0, recent>0  → null; newly_active_flag=True
                          · history < 56 days     → null
trend_history_sufficient: True when the series contains ≥ 56 calendar days,
                          which is the minimum needed to compute both 28-day
                          windows.
newly_active_flag       : True when previous_28d_views=0 and
                          recent_28d_views>0 (mathematically undefined
                          growth percentage).
usage_trend_12w_slope   : Linear slope (views per week) estimated from the
                          most recent 12 complete ISO weeks. Computed via
                          ordinary least squares against week index (0–11).
                          Null when fewer than MIN_TREND_WEEKS complete weeks
                          are available. This is a rate (views/week), not a
                          percentage.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Minimum number of complete ISO weeks required to compute usage_trend_12w_slope.
MIN_TREND_WEEKS: int = 8


def _empty_features() -> pd.DataFrame:
    """Return an empty report feature frame with the expected schema."""
    return pd.DataFrame(
        columns=[
            "report_id",
            "report_name",
            "avg_views",
            "repeat_rate",
            "top_1_user_view_share",
            "days_active",
            "total_views",
            "unique_users",
            "avg_load_time",
            "p90_load_time",
            "recent_28d_views",
            "previous_28d_views",
            "usage_change_28d_pct",
            "newly_active_flag",
            "trend_history_sufficient",
            "usage_trend_12w_slope",
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
    """Return the best available report view measure as a numeric series."""
    if "view_count" in df.columns:
        return pd.to_numeric(df["view_count"], errors="coerce").fillna(0)
    if "daily_views" in df.columns:
        return pd.to_numeric(df["daily_views"], errors="coerce").fillna(0)
    if "views" in df.columns:
        return pd.to_numeric(df["views"], errors="coerce").fillna(0)
    return pd.Series(1, index=df.index, dtype="float64")


def _compute_window_metrics(series: pd.DataFrame) -> dict:
    """Compute 28-day window metrics and 12-week trend slope for one report.

    Parameters
    ----------
    series:
        Daily rows for a single report with at minimum ``date`` and
        ``daily_views`` columns, sorted ascending by date.

    Returns
    -------
    dict
        Keys: ``recent_28d_views``, ``previous_28d_views``,
        ``usage_change_28d_pct``, ``newly_active_flag``,
        ``trend_history_sufficient``, ``usage_trend_12w_slope``.
    """
    result: dict = {
        "recent_28d_views": np.nan,
        "previous_28d_views": np.nan,
        "usage_change_28d_pct": np.nan,
        "newly_active_flag": False,
        "trend_history_sufficient": False,
        "usage_trend_12w_slope": np.nan,
    }

    s = series.sort_values("date").reset_index(drop=True)
    n = len(s)

    if n < 56:
        return result

    result["trend_history_sufficient"] = True

    recent = int(s["daily_views"].iloc[-28:].sum())
    previous = int(s["daily_views"].iloc[-56:-28].sum())

    result["recent_28d_views"] = recent
    result["previous_28d_views"] = previous

    if previous == 0 and recent == 0:
        result["usage_change_28d_pct"] = 0.0
    elif previous == 0:
        # Mathematically undefined: report was dormant then reactivated.
        result["usage_change_28d_pct"] = np.nan
        result["newly_active_flag"] = True
    else:
        result["usage_change_28d_pct"] = (recent - previous) / previous

    # --- 12-week linear trend ---
    s_weekly = s.copy()
    s_weekly["week_start"] = s_weekly["date"].dt.to_period("W").dt.start_time
    weekly = (
        s_weekly.groupby("week_start")["daily_views"]
        .agg(week_views="sum", week_days="count")
        .reset_index()
        .sort_values("week_start")
    )

    # Drop an incomplete most-recent week (< 7 days in series).
    if not weekly.empty and weekly.iloc[-1]["week_days"] < 7:
        weekly = weekly.iloc[:-1]

    if len(weekly) >= MIN_TREND_WEEKS:
        weekly_12 = weekly.tail(12)
        x = np.arange(len(weekly_12), dtype=float)
        y = weekly_12["week_views"].values.astype(float)
        slope = float(np.polyfit(x, y, 1)[0])
        result["usage_trend_12w_slope"] = slope

    return result


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
        Must contain at minimum ``report_id`` and ``daily_views`` columns.
        A ``date`` column (or ``date_key`` when ``dim_date`` is supplied)
        is required for window-based metrics.
    fact_report_views:
        Optional user-level report view fact table. Used to compute
        ``repeat_rate``, ``unique_users``, and ``top_1_user_view_share``
        when available.
    report_performance:
        Optional processed report performance table. Used to summarise
        ``avg_load_time`` and ``p90_load_time`` across the history window.
    dim_report:
        Optional report dimension used to attach ``report_name``.
    dim_date:
        Optional date dimension used when facts contain ``date_key`` instead
        of ``date``.

    Returns
    -------
    pd.DataFrame
        One row per report with columns defined in the module docstring.
        See :func:`_empty_features` for the full column list.
    """
    if daily_adoption is None or daily_adoption.empty:
        return _empty_features()

    usage = _with_date(daily_adoption, dim_date)
    if "report_id" not in usage.columns or "daily_views" not in usage.columns:
        return _empty_features()

    usage["daily_views"] = pd.to_numeric(usage["daily_views"], errors="coerce").fillna(0)
    usage = usage.dropna(subset=["report_id"])

    # --- Whole-history aggregates ---
    feature_df = (
        usage.groupby("report_id", as_index=False)
        .agg(
            avg_views=("daily_views", "mean"),
            total_views=("daily_views", "sum"),
            days_active=("daily_views", lambda s: int((s > 0).sum())),
        )
    )

    # --- 28-day window metrics and 12-week slope ---
    if "date" in usage.columns:
        window_records = (
            usage.groupby("report_id", group_keys=False)[["date", "daily_views"]]
            .apply(_compute_window_metrics)
        )
        window_df = pd.DataFrame(
            list(window_records), index=window_records.index
        ).reset_index()
        window_df = window_df.rename(columns={"index": "report_id"})
        feature_df = feature_df.merge(window_df, on="report_id", how="left")
    else:
        for col in (
            "recent_28d_views",
            "previous_28d_views",
            "usage_change_28d_pct",
            "newly_active_flag",
            "trend_history_sufficient",
            "usage_trend_12w_slope",
        ):
            feature_df[col] = np.nan

    # --- Report name from dimension ---
    if dim_report is not None and {"report_id", "report_name"}.issubset(dim_report.columns):
        feature_df = feature_df.merge(
            dim_report[["report_id", "report_name"]].drop_duplicates(),
            on="report_id",
            how="left",
        )
    else:
        feature_df["report_name"] = np.nan

    # --- User-level metrics from raw fact table ---
    if fact_report_views is not None and not fact_report_views.empty:
        views = _with_date(fact_report_views, dim_date)
        user_col = (
            "user_id"
            if "user_id" in views.columns
            else "user_key"
            if "user_key" in views.columns
            else None
        )

        if "report_id" in views.columns and user_col is not None:
            views = views.dropna(subset=["report_id", user_col]).copy()
            views["view_count"] = _view_measure(views)

            user_report_views = (
                views.groupby(["report_id", user_col], as_index=False)["view_count"].sum()
            )

            repeat = (
                user_report_views.assign(
                    is_repeat_user=lambda df: df["view_count"] > 1
                )
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
            # top_1_user_view_share: fraction of total views from the single
            # highest-consuming user.  Measures single-user dependency risk.
            repeat["top_1_user_view_share"] = repeat["top_user_views"].div(
                repeat["user_level_views"].replace(0, np.nan)
            )
            feature_df = feature_df.merge(
                repeat[["report_id", "unique_users", "repeat_rate", "top_1_user_view_share"]],
                on="report_id",
                how="left",
            )
        else:
            feature_df["unique_users"] = np.nan
            feature_df["repeat_rate"] = np.nan
            feature_df["top_1_user_view_share"] = np.nan
    else:
        feature_df["unique_users"] = np.nan
        feature_df["repeat_rate"] = np.nan
        feature_df["top_1_user_view_share"] = np.nan

    # --- Performance summary from performance mart ---
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

    for col in ("avg_load_time", "p90_load_time"):
        if col not in feature_df.columns:
            feature_df[col] = np.nan

    output_columns = _empty_features().columns.tolist()
    feature_df = feature_df.reindex(columns=output_columns)

    return feature_df.sort_values("report_id").reset_index(drop=True)
