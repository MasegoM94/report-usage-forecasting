"""User-level behavioural feature helpers.

These functions aggregate existing processed Power BI usage tables into one
simple row per user. They do not regenerate source data or alter forecasting
logic.

Privacy note
------------
Outputs contain only ``user_key`` (surrogate identifier — e.g. UK_0001).
Direct identifiers (user_id / email, unique_user / display name) are never
included in analytics outputs. See src/analytics/privacy_policy.py.

Engagement definitions
----------------------
lifetime_returned_flag : True when a user has at least one active day after
    their first-ever active day (active_days > 1). This is a lifetime-history
    attribute, NOT a windowed returning-user metric.
    See src/analytics/engagement_definitions.py for canonical definitions.

repeat_usage_flag : DEPRECATED alias for lifetime_returned_flag.
    Retained for backwards compatibility. Remove in Sprint 7.
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

from src.analytics.privacy_policy import (
    PROHIBITED_OUTPUT_COLUMNS,
    validate_no_direct_identifiers,
)


def _empty_features() -> pd.DataFrame:
    """Return an empty user feature frame with the expected columns.

    Note: user_id and unique_user are intentionally excluded — outputs must
    contain only user_key (surrogate key). See privacy_policy.py.
    """
    return pd.DataFrame(
        columns=[
            "user_key",
            "total_views",
            "active_days",
            "distinct_reports",
            "avg_views_per_active_day",
            "first_active_date",
            "last_active_date",
            "days_since_last_active",
            "lifetime_returned_flag",
            # DEPRECATED: repeat_usage_flag is an alias for lifetime_returned_flag.
            # Remove in Sprint 7.
            "repeat_usage_flag",
        ]
    )


def _choose_user_column(df: pd.DataFrame) -> str | None:
    """Choose the best available user identifier column."""
    if "user_key" in df.columns:
        return "user_key"
    if "user_id" in df.columns:
        return "user_id"
    return None


def _with_date(df: pd.DataFrame, dim_date: pd.DataFrame | None = None) -> pd.DataFrame:
    """Return a copy of df with a parsed date column when possible."""
    result = df.copy()
    if "date" in result.columns:
        result["date"] = pd.to_datetime(result["date"], errors="coerce")
        return result

    if "date_key" in result.columns and dim_date is not None and not dim_date.empty:
        if {"date_key", "date"}.issubset(dim_date.columns):
            date_lookup = dim_date[["date_key", "date"]].copy()
            date_lookup["date"] = pd.to_datetime(date_lookup["date"], errors="coerce")
            result = result.merge(date_lookup, on="date_key", how="left")

    return result


def _view_measure(df: pd.DataFrame) -> pd.Series:
    """Return the best available usage count measure.

    ``view_count`` is the preferred report-view measure. ``page_view_count`` is
    used only as a fallback proxy when report-view facts are unavailable.
    """
    for column in ["view_count", "daily_views", "views", "page_view_count"]:
        if column in df.columns:
            return pd.to_numeric(df[column], errors="coerce").fillna(0)
    return pd.Series(1, index=df.index, dtype="float64")


def build_user_features(
    usage_events: pd.DataFrame,
    dim_user: pd.DataFrame | None = None,
    dim_date: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build one explainable behavioural feature row per user.

    Parameters
    ----------
    usage_events:
        Processed user-level usage table. ``fact_report_views.csv`` is
        preferred because it records report views by user. ``fact_page_views``
        can be used as a fallback proxy if report-view facts are unavailable.
    dim_user:
        Optional user dimension. NOT joined into output (privacy restriction).
        Accepted for API compatibility but its identity columns are not used.
    dim_date:
        Optional date dimension used when facts contain ``date_key`` instead of
        ``date``.

    Returns
    -------
    pd.DataFrame
        One row per user_key. Contains only pseudonymous identifiers (user_key).
        Direct identifiers (user_id, unique_user) are never included.
    """
    if usage_events is None or usage_events.empty:
        return _empty_features()

    usage = _with_date(usage_events, dim_date)
    user_col = _choose_user_column(usage)
    if user_col is None:
        return _empty_features()

    usage = usage.dropna(subset=[user_col]).copy()
    if usage.empty:
        return _empty_features()

    usage["view_measure"] = _view_measure(usage)

    feature_df = (
        usage.groupby(user_col, as_index=False)
        .agg(total_views=("view_measure", "sum"))
    )

    if "date" in usage.columns:
        activity_dates = usage.dropna(subset=["date"]).copy()
        if not activity_dates.empty:
            date_features = (
                activity_dates.groupby(user_col, as_index=False)
                .agg(
                    active_days=("date", "nunique"),
                    first_active_date=("date", "min"),
                    last_active_date=("date", "max"),
                )
            )
            feature_df = feature_df.merge(date_features, on=user_col, how="left")
            reference_date = activity_dates["date"].max()
            feature_df["days_since_last_active"] = (
                reference_date - feature_df["last_active_date"]
            ).dt.days
        else:
            feature_df["active_days"] = np.nan
            feature_df["first_active_date"] = pd.NaT
            feature_df["last_active_date"] = pd.NaT
            feature_df["days_since_last_active"] = np.nan
    else:
        feature_df["active_days"] = np.nan
        feature_df["first_active_date"] = pd.NaT
        feature_df["last_active_date"] = pd.NaT
        feature_df["days_since_last_active"] = np.nan

    if "report_id" in usage.columns:
        distinct_reports = (
            usage.groupby(user_col, as_index=False)["report_id"]
            .nunique()
            .rename(columns={"report_id": "distinct_reports"})
        )
        feature_df = feature_df.merge(distinct_reports, on=user_col, how="left")
    else:
        feature_df["distinct_reports"] = np.nan

    feature_df["avg_views_per_active_day"] = feature_df["total_views"].div(
        feature_df["active_days"].replace(0, np.nan)
    )

    # CANONICAL: lifetime_returned_flag — True when active_days > 1.
    # Definition: user has at least one active day strictly after their first.
    # This is a lifetime-history attribute (NOT a windowed returning-user metric).
    # See src/analytics/engagement_definitions.py.
    feature_df["lifetime_returned_flag"] = (
        feature_df["active_days"].fillna(0).gt(1)
    )

    # DEPRECATED: repeat_usage_flag — alias for lifetime_returned_flag.
    # Old definition was ambiguous: (active_days > 1) OR (total_views > 1).
    # New definition: active_days > 1 only. Retained for backwards compatibility.
    # Remove in Sprint 7.
    warnings.warn(
        "repeat_usage_flag is deprecated. Use lifetime_returned_flag instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    feature_df["repeat_usage_flag"] = feature_df["lifetime_returned_flag"]

    # Ensure user_key column is set correctly (never user_id in output).
    if user_col == "user_key" and "user_key" not in feature_df.columns:
        feature_df["user_key"] = feature_df[user_col]
    elif user_col != "user_key":
        # Input used user_id as fallback; create a placeholder user_key column.
        feature_df["user_key"] = feature_df[user_col]

    for column in ["first_active_date", "last_active_date"]:
        feature_df[column] = pd.to_datetime(feature_df[column], errors="coerce").dt.date

    output_columns = _empty_features().columns.tolist()
    feature_df = feature_df.reindex(columns=output_columns)

    # Privacy guard: ensure no prohibited columns leaked into output.
    validate_no_direct_identifiers(feature_df, context="user_features output")

    return feature_df.sort_values(["total_views", "user_key"], ascending=[False, True]).reset_index(
        drop=True
    )
