"""Feature engineering helpers for report-level adoption marts."""

from __future__ import annotations

import pandas as pd

from src.features._common import (
    _normalize_date_column,
    _validate_input_columns,
    _validate_unique_grain,
)
from src.features.validate_series import validate_report_daily_series


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
    working_df["date"] = _normalize_date_column(working_df, date_col)
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
    enriched_df[date_col] = _normalize_date_column(enriched_df, date_col)

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


def build_report_daily_series(
    fact_report_views: pd.DataFrame,
    report_active_periods: pd.DataFrame | None = None,
    date_col: str = "date_key",
    report_col: str = "report_id",
    views_col: str | None = "view_count",
    active_start_col: str = "history_start",
    active_end_col: str = "history_end",
) -> pd.DataFrame:
    """Build the canonical daily Power BI report-usage series.

    Grain
    -----
    One row per (report_id, date) covering every calendar day in the report's
    active period.  ``daily_views`` is zero on days with no usage events —
    these rows are *not* back-written to the raw fact table.

    Zero-fill rule
    --------------
    A date is included in the output only when the report was active on that
    day.  Active rows that have no matching events in ``fact_report_views``
    receive ``daily_views = 0``.  The indicator columns distinguish observed
    days from imputed zeros:

    * ``is_observed_day`` — True when at least one source event existed for
      that report-date.
    * ``is_imputed_zero`` — True when the report was active but no event
      existed (i.e. ``daily_views == 0`` by construction).

    Active-period assumption
    ------------------------
    If ``report_active_periods`` is supplied, the active period for each
    report is taken from ``active_start_col`` and ``active_end_col`` (default
    ``history_start`` / ``history_end``).  These columns are expected to be
    present in ``report_archetypes.csv`` and are aligned with the report's
    launch date and either its retirement date or the dataset end date.

    If ``report_active_periods`` is *not* supplied, the function falls back to
    a **portfolio assumption**: active period = first observed event date
    through the dataset-wide maximum date.  This assumption is documented in
    the output via the ``active_period_source`` attribute on the returned
    DataFrame and should be replaced with explicit metadata once reliable
    launch/retire dates are available.

    Parameters
    ----------
    fact_report_views:
        Event-level or pre-aggregated fact table containing at minimum a date
        column and a report identifier column.  If ``views_col`` is provided
        and present, its values are summed; otherwise row count is used.
    report_active_periods:
        Optional table with one row per ``report_id`` and columns
        ``active_start_col`` / ``active_end_col`` defining the inclusive
        active window.  When omitted, the portfolio assumption is used.
    date_col:
        Name of the date column in ``fact_report_views``.  Accepts integer
        date-keys (``YYYYMMDD``), date strings, or datetime values.
    report_col:
        Name of the report identifier column in ``fact_report_views``.
    views_col:
        Optional name of a pre-aggregated view-count column.  When supplied
        and present, values are summed to produce ``daily_views``; when
        omitted or absent, row count is used instead.
    active_start_col:
        Column name in ``report_active_periods`` for the inclusive start of
        the active window (default ``"history_start"``).
    active_end_col:
        Column name in ``report_active_periods`` for the inclusive end of the
        active window (default ``"history_end"``).

    Returns
    -------
    pd.DataFrame
        Columns: ``report_id``, ``date``, ``daily_views``,
        ``is_observed_day``, ``is_imputed_zero``.
        Sorted by ``report_id`` then ``date``.
        ``daily_views`` is always a non-negative integer.
        Unique at the (``report_id``, ``date``) grain — raises
        ``ValueError`` if duplicates are detected before returning.

    Raises
    ------
    TypeError
        When ``fact_report_views`` or ``report_active_periods`` is not a
        DataFrame.
    ValueError
        When required columns are missing, dates cannot be parsed, or the
        output contains duplicate (``report_id``, ``date``) pairs.
    """
    if not isinstance(fact_report_views, pd.DataFrame):
        raise TypeError("fact_report_views must be a pandas DataFrame.")

    required_cols = [date_col, report_col]
    if views_col is not None and views_col in fact_report_views.columns:
        required_cols.append(views_col)
    _validate_input_columns(fact_report_views, [date_col, report_col])

    if fact_report_views.empty:
        return pd.DataFrame(
            columns=[
                "report_id",
                "date",
                "daily_views",
                "is_observed_day",
                "is_imputed_zero",
            ]
        )

    # ------------------------------------------------------------------ #
    # Step 1 — normalise the fact table and aggregate to daily grain       #
    # ------------------------------------------------------------------ #
    work = fact_report_views.copy()
    work["date"] = _normalize_date_column(work, date_col)
    work["report_id"] = work[report_col]

    _views_col = views_col if (views_col is not None and views_col in work.columns) else None

    if _views_col is not None:
        if not pd.api.types.is_numeric_dtype(work[_views_col]):
            raise TypeError(f"'{_views_col}' must be numeric when provided.")
        daily_obs = (
            work.groupby(["report_id", "date"], as_index=False)
            .agg(daily_views=(_views_col, "sum"))
        )
    else:
        daily_obs = (
            work.groupby(["report_id", "date"], as_index=False)
            .agg(daily_views=("report_id", "size"))
        )

    daily_obs["daily_views"] = daily_obs["daily_views"].astype(int)

    # ------------------------------------------------------------------ #
    # Step 2 — determine the active period for each report                 #
    # ------------------------------------------------------------------ #
    all_report_ids = daily_obs["report_id"].unique()
    dataset_end = daily_obs["date"].max()

    if report_active_periods is not None:
        if not isinstance(report_active_periods, pd.DataFrame):
            raise TypeError("report_active_periods must be a pandas DataFrame.")
        _validate_input_columns(
            report_active_periods,
            [report_col, active_start_col, active_end_col],
        )

        periods = report_active_periods[[report_col, active_start_col, active_end_col]].copy()
        periods = periods.rename(columns={report_col: "report_id"})
        periods["active_start"] = pd.to_datetime(periods[active_start_col], errors="coerce")
        periods["active_end"] = pd.to_datetime(periods[active_end_col], errors="coerce")

        # Reports with no retire date are active through the dataset end
        periods["active_end"] = periods["active_end"].fillna(dataset_end)

        active_period_source = "report_active_periods"
    else:
        # Portfolio fallback: first observation → dataset end
        first_obs = daily_obs.groupby("report_id", as_index=False)["date"].min()
        first_obs = first_obs.rename(columns={"date": "active_start"})
        first_obs["active_end"] = dataset_end
        periods = first_obs
        active_period_source = "first_observation_to_dataset_end"

    # Keep only reports that appear in the fact data
    periods = periods[periods["report_id"].isin(all_report_ids)].reset_index(drop=True)

    # ------------------------------------------------------------------ #
    # Step 3 — build the date spine                                        #
    # ------------------------------------------------------------------ #
    spine_frames = [
        pd.DataFrame(
            {
                "report_id": row.report_id,
                "date": pd.date_range(
                    start=row.active_start,
                    end=row.active_end,
                    freq="D",
                ),
            }
        )
        for row in periods[["report_id", "active_start", "active_end"]].itertuples(index=False)
    ]

    if not spine_frames:
        return pd.DataFrame(
            columns=[
                "report_id",
                "date",
                "daily_views",
                "is_observed_day",
                "is_imputed_zero",
            ]
        )

    spine = pd.concat(spine_frames, ignore_index=True)

    # ------------------------------------------------------------------ #
    # Step 4 — join observations onto the spine and fill zeros             #
    # ------------------------------------------------------------------ #
    result = spine.merge(daily_obs, on=["report_id", "date"], how="left")

    result["is_observed_day"] = result["daily_views"].notna()
    result["is_imputed_zero"] = ~result["is_observed_day"]

    result["daily_views"] = (
        result["daily_views"]
        .fillna(0)
        .astype(int)
    )

    result = (
        result[["report_id", "date", "daily_views", "is_observed_day", "is_imputed_zero"]]
        .sort_values(["report_id", "date"])
        .reset_index(drop=True)
    )

    # ------------------------------------------------------------------ #
    # Step 5 — validate the complete series before returning               #
    # ------------------------------------------------------------------ #
    validate_report_daily_series(result, expected_row_count=len(spine))

    # Attach source metadata as a non-persistent attribute for transparency
    result.attrs["active_period_source"] = active_period_source

    return result
