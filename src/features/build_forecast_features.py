"""Feature assembly helpers for modeling-ready forecasting marts."""

from __future__ import annotations

import pandas as pd

from src.features._common import (
    _normalize_date_column,
    _validate_input_columns,
    _validate_unique_grain,
)


def build_forecast_feature_table(
    mart_report_daily_adoption: pd.DataFrame,
    mart_user_engagement: pd.DataFrame,
    mart_report_performance: pd.DataFrame,
    date_col: str = "date",
    report_col: str = "report_id",
) -> pd.DataFrame:
    """Assemble the final modeling-ready forecast feature table.

    Parameters
    ----------
    mart_report_daily_adoption:
        Daily adoption mart containing the base usage and time-series features.
    mart_user_engagement:
        Behavioural feature mart at the `date` and `report_id` grain.
    mart_report_performance:
        Performance feature mart at the `date` and `report_id` grain.
    date_col:
        Name of the date join key in each input.
    report_col:
        Name of the report identifier join key in each input.

    Returns
    -------
    pd.DataFrame
        Final joined feature table for downstream forecasting work.
    """
    inputs = {
        "mart_report_daily_adoption": mart_report_daily_adoption,
        "mart_user_engagement": mart_user_engagement,
        "mart_report_performance": mart_report_performance,
    }

    for df_name, df in inputs.items():
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"{df_name} must be a pandas DataFrame.")
        _validate_input_columns(df, [date_col, report_col])

    adoption_df = mart_report_daily_adoption.copy()
    engagement_df = mart_user_engagement.copy()
    performance_df = mart_report_performance.copy()

    adoption_df[date_col] = _normalize_date_column(adoption_df, date_col)
    engagement_df[date_col] = _normalize_date_column(engagement_df, date_col)
    performance_df[date_col] = _normalize_date_column(performance_df, date_col)

    key_columns = [date_col, report_col]
    _validate_unique_grain(adoption_df, key_columns, "mart_report_daily_adoption")
    _validate_unique_grain(engagement_df, key_columns, "mart_user_engagement")
    _validate_unique_grain(performance_df, key_columns, "mart_report_performance")

    if "top_10pct_user_share" in engagement_df.columns and "top_user_share" not in engagement_df.columns:
        engagement_df = engagement_df.rename(
            columns={"top_10pct_user_share": "top_user_share"}
        )

    final_df = (
        adoption_df.merge(engagement_df, on=key_columns, how="left")
        .merge(performance_df, on=key_columns, how="left")
        .sort_values([report_col, date_col])
        .reset_index(drop=True)
    )

    return final_df
