"""Performance feature engineering helpers for report telemetry marts."""

from __future__ import annotations

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


def build_report_performance_features(
    fact_report_loads: pd.DataFrame,
    date_col: str = "date",
    report_col: str = "report_id",
    load_time_col: str = "load_time",
) -> pd.DataFrame:
    """Build daily report-level performance features from load telemetry.

    Parameters
    ----------
    fact_report_loads:
        Report-load fact table at a load-event grain.
    date_col:
        Name of the source date column.
    report_col:
        Name of the source report identifier column.
    load_time_col:
        Name of the source load-time measure column.

    Returns
    -------
    pd.DataFrame
        Daily report-level performance mart with:
        `avg_load_time`, `p90_load_time`, `avg_load_time_7d`,
        and `load_time_wow_change`.
    """
    if not isinstance(fact_report_loads, pd.DataFrame):
        raise TypeError("fact_report_loads must be a pandas DataFrame.")

    _validate_input_columns(fact_report_loads, [date_col, report_col, load_time_col])

    if fact_report_loads.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "report_id",
                "avg_load_time",
                "p90_load_time",
                "avg_load_time_7d",
                "load_time_wow_change",
            ]
        )

    working_df = fact_report_loads.copy()
    working_df["date"] = _coerce_to_datetime(working_df[date_col]).dt.normalize()
    if working_df["date"].isna().any():
        raise ValueError(
            f"Unable to parse all values in '{date_col}' into valid datetimes."
        )

    working_df["report_id"] = working_df[report_col]
    working_df["load_time"] = pd.to_numeric(working_df[load_time_col], errors="coerce")
    if working_df["load_time"].isna().any():
        raise ValueError(
            f"Unable to convert all values in '{load_time_col}' to numeric load times."
        )
    if (working_df["load_time"] < 0).any():
        raise ValueError(f"'{load_time_col}' must not contain negative values.")

    performance_daily = (
        working_df.groupby(["date", "report_id"], as_index=False)
        .agg(
            avg_load_time=("load_time", "mean"),
            p90_load_time=("load_time", lambda series: series.quantile(0.90)),
            load_events=("load_time", "size"),
        )
        .sort_values(["report_id", "date"])
        .reset_index(drop=True)
    )

    report_groups = performance_daily.groupby("report_id", group_keys=False)
    performance_daily["avg_load_time_7d"] = report_groups["avg_load_time"].transform(
        lambda series: series.rolling(window=7, min_periods=1).mean()
    )

    lag_7d = report_groups["avg_load_time"].shift(7)
    performance_daily["load_time_wow_change"] = (
        performance_daily["avg_load_time"] - lag_7d
    ).div(lag_7d.where(lag_7d.ne(0)))
    performance_daily["load_time_wow_change"] = performance_daily[
        "load_time_wow_change"
    ].replace([float("inf"), float("-inf")], pd.NA)

    result = (
        performance_daily[
            [
                "date",
                "report_id",
                "avg_load_time",
                "p90_load_time",
                "avg_load_time_7d",
                "load_time_wow_change",
                "load_events",
            ]
        ]
        .sort_values(["report_id", "date"])
        .reset_index(drop=True)
    )

    return result
