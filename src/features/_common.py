"""Shared private helpers for feature engineering modules."""

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


def _normalize_date_column(df: pd.DataFrame, date_col: str) -> pd.Series:
    """Return a normalized datetime series for a date column."""
    normalized_dates = _coerce_to_datetime(df[date_col]).dt.normalize()
    if normalized_dates.isna().any():
        raise ValueError(
            f"Unable to parse all values in '{date_col}' into valid datetimes."
        )
    return normalized_dates


def _validate_unique_grain(df: pd.DataFrame, key_columns: list[str], df_name: str) -> None:
    """Ensure a DataFrame is unique at the requested grain."""
    if df.duplicated(subset=key_columns).any():
        key_list = ", ".join(key_columns)
        raise ValueError(f"{df_name} must be unique at the grain: {key_list}")
