"""Simple baseline forecasting models for report usage.

These functions are intentionally lightweight Day 1 baselines. They follow the
feature mart column conventions produced by the feature engineering notebook:
`date`, `report_id`, and `daily_views`.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


DEFAULT_DATE_COL = "date"
DEFAULT_TARGET_COL = "daily_views"
DEFAULT_SEASON_LENGTH = 7


def _prepare_series(
    df: pd.DataFrame,
    date_col: str = DEFAULT_DATE_COL,
    target_col: str = DEFAULT_TARGET_COL,
) -> pd.Series:
    """Return a clean, date-sorted daily target series."""
    required_columns = {date_col, target_col}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    prepared = df[[date_col, target_col]].copy()
    prepared[date_col] = pd.to_datetime(prepared[date_col], errors="coerce")
    prepared[target_col] = pd.to_numeric(prepared[target_col], errors="coerce")
    prepared = prepared.dropna(subset=[date_col, target_col]).sort_values(date_col)

    if prepared.empty:
        raise ValueError("No valid rows available for forecasting.")

    daily = prepared.groupby(date_col, as_index=True)[target_col].sum().sort_index()
    daily.index.name = date_col
    return daily


def _future_dates(last_date: pd.Timestamp, horizon: int) -> pd.DatetimeIndex:
    """Build the forecast date index immediately after the latest observation."""
    if horizon <= 0:
        raise ValueError("horizon must be greater than zero.")

    return pd.date_range(
        start=pd.to_datetime(last_date) + pd.Timedelta(days=1),
        periods=horizon,
        freq="D",
    )


def _format_forecast(
    dates: pd.DatetimeIndex,
    forecast_values: Any,
    model_name: str,
    date_col: str = DEFAULT_DATE_COL,
) -> pd.DataFrame:
    """Return forecasts in the common output schema."""
    forecast = np.clip(np.asarray(forecast_values, dtype=float), 0, None)
    return pd.DataFrame(
        {
            date_col: dates,
            "forecast": forecast,
            "model_name": model_name,
        }
    )


def naive_forecast(
    df: pd.DataFrame,
    horizon: int = 30,
    date_col: str = DEFAULT_DATE_COL,
    target_col: str = DEFAULT_TARGET_COL,
) -> pd.DataFrame:
    """Forecast each future day as the most recent observed value."""
    series = _prepare_series(df, date_col=date_col, target_col=target_col)
    dates = _future_dates(series.index[-1], horizon)
    values = np.repeat(series.iloc[-1], horizon)
    return _format_forecast(dates, values, "naive", date_col=date_col)


def moving_average_forecast(
    df: pd.DataFrame,
    horizon: int = 30,
    window: int = 7,
    date_col: str = DEFAULT_DATE_COL,
    target_col: str = DEFAULT_TARGET_COL,
) -> pd.DataFrame:
    """Forecast each future day as the recent moving average."""
    if window <= 0:
        raise ValueError("window must be greater than zero.")

    series = _prepare_series(df, date_col=date_col, target_col=target_col)
    dates = _future_dates(series.index[-1], horizon)
    average_value = series.tail(window).mean()
    values = np.repeat(average_value, horizon)
    return _format_forecast(dates, values, f"moving_average_{window}d", date_col=date_col)


def seasonal_naive_forecast(
    df: pd.DataFrame,
    horizon: int = 30,
    season_length: int = DEFAULT_SEASON_LENGTH,
    date_col: str = DEFAULT_DATE_COL,
    target_col: str = DEFAULT_TARGET_COL,
) -> pd.DataFrame:
    """Forecast future days by repeating the most recent seasonal pattern."""
    if season_length <= 0:
        raise ValueError("season_length must be greater than zero.")

    series = _prepare_series(df, date_col=date_col, target_col=target_col)
    dates = _future_dates(series.index[-1], horizon)

    if len(series) < season_length:
        values = np.repeat(series.iloc[-1], horizon)
    else:
        last_season = series.tail(season_length).to_numpy(dtype=float)
        repeats = int(np.ceil(horizon / season_length))
        values = np.tile(last_season, repeats)[:horizon]

    return _format_forecast(
        dates,
        values,
        f"seasonal_naive_{season_length}d",
        date_col=date_col,
    )
