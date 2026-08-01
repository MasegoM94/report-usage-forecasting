"""Tests for normalize_forecast_date in src/app/utils/load_data.py."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.app.utils.load_data import normalize_forecast_date


class TestNormalizeForecastDate:
    def test_forecast_date_promoted_to_Date(self):
        df = pd.DataFrame({
            "forecast_date": ["2026-01-01", "2026-01-02"],
            "forecast": [10.0, 11.0],
        })
        result = normalize_forecast_date(df)
        assert "Date" in result.columns

    def test_promoted_Date_is_datetime(self):
        df = pd.DataFrame({"forecast_date": ["2026-01-01", "2026-01-02"], "forecast": [1.0, 2.0]})
        result = normalize_forecast_date(df)
        assert pd.api.types.is_datetime64_any_dtype(result["Date"])

    def test_existing_Date_column_preserved(self):
        df = pd.DataFrame({
            "Date": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "forecast": [1.0, 2.0],
        })
        result = normalize_forecast_date(df)
        assert "Date" in result.columns
        assert list(result["Date"]) == list(df["Date"])

    def test_existing_Date_takes_priority_over_forecast_date(self):
        df = pd.DataFrame({
            "Date":          pd.to_datetime(["2026-01-01"]),
            "forecast_date": ["2026-02-01"],
            "forecast": [1.0],
        })
        result = normalize_forecast_date(df)
        # Date already present — should not be overwritten
        assert result["Date"].iloc[0] == pd.Timestamp("2026-01-01")

    def test_Date_converted_to_datetime_when_string(self):
        df = pd.DataFrame({
            "Date": ["2026-01-01", "2026-01-02"],
            "forecast": [1.0, 2.0],
        })
        result = normalize_forecast_date(df)
        assert pd.api.types.is_datetime64_any_dtype(result["Date"])

    def test_invalid_dates_coerced_to_NaT(self):
        df = pd.DataFrame({
            "forecast_date": ["2026-01-01", "not-a-date", "2026-01-03"],
            "forecast": [1.0, 2.0, 3.0],
        })
        result = normalize_forecast_date(df)
        assert pd.isna(result["Date"].iloc[1])
        assert not pd.isna(result["Date"].iloc[0])

    def test_empty_dataframe_returned_unchanged(self):
        df = pd.DataFrame()
        result = normalize_forecast_date(df)
        assert result.empty

    def test_neither_Date_nor_forecast_date_leaves_no_Date_column(self):
        df = pd.DataFrame({"forecast": [1.0, 2.0]})
        result = normalize_forecast_date(df)
        assert "Date" not in result.columns

    def test_original_dataframe_not_mutated(self):
        df = pd.DataFrame({"forecast_date": ["2026-01-01"], "forecast": [1.0]})
        _ = normalize_forecast_date(df)
        assert "Date" not in df.columns
