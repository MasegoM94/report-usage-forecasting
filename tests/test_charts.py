"""Tests for src/app/utils/charts.py."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.app.utils.charts import usage_forecast_chart, _empty_figure


def _base_df(**overrides) -> pd.DataFrame:
    """Return a minimal forecast DataFrame with Date, actual, and forecast columns."""
    rows = {
        "Date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03",
                                 "2026-01-04", "2026-01-05"]),
        "actual":   [10.0, 12.0, 11.0, np.nan, np.nan],
        "forecast": [np.nan, np.nan, np.nan, 13.0, 14.0],
        "lower_ci": [np.nan, np.nan, np.nan, 11.0, 12.0],
        "upper_ci": [np.nan, np.nan, np.nan, 15.0, 16.0],
    }
    rows.update(overrides)
    return pd.DataFrame(rows)


class TestEmptyAndMissingCases:
    def test_empty_dataframe_returns_empty_figure(self):
        fig = usage_forecast_chart(pd.DataFrame())
        assert len(fig.data) == 0
        assert fig.layout.annotations

    def test_missing_date_column_returns_empty_figure(self):
        df = pd.DataFrame({"forecast": [1.0, 2.0]})
        fig = usage_forecast_chart(df)
        assert len(fig.data) == 0

    def test_no_actuals_and_no_forecast_returns_empty_figure(self):
        df = _base_df(actual=[np.nan] * 5, forecast=[np.nan] * 5)
        fig = usage_forecast_chart(df)
        assert len(fig.data) == 0


class TestActualsPresent:
    def test_historical_trace_added_when_actuals_present(self):
        df = _base_df()
        fig = usage_forecast_chart(df)
        names = [t.name for t in fig.data]
        assert "Historical usage" in names

    def test_historical_trace_uses_correct_values(self):
        df = _base_df()
        fig = usage_forecast_chart(df)
        hist = next(t for t in fig.data if t.name == "Historical usage")
        assert list(hist.y) == [10.0, 12.0, 11.0]

    def test_forecast_trace_added_alongside_actuals(self):
        df = _base_df()
        fig = usage_forecast_chart(df)
        names = [t.name for t in fig.data]
        assert "Forecast" in names

    def test_prediction_interval_band_added_when_ci_columns_present(self):
        df = _base_df()
        fig = usage_forecast_chart(df)
        names = [t.name for t in fig.data]
        assert "Prediction interval" in names

    def test_no_confidence_interval_label_anywhere(self):
        df = _base_df()
        fig = usage_forecast_chart(df)
        names = [t.name for t in fig.data]
        assert "Confidence interval" not in names


class TestActualColumnAbsent:
    def test_no_actual_column_does_not_crash(self):
        df = _base_df()
        df = df.drop(columns=["actual"])
        fig = usage_forecast_chart(df)  # must not raise
        assert fig is not None

    def test_forecast_trace_present_when_actual_column_absent(self):
        df = _base_df().drop(columns=["actual"])
        fig = usage_forecast_chart(df)
        names = [t.name for t in fig.data]
        assert "Forecast" in names

    def test_no_historical_trace_when_actual_column_absent(self):
        df = _base_df().drop(columns=["actual"])
        fig = usage_forecast_chart(df)
        names = [t.name for t in fig.data]
        assert "Historical usage" not in names


class TestAllActualsNull:
    def test_entirely_null_actuals_does_not_crash(self):
        df = _base_df(actual=[np.nan] * 5)
        fig = usage_forecast_chart(df)  # must not raise
        assert fig is not None

    def test_no_historical_trace_when_all_actuals_null(self):
        df = _base_df(actual=[np.nan] * 5)
        fig = usage_forecast_chart(df)
        names = [t.name for t in fig.data]
        assert "Historical usage" not in names

    def test_forecast_trace_present_when_all_actuals_null(self):
        df = _base_df(actual=[np.nan] * 5)
        fig = usage_forecast_chart(df)
        names = [t.name for t in fig.data]
        assert "Forecast" in names


class TestForecastOnly:
    def test_forecast_only_dataframe_renders(self):
        df = pd.DataFrame({
            "Date": pd.to_datetime(["2026-01-04", "2026-01-05"]),
            "forecast": [13.0, 14.0],
        })
        fig = usage_forecast_chart(df)
        names = [t.name for t in fig.data]
        assert "Forecast" in names
        assert "Historical usage" not in names

    def test_forecast_only_no_ci_skips_band(self):
        df = pd.DataFrame({
            "Date": pd.to_datetime(["2026-01-04", "2026-01-05"]),
            "forecast": [13.0, 14.0],
        })
        fig = usage_forecast_chart(df)
        names = [t.name for t in fig.data]
        assert "Prediction interval" not in names


class TestAccessibility:
    """Actuals and forecast must be distinguishable without relying on colour alone."""

    def test_historical_uses_solid_line(self):
        df = _base_df()
        fig = usage_forecast_chart(df)
        hist = next(t for t in fig.data if t.name == "Historical usage")
        assert hist.line.dash in (None, "solid")

    def test_forecast_uses_dash_line(self):
        df = _base_df()
        fig = usage_forecast_chart(df)
        fc = next(t for t in fig.data if t.name == "Forecast")
        assert fc.line.dash == "dash"

    def test_historical_has_circle_marker(self):
        df = _base_df()
        fig = usage_forecast_chart(df)
        hist = next(t for t in fig.data if t.name == "Historical usage")
        assert hist.marker.symbol == "circle"

    def test_forecast_has_diamond_marker(self):
        df = _base_df()
        fig = usage_forecast_chart(df)
        fc = next(t for t in fig.data if t.name == "Forecast")
        assert fc.marker.symbol == "diamond"

    def test_chart_has_title(self):
        df = _base_df()
        fig = usage_forecast_chart(df)
        assert fig.layout.title.text

    def test_chart_title_includes_report_name_when_provided(self):
        df = _base_df()
        fig = usage_forecast_chart(df, report_title="Finance Dashboard")
        assert "Finance Dashboard" in fig.layout.title.text

    def test_chart_has_yaxis_label(self):
        df = _base_df()
        fig = usage_forecast_chart(df)
        assert fig.layout.yaxis.title.text == "Views"

    def test_chart_has_xaxis_label(self):
        df = _base_df()
        fig = usage_forecast_chart(df)
        assert fig.layout.xaxis.title.text == "Date"

    def test_prediction_interval_in_legend(self):
        df = _base_df()
        fig = usage_forecast_chart(df)
        pi = next((t for t in fig.data if t.name == "Prediction interval"), None)
        assert pi is not None
        assert pi.showlegend is True

    def test_invalid_dates_coerced_no_crash(self):
        df = _base_df()
        df["Date"] = ["bad-date", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"]
        fig = usage_forecast_chart(df)
        assert fig is not None
