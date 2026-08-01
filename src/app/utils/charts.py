"""Plotly chart builders for the Streamlit app."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


def _empty_figure(message: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        x=0.5,
        y=0.5,
        showarrow=False,
        xref="paper",
        yref="paper",
    )
    fig.update_layout(height=360, margin=dict(l=20, r=20, t=40, b=20))
    return fig


def usage_forecast_chart(report_forecasts: pd.DataFrame) -> go.Figure:
    """Create a combined historical actuals and forecast chart.

    Renders actuals (when present), forecast values, and a CI band.
    Returns an empty figure only when neither actuals nor forecast values
    are available — not when only one of the two is missing.
    """
    if report_forecasts.empty or "Date" not in report_forecasts.columns:
        return _empty_figure("No forecast data available for this report.")

    has_actuals = "actual" in report_forecasts.columns and report_forecasts["actual"].notna().any()
    has_forecast = "forecast" in report_forecasts.columns and report_forecasts["forecast"].notna().any()
    if not has_actuals and not has_forecast:
        return _empty_figure("No forecast data available for this report.")

    df = report_forecasts.sort_values("Date")
    fig = go.Figure()

    historical = df[df["actual"].notna()] if "actual" in df.columns else pd.DataFrame()
    if not historical.empty:
        fig.add_trace(
            go.Scatter(
                x=historical["Date"],
                y=historical["actual"],
                mode="lines",
                name="Historical usage",
                line=dict(color="#2563eb", width=2),
            )
        )

    if "forecast" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["Date"],
                y=df["forecast"],
                mode="lines",
                name="Forecast",
                line=dict(color="#f97316", width=2),
            )
        )

    if {"lower_ci", "upper_ci"}.issubset(df.columns):
        ci = df[df["lower_ci"].notna() & df["upper_ci"].notna()]
        if not ci.empty:
            fig.add_trace(
                go.Scatter(
                    x=pd.concat([ci["Date"], ci["Date"].iloc[::-1]]),
                    y=pd.concat([ci["upper_ci"], ci["lower_ci"].iloc[::-1]]),
                    fill="toself",
                    fillcolor="rgba(249, 115, 22, 0.15)",
                    line=dict(color="rgba(255,255,255,0)"),
                    hoverinfo="skip",
                    name="Confidence interval",
                )
            )

    fig.update_layout(
        height=420,
        hovermode="x unified",
        margin=dict(l=20, r=20, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis_title=None,
        yaxis_title="Views",
        template="plotly_white",
    )
    return fig
