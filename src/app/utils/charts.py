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
        font=dict(size=13),
    )
    fig.update_layout(
        height=360,
        margin=dict(l=20, r=20, t=40, b=20),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
    )
    return fig


def usage_forecast_chart(
    report_forecasts: pd.DataFrame,
    report_title: str | None = None,
) -> go.Figure:
    """Create a combined historical actuals and forecast chart.

    Renders actuals (when present), forecast values, and a prediction interval band.
    Actuals and forecasts are distinguished by both colour AND line style so the
    chart is readable without relying on colour alone.

    Parameters
    ----------
    report_forecasts:
        DataFrame with at least a ``Date`` column plus some subset of
        ``actual``, ``forecast``, ``lower_ci`` / ``upper_ci``.
    report_title:
        Optional report name appended to the chart title for context.
    """
    if report_forecasts.empty or "Date" not in report_forecasts.columns:
        return _empty_figure("No forecast data available for this report.")

    has_actuals = "actual" in report_forecasts.columns and report_forecasts["actual"].notna().any()
    has_forecast = "forecast" in report_forecasts.columns and report_forecasts["forecast"].notna().any()
    if not has_actuals and not has_forecast:
        return _empty_figure("No forecast data available for this report.")

    df = report_forecasts.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).sort_values("Date")

    fig = go.Figure()

    # Historical actuals — solid blue line with circle markers
    historical = df[df["actual"].notna()] if "actual" in df.columns else pd.DataFrame()
    if not historical.empty:
        fig.add_trace(
            go.Scatter(
                x=historical["Date"],
                y=historical["actual"],
                mode="lines+markers",
                name="Historical usage",
                line=dict(color="#2563eb", width=2, dash="solid"),
                marker=dict(symbol="circle", size=4, color="#2563eb"),
            )
        )

    # Forecast — dashed orange line (distinguishable without colour)
    if "forecast" in df.columns:
        forecast_df = df[df["forecast"].notna()]
        if not forecast_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=forecast_df["Date"],
                    y=forecast_df["forecast"],
                    mode="lines+markers",
                    name="Forecast",
                    line=dict(color="#f97316", width=2, dash="dash"),
                    marker=dict(symbol="diamond", size=4, color="#f97316"),
                )
            )

    # Prediction interval band — correctly labelled (not "confidence interval")
    if {"lower_ci", "upper_ci"}.issubset(df.columns):
        pi = df[df["lower_ci"].notna() & df["upper_ci"].notna()]
        if not pi.empty:
            fig.add_trace(
                go.Scatter(
                    x=pd.concat([pi["Date"], pi["Date"].iloc[::-1]]),
                    y=pd.concat([pi["upper_ci"], pi["lower_ci"].iloc[::-1]]),
                    fill="toself",
                    fillcolor="rgba(249, 115, 22, 0.15)",
                    line=dict(color="rgba(255,255,255,0)"),
                    hoverinfo="skip",
                    name="Prediction interval",
                    showlegend=True,
                )
            )

    title_text = "Report usage forecast"
    if report_title:
        title_text = f"{report_title} — usage forecast"

    fig.update_layout(
        title=dict(text=title_text, font=dict(size=14)),
        height=420,
        hovermode="x unified",
        margin=dict(l=20, r=20, t=56, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        xaxis_title="Date",
        yaxis_title="Views",
        template="plotly_white",
    )
    return fig
