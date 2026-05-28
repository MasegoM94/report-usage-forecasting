"""Lightweight Streamlit reviewer app for Power BI usage forecasting outputs."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

APP_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from utils.charts import usage_forecast_chart
from utils.load_data import (
    available_reports,
    calculate_forecast_reliability_summary,
    calculate_report_adoption_metrics,
    calculate_user_adoption_metrics,
    get_at_risk_reports,
    load_app_data,
    row_for_report,
)


st.set_page_config(
    page_title="Power BI Report Usage Forecasts",
    page_icon=":material/bar_chart:",
    layout="wide",
)


def fmt_number(value: Any, decimals: int = 0) -> str:
    """Format numbers for metric cards without exposing pandas nulls."""
    if pd.isna(value):
        return "N/A"
    try:
        return f"{float(value):,.{decimals}f}"
    except (TypeError, ValueError):
        return str(value)


def fmt_percent(value: Any, decimals: int = 1) -> str:
    """Format ratio-like values as percentages."""
    if pd.isna(value):
        return "N/A"
    try:
        return f"{float(value) * 100:.{decimals}f}%"
    except (TypeError, ValueError):
        return str(value)


def adoption_value(metrics: dict[str, Any]) -> str:
    """Format total/active adoption metrics for compact overview cards."""
    return (
        f"{fmt_number(metrics.get('total'))} total | "
        f"{fmt_number(metrics.get('active'))} active | "
        f"{fmt_percent(metrics.get('active_pct'))} active"
    )


def metric_with_help(container: Any, label: str, value: Any, help_text: str) -> None:
    """Render st.metric with a caption fallback for older Streamlit versions."""
    try:
        container.metric(label, value, help=help_text)
    except TypeError:
        container.metric(label, value)
        container.caption(help_text)


def rerun_app() -> None:
    """Rerun the app after a session-state selector change."""
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()


def truthy(value: Any) -> bool:
    """Interpret boolean-ish CSV values without treating 'False' as true."""
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    if pd.isna(value):
        return False
    return bool(value)


def reliability_label(row: pd.Series) -> str:
    """Convert forecast reliability fields into a concise reviewer label."""
    if row.empty:
        return "Unknown"
    reliable = row.get("forecast_reliable")
    if pd.notna(reliable):
        return "Reliable" if truthy(reliable) else "At risk"
    wape = row.get("selected_wape")
    if pd.notna(wape):
        return "Reliable" if float(wape) <= 0.30 else "Review"
    return "Unknown"


def list_items(items: Any) -> None:
    """Render JSON list fields as compact bullets."""
    if isinstance(items, list) and items:
        for item in items:
            st.markdown(f"- {item}")
    elif isinstance(items, str) and items:
        st.write(items)
    else:
        st.caption("No details available.")


def diagnostics_flags(row: pd.Series) -> pd.DataFrame:
    """Build a small diagnostics table for the selected report."""
    if row.empty:
        return pd.DataFrame()
    flag_columns = [
        "performance_issue",
        "engagement_issue",
        "dependency_risk",
        "inactive_risk",
    ]
    rows = []
    for column in flag_columns:
        if column in row.index:
            rows.append(
                {
                    "Diagnostic": column.replace("_", " ").title(),
                    "Flagged": truthy(row.get(column)),
                }
            )
    return pd.DataFrame(rows)


def render_overview(data: dict[str, pd.DataFrame]) -> None:
    st.header("Overview")
    user_adoption = calculate_user_adoption_metrics(data)
    report_adoption = calculate_report_adoption_metrics(data)
    reliability = calculate_forecast_reliability_summary(data)
    at_risk_reports = get_at_risk_reports(data)

    at_risk_help = (
        "At risk reports are reports flagged by the analytics layer as showing declining usage, "
        "low adoption, weak repeat engagement, high dependence on a small number of users, "
        "performance issues, or unreliable forecast behaviour."
    )
    reliability_help = (
        "Forecast reliability represents the share or percentage of report forecasts that passed "
        "the project's reliability rules. A reliable forecast is one where the selected model has "
        "acceptable error, enough historical data, and stable enough usage patterns to support "
        "short-term forecasting. This percentage is the proportion of reports classified as "
        "forecast reliable; it does not mean the model is that percent accurate."
    )

    col1, col2, col3, col4 = st.columns(4)
    metric_with_help(
        col1,
        "Total Users",
        adoption_value(user_adoption),
        (
            "Total users are all users available in the user dimension or the most complete "
            "user-level output. Active users have at least one view or usage event. "
            f"{user_adoption.get('assumption', '')}"
        ),
    )
    metric_with_help(
        col2,
        "Total Reports",
        adoption_value(report_adoption),
        (
            "Total reports are all reports available in the report dimension or the most complete "
            "report-level output. Active reports have at least one view or usage event. "
            f"{report_adoption.get('assumption', '')}"
        ),
    )
    metric_with_help(col3, "Reports At Risk", fmt_number(len(at_risk_reports)), at_risk_help)
    metric_with_help(
        col4,
        "Forecast Reliability",
        f"{fmt_percent(reliability.get('reliable_pct'))} reliable",
        reliability_help,
    )

    warnings = (
        user_adoption.get("warnings", [])
        + report_adoption.get("warnings", [])
        + reliability.get("warnings", [])
    )
    if warnings:
        with st.expander("Overview metric assumptions"):
            for warning in warnings:
                st.caption(warning)

    st.subheader("Reports to investigate")
    if at_risk_reports.empty:
        st.success("No at-risk reports were flagged in the available analytics outputs.")
        return

    st.dataframe(at_risk_reports, hide_index=True, width="stretch")
    selected_risk_report = st.selectbox(
        "Select an at-risk report to inspect",
        at_risk_reports["report_id"].tolist(),
        format_func=lambda report_id: (
            at_risk_reports.loc[
                at_risk_reports["report_id"] == report_id, "report_name"
            ].iloc[0]
            + f" ({report_id})"
        ),
    )
    if st.button("Open selected report"):
        st.session_state["selected_report_id"] = selected_risk_report
        rerun_app()


def render_forecast_explorer(data: dict[str, pd.DataFrame], report_id: str) -> None:
    st.header("Forecast Explorer")
    forecasts = data["forecasts"]
    report_forecasts = (
        forecasts[forecasts["report_id"] == report_id] if "report_id" in forecasts else pd.DataFrame()
    )
    metric_row = row_for_report(data["metrics"], report_id)

    st.plotly_chart(usage_forecast_chart(report_forecasts), width="stretch")

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Model used", metric_row.get("selected_model", metric_row.get("model_str", "N/A")))
    col2.metric("MAE", fmt_number(metric_row.get("selected_mae"), 2))
    col3.metric("RMSE", fmt_number(metric_row.get("selected_rmse"), 2))
    col4.metric("WAPE", fmt_percent(metric_row.get("selected_wape")))
    col5.metric("Reliability", reliability_label(metric_row))


def render_behaviour_insights(data: dict[str, pd.DataFrame], report_id: str) -> None:
    st.header("Behaviour Insights")
    feature_row = row_for_report(data["report_features"], report_id)
    segment_row = row_for_report(data["segments"], report_id)
    diagnostic_row = row_for_report(data["diagnostics"], report_id)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Report segment", segment_row.get("report_segment", "N/A"))
    col2.metric("Repeat rate", fmt_percent(feature_row.get("repeat_rate")))
    col3.metric("Top-user concentration", fmt_percent(feature_row.get("top_user_concentration")))
    col4.metric("Days active", fmt_number(feature_row.get("days_active")))

    if not segment_row.empty and pd.notna(segment_row.get("segment_reason")):
        st.info(segment_row.get("segment_reason"))

    left, right = st.columns([1, 2])
    with left:
        st.subheader("Diagnostic flags")
        flags = diagnostics_flags(diagnostic_row)
        if flags.empty:
            st.caption("No diagnostics available for this report.")
        else:
            st.dataframe(flags, hide_index=True, width="stretch")
    with right:
        st.subheader("Diagnostic summary")
        st.write(diagnostic_row.get("diagnostic_summary", "No diagnostic summary available."))


def render_ai_insights(data: dict[str, pd.DataFrame], report_id: str) -> None:
    st.header("AI Insights")
    insight_row = row_for_report(data["insights"], report_id)
    if insight_row.empty:
        st.caption("No AI insight file found for this report.")
        return

    col1, col2 = st.columns([3, 1])
    with col1:
        st.subheader("Forecast summary")
        st.write(insight_row.get("forecast_summary", "No forecast summary available."))
    with col2:
        st.metric("Confidence", str(insight_row.get("confidence", "N/A")).title())

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Hypotheses")
        list_items(insight_row.get("hypotheses"))
    with col2:
        st.subheader("Recommendations")
        list_items(insight_row.get("recommended_actions"))


def main() -> None:
    data = load_app_data()
    reports = available_reports(data)

    st.title("Power BI Report Usage Forecasts")
    st.caption("A lightweight reviewer app for forecasts, metrics, segments, diagnostics, and GenAI insights.")

    if reports.empty:
        st.warning("No report outputs were found. Run the forecasting and analytics notebooks or pipelines first.")
        return

    if "selected_report_id" not in st.session_state:
        st.session_state["selected_report_id"] = reports.iloc[0]["report_id"]

    matching_index = reports.index[reports["report_id"] == st.session_state["selected_report_id"]]
    selected_index = int(matching_index[0]) if len(matching_index) else 0

    selected_label = st.sidebar.selectbox(
        "Select report",
        reports["label"].tolist(),
        index=selected_index,
    )
    selected_report = reports.loc[reports["label"] == selected_label].iloc[0]
    report_id = selected_report["report_id"]
    st.session_state["selected_report_id"] = report_id

    st.sidebar.markdown("### Report")
    st.sidebar.write(selected_report["report_name"])
    st.sidebar.caption(report_id)

    tabs = st.tabs(["Overview", "Forecast Explorer", "Behaviour Insights", "AI Insights"])
    with tabs[0]:
        render_overview(data)
    with tabs[1]:
        render_forecast_explorer(data, report_id)
    with tabs[2]:
        render_behaviour_insights(data, report_id)
    with tabs[3]:
        render_ai_insights(data, report_id)


if __name__ == "__main__":
    main()
