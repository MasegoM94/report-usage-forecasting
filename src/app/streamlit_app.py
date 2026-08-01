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
    first_existing_column,
    get_dashboard_analysis_period,
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


def fmt_date(value: Any) -> str:
    """Format a date for the Overview analysis period."""
    if pd.isna(value):
        return "Not Available"
    try:
        return pd.to_datetime(value).strftime("%d %b %Y")
    except (TypeError, ValueError):
        return "Not Available"


def first_row_value(row: pd.Series, candidates: list[str], default: Any = "N/A") -> Any:
    """Return the first non-empty value from a row using flexible column names."""
    if row.empty:
        return default
    for column in candidates:
        if column in row.index and pd.notna(row.get(column)):
            return row.get(column)
    return default


def fmt_analysis_period(period: dict[str, Any]) -> str:
    """Format the selected analysis period for display."""
    start_date = period.get("start_date")
    end_date = period.get("end_date")
    if start_date is None or end_date is None:
        return "Not Available"
    return f"{fmt_date(start_date)} - {fmt_date(end_date)}"


def metric_with_help(container: Any, label: str, value: Any, help_text: str) -> None:
    """Render st.metric with a caption fallback for older Streamlit versions."""
    try:
        container.metric(label, value, help=help_text)
    except TypeError:
        container.metric(label, value)
        container.caption(help_text)


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
    reliable = first_row_value(
        row,
        [
            "forecast_reliable",
            "forecast_reliability",
            "reliability",
            "reliability_label",
            "publish_flag",
        ],
        default=None,
    )
    if pd.notna(reliable):
        if isinstance(reliable, str):
            label = reliable.strip()
            lowered = label.lower()
            if lowered in {"reliable", "high"}:
                return "Reliable"
            if lowered in {"medium", "caution", "review"}:
                return "Caution"
            if lowered in {"low", "not reliable", "not_reliable", "at risk", "false", "0", "no"}:
                return "Not reliable"
            return label
        return "Reliable" if truthy(reliable) else "At risk"
    wape = first_row_value(row, ["selected_wape", "wape", "WAPE"], default=None)
    if pd.notna(wape):
        return "Reliable" if float(wape) <= 0.30 else "Review"
    return "Unknown"


def selected_report_display_name(
    data: dict[str, pd.DataFrame], report_id: str, selected_report_name: Any = None
) -> str:
    """Choose a friendly report name, falling back to the report id."""
    if pd.notna(selected_report_name) and str(selected_report_name).strip():
        return str(selected_report_name)

    for source in ["metrics", "forecasts", "report_features", "segments", "diagnostics", "dim_report"]:
        df = data.get(source, pd.DataFrame())
        if df.empty or "report_id" not in df.columns:
            continue
        name_col = first_existing_column(df, ["report_name", "ReportName"])
        if not name_col:
            continue
        match = df.loc[df["report_id"] == report_id, name_col].dropna()
        if not match.empty and str(match.iloc[0]).strip():
            return str(match.iloc[0])
    return str(report_id)


def forecast_rows_only(report_forecasts: pd.DataFrame) -> pd.DataFrame:
    """Return rows that represent future forecast dates where possible."""
    if report_forecasts.empty:
        return report_forecasts

    if "IsForecast" in report_forecasts.columns:
        forecast_mask = report_forecasts["IsForecast"].astype(str).str.strip().str.lower().isin(
            {"true", "1", "yes", "y"}
        )
        return report_forecasts.loc[forecast_mask]

    if "actual" in report_forecasts.columns:
        return report_forecasts.loc[report_forecasts["actual"].isna()]

    if "forecast" in report_forecasts.columns:
        return report_forecasts.loc[report_forecasts["forecast"].notna()]

    return report_forecasts


def get_forecast_horizon_text(report_forecasts: pd.DataFrame) -> str:
    """Infer a reviewer-friendly forecast horizon from the selected report output."""
    forecast_rows = forecast_rows_only(report_forecasts)
    if forecast_rows.empty:
        return "the selected forecast horizon"

    horizon_col = first_existing_column(
        forecast_rows,
        ["horizon", "forecast_horizon", "forecast_horizon_days", "Horizon"],
    )
    if horizon_col:
        horizon_values = pd.to_numeric(forecast_rows[horizon_col], errors="coerce").dropna()
        if not horizon_values.empty:
            days = int(horizon_values.max())
            return f"next {days} {'day' if days == 1 else 'days'}"

    date_col = first_existing_column(
        forecast_rows,
        ["date", "Date", "ds", "forecast_date", "ForecastDate", "prediction_date"],
    )
    if not date_col:
        return "the selected forecast horizon"

    forecast_dates = pd.to_datetime(forecast_rows[date_col], errors="coerce").dropna()
    if forecast_dates.empty:
        return "the selected forecast horizon"

    days = int(forecast_dates.dt.normalize().nunique())
    return f"next {days} {'day' if days == 1 else 'days'}"


def get_forecast_window_text(report_forecasts: pd.DataFrame) -> str:
    """Return the selected report's forecast date window when dates are available."""
    forecast_rows = forecast_rows_only(report_forecasts)
    if forecast_rows.empty:
        return "Not available"

    date_col = first_existing_column(
        forecast_rows,
        ["date", "Date", "ds", "forecast_date", "ForecastDate", "prediction_date"],
    )
    if not date_col:
        return "Not available"

    forecast_dates = pd.to_datetime(forecast_rows[date_col], errors="coerce").dropna()
    if forecast_dates.empty:
        return "Not available"

    start_date = forecast_dates.min().strftime("%d %b %Y")
    end_date = forecast_dates.max().strftime("%d %b %Y")
    return f"{start_date} - {end_date}"


def friendly_model_name(raw_model: Any, model_detail: Any = None) -> str:
    """Map technical model metadata to a business-facing model name."""
    raw = "" if pd.isna(raw_model) else str(raw_model).strip()
    detail = "" if pd.isna(model_detail) else str(model_detail).strip()
    normalized = raw.lower().replace("-", "_").replace(" ", "_")
    detail_normalized = detail.lower().replace("-", "_").replace(" ", "_")

    if normalized in {"naive", "baseline_naive"}:
        return "Naive"
    if normalized in {"seasonal_naive", "seasonalnaive", "snaive"}:
        return "Seasonal Naive"
    if normalized in {"moving_average", "movingaverage", "rolling_average"}:
        return "Moving Average"
    if normalized in {"sarimax"} or "sarimax" in detail_normalized:
        return "SARIMAX"
    if normalized in {"sarima", "seasonal_arima"}:
        return "SARIMA"
    if normalized in {"arima"}:
        return "ARIMA"
    if normalized in {"arma"}:
        return "ARMA"
    if normalized in {"ma", "moving_average_model"}:
        return "MA"
    if normalized in {"ar"}:
        return "AR"
    if normalized in {"auto_arima", "pmdarima", "auto_arima_model"}:
        return "Auto-selected ARIMA/SARIMA family model"

    if "seasonal_arima" in detail_normalized or "x(" in detail_normalized:
        return "SARIMA"
    if "arima" in detail_normalized:
        return "ARIMA"
    if "arma" in detail_normalized:
        return "ARMA"
    if raw:
        return raw.replace("_", " ").title()
    return "N/A"


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
                    "Status": "Flagged" if truthy(row.get(column)) else "Clear",
                }
            )
    return pd.DataFrame(rows)


def _mart_report_counts(mart: "pd.DataFrame") -> dict[str, Any]:
    """Derive simple report counts from the canonical mart when it is available.

    Returns a dict with total / active / action counts or None values when the
    mart is empty or missing the required columns.  All calculations are simple
    row counts and category distributions — no threshold logic is duplicated here.
    """
    if mart.empty or "report_id" not in mart.columns:
        return {}
    total = int(mart["report_id"].nunique())
    out: dict[str, Any] = {"total_reports": total}
    if "overall_report_status" in mart.columns:
        status_counts = mart["overall_report_status"].value_counts().to_dict()
        out["status_counts"] = status_counts
    if "overall_review_priority" in mart.columns:
        priority_counts = mart["overall_review_priority"].value_counts().to_dict()
        out["priority_counts"] = priority_counts
        # Reports flagged as high or critical review priority
        high_priority = mart[
            mart["overall_review_priority"].isin({"high", "critical"})
        ]
        out["high_priority_count"] = len(high_priority)
    if "recommended_report_action" in mart.columns:
        action_counts = mart["recommended_report_action"].value_counts().to_dict()
        out["action_counts"] = action_counts
    if "forecast_outlook_status" in mart.columns:
        out["forecast_outlook_counts"] = mart["forecast_outlook_status"].value_counts().to_dict()
    if "analytics_run_id" in mart.columns and not mart.empty:
        out["analytics_run_id"] = mart["analytics_run_id"].iloc[0]
    if "analytics_as_of_date" in mart.columns and not mart.empty:
        out["analytics_as_of_date"] = mart["analytics_as_of_date"].iloc[0]
    return out


def render_overview(data: dict[str, pd.DataFrame]) -> None:
    st.header("Overview")
    analysis_period = get_dashboard_analysis_period(data)
    mart = data.get("report_analytics", pd.DataFrame())
    mart_counts = _mart_report_counts(mart)
    user_adoption = calculate_user_adoption_metrics(data)
    report_adoption = calculate_report_adoption_metrics(data)
    reliability = calculate_forecast_reliability_summary(data)
    at_risk_reports = get_at_risk_reports(data)

    at_risk_help = (
        "Reports at risk are reports showing signs of declining usage, weak repeat engagement, "
        "high reliance on a small group of users, performance issues, or unreliable forecasting "
        "patterns."
    )
    reliability_help = (
        "Forecast reliability shows the share of report forecasts that passed the project's "
        "quality checks. It does not mean the model is that percentage accurate."
    )

    st.subheader("User Adoption")
    user_cols = st.columns(3)
    metric_with_help(
        user_cols[0],
        "Total Users",
        fmt_number(user_adoption.get("total_users")),
        "The number of users available in the usage dataset.",
    )
    metric_with_help(
        user_cols[1],
        "Active Users",
        fmt_number(user_adoption.get("active_users")),
        "The number of users who viewed at least one report during the analysis period.",
    )
    metric_with_help(
        user_cols[2],
        "Active User Rate",
        fmt_percent(user_adoption.get("active_user_rate")),
        "The percentage of available users who actively viewed at least one report.",
    )

    st.subheader("Report Adoption")
    # Prefer mart-derived counts when the canonical mart is loaded.
    mart_total = mart_counts.get("total_reports")
    display_total = mart_total if mart_total is not None else report_adoption.get("total_reports")
    report_cols = st.columns(3)
    metric_with_help(
        report_cols[0],
        "Total Reports",
        fmt_number(display_total),
        "The number of reports in the canonical analytics mart.",
    )
    metric_with_help(
        report_cols[1],
        "Used Reports",
        fmt_number(report_adoption.get("used_reports")),
        "The number of reports that received at least one view during the analysis period.",
    )
    metric_with_help(
        report_cols[2],
        "Report Usage Rate",
        fmt_percent(report_adoption.get("report_usage_rate")),
        "The percentage of available reports that were viewed at least once.",
    )

    # Analytics freshness — show run ID and as-of date when the mart is loaded
    if mart_counts.get("analytics_as_of_date"):
        st.caption(
            f"Analytics as of: **{mart_counts['analytics_as_of_date']}** "
            f"(run: {mart_counts.get('analytics_run_id', 'unknown')})"
        )

    st.subheader("Monitoring")
    monitoring_cols = st.columns(2)
    metric_with_help(
        monitoring_cols[0],
        "Reports At Risk",
        fmt_number(len(at_risk_reports)),
        at_risk_help,
    )
    metric_with_help(
        monitoring_cols[1],
        "Forecast Reliability",
        fmt_percent(reliability.get("reliable_pct")),
        reliability_help,
    )

    warnings = (
        analysis_period.get("warnings", [])
        + user_adoption.get("warnings", [])
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

    display = at_risk_reports.rename(
        columns={
            "report_name": "Report Name",
            "segment_or_health_status": "Segment / Status",
            "diagnostic": "Diagnostic Summary",
            "forecast_reliability": "Forecast Reliability",
        }
    )
    visible_columns = [
        "Report Name",
        "Segment / Status",
        "Diagnostic Summary",
        "Forecast Reliability",
    ]
    st.dataframe(display[visible_columns], hide_index=True, width="stretch")


def render_forecast_explorer(
    data: dict[str, pd.DataFrame], report_id: str, selected_report_name: Any = None
) -> None:
    st.header("Forecast Explorer")
    forecasts = data["forecasts"]
    report_forecasts = (
        forecasts[forecasts["report_id"] == report_id] if "report_id" in forecasts else pd.DataFrame()
    )
    metric_row = row_for_report(data["metrics"], report_id)

    report_display_name = selected_report_display_name(data, report_id, selected_report_name)
    forecast_horizon = get_forecast_horizon_text(report_forecasts)
    forecast_window = get_forecast_window_text(report_forecasts)
    st.subheader(f"Now viewing forecast for: {report_display_name}")
    forecast_period = (
        f"the {forecast_horizon}"
        if forecast_horizon.startswith("next ")
        else forecast_horizon
    )
    st.caption(
        f"This forecast estimates expected report views over {forecast_period} based on recent "
        "historical usage patterns."
    )
    if forecast_window != "Not available":
        st.caption(f"Forecast window: {forecast_window}")

    st.plotly_chart(usage_forecast_chart(report_forecasts), width="stretch")

    model_help = (
        "The model shown here is the selected forecasting model for this report. Automated model "
        "selection may be used to choose the most suitable time-series structure, but the resulting "
        "model is typically a statistical forecasting model such as AR, MA, ARMA, ARIMA, SARIMA, "
        "or SARIMAX depending on the usage pattern and available history."
    )
    mae_help = (
        "Mean Absolute Error shows the average difference between actual report views and predicted "
        "report views. Lower is better. In business terms, a lower MAE means the forecast is "
        "usually closer to the real usage count."
    )
    rmse_help = (
        "Root Mean Squared Error measures forecast error while giving larger mistakes more weight. "
        "Lower is better. In business terms, a high RMSE means the model sometimes makes large "
        "forecasting errors."
    )
    wape_help = (
        "Weighted Absolute Percentage Error shows total forecast error as a percentage of total "
        "actual usage. Lower is better. In business terms, it helps compare forecast quality "
        "across reports with different usage volumes."
    )
    reliability_help = (
        "Forecast reliability indicates whether the report has enough usable history and acceptable "
        "forecast error to support a short-term forecast. It is a quality flag, not a guarantee."
    )

    model_used = first_row_value(
        metric_row,
        ["selected_model", "model", "model_used", "best_model", "model_name", "ModelName"],
    )
    model_detail = first_row_value(metric_row, ["model_str", "model_details", "model_spec"], default=None)
    display_model = friendly_model_name(model_used, model_detail)
    mae = first_row_value(metric_row, ["selected_mae", "mae", "MAE"], default=None)
    rmse = first_row_value(metric_row, ["selected_rmse", "rmse", "RMSE"], default=None)
    wape = first_row_value(metric_row, ["selected_wape", "wape", "WAPE"], default=None)
    reliability = reliability_label(metric_row)

    col1, col2, col3, col4, col5 = st.columns(5)
    metric_with_help(col1, "Model Used", display_model, model_help)
    metric_with_help(col2, "MAE", fmt_number(mae, 2), mae_help)
    metric_with_help(col3, "RMSE", fmt_number(rmse, 2), rmse_help)
    metric_with_help(col4, "WAPE", fmt_percent(wape), wape_help)
    metric_with_help(col5, "Forecast Reliability", reliability, reliability_help)

    if metric_row.empty:
        st.warning("Forecast metric details are not available for this report.")

    with st.expander("Model definitions"):
        st.write(model_help)
        st.markdown("- **AR:** Uses previous usage values to forecast future usage.")
        st.markdown("- **MA:** Uses previous forecast errors to improve future predictions.")
        st.markdown(
            "- **ARMA:** Combines previous usage values and previous forecast errors."
        )
        st.markdown("- **ARIMA:** Extends ARMA by handling trends in the data.")
        st.markdown(
            "- **SARIMA:** Extends ARIMA by adding repeating seasonal patterns, such as weekly "
            "usage behaviour."
        )
        st.markdown(
            "- **SARIMAX:** Extends SARIMA by allowing additional external variables if included."
        )
        st.markdown("- **Naive:** Uses the most recent observed usage value as the forecast.")
        st.markdown(
            "- **Seasonal Naive:** Uses the value from the same point in the previous season, "
            "such as the same day last week."
        )
        st.markdown(
            "- **Moving Average:** Uses recent average usage to smooth short-term fluctuations."
        )
        st.caption(
            "The aim is not to use the most complex model, but to select a model that gives a "
            "reasonable short-term forecast for each report based on the quality and volume of "
            "usage history."
        )

    with st.expander("Forecast reliability guide"):
        st.write(reliability_help)
        st.markdown(
            "- **Reliable / High:** The forecast passed the project's quality checks and can be "
            "interpreted with more confidence."
        )
        st.markdown(
            "- **Medium / Caution:** The forecast may be useful directionally but should be "
            "interpreted carefully."
        )
        st.markdown(
            "- **Low / Not reliable:** The report has limited, sparse, volatile, or poorly "
            "performing forecast results, so the forecast should not be used as a strong signal."
        )


def _suppressed_or(value: Any, suppressed: bool, label: str = "Suppressed (privacy)") -> str:
    """Return *label* when *suppressed* is true, otherwise format *value*."""
    if suppressed:
        return label
    return fmt_percent(value)


def _engagement_evidence_note(eng_row: "pd.Series[Any]") -> str | None:
    """Return a human-readable explanation when engagement evidence is limited."""
    if eng_row.empty:
        return "No engagement data is available for this report."
    status = eng_row.get("engagement_evidence_status", "")
    suppression = eng_row.get("privacy_suppression_status", "not_suppressed")
    if suppression not in ("not_suppressed", None, ""):
        field_count = eng_row.get("privacy_suppressed_field_count", "")
        return (
            f"Some engagement fields are privacy-suppressed ({field_count} field(s)). "
            "Suppressed values are not shown to protect user privacy."
        )
    if str(status).startswith("incomplete") or str(status) == "insufficient":
        missing = eng_row.get("missing_engagement_evidence", "")
        return f"Engagement evidence is incomplete: {missing}." if missing else "Engagement evidence is incomplete."
    return None


def render_behaviour_insights(
    data: dict[str, pd.DataFrame],
    report_id: str,
    selected_report_name: Any = None,
    total_days: int | None = None,
) -> None:
    st.header("Behaviour Insights")
    report_display_name = selected_report_display_name(data, report_id, selected_report_name)
    st.subheader(f"Now viewing behaviour insights for: {report_display_name}")
    segment_row = row_for_report(data["segments"], report_id)
    diagnostic_row = row_for_report(data["diagnostics"], report_id)

    # Engagement fields come from the engagement mart (report-level aggregates only).
    # Fields that were previously read from report_features (repeat_rate,
    # top_1_user_view_share, days_active) were removed in the v2 schema migration
    # and now live in mart_report_engagement.csv under updated names.
    eng_row = row_for_report(data.get("engagement", pd.DataFrame()), report_id)

    def _is_suppressed(field: str) -> bool:
        """True when the engagement mart explicitly marks this field as suppressed."""
        if eng_row.empty:
            return False
        fields_str = str(eng_row.get("privacy_suppressed_fields", "") or "")
        return field in fields_str

    # Active days (active_usage_days_28d: distinct days with ≥1 view in last 28 days)
    days_active_raw = eng_row.get("active_usage_days_28d") if not eng_row.empty else None
    activity_suppressed = _is_suppressed("activity") or (
        not eng_row.empty and truthy(eng_row.get("activity_privacy_suppressed"))
    )
    if activity_suppressed:
        days_active_display = "Suppressed (privacy)"
    elif pd.notna(days_active_raw) and total_days is not None:
        days_active_display = f"{fmt_number(days_active_raw)} / {total_days} days"
    else:
        days_active_display = fmt_number(days_active_raw)

    # Returning-user share (repeat_view_user_share_28d: share of users who viewed >1 time)
    cohort_suppressed = _is_suppressed("cohort") or (
        not eng_row.empty and truthy(eng_row.get("cohort_privacy_suppressed"))
    )
    returning_share_raw = eng_row.get("returning_user_share_28d") if not eng_row.empty else None
    returning_share_display = _suppressed_or(returning_share_raw, cohort_suppressed)

    # Top-user concentration (top_1_user_view_share_28d: share of views by single top user)
    concentration_suppressed = _is_suppressed("concentration") or (
        not eng_row.empty and truthy(eng_row.get("concentration_privacy_suppressed"))
    )
    top_user_raw = eng_row.get("top_1_user_view_share_28d") if not eng_row.empty else None
    top_user_display = _suppressed_or(top_user_raw, concentration_suppressed)

    col1, col2, col3, col4 = st.columns(4)
    metric_with_help(
        col1,
        "Report segment",
        segment_row.get("report_segment", "N/A"),
        "A category that describes how this report is being used overall. "
        "It helps you quickly understand where a report sits in its usage lifecycle.\n\n"
        "Possible segments:\n\n"
        "High Value — the report has strong viewership and a broad, active audience.\n\n"
        "Niche — the report has a loyal or specialist audience, often with healthy repeat visits or concentrated usage.\n\n"
        "At Risk — usage is declining and the report shows signs of low engagement or performance issues.\n\n"
        "Inactive — the report has had little to no recent activity.",
    )
    metric_with_help(
        col2,
        "Returning users (28d)",
        returning_share_display,
        "The share of active users in the last 28 days who had previously viewed this report "
        "(returning, not first-time visitors). "
        "A high share suggests the report is genuinely useful and people keep coming back. "
        "A low share may mean most users visited only once.\n\n"
        "Shown as 'Suppressed (privacy)' when the user population is too small to share safely.",
    )
    metric_with_help(
        col3,
        "Top user share (28d)",
        top_user_display,
        "The share of total report views in the last 28 days attributable to the single most "
        "active user. A high value means one person drives most of the traffic, which is a "
        "dependency risk if that user changes roles or leaves. A lower value suggests broader, "
        "more resilient usage.\n\n"
        "Shown as 'Suppressed (privacy)' when the user population is too small to share safely.",
    )
    metric_with_help(
        col4,
        "Active days (28d)",
        days_active_display,
        "The number of distinct days on which this report received at least one view during "
        "the last 28 days. More active days generally indicate a report that is regularly "
        "consulted rather than viewed in occasional bursts.\n\n"
        "Shown as 'Suppressed (privacy)' when activity data cannot be shared safely.",
    )

    # Surface evidence or suppression limitations above the diagnostics section
    evidence_note = _engagement_evidence_note(eng_row)
    if evidence_note:
        st.info(evidence_note)

    st.subheader("Diagnostic summary")
    st.write(diagnostic_row.get("diagnostic_summary", "No diagnostic summary available."))

    st.subheader("Diagnostic flags")
    flags = diagnostics_flags(diagnostic_row)
    if flags.empty:
        st.caption("No diagnostics available for this report.")
    else:
        st.dataframe(flags, hide_index=True, width="stretch")

    with st.expander("Diagnostic flag definitions"):
        st.markdown(
            "**Performance Issue** — The report is loading slowly compared to other reports "
            "and its usage is also declining. Slow load times can discourage users from returning, "
            "so this combination is worth investigating."
        )
        st.markdown(
            "**Engagement Issue** — Users are not returning to this report as frequently as "
            "they do for others, and overall usage is declining. This may suggest the report "
            "is not meeting its audience's needs or has become less relevant over time."
        )
        st.markdown(
            "**Dependency Risk** — A very large share of the report's views comes from a "
            "small number of users. If those users change roles or leave, usage could drop "
            "significantly. Broader adoption would reduce this risk."
        )
        st.markdown(
            "**Inactive Risk** — The report has had little to no recent activity. It may "
            "have been replaced by another report, fallen out of use, or no longer be relevant "
            "to its original audience."
        )


def render_ai_insights(
    data: dict[str, pd.DataFrame], report_id: str, selected_report_name: Any = None
) -> None:
    st.header("AI Insights")
    report_display_name = selected_report_display_name(data, report_id, selected_report_name)
    st.subheader(f"Now viewing AI insights for: {report_display_name}")
    insight_row = row_for_report(data["insights"], report_id)
    if insight_row.empty:
        st.caption("No AI insight file found for this report.")
        return

    st.subheader("At a glance")
    col1, col2 = st.columns([3, 1])
    with col1:
        st.write(insight_row.get("forecast_summary", "No summary available."))
    with col2:
        metric_with_help(
            col2,
            "Confidence",
            str(insight_row.get("confidence", "N/A")).title(),
            "How much weight to place on the insight for this report. "
            "This is not a measure of forecast accuracy — it reflects how well-supported "
            "the insight is based on the available data.\n\n"
            "High — the report has reliable forecast data and clear usage signals to draw from.\n\n"
            "Medium — the forecast passed quality checks but some signals are limited or mixed.\n\n"
            "Low — the forecast did not pass reliability checks or the report has limited history, "
            "so the insight should be treated as indicative only.",
        )

    st.divider()

    st.subheader("Recommendations")
    st.caption("Suggested actions based on the usage patterns and forecast for this report.")
    list_items(insight_row.get("recommended_actions"))

    st.divider()

    with st.expander("Hypotheses"):
        st.caption("Possible explanations for the usage patterns observed in this report.")
        list_items(insight_row.get("hypotheses"))


def main() -> None:
    data = load_app_data()
    reports = available_reports(data)
    analysis_period = get_dashboard_analysis_period(data)
    st.title("Power BI Report Usage Forecasts")
    st.caption("This dashboard explores how report usage changes over time using a synthetic Power BI-style usage dataset. The analysis focuses on understanding report adoption, user engagement, and usage trends across the historical analysis period, while identifying reports that may be at risk of declining usage or disengagement.")
    st.markdown(f"**Analysis Period:** {fmt_analysis_period(analysis_period)}")
    st.caption(
        "The metrics, forecasts, diagnostics, and insights shown in this dashboard are based "
        "on activity observed during this analysis period."
    )
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
        render_forecast_explorer(data, report_id, selected_report.get("report_name"))
    with tabs[2]:
        start_date = analysis_period.get("start_date")
        end_date = analysis_period.get("end_date")
        total_days = (
            int((end_date - start_date).days) + 1
            if start_date is not None and end_date is not None
            else None
        )
        render_behaviour_insights(data, report_id, selected_report.get("report_name"), total_days)
    with tabs[3]:
        render_ai_insights(data, report_id, selected_report.get("report_name"))


if __name__ == "__main__":
    main()
