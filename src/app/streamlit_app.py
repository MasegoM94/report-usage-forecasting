"""Streamlit reviewer app for Power BI report usage forecasting outputs."""

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
from utils.portfolio_helpers import (
    attention_shortlist as _attention_shortlist,
    distribution_table as _distribution_table,
    portfolio_headline_metrics as _portfolio_headline_metrics,
    status_label as _status_label,
    STATUS_ORDER as _STATUS_ORDER,
)
from utils.report_helpers import (
    GENAI_STATE_LABELS,
    REPORT_GENAI_FIELDS,
    build_report_detail,
    classify_genai_state,
    fmt_pct_change,
    get_genai_field,
    is_field_suppressed,
    parse_report_reasons,
    report_display_name as _report_display_name,
    suppression_aware_metric,
)
from utils.definitions import DEFINITIONS, status_label as _def_status_label
from utils.filter_helpers import (
    FILTERABLE_FIELDS,
    active_filter_summary,
    apply_attention_filter,
    apply_filters,
    check_filter_availability,
    default_filter_state,
    extract_filter_options,
    safe_session_report,
    search_reports,
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


# ---------------------------------------------------------------------------
# Portfolio GenAI section helpers (pure-logic helpers imported from portfolio_helpers)
# ---------------------------------------------------------------------------

def _render_portfolio_genai(insight: dict[str, Any], status: str) -> None:
    """Render the persisted portfolio AI insight section."""
    st.subheader("Portfolio AI Summary")

    generation_status = insight.get("generation_status", "")
    validation_status = insight.get("validation_status", "")

    if status == "absent":
        st.info("No portfolio AI summary is available. Run the GenAI pipeline to generate one.")
        return

    if status == "empty":
        st.info("The portfolio AI summary file is empty.")
        return

    if status in ("malformed_json", "unexpected_structure"):
        st.warning("The portfolio AI summary file could not be read. The file may be corrupted.")
        return

    if status == "validation_failed":
        st.warning(
            "The portfolio AI summary did not pass validation checks. "
            "The narrative below may not be reliable — review the pipeline output."
        )

    # Generation-status label shown at the top of the section
    if generation_status == "rule_based":
        st.caption(
            "ℹ️ This summary was produced by the deterministic rule-based fallback — "
            "not by a live language model. It reflects validated portfolio aggregates."
        )
    elif generation_status in ("fallback_schema_invalid", "fallback_api_error"):
        st.caption(
            "⚠️ The AI model call failed. This is a fallback summary based on validated aggregates."
        )

    # Narrative sections
    executive = insight.get("executive_summary")
    if executive:
        st.markdown(f"**{executive}**")

    col_left, col_right = st.columns(2)
    with col_left:
        _render_insight_section("Usage", insight.get("portfolio_usage_summary"))
        _render_insight_section("Forecast", insight.get("portfolio_forecast_summary"))
        _render_insight_section("Model health", insight.get("portfolio_model_health_summary"))
    with col_right:
        _render_insight_section("Engagement", insight.get("portfolio_engagement_summary"))
        _render_list_section("Priority actions", insight.get("priority_actions"))
        _render_list_section("Positive signals", insight.get("positive_signals"))

    limitations = insight.get("evidence_limitations")
    if limitations:
        with st.expander("Evidence limitations"):
            if isinstance(limitations, list):
                for item in limitations:
                    st.markdown(f"- {item}")
            else:
                st.write(limitations)

    # Lineage metadata in an expander
    with st.expander("Summary metadata"):
        meta_cols = st.columns(2)
        meta = {
            "Analytics as of":   insight.get("analytics_as_of_date"),
            "Report count":      insight.get("report_count"),
            "Generation status": generation_status,
            "Validation status": validation_status,
            "Prompt version":    insight.get("prompt_version"),
            "Model":             insight.get("model_name"),
            "Generated at":      str(insight.get("generated_at", ""))[:19],
        }
        items = list(meta.items())
        for i, (label, value) in enumerate(items):
            meta_cols[i % 2].caption(f"**{label}:** {value or '—'}")


def _render_insight_section(label: str, value: Any) -> None:
    if value:
        st.markdown(f"**{label}**")
        st.write(value)


def _render_list_section(label: str, value: Any) -> None:
    if not value:
        return
    st.markdown(f"**{label}**")
    if isinstance(value, list):
        for item in value:
            st.markdown(f"- {item}")
    else:
        st.write(value)


# ---------------------------------------------------------------------------
# Sidebar — search, filters, report selector
# ---------------------------------------------------------------------------

# Session-state key prefix for all filter widgets.
# Incrementing _SF_V forces Streamlit to recreate the widgets (clear effect).
_SF_PFX = "sf_"


def _sf_key(name: str) -> str:
    """Sidebar filter key using the current filter version."""
    v = st.session_state.get("_sf_v", 0)
    return f"{_SF_PFX}{name}_{v}"


def _clear_sidebar_filters() -> None:
    """Increment the filter version so all filter widgets reset on next rerun."""
    st.session_state["_sf_v"] = st.session_state.get("_sf_v", 0) + 1


def render_sidebar(
    data: dict[str, pd.DataFrame],
    reports: pd.DataFrame,
) -> dict[str, Any]:
    """Render the full sidebar and return the selection state.

    Returns a dict with keys:
        report_id          — currently selected report_id (str or None)
        filtered_mart      — mart filtered by active portfolio filters
        selectable_reports — DataFrame of reports visible after search+filter
        active_filters     — dict of {field: [values]}
        search_query       — current search string
        attention_only     — bool
        total_count        — total reports in mart before filtering
        filtered_count     — reports after filters (before search)
    """
    mart = data.get("report_analytics", pd.DataFrame())
    total_count = len(mart) if not mart.empty else 0

    # ── Search ────────────────────────────────────────────────────────────
    st.sidebar.markdown("**Find a report**")
    search_query: str = st.sidebar.text_input(
        "Search by name or ID",
        key=_sf_key("search"),
        placeholder="Type to filter…",
        label_visibility="collapsed",
    )

    # ── Portfolio filters ─────────────────────────────────────────────────
    availability = check_filter_availability(mart)
    active_filters: dict[str, list[str]] = {}

    usable_fields = [
        (field, label)
        for field, label in FILTERABLE_FIELDS
        if availability.get(field, False)
    ]

    if usable_fields:
        with st.sidebar.expander("Portfolio filters", expanded=False):
            for field, label in usable_fields:
                options_pairs = extract_filter_options(mart, field)
                if len(options_pairs) < 2:
                    continue
                raw_values = [v for _, v in options_pairs]
                selected = st.multiselect(
                    label,
                    options=raw_values,
                    format_func=_status_label,
                    key=_sf_key(field),
                    default=[],
                )
                if selected:
                    active_filters[field] = selected

    # ── Display controls ─────────────────────────────────────────────────
    attention_only: bool = st.sidebar.checkbox(
        "Show only reports requiring attention",
        key=_sf_key("attention"),
        value=False,
        help=(
            "Requires attention: review priority is high or critical, "
            "or recommended action is not 'continue monitoring'.\n\n"
            + DEFINITIONS["review_priority"]
        ),
    )

    any_active = bool(active_filters) or bool(search_query.strip()) or attention_only
    if any_active:
        if st.sidebar.button("Clear all filters", key="sf_clear"):
            _clear_sidebar_filters()
            st.rerun()

    # ── Apply portfolio filters ───────────────────────────────────────────
    filtered_mart = apply_filters(mart, active_filters)
    if attention_only:
        filtered_mart = apply_attention_filter(filtered_mart)

    filtered_count = len(filtered_mart)

    # ── Build selectable report list ──────────────────────────────────────
    if not filtered_mart.empty and "report_id" in filtered_mart.columns:
        filtered_ids = set(filtered_mart["report_id"].tolist())
        selectable = reports[reports["report_id"].isin(filtered_ids)].copy()
    else:
        selectable = reports.iloc[:0].copy()

    # Apply text search
    if search_query.strip():
        selectable = search_reports(selectable, search_query.strip())

    # ── Filter context caption ────────────────────────────────────────────
    if not mart.empty:
        n_sel = len(selectable)
        if any_active or n_sel < total_count:
            st.sidebar.caption(f"Showing {n_sel} of {total_count} reports")
        else:
            st.sidebar.caption(f"All {total_count} reports shown")

    # ── Report selector ───────────────────────────────────────────────────
    st.sidebar.divider()

    if selectable.empty:
        st.sidebar.warning(
            "No reports match the current search or filters. "
            "Clear filters to restore the full list."
        )
        report_id = None
    else:
        current_id = st.session_state.get("selected_report_id")
        report_id = safe_session_report(selectable["report_id"].tolist(), current_id)

        # Index of current selection in the visible list
        id_list = selectable["report_id"].tolist()
        label_list = selectable["label"].tolist()
        try:
            sel_index = id_list.index(report_id) if report_id else 0
        except ValueError:
            sel_index = 0

        selected_label = st.sidebar.selectbox(
            "Select report",
            label_list,
            index=sel_index,
            key="sidebar_report_selector",
        )
        # Map label back to report_id
        matched = selectable[selectable["label"] == selected_label]
        if not matched.empty:
            report_id = matched.iloc[0]["report_id"]

    st.session_state["selected_report_id"] = report_id

    # ── Selected report info ───────────────────────────────────────────────
    if report_id:
        row = selectable[selectable["report_id"] == report_id]
        if not row.empty:
            st.sidebar.markdown("**Selected report**")
            st.sidebar.write(row.iloc[0].get("report_name", report_id))
            st.sidebar.caption(f"`{report_id}`")

    # ── Active filter summary (after selector) ────────────────────────────
    filter_desc = active_filter_summary(active_filters, search_query, attention_only)
    if filter_desc:
        with st.sidebar.expander("Active filters"):
            for desc in filter_desc:
                st.caption(f"• {desc}")

    return {
        "report_id":          report_id,
        "filtered_mart":      filtered_mart,
        "selectable_reports": selectable,
        "active_filters":     active_filters,
        "search_query":       search_query,
        "attention_only":     attention_only,
        "total_count":        total_count,
        "filtered_count":     filtered_count,
    }


# ---------------------------------------------------------------------------
# Overview render
# ---------------------------------------------------------------------------

def render_overview(data: dict[str, pd.DataFrame], filtered_mart: pd.DataFrame | None = None) -> None:
    st.header("Portfolio Overview")
    mart = data.get("report_analytics", pd.DataFrame())

    # Use filtered_mart for all counts and distributions; fall back to full mart.
    display_mart = filtered_mart if filtered_mart is not None else mart
    total_count = len(mart) if not mart.empty else 0
    filtered_count = len(display_mart)
    is_filtered = filtered_mart is not None and filtered_count < total_count

    metrics = _portfolio_headline_metrics(display_mart)
    as_of = (
        (mart["analytics_as_of_date"].iloc[0] if "analytics_as_of_date" in mart.columns and not mart.empty else None)
    )

    # --- Data freshness banner ---
    if as_of:
        filter_note = f" · Showing **{filtered_count} of {total_count}** reports" if is_filtered else f" · **{total_count}** reports"
        st.caption(f"Analytics as of **{as_of}**{filter_note}")
    elif mart.empty:
        st.warning(
            "The canonical analytics mart (`outputs/analytics/mart_report_analytics.csv`) "
            "was not found. Headline metrics are unavailable. Run the analytics pipeline first."
        )

    # ── 1. Headline metrics ────────────────────────────────────────────────
    total = metrics.get("total_reports")
    with_usage = metrics.get("with_recent_usage")
    requiring = metrics.get("requiring_review")
    high_pri = metrics.get("high_priority")
    suppressed = metrics.get("privacy_suppressed")

    h_cols = st.columns(5)
    metric_with_help(
        h_cols[0], "Total reports", fmt_number(total),
        "Number of unique reports in the analytics mart.",
    )
    metric_with_help(
        h_cols[1], "Recent usage", fmt_number(with_usage),
        "Reports with at least one view in the last 28 days "
        f"(out of {fmt_number(total)} total).",
    )
    metric_with_help(
        h_cols[2], "Requiring review",
        fmt_number(requiring),
        "Reports with a recommended action other than 'continue monitoring'. "
        "Based on the deterministic recommended_report_action field from the mart.",
    )
    metric_with_help(
        h_cols[3], "High priority",
        fmt_number(high_pri),
        "Reports where overall_review_priority is high or critical.",
    )
    metric_with_help(
        h_cols[4], "Privacy suppressed",
        fmt_number(suppressed),
        "Reports where at least one engagement metric is suppressed for privacy. "
        "Suppressed fields are not shown as zero.",
    )

    st.divider()

    # ── 2. Portfolio AI summary ────────────────────────────────────────────
    portfolio_insight: dict[str, Any] = data.get("_portfolio_insight", {})  # type: ignore[assignment]
    portfolio_status: str = data.get("_portfolio_insight_status", "absent")  # type: ignore[assignment]
    if is_filtered:
        st.caption(
            "ℹ️ **Portfolio AI summary reflects the full persisted portfolio, "
            "not the current filter selection.** "
            "Filter-specific GenAI summaries are not generated."
        )
    _render_portfolio_genai(portfolio_insight, portfolio_status)

    st.divider()

    # ── 3. Attention shortlist ─────────────────────────────────────────────
    st.subheader("Attention shortlist")
    # Shortlist is always full-portfolio (deterministic, not re-ranked by filters)
    shortlist = _attention_shortlist(mart)
    if is_filtered:
        st.caption(
            "Shortlist reflects the **full portfolio** deterministic sort — "
            "not the current filter selection. Items outside the current filter are still shown.",
            help=DEFINITIONS["deterministic_shortlist"],
        )
    total_actionable = (
        int((mart["recommended_report_action"] != "continue_monitoring").sum())
        if not mart.empty and "recommended_report_action" in mart.columns
        else None
    )

    if shortlist.empty:
        if mart.empty:
            st.info("No mart data available to build the shortlist.")
        else:
            st.success("No reports require action — all are on 'continue monitoring'.")
    else:
        cap = len(shortlist)
        note = (
            f"Showing {cap} of {total_actionable} reports requiring action."
            if total_actionable and total_actionable > cap
            else f"Showing all {cap} report(s) requiring action."
        )
        st.caption(note + " This is a capped deterministic shortlist, not the full action queue.")

        # Friendly column names for display
        rename_map = {
            "report_name":              "Report",
            "overall_review_priority":  "Priority",
            "overall_report_status":    "Status",
            "primary_diagnostic":       "Primary diagnostic",
            "recommended_report_action":"Recommended action",
            "overall_evidence_status":  "Evidence",
        }
        display_cols = [c for c in rename_map if c in shortlist.columns]
        display_df = shortlist[display_cols].rename(columns=rename_map)
        # Apply human-readable labels to coded columns
        for src_col, display_col in rename_map.items():
            if display_col in display_df.columns and src_col in (
                "overall_review_priority", "overall_report_status",
                "recommended_report_action",
            ):
                display_df[display_col] = display_df[display_col].map(_status_label)
        st.dataframe(display_df, hide_index=True, use_container_width=True)

    st.divider()

    # ── 4. Status distributions ────────────────────────────────────────────
    if not display_mart.empty:
        st.subheader(
            "Portfolio distributions"
            + (f" ({filtered_count} of {total_count} reports)" if is_filtered else "")
        )

        dist_configs = [
            ("Historical usage", "historical_usage_status"),
            ("Forecast outlook", "forecast_outlook_status"),
            ("Engagement",       "overall_engagement_status"),
            ("Review priority",  "overall_review_priority"),
            ("Recommended action", "recommended_report_action"),
        ]

        row1_cols = st.columns(3)
        row2_cols = st.columns(2)
        all_cols = row1_cols + row2_cols

        for i, (label, field) in enumerate(dist_configs):
            order = _STATUS_ORDER.get(field)
            df_dist = _distribution_table(display_mart, field, order)
            with all_cols[i]:
                st.markdown(f"**{label}**")
                if df_dist.empty:
                    st.caption(f"'{field}' not available in mart.")
                else:
                    st.dataframe(df_dist, hide_index=True, use_container_width=True)

        # Model health — shown separately with an evidence-limitation note
        st.markdown("**Model health**")
        mh_dist = _distribution_table(
            display_mart, "model_diagnostic_status",
            _STATUS_ORDER.get("model_diagnostic_status"),
        )
        if mh_dist.empty:
            st.caption("'model_diagnostic_status' not available in mart.")
        else:
            st.dataframe(mh_dist, hide_index=True, use_container_width=True)
            insuff_count = int(
                (display_mart.get("model_diagnostic_status", pd.Series()) == "insufficient_evidence").sum()
            ) if "model_diagnostic_status" in display_mart.columns else 0
            if insuff_count > 0:
                st.caption(
                    f"ℹ️ {insuff_count} report(s) show 'Insufficient evidence' — this means the "
                    "model has not yet accumulated enough production run history to assess health. "
                    "It does not mean the model is unhealthy."
                )
    else:
        if display_mart.empty and not mart.empty:
            st.info("No reports match the current filters — status distributions are empty.")
        elif mart.empty:
            st.info("Status distributions are unavailable — mart not loaded.")

    # ── 5. Legacy metric fallback (shown only when mart is missing) ─────────
    if mart.empty:
        st.subheader("Fallback metrics (legacy sources)")
        user_adoption = calculate_user_adoption_metrics(data)
        report_adoption = calculate_report_adoption_metrics(data)
        reliability = calculate_forecast_reliability_summary(data)
        leg_cols = st.columns(4)
        metric_with_help(leg_cols[0], "Total Users", fmt_number(user_adoption.get("total_users")),
                         "From usage dataset.")
        metric_with_help(leg_cols[1], "Active Users", fmt_number(user_adoption.get("active_users")),
                         "Users who viewed ≥1 report.")
        metric_with_help(leg_cols[2], "Total Reports (est.)",
                         fmt_number(report_adoption.get("total_reports")), "From report catalogue.")
        metric_with_help(leg_cols[3], "Forecast Reliability",
                         fmt_percent(reliability.get("reliable_pct")),
                         "Share of reports with reliable forecasts.")


# ---------------------------------------------------------------------------
# Report Explorer — section renderers
# ---------------------------------------------------------------------------

_USAGE_STATUS_HELP = {
    "growing_usage":             "Usage has been increasing over the recent 28-day window.",
    "stable_regular_usage":      "Usage is broadly consistent from period to period.",
    "stable_intermittent_usage": "Usage occurs in bursts with quiet periods in between.",
    "bursty_usage":              "Usage spikes sharply on certain days.",
    "declining_usage":           "Usage has been falling over the recent 28-day window.",
    "prolonged_inactivity":      "No views have been recorded for an extended period.",
}

_FORECAST_STATUS_HELP = {
    "growth_expected":       "The model expects usage to increase over the forecast horizon.",
    "reactivation_expected": "The model expects some recovery after a period of low or zero usage.",
    "stable_outlook":        "The model expects usage to remain broadly stable.",
    "mixed_outlook":         "No single directional signal dominated — the outlook is ambiguous.",
    "low_usage_expected":    "The forecast horizon is predominantly in the low-usage band (≥ 50 % of days).",
    "uncertain_outlook":     "The prediction interval is very wide — the direction cannot be determined reliably.",
    "inactivity_expected":   "The model expects usage to reach zero over the forecast horizon.",
    "decline_expected":      "The model expects usage to decrease over the forecast horizon.",
    "insufficient_evidence": "No forecast is available for this report — either no forecast rows exist or the alignment is incompatible.",
    "invalid_forecast":      "The forecast source status is invalid; the forecast cannot be used.",
}

_MODEL_STATUS_HELP = {
    "healthy":               "No issues detected; the model has sufficient evidence.",
    "sufficient_evidence":   "Enough backtest history to assess model health.",
    "insufficient_evidence": "The model has not yet accumulated enough production run history "
                             "to assess health. This does not mean the model is unhealthy.",
    "degraded":              "One or more model health components show warning-level issues.",
    "failing":               "At least one model health component has a critical issue.",
}

_ENGAGEMENT_STATUS_HELP = {
    "healthy_broad_adoption":  "A wide audience uses the report regularly.",
    "healthy_niche_adoption":  "A small but loyal audience uses the report regularly.",
    "growing_adoption":        "The user base is expanding.",
    "declining_adoption":      "The active user count is falling.",
    "elevated_lapse":          "A higher-than-typical share of users have stopped returning.",
    "inactive":                "No recent user activity detected.",
}

_REVIEW_PRIORITY_HELP = {
    "low":      "No immediate action is required.",
    "medium":   "Worth monitoring; consider reviewing at the next scheduled cycle.",
    "high":     "Requires attention soon.",
    "critical": "Requires immediate review.",
}

_ACTION_HELP = {
    "continue_monitoring":         "No specific action needed — keep monitoring as usual.",
    "investigate_usage_decline":   "Review why usage is declining before next planning cycle.",
    "review_planned_deprecation":  "Assess whether this report should be formally retired.",
    "review_forecast_uncertainty": "High forecast uncertainty — treat the forecast directionally only.",
    "review_model_health":         "The forecasting model has health issues that warrant investigation.",
}


def _section_header(label: str) -> None:
    st.markdown(f"### {label}")


def _kv_table(rows: list[tuple[str, str]]) -> None:
    """Render a compact two-column key-value table."""
    df = pd.DataFrame(rows, columns=["", ""])
    st.dataframe(df, hide_index=True, use_container_width=True)


def render_report_summary_header(detail: dict[str, Any]) -> None:
    """Summary header: name, status, priority, action, evidence."""
    identity = detail["identity"]
    decision = detail["decision"]

    name = identity.get("report_name") or identity.get("report_id") or "Unknown"
    st.subheader(name)
    st.caption(f"Report ID: `{identity.get('report_id', '—')}`  ·  Analytics as of: {identity.get('analytics_as_of_date', '—')}")

    criticality = identity.get("criticality_level")
    cadence = identity.get("expected_usage_cadence")
    if criticality and str(criticality).lower() not in ("unknown", "nan", "none"):
        st.caption(f"Criticality: **{criticality}** · Expected cadence: {cadence or '—'}")

    status_raw = decision.get("overall_report_status") or "—"
    priority_raw = decision.get("overall_review_priority") or "—"
    action_raw = decision.get("recommended_report_action") or "—"
    evidence_raw = decision.get("overall_evidence_status") or "—"

    status_label = _status_label(status_raw)
    priority_label = _status_label(priority_raw)
    action_label = _status_label(action_raw)
    evidence_label = _status_label(evidence_raw)

    h_cols = st.columns(4)
    metric_with_help(
        h_cols[0], "Overall status", status_label,
        "The combined report health classification based on usage, engagement, "
        "forecast, and model health signals.",
    )
    metric_with_help(
        h_cols[1], "Review priority", priority_label,
        _REVIEW_PRIORITY_HELP.get(priority_raw, "Review urgency assigned by the analytics pipeline."),
    )
    metric_with_help(
        h_cols[2], "Recommended action", action_label,
        _ACTION_HELP.get(action_raw, "Action recommended by the analytics pipeline.")
        + "\n\nThis is a suggested action — it has not been executed.",
    )
    metric_with_help(
        h_cols[3], "Evidence status", evidence_label,
        "How complete the analytical evidence is for this report. "
        "'Complete' means all major evidence sources were available. "
        "'Partial' or 'insufficient' means some signals could not be computed.",
    )


def render_historical_usage_section(detail: dict[str, Any]) -> None:
    """Historical usage: recent views, change, status, streak, anomaly."""
    _section_header("Historical usage")
    h = detail["historical_usage"]

    recent = h.get("recent_28d_views")
    previous = h.get("previous_28d_views")
    change_pct = h.get("usage_change_28d_pct")
    status_raw = h.get("historical_usage_status")
    days_since = h.get("days_since_last_use")
    zero_streak = h.get("current_zero_usage_streak_days")
    volatility = h.get("usage_volatility_status")
    anomaly = h.get("latest_usage_anomaly_status")
    sufficient = h.get("history_sufficient_28d")

    insufficient = sufficient is not None and not truthy(sufficient)
    change_display = (
        "— (comparison unavailable)"
        if previous is None or (pd.notna(previous) and float(previous) == 0)
        else fmt_pct_change(change_pct)
    )

    u_cols = st.columns(4)
    metric_with_help(
        u_cols[0], "Recent views (28d)", fmt_number(recent),
        "Total views in the most recent 28-day window.",
    )
    metric_with_help(
        u_cols[1], "Previous views (28d)", fmt_number(previous),
        "Total views in the 28-day window before the most recent window.",
    )
    metric_with_help(
        u_cols[2], "28d change", change_display,
        "Percentage change from the previous 28-day period to the most recent. "
        "Shown as unavailable when the comparison period has zero views.",
    )
    metric_with_help(
        u_cols[3], "Days since last use",
        fmt_number(days_since) if days_since is not None else "—",
        "Number of days since the most recent recorded view.",
    )

    if insufficient:
        st.info(
            "Usage history is not sufficient to compute all metrics. "
            "Figures may be based on a shorter window than 28 days."
        )

    if status_raw:
        st.markdown(
            f"**Usage status:** {_status_label(status_raw)}  "
            f"— {_USAGE_STATUS_HELP.get(status_raw, '')}",
            help="Historical usage classification assigned by the analytics pipeline.",
        )

    detail_rows: list[tuple[str, str]] = []
    if zero_streak is not None and pd.notna(zero_streak) and float(zero_streak) > 0:
        detail_rows.append(("Zero-usage streak (days)", fmt_number(zero_streak)))
    if volatility:
        detail_rows.append(("Volatility", _status_label(volatility)))
    if anomaly and str(anomaly).lower() not in ("normal", "none", "nan"):
        detail_rows.append(("Anomaly indicator", _status_label(anomaly)))
    if detail_rows:
        with st.expander("Additional usage detail"):
            _kv_table(detail_rows)


def render_forecast_section(
    data: dict[str, pd.DataFrame],
    report_id: str,
    detail: dict[str, Any],
) -> None:
    """Forecast chart + mart-derived outlook metrics."""
    _section_header("Forecast")
    fc = detail["forecast"]

    forecasts = data.get("forecasts", pd.DataFrame())
    report_forecasts = (
        forecasts[forecasts["report_id"] == report_id]
        if not forecasts.empty and "report_id" in forecasts.columns
        else pd.DataFrame()
    )

    forecast_window = get_forecast_window_text(report_forecasts)
    if forecast_window != "Not available":
        st.caption(f"Forecast window: {forecast_window}")

    if not report_forecasts.empty:
        report_name_for_chart = detail["identity"].get("report_name") or report_id
        st.plotly_chart(
            usage_forecast_chart(report_forecasts, report_title=report_name_for_chart),
            use_container_width=True,
        )
        st.caption(
            "The shaded band shows the **prediction interval** — the range within which future "
            "views are expected to fall based on the model's uncertainty. It is not a confidence "
            "interval for a parameter — it describes the spread of plausible individual outcomes."
        )
    else:
        st.info("No forecast rows are available for this report.")

    outlook_raw     = fc.get("forecast_outlook_status")
    uncertainty_raw = fc.get("forecast_uncertainty_status")
    interp_raw      = fc.get("forecast_interpretation_status")
    model_raw       = fc.get("selected_model_name")
    horizon         = fc.get("available_forecast_horizon_days")
    total_28d       = fc.get("forecast_total_28d")
    change_28d      = fc.get("forecast_change_vs_actual_28d_pct")
    lower_28d       = fc.get("forecast_lower_total_28d")
    upper_28d       = fc.get("forecast_upper_total_28d")

    f_cols = st.columns(4)
    metric_with_help(
        f_cols[0], "Forecast outlook", _status_label(outlook_raw) if outlook_raw else "—",
        _FORECAST_STATUS_HELP.get(outlook_raw or "", "Direction of expected usage change."),
    )
    metric_with_help(
        f_cols[1], "Forecast vs recent (%)", fmt_pct_change(change_28d),
        "Percentage change in expected 28-day total views relative to the most recent 28-day actuals.",
    )
    metric_with_help(
        f_cols[2], "Forecast uncertainty",
        _status_label(uncertainty_raw) if uncertainty_raw else "—",
        "Width of the prediction interval relative to the forecast. "
        "High uncertainty means the range of plausible outcomes is wide — "
        "the direction of the forecast may still be reliable even if the exact magnitude is not.",
    )
    metric_with_help(
        f_cols[3], "Horizon (days)", fmt_number(horizon) if horizon else "—",
        "Number of days the forecast extends from the training cutoff.",
    )

    # Mart-derived accuracy metrics from the legacy metrics file
    metric_row = row_for_report(data.get("metrics", pd.DataFrame()), report_id)
    mae  = first_row_value(metric_row, ["selected_mae", "mae", "MAE"], default=None)
    rmse = first_row_value(metric_row, ["selected_rmse", "rmse", "RMSE"], default=None)
    wape = first_row_value(metric_row, ["selected_wape", "wape", "WAPE"], default=None)

    if any(v is not None for v in [mae, rmse, wape]):
        with st.expander("Backtest accuracy metrics"):
            st.caption(
                "These metrics measure how closely the model tracked actual usage on held-out "
                "backtest periods. They describe in-sample backtest performance — they do not "
                "guarantee future forecast accuracy. Insufficient backtest evidence does not "
                "mean the model is performing poorly."
            )
            acc_cols = st.columns(3)
            metric_with_help(
                acc_cols[0], "MAE", fmt_number(mae, 2),
                "Mean Absolute Error: average absolute difference between actual and forecast views. "
                "Lower is better.",
            )
            metric_with_help(
                acc_cols[1], "RMSE", fmt_number(rmse, 2),
                "Root Mean Squared Error: gives more weight to large errors. Lower is better.",
            )
            metric_with_help(
                acc_cols[2], "WAPE", fmt_percent(wape),
                "Weighted Absolute Percentage Error: total forecast error as a percentage of total "
                "actual usage. Not suitable when actuals include zero-view days.",
            )

    with st.expander("Forecast definitions"):
        st.markdown(
            f"**Selected model:** {friendly_model_name(model_raw)} — "
            "Automated model selection chose this statistical model based on usage pattern and "
            "available history."
        )
        st.markdown(
            "**Prediction interval:** The shaded band in the chart. "
            "It covers the range of plausible individual future view counts, not a parameter estimate. "
            "A wide interval indicates greater uncertainty in the point forecast."
        )
        if outlook_raw:
            st.markdown(
                f"**Forecast outlook ({_status_label(outlook_raw)}):** "
                + _FORECAST_STATUS_HELP.get(outlook_raw, "")
            )
        if interp_raw and str(interp_raw).lower() not in ("none", "nan", "normal"):
            st.markdown(
                f"**Forecast interpretation status:** {_status_label(interp_raw)}  "
                "— This reflects the pipeline's assessment of how much weight to place on the "
                "numerical forecast given current model health and evidence."
            )
        if lower_28d is not None and upper_28d is not None:
            st.markdown(
                f"**28-day prediction interval:** {fmt_number(lower_28d)} – {fmt_number(upper_28d)} views  "
                f"(point estimate: {fmt_number(total_28d)})"
            )
        if fc.get("training_cutoff"):
            st.caption(f"Training cutoff: {fc.get('training_cutoff')} · "
                       f"Forecast as of: {fc.get('forecast_as_of_date', '—')}")


def render_model_health_section(detail: dict[str, Any]) -> None:
    """Model health: diagnostic status, evidence, component details."""
    _section_header("Model health")
    mh = detail["model_health"]
    genai = detail["genai"]

    status_raw   = mh.get("model_diagnostic_status")
    issue_raw    = mh.get("primary_model_issue")
    evidence_mat = mh.get("production_evidence_maturity")
    model_ev     = mh.get("model_evidence_status")
    deterioration = mh.get("production_deterioration_status")

    status_label_str = _status_label(status_raw) if status_raw else "—"

    metric_with_help(
        st, "Model diagnostic status", status_label_str,
        _MODEL_STATUS_HELP.get(
            status_raw or "",
            "Overall health of the forecasting model based on backtest and production diagnostics.",
        ),
    )

    if status_raw == "insufficient_evidence":
        st.info(
            "**Insufficient evidence** means the model has not yet run in production long enough "
            "to assess its health. This is an evidence-maturity issue — it does not mean the "
            "model is producing poor forecasts. As more production cycles accumulate, this status "
            "will update automatically."
        )

    # Model confidence note from GenAI (if valid)
    genai_state = genai.get("state")
    confidence_note = genai.get("model_confidence_note")
    if confidence_note and genai_state in ("valid", "reused", "rule_based"):
        st.markdown(f"*{confidence_note}*")

    # Component detail table
    component_rows: list[tuple[str, str]] = []
    if issue_raw and str(issue_raw).lower() not in ("none", "nan", "no_issue"):
        component_rows.append(("Primary model issue", _status_label(issue_raw)))
    if mh.get("bias_status") and str(mh["bias_status"]).lower() not in ("nan", "none"):
        component_rows.append(("Bias", _status_label(mh["bias_status"])))
    if mh.get("residual_autocorrelation_status") and str(mh["residual_autocorrelation_status"]).lower() not in ("nan", "none"):
        component_rows.append(("Residual autocorrelation", _status_label(mh["residual_autocorrelation_status"])))
    if mh.get("interval_calibration_status") and str(mh["interval_calibration_status"]).lower() not in ("nan", "none"):
        component_rows.append(("Interval calibration", _status_label(mh["interval_calibration_status"])))
    if evidence_mat and str(evidence_mat).lower() not in ("nan", "none"):
        component_rows.append(("Production evidence maturity", _status_label(evidence_mat)))
    if deterioration and str(deterioration).lower() not in ("nan", "none", "no_deterioration"):
        component_rows.append(("Production deterioration", _status_label(deterioration)))

    if component_rows:
        with st.expander("Component diagnostics"):
            st.caption(
                "These components measure separate aspects of model health. "
                "Not all components are computable from backtest data alone — some require "
                "production runs. Unavailable components do not indicate a problem."
            )
            _kv_table(component_rows)
    elif status_raw == "insufficient_evidence":
        st.caption(
            "Component diagnostics are not yet available — they require sufficient "
            "production run history."
        )

    st.caption(
        "ℹ️ Model health reflects the forecasting process, not the report's business value. "
        "A report with insufficient model evidence can still have reliable usage trends."
    )


def render_engagement_section(detail: dict[str, Any]) -> None:
    """Engagement: unique users, returning share, lapse, concentration, status."""
    _section_header("Engagement")
    eng = detail["engagement"]

    any_suppressed     = eng.get("_any_suppressed", False)
    cohort_sup         = eng.get("_cohort_suppressed", False)
    concentration_sup  = eng.get("_concentration_suppressed", False)
    activity_sup       = eng.get("_activity_suppressed", False)
    frequency_sup      = eng.get("_frequency_suppressed", False)
    evidence_status    = eng.get("engagement_evidence_status")

    # Unique users — always show if available (not suppressed by activity flag)
    unique_users = eng.get("unique_users_28d")
    users_display = (
        "Suppressed (privacy)" if activity_sup
        else (fmt_number(unique_users) if unique_users is not None else "—")
    )

    returning_raw    = eng.get("returning_user_share_28d")
    lapse_raw        = eng.get("lapse_rate_28d")
    retained_raw     = eng.get("retained_user_rate_28d")
    top1_raw         = eng.get("top_1_user_view_share_28d")
    views_per_user   = eng.get("views_per_active_user_28d")
    direction        = eng.get("active_user_direction_28d")
    overall_status   = eng.get("overall_engagement_status")

    returning_display  = suppression_aware_metric(returning_raw, suppressed=cohort_sup, fmt_fn=fmt_percent)
    lapse_display      = suppression_aware_metric(lapse_raw, suppressed=cohort_sup, fmt_fn=fmt_percent)
    concentration_disp = suppression_aware_metric(top1_raw, suppressed=concentration_sup, fmt_fn=fmt_percent)

    e_cols = st.columns(4)
    metric_with_help(
        e_cols[0], "Unique users (28d)", users_display,
        "Number of distinct users who viewed this report in the last 28 days. "
        "User identifiers are not shown.",
    )
    metric_with_help(
        e_cols[1], "Returning-user share (28d)", returning_display,
        "Share of active users who had previously viewed this report (not first-time visitors). "
        "A high share suggests the report has an established, returning audience. "
        "Suppressed when the user population is too small to share safely.",
    )
    metric_with_help(
        e_cols[2], "Lapse rate (28d)", lapse_display,
        "Share of previously active users who did not return in the current 28-day window. "
        "A higher lapse rate may indicate declining relevance. "
        "Suppressed when the cohort is too small to share safely.",
    )
    metric_with_help(
        e_cols[3], "Top-user share (28d)", concentration_disp,
        "Share of total views attributable to the single most active user. "
        "A high value indicates concentrated usage, which may be a dependency risk. "
        "Low concentration means usage is broadly distributed. "
        "Suppressed when the user population is too small to share safely.",
    )

    if any_suppressed:
        st.info(
            "Some engagement metrics are suppressed because the user population for this "
            "report is too small to display safely. Suppressed fields do not represent zero — "
            "they represent unavailable evidence."
        )
    elif evidence_status and str(evidence_status).lower() in ("insufficient", "incomplete"):
        st.info(
            "Engagement evidence for this report is incomplete. "
            "Some metrics may reflect a shorter window than 28 days."
        )

    if overall_status:
        st.markdown(
            f"**Engagement status:** {_status_label(overall_status)}  "
            f"— {_ENGAGEMENT_STATUS_HELP.get(overall_status, '')}",
        )
        st.caption(
            "Low engagement does not imply low business value. "
            "A niche but loyal audience may indicate a specialist report with strong relevance "
            "to a small team."
        )

    with st.expander("Additional engagement detail"):
        extra_rows: list[tuple[str, str]] = []
        if retained_raw is not None:
            extra_rows.append(("Retained-user rate (28d)",
                               suppression_aware_metric(retained_raw, suppressed=cohort_sup,
                                                        fmt_fn=fmt_percent)))
        if views_per_user is not None:
            extra_rows.append(("Views per active user (28d)",
                               suppression_aware_metric(views_per_user, suppressed=frequency_sup,
                                                        fmt_fn=lambda v: fmt_number(v, 1))))
        if direction and str(direction).lower() not in ("nan", "none"):
            extra_rows.append(("Active-user direction", _status_label(direction)))
        if extra_rows:
            _kv_table(extra_rows)
        else:
            st.caption("No additional engagement detail is available.")


def render_diagnostics_section(detail: dict[str, Any]) -> None:
    """Diagnostics and decision: primary diagnostic, reasons, action."""
    _section_header("Diagnostics and decision")
    dec = detail["decision"]

    primary_diag = dec.get("primary_diagnostic")
    diag_cat     = dec.get("primary_diagnostic_category")
    action_raw   = dec.get("recommended_report_action")
    reasons_raw  = dec.get("report_reasons")

    if primary_diag and str(primary_diag).lower() not in ("none", "nan"):
        st.markdown(f"**Primary diagnostic:** {_status_label(primary_diag)}")
        if diag_cat and str(diag_cat).lower() not in ("none", "nan"):
            st.caption(f"Category: {_status_label(diag_cat)}")

    if action_raw:
        action_label = _status_label(action_raw)
        help_text = _ACTION_HELP.get(action_raw, "")
        st.markdown(
            f"**Recommended action:** {action_label}",
            help=help_text + "\n\nThis recommendation was produced deterministically "
                             "by the analytics pipeline. It is a suggestion — it has not been executed.",
        )

    reasons = parse_report_reasons(reasons_raw)
    if reasons:
        with st.expander("Diagnostic reasons"):
            st.caption(
                "These are the individual signals that contributed to the overall status "
                "and recommended action. Each reason uses the pipeline's internal terminology."
            )
            for r in reasons:
                st.markdown(f"- `{r}`")


def render_report_genai_section(detail: dict[str, Any]) -> None:
    """Report-level GenAI narrative using Sprint-8 fields."""
    _section_header("AI summary")
    genai = detail["genai"]
    state = genai.get("state", "missing")
    state_label = GENAI_STATE_LABELS.get(state, state)

    if state == "missing":
        st.info("No AI summary is available for this report. Run the GenAI pipeline to generate one.")
        return

    if state == "invalid":
        st.warning(
            "The AI summary for this report did not pass validation checks and is not shown. "
            "The deterministic analytics sections above remain authoritative."
        )
        return

    # Generation-status label
    if state == "rule_based":
        st.caption(
            "ℹ️ This summary was produced by the deterministic rule-based fallback — "
            "not by a live language model. It reflects validated analytics."
        )
    elif state == "fallback":
        st.caption(
            "⚠️ The AI model call failed. This is a fallback summary based on validated analytics."
        )
    elif state == "reused":
        st.caption("This validated summary was reused from a previous run (inputs unchanged).")

    executive = genai.get("executive_summary")
    if executive:
        st.markdown(f"**{executive}**")

    left, right = st.columns(2)
    with left:
        usage_insight = genai.get("usage_insight")
        if usage_insight:
            st.markdown("**Usage**")
            st.write(usage_insight)
        forecast_insight = genai.get("forecast_insight")
        if forecast_insight:
            st.markdown("**Forecast**")
            st.write(forecast_insight)
    with right:
        engagement_insight = genai.get("engagement_insight")
        if engagement_insight:
            st.markdown("**Engagement**")
            st.write(engagement_insight)
        recommended_action = genai.get("recommended_action")
        if recommended_action:
            st.markdown("**Recommended action**")
            if isinstance(recommended_action, list):
                for item in recommended_action:
                    st.markdown(f"- {item}")
            else:
                st.write(recommended_action)

    evidence_limitations = genai.get("evidence_limitations")
    if evidence_limitations:
        with st.expander("Evidence limitations"):
            if isinstance(evidence_limitations, list):
                for item in evidence_limitations:
                    st.markdown(f"- {item}")
            else:
                st.write(evidence_limitations)

    with st.expander("AI summary metadata"):
        meta_cols = st.columns(2)
        meta_items = [
            ("Generation", state_label),
            ("Validation status", genai.get("validation_status") or "—"),
            ("Analytics as of", genai.get("analytics_as_of_date") or "—"),
            ("Prompt version", genai.get("prompt_version") or "—"),
            ("Model", genai.get("model_name") or "—"),
            ("Generated at", str(genai.get("generated_at") or "")[:19] or "—"),
        ]
        for i, (label, value) in enumerate(meta_items):
            meta_cols[i % 2].caption(f"**{label}:** {value}")


def render_lineage_expander(detail: dict[str, Any], data: dict[str, pd.DataFrame], report_id: str) -> None:
    """Technical lineage details in an expander."""
    identity = detail["identity"]
    fc       = detail["forecast"]
    genai    = detail["genai"]
    mh       = detail["model_health"]

    with st.expander("Lineage and technical details"):
        # run_id is shown here, not in the top banner, to keep the main view clean
        rows: list[tuple[str, str]] = [
            ("Analytics run ID",     str(identity.get("analytics_run_id") or "—")),
            ("Analytics as of",      str(identity.get("analytics_as_of_date") or "—")),
            ("Forecast as of",       str(fc.get("forecast_as_of_date") or "—")),
            ("Training cutoff",      str(fc.get("training_cutoff") or "—")),
        ]
        if mh.get("model_evidence_status"):
            rows.append(("Model evidence status", str(mh["model_evidence_status"])))
        if genai.get("genai_run_id"):
            rows.append(("GenAI run ID",    str(genai.get("genai_run_id"))))
        if genai.get("prompt_version"):
            rows.append(("Prompt version",  str(genai.get("prompt_version"))))
        if genai.get("model_name"):
            rows.append(("AI model",        str(genai.get("model_name"))))
        if genai.get("generated_at"):
            rows.append(("Generated at",    str(genai.get("generated_at"))[:19]))
        if genai.get("validation_status"):
            rows.append(("Validation status", str(genai.get("validation_status"))))
        _kv_table(rows)


def render_report_explorer(
    data: dict[str, pd.DataFrame],
    report_id: str,
    selected_report_name: Any = None,
) -> None:
    """Unified report-level explorer — renders all report sections in sequence."""
    mart     = data.get("report_analytics", pd.DataFrame())
    mart_row = row_for_report(mart, report_id)
    eng_row  = row_for_report(data.get("engagement", pd.DataFrame()), report_id)
    insight_row = row_for_report(data.get("insights", pd.DataFrame()), report_id)

    # Fall back to name from sidebar when mart is missing
    if mart_row.empty and selected_report_name:
        mart_row = pd.Series({"report_id": report_id, "report_name": selected_report_name})

    detail = build_report_detail(mart_row, eng_row, insight_row)

    # ── 1. Summary header ──────────────────────────────────────────────────
    if not mart_row.empty:
        render_report_summary_header(detail)
    else:
        st.subheader(selected_report_name or report_id)
        st.caption(f"Report ID: `{report_id}`")
        st.warning(
            "This report is not present in the canonical analytics mart. "
            "Detailed analytics sections are unavailable. "
            "Run the analytics pipeline to generate mart outputs."
        )
        return

    st.divider()

    # ── 2. Deterministic review action (pulled out prominently) ────────────
    action_raw = detail["decision"].get("recommended_report_action")
    if action_raw and action_raw != "continue_monitoring":
        st.info(
            f"**Review action:** {_status_label(action_raw)}  \n"
            + _ACTION_HELP.get(action_raw, "")
            + "\n\n*This recommendation was produced deterministically and has not been executed.*"
        )

    # ── 3. AI summary ─────────────────────────────────────────────────────
    render_report_genai_section(detail)
    st.divider()

    # ── 4. Historical usage ────────────────────────────────────────────────
    render_historical_usage_section(detail)
    st.divider()

    # ── 5. Forecast ───────────────────────────────────────────────────────
    render_forecast_section(data, report_id, detail)
    st.divider()

    # ── 6. Model health ───────────────────────────────────────────────────
    render_model_health_section(detail)
    st.divider()

    # ── 7. Engagement ─────────────────────────────────────────────────────
    render_engagement_section(detail)
    st.divider()

    # ── 8. Diagnostics and evidence ───────────────────────────────────────
    render_diagnostics_section(detail)
    st.divider()

    # ── 9. Lineage ────────────────────────────────────────────────────────
    render_lineage_expander(detail, data, report_id)


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

    # Load-state summary in sidebar footer
    mart = data.get("report_analytics", pd.DataFrame())
    portfolio_status = data.get("_portfolio_insight_status", "absent")
    report_insight_df = data.get("insights", pd.DataFrame())
    with st.sidebar:
        st.divider()
        _genai_portfolio_note = {
            "ok": "Portfolio AI summary: available",
            "absent": "Portfolio AI summary: not generated",
            "empty": "Portfolio AI summary: empty",
            "malformed_json": "Portfolio AI summary: unreadable",
            "unexpected_structure": "Portfolio AI summary: unreadable",
            "validation_failed": "Portfolio AI summary: validation failed",
        }.get(str(portfolio_status), f"Portfolio AI summary: {portfolio_status}")
        _report_genai_count = (
            len(report_insight_df) if not report_insight_df.empty else 0
        )
        st.caption(
            f"**Data status**  \n"
            f"Canonical mart: {'loaded' if not mart.empty else 'not found'}  \n"
            f"Report AI insights: {_report_genai_count} record(s)  \n"
            f"{_genai_portfolio_note}"
        )

    # Initialise filter version (used to clear all filter widgets atomically)
    if "_sf_v" not in st.session_state:
        st.session_state["_sf_v"] = 0

    # Initialise selected report from first available
    if "selected_report_id" not in st.session_state:
        st.session_state["selected_report_id"] = reports.iloc[0]["report_id"]

    sidebar = render_sidebar(data, reports)
    report_id = sidebar["report_id"]
    filtered_mart = sidebar["filtered_mart"]

    if report_id is None:
        st.info(
            "No reports match the current search or filters. "
            "Use the sidebar to clear filters or broaden the search."
        )
        return

    # Get display name from the selectable list (already deduplicated)
    selectable = sidebar["selectable_reports"]
    selected_row = selectable[selectable["report_id"] == report_id]
    report_name = selected_row.iloc[0].get("report_name", report_id) if not selected_row.empty else report_id

    tabs = st.tabs(["Portfolio Overview", "Report Explorer"])
    with tabs[0]:
        render_overview(data, filtered_mart=filtered_mart)
    with tabs[1]:
        render_report_explorer(data, report_id, report_name)


if __name__ == "__main__":
    main()
