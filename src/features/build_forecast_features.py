"""Feature assembly helpers for the daily diagnostic context mart.

``mart_report_daily_context`` is the wide analytical table joining daily
adoption, engagement, and performance features for diagnostics, segmentation,
and Streamlit.  It is NOT the SARIMA model input — the forecasting pipeline
consumes ``mart_report_daily_series`` (report_id, date, daily_views only).

``mart_report_insight_context`` is the post-forecast summary table that joins
context with forecast outputs for GenAI insight generation and the Streamlit
report detail view.
"""

from __future__ import annotations

import pandas as pd

from src.features._common import (
    _normalize_date_column,
    _validate_input_columns,
    _validate_unique_grain,
)

# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------

DAILY_CONTEXT_REQUIRED_COLS: list[str] = ["date", "report_id", "daily_views"]

INSIGHT_CONTEXT_REQUIRED_COLS: list[str] = [
    "report_id",
    "report_name",
    "segment",
    "forecast_reliable",
]


# ---------------------------------------------------------------------------
# Schema validators
# ---------------------------------------------------------------------------

def validate_daily_context_schema(df: pd.DataFrame) -> None:
    """Raise ValueError when mart_report_daily_context is missing required columns.

    Parameters
    ----------
    df:
        The assembled daily context mart to validate.

    Raises
    ------
    ValueError
        When any required column is absent or the grain is not unique at
        (date, report_id).
    """
    missing = [c for c in DAILY_CONTEXT_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f"mart_report_daily_context is missing required columns: {missing}"
        )
    _validate_unique_grain(df, ["date", "report_id"], "mart_report_daily_context")


def validate_insight_context_schema(df: pd.DataFrame) -> None:
    """Raise ValueError when mart_report_insight_context is missing required columns.

    Parameters
    ----------
    df:
        The assembled insight context mart to validate.

    Raises
    ------
    ValueError
        When any required column is absent or the grain is not unique at
        report_id.
    """
    missing = [c for c in INSIGHT_CONTEXT_REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f"mart_report_insight_context is missing required columns: {missing}"
        )
    _validate_unique_grain(df, ["report_id"], "mart_report_insight_context")


# ---------------------------------------------------------------------------
# Core builder
# ---------------------------------------------------------------------------

def build_report_daily_context(
    mart_report_daily_adoption: pd.DataFrame,
    mart_user_engagement: pd.DataFrame,
    mart_report_performance: pd.DataFrame,
    date_col: str = "date",
    report_col: str = "report_id",
) -> pd.DataFrame:
    """Assemble the daily diagnostic context mart.

    Joins the daily adoption mart with behavioural engagement and performance
    features.  The output is the input for report-level analytics, Streamlit,
    and segmentation pipelines.  It is **not** passed to SARIMA — the
    forecasting pipeline reads ``mart_report_daily_series`` instead.

    Parameters
    ----------
    mart_report_daily_adoption:
        Daily adoption mart containing base usage and time-series features.
    mart_user_engagement:
        Behavioural feature mart at the ``date`` × ``report_id`` grain.
    mart_report_performance:
        Performance feature mart at the ``date`` × ``report_id`` grain.
    date_col:
        Name of the date join key in each input.
    report_col:
        Name of the report identifier join key in each input.

    Returns
    -------
    pd.DataFrame
        Wide diagnostic context table at the ``date`` × ``report_id`` grain.
        Column names are preserved exactly as they appear in each input mart;
        ``top_10pct_user_share`` and ``top_1_user_view_share`` are kept under
        their explicit names so their distinct semantics remain visible.
    """
    inputs = {
        "mart_report_daily_adoption": mart_report_daily_adoption,
        "mart_user_engagement": mart_user_engagement,
        "mart_report_performance": mart_report_performance,
    }

    for df_name, df in inputs.items():
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"{df_name} must be a pandas DataFrame.")
        _validate_input_columns(df, [date_col, report_col])

    adoption_df = mart_report_daily_adoption.copy()
    engagement_df = mart_user_engagement.copy()
    performance_df = mart_report_performance.copy()

    adoption_df[date_col] = _normalize_date_column(adoption_df, date_col)
    engagement_df[date_col] = _normalize_date_column(engagement_df, date_col)
    performance_df[date_col] = _normalize_date_column(performance_df, date_col)

    key_columns = [date_col, report_col]
    _validate_unique_grain(adoption_df, key_columns, "mart_report_daily_adoption")
    _validate_unique_grain(engagement_df, key_columns, "mart_user_engagement")
    _validate_unique_grain(performance_df, key_columns, "mart_report_performance")

    result = (
        adoption_df.merge(engagement_df, on=key_columns, how="left")
        .merge(performance_df, on=key_columns, how="left")
        .sort_values([report_col, date_col])
        .reset_index(drop=True)
    )

    validate_daily_context_schema(result)
    return result


def build_report_insight_context(
    report_features: pd.DataFrame,
    report_segments: pd.DataFrame,
    report_diagnostics: pd.DataFrame,
    forecast_metrics: pd.DataFrame | None = None,
    report_col: str = "report_id",
) -> pd.DataFrame:
    """Assemble the post-forecast report-level insight context mart.

    Joins report-level analytics outputs (features, segments, diagnostics) with
    optional forecast reliability metrics into a single report-grain summary
    table.  This is the input for GenAI insight generation and the Streamlit
    report detail view.

    Parameters
    ----------
    report_features:
        Report-level feature summary from the analytics pipeline.
    report_segments:
        Report-level segment assignments.
    report_diagnostics:
        Report-level diagnostic flags and summaries.
    forecast_metrics:
        Optional forecast reliability table from the forecasting pipeline.
        When supplied, ``forecast_reliable`` and related metrics are joined in.
    report_col:
        Name of the report identifier column.

    Returns
    -------
    pd.DataFrame
        One row per report_id with features, segment, diagnostics, and (when
        available) forecast reliability joined together.
    """
    for df_name, df in [
        ("report_features", report_features),
        ("report_segments", report_segments),
        ("report_diagnostics", report_diagnostics),
    ]:
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"{df_name} must be a pandas DataFrame.")
        if not df.empty:
            _validate_input_columns(df, [report_col])

    if report_features.empty:
        return pd.DataFrame(columns=INSIGHT_CONTEXT_REQUIRED_COLS)

    result = report_features.copy()

    if not report_segments.empty and report_col in report_segments.columns:
        seg_cols = [report_col] + [
            c for c in report_segments.columns
            if c != report_col and c not in result.columns
        ]
        result = result.merge(
            report_segments[seg_cols].drop_duplicates(subset=[report_col]),
            on=report_col,
            how="left",
        )

    if not report_diagnostics.empty and report_col in report_diagnostics.columns:
        diag_cols = [report_col] + [
            c for c in report_diagnostics.columns
            if c != report_col and c not in result.columns
        ]
        result = result.merge(
            report_diagnostics[diag_cols].drop_duplicates(subset=[report_col]),
            on=report_col,
            how="left",
        )

    if forecast_metrics is not None and not forecast_metrics.empty:
        if report_col in forecast_metrics.columns:
            fc_cols = [report_col] + [
                c for c in forecast_metrics.columns
                if c != report_col and c not in result.columns
            ]
            result = result.merge(
                forecast_metrics[fc_cols].drop_duplicates(subset=[report_col]),
                on=report_col,
                how="left",
            )

    # Ensure the forecast_reliable column exists (False when not joined)
    if "forecast_reliable" not in result.columns:
        result["forecast_reliable"] = False

    result = result.sort_values(report_col).reset_index(drop=True)
    validate_insight_context_schema(result)
    return result


# ---------------------------------------------------------------------------
# Backward-compatibility alias
# ---------------------------------------------------------------------------

def build_forecast_feature_table(
    mart_report_daily_adoption: pd.DataFrame,
    mart_user_engagement: pd.DataFrame,
    mart_report_performance: pd.DataFrame,
    date_col: str = "date",
    report_col: str = "report_id",
) -> pd.DataFrame:
    """Deprecated alias for ``build_report_daily_context``.

    Use ``build_report_daily_context`` in new code.  This alias is kept so
    existing notebook cells that import ``build_forecast_feature_table`` do not
    break until they are migrated.
    """
    return build_report_daily_context(
        mart_report_daily_adoption,
        mart_user_engagement,
        mart_report_performance,
        date_col=date_col,
        report_col=report_col,
    )
