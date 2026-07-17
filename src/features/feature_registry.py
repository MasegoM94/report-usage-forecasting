"""Feature metadata registry for the Power BI usage forecasting pipeline.

Each entry in FEATURE_REGISTRY describes one column that appears in the
forecasting mart (``mart_forecast_features.csv``) or an intermediate mart.
The registry is the single authoritative source of truth for how each feature
is defined, what role it plays, and whether it can be used as a predictor
without temporal leakage.

Feature roles
-------------
target
    The quantity being forecast (``daily_views``).  Must never appear as a
    predictor on the same row.
identifier
    Grain key or calendar label — not a modelling input.
known-in-advance predictor
    Can be determined before day t begins (e.g. calendar attributes).
    No leakage risk.
historical predictor
    Derived from observations on days < t.  Safe to use as a predictor for
    day t after the ``shift(1)`` leakage guard is applied.
diagnostic-only
    Computed from same-day actuals (load times, user sessions, etc.).
    Useful for post-hoc analysis and dashboards, but **must not** be passed
    to a model as a predictor for the same-day target without additional
    lagging.  The pipeline does not apply ``shift(1)`` to these fields
    because they are not intended for the forecasting model.

Null policies
-------------
first_row_nan
    NaN only on the first calendar row per report (no prior-day history).
    Downstream models should handle this with imputation or by dropping the
    first row of each report's series.
insufficient_history
    NaN until enough history has accumulated (e.g. ≥ 8 days for lag_8 in
    wow_change_views).
always_present
    Non-null for every row that passes through the pipeline.
conditional
    Null under documented conditions (e.g. zero-division, newly-active flag).
"""

from __future__ import annotations

import pandas as pd


FEATURE_REGISTRY: list[dict] = [
    # ------------------------------------------------------------------ #
    # Identifiers                                                          #
    # ------------------------------------------------------------------ #
    {
        "feature_name": "report_id",
        "description": "Natural key identifying the Power BI report.",
        "grain": "daily (report_id, date)",
        "calculation_window": "n/a",
        "feature_role": "identifier",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    {
        "feature_name": "date",
        "description": "Calendar date of the observation.",
        "grain": "daily (report_id, date)",
        "calculation_window": "n/a",
        "feature_role": "identifier",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    # ------------------------------------------------------------------ #
    # Target                                                               #
    # ------------------------------------------------------------------ #
    {
        "feature_name": "daily_views",
        "description": (
            "Total view events for the report on the given date. "
            "This is the quantity being forecast. Zero when the report "
            "was active but no events were recorded (imputed by the spine)."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "single day",
        "feature_role": "target",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    # ------------------------------------------------------------------ #
    # Historical predictors (shift(1) applied before rolling/lag)         #
    # Source: add_time_series_usage_features in report_features.py        #
    # ------------------------------------------------------------------ #
    {
        "feature_name": "views_7d",
        "description": (
            "Sum of daily_views over the 7 calendar days ending yesterday "
            "(days t-1 … t-7).  shift(1) is applied before the rolling "
            "accumulation to exclude the current day's target value."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "7-day trailing window, lagged 1 day",
        "feature_role": "historical predictor",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": True,
        "null_policy": "first_row_nan",
    },
    {
        "feature_name": "views_28d",
        "description": (
            "Sum of daily_views over the 28 calendar days ending yesterday "
            "(days t-1 … t-28).  shift(1) applied before rolling."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "28-day trailing window, lagged 1 day",
        "feature_role": "historical predictor",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": True,
        "null_policy": "first_row_nan",
    },
    {
        "feature_name": "viewers_7d",
        "description": (
            "Sum of unique_viewers over the 7 days ending yesterday. "
            "shift(1) applied before rolling."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "7-day trailing window, lagged 1 day",
        "feature_role": "historical predictor",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": True,
        "null_policy": "first_row_nan",
    },
    {
        "feature_name": "viewers_28d",
        "description": (
            "Sum of unique_viewers over the 28 days ending yesterday. "
            "shift(1) applied before rolling."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "28-day trailing window, lagged 1 day",
        "feature_role": "historical predictor",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": True,
        "null_policy": "first_row_nan",
    },
    {
        "feature_name": "wow_change_views",
        "description": (
            "Week-over-week fractional change in daily views: "
            "(daily_views_{t-1} - daily_views_{t-8}) / daily_views_{t-8}. "
            "Compares yesterday's views with the same day last week. "
            "Null when daily_views_{t-8} is zero or absent."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "lag 1 day vs lag 8 days",
        "feature_role": "historical predictor",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": True,
        "null_policy": "insufficient_history",
    },
    # ------------------------------------------------------------------ #
    # Known-in-advance predictors (calendar)                              #
    # Not yet computed in the pipeline; reserved for future addition.     #
    # ------------------------------------------------------------------ #
    {
        "feature_name": "day_of_week",
        "description": (
            "Integer day of week (0=Monday … 6=Sunday). "
            "Known before the day begins; no leakage risk."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "n/a (calendar)",
        "feature_role": "known-in-advance predictor",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": False,  # not yet wired in
        "null_policy": "always_present",
    },
    {
        "feature_name": "is_weekend",
        "description": "True when day_of_week ∈ {5, 6}.",
        "grain": "daily (report_id, date)",
        "calculation_window": "n/a (calendar)",
        "feature_role": "known-in-advance predictor",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    {
        "feature_name": "is_month_end",
        "description": "True when the date is the last calendar day of a month.",
        "grain": "daily (report_id, date)",
        "calculation_window": "n/a (calendar)",
        "feature_role": "known-in-advance predictor",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    {
        "feature_name": "is_quarter_end",
        "description": "True when the date is the last calendar day of a quarter.",
        "grain": "daily (report_id, date)",
        "calculation_window": "n/a (calendar)",
        "feature_role": "known-in-advance predictor",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    # ------------------------------------------------------------------ #
    # Diagnostic-only — same-day actuals from the adoption mart           #
    # ------------------------------------------------------------------ #
    {
        "feature_name": "unique_viewers",
        "description": (
            "Distinct user count on the given date. "
            "Observed on day t; cannot be known before day t ends."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "single day",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    {
        "feature_name": "views_per_user",
        "description": (
            "daily_views / unique_viewers on the given date. "
            "Same-day actual; diagnostic only."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "single day",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "conditional",
    },
    # ------------------------------------------------------------------ #
    # Diagnostic-only — engagement mart (same-day actuals)               #
    # Source: build_user_engagement_features in engagement_features.py   #
    # These are not modified for forecasting; label accurately.           #
    # ------------------------------------------------------------------ #
    {
        "feature_name": "repeat_user_rate",
        "description": (
            "Fraction of active users on day t who had previously viewed "
            "the report on an earlier date. Computed from same-day events."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "single day (compared to prior history)",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    {
        "feature_name": "top_1_user_view_share",
        "description": (
            "Fraction of day t's total views from the single highest-consuming "
            "user. Measures single-user dependency risk. Same-day actual."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "single day",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    {
        "feature_name": "top_10pct_user_share",
        "description": (
            "Fraction of day t's total views from the top 10 percent of "
            "users by view volume. Distinct from top_1_user_view_share. "
            "Same-day actual."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "single day",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    {
        "feature_name": "days_since_last_use",
        "description": (
            "Calendar days since the report's most recent prior active date "
            "(a date with daily_views > 0, strictly before day t). "
            "Computed via shift(1) on the active-date sequence — no same-day "
            "leakage, but classified as diagnostic because it describes "
            "recency context rather than a stable predictive signal."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "lookback to previous active day",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    {
        "feature_name": "avg_pages_per_user",
        "description": (
            "Mean page views per user on day t, from fact_page_views. "
            "Same-day actual; diagnostic only."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "single day",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    # ------------------------------------------------------------------ #
    # Diagnostic-only — performance mart (same-day actuals)              #
    # Source: build_report_performance_features                           #
    # ------------------------------------------------------------------ #
    {
        "feature_name": "avg_load_time",
        "description": (
            "Mean report load time (ms) across all load events on day t. "
            "Same-day actual; diagnostic only."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "single day",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "conditional",
    },
    {
        "feature_name": "p90_load_time",
        "description": (
            "90th-percentile report load time (ms) on day t. "
            "Same-day actual; diagnostic only."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "single day",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "conditional",
    },
    {
        "feature_name": "avg_load_time_7d",
        "description": (
            "7-day rolling mean of avg_load_time (includes same-day). "
            "Not shifted because load-time features are diagnostic-only and "
            "are not passed to the forecasting model."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "7-day trailing (same-day included)",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "first_row_nan",
    },
    {
        "feature_name": "load_time_wow_change",
        "description": (
            "Fractional week-over-week change in avg_load_time. "
            "Diagnostic only; not shifted."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "lag 0 vs lag 7 (diagnostic, not for forecasting)",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "insufficient_history",
    },
    {
        "feature_name": "load_events",
        "description": "Count of load events on day t. Same-day actual; diagnostic.",
        "grain": "daily (report_id, date)",
        "calculation_window": "single day",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    # ------------------------------------------------------------------ #
    # Spine indicators                                                     #
    # ------------------------------------------------------------------ #
    {
        "feature_name": "is_observed_day",
        "description": (
            "True when at least one source event existed for this "
            "(report_id, date). False on imputed-zero rows."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "n/a",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    {
        "feature_name": "is_imputed_zero",
        "description": (
            "True when the report was active but no event existed "
            "(daily_views set to 0 by the spine). Complement of is_observed_day."
        ),
        "grain": "daily (report_id, date)",
        "calculation_window": "n/a",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": False,
        "used_by_forecasting_model": False,
        "null_policy": "always_present",
    },
    # ------------------------------------------------------------------ #
    # Report-level summary features (analytics mart, one row per report)  #
    # Source: src/analytics/report_features.py                            #
    # ------------------------------------------------------------------ #
    {
        "feature_name": "recent_28d_views",
        "description": (
            "Total daily_views over the most recent 28 calendar days of the "
            "report's history. Report-level summary; not daily-grain."
        ),
        "grain": "report",
        "calculation_window": "last 28 days of available history",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": False,
        "null_policy": "conditional",
    },
    {
        "feature_name": "previous_28d_views",
        "description": (
            "Total daily_views over the 28 days immediately preceding the "
            "recent window. Report-level summary."
        ),
        "grain": "report",
        "calculation_window": "days -56 to -29 of available history",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": False,
        "null_policy": "conditional",
    },
    {
        "feature_name": "usage_change_28d_pct",
        "description": (
            "(recent_28d_views - previous_28d_views) / previous_28d_views. "
            "Null when previous=0 and recent>0, or when history < 56 days."
        ),
        "grain": "report",
        "calculation_window": "28d vs prior 28d",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": False,
        "null_policy": "conditional",
    },
    {
        "feature_name": "usage_trend_12w_slope",
        "description": (
            "OLS slope (views/week) from a linear fit over the 12 most recent "
            "complete ISO weeks. Not a percentage. Null when < MIN_TREND_WEEKS "
            "complete weeks are available."
        ),
        "grain": "report",
        "calculation_window": "12 most recent complete ISO weeks",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": False,
        "null_policy": "conditional",
    },
    {
        "feature_name": "top_1_user_view_share_lifetime",
        "description": (
            "Report-level summary: fraction of total lifetime views from the "
            "single highest-consuming user across all history. Computed in "
            "src/analytics/report_features.py (one row per report). "
            "Distinct from the daily-grain top_1_user_view_share."
        ),
        "grain": "report",
        "calculation_window": "full history",
        "feature_role": "diagnostic-only",
        "available_at_forecast_time": True,
        "used_by_forecasting_model": False,
        "null_policy": "conditional",
    },
]


def get_feature_registry() -> pd.DataFrame:
    """Return the feature registry as a tidy DataFrame.

    Returns
    -------
    pd.DataFrame
        One row per feature with columns: ``feature_name``, ``description``,
        ``grain``, ``calculation_window``, ``feature_role``,
        ``available_at_forecast_time``, ``used_by_forecasting_model``,
        ``null_policy``.
    """
    return pd.DataFrame(FEATURE_REGISTRY)


def get_predictor_features() -> list[str]:
    """Return names of features safe to use as predictors (no leakage).

    Returns
    -------
    list[str]
        Feature names whose ``feature_role`` is one of:
        ``'historical predictor'`` or ``'known-in-advance predictor'``.
    """
    predictor_roles = {"historical predictor", "known-in-advance predictor"}
    return [
        entry["feature_name"]
        for entry in FEATURE_REGISTRY
        if entry["feature_role"] in predictor_roles
    ]


def get_diagnostic_features() -> list[str]:
    """Return names of diagnostic-only features that must not be used as predictors."""
    return [
        entry["feature_name"]
        for entry in FEATURE_REGISTRY
        if entry["feature_role"] == "diagnostic-only"
    ]
