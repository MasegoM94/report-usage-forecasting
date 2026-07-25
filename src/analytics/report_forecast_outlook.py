"""
Report-level forecast outlook layer.

Produces one row per report summarising forecast evidence, horizon coverage,
predicted usage direction, uncertainty classification, and a composite outlook
status. Reads from the production forecast CSV and report_features.csv; writes
to outputs/analytics/report_forecast_outlook.csv.

Do NOT modify forecast generation, model selection, model fitting, retraining,
model diagnostics, engagement metrics, report diagnostics, report segmentation,
GenAI, or Streamlit files.
"""
import hashlib
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

FORECAST_OUTLOOK_SCHEMA_VERSION = "1.0.0"


@dataclass(frozen=True)
class ForecastOutlookConfig:
    # Direction thresholds
    MIN_ABSOLUTE_FORECAST_CHANGE_7D: int = 3
    MIN_ABSOLUTE_FORECAST_CHANGE_28D: int = 10
    FORECAST_GROWTH_PCT_THRESHOLD: float = 0.10
    FORECAST_DECLINE_PCT_THRESHOLD: float = 0.10
    MIN_ACTUAL_VIEWS_FOR_PCT_DIRECTION: int = 5
    # Uncertainty thresholds (relative: interval_width_total / forecast_total)
    LOW_RELATIVE_UNCERTAINTY_MAX: float = 0.25
    MODERATE_RELATIVE_UNCERTAINTY_MAX: float = 0.75
    HIGH_RELATIVE_UNCERTAINTY_MAX: float = 1.50
    MIN_FORECAST_TOTAL_FOR_RELATIVE_UNCERTAINTY: int = 1
    # Low-usage
    LOW_USAGE_DAILY_VIEW_THRESHOLD: float = 1.0
    FORECAST_ZERO_TOLERANCE: float = 0.5
    # Trend
    TREND_SLOPE_STABLE_MAX: float = 0.1  # views/day
    TREND_STRONG_THRESHOLD: float = 0.5
    # Alignment tolerance
    ALLOWED_ALIGNMENT_LAG_DAYS: int = 0
    # Staleness
    MAX_FORECAST_AGE_DAYS: int = 30
    SCHEMA_VERSION: str = "1.0.0"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

FORECAST_OUTLOOK_COLS = [
    # Identity and lineage
    "forecast_run_id", "generated_at", "forecast_generated_at",
    "forecast_as_of_date", "training_cutoff", "report_id", "report_name",
    "workspace_id", "selected_model_name", "selected_m", "selection_run_id",
    "forecast_schema_version",
    # Source evidence
    "forecast_source_status", "forecast_row_count", "earliest_forecast_date",
    "latest_forecast_date", "available_forecast_horizon_days",
    "forecast_horizon_sufficient_7d", "forecast_horizon_sufficient_28d",
    "forecast_evidence_status", "missing_forecast_evidence",
    "forecast_evidence_reasons",
    # Forecast totals
    "forecast_total_7d", "forecast_daily_average_7d",
    "forecast_total_28d", "forecast_daily_average_28d",
    "forecast_median_daily_28d", "forecast_max_daily_28d", "forecast_min_daily_28d",
    # Prediction intervals
    "forecast_lower_total_7d", "forecast_upper_total_7d",
    "forecast_lower_total_28d", "forecast_upper_total_28d",
    "forecast_interval_width_total_7d", "forecast_interval_width_total_28d",
    "forecast_interval_width_pct_7d", "forecast_interval_width_pct_28d",
    # Actual comparison
    "recent_actual_total_7d", "recent_actual_daily_average_7d",
    "recent_actual_total_28d", "recent_actual_daily_average_28d",
    "actual_forecast_alignment_status", "actual_forecast_alignment_days",
    "forecast_change_vs_actual_7d", "forecast_change_vs_actual_7d_pct",
    "forecast_change_vs_actual_28d", "forecast_change_vs_actual_28d_pct",
    "forecast_direction_7d", "forecast_direction_28d",
    # Forecast shape
    "forecast_slope_7d", "forecast_slope_28d", "forecast_trend_status",
    "forecast_peak_date_28d", "forecast_peak_daily_views_28d",
    "forecast_trough_date_28d", "forecast_trough_daily_views_28d",
    "forecast_zero_usage_days_7d", "forecast_zero_usage_days_28d",
    "forecast_low_usage_days_7d", "forecast_low_usage_days_28d",
    "forecast_low_usage_share_28d",
    # Uncertainty
    "mean_prediction_interval_width_7d", "mean_prediction_interval_width_28d",
    "median_prediction_interval_width_28d",
    "relative_uncertainty_7d", "relative_uncertainty_28d",
    "forecast_uncertainty_status", "uncertainty_evidence_status",
    # Final outlook
    "forecast_outlook_status", "primary_forecast_outlook_issue",
    "forecast_outlook_issue_count", "forecast_outlook_reasons",
    "recommended_forecast_review_action", "forecast_review_required",
]

# Allowed values (for validation)
_ALLOWED_OUTLOOK_STATUSES = frozenset({
    "growth_expected", "stable_outlook", "decline_expected",
    "inactivity_expected", "low_usage_expected", "reactivation_expected",
    "uncertain_outlook", "mixed_outlook", "insufficient_evidence",
    "invalid_forecast",
})

_ALLOWED_UNCERTAINTY_STATUSES = frozenset({
    "low_uncertainty", "moderate_uncertainty", "high_uncertainty",
    "very_high_uncertainty", "intervals_unavailable", "insufficient_horizon",
    "invalid_intervals",
})

_PROHIBITED_ACTIONS = frozenset({
    "retire_report", "delete_report", "retrain_model_automatically",
    "change_selected_model",
})


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

def normalize_production_forecasts(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise raw forecast CSV columns to canonical names."""
    df = df.copy()

    # Column alias maps
    _aliases = {
        "forecast_date": ["Date", "date", "forecast_date"],
        "report_id": ["ReportId", "report_id"],
        "report_name": ["ReportName", "report_name"],
        "point_forecast": ["forecast", "Forecast", "forecast_views", "point_forecast"],
        "selected_model_name": ["ModelName", "model_name", "selected_model",
                                "selected_model_name"],
        "lower_bound": ["lower_ci", "LowerCI", "lower_bound"],
        "upper_bound": ["upper_ci", "UpperCI", "upper_bound"],
        "forecast_run_id": ["run_id", "RunId", "forecast_run_id"],
        "forecast_generated_at": ["ModelRunTimestamp", "run_timestamp",
                                   "forecast_generated_at"],
        "training_cutoff": ["TrainingCutoff", "training_cutoff"],
        "selected_m": ["selected_m", "SelectedM"],
        "is_forecast": ["IsForecast", "is_forecast"],
        "is_placeholder": ["IsPlaceholderRow", "is_placeholder"],
    }

    rename_map = {}
    for canonical, aliases in _aliases.items():
        if canonical in df.columns:
            continue
        for alias in aliases:
            if alias in df.columns and alias != canonical:
                rename_map[alias] = canonical
                break

    df = df.rename(columns=rename_map)

    # Parse forecast_date
    if "forecast_date" in df.columns:
        df["forecast_date"] = pd.to_datetime(df["forecast_date"]).dt.date

    # Ensure point_forecast is float
    if "point_forecast" in df.columns:
        df["point_forecast"] = pd.to_numeric(df["point_forecast"], errors="coerce").astype(float)

    # Coerce bounds
    for col in ("lower_bound", "upper_bound"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    return df


# ---------------------------------------------------------------------------
# Run-level metadata
# ---------------------------------------------------------------------------

def resolve_forecast_cutoff(forecast_df: pd.DataFrame) -> dict:
    """Derive run-level metadata from the normalised forecast DataFrame."""
    meta: dict = {}

    if "forecast_run_id" in forecast_df.columns and not forecast_df["forecast_run_id"].isna().all():
        meta["forecast_run_id"] = str(forecast_df["forecast_run_id"].iloc[0])
    else:
        # Stable hash from earliest date + first report
        seed = ""
        if "forecast_date" in forecast_df.columns:
            seed += str(forecast_df["forecast_date"].min())
        if "report_id" in forecast_df.columns:
            seed += str(forecast_df["report_id"].iloc[0])
        meta["forecast_run_id"] = hashlib.md5(seed.encode()).hexdigest()[:16]

    if "forecast_generated_at" in forecast_df.columns and not forecast_df["forecast_generated_at"].isna().all():
        meta["forecast_generated_at"] = str(forecast_df["forecast_generated_at"].iloc[0])
    else:
        meta["forecast_generated_at"] = None

    if "training_cutoff" in forecast_df.columns and not forecast_df["training_cutoff"].isna().all():
        meta["training_cutoff"] = str(forecast_df["training_cutoff"].iloc[0])
    else:
        meta["training_cutoff"] = None

    return meta


# ---------------------------------------------------------------------------
# Horizon windowing
# ---------------------------------------------------------------------------

def build_forecast_horizon_windows(
    report_forecasts: pd.DataFrame,
    forecast_as_of_date: date,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (df_7d, df_28d) slices from a single report's forecast rows.

    Only dates strictly after forecast_as_of_date are considered future.
    """
    future = report_forecasts[
        report_forecasts["forecast_date"] > forecast_as_of_date
    ].sort_values("forecast_date").reset_index(drop=True)

    df_7d = future.head(7)
    df_28d = future.head(28)

    return df_7d, df_28d


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate_report_forecast_horizons(
    df_7d: pd.DataFrame,
    df_28d: pd.DataFrame,
) -> dict:
    """Aggregate forecast point values and prediction intervals for 7d/28d."""
    result: dict = {}

    has_lower = "lower_bound" in df_7d.columns and not df_7d["lower_bound"].isna().all()
    has_lower_28 = "lower_bound" in df_28d.columns and not df_28d["lower_bound"].isna().all()

    # --- 7d ---
    if len(df_7d) >= 7:
        vals = df_7d["point_forecast"].values
        result["forecast_total_7d"] = float(np.sum(vals))
        result["forecast_daily_average_7d"] = float(np.mean(vals))

        if has_lower and "upper_bound" in df_7d.columns and not df_7d["upper_bound"].isna().all():
            result["forecast_lower_total_7d"] = float(df_7d["lower_bound"].sum())
            result["forecast_upper_total_7d"] = float(df_7d["upper_bound"].sum())
            width = result["forecast_upper_total_7d"] - result["forecast_lower_total_7d"]
            result["forecast_interval_width_total_7d"] = float(width)
            total = result["forecast_total_7d"]
            result["forecast_interval_width_pct_7d"] = (
                float(width / total) if total > 0 else None
            )
        else:
            result["forecast_lower_total_7d"] = None
            result["forecast_upper_total_7d"] = None
            result["forecast_interval_width_total_7d"] = None
            result["forecast_interval_width_pct_7d"] = None
    else:
        for k in ("forecast_total_7d", "forecast_daily_average_7d",
                  "forecast_lower_total_7d", "forecast_upper_total_7d",
                  "forecast_interval_width_total_7d", "forecast_interval_width_pct_7d"):
            result[k] = None

    # --- 28d ---
    if len(df_28d) >= 28:
        vals28 = df_28d["point_forecast"].values
        result["forecast_total_28d"] = float(np.sum(vals28))
        result["forecast_daily_average_28d"] = float(np.mean(vals28))
        result["forecast_median_daily_28d"] = float(np.median(vals28))
        result["forecast_max_daily_28d"] = float(np.max(vals28))
        result["forecast_min_daily_28d"] = float(np.min(vals28))

        if has_lower_28 and "upper_bound" in df_28d.columns and not df_28d["upper_bound"].isna().all():
            result["forecast_lower_total_28d"] = float(df_28d["lower_bound"].sum())
            result["forecast_upper_total_28d"] = float(df_28d["upper_bound"].sum())
            width28 = result["forecast_upper_total_28d"] - result["forecast_lower_total_28d"]
            result["forecast_interval_width_total_28d"] = float(width28)
            total28 = result["forecast_total_28d"]
            result["forecast_interval_width_pct_28d"] = (
                float(width28 / total28) if total28 > 0 else None
            )
        else:
            result["forecast_lower_total_28d"] = None
            result["forecast_upper_total_28d"] = None
            result["forecast_interval_width_total_28d"] = None
            result["forecast_interval_width_pct_28d"] = None
    else:
        for k in ("forecast_total_28d", "forecast_daily_average_28d",
                  "forecast_median_daily_28d", "forecast_max_daily_28d",
                  "forecast_min_daily_28d", "forecast_lower_total_28d",
                  "forecast_upper_total_28d", "forecast_interval_width_total_28d",
                  "forecast_interval_width_pct_28d"):
            result[k] = None

    return result


# ---------------------------------------------------------------------------
# Direction classification
# ---------------------------------------------------------------------------

def classify_forecast_direction(
    forecast_total: Optional[float],
    recent_actual: Optional[float],
    cfg: ForecastOutlookConfig,
    label: str = "28d",
) -> str:
    """Classify expected direction relative to recent actual usage."""
    if forecast_total is None:
        return "insufficient_evidence"
    if recent_actual is None:
        return "insufficient_evidence"
    if np.isnan(forecast_total):
        return "invalid_forecast"

    min_abs = (
        cfg.MIN_ABSOLUTE_FORECAST_CHANGE_28D
        if label == "28d"
        else cfg.MIN_ABSOLUTE_FORECAST_CHANGE_7D
    )

    actual = float(recent_actual)
    fcast = float(forecast_total)

    if actual == 0 and fcast > cfg.FORECAST_ZERO_TOLERANCE:
        return "expected_reactivation"
    if actual <= cfg.FORECAST_ZERO_TOLERANCE and fcast <= cfg.FORECAST_ZERO_TOLERANCE:
        return "expected_inactive"

    change = fcast - actual
    if actual > 0:
        pct = change / actual
    else:
        pct = 0.0

    abs_change = abs(change)
    if abs_change < min_abs or abs(pct) < cfg.FORECAST_GROWTH_PCT_THRESHOLD:
        return "expected_stability"
    if change >= min_abs and pct >= cfg.FORECAST_GROWTH_PCT_THRESHOLD and actual >= cfg.MIN_ACTUAL_VIEWS_FOR_PCT_DIRECTION:
        return "expected_growth"
    if change <= -min_abs and pct <= -cfg.FORECAST_DECLINE_PCT_THRESHOLD and actual >= cfg.MIN_ACTUAL_VIEWS_FOR_PCT_DIRECTION:
        return "expected_decline"
    return "expected_stability"


# ---------------------------------------------------------------------------
# Actual comparison
# ---------------------------------------------------------------------------

def calculate_forecast_actual_comparisons(
    forecast_dict: dict,
    features_row: pd.Series,
    cfg: ForecastOutlookConfig,
    forecast_as_of_date: Optional[date] = None,
) -> dict:
    """Build forecast-vs-actual comparison fields from features row."""
    result: dict = {}

    r7 = features_row.get("recent_7d_views")
    r28 = features_row.get("recent_28d_views")
    r7 = float(r7) if r7 is not None and not (isinstance(r7, float) and np.isnan(r7)) else None
    r28 = float(r28) if r28 is not None and not (isinstance(r28, float) and np.isnan(r28)) else None

    result["recent_actual_total_7d"] = r7
    result["recent_actual_daily_average_7d"] = (r7 / 7.0) if r7 is not None else None
    result["recent_actual_total_28d"] = r28
    result["recent_actual_daily_average_28d"] = (r28 / 28.0) if r28 is not None else None

    # Temporal alignment
    features_as_of = features_row.get("analytics_as_of_date")
    if features_as_of is not None and forecast_as_of_date is not None:
        try:
            f_date = (
                features_as_of if isinstance(features_as_of, date)
                else pd.to_datetime(str(features_as_of)).date()
            )
            lag = (f_date - forecast_as_of_date).days
            result["actual_forecast_alignment_days"] = lag
            if abs(lag) <= cfg.ALLOWED_ALIGNMENT_LAG_DAYS:
                result["actual_forecast_alignment_status"] = "aligned"
            else:
                result["actual_forecast_alignment_status"] = (
                    "lagged" if lag > 0 else "incompatible"
                )
        except Exception:
            result["actual_forecast_alignment_status"] = "unknown"
            result["actual_forecast_alignment_days"] = None
    else:
        result["actual_forecast_alignment_status"] = "unknown"
        result["actual_forecast_alignment_days"] = None

    # Changes
    f7 = forecast_dict.get("forecast_total_7d")
    f28 = forecast_dict.get("forecast_total_28d")

    if f7 is not None and r7 is not None:
        ch7 = f7 - r7
        result["forecast_change_vs_actual_7d"] = float(ch7)
        result["forecast_change_vs_actual_7d_pct"] = float(ch7 / r7) if r7 > 0 else None
    else:
        result["forecast_change_vs_actual_7d"] = None
        result["forecast_change_vs_actual_7d_pct"] = None

    if f28 is not None and r28 is not None:
        ch28 = f28 - r28
        result["forecast_change_vs_actual_28d"] = float(ch28)
        result["forecast_change_vs_actual_28d_pct"] = float(ch28 / r28) if r28 > 0 else None
    else:
        result["forecast_change_vs_actual_28d"] = None
        result["forecast_change_vs_actual_28d_pct"] = None

    result["forecast_direction_7d"] = classify_forecast_direction(f7, r7, cfg, "7d")
    result["forecast_direction_28d"] = classify_forecast_direction(f28, r28, cfg, "28d")

    return result


# ---------------------------------------------------------------------------
# Forecast shape
# ---------------------------------------------------------------------------

def _compute_slope(values: np.ndarray) -> Optional[float]:
    """OLS slope of values over their index."""
    n = len(values)
    if n < 2:
        return None
    x = np.arange(n, dtype=float)
    xm, ym = x.mean(), values.mean()
    denom = np.sum((x - xm) ** 2)
    if denom == 0:
        return 0.0
    return float(np.sum((x - xm) * (values - ym)) / denom)


def calculate_forecast_shape(
    df_7d: pd.DataFrame,
    df_28d: pd.DataFrame,
    cfg: ForecastOutlookConfig,
) -> dict:
    """Compute slope, trend status, peak/trough, and low-usage day counts."""
    result: dict = {}

    # Slopes
    if len(df_7d) >= 7:
        slope7 = _compute_slope(df_7d["point_forecast"].values)
        result["forecast_slope_7d"] = slope7
    else:
        result["forecast_slope_7d"] = None

    if len(df_28d) >= 28:
        vals28 = df_28d["point_forecast"].values
        slope28 = _compute_slope(vals28)
        result["forecast_slope_28d"] = slope28

        # Trend status
        if slope28 is None:
            result["forecast_trend_status"] = "insufficient_data"
        elif abs(slope28) <= cfg.TREND_SLOPE_STABLE_MAX:
            result["forecast_trend_status"] = "stable_trend"
        elif slope28 > cfg.TREND_STRONG_THRESHOLD:
            result["forecast_trend_status"] = "strong_upward_trend"
        elif slope28 > cfg.TREND_SLOPE_STABLE_MAX:
            result["forecast_trend_status"] = "moderate_upward_trend"
        elif slope28 < -cfg.TREND_STRONG_THRESHOLD:
            result["forecast_trend_status"] = "strong_downward_trend"
        else:
            result["forecast_trend_status"] = "moderate_downward_trend"

        # Peak / trough
        peak_idx = int(np.argmax(vals28))
        trough_idx = int(np.argmin(vals28))
        result["forecast_peak_date_28d"] = str(df_28d["forecast_date"].iloc[peak_idx])
        result["forecast_peak_daily_views_28d"] = float(vals28[peak_idx])
        result["forecast_trough_date_28d"] = str(df_28d["forecast_date"].iloc[trough_idx])
        result["forecast_trough_daily_views_28d"] = float(vals28[trough_idx])

        # Low/zero usage days
        zero7 = int((df_7d["point_forecast"] <= cfg.FORECAST_ZERO_TOLERANCE).sum()) if len(df_7d) >= 7 else None
        zero28 = int((vals28 <= cfg.FORECAST_ZERO_TOLERANCE).sum())
        low7 = int((df_7d["point_forecast"] < cfg.LOW_USAGE_DAILY_VIEW_THRESHOLD).sum()) if len(df_7d) >= 7 else None
        low28 = int((vals28 < cfg.LOW_USAGE_DAILY_VIEW_THRESHOLD).sum())

        result["forecast_zero_usage_days_7d"] = zero7
        result["forecast_zero_usage_days_28d"] = zero28
        result["forecast_low_usage_days_7d"] = low7
        result["forecast_low_usage_days_28d"] = low28
        result["forecast_low_usage_share_28d"] = float(low28 / 28)
    else:
        for k in ("forecast_slope_28d", "forecast_trend_status",
                  "forecast_peak_date_28d", "forecast_peak_daily_views_28d",
                  "forecast_trough_date_28d", "forecast_trough_daily_views_28d",
                  "forecast_zero_usage_days_7d", "forecast_zero_usage_days_28d",
                  "forecast_low_usage_days_7d", "forecast_low_usage_days_28d",
                  "forecast_low_usage_share_28d"):
            result[k] = None

    return result


# ---------------------------------------------------------------------------
# Uncertainty
# ---------------------------------------------------------------------------

def classify_forecast_uncertainty_status(
    relative_uncertainty: Optional[float],
    has_intervals: bool,
    cfg: ForecastOutlookConfig,
) -> str:
    if not has_intervals:
        return "intervals_unavailable"
    if relative_uncertainty is None:
        return "insufficient_horizon"
    if relative_uncertainty < 0:
        return "invalid_intervals"
    if relative_uncertainty <= cfg.LOW_RELATIVE_UNCERTAINTY_MAX:
        return "low_uncertainty"
    if relative_uncertainty <= cfg.MODERATE_RELATIVE_UNCERTAINTY_MAX:
        return "moderate_uncertainty"
    if relative_uncertainty <= cfg.HIGH_RELATIVE_UNCERTAINTY_MAX:
        return "high_uncertainty"
    return "very_high_uncertainty"


def calculate_forecast_uncertainty(
    df_7d: pd.DataFrame,
    df_28d: pd.DataFrame,
    forecast_total_7d: Optional[float],
    forecast_total_28d: Optional[float],
    cfg: ForecastOutlookConfig,
) -> dict:
    """Compute prediction interval widths and uncertainty classification."""
    result: dict = {}

    has_intervals = (
        "lower_bound" in df_28d.columns
        and "upper_bound" in df_28d.columns
        and not df_28d["lower_bound"].isna().all()
        and not df_28d["upper_bound"].isna().all()
    )

    # 7d interval stats
    if has_intervals and len(df_7d) >= 7 and "lower_bound" in df_7d.columns:
        widths7 = (df_7d["upper_bound"] - df_7d["lower_bound"]).dropna()
        if len(widths7) > 0:
            result["mean_prediction_interval_width_7d"] = float(widths7.mean())
        else:
            result["mean_prediction_interval_width_7d"] = None
    else:
        result["mean_prediction_interval_width_7d"] = None

    # 28d interval stats
    if has_intervals and len(df_28d) >= 28:
        widths28 = (df_28d["upper_bound"] - df_28d["lower_bound"]).dropna()
        if len(widths28) > 0:
            result["mean_prediction_interval_width_28d"] = float(widths28.mean())
            result["median_prediction_interval_width_28d"] = float(widths28.median())
        else:
            result["mean_prediction_interval_width_28d"] = None
            result["median_prediction_interval_width_28d"] = None
    else:
        result["mean_prediction_interval_width_28d"] = None
        result["median_prediction_interval_width_28d"] = None

    # Relative uncertainty
    def _rel(total_width, forecast_total):
        if total_width is None or forecast_total is None:
            return None
        if forecast_total < cfg.MIN_FORECAST_TOTAL_FOR_RELATIVE_UNCERTAINTY:
            return None
        return float(total_width / forecast_total)

    width7_total = None
    if has_intervals and len(df_7d) >= 7 and "lower_bound" in df_7d.columns:
        lb = df_7d["lower_bound"].sum() if not df_7d["lower_bound"].isna().all() else None
        ub = df_7d["upper_bound"].sum() if not df_7d["upper_bound"].isna().all() else None
        if lb is not None and ub is not None:
            width7_total = float(ub - lb)

    width28_total = None
    if has_intervals and len(df_28d) >= 28:
        lb28 = df_28d["lower_bound"].sum() if not df_28d["lower_bound"].isna().all() else None
        ub28 = df_28d["upper_bound"].sum() if not df_28d["upper_bound"].isna().all() else None
        if lb28 is not None and ub28 is not None:
            width28_total = float(ub28 - lb28)

    result["relative_uncertainty_7d"] = _rel(width7_total, forecast_total_7d)
    result["relative_uncertainty_28d"] = _rel(width28_total, forecast_total_28d)

    # Status uses 28d
    if not has_intervals:
        result["forecast_uncertainty_status"] = "intervals_unavailable"
        result["uncertainty_evidence_status"] = "no_prediction_intervals"
    elif len(df_28d) < 28:
        result["forecast_uncertainty_status"] = "insufficient_horizon"
        result["uncertainty_evidence_status"] = "horizon_too_short"
    else:
        status = classify_forecast_uncertainty_status(
            result["relative_uncertainty_28d"], has_intervals, cfg
        )
        result["forecast_uncertainty_status"] = status
        result["uncertainty_evidence_status"] = "intervals_available"

    return result


# ---------------------------------------------------------------------------
# Outlook classification
# ---------------------------------------------------------------------------

def classify_forecast_outlook_status(row_dict: dict, cfg: ForecastOutlookConfig) -> str:
    """Classify composite outlook status from row fields."""
    src = row_dict.get("forecast_source_status", "")
    h7 = row_dict.get("forecast_horizon_sufficient_7d", False)
    h28 = row_dict.get("forecast_horizon_sufficient_28d", False)
    align = row_dict.get("actual_forecast_alignment_status", "")
    dir28 = row_dict.get("forecast_direction_28d", "")
    low_share = row_dict.get("forecast_low_usage_share_28d")
    unc = row_dict.get("forecast_uncertainty_status", "")

    # 1. Invalid
    if src == "invalid":
        return "invalid_forecast"

    # 2. Missing source
    if not h7 and src == "missing":
        return "insufficient_evidence"

    # 3. Incompatible alignment
    if align == "incompatible":
        return "insufficient_evidence"

    # 4. Inactivity
    if dir28 == "expected_inactive" and h28:
        return "inactivity_expected"

    # 5. Low usage
    if low_share is not None and low_share >= 0.5 and dir28 != "expected_inactive":
        return "low_usage_expected"

    # 6. Decline
    if dir28 == "expected_decline" and unc != "very_high_uncertainty":
        return "decline_expected"

    # 7. Reactivation
    if dir28 == "expected_reactivation":
        return "reactivation_expected"

    # 8. Uncertain
    if unc == "very_high_uncertainty":
        return "uncertain_outlook"

    # 9. Growth
    if dir28 == "expected_growth" and unc != "very_high_uncertainty":
        return "growth_expected"

    # 10. Stable
    if dir28 == "expected_stability":
        return "stable_outlook"

    return "mixed_outlook"


def determine_primary_forecast_issue(row_dict: dict) -> str:
    """Return the single highest-priority issue label."""
    src = row_dict.get("forecast_source_status", "")
    h28 = row_dict.get("forecast_horizon_sufficient_28d", False)
    status = row_dict.get("forecast_outlook_status", "")
    unc = row_dict.get("forecast_uncertainty_status", "")
    dir7 = row_dict.get("forecast_direction_7d", "")
    dir28 = row_dict.get("forecast_direction_28d", "")

    if src == "missing":
        return "missing_forecast"
    if src == "invalid":
        return "invalid_forecast"
    if not h28:
        return "insufficient_horizon"
    if src == "stale":
        return "stale_forecast"
    # Check for invalid intervals (lower > upper)
    lb = row_dict.get("forecast_lower_total_28d")
    ub = row_dict.get("forecast_upper_total_28d")
    if lb is not None and ub is not None and lb > ub:
        return "invalid_intervals"
    if status == "inactivity_expected":
        return "expected_inactivity"
    if status == "low_usage_expected":
        return "expected_low_usage"
    if status == "decline_expected":
        return "expected_decline"
    if unc in ("high_uncertainty", "very_high_uncertainty"):
        return "high_uncertainty"
    # Conflicting signals
    valid_dirs = {
        "expected_growth", "expected_stability", "expected_decline",
        "expected_reactivation", "expected_inactive",
    }
    if dir7 in valid_dirs and dir28 in valid_dirs and dir7 != dir28:
        return "conflicting_forecast_signals"
    return "none"


# ---------------------------------------------------------------------------
# Reasons builder
# ---------------------------------------------------------------------------

def build_forecast_outlook_reasons(row_dict: dict, cfg: ForecastOutlookConfig) -> str:
    """Build a deterministic 10-slot pipe-separated reason string."""
    slots = []

    # 1. Forecast evidence
    src = row_dict.get("forecast_source_status", "unknown")
    n = row_dict.get("forecast_row_count", 0) or 0
    slots.append(f"source:{src}|rows:{n}")

    # 2. Model lineage
    model = row_dict.get("selected_model_name") or "unknown"
    slots.append(f"model:{model}")

    # 3. Horizon coverage
    h7 = row_dict.get("forecast_horizon_sufficient_7d", False)
    h28 = row_dict.get("forecast_horizon_sufficient_28d", False)
    avail = row_dict.get("available_forecast_horizon_days", 0) or 0
    slots.append(f"horizon_7d:{h7}|horizon_28d:{h28}|avail:{avail}d")

    # 4. Recent observed usage
    r28 = row_dict.get("recent_actual_total_28d")
    r7 = row_dict.get("recent_actual_total_7d")
    slots.append(f"actual_7d:{r7}|actual_28d:{r28}")

    # 5. Forecast total
    f7 = row_dict.get("forecast_total_7d")
    f28 = row_dict.get("forecast_total_28d")
    slots.append(f"forecast_7d:{f7}|forecast_28d:{f28}")

    # 6. Forecast vs actual change
    ch28 = row_dict.get("forecast_change_vs_actual_28d")
    ch28_pct = row_dict.get("forecast_change_vs_actual_28d_pct")
    pct_str = f"{ch28_pct:.2f}" if ch28_pct is not None else "N/A"
    slots.append(f"change_28d:{ch28}|pct:{pct_str}")

    # 7. Forecast shape
    slope28 = row_dict.get("forecast_slope_28d")
    trend = row_dict.get("forecast_trend_status") or "unknown"
    slope_str = f"{slope28:.3f}" if slope28 is not None else "N/A"
    slots.append(f"slope_28d:{slope_str}|trend:{trend}")

    # 8. Low/zero usage
    zero28 = row_dict.get("forecast_zero_usage_days_28d")
    low28 = row_dict.get("forecast_low_usage_days_28d")
    low_share = row_dict.get("forecast_low_usage_share_28d")
    share_str = f"{low_share:.2f}" if low_share is not None else "N/A"
    slots.append(f"zero_days_28d:{zero28}|low_days_28d:{low28}|low_share:{share_str}")

    # 9. Prediction uncertainty
    unc = row_dict.get("forecast_uncertainty_status") or "unknown"
    rel28 = row_dict.get("relative_uncertainty_28d")
    rel_str = f"{rel28:.3f}" if rel28 is not None else "N/A"
    slots.append(f"uncertainty:{unc}|rel_28d:{rel_str}")

    # 10. Final status
    outlook = row_dict.get("forecast_outlook_status") or "unknown"
    issue = row_dict.get("primary_forecast_outlook_issue") or "none"
    slots.append(f"outlook:{outlook}|issue:{issue}")

    return " | ".join(slots)


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_report_forecast_outlook(
    forecast_df: pd.DataFrame,
    features_df: pd.DataFrame,
    dim_report_df: pd.DataFrame,
    cfg: ForecastOutlookConfig,
    run_id: str,
) -> pd.DataFrame:
    """Build the full outlook DataFrame, one row per report in dim_report_df."""
    # Normalise forecast input
    forecast_df = normalize_production_forecasts(forecast_df)

    # Filter to future-only rows if IsForecast flag is present
    if "is_forecast" in forecast_df.columns:
        forecast_df = forecast_df[forecast_df["is_forecast"] == 1].copy()

    # Drop placeholder rows
    if "is_placeholder" in forecast_df.columns:
        forecast_df = forecast_df[forecast_df["is_placeholder"] != 1].copy()

    # Validate grain (no duplicate report-date)
    if "forecast_date" in forecast_df.columns:
        dup_mask = forecast_df.duplicated(subset=["report_id", "forecast_date"], keep=False)
        if dup_mask.any():
            # Keep first occurrence per (report, date)
            forecast_df = forecast_df[~dup_mask | ~forecast_df.duplicated(
                subset=["report_id", "forecast_date"], keep="first"
            )].copy()

    # Run-level metadata
    run_meta = resolve_forecast_cutoff(forecast_df) if len(forecast_df) > 0 else {
        "forecast_run_id": run_id,
        "forecast_generated_at": None,
        "training_cutoff": None,
    }

    now_str = datetime.utcnow().isoformat()
    features_df = features_df.copy()
    features_df["report_id"] = features_df["report_id"].astype(str)
    feat_index = features_df.set_index("report_id")

    rows = []
    for _, dim_row in dim_report_df.iterrows():
        rid = str(dim_row["report_id"])
        report_name = dim_row.get("report_name", rid)
        workspace_id = dim_row.get("workspace_id", None)

        row: dict = {
            "forecast_run_id": run_id,
            "generated_at": now_str,
            "forecast_generated_at": run_meta.get("forecast_generated_at"),
            "training_cutoff": run_meta.get("training_cutoff"),
            "report_id": rid,
            "report_name": report_name,
            "workspace_id": workspace_id,
            "forecast_schema_version": FORECAST_OUTLOOK_SCHEMA_VERSION,
            "selection_run_id": run_meta.get("forecast_run_id"),
        }

        # Get features row
        if rid in feat_index.index:
            feat_row = feat_index.loc[rid]
        else:
            feat_row = pd.Series(dtype=object)

        # Report-level forecast data
        rfc = forecast_df[forecast_df["report_id"] == rid].copy()

        if len(rfc) == 0:
            # No forecast rows
            row.update({
                "selected_model_name": None,
                "selected_m": None,
                "forecast_source_status": "missing",
                "forecast_row_count": 0,
                "earliest_forecast_date": None,
                "latest_forecast_date": None,
                "available_forecast_horizon_days": 0,
                "forecast_horizon_sufficient_7d": False,
                "forecast_horizon_sufficient_28d": False,
                "forecast_evidence_status": "no_forecast_data",
                "missing_forecast_evidence": True,
                "forecast_evidence_reasons": "no_forecast_rows",
                "forecast_as_of_date": None,
            })
            # Nullify all forecast value fields
            for col in FORECAST_OUTLOOK_COLS:
                if col not in row:
                    row[col] = None

            row["forecast_outlook_status"] = "insufficient_evidence"
            row["primary_forecast_outlook_issue"] = "missing_forecast"
            row["forecast_outlook_issue_count"] = 1
            row["forecast_review_required"] = True
            row["recommended_forecast_review_action"] = "investigate_missing_forecast"
            row["forecast_outlook_reasons"] = build_forecast_outlook_reasons(row, cfg)
            rows.append(row)
            continue

        # Derive forecast_as_of_date: min forecast date minus 1 day
        rfc = rfc.sort_values("forecast_date").reset_index(drop=True)
        min_date = rfc["forecast_date"].min()
        forecast_as_of_date = min_date - timedelta(days=1)

        # Model lineage
        model_name = str(rfc["selected_model_name"].iloc[0]) if "selected_model_name" in rfc.columns else None
        sel_m = rfc["selected_m"].iloc[0] if "selected_m" in rfc.columns and not rfc["selected_m"].isna().all() else None

        row["selected_model_name"] = model_name
        row["selected_m"] = sel_m
        row["forecast_as_of_date"] = str(forecast_as_of_date)

        # Validate point forecast values
        invalid_fc = rfc["point_forecast"].isna().any() if "point_forecast" in rfc.columns else True
        if invalid_fc or "point_forecast" not in rfc.columns:
            row["forecast_source_status"] = "invalid"
        else:
            row["forecast_source_status"] = "available"

        # Horizon metadata
        max_date = rfc["forecast_date"].max()
        avail_days = (max_date - forecast_as_of_date).days
        row["forecast_row_count"] = len(rfc)
        row["earliest_forecast_date"] = str(min_date)
        row["latest_forecast_date"] = str(max_date)
        row["available_forecast_horizon_days"] = avail_days
        row["forecast_horizon_sufficient_7d"] = avail_days >= 7
        row["forecast_horizon_sufficient_28d"] = avail_days >= 28

        # Evidence summary
        evidence_reasons = []
        if row["forecast_source_status"] == "invalid":
            evidence_reasons.append("invalid_point_forecast_values")
        if not row["forecast_horizon_sufficient_7d"]:
            evidence_reasons.append("horizon_lt_7d")
        if not row["forecast_horizon_sufficient_28d"]:
            evidence_reasons.append("horizon_lt_28d")

        row["forecast_evidence_status"] = "sufficient" if not evidence_reasons else "insufficient"
        row["missing_forecast_evidence"] = bool(evidence_reasons)
        row["forecast_evidence_reasons"] = "|".join(evidence_reasons) if evidence_reasons else None

        # Build horizon windows
        df_7d, df_28d = build_forecast_horizon_windows(rfc, forecast_as_of_date)

        # Aggregation
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        row.update(agg)

        # Actual comparisons
        comp = calculate_forecast_actual_comparisons(agg, feat_row, cfg, forecast_as_of_date)
        row.update(comp)

        # Shape
        shape = calculate_forecast_shape(df_7d, df_28d, cfg)
        row.update(shape)

        # Uncertainty
        unc = calculate_forecast_uncertainty(
            df_7d, df_28d,
            agg.get("forecast_total_7d"),
            agg.get("forecast_total_28d"),
            cfg,
        )
        row.update(unc)

        # Outlook
        outlook = classify_forecast_outlook_status(row, cfg)
        row["forecast_outlook_status"] = outlook
        row["primary_forecast_outlook_issue"] = determine_primary_forecast_issue(row)

        # Issue count
        issue_count = 0
        if row["primary_forecast_outlook_issue"] != "none":
            issue_count += 1
        if not row.get("forecast_horizon_sufficient_28d", True):
            issue_count += 1
        row["forecast_outlook_issue_count"] = issue_count

        # Review flag
        needs_review = row["primary_forecast_outlook_issue"] not in ("none",)
        row["forecast_review_required"] = needs_review

        # Recommended action
        issue = row["primary_forecast_outlook_issue"]
        if issue == "missing_forecast":
            row["recommended_forecast_review_action"] = "investigate_missing_forecast"
        elif issue == "invalid_forecast":
            row["recommended_forecast_review_action"] = "review_forecast_generation"
        elif issue == "insufficient_horizon":
            row["recommended_forecast_review_action"] = "extend_forecast_horizon"
        elif issue == "stale_forecast":
            row["recommended_forecast_review_action"] = "refresh_forecast"
        elif issue == "expected_decline":
            row["recommended_forecast_review_action"] = "investigate_declining_usage"
        elif issue == "expected_inactivity":
            row["recommended_forecast_review_action"] = "review_inactive_report"
        elif issue == "high_uncertainty":
            row["recommended_forecast_review_action"] = "review_model_uncertainty"
        elif issue == "conflicting_forecast_signals":
            row["recommended_forecast_review_action"] = "review_conflicting_signals"
        else:
            row["recommended_forecast_review_action"] = "no_action_required"

        row["forecast_outlook_reasons"] = build_forecast_outlook_reasons(row, cfg)

        rows.append(row)

    df_out = pd.DataFrame(rows)

    # Ensure all schema columns present
    for col in FORECAST_OUTLOOK_COLS:
        if col not in df_out.columns:
            df_out[col] = None

    df_out = df_out[FORECAST_OUTLOOK_COLS].sort_values("report_id").reset_index(drop=True)
    return df_out


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_report_forecast_outlook(df: pd.DataFrame) -> None:
    """Raise ValueError if the forecast outlook DataFrame fails integrity checks."""
    errors = []

    # Required columns
    missing_cols = set(FORECAST_OUTLOOK_COLS) - set(df.columns)
    if missing_cols:
        errors.append(f"missing_columns:{sorted(missing_cols)}")

    # Unique (forecast_run_id, report_id)
    if "forecast_run_id" in df.columns and "report_id" in df.columns:
        if df.duplicated(subset=["forecast_run_id", "report_id"]).any():
            errors.append("duplicate_(forecast_run_id,report_id)")

    # No user identifiers
    user_cols = {"user_id", "user_key", "email", "email_address", "display_name"}
    present_user_cols = user_cols & set(df.columns)
    if present_user_cols:
        errors.append(f"user_identifier_columns:{sorted(present_user_cols)}")

    # Allowed status values
    if "forecast_outlook_status" in df.columns:
        bad_statuses = set(df["forecast_outlook_status"].dropna()) - _ALLOWED_OUTLOOK_STATUSES
        if bad_statuses:
            errors.append(f"invalid_outlook_status:{sorted(bad_statuses)}")

    # Interval ordering: lower <= point <= upper
    for horizon in ("7d", "28d"):
        lt_col = f"forecast_lower_total_{horizon}"
        pt_col = f"forecast_total_{horizon}"
        ut_col = f"forecast_upper_total_{horizon}"
        if all(c in df.columns for c in (lt_col, pt_col, ut_col)):
            sub = df[[lt_col, pt_col, ut_col]].dropna()
            if len(sub) > 0:
                if (sub[lt_col] > sub[pt_col] + 1e-6).any():
                    errors.append(f"lower_bound_exceeds_point_{horizon}")
                if (sub[pt_col] > sub[ut_col] + 1e-6).any():
                    errors.append(f"point_exceeds_upper_bound_{horizon}")

    # Relative uncertainty null when forecast_total == 0
    if "relative_uncertainty_28d" in df.columns and "forecast_total_28d" in df.columns:
        zero_total = df["forecast_total_28d"] == 0
        non_null_unc = df["relative_uncertainty_28d"].notna()
        if (zero_total & non_null_unc).any():
            errors.append("relative_uncertainty_28d_non_null_when_total_zero")

    # Prohibited actions
    if "recommended_forecast_review_action" in df.columns:
        bad_actions = set(df["recommended_forecast_review_action"].dropna()) & _PROHIBITED_ACTIONS
        if bad_actions:
            errors.append(f"prohibited_action:{sorted(bad_actions)}")

    if errors:
        raise ValueError("forecast_outlook validation failed: " + "; ".join(errors))


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_report_forecast_outlook(
    df: pd.DataFrame,
    project_root: Path,
) -> Path:
    """Validate and write the outlook DataFrame."""
    validate_report_forecast_outlook(df)
    out_dir = project_root / "outputs" / "analytics"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "report_forecast_outlook.csv"
    df.to_csv(out_path, index=False)
    return out_path
