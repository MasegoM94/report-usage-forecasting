"""
Observation windows and report-history sufficiency framework.

Sprint 6 — User Analytics
Scope: window boundary computation, report history sufficiency classification,
       windowed filtering helpers, output persistence.

NOT in scope: active-user counts, returning-user metrics, cohorts, frequency,
              concentration, GenAI, Streamlit.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from src.analytics.privacy_policy import validate_no_direct_identifiers

# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------

WINDOW_BOUNDARIES_COLS = [
    "analytics_run_id",
    "generated_at",
    "analytics_timezone",
    "source_max_usage_date",
    "analytics_as_of_date",
    "as_of_date_policy",
    "latest_date_completeness_status",
    "window_7d_start",
    "window_7d_end",
    "window_28d_start",
    "window_28d_end",
    "previous_28d_start",
    "previous_28d_end",
    "window_90d_start",
    "window_90d_end",
    "previous_90d_start",
    "previous_90d_end",
    "pre_previous_28d_end",
]

REPORT_HISTORY_SUFFICIENCY_COLS = [
    "analytics_run_id",
    "generated_at",
    "analytics_as_of_date",
    "report_id",
    "report_name",
    "report_activation_date",
    "activation_date_status",
    "history_inference_method",
    "first_observed_usage_date",
    "latest_observed_usage_date",
    "available_calendar_history_days",
    "active_usage_days_lifetime",
    "has_any_valid_user_activity",
    "report_active_as_of_date",
    "source_coverage_start_date",
    "history_sufficient_7d",
    "history_sufficient_28d",
    "history_sufficient_previous_28d",
    "comparison_history_sufficient_28d",
    "history_sufficient_90d",
    "history_sufficient_previous_90d",
    "comparison_history_sufficient_90d",
    "activation_before_7d_window",
    "activation_before_28d_window",
    "activation_before_previous_28d_window",
    "activation_before_90d_window",
    "activation_before_previous_90d_window",
    "history_sufficiency_status",
    "history_sufficiency_reasons",
    "history_source_status",
]


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EngagementWindowConfig:
    LATEST_SOURCE_DATE_IS_COMPLETE: bool = True
    # If False, analytics_as_of_date = source_max_date - 1 day
    ALLOW_OBSERVED_HISTORY_ACTIVATION_FALLBACK: bool = True
    REQUIRE_KNOWN_ACTIVATION_FOR_COMPARISON_WINDOWS: bool = False
    PROJECT_ANALYTICS_TIMEZONE: str = "UTC"
    # Minimum coverage fraction required for a window to be usable
    REQUIRED_SOURCE_COVERAGE_FOR_WINDOWS: float = 1.0


# ---------------------------------------------------------------------------
# Boundary dataclass
# ---------------------------------------------------------------------------

@dataclass
class EngagementWindowBoundaries:
    analytics_run_id: str
    generated_at: datetime
    analytics_timezone: str
    source_max_usage_date: date
    analytics_as_of_date: date
    as_of_date_policy: str
    latest_date_completeness_status: str
    window_7d_start: date
    window_7d_end: date
    window_28d_start: date
    window_28d_end: date
    previous_28d_start: date
    previous_28d_end: date
    window_90d_start: date
    window_90d_end: date
    previous_90d_start: date
    previous_90d_end: date
    pre_previous_28d_end: date


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_usage_dates(mart_df: pd.DataFrame) -> pd.Series:
    """Return a Series of datetime.date parsed from mart_df['usage_date']."""
    return pd.to_datetime(mart_df["usage_date"], errors="coerce").dt.date


def _safe_date(val) -> Optional[date]:
    """Convert a value to datetime.date or None."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, date):
        return val
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Core resolution
# ---------------------------------------------------------------------------

def resolve_analytics_as_of_date(
    mart_df: pd.DataFrame,
    cfg: EngagementWindowConfig,
    analytics_run_id: str,
) -> tuple[date, str, str, date]:
    """
    Determine the analytics_as_of_date from the mart data.

    Returns
    -------
    (analytics_as_of_date, as_of_date_policy, completeness_status, source_max_usage_date)

    Does NOT use datetime.now() or date.today() as the anchor.
    """
    if mart_df.empty:
        raise ValueError("No valid usage dates in mart")

    parsed = _parse_usage_dates(mart_df).dropna()
    if parsed.empty:
        raise ValueError("No valid usage dates in mart")

    source_max_usage_date: date = parsed.max()

    if cfg.LATEST_SOURCE_DATE_IS_COMPLETE:
        analytics_as_of_date = source_max_usage_date
        as_of_date_policy = "source_max_date"
        completeness_status = "complete"
    else:
        analytics_as_of_date = source_max_usage_date - timedelta(days=1)
        as_of_date_policy = "source_max_date_minus_1_assumed_incomplete"
        completeness_status = "assumed_incomplete"

    return analytics_as_of_date, as_of_date_policy, completeness_status, source_max_usage_date


def build_engagement_window_boundaries(
    mart_df: pd.DataFrame,
    cfg: EngagementWindowConfig,
    analytics_run_id: str,
) -> EngagementWindowBoundaries:
    """
    Compute all observation window boundaries from the mart data.
    """
    (
        analytics_as_of_date,
        as_of_date_policy,
        completeness_status,
        source_max_usage_date,
    ) = resolve_analytics_as_of_date(mart_df, cfg, analytics_run_id)

    aod = analytics_as_of_date

    window_7d_end = aod
    window_7d_start = aod - timedelta(days=6)

    window_28d_end = aod
    window_28d_start = aod - timedelta(days=27)

    previous_28d_end = window_28d_start - timedelta(days=1)
    previous_28d_start = previous_28d_end - timedelta(days=27)

    window_90d_end = aod
    window_90d_start = aod - timedelta(days=89)

    previous_90d_end = window_90d_start - timedelta(days=1)
    previous_90d_start = previous_90d_end - timedelta(days=89)

    pre_previous_28d_end = previous_28d_start - timedelta(days=1)

    boundaries = EngagementWindowBoundaries(
        analytics_run_id=analytics_run_id,
        generated_at=datetime.utcnow(),
        analytics_timezone=cfg.PROJECT_ANALYTICS_TIMEZONE,
        source_max_usage_date=source_max_usage_date,
        analytics_as_of_date=aod,
        as_of_date_policy=as_of_date_policy,
        latest_date_completeness_status=completeness_status,
        window_7d_start=window_7d_start,
        window_7d_end=window_7d_end,
        window_28d_start=window_28d_start,
        window_28d_end=window_28d_end,
        previous_28d_start=previous_28d_start,
        previous_28d_end=previous_28d_end,
        window_90d_start=window_90d_start,
        window_90d_end=window_90d_end,
        previous_90d_start=previous_90d_start,
        previous_90d_end=previous_90d_end,
        pre_previous_28d_end=pre_previous_28d_end,
    )

    validate_engagement_window_boundaries(boundaries)
    return boundaries


# ---------------------------------------------------------------------------
# Window filtering helpers
# ---------------------------------------------------------------------------

def filter_report_user_window(
    df: pd.DataFrame,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Return rows of mart_df where start_date <= usage_date <= end_date (inclusive).
    Does not modify the original DataFrame.
    """
    df = df.copy()
    parsed = _parse_usage_dates(df)
    mask = (parsed >= start_date) & (parsed <= end_date)
    return df[mask].reset_index(drop=True)


def assign_engagement_window(
    df: pd.DataFrame,
    boundaries: EngagementWindowBoundaries,
) -> pd.DataFrame:
    """
    Add an ``engagement_window`` column assigning each row to the most specific
    containing window. Windows nest (7d ⊂ 28d ⊂ 90d), so the most-specific
    label is used. Use filter_report_user_window (not the label column) as the
    primary interface for computing windowed metrics.

    Labels
    ------
    recent_7d               — within 7d window (most specific)
    recent_28d_not_7d       — within 28d but outside 7d
    recent_90d_not_28d      — within 90d but outside 28d
    previous_28d            — within previous 28d window
    previous_90d_not_28d    — within previous 90d but outside previous 28d
    pre_previous_history    — before pre_previous_28d_end (inclusive)
    outside_windows         — anything else (e.g. future relative to as_of)
    """
    df = df.copy()
    parsed = _parse_usage_dates(df)

    b = boundaries
    labels = pd.Series("outside_windows", index=df.index, dtype=object)

    labels[parsed <= b.pre_previous_28d_end] = "pre_previous_history"
    labels[
        (parsed >= b.previous_90d_start) & (parsed <= b.previous_90d_end)
        & ~((parsed >= b.previous_28d_start) & (parsed <= b.previous_28d_end))
    ] = "previous_90d_not_28d"
    labels[
        (parsed >= b.previous_28d_start) & (parsed <= b.previous_28d_end)
    ] = "previous_28d"
    labels[
        (parsed >= b.window_90d_start) & (parsed <= b.window_90d_end)
        & ~((parsed >= b.window_28d_start) & (parsed <= b.window_28d_end))
    ] = "recent_90d_not_28d"
    labels[
        (parsed >= b.window_28d_start) & (parsed <= b.window_28d_end)
        & ~((parsed >= b.window_7d_start) & (parsed <= b.window_7d_end))
    ] = "recent_28d_not_7d"
    labels[
        (parsed >= b.window_7d_start) & (parsed <= b.window_7d_end)
    ] = "recent_7d"

    df["engagement_window"] = labels
    return df


# ---------------------------------------------------------------------------
# Report history sufficiency
# ---------------------------------------------------------------------------

def _compute_sufficiency_row(
    report_id: str,
    report_name,
    raw_activation_date,
    mart_rows: pd.DataFrame,
    quality_row: Optional[pd.Series],
    boundaries: EngagementWindowBoundaries,
    cfg: EngagementWindowConfig,
    analytics_run_id: str,
    generated_at: datetime,
    global_source_coverage_start: date,
    source_coverage_start_date_param: Optional[date],
) -> dict:
    aod = boundaries.analytics_as_of_date
    generated_at_str = generated_at.isoformat()

    # ── Source coverage start ──────────────────────────────────────────────
    source_cov_start = source_coverage_start_date_param or global_source_coverage_start

    # ── Mart observation for this report ──────────────────────────────────
    if mart_rows.empty:
        first_obs = None
        latest_obs = None
        active_usage_days = 0
        has_activity = False
        if quality_row is not None:
            history_source_status = "mart_excluded"
        else:
            history_source_status = "mart_absent"
    else:
        dates_in_mart = _parse_usage_dates(mart_rows).dropna()
        first_obs = dates_in_mart.min() if not dates_in_mart.empty else None
        latest_obs = dates_in_mart.max() if not dates_in_mart.empty else None
        active_usage_days = dates_in_mart.nunique()
        has_activity = active_usage_days > 0
        history_source_status = "mart_available"

    # ── Activation date resolution ─────────────────────────────────────────
    activation_date = _safe_date(raw_activation_date)
    activation_date_status: str
    history_inference_method: str

    if activation_date is not None:
        activation_date_status = "known"
        history_inference_method = "known_activation_date"
    elif cfg.ALLOW_OBSERVED_HISTORY_ACTIVATION_FALLBACK and first_obs is not None:
        activation_date = first_obs
        activation_date_status = "missing_observed_history_fallback"
        history_inference_method = "observed_history_fallback"
    else:
        activation_date = None
        activation_date_status = "missing_insufficient_evidence"
        history_inference_method = "none"

    # ── report_active_as_of_date ───────────────────────────────────────────
    report_active_as_of_date: bool
    if activation_date is not None:
        report_active_as_of_date = activation_date <= aod
    else:
        report_active_as_of_date = False

    # ── available_calendar_history_days ────────────────────────────────────
    if activation_date is not None and report_active_as_of_date:
        eff_start = max(activation_date, source_cov_start)
        available_calendar_history_days = max(0, (aod - eff_start).days + 1)
    else:
        available_calendar_history_days = 0

    # ── Per-window activation flags ────────────────────────────────────────
    def _before_or_on(act: Optional[date], target: date) -> bool:
        return act is not None and act <= target

    activation_before_7d = _before_or_on(activation_date, boundaries.window_7d_start)
    activation_before_28d = _before_or_on(activation_date, boundaries.window_28d_start)
    activation_before_prev28d = _before_or_on(activation_date, boundaries.previous_28d_start)
    activation_before_90d = _before_or_on(activation_date, boundaries.window_90d_start)
    activation_before_prev90d = _before_or_on(activation_date, boundaries.previous_90d_start)

    # ── Source coverage per-window flags ───────────────────────────────────
    src_covers_7d = source_cov_start <= boundaries.window_7d_start
    src_covers_28d = source_cov_start <= boundaries.window_28d_start
    src_covers_prev28d = source_cov_start <= boundaries.previous_28d_start
    src_covers_90d = source_cov_start <= boundaries.window_90d_start
    src_covers_prev90d = source_cov_start <= boundaries.previous_90d_start

    # ── Sufficiency flags ──────────────────────────────────────────────────
    if activation_date is None:
        # All false — no evidence
        suf_7d = suf_28d = suf_prev28d = suf_90d = suf_prev90d = False
    else:
        suf_7d = activation_before_7d and src_covers_7d
        suf_28d = activation_before_28d and src_covers_28d
        suf_prev28d = activation_before_prev28d and src_covers_prev28d
        suf_90d = activation_before_90d and src_covers_90d
        suf_prev90d = activation_before_prev90d and src_covers_prev90d

    comparison_28d = suf_28d and suf_prev28d
    comparison_90d = suf_90d and suf_prev90d

    # ── Data quality ──────────────────────────────────────────────────────
    dq_status: Optional[str] = None
    if quality_row is not None and "data_quality_status" in quality_row.index:
        dq_status = quality_row["data_quality_status"]

    # ── history_sufficiency_status ─────────────────────────────────────────
    no_valid_data = (dq_status == "no_valid_user_data") and not has_activity

    if comparison_90d:
        status = "complete_90d_history"
    elif comparison_28d:
        status = "complete_28d_comparison_history"
    elif suf_28d:
        status = "complete_recent_28d_only"
    elif suf_7d:
        status = "complete_recent_7d_only"
    elif no_valid_data:
        status = "no_valid_user_data"
    else:
        status = "insufficient_history"

    # ── history_sufficiency_reasons ────────────────────────────────────────
    reasons = []

    # 1. Metadata validity
    raw_act = _safe_date(raw_activation_date)
    if raw_act is not None and raw_act > aod:
        reasons.append("activation_date_after_as_of_date")

    # 2. Activation age constraints
    if activation_date is None:
        reasons.append("no_activation_date_available")
    else:
        if not activation_before_7d:
            reasons.append("activation_too_recent_for_7d_window")
        if not activation_before_28d:
            reasons.append("activation_too_recent_for_28d_window")
        if not activation_before_prev28d:
            reasons.append("activation_too_recent_for_previous_28d_window")
        if not activation_before_90d:
            reasons.append("activation_too_recent_for_90d_window")
        if not activation_before_prev90d:
            reasons.append("activation_too_recent_for_previous_90d_window")

    # 3. Source coverage constraints
    if not src_covers_7d:
        reasons.append("source_coverage_insufficient_for_7d_window")
    if not src_covers_28d:
        reasons.append("source_coverage_insufficient_for_28d_window")
    if not src_covers_prev28d:
        reasons.append("source_coverage_insufficient_for_previous_28d_window")
    if not src_covers_90d:
        reasons.append("source_coverage_insufficient_for_90d_window")
    if not src_covers_prev90d:
        reasons.append("source_coverage_insufficient_for_previous_90d_window")

    # 4. Valid-user-data availability
    if no_valid_data:
        reasons.append("no_valid_user_data_in_source")

    # 5. Recent-window sufficiency
    if suf_7d:
        reasons.append("7d_window_sufficient")
    if suf_28d:
        reasons.append("28d_window_sufficient")

    # 6. Comparison-window sufficiency
    if comparison_28d:
        reasons.append("28d_comparison_sufficient")
    if comparison_90d:
        reasons.append("90d_comparison_sufficient")

    # 7. No-activity evidence
    if (suf_7d or suf_28d or suf_90d) and not has_activity:
        reasons.append("no_user_activity_observed_in_sufficient_window")

    # 8. Inference method
    if activation_date_status == "missing_observed_history_fallback":
        reasons.append("activation_date_inferred_from_observed_history")

    reasons_str = "|".join(reasons) if reasons else "none"

    return {
        "analytics_run_id": analytics_run_id,
        "generated_at": generated_at_str,
        "analytics_as_of_date": str(aod),
        "report_id": report_id,
        "report_name": report_name if report_name is not None else "",
        "report_activation_date": str(activation_date) if (activation_date is not None and activation_date_status == "known") else None,
        "activation_date_status": activation_date_status,
        "history_inference_method": history_inference_method,
        "first_observed_usage_date": str(first_obs) if first_obs is not None else None,
        "latest_observed_usage_date": str(latest_obs) if latest_obs is not None else None,
        "available_calendar_history_days": available_calendar_history_days,
        "active_usage_days_lifetime": active_usage_days,
        "has_any_valid_user_activity": has_activity,
        "report_active_as_of_date": report_active_as_of_date,
        "source_coverage_start_date": str(source_cov_start),
        "history_sufficient_7d": suf_7d,
        "history_sufficient_28d": suf_28d,
        "history_sufficient_previous_28d": suf_prev28d,
        "comparison_history_sufficient_28d": comparison_28d,
        "history_sufficient_90d": suf_90d,
        "history_sufficient_previous_90d": suf_prev90d,
        "comparison_history_sufficient_90d": comparison_90d,
        "activation_before_7d_window": activation_before_7d,
        "activation_before_28d_window": activation_before_28d,
        "activation_before_previous_28d_window": activation_before_prev28d,
        "activation_before_90d_window": activation_before_90d,
        "activation_before_previous_90d_window": activation_before_prev90d,
        "history_sufficiency_status": status,
        "history_sufficiency_reasons": reasons_str,
        "history_source_status": history_source_status,
    }


def build_report_history_sufficiency(
    report_meta_df: pd.DataFrame,
    mart_df: pd.DataFrame,
    quality_df: pd.DataFrame,
    boundaries: EngagementWindowBoundaries,
    cfg: EngagementWindowConfig,
    analytics_run_id: str,
    source_coverage_start_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    Build the report history sufficiency table.

    Uses report_meta_df as the spine — all reports appear in the output,
    including those with no user activity. Does not expose user-level data.

    Parameters
    ----------
    report_meta_df : spine of reports (must have report_id; may have report_name,
                     report_activation_date or launch_date)
    mart_df : mart_report_user_daily — must have report_id, usage_date
    quality_df : report_user_data_quality — must have report_id, data_quality_status
    boundaries : pre-computed EngagementWindowBoundaries
    cfg : EngagementWindowConfig
    analytics_run_id : UUID string for this analytics run
    source_coverage_start_date : explicit override; if None, inferred from mart_df
    """
    generated_at = datetime.utcnow()

    # ── Resolve global source coverage start ──────────────────────────────
    if source_coverage_start_date is not None:
        global_source_cov_start = source_coverage_start_date
    elif not mart_df.empty and "usage_date" in mart_df.columns:
        parsed_all = _parse_usage_dates(mart_df).dropna()
        global_source_cov_start = parsed_all.min() if not parsed_all.empty else boundaries.analytics_as_of_date
    else:
        global_source_cov_start = boundaries.analytics_as_of_date

    # ── Normalise report_meta_df columns ──────────────────────────────────
    meta = report_meta_df.copy()
    if "report_activation_date" not in meta.columns:
        # Try launch_date from dim_report.csv
        if "launch_date" in meta.columns:
            meta["report_activation_date"] = meta["launch_date"]
        else:
            meta["report_activation_date"] = None

    if "report_name" not in meta.columns:
        meta["report_name"] = None

    # Index mart and quality by report_id for fast lookup
    mart_by_report: dict[str, pd.DataFrame] = {}
    if not mart_df.empty and "report_id" in mart_df.columns:
        for rid, grp in mart_df.groupby("report_id"):
            mart_by_report[rid] = grp

    quality_by_report: dict[str, pd.Series] = {}
    if not quality_df.empty and "report_id" in quality_df.columns:
        for _, row in quality_df.iterrows():
            quality_by_report[str(row["report_id"])] = row

    # ── Build one row per report ──────────────────────────────────────────
    rows = []
    for _, meta_row in meta.sort_values("report_id").iterrows():
        report_id = str(meta_row["report_id"])
        report_name = meta_row.get("report_name")
        raw_activation = meta_row.get("report_activation_date")

        mart_rows = mart_by_report.get(report_id, pd.DataFrame())
        quality_row = quality_by_report.get(report_id, None)

        try:
            row_dict = _compute_sufficiency_row(
                report_id=report_id,
                report_name=report_name,
                raw_activation_date=raw_activation,
                mart_rows=mart_rows,
                quality_row=quality_row,
                boundaries=boundaries,
                cfg=cfg,
                analytics_run_id=analytics_run_id,
                generated_at=generated_at,
                global_source_coverage_start=global_source_cov_start,
                source_coverage_start_date_param=source_coverage_start_date,
            )
        except Exception as exc:
            # Fail gracefully — record the error but don't stop the pipeline
            row_dict = {col: None for col in REPORT_HISTORY_SUFFICIENCY_COLS}
            row_dict["analytics_run_id"] = analytics_run_id
            row_dict["generated_at"] = generated_at.isoformat()
            row_dict["analytics_as_of_date"] = str(boundaries.analytics_as_of_date)
            row_dict["report_id"] = report_id
            row_dict["history_sufficiency_status"] = "insufficient_history"
            row_dict["history_sufficiency_reasons"] = f"calculation_failure:{exc}"
            row_dict["history_source_status"] = "mart_absent"

        rows.append(row_dict)

    result = pd.DataFrame(rows, columns=REPORT_HISTORY_SUFFICIENCY_COLS)
    validate_report_history_sufficiency(result)
    return result


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_engagement_window_boundaries(boundaries: EngagementWindowBoundaries) -> None:
    """Validate all window boundary constraints. Raises ValueError on failure."""
    b = boundaries

    # 7d window: exactly 7 inclusive days
    days_7 = (b.window_7d_end - b.window_7d_start).days + 1
    if days_7 != 7:
        raise ValueError(f"7d window has {days_7} days, expected 7")

    # 28d window: exactly 28 inclusive days
    days_28 = (b.window_28d_end - b.window_28d_start).days + 1
    if days_28 != 28:
        raise ValueError(f"28d window has {days_28} days, expected 28")

    # previous 28d window: exactly 28 inclusive days
    days_prev28 = (b.previous_28d_end - b.previous_28d_start).days + 1
    if days_prev28 != 28:
        raise ValueError(f"previous_28d window has {days_prev28} days, expected 28")

    # 90d window: exactly 90 inclusive days
    days_90 = (b.window_90d_end - b.window_90d_start).days + 1
    if days_90 != 90:
        raise ValueError(f"90d window has {days_90} days, expected 90")

    # previous 90d: exactly 90 inclusive days
    days_prev90 = (b.previous_90d_end - b.previous_90d_start).days + 1
    if days_prev90 != 90:
        raise ValueError(f"previous_90d window has {days_prev90} days, expected 90")

    # No gap/overlap between previous_28d and recent_28d
    if b.previous_28d_end != b.window_28d_start - timedelta(days=1):
        raise ValueError(
            f"previous_28d_end ({b.previous_28d_end}) must be exactly 1 day before "
            f"window_28d_start ({b.window_28d_start})"
        )

    # No gap/overlap between previous_90d and recent_90d
    if b.previous_90d_end != b.window_90d_start - timedelta(days=1):
        raise ValueError(
            f"previous_90d_end ({b.previous_90d_end}) must be exactly 1 day before "
            f"window_90d_start ({b.window_90d_start})"
        )

    # pre_previous_28d_end
    if b.pre_previous_28d_end != b.previous_28d_start - timedelta(days=1):
        raise ValueError(
            f"pre_previous_28d_end ({b.pre_previous_28d_end}) must be 1 day before "
            f"previous_28d_start ({b.previous_28d_start})"
        )

    # analytics_as_of_date <= source_max_usage_date
    if b.analytics_as_of_date > b.source_max_usage_date:
        raise ValueError(
            f"analytics_as_of_date ({b.analytics_as_of_date}) > "
            f"source_max_usage_date ({b.source_max_usage_date})"
        )

    # No overlap: previous_28d must end before recent_28d starts
    if not (b.previous_28d_end < b.window_28d_start):
        raise ValueError(
            f"previous_28d_end ({b.previous_28d_end}) must be < window_28d_start ({b.window_28d_start})"
        )


def validate_report_history_sufficiency(df: pd.DataFrame) -> None:
    """Validate the report history sufficiency DataFrame. Raises ValueError on failure."""

    # 1. Required columns present
    missing_cols = [c for c in REPORT_HISTORY_SUFFICIENCY_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in sufficiency output: {missing_cols}")

    if df.empty:
        return

    # 2. No duplicate (analytics_run_id, report_id)
    dupes = df.duplicated(subset=["analytics_run_id", "report_id"])
    if dupes.any():
        bad = df.loc[dupes, "report_id"].tolist()
        raise ValueError(f"Duplicate (analytics_run_id, report_id) rows: {bad}")

    # 3. Comparison flags consistent with component flags
    if "comparison_history_sufficient_28d" in df.columns:
        bad_28d = df[
            df["comparison_history_sufficient_28d"] == True
        ].loc[
            lambda r: (r["history_sufficient_28d"] == False) | (r["history_sufficient_previous_28d"] == False)
        ]
        if not bad_28d.empty:
            raise ValueError(
                f"comparison_history_sufficient_28d=True but component flag False for: "
                f"{bad_28d['report_id'].tolist()}"
            )

    if "comparison_history_sufficient_90d" in df.columns:
        bad_90d = df[
            df["comparison_history_sufficient_90d"] == True
        ].loc[
            lambda r: (r["history_sufficient_90d"] == False) | (r["history_sufficient_previous_90d"] == False)
        ]
        if not bad_90d.empty:
            raise ValueError(
                f"comparison_history_sufficient_90d=True but component flag False for: "
                f"{bad_90d['report_id'].tolist()}"
            )

    # 4. history_sufficiency_status must be one of the known values
    valid_statuses = {
        "complete_90d_history",
        "complete_28d_comparison_history",
        "complete_recent_28d_only",
        "complete_recent_7d_only",
        "no_valid_user_data",
        "insufficient_history",
    }
    bad_status = df.loc[df["history_sufficiency_status"].notna(), "history_sufficiency_status"]
    invalid = bad_status[~bad_status.isin(valid_statuses)]
    if not invalid.empty:
        raise ValueError(f"Invalid history_sufficiency_status values: {invalid.unique().tolist()}")

    # 5. history_source_status must be one of known values
    valid_source = {"mart_available", "mart_excluded", "mart_absent"}
    bad_src = df.loc[df["history_source_status"].notna(), "history_source_status"]
    inv_src = bad_src[~bad_src.isin(valid_source)]
    if not inv_src.empty:
        raise ValueError(f"Invalid history_source_status values: {inv_src.unique().tolist()}")

    # 6. No prohibited user identifier columns
    validate_no_direct_identifiers(df, context="report_history_sufficiency")

    # 7. available_calendar_history_days >= 0
    if "available_calendar_history_days" in df.columns:
        neg = df[df["available_calendar_history_days"].notna() & (df["available_calendar_history_days"] < 0)]
        if not neg.empty:
            raise ValueError(f"Negative available_calendar_history_days for: {neg['report_id'].tolist()}")

    # 8. activation_date_status must be one of known values
    valid_act_status = {
        "known",
        "missing_observed_history_fallback",
        "missing_insufficient_evidence",
    }
    bad_act = df.loc[df["activation_date_status"].notna(), "activation_date_status"]
    inv_act = bad_act[~bad_act.isin(valid_act_status)]
    if not inv_act.empty:
        raise ValueError(f"Invalid activation_date_status values: {inv_act.unique().tolist()}")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_engagement_window_outputs(
    boundaries: EngagementWindowBoundaries,
    sufficiency_df: pd.DataFrame,
    project_root: Path,
) -> dict[str, Path]:
    """
    Write engagement window boundaries and report history sufficiency to CSV.

    Validates both outputs before writing. Returns dict with keys
    'boundaries' and 'sufficiency'.
    """
    # Validate before writing
    validate_engagement_window_boundaries(boundaries)
    validate_report_history_sufficiency(sufficiency_df)

    output_dir = project_root / "outputs" / "analytics"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert boundaries to single-row DataFrame
    boundaries_dict = {
        "analytics_run_id": boundaries.analytics_run_id,
        "generated_at": boundaries.generated_at.isoformat(),
        "analytics_timezone": boundaries.analytics_timezone,
        "source_max_usage_date": str(boundaries.source_max_usage_date),
        "analytics_as_of_date": str(boundaries.analytics_as_of_date),
        "as_of_date_policy": boundaries.as_of_date_policy,
        "latest_date_completeness_status": boundaries.latest_date_completeness_status,
        "window_7d_start": str(boundaries.window_7d_start),
        "window_7d_end": str(boundaries.window_7d_end),
        "window_28d_start": str(boundaries.window_28d_start),
        "window_28d_end": str(boundaries.window_28d_end),
        "previous_28d_start": str(boundaries.previous_28d_start),
        "previous_28d_end": str(boundaries.previous_28d_end),
        "window_90d_start": str(boundaries.window_90d_start),
        "window_90d_end": str(boundaries.window_90d_end),
        "previous_90d_start": str(boundaries.previous_90d_start),
        "previous_90d_end": str(boundaries.previous_90d_end),
        "pre_previous_28d_end": str(boundaries.pre_previous_28d_end),
    }
    boundaries_df = pd.DataFrame([boundaries_dict], columns=WINDOW_BOUNDARIES_COLS)

    boundaries_path = output_dir / "engagement_window_boundaries.csv"
    sufficiency_path = output_dir / "report_engagement_history_sufficiency.csv"

    boundaries_df.to_csv(boundaries_path, index=False)
    sufficiency_df.to_csv(sufficiency_path, index=False)

    return {"boundaries": boundaries_path, "sufficiency": sufficiency_path}
