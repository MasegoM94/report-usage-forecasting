"""
Report-level user-concentration and dependency metrics.

Sprint 6 — User Analytics
Scope: HHI, top-user shares, effective user count, concentration direction,
       dependency change status, privacy suppression, output persistence.

NOT in scope: overall engagement status, final engagement mart, GenAI, Streamlit.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.analytics.privacy_policy import (
    PROHIBITED_OUTPUT_COLUMNS,
    validate_no_direct_identifiers,
    detect_email_like_values,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ConcentrationMetricsConfig:
    # Privacy thresholds
    MIN_USERS_FOR_CONCENTRATION_METRICS: int = 5
    MIN_USERS_FOR_TOP_1_SHARE: int = 5
    MIN_USERS_FOR_TOP_3_SHARE: int = 5
    MIN_USERS_FOR_HHI: int = 5
    ALLOW_SINGLE_USER_DEPENDENCY_STATUS: bool = False
    # Classification thresholds
    BROAD_HHI_MAX: float = 0.15
    MODERATE_HHI_MAX: float = 0.35
    HIGH_CONCENTRATION_HHI_MIN: float = 0.35
    TOP1_WARNING_SHARE: float = 0.40
    TOP1_POOR_SHARE: float = 0.70
    TOP3_WARNING_SHARE: float = 0.60
    TOP3_POOR_SHARE: float = 0.85
    EFFECTIVE_USER_SHARE_WARNING: float = 0.40
    EFFECTIVE_USER_SHARE_POOR: float = 0.20
    # Change thresholds
    CONCENTRATION_CHANGE_WARNING: float = 0.05
    CONCENTRATION_CHANGE_POOR: float = 0.10
    MIN_USERS_FOR_CONCENTRATION_STATUS: int = 3
    # Percentile
    PERCENTILE_INTERPOLATION_METHOD: str = "linear"


# ---------------------------------------------------------------------------
# Schema constant
# ---------------------------------------------------------------------------

REPORT_USER_CONCENTRATION_METRICS_COLS = [
    # Identity and evidence
    "analytics_run_id", "generated_at", "analytics_as_of_date",
    "report_id", "report_name",
    "history_sufficient_28d", "history_sufficient_previous_28d",
    "comparison_history_sufficient_28d",
    "history_sufficient_90d", "history_sufficient_previous_90d",
    "comparison_history_sufficient_90d",
    "has_any_valid_user_activity", "user_data_quality_status",
    "privacy_suppression_status",
    # Recent 28d
    "active_user_count_28d", "total_views_28d",
    "top_1_user_view_share_28d", "top_3_users_view_share_28d",
    "top_10pct_users_view_share_28d", "top_10pct_user_count_28d",
    "user_view_hhi_28d", "effective_user_count_28d", "effective_user_share_28d",
    "largest_user_view_count_28d", "third_largest_user_view_count_28d",
    "concentration_status_28d",
    # Previous 28d
    "active_user_count_previous_28d", "total_views_previous_28d",
    "top_1_user_view_share_previous_28d", "top_3_users_view_share_previous_28d",
    "top_10pct_users_view_share_previous_28d", "top_10pct_user_count_previous_28d",
    "user_view_hhi_previous_28d", "effective_user_count_previous_28d",
    "effective_user_share_previous_28d",
    "concentration_status_previous_28d",
    # Recent 90d
    "active_user_count_90d", "total_views_90d",
    "top_1_user_view_share_90d", "top_3_users_view_share_90d",
    "top_10pct_users_view_share_90d", "top_10pct_user_count_90d",
    "user_view_hhi_90d", "effective_user_count_90d", "effective_user_share_90d",
    "concentration_status_90d",
    # Change metrics
    "top_1_share_change_28d", "top_3_share_change_28d",
    "top_10pct_share_change_28d", "hhi_change_28d",
    "effective_user_count_change_28d", "effective_user_share_change_28d",
    "concentration_direction", "dependency_change_status",
    # Overall status
    "concentration_status", "concentration_evidence_status", "concentration_reasons",
    # Privacy
    "concentration_privacy_suppressed", "concentration_privacy_suppression_reason",
    "suppressed_concentration_fields",
]

_VALID_CONCENTRATION_STATUSES = {
    "broadly_distributed",
    "moderately_concentrated",
    "highly_concentrated",
    "single_user_dependent",
    "no_recent_activity",
    "privacy_suppressed",
    "insufficient_history",
    "no_valid_user_data",
    "calculation_failed",
}

_VALID_CONCENTRATION_DIRECTIONS = {
    "broadening", "stable", "concentrating", "newly_active", "inactive",
    "insufficient_history", "privacy_suppressed", "no_valid_user_data",
}

_VALID_DEPENDENCY_CHANGE_STATUSES = {
    "dependency_decreasing", "stable_dependency", "dependency_increasing",
    "insufficient_evidence", "privacy_suppressed",
}


# ---------------------------------------------------------------------------
# Date parsing helper
# ---------------------------------------------------------------------------

def _parse_date(val) -> Optional[date]:
    if val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, date):
        return val
    return pd.to_datetime(str(val)).date()


def _to_float_or_none(val) -> Optional[float]:
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return float(val)


# ---------------------------------------------------------------------------
# Core computation functions
# ---------------------------------------------------------------------------

def aggregate_user_window_views(
    mart_df: pd.DataFrame,
    report_id: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """
    Filter mart_df to report_id and inclusive [start_date, end_date].
    Group by user_key -> sum daily_views -> user_window_views.
    Sort by user_window_views DESCENDING, then user_key ASCENDING (deterministic).
    Returns DataFrame with columns: user_key, user_window_views, rank (1-based).
    Returns empty DataFrame with those columns if no rows.
    """
    empty = pd.DataFrame(columns=["user_key", "user_window_views", "rank"])

    if mart_df.empty:
        return empty

    df = mart_df.copy()
    df["_usage_date"] = pd.to_datetime(df["usage_date"], errors="coerce").dt.date

    mask = (
        (df["report_id"] == report_id)
        & (df["_usage_date"] >= start_date)
        & (df["_usage_date"] <= end_date)
    )
    filtered = df[mask]

    if filtered.empty:
        return empty

    grouped = (
        filtered.groupby("user_key", sort=False)
        .agg(user_window_views=("daily_views", "sum"))
        .reset_index()
    )

    # Sort: views descending, user_key ascending for deterministic tie-break
    grouped = grouped.sort_values(
        ["user_window_views", "user_key"],
        ascending=[False, True],
    ).reset_index(drop=True)

    grouped["rank"] = range(1, len(grouped) + 1)

    return grouped[["user_key", "user_window_views", "rank"]]


def calculate_top_user_shares(
    user_views_df: pd.DataFrame,
    total_views: int,
) -> dict:
    """
    Calculate top-user share metrics from sorted user_views_df.

    Returns dict with active_user_count, total_views, top shares, and counts.
    When user_views_df is empty: all null except active_user_count=0, total_views=0.
    When total_views=0: all shares null.
    """
    if user_views_df.empty:
        return {
            "active_user_count": 0,
            "total_views": 0,
            "top_1_user_view_share": None,
            "largest_user_view_count": None,
            "top_3_users_view_share": None,
            "third_largest_user_view_count": None,
            "top_10pct_user_count": 0,
            "top_10pct_users_view_share": None,
        }

    n = len(user_views_df)

    if total_views == 0:
        top_10pct_count = max(1, math.ceil(0.10 * n)) if n > 0 else 0
        return {
            "active_user_count": n,
            "total_views": 0,
            "top_1_user_view_share": None,
            "largest_user_view_count": int(user_views_df.iloc[0]["user_window_views"]),
            "top_3_users_view_share": None,
            "third_largest_user_view_count": (
                int(user_views_df.iloc[2]["user_window_views"]) if n >= 3 else None
            ),
            "top_10pct_user_count": top_10pct_count,
            "top_10pct_users_view_share": None,
        }

    top_10pct_count = max(1, math.ceil(0.10 * n))
    top_1_views = int(user_views_df.iloc[0]["user_window_views"])
    top_3_views = int(user_views_df.head(3)["user_window_views"].sum())
    top_10pct_views = int(user_views_df.head(top_10pct_count)["user_window_views"].sum())

    return {
        "active_user_count": n,
        "total_views": total_views,
        "top_1_user_view_share": top_1_views / total_views,
        "largest_user_view_count": top_1_views,
        "top_3_users_view_share": top_3_views / total_views,
        "third_largest_user_view_count": (
            int(user_views_df.iloc[2]["user_window_views"]) if n >= 3 else None
        ),
        "top_10pct_user_count": top_10pct_count,
        "top_10pct_users_view_share": top_10pct_views / total_views,
    }


def calculate_user_view_hhi(
    user_views_df: pd.DataFrame,
    total_views: int,
) -> dict:
    """
    Calculate Herfindahl-Hirschman Index for user view concentration.

    Mathematical property: for N equal users, HHI = 1/N.
    Returns dict with user_view_hhi, effective_user_count, effective_user_share.
    All null when empty or total_views=0.
    """
    null_result = {
        "user_view_hhi": None,
        "effective_user_count": None,
        "effective_user_share": None,
    }

    if user_views_df.empty or total_views == 0:
        return null_result

    n = len(user_views_df)
    shares = user_views_df["user_window_views"] / total_views
    hhi = float((shares ** 2).sum())

    effective_user_count = (1.0 / hhi) if hhi > 0 else None
    effective_user_share = (
        effective_user_count / n if (effective_user_count is not None and n > 0) else None
    )

    return {
        "user_view_hhi": hhi,
        "effective_user_count": effective_user_count,
        "effective_user_share": effective_user_share,
    }


def calculate_concentration_change(
    recent: dict,
    previous: dict,
    comparison_sufficient: bool,
) -> dict:
    """
    Compute change metrics comparing recent vs previous 28d window.
    All null if not comparison_sufficient.
    """
    null_result = {
        "top_1_share_change_28d": None,
        "top_3_share_change_28d": None,
        "top_10pct_share_change_28d": None,
        "hhi_change_28d": None,
        "effective_user_count_change_28d": None,
        "effective_user_share_change_28d": None,
    }

    if not comparison_sufficient:
        return null_result

    def _diff(key_recent, key_prev=None):
        k_prev = key_prev or key_recent
        r = recent.get(key_recent)
        p = previous.get(k_prev)
        if r is None or p is None:
            return None
        return r - p

    return {
        "top_1_share_change_28d": _diff("top_1_user_view_share"),
        "top_3_share_change_28d": _diff("top_3_users_view_share"),
        "top_10pct_share_change_28d": _diff("top_10pct_users_view_share"),
        "hhi_change_28d": _diff("user_view_hhi"),
        "effective_user_count_change_28d": _diff("effective_user_count"),
        "effective_user_share_change_28d": _diff("effective_user_share"),
    }


def classify_concentration_direction(
    recent_hhi: Optional[float],
    previous_hhi: Optional[float],
    hhi_change: Optional[float],
    recent_users: int,
    previous_users: int,
    comparison_sufficient: bool,
    has_valid_data: bool,
    is_suppressed: bool,
    cfg: ConcentrationMetricsConfig,
    top_1_share_change: Optional[float] = None,
) -> tuple[str, str]:
    """
    Returns (concentration_direction, dependency_change_status).
    """
    if not has_valid_data:
        return "no_valid_user_data", "insufficient_evidence"
    if is_suppressed:
        return "privacy_suppressed", "privacy_suppressed"
    if not comparison_sufficient:
        return "insufficient_history", "insufficient_evidence"
    if recent_users == 0 and previous_users == 0:
        return "inactive", "insufficient_evidence"
    if previous_users == 0 and recent_users > 0:
        return "newly_active", "insufficient_evidence"
    if hhi_change is None:
        return "insufficient_history", "insufficient_evidence"

    if hhi_change > cfg.CONCENTRATION_CHANGE_WARNING:
        direction = "concentrating"
    elif hhi_change < -cfg.CONCENTRATION_CHANGE_WARNING:
        direction = "broadening"
    else:
        direction = "stable"

    # Dependency change uses top_1_share_change as the signal
    if top_1_share_change is None:
        dep_status = "insufficient_evidence"
    elif top_1_share_change > 0.05:
        dep_status = "dependency_increasing"
    elif top_1_share_change < -0.05:
        dep_status = "dependency_decreasing"
    else:
        dep_status = "stable_dependency"

    return direction, dep_status


def classify_concentration_status_single_window(
    user_views_df: pd.DataFrame,
    hhi: Optional[float],
    effective_user_share: Optional[float],
    top_1_share: Optional[float],
    top_3_share: Optional[float],
    active_user_count: int,
    history_sufficient: bool,
    has_valid_data: bool,
    is_suppressed: bool,
    cfg: ConcentrationMetricsConfig,
) -> str:
    """
    Classify concentration status for a single window.
    Priority order determines status.
    """
    if not has_valid_data:
        return "no_valid_user_data"
    if not history_sufficient:
        return "insufficient_history"
    if is_suppressed:
        return "privacy_suppressed"
    if active_user_count == 0:
        return "no_recent_activity"

    # single_user_dependent: only if explicitly allowed
    if (
        cfg.ALLOW_SINGLE_USER_DEPENDENCY_STATUS
        and top_1_share is not None
        and top_1_share >= cfg.TOP1_POOR_SHARE
        and active_user_count > 1
    ):
        return "single_user_dependent"

    # highly_concentrated
    if hhi is not None and hhi > cfg.HIGH_CONCENTRATION_HHI_MIN:
        return "highly_concentrated"
    if effective_user_share is not None and effective_user_share < cfg.EFFECTIVE_USER_SHARE_POOR:
        return "highly_concentrated"
    if top_3_share is not None and top_3_share >= cfg.TOP3_POOR_SHARE:
        return "highly_concentrated"

    # moderately_concentrated
    if hhi is not None and hhi > cfg.BROAD_HHI_MAX:
        return "moderately_concentrated"
    if effective_user_share is not None and effective_user_share < cfg.EFFECTIVE_USER_SHARE_WARNING:
        return "moderately_concentrated"

    return "broadly_distributed"


def classify_concentration_status(
    recent_28d: dict,
    previous_28d: dict,
    concentration_direction: str,
    comparison_sufficient: bool,
    history_sufficient_28d: bool,
    has_valid_data: bool,
    is_suppressed: bool,
    cfg: ConcentrationMetricsConfig,
) -> tuple[str, str, list[str]]:
    """
    Returns (concentration_status, concentration_evidence_status, reasons_list).
    The primary concentration_status is taken from the 28d window status.
    """
    try:
        # Primary status is the 28d window status (already computed)
        concentration_status = recent_28d.get("concentration_status_28d", "no_valid_user_data")

        # Evidence status
        if not has_valid_data:
            evidence_status = "no_valid_user_data"
        elif history_sufficient_28d:
            evidence_status = "sufficient"
        else:
            evidence_status = "insufficient_history"

        # Deterministic 10-item reasons list
        reasons: list[str] = []

        # 1. data validity
        if not has_valid_data:
            reasons.append("no_valid_user_data")
        else:
            reasons.append("valid_user_data")

        # 2. history sufficiency
        if history_sufficient_28d:
            reasons.append("history_sufficient_28d")
        else:
            reasons.append("history_insufficient_28d")

        # 3. privacy suppression
        if is_suppressed:
            reasons.append("privacy_suppressed")
        else:
            reasons.append("not_suppressed")

        # 4. active user count
        active_28d = recent_28d.get("active_user_count", 0) or 0
        reasons.append(f"active_users_28d:{active_28d}")

        # 5. comparison history
        if comparison_sufficient:
            reasons.append("comparison_history_sufficient")
        else:
            reasons.append("comparison_history_insufficient")

        # 6. concentration direction
        reasons.append(f"direction:{concentration_direction}")

        # 7. HHI value
        hhi = recent_28d.get("user_view_hhi")
        if hhi is not None:
            reasons.append(f"hhi_28d:{hhi:.4f}")
        else:
            reasons.append("hhi_28d:null")

        # 8. top_1 share
        top1 = recent_28d.get("top_1_user_view_share")
        if top1 is not None:
            reasons.append(f"top_1_share_28d:{top1:.4f}")
        else:
            reasons.append("top_1_share_28d:null")

        # 9. effective user share
        eff_share = recent_28d.get("effective_user_share")
        if eff_share is not None:
            reasons.append(f"effective_user_share_28d:{eff_share:.4f}")
        else:
            reasons.append("effective_user_share_28d:null")

        # 10. concentration status
        reasons.append(f"concentration_status:{concentration_status}")

        return concentration_status, evidence_status, reasons

    except Exception as exc:
        return "calculation_failed", "calculation_failed", [f"error:{exc}"]


def apply_concentration_privacy_suppression(
    concentration_dict: dict,
    active_user_count: Optional[int],
    cfg: ConcentrationMetricsConfig,
    window_suffix: str = "_28d",
    is_previous: bool = False,
) -> dict:
    """
    Apply suppression to concentration fields for a single window.
    Returns dict with suppression metadata added.

    When active_user_count < MIN_USERS_FOR_CONCENTRATION_METRICS, nulls sensitive fields.
    NOT suppressed: active_user_count, total_views.
    """
    d = dict(concentration_dict)

    if active_user_count is None or active_user_count >= cfg.MIN_USERS_FOR_CONCENTRATION_METRICS:
        return d

    # Fields to suppress (sensitive distribution fields)
    if is_previous:
        suppressible = [
            f"top_1_user_view_share{window_suffix}",
            f"top_3_users_view_share{window_suffix}",
            f"top_10pct_users_view_share{window_suffix}",
            f"user_view_hhi{window_suffix}",
            f"effective_user_count{window_suffix}",
            f"effective_user_share{window_suffix}",
            f"concentration_status{window_suffix}",
        ]
    else:
        suppressible = [
            f"top_1_user_view_share{window_suffix}",
            f"top_3_users_view_share{window_suffix}",
            f"top_10pct_users_view_share{window_suffix}",
            f"user_view_hhi{window_suffix}",
            f"effective_user_count{window_suffix}",
            f"effective_user_share{window_suffix}",
            f"largest_user_view_count{window_suffix}",
            f"third_largest_user_view_count{window_suffix}",
            f"concentration_status{window_suffix}",
        ]

    for field in suppressible:
        if field in d:
            d[field] = None

    return d


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_report_concentration_metrics(
    sufficiency_df: pd.DataFrame,
    mart_df: pd.DataFrame,
    quality_df: pd.DataFrame,
    boundaries_df: pd.DataFrame,
    cfg: ConcentrationMetricsConfig,
    analytics_run_id: str,
) -> pd.DataFrame:
    """
    Build one row per report in sufficiency_df with concentration metrics.

    Returns DataFrame with REPORT_USER_CONCENTRATION_METRICS_COLS, sorted by report_id.
    """
    generated_at = datetime.utcnow().isoformat()

    # ── Parse window boundaries ────────────────────────────────────────────
    if boundaries_df.empty:
        raise ValueError("boundaries_df is empty — run engagement windows step first")

    b = boundaries_df.iloc[0]
    analytics_as_of_date = str(b.get("analytics_as_of_date", ""))

    w28s = _parse_date(b.get("window_28d_start"))
    w28e = _parse_date(b.get("window_28d_end"))
    pw28s = _parse_date(b.get("previous_28d_start"))
    pw28e = _parse_date(b.get("previous_28d_end"))
    w90s = _parse_date(b.get("window_90d_start"))
    w90e = _parse_date(b.get("window_90d_end"))

    # ── Pre-parse mart ─────────────────────────────────────────────────────
    mart_parsed = mart_df.copy()
    if not mart_parsed.empty and "usage_date" in mart_parsed.columns:
        mart_parsed["usage_date"] = pd.to_datetime(
            mart_parsed["usage_date"], errors="coerce"
        ).dt.date

    # ── Index quality by report_id ─────────────────────────────────────────
    quality_by_report: dict = {}
    if not quality_df.empty and "report_id" in quality_df.columns:
        for _, row in quality_df.iterrows():
            quality_by_report[str(row["report_id"])] = row

    rows = []
    for _, suf_row in sufficiency_df.sort_values("report_id").iterrows():
        rid = str(suf_row["report_id"])
        report_name = suf_row.get("report_name", "")

        # ── Sufficiency flags ──────────────────────────────────────────────
        suf_28d = bool(suf_row.get("history_sufficient_28d", False))
        suf_prev28d = bool(suf_row.get("history_sufficient_previous_28d", False))
        cmp_28d = bool(suf_row.get("comparison_history_sufficient_28d", False))
        suf_90d = bool(suf_row.get("history_sufficient_90d", False))
        suf_prev90d = bool(suf_row.get("history_sufficient_previous_90d", False))
        cmp_90d = bool(suf_row.get("comparison_history_sufficient_90d", False))
        has_any_activity = bool(suf_row.get("has_any_valid_user_activity", False))

        # ── Quality info ───────────────────────────────────────────────────
        q_row = quality_by_report.get(rid)
        if q_row is not None:
            dq_status = str(q_row.get("data_quality_status", "good"))
        else:
            dq_status = "good"
        has_valid_data = dq_status != "no_valid_user_data"

        # Helper: compute concentration metrics for a window
        def _window_metrics(suf_flag, start, end, window_suf):
            """
            Returns dict with all raw concentration fields for this window.
            All null if suf_flag is False or data invalid.
            """
            if not window_suf or not has_valid_data or start is None or end is None:
                return {
                    "active_user_count": None,
                    "total_views": None,
                    "top_1_user_view_share": None,
                    "top_3_users_view_share": None,
                    "top_10pct_users_view_share": None,
                    "top_10pct_user_count": None,
                    "user_view_hhi": None,
                    "effective_user_count": None,
                    "effective_user_share": None,
                    "largest_user_view_count": None,
                    "third_largest_user_view_count": None,
                    "user_views_df": pd.DataFrame(columns=["user_key", "user_window_views", "rank"]),
                }

            udf = aggregate_user_window_views(mart_parsed, rid, start, end)
            total_views = int(udf["user_window_views"].sum()) if not udf.empty else 0
            top_shares = calculate_top_user_shares(udf, total_views)
            hhi_metrics = calculate_user_view_hhi(udf, total_views)

            return {
                "active_user_count": top_shares["active_user_count"],
                "total_views": top_shares["total_views"],
                "top_1_user_view_share": top_shares["top_1_user_view_share"],
                "top_3_users_view_share": top_shares["top_3_users_view_share"],
                "top_10pct_users_view_share": top_shares["top_10pct_users_view_share"],
                "top_10pct_user_count": top_shares["top_10pct_user_count"],
                "user_view_hhi": hhi_metrics["user_view_hhi"],
                "effective_user_count": hhi_metrics["effective_user_count"],
                "effective_user_share": hhi_metrics["effective_user_share"],
                "largest_user_view_count": top_shares["largest_user_view_count"],
                "third_largest_user_view_count": top_shares["third_largest_user_view_count"],
                "user_views_df": udf,
            }

        # ── Compute windows ────────────────────────────────────────────────
        m28 = _window_metrics(suf_28d, w28s, w28e, suf_28d)
        mp28 = _window_metrics(suf_prev28d, pw28s, pw28e, suf_prev28d)
        m90 = _window_metrics(suf_90d, w90s, w90e, suf_90d)

        # ── Suppression per window ─────────────────────────────────────────
        active_28d = m28["active_user_count"]
        active_prev28d = mp28["active_user_count"]
        active_90d = m90["active_user_count"]

        suppressed_28d = (
            active_28d is not None
            and active_28d < cfg.MIN_USERS_FOR_CONCENTRATION_METRICS
        )
        suppressed_prev28d = (
            active_prev28d is not None
            and active_prev28d < cfg.MIN_USERS_FOR_CONCENTRATION_METRICS
        )
        suppressed_90d = (
            active_90d is not None
            and active_90d < cfg.MIN_USERS_FOR_CONCENTRATION_METRICS
        )

        # Build windowed dicts for suppression application
        raw_28d = {
            "top_1_user_view_share_28d": m28["top_1_user_view_share"],
            "top_3_users_view_share_28d": m28["top_3_users_view_share"],
            "top_10pct_users_view_share_28d": m28["top_10pct_users_view_share"],
            "user_view_hhi_28d": m28["user_view_hhi"],
            "effective_user_count_28d": m28["effective_user_count"],
            "effective_user_share_28d": m28["effective_user_share"],
            "largest_user_view_count_28d": m28["largest_user_view_count"],
            "third_largest_user_view_count_28d": m28["third_largest_user_view_count"],
        }
        raw_prev28d = {
            "top_1_user_view_share_previous_28d": mp28["top_1_user_view_share"],
            "top_3_users_view_share_previous_28d": mp28["top_3_users_view_share"],
            "top_10pct_users_view_share_previous_28d": mp28["top_10pct_users_view_share"],
            "user_view_hhi_previous_28d": mp28["user_view_hhi"],
            "effective_user_count_previous_28d": mp28["effective_user_count"],
            "effective_user_share_previous_28d": mp28["effective_user_share"],
        }
        raw_90d = {
            "top_1_user_view_share_90d": m90["top_1_user_view_share"],
            "top_3_users_view_share_90d": m90["top_3_users_view_share"],
            "top_10pct_users_view_share_90d": m90["top_10pct_users_view_share"],
            "user_view_hhi_90d": m90["user_view_hhi"],
            "effective_user_count_90d": m90["effective_user_count"],
            "effective_user_share_90d": m90["effective_user_share"],
        }

        # Apply suppression
        if suppressed_28d:
            for field in list(raw_28d.keys()):
                raw_28d[field] = None
        if suppressed_prev28d:
            for field in list(raw_prev28d.keys()):
                raw_prev28d[field] = None
        if suppressed_90d:
            for field in list(raw_90d.keys()):
                raw_90d[field] = None

        # ── Classify per-window concentration status ───────────────────────
        status_28d = classify_concentration_status_single_window(
            user_views_df=m28["user_views_df"],
            hhi=raw_28d.get("user_view_hhi_28d"),
            effective_user_share=raw_28d.get("effective_user_share_28d"),
            top_1_share=raw_28d.get("top_1_user_view_share_28d"),
            top_3_share=raw_28d.get("top_3_users_view_share_28d"),
            active_user_count=active_28d or 0,
            history_sufficient=suf_28d,
            has_valid_data=has_valid_data,
            is_suppressed=suppressed_28d,
            cfg=cfg,
        )
        status_prev28d = classify_concentration_status_single_window(
            user_views_df=mp28["user_views_df"],
            hhi=raw_prev28d.get("user_view_hhi_previous_28d"),
            effective_user_share=raw_prev28d.get("effective_user_share_previous_28d"),
            top_1_share=raw_prev28d.get("top_1_user_view_share_previous_28d"),
            top_3_share=raw_prev28d.get("top_3_users_view_share_previous_28d"),
            active_user_count=active_prev28d or 0,
            history_sufficient=suf_prev28d,
            has_valid_data=has_valid_data,
            is_suppressed=suppressed_prev28d,
            cfg=cfg,
        )
        status_90d = classify_concentration_status_single_window(
            user_views_df=m90["user_views_df"],
            hhi=raw_90d.get("user_view_hhi_90d"),
            effective_user_share=raw_90d.get("effective_user_share_90d"),
            top_1_share=raw_90d.get("top_1_user_view_share_90d"),
            top_3_share=raw_90d.get("top_3_users_view_share_90d"),
            active_user_count=active_90d or 0,
            history_sufficient=suf_90d,
            has_valid_data=has_valid_data,
            is_suppressed=suppressed_90d,
            cfg=cfg,
        )

        # ── Change metrics ─────────────────────────────────────────────────
        recent_for_change = {
            "top_1_user_view_share": raw_28d.get("top_1_user_view_share_28d"),
            "top_3_users_view_share": raw_28d.get("top_3_users_view_share_28d"),
            "top_10pct_users_view_share": raw_28d.get("top_10pct_users_view_share_28d"),
            "user_view_hhi": raw_28d.get("user_view_hhi_28d"),
            "effective_user_count": raw_28d.get("effective_user_count_28d"),
            "effective_user_share": raw_28d.get("effective_user_share_28d"),
        }
        prev_for_change = {
            "top_1_user_view_share": raw_prev28d.get("top_1_user_view_share_previous_28d"),
            "top_3_users_view_share": raw_prev28d.get("top_3_users_view_share_previous_28d"),
            "top_10pct_users_view_share": raw_prev28d.get("top_10pct_users_view_share_previous_28d"),
            "user_view_hhi": raw_prev28d.get("user_view_hhi_previous_28d"),
            "effective_user_count": raw_prev28d.get("effective_user_count_previous_28d"),
            "effective_user_share": raw_prev28d.get("effective_user_share_previous_28d"),
        }

        change_metrics = calculate_concentration_change(
            recent=recent_for_change,
            previous=prev_for_change,
            comparison_sufficient=cmp_28d,
        )

        # ── Concentration direction ────────────────────────────────────────
        overall_suppressed = suppressed_28d

        concentration_direction, dependency_change_status = classify_concentration_direction(
            recent_hhi=raw_28d.get("user_view_hhi_28d"),
            previous_hhi=raw_prev28d.get("user_view_hhi_previous_28d"),
            hhi_change=change_metrics.get("hhi_change_28d"),
            recent_users=active_28d or 0,
            previous_users=active_prev28d or 0,
            comparison_sufficient=cmp_28d,
            has_valid_data=has_valid_data,
            is_suppressed=overall_suppressed,
            cfg=cfg,
            top_1_share_change=change_metrics.get("top_1_share_change_28d"),
        )

        # ── Overall concentration status ───────────────────────────────────
        recent_for_status = dict(raw_28d)
        recent_for_status["active_user_count"] = active_28d
        recent_for_status["concentration_status_28d"] = status_28d

        overall_status, evidence_status, reasons = classify_concentration_status(
            recent_28d=recent_for_status,
            previous_28d={},
            concentration_direction=concentration_direction,
            comparison_sufficient=cmp_28d,
            history_sufficient_28d=suf_28d,
            has_valid_data=has_valid_data,
            is_suppressed=overall_suppressed,
            cfg=cfg,
        )

        # ── Privacy suppression metadata ───────────────────────────────────
        if overall_suppressed:
            privacy_suppression_status = "suppressed"
            suppression_reason = "unique_users_below_minimum"
            suppressed_fields = [
                "top_1_user_view_share_28d", "top_3_users_view_share_28d",
                "top_10pct_users_view_share_28d", "user_view_hhi_28d",
                "effective_user_count_28d", "effective_user_share_28d",
                "largest_user_view_count_28d", "third_largest_user_view_count_28d",
            ]
            suppressed_concentration_fields = ",".join(suppressed_fields)
        else:
            privacy_suppression_status = "not_suppressed"
            suppression_reason = None
            suppressed_concentration_fields = None

        row = {
            "analytics_run_id": analytics_run_id,
            "generated_at": generated_at,
            "analytics_as_of_date": analytics_as_of_date,
            "report_id": rid,
            "report_name": report_name,
            "history_sufficient_28d": suf_28d,
            "history_sufficient_previous_28d": suf_prev28d,
            "comparison_history_sufficient_28d": cmp_28d,
            "history_sufficient_90d": suf_90d,
            "history_sufficient_previous_90d": suf_prev90d,
            "comparison_history_sufficient_90d": cmp_90d,
            "has_any_valid_user_activity": has_any_activity,
            "user_data_quality_status": dq_status,
            "privacy_suppression_status": privacy_suppression_status,
            # Recent 28d
            "active_user_count_28d": active_28d,
            "total_views_28d": m28["total_views"],
            "top_1_user_view_share_28d": raw_28d.get("top_1_user_view_share_28d"),
            "top_3_users_view_share_28d": raw_28d.get("top_3_users_view_share_28d"),
            "top_10pct_users_view_share_28d": raw_28d.get("top_10pct_users_view_share_28d"),
            "top_10pct_user_count_28d": m28["top_10pct_user_count"] if not suppressed_28d else None,
            "user_view_hhi_28d": raw_28d.get("user_view_hhi_28d"),
            "effective_user_count_28d": raw_28d.get("effective_user_count_28d"),
            "effective_user_share_28d": raw_28d.get("effective_user_share_28d"),
            "largest_user_view_count_28d": raw_28d.get("largest_user_view_count_28d"),
            "third_largest_user_view_count_28d": raw_28d.get("third_largest_user_view_count_28d"),
            "concentration_status_28d": status_28d,
            # Previous 28d
            "active_user_count_previous_28d": active_prev28d,
            "total_views_previous_28d": mp28["total_views"],
            "top_1_user_view_share_previous_28d": raw_prev28d.get("top_1_user_view_share_previous_28d"),
            "top_3_users_view_share_previous_28d": raw_prev28d.get("top_3_users_view_share_previous_28d"),
            "top_10pct_users_view_share_previous_28d": raw_prev28d.get("top_10pct_users_view_share_previous_28d"),
            "top_10pct_user_count_previous_28d": mp28["top_10pct_user_count"] if not suppressed_prev28d else None,
            "user_view_hhi_previous_28d": raw_prev28d.get("user_view_hhi_previous_28d"),
            "effective_user_count_previous_28d": raw_prev28d.get("effective_user_count_previous_28d"),
            "effective_user_share_previous_28d": raw_prev28d.get("effective_user_share_previous_28d"),
            "concentration_status_previous_28d": status_prev28d,
            # Recent 90d
            "active_user_count_90d": active_90d,
            "total_views_90d": m90["total_views"],
            "top_1_user_view_share_90d": raw_90d.get("top_1_user_view_share_90d"),
            "top_3_users_view_share_90d": raw_90d.get("top_3_users_view_share_90d"),
            "top_10pct_users_view_share_90d": raw_90d.get("top_10pct_users_view_share_90d"),
            "top_10pct_user_count_90d": m90["top_10pct_user_count"] if not suppressed_90d else None,
            "user_view_hhi_90d": raw_90d.get("user_view_hhi_90d"),
            "effective_user_count_90d": raw_90d.get("effective_user_count_90d"),
            "effective_user_share_90d": raw_90d.get("effective_user_share_90d"),
            "concentration_status_90d": status_90d,
            # Change metrics
            "top_1_share_change_28d": change_metrics.get("top_1_share_change_28d"),
            "top_3_share_change_28d": change_metrics.get("top_3_share_change_28d"),
            "top_10pct_share_change_28d": change_metrics.get("top_10pct_share_change_28d"),
            "hhi_change_28d": change_metrics.get("hhi_change_28d"),
            "effective_user_count_change_28d": change_metrics.get("effective_user_count_change_28d"),
            "effective_user_share_change_28d": change_metrics.get("effective_user_share_change_28d"),
            "concentration_direction": concentration_direction,
            "dependency_change_status": dependency_change_status,
            # Overall
            "concentration_status": overall_status,
            "concentration_evidence_status": evidence_status,
            "concentration_reasons": "|".join(reasons) if reasons else "none",
            # Privacy
            "concentration_privacy_suppressed": overall_suppressed,
            "concentration_privacy_suppression_reason": suppression_reason,
            "suppressed_concentration_fields": suppressed_concentration_fields,
        }
        rows.append(row)

    result = pd.DataFrame(rows, columns=REPORT_USER_CONCENTRATION_METRICS_COLS)
    return result


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_report_concentration_metrics(
    df: pd.DataFrame,
    cfg: Optional[ConcentrationMetricsConfig] = None,
) -> None:
    """Validate concentration metrics DataFrame. Raises ValueError on failure."""
    if cfg is None:
        cfg = ConcentrationMetricsConfig()

    # 1. Required columns present
    missing = [c for c in REPORT_USER_CONCENTRATION_METRICS_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in concentration metrics output: {missing}")

    if df.empty:
        return

    # 2. Unique (analytics_run_id, report_id) grain
    dupes = df.duplicated(subset=["analytics_run_id", "report_id"])
    if dupes.any():
        raise ValueError(
            f"Duplicate (analytics_run_id, report_id) rows: {df.loc[dupes, 'report_id'].tolist()}"
        )

    # 3. active_user_count >= 0 where not null
    for col in ["active_user_count_28d", "active_user_count_previous_28d", "active_user_count_90d"]:
        if col in df.columns:
            bad = df[df[col].notna() & (df[col] < 0)]
            if not bad.empty:
                raise ValueError(f"Negative values in {col}: {bad['report_id'].tolist()}")

    # 4. total_views >= 0 where not null
    for col in ["total_views_28d", "total_views_previous_28d", "total_views_90d"]:
        if col in df.columns:
            bad = df[df[col].notna() & (df[col] < 0)]
            if not bad.empty:
                raise ValueError(f"Negative values in {col}: {bad['report_id'].tolist()}")

    # 5. 0 <= top_1_share <= top_3_share approximately
    for top1, top3 in [
        ("top_1_user_view_share_28d", "top_3_users_view_share_28d"),
        ("top_1_user_view_share_previous_28d", "top_3_users_view_share_previous_28d"),
        ("top_1_user_view_share_90d", "top_3_users_view_share_90d"),
    ]:
        if top1 in df.columns and top3 in df.columns:
            both = df[df[top1].notna() & df[top3].notna()]
            bad = both[both[top1] > both[top3] + 1e-9]
            if not bad.empty:
                raise ValueError(f"{top1} > {top3} for: {bad['report_id'].tolist()}")

    # 6. 0 <= top_1_share <= top_10pct_share
    for top1, top10 in [
        ("top_1_user_view_share_28d", "top_10pct_users_view_share_28d"),
        ("top_1_user_view_share_90d", "top_10pct_users_view_share_90d"),
    ]:
        if top1 in df.columns and top10 in df.columns:
            both = df[df[top1].notna() & df[top10].notna()]
            bad = both[both[top1] > both[top10] + 1e-9]
            if not bad.empty:
                raise ValueError(f"{top1} > {top10} for: {bad['report_id'].tolist()}")

    # 7. top_3_share <= top_10pct_share ONLY when top_10pct_user_count >= 3
    for top3, top10, count_col in [
        ("top_3_users_view_share_28d", "top_10pct_users_view_share_28d", "top_10pct_user_count_28d"),
    ]:
        if all(c in df.columns for c in [top3, top10, count_col]):
            subset = df[
                df[top3].notna() & df[top10].notna() & df[count_col].notna()
                & (df[count_col] >= 3)
            ]
            bad = subset[subset[top3] > subset[top10] + 1e-9]
            if not bad.empty:
                raise ValueError(
                    f"{top3} > {top10} when top_10pct_user_count >= 3 for: "
                    f"{bad['report_id'].tolist()}"
                )

    # 8. 0 <= HHI <= 1
    for col in ["user_view_hhi_28d", "user_view_hhi_previous_28d", "user_view_hhi_90d"]:
        if col in df.columns:
            bad = df[df[col].notna() & ((df[col] < 0) | (df[col] > 1 + 1e-9))]
            if not bad.empty:
                raise ValueError(f"HHI out of [0,1] in {col}: {bad['report_id'].tolist()}")

    # 9. effective_user_count = 1/hhi approximately (±0.001) where both defined
    for hhi_col, eff_col in [
        ("user_view_hhi_28d", "effective_user_count_28d"),
    ]:
        if hhi_col in df.columns and eff_col in df.columns:
            both = df[df[hhi_col].notna() & df[eff_col].notna() & (df[hhi_col] > 0)]
            expected = 1.0 / both[hhi_col]
            bad = both[(both[eff_col] - expected).abs() > 0.001]
            if not bad.empty:
                raise ValueError(
                    f"{eff_col} != 1/{hhi_col} (±0.001) for: {bad['report_id'].tolist()}"
                )

    # 10. effective_user_count <= active_user_count where both defined
    for eff_col, act_col in [
        ("effective_user_count_28d", "active_user_count_28d"),
        ("effective_user_count_90d", "active_user_count_90d"),
    ]:
        if eff_col in df.columns and act_col in df.columns:
            both = df[df[eff_col].notna() & df[act_col].notna()]
            bad = both[both[eff_col] > both[act_col] + 1e-9]
            if not bad.empty:
                raise ValueError(
                    f"{eff_col} > {act_col} for: {bad['report_id'].tolist()}"
                )

    # 11. 0 < effective_user_share <= 1 where defined
    for col in ["effective_user_share_28d", "effective_user_share_90d"]:
        if col in df.columns:
            bad = df[df[col].notna() & ((df[col] <= 0) | (df[col] > 1 + 1e-9))]
            if not bad.empty:
                raise ValueError(f"{col} out of (0,1] for: {bad['report_id'].tolist()}")

    # 12. Shares null when total_views = 0
    for tv_col, share_cols in [
        ("total_views_28d", ["top_1_user_view_share_28d", "top_3_users_view_share_28d", "user_view_hhi_28d"]),
    ]:
        if tv_col in df.columns:
            zero_rows = df[df[tv_col].notna() & (df[tv_col] == 0)]
            for sc in share_cols:
                if sc in df.columns:
                    bad = zero_rows[zero_rows[sc].notna()]
                    if not bad.empty:
                        raise ValueError(
                            f"{sc} must be null when {tv_col}=0 for: {bad['report_id'].tolist()}"
                        )

    # 13. Suppressed fields are null when suppressed
    if "concentration_privacy_suppressed" in df.columns:
        suppressed = df[df["concentration_privacy_suppressed"] == True]
        suppressed_share_cols = [
            "top_1_user_view_share_28d", "top_3_users_view_share_28d",
            "top_10pct_users_view_share_28d", "user_view_hhi_28d",
            "effective_user_count_28d", "effective_user_share_28d",
        ]
        for col in suppressed_share_cols:
            if col in df.columns:
                bad = suppressed[suppressed[col].notna()]
                if not bad.empty:
                    raise ValueError(
                        f"Suppressed field {col} must be null for: {bad['report_id'].tolist()}"
                    )

    # 14. No prohibited identifier columns
    validate_no_direct_identifiers(df, context="report_user_concentration_metrics")

    # 15. No user_key column in output
    if "user_key" in df.columns:
        raise ValueError("Output must not contain user_key column")

    # 16. Status values in allowed set
    status_col_map = {
        "concentration_status": _VALID_CONCENTRATION_STATUSES,
        "concentration_status_28d": _VALID_CONCENTRATION_STATUSES,
        "concentration_status_previous_28d": _VALID_CONCENTRATION_STATUSES,
        "concentration_status_90d": _VALID_CONCENTRATION_STATUSES,
    }
    for col, valid_set in status_col_map.items():
        if col in df.columns:
            bad = df[df[col].notna() & ~df[col].isin(valid_set)]
            if not bad.empty:
                raise ValueError(f"Invalid {col} values: {bad[col].unique().tolist()}")

    # 17. No list/array type columns
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, set, tuple))).any():
            raise ValueError(f"Privacy violation: column '{col}' contains user lists.")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_report_concentration_metrics(
    df: pd.DataFrame,
    project_root: Path,
) -> Path:
    """Validate and write to outputs/analytics/report_user_concentration_metrics.csv."""
    validate_report_concentration_metrics(df)
    output_dir = project_root / "outputs" / "analytics"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "report_user_concentration_metrics.csv"
    df_sorted = df.sort_values("report_id").reset_index(drop=True)
    df_sorted.to_csv(out_path, index=False)
    return out_path
