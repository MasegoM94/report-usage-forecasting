"""Canonical report analytics mart for Sprint 7.

Joins all context sources from the report features spine and derives
high-level status, priority, and recommended action columns.
Does NOT recalculate any source metric.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from src.analytics.report_segmentation import (
    build_report_segments,
    SegmentationConfig,
)

# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------

MART_SCHEMA_VERSION = "1.0.0"

MART_IDENTITY_COLS = [
    "analytics_run_id", "generated_at", "analytics_as_of_date",
    "report_id", "report_name", "workspace_id", "schema_version",
]

MART_SUMMARY_COLS = [
    "overall_report_status", "primary_report_issue",
    "overall_evidence_status", "overall_review_priority",
    "recommended_report_action", "report_reasons", "report_review_required",
]

PROHIBITED_MART_COLS = frozenset({
    "user_id", "email", "email_address", "user_name", "username",
    "display_name", "unique_user", "principal_name", "user_key",
    "repeat_rate", "latest_views", "prior_views", "top_user_concentration",
})

ALLOWED_OVERALL_STATUS = frozenset({
    "healthy", "growing", "stable_niche", "declining", "inactive",
    "at_risk", "model_limited", "forecast_uncertain", "newly_launched",
    "planned_deprecation", "mixed_signals", "insufficient_evidence",
    "data_quality_issue",
})

ALLOWED_OVERALL_EVIDENCE = frozenset({
    "complete", "complete_with_privacy_limits", "partial",
    "insufficient", "invalid",
})

ALLOWED_REVIEW_PRIORITY = frozenset({
    "low", "medium", "high", "insufficient_evidence",
})

ALLOWED_MART_ACTIONS = frozenset({
    "continue_monitoring", "monitor_new_report", "support_new_user_onboarding",
    "investigate_usage_decline", "investigate_user_decline",
    "improve_repeat_engagement", "investigate_user_lapse",
    "review_concentrated_dependency", "review_forecast_decline",
    "review_forecast_uncertainty", "review_model_health",
    "validate_report_audience", "complete_report_metadata",
    "investigate_data_quality", "review_planned_deprecation",
    "insufficient_evidence",
})

PROHIBITED_MART_ACTIONS = frozenset({
    "retire_report", "delete_report", "automatically_retrain",
    "change_selected_model", "restrict_user", "contact_specific_user",
})

# ---------------------------------------------------------------------------
# Status / action mappings
# ---------------------------------------------------------------------------

SEGMENT_TO_OVERALL_STATUS = {
    "data_quality_issue": "data_quality_issue",
    "insufficient_evidence": "insufficient_evidence",
    "planned_deprecation": "planned_deprecation",
    "inactive_report": "inactive",
    "model_review_needed": "model_limited",
    "declining_report": "declining",
    "elevated_lapse": "at_risk",
    "concentrated_dependency": "at_risk",
    "low_repeat_usage": "at_risk",
    "uncertain_forecast": "forecast_uncertain",
    "growing_report": "growing",
    "newly_launched": "newly_launched",
    "healthy_broad_adoption": "healthy",
    "healthy_niche_adoption": "stable_niche",
    "mixed_signals": "mixed_signals",
}

OVERALL_STATUS_TO_ACTION = {
    "data_quality_issue": "investigate_data_quality",
    "insufficient_evidence": "insufficient_evidence",
    "planned_deprecation": "review_planned_deprecation",
    "inactive": "investigate_usage_decline",
    "model_limited": "review_model_health",
    "declining": "investigate_usage_decline",
    "at_risk": "investigate_user_decline",
    "forecast_uncertain": "review_forecast_uncertainty",
    "growing": "continue_monitoring",
    "newly_launched": "monitor_new_report",
    "healthy": "continue_monitoring",
    "stable_niche": "continue_monitoring",
    "mixed_signals": "continue_monitoring",
}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MartConfig:
    SCHEMA_VERSION: str = "1.0.0"


_DEFAULT_MART_CFG = MartConfig()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_get(row: Optional[pd.Series], col: str, default=None):
    if row is None:
        return default
    val = row.get(col, default)
    if val is None:
        return default
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    return val


def _determine_review_priority(overall_status: str, diagnostic_severity: str) -> str:
    if overall_status in {"data_quality_issue", "inactive", "declining"}:
        return "high"
    if overall_status in {"at_risk", "model_limited"}:
        return "high"
    if overall_status in {"forecast_uncertain", "planned_deprecation"}:
        return "medium"
    if overall_status == "insufficient_evidence":
        return "insufficient_evidence"
    if str(diagnostic_severity) in {"poor", "warning"}:
        return "medium"
    return "low"


def _determine_recommended_action(overall_status: str, primary_report_segment: str) -> str:
    action = OVERALL_STATUS_TO_ACTION.get(overall_status, "continue_monitoring")
    # Refine "at_risk" based on primary segment
    if overall_status == "at_risk":
        if primary_report_segment == "elevated_lapse":
            action = "investigate_user_lapse"
        elif primary_report_segment == "concentrated_dependency":
            action = "review_concentrated_dependency"
        elif primary_report_segment == "low_repeat_usage":
            action = "improve_repeat_engagement"
    return action


def _determine_overall_evidence(
    engagement_row: Optional[pd.Series],
    diagnostic_row: Optional[pd.Series],
    available_source_count: int,
    total_sources: int = 6,
) -> str:
    privacy_suppressed = (
        engagement_row is not None
        and str(_safe_get(engagement_row, "privacy_suppression_status", "") or "")
        in {"privacy_suppressed", "partial_suppression"}
    )
    if available_source_count == total_sources and privacy_suppressed:
        return "complete_with_privacy_limits"
    if available_source_count == total_sources:
        return "complete"
    if available_source_count >= 4:
        return "partial"
    if available_source_count >= 2:
        return "insufficient"
    return "invalid"


def _count_available_sources(
    forecast_row, health_row, eng_row, meta_row, diag_row, seg_row
) -> int:
    # features always present (spine); count the others
    return sum([
        forecast_row is not None,
        health_row is not None,
        eng_row is not None,
        meta_row is not None,
        diag_row is not None,
        seg_row is not None,
    ])


# ---------------------------------------------------------------------------
# Main build function
# ---------------------------------------------------------------------------

def build_mart_report_analytics(
    features_df: pd.DataFrame,
    forecast_df: Optional[pd.DataFrame],
    model_health_df: Optional[pd.DataFrame],
    engagement_df: Optional[pd.DataFrame],
    metadata_df: Optional[pd.DataFrame],
    diagnostics_df: Optional[pd.DataFrame],
    segments_df: Optional[pd.DataFrame],
    analytics_run_id: str,
    cfg: Optional[MartConfig] = None,
) -> pd.DataFrame:
    """Join all sources from the report features spine and build summary columns."""
    cfg = cfg or _DEFAULT_MART_CFG

    def _validate_unique(df: Optional[pd.DataFrame], name: str) -> None:
        if df is None or df.empty:
            return
        if df.duplicated(subset=["report_id"]).any():
            raise ValueError(f"Duplicate report_id in source: {name}")

    _validate_unique(forecast_df, "forecast_df")
    _validate_unique(model_health_df, "model_health_df")
    _validate_unique(engagement_df, "engagement_df")
    _validate_unique(metadata_df, "metadata_df")
    _validate_unique(diagnostics_df, "diagnostics_df")
    _validate_unique(segments_df, "segments_df")

    # Build mart starting from features spine
    mart = features_df.copy()
    existing_cols: set = set(mart.columns)

    def _merge(source_df: Optional[pd.DataFrame], name: str) -> None:
        nonlocal mart, existing_cols
        if source_df is None or source_df.empty:
            return
        # Only keep columns not already in mart (plus report_id for join)
        keep = ["report_id"] + [c for c in source_df.columns if c not in existing_cols and c != "report_id"]
        mart_new = mart.merge(source_df[keep], on="report_id", how="left")
        mart = mart_new
        existing_cols = set(mart.columns)

    _merge(forecast_df, "forecast")
    _merge(model_health_df, "model_health")
    _merge(engagement_df, "engagement")
    _merge(metadata_df, "metadata")
    _merge(diagnostics_df, "diagnostics")
    _merge(segments_df, "segments")

    # Drop prohibited columns
    bad_cols = set(mart.columns) & PROHIBITED_MART_COLS
    if bad_cols:
        mart = mart.drop(columns=list(bad_cols))

    # Add identity fields
    if "schema_version" not in mart.columns:
        mart["schema_version"] = MART_SCHEMA_VERSION
    else:
        mart["schema_version"] = MART_SCHEMA_VERSION  # enforce

    generated_at = datetime.utcnow().isoformat()
    mart["generated_at"] = generated_at  # overwrite with mart generation time

    # Build per-row lookup indexes for quick access
    def _idx(df: Optional[pd.DataFrame]) -> Optional[dict]:
        if df is None or df.empty:
            return None
        return {str(r["report_id"]): r for _, r in df.iterrows()}

    forecast_idx = _idx(forecast_df)
    health_idx = _idx(model_health_df)
    eng_idx = _idx(engagement_df)
    meta_idx = _idx(metadata_df)
    diag_idx = _idx(diagnostics_df)
    seg_idx = _idx(segments_df)

    # Derive summary columns row-by-row
    overall_statuses, primary_issues, evidence_statuses = [], [], []
    review_priorities, recommended_actions, report_reasons_list, review_required_list = [], [], [], []

    for _, row in mart.iterrows():
        rid = str(row.get("report_id", ""))

        seg_row = seg_idx.get(rid) if seg_idx else None
        diag_row = diag_idx.get(rid) if diag_idx else None
        eng_row = eng_idx.get(rid) if eng_idx else None
        forecast_row = forecast_idx.get(rid) if forecast_idx else None
        health_row = health_idx.get(rid) if health_idx else None
        meta_row = meta_idx.get(rid) if meta_idx else None

        # Primary segment drives overall status
        primary_seg = str(_safe_get(seg_row, "primary_report_segment", "") or "")
        if not primary_seg:
            # Fall back to features usage status
            hist_status = str(row.get("historical_usage_status", "") or "")
            primary_seg = "insufficient_evidence" if not hist_status else "mixed_signals"

        overall_status = SEGMENT_TO_OVERALL_STATUS.get(primary_seg, "mixed_signals")

        # Primary issue: diagnostics first, then segment
        primary_issue = str(_safe_get(diag_row, "primary_diagnostic", "") or "")
        if not primary_issue or primary_issue == "none":
            primary_issue = primary_seg

        # Evidence status
        avail_count = _count_available_sources(forecast_row, health_row, eng_row, meta_row, diag_row, seg_row)
        evidence_status = _determine_overall_evidence(eng_row, diag_row, avail_count)

        # Review priority
        diag_severity = str(_safe_get(diag_row, "overall_diagnostic_severity", "") or "")
        priority = _determine_review_priority(overall_status, diag_severity)

        # Recommended action
        action = _determine_recommended_action(overall_status, primary_seg)

        # Reasons
        reasons = (
            f"status:{overall_status} | issue:{primary_issue} | "
            f"evidence:{evidence_status} | priority:{priority}"
        )

        # Review required
        review_req = priority in {"high"} or overall_status in {
            "declining", "inactive", "at_risk", "model_limited", "data_quality_issue",
        }

        overall_statuses.append(overall_status)
        primary_issues.append(primary_issue)
        evidence_statuses.append(evidence_status)
        review_priorities.append(priority)
        recommended_actions.append(action)
        report_reasons_list.append(reasons)
        review_required_list.append(review_req)

    mart["overall_report_status"] = overall_statuses
    mart["primary_report_issue"] = primary_issues
    mart["overall_evidence_status"] = evidence_statuses
    mart["overall_review_priority"] = review_priorities
    mart["recommended_report_action"] = recommended_actions
    mart["report_reasons"] = report_reasons_list
    mart["report_review_required"] = review_required_list

    return mart.sort_values(["analytics_run_id", "report_id"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_mart_report_analytics(df: pd.DataFrame) -> None:
    if df.duplicated(subset=["report_id"]).any():
        raise ValueError("Duplicate report_id in mart")

    bad_cols = set(df.columns) & PROHIBITED_MART_COLS
    if bad_cols:
        raise ValueError(f"Prohibited columns present: {bad_cols}")

    if "recommended_report_action" in df.columns:
        bad_actions = set(df["recommended_report_action"].dropna()) & PROHIBITED_MART_ACTIONS
        if bad_actions:
            raise ValueError(f"Prohibited actions present: {bad_actions}")

    if "overall_report_status" in df.columns:
        invalid = set(df["overall_report_status"].dropna()) - ALLOWED_OVERALL_STATUS
        if invalid:
            raise ValueError(f"Invalid overall_report_status values: {invalid}")

    if "overall_review_priority" in df.columns:
        invalid = set(df["overall_review_priority"].dropna()) - ALLOWED_REVIEW_PRIORITY
        if invalid:
            raise ValueError(f"Invalid overall_review_priority values: {invalid}")

    email_pat = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
    for col in df.select_dtypes(include="object").columns:
        for val in df[col].dropna().astype(str).head(100):
            if email_pat.search(val):
                raise ValueError(f"Email-like value found in column {col}")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_mart_report_analytics(df: pd.DataFrame, project_root: Path) -> Path:
    validate_mart_report_analytics(df)
    out_dir = Path(project_root) / "outputs" / "analytics"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "mart_report_analytics.csv"
    df.sort_values(["analytics_run_id", "report_id"]).to_csv(out_path, index=False)
    return out_path
