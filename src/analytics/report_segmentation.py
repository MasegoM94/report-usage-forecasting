"""
Deterministic report segmentation layer for Sprint 7.

Each dimensional segment is independent. Primary segment uses precedence ordering.
Does not conflate unrelated signals (no old 'niche' rule).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Schema constants
# ---------------------------------------------------------------------------

REPORT_SEGMENTS_COLS = [
    "analytics_run_id", "generated_at", "analytics_as_of_date",
    "report_id", "report_name",
    "usage_segment", "engagement_segment", "forecast_segment",
    "model_health_segment", "dependency_segment",
    "lifecycle_segment", "metadata_segment",
    "primary_report_segment",
    "segment_evidence_status", "segment_reasons", "segment_review_required",
]

ALLOWED_USAGE_SEGMENTS = frozenset({
    "growing_usage", "stable_regular_usage", "stable_intermittent_usage",
    "declining_usage", "inactive", "newly_active", "insufficient_evidence",
})

ALLOWED_ENGAGEMENT_SEGMENTS = frozenset({
    "broad_healthy_engagement", "niche_healthy_engagement", "growing_engagement",
    "low_repeat_engagement", "declining_engagement", "elevated_lapse",
    "inactive", "privacy_limited", "insufficient_evidence",
})

ALLOWED_FORECAST_SEGMENTS = frozenset({
    "growth_expected", "stable_outlook", "decline_expected",
    "low_usage_expected", "inactivity_expected", "uncertain_outlook",
    "insufficient_evidence",
})

ALLOWED_MODEL_HEALTH_SEGMENTS = frozenset({
    "healthy_model", "warning_model", "poor_model",
    "immature_production_evidence", "insufficient_evidence",
})

ALLOWED_DEPENDENCY_SEGMENTS = frozenset({
    "broadly_distributed", "moderately_concentrated", "highly_concentrated",
    "increasing_dependency", "privacy_limited", "insufficient_evidence",
})

ALLOWED_LIFECYCLE_SEGMENTS = frozenset({
    "newly_launched", "maturing", "established", "dormant",
    "planned_deprecation", "archived", "unknown",
})

ALLOWED_METADATA_SEGMENTS = frozenset({
    "metadata_complete", "metadata_partial", "metadata_limited",
    "metadata_missing", "invalid_metadata",
})

ALLOWED_PRIMARY_SEGMENTS = frozenset({
    "healthy_broad_adoption", "healthy_niche_adoption", "growing_report",
    "declining_report", "inactive_report", "elevated_lapse", "low_repeat_usage",
    "concentrated_dependency", "uncertain_forecast", "model_review_needed",
    "newly_launched", "planned_deprecation", "mixed_signals",
    "insufficient_evidence", "data_quality_issue",
})

PROHIBITED_SEGMENT_COLS = frozenset({
    "user_id", "email", "email_address", "user_name", "username",
    "display_name", "unique_user", "principal_name",
    "repeat_rate", "latest_views", "prior_views", "top_user_concentration",
    "niche",
})


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SegmentationConfig:
    # Usage statuses
    GROWING_STATUSES: frozenset = frozenset({"growing_usage"})
    STABLE_REGULAR_STATUSES: frozenset = frozenset({"stable_regular_usage"})
    STABLE_INTERMITTENT_STATUSES: frozenset = frozenset({"stable_intermittent_usage"})
    DECLINING_STATUSES: frozenset = frozenset({"declining_usage"})
    INACTIVE_STATUSES: frozenset = frozenset({"recently_inactive", "prolonged_inactivity"})
    NEWLY_ACTIVE_STATUSES: frozenset = frozenset({"newly_active"})
    USAGE_INSUFFICIENT_STATUSES: frozenset = frozenset({
        "insufficient_history", "no_valid_usage_data", "calculation_failed",
    })

    # Engagement thresholds
    HIGH_LAPSE_THRESHOLD: float = 0.40
    NICHE_MAX_UNIQUE_USERS_28D: int = 10
    NICHE_MIN_RETURNING_SHARE: float = 0.30

    ENGAGEMENT_INSUFFICIENT_STATUSES: frozenset = frozenset({
        "insufficient", "missing", "no_data", "calculation_failed",
    })
    ENGAGEMENT_PRIVACY_FULL_SUPPRESSION: frozenset = frozenset({
        "privacy_suppressed", "fully_suppressed",
    })
    ENGAGEMENT_INACTIVE_STATUSES: frozenset = frozenset({
        "recently_inactive", "prolonged_inactivity",
    })
    LOW_REPEAT_STATUSES: frozenset = frozenset({
        "low_repeat", "one_time_dominated",
    })

    # Forecast statuses
    FORECAST_GROWTH_STATUSES: frozenset = frozenset({"growth_expected"})
    FORECAST_STABLE_STATUSES: frozenset = frozenset({"stable_outlook", "mixed_outlook"})
    FORECAST_DECLINE_STATUSES: frozenset = frozenset({"decline_expected"})
    FORECAST_LOW_STATUSES: frozenset = frozenset({"low_usage_expected", "reactivation_expected"})
    FORECAST_INACTIVE_STATUSES: frozenset = frozenset({"inactivity_expected"})
    FORECAST_UNCERTAIN_STATUSES: frozenset = frozenset({"uncertain_outlook"})
    FORECAST_INSUFFICIENT_STATUSES: frozenset = frozenset({
        "insufficient_evidence", "invalid_forecast",
    })

    # Model health statuses
    HEALTHY_MODEL_STATUSES: frozenset = frozenset({"good", "healthy"})
    WARNING_MODEL_STATUSES: frozenset = frozenset({"watch", "warning"})
    POOR_MODEL_STATUSES: frozenset = frozenset({"poor"})
    IMMATURE_EVIDENCE_STATUSES: frozenset = frozenset({
        "immature_production", "insufficient_evidence",
    })
    INVALID_MODEL_STATUSES: frozenset = frozenset({
        "calculation_failed", "invalid", "missing",
    })

    # Dependency — actual values are long strings; we match by substring
    HIGH_DEPENDENCY_KEYWORDS: frozenset = frozenset({
        "high_dependency", "single_user_dependency",
    })
    MODERATE_DEPENDENCY_KEYWORDS: frozenset = frozenset({"moderate_dependency"})

    # Metadata thresholds
    METADATA_COMPLETE_THRESHOLD: float = 1.0
    METADATA_PARTIAL_THRESHOLD: float = 0.80
    METADATA_LIMITED_THRESHOLD: float = 0.50


_DEFAULT_CFG = SegmentationConfig()


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


def _dep_contains(status_val: str, keywords: frozenset) -> bool:
    """Substring match for long dependency_status strings."""
    if not status_val:
        return False
    lower = str(status_val).lower()
    return any(kw.lower() in lower for kw in keywords)


# ---------------------------------------------------------------------------
# Segment builders
# ---------------------------------------------------------------------------

def _build_usage_segment(feat_row: pd.Series, cfg: SegmentationConfig) -> str:
    status = _safe_get(feat_row, "historical_usage_status", "")
    if not status:
        return "insufficient_evidence"
    if status in cfg.USAGE_INSUFFICIENT_STATUSES:
        return "insufficient_evidence"
    if status in cfg.GROWING_STATUSES:
        return "growing_usage"
    if status in cfg.STABLE_REGULAR_STATUSES:
        return "stable_regular_usage"
    if status in cfg.STABLE_INTERMITTENT_STATUSES:
        return "stable_intermittent_usage"
    if status in cfg.DECLINING_STATUSES:
        return "declining_usage"
    if status in cfg.INACTIVE_STATUSES:
        return "inactive"
    if status in cfg.NEWLY_ACTIVE_STATUSES:
        return "newly_active"
    return "insufficient_evidence"


def _build_engagement_segment(eng_row: Optional[pd.Series], cfg: SegmentationConfig) -> str:
    if eng_row is None:
        return "insufficient_evidence"

    evidence_status = str(_safe_get(eng_row, "engagement_evidence_status", "") or "").lower()
    if evidence_status in cfg.ENGAGEMENT_INSUFFICIENT_STATUSES:
        return "insufficient_evidence"

    privacy_status = str(_safe_get(eng_row, "privacy_suppression_status", "") or "")
    if privacy_status in cfg.ENGAGEMENT_PRIVACY_FULL_SUPPRESSION:
        return "privacy_limited"

    overall_status = str(_safe_get(eng_row, "overall_engagement_status", "") or "").lower()
    if any(s in overall_status for s in cfg.ENGAGEMENT_INACTIVE_STATUSES):
        return "inactive"

    lapse_rate = _safe_get(eng_row, "lapse_rate_28d", None)
    if lapse_rate is not None:
        try:
            if float(lapse_rate) > cfg.HIGH_LAPSE_THRESHOLD:
                return "elevated_lapse"
        except (TypeError, ValueError):
            pass

    user_direction = str(_safe_get(eng_row, "active_user_direction_28d", "") or "")
    if user_direction == "declining":
        return "declining_engagement"

    repeat_status = str(_safe_get(eng_row, "repeat_engagement_status", "") or "")
    if repeat_status in cfg.LOW_REPEAT_STATUSES:
        return "low_repeat_engagement"

    if user_direction == "growing":
        return "growing_engagement"

    unique_users = _safe_get(eng_row, "unique_users_28d", None)
    returning_share = _safe_get(eng_row, "returning_user_share_28d", None)
    try:
        if (
            unique_users is not None
            and int(float(unique_users)) <= cfg.NICHE_MAX_UNIQUE_USERS_28D
            and returning_share is not None
            and float(returning_share) >= cfg.NICHE_MIN_RETURNING_SHARE
        ):
            return "niche_healthy_engagement"
    except (TypeError, ValueError):
        pass

    return "broad_healthy_engagement"


def _build_forecast_segment(outlook_row: Optional[pd.Series], cfg: SegmentationConfig) -> str:
    if outlook_row is None:
        return "insufficient_evidence"
    status = str(_safe_get(outlook_row, "forecast_outlook_status", "") or "")
    if not status:
        return "insufficient_evidence"
    if status in cfg.FORECAST_INSUFFICIENT_STATUSES:
        return "insufficient_evidence"
    if status in cfg.FORECAST_GROWTH_STATUSES:
        return "growth_expected"
    if status in cfg.FORECAST_STABLE_STATUSES:
        return "stable_outlook"
    if status in cfg.FORECAST_DECLINE_STATUSES:
        return "decline_expected"
    if status in cfg.FORECAST_LOW_STATUSES:
        return "low_usage_expected"
    if status in cfg.FORECAST_INACTIVE_STATUSES:
        return "inactivity_expected"
    if status in cfg.FORECAST_UNCERTAIN_STATUSES:
        return "uncertain_outlook"
    return "insufficient_evidence"


def _build_model_health_segment(health_row: Optional[pd.Series], cfg: SegmentationConfig) -> str:
    if health_row is None:
        return "insufficient_evidence"
    status = str(_safe_get(health_row, "model_diagnostic_status", "") or "")
    if not status:
        return "insufficient_evidence"
    if status in cfg.HEALTHY_MODEL_STATUSES:
        return "healthy_model"
    if status in cfg.WARNING_MODEL_STATUSES:
        return "warning_model"
    if status in cfg.POOR_MODEL_STATUSES:
        return "poor_model"
    if status in cfg.IMMATURE_EVIDENCE_STATUSES:
        return "immature_production_evidence"
    if status in cfg.INVALID_MODEL_STATUSES:
        return "insufficient_evidence"
    return "insufficient_evidence"


def _build_dependency_segment(eng_row: Optional[pd.Series], cfg: SegmentationConfig) -> str:
    if eng_row is None:
        return "insufficient_evidence"

    privacy_status = str(_safe_get(eng_row, "privacy_suppression_status", "") or "")
    suppressed_count = _safe_get(eng_row, "privacy_suppressed_field_count", 0)
    try:
        suppressed_count = int(float(suppressed_count)) if suppressed_count is not None else 0
    except (TypeError, ValueError):
        suppressed_count = 0

    if privacy_status in cfg.ENGAGEMENT_PRIVACY_FULL_SUPPRESSION or suppressed_count > 0:
        return "privacy_limited"

    dep_status = str(_safe_get(eng_row, "dependency_status", "") or "")
    if not dep_status:
        return "insufficient_evidence"

    if _dep_contains(dep_status, cfg.HIGH_DEPENDENCY_KEYWORDS):
        return "highly_concentrated"
    if _dep_contains(dep_status, cfg.MODERATE_DEPENDENCY_KEYWORDS):
        return "moderately_concentrated"

    concentration_dir = str(_safe_get(eng_row, "concentration_direction", "") or "")
    if concentration_dir == "increasing":
        return "increasing_dependency"

    return "broadly_distributed"


def _build_lifecycle_segment(feat_row: pd.Series, meta_row: Optional[pd.Series]) -> str:
    if meta_row is not None:
        dep_status = str(_safe_get(meta_row, "deprecation_status", "") or "").lower()
        if dep_status in {"deprecated", "planned_deprecation"}:
            return "planned_deprecation"
        meta_lc = str(_safe_get(meta_row, "report_lifecycle_status", "") or "").lower()
        if meta_lc == "archived":
            return "archived"

    lifecycle = str(_safe_get(feat_row, "report_lifecycle_status", "") or "").lower()
    adoption = str(_safe_get(feat_row, "adoption_maturity_status", "") or "").lower()

    if lifecycle == "established":
        return "established"
    if lifecycle == "maturing":
        return "maturing"
    if lifecycle in {"newly_launched", "newly_active"} or adoption in {"newly_launched", "newly_active"}:
        return "newly_launched"
    if lifecycle in {"dormant", "recently_inactive", "prolonged_inactivity"}:
        return "dormant"
    if lifecycle == "planned_deprecation":
        return "planned_deprecation"
    if lifecycle == "archived":
        return "archived"
    return "unknown"


def _build_metadata_segment(meta_row: Optional[pd.Series], cfg: SegmentationConfig) -> str:
    if meta_row is None:
        return "metadata_missing"

    interp = str(_safe_get(meta_row, "metadata_interpretation_status", "") or "").lower()
    if "invalid" in interp:
        return "invalid_metadata"

    score = _safe_get(meta_row, "metadata_completeness_score", None)
    if score is None:
        return "metadata_missing"
    try:
        score = float(score)
    except (TypeError, ValueError):
        return "metadata_missing"

    if score >= cfg.METADATA_COMPLETE_THRESHOLD:
        return "metadata_complete"
    if score >= cfg.METADATA_PARTIAL_THRESHOLD:
        return "metadata_partial"
    if score >= cfg.METADATA_LIMITED_THRESHOLD:
        return "metadata_limited"
    return "metadata_missing"


# ---------------------------------------------------------------------------
# Primary segment — 15-step deterministic precedence
# ---------------------------------------------------------------------------

def _determine_primary_segment(
    usage_seg: str,
    eng_seg: str,
    forecast_seg: str,
    model_seg: str,
    dep_seg: str,
    lifecycle_seg: str,
    meta_seg: str,
    diag_row: Optional[pd.Series],
    feat_row: pd.Series,
    cfg: SegmentationConfig,
) -> str:
    primary_diagnostic = str(_safe_get(diag_row, "primary_diagnostic", "") or "") if diag_row is not None else ""
    overall_diag_sev = str(_safe_get(diag_row, "overall_diagnostic_severity", "none") or "none") if diag_row is not None else "none"

    # 1. Data quality issue
    if usage_seg == "insufficient_evidence" and primary_diagnostic == "no_valid_data":
        return "data_quality_issue"

    # 2. Insufficient evidence
    if usage_seg == "insufficient_evidence":
        return "insufficient_evidence"

    # 3. Planned deprecation or archived
    if lifecycle_seg in {"planned_deprecation", "archived"}:
        return "planned_deprecation"

    # 4. Inactive
    if usage_seg == "inactive" or eng_seg == "inactive":
        return "inactive_report"

    # 5. Severe model review needed
    if model_seg == "poor_model" and overall_diag_sev == "poor":
        return "model_review_needed"

    # 6. Declining report
    if usage_seg == "declining_usage" or eng_seg == "declining_engagement":
        return "declining_report"

    # 7. Elevated lapse
    if eng_seg == "elevated_lapse":
        return "elevated_lapse"

    # 8. Concentrated dependency (only confirmed, not privacy-suppressed)
    if dep_seg == "highly_concentrated":
        return "concentrated_dependency"

    # 9. Low repeat usage
    if eng_seg == "low_repeat_engagement":
        return "low_repeat_usage"

    # 10. Uncertain forecast
    if forecast_seg == "uncertain_outlook" or model_seg == "warning_model":
        return "uncertain_forecast"

    # 11. Growing report
    if usage_seg == "growing_usage" or forecast_seg == "growth_expected":
        return "growing_report"

    # 12. Newly launched
    if lifecycle_seg == "newly_launched":
        return "newly_launched"

    # 13. Healthy broad adoption
    if eng_seg == "broad_healthy_engagement" and usage_seg in {"stable_regular_usage", "growing_usage"}:
        return "healthy_broad_adoption"

    # 14. Healthy niche adoption
    if eng_seg == "niche_healthy_engagement":
        return "healthy_niche_adoption"

    # 15. Mixed signals
    return "mixed_signals"


# ---------------------------------------------------------------------------
# Evidence and reasons
# ---------------------------------------------------------------------------

def _build_segment_evidence_status(
    features_row: pd.Series,
    forecast_row: Optional[pd.Series],
    health_row: Optional[pd.Series],
    engagement_row: Optional[pd.Series],
    metadata_row: Optional[pd.Series],
    diagnostics_row: Optional[pd.Series],
) -> str:
    count = sum([
        features_row is not None,
        forecast_row is not None,
        health_row is not None,
        engagement_row is not None,
        metadata_row is not None,
        diagnostics_row is not None,
    ])
    if count == 6:
        return "complete"
    if count == 5:
        return "mostly_complete"
    if count == 4:
        return "partial"
    return "limited"


def _build_segment_reasons(
    usage_seg: str, eng_seg: str, forecast_seg: str,
    model_seg: str, dep_seg: str, lifecycle_seg: str,
    meta_seg: str, primary_seg: str,
) -> str:
    return (
        f"usage:{usage_seg} | engagement:{eng_seg} | forecast:{forecast_seg} | "
        f"model:{model_seg} | dependency:{dep_seg} | lifecycle:{lifecycle_seg} | "
        f"metadata:{meta_seg} | primary:{primary_seg}"
    )


# ---------------------------------------------------------------------------
# Main build function
# ---------------------------------------------------------------------------

def build_report_segments(
    features_df: pd.DataFrame,
    forecast_df: Optional[pd.DataFrame],
    model_health_df: Optional[pd.DataFrame],
    engagement_df: Optional[pd.DataFrame],
    metadata_df: Optional[pd.DataFrame],
    diagnostics_df: Optional[pd.DataFrame],
    analytics_run_id: str,
    cfg: Optional[SegmentationConfig] = None,
) -> pd.DataFrame:
    """Assign multi-dimensional segments to every report in features_df."""
    cfg = cfg or _DEFAULT_CFG

    def _index(df: Optional[pd.DataFrame]) -> Optional[dict]:
        if df is None or df.empty:
            return None
        return {str(r["report_id"]): r for _, r in df.iterrows()}

    forecast_idx = _index(forecast_df)
    health_idx = _index(model_health_df)
    engagement_idx = _index(engagement_df)
    metadata_idx = _index(metadata_df)
    diagnostics_idx = _index(diagnostics_df)

    generated_at = datetime.utcnow().isoformat()
    rows = []

    for _, feat_row in features_df.iterrows():
        rid = str(feat_row.get("report_id", ""))

        outlook_row = forecast_idx.get(rid) if forecast_idx else None
        health_row = health_idx.get(rid) if health_idx else None
        eng_row = engagement_idx.get(rid) if engagement_idx else None
        meta_row = metadata_idx.get(rid) if metadata_idx else None
        diag_row = diagnostics_idx.get(rid) if diagnostics_idx else None

        usage_seg = _build_usage_segment(feat_row, cfg)
        eng_seg = _build_engagement_segment(eng_row, cfg)
        forecast_seg = _build_forecast_segment(outlook_row, cfg)
        model_seg = _build_model_health_segment(health_row, cfg)
        dep_seg = _build_dependency_segment(eng_row, cfg)
        lifecycle_seg = _build_lifecycle_segment(feat_row, meta_row)
        meta_seg = _build_metadata_segment(meta_row, cfg)

        primary_seg = _determine_primary_segment(
            usage_seg, eng_seg, forecast_seg, model_seg,
            dep_seg, lifecycle_seg, meta_seg, diag_row, feat_row, cfg,
        )

        evidence_status = _build_segment_evidence_status(
            feat_row, outlook_row, health_row, eng_row, meta_row, diag_row,
        )
        reasons = _build_segment_reasons(
            usage_seg, eng_seg, forecast_seg, model_seg,
            dep_seg, lifecycle_seg, meta_seg, primary_seg,
        )

        review_required = primary_seg in {
            "declining_report", "inactive_report", "model_review_needed",
            "elevated_lapse", "concentrated_dependency", "data_quality_issue",
        }

        rows.append({
            "analytics_run_id": analytics_run_id,
            "generated_at": generated_at,
            "analytics_as_of_date": feat_row.get("analytics_as_of_date", None),
            "report_id": rid,
            "report_name": feat_row.get("report_name", None),
            "usage_segment": usage_seg,
            "engagement_segment": eng_seg,
            "forecast_segment": forecast_seg,
            "model_health_segment": model_seg,
            "dependency_segment": dep_seg,
            "lifecycle_segment": lifecycle_seg,
            "metadata_segment": meta_seg,
            "primary_report_segment": primary_seg,
            "segment_evidence_status": evidence_status,
            "segment_reasons": reasons,
            "segment_review_required": review_required,
        })

    df = pd.DataFrame(rows, columns=REPORT_SEGMENTS_COLS)
    return df.sort_values("report_id").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_report_segments(df: pd.DataFrame) -> None:
    missing = [c for c in REPORT_SEGMENTS_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    bad_cols = set(df.columns) & PROHIBITED_SEGMENT_COLS
    if bad_cols:
        raise ValueError(f"Prohibited columns present: {bad_cols}")

    if df.duplicated(subset=["analytics_run_id", "report_id"]).any():
        raise ValueError("Duplicate (analytics_run_id, report_id) grain in segments")

    for col, allowed in [
        ("usage_segment", ALLOWED_USAGE_SEGMENTS),
        ("engagement_segment", ALLOWED_ENGAGEMENT_SEGMENTS),
        ("forecast_segment", ALLOWED_FORECAST_SEGMENTS),
        ("model_health_segment", ALLOWED_MODEL_HEALTH_SEGMENTS),
        ("dependency_segment", ALLOWED_DEPENDENCY_SEGMENTS),
        ("lifecycle_segment", ALLOWED_LIFECYCLE_SEGMENTS),
        ("metadata_segment", ALLOWED_METADATA_SEGMENTS),
        ("primary_report_segment", ALLOWED_PRIMARY_SEGMENTS),
    ]:
        invalid = set(df[col].dropna()) - allowed
        if invalid:
            raise ValueError(f"Invalid {col} values: {invalid}")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_report_segments(df: pd.DataFrame, project_root: Path) -> Path:
    validate_report_segments(df)
    out_dir = Path(project_root) / "outputs" / "analytics"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "report_segments.csv"
    df.sort_values(["analytics_run_id", "report_id"])[REPORT_SEGMENTS_COLS].to_csv(out_path, index=False)
    return out_path
