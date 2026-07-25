"""
Report engagement context layer for Sprint 7.

Extracts a canonical subset of mart_report_engagement.csv fields for use
in the Sprint 7 report analytics mart. Does not recalculate any engagement
metrics. Derives an interpretation status from existing fields.
"""
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import pandas as pd

ENGAGEMENT_CONTEXT_SCHEMA_VERSION = "1.0.0"

ALLOWED_INTERPRETATION_STATUSES = frozenset({
    "engagement_supported",
    "engagement_supported_with_privacy_limits",
    "partial_engagement_evidence",
    "insufficient_engagement_evidence",
    "no_valid_user_data",
    "invalid_temporal_alignment",
})

ALLOWED_ENGAGEMENT_STATUSES = frozenset({
    "healthy_broad_adoption", "healthy_niche_adoption", "growing_adoption",
    "stable_engagement", "declining_adoption", "low_repeat_usage",
    "concentrated_dependency", "elevated_lapse", "newly_active", "inactive",
    "mixed_signals", "privacy_limited", "insufficient_evidence",
    "no_valid_user_data", "calculation_failed",
})

ALLOWED_ENGAGEMENT_ACTIONS = frozenset({
    "continue_monitoring", "support_new_user_onboarding",
    "investigate_user_decline", "improve_repeat_engagement",
    "investigate_user_lapse", "review_concentrated_dependency",
    "assess_report_discoverability", "validate_report_audience",
    "monitor_new_adoption", "investigate_data_quality", "insufficient_evidence",
})

PROHIBITED_COLUMNS = frozenset({
    "user_key", "user_id", "email", "email_address", "display_name",
    "unique_user", "principal_name", "repeat_rate",
})

# Canonical field list for the engagement context output
ENGAGEMENT_CONTEXT_COLS = [
    # Identity and lineage
    "analytics_run_id", "generated_at", "analytics_as_of_date",
    "report_id", "report_name", "workspace_id",
    # Evidence and privacy
    "engagement_evidence_status", "user_data_quality_status",
    "missing_engagement_evidence", "privacy_suppression_status",
    "privacy_suppressed_field_count", "privacy_suppressed_fields",
    # Breadth
    "unique_users_7d", "unique_users_28d", "unique_users_previous_28d",
    "active_user_change_28d", "active_user_change_28d_pct",
    "active_user_direction_28d", "breadth_status",
    # Repeat engagement
    "returning_user_share_28d", "one_time_user_share_28d",
    "returning_user_share_change_28d", "median_active_days_per_user_28d",
    "repeat_engagement_status",
    # Cohorts
    "newly_adopted_users_28d", "retained_users_28d", "reactivated_users_28d",
    "lapsed_users_28d", "retained_user_rate_28d", "lapse_rate_28d",
    "adoption_transition_status",
    # Frequency
    "views_per_active_user_28d", "views_per_user_day_28d",
    "median_views_per_user_28d", "median_return_gap_days_28d",
    "frequency_direction", "usage_pattern_status",
    # Concentration
    "top_1_user_view_share_28d", "top_3_users_view_share_28d",
    "user_view_hhi_28d", "effective_user_count_28d", "effective_user_share_28d",
    "concentration_direction", "dependency_status",
    # Summary
    "overall_engagement_status", "primary_engagement_issue",
    "engagement_issue_count", "recommended_engagement_action",
    "engagement_action_priority", "engagement_reasons",
    "engagement_review_required",
    # Interpretation
    "engagement_interpretation_status", "temporal_alignment_status",
    "temporal_alignment_reasons",
]

# Column mapping: ENGAGEMENT_CONTEXT_COLS name -> mart column name (if different)
# Based on actual mart columns observed in STEP 0.
# All other context columns exist verbatim in the mart.
_MART_COLUMN_ALIASES: dict[str, str] = {
    "engagement_review_required": "review_required",
}


def _check_temporal_alignment(
    engagement_df: pd.DataFrame,
    features_df: Optional[pd.DataFrame],
) -> tuple[str, Optional[str]]:
    """Return (alignment_status, reasons)."""
    if engagement_df is None or len(engagement_df) == 0:
        return "missing_engagement", None
    if features_df is None:
        return "missing_features", None

    eng_date = str(engagement_df["analytics_as_of_date"].iloc[0])
    feat_date = str(features_df["analytics_as_of_date"].iloc[0])

    if eng_date == feat_date:
        return "aligned", None
    return "mismatched", f"engagement={eng_date},features={feat_date}"


def _classify_engagement_interpretation(row: pd.Series, alignment_status: str) -> str:
    """Derive interpretation status from row fields and alignment status."""
    # 1. Temporal mismatch overrides everything
    if alignment_status == "mismatched":
        return "invalid_temporal_alignment"

    # 2. No valid user data
    if (
        row.get("user_data_quality_status") == "no_valid_user_data"
        or row.get("overall_engagement_status") == "no_valid_user_data"
    ):
        return "no_valid_user_data"

    # 3. Insufficient evidence
    evidence_status = row.get("engagement_evidence_status", "")
    overall_status = row.get("overall_engagement_status", "")
    if evidence_status in ("insufficient_history", "no_valid_user_data") or overall_status == "insufficient_evidence":
        return "insufficient_engagement_evidence"

    # 4. Partial evidence
    if evidence_status in ("recent_window_only", "partial_history"):
        return "partial_engagement_evidence"

    # 5. Privacy limits
    suppressed_count = row.get("privacy_suppressed_field_count", 0)
    try:
        suppressed_count = int(suppressed_count) if pd.notna(suppressed_count) else 0
    except (TypeError, ValueError):
        suppressed_count = 0
    if suppressed_count > 0:
        return "engagement_supported_with_privacy_limits"

    # 6. Default
    return "engagement_supported"


def _extract_engagement_row(
    mart_row: pd.Series,
    alignment_status: str,
    alignment_reasons: Optional[str],
    col_map: dict,
    run_id: str,
) -> dict:
    """Extract one output row from one mart row."""
    out: dict = {}

    for ctx_col in ENGAGEMENT_CONTEXT_COLS:
        # Skip derived fields — handled below
        if ctx_col in ("generated_at", "engagement_interpretation_status",
                        "temporal_alignment_status", "temporal_alignment_reasons"):
            continue

        mart_col = col_map.get(ctx_col, ctx_col)

        if mart_col in mart_row.index:
            val = mart_row[mart_col]
            # Normalize NaN to None
            out[ctx_col] = None if (isinstance(val, float) and pd.isna(val)) else val
        else:
            out[ctx_col] = None

    # Override analytics_run_id: prefer mart value, fall back to provided run_id
    if out.get("analytics_run_id") is None:
        out["analytics_run_id"] = run_id

    # Derived fields
    out["generated_at"] = datetime.now(timezone.utc).isoformat()
    out["engagement_interpretation_status"] = _classify_engagement_interpretation(mart_row, alignment_status)
    out["temporal_alignment_status"] = alignment_status
    out["temporal_alignment_reasons"] = alignment_reasons

    # Safety: never expose prohibited columns
    for bad_col in PROHIBITED_COLUMNS:
        out.pop(bad_col, None)

    return out


def build_report_engagement_context(
    engagement_df: pd.DataFrame,
    features_df: Optional[pd.DataFrame],
    run_id: str,
) -> pd.DataFrame:
    """
    Build the engagement context layer from the engagement mart.

    Does not recalculate metrics. Extracts canonical fields, derives
    interpretation status, and checks temporal alignment.
    """
    if engagement_df is None or len(engagement_df) == 0:
        return pd.DataFrame(columns=ENGAGEMENT_CONTEXT_COLS)

    alignment_status, alignment_reasons = _check_temporal_alignment(engagement_df, features_df)

    # Build column map: context name -> actual mart column name
    col_map = dict(_MART_COLUMN_ALIASES)  # copy so we don't mutate the module-level dict

    rows = []
    for _, mart_row in engagement_df.iterrows():
        row_out = _extract_engagement_row(
            mart_row, alignment_status, alignment_reasons, col_map, run_id
        )
        rows.append(row_out)

    df = pd.DataFrame(rows)[ENGAGEMENT_CONTEXT_COLS]
    df = df.sort_values("report_id").reset_index(drop=True)
    return df


def validate_report_engagement_context(df: pd.DataFrame) -> None:
    """
    Validate the engagement context DataFrame.

    Raises ValueError on any violation.
    """
    # All required columns present
    missing_cols = [c for c in ENGAGEMENT_CONTEXT_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Unique grain
    if df.duplicated(subset=["analytics_run_id", "report_id"]).any():
        raise ValueError("Duplicate (analytics_run_id, report_id) rows found")

    # No prohibited columns
    bad_cols = set(df.columns) & PROHIBITED_COLUMNS
    if bad_cols:
        raise ValueError(f"Prohibited columns present: {sorted(bad_cols)}")

    # repeat_rate must not be present
    if "repeat_rate" in df.columns:
        raise ValueError("Column 'repeat_rate' must not be present")

    # Interpretation status values
    if "engagement_interpretation_status" in df.columns:
        bad = set(df["engagement_interpretation_status"].dropna()) - ALLOWED_INTERPRETATION_STATUSES
        if bad:
            raise ValueError(f"Invalid engagement_interpretation_status values: {sorted(bad)}")

    # Overall engagement status values
    if "overall_engagement_status" in df.columns:
        bad = set(df["overall_engagement_status"].dropna()) - ALLOWED_ENGAGEMENT_STATUSES
        if bad:
            raise ValueError(f"Invalid overall_engagement_status values: {sorted(bad)}")

    # Recommended action values
    if "recommended_engagement_action" in df.columns:
        bad = set(df["recommended_engagement_action"].dropna()) - ALLOWED_ENGAGEMENT_ACTIONS
        if bad:
            raise ValueError(f"Invalid or prohibited recommended_engagement_action values: {sorted(bad)}")

    # No retire/delete actions (belt-and-suspenders)
    prohibited_actions = {"retire_report", "delete_report", "restrict_user",
                          "contact_specific_user", "automatic_intervention"}
    if "recommended_engagement_action" in df.columns:
        bad_actions = set(df["recommended_engagement_action"].dropna()) & prohibited_actions
        if bad_actions:
            raise ValueError(f"Prohibited actions present: {sorted(bad_actions)}")

    # no_valid_user_data must not coexist with 'inactive' overall status
    if "engagement_interpretation_status" in df.columns and "overall_engagement_status" in df.columns:
        bad_combo = df[
            (df["engagement_interpretation_status"] == "no_valid_user_data") &
            (df["overall_engagement_status"] == "inactive")
        ]
        if len(bad_combo) > 0:
            raise ValueError(
                "engagement_interpretation_status='no_valid_user_data' must not "
                "have overall_engagement_status='inactive'"
            )

    # Privacy warning (best effort — warn, don't fail)
    if all(c in df.columns for c in ["privacy_suppression_status", "privacy_suppressed_field_count",
                                      "concentration_privacy_suppressed", "top_1_user_view_share_28d"]):
        suppressed_concentration = df[
            (df.get("concentration_privacy_suppressed", pd.Series(False, index=df.index)) == True) &
            (df["top_1_user_view_share_28d"].notna())
        ]
        if len(suppressed_concentration) > 0:
            import warnings
            warnings.warn(
                f"{len(suppressed_concentration)} rows have concentration privacy suppressed "
                "but top_1_user_view_share_28d is not null",
                UserWarning,
                stacklevel=2,
            )


def persist_report_engagement_context(df: pd.DataFrame, project_root: Path) -> Path:
    """Validate, write, and return path to the engagement context CSV."""
    validate_report_engagement_context(df)

    out_dir = project_root / "outputs" / "analytics"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "report_engagement_context.csv"

    sorted_df = df.sort_values(["analytics_run_id", "report_id"]).reset_index(drop=True)
    sorted_df[ENGAGEMENT_CONTEXT_COLS].to_csv(out_path, index=False)

    return out_path
