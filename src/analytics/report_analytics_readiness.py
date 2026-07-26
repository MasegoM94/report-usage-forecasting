"""
Prerequisite readiness validator for Sprint 7 report analytics.

Validates that report_features.csv, mart_report_engagement.csv, and
report_model_diagnostics.csv are present, schema-valid, lineage-complete,
and temporally aligned before the Sprint 7 mart build begins.
"""
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Optional
import pandas as pd

READINESS_SCHEMA_VERSION = "1.0.0"

# Canonical field sets (what Sprint 7 requires from each prerequisite)
REPORT_FEATURES_REQUIRED_COLS = frozenset({
    # v2.0.0 schema (evidence-aware historical feature layer)
    "report_id", "analytics_run_id", "generated_at", "analytics_as_of_date",
    "schema_version",
    "recent_28d_views", "previous_28d_views",
    "history_sufficient_28d", "comparison_history_sufficient_28d",
    "usage_direction_28d", "usage_change_materiality",
    "historical_usage_status", "primary_historical_usage_issue",
})
REPORT_FEATURES_DEPRECATED_COLS = frozenset({
    # v1.x columns no longer present in v2
    "usage_change_pct", "latest_views", "prior_views", "top_user_concentration",
    "repeat_rate", "avg_views", "days_active", "top_1_user_view_share",
    "trend_history_sufficient", "usage_trend_12w_slope", "newly_active_flag",
})

ENGAGEMENT_MART_REQUIRED_COLS = frozenset({
    "analytics_run_id", "report_id", "analytics_as_of_date",
    "unique_users_28d", "overall_engagement_status", "engagement_evidence_status",
    "recommended_engagement_action",
})
ENGAGEMENT_PROHIBITED_COLS = frozenset({
    "user_key", "user_id", "email", "email_address", "display_name",
    "unique_user", "principal_name",
})

FORECAST_OUTLOOK_REQUIRED_COLS = frozenset({
    "forecast_run_id", "report_id", "forecast_as_of_date",
    "forecast_horizon_sufficient_28d", "forecast_evidence_status",
    "forecast_outlook_status",
})

MODEL_DIAGNOSTICS_REQUIRED_COLS = frozenset({
    "report_id", "generated_at",
    "model_diagnostic_status", "primary_model_issue", "recommended_model_action",
})

MODEL_HEALTH_CONTEXT_REQUIRED_COLS = frozenset({
    "diagnostic_run_id", "report_id", "model_diagnostic_status",
    "forecast_interpretation_status", "recommended_model_action",
})

ALLOWED_READINESS_STATUSES = frozenset({
    "ready", "stale", "schema_mismatch", "missing",
    "invalid_grain", "incomplete_lineage", "generation_failed",
})

ALLOWED_RECONCILIATION_STATUSES = frozenset({
    "complete", "expected_missing_forecast", "expected_missing_engagement",
    "missing_required_source", "orphan_source_record", "duplicate_report",
    "inactive_report",
})

ENGAGEMENT_CONTEXT_REQUIRED_COLS = frozenset({
    "analytics_run_id", "report_id", "analytics_as_of_date",
    "engagement_evidence_status", "overall_engagement_status",
    "engagement_interpretation_status",
})

METADATA_CONTEXT_REQUIRED_COLS = frozenset({
    "analytics_run_id", "report_id", "analytics_as_of_date",
    "metadata_completeness_score", "metadata_evidence_status",
    "metadata_interpretation_status",
})

READINESS_OUTPUT_COLS = [
    "prerequisite_name", "file_path", "file_exists", "schema_valid",
    "grain_valid", "lineage_valid", "freshness_status", "row_count",
    "report_count", "analytics_as_of_date", "generated_at", "schema_version",
    "readiness_status", "readiness_reasons",
]

RECONCILIATION_OUTPUT_COLS = [
    "report_id", "report_name", "in_report_metadata", "in_report_features",
    "in_engagement_mart", "in_model_diagnostics", "in_production_forecast",
    "active_report", "missing_sources", "reconciliation_status",
    "reconciliation_reasons",
]


def validate_report_features_readiness(
    file_path: Path,
    max_staleness_days: int = 7,
) -> dict:
    name = "report_features"
    reasons = []

    if not file_path.exists():
        return _missing_result(name, file_path)

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return _failed_result(name, file_path, f"Cannot read file: {e}")

    # Schema check
    present_cols = set(df.columns)
    missing_required = REPORT_FEATURES_REQUIRED_COLS - present_cols
    deprecated_present = REPORT_FEATURES_DEPRECATED_COLS & present_cols
    schema_valid = len(missing_required) == 0
    if missing_required:
        reasons.append(f"missing_required_columns:{sorted(missing_required)}")
    if deprecated_present:
        reasons.append(f"deprecated_columns_present:{sorted(deprecated_present)}")

    # Grain check
    grain_valid = not df.duplicated(subset=["report_id"]).any()
    if not grain_valid:
        reasons.append("duplicate_report_id")

    # Lineage check
    lineage_valid = "analytics_run_id" in present_cols and "generated_at" in present_cols
    if not lineage_valid:
        reasons.append("missing_lineage_fields")

    # Freshness check
    freshness_status, _ = _check_freshness(df, "generated_at", max_staleness_days)
    if freshness_status == "stale":
        reasons.append(f"stale_output:{_}")

    # Use source-derived analytics_as_of_date (not generated_at) for temporal alignment
    as_of = df["analytics_as_of_date"].iloc[0] if "analytics_as_of_date" in df.columns and len(df) > 0 else None

    return _build_result(
        name, file_path, df, schema_valid, grain_valid, lineage_valid,
        freshness_status, as_of, reasons
    )


def validate_engagement_mart_readiness(
    file_path: Path,
    max_staleness_days: int = 7,
) -> dict:
    name = "engagement_mart"
    if not file_path.exists():
        return _missing_result(name, file_path)

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return _failed_result(name, file_path, f"Cannot read file: {e}")

    reasons = []
    present_cols = set(df.columns)

    # Schema
    missing_required = ENGAGEMENT_MART_REQUIRED_COLS - present_cols
    prohibited_present = ENGAGEMENT_PROHIBITED_COLS & present_cols
    schema_valid = len(missing_required) == 0 and len(prohibited_present) == 0
    if missing_required:
        reasons.append(f"missing_required_columns:{sorted(missing_required)}")
    if prohibited_present:
        reasons.append(f"prohibited_identifier_columns:{sorted(prohibited_present)}")

    # Grain
    key_cols = [c for c in ["analytics_run_id", "report_id"] if c in present_cols]
    grain_valid = not df.duplicated(subset=key_cols).any() if key_cols else False
    if not grain_valid:
        reasons.append(f"duplicate_grain:{key_cols}")

    # Lineage
    lineage_valid = all(c in present_cols for c in ["analytics_run_id", "generated_at"])
    if not lineage_valid:
        reasons.append("missing_lineage_fields")

    # Privacy check
    if "user_key" in df.columns:
        reasons.append("user_key_in_output")
        schema_valid = False

    # Status check
    if "overall_engagement_status" in df.columns:
        bad = set(df["overall_engagement_status"].dropna()) - {
            "healthy_broad_adoption", "healthy_niche_adoption", "growing_adoption",
            "stable_engagement", "declining_adoption", "low_repeat_usage",
            "concentrated_dependency", "elevated_lapse", "newly_active",
            "inactive", "mixed_signals", "privacy_limited", "insufficient_evidence",
            "no_valid_user_data", "calculation_failed",
        }
        if bad:
            reasons.append(f"invalid_engagement_status:{sorted(bad)}")
            schema_valid = False

    # Action check
    if "recommended_engagement_action" in df.columns:
        prohibited_actions = {"retire_report", "delete_report", "restrict_user",
                              "contact_specific_user", "automatic_intervention"}
        bad_actions = set(df["recommended_engagement_action"].dropna()) & prohibited_actions
        if bad_actions:
            reasons.append(f"prohibited_action:{sorted(bad_actions)}")
            schema_valid = False

    freshness_status, as_of = _check_freshness(df, "generated_at", max_staleness_days)
    if freshness_status == "stale":
        reasons.append(f"stale_output:{as_of}")

    as_of_date = df["analytics_as_of_date"].iloc[0] if "analytics_as_of_date" in df.columns and len(df) > 0 else None

    return _build_result(
        name, file_path, df, schema_valid, grain_valid, lineage_valid,
        freshness_status, as_of_date, reasons
    )


def validate_model_diagnostics_readiness(
    file_path: Path,
    max_staleness_days: int = 30,
) -> dict:
    name = "model_diagnostics"
    if not file_path.exists():
        return _missing_result(name, file_path)

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return _failed_result(name, file_path, f"Cannot read file: {e}")

    reasons = []
    present_cols = set(df.columns)

    missing_required = MODEL_DIAGNOSTICS_REQUIRED_COLS - present_cols
    schema_valid = len(missing_required) == 0
    if missing_required:
        reasons.append(f"missing_required_columns:{sorted(missing_required)}")

    grain_valid = not df.duplicated(subset=["report_id"]).any()
    if not grain_valid:
        reasons.append("duplicate_report_id")

    lineage_valid = "generated_at" in present_cols
    if not lineage_valid:
        reasons.append("missing_lineage_fields")

    # automatic_retraining_triggered must always be False
    if "automatic_retraining_triggered" in df.columns:
        if df["automatic_retraining_triggered"].any():
            reasons.append("automatic_retraining_triggered_is_true")
            schema_valid = False

    freshness_status, as_of = _check_freshness(df, "generated_at", max_staleness_days)
    if freshness_status == "stale":
        reasons.append(f"stale_output:{as_of}")

    training_cutoff = df["training_cutoff"].max() if "training_cutoff" in df.columns and len(df) > 0 else None

    return _build_result(
        name, file_path, df, schema_valid, grain_valid, lineage_valid,
        freshness_status, training_cutoff, reasons
    )


def validate_model_health_context_readiness(
    file_path: Path,
    max_staleness_days: int = 30,
) -> dict:
    name = "model_health_context"
    if not file_path.exists():
        return _missing_result(name, file_path)
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return _failed_result(name, file_path, f"Cannot read: {e}")

    reasons = []
    present_cols = set(df.columns)
    missing = MODEL_HEALTH_CONTEXT_REQUIRED_COLS - present_cols
    schema_valid = len(missing) == 0
    if missing:
        reasons.append(f"missing_required_columns:{sorted(missing)}")

    grain_valid = not df.duplicated(subset=["report_id"]).any()
    if not grain_valid:
        reasons.append("duplicate_report_id")

    lineage_valid = "diagnostic_run_id" in present_cols and "generated_at" in present_cols
    if not lineage_valid:
        reasons.append("missing_lineage_fields")

    # Safety check
    if "automatic_retraining_triggered" in present_cols:
        if df["automatic_retraining_triggered"].any():
            reasons.append("automatic_retraining_triggered_is_true")
            schema_valid = False

    freshness_status, as_of = _check_freshness(df, "generated_at", max_staleness_days)
    if freshness_status == "stale":
        reasons.append(f"stale_output:{as_of}")

    diag_run_at = df["generated_at"].max() if "generated_at" in present_cols and len(df) > 0 else None

    return _build_result(
        name, file_path, df, schema_valid, grain_valid, lineage_valid,
        freshness_status, diag_run_at, reasons
    )


def validate_forecast_outlook_readiness(
    file_path: Path,
    features_as_of_date: Optional[str] = None,
    max_staleness_days: int = 30,
) -> dict:
    name = "forecast_outlook"
    if not file_path.exists():
        return _missing_result(name, file_path)
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return _failed_result(name, file_path, f"Cannot read: {e}")

    reasons = []
    present_cols = set(df.columns)
    missing = FORECAST_OUTLOOK_REQUIRED_COLS - present_cols
    schema_valid = len(missing) == 0
    if missing:
        reasons.append(f"missing_required_columns:{sorted(missing)}")

    grain_valid = not df.duplicated(subset=["report_id"]).any()
    if not grain_valid:
        reasons.append("duplicate_report_id")

    lineage_valid = all(c in present_cols for c in ["forecast_run_id", "forecast_as_of_date"])
    if not lineage_valid:
        reasons.append("missing_lineage_fields")

    # Temporal alignment check
    if features_as_of_date and "forecast_as_of_date" in present_cols and len(df) > 0:
        fc_as_of = str(df["forecast_as_of_date"].iloc[0])
        if fc_as_of != str(features_as_of_date):
            reasons.append(
                f"as_of_mismatch:features={features_as_of_date},forecast={fc_as_of}"
            )

    freshness_status, _ = _check_freshness(df, "generated_at", max_staleness_days)
    if freshness_status == "stale":
        reasons.append(f"stale_output:{_}")

    fc_as_of_date = (
        df["forecast_as_of_date"].iloc[0]
        if "forecast_as_of_date" in present_cols and len(df) > 0
        else None
    )

    return _build_result(
        name, file_path, df, schema_valid, grain_valid, lineage_valid,
        freshness_status, fc_as_of_date, reasons
    )


def validate_engagement_context_readiness(
    file_path: Path,
    features_as_of_date: Optional[str] = None,
    max_staleness_days: int = 7,
) -> dict:
    name = "engagement_context"
    if not file_path.exists():
        return _missing_result(name, file_path)
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return _failed_result(name, file_path, f"Cannot read: {e}")

    reasons = []
    present_cols = set(df.columns)

    # Schema
    missing = ENGAGEMENT_CONTEXT_REQUIRED_COLS - present_cols
    schema_valid = len(missing) == 0
    if missing:
        reasons.append(f"missing_required_columns:{sorted(missing)}")

    # Privacy check
    prohibited = {"user_key", "user_id", "email", "repeat_rate"} & present_cols
    if prohibited:
        reasons.append(f"prohibited_columns_present:{sorted(prohibited)}")
        schema_valid = False

    # Grain
    grain_valid = not df.duplicated(subset=["report_id"]).any()
    if not grain_valid:
        reasons.append("duplicate_report_id")

    # Lineage
    lineage_valid = all(c in present_cols for c in ["analytics_run_id", "analytics_as_of_date"])
    if not lineage_valid:
        reasons.append("missing_lineage_fields")

    # Temporal alignment
    if features_as_of_date and "analytics_as_of_date" in present_cols and len(df) > 0:
        eng_as_of = str(df["analytics_as_of_date"].iloc[0])
        if eng_as_of != str(features_as_of_date):
            reasons.append(f"as_of_mismatch:features={features_as_of_date},engagement={eng_as_of}")

    freshness_status, _ = _check_freshness(df, "generated_at", max_staleness_days)
    as_of_date = df["analytics_as_of_date"].iloc[0] if "analytics_as_of_date" in present_cols and len(df) > 0 else None

    return _build_result(
        name, file_path, df, schema_valid, grain_valid, lineage_valid,
        freshness_status, as_of_date, reasons
    )


def validate_metadata_context_readiness(
    file_path: Path,
    max_staleness_days: int = 30,
) -> dict:
    name = "metadata_context"
    if not file_path.exists():
        return _missing_result(name, file_path)
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return _failed_result(name, file_path, f"Cannot read: {e}")

    reasons = []
    present_cols = set(df.columns)
    missing = METADATA_CONTEXT_REQUIRED_COLS - present_cols
    schema_valid = len(missing) == 0
    if missing:
        reasons.append(f"missing_required_columns:{sorted(missing)}")

    # Privacy: no emails
    import re
    email_pat = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
    for col in df.select_dtypes(include="object").columns:
        sample = df[col].dropna().astype(str).head(5)
        for val in sample:
            if email_pat.search(val):
                reasons.append(f"email_detected_in_col:{col}")
                schema_valid = False
                break

    grain_valid = not df.duplicated(subset=["report_id"]).any()
    if not grain_valid:
        reasons.append("duplicate_report_id")

    lineage_valid = all(c in present_cols for c in ["analytics_run_id", "analytics_as_of_date"])
    if not lineage_valid:
        reasons.append("missing_lineage_fields")

    freshness_status, _ = _check_freshness(df, "generated_at", max_staleness_days)
    as_of_date = df["analytics_as_of_date"].iloc[0] if "analytics_as_of_date" in present_cols and len(df) > 0 else None

    return _build_result(
        name, file_path, df, schema_valid, grain_valid, lineage_valid,
        freshness_status, as_of_date, reasons
    )


REPORT_DIAGNOSTICS_REQUIRED_COLS = frozenset({
    "analytics_run_id", "report_id", "analytics_as_of_date",
    "primary_diagnostic", "overall_diagnostic_severity",
    "recommended_diagnostic_action",
})


def validate_report_diagnostics_readiness(
    file_path: Path,
    max_staleness_days: int = 7,
) -> dict:
    name = "report_diagnostics"
    if not file_path.exists():
        return _missing_result(name, file_path)
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return _failed_result(name, file_path, f"Cannot read: {e}")

    reasons = []
    present = set(df.columns)
    missing = REPORT_DIAGNOSTICS_REQUIRED_COLS - present
    schema_valid = not missing
    if missing:
        reasons.append(f"missing_cols:{sorted(missing)}")

    # No prohibited actions
    if "recommended_diagnostic_action" in present:
        from src.analytics.report_diagnostics import PROHIBITED_ACTIONS
        bad = set(df["recommended_diagnostic_action"].dropna()) & PROHIBITED_ACTIONS
        if bad:
            schema_valid = False
            reasons.append(f"prohibited_actions:{sorted(bad)}")

    grain_valid = not df.duplicated(subset=["report_id"]).any()
    if not grain_valid:
        reasons.append("duplicate_report_id")

    lineage_valid = all(c in present for c in ["analytics_run_id", "analytics_as_of_date"])
    if not lineage_valid:
        reasons.append("missing_lineage")

    freshness_status, _ = _check_freshness(df, "generated_at", max_staleness_days)
    as_of = df["analytics_as_of_date"].iloc[0] if "analytics_as_of_date" in present and len(df) > 0 else None

    return _build_result(name, file_path, df, schema_valid, grain_valid, lineage_valid,
                         freshness_status, as_of, reasons)


def validate_report_analytics_prerequisites(
    project_root: Path,
    max_feature_staleness_days: int = 7,
    max_engagement_staleness_days: int = 7,
    max_diagnostics_staleness_days: int = 30,
    max_forecast_outlook_staleness_days: int = 30,
) -> list[dict]:
    results = []
    results.append(validate_report_features_readiness(
        project_root / "outputs" / "metrics" / "report_features.csv",
        max_feature_staleness_days,
    ))
    results.append(validate_engagement_mart_readiness(
        project_root / "outputs" / "analytics" / "mart_report_engagement.csv",
        max_engagement_staleness_days,
    ))
    results.append(validate_model_diagnostics_readiness(
        project_root / "outputs" / "diagnostics" / "report_model_diagnostics.csv",
        max_diagnostics_staleness_days,
    ))
    results.append(validate_forecast_outlook_readiness(
        project_root / "outputs" / "analytics" / "report_forecast_outlook.csv",
        max_staleness_days=max_forecast_outlook_staleness_days,
    ))
    results.append(validate_model_health_context_readiness(
        project_root / "outputs" / "analytics" / "report_model_health_context.csv",
    ))
    results.append(validate_engagement_context_readiness(
        project_root / "outputs" / "analytics" / "report_engagement_context.csv",
    ))
    results.append(validate_metadata_context_readiness(
        project_root / "outputs" / "analytics" / "report_metadata_context.csv",
    ))
    results.append(validate_report_diagnostics_readiness(
        project_root / "outputs" / "analytics" / "report_diagnostics.csv",
    ))
    return results


def build_report_analytics_readiness_summary(
    results: list[dict],
    run_id: Optional[str] = None,
) -> pd.DataFrame:
    run_id = run_id or str(uuid.uuid4())
    rows = []
    for r in results:
        rows.append({
            "prerequisite_name": r["prerequisite_name"],
            "file_path": r["file_path"],
            "file_exists": r["file_exists"],
            "schema_valid": r["schema_valid"],
            "grain_valid": r["grain_valid"],
            "lineage_valid": r["lineage_valid"],
            "freshness_status": r["freshness_status"],
            "row_count": r["row_count"],
            "report_count": r["report_count"],
            "analytics_as_of_date": r["analytics_as_of_date"],
            "generated_at": r["generated_at"],
            "schema_version": READINESS_SCHEMA_VERSION,
            "readiness_status": r["readiness_status"],
            "readiness_reasons": r["readiness_reasons"],
        })
    df = pd.DataFrame(rows)[READINESS_OUTPUT_COLS]
    return df


def build_report_spine_reconciliation(
    dim_report_df: pd.DataFrame,
    report_features_df: Optional[pd.DataFrame],
    engagement_mart_df: Optional[pd.DataFrame],
    model_diagnostics_df: Optional[pd.DataFrame],
    production_forecast_df: Optional[pd.DataFrame],
) -> pd.DataFrame:
    """Reconcile report populations across all prerequisite sources."""
    spine = dim_report_df[["report_id"]].copy()
    if "report_name" in dim_report_df.columns:
        spine["report_name"] = dim_report_df["report_name"]
    else:
        spine["report_name"] = spine["report_id"]

    feat_ids = set(report_features_df["report_id"]) if report_features_df is not None else set()
    eng_ids = set(engagement_mart_df["report_id"]) if engagement_mart_df is not None else set()
    diag_ids = set(model_diagnostics_df["report_id"]) if model_diagnostics_df is not None else set()
    fc_ids = set(production_forecast_df["report_id"]) if production_forecast_df is not None else set()
    meta_ids = set(spine["report_id"])

    all_ids = meta_ids | feat_ids | eng_ids | diag_ids | fc_ids

    rows = []
    for rid in sorted(all_ids):
        in_meta = rid in meta_ids
        in_feat = rid in feat_ids
        in_eng = rid in eng_ids
        in_diag = rid in diag_ids
        in_fc = rid in fc_ids

        name_row = dim_report_df[dim_report_df["report_id"] == rid]
        report_name = name_row["report_name"].iloc[0] if len(name_row) > 0 and "report_name" in name_row.columns else None

        # Active status
        active = True
        if "retire_date" in dim_report_df.columns and len(name_row) > 0:
            rd = name_row["retire_date"].iloc[0]
            active = pd.isna(rd)

        missing = []
        if not in_meta:
            missing.append("report_metadata")
        if not in_feat:
            missing.append("report_features")
        if not in_eng:
            missing.append("engagement_mart")
        if not in_diag:
            missing.append("model_diagnostics")
        if not in_fc:
            missing.append("production_forecast")

        # Classify
        if not in_meta and (in_feat or in_eng or in_diag):
            status = "orphan_source_record"
            reasons = ["report_id_not_in_dim_report"]
        elif not active:
            status = "inactive_report"
            reasons = ["retire_date_set"]
        elif not in_feat:
            status = "missing_required_source"
            reasons = ["missing_report_features"]
        elif not in_eng and engagement_mart_df is not None:
            status = "expected_missing_engagement"
            reasons = ["no_valid_user_activity"]
        elif not in_fc and production_forecast_df is not None:
            status = "expected_missing_forecast"
            reasons = ["insufficient_forecast_data"]
        elif len(missing) == 0:
            status = "complete"
            reasons = []
        else:
            status = "missing_required_source"
            reasons = [f"missing:{m}" for m in missing]

        rows.append({
            "report_id": rid,
            "report_name": report_name,
            "in_report_metadata": in_meta,
            "in_report_features": in_feat,
            "in_engagement_mart": in_eng,
            "in_model_diagnostics": in_diag,
            "in_production_forecast": in_fc,
            "active_report": active,
            "missing_sources": ",".join(sorted(missing)) if missing else None,
            "reconciliation_status": status,
            "reconciliation_reasons": "|".join(reasons) if reasons else None,
        })

    return pd.DataFrame(rows)[RECONCILIATION_OUTPUT_COLS]


def persist_readiness_outputs(
    readiness_df: pd.DataFrame,
    reconciliation_df: pd.DataFrame,
    project_root: Path,
) -> tuple[Path, Path]:
    out_dir = project_root / "outputs" / "analytics"
    out_dir.mkdir(parents=True, exist_ok=True)

    r_path = out_dir / "report_analytics_readiness.csv"
    rec_path = out_dir / "report_analytics_report_reconciliation.csv"

    readiness_df.to_csv(r_path, index=False)
    reconciliation_df.to_csv(rec_path, index=False)

    return r_path, rec_path


# ---- helpers ----

def _check_freshness(df: pd.DataFrame, col: str, max_days: int) -> tuple[str, Optional[str]]:
    if col not in df.columns or len(df) == 0:
        return "unknown", None
    try:
        ts = pd.to_datetime(df[col]).max()
        age = (datetime.utcnow() - ts.to_pydatetime().replace(tzinfo=None)).days
        status = "fresh" if age <= max_days else "stale"
        return status, str(ts.date())
    except Exception:
        return "unknown", None


def _missing_result(name: str, file_path: Path) -> dict:
    return {
        "prerequisite_name": name, "file_path": str(file_path),
        "file_exists": False, "schema_valid": False, "grain_valid": False,
        "lineage_valid": False, "freshness_status": "missing",
        "row_count": 0, "report_count": 0,
        "analytics_as_of_date": None, "generated_at": None,
        "readiness_status": "missing", "readiness_reasons": "file_not_found",
    }


def _failed_result(name: str, file_path: Path, reason: str) -> dict:
    return {
        "prerequisite_name": name, "file_path": str(file_path),
        "file_exists": True, "schema_valid": False, "grain_valid": False,
        "lineage_valid": False, "freshness_status": "unknown",
        "row_count": 0, "report_count": 0,
        "analytics_as_of_date": None, "generated_at": None,
        "readiness_status": "generation_failed", "readiness_reasons": reason,
    }


def _build_result(
    name, file_path, df, schema_valid, grain_valid, lineage_valid,
    freshness_status, as_of_date, reasons
) -> dict:
    issues = [r for r in reasons if r]
    if not schema_valid:
        status = "schema_mismatch"
    elif not grain_valid:
        status = "invalid_grain"
    elif not lineage_valid:
        status = "incomplete_lineage"
    elif freshness_status == "stale":
        status = "stale"
    elif issues:
        status = "schema_mismatch"
    else:
        status = "ready"

    gen_at = None
    if "generated_at" in df.columns and len(df) > 0:
        gen_at = str(pd.to_datetime(df["generated_at"]).max())

    return {
        "prerequisite_name": name,
        "file_path": str(file_path),
        "file_exists": True,
        "schema_valid": schema_valid,
        "grain_valid": grain_valid,
        "lineage_valid": lineage_valid,
        "freshness_status": freshness_status,
        "row_count": len(df),
        "report_count": df["report_id"].nunique() if "report_id" in df.columns else 0,
        "analytics_as_of_date": as_of_date,
        "generated_at": gen_at,
        "readiness_status": status,
        "readiness_reasons": "|".join(issues) if issues else None,
    }
