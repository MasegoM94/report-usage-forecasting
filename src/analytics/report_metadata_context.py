"""
Report metadata context layer for Sprint 7.

Combines explicit report dimension metadata with lifecycle fields from
report_features.csv. Does not infer business metadata from usage patterns.
"""
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

METADATA_CONTEXT_SCHEMA_VERSION = "1.0.0"

# Fields that contribute to completeness score
COMPLETENESS_REQUIRED_FIELDS = [
    "report_activation_date",
    "report_owner_team",
    "report_category",
    "expected_usage_cadence",
    "criticality_level",
    "report_status",
]

ALLOWED_METADATA_EVIDENCE_STATUSES = frozenset({
    "complete", "mostly_complete", "partial", "minimal", "missing", "invalid_metadata",
})

ALLOWED_INTERPRETATION_STATUSES = frozenset({
    "metadata_supported", "metadata_supported_with_gaps",
    "limited_metadata", "missing_metadata", "invalid_metadata",
})

ALLOWED_CADENCE_VALUES = frozenset({
    "daily", "weekly", "monthly", "quarterly",
    "event_driven", "ad_hoc", "unknown",
})

ALLOWED_CRITICALITY_VALUES = frozenset({
    "critical", "high", "medium", "low", "unknown",
})

ALLOWED_CERTIFICATION_VALUES = frozenset({
    "certified", "promoted", "endorsed", "uncertified", "unknown",
})

ALLOWED_LIFECYCLE_STATUSES = frozenset({
    "pre_activation", "newly_launched", "maturing", "established",
    "dormant", "unknown",
})

ALLOWED_MATURITY_STATUSES = frozenset({
    "newly_launched", "maturing", "mature", "unknown",
})

PROHIBITED_METADATA_COLS = frozenset({
    "user_id", "email", "email_address", "user_name", "username",
    "display_name", "unique_user", "principal_name",
})

METADATA_CONTEXT_COLS = [
    # Identity
    "analytics_run_id", "generated_at", "analytics_as_of_date",
    "report_id", "report_name", "workspace_id", "workspace_name",
    # Ownership
    "report_owner_team", "business_area", "department",
    "report_steward", "ownership_status",
    # Lifecycle (from report_features)
    "report_activation_date", "report_age_days",
    "first_observed_usage_date", "latest_observed_usage_date",
    "days_since_last_use", "adoption_maturity_status", "report_lifecycle_status",
    # Operational metadata (from dim_report)
    "report_status", "report_category", "expected_usage_cadence",
    "certification_status", "endorsement_status", "criticality_level",
    "service_level_tier", "source_system", "replacement_report_id",
    "successor_report_id", "deprecation_status",
    # Evidence
    "activation_date_status", "owner_metadata_status", "cadence_metadata_status",
    "criticality_metadata_status", "replacement_metadata_status",
    "metadata_completeness_score", "metadata_evidence_status",
    "missing_metadata_fields", "metadata_reasons",
    # Interpretation
    "metadata_interpretation_status",
]

_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")


def _normalize_cadence(val) -> Optional[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "unknown"
    s = str(val).strip().lower().replace(" ", "_")
    return s if s in ALLOWED_CADENCE_VALUES else "unknown"


def _normalize_criticality(val) -> Optional[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "unknown"
    s = str(val).strip().lower()
    return s if s in ALLOWED_CRITICALITY_VALUES else "unknown"


def _normalize_certification(val) -> Optional[str]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "unknown"
    s = str(val).strip().lower()
    return s if s in ALLOWED_CERTIFICATION_VALUES else "unknown"


def _is_blank(val) -> bool:
    if val is None:
        return True
    if isinstance(val, float) and pd.isna(val):
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    return False


def _normalize_dim_report_row(row: pd.Series, actual_columns: set) -> dict:
    """Map available dim_report columns to canonical output fields."""
    def get(*keys):
        for k in keys:
            if k in actual_columns:
                v = row.get(k)
                if not _is_blank(v):
                    return v
        return None

    # Identity
    report_id = get("report_id")
    report_name = get("report_name", "name")
    workspace_id = get("workspace_id")
    workspace_name = get("workspace_name", "workspace")

    # Ownership — detect and redact emails
    raw_owner = get("owner_team", "owner", "report_owner_team")
    owner_email_detected = False
    report_owner_team = None
    if raw_owner is not None:
        if _EMAIL_RE.search(str(raw_owner)):
            owner_email_detected = True
            report_owner_team = None
        else:
            report_owner_team = raw_owner

    department = get("department", "dept")
    business_area = get("business_area")
    report_steward = get("report_steward")

    # Activation / lifecycle — dim_report fallback only; features override later
    launch_date = get("launch_date", "activation_date", "report_activation_date")

    # Operational
    report_status = get("report_status", "status")
    report_category = get("report_category", "category", "report_type")
    cadence_raw = get("expected_usage_cadence", "cadence", "usage_cadence", "expected_cadence")
    expected_usage_cadence = _normalize_cadence(cadence_raw)
    criticality_level = _normalize_criticality(get("criticality_level", "criticality"))
    certification_status = _normalize_certification(get("certification_status", "certified"))
    endorsement_status = _normalize_certification(get("endorsement_status", "endorsed"))
    service_level_tier = get("service_level_tier", "service_level")
    source_system = get("source_system")
    replacement_report_id = get("replacement_report_id")
    successor_report_id = get("successor_report_id")

    # Deprecation status from retire_date or is_active
    deprecation_status = None
    retire_date = get("retire_date", "deprecation_date")
    is_active = get("is_active", "active")
    if retire_date is not None:
        deprecation_status = "deprecated"
    elif is_active is not None:
        active_val = str(is_active).strip().lower()
        if active_val in ("false", "0", "no"):
            deprecation_status = "deprecated"
        else:
            deprecation_status = "active"

    return {
        "report_id": report_id,
        "report_name": report_name,
        "workspace_id": workspace_id,
        "workspace_name": workspace_name,
        "report_owner_team": report_owner_team,
        "business_area": business_area,
        "department": department,
        "report_steward": report_steward,
        "_owner_email_detected": owner_email_detected,
        "_launch_date_from_dim": launch_date,
        "report_status": report_status,
        "report_category": report_category,
        "expected_usage_cadence": expected_usage_cadence,
        "criticality_level": criticality_level,
        "certification_status": certification_status,
        "endorsement_status": endorsement_status,
        "service_level_tier": service_level_tier,
        "source_system": source_system,
        "replacement_report_id": replacement_report_id,
        "successor_report_id": successor_report_id,
        "deprecation_status": deprecation_status,
    }


def _calculate_completeness(fields_dict: dict) -> tuple:
    """Returns (completeness_score, missing_fields_list)."""
    present_count = 0
    missing_fields = []
    for f in COMPLETENESS_REQUIRED_FIELDS:
        val = fields_dict.get(f)
        if not _is_blank(val) and val != "unknown":
            present_count += 1
        else:
            missing_fields.append(f)
    score = present_count / len(COMPLETENESS_REQUIRED_FIELDS)
    return score, sorted(missing_fields)


def _classify_metadata_evidence(completeness_score: float, missing_fields: list) -> tuple:
    """Returns (metadata_evidence_status, metadata_interpretation_status)."""
    if completeness_score == 1.0:
        evidence = "complete"
    elif completeness_score >= 0.80:
        evidence = "mostly_complete"
    elif completeness_score >= 0.50:
        evidence = "partial"
    elif completeness_score >= 0.25:
        evidence = "minimal"
    else:
        evidence = "missing"

    if completeness_score >= 0.80:
        interpretation = "metadata_supported"
    elif completeness_score >= 0.50:
        interpretation = "metadata_supported_with_gaps"
    elif completeness_score >= 0.25:
        interpretation = "limited_metadata"
    else:
        interpretation = "missing_metadata"

    return evidence, interpretation


def _build_ownership_status(owner_team, owner_email_detected: bool) -> str:
    if owner_email_detected:
        return "email_redacted"
    if owner_team is not None:
        return "known"
    return "unknown"


def _build_metadata_reasons(row_dict: dict, missing_fields: list) -> str:
    parts = []

    # 1. Activation date
    act_status = row_dict.get("activation_date_status") or "unavailable"
    act_date = row_dict.get("report_activation_date")
    if act_date:
        parts.append(f"activation_date:{act_date}|status:{act_status}")
    else:
        parts.append(f"activation_date:missing|status:{act_status}")

    # 2. Ownership
    own_status = row_dict.get("ownership_status", "unknown")
    owner = row_dict.get("report_owner_team")
    if owner:
        parts.append(f"owner_team:{owner}|ownership:{own_status}")
    else:
        parts.append(f"owner_team:missing|ownership:{own_status}")

    # 3. Category and cadence
    cat = row_dict.get("report_category") or "missing"
    cad = row_dict.get("expected_usage_cadence") or "unknown"
    parts.append(f"category:{cat}|cadence:{cad}")

    # 4. Criticality and service level
    crit = row_dict.get("criticality_level") or "unknown"
    slt = row_dict.get("service_level_tier") or "missing"
    parts.append(f"criticality:{crit}|service_level_tier:{slt}")

    # 5. Certification and endorsement
    cert = row_dict.get("certification_status") or "unknown"
    end = row_dict.get("endorsement_status") or "unknown"
    parts.append(f"certification:{cert}|endorsement:{end}")

    # 6. Replacement and successor
    rep = row_dict.get("replacement_report_id") or "none"
    suc = row_dict.get("successor_report_id") or "none"
    parts.append(f"replacement:{rep}|successor:{suc}")

    # 7. Completeness summary
    score = row_dict.get("metadata_completeness_score", 0.0)
    n_missing = len(missing_fields)
    parts.append(f"completeness_score:{score:.2f}|missing_fields:{n_missing}")

    # 8. Interpretation conclusion
    interp = row_dict.get("metadata_interpretation_status", "missing_metadata")
    parts.append(f"interpretation:{interp}")

    return "|".join(parts)


def build_report_metadata_context(
    dim_report_df: pd.DataFrame,
    features_df: Optional[pd.DataFrame],
    analytics_as_of_date: str,
    analytics_run_id: str,
) -> pd.DataFrame:
    """Build the report metadata context layer."""
    generated_at = datetime.utcnow().isoformat()
    actual_columns = set(dim_report_df.columns)

    # Build features lookup
    features_lookup: dict = {}
    if features_df is not None:
        for _, frow in features_df.iterrows():
            rid = frow.get("report_id")
            if rid is not None:
                features_lookup[rid] = frow

    rows = []
    for _, dim_row in dim_report_df.iterrows():
        norm = _normalize_dim_report_row(dim_row, actual_columns)
        report_id = norm["report_id"]

        # Self-reference check
        if norm["replacement_report_id"] is not None and norm["replacement_report_id"] == report_id:
            raise ValueError(
                f"Self-referencing replacement_report_id detected for report_id={report_id}"
            )
        if norm["successor_report_id"] is not None and norm["successor_report_id"] == report_id:
            raise ValueError(
                f"Self-referencing successor_report_id detected for report_id={report_id}"
            )

        # Lifecycle fields — features take precedence
        feat = features_lookup.get(report_id)

        def feat_get(col):
            if feat is None:
                return None
            v = feat.get(col)
            return None if _is_blank(v) else v

        report_activation_date = feat_get("report_activation_date") or norm["_launch_date_from_dim"]
        report_age_days = feat_get("report_age_days")
        first_observed_usage_date = feat_get("first_observed_usage_date")
        latest_observed_usage_date = feat_get("latest_observed_usage_date")
        days_since_last_use = feat_get("days_since_last_use")
        adoption_maturity_status = feat_get("adoption_maturity_status")
        report_lifecycle_status = feat_get("report_lifecycle_status")
        activation_date_status = feat_get("activation_date_status")

        # Ownership status
        owner_email_detected = norm.pop("_owner_email_detected")
        norm.pop("_launch_date_from_dim")
        ownership_status = _build_ownership_status(norm["report_owner_team"], owner_email_detected)
        owner_metadata_status = "email_redacted" if owner_email_detected else (
            "known" if norm["report_owner_team"] is not None else "missing"
        )

        # Cadence/criticality status
        cadence_metadata_status = (
            "known" if norm["expected_usage_cadence"] not in (None, "unknown") else "missing"
        )
        criticality_metadata_status = (
            "known" if norm["criticality_level"] not in (None, "unknown") else "missing"
        )
        replacement_metadata_status = (
            "known" if norm["replacement_report_id"] is not None else "not_applicable"
        )

        # Completeness score — assemble fields dict for scoring
        scoring_dict = {
            "report_activation_date": report_activation_date,
            "report_owner_team": norm["report_owner_team"],
            "report_category": norm["report_category"],
            "expected_usage_cadence": norm["expected_usage_cadence"],
            "criticality_level": norm["criticality_level"],
            "report_status": norm["report_status"],
        }
        completeness_score, missing_fields = _calculate_completeness(scoring_dict)
        evidence_status, interpretation_status = _classify_metadata_evidence(
            completeness_score, missing_fields
        )

        missing_metadata_fields = ",".join(missing_fields) if missing_fields else None

        # Assemble full row_dict for reasons
        row_dict = {
            **norm,
            "report_activation_date": report_activation_date,
            "activation_date_status": activation_date_status or "unavailable",
            "ownership_status": ownership_status,
            "metadata_completeness_score": completeness_score,
            "metadata_interpretation_status": interpretation_status,
        }
        metadata_reasons = _build_metadata_reasons(row_dict, missing_fields)

        rows.append({
            "analytics_run_id": analytics_run_id,
            "generated_at": generated_at,
            "analytics_as_of_date": analytics_as_of_date,
            "report_id": report_id,
            "report_name": norm["report_name"],
            "workspace_id": norm["workspace_id"],
            "workspace_name": norm["workspace_name"],
            "report_owner_team": norm["report_owner_team"],
            "business_area": norm["business_area"],
            "department": norm["department"],
            "report_steward": norm["report_steward"],
            "ownership_status": ownership_status,
            "report_activation_date": report_activation_date,
            "report_age_days": report_age_days,
            "first_observed_usage_date": first_observed_usage_date,
            "latest_observed_usage_date": latest_observed_usage_date,
            "days_since_last_use": days_since_last_use,
            "adoption_maturity_status": adoption_maturity_status,
            "report_lifecycle_status": report_lifecycle_status,
            "report_status": norm["report_status"],
            "report_category": norm["report_category"],
            "expected_usage_cadence": norm["expected_usage_cadence"],
            "certification_status": norm["certification_status"],
            "endorsement_status": norm["endorsement_status"],
            "criticality_level": norm["criticality_level"],
            "service_level_tier": norm["service_level_tier"],
            "source_system": norm["source_system"],
            "replacement_report_id": norm["replacement_report_id"],
            "successor_report_id": norm["successor_report_id"],
            "deprecation_status": norm["deprecation_status"],
            "activation_date_status": activation_date_status or "unavailable",
            "owner_metadata_status": owner_metadata_status,
            "cadence_metadata_status": cadence_metadata_status,
            "criticality_metadata_status": criticality_metadata_status,
            "replacement_metadata_status": replacement_metadata_status,
            "metadata_completeness_score": completeness_score,
            "metadata_evidence_status": evidence_status,
            "missing_metadata_fields": missing_metadata_fields,
            "metadata_reasons": metadata_reasons,
            "metadata_interpretation_status": interpretation_status,
        })

    df = pd.DataFrame(rows)
    # Ensure all cols present
    for col in METADATA_CONTEXT_COLS:
        if col not in df.columns:
            df[col] = None
    return df[METADATA_CONTEXT_COLS].sort_values("report_id").reset_index(drop=True)


def validate_report_metadata_context(df: pd.DataFrame) -> None:
    """Validate the metadata context output DataFrame."""
    # All required cols present
    missing_cols = [c for c in METADATA_CONTEXT_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Unique grain
    if df.duplicated(subset=["analytics_run_id", "report_id"]).any():
        raise ValueError("Duplicate (analytics_run_id, report_id) found")

    # No prohibited columns
    bad_cols = set(df.columns) & PROHIBITED_METADATA_COLS
    if bad_cols:
        raise ValueError(f"Prohibited columns present: {bad_cols}")

    # Status values
    if "metadata_evidence_status" in df.columns:
        bad = set(df["metadata_evidence_status"].dropna()) - ALLOWED_METADATA_EVIDENCE_STATUSES
        if bad:
            raise ValueError(f"Invalid metadata_evidence_status values: {bad}")

    if "metadata_interpretation_status" in df.columns:
        bad = set(df["metadata_interpretation_status"].dropna()) - ALLOWED_INTERPRETATION_STATUSES
        if bad:
            raise ValueError(f"Invalid metadata_interpretation_status values: {bad}")

    if "expected_usage_cadence" in df.columns:
        bad = set(df["expected_usage_cadence"].dropna()) - ALLOWED_CADENCE_VALUES
        if bad:
            raise ValueError(f"Invalid expected_usage_cadence values: {bad}")

    if "criticality_level" in df.columns:
        bad = set(df["criticality_level"].dropna()) - ALLOWED_CRITICALITY_VALUES
        if bad:
            raise ValueError(f"Invalid criticality_level values: {bad}")

    if "certification_status" in df.columns:
        bad = set(df["certification_status"].dropna()) - ALLOWED_CERTIFICATION_VALUES
        if bad:
            raise ValueError(f"Invalid certification_status values: {bad}")

    if "metadata_completeness_score" in df.columns:
        scores = df["metadata_completeness_score"].dropna()
        if (scores < 0.0).any() or (scores > 1.0).any():
            raise ValueError("metadata_completeness_score out of [0.0, 1.0] range")

    # No personal emails in any string column
    email_pat = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
    for col in df.select_dtypes(include="object").columns:
        for val in df[col].dropna():
            if email_pat.search(str(val)):
                raise ValueError(f"Email address detected in column '{col}': {val}")

    # No self-referencing
    if "replacement_report_id" in df.columns and "report_id" in df.columns:
        self_ref = df[df["replacement_report_id"] == df["report_id"]]
        if len(self_ref) > 0:
            raise ValueError(f"Self-referencing replacement_report_id: {list(self_ref['report_id'])}")

    if "successor_report_id" in df.columns and "report_id" in df.columns:
        self_ref = df[df["successor_report_id"] == df["report_id"]]
        if len(self_ref) > 0:
            raise ValueError(f"Self-referencing successor_report_id: {list(self_ref['report_id'])}")


def persist_report_metadata_context(df: pd.DataFrame, project_root: Path) -> Path:
    """Validate and write to outputs/analytics/report_metadata_context.csv."""
    validate_report_metadata_context(df)
    out_dir = project_root / "outputs" / "analytics"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "report_metadata_context.csv"
    df.sort_values(["analytics_run_id", "report_id"]).reset_index(drop=True).to_csv(
        out_path, index=False
    )
    return out_path
