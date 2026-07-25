"""
Report model-health context layer for Sprint 7.

Joins model diagnostics with forecast outlook to produce an interpretation
of how confidently the forecast can be used. Does not recalculate any
diagnostic metrics.
"""
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import pandas as pd

MODEL_HEALTH_CONTEXT_SCHEMA_VERSION = "1.0.0"


@dataclass(frozen=True)
class ModelHealthContextConfig:
    # Lineage tolerance
    ALLOW_MISSING_SELECTION_RUN_ID: bool = True
    ALLOW_MISSING_FORECAST_RUN_ID: bool = True
    # Interpretation thresholds
    POOR_HEALTH_INTERPRETATION_STATUS: str = "model_health_limited"
    WARNING_HEALTH_INTERPRETATION_STATUS: str = "outlook_supported_with_caution"


# Allowed categorical values
ALLOWED_INTERPRETATION_STATUSES = frozenset({
    "outlook_supported",
    "outlook_supported_with_caution",
    "model_health_limited",
    "uncertainty_limited",
    "insufficient_model_evidence",
    "invalid_model_evidence",
})

ALLOWED_EVIDENCE_STATUSES = frozenset({
    "complete", "partial", "insufficient", "calculation_failed", "missing",
})

ALLOWED_FORECAST_REVIEW_ACTIONS = frozenset({
    "continue_monitoring",
    "review_residual_autocorrelation",
    "review_bias",
    "review_variance_instability",
    "review_outliers",
    "review_interval_calibration",
    "review_production_deterioration",
    "investigate_data_quality",
    "insufficient_evidence",
})

PROHIBITED_ACTIONS = frozenset({
    "automatic_retraining", "change_selected_model", "retire_report",
    "delete_report", "restrict_user",
})

MODEL_HEALTH_CONTEXT_COLS = [
    # Identity and lineage
    "diagnostic_run_id", "generated_at", "report_id", "report_name",
    "selected_model_name", "selected_m", "selection_run_id",
    "evaluation_run_id", "training_cutoff", "forecast_run_id",
    # Evidence
    "model_diagnostic_status", "model_evidence_status",
    "backtest_evidence_status", "production_evidence_status",
    "production_evidence_maturity", "missing_model_evidence",
    # Diagnostic results
    "primary_model_issue", "residual_autocorrelation_status", "bias_status",
    "variance_stability_status", "outlier_status", "interval_calibration_status",
    "production_deterioration_status", "model_issue_count", "model_warning_count",
    # Actions
    "recommended_model_action", "model_review_required", "model_health_reasons",
    # Forecast interpretation
    "forecast_outlook_status", "forecast_uncertainty_status",
    "forecast_interpretation_status", "forecast_interpretation_reasons",
    # Lineage validation
    "lineage_validation_status", "lineage_mismatch_fields",
]

# ---------------------------------------------------------------------------
# Internal mappings
# ---------------------------------------------------------------------------

# Map diagnostic_evidence_status → model_evidence_status (simplified)
_EVIDENCE_STATUS_MAP = {
    "complete": "complete",
    "strong_backtest_limited_production": "partial",
    "partial": "partial",
    "insufficient_evidence": "insufficient",
    "incomplete_lineage": "missing",
    "calculation_failed": "calculation_failed",
}

# Map diagnostic recommended_model_action → context review action
_MODEL_ACTION_TO_REVIEW_ACTION = {
    "continue_monitoring": "continue_monitoring",
    "review_model_specification": "review_residual_autocorrelation",
    "review_seasonality": "review_residual_autocorrelation",
    "investigate_bias": "review_bias",
    "investigate_variance_instability": "review_variance_instability",
    "investigate_outliers": "review_outliers",
    "review_interval_calibration": "review_interval_calibration",
    "review_production_deterioration": "review_production_deterioration",
    "consider_retraining": None,  # refined using primary_model_issue
    "insufficient_evidence": "insufficient_evidence",
}

_PRIMARY_ISSUE_TO_REVIEW_ACTION = {
    "production_deterioration": "review_production_deterioration",
    "residual_autocorrelation": "review_residual_autocorrelation",
    "persistent_bias": "review_bias",
    "interval_undercoverage": "review_interval_calibration",
    "interval_overwidth": "review_interval_calibration",
    "variance_instability": "review_variance_instability",
    "repeated_outliers": "review_outliers",
    "distribution_caution": "review_residual_autocorrelation",
    "insufficient_evidence": "insufficient_evidence",
    "none": "continue_monitoring",
}


# ---------------------------------------------------------------------------
# Scalar helpers
# ---------------------------------------------------------------------------

def _get(row: pd.Series, col, default=None):
    """Safely get a value from a Series, returning default on NaN or missing."""
    val = row.get(col, default)
    if val is None:
        return default
    try:
        if pd.isna(val):
            return default
    except (TypeError, ValueError):
        pass
    return val


def _str_get(row: pd.Series, col, default=None):
    v = _get(row, col, default)
    return str(v) if v is not None else default


def _int_get(row: pd.Series, col, default=0) -> int:
    v = _get(row, col)
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _bool_get(row: pd.Series, col, default=False) -> bool:
    v = _get(row, col)
    if v is None:
        return default
    try:
        return bool(v)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Evidence derivation helpers
# ---------------------------------------------------------------------------

def _derive_model_evidence_status(diag_row: pd.Series) -> str:
    """Derive simplified model_evidence_status from diagnostic row."""
    # Prefer direct column if present (e.g. from test fixtures)
    direct = _str_get(diag_row, "model_evidence_status")
    if direct is not None:
        return direct
    diag_ev = _str_get(diag_row, "diagnostic_evidence_status", "insufficient")
    return _EVIDENCE_STATUS_MAP.get(diag_ev, "insufficient")


def _derive_backtest_evidence_status(diag_row: pd.Series) -> str:
    """Derive backtest_evidence_status from diagnostic row."""
    direct = _str_get(diag_row, "backtest_evidence_status")
    if direct is not None:
        return direct
    bt_avail = _bool_get(diag_row, "backtest_diagnostics_available", False)
    diag_ev = _str_get(diag_row, "diagnostic_evidence_status", "insufficient_evidence")
    if not bt_avail:
        return "missing"
    if diag_ev in ("insufficient_evidence", "incomplete_lineage"):
        return "insufficient"
    return "complete"


def _derive_production_evidence_status(diag_row: pd.Series) -> str:
    """Derive production_evidence_status from diagnostic row."""
    direct = _str_get(diag_row, "production_evidence_status")
    if direct is not None:
        return direct
    prod_avail = _bool_get(diag_row, "production_diagnostics_available", False)
    return "partial" if prod_avail else "missing"


def _derive_production_evidence_maturity(diag_row: pd.Series) -> Optional[str]:
    """Derive production_evidence_maturity from diagnostic row."""
    direct = _str_get(diag_row, "production_evidence_maturity")
    if direct is not None:
        return direct
    prod_count = _int_get(diag_row, "production_error_count", 0)
    completed = _int_get(diag_row, "production_completed_run_count", 0)
    if prod_count == 0 and completed == 0:
        return "none"
    if prod_count < 10 or completed < 2:
        return "immature"
    return "established"


def _derive_production_deterioration_status(diag_row: pd.Series) -> Optional[str]:
    """Derive production_deterioration_status from diagnostic row fields."""
    direct = _str_get(diag_row, "production_deterioration_status")
    if direct is not None:
        return direct
    flag = _bool_get(diag_row, "accuracy_deterioration_flag", False)
    severity = _str_get(diag_row, "deterioration_severity", "none")
    if not flag or severity in ("none", None):
        return "no_deterioration"
    return severity  # "confirmed", "unconfirmed_limited_evidence", "unknown"


def _map_review_action(diag_row: pd.Series) -> Optional[str]:
    """Map diagnostic recommended_model_action to context-layer review action."""
    diag_action = _str_get(diag_row, "recommended_model_action")
    if diag_action is None:
        return None
    # Check if already a context-layer action
    if diag_action in ALLOWED_FORECAST_REVIEW_ACTIONS:
        return diag_action
    mapped = _MODEL_ACTION_TO_REVIEW_ACTION.get(diag_action)
    if mapped is not None:
        return mapped
    # consider_retraining: use primary issue
    if diag_action == "consider_retraining":
        primary = _str_get(diag_row, "primary_model_issue", "none")
        return _PRIMARY_ISSUE_TO_REVIEW_ACTION.get(primary, "review_residual_autocorrelation")
    return None


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def _validate_lineage_agreement(
    diag_row: pd.Series,
    outlook_row: Optional[pd.Series],
    cfg: ModelHealthContextConfig,
) -> tuple[str, Optional[str]]:
    """Check that diagnostic and forecast outlook agree on model identity.

    Returns (lineage_validation_status, lineage_mismatch_fields_str).
    """
    if outlook_row is None:
        return ("missing_forecast_outlook", None)

    mismatches = []

    # 1. selected_model_name — exact match
    diag_name = _str_get(diag_row, "selected_model_name")
    out_name = _str_get(outlook_row, "selected_model_name")
    if diag_name is not None and out_name is not None:
        if diag_name != out_name:
            mismatches.append("selected_model_name")

    # 2. selected_m — compare as int, handle NaN
    diag_m = _get(diag_row, "selected_m")
    out_m = _get(outlook_row, "selected_m")
    diag_m_int = None if diag_m is None else (int(diag_m) if not (isinstance(diag_m, float) and pd.isna(diag_m)) else None)
    out_m_int = None if out_m is None else (int(out_m) if not (isinstance(out_m, float) and pd.isna(out_m)) else None)
    if diag_m_int is not None and out_m_int is not None:
        if diag_m_int != out_m_int:
            mismatches.append("selected_m")

    # 3. training_cutoff — compare as stripped string
    diag_cutoff = _str_get(diag_row, "training_cutoff")
    out_cutoff = _str_get(outlook_row, "training_cutoff")
    if diag_cutoff is not None and out_cutoff is not None:
        if diag_cutoff.strip() != out_cutoff.strip():
            mismatches.append("training_cutoff")

    if mismatches:
        return ("mismatch", "|".join(mismatches))
    return ("valid", None)


def _classify_forecast_interpretation(
    diag_row: pd.Series,
    outlook_row: Optional[pd.Series],
    lineage_status: str,
    cfg: ModelHealthContextConfig,
) -> str:
    """Classify forecast interpretation status using precedence rules.

    Precedence (return first match):
    1. invalid_model_evidence  — calculation_failed or lineage mismatch
    2. insufficient_model_evidence — insufficient/missing evidence
    3. model_health_limited — poor diagnostic status
    4. uncertainty_limited — very high uncertainty in outlook
    5. outlook_supported_with_caution — watch/warning diagnostic status
    6. outlook_supported — default
    """
    model_status = _str_get(diag_row, "model_diagnostic_status", "")
    model_ev = _derive_model_evidence_status(diag_row)
    bt_ev = _derive_backtest_evidence_status(diag_row)

    # 1. Invalid evidence
    if model_status == "calculation_failed" or lineage_status == "mismatch":
        return "invalid_model_evidence"

    # 2. Insufficient evidence
    if model_ev in ("insufficient", "missing") or bt_ev in ("insufficient", "missing") or bt_ev is None:
        return "insufficient_model_evidence"
    if model_status == "insufficient_evidence":
        return "insufficient_model_evidence"

    # 3. Poor model health
    if model_status == "poor":
        return "model_health_limited"

    # 4. Uncertainty limited
    if outlook_row is not None:
        fc_uncertainty = _str_get(outlook_row, "forecast_uncertainty_status", "")
        if fc_uncertainty in ("very_high_uncertainty",):
            return "uncertainty_limited"

    # 5. Watch/caution
    if model_status in ("watch", "warning"):
        return "outlook_supported_with_caution"

    # 6. Default
    return "outlook_supported"


def _build_interpretation_reasons(
    diag_row: pd.Series,
    outlook_row: Optional[pd.Series],
    interp_status: str,
    lineage_status: str,
) -> str:
    """Build pipe-separated deterministic reasons for the interpretation.

    Slots (skip if no meaningful content):
    1. Evidence status
    2. Model lineage
    3. Backtest diagnostics
    4. Production maturity
    5. Forecast uncertainty (from outlook_row if available)
    6. Interpretation conclusion
    """
    parts = []

    # 1. Evidence status
    model_ev = _derive_model_evidence_status(diag_row)
    bt_ev = _derive_backtest_evidence_status(diag_row)
    if model_ev in ("insufficient", "missing", "calculation_failed"):
        parts.append(f"Model evidence is {model_ev}; diagnostic conclusions may not be reliable.")
    elif bt_ev in ("insufficient", "missing"):
        parts.append(f"Backtest evidence is {bt_ev}; model health cannot be fully assessed.")
    elif model_ev == "partial":
        parts.append("Model evidence is partial; some diagnostic categories are unavailable.")

    # 2. Model lineage
    if lineage_status == "mismatch":
        parts.append("Model identity mismatch between diagnostics and forecast outlook; lineage cannot be confirmed.")
    elif lineage_status == "missing_forecast_outlook":
        parts.append("No forecast outlook is available; interpretation is based on model diagnostics alone.")
    elif lineage_status == "valid":
        parts.append("Model identity is consistent between diagnostics and forecast outlook.")

    # 3. Backtest diagnostics
    model_status = _str_get(diag_row, "model_diagnostic_status", "")
    primary_issue = _str_get(diag_row, "primary_model_issue", "none")
    if model_status == "poor":
        parts.append(f"Model diagnostics indicate poor health with primary issue: {primary_issue}.")
    elif model_status in ("watch", "warning"):
        parts.append(f"Model diagnostics indicate caution with primary issue: {primary_issue}.")
    elif model_status == "healthy":
        parts.append("Model diagnostics indicate acceptable health across evaluated diagnostic components.")
    elif model_status == "insufficient_evidence":
        parts.append("Insufficient backtest evidence exists to assess model health.")

    # 4. Production maturity
    prod_maturity = _derive_production_evidence_maturity(diag_row)
    if prod_maturity and prod_maturity not in ("none", "established"):
        parts.append(f"Production evidence maturity is {prod_maturity}; production-based conclusions are preliminary.")

    # 5. Forecast uncertainty
    if outlook_row is not None:
        fc_uncertainty = _str_get(outlook_row, "forecast_uncertainty_status")
        if fc_uncertainty:
            parts.append(f"Forecast uncertainty status is {fc_uncertainty}.")

    # 6. Interpretation conclusion
    _conclusion_map = {
        "outlook_supported": "Forecast outlook can be used with standard confidence given available evidence.",
        "outlook_supported_with_caution": "Forecast outlook should be interpreted with caution given model diagnostic warnings.",
        "model_health_limited": "Forecast outlook is limited by poor model health; interpret results carefully.",
        "uncertainty_limited": "Forecast uncertainty is very high; confidence intervals should be reviewed before use.",
        "insufficient_model_evidence": "Insufficient model evidence exists to validate forecast reliability.",
        "invalid_model_evidence": "Model evidence is invalid or inconsistent; forecast interpretation is unreliable.",
    }
    conclusion = _conclusion_map.get(interp_status)
    if conclusion:
        parts.append(conclusion)

    return "|".join(parts) if parts else "No interpretation reasons available."


def _count_model_issues(diag_row: pd.Series) -> tuple[int, int]:
    """Return (issue_count, warning_count) from diagnostic row.

    Uses pre-computed counts from the diagnostic layer if available.
    Falls back to counting from status fields.
    """
    # Prefer pre-computed counts
    issue_count = _int_get(diag_row, "diagnostic_issue_count", -1)
    warning_count = _int_get(diag_row, "warning_issue_count", -1)

    if issue_count >= 0 and warning_count >= 0:
        return issue_count, warning_count

    # Fallback: count from individual status fields
    _ok_statuses = frozenset({
        "normal", "ok", "healthy", "well_calibrated", "good",
        "no_deterioration", "stable", "low", "passed", "none",
        "no_significant_bias",
    })
    _warning_suffixes = ("warning",)
    _warning_values = frozenset({"elevated", "slight_undercoverage", "wide_but_usable"})

    status_cols = [
        "residual_autocorrelation_status", "bias_status", "variance_stability_status",
        "outlier_status", "interval_calibration_status", "production_deterioration_status",
        # Also check backtest-prefixed variants
        "backtest_autocorrelation_status", "backtest_bias_status",
        "backtest_outlier_status", "backtest_calibration_status",
    ]

    issue_count = 0
    warning_count = 0
    for col in status_cols:
        val = _str_get(diag_row, col)
        if val is None or val in _ok_statuses:
            continue
        if val in _ok_statuses:
            continue
        is_warning = (
            any(val.endswith(s) for s in _warning_suffixes) or val in _warning_values
        )
        if is_warning:
            warning_count += 1
        else:
            issue_count += 1

    return issue_count, warning_count


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_report_model_health_context(
    diag_df: pd.DataFrame,
    outlook_df: Optional[pd.DataFrame],
    cfg: ModelHealthContextConfig,
    run_id: str,
) -> pd.DataFrame:
    """Build model-health context rows by joining diagnostics with forecast outlook.

    Parameters
    ----------
    diag_df:
        Report model diagnostics (one row per report_id).
    outlook_df:
        Forecast outlook (optional; one row per report_id).
    cfg:
        Configuration.
    run_id:
        Pipeline run ID for this pass.

    Returns
    -------
    DataFrame with MODEL_HEALTH_CONTEXT_COLS, sorted by report_id.
    """
    if diag_df is None or diag_df.empty:
        raise ValueError("build_report_model_health_context: diag_df is empty or None.")

    # Build outlook lookup
    outlook_lookup: dict[str, pd.Series] = {}
    if outlook_df is not None and not outlook_df.empty:
        if "report_id" in outlook_df.columns:
            if outlook_df["report_id"].duplicated().any():
                raise ValueError(
                    "build_report_model_health_context: outlook_df has duplicate report_id values."
                )
            for _, row in outlook_df.iterrows():
                outlook_lookup[str(row["report_id"])] = row

    generated_at = datetime.now(timezone.utc).isoformat()
    rows = []

    for _, diag_row in diag_df.iterrows():
        report_id = _str_get(diag_row, "report_id", "")
        outlook_row = outlook_lookup.get(report_id)

        # Lineage validation
        lineage_status, lineage_mismatch = _validate_lineage_agreement(diag_row, outlook_row, cfg)

        # Interpretation
        interp_status = _classify_forecast_interpretation(diag_row, outlook_row, lineage_status, cfg)
        interp_reasons = _build_interpretation_reasons(diag_row, outlook_row, interp_status, lineage_status)

        # Issue counts
        issue_count, warning_count = _count_model_issues(diag_row)

        # Evidence fields
        model_ev = _derive_model_evidence_status(diag_row)
        bt_ev = _derive_backtest_evidence_status(diag_row)
        prod_ev = _derive_production_evidence_status(diag_row)
        prod_maturity = _derive_production_evidence_maturity(diag_row)
        det_status = _derive_production_deterioration_status(diag_row)

        # Action mapping
        review_action = _map_review_action(diag_row)

        # Forecast fields from outlook
        fc_run_id = None
        fc_outlook_status = None
        fc_uncertainty_status = None
        if outlook_row is not None:
            fc_run_id = _str_get(outlook_row, "forecast_run_id")
            fc_outlook_status = _str_get(outlook_row, "forecast_outlook_status")
            fc_uncertainty_status = _str_get(outlook_row, "forecast_uncertainty_status")

        # Diagnostic run id
        diag_run_id = _str_get(diag_row, "diagnostic_run_id") or str(uuid.uuid4())

        row = {
            # Identity and lineage
            "diagnostic_run_id": diag_run_id,
            "generated_at": generated_at,
            "report_id": report_id,
            "report_name": _str_get(diag_row, "report_name"),
            "selected_model_name": _str_get(diag_row, "selected_model_name"),
            "selected_m": _get(diag_row, "selected_m"),
            "selection_run_id": _str_get(diag_row, "selection_run_id"),
            "evaluation_run_id": _str_get(diag_row, "evaluation_run_id"),
            "training_cutoff": _str_get(diag_row, "training_cutoff"),
            "forecast_run_id": fc_run_id,
            # Evidence
            "model_diagnostic_status": _str_get(diag_row, "model_diagnostic_status"),
            "model_evidence_status": model_ev,
            "backtest_evidence_status": bt_ev,
            "production_evidence_status": prod_ev,
            "production_evidence_maturity": prod_maturity,
            "missing_model_evidence": _str_get(diag_row, "missing_evidence_categories"),
            # Diagnostic results
            "primary_model_issue": _str_get(diag_row, "primary_model_issue"),
            "residual_autocorrelation_status": (
                _str_get(diag_row, "residual_autocorrelation_status")
                or _str_get(diag_row, "backtest_autocorrelation_status")
                or _str_get(diag_row, "production_autocorrelation_status")
            ),
            "bias_status": (
                _str_get(diag_row, "bias_status")
                or _str_get(diag_row, "backtest_bias_status")
                or _str_get(diag_row, "production_bias_status")
            ),
            "variance_stability_status": _str_get(diag_row, "variance_stability_status"),
            "outlier_status": (
                _str_get(diag_row, "outlier_status")
                or _str_get(diag_row, "backtest_outlier_status")
                or _str_get(diag_row, "production_outlier_status")
            ),
            "interval_calibration_status": (
                _str_get(diag_row, "interval_calibration_status")
                or _str_get(diag_row, "backtest_calibration_status")
                or _str_get(diag_row, "production_calibration_status")
            ),
            "production_deterioration_status": det_status,
            "model_issue_count": issue_count,
            "model_warning_count": warning_count,
            # Actions
            "recommended_model_action": review_action,
            "model_review_required": (
                _bool_get(diag_row, "model_review_required", None)
                if _get(diag_row, "model_review_required") is not None
                else _bool_get(diag_row, "review_required", False)
            ),
            "model_health_reasons": _str_get(diag_row, "model_diagnostic_reasons"),
            # Forecast interpretation
            "forecast_outlook_status": fc_outlook_status,
            "forecast_uncertainty_status": fc_uncertainty_status,
            "forecast_interpretation_status": interp_status,
            "forecast_interpretation_reasons": interp_reasons,
            # Lineage validation
            "lineage_validation_status": lineage_status,
            "lineage_mismatch_fields": lineage_mismatch,
        }

        rows.append(row)

    out = pd.DataFrame(rows)
    for col in MODEL_HEALTH_CONTEXT_COLS:
        if col not in out.columns:
            out[col] = None

    out = out.sort_values("report_id", ignore_index=True)
    return out[MODEL_HEALTH_CONTEXT_COLS]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_report_model_health_context(df: pd.DataFrame) -> None:
    """Validate schema, grain, and safety constraints.  Raises ValueError."""
    # All required columns present
    missing_cols = [c for c in MODEL_HEALTH_CONTEXT_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(
            f"report_model_health_context: missing columns {missing_cols}"
        )

    if df.empty:
        return

    # Unique grain: (diagnostic_run_id, report_id)
    dupes = df.duplicated(subset=["diagnostic_run_id", "report_id"], keep=False)
    if dupes.any():
        n = int(dupes.sum())
        raise ValueError(
            f"report_model_health_context: {n} duplicate row(s) on "
            "(diagnostic_run_id, report_id)."
        )

    # forecast_interpretation_status in allowed set
    bad_interp = df["forecast_interpretation_status"].dropna()
    inv = bad_interp[~bad_interp.isin(ALLOWED_INTERPRETATION_STATUSES)]
    if not inv.empty:
        raise ValueError(
            f"report_model_health_context: invalid forecast_interpretation_status "
            f"values {inv.unique().tolist()}"
        )

    # recommended_model_action in allowed set (or None)
    bad_action = df["recommended_model_action"].dropna()
    inv_act = bad_action[~bad_action.isin(ALLOWED_FORECAST_REVIEW_ACTIONS)]
    if not inv_act.empty:
        raise ValueError(
            f"report_model_health_context: invalid recommended_model_action "
            f"values {inv_act.unique().tolist()}"
        )

    # No prohibited actions
    all_actions = set(df["recommended_model_action"].dropna())
    prohibited_found = all_actions & PROHIBITED_ACTIONS
    if prohibited_found:
        raise ValueError(
            f"report_model_health_context: prohibited action(s) found: {prohibited_found}"
        )

    # No direct user identifiers
    _USER_ID_COLS = frozenset({"user_id", "email", "email_address", "display_name",
                                "unique_user", "principal_name", "user_key"})
    present_user_cols = _USER_ID_COLS & set(df.columns)
    if present_user_cols:
        raise ValueError(
            f"report_model_health_context: user identifier columns present: {present_user_cols}"
        )

    # model_issue_count >= 0
    if "model_issue_count" in df.columns:
        vals = pd.to_numeric(df["model_issue_count"], errors="coerce").dropna()
        if (vals < 0).any():
            raise ValueError("report_model_health_context: model_issue_count has negative values.")

    # model_review_required is bool or null
    if "model_review_required" in df.columns:
        non_null = df["model_review_required"].dropna()
        invalid_review = non_null[~non_null.isin([True, False, 0, 1])]
        if not invalid_review.empty:
            raise ValueError(
                "report_model_health_context: model_review_required has non-boolean values."
            )

    # No "retire" in values (heuristic)
    for col in df.select_dtypes(include="object").columns:
        if df[col].astype(str).str.contains("retire", case=False, na=False).any():
            raise ValueError(
                f"report_model_health_context: 'retire' found in column '{col}'; "
                "this column must not contain retirement recommendations."
            )

    # automatic_retraining_triggered must not be True if column present
    if "automatic_retraining_triggered" in df.columns:
        if df["automatic_retraining_triggered"].any():
            raise ValueError(
                "report_model_health_context: automatic_retraining_triggered must be False."
            )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist_report_model_health_context(
    df: pd.DataFrame,
    project_root: Path,
) -> Path:
    """Validate and write model health context to disk.

    Writes ``<project_root>/outputs/analytics/report_model_health_context.csv``.
    Returns path.
    """
    validate_report_model_health_context(df)

    out_dir = project_root / "outputs" / "analytics"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "report_model_health_context.csv"

    df_out = df.sort_values(["diagnostic_run_id", "report_id"], ignore_index=True)
    df_out.to_csv(path, index=False)
    return path
