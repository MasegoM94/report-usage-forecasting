"""Portfolio-level GenAI insight generation.

Design principle: deterministic code calculates the portfolio;
GenAI explains the portfolio.

Canonical input: outputs/analytics/mart_report_analytics.csv
Output: outputs/insights/portfolio_ai_insight.json (canonical)
        outputs/insights/portfolio_ai_insight.md  (optional human-readable)

All portfolio aggregates are computed deterministically before any LLM call.
The LLM receives only validated, privacy-safe aggregates and returns a
structured management summary that is validated before being written to disk.

If no API key is available, a deterministic rule-based fallback is used.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

from src.genai.prompts import (
    PORTFOLIO_INSIGHT_PROMPT_VERSION,
    PORTFOLIO_INSIGHT_SYSTEM_PROMPT,
    build_portfolio_insight_prompt,
)

# ── Constants ──────────────────────────────────────────────────────────────────
PORTFOLIO_OUTPUT_JSON = "portfolio_ai_insight.json"
PORTFOLIO_OUTPUT_MD   = "portfolio_ai_insight.md"
SHORTLIST_MAX = 5
PORTFOLIO_INSIGHT_SHORTLIST_MAX = SHORTLIST_MAX   # public alias used in tests
MAX_API_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3

# Required fields in the structured LLM output
REQUIRED_PORTFOLIO_FIELDS = frozenset({
    "executive_summary",
    "portfolio_usage_summary",
    "portfolio_engagement_summary",
    "portfolio_forecast_summary",
    "portfolio_model_health_summary",
    "priority_actions",
    "positive_signals",
    "evidence_limitations",
})

# Action categories the LLM may reference (derived from mart recommended_report_action values)
PORTFOLIO_ALLOWED_ACTION_KEYWORDS = frozenset({
    "continue monitoring",
    "continue_monitoring",
    "investigate usage decline",
    "investigate_usage_decline",
    "review planned deprecation",
    "review_planned_deprecation",
    "review forecast uncertainty",
    "review_forecast_uncertainty",
    "investigate user decline",
    "investigate_user_decline",
    "investigate user lapse",
    "investigate_user_lapse",
    "validate report audience",
    "validate_report_audience",
    "review model health",
    "review model uncertainty",
    "complete report metadata",
    "review inactivity",
    "insufficient evidence",
})

# Reuse report-level prohibited-phrase patterns (no imports from insight_generator to
# avoid circular imports; patterns are duplicated here intentionally).
_PROHIBITED_PATTERNS: list[re.Pattern] = [
    re.compile(r"\bretire\b",                           re.IGNORECASE),
    re.compile(r"\bretirement\b",                       re.IGNORECASE),
    re.compile(r"\bdelete\b",                           re.IGNORECASE),
    re.compile(r"\bdeleting\b",                         re.IGNORECASE),
    re.compile(r"\bdeletion\b",                         re.IGNORECASE),
    re.compile(r"\bretrain\b",                          re.IGNORECASE),
    re.compile(r"\bretraining\b",                       re.IGNORECASE),
    re.compile(r"\breplace(?:s|d)?\s+the\s+model\b",   re.IGNORECASE),
    re.compile(r"\breplacing\s+the\s+model\b",          re.IGNORECASE),
    re.compile(r"\breplace\s+model\b",                  re.IGNORECASE),
    re.compile(r"\bmodel\s+replacement\b",              re.IGNORECASE),
    re.compile(r"\brestrict\s+user\b",                  re.IGNORECASE),
    re.compile(r"\bcontact\s+(?:specific\s+)?user\b",   re.IGNORECASE),
]

_IDENTIFIER_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("user_id",    re.compile(r"\buser[_\s]id\b",  re.IGNORECASE)),
    ("user_key",   re.compile(r"\buser[_\s]key\b", re.IGNORECASE)),
    ("email_addr", re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}",
                              re.IGNORECASE)),
]

# Grounding tolerance for portfolio percentages (pp)
_GROUNDING_TOLERANCE = 5.0
_COUNT_GROUNDING_MIN = 5   # lower than report-level because portfolio counts can be small


# ── Helpers ────────────────────────────────────────────────────────────────────

def _pct(count: int, total: int, decimals: int = 1) -> float:
    return round(count / total * 100, decimals) if total > 0 else 0.0


def _vc_sorted(series: pd.Series) -> dict[str, int]:
    """Value counts as a deterministically sorted dict (desc count, then alpha key)."""
    vc = series.value_counts(dropna=True)
    return {str(k): int(v) for k, v in sorted(vc.items(), key=lambda x: (-x[1], x[0]))}


def _count_in(series: pd.Series, values: set) -> int:
    return int(series.isin(values).sum())


# ── Portfolio context builder ──────────────────────────────────────────────────

def build_portfolio_context(mart_df: pd.DataFrame) -> dict[str, Any]:
    """
    Build a privacy-safe, deterministic portfolio context dict from mart.

    All aggregates are computed here before any LLM call.
    No user-level data, no per-report narratives, no suppressed reconstruction.
    """
    if mart_df.empty:
        raise ValueError("mart_df is empty — cannot build portfolio context.")

    n = len(mart_df)

    def _col(name: str) -> pd.Series:
        return mart_df[name] if name in mart_df.columns else pd.Series(dtype=object)

    # ── Metadata ────────────────────────────────────────────────────────────────
    run_id = mart_df["analytics_run_id"].iloc[0] if "analytics_run_id" in mart_df.columns else None
    as_of  = mart_df["analytics_as_of_date"].iloc[0] if "analytics_as_of_date" in mart_df.columns else None

    # ── Portfolio size and evidence ─────────────────────────────────────────────
    ev_col = _col("overall_evidence_status")
    sufficient_ev   = int((ev_col == "complete").sum())
    insufficient_ev = n - sufficient_ev
    privacy_supp    = int((_col("privacy_suppression_status") == "suppressed").sum())
    missing_meta    = int(_col("metadata_evidence_status").isin({"minimal", "missing"}).sum())

    # ── Historical usage ────────────────────────────────────────────────────────
    hist = _col("historical_usage_status")
    growing_hist   = _count_in(hist, {"growing_usage", "stable_growing"})
    stable_hist    = _count_in(hist, {"stable_regular_usage", "bursty_usage"})
    declining_hist = _count_in(hist, {"declining_usage", "severe_historical_decline",
                                      "moderate_decline", "low_usage_declining"})
    inactive_hist  = _count_in(hist, {"prolonged_inactivity", "reactivating"})
    other_hist     = n - growing_hist - stable_hist - declining_hist - inactive_hist

    zero_col = _col("current_zero_usage_streak_days")
    long_zero_streak = int((zero_col.fillna(0) >= 14).sum()) if not zero_col.empty else 0

    # ── Forecast outlook ────────────────────────────────────────────────────────
    outlook = _col("forecast_outlook_status")
    growth_exp       = int((outlook == "growth_expected").sum())
    stable_exp       = int((outlook == "stable_outlook").sum())
    decline_exp      = int((outlook == "decline_expected").sum())
    reactiv_exp      = int((outlook == "reactivation_expected").sum())
    uncertain_out    = int((outlook == "uncertain_outlook").sum())

    unc_col = _col("forecast_uncertainty_status")
    high_unc         = _count_in(unc_col, {"high_uncertainty", "very_high_uncertainty"})
    intervals_unavail = int((unc_col == "intervals_unavailable").sum())

    # ── Model health ────────────────────────────────────────────────────────────
    model_counts  = _vc_sorted(_col("model_diagnostic_status")) if "model_diagnostic_status" in mart_df.columns else {}
    model_actions = _vc_sorted(_col("recommended_model_action")) if "recommended_model_action" in mart_df.columns else {}
    model_issues  = _vc_sorted(_col("primary_model_issue")) if "primary_model_issue" in mart_df.columns else {}

    calib_col = _col("interval_calibration_status")
    poor_calib = _count_in(calib_col, {"poor_calibration", "miscalibrated"}) if not calib_col.empty else 0

    # ── Engagement ──────────────────────────────────────────────────────────────
    eng_counts   = _vc_sorted(_col("overall_engagement_status")) if "overall_engagement_status" in mart_df.columns else {}
    active_dir   = _col("active_user_direction_28d")
    decl_breadth = int((active_dir == "declining").sum())

    lapse_col       = _col("adoption_transition_status")
    elevated_lapse  = int((lapse_col == "elevated_lapse").sum())
    strong_retain   = int((lapse_col == "strong_retention").sum())

    dep_col       = _col("dependency_status")
    high_conc_dep = _count_in(dep_col, {
        "moderately_concentrated_stable_dependency",
        "moderately_concentrated_dependency_increasing",
    })

    # ── Decision support ────────────────────────────────────────────────────────
    status_counts   = _vc_sorted(_col("overall_report_status")) if "overall_report_status" in mart_df.columns else {}
    priority_counts = _vc_sorted(_col("overall_review_priority")) if "overall_review_priority" in mart_df.columns else {}
    action_counts   = _vc_sorted(_col("recommended_report_action")) if "recommended_report_action" in mart_df.columns else {}

    # ── Top risks / positive signals (deterministic strings, not LLM output) ────
    top_risks: list[str] = []
    if declining_hist > 0:
        top_risks.append(f"{declining_hist} reports show declining historical usage")
    if decline_exp > 0:
        top_risks.append(f"{decline_exp} reports have declining forecast outlook")
    if elevated_lapse > 0:
        top_risks.append(f"{elevated_lapse} reports show elevated user lapse")
    if high_unc > 0:
        top_risks.append(f"{high_unc} reports have high or very high forecast uncertainty")
    if inactive_hist > 0:
        top_risks.append(f"{inactive_hist} reports show prolonged inactivity")
    if decl_breadth > 0:
        top_risks.append(f"{decl_breadth} reports show declining active-user breadth")

    top_positive: list[str] = []
    if growing_hist > 0:
        top_positive.append(f"{growing_hist} reports show growing historical usage")
    if growth_exp > 0:
        top_positive.append(f"{growth_exp} reports are expected to grow")
    if strong_retain > 0:
        top_positive.append(f"{strong_retain} reports show strong user retention")

    return {
        "analytics_run_id": str(run_id) if run_id is not None else None,
        "analytics_as_of_date": str(as_of) if as_of is not None else None,
        "total_report_count": n,
        "portfolio_evidence": {
            "reports_with_sufficient_evidence": sufficient_ev,
            "reports_with_insufficient_evidence": insufficient_ev,
            "reports_with_privacy_suppression": privacy_supp,
            "reports_with_missing_metadata": missing_meta,
        },
        "historical_usage": {
            "growing": growing_hist,
            "growing_share_pct": _pct(growing_hist, n),
            "stable": stable_hist,
            "stable_share_pct": _pct(stable_hist, n),
            "declining": declining_hist,
            "declining_share_pct": _pct(declining_hist, n),
            "inactive": inactive_hist,
            "inactive_share_pct": _pct(inactive_hist, n),
            "other": other_hist,
            "status_counts": _vc_sorted(hist) if not hist.empty else {},
            "long_zero_usage_streak_count": long_zero_streak,
        },
        "forecast_outlook": {
            "growth_expected": growth_exp,
            "growth_expected_share_pct": _pct(growth_exp, n),
            "stable_expected": stable_exp,
            "decline_expected": decline_exp,
            "decline_expected_share_pct": _pct(decline_exp, n),
            "reactivation_expected": reactiv_exp,
            "uncertain_outlook": uncertain_out,
            "high_or_very_high_uncertainty": high_unc,
            "high_uncertainty_share_pct": _pct(high_unc, n),
            "intervals_unavailable": intervals_unavail,
            "status_counts": _vc_sorted(outlook) if not outlook.empty else {},
        },
        "model_health": {
            "status_counts": model_counts,
            "recommended_action_counts": model_actions,
            "primary_issue_counts": model_issues,
            "poor_calibration_count": poor_calib,
        },
        "engagement": {
            "status_counts": eng_counts,
            "declining_active_user_breadth": decl_breadth,
            "elevated_lapse": elevated_lapse,
            "strong_retention": strong_retain,
            "high_user_concentration_or_dependency": high_conc_dep,
        },
        "decision_support": {
            "overall_status_counts": status_counts,
            "review_priority_counts": priority_counts,
            "recommended_action_counts": action_counts,
        },
        "top_risks": sorted(top_risks),
        "top_positive_signals": sorted(top_positive),
        "attention_shortlist": _build_attention_shortlist(mart_df),
    }


def _build_attention_shortlist(mart_df: pd.DataFrame, max_reports: int = SHORTLIST_MAX) -> list[dict]:
    """
    Select up to max_reports reports requiring management attention.
    Selection is purely deterministic: priority rank → status rank → report_id.
    Only reports with a non-monitoring action are included.
    """
    _PRIORITY_RANK = {"high": 0, "medium": 1, "low": 2, "insufficient_evidence": 3}
    _STATUS_RANK   = {
        "declining": 0, "planned_deprecation": 1, "inactive": 2,
        "insufficient_evidence": 3, "growing": 4, "healthy": 5,
    }
    _ATTENTION_ACTIONS = {
        "investigate_usage_decline", "review_planned_deprecation",
        "review_forecast_uncertainty", "investigate_user_decline",
        "investigate_user_lapse", "validate_report_audience",
        "complete_report_metadata", "review_inactivity",
    }

    if "recommended_report_action" not in mart_df.columns:
        return []

    df = mart_df[mart_df["recommended_report_action"].isin(_ATTENTION_ACTIONS)].copy()
    if df.empty:
        return []

    df["_pri"] = df["overall_review_priority"].map(_PRIORITY_RANK).fillna(99)
    df["_sta"] = df["overall_report_status"].map(_STATUS_RANK).fillna(99)
    df = df.sort_values(["_pri", "_sta", "report_id"]).head(max_reports)

    result = []
    for _, row in df.iterrows():
        result.append({
            "report_id":                str(row.get("report_id", "")),
            "report_name":              str(row.get("report_name", "")),
            "overall_review_priority":  row.get("overall_review_priority"),
            "overall_report_status":    row.get("overall_report_status"),
            "primary_diagnostic":       row.get("primary_diagnostic"),
            "recommended_report_action": row.get("recommended_report_action"),
            "overall_evidence_status":  row.get("overall_evidence_status"),
        })
    return result


# ── Hash / caching ─────────────────────────────────────────────────────────────

def _compute_portfolio_hash(context: dict, prompt_version: str, model_name: str) -> str:
    """Deterministic hash of portfolio context + prompt version + model name."""
    # Exclude attention_shortlist from hash — it's a convenience view of the aggregates,
    # not an independent input; the underlying fields that feed the shortlist are already
    # captured in the aggregate counts.
    hashable = {k: v for k, v in context.items() if k != "attention_shortlist"}
    payload = {
        "context": json.dumps(hashable, sort_keys=True, default=str),
        "prompt_version": prompt_version,
        "model_name": model_name,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _load_existing_portfolio(project_root: Path) -> dict | None:
    """Load a previous valid portfolio insight for skip-unchanged comparison."""
    path = project_root / "outputs" / "insights" / PORTFOLIO_OUTPUT_JSON
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data.get("input_hash") and data.get("generation_status") in {
            "success", "reused"
        }:
            return data
    except Exception:
        pass
    return None


# ── JSON parsing ───────────────────────────────────────────────────────────────

def _parse_portfolio_json(text: str) -> dict:
    """Extract structured JSON dict from LLM text response."""
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, re.IGNORECASE)
    candidate = fence.group(1).strip() if fence else text.strip()
    brace = re.search(r"\{[\s\S]*\}", candidate)
    if brace:
        candidate = brace.group(0)
    parsed = json.loads(candidate)
    if not isinstance(parsed, dict):
        raise ValueError(f"Expected dict, got {type(parsed).__name__}")
    return parsed


# ── Validation ─────────────────────────────────────────────────────────────────

def _extract_context_numbers(context: dict) -> set[float]:
    """Recursively extract all numeric values from portfolio context."""
    nums: set[float] = set()

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, (list, tuple)):
            for item in obj:
                _walk(item)
        elif isinstance(obj, (int, float)) and not isinstance(obj, bool):
            nums.add(round(float(obj), 1))

    _walk(context)
    return nums


def _validate_portfolio_schema(insight: dict, context: dict) -> list[str]:
    """
    Validate portfolio insight dict against context aggregates.

    Error codes:
      missing_field / empty_field       → hard error
      prohibited_phrase                 → hard error
      ungrounded_number                 → hard error (count or % not in context ±5)
      invalid_action_category           → hard error
      missing_evidence_limitations      → hard error
      potential_identifier              → warning only
    """
    errors: list[str] = []

    # Flatten all text from the insight
    text_parts: list[str] = []
    for v in insight.values():
        if isinstance(v, str):
            text_parts.append(v)
        elif isinstance(v, list):
            text_parts.extend(str(item) for item in v)
    all_text = " ".join(text_parts)
    all_text_lower = all_text.lower()

    # ── 1. Required fields ───────────────────────────────────────────────────
    for field in REQUIRED_PORTFOLIO_FIELDS:
        if field not in insight:
            errors.append(f"missing_field:{field}")
        else:
            val = insight[field]
            empty = (
                (isinstance(val, str) and not val.strip()) or
                (isinstance(val, list) and len(val) == 0)
            )
            if empty:
                errors.append(f"empty_field:{field}")

    # ── 2. Prohibited action patterns ────────────────────────────────────────
    for pat in _PROHIBITED_PATTERNS:
        if pat.search(all_text):
            errors.append(f"prohibited_phrase:{pat.pattern}")

    # ── 3. User-identifier detection (warning only) ───────────────────────────
    for label, pat in _IDENTIFIER_PATTERNS:
        if pat.search(all_text):
            errors.append(f"potential_identifier:{label}")

    # ── 4. Evidence limitations must be present ──────────────────────────────
    ev_lims = insight.get("evidence_limitations")
    if isinstance(ev_lims, list) and len(ev_lims) > 0:
        first = str(ev_lims[0]).lower().strip()
        if "no " in first and "limitation" in first:
            # "No limitations" is only acceptable if evidence is actually complete
            model_counts = context.get("model_health", {}).get("status_counts", {})
            if model_counts.get("insufficient_evidence", 0) > 0:
                errors.append("missing_evidence_limitations:model_health_insufficient")

    # ── 5. priority_actions must reference deterministic categories ───────────
    priority_actions = insight.get("priority_actions", [])
    if isinstance(priority_actions, list):
        for action in priority_actions:
            action_lower = str(action).lower()
            # Check if any allowed keyword appears in the action text
            if not any(kw in action_lower for kw in PORTFOLIO_ALLOWED_ACTION_KEYWORDS):
                errors.append(f"invalid_action_category:{action[:60]}")

    # ── 6. Numerical grounding ────────────────────────────────────────────────
    context_numbers = _extract_context_numbers(context)

    # 6a. Percentage claims
    pct_matches = re.findall(r"\b(\d+(?:\.\d+)?)\s*%", all_text_lower)
    for num_str in pct_matches:
        try:
            num = float(num_str)
            if not any(abs(num - cn) <= _GROUNDING_TOLERANCE for cn in context_numbers):
                errors.append(f"ungrounded_number:{num}%")
        except ValueError:
            pass

    # 6b. Plain integer counts (≥ _COUNT_GROUNDING_MIN, not followed by time labels)
    count_matches = re.findall(
        r"\b(\d{1,})\b(?!\s*%|\s*[-]?(?:day|days|week|weeks|month|months|year|years)\b)",
        all_text_lower,
    )
    for num_str in count_matches:
        try:
            num = float(num_str)
            if num < _COUNT_GROUNDING_MIN:
                continue
            if not any(abs(num - cn) <= _GROUNDING_TOLERANCE for cn in context_numbers):
                errors.append(f"ungrounded_count:{int(num)}")
        except ValueError:
            pass

    return errors


# ── Deterministic fallback ─────────────────────────────────────────────────────

def generate_rule_based_portfolio_insight(context: dict[str, Any]) -> dict[str, Any]:
    """
    Rule-based portfolio summary using the same schema as the LLM output.
    Triggered when: no API key, API failure, invalid JSON, or validation failure.
    """
    n = context.get("total_report_count", 0)
    hist = context.get("historical_usage", {})
    fcast = context.get("forecast_outlook", {})
    eng = context.get("engagement", {})
    model = context.get("model_health", {})
    ds = context.get("decision_support", {})
    ev = context.get("portfolio_evidence", {})

    # Executive summary
    growing = hist.get("growing", 0)
    declining = hist.get("declining", 0)
    high_pri = ds.get("review_priority_counts", {}).get("high", 0)
    executive_summary = (
        f"The portfolio contains {n} reports. "
        f"{growing} report{'s' if growing != 1 else ''} show growing usage and "
        f"{declining} show declining usage. "
        f"{high_pri} report{'s' if high_pri != 1 else ''} have been assigned high review priority "
        f"by the analytics pipeline."
    )

    # Usage summary
    stable = hist.get("stable", 0)
    inactive = hist.get("inactive", 0)
    portfolio_usage_summary = (
        f"Of {n} reports: {growing} growing, {stable} stable, "
        f"{declining} declining, {inactive} inactive. "
        + (f"{hist.get('long_zero_usage_streak_count', 0)} report(s) have extended zero-usage streaks." if hist.get("long_zero_usage_streak_count", 0) > 0 else "")
    ).strip()

    # Engagement summary
    decl_breadth = eng.get("declining_active_user_breadth", 0)
    elevated_lapse = eng.get("elevated_lapse", 0)
    eng_status_counts = eng.get("status_counts", {})
    top_eng = next(iter(eng_status_counts), "unknown") if eng_status_counts else "unknown"
    portfolio_engagement_summary = (
        f"The most common engagement status is {top_eng.replace('_', ' ')}. "
        + (f"{decl_breadth} report(s) show declining active-user breadth. " if decl_breadth > 0 else "")
        + (f"{elevated_lapse} report(s) have elevated user lapse." if elevated_lapse > 0 else "")
    ).strip()

    # Forecast summary
    growth_exp = fcast.get("growth_expected", 0)
    decline_exp = fcast.get("decline_expected", 0)
    high_unc = fcast.get("high_or_very_high_uncertainty", 0)
    portfolio_forecast_summary = (
        f"{growth_exp} report{'s' if growth_exp != 1 else ''} {'are' if growth_exp != 1 else 'is'} "
        f"expected to grow; {decline_exp} {'are' if decline_exp != 1 else 'is'} expected to decline. "
        f"{high_unc} report{'s' if high_unc != 1 else ''} "
        f"{'have' if high_unc != 1 else 'has'} high or very high forecast uncertainty."
    )

    # Model health
    model_counts = model.get("status_counts", {})
    insuf_ev = model_counts.get("insufficient_evidence", 0)
    portfolio_model_health_summary = (
        f"Model diagnostic evidence is insufficient for {insuf_ev} of {n} reports. "
        "Model health assessment will improve as production forecasts mature."
        if insuf_ev > 0
        else f"Model health covers all {n} reports."
    )

    # Priority actions from deterministic action counts
    action_cts = ds.get("recommended_action_counts", {})
    priority_actions = [
        f"Review {cnt} report{'s' if cnt != 1 else ''} with action: {action.replace('_', ' ')}"
        for action, cnt in sorted(action_cts.items(), key=lambda x: -x[1])
        if action not in {"continue_monitoring", "insufficient_evidence"} and cnt > 0
    ][:5] or ["Continue monitoring all reports."]

    # Positive signals
    positive_signals = list(context.get("top_positive_signals", []))[:3] or ["No positive signals identified."]

    # Evidence limitations
    limitations: list[str] = []
    if ev.get("reports_with_missing_metadata", 0) > 0:
        limitations.append(
            f"{ev['reports_with_missing_metadata']} reports have missing metadata "
            "(criticality, ownership, expected cadence unknown)."
        )
    if ev.get("reports_with_privacy_suppression", 0) > 0:
        limitations.append(
            f"{ev['reports_with_privacy_suppression']} reports have privacy-suppressed engagement metrics."
        )
    if insuf_ev > 0:
        limitations.append(
            f"Model diagnostic evidence is insufficient for {insuf_ev} report(s) — "
            "model health assessment is limited."
        )
    if not limitations:
        limitations.append("No material evidence limitations identified.")

    return {
        "executive_summary": executive_summary,
        "portfolio_usage_summary": portfolio_usage_summary,
        "portfolio_engagement_summary": portfolio_engagement_summary,
        "portfolio_forecast_summary": portfolio_forecast_summary,
        "portfolio_model_health_summary": portfolio_model_health_summary,
        "priority_actions": priority_actions,
        "positive_signals": positive_signals,
        "evidence_limitations": limitations,
        "generation_mode": "rule_based_fallback",
    }


# ── API call ───────────────────────────────────────────────────────────────────

def _call_api_for_portfolio(context: dict, model: str, api_key: str) -> dict:
    """Call OpenAI API with portfolio prompt and return validated insight dict."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "input": [
            {"role": "system", "content": PORTFOLIO_INSIGHT_SYSTEM_PROMPT},
            {"role": "user",   "content": build_portfolio_insight_prompt(context)},
        ],
        "temperature": 0.1,
        "max_output_tokens": 1200,
        "text": {"format": {"type": "json_object"}},
    }

    last_exc: Exception | None = None
    for attempt in range(1, MAX_API_RETRIES + 1):
        try:
            resp = requests.post(
                "https://api.openai.com/v1/responses",
                headers=headers,
                json=payload,
                timeout=60,
            )
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 2 ** attempt * RETRY_BACKOFF_SECONDS))
                time.sleep(retry_after)
                continue
            if resp.status_code == 401:
                raise ValueError("OpenAI authentication failed (401). Check API key.")
            resp.raise_for_status()

            data = resp.json()
            output_text = ""
            try:
                output_text = data["output"][0]["content"][0]["text"]
            except (KeyError, IndexError, TypeError):
                try:
                    output_text = data["choices"][0]["message"]["content"]
                except (KeyError, IndexError, TypeError):
                    pass

            parsed = _parse_portfolio_json(output_text)
            errors = _validate_portfolio_schema(parsed, context)
            return {
                "parsed": parsed,
                "validation_errors": errors,
                "api_attempts": attempt,
                "raw_response": output_text[:500],
            }

        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            time.sleep(2 ** attempt * RETRY_BACKOFF_SECONDS)
        except ValueError:
            raise
        except Exception as exc:
            last_exc = exc
            time.sleep(2 ** attempt * RETRY_BACKOFF_SECONDS)

    raise RuntimeError(f"Portfolio API call failed after {MAX_API_RETRIES} attempts: {last_exc}")


# ── Main generation ────────────────────────────────────────────────────────────

def generate_portfolio_insight(
    context: dict[str, Any],
    model: str,
    api_key: str | None = None,
    existing_insight: dict | None = None,
    prompt_version: str | None = None,
) -> dict[str, Any]:
    """
    Generate one portfolio-level insight from pre-computed context.

    Returns a lineage-enriched dict with:
      - All structured portfolio fields
      - generation_status: 'success' | 'reused' | 'rule_based' | 'fallback_*'
      - Full lineage fields
    """
    if prompt_version is None:
        prompt_version = PORTFOLIO_INSIGHT_PROMPT_VERSION

    input_hash = _compute_portfolio_hash(context, prompt_version, model)

    # Skip-unchanged: reuse a valid previous output when hash matches
    if (
        existing_insight is not None
        and existing_insight.get("input_hash") == input_hash
        and existing_insight.get("generation_status") in {"success", "reused"}
    ):
        return {
            **existing_insight,
            "generation_status": "reused",
            "reused_at": datetime.now(timezone.utc).isoformat(),
        }

    lineage: dict[str, Any] = {
        "analytics_run_id":    context.get("analytics_run_id"),
        "analytics_as_of_date": context.get("analytics_as_of_date"),
        "report_count":        context.get("total_report_count"),
        "prompt_version":      prompt_version,
        "model_name":          model,
        "input_hash":          input_hash,
        "generated_at":        datetime.now(timezone.utc).isoformat(),
    }

    # Try LLM
    if api_key:
        try:
            result  = _call_api_for_portfolio(context, model, api_key)
            parsed  = result["parsed"]
            errors  = result["validation_errors"]

            hard_errors    = [e for e in errors if not e.startswith("potential_identifier")]
            warning_errors = [e for e in errors if e.startswith("potential_identifier")]

            if hard_errors:
                fallback = generate_rule_based_portfolio_insight(context)
                return {
                    **lineage,
                    **fallback,
                    "generation_status": "fallback_schema_invalid",
                    "validation_status": "invalid",
                    "generation_error":  f"hard_errors:{hard_errors}",
                    "api_attempts":      result.get("api_attempts", 0),
                }

            return {
                **lineage,
                **parsed,
                "generation_status": "success",
                "validation_status": "warnings" if warning_errors else "valid",
                "generation_error":  None,
                "api_attempts":      result.get("api_attempts", 0),
            }

        except Exception as exc:
            fallback = generate_rule_based_portfolio_insight(context)
            return {
                **lineage,
                **fallback,
                "generation_status": "fallback_api_error",
                "validation_status": "valid",
                "generation_error":  str(exc)[:300],
                "api_attempts":      MAX_API_RETRIES,
            }

    # No API key — rule-based only
    fallback = generate_rule_based_portfolio_insight(context)
    return {
        **lineage,
        **fallback,
        "generation_status": "rule_based",
        "validation_status": "valid",
        "generation_error":  None,
        "api_attempts":      0,
    }


# ── Save / render ──────────────────────────────────────────────────────────────

def save_portfolio_insight(
    insight: dict[str, Any],
    project_root: Path | None = None,
) -> dict[str, Path]:
    """Write portfolio insight to JSON (canonical) and Markdown (optional)."""
    root = project_root or _get_project_root()
    output_dir = root / "outputs" / "insights"
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / PORTFOLIO_OUTPUT_JSON
    md_path   = output_dir / PORTFOLIO_OUTPUT_MD

    json_path.write_text(json.dumps(insight, indent=2, default=str), encoding="utf-8")
    md_path.write_text(_render_portfolio_markdown(insight), encoding="utf-8")
    return {"json": json_path, "markdown": md_path}


def _render_portfolio_markdown(insight: dict[str, Any]) -> str:
    """Render a portfolio insight as a brief Markdown management summary."""
    lines = [
        "# Portfolio AI Insight",
        "",
        f"- **Report count:** {insight.get('report_count', 'N/A')}",
        f"- **Analytics as-of:** {insight.get('analytics_as_of_date', 'N/A')}",
        f"- **Generation status:** {insight.get('generation_status', 'N/A')}",
        f"- **Prompt version:** {insight.get('prompt_version', 'N/A')}",
        "",
        "## Executive Summary",
        "",
        str(insight.get("executive_summary", "")),
        "",
        "## Usage",
        "",
        str(insight.get("portfolio_usage_summary", "")),
        "",
        "## Engagement",
        "",
        str(insight.get("portfolio_engagement_summary", "")),
        "",
        "## Forecast Outlook",
        "",
        str(insight.get("portfolio_forecast_summary", "")),
        "",
        "## Model Health",
        "",
        str(insight.get("portfolio_model_health_summary", "")),
        "",
        "## Priority Actions",
        "",
    ]
    for action in insight.get("priority_actions", []):
        lines.append(f"- {action}")
    lines += ["", "## Positive Signals", ""]
    for sig in insight.get("positive_signals", []):
        lines.append(f"- {sig}")
    lines += ["", "## Evidence Limitations", ""]
    for lim in insight.get("evidence_limitations", []):
        lines.append(f"- {lim}")
    lines.append("")
    return "\n".join(lines)


# ── Pipeline entry point ───────────────────────────────────────────────────────

def run_portfolio_pipeline(
    project_root: Path | None = None,
    model: str = "gpt-4.1-mini",
    genai_run_id: str | None = None,
) -> dict[str, Path]:
    """
    Build portfolio context from mart, generate portfolio insight, and save outputs.

    May be called standalone or from insight_generator.run_pipeline() so it shares
    the same genai_run_id as the report-level batch.
    """
    root = project_root or _get_project_root()
    load_dotenv(root / ".env", override=True)
    api_key = os.environ.get("OPENAI_API_KEY", "")

    mart_path = root / "outputs" / "analytics" / "mart_report_analytics.csv"
    if not mart_path.exists():
        raise FileNotFoundError(
            f"Canonical mart not found: {mart_path}. "
            "Run the analytics pipeline first."
        )

    mart_df = pd.read_csv(mart_path)
    print(f"[portfolio] Loaded mart: {len(mart_df)} reports")

    context = build_portfolio_context(mart_df)
    existing = _load_existing_portfolio(root)

    insight = generate_portfolio_insight(
        context,
        model=model,
        api_key=api_key or None,
        existing_insight=existing,
        prompt_version=PORTFOLIO_INSIGHT_PROMPT_VERSION,
    )
    insight["genai_run_id"] = genai_run_id or str(uuid.uuid4())
    print(f"[portfolio] Generation status: {insight.get('generation_status')}")

    paths = save_portfolio_insight(insight, root)
    for label, path in paths.items():
        print(f"[portfolio] {label}: {path}")
    return paths


def _get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


if __name__ == "__main__":
    run_portfolio_pipeline()
