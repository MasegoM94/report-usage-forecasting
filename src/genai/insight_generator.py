"""Generate batch report-level AI insights from the analytics mart.

Design principle: the analytics layer calculates and decides; the GenAI layer explains.

Canonical input: outputs/analytics/mart_report_analytics.csv
Output: outputs/insights/report_ai_insights.json

If OPENAI_API_KEY is not set, deterministic rule-based fallbacks are used so
demos and portfolio reviews still run without any API access.

Legacy 4-CSV re-join (forecast/metrics/segments/diagnostics) has been replaced
by the mart as the canonical input. load_input_tables and build_report_contexts
are retained for backward compatibility but are no longer called by the main pipeline.
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
    REPORT_INSIGHT_SYSTEM_PROMPT,
    REPORT_INSIGHT_PROMPT_VERSION,
    build_report_insight_prompt,
)

# ── Paths ──────────────────────────────────────────────────────────────────
MART_REPORT_ANALYTICS_PATH = "outputs/analytics/mart_report_analytics.csv"

# ── Legacy input candidates (kept for backward compatibility) ──────────────
FORECAST_INPUT_CANDIDATES = [
    "report_forecasts.csv",
    "report_view_forecasts_latest.csv",
    "sample_baseline_forecasts.csv",
]
METRICS_INPUT_CANDIDATES = [
    "model_performance.csv",
    "report_view_metrics_latest.csv",
    "report_model_comparison_latest.csv",
]
SEGMENTS_INPUT = "report_segments.csv"
DIAGNOSTICS_INPUT = "report_diagnostics.csv"

DEFAULT_MODEL = "gpt-4.1-mini"
OUTPUT_JSON = "report_ai_insights.json"
OUTPUT_MARKDOWN = "report_ai_insights.md"
MAX_API_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3
REQUEST_DELAY_SECONDS = 3

# ── Explicit allowlist — only these fields enter the LLM context ───────────
INSIGHT_CONTEXT_ALLOWLIST = [
    "report_id", "report_name", "analytics_run_id", "analytics_as_of_date",
    "historical_usage_status", "recent_28d_views", "previous_28d_views",
    "usage_change_28d_pct", "days_since_last_use",
    # forecast
    "forecast_total_28d", "forecast_change_vs_actual_28d_pct",
    "forecast_outlook_status", "forecast_uncertainty_status",
    # model
    "model_diagnostic_status", "primary_model_issue", "forecast_interpretation_status",
    # engagement
    "unique_users_28d", "active_user_direction_28d", "returning_user_share_28d",
    "retained_user_rate_28d", "lapse_rate_28d", "overall_engagement_status",
    "dependency_status",
    # metadata
    "criticality_level", "expected_usage_cadence",
    # diagnostics and final status
    "primary_diagnostic", "overall_report_status", "overall_evidence_status",
    "recommended_report_action", "overall_review_priority", "report_reasons",
    # privacy
    "privacy_suppression_status", "privacy_suppressed_fields",
    "missing_engagement_evidence",
]

# Prohibited field patterns — never allowed in context regardless of allowlist
PROHIBITED_CONTEXT_PATTERNS = frozenset({
    "user_id", "user_key", "email", "email_address", "user_name", "username",
    "display_name", "unique_user", "principal_name",
    "repeat_rate", "latest_views", "prior_views", "top_user_concentration",
    "usage_change_pct",  # deprecated (canonical: usage_change_28d_pct)
})

# Prohibited phrases in LLM output
PROHIBITED_OUTPUT_PHRASES = frozenset({
    "retire", "retirement", "delete", "deleting", "deletion", "retrain", "retraining",
    "replace the model", "replace model", "model replacement",
    "restrict user", "contact user",
})

# Structured output required fields
REQUIRED_INSIGHT_FIELDS = {
    "executive_summary", "usage_insight", "engagement_insight",
    "forecast_insight", "model_confidence_note", "recommended_action",
    "evidence_limitations",
}

# Action allowlist keywords (matches mart constants)
ALLOWED_INSIGHT_ACTIONS_KEYWORDS = {
    "continue monitoring", "monitor", "investigate", "review", "improve",
    "validate", "complete", "support", "insufficient evidence",
}


# ── Project root ───────────────────────────────────────────────────────────

def get_project_root() -> Path:
    """Return the repository root based on this module file location."""
    return Path(__file__).resolve().parents[2]


# ── Context building ───────────────────────────────────────────────────────

def build_mart_context(mart_df: pd.DataFrame, report_id_filter=None) -> list[dict]:
    """
    Build one privacy-safe context dict per report from mart_report_analytics.csv.

    - Applies INSIGHT_CONTEXT_ALLOWLIST (only listed fields enter the LLM)
    - Strips any remaining prohibited fields
    - Does NOT reconstruct suppressed values
    """
    if report_id_filter is not None:
        mart_df = mart_df[mart_df["report_id"].astype(str).isin(
            [str(r) for r in report_id_filter]
        )]

    contexts = []
    for _, row in mart_df.iterrows():
        ctx = {}
        for field in INSIGHT_CONTEXT_ALLOWLIST:
            if field in mart_df.columns:
                val = row[field]
                ctx[field] = None if (isinstance(val, float) and pd.isna(val)) else val

        # Safety net: strip any prohibited field that slipped through
        for pat in PROHIBITED_CONTEXT_PATTERNS:
            ctx.pop(pat, None)

        # Verify no prohibited field names remain (exact match only — the allowlist
        # may contain fields whose names contain prohibited substrings, e.g. unique_users_28d)
        remaining_prohibited = {k for k in ctx if k in PROHIBITED_CONTEXT_PATTERNS}
        if remaining_prohibited:
            raise ValueError(
                f"Prohibited fields in context for {row.get('report_id')}: {remaining_prohibited}"
            )

        contexts.append(ctx)

    return contexts


# ── Legacy loaders (kept for backward compatibility) ───────────────────────

def _read_first_existing_csv(directory: Path, filenames: list[str]) -> pd.DataFrame:
    """Read the first matching CSV from a directory, or return an empty frame."""
    for filename in filenames:
        path = directory / filename
        if path.exists():
            return pd.read_csv(path)
    return pd.DataFrame()


def _standardize_report_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize common report identifier column variants."""
    if df.empty:
        return df.copy()
    rename_map = {}
    if "ReportId" in df.columns and "report_id" not in df.columns:
        rename_map["ReportId"] = "report_id"
    if "ReportName" in df.columns and "report_name" not in df.columns:
        rename_map["ReportName"] = "report_name"
    return df.rename(columns=rename_map).copy()


def _to_bool(value: Any) -> bool:
    """Convert common CSV boolean representations to a Python bool."""
    if value is None or pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def load_input_tables(project_root: Path | None = None) -> dict[str, pd.DataFrame]:
    """Load forecast, performance, segment, and diagnostic CSV outputs (legacy)."""
    root = project_root or get_project_root()
    return {
        "forecasts": _read_first_existing_csv(root / "outputs" / "forecasts", FORECAST_INPUT_CANDIDATES),
        "metrics": _read_first_existing_csv(root / "outputs" / "metrics", METRICS_INPUT_CANDIDATES),
        "segments": _read_first_existing_csv(root / "outputs" / "segments", [SEGMENTS_INPUT]),
        "diagnostics": _read_first_existing_csv(root / "outputs" / "diagnostics", [DIAGNOSTICS_INPUT]),
    }


def summarize_forecasts(forecasts: pd.DataFrame) -> pd.DataFrame:
    """Summarize report-level forecast output to one row per report (legacy)."""
    forecasts = _standardize_report_columns(forecasts)
    if forecasts.empty or "report_id" not in forecasts.columns:
        return pd.DataFrame(columns=["report_id", "report_name"])
    df = forecasts.dropna(subset=["report_id"]).copy()
    if df.empty:
        return pd.DataFrame(columns=["report_id", "report_name"])
    if "IsPlaceholderRow" in df.columns:
        df = df[df["IsPlaceholderRow"].fillna(0).astype(int).eq(0)]
    if "IsForecast" in df.columns:
        df = df[df["IsForecast"].fillna(1).astype(int).eq(1)]
    if df.empty:
        return pd.DataFrame(columns=["report_id", "report_name"])
    if "forecast" in df.columns:
        df["forecast"] = pd.to_numeric(df["forecast"], errors="coerce")
    else:
        df["forecast"] = pd.NA
    group_cols = ["report_id"]
    agg = df.groupby(group_cols, as_index=False).agg(
        report_name=("report_name", "first") if "report_name" in df.columns else ("report_id", "first"),
        forecast_periods=("forecast", "count"),
        avg_forecast=("forecast", "mean"),
        min_forecast=("forecast", "min"),
        max_forecast=("forecast", "max"),
    )
    return agg


def summarize_metrics(metrics: pd.DataFrame) -> pd.DataFrame:
    """Summarize model performance output to one row per report (legacy)."""
    metrics = _standardize_report_columns(metrics)
    if metrics.empty or "report_id" not in metrics.columns:
        return pd.DataFrame(columns=["report_id", "report_name"])
    df = metrics.dropna(subset=["report_id"]).copy()
    if "selected_model_flag" in df.columns:
        selected = df[df["selected_model_flag"].astype(str).str.lower().isin(["true", "1"])]
        if not selected.empty:
            df = selected
    sort_cols = [col for col in ["forecast_reliable", "selected_wape", "wape", "mae"] if col in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=[False] + [True] * (len(sort_cols) - 1))
    summary = df.groupby("report_id", as_index=False).first()
    keep_cols = [
        "report_id", "report_name", "selected_model", "model_name",
        "selected_mae", "mae", "selected_rmse", "rmse",
        "selected_wape", "wape", "forecast_reliable",
        "passes_data_criteria", "passes_model_criteria",
    ]
    return summary[[col for col in keep_cols if col in summary.columns]]


def build_report_contexts(tables: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    """Join input tables at report_id level (legacy — not used in new pipeline)."""
    forecast_summary = summarize_forecasts(tables.get("forecasts", pd.DataFrame()))
    metrics_summary = summarize_metrics(tables.get("metrics", pd.DataFrame()))
    segments = _standardize_report_columns(tables.get("segments", pd.DataFrame()))
    diagnostics = _standardize_report_columns(tables.get("diagnostics", pd.DataFrame()))
    joined = forecast_summary
    for frame in [metrics_summary, segments, diagnostics]:
        if frame.empty or "report_id" not in frame.columns:
            continue
        if joined.empty:
            joined = frame.copy()
        else:
            joined = joined.merge(frame, on="report_id", how="outer", suffixes=("", "_extra"))
        for extra_col in [col for col in joined.columns if col.endswith("_extra")]:
            base_col = extra_col.removesuffix("_extra")
            if base_col in joined.columns:
                joined[base_col] = joined[base_col].fillna(joined[extra_col])
            joined = joined.drop(columns=[extra_col])
    if joined.empty or "report_id" not in joined.columns:
        return []
    contexts = []
    for _, row in joined.sort_values("report_id").iterrows():
        clean_row = {
            key: None if pd.isna(value) else value
            for key, value in row.to_dict().items()
        }
        contexts.append(clean_row)
    return contexts


# ── Hashing and skip-unchanged ─────────────────────────────────────────────

def _compute_context_hash(context: dict, prompt_version: str, model_name: str) -> str:
    """Deterministic hash of context + prompt version + model name."""
    payload = {
        "context": {k: str(v) for k, v in sorted(context.items())},
        "prompt_version": prompt_version,
        "model_name": model_name,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def _load_existing_insights(project_root: Path) -> dict:
    """Load existing insights indexed by report_id for skip-unchanged logic."""
    insights_path = project_root / "outputs" / "insights" / "report_ai_insights.json"
    if not insights_path.exists():
        return {}
    try:
        with open(insights_path) as f:
            existing = json.load(f)
        return {
            str(item["report_id"]): item
            for item in existing
            if item.get("input_hash") and item.get("generation_status") in {"success", "reused"}
        }
    except Exception:
        return {}


# ── JSON parsing and schema validation ────────────────────────────────────

def _parse_json_response(text: str) -> dict:
    """Extract and validate structured JSON from LLM response."""
    # Try fenced block first
    fence_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, re.IGNORECASE)
    if fence_match:
        candidate = fence_match.group(1).strip()
    else:
        brace_match = re.search(r"\{[\s\S]*\}", text)
        candidate = brace_match.group(0) if brace_match else text.strip()

    try:
        result = json.loads(candidate)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON parse failed: {e}") from e

    if not isinstance(result, dict):
        raise ValueError(f"Expected dict, got {type(result).__name__}")

    return result


def _validate_insight_schema(insight: dict, context: dict) -> list[str]:
    """
    Validate structured insight. Returns list of validation errors.
    Empty list = valid. Ungrounded numbers are warnings, not hard errors.
    """
    errors = []

    # 1. Required fields present and non-empty
    for field in REQUIRED_INSIGHT_FIELDS:
        if field not in insight:
            errors.append(f"missing_field:{field}")
        elif not str(insight[field]).strip():
            errors.append(f"empty_field:{field}")

    # 2. No prohibited phrases in any text field
    all_text = " ".join(str(v) for v in insight.values() if isinstance(v, str)).lower()
    for phrase in PROHIBITED_OUTPUT_PHRASES:
        if phrase in all_text:
            errors.append(f"prohibited_phrase:{phrase}")

    # 3. No user identifiers in output
    for pat in {"user_id", "user_key", "@", "email"}:
        if pat in all_text:
            errors.append(f"potential_identifier:{pat}")

    # 4. Numerical grounding — extract percentages and check against context
    numbers_in_text = re.findall(r"\b(\d+(?:\.\d+)?)\s*%", all_text)
    context_numbers = set()
    for v in context.values():
        if v is None:
            continue
        try:
            fv = float(v)
            context_numbers.add(round(fv, 1))
            context_numbers.add(round(fv * 100, 1))
        except (TypeError, ValueError):
            pass

    for num_str in numbers_in_text:
        try:
            num = float(num_str)
            if not any(abs(num - cn) <= 2.0 for cn in context_numbers):
                errors.append(f"ungrounded_number:{num}%")
        except ValueError:
            pass

    # 5. recommended_action must not be empty
    if "recommended_action" in insight and not str(insight["recommended_action"]).strip():
        errors.append("empty_recommended_action")

    return errors


# ── API call ───────────────────────────────────────────────────────────────

def _call_openai_api(report_context: dict, model: str, api_key: str) -> dict:
    """
    Call OpenAI with exponential backoff, JSON-mode, and schema validation.
    Returns parsed and validated insight dict.
    Raises on unrecoverable failure.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": model,
        "input": [
            {"role": "system", "content": REPORT_INSIGHT_SYSTEM_PROMPT},
            {"role": "user", "content": build_report_insight_prompt(report_context)},
        ],
        "temperature": 0.1,
        "max_output_tokens": 700,
        "text": {"format": {"type": "json_object"}},
    }

    last_exc = None
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
                output_text = data.get("output", [{}])[0].get("content", [{}])[0].get("text", "")
            except (IndexError, AttributeError, TypeError):
                pass
            if not output_text:
                try:
                    output_text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                except (IndexError, AttributeError, TypeError):
                    pass

            parsed = _parse_json_response(output_text)
            errors = _validate_insight_schema(parsed, report_context)

            return {
                "parsed": parsed,
                "validation_errors": errors,
                "api_attempts": attempt,
                "raw_response": output_text[:500],
            }

        except (requests.Timeout, requests.ConnectionError) as exc:
            last_exc = exc
            wait = 2 ** attempt * RETRY_BACKOFF_SECONDS
            time.sleep(wait)
        except ValueError:
            raise
        except Exception as exc:
            last_exc = exc
            wait = 2 ** attempt * RETRY_BACKOFF_SECONDS
            time.sleep(wait)

    raise RuntimeError(f"API call failed after {MAX_API_RETRIES} attempts: {last_exc}")


# Keep legacy name as alias
_call_openai_responses_api = _call_openai_api


# ── Rule-based fallback ────────────────────────────────────────────────────

def generate_rule_based_insight(report_context: dict[str, Any]) -> dict[str, Any]:
    """
    Deterministic fallback insight using only fields from the report context.
    Returns the new structured schema plus backward-compat Streamlit aliases.
    No LLM calls. No field inference.
    """
    report_name = report_context.get("report_name", "this report")

    # Historical usage
    hist_status = report_context.get("historical_usage_status", "unknown") or "unknown"
    recent_views = report_context.get("recent_28d_views")
    usage_str = f"{int(recent_views)} views" if recent_views is not None else "unknown views"
    usage_insight = (
        f"{report_name} shows {hist_status.replace('_', ' ')} with {usage_str} "
        f"in the recent 28-day window."
    )

    # Engagement
    eng_status = report_context.get("overall_engagement_status", "") or ""
    eng_evidence = (
        report_context.get("missing_engagement_evidence")
        or report_context.get("overall_evidence_status", "")
        or ""
    )
    if eng_evidence and str(eng_evidence).lower() not in ("complete", "none", "nan", ""):
        engagement_insight = (
            f"Engagement evidence is limited ({eng_evidence}); full engagement analysis is unavailable."
        )
    elif eng_status:
        unique_users = report_context.get("unique_users_28d")
        users_str = f" ({int(unique_users)} active users)" if unique_users is not None else ""
        engagement_insight = f"User engagement status is {eng_status.replace('_', ' ')}{users_str}."
    else:
        engagement_insight = "Engagement data is unavailable for this report."

    # Forecast
    forecast_status = report_context.get("forecast_outlook_status", "") or ""
    uncertainty = report_context.get("forecast_uncertainty_status", "") or ""
    if forecast_status:
        forecast_insight = (
            f"The forecast outlook is {forecast_status.replace('_', ' ')}. "
            + (f"Forecast uncertainty is {uncertainty.replace('_', ' ')}." if uncertainty else "")
        ).strip()
    else:
        forecast_insight = "Forecast data is unavailable for this report."

    # Model confidence
    model_status = report_context.get("model_diagnostic_status", "") or ""
    interp_status = report_context.get("forecast_interpretation_status", "") or ""
    if model_status:
        model_confidence_note = (
            f"Model diagnostic status is {model_status.replace('_', ' ')}. "
            + (f"Forecast interpretation: {interp_status.replace('_', ' ')}." if interp_status else "")
        ).strip()
    else:
        model_confidence_note = "Model diagnostic information is unavailable."

    # Recommended action
    recommended_report_action = (
        report_context.get("recommended_report_action", "continue_monitoring") or "continue_monitoring"
    )
    recommended_action = recommended_report_action.replace("_", " ").capitalize() + "."

    # Evidence limitations
    limitations = []
    overall_ev = report_context.get("overall_evidence_status", "") or ""
    if overall_ev and str(overall_ev).lower() not in ("complete", "none", ""):
        limitations.append(f"Overall evidence: {overall_ev}")
    priv = report_context.get("privacy_suppression_status", "") or ""
    if priv and str(priv).lower() not in ("not_suppressed", "none", ""):
        limitations.append("Some metrics are privacy-suppressed")

    # Executive summary
    overall_status = report_context.get("overall_report_status", hist_status) or hist_status
    primary_diag = report_context.get("primary_diagnostic", "none") or "none"
    executive_summary = (
        f"{report_name} has an overall status of {overall_status.replace('_', ' ')}. "
        f"The primary diagnostic is {primary_diag.replace('_', ' ')}. "
        f"Recommended action: {recommended_action}"
    )

    result = {
        # New schema
        "executive_summary": executive_summary,
        "usage_insight": usage_insight,
        "engagement_insight": engagement_insight,
        "forecast_insight": forecast_insight,
        "model_confidence_note": model_confidence_note,
        "recommended_action": recommended_action,
        "evidence_limitations": limitations if limitations else ["No significant evidence limitations identified."],
        # Backward-compat aliases for Streamlit
        "health_status": overall_status,
        "forecast_summary": forecast_insight,
        "recommended_actions": [recommended_action],
        "hypotheses": [],
        "key_drivers": [],
        "confidence": "medium",
        "generation_mode": "rule_based_fallback",
    }
    return result


# ── Main AI insight generation ─────────────────────────────────────────────

def generate_ai_insight(
    report_context: dict[str, Any],
    model: str = DEFAULT_MODEL,
    api_key: str | None = None,
    existing_insight: dict | None = None,
    prompt_version: str | None = None,
) -> dict[str, Any]:
    """
    Generate one structured insight for a report context.

    Returns a lineage-enriched dict with:
    - All structured insight fields (new schema)
    - Backward-compat aliases for Streamlit (health_status, forecast_summary, etc.)
    - generation_status: 'success' | 'reused' | 'rule_based' | 'fallback_*'
    - validation_status: 'valid' | 'invalid' | 'warnings'
    - Full lineage fields
    """
    if prompt_version is None:
        prompt_version = REPORT_INSIGHT_PROMPT_VERSION

    input_hash = _compute_context_hash(report_context, prompt_version, model)

    # --- Skip-unchanged logic ---
    if existing_insight and existing_insight.get("input_hash") == input_hash:
        return {
            **existing_insight,
            "generation_status": "reused",
            "reused_at": datetime.now(timezone.utc).isoformat(),
        }

    lineage = {
        "report_id": report_context.get("report_id"),
        "report_name": report_context.get("report_name"),
        "analytics_run_id": report_context.get("analytics_run_id"),
        "analytics_as_of_date": report_context.get("analytics_as_of_date"),
        "prompt_version": prompt_version,
        "model_name": model,
        "input_hash": input_hash,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # --- Try LLM ---
    if api_key:
        try:
            result = _call_openai_api(report_context, model, api_key)
            parsed = result["parsed"]
            errors = result["validation_errors"]

            schema_errors = [
                e for e in errors
                if "missing_field" in e or "empty_field" in e or "prohibited_phrase" in e
            ]
            warning_errors = [e for e in errors if "ungrounded_number" in e]

            if schema_errors:
                validation_status = "invalid"
            elif warning_errors:
                validation_status = "warnings"
            else:
                validation_status = "valid"

            # On invalid schema, fall back to rule-based
            if validation_status == "invalid":
                fallback = generate_rule_based_insight(report_context)
                return {
                    **lineage,
                    **fallback,
                    "generation_status": "fallback_schema_invalid",
                    "validation_status": "invalid",
                    "generation_error": f"schema_errors:{schema_errors}",
                    "api_attempts": result.get("api_attempts", 0),
                }

            # Add backward-compat aliases for Streamlit
            parsed.setdefault("health_status", report_context.get("overall_report_status", ""))
            parsed.setdefault("forecast_summary", parsed.get("forecast_insight", ""))
            parsed.setdefault("recommended_actions", [parsed.get("recommended_action", "")])
            parsed.setdefault("hypotheses", [])
            parsed.setdefault("key_drivers", [])
            parsed.setdefault("confidence", "medium")
            parsed.setdefault("generation_mode", "openai")

            return {
                **lineage,
                **parsed,
                "generation_status": "success",
                "validation_status": validation_status,
                "generation_error": None,
                "api_attempts": result.get("api_attempts", 0),
            }

        except Exception as exc:
            fallback = generate_rule_based_insight(report_context)
            return {
                **lineage,
                **fallback,
                "generation_status": "fallback_api_error",
                "validation_status": "valid",
                "generation_error": str(exc)[:300],
                "api_attempts": MAX_API_RETRIES,
            }

    # --- No API key — rule-based only ---
    fallback = generate_rule_based_insight(report_context)
    return {
        **lineage,
        **fallback,
        "generation_status": "rule_based",
        "validation_status": "valid",
        "generation_error": None,
        "api_attempts": 0,
    }


# ── Batch generation ───────────────────────────────────────────────────────

def generate_report_insights(
    project_root: Path | None = None,
    model: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """
    Main batch generation function. Reads mart_report_analytics.csv as canonical input.
    Uses skip-unchanged logic based on context hashing.
    One report failure does not stop the batch.
    """
    project_root = project_root or get_project_root()
    load_dotenv(project_root / ".env", override=True)
    model = model or DEFAULT_MODEL
    api_key = os.environ.get("OPENAI_API_KEY", "")

    # --- Load canonical mart ---
    mart_path = project_root / MART_REPORT_ANALYTICS_PATH
    if not mart_path.exists():
        raise FileNotFoundError(
            f"Canonical mart not found: {mart_path}. "
            "Run the Sprint 7 analytics pipeline first."
        )
    mart_df = pd.read_csv(mart_path)
    print(f"Loaded mart: {len(mart_df)} reports x {len(mart_df.columns)} columns")

    # --- Build contexts ---
    contexts = build_mart_context(mart_df)
    if limit is not None:
        contexts = contexts[:limit]

    # --- Load existing insights for skip-unchanged ---
    existing_insights = _load_existing_insights(project_root)

    # --- Generate (with shared genai_run_id for this batch) ---
    genai_run_id = str(uuid.uuid4())
    print(f"GenAI run: {genai_run_id}")

    insights = []
    for i, ctx in enumerate(contexts):
        report_id = str(ctx.get("report_id", ""))
        existing = existing_insights.get(report_id)

        try:
            insight = generate_ai_insight(
                ctx, model, api_key,
                existing_insight=existing,
                prompt_version=REPORT_INSIGHT_PROMPT_VERSION,
            )
        except Exception as exc:
            # One failure must not stop the batch
            try:
                fallback = generate_rule_based_insight(ctx)
            except Exception:
                fallback = {}
            insight = {
                "report_id": ctx.get("report_id"),
                "report_name": ctx.get("report_name"),
                "analytics_run_id": ctx.get("analytics_run_id"),
                "analytics_as_of_date": ctx.get("analytics_as_of_date"),
                "prompt_version": REPORT_INSIGHT_PROMPT_VERSION,
                "model_name": model,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "input_hash": None,
                **fallback,
                "generation_status": "fallback_exception",
                "validation_status": "valid",
                "generation_error": str(exc)[:300],
                "api_attempts": 0,
            }

        insight["genai_run_id"] = genai_run_id
        insights.append(insight)

        status = insight.get("generation_status", "unknown")
        print(f"  [{i+1}/{len(contexts)}] {report_id}: {status}")

        if status not in ("reused",) and api_key:
            time.sleep(REQUEST_DELAY_SECONDS)

    return insights


# ── Save / render ──────────────────────────────────────────────────────────

def save_insights(
    insights: list[dict[str, Any]],
    project_root: Path | None = None,
) -> dict[str, Path]:
    """Save insights to JSON and Markdown output files."""
    root = project_root or get_project_root()
    output_dir = root / "outputs" / "insights"
    output_dir.mkdir(parents=True, exist_ok=True)

    json_path = output_dir / OUTPUT_JSON
    markdown_path = output_dir / OUTPUT_MARKDOWN

    json_path.write_text(json.dumps(insights, indent=2, default=str), encoding="utf-8")
    markdown_path.write_text(render_insights_markdown(insights), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path}


def render_insights_markdown(insights: list[dict[str, Any]]) -> str:
    """Render report insights as a simple Markdown review file."""
    lines = ["# Report AI Insights", ""]
    if not insights:
        lines.extend(["No report insights were generated.", ""])
        return "\n".join(lines)

    for insight in insights:
        title = insight.get("report_name") or insight.get("report_id")
        lines.extend([
            f"## {title}",
            "",
            f"- Report ID: {insight.get('report_id')}",
            f"- Overall status: {insight.get('health_status') or insight.get('overall_report_status')}",
            f"- Generation status: {insight.get('generation_status')}",
            f"- Prompt version: {insight.get('prompt_version')}",
            "",
            f"**Executive summary:** {insight.get('executive_summary', insight.get('forecast_summary', 'N/A'))}",
            "",
            "**Usage insight**",
        ])
        usage = insight.get("usage_insight", "")
        if usage:
            lines.append(usage)
        lines.extend(["", "**Engagement insight**"])
        eng = insight.get("engagement_insight", "")
        if eng:
            lines.append(eng)
        lines.extend(["", "**Forecast insight**"])
        fcst = insight.get("forecast_insight", insight.get("forecast_summary", ""))
        if fcst:
            lines.append(fcst)
        lines.extend(["", "**Recommended action**"])
        rec = insight.get("recommended_action", "")
        if rec:
            lines.append(rec)
        ev = insight.get("evidence_limitations", [])
        if ev:
            lines.extend(["", "**Evidence limitations**"])
            if isinstance(ev, list):
                lines.extend(f"- {item}" for item in ev)
            else:
                lines.append(str(ev))
        lines.append("")

    return "\n".join(lines)


# ── Pipeline entry point ───────────────────────────────────────────────────

def run_pipeline(project_root: Path | None = None, model: str = DEFAULT_MODEL) -> dict[str, Path]:
    """Generate and save all report insights."""
    root = project_root or get_project_root()
    insights = generate_report_insights(project_root=root, model=model)
    output_paths = save_insights(insights, project_root=root)
    print(f"Generated {len(insights)} report AI insights.")
    for label, path in output_paths.items():
        print(f"{label}: {path}")
    return output_paths


if __name__ == "__main__":
    run_pipeline()
