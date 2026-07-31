"""
Deterministic evaluation framework for GenAI insights.

Design principle: Forecast metrics evaluate the analytical model;
groundedness and usefulness evaluate the GenAI layer.

No LLM-as-judge. All checks are deterministic pattern and value matching.
Readability scores are heuristic approximations; human review is the
authoritative source for readability and stakeholder usefulness.

Covers:
  - Report-level insights (src/genai/insight_generator.py)
  - Portfolio-level insights (src/genai/portfolio_insights.py)
  - Deterministic fallback outputs
  - Prompt/model-version regression comparison
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.genai.insight_generator import (
    REQUIRED_INSIGHT_FIELDS,
    _PROHIBITED_ACTION_PATTERNS,
    _IDENTIFIER_PATTERNS,
    _check_direction_conflicts,
    _validate_insight_schema,
)
from src.genai.portfolio_insights import (
    REQUIRED_PORTFOLIO_FIELDS,
    _PROHIBITED_PATTERNS as _PORTFOLIO_PROHIBITED_PATTERNS,
    _IDENTIFIER_PATTERNS as _PORTFOLIO_IDENTIFIER_PATTERNS,
    _extract_context_numbers,
    _validate_portfolio_schema,
    build_portfolio_context,
)
from src.genai.prompts import (
    REPORT_INSIGHT_PROMPT_VERSION,
    PORTFOLIO_INSIGHT_PROMPT_VERSION,
)

# ── Paths ──────────────────────────────────────────────────────────────────────
_FIXTURES_DIR = Path(__file__).resolve().parents[2] / "tests" / "fixtures"
EVALUATION_CASES_FILE = _FIXTURES_DIR / "genai_evaluation_cases.json"
GOLDEN_OUTPUTS_FILE   = _FIXTURES_DIR / "genai_golden_outputs.json"

# ── Field limits (word count) ──────────────────────────────────────────────────
_REPORT_FIELD_WORD_LIMITS: dict[str, int] = {
    "executive_summary":   60,
    "usage_insight":       50,
    "engagement_insight":  50,
    "forecast_insight":    50,
    "model_confidence_note": 40,
    "recommended_action":  30,
    "evidence_limitations": 60,   # per item
}
_PORTFOLIO_FIELD_WORD_LIMITS: dict[str, int] = {
    "executive_summary":              80,
    "portfolio_usage_summary":        70,
    "portfolio_engagement_summary":   70,
    "portfolio_forecast_summary":     70,
    "portfolio_model_health_summary": 50,
    "priority_actions":               30,   # per item
    "positive_signals":               25,   # per item
    "evidence_limitations":           60,   # per item
}

# ── Action keyword map ─────────────────────────────────────────────────────────
# Maps mart recommended_report_action values → keywords that should appear
# in the generated recommended_action text.
_ACTION_KEYWORD_MAP: dict[str, set[str]] = {
    "continue_monitoring":           {"continue", "monitor"},
    "investigate_usage_decline":     {"investigate"},
    "review_planned_deprecation":    {"review", "deprecat"},
    "review_forecast_uncertainty":   {"review", "uncertain"},
    "investigate_user_decline":      {"investigate"},
    "investigate_user_lapse":        {"investigate", "lapse"},
    "validate_report_audience":      {"validate", "audience"},
    "complete_report_metadata":      {"complete", "metadata"},
    "review_model_health":           {"review", "model"},
    "review_model_uncertainty":      {"review", "uncertain"},
    "review_inactivity":             {"review", "inactiv"},
    "insufficient_evidence":         {"insufficient", "evidence"},
}

# ── Generic / vacuous phrases that indicate low-quality output ─────────────────
_GENERIC_PHRASES = frozenset({
    "n/a", "none", "no information available", "not applicable",
    "data is available", "information not found",
})

# ── Grounding tolerance (mirrors report-level and portfolio-level validators) ──
_GROUNDING_TOLERANCE = 5.0
_COUNT_GROUNDING_MIN_REPORT    = 10
_COUNT_GROUNDING_MIN_PORTFOLIO =  5
_RATE_FIELD_SUFFIXES = ("_pct", "_rate", "_share", "_ratio")


# ── Evaluation result ──────────────────────────────────────────────────────────

@dataclass
class EvaluationResult:
    case_id: str
    insight_type: str               # "report" | "portfolio"
    prompt_version: str
    model_name: str
    generation_status: str
    # Hard-failure dimensions
    completeness_pass: bool
    safety_pass: bool
    groundedness_pass: bool         # direction_pass AND numerical_pass
    direction_pass: bool
    numerical_pass: bool
    action_alignment_pass: bool
    evidence_disclosure_pass: bool
    # Soft scores (0.0–1.0)
    readability_score: float
    conciseness_score: float
    # Aggregate
    overall_pass: bool
    failure_reasons: list[str] = field(default_factory=list)
    evaluated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        d = asdict(self)
        d["failure_reasons"] = json.dumps(d["failure_reasons"])
        return d


def _overall_pass(
    completeness_pass: bool,
    safety_pass: bool,
    groundedness_pass: bool,
    direction_pass: bool,
    numerical_pass: bool,
    action_alignment_pass: bool,
    evidence_disclosure_pass: bool,
    readability_score: float,
) -> bool:
    """
    Hard-failure rule: ALL of the named boolean dimensions must be True,
    AND readability_score must meet the minimum threshold.
    readability and conciseness are soft scores; only readability is used
    in the overall_pass gate (threshold 0.5).
    """
    return all([
        completeness_pass,
        safety_pass,
        groundedness_pass,
        direction_pass,
        numerical_pass,
        action_alignment_pass,
        evidence_disclosure_pass,
        readability_score >= 0.5,
    ])


# ── Text helpers ───────────────────────────────────────────────────────────────

def _collect_text(insight: dict) -> str:
    """Flatten all string values (including list items) into one string."""
    parts: list[str] = []
    for v in insight.values():
        if isinstance(v, str):
            parts.append(v)
        elif isinstance(v, list):
            parts.extend(str(item) for item in v)
    return " ".join(parts)


def _collect_narrative_text(insight: dict, insight_type: str) -> str:
    """
    Flatten only the required narrative fields into one string.
    Used for numerical grounding so that lineage fields (dates, UUIDs,
    timestamps) do not generate false ungrounded-number failures.
    """
    required = (
        REQUIRED_INSIGHT_FIELDS if insight_type == "report"
        else REQUIRED_PORTFOLIO_FIELDS
    )
    parts: list[str] = []
    for k in required:
        v = insight.get(k)
        if isinstance(v, str):
            parts.append(v)
        elif isinstance(v, list):
            parts.extend(str(item) for item in v)
    return " ".join(parts)


def _collect_sentences(insight: dict) -> list[str]:
    """Extract all sentences from the insight text."""
    text = _collect_text(insight)
    return [s.strip() for s in re.split(r"[.!?]+", text) if s.strip()]


def _word_count(text: str) -> int:
    return len(text.split())


def _build_report_context_numbers(context: dict) -> set[float]:
    """Mirror the context_numbers logic from _validate_insight_schema."""
    nums: set[float] = set()
    for key, v in context.items():
        if v is None:
            continue
        try:
            fv = float(v)
            nums.add(round(fv, 1))
            if any(s in str(key) for s in _RATE_FIELD_SUFFIXES) and 0.0 < fv <= 1.0:
                nums.add(round(fv * 100, 1))
        except (TypeError, ValueError):
            pass
    return nums


# ── Individual dimension evaluators ───────────────────────────────────────────

def evaluate_completeness(
    insight: dict, insight_type: str
) -> tuple[bool, list[str]]:
    """Check all required fields are present and non-empty."""
    required = (
        REQUIRED_INSIGHT_FIELDS if insight_type == "report"
        else REQUIRED_PORTFOLIO_FIELDS
    )
    failures: list[str] = []
    for f in required:
        if f not in insight:
            failures.append(f"missing_field:{f}")
        else:
            val = insight[f]
            empty = (
                (isinstance(val, str) and not val.strip()) or
                (isinstance(val, list) and len(val) == 0)
            )
            if empty:
                failures.append(f"empty_field:{f}")
    return len(failures) == 0, failures


def evaluate_safety(
    insight: dict, insight_type: str = "report"
) -> tuple[bool, list[str]]:
    """Check for prohibited action phrases and user-identifier patterns."""
    patterns = (
        _PROHIBITED_ACTION_PATTERNS if insight_type == "report"
        else _PORTFOLIO_PROHIBITED_PATTERNS
    )
    id_patterns = (
        _IDENTIFIER_PATTERNS if insight_type == "report"
        else _PORTFOLIO_IDENTIFIER_PATTERNS
    )
    all_text = _collect_text(insight)
    failures: list[str] = []
    for pat in patterns:
        if pat.search(all_text):
            failures.append(f"prohibited_phrase:{pat.pattern}")
    # Identifier patterns are safety violations at portfolio level
    # and warnings at report level; we report both here
    for label, pat in id_patterns:
        if pat.search(all_text):
            failures.append(f"identifier_exposure:{label}")
    return len(failures) == 0, failures


def evaluate_directional_consistency(
    insight: dict, context: dict, insight_type: str = "report"
) -> tuple[bool, list[str]]:
    """
    Check that direction language in insight matches deterministic context statuses.
    For report-level: delegates to _check_direction_conflicts.
    For portfolio-level: checks that usage/forecast summary language does not
    contradict the deterministic aggregate direction distributions.
    """
    if insight_type == "report":
        conflicts = _check_direction_conflicts(insight, context)
        return len(conflicts) == 0, conflicts

    # Portfolio-level direction check: if declining > growing in historical_usage,
    # the usage summary must not use purely positive language, and vice-versa.
    failures: list[str] = []
    hist = context.get("historical_usage", {})
    growing = hist.get("growing", 0)
    declining = hist.get("declining", 0)
    usage_text = str(insight.get("portfolio_usage_summary", "")).lower()

    _GROWING = re.compile(
        r"\b(grow(?:ing|th)?|increas(?:ing|ed?)|expan(?:ding|sion)|upward)\b",
        re.IGNORECASE
    )
    _DECLINING = re.compile(
        r"\b(declin(?:ing|ed?)|decreas(?:ing|ed?)|drop(?:ping|ped)?|fall(?:ing)?|downward)\b",
        re.IGNORECASE
    )

    if declining > growing * 2 and _GROWING.search(usage_text) and not _DECLINING.search(usage_text):
        failures.append("direction_conflict:portfolio_usage_overly_positive")
    if growing > declining * 2 and _DECLINING.search(usage_text) and not _GROWING.search(usage_text):
        failures.append("direction_conflict:portfolio_usage_overly_negative")

    # Forecast direction
    fcast = context.get("forecast_outlook", {})
    growth_exp = fcast.get("growth_expected", 0)
    decline_exp = fcast.get("decline_expected", 0)
    forecast_text = str(insight.get("portfolio_forecast_summary", "")).lower()

    if decline_exp > growth_exp * 2 and _GROWING.search(forecast_text) and not _DECLINING.search(forecast_text):
        failures.append("direction_conflict:portfolio_forecast_overly_positive")

    return len(failures) == 0, failures


def evaluate_numerical_grounding(
    insight: dict, context: dict, insight_type: str = "report"
) -> tuple[bool, list[str]]:
    """
    Check that specific numbers in the insight are supported by the context.
    Tolerance: ±5 pp / ±5 count.
    Only scans narrative fields (not lineage fields such as dates or UUIDs).
    """
    all_text_lower = _collect_narrative_text(insight, insight_type).lower()
    failures: list[str] = []

    if insight_type == "report":
        context_numbers = _build_report_context_numbers(context)
        min_count = _COUNT_GROUNDING_MIN_REPORT
    else:
        context_numbers = _extract_context_numbers(context)
        min_count = _COUNT_GROUNDING_MIN_PORTFOLIO

    # Percentage claims
    for num_str in re.findall(r"\b(\d+(?:\.\d+)?)\s*%", all_text_lower):
        try:
            num = float(num_str)
            if not any(abs(num - cn) <= _GROUNDING_TOLERANCE for cn in context_numbers):
                failures.append(f"ungrounded_number:{num}%")
        except ValueError:
            pass

    # Count claims (exclude time-period labels)
    for num_str in re.findall(
        r"\b(\d{1,})\b(?!\s*%|\s*[-]?(?:day|days|week|weeks|month|months|year|years)\b)",
        all_text_lower,
    ):
        try:
            num = float(num_str)
            if num < min_count:
                continue
            if not any(abs(num - cn) <= _GROUNDING_TOLERANCE for cn in context_numbers):
                failures.append(f"ungrounded_count:{int(num)}")
        except ValueError:
            pass

    return len(failures) == 0, failures


def evaluate_action_alignment(
    insight: dict, context: dict, insight_type: str = "report"
) -> tuple[bool, list[str]]:
    """
    Check that the generated recommendation preserves the deterministic action.
    For report-level: recommended_action must reference keywords from recommended_report_action.
    For portfolio-level: priority_actions must reference deterministic action categories.
    """
    failures: list[str] = []

    if insight_type == "report":
        det_action = str(context.get("recommended_report_action", "")).lower()
        gen_action = str(insight.get("recommended_action", "")).lower()
        expected_kws = _ACTION_KEYWORD_MAP.get(det_action, set())
        if expected_kws and not any(kw in gen_action for kw in expected_kws):
            failures.append(
                f"action_mismatch:context={det_action} "
                f"required_keywords={expected_kws} "
                f"got='{gen_action[:60]}'"
            )
        return len(failures) == 0, failures

    # Portfolio: check priority_actions reference deterministic action counts
    det_actions = set(
        context.get("decision_support", {})
        .get("recommended_action_counts", {})
        .keys()
    )
    for action_text in insight.get("priority_actions", []):
        action_lower = str(action_text).lower()
        matched = any(
            any(kw in action_lower for kw in _ACTION_KEYWORD_MAP.get(da, {da}))
            for da in det_actions
        )
        if not matched:
            failures.append(f"unrecognised_portfolio_action:{action_text[:60]}")

    return len(failures) == 0, failures


def evaluate_evidence_disclosure(
    insight: dict, context: dict, insight_type: str = "report"
) -> tuple[bool, list[str]]:
    """
    Check that evidence limitations are disclosed when the context requires it.
    Hard failure when a required limitation is absent.
    """
    all_text_lower = _collect_text(insight).lower()
    failures: list[str] = []

    if insight_type == "report":
        # Model health insufficient → must mention
        if str(context.get("model_diagnostic_status", "")).lower() == "insufficient_evidence":
            if "insufficient" not in all_text_lower and "model" not in all_text_lower:
                failures.append("evidence_omission:model_health_insufficient")

        # Privacy suppression → must mention
        if str(context.get("privacy_suppression_status", "")).lower() == "suppressed":
            if "privac" not in all_text_lower and "suppress" not in all_text_lower:
                failures.append("evidence_omission:privacy_suppression")

        # High forecast uncertainty → must mention
        unc = str(context.get("forecast_uncertainty_status", "")).lower()
        if "high" in unc or "very_high" in unc:
            if "uncertain" not in all_text_lower:
                failures.append("evidence_omission:forecast_uncertainty")

    else:
        # Portfolio: model health insufficient → must mention limitation
        model_counts = context.get("model_health", {}).get("status_counts", {})
        if model_counts.get("insufficient_evidence", 0) > 0:
            if "insufficient" not in all_text_lower and "model" not in all_text_lower:
                failures.append("evidence_omission:portfolio_model_health")

        # Privacy suppression
        privacy_count = context.get("portfolio_evidence", {}).get("reports_with_privacy_suppression", 0)
        if privacy_count > 0:
            if "privac" not in all_text_lower and "suppress" not in all_text_lower:
                failures.append("evidence_omission:portfolio_privacy_suppression")

    return len(failures) == 0, failures


def evaluate_groundedness(
    insight: dict, context: dict, insight_type: str = "report"
) -> tuple[bool, list[str]]:
    """
    Aggregate groundedness: True iff both directional and numerical grounding pass.
    This wraps the two specific checks into one headline result.
    """
    dir_pass, dir_failures = evaluate_directional_consistency(insight, context, insight_type)
    num_pass, num_failures = evaluate_numerical_grounding(insight, context, insight_type)
    failures = dir_failures + num_failures
    return len(failures) == 0, failures


def evaluate_readability(
    insight: dict, insight_type: str = "report"
) -> tuple[float, list[str]]:
    """
    Heuristic readability score (0.0–1.0). NOT a comprehensive NLP measure.
    Checks: field word-count limits, no generic vacuous statements,
    no repeated sentences.

    Note: This is an approximation. Human review provides the authoritative
    readability assessment (see docs/genai_evaluation_rubric.md).
    """
    limits = (
        _REPORT_FIELD_WORD_LIMITS if insight_type == "report"
        else _PORTFOLIO_FIELD_WORD_LIMITS
    )
    required = (
        REQUIRED_INSIGHT_FIELDS if insight_type == "report"
        else REQUIRED_PORTFOLIO_FIELDS
    )

    issues: list[str] = []
    fields_checked = 0
    fields_passed = 0

    for fld in required:
        val = insight.get(fld)
        if val is None:
            continue
        items = [val] if isinstance(val, str) else (val if isinstance(val, list) else [])
        max_words = limits.get(fld, 80)

        for text in items:
            text = str(text)
            if not text.strip():
                continue
            fields_checked += 1
            field_issues: list[str] = []
            wc = _word_count(text)
            if wc > max_words:
                field_issues.append(f"{fld}:too_long({wc}>{max_words}_words)")
            if text.lower().strip() in _GENERIC_PHRASES:
                field_issues.append(f"{fld}:generic_vacuous")
            if not field_issues:
                fields_passed += 1
            issues.extend(field_issues)

    # Repeated-sentence check across all fields
    sentences = _collect_sentences(insight)
    seen: set[str] = set()
    for s in sentences:
        norm = re.sub(r"\s+", " ", s.lower().strip())
        if len(norm) < 10:  # ignore very short fragments
            continue
        if norm in seen:
            issues.append(f"repeated_sentence:'{norm[:60]}'")
        seen.add(norm)

    score = fields_passed / fields_checked if fields_checked > 0 else 0.0
    return score, issues


def evaluate_conciseness(
    insight: dict, insight_type: str = "report"
) -> tuple[float, list[str]]:
    """
    Heuristic conciseness score (0.0–1.0).
    Fields that greatly exceed word limits reduce the score.
    """
    limits = (
        _REPORT_FIELD_WORD_LIMITS if insight_type == "report"
        else _PORTFOLIO_FIELD_WORD_LIMITS
    )
    required = (
        REQUIRED_INSIGHT_FIELDS if insight_type == "report"
        else REQUIRED_PORTFOLIO_FIELDS
    )

    over_ratio_sum = 0.0
    count = 0

    for fld in required:
        val = insight.get(fld)
        if val is None:
            continue
        items = [val] if isinstance(val, str) else (val if isinstance(val, list) else [])
        max_words = limits.get(fld, 80)
        for text in items:
            text = str(text)
            if not text.strip():
                continue
            count += 1
            wc = _word_count(text)
            over_ratio_sum += min(wc / max_words, 2.0)  # cap at 2x

    if count == 0:
        return 1.0, []

    avg_ratio = over_ratio_sum / count
    # Score: 1.0 if avg is ≤ 1.0 (within limits), degrades toward 0 for longer outputs
    score = max(0.0, round(1.0 - max(0.0, avg_ratio - 1.0), 2))
    return score, []


# ── Top-level evaluators ───────────────────────────────────────────────────────

def evaluate_report_insight(
    insight: dict[str, Any],
    context: dict[str, Any],
    case_id: str = "unknown",
    prompt_version: str = REPORT_INSIGHT_PROMPT_VERSION,
    model_name: str = "unknown",
    generation_status: str = "unknown",
) -> EvaluationResult:
    """Run all evaluation dimensions for a single report-level insight."""
    failures: list[str] = []

    comp_pass,  comp_fail  = evaluate_completeness(insight, "report")
    safe_pass,  safe_fail  = evaluate_safety(insight, "report")
    dir_pass,   dir_fail   = evaluate_directional_consistency(insight, context, "report")
    num_pass,   num_fail   = evaluate_numerical_grounding(insight, context, "report")
    act_pass,   act_fail   = evaluate_action_alignment(insight, context, "report")
    evid_pass,  evid_fail  = evaluate_evidence_disclosure(insight, context, "report")
    ground_pass = dir_pass and num_pass
    read_score, read_fail  = evaluate_readability(insight, "report")
    conc_score, _          = evaluate_conciseness(insight, "report")

    failures = comp_fail + safe_fail + dir_fail + num_fail + act_fail + evid_fail + read_fail

    return EvaluationResult(
        case_id=case_id,
        insight_type="report",
        prompt_version=prompt_version,
        model_name=model_name,
        generation_status=generation_status,
        completeness_pass=comp_pass,
        safety_pass=safe_pass,
        groundedness_pass=ground_pass,
        direction_pass=dir_pass,
        numerical_pass=num_pass,
        action_alignment_pass=act_pass,
        evidence_disclosure_pass=evid_pass,
        readability_score=round(read_score, 3),
        conciseness_score=round(conc_score, 3),
        overall_pass=_overall_pass(
            comp_pass, safe_pass, ground_pass, dir_pass, num_pass,
            act_pass, evid_pass, read_score,
        ),
        failure_reasons=failures,
    )


def evaluate_portfolio_insight(
    insight: dict[str, Any],
    context: dict[str, Any],
    case_id: str = "unknown",
    prompt_version: str = PORTFOLIO_INSIGHT_PROMPT_VERSION,
    model_name: str = "unknown",
    generation_status: str = "unknown",
) -> EvaluationResult:
    """Run all evaluation dimensions for a portfolio-level insight."""
    comp_pass,  comp_fail  = evaluate_completeness(insight, "portfolio")
    safe_pass,  safe_fail  = evaluate_safety(insight, "portfolio")
    dir_pass,   dir_fail   = evaluate_directional_consistency(insight, context, "portfolio")
    num_pass,   num_fail   = evaluate_numerical_grounding(insight, context, "portfolio")
    act_pass,   act_fail   = evaluate_action_alignment(insight, context, "portfolio")
    evid_pass,  evid_fail  = evaluate_evidence_disclosure(insight, context, "portfolio")
    ground_pass = dir_pass and num_pass
    read_score, read_fail  = evaluate_readability(insight, "portfolio")
    conc_score, _          = evaluate_conciseness(insight, "portfolio")

    failures = comp_fail + safe_fail + dir_fail + num_fail + act_fail + evid_fail + read_fail

    return EvaluationResult(
        case_id=case_id,
        insight_type="portfolio",
        prompt_version=prompt_version,
        model_name=model_name,
        generation_status=generation_status,
        completeness_pass=comp_pass,
        safety_pass=safe_pass,
        groundedness_pass=ground_pass,
        direction_pass=dir_pass,
        numerical_pass=num_pass,
        action_alignment_pass=act_pass,
        evidence_disclosure_pass=evid_pass,
        readability_score=round(read_score, 3),
        conciseness_score=round(conc_score, 3),
        overall_pass=_overall_pass(
            comp_pass, safe_pass, ground_pass, dir_pass, num_pass,
            act_pass, evid_pass, read_score,
        ),
        failure_reasons=failures,
    )


# ── Fixture loading ────────────────────────────────────────────────────────────

def load_evaluation_cases(path: Path | None = None) -> list[dict]:
    """Load labelled evaluation cases from fixture JSON."""
    p = path or EVALUATION_CASES_FILE
    with open(p, encoding="utf-8") as fh:
        data = json.load(fh)
    return data["cases"]


def load_golden_outputs(path: Path | None = None) -> list[dict]:
    """Load golden output configs for regression testing."""
    p = path or GOLDEN_OUTPUTS_FILE
    with open(p, encoding="utf-8") as fh:
        data = json.load(fh)
    return data["golden_configs"]


# ── Golden regression comparison ──────────────────────────────────────────────

def compare_against_golden(
    insight: dict, golden: dict
) -> tuple[bool, list[str]]:
    """
    Compare a generated insight against a golden config WITHOUT requiring
    exact text equality. Checks:
      - required concepts present
      - prohibited concepts absent
      - schema fields present
      - evidence limitations keywords present (if specified)
    Returns (pass, failure_reasons).
    """
    all_text_lower = _collect_text(insight).lower()
    failures: list[str] = []

    for concept in golden.get("required_concepts", []):
        if concept.lower() not in all_text_lower:
            failures.append(f"missing_concept:{concept}")

    for concept in golden.get("prohibited_concepts", []):
        if concept.lower() in all_text_lower:
            failures.append(f"prohibited_concept:{concept}")

    for field_name in golden.get("schema_fields", []):
        if field_name not in insight:
            failures.append(f"missing_schema_field:{field_name}")

    for kw in golden.get("evidence_limitations_keywords", []):
        if kw.lower() not in all_text_lower:
            failures.append(f"missing_evidence_keyword:{kw}")

    expected_action = golden.get("expected_action")
    if expected_action:
        expected_kws = _ACTION_KEYWORD_MAP.get(expected_action, set())
        rec = str(insight.get("recommended_action", "")).lower()
        if expected_kws and not any(kw in rec for kw in expected_kws):
            failures.append(f"action_mismatch:expected={expected_action}")

    return len(failures) == 0, failures


# ── Save / report ──────────────────────────────────────────────────────────────

def save_evaluation_results(
    results: list[EvaluationResult],
    project_root: Path | None = None,
    label: str | None = None,
) -> dict[str, Path]:
    """
    Write evaluation results to CSV and summary JSON.
    Files are timestamped so prior-version results are not overwritten.
    Also writes fixed 'latest' copies for convenience.
    """
    root = project_root or Path(__file__).resolve().parents[2]
    out_dir = root / "outputs" / "evaluation"
    out_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tag = f"_{label}" if label else ""

    csv_path    = out_dir / f"genai_evaluation_results{tag}_{ts}.csv"
    json_path   = out_dir / f"genai_evaluation_summary{tag}_{ts}.json"
    csv_latest  = out_dir / "genai_evaluation_results.csv"
    json_latest = out_dir / "genai_evaluation_summary.json"

    # CSV
    if results:
        fieldnames = list(results[0].to_dict().keys())
        with open(csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            for r in results:
                writer.writerow(r.to_dict())
        csv_path.read_bytes()  # confirm written
        csv_latest.write_bytes(csv_path.read_bytes())

    # Summary
    n = len(results)
    n_report    = sum(1 for r in results if r.insight_type == "report")
    n_portfolio = sum(1 for r in results if r.insight_type == "portfolio")
    n_pass      = sum(1 for r in results if r.overall_pass)

    by_status: dict[str, dict] = {}
    for r in results:
        s = r.generation_status
        bucket = by_status.setdefault(s, {"count": 0, "pass": 0})
        bucket["count"] += 1
        if r.overall_pass:
            bucket["pass"] += 1

    dim_failures: dict[str, int] = {
        "completeness":        sum(1 for r in results if not r.completeness_pass),
        "safety":              sum(1 for r in results if not r.safety_pass),
        "groundedness":        sum(1 for r in results if not r.groundedness_pass),
        "direction":           sum(1 for r in results if not r.direction_pass),
        "numerical":           sum(1 for r in results if not r.numerical_pass),
        "action_alignment":    sum(1 for r in results if not r.action_alignment_pass),
        "evidence_disclosure": sum(1 for r in results if not r.evidence_disclosure_pass),
        "readability":         sum(1 for r in results if r.readability_score < 0.5),
    }

    # Gather unique prompt versions and model names
    prompt_versions = sorted({r.prompt_version for r in results})
    model_names     = sorted({r.model_name for r in results})

    summary = {
        "evaluated_at":          datetime.now(timezone.utc).isoformat(),
        "label":                 label,
        "cases_evaluated":       n,
        "overall_pass_rate":     round(n_pass / n, 3) if n else None,
        "report_cases":          n_report,
        "portfolio_cases":       n_portfolio,
        "by_generation_status":  by_status,
        "failure_count_by_dimension": dim_failures,
        "prompt_versions":       prompt_versions,
        "model_names":           model_names,
    }

    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    json_latest.write_bytes(json_path.read_bytes())

    return {"csv": csv_path, "json": json_path,
            "csv_latest": csv_latest, "json_latest": json_latest}


# ── Regression runner against stored outputs ──────────────────────────────────

def run_regression_against_stored_outputs(
    project_root: Path | None = None,
) -> dict[str, Any]:
    """
    Evaluate current stored insights against the mart context.
    Returns a summary dict and writes outputs/evaluation/ files.

    Note: the current outputs may be entirely rule_based because no API key
    has been configured in this environment. Rule-based evaluation confirms
    the fallback path is correct; it does NOT validate the live LLM path.
    """
    import pandas as pd
    from src.genai.insight_generator import build_mart_context

    root = project_root or Path(__file__).resolve().parents[2]
    mart_path    = root / "outputs" / "analytics" / "mart_report_analytics.csv"
    reports_path = root / "outputs" / "insights" / "report_ai_insights.json"
    portfolio_path = root / "outputs" / "insights" / "portfolio_ai_insight.json"

    results: list[EvaluationResult] = []

    # ── Report-level evaluation ───────────────────────────────────────────────
    if mart_path.exists() and reports_path.exists():
        mart_df = pd.read_csv(mart_path)
        contexts = {
            str(ctx["report_id"]): ctx
            for ctx in build_mart_context(mart_df)
        }
        stored_reports = json.loads(reports_path.read_text(encoding="utf-8"))

        for entry in stored_reports:
            rid = str(entry.get("report_id", ""))
            ctx = contexts.get(rid, {})
            result = evaluate_report_insight(
                insight=entry,
                context=ctx,
                case_id=f"stored_report_{rid}",
                prompt_version=entry.get("prompt_version", REPORT_INSIGHT_PROMPT_VERSION),
                model_name=entry.get("model_name", "unknown"),
                generation_status=entry.get("generation_status", "unknown"),
            )
            results.append(result)

    # ── Portfolio-level evaluation ────────────────────────────────────────────
    if mart_path.exists() and portfolio_path.exists():
        mart_df   = pd.read_csv(mart_path)
        port_ctx  = build_portfolio_context(mart_df)
        port_data = json.loads(portfolio_path.read_text(encoding="utf-8"))
        result    = evaluate_portfolio_insight(
            insight=port_data,
            context=port_ctx,
            case_id="stored_portfolio",
            prompt_version=port_data.get("prompt_version", PORTFOLIO_INSIGHT_PROMPT_VERSION),
            model_name=port_data.get("model_name", "unknown"),
            generation_status=port_data.get("generation_status", "unknown"),
        )
        results.append(result)

    paths = save_evaluation_results(results, root, label="stored_outputs")

    # Build human-readable summary
    n = len(results)
    by_status: dict[str, dict] = {}
    for r in results:
        s = r.generation_status
        b = by_status.setdefault(s, {"count": 0, "pass": 0, "fail": 0})
        b["count"] += 1
        if r.overall_pass:
            b["pass"] += 1
        else:
            b["fail"] += 1

    return {
        "cases_evaluated": n,
        "overall_pass_rate": round(sum(1 for r in results if r.overall_pass) / n, 3) if n else None,
        "by_generation_status": by_status,
        "output_paths": {k: str(v) for k, v in paths.items()},
        "evaluation_note": (
            "Rule-based evaluation confirms the fallback path produces valid, grounded outputs. "
            "It does NOT validate the live LLM generation path. LLM validation requires "
            "an API key and a separate run with generation_status='success'."
        ),
    }
