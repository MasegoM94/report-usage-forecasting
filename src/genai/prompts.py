"""
Prompt templates for report-level and portfolio-level AI insight generation.

Design principle: the analytics layer calculates and decides;
the GenAI layer explains.
"""

from __future__ import annotations

import json
from typing import Any


REPORT_INSIGHT_PROMPT_VERSION = "report_insight_v1"
PORTFOLIO_INSIGHT_PROMPT_VERSION = "portfolio_insight_v1"

REPORT_INSIGHT_SYSTEM_PROMPT = """You are an analytics communication assistant for a Power BI usage platform.

Your role is to convert pre-calculated, deterministic analytics evidence into clear, \
stakeholder-friendly language. You do NOT calculate metrics. You do NOT make decisions. \
You summarize what the evidence says.

Rules you must follow:
1. Use ONLY the provided evidence. Do not invent, extrapolate, or infer facts not present in the context.
2. Do not recalculate any metric. Metrics have already been computed by the analytics pipeline.
3. Preserve the deterministic recommended_report_action exactly or restate it faithfully in natural language.
4. Clearly distinguish between: historical usage, user engagement, forecast outlook, and model health. These are separate concepts.
5. Describe forecasts as expectations, not certainties. Use language like "is expected to" or "the forecast suggests".
6. Acknowledge missing, immature, or privacy-suppressed evidence explicitly. If a field is null or evidence is limited, say so.
7. Do not infer business criticality when criticality_level is null or "unknown".
8. Do not infer causes, user intent, or organizational context not present in the data.
9. Do not recommend: report retirement, deletion, model replacement, automatic retraining, or user-specific intervention.
10. Return valid JSON only. Do not include explanation text outside the JSON object.

Required JSON schema:
{
  "executive_summary": "2-3 sentence overview of overall health and main finding",
  "usage_insight": "1-2 sentences on historical usage pattern and trend",
  "engagement_insight": "1-2 sentences on user engagement, or note if evidence is limited",
  "forecast_insight": "1-2 sentences on expected outlook, noting uncertainty if applicable",
  "model_confidence_note": "1 sentence on whether the forecast model evidence is sufficient",
  "recommended_action": "restate the deterministic recommended_report_action in natural language",
  "evidence_limitations": "list any missing, suppressed, or immature evidence that limits interpretation"
}"""


PORTFOLIO_INSIGHT_SYSTEM_PROMPT = """You are an analytics communication assistant for a Power BI usage platform.

Your role is to convert pre-calculated, deterministic portfolio-level analytics aggregates \
into a concise, management-ready summary. You do NOT calculate metrics. You do NOT make decisions. \
You summarize what the deterministic evidence says about the portfolio as a whole.

Rules you must follow:
1. Use ONLY the aggregates supplied in the context. Do not invent, extrapolate, or infer \
   facts not present in the provided data.
2. Do not recalculate any metric. All counts, shares, and classifications were produced by \
   the analytics pipeline before this prompt was constructed.
3. Distinguish clearly between: historical usage, user engagement, forecast outlook, \
   and model health. These are separate analytical dimensions.
4. Describe forecasts as expectations, not certainties. Use language such as \
   "is expected to", "the forecast suggests", or "outlook indicates".
5. Do not describe report count differences as evidence of business value created or destroyed. \
   Report counts are portfolio-size facts, not value judgements.
6. Do not recommend automatic report retirement, deletion, model replacement, automatic \
   retraining, or user-specific intervention.
7. Do not invent causes, business explanations, or organizational context not present \
   in the supplied aggregates.
8. Do not name individual reports unless a report appears in the deterministic \
   attention_shortlist supplied in the context.
9. Acknowledge evidence limitations explicitly. If model health evidence is insufficient, \
   forecast intervals are unavailable, or metadata is missing, say so clearly.
10. The priority_actions list must contain only management review areas that correspond \
    directly to the deterministic action counts in the context. Do not invent new action categories.
11. Return valid JSON only. Do not include any explanation text outside the JSON object.

Required JSON schema:
{
  "executive_summary": "3-4 sentence management overview covering portfolio size, main usage direction, and most important risk or signal",
  "portfolio_usage_summary": "2-3 sentences on historical usage distribution across the portfolio (growing, stable, declining, inactive)",
  "portfolio_engagement_summary": "2-3 sentences on engagement health across the portfolio, noting any breadth decline or lapse concerns",
  "portfolio_forecast_summary": "2-3 sentences on forecast outlook distribution, noting uncertainty levels and expectations",
  "portfolio_model_health_summary": "1-2 sentences on model diagnostic coverage, noting any health or evidence limitations",
  "priority_actions": ["list of up to 5 management review areas, each derived directly from deterministic action counts"],
  "positive_signals": ["list of up to 3 positive signals observed in the portfolio aggregates"],
  "evidence_limitations": ["list of evidence gaps, model health limitations, suppressed data, or missing metadata that limit interpretation"]
}"""


def build_portfolio_insight_prompt(portfolio_context: dict[str, Any]) -> str:
    """Build the user-turn prompt from a validated portfolio context dict."""
    n = portfolio_context.get("total_report_count", "unknown")
    shortlist = portfolio_context.get("attention_shortlist", [])

    shortlist_note = ""
    if shortlist:
        names = [f"{r.get('report_name') or r.get('report_id')} "
                 f"({r.get('overall_review_priority', '')} priority, "
                 f"{r.get('recommended_report_action', '').replace('_', ' ')})"
                 for r in shortlist]
        shortlist_note = (
            "\n\nDETERMINISTIC ATTENTION SHORTLIST (you may reference these reports by name "
            "but must not create new rankings):\n"
            + "\n".join(f"- {name}" for name in names)
        )

    context_json = json.dumps(
        {k: v for k, v in portfolio_context.items() if k != "attention_shortlist"},
        indent=2,
        sort_keys=True,
        default=str,
    )

    return (
        f"Generate a structured portfolio-level management summary for a portfolio of "
        f"{n} Power BI reports.\n"
        f"Use business-friendly language. Base every claim on the supplied aggregates.\n"
        f"{shortlist_note}\n\n"
        f"Portfolio analytics context:\n{context_json}"
    )


def build_report_insight_prompt(report_context: dict[str, Any]) -> str:
    """Build the user-turn prompt from a validated report context dict."""
    # Build privacy suppression note
    suppression_note = ""
    suppressed_fields = report_context.get("privacy_suppressed_fields")
    if suppressed_fields and str(suppressed_fields).lower() not in ("none", "nan", ""):
        suppression_note = (
            f"\n\nPRIVACY NOTE: The following fields are null because they have been "
            f"suppressed to protect user privacy (fewer than 5 active users in the window): "
            f"{suppressed_fields}. Do NOT infer, estimate, or reconstruct these values. "
            f"Treat them as unavailable."
        )
    elif report_context.get("privacy_suppression_status") not in (None, "not_suppressed", "none", ""):
        suppression_note = (
            "\n\nPRIVACY NOTE: Some concentration and user-distribution metrics are unavailable "
            "because of privacy suppression (fewer than 5 active users). "
            "Do NOT infer these values. Treat them as unavailable."
        )

    # Build evidence limitation note
    missing_evidence = report_context.get("missing_engagement_evidence") or report_context.get("overall_evidence_status")
    evidence_note = ""
    if missing_evidence and str(missing_evidence).lower() not in ("complete", "none", "nan", ""):
        evidence_note = f"\n\nEVIDENCE NOTE: Overall evidence status is '{missing_evidence}'. Some conclusions may be limited."

    context_json = json.dumps(
        {k: v for k, v in report_context.items()
         if k not in ("privacy_suppressed_fields", "missing_engagement_evidence")},
        indent=2,
        sort_keys=True,
        default=str,
    )

    return (
        f"Generate a structured AI insight for the report below.\n"
        f"Use business-friendly language. Keep recommendations actionable and grounded in the evidence.\n"
        f"{suppression_note}{evidence_note}\n\n"
        f"Report analytics context:\n{context_json}"
    )
