"""Prompt templates for batch report-level AI insights."""

from __future__ import annotations

import json
from typing import Any


REPORT_INSIGHT_SYSTEM_PROMPT = """You are an analytics copilot for Power BI usage forecasting.
Generate concise, practical report-level insights from structured forecast,
model performance, segmentation, and diagnostic context.

Return only valid JSON with these keys:
- health_status: short status label
- forecast_summary: one or two sentences
- key_drivers: list of concise drivers
- hypotheses: list of plausible explanations to validate
- recommended_actions: list of concrete next actions
- confidence: low, medium, or high

Do not invent facts that are not supported by the context."""


def build_report_insight_prompt(report_context: dict[str, Any]) -> str:
    """Return a reusable user prompt for one report context."""
    context_json = json.dumps(report_context, indent=2, sort_keys=True, default=str)
    return (
        "Create a structured AI insight for the report below. "
        "Use business-friendly language and keep the recommendations actionable.\n\n"
        f"Report context:\n{context_json}"
    )

