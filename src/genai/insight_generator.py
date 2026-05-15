"""Generate batch report-level AI insights from existing CSV outputs.

The module is intentionally lightweight. It joins forecast, model performance,
segment, and diagnostic CSV outputs at report_id level, then writes structured
insights to JSON and Markdown. If ``OPENAI_API_KEY`` is not set, it uses a
deterministic rule-based fallback so demos and portfolio reviews still run.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from src.genai.prompts import REPORT_INSIGHT_SYSTEM_PROMPT, build_report_insight_prompt


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


def get_project_root() -> Path:
    """Return the repository root based on this module file location."""
    return Path(__file__).resolve().parents[2]


def _read_first_existing_csv(directory: Path, filenames: list[str]) -> pd.DataFrame:
    """Read the first matching CSV from a directory, or return an empty frame."""
    for filename in filenames:
        path = directory / filename
        if path.exists():
            return pd.read_csv(path)
    return pd.DataFrame()


def load_input_tables(project_root: Path | None = None) -> dict[str, pd.DataFrame]:
    """Load forecast, performance, segment, and diagnostic CSV outputs."""
    root = project_root or get_project_root()
    return {
        "forecasts": _read_first_existing_csv(root / "outputs" / "forecasts", FORECAST_INPUT_CANDIDATES),
        "metrics": _read_first_existing_csv(root / "outputs" / "metrics", METRICS_INPUT_CANDIDATES),
        "segments": _read_first_existing_csv(root / "outputs" / "segments", [SEGMENTS_INPUT]),
        "diagnostics": _read_first_existing_csv(root / "outputs" / "diagnostics", [DIAGNOSTICS_INPUT]),
    }


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


def summarize_forecasts(forecasts: pd.DataFrame) -> pd.DataFrame:
    """Summarize report-level forecast output to one row per report."""
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
    """Summarize model performance output to one row per report."""
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
        "report_id",
        "report_name",
        "selected_model",
        "model_name",
        "selected_mae",
        "mae",
        "selected_rmse",
        "rmse",
        "selected_wape",
        "wape",
        "forecast_reliable",
        "passes_data_criteria",
        "passes_model_criteria",
    ]
    return summary[[col for col in keep_cols if col in summary.columns]]


def build_report_contexts(tables: dict[str, pd.DataFrame]) -> list[dict[str, Any]]:
    """Join input tables at report_id level and return one context per report."""
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


def generate_rule_based_insight(report_context: dict[str, Any]) -> dict[str, Any]:
    """Generate deterministic placeholder insights without calling an AI API."""
    segment = str(report_context.get("report_segment") or "unsegmented")
    diagnostic = str(report_context.get("main_diagnostic") or "healthy_or_monitor")
    reliable = _to_bool(report_context.get("forecast_reliable"))
    avg_forecast = report_context.get("avg_forecast")

    if diagnostic == "inactive_risk" or segment == "inactive":
        health_status = "at_risk"
    elif diagnostic in {"performance_issue", "engagement_issue"} or segment == "at_risk":
        health_status = "watch"
    elif segment == "high_value":
        health_status = "healthy_high_value"
    else:
        health_status = "monitor"

    if avg_forecast is not None:
        forecast_summary = f"Average forecast demand is about {float(avg_forecast):.1f} views per period."
    else:
        forecast_summary = "Forecast output is not available for this report yet."

    key_drivers = [
        f"Segment: {segment}.",
        f"Primary diagnostic: {diagnostic}.",
        "Forecast reliability flag is positive." if reliable else "Forecast reliability is missing or false.",
    ]
    if report_context.get("diagnostic_summary"):
        key_drivers.append(str(report_context["diagnostic_summary"]))

    return {
        "report_id": report_context.get("report_id"),
        "report_name": report_context.get("report_name") or report_context.get("report_id"),
        "health_status": health_status,
        "forecast_summary": forecast_summary,
        "key_drivers": key_drivers,
        "hypotheses": [
            "Recent usage patterns may reflect a business cycle, reporting deadline, or stakeholder adoption change.",
            "Performance, engagement, or dependency signals should be validated with report owners before action.",
        ],
        "recommended_actions": [
            "Review forecast and diagnostic flags with the report owner.",
            "Check whether recent usage changes align with known business events.",
            "Monitor this report in the next forecast refresh.",
        ],
        "confidence": "medium" if reliable else "low",
        "generation_mode": "rule_based_fallback",
    }


def _call_openai_responses_api(
    report_context: dict[str, Any],
    model: str,
    api_key: str,
) -> dict[str, Any]:
    """Call OpenAI's Responses API and parse a JSON insight response."""
    payload = {
        "model": model,
        "input": [
            {"role": "system", "content": REPORT_INSIGHT_SYSTEM_PROMPT},
            {"role": "user", "content": build_report_insight_prompt(report_context)},
        ],
        "temperature": 0.2,
    }
    response = requests.post(
        "https://api.openai.com/v1/responses",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()
    text = data.get("output_text")
    if not text:
        output = data.get("output", [])
        text_parts = []
        for item in output:
            for content in item.get("content", []):
                if content.get("type") in {"output_text", "text"}:
                    text_parts.append(content.get("text", ""))
        text = "\n".join(text_parts).strip()

    insight = json.loads(text)
    insight["report_id"] = report_context.get("report_id")
    insight["report_name"] = report_context.get("report_name") or report_context.get("report_id")
    insight["generation_mode"] = "openai"
    return insight


def generate_ai_insight(
    report_context: dict[str, Any],
    model: str = DEFAULT_MODEL,
    api_key: str | None = None,
) -> dict[str, Any]:
    """Generate one report insight using OpenAI when configured, else fallback."""
    resolved_api_key = api_key or os.getenv("OPENAI_API_KEY")
    if not resolved_api_key:
        return generate_rule_based_insight(report_context)

    try:
        return _call_openai_responses_api(report_context, model, resolved_api_key)
    except Exception as exc:
        insight = generate_rule_based_insight(report_context)
        insight["generation_mode"] = "rule_based_fallback_after_api_error"
        insight["api_error"] = str(exc)
        return insight


def generate_report_insights(
    project_root: Path | None = None,
    model: str = DEFAULT_MODEL,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Load input CSVs, build contexts, and generate report-level insights."""
    tables = load_input_tables(project_root)
    contexts = build_report_contexts(tables)
    if limit is not None:
        contexts = contexts[:limit]
    return [generate_ai_insight(context, model=model) for context in contexts]


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
        lines.extend(
            [
                f"## {title}",
                "",
                f"- Report ID: {insight.get('report_id')}",
                f"- Health status: {insight.get('health_status')}",
                f"- Confidence: {insight.get('confidence')}",
                f"- Generation mode: {insight.get('generation_mode')}",
                "",
                f"**Forecast summary:** {insight.get('forecast_summary')}",
                "",
                "**Key drivers**",
            ]
        )
        lines.extend(f"- {item}" for item in insight.get("key_drivers", []))
        lines.extend(["", "**Hypotheses**"])
        lines.extend(f"- {item}" for item in insight.get("hypotheses", []))
        lines.extend(["", "**Recommended actions**"])
        lines.extend(f"- {item}" for item in insight.get("recommended_actions", []))
        lines.append("")
    return "\n".join(lines)


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
