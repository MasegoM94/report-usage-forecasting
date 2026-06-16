"""Report health diagnostic rule helpers."""

from __future__ import annotations

import numpy as np
import pandas as pd


def _quantile(series: pd.Series, q: float) -> float:
    """Return a quantile while tolerating empty or all-null inputs."""
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return float("nan")
    return float(numeric.quantile(q))


def build_report_diagnostics(
    report_features: pd.DataFrame,
    report_segments: pd.DataFrame,
) -> pd.DataFrame:
    """Apply simple health diagnostic flags to segmented report features."""
    if report_features is None or report_features.empty:
        return pd.DataFrame(
            columns=[
                "report_id",
                "report_name",
                "report_segment",
                "performance_issue",
                "engagement_issue",
                "dependency_risk",
                "inactive_risk",
                "main_diagnostic",
                "diagnostic_summary",
            ]
        )

    diagnostics = report_features.copy()
    if report_segments is not None and not report_segments.empty:
        diagnostics = diagnostics.merge(
            report_segments[["report_id", "report_segment"]],
            on="report_id",
            how="left",
        )
    else:
        diagnostics["report_segment"] = np.nan

    avg_load_q75 = _quantile(diagnostics["avg_load_time"], 0.75)
    p90_load_q75 = _quantile(diagnostics["p90_load_time"], 0.75)
    repeat_q25 = _quantile(diagnostics["repeat_rate"], 0.25)
    concentration_q75 = _quantile(diagnostics["top_user_concentration"], 0.75)
    concentration_threshold = max(0.50, concentration_q75)

    high_load_time = (
        diagnostics["avg_load_time"].ge(avg_load_q75)
        | diagnostics["p90_load_time"].ge(p90_load_q75)
    )
    declining_usage = diagnostics["usage_change_pct"].lt(0)

    diagnostics["performance_issue"] = high_load_time & declining_usage
    diagnostics["engagement_issue"] = (
        diagnostics["repeat_rate"].lt(repeat_q25) & declining_usage
    )
    diagnostics["dependency_risk"] = diagnostics["top_user_concentration"].ge(
        concentration_threshold
    )
    diagnostics["inactive_risk"] = diagnostics["report_segment"].eq("inactive")

    def _main_diagnostic(row: pd.Series) -> str:
        if row["inactive_risk"]:
            return "inactive_risk"
        if row["performance_issue"]:
            return "performance_issue"
        if row["engagement_issue"]:
            return "engagement_issue"
        if row["dependency_risk"]:
            return "dependency_risk"
        return "healthy_or_monitor"

    diagnostics["main_diagnostic"] = diagnostics.apply(_main_diagnostic, axis=1)

    flag_messages = {
        "inactive_risk": "Report appears inactive based on latest usage or active days.",
        "performance_issue": "Usage is declining while load times are relatively high.",
        "engagement_issue": "Repeat usage is relatively low, suggesting weak ongoing engagement.",
        "dependency_risk": "A large share of views comes from a very small number of users.",
    }

    def _build_summary(row: pd.Series) -> str:
        messages = [msg for flag, msg in flag_messages.items() if row.get(flag)]
        if not messages:
            return "No major diagnostic rule was triggered; continue monitoring."
        return " ".join(messages)

    diagnostics["diagnostic_summary"] = diagnostics.apply(_build_summary, axis=1)

    output_columns = [
        "report_id",
        "report_name",
        "report_segment",
        "performance_issue",
        "engagement_issue",
        "dependency_risk",
        "inactive_risk",
        "main_diagnostic",
        "diagnostic_summary",
    ]
    return diagnostics.reindex(columns=output_columns).sort_values("report_id").reset_index(drop=True)
