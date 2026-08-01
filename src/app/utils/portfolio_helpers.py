"""Pure-logic helpers for the portfolio overview tab.

These functions contain no Streamlit calls and can be tested independently.
All inputs and outputs are plain Python types (dicts, DataFrames, strings).
"""

from __future__ import annotations

from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Status label mappings
# ---------------------------------------------------------------------------

STATUS_LABELS: dict[str, str] = {
    # historical_usage_status
    "growing_usage":              "Growing",
    "stable_regular_usage":       "Stable (regular)",
    "stable_intermittent_usage":  "Stable (intermittent)",
    "bursty_usage":               "Bursty",
    "declining_usage":            "Declining",
    "prolonged_inactivity":       "Inactive",
    # forecast_outlook_status
    "growth_expected":            "Growth expected",
    "stable_outlook":             "Stable outlook",
    "reactivation_expected":      "Reactivation expected",
    "uncertain_outlook":          "Uncertain",
    "decline_expected":           "Decline expected",
    # overall_engagement_status
    "healthy_broad_adoption":     "Healthy – broad",
    "healthy_niche_adoption":     "Healthy – niche",
    "growing_adoption":           "Growing",
    "declining_adoption":         "Declining",
    "elevated_lapse":             "Elevated lapse",
    "inactive":                   "Inactive",
    # model_diagnostic_status
    "healthy":                    "Healthy",
    "sufficient_evidence":        "Sufficient evidence",
    "insufficient_evidence":      "Insufficient evidence",
    "degraded":                   "Degraded",
    "failing":                    "Failing",
    # overall_review_priority
    "low":                        "Low",
    "medium":                     "Medium",
    "high":                       "High",
    "critical":                   "Critical",
    # recommended_report_action
    "continue_monitoring":        "Continue monitoring",
    "investigate_usage_decline":  "Investigate decline",
    "review_planned_deprecation": "Review deprecation",
    "review_forecast_uncertainty":"Review uncertainty",
    "review_model_health":        "Review model health",
}

STATUS_ORDER: dict[str, list[str]] = {
    "historical_usage_status": [
        "growing_usage", "stable_regular_usage", "stable_intermittent_usage",
        "bursty_usage", "declining_usage", "prolonged_inactivity",
    ],
    "forecast_outlook_status": [
        "growth_expected", "reactivation_expected", "stable_outlook",
        "uncertain_outlook", "decline_expected",
    ],
    "overall_engagement_status": [
        "healthy_broad_adoption", "healthy_niche_adoption", "growing_adoption",
        "elevated_lapse", "declining_adoption", "inactive",
    ],
    "model_diagnostic_status": [
        "healthy", "sufficient_evidence", "insufficient_evidence",
        "degraded", "failing",
    ],
    "overall_review_priority": ["low", "medium", "high", "critical"],
    "recommended_report_action": [
        "continue_monitoring", "investigate_usage_decline",
        "review_planned_deprecation", "review_forecast_uncertainty",
        "review_model_health",
    ],
}


def status_label(code: str) -> str:
    """Return a human-readable label for a pipeline status code."""
    return STATUS_LABELS.get(str(code), str(code).replace("_", " ").title())


# ---------------------------------------------------------------------------
# Distribution table
# ---------------------------------------------------------------------------

def distribution_table(
    mart: pd.DataFrame, column: str, order: list[str] | None = None
) -> pd.DataFrame:
    """Return a tidy (Status, Count, Share %) DataFrame for a status column.

    When *order* is provided, categories appear in that order.
    Unknown categories are appended after the ordered set.
    Returns an empty DataFrame when the column is absent or the mart is empty.
    """
    if mart.empty or column not in mart.columns:
        return pd.DataFrame(columns=["Status", "Count", "Share %"])
    counts = mart[column].value_counts()
    total = int(counts.sum())
    if order:
        all_cats = order + [c for c in counts.index if c not in order]
        counts = counts.reindex(all_cats).dropna().astype(int)
    rows = [
        {
            "Status": status_label(cat),
            "Count": int(cnt),
            "Share %": f"{100 * cnt / total:.0f}%" if total else "—",
        }
        for cat, cnt in counts.items()
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Headline metrics
# ---------------------------------------------------------------------------

def portfolio_headline_metrics(mart: pd.DataFrame) -> dict[str, Any]:
    """Derive all portfolio headline metric values from the canonical mart.

    Returns a dict with numeric values (or None) for each metric.
    All values are simple row counts from validated mart status fields.
    No upstream threshold rules are reproduced here.

    Keys:
      total_reports         — unique report count
      with_recent_usage     — reports with recent_28d_views > 0
      requiring_review      — reports where action != continue_monitoring
      high_priority         — reports with priority high or critical
      privacy_suppressed    — reports with privacy_suppression_status == suppressed
      analytics_run_id      — mart lineage
      analytics_as_of_date  — mart freshness date
    """
    _none: dict[str, Any] = {
        "total_reports": None, "with_recent_usage": None, "requiring_review": None,
        "high_priority": None, "privacy_suppressed": None,
        "analytics_run_id": None, "analytics_as_of_date": None,
    }
    if mart.empty or "report_id" not in mart.columns:
        return _none

    total = int(mart["report_id"].nunique())
    out: dict[str, Any] = {"total_reports": total}

    if "recent_28d_views" in mart.columns:
        out["with_recent_usage"] = int(
            (pd.to_numeric(mart["recent_28d_views"], errors="coerce").fillna(0) > 0).sum()
        )
    else:
        out["with_recent_usage"] = None

    if "recommended_report_action" in mart.columns:
        out["requiring_review"] = int(
            (mart["recommended_report_action"] != "continue_monitoring").sum()
        )
    else:
        out["requiring_review"] = None

    if "overall_review_priority" in mart.columns:
        out["high_priority"] = int(
            mart["overall_review_priority"].isin({"high", "critical"}).sum()
        )
    else:
        out["high_priority"] = None

    if "privacy_suppression_status" in mart.columns:
        out["privacy_suppressed"] = int(
            (mart["privacy_suppression_status"] == "suppressed").sum()
        )
    else:
        out["privacy_suppressed"] = None

    out["analytics_run_id"] = (
        mart["analytics_run_id"].iloc[0] if "analytics_run_id" in mart.columns else None
    )
    out["analytics_as_of_date"] = (
        mart["analytics_as_of_date"].iloc[0] if "analytics_as_of_date" in mart.columns else None
    )
    return out


# ---------------------------------------------------------------------------
# Attention shortlist
# ---------------------------------------------------------------------------

def attention_shortlist(mart: pd.DataFrame, cap: int = 5) -> pd.DataFrame:
    """Return the deterministic attention shortlist from the mart.

    Selects actionable reports (action != continue_monitoring), sorts by
    priority (high/critical first) then alphabetically by report_id within
    each tier, and caps at *cap* rows.

    No scores are computed. The sort is a deterministic function of the mart's
    own validated fields. Re-ranking in Streamlit is intentionally avoided.
    """
    required = {"report_id", "overall_review_priority", "recommended_report_action"}
    if mart.empty or not required.issubset(mart.columns):
        return pd.DataFrame()

    priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    df = mart.copy()
    df["_priority_rank"] = df["overall_review_priority"].map(priority_order).fillna(9)

    actionable = df[df["recommended_report_action"] != "continue_monitoring"].copy()
    if actionable.empty:
        return pd.DataFrame()

    actionable = (
        actionable
        .sort_values(["_priority_rank", "report_id"], ascending=[True, True])
        .head(cap)
    )

    display_cols = [
        c for c in [
            "report_id", "report_name", "overall_review_priority",
            "overall_report_status", "primary_diagnostic",
            "recommended_report_action", "overall_evidence_status",
        ]
        if c in actionable.columns
    ]
    return actionable[display_cols].reset_index(drop=True)
