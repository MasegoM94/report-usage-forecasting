"""Rule-based report segmentation helpers."""

from __future__ import annotations

import pandas as pd


def _quantile(series: pd.Series, q: float) -> float:
    """Return a quantile while tolerating empty or all-null inputs."""
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return float("nan")
    return float(numeric.quantile(q))


def build_report_segments(report_features: pd.DataFrame) -> pd.DataFrame:
    """Assign business-friendly rule-based segments to reports.

    Segment labels are ``high_value``, ``niche``, ``at_risk``, and ``inactive``.
    Quantiles are used where fixed business thresholds are not available.
    """
    if report_features is None or report_features.empty:
        return pd.DataFrame(
            columns=["report_id", "report_name", "report_segment", "segment_reason"]
        )

    features = report_features.copy()
    avg_views_q75 = _quantile(features["avg_views"], 0.75)
    users_q75 = _quantile(features["unique_users"], 0.75)
    repeat_q25 = _quantile(features["repeat_rate"], 0.25)
    repeat_q50 = _quantile(features["repeat_rate"], 0.50)
    concentration_q75 = _quantile(features["top_user_concentration"], 0.75)
    avg_load_q75 = _quantile(features["avg_load_time"], 0.75)
    p90_load_q75 = _quantile(features["p90_load_time"], 0.75)

    rows = []
    for _, row in features.iterrows():
        latest_views = row.get("latest_views")
        days_active = row.get("days_active")
        usage_change_pct = row.get("usage_change_pct")
        repeat_rate = row.get("repeat_rate")
        top_user_concentration = row.get("top_user_concentration")
        avg_load_time = row.get("avg_load_time")
        p90_load_time = row.get("p90_load_time")

        inactive = (pd.notna(days_active) and days_active <= 1) or (
            pd.notna(latest_views) and latest_views <= 0
        )
        high_value = (
            pd.notna(row.get("avg_views"))
            and pd.notna(row.get("unique_users"))
            and row["avg_views"] >= avg_views_q75
            and row["unique_users"] >= users_q75
        )
        poor_performance = (
            (pd.notna(avg_load_time) and avg_load_time >= avg_load_q75)
            or (pd.notna(p90_load_time) and p90_load_time >= p90_load_q75)
        )
        at_risk = (
            pd.notna(usage_change_pct)
            and usage_change_pct < 0
            and (
                (pd.notna(repeat_rate) and repeat_rate < repeat_q25)
                or poor_performance
            )
        )
        niche = (
            (pd.notna(repeat_rate) and repeat_rate >= repeat_q50)
            or (
                pd.notna(top_user_concentration)
                and top_user_concentration >= concentration_q75
            )
        )

        if inactive:
            segment = "inactive"
            reason = "Latest usage is zero or the report has very few active days."
        elif high_value:
            segment = "high_value"
            reason = "Average views and unique users are both in the top quartile."
        elif at_risk:
            segment = "at_risk"
            reason = "Usage is declining with weak repeat usage or slower performance."
        elif niche:
            segment = "niche"
            reason = "Usage is specialized, with healthy repeat behaviour or concentration."
        else:
            segment = "niche"
            reason = "Usage is moderate or low without a clear health warning."

        rows.append(
            {
                "report_id": row.get("report_id"),
                "report_name": row.get("report_name"),
                "report_segment": segment,
                "segment_reason": reason,
            }
        )

    return pd.DataFrame(rows)
