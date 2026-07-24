"""Rule-based user segmentation helpers.

Privacy note
------------
Outputs contain only ``user_key`` (surrogate identifier).
Direct identifiers (user_id, unique_user) are never included.
See src/analytics/privacy_policy.py.

Engagement definitions
----------------------
one_time segment: user has exactly 1 distinct active day (lifetime).
    CANONICAL: one_active_day_user_flag = (active_days == 1)
    DEPRECATED definition removed: OR (total_views == 1).
    See src/analytics/engagement_definitions.py.
"""

from __future__ import annotations

import pandas as pd


def _quantile(series: pd.Series, q: float) -> float:
    """Return a quantile while tolerating empty or all-null inputs."""
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return float("nan")
    return float(numeric.quantile(q))


def build_user_segments(user_features: pd.DataFrame) -> pd.DataFrame:
    """Assign business-friendly rule-based segments to users.

    Segment labels are ``power``, ``regular``, ``casual``, and ``one_time``.
    Quantiles are used because fixed business thresholds are not yet defined.

    Privacy: only user_key is included in the output — no direct identifiers.
    """
    if user_features is None or user_features.empty:
        return pd.DataFrame(
            columns=["user_key", "user_segment", "segment_reason"]
        )

    features = user_features.copy()

    # Only include user_key in the output — never user_id or unique_user.
    id_columns = ["user_key"] if "user_key" in features.columns else []

    total_views_q50 = _quantile(features["total_views"], 0.50)
    total_views_q75 = _quantile(features["total_views"], 0.75)
    distinct_reports_q50 = _quantile(features["distinct_reports"], 0.50)
    distinct_reports_q75 = _quantile(features["distinct_reports"], 0.75)

    rows = []
    for _, row in features.iterrows():
        total_views = row.get("total_views")
        active_days = row.get("active_days")
        distinct_reports = row.get("distinct_reports")

        # CANONICAL: one_active_day_user_flag — active on exactly 1 distinct date (lifetime).
        # DEPRECATED definition removed: OR (total_views == 1). See engagement_definitions.py.
        one_active_day = pd.notna(active_days) and active_days == 1

        power = (
            pd.notna(total_views)
            and pd.notna(distinct_reports)
            and total_views >= total_views_q75
            and distinct_reports >= distinct_reports_q75
        )
        regular = (
            pd.notna(active_days)
            and active_days > 1
            and (
                (pd.notna(total_views) and total_views >= total_views_q50)
                or (
                    pd.notna(distinct_reports)
                    and distinct_reports >= distinct_reports_q50
                )
            )
        )

        if one_active_day:
            segment = "one_time"
            reason = "User has exactly one active day."
        elif power:
            segment = "power"
            reason = "Total views and distinct reports are both in the top quartile."
        elif regular:
            segment = "regular"
            reason = "User has repeated activity and above-median views or report breadth."
        else:
            segment = "casual"
            reason = "User has some usage but does not meet regular or power thresholds."

        segment_row = {column: row.get(column) for column in id_columns}
        segment_row.update(
            {
                "user_segment": segment,
                "segment_reason": reason,
            }
        )
        rows.append(segment_row)

    output_columns = id_columns + ["user_segment", "segment_reason"]
    return pd.DataFrame(rows).reindex(columns=output_columns)
