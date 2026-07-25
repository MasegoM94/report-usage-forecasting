# Canonical Field Names — Sprint 7

## Report Features

| Canonical | Deprecated | Definition |
|---|---|---|
| `recent_28d_views` | `latest_views` | Total views in the 28 days ending analytics_as_of_date |
| `previous_28d_views` | `prior_views` | Total views in the 28-day window before the recent window |
| `usage_change_28d_pct` | `usage_change_pct` | (recent - previous) / previous × 100 |
| `top_1_user_view_share` | `top_user_concentration` | Single-user share of lifetime views |

## Engagement Mart

| Canonical | Deprecated | Notes |
|---|---|---|
| `returning_user_share_28d` | `repeat_rate` | Returning = active on ≥2 distinct dates. repeat_rate was view-count-based. |

## Model Diagnostics

| Field | Definition |
|---|---|
| `report_model_diagnostics.csv` | Canonical latest replaceable output |

Deprecated: `report_model_diagnostics_latest.csv` — if present, treat as stale duplicate.
