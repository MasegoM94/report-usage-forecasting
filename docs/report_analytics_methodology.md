# Report Analytics Methodology

This document describes the methodological decisions governing the report analytics
pipeline: temporal alignment, evidence gating, deterministic precedence, privacy suppression,
the complete daily spine policy, and what is explicitly not inferred from usage.

---

## 1. Source-Derived `analytics_as_of_date` Policy

All outputs set `analytics_as_of_date` from `max(usage_date)` in the source mart
(mart_report_daily_series). This date is never derived from `date.today()` or wall-clock time.

**Why:** Using `date.today()` would make outputs non-reproducible and would silently shift
lookback windows (28d, 90d) between runs. Using the source max date makes every output
deterministic for a given input snapshot.

**Forecast alignment:** The forecast as-of date is set as `min(forecast_date) - 1 day` from
horizon files, aligning with the historical analytics_as_of_date. Misalignment is flagged in
`actual_forecast_alignment_status` and `temporal_alignment_status`.

---

## 2. Evidence-Aware Risk Gating

Each risk flag follows a three-step gate:

```
Source metric available?
    No  -> risk flag = null (not False, not "no_risk")
    Yes -> Evidence sufficient?
              No  -> risk flag = null
              Yes -> Threshold exceeded?
                        No  -> risk flag = False
                        Yes -> risk flag = True
```

A null risk flag means evidence was unavailable — not that the risk is absent.
Risk is only raised (True) when supporting, non-suppressed evidence is available.

Examples:
- `usage_decline_risk` is null if `history_sufficient_28d` is False
- `concentrated_dependency_risk` is null if privacy is suppressed — the diagnostics pipeline reads the upstream `privacy_suppression_status` flag from the engagement context; it does not apply its own user-count threshold
- `elevated_lapse_risk` is null if cohort window is insufficient (< 28 days)

---

## 3. Deterministic Precedence Lists

### 3a. Primary Diagnostic Precedence (14 steps)

1. no_valid_data
2. prolonged_inactivity
3. severe_historical_decline
4. expected_inactivity
5. severe_model_health_issue
6. elevated_lapse
7. active_user_decline
8. concentrated_dependency
9. high_forecast_uncertainty
10. declining_frequency
11. low_repeat_engagement
12. metadata_limitation
13. newly_launched_or_immature
14. none

### 3b. Primary Report Segment Precedence (15 steps)

1. data_quality_issue
2. inactive_report
3. planned_deprecation
4. declining_report
5. elevated_lapse
6. concentrated_dependency
7. model_review_needed
8. uncertain_forecast
9. low_repeat_usage
10. growing_report
11. healthy_broad_adoption
12. healthy_niche_adoption
13. mixed_signals
14. newly_launched
15. insufficient_evidence

---

## 4. Privacy Suppression

The report analytics pipeline does not apply privacy suppression itself. It receives
pre-suppressed values from the upstream user analytics pipeline and propagates them unchanged.

**Where suppression is applied — upstream user analytics modules:**

The following fields arrive null in the engagement context because they were suppressed upstream
when `active_user_count_28d` fell below each module's threshold:

| Field | Upstream module | Gate attribute | Threshold |
|---|---|---|---|
| `top_1_user_view_share_28d`, `top_3_users_view_share_28d`, `user_view_hhi_28d`, `effective_user_count_28d`, `effective_user_share_28d` | `src/analytics/user_concentration_metrics.py` | `ConcentrationMetricsConfig.MIN_USERS_FOR_CONCENTRATION_METRICS` | 5 |
| `returning_user_share_28d` and other activity share fields | `src/analytics/user_engagement_metrics.py` | `UserEngagementMetricsConfig.MIN_USERS_FOR_DISTRIBUTION_METRICS` | 5 |
| Cohort share fields (`retained_user_rate_28d`, `lapse_rate_28d`, etc.) | `src/analytics/user_engagement_cohorts.py` | `CohortConfig.MIN_USERS_FOR_COHORT_BREAKDOWN` | 5 |
| Frequency distribution fields | `src/analytics/user_frequency_metrics.py` | `FrequencyMetricsConfig.MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS` | 5 |

`concentration_direction` is derived inside `report_engagement_mart.py` from the concentration
outputs; when `concentration_status` is null or privacy_suppressed upstream,
`concentration_direction` is also unavailable.

**How the report analytics pipeline uses suppression state:**

- `privacy_suppression_status` and `privacy_suppressed_field_count` are carried unchanged from
  the upstream engagement context into `report_engagement_context.csv`
- `report_diagnostics.py` reads `privacy_suppression_status` from the engagement context
  (`src/analytics/report_diagnostics.py:474`) and returns no-risk for concentration flags
  when that status is in `PRIVACY_SUPPRESSED_STATUSES` — it applies no user-count threshold itself
- `concentrated_dependency_risk` and `increasing_dependency_risk` are never raised from
  suppressed metrics; this is enforced by the diagnostics pipeline reading the upstream flag

**Suppression sentinel rules:**

- Suppressed numeric fields are null — never zero
- `privacy_suppression_status` = "suppressed" or "partial_suppression" when any field was suppressed
- `privacy_suppressed_field_count` records the count of suppressed fields
- `repeat_engagement_status` returns the sentinel value `privacy_suppressed` (not null) when
  the upstream `activity_privacy_suppressed` flag is True (gated at unique_users_28d < 5)

---

## 5. Complete Daily Spine Policy

mart_report_daily_series maintains one row per active report per calendar date, zero-filled
for dates with no observed usage.

Implications:
- `recent_28d_views` is always a sum over the full 28-day window
- `zero_usage_days_28d` counts calendar days with zero views — not missing days
- Inactivity streaks are computed from consecutive zero-view calendar days on the complete spine

---

## 6. What Is Explicitly NOT Inferred from Usage

The following attributes are used only when explicitly present in dim_report.csv:

| Attribute | Not inferred from |
|---|---|
| expected_usage_cadence | Access frequency patterns |
| criticality_level | View volume or user count |
| report_category | Report name or usage behaviour |
| report_owner_team | Access patterns or workspace membership |
| certification_status | Data quality metrics |
| deprecation_status | Declining usage or inactivity |
| Business value | Usage volume, engagement, or segment |

When missing from dim_report, these default to "unknown" — never to an inferred value.

---

## 7. Prohibited Actions

Never produced by this pipeline: retire_report, delete_report, automatically_retrain,
change_selected_model, restrict_user, contact_specific_user.

See PROHIBITED_ACTIONS in src/analytics/report_diagnostics.py and
PROHIBITED_MART_ACTIONS in src/analytics/report_analytics_mart.py.
