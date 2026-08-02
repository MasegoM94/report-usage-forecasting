# Data Dictionary — User Analytics Outputs

This document describes all nine output files produced by the user analytics pipeline.
All outputs are **report-level**. No file contains user keys, email addresses, display names,
or any other direct identifier. `user_key` (a pseudonymous surrogate) appears only in
`mart_report_user_daily.csv` and is excluded from all downstream aggregated outputs.

---

## 1. `outputs/analytics/mart_report_user_daily.csv`

**Purpose**: Canonical report-user-day mart. Foundational input for all user analytics engagement metrics.

**Grain**: one row per (analytics_run_id, report_id, user_key, usage_date) with positive usage.

**Unique key**: (analytics_run_id, report_id, user_key, usage_date)

**Privacy classification**: Contains `user_key` (pseudonymous surrogate). Must not be joined to
`dim_user.csv`. Must not expose `user_key` values in any downstream output or UI.

| Column | Type | Description |
|--------|------|-------------|
| analytics_run_id | string | UUID identifying the analytics run that produced this row |
| generated_at | timestamp | UTC timestamp when the row was generated |
| report_id | string | Unique report identifier |
| user_key | string | Pseudonymous user surrogate (e.g. UK_0001). Never a direct identifier |
| usage_date | date | Calendar date of usage |
| daily_views | integer | Total positive-view events for this user-report-date |
| active_user_day | boolean | Always True for rows in this mart (positive usage only) |
| source_event_count | integer | Number of source events contributing to this aggregated row |
| duplicate_event_count_removed | integer | Exact duplicate events removed before aggregation |
| first_report_use_date | date | Earliest date this user_key was seen for this report_id (lifetime) |
| latest_report_use_date | date | Most recent date this user_key was seen for this report_id (lifetime) |
| report_user_tenure_days | integer | Days from first_report_use_date to usage_date |
| first_use_flag | boolean | True if this row represents the user's first ever date for this report |
| lifetime_returned_flag | boolean | True if user_key has activity on any date after their first_report_use_date |
| user_identifier_status | string | Validation status of the user_key (valid, missing, invalid, prohibited) |
| source_record_valid | boolean | True if the source event passed all quality checks |
| record_valid | boolean | Composite validity flag; rows with False should not enter metric computation |
| source_file | string | Source file name (for audit trail) |
| source_row_count | integer | Row count of the source file at ingestion time |

**Null vs zero policy**: Rows exist only for (report, user, date) triples with positive usage.
Zero-view dates are not represented. `daily_views` is always > 0 in this mart.

**Suppression fields**: None. Suppression is applied in downstream aggregated outputs.

---

## 2. `outputs/analytics/report_user_data_quality.csv`

**Purpose**: Per-report data quality summary. One row per report_id, showing how many source events
were valid, excluded, and why.

**Grain**: one row per (analytics_run_id, report_id).

**Unique key**: (analytics_run_id, report_id)

**Privacy classification**: Report-level only. No user_key or direct identifiers.

| Column | Type | Description |
|--------|------|-------------|
| analytics_run_id | string | UUID for this analytics run |
| generated_at | timestamp | UTC generation timestamp |
| report_id | string | Unique report identifier |
| report_name | string | Human-readable report name |
| source_event_count | integer | Total source events for this report |
| valid_user_event_count | integer | Events with a valid user_key |
| valid_positive_usage_event_count | integer | Events with valid user_key AND view_count > 0 |
| exact_duplicate_event_count | integer | Rows that were exact duplicates and removed |
| duplicate_event_count_removed | integer | Alias for exact_duplicate_event_count (removal count) |
| missing_user_id_event_count | integer | Events excluded due to missing user identifier |
| invalid_user_id_event_count | integer | Events excluded due to invalid user identifier format |
| prohibited_identifier_event_count | integer | Events excluded because identifier matched a prohibited pattern |
| invalid_report_id_event_count | integer | Events excluded due to missing or invalid report_id |
| invalid_date_event_count | integer | Events excluded due to unparseable date |
| future_date_event_count | integer | Events excluded because usage_date > analytics_as_of_date |
| non_finite_view_count_event_count | integer | Events excluded due to NaN or infinite view_count |
| zero_or_negative_view_event_count | integer | Events excluded because view_count <= 0 |
| excluded_event_count | integer | Total excluded events (all exclusion reasons combined) |
| excluded_user_event_share | float | excluded_event_count / source_event_count (0.0–1.0) |
| data_quality_status | string | Classification: good, degraded, poor, critical, no_data |
| data_quality_reasons | string | Pipe-delimited list of quality flags triggered |

**Status values for `data_quality_status`**: good, degraded, poor, critical, no_data

**Null vs zero policy**: `excluded_user_event_share` is 0.0 when all events are valid.
Null only when `source_event_count` is 0.

---

## 3. `outputs/analytics/engagement_window_boundaries.csv`

**(Computed in memory; not written to disk in current pipeline — available via EngagementWindowBoundaries dataclass)**

**Purpose**: Canonical observation window definitions anchored to the analytics as-of date.
One row per analytics run.

**Unique key**: analytics_run_id

| Column | Type | Description |
|--------|------|-------------|
| analytics_run_id | string | UUID for this analytics run |
| generated_at | timestamp | UTC generation timestamp |
| analytics_timezone | string | Timezone used for date interpretation |
| source_max_usage_date | date | Maximum usage_date found in mart_report_user_daily |
| analytics_as_of_date | date | The anchor date for all windows (= source_max_usage_date after policy adjustment) |
| as_of_date_policy | string | How the as-of date was resolved: exact_max, truncated_to_complete_day |
| latest_date_completeness_status | string | Whether the latest date's data appears complete |
| window_7d_start | date | Start of the 7-day window (inclusive) |
| window_7d_end | date | End of the 7-day window (= analytics_as_of_date) |
| window_28d_start | date | Start of the 28-day window (inclusive) |
| window_28d_end | date | End of the 28-day window (= analytics_as_of_date) |
| previous_28d_start | date | Start of the previous 28-day window |
| previous_28d_end | date | End of the previous 28-day window (day before window_28d_start) |
| window_90d_start | date | Start of the 90-day window |
| window_90d_end | date | End of the 90-day window (= analytics_as_of_date) |
| previous_90d_start | date | Start of the previous 90-day window |
| previous_90d_end | date | End of the previous 90-day window |
| pre_previous_28d_end | date | End of the pre-previous 28-day window (used for reactivation cohort detection) |

---

## 4. `outputs/analytics/report_engagement_history_sufficiency.csv`

**(Computed in memory; also available via build_report_history_sufficiency)**

**Purpose**: Per-report assessment of whether enough historical data exists to compute each window's metrics.

**Grain**: one row per (analytics_run_id, report_id).

**Unique key**: (analytics_run_id, report_id)

| Column | Type | Description |
|--------|------|-------------|
| analytics_run_id | string | UUID for this analytics run |
| generated_at | timestamp | UTC generation timestamp |
| analytics_as_of_date | date | Analytics anchor date |
| report_id | string | Unique report identifier |
| report_name | string | Human-readable report name |
| report_activation_date | date | First known activation date for this report |
| activation_date_status | string | known, inferred, unknown |
| history_inference_method | string | How first_observed_usage_date was determined |
| first_observed_usage_date | date | Earliest date with valid user activity in the mart |
| latest_observed_usage_date | date | Most recent date with valid user activity |
| available_calendar_history_days | integer | Days from first_observed_usage_date to analytics_as_of_date |
| active_usage_days_lifetime | integer | Count of distinct dates with valid user activity |
| has_any_valid_user_activity | boolean | True if any valid user event exists for this report |
| report_active_as_of_date | boolean | True if latest_observed_usage_date == analytics_as_of_date |
| source_coverage_start_date | date | Earliest date in the mart (used for new vs reactivated distinction) |
| history_sufficient_7d | boolean | Mart covers the full 7-day window |
| history_sufficient_28d | boolean | Mart covers the full 28-day window |
| history_sufficient_previous_28d | boolean | Mart covers the full previous 28-day window |
| comparison_history_sufficient_28d | boolean | Both recent and previous 28d windows are covered |
| history_sufficient_90d | boolean | Mart covers the full 90-day window |
| history_sufficient_previous_90d | boolean | Mart covers the full previous 90-day window |
| comparison_history_sufficient_90d | boolean | Both recent and previous 90d windows are covered |
| activation_before_7d_window | boolean | Report activated before the 7d window start |
| activation_before_28d_window | boolean | Report activated before the 28d window start |
| activation_before_previous_28d_window | boolean | Report activated before the previous 28d window start |
| activation_before_90d_window | boolean | Report activated before the 90d window start |
| activation_before_previous_90d_window | boolean | Report activated before the previous 90d window start |
| history_sufficiency_status | string | Summary status: complete_90d_history, complete_recent_28d_only, etc. |
| history_sufficiency_reasons | string | Pipe-delimited list of sufficiency flags |
| history_source_status | string | mart_available, no_mart_data, inferred |

---

## 5. `outputs/analytics/report_user_activity_metrics.csv`

**Purpose**: Report-level active-user breadth and returning/one-time user metrics for each window.

**Grain**: one row per (analytics_run_id, report_id).

**Unique key**: (analytics_run_id, report_id)

**Privacy classification**: Report-level. Share and per-user distribution fields (returning_user_share_28d, one_time_user_share_28d, repeat_view_user_share_28d) are set to null when unique_users < UserEngagementMetricsConfig.MIN_USERS_FOR_DISTRIBUTION_METRICS (default: 5). repeat_usage_status returns `privacy_suppressed` when unique_users < UserEngagementMetricsConfig.MIN_USERS_FOR_REPEAT_STATUS (default: 3).

Selected columns (full list in `UserEngagementMetricsConfig`):

| Column | Type | Description |
|--------|------|-------------|
| unique_users_28d | integer or null | Count of distinct user_keys with ≥ 1 view in 28d window |
| unique_users_previous_28d | integer or null | Same for the previous 28d window |
| unique_users_7d | integer or null | Count in 7d window |
| unique_users_90d | integer or null | Count in 90d window |
| active_user_change_28d | integer or null | unique_users_28d - unique_users_previous_28d |
| active_user_change_28d_pct | float or null | Percentage change in active users |
| active_user_direction_28d | string | growing, stable, declining, insufficient_history |
| returning_users_28d | integer or null | Users active on >= 2 distinct dates in 28d window |
| one_time_users_28d | integer or null | Users active on exactly 1 date in 28d window |
| returning_user_share_28d | float or null | returning_users_28d / unique_users_28d |
| one_time_user_share_28d | float or null | one_time_users_28d / unique_users_28d |
| repeat_view_users_28d | integer or null | Users with > 1 total view in 28d window (regardless of dates) |
| repeat_view_user_share_28d | float or null | repeat_view_users_28d / unique_users_28d |
| repeat_usage_status | string | strong, moderate, low, insufficient_evidence |
| activity_evidence_status | string | full_evidence, partial_history, no_recent_activity, no_valid_data |
| activity_privacy_suppressed | boolean | True if any activity metric was suppressed |
| activity_suppressed_fields | string | Pipe-delimited list of suppressed field names |

**Null vs zero policy**: Null means the metric could not be computed (insufficient history or suppression).
Zero means the report had zero users in that window (e.g. inactive reports get unique_users_28d = 0).

---

## 6. `outputs/analytics/report_engagement_cohorts.csv`

**Purpose**: Cohort classification for each report's 28-day active user base.

**Grain**: one row per (analytics_run_id, report_id).

**Privacy classification**: Report-level. Cohort counts (retained, reactivated, lapsed, newly_adopted) and their share fields are set to null when recent_users_28d or previous_users_28d < CohortConfig.MIN_USERS_FOR_COHORT_BREAKDOWN (default: 5). recent_users_28d and previous_users_28d are never suppressed.

| Column | Type | Description |
|--------|------|-------------|
| recent_users_28d | integer or null | Users active in the recent 28d window |
| previous_users_28d | integer or null | Users active in the previous 28d window |
| newly_adopted_users_28d | integer or null | First-ever users who appeared in the recent 28d window |
| retained_users_28d | integer or null | Users active in both recent and previous windows |
| reactivated_users_28d | integer or null | Users who returned in recent window after being absent in previous window |
| lapsed_users_28d | integer or null | Users in previous window who did not appear in recent window |
| unclassified_recent_users_28d | integer or null | Recent users who cannot be confirmed as new or reactivated |
| newly_adopted_user_share_28d | float or null | newly_adopted / recent_users_28d |
| retained_user_rate_28d | float or null | retained / recent_users_28d |
| lapse_rate_28d | float or null | lapsed / previous_users_28d |
| reactivated_user_share_28d | float or null | reactivated / recent_users_28d |
| cohort_status | string | growing, stable, high_lapse, high_newly_adopted, no_cohort_data |
| cohort_evidence_status | string | full_evidence, comparison_insufficient, no_recent_activity, etc. |
| cohort_privacy_suppressed | boolean | True if cohort metrics were suppressed |

---

## 7. `outputs/analytics/report_user_frequency_metrics.csv`

**Purpose**: Report-level frequency and intensity metrics per window.

**Grain**: one row per (analytics_run_id, report_id).

**Privacy classification**: Report-level. Per-user distribution fields (views_per_active_user_28d, views_per_user_day_28d, median_views_per_user_28d, p90_views_per_user_28d, median_user_active_days_28d, mean_return_gap_days_28d, median_return_gap_days_28d) are set to null when unique_users_28d < FrequencyMetricsConfig.MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS (default: 5). total_views_28d is never suppressed.

| Column | Type | Description |
|--------|------|-------------|
| total_views_28d | integer or null | Total views in the 28d window |
| total_user_report_days_28d | integer or null | Sum of distinct (user, date) pairs in 28d window |
| views_per_active_user_28d | float or null | total_views_28d / unique_users_28d |
| views_per_user_day_28d | float or null | total_views_28d / total_user_report_days_28d |
| median_views_per_user_28d | float or null | Median of per-user view counts |
| p90_views_per_user_28d | float or null | 90th percentile of per-user view counts |
| median_user_active_days_28d | float or null | Median of per-user distinct active date counts |
| mean_return_gap_days_28d | float or null | Mean days between consecutive visits (returning users only) |
| median_return_gap_days_28d | float or null | Median return gap (returning users only) |
| frequency_direction | string | increasing, stable, decreasing, insufficient_history |
| frequency_status | string | high_frequency, moderate_frequency, low_frequency, etc. |
| frequency_evidence_status | string | full_evidence, partial_history, no_recent_activity, no_valid_data |
| frequency_privacy_suppressed | boolean | True if frequency metrics were suppressed |

---

## 8. `outputs/analytics/report_user_concentration_metrics.csv`

**Purpose**: Report-level view concentration metrics (how evenly views are spread across users).

**Grain**: one row per (analytics_run_id, report_id).

**Privacy classification**: HHI, top-user shares, and concentration_status_28d are suppressed when active_user_count_28d < ConcentrationMetricsConfig.MIN_USERS_FOR_CONCENTRATION_METRICS (default: 5). active_user_count_28d and total_views_28d are never suppressed.

| Column | Type | Description |
|--------|------|-------------|
| active_user_count_28d | integer or null | Unique users with ≥ 1 view in 28d window |
| total_views_28d | integer or null | Total views in 28d window |
| top_1_user_view_share_28d | float or null | Views from single most active user / total_views_28d |
| top_3_users_view_share_28d | float or null | Views from top 3 users / total_views_28d |
| top_10pct_users_view_share_28d | float or null | Views from top 10% of users / total_views_28d |
| top_10pct_user_count_28d | integer or null | Number of users in the top-10% group (capped at 5) |
| user_view_hhi_28d | float or null | Herfindahl-Hirschman Index (0 = equal; 1 = one user dominates) |
| effective_user_count_28d | float or null | 1 / HHI (equivalent number of equally active users) |
| effective_user_share_28d | float or null | effective_user_count_28d / active_user_count_28d |
| concentration_status_28d | string | highly_concentrated, moderately_concentrated, broadly_distributed. Null when active_user_count_28d < 5. |
| concentration_evidence_status | string | full_evidence, partial_history, etc. |
| concentration_privacy_suppressed | boolean | True if concentration metrics were suppressed |
| suppressed_concentration_fields | string | Pipe-delimited list of suppressed field names |

**Suppression policy**: top_1_user_view_share_28d, top_3_users_view_share_28d,
top_10pct_users_view_share_28d, user_view_hhi_28d, effective_user_count_28d,
effective_user_share_28d, and concentration_status_28d are set to null when
active_user_count_28d < ConcentrationMetricsConfig.MIN_USERS_FOR_CONCENTRATION_METRICS (default: 5).
active_user_count_28d and total_views_28d are never suppressed.

---

## 9. `outputs/analytics/mart_report_engagement.csv`

**Purpose**: Final, canonical engagement mart. One row per report, containing all
engagement status flags, issue classifications, and recommended actions.

**Grain**: one row per (analytics_run_id, report_id).

**Unique key**: (analytics_run_id, report_id)

**Privacy classification**: No user_key. Report-level only. Safe to share with analytics consumers.

**`overall_engagement_status` values** (priority-ordered):

| Status | Meaning |
|--------|---------|
| no_valid_user_data | No valid user events exist for this report |
| privacy_limited | Too few users for classification (below suppression threshold) |
| insufficient_evidence | History too short to evaluate the primary windows |
| inactive | No users in last 28 days; prior history exists |
| newly_active | Report first seen in the last 14 days |
| declining_adoption | User count dropped >= 20% vs previous 28d |
| elevated_lapse | Lapse rate >= 40% of previous period's users |
| concentrated_dependency | HHI > 0.35 |
| low_repeat_usage | Returning user share < 25% |
| growing_adoption | User count grew >= 20% vs previous 28d |
| healthy_broad_adoption | Many users (>= 10), good returning share |
| healthy_niche_adoption | Few users (< 10) but consistent returning behaviour |
| stable_engagement | All checks pass; no material changes detected |

**`recommended_engagement_action` values**:

| Action | Trigger |
|--------|---------|
| continue_monitoring | Healthy or stable engagement |
| investigate_user_decline | Active user decline detected |
| investigate_user_lapse | Elevated lapse rate |
| review_concentrated_dependency | High HHI / top-user dependency |
| improve_repeat_engagement | Low returning user share |
| monitor_new_adoption | Newly active report |
| validate_report_audience | Inactive report |
| support_new_user_onboarding | Growing but low retention |
| assess_report_discoverability | Low unique users |
| investigate_data_quality | Data quality issues |
| insufficient_evidence | History too short |

**No action recommends retiring, deleting, or restricting access to any report.**

Key columns (full list in `MART_REPORT_ENGAGEMENT_COLS`):

| Column | Type | Description |
|--------|------|-------------|
| report_id | string | Unique report identifier |
| analytics_as_of_date | date | Analytics anchor date |
| overall_engagement_status | string | Primary engagement classification (see values above) |
| primary_engagement_issue | string | Highest-priority issue driving the status |
| recommended_engagement_action | string | Standardised next-step recommendation |
| engagement_evidence_status | string | full_evidence, partial_history, no_valid_data, etc. |
| privacy_suppression_status | string | not_suppressed, partially_suppressed, fully_suppressed |
| review_required | boolean | True if manual review is recommended |
| engagement_reasons | string | Plain-language explanation of the status classification |
| engagement_issue_count | integer | Number of issue flags triggered |
| engagement_warning_count | integer | Number of warning flags triggered |
| engagement_action_priority | string | high, medium, low |
