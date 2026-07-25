# User Analytics Methodology — Sprint 6

This document describes the metric computation methodology for all Sprint 6 user engagement outputs.
The canonical definitions are implemented in `src/analytics/engagement_definitions.py`.
Configuration thresholds are in `src/analytics/report_engagement_status.py` (EngagementStatusConfig)
and `src/analytics/privacy_policy.py` (PrivacyConfig).

---

## 1. Active-User Definition

An **active user** for a report is a `user_key` with at least one valid positive-view event
(view_count > 0) within the defined observation window.

- Grain: (report_id, user_key, window_start, window_end)
- Source: `mart_report_user_daily` rows where `record_valid = True` and `daily_views > 0`
- A user must have at least one active user day within the window to be counted as active
- Zero-view events and invalid events do not count toward active-user status

---

## 2. Returning-User Definition

A **returning user** is an active user who has activity on **at least two distinct usage dates**
within the same observation window.

- Two views on the same calendar date do NOT make a user "returning"
- Three views across three separate dates DOES make a user "returning"
- This definition applies to all `returning_users_Xd` and `returning_user_share_Xd` fields
- Source: per-user count of distinct `usage_date` values within the window >= 2

**This is the only permitted definition for "returning user" in windowed metrics.**
Do not use `lifetime_returned_flag` as a proxy for returning within a window.

---

## 3. One-Time-User Definition

A **one-time user** is an active user who has activity on **exactly one distinct usage date**
within the observation window.

- May have multiple views on that single date and remain a one-time user
- Mutually exclusive and exhaustive with returning users among active users:
  `one_time_users + returning_users = unique_users` (within the window)
- Field names: `one_time_users_Xd`, `one_time_user_share_Xd`

---

## 4. Repeat-View-User Definition

A **repeat-view user** is an active user with **more than one total view** in the window,
regardless of whether those views occurred on one date or multiple dates.

- A user with 5 views on a single date is a repeat-view user but NOT a returning user
- A user with 1 view per day across 5 days is BOTH a repeat-view user AND a returning user
- Field names: `repeat_view_users_Xd`, `repeat_view_user_share_Xd`
- Must not be labelled "returning user" — use the exact field names only

---

## 5. Cohort Definitions

Cohorts classify each recently-active user based on their cross-window history:

| Cohort | Condition |
|--------|-----------|
| newly_adopted | Active in recent 28d; no activity in previous 28d; first_observed_usage_date >= window_28d_start |
| retained | Active in both recent and previous 28d windows |
| reactivated | Active in recent 28d; not in previous 28d; has activity before the previous window (pre_previous history available) |
| lapsed | Active in previous 28d; NOT active in recent 28d |
| unclassified_recent | Active recently; not in previous 28d; insufficient history to confirm new vs reactivated |

All cohort classifications require `comparison_history_sufficient_28d = True` to be computed.

---

## 6. Cohort Denominators

| Metric | Denominator |
|--------|------------|
| newly_adopted_user_share_28d | recent_users_28d (users active in recent window) |
| retained_user_rate_28d | recent_users_28d |
| reactivated_user_share_28d | recent_users_28d |
| unclassified_recent_user_share_28d | recent_users_28d |
| lapse_rate_28d | previous_users_28d (users active in previous window) |

**Rationale**: lapse rate is a property of the previous cohort (how many didn't come back).
All other cohort shares are properties of the recent cohort (composition of current users).

---

## 7. Window Definitions

All windows are anchored to `analytics_as_of_date` (the maximum valid usage date in the mart):

| Window | Duration | Start | End |
|--------|----------|-------|-----|
| 7-day | 7 days | as_of - 6 days | as_of |
| 28-day (recent) | 28 days | as_of - 27 days | as_of |
| Previous 28-day | 28 days | (28d_start - 28 days) | (28d_start - 1 day) |
| 90-day | 90 days | as_of - 89 days | as_of |
| Previous 90-day | 90 days | (90d_start - 90 days) | (90d_start - 1 day) |

Windows are inclusive on both ends (start <= usage_date <= end).

The `as_of_date_policy` field records whether the as-of date was adjusted (e.g. for partial-day
data truncation). The pipeline is deterministic: given the same mart snapshot, the same windows
are always computed.

---

## 8. Frequency Formulas

| Metric | Formula |
|--------|---------|
| views_per_active_user_28d | SUM(daily_views) / COUNT(DISTINCT user_key) in 28d window |
| views_per_user_day_28d | SUM(daily_views) / COUNT(DISTINCT (user_key, usage_date)) in 28d window |
| mean_views_per_user_28d | MEAN of per-user total view counts |
| median_views_per_user_28d | MEDIAN of per-user total view counts |
| mean_user_active_days_28d | MEAN of per-user distinct active date counts |
| median_user_active_days_28d | MEDIAN of per-user distinct active date counts |

All frequency metrics are null when `history_sufficient_28d = False`.

---

## 9. Return-Gap Policy

Return gap metrics measure the number of days between a returning user's consecutive visits.

- Only users with >= 2 distinct active dates within the window contribute
- Gap = (later_date - earlier_date).days for each consecutive pair of active dates per user
- A user active on days 1, 8, and 15 contributes gaps of 7 and 7 days
- `mean_return_gap_days_28d` = mean of all gap values across all contributing users
- `median_return_gap_days_28d` = median of all gap values
- `returning_user_gap_observation_count_28d` = total number of gap observations

Null when no returning users exist in the window.

---

## 10. Percentile Interpolation

All percentile metrics (p75, p90) use `pandas.Series.quantile(q, interpolation='linear')`.

- This applies linear interpolation between adjacent sorted values when the exact quantile
  does not fall on an observation.
- p90_views_per_user_28d: 90th percentile of per-user total view counts in the 28d window.
- p90_user_active_days_28d: 90th percentile of per-user distinct active date counts.

---

## 11. Top-User Share Formulas

| Metric | Formula |
|--------|---------|
| top_1_user_view_share_28d | views from the highest-view user / total_views_28d |
| top_3_users_view_share_28d | views from the top 3 users by view count / total_views_28d |
| top_10pct_users_view_share_28d | views from the top-10% group (see ceiling rule) / total_views_28d |

Users are ranked by descending total view count within the 28-day window.
Ties in view count are resolved by user_key sort order (deterministic).

---

## 12. Top-10% Ceiling Rule

To avoid 100% concentration for single-user reports, the top-10% group size is computed as:

```
top_10pct_count = max(1, min(ceil(0.10 * unique_users_28d), 5))
```

The ceiling of 5 ensures that for large reports, the "top 10%" group is not dominated by a
handful of extreme users in a misleading way, and maintains comparability across reports of
different sizes.

---

## 13. Herfindahl-Hirschman Index (HHI)

The HHI measures how concentrated view activity is across users:

```
HHI = SUM((user_views_i / total_views_28d) ^ 2)  for all active users i
```

- HHI = 1.0: one user accounts for all views (maximum concentration)
- HHI ≈ 0: views are distributed equally across many users (minimum concentration)
- HHI > 0.35: classified as `concentrated_dependency` (configurable threshold in EngagementStatusConfig)

HHI is suppressed (null) when active user count < PrivacyConfig.MIN_GROUP_SIZE.

---

## 14. Effective User Count

```
effective_user_count_28d = 1 / user_view_hhi_28d
effective_user_share_28d = effective_user_count_28d / unique_users_28d
```

The effective user count is the number of equally active users that would produce the
same HHI. It provides an intuitive measure of how "wide" the engagement really is,
independent of the total user count.

Null when HHI is null or zero (which would produce division by zero).

---

## 15. Engagement-Status Hierarchy

The `overall_engagement_status` is determined by evaluating issue flags in priority order.
The first matching condition determines the status:

1. **no_valid_user_data** — `has_any_valid_user_activity = False` AND data quality is critical
2. **privacy_limited** — active user count < suppression threshold in the 28d window
3. **insufficient_evidence** — `history_sufficient_28d = False`
4. **inactive** — `unique_users_28d = 0` but `has_any_valid_user_activity = True`
5. **newly_active** — `days_since_first_valid_user_activity <= IMMATURE_DAYS_THRESHOLD` (default 14)
6. **declining_adoption** — `active_user_change_28d_pct <= -DECLINE_MATERIAL_PCT` (default -20%)
   AND `abs(active_user_change_28d) >= MIN_ABSOLUTE_CHANGE` (default 2)
7. **elevated_lapse** — `lapse_rate_28d >= ELEVATED_LAPSE_THRESHOLD` (default 40%)
8. **concentrated_dependency** — `user_view_hhi_28d > CONCENTRATION_ISSUE_HHI` (default 0.35)
9. **low_repeat_usage** — `returning_user_share_28d < LOW_REPEAT_SHARE_THRESHOLD` (default 25%)
10. **growing_adoption** — `active_user_change_28d_pct >= GROWTH_MATERIAL_PCT` (default +20%)
11. **healthy_broad_adoption** — `unique_users_28d >= NICHE_ACTIVE_USER_MAX` (default 10)
    AND `returning_user_share_28d >= NICHE_RETURNING_SHARE_MIN` (default 25%)
12. **healthy_niche_adoption** — small user count but consistent returning behaviour
13. **stable_engagement** — all checks pass; no material changes

---

## 16. Recommended-Action Mapping

Each `overall_engagement_status` maps to a standardised recommended action:

| Status | Action |
|--------|--------|
| no_valid_user_data | investigate_data_quality |
| privacy_limited | validate_report_audience |
| insufficient_evidence | insufficient_evidence |
| inactive | validate_report_audience |
| newly_active | monitor_new_adoption |
| declining_adoption | investigate_user_decline |
| elevated_lapse | investigate_user_lapse |
| concentrated_dependency | review_concentrated_dependency |
| low_repeat_usage | improve_repeat_engagement |
| growing_adoption | continue_monitoring |
| healthy_broad_adoption | continue_monitoring |
| healthy_niche_adoption | continue_monitoring |
| stable_engagement | continue_monitoring |

**No action recommends retiring, deleting, or restricting access to any report.**
These decisions require organisational context that analytics outputs alone cannot provide.

---

## 17. Privacy Suppression Policy

Suppression is applied at the **metric level** (individual fields set to null), not at the row level.
All reports always appear in `mart_report_engagement`, even if suppressed.

**When suppression is applied**:
- `active_user_count_28d < PrivacyConfig.MIN_GROUP_SIZE` (default 5):
  - `top_1_user_view_share_28d`, `top_3_users_view_share_28d`, `top_10pct_users_view_share_28d`
  - `user_view_hhi_28d`, `effective_user_count_28d`, `effective_user_share_28d`
  - Cohort-level shares that could identify individuals in a small group

**What suppression does not affect**:
- `unique_users_28d` (a count, not a share — cannot identify individuals)
- `overall_engagement_status` (set to `privacy_limited` when suppression prevents classification)
- All rows remain in the mart with a non-null status

**Prohibited output fields** (defined in `privacy_policy.py`):
Never appear in any analytics output file:
- user_id, email, email_address, user_name, username, display_name, unique_user, principal_name, directory_id

---

## 18. No-Activity vs Insufficient-History Distinction

These two cases look identical at the mart level (zero recent users) but have different causes:

| Condition | `has_any_valid_user_activity` | `history_sufficient_28d` | Status |
|-----------|------------------------------|--------------------------|--------|
| Genuinely inactive | True | True | inactive |
| Never had activity | False | True | no_valid_user_data |
| Too new to evaluate | True or False | False | insufficient_evidence |
| Not yet activated | False | False | insufficient_evidence |

Use `history_sufficiency_status` to understand which scenario applies.
Use `has_any_valid_user_activity` to distinguish inactive from never-used.

---

## 19. Stable Niche Interpretation

A report with a small but consistent user base is not a signal of failure.

**`healthy_niche_adoption`** requires ALL of:
- `unique_users_28d < NICHE_ACTIVE_USER_MAX` (default 10)
- `returning_user_share_28d >= NICHE_RETURNING_SHARE_MIN` (default 25%)
- `median_user_active_days_28d >= NICHE_MEDIAN_ACTIVE_DAYS_MIN` (default 1.5)
- No material decline, elevated lapse, or concentration issues

This reflects specialised reports (e.g. executive dashboards, compliance tools) where
a small number of regular users represents expected and healthy usage.

---

## 20. New-Adoption Maturity Policy

Reports within their first 14 days of activity are classified as `newly_active` regardless of
engagement signals. This prevents premature classification of reports that haven't had time to
establish baseline metrics.

| Days since first activity | Maturity status |
|---------------------------|-----------------|
| <= IMMATURE_DAYS_THRESHOLD (14) | newly_active |
| 15 to MATURING_DAYS_THRESHOLD (28) | maturing |
| > 28 | mature |

The `repeat_engagement_maturity_status` field records this classification.
Immature reports are excluded from the engagement-status hierarchy (steps 6–13 above)
and receive the `newly_active` status and `monitor_new_adoption` action.
