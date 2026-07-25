# Deprecated Fields — Sprint 6

This document lists engagement-related fields that were replaced in Sprint 6.
Do not use these fields in new code. The replacements are described below.

---

## `repeat_rate`

**Previous location**: `src/analytics/report_features.py` (legacy feature mart)

**Old definition**:
```
repeat_rate = COUNT(users with view_count > 1) / COUNT(unique_users)
```
This counted users who had more than one total view (regardless of date) divided by
the lifetime unique user count. It mixed repeat-view semantics (same-day multi-views)
with lifetime-history semantics (activity across any dates).

**Reason for deprecation**:
1. The denominator used lifetime unique users, making window comparisons unreliable.
2. "Repeat view" (multiple views on one session/day) was conflated with "returning user"
   (returning on a later date), producing a misleading metric that understated retention.
3. The lifetime denominator meant the metric would decrease as more users are added over time,
   even if recent engagement was stable.

**Replacement**: `returning_user_share_28d`
```
returning_user_share_28d = returning_users_28d / unique_users_28d
```
Where `returning_users_28d` counts users active on >= 2 **distinct dates** within the
28-day window. See `src/analytics/engagement_definitions.py` for the canonical definition.

**Compatibility**: `repeat_rate` is no longer written to any output file. Downstream consumers
that read `repeat_rate` from `mart_report_daily_context.csv` should migrate to `returning_user_share_28d`
from `mart_report_engagement.csv`.

---

## `is_repeat_user`

**Previous location**: `src/features/engagement_features.py` (legacy engagement feature mart)

**Old definition**:
```
is_repeat_user = (usage_date > first_view_date)
```
This flagged any user-report row where the usage date was after the user's very first view date
for that report — effectively the same as `lifetime_returned_flag`. It was a row-level boolean
in the feature mart.

**Reason for deprecation**:
1. The name "repeat user" in the engagement literature implies returning on a separate date,
   but this field captured the same user returning on any later date including many months later.
2. It was computed at the user-day grain and was not a useful windowed metric.
3. Aggregating it by report required a custom denominator that varied by context.
4. It did not distinguish between a user who returned once after a year vs a weekly regular.

**Replacement**: `returning_user_share_28d` for windowed metrics;
`lifetime_returned_flag` (in `mart_report_user_daily`) for the lifetime-history concept.

`lifetime_returned_flag = True` means the user has at least one usage_date strictly later
than their `first_report_use_date` for this report. This is the correct name for the
lifetime-history concept that `is_repeat_user` was attempting to capture.

**Compatibility**: `is_repeat_user` is no longer written to any analytics output. The
`mart_report_user_daily` mart contains `lifetime_returned_flag` as the approved replacement.

---

## `repeat_usage_flag`

**Previous location**: `src/features/engagement_features.py` (legacy feature mart)

**Old definition**:
```
repeat_usage_flag = (usage_date > first_view_date)  # same as is_repeat_user
```
This was an alias or near-duplicate of `is_repeat_user` with the same definition and
the same limitations.

**Reason for deprecation**:
Same as `is_repeat_user`. The name "repeat usage" does not specify whether it means
same-day multi-views, cross-date returns, or lifetime history — creating ambiguity across
downstream consumers. Sprint 6 replaces this with precise, context-specific fields.

**Replacement**: `lifetime_returned_flag` (in `mart_report_user_daily`) for the lifetime concept;
`repeat_view_user_share_28d` if the intent was to capture multiple views;
`returning_user_share_28d` if the intent was to capture cross-date returns.

**Compatibility**: `repeat_usage_flag` is no longer written to any analytics output.
The field `repeat_usage_status` in `mart_report_engagement` is a different field —
it classifies the report's overall returning-user level (strong / moderate / low),
not the per-user boolean flag.

---

## Summary Table

| Deprecated field | Location | Replaced by | Notes |
|-----------------|----------|-------------|-------|
| `repeat_rate` | report_features.py / mart_report_daily_context | `returning_user_share_28d` in mart_report_engagement | Window-based; uses distinct-date definition |
| `is_repeat_user` | engagement_features.py | `lifetime_returned_flag` in mart_report_user_daily | Lifetime history; or use `returning_user_share_28d` for windows |
| `repeat_usage_flag` | engagement_features.py | `lifetime_returned_flag` or `returning_user_share_28d` | Depends on original intent |
