"""
Canonical user-engagement definitions for this repository.

These definitions are the authoritative source for all Sprint 6+ engagement metrics.
Any function computing engagement metrics must import and document which definition applies.
"""

# ─── Canonical Definitions ───────────────────────────────────────────────────

# ACTIVE USER
# A user with at least one valid positive-usage event (view_count > 0) for a
# specific report within a defined observation window.
# Grain: (report_id, user_key, window_start, window_end)

# ACTIVE USER DAY
# One distinct (report_id, user_key, usage_date) triple with positive usage.
# Grain: one row per report × user × date with view_count > 0.

# RETURNING USER WITHIN A WINDOW (canonical for returning_users_28d etc.)
# A user active on AT LEAST TWO distinct usage dates within the same analysis window.
# Two views on the same date = NOT a returning user for that window.
# This is the only definition permitted for "returning_user" in windowed metrics.

# ONE-TIME USER WITHIN A WINDOW (canonical for one_time_user_count_28d etc.)
# A user active on EXACTLY ONE distinct usage date within the analysis window.
# May have multiple views on that one date and remain a one-time user.
# One-time and returning are mutually exclusive and exhaustive for active users.

# REPEAT-VIEW USER WITHIN A WINDOW
# A user with MORE THAN ONE total view within a window, regardless of dates.
# MUST NOT be called "returning user". Use field name: repeat_view_user_share_Xd.
# A user may be both a one-time user and a repeat-view user (2 views on 1 day).

# LIFETIME RETURNED USER
# A user who has report activity on at least one date strictly later than their
# first-ever use date for that report.
# This is a LIFETIME HISTORY attribute, not a trailing-window metric.
# Field name: lifetime_returned_flag
# Must NOT be used as the returning_user definition in any windowed metric.

# ─── Legacy / Deprecated Definitions ─────────────────────────────────────────

# DEPRECATED: is_repeat_user (engagement_features.py)
# Old definition: date > first_view_date (same as lifetime_returned_flag)
# Replacement: returning_user within a window (two distinct active dates)
# Do not use is_repeat_user in new Sprint 6+ code.

# DEPRECATED: repeat_rate (analytics/report_features.py)
# Old definition: count of users with view_count > 1 / unique_users (lifetime)
# This mixes repeat-view semantics with lifetime-history semantics.
# Replacement: returning_user_share_28d (windowed, date-based)
# The column repeat_rate is DEPRECATED. Do not use in Sprint 6+ outputs.

# DEPRECATED: repeat_usage_flag (analytics/user_features.py)
# Old definition: (active_days > 1) OR (total_views > 1) — ambiguous
# Replacement: lifetime_returned_flag = (active_days > 1)
# The column repeat_usage_flag is DEPRECATED. Do not use in Sprint 6+ outputs.

# DEPRECATED: one_time segment using (active_days == 1) OR (total_views == 1)
# Replacement: one_active_day_user_flag = (active_days == 1)
# Segment name "one_time" is retained for backwards compatibility in user_segmentation.
# The segment now uses only the active_days == 1 criterion.

# ─── Window Constants ─────────────────────────────────────────────────────────

WINDOW_SHORT_DAYS: int = 7
WINDOW_MEDIUM_DAYS: int = 28
WINDOW_LONG_DAYS: int = 90

# ─── Field Name Registry ──────────────────────────────────────────────────────

# These are the canonical output column names for future Sprint 6+ metrics.
# Do not invent new names; extend this registry instead.

CANONICAL_FIELD_NAMES = {
    "returning_user_count_28d": "Users active on ≥2 distinct dates in trailing 28d",
    "returning_user_share_28d": "returning_user_count_28d / active_user_count_28d",
    "one_time_user_count_28d": "Users active on exactly 1 date in trailing 28d",
    "repeat_view_user_share_28d": "Users with >1 total view in trailing 28d / active_user_count_28d",
    "lifetime_returned_flag": "True if user has any usage date after their first-ever use date",
    "one_active_day_user_flag": "True if user has exactly 1 distinct active date (lifetime)",
    "active_user_count_7d": "Distinct user_key values with usage in trailing 7 days",
    "active_user_count_28d": "Distinct user_key values with usage in trailing 28 days",
    "active_user_count_90d": "Distinct user_key values with usage in trailing 90 days",
}

DEPRECATED_FIELD_NAMES = {
    "is_repeat_user": "Use lifetime_returned_flag for lifetime history; returning_user_count_28d for windows",
    "repeat_rate": "Use returning_user_share_28d",
    "repeat_usage_flag": "Use lifetime_returned_flag",
}
