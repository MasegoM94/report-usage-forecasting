# User Analytics Privacy Policy

This document describes the privacy controls applied to all user-level analytics in this repository.

---

## 1. Restricted Identity Layer

`data/processed/dim_user.csv` is the **restricted identity layer**. It contains:

| Column | Content |
|---|---|
| `user_key` | Surrogate key (e.g. UK_0001) |
| `user_id` | Email address (e.g. user001@masegoinc.com) |
| `unique_user` | Display name (e.g. User 001) |

**Access controls:**
- Must NOT be joined into any public analytics output.
- Must NOT be loaded into the Streamlit reviewer app.
- Must NOT be passed to any GenAI prompt.
- Must NOT appear in `outputs/metrics/` or `outputs/segments/`.
- Access requires explicit data-governance approval.

The file is retained in `data/processed/` for authorised identity-resolution workflows only.

---

## 2. Privacy-Safe Analytics Outputs

All user-level analytics outputs use only the surrogate key `user_key`.

| File | Identifier | Notes |
|---|---|---|
| `outputs/metrics/user_features.csv` | `user_key` only | Pseudonymous — no email, no display name |
| `outputs/segments/user_segments.csv` | `user_key` only | Pseudonymous — no email, no display name |

These files are marked **pseudonymous — user_key only**.

---

## 3. Prohibited Output Columns

The following column names must never appear in any analytics output file or DataFrame returned by an analytics function:

```
user_id
email
email_address
user_name
username
display_name
unique_user
principal_name
directory_id
```

The full list is maintained in `src/analytics/privacy_policy.py` as `PROHIBITED_OUTPUT_COLUMNS`.

The single approved user-level identifier for analytics outputs is `user_key`.

---

## 4. Small-Group Suppression Policy

Distribution metrics are suppressed when `unique_users < 5` to prevent re-identification from small groups.

**Rules:**
- Suppressed values are set to `None` (null). Never `0` — zero would be misleading.
- The count column (`unique_users`) itself is never suppressed.
- Total view counts (`total_views`) are never suppressed.
- Suppressed outputs include metadata columns:
  - `privacy_suppressed` (bool): True for suppressed rows.
  - `privacy_suppression_reason` (str): reason code, e.g. `unique_users_below_minimum`.
  - `suppressed_fields` (str): comma-separated list of suppressed column names.

The suppression threshold is defined in `src/analytics/privacy_policy.py` as `PrivacyConfig.MIN_USERS_FOR_DISTRIBUTION_METRICS = 5`.

---

## 5. Canonical Engagement Definitions

The authoritative definitions are in `src/analytics/engagement_definitions.py`.

| Term | Definition |
|---|---|
| Returning user (windowed) | Active on ≥ 2 distinct dates within the analysis window |
| One-time user (windowed) | Active on exactly 1 distinct date within the analysis window |
| Repeat-view user (windowed) | More than 1 total view in the window, regardless of dates |
| Lifetime returned flag | User has any activity after their first-ever use date |
| One-active-day flag | User has exactly 1 distinct active date (lifetime) |

One-time users and returning users are mutually exclusive and exhaustive for active users within a window. A user can be both a one-time user and a repeat-view user (e.g. 2 views on 1 date).

---

## 6. Deprecated Legacy Fields

The following fields are deprecated. Do not use them in new or modified code.

| Deprecated field | Module | Old definition | Canonical replacement |
|---|---|---|---|
| `repeat_rate` | `src/analytics/report_features.py` | Users with view_count > 1 / unique_users (lifetime, repeat-view semantics) | `returning_user_share_28d` |
| `is_repeat_user` | `src/features/engagement_features.py` | `date > first_view_date` (lifetime semantics) | `lifetime_returned_flag` |
| `repeat_usage_flag` | `src/analytics/user_features.py` | `(active_days > 1) OR (total_views > 1)` — ambiguous | `lifetime_returned_flag` |

Deprecated fields are retained for backwards compatibility. No scheduled removal date has been set; they may be removed in a future pipeline version without notice.

---

## 7. GenAI: Report-Level Aggregates Only

The GenAI insight layer (`src/genai/`) receives only report-level aggregate metrics. No user identifiers, user segments, or user-level behavioural data are passed to any LLM prompt.

---

## 8. Streamlit App: No Direct User Identities

The Streamlit reviewer app (`src/app/`) does not load any user-level identity or behavioural files:

- `dim_user.csv` — excluded (restricted identity data)
- `user_features.csv` — excluded (not loaded into app memory)
- `user_segments.csv` — excluded (not loaded into app memory)

The app operates on report-level aggregates and `fact_report_views.csv` (which contains `user_key` only, not emails).

Report-level functionality (forecasts, metrics, segments, diagnostics, insights) is unaffected.

---

## References

- Privacy controls implementation: `src/analytics/privacy_policy.py`
- Engagement definitions: `src/analytics/engagement_definitions.py`
- App data loader: `src/app/utils/load_data.py`
- Privacy tests: `tests/test_privacy_controls.py`
