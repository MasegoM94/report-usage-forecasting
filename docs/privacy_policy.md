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

Distribution metrics are suppressed when a report's active user count falls below the applicable minimum to prevent re-identification from small groups.

### 4a. PrivacyConfig (src/analytics/privacy_policy.py)

`PrivacyConfig` is the central policy dataclass. All four fields are immutable (`frozen=True`):

| Attribute | Default | Purpose |
|---|---|---|
| `MIN_USERS_FOR_DISTRIBUTION_METRICS` | 5 | Minimum unique users required before per-user ratio and share fields are populated. Used by `apply_small_group_suppression()` and the engagement metrics module. |
| `MIN_USERS_FOR_COHORT_BREAKDOWN` | 5 | Minimum users required before cohort counts (retained, reactivated, lapsed) are populated. Declared on `PrivacyConfig`; the cohorts module carries a parallel local copy (`CohortConfig.MIN_USERS_FOR_COHORT_BREAKDOWN = 5`) that is not imported from `PrivacyConfig` but shares the same default. |
| `SUPPRESS_SMALL_GROUPS` | `True` | Policy flag. When `True`, suppression is active. The analytics pipeline treats this as always-on; changing it to `False` would disable suppression globally for callers of `apply_small_group_suppression()`. |
| `SUPPRESSED_SENTINEL` | `None` | The replacement value for suppressed fields. Always `None` (null) — never `0`. Zero would imply the metric was measured and found to be zero, which is a different state. |

### 4b. Module-local thresholds

Three modules define their own threshold constants that are structurally independent of `PrivacyConfig`. They are not read from `PrivacyConfig` at runtime:

| Module | Attribute | Default | Governs |
|---|---|---|---|
| `user_concentration_metrics.py` | `MIN_USERS_FOR_CONCENTRATION_METRICS` | 5 | HHI and top-user share calculations |
| `user_concentration_metrics.py` | `MIN_USERS_FOR_TOP_1_SHARE` | 5 | Top-1 user share field |
| `user_concentration_metrics.py` | `MIN_USERS_FOR_TOP_3_SHARE` | 5 | Top-3 user share field |
| `user_concentration_metrics.py` | `MIN_USERS_FOR_HHI` | 5 | HHI field |
| `user_concentration_metrics.py` | `MIN_USERS_FOR_CONCENTRATION_STATUS` | **3** | Concentration status classification |
| `user_engagement_metrics.py` | `MIN_USERS_FOR_DISTRIBUTION_METRICS` | 5 | Engagement distribution and share fields |
| `user_engagement_metrics.py` | `MIN_USERS_FOR_SHARE_METRICS` | 5 | Returning-user share fields |
| `user_engagement_metrics.py` | `MIN_USERS_FOR_REPEAT_STATUS` | **3** | Repeat-engagement status classification |
| `user_frequency_metrics.py` | `MIN_USERS_FOR_FREQUENCY_DISTRIBUTIONS` | 5 | Views-per-user and return-gap distribution fields |

`MIN_USERS_FOR_REPEAT_STATUS = 3` governs status classification only — a report with 3–4 active users receives `repeat_usage_status = privacy_suppressed` (a classified sentinel value, not null) while all share and ratio columns remain suppressed (threshold 5 applies to those).

`MIN_USERS_FOR_CONCENTRATION_STATUS = 3` is declared on `ConcentrationMetricsConfig` but is not currently enforced in the suppression logic. In the current implementation, `concentration_status_28d` is suppressed (null) at threshold 5 by the same gate as the numeric concentration fields (`MIN_USERS_FOR_CONCENTRATION_METRICS`).

### 4c. Suppression rules

- Suppressed values are set to `None` (null). Never `0`.
- The count column (`unique_users`) is never suppressed.
- Total view counts (`total_views`) are never suppressed.
- Suppressed rows include three metadata columns added by `apply_small_group_suppression()`:
  - `privacy_suppressed` (bool): `True` for suppressed rows.
  - `privacy_suppression_reason` (str): reason code, e.g. `unique_users_below_minimum`.
  - `suppressed_fields` (str): comma-separated list of suppressed column names.

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

| Deprecated field | Module | Old definition | Canonical replacement | Enforcement |
|---|---|---|---|---|
| `repeat_rate` | `src/analytics/report_features.py` | Users with view_count > 1 / unique_users (lifetime, repeat-view semantics) | `returning_user_share_28d` | **Blocked** — raises `ValueError` if present in a DataFrame passed to `report_features` validation |
| `is_repeat_user` | `src/features/engagement_features.py` | `date > first_view_date` (lifetime semantics) | `lifetime_returned_flag` | Retained as alias; emits deprecation warning |
| `repeat_usage_flag` | `src/analytics/user_features.py` | `(active_days > 1) OR (total_views > 1)` — ambiguous | `lifetime_returned_flag` | Retained as alias; emits deprecation warning |

`repeat_rate` is not available in any current output — its presence causes a pipeline error. `is_repeat_user` and `repeat_usage_flag` are still produced as aliases of `lifetime_returned_flag` for backwards compatibility; no removal date has been set.

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
