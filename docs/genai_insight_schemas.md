# GenAI Insight Layer — Schema Reference

**Report-Usage Forecasting Project**

This document describes every field in the GenAI insight layer: report-level context input, report-level insight output, portfolio context, portfolio insight output, and evaluation result. Fields that do not exist in the implementation are not documented here.

---

## 1. Report-Level Context (Input)

Built by `build_mart_context()` in `src/genai/insight_generator.py`.
Source: `outputs/analytics/mart_report_analytics.csv` (canonical mart, ~305 columns).
Only the 34-field `INSIGHT_CONTEXT_ALLOWLIST` is passed to the LLM — all other mart columns are excluded.

### 1.1 Identity and lineage

| Field | Type | Meaning | Null behaviour |
|-------|------|---------|----------------|
| `report_id` | string | Unique report identifier (e.g., `R_001`) | Never null |
| `report_name` | string | Human-readable report name | Never null |
| `analytics_run_id` | string | UUID of the analytics pipeline run | Never null |
| `analytics_as_of_date` | string (ISO date) | Date the analytics mart was built | Never null |

### 1.2 Historical usage

| Field | Type | Meaning | Null behaviour |
|-------|------|---------|----------------|
| `historical_usage_status` | string | Classified usage trajectory (e.g., `growing_usage`, `declining_usage`, `stable_regular_usage`, `prolonged_inactivity`, `bursty_usage`) | Never null |
| `recent_28d_views` | float | Total view events in the most recent 28-day window | Never null |
| `previous_28d_views` | float | Total view events in the preceding 28-day window | Never null |
| `usage_change_28d_pct` | float | Fractional change from previous to recent window (e.g., 0.10 = +10%) | May be null for inactive reports |
| `days_since_last_use` | float | Days elapsed since the last recorded view | Never null |

### 1.3 Forecast

| Field | Type | Meaning | Null behaviour |
|-------|------|---------|----------------|
| `forecast_total_28d` | float | Forecasted total views for the next 28-day window | May be null for inactive reports |
| `forecast_change_vs_actual_28d_pct` | float | Forecasted change vs most recent actuals (fractional) | May be null |
| `forecast_outlook_status` | string | Classified forecast direction (`growth_expected`, `decline_expected`, `stable_outlook`, `uncertain_outlook`, `reactivation_expected`) | Never null |
| `forecast_uncertainty_status` | string | Forecast precision classification (`low_uncertainty`, `moderate_uncertainty`, `high_uncertainty`, `very_high_uncertainty`, `intervals_unavailable`) | Never null |
| `forecast_interpretation_status` | string | Interpretability flag (`sufficient_evidence`, `insufficient_model_evidence`) | Never null |

### 1.4 Model health

| Field | Type | Meaning | Null behaviour |
|-------|------|---------|----------------|
| `model_diagnostic_status` | string | Model health classification (e.g., `insufficient_evidence`, `good_calibration`) | Never null |
| `primary_model_issue` | string | Primary model issue identifier | May be null when no issue detected |

### 1.5 User engagement

| Field | Type | Meaning | Null behaviour | Privacy |
|-------|------|---------|----------------|---------|
| `unique_users_28d` | float | Count of distinct users in the 28-day window | **Null when privacy-suppressed** | Suppressed if count < 5 |
| `active_user_direction_28d` | string | Directional trend in active users (`growing`, `stable`, `declining`, `inactive`) | **Null when privacy-suppressed** | Suppressed if count < 5 |
| `returning_user_share_28d` | float (0–1) | Fraction of views from returning users | **Null when privacy-suppressed** | Suppressed if count < 5 |
| `retained_user_rate_28d` | float (0–1) | Fraction of prior-period users retained in the current period | **Null when privacy-suppressed** | Suppressed if count < 5 |
| `lapse_rate_28d` | float (0–1) | Fraction of active users who lapsed in the current period | **Null when privacy-suppressed** | Suppressed if count < 5 |
| `overall_engagement_status` | string | Classified engagement health (e.g., `healthy_broad_adoption`, `declining_adoption`, `elevated_lapse`) | **Null when privacy-suppressed or inactive** | — |

### 1.6 Dependency

| Field | Type | Meaning | Null behaviour |
|-------|------|---------|----------------|
| `dependency_status` | string | Dependency concentration classification (e.g., `broadly_distributed_stable_dependency`, `moderately_concentrated_stable_dependency`) | May be null |

### 1.7 Decision support

| Field | Type | Meaning | Null behaviour |
|-------|------|---------|----------------|
| `primary_diagnostic` | string | Dominant diagnostic code for this report | Never null |
| `overall_report_status` | string | Aggregated status (`growing`, `healthy`, `declining`, `planned_deprecation`, `insufficient_evidence`) | Never null |
| `overall_evidence_status` | string | Evidence completeness (`complete`, `incomplete`) | Never null |
| `recommended_report_action` | string | Deterministic action from analytics layer (e.g., `continue_monitoring`, `investigate_usage_decline`, `review_planned_deprecation`) | Never null |
| `overall_review_priority` | string | Deterministic review priority (`low`, `medium`, `high`) | Never null |
| `report_reasons` | string (JSON array or null) | Reasons for the assigned action | May be null |

### 1.8 Metadata and evidence

| Field | Type | Meaning | Null behaviour |
|-------|------|---------|----------------|
| `criticality_level` | string | Business criticality label | May be null (`unknown`) |
| `expected_usage_cadence` | string | Expected usage pattern (daily, weekly, etc.) | May be null |
| `privacy_suppression_status` | string | Privacy flag (`not_suppressed`, `suppressed`) | Never null |
| `privacy_suppressed_fields` | string (comma-separated) or null | Fields withheld due to privacy suppression | Null when not suppressed |
| `missing_engagement_evidence` | string or null | Reason engagement evidence is unavailable | Null when evidence is complete |

### 1.9 Privacy rules

- Fields that are null due to privacy suppression are passed as `null` in the context, not omitted.
- The LLM must not infer, estimate, or reconstruct suppressed values.
- `privacy_suppression_status` and `privacy_suppressed_fields` are always passed so the LLM can disclose the limitation.
- No individual user identifiers (`user_id`, email, name) are passed.

---

## 2. Report-Level Insight Output

Written to `outputs/insights/report_ai_insights.json` (JSON array, one object per report).
Schema enforced by `_validate_insight_schema()` in `src/genai/insight_generator.py`.

### 2.1 Required narrative fields

| Field | Type | Max words (heuristic) | Description |
|-------|------|-----------------------|-------------|
| `executive_summary` | string | 60 | One-paragraph stakeholder summary of the report's status and most important finding |
| `usage_insight` | string | 50 | Narrative description of historical usage trend using context numbers and direction |
| `engagement_insight` | string | 50 | Narrative description of user engagement patterns; must acknowledge privacy suppression if present |
| `forecast_insight` | string | 50 | Narrative description of the forecast outlook; must acknowledge high uncertainty if present |
| `model_confidence_note` | string | 40 | Narrative note on model diagnostic evidence; must acknowledge insufficient evidence if present |
| `recommended_action` | string | 30 | Plain-language restatement of the deterministic `recommended_report_action`; must preserve the action category |
| `evidence_limitations` | list[string] | 60 per item | Material limitations the reader needs to calibrate trust |

### 2.2 Lineage fields

| Field | Type | Description |
|-------|------|-------------|
| `report_id` | string | Report identifier (echoed from context) |
| `report_name` | string | Report name (echoed from context) |
| `analytics_run_id` | string | UUID of the analytics run used as input |
| `analytics_as_of_date` | string | Date of the analytics mart |
| `prompt_version` | string | Prompt version string (`report_insight_v1`) |
| `model_name` | string | Model name used for generation |
| `input_hash` | string | SHA-256 of sorted context + prompt version + model name |
| `generated_at` | string (ISO 8601) | Timestamp of generation |
| `genai_run_id` | string | UUID of the GenAI pipeline run |

### 2.3 Generation and validation status fields

| Field | Type | Values | Description |
|-------|------|--------|-------------|
| `generation_status` | string | `success`, `reused`, `rule_based`, `fallback_schema_invalid`, `fallback_api_error` | How the insight was produced |
| `validation_status` | string | `valid`, `invalid` | Schema and grounding validation result |
| `generation_error` | string or null | Error message | Populated when generation failed; null otherwise |
| `api_attempts` | integer | 0, 1, 2 | Number of API calls made |
| `generation_mode` | string | `openai_api`, `rule_based_fallback` | Technical generation method |

### 2.4 Legacy fields (backward-compatible)

The following fields are present in stored outputs for Streamlit backward compatibility. They are populated by the rule-based fallback path and may not reflect LLM-generated values:

| Field | Type | Description |
|-------|------|-------------|
| `health_status` | string | Echoes `overall_report_status` |
| `forecast_summary` | string | Short forecast string (same content as `forecast_insight` in fallback outputs) |
| `confidence` | string | `high`, `medium`, or `low` derived from diagnostic status |
| `recommended_actions` | list[string] | Single-item list wrapping `recommended_action` |
| `hypotheses` | list | Empty list in current fallback path |
| `key_drivers` | list | Empty list in current fallback path |

These fields are read by the Streamlit dashboard (`src/app/streamlit_app.py`). They are not validated by the evaluation framework and are not part of the canonical GenAI schema.

### 2.5 Generation status semantics

| Status | Meaning |
|--------|---------|
| `success` | LLM API call succeeded and output passed validation |
| `reused` | Input hash matched a prior successful or reused result; no API call made |
| `rule_based` | No API key available; deterministic fallback used |
| `fallback_schema_invalid` | LLM response failed validation; deterministic fallback used |
| `fallback_api_error` | API call failed (timeout, rate limit, error); deterministic fallback used |

---

## 3. Portfolio Context

Built by `build_portfolio_context()` in `src/genai/portfolio_insights.py`.
Same canonical source: `outputs/analytics/mart_report_analytics.csv`.
No user-level data, no individual report narratives.

### 3.1 Top-level fields

| Field | Type | Description |
|-------|------|-------------|
| `analytics_run_id` | string | UUID of the analytics run |
| `analytics_as_of_date` | string | Date of the analytics mart |
| `total_report_count` | integer | Total reports in the mart |

### 3.2 portfolio_evidence

| Field | Type | Description |
|-------|------|-------------|
| `reports_with_sufficient_evidence` | integer | Reports where `overall_evidence_status = complete` |
| `reports_with_insufficient_evidence` | integer | Reports where evidence is incomplete |
| `reports_with_privacy_suppression` | integer | Reports where `privacy_suppression_status = suppressed` |
| `reports_with_missing_metadata` | integer | Reports where `criticality_level = unknown` |

### 3.3 historical_usage

| Field | Type | Description |
|-------|------|-------------|
| `growing` | integer | Count of reports with growing historical status |
| `growing_share_pct` | float | Percentage of total reports |
| `stable` | integer | Count of stable reports |
| `stable_share_pct` | float | Percentage |
| `declining` | integer | Count of declining reports |
| `declining_share_pct` | float | Percentage |
| `inactive` | integer | Count of inactive reports |
| `inactive_share_pct` | float | Percentage |
| `other` | integer | Count with other statuses |
| `status_counts` | dict | Raw count per `historical_usage_status` value |
| `long_zero_usage_streak_count` | integer | Reports with zero-usage streak ≥ 14 days |

### 3.4 forecast_outlook

| Field | Type | Description |
|-------|------|-------------|
| `growth_expected` | integer | Reports with `forecast_outlook_status = growth_expected` |
| `growth_expected_share_pct` | float | Percentage |
| `stable_expected` | integer | Stable forecast count |
| `decline_expected` | integer | Decline-expected count |
| `decline_expected_share_pct` | float | Percentage |
| `reactivation_expected` | integer | Reactivation-expected count |
| `uncertain_outlook` | integer | Uncertain-outlook count |
| `high_or_very_high_uncertainty` | integer | High or very high uncertainty count |
| `high_uncertainty_share_pct` | float | Percentage |
| `intervals_unavailable` | integer | Reports with no prediction intervals |
| `status_counts` | dict | Raw count per `forecast_outlook_status` value |

### 3.5 model_health

| Field | Type | Description |
|-------|------|-------------|
| `status_counts` | dict | Count per `model_diagnostic_status` value |
| `recommended_action_counts` | dict | Count per model diagnostic recommended action |
| `primary_issue_counts` | dict | Count per `primary_model_issue` value |
| `poor_calibration_count` | integer | Reports with poor calibration |

### 3.6 engagement

| Field | Type | Description |
|-------|------|-------------|
| `status_counts` | dict | Count per `overall_engagement_status` value |
| `declining_active_user_breadth` | integer | Reports with `active_user_direction_28d = declining` |
| `elevated_lapse` | integer | Reports with elevated lapse status |
| `strong_retention` | integer | Reports with `retained_user_rate_28d ≥ 0.7` |
| `high_user_concentration_or_dependency` | integer | Reports with concentrated dependency |

### 3.7 decision_support

| Field | Type | Description |
|-------|------|-------------|
| `overall_status_counts` | dict | Count per `overall_report_status` value |
| `review_priority_counts` | dict | Count per `overall_review_priority` value |
| `recommended_action_counts` | dict | Count per `recommended_report_action` value |

### 3.8 top-level risk / signal strings

| Field | Type | Description |
|-------|------|-------------|
| `top_risks` | list[string] | Up to 3 deterministic risk summary strings |
| `top_positive_signals` | list[string] | Up to 3 deterministic positive signal strings |

### 3.9 attention_shortlist

Up to 5 reports with non-`continue_monitoring` actions, ranked deterministically by `overall_review_priority` (high → medium → low), then `overall_report_status`, then `report_id`.

Each shortlist entry:

| Field | Type | Description |
|-------|------|-------------|
| `report_id` | string | Report identifier |
| `report_name` | string | Report name |
| `overall_review_priority` | string | `high`, `medium`, or `low` |
| `overall_report_status` | string | Report status |
| `primary_diagnostic` | string | Primary diagnostic code |
| `recommended_report_action` | string | Deterministic recommended action |
| `overall_evidence_status` | string | Evidence completeness |

The shortlist is limited to 5 items and is not the complete action queue for the portfolio.

---

## 4. Portfolio Insight Output

Written to `outputs/insights/portfolio_ai_insight.json` (single JSON object).
Schema enforced by `_validate_portfolio_schema()` in `src/genai/portfolio_insights.py`.

### 4.1 Required narrative fields

| Field | Type | Max words (heuristic) | Description |
|-------|------|-----------------------|-------------|
| `executive_summary` | string | 80 | One-paragraph portfolio status overview for leadership |
| `portfolio_usage_summary` | string | 70 | Narrative covering portfolio usage distribution (growing / stable / declining / inactive) |
| `portfolio_engagement_summary` | string | 70 | Narrative covering engagement health across reports |
| `portfolio_forecast_summary` | string | 70 | Narrative covering forecast outlook distribution and uncertainty |
| `portfolio_model_health_summary` | string | 50 | Narrative on model diagnostic evidence; must acknowledge insufficient evidence when present |
| `priority_actions` | list[string] | 30 per item | Priority actions referencing deterministic action categories only |
| `positive_signals` | list[string] | 25 per item | Positive trends from the deterministic aggregates |
| `evidence_limitations` | list[string] | 60 per item | Material limitations; required when model health is insufficient |

### 4.2 Lineage fields

| Field | Type | Description |
|-------|------|-------------|
| `analytics_run_id` | string | UUID of the analytics run |
| `analytics_as_of_date` | string | Date of the analytics mart |
| `report_count` | integer | Number of reports in the portfolio context |
| `prompt_version` | string | Prompt version (`portfolio_insight_v1`) |
| `model_name` | string | Model name |
| `input_hash` | string | SHA-256 of sorted context (excluding `attention_shortlist`) + prompt version + model name |
| `generated_at` | string (ISO 8601) | Generation timestamp |
| `genai_run_id` | string | UUID of the GenAI pipeline run |

### 4.3 Status fields

| Field | Type | Values | Description |
|-------|------|--------|-------------|
| `generation_status` | string | `success`, `reused`, `rule_based`, `fallback_schema_invalid`, `fallback_api_error` | How the portfolio insight was produced |
| `validation_status` | string | `valid`, `invalid` | Schema and grounding validation result |
| `generation_error` | string or null | Error message | Null when generation succeeded |
| `api_attempts` | integer | 0, 1 | Number of API calls made |
| `generation_mode` | string | `openai_api`, `rule_based_fallback` | Technical method |

Note: the `attention_shortlist` is included in the JSON output for completeness but is excluded from the hash computation.

---

## 5. Evaluation Result

Produced by `src/genai/evaluation.py`. Written to `outputs/evaluation/genai_evaluation_results.csv`.

### 5.1 Identity and generation fields

| Field | Type | Description |
|-------|------|-------------|
| `case_id` | string | Identifier for the evaluated case |
| `insight_type` | string | `report` or `portfolio` |
| `prompt_version` | string | Prompt version string |
| `model_name` | string | Model name used for generation |
| `generation_status` | string | Generation status of the evaluated insight |

### 5.2 Hard-pass dimension fields

All boolean. A `False` value contributes to `overall_pass = False`.

| Field | Failure condition |
|-------|-------------------|
| `completeness_pass` | Any required schema field is missing or empty |
| `safety_pass` | Any prohibited action phrase detected |
| `groundedness_pass` | `direction_pass AND numerical_pass` |
| `direction_pass` | Directional language contradicts `historical_usage_status` or `forecast_outlook_status` |
| `numerical_pass` | A % or count claim in narrative text is not within ±5 of any context value |
| `action_alignment_pass` | Generated action does not contain keywords matching `recommended_report_action` |
| `evidence_disclosure_pass` | A required limitation is not disclosed (model health, privacy suppression, high uncertainty) |

### 5.3 Soft-scored fields

| Field | Type | Description |
|-------|------|-------------|
| `readability_score` | float (0.0–1.0) | Heuristic: fraction of fields within word-count limits and free of generic phrases |
| `conciseness_score` | float (0.0–1.0) | Heuristic: derived from average word-count ratio vs field limits |

### 5.4 Overall pass rule

```
overall_pass = (
    completeness_pass AND safety_pass AND groundedness_pass AND
    direction_pass AND numerical_pass AND action_alignment_pass AND
    evidence_disclosure_pass AND readability_score >= 0.5
)
```

### 5.5 Failure reason format

`failure_reasons` is stored as a JSON string (list of strings). Each reason has the format:

```
<category>:<detail>
```

Examples: `ungrounded_number:95.0%`, `prohibited_phrase:\bretire\b`, `evidence_omission:model_health_insufficient`, `direction_conflict:historical_usage`, `missing_field:executive_summary`.

### 5.6 Notes

- Numerical grounding scans only the required narrative fields (not lineage fields such as dates or UUIDs).
- Percentage claims are matched within ±5 pp; count claims within ±5 units.
- Rate/share fields (containing `_pct`, `_rate`, `_share`, or `_ratio`) stored as fractions (0–1) are also included as their ×100 equivalents in the context number set.
- Readability and conciseness scores are heuristic approximations. Human review (`docs/genai_evaluation_rubric.md`) is the authoritative quality signal.
