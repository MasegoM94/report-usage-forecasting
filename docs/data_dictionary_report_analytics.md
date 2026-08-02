# Data Dictionary — Report Analytics Outputs

This document describes all eight output files produced by the report analytics pipeline.
All outputs are **report-level**. No file contains user keys, email addresses, display names,
or any other direct identifier. Aggregated user metrics use privacy suppression when the active
user count is below the minimum threshold (MIN_USERS = 5).

---

## 1. `outputs/metrics/report_features.csv`

**Grain:** one row per (analytics_run_id, report_id)
**Key:** report_id
**Source:** mart_report_daily_series (zero-filled daily spine per report)
**Evidence fields:** usage_evidence_status, usage_evidence_reasons, trend_evidence_status, anomaly_evidence_status
**Status values:**
- `historical_usage_status`: active_healthy, active_declining, active_volatile, inactive, insufficient_evidence
- `adoption_maturity_status`: newly_launched, maturing, mature, stagnant
- `inactivity_status`: not_inactive, short_term, long_term, prolonged
- `usage_volatility_status`: low_volatility, moderate_volatility, high_volatility, insufficient_evidence
**Null vs zero:** Zero views on a given day = observed zero (report available, no activity). Null metrics = insufficient evidence window (e.g., < 14 days for anomaly detection). Null is never treated as zero.
**Privacy:** No user-level fields. Report-level aggregates only.

---

## 2. `outputs/analytics/report_forecast_outlook.csv`

**Grain:** one row per (forecast_run_id, report_id)
**Key:** report_id
**Source:** forecast outputs (per-report SARIMA horizon files) + report_features.csv
**Evidence fields:** forecast_evidence_status, missing_forecast_evidence, forecast_evidence_reasons, uncertainty_evidence_status
**Status values:**
- `forecast_outlook_status`: growth_expected, stable_outlook, decline_expected, low_usage_risk, forecast_inactivity_risk, high_uncertainty, insufficient_evidence
- `forecast_direction_28d`: growing, stable, declining
- `forecast_uncertainty_status`: low, moderate, high, very_high, insufficient_evidence
- `forecast_trend_status`: rising, flat, falling, mixed, insufficient_evidence
**Null vs zero:** Null forecast values = forecast not available for that report (missing horizon file or insufficient history). Interval bounds are summed — they are conservative and do not represent the true range of the sum.
**Privacy:** No user-level fields.

---

## 3. `outputs/analytics/report_model_health_context.csv`

**Grain:** one row per (diagnostic_run_id, report_id)
**Key:** report_id
**Source:** model evaluation outputs (backtest metrics per report) + forecast_run metadata
**Evidence fields:** model_evidence_status, backtest_evidence_status, production_evidence_status, missing_model_evidence
**Status values:**
- `model_diagnostic_status`: good, watch, poor, insufficient_evidence
- `production_evidence_maturity`: mature, immature, no_production_evidence
- `bias_status`: unbiased, mild_bias, significant_bias, insufficient_evidence
- `residual_autocorrelation_status`: none, mild, significant, insufficient_evidence
- `interval_calibration_status`: calibrated, over_confident, under_confident, insufficient_evidence
- `forecast_interpretation_status`: reliable, use_with_caution, unreliable, insufficient_evidence
**Null vs zero:** Null diagnostic fields = backtest or production evidence not available. Poor model health does not imply poor report performance.
**Privacy:** No user-level fields.

---

## 4. `outputs/analytics/report_engagement_context.csv`

**Grain:** one row per (analytics_run_id, report_id)
**Key:** report_id
**Source:** mart_report_user_daily.csv (aggregated; user_key never exposed downstream)
**Evidence fields:** engagement_evidence_status, user_data_quality_status, missing_engagement_evidence, temporal_alignment_status
**Status values:**
- `overall_engagement_status`: strong, moderate, weak, declining, insufficient_evidence
- `breadth_status`: broad, niche, single_user, insufficient_evidence
- `repeat_engagement_status`: strong_repeat, moderate_repeat, low_repeat, one_time_only, insufficient_evidence
- `dependency_status`: broadly_distributed, moderately_concentrated, highly_concentrated, insufficient_evidence
- `privacy_suppression_status`: not_suppressed, suppressed (when unique_users_28d < 5)
**Null vs zero:** Concentration metrics (top_1_user_view_share_28d, user_view_hhi_28d) are null when privacy is suppressed — not zero. Lapse rate is null when cohort history is insufficient — not "no lapse".
**Privacy:** Aggregated only. privacy_suppressed_field_count records how many fields were suppressed. No user_key in this file.

---

## 5. `outputs/analytics/report_metadata_context.csv`

**Grain:** one row per (analytics_run_id, report_id)
**Key:** report_id
**Source:** dim_report.csv (explicit metadata only — nothing inferred from usage)
**Evidence fields:** metadata_evidence_status, missing_metadata_fields, metadata_reasons
**Status values:**
- `metadata_interpretation_status`: complete, partial, minimal, missing
- `ownership_status`: owned, unowned, unknown
- `cadence_metadata_status`: available, missing
- `criticality_metadata_status`: available, missing
- `deprecation_status`: active, deprecated, planned_deprecation, unknown
- `certification_status`: certified, endorsed, uncertified, unknown
**Null vs zero:** Missing metadata fields default to "unknown" — not "low value". metadata_completeness_score (0–1) reflects fraction of key fields populated. No inference from usage patterns.
**Privacy:** No user-level fields. report_steward is an ownership role label, not a personal identifier.

---

## 6. `outputs/analytics/report_diagnostics.csv`

**Grain:** one row per (analytics_run_id, report_id)
**Key:** report_id
**Source:** report_features.csv + report_forecast_outlook.csv + report_model_health_context.csv + report_engagement_context.csv + report_metadata_context.csv
**Evidence fields:** diagnostic_evidence_status, missing_diagnostic_evidence, privacy_suppression_status
**Status values:**
- `overall_diagnostic_severity`: poor, warning, informational, none, insufficient_evidence
- `primary_diagnostic_category`: historical_usage, forecast_outlook, model_health, engagement, dependency, lifecycle, metadata, data_quality, none
- `primary_diagnostic`: no_valid_data, prolonged_inactivity, severe_historical_decline, expected_inactivity, severe_model_health_issue, elevated_lapse, active_user_decline, concentrated_dependency, high_forecast_uncertainty, declining_frequency, low_repeat_engagement, metadata_limitation, newly_launched_or_immature, none
- `recommended_diagnostic_action`: (see ALLOWED_RECOMMENDED_ACTIONS in src/analytics/report_diagnostics.py)
**Null vs zero:** Risk flags are null when evidence is insufficient to raise them. A null risk flag means "unknown" — not "no risk".
**Privacy:** Concentration-based risks (concentrated_dependency_risk, increasing_dependency_risk) are never raised from suppressed metrics.

---

## 7. `outputs/analytics/report_segments.csv`

**Grain:** one row per (analytics_run_id, report_id)
**Key:** report_id
**Source:** report_features.csv + report_forecast_outlook.csv + report_model_health_context.csv + report_engagement_context.csv + report_metadata_context.csv (via diagnostics layer)
**Evidence fields:** segment_evidence_status, segment_reasons
**Status values:**
- `usage_segment`: growing_usage, stable_usage, declining_usage, inactive_usage, insufficient_evidence
- `engagement_segment`: broad_healthy_engagement, niche_healthy_engagement, declining_engagement, low_engagement, insufficient_evidence
- `forecast_segment`: growth_expected, stable_outlook, decline_expected, high_uncertainty, insufficient_evidence
- `model_health_segment`: healthy_model, watch_model, poor_model, immature_production_evidence, insufficient_evidence
- `dependency_segment`: broadly_distributed, moderately_concentrated, highly_concentrated, insufficient_evidence
- `lifecycle_segment`: newly_launched, established, at_risk_of_deprecation, deprecated, insufficient_evidence
- `metadata_segment`: metadata_complete, metadata_partial, metadata_missing, insufficient_evidence
- `primary_report_segment`: (see ALLOWED_PRIMARY_SEGMENTS in src/analytics/report_segmentation.py)
**Null vs zero:** Segment values of "insufficient_evidence" mean evidence was unavailable — not that the report is unimportant.
**Privacy:** No user-level fields. Concentration-based dependency_segment is never set from suppressed metrics.

---

## 8. `outputs/analytics/mart_report_analytics.csv`

**Grain:** one row per (analytics_run_id, report_id)
**Key:** report_id
**Source:** All seven sources above, joined on report_id
**Evidence fields:** overall_evidence_status (composite of all source evidence statuses)
**Status values:**
- `overall_report_status`: (see ALLOWED_OVERALL_STATUS in src/analytics/report_analytics_mart.py)
- `overall_review_priority`: high, medium, low, insufficient_evidence
- `recommended_report_action`: (see ALLOWED_RECOMMENDED_ACTIONS; PROHIBITED_MART_ACTIONS are never present)
**Null vs zero:** Null fields from any source remain null in the mart. The mart never replaces null with zero or with a default value unless explicitly specified in the source module.
**Privacy:** Inherits all privacy constraints from source files. No user-level fields. No email patterns. No direct identifiers.

