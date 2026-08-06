# Data Dictionary — Report Analytics Outputs

This document describes all eight output files produced by the report analytics pipeline.
All outputs are **report-level**. No file contains user keys, email addresses, display names,
or any other direct identifier. Where user-derived fields appear (section 4), the values
are carried from upstream user analytics outputs that have already applied privacy suppression.
The report analytics pipeline preserves those suppression results; it does not re-apply or
override them. Suppression thresholds and the specific fields they govern are documented
in each section below.

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
- `forecast_outlook_status`: growth_expected (forecast direction growing, uncertainty not very high), stable_outlook (forecast direction stable), decline_expected (forecast direction declining, uncertainty not very high), reactivation_expected (forecast direction is expected reactivation), uncertain_outlook (forecast_uncertainty_status is very_high_uncertainty regardless of direction), mixed_outlook (no other branch matched), inactivity_expected (forecast direction is expected inactivity with sufficient horizon), low_usage_expected (≥ 50 % of the 28-day horizon falls in the low-usage band), insufficient_evidence (no forecast rows, incompatible alignment, or insufficient horizon), invalid_forecast (source status is invalid). Note: forecast_low_usage_risk and forecast_inactivity_risk are separate Boolean fields — they are not forecast_outlook_status values.
- `forecast_direction_28d`: growing, stable, declining
- `forecast_uncertainty_status`: low_uncertainty (relative uncertainty ≤ 0.25), moderate_uncertainty (0.25–0.75), high_uncertainty (0.75–1.50), very_high_uncertainty (≥ 1.50), intervals_unavailable (no prediction interval bounds produced), insufficient_horizon (bounds exist but fewer than 28 horizon rows), invalid_intervals (calculated relative uncertainty is negative)
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
- `overall_engagement_status`: no_valid_user_data, insufficient_evidence, inactive, newly_active, declining_adoption, elevated_lapse, low_repeat_usage, concentrated_dependency, growing_adoption, healthy_broad_adoption, healthy_niche_adoption, stable_engagement, privacy_limited, mixed_signals. Validated against `_ALLOWED_OVERALL_STATUSES` at write time (`classify_overall_engagement_status()`, `src/analytics/report_engagement_status.py`).
- `breadth_status`: no_valid_user_data, insufficient_history, no_recent_activity, growing_breadth, declining_breadth, broad_adoption, niche_adoption. Not privacy-suppressed — classifies from any non-zero user count (`classify_breadth_status()`, `src/analytics/report_engagement_status.py`).
- `repeat_engagement_status`: no_valid_user_data, insufficient_history, too_early_to_assess, no_recent_activity, privacy_suppressed, insufficient_data, strong_repeat_engagement, moderate_repeat_engagement, low_repeat_engagement. The value `privacy_suppressed` is a classified sentinel (not null) returned when the upstream `activity_privacy_suppressed` flag is True (`classify_repeat_engagement_status()`, `src/analytics/report_engagement_status.py`).
- `dependency_status`: constructed dynamically by `_classify_dependency()` (`src/analytics/report_engagement_mart.py`) as `f"{concentration_status}_{dependency_change_status}"` or bare `concentration_status`. Sentinel `unavailable` (not null, not absent) when upstream `concentration_status` is null, `no_valid_user_data`, `insufficient_history`, or `privacy_suppressed`. Base concentration components: `broadly_distributed`, `moderately_concentrated`, `highly_concentrated`, `single_user_dependent`, `no_recent_activity`, `calculation_failed` (from `_VALID_CONCENTRATION_STATUSES`, `src/analytics/user_concentration_metrics.py`). Dependency-change suffixes appended when present and not `stable`/`no_valid_user_data`/`insufficient_history`: `_stable_dependency`, `_dependency_increasing`, `_dependency_decreasing`, `_insufficient_evidence`. Representative values: `broadly_distributed_stable_dependency`, `moderately_concentrated_dependency_increasing`, `highly_concentrated_dependency_decreasing`, `single_user_dependent_stable_dependency`, `broadly_distributed` (no suffix when change is null or `stable`).
- `privacy_suppression_status`: not_suppressed, suppressed. Assigned in `src/analytics/user_engagement_metrics.py`, `user_concentration_metrics.py`, and `user_frequency_metrics.py`; propagated unchanged through `report_engagement_mart.py`. The value `privacy_suppressed` (with underscore) does not appear in any producer — stale test fixtures using that form are incorrect.
**Null vs zero:** Concentration metrics (top_1_user_view_share_28d, user_view_hhi_28d) are null when privacy is suppressed — not zero. Lapse rate is null when cohort history is insufficient — not "no lapse".
**Privacy:** All user-derived values are carried from upstream user analytics outputs. Suppression is applied upstream, not by this file's pipeline:
- `top_1_user_view_share_28d`, `top_3_users_view_share_28d`, `user_view_hhi_28d`, `effective_user_count_28d`, `effective_user_share_28d` — null when active_user_count_28d < 5 (`ConcentrationMetricsConfig.MIN_USERS_FOR_CONCENTRATION_METRICS`, `src/analytics/user_concentration_metrics.py`)
- `returning_user_share_28d` and related share inputs — null when unique_users_28d < 5 (`UserEngagementMetricsConfig.MIN_USERS_FOR_DISTRIBUTION_METRICS`, `src/analytics/user_engagement_metrics.py`)
- `repeat_engagement_status` — returns `privacy_suppressed` when the upstream `activity_privacy_suppressed` flag is True (set at unique_users_28d < 5); the field holds a sentinel value, not null
- `dependency_status` — returns `unavailable` when `concentration_status` is null or privacy_suppressed; the upstream concentration gate is the same threshold-5 attribute above
- `breadth_status` — not privacy-suppressed; classifies from any non-zero user count
- `privacy_suppressed_field_count` records how many upstream fields were suppressed. No user_key in this file.

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
**Privacy:** Concentration-based risks (concentrated_dependency_risk, increasing_dependency_risk) are never raised when `concentration_privacy_suppressed` is True — this flag is set upstream when active_user_count_28d < ConcentrationMetricsConfig.MIN_USERS_FOR_CONCENTRATION_METRICS (default: 5). The diagnostics pipeline reads the pre-suppressed engagement context; it does not apply its own suppression threshold.

---

## 7. `outputs/analytics/report_segments.csv`

**Grain:** one row per (analytics_run_id, report_id)
**Key:** report_id
**Source:** report_features.csv + report_forecast_outlook.csv + report_model_health_context.csv + report_engagement_context.csv + report_metadata_context.csv (via diagnostics layer)
**Evidence fields:** segment_evidence_status, segment_reasons
**Status values:**
- `usage_segment`: growing_usage, stable_usage, declining_usage, inactive_usage, insufficient_evidence
- `engagement_segment`: broad_healthy_engagement, niche_healthy_engagement, declining_engagement, low_engagement, insufficient_evidence
- `forecast_segment`: growth_expected, stable_outlook, decline_expected, low_usage_expected, inactivity_expected, uncertain_outlook, insufficient_evidence
- `model_health_segment`: healthy_model, watch_model, poor_model, immature_production_evidence, insufficient_evidence
- `dependency_segment`: broadly_distributed, moderately_concentrated, highly_concentrated, insufficient_evidence
- `lifecycle_segment`: newly_launched, established, at_risk_of_deprecation, deprecated, insufficient_evidence
- `metadata_segment`: metadata_complete, metadata_partial, metadata_missing, insufficient_evidence
- `primary_report_segment`: (see ALLOWED_PRIMARY_SEGMENTS in src/analytics/report_segmentation.py)
**Null vs zero:** Segment values of "insufficient_evidence" mean evidence was unavailable — not that the report is unimportant.
**Privacy:** No user-level fields. `dependency_segment` is derived from `dependency_status` in the upstream engagement context; when `concentration_privacy_suppressed` is True (active_user_count_28d < ConcentrationMetricsConfig.MIN_USERS_FOR_CONCENTRATION_METRICS, default: 5), `dependency_status` is `unavailable` and `dependency_segment` reflects that absence rather than a measured concentration level.

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
**Privacy:** Carries all suppression results from source files unchanged — null fields remain null, sentinel values (e.g. `privacy_suppressed`, `unavailable`) remain as set by upstream modules. No user-level fields. No email patterns. No direct identifiers.

