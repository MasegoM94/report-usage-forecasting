# Data Dictionary — Model Diagnostic Outputs

All files are written to `outputs/diagnostics/` by the model diagnostics pipeline.
Files use `_latest.csv` suffix to indicate the most recent pipeline run.

Sign convention: `residual = actual - forecast` (positive = underforecast) unless stated otherwise.

---

## 1. training_residuals_latest.csv

**File path:** `outputs/diagnostics/training_residuals_latest.csv`

**Grain:** `(diagnostic_run_id, report_id, fit_scope, model_name, candidate_m, fold_number, residual_date)`

**Description:** One row per valid fitted observation per selected model, per report,
per fold. Captures in-sample model fit evidence. May be optimistic compared with
out-of-sample errors.

| Column | Type | Notes |
|--------|------|-------|
| `diagnostic_run_id` | string | UUID for the diagnostic run |
| `report_id` | string | Report identifier |
| `fit_scope` | string | `training` or `full` |
| `model_name` | string | Model family name |
| `candidate_m` | integer | Seasonal period candidate |
| `fold_number` | integer | Backtest fold index |
| `residual_date` | date | Date of the residual observation |
| `actual` | float | Observed daily views |
| `fitted` | float | Model fitted value |
| `residual` | float | `actual - fitted`; positive = underforecast |
| `residual_observation_valid` | boolean | False for stub rows with null residual |
| `residual_extraction_status` | string | `ok`, `no_fitted_values`, `all_invalid`, etc. |

**Null policy:** Rows with `residual_extraction_status != "ok"` or non-finite residuals
are excluded. One stub row is inserted per (report, model, fold) with zero valid rows.

---

## 2. backtest_forecast_errors_latest.csv

**File path:** `outputs/diagnostics/backtest_forecast_errors_latest.csv`

**Grain:** `(evaluation_run_id, report_id, fold_number, model_name, candidate_m, forecast_date)`

**Description:** One row per out-of-sample forecast observation from the rolling-origin
backtest. Primary residual source for diagnostics.

| Column | Type | Notes |
|--------|------|-------|
| `evaluation_run_id` | string | UUID for the evaluation run |
| `report_id` | string | Report identifier |
| `fold_number` | integer | Backtest fold index |
| `model_name` | string | Model family name |
| `candidate_m` | integer | Seasonal period used in this fold |
| `forecast_date` | date | Forecast horizon date |
| `horizon_days` | integer | Days ahead from forecast origin |
| `actual` | float | Observed daily views |
| `forecast` | float | Point forecast |
| `residual` | float | `actual - forecast`; positive = underforecast |
| `lower_95` | float | Lower bound of 95% prediction interval (may be null) |
| `upper_95` | float | Upper bound of 95% prediction interval (may be null) |

---

## 3. production_forecast_errors_latest.csv

**File path:** `outputs/diagnostics/production_forecast_errors_latest.csv`

**Grain:** `(run_id, report_id, forecast_date)`

**Description:** One row per realized production forecast comparison. Derived from
`outputs/metrics/realized_forecast_history.csv`. May contain limited observations early
in the pipeline lifecycle.

| Column | Type | Notes |
|--------|------|-------|
| `run_id` | string | Production forecast run ID |
| `report_id` | string | Report identifier |
| `forecast_date` | date | Forecast horizon date |
| `actual` | float | Observed daily views |
| `forecast` | float | Production point forecast |
| `signed_error` | float | `forecast - actual`; positive = overforecast |
| `residual` | float | `actual - forecast = -signed_error`; positive = underforecast |
| `lower_95` | float | Lower bound of production prediction interval (may be null) |
| `upper_95` | float | Upper bound of production prediction interval (may be null) |

**Note:** `signed_error` is preserved from the canonical production history. `residual`
is derived for diagnostic consistency and equals `-signed_error`.

---

## 4. training_autocorrelation_diagnostics_latest.csv

**File path:** `outputs/diagnostics/training_autocorrelation_diagnostics_latest.csv`

**Grain:** `(diagnostic_run_id, report_id, fit_scope, model_name, candidate_m)`

**Description:** Autocorrelation diagnostics computed on training residuals.

| Column | Type | Notes |
|--------|------|-------|
| `report_id` | string | Report identifier |
| `model_name` | string | Model family name |
| `candidate_m` | integer | Seasonal period |
| `n_residuals` | integer | Number of valid residuals |
| `acf_lag1` | float | ACF at lag 1 |
| `acf_lag_m` | float | ACF at seasonal lag `candidate_m` |
| `acf_max` | float | Maximum absolute ACF over tested lags |
| `acf_max_lag` | integer | Lag at which `acf_max` occurs |
| `ljung_box_pvalue` | float | Ljung-Box joint significance test p-value |
| `ljung_box_lags` | integer | Number of lags tested |
| `durbin_watson` | float | Durbin-Watson statistic (2 = no serial correlation) |
| `acf_classification` | string | `ok`, `warning`, `poor`, `insufficient_data` |

---

## 5. backtest_autocorrelation_by_fold_latest.csv

**File path:** `outputs/diagnostics/backtest_autocorrelation_by_fold_latest.csv`

**Grain:** `(evaluation_run_id, report_id, fold_number, model_name, candidate_m)`

**Description:** Per-fold autocorrelation diagnostics for backtest residuals.

Same columns as training ACF plus `fold_number`.

---

## 6. backtest_autocorrelation_summary_latest.csv

**File path:** `outputs/diagnostics/backtest_autocorrelation_summary_latest.csv`

**Grain:** `(evaluation_run_id, report_id, model_name, candidate_m)`

**Description:** Cross-fold summary of autocorrelation diagnostics. Primary ACF
source for model-health classification.

| Column | Type | Notes |
|--------|------|-------|
| `report_id` | string | Report identifier |
| `n_valid_folds` | integer | Number of folds with valid ACF |
| `mean_acf_lag1` | float | Mean ACF lag-1 across folds |
| `mean_acf_lag_m` | float | Mean ACF at seasonal lag across folds |
| `mean_acf_max` | float | Mean max ACF across folds |
| `mean_ljung_box_pvalue` | float | Mean LB p-value across folds |
| `mean_durbin_watson` | float | Mean DW statistic across folds |
| `acf_classification` | string | `ok`, `warning`, `poor`, `insufficient_data` |

---

## 7. production_autocorrelation_diagnostics_latest.csv

**File path:** `outputs/diagnostics/production_autocorrelation_diagnostics_latest.csv`

**Grain:** `(diagnostic_run_id, report_id)`

**Description:** ACF diagnostics computed on production forecast errors. Same
column structure as training ACF.

---

## 8. training_bias_stability_diagnostics_latest.csv

**File path:** `outputs/diagnostics/training_bias_stability_diagnostics_latest.csv`

**Grain:** `(diagnostic_run_id, report_id, fit_scope, model_name, candidate_m)`

**Description:** Bias and variance stability diagnostics computed on training residuals.

| Column | Type | Notes |
|--------|------|-------|
| `report_id` | string | Report identifier |
| `n_residuals` | integer | Number of valid residuals |
| `mean_residual` | float | Mean of residuals (positive = underforecast on average) |
| `median_residual` | float | Median of residuals |
| `normalized_bias` | float | `sum(residuals) / sum(actuals)` |
| `residual_variance` | float | Variance of residuals |
| `bias_classification` | string | `ok`, `warning`, `poor`, `insufficient_data` |
| `variance_status` | string | `stable`, `warning`, `unstable`, `insufficient_data` |

---

## 9. backtest_bias_stability_by_fold_latest.csv

**File path:** `outputs/diagnostics/backtest_bias_stability_by_fold_latest.csv`

**Grain:** `(evaluation_run_id, report_id, fold_number, model_name, candidate_m)`

**Description:** Per-fold bias and variance diagnostics for backtest residuals.
Includes horizon-bucket breakdown (short / medium / long).

Same columns as training bias plus `fold_number`, `horizon_bucket`.

---

## 10. backtest_bias_stability_summary_latest.csv

**File path:** `outputs/diagnostics/backtest_bias_stability_summary_latest.csv`

**Grain:** `(evaluation_run_id, report_id, model_name, candidate_m)`

**Description:** Cross-fold summary of bias and variance diagnostics. Primary bias
source for model-health classification.

| Column | Type | Notes |
|--------|------|-------|
| `report_id` | string | Report identifier |
| `n_valid_folds` | integer | Number of folds with valid bias |
| `mean_residual` | float | Mean bias across folds |
| `median_residual` | float | Median bias across folds |
| `normalized_bias` | float | Aggregate normalized bias |
| `residual_variance` | float | Aggregate residual variance |
| `variance_change_ratio` | float | Recent vs. previous fold variance ratio |
| `bias_classification` | string | `ok`, `warning`, `poor`, `insufficient_data` |
| `variance_status` | string | `stable`, `warning`, `unstable`, `insufficient_data` |

---

## 11. production_bias_stability_diagnostics_latest.csv

**File path:** `outputs/diagnostics/production_bias_stability_diagnostics_latest.csv`

**Grain:** `(diagnostic_run_id, report_id)`

**Description:** Bias and variance stability for production forecast errors.
Same column structure as training bias.

---

## 12. training_outlier_distribution_diagnostics_latest.csv

**File path:** `outputs/diagnostics/training_outlier_distribution_diagnostics_latest.csv`

**Grain:** `(diagnostic_run_id, report_id, fit_scope, model_name, candidate_m)`

**Description:** Outlier detection and distribution shape diagnostics for training residuals.

| Column | Type | Notes |
|--------|------|-------|
| `report_id` | string | Report identifier |
| `n_residuals` | integer | Number of valid residuals |
| `outlier_rate` | float | Fraction of residuals flagged as outliers (robust_z > 3) |
| `n_outliers` | integer | Count of flagged outliers |
| `largest_miss` | float | Absolute value of the largest residual |
| `max_robust_z` | float | Maximum robust z-score |
| `mad` | float | Median absolute deviation |
| `outlier_scale_method` | string | `mad`, `iqr`, `std`, `exact` (fallback used) |
| `skewness` | float | Residual skewness |
| `excess_kurtosis` | float | Excess kurtosis (0 = normal) |
| `jarque_bera_pvalue` | float | Jarque-Bera normality test p-value |
| `shapiro_pvalue` | float | Shapiro-Wilk p-value (null if n >= 5000) |
| `outlier_classification` | string | `ok`, `warning`, `poor`, `insufficient_data` |
| `distribution_classification` | string | `normal`, `non_normal`, `insufficient_data` |

---

## 13. backtest_outlier_distribution_by_fold_latest.csv

**File path:** `outputs/diagnostics/backtest_outlier_distribution_by_fold_latest.csv`

**Grain:** `(evaluation_run_id, report_id, fold_number, model_name, candidate_m)`

**Description:** Per-fold outlier and distribution diagnostics for backtest residuals.

Same columns as training outlier plus `fold_number`.

---

## 14. backtest_outlier_distribution_summary_latest.csv

**File path:** `outputs/diagnostics/backtest_outlier_distribution_summary_latest.csv`

**Grain:** `(evaluation_run_id, report_id, model_name, candidate_m)`

**Description:** Cross-fold summary of outlier and distribution diagnostics. Primary
source for model-health classification of outlier and distribution components.

Same columns as training outlier but aggregated across folds (means/maximums
depending on column semantics).

---

## 15. production_outlier_distribution_diagnostics_latest.csv

**File path:** `outputs/diagnostics/production_outlier_distribution_diagnostics_latest.csv`

**Grain:** `(diagnostic_run_id, report_id)`

**Description:** Outlier and distribution diagnostics for production forecast errors.
Same column structure as training outlier.

---

## 16. backtest_interval_calibration_by_fold_latest.csv

**File path:** `outputs/diagnostics/backtest_interval_calibration_by_fold_latest.csv`

**Grain:** `(evaluation_run_id, report_id, fold_number, model_name, candidate_m)`

**Description:** Per-fold prediction interval calibration diagnostics.

| Column | Type | Notes |
|--------|------|-------|
| `report_id` | string | Report identifier |
| `fold_number` | integer | Backtest fold index |
| `n_observations` | integer | Observations with valid intervals |
| `n_with_intervals` | integer | Observations where intervals were available |
| `nominal_coverage` | float | Target coverage (default 0.95) |
| `coverage` | float | Observed fraction of actuals within interval |
| `coverage_gap` | float | `nominal_coverage - coverage`; positive = undercoverage |
| `lower_miss_rate` | float | Fraction below lower bound |
| `upper_miss_rate` | float | Fraction above upper bound |
| `interval_width` | float | Mean width of prediction intervals |
| `winkler_score` | float | Mean Winkler interval score (lower = better) |
| `horizon_bucket` | string | `short`, `medium`, `long` |
| `interval_classification` | string | `ok`, `warning`, `poor`, `no_intervals` |

---

## 17. backtest_interval_calibration_summary_latest.csv

**File path:** `outputs/diagnostics/backtest_interval_calibration_summary_latest.csv`

**Grain:** `(evaluation_run_id, report_id, model_name, candidate_m)`

**Description:** Cross-fold summary of interval calibration. Primary source for
model-health interval classification.

Same columns as per-fold interval calibration, aggregated across folds.

---

## 18. production_interval_calibration_latest.csv

**File path:** `outputs/diagnostics/production_interval_calibration_latest.csv`

**Grain:** `(diagnostic_run_id, report_id)`

**Description:** Interval calibration diagnostics for production forecast observations.

Same column structure as backtest interval summary.

---

## 19. report_model_diagnostics_latest.csv

**File path:** `outputs/diagnostics/report_model_diagnostics_latest.csv`

**Grain:** `(diagnostic_run_id, report_id)`

**Description:** Consolidated model-health summary. One row per report. Primary
output consumed by downstream notebooks, GenAI layer, and Streamlit app.

| Column | Type | Notes |
|--------|------|-------|
| `diagnostic_run_id` | string | UUID for this diagnostic run |
| `report_id` | string | Report identifier |
| `model_diagnostic_status` | string | `healthy`, `watch`, `poor`, `insufficient_evidence`, `calculation_failed` |
| `evidence_status` | string | Summary of evidence sufficiency across components |
| `acf_status` | string | `ok`, `warning`, `poor`, `insufficient_data` |
| `bias_status` | string | `ok`, `warning`, `poor`, `insufficient_data` |
| `variance_status` | string | `stable`, `warning`, `unstable`, `insufficient_data` |
| `outlier_status` | string | `ok`, `warning`, `poor`, `insufficient_data` |
| `distribution_status` | string | `normal`, `non_normal`, `insufficient_data` |
| `interval_status` | string | `ok`, `warning`, `poor`, `no_intervals` |
| `production_status` | string | Status based on production evidence (may be null) |
| `n_valid_backtest_folds` | integer | Valid backtest folds used |
| `n_production_errors` | integer | Valid production error observations |
| `recommended_action` | string | `monitor`, `review`, `consider_retraining`, `collect_more_data`, `investigate_pipeline` |
| `automatic_retraining_triggered` | boolean | Always `False` |
| `diagnostic_run_timestamp` | datetime | When this run was computed |

**Status values — model_diagnostic_status:**

| Value | Meaning |
|-------|---------|
| `healthy` | No component in poor/warning; sufficient evidence |
| `watch` | One or more warnings |
| `poor` | At least one critical component issue |
| `insufficient_evidence` | Below minimum evidence thresholds |
| `calculation_failed` | Diagnostic calculation failed |

**Null policy:** All status fields are populated where data is available. Null values
indicate that the corresponding diagnostic could not be computed for that report.
Consumers should check `evidence_status` before relying on per-component status values.
