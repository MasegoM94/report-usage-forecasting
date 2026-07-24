# Model Diagnostics Methodology (Sprint 5)

This document describes the statistical methods, policies, and thresholds used
by the Sprint 5 diagnostic modules in `src/models/`.

---

## 1. Residual Sign Convention

All three residual datasets use the diagnostic residual convention:

```
residual = actual - forecast_or_fitted
```

| Residual sign | Interpretation |
|--------------|----------------|
| positive | Model underforecast (actual > forecast) |
| negative | Model overforecast (actual < forecast) |
| zero | Perfect point forecast |

The canonical production monitoring history stores:

```
signed_error = forecast - actual   (positive = overforecast)
```

For `production_forecast_errors_latest.csv`, both columns are present. The
`signed_error` column is preserved as-is. The `residual` column is derived as
`-signed_error`.

---

## 2. Raw-Series ACF vs. Residual ACF

The autocorrelation diagnostics operate on **residuals** (actual - forecast), not
on the raw time series. This distinction matters:

- Raw-series ACF captures the temporal structure of the input data (trend,
  seasonality, cycles).
- Residual ACF captures structure that the model **failed to explain**. Persistent
  residual autocorrelation indicates that the model is missing a systematic pattern.

A model with high raw-series ACF but low residual ACF is capturing temporal structure
well. A model with low raw-series ACF but high residual ACF is failing to explain
structure that does exist.

---

## 3. Ljung-Box Interpretation

The Ljung-Box test tests the null hypothesis that all autocorrelations up to lag
`k` are jointly zero:

- **Small p-value (< 0.05):** Evidence of residual autocorrelation at some lag up
  to `k`.
- **Large p-value (>= 0.05):** Insufficient evidence to reject independence.

**Sprint 5 policy:** A significant Ljung-Box p-value does not automatically classify
a model as poor. The practical severity depends on:

1. The magnitude of individual ACF coefficients, not just the test p-value.
2. Sample size: large samples detect trivially small autocorrelations as significant.
3. Whether the largest ACF is at a lag that is operationally meaningful.

Non-significant Ljung-Box alone (with poor bias or interval coverage) does not
override those signals.

---

## 4. Durbin-Watson Interpretation

The Durbin-Watson statistic tests for first-order serial correlation in residuals:

| DW value | Interpretation |
|----------|----------------|
| ~2 | No first-order serial correlation |
| < 1 | Positive serial correlation |
| > 3 | Negative serial correlation |

DW is computed in addition to Ljung-Box because it is sensitive to lag-1 correlation
specifically, whereas Ljung-Box tests over multiple lags jointly.

---

## 5. Bias Normalization Policy

Normalized bias is computed as:

```
normalized_bias = sum(residuals) / sum(actuals)
```

where both sums are taken over all valid residual observations for the report/fold/scope.

This formulation is:
- Scale-free: comparable across reports with different usage volumes.
- Interpretable: `+0.05` means the model underforecast by 5% of total actuals.
- Robust: avoids division by a single potentially small mean.

Zero actual values are excluded from the denominator to prevent division by zero.
If all actuals are zero, `normalized_bias` is set to `null`.

---

## 6. Variance Window Policy

Residual variance stability is assessed by comparing the variance of recent folds
against earlier folds. The default policy splits folds at the midpoint:

- `recent_folds`: the second half of backtest folds by fold number.
- `previous_folds`: the first half of backtest folds by fold number.

The change ratio is computed as:

```
variance_change_ratio = variance(recent_folds) / variance(previous_folds)
```

A ratio > 2.0 is flagged as a warning; > 4.0 is flagged as poor. When there are
fewer than 4 folds, variance stability cannot be assessed reliably and the result
is noted in `evidence_status`.

---

## 7. MAD Outlier Method and Fallback

Outliers are identified using the Modified Z-score based on the Median Absolute
Deviation (MAD):

```
robust_z = (residual - median(residuals)) / (1.4826 * MAD)
```

The factor 1.4826 makes the MAD consistent with the standard deviation for normally
distributed data.

**Fallback order when MAD = 0 (degenerate case):**

1. **IQR-based scale:** `scale = 0.7413 * IQR` (consistent with std for normal data).
2. **Standard deviation:** if IQR is also 0, fall back to `std(residuals)`.
3. **Exact deviation:** if std is also 0 (all residuals identical), the scale is set
   to 1.0 and any non-zero deviation is flagged.

Residuals with `|robust_z| > 3.0` are classified as outliers. This threshold is
configurable but defaults to 3.0 for consistency with standard practice.

---

## 8. Distribution Test Policy

Sprint 5 tests residual normality using:

1. **Jarque-Bera test:** joint test of skewness and kurtosis. Applied to all sample
   sizes.
2. **Shapiro-Wilk test:** direct normality test. Applied only when `n < 5000` due
   to computational constraints.

**Sprint 5 policy:** Non-normality (significant JB or SW p-value) does NOT
automatically classify a model as poor. Non-normality primarily affects the
reliability of prediction intervals, not point-forecast accuracy. MASE and WAPE
are distribution-free metrics unaffected by residual non-normality.

Distribution results are reported and contribute to model-health classification
only when they combine with other warning signals.

---

## 9. Nominal Coverage

Prediction intervals are evaluated at nominal coverage level:

```
alpha = 0.05
nominal_coverage = 1 - alpha = 0.95
```

This means the interval is expected to contain the actual observation 95% of the
time under the model's assumptions.

**Coverage gap:**

```
coverage_gap = nominal_coverage - observed_coverage
```

A positive gap means undercoverage (intervals too narrow). A negative gap means
overcoverage (intervals too wide). Both are penalised by the Winkler score.

---

## 10. Winkler Interval Score Formula

The Winkler interval score penalises both undercoverage and excessive width:

```
For each observation:
  Winkler = width(t) + (2/alpha) * max(lower(t) - actual(t), 0)
                     + (2/alpha) * max(actual(t) - upper(t), 0)
```

where `width(t) = upper(t) - lower(t)` and `alpha = 0.05`.

Lower Winkler score is better. A perfect interval (correct coverage, minimal
width) achieves a Winkler score equal to its mean width. Misses beyond the
interval boundaries are penalised by `2/alpha = 40` per unit of miss.

---

## 11. Model-Health Classification Hierarchy

The five diagnostic components are consolidated into one status per report using
a deterministic precedence hierarchy (highest priority first):

| Priority | Status | Condition |
|----------|--------|-----------|
| 1 | `calculation_failed` | Diagnostic computation failed |
| 2 | `insufficient_evidence` | Fewer than `MIN_VALID_BACKTEST_FOLDS=2` valid folds, or no production evidence and no backtest evidence |
| 3 | `poor` | At least one component with poor status; or severe production deterioration confirmed |
| 4 | `watch` | One or more warning-level issues; or limited production evidence with acceptable backtest |
| 5 | `healthy` | No component in poor or warning status; sufficient evidence; no material production deterioration |

**Non-normality alone** does not classify a model as `poor`. One significant
Ljung-Box, Jarque-Bera, or Shapiro-Wilk p-value does not override stronger
practical evidence from bias, outlier, or calibration components.

---

## 12. Evidence-Sufficiency Rules

| Threshold | Value | Effect |
|-----------|-------|--------|
| `MIN_VALID_BACKTEST_FOLDS` | 2 | Fewer valid folds → `insufficient_evidence` |
| `MIN_PRODUCTION_ERROR_COUNT` | 10 | Fewer production observations → production diagnostics not used as primary basis |

When evidence is insufficient:

- The report receives `model_diagnostic_status = insufficient_evidence`.
- Per-component status fields are still populated where calculable, but the
  consolidated status reflects the evidence limitation.
- This is not an error; it is expected early in the pipeline lifecycle.

---

## 13. Recommended-Action Mapping

| Model-health status | Recommended action | Notes |
|---------------------|--------------------|-------|
| `healthy` | `monitor` | Continue regular monitoring |
| `watch` | `review` | Human review of warning components |
| `poor` | `consider_retraining` | Human investigation required |
| `insufficient_evidence` | `collect_more_data` | No action until evidence accrues |
| `calculation_failed` | `investigate_pipeline` | Check source data and logs |

---

## 14. No Automatic Retraining Policy

`automatic_retraining_triggered` is always `False` in Sprint 5.

The field is preserved in the output schema to support future pipeline extensions,
but no automatic action is taken. The `consider_retraining` recommended action
is a **signal for human review only**. No model weights, selection decisions, or
pipeline state are modified by diagnostic output.

This policy exists because:

1. A poor health signal may reflect data quality, regime change, or unusual
   recent observations — not necessarily a model failure.
2. Automated retraining without human review can amplify errors if the signal
   is spurious.
3. The diagnostic layer is designed to inform decisions, not replace them.

---

## 15. Future Potential Guardrails

The following guardrails are documented as future extensions but are **not
implemented in Sprint 5**:

1. **Automatic retraining trigger:** if `model_diagnostic_status = poor` persists
   for N consecutive diagnostic runs, trigger a supervised retraining workflow.
2. **Drift detection gate:** halt production forecasting for a report if production
   residuals show persistent structural drift exceeding a configured threshold.
3. **Interval recalibration:** adjust prediction-interval width empirically when
   observed coverage deviates from nominal by more than a configured tolerance.
4. **Seasonal-period re-assessment:** flag for re-profiling if residual ACF at the
   current `selected_m` is significantly higher than at alternative seasonal periods.
5. **Alert integration:** push `model_diagnostic_status = poor` events to an
   operational alerting system for stakeholder notification.

These extensions require additional pipeline infrastructure and human-review
workflows not present in the current version.
