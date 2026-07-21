# Feature Engineering Reference

This document covers feature definitions, null-handling rules, active-period assumptions,
and the three-mart output boundary produced by `notebooks/04_feature_engineering.ipynb`
and `src/features/`.

---

## Three Output Marts

Feature engineering produces three distinct output tables with different consumers.
The mart boundary is enforced by `validate_forecasting_series_input` in the forecasting
pipeline and by `validate_daily_context_schema` / `validate_insight_context_schema` in
`src/features/build_forecast_features.py`.

### 1. Forecasting Input — `mart_report_daily_series`

**File:** `data/processed/mart_report_daily_series.csv`

**Consumer:** `src/pipelines/run_forecasting_pipeline.py` → pmdarima `auto_arima`

**Grain:** one row per `(report_id, date)` within the active period.

**Columns (exactly 5):**

| Column | Type | Description |
|---|---|---|
| `report_id` | string | Report identifier |
| `date` | date | Calendar date |
| `daily_views` | int | Total report views on this date (0 for imputed days) |
| `is_observed_day` | bool/int | 1 when at least one view was recorded in the event facts |
| `is_imputed_zero` | bool/int | 1 when zero was filled in for a missing active day |

`is_observed_day` and `is_imputed_zero` are mutually exclusive and together cover
every row.

**What this mart is NOT:**  
It does not contain engagement columns, performance columns, rolling feature columns,
or any other derived context. `standardise_forecasting_columns` enforces this by keeping
only `[date, report_id, daily_views]` before the model sees the data.

---

### 2. Diagnostic Context — `mart_report_daily_context`

**File:** `data/processed/mart_report_daily_context.csv`

**Consumer:** segmentation, diagnostics, Streamlit reviewer app, analytics notebooks (06–07).

**Grain:** one row per `(report_id, date)` within the active period.

**Built by:** `build_report_daily_context` in `src/features/build_forecast_features.py`,
which left-joins `mart_report_daily_adoption` with `mart_user_engagement` and
`mart_report_performance` on `(date, report_id)`.

**Contains:** all adoption columns, rolling usage features, engagement features, and
performance features (see definitions below).

**This mart is NOT passed to ARIMA.** Engagement and performance columns are diagnostic
context only. Any future use of these columns as SARIMAX exogenous inputs requires an
explicit review for leakage and availability at forecast time.

---

### 3. GenAI and Streamlit Context — `mart_report_insight_context`

**File:** written to `data/processed/` or `outputs/insights/` by Notebook 08.

**Consumer:** `src/genai/insight_generator.py`, Streamlit reviewer app.

**Grain:** one row per `report_id`.

**Built by:** `build_report_insight_context` in `src/features/build_forecast_features.py`
after forecasting is complete. Joins report-level features, segment assignments, diagnostic
flags, and (when available) forecast reliability metrics into a single summary row.

**Required columns:** `report_id`, `report_name`, `segment`, `forecast_reliable`.

---

## Active-Period Assumptions

The active period for each report is anchored to `dim_report.launch_date` and
`dim_report.retire_date`.

### Synthetic data rules

- Every report has a `launch_date` set to a staggered date within the simulation window.
  Reports launched later have shorter series and may not pass data sufficiency gates.
- `retire_date` is `null` for reports still active at the end of the simulation window.
  A null `retire_date` means the report is treated as active through the last date in
  `dim_date`.
- A date is included in the daily series if and only if
  `launch_date <= date <= retire_date` (or `<= simulation_end` when `retire_date` is null).
- No rows are created for dates before `launch_date`, even if a stray view event somehow
  appears in the raw data — the active-period filter is applied before zero-fill.

### Zero-fill within the active period

After the active-date spine is created, days on which no view events appear in
`fact_report_views` receive `daily_views = 0`, `is_observed_day = 0`, and
`is_imputed_zero = 1`. This is a spine join, not a fabricated event. The event fact
tables are never modified.

---

## Usage Feature Definitions

These features are computed by `add_time_series_usage_features` in
`src/features/report_features.py`. All rolling windows use `shift(1)` before applying
`rolling()`, so the window at date *t* covers observations on days *t*−1, *t*−2, …
(leakage guard: the same-day value is never included in a window that feeds day *t*).

### `views_7d`

Sum of `daily_views` over the 7-calendar-day window ending the day before the current
date. Equivalent to `daily_views.shift(1).rolling(7, min_periods=1).sum()` within each
report group.

**Null policy:** NaN on the first row per report (no prior observation). NaN propagates
forward only when `min_periods` is not met; once at least one prior day exists the value
is non-null.

### `views_28d`

Sum of `daily_views` over the 28-calendar-day window ending the day before the current
date. Same leakage guard as `views_7d`.

**Null policy:** NaN on the first row per report.

### `viewers_7d`

Count of unique viewers over the 7-day window ending the day before the current date,
computed from `fact_report_views` before aggregation and then carried through the same
rolling mechanism.

**Null policy:** NaN on the first row per report.

### `viewers_28d`

Count of unique viewers over the 28-day window ending the day before the current date.

**Null policy:** NaN on the first row per report.

### `wow_change_views`

Week-over-week view change: `(views_{t-1} − views_{t-8}) / views_{t-8}`.
Uses lagged values (shift 1 and shift 8) so the ratio at date *t* reflects the change
between last week and the week before that.

**Null policy:** NaN when either lagged value is missing (first 8 rows per report) or
when the denominator is zero.

### `usage_change_28d_pct`

Percentage change in `views_28d` relative to the equivalent window one period earlier
(`views_28d_lag28`):

```
usage_change_28d_pct = (views_28d − views_28d.shift(28)) / views_28d.shift(28)
```

Both `views_28d` and the lagged value already exclude the current day (leakage guard
is inherited). The percentage is expressed as a decimal (0.25 = 25 % increase).

**Null policy:** NaN when fewer than 56 days of history exist per report, or when the
denominator is zero. Reports with insufficient history receive `insufficient_history = 1`
from `flag_insufficient_history`.

### `usage_trend_12w_slope`

OLS slope of `daily_views` regressed on a linear time index over the trailing 84-day
(12-week) window, shifted by 1 day (leakage guard). Computed with a rolling apply using
`np.polyfit`. Positive values indicate an upward trend; negative values indicate decline.

**Null policy:** NaN when fewer than 84 prior days exist. Null for the first 85 rows per
report. `insufficient_history` flag is set when fewer than 84 days of history exist across
the whole series.

---

## Engagement Feature Definitions

These features are computed by `build_user_engagement_features` in
`src/features/engagement_features.py` from `fact_page_views`.

**Input requirement:** `fact_page_views` must contain either `section_id` or `page_key`
as the page identifier column (not `page_id`). The function raises `ValueError` if
neither is present.

**Grain of computation:** `(date, report_id)` — features are the same-day aggregate of
all page views for that report on that date.

**Availability at forecast time:** engagement features are computed from same-day actuals
and are therefore NOT available at forecast time. They are diagnostic-only.

### `top_1_user_view_share`

Share of total daily views attributable to the single highest-consuming user:

```
top_1_user_view_share = max(user_daily_views) / sum(user_daily_views)
```

where the sum and max are taken across all users who viewed the report on date *t*.

A value of 1.0 means a single user accounted for all views. Low values indicate broad
audience distribution.

**Null policy:** NaN on days with no page-view records (imputed-zero days or days with
no section_id events). Never imputed or forward-filled.

### `top_10pct_users_view_share`

Share of total daily views attributable to the top 10 % of users by view count on date *t*:

```
top_10_n = ceil(distinct_user_count * 0.10)
top_10pct_users_view_share = sum(top_top_10_n_users_views) / sum(all_users_views)
```

When there is only one user, equals 1.0. When distinct users < 10, the threshold rounds
up to 1 user (equivalent to `top_1_user_view_share`).

**Null policy:** NaN on days with no page-view records. Column name in output is
`top_10pct_user_share` (singular `user`).

### `repeat_user_rate`

Proportion of users on date *t* who also had at least one view on the preceding 6 days
(rolling 7-day window ending the day before *t*):

```
repeat_user_rate = |users_today ∩ users_{t-7..t-1}| / |users_today|
```

**Null policy:** NaN on the first 7 rows per report, and NaN on imputed-zero days.

### `avg_pages_per_user`

Mean number of distinct pages viewed per user per day:

```
avg_pages_per_user = sum(distinct_pages_per_user) / distinct_user_count
```

**Null policy:** NaN on days with no page-view records.

---

## Performance Feature Definitions

These features are computed from `fact_report_loads` by
`build_report_performance_features` in `src/features/performance_features.py`.

**Availability at forecast time:** load-time features are diagnostic-only. They are
computed from same-day actuals and are not available before the report is loaded.

### `avg_load_time`

Mean `load_time_ms` across all load events for the report on date *t*.

**Null policy:** NaN on days with no load events. This includes imputed-zero days and
days where `fact_report_loads` has no records for the report.

### `p90_load_time`

90th percentile of `load_time_ms` across all load events for the report on date *t*.

**Null policy:** NaN on days with no load events.

### `avg_load_time_7d`

Rolling 7-day mean of `avg_load_time`, shifted by 1 day (leakage guard). Only includes
days on which at least one load event was recorded; NaN days in `avg_load_time` are
excluded from the rolling mean.

**Null policy:** NaN when no prior non-null `avg_load_time` exists within the window.

### `load_time_wow_change`

Week-over-week change in `avg_load_time`:

```
load_time_wow_change = avg_load_time_{t-1} − avg_load_time_{t-8}
```

**Null policy:** NaN when either lagged value is missing (first 8 rows per report) or
when the day had no load events.

---

## Null-Handling Rules Summary

| Feature | Source | Null trigger | Policy |
|---|---|---|---|
| `daily_views` | mart_report_daily_series | No event + imputed-zero | Set to 0 |
| `is_observed_day` | mart_report_daily_series | Imputed row | Set to 0 |
| `is_imputed_zero` | mart_report_daily_series | Observed row | Set to 0 |
| `views_7d` | adoption | First row per report | NaN; never forward-filled |
| `views_28d` | adoption | First row per report | NaN; never forward-filled |
| `viewers_7d` | adoption | First row per report | NaN; never forward-filled |
| `viewers_28d` | adoption | First row per report | NaN; never forward-filled |
| `wow_change_views` | adoption | First 8 rows; zero denominator | NaN |
| `usage_change_28d_pct` | adoption | Fewer than 56 days of history | NaN |
| `usage_trend_12w_slope` | adoption | Fewer than 84 days of history | NaN |
| `top_1_user_view_share` | engagement | Imputed-zero day or no page events | NaN |
| `top_10pct_user_share` | engagement | Imputed-zero day or no page events | NaN |
| `repeat_user_rate` | engagement | First 7 rows; imputed-zero day | NaN |
| `avg_pages_per_user` | engagement | Imputed-zero day or no page events | NaN |
| `avg_load_time` | performance | No load events on this day | NaN |
| `p90_load_time` | performance | No load events on this day | NaN |
| `avg_load_time_7d` | performance | No non-null prior values in window | NaN |
| `load_time_wow_change` | performance | First 8 rows; no load events | NaN |

**Global rule:** NaN values in engagement and performance features are never forward-filled
or imputed within the feature mart. The downstream consumer (segmentation, GenAI insight)
must handle nulls according to its own logic. Null `daily_views` in the forecasting input
is a hard error — `validate_forecasting_series_input` raises `ValueError` before any model
is fitted.

---

## Feature Role Registry

The `FEATURE_REGISTRY` in `src/features/feature_registry.py` classifies every feature
by role and availability at forecast time. The three roles are:

| Role | Available at forecast time | Used by ARIMA | Examples |
|---|---|---|---|
| `target` | No | Yes (as *y*) | `daily_views` |
| `historical predictor` | Yes | No (SARIMA is univariate) | `views_7d`, `views_28d`, `day_of_week` |
| `diagnostic-only` | No | No | all engagement and performance features |

No `diagnostic-only` feature has `used_by_forecasting_model = True`. The registry is
validated by `tests/test_temporal_leakage.py::TestFeatureRegistry`.
