# Data Model

## Overview

This project reconstructs Power BI usage telemetry into a clean analytics model for behavioural analysis, forecasting, and later GenAI-generated insights.

The goal of this data model is to move from raw usage-style tables into a structured semantic layer that supports:

- report-level usage analysis
- page-level adoption analysis
- performance monitoring
- feature engineering for forecasting
- future AI-generated explanations and recommendations

This document defines the current version of the model, including raw source-inspired tables, fact vs dimension classification, table grain, key columns, and core relationships.

---

## Modeling Approach

The model is designed as a **star schema** with:

- **dimension tables** for descriptive business context
- **fact tables** for measurable usage and performance events

This version focuses on the core telemetry needed to support usage analytics and forecasting.

---

## Modeling Assumptions

The assumptions below reflect the implementation in notebooks `01_generate_raw_tables.ipynb`, `02_build_semantic_model_csv.ipynb`, and `03_validate_semantic_model_hybrid_gx_csv.ipynb`.

- The current data is synthetic and is designed to mimic Power BI usage telemetry rather than reproduce an exported production schema exactly.
- The synthetic dataset is generated with a fixed random seed so the sample data can be recreated consistently during development.
- The current simulation covers 30 reports, 200 users, and the date range from `2025-01-01` through `2026-03-31`.
- `reports`, `users`, `report_pages`, and `dates` are treated as raw lookup/source-aligned tables that become dimensions in the processed semantic model.
- `report_views`, `report_page_views`, and `report_load_times` are treated as raw telemetry tables that become fact tables in the processed semantic model.
- `dim_date.date_key` is derived from `date` using `YYYYMMDD` integer format.
- `dim_user.user_key` is the canonical user join key in the semantic model. `user_id` is retained as descriptive user information where available.
- `dim_report.report_id` is the canonical report join key. `dim_report.launch_date` and `dim_report.retire_date` anchor the active period for each report. Days outside this window are excluded from the daily series.
- `dim_page.page_key` is a surrogate key generated in the semantic build because the natural page business key is `report_id + section_id`. The `section_id` column (not `page_id`) is the required join key in `fact_page_views` and in engagement feature functions.
- Only reports with type `Report` or `Dashboard` receive page records in the synthetic raw data. `Paginated` reports do not receive page-level rows in `report_pages`.
- `report_views` is generated only when at least one report view occurs for a `date x report x user` combination. No zero-view rows are inserted into raw or fact tables.
- `fact_report_views` keeps the report-level usage grain as `date x report x user x consumption_method x distribution_method` and stores `view_count` as the usage measure.
- `report_page_views` is derived from `report_views`, so page-level activity exists only for report views where the report has page metadata.
- `fact_page_views` is event-level in the current processed model. Each row represents one simulated page-view event and `page_view_count` is set to `1`.
- `report_load_times` is derived from `report_views`, so each report-view row receives a corresponding simulated load-time record.
- `fact_report_loads` is event-level in the current processed model and stores `load_time_ms` as the performance measure.
- The semantic model does not currently implement slowly changing dimensions. Dimension rows are treated as the latest/static descriptive state for this first version.
- Because the synthetic raw lookup tables are generated with unique keys, the processed table row counts are expected to match the corresponding raw table row counts.
- The validation layer assumes dimension primary keys are unique, required fact keys are non-null, fact foreign keys exist in the matching dimensions, and raw-to-processed row counts reconcile.

### Raw event data integrity

Raw event facts (`fact_report_views`, `fact_page_views`, `fact_report_loads`) contain
only rows produced by actual (synthetic) events. Zero-view days are never represented
as fabricated rows in these tables. Missing daily activity is modelled as absence, not
as a zero-count event row.

Zero-fill for missing active days happens exclusively in the feature mart layer
(`mart_report_daily_series`) during the `build_report_daily_series` step in Notebook 04.
This keeps the event-level fact layer clean and prevents double-counting when the same
source table is aggregated at different granularities.

---

## Table Inventory

### Source-inspired tables

The following tables are based on the Power BI usage schema being recreated in this project:

- `reports`
- `users`
- `report_pages`
- `dates`
- `report_views`
- `report_page_views`
- `report_load_times`

### Clean semantic model tables

#### Dimensions
- `dim_date`
- `dim_user`
- `dim_report`
- `dim_page`

#### Facts
- `fact_report_views`
- `fact_page_views`
- `fact_report_loads`

### Feature marts (produced by Notebook 04)

#### Forecasting input
- `mart_report_daily_series` — canonical SARIMA input (5 columns, one row per active report-date, zero-filled)

#### Diagnostic context
- `mart_report_daily_adoption` — base daily views + rolling usage features
- `mart_user_engagement` — engagement features at the date × report_id grain
- `mart_report_performance` — load-time features at the date × report_id grain
- `mart_report_daily_context` — wide diagnostic context (joins adoption, engagement, performance)

#### GenAI / Streamlit context
- `mart_report_insight_context` — post-forecast report-level summary (one row per report_id)

See [feature_engineering.md](feature_engineering.md) for full column definitions, feature formulas, null-handling rules, and the three-mart boundary.

---

## Fact vs Dimension Classification

| Table Name | Table Type | Purpose |
|---|---|---|
| `reports` | Source lookup | Raw report metadata |
| `users` | Source lookup | Raw user metadata |
| `report_pages` | Source lookup | Raw report page metadata |
| `dates` | Source lookup | Raw calendar/date reference |
| `report_views` | Source fact-like | Raw report usage events or daily usage records |
| `report_page_views` | Source fact-like | Raw page-level usage events |
| `report_load_times` | Source fact-like | Raw performance/load telemetry |
| `dim_date` | Dimension | Calendar context for analysis |
| `dim_user` | Dimension | User reference for usage analysis |
| `dim_report` | Dimension | Report reference and metadata |
| `dim_page` | Dimension | Page reference within reports |
| `fact_report_views` | Fact | Report-level usage measures |
| `fact_page_views` | Fact | Page-level usage measures |
| `fact_report_loads` | Fact | Performance/load measures |

---

## Source-Inspired Tables

## `reports`

**Type:** Source lookup  
**Business purpose:** Stores report metadata used to describe the report being viewed or analyzed.

**Grain:**  
One row per report.

**Candidate key:**  
- `report_id`

**Important columns:**  
- `report_id`
- `report_name`
- `workspace_id`
- `report_type`
- `is_usage_metrics_report`

---

## `users`

**Type:** Source lookup  
**Business purpose:** Stores user identifiers used to connect report usage and page usage to individual users.

**Grain:**  
One row per user.

**Candidate key:**  
- `user_key`

**Important columns:**  
- `user_key`
- `user_id`
- `unique_user`

---

## `report_pages`

**Type:** Source lookup  
**Business purpose:** Stores page metadata for each report.

**Grain:**  
One row per page within a report.

**Candidate key:**  
- composite: `report_id + section_id`

**Important columns:**  
- `report_id`
- `section_id`
- `section_name`

---

## `dates`

**Type:** Source lookup  
**Business purpose:** Stores date attributes used for time-based analysis and joins.

**Grain:**  
One row per calendar date.

**Candidate key:**  
- `date`

**Important columns:**  
- `date`
- `day_of_week`
- `week_start_date`
- `month`
- `is_weekend`

---

## `report_views`

**Type:** Source fact-like  
**Business purpose:** Represents report usage at report-user-date level.

**Grain:**  
One row per `date x report x user x consumption_method x distribution_method`.

**Candidate key:**  
- composite: `date + report_id + user_key + consumption_method + distribution_method`

**Important columns:**  
- `date`
- `report_id`
- `user_key`
- `user_id`
- `consumption_method`
- `distribution_method`
- `user_agent`
- `view_count`

**Measures:**  
- `view_count`

---

## `report_page_views`

**Type:** Source fact-like  
**Business purpose:** Represents page-level usage behaviour within reports.

**Grain:**  
One row per page-view event.

**Candidate key:**  
- No explicit stable event ID is generated in the current synthetic data.
- Row identity is implied by the event row.

**Important columns:**  
- `timestamp`
- `date`
- `report_id`
- `section_id`
- `user_key`
- `client`
- `session_source`

**Measures:**  
- `page_view_count`

---

## `report_load_times`

**Type:** Source fact-like  
**Business purpose:** Represents report performance telemetry for report load events.

**Grain:**  
One row per report-load event.

**Candidate key:**  
- event grain: `timestamp`
- or composite: `timestamp + report_id + user_id`

**Important columns:**  
- `timestamp`
- `date`
- `report_id`
- `user_id`
- `browser`
- `client`
- `country`
- `load_time_ms`

**Measures:**  
- `load_time_ms`

---

## Clean Semantic Model

## Dimensions

## `dim_date`

**Type:** Dimension  
**Business purpose:** Provides standard calendar attributes for filtering, grouping, and time series analysis.

**Grain:**  
One row per calendar date.

**Primary key:**  
- `date_key`

**Columns:**  
- `date_key`
- `date`
- `day_of_week`
- `week_start_date`
- `month`
- `is_weekend`

---

## `dim_user`

**Type:** Dimension  
**Business purpose:** Provides user-level descriptive context for usage analysis.

**Grain:**  
One row per user.

**Primary key:**  
- `user_key`

**Columns:**  
- `user_key`
- `user_id`
- `unique_user`

---

## `dim_report`

**Type:** Dimension  
**Business purpose:** Provides report-level descriptive metadata.

**Grain:**  
One row per report.

**Primary key:**  
- `report_id`

**Columns:**  
- `report_id`
- `report_name`
- `workspace_id`
- `report_type`
- `is_usage_metrics_report`

---

## `dim_page`

**Type:** Dimension  
**Business purpose:** Provides page-level descriptive metadata for reports.

**Grain:**  
One row per page within a report.

**Primary key:**  
- `page_key`

**Business key:**  
- `report_id + section_id`

**Columns:**  
- `page_key`
- `report_id`
- `section_id`
- `section_name`

---

## Facts

## `fact_report_views`

**Type:** Fact  
**Business purpose:** Stores report-level usage measures for behavioural analysis and forecasting.

**Grain:**  
One row per `date x report x user x consumption_method x distribution_method`.

**Foreign keys:**  
- `date_key`
- `report_id`
- `user_key`

**Columns:**  
- `date_key`
- `report_id`
- `user_key`
- `consumption_method`
- `distribution_method`
- `view_count`

**Measures:**  
- `view_count`

---

## `fact_page_views`

**Type:** Fact  
**Business purpose:** Stores page-level interaction measures used to understand report engagement depth.

**Grain:**  
One row per page-view event.

**Foreign keys:**  
- `date_key`
- `report_id`
- `page_key`
- `user_key`

**Columns:**  
- `date_key`
- `report_id`
- `page_key`
- `user_key`
- `client`
- `session_source`
- `page_view_count`

**Measures:**  
- `page_view_count`

---

## `fact_report_loads`

**Type:** Fact  
**Business purpose:** Stores report performance metrics for load-time analysis.

**Grain:**  
One row per report-load event.

**Foreign keys:**  
- `date_key`
- `report_id`
- `user_key`

**Columns:**  
- `date_key`
- `report_id`
- `user_key`
- `browser`
- `client`
- `country`
- `load_time_ms`

**Measures:**  
- `load_time_ms`

---

## Relationship Sketch

The model follows a star-schema pattern.

### Core relationships

- `fact_report_views.date_key -> dim_date.date_key`
- `fact_report_views.report_id -> dim_report.report_id`
- `fact_report_views.user_key -> dim_user.user_key`

- `fact_page_views.date_key -> dim_date.date_key`
- `fact_page_views.report_id -> dim_report.report_id`
- `fact_page_views.page_key -> dim_page.page_key`
- `fact_page_views.user_key -> dim_user.user_key`

- `fact_report_loads.date_key -> dim_date.date_key`
- `fact_report_loads.report_id -> dim_report.report_id`
- `fact_report_loads.user_key -> dim_user.user_key`

### Supporting relationship

- `dim_page.report_id -> dim_report.report_id`

---

## Visual Relationship Sketch

```text
               dim_user
                  |
                  |
dim_date ---- fact_report_views ---- dim_report (launch_date, retire_date)
   |                                  |
   |                                  |
   |                              dim_page (section_id is the page join key)
   |
   +---- fact_page_views -----------|
   |
   +---- fact_report_loads
```

---

## Feature Marts

Feature marts are produced by `notebooks/04_feature_engineering.ipynb` and written
to `data/processed/`. They are derived tables — not raw facts — and have their own
grain and consumer contract.

---

## `mart_report_daily_series`

**Type:** Forecasting input  
**Business purpose:** Canonical SARIMA input. One contiguous row per active report-date.
Missing active days are zero-filled. No fabricated events in the underlying fact tables.

**Grain:**  
One row per `(report_id, date)` within the active period.

**Columns:**

| Column | Type | Description |
|---|---|---|
| `report_id` | string | Report identifier |
| `date` | date | Calendar date |
| `daily_views` | int | Total report views (0 for imputed days) |
| `is_observed_day` | int | 1 when at least one view event was recorded |
| `is_imputed_zero` | int | 1 when zero was filled for a missing active day |

`is_observed_day` and `is_imputed_zero` are mutually exclusive and always sum to 1.

**Validated by:** `validate_forecasting_series_input` — raises on null `daily_views`
or duplicate `(report_id, date)` pairs before any model is fitted.

**NOT consumed by:** any diagnostic, engagement, segmentation, or analytics function.

---

## `mart_report_daily_adoption`

**Type:** Intermediate mart  
**Business purpose:** Base daily usage features for one report-date pair,
including rolling views, rolling viewers, week-over-week change, trend slope, and
usage-change metrics. Input to `mart_report_daily_context`.

**Grain:**  
One row per `(report_id, date)` within the active period.

**Key columns:** `report_id`, `date`, `daily_views`, `unique_viewers`,
`views_7d`, `views_28d`, `viewers_7d`, `viewers_28d`, `wow_change_views`,
`usage_change_28d_pct`, `usage_trend_12w_slope`, `insufficient_history`.

---

## `mart_user_engagement`

**Type:** Intermediate mart  
**Business purpose:** User behavioural concentration features derived from `fact_page_views`.
Diagnostic context only — not passed to ARIMA.

**Grain:**  
One row per `(report_id, date)`.

**Key columns:** `report_id`, `date`, `top_1_user_view_share`, `top_10pct_user_share`,
`repeat_user_rate`, `avg_pages_per_user`.

**Join key note:** `fact_page_views` is joined via `section_id` (not `page_id`).

---

## `mart_report_performance`

**Type:** Intermediate mart  
**Business purpose:** Report load-time features derived from `fact_report_loads`.
Diagnostic context only — not passed to ARIMA.

**Grain:**  
One row per `(report_id, date)`.

**Key columns:** `report_id`, `date`, `avg_load_time`, `p90_load_time`,
`avg_load_time_7d`, `load_time_wow_change`.

---

## `mart_report_daily_context`

**Type:** Diagnostic context  
**Business purpose:** Wide diagnostic table joining adoption, engagement, and performance
features. Used by segmentation, diagnostics, and the Streamlit reviewer app.

**Grain:**  
One row per `(report_id, date)` within the active period.

**Built by:** `build_report_daily_context` in `src/features/build_forecast_features.py`.
Left-joins `mart_report_daily_adoption` with `mart_user_engagement` and
`mart_report_performance` on `(date, report_id)`.

**NOT passed to ARIMA.** All engagement and performance columns are diagnostic-only
unless explicitly re-approved as exogenous SARIMAX inputs in a future phase.

---

## `mart_report_insight_context`

**Type:** GenAI and Streamlit context  
**Business purpose:** Post-forecast report-level summary for GenAI insight generation
and the Streamlit reviewer app.

**Grain:**  
One row per `report_id`.

**Built by:** `build_report_insight_context` in `src/features/build_forecast_features.py`,
after the forecasting pipeline has written `forecast_reliable` to outputs.

**Required columns:** `report_id`, `report_name`, `segment`, `forecast_reliable`.
Additional columns from report features, segment assignments, and diagnostic flags are
joined in where available.

---

For feature-level definitions (formulas, null policy, leakage guard, active-period rules,
and the three-mart boundary), see [feature_engineering.md](feature_engineering.md).
