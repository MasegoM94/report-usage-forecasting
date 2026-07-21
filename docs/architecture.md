# Architecture

This document describes the pipeline structure and mart boundary rules for
**Power BI Usage Intelligence: Forecasting, Behavioural Analytics, and GenAI Insights**.

The project uses synthetic, shareable Power BI-style telemetry data. It does not
connect to, extract from, or deploy against the real Power BI service.

---

## Pipeline Tree

```text
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 1 — RAW                                                          │
│                                                                         │
│  data/raw/                                                              │
│  ├── reports.csv            Report metadata                             │
│  ├── users.csv              User metadata                               │
│  ├── report_pages.csv       Page metadata                               │
│  ├── dates.csv              Calendar reference                          │
│  ├── report_views.csv       Report-user-date usage events               │
│  ├── report_page_views.csv  Page-view events                            │
│  └── report_load_times.csv  Load-time telemetry events                  │
│                                                                         │
│  Raw event facts are EVENT-LEVEL.                                       │
│  No fabricated zero-view events are inserted.                           │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │  Notebook 01 (generate)
                                 │  Notebook 02 (semantic model build)
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 2 — SEMANTIC MODEL                                               │
│                                                                         │
│  data/processed/                                                        │
│  ├── dim_date.csv                                                       │
│  ├── dim_user.csv                                                       │
│  ├── dim_report.csv          launch_date / retire_date anchor the       │
│  ├── dim_page.csv            active period for each report              │
│  ├── fact_report_views.csv   event-level; no zero rows                  │
│  ├── fact_page_views.csv     event-level; section_id is the join key    │
│  └── fact_report_loads.csv   event-level; load_time_ms per event        │
│                                                                         │
│  Validation: Notebook 03 / src/data/validate_model.py                  │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │  Notebook 04 (feature engineering)
                                 │  src/features/
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 3 — FEATURE MARTS                                                │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  mart_report_daily_series.csv          ← SARIMA INPUT            │   │
│  │                                                                  │   │
│  │  Grain: one row per (report_id, date) within the active period.  │   │
│  │  Columns: report_id, date, daily_views,                          │   │
│  │           is_observed_day, is_imputed_zero                       │   │
│  │                                                                  │   │
│  │  Missing active days are zero-filled (is_imputed_zero = 1).      │   │
│  │  Days outside the active period are excluded entirely.           │   │
│  │  Zero-view days within the active period are preserved —         │   │
│  │  sparse reports are excluded by data-sufficiency gating,         │   │
│  │  not by dropping zeros.                                          │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  mart_report_daily_context.csv         ← DIAGNOSTIC CONTEXT      │   │
│  │                                                                  │   │
│  │  Grain: one row per (report_id, date) within the active period.  │   │
│  │  Built by joining mart_report_daily_adoption (base usage +        │   │
│  │  rolling features) with engagement and performance feature        │   │
│  │  marts.  Wide table — engagement/performance columns are          │   │
│  │  diagnostic-only and are NOT passed to ARIMA.                    │   │
│  │                                                                  │   │
│  │  Consumed by: segmentation, diagnostics, Streamlit app.          │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  Supporting marts also written to data/processed/:                      │
│  ├── mart_report_daily_adoption.csv   base daily views + rolling cols   │
│  ├── mart_user_engagement.csv         user-level engagement summary     │
│  └── mart_report_performance.csv      load-time summary per report-date │
└──────────┬───────────────────────────────────────┬──────────────────────┘
           │  Notebook 05                          │  Notebooks 06–07
           │  src/pipelines/                       │  src/analytics/
           ▼                                       ▼
┌────────────────────────────┐      ┌──────────────────────────────────────┐
│  LAYER 4a — FORECASTING    │      │  LAYER 4b — ANALYTICS                │
│                            │      │                                      │
│  Reads: mart_report_daily  │      │  Reads: mart_report_daily_context    │
│  _series.csv only.         │      │  Writes: segments, diagnostics,      │
│  Strips non-target cols.   │      │          engagement metrics,         │
│  Validates before fitting. │      │          user analytics.             │
│  Gates on data sufficiency │      │                                      │
│  after series is built.    │      │  outputs/segments/                   │
│                            │      │  outputs/diagnostics/                │
│  outputs/forecasts/        │      │  outputs/metrics/                    │
│  outputs/metrics/          │      └──────────────────────┬───────────────┘
└──────────┬─────────────────┘                             │
           │                                               │
           └──────────────────────┬────────────────────────┘
                                  │  Notebook 08
                                  │  src/genai/
                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 5 — INSIGHT CONTEXT + GENAI                                      │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  mart_report_insight_context            ← GENAI / STREAMLIT      │   │
│  │                                                                  │   │
│  │  Grain: one row per report_id.                                   │   │
│  │  Joins: report-level features, segment assignments, diagnostic   │   │
│  │  flags, and (when available) forecast reliability metrics.       │   │
│  │  Built after forecasting so it can include forecast_reliable.    │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  outputs/insights/report_ai_insights.json                               │
│  outputs/insights/report_ai_insights.md                                 │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Mart Boundary Rules

### Rule 1 — Forecasting inputs are narrow

`mart_report_daily_series` exposes exactly five columns:
`report_id`, `date`, `daily_views`, `is_observed_day`, `is_imputed_zero`.

`standardise_forecasting_columns` strips any additional columns from wider source
tables before the frame reaches `validate_forecasting_series_input`.
`adapt_to_forecasting_schema` then translates to the internal legacy column names.
No engagement or performance column crosses any of those boundaries.

### Rule 2 — Engagement and performance columns are diagnostic context

Features built from `fact_page_views` (e.g. `top_1_user_view_share`,
`top_10pct_user_share`, `repeat_user_rate`) and from `fact_report_loads` (e.g.
`avg_load_time`, `p90_load_time`) are written to `mart_report_daily_context`.
They are consumed by segmentation, diagnostics, and the Streamlit app, but are
never included in the ARIMA fit unless explicitly re-approved as valid exogenous
SARIMAX variables in a future phase.

### Rule 3 — Zero-fill happens at the series level, not the event level

Event fact tables (`fact_report_views`, `fact_page_views`, `fact_report_loads`)
never contain fabricated rows. Zero-view days are represented only in
`mart_report_daily_series` via `is_imputed_zero = 1` rows created during the
`build_report_daily_series` step in Notebook 04.

### Rule 4 — Data sufficiency gating follows series construction

`filter_by_data_criteria` is called after `build_daily_series_for_all_reports`
has constructed the complete zero-filled series. Zeros within a passing series
are never removed; sparse reports are excluded in full.

### Rule 5 — Active period is anchored to dim_report

`launch_date` and `retire_date` in `dim_report` define the active window for each
report. Synthetic data: reports begin at a staggered launch date and retire_date
is null for still-active reports. Days before `launch_date` and after
`retire_date` are excluded from the daily series entirely.

---

## Source Module Map

| Module | Responsibility |
|---|---|
| `src/data/generate_synthetic_data.py` | Raw synthetic CSV tables |
| `src/data/build_semantic_model.py` | Dimensions + event facts |
| `src/data/validate_model.py` | Semantic model quality checks |
| `src/features/report_features.py` | `mart_report_daily_series`, rolling usage features |
| `src/features/engagement_features.py` | User engagement features (diagnostic context) |
| `src/features/performance_features.py` | Load-time features (diagnostic context) |
| `src/features/build_forecast_features.py` | `mart_report_daily_context` assembler; `mart_report_insight_context` assembler |
| `src/pipelines/run_forecasting_pipeline.py` | SARIMA pipeline; choose/standardise/validate/adapt boundary |
| `src/models/baselines.py` | Naive and seasonal-naive baselines |
| `src/models/evaluate.py` | MAE, RMSE, WAPE metrics |
| `src/analytics/` | Segmentation, diagnostics, user analytics |
| `src/genai/insight_generator.py` | Batch GenAI insight generation |

---

## Notebook to Mart Mapping

| Notebook | Primary outputs written to data/processed/ |
|---|---|
| 01 | raw tables in data/raw/ |
| 02 | dim_date, dim_user, dim_report, dim_page, fact_report_views, fact_page_views, fact_report_loads |
| 03 | validation results in outputs/validation/ |
| 04 | mart_report_daily_series (**SARIMA input**), mart_report_daily_context, mart_report_daily_adoption, mart_user_engagement, mart_report_performance |
| 05 | forecast outputs in outputs/forecasts/ and outputs/metrics/ |
| 06–07 | analytics outputs in outputs/segments/, outputs/diagnostics/, outputs/metrics/ |
| 08 | mart_report_insight_context; AI insights in outputs/insights/ |

---

## Future Improvements

- Rolling-origin backtesting for stronger forecast validation.
- Calendar regressors (holidays, known events) as explicitly approved SARIMAX exogenous inputs.
- Optional open-source forecasting model comparison (Prophet, statsforecast).
- GenAI evaluation for insight quality and consistency.
- Streamlit reviewer app with forecast exploration, behavioural diagnostics, and AI insight tabs.
