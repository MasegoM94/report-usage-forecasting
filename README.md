# Power BI Usage Intelligence: Forecasting, Behavioural Analytics, and GenAI Insights

This project explores how synthetic Power BI-style usage data can be turned into practical intelligence for analytics teams. The current version includes a notebook-first forecasting baseline, behavioural analytics, report and user segmentation, diagnostics, and a lightweight batch GenAI insight layer built from shareable synthetic data.

The repository is structured so the workflow can be reviewed through notebooks or regenerated through Python scripts, while leaving a clean foundation for future modelling, evaluation, and demo work.

## Project Overview

The notebooks demonstrate an end-to-end workflow for report usage forecasting, behavioural analytics, and GenAI-assisted insight generation:

- Generates synthetic report usage data with weekly patterns, trend, noise, and zero-activity days.
- Builds a clean semantic model from raw telemetry-style tables.
- Validates the semantic model before feature engineering.
- Builds a canonical daily report series (`mart_report_daily_series`) with one row per active report-date, zero-filling missing active days.
- Builds diagnostic context marts for engagement and performance (`mart_report_daily_context`).
- Applies data sufficiency checks after the complete daily series is built.
- Trains per-report Auto-ARIMA models on the univariate daily-views series.
- Compares model performance against naive and seasonal-naive baselines.
- Builds report and user analytics outputs, including segmentation and diagnostics.
- Publishes forecast, metrics, segment, diagnostic, validation, and insight outputs for downstream review.

The project is designed as a portfolio-friendly version of a realistic analytics problem, without exposing private Power BI or organisational usage data.

## Business Problem

Analytics teams often know which Power BI reports exist, but not which ones are becoming more important, which ones are losing engagement, or where future demand may require support. A usage intelligence workflow can help answer questions such as:

- Which reports are likely to see higher demand over the next month?
- Which reports have stable enough usage patterns to forecast responsibly?
- Which reports should be monitored because their usage is volatile, declining, or difficult to predict?
- How can GenAI summaries help stakeholders understand changes in report behaviour?

The current project includes the forecasting feature layer, behavioural analytics outputs, performance telemetry features, and a lightweight batch GenAI insight layer. Richer modelling beyond the baseline remains a planned extension.

## Architecture

The pipeline separates three concerns — modelling inputs, diagnostic context, and insight context — at the mart layer:

```text
Raw event facts (event-level, no fabricated zeros)
        |
        v
Semantic model (dimensions + facts)
        |
        +---> mart_report_daily_series      <- SARIMA input: report_id, date, daily_views
        |     (one row per active report-date; missing active days filled with zero)
        |
        +---> mart_report_daily_context     <- Diagnostic context: wide engagement +
        |     (joins adoption, engagement,     performance columns; NOT passed to SARIMA)
        |      and performance features)
        |
        v
Forecasting pipeline
  - consumes mart_report_daily_series only
  - strips all non-target columns before fitting
  - gates on data sufficiency after series is built
        |
        v
Forecast + metrics outputs
        |
        +---> mart_report_insight_context   <- GenAI / Streamlit context: joins forecast
              (post-forecast summary per         reliability with report-level features
               report_id)
```

See [docs/architecture.md](docs/architecture.md) for the full pipeline tree and mart boundary rules.

## What Makes This Project Different

This is not just a time-series notebook. The aim is to show how forecasting can become part of a broader usage intelligence product:

- **Forecasting:** univariate SARIMA on a clean zero-filled daily series with defensible baseline comparisons.
- **Behavioural analytics:** feature marts for repeat use, concentration, inactivity gaps, and page-depth proxies.
- **Performance telemetry:** feature marts for load-time levels, tails, and rolling performance signals.
- **GenAI insight layer:** lightweight batch-generated report summaries that explain forecast changes, risks, and stakeholder actions in plain language.
- **Operational thinking:** outputs include schema-safe tables, forecast history, realised-error backfill, and data-sufficiency diagnostics.

The GenAI layer is intentionally lightweight in Version 0.1. It reads existing CSV outputs and writes structured report-level insights without adding a chatbot, vector database, or app layer.

## Repository Structure

```text
report-usage-forecasting/
├── data/
│   ├── raw/                      # Synthetic raw telemetry-style CSV tables
│   └── processed/                # Clean semantic model CSV tables and feature marts
│       ├── mart_report_daily_series.csv     # Canonical SARIMA input (5 cols)
│       ├── mart_report_daily_context.csv    # Wide diagnostic context mart
│       └── mart_report_insight_context.csv  # Post-forecast report-level summary
├── docs/
│   ├── architecture.md           # Pipeline tree and mart boundary rules
│   ├── data_model.md             # Semantic model table definitions
│   └── feature_engineering.md   # Feature definitions, null policy, active-period rules
├── notebooks/
│   ├── 01_generate_raw_tables.ipynb
│   ├── 02_build_semantic_model_csv.ipynb
│   ├── 03_validate_semantic_model_hybrid_gx_csv.ipynb
│   ├── 04_feature_engineering.ipynb
│   ├── 05_forecasting_baseline.ipynb
│   ├── 06_report_analytics.ipynb
│   ├── 07_user_analytics.ipynb
│   └── 08_genai_insights.ipynb
├── outputs/
│   ├── validation/               # Validation results and reconciliation outputs
│   ├── forecasts/                # Latest forecasts and forecast history
│   │   ├── production_forecasts_history.csv   # Canonical production forecast log (append-only)
│   │   └── forecasts_history.csv              # Legacy forecast log (read by migration only)
│   ├── metrics/                  # Latest metrics, model comparisons, and realized history
│   │   └── realized_forecast_history.csv      # CANONICAL realized forecast history (24-column schema)
│   │                                          # Source of truth for all post-hoc accuracy monitoring.
│   │                                          # realized_errors_history.csv is LEGACY — do not read it.
│   ├── archive/                  # Retired files (e.g. migrated realized_errors_history snapshots)
│   ├── segments/                 # Report and user segmentation outputs
│   ├── diagnostics/              # Diagnostic rule outputs
│   ├── insights/                 # Batch-generated GenAI insight outputs
│   └── anomalies/                # Optional anomaly outputs placeholder
├── src/
│   ├── data/
│   │   ├── generate_synthetic_data.py
│   │   ├── build_semantic_model.py
│   │   └── validate_model.py
│   ├── features/
│   │   ├── report_features.py        # Daily adoption series + rolling usage features
│   │   ├── engagement_features.py    # User engagement features (diagnostic context)
│   │   ├── performance_features.py   # Load-time features (diagnostic context)
│   │   └── build_forecast_features.py  # mart_report_daily_context assembler
│   ├── models/
│   │   ├── baselines.py
│   │   └── evaluate.py
│   ├── analytics/
│   │   ├── report_features.py
│   │   ├── report_segmentation.py
│   │   ├── report_diagnostics.py
│   │   ├── user_features.py
│   │   └── user_segmentation.py
│   ├── genai/
│   │   ├── prompts.py
│   │   └── insight_generator.py
│   └── pipelines/
│       ├── run_forecasting_pipeline.py
│       ├── run_report_analytics_pipeline.py
│       └── run_user_analytics_pipeline.py
├── tests/
│   ├── test_feature_engineering_integration.py
│   ├── test_forecasting_pipeline_smoke.py
│   └── test_temporal_leakage.py
├── .gitignore
├── LICENSE
├── README.md
└── requirements.txt
```

## How To Run

From the project root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
jupyter notebook notebooks/01_generate_raw_tables.ipynb
```

Then run the notebooks in order. Generated CSV outputs are written to `data/raw/`, `data/processed/`, and the project-level `outputs/` folder.

To open the lightweight reviewer app after outputs have been generated:

```bash
streamlit run src/app/streamlit_app.py
```

## Running the Data Pipeline

The data pipeline can be run in two ways:

- **Notebooks** for exploration, transparency, and storytelling.
- **Python scripts** for a repeatable CSV-based pipeline.

Pipeline flow:

```text
data/raw/
    -> data/processed/                         (semantic model)
    -> data/processed/mart_report_daily_series.csv   (SARIMA input, produced by Notebook 04)
    -> outputs/validation/                     (data quality results)
    -> outputs/forecasts/ + outputs/metrics/   (produced by Notebook 05)
```

### Option 1 — Run via Notebooks (Recommended for exploration)

Use this path when you want to inspect the logic, understand the modelling choices, or walk through the workflow step by step.

Run the notebooks in this order:

1. `notebooks/01_generate_raw_tables.ipynb`
   - Generates synthetic raw telemetry-style tables.
   - Writes CSV files to `data/raw/`.

2. `notebooks/02_build_semantic_model_csv.ipynb`
   - Builds clean dimension and fact tables.
   - Writes CSV files to `data/processed/`.

3. `notebooks/03_validate_semantic_model_hybrid_gx_csv.ipynb`
   - Runs data quality checks using Great Expectations and pandas.
   - Writes validation outputs to `outputs/validation/`.

4. `notebooks/04_feature_engineering.ipynb`
   - Builds the canonical daily series (`mart_report_daily_series`), the wide diagnostic
     context mart (`mart_report_daily_context`), and supporting feature marts.
   - Applies zero-fill for missing active days; never inserts fabricated events into fact tables.
   - Writes feature tables to `data/processed/`.

5. `notebooks/05_forecasting_baseline.ipynb`
   - Reads `data/processed/mart_report_daily_series.csv` as the canonical SARIMA input.
   - Strips all engagement and performance columns before fitting — only `daily_views` reaches the model.
   - Applies data sufficiency gating after the complete series is built.
   - Trains the forecasting baseline and writes model outputs to `outputs/`.

6. `notebooks/06_model_diagnostics.ipynb` *(Sprint 5)*
   - Loads all 19 diagnostic output files from `outputs/diagnostics/`.
   - Explores autocorrelation, bias stability, outlier, distribution, and interval calibration diagnostics.
   - Displays the consolidated model-health summary from `report_model_diagnostics_latest.csv`.
   - Read-only: does not rerun the pipeline, generate synthetic data, or alter model selection.

7. `notebooks/06_report_analytics.ipynb`
   - Builds report-level analytics, segmentation, and diagnostics.
   - Writes outputs to `outputs/segments/`, `outputs/diagnostics/`, and `outputs/metrics/`.

8. `notebooks/07_user_analytics.ipynb`
   - Builds user-level engagement features and segmentation outputs.
   - Writes outputs to `outputs/segments/` and `outputs/metrics/`.

9. `notebooks/08_genai_insights.ipynb`
   - Reads forecast, model performance, segment, and diagnostic CSV outputs.
   - Writes AI insight outputs to `outputs/insights/`.

### Option 2 — Run via Python Scripts (Reproducible pipeline)

Use this path when you want to regenerate the pipeline outputs consistently from the command line.

From the project root, run:

```bash
python src/data/generate_synthetic_data.py
python src/data/build_semantic_model.py
python src/data/validate_model.py
python -m src.pipelines.run_forecasting_pipeline
python -m src.pipelines.run_report_analytics_pipeline
python -m src.pipelines.run_user_analytics_pipeline
python -m src.genai.insight_generator
```

The scripts perform the same core workflow as the notebooks:

- `generate_synthetic_data.py` creates raw synthetic tables in `data/raw/`.
- `build_semantic_model.py` builds cleaned dimensions and fact tables in `data/processed/`.
- `validate_model.py` runs validation checks and writes results to `outputs/validation/`.
- `run_forecasting_pipeline.py` prefers `data/processed/mart_report_daily_series.csv` as its input.
  Falls back to `mart_report_daily_context.csv` and older processed tables when the canonical file
  is absent. Engagement and performance columns are stripped before fitting regardless of source.
  Writes forecast outputs to `outputs/forecasts/` and metrics outputs to `outputs/metrics/`.
- `run_report_analytics_pipeline.py` writes report segments and diagnostics to `outputs/segments/`
  and `outputs/diagnostics/`.
- `run_user_analytics_pipeline.py` writes user engagement features and user segments to
  `outputs/metrics/` and `outputs/segments/`.
- `insight_generator.py` reads `outputs/analytics/mart_report_analytics.csv` as its sole
  analytics input and writes structured, lineage-enriched insights to
  `outputs/insights/report_ai_insights.json`. `portfolio_insights.py` reads the same mart
  and writes the portfolio-level insight to `outputs/insights/portfolio_ai_insight.json`.

## Current Outputs

- `outputs/forecasts/` stores forecast outputs.
- `outputs/metrics/` stores model performance and comparison outputs.
- `outputs/segments/` stores report and user segmentation outputs.
- `outputs/diagnostics/` stores diagnostic rule outputs.
- `outputs/analytics/` stores the canonical analytics mart (`mart_report_analytics.csv`, `mart_report_engagement.csv`).
- `outputs/insights/` stores GenAI-generated insight outputs (report-level and portfolio-level).
- `outputs/evaluation/` stores GenAI evaluation results and regression summaries.
- `outputs/validation/` stores validation and reconciliation outputs.

## Streamlit Application

```bash
streamlit run src/app/streamlit_app.py
```

### Canonical inputs (Sprint 9+)

The app reads the following outputs. Missing optional files produce a graceful empty state — the app does not crash.

| Key | File | Required | Purpose |
|-----|------|----------|---------|
| `report_analytics` | `outputs/analytics/mart_report_analytics.csv` | Optional | Canonical report-level mart: one row per report, analytics run ID, as-of date, status, priority, recommended action |
| `engagement` | `outputs/analytics/mart_report_engagement.csv` | Optional | Report-level engagement aggregates: returning-user share, concentration, active days, privacy suppression flags |
| `insights` | `outputs/insights/report_ai_insights.json` | Optional | Report-level GenAI insights (Sprint 8 fields + legacy aliases) |
| `forecasts` | `outputs/forecasts/report_view_forecasts_latest.csv` | Optional | Legacy forecast output (replaced by `production_forecasts_latest.csv` when present) |
| `metrics` | `outputs/metrics/report_view_metrics_latest.csv` | Optional | Forecast accuracy metrics and reliability flags |
| `segments` | `outputs/segments/report_segments.csv` | Optional | Report segment labels |
| `diagnostics` | `outputs/diagnostics/report_diagnostics.csv` | Optional | Diagnostic flag columns |

### Forecast date normalization

Production forecast outputs use `forecast_date`; legacy outputs use `Date`. `normalize_forecast_date()` in `src/app/utils/load_data.py` promotes `forecast_date` → `Date` when `Date` is absent, so both sources work with the same chart code.

### Caching

`load_app_data()` is decorated with `@st.cache_data` when running inside a Streamlit session. File reads and normalization are cached for the duration of the session. To force a full reload during development, press **Shift + ⟳ Rerun** in the browser, or call `st.cache_data.clear()` from a Python console. Outside Streamlit (tests, scripts) the decorator is a no-op.

### Privacy suppression

Engagement fields that cannot be shown due to small user populations are displayed as **Suppressed (privacy)** — they are never shown as zero. The `privacy_suppression_status`, `*_privacy_suppressed`, and `privacy_suppressed_fields` columns in the engagement mart control this behaviour.

## Sprint 5: Model Diagnostics

Sprint 5 adds a post-selection diagnostic layer that evaluates model health
**after** the rolling-origin backtesting pipeline has selected a model for each
report. Diagnostics do not alter model selection; MASE remains the primary
selection metric.

### Purpose

To answer the question: *given the model that was selected by MASE, how well
does it behave in practice?*  Diagnostics reveal systematic patterns that
accuracy metrics alone may miss, such as autocorrelated residuals, directional
bias, distributional non-normality, and interval miscalibration.

### Three residual sources

| Source | File | Notes |
|--------|------|-------|
| Training residuals | `training_residuals_latest.csv` | In-sample fit; may be optimistic |
| Backtest forecast errors | `backtest_forecast_errors_latest.csv` | Out-of-sample; primary source |
| Production forecast errors | `production_forecast_errors_latest.csv` | Realized operational; may be sparse early |

All three use `residual = actual - forecast` (positive = underforecast).

### Five diagnostic components

1. **Autocorrelation** — ACF, Ljung-Box, Durbin-Watson on residuals.
2. **Bias stability** — mean/median/normalized bias per fold and horizon bucket.
3. **Outlier detection** — MAD robust z-scores, outlier rate, largest miss.
4. **Distribution shape** — skewness, kurtosis, Jarque-Bera, Shapiro-Wilk.
5. **Interval calibration** — coverage, coverage gap, Winkler score, lower/upper miss rates.

### Consolidated output

`outputs/diagnostics/report_model_diagnostics_latest.csv` — one row per report.

| Status | Meaning |
|--------|---------|
| `healthy` | No component in poor/warning status; sufficient evidence |
| `watch` | One or more warning-level issues |
| `poor` | At least one critical issue or multiple poor signals |
| `insufficient_evidence` | Fewer than 2 valid backtest folds or no production evidence |
| `calculation_failed` | Diagnostic calculation failed |

### No automatic retraining

`automatic_retraining_triggered` is always `False` in Sprint 5.
The `consider_retraining` recommended action is a signal for human review only.

### Notebook

`notebooks/06_model_diagnostics.ipynb` — read-only exploration of all 19
diagnostic output files. Requires the pipeline to have been run first.

See [docs/model_diagnostics_methodology.md](docs/model_diagnostics_methodology.md)
for methodology details and
[docs/data_dictionary_sprint5.md](docs/data_dictionary_sprint5.md) for column
definitions.

## GenAI Insight Layer

Version 0.1 adds a batch-generated report insight layer under `src/genai/`.

Expected inputs:

- `outputs/forecasts/report_forecasts.csv`
- `outputs/metrics/model_performance.csv`
- `outputs/segments/report_segments.csv`
- `outputs/diagnostics/report_diagnostics.csv`

## Streamlit Reviewer App

The demo app reads the generated CSV and JSON outputs directly. It includes an overview page for user adoption, report adoption, at-risk reports, and forecast reliability, plus tabs for forecast exploration, behavioural diagnostics, and AI-generated report insights.

Run from the project root:

```bash
streamlit run src/app/streamlit_app.py
```

For compatibility with the current forecasting pipeline, the generator also recognises `report_view_forecasts_latest.csv`, `report_view_metrics_latest.csv`, and `report_model_comparison_latest.csv`.

Outputs:

- `outputs/insights/report_ai_insights.json`
- `outputs/insights/report_ai_insights.md`

To use an OpenAI model, set `OPENAI_API_KEY` in your environment before running the script. Do not store API keys in the repository. If `OPENAI_API_KEY` is missing, the script generates deterministic rule-based placeholder insights so the notebook and command-line workflow still run.

### Why This Structure?

- Separates raw telemetry-style event data (no fabricated zeros) from derived daily summary tables.
- Keeps forecasting inputs narrow — only the daily-views series reaches ARIMA.
- Keeps diagnostic context separate — engagement and performance features are available for
  segmentation and GenAI summaries but never influence the ARIMA fit.
- Mirrors a real-world analytics engineering workflow with a clean mart layer boundary.
- Supports both experimentation and reproducibility.

## Current Status

Implemented:

- Synthetic Power BI-style usage dataset.
- Semantic model build.
- Hybrid validation using Great Expectations and pandas checks.
- Canonical daily report series (`mart_report_daily_series`) with zero-fill for missing active days.
- Wide diagnostic context mart (`mart_report_daily_context`) with engagement and performance features.
- Forecasting baseline consuming `mart_report_daily_series` with naive and seasonal-naive comparisons.
- Data sufficiency gating applied after the complete series is built.
- Report analytics, user analytics, diagnostics, and segmentation.
- Batch GenAI insight layer.
- Lightweight Streamlit reviewer app.
- Integration tests for the feature-engineering pipeline and forecasting pipeline smoke tests.

Planned next:

- Improve forecast evaluation with rolling-origin backtesting.
- Add calendar regressors (holidays, known events) as explicitly approved exogenous SARIMAX inputs.
- Add a stronger model governance table.
- Add optional open-source forecasting model comparison.
- Add GenAI output evaluation or prompt quality checks.

## Concurrency and Storage Limitations

The history persistence layer (`append_forecasts_history`, `append_metrics_history`,
`write_realized_forecast_history`, `migrate_legacy_realized_errors`) uses append-only
CSV files with a read-check-append-write pattern that is **not transactional**.

| Scope | Status |
|---|---|
| Two threads in the same Python process | **Guarded** — `threading.Lock` per file path via `src/persistence/_csv_lock.py` |
| Two separate OS processes | **Not coordinated** — no cross-process locking |
| CSV writes | **Not transactional** — partial writes or interleaved appends are possible under concurrent processes |

### What this means in practice

- A single scheduled pipeline run is safe.
- Running two pipeline instances in parallel (e.g., manual trigger while a scheduled job runs) can produce duplicate or lost rows.
- CI/CD parallel test workers that write to the same output directory will corrupt history files (tests use `tmp_path` to avoid this).

### Production deployment recommendation

For environments where multiple pipeline processes may run concurrently, replace CSV persistence with one of:

- A transactional relational database (PostgreSQL, SQLite with WAL mode)
- Delta Lake or Apache Iceberg tables (ACID append semantics)
- Database upserts keyed on the deduplication key
- Orchestrator-enforced mutual exclusion (Airflow, Prefect, Dagster, etc.)
- An explicit cross-process file-locking library (`filelock`, `fcntl.flock`)

## User Analytics Privacy

This project applies privacy-by-design to all user-level analytics outputs.

### Restricted identity layer

`data/processed/dim_user.csv` maps `user_key` to `user_id` (email address) and `unique_user` (display name). This file is **restricted identity data**:

- Must not be joined into any public analytics output.
- Must not be loaded into the Streamlit app.
- Must not be used in any GenAI context.
- Access requires explicit data-governance approval.

### Privacy-safe analytics layer

`outputs/metrics/user_features.csv` and `outputs/segments/user_segments.csv` contain only `user_key` (a stable surrogate key, e.g. `UK_0001`). No email addresses or display names are included.

These files are marked **pseudonymous — user_key only**.

### Small-group suppression

Distribution metrics (e.g. median views per user) are suppressed when `unique_users < 5`. Suppressed values are set to `None` (null), never `0`. The columns `privacy_suppressed`, `privacy_suppression_reason`, and `suppressed_fields` are added to suppressed outputs.

### Streamlit app

The Streamlit reviewer app does not load user-level behavioural files (`user_features.csv`, `user_segments.csv`, `dim_user.csv`). It operates on report-level aggregates only.

### GenAI insight layer

The GenAI layer receives only report-level aggregates. No user identifiers are passed to any LLM prompt.

### Canonical engagement definitions

See `src/analytics/engagement_definitions.py` for the authoritative definitions of all engagement metrics.

Key definitions:
- **Returning user**: active on at least 2 distinct dates within a window.
- **One-time user**: active on exactly 1 distinct date within a window.
- **Repeat-view user**: more than 1 total view within a window (regardless of dates).
- **Lifetime returned flag**: user has any activity after their first-ever use date.

### Deprecated fields

The following fields are deprecated and must not be used in Sprint 6+ outputs:

| Deprecated field | Replacement |
|---|---|
| `repeat_rate` | `returning_user_share_28d` |
| `is_repeat_user` | `lifetime_returned_flag` |
| `repeat_usage_flag` | `lifetime_returned_flag` |

## Roadmap

1. Improve forecast evaluation with rolling-origin backtesting.
2. Add calendar regressors as approved exogenous SARIMAX inputs.
3. Add a stronger model governance table.
4. Add optional open-source forecasting model comparison.
5. Add GenAI output evaluation or prompt quality checks.


## Sprint 6 — User Engagement and Adoption Analytics

Sprint 6 adds a complete, privacy-safe user engagement analytics layer. All outputs are
**report-level** — no individual user data, email addresses, or direct identifiers appear
in any analytics output file.

### What Sprint 6 produces

**Privacy-safe report-user-day mart** (`mart_report_user_daily`)
- Pseudonymous user keys only (`user_key`); never joined to `dim_user.csv`
- One row per (report, user_key, usage_date) with positive usage
- Includes lifetime history fields (`first_report_use_date`, `lifetime_returned_flag`)
- Source quality classification applied before mart build

**Complete observation windows**
- 7-day, 28-day, previous 28-day, 90-day windows anchored to the data as-of date
- Deterministic window computation from mart max date (not `datetime.now()`)
- History sufficiency checks gate metric computation per report per window

**Active-user breadth metrics**
- Unique users per window (7d, 28d, previous 28d, 90d)
- Active-user direction: growing, stable, declining
- Returning user share and one-time user share (canonical returning = 2+ distinct dates)

**Engagement cohorts**
- Newly adopted, retained, reactivated, lapsed, and unclassified recent users
- Lapse rate over previous cohort denominator; all other shares over recent cohort denominator
- Requires comparison window sufficiency (both recent and previous 28d covered)

**Frequency and intensity**
- Views per active user, views per user-day, median active days per user
- Return-gap analysis (mean and median days between visits for returning users)

**Concentration and HHI**
- Top-1, top-3, and top-10% user view shares
- Herfindahl-Hirschman Index (HHI) and effective user count
- Suppressed for small groups (below `PrivacyConfig.MIN_GROUP_SIZE`)

**Final engagement mart** (`mart_report_engagement`)
- One row per report with full engagement status classification
- Priority-ordered status hierarchy (14 possible statuses)
- Standardised recommended action (no retire/delete recommendations)
- Plain-language engagement reasons for downstream consumers

### Notebook

Walkthrough of all Sprint 6 analytics:
```
notebooks/07_user_analytics.ipynb
```

### No GenAI or Streamlit integration in Sprint 6

Sprint 6 produces CSV outputs only. The GenAI insight layer and Streamlit dashboard
are planned for a future sprint. `mart_report_engagement` is designed as input for
those consumers but does not integrate with them yet.

### No report-retirement decisions

No Sprint 6 output recommends retiring, deleting, or restricting access to any report.
Engagement volume does not determine business value. All recommended actions are limited
to monitoring, investigation, and data quality review.

### Documentation

| File | Contents |
|------|----------|
| `docs/data_dictionary_sprint6.md` | Column-level definitions for all 9 Sprint 6 output files |
| `docs/user_analytics_methodology.md` | Metric formulas, cohort denominators, suppression policy |
| `docs/deprecated_fields_sprint6.md` | Deprecated fields and their Sprint 6 replacements |

---

## Sprint 7 — Report Analytics and Decision Support

Sprint 7 builds the report analytics layer that combines all prior sprint outputs into
a canonical decision-support mart for report-level review and action.

### What Sprint 7 builds

Seven analytics layers joined into one mart:

1. **Historical usage context** — 28d/90d trends, inactivity streaks, volatility, anomaly evidence
2. **Forecast outlook** — 28d horizon direction, uncertainty status, actual vs. forecast comparison
3. **Model health context** — backtest diagnostics, production evidence maturity, interpretation guidance
4. **Engagement context** — breadth, repeat, lapse cohorts, frequency, concentration (privacy-gated)
5. **Metadata context** — explicit dim_report fields only; completeness score; nothing inferred from usage
6. **Deterministic diagnostics** — 14-step precedence, evidence-gated risk flags, review triggers
7. **Dimensional segmentation** — 7 independent dimensions + 15-step primary segment precedence

Output: `mart_report_analytics.csv` — one row per report, 305 columns.

### Key design principles

- **Evidence gating:** risk flags are null when evidence is insufficient — not False
- **No inference from usage:** cadence, criticality, and business value are never inferred from view counts
- **Deterministic precedence:** primary diagnostic and primary segment follow strict ordered rules
- **Privacy suppression:** concentration metrics suppressed when unique_users < 5; suppressed values never treated as zero
- **No prohibited actions:** retire_report, delete_report, automatically_retrain, change_selected_model, restrict_user, and contact_specific_user are never produced
- **Source-derived dates:** analytics_as_of_date = max(usage_date) from source mart; never date.today()

### Output files

| File | Grain | Rows | Columns |
|---|---|---|---|
| `outputs/metrics/report_features.csv` | report_id | 30 | 78 |
| `outputs/analytics/report_forecast_outlook.csv` | report_id | 30 | 74 |
| `outputs/analytics/report_model_health_context.csv` | report_id | 30 | 34 |
| `outputs/analytics/report_engagement_context.csv` | report_id | 30 | 54 |
| `outputs/analytics/report_metadata_context.csv` | report_id | 30 | 40 |
| `outputs/analytics/report_diagnostics.csv` | report_id | 30 | 47 |
| `outputs/analytics/report_segments.csv` | report_id | 30 | 16 |
| `outputs/analytics/mart_report_analytics.csv` | report_id | 30 | 305 |

### Notebook

```
notebooks/08_report_analytics.ipynb
```

15 sections: Business Objective, Architecture, Historical Usage, Forecast Outlook, Model Health,
Engagement, Metadata and Lifecycle, Diagnostics, Segmentation, Canonical Mart, Case Studies,
Evidence and Privacy Limitations, Action Policy, Relationship to Later Sprints, Limitations.

### Test coverage

| Test file | Tests | Scope |
|---|---|---|
| `tests/test_report_analytics_notebook.py` | 30 | Notebook structure, data loading, prohibited content |
| `tests/test_report_analytics_pipeline_integration.py` | 40+ | File existence, spine preservation, temporal alignment, privacy, prohibited actions, determinism |

### GenAI (Sprint 8) and Streamlit (Sprint 9) not yet modified

Sprint 7 produces CSV outputs only. `mart_report_analytics.csv` is designed as the input for
Sprint 8 (GenAI narrative generation) and Sprint 9 (Streamlit dashboard), but neither is
modified in this sprint.

### Documentation

| File | Contents |
|------|----------|
| `docs/data_dictionary_sprint7.md` | Column-level definitions for all 8 Sprint 7 output files |
| `docs/report_analytics_methodology_sprint7.md` | Temporal policy, evidence gating, precedence lists, privacy suppression |
| `docs/deprecated_fields_sprint7.md` | Deprecated fields and their Sprint 7 replacements |

## Sprint 8 — GenAI Insight Layer

**Design principle:** The analytics layer calculates and decides; the GenAI layer explains.
This applies at both report level and portfolio level.

### Canonical input
`outputs/analytics/mart_report_analytics.csv` is the sole analytics source for all GenAI insights
(report-level and portfolio-level). Deterministic metrics, risk flags, statuses, and actions are
pre-computed by the Sprint 7 pipeline. The LLM converts them into stakeholder-friendly language —
it does not recalculate or reclassify. The legacy 4-CSV re-join is no longer used.

---

### Report-level insight flow (`src/genai/insight_generator.py`)

One structured insight is generated per report from the 34-field allowlist context.

**Context:** `build_mart_context()` extracts `INSIGHT_CONTEXT_ALLOWLIST` fields per row.
Privacy-suppressed values are passed as `null` with an explicit context note.

**Prompt:** `REPORT_INSIGHT_PROMPT_VERSION = "report_insight_v1"`.
The LLM returns validated JSON: `executive_summary`, `usage_insight`, `engagement_insight`,
`forecast_insight`, `model_confidence_note`, `recommended_action`, `evidence_limitations`.

**LLM responsibility:** restate deterministic statuses and actions in clear natural language.

**Deterministic responsibility:** calculate all metrics, choose the recommended action, assign
review priority, classify status. The LLM receives the completed result, not raw data.

**Canonical output:** `outputs/insights/report_ai_insights.json` (array, one entry per report).
Optional human-readable: `outputs/insights/report_ai_insights.md`.

---

### Portfolio-level insight flow (`src/genai/portfolio_insights.py`)

One structured management summary is generated for the portfolio as a whole.

**Aggregates computed deterministically** before any LLM call:
- Portfolio size and evidence completeness
- Historical usage distribution (growing / stable / declining / inactive)
- Forecast outlook distribution and uncertainty levels
- Model health coverage and common issues
- Engagement health (active-user breadth, lapse, retention, dependency)
- Decision support (status, review priority, recommended actions by count)
- Top risks and positive signals (deterministic summary strings)
- Attention shortlist (up to 5 reports with non-monitoring actions, ranked by priority then status)

**Context:** a structured dict of portfolio-level aggregates — no user-level data, no individual
report narratives, no suppressed-field reconstruction. Individual report names appear only in the
deterministic attention shortlist.

**Prompt:** `PORTFOLIO_INSIGHT_PROMPT_VERSION = "portfolio_insight_v1"`.
The LLM returns validated JSON: `executive_summary`, `portfolio_usage_summary`,
`portfolio_engagement_summary`, `portfolio_forecast_summary`, `portfolio_model_health_summary`,
`priority_actions`, `positive_signals`, `evidence_limitations`.

**LLM responsibility:** synthesise aggregate evidence into a management narrative. The LLM may
not rank reports, invent causes, name individual users, or recommend automated actions.

**Deterministic responsibility:** calculate every aggregate count and share, build the attention
shortlist, produce the top-risk and positive-signal strings, assign the deterministic fallback.

**Canonical output:** `outputs/insights/portfolio_ai_insight.json` (single dict).
Optional human-readable: `outputs/insights/portfolio_ai_insight.md`.

---

### Validation and fallback (both levels)

| Check | Report-level | Portfolio-level |
|---|---|---|
| Required schema fields | yes — 7 fields | yes — 8 fields |
| Prohibited action phrases | yes (retire, delete, retrain, replace model) | yes (same patterns) |
| User-identifier detection | warning only | warning only |
| Numerical grounding | ±5 pp / ±5 count tolerance | ±5 pp / ±5 count tolerance |
| Direction conflict | yes (usage/forecast/engagement) | — |
| Action category allowlist | — | yes (priority_actions mapped to mart values) |
| Evidence limitations required | — | yes when model health is insufficient |

A clear contradiction or unsupported material number invalidates the LLM response.
Both levels fall back to a deterministic rule-based summary in that case, or when the API
is unavailable or returns invalid JSON. One report failure does not block portfolio generation.
Portfolio failure does not remove report-level outputs.

### Hash reuse (both levels)

A SHA-256 hash of the sorted context + prompt version + model name is computed before each
generation. If the hash matches a previous successful output, the output is reused with no API call.
Failed or invalid prior outputs are never reused. A changed aggregate, prompt version, or model
triggers regeneration.

### Lineage fields (portfolio)

`analytics_run_id`, `analytics_as_of_date`, `genai_run_id`, `generated_at`,
`prompt_version`, `model_name`, `input_hash`, `generation_status`, `validation_status`,
`generation_error`, `report_count`.

### Privacy (both levels)

No user-level data, no individual user identifiers, no event-level records enter any LLM context.
The portfolio context contains only aggregated counts, shares, and status distributions.

### Current limitations

- Model diagnostic evidence is `insufficient_evidence` for all reports in the current synthetic
  dataset (production backtests have not been run). Portfolio model-health commentary is therefore
  limited to acknowledging this gap.
- Streamlit has not been modified to display portfolio insights (future sprint).

---

### GenAI Evaluation Framework (`src/genai/evaluation.py`)

**Design principle:** Forecast metrics evaluate the analytical model; groundedness and usefulness
evaluate the GenAI layer. These are separate concerns.

No LLM-as-judge. All automated evaluation checks are deterministic regex/value matching.

#### Evaluation dimensions

| Dimension | Type | Description |
|-----------|------|-------------|
| Completeness | Hard | All required schema fields present and non-empty |
| Safety | Hard | No prohibited action phrases (retire, delete, retrain, replace model) |
| Groundedness | Hard | Directional language AND numerical claims match context |
| Direction | Hard | Usage/forecast language matches `historical_usage_status` / `forecast_outlook_status` |
| Numerical | Hard | % and count claims within ±5 pp / ±5 of context values |
| Action alignment | Hard | Generated action aligns with deterministic `recommended_report_action` |
| Evidence disclosure | Hard | Model insufficiency, privacy suppression, forecast uncertainty disclosed when present |
| Readability | Soft (0–1) | Heuristic field-length and generic-phrase check; threshold ≥ 0.5 for overall pass |
| Conciseness | Soft (0–1) | Average word-count ratio vs. per-field limits; informational only |

**Overall pass rule:** all 7 hard dimensions True AND readability_score ≥ 0.5.
The automated pass gate is necessary but not sufficient for stakeholder usefulness —
human review (`docs/genai_evaluation_rubric.md`) is the authoritative quality signal.

#### Fixture-based evaluation set

`tests/fixtures/genai_evaluation_cases.json` — 15 report-level + 8 portfolio-level labelled cases:

- 12 report cases expected to **pass** (growing, declining, stable, inactive, privacy-suppressed,
  high-uncertainty, insufficient model health, missing metadata, conflicting signals, etc.)
- 3 report cases expected to **fail** (retirement language, hallucinated number, incorrect direction)
- 5 portfolio cases expected to **pass** (stable, elevated decline, high uncertainty, all-insufficient model evidence, partial privacy suppression)
- 3 portfolio cases expected to **pass** (shortlist capped, missing metadata, valid fallback)

Tests in `tests/test_genai_evaluation.py` run all fixture cases through the deterministic
evaluators and assert that the actual outcome matches `expected_validation_outcome`.

#### Golden regression testing

`tests/fixtures/genai_golden_outputs.json` — 10 golden configs (concept-level expectations only,
no exact text). Each config defines `required_concepts`, `prohibited_concepts`, `schema_fields`,
`evidence_limitations_keywords`, and (for report-level) `expected_action_keywords`. Used by
`compare_against_golden()` to verify that a regenerated insight preserves key semantic properties
after a prompt or model version change.

Two golden configs mark expected-fail cases (retirement language, hallucinated number) — used to
confirm the safety and numerical checks still catch those cases after any code change.

#### Regression against stored outputs

```python
from src.genai.evaluation import run_regression_against_stored_outputs
summary = run_regression_against_stored_outputs()
```

Loads `outputs/analytics/mart_report_analytics.csv`, `outputs/insights/report_ai_insights.json`,
and `outputs/insights/portfolio_ai_insight.json`. Evaluates every stored insight against its
mart context and writes timestamped results to `outputs/evaluation/`.

**Note on current stored outputs:** in this environment no API key is configured, so all stored
outputs are rule-based fallbacks. Rule-based evaluation confirms that the deterministic fallback
path produces valid, grounded, safe outputs. It does not validate the live LLM generation path.
LLM evaluation requires a separate run with `generation_status='success'` entries.

#### Human-review rubric

`docs/genai_evaluation_rubric.md` defines a 7-dimension 1–5 scoring rubric covering:
Factual Groundedness, Evidence Disclosure, Action Alignment, Readability, Conciseness,
Safety and Boundary Respect, and Stakeholder Usefulness. The rubric is used for periodic
human review and cannot be replaced by automated checks.

Recommended cadence: full rubric review on every prompt version change or model version change;
quarterly spot-check of 10 insights otherwise.
