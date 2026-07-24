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

6. `notebooks/06_report_analytics.ipynb`
   - Builds report-level analytics, segmentation, and diagnostics.
   - Writes outputs to `outputs/segments/`, `outputs/diagnostics/`, and `outputs/metrics/`.

7. `notebooks/07_user_analytics.ipynb`
   - Builds user-level engagement features and segmentation outputs.
   - Writes outputs to `outputs/segments/` and `outputs/metrics/`.

8. `notebooks/08_genai_insights.ipynb`
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
- `insight_generator.py` reads the latest report forecast, metric, segment, and diagnostic CSVs
  and writes structured insights to `outputs/insights/`.

## Current Outputs

- `outputs/forecasts/` stores forecast outputs.
- `outputs/metrics/` stores model performance and comparison outputs.
- `outputs/segments/` stores report and user segmentation outputs.
- `outputs/diagnostics/` stores diagnostic rule outputs.
- `outputs/insights/` stores GenAI-generated insight outputs.
- `outputs/validation/` stores validation and reconciliation outputs.

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

## Roadmap

1. Improve forecast evaluation with rolling-origin backtesting.
2. Add calendar regressors as approved exogenous SARIMAX inputs.
3. Add a stronger model governance table.
4. Add optional open-source forecasting model comparison.
5. Add GenAI output evaluation or prompt quality checks.

