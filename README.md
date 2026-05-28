# Power BI Usage Intelligence: Forecasting, Behavioural Analytics, and GenAI Insights

This project explores how synthetic Power BI-style usage data can be turned into practical intelligence for analytics teams. The current version includes a notebook-first forecasting baseline, behavioural analytics, report and user segmentation, diagnostics, and a lightweight batch GenAI insight layer built from shareable synthetic data.

The repository is structured so the workflow can be reviewed through notebooks or regenerated through Python scripts, while leaving a clean foundation for future modelling, evaluation, and demo work.

## Project Overview

The notebooks demonstrate an end-to-end workflow for report usage forecasting, behavioural analytics, and GenAI-assisted insight generation:

- Generates synthetic report usage data with weekly patterns, trend, noise, and zero-activity days.
- Builds a clean semantic model from raw telemetry-style tables.
- Validates the semantic model before feature engineering.
- Builds daily report-level time series from user-level usage records.
- Builds feature marts for adoption, engagement, performance, and final forecast features.
- Applies data sufficiency checks before modelling.
- Trains per-report Auto-ARIMA models.
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

The current project now includes the forecasting feature layer, behavioural analytics outputs, performance telemetry features, and a lightweight batch GenAI insight layer. Richer modelling beyond the baseline remains a planned extension.

## Simple Architecture

The current workflow is intentionally lightweight:

1. **Synthetic usage data** is generated into raw CSV tables.
2. **Semantic model build** creates cleaned dimensions and facts under `data/processed/`.
3. **Validation** checks the semantic model before downstream use.
4. **Feature engineering** in `notebooks/04_feature_engineering.ipynb` builds reusable marts under `data/processed/`.
5. **Forecasting baseline** in `notebooks/05_forecasting_baseline.ipynb` consumes `data/processed/mart_forecast_features.csv`, trains Auto-ARIMA models, and compares them with simple baselines.
6. **Report and user analytics** create segmentation, diagnostics, and engagement outputs.
7. **Output tables** are written to `outputs/` for forecasts, metrics, segments, diagnostics, validation, and insights.
8. **GenAI insights** read the output CSVs and publish structured report summaries under `outputs/insights/`.
9. **Future layers** may add richer modelling, reviewer-friendly demo views, and stronger evaluation checks.

See [docs/architecture.md](docs/architecture.md) for a small architecture note and future direction.

## What Makes This Project Different

This is not just a time-series notebook. The aim is to show how forecasting can become part of a broader usage intelligence product:

- **Forecasting:** predict future report usage and compare against defensible baselines.
- **Behavioural analytics:** implemented feature marts for repeat use, concentration, inactivity gaps, and page-depth proxies.
- **Performance telemetry:** implemented feature marts for load-time levels, tails, and rolling performance signals.
- **GenAI insight layer:** lightweight batch-generated report summaries that explain forecast changes, risks, and stakeholder actions in plain language.
- **Operational thinking:** current outputs already consider schema-safe tables, forecast history, and realised-error backfill concepts.

The GenAI layer is intentionally lightweight in Version 0.1. It reads existing CSV outputs and writes structured report-level insights without adding a chatbot, vector database, or app layer.

## Repository Structure

```text
report-usage-forecasting/
├── data/
│   ├── raw/                      # Synthetic raw telemetry-style CSV tables
│   └── processed/                # Clean semantic model CSV tables
├── docs/                         # Architecture and data model notes
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
│   ├── metrics/                  # Latest metrics, model comparisons, and error history
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
│   │   ├── report_features.py
│   │   ├── engagement_features.py
│   │   ├── performance_features.py
│   │   └── build_forecast_features.py
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
data/raw/ -> data/processed/ -> outputs/validation/
data/processed/mart_forecast_features.csv -> outputs/forecasts/ + outputs/metrics/
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
   - Builds report adoption, behavioural, performance, and final forecast feature marts.
   - Writes feature tables to `data/processed/`.

5. `notebooks/05_forecasting_baseline.ipynb`
   - Reads `data/processed/mart_forecast_features.csv`.
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
- `run_forecasting_pipeline.py` consumes `data/processed/mart_forecast_features.csv` when available, falls back to compatible processed report-level tables, and writes forecast outputs to `outputs/forecasts/` plus metrics outputs to `outputs/metrics/`.
- `run_report_analytics_pipeline.py` writes report segments and diagnostics to `outputs/segments/` and `outputs/diagnostics/`.
- `run_user_analytics_pipeline.py` writes user engagement features and user segments to `outputs/metrics/` and `outputs/segments/`.
- `insight_generator.py` reads the latest report forecast, metric, segment, and diagnostic CSVs and writes structured insights to `outputs/insights/`.

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

For compatibility with the current forecasting pipeline, the generator also recognizes `report_view_forecasts_latest.csv`, `report_view_metrics_latest.csv`, and `report_model_comparison_latest.csv`.

Run from the project root:

```bash
python -m src.genai.insight_generator
```

Outputs:

- `outputs/insights/report_ai_insights.json`
- `outputs/insights/report_ai_insights.md`

To use an OpenAI model, set `OPENAI_API_KEY` in your environment before running the script. Do not store API keys in the repository. If `OPENAI_API_KEY` is missing, the script generates deterministic rule-based placeholder insights so the notebook and command-line workflow still run.

### Why This Structure?

- Separates raw telemetry-style data from cleaned semantic model outputs.
- Mirrors a real-world analytics engineering workflow.
- Supports both experimentation and reproducibility.
- Makes the project easier to extend with forecasting features, behavioural analytics, and GenAI insight evaluation.

## Current Status

Implemented now:

- Synthetic Power BI-style usage dataset.
- Semantic model build.
- Hybrid validation using Great Expectations and pandas checks.
- Feature marts for report usage, engagement, performance, and forecasting.
- Forecasting baseline with naive and seasonal-naive comparisons.
- Report analytics.
- User analytics.
- Diagnostics.
- Segmentation.
- Batch GenAI insight layer.
- Lightweight Streamlit reviewer app.

Planned next:

- Add screenshots or sample output images to the README.
- Improve forecast evaluation with rolling-origin backtesting.
- Add a stronger model governance table.
- Add optional open-source forecasting model comparison.
- Add GenAI output evaluation or prompt quality checks.

## Roadmap

1. Add README screenshots or sample output images.
2. Improve forecast evaluation with rolling-origin backtesting.
3. Add a stronger model governance table.
4. Add optional open-source forecasting model comparison.
5. Add GenAI output evaluation or prompt quality checks.
