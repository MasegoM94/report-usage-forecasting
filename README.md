# Power BI Usage Intelligence: Forecasting, Behavioural Analytics, and GenAI Insights

This project explores how Power BI usage data can be turned into practical intelligence for analytics teams. The current version focuses on a notebook-first forecasting baseline that predicts daily report usage from synthetic, shareable data. The longer-term direction is to combine forecasting, behavioural analytics, and GenAI-assisted explanations into a lightweight decision-support workflow.

The repository is intentionally small at this stage. It is structured so the current notebook can be reviewed easily, while leaving a clean foundation for future modelling, evaluation, and documentation work.

## Project Overview

The current notebook demonstrates an end-to-end baseline workflow for report usage forecasting:

- Generates synthetic report usage data with weekly patterns, trend, noise, and zero-activity days.
- Builds daily report-level time series from user-level usage records.
- Applies data sufficiency checks before modelling.
- Trains per-report Auto-ARIMA models.
- Compares model performance against naive and seasonal-naive baselines.
- Publishes forecast, metrics, history, and realised-error style outputs for downstream review.

The project is designed as a portfolio-friendly version of a realistic analytics problem, without exposing private Power BI or organisational usage data.

## Business Problem

Analytics teams often know which Power BI reports exist, but not which ones are becoming more important, which ones are losing engagement, or where future demand may require support. A usage intelligence workflow can help answer questions such as:

- Which reports are likely to see higher demand over the next month?
- Which reports have stable enough usage patterns to forecast responsibly?
- Which reports should be monitored because their usage is volatile, declining, or difficult to predict?
- How could future GenAI summaries help stakeholders understand changes in report behaviour?

The current project focuses on the forecasting foundation. Behavioural segmentation and GenAI-generated insights are planned extensions, not completed features.

## Simple Architecture

The current workflow is intentionally lightweight:

1. **Synthetic usage data** is generated inside the notebook.
2. **Data quality checks** identify report series suitable for forecasting.
3. **Forecasting baseline** trains Auto-ARIMA models and compares them with simple baselines.
4. **Output tables** are written to `outputs/` for forecasts, metrics, history, and realised-error tracking.
5. **Future layers** may add behavioural analytics, GenAI narrative summaries, and production orchestration.

See [docs/architecture.md](docs/architecture.md) for a small architecture note and future direction.

## What Makes This Project Different

This is not just a time-series notebook. The aim is to show how forecasting can become part of a broader usage intelligence product:

- **Forecasting:** predict future report usage and compare against defensible baselines.
- **Behavioural analytics:** planned analysis of usage patterns, engagement changes, and report adoption signals.
- **GenAI direction:** planned narrative summaries that explain forecast changes, risks, and stakeholder actions in plain language.
- **Operational thinking:** current outputs already consider schema-safe tables, forecast history, and realised-error backfill concepts.

The GenAI layer is deliberately not implemented yet. It is included as a roadmap direction so the project can grow from a modelling exercise into a more complete analytics product.

## Repository Structure

```text
report-usage-forecasting/
├── data/
│   ├── raw/                      # Synthetic raw telemetry-style CSV tables
│   └── processed/                # Clean semantic model CSV tables
├── docs/                         # Architecture and data model notes
├── notebooks/
│   ├── 01_forecasting_baseline.ipynb
│   ├── 02_generate_raw_tables.ipynb
│   ├── 03_build_semantic_model_csv.ipynb
│   └── 04_validate_semantic_model_hybrid_gx_csv.ipynb
├── outputs/
│   └── validation/               # Validation results and reconciliation outputs
├── src/
│   └── data/
│       ├── generate_synthetic_data.py
│       ├── build_semantic_model.py
│       └── validate_model.py
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
jupyter notebook notebooks/01_forecasting_baseline.ipynb
```

Then run the notebook cells in order. Generated CSV outputs are written to the project-level `outputs/` folder.

## Running the Data Pipeline

The data pipeline can be run in two ways:

- **Notebooks** for exploration, transparency, and storytelling.
- **Python scripts** for a repeatable CSV-based pipeline.

Pipeline flow:

```text
data/raw/ -> data/processed/ -> outputs/validation/
```

### Option 1 — Run via Notebooks (Recommended for exploration)

Use this path when you want to inspect the logic, understand the modelling choices, or walk through the workflow step by step.

Run the notebooks in this order:

1. `notebooks/02_generate_raw_tables.ipynb`
   - Generates synthetic raw telemetry-style tables.
   - Writes CSV files to `data/raw/`.

2. `notebooks/03_build_semantic_model_csv.ipynb`
   - Builds clean dimension and fact tables.
   - Writes CSV files to `data/processed/`.

3. `notebooks/04_validate_semantic_model_hybrid_gx_csv.ipynb`
   - Runs data quality checks using Great Expectations and pandas.
   - Writes validation outputs to `outputs/validation/`.

### Option 2 — Run via Python Scripts (Reproducible pipeline)

Use this path when you want to regenerate the pipeline outputs consistently from the command line.

From the project root, run:

```bash
python src/data/generate_synthetic_data.py
python src/data/build_semantic_model.py
python src/data/validate_model.py
```

The scripts perform the same core workflow as the notebooks:

- `generate_synthetic_data.py` creates raw synthetic tables in `data/raw/`.
- `build_semantic_model.py` builds cleaned dimensions and fact tables in `data/processed/`.
- `validate_model.py` runs validation checks and writes results to `outputs/validation/`.

### Why This Structure?

- Separates raw telemetry-style data from cleaned semantic model outputs.
- Mirrors a real-world analytics engineering workflow.
- Supports both experimentation and reproducibility.
- Makes the project easier to extend with forecasting features, behavioural analytics, and future GenAI insight layers.

## Current Status

Implemented now:

- Synthetic Power BI-style usage dataset.
- Baseline report-level forecasting notebook.
- Auto-ARIMA modelling with naive and seasonal-naive comparisons.
- Basic model acceptance criteria.
- Forecast, metrics, history, and realised-error output patterns.
- Lightweight project structure for continued development.

Planned next:

- Stronger evaluation using rolling-origin backtesting.
- More visual diagnostics for forecast quality and residual behaviour.
- Behavioural analytics features such as report usage segments and adoption signals.
- A small GenAI insight layer that summarises forecast changes and potential actions.
- Optional extraction of reusable notebook logic into `src/` modules.

## Roadmap

1. Add rolling-origin evaluation and per-horizon metrics.
2. Add a concise diagnostics section with forecast-vs-actual and error-by-horizon visuals.
3. Create a small behavioural analytics notebook or module for report usage segmentation.
4. Draft GenAI prompt templates for future usage summaries, without connecting to an API yet.
5. Move stable notebook functions into `src/` only when the workflow is mature enough to reuse.
