# Forecasting Report Usage with Auto-ARIMA (Synthetic Data)

Portfolio-ready data science project demonstrating end-to-end time-series forecasting and deployment planning.

## Project goals
- Forecast daily report usage for multiple reports
- Compare ARIMA vs naive and seasonal-naive baselines
- Apply acceptance criteria to publish only useful forecasts
- Track forecast/metrics history and realized errors
- Provide a practical Azure Databricks ML pipeline path

## Main artifact
- `Forecast_Report_Usage_synthetic_improved.ipynb`

## Highlights
- Synthetic dataset with realistic behavior: weekly seasonality, trend, noise, zero days
- Per-report daily series modeling with `pmdarima.auto_arima`
- Robust metrics: MAE, RMSE, WAPE, improvement vs baselines
- Quality gates for model acceptance
- Non-negative clipping for count forecasts
- Schema-safe outputs for BI consumption

## Repository structure
- `Forecast_Report_Usage_synthetic_improved.ipynb`: full workflow
- `outputs/`: generated CSV outputs (latest + history files)
- `requirements.txt`: Python dependencies

## Setup
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
jupyter notebook
```

## Output files
Generated in `outputs/`:
- `report_view_forecasts_latest.csv`
- `report_view_metrics_latest.csv`
- `forecasts_history.csv`
- `metrics_history.csv`
- `realized_errors_history.csv`

## Databricks expansion
The notebook includes a dedicated section with:
- Bronze/Silver/Gold Delta design
- Databricks Workflows orchestration pattern
- MLflow tracking integration points
- Example pseudocode for Spark + Delta writes

## Suggested next enhancements
1. Add walk-forward cross-validation per report
2. Log experiments to MLflow with model/version tags
3. Add CI checks for notebook execution and schema validation
4. Add dashboard visuals for forecast performance by horizon
