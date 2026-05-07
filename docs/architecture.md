# Architecture Note

This document is lightweight scaffolding for the project architecture. It captures the current shape of the work and the intended direction without claiming that future modules are already implemented.

## Current Flow

```text
Synthetic usage data
        |
        v
Semantic model CSV tables
        |
        v
Validation checks
        |
        v
Feature engineering marts
        |
        v
Report-level forecasting baseline
        |
        v
Forecast, metrics, history, and realised-error outputs
```

## Current Structure

```text
data/raw/
        Raw synthetic telemetry-style CSV tables.

data/processed/
        Clean dimensions, facts, and feature marts, including
        mart_forecast_features.csv.

notebooks/
        Ordered notebook workflow from raw data generation through the
        forecasting baseline.

src/data/
        Script versions of data generation, semantic model build, and
        validation.

src/features/
        Reusable feature builders for report adoption, engagement,
        performance, and joined forecast features.

src/models/
        Lightweight baseline and metric helpers.

src/pipelines/
        Command-line forecasting baseline runner that mirrors the current
        notebook output contract.

outputs/validation/
        Data quality and reconciliation results.

outputs/forecasts/
        Latest forecast table and appended forecast history.

outputs/metrics/
        Latest forecast metrics, model comparison, metrics history, and
        realised-error history.

outputs/diagnostics/, outputs/anomalies/, outputs/segments/
        Reserved extension folders for forecast diagnostics, anomaly flags,
        and behavioural segmentation outputs.
```

## Future Direction

```text
Power BI usage data
        |
        v
Forecasting + behavioural analytics
        |
        v
Forecast diagnostics and usage segments
        |
        v
GenAI-assisted summaries for stakeholders
```

The immediate goal is to keep the notebook and script output contract aligned while strengthening backtesting, diagnostics, and future GenAI components.
