# Architecture Note

This document is a lightweight architecture note for **Power BI Usage Intelligence: Forecasting, Behavioural Analytics, and GenAI Insights**.

The project uses synthetic, shareable Power BI-style telemetry data. It does not connect to, extract from, or deploy against the real Power BI service.

## Current Flow

```text
Synthetic telemetry tables
        |
        v
Raw CSV tables
        |
        v
Semantic model build
        |
        v
Processed dimensions and facts
        |
        v
Feature marts
        |
        v
Analytics outputs
        |
        v
Forecasting outputs
        |
        v
GenAI insight outputs
        |
        v
Future Streamlit app
```

## Current Structure

```text
data/raw/
        Synthetic raw telemetry-style CSV tables.

data/processed/
        Clean dimensions, facts, and reusable feature marts.

docs/data_model.md
        Detailed table notes for the semantic model and output tables.

notebooks/
        Ordered workflow from synthetic data generation through validation,
        feature engineering, forecasting, analytics, and GenAI insights.

src/data/
        Script versions of data generation, semantic model build, and
        validation.

src/features/
        Reusable feature builders for report adoption, engagement,
        performance, and joined forecast features.

src/models/
        Forecasting baselines and metric helpers.

src/analytics/
        Report analytics, user analytics, diagnostics, and segmentation.

src/genai/
        Lightweight batch insight generation from forecast, segment, metric,
        and diagnostic outputs.

src/pipelines/
        Command-line runners for forecasting, report analytics, and user
        analytics.

outputs/validation/
        Data quality and reconciliation results.

outputs/forecasts/
        Forecast tables and forecast history.

outputs/metrics/
        Forecast metrics, model comparison, analytics metrics, and history.

outputs/segments/
        Report and user segmentation outputs.

outputs/diagnostics/
        Report diagnostic rule outputs.

outputs/insights/
        Batch-generated GenAI report insight outputs.

outputs/anomalies/
        Optional anomaly output placeholder.
```

## Future Improvements

```text
Streamlit app for reviewer-friendly exploration
Rolling-origin backtesting for stronger forecast validation
Optional open-source forecasting model comparison
GenAI evaluation for insight quality and consistency
```

The immediate goal is to keep the notebook and script output contracts aligned while preserving a simple, portfolio-friendly workflow.
