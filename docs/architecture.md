# Architecture Note

This document is lightweight scaffolding for the project architecture. It captures the current shape of the work and the intended direction without claiming that future modules are already implemented.

## Current Flow

```text
Synthetic usage data
        |
        v
Data quality checks
        |
        v
Report-level forecasting baseline
        |
        v
Forecast, metrics, history, and realised-error outputs
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

The immediate goal is to strengthen the forecasting baseline before adding reusable modules or GenAI components.
