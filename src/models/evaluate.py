"""Forecast evaluation metrics."""

from __future__ import annotations

import numpy as np
import pandas as pd


def calculate_forecast_metrics(actual: pd.Series, forecast: pd.Series) -> dict[str, float]:
    """Calculate MAE, RMSE, and MAPE with safe zero-actual handling."""
    actual_values = pd.to_numeric(pd.Series(actual), errors="coerce")
    forecast_values = pd.to_numeric(pd.Series(forecast), errors="coerce")

    aligned = pd.concat(
        [actual_values.rename("actual"), forecast_values.rename("forecast")],
        axis=1,
    ).dropna()

    if aligned.empty:
        return {"mae": np.nan, "rmse": np.nan, "mape": np.nan}

    errors = aligned["actual"] - aligned["forecast"]
    absolute_errors = errors.abs()

    nonzero_actuals = aligned["actual"] != 0
    if nonzero_actuals.any():
        mape = (
            absolute_errors.loc[nonzero_actuals]
            / aligned.loc[nonzero_actuals, "actual"].abs()
        ).mean() * 100
    else:
        mape = np.nan

    return {
        "mae": float(absolute_errors.mean()),
        "rmse": float(np.sqrt(np.square(errors).mean())),
        "mape": float(mape) if not pd.isna(mape) else np.nan,
    }
