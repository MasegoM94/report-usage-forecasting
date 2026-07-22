"""Deprecated: use src.models.metrics instead.

``calculate_forecast_metrics`` is retained for backwards compatibility only.
It returned MAPE, which is undefined for zero-valued actuals and has been
removed from the primary metric set.  New code should call
``src.models.metrics.calculate_point_metrics``, which returns MAE, RMSE,
WAPE, MASE, and bias without silently excluding zero rows.
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

from src.models.metrics import calculate_point_metrics  # noqa: F401 — re-export


def calculate_forecast_metrics(actual: pd.Series, forecast: pd.Series) -> dict[str, float]:
    """Deprecated — use calculate_point_metrics from src.models.metrics.

    Returns MAE and RMSE via the new module; ``mape`` is always ``np.nan``
    because MAPE has been removed as a primary metric (undefined on zero
    actuals, asymmetric on small actuals).
    """
    warnings.warn(
        "calculate_forecast_metrics is deprecated and will be removed in a future release. "
        "Use src.models.metrics.calculate_point_metrics instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    m = calculate_point_metrics(actual, forecast)
    return {"mae": m["mae"], "rmse": m["rmse"], "mape": np.nan}
