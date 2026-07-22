"""Central forecasting evaluation configuration.

Portfolio assumptions
---------------------
* FORECAST_HORIZON_DAYS = 28
    Four complete weeks.  Report-usage patterns repeat on a weekly cycle, so a
    horizon that is an exact multiple of SEASONAL_PERIOD avoids boundary effects
    in seasonal-naive baselines and makes period-over-period comparisons
    straightforward for stakeholders.

* SEASONAL_PERIOD = 7
    Report-usage data has a strong day-of-week pattern (weekday vs. weekend
    consumption differs materially across the portfolio).  Weekly seasonality is
    the dominant cycle observed in all sufficiently long series and is the lag
    used by SARIMA (m=7) and the seasonal-naive baseline.

Backtesting stubs (not yet active — reserved for rolling-origin evaluation)
---------------------------------------------------------------------------
BACKTEST_FOLDS, BACKTEST_STEP_DAYS, MIN_TRAIN_DAYS, and MIN_VALID_FOLDS are
defined here so future rolling-origin evaluation has a single authoritative
source for these values.  They are not used by any pipeline code today.
"""

# --- Forecast horizon -------------------------------------------------------
FORECAST_HORIZON_DAYS: int = 28
# Four weeks; must be an exact multiple of SEASONAL_PERIOD.

# --- Seasonality ------------------------------------------------------------
SEASONAL_PERIOD: int = 7
# Weekly cycle — dominant pattern across the report portfolio.

# --- Backtesting (reserved) -------------------------------------------------
BACKTEST_FOLDS: int = 4
# Number of rolling-origin evaluation windows.

BACKTEST_STEP_DAYS: int = 28
# Stride between successive backtest cutoff dates; equals the forecast horizon
# so windows do not overlap in the test region.

MIN_TRAIN_DAYS: int = 180
# Minimum in-sample training observations required before a fold is valid.
# Longer than the data-eligibility gate (MIN_DAYS=90) to ensure enough history
# for seasonal decomposition with multiple full seasonal cycles.

MIN_VALID_FOLDS: int = 3
# A series must produce at least this many valid backtest folds before its
# rolling-origin metrics are reported.  Guards against sparse-series artefacts.
