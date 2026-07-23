"""Central forecasting evaluation configuration.

Forecast horizon
----------------
FORECAST_HORIZON_DAYS = 28
    Four complete calendar weeks.  The horizon is the same for every report
    regardless of which seasonal period that report uses; 28 is an exact
    multiple of 7, 14, and 28, which avoids boundary artefacts in
    seasonal-naive baselines for those cycles.

Seasonal period — per-report, not portfolio-global
--------------------------------------------------
There is no single global seasonal period.  Reports in the Power BI portfolio
exhibit several distinct dominant cycles:

  NON_SEASONAL_PERIOD = 1
      Indicates that no seasonality is modelled.  Used for pure trend models
      or when a series shows no detectable periodic structure.

  SEASONAL_CANDIDATES = (7, 14, 28, 30, 90)
      The candidate periods screened during per-report period detection:

      7   — Weekly cycle.  Most common; caused by the Monday-to-Friday
            consumption spike vs. the weekend drop.

      14  — Biweekly cycle.  Appears in reports tied to sprint or fortnightly
            reporting cadences.

      28  — Four-week cycle.  Distinct from calendar-monthly: always exactly
            four weeks, so it preserves alignment with FORECAST_HORIZON_DAYS.

      30  — Approximate monthly cycle.  A fixed-period approximation of the
            calendar month (30 days rather than 28/29/30/31).  Does NOT track
            actual month-end dates; it is useful when the dominant driver is
            "roughly monthly" behaviour without strict calendar anchoring.

      90  — Approximate quarterly cycle.  A fixed-period approximation of the
            fiscal or calendar quarter (90 days, not March/June/September/
            December month-end).  Same caveat as 30: it captures "roughly
            quarterly" patterns, not true quarter-end spikes.

  Annual seasonality (365 / 52 weeks) is intentionally absent.  Estimating
  it reliably requires several full years of history; the minimum eligibility
  gate (MIN_TRAIN_DAYS = 180 days) is less than one year, so annual
  coefficients would be poorly identified on most series in the portfolio.

Candidate-selection constraints
--------------------------------
MIN_SEASONAL_CYCLES = 3
    A candidate period m is only evaluated on a fold when the fold's training
    window contains at least MIN_SEASONAL_CYCLES × m observations.  Below this
    threshold the seasonal coefficients are likely to overfit.

PREFERRED_SEASONAL_CYCLES = 4
    The target training length, expressed in cycles, for a period to be
    considered well-estimated.  Periods that meet MIN_SEASONAL_CYCLES but not
    PREFERRED_SEASONAL_CYCLES are used with a quality warning.

MAX_SEASONAL_CANDIDATES_PER_FOLD = 3
    Cap on how many candidate periods are evaluated per fold to control
    runtime.  When more than this many candidates pass the cycle-count gate,
    the shorter periods are preferred (they require less history and tend to
    be more identifiable).

SEASONAL_MATCH_TOLERANCE = 0.10
    Relative tolerance when comparing two candidate periods for equivalence.
    Two periods p1, p2 with |p1 - p2| / max(p1, p2) ≤ this value are
    treated as the same cycle class (e.g. 28 and 30 are distinct; 7 and 7
    are identical).  Used to deduplicate candidate lists and to guard against
    evaluating nearly-identical models twice.

Backtesting evaluation
-----------------------
BACKTEST_FOLDS, BACKTEST_STEP_DAYS, MIN_TRAIN_DAYS, and MIN_VALID_FOLDS
control rolling-origin fold generation and model selection policy.  They are
independent of the seasonal-period candidates above.
"""

# ---------------------------------------------------------------------------
# Forecast horizon
# ---------------------------------------------------------------------------
FORECAST_HORIZON_DAYS: int = 28
# Four calendar weeks; equal to 4 × 7 = 2 × 14 = 1 × 28.
# Must remain unchanged — the backtest test-window size equals this value
# (BACKTEST_STEP_DAYS = FORECAST_HORIZON_DAYS) to prevent overlapping windows.

# ---------------------------------------------------------------------------
# Non-seasonal indicator
# ---------------------------------------------------------------------------
NON_SEASONAL_PERIOD: int = 1
# Pass this value as seasonal_period to any model function to request a
# non-seasonal specification (ARIMA without seasonal terms, ETS(A,A,N), etc.).

# ---------------------------------------------------------------------------
# Seasonal-period candidates
# ---------------------------------------------------------------------------
SEASONAL_CANDIDATES: tuple[int, ...] = (7, 14, 28, 30, 90)
# Ordered from shortest to longest.  The detection step screens these in
# order and caps evaluation at MAX_SEASONAL_CANDIDATES_PER_FOLD.
#
# Interpretations:
#   7  — weekly (day-of-week effect)
#  14  — biweekly (fortnightly cadence)
#  28  — four-week cycle (aligns with FORECAST_HORIZON_DAYS)
#  30  — approximate monthly (fixed 30-day period, not calendar month-end)
#  90  — approximate quarterly (fixed 90-day period, not fiscal quarter-end)
#
# Annual (365 / 52) is deliberately excluded; see module docstring.

# ---------------------------------------------------------------------------
# Candidate-selection constraints
# ---------------------------------------------------------------------------
MIN_SEASONAL_CYCLES: int = 3
# Minimum number of complete cycles of period m required in a fold's training
# window before that candidate is evaluated.  Ensures at least 3 × m
# observations are available for seasonal coefficient estimation.

PREFERRED_SEASONAL_CYCLES: int = 4
# Preferred training length in cycles.  Periods meeting MIN_SEASONAL_CYCLES
# but not PREFERRED_SEASONAL_CYCLES may be evaluated but are flagged.

MAX_SEASONAL_CANDIDATES_PER_FOLD: int = 3
# Hard cap on candidates evaluated per fold.  When more pass the
# MIN_SEASONAL_CYCLES gate, the shortest-period candidates are preferred.

SEASONAL_MATCH_TOLERANCE: float = 0.10
# Relative tolerance for treating two period values as equivalent:
#   |p1 - p2| / max(p1, p2) ≤ SEASONAL_MATCH_TOLERANCE → same class.
# For the default candidates, no pair is within 10 %:
#   7 vs 14 → 50 %; 14 vs 28 → 50 %; 28 vs 30 → 6.7 % (just outside).
# Setting this to 0.07 would merge 28 and 30 — do not do that by default.

# ---------------------------------------------------------------------------
# Rolling-origin backtest configuration
# ---------------------------------------------------------------------------
BACKTEST_FOLDS: int = 4
# Number of rolling-origin evaluation windows per report.

BACKTEST_STEP_DAYS: int = 28
# Stride between successive cutoff dates.  Equals FORECAST_HORIZON_DAYS so
# test windows are non-overlapping.

MIN_TRAIN_DAYS: int = 180
# Minimum training observations required for the first fold.  Ensures at
# least MIN_TRAIN_DAYS / 7 ≈ 25 weekly cycles of history before evaluation.

MIN_VALID_FOLDS: int = 3
# Minimum number of non-failed folds a model must produce to be eligible for
# selection.  Guards against sparse-series artefacts.
