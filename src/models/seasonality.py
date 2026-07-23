"""Leakage-safe seasonal-period candidate profiling.

Public API
----------
profile_seasonality(
    training_series,
    candidate_periods,
    min_cycles,
    max_candidates,
) -> SeasonalityProfile

Given *only* the observations in one rolling-fold training window, this
function proposes which seasonal periods are worth evaluating on that fold.
It does **not** make a final selection — that responsibility belongs to the
rolling backtest comparison in ``backtest_evaluation.py``.

Design rules
------------
* Only training-window observations are accepted.  The function signature
  forces the caller to slice before calling; no internal slicing occurs.
* Spectral peaks are matched only to periods in *candidate_periods*.  Peaks
  at other frequencies are silently ignored, not promoted.
* ``NON_SEASONAL_PERIOD = 1`` is always included in ``selected_candidate_periods``
  regardless of evidence.  This ensures a non-seasonal model is always
  evaluated alongside seasonal ones.
* Candidate exclusion is deterministic — the same series always produces
  the same profile.
* Degenerate inputs (constant, all-zero, all-NaN, too short) are handled
  gracefully; the function never raises on data-quality issues.

Scoring formula
---------------
For each candidate period m the composite score is:

    score(m) = w_ac * acf_norm(m) + w_sp * spectral_norm(m)

where:
    acf_norm(m)     = max(0, autocorrelation at lag m) / max_positive_acf
    spectral_norm(m)= spectral_power(m) / total_power

Weights default to SCORE_WEIGHTS = {"acf": 0.5, "spectral": 0.5}.
Normalisation is applied across eligible candidates per call so scores are
comparable within a single profile but not across profiles from different
series.  Scores are always in [0, 1].
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from src.config.forecasting import (
    MAX_SEASONAL_CANDIDATES_PER_FOLD,
    MIN_SEASONAL_CYCLES,
    NON_SEASONAL_PERIOD,
    SEASONAL_CANDIDATES,
    SEASONAL_MATCH_TOLERANCE,
)

# ---------------------------------------------------------------------------
# Default scoring weights (exposed so callers can override in tests)
# ---------------------------------------------------------------------------

SCORE_WEIGHTS: dict[str, float] = {"acf": 0.5, "spectral": 0.5}
"""Weights for the composite candidate score.

``acf``       weight applied to the normalised lag-m autocorrelation.
``spectral``  weight applied to the normalised spectral power at frequency 1/m.

Must sum to 1.0.  Adjust via the ``score_weights`` parameter of
``profile_seasonality`` — do not mutate this module-level dict.
"""

# ---------------------------------------------------------------------------
# Return type
# ---------------------------------------------------------------------------

@dataclass
class SeasonalityProfile:
    """All outputs from a single call to ``profile_seasonality``.

    Attributes
    ----------
    selected_candidate_periods:
        Ordered list of periods recommended for this fold, including
        ``NON_SEASONAL_PERIOD = 1`` (always first).  Length ≤
        ``max_candidates + 1``.  The ordering within seasonal candidates
        is highest-score-first.
    eligible_candidate_periods:
        Candidates that passed the ``min_cycles`` gate (before the
        ``max_candidates`` cap is applied).  Does not include m=1.
    excluded_periods:
        Candidates that were screened out, with the reason for each.
    cycles_available_by_period:
        ``floor(len(training_series) / m)`` for each input candidate.
    autocorrelation_by_period:
        Raw lag-m autocorrelation for each input candidate (NaN when
        the series is degenerate or the lag exceeds the series length).
    spectral_power_by_period:
        Spectral power estimated at the frequency nearest to 1/m for
        each input candidate.
    candidate_score_by_period:
        Composite score (0–1) for each *eligible* candidate.  NaN for
        excluded candidates.
    exclusion_reason_by_period:
        Human-readable exclusion reason for each excluded period, empty
        string for eligible/selected candidates.
    dominant_detected_period:
        The eligible candidate with the highest composite score, or
        ``None`` when no eligible candidates exist.
    seasonality_status:
        One of: ``"seasonal"`` (≥1 eligible candidate with score > 0),
        ``"non_seasonal"`` (eligible candidates exist but all score ~0),
        ``"insufficient_history"`` (all candidates excluded for history),
        ``"degenerate"`` (series is constant, all-zero, or all-NaN).
    diagnostic_notes:
        List of plain-English notes about data quality or degenerate
        conditions encountered during profiling.
    """
    selected_candidate_periods: list[int]
    eligible_candidate_periods: list[int]
    excluded_periods: dict[int, str]
    cycles_available_by_period: dict[int, int]
    autocorrelation_by_period: dict[int, float]
    spectral_power_by_period: dict[int, float]
    candidate_score_by_period: dict[int, float]
    exclusion_reason_by_period: dict[int, str]
    dominant_detected_period: Optional[int]
    seasonality_status: str
    diagnostic_notes: list[str]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _detrend_linear(y: np.ndarray) -> np.ndarray:
    """Remove linear trend from *y* using least-squares fit.

    Returns the residuals.  Falls back to mean-centering when the
    least-squares solve is degenerate.
    """
    n = len(y)
    if n < 2:
        return y - y.mean() if n == 1 else y.copy()
    t = np.arange(n, dtype=float)
    # [t, 1] design matrix
    A = np.column_stack([t, np.ones(n)])
    try:
        coeffs, _, _, _ = np.linalg.lstsq(A, y, rcond=None)
        trend = A @ coeffs
        return y - trend
    except np.linalg.LinAlgError:
        return y - y.mean()


def _acf_at_lag(y: np.ndarray, lag: int) -> float:
    """Compute the sample autocorrelation of *y* at *lag*.

    Uses the biased (1/N) normalisation so the result is always in [-1, 1].
    Returns NaN when the series variance is zero or lag ≥ len(y).
    """
    n = len(y)
    if lag >= n or lag < 1:
        return float("nan")
    y_centered = y - y.mean()
    var = float(np.dot(y_centered, y_centered))
    if var == 0.0:
        return float("nan")
    cov = float(np.dot(y_centered[lag:], y_centered[:-lag]))
    return cov / var  # biased: divides by n implicitly via var = sum/n * n


def _periodogram_power(y: np.ndarray, period: int, n: int) -> float:
    """Estimate spectral power at the frequency bin nearest to 1/period.

    Uses the squared-magnitude DFT (unnormalised periodogram) on *y*,
    which must already be detrended.  The power at the bin closest to
    frequency k/n (where k = round(n/period)) is returned as a fraction
    of the total power across all positive-frequency bins.

    Returns NaN when ``period > n`` or ``n < 2``.
    """
    if n < 2 or period > n:
        return float("nan")

    spectrum = np.abs(np.fft.rfft(y)) ** 2
    # Frequencies: k/n for k=0,1,...,n//2
    # Target frequency: 1/period → target bin: n/period
    target_bin = n / period
    # Nearest integer bin (clamp to valid range)
    k = int(round(target_bin))
    k = max(1, min(k, len(spectrum) - 1))

    total_power = float(spectrum[1:].sum())  # exclude DC (k=0)
    if total_power == 0.0:
        return 0.0
    return float(spectrum[k]) / total_power


def _is_degenerate(y: np.ndarray) -> tuple[bool, str]:
    """Return (True, reason) if *y* is unsuitable for seasonal analysis."""
    if len(y) == 0:
        return True, "empty series"
    finite = y[np.isfinite(y)]
    if len(finite) == 0:
        return True, "all values are NaN/Inf"
    if len(finite) < len(y):
        # Partial NaN — continue with a note (handled by caller)
        pass
    if np.all(finite == 0.0):
        return True, "all values are zero"
    if np.ptp(finite) == 0.0:
        return True, "series is constant (zero variance)"
    return False, ""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def profile_seasonality(
    training_series: pd.Series,
    candidate_periods: tuple[int, ...] = SEASONAL_CANDIDATES,
    min_cycles: int = MIN_SEASONAL_CYCLES,
    max_candidates: int = MAX_SEASONAL_CANDIDATES_PER_FOLD,
    score_weights: Optional[dict[str, float]] = None,
) -> SeasonalityProfile:
    """Profile seasonal-period candidates from a fold training series.

    Parameters
    ----------
    training_series:
        A continuous daily ``pd.Series`` containing **only** the observations
        in the current rolling-fold training window.  The function reads
        nothing outside this series; the caller is responsible for correct
        slicing before passing.
    candidate_periods:
        Tuple of business-plausible candidate periods to evaluate, ordered
        shortest to longest.  Defaults to ``SEASONAL_CANDIDATES``.
        ``NON_SEASONAL_PERIOD = 1`` is always included in the output
        regardless of whether it appears here.
    min_cycles:
        Minimum number of complete cycles of period m required in
        *training_series*.  Candidates with fewer cycles are excluded.
        Defaults to ``MIN_SEASONAL_CYCLES``.
    max_candidates:
        Maximum number of seasonal candidates (not counting m=1) to
        include in ``selected_candidate_periods``.  The highest-scoring
        eligible candidates are preferred.  Defaults to
        ``MAX_SEASONAL_CANDIDATES_PER_FOLD``.
    score_weights:
        Override the default ``SCORE_WEIGHTS`` dict.  Keys: ``"acf"``,
        ``"spectral"``.  Values must be non-negative and sum to 1.0.

    Returns
    -------
    SeasonalityProfile
        See class docstring for field definitions.

    Notes
    -----
    * This function **proposes** candidates for evaluation; the rolling
      backtest makes the final model selection.
    * m=1 (``NON_SEASONAL_PERIOD``) is always the first element of
      ``selected_candidate_periods`` so a non-seasonal model is always
      evaluated.
    * Spectral peaks outside *candidate_periods* are ignored.
    * Degenerate inputs never raise; they return a profile with status
      ``"degenerate"`` and an empty eligible list.
    """
    if score_weights is None:
        score_weights = SCORE_WEIGHTS
    w_acf = float(score_weights.get("acf", 0.5))
    w_sp = float(score_weights.get("spectral", 0.5))

    notes: list[str] = []

    # ------------------------------------------------------------------
    # 1. Input coercion and degenerate checks
    # ------------------------------------------------------------------
    if not isinstance(training_series, pd.Series):
        raise TypeError(
            f"training_series must be a pd.Series, got {type(training_series).__name__}."
        )

    y_raw = training_series.to_numpy(dtype=float)
    n = len(y_raw)

    # Handle NaN — use finite values only; note any removal
    finite_mask = np.isfinite(y_raw)
    n_nan = int((~finite_mask).sum())
    if n_nan > 0:
        notes.append(f"{n_nan} non-finite value(s) in training series replaced with series mean.")
        finite_vals = y_raw[finite_mask]
        fill = float(finite_vals.mean()) if len(finite_vals) > 0 else 0.0
        y_raw = y_raw.copy()
        y_raw[~finite_mask] = fill

    is_degen, degen_reason = _is_degenerate(y_raw)

    # Initialise per-candidate containers
    cycles_avail: dict[int, int] = {}
    acf_by_period: dict[int, float] = {}
    spectral_by_period: dict[int, float] = {}
    score_by_period: dict[int, float] = {}
    excl_reason: dict[int, str] = {}

    for m in candidate_periods:
        cycles_avail[m] = int(n // m)
        acf_by_period[m] = float("nan")
        spectral_by_period[m] = float("nan")
        score_by_period[m] = float("nan")
        excl_reason[m] = ""

    if is_degen:
        notes.append(f"Degenerate series: {degen_reason}.")
        return SeasonalityProfile(
            selected_candidate_periods=[NON_SEASONAL_PERIOD],
            eligible_candidate_periods=[],
            excluded_periods={m: degen_reason for m in candidate_periods},
            cycles_available_by_period=cycles_avail,
            autocorrelation_by_period=acf_by_period,
            spectral_power_by_period=spectral_by_period,
            candidate_score_by_period=score_by_period,
            exclusion_reason_by_period={m: degen_reason for m in candidate_periods},
            dominant_detected_period=None,
            seasonality_status="degenerate",
            diagnostic_notes=notes,
        )

    # ------------------------------------------------------------------
    # 2. Detrend for spectral / ACF analysis
    # ------------------------------------------------------------------
    y_detrended = _detrend_linear(y_raw)

    # Additional variance check after detrending (trend-only series)
    if np.ptp(y_detrended) == 0.0 or np.var(y_detrended) == 0.0:
        notes.append(
            "After linear detrending series has zero variance "
            "(pure linear trend with no residual variation)."
        )
        reason = "zero variance after detrending"
        return SeasonalityProfile(
            selected_candidate_periods=[NON_SEASONAL_PERIOD],
            eligible_candidate_periods=[],
            excluded_periods={m: reason for m in candidate_periods},
            cycles_available_by_period=cycles_avail,
            autocorrelation_by_period=acf_by_period,
            spectral_power_by_period=spectral_by_period,
            candidate_score_by_period=score_by_period,
            exclusion_reason_by_period={m: reason for m in candidate_periods},
            dominant_detected_period=None,
            seasonality_status="non_seasonal",
            diagnostic_notes=notes,
        )

    # ------------------------------------------------------------------
    # 3. Per-candidate ACF and spectral power
    # ------------------------------------------------------------------
    for m in candidate_periods:
        if cycles_avail[m] < min_cycles:
            excl_reason[m] = (
                f"insufficient history: {cycles_avail[m]} cycle(s) available, "
                f"need ≥{min_cycles}"
            )
            continue
        if m >= n:
            excl_reason[m] = f"lag {m} ≥ series length {n}"
            continue
        acf_by_period[m] = _acf_at_lag(y_detrended, m)
        spectral_by_period[m] = _periodogram_power(y_detrended, m, n)

    # ------------------------------------------------------------------
    # 4. Eligibility gate: sufficient history
    # ------------------------------------------------------------------
    eligible = [m for m in candidate_periods if not excl_reason[m]]

    if not eligible:
        all_excluded = {m: excl_reason[m] for m in candidate_periods}
        return SeasonalityProfile(
            selected_candidate_periods=[NON_SEASONAL_PERIOD],
            eligible_candidate_periods=[],
            excluded_periods=all_excluded,
            cycles_available_by_period=cycles_avail,
            autocorrelation_by_period=acf_by_period,
            spectral_power_by_period=spectral_by_period,
            candidate_score_by_period=score_by_period,
            exclusion_reason_by_period={m: excl_reason[m] for m in candidate_periods},
            dominant_detected_period=None,
            seasonality_status="insufficient_history",
            diagnostic_notes=notes,
        )

    # ------------------------------------------------------------------
    # 5. Composite scoring (normalised across eligible candidates)
    # ------------------------------------------------------------------
    # Normalise ACF contributions: use max positive ACF among eligible
    acf_vals = np.array(
        [max(0.0, acf_by_period[m]) if np.isfinite(acf_by_period[m]) else 0.0
         for m in eligible]
    )
    max_acf = float(acf_vals.max())

    # Normalise spectral contributions: already expressed as fraction of total
    # power, but re-normalise across eligible candidates to make them
    # comparable when only a subset passes the history gate.
    sp_vals = np.array(
        [spectral_by_period[m] if np.isfinite(spectral_by_period[m]) else 0.0
         for m in eligible]
    )
    max_sp = float(sp_vals.max())

    for i, m in enumerate(eligible):
        acf_norm = (acf_vals[i] / max_acf) if max_acf > 0 else 0.0
        sp_norm = (sp_vals[i] / max_sp) if max_sp > 0 else 0.0
        score_by_period[m] = w_acf * acf_norm + w_sp * sp_norm

    # ------------------------------------------------------------------
    # 6. Select top-k by score (deterministic: ties broken by shorter period)
    # ------------------------------------------------------------------
    eligible_sorted = sorted(eligible, key=lambda m: (-score_by_period[m], m))
    selected_seasonal = eligible_sorted[:max_candidates]

    # ------------------------------------------------------------------
    # 7. Dominant period: highest-scoring eligible candidate
    # ------------------------------------------------------------------
    dominant = eligible_sorted[0] if eligible_sorted else None

    # ------------------------------------------------------------------
    # 8. Seasonality status
    # ------------------------------------------------------------------
    max_score = max((score_by_period[m] for m in eligible), default=0.0)
    if max_score > 0.0:
        status = "seasonal"
    else:
        status = "non_seasonal"

    # ------------------------------------------------------------------
    # 9. Build final output
    # ------------------------------------------------------------------
    all_excl = {m: excl_reason[m] for m in candidate_periods if excl_reason[m]}
    excl_reason_full = {m: excl_reason[m] for m in candidate_periods}

    return SeasonalityProfile(
        selected_candidate_periods=[NON_SEASONAL_PERIOD] + selected_seasonal,
        eligible_candidate_periods=eligible,
        excluded_periods=all_excl,
        cycles_available_by_period=cycles_avail,
        autocorrelation_by_period=acf_by_period,
        spectral_power_by_period=spectral_by_period,
        candidate_score_by_period=score_by_period,
        exclusion_reason_by_period=excl_reason_full,
        dominant_detected_period=dominant,
        seasonality_status=status,
        diagnostic_notes=notes,
    )
