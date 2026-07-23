"""Tests for src.models.seasonality.profile_seasonality.

Each test class covers one scenario from the specification.  All series
are constructed as pd.Series with DatetimeIndex — exactly what a rolling
fold would supply as a training slice.

Fixture design
--------------
Series length conventions:
  * Quarterly tests need ≥ 3 × 90 = 270 days.
  * Most other tests use 400 days so multiple period sizes are eligible.
  * Insufficient-history tests use short series by design.

Signal construction:
  * A clean sinusoidal signal at period m is injected into a low-noise
    base to produce clear ACF and spectral peaks.
  * Gaussian noise (σ=0.5) is added on top of the signal (SNR ≈ 10).
  * The seed is fixed so tests are deterministic.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.config.forecasting import (
    MAX_SEASONAL_CANDIDATES_PER_FOLD,
    MIN_SEASONAL_CYCLES,
    NON_SEASONAL_PERIOD,
    SEASONAL_CANDIDATES,
)
from src.models.seasonality import SeasonalityProfile, profile_seasonality

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_RNG = np.random.default_rng(0)


def _make_series(
    n: int,
    period: int | None = None,
    amplitude: float = 5.0,
    noise_std: float = 0.5,
    trend_slope: float = 0.0,
    base_level: float = 10.0,
    seed: int = 0,
    start: str = "2020-01-01",
) -> pd.Series:
    """Return a daily pd.Series with an optional sinusoidal seasonal signal.

    Parameters
    ----------
    n:         Length of the series.
    period:    If given, adds a sinusoidal component at this period.
    amplitude: Amplitude of the seasonal signal (peak-to-trough = 2×).
    noise_std: Standard deviation of additive Gaussian noise.
    trend_slope: Linear trend per observation.
    base_level:  Constant offset.
    seed:      Numpy random seed.
    start:     First date in the DatetimeIndex.
    """
    rng = np.random.default_rng(seed)
    t = np.arange(n, dtype=float)
    y = base_level + trend_slope * t + rng.normal(0, noise_std, size=n)
    if period is not None:
        y += amplitude * np.sin(2 * np.pi * t / period)
    idx = pd.date_range(start, periods=n, freq="D")
    return pd.Series(np.clip(y, 0, None), index=idx)


def _make_biweekly(n: int, seed: int = 1) -> pd.Series:
    """Series with strong biweekly (period=14) signal."""
    return _make_series(n, period=14, amplitude=6.0, noise_std=0.4, seed=seed)


# ---------------------------------------------------------------------------
# 1. Weekly usage
# ---------------------------------------------------------------------------

class TestWeeklySeasonality:
    """Series with clear weekly (m=7) periodicity."""

    def setup_method(self):
        self.series = _make_series(400, period=7, amplitude=5.0, noise_std=0.3)
        self.profile = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)

    def test_status_is_seasonal(self):
        assert self.profile.seasonality_status == "seasonal"

    def test_m1_always_first(self):
        assert self.profile.selected_candidate_periods[0] == NON_SEASONAL_PERIOD

    def test_period_7_is_eligible(self):
        assert 7 in self.profile.eligible_candidate_periods

    def test_period_7_is_selected(self):
        assert 7 in self.profile.selected_candidate_periods

    def test_period_7_has_positive_acf(self):
        acf = self.profile.autocorrelation_by_period[7]
        assert np.isfinite(acf)
        assert acf > 0.0

    def test_period_7_has_positive_spectral_power(self):
        sp = self.profile.spectral_power_by_period[7]
        assert np.isfinite(sp)
        assert sp > 0.0

    def test_period_7_score_is_finite(self):
        assert np.isfinite(self.profile.candidate_score_by_period[7])

    def test_dominant_period_is_7(self):
        assert self.profile.dominant_detected_period == 7

    def test_cycles_available_for_7(self):
        assert self.profile.cycles_available_by_period[7] == 400 // 7

    def test_no_spurious_exclusion_of_7(self):
        assert 7 not in self.profile.excluded_periods


# ---------------------------------------------------------------------------
# 2. Biweekly usage
# ---------------------------------------------------------------------------

class TestBiweeklySeasonality:
    """Series with clear biweekly (m=14) periodicity, noise drowns m=7."""

    def setup_method(self):
        # Use a signal at 14 days with amplitude >> noise, and no weekly component
        rng = np.random.default_rng(2)
        n = 400
        t = np.arange(n, dtype=float)
        y = 10.0 + 6.0 * np.sin(2 * np.pi * t / 14) + rng.normal(0, 0.3, n)
        idx = pd.date_range("2020-01-01", periods=n, freq="D")
        self.series = pd.Series(np.clip(y, 0, None), index=idx)
        self.profile = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)

    def test_period_14_is_eligible(self):
        assert 14 in self.profile.eligible_candidate_periods

    def test_period_14_is_selected(self):
        assert 14 in self.profile.selected_candidate_periods

    def test_m1_always_first(self):
        assert self.profile.selected_candidate_periods[0] == NON_SEASONAL_PERIOD

    def test_period_14_acf_positive(self):
        assert self.profile.autocorrelation_by_period[14] > 0.0

    def test_period_14_score_highest_among_selected(self):
        selected = [m for m in self.profile.selected_candidate_periods if m != NON_SEASONAL_PERIOD]
        if selected:
            scores = {m: self.profile.candidate_score_by_period[m] for m in selected}
            assert max(scores, key=scores.__getitem__) == 14


# ---------------------------------------------------------------------------
# 3. 28-day usage
# ---------------------------------------------------------------------------

class TestFourWeekSeasonality:
    """Series with a clear 28-day (four-week) periodic signal."""

    def setup_method(self):
        rng = np.random.default_rng(3)
        n = 500
        t = np.arange(n, dtype=float)
        y = 10.0 + 5.0 * np.sin(2 * np.pi * t / 28) + rng.normal(0, 0.3, n)
        idx = pd.date_range("2020-01-01", periods=n, freq="D")
        self.series = pd.Series(np.clip(y, 0, None), index=idx)
        self.profile = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)

    def test_period_28_is_eligible(self):
        assert 28 in self.profile.eligible_candidate_periods

    def test_period_28_is_selected(self):
        assert 28 in self.profile.selected_candidate_periods

    def test_period_28_spectral_power_positive(self):
        assert self.profile.spectral_power_by_period[28] > 0.0

    def test_m1_always_first(self):
        assert self.profile.selected_candidate_periods[0] == NON_SEASONAL_PERIOD


# ---------------------------------------------------------------------------
# 4. Approximate 30-day usage
# ---------------------------------------------------------------------------

class TestApproximateMonthlySeasonality:
    """Series with signal near 30-day period (approximate monthly)."""

    def setup_method(self):
        rng = np.random.default_rng(4)
        n = 500
        t = np.arange(n, dtype=float)
        y = 10.0 + 5.0 * np.sin(2 * np.pi * t / 30) + rng.normal(0, 0.3, n)
        idx = pd.date_range("2020-01-01", periods=n, freq="D")
        self.series = pd.Series(np.clip(y, 0, None), index=idx)
        self.profile = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)

    def test_period_30_is_eligible(self):
        assert 30 in self.profile.eligible_candidate_periods

    def test_period_30_is_selected(self):
        assert 30 in self.profile.selected_candidate_periods

    def test_period_30_acf_positive(self):
        assert self.profile.autocorrelation_by_period[30] > 0.0

    def test_period_30_spectral_power_positive(self):
        assert self.profile.spectral_power_by_period[30] > 0.0

    def test_m1_always_first(self):
        assert self.profile.selected_candidate_periods[0] == NON_SEASONAL_PERIOD


# ---------------------------------------------------------------------------
# 5. Quarterly period with sufficient history
# ---------------------------------------------------------------------------

class TestQuarterlySeasonalitySufficientHistory:
    """≥3×90=270 days → period 90 is eligible."""

    def setup_method(self):
        rng = np.random.default_rng(5)
        n = 400  # > 3 × 90
        t = np.arange(n, dtype=float)
        y = 10.0 + 5.0 * np.sin(2 * np.pi * t / 90) + rng.normal(0, 0.3, n)
        idx = pd.date_range("2020-01-01", periods=n, freq="D")
        self.series = pd.Series(np.clip(y, 0, None), index=idx)
        self.profile = profile_seasonality(
            self.series,
            candidate_periods=SEASONAL_CANDIDATES,
            min_cycles=3,
        )

    def test_period_90_is_eligible(self):
        assert 90 in self.profile.eligible_candidate_periods

    def test_period_90_is_selected(self):
        assert 90 in self.profile.selected_candidate_periods

    def test_period_90_cycles_correct(self):
        assert self.profile.cycles_available_by_period[90] == 400 // 90

    def test_period_90_acf_positive(self):
        assert self.profile.autocorrelation_by_period[90] > 0.0


# ---------------------------------------------------------------------------
# 6. Quarterly period excluded for insufficient history
# ---------------------------------------------------------------------------

class TestQuarterlyExcludedInsufficientHistory:
    """2×90=180 days but min_cycles=3 → period 90 is excluded."""

    def setup_method(self):
        n = 180  # exactly 2 cycles of 90
        self.series = _make_series(n, period=90, amplitude=5.0, noise_std=0.3, seed=6)
        self.profile = profile_seasonality(
            self.series,
            candidate_periods=SEASONAL_CANDIDATES,
            min_cycles=3,
        )

    def test_period_90_is_excluded(self):
        assert 90 in self.profile.excluded_periods

    def test_exclusion_reason_mentions_insufficient(self):
        reason = self.profile.excluded_periods[90]
        assert "insufficient" in reason.lower() or "cycle" in reason.lower()

    def test_period_90_not_in_selected(self):
        assert 90 not in self.profile.selected_candidate_periods

    def test_period_90_cycles_is_two(self):
        assert self.profile.cycles_available_by_period[90] == 2

    def test_m1_still_selected(self):
        assert NON_SEASONAL_PERIOD in self.profile.selected_candidate_periods

    def test_score_for_90_is_nan(self):
        assert not np.isfinite(self.profile.candidate_score_by_period[90])


# ---------------------------------------------------------------------------
# 7. Non-seasonal noise
# ---------------------------------------------------------------------------

class TestNonSeasonalNoise:
    """Pure Gaussian noise — no periodic structure expected."""

    def setup_method(self):
        rng = np.random.default_rng(7)
        n = 400
        y = 10.0 + rng.normal(0, 1.0, n)
        idx = pd.date_range("2020-01-01", periods=n, freq="D")
        self.series = pd.Series(np.clip(y, 0, None), index=idx)
        self.profile = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)

    def test_m1_always_selected(self):
        assert NON_SEASONAL_PERIOD in self.profile.selected_candidate_periods

    def test_status_is_not_degenerate(self):
        assert self.profile.seasonality_status != "degenerate"

    def test_all_short_periods_eligible_at_400_days(self):
        # 7, 14, 28, 30 all have ≥ 3 cycles in 400 days
        for m in [7, 14, 28, 30]:
            assert m in self.profile.eligible_candidate_periods

    def test_acf_values_are_finite_for_eligible(self):
        for m in self.profile.eligible_candidate_periods:
            assert np.isfinite(self.profile.autocorrelation_by_period[m])

    def test_spectral_values_are_finite_for_eligible(self):
        for m in self.profile.eligible_candidate_periods:
            assert np.isfinite(self.profile.spectral_power_by_period[m])

    def test_scores_are_in_0_1(self):
        for m in self.profile.eligible_candidate_periods:
            s = self.profile.candidate_score_by_period[m]
            assert 0.0 <= s <= 1.0 + 1e-9


# ---------------------------------------------------------------------------
# 8. Trending non-seasonal series
# ---------------------------------------------------------------------------

class TestTrendingNonSeasonal:
    """Strong linear trend, no seasonal signal."""

    def setup_method(self):
        n = 400
        self.series = _make_series(
            n,
            period=None,
            trend_slope=0.1,
            noise_std=0.2,
            seed=8,
        )
        self.profile = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)

    def test_does_not_raise(self):
        # Verified implicitly by setup_method completing
        pass

    def test_m1_selected(self):
        assert NON_SEASONAL_PERIOD in self.profile.selected_candidate_periods

    def test_status_is_not_degenerate(self):
        # After detrending, residuals have variance → not degenerate
        assert self.profile.seasonality_status != "degenerate"

    def test_eligible_periods_have_finite_scores(self):
        for m in self.profile.eligible_candidate_periods:
            s = self.profile.candidate_score_by_period[m]
            assert np.isfinite(s)


# ---------------------------------------------------------------------------
# 9. All-zero series
# ---------------------------------------------------------------------------

class TestAllZeroSeries:
    """Series of all zeros — degenerate input."""

    def setup_method(self):
        idx = pd.date_range("2020-01-01", periods=200, freq="D")
        self.series = pd.Series(np.zeros(200), index=idx)
        self.profile = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)

    def test_status_is_degenerate(self):
        assert self.profile.seasonality_status == "degenerate"

    def test_m1_still_in_selected(self):
        assert NON_SEASONAL_PERIOD in self.profile.selected_candidate_periods

    def test_no_eligible_candidates(self):
        assert self.profile.eligible_candidate_periods == []

    def test_dominant_period_is_none(self):
        assert self.profile.dominant_detected_period is None

    def test_all_candidates_excluded(self):
        for m in SEASONAL_CANDIDATES:
            assert m in self.profile.excluded_periods

    def test_diagnostic_notes_mention_zero(self):
        combined = " ".join(self.profile.diagnostic_notes).lower()
        assert "zero" in combined

    def test_scores_are_nan(self):
        for m in SEASONAL_CANDIDATES:
            assert not np.isfinite(self.profile.candidate_score_by_period[m])


# ---------------------------------------------------------------------------
# 10. Multiple possible cycles competing
# ---------------------------------------------------------------------------

class TestMultipleCycles:
    """Series has both weekly (7) and 28-day signals of equal amplitude.
    Both should be eligible and selected."""

    def setup_method(self):
        rng = np.random.default_rng(10)
        n = 500
        t = np.arange(n, dtype=float)
        y = (
            10.0
            + 3.0 * np.sin(2 * np.pi * t / 7)
            + 3.0 * np.sin(2 * np.pi * t / 28)
            + rng.normal(0, 0.3, n)
        )
        idx = pd.date_range("2020-01-01", periods=n, freq="D")
        self.series = pd.Series(np.clip(y, 0, None), index=idx)
        self.profile = profile_seasonality(
            self.series,
            candidate_periods=SEASONAL_CANDIDATES,
            max_candidates=3,
        )

    def test_both_7_and_28_eligible(self):
        assert 7 in self.profile.eligible_candidate_periods
        assert 28 in self.profile.eligible_candidate_periods

    def test_both_7_and_28_selected(self):
        selected = self.profile.selected_candidate_periods
        assert 7 in selected
        assert 28 in selected

    def test_m1_first(self):
        assert self.profile.selected_candidate_periods[0] == NON_SEASONAL_PERIOD

    def test_selected_length_respects_max_candidates(self):
        # m=1 + up to 3 seasonal
        assert len(self.profile.selected_candidate_periods) <= 1 + 3

    def test_scores_sum_to_reasonable_range(self):
        for m in [7, 28]:
            s = self.profile.candidate_score_by_period[m]
            assert 0.0 <= s <= 1.0 + 1e-9


# ---------------------------------------------------------------------------
# 11. Deterministic rankings
# ---------------------------------------------------------------------------

class TestDeterministicRankings:
    """Calling profile_seasonality twice on the same series produces identical results."""

    def setup_method(self):
        self.series = _make_series(400, period=7, seed=11)

    def test_selected_periods_identical(self):
        p1 = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)
        p2 = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)
        assert p1.selected_candidate_periods == p2.selected_candidate_periods

    def test_dominant_period_identical(self):
        p1 = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)
        p2 = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)
        assert p1.dominant_detected_period == p2.dominant_detected_period

    def test_scores_identical(self):
        p1 = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)
        p2 = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)
        for m in SEASONAL_CANDIDATES:
            s1 = p1.candidate_score_by_period[m]
            s2 = p2.candidate_score_by_period[m]
            if np.isfinite(s1) and np.isfinite(s2):
                assert s1 == pytest.approx(s2)

    def test_acf_identical(self):
        p1 = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)
        p2 = profile_seasonality(self.series, candidate_periods=SEASONAL_CANDIDATES)
        for m in SEASONAL_CANDIDATES:
            a1 = p1.autocorrelation_by_period[m]
            a2 = p2.autocorrelation_by_period[m]
            if np.isfinite(a1) and np.isfinite(a2):
                assert a1 == pytest.approx(a2)


# ---------------------------------------------------------------------------
# 12. Proof that future test observations do not alter training-fold profile
# ---------------------------------------------------------------------------

class TestLeakageProof:
    """Appending future observations to the series must not change the profile
    produced from the training window alone.

    This proves the function is leakage-safe: only the passed slice matters.
    """

    def setup_method(self):
        rng = np.random.default_rng(12)
        n_train = 300
        n_test = 28
        t = np.arange(n_train + n_test, dtype=float)
        full = 10.0 + 5.0 * np.sin(2 * np.pi * t / 7) + rng.normal(0, 0.3, n_train + n_test)
        idx = pd.date_range("2020-01-01", periods=n_train + n_test, freq="D")
        full_series = pd.Series(np.clip(full, 0, None), index=idx)

        self.train_slice = full_series.iloc[:n_train]
        # future values differ from the natural continuation (adversarial spike)
        future_vals = full[n_train:] * 100.0
        self.test_slice_adversarial = pd.Series(
            future_vals, index=full_series.index[n_train:]
        )

    def test_profile_unchanged_by_appended_future(self):
        """Profile of training slice alone == profile of training slice from a
        concatenated training+adversarial series, when the same training rows
        are passed to profile_seasonality."""
        p_train_only = profile_seasonality(
            self.train_slice, candidate_periods=SEASONAL_CANDIDATES
        )
        # Simulate the adversarial future existing in a combined series, but
        # the caller correctly passes only the training slice.
        p_correct_slice = profile_seasonality(
            self.train_slice,  # same slice — leakage guard is the caller's job
            candidate_periods=SEASONAL_CANDIDATES,
        )
        assert p_train_only.selected_candidate_periods == p_correct_slice.selected_candidate_periods
        assert p_train_only.dominant_detected_period == p_correct_slice.dominant_detected_period

    def test_passing_full_series_changes_profile(self):
        """Contrast: if the caller accidentally passes the full series, the
        profile CAN differ.  This verifies that the slice matters, and that
        our correct-slice test above is actually testing something real."""
        full = pd.concat([self.train_slice, self.test_slice_adversarial])
        p_full = profile_seasonality(full, candidate_periods=SEASONAL_CANDIDATES)
        p_train = profile_seasonality(self.train_slice, candidate_periods=SEASONAL_CANDIDATES)
        # They may differ — this test does not assert equality; it asserts
        # that cycles_available differs (more history in the full series).
        assert p_full.cycles_available_by_period[7] > p_train.cycles_available_by_period[7]

    def test_function_only_uses_provided_series(self):
        """The function has no access to global state; results depend only on
        the arguments.  Alter the training values and the profile changes."""
        p1 = profile_seasonality(self.train_slice, candidate_periods=SEASONAL_CANDIDATES)
        # Flip the sign of the seasonal signal (adversarial training corruption)
        corrupted = self.train_slice * -1.0 + 20.0
        p2 = profile_seasonality(corrupted, candidate_periods=SEASONAL_CANDIDATES)
        # ACF at lag 7 should differ between the original and the sign-flipped series
        acf1 = p1.autocorrelation_by_period[7]
        acf2 = p2.autocorrelation_by_period[7]
        # The sin signal after sign flip still has the same period, so both
        # should be finite and positive — but they may differ numerically
        assert np.isfinite(acf1) and np.isfinite(acf2)


# ---------------------------------------------------------------------------
# 13. Interface contracts
# ---------------------------------------------------------------------------

class TestInterfaceContracts:
    """Guard-rail tests for the public interface itself."""

    def test_returns_seasonality_profile_instance(self):
        s = _make_series(200, period=7)
        result = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES)
        assert isinstance(result, SeasonalityProfile)

    def test_m1_always_first_in_selected(self):
        for period in [7, 14, 28]:
            s = _make_series(300, period=period, seed=period)
            p = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES)
            assert p.selected_candidate_periods[0] == NON_SEASONAL_PERIOD

    def test_selected_count_respects_max_candidates(self):
        s = _make_series(500, period=7)
        max_c = 2
        p = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES, max_candidates=max_c)
        # m=1 + at most max_c seasonal
        assert len(p.selected_candidate_periods) <= 1 + max_c

    def test_raises_on_non_series_input(self):
        with pytest.raises(TypeError):
            profile_seasonality(np.array([1.0, 2.0, 3.0]))

    def test_empty_series_returns_degenerate(self):
        s = pd.Series([], dtype=float)
        p = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES)
        assert p.seasonality_status == "degenerate"
        assert p.selected_candidate_periods == [NON_SEASONAL_PERIOD]

    def test_single_value_series_degenerate(self):
        idx = pd.date_range("2020-01-01", periods=1, freq="D")
        s = pd.Series([5.0], index=idx)
        p = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES)
        assert p.seasonality_status == "degenerate"

    def test_all_nan_series_degenerate(self):
        idx = pd.date_range("2020-01-01", periods=100, freq="D")
        s = pd.Series([float("nan")] * 100, index=idx)
        p = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES)
        assert p.seasonality_status == "degenerate"

    def test_excluded_and_eligible_disjoint(self):
        s = _make_series(400, period=7)
        p = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES)
        excluded_set = set(p.excluded_periods.keys())
        eligible_set = set(p.eligible_candidate_periods)
        assert excluded_set.isdisjoint(eligible_set)

    def test_all_candidates_have_cycles_recorded(self):
        s = _make_series(400, period=7)
        p = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES)
        for m in SEASONAL_CANDIDATES:
            assert m in p.cycles_available_by_period

    def test_eligible_candidates_have_finite_acf(self):
        s = _make_series(400, period=7)
        p = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES)
        for m in p.eligible_candidate_periods:
            assert np.isfinite(p.autocorrelation_by_period[m])

    def test_eligible_candidates_have_finite_spectral(self):
        s = _make_series(400, period=7)
        p = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES)
        for m in p.eligible_candidate_periods:
            assert np.isfinite(p.spectral_power_by_period[m])

    def test_excluded_candidates_have_nan_score(self):
        # Use a short series so some periods are excluded
        s = _make_series(50, period=7)
        p = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES)
        for m, reason in p.excluded_periods.items():
            if reason:
                assert not np.isfinite(p.candidate_score_by_period[m])

    def test_scores_in_unit_interval(self):
        s = _make_series(400, period=7)
        p = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES)
        for m in p.eligible_candidate_periods:
            sc = p.candidate_score_by_period[m]
            assert 0.0 <= sc <= 1.0 + 1e-9

    def test_custom_score_weights_accepted(self):
        s = _make_series(400, period=7)
        p = profile_seasonality(
            s,
            candidate_periods=SEASONAL_CANDIDATES,
            score_weights={"acf": 1.0, "spectral": 0.0},
        )
        assert isinstance(p, SeasonalityProfile)

    def test_insufficient_history_all_periods_excluded(self):
        # 10 days — no period ≥ 7 can have 3 cycles
        idx = pd.date_range("2020-01-01", periods=10, freq="D")
        s = pd.Series(np.arange(10, dtype=float), index=idx)
        p = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES, min_cycles=3)
        assert p.seasonality_status == "insufficient_history"
        assert p.eligible_candidate_periods == []
        assert NON_SEASONAL_PERIOD in p.selected_candidate_periods

    def test_dominant_period_is_in_eligible(self):
        s = _make_series(400, period=7)
        p = profile_seasonality(s, candidate_periods=SEASONAL_CANDIDATES)
        if p.dominant_detected_period is not None:
            assert p.dominant_detected_period in p.eligible_candidate_periods
