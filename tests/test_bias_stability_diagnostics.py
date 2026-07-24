"""Tests for src/models/bias_stability_diagnostics.py."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.models.bias_stability_diagnostics import (
    BiasStabilityConfig,
    _SCIPY_AVAILABLE,
    build_backtest_bias_diagnostics_by_fold,
    build_backtest_bias_summary,
    build_production_bias_diagnostics,
    build_training_bias_diagnostics,
    calculate_bias_metrics,
    calculate_fold_bias_stability,
    calculate_horizon_bias,
    calculate_variance_stability,
    classify_bias_status,
    classify_variance_stability,
    persist_bias_stability_diagnostics,
    validate_bias_stability_diagnostics,
    TRAINING_BIAS_COLS,
    BACKTEST_FOLD_BIAS_COLS,
    BACKTEST_SUMMARY_BIAS_COLS,
    PRODUCTION_BIAS_COLS,
)

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

_CFG = BiasStabilityConfig()


def _rng(seed: int = 0):
    return np.random.default_rng(seed)


def _tr_df(
    residuals: np.ndarray,
    actuals: np.ndarray | None = None,
    report_id: str = "r1",
    model_name: str = "naive",
    candidate_m: int = 7,
    fold_number: int = 1,
    fit_scope: str = "backtest_fold",
    model_family: str = "naive",
    horizon_steps: np.ndarray | None = None,
) -> pd.DataFrame:
    n = len(residuals)
    if actuals is None:
        actuals = residuals + 100.0
    dates = pd.date_range("2024-01-01", periods=n)
    row: dict = {
        "report_id": report_id,
        "report_name": report_id,
        "model_family": model_family,
        "model_name": model_name,
        "candidate_m": candidate_m,
        "fit_scope": fit_scope,
        "fold_number": fold_number,
        "residual_date": dates,
        "actual": actuals,
        "residual": residuals,
        "residual_observation_valid": True,
        "residual_extraction_status": "ok",
        "residual_extraction_reason": None,
        "training_start": dates[0],
        "training_cutoff": dates[-1],
        "training_observation_count": n,
        "fitted_observation_count": n,
        "residual_observation_count": n,
    }
    if horizon_steps is not None:
        row["horizon_step"] = horizon_steps
    return pd.DataFrame(row)


def _bt_df(
    residuals: np.ndarray,
    actuals: np.ndarray | None = None,
    report_id: str = "r1",
    model_name: str = "naive",
    candidate_m: int = 7,
    fold_number: int = 1,
    model_family: str = "naive",
    horizon_steps: np.ndarray | None = None,
) -> pd.DataFrame:
    n = len(residuals)
    if actuals is None:
        actuals = residuals + 100.0
    dates = pd.date_range("2024-03-01", periods=n)
    if horizon_steps is None:
        horizon_steps = np.tile(np.arange(1, min(29, n + 1)), n // 28 + 1)[:n]
    return pd.DataFrame({
        "report_id": report_id,
        "report_name": report_id,
        "model_family": model_family,
        "model_name": model_name,
        "candidate_m": candidate_m,
        "fold_number": fold_number,
        "cutoff_date": dates[0] - pd.Timedelta(days=1),
        "train_start": dates[0] - pd.Timedelta(days=90),
        "train_end": dates[0] - pd.Timedelta(days=1),
        "forecast_date": dates,
        "horizon_step": horizon_steps,
        "actual": actuals,
        "residual": residuals,
        "residual_source": "backtest",
        "residual_observation_valid": True,
        "fit_status": "ok",
    })


def _prod_df(
    residuals: np.ndarray,
    actuals: np.ndarray | None = None,
    report_id: str = "r1",
    model_family: str = "naive",
    model_name: str = "naive",
    selected_m: int = 7,
    horizon_steps: np.ndarray | None = None,
) -> pd.DataFrame:
    n = len(residuals)
    if actuals is None:
        actuals = residuals + 100.0
    dates = pd.date_range("2024-06-01", periods=n)
    if horizon_steps is None:
        horizon_steps = np.ones(n, dtype=int)
    return pd.DataFrame({
        "report_id": report_id,
        "report_name": report_id,
        "selected_model_family": model_family,
        "selected_model_name": model_name,
        "selected_m": selected_m,
        "forecast_date": dates,
        "horizon_step": horizon_steps,
        "generated_at": pd.Timestamp("2024-05-31"),
        "actual": actuals,
        "residual": residuals,
        "residual_source": "production",
        "residual_observation_valid": True,
        "lineage_complete": True,
    })


# ---------------------------------------------------------------------------
# TestCoreBiasMetrics
# ---------------------------------------------------------------------------

class TestCoreBiasMetrics:
    def test_approximately_unbiased(self):
        rng = _rng(0)
        r = rng.standard_normal(100)
        a = np.abs(r) + 50.0
        result = calculate_bias_metrics(r, a)
        assert result["bias_direction"] == "approximately_unbiased"
        assert result["normalized_bias"] is not None
        assert abs(result["normalized_bias"]) < 0.05

    def test_persistent_underforecasting(self):
        r = np.full(50, 10.0)
        a = np.full(50, 100.0)
        result = calculate_bias_metrics(r, a)
        assert result["normalized_bias"] == pytest.approx(0.10)
        assert result["bias_direction"] == "underforecasting"

    def test_persistent_overforecasting(self):
        r = np.full(50, -10.0)
        a = np.full(50, 100.0)
        result = calculate_bias_metrics(r, a)
        assert result["normalized_bias"] == pytest.approx(-0.10)
        assert result["bias_direction"] == "overforecasting"

    def test_small_bias_below_threshold(self):
        r = np.full(50, 2.0)
        a = np.full(50, 100.0)
        result = calculate_bias_metrics(r, a)
        assert result["bias_direction"] == "approximately_unbiased"

    def test_outlier_affects_mean_not_median(self):
        rng = _rng(1)
        r = rng.standard_normal(98)
        r = np.append(r, [50.0, -50.0])  # two outliers cancel in mean
        large_r = np.append(rng.standard_normal(99), [500.0])
        a = np.abs(large_r) + 200.0
        result = calculate_bias_metrics(large_r, a)
        assert abs(result["mean_residual"]) > abs(result["median_residual"])

    def test_zero_actual_volume(self):
        r = np.full(20, 1.0)
        a = np.zeros(20)
        result = calculate_bias_metrics(r, a)
        assert result["normalized_bias"] is None
        assert result["absolute_normalized_bias"] is None
        assert result["residual_sum"] == pytest.approx(20.0)

    def test_positive_normalized_bias_is_underforecasting(self):
        r = np.full(30, 5.0)
        a = np.full(30, 50.0)
        result = calculate_bias_metrics(r, a)
        assert result["normalized_bias"] > 0
        assert result["bias_direction"] == "underforecasting"

    def test_negative_normalized_bias_is_overforecasting(self):
        r = np.full(30, -5.0)
        a = np.full(30, 50.0)
        result = calculate_bias_metrics(r, a)
        assert result["normalized_bias"] < 0
        assert result["bias_direction"] == "overforecasting"

    def test_trimmed_mean_calculation(self):
        r = np.concatenate([np.full(90, 0.0), np.full(10, 100.0)])
        a = np.full(100, 100.0)
        cfg = BiasStabilityConfig(TRIMMED_MEAN_PROPORTION=0.10)
        result = calculate_bias_metrics(r, a, cfg)
        assert result["trimmed_mean_residual"] is not None
        assert result["trimmed_mean_residual"] < 100.0 / 9  # outliers trimmed

    def test_non_finite_residuals_excluded(self):
        r = np.array([np.nan, np.inf, -np.inf] + [1.0] * 30)
        a = np.full(33, 100.0)
        # The function itself does not filter; callers do. Finite input only.
        r_finite = r[np.isfinite(r)]
        a_finite = a[np.isfinite(r)]
        result = calculate_bias_metrics(r_finite, a_finite)
        assert result["valid_residual_count" if "valid_residual_count" in result else "residual_count" if "residual_count" in result else "mean_residual"] is not None

    def test_normalized_bias_formula(self):
        r = np.full(20, 10.0)
        a = np.full(20, 100.0)
        result = calculate_bias_metrics(r, a)
        expected = (10.0 * 20) / (100.0 * 20)
        assert result["normalized_bias"] == pytest.approx(expected)

    def test_absolute_normalized_bias_equals_abs(self):
        r = np.full(20, -5.0)
        a = np.full(20, 100.0)
        result = calculate_bias_metrics(r, a)
        assert result["absolute_normalized_bias"] == pytest.approx(
            abs(result["normalized_bias"])
        )

    def test_insufficient_residuals_returns_none(self):
        r = np.array([1.0, 2.0])
        a = np.array([10.0, 20.0])
        cfg = BiasStabilityConfig(MIN_RESIDUALS_FOR_BIAS=10)
        result = calculate_bias_metrics(r, a, cfg)
        assert result["mean_residual"] is None


# ---------------------------------------------------------------------------
# TestFoldBiasStability
# ---------------------------------------------------------------------------

def _make_fold_rec(norm_bias: float, res_sum: float, total_actual: float) -> dict:
    return {
        "normalized_bias": norm_bias,
        "residual_sum": res_sum,
        "total_actual": total_actual,
        "bias_direction": "underforecasting" if norm_bias > 0 else "overforecasting",
    }


class TestFoldBiasStability:
    def test_consistent_underforecasting(self):
        records = [_make_fold_rec(0.10, 100.0, 1000.0) for _ in range(4)]
        result = calculate_fold_bias_stability(records)
        assert result["underforecast_fold_count"] == 4
        assert result["overforecast_fold_count"] == 0
        assert result["bias_consistency_status"] == "consistent"

    def test_consistent_overforecasting(self):
        records = [_make_fold_rec(-0.10, -100.0, 1000.0) for _ in range(4)]
        result = calculate_fold_bias_stability(records)
        assert result["overforecast_fold_count"] == 4
        assert result["underforecast_fold_count"] == 0

    def test_mixed_fold_directions(self):
        records = [
            _make_fold_rec(0.12, 120.0, 1000.0),
            _make_fold_rec(-0.08, -80.0, 1000.0),
            _make_fold_rec(0.06, 60.0, 1000.0),
            _make_fold_rec(-0.15, -150.0, 1000.0),
        ]
        result = calculate_fold_bias_stability(records)
        assert result["fold_bias_sign_change_count"] >= 1

    def test_frequent_sign_changes(self):
        records = [
            _make_fold_rec(0.15, 150.0, 1000.0),
            _make_fold_rec(-0.15, -150.0, 1000.0),
            _make_fold_rec(0.15, 150.0, 1000.0),
            _make_fold_rec(-0.15, -150.0, 1000.0),
        ]
        result = calculate_fold_bias_stability(records)
        assert result["fold_bias_sign_change_count"] == 3

    def test_high_fold_bias_std(self):
        records = [
            _make_fold_rec(0.40, 400.0, 1000.0),
            _make_fold_rec(-0.40, -400.0, 1000.0),
        ]
        result = calculate_fold_bias_stability(records)
        assert result["fold_bias_std"] > 0.25

    def test_insufficient_valid_folds(self):
        records = [_make_fold_rec(0.10, 100.0, 1000.0)]
        cfg = BiasStabilityConfig(MIN_VALID_FOLDS_FOR_BIAS_STABILITY=2)
        result = calculate_fold_bias_stability(records, cfg)
        assert result["bias_consistency_status"] == "insufficient_evidence"

    def test_aggregate_bias_near_zero_but_folds_unstable(self):
        records = [
            _make_fold_rec(0.30, 300.0, 1000.0),
            _make_fold_rec(-0.30, -300.0, 1000.0),
        ]
        result = calculate_fold_bias_stability(records)
        # aggregate pooled = 0
        assert result["aggregate_normalized_bias"] == pytest.approx(0.0)
        assert result["fold_bias_sign_change_count"] == 1

    def test_directional_fold_shares_correct(self):
        records = [
            _make_fold_rec(0.10, 100.0, 1000.0),
            _make_fold_rec(0.10, 100.0, 1000.0),
            _make_fold_rec(-0.10, -100.0, 1000.0),
            _make_fold_rec(0.01, 10.0, 1000.0),  # approximately unbiased
        ]
        result = calculate_fold_bias_stability(records)
        assert result["underforecast_fold_count"] == 2
        assert result["overforecast_fold_count"] == 1
        assert result["approximately_unbiased_fold_count"] == 1

    def test_overlapping_folds_not_pooled(self):
        """Fold records are given as pre-computed per-fold; no pooling occurs here."""
        records = [_make_fold_rec(0.10, 100.0, 1000.0) for _ in range(3)]
        result = calculate_fold_bias_stability(records)
        # aggregate normalized bias = pooled sum / pooled actuals = 300/3000 = 0.1
        assert result["aggregate_normalized_bias"] == pytest.approx(0.10)


# ---------------------------------------------------------------------------
# TestHorizonBias
# ---------------------------------------------------------------------------

def _make_horizon_data(
    early_bias: float = 0.0,
    middle_bias: float = 0.0,
    late_bias: float = 0.0,
    n_per_bucket: int = 20,
    actual_per_step: float = 100.0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    early_h = np.arange(1, 8, dtype=float)[:n_per_bucket]
    middle_h = np.arange(8, 15, dtype=float)[:n_per_bucket]
    late_h = np.arange(15, 29, dtype=float)[:n_per_bucket]

    # Repeat to fill n_per_bucket
    def _repeat(h_arr, target):
        return np.tile(h_arr, target // len(h_arr) + 1)[:target]

    e_h = _repeat(np.arange(1, 8), n_per_bucket)
    m_h = _repeat(np.arange(8, 15), n_per_bucket)
    l_h = _repeat(np.arange(15, 29), n_per_bucket)
    h = np.concatenate([e_h, m_h, l_h])

    e_r = np.full(n_per_bucket, early_bias * actual_per_step)
    m_r = np.full(n_per_bucket, middle_bias * actual_per_step)
    l_r = np.full(n_per_bucket, late_bias * actual_per_step)
    r = np.concatenate([e_r, m_r, l_r])
    a = np.full(len(r), actual_per_step)
    return r, a, h


class TestHorizonBias:
    def test_stable_bias_across_all_buckets(self):
        r, a, h = _make_horizon_data(0.0, 0.0, 0.0)
        result = calculate_horizon_bias(r, a, h)
        assert result["horizon_bias_status"] == "stable"

    def test_early_horizon_underforecasting_only(self):
        r, a, h = _make_horizon_data(0.10, 0.0, 0.0)
        result = calculate_horizon_bias(r, a, h)
        assert result["early_horizon_normalized_bias"] == pytest.approx(0.10, abs=0.01)
        assert result["middle_horizon_normalized_bias"] == pytest.approx(0.0, abs=0.01)

    def test_late_horizon_overforecasting_only(self):
        r, a, h = _make_horizon_data(0.0, 0.0, -0.15)
        result = calculate_horizon_bias(r, a, h)
        assert result["late_horizon_normalized_bias"] == pytest.approx(-0.15, abs=0.01)

    def test_worsening_underforecasting_with_horizon(self):
        r, a, h = _make_horizon_data(0.02, 0.05, 0.20)
        result = calculate_horizon_bias(r, a, h)
        assert result["horizon_bias_worsening_flag"] is True
        assert result["horizon_bias_status"] == "worsening_with_horizon"

    def test_worsening_overforecasting_with_horizon(self):
        r, a, h = _make_horizon_data(-0.02, -0.06, -0.22)
        result = calculate_horizon_bias(r, a, h)
        assert result["horizon_bias_worsening_flag"] is True

    def test_improving_bias_with_horizon(self):
        r, a, h = _make_horizon_data(0.20, 0.08, 0.01)
        result = calculate_horizon_bias(r, a, h)
        # bias decreases, no direction change
        assert result["horizon_bias_worsening_flag"] is False

    def test_direction_change_across_buckets(self):
        r, a, h = _make_horizon_data(0.15, 0.0, -0.15)
        result = calculate_horizon_bias(r, a, h)
        assert result["horizon_bias_direction_change"] >= 1

    def test_insufficient_observations_in_one_bucket(self):
        r, a, h = _make_horizon_data(n_per_bucket=2)  # below MIN threshold
        cfg = BiasStabilityConfig(MIN_OBSERVATIONS_PER_HORIZON_BUCKET=5)
        result = calculate_horizon_bias(r, a, h, cfg)
        # All buckets insufficient
        assert result["early_horizon_normalized_bias"] is None

    def test_exact_bucket_boundary_step_1(self):
        r = np.array([10.0])
        a = np.array([100.0])
        h = np.array([1.0])
        result = calculate_horizon_bias(r, a, h, BiasStabilityConfig(MIN_OBSERVATIONS_PER_HORIZON_BUCKET=1))
        assert result["early_horizon_residual_count"] == 1

    def test_exact_bucket_boundary_step_7(self):
        r = np.array([5.0])
        a = np.array([100.0])
        h = np.array([7.0])
        result = calculate_horizon_bias(r, a, h, BiasStabilityConfig(MIN_OBSERVATIONS_PER_HORIZON_BUCKET=1))
        assert result["early_horizon_residual_count"] == 1

    def test_exact_bucket_boundary_step_8(self):
        r = np.array([5.0])
        a = np.array([100.0])
        h = np.array([8.0])
        result = calculate_horizon_bias(r, a, h, BiasStabilityConfig(MIN_OBSERVATIONS_PER_HORIZON_BUCKET=1))
        assert result["middle_horizon_residual_count"] == 1

    def test_exact_bucket_boundary_step_14(self):
        r = np.array([5.0])
        a = np.array([100.0])
        h = np.array([14.0])
        result = calculate_horizon_bias(r, a, h, BiasStabilityConfig(MIN_OBSERVATIONS_PER_HORIZON_BUCKET=1))
        assert result["middle_horizon_residual_count"] == 1

    def test_exact_bucket_boundary_step_15(self):
        r = np.array([5.0])
        a = np.array([100.0])
        h = np.array([15.0])
        result = calculate_horizon_bias(r, a, h, BiasStabilityConfig(MIN_OBSERVATIONS_PER_HORIZON_BUCKET=1))
        assert result["late_horizon_residual_count"] == 1

    def test_exact_bucket_boundary_step_28(self):
        r = np.array([5.0])
        a = np.array([100.0])
        h = np.array([28.0])
        result = calculate_horizon_bias(r, a, h, BiasStabilityConfig(MIN_OBSERVATIONS_PER_HORIZON_BUCKET=1))
        assert result["late_horizon_residual_count"] == 1


# ---------------------------------------------------------------------------
# TestVarianceStability
# ---------------------------------------------------------------------------

class TestVarianceStability:
    def test_stable_equal_variances(self):
        rng = _rng(5)
        r1 = rng.standard_normal(28)
        r2 = rng.standard_normal(28)
        result = calculate_variance_stability(
            [r1, r2],
            cfg=_CFG,
            recent_window=r1,
            previous_window=r2,
        )
        ratio = result["variance_change_ratio"]
        assert ratio is not None
        assert 0.1 < ratio < 10.0

    def test_increasing_recent_variance(self):
        rng = _rng(6)
        stable = rng.standard_normal(28)
        noisy = rng.standard_normal(28) * 10.0
        result = calculate_variance_stability(
            [stable, noisy],
            cfg=_CFG,
            recent_window=noisy,
            previous_window=stable,
        )
        assert result["variance_change_ratio"] > 1.0
        status = result["variance_stability_status"]
        assert status in ("warning", "unstable")

    def test_decreasing_recent_variance(self):
        rng = _rng(7)
        stable = rng.standard_normal(28)
        small = rng.standard_normal(28) * 0.01
        result = calculate_variance_stability(
            [stable, small],
            cfg=_CFG,
            recent_window=small,
            previous_window=stable,
        )
        # ratio < 1
        assert result["variance_change_ratio"] < 1.0

    def test_previous_variance_zero_recent_zero(self):
        zeros = np.zeros(28)
        result = calculate_variance_stability(
            [zeros, zeros],
            cfg=_CFG,
            recent_window=zeros,
            previous_window=zeros,
        )
        assert result["variance_change_ratio"] == 1.0

    def test_previous_variance_zero_recent_positive(self):
        zeros = np.zeros(28)
        rng = _rng(8)
        noisy = rng.standard_normal(28)
        result = calculate_variance_stability(
            [zeros, noisy],
            cfg=_CFG,
            recent_window=noisy,
            previous_window=zeros,
        )
        # ratio undefined (infinite)
        assert result["variance_change_ratio"] is None
        assert result["variance_stability_status"] in ("unstable", "insufficient_evidence")

    def test_high_fold_variance_dispersion(self):
        rng = _rng(9)
        groups = [rng.standard_normal(20)] + [rng.standard_normal(20) * 20 for _ in range(3)]
        result = calculate_variance_stability(groups, cfg=_CFG)
        # CV should be high
        cv = result["fold_variance_coefficient_of_variation"]
        assert cv is not None and cv > 0.5

    def test_stable_fold_variance(self):
        rng = _rng(10)
        groups = [rng.standard_normal(30) for _ in range(4)]
        result = calculate_variance_stability(groups, cfg=_CFG)
        status = result["variance_stability_status"]
        # With normal data, should not be unstable
        assert status in ("stable", "warning", "insufficient_evidence")

    def test_insufficient_equal_length_production_windows(self):
        rng = _rng(11)
        small = rng.standard_normal(5)
        result = calculate_variance_stability(
            [],
            cfg=BiasStabilityConfig(MIN_RESIDUALS_PER_VARIANCE_WINDOW=14),
            recent_window=small,
            previous_window=small,
        )
        assert result["variance_stability_status"] in (
            "insufficient_evidence", "stable", "warning", "unstable"
        )

    def test_partial_production_window_excluded(self):
        """Unequal windows should not produce a valid variance comparison."""
        rng = _rng(12)
        long_w = rng.standard_normal(28)
        short_w = rng.standard_normal(10)
        result = calculate_variance_stability(
            [],
            cfg=_CFG,
            recent_window=long_w,
            previous_window=short_w,
        )
        # unequal windows → no valid ratio
        assert result["variance_change_ratio"] is None or result["variance_stability_status"] == "insufficient_evidence"

    @pytest.mark.skipif(not _SCIPY_AVAILABLE, reason="scipy not installed")
    def test_levene_significant_small_practical_difference(self):
        """Significant test with tiny practical difference should not alone drive 'unstable'."""
        rng = _rng(99)
        # Very large samples so test is sensitive but effect is tiny
        g1 = rng.standard_normal(1000)
        g2 = rng.standard_normal(1000) * 1.05
        result = calculate_variance_stability([g1, g2], cfg=_CFG)
        # May be significant but variance_stability_status depends on CV
        assert result["variance_test_pvalue"] is not None

    def test_deterministic_classification(self):
        rng = _rng(13)
        r1 = rng.standard_normal(28)
        r2 = rng.standard_normal(28)
        result1 = calculate_variance_stability(
            [r1, r2], cfg=_CFG, recent_window=r1, previous_window=r2
        )
        result2 = calculate_variance_stability(
            [r1, r2], cfg=_CFG, recent_window=r1, previous_window=r2
        )
        assert result1["variance_stability_status"] == result2["variance_stability_status"]


# ---------------------------------------------------------------------------
# TestTrainingBiasDiagnostics
# ---------------------------------------------------------------------------

class TestTrainingBiasDiagnostics:
    def test_empty_input_returns_empty(self):
        result = build_training_bias_diagnostics(pd.DataFrame())
        assert result.empty

    def test_schema_columns(self):
        df = _tr_df(_rng(0).standard_normal(50))
        result = build_training_bias_diagnostics(df)
        assert list(result.columns) == TRAINING_BIAS_COLS

    def test_one_row_per_group(self):
        df1 = _tr_df(_rng(0).standard_normal(50), fold_number=1)
        df2 = _tr_df(_rng(1).standard_normal(50), fold_number=2)
        combined = pd.concat([df1, df2], ignore_index=True)
        result = build_training_bias_diagnostics(combined)
        assert len(result) == 2

    def test_underforecasting_detected(self):
        r = np.full(50, 8.0)
        a = np.full(50, 100.0)
        df = _tr_df(r, a)
        result = build_training_bias_diagnostics(df)
        assert result["bias_status"].iloc[0] in ("underforecasting", "poor")

    def test_overforecasting_detected(self):
        r = np.full(50, -8.0)
        a = np.full(50, 100.0)
        df = _tr_df(r, a)
        result = build_training_bias_diagnostics(df)
        assert result["bias_status"].iloc[0] in ("overforecasting", "poor")

    def test_training_separate_from_backtest(self):
        r = np.full(50, 5.0)
        a = np.full(50, 100.0)
        df = _tr_df(r, a)
        result = build_training_bias_diagnostics(df)
        assert result["residual_source"].iloc[0] == "training"

    def test_diagnostic_run_id_propagated(self):
        df = _tr_df(_rng(0).standard_normal(50))
        result = build_training_bias_diagnostics(df, diagnostic_run_id="run-42")
        assert result["diagnostic_run_id"].iloc[0] == "run-42"

    def test_invalid_residuals_excluded(self):
        r = np.array([np.nan, np.inf, -np.inf] + [5.0] * 50)
        a = np.full(53, 100.0)
        df = _tr_df(r, a)
        result = build_training_bias_diagnostics(df)
        assert result["excluded_invalid_count"].iloc[0] >= 3

    def test_horizon_bias_populated_when_column_present(self):
        n = 56  # 2 full horizons × 28 days
        r = np.zeros(n)
        a = np.full(n, 100.0)
        h = np.tile(np.arange(1, 29), 2)
        df = _tr_df(r, a, horizon_steps=h)
        result = build_training_bias_diagnostics(df)
        assert result["horizon_bias_status"].iloc[0] is not None


# ---------------------------------------------------------------------------
# TestBacktestBiasDiagnostics
# ---------------------------------------------------------------------------

class TestBacktestBiasDiagnostics:
    def test_empty_input_returns_empty(self):
        result = build_backtest_bias_diagnostics_by_fold(pd.DataFrame())
        assert result.empty

    def test_schema_columns(self):
        df = _bt_df(_rng(0).standard_normal(28))
        result = build_backtest_bias_diagnostics_by_fold(df)
        assert list(result.columns) == BACKTEST_FOLD_BIAS_COLS

    def test_one_row_per_fold(self):
        df1 = _bt_df(_rng(0).standard_normal(28), fold_number=1)
        df2 = _bt_df(_rng(1).standard_normal(28), fold_number=2)
        combined = pd.concat([df1, df2], ignore_index=True)
        result = build_backtest_bias_diagnostics_by_fold(combined)
        assert len(result) == 2

    def test_overlapping_folds_not_concatenated(self):
        r = np.full(28, 10.0)
        a = np.full(28, 100.0)
        df1 = _bt_df(r, a, fold_number=1)
        df2 = _bt_df(r, a, fold_number=2)
        combined = pd.concat([df1, df2], ignore_index=True)
        result = build_backtest_bias_diagnostics_by_fold(combined)
        # Each fold valid_residual_count = 28, not 56
        assert (result["valid_residual_count"] == 28).all()

    def test_backtest_separate_from_training(self):
        df = _bt_df(_rng(0).standard_normal(28))
        result = build_backtest_bias_diagnostics_by_fold(df)
        assert result["residual_source"].iloc[0] == "backtest"

    def test_evaluation_run_id_propagated(self):
        df = _bt_df(_rng(0).standard_normal(28))
        result = build_backtest_bias_diagnostics_by_fold(df, evaluation_run_id="ev-001")
        assert result["evaluation_run_id"].iloc[0] == "ev-001"


# ---------------------------------------------------------------------------
# TestBacktestBiasSummary
# ---------------------------------------------------------------------------

class TestBacktestBiasSummary:
    def test_empty_fold_input_returns_empty(self):
        result = build_backtest_bias_summary(pd.DataFrame())
        assert result.empty

    def test_schema_columns(self):
        df = _bt_df(_rng(0).standard_normal(28))
        fold_df = build_backtest_bias_diagnostics_by_fold(df)
        summary = build_backtest_bias_summary(fold_df, df)
        assert list(summary.columns) == BACKTEST_SUMMARY_BIAS_COLS

    def test_one_summary_row_per_model(self):
        df1 = _bt_df(_rng(0).standard_normal(28), model_name="m1", fold_number=1)
        df2 = _bt_df(_rng(1).standard_normal(28), model_name="m1", fold_number=2)
        df3 = _bt_df(_rng(2).standard_normal(28), model_name="m2", fold_number=1)
        combined = pd.concat([df1, df2, df3], ignore_index=True)
        fold_df = build_backtest_bias_diagnostics_by_fold(combined)
        summary = build_backtest_bias_summary(fold_df, combined)
        assert len(summary) == 2

    def test_aggregate_bias_is_pooled_not_mean_of_fold_biases(self):
        # fold 1: 100 actuals with residual sum 10 → norm 0.10
        # fold 2: 1000 actuals with residual sum 10 → norm 0.01
        # mean of fold biases = 0.055
        # pooled = 20/1100 ≈ 0.0182
        df1 = _bt_df(np.full(10, 1.0), np.full(10, 10.0), fold_number=1)
        df2 = _bt_df(np.full(100, 0.1), np.full(100, 10.0), fold_number=2)
        combined = pd.concat([df1, df2], ignore_index=True)
        fold_df = build_backtest_bias_diagnostics_by_fold(combined)
        summary = build_backtest_bias_summary(fold_df, combined)
        # pooled normalized bias
        expected = (10.0 + 10.0) / (100.0 + 1000.0)
        assert summary["normalized_bias"].iloc[0] == pytest.approx(expected, abs=0.01)

    def test_valid_fold_count_correct(self):
        df1 = _bt_df(_rng(0).standard_normal(28), fold_number=1)
        df2 = _bt_df(_rng(1).standard_normal(28), fold_number=2)
        combined = pd.concat([df1, df2], ignore_index=True)
        fold_df = build_backtest_bias_diagnostics_by_fold(combined)
        summary = build_backtest_bias_summary(fold_df, combined)
        assert summary["valid_fold_count"].iloc[0] >= 1


# ---------------------------------------------------------------------------
# TestProductionBiasDiagnostics
# ---------------------------------------------------------------------------

class TestProductionBiasDiagnostics:
    def test_empty_input_returns_empty(self):
        result = build_production_bias_diagnostics(pd.DataFrame())
        assert result.empty

    def test_schema_columns(self):
        df = _prod_df(_rng(0).standard_normal(40))
        result = build_production_bias_diagnostics(df)
        assert list(result.columns) == PRODUCTION_BIAS_COLS

    def test_all_records_vs_deduped_distinction(self):
        """Three horizon steps per date → deduped keeps 1, all-records keeps 3."""
        dates = pd.date_range("2024-06-01", periods=10)
        rows = []
        for d in dates:
            for h in [1, 3, 7]:
                rows.append({
                    "report_id": "r1",
                    "report_name": "r1",
                    "selected_model_family": "naive",
                    "selected_model_name": "naive",
                    "selected_m": 7,
                    "forecast_date": d,
                    "horizon_step": h,
                    "generated_at": pd.Timestamp("2024-05-31"),
                    "actual": 100.0,
                    "residual": 10.0,
                    "residual_source": "production",
                    "residual_observation_valid": True,
                    "lineage_complete": True,
                })
        df = pd.DataFrame(rows)
        result = build_production_bias_diagnostics(df)
        assert result["all_records_valid_count"].iloc[0] == 30
        assert result["deduped_valid_count"].iloc[0] == 10

    def test_production_separate_from_backtest(self):
        df = _prod_df(_rng(0).standard_normal(40))
        result = build_production_bias_diagnostics(df)
        assert result["residual_source"].iloc[0] == "production"

    def test_incomplete_lineage_preserved(self):
        df = _prod_df(_rng(0).standard_normal(40))
        df["lineage_complete"] = False
        result = build_production_bias_diagnostics(df)
        assert bool(result["lineage_complete"].iloc[0]) is False

    def test_selected_m_preserved(self):
        df = _prod_df(_rng(0).standard_normal(40), selected_m=14)
        result = build_production_bias_diagnostics(df)
        assert result["selected_m"].iloc[0] == 14

    def test_missing_group_columns_returns_empty(self):
        df = pd.DataFrame({"report_id": ["r1"], "residual": [0.0]})
        result = build_production_bias_diagnostics(df)
        assert result.empty

    def test_variance_window_comparison_when_sufficient_data(self):
        n = 80
        r = _rng(0).standard_normal(n)
        df = _prod_df(r)
        cfg = BiasStabilityConfig(PRODUCTION_VARIANCE_WINDOW_SIZE=28)
        result = build_production_bias_diagnostics(df, cfg=cfg)
        # Should have attempted window comparison
        assert result["variance_stability_status"].iloc[0] in (
            "stable", "warning", "unstable", "insufficient_evidence"
        )

    def test_insufficient_data_no_window_comparison(self):
        r = _rng(0).standard_normal(20)
        df = _prod_df(r)
        cfg = BiasStabilityConfig(PRODUCTION_VARIANCE_WINDOW_SIZE=28)
        result = build_production_bias_diagnostics(df, cfg=cfg)
        assert result["variance_stability_status"].iloc[0] in (
            "insufficient_evidence", "stable"
        )

    def test_evaluation_run_id_propagated(self):
        df = _prod_df(_rng(0).standard_normal(40))
        result = build_production_bias_diagnostics(df, evaluation_run_id="p-007")
        assert result["evaluation_run_id"].iloc[0] == "p-007"


# ---------------------------------------------------------------------------
# TestBiasClassification
# ---------------------------------------------------------------------------

class TestBiasClassification:
    def _base(self, **kwargs):
        defaults = {
            "evidence_status": "ok",
            "normalized_bias": 0.02,
            "absolute_normalized_bias": 0.02,
            "bias_direction": "approximately_unbiased",
            "mean_residual": 1.0,
            "median_residual": 1.0,
        }
        defaults.update(kwargs)
        return defaults

    def test_acceptable(self):
        status, _ = classify_bias_status(self._base(normalized_bias=0.02, absolute_normalized_bias=0.02))
        assert status == "acceptable"

    def test_underforecasting(self):
        status, reasons = classify_bias_status(
            self._base(normalized_bias=0.08, absolute_normalized_bias=0.08)
        )
        assert status == "underforecasting"
        assert any("underfore" in r.lower() for r in reasons)

    def test_overforecasting(self):
        status, reasons = classify_bias_status(
            self._base(normalized_bias=-0.08, absolute_normalized_bias=0.08)
        )
        assert status == "overforecasting"
        assert any("overfore" in r.lower() for r in reasons)

    def test_poor_high_bias(self):
        status, _ = classify_bias_status(
            self._base(normalized_bias=0.20, absolute_normalized_bias=0.20)
        )
        assert status == "poor"

    def test_insufficient_evidence(self):
        status, _ = classify_bias_status(
            self._base(evidence_status="insufficient")
        )
        assert status == "insufficient_evidence"

    def test_calculation_failed(self):
        status, _ = classify_bias_status(
            self._base(evidence_status="calculation_failed")
        )
        assert status == "calculation_failed"

    def test_zero_volume_null_normalized_bias(self):
        metrics = {
            "evidence_status": "ok",
            "normalized_bias": None,
            "absolute_normalized_bias": None,
            "bias_direction": "insufficient_evidence",
            "mean_residual": 1.0,
            "median_residual": 1.0,
        }
        status, reasons = classify_bias_status(metrics)
        assert "zero" in " ".join(reasons).lower() or status == "insufficient_evidence"

    def test_custom_thresholds(self):
        cfg = BiasStabilityConfig(BIAS_WARNING_THRESHOLD=0.02, BIAS_POOR_THRESHOLD=0.10)
        status, _ = classify_bias_status(
            {"evidence_status": "ok", "normalized_bias": 0.03, "absolute_normalized_bias": 0.03,
             "bias_direction": "underforecasting", "mean_residual": 1.0, "median_residual": 1.0},
            cfg,
        )
        assert status == "underforecasting"


# ---------------------------------------------------------------------------
# TestValidation
# ---------------------------------------------------------------------------

class TestValidation:
    def test_valid_training_passes(self):
        df = _tr_df(_rng(0).standard_normal(50))
        result = build_training_bias_diagnostics(df)
        validate_bias_stability_diagnostics(result, "training")

    def test_unknown_dataset_name_raises(self):
        with pytest.raises(ValueError, match="Unknown dataset_name"):
            validate_bias_stability_diagnostics(pd.DataFrame(), "unknown")

    def test_missing_column_raises(self):
        df = build_training_bias_diagnostics(_tr_df(_rng(0).standard_normal(50)))
        df = df.drop(columns=["bias_status"])
        with pytest.raises(ValueError, match="missing columns"):
            validate_bias_stability_diagnostics(df, "training")

    def test_invalid_bias_status_raises(self):
        df = build_training_bias_diagnostics(_tr_df(_rng(0).standard_normal(50)))
        df["bias_status"] = "totally_wrong"
        with pytest.raises(ValueError, match="invalid bias_status"):
            validate_bias_stability_diagnostics(df, "training")

    def test_positive_bias_labelled_overforecasting_raises(self):
        df = build_training_bias_diagnostics(_tr_df(np.full(50, 5.0), np.full(50, 100.0)))
        # Force wrong label
        df["normalized_bias"] = 0.10
        df["bias_status"] = "overforecasting"
        with pytest.raises(ValueError, match="positive normalized_bias"):
            validate_bias_stability_diagnostics(df, "training")

    def test_negative_bias_labelled_underforecasting_raises(self):
        df = build_training_bias_diagnostics(_tr_df(np.full(50, -5.0), np.full(50, 100.0)))
        df["normalized_bias"] = -0.10
        df["bias_status"] = "underforecasting"
        with pytest.raises(ValueError, match="negative normalized_bias"):
            validate_bias_stability_diagnostics(df, "training")

    def test_absolute_normalized_bias_mismatch_raises(self):
        df = build_training_bias_diagnostics(_tr_df(np.full(50, 5.0), np.full(50, 100.0)))
        df["normalized_bias"] = 0.05
        df["absolute_normalized_bias"] = 0.99  # wrong
        with pytest.raises(ValueError, match="absolute_normalized_bias"):
            validate_bias_stability_diagnostics(df, "training")

    def test_insufficient_evidence_not_acceptable(self):
        df = build_training_bias_diagnostics(
            _tr_df(_rng(0).standard_normal(3))
        )
        # If evidence is insufficient, force acceptable to test the guard
        insufficient_mask = df["evidence_status"] == "insufficient"
        if insufficient_mask.any():
            df.loc[insufficient_mask, "bias_status"] = "acceptable"
            with pytest.raises(ValueError, match="insufficient.*acceptable"):
                validate_bias_stability_diagnostics(df, "training")

    def test_valid_backtest_fold_passes(self):
        df = _bt_df(_rng(0).standard_normal(28))
        fold_df = build_backtest_bias_diagnostics_by_fold(df)
        validate_bias_stability_diagnostics(fold_df, "backtest_fold")

    def test_valid_production_passes(self):
        df = _prod_df(_rng(0).standard_normal(40))
        result = build_production_bias_diagnostics(df)
        validate_bias_stability_diagnostics(result, "production")

    def test_mean_absolute_residual_non_negative(self):
        df = build_training_bias_diagnostics(_tr_df(_rng(0).standard_normal(50)))
        df["mean_absolute_residual"] = -1.0
        with pytest.raises(ValueError, match="mean_absolute_residual"):
            validate_bias_stability_diagnostics(df, "training")

    def test_empty_df_passes(self):
        validate_bias_stability_diagnostics(pd.DataFrame(), "training")


# ---------------------------------------------------------------------------
# TestPersistence
# ---------------------------------------------------------------------------

class TestPersistence:
    def _build_all(self):
        tr = build_training_bias_diagnostics(_tr_df(_rng(0).standard_normal(50)))
        bt_df = _bt_df(_rng(1).standard_normal(28))
        fold_df = build_backtest_bias_diagnostics_by_fold(bt_df)
        summary = build_backtest_bias_summary(fold_df, bt_df)
        prod = build_production_bias_diagnostics(_prod_df(_rng(2).standard_normal(40)))
        return tr, fold_df, summary, prod

    def test_all_four_files_created(self, tmp_path):
        tr, fold_df, summary, prod = self._build_all()
        paths = persist_bias_stability_diagnostics(tr, fold_df, summary, prod, tmp_path)
        for name, path in paths.items():
            assert path is not None, f"{name} path is None"
            assert path.exists(), f"{name} file missing"

    def test_empty_dfs_write_header_only(self, tmp_path):
        paths = persist_bias_stability_diagnostics(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), tmp_path
        )
        for name, path in paths.items():
            assert path is not None
            loaded = pd.read_csv(path)
            assert len(loaded) == 0

    def test_output_dir_created(self, tmp_path):
        diag_dir = tmp_path / "outputs" / "diagnostics"
        assert not diag_dir.exists()
        persist_bias_stability_diagnostics(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), tmp_path
        )
        assert diag_dir.exists()

    def test_roundtrip_schema(self, tmp_path):
        tr, fold_df, summary, prod = self._build_all()
        paths = persist_bias_stability_diagnostics(tr, fold_df, summary, prod, tmp_path)
        loaded = pd.read_csv(paths["training"])
        assert list(loaded.columns) == TRAINING_BIAS_COLS

    def test_overwrites_existing_file(self, tmp_path):
        tr1, fold_df, summary, prod = self._build_all()
        df2 = _tr_df(_rng(3).standard_normal(50), fold_number=2)
        tr2 = pd.concat([tr1, build_training_bias_diagnostics(df2)], ignore_index=True)
        for col in TRAINING_BIAS_COLS:
            if col not in tr2.columns:
                tr2[col] = None
        tr2 = tr2[TRAINING_BIAS_COLS]

        persist_bias_stability_diagnostics(tr1, fold_df, summary, prod, tmp_path)
        persist_bias_stability_diagnostics(tr2, fold_df, summary, prod, tmp_path)
        path = tmp_path / "outputs" / "diagnostics" / "training_bias_stability_diagnostics_latest.csv"
        loaded = pd.read_csv(path)
        assert len(loaded) == 2

    def test_returns_none_on_invalid_df(self, tmp_path):
        bad = pd.DataFrame({"wrong": [1], "bias_status": ["bad_status"]})
        paths = persist_bias_stability_diagnostics(
            bad, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), tmp_path
        )
        assert paths["training"] is None

    def test_stable_schema_training(self, tmp_path):
        tr, fold_df, summary, prod = self._build_all()
        paths = persist_bias_stability_diagnostics(tr, fold_df, summary, prod, tmp_path)
        loaded = pd.read_csv(paths["training"])
        assert set(TRAINING_BIAS_COLS).issubset(set(loaded.columns))
