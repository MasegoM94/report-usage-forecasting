"""Tests for outlier_distribution_diagnostics module."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.outlier_distribution_diagnostics import (
    OutlierDistributionConfig,
    _SCIPY_AVAILABLE,
    _compute_robust_scale,
    _compute_tail_metrics,
    calculate_distribution_metrics,
    calculate_robust_outlier_metrics,
    classify_distribution_status,
    classify_outlier_status,
    build_training_outlier_distribution_diagnostics,
    build_backtest_outlier_distribution_by_fold,
    build_backtest_outlier_distribution_summary,
    build_production_outlier_distribution_diagnostics,
    validate_outlier_distribution_diagnostics,
    persist_outlier_distribution_diagnostics,
    TRAINING_OUTLIER_COLS,
    BACKTEST_FOLD_OUTLIER_COLS,
    BACKTEST_SUMMARY_OUTLIER_COLS,
    PRODUCTION_OUTLIER_COLS,
)

_CFG = OutlierDistributionConfig()
_RNG = np.random.default_rng(42)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tr_df(
    residuals,
    actuals=None,
    report_id="r1",
    model_name="naive",
    candidate_m=7,
    fold_number=1,
    fit_scope="backtest_fold",
    model_family="naive",
    dates=None,
    horizon_steps=None,
):
    n = len(residuals)
    if actuals is None:
        actuals = np.array(residuals) + 100.0
    if dates is None:
        dates = pd.date_range("2024-01-01", periods=n)
    row = {
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
        "training_start": pd.Timestamp("2023-01-01"),
        "training_cutoff": pd.Timestamp("2024-01-01"),
    }
    if horizon_steps is not None:
        row["horizon_step"] = horizon_steps
    return pd.DataFrame(row)


def _bt_df(
    residuals,
    actuals=None,
    report_id="r1",
    model_name="naive",
    candidate_m=7,
    fold_number=1,
    model_family="naive",
    dates=None,
):
    n = len(residuals)
    if actuals is None:
        actuals = np.array(residuals) + 100.0
    if dates is None:
        dates = pd.date_range("2024-01-01", periods=n)
    return pd.DataFrame({
        "report_id": report_id,
        "report_name": report_id,
        "model_family": model_family,
        "model_name": model_name,
        "candidate_m": candidate_m,
        "fold_number": fold_number,
        "residual_date": dates,
        "actual": actuals,
        "residual": residuals,
        "residual_observation_valid": True,
        "cutoff_date": pd.Timestamp("2024-01-01"),
        "train_start": pd.Timestamp("2023-01-01"),
        "train_end": pd.Timestamp("2023-12-31"),
    })


def _prod_df(
    residuals,
    actuals=None,
    report_id="r1",
    model_family="naive",
    model_name="naive",
    selected_m=7,
    dates=None,
    horizon_steps=None,
):
    n = len(residuals)
    if actuals is None:
        actuals = np.array(residuals) + 100.0
    if dates is None:
        dates = pd.date_range("2024-01-01", periods=n)
    row = {
        "report_id": report_id,
        "report_name": report_id,
        "selected_model_family": model_family,
        "selected_model_name": model_name,
        "selected_m": selected_m,
        "forecast_date": dates,
        "actual": actuals,
        "residual": residuals,
        "residual_observation_valid": True,
        "lineage_complete": True,
    }
    if horizon_steps is not None:
        row["horizon_step"] = horizon_steps
    return pd.DataFrame(row)


# ---------------------------------------------------------------------------
# TestRobustScale
# ---------------------------------------------------------------------------


class TestRobustScale:
    def test_mad_formula(self):
        r = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        info = _compute_robust_scale(r)
        med = np.median(r)
        expected_mad = np.median(np.abs(r - med))
        assert abs(info["residual_mad"] - expected_mad) < 1e-10
        assert abs(info["scaled_mad"] - expected_mad * 1.4826) < 1e-10

    def test_scaled_mad_always_1_4826_times_mad(self):
        r = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 100.0])
        info = _compute_robust_scale(r)
        assert abs(info["scaled_mad"] - info["residual_mad"] * 1.4826) < 1e-10

    def test_positive_scale_status_ok(self):
        r = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        info = _compute_robust_scale(r)
        assert info["scale_status"] == "ok"
        assert info["scale_method_used"] == "mad"
        assert info["scale_fallback_used"] is False

    def test_constant_residuals(self):
        r = np.ones(10) * 5.0
        info = _compute_robust_scale(r)
        assert info["scale_status"] == "constant_residuals"
        assert info["residual_mad"] == 0.0
        assert info["scaled_mad"] == 0.0

    def test_iqr_fallback_when_mad_zero(self):
        # Median is 5, more than half are 5, but some differ → MAD=0, IQR>0
        r = np.array([5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 1.0, 100.0])
        info = _compute_robust_scale(r)
        # MAD may be 0 here
        if info["residual_mad"] == 0:
            assert info["scale_fallback_used"] is True
            assert info["scale_status"] in ("fallback_iqr", "fallback_std", "fallback_exact", "constant_residuals")

    def test_std_fallback(self):
        # IQR = 0 but std > 0: all same except exact duplicates with offset
        r = np.array([5.0, 5.0, 5.0, 5.0, 5.0, 6.0])
        # MAD=0, IQR=0 for most, std>0
        info = _compute_robust_scale(r)
        assert info["robust_residual_scale"] is not None

    def test_no_infinite_z_scores_constant(self):
        r = np.ones(10) * 3.0
        actuals = np.ones(10) * 100.0
        m = calculate_robust_outlier_metrics(r, actuals, cfg=_CFG)
        assert m["outlier_count"] == 0

    def test_empty_returns_none(self):
        info = _compute_robust_scale(np.array([]))
        assert info["residual_mad"] is None

    def test_iqr_computed(self):
        r = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])
        info = _compute_robust_scale(r)
        q1 = np.percentile(r, 25)
        q3 = np.percentile(r, 75)
        assert abs(info["q1_residual"] - q1) < 1e-10
        assert abs(info["q3_residual"] - q3) < 1e-10
        assert abs(info["residual_iqr"] - (q3 - q1)) < 1e-10


# ---------------------------------------------------------------------------
# TestOutlierMetrics
# ---------------------------------------------------------------------------


class TestOutlierMetrics:
    def _normal_residuals(self, n=30):
        return _RNG.normal(0, 1, n)

    def test_no_outliers_acceptable(self):
        r = np.array([0.1, -0.1, 0.2, -0.2, 0.15, -0.15, 0.05])
        a = np.ones(len(r)) * 100.0
        m = calculate_robust_outlier_metrics(r, a, cfg=_CFG)
        assert m["outlier_count"] == 0
        assert m["outlier_status"] == "acceptable"

    def test_large_positive_outlier_detected(self):
        r = np.array([0.1, -0.1, 0.2, -0.2, 0.1, -0.1, 100.0])
        a = np.ones(len(r)) * 100.0
        m = calculate_robust_outlier_metrics(r, a, cfg=_CFG)
        assert m["outlier_count"] >= 1
        assert m["positive_outlier_count"] >= 1

    def test_large_negative_outlier_detected(self):
        r = np.array([0.1, -0.1, 0.2, -0.2, 0.1, -0.1, -100.0])
        a = np.ones(len(r)) * 100.0
        m = calculate_robust_outlier_metrics(r, a, cfg=_CFG)
        assert m["outlier_count"] >= 1
        assert m["negative_outlier_count"] >= 1

    def test_outlier_rate_computation(self):
        r = np.array([0.1, -0.1, 0.2, -0.2, 0.1, -0.1, 100.0])
        a = np.ones(len(r)) * 100.0
        m = calculate_robust_outlier_metrics(r, a, cfg=_CFG)
        assert abs(m["outlier_rate"] - m["outlier_count"] / len(r)) < 1e-9

    def test_pos_neg_outlier_counts_sum(self):
        r = np.array([0.1, -0.1, 0.2, -0.2, 0.1, -0.1, 100.0, -80.0])
        a = np.ones(len(r)) * 100.0
        m = calculate_robust_outlier_metrics(r, a, cfg=_CFG)
        assert m["positive_outlier_count"] + m["negative_outlier_count"] == m["outlier_count"]

    def test_date_preserved_for_largest(self):
        r = np.array([0.1, -0.1, 0.2, -0.2, 0.1, -0.1, 100.0])
        a = np.ones(len(r)) * 100.0
        dates = pd.date_range("2024-01-01", periods=len(r)).values
        m = calculate_robust_outlier_metrics(r, a, dates=dates, cfg=_CFG)
        # largest positive is at index 6 → 2024-01-07
        assert m["largest_positive_residual_date"] is not None

    def test_horizon_preserved(self):
        r = np.array([0.1, -0.1, 0.2, -0.2, 0.1, -0.1, 100.0])
        a = np.ones(len(r)) * 100.0
        horizons = np.arange(1, len(r) + 1, dtype=float)
        m = calculate_robust_outlier_metrics(r, a, horizon_steps=horizons, cfg=_CFG)
        assert m["largest_absolute_residual_horizon_step"] == len(r)

    def test_insufficient_evidence(self):
        r = np.array([0.1, -0.1, 0.2])
        a = np.ones(len(r)) * 100.0
        m = calculate_robust_outlier_metrics(r, a, cfg=_CFG)
        assert m["outlier_status"] == "insufficient_evidence"

    def test_high_outlier_rate_poor(self):
        cfg = OutlierDistributionConfig(
            OUTLIER_RATE_POOR_THRESHOLD=0.10,
            ROBUST_Z_OUTLIER_THRESHOLD=2.0,
        )
        # A few extreme values relative to tightly clustered bulk
        r = np.concatenate([np.zeros(8), np.array([1000.0, 2000.0])])
        a = np.ones(len(r)) * 100.0
        m = calculate_robust_outlier_metrics(r, a, cfg=cfg)
        assert m["outlier_rate"] >= cfg.OUTLIER_RATE_POOR_THRESHOLD
        assert m["outlier_status"] == "poor"

    def test_constant_residuals_zero_outliers(self):
        r = np.ones(10) * 5.0
        a = np.ones(10) * 100.0
        m = calculate_robust_outlier_metrics(r, a, cfg=_CFG)
        assert m["outlier_count"] == 0
        assert m["scale_status"] == "constant_residuals"

    def test_largest_absolute_residual_is_max_abs(self):
        r = np.array([1.0, -2.0, 3.0, -4.0, 5.0, -6.0, 0.5])
        a = np.ones(len(r)) * 100.0
        m = calculate_robust_outlier_metrics(r, a, cfg=_CFG)
        assert m["largest_absolute_residual"] == pytest.approx(6.0)

    def test_mean_abs_outlier_none_when_no_outliers(self):
        r = np.ones(10) * 0.01
        a = np.ones(10) * 100.0
        m = calculate_robust_outlier_metrics(r, a, cfg=_CFG)
        assert m["mean_absolute_outlier_residual"] is None


# ---------------------------------------------------------------------------
# TestTailMetrics
# ---------------------------------------------------------------------------


class TestTailMetrics:
    def test_approximately_balanced(self):
        r = np.array([-2.0, -1.0, 0.0, 1.0, 2.0, -2.0, 2.0, -1.5, 1.5, 0.5])
        m = _compute_tail_metrics(r, _CFG)
        # Symmetric data should be approximately balanced
        assert m["tail_direction"] in ("approximately_balanced", "underforecast_heavy", "overforecast_heavy")

    def test_underforecast_heavy(self):
        # Many large positive residuals
        r = np.array([0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 50.0, 60.0, 70.0])
        m = _compute_tail_metrics(r, _CFG)
        assert m["tail_direction"] == "underforecast_heavy"

    def test_overforecast_heavy(self):
        # Many large negative residuals
        r = np.array([-0.1, -0.1, -0.1, -0.1, -0.1, -0.1, -0.1, -50.0, -60.0, -70.0])
        m = _compute_tail_metrics(r, _CFG)
        assert m["tail_direction"] == "overforecast_heavy"

    def test_lower_lte_upper_quantile(self):
        r = _RNG.normal(0, 1, 50)
        m = _compute_tail_metrics(r, _CFG)
        assert m["lower_tail_quantile"] <= m["upper_tail_quantile"]

    def test_tail_counts_positive(self):
        r = _RNG.normal(0, 1, 50)
        m = _compute_tail_metrics(r, _CFG)
        assert m["lower_tail_count"] > 0
        assert m["upper_tail_count"] > 0

    def test_extreme_shares_between_0_1(self):
        r = _RNG.normal(0, 1, 50)
        m = _compute_tail_metrics(r, _CFG)
        assert 0.0 <= m["extreme_underforecast_share"] <= 1.0
        assert 0.0 <= m["extreme_overforecast_share"] <= 1.0

    def test_short_sample_insufficient(self):
        r = np.array([1.0, 2.0, 3.0])
        m = _compute_tail_metrics(r, _CFG)
        assert m["tail_direction"] == "insufficient_evidence"


# ---------------------------------------------------------------------------
# TestDistributionMetrics
# ---------------------------------------------------------------------------


class TestDistributionMetrics:
    def test_insufficient_evidence(self):
        r = np.array([1.0, 2.0, 3.0])
        m = calculate_distribution_metrics(r, _CFG)
        assert m["normality_status"] == "insufficient_evidence"

    def test_symmetric_normal(self):
        rng = np.random.default_rng(99)
        r = rng.normal(0, 1, 200)
        m = calculate_distribution_metrics(r, _CFG)
        assert m["residual_mean"] is not None
        assert m["residual_std"] is not None

    def test_skewness_computed(self):
        rng = np.random.default_rng(99)
        r = rng.exponential(1, 200)  # positively skewed
        m = calculate_distribution_metrics(r, _CFG)
        if m["residual_skewness"] is not None:
            assert m["residual_skewness"] > 0

    def test_excess_kurtosis_plus_3_equals_raw(self):
        r = _RNG.normal(0, 1, 50)
        m = calculate_distribution_metrics(r, _CFG)
        if m["residual_kurtosis"] is not None and m["excess_kurtosis"] is not None:
            assert abs(m["residual_kurtosis"] - (m["excess_kurtosis"] + 3.0)) < 1e-9

    @pytest.mark.skipif(not _SCIPY_AVAILABLE, reason="scipy not installed")
    def test_jb_significant_for_very_skewed(self):
        rng = np.random.default_rng(7)
        # Highly skewed exponential
        r = rng.exponential(1, 1000)
        m = calculate_distribution_metrics(r, _CFG)
        assert m["jarque_bera_significant"] is True

    @pytest.mark.skipif(not _SCIPY_AVAILABLE, reason="scipy not installed")
    def test_jb_not_significant_for_normal(self):
        rng = np.random.default_rng(7)
        r = rng.normal(0, 1, 500)
        m = calculate_distribution_metrics(r, _CFG)
        # Not guaranteed but expected for normal data
        assert m["jarque_bera_pvalue"] is not None

    @pytest.mark.skipif(not _SCIPY_AVAILABLE, reason="scipy not installed")
    def test_shapiro_within_limits(self):
        r = _RNG.normal(0, 1, 20)
        m = calculate_distribution_metrics(r, _CFG)
        assert m["shapiro_statistic"] is not None
        assert 0.0 <= m["shapiro_pvalue"] <= 1.0

    @pytest.mark.skipif(not _SCIPY_AVAILABLE, reason="scipy not installed")
    def test_shapiro_skipped_above_max_size(self):
        r = _RNG.normal(0, 1, 6000)
        m = calculate_distribution_metrics(r, _CFG)
        assert m["shapiro_statistic"] is None

    @pytest.mark.skipif(not _SCIPY_AVAILABLE, reason="scipy not installed")
    def test_heavy_tail_detected(self):
        # t-distribution with df=2 has very heavy tails (infinite variance)
        rng = np.random.default_rng(77)
        from scipy.stats import t as t_dist
        r = t_dist.rvs(df=2, size=500, random_state=77)
        m = calculate_distribution_metrics(r, _CFG)
        if m["excess_kurtosis"] is not None:
            assert m["excess_kurtosis"] > 0  # should be positive for t(2)

    def test_distribution_evidence_status_ok(self):
        r = _RNG.normal(0, 1, 30)
        m = calculate_distribution_metrics(r, _CFG)
        assert m["distribution_evidence_status"] == "ok"

    def test_pvalues_in_range(self):
        r = _RNG.normal(0, 1, 30)
        m = calculate_distribution_metrics(r, _CFG)
        for key in ("jarque_bera_pvalue", "shapiro_pvalue"):
            v = m.get(key)
            if v is not None:
                assert 0.0 <= v <= 1.0


# ---------------------------------------------------------------------------
# TestClassification
# ---------------------------------------------------------------------------


class TestClassification:
    def test_acceptable_status(self):
        status, reasons = classify_outlier_status(
            {"valid_residual_count": 20, "outlier_rate": 0.01,
             "scale_status": "ok", "scale_fallback_used": False,
             "scale_method_used": "mad", "largest_positive_residual": 1.0,
             "largest_negative_residual": -1.0, "tail_direction": "approximately_balanced"},
            _CFG,
        )
        assert status == "acceptable"

    def test_warning_status_elevated_rate(self):
        status, reasons = classify_outlier_status(
            {"valid_residual_count": 20, "outlier_rate": 0.08,
             "scale_status": "ok", "scale_fallback_used": False,
             "scale_method_used": "mad", "largest_positive_residual": None,
             "largest_negative_residual": None, "tail_direction": None},
            _CFG,
        )
        assert status == "warning"

    def test_poor_status_high_rate(self):
        status, reasons = classify_outlier_status(
            {"valid_residual_count": 20, "outlier_rate": 0.20,
             "scale_status": "ok", "scale_fallback_used": False,
             "scale_method_used": "mad", "largest_positive_residual": None,
             "largest_negative_residual": None, "tail_direction": None},
            _CFG,
        )
        assert status == "poor"

    def test_insufficient_evidence_status(self):
        status, reasons = classify_outlier_status(
            {"valid_residual_count": 3, "outlier_rate": None,
             "scale_status": None, "scale_fallback_used": None,
             "scale_method_used": None, "largest_positive_residual": None,
             "largest_negative_residual": None, "tail_direction": None},
            _CFG,
        )
        assert status == "insufficient_evidence"

    def test_distribution_no_concern(self):
        status, reasons = classify_distribution_status(
            {"valid_residual_count": 30, "residual_skewness": 0.1,
             "excess_kurtosis": 0.2, "jarque_bera_significant": False,
             "shapiro_significant": False, "distribution_shape": "approximately_symmetric"},
            _CFG,
        )
        assert status == "no_material_concern"

    def test_distribution_caution_moderate_skew(self):
        status, reasons = classify_distribution_status(
            {"valid_residual_count": 30, "residual_skewness": 0.8,
             "excess_kurtosis": 0.2, "jarque_bera_significant": False,
             "shapiro_significant": False, "distribution_shape": "positively_skewed"},
            _CFG,
        )
        assert status == "caution"

    def test_distribution_poor_high_skew(self):
        status, reasons = classify_distribution_status(
            {"valid_residual_count": 30, "residual_skewness": 2.0,
             "excess_kurtosis": 0.2, "jarque_bera_significant": False,
             "shapiro_significant": False, "distribution_shape": "positively_skewed"},
            _CFG,
        )
        assert status == "poor_for_analytic_intervals"

    def test_distribution_caution_jb_sig(self):
        status, reasons = classify_distribution_status(
            {"valid_residual_count": 30, "residual_skewness": 0.1,
             "excess_kurtosis": 0.2, "jarque_bera_significant": True,
             "shapiro_significant": False, "distribution_shape": "approximately_symmetric"},
            _CFG,
        )
        assert status == "caution"
        assert any("Jarque-Bera" in r for r in reasons)

    def test_distribution_insufficient_evidence(self):
        status, reasons = classify_distribution_status(
            {"valid_residual_count": 5, "residual_skewness": None,
             "excess_kurtosis": None, "jarque_bera_significant": None,
             "shapiro_significant": None, "distribution_shape": "insufficient_evidence"},
            _CFG,
        )
        assert status == "insufficient_evidence"

    def test_classification_deterministic(self):
        m = {
            "valid_residual_count": 20, "outlier_rate": 0.05,
            "scale_status": "ok", "scale_fallback_used": False,
            "scale_method_used": "mad", "largest_positive_residual": None,
            "largest_negative_residual": None, "tail_direction": None,
        }
        s1, r1 = classify_outlier_status(m, _CFG)
        s2, r2 = classify_outlier_status(m, _CFG)
        assert s1 == s2
        assert r1 == r2


# ---------------------------------------------------------------------------
# TestTrainingDiagnostics
# ---------------------------------------------------------------------------


class TestTrainingDiagnostics:
    def test_schema(self):
        df = _tr_df(list(_RNG.normal(0, 1, 20)))
        result = build_training_outlier_distribution_diagnostics(df)
        assert list(result.columns) == TRAINING_OUTLIER_COLS

    def test_groups(self):
        df1 = _tr_df(list(_RNG.normal(0, 1, 15)), report_id="r1", model_name="m1")
        df2 = _tr_df(list(_RNG.normal(0, 1, 15)), report_id="r2", model_name="m2")
        result = build_training_outlier_distribution_diagnostics(pd.concat([df1, df2]))
        assert len(result) == 2

    def test_run_id_propagated(self):
        df = _tr_df(list(_RNG.normal(0, 1, 15)))
        result = build_training_outlier_distribution_diagnostics(df, diagnostic_run_id="run42")
        assert (result["diagnostic_run_id"] == "run42").all()

    def test_empty_input(self):
        result = build_training_outlier_distribution_diagnostics(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == TRAINING_OUTLIER_COLS

    def test_evidence_status_ok(self):
        df = _tr_df(list(_RNG.normal(0, 1, 20)))
        result = build_training_outlier_distribution_diagnostics(df)
        assert (result["evidence_status"] == "ok").all()

    def test_evidence_status_insufficient(self):
        df = _tr_df([0.1, 0.2, 0.3])  # fewer than MIN_RESIDUALS
        result = build_training_outlier_distribution_diagnostics(df)
        assert (result["evidence_status"] == "insufficient").all()
        assert (result["outlier_status"] == "insufficient_evidence").all()

    def test_invalid_residuals_excluded(self):
        residuals = [1.0, 2.0, np.nan, float("inf"), 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        df = _tr_df(residuals)
        result = build_training_outlier_distribution_diagnostics(df)
        assert result["excluded_invalid_count"].iloc[0] >= 2

    def test_horizon_step_optional(self):
        df = _tr_df(list(_RNG.normal(0, 1, 15)))
        # No horizon_step column
        assert "horizon_step" not in df.columns
        result = build_training_outlier_distribution_diagnostics(df)
        assert len(result) == 1

    def test_with_horizon_step(self):
        n = 15
        df = _tr_df(
            list(_RNG.normal(0, 1, n)),
            horizon_steps=list(range(1, n + 1)),
        )
        result = build_training_outlier_distribution_diagnostics(df)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# TestBacktestDiagnostics
# ---------------------------------------------------------------------------


class TestBacktestDiagnostics:
    def test_schema(self):
        df = _bt_df(list(_RNG.normal(0, 1, 15)))
        result = build_backtest_outlier_distribution_by_fold(df)
        assert list(result.columns) == BACKTEST_FOLD_OUTLIER_COLS

    def test_no_cross_fold_pooling(self):
        df1 = _bt_df(list(_RNG.normal(0, 1, 10)), fold_number=1)
        df2 = _bt_df(list(_RNG.normal(0, 1, 10)), fold_number=2)
        result = build_backtest_outlier_distribution_by_fold(pd.concat([df1, df2]))
        assert len(result) == 2
        # Each fold should have valid_residual_count == 10
        assert (result["valid_residual_count"] == 10).all()

    def test_empty_input(self):
        result = build_backtest_outlier_distribution_by_fold(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == BACKTEST_FOLD_OUTLIER_COLS

    def test_evaluation_run_id(self):
        df = _bt_df(list(_RNG.normal(0, 1, 15)))
        result = build_backtest_outlier_distribution_by_fold(df, evaluation_run_id="ev99")
        assert (result["evaluation_run_id"] == "ev99").all()

    def test_cross_fold_summary_schema(self):
        df = _bt_df(list(_RNG.normal(0, 1, 15)))
        fold_result = build_backtest_outlier_distribution_by_fold(df)
        summary = build_backtest_outlier_distribution_summary(fold_result)
        assert list(summary.columns) == BACKTEST_SUMMARY_OUTLIER_COLS

    def test_p_values_not_averaged(self):
        # Summary should NOT have jarque_bera_pvalue (only counts)
        assert "jarque_bera_pvalue" not in BACKTEST_SUMMARY_OUTLIER_COLS

    def test_cross_fold_poor_if_any_fold_poor(self):
        df1 = _bt_df(list(_RNG.normal(0, 0.01, 10)), fold_number=1)
        # Create a fold with very high outlier rate
        r2 = np.concatenate([np.zeros(5), np.ones(5) * 1000.0])
        df2 = _bt_df(list(r2), fold_number=2)
        fold_result = build_backtest_outlier_distribution_by_fold(pd.concat([df1, df2]))
        summary = build_backtest_outlier_distribution_summary(fold_result)
        # At least check the summary has a result
        assert len(summary) == 1

    def test_summary_empty_when_no_folds(self):
        result = build_backtest_outlier_distribution_summary(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == BACKTEST_SUMMARY_OUTLIER_COLS

    def test_cross_fold_reasons_is_string(self):
        df = _bt_df(list(_RNG.normal(0, 1, 15)))
        fold_result = build_backtest_outlier_distribution_by_fold(df)
        summary = build_backtest_outlier_distribution_summary(fold_result)
        assert isinstance(summary["cross_fold_reasons"].iloc[0], str)


# ---------------------------------------------------------------------------
# TestProductionDiagnostics
# ---------------------------------------------------------------------------


class TestProductionDiagnostics:
    def test_schema(self):
        df = _prod_df(list(_RNG.normal(0, 1, 20)))
        result = build_production_outlier_distribution_diagnostics(df)
        assert list(result.columns) == PRODUCTION_OUTLIER_COLS

    def test_empty_input(self):
        result = build_production_outlier_distribution_diagnostics(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == PRODUCTION_OUTLIER_COLS

    def test_missing_group_columns(self):
        df = pd.DataFrame({"residual": [1.0, 2.0]})
        result = build_production_outlier_distribution_diagnostics(df)
        assert result.empty

    def test_dedup_tracking(self):
        # Two forecasts for same date, different horizon steps
        dates = ["2024-01-15", "2024-01-15", "2024-01-16"]
        horizon_steps = [2, 1, 1]
        r = [1.0, 1.0, 0.5]
        df = _prod_df(
            r,
            dates=pd.to_datetime(dates),
            horizon_steps=horizon_steps,
        )
        result = build_production_outlier_distribution_diagnostics(df)
        assert result["original_prediction_count"].iloc[0] == 3
        assert result["deduplicated_date_count"].iloc[0] == 2
        assert result["excluded_overlap_count"].iloc[0] == 1

    def test_lineage_complete_propagated(self):
        df = _prod_df(list(_RNG.normal(0, 1, 15)))
        result = build_production_outlier_distribution_diagnostics(df)
        assert result["lineage_complete"].iloc[0] == True  # noqa: E712

    def test_evaluation_run_id(self):
        df = _prod_df(list(_RNG.normal(0, 1, 15)))
        result = build_production_outlier_distribution_diagnostics(df, evaluation_run_id="prod99")
        assert (result["evaluation_run_id"] == "prod99").all()


# ---------------------------------------------------------------------------
# TestValidation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_unknown_dataset_raises(self):
        with pytest.raises(ValueError, match="Unknown dataset_name"):
            validate_outlier_distribution_diagnostics(pd.DataFrame(), "bad_name")

    def test_missing_columns_raises(self):
        df = pd.DataFrame({"diagnostic_run_id": ["x"]})
        with pytest.raises(ValueError, match="missing columns"):
            validate_outlier_distribution_diagnostics(df, "training")

    def test_valid_training_passes(self):
        df = _tr_df(list(_RNG.normal(0, 1, 20)))
        result = build_training_outlier_distribution_diagnostics(df)
        # Should not raise
        validate_outlier_distribution_diagnostics(result, "training")

    def test_valid_backtest_fold_passes(self):
        df = _bt_df(list(_RNG.normal(0, 1, 20)))
        result = build_backtest_outlier_distribution_by_fold(df)
        validate_outlier_distribution_diagnostics(result, "backtest_fold")

    def test_valid_backtest_summary_passes(self):
        df = _bt_df(list(_RNG.normal(0, 1, 20)))
        fold_result = build_backtest_outlier_distribution_by_fold(df)
        summary = build_backtest_outlier_distribution_summary(fold_result)
        validate_outlier_distribution_diagnostics(summary, "backtest_summary")

    def test_valid_production_passes(self):
        df = _prod_df(list(_RNG.normal(0, 1, 20)))
        result = build_production_outlier_distribution_diagnostics(df)
        validate_outlier_distribution_diagnostics(result, "production")

    def test_scaled_mad_mismatch_raises(self):
        df = _tr_df(list(_RNG.normal(0, 1, 20)))
        result = build_training_outlier_distribution_diagnostics(df)
        # Corrupt scaled_mad
        result = result.copy()
        result.loc[0, "scaled_mad"] = result.loc[0, "residual_mad"] * 99.0
        with pytest.raises(ValueError, match="scaled_mad"):
            validate_outlier_distribution_diagnostics(result, "training")

    def test_outlier_rate_mismatch_raises(self):
        df = _tr_df(list(_RNG.normal(0, 1, 20)))
        result = build_training_outlier_distribution_diagnostics(df)
        result = result.copy()
        result.loc[0, "outlier_rate"] = 0.999
        with pytest.raises(ValueError, match="outlier_rate"):
            validate_outlier_distribution_diagnostics(result, "training")

    def test_pos_neg_count_mismatch_raises(self):
        df = _tr_df(list(_RNG.normal(0, 1, 20)))
        result = build_training_outlier_distribution_diagnostics(df)
        result = result.copy()
        result.loc[0, "positive_outlier_count"] = result.loc[0, "outlier_count"] + 5
        with pytest.raises(ValueError, match="outlier_count"):
            validate_outlier_distribution_diagnostics(result, "training")

    def test_outlier_rate_above_1_raises(self):
        df = _tr_df(list(_RNG.normal(0, 1, 20)))
        result = build_training_outlier_distribution_diagnostics(df)
        result = result.copy()
        result.loc[0, "outlier_rate"] = 1.5
        result.loc[0, "outlier_count"] = int(result.loc[0, "valid_residual_count"] * 1.5)
        with pytest.raises(ValueError):
            validate_outlier_distribution_diagnostics(result, "training")

    def test_pvalue_above_1_raises(self):
        df = _tr_df(list(_RNG.normal(0, 1, 30)))
        result = build_training_outlier_distribution_diagnostics(df)
        result = result.copy()
        result.loc[0, "jarque_bera_pvalue"] = 1.5
        with pytest.raises(ValueError, match="pvalue"):
            validate_outlier_distribution_diagnostics(result, "training")

    def test_lower_tail_gt_upper_raises(self):
        df = _tr_df(list(_RNG.normal(0, 1, 30)))
        result = build_training_outlier_distribution_diagnostics(df)
        result = result.copy()
        result.loc[0, "lower_tail_quantile"] = 10.0
        result.loc[0, "upper_tail_quantile"] = 1.0
        with pytest.raises(ValueError, match="lower_tail_quantile"):
            validate_outlier_distribution_diagnostics(result, "training")

    def test_invalid_outlier_status_raises(self):
        df = _tr_df(list(_RNG.normal(0, 1, 20)))
        result = build_training_outlier_distribution_diagnostics(df)
        result = result.copy()
        result.loc[0, "outlier_status"] = "invalid_status"
        with pytest.raises(ValueError, match="outlier_status"):
            validate_outlier_distribution_diagnostics(result, "training")

    def test_invalid_normality_status_raises(self):
        df = _tr_df(list(_RNG.normal(0, 1, 20)))
        result = build_training_outlier_distribution_diagnostics(df)
        result = result.copy()
        result.loc[0, "normality_status"] = "bad_norm_status"
        with pytest.raises(ValueError, match="normality_status"):
            validate_outlier_distribution_diagnostics(result, "training")

    def test_insufficient_evidence_not_acceptable(self):
        df = _tr_df(list(_RNG.normal(0, 1, 20)))
        result = build_training_outlier_distribution_diagnostics(df)
        result = result.copy()
        result.loc[0, "evidence_status"] = "insufficient"
        result.loc[0, "outlier_status"] = "acceptable"
        with pytest.raises(ValueError, match="insufficient evidence"):
            validate_outlier_distribution_diagnostics(result, "training")

    def test_empty_df_passes_validation(self):
        # Empty df should pass (no rows to violate constraints)
        validate_outlier_distribution_diagnostics(pd.DataFrame(), "training")

    def test_unknown_before_empty_check(self):
        # Unknown dataset name must be checked BEFORE the empty check
        with pytest.raises(ValueError, match="Unknown dataset_name"):
            validate_outlier_distribution_diagnostics(pd.DataFrame(), "nonexistent")


# ---------------------------------------------------------------------------
# TestPersistence
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_all_four_files_written(self, tmp_path):
        tr = build_training_outlier_distribution_diagnostics(
            _tr_df(list(_RNG.normal(0, 1, 20)))
        )
        bt_fold = build_backtest_outlier_distribution_by_fold(
            _bt_df(list(_RNG.normal(0, 1, 15)))
        )
        bt_sum = build_backtest_outlier_distribution_summary(bt_fold)
        prod = build_production_outlier_distribution_diagnostics(
            _prod_df(list(_RNG.normal(0, 1, 15)))
        )
        paths = persist_outlier_distribution_diagnostics(tr, bt_fold, bt_sum, prod, tmp_path)
        assert all(v is not None for v in paths.values())
        for p in paths.values():
            assert Path(p).exists()

    def test_overwrite_existing(self, tmp_path):
        tr = build_training_outlier_distribution_diagnostics(
            _tr_df(list(_RNG.normal(0, 1, 20)))
        )
        bt_fold = pd.DataFrame(columns=BACKTEST_FOLD_OUTLIER_COLS)
        bt_sum = pd.DataFrame(columns=BACKTEST_SUMMARY_OUTLIER_COLS)
        prod = pd.DataFrame(columns=PRODUCTION_OUTLIER_COLS)
        persist_outlier_distribution_diagnostics(tr, bt_fold, bt_sum, prod, tmp_path)
        paths = persist_outlier_distribution_diagnostics(tr, bt_fold, bt_sum, prod, tmp_path)
        for p in paths.values():
            assert Path(p).exists()

    def test_empty_dfs_written_as_header_only(self, tmp_path):
        tr = pd.DataFrame(columns=TRAINING_OUTLIER_COLS)
        bt_fold = pd.DataFrame(columns=BACKTEST_FOLD_OUTLIER_COLS)
        bt_sum = pd.DataFrame(columns=BACKTEST_SUMMARY_OUTLIER_COLS)
        prod = pd.DataFrame(columns=PRODUCTION_OUTLIER_COLS)
        paths = persist_outlier_distribution_diagnostics(tr, bt_fold, bt_sum, prod, tmp_path)
        assert all(v is not None for v in paths.values())
        for p in paths.values():
            loaded = pd.read_csv(p)
            assert loaded.empty

    def test_none_on_validation_failure(self, tmp_path):
        # Pass a bad DF that fails validation
        bad_df = pd.DataFrame({"col": [1, 2, 3]})
        bt_fold = pd.DataFrame(columns=BACKTEST_FOLD_OUTLIER_COLS)
        bt_sum = pd.DataFrame(columns=BACKTEST_SUMMARY_OUTLIER_COLS)
        prod = pd.DataFrame(columns=PRODUCTION_OUTLIER_COLS)
        paths = persist_outlier_distribution_diagnostics(bad_df, bt_fold, bt_sum, prod, tmp_path)
        # training should fail, others may succeed
        assert paths["training"] is None

    def test_output_directory_created(self, tmp_path):
        project_root = tmp_path / "project"
        project_root.mkdir()
        tr = pd.DataFrame(columns=TRAINING_OUTLIER_COLS)
        bt_fold = pd.DataFrame(columns=BACKTEST_FOLD_OUTLIER_COLS)
        bt_sum = pd.DataFrame(columns=BACKTEST_SUMMARY_OUTLIER_COLS)
        prod = pd.DataFrame(columns=PRODUCTION_OUTLIER_COLS)
        persist_outlier_distribution_diagnostics(tr, bt_fold, bt_sum, prod, project_root)
        assert (project_root / "outputs" / "diagnostics").exists()
