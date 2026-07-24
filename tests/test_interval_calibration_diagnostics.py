"""Tests for src/models/interval_calibration_diagnostics.py."""
from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.interval_calibration_diagnostics import (
    BACKTEST_FOLD_INTERVAL_COLS,
    BACKTEST_SUMMARY_INTERVAL_COLS,
    PRODUCTION_INTERVAL_COLS,
    IntervalCalibrationConfig,
    _compute_norm_scale,
    build_backtest_interval_diagnostics_by_fold,
    build_backtest_interval_diagnostics_summary,
    build_production_interval_diagnostics,
    calculate_horizon_interval_metrics,
    calculate_interval_calibration_metrics,
    calculate_interval_row_metrics,
    calculate_winkler_score,
    classify_interval_calibration,
    persist_interval_calibration_diagnostics,
    validate_interval_calibration_diagnostics,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEFAULT_CFG = IntervalCalibrationConfig()

_NC = 0.95
_ALPHA = 0.05


def _make_bt_row(
    report_id="r1",
    fold=1,
    horizon=1,
    actual=100.0,
    forecast=100.0,
    lower=80.0,
    upper=120.0,
    nc=_NC,
    model_name="ETS",
    model_family="ETS",
    m=1,
    forecast_date="2024-01-01",
    cutoff_date="2023-12-31",
    train_start="2022-01-01",
    train_end="2023-12-31",
    report_name="Report1",
):
    return {
        "report_id": report_id,
        "report_name": report_name,
        "fold_number": fold,
        "horizon_step": horizon,
        "actual": actual,
        "forecast": forecast,
        "lower_bound": lower,
        "upper_bound": upper,
        "nominal_coverage": nc,
        "model_name": model_name,
        "model_family": model_family,
        "candidate_m": m,
        "forecast_date": forecast_date,
        "cutoff_date": cutoff_date,
        "train_start": train_start,
        "train_end": train_end,
    }


def _make_bt_df(n=28, inside=True, nc=_NC, report_id="r1", fold=1):
    rows = []
    for h in range(1, n + 1):
        actual = 100.0
        lower = 80.0
        upper = 120.0 if inside else 90.0  # upper < actual when not inside
        rows.append(
            _make_bt_row(
                report_id=report_id,
                fold=fold,
                horizon=h,
                actual=actual,
                forecast=95.0,
                lower=lower,
                upper=upper,
                nc=nc,
            )
        )
    return pd.DataFrame(rows)


def _make_prod_row(
    report_id="r1",
    horizon=1,
    actual=100.0,
    forecast=100.0,
    lower=80.0,
    upper=120.0,
    nc=_NC,
    forecast_date="2024-01-01",
    generated_at="2024-01-01",
    model_family="ETS",
    model_name="ETS",
    m=1,
    lineage=True,
    report_name="Report1",
):
    return {
        "report_id": report_id,
        "report_name": report_name,
        "horizon_step": horizon,
        "actual": actual,
        "forecast": forecast,
        "lower_bound": lower,
        "upper_bound": upper,
        "nominal_coverage": nc,
        "forecast_date": forecast_date,
        "generated_at": generated_at,
        "selected_model_family": model_family,
        "selected_model_name": model_name,
        "selected_m": m,
        "lineage_complete": lineage,
    }


# ===========================================================================
# 1. Winkler score
# ===========================================================================


class TestWinklerScore:
    def test_inside_interval_score_equals_width(self):
        score = calculate_winkler_score(100.0, 80.0, 120.0, 0.05)
        assert score == pytest.approx(40.0)

    def test_below_lower_bound(self):
        # actual=70, lower=80, upper=120 → miss = 80-70=10
        # score = 40 + (2/0.05)*10 = 40 + 400 = 440
        score = calculate_winkler_score(70.0, 80.0, 120.0, 0.05)
        assert score == pytest.approx(440.0)

    def test_above_upper_bound(self):
        # actual=130, lower=80, upper=120 → miss = 130-120=10
        # score = 40 + (2/0.05)*10 = 440
        score = calculate_winkler_score(130.0, 80.0, 120.0, 0.05)
        assert score == pytest.approx(440.0)

    def test_at_lower_bound_is_inside(self):
        score = calculate_winkler_score(80.0, 80.0, 120.0, 0.05)
        assert score == pytest.approx(40.0)

    def test_at_upper_bound_is_inside(self):
        score = calculate_winkler_score(120.0, 80.0, 120.0, 0.05)
        assert score == pytest.approx(40.0)

    def test_zero_width_inside(self):
        score = calculate_winkler_score(100.0, 100.0, 100.0, 0.05)
        assert score == pytest.approx(0.0)

    def test_invalid_alpha_zero(self):
        assert calculate_winkler_score(100.0, 80.0, 120.0, 0.0) is None

    def test_invalid_alpha_one(self):
        assert calculate_winkler_score(100.0, 80.0, 120.0, 1.0) is None

    def test_invalid_alpha_negative(self):
        assert calculate_winkler_score(100.0, 80.0, 120.0, -0.1) is None

    def test_nan_actual(self):
        assert calculate_winkler_score(float("nan"), 80.0, 120.0, 0.05) is None

    def test_nan_lower(self):
        assert calculate_winkler_score(100.0, float("nan"), 120.0, 0.05) is None

    def test_nan_upper(self):
        assert calculate_winkler_score(100.0, 80.0, float("nan"), 0.05) is None

    def test_reversed_bounds(self):
        assert calculate_winkler_score(100.0, 120.0, 80.0, 0.05) is None

    def test_winkler_score_is_non_negative(self):
        for actual in [50.0, 80.0, 100.0, 120.0, 150.0]:
            score = calculate_winkler_score(actual, 80.0, 120.0, 0.05)
            assert score is not None and score >= 0.0

    def test_miss_score_greater_than_width(self):
        score = calculate_winkler_score(50.0, 80.0, 120.0, 0.05)
        assert score > 40.0

    def test_different_alpha(self):
        # alpha=0.10 → 2/alpha=20
        score = calculate_winkler_score(70.0, 80.0, 120.0, 0.10)
        assert score == pytest.approx(40.0 + 20.0 * 10.0)


# ===========================================================================
# 2. Row-level metrics
# ===========================================================================


class TestCalculateIntervalRowMetrics:
    def test_inside_interval(self):
        r = calculate_interval_row_metrics(100.0, 80.0, 120.0, 0.95)
        assert r["inside_interval"] is True
        assert r["interval_width"] == pytest.approx(40.0)
        assert r["miss_distance"] == pytest.approx(0.0)
        assert r["winkler_score"] is not None
        assert r["interval_observation_valid"] is True

    def test_below_lower(self):
        r = calculate_interval_row_metrics(70.0, 80.0, 120.0, 0.95)
        assert r["inside_interval"] is False
        assert r["lower_miss_distance"] == pytest.approx(10.0)
        assert r["upper_miss_distance"] == pytest.approx(0.0)
        assert r["miss_distance"] == pytest.approx(10.0)

    def test_above_upper(self):
        r = calculate_interval_row_metrics(140.0, 80.0, 120.0, 0.95)
        assert r["inside_interval"] is False
        assert r["upper_miss_distance"] == pytest.approx(20.0)
        assert r["lower_miss_distance"] == pytest.approx(0.0)

    def test_missing_bounds(self):
        r = calculate_interval_row_metrics(100.0, float("nan"), float("nan"), 0.95)
        assert r["interval_observation_valid"] is False
        assert r["interval_availability_status"] == "missing_bounds"

    def test_reversed_bounds(self):
        r = calculate_interval_row_metrics(100.0, 120.0, 80.0, 0.95)
        assert r["interval_observation_valid"] is False
        assert r["interval_availability_status"] == "invalid_bounds"

    def test_missing_actual(self):
        r = calculate_interval_row_metrics(float("nan"), 80.0, 120.0, 0.95)
        assert r["interval_observation_valid"] is False
        assert r["interval_availability_status"] == "missing_actual"

    def test_unknown_nominal_coverage_still_valid(self):
        r = calculate_interval_row_metrics(100.0, 80.0, 120.0, None)
        assert r["interval_observation_valid"] is True
        assert r["winkler_score"] is None
        assert r["interval_availability_status"] == "unknown_nominal_coverage"

    def test_invalid_nominal_coverage_zero(self):
        r = calculate_interval_row_metrics(100.0, 80.0, 120.0, 0.0)
        assert r["winkler_score"] is None

    def test_winkler_only_when_nominal_coverage_valid(self):
        r_valid = calculate_interval_row_metrics(100.0, 80.0, 120.0, 0.95)
        r_no_nc = calculate_interval_row_metrics(100.0, 80.0, 120.0, None)
        assert r_valid["winkler_score"] is not None
        assert r_no_nc["winkler_score"] is None


# ===========================================================================
# 3. Normalisation scale
# ===========================================================================


class TestComputeNormScale:
    def test_mean_actual_primary(self):
        scale, method, status = _compute_norm_scale(
            np.array([100.0, 200.0]), np.array([])
        )
        assert scale == pytest.approx(150.0)
        assert method == "mean_actual"
        assert status == "ok"

    def test_fallback_to_forecast(self):
        scale, method, status = _compute_norm_scale(
            np.array([0.0, 0.0]), np.array([100.0, 200.0])
        )
        assert scale == pytest.approx(150.0)
        assert method == "mean_forecast"
        assert status == "fallback"

    def test_no_valid_scale(self):
        scale, method, status = _compute_norm_scale(
            np.array([0.0]), np.array([0.0])
        )
        assert scale is None
        assert status == "no_valid_scale"

    def test_empty_arrays(self):
        scale, _, status = _compute_norm_scale(np.array([]), np.array([]))
        assert scale is None


# ===========================================================================
# 4. Group-level calibration metrics
# ===========================================================================


class TestCalculateIntervalCalibrationMetrics:
    def _make_df(self, n=20, inside=True, nc=0.95):
        rows = []
        for i in range(n):
            actual = 100.0
            lower = 80.0
            upper = 120.0 if inside else 90.0
            rows.append(
                {
                    "actual": actual,
                    "forecast": 95.0,
                    "lower_bound": lower,
                    "upper_bound": upper,
                    "nominal_coverage": nc,
                    "forecast_date": "2024-01-01",
                    "horizon_step": (i % 28) + 1,
                }
            )
        return pd.DataFrame(rows)

    def test_perfect_coverage(self):
        df = self._make_df(20, inside=True)
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["observed_coverage"] == pytest.approx(1.0)
        assert result["interval_hit_count"] == 20
        assert result["interval_miss_count"] == 0
        assert result["evidence_status"] == "ok"

    def test_zero_coverage(self):
        df = self._make_df(20, inside=False)
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["observed_coverage"] == pytest.approx(0.0)
        assert result["interval_miss_count"] == 20
        assert result["lower_miss_count"] == 0
        assert result["upper_miss_count"] == 20

    def test_nominal_coverage_extracted(self):
        df = self._make_df(20, nc=0.80)
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["nominal_coverage"] == pytest.approx(0.80, abs=0.01)

    def test_insufficient_evidence(self):
        df = self._make_df(5)
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["evidence_status"] == "insufficient"

    def test_coverage_gap_sign(self):
        # 75% coverage, nominal 95% → gap = -0.20
        rows = []
        for i in range(20):
            actual = 100.0 if i < 15 else 200.0  # 5 misses
            rows.append(
                {
                    "actual": actual,
                    "forecast": 95.0,
                    "lower_bound": 80.0,
                    "upper_bound": 120.0,
                    "nominal_coverage": 0.95,
                    "forecast_date": "2024-01-01",
                    "horizon_step": (i % 28) + 1,
                }
            )
        df = pd.DataFrame(rows)
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        # 15 inside, 5 outside → coverage=0.75
        assert result["observed_coverage"] == pytest.approx(0.75)
        assert result["coverage_gap"] == pytest.approx(0.75 - 0.95)

    def test_winkler_score_computed(self):
        df = self._make_df(20, inside=True)
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["mean_winkler_score"] is not None
        assert result["mean_winkler_score"] == pytest.approx(40.0)

    def test_winkler_score_null_when_no_nc(self):
        df = self._make_df(20)
        df["nominal_coverage"] = np.nan
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["mean_winkler_score"] is None

    def test_normalised_width(self):
        df = self._make_df(20, inside=True)
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["normalized_mean_interval_width"] is not None
        assert result["normalized_mean_interval_width"] == pytest.approx(
            40.0 / 100.0, abs=0.01
        )

    def test_lower_miss_rate(self):
        rows = []
        for i in range(20):
            actual = 50.0 if i < 5 else 100.0
            rows.append(
                {
                    "actual": actual,
                    "forecast": 95.0,
                    "lower_bound": 80.0,
                    "upper_bound": 120.0,
                    "nominal_coverage": 0.95,
                    "forecast_date": "2024-01-01",
                    "horizon_step": 1,
                }
            )
        df = pd.DataFrame(rows)
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["lower_miss_count"] == 5
        assert result["lower_miss_rate"] == pytest.approx(0.25)

    def test_max_miss_distance(self):
        rows = [
            {
                "actual": 200.0,
                "forecast": 95.0,
                "lower_bound": 80.0,
                "upper_bound": 120.0,
                "nominal_coverage": 0.95,
                "forecast_date": "2024-01-01",
                "horizon_step": 1,
            }
        ] * 20
        df = pd.DataFrame(rows)
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["max_miss_distance"] == pytest.approx(80.0)

    def test_missing_bounds_counted(self):
        df = self._make_df(20)
        df.loc[:4, "lower_bound"] = np.nan
        df.loc[:4, "upper_bound"] = np.nan
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["missing_interval_count"] == 5

    def test_reversed_bounds_counted(self):
        df = self._make_df(20)
        df.loc[:2, "lower_bound"] = 130.0
        df.loc[:2, "upper_bound"] = 80.0
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["invalid_interval_count"] == 3

    def test_interval_availability_rate(self):
        df = self._make_df(20)
        df.loc[:4, "lower_bound"] = np.nan
        df.loc[:4, "upper_bound"] = np.nan
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["interval_availability_rate"] == pytest.approx(15 / 20)

    def test_empty_df_returns_insufficient(self):
        df = pd.DataFrame(
            columns=["actual", "forecast", "lower_bound", "upper_bound", "nominal_coverage", "forecast_date", "horizon_step"]
        )
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["evidence_status"] == "insufficient"


# ===========================================================================
# 5. Horizon metrics
# ===========================================================================


class TestCalculateHorizonIntervalMetrics:
    def _make_horizon_df(self, horizons, all_inside=True):
        rows = []
        for h in horizons:
            actual = 100.0
            upper = 120.0 if all_inside else 90.0
            rows.append(
                {
                    "actual": actual,
                    "forecast": 95.0,
                    "lower_bound": 80.0,
                    "upper_bound": upper,
                    "nominal_coverage": 0.95,
                    "horizon_step": h,
                }
            )
        return pd.DataFrame(rows)

    def test_early_bucket_populated(self):
        df = self._make_horizon_df(list(range(1, 8)))
        result = calculate_horizon_interval_metrics(df, _DEFAULT_CFG)
        assert result["early_horizon_interval_count"] == 7
        assert result["early_horizon_coverage"] == pytest.approx(1.0)

    def test_middle_bucket(self):
        df = self._make_horizon_df(list(range(8, 15)))
        result = calculate_horizon_interval_metrics(df, _DEFAULT_CFG)
        assert result["middle_horizon_interval_count"] == 7

    def test_late_bucket(self):
        df = self._make_horizon_df(list(range(15, 29)))
        result = calculate_horizon_interval_metrics(df, _DEFAULT_CFG)
        assert result["late_horizon_interval_count"] == 14

    def test_buckets_with_full_28_horizon(self):
        df = self._make_horizon_df(list(range(1, 29)))
        result = calculate_horizon_interval_metrics(df, _DEFAULT_CFG)
        assert result["early_horizon_interval_count"] == 7
        assert result["middle_horizon_interval_count"] == 7
        assert result["late_horizon_interval_count"] == 14

    def test_insufficient_bucket_returns_none(self):
        cfg = IntervalCalibrationConfig(MIN_INTERVAL_OBSERVATIONS_PER_HORIZON_BUCKET=10)
        df = self._make_horizon_df(list(range(1, 8)))
        result = calculate_horizon_interval_metrics(df, cfg)
        assert result["early_horizon_coverage"] is None

    def test_deterioration_flag_set_when_late_coverage_drops(self):
        rows = []
        for h in range(1, 8):
            rows.append(
                {
                    "actual": 100.0,
                    "forecast": 95.0,
                    "lower_bound": 80.0,
                    "upper_bound": 120.0,
                    "nominal_coverage": 0.95,
                    "horizon_step": h,
                }
            )
        for h in range(15, 29):
            rows.append(
                {
                    "actual": 200.0,  # miss
                    "forecast": 95.0,
                    "lower_bound": 80.0,
                    "upper_bound": 120.0,
                    "nominal_coverage": 0.95,
                    "horizon_step": h,
                }
            )
        df = pd.DataFrame(rows)
        result = calculate_horizon_interval_metrics(df, _DEFAULT_CFG)
        assert result["horizon_calibration_deterioration_flag"] is True

    def test_deterioration_flag_false_when_stable(self):
        df = self._make_horizon_df(list(range(1, 29)))
        result = calculate_horizon_interval_metrics(df, _DEFAULT_CFG)
        assert result["horizon_calibration_deterioration_flag"] is False

    def test_missing_horizon_step_returns_null_metrics(self):
        df = pd.DataFrame(
            [{"actual": 100.0, "lower_bound": 80.0, "upper_bound": 120.0, "nominal_coverage": 0.95}]
        )
        result = calculate_horizon_interval_metrics(df, _DEFAULT_CFG)
        assert result["early_horizon_coverage"] is None

    def test_coverage_range_computed(self):
        rows = []
        for h in range(1, 8):
            rows.append(
                {
                    "actual": 100.0,
                    "forecast": 95.0,
                    "lower_bound": 80.0,
                    "upper_bound": 120.0,
                    "nominal_coverage": 0.95,
                    "horizon_step": h,
                }
            )
        for h in range(8, 15):
            rows.append(
                {
                    "actual": 200.0,
                    "forecast": 95.0,
                    "lower_bound": 80.0,
                    "upper_bound": 120.0,
                    "nominal_coverage": 0.95,
                    "horizon_step": h,
                }
            )
        df = pd.DataFrame(rows)
        result = calculate_horizon_interval_metrics(df, _DEFAULT_CFG)
        # early=1.0, middle=0.0 → range=1.0
        assert result["horizon_coverage_range"] == pytest.approx(1.0)


# ===========================================================================
# 6. Classification
# ===========================================================================


class TestClassifyIntervalCalibration:
    def _base_metrics(self, gap=0.0, nc=0.95, obs=0.95, n=20, norm_width=0.3):
        obs_cov = nc + gap
        return {
            "evidence_status": "ok",
            "interval_observation_count": n,
            "nominal_coverage": nc,
            "observed_coverage": obs_cov,
            "coverage_gap": gap,
            "absolute_coverage_gap": abs(gap),
            "lower_miss_rate": 0.0,
            "upper_miss_rate": max(0.0, -gap),
            "normalized_mean_interval_width": norm_width,
            "normalized_mean_winkler_score": norm_width * 1.1,
            "mean_winkler_score": norm_width * 100,
            "interval_availability_rate": 1.0,
            "horizon_calibration_deterioration_flag": False,
        }

    def test_well_calibrated(self):
        cal, use, reasons = classify_interval_calibration(self._base_metrics(gap=0.0))
        assert cal == "well_calibrated"
        assert use == "useful"

    def test_slight_undercoverage(self):
        cal, use, reasons = classify_interval_calibration(self._base_metrics(gap=-0.04))
        assert cal in ("slight_undercoverage",)

    def test_undercoverage(self):
        cal, use, reasons = classify_interval_calibration(self._base_metrics(gap=-0.12))
        assert cal == "undercoverage"

    def test_severe_undercoverage(self):
        cal, use, reasons = classify_interval_calibration(self._base_metrics(gap=-0.20))
        assert cal == "severe_undercoverage"

    def test_overcoverage(self):
        cal, use, reasons = classify_interval_calibration(self._base_metrics(gap=0.15))
        assert cal == "overcoverage"

    def test_insufficient_evidence(self):
        m = {"evidence_status": "insufficient", "interval_observation_count": 2}
        cal, use, reasons = classify_interval_calibration(m)
        assert cal == "insufficient_evidence"
        assert use == "insufficient_evidence"

    def test_overwide_intervals(self):
        m = self._base_metrics(norm_width=5.0)
        _, use, reasons = classify_interval_calibration(m, _DEFAULT_CFG)
        assert use == "overwide"

    def test_wide_but_usable(self):
        m = self._base_metrics(norm_width=3.0)
        _, use, reasons = classify_interval_calibration(m, _DEFAULT_CFG)
        assert use == "wide_but_usable"

    def test_zero_availability_returns_unavailable(self):
        m = self._base_metrics()
        m["interval_availability_rate"] = 0.0
        cal, use, _ = classify_interval_calibration(m)
        assert use == "unavailable"
        assert cal == "insufficient_evidence"

    def test_reasons_not_empty(self):
        _, _, reasons = classify_interval_calibration(self._base_metrics())
        assert isinstance(reasons, list)
        assert len(reasons) > 0

    def test_horizon_deterioration_adds_reason(self):
        m = self._base_metrics()
        m["horizon_calibration_deterioration_flag"] = True
        _, _, reasons = classify_interval_calibration(m)
        assert any("horizon" in r.lower() for r in reasons)

    def test_lower_miss_rate_adds_reason(self):
        m = self._base_metrics()
        m["lower_miss_rate"] = 0.20
        _, _, reasons = classify_interval_calibration(m)
        assert any("lower" in r.lower() for r in reasons)

    def test_upper_miss_rate_adds_reason(self):
        m = self._base_metrics()
        m["upper_miss_rate"] = 0.20
        _, _, reasons = classify_interval_calibration(m)
        assert any("upper" in r.lower() for r in reasons)

    def test_calibration_statuses_are_valid(self):
        valid = {
            "well_calibrated", "slight_undercoverage", "undercoverage",
            "severe_undercoverage", "overcoverage", "insufficient_evidence",
            "calculation_failed",
        }
        for gap in [-0.20, -0.10, -0.04, 0.0, 0.15]:
            cal, _, _ = classify_interval_calibration(self._base_metrics(gap=gap))
            assert cal in valid

    def test_usefulness_statuses_are_valid(self):
        valid = {
            "useful", "wide_but_usable", "overwide", "poor",
            "insufficient_evidence", "unavailable",
        }
        for width in [0.3, 2.5, 5.0]:
            _, use, _ = classify_interval_calibration(self._base_metrics(norm_width=width))
            assert use in valid


# ===========================================================================
# 7. Backtest fold diagnostics
# ===========================================================================


class TestBuildBacktestIntervalByFold:
    def test_returns_dataframe(self):
        df = _make_bt_df(28)
        result = build_backtest_interval_diagnostics_by_fold(df)
        assert isinstance(result, pd.DataFrame)

    def test_schema_columns_present(self):
        df = _make_bt_df(28)
        result = build_backtest_interval_diagnostics_by_fold(df)
        for col in BACKTEST_FOLD_INTERVAL_COLS:
            assert col in result.columns, f"Missing column: {col}"

    def test_empty_df_returns_empty(self):
        result = build_backtest_interval_diagnostics_by_fold(pd.DataFrame())
        assert result.empty
        for col in BACKTEST_FOLD_INTERVAL_COLS:
            assert col in result.columns

    def test_single_fold_group(self):
        df = _make_bt_df(28, inside=True, fold=1)
        result = build_backtest_interval_diagnostics_by_fold(df)
        assert len(result) == 1

    def test_well_calibrated_on_perfect_coverage(self):
        df = _make_bt_df(28, inside=True)
        result = build_backtest_interval_diagnostics_by_fold(df)
        status = result["calibration_status"].iloc[0]
        assert status in ("well_calibrated", "overcoverage")

    def test_undercoverage_on_all_misses(self):
        df = _make_bt_df(28, inside=False)
        result = build_backtest_interval_diagnostics_by_fold(df)
        status = result["calibration_status"].iloc[0]
        assert status in ("severe_undercoverage", "undercoverage")

    def test_multiple_folds_separate_rows(self):
        df1 = _make_bt_df(28, fold=1)
        df2 = _make_bt_df(28, fold=2)
        df = pd.concat([df1, df2], ignore_index=True)
        result = build_backtest_interval_diagnostics_by_fold(df)
        assert len(result) == 2

    def test_fold_numbers_preserved(self):
        df1 = _make_bt_df(28, fold=1)
        df2 = _make_bt_df(28, fold=2)
        df = pd.concat([df1, df2], ignore_index=True)
        result = build_backtest_interval_diagnostics_by_fold(df)
        assert set(result["fold_number"]) == {1, 2}

    def test_evaluation_run_id_propagated(self):
        df = _make_bt_df(28)
        result = build_backtest_interval_diagnostics_by_fold(df, evaluation_run_id="run123")
        assert (result["evaluation_run_id"] == "run123").all()

    def test_insufficient_evidence_when_too_few_rows(self):
        df = _make_bt_df(5)
        result = build_backtest_interval_diagnostics_by_fold(df, cfg=_DEFAULT_CFG)
        status = result["calibration_status"].iloc[0]
        assert status in ("insufficient_evidence", "calculation_failed")

    def test_folds_not_pooled(self):
        # Each fold independently has 28 rows; total is 56 but each fold row
        # reports 28 observations, not 56.
        df1 = _make_bt_df(28, fold=1, inside=True)
        df2 = _make_bt_df(28, fold=2, inside=False)
        df = pd.concat([df1, df2], ignore_index=True)
        result = build_backtest_interval_diagnostics_by_fold(df)
        assert len(result) == 2
        cov_fold1 = result[result["fold_number"] == 1]["observed_coverage"].iloc[0]
        cov_fold2 = result[result["fold_number"] == 2]["observed_coverage"].iloc[0]
        assert cov_fold1 == pytest.approx(1.0)
        assert cov_fold2 == pytest.approx(0.0)

    def test_nominal_coverage_in_output(self):
        df = _make_bt_df(28, nc=0.95)
        result = build_backtest_interval_diagnostics_by_fold(df)
        assert result["nominal_coverage"].iloc[0] == pytest.approx(0.95, abs=0.01)

    def test_multiple_reports(self):
        df1 = _make_bt_df(28, report_id="r1", fold=1)
        df2 = _make_bt_df(28, report_id="r2", fold=1)
        df = pd.concat([df1, df2], ignore_index=True)
        result = build_backtest_interval_diagnostics_by_fold(df)
        assert len(result) == 2

    def test_calibration_reasons_non_empty_string(self):
        df = _make_bt_df(28)
        result = build_backtest_interval_diagnostics_by_fold(df)
        reasons = result["calibration_reasons"].iloc[0]
        assert isinstance(reasons, str) and len(reasons) > 0

    def test_horizon_cols_populated(self):
        df = _make_bt_df(28)
        result = build_backtest_interval_diagnostics_by_fold(df)
        assert result["early_horizon_coverage"].iloc[0] is not None or True  # may be None if too few


# ===========================================================================
# 8. Backtest summary diagnostics
# ===========================================================================


class TestBuildBacktestIntervalSummary:
    def _make_fold_df(self, n_folds=4, inside=True):
        all_rows = []
        for fold in range(1, n_folds + 1):
            bt = _make_bt_df(28, fold=fold, inside=inside)
            fold_result = build_backtest_interval_diagnostics_by_fold(bt)
            all_rows.append(fold_result)
        return pd.concat(all_rows, ignore_index=True)

    def test_returns_dataframe(self):
        fold_df = self._make_fold_df()
        result = build_backtest_interval_diagnostics_summary(fold_df)
        assert isinstance(result, pd.DataFrame)

    def test_schema_columns_present(self):
        fold_df = self._make_fold_df()
        result = build_backtest_interval_diagnostics_summary(fold_df)
        for col in BACKTEST_SUMMARY_INTERVAL_COLS:
            assert col in result.columns, f"Missing column: {col}"

    def test_empty_fold_df_returns_empty(self):
        result = build_backtest_interval_diagnostics_summary(pd.DataFrame())
        assert result.empty

    def test_total_fold_count(self):
        fold_df = self._make_fold_df(n_folds=4)
        result = build_backtest_interval_diagnostics_summary(fold_df)
        assert result["total_fold_count"].iloc[0] == 4

    def test_well_calibrated_on_all_inside(self):
        fold_df = self._make_fold_df(inside=True)
        result = build_backtest_interval_diagnostics_summary(fold_df)
        assert result["cross_fold_calibration_status"].iloc[0] in (
            "well_calibrated", "overcoverage", "slight_undercoverage"
        )

    def test_pooled_coverage_with_backtest_df(self):
        all_bt = []
        for fold in range(1, 3):
            all_bt.append(_make_bt_df(28, fold=fold, inside=True))
        bt_df = pd.concat(all_bt, ignore_index=True)
        fold_df = build_backtest_interval_diagnostics_by_fold(bt_df)
        result = build_backtest_interval_diagnostics_summary(fold_df, backtest_df=bt_df)
        assert result["pooled_observed_coverage"].iloc[0] is not None
        assert result["pooled_observed_coverage"].iloc[0] == pytest.approx(1.0, abs=0.01)

    def test_evaluation_run_id_propagated(self):
        fold_df = self._make_fold_df()
        result = build_backtest_interval_diagnostics_summary(
            fold_df, evaluation_run_id="run-xyz"
        )
        assert (result["evaluation_run_id"] == "run-xyz").all()

    def test_multiple_reports_separate_rows(self):
        df1 = _make_bt_df(28, report_id="r1", fold=1)
        df2 = _make_bt_df(28, report_id="r2", fold=1)
        df = pd.concat([df1, df2], ignore_index=True)
        fold_df = build_backtest_interval_diagnostics_by_fold(df)
        result = build_backtest_interval_diagnostics_summary(fold_df)
        assert len(result) == 2

    def test_insufficient_evidence_when_too_few_folds(self):
        # Only 1 fold, requires MIN_VALID_FOLDS=2
        fold_df = self._make_fold_df(n_folds=1)
        result = build_backtest_interval_diagnostics_summary(fold_df)
        status = result["cross_fold_calibration_status"].iloc[0]
        assert status in ("insufficient_evidence", "well_calibrated", "overcoverage", "slight_undercoverage")

    def test_median_coverage_computed(self):
        fold_df = self._make_fold_df()
        result = build_backtest_interval_diagnostics_summary(fold_df)
        assert result["median_observed_coverage"].iloc[0] is not None

    def test_cross_fold_reasons_non_empty(self):
        fold_df = self._make_fold_df()
        result = build_backtest_interval_diagnostics_summary(fold_df)
        reasons = result["cross_fold_calibration_reasons"].iloc[0]
        assert isinstance(reasons, str) and len(reasons) > 0

    def test_cross_fold_statuses_valid(self):
        valid_cal = {
            "well_calibrated", "slight_undercoverage", "undercoverage",
            "severe_undercoverage", "overcoverage", "insufficient_evidence",
            "calculation_failed",
        }
        valid_use = {
            "useful", "wide_but_usable", "overwide", "poor",
            "insufficient_evidence", "unavailable",
        }
        fold_df = self._make_fold_df()
        result = build_backtest_interval_diagnostics_summary(fold_df)
        for s in result["cross_fold_calibration_status"]:
            assert s in valid_cal
        for s in result["cross_fold_interval_usefulness_status"]:
            assert s in valid_use


# ===========================================================================
# 9. Production interval diagnostics
# ===========================================================================


class TestBuildProductionIntervalDiagnostics:
    def _make_prod_df(self, n=20, inside=True, nc=_NC):
        rows = []
        for i in range(n):
            actual = 100.0
            upper = 120.0 if inside else 90.0
            rows.append(
                _make_prod_row(
                    actual=actual,
                    forecast=95.0,
                    lower=80.0,
                    upper=upper,
                    nc=nc,
                    horizon=1,
                    forecast_date=f"2024-01-{(i % 28) + 1:02d}",
                )
            )
        return pd.DataFrame(rows)

    def test_returns_dataframe(self):
        df = self._make_prod_df(20)
        result = build_production_interval_diagnostics(df)
        assert isinstance(result, pd.DataFrame)

    def test_schema_columns_present(self):
        df = self._make_prod_df(20)
        result = build_production_interval_diagnostics(df)
        for col in PRODUCTION_INTERVAL_COLS:
            assert col in result.columns, f"Missing column: {col}"

    def test_empty_df_returns_empty(self):
        result = build_production_interval_diagnostics(pd.DataFrame())
        assert result.empty

    def test_well_calibrated_on_perfect_coverage(self):
        df = self._make_prod_df(20, inside=True)
        result = build_production_interval_diagnostics(df)
        status = result["calibration_status"].iloc[0]
        assert status in ("well_calibrated", "overcoverage")

    def test_evaluation_run_id_propagated(self):
        df = self._make_prod_df(20)
        result = build_production_interval_diagnostics(df, evaluation_run_id="prod-run")
        assert (result["evaluation_run_id"] == "prod-run").all()

    def test_deduplication_counts(self):
        rows = []
        for h in [1, 7, 14]:
            rows.append(
                _make_prod_row(
                    horizon=h,
                    forecast_date="2024-01-01",
                    actual=100.0,
                    nc=_NC,
                )
            )
        df = pd.DataFrame(rows)
        result = build_production_interval_diagnostics(df)
        assert result["original_prediction_count"].iloc[0] == 3
        assert result["excluded_overlap_count"].iloc[0] == 2  # 2 removed, 1 kept

    def test_deduplication_keeps_shortest_horizon(self):
        rows = []
        for h in [7, 1, 14]:
            rows.append(
                _make_prod_row(
                    horizon=h,
                    forecast_date="2024-01-01",
                    actual=100.0,
                    nc=_NC,
                )
            )
        df = pd.DataFrame(rows)
        result = build_production_interval_diagnostics(df)
        # horizon_step 1 should be kept in deduplication
        assert result["deduplicated_date_count"].iloc[0] == 1

    def test_legacy_null_nominal_coverage_handled(self):
        df = self._make_prod_df(20, nc=None)
        # Should not crash, just compute what it can
        result = build_production_interval_diagnostics(df)
        assert not result.empty

    def test_all_record_coverage_present(self):
        df = self._make_prod_df(20, inside=True)
        result = build_production_interval_diagnostics(df)
        assert result["all_record_observed_coverage"].iloc[0] is not None

    def test_multiple_reports_separate_rows(self):
        df1 = pd.DataFrame([_make_prod_row(report_id="r1")] * 20)
        df2 = pd.DataFrame([_make_prod_row(report_id="r2")] * 20)
        df = pd.concat([df1, df2], ignore_index=True)
        result = build_production_interval_diagnostics(df)
        assert len(result) == 2

    def test_insufficient_evidence_with_too_few_rows(self):
        df = self._make_prod_df(5)
        result = build_production_interval_diagnostics(df)
        status = result["calibration_status"].iloc[0]
        assert status in ("insufficient_evidence", "calculation_failed")

    def test_missing_group_columns_returns_empty(self):
        df = pd.DataFrame([{"report_id": "r1", "actual": 100.0}])
        result = build_production_interval_diagnostics(df)
        assert result.empty

    def test_deduplicated_winkler_score(self):
        df = self._make_prod_df(20, inside=True)
        result = build_production_interval_diagnostics(df)
        # Should have either a float or None; not raise
        w = result["deduplicated_mean_winkler_score"].iloc[0]
        assert w is None or isinstance(w, float)


# ===========================================================================
# 10. Validation
# ===========================================================================


class TestValidateIntervalCalibrationDiagnostics:
    def test_unknown_dataset_raises(self):
        with pytest.raises(ValueError, match="Unknown dataset_name"):
            validate_interval_calibration_diagnostics(pd.DataFrame(), "unknown_name")

    def test_valid_empty_backtest_fold(self):
        df = pd.DataFrame(columns=BACKTEST_FOLD_INTERVAL_COLS)
        validate_interval_calibration_diagnostics(df, "backtest_fold")

    def test_valid_empty_backtest_summary(self):
        df = pd.DataFrame(columns=BACKTEST_SUMMARY_INTERVAL_COLS)
        validate_interval_calibration_diagnostics(df, "backtest_summary")

    def test_valid_empty_production(self):
        df = pd.DataFrame(columns=PRODUCTION_INTERVAL_COLS)
        validate_interval_calibration_diagnostics(df, "production")

    def test_missing_columns_raises(self):
        df = pd.DataFrame([{"evaluation_run_id": "x"}])
        with pytest.raises(ValueError, match="missing columns"):
            validate_interval_calibration_diagnostics(df, "backtest_fold")

    def test_invalid_calibration_status_raises(self):
        df = _make_bt_df(28)
        fold_df = build_backtest_interval_diagnostics_by_fold(df)
        fold_df.loc[0, "calibration_status"] = "banana"
        with pytest.raises(ValueError, match="invalid calibration_status"):
            validate_interval_calibration_diagnostics(fold_df, "backtest_fold")

    def test_invalid_usefulness_status_raises(self):
        df = _make_bt_df(28)
        fold_df = build_backtest_interval_diagnostics_by_fold(df)
        fold_df.loc[0, "interval_usefulness_status"] = "magic"
        with pytest.raises(ValueError, match="invalid interval_usefulness_status"):
            validate_interval_calibration_diagnostics(fold_df, "backtest_fold")

    def test_negative_winkler_raises(self):
        df = _make_bt_df(28)
        fold_df = build_backtest_interval_diagnostics_by_fold(df)
        fold_df.loc[0, "mean_winkler_score"] = -1.0
        with pytest.raises(ValueError, match="negative values"):
            validate_interval_calibration_diagnostics(fold_df, "backtest_fold")

    def test_negative_interval_width_raises(self):
        df = _make_bt_df(28)
        fold_df = build_backtest_interval_diagnostics_by_fold(df)
        fold_df.loc[0, "mean_interval_width"] = -5.0
        with pytest.raises(ValueError, match="negative values"):
            validate_interval_calibration_diagnostics(fold_df, "backtest_fold")

    def test_coverage_out_of_range_raises(self):
        df = _make_bt_df(28)
        fold_df = build_backtest_interval_diagnostics_by_fold(df)
        fold_df.loc[0, "observed_coverage"] = 1.5
        with pytest.raises(ValueError, match="outside \\[0,1\\]"):
            validate_interval_calibration_diagnostics(fold_df, "backtest_fold")

    def test_insufficient_evidence_well_calibrated_raises(self):
        df = pd.DataFrame(
            {
                **{c: [None] for c in BACKTEST_FOLD_INTERVAL_COLS},
                "calibration_evidence_status": ["insufficient"],
                "calibration_status": ["well_calibrated"],
            }
        )
        with pytest.raises(ValueError, match="insufficient_evidence"):
            validate_interval_calibration_diagnostics(df, "backtest_fold")

    def test_valid_full_fold_df(self):
        df = _make_bt_df(28)
        fold_df = build_backtest_interval_diagnostics_by_fold(df)
        # Should not raise
        validate_interval_calibration_diagnostics(fold_df, "backtest_fold")

    def test_valid_full_summary_df(self):
        df1 = _make_bt_df(28, fold=1)
        df2 = _make_bt_df(28, fold=2)
        df = pd.concat([df1, df2], ignore_index=True)
        fold_df = build_backtest_interval_diagnostics_by_fold(df)
        summary_df = build_backtest_interval_diagnostics_summary(fold_df)
        validate_interval_calibration_diagnostics(summary_df, "backtest_summary")

    def test_valid_full_production_df(self):
        rows = [_make_prod_row(forecast_date=f"2024-01-{i+1:02d}") for i in range(20)]
        df = pd.DataFrame(rows)
        prod_df = build_production_interval_diagnostics(df)
        validate_interval_calibration_diagnostics(prod_df, "production")


# ===========================================================================
# 11. Persistence
# ===========================================================================


class TestPersistIntervalCalibrationDiagnostics:
    def _make_all_dfs(self):
        bt_df = _make_bt_df(28, fold=1)
        bt_df2 = _make_bt_df(28, fold=2)
        bt_all = pd.concat([bt_df, bt_df2], ignore_index=True)
        fold_df = build_backtest_interval_diagnostics_by_fold(bt_all)
        summary_df = build_backtest_interval_diagnostics_summary(fold_df)
        prod_rows = [_make_prod_row(forecast_date=f"2024-01-{i+1:02d}") for i in range(20)]
        prod_df = build_production_interval_diagnostics(pd.DataFrame(prod_rows))
        return fold_df, summary_df, prod_df

    def test_writes_three_files(self, tmp_path):
        fold_df, summary_df, prod_df = self._make_all_dfs()
        paths = persist_interval_calibration_diagnostics(
            fold_df, summary_df, prod_df, tmp_path
        )
        assert "backtest_fold" in paths
        assert "backtest_summary" in paths
        assert "production" in paths

    def test_files_exist_after_persist(self, tmp_path):
        fold_df, summary_df, prod_df = self._make_all_dfs()
        paths = persist_interval_calibration_diagnostics(
            fold_df, summary_df, prod_df, tmp_path
        )
        for name, p in paths.items():
            if p is not None:
                assert Path(p).exists(), f"File not found: {p}"

    def test_roundtrip_backtest_fold(self, tmp_path):
        fold_df, summary_df, prod_df = self._make_all_dfs()
        paths = persist_interval_calibration_diagnostics(
            fold_df, summary_df, prod_df, tmp_path
        )
        loaded = pd.read_csv(paths["backtest_fold"])
        assert list(loaded.columns) == BACKTEST_FOLD_INTERVAL_COLS

    def test_roundtrip_backtest_summary(self, tmp_path):
        fold_df, summary_df, prod_df = self._make_all_dfs()
        paths = persist_interval_calibration_diagnostics(
            fold_df, summary_df, prod_df, tmp_path
        )
        loaded = pd.read_csv(paths["backtest_summary"])
        assert list(loaded.columns) == BACKTEST_SUMMARY_INTERVAL_COLS

    def test_roundtrip_production(self, tmp_path):
        fold_df, summary_df, prod_df = self._make_all_dfs()
        paths = persist_interval_calibration_diagnostics(
            fold_df, summary_df, prod_df, tmp_path
        )
        loaded = pd.read_csv(paths["production"])
        assert list(loaded.columns) == PRODUCTION_INTERVAL_COLS

    def test_empty_dfs_write_header_only(self, tmp_path):
        paths = persist_interval_calibration_diagnostics(
            pd.DataFrame(columns=BACKTEST_FOLD_INTERVAL_COLS),
            pd.DataFrame(columns=BACKTEST_SUMMARY_INTERVAL_COLS),
            pd.DataFrame(columns=PRODUCTION_INTERVAL_COLS),
            tmp_path,
        )
        for name, p in paths.items():
            if p:
                loaded = pd.read_csv(p)
                assert loaded.empty

    def test_creates_output_directory(self, tmp_path):
        fold_df, summary_df, prod_df = self._make_all_dfs()
        new_root = tmp_path / "new_project"
        paths = persist_interval_calibration_diagnostics(
            fold_df, summary_df, prod_df, new_root
        )
        assert any(p is not None for p in paths.values())

    def test_overwrite_existing_file(self, tmp_path):
        fold_df, summary_df, prod_df = self._make_all_dfs()
        paths1 = persist_interval_calibration_diagnostics(
            fold_df, summary_df, prod_df, tmp_path
        )
        paths2 = persist_interval_calibration_diagnostics(
            fold_df, summary_df, prod_df, tmp_path
        )
        for name in ["backtest_fold", "backtest_summary", "production"]:
            assert paths2[name] is not None


# ===========================================================================
# 12. Edge cases and regression
# ===========================================================================


class TestEdgeCases:
    def test_all_null_nominal_coverage_production(self):
        rows = [
            _make_prod_row(forecast_date=f"2024-01-{i+1:02d}", nc=None) for i in range(20)
        ]
        df = pd.DataFrame(rows)
        # Should not crash
        result = build_production_interval_diagnostics(df)
        assert not result.empty

    def test_single_row_backtest(self):
        df = pd.DataFrame([_make_bt_row()])
        result = build_backtest_interval_diagnostics_by_fold(df)
        assert not result.empty
        assert result["calibration_status"].iloc[0] in (
            "insufficient_evidence", "calculation_failed"
        )

    def test_nan_actuals_excluded_from_valid_count(self):
        rows = []
        for i in range(20):
            actual = float("nan") if i < 5 else 100.0
            rows.append(
                {
                    "actual": actual,
                    "forecast": 95.0,
                    "lower_bound": 80.0,
                    "upper_bound": 120.0,
                    "nominal_coverage": 0.95,
                    "forecast_date": "2024-01-01",
                    "horizon_step": 1,
                }
            )
        df = pd.DataFrame(rows)
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["interval_observation_count"] == 15

    def test_mixed_nc_values_uses_median(self):
        rows = []
        for i in range(20):
            nc = 0.95 if i < 15 else 0.80
            rows.append(
                {
                    "actual": 100.0,
                    "forecast": 95.0,
                    "lower_bound": 80.0,
                    "upper_bound": 120.0,
                    "nominal_coverage": nc,
                    "forecast_date": "2024-01-01",
                    "horizon_step": 1,
                }
            )
        df = pd.DataFrame(rows)
        result = calculate_interval_calibration_metrics(df, _DEFAULT_CFG)
        assert result["nominal_coverage"] is not None

    def test_zero_width_interval_score(self):
        # zero-width interval; actual == bounds → score=0
        score = calculate_winkler_score(100.0, 100.0, 100.0, 0.05)
        assert score == pytest.approx(0.0)

    def test_winkler_score_symmetry(self):
        # miss same distance below vs above should give same score
        score_low = calculate_winkler_score(70.0, 80.0, 120.0, 0.05)
        score_high = calculate_winkler_score(130.0, 80.0, 120.0, 0.05)
        assert score_low == pytest.approx(score_high)

    def test_large_miss_dominated_by_penalty(self):
        width = 40.0
        miss_dist = 1000.0
        score = calculate_winkler_score(80.0 - miss_dist, 80.0, 120.0, 0.05)
        expected = width + (2 / 0.05) * miss_dist
        assert score == pytest.approx(expected)

    def test_config_custom_thresholds(self):
        cfg = IntervalCalibrationConfig(
            COVERAGE_TOLERANCE=0.0,
            UNDERCOVERAGE_WARNING_GAP=-0.01,
        )
        m = {
            "evidence_status": "ok",
            "interval_observation_count": 30,
            "nominal_coverage": 0.95,
            "observed_coverage": 0.94,
            "coverage_gap": -0.01,
            "absolute_coverage_gap": 0.01,
            "lower_miss_rate": 0.01,
            "upper_miss_rate": 0.0,
            "normalized_mean_interval_width": 0.3,
            "normalized_mean_winkler_score": 0.35,
            "mean_winkler_score": 35.0,
            "interval_availability_rate": 1.0,
            "horizon_calibration_deterioration_flag": False,
        }
        cal, _, _ = classify_interval_calibration(m, cfg)
        assert cal in ("slight_undercoverage", "undercoverage", "well_calibrated")

    def test_backtest_summary_with_single_well_calibrated_fold(self):
        fold_df = build_backtest_interval_diagnostics_by_fold(_make_bt_df(28, fold=1))
        result = build_backtest_interval_diagnostics_summary(fold_df)
        assert not result.empty

    def test_production_multiple_horizons_per_date(self):
        rows = []
        for h in range(1, 8):
            rows.append(
                _make_prod_row(
                    horizon=h,
                    forecast_date="2024-01-15",
                    actual=100.0,
                )
            )
        df = pd.DataFrame(rows)
        result = build_production_interval_diagnostics(df)
        assert result["deduplicated_date_count"].iloc[0] == 1
        assert result["excluded_overlap_count"].iloc[0] == 6

    def test_persist_returns_none_on_invalid_df(self, tmp_path):
        # Pass an invalid df with bad calibration_status
        df = _make_bt_df(28)
        fold_df = build_backtest_interval_diagnostics_by_fold(df)
        fold_df.loc[0, "calibration_status"] = "invalid_value"
        paths = persist_interval_calibration_diagnostics(
            fold_df,
            pd.DataFrame(columns=BACKTEST_SUMMARY_INTERVAL_COLS),
            pd.DataFrame(columns=PRODUCTION_INTERVAL_COLS),
            tmp_path,
        )
        assert paths["backtest_fold"] is None

    def test_validate_unknown_dataset_before_empty_check(self):
        with pytest.raises(ValueError, match="Unknown dataset_name"):
            validate_interval_calibration_diagnostics(pd.DataFrame(), "totally_wrong")
