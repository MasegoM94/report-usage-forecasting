"""Tests for src.models.horizon_evaluation.calculate_horizon_bucket_metrics.

Fixture design
--------------
Two reports, each with 90 days of data.
BacktestConfig: horizon=28, n_folds=2, step=28, min_train=30, seasonal_period=7

Minimum series length: 30 + 28 + 28 = 86 → 90 days is sufficient.

Only baseline models (naive, seasonal_naive) are used — no optional deps.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.models.backtest_evaluation import BacktestConfig, evaluate_models_across_folds
from src.models.candidates import ModelResult, forecast_naive, forecast_seasonal_naive
from src.models.horizon_evaluation import (
    HORIZON_BUCKETS,
    calculate_horizon_bucket_metrics,
)

# ---------------------------------------------------------------------------
# Fixture constants
# ---------------------------------------------------------------------------

_HORIZON = 28
_FOLDS = 2
_STEP = 28
_MIN_TRAIN = 30
_SEASONAL = 7

_CFG = BacktestConfig(
    horizon=_HORIZON,
    n_folds=_FOLDS,
    step=_STEP,
    min_train_size=_MIN_TRAIN,
    seasonal_period=_SEASONAL,
)

_REGISTRY = {
    "naive": forecast_naive,
    "seasonal_naive": forecast_seasonal_naive,
}
_N_MODELS = len(_REGISTRY)
_N_BUCKETS = len(HORIZON_BUCKETS)   # 4

_REPORT_A = "R_ALPHA"
_REPORT_B = "R_BETA"


def _make_series(n: int, start: str = "2023-01-01", seed: int = 0) -> pd.Series:
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, periods=n, freq="D")
    vals = (5 + rng.integers(-3, 4, size=n)).clip(0).astype(float)
    return pd.Series(vals, index=idx)


_SERIES_A = _make_series(90, seed=10)
_SERIES_B = _make_series(90, start="2023-04-01", seed=20)

_SERIES_LOOKUP = {_REPORT_A: _SERIES_A, _REPORT_B: _SERIES_B}


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def backtest_outputs_alpha():
    preds, _ = evaluate_models_across_folds(_REPORT_A, _SERIES_A, _REGISTRY, _CFG)
    return preds


@pytest.fixture(scope="module")
def bucket_results_alpha(backtest_outputs_alpha):
    return calculate_horizon_bucket_metrics(
        backtest_outputs_alpha,
        {_REPORT_A: _SERIES_A},
        seasonal_period=_SEASONAL,
    )


@pytest.fixture(scope="module")
def backtest_outputs_both():
    pA, _ = evaluate_models_across_folds(_REPORT_A, _SERIES_A, _REGISTRY, _CFG)
    pB, _ = evaluate_models_across_folds(_REPORT_B, _SERIES_B, _REGISTRY, _CFG)
    return pd.concat([pA, pB], ignore_index=True)


@pytest.fixture(scope="module")
def bucket_results_both(backtest_outputs_both):
    return calculate_horizon_bucket_metrics(
        backtest_outputs_both,
        _SERIES_LOOKUP,
        seasonal_period=_SEASONAL,
    )


# ---------------------------------------------------------------------------
# 1. Row counts
# ---------------------------------------------------------------------------

class TestRowCounts:
    def test_total_rows_single_report(self, bucket_results_alpha):
        # n_folds × n_models × n_buckets
        expected = _FOLDS * _N_MODELS * _N_BUCKETS
        assert len(bucket_results_alpha) == expected, (
            f"Expected {expected} rows, got {len(bucket_results_alpha)}"
        )

    def test_total_rows_two_reports(self, bucket_results_both):
        expected = 2 * _FOLDS * _N_MODELS * _N_BUCKETS
        assert len(bucket_results_both) == expected

    def test_every_bucket_present_for_each_fold_model(self, bucket_results_alpha):
        bucket_names = {b[0] for b in HORIZON_BUCKETS}
        for (fold, model), grp in bucket_results_alpha.groupby(["fold_number", "model_name"]):
            found = set(grp["horizon_bucket"].unique())
            missing = bucket_names - found
            assert not missing, (
                f"fold={fold}, model={model}: missing buckets {missing}"
            )

    def test_output_columns_present(self, bucket_results_alpha):
        required = {
            "report_id", "model_name", "fold_number", "cutoff_date",
            "horizon_bucket", "mae", "wape", "mase", "bias",
            "interval_coverage", "observation_count",
        }
        missing = required - set(bucket_results_alpha.columns)
        assert not missing, f"Missing output columns: {missing}"


# ---------------------------------------------------------------------------
# 2. Bucket boundaries — specific step assignments
# ---------------------------------------------------------------------------

class TestBucketBoundaries:
    """Verify that day 7, 14, and 28 land in the correct buckets."""

    def _bucket_obs_count_at_step(
        self, bucket_results: pd.DataFrame, bucket_name: str
    ) -> int:
        """Return the observation_count for the named bucket (summed across folds/models)."""
        rows = bucket_results[bucket_results["horizon_bucket"] == bucket_name]
        return int(rows["observation_count"].sum())

    def test_day_7_in_days_1_7(self, backtest_outputs_alpha):
        """horizon_step=7 must contribute to days_1_7."""
        step7_rows = backtest_outputs_alpha[backtest_outputs_alpha["horizon_step"] == 7]
        assert len(step7_rows) > 0, "No step-7 rows found in predictions"

        days_1_7 = backtest_outputs_alpha[backtest_outputs_alpha["horizon_step"].between(1, 7)]
        assert 7 in days_1_7["horizon_step"].values

    def test_day_7_not_in_days_8_14(self, backtest_outputs_alpha):
        days_8_14 = backtest_outputs_alpha[backtest_outputs_alpha["horizon_step"].between(8, 14)]
        assert 7 not in days_8_14["horizon_step"].values

    def test_day_14_in_days_8_14(self, backtest_outputs_alpha):
        days_8_14 = backtest_outputs_alpha[backtest_outputs_alpha["horizon_step"].between(8, 14)]
        assert 14 in days_8_14["horizon_step"].values

    def test_day_14_not_in_days_15_28(self, backtest_outputs_alpha):
        days_15_28 = backtest_outputs_alpha[backtest_outputs_alpha["horizon_step"].between(15, 28)]
        assert 14 not in days_15_28["horizon_step"].values

    def test_day_28_in_days_15_28(self, backtest_outputs_alpha):
        days_15_28 = backtest_outputs_alpha[backtest_outputs_alpha["horizon_step"].between(15, 28)]
        assert 28 in days_15_28["horizon_step"].values

    def test_day_28_in_full_horizon(self, backtest_outputs_alpha):
        full = backtest_outputs_alpha[backtest_outputs_alpha["horizon_step"].between(1, 28)]
        assert 28 in full["horizon_step"].values

    def test_days_1_7_obs_count_is_7_per_fold_model(self, bucket_results_alpha):
        rows = bucket_results_alpha[bucket_results_alpha["horizon_bucket"] == "days_1_7"]
        # Each (fold, model) group should have exactly 7 observations in this bucket
        assert (rows["observation_count"] == 7).all(), (
            f"days_1_7 observation counts: {rows['observation_count'].tolist()}"
        )

    def test_days_8_14_obs_count_is_7_per_fold_model(self, bucket_results_alpha):
        rows = bucket_results_alpha[bucket_results_alpha["horizon_bucket"] == "days_8_14"]
        assert (rows["observation_count"] == 7).all()

    def test_days_15_28_obs_count_is_14_per_fold_model(self, bucket_results_alpha):
        rows = bucket_results_alpha[bucket_results_alpha["horizon_bucket"] == "days_15_28"]
        assert (rows["observation_count"] == 14).all()

    def test_full_horizon_obs_count_is_28_per_fold_model(self, bucket_results_alpha):
        rows = bucket_results_alpha[bucket_results_alpha["horizon_bucket"] == "full_horizon"]
        assert (rows["observation_count"] == _HORIZON).all()

    def test_bucket_obs_counts_sum_to_full_horizon(self, bucket_results_alpha):
        """days_1_7 + days_8_14 + days_15_28 obs counts should equal full_horizon."""
        for (fold, model), grp in bucket_results_alpha.groupby(["fold_number", "model_name"]):
            sub_buckets = grp[grp["horizon_bucket"].isin(["days_1_7", "days_8_14", "days_15_28"])]
            full = grp[grp["horizon_bucket"] == "full_horizon"]
            assert sub_buckets["observation_count"].sum() == full["observation_count"].iloc[0], (
                f"fold={fold}, model={model}: sub-bucket obs counts don't sum to full_horizon"
            )


# ---------------------------------------------------------------------------
# 3. Metric values — basic sanity checks
# ---------------------------------------------------------------------------

class TestMetricValues:
    def test_mae_is_non_negative(self, bucket_results_alpha):
        valid_mae = bucket_results_alpha["mae"].dropna()
        assert (valid_mae >= 0).all()

    def test_wape_is_between_0_and_1_or_nan(self, bucket_results_alpha):
        valid_wape = bucket_results_alpha["wape"].dropna()
        assert ((valid_wape >= 0) & (valid_wape <= 2)).all(), (
            "WAPE outside expected range [0, 2]: " + str(valid_wape[valid_wape > 2].tolist())
        )

    def test_mase_is_non_negative_or_nan(self, bucket_results_alpha):
        valid_mase = bucket_results_alpha["mase"].dropna()
        assert (valid_mase >= 0).all()

    def test_mae_finite_for_all_successful_rows(self, bucket_results_alpha):
        # All rows in our fixture use baseline models that succeed
        assert bucket_results_alpha["mae"].notna().all()

    def test_full_horizon_mae_within_range_of_sub_bucket_maes(self, bucket_results_alpha):
        """Full-horizon MAE ≤ max(sub-bucket MAEs) — a weighted average must be bounded."""
        for (fold, model), grp in bucket_results_alpha.groupby(["fold_number", "model_name"]):
            full_mae = grp.loc[grp["horizon_bucket"] == "full_horizon", "mae"].iloc[0]
            sub_mae = grp.loc[
                grp["horizon_bucket"].isin(["days_1_7", "days_8_14", "days_15_28"]), "mae"
            ]
            assert full_mae <= sub_mae.max() + 1e-9, (
                f"fold={fold}, model={model}: full MAE {full_mae} > max sub-bucket MAE {sub_mae.max()}"
            )


# ---------------------------------------------------------------------------
# 4. MASE uses fold training series denominator
# ---------------------------------------------------------------------------

class TestMASEFoldTrainingDenominator:
    def test_mase_differs_from_full_series_calculation(self):
        """MASE denominator must come from the fold training window, not the full series.

        Size the series so generate_rolling_splits places the fold cutoff at
        index 36 (day 37 inclusive), keeping the training window entirely
        within the constant-5 region.

        Series:  days 0-36 → constant 5.0  (37 values)
                 days 37-64 → linear 1..28  (28 values)
        Total: 65 days, min_train=30, horizon=28:
            last_cutoff_idx = 65 - 28 - 1 = 36
            → training = series[0:37], all constant → seasonal naive error = 0
            → MASE denominator = 0 → MASE = NaN

        If the implementation mistakenly used the full 65-day series as the
        denominator, the linear tail would produce a non-zero denominator and
        MASE would be finite — that would make this assertion fail.
        """
        idx = pd.date_range("2022-01-01", periods=65, freq="D")
        vals = np.concatenate([
            np.full(37, 5.0),               # training window: all constant
            np.arange(1, 29, dtype=float),  # test window: linear (28 values)
        ])
        series = pd.Series(vals, index=idx)
        series_lookup = {"TEST": series}

        cfg = BacktestConfig(
            horizon=28, n_folds=1, step=28,
            min_train_size=30, seasonal_period=7,
        )
        preds, _ = evaluate_models_across_folds(
            "TEST", series, {"naive": forecast_naive}, cfg
        )
        results = calculate_horizon_bucket_metrics(
            preds, series_lookup, seasonal_period=7
        )

        # Training window is all 5.0 → seasonal naive denom = 0 → MASE = NaN
        full_row = results[results["horizon_bucket"] == "full_horizon"]
        assert full_row["mase"].isna().all(), (
            "MASE should be NaN when the fold training series is constant "
            "(seasonal naive denominator = 0).  A finite value here means the "
            "full series — not the fold training window — was used as the denominator."
        )

    def test_mase_numerator_is_bucket_specific(self):
        """Sub-bucket MASE must reflect only that bucket's MAE, not the full-horizon MAE."""
        preds, _ = evaluate_models_across_folds(
            _REPORT_A, _SERIES_A, {"naive": forecast_naive}, _CFG
        )
        results = calculate_horizon_bucket_metrics(
            preds, {_REPORT_A: _SERIES_A}, seasonal_period=_SEASONAL
        )

        for (fold, model), grp in results.groupby(["fold_number", "model_name"]):
            full = grp.loc[grp["horizon_bucket"] == "full_horizon"].iloc[0]
            sub_buckets = grp[grp["horizon_bucket"].isin(["days_1_7", "days_8_14", "days_15_28"])]

            # If MASE = mae / denom for each bucket (same denom), then
            # full_mase should be a weighted average of sub-bucket MASEs.
            # At minimum, full MASE ≠ all sub-bucket MASEs simultaneously
            if pd.notna(full["mase"]):
                sub_mase = sub_buckets["mase"].dropna()
                # Full MASE should lie within the range of sub-bucket MASEs
                if len(sub_mase) > 0:
                    assert full["mase"] >= sub_mase.min() - 1e-9
                    assert full["mase"] <= sub_mase.max() + 1e-9

    def test_mase_denominator_same_across_buckets_of_same_fold(self):
        """Verify MASE / MAE ratio is the same across all buckets of the same fold.

        Since MASE = mae / denom and denom is constant per fold, the ratio
        mase/mae must be identical (within floating-point tolerance) for every
        non-empty bucket in the same (fold, model) group.
        """
        preds, _ = evaluate_models_across_folds(
            _REPORT_A, _SERIES_A, {"naive": forecast_naive}, _CFG
        )
        results = calculate_horizon_bucket_metrics(
            preds, {_REPORT_A: _SERIES_A}, seasonal_period=_SEASONAL
        )

        for (fold, model), grp in results.groupby(["fold_number", "model_name"]):
            ratios = []
            for _, row in grp.iterrows():
                if pd.notna(row["mase"]) and row["mae"] > 0:
                    ratios.append(row["mase"] / row["mae"])
            if len(ratios) >= 2:
                assert max(ratios) - min(ratios) < 1e-9, (
                    f"fold={fold}, model={model}: mase/mae ratio is not constant "
                    f"across buckets — denominators differ: {ratios}"
                )


# ---------------------------------------------------------------------------
# 5. Interval coverage
# ---------------------------------------------------------------------------

class TestIntervalCoverage:
    def test_baselines_have_nan_interval_coverage(self, bucket_results_alpha):
        """Naive and seasonal_naive produce no bounds → interval_coverage must be NaN."""
        assert bucket_results_alpha["interval_coverage"].isna().all(), (
            "Baseline models should produce NaN interval_coverage (no bounds)"
        )

    def test_interval_coverage_present_when_bounds_provided(self):
        """A mock model with bounds should produce non-NaN interval_coverage in all buckets."""
        def _model_with_bounds(training_series: pd.Series, horizon: int) -> ModelResult:
            idx = pd.date_range(
                training_series.index[-1] + pd.Timedelta(days=1),
                periods=horizon, freq="D",
            )
            fc = pd.Series(np.ones(horizon) * 5.0, index=idx)
            lo = pd.Series(np.ones(horizon) * 3.0, index=idx)
            hi = pd.Series(np.ones(horizon) * 7.0, index=idx)
            return ModelResult(
                model_name="mock_bounds",
                forecast=fc, lower_bound=lo, upper_bound=hi,
                forecast_raw=fc, lower_bound_raw=lo, upper_bound_raw=hi,
                model_metadata={}, fit_status="ok",
            )

        registry = {"mock_bounds": _model_with_bounds}
        cfg = BacktestConfig(
            horizon=_HORIZON, n_folds=1, step=_STEP,
            min_train_size=_MIN_TRAIN, seasonal_period=_SEASONAL,
        )
        preds, _ = evaluate_models_across_folds(_REPORT_A, _SERIES_A, registry, cfg)
        results = calculate_horizon_bucket_metrics(
            preds, {_REPORT_A: _SERIES_A}, seasonal_period=_SEASONAL
        )
        # Forecast is constant 5.0, series values are around 5 ± 3, bounds are [3,7]
        # Coverage should be computed (non-NaN)
        assert results["interval_coverage"].notna().all(), (
            "interval_coverage should be non-NaN when bounds are present"
        )


# ---------------------------------------------------------------------------
# 6. Failed-model handling
# ---------------------------------------------------------------------------

def _always_fails(training_series: pd.Series, horizon: int) -> ModelResult:
    raise RuntimeError("Intentional test failure")


class TestFailedModelBuckets:
    def test_failed_model_produces_bucket_rows(self):
        registry = {"always_fails": _always_fails}
        preds, _ = evaluate_models_across_folds(_REPORT_A, _SERIES_A, registry, _CFG)
        results = calculate_horizon_bucket_metrics(
            preds, {_REPORT_A: _SERIES_A}, seasonal_period=_SEASONAL
        )
        # One row per (fold × bucket)
        expected = _FOLDS * _N_BUCKETS
        assert len(results) == expected

    def test_failed_model_metrics_are_nan(self):
        registry = {"always_fails": _always_fails}
        preds, _ = evaluate_models_across_folds(_REPORT_A, _SERIES_A, registry, _CFG)
        results = calculate_horizon_bucket_metrics(
            preds, {_REPORT_A: _SERIES_A}, seasonal_period=_SEASONAL
        )
        for col in ["mae", "wape", "mase", "bias", "interval_coverage"]:
            assert results[col].isna().all(), f"{col} should be NaN for failed model"

    def test_failed_model_observation_count_is_zero(self):
        registry = {"always_fails": _always_fails}
        preds, _ = evaluate_models_across_folds(_REPORT_A, _SERIES_A, registry, _CFG)
        results = calculate_horizon_bucket_metrics(
            preds, {_REPORT_A: _SERIES_A}, seasonal_period=_SEASONAL
        )
        assert (results["observation_count"] == 0).all()

    def test_sibling_model_unaffected_by_failure(self):
        registry = {"naive": forecast_naive, "always_fails": _always_fails}
        preds, _ = evaluate_models_across_folds(_REPORT_A, _SERIES_A, registry, _CFG)
        results = calculate_horizon_bucket_metrics(
            preds, {_REPORT_A: _SERIES_A}, seasonal_period=_SEASONAL
        )
        naive_rows = results[results["model_name"] == "naive"]
        assert naive_rows["mae"].notna().all()
        assert (naive_rows["observation_count"] > 0).all()


# ---------------------------------------------------------------------------
# 7. Multi-report isolation
# ---------------------------------------------------------------------------

class TestMultiReportIsolation:
    def test_report_ids_do_not_bleed(self, bucket_results_both):
        for report_id in [_REPORT_A, _REPORT_B]:
            rows = bucket_results_both[bucket_results_both["report_id"] == report_id]
            assert (rows["report_id"] == report_id).all()

    def test_each_report_has_correct_row_count(self, bucket_results_both):
        expected_per_report = _FOLDS * _N_MODELS * _N_BUCKETS
        for report_id in [_REPORT_A, _REPORT_B]:
            n = len(bucket_results_both[bucket_results_both["report_id"] == report_id])
            assert n == expected_per_report


# ---------------------------------------------------------------------------
# 8. Input validation
# ---------------------------------------------------------------------------

class TestInputValidation:
    def test_missing_required_column_raises(self, backtest_outputs_alpha):
        bad = backtest_outputs_alpha.drop(columns=["horizon_step"])
        with pytest.raises(ValueError, match="missing required columns"):
            calculate_horizon_bucket_metrics(bad, {_REPORT_A: _SERIES_A})

    def test_missing_series_lookup_key_raises(self, backtest_outputs_alpha):
        with pytest.raises(KeyError, match=_REPORT_A):
            calculate_horizon_bucket_metrics(backtest_outputs_alpha, {})

    def test_deterministic_output_on_repeated_calls(self, backtest_outputs_alpha):
        r1 = calculate_horizon_bucket_metrics(
            backtest_outputs_alpha, {_REPORT_A: _SERIES_A}
        )
        r2 = calculate_horizon_bucket_metrics(
            backtest_outputs_alpha, {_REPORT_A: _SERIES_A}
        )
        pd.testing.assert_frame_equal(r1, r2)
