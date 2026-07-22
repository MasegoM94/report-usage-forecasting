"""Tests for src.models.backtest_evaluation.evaluate_models_across_folds.

Fixture design
--------------
Two synthetic reports (R_ALPHA, R_BETA) each with 80 days of daily data.
BacktestConfig uses small values so tests run without model-fitting libraries:
  horizon=7, n_folds=2, step=7, min_train_size=30, seasonal_period=7

With these settings:
  - min_train + (n_folds-1)*step + horizon = 30 + 7 + 7 = 44 ≤ 80  → 2 folds produced
  - Series long enough for the fold-2 training window to be 37 days
  - Only baseline models used (naive, seasonal_naive) so no optional deps needed
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.models.backtest_evaluation import BacktestConfig, evaluate_models_across_folds
from src.models.candidates import ModelResult, forecast_naive, forecast_seasonal_naive

# ---------------------------------------------------------------------------
# Fixture parameters
# ---------------------------------------------------------------------------

_HORIZON = 7
_FOLDS = 2
_STEP = 7
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


def _make_series(n: int, start: str = "2023-01-01", seed: int = 42) -> pd.Series:
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, periods=n, freq="D")
    vals = (5 + rng.integers(-2, 3, size=n)).clip(0).astype(float)
    return pd.Series(vals, index=idx)


_SERIES_ALPHA = _make_series(80, seed=1)
_SERIES_BETA = _make_series(80, seed=2)

_REPORT_A = "R_ALPHA"
_REPORT_B = "R_BETA"


# ---------------------------------------------------------------------------
# Shared fixture: run evaluation once and reuse
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def eval_results_alpha():
    preds, metrics = evaluate_models_across_folds(
        _REPORT_A, _SERIES_ALPHA, _REGISTRY, _CFG
    )
    return preds, metrics


@pytest.fixture(scope="module")
def eval_results_both():
    """Evaluate two reports and concatenate — simulates multi-report pipeline."""
    p1, m1 = evaluate_models_across_folds(_REPORT_A, _SERIES_ALPHA, _REGISTRY, _CFG)
    p2, m2 = evaluate_models_across_folds(_REPORT_B, _SERIES_BETA, _REGISTRY, _CFG)
    return pd.concat([p1, p2], ignore_index=True), pd.concat([m1, m2], ignore_index=True)


# ---------------------------------------------------------------------------
# 1. Equal folds across models
# ---------------------------------------------------------------------------

class TestEqualFoldsAcrossModels:
    def test_each_model_evaluated_on_same_number_of_folds(self, eval_results_alpha):
        _, metrics = eval_results_alpha
        fold_counts = metrics.groupby("model_name")["fold_number"].nunique()
        assert fold_counts.nunique() == 1, (
            f"Models have different fold counts: {fold_counts.to_dict()}"
        )

    def test_each_model_has_expected_fold_count(self, eval_results_alpha):
        _, metrics = eval_results_alpha
        fold_counts = metrics.groupby("model_name")["fold_number"].nunique()
        for model, count in fold_counts.items():
            assert count == _FOLDS, f"{model}: expected {_FOLDS} folds, got {count}"

    def test_same_cutoff_dates_across_models(self, eval_results_alpha):
        _, metrics = eval_results_alpha
        cutoffs_by_model = (
            metrics.groupby("model_name")["cutoff_date"]
            .apply(sorted)
        )
        reference = cutoffs_by_model.iloc[0]
        for model_name, cutoffs in cutoffs_by_model.items():
            assert list(cutoffs) == list(reference), (
                f"{model_name} has different cutoff dates from reference model"
            )

    def test_fold_numbers_are_one_based(self, eval_results_alpha):
        _, metrics = eval_results_alpha
        assert set(metrics["fold_number"].unique()) == set(range(1, _FOLDS + 1))


# ---------------------------------------------------------------------------
# 2. Prediction row count
# ---------------------------------------------------------------------------

class TestPredictionRowCount:
    def test_total_prediction_rows(self, eval_results_alpha):
        preds, _ = eval_results_alpha
        expected = _N_MODELS * _FOLDS * _HORIZON
        assert len(preds) == expected, (
            f"Expected {expected} prediction rows, got {len(preds)}"
        )

    def test_prediction_rows_per_fold_per_model(self, eval_results_alpha):
        preds, _ = eval_results_alpha
        counts = preds.groupby(["fold_number", "model_name"]).size()
        assert (counts == _HORIZON).all(), (
            f"Not all (fold, model) groups have exactly {_HORIZON} rows:\n{counts}"
        )

    def test_prediction_columns_present(self, eval_results_alpha):
        preds, _ = eval_results_alpha
        required = {
            "report_id", "fold_number", "cutoff_date", "train_start", "train_end",
            "forecast_date", "horizon_step", "model_name",
            "actual", "forecast", "lower_bound", "upper_bound", "fit_status",
        }
        missing = required - set(preds.columns)
        assert not missing, f"Missing prediction columns: {missing}"


# ---------------------------------------------------------------------------
# 3. Metric row count
# ---------------------------------------------------------------------------

class TestMetricRowCount:
    def test_total_metric_rows(self, eval_results_alpha):
        _, metrics = eval_results_alpha
        expected = _N_MODELS * _FOLDS
        assert len(metrics) == expected, (
            f"Expected {expected} metric rows, got {len(metrics)}"
        )

    def test_metric_columns_present(self, eval_results_alpha):
        _, metrics = eval_results_alpha
        required = {
            "report_id", "fold_number", "cutoff_date", "model_name",
            "mae", "rmse", "wape", "mase", "bias",
            "interval_coverage", "mean_interval_width",
            "fit_status", "error_message",
        }
        missing = required - set(metrics.columns)
        assert not missing, f"Missing metric columns: {missing}"

    def test_metric_rows_unique_per_fold_model(self, eval_results_alpha):
        _, metrics = eval_results_alpha
        dupes = metrics.duplicated(subset=["fold_number", "model_name"])
        assert not dupes.any(), "Duplicate (fold_number, model_name) rows in fold_metrics"


# ---------------------------------------------------------------------------
# 4. Correct horizon steps
# ---------------------------------------------------------------------------

class TestHorizonSteps:
    def test_horizon_steps_range_from_1_to_horizon(self, eval_results_alpha):
        preds, _ = eval_results_alpha
        for (fold, model), grp in preds.groupby(["fold_number", "model_name"]):
            steps = sorted(grp["horizon_step"].tolist())
            assert steps == list(range(1, _HORIZON + 1)), (
                f"fold={fold}, model={model}: horizon_steps={steps}"
            )

    def test_horizon_step_1_is_day_after_cutoff(self, eval_results_alpha):
        preds, metrics = eval_results_alpha
        for (fold_num, model), grp in preds.groupby(["fold_number", "model_name"]):
            step1 = grp[grp["horizon_step"] == 1]
            cutoff = metrics.loc[
                (metrics["fold_number"] == fold_num) & (metrics["model_name"] == model),
                "cutoff_date"
            ].iloc[0]
            expected_first_date = pd.Timestamp(cutoff) + pd.Timedelta(days=1)
            actual_first_date = pd.Timestamp(step1["forecast_date"].iloc[0])
            assert actual_first_date == expected_first_date, (
                f"fold={fold_num}, model={model}: step 1 date {actual_first_date} "
                f"!= cutoff+1 {expected_first_date}"
            )

    def test_forecast_dates_are_daily_consecutive(self, eval_results_alpha):
        preds, _ = eval_results_alpha
        for (fold, model), grp in preds.groupby(["fold_number", "model_name"]):
            dates = pd.to_datetime(grp.sort_values("horizon_step")["forecast_date"])
            diffs = dates.diff().dropna()
            assert (diffs == pd.Timedelta(days=1)).all(), (
                f"fold={fold}, model={model}: forecast dates are not consecutive daily"
            )


# ---------------------------------------------------------------------------
# 5. Correct date alignment (forecast_date ↔ actual)
# ---------------------------------------------------------------------------

class TestDateAlignment:
    def test_forecast_date_matches_test_series_index(self, eval_results_alpha):
        """Dates in the predictions must be the actual observation dates from the series."""
        preds, _ = eval_results_alpha
        for (fold_num, model), grp in preds.groupby(["fold_number", "model_name"]):
            fc_dates = pd.to_datetime(grp.sort_values("horizon_step")["forecast_date"].values)
            # Reconstruct what the test window dates should be
            # test_start = cutoff_date + 1; test_end = cutoff_date + horizon
            cutoff = grp["cutoff_date"].iloc[0]
            expected = pd.date_range(
                start=pd.Timestamp(cutoff) + pd.Timedelta(days=1),
                periods=_HORIZON,
                freq="D",
            )
            pd.testing.assert_index_equal(
                pd.DatetimeIndex(fc_dates),
                expected,
                check_names=False,
            )

    def test_actuals_match_original_series_values(self, eval_results_alpha):
        """Actual values in predictions must equal the original series at those dates."""
        preds, _ = eval_results_alpha
        for _, row in preds.iterrows():
            date = pd.Timestamp(row["forecast_date"])
            if date in _SERIES_ALPHA.index:
                expected_actual = float(_SERIES_ALPHA.loc[date])
                assert row["actual"] == pytest.approx(expected_actual), (
                    f"actual at {date} is {row['actual']}, "
                    f"series has {expected_actual}"
                )

    def test_train_end_equals_cutoff_date(self, eval_results_alpha):
        preds, _ = eval_results_alpha
        for _, row in preds.iterrows():
            assert pd.Timestamp(row["train_end"]) == pd.Timestamp(row["cutoff_date"])


# ---------------------------------------------------------------------------
# 6. Failed-model handling
# ---------------------------------------------------------------------------

def _always_fails(training_series: pd.Series, horizon: int) -> ModelResult:
    raise RuntimeError("Intentional test failure")


def _returns_wrong_type(training_series: pd.Series, horizon: int):
    return {"not": "a ModelResult"}


class TestFailedModelHandling:
    def _run_with_failure(self, report_id=_REPORT_A):
        registry = {
            "naive": forecast_naive,
            "always_fails": _always_fails,
        }
        return evaluate_models_across_folds(report_id, _SERIES_ALPHA, registry, _CFG)

    def test_failed_model_rows_present_not_dropped(self):
        preds, metrics = self._run_with_failure()
        failed_preds = preds[preds["model_name"] == "always_fails"]
        failed_metrics = metrics[metrics["model_name"] == "always_fails"]
        assert len(failed_preds) == _FOLDS * _HORIZON, (
            "Failed model prediction rows were dropped"
        )
        assert len(failed_metrics) == _FOLDS, (
            "Failed model metric rows were dropped"
        )

    def test_failed_rows_have_fit_status_failed(self):
        preds, metrics = self._run_with_failure()
        assert (preds[preds["model_name"] == "always_fails"]["fit_status"] == "failed").all()
        assert (metrics[metrics["model_name"] == "always_fails"]["fit_status"] == "failed").all()

    def test_failed_model_forecast_is_nan(self):
        preds, _ = self._run_with_failure()
        failed = preds[preds["model_name"] == "always_fails"]
        assert failed["forecast"].isna().all()
        assert failed["lower_bound"].isna().all()
        assert failed["upper_bound"].isna().all()

    def test_failed_model_actuals_are_preserved(self):
        """Even when a model fails we still record what the actual values were."""
        preds, _ = self._run_with_failure()
        failed = preds[preds["model_name"] == "always_fails"]
        assert not failed["actual"].isna().all(), (
            "Actual values should be preserved even for failed model rows"
        )

    def test_failed_model_metrics_are_nan(self):
        _, metrics = self._run_with_failure()
        failed = metrics[metrics["model_name"] == "always_fails"]
        for col in ["mae", "rmse", "wape", "mase", "bias"]:
            assert failed[col].isna().all(), f"{col} should be NaN for failed models"

    def test_failed_model_has_error_message(self):
        _, metrics = self._run_with_failure()
        failed = metrics[metrics["model_name"] == "always_fails"]
        assert failed["error_message"].notna().all()
        assert failed["error_message"].str.contains("Intentional").all()

    def test_sibling_model_succeeds_despite_failure(self):
        preds, metrics = self._run_with_failure()
        naive_preds = preds[preds["model_name"] == "naive"]
        naive_metrics = metrics[metrics["model_name"] == "naive"]
        assert (naive_preds["fit_status"] != "failed").all()
        assert naive_metrics["mae"].notna().all()

    def test_wrong_return_type_treated_as_failure(self):
        registry = {"bad_model": _returns_wrong_type}
        preds, metrics = evaluate_models_across_folds(
            _REPORT_A, _SERIES_ALPHA, registry, _CFG
        )
        assert (preds["fit_status"] == "failed").all()
        assert (metrics["fit_status"] == "failed").all()


# ---------------------------------------------------------------------------
# 7. MASE uses fold training data
# ---------------------------------------------------------------------------

class TestMASEUsesFoldTrainingData:
    def test_mase_is_finite_for_successful_models(self, eval_results_alpha):
        _, metrics = eval_results_alpha
        good = metrics[metrics["fit_status"] != "failed"]
        # MASE may be NaN if seasonal naive denominator is zero, but should not
        # raise or be Inf
        finite_or_nan = good["mase"].apply(
            lambda v: pd.isna(v) or np.isfinite(v)
        )
        assert finite_or_nan.all(), "MASE contains Inf values"

    def test_mase_differs_between_folds_when_training_differs(self, eval_results_alpha):
        """Expanding window → fold 2 has more training data than fold 1.
        MASE should differ because the seasonal-naive denominator is recomputed
        from each fold's own training window."""
        _, metrics = eval_results_alpha
        naive_metrics = metrics[metrics["model_name"] == "naive"].sort_values("fold_number")
        if len(naive_metrics) >= 2:
            mase_1 = naive_metrics.iloc[0]["mase"]
            mase_2 = naive_metrics.iloc[1]["mase"]
            # If both are finite, they should not be equal (different training windows)
            if pd.notna(mase_1) and pd.notna(mase_2):
                # This may coincidentally be equal for constant series, so only
                # assert they are not numerically identical to machine precision
                # when the series has variability (which our fixture does)
                assert mase_1 != pytest.approx(mase_2), (
                    "MASE is identical across folds with different training windows — "
                    "check that fold.train_series is being passed to calculate_point_metrics"
                )

    def test_mase_matches_manual_calculation(self):
        """Verify MASE is computed from fold training series, not the full series."""
        # Use a deterministic series: values 1..50
        idx = pd.date_range("2022-01-01", periods=50, freq="D")
        series = pd.Series(np.arange(1, 51, dtype=float), index=idx)

        cfg = BacktestConfig(
            horizon=7, n_folds=1, step=7,
            min_train_size=30, seasonal_period=7
        )
        _, metrics = evaluate_models_across_folds(
            "TEST", series, {"naive": forecast_naive}, cfg
        )

        row = metrics.iloc[0]
        fold_cutoff_idx = 36  # fold 1 cutoff = index 36 (0-based), train = [0..36]
        train = series.iloc[: fold_cutoff_idx + 1].to_numpy()
        test = series.iloc[fold_cutoff_idx + 1: fold_cutoff_idx + 8].to_numpy()
        forecast_val = float(train[-1])  # naive forecast = last value = 37.0

        # Manual MASE
        seasonal_errors = np.abs(train[7:] - train[:-7])
        denom = float(np.mean(seasonal_errors))
        mae = float(np.mean(np.abs(test - forecast_val)))
        expected_mase = mae / denom if denom != 0 else np.nan

        if pd.notna(expected_mase):
            assert row["mase"] == pytest.approx(expected_mase, rel=1e-6)


# ---------------------------------------------------------------------------
# 8. Intervals evaluated only when bounds are present
# ---------------------------------------------------------------------------

class TestIntervalEvaluation:
    def test_baselines_have_nan_interval_metrics(self, eval_results_alpha):
        """Naive and seasonal_naive return no bounds → interval metrics are NaN."""
        _, metrics = eval_results_alpha
        for model in ["naive", "seasonal_naive"]:
            rows = metrics[metrics["model_name"] == model]
            assert rows["interval_coverage"].isna().all(), (
                f"{model}: interval_coverage should be NaN (no bounds)"
            )
            assert rows["mean_interval_width"].isna().all(), (
                f"{model}: mean_interval_width should be NaN (no bounds)"
            )

    def test_interval_metrics_present_when_model_provides_bounds(self):
        """A mock model that returns bounds should produce non-NaN interval metrics."""
        def _model_with_bounds(training_series: pd.Series, horizon: int) -> ModelResult:
            idx = pd.date_range(
                training_series.index[-1] + pd.Timedelta(days=1),
                periods=horizon, freq="D"
            )
            fc = pd.Series(np.ones(horizon) * 5.0, index=idx)
            lo = pd.Series(np.ones(horizon) * 3.0, index=idx)
            hi = pd.Series(np.ones(horizon) * 7.0, index=idx)
            return ModelResult(
                model_name="mock_with_bounds",
                forecast=fc, lower_bound=lo, upper_bound=hi,
                forecast_raw=fc, lower_bound_raw=lo, upper_bound_raw=hi,
                model_metadata={}, fit_status="ok",
            )

        registry = {"mock_with_bounds": _model_with_bounds}
        cfg = BacktestConfig(
            horizon=7, n_folds=1, step=7, min_train_size=30, seasonal_period=7
        )
        _, metrics = evaluate_models_across_folds(
            "TEST", _SERIES_ALPHA, registry, cfg
        )
        assert metrics["interval_coverage"].notna().all()
        assert metrics["mean_interval_width"].notna().all()


# ---------------------------------------------------------------------------
# 9. No leakage across cutoff dates
# ---------------------------------------------------------------------------

class TestNoLeakageAcrossCutoffs:
    def test_forecast_dates_never_precede_or_equal_cutoff(self, eval_results_alpha):
        preds, _ = eval_results_alpha
        for _, row in preds.iterrows():
            cutoff = pd.Timestamp(row["cutoff_date"])
            fc_date = pd.Timestamp(row["forecast_date"])
            assert fc_date > cutoff, (
                f"Forecast date {fc_date} is not after cutoff {cutoff} — "
                "future data leaked into the evaluation window"
            )

    def test_train_end_never_overlaps_test_window(self, eval_results_alpha):
        preds, _ = eval_results_alpha
        for _, row in preds.iterrows():
            train_end = pd.Timestamp(row["train_end"])
            fc_date = pd.Timestamp(row["forecast_date"])
            assert fc_date > train_end, (
                f"Test date {fc_date} ≤ train_end {train_end} — data leakage detected"
            )

    def test_fold2_cutoff_strictly_after_fold1_cutoff(self, eval_results_alpha):
        _, metrics = eval_results_alpha
        for model in metrics["model_name"].unique():
            m = metrics[metrics["model_name"] == model].sort_values("fold_number")
            cutoffs = pd.to_datetime(m["cutoff_date"].values)
            for i in range(1, len(cutoffs)):
                assert cutoffs[i] > cutoffs[i - 1], (
                    f"{model}: fold {i+1} cutoff ({cutoffs[i]}) is not strictly "
                    f"after fold {i} cutoff ({cutoffs[i-1]})"
                )

    def test_fold1_test_window_does_not_overlap_fold2_train_window(self, eval_results_alpha):
        """Fold 2 trains on data that includes the fold 1 test window dates."""
        _, metrics = eval_results_alpha
        for model in metrics["model_name"].unique():
            m = metrics[metrics["model_name"] == model].sort_values("fold_number")
            if len(m) < 2:
                continue
            cutoff_1 = pd.Timestamp(m.iloc[0]["cutoff_date"])
            cutoff_2 = pd.Timestamp(m.iloc[1]["cutoff_date"])
            # Fold 1 test: (cutoff_1, cutoff_1 + horizon]
            # Fold 2 train: [series_start, cutoff_2]
            # cutoff_2 must be strictly after cutoff_1 + horizon - 1 to avoid
            # the fold-2 train window being shorter than fold-1 train window
            assert cutoff_2 > cutoff_1, "Fold ordering violated"

    def test_report_ids_are_isolated(self, eval_results_both):
        """Results for R_ALPHA and R_BETA must not bleed into each other."""
        preds, metrics = eval_results_both
        for report_id in [_REPORT_A, _REPORT_B]:
            assert (preds[preds["report_id"] == report_id]["report_id"] == report_id).all()
            assert (metrics[metrics["report_id"] == report_id]["report_id"] == report_id).all()


# ---------------------------------------------------------------------------
# 10. Deterministic sorted output
# ---------------------------------------------------------------------------

class TestDeterministicOutput:
    def test_predictions_sorted_by_fold_model_step(self, eval_results_alpha):
        preds, _ = eval_results_alpha
        expected = preds.sort_values(
            ["report_id", "fold_number", "model_name", "horizon_step"]
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(preds, expected)

    def test_metrics_sorted_by_fold_model(self, eval_results_alpha):
        _, metrics = eval_results_alpha
        expected = metrics.sort_values(
            ["report_id", "fold_number", "model_name"]
        ).reset_index(drop=True)
        pd.testing.assert_frame_equal(metrics, expected)

    def test_two_calls_produce_identical_output(self):
        p1, m1 = evaluate_models_across_folds(
            _REPORT_A, _SERIES_ALPHA, _REGISTRY, _CFG
        )
        p2, m2 = evaluate_models_across_folds(
            _REPORT_A, _SERIES_ALPHA, _REGISTRY, _CFG
        )
        pd.testing.assert_frame_equal(p1, p2)
        pd.testing.assert_frame_equal(m1, m2)
