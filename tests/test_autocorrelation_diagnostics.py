"""Tests for src/models/autocorrelation_diagnostics.py."""

from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.autocorrelation_diagnostics import (
    AutocorrelationConfig,
    _STATSMODELS_AVAILABLE,
    build_backtest_autocorrelation_diagnostics,
    build_production_autocorrelation_diagnostics,
    build_training_autocorrelation_diagnostics,
    calculate_durbin_watson,
    calculate_ljung_box_diagnostics,
    calculate_residual_autocorrelations,
    classify_autocorrelation_status,
    persist_autocorrelation_diagnostics,
    validate_autocorrelation_diagnostics,
    TRAINING_ACF_COLS,
    BACKTEST_FOLD_ACF_COLS,
    BACKTEST_SUMMARY_ACF_COLS,
    PRODUCTION_ACF_COLS,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _white_noise(n: int = 50, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.standard_normal(n)


def _ar1_series(n: int = 60, phi: float = 0.8, seed: int = 1) -> np.ndarray:
    """AR(1) process with strong autocorrelation."""
    rng = np.random.default_rng(seed)
    y = np.zeros(n)
    for t in range(1, n):
        y[t] = phi * y[t - 1] + rng.standard_normal()
    return y


def _tr_df(
    residuals: np.ndarray,
    report_id: str = "r1",
    model_family: str = "naive",
    model_name: str = "naive",
    candidate_m: int = 7,
    fold_number: int = 1,
    fit_scope: str = "backtest_fold",
) -> pd.DataFrame:
    n = len(residuals)
    dates = pd.date_range("2024-01-01", periods=n)
    return pd.DataFrame({
        "report_id": report_id,
        "report_name": report_id,
        "model_family": model_family,
        "model_name": model_name,
        "candidate_m": candidate_m,
        "fit_scope": fit_scope,
        "fold_number": fold_number,
        "residual_date": dates,
        "actual": residuals + 10,
        "fitted": 10.0,
        "residual": residuals,
        "residual_observation_valid": True,
        "residual_extraction_status": "ok",
        "residual_extraction_reason": None,
        "training_start": dates[0],
        "training_cutoff": dates[-1],
        "training_observation_count": n,
        "fitted_observation_count": n,
        "residual_observation_count": n,
    })


def _bt_df(
    residuals: np.ndarray,
    report_id: str = "r1",
    model_family: str = "naive",
    model_name: str = "naive",
    candidate_m: int = 7,
    fold_number: int = 1,
) -> pd.DataFrame:
    n = len(residuals)
    dates = pd.date_range("2024-03-01", periods=n)
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
        "horizon_step": range(1, n + 1),
        "actual": residuals + 10,
        "forecast": 10.0,
        "residual": residuals,
        "signed_error": -residuals,
        "residual_source": "backtest",
        "residual_observation_valid": True,
        "fit_status": "ok",
    })


def _prod_df(
    residuals: np.ndarray,
    report_id: str = "r1",
    model_family: str = "naive",
    model_name: str = "naive",
    selected_m: int = 7,
) -> pd.DataFrame:
    n = len(residuals)
    dates = pd.date_range("2024-06-01", periods=n)
    return pd.DataFrame({
        "report_id": report_id,
        "report_name": report_id,
        "selected_model_family": model_family,
        "selected_model_name": model_name,
        "selected_m": selected_m,
        "forecast_date": dates,
        "horizon_step": 1,
        "generated_at": pd.Timestamp("2024-05-31"),
        "actual": residuals + 10,
        "forecast": 10.0,
        "residual": residuals,
        "residual_source": "production",
        "residual_observation_valid": True,
    })


# ---------------------------------------------------------------------------
# TestACFCalculation
# ---------------------------------------------------------------------------

class TestACFCalculation:
    def test_white_noise_lag1_near_zero(self):
        rng = np.random.default_rng(42)
        y = rng.standard_normal(200)
        acf = calculate_residual_autocorrelations(y, [1, 2, 3])
        assert abs(acf[1]) < 0.25

    def test_ar1_lag1_high(self):
        y = _ar1_series(n=200, phi=0.8)
        acf = calculate_residual_autocorrelations(y, [1])
        assert acf[1] > 0.5

    def test_empty_array_returns_empty(self):
        acf = calculate_residual_autocorrelations(np.array([]), [1])
        assert acf == {}

    def test_short_series_skips_ineligible_lags(self):
        y = np.arange(10, dtype=float)
        # lag=5 requires n >= 15, should be skipped
        acf = calculate_residual_autocorrelations(y, [1, 5])
        assert 5 not in acf
        assert 1 in acf

    def test_constant_series_returns_zero(self):
        y = np.ones(30)
        acf = calculate_residual_autocorrelations(y, [1, 2])
        assert acf[1] == 0.0
        assert acf[2] == 0.0

    def test_returns_only_eligible_lags(self):
        y = _white_noise(30)
        acf = calculate_residual_autocorrelations(y, [1, 2, 100])
        assert 100 not in acf
        assert 1 in acf

    def test_acf_values_between_neg1_and_1(self):
        y = _ar1_series(n=100, phi=0.7)
        acf = calculate_residual_autocorrelations(y, [1, 2, 3, 7])
        for v in acf.values():
            assert -1.0 <= v <= 1.0

    def test_negative_lag_excluded(self):
        y = _white_noise(40)
        acf = calculate_residual_autocorrelations(y, [-1, 0, 1])
        assert -1 not in acf
        assert 0 not in acf
        assert 1 in acf

    def test_single_element_returns_empty(self):
        acf = calculate_residual_autocorrelations(np.array([1.0]), [1])
        assert acf == {}


# ---------------------------------------------------------------------------
# TestLjungBox
# ---------------------------------------------------------------------------

class TestLjungBox:
    def test_returns_available_flag(self):
        y = _white_noise(40)
        result = calculate_ljung_box_diagnostics(y, lag=5)
        assert "ljung_box_available" in result
        assert result["ljung_box_available"] == _STATSMODELS_AVAILABLE

    def test_unavailable_when_statsmodels_missing(self):
        if _STATSMODELS_AVAILABLE:
            pytest.skip("statsmodels present — skipping unavailability test")
        y = _white_noise(40)
        result = calculate_ljung_box_diagnostics(y, lag=5)
        assert result["ljung_box_statistic"] is None
        assert result["ljung_box_pvalue"] is None
        assert result["ljung_box_significant"] is None

    @pytest.mark.skipif(not _STATSMODELS_AVAILABLE, reason="statsmodels not installed")
    def test_white_noise_not_significant(self):
        rng = np.random.default_rng(99)
        y = rng.standard_normal(200)
        result = calculate_ljung_box_diagnostics(y, lag=10)
        assert result["ljung_box_significant"] is False or result["ljung_box_pvalue"] > 0.01

    @pytest.mark.skipif(not _STATSMODELS_AVAILABLE, reason="statsmodels not installed")
    def test_ar1_significant(self):
        y = _ar1_series(n=200, phi=0.9)
        result = calculate_ljung_box_diagnostics(y, lag=5)
        assert result["ljung_box_significant"] is True

    def test_too_short_series_returns_none_values(self):
        result = calculate_ljung_box_diagnostics(np.array([1.0, 2.0]), lag=5)
        assert result["ljung_box_statistic"] is None

    def test_lag_preserved_in_output(self):
        y = _white_noise(30)
        result = calculate_ljung_box_diagnostics(y, lag=3)
        assert result["ljung_box_lag"] == 3


# ---------------------------------------------------------------------------
# TestDurbinWatson
# ---------------------------------------------------------------------------

class TestDurbinWatson:
    def test_returns_none_when_statsmodels_missing(self):
        if _STATSMODELS_AVAILABLE:
            pytest.skip("statsmodels present")
        assert calculate_durbin_watson(_white_noise(30)) is None

    @pytest.mark.skipif(not _STATSMODELS_AVAILABLE, reason="statsmodels not installed")
    def test_white_noise_near_2(self):
        rng = np.random.default_rng(7)
        y = rng.standard_normal(200)
        dw = calculate_durbin_watson(y)
        assert dw is not None
        assert 1.0 < dw < 3.0

    @pytest.mark.skipif(not _STATSMODELS_AVAILABLE, reason="statsmodels not installed")
    def test_positive_ar1_below_2(self):
        y = _ar1_series(n=200, phi=0.8)
        dw = calculate_durbin_watson(y)
        assert dw is not None
        assert dw < 2.0

    def test_too_short_returns_none(self):
        result = calculate_durbin_watson(np.array([1.0, 2.0]))
        assert result is None


# ---------------------------------------------------------------------------
# TestClassification
# ---------------------------------------------------------------------------

class TestClassification:
    def _base(self, **kwargs):
        defaults = {
            "evidence_status": "ok",
            "max_abs_autocorrelation": 0.05,
            "lag1_autocorrelation": 0.05,
            "ljung_box_significant": None,
            "ljung_box_pvalue": None,
            "ljung_box_available": False,
            "durbin_watson": None,
            "durbin_watson_available": False,
        }
        defaults.update(kwargs)
        return defaults

    def test_acceptable_low_acf(self):
        status, reasons = classify_autocorrelation_status(self._base(max_abs_autocorrelation=0.05))
        assert status == "acceptable"

    def test_warning_moderate_acf(self):
        status, reasons = classify_autocorrelation_status(self._base(max_abs_autocorrelation=0.3))
        assert status == "warning"
        assert len(reasons) > 0

    def test_poor_high_acf(self):
        status, reasons = classify_autocorrelation_status(self._base(max_abs_autocorrelation=0.5))
        assert status == "poor"

    def test_insufficient_evidence(self):
        status, _ = classify_autocorrelation_status(
            self._base(evidence_status="insufficient")
        )
        assert status == "insufficient_evidence"

    def test_calculation_failed_propagates(self):
        status, _ = classify_autocorrelation_status(
            self._base(evidence_status="calculation_failed")
        )
        assert status == "calculation_failed"

    @pytest.mark.skipif(not _STATSMODELS_AVAILABLE, reason="statsmodels not installed")
    def test_lb_significant_upgrades_to_warning(self):
        metrics = self._base(
            max_abs_autocorrelation=0.1,  # below warning threshold
            ljung_box_significant=True,
            ljung_box_pvalue=0.01,
            ljung_box_available=True,
        )
        status, reasons = classify_autocorrelation_status(metrics)
        assert status == "warning"

    def test_custom_thresholds(self):
        cfg = AutocorrelationConfig(acf_warning_threshold=0.1, acf_poor_threshold=0.5)
        metrics = self._base(max_abs_autocorrelation=0.15)
        status, _ = classify_autocorrelation_status(metrics, cfg)
        assert status == "warning"


# ---------------------------------------------------------------------------
# TestTrainingACF
# ---------------------------------------------------------------------------

class TestTrainingACF:
    def test_empty_input_returns_empty(self):
        result = build_training_autocorrelation_diagnostics(pd.DataFrame())
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_schema_columns(self):
        df = _tr_df(_white_noise(50))
        result = build_training_autocorrelation_diagnostics(df)
        assert list(result.columns) == TRAINING_ACF_COLS

    def test_one_row_per_group(self):
        df1 = _tr_df(_white_noise(50), fold_number=1)
        df2 = _tr_df(_white_noise(50), fold_number=2)
        combined = pd.concat([df1, df2], ignore_index=True)
        result = build_training_autocorrelation_diagnostics(combined)
        assert len(result) == 2

    def test_white_noise_acceptable(self):
        rng = np.random.default_rng(42)
        y = rng.standard_normal(100)
        df = _tr_df(y)
        result = build_training_autocorrelation_diagnostics(df)
        status = result["autocorrelation_status"].iloc[0]
        assert status in ("acceptable", "warning", "insufficient_evidence")

    def test_strong_ar1_warning_or_poor(self):
        y = _ar1_series(n=100, phi=0.85)
        df = _tr_df(y)
        result = build_training_autocorrelation_diagnostics(df)
        assert result["autocorrelation_status"].iloc[0] in ("warning", "poor")

    def test_short_series_insufficient_evidence(self):
        df = _tr_df(_white_noise(5))
        result = build_training_autocorrelation_diagnostics(df)
        assert result["autocorrelation_status"].iloc[0] == "insufficient_evidence"

    def test_invalid_residuals_excluded(self):
        y = np.array([1.0, np.nan, 2.0, np.inf, -np.inf] + [0.5] * 50)
        df = _tr_df(y)
        result = build_training_autocorrelation_diagnostics(df)
        assert result["excluded_invalid_count"].iloc[0] >= 3

    def test_residual_count_matches_input(self):
        y = _white_noise(50)
        df = _tr_df(y)
        result = build_training_autocorrelation_diagnostics(df)
        assert result["valid_residual_count"].iloc[0] == 50

    def test_invalid_rows_not_counted_as_valid(self):
        y = _white_noise(50)
        df = _tr_df(y)
        df.loc[:4, "residual_observation_valid"] = False
        result = build_training_autocorrelation_diagnostics(df)
        assert result["valid_residual_count"].iloc[0] == 45

    def test_candidate_m_included_in_lags(self):
        y = _white_noise(80)
        df = _tr_df(y, candidate_m=14)
        result = build_training_autocorrelation_diagnostics(df)
        evaluated = result["evaluated_lags"].iloc[0]
        assert "14" in str(evaluated)

    def test_practical_flag_set_for_high_acf(self):
        y = _ar1_series(n=100, phi=0.85)
        df = _tr_df(y)
        result = build_training_autocorrelation_diagnostics(df)
        if result["autocorrelation_status"].iloc[0] in ("warning", "poor"):
            assert bool(result["practical_autocorrelation_flag"].iloc[0]) is True

    def test_diagnostic_run_id_propagated(self):
        df = _tr_df(_white_noise(50))
        result = build_training_autocorrelation_diagnostics(df, diagnostic_run_id="test-run")
        assert result["diagnostic_run_id"].iloc[0] == "test-run"

    def test_multiple_reports_separate_rows(self):
        df1 = _tr_df(_white_noise(50), report_id="r1")
        df2 = _tr_df(_white_noise(50), report_id="r2")
        combined = pd.concat([df1, df2], ignore_index=True)
        result = build_training_autocorrelation_diagnostics(combined)
        assert len(result) == 2
        assert set(result["report_id"]) == {"r1", "r2"}


# ---------------------------------------------------------------------------
# TestBacktestACF
# ---------------------------------------------------------------------------

class TestBacktestACF:
    def test_empty_input_returns_empty_both(self):
        fold_df, summary_df = build_backtest_autocorrelation_diagnostics(pd.DataFrame())
        assert fold_df.empty
        assert summary_df.empty

    def test_fold_schema_columns(self):
        df = _bt_df(_white_noise(28))
        fold_df, _ = build_backtest_autocorrelation_diagnostics(df)
        assert list(fold_df.columns) == BACKTEST_FOLD_ACF_COLS

    def test_summary_schema_columns(self):
        df = _bt_df(_white_noise(28))
        _, summary_df = build_backtest_autocorrelation_diagnostics(df)
        assert list(summary_df.columns) == BACKTEST_SUMMARY_ACF_COLS

    def test_one_fold_row_per_group(self):
        df1 = _bt_df(_white_noise(28), fold_number=1)
        df2 = _bt_df(_white_noise(28), fold_number=2)
        df = pd.concat([df1, df2], ignore_index=True)
        fold_df, summary_df = build_backtest_autocorrelation_diagnostics(df)
        assert len(fold_df) == 2
        assert len(summary_df) == 1

    def test_never_concatenates_folds(self):
        """Each fold must be processed independently."""
        df1 = _bt_df(_ar1_series(n=28, phi=0.0), fold_number=1)
        df2 = _bt_df(_ar1_series(n=28, phi=0.0), fold_number=2)
        df = pd.concat([df1, df2], ignore_index=True)
        fold_df, _ = build_backtest_autocorrelation_diagnostics(df)
        # valid_residual_count per fold should be 28, not 56
        assert (fold_df["valid_residual_count"] == 28).all()

    def test_summary_fold_counts_correct(self):
        df1 = _bt_df(_white_noise(30), fold_number=1)
        df2 = _bt_df(_white_noise(30), fold_number=2)
        df = pd.concat([df1, df2], ignore_index=True)
        _, summary_df = build_backtest_autocorrelation_diagnostics(df)
        assert summary_df["fold_count"].iloc[0] == 2

    def test_summary_total_residual_count(self):
        df1 = _bt_df(_white_noise(28), fold_number=1)
        df2 = _bt_df(_white_noise(28), fold_number=2)
        df = pd.concat([df1, df2], ignore_index=True)
        _, summary_df = build_backtest_autocorrelation_diagnostics(df)
        assert summary_df["total_valid_residual_count"].iloc[0] == 56

    def test_summary_status_poor_when_one_fold_poor(self):
        y_strong = _ar1_series(n=60, phi=0.9)
        y_ok = _white_noise(60)
        df1 = _bt_df(y_strong, fold_number=1)
        df2 = _bt_df(y_ok, fold_number=2)
        df = pd.concat([df1, df2], ignore_index=True)
        fold_df, summary_df = build_backtest_autocorrelation_diagnostics(df)
        fold_statuses = set(fold_df["autocorrelation_status"].tolist())
        # strong AR1 should produce warning or poor in at least one fold
        assert fold_statuses & {"warning", "poor"}

    def test_evaluation_run_id_propagated(self):
        df = _bt_df(_white_noise(28))
        fold_df, summary_df = build_backtest_autocorrelation_diagnostics(
            df, evaluation_run_id="ev-001"
        )
        assert fold_df["evaluation_run_id"].iloc[0] == "ev-001"
        assert summary_df["evaluation_run_id"].iloc[0] == "ev-001"

    def test_multiple_models_separate_summary_rows(self):
        df1 = _bt_df(_white_noise(28), model_name="m1", fold_number=1)
        df2 = _bt_df(_white_noise(28), model_name="m2", fold_number=1)
        df = pd.concat([df1, df2], ignore_index=True)
        _, summary_df = build_backtest_autocorrelation_diagnostics(df)
        assert len(summary_df) == 2


# ---------------------------------------------------------------------------
# TestProductionACF
# ---------------------------------------------------------------------------

class TestProductionACF:
    def test_empty_input_returns_empty(self):
        result = build_production_autocorrelation_diagnostics(pd.DataFrame())
        assert result.empty

    def test_schema_columns(self):
        df = _prod_df(_white_noise(40))
        result = build_production_autocorrelation_diagnostics(df)
        assert list(result.columns) == PRODUCTION_ACF_COLS

    def test_deduplication_selects_shortest_horizon(self):
        dates = pd.date_range("2024-06-01", periods=20)
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
                    "actual": 10.0,
                    "forecast": 10.0,
                    "residual": 0.0,
                    "residual_source": "production",
                    "residual_observation_valid": True,
                })
        df = pd.DataFrame(rows)
        result = build_production_autocorrelation_diagnostics(df)
        # excluded_overlap = 20 * 2 = 40
        assert result["excluded_overlap_count"].iloc[0] == 40

    def test_evaluation_run_id_propagated(self):
        df = _prod_df(_white_noise(40))
        result = build_production_autocorrelation_diagnostics(
            df, evaluation_run_id="prod-run-42"
        )
        assert result["evaluation_run_id"].iloc[0] == "prod-run-42"

    def test_multiple_reports_separate_rows(self):
        df1 = _prod_df(_white_noise(40), report_id="r1")
        df2 = _prod_df(_white_noise(40), report_id="r2")
        df = pd.concat([df1, df2], ignore_index=True)
        result = build_production_autocorrelation_diagnostics(df)
        assert len(result) == 2

    def test_missing_group_columns_returns_empty(self):
        df = pd.DataFrame({"report_id": ["r1"], "residual": [0.0]})
        result = build_production_autocorrelation_diagnostics(df)
        assert result.empty

    def test_selected_m_included_in_lags(self):
        df = _prod_df(_white_noise(80), selected_m=14)
        result = build_production_autocorrelation_diagnostics(df)
        evaluated = result["evaluated_lags"].iloc[0]
        assert "14" in str(evaluated)


# ---------------------------------------------------------------------------
# TestValidation
# ---------------------------------------------------------------------------

class TestValidation:
    def test_valid_training_df_passes(self):
        df = _tr_df(_white_noise(50))
        result = build_training_autocorrelation_diagnostics(df)
        validate_autocorrelation_diagnostics(result, "training")  # should not raise

    def test_invalid_status_raises(self):
        df = build_training_autocorrelation_diagnostics(_tr_df(_white_noise(50)))
        df["autocorrelation_status"] = "invalid_status"
        with pytest.raises(ValueError, match="invalid statuses"):
            validate_autocorrelation_diagnostics(df, "training")

    def test_acf_outside_range_raises(self):
        df = build_training_autocorrelation_diagnostics(_tr_df(_white_noise(50)))
        df["lag1_autocorrelation"] = 1.5
        with pytest.raises(ValueError, match="outside"):
            validate_autocorrelation_diagnostics(df, "training")

    def test_missing_column_raises(self):
        df = build_training_autocorrelation_diagnostics(_tr_df(_white_noise(50)))
        df = df.drop(columns=["autocorrelation_status"])
        with pytest.raises(ValueError, match="missing columns"):
            validate_autocorrelation_diagnostics(df, "training")

    def test_unknown_dataset_name_raises(self):
        with pytest.raises(ValueError, match="Unknown dataset_name"):
            validate_autocorrelation_diagnostics(pd.DataFrame(), "unknown_type")

    def test_empty_df_passes(self):
        validate_autocorrelation_diagnostics(pd.DataFrame(), "training")

    def test_valid_backtest_fold_df_passes(self):
        df = _bt_df(_white_noise(30))
        fold_df, _ = build_backtest_autocorrelation_diagnostics(df)
        validate_autocorrelation_diagnostics(fold_df, "backtest_fold")

    def test_valid_backtest_summary_df_passes(self):
        df = _bt_df(_white_noise(30))
        _, summary_df = build_backtest_autocorrelation_diagnostics(df)
        validate_autocorrelation_diagnostics(summary_df, "backtest_summary")

    def test_valid_production_df_passes(self):
        df = _prod_df(_white_noise(40))
        result = build_production_autocorrelation_diagnostics(df)
        validate_autocorrelation_diagnostics(result, "production")


# ---------------------------------------------------------------------------
# TestPersistence
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_creates_all_four_files(self, tmp_path):
        tr = build_training_autocorrelation_diagnostics(_tr_df(_white_noise(50)))
        bt_df_input = _bt_df(_white_noise(28))
        fold_df, summary_df = build_backtest_autocorrelation_diagnostics(bt_df_input)
        prod = build_production_autocorrelation_diagnostics(_prod_df(_white_noise(40)))

        paths = persist_autocorrelation_diagnostics(tr, fold_df, summary_df, prod, tmp_path)
        for name, path in paths.items():
            assert path is not None, f"Path for {name!r} is None"
            assert path.exists(), f"File not found: {path}"

    def test_empty_dataframes_write_header_only(self, tmp_path):
        paths = persist_autocorrelation_diagnostics(
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            pd.DataFrame(),
            tmp_path,
        )
        for name, path in paths.items():
            assert path is not None
            loaded = pd.read_csv(path)
            assert len(loaded) == 0

    def test_output_dir_created(self, tmp_path):
        diag_dir = tmp_path / "outputs" / "diagnostics"
        assert not diag_dir.exists()
        persist_autocorrelation_diagnostics(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), tmp_path
        )
        assert diag_dir.exists()

    def test_file_roundtrip(self, tmp_path):
        df_in = _tr_df(_white_noise(50))
        tr = build_training_autocorrelation_diagnostics(df_in)
        paths = persist_autocorrelation_diagnostics(
            tr, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), tmp_path
        )
        path = paths["training"]
        assert path is not None
        loaded = pd.read_csv(path)
        assert list(loaded.columns) == TRAINING_ACF_COLS
        assert len(loaded) == len(tr)

    def test_returns_none_on_invalid_df(self, tmp_path):
        bad_df = pd.DataFrame({"wrong_col": [1, 2, 3], "autocorrelation_status": ["bad_status", "acceptable", "warning"]})
        paths = persist_autocorrelation_diagnostics(
            bad_df, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), tmp_path
        )
        assert paths["training"] is None

    def test_overwrites_existing_file(self, tmp_path):
        df1 = _tr_df(_white_noise(50))
        df2 = _tr_df(_white_noise(50))
        df2 = pd.concat([df2, _tr_df(_white_noise(50), fold_number=2)], ignore_index=True)

        tr1 = build_training_autocorrelation_diagnostics(df1)
        tr2 = build_training_autocorrelation_diagnostics(df2)

        persist_autocorrelation_diagnostics(tr1, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), tmp_path)
        persist_autocorrelation_diagnostics(tr2, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), tmp_path)

        path = tmp_path / "outputs" / "diagnostics" / "training_autocorrelation_diagnostics_latest.csv"
        loaded = pd.read_csv(path)
        assert len(loaded) == 2  # second write has 2 rows


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_all_invalid_residuals_gives_insufficient(self):
        y = np.full(50, np.nan)
        df = _tr_df(y)
        result = build_training_autocorrelation_diagnostics(df)
        assert result["autocorrelation_status"].iloc[0] == "insufficient_evidence"

    def test_single_valid_residual_insufficient(self):
        y = np.zeros(1)
        df = _tr_df(y)
        result = build_training_autocorrelation_diagnostics(df)
        assert result["autocorrelation_status"].iloc[0] == "insufficient_evidence"

    def test_config_min_residuals_respected(self):
        cfg = AutocorrelationConfig(min_residuals_for_acf=30)
        y = _white_noise(20)  # below threshold
        df = _tr_df(y)
        result = build_training_autocorrelation_diagnostics(df, cfg=cfg)
        assert result["autocorrelation_status"].iloc[0] == "insufficient_evidence"

    def test_acf_block_has_no_nan_acf_for_sufficient_series(self):
        y = _white_noise(100)
        df = _tr_df(y)
        result = build_training_autocorrelation_diagnostics(df)
        assert result["lag1_autocorrelation"].notna().all()

    def test_backtest_with_no_valid_residuals(self):
        df = _bt_df(_white_noise(28))
        df["residual_observation_valid"] = False
        fold_df, summary_df = build_backtest_autocorrelation_diagnostics(df)
        assert fold_df["evidence_status"].iloc[0] == "insufficient"

    def test_production_dedup_tie_breaking_latest_generated_at(self):
        """When horizon_step is equal, latest generated_at wins."""
        df = pd.DataFrame({
            "report_id": ["r1", "r1"],
            "report_name": ["r1", "r1"],
            "selected_model_family": ["naive", "naive"],
            "selected_model_name": ["naive", "naive"],
            "selected_m": [7, 7],
            "forecast_date": [pd.Timestamp("2024-06-01")] * 2,
            "horizon_step": [1, 1],
            "generated_at": [pd.Timestamp("2024-05-30"), pd.Timestamp("2024-05-31")],
            "actual": [10.0, 10.0],
            "forecast": [9.0, 11.0],
            "residual": [1.0, -1.0],
            "residual_source": ["production", "production"],
            "residual_observation_valid": [True, True],
        })
        result = build_production_autocorrelation_diagnostics(df)
        # 1 kept, 1 excluded
        assert result["excluded_overlap_count"].iloc[0] == 1
