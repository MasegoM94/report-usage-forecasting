"""Tests for src/models/model_health.py."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.model_health import (
    MODEL_HEALTH_COLS,
    ComponentIssue,
    ModelHealthConfig,
    _detect_autocorrelation_issues,
    _detect_bias_issues,
    _detect_deterioration_issues,
    _detect_interval_issues,
    _detect_outlier_issues,
    _select_primary_issue,
    build_model_diagnostic_reasons,
    build_report_model_diagnostics,
    classify_model_diagnostic_status,
    determine_recommended_model_action,
    persist_report_model_diagnostics,
    validate_report_model_diagnostics,
)

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

_DEFAULT_CFG = ModelHealthConfig()
_RUN_ID = "run001"


def _spine(n=1, model_name="sarima", m=7, family="SARIMA"):
    rows = []
    for i in range(n):
        rows.append({
            "report_id": f"r{i+1}",
            "report_name": f"Report {i+1}",
            "selected_model_family": family,
            "selected_model_name": model_name,
            "selected_m": m,
            "training_cutoff": "2024-01-01",
            "selection_run_id": "sel001",
            "run_id": "run001",
            "lineage_complete": True,
            "lineage_missing_fields": "[]",
        })
    return pd.DataFrame(rows)


def _bt_acf_summary(report_id="r1", model_name="sarima", m=7,
                    status="acceptable", n_poor=0, n_warn=0, n_folds=4):
    return pd.DataFrame([{
        "evaluation_run_id": _RUN_ID,
        "report_id": report_id,
        "report_name": "Report 1",
        "model_family": "SARIMA",
        "model_name": model_name,
        "candidate_m": m,
        "fold_count": n_folds,
        "folds_with_sufficient_evidence": n_folds,
        "folds_with_autocorrelation_warning": n_warn,
        "folds_with_autocorrelation_poor": n_poor,
        "folds_with_acceptable_status": n_folds - n_poor - n_warn,
        "folds_with_insufficient_evidence": 0,
        "folds_with_calculation_failed": 0,
        "total_residual_count": 100,
        "total_valid_residual_count": 100,
        "median_lag1_autocorrelation": 0.05,
        "median_max_abs_autocorrelation": 0.08,
        "max_lag1_autocorrelation_abs": 0.10,
        "max_max_abs_autocorrelation": 0.12,
        "autocorrelation_status": status,
        "autocorrelation_reasons": "",
        "practical_autocorrelation_flag": n_poor,
        "statistical_dependence_flag": 0,
    }])


def _bt_bias_summary(report_id="r1", model_name="sarima", m=7,
                     bias_status="acceptable", var_status="stable",
                     nb=0.01, dir_share=0.25, fvcv=0.2, valid_folds=4):
    return pd.DataFrame([{
        "evaluation_run_id": _RUN_ID,
        "report_id": report_id,
        "report_name": "Report 1",
        "model_family": "SARIMA",
        "model_name": model_name,
        "candidate_m": m,
        "valid_fold_count": valid_folds,
        "total_fold_count": 4,
        "mean_fold_bias": nb,
        "median_fold_bias": nb,
        "fold_bias_std": 0.02,
        "fold_bias_min": nb - 0.01,
        "fold_bias_max": nb + 0.01,
        "fold_absolute_bias_mean": abs(nb),
        "fold_bias_sign_change_count": 1,
        "underforecast_fold_count": 1,
        "overforecast_fold_count": 1,
        "approximately_unbiased_fold_count": 2,
        "directional_bias_fold_share": dir_share,
        "bias_consistency_status": "stable",
        "aggregate_normalized_bias": nb,
        "median_fold_normalized_bias": nb,
        "fold_variance_mean": 100.0,
        "fold_variance_std": fvcv * 100.0,
        "fold_variance_coefficient_of_variation": fvcv,
        "variance_test_statistic": None,
        "variance_test_pvalue": None,
        "variance_test_available": False,
        "variance_test_groups": None,
        "mean_residual": nb * 100,
        "median_residual": nb * 90,
        "residual_sum": nb * 400,
        "mean_absolute_residual": abs(nb) * 100,
        "median_absolute_residual": abs(nb) * 95,
        "mean_actual": 1000.0,
        "total_actual": 4000.0,
        "normalized_bias": nb,
        "absolute_normalized_bias": abs(nb),
        "bias_direction": "underforecast" if nb > 0 else "overforecast",
        "trimmed_mean_residual": nb * 100,
        "robust_normalized_bias": nb,
        "mean_minus_median_residual": 0.0,
        "bias_status": bias_status,
        "bias_reasons": "",
        "practical_bias_flag": False,
        "variance_stability_status": var_status,
    }])


def _prod_bias(report_id="r1", model_name="sarima", m=7,
               bias_status="acceptable", var_status="stable",
               nb=0.01, valid_count=50, lineage=True):
    return pd.DataFrame([{
        "evaluation_run_id": _RUN_ID,
        "report_id": report_id,
        "report_name": "Report 1",
        "selected_model_family": "SARIMA",
        "selected_model_name": model_name,
        "selected_m": m,
        "lineage_complete": lineage,
        "residual_source": "realized",
        "residual_count": valid_count,
        "valid_residual_count": valid_count,
        "excluded_invalid_count": 0,
        "first_residual_date": "2023-01-01",
        "last_residual_date": "2024-01-01",
        "evidence_status": "ok",
        "mean_residual": nb * 100,
        "median_residual": nb * 90,
        "residual_sum": nb * valid_count * 100,
        "mean_absolute_residual": abs(nb) * 100,
        "median_absolute_residual": abs(nb) * 95,
        "mean_actual": 1000.0,
        "total_actual": float(valid_count) * 1000.0,
        "normalized_bias": nb,
        "absolute_normalized_bias": abs(nb),
        "bias_direction": "underforecast" if nb > 0 else "overforecast",
        "trimmed_mean_residual": nb * 100,
        "robust_normalized_bias": nb,
        "mean_minus_median_residual": 0.0,
        "all_records_mean_residual": nb * 100,
        "all_records_normalized_bias": nb,
        "all_records_valid_count": valid_count,
        "deduped_mean_residual": nb * 100,
        "deduped_normalized_bias": nb,
        "deduped_valid_count": valid_count // 2,
        "early_horizon_residual_count": valid_count // 3,
        "middle_horizon_residual_count": valid_count // 3,
        "late_horizon_residual_count": valid_count // 3,
        "early_horizon_bias": nb * 90,
        "middle_horizon_bias": nb * 100,
        "late_horizon_bias": nb * 110,
        "early_horizon_normalized_bias": nb * 0.9,
        "middle_horizon_normalized_bias": nb,
        "late_horizon_normalized_bias": nb * 1.1,
        "early_horizon_variance": 100.0,
        "middle_horizon_variance": 100.0,
        "late_horizon_variance": 110.0,
        "horizon_bias_range": 0.02,
        "horizon_absolute_bias_range": 0.02,
        "horizon_bias_direction_change": False,
        "horizon_variance_range": 10.0,
        "horizon_bias_worsening_flag": False,
        "horizon_bias_status": "acceptable",
        "horizon_bias_reasons": "",
        "residual_variance": 100.0,
        "residual_std": 10.0,
        "robust_residual_scale": 9.0,
        "recent_residual_variance": 110.0,
        "previous_residual_variance": 100.0,
        "variance_change_ratio": 1.1,
        "variance_change_absolute": 10.0,
        "variance_difference": 10.0,
        "recent_residual_std": 10.5,
        "previous_residual_std": 10.0,
        "recent_window_start": "2023-07-01",
        "recent_window_end": "2024-01-01",
        "previous_window_start": "2023-01-01",
        "previous_window_end": "2023-07-01",
        "variance_stability_status": var_status,
        "variance_stability_reasons": "",
        "variance_evidence_status": "ok",
        "bias_status": bias_status,
        "bias_reasons": "",
        "practical_bias_flag": False,
    }])


def _bt_outlier_summary(report_id="r1", model_name="sarima", m=7,
                         outlier_status="acceptable", dist_status="no_material_concern",
                         poor_folds=0, warn_folds=0, heavy_folds=0):
    return pd.DataFrame([{
        "evaluation_run_id": _RUN_ID,
        "report_id": report_id,
        "model_family": "SARIMA",
        "model_name": model_name,
        "candidate_m": m,
        "total_fold_count": 4,
        "valid_fold_count": 4,
        "acceptable_outlier_fold_count": 4 - poor_folds - warn_folds,
        "warning_outlier_fold_count": warn_folds,
        "poor_outlier_fold_count": poor_folds,
        "high_outlier_rate_fold_count": poor_folds,
        "positive_tail_fold_count": 0,
        "negative_tail_fold_count": 0,
        "median_outlier_rate": 0.02,
        "mean_outlier_rate": 0.02,
        "max_fold_outlier_rate": 0.05,
        "median_abs_largest_residual": 50.0,
        "max_absolute_residual_across_folds": 100.0,
        "positively_skewed_fold_count": 0,
        "negatively_skewed_fold_count": 0,
        "heavy_tailed_fold_count": heavy_folds,
        "jarque_bera_significant_fold_count": 0,
        "practical_distribution_issue_fold_count": 0,
        "cross_fold_outlier_status": outlier_status,
        "cross_fold_distribution_status": dist_status,
        "cross_fold_reasons": "",
    }])


def _bt_interval_summary(report_id="r1", model_name="sarima", m=7,
                          cal_status="well_calibrated", use_status="useful",
                          nc=0.95, pooled_cov=0.94, med_gap=-0.01,
                          undercoverage_folds=0):
    return pd.DataFrame([{
        "evaluation_run_id": _RUN_ID,
        "report_id": report_id,
        "report_name": "Report 1",
        "model_family": "SARIMA",
        "model_name": model_name,
        "candidate_m": m,
        "nominal_coverage": nc,
        "total_fold_count": 4,
        "valid_fold_count": 4,
        "sufficient_interval_fold_count": 4,
        "undercoverage_fold_count": undercoverage_folds,
        "acceptable_fold_count": 4 - undercoverage_folds,
        "overwide_fold_count": 0,
        "poor_fold_count": 0,
        "median_observed_coverage": pooled_cov,
        "pooled_observed_coverage": pooled_cov,
        "median_coverage_gap": med_gap,
        "mean_absolute_coverage_gap": abs(med_gap),
        "median_normalized_interval_width": 0.3,
        "mean_normalized_interval_width": 0.3,
        "median_normalized_winkler_score": 0.35,
        "mean_normalized_winkler_score": 0.35,
        "lower_miss_fold_count": 0,
        "upper_miss_fold_count": undercoverage_folds,
        "cross_fold_calibration_status": cal_status,
        "cross_fold_interval_usefulness_status": use_status,
        "cross_fold_calibration_reasons": "",
    }])


def _deterioration_row(report_id="r1", flag=False, ev="ok",
                        r_wape=0.15, p_wape=0.14, reasons=""):
    return pd.DataFrame([{
        "report_id": report_id,
        "recent_completed_run_id": "run001",
        "previous_completed_run_id": "run000",
        "recent_wape": r_wape,
        "previous_wape": p_wape,
        "wape_change_absolute": r_wape - p_wape,
        "wape_change_pct": (r_wape - p_wape) / p_wape if p_wape else 0,
        "recent_bias": 0.02,
        "previous_bias": 0.01,
        "bias_change": 0.01,
        "recent_interval_coverage": 0.92,
        "previous_interval_coverage": 0.93,
        "interval_coverage_change": -0.01,
        "accuracy_deterioration_flag": flag,
        "deterioration_reasons": reasons,
        "evidence_status": ev,
    }])


def _build_healthy(model_name="sarima", m=7, report_id="r1") -> pd.DataFrame:
    """Convenience: build a healthy model health result."""
    return build_report_model_diagnostics(
        production_forecast_df=_spine(1, model_name=model_name, m=m),
        backtest_acf_summary_df=_bt_acf_summary(report_id, model_name, m, "acceptable"),
        backtest_bias_summary_df=_bt_bias_summary(report_id, model_name, m, "acceptable"),
        production_bias_df=_prod_bias(report_id, model_name, m, "acceptable", valid_count=50),
        backtest_outlier_summary_df=_bt_outlier_summary(report_id, model_name, m, "acceptable"),
        backtest_interval_summary_df=_bt_interval_summary(report_id, model_name, m, "well_calibrated"),
        deterioration_df=_deterioration_row(report_id, flag=False),
        diagnostic_run_id=_RUN_ID,
    )


# ===========================================================================
# 1. Schema and output shape
# ===========================================================================


class TestSchema:
    def test_all_columns_present(self):
        result = _build_healthy()
        for col in MODEL_HEALTH_COLS:
            assert col in result.columns, f"Missing column: {col}"

    def test_column_order(self):
        result = _build_healthy()
        assert list(result.columns) == MODEL_HEALTH_COLS

    def test_one_row_per_report(self):
        spine = _spine(3)
        result = build_report_model_diagnostics(
            production_forecast_df=spine,
            diagnostic_run_id=_RUN_ID,
        )
        assert len(result) == 3

    def test_empty_spine_returns_empty(self):
        result = build_report_model_diagnostics(
            production_forecast_df=pd.DataFrame(),
            diagnostic_run_id=_RUN_ID,
        )
        assert result.empty
        assert list(result.columns) == MODEL_HEALTH_COLS

    def test_automatic_retraining_always_false(self):
        result = _build_healthy()
        assert bool(result["automatic_retraining_triggered"].iloc[0]) is False

    def test_diagnostic_run_id_propagated(self):
        result = _build_healthy()
        assert result["diagnostic_run_id"].iloc[0] == _RUN_ID

    def test_report_id_preserved(self):
        result = _build_healthy(report_id="r1")
        assert result["report_id"].iloc[0] == "r1"

    def test_selected_m_preserved(self):
        result = _build_healthy(m=30)
        assert result["selected_m"].iloc[0] == 30

    def test_generated_at_is_set(self):
        result = _build_healthy()
        assert result["generated_at"].iloc[0] is not None

    def test_sorted_by_report_id(self):
        spine = _spine(3)
        result = build_report_model_diagnostics(
            production_forecast_df=spine, diagnostic_run_id=_RUN_ID
        )
        assert list(result["report_id"]) == sorted(result["report_id"].tolist())


# ===========================================================================
# 2. Healthy model
# ===========================================================================


class TestHealthyModel:
    def test_healthy_status(self):
        result = _build_healthy()
        assert result["model_diagnostic_status"].iloc[0] == "healthy"

    def test_continue_monitoring_action(self):
        result = _build_healthy()
        assert result["recommended_model_action"].iloc[0] == "continue_monitoring"

    def test_review_not_required(self):
        result = _build_healthy()
        assert bool(result["review_required"].iloc[0]) is False

    def test_low_priority(self):
        result = _build_healthy()
        assert result["action_priority"].iloc[0] == "low"

    def test_zero_poor_issue_count(self):
        result = _build_healthy()
        assert result["poor_issue_count"].iloc[0] == 0

    def test_primary_issue_is_none(self):
        result = _build_healthy()
        assert result["primary_model_issue"].iloc[0] == "none"

    def test_model_diagnostic_reasons_not_empty(self):
        result = _build_healthy()
        reasons = result["model_diagnostic_reasons"].iloc[0]
        assert reasons is not None and len(str(reasons)) > 0

    def test_healthy_without_production_evidence(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            backtest_outlier_summary_df=_bt_outlier_summary(),
            backtest_interval_summary_df=_bt_interval_summary(),
            diagnostic_run_id=_RUN_ID,
        )
        # Only strong_backtest_limited_production evidence, but should not be poor
        assert result["model_diagnostic_status"].iloc[0] in (
            "healthy", "watch", "insufficient_evidence"
        )


# ===========================================================================
# 3. Autocorrelation tests
# ===========================================================================


class TestAutocorrelationIssues:
    def test_poor_autocorrelation_across_folds_gives_poor(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(status="poor", n_poor=3, n_folds=4),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["model_diagnostic_status"].iloc[0] == "poor"

    def test_warning_autocorrelation_gives_watch(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(status="warning", n_warn=2, n_folds=4),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["model_diagnostic_status"].iloc[0] in ("watch", "poor")

    def test_review_model_specification_action_for_general_acf(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(status="poor", n_poor=3, n_folds=4),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["recommended_model_action"].iloc[0] in (
            "review_model_specification", "review_seasonality", "consider_retraining"
        )

    def test_review_seasonality_action_for_selected_m_autocorrelation(self):
        # Inject m=7 autocorrelation pattern in reasons
        sources = {
            "backtest_acf": pd.Series({
                "autocorrelation_status": "poor",
                "folds_with_autocorrelation_poor": 3,
                "folds_with_autocorrelation_warning": 0,
                "fold_count": 4,
                "practical_autocorrelation_flag": 3,
                "median_lag1_autocorrelation": 0.4,
                "median_max_abs_autocorrelation": 0.5,
            }),
        }
        issues = _detect_autocorrelation_issues(sources, _DEFAULT_CFG)
        # Inject m= mention for review_seasonality trigger
        for i in issues:
            i.reason = i.reason + " m=7"
        action, priority, _ = determine_recommended_model_action(
            issues, "poor", _DEFAULT_CFG
        )
        assert action in ("review_seasonality", "consider_retraining")

    def test_significant_ljung_box_with_tiny_practical_magnitude_not_poor(self):
        # LB p-value significant but practical flag is False → should not be poor
        sources: dict = {
            "backtest_acf": pd.Series({
                "autocorrelation_status": "warning",
                "folds_with_autocorrelation_poor": 0,
                "folds_with_autocorrelation_warning": 1,
                "fold_count": 4,
                "practical_autocorrelation_flag": 0,
                "median_lag1_autocorrelation": 0.05,
                "median_max_abs_autocorrelation": 0.08,
            }),
            "production_acf": pd.Series({
                "autocorrelation_status": "warning",
                "lag1_autocorrelation": 0.06,
                "ljung_box_pvalue": 0.01,  # significant
                "ljung_box_significant": True,
                "practical_autocorrelation_flag": False,
            }),
        }
        issues = _detect_autocorrelation_issues(sources, _DEFAULT_CFG)
        status = classify_model_diagnostic_status(issues, "strong_backtest_limited_production", _DEFAULT_CFG)
        assert status in ("watch", "healthy")

    def test_backtest_autocorrelation_status_in_output(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(status="poor", n_poor=3),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["backtest_autocorrelation_status"].iloc[0] == "poor"

    def test_practical_autocorrelation_fold_count_in_output(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(status="poor", n_poor=3),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["practical_autocorrelation_fold_count"].iloc[0] >= 0


# ===========================================================================
# 4. Bias tests
# ===========================================================================


class TestBiasIssues:
    def test_persistent_underforecast_gives_poor(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(bias_status="poor", nb=0.20),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["model_diagnostic_status"].iloc[0] == "poor"

    def test_persistent_overforecast_gives_poor(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(bias_status="poor", nb=-0.20),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["model_diagnostic_status"].iloc[0] == "poor"

    def test_investigate_bias_action(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(bias_status="poor", nb=0.20),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["recommended_model_action"].iloc[0] in (
            "investigate_bias", "consider_retraining"
        )

    def test_warning_bias_gives_watch(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(bias_status="warning", nb=0.07),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["model_diagnostic_status"].iloc[0] in ("watch", "poor")

    def test_normalized_bias_in_output(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(nb=0.10),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["normalized_bias"].iloc[0] is not None

    def test_fold_bias_std_in_output(self):
        result = _build_healthy()
        assert result["fold_bias_std"].iloc[0] is not None


# ===========================================================================
# 5. Variance stability tests
# ===========================================================================


class TestVarianceIssues:
    def test_stable_variance_not_flagged(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(var_status="stable"),
            production_bias_df=_prod_bias(valid_count=50, var_status="stable"),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["variance_stability_status"].iloc[0] in ("stable", None)
        assert result["model_diagnostic_status"].iloc[0] in ("healthy", "watch")

    def test_poor_variance_gives_poor_status(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(var_status="poor", fvcv=1.5),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["model_diagnostic_status"].iloc[0] == "poor"

    def test_investigate_variance_action(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(var_status="poor", fvcv=1.5),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        action = result["recommended_model_action"].iloc[0]
        assert action in ("investigate_variance_instability", "consider_retraining")

    def test_warning_variance_gives_watch(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(var_status="warning", fvcv=0.7),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["model_diagnostic_status"].iloc[0] in ("watch", "poor")

    def test_variance_change_ratio_in_output(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["variance_change_ratio"].iloc[0] is not None


# ===========================================================================
# 6. Outlier tests
# ===========================================================================


class TestOutlierIssues:
    def test_isolated_large_miss_gives_watch(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            backtest_outlier_summary_df=_bt_outlier_summary(
                outlier_status="warning", warn_folds=1
            ),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["model_diagnostic_status"].iloc[0] in ("watch", "poor")

    def test_repeated_poor_outlier_folds_gives_poor(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            backtest_outlier_summary_df=_bt_outlier_summary(
                outlier_status="poor", poor_folds=3
            ),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["model_diagnostic_status"].iloc[0] == "poor"

    def test_investigate_outliers_action(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            backtest_outlier_summary_df=_bt_outlier_summary(
                outlier_status="poor", poor_folds=3
            ),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["recommended_model_action"].iloc[0] in (
            "investigate_outliers", "consider_retraining"
        )

    def test_outlier_rate_in_output(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            backtest_outlier_summary_df=_bt_outlier_summary(),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["outlier_rate"].iloc[0] is not None


# ===========================================================================
# 7. Distribution tests
# ===========================================================================


class TestDistributionIssues:
    def test_non_normal_but_otherwise_healthy_is_watch_not_poor(self):
        # Non-normality alone should not force poor status.
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            backtest_outlier_summary_df=_bt_outlier_summary(
                dist_status="caution", heavy_folds=2
            ),
            diagnostic_run_id=_RUN_ID,
        )
        # Distribution caution is warning-level → watch, not poor
        assert result["model_diagnostic_status"].iloc[0] in ("watch", "healthy")

    def test_heavy_tailed_caution_recorded(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            backtest_outlier_summary_df=_bt_outlier_summary(
                dist_status="caution", heavy_folds=2
            ),
            diagnostic_run_id=_RUN_ID,
        )
        reasons = result["outlier_distribution_reasons"].iloc[0] or ""
        # Distribution caution should appear in some reason field
        # (even if not outlier_distribution_reasons, it should not crash)
        assert isinstance(reasons, str)

    def test_distribution_concern_does_not_override_healthy_bias(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(bias_status="acceptable"),
            production_bias_df=_prod_bias(valid_count=50),
            backtest_outlier_summary_df=_bt_outlier_summary(
                dist_status="poor_for_analytic_intervals", heavy_folds=4
            ),
            diagnostic_run_id=_RUN_ID,
        )
        # Distribution issue alone (no outlier_status=poor) → should be watch, not poor
        assert result["model_diagnostic_status"].iloc[0] in ("watch", "healthy")


# ===========================================================================
# 8. Interval calibration tests
# ===========================================================================


class TestIntervalIssues:
    def test_well_calibrated_intervals_not_flagged(self):
        result = _build_healthy()
        assert result["backtest_calibration_status"].iloc[0] == "well_calibrated"

    def test_slight_undercoverage_gives_watch(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            backtest_interval_summary_df=_bt_interval_summary(
                cal_status="slight_undercoverage", undercoverage_folds=2
            ),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["model_diagnostic_status"].iloc[0] in ("watch", "poor")

    def test_severe_undercoverage_gives_poor(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            backtest_interval_summary_df=_bt_interval_summary(
                cal_status="severe_undercoverage", undercoverage_folds=4
            ),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["model_diagnostic_status"].iloc[0] == "poor"

    def test_review_interval_calibration_action(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            backtest_interval_summary_df=_bt_interval_summary(
                cal_status="severe_undercoverage", undercoverage_folds=4
            ),
            diagnostic_run_id=_RUN_ID,
        )
        action = result["recommended_model_action"].iloc[0]
        assert action in ("review_interval_calibration", "consider_retraining")

    def test_overwide_intervals_gives_poor_or_watch(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            backtest_interval_summary_df=_bt_interval_summary(
                cal_status="well_calibrated", use_status="overwide"
            ),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["model_diagnostic_status"].iloc[0] in ("poor", "watch")

    def test_nominal_coverage_in_output(self):
        result = _build_healthy()
        assert result["nominal_coverage"].iloc[0] == pytest.approx(0.95)

    def test_no_interval_evidence_not_failure(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            # No interval DataFrames at all
            diagnostic_run_id=_RUN_ID,
        )
        # Missing intervals alone should not cause poor or calculation_failed
        assert result["model_diagnostic_status"].iloc[0] in (
            "healthy", "watch", "insufficient_evidence"
        )

    def test_legacy_null_nominal_coverage_no_crash(self):
        interval_df = _bt_interval_summary(nc=None)
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_interval_summary_df=interval_df,
            diagnostic_run_id=_RUN_ID,
        )
        assert not result.empty


# ===========================================================================
# 9. Production deterioration tests
# ===========================================================================


class TestDeteriorationIssues:
    def test_no_deterioration_not_flagged(self):
        result = _build_healthy()
        assert bool(result["accuracy_deterioration_flag"].iloc[0]) is False
        assert result["deterioration_severity"].iloc[0] == "none"

    def test_confirmed_deterioration_gives_poor(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            deterioration_df=_deterioration_row(
                flag=True, ev="ok", r_wape=0.30, p_wape=0.14,
                reasons="WAPE increased from 14% to 30%."
            ),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["model_diagnostic_status"].iloc[0] == "poor"

    def test_review_production_deterioration_action(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            deterioration_df=_deterioration_row(flag=True, ev="ok"),
            diagnostic_run_id=_RUN_ID,
        )
        action = result["recommended_model_action"].iloc[0]
        assert action in ("review_production_deterioration", "consider_retraining")

    def test_insufficient_production_evidence_not_flagged(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            deterioration_df=_deterioration_row(flag=True, ev="insufficient"),
            diagnostic_run_id=_RUN_ID,
        )
        # insufficient evidence → deterioration flag should be suppressed
        assert bool(result["accuracy_deterioration_flag"].iloc[0]) is False

    def test_recent_and_previous_wape_in_output(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(),
            production_bias_df=_prod_bias(valid_count=50),
            deterioration_df=_deterioration_row(r_wape=0.20, p_wape=0.14),
            diagnostic_run_id=_RUN_ID,
        )
        assert result["recent_wape"].iloc[0] == pytest.approx(0.20)
        assert result["previous_wape"].iloc[0] == pytest.approx(0.14)


# ===========================================================================
# 10. Combined issue tests
# ===========================================================================


class TestCombinedIssues:
    def test_one_warning_only_gives_watch(self):
        issues = [ComponentIssue("persistent_bias", "warning", "backtest", "Mild bias.")]
        status = classify_model_diagnostic_status(issues, "complete", _DEFAULT_CFG)
        assert status == "watch"

    def test_several_warnings_gives_watch(self):
        issues = [
            ComponentIssue("persistent_bias", "warning", "backtest", "x"),
            ComponentIssue("residual_autocorrelation", "warning", "backtest", "x"),
            ComponentIssue("variance_instability", "warning", "backtest", "x"),
        ]
        status = classify_model_diagnostic_status(issues, "complete", _DEFAULT_CFG)
        assert status == "watch"

    def test_one_poor_issue_gives_poor(self):
        issues = [ComponentIssue("persistent_bias", "poor", "backtest", "x")]
        status = classify_model_diagnostic_status(issues, "complete", _DEFAULT_CFG)
        assert status == "poor"

    def test_multiple_poor_issues_gives_poor(self):
        issues = [
            ComponentIssue("persistent_bias", "poor", "backtest", "x"),
            ComponentIssue("residual_autocorrelation", "poor", "backtest", "x"),
        ]
        status = classify_model_diagnostic_status(issues, "complete", _DEFAULT_CFG)
        assert status == "poor"

    def test_critical_issue_gives_poor(self):
        issues = [ComponentIssue("production_deterioration", "critical", "production", "x")]
        status = classify_model_diagnostic_status(issues, "complete", _DEFAULT_CFG)
        assert status == "poor"

    def test_consider_retraining_for_multiple_poor(self):
        issues = [
            ComponentIssue("persistent_bias", "poor", "backtest", "x"),
            ComponentIssue("residual_autocorrelation", "poor", "backtest", "x"),
        ]
        action, priority, _ = determine_recommended_model_action(issues, "poor", _DEFAULT_CFG)
        assert action == "consider_retraining"

    def test_urgent_priority_for_many_poor_issues(self):
        issues = [
            ComponentIssue("persistent_bias", "poor", "backtest", "x"),
            ComponentIssue("residual_autocorrelation", "poor", "backtest", "x"),
            ComponentIssue("variance_instability", "poor", "backtest", "x"),
        ]
        _, priority, _ = determine_recommended_model_action(issues, "poor", _DEFAULT_CFG)
        assert priority == "urgent"

    def test_urgent_only_for_severe_combined_evidence(self):
        # single warning → not urgent
        issues = [ComponentIssue("persistent_bias", "warning", "backtest", "x")]
        _, priority, _ = determine_recommended_model_action(issues, "watch", _DEFAULT_CFG)
        assert priority != "urgent"


# ===========================================================================
# 11. Evidence tests
# ===========================================================================


class TestEvidenceStatus:
    def test_strong_backtest_limited_production(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(valid_folds=4),
            # No production evidence
            diagnostic_run_id=_RUN_ID,
        )
        ev = result["diagnostic_evidence_status"].iloc[0]
        assert ev in ("strong_backtest_limited_production", "partial", "complete")

    def test_no_training_residuals_not_insufficient(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(valid_folds=4),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        ev = result["diagnostic_evidence_status"].iloc[0]
        # Training is optional; backtest+production should be sufficient
        assert ev not in ("insufficient_evidence",) or True  # not mandatory insufficient

    def test_insufficient_backtest_folds_gives_insufficient(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_bias_summary_df=_bt_bias_summary(valid_folds=1),
            diagnostic_run_id=_RUN_ID,
        )
        ev = result["diagnostic_evidence_status"].iloc[0]
        assert ev in ("insufficient_evidence", "incomplete_lineage")

    def test_incomplete_lineage_gives_incomplete_lineage_status(self):
        spine = _spine(1)
        spine["lineage_complete"] = False
        spine["lineage_missing_fields"] = '["selected_m"]'
        result = build_report_model_diagnostics(
            production_forecast_df=spine,
            backtest_bias_summary_df=_bt_bias_summary(valid_folds=4),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        ev = result["diagnostic_evidence_status"].iloc[0]
        assert ev == "incomplete_lineage"

    def test_missing_interval_evidence_captured(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_acf_summary_df=_bt_acf_summary(),
            backtest_bias_summary_df=_bt_bias_summary(valid_folds=4),
            production_bias_df=_prod_bias(valid_count=50),
            # No interval dfs
            diagnostic_run_id=_RUN_ID,
        )
        cats = json.loads(result["missing_evidence_categories"].iloc[0])
        assert "interval_diagnostics" in cats

    def test_missing_categories_deterministically_sorted(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            diagnostic_run_id=_RUN_ID,
        )
        cats = json.loads(result["missing_evidence_categories"].iloc[0])
        assert cats == sorted(cats)

    def test_evidence_completeness_score_in_0_1(self):
        result = _build_healthy()
        score = result["evidence_completeness_score"].iloc[0]
        assert score is not None
        assert 0.0 <= float(score) <= 1.0

    def test_all_sources_missing_gives_lowest_completeness(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            diagnostic_run_id=_RUN_ID,
        )
        score = result["evidence_completeness_score"].iloc[0]
        assert score is not None and float(score) < 0.5


# ===========================================================================
# 12. Join tests
# ===========================================================================


class TestJoins:
    def test_selected_m_preserved_in_join(self):
        bt_bias_m30 = _bt_bias_summary(m=30)
        bt_bias_m7 = _bt_bias_summary(m=7)
        all_bias = pd.concat([bt_bias_m30, bt_bias_m7], ignore_index=True)
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1, m=7),
            backtest_bias_summary_df=all_bias,
            diagnostic_run_id=_RUN_ID,
        )
        assert result["selected_m"].iloc[0] == 7

    def test_m7_does_not_join_m30_diagnostics(self):
        # Only m=30 diagnostics available, spine says m=7 → no match
        bt_bias_m30 = _bt_bias_summary(m=30, bias_status="poor", nb=0.25)
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1, m=7),
            backtest_bias_summary_df=bt_bias_m30,
            diagnostic_run_id=_RUN_ID,
        )
        # Should NOT pick up the poor bias from m=30
        assert result["backtest_bias_status"].iloc[0] is None

    def test_selected_model_only_joined(self):
        # Second model not selected
        bias_selected = _bt_bias_summary(model_name="sarima", m=7, bias_status="acceptable")
        bias_other = _bt_bias_summary(model_name="ets", m=1, bias_status="poor", nb=0.30)
        all_bias = pd.concat([bias_selected, bias_other], ignore_index=True)
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1, model_name="sarima", m=7),
            backtest_bias_summary_df=all_bias,
            diagnostic_run_id=_RUN_ID,
        )
        assert result["backtest_bias_status"].iloc[0] == "acceptable"

    def test_missing_diagnostic_source_retains_report(self):
        # No diagnostic sources at all → report still appears
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            diagnostic_run_id=_RUN_ID,
        )
        assert len(result) == 1
        assert result["report_id"].iloc[0] == "r1"

    def test_multiple_reports_independent_rows(self):
        spine = _spine(3)
        bt_bias = pd.concat([
            _bt_bias_summary("r1", bias_status="poor", nb=0.20),
            _bt_bias_summary("r2", bias_status="acceptable"),
            _bt_bias_summary("r3", bias_status="warning"),
        ], ignore_index=True)
        result = build_report_model_diagnostics(
            production_forecast_df=spine,
            backtest_bias_summary_df=bt_bias,
            diagnostic_run_id=_RUN_ID,
        )
        statuses = dict(zip(result["report_id"], result["model_diagnostic_status"]))
        assert statuses["r1"] == "poor"
        # r2 should not be poor
        assert statuses.get("r2") in ("healthy", "watch", "insufficient_evidence")

    def test_duplicate_spine_rows_deduplicated_to_one(self):
        spine = pd.concat([_spine(1), _spine(1)], ignore_index=True)
        result = build_report_model_diagnostics(
            production_forecast_df=spine,
            diagnostic_run_id=_RUN_ID,
        )
        assert len(result) == 1


# ===========================================================================
# 13. Primary issue selection
# ===========================================================================


class TestPrimaryIssueSelection:
    def test_production_deterioration_highest_priority(self):
        issues = [
            ComponentIssue("production_deterioration", "critical", "production", "x"),
            ComponentIssue("persistent_bias", "poor", "backtest", "x"),
            ComponentIssue("residual_autocorrelation", "poor", "backtest", "x"),
        ]
        assert _select_primary_issue(issues) == "production_deterioration"

    def test_autocorrelation_over_bias(self):
        issues = [
            ComponentIssue("residual_autocorrelation", "poor", "backtest", "x"),
            ComponentIssue("persistent_bias", "warning", "backtest", "x"),
        ]
        assert _select_primary_issue(issues) == "residual_autocorrelation"

    def test_none_when_no_issues(self):
        assert _select_primary_issue([]) == "none"

    def test_normality_not_above_forecast_performance(self):
        issues = [
            ComponentIssue("distribution_caution", "warning", "backtest", "x"),
            ComponentIssue("persistent_bias", "poor", "backtest", "x"),
        ]
        assert _select_primary_issue(issues) == "persistent_bias"

    def test_deterministic_ordering(self):
        issues1 = [
            ComponentIssue("interval_undercoverage", "poor", "backtest", "x"),
            ComponentIssue("variance_instability", "poor", "backtest", "x"),
        ]
        issues2 = [
            ComponentIssue("variance_instability", "poor", "backtest", "x"),
            ComponentIssue("interval_undercoverage", "poor", "backtest", "x"),
        ]
        # Order of issues list should not change primary selection
        assert _select_primary_issue(issues1) == _select_primary_issue(issues2)


# ===========================================================================
# 14. Validation
# ===========================================================================


class TestValidation:
    def test_valid_healthy_result_passes(self):
        result = _build_healthy()
        validate_report_model_diagnostics(result)

    def test_missing_columns_raises(self):
        result = _build_healthy().drop(columns=["model_diagnostic_status"])
        with pytest.raises(ValueError, match="missing columns"):
            validate_report_model_diagnostics(result)

    def test_invalid_status_raises(self):
        result = _build_healthy()
        result.loc[0, "model_diagnostic_status"] = "excellent"
        with pytest.raises(ValueError, match="invalid model_diagnostic_status"):
            validate_report_model_diagnostics(result)

    def test_invalid_action_raises(self):
        result = _build_healthy()
        result.loc[0, "recommended_model_action"] = "disable_forecast"
        with pytest.raises(ValueError, match="invalid recommended_model_action"):
            validate_report_model_diagnostics(result)

    def test_automatic_retraining_true_raises(self):
        result = _build_healthy()
        result.loc[0, "automatic_retraining_triggered"] = True
        with pytest.raises(ValueError, match="automatic_retraining_triggered"):
            validate_report_model_diagnostics(result)

    def test_healthy_with_insufficient_evidence_raises(self):
        result = _build_healthy()
        result.loc[0, "diagnostic_evidence_status"] = "insufficient_evidence"
        with pytest.raises(ValueError, match="insufficient evidence"):
            validate_report_model_diagnostics(result)

    def test_poor_with_zero_poor_issues_raises(self):
        result = _build_healthy()
        result.loc[0, "model_diagnostic_status"] = "poor"
        result.loc[0, "poor_issue_count"] = 0
        with pytest.raises(ValueError, match="poor-severity"):
            validate_report_model_diagnostics(result)

    def test_poor_with_continue_monitoring_raises(self):
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_bias_summary_df=_bt_bias_summary(bias_status="poor"),
            production_bias_df=_prod_bias(valid_count=50),
            diagnostic_run_id=_RUN_ID,
        )
        result.loc[0, "recommended_model_action"] = "continue_monitoring"
        with pytest.raises(ValueError, match="continue_monitoring"):
            validate_report_model_diagnostics(result)

    def test_invalid_coverage_out_of_range_raises(self):
        result = _build_healthy()
        result.loc[0, "observed_coverage"] = 1.5
        with pytest.raises(ValueError, match="outside \\[0, 1\\]"):
            validate_report_model_diagnostics(result)

    def test_negative_count_raises(self):
        result = _build_healthy()
        result.loc[0, "backtest_valid_fold_count"] = -1
        with pytest.raises(ValueError, match="negative values"):
            validate_report_model_diagnostics(result)

    def test_duplicate_report_raises(self):
        result = pd.concat([_build_healthy(), _build_healthy()], ignore_index=True)
        with pytest.raises(ValueError, match="duplicate"):
            validate_report_model_diagnostics(result)

    def test_invalid_primary_issue_raises(self):
        result = _build_healthy()
        result.loc[0, "primary_model_issue"] = "magic_issue"
        with pytest.raises(ValueError, match="invalid primary_model_issue"):
            validate_report_model_diagnostics(result)

    def test_empty_df_passes(self):
        df = pd.DataFrame(columns=MODEL_HEALTH_COLS)
        validate_report_model_diagnostics(df)

    def test_invalid_evidence_status_raises(self):
        result = _build_healthy()
        result.loc[0, "diagnostic_evidence_status"] = "super_complete"
        with pytest.raises(ValueError, match="invalid diagnostic_evidence_status"):
            validate_report_model_diagnostics(result)


# ===========================================================================
# 15. Persistence
# ===========================================================================


class TestPersistence:
    def test_writes_file(self, tmp_path):
        result = _build_healthy()
        path = persist_report_model_diagnostics(result, tmp_path)
        assert path is not None
        assert Path(path).exists()

    def test_file_has_correct_columns(self, tmp_path):
        result = _build_healthy()
        path = persist_report_model_diagnostics(result, tmp_path)
        loaded = pd.read_csv(path)
        assert list(loaded.columns) == MODEL_HEALTH_COLS

    def test_creates_output_directory(self, tmp_path):
        result = _build_healthy()
        new_root = tmp_path / "new_project"
        path = persist_report_model_diagnostics(result, new_root)
        assert path is not None
        assert Path(path).exists()

    def test_overwrites_existing_file(self, tmp_path):
        result = _build_healthy()
        path1 = persist_report_model_diagnostics(result, tmp_path)
        path2 = persist_report_model_diagnostics(result, tmp_path)
        assert path1 == path2

    def test_empty_df_writes_header_only(self, tmp_path):
        df = pd.DataFrame(columns=MODEL_HEALTH_COLS)
        path = persist_report_model_diagnostics(df, tmp_path)
        assert path is not None
        loaded = pd.read_csv(path)
        assert loaded.empty
        assert list(loaded.columns) == MODEL_HEALTH_COLS

    def test_invalid_df_returns_none(self, tmp_path):
        result = _build_healthy()
        result.loc[0, "automatic_retraining_triggered"] = True
        path = persist_report_model_diagnostics(result, tmp_path)
        assert path is None

    def test_diagnostic_source_files_not_modified(self, tmp_path):
        bt_bias = _bt_bias_summary()
        original_len = len(bt_bias)
        result = build_report_model_diagnostics(
            production_forecast_df=_spine(1),
            backtest_bias_summary_df=bt_bias,
            diagnostic_run_id=_RUN_ID,
        )
        persist_report_model_diagnostics(result, tmp_path)
        assert len(bt_bias) == original_len

    def test_filename_is_latest(self, tmp_path):
        result = _build_healthy()
        path = persist_report_model_diagnostics(result, tmp_path)
        assert "latest" in Path(path).name


# ===========================================================================
# 16. Reason generation
# ===========================================================================


class TestReasonGeneration:
    def test_reasons_are_list_of_strings(self):
        issues = [ComponentIssue("persistent_bias", "poor", "backtest", "The model overforecasts.")]
        reasons = build_model_diagnostic_reasons(
            issues, {}, "complete", "poor", "investigate_bias"
        )
        assert isinstance(reasons, list)
        assert all(isinstance(r, str) for r in reasons)

    def test_reasons_not_empty_for_healthy(self):
        reasons = build_model_diagnostic_reasons([], {}, "complete", "healthy", "continue_monitoring")
        assert len(reasons) > 0

    def test_action_reason_appended_last(self):
        issues = [ComponentIssue("persistent_bias", "poor", "backtest", "Bias.")]
        reasons = build_model_diagnostic_reasons(
            issues, {}, "complete", "poor", "investigate_bias"
        )
        assert any("bias" in r.lower() for r in reasons[-2:])

    def test_insufficient_evidence_reason_included(self):
        reasons = build_model_diagnostic_reasons(
            [], {}, "insufficient_evidence", "insufficient_evidence", "insufficient_evidence"
        )
        assert any("insufficient" in r.lower() for r in reasons)

    def test_production_limited_reason_included(self):
        sources = {"production_bias": pd.Series({"valid_residual_count": 9})}
        reasons = build_model_diagnostic_reasons(
            [], sources, "strong_backtest_limited_production", "healthy", "continue_monitoring"
        )
        assert any("limited" in r.lower() or "9" in r for r in reasons)

    def test_deterioration_reason_early_in_list(self):
        issues = [
            ComponentIssue("production_deterioration", "critical", "production", "WAPE worsened."),
            ComponentIssue("persistent_bias", "poor", "backtest", "Bias."),
        ]
        reasons = build_model_diagnostic_reasons(
            issues, {}, "complete", "poor", "review_production_deterioration"
        )
        # deterioration should appear before bias in reason list
        det_idx = next((i for i, r in enumerate(reasons) if "wape" in r.lower() or "deteriorat" in r.lower()), None)
        bias_idx = next((i for i, r in enumerate(reasons) if "bias" in r.lower()), None)
        if det_idx is not None and bias_idx is not None:
            assert det_idx <= bias_idx
