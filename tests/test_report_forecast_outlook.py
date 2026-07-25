"""Tests for report_forecast_outlook.py"""
import pytest
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path

from src.analytics.report_forecast_outlook import (
    ForecastOutlookConfig,
    normalize_production_forecasts,
    build_forecast_horizon_windows,
    aggregate_report_forecast_horizons,
    calculate_forecast_actual_comparisons,
    calculate_forecast_uncertainty,
    calculate_forecast_shape,
    classify_forecast_direction,
    classify_forecast_outlook_status,
    classify_forecast_uncertainty_status,
    validate_report_forecast_outlook,
    persist_report_forecast_outlook,
    build_report_forecast_outlook,
    build_forecast_outlook_reasons,
    determine_primary_forecast_issue,
    FORECAST_OUTLOOK_COLS,
    FORECAST_OUTLOOK_SCHEMA_VERSION,
)

CUTOFF = date(2024, 3, 31)
CFG = ForecastOutlookConfig()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _fc_rows(report_id, n=30, base_views=10.0, start_after=CUTOFF,
             lower=None, upper=None, model="SARIMA", is_forecast=1):
    rows = []
    for i in range(n):
        d = start_after + timedelta(days=i + 1)
        rows.append({
            "report_id": report_id,
            "forecast_date": d,
            "point_forecast": base_views,
            "lower_bound": lower,
            "upper_bound": upper,
            "selected_model_name": model,
            "selected_m": 7,
            "training_cutoff": str(CUTOFF),
            "forecast_generated_at": "2024-04-01T00:00:00",
            "forecast_run_id": "run-001",
            "is_forecast": is_forecast,
        })
    return pd.DataFrame(rows)


def _features_row(report_id, recent_7d=70, recent_28d=280, as_of=CUTOFF):
    return pd.Series({
        "report_id": report_id,
        "report_name": f"Report {report_id}",
        "recent_7d_views": recent_7d,
        "recent_28d_views": recent_28d,
        "analytics_as_of_date": str(as_of),
        "workspace_id": "WS001",
    })


def _dim(report_ids):
    return pd.DataFrame([{
        "report_id": rid,
        "report_name": f"Report {rid}",
        "workspace_id": "WS001",
    } for rid in report_ids])


def _features_df(reports, recent_7d=70, recent_28d=280):
    rows = [_features_row(rid, recent_7d=recent_7d, recent_28d=recent_28d) for rid in reports]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# TestSourceAndHorizon
# ---------------------------------------------------------------------------

class TestSourceAndHorizon:
    def test_missing_report_yields_missing_status(self):
        fc_df = _fc_rows("R_002")
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(fc_df, feats, dim, CFG, "run-x")
        row = result.iloc[0]
        assert row["forecast_source_status"] == "missing"
        assert row["forecast_outlook_status"] == "insufficient_evidence"
        assert row["primary_forecast_outlook_issue"] == "missing_forecast"

    def test_available_forecast_sets_available_status(self):
        fc_df = _fc_rows("R_001")
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(fc_df, feats, dim, CFG, "run-x")
        assert result.iloc[0]["forecast_source_status"] == "available"

    def test_horizon_sufficient_7d_and_28d(self):
        fc_df = _fc_rows("R_001", n=30)
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(fc_df, feats, dim, CFG, "run-x")
        row = result.iloc[0]
        assert row["forecast_horizon_sufficient_7d"] == True
        assert row["forecast_horizon_sufficient_28d"] == True
        assert int(row["available_forecast_horizon_days"]) >= 28

    def test_short_horizon_flags_insufficient(self):
        fc_df = _fc_rows("R_001", n=5)
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(fc_df, feats, dim, CFG, "run-x")
        row = result.iloc[0]
        assert row["forecast_horizon_sufficient_7d"] == False
        assert row["forecast_horizon_sufficient_28d"] == False
        assert row["primary_forecast_outlook_issue"] == "insufficient_horizon"

    def test_forecast_as_of_date_is_min_minus_one(self):
        fc_df = _fc_rows("R_001", n=30, start_after=CUTOFF)
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(fc_df, feats, dim, CFG, "run-x")
        assert result.iloc[0]["forecast_as_of_date"] == str(CUTOFF)

    def test_is_forecast_flag_filters_actuals(self):
        # Mix actual and forecast rows; only is_forecast=1 should be used
        fc_rows = _fc_rows("R_001", n=30, is_forecast=1)
        actual_rows = _fc_rows("R_001", n=10, start_after=CUTOFF - timedelta(days=10),
                               base_views=99.0, is_forecast=0)
        combined = pd.concat([fc_rows, actual_rows], ignore_index=True)
        # rename to raw CSV format
        combined = combined.rename(columns={
            "forecast_date": "Date", "point_forecast": "forecast",
            "report_id": "ReportId", "is_forecast": "IsForecast",
            "lower_bound": "lower_ci", "upper_bound": "upper_ci",
            "selected_model_name": "ModelName", "forecast_run_id": "run_id",
            "forecast_generated_at": "ModelRunTimestamp",
        })
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(combined, feats, dim, CFG, "run-x")
        row = result.iloc[0]
        assert row["forecast_source_status"] == "available"
        # forecast total should reflect base_views=10 * 7 = 70
        assert abs(row["forecast_total_7d"] - 70.0) < 1.0

    def test_raw_csv_column_aliases_normalised(self):
        raw = _fc_rows("R_001", n=30)
        raw = raw.rename(columns={
            "forecast_date": "Date",
            "point_forecast": "forecast",
            "report_id": "ReportId",
            "lower_bound": "lower_ci",
            "upper_bound": "upper_ci",
            "selected_model_name": "ModelName",
            "forecast_run_id": "run_id",
            "forecast_generated_at": "ModelRunTimestamp",
        })
        normed = normalize_production_forecasts(raw)
        assert "report_id" in normed.columns
        assert "point_forecast" in normed.columns
        assert "forecast_date" in normed.columns


# ---------------------------------------------------------------------------
# TestAggregation
# ---------------------------------------------------------------------------

class TestAggregation:
    def test_7d_total_and_average(self):
        fc = _fc_rows("R_001", n=30, base_views=5.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        assert abs(agg["forecast_total_7d"] - 35.0) < 1e-6
        assert abs(agg["forecast_daily_average_7d"] - 5.0) < 1e-6

    def test_28d_total_and_stats(self):
        fc = _fc_rows("R_001", n=30, base_views=4.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        assert abs(agg["forecast_total_28d"] - 112.0) < 1e-6
        assert abs(agg["forecast_median_daily_28d"] - 4.0) < 1e-6
        assert abs(agg["forecast_max_daily_28d"] - 4.0) < 1e-6
        assert abs(agg["forecast_min_daily_28d"] - 4.0) < 1e-6

    def test_null_when_horizon_insufficient_for_7d(self):
        fc = _fc_rows("R_001", n=5, base_views=10.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        assert agg["forecast_total_7d"] is None
        assert agg["forecast_total_28d"] is None

    def test_intervals_aggregated_when_present(self):
        fc = _fc_rows("R_001", n=30, base_views=10.0, lower=8.0, upper=12.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        assert agg["forecast_lower_total_7d"] is not None
        assert abs(agg["forecast_interval_width_total_7d"] - 4 * 7) < 1e-6

    def test_interval_fields_null_when_no_bounds(self):
        fc = _fc_rows("R_001", n=30)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        assert agg["forecast_lower_total_28d"] is None
        assert agg["forecast_upper_total_28d"] is None

    def test_interval_pct_null_when_total_zero(self):
        fc = _fc_rows("R_001", n=30, base_views=0.0, lower=0.0, upper=2.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        assert agg["forecast_interval_width_pct_7d"] is None


# ---------------------------------------------------------------------------
# TestComparison
# ---------------------------------------------------------------------------

class TestComparison:
    def test_change_computed_correctly(self):
        fc = _fc_rows("R_001", n=30, base_views=10.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        feat = _features_row("R_001", recent_7d=70, recent_28d=280)
        comp = calculate_forecast_actual_comparisons(agg, feat, CFG, CUTOFF)
        # forecast_total_7d=70, recent=70 => change=0
        assert abs(comp["forecast_change_vs_actual_7d"]) < 1e-6
        assert abs(comp["forecast_change_vs_actual_28d"]) < 1e-6

    def test_alignment_aligned_when_dates_match(self):
        agg = {"forecast_total_7d": 70.0, "forecast_total_28d": 280.0}
        feat = _features_row("R_001", as_of=CUTOFF)
        comp = calculate_forecast_actual_comparisons(agg, feat, CFG, CUTOFF)
        assert comp["actual_forecast_alignment_status"] == "aligned"
        assert comp["actual_forecast_alignment_days"] == 0

    def test_alignment_incompatible_when_dates_mismatch_beyond_tolerance(self):
        cfg = ForecastOutlookConfig(ALLOWED_ALIGNMENT_LAG_DAYS=0)
        agg = {"forecast_total_7d": None, "forecast_total_28d": None}
        feat = _features_row("R_001", as_of=CUTOFF - timedelta(days=5))
        comp = calculate_forecast_actual_comparisons(agg, feat, cfg, CUTOFF)
        assert comp["actual_forecast_alignment_status"] in ("incompatible", "lagged")

    def test_direction_growth_expected(self):
        fc = _fc_rows("R_001", n=30, base_views=20.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        feat = _features_row("R_001", recent_7d=70, recent_28d=100)  # 560 vs 280
        comp = calculate_forecast_actual_comparisons(agg, feat, CFG, CUTOFF)
        assert comp["forecast_direction_28d"] == "expected_growth"

    def test_direction_decline_expected(self):
        fc = _fc_rows("R_001", n=30, base_views=2.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        feat = _features_row("R_001", recent_7d=70, recent_28d=280)
        comp = calculate_forecast_actual_comparisons(agg, feat, CFG, CUTOFF)
        assert comp["forecast_direction_28d"] == "expected_decline"

    def test_direction_reactivation_when_actual_zero(self):
        direction = classify_forecast_direction(100.0, 0.0, CFG, "28d")
        assert direction == "expected_reactivation"

    def test_direction_inactive_when_both_zero(self):
        direction = classify_forecast_direction(0.0, 0.0, CFG, "28d")
        assert direction == "expected_inactive"

    def test_direction_insufficient_evidence_when_forecast_none(self):
        direction = classify_forecast_direction(None, 100.0, CFG, "28d")
        assert direction == "insufficient_evidence"


# ---------------------------------------------------------------------------
# TestTrend
# ---------------------------------------------------------------------------

class TestTrend:
    def test_stable_trend_flat_forecast(self):
        fc = _fc_rows("R_001", n=30, base_views=10.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        shape = calculate_forecast_shape(df_7d, df_28d, CFG)
        assert shape["forecast_trend_status"] == "stable_trend"
        assert abs(shape["forecast_slope_28d"]) < 1e-6

    def test_upward_trend_detected(self):
        rows = []
        for i in range(30):
            d = CUTOFF + timedelta(days=i + 1)
            rows.append({"report_id": "R_001", "forecast_date": d,
                         "point_forecast": float(i + 1), "lower_bound": None, "upper_bound": None})
        fc = pd.DataFrame(rows)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        shape = calculate_forecast_shape(df_7d, df_28d, CFG)
        assert shape["forecast_slope_28d"] > 0

    def test_peak_and_trough_correctly_identified(self):
        # 30-row sequence; 28d window takes first 28 rows
        # peak at position 14 (20.0), trough at position 27 (1.0)
        vals = [5.0] * 14 + [20.0] + [5.0] * 12 + [1.0] + [5.0, 5.0]
        assert len(vals) == 30
        rows = [{"report_id": "R_001",
                 "forecast_date": CUTOFF + timedelta(days=i + 1),
                 "point_forecast": vals[i], "lower_bound": None, "upper_bound": None}
                for i in range(30)]
        fc = pd.DataFrame(rows)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        shape = calculate_forecast_shape(df_7d, df_28d, CFG)
        assert float(shape["forecast_peak_daily_views_28d"]) == 20.0
        assert float(shape["forecast_trough_daily_views_28d"]) == 1.0

    def test_null_slope_when_insufficient_horizon(self):
        fc = _fc_rows("R_001", n=5, base_views=10.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        shape = calculate_forecast_shape(df_7d, df_28d, CFG)
        assert shape["forecast_slope_28d"] is None
        assert shape["forecast_trend_status"] is None


# ---------------------------------------------------------------------------
# TestLowUsage
# ---------------------------------------------------------------------------

class TestLowUsage:
    def test_zero_usage_days_counted(self):
        vals = [0.0] * 5 + [10.0] * 25
        rows = [{"report_id": "R_001",
                 "forecast_date": CUTOFF + timedelta(days=i + 1),
                 "point_forecast": vals[i], "lower_bound": None, "upper_bound": None}
                for i in range(30)]
        fc = pd.DataFrame(rows)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        shape = calculate_forecast_shape(df_7d, df_28d, CFG)
        assert shape["forecast_zero_usage_days_28d"] == 5

    def test_low_usage_share_computation(self):
        # 14 low days out of 28 (need at least 30 rows so 28d window is filled)
        vals = [0.5] * 14 + [10.0] * 16
        rows = [{"report_id": "R_001",
                 "forecast_date": CUTOFF + timedelta(days=i + 1),
                 "point_forecast": vals[i], "lower_bound": None, "upper_bound": None}
                for i in range(30)]
        fc = pd.DataFrame(rows)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        shape = calculate_forecast_shape(df_7d, df_28d, CFG)
        assert abs(shape["forecast_low_usage_share_28d"] - 14 / 28) < 1e-6

    def test_low_usage_expected_status_when_share_over_50pct(self):
        vals = [0.5] * 28 + [0.5, 0.5]
        rows = [{"report_id": "R_001",
                 "forecast_date": CUTOFF + timedelta(days=i + 1),
                 "point_forecast": vals[i], "lower_bound": None, "upper_bound": None}
                for i in range(30)]
        fc = pd.DataFrame(rows)
        feats = _features_df(["R_001"], recent_28d=20)
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(fc, feats, dim, CFG, "run-x")
        assert result.iloc[0]["forecast_outlook_status"] in (
            "low_usage_expected", "inactivity_expected"
        )

    def test_inactive_expected_when_all_zero(self):
        fc = _fc_rows("R_001", n=30, base_views=0.0)
        feats = _features_df(["R_001"], recent_7d=0, recent_28d=0)
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(fc, feats, dim, CFG, "run-x")
        assert result.iloc[0]["forecast_outlook_status"] == "inactivity_expected"


# ---------------------------------------------------------------------------
# TestUncertainty
# ---------------------------------------------------------------------------

class TestUncertainty:
    def test_intervals_unavailable_when_no_bounds(self):
        fc = _fc_rows("R_001", n=30)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        unc = calculate_forecast_uncertainty(df_7d, df_28d, agg.get("forecast_total_7d"),
                                             agg.get("forecast_total_28d"), CFG)
        assert unc["forecast_uncertainty_status"] == "intervals_unavailable"
        assert unc["relative_uncertainty_28d"] is None

    def test_low_uncertainty_with_narrow_intervals(self):
        # point=100, width=10 per day => rel = 280/2800 ≈ 0.10 < 0.25
        fc = _fc_rows("R_001", n=30, base_views=100.0, lower=95.0, upper=105.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        unc = calculate_forecast_uncertainty(df_7d, df_28d, agg["forecast_total_7d"],
                                             agg["forecast_total_28d"], CFG)
        assert unc["forecast_uncertainty_status"] == "low_uncertainty"

    def test_very_high_uncertainty_with_wide_intervals(self):
        # point=10, width=30 per day => rel = 840/280 = 3.0 > 1.5
        fc = _fc_rows("R_001", n=30, base_views=10.0, lower=0.0, upper=30.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        unc = calculate_forecast_uncertainty(df_7d, df_28d, agg["forecast_total_7d"],
                                             agg["forecast_total_28d"], CFG)
        assert unc["forecast_uncertainty_status"] == "very_high_uncertainty"

    def test_relative_uncertainty_null_when_total_zero(self):
        fc = _fc_rows("R_001", n=30, base_views=0.0, lower=0.0, upper=0.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        unc = calculate_forecast_uncertainty(df_7d, df_28d, 0.0, 0.0, CFG)
        assert unc["relative_uncertainty_28d"] is None

    def test_mean_interval_width_computed(self):
        fc = _fc_rows("R_001", n=30, base_views=10.0, lower=8.0, upper=12.0)
        df_7d, df_28d = build_forecast_horizon_windows(fc, CUTOFF)
        agg = aggregate_report_forecast_horizons(df_7d, df_28d)
        unc = calculate_forecast_uncertainty(df_7d, df_28d, agg["forecast_total_7d"],
                                             agg["forecast_total_28d"], CFG)
        assert abs(unc["mean_prediction_interval_width_28d"] - 4.0) < 1e-6

    def test_classify_uncertainty_invalid_when_negative_width(self):
        result = classify_forecast_uncertainty_status(-0.1, True, CFG)
        assert result == "invalid_intervals"


# ---------------------------------------------------------------------------
# TestStatus
# ---------------------------------------------------------------------------

class TestStatus:
    def test_growth_expected_status(self):
        row = {
            "forecast_source_status": "available",
            "forecast_horizon_sufficient_7d": True,
            "forecast_horizon_sufficient_28d": True,
            "actual_forecast_alignment_status": "aligned",
            "forecast_direction_28d": "expected_growth",
            "forecast_low_usage_share_28d": 0.0,
            "forecast_uncertainty_status": "low_uncertainty",
        }
        assert classify_forecast_outlook_status(row, CFG) == "growth_expected"

    def test_decline_expected_status(self):
        row = {
            "forecast_source_status": "available",
            "forecast_horizon_sufficient_7d": True,
            "forecast_horizon_sufficient_28d": True,
            "actual_forecast_alignment_status": "aligned",
            "forecast_direction_28d": "expected_decline",
            "forecast_low_usage_share_28d": 0.0,
            "forecast_uncertainty_status": "moderate_uncertainty",
        }
        assert classify_forecast_outlook_status(row, CFG) == "decline_expected"

    def test_inactivity_expected_status(self):
        row = {
            "forecast_source_status": "available",
            "forecast_horizon_sufficient_7d": True,
            "forecast_horizon_sufficient_28d": True,
            "actual_forecast_alignment_status": "aligned",
            "forecast_direction_28d": "expected_inactive",
            "forecast_low_usage_share_28d": 1.0,
            "forecast_uncertainty_status": "low_uncertainty",
        }
        assert classify_forecast_outlook_status(row, CFG) == "inactivity_expected"

    def test_uncertain_outlook_when_very_high_uncertainty(self):
        row = {
            "forecast_source_status": "available",
            "forecast_horizon_sufficient_7d": True,
            "forecast_horizon_sufficient_28d": True,
            "actual_forecast_alignment_status": "aligned",
            "forecast_direction_28d": "expected_growth",
            "forecast_low_usage_share_28d": 0.0,
            "forecast_uncertainty_status": "very_high_uncertainty",
        }
        assert classify_forecast_outlook_status(row, CFG) == "uncertain_outlook"

    def test_insufficient_evidence_when_missing(self):
        row = {
            "forecast_source_status": "missing",
            "forecast_horizon_sufficient_7d": False,
            "forecast_horizon_sufficient_28d": False,
            "actual_forecast_alignment_status": "unknown",
            "forecast_direction_28d": "insufficient_evidence",
            "forecast_low_usage_share_28d": None,
            "forecast_uncertainty_status": "insufficient_horizon",
        }
        assert classify_forecast_outlook_status(row, CFG) == "insufficient_evidence"

    def test_stable_outlook_stability_direction(self):
        row = {
            "forecast_source_status": "available",
            "forecast_horizon_sufficient_7d": True,
            "forecast_horizon_sufficient_28d": True,
            "actual_forecast_alignment_status": "aligned",
            "forecast_direction_28d": "expected_stability",
            "forecast_low_usage_share_28d": 0.0,
            "forecast_uncertainty_status": "low_uncertainty",
        }
        assert classify_forecast_outlook_status(row, CFG) == "stable_outlook"

    def test_reactivation_expected_status(self):
        row = {
            "forecast_source_status": "available",
            "forecast_horizon_sufficient_7d": True,
            "forecast_horizon_sufficient_28d": True,
            "actual_forecast_alignment_status": "aligned",
            "forecast_direction_28d": "expected_reactivation",
            "forecast_low_usage_share_28d": 0.0,
            "forecast_uncertainty_status": "moderate_uncertainty",
        }
        assert classify_forecast_outlook_status(row, CFG) == "reactivation_expected"


# ---------------------------------------------------------------------------
# TestLineage
# ---------------------------------------------------------------------------

class TestLineage:
    def test_run_id_populated(self):
        fc = _fc_rows("R_001", n=30)
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(fc, feats, dim, CFG, "my-run-id")
        assert result.iloc[0]["forecast_run_id"] == "my-run-id"

    def test_schema_version_populated(self):
        fc = _fc_rows("R_001", n=30)
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(fc, feats, dim, CFG, "run-x")
        assert result.iloc[0]["forecast_schema_version"] == FORECAST_OUTLOOK_SCHEMA_VERSION

    def test_generated_at_is_string(self):
        fc = _fc_rows("R_001", n=30)
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(fc, feats, dim, CFG, "run-x")
        assert isinstance(result.iloc[0]["generated_at"], str)

    def test_model_name_carried_through(self):
        fc = _fc_rows("R_001", n=30, model="prophet")
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(fc, feats, dim, CFG, "run-x")
        assert result.iloc[0]["selected_model_name"] == "prophet"

    def test_all_schema_columns_present(self):
        fc = _fc_rows("R_001", n=30)
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(fc, feats, dim, CFG, "run-x")
        missing = set(FORECAST_OUTLOOK_COLS) - set(result.columns)
        assert len(missing) == 0, f"Missing columns: {missing}"

    def test_reasons_string_has_10_slots(self):
        fc = _fc_rows("R_001", n=30)
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        result = build_report_forecast_outlook(fc, feats, dim, CFG, "run-x")
        reasons = result.iloc[0]["forecast_outlook_reasons"]
        assert reasons is not None
        parts = reasons.split(" | ")
        assert len(parts) == 10


# ---------------------------------------------------------------------------
# TestValidation
# ---------------------------------------------------------------------------

class TestValidation:
    def _valid_df(self):
        fc = _fc_rows("R_001", n=30)
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        return build_report_forecast_outlook(fc, feats, dim, CFG, "run-x")

    def test_valid_df_passes(self):
        df = self._valid_df()
        validate_report_forecast_outlook(df)  # should not raise

    def test_raises_on_missing_column(self):
        df = self._valid_df().drop(columns=["forecast_run_id"])
        with pytest.raises(ValueError, match="missing_columns"):
            validate_report_forecast_outlook(df)

    def test_raises_on_duplicate_run_report(self):
        df = self._valid_df()
        df2 = pd.concat([df, df], ignore_index=True)
        with pytest.raises(ValueError, match="duplicate"):
            validate_report_forecast_outlook(df2)

    def test_raises_on_user_identifier_column(self):
        df = self._valid_df()
        df["user_id"] = "u1"
        with pytest.raises(ValueError, match="user_identifier"):
            validate_report_forecast_outlook(df)

    def test_raises_on_invalid_outlook_status(self):
        df = self._valid_df()
        df["forecast_outlook_status"] = "retire_immediately"
        with pytest.raises(ValueError, match="invalid_outlook_status"):
            validate_report_forecast_outlook(df)

    def test_raises_on_prohibited_action(self):
        df = self._valid_df()
        df["recommended_forecast_review_action"] = "retire_report"
        with pytest.raises(ValueError, match="prohibited_action"):
            validate_report_forecast_outlook(df)

    def test_output_sorted_by_report_id(self):
        fc = pd.concat([_fc_rows("R_003", n=30), _fc_rows("R_001", n=30),
                        _fc_rows("R_002", n=30)])
        feats = _features_df(["R_001", "R_002", "R_003"])
        dim = _dim(["R_001", "R_002", "R_003"])
        result = build_report_forecast_outlook(fc, feats, dim, CFG, "run-x")
        assert list(result["report_id"]) == ["R_001", "R_002", "R_003"]


# ---------------------------------------------------------------------------
# TestPersistence
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_persist_writes_csv(self, tmp_path):
        fc = _fc_rows("R_001", n=30)
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        df = build_report_forecast_outlook(fc, feats, dim, CFG, "run-x")
        out = persist_report_forecast_outlook(df, tmp_path)
        assert out.exists()
        loaded = pd.read_csv(out)
        assert len(loaded) == 1
        assert "forecast_outlook_status" in loaded.columns

    def test_persist_to_correct_path(self, tmp_path):
        fc = _fc_rows("R_001", n=30)
        feats = _features_df(["R_001"])
        dim = _dim(["R_001"])
        df = build_report_forecast_outlook(fc, feats, dim, CFG, "run-x")
        out = persist_report_forecast_outlook(df, tmp_path)
        assert out == tmp_path / "outputs" / "analytics" / "report_forecast_outlook.csv"

    def test_persist_raises_on_invalid_df(self, tmp_path):
        df = pd.DataFrame({"bad_col": [1]})
        with pytest.raises(ValueError):
            persist_report_forecast_outlook(df, tmp_path)

    def test_multiple_reports_all_written(self, tmp_path):
        fc = pd.concat([_fc_rows("R_001", n=30), _fc_rows("R_002", n=30)])
        feats = _features_df(["R_001", "R_002"])
        dim = _dim(["R_001", "R_002"])
        df = build_report_forecast_outlook(fc, feats, dim, CFG, "run-x")
        out = persist_report_forecast_outlook(df, tmp_path)
        loaded = pd.read_csv(out)
        assert len(loaded) == 2
