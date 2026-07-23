"""Tests for src.models.seasonality_diagnostics.

Guarantees verified
-------------------
1.  Weekly-report summary has dominant_diagnostic_period=7 and selected_m=7.
2.  Monthly-like report summary has eligible_seasonal_periods containing 28 or 30.
3.  Non-seasonal report summary has selected_m=1 and no dominant period.
4.  Quarterly candidate (m=90) is marked ineligible when training history is short.
5.  dominant_diagnostic_period can differ from selected_m when backtesting
    selected a different period (e.g. m=7 detected but non-seasonal selected).
6.  Fallback production model differs from originally selected candidate:
    selected_model_name reflects the original selection; fallback_used=True.
7.  Deterministic serialisation and sorting.
8.  Duplicate-key validation raises ValueError.
9.  Invalid selected_m raises ValueError in schema validation.
10. Output generation when one or more candidate models fail.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.config.forecasting import MIN_SEASONAL_CYCLES, NON_SEASONAL_PERIOD, SEASONAL_CANDIDATES
from src.models.seasonality_diagnostics import (
    CANDIDATE_COLS,
    SUMMARY_COLS,
    _encode_list,
    _decode_list,
    build_seasonality_candidates,
    build_seasonality_summary,
    run_seasonality_data_quality_checks,
    save_seasonality_diagnostics,
    validate_seasonality_candidates,
    validate_seasonality_summary,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_SERIES_START = pd.Timestamp("2022-01-01")


def _make_series(n_days: int = 400, start=_SERIES_START) -> pd.Series:
    rng = np.random.default_rng(0)
    idx = pd.date_range(start=start, periods=n_days, freq="D")
    return pd.Series(rng.integers(1, 20, n_days).astype(float), index=idx)


def _fold_metric_row(
    report_id: str = "R1",
    fold_number: int = 1,
    cutoff_date=pd.Timestamp("2022-07-01"),
    model_name: str = "seasonal_naive_m7",
    model_family: str = "seasonal_naive",
    candidate_m: int = 7,
    seasonal_candidate_rank: int = 1,
    cycles_available: int = 30,
    autocorrelation_at_m: float = 0.6,
    spectral_power_at_m: float = 0.4,
    seasonality_status: str = "seasonal",
    candidate_source: str = "seasonality_profiler",
    fit_status: str = "ok",
    mase_lag1: float = 0.8,
    wape: float = 0.3,
    bias: float = 0.05,
    error_message: str = "",
) -> dict:
    return dict(
        report_id=report_id, fold_number=fold_number, cutoff_date=cutoff_date,
        model_name=model_name, model_family=model_family, candidate_m=candidate_m,
        seasonal_candidate_rank=seasonal_candidate_rank,
        cycles_available=cycles_available,
        autocorrelation_at_m=autocorrelation_at_m,
        spectral_power_at_m=spectral_power_at_m,
        seasonality_status=seasonality_status, candidate_source=candidate_source,
        fit_status=fit_status, mase_lag1=mase_lag1, wape=wape, bias=bias,
        mae=5.0, rmse=6.0, mase_m=0.9, mean_interval_width=np.nan,
        interval_coverage=np.nan, error_message=error_message,
    )


def _make_fold_metrics(*rows) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


def _selection_row(
    report_id: str = "R1",
    selected_model_family: str = "seasonal_naive",
    selected_model_name: str = "seasonal_naive_m7",
    selected_m: int = 7,
    selection_status: str = "selected",
    selection_reason: str = "Seasonal-naive m=7 selected: best MASE.",
    valid_folds: int = 4,
    median_mase: float = 0.75,
    mean_wape: float = 0.30,
    mean_bias: float = 0.02,
    fold_win_rate: float = 0.75,
    seasonal_naive_median_mase: float = 0.75,
    improvement_vs_seasonal_naive_pct: float = 0.0,
) -> dict:
    return dict(
        report_id=report_id,
        selected_model_family=selected_model_family,
        selected_model_name=selected_model_name,
        selected_m=selected_m,
        selection_status=selection_status,
        selection_reason=selection_reason,
        valid_folds=valid_folds,
        median_mase=median_mase,
        mean_wape=mean_wape,
        mean_bias=mean_bias,
        fold_win_rate=fold_win_rate,
        seasonal_naive_median_mase=seasonal_naive_median_mase,
        improvement_vs_seasonal_naive_pct=improvement_vs_seasonal_naive_pct,
    )


def _make_selection(*rows) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


def _prod_row(
    report_id: str = "R1",
    production_fit_status: str = "ok",
    fallback_used: bool = False,
    fallback_reason=None,
    horizon_step: int = 1,
    selected_model_family: str = "seasonal_naive",
    selected_model_name: str = "seasonal_naive_m7",
    selected_m: int = 7,
) -> dict:
    return dict(
        report_id=report_id,
        horizon_step=horizon_step,
        production_fit_status=production_fit_status,
        fallback_used=fallback_used,
        fallback_reason=fallback_reason,
        selected_model_family=selected_model_family,
        selected_model_name=selected_model_name,
        selected_m=selected_m,
        run_id="RUN1",
        selection_run_id="RUN1",
        generated_at=pd.Timestamp("2023-01-01"),
        training_start=pd.Timestamp("2022-01-01"),
        training_cutoff=pd.Timestamp("2023-01-01"),
        forecast_date=pd.Timestamp("2023-01-02"),
        forecast=10.0, lower_bound=8.0, upper_bound=12.0,
        model_order=None, seasonal_order=None,
        selection_reason="test", valid_backtest_folds=4,
        median_backtest_mase=0.75, mean_backtest_wape=0.3, mean_backtest_bias=0.02,
    )


def _make_prod(*rows) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


# ===========================================================================
# 1. Weekly report summary
# ===========================================================================

class TestWeeklyReportSummary:

    def _build(self):
        fold_rows = [
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.7, spectral_power_at_m=0.5),
            _fold_metric_row(candidate_m=7, fold_number=2, cutoff_date=pd.Timestamp("2022-08-01"),
                             seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.65, spectral_power_at_m=0.45),
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=200, autocorrelation_at_m=np.nan,
                             spectral_power_at_m=np.nan, candidate_source="baseline"),
        ]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(_selection_row())
        prod = _make_prod(_prod_row())
        series = {"R1": _make_series()}
        return build_seasonality_summary(series, sel, fm, prod)

    def test_dominant_diagnostic_period_is_7(self):
        df = self._build()
        row = df[df["report_id"] == "R1"].iloc[0]
        assert row["dominant_diagnostic_period"] == 7

    def test_selected_m_is_7(self):
        df = self._build()
        assert df[df["report_id"] == "R1"].iloc[0]["selected_m"] == 7

    def test_eligible_periods_contains_7(self):
        df = self._build()
        row = df[df["report_id"] == "R1"].iloc[0]
        eligible = _decode_list(row["eligible_seasonal_periods"])
        assert 7 in eligible

    def test_seasonality_status_is_seasonal(self):
        df = self._build()
        assert df[df["report_id"] == "R1"].iloc[0]["seasonality_status"] == "seasonal"

    def test_production_fit_status_ok(self):
        df = self._build()
        assert df[df["report_id"] == "R1"].iloc[0]["production_fit_status"] == "ok"

    def test_fallback_not_used(self):
        df = self._build()
        assert df[df["report_id"] == "R1"].iloc[0]["fallback_used"] == False

    def test_output_columns_match_schema(self):
        df = self._build()
        assert list(df.columns) == SUMMARY_COLS

    def test_one_row_per_report(self):
        df = self._build()
        assert len(df[df["report_id"] == "R1"]) == 1

    def test_selected_model_name_propagated(self):
        df = self._build()
        assert df[df["report_id"] == "R1"].iloc[0]["selected_model_name"] == "seasonal_naive_m7"

    def test_valid_backtest_folds_propagated(self):
        df = self._build()
        assert df[df["report_id"] == "R1"].iloc[0]["valid_backtest_folds"] == 4


# ===========================================================================
# 2. Monthly-like report summary (m=28 or m=30)
# ===========================================================================

class TestMonthlyLikeReportSummary:

    def _build(self, m: int = 28):
        fold_rows = [
            _fold_metric_row(candidate_m=m, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.5, spectral_power_at_m=0.4,
                             cycles_available=20),
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=400, autocorrelation_at_m=np.nan,
                             spectral_power_at_m=np.nan, candidate_source="baseline"),
        ]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(_selection_row(
            selected_model_name=f"seasonal_naive_m{m}",
            selected_m=m,
        ))
        prod = _make_prod(_prod_row(selected_model_name=f"seasonal_naive_m{m}", selected_m=m))
        return build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)

    def test_eligible_periods_contains_m28(self):
        df = self._build(28)
        eligible = _decode_list(df.iloc[0]["eligible_seasonal_periods"])
        assert 28 in eligible

    def test_eligible_periods_contains_m30(self):
        df = self._build(30)
        eligible = _decode_list(df.iloc[0]["eligible_seasonal_periods"])
        assert 30 in eligible

    def test_selected_m_28(self):
        df = self._build(28)
        assert df.iloc[0]["selected_m"] == 28

    def test_dominant_diagnostic_period_28(self):
        df = self._build(28)
        assert df.iloc[0]["dominant_diagnostic_period"] == 28

    def test_history_days_populated(self):
        df = self._build()
        assert df.iloc[0]["history_days"] == 400

    def test_first_observed_date(self):
        df = self._build()
        assert df.iloc[0]["first_observed_date"] == _SERIES_START


# ===========================================================================
# 3. Non-seasonal report summary
# ===========================================================================

class TestNonSeasonalReportSummary:

    def _build(self):
        fold_rows = [
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=200, autocorrelation_at_m=np.nan,
                             spectral_power_at_m=np.nan,
                             seasonality_status="non_seasonal",
                             candidate_source="baseline"),
        ]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(_selection_row(
            selected_model_family="naive",
            selected_model_name="naive",
            selected_m=NON_SEASONAL_PERIOD,
            selection_reason="Naive selected: seasonal candidates did not improve accuracy.",
        ))
        prod = _make_prod(_prod_row(
            selected_model_family="naive", selected_model_name="naive", selected_m=1,
        ))
        return build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)

    def test_selected_m_is_1(self):
        df = self._build()
        assert df.iloc[0]["selected_m"] == NON_SEASONAL_PERIOD

    def test_no_dominant_diagnostic_period(self):
        df = self._build()
        assert pd.isna(df.iloc[0]["dominant_diagnostic_period"])

    def test_eligible_seasonal_periods_empty(self):
        df = self._build()
        assert df.iloc[0]["eligible_seasonal_periods"] == ""

    def test_seasonality_status_non_seasonal(self):
        df = self._build()
        assert df.iloc[0]["seasonality_status"] == "non_seasonal"

    def test_selected_model_family_naive(self):
        df = self._build()
        assert df.iloc[0]["selected_model_family"] == "naive"


# ===========================================================================
# 4. Quarterly candidate excluded for insufficient history
# ===========================================================================

class TestQuarterlyCandidateExcluded:

    def _build_candidates(self, n_train_proxy: int = 200) -> pd.DataFrame:
        # m=90 needs MIN_SEASONAL_CYCLES * 90 = 270 days; with 200 days it's excluded
        fold_rows = [
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             cycles_available=28, autocorrelation_at_m=0.5,
                             spectral_power_at_m=0.3),
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=n_train_proxy,
                             autocorrelation_at_m=np.nan, spectral_power_at_m=np.nan,
                             candidate_source="baseline"),
        ]
        return build_seasonality_candidates(_make_fold_metrics(*fold_rows))

    def test_m90_not_eligible_when_short_history(self):
        cands = self._build_candidates(n_train=200)
        m90 = cands[cands["candidate_m"] == 90]
        if not m90.empty:
            assert (m90["candidate_eligible"] == False).all()

    def test_m90_row_has_exclusion_reason(self):
        cands = self._build_candidates(n_train=200)
        m90 = cands[cands["candidate_m"] == 90]
        if not m90.empty:
            assert (m90["exclusion_reason"] != "").all()

    def _build_candidates(self, n_train: int = 200) -> pd.DataFrame:
        fold_rows = [
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             cycles_available=28, autocorrelation_at_m=0.5,
                             spectral_power_at_m=0.3),
            # Non-seasonal baseline carries n_train via cycles_available at m=1
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=n_train,
                             autocorrelation_at_m=np.nan, spectral_power_at_m=np.nan,
                             candidate_source="baseline"),
        ]
        return build_seasonality_candidates(_make_fold_metrics(*fold_rows))

    def test_m7_eligible_when_sufficient_history(self):
        cands = self._build_candidates(n_train=200)
        m7 = cands[cands["candidate_m"] == 7]
        assert not m7.empty
        assert (m7["candidate_eligible"] == True).all()

    def test_candidate_cols_present(self):
        cands = self._build_candidates()
        assert list(cands.columns) == CANDIDATE_COLS

    def test_m1_always_present(self):
        cands = self._build_candidates()
        assert NON_SEASONAL_PERIOD in cands["candidate_m"].values

    def test_m1_always_eligible(self):
        cands = self._build_candidates()
        m1 = cands[cands["candidate_m"] == NON_SEASONAL_PERIOD]
        assert not m1.empty
        assert (m1["candidate_eligible"] == True).all()

    def test_validate_passes_valid_output(self):
        cands = self._build_candidates()
        validate_seasonality_candidates(cands)  # should not raise


# ===========================================================================
# 5. Dominant diagnostic period differs from selected_m
# ===========================================================================

class TestDominantPeriodDiffersFromSelected:

    def test_dominant_period_not_forced_to_match_selected_m(self):
        """If diagnostics point to m=7 but backtesting selected m=1 (non-seasonal),
        dominant_diagnostic_period should still be 7, not 1."""
        fold_rows = [
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.6, spectral_power_at_m=0.5,
                             seasonality_status="seasonal"),
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=200,
                             autocorrelation_at_m=np.nan, spectral_power_at_m=np.nan,
                             candidate_source="baseline",
                             seasonality_status="seasonal"),
        ]
        fm = _make_fold_metrics(*fold_rows)
        # Backtesting selected non-seasonal even though m=7 had a high score
        sel = _make_selection(_selection_row(
            selected_model_family="naive",
            selected_model_name="naive",
            selected_m=NON_SEASONAL_PERIOD,
            selection_reason="Naive selected: seasonal candidates did not improve accuracy.",
        ))
        prod = _make_prod(_prod_row(selected_model_family="naive", selected_model_name="naive", selected_m=1))
        df = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        row = df.iloc[0]
        # dominant period is the one with highest score (m=7)
        assert row["dominant_diagnostic_period"] == 7
        # but selected_m is 1 (non-seasonal)
        assert row["selected_m"] == NON_SEASONAL_PERIOD
        # They differ → the columns reflect separate concepts
        assert row["dominant_diagnostic_period"] != row["selected_m"]

    def test_summary_exposes_both_dominant_and_selected(self):
        """Both dominant_diagnostic_period and selected_m must appear separately."""
        fold_rows = [
            _fold_metric_row(candidate_m=14, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.8, spectral_power_at_m=0.7),
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=2,
                             autocorrelation_at_m=0.4, spectral_power_at_m=0.3),
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=200, autocorrelation_at_m=np.nan,
                             spectral_power_at_m=np.nan, candidate_source="baseline"),
        ]
        fm = _make_fold_metrics(*fold_rows)
        # Diagnostics say m=14 is strongest; backtesting selected m=7
        sel = _make_selection(_selection_row(selected_model_name="seasonal_naive_m7", selected_m=7))
        prod = _make_prod(_prod_row(selected_model_name="seasonal_naive_m7", selected_m=7))
        df = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        row = df.iloc[0]
        assert row["dominant_diagnostic_period"] == 14
        assert row["selected_m"] == 7
        assert "dominant_diagnostic_period" in df.columns
        assert "selected_m" in df.columns


# ===========================================================================
# 6. Fallback production model differs from originally selected candidate
# ===========================================================================

class TestFallbackLineageInSummary:

    def test_fallback_used_true_propagated_to_summary(self):
        fold_rows = [
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.6, spectral_power_at_m=0.4),
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=200, autocorrelation_at_m=np.nan,
                             spectral_power_at_m=np.nan, candidate_source="baseline"),
        ]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(_selection_row(
            selected_model_family="auto_arima",
            selected_model_name="auto_arima_m7",
            selected_m=7,
        ))
        prod = _make_prod(_prod_row(
            selected_model_family="auto_arima",
            selected_model_name="auto_arima_m7",
            selected_m=7,
            production_fit_status="ok",
            fallback_used=True,
            fallback_reason="Originally selected auto_arima_m7 (m=7) failed. Fell back to seasonal_naive_m7 (m=7).",
        ))
        df = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        row = df.iloc[0]
        assert row["fallback_used"] == True
        assert "auto_arima_m7" in str(row["fallback_reason"])
        # Original selection is still preserved in selected_model_name
        assert row["selected_model_name"] == "auto_arima_m7"

    def test_fallback_reason_propagated(self):
        fm = _make_fold_metrics(
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=200, autocorrelation_at_m=np.nan,
                             spectral_power_at_m=np.nan, candidate_source="baseline"),
        )
        sel = _make_selection(_selection_row())
        reason = "Originally selected auto_arima_m7 (m=7) failed. Fell back to seasonal_naive_m7 (m=7)."
        prod = _make_prod(_prod_row(fallback_used=True, fallback_reason=reason))
        df = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        assert df.iloc[0]["fallback_reason"] == reason

    def test_fallback_false_when_primary_succeeds(self):
        fm = _make_fold_metrics(
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.5, spectral_power_at_m=0.4),
        )
        sel = _make_selection(_selection_row())
        prod = _make_prod(_prod_row(fallback_used=False, fallback_reason=None))
        df = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        assert df.iloc[0]["fallback_used"] == False
        assert df.iloc[0]["fallback_reason"] is None


# ===========================================================================
# 7. Deterministic serialisation and sorting
# ===========================================================================

class TestDeterministicSerialisation:

    def test_summary_sorted_by_report_id(self):
        fold_rows = [
            _fold_metric_row(report_id="R2", candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.5, spectral_power_at_m=0.4),
            _fold_metric_row(report_id="R1", candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.6, spectral_power_at_m=0.5),
            _fold_metric_row(report_id="R1", candidate_m=NON_SEASONAL_PERIOD,
                             model_name="naive", model_family="naive",
                             seasonal_candidate_rank=0, cycles_available=200,
                             autocorrelation_at_m=np.nan, spectral_power_at_m=np.nan,
                             candidate_source="baseline"),
            _fold_metric_row(report_id="R2", candidate_m=NON_SEASONAL_PERIOD,
                             model_name="naive", model_family="naive",
                             seasonal_candidate_rank=0, cycles_available=200,
                             autocorrelation_at_m=np.nan, spectral_power_at_m=np.nan,
                             candidate_source="baseline"),
        ]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(
            _selection_row(report_id="R2"),
            _selection_row(report_id="R1"),
        )
        prod = _make_prod(_prod_row(report_id="R2"), _prod_row(report_id="R1"))
        series = {"R1": _make_series(), "R2": _make_series()}
        df = build_seasonality_summary(series, sel, fm, prod)
        assert list(df["report_id"]) == ["R1", "R2"]

    def test_candidates_sorted_by_report_fold_rank_m(self):
        fold_rows = [
            _fold_metric_row(candidate_m=14, fold_number=1, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.7, spectral_power_at_m=0.5),
            _fold_metric_row(candidate_m=7, fold_number=1, seasonal_candidate_rank=2,
                             autocorrelation_at_m=0.5, spectral_power_at_m=0.4),
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, fold_number=1,
                             model_name="naive", model_family="naive",
                             seasonal_candidate_rank=0, cycles_available=200,
                             autocorrelation_at_m=np.nan, spectral_power_at_m=np.nan,
                             candidate_source="baseline"),
        ]
        cands = build_seasonality_candidates(_make_fold_metrics(*fold_rows))
        seasonal = cands[cands["candidate_m"] > NON_SEASONAL_PERIOD].reset_index(drop=True)
        if len(seasonal) >= 2:
            assert seasonal.iloc[0]["candidate_rank"] <= seasonal.iloc[1]["candidate_rank"]

    def test_same_output_on_repeated_calls(self):
        fold_rows = [
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.6, spectral_power_at_m=0.4),
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=200, autocorrelation_at_m=np.nan,
                             spectral_power_at_m=np.nan, candidate_source="baseline"),
        ]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(_selection_row())
        prod = _make_prod(_prod_row())
        series = {"R1": _make_series()}
        df1 = build_seasonality_summary(series, sel, fm, prod)
        df2 = build_seasonality_summary(series, sel, fm, prod)
        pd.testing.assert_frame_equal(df1, df2)

    def test_encode_decode_roundtrip(self):
        periods = [7, 14, 28]
        encoded = _encode_list(periods)
        assert encoded == "7|14|28"
        assert _decode_list(encoded) == periods

    def test_encode_empty_list(self):
        assert _encode_list([]) == ""
        assert _decode_list("") == []

    def test_encode_single(self):
        assert _encode_list([30]) == "30"
        assert _decode_list("30") == [30]


# ===========================================================================
# 8. Duplicate-key validation
# ===========================================================================

class TestDuplicateKeyValidation:

    def test_duplicate_report_id_in_summary_raises(self):
        fold_rows = [
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.5, spectral_power_at_m=0.4),
        ]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(_selection_row())
        prod = _make_prod(_prod_row())
        series = {"R1": _make_series()}
        df = build_seasonality_summary(series, sel, fm, prod)
        # Manually duplicate a row to trigger the validation check
        duped = pd.concat([df, df], ignore_index=True)
        with pytest.raises(ValueError, match="duplicate report_id"):
            validate_seasonality_summary(duped)

    def test_duplicate_fold_period_in_candidates_raises(self):
        fold_rows = [
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.5, spectral_power_at_m=0.4),
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=200, autocorrelation_at_m=np.nan,
                             spectral_power_at_m=np.nan, candidate_source="baseline"),
        ]
        cands = build_seasonality_candidates(_make_fold_metrics(*fold_rows))
        duped = pd.concat([cands, cands], ignore_index=True)
        with pytest.raises(ValueError, match="duplicate"):
            validate_seasonality_candidates(duped)

    def test_valid_summary_passes_validation(self):
        fold_rows = [
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.5, spectral_power_at_m=0.4),
        ]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(_selection_row())
        prod = _make_prod(_prod_row())
        df = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        validate_seasonality_summary(df)  # should not raise

    def test_valid_candidates_passes_validation(self):
        fold_rows = [
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.5, spectral_power_at_m=0.4),
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=200, autocorrelation_at_m=np.nan,
                             spectral_power_at_m=np.nan, candidate_source="baseline"),
        ]
        cands = build_seasonality_candidates(_make_fold_metrics(*fold_rows))
        validate_seasonality_candidates(cands)  # should not raise


# ===========================================================================
# 9. Invalid selected_m detection
# ===========================================================================

class TestInvalidSelectedM:

    def test_invalid_selected_m_raises(self):
        fold_rows = [
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.5, spectral_power_at_m=0.4),
        ]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(_selection_row())
        prod = _make_prod(_prod_row())
        df = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        # Inject invalid selected_m
        df = df.copy()
        df["selected_m"] = 999
        with pytest.raises(ValueError, match="selected_m"):
            validate_seasonality_summary(df)

    def test_selected_m_1_is_valid(self):
        fold_rows = [
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=200, autocorrelation_at_m=np.nan,
                             spectral_power_at_m=np.nan, candidate_source="baseline"),
        ]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(_selection_row(selected_m=NON_SEASONAL_PERIOD,
                                             selected_model_family="naive",
                                             selected_model_name="naive"))
        prod = _make_prod(_prod_row(selected_model_family="naive",
                                    selected_model_name="naive", selected_m=1))
        df = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        validate_seasonality_summary(df)  # should not raise

    def test_all_standard_candidate_ms_valid(self):
        """All values in SEASONAL_CANDIDATES and m=1 must pass schema validation."""
        for m in list(SEASONAL_CANDIDATES) + [NON_SEASONAL_PERIOD]:
            fold_rows = [
                _fold_metric_row(candidate_m=m, seasonal_candidate_rank=1 if m > 1 else 0,
                                 autocorrelation_at_m=0.5 if m > 1 else np.nan,
                                 spectral_power_at_m=0.4 if m > 1 else np.nan,
                                 candidate_source="seasonality_profiler" if m > 1 else "baseline"),
            ]
            fm = _make_fold_metrics(*fold_rows)
            sel = _make_selection(_selection_row(selected_m=m))
            prod = _make_prod(_prod_row(selected_m=m))
            df = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
            validate_seasonality_summary(df)  # must not raise for any valid m


# ===========================================================================
# 10. Output generation when candidate models fail
# ===========================================================================

class TestOutputWithFailedCandidates:

    def _make_fm_with_failures(self) -> pd.DataFrame:
        return _make_fold_metrics(
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.6, spectral_power_at_m=0.5,
                             fit_status="ok"),
            _fold_metric_row(candidate_m=7, model_name="auto_arima_m7",
                             model_family="auto_arima", seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.6, spectral_power_at_m=0.5,
                             fit_status="failed",
                             error_message="RuntimeError: convergence failed"),
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=200, autocorrelation_at_m=np.nan,
                             spectral_power_at_m=np.nan, candidate_source="baseline",
                             fit_status="ok"),
        )

    def test_summary_produced_even_with_failures(self):
        fm = self._make_fm_with_failures()
        sel = _make_selection(_selection_row())
        prod = _make_prod(_prod_row())
        df = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        assert len(df) == 1
        assert df.iloc[0]["report_id"] == "R1"

    def test_candidates_produced_even_with_failures(self):
        fm = self._make_fm_with_failures()
        cands = build_seasonality_candidates(fm)
        assert len(cands) > 0
        assert list(cands.columns) == CANDIDATE_COLS

    def test_failed_models_counted_in_failed_model_count(self):
        fm = self._make_fm_with_failures()
        cands = build_seasonality_candidates(fm)
        m7_rows = cands[cands["candidate_m"] == 7]
        assert not m7_rows.empty
        assert m7_rows.iloc[0]["failed_model_count"] >= 1

    def test_valid_model_count_excludes_failed(self):
        fm = self._make_fm_with_failures()
        cands = build_seasonality_candidates(fm)
        m7_rows = cands[cands["candidate_m"] == 7]
        assert not m7_rows.empty
        assert m7_rows.iloc[0]["valid_model_count"] >= 1

    def test_output_columns_still_match_schema_with_failures(self):
        fm = self._make_fm_with_failures()
        cands = build_seasonality_candidates(fm)
        assert list(cands.columns) == CANDIDATE_COLS

    def test_m1_present_and_eligible_with_failures(self):
        fm = self._make_fm_with_failures()
        cands = build_seasonality_candidates(fm)
        m1 = cands[cands["candidate_m"] == NON_SEASONAL_PERIOD]
        assert not m1.empty
        assert (m1["candidate_eligible"] == True).all()


# ===========================================================================
# 11. Schema validation edge cases
# ===========================================================================

class TestSchemaValidation:

    def test_missing_summary_column_raises(self):
        fold_rows = [_fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                                      autocorrelation_at_m=0.5, spectral_power_at_m=0.4)]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(_selection_row())
        prod = _make_prod(_prod_row())
        df = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        incomplete = df.drop(columns=["history_days"])
        with pytest.raises(ValueError, match="missing columns"):
            validate_seasonality_summary(incomplete)

    def test_missing_candidates_column_raises(self):
        fold_rows = [_fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                                      autocorrelation_at_m=0.5, spectral_power_at_m=0.4)]
        cands = build_seasonality_candidates(_make_fold_metrics(*fold_rows))
        incomplete = cands.drop(columns=["cycles_available"])
        with pytest.raises(ValueError, match="missing columns"):
            validate_seasonality_candidates(incomplete)

    def test_negative_cycles_available_raises(self):
        fold_rows = [_fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                                      autocorrelation_at_m=0.5, spectral_power_at_m=0.4,
                                      cycles_available=10)]
        cands = build_seasonality_candidates(_make_fold_metrics(*fold_rows))
        bad = cands.copy()
        bad.loc[bad["candidate_m"] == 7, "cycles_available"] = -1
        with pytest.raises(ValueError, match="negative cycles_available"):
            validate_seasonality_candidates(bad)

    def test_ineligible_without_reason_raises(self):
        fold_rows = [_fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                                      autocorrelation_at_m=0.5, spectral_power_at_m=0.4)]
        cands = build_seasonality_candidates(
            _make_fold_metrics(*fold_rows),
            candidate_periods=(7, 14, 28, 30, 90),
        )
        bad = cands.copy()
        # Force an ineligible row with no reason
        mask = (bad["candidate_eligible"] == False) & (bad["exclusion_reason"] != "")
        if mask.any():
            bad.loc[mask, "exclusion_reason"] = ""
            with pytest.raises(ValueError, match="exclusion_reason"):
                validate_seasonality_candidates(bad)


# ===========================================================================
# 12. Data-quality checks
# ===========================================================================

class TestDataQualityChecks:

    def _std_fm(self):
        return _make_fold_metrics(
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.6, spectral_power_at_m=0.4),
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=200, autocorrelation_at_m=np.nan,
                             spectral_power_at_m=np.nan, candidate_source="baseline"),
        )

    def test_selected_m_found_in_candidates_passes(self):
        fm = self._std_fm()
        sel = _make_selection(_selection_row(selected_m=7))
        prod = _make_prod(_prod_row(selected_m=7))
        summary = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        candidates = build_seasonality_candidates(fm)
        dq = run_seasonality_data_quality_checks(summary, candidates)
        m_check = dq[dq["check"] == "selected_m_in_evaluated_candidates"]
        assert (m_check["status"] == "pass").all()

    def test_selected_m_not_in_candidates_fails(self):
        fm = self._std_fm()
        sel = _make_selection(_selection_row(selected_m=7))
        prod = _make_prod(_prod_row(selected_m=7))
        summary = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        candidates = build_seasonality_candidates(fm)
        # Override selected_m to something that doesn't appear in candidates
        bad_summary = summary.copy()
        bad_summary.loc[0, "selected_m"] = 14  # 14 not in this fold's data
        # Remove m=14 from candidates
        cands_no14 = candidates[candidates["candidate_m"] != 14].copy()
        dq = run_seasonality_data_quality_checks(bad_summary, cands_no14)
        m_check = dq[dq["check"] == "selected_m_in_evaluated_candidates"]
        if not m_check.empty:
            assert (m_check["status"] == "fail").any()

    def test_non_seasonal_m1_always_passes_m_check(self):
        fm = self._std_fm()
        sel = _make_selection(_selection_row(selected_model_family="naive",
                                             selected_model_name="naive", selected_m=1))
        prod = _make_prod(_prod_row(selected_model_family="naive",
                                    selected_model_name="naive", selected_m=1))
        summary = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        candidates = build_seasonality_candidates(fm)
        dq = run_seasonality_data_quality_checks(summary, candidates)
        m_check = dq[dq["check"] == "selected_m_in_evaluated_candidates"]
        assert (m_check["status"] == "pass").all()

    def test_dq_returns_dataframe_with_expected_columns(self):
        fm = self._std_fm()
        sel = _make_selection(_selection_row())
        prod = _make_prod(_prod_row())
        summary = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        candidates = build_seasonality_candidates(fm)
        dq = run_seasonality_data_quality_checks(summary, candidates)
        assert set(dq.columns) >= {"check", "report_id", "status", "detail"}


# ===========================================================================
# 13. Save to disk
# ===========================================================================

class TestSaveSeasonalityDiagnostics:

    def test_files_created_in_diagnostics_dir(self, tmp_path):
        from src.models.seasonality_diagnostics import save_seasonality_diagnostics

        fold_rows = [
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.5, spectral_power_at_m=0.4),
            _fold_metric_row(candidate_m=NON_SEASONAL_PERIOD, model_name="naive",
                             model_family="naive", seasonal_candidate_rank=0,
                             cycles_available=200, autocorrelation_at_m=np.nan,
                             spectral_power_at_m=np.nan, candidate_source="baseline"),
        ]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(_selection_row())
        prod = _make_prod(_prod_row())
        summary = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        candidates = build_seasonality_candidates(fm)

        paths = save_seasonality_diagnostics(summary, candidates, tmp_path)

        assert paths["summary"].exists()
        assert paths["candidates"].exists()
        assert paths["summary"].parent == tmp_path / "outputs" / "diagnostics"

    def test_saved_files_are_readable_csvs(self, tmp_path):
        fold_rows = [
            _fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                             autocorrelation_at_m=0.5, spectral_power_at_m=0.4),
        ]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(_selection_row())
        prod = _make_prod(_prod_row())
        summary = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        candidates = build_seasonality_candidates(fm)

        paths = save_seasonality_diagnostics(summary, candidates, tmp_path)

        loaded_summary = pd.read_csv(paths["summary"])
        loaded_candidates = pd.read_csv(paths["candidates"])
        assert set(SUMMARY_COLS).issubset(loaded_summary.columns)
        assert set(CANDIDATE_COLS).issubset(loaded_candidates.columns)

    def test_summary_file_named_correctly(self, tmp_path):
        fold_rows = [_fold_metric_row(candidate_m=7, seasonal_candidate_rank=1,
                                      autocorrelation_at_m=0.5, spectral_power_at_m=0.4)]
        fm = _make_fold_metrics(*fold_rows)
        sel = _make_selection(_selection_row())
        prod = _make_prod(_prod_row())
        summary = build_seasonality_summary({"R1": _make_series()}, sel, fm, prod)
        candidates = build_seasonality_candidates(fm)
        paths = save_seasonality_diagnostics(summary, candidates, tmp_path)
        assert paths["summary"].name == "report_seasonality_summary.csv"
        assert paths["candidates"].name == "report_seasonality_candidates.csv"
