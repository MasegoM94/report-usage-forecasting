"""Tests for src.models.model_summary.summarise_model_performance.

All tests construct fold_metrics DataFrames directly — no pipeline execution.
This keeps tests fast, isolated, and fully deterministic.

Fixture conventions
-------------------
_fold_metrics(**kwargs) builds a minimal valid fold_metrics row.
_make_metrics(rows) builds a complete fold_metrics DataFrame from a list of
row dicts, filling missing optional fields with sensible defaults.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.models.model_summary import (
    MASE_TIE_TOLERANCE,
    _OUTPUT_COLS,
    summarise_model_performance,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REPORT = "R_001"
_REPORT_B = "R_002"
_CUTOFF = pd.Timestamp("2023-06-01")


def _row(
    report_id: str = _REPORT,
    fold_number: int = 1,
    model_name: str = "naive",
    mae: float = 1.0,
    rmse: float = 1.2,
    wape: float = 0.3,
    mase: float = 0.8,
    bias: float = 0.1,
    interval_coverage: float = np.nan,
    mean_interval_width: float = np.nan,
    fit_status: str = "ok",
    error_message: str | None = None,
) -> dict:
    return {
        "report_id": report_id,
        "fold_number": fold_number,
        "cutoff_date": _CUTOFF + pd.Timedelta(days=28 * (fold_number - 1)),
        "model_name": model_name,
        "mae": mae,
        "rmse": rmse,
        "wape": wape,
        "mase": mase,
        "bias": bias,
        "interval_coverage": interval_coverage,
        "mean_interval_width": mean_interval_width,
        "fit_status": fit_status,
        "error_message": error_message,
    }


def _make(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 1. Complete model results — all folds succeed
# ---------------------------------------------------------------------------

class TestCompleteModelResults:
    """All (report_id, fold_number, model_name) combinations succeed."""

    @pytest.fixture
    def fm(self):
        # 3 folds × 2 models; naive wins fold 1, snaive wins folds 2 and 3
        return _make([
            _row(fold_number=1, model_name="naive",          mase=0.70),
            _row(fold_number=1, model_name="seasonal_naive", mase=0.80),
            _row(fold_number=2, model_name="naive",          mase=0.90),
            _row(fold_number=2, model_name="seasonal_naive", mase=0.75),
            _row(fold_number=3, model_name="naive",          mase=0.85),
            _row(fold_number=3, model_name="seasonal_naive", mase=0.72),
        ])

    def test_one_row_per_model(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        assert len(result) == 2
        assert set(result["model_name"]) == {"naive", "seasonal_naive"}

    def test_output_columns_complete(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        missing = set(_OUTPUT_COLS) - set(result.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_valid_folds_count(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        for _, row in result.iterrows():
            assert row["valid_folds"] == 3
        assert (result["failed_folds"] == 0).all()

    def test_has_sufficient_folds_true_at_threshold(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=3)
        assert result["has_sufficient_folds"].all()

    def test_has_sufficient_folds_false_above_threshold(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=4)
        assert (~result["has_sufficient_folds"]).all()

    def test_fold_win_counts(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        naive = result.loc[result["model_name"] == "naive", "fold_win_count"].iloc[0]
        snaive = result.loc[result["model_name"] == "seasonal_naive", "fold_win_count"].iloc[0]
        assert naive == 1    # wins fold 1 (mase=0.70 < 0.80)
        assert snaive == 2   # wins folds 2 and 3

    def test_fold_win_rate(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        naive_rate = result.loc[result["model_name"] == "naive", "fold_win_rate"].iloc[0]
        snaive_rate = result.loc[result["model_name"] == "seasonal_naive", "fold_win_rate"].iloc[0]
        assert naive_rate == pytest.approx(1 / 3)
        assert snaive_rate == pytest.approx(2 / 3)

    def test_sorted_by_mean_mase_ascending(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        mases = result["mean_mase"].tolist()
        assert mases == sorted(mases)


# ---------------------------------------------------------------------------
# 2. One failed fold
# ---------------------------------------------------------------------------

class TestOneFailedFold:
    @pytest.fixture
    def fm(self):
        # naive: fold 1 ok, fold 2 ok, fold 3 failed
        # snaive: all ok
        return _make([
            _row(fold_number=1, model_name="naive",          mase=0.80),
            _row(fold_number=1, model_name="seasonal_naive", mase=0.75),
            _row(fold_number=2, model_name="naive",          mase=0.70),
            _row(fold_number=2, model_name="seasonal_naive", mase=0.72),
            _row(fold_number=3, model_name="naive",
                 mase=np.nan, mae=np.nan, rmse=np.nan, wape=np.nan, bias=np.nan,
                 fit_status="failed", error_message="RuntimeError: oops"),
            _row(fold_number=3, model_name="seasonal_naive", mase=0.78),
        ])

    def test_failed_fold_counted(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        naive = result.loc[result["model_name"] == "naive"].iloc[0]
        assert naive["failed_folds"] == 1
        assert naive["valid_folds"] == 2

    def test_snaive_has_no_failures(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        snaive = result.loc[result["model_name"] == "seasonal_naive"].iloc[0]
        assert snaive["failed_folds"] == 0
        assert snaive["valid_folds"] == 3

    def test_failed_fold_excluded_from_aggregates(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        naive = result.loc[result["model_name"] == "naive"].iloc[0]
        # mean_mase should be computed only over folds 1 and 2
        expected = np.mean([0.80, 0.70])
        assert naive["mean_mase"] == pytest.approx(expected)

    def test_failed_model_not_eligible_for_fold_win(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        naive = result.loc[result["model_name"] == "naive"].iloc[0]
        # naive only competes in folds 1 and 2
        # fold 1: naive 0.80 > snaive 0.75 → snaive wins
        # fold 2: naive 0.70 < snaive 0.72 → naive wins
        assert naive["fold_win_count"] == 1

    def test_failed_folds_not_in_win_denominator(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        naive = result.loc[result["model_name"] == "naive"].iloc[0]
        # fold_win_rate = 1 win / 2 valid folds (fold 3 not counted)
        assert naive["fold_win_rate"] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# 3. Insufficient valid folds
# ---------------------------------------------------------------------------

class TestInsufficientValidFolds:
    @pytest.fixture
    def fm(self):
        # naive: only 2 valid folds; snaive: 4 valid folds
        return _make([
            _row(fold_number=1, model_name="naive",          mase=0.80),
            _row(fold_number=1, model_name="seasonal_naive", mase=0.75),
            _row(fold_number=2, model_name="naive",          mase=0.70),
            _row(fold_number=2, model_name="seasonal_naive", mase=0.72),
            _row(fold_number=3, model_name="naive",
                 mase=np.nan, fit_status="failed", mae=np.nan, rmse=np.nan,
                 wape=np.nan, bias=np.nan),
            _row(fold_number=3, model_name="seasonal_naive", mase=0.78),
            _row(fold_number=4, model_name="naive",
                 mase=np.nan, fit_status="failed", mae=np.nan, rmse=np.nan,
                 wape=np.nan, bias=np.nan),
            _row(fold_number=4, model_name="seasonal_naive", mase=0.81),
        ])

    def test_insufficient_flagged(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=3)
        naive = result.loc[result["model_name"] == "naive"].iloc[0]
        assert not naive["has_sufficient_folds"]

    def test_sufficient_not_flagged(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=3)
        snaive = result.loc[result["model_name"] == "seasonal_naive"].iloc[0]
        assert snaive["has_sufficient_folds"]

    def test_insufficient_model_still_has_aggregates(self, fm):
        """Aggregates are still computed for the available valid folds."""
        result = summarise_model_performance(fm, min_valid_folds=3)
        naive = result.loc[result["model_name"] == "naive"].iloc[0]
        assert pd.notna(naive["mean_mase"])
        assert naive["mean_mase"] == pytest.approx(np.mean([0.80, 0.70]))

    def test_both_models_present_in_output(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=3)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# 4. Tied fold winners
# ---------------------------------------------------------------------------

class TestTiedFoldWinners:
    """Two models whose MASE values differ by ≤ MASE_TIE_TOLERANCE both win."""

    @pytest.fixture
    def fm(self):
        # Fold 1: naive=0.800, snaive=0.800 + tolerance/2 → tie
        # Fold 2: naive=0.900, snaive=0.700 → snaive clear winner
        delta = MASE_TIE_TOLERANCE / 2
        return _make([
            _row(fold_number=1, model_name="naive",          mase=0.800),
            _row(fold_number=1, model_name="seasonal_naive", mase=0.800 + delta),
            _row(fold_number=2, model_name="naive",          mase=0.900),
            _row(fold_number=2, model_name="seasonal_naive", mase=0.700),
        ])

    def test_tied_fold_both_credited(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        naive_wins = result.loc[result["model_name"] == "naive", "fold_win_count"].iloc[0]
        snaive_wins = result.loc[result["model_name"] == "seasonal_naive", "fold_win_count"].iloc[0]
        # Fold 1: both win (tie); fold 2: only snaive wins
        assert naive_wins == 1
        assert snaive_wins == 2

    def test_outside_tolerance_not_credited(self):
        """A MASE difference just above tolerance does NOT constitute a tie."""
        delta = MASE_TIE_TOLERANCE * 1.5   # > tolerance
        fm = _make([
            _row(fold_number=1, model_name="naive",          mase=0.800),
            _row(fold_number=1, model_name="seasonal_naive", mase=0.800 + delta),
        ])
        result = summarise_model_performance(fm, min_valid_folds=1)
        naive_wins = result.loc[result["model_name"] == "naive", "fold_win_count"].iloc[0]
        snaive_wins = result.loc[result["model_name"] == "seasonal_naive", "fold_win_count"].iloc[0]
        assert naive_wins == 1
        assert snaive_wins == 0

    def test_tied_win_rate_when_all_folds_tied(self):
        """When every fold is tied, both models have fold_win_rate = 1.0."""
        delta = MASE_TIE_TOLERANCE / 3
        fm = _make([
            _row(fold_number=1, model_name="naive",          mase=0.8),
            _row(fold_number=1, model_name="seasonal_naive", mase=0.8 + delta),
            _row(fold_number=2, model_name="naive",          mase=0.7),
            _row(fold_number=2, model_name="seasonal_naive", mase=0.7 + delta),
        ])
        result = summarise_model_performance(fm, min_valid_folds=1)
        assert all(abs(v - 1.0) < 1e-9 for v in result["fold_win_rate"])


# ---------------------------------------------------------------------------
# 5. Correct median and mean calculations
# ---------------------------------------------------------------------------

class TestAggregateCalculations:
    @pytest.fixture
    def fm(self):
        # 5 folds so median and mean are clearly distinguishable
        mase_vals = [0.5, 0.7, 0.9, 1.1, 1.3]
        mae_vals  = [1.0, 2.0, 3.0, 4.0, 5.0]
        wape_vals = [0.1, 0.2, 0.3, 0.4, 0.5]
        bias_vals = [-1.0, 0.5, 0.0, 1.5, -0.5]
        return _make([
            _row(fold_number=i + 1, model_name="naive",
                 mase=mase_vals[i], mae=mae_vals[i],
                 wape=wape_vals[i], bias=bias_vals[i])
            for i in range(5)
        ])

    def test_mean_mase(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        row = result.loc[result["model_name"] == "naive"].iloc[0]
        assert row["mean_mase"] == pytest.approx(np.mean([0.5, 0.7, 0.9, 1.1, 1.3]))

    def test_median_mase(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        row = result.loc[result["model_name"] == "naive"].iloc[0]
        assert row["median_mase"] == pytest.approx(np.median([0.5, 0.7, 0.9, 1.1, 1.3]))

    def test_mase_std(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        row = result.loc[result["model_name"] == "naive"].iloc[0]
        assert row["mase_std"] == pytest.approx(np.std([0.5, 0.7, 0.9, 1.1, 1.3], ddof=1))

    def test_mean_mae(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        row = result.loc[result["model_name"] == "naive"].iloc[0]
        assert row["mean_mae"] == pytest.approx(np.mean([1.0, 2.0, 3.0, 4.0, 5.0]))

    def test_mean_wape(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        row = result.loc[result["model_name"] == "naive"].iloc[0]
        assert row["mean_wape"] == pytest.approx(np.mean([0.1, 0.2, 0.3, 0.4, 0.5]))

    def test_mean_bias(self, fm):
        bias_vals = [-1.0, 0.5, 0.0, 1.5, -0.5]
        result = summarise_model_performance(fm, min_valid_folds=1)
        row = result.loc[result["model_name"] == "naive"].iloc[0]
        assert row["mean_bias"] == pytest.approx(np.mean(bias_vals))

    def test_absolute_mean_bias(self, fm):
        bias_vals = [-1.0, 0.5, 0.0, 1.5, -0.5]
        result = summarise_model_performance(fm, min_valid_folds=1)
        row = result.loc[result["model_name"] == "naive"].iloc[0]
        assert row["absolute_mean_bias"] == pytest.approx(abs(np.mean(bias_vals)))

    def test_absolute_mean_bias_is_non_negative(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        assert (result["absolute_mean_bias"].dropna() >= 0).all()

    def test_valid_folds_count_is_five(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        assert result["valid_folds"].iloc[0] == 5


# ---------------------------------------------------------------------------
# 6. Fold-win rates
# ---------------------------------------------------------------------------

class TestFoldWinRates:
    def test_rate_is_wins_over_valid_folds(self):
        # naive wins 2 of 4 valid folds → rate = 0.5
        fm = _make([
            _row(fold_number=1, model_name="naive",          mase=0.60),
            _row(fold_number=1, model_name="seasonal_naive", mase=0.80),
            _row(fold_number=2, model_name="naive",          mase=0.90),
            _row(fold_number=2, model_name="seasonal_naive", mase=0.70),
            _row(fold_number=3, model_name="naive",          mase=0.55),
            _row(fold_number=3, model_name="seasonal_naive", mase=0.80),
            _row(fold_number=4, model_name="naive",          mase=0.95),
            _row(fold_number=4, model_name="seasonal_naive", mase=0.65),
        ])
        result = summarise_model_performance(fm, min_valid_folds=1)
        naive = result.loc[result["model_name"] == "naive"].iloc[0]
        snaive = result.loc[result["model_name"] == "seasonal_naive"].iloc[0]
        assert naive["fold_win_count"] == 2
        assert naive["fold_win_rate"] == pytest.approx(0.5)
        assert snaive["fold_win_count"] == 2
        assert snaive["fold_win_rate"] == pytest.approx(0.5)

    def test_win_rate_nan_when_no_valid_folds(self):
        """A model with zero valid folds should have fold_win_rate = NaN."""
        fm = _make([
            _row(fold_number=1, model_name="always_fails",
                 mase=np.nan, fit_status="failed",
                 mae=np.nan, rmse=np.nan, wape=np.nan, bias=np.nan),
        ])
        result = summarise_model_performance(fm, min_valid_folds=1)
        row = result.loc[result["model_name"] == "always_fails"].iloc[0]
        assert pd.isna(row["fold_win_rate"])

    def test_win_count_zero_when_no_valid_folds(self):
        fm = _make([
            _row(fold_number=1, model_name="always_fails",
                 mase=np.nan, fit_status="failed",
                 mae=np.nan, rmse=np.nan, wape=np.nan, bias=np.nan),
        ])
        result = summarise_model_performance(fm, min_valid_folds=1)
        row = result.loc[result["model_name"] == "always_fails"].iloc[0]
        assert row["fold_win_count"] == 0

    def test_rate_denominates_by_valid_not_total_folds(self):
        """fold_win_rate uses valid folds (not total folds including failed) as denominator."""
        # naive: 3 folds total, 1 failed → 2 valid; wins 1
        # → rate should be 1/2, not 1/3
        fm = _make([
            _row(fold_number=1, model_name="naive",          mase=0.60),
            _row(fold_number=1, model_name="seasonal_naive", mase=0.80),
            _row(fold_number=2, model_name="naive",          mase=0.90),
            _row(fold_number=2, model_name="seasonal_naive", mase=0.70),
            _row(fold_number=3, model_name="naive",
                 mase=np.nan, fit_status="failed",
                 mae=np.nan, rmse=np.nan, wape=np.nan, bias=np.nan),
            _row(fold_number=3, model_name="seasonal_naive", mase=0.65),
        ])
        result = summarise_model_performance(fm, min_valid_folds=1)
        naive = result.loc[result["model_name"] == "naive"].iloc[0]
        assert naive["fold_win_rate"] == pytest.approx(0.5)   # 1 win / 2 valid folds


# ---------------------------------------------------------------------------
# 7. Interval coverage aggregation
# ---------------------------------------------------------------------------

class TestIntervalCoverageAggregation:
    def test_mean_coverage_averaged_over_valid_folds(self):
        fm = _make([
            _row(fold_number=1, model_name="sarima",
                 mase=0.70, interval_coverage=0.90, mean_interval_width=5.0),
            _row(fold_number=2, model_name="sarima",
                 mase=0.75, interval_coverage=0.95, mean_interval_width=6.0),
            _row(fold_number=3, model_name="sarima",
                 mase=0.80, interval_coverage=0.88, mean_interval_width=4.5),
        ])
        result = summarise_model_performance(fm, min_valid_folds=1)
        row = result.iloc[0]
        assert row["mean_interval_coverage"] == pytest.approx(np.mean([0.90, 0.95, 0.88]))
        assert row["mean_interval_width"] == pytest.approx(np.mean([5.0, 6.0, 4.5]))

    def test_baseline_nan_coverage_stays_nan(self):
        fm = _make([
            _row(fold_number=1, mase=0.70, interval_coverage=np.nan, mean_interval_width=np.nan),
            _row(fold_number=2, mase=0.75, interval_coverage=np.nan, mean_interval_width=np.nan),
        ])
        result = summarise_model_performance(fm, min_valid_folds=1)
        assert pd.isna(result["mean_interval_coverage"].iloc[0])
        assert pd.isna(result["mean_interval_width"].iloc[0])


# ---------------------------------------------------------------------------
# 8. Multi-report output
# ---------------------------------------------------------------------------

class TestMultiReport:
    @pytest.fixture
    def fm(self):
        rows = []
        for report_id, mase_a, mase_b in [
            (_REPORT,   0.70, 0.80),
            (_REPORT_B, 0.60, 0.65),
        ]:
            for fold in range(1, 4):
                rows.append(_row(report_id=report_id, fold_number=fold,
                                 model_name="naive", mase=mase_a))
                rows.append(_row(report_id=report_id, fold_number=fold,
                                 model_name="seasonal_naive", mase=mase_b))
        return _make(rows)

    def test_two_rows_per_report(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        for report_id in [_REPORT, _REPORT_B]:
            n = len(result[result["report_id"] == report_id])
            assert n == 2

    def test_reports_do_not_bleed(self, fm):
        result = summarise_model_performance(fm, min_valid_folds=1)
        r1 = result[result["report_id"] == _REPORT]
        r2 = result[result["report_id"] == _REPORT_B]
        # naive wins all folds in both reports; check win counts independently
        assert r1.loc[r1["model_name"] == "naive", "fold_win_count"].iloc[0] == 3
        assert r2.loc[r2["model_name"] == "naive", "fold_win_count"].iloc[0] == 3


# ---------------------------------------------------------------------------
# 9. Input validation and edge cases
# ---------------------------------------------------------------------------

class TestInputValidation:
    def test_missing_required_column_raises(self):
        fm = _make([_row()]).drop(columns=["mase"])
        with pytest.raises(ValueError, match="missing required columns"):
            summarise_model_performance(fm)

    def test_empty_dataframe_returns_empty(self):
        fm = pd.DataFrame(columns=list(_make([_row()]).columns))
        result = summarise_model_performance(fm)
        assert result.empty
        assert list(result.columns) == _OUTPUT_COLS

    def test_single_model_single_fold(self):
        fm = _make([_row(mase=0.85)])
        result = summarise_model_performance(fm, min_valid_folds=1)
        assert len(result) == 1
        assert result.iloc[0]["valid_folds"] == 1
        assert result.iloc[0]["fold_win_count"] == 1
        assert result.iloc[0]["fold_win_rate"] == pytest.approx(1.0)

    def test_all_folds_failed_produces_row_with_nan_metrics(self):
        fm = _make([
            _row(fold_number=i, mase=np.nan, fit_status="failed",
                 mae=np.nan, rmse=np.nan, wape=np.nan, bias=np.nan)
            for i in range(1, 4)
        ])
        result = summarise_model_performance(fm, min_valid_folds=1)
        assert len(result) == 1
        row = result.iloc[0]
        assert row["failed_folds"] == 3
        assert row["valid_folds"] == 0
        assert pd.isna(row["mean_mase"])
        assert row["fold_win_count"] == 0
        assert pd.isna(row["fold_win_rate"])
        assert not row["has_sufficient_folds"]

    def test_output_sorted_by_mean_mase_ascending(self):
        fm = _make([
            _row(fold_number=1, model_name="naive",          mase=1.2),
            _row(fold_number=1, model_name="seasonal_naive", mase=0.6),
            _row(fold_number=1, model_name="moving_average", mase=0.9),
        ])
        result = summarise_model_performance(fm, min_valid_folds=1)
        mases = result["mean_mase"].tolist()
        assert mases == sorted(mases)
        assert result.iloc[0]["model_name"] == "seasonal_naive"

    def test_nan_mean_mase_sorted_last(self):
        fm = _make([
            _row(fold_number=1, model_name="good_model",   mase=0.80),
            _row(fold_number=1, model_name="failed_model",
                 mase=np.nan, fit_status="failed",
                 mae=np.nan, rmse=np.nan, wape=np.nan, bias=np.nan),
        ])
        result = summarise_model_performance(fm, min_valid_folds=1)
        assert result.iloc[-1]["model_name"] == "failed_model"

    def test_mase_nan_but_fit_ok_excluded_from_valid_count(self):
        """A fold where fit succeeded but MASE is NaN (e.g. zero denominator)
        must not be counted in valid_folds."""
        fm = _make([
            _row(fold_number=1, model_name="naive", mase=0.80),
            _row(fold_number=2, model_name="naive", mase=np.nan, fit_status="ok"),
        ])
        result = summarise_model_performance(fm, min_valid_folds=1)
        row = result.iloc[0]
        # Only fold 1 has a finite MASE → valid_folds should be 1
        assert row["valid_folds"] == 1
        assert row["failed_folds"] == 0
