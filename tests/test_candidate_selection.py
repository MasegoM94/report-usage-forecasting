"""Tests for joint model-family + seasonal-period summarisation and selection.

Covers:
- summarise_candidate_performance: grouping, column presence, fold-win logic
- select_candidate_models: weekly winner, approximate monthly winner,
  non-seasonal winner, practical tie, insufficient quarterly folds,
  excessive bias, candidate model failures, deterministic selection reasons
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.config.forecasting import MIN_VALID_FOLDS, NON_SEASONAL_PERIOD
from src.models.model_summary import (
    MASE_TIE_TOLERANCE,
    _CANDIDATE_OUTPUT_COLS,
    _CANDIDATE_REQUIRED_INPUT_COLS,
    summarise_candidate_performance,
)
from src.models.selection import (
    MAX_BIAS_RATIO,
    MODEL_COMPLEXITY,
    RELATIVE_IMPROVEMENT_TOLERANCE,
    _CANDIDATE_OUTPUT_COLS as SELECTION_OUTPUT_COLS,
    _CANDIDATE_REQUIRED_INPUT_COLS as SELECTION_REQUIRED_COLS,
    select_candidate_models,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REPORT = "RPT_001"


def _fold_row(
    *,
    report_id: str = _REPORT,
    fold_number: int = 1,
    model_name: str = "naive",
    model_family: str = "naive",
    candidate_m: int = NON_SEASONAL_PERIOD,
    mae: float = 10.0,
    rmse: float = 12.0,
    wape: float = 0.30,
    mase_lag1: float = 0.80,
    bias: float = 0.5,
    fit_status: str = "success",
) -> dict:
    return {
        "report_id": report_id,
        "fold_number": fold_number,
        "model_name": model_name,
        "model_family": model_family,
        "candidate_m": candidate_m,
        "mae": mae,
        "rmse": rmse,
        "wape": wape,
        "mase_lag1": mase_lag1,
        "bias": bias,
        "fit_status": fit_status,
    }


def _make_folds(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


def _summary_row(
    *,
    report_id: str = _REPORT,
    model_family: str = "naive",
    model_name: str = "naive",
    candidate_m: int = NON_SEASONAL_PERIOD,
    candidate_fold_count: int = 3,
    valid_folds: int = 3,
    failed_folds: int = 0,
    has_sufficient_folds: bool = True,
    median_mase: float = 0.80,
    mean_mase: float = 0.82,
    mase_std: float = 0.05,
    mean_wape: float = 0.30,
    mean_mae: float = 10.0,
    mean_rmse: float = 12.0,
    mean_bias: float = 0.5,
    absolute_mean_bias: float = 0.5,
    fold_win_count: int = 1,
    fold_win_rate: float = 0.33,
    mean_interval_coverage: float = np.nan,
    mean_interval_width: float = np.nan,
) -> dict:
    return {
        "report_id": report_id,
        "model_family": model_family,
        "model_name": model_name,
        "candidate_m": candidate_m,
        "candidate_fold_count": candidate_fold_count,
        "valid_folds": valid_folds,
        "failed_folds": failed_folds,
        "has_sufficient_folds": has_sufficient_folds,
        "median_mase": median_mase,
        "mean_mase": mean_mase,
        "mase_std": mase_std,
        "mean_wape": mean_wape,
        "mean_mae": mean_mae,
        "mean_rmse": mean_rmse,
        "mean_bias": mean_bias,
        "absolute_mean_bias": absolute_mean_bias,
        "fold_win_count": fold_win_count,
        "fold_win_rate": fold_win_rate,
        "mean_interval_coverage": mean_interval_coverage,
        "mean_interval_width": mean_interval_width,
    }


def _make_summary(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


# ---------------------------------------------------------------------------
# summarise_candidate_performance tests
# ---------------------------------------------------------------------------


class TestSummariseCandidatePerformance:
    def test_output_columns_present(self):
        rows = []
        for fold in range(1, 4):
            rows.append(_fold_row(fold_number=fold, model_name="naive", model_family="naive", candidate_m=1))
            rows.append(_fold_row(
                fold_number=fold,
                model_name="seasonal_naive_m7",
                model_family="seasonal_naive",
                candidate_m=7,
                mase_lag1=0.70,
            ))
        df = summarise_candidate_performance(_make_folds(*rows))
        assert set(_CANDIDATE_OUTPUT_COLS).issubset(set(df.columns))

    def test_one_row_per_family_m_pair(self):
        rows = []
        for fold in range(1, 4):
            rows.append(_fold_row(fold_number=fold, model_family="naive", candidate_m=1))
            rows.append(_fold_row(
                fold_number=fold,
                model_name="seasonal_naive_m7",
                model_family="seasonal_naive",
                candidate_m=7,
                mase_lag1=0.65,
            ))
        df = summarise_candidate_performance(_make_folds(*rows))
        assert len(df) == 2

    def test_median_mase_computed_from_mase_lag1(self):
        mase_values = [0.80, 0.90, 1.00]
        rows = [
            _fold_row(fold_number=i + 1, mase_lag1=v)
            for i, v in enumerate(mase_values)
        ]
        df = summarise_candidate_performance(_make_folds(*rows))
        assert df["median_mase"].iloc[0] == pytest.approx(np.median(mase_values))

    def test_failed_folds_excluded_from_aggregates(self):
        rows = [
            _fold_row(fold_number=1, mase_lag1=0.80),
            _fold_row(fold_number=2, mase_lag1=0.90),
            _fold_row(fold_number=3, fit_status="failed", mase_lag1=np.nan),
        ]
        df = summarise_candidate_performance(_make_folds(*rows))
        assert df["valid_folds"].iloc[0] == 2
        assert df["failed_folds"].iloc[0] == 1
        assert df["median_mase"].iloc[0] == pytest.approx(0.85)

    def test_candidate_fold_count_includes_failed(self):
        rows = [
            _fold_row(fold_number=1),
            _fold_row(fold_number=2),
            _fold_row(fold_number=3, fit_status="failed", mase_lag1=np.nan),
        ]
        df = summarise_candidate_performance(_make_folds(*rows))
        assert df["candidate_fold_count"].iloc[0] == 3

    def test_has_sufficient_folds_flag(self):
        rows = [_fold_row(fold_number=i + 1) for i in range(MIN_VALID_FOLDS - 1)]
        df = summarise_candidate_performance(_make_folds(*rows), min_valid_folds=MIN_VALID_FOLDS)
        assert not df["has_sufficient_folds"].iloc[0]

    def test_fold_winners_tracked_across_all_candidates(self):
        """The winning candidate per fold is the one with the lowest mase_lag1
        globally (across all families and m values), not just within its group."""
        rows = []
        for fold in range(1, 4):
            # seasonal_naive_m7 consistently wins each fold
            rows.append(_fold_row(
                fold_number=fold, model_name="naive", model_family="naive",
                candidate_m=1, mase_lag1=1.0,
            ))
            rows.append(_fold_row(
                fold_number=fold, model_name="seasonal_naive_m7",
                model_family="seasonal_naive", candidate_m=7, mase_lag1=0.5,
            ))
        df = summarise_candidate_performance(_make_folds(*rows))
        sn_row = df[df["model_family"] == "seasonal_naive"].iloc[0]
        naive_row = df[df["model_family"] == "naive"].iloc[0]
        assert sn_row["fold_win_count"] == 3
        assert naive_row["fold_win_count"] == 0

    def test_missing_required_column_raises(self):
        rows = [_fold_row()]
        df = _make_folds(*rows).drop(columns=["model_family"])
        with pytest.raises(ValueError, match="missing required columns"):
            summarise_candidate_performance(df)

    def test_empty_input_returns_empty_dataframe(self):
        # Empty DataFrame must still have the required columns for validation to pass.
        empty = pd.DataFrame(columns=list(_CANDIDATE_REQUIRED_INPUT_COLS))
        df = summarise_candidate_performance(empty)
        assert df.empty
        assert set(_CANDIDATE_OUTPUT_COLS).issubset(set(df.columns))


# ---------------------------------------------------------------------------
# select_candidate_models tests
# ---------------------------------------------------------------------------


class TestWeeklyWinner:
    """SARIMA m=7 clearly beats non-seasonal and seasonal-naive m=7."""

    def _build(self):
        return _make_summary(
            _summary_row(model_family="naive", model_name="naive", candidate_m=1, median_mase=1.20, mean_mae=10.0, mean_bias=0.5, absolute_mean_bias=0.5),
            _summary_row(model_family="moving_average", model_name="moving_average", candidate_m=1, median_mase=1.10, mean_mae=10.0, mean_bias=0.3, absolute_mean_bias=0.3),
            _summary_row(model_family="seasonal_naive", model_name="seasonal_naive_m7", candidate_m=7, median_mase=0.90, mean_mae=9.0, mean_bias=0.2, absolute_mean_bias=0.2),
            _summary_row(model_family="auto_arima", model_name="auto_arima_m7", candidate_m=7, median_mase=0.65, mean_mae=8.0, mean_bias=0.1, absolute_mean_bias=0.1, fold_win_count=3, fold_win_rate=1.0),
        )

    def test_selected_family_and_m(self):
        result = select_candidate_models(self._build())
        row = result.iloc[0]
        assert row["selected_model_family"] == "auto_arima"
        assert row["selected_m"] == 7

    def test_selected_model_name(self):
        result = select_candidate_models(self._build())
        assert result.iloc[0]["selected_model_name"] == "auto_arima_m7"

    def test_selection_status(self):
        result = select_candidate_models(self._build())
        assert result.iloc[0]["selection_status"] == "selected"

    def test_seasonal_naive_benchmark_populated(self):
        result = select_candidate_models(self._build())
        # seasonal naive at m=7 is 0.90
        assert result.iloc[0]["seasonal_naive_median_mase"] == pytest.approx(0.90)

    def test_improvement_vs_seasonal_naive_positive(self):
        result = select_candidate_models(self._build())
        pct = result.iloc[0]["improvement_vs_seasonal_naive_pct"]
        assert pct > 0  # SARIMA m=7 beats seasonal naive m=7

    def test_reason_mentions_sarima(self):
        result = select_candidate_models(self._build())
        assert "SARIMA m=7" in result.iloc[0]["selection_reason"]

    def test_output_columns_complete(self):
        result = select_candidate_models(self._build())
        assert list(result.columns) == SELECTION_OUTPUT_COLS


class TestApproximateMonthlyWinner:
    """Seasonal ETS m=28 beats all non-seasonal and weekly candidates."""

    def _build(self):
        return _make_summary(
            _summary_row(model_family="naive", model_name="naive", candidate_m=1, median_mase=1.30, mean_mae=10.0, mean_bias=0.5, absolute_mean_bias=0.5),
            _summary_row(model_family="seasonal_naive", model_name="seasonal_naive_m7", candidate_m=7, median_mase=1.00, mean_mae=9.0, mean_bias=0.3, absolute_mean_bias=0.3),
            _summary_row(model_family="seasonal_naive", model_name="seasonal_naive_m28", candidate_m=28, median_mase=0.85, mean_mae=8.5, mean_bias=0.2, absolute_mean_bias=0.2),
            _summary_row(model_family="ets", model_name="ets_m28", candidate_m=28, median_mase=0.65, mean_mae=8.0, mean_bias=0.1, absolute_mean_bias=0.1, fold_win_count=3, fold_win_rate=1.0),
        )

    def test_selected_family_and_m(self):
        result = select_candidate_models(self._build())
        row = result.iloc[0]
        assert row["selected_model_family"] == "ets"
        assert row["selected_m"] == 28

    def test_seasonal_naive_benchmark_is_m28(self):
        result = select_candidate_models(self._build())
        # seasonal naive at same m=28 is 0.85
        assert result.iloc[0]["seasonal_naive_median_mase"] == pytest.approx(0.85)

    def test_reason_mentions_ets_m28(self):
        result = select_candidate_models(self._build())
        assert "m=28" in result.iloc[0]["selection_reason"]


class TestNonSeasonalWinner:
    """Naive non-seasonal beats all seasonal candidates."""

    def _build(self):
        return _make_summary(
            _summary_row(model_family="naive", model_name="naive", candidate_m=1, median_mase=0.65, mean_mae=10.0, mean_bias=0.5, absolute_mean_bias=0.5, fold_win_count=3, fold_win_rate=1.0),
            _summary_row(model_family="seasonal_naive", model_name="seasonal_naive_m7", candidate_m=7, median_mase=1.10, mean_mae=10.0, mean_bias=0.3, absolute_mean_bias=0.3),
            _summary_row(model_family="auto_arima", model_name="auto_arima_m7", candidate_m=7, median_mase=0.95, mean_mae=9.0, mean_bias=0.2, absolute_mean_bias=0.2),
        )

    def test_selected_is_non_seasonal(self):
        result = select_candidate_models(self._build())
        row = result.iloc[0]
        assert row["selected_m"] == NON_SEASONAL_PERIOD
        assert row["selected_model_family"] == "naive"

    def test_seasonal_naive_benchmark_is_nan_for_non_seasonal(self):
        result = select_candidate_models(self._build())
        # selected m=1 — no seasonal naive at m=1
        assert pd.isna(result.iloc[0]["seasonal_naive_median_mase"])

    def test_reason_mentions_seasonal_candidates_did_not_improve(self):
        result = select_candidate_models(self._build())
        reason = result.iloc[0]["selection_reason"]
        assert "seasonal candidates did not improve" in reason

    def test_reason_includes_best_seasonal_mase(self):
        result = select_candidate_models(self._build())
        reason = result.iloc[0]["selection_reason"]
        # Best seasonal is auto_arima_m7 at 0.95
        assert "0.950" in reason


class TestPracticalTie:
    """When candidates are within tolerance, simpler / shorter-period wins."""

    def test_simpler_family_retained_in_tie(self):
        """ARIMA m=7 is within 5 % of seasonal_naive m=7 → seasonal_naive retained."""
        sn_mase = 0.90
        arima_mase = sn_mase * (1.0 - RELATIVE_IMPROVEMENT_TOLERANCE / 2)
        summary = _make_summary(
            _summary_row(model_family="seasonal_naive", model_name="seasonal_naive_m7", candidate_m=7, median_mase=sn_mase, mean_mae=9.0, mean_bias=0.2, absolute_mean_bias=0.2),
            _summary_row(model_family="auto_arima", model_name="auto_arima_m7", candidate_m=7, median_mase=arima_mase, mean_mae=8.5, mean_bias=0.1, absolute_mean_bias=0.1),
        )
        result = select_candidate_models(summary)
        assert result.iloc[0]["selected_model_family"] == "seasonal_naive"

    def test_reason_mentions_tolerance_when_tie_triggered(self):
        sn_mase = 0.90
        arima_mase = sn_mase * (1.0 - RELATIVE_IMPROVEMENT_TOLERANCE / 2)
        summary = _make_summary(
            _summary_row(model_family="seasonal_naive", model_name="seasonal_naive_m7", candidate_m=7, median_mase=sn_mase, mean_mae=9.0, mean_bias=0.2, absolute_mean_bias=0.2),
            _summary_row(model_family="auto_arima", model_name="auto_arima_m7", candidate_m=7, median_mase=arima_mase, mean_mae=8.5, mean_bias=0.1, absolute_mean_bias=0.1),
        )
        result = select_candidate_models(summary)
        reason = result.iloc[0]["selection_reason"]
        assert "retained" in reason
        assert "tolerance" in reason

    def test_shorter_period_preferred_for_same_family(self):
        """seasonal_naive m=28 slightly beats m=7 but within tolerance → m=7 preferred."""
        mase_m7 = 0.90
        mase_m28 = mase_m7 * (1.0 - RELATIVE_IMPROVEMENT_TOLERANCE / 2)
        summary = _make_summary(
            _summary_row(model_family="seasonal_naive", model_name="seasonal_naive_m7", candidate_m=7, median_mase=mase_m7, mean_mae=9.0, mean_bias=0.2, absolute_mean_bias=0.2),
            _summary_row(model_family="seasonal_naive", model_name="seasonal_naive_m28", candidate_m=28, median_mase=mase_m28, mean_mae=8.8, mean_bias=0.2, absolute_mean_bias=0.2),
        )
        result = select_candidate_models(summary)
        assert result.iloc[0]["selected_m"] == 7

    def test_clearly_better_complex_model_beats_simpler(self):
        """When improvement exceeds tolerance, complex model should win."""
        sn_mase = 0.90
        arima_mase = sn_mase * (1.0 - RELATIVE_IMPROVEMENT_TOLERANCE * 2)
        summary = _make_summary(
            _summary_row(model_family="seasonal_naive", model_name="seasonal_naive_m7", candidate_m=7, median_mase=sn_mase, mean_mae=9.0, mean_bias=0.2, absolute_mean_bias=0.2),
            _summary_row(model_family="auto_arima", model_name="auto_arima_m7", candidate_m=7, median_mase=arima_mase, mean_mae=8.5, mean_bias=0.1, absolute_mean_bias=0.1),
        )
        result = select_candidate_models(summary)
        assert result.iloc[0]["selected_model_family"] == "auto_arima"


class TestInsufficientQuarterlyFolds:
    """m=90 candidate has only 1 valid fold — must be excluded."""

    def _build(self):
        return _make_summary(
            _summary_row(
                model_family="seasonal_naive", model_name="seasonal_naive_m7",
                candidate_m=7, valid_folds=3, has_sufficient_folds=True,
                median_mase=0.90, mean_mae=9.0, mean_bias=0.2, absolute_mean_bias=0.2,
            ),
            _summary_row(
                model_family="auto_arima", model_name="auto_arima_m90",
                candidate_m=90, valid_folds=1, has_sufficient_folds=False,
                median_mase=0.50, mean_mae=8.0, mean_bias=0.1, absolute_mean_bias=0.1,
            ),
        )

    def test_m90_not_selected(self):
        result = select_candidate_models(self._build())
        assert result.iloc[0]["selected_m"] != 90

    def test_m7_selected_instead(self):
        result = select_candidate_models(self._build())
        assert result.iloc[0]["selected_m"] == 7

    def test_all_insufficient_gives_no_reliable_model(self):
        summary = _make_summary(
            _summary_row(
                model_family="auto_arima", model_name="auto_arima_m90",
                candidate_m=90, valid_folds=1, has_sufficient_folds=False,
                median_mase=0.50, mean_mae=8.0, mean_bias=0.1, absolute_mean_bias=0.1,
            ),
        )
        result = select_candidate_models(summary)
        assert result.iloc[0]["selection_status"] == "no_reliable_model"
        assert "fewer than" in result.iloc[0]["selection_reason"]


class TestExcessiveBias:
    """Candidate with |bias|/MAE > MAX_BIAS_RATIO must be excluded."""

    def _build(self):
        return _make_summary(
            # Biased candidate: |0.6|/1.0 = 0.60 > 0.50 threshold → excluded
            _summary_row(
                model_family="auto_arima", model_name="auto_arima_m7",
                candidate_m=7, median_mase=0.60,
                mean_mae=1.0, mean_bias=0.6, absolute_mean_bias=0.6,
            ),
            # Unbiased candidate: |0.1|/1.0 = 0.10 → passes
            _summary_row(
                model_family="seasonal_naive", model_name="seasonal_naive_m7",
                candidate_m=7, median_mase=0.90,
                mean_mae=1.0, mean_bias=0.1, absolute_mean_bias=0.1,
            ),
        )

    def test_biased_candidate_not_selected(self):
        result = select_candidate_models(self._build())
        assert result.iloc[0]["selected_model_family"] == "seasonal_naive"

    def test_all_biased_gives_no_reliable_model(self):
        summary = _make_summary(
            _summary_row(
                model_family="auto_arima", model_name="auto_arima_m7",
                candidate_m=7, median_mase=0.60,
                mean_mae=1.0, mean_bias=0.6, absolute_mean_bias=0.6,
            ),
        )
        result = select_candidate_models(summary)
        assert result.iloc[0]["selection_status"] == "no_reliable_model"
        assert "bias guardrail" in result.iloc[0]["selection_reason"]

    def test_bias_guardrail_disabled_selects_biased(self):
        result = select_candidate_models(self._build(), max_bias_ratio=None)
        assert result.iloc[0]["selected_model_family"] == "auto_arima"


class TestCandidateModelFailures:
    """Failed folds do not prevent selection when enough valid folds remain."""

    def test_candidate_with_one_failed_fold_still_eligible(self):
        summary = _make_summary(
            _summary_row(
                model_family="auto_arima", model_name="auto_arima_m7",
                candidate_m=7, valid_folds=3, failed_folds=1,
                candidate_fold_count=4, has_sufficient_folds=True,
                median_mase=0.70, mean_mae=8.0, mean_bias=0.1, absolute_mean_bias=0.1,
            ),
            _summary_row(
                model_family="naive", model_name="naive",
                candidate_m=1, valid_folds=4, failed_folds=0,
                has_sufficient_folds=True,
                median_mase=1.10, mean_mae=10.0, mean_bias=0.5, absolute_mean_bias=0.5,
            ),
        )
        result = select_candidate_models(summary)
        assert result.iloc[0]["selected_model_family"] == "auto_arima"

    def test_all_folds_failed_gives_no_reliable_model(self):
        summary = _make_summary(
            _summary_row(
                model_family="auto_arima", model_name="auto_arima_m7",
                candidate_m=7, valid_folds=0, failed_folds=3,
                has_sufficient_folds=False,
                median_mase=np.nan, mean_mae=np.nan, mean_bias=np.nan, absolute_mean_bias=np.nan,
            ),
        )
        result = select_candidate_models(summary)
        assert result.iloc[0]["selection_status"] == "no_reliable_model"

    def test_fold_counts_propagated_to_output(self):
        summary = _make_summary(
            _summary_row(
                model_family="seasonal_naive", model_name="seasonal_naive_m7",
                candidate_m=7, valid_folds=3, failed_folds=1,
                has_sufficient_folds=True,
                median_mase=0.80, mean_mae=9.0, mean_bias=0.2, absolute_mean_bias=0.2,
            ),
        )
        result = select_candidate_models(summary)
        assert result.iloc[0]["valid_folds"] == 3


class TestDeterministicSelectionReasons:
    """Same input must always yield identical output."""

    def _build(self):
        return _make_summary(
            _summary_row(model_family="naive", model_name="naive", candidate_m=1, median_mase=1.10, mean_mae=10.0, mean_bias=0.5, absolute_mean_bias=0.5),
            _summary_row(model_family="seasonal_naive", model_name="seasonal_naive_m7", candidate_m=7, median_mase=0.90, mean_mae=9.0, mean_bias=0.2, absolute_mean_bias=0.2),
            _summary_row(model_family="auto_arima", model_name="auto_arima_m7", candidate_m=7, median_mase=0.65, mean_mae=8.0, mean_bias=0.1, absolute_mean_bias=0.1, fold_win_count=3, fold_win_rate=1.0),
        )

    def test_identical_results_on_repeated_calls(self):
        r1 = select_candidate_models(self._build())
        r2 = select_candidate_models(self._build())
        pd.testing.assert_frame_equal(r1, r2)

    def test_identical_reason_text_on_repeated_calls(self):
        r1 = select_candidate_models(self._build())
        r2 = select_candidate_models(self._build())
        assert r1.iloc[0]["selection_reason"] == r2.iloc[0]["selection_reason"]

    def test_row_order_is_deterministic_for_multi_report(self):
        summary = _make_summary(
            _summary_row(report_id="B", model_family="naive", model_name="naive", candidate_m=1, median_mase=0.80, mean_mae=10.0, mean_bias=0.5, absolute_mean_bias=0.5),
            _summary_row(report_id="A", model_family="naive", model_name="naive", candidate_m=1, median_mase=0.80, mean_mae=10.0, mean_bias=0.5, absolute_mean_bias=0.5),
        )
        r1 = select_candidate_models(summary)
        r2 = select_candidate_models(summary)
        assert list(r1["report_id"]) == list(r2["report_id"])

    def test_sorted_by_report_id(self):
        summary = _make_summary(
            _summary_row(report_id="Z", model_family="naive", model_name="naive", candidate_m=1, median_mase=0.80, mean_mae=10.0, mean_bias=0.5, absolute_mean_bias=0.5),
            _summary_row(report_id="A", model_family="naive", model_name="naive", candidate_m=1, median_mase=0.80, mean_mae=10.0, mean_bias=0.5, absolute_mean_bias=0.5),
        )
        result = select_candidate_models(summary)
        assert list(result["report_id"]) == ["A", "Z"]


class TestSelectCandidateEdgeCases:
    def test_missing_required_column_raises(self):
        summary = _make_summary(_summary_row()).drop(columns=["median_mase"])
        with pytest.raises(ValueError, match="missing required columns"):
            select_candidate_models(summary)

    def test_empty_input_returns_empty_dataframe(self):
        empty = pd.DataFrame(columns=list(SELECTION_REQUIRED_COLS))
        result = select_candidate_models(empty)
        assert result.empty
        assert list(result.columns) == SELECTION_OUTPUT_COLS

    def test_multi_report_produces_one_row_per_report(self):
        summary = _make_summary(
            _summary_row(report_id="R1", model_family="naive", model_name="naive", candidate_m=1, median_mase=0.80, mean_mae=10.0, mean_bias=0.5, absolute_mean_bias=0.5),
            _summary_row(report_id="R2", model_family="naive", model_name="naive", candidate_m=1, median_mase=0.75, mean_mae=9.5, mean_bias=0.4, absolute_mean_bias=0.4),
        )
        result = select_candidate_models(summary)
        assert len(result) == 2
        assert set(result["report_id"]) == {"R1", "R2"}
