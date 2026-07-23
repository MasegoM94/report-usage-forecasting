"""Tests for src.models.selection.select_models.

All tests construct model_summary DataFrames directly — no pipeline execution.
The _row() helper produces a minimal valid summary row; individual tests
override only the fields they care about.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.models.selection import (
    MAX_BIAS_RATIO,
    MODEL_COMPLEXITY,
    RELATIVE_IMPROVEMENT_TOLERANCE,
    SEASONAL_NAIVE_NAME,
    _OUTPUT_COLS,
    select_models,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_REPORT = "R_001"


def _row(
    report_id: str = _REPORT,
    model_name: str = "naive",
    has_sufficient_folds: bool = True,
    valid_folds: int = 4,
    failed_folds: int = 0,
    median_mase_lag1: float = 0.80,
    mean_mase_lag1: float = 0.82,
    mean_wape: float = 0.30,
    mean_mae: float = 10.0,
    mean_rmse: float = 12.0,
    mean_bias: float = 0.5,
    absolute_mean_bias: float = 0.5,
    fold_win_count: int = 2,
    fold_win_rate: float = 0.5,
    mase_lag1_std: float = 0.05,
    mean_interval_coverage: float = np.nan,
    mean_interval_width: float = np.nan,
) -> dict:
    return {
        "report_id": report_id,
        "model_name": model_name,
        "has_sufficient_folds": has_sufficient_folds,
        "valid_folds": valid_folds,
        "failed_folds": failed_folds,
        "median_mase_lag1": median_mase_lag1,
        "mean_mase_lag1": mean_mase_lag1,
        "mase_lag1_std": mase_lag1_std,
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


def _make(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


# ---------------------------------------------------------------------------
# 1. Clear winner: complex model genuinely outperforms all others
# ---------------------------------------------------------------------------

class TestClearComplexWinner:
    """auto_arima/ets wins by more than the tolerance — should be selected."""

    @pytest.fixture
    def fm(self):
        sn_mase = 1.00
        arima_mase = 0.75    # 25 % improvement — well above 5 % tolerance
        return _make(
            _row(model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=sn_mase, fold_win_count=1),
            _row(model_name="auto_arima",        median_mase_lag1=arima_mase, fold_win_count=3),
        )

    def test_auto_arima_selected(self, fm):
        result = select_models(fm, max_bias_ratio=None)
        assert result.iloc[0]["selected_model"] == "auto_arima"

    def test_status_is_selected(self, fm):
        result = select_models(fm, max_bias_ratio=None)
        assert result.iloc[0]["selection_status"] == "selected"

    def test_improvement_pct_positive(self, fm):
        result = select_models(fm, max_bias_ratio=None)
        pct = result.iloc[0]["improvement_vs_seasonal_naive_pct"]
        assert pct == pytest.approx(25.0)

    def test_seasonal_naive_benchmark_populated(self, fm):
        result = select_models(fm, max_bias_ratio=None)
        assert result.iloc[0]["seasonal_naive_median_mase"] == pytest.approx(1.00)

    def test_reason_mentions_model_and_improvement(self, fm):
        result = select_models(fm, max_bias_ratio=None)
        reason = result.iloc[0]["selection_reason"].lower()
        assert "auto arima" in reason or "auto_arima" in reason
        assert "25.0%" in result.iloc[0]["selection_reason"]

    def test_reason_mentions_fold_wins(self, fm):
        """Reason should reference fold wins when the model has valid counts."""
        result = select_models(fm, max_bias_ratio=None)
        reason = result.iloc[0]["selection_reason"]
        assert "3 of 4 folds" in reason

    def test_output_columns_complete(self, fm):
        result = select_models(fm, max_bias_ratio=None)
        assert list(result.columns) == _OUTPUT_COLS


# ---------------------------------------------------------------------------
# 2. Seasonal naive retained: complex model improvement below tolerance
# ---------------------------------------------------------------------------

class TestNegligibleImprovement:
    """Complex model's MASE only 3 % better than seasonal naive — retain naive."""

    @pytest.fixture
    def fm(self):
        sn_mase = 1.00
        arima_mase = sn_mase * (1 - 0.03)   # 3 % — below 5 % tolerance
        return _make(
            _row(model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=sn_mase,
                 mean_mae=10.0, mean_bias=0.0, absolute_mean_bias=0.0),
            _row(model_name="auto_arima",        median_mase_lag1=arima_mase,
                 mean_mae=9.7, mean_bias=0.1, absolute_mean_bias=0.1),
        )

    def test_seasonal_naive_selected(self, fm):
        result = select_models(fm, max_bias_ratio=None)
        assert result.iloc[0]["selected_model"] == SEASONAL_NAIVE_NAME

    def test_reason_mentions_tolerance(self, fm):
        result = select_models(fm, max_bias_ratio=None)
        reason = result.iloc[0]["selection_reason"]
        assert "tolerance" in reason.lower()

    def test_reason_shows_improvement_that_was_rejected(self, fm):
        """The reason should name the improvement percentage that was too small."""
        result = select_models(fm, max_bias_ratio=None)
        reason = result.iloc[0]["selection_reason"]
        # The 3 % improvement should appear somewhere in the reason
        assert "3." in reason or "3%" in reason

    def test_improvement_pct_near_zero(self, fm):
        """Improvement of seasonal naive over itself is 0."""
        result = select_models(fm, max_bias_ratio=None)
        pct = result.iloc[0]["improvement_vs_seasonal_naive_pct"]
        assert pct == pytest.approx(0.0, abs=1e-9)

    def test_custom_tolerance_allows_selection(self, fm):
        """With tolerance set to 1 %, the 3 % improvement is enough to select arima."""
        result = select_models(fm, relative_improvement_tolerance=0.01, max_bias_ratio=None)
        assert result.iloc[0]["selected_model"] == "auto_arima"


# ---------------------------------------------------------------------------
# 3. Simpler model preferred in a practical tie
# ---------------------------------------------------------------------------

class TestPracticalTieSimplestWins:
    """ETS and seasonal naive within tolerance — seasonal naive (simpler) wins."""

    @pytest.fixture
    def fm(self):
        sn_mase = 0.90
        ets_mase = sn_mase * (1 - RELATIVE_IMPROVEMENT_TOLERANCE / 2)   # halfway inside
        return _make(
            _row(model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=sn_mase,
                 mean_mae=10.0, mean_bias=0.0, absolute_mean_bias=0.0),
            _row(model_name="ets",               median_mase_lag1=ets_mase,
                 mean_mae=9.6, mean_bias=0.2, absolute_mean_bias=0.2),
        )

    def test_simpler_model_selected(self, fm):
        result = select_models(fm, max_bias_ratio=None)
        assert result.iloc[0]["selected_model"] == SEASONAL_NAIVE_NAME

    def test_reason_names_model_that_was_bypassed(self, fm):
        reason = select_models(fm, max_bias_ratio=None).iloc[0]["selection_reason"].lower()
        assert "ets" in reason

    def test_three_way_tie_picks_least_complex(self):
        """When naive, seasonal_naive, and moving_average all tie, naive wins."""
        base_mase = 1.0
        delta = RELATIVE_IMPROVEMENT_TOLERANCE / 4   # all within tolerance
        fm = _make(
            _row(model_name="moving_average",    median_mase_lag1=base_mase),
            _row(model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=base_mase * (1 + delta)),
            _row(model_name="naive",             median_mase_lag1=base_mase * (1 + 2 * delta)),
        )
        result = select_models(fm, max_bias_ratio=None)
        assert result.iloc[0]["selected_model"] == "naive"

    def test_complexity_ordering_respected(self):
        """Confirm MODEL_COMPLEXITY gives naive < seasonal_naive < moving_average < ets < auto_arima."""
        order = ["naive", "seasonal_naive", "moving_average", "ets", "auto_arima"]
        for a, b in zip(order, order[1:]):
            assert MODEL_COMPLEXITY[a] < MODEL_COMPLEXITY[b]


# ---------------------------------------------------------------------------
# 4. Bias guardrail rejection
# ---------------------------------------------------------------------------

class TestBiasGuardrail:
    """Models with |mean_bias| / mean_mae > MAX_BIAS_RATIO should be excluded."""

    @pytest.fixture
    def fm(self):
        return _make(
            _row(model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=0.90,
                 mean_mae=10.0, mean_bias=0.5, absolute_mean_bias=0.5),   # ratio 0.05 — passes
            _row(model_name="auto_arima",        median_mase_lag1=0.70,
                 mean_mae=10.0, mean_bias=6.0, absolute_mean_bias=6.0),   # ratio 0.60 — fails
        )

    def test_biased_model_excluded(self, fm):
        result = select_models(fm, max_bias_ratio=0.5)
        assert result.iloc[0]["selected_model"] == SEASONAL_NAIVE_NAME

    def test_reason_mentions_guardrail_exclusion(self, fm):
        result = select_models(fm, max_bias_ratio=0.5)
        reason = result.iloc[0]["selection_reason"].lower()
        assert "bias guardrail" in reason or "bias" in reason

    def test_bias_guardrail_disabled_selects_biased_model(self, fm):
        """With guardrail off the lower-MASE (biased) model should win."""
        result = select_models(fm, max_bias_ratio=None)
        assert result.iloc[0]["selected_model"] == "auto_arima"

    def test_all_models_biased_gives_no_reliable_model(self):
        fm = _make(
            _row(model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=0.90,
                 mean_mae=10.0, mean_bias=8.0, absolute_mean_bias=8.0),   # ratio 0.80
            _row(model_name="naive",             median_mase_lag1=0.80,
                 mean_mae=10.0, mean_bias=9.0, absolute_mean_bias=9.0),   # ratio 0.90
        )
        result = select_models(fm, max_bias_ratio=0.5)
        assert result.iloc[0]["selection_status"] == "no_reliable_model"

    def test_no_model_reason_names_excluded_models(self):
        fm = _make(
            _row(model_name="auto_arima", median_mase_lag1=0.80,
                 mean_mae=10.0, mean_bias=8.0, absolute_mean_bias=8.0),
        )
        result = select_models(fm, max_bias_ratio=0.5)
        reason = result.iloc[0]["selection_reason"]
        assert "auto_arima" in reason


# ---------------------------------------------------------------------------
# 5. Insufficient folds
# ---------------------------------------------------------------------------

class TestInsufficientFolds:
    def test_all_models_insufficient(self):
        fm = _make(
            _row(model_name="naive",          has_sufficient_folds=False, valid_folds=1),
            _row(model_name=SEASONAL_NAIVE_NAME, has_sufficient_folds=False, valid_folds=2),
        )
        result = select_models(fm, min_valid_folds=3)
        assert result.iloc[0]["selection_status"] == "no_reliable_model"

    def test_reason_mentions_fold_count(self):
        fm = _make(
            _row(model_name="naive", has_sufficient_folds=False, valid_folds=1),
        )
        result = select_models(fm, min_valid_folds=3)
        reason = result.iloc[0]["selection_reason"].lower()
        assert "fold" in reason

    def test_partial_sufficiency_selects_eligible_model(self):
        """Only models with sufficient folds are candidates; others are ignored."""
        fm = _make(
            _row(model_name="naive",             has_sufficient_folds=False,
                 valid_folds=1, median_mase_lag1=0.50),
            _row(model_name=SEASONAL_NAIVE_NAME, has_sufficient_folds=True,
                 valid_folds=4, median_mase_lag1=0.90),
        )
        result = select_models(fm, max_bias_ratio=None)
        # naive has better MASE but insufficient folds — seasonal naive should win
        assert result.iloc[0]["selected_model"] == SEASONAL_NAIVE_NAME

    def test_seasonal_naive_mase_still_populated_when_insufficient(self):
        """Benchmark column reflects the seasonal naive MASE even if it was excluded."""
        fm = _make(
            _row(model_name=SEASONAL_NAIVE_NAME, has_sufficient_folds=False,
                 valid_folds=1, median_mase_lag1=0.95),
            _row(model_name="naive",             has_sufficient_folds=True,
                 valid_folds=4, median_mase_lag1=0.80),
        )
        result = select_models(fm, max_bias_ratio=None)
        assert result.iloc[0]["seasonal_naive_median_mase"] == pytest.approx(0.95)


# ---------------------------------------------------------------------------
# 6. All models failed
# ---------------------------------------------------------------------------

class TestAllModelsFailed:
    def test_no_reliable_model_when_all_failed(self):
        fm = _make(
            _row(model_name="naive",          has_sufficient_folds=False,
                 valid_folds=0, median_mase_lag1=np.nan),
            _row(model_name=SEASONAL_NAIVE_NAME, has_sufficient_folds=False,
                 valid_folds=0, median_mase_lag1=np.nan),
        )
        result = select_models(fm)
        row = result.iloc[0]
        assert row["selection_status"] == "no_reliable_model"
        assert row["selected_model"] is None
        assert pd.isna(row["median_mase_lag1"])

    def test_empty_summary_returns_empty_result(self):
        empty = pd.DataFrame(columns=list(_make(_row()).columns))
        result = select_models(empty)
        assert result.empty
        assert list(result.columns) == _OUTPUT_COLS


# ---------------------------------------------------------------------------
# 7. Missing seasonal naive benchmark
# ---------------------------------------------------------------------------

class TestMissingSeasonalNaive:
    @pytest.fixture
    def fm(self):
        # Only naive and auto_arima — no seasonal_naive in the portfolio
        return _make(
            _row(model_name="naive",      median_mase_lag1=1.10, fold_win_count=1),
            _row(model_name="auto_arima", median_mase_lag1=0.75, fold_win_count=3),
        )

    def test_auto_arima_still_selected(self, fm):
        result = select_models(fm, max_bias_ratio=None)
        assert result.iloc[0]["selected_model"] == "auto_arima"

    def test_seasonal_naive_mase_is_nan(self, fm):
        result = select_models(fm, max_bias_ratio=None)
        assert pd.isna(result.iloc[0]["seasonal_naive_median_mase"])

    def test_improvement_pct_is_nan(self, fm):
        result = select_models(fm, max_bias_ratio=None)
        assert pd.isna(result.iloc[0]["improvement_vs_seasonal_naive_pct"])

    def test_reason_notes_missing_benchmark(self, fm):
        result = select_models(fm, max_bias_ratio=None)
        reason = result.iloc[0]["selection_reason"].lower()
        assert "seasonal naive" in reason or "benchmark" in reason


# ---------------------------------------------------------------------------
# 8. Deterministic selection reason
# ---------------------------------------------------------------------------

class TestDeterministicReason:
    """Calling select_models twice with identical inputs must produce the same reason."""

    def test_reason_is_deterministic(self):
        fm = _make(
            _row(model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=1.00,
                 mean_mae=10.0, mean_bias=0.0, absolute_mean_bias=0.0),
            _row(model_name="ets",               median_mase_lag1=0.72,
                 mean_mae=9.0, mean_bias=0.2, absolute_mean_bias=0.2,
                 fold_win_count=3),
        )
        r1 = select_models(fm, max_bias_ratio=None)
        r2 = select_models(fm, max_bias_ratio=None)
        assert r1.iloc[0]["selection_reason"] == r2.iloc[0]["selection_reason"]

    def test_reason_is_non_empty_string(self):
        fm = _make(_row(model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=0.80))
        result = select_models(fm, max_bias_ratio=None)
        reason = result.iloc[0]["selection_reason"]
        assert isinstance(reason, str) and len(reason) > 0

    def test_no_model_reason_non_empty(self):
        fm = _make(_row(has_sufficient_folds=False, valid_folds=1))
        result = select_models(fm)
        assert len(result.iloc[0]["selection_reason"]) > 0

    def test_reason_ends_with_period(self):
        """Reasons should be complete sentences ending with a period."""
        fm = _make(
            _row(model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=0.80),
            _row(model_name="auto_arima",        median_mase_lag1=0.60, fold_win_count=4),
        )
        result = select_models(fm, max_bias_ratio=None)
        assert result.iloc[0]["selection_reason"].endswith(".")


# ---------------------------------------------------------------------------
# 9. Multi-report output
# ---------------------------------------------------------------------------

class TestMultiReport:
    def test_one_row_per_report(self):
        fm = _make(
            _row(report_id="R_001", model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=0.90),
            _row(report_id="R_001", model_name="auto_arima",         median_mase_lag1=0.70),
            _row(report_id="R_002", model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=1.10),
            _row(report_id="R_002", model_name="ets",                median_mase_lag1=0.80),
        )
        result = select_models(fm, max_bias_ratio=None)
        assert len(result) == 2
        assert set(result["report_id"]) == {"R_001", "R_002"}

    def test_sorted_by_report_id(self):
        fm = _make(
            _row(report_id="R_002", model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=0.90),
            _row(report_id="R_001", model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=0.80),
        )
        result = select_models(fm, max_bias_ratio=None)
        assert list(result["report_id"]) == ["R_001", "R_002"]

    def test_reports_do_not_influence_each_other(self):
        """Bias exclusion in one report must not affect another report."""
        fm = _make(
            # R_001: auto_arima fails bias guardrail
            _row(report_id="R_001", model_name=SEASONAL_NAIVE_NAME,
                 median_mase_lag1=0.90, mean_mae=10.0, mean_bias=0.0, absolute_mean_bias=0.0),
            _row(report_id="R_001", model_name="auto_arima",
                 median_mase_lag1=0.70, mean_mae=10.0, mean_bias=6.0, absolute_mean_bias=6.0),
            # R_002: auto_arima passes — low bias
            _row(report_id="R_002", model_name=SEASONAL_NAIVE_NAME,
                 median_mase_lag1=0.90, mean_mae=10.0, mean_bias=0.0, absolute_mean_bias=0.0),
            _row(report_id="R_002", model_name="auto_arima",
                 median_mase_lag1=0.70, mean_mae=10.0, mean_bias=0.4, absolute_mean_bias=0.4),
        )
        result = select_models(fm, max_bias_ratio=0.5)
        r1 = result[result["report_id"] == "R_001"].iloc[0]
        r2 = result[result["report_id"] == "R_002"].iloc[0]
        assert r1["selected_model"] == SEASONAL_NAIVE_NAME   # biased arima excluded
        assert r2["selected_model"] == "auto_arima"           # clean arima wins


# ---------------------------------------------------------------------------
# 10. Input validation
# ---------------------------------------------------------------------------

class TestInputValidation:
    def test_missing_column_raises(self):
        fm = _make(_row()).drop(columns=["median_mase_lag1"])
        with pytest.raises(ValueError, match="missing required columns"):
            select_models(fm)

    def test_output_has_exactly_required_columns(self):
        fm = _make(_row(model_name=SEASONAL_NAIVE_NAME, median_mase_lag1=0.80))
        result = select_models(fm, max_bias_ratio=None)
        assert list(result.columns) == _OUTPUT_COLS

    def test_selected_model_none_for_no_reliable_model(self):
        fm = _make(_row(has_sufficient_folds=False, valid_folds=0))
        result = select_models(fm)
        assert result.iloc[0]["selected_model"] is None
