"""Tests for src/monitoring/deterioration.py — per-report deterioration monitoring.

All tests use the new ``by_report_run`` input (one row per (report_id, run_id))
rather than the old portfolio-level ``by_run`` table.  This reflects the
corrected grain: eligible runs are selected independently for each report so
that two reports in the same pipeline run can have different recent/previous
metrics and different deterioration flags.

Test inventory
--------------
TestOutputSchema
    columns, one-row-per-report, sorted, empty

TestClearDeterioration
    WAPE deterioration flagged, values populated, run IDs populated

TestStablePerformance
    no flag when unchanged, no flag below threshold

TestImprovingPerformance
    no flag, negative change

TestPartialRunExcluded
    partial excluded, only one complete → insufficient, min_observations guard

TestOnlyOneCompletedRun
    single eligible → insufficient, zero eligible → insufficient

TestBiasDeterioration
    per-report bias shift flagged, change computed, near-zero description,
    small change not flagged

TestIntervalCoverageDeterioration
    drop flagged, change computed, improvement not flagged, small drop not flagged

TestDeterministicReasons
    same input → same reasons, values quoted, multiple reasons, no reasons

TestNoActuals
    NaN WAPE → no_actuals status

TestValidation
    missing required column raises

TestTwoReportsInSameRun
    two reports share a run; one deteriorates, the other improves;
    each receives its own metrics (not the portfolio average)

TestPerReportBiasDeterioration
    R1 bias shifts; R2 bias is stable — only R1 is flagged

TestPerReportCoverageDeterioration
    R1 coverage drops; R2 coverage is stable — only R1 is flagged

TestPartialLatestRunExcluded
    the most recent pipeline run is partial for a specific report;
    that report falls back to the two preceding complete runs

TestOnlyOneCompletedReportRun
    report has only one eligible run → insufficient_evidence
    (even when other reports have more)

TestZeroActualVolume
    report with all-zero actuals → no_actuals status
    (even when neighboring reports have non-zero actuals)

TestModelChangeBetweenRuns
    model family or selected_m change between runs does NOT produce a reason;
    change is context only, not a deterioration cause

TestSelectedMChangeBetweenRuns
    same as above: selected_m change is not a deterioration cause

TestPortfolioMetricsNotCopied
    two reports in the same run have different WAPE in by_report_run;
    portfolio-level average (which differs from both) is NEVER used

TestHelperFunctions
    _safe_float, _delta, _pct_change, _bias_description unit tests

TestMostRecentRunsSelected
    lexicographic latest two run IDs selected; older runs ignored
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from src.monitoring.deterioration import (
    DETERIORATION_COLS,
    DeteriorationConfig,
    _bias_description,
    _build_reasons,
    _delta,
    _pct_change,
    _safe_float,
    compute_deterioration_report,
)


# ---------------------------------------------------------------------------
# Shared fixtures / builders
# ---------------------------------------------------------------------------

def _report_run_row(
    report_id: str,
    run_id: str,
    *,
    realization_rate: float = 1.0,
    realized_prediction_count: int = 28,
    wape: float = 0.10,
    bias: float = 0.0,
    interval_coverage: float = 0.90,
    selected_model_family: str = "seasonal_naive",
    selected_model_name: str = "seasonal_naive_m7",
    selected_m: int = 7,
) -> dict:
    """Build a minimal by_report_run row for testing."""
    return {
        "report_id":                 report_id,
        "report_name":               f"Report {report_id}",
        "run_id":                    run_id,
        "selection_run_id":          run_id,
        "generated_at":              None,
        "training_cutoff":           None,
        "selected_model_family":     selected_model_family,
        "selected_model_name":       selected_model_name,
        "selected_m":                selected_m,
        "expected_prediction_count": 28,
        "realized_prediction_count": realized_prediction_count,
        "realization_rate":          realization_rate,
        "fully_realized":            realized_prediction_count == 28,
        "comparable_run":            realization_rate >= 0.90 and realized_prediction_count >= 1,
        "mae":                       np.nan,
        "rmse":                      np.nan,
        "wape":                      wape,
        "bias":                      bias,
        "absolute_bias":             abs(bias) if not math.isnan(bias) else np.nan,
        "interval_observation_count": realized_prediction_count,
        "interval_coverage":         interval_coverage,
        "mean_interval_width":       np.nan,
        "lineage_complete":          True,
        "run_status":                "complete" if realization_rate >= 0.90 else "partial",
    }


def _make(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# Tiny config with a low observation floor so small fixtures work
_CFG = DeteriorationConfig(
    min_realization_rate=0.90,
    min_observations_per_run=1,
    wape_change_threshold=0.05,
    bias_change_threshold=5.0,
    interval_coverage_drop_threshold=0.10,
    bias_near_zero_band=2.0,
)


# ---------------------------------------------------------------------------
# TestOutputSchema
# ---------------------------------------------------------------------------

class TestOutputSchema:
    def test_columns_present(self):
        rows = [
            _report_run_row("R1", "run_002"),
            _report_run_row("R1", "run_001"),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        assert list(out.columns) == DETERIORATION_COLS

    def test_one_row_per_report(self):
        rows = [
            _report_run_row("R1", "run_003"),
            _report_run_row("R1", "run_002"),
            _report_run_row("R2", "run_003"),
            _report_run_row("R2", "run_002"),
            _report_run_row("R3", "run_003"),
            _report_run_row("R3", "run_002"),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        assert len(out) == 3
        assert list(out["report_id"]) == ["R1", "R2", "R3"]

    def test_sorted_by_report_id(self):
        rows = [
            _report_run_row("Z_report", "run_002"),
            _report_run_row("Z_report", "run_001"),
            _report_run_row("A_report", "run_002"),
            _report_run_row("A_report", "run_001"),
            _report_run_row("M_report", "run_002"),
            _report_run_row("M_report", "run_001"),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        assert list(out["report_id"]) == ["A_report", "M_report", "Z_report"]

    def test_empty_when_empty_input(self):
        empty = pd.DataFrame(columns=list(_report_run_row("R1", "r").keys()))
        out = compute_deterioration_report(empty, config=_CFG)
        assert out.empty
        assert list(out.columns) == DETERIORATION_COLS


# ---------------------------------------------------------------------------
# TestClearDeterioration
# ---------------------------------------------------------------------------

class TestClearDeterioration:
    def test_wape_deterioration_flagged(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.30),  # recent — worse
            _report_run_row("R1", "run_001", wape=0.10),  # previous — better
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        row = out.iloc[0]
        assert row["accuracy_deterioration_flag"] == True
        assert row["evidence_status"] == "ok"
        assert any("WAPE" in r for r in row["deterioration_reasons"])

    def test_wape_values_populated(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.30),
            _report_run_row("R1", "run_001", wape=0.10),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        row = out.iloc[0]
        assert pytest.approx(row["recent_wape"],        abs=1e-6) == 0.30
        assert pytest.approx(row["previous_wape"],      abs=1e-6) == 0.10
        assert pytest.approx(row["wape_change_absolute"], abs=1e-6) == 0.20
        assert pytest.approx(row["wape_change_pct"],    abs=1e-3) == 200.0

    def test_run_ids_populated(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.30),
            _report_run_row("R1", "run_001", wape=0.10),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        row = out.iloc[0]
        assert row["recent_completed_run_id"]   == "run_002"
        assert row["previous_completed_run_id"] == "run_001"


# ---------------------------------------------------------------------------
# TestStablePerformance
# ---------------------------------------------------------------------------

class TestStablePerformance:
    def test_no_flag_when_wape_unchanged(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.10),
            _report_run_row("R1", "run_001", wape=0.10),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        row = out.iloc[0]
        assert row["accuracy_deterioration_flag"] == False
        assert row["deterioration_reasons"] == []
        assert row["evidence_status"] == "ok"

    def test_no_flag_below_threshold(self):
        # Change of 0.03 is below 0.05 threshold
        rows = [
            _report_run_row("R1", "run_002", wape=0.13),
            _report_run_row("R1", "run_001", wape=0.10),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        assert out.iloc[0]["accuracy_deterioration_flag"] == False


# ---------------------------------------------------------------------------
# TestImprovingPerformance
# ---------------------------------------------------------------------------

class TestImprovingPerformance:
    def test_no_flag_when_wape_improves(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.05),  # recent — better
            _report_run_row("R1", "run_001", wape=0.20),  # previous — worse
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        row = out.iloc[0]
        assert row["accuracy_deterioration_flag"] == False
        assert row["deterioration_reasons"] == []

    def test_wape_change_negative_when_improving(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.05),
            _report_run_row("R1", "run_001", wape=0.20),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        assert out.iloc[0]["wape_change_absolute"] < 0


# ---------------------------------------------------------------------------
# TestPartialRunExcluded
# ---------------------------------------------------------------------------

class TestPartialRunExcluded:
    def test_partial_run_not_used_in_comparison(self):
        """A run with realization_rate=0.40 must be excluded for this report."""
        rows = [
            _report_run_row("R1", "run_003", realization_rate=0.40, wape=0.50),
            _report_run_row("R1", "run_002", realization_rate=1.00, wape=0.10),
            _report_run_row("R1", "run_001", realization_rate=1.00, wape=0.09),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        row = out.iloc[0]
        assert row["recent_completed_run_id"]   == "run_002"
        assert row["previous_completed_run_id"] == "run_001"

    def test_only_one_complete_run_gives_insufficient_evidence(self):
        rows = [
            _report_run_row("R1", "run_002", realization_rate=1.00, wape=0.10),
            _report_run_row("R1", "run_001", realization_rate=0.30, wape=0.50),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        row = out.iloc[0]
        assert row["evidence_status"] == "insufficient_evidence"
        assert row["accuracy_deterioration_flag"] == False
        assert math.isnan(row["wape_change_absolute"])

    def test_min_observations_guard(self):
        """A run with too few observations is excluded even if realization_rate is high."""
        cfg = DeteriorationConfig(
            min_realization_rate=0.90,
            min_observations_per_run=20,
            wape_change_threshold=0.05,
            bias_change_threshold=5.0,
            interval_coverage_drop_threshold=0.10,
        )
        rows = [
            _report_run_row("R1", "run_002", realized_prediction_count=5,  wape=0.40),
            _report_run_row("R1", "run_001", realized_prediction_count=28, wape=0.10),
        ]
        out = compute_deterioration_report(_make(rows), config=cfg)
        assert out.iloc[0]["evidence_status"] == "insufficient_evidence"


# ---------------------------------------------------------------------------
# TestOnlyOneCompletedRun
# ---------------------------------------------------------------------------

class TestOnlyOneCompletedRun:
    def test_single_eligible_run_is_insufficient(self):
        rows = [_report_run_row("R1", "run_001", realization_rate=1.0, wape=0.10)]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        row = out.iloc[0]
        assert row["evidence_status"] == "insufficient_evidence"
        assert row["recent_completed_run_id"] is None

    def test_no_eligible_runs_is_insufficient(self):
        rows = [_report_run_row("R1", "run_001", realization_rate=0.20)]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        assert out.iloc[0]["evidence_status"] == "insufficient_evidence"


# ---------------------------------------------------------------------------
# TestBiasDeterioration
# ---------------------------------------------------------------------------

class TestBiasDeterioration:
    def test_bias_shift_flagged_without_wape_change(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.10, bias=12.0),
            _report_run_row("R1", "run_001", wape=0.10, bias= 0.5),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        row = out.iloc[0]
        assert row["accuracy_deterioration_flag"] == True
        reasons_text = " ".join(row["deterioration_reasons"])
        assert "bias" in reasons_text.lower()
        assert not any("WAPE" in r for r in row["deterioration_reasons"])

    def test_bias_change_computed_correctly(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.10, bias=10.0),
            _report_run_row("R1", "run_001", wape=0.10, bias= 1.0),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        assert pytest.approx(out.iloc[0]["bias_change"], abs=1e-6) == 9.0

    def test_near_zero_to_persistent_description(self):
        desc = _bias_description(0.5, near_zero_band=2.0)
        assert desc == "near-zero"
        desc2 = _bias_description(15.0, near_zero_band=2.0)
        assert "overforecasting" in desc2

    def test_small_bias_change_below_threshold_not_flagged(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.10, bias=3.0),
            _report_run_row("R1", "run_001", wape=0.10, bias=1.0),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        assert out.iloc[0]["accuracy_deterioration_flag"] == False


# ---------------------------------------------------------------------------
# TestIntervalCoverageDeterioration
# ---------------------------------------------------------------------------

class TestIntervalCoverageDeterioration:
    def test_coverage_drop_flagged(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.10, interval_coverage=0.75),
            _report_run_row("R1", "run_001", wape=0.10, interval_coverage=0.92),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        row = out.iloc[0]
        assert row["accuracy_deterioration_flag"] == True
        assert any("coverage" in r.lower() for r in row["deterioration_reasons"])

    def test_coverage_change_computed(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.10, interval_coverage=0.75),
            _report_run_row("R1", "run_001", wape=0.10, interval_coverage=0.92),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        assert pytest.approx(out.iloc[0]["interval_coverage_change"], abs=1e-6) == -0.17

    def test_coverage_improvement_not_flagged(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.10, interval_coverage=0.95),
            _report_run_row("R1", "run_001", wape=0.10, interval_coverage=0.70),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        assert out.iloc[0]["accuracy_deterioration_flag"] == False

    def test_small_coverage_drop_below_threshold(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.10, interval_coverage=0.86),
            _report_run_row("R1", "run_001", wape=0.10, interval_coverage=0.90),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        assert out.iloc[0]["accuracy_deterioration_flag"] == False


# ---------------------------------------------------------------------------
# TestDeterministicReasons
# ---------------------------------------------------------------------------

class TestDeterministicReasons:
    def test_same_input_same_reasons(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.30, bias=15.0, interval_coverage=0.60),
            _report_run_row("R1", "run_001", wape=0.10, bias= 0.5, interval_coverage=0.92),
        ]
        df = _make(rows)
        out1 = compute_deterioration_report(df, config=_CFG)
        out2 = compute_deterioration_report(df, config=_CFG)
        assert out1.iloc[0]["deterioration_reasons"] == out2.iloc[0]["deterioration_reasons"]

    def test_reason_includes_metric_values(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.30),
            _report_run_row("R1", "run_001", wape=0.10),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        reason = out.iloc[0]["deterioration_reasons"][0]
        assert "10%" in reason or "30%" in reason

    def test_multiple_reasons_when_multiple_metrics_degrade(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.30, bias=15.0, interval_coverage=0.60),
            _report_run_row("R1", "run_001", wape=0.10, bias= 0.5, interval_coverage=0.92),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        reasons = out.iloc[0]["deterioration_reasons"]
        assert len(reasons) == 3  # WAPE + bias + coverage

    def test_no_reasons_when_stable(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.10, bias=0.5, interval_coverage=0.90),
            _report_run_row("R1", "run_001", wape=0.10, bias=0.5, interval_coverage=0.90),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        assert out.iloc[0]["deterioration_reasons"] == []

    def test_reason_order_is_wape_bias_coverage(self):
        """Reasons must appear in fixed order: WAPE, bias, coverage."""
        rows = [
            _report_run_row("R1", "run_002", wape=0.30, bias=15.0, interval_coverage=0.60),
            _report_run_row("R1", "run_001", wape=0.10, bias= 0.5, interval_coverage=0.92),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        reasons = out.iloc[0]["deterioration_reasons"]
        assert "WAPE"     in reasons[0]
        assert "bias"     in reasons[1].lower()
        assert "coverage" in reasons[2].lower()


# ---------------------------------------------------------------------------
# TestNoActuals
# ---------------------------------------------------------------------------

class TestNoActuals:
    def test_nan_wape_gives_no_actuals_status(self):
        rows = [
            _report_run_row("R1", "run_002", wape=np.nan),
            _report_run_row("R1", "run_001", wape=np.nan),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        row = out.iloc[0]
        assert row["evidence_status"] == "no_actuals"
        assert row["accuracy_deterioration_flag"] == False
        assert row["recent_completed_run_id"]   == "run_002"
        assert row["previous_completed_run_id"] == "run_001"


# ---------------------------------------------------------------------------
# TestValidation
# ---------------------------------------------------------------------------

class TestValidation:
    def test_missing_required_column_raises(self):
        bad = pd.DataFrame({"report_id": ["R1"], "run_id": ["r1"], "wape": [0.1]})
        with pytest.raises(ValueError, match="missing required column"):
            compute_deterioration_report(bad, config=_CFG)


# ---------------------------------------------------------------------------
# TestTwoReportsInSameRun
# Scenario: two reports appear in the same runs; they have different per-report
# WAPE values so one deteriorates and the other improves.
# ---------------------------------------------------------------------------

class TestTwoReportsInSameRun:
    def _build(self):
        """
        run_002 (recent): R1 WAPE=0.30 (worse), R2 WAPE=0.05 (better)
        run_001 (prev  ): R1 WAPE=0.10,          R2 WAPE=0.20
        → R1 should be flagged, R2 should not.
        """
        rows = [
            _report_run_row("R1", "run_002", wape=0.30),
            _report_run_row("R1", "run_001", wape=0.10),
            _report_run_row("R2", "run_002", wape=0.05),
            _report_run_row("R2", "run_001", wape=0.20),
        ]
        return compute_deterioration_report(_make(rows), config=_CFG)

    def test_two_reports_one_row_each(self):
        out = self._build()
        assert len(out) == 2
        assert set(out["report_id"]) == {"R1", "R2"}

    def test_r1_deteriorates(self):
        out = self._build()
        r1 = out[out["report_id"] == "R1"].iloc[0]
        assert r1["accuracy_deterioration_flag"] == True
        assert pytest.approx(r1["recent_wape"],   abs=1e-6) == 0.30
        assert pytest.approx(r1["previous_wape"], abs=1e-6) == 0.10

    def test_r2_improves(self):
        out = self._build()
        r2 = out[out["report_id"] == "R2"].iloc[0]
        assert r2["accuracy_deterioration_flag"] == False
        assert pytest.approx(r2["recent_wape"],   abs=1e-6) == 0.05
        assert pytest.approx(r2["previous_wape"], abs=1e-6) == 0.20

    def test_reports_do_not_share_metrics(self):
        """R1's recent_wape must NOT equal R2's recent_wape."""
        out = self._build()
        r1_wape = out[out["report_id"] == "R1"]["recent_wape"].iloc[0]
        r2_wape = out[out["report_id"] == "R2"]["recent_wape"].iloc[0]
        assert r1_wape != r2_wape


# ---------------------------------------------------------------------------
# TestPerReportBiasDeterioration
# Scenario: R1 has a large bias shift; R2 is stable.
# ---------------------------------------------------------------------------

class TestPerReportBiasDeterioration:
    def _build(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.10, bias=15.0),
            _report_run_row("R1", "run_001", wape=0.10, bias= 0.5),
            _report_run_row("R2", "run_002", wape=0.10, bias= 1.0),
            _report_run_row("R2", "run_001", wape=0.10, bias= 0.8),
        ]
        return compute_deterioration_report(_make(rows), config=_CFG)

    def test_r1_bias_flagged(self):
        out = self._build()
        r1 = out[out["report_id"] == "R1"].iloc[0]
        assert r1["accuracy_deterioration_flag"] == True
        assert any("bias" in r.lower() for r in r1["deterioration_reasons"])

    def test_r2_bias_not_flagged(self):
        out = self._build()
        r2 = out[out["report_id"] == "R2"].iloc[0]
        assert r2["accuracy_deterioration_flag"] == False
        assert r2["deterioration_reasons"] == []

    def test_r1_bias_change_is_report_specific(self):
        out = self._build()
        r1 = out[out["report_id"] == "R1"].iloc[0]
        assert pytest.approx(r1["bias_change"], abs=1e-6) == 14.5  # 15.0 - 0.5


# ---------------------------------------------------------------------------
# TestPerReportCoverageDeterioration
# Scenario: R1 coverage drops; R2 coverage is stable.
# ---------------------------------------------------------------------------

class TestPerReportCoverageDeterioration:
    def _build(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.10, interval_coverage=0.70),
            _report_run_row("R1", "run_001", wape=0.10, interval_coverage=0.92),
            _report_run_row("R2", "run_002", wape=0.10, interval_coverage=0.91),
            _report_run_row("R2", "run_001", wape=0.10, interval_coverage=0.90),
        ]
        return compute_deterioration_report(_make(rows), config=_CFG)

    def test_r1_coverage_flagged(self):
        out = self._build()
        r1 = out[out["report_id"] == "R1"].iloc[0]
        assert r1["accuracy_deterioration_flag"] == True
        assert any("coverage" in r.lower() for r in r1["deterioration_reasons"])

    def test_r2_coverage_not_flagged(self):
        out = self._build()
        r2 = out[out["report_id"] == "R2"].iloc[0]
        assert r2["accuracy_deterioration_flag"] == False

    def test_r1_coverage_change_is_report_specific(self):
        out = self._build()
        r1 = out[out["report_id"] == "R1"].iloc[0]
        assert pytest.approx(r1["interval_coverage_change"], abs=1e-6) == -0.22


# ---------------------------------------------------------------------------
# TestPartialLatestRunExcluded
# Scenario: the most recent pipeline run is partial for R1 (realization_rate=0.40)
# so R1 falls back to comparing run_002 vs run_001.
# R2 has full realization in all three runs.
# ---------------------------------------------------------------------------

class TestPartialLatestRunExcluded:
    def _build(self):
        rows = [
            # R1: run_003 is partial — must be excluded for R1
            _report_run_row("R1", "run_003", realization_rate=0.40, wape=0.99),
            _report_run_row("R1", "run_002", realization_rate=1.00, wape=0.30),
            _report_run_row("R1", "run_001", realization_rate=1.00, wape=0.10),
            # R2: all three runs complete
            _report_run_row("R2", "run_003", realization_rate=1.00, wape=0.12),
            _report_run_row("R2", "run_002", realization_rate=1.00, wape=0.10),
            _report_run_row("R2", "run_001", realization_rate=1.00, wape=0.09),
        ]
        return compute_deterioration_report(_make(rows), config=_CFG)

    def test_r1_uses_run_002_and_run_001(self):
        out = self._build()
        r1 = out[out["report_id"] == "R1"].iloc[0]
        assert r1["recent_completed_run_id"]   == "run_002"
        assert r1["previous_completed_run_id"] == "run_001"

    def test_r2_uses_run_003_and_run_002(self):
        out = self._build()
        r2 = out[out["report_id"] == "R2"].iloc[0]
        assert r2["recent_completed_run_id"]   == "run_003"
        assert r2["previous_completed_run_id"] == "run_002"

    def test_r1_partial_run_wape_not_used(self):
        """The wape=0.99 from the partial run must not appear in R1's output."""
        out = self._build()
        r1 = out[out["report_id"] == "R1"].iloc[0]
        assert r1["recent_wape"] != pytest.approx(0.99)


# ---------------------------------------------------------------------------
# TestOnlyOneCompletedReportRun
# Scenario: R2 has only one comparable run; R1 has two.
# ---------------------------------------------------------------------------

class TestOnlyOneCompletedReportRun:
    def _build(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.20),
            _report_run_row("R1", "run_001", wape=0.10),
            # R2 appears only in run_002 (one comparable run)
            _report_run_row("R2", "run_002", wape=0.15),
        ]
        return compute_deterioration_report(_make(rows), config=_CFG)

    def test_r1_has_evidence(self):
        out = self._build()
        r1 = out[out["report_id"] == "R1"].iloc[0]
        assert r1["evidence_status"] == "ok"

    def test_r2_insufficient_evidence(self):
        out = self._build()
        r2 = out[out["report_id"] == "R2"].iloc[0]
        assert r2["evidence_status"] == "insufficient_evidence"
        assert r2["accuracy_deterioration_flag"] == False
        assert math.isnan(r2["wape_change_absolute"])

    def test_r2_insufficient_does_not_affect_r1(self):
        out = self._build()
        r1 = out[out["report_id"] == "R1"].iloc[0]
        assert r1["evidence_status"] == "ok"
        assert pytest.approx(r1["recent_wape"], abs=1e-6) == 0.20


# ---------------------------------------------------------------------------
# TestZeroActualVolume
# Scenario: R1 has zero actuals (WAPE=NaN); R2 has normal actuals.
# ---------------------------------------------------------------------------

class TestZeroActualVolume:
    def _build(self):
        rows = [
            _report_run_row("R1", "run_002", wape=np.nan),
            _report_run_row("R1", "run_001", wape=np.nan),
            _report_run_row("R2", "run_002", wape=0.15),
            _report_run_row("R2", "run_001", wape=0.10),
        ]
        return compute_deterioration_report(_make(rows), config=_CFG)

    def test_r1_no_actuals_status(self):
        out = self._build()
        r1 = out[out["report_id"] == "R1"].iloc[0]
        assert r1["evidence_status"] == "no_actuals"
        assert r1["accuracy_deterioration_flag"] == False

    def test_r2_unaffected(self):
        out = self._build()
        r2 = out[out["report_id"] == "R2"].iloc[0]
        assert r2["evidence_status"] == "ok"
        assert pytest.approx(r2["recent_wape"], abs=1e-6) == 0.15


# ---------------------------------------------------------------------------
# TestModelChangeBetweenRuns
# Scenario: selected_model_family changes between runs.
# This must not appear as a deterioration reason.
# ---------------------------------------------------------------------------

class TestModelChangeBetweenRuns:
    def _build(self):
        rows = [
            _report_run_row(
                "R1", "run_002",
                wape=0.12,
                selected_model_family="auto_arima",
                selected_model_name="SARIMA(1,1,1)(0,1,1,7)",
                selected_m=7,
            ),
            _report_run_row(
                "R1", "run_001",
                wape=0.10,
                selected_model_family="seasonal_naive",
                selected_model_name="seasonal_naive_m7",
                selected_m=7,
            ),
        ]
        return compute_deterioration_report(_make(rows), config=_CFG)

    def test_model_change_does_not_trigger_flag(self):
        """WAPE change of 0.02 is below threshold; model change is not a reason."""
        out = self._build()
        r1 = out[out["report_id"] == "R1"].iloc[0]
        assert r1["accuracy_deterioration_flag"] == False

    def test_no_model_reason_in_reasons(self):
        """No reason string should mention 'model' as a cause."""
        rows = [
            _report_run_row(
                "R1", "run_002", wape=0.30,
                selected_model_family="auto_arima",
            ),
            _report_run_row(
                "R1", "run_001", wape=0.10,
                selected_model_family="seasonal_naive",
            ),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        r1 = out[out["report_id"] == "R1"].iloc[0]
        for reason in r1["deterioration_reasons"]:
            assert "model" not in reason.lower(), (
                f"Deterioration reason should not mention model change: {reason}"
            )


# ---------------------------------------------------------------------------
# TestSelectedMChangeBetweenRuns
# ---------------------------------------------------------------------------

class TestSelectedMChangeBetweenRuns:
    def test_selected_m_change_does_not_trigger_flag(self):
        """A change from m=7 to m=30 must not itself cause deterioration."""
        rows = [
            _report_run_row("R1", "run_002", wape=0.11, selected_m=30),
            _report_run_row("R1", "run_001", wape=0.10, selected_m=7),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        r1 = out[out["report_id"] == "R1"].iloc[0]
        assert r1["accuracy_deterioration_flag"] == False

    def test_no_seasonal_period_reason_in_reasons(self):
        """Reason strings must not reference 'seasonality' or 'seasonal period'."""
        rows = [
            _report_run_row("R1", "run_002", wape=0.30, selected_m=30),
            _report_run_row("R1", "run_001", wape=0.10, selected_m=7),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        r1 = out[out["report_id"] == "R1"].iloc[0]
        for reason in r1["deterioration_reasons"]:
            assert "seasonal" not in reason.lower()
            assert "period"   not in reason.lower()


# ---------------------------------------------------------------------------
# TestPortfolioMetricsNotCopied
# Scenario: verify that the portfolio-average WAPE (which differs from both
# per-report values) is never placed into a report's deterioration fields.
# ---------------------------------------------------------------------------

class TestPortfolioMetricsNotCopied:
    def test_per_report_wape_not_portfolio_average(self):
        """
        Portfolio average of recent WAPE = (0.30 + 0.06) / 2 = 0.18.
        R1 recent = 0.30, R2 recent = 0.06.
        Neither report should show recent_wape == 0.18.
        """
        rows = [
            _report_run_row("R1", "run_002", wape=0.30),
            _report_run_row("R1", "run_001", wape=0.10),
            _report_run_row("R2", "run_002", wape=0.06),
            _report_run_row("R2", "run_001", wape=0.10),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        portfolio_avg_recent = (0.30 + 0.06) / 2  # 0.18

        for _, row in out.iterrows():
            assert row["recent_wape"] != pytest.approx(portfolio_avg_recent, abs=1e-6), (
                f"Report {row['report_id']} shows the portfolio average WAPE "
                f"({portfolio_avg_recent}) instead of its own run-specific WAPE."
            )

    def test_r1_and_r2_have_different_recent_wape(self):
        rows = [
            _report_run_row("R1", "run_002", wape=0.30),
            _report_run_row("R1", "run_001", wape=0.10),
            _report_run_row("R2", "run_002", wape=0.06),
            _report_run_row("R2", "run_001", wape=0.10),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        r1_wape = out[out["report_id"] == "R1"]["recent_wape"].iloc[0]
        r2_wape = out[out["report_id"] == "R2"]["recent_wape"].iloc[0]
        assert r1_wape != r2_wape, (
            "R1 and R2 have different per-report WAPEs; they must not share "
            "the same recent_wape value."
        )


# ---------------------------------------------------------------------------
# TestHelperFunctions
# ---------------------------------------------------------------------------

class TestHelperFunctions:
    def test_safe_float_normal(self):
        assert _safe_float(0.5) == pytest.approx(0.5)

    def test_safe_float_nan_string(self):
        assert math.isnan(_safe_float("nan"))

    def test_safe_float_none(self):
        assert math.isnan(_safe_float(None))

    def test_safe_float_inf(self):
        assert math.isnan(_safe_float(float("inf")))

    def test_delta_normal(self):
        assert _delta(0.3, 0.1) == pytest.approx(0.2)

    def test_delta_with_nan(self):
        assert math.isnan(_delta(float("nan"), 0.1))

    def test_pct_change_normal(self):
        assert _pct_change(0.3, 0.1) == pytest.approx(200.0)

    def test_pct_change_zero_previous(self):
        assert math.isnan(_pct_change(0.3, 0.0))

    def test_bias_description_near_zero(self):
        assert _bias_description(1.0, 2.0) == "near-zero"

    def test_bias_description_overforecasting(self):
        desc = _bias_description(10.0, 2.0)
        assert "overforecasting" in desc

    def test_bias_description_underforecasting(self):
        desc = _bias_description(-10.0, 2.0)
        assert "underforecasting" in desc


# ---------------------------------------------------------------------------
# TestMostRecentRunsSelected
# ---------------------------------------------------------------------------

class TestMostRecentRunsSelected:
    def test_lexicographically_latest_run_ids_selected(self):
        """run_id sort order: run_003 > run_002 > run_001."""
        rows = [
            _report_run_row("R1", "run_001", wape=0.50),
            _report_run_row("R1", "run_003", wape=0.30),
            _report_run_row("R1", "run_002", wape=0.10),
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        row = out.iloc[0]
        assert row["recent_completed_run_id"]   == "run_003"
        assert row["previous_completed_run_id"] == "run_002"

    def test_third_and_older_runs_ignored(self):
        """With three complete runs the oldest (run_001) is not compared."""
        rows = [
            _report_run_row("R1", "run_003", wape=0.10),
            _report_run_row("R1", "run_002", wape=0.09),
            _report_run_row("R1", "run_001", wape=0.50),  # bad but oldest — ignored
        ]
        out = compute_deterioration_report(_make(rows), config=_CFG)
        row = out.iloc[0]
        assert row["accuracy_deterioration_flag"] == False
        assert row["previous_completed_run_id"] == "run_002"
