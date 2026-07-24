"""Tests for src/monitoring/deterioration.py — cross-run deterioration monitoring."""

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

def _run_row(
    run_id: str,
    realization_rate: float = 1.0,
    realized_prediction_count: int = 28,
    wape: float = 0.10,
    bias: float = 0.0,
    interval_coverage: float = 0.90,
) -> dict:
    return {
        "run_id":                    run_id,
        "realization_rate":          realization_rate,
        "realized_prediction_count": realized_prediction_count,
        "wape":                      wape,
        "bias":                      bias,
        "interval_coverage":         interval_coverage,
    }


def _make_by_run(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


def _make_by_report(*report_ids: str) -> pd.DataFrame:
    return pd.DataFrame({"report_id": list(report_ids)})


# Tiny config with low observation threshold so small fixtures work
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
        by_run = _make_by_run(
            _run_row("run_002"),
            _run_row("run_001"),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        assert list(out.columns) == DETERIORATION_COLS

    def test_one_row_per_report(self):
        by_run = _make_by_run(
            _run_row("run_003"),
            _run_row("run_002"),
            _run_row("run_001"),
        )
        by_report = _make_by_report("R1", "R2", "R3")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        assert len(out) == 3
        assert list(out["report_id"]) == ["R1", "R2", "R3"]

    def test_sorted_by_report_id(self):
        by_run = _make_by_run(_run_row("run_002"), _run_row("run_001"))
        by_report = _make_by_report("Z_report", "A_report", "M_report")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        assert list(out["report_id"]) == ["A_report", "M_report", "Z_report"]

    def test_empty_when_both_empty(self):
        out = compute_deterioration_report(
            pd.DataFrame(columns=list(_run_row("x").keys())),
            pd.DataFrame(columns=["report_id"]),
            config=_CFG,
        )
        assert out.empty
        assert list(out.columns) == DETERIORATION_COLS


# ---------------------------------------------------------------------------
# TestClearDeterioration
# ---------------------------------------------------------------------------

class TestClearDeterioration:
    def test_wape_deterioration_flagged(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.30),  # recent — worse
            _run_row("run_001", wape=0.10),  # previous — better
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        row = out.iloc[0]
        assert row["accuracy_deterioration_flag"] == True
        assert row["evidence_status"] == "ok"
        assert any("WAPE" in r for r in row["deterioration_reasons"])

    def test_wape_values_populated(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.30),
            _run_row("run_001", wape=0.10),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        row = out.iloc[0]
        assert pytest.approx(row["recent_wape"],   abs=1e-6) == 0.30
        assert pytest.approx(row["previous_wape"], abs=1e-6) == 0.10
        assert pytest.approx(row["wape_change_absolute"], abs=1e-6) == 0.20
        assert pytest.approx(row["wape_change_pct"],      abs=1e-3) == 200.0

    def test_run_ids_populated(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.30),
            _run_row("run_001", wape=0.10),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        row = out.iloc[0]
        assert row["recent_completed_run_id"]   == "run_002"
        assert row["previous_completed_run_id"] == "run_001"


# ---------------------------------------------------------------------------
# TestStablePerformance
# ---------------------------------------------------------------------------

class TestStablePerformance:
    def test_no_flag_when_wape_unchanged(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.10),
            _run_row("run_001", wape=0.10),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        row = out.iloc[0]
        assert row["accuracy_deterioration_flag"] == False
        assert row["deterioration_reasons"] == []
        assert row["evidence_status"] == "ok"

    def test_no_flag_below_threshold(self):
        # Change of 0.03 is below 0.05 threshold
        by_run = _make_by_run(
            _run_row("run_002", wape=0.13),
            _run_row("run_001", wape=0.10),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        assert out.iloc[0]["accuracy_deterioration_flag"] == False


# ---------------------------------------------------------------------------
# TestImprovingPerformance
# ---------------------------------------------------------------------------

class TestImprovingPerformance:
    def test_no_flag_when_wape_improves(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.05),  # recent — better
            _run_row("run_001", wape=0.20),  # previous — worse
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        row = out.iloc[0]
        assert row["accuracy_deterioration_flag"] == False
        assert row["deterioration_reasons"] == []

    def test_wape_change_negative_when_improving(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.05),
            _run_row("run_001", wape=0.20),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        assert out.iloc[0]["wape_change_absolute"] < 0


# ---------------------------------------------------------------------------
# TestPartialRunExcluded
# ---------------------------------------------------------------------------

class TestPartialRunExcluded:
    def test_partial_run_not_used_in_comparison(self):
        """A run with realization_rate=0.40 must be excluded."""
        by_run = _make_by_run(
            _run_row("run_003", realization_rate=0.40, wape=0.50),  # partial
            _run_row("run_002", realization_rate=1.00, wape=0.10),  # complete
            _run_row("run_001", realization_rate=1.00, wape=0.09),  # complete
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        row = out.iloc[0]
        # recent is run_002, previous is run_001 — run_003 excluded
        assert row["recent_completed_run_id"]   == "run_002"
        assert row["previous_completed_run_id"] == "run_001"

    def test_only_one_complete_run_gives_insufficient_evidence(self):
        """Only run_002 is complete; run_001 is partial — not enough to compare."""
        by_run = _make_by_run(
            _run_row("run_002", realization_rate=1.00, wape=0.10),
            _run_row("run_001", realization_rate=0.30, wape=0.50),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
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
        by_run = _make_by_run(
            _run_row("run_002", realized_prediction_count=5,  wape=0.40),  # too few
            _run_row("run_001", realized_prediction_count=28, wape=0.10),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=cfg)
        assert out.iloc[0]["evidence_status"] == "insufficient_evidence"


# ---------------------------------------------------------------------------
# TestOnlyOneCompletedRun
# ---------------------------------------------------------------------------

class TestOnlyOneCompletedRun:
    def test_single_eligible_run_is_insufficient(self):
        by_run = _make_by_run(
            _run_row("run_001", realization_rate=1.0, wape=0.10),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        row = out.iloc[0]
        assert row["evidence_status"] == "insufficient_evidence"
        assert row["recent_completed_run_id"] is None

    def test_no_eligible_runs_is_insufficient(self):
        by_run = _make_by_run(
            _run_row("run_001", realization_rate=0.20),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        assert out.iloc[0]["evidence_status"] == "insufficient_evidence"


# ---------------------------------------------------------------------------
# TestBiasDeterioration
# ---------------------------------------------------------------------------

class TestBiasDeterioration:
    def test_bias_shift_flagged_without_wape_change(self):
        # WAPE identical; bias changes by 20 (well above threshold of 5)
        by_run = _make_by_run(
            _run_row("run_002", wape=0.10, bias=12.0),
            _run_row("run_001", wape=0.10, bias= 0.5),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        row = out.iloc[0]
        assert row["accuracy_deterioration_flag"] == True
        reasons_text = " ".join(row["deterioration_reasons"])
        assert "bias" in reasons_text.lower()
        # WAPE didn't deteriorate — no WAPE reason
        assert not any("WAPE" in r for r in row["deterioration_reasons"])

    def test_bias_change_computed_correctly(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.10, bias=10.0),
            _run_row("run_001", wape=0.10, bias= 1.0),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        assert pytest.approx(out.iloc[0]["bias_change"], abs=1e-6) == 9.0

    def test_near_zero_to_persistent_description(self):
        desc = _bias_description(0.5, near_zero_band=2.0)
        assert desc == "near-zero"
        desc2 = _bias_description(15.0, near_zero_band=2.0)
        assert "overforecasting" in desc2

    def test_small_bias_change_below_threshold_not_flagged(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.10, bias=3.0),
            _run_row("run_001", wape=0.10, bias=1.0),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        assert out.iloc[0]["accuracy_deterioration_flag"] == False


# ---------------------------------------------------------------------------
# TestIntervalCoverageDeterioration
# ---------------------------------------------------------------------------

class TestIntervalCoverageDeterioration:
    def test_coverage_drop_flagged(self):
        # Coverage falls from 0.92 to 0.75 (−0.17, above 0.10 threshold)
        by_run = _make_by_run(
            _run_row("run_002", wape=0.10, interval_coverage=0.75),
            _run_row("run_001", wape=0.10, interval_coverage=0.92),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        row = out.iloc[0]
        assert row["accuracy_deterioration_flag"] == True
        assert any("coverage" in r.lower() for r in row["deterioration_reasons"])

    def test_coverage_change_computed(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.10, interval_coverage=0.75),
            _run_row("run_001", wape=0.10, interval_coverage=0.92),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        assert pytest.approx(out.iloc[0]["interval_coverage_change"], abs=1e-6) == -0.17

    def test_coverage_improvement_not_flagged(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.10, interval_coverage=0.95),
            _run_row("run_001", wape=0.10, interval_coverage=0.70),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        assert out.iloc[0]["accuracy_deterioration_flag"] == False

    def test_small_coverage_drop_below_threshold(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.10, interval_coverage=0.86),
            _run_row("run_001", wape=0.10, interval_coverage=0.90),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        assert out.iloc[0]["accuracy_deterioration_flag"] == False


# ---------------------------------------------------------------------------
# TestDeterministicReasons
# ---------------------------------------------------------------------------

class TestDeterministicReasons:
    def test_same_input_same_reasons(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.30, bias=15.0, interval_coverage=0.60),
            _run_row("run_001", wape=0.10, bias= 0.5, interval_coverage=0.92),
        )
        by_report = _make_by_report("R1")
        out1 = compute_deterioration_report(by_run, by_report, config=_CFG)
        out2 = compute_deterioration_report(by_run, by_report, config=_CFG)
        assert out1.iloc[0]["deterioration_reasons"] == out2.iloc[0]["deterioration_reasons"]

    def test_reason_includes_metric_values(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.30),
            _run_row("run_001", wape=0.10),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        reason = out.iloc[0]["deterioration_reasons"][0]
        # Reason must quote from% and to%
        assert "10%" in reason or "30%" in reason

    def test_multiple_reasons_when_multiple_metrics_degrade(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.30, bias=15.0, interval_coverage=0.60),
            _run_row("run_001", wape=0.10, bias= 0.5, interval_coverage=0.92),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        reasons = out.iloc[0]["deterioration_reasons"]
        assert len(reasons) == 3  # WAPE + bias + coverage

    def test_no_reasons_when_stable(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=0.10, bias=0.5, interval_coverage=0.90),
            _run_row("run_001", wape=0.10, bias=0.5, interval_coverage=0.90),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        assert out.iloc[0]["deterioration_reasons"] == []


# ---------------------------------------------------------------------------
# TestNoActuals
# ---------------------------------------------------------------------------

class TestNoActuals:
    def test_nan_wape_gives_no_actuals_status(self):
        by_run = _make_by_run(
            _run_row("run_002", wape=np.nan),
            _run_row("run_001", wape=np.nan),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
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
        bad = pd.DataFrame({"run_id": ["r1"], "wape": [0.1]})
        by_report = _make_by_report("R1")
        with pytest.raises(ValueError, match="missing required column"):
            compute_deterioration_report(bad, by_report, config=_CFG)


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
        # (0.3 - 0.1) / 0.1 * 100 = 200
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
        by_run = _make_by_run(
            _run_row("run_001", wape=0.50),
            _run_row("run_003", wape=0.30),
            _run_row("run_002", wape=0.10),
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        row = out.iloc[0]
        assert row["recent_completed_run_id"]   == "run_003"
        assert row["previous_completed_run_id"] == "run_002"

    def test_third_and_older_runs_ignored(self):
        """With three complete runs the oldest (run_001) is not compared."""
        by_run = _make_by_run(
            _run_row("run_003", wape=0.10),
            _run_row("run_002", wape=0.09),
            _run_row("run_001", wape=0.50),  # very bad but oldest — ignored
        )
        by_report = _make_by_report("R1")
        out = compute_deterioration_report(by_run, by_report, config=_CFG)
        row = out.iloc[0]
        # wape_change = 0.10 - 0.09 = 0.01 → below threshold
        assert row["accuracy_deterioration_flag"] == False
        assert row["previous_completed_run_id"] == "run_002"
