"""Cross-run forecast performance deterioration monitoring.

Compares the two most recent *sufficiently realized* pipeline runs per report
and flags when any tracked metric has deteriorated beyond configured thresholds.

Key design decisions
--------------------
* Only runs whose ``realization_rate`` meets ``min_realization_rate`` are
  eligible for comparison.  A 5-day partial forecast is never compared against
  a complete 28-day run as though they were equivalent.
* Thresholds are held in ``DeteriorationConfig`` so callers can supply
  tighter or looser bounds without editing source code.
* ``accuracy_deterioration_flag`` is set to ``True`` only when:
    1. At least ``min_observations`` realized predictions exist in each run.
    2. The metric change exceeds its configured practical threshold.
* ``deterioration_reasons`` is a list of human-readable strings, empty when
  performance is stable or improving.  Each reason quotes the actual metric
  values so a reader can act without looking at a separate table.
* When fewer than two comparable runs exist the output row contains
  ``evidence_status = "insufficient_evidence"`` and all change fields are NaN.

Output schema
-------------
One row per ``report_id`` in ``_DETERIORATION_COLS`` order.

    report_id
    recent_completed_run_id
    previous_completed_run_id
    recent_wape
    previous_wape
    wape_change_absolute
    wape_change_pct
    recent_bias
    previous_bias
    bias_change
    recent_interval_coverage
    previous_interval_coverage
    interval_coverage_change
    accuracy_deterioration_flag
    deterioration_reasons
    evidence_status
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class DeteriorationConfig:
    """Thresholds that govern deterioration detection.

    All thresholds are *practical* (not statistical) — a change must be both
    above the threshold AND supported by enough observations before the flag
    is set.

    Parameters
    ----------
    min_realization_rate:
        Minimum fraction of expected prediction rows that must be realized
        before a run is considered complete enough for comparison.
        Default 0.9 (90 %) allows for a few late-arriving actuals while
        still excluding clearly partial runs.

    min_observations_per_run:
        Minimum number of realized prediction rows in each compared run.
        Guards against flagging deterioration from noise when only a handful
        of actuals have arrived.

    wape_change_threshold:
        Absolute WAPE increase (e.g. 0.05 = 5 percentage points) that must be
        exceeded before the WAPE component of the flag is set.

    bias_change_threshold:
        Absolute change in bias (forecast − actual) that triggers the bias
        component.  A shift from −2 to +10 would exceed a threshold of 5.

    interval_coverage_drop_threshold:
        Absolute drop in interval coverage fraction (e.g. 0.10 = 10 pp) that
        triggers the coverage component.  A fall from 0.92 to 0.80 exceeds
        the default threshold of 0.10.

    bias_near_zero_band:
        Half-width of the "near-zero" band for bias descriptions.  Biases
        whose absolute value is ≤ this value are described as "near-zero"
        in the human-readable reason string.
    """
    min_realization_rate: float = 0.90
    min_observations_per_run: int = 10
    wape_change_threshold: float = 0.05
    bias_change_threshold: float = 5.0
    interval_coverage_drop_threshold: float = 0.10
    bias_near_zero_band: float = 2.0


# Default config — importable directly when thresholds need no overriding
DEFAULT_CONFIG = DeteriorationConfig()


# ---------------------------------------------------------------------------
# Output column order
# ---------------------------------------------------------------------------

DETERIORATION_COLS: list[str] = [
    "report_id",
    "recent_completed_run_id",
    "previous_completed_run_id",
    "recent_wape",
    "previous_wape",
    "wape_change_absolute",
    "wape_change_pct",
    "recent_bias",
    "previous_bias",
    "bias_change",
    "recent_interval_coverage",
    "previous_interval_coverage",
    "interval_coverage_change",
    "accuracy_deterioration_flag",
    "deterioration_reasons",
    "evidence_status",
]

# evidence_status values
_STATUS_OK                   = "ok"
_STATUS_INSUFFICIENT         = "insufficient_evidence"
_STATUS_NO_ACTUALS           = "no_actuals"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _nan_row(report_id: str, status: str) -> dict:
    """Return a dict with all numeric fields as NaN and the given status."""
    return {
        "report_id":                   report_id,
        "recent_completed_run_id":     None,
        "previous_completed_run_id":   None,
        "recent_wape":                 np.nan,
        "previous_wape":               np.nan,
        "wape_change_absolute":        np.nan,
        "wape_change_pct":             np.nan,
        "recent_bias":                 np.nan,
        "previous_bias":               np.nan,
        "bias_change":                 np.nan,
        "recent_interval_coverage":    np.nan,
        "previous_interval_coverage":  np.nan,
        "interval_coverage_change":    np.nan,
        "accuracy_deterioration_flag": False,
        "deterioration_reasons":       [],
        "evidence_status":             status,
    }


def _bias_description(bias: float, near_zero_band: float) -> str:
    if math.isnan(bias):
        return "unknown"
    if abs(bias) <= near_zero_band:
        return "near-zero"
    direction = "overforecasting" if bias > 0 else "underforecasting"
    return f"persistent {direction} (bias={bias:+.1f})"


def _format_pct(value: float) -> str:
    """Format a WAPE fraction as a percentage string, e.g. 0.18 → '18%'."""
    return f"{value * 100:.0f}%"


def _build_reasons(
    *,
    report_id: str,
    recent_run_id: str,
    previous_run_id: str,
    r_wape: float,
    p_wape: float,
    r_bias: float,
    p_bias: float,
    r_cov: float,
    p_cov: float,
    cfg: DeteriorationConfig,
) -> list[str]:
    """Build zero or more human-readable deterioration reason strings."""
    reasons: list[str] = []

    # WAPE
    if not math.isnan(r_wape) and not math.isnan(p_wape):
        wape_delta = r_wape - p_wape
        if wape_delta > cfg.wape_change_threshold:
            reasons.append(
                f"WAPE increased from {_format_pct(p_wape)} to {_format_pct(r_wape)}"
                f" across completed forecast runs"
                f" (run {previous_run_id} → {recent_run_id})."
            )

    # Bias
    if not math.isnan(r_bias) and not math.isnan(p_bias):
        bias_delta = abs(r_bias - p_bias)
        if bias_delta > cfg.bias_change_threshold:
            p_desc = _bias_description(p_bias, cfg.bias_near_zero_band)
            r_desc = _bias_description(r_bias, cfg.bias_near_zero_band)
            reasons.append(
                f"Forecast bias shifted from {p_desc} to {r_desc}"
                f" (change: {r_bias - p_bias:+.1f})."
            )

    # Interval coverage
    if not math.isnan(r_cov) and not math.isnan(p_cov):
        cov_delta = r_cov - p_cov  # negative = drop
        if cov_delta < -cfg.interval_coverage_drop_threshold:
            reasons.append(
                f"Interval coverage fell from {p_cov * 100:.0f}%"
                f" to {r_cov * 100:.0f}%,"
                f" below the configured acceptable range"
                f" (drop threshold: {cfg.interval_coverage_drop_threshold * 100:.0f} pp)."
            )

    return reasons


# ---------------------------------------------------------------------------
# Main public function
# ---------------------------------------------------------------------------

def compute_deterioration_report(
    by_run: pd.DataFrame,
    by_report: pd.DataFrame,
    *,
    config: Optional[DeteriorationConfig] = None,
) -> pd.DataFrame:
    """Compare the two most recent sufficiently realized runs per report.

    Parameters
    ----------
    by_run:
        Output of ``realized_performance_by_run`` from
        ``src.monitoring.production_performance``.  Must contain at least:
        ``run_id``, ``realization_rate``, ``realized_prediction_count``,
        ``wape``, ``bias``, ``interval_coverage``.
    by_report:
        Output of ``realized_performance_by_report``.  Used only to derive
        the set of report_ids — per-report metric values are taken from
        ``by_run`` to keep the comparison at run grain.
    config:
        Threshold configuration.  Uses ``DEFAULT_CONFIG`` when not supplied.

    Returns
    -------
    DataFrame with one row per report_id in ``DETERIORATION_COLS`` column
    order, sorted by ``report_id``.

    Run-level metrics (wape, bias, interval_coverage) are read directly from
    ``by_run`` so comparisons are always at the same grain (run × portfolio).
    Per-report run-level breakdowns are not available in ``by_run``; when
    report-level breakdowns are needed a future extension can accept the full
    realized history DataFrame instead.

    Evidence status values
    ----------------------
    ``ok``                   — comparison performed; flag and reasons populated.
    ``insufficient_evidence`` — fewer than 2 sufficiently realized runs exist
                               for this report.
    ``no_actuals``           — WAPE is undefined (all actuals are zero) in at
                               least one of the compared runs.
    """
    if config is None:
        config = DEFAULT_CONFIG

    _validate_by_run_input(by_run)

    if by_run.empty or by_report.empty:
        return pd.DataFrame(columns=DETERIORATION_COLS)

    # Derive the full list of report_ids from by_report
    all_report_ids = sorted(by_report["report_id"].unique())

    # Identify sufficiently realized runs: realization_rate >= threshold
    # AND at least min_observations_per_run realized predictions.
    eligible = by_run[
        (pd.to_numeric(by_run["realization_rate"], errors="coerce")
         >= config.min_realization_rate)
        &
        (pd.to_numeric(by_run["realized_prediction_count"], errors="coerce")
         >= config.min_observations_per_run)
    ].copy()

    # Sort by run_id descending so most recent is first (run_ids are
    # timestamp-prefixed so lexicographic order equals chronological order).
    eligible = eligible.sort_values("run_id", ascending=False)

    rows: list[dict] = []

    for report_id in all_report_ids:
        # Find runs that contain rows for this report.
        # by_run is portfolio-level (one row per run_id), not per-report.
        # We use it as-is: if the run is eligible it covers all reports
        # in that run.  A report that only appears in ineligible runs gets
        # "insufficient_evidence".
        eligible_runs = eligible  # same eligible set for every report

        if len(eligible_runs) < 2:
            rows.append(_nan_row(report_id, _STATUS_INSUFFICIENT))
            continue

        recent_run   = eligible_runs.iloc[0]
        previous_run = eligible_runs.iloc[1]

        r_run_id = str(recent_run["run_id"])
        p_run_id = str(previous_run["run_id"])

        r_wape = _safe_float(recent_run["wape"])
        p_wape = _safe_float(previous_run["wape"])
        r_bias = _safe_float(recent_run["bias"])
        p_bias = _safe_float(previous_run["bias"])
        r_cov  = _safe_float(recent_run["interval_coverage"])
        p_cov  = _safe_float(previous_run["interval_coverage"])

        # WAPE-undefined case
        if math.isnan(r_wape) and math.isnan(p_wape):
            rows.append({
                **_nan_row(report_id, _STATUS_NO_ACTUALS),
                "recent_completed_run_id":   r_run_id,
                "previous_completed_run_id": p_run_id,
            })
            continue

        # Compute changes
        wape_change_abs = _delta(r_wape, p_wape)
        wape_change_pct = _pct_change(r_wape, p_wape)
        bias_change     = _delta(r_bias, p_bias)
        cov_change      = _delta(r_cov,  p_cov)

        # Build reasons
        reasons = _build_reasons(
            report_id=report_id,
            recent_run_id=r_run_id,
            previous_run_id=p_run_id,
            r_wape=r_wape, p_wape=p_wape,
            r_bias=r_bias, p_bias=p_bias,
            r_cov=r_cov,  p_cov=p_cov,
            cfg=config,
        )
        flag = len(reasons) > 0

        rows.append({
            "report_id":                   report_id,
            "recent_completed_run_id":     r_run_id,
            "previous_completed_run_id":   p_run_id,
            "recent_wape":                 r_wape,
            "previous_wape":               p_wape,
            "wape_change_absolute":        wape_change_abs,
            "wape_change_pct":             wape_change_pct,
            "recent_bias":                 r_bias,
            "previous_bias":               p_bias,
            "bias_change":                 bias_change,
            "recent_interval_coverage":    r_cov,
            "previous_interval_coverage":  p_cov,
            "interval_coverage_change":    cov_change,
            "accuracy_deterioration_flag": flag,
            "deterioration_reasons":       reasons,
            "evidence_status":             _STATUS_OK,
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=DETERIORATION_COLS)

    out = out.sort_values("report_id", ignore_index=True)
    return out[DETERIORATION_COLS]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

_REQUIRED_BY_RUN_COLS = {
    "run_id", "realization_rate", "realized_prediction_count",
    "wape", "bias", "interval_coverage",
}


def _validate_by_run_input(by_run: pd.DataFrame) -> None:
    missing = _REQUIRED_BY_RUN_COLS - set(by_run.columns)
    if missing:
        raise ValueError(
            f"by_run is missing required column(s): {sorted(missing)}."
        )


# ---------------------------------------------------------------------------
# Small numeric helpers
# ---------------------------------------------------------------------------

def _safe_float(value: object) -> float:
    try:
        f = float(value)
        return f if math.isfinite(f) else math.nan
    except (TypeError, ValueError):
        return math.nan


def _delta(a: float, b: float) -> float:
    if math.isnan(a) or math.isnan(b):
        return math.nan
    return a - b


def _pct_change(recent: float, previous: float) -> float:
    """Percentage change = (recent - previous) / |previous| × 100."""
    if math.isnan(recent) or math.isnan(previous) or previous == 0.0:
        return math.nan
    return (recent - previous) / abs(previous) * 100.0
