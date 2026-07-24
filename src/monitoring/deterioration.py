"""Per-report cross-run forecast performance deterioration monitoring.

Compares the two most recent *sufficiently realized* pipeline runs **for each
report individually** and flags when any tracked metric has deteriorated beyond
configured thresholds.

Key design decisions
--------------------
* The input table is ``realized_performance_by_report_run`` — one row per
  (report_id, run_id).  Eligible runs are selected **per report**, so a report
  that was only partially realized in the most recent portfolio run still
  participates if it was fully realized in that specific run.
* This replaces the previous portfolio-level approach where every report
  received the same recent/previous metrics from the portfolio-level run table.
* Thresholds are held in ``DeteriorationConfig`` so callers can supply
  tighter or looser bounds without editing source code.
* ``accuracy_deterioration_flag`` is set to ``True`` only when:
    1. At least ``min_observations_per_run`` realized predictions exist for
       this report in each compared run.
    2. The metric change exceeds its configured practical threshold.
* ``deterioration_reasons`` is a list of human-readable strings, empty when
  performance is stable or improving.  Each reason quotes the actual metric
  values so a reader can act without looking at a separate table.
* Model or selected_m changes between runs are recorded as context only —
  the function does NOT attribute deterioration to a model change.
* When fewer than two comparable report-runs exist the output row contains
  ``evidence_status = "insufficient_evidence"`` and all change fields are NaN.

Output schema
-------------
One row per ``report_id`` in ``DETERIORATION_COLS`` order.

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
from dataclasses import dataclass
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
        before a report-run is considered comparable.
        Default 0.9 (90 %) allows for a few late-arriving actuals while
        still excluding clearly partial runs.

    min_observations_per_run:
        Minimum number of realized prediction rows for this report in each
        compared run.  Guards against flagging deterioration from noise when
        only a handful of actuals have arrived.

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
_STATUS_OK           = "ok"
_STATUS_INSUFFICIENT = "insufficient_evidence"
_STATUS_NO_ACTUALS   = "no_actuals"


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
    """Build zero or more human-readable deterioration reason strings.

    Reason ordering is deterministic: WAPE first, then bias, then coverage.
    Model changes are NOT reported as a deterioration cause.
    """
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

_REQUIRED_BY_REPORT_RUN_COLS = {
    "report_id", "run_id", "realization_rate", "realized_prediction_count",
    "wape", "bias", "interval_coverage",
}


def compute_deterioration_report(
    by_report_run: pd.DataFrame,
    *,
    config: Optional[DeteriorationConfig] = None,
) -> pd.DataFrame:
    """Compare the two most recent comparable runs **per report**.

    Unlike the previous portfolio-level approach, this function selects
    eligible runs separately for each report.  Two reports in the same
    pipeline run can therefore have different recent/previous metrics and
    different deterioration flags.

    Parameters
    ----------
    by_report_run:
        Output of ``realized_performance_by_report_run`` from
        ``src.monitoring.production_performance``.  Must contain at least:
        ``report_id``, ``run_id``, ``realization_rate``,
        ``realized_prediction_count``, ``wape``, ``bias``,
        ``interval_coverage``.
        One row per (report_id, run_id).
    config:
        Threshold configuration.  Uses ``DEFAULT_CONFIG`` when not supplied.

    Returns
    -------
    DataFrame with one row per report_id in ``DETERIORATION_COLS`` column
    order, sorted by ``report_id``.

    Eligible-run selection
    ----------------------
    For each report, a run is eligible when BOTH:
    * ``realization_rate >= config.min_realization_rate``
    * ``realized_prediction_count >= config.min_observations_per_run``

    The two most recent eligible runs (by ``run_id`` lexicographic descending,
    which equals chronological descending for timestamp-prefixed IDs) are
    compared.  Runs that do not meet both criteria are excluded for that
    report regardless of whether they qualify for other reports.

    Evidence status values
    ----------------------
    ``ok``                    — comparison performed; flag and reasons populated.
    ``insufficient_evidence`` — fewer than 2 comparable runs exist for this
                                report.
    ``no_actuals``            — WAPE is undefined (all actuals are zero) in
                                at least one of the compared runs.
    """
    if config is None:
        config = DEFAULT_CONFIG

    _validate_by_report_run_input(by_report_run)

    if by_report_run.empty:
        return pd.DataFrame(columns=DETERIORATION_COLS)

    all_report_ids = sorted(by_report_run["report_id"].unique())

    rows: list[dict] = []

    for report_id in all_report_ids:
        # --- select eligible runs for THIS report only ---
        report_rows = by_report_run[by_report_run["report_id"] == report_id]

        rate_ok = (
            pd.to_numeric(report_rows["realization_rate"], errors="coerce")
            >= config.min_realization_rate
        )
        count_ok = (
            pd.to_numeric(report_rows["realized_prediction_count"], errors="coerce")
            >= config.min_observations_per_run
        )
        eligible = report_rows[rate_ok & count_ok].copy()

        # Sort descending by run_id: lexicographic order == chronological order
        # for timestamp-prefixed run IDs.
        eligible = eligible.sort_values("run_id", ascending=False)

        if len(eligible) < 2:
            rows.append(_nan_row(report_id, _STATUS_INSUFFICIENT))
            continue

        recent_row   = eligible.iloc[0]
        previous_row = eligible.iloc[1]

        r_run_id = str(recent_row["run_id"])
        p_run_id = str(previous_row["run_id"])

        r_wape = _safe_float(recent_row["wape"])
        p_wape = _safe_float(previous_row["wape"])
        r_bias = _safe_float(recent_row["bias"])
        p_bias = _safe_float(previous_row["bias"])
        r_cov  = _safe_float(recent_row["interval_coverage"])
        p_cov  = _safe_float(previous_row["interval_coverage"])

        # WAPE-undefined case (zero actual volume in both compared runs)
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

def _validate_by_report_run_input(by_report_run: pd.DataFrame) -> None:
    missing = _REQUIRED_BY_REPORT_RUN_COLS - set(by_report_run.columns)
    if missing:
        raise ValueError(
            f"by_report_run is missing required column(s): {sorted(missing)}."
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
