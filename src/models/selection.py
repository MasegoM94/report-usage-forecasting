"""Per-report model selection from cross-fold performance summaries.

Takes the output of ``summarise_model_performance`` (one row per
report_id × model_name) and returns one row per report_id with the
chosen model and a human-readable explanation.

Selection policy
----------------
1.  Require ``MIN_VALID_FOLDS`` valid backtest folds; models below the
    threshold are excluded before any ranking step.
2.  Apply a scale-invariant bias guardrail: exclude models whose
    ``|mean_bias| / mean_mae`` exceeds ``max_bias_ratio``.
3.  Rank surviving candidates by ``median_mase`` ascending (NaN last).
4.  Build a "practical tie" group: all candidates whose ``median_mase``
    is within ``relative_improvement_tolerance`` (relative) of the best.
5.  Within the tie group, prefer the simpler model using the ordering
    defined in ``MODEL_COMPLEXITY``.
6.  If no candidate survives guardrails, mark the report
    ``no_reliable_model`` with an explanatory reason.

Simplicity ordering (MODEL_COMPLEXITY)
---------------------------------------
naive (0) < seasonal_naive (1) < moving_average (2) < ets (3) < auto_arima (4)
Unknown model names are assigned complexity 99 and therefore always ranked
after all known models within a tie group.

Benchmark
---------
Seasonal naive is used as the primary benchmark regardless of whether it
survived the guardrail steps.  ``seasonal_naive_median_mase`` is always
populated from the input summary when the model ran.
``improvement_vs_seasonal_naive_pct`` is positive when the selected model
is better than seasonal naive:

    (seasonal_naive_mase - selected_mase) / seasonal_naive_mase × 100

When seasonal naive was not evaluated (or has no valid MASE), both
columns are NaN.

Public API
----------
RELATIVE_IMPROVEMENT_TOLERANCE : float
    Default relative MASE gap below which a simpler model is preferred.
MAX_BIAS_RATIO : float
    Default bias guardrail: |mean_bias| / mean_mae must not exceed this.
MODEL_COMPLEXITY : dict[str, int]
    Complexity rank used for tie-breaking.
select_models(model_summary, ...) -> pd.DataFrame
    One row per report_id.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from src.config.forecasting import MIN_VALID_FOLDS

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

RELATIVE_IMPROVEMENT_TOLERANCE: float = 0.05
"""Relative MASE improvement required before a more complex model is preferred.

A complex model whose median MASE is less than 5 % below the simpler model's
MASE is treated as a practical tie, and the simpler model is kept.
"""

MAX_BIAS_RATIO: float = 0.5
"""Bias guardrail: reject a model when |mean_bias| / mean_mae > this value.

A ratio of 0.5 means the systematic directional error is more than half the
average absolute error — indicating the model is consistently pushed in one
direction beyond acceptable limits.  Set to ``None`` to disable the guardrail.
"""

SEASONAL_NAIVE_NAME: str = "seasonal_naive"
"""Canonical model name used as the complexity benchmark."""

MODEL_COMPLEXITY: dict[str, int] = {
    "naive": 0,
    "seasonal_naive": 1,
    "moving_average": 2,
    "ets": 3,
    "auto_arima": 4,
}
"""Lower number = simpler model.  Used to break ties within the tolerance band."""

# Required columns supplied by summarise_model_performance
_REQUIRED_INPUT_COLS = {
    "report_id", "model_name",
    "has_sufficient_folds", "valid_folds", "failed_folds",
    "median_mase", "mean_mase", "mean_wape", "mean_mae",
    "mean_bias", "absolute_mean_bias",
    "fold_win_count", "fold_win_rate",
}

_OUTPUT_COLS = [
    "report_id",
    "selected_model",
    "selection_status",
    "selection_reason",
    "valid_folds",
    "median_mase",
    "mean_wape",
    "mean_bias",
    "fold_win_rate",
    "seasonal_naive_median_mase",
    "improvement_vs_seasonal_naive_pct",
]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _complexity(name: str) -> int:
    return MODEL_COMPLEXITY.get(name, 99)


def _improvement_pct(baseline_mase: float, candidate_mase: float) -> float:
    """Relative MASE reduction vs baseline, in percentage points.

    Positive = candidate is better (lower MASE) than baseline.
    Returns NaN when baseline is zero or either value is NaN.
    """
    if not np.isfinite(baseline_mase) or not np.isfinite(candidate_mase):
        return np.nan
    if baseline_mase == 0.0:
        return np.nan
    return (baseline_mase - candidate_mase) / baseline_mase * 100.0


def _no_model_row(
    report_id: str,
    reason: str,
    seasonal_naive_mase: float = np.nan,
) -> dict:
    return {
        "report_id": report_id,
        "selected_model": None,
        "selection_status": "no_reliable_model",
        "selection_reason": reason,
        "valid_folds": np.nan,
        "median_mase": np.nan,
        "mean_wape": np.nan,
        "mean_bias": np.nan,
        "fold_win_rate": np.nan,
        "seasonal_naive_median_mase": seasonal_naive_mase,
        "improvement_vs_seasonal_naive_pct": np.nan,
    }


def _selected_row(
    report_id: str,
    selected: pd.Series,
    reason: str,
    seasonal_naive_mase: float,
) -> dict:
    candidate_mase = selected["median_mase"]
    return {
        "report_id": report_id,
        "selected_model": selected["model_name"],
        "selection_status": "selected",
        "selection_reason": reason,
        "valid_folds": int(selected["valid_folds"]),
        "median_mase": candidate_mase,
        "mean_wape": selected.get("mean_wape", np.nan),
        "mean_bias": selected.get("mean_bias", np.nan),
        "fold_win_rate": selected.get("fold_win_rate", np.nan),
        "seasonal_naive_median_mase": seasonal_naive_mase,
        "improvement_vs_seasonal_naive_pct": _improvement_pct(seasonal_naive_mase, candidate_mase),
    }


def _select_for_report(
    report_df: pd.DataFrame,
    min_valid_folds: int,
    rel_tol: float,
    max_bias_ratio: Optional[float],
) -> dict:
    """Run the selection policy for a single report_id."""
    report_id: str = report_df["report_id"].iloc[0]

    # --- Seasonal naive benchmark (always extracted, regardless of eligibility) ---
    sn_rows = report_df[report_df["model_name"] == SEASONAL_NAIVE_NAME]
    if not sn_rows.empty and pd.notna(sn_rows["median_mase"].iloc[0]):
        seasonal_naive_mase: float = float(sn_rows["median_mase"].iloc[0])
    else:
        seasonal_naive_mase = np.nan

    # --- 1. Require sufficient valid folds ---
    eligible = report_df[report_df["has_sufficient_folds"]].copy()

    if eligible.empty:
        max_valid = int(report_df["valid_folds"].max()) if not report_df.empty else 0
        return _no_model_row(
            report_id,
            reason=(
                f"No reliable model: fewer than {min_valid_folds} valid folds "
                f"(best was {max_valid})."
            ),
            seasonal_naive_mase=seasonal_naive_mase,
        )

    # --- 2. Bias guardrail ---
    bias_excluded: list[str] = []
    if max_bias_ratio is not None and "absolute_mean_bias" in eligible.columns:
        with np.errstate(divide="ignore", invalid="ignore"):
            ratio = eligible["absolute_mean_bias"] / eligible["mean_mae"]
        passes = ratio.fillna(0.0) <= max_bias_ratio
        bias_excluded = eligible.loc[~passes, "model_name"].tolist()
        eligible = eligible[passes].copy()

    if eligible.empty:
        excluded_str = ", ".join(bias_excluded)
        return _no_model_row(
            report_id,
            reason=(
                f"No reliable model: all candidates excluded by bias guardrail "
                f"(|bias|/MAE > {max_bias_ratio}) — {excluded_str}."
            ),
            seasonal_naive_mase=seasonal_naive_mase,
        )

    # --- 3. Drop models without a finite median MASE, rank remaining ---
    eligible = eligible.dropna(subset=["median_mase"]).copy()

    if eligible.empty:
        return _no_model_row(
            report_id,
            reason="No reliable model: no eligible model produced a valid median MASE.",
            seasonal_naive_mase=seasonal_naive_mase,
        )

    eligible = eligible.sort_values("median_mase", ignore_index=True)
    best_mase: float = float(eligible["median_mase"].iloc[0])
    absolute_best_name: str = eligible["model_name"].iloc[0]

    # --- 4. Build practical tie group (within relative tolerance of best) ---
    tie_threshold = best_mase * (1.0 + rel_tol)
    tie_group = eligible[eligible["median_mase"] <= tie_threshold].copy()

    # --- 5. Prefer the simplest model within the tie group ---
    tie_group["_cplx"] = tie_group["model_name"].map(_complexity)
    tie_group = tie_group.sort_values(["_cplx", "median_mase"], ignore_index=True)
    selected: pd.Series = tie_group.iloc[0]
    selected_name: str = selected["model_name"]
    selected_mase: float = float(selected["median_mase"])

    tie_triggered: bool = selected_name != absolute_best_name

    # --- 6. Build human-readable reason ---
    bias_note = (
        f" ({len(bias_excluded)} model(s) excluded by bias guardrail)"
        if bias_excluded else ""
    )

    if tie_triggered:
        # Simpler model retained due to negligible improvement by the best
        improvement = _improvement_pct(selected_mase, best_mase)
        tol_pct = rel_tol * 100.0
        reason = (
            f"{selected_name.replace('_', ' ').title()} selected: "
            f"{absolute_best_name.replace('_', ' ')} improvement "
            f"({improvement:.1f}%) was below the {tol_pct:.1f}% tolerance"
            f"{bias_note}."
        )
    elif selected_name == SEASONAL_NAIVE_NAME:
        reason = (
            f"Seasonal naive selected: lowest median MASE ({selected_mase:.3f}) "
            f"among eligible models{bias_note}."
        )
    else:
        # Complex model genuinely outperforms all within-tolerance alternatives
        if np.isfinite(seasonal_naive_mase):
            pct = _improvement_pct(seasonal_naive_mase, selected_mase)
            wins = selected.get("fold_win_count", np.nan)
            valid = selected.get("valid_folds", np.nan)
            wins_clause = (
                f" and won {int(wins)} of {int(valid)} folds"
                if pd.notna(wins) and pd.notna(valid) else ""
            )
            reason = (
                f"{selected_name.replace('_', ' ').title()} selected: lowest median MASE "
                f"({selected_mase:.3f}), improved over seasonal naive by "
                f"{pct:.1f}%{wins_clause}{bias_note}."
            )
        else:
            # No seasonal naive benchmark available
            reason = (
                f"{selected_name.replace('_', ' ').title()} selected: lowest median MASE "
                f"({selected_mase:.3f}); no seasonal naive benchmark available{bias_note}."
            )

    return _selected_row(report_id, selected, reason, seasonal_naive_mase)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def select_models(
    model_summary: pd.DataFrame,
    min_valid_folds: int = MIN_VALID_FOLDS,
    relative_improvement_tolerance: float = RELATIVE_IMPROVEMENT_TOLERANCE,
    max_bias_ratio: Optional[float] = MAX_BIAS_RATIO,
) -> pd.DataFrame:
    """Select the best forecasting model for each report from cross-fold summaries.

    Parameters
    ----------
    model_summary:
        Output of ``summarise_model_performance`` — one row per
        (report_id, model_name).  Required columns: ``report_id``,
        ``model_name``, ``has_sufficient_folds``, ``valid_folds``,
        ``failed_folds``, ``median_mase``, ``mean_mase``, ``mean_wape``,
        ``mean_mae``, ``mean_bias``, ``absolute_mean_bias``,
        ``fold_win_count``, ``fold_win_rate``.
    min_valid_folds:
        Minimum valid backtest folds a model must have to be eligible.
        Models below this threshold are excluded before ranking.
        Defaults to ``MIN_VALID_FOLDS`` from the central config.
    relative_improvement_tolerance:
        Relative MASE improvement threshold for preferring a simpler model.
        A complex model must beat the simpler model's MASE by more than
        this fraction to be selected over it.  Default 0.05 (5 %).
    max_bias_ratio:
        Bias guardrail — scale-invariant.  Models whose
        ``|mean_bias| / mean_mae`` exceeds this value are excluded.
        ``None`` disables the guardrail.  Default 0.5.

    Returns
    -------
    pd.DataFrame
        One row per report_id with columns:
        ``report_id``, ``selected_model``, ``selection_status``,
        ``selection_reason``, ``valid_folds``, ``median_mase``,
        ``mean_wape``, ``mean_bias``, ``fold_win_rate``,
        ``seasonal_naive_median_mase``,
        ``improvement_vs_seasonal_naive_pct``.

        ``selection_status`` is either ``"selected"`` or
        ``"no_reliable_model"``.

        Sorted by ``report_id`` ascending.

    Raises
    ------
    ValueError
        If any required column is missing from *model_summary*.
    """
    missing = _REQUIRED_INPUT_COLS - set(model_summary.columns)
    if missing:
        raise ValueError(
            f"model_summary is missing required columns: {sorted(missing)}"
        )

    if model_summary.empty:
        return pd.DataFrame(columns=_OUTPUT_COLS)

    rows: list[dict] = []
    for report_id, group in model_summary.groupby("report_id", sort=False):
        rows.append(
            _select_for_report(
                report_df=group.reset_index(drop=True),
                min_valid_folds=min_valid_folds,
                rel_tol=relative_improvement_tolerance,
                max_bias_ratio=max_bias_ratio,
            )
        )

    result = pd.DataFrame(rows)[_OUTPUT_COLS]
    return result.sort_values("report_id", ignore_index=True)
