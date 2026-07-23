"""Cross-fold model performance summaries.

Aggregates fold-level backtest metrics (output of ``evaluate_models_across_folds``)
into one row per ``(report_id, model_name)``, suitable for downstream model
selection and Streamlit display.

Public API
----------
MASE_TIE_TOLERANCE : float
    Absolute tolerance used when declaring fold winners.  Two models whose
    MASE values differ by at most this amount are both credited with winning
    the fold.

summarise_model_performance(fold_metrics, min_valid_folds, mase_tie_tolerance)
    Returns a tidy summary DataFrame sorted by (report_id, mean_mase).

Fold-winner definition
----------------------
For each (report_id, fold_number) a model wins the fold when::

    model_mase ≤ min_valid_mase_in_fold + MASE_TIE_TOLERANCE

where ``min_valid_mase_in_fold`` is the lowest MASE among all models that
had a valid (non-NaN, non-failed) result on that fold.  Ties are therefore
inclusive: multiple models may share a fold win.

``fold_win_rate`` is ``fold_win_count / valid_folds`` — the fraction of folds
this model could participate in that it also won.

Aggregation rules
-----------------
* Failed model-folds (``fit_status == "failed"``) are excluded from every
  numeric aggregate.  Their count is preserved in ``failed_folds``.
* A model with fewer valid folds than ``min_valid_folds`` is flagged via
  ``has_sufficient_folds = False``.  Its summary row is still present; all
  numeric aggregates that can be computed from the available folds are
  included.  Downstream callers decide whether to use or hide these rows.
* ``absolute_mean_bias`` = ``|mean_bias|`` — the magnitude of systematic
  directional error.  A large value means the model consistently over- or
  under-forecasts; the sign of ``mean_bias`` tells which direction.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.config.forecasting import MIN_VALID_FOLDS

# Absolute tolerance for MASE tie-breaking when identifying fold winners.
# Two models whose MASE values differ by ≤ this amount both receive credit
# for winning the fold.
MASE_TIE_TOLERANCE: float = 0.01

# Required columns in the input fold_metrics DataFrame
_REQUIRED_INPUT_COLS = {
    "report_id", "fold_number", "model_name",
    "mae", "rmse", "wape", "mase_lag1", "bias",
    "fit_status",
}

# Output column order
_OUTPUT_COLS = [
    "report_id",
    "model_name",
    "valid_folds",
    "failed_folds",
    "has_sufficient_folds",
    "median_mase_lag1",
    "mean_mase_lag1",
    "mase_lag1_std",
    "mean_wape",
    "mean_mae",
    "mean_rmse",
    "mean_bias",
    "absolute_mean_bias",
    "mean_interval_coverage",
    "mean_interval_width",
    "fold_win_count",
    "fold_win_rate",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fold_winners(
    valid_folds_df: pd.DataFrame,
    tie_tolerance: float,
) -> pd.DataFrame:
    """Return a DataFrame marking which (report_id, fold_number, model_name)
    combinations win their fold.

    A model wins fold F when its MASE is within *tie_tolerance* of the
    lowest valid MASE observed among all models on fold F.

    Parameters
    ----------
    valid_folds_df:
        Subset of fold_metrics containing only non-failed rows that also have
        a finite (non-NaN) MASE value.
    tie_tolerance:
        Inclusive margin above the per-fold minimum MASE for declaring a win.

    Returns
    -------
    DataFrame with columns (report_id, fold_number, model_name, is_fold_winner).
    """
    if valid_folds_df.empty:
        return pd.DataFrame(
            columns=["report_id", "fold_number", "model_name", "is_fold_winner"]
        )

    # Minimum mase_lag1 per (report_id, fold_number) across all competing models
    fold_min = (
        valid_folds_df
        .groupby(["report_id", "fold_number"])["mase_lag1"]
        .min()
        .rename("min_fold_mase")
        .reset_index()
    )

    merged = valid_folds_df[["report_id", "fold_number", "model_name", "mase_lag1"]].merge(
        fold_min, on=["report_id", "fold_number"], how="left"
    )
    merged["is_fold_winner"] = merged["mase_lag1"] <= (merged["min_fold_mase"] + tie_tolerance)
    return merged[["report_id", "fold_number", "model_name", "is_fold_winner"]]


def _aggregate_valid(valid_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate numeric metrics across valid folds for each (report_id, model_name)."""
    optional_cols = {"interval_coverage": "mean_interval_coverage",
                     "mean_interval_width": "mean_interval_width"}

    agg_spec: dict[str, tuple[str, str]] = {
        "valid_folds":      ("mase_lag1", "count"),  # proxy: rows with non-NaN mase_lag1
        "median_mase_lag1": ("mase_lag1", "median"),
        "mean_mase_lag1":   ("mase_lag1", "mean"),
        "mase_lag1_std":    ("mase_lag1", "std"),
        "mean_wape":        ("wape", "mean"),
        "mean_mae":         ("mae", "mean"),
        "mean_rmse":        ("rmse", "mean"),
        "mean_bias":        ("bias", "mean"),
    }

    # Build aggregation using groupby + agg
    agg_fns: dict[str, list] = {}
    for result_col, (src_col, fn) in agg_spec.items():
        agg_fns.setdefault(src_col, []).append((result_col, fn))

    # Flatten into pandas named-aggregation format
    named_agg = {}
    for src_col, pairs in agg_fns.items():
        for result_col, fn in pairs:
            named_agg[result_col] = pd.NamedAgg(column=src_col, aggfunc=fn)

    # Interval metrics — only aggregate if columns exist
    for src_col, dest_col in optional_cols.items():
        if src_col in valid_df.columns:
            named_agg[dest_col] = pd.NamedAgg(column=src_col, aggfunc="mean")

    if valid_df.empty:
        cols = list(named_agg.keys()) + ["report_id", "model_name"]
        return pd.DataFrame(columns=cols)

    summary = (
        valid_df
        .groupby(["report_id", "model_name"])
        .agg(**named_agg)
        .reset_index()
    )

    # Derived columns
    summary["absolute_mean_bias"] = summary["mean_bias"].abs()

    # Add missing optional columns as NaN if not computed
    for _, dest_col in optional_cols.items():
        if dest_col not in summary.columns:
            summary[dest_col] = np.nan

    return summary


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def summarise_model_performance(
    fold_metrics: pd.DataFrame,
    min_valid_folds: int = MIN_VALID_FOLDS,
    mase_tie_tolerance: float = MASE_TIE_TOLERANCE,
) -> pd.DataFrame:
    """Aggregate fold-level metrics into one summary row per (report_id, model_name).

    Parameters
    ----------
    fold_metrics:
        Fold-level evaluation DataFrame produced by ``evaluate_models_across_folds``.
        Required columns: ``report_id``, ``fold_number``, ``model_name``, ``mae``,
        ``rmse``, ``wape``, ``mase``, ``bias``, ``fit_status``.
        Optional: ``interval_coverage``, ``mean_interval_width``.
    min_valid_folds:
        Minimum number of non-failed folds required for a model to be considered
        reliably evaluated.  Models below this threshold have
        ``has_sufficient_folds = False``.  All numeric aggregates that can be
        computed from the available folds are still included.
    mase_tie_tolerance:
        Absolute MASE margin for fold-winner ties.  A model wins a fold when
        its MASE is within this tolerance of the lowest MASE on that fold.
        Defaults to ``MASE_TIE_TOLERANCE = 0.01``.

    Returns
    -------
    pd.DataFrame
        One row per (report_id, model_name).  Columns follow ``_OUTPUT_COLS``:
        report_id, model_name, valid_folds, failed_folds, has_sufficient_folds,
        median_mase, mean_mase, mase_std, mean_wape, mean_mae, mean_rmse,
        mean_bias, absolute_mean_bias, mean_interval_coverage,
        mean_interval_width, fold_win_count, fold_win_rate.

        Sorted by (report_id ascending, mean_mase ascending — NaN last).

    Raises
    ------
    ValueError
        If any required column is missing from *fold_metrics*.

    Notes
    -----
    ``valid_folds`` counts folds where ``fit_status != "failed"`` AND the
    model produced a finite MASE.  ``failed_folds`` counts folds where
    ``fit_status == "failed"``.  A fold can be neither (fit succeeded but
    MASE is NaN due to a zero denominator) — such folds are excluded from
    numeric aggregates but not counted as failed.

    ``fold_win_count`` can exceed ``valid_folds`` only if ties give multiple
    models credit for the same fold.  ``fold_win_rate`` is capped by
    ``valid_folds``, so values above 1.0 are not possible when only one model
    wins each fold.  With ties enabled, the sum of ``fold_win_count`` across
    models for a given (report_id, fold_number) may exceed 1.
    """
    missing_cols = _REQUIRED_INPUT_COLS - set(fold_metrics.columns)
    if missing_cols:
        raise ValueError(
            f"fold_metrics is missing required columns: {sorted(missing_cols)}"
        )

    if fold_metrics.empty:
        return pd.DataFrame(columns=_OUTPUT_COLS)

    # ------------------------------------------------------------------
    # 1.  Separate failed from valid folds
    # ------------------------------------------------------------------
    is_failed = fold_metrics["fit_status"] == "failed"

    failed_counts = (
        fold_metrics[is_failed]
        .groupby(["report_id", "model_name"])
        .size()
        .rename("failed_folds")
        .reset_index()
    )

    # Valid rows: not failed AND mase_lag1 is finite (non-NaN)
    valid_mask = (~is_failed) & fold_metrics["mase_lag1"].notna()
    valid_df = fold_metrics[valid_mask].copy()

    # ------------------------------------------------------------------
    # 2.  Aggregate numeric metrics over valid folds
    # ------------------------------------------------------------------
    agg = _aggregate_valid(valid_df)

    # ------------------------------------------------------------------
    # 3.  Fold-winner statistics
    # ------------------------------------------------------------------
    winners = _fold_winners(valid_df, mase_tie_tolerance)
    if not winners.empty:
        win_counts = (
            winners[winners["is_fold_winner"]]
            .groupby(["report_id", "model_name"])
            .size()
            .rename("fold_win_count")
            .reset_index()
        )
    else:
        win_counts = pd.DataFrame(columns=["report_id", "model_name", "fold_win_count"])

    # ------------------------------------------------------------------
    # 4.  Build the complete model × report index
    #     (include every (report_id, model_name) that appeared in the input,
    #      even if all folds failed)
    # ------------------------------------------------------------------
    all_pairs = (
        fold_metrics[["report_id", "model_name"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    summary = all_pairs.merge(agg, on=["report_id", "model_name"], how="left")
    summary = summary.merge(failed_counts, on=["report_id", "model_name"], how="left")
    summary = summary.merge(win_counts, on=["report_id", "model_name"], how="left")

    # Fill NaN counts with 0
    summary["failed_folds"] = summary["failed_folds"].fillna(0).astype(int)
    summary["fold_win_count"] = summary["fold_win_count"].fillna(0).astype(int)
    summary["valid_folds"] = summary.get("valid_folds", pd.Series(0, index=summary.index))
    summary["valid_folds"] = summary["valid_folds"].fillna(0).astype(int)

    # Derived: sufficiency flag and fold-win rate
    summary["has_sufficient_folds"] = summary["valid_folds"] >= min_valid_folds
    summary["fold_win_rate"] = np.where(
        summary["valid_folds"] > 0,
        summary["fold_win_count"] / summary["valid_folds"],
        np.nan,
    )

    # ------------------------------------------------------------------
    # 5.  Enforce output column order and sort
    # ------------------------------------------------------------------
    # Add any missing optional columns
    for col in _OUTPUT_COLS:
        if col not in summary.columns:
            summary[col] = np.nan

    summary = summary[_OUTPUT_COLS].copy()

    # Sort: report_id ascending, then mean_mase_lag1 ascending with NaN last
    summary = summary.sort_values(
        ["report_id", "mean_mase_lag1"],
        ascending=[True, True],
        na_position="last",
        ignore_index=True,
    )

    return summary
