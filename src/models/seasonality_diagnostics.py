"""Durable seasonality diagnostic outputs for portfolio review and governance.

Two tidy DataFrames are produced:

``build_seasonality_summary``
    One row per ``report_id``.  Joins series metadata, fold-level seasonality
    profiles, backtest selection outcomes, and production forecast status into
    a single governance-ready view.

``build_seasonality_candidates``
    One row per ``(report_id, fold_number, candidate_m)``.  Exposes per-fold
    per-period diagnostic signals (autocorrelation, spectral power, composite
    score, eligibility) alongside the models evaluated on each candidate and
    their aggregate fold-level outcomes.

Design rules
------------
* m=1 (``NON_SEASONAL_PERIOD``) is always represented as the non-seasonal
  candidate.
* ``dominant_diagnostic_period`` is the period with the highest composite
  score across evaluated folds.  It is **not** described as the selected
  production m unless backtesting independently selected the same value.
* The four period concepts (diagnostically strongest, shortlisted candidate,
  backtest-selected, production period actually used) are preserved as
  separate columns.
* List-like fields (e.g. ``eligible_seasonal_periods``) are serialised as
  pipe-delimited strings (``"7|14|28"``).  An empty list serialises as ``""``.
* Outputs are sorted deterministically:
    - summary: by ``report_id``
    - candidates: by ``(report_id, fold_number, candidate_rank, candidate_m)``

Schema validation is provided via ``validate_seasonality_summary`` and
``validate_seasonality_candidates``, which raise ``ValueError`` on violation.

Public API
----------
SUMMARY_COLS : list[str]
CANDIDATE_COLS : list[str]
build_seasonality_summary(...)
build_seasonality_candidates(...)
validate_seasonality_summary(df)
validate_seasonality_candidates(df)
save_seasonality_diagnostics(summary, candidates, project_root)
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.config.forecasting import (
    MIN_SEASONAL_CYCLES,
    NON_SEASONAL_PERIOD,
    SEASONAL_CANDIDATES,
)

# ---------------------------------------------------------------------------
# Column-order constants (authoritative schema)
# ---------------------------------------------------------------------------

SUMMARY_COLS: list[str] = [
    "report_id",
    "history_days",
    "first_observed_date",
    "last_observed_date",
    "zero_share",
    "seasonality_status",
    "dominant_diagnostic_period",
    "dominant_period_score",
    "eligible_seasonal_periods",
    "excluded_seasonal_periods",
    "selected_model_family",
    "selected_model_name",
    "selected_m",
    "valid_backtest_folds",
    "median_mase",
    "mean_wape",
    "mean_bias",
    "fold_win_rate",
    "improvement_vs_best_baseline_pct",
    "selection_reason",
    "production_fit_status",
    "fallback_used",
    "fallback_reason",
]

CANDIDATE_COLS: list[str] = [
    "report_id",
    "fold_number",
    "cutoff_date",
    "candidate_m",
    "candidate_eligible",
    "exclusion_reason",
    "cycles_available",
    "autocorrelation_at_m",
    "spectral_power_at_m",
    "candidate_score",
    "candidate_rank",
    "seasonality_status",
    "candidate_source",
    "models_evaluated",
    "valid_model_count",
    "failed_model_count",
]

# Pipe delimiter for list-like fields
_LIST_SEP = "|"

# Valid seasonality_status values (from SeasonalityProfile)
_VALID_STATUSES = frozenset(
    {"seasonal", "non_seasonal", "insufficient_history", "degenerate", "unknown"}
)

# Valid candidate_m values (SEASONAL_CANDIDATES plus m=1)
_VALID_CANDIDATE_MS = frozenset({NON_SEASONAL_PERIOD} | set(SEASONAL_CANDIDATES))


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _encode_list(values: list[int]) -> str:
    """Serialise an integer list as a pipe-delimited string.

    ``[7, 14, 28]`` → ``"7|14|28"``
    ``[]``           → ``""``
    """
    return _LIST_SEP.join(str(v) for v in sorted(values))


def _decode_list(s: str) -> list[int]:
    """Parse a pipe-delimited string back to a sorted integer list.

    ``"7|14|28"`` → ``[7, 14, 28]``
    ``""``         → ``[]``
    """
    if not s or pd.isna(s):
        return []
    return sorted(int(x) for x in str(s).split(_LIST_SEP) if x.strip())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _series_meta(series: pd.Series) -> dict:
    """Return basic metadata for a single report series."""
    n = len(series)
    if n == 0:
        return {
            "history_days": 0,
            "first_observed_date": pd.NaT,
            "last_observed_date": pd.NaT,
            "zero_share": np.nan,
        }
    values = series.to_numpy(dtype=float)
    zero_share = float((values == 0).sum()) / n
    return {
        "history_days": n,
        "first_observed_date": series.index.min(),
        "last_observed_date": series.index.max(),
        "zero_share": round(zero_share, 6),
    }


def _dominant_period_from_fold_metrics(
    fold_metrics: pd.DataFrame,
    report_id: str,
) -> tuple[Optional[int], float]:
    """Find the diagnostically dominant period for *report_id*.

    The dominant period is the candidate_m (excluding m=1) with the highest
    mean ``candidate_score`` across valid folds.  Returns ``(None, nan)`` when
    no seasonal candidates were evaluated.
    """
    if fold_metrics.empty:
        return None, float("nan")

    needed = {"candidate_m", "autocorrelation_at_m", "spectral_power_at_m", "fit_status"}
    if not needed.issubset(fold_metrics.columns):
        return None, float("nan")

    sub = fold_metrics[
        (fold_metrics["report_id"] == report_id)
        & (fold_metrics["candidate_m"] > NON_SEASONAL_PERIOD)
        & (fold_metrics["fit_status"] != "failed")
    ].copy()

    if sub.empty:
        return None, float("nan")

    # Compute a simple composite score per row from raw diagnostic signals
    # (these are already normalised within each fold by the profiler, but we
    # want a cross-fold signal here so we use the raw values directly).
    # Use 0.5 × acf_norm + 0.5 × spectral_norm approximated per-row.
    acf = sub["autocorrelation_at_m"].clip(lower=0)
    sp = sub["spectral_power_at_m"].clip(lower=0)
    # Normalise across all rows of this report so the scale is consistent
    max_acf = acf.max()
    max_sp = sp.max()
    acf_n = acf / max_acf if max_acf > 0 else acf * 0
    sp_n = sp / max_sp if max_sp > 0 else sp * 0
    sub = sub.copy()
    sub["_score"] = 0.5 * acf_n + 0.5 * sp_n

    by_period = sub.groupby("candidate_m")["_score"].mean()
    if by_period.empty:
        return None, float("nan")

    dominant_m = int(by_period.idxmax())
    dominant_score = float(by_period.max())
    return dominant_m, round(dominant_score, 6)


def _aggregate_seasonality_status(
    fold_metrics: pd.DataFrame,
    report_id: str,
) -> str:
    """Return the most common seasonality_status across folds for a report."""
    if fold_metrics.empty or "seasonality_status" not in fold_metrics.columns:
        return "unknown"
    sub = fold_metrics[fold_metrics["report_id"] == report_id]["seasonality_status"]
    if sub.empty:
        return "unknown"
    mode_result = sub.mode()
    return str(mode_result.iloc[0]) if not mode_result.empty else "unknown"


def _eligible_excluded_periods(
    fold_metrics: pd.DataFrame,
    report_id: str,
    candidate_periods: tuple[int, ...],
) -> tuple[list[int], list[int]]:
    """Return (eligible_periods, excluded_periods) for a report.

    A period is eligible if it appears in ``fold_metrics`` with candidate_m >
    1 and at least one non-failed fold.  A period from ``candidate_periods``
    that does not appear as eligible is considered excluded.
    """
    if fold_metrics.empty or "candidate_m" not in fold_metrics.columns:
        return [], list(candidate_periods)

    sub = fold_metrics[
        (fold_metrics["report_id"] == report_id)
        & (fold_metrics["candidate_m"] > NON_SEASONAL_PERIOD)
        & (fold_metrics["fit_status"] != "failed")
    ]
    eligible = sorted(sub["candidate_m"].unique().tolist())
    excluded = sorted(m for m in candidate_periods if m not in eligible)
    return eligible, excluded


def _improvement_vs_best_baseline(
    selection_row: pd.Series,
    candidate_summary: pd.DataFrame,
    report_id: str,
) -> float:
    """Compute improvement of selected model over the best non-seasonal baseline.

    Returns ``(baseline_mase - selected_mase) / baseline_mase × 100``.
    Falls back to ``improvement_vs_seasonal_naive_pct`` from the selection row
    when the candidate summary does not contain baseline rows.
    """
    if selection_row.empty:
        return float("nan")

    selected_mase = _safe_float(selection_row.get("median_mase"))
    if pd.isna(selected_mase):
        return float("nan")

    # Try to compute from candidate_summary (baseline = naive or moving_average)
    if not candidate_summary.empty and "model_family" in candidate_summary.columns:
        baselines = candidate_summary[
            (candidate_summary["report_id"] == report_id)
            & (candidate_summary["model_family"].isin(["naive", "moving_average"]))
            & (candidate_summary["has_sufficient_folds"] == True)
        ]
        if not baselines.empty and "median_mase" in baselines.columns:
            best_baseline_mase = float(baselines["median_mase"].dropna().min())
            if pd.notna(best_baseline_mase) and best_baseline_mase > 0:
                return round(
                    (best_baseline_mase - selected_mase) / best_baseline_mase * 100, 4
                )

    # Fall back to improvement_vs_seasonal_naive_pct from selection
    return _safe_float(selection_row.get("improvement_vs_seasonal_naive_pct", float("nan")))


def _safe_float(v) -> float:
    try:
        f = float(v)
        return f if np.isfinite(f) else float("nan")
    except (TypeError, ValueError):
        return float("nan")


def _safe_bool(v) -> Optional[bool]:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    return bool(v)


# ---------------------------------------------------------------------------
# Public builders
# ---------------------------------------------------------------------------

def build_seasonality_summary(
    series_dict: dict[str, pd.Series],
    selection: pd.DataFrame,
    fold_metrics: pd.DataFrame,
    production_df: pd.DataFrame,
    candidate_summary: Optional[pd.DataFrame] = None,
    candidate_periods: tuple[int, ...] = SEASONAL_CANDIDATES,
) -> pd.DataFrame:
    """Build ``report_seasonality_summary`` — one row per report_id.

    Parameters
    ----------
    series_dict:
        Per-report daily view series (output of
        ``build_daily_series_for_all_reports``).
    selection:
        Output of ``select_candidate_models``; one row per report_id.
        Required columns: ``report_id``, ``selected_model_family``,
        ``selected_model_name``, ``selected_m``, ``selection_reason``,
        ``valid_folds``, ``median_mase``, ``mean_wape``, ``mean_bias``,
        ``fold_win_rate``.
    fold_metrics:
        Output of ``evaluate_candidates_across_folds``; one row per
        (report_id, fold_number, candidate).  Used for diagnostic signals.
    production_df:
        Output of ``build_production_forecast``; one row per
        (report_id, horizon_step) or a single placeholder row per report.
        Used to resolve ``production_fit_status``, ``fallback_used``, and
        ``fallback_reason``.
    candidate_summary:
        Output of ``summarise_candidate_performance`` (optional).  Used for
        baseline improvement computation.
    candidate_periods:
        Candidate periods used during profiling; governs excluded_periods.

    Returns
    -------
    pd.DataFrame
        One row per report_id with columns matching ``SUMMARY_COLS``,
        sorted by report_id.
    """
    if candidate_summary is None:
        candidate_summary = pd.DataFrame()

    # Build report → selection lookup
    sel_lookup: dict[str, pd.Series] = {}
    if not selection.empty and "report_id" in selection.columns:
        for _, row in selection.iterrows():
            sel_lookup[str(row["report_id"])] = row

    # Build report → first production row lookup
    prod_lookup: dict[str, pd.Series] = {}
    if not production_df.empty and "report_id" in production_df.columns:
        for rid, grp in production_df.groupby("report_id"):
            # Prefer first successful forecast row; fall back to first row
            fc_rows = grp[grp["horizon_step"].notna()]
            prod_lookup[str(rid)] = fc_rows.iloc[0] if not fc_rows.empty else grp.iloc[0]

    records: list[dict] = []
    all_report_ids = sorted(set(series_dict.keys()) | set(sel_lookup.keys()))

    for rid in all_report_ids:
        series = series_dict.get(rid)
        meta = _series_meta(series) if series is not None else {
            "history_days": 0,
            "first_observed_date": pd.NaT,
            "last_observed_date": pd.NaT,
            "zero_share": np.nan,
        }

        sel_row = sel_lookup.get(rid, pd.Series(dtype=object))
        prod_row = prod_lookup.get(rid, pd.Series(dtype=object))

        dominant_m, dominant_score = _dominant_period_from_fold_metrics(fold_metrics, rid)
        seasonality_status = _aggregate_seasonality_status(fold_metrics, rid)
        eligible_periods, excluded_periods = _eligible_excluded_periods(
            fold_metrics, rid, candidate_periods
        )

        selected_m_raw = sel_row.get("selected_m") if len(sel_row) else None
        selected_m: Optional[int] = None
        if selected_m_raw is not None and pd.notna(selected_m_raw):
            selected_m = int(selected_m_raw)

        records.append({
            "report_id": rid,
            "history_days": meta["history_days"],
            "first_observed_date": meta["first_observed_date"],
            "last_observed_date": meta["last_observed_date"],
            "zero_share": meta["zero_share"],
            "seasonality_status": seasonality_status,
            # dominant_diagnostic_period is the diagnostically strongest detected
            # period — NOT forced to equal selected_m.
            "dominant_diagnostic_period": dominant_m,
            "dominant_period_score": dominant_score,
            "eligible_seasonal_periods": _encode_list(eligible_periods),
            "excluded_seasonal_periods": _encode_list(excluded_periods),
            # Selection lineage — directly from select_candidate_models output
            "selected_model_family": sel_row.get("selected_model_family") if len(sel_row) else None,
            "selected_model_name": sel_row.get("selected_model_name") if len(sel_row) else None,
            "selected_m": selected_m,
            "valid_backtest_folds": int(sel_row["valid_folds"]) if len(sel_row) and pd.notna(sel_row.get("valid_folds")) else None,
            "median_mase": _safe_float(sel_row.get("median_mase")) if len(sel_row) else float("nan"),
            "mean_wape": _safe_float(sel_row.get("mean_wape")) if len(sel_row) else float("nan"),
            "mean_bias": _safe_float(sel_row.get("mean_bias")) if len(sel_row) else float("nan"),
            "fold_win_rate": _safe_float(sel_row.get("fold_win_rate")) if len(sel_row) else float("nan"),
            "improvement_vs_best_baseline_pct": _improvement_vs_best_baseline(
                sel_row, candidate_summary, rid
            ),
            "selection_reason": sel_row.get("selection_reason") if len(sel_row) else None,
            # Production status — from build_production_forecast output
            "production_fit_status": prod_row.get("production_fit_status") if len(prod_row) else None,
            "fallback_used": _safe_bool(prod_row.get("fallback_used")) if len(prod_row) else None,
            "fallback_reason": prod_row.get("fallback_reason") if len(prod_row) else None,
        })

    df = pd.DataFrame(records, columns=SUMMARY_COLS)
    return df.sort_values("report_id", ignore_index=True)


def build_seasonality_candidates(
    fold_metrics: pd.DataFrame,
    candidate_periods: tuple[int, ...] = SEASONAL_CANDIDATES,
    min_cycles: int = MIN_SEASONAL_CYCLES,
) -> pd.DataFrame:
    """Build ``report_seasonality_candidates`` — one row per
    ``(report_id, fold_number, candidate_m)``.

    The non-seasonal candidate (m=1) is always included.  For each
    (report_id, fold_number, candidate_m) triple the row records:

    * Whether the candidate was eligible (passed the cycles gate).
    * The raw diagnostic signals (ACF, spectral power, composite score).
    * How many models were evaluated on this candidate in this fold.
    * How many succeeded vs failed.

    Parameters
    ----------
    fold_metrics:
        Output of ``evaluate_candidates_across_folds``; one row per
        (report_id, fold_number, candidate).
    candidate_periods:
        Full set of candidate periods (including those excluded for history).
    min_cycles:
        Minimum cycles required for eligibility (used to compute whether a
        candidate that is absent from fold_metrics would have been eligible).

    Returns
    -------
    pd.DataFrame
        Columns matching ``CANDIDATE_COLS``.  Sorted by
        ``(report_id, fold_number, candidate_rank, candidate_m)``.
    """
    if fold_metrics.empty:
        return pd.DataFrame(columns=CANDIDATE_COLS)

    required = {"report_id", "fold_number", "cutoff_date", "candidate_m", "fit_status"}
    missing = required - set(fold_metrics.columns)
    if missing:
        raise ValueError(
            f"fold_metrics is missing required columns: {sorted(missing)}"
        )

    records: list[dict] = []

    for (rid, fold_num), fold_grp in fold_metrics.groupby(
        ["report_id", "fold_number"], sort=True
    ):
        cutoff = fold_grp["cutoff_date"].iloc[0]

        # All candidate_m values present in this fold
        evaluated_ms = set(fold_grp["candidate_m"].unique())

        # Determine training length from fold data (cycles_available for m=1)
        ns_row = fold_grp[fold_grp["candidate_m"] == NON_SEASONAL_PERIOD]
        n_train: Optional[int] = None
        if not ns_row.empty and "cycles_available" in fold_grp.columns:
            n_train_raw = ns_row["cycles_available"].iloc[0]
            n_train = int(n_train_raw) if pd.notna(n_train_raw) else None

        # Build one row per (fold, m) — include all evaluated m values plus
        # any missing periods that should have been candidates (for excluded rows)
        all_ms = sorted(evaluated_ms | set(candidate_periods) | {NON_SEASONAL_PERIOD})

        for m in all_ms:
            m_grp = fold_grp[fold_grp["candidate_m"] == m]
            in_fold = not m_grp.empty

            # Eligibility
            if in_fold:
                # Present in fold — was evaluated, hence eligible
                eligible = True
                excl_reason = ""
            elif m == NON_SEASONAL_PERIOD:
                # m=1 is always a non-seasonal baseline; always eligible
                eligible = True
                excl_reason = ""
            else:
                # Absent from fold — determine why
                if n_train is not None and m > NON_SEASONAL_PERIOD:
                    cycles = n_train // m
                    if cycles < min_cycles:
                        eligible = False
                        excl_reason = (
                            f"insufficient history: {cycles} cycle(s) available, "
                            f"need ≥{min_cycles}"
                        )
                    else:
                        eligible = False
                        excl_reason = "not shortlisted by seasonality profiler"
                else:
                    eligible = False
                    excl_reason = "not evaluated in this fold"

            # Diagnostic signals
            if in_fold and "autocorrelation_at_m" in fold_grp.columns:
                acf_val = float(m_grp["autocorrelation_at_m"].iloc[0]) if not m_grp["autocorrelation_at_m"].isna().all() else float("nan")
            else:
                acf_val = float("nan")

            if in_fold and "spectral_power_at_m" in fold_grp.columns:
                sp_val = float(m_grp["spectral_power_at_m"].iloc[0]) if not m_grp["spectral_power_at_m"].isna().all() else float("nan")
            else:
                sp_val = float("nan")

            # Composite candidate_score: same formula as SeasonalityProfile
            if in_fold and pd.notna(acf_val) and pd.notna(sp_val):
                max_acf_fold = float(
                    fold_grp[fold_grp["candidate_m"] > NON_SEASONAL_PERIOD]["autocorrelation_at_m"]
                    .clip(lower=0).max()
                ) if not fold_grp[fold_grp["candidate_m"] > NON_SEASONAL_PERIOD].empty else 0.0
                max_sp_fold = float(
                    fold_grp[fold_grp["candidate_m"] > NON_SEASONAL_PERIOD]["spectral_power_at_m"]
                    .clip(lower=0).max()
                ) if not fold_grp[fold_grp["candidate_m"] > NON_SEASONAL_PERIOD].empty else 0.0
                acf_n = max(0.0, acf_val) / max_acf_fold if max_acf_fold > 0 else 0.0
                sp_n = max(0.0, sp_val) / max_sp_fold if max_sp_fold > 0 else 0.0
                candidate_score: float = round(0.5 * acf_n + 0.5 * sp_n, 6)
            else:
                candidate_score = float("nan")

            # Rank (1 = highest score among seasonal candidates in this fold)
            if in_fold and "seasonal_candidate_rank" in fold_grp.columns:
                rank_val = m_grp["seasonal_candidate_rank"].iloc[0]
                candidate_rank = int(rank_val) if pd.notna(rank_val) else None
            else:
                candidate_rank = None

            # Seasonality status for this fold
            if in_fold and "seasonality_status" in fold_grp.columns:
                status = str(m_grp["seasonality_status"].iloc[0])
            elif "seasonality_status" in fold_grp.columns:
                status = str(fold_grp["seasonality_status"].iloc[0])
            else:
                status = "unknown"

            # Candidate source
            if in_fold and "candidate_source" in fold_grp.columns:
                source = str(m_grp["candidate_source"].iloc[0])
            elif m == NON_SEASONAL_PERIOD:
                source = "baseline"
            elif in_fold:
                source = "seasonality_profiler"
            else:
                source = "not_evaluated"

            # cycles_available
            if in_fold and "cycles_available" in fold_grp.columns:
                cyc = m_grp["cycles_available"].iloc[0]
                cycles_available: int = int(cyc) if pd.notna(cyc) else 0
            elif n_train is not None and m > 0:
                cycles_available = n_train // m
            else:
                cycles_available = 0

            # Model counts
            if in_fold:
                models_list = sorted(m_grp["model_name"].unique().tolist()) if "model_name" in m_grp.columns else []
                models_evaluated_str = _encode_list_str(models_list)
                valid_count = int((m_grp["fit_status"] != "failed").sum())
                failed_count = int((m_grp["fit_status"] == "failed").sum())
            else:
                models_evaluated_str = ""
                valid_count = 0
                failed_count = 0

            records.append({
                "report_id": rid,
                "fold_number": int(fold_num),
                "cutoff_date": cutoff,
                "candidate_m": int(m),
                "candidate_eligible": eligible,
                "exclusion_reason": excl_reason,
                "cycles_available": cycles_available,
                "autocorrelation_at_m": acf_val,
                "spectral_power_at_m": sp_val,
                "candidate_score": candidate_score,
                "candidate_rank": candidate_rank,
                "seasonality_status": status,
                "candidate_source": source,
                "models_evaluated": models_evaluated_str,
                "valid_model_count": valid_count,
                "failed_model_count": failed_count,
            })

    df = pd.DataFrame(records, columns=CANDIDATE_COLS)
    # Deterministic sort: report_id, fold_number, candidate_rank (NaN last), candidate_m
    df = df.sort_values(
        ["report_id", "fold_number", "candidate_rank", "candidate_m"],
        na_position="last",
        ignore_index=True,
    )
    return df


def _encode_list_str(values: list[str]) -> str:
    """Pipe-delimit a list of strings."""
    return _LIST_SEP.join(str(v) for v in sorted(values))


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def validate_seasonality_summary(df: pd.DataFrame) -> None:
    """Validate the ``report_seasonality_summary`` DataFrame.

    Checks
    ------
    1. Required columns present.
    2. report_id is unique (one row per report).
    3. No null report_id.
    4. selected_m is in VALID_CANDIDATE_MS when not null.
    5. valid_backtest_folds is non-negative when not null.
    6. zero_share is in [0, 1] when not null.

    Raises
    ------
    ValueError
        On the first failing check, with a descriptive message.
    """
    missing = [c for c in SUMMARY_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f"report_seasonality_summary is missing columns: {missing}"
        )

    if df["report_id"].isna().any():
        raise ValueError("report_seasonality_summary has null report_id values.")

    dups = df[df.duplicated("report_id", keep=False)]["report_id"].unique().tolist()
    if dups:
        raise ValueError(
            f"report_seasonality_summary has duplicate report_id values: {dups[:5]}"
        )

    sel_m = df["selected_m"].dropna()
    invalid_m = sel_m[~sel_m.astype(int).isin(_VALID_CANDIDATE_MS)]
    if not invalid_m.empty:
        raise ValueError(
            f"report_seasonality_summary has invalid selected_m values: "
            f"{sorted(invalid_m.unique().tolist())}"
        )

    folds = df["valid_backtest_folds"].dropna()
    if (folds < 0).any():
        raise ValueError(
            "report_seasonality_summary has negative valid_backtest_folds values."
        )

    zs = df["zero_share"].dropna()
    if ((zs < 0) | (zs > 1)).any():
        raise ValueError(
            "report_seasonality_summary has zero_share values outside [0, 1]."
        )


def validate_seasonality_candidates(
    df: pd.DataFrame,
    min_cycles: int = MIN_SEASONAL_CYCLES,
) -> None:
    """Validate the ``report_seasonality_candidates`` DataFrame.

    Checks
    ------
    1. Required columns present.
    2. No duplicate (report_id, fold_number, candidate_m) triples.
    3. candidate_m values are in VALID_CANDIDATE_MS.
    4. cycles_available is non-negative.
    5. Ineligible candidates have a non-empty exclusion_reason.
    6. m=90 is not eligible when cycles_available < min_cycles.

    Raises
    ------
    ValueError
        On the first failing check.
    """
    missing = [c for c in CANDIDATE_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f"report_seasonality_candidates is missing columns: {missing}"
        )

    key_cols = ["report_id", "fold_number", "candidate_m"]
    dups = df[df.duplicated(key_cols, keep=False)]
    if not dups.empty:
        sample = dups[key_cols].head(3).to_dict(orient="records")
        raise ValueError(
            f"report_seasonality_candidates has duplicate "
            f"(report_id, fold_number, candidate_m) rows: {sample}"
        )

    invalid_m = df[~df["candidate_m"].isin(_VALID_CANDIDATE_MS)]["candidate_m"].unique()
    if len(invalid_m) > 0:
        raise ValueError(
            f"report_seasonality_candidates has candidate_m values outside "
            f"the valid set {sorted(_VALID_CANDIDATE_MS)}: {sorted(invalid_m.tolist())}"
        )

    if (df["cycles_available"] < 0).any():
        raise ValueError(
            "report_seasonality_candidates has negative cycles_available values."
        )

    ineligible_no_reason = df[
        (df["candidate_eligible"] == False)
        & (df["exclusion_reason"].isna() | (df["exclusion_reason"] == ""))
    ]
    if not ineligible_no_reason.empty:
        n = len(ineligible_no_reason)
        raise ValueError(
            f"{n} ineligible candidate(s) in report_seasonality_candidates "
            "have no exclusion_reason."
        )

    # m=90 cannot be eligible with fewer than min_cycles cycles
    m90_eligible = df[
        (df["candidate_m"] == 90)
        & (df["candidate_eligible"] == True)
        & (df["cycles_available"] < min_cycles)
    ]
    if not m90_eligible.empty:
        n = len(m90_eligible)
        raise ValueError(
            f"{n} row(s) in report_seasonality_candidates show m=90 as eligible "
            f"but cycles_available < {min_cycles}."
        )


# ---------------------------------------------------------------------------
# Data-quality checks (lightweight, separate from schema validation)
# ---------------------------------------------------------------------------

def run_seasonality_data_quality_checks(
    summary: pd.DataFrame,
    candidates: pd.DataFrame,
    candidate_summary: Optional[pd.DataFrame] = None,
    production_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Run lightweight data-quality checks across the diagnostic outputs.

    Checks
    ------
    1. selected_m appears among evaluated candidates unless selected model
       is non-seasonal (selected_m == 1).
    2. valid_backtest_folds in summary matches candidate_summary when available.
    3. production_fit_status reconciles with production_df (when supplied).

    Returns
    -------
    pd.DataFrame
        One row per check with columns ``check``, ``report_id``,
        ``status`` (``"pass"`` / ``"fail"``), ``detail``.
    """
    rows: list[dict] = []

    def _add(check: str, rid: str, status: str, detail: str = "") -> None:
        rows.append({"check": check, "report_id": rid, "status": status, "detail": detail})

    # Check 1: selected_m appears among evaluated candidates
    if not summary.empty and not candidates.empty:
        for _, srow in summary.iterrows():
            rid = str(srow["report_id"])
            sel_m = srow["selected_m"]
            if pd.isna(sel_m):
                continue
            sel_m_int = int(sel_m)
            if sel_m_int == NON_SEASONAL_PERIOD:
                _add("selected_m_in_evaluated_candidates", rid, "pass",
                     "non-seasonal m=1 is always evaluated")
                continue
            report_cands = candidates[candidates["report_id"] == rid]
            evaluated_ms = set(report_cands["candidate_m"].tolist())
            if sel_m_int in evaluated_ms:
                _add("selected_m_in_evaluated_candidates", rid, "pass",
                     f"m={sel_m_int} found in candidates table")
            else:
                _add("selected_m_in_evaluated_candidates", rid, "fail",
                     f"selected_m={sel_m_int} not in evaluated candidates {sorted(evaluated_ms)}")

    # Check 2: valid_backtest_folds matches candidate_summary
    if not summary.empty and candidate_summary is not None and not candidate_summary.empty:
        if "valid_folds" in candidate_summary.columns and "report_id" in candidate_summary.columns:
            cs_folds = candidate_summary.groupby("report_id")["valid_folds"].max()
            for _, srow in summary.iterrows():
                rid = str(srow["report_id"])
                summ_folds = srow["valid_backtest_folds"]
                if pd.isna(summ_folds) or rid not in cs_folds.index:
                    continue
                cs_val = cs_folds[rid]
                if abs(int(summ_folds) - int(cs_val)) <= 0:
                    _add("valid_folds_reconciliation", rid, "pass")
                else:
                    _add("valid_folds_reconciliation", rid, "fail",
                         f"summary={int(summ_folds)} vs candidate_summary_max={int(cs_val)}")

    # Check 3: production_fit_status reconciles with production_df
    if not summary.empty and production_df is not None and not production_df.empty:
        if "production_fit_status" in production_df.columns:
            for _, srow in summary.iterrows():
                rid = str(srow["report_id"])
                summ_status = srow.get("production_fit_status")
                prod_rows = production_df[production_df["report_id"] == rid]
                if prod_rows.empty:
                    continue
                prod_status = prod_rows["production_fit_status"].iloc[0]
                if summ_status == prod_status:
                    _add("production_fit_status_reconciliation", rid, "pass")
                else:
                    _add("production_fit_status_reconciliation", rid, "fail",
                         f"summary={summ_status} vs production_df={prod_status}")

    return pd.DataFrame(rows, columns=["check", "report_id", "status", "detail"])


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_seasonality_diagnostics(
    summary: pd.DataFrame,
    candidates: pd.DataFrame,
    project_root: Path,
) -> dict[str, Path]:
    """Save both diagnostic outputs to ``outputs/diagnostics/``.

    The diagnostics directory already exists under the repository output
    structure (``outputs/diagnostics/``).  Both files are written as
    latest-only CSVs (overwrite on each run).

    Parameters
    ----------
    summary:
        Output of ``build_seasonality_summary``.
    candidates:
        Output of ``build_seasonality_candidates``.
    project_root:
        Repository root.

    Returns
    -------
    dict mapping ``"summary"`` and ``"candidates"`` to their saved ``Path``.
    """
    diag_dir = project_root / "outputs" / "diagnostics"
    diag_dir.mkdir(parents=True, exist_ok=True)

    summary_path = diag_dir / "report_seasonality_summary.csv"
    candidates_path = diag_dir / "report_seasonality_candidates.csv"

    summary.to_csv(summary_path, index=False)
    candidates.to_csv(candidates_path, index=False)

    return {"summary": summary_path, "candidates": candidates_path}
