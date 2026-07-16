"""Validation for the canonical daily report-usage series.

All public functions raise ``SeriesValidationError`` (a subclass of
``ValueError``) on failure.  They never return a status code or rely solely on
print statements — callers can rely on the absence of an exception as proof
that every check passed.

Typical call site (inside ``build_report_daily_series``)::

    validate_report_daily_series(result, expected_row_count=len(spine))

The validator collects *all* failures before raising so that one run surfaces
every issue rather than stopping at the first problem.
"""

from __future__ import annotations

import textwrap
from typing import Sequence

import pandas as pd

# ---------------------------------------------------------------------------
# Public exception
# ---------------------------------------------------------------------------

REQUIRED_COLUMNS: Sequence[str] = (
    "report_id",
    "date",
    "daily_views",
    "is_observed_day",
    "is_imputed_zero",
)


class SeriesValidationError(ValueError):
    """Raised when one or more checks on the daily series fail.

    The exception message lists every failing check with the affected
    ``report_id`` values and up to five sample dates so the caller can
    diagnose problems without inspecting the DataFrame manually.
    """


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fmt_reports(report_ids: pd.Series, limit: int = 5) -> str:
    sample = sorted(report_ids.unique()[:limit])
    suffix = f" … (+{len(report_ids.unique()) - limit} more)" if len(report_ids.unique()) > limit else ""
    return ", ".join(str(r) for r in sample) + suffix


def _fmt_dates(dates: pd.Series, limit: int = 5) -> str:
    sample = sorted(dates.unique()[:limit])
    suffix = f" … (+{len(dates.unique()) - limit} more)" if len(dates.unique()) > limit else ""
    return ", ".join(str(d)[:10] for d in sample) + suffix


def _collect(failures: list[str], condition: bool, message: str) -> None:
    """Append *message* to *failures* when *condition* is True (i.e. a check failed)."""
    if condition:
        failures.append(message)


# ---------------------------------------------------------------------------
# Individual check functions (each returns a failure message or None)
# ---------------------------------------------------------------------------

def _check_required_columns(df: pd.DataFrame) -> str | None:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        return f"[1] Missing required columns: {missing}"
    return None


def _check_report_id_nulls(df: pd.DataFrame) -> str | None:
    n = df["report_id"].isna().sum()
    if n:
        return f"[2] report_id contains {n} null value(s)"
    return None


def _check_date_nulls(df: pd.DataFrame) -> str | None:
    n = df["date"].isna().sum()
    if n:
        return f"[3] date contains {n} null value(s)"
    return None


def _check_daily_views_nulls(df: pd.DataFrame) -> str | None:
    n = df["daily_views"].isna().sum()
    if n:
        return f"[4] daily_views contains {n} null value(s)"
    return None


def _check_daily_views_non_negative(df: pd.DataFrame) -> str | None:
    bad = df[df["daily_views"] < 0]
    if not bad.empty:
        return (
            f"[5] daily_views is negative in {len(bad)} row(s). "
            f"Affected reports: {_fmt_reports(bad['report_id'])}. "
            f"Sample dates: {_fmt_dates(bad['date'])}."
        )
    return None


def _check_daily_views_integer(df: pd.DataFrame) -> str | None:
    if pd.api.types.is_integer_dtype(df["daily_views"]):
        return None
    # float column — accept only if every value is a whole number
    non_whole = df["daily_views"].dropna()
    non_whole = non_whole[non_whole != non_whole.round()]
    if not non_whole.empty:
        return (
            f"[6] daily_views contains {len(non_whole)} non-integer value(s). "
            f"Affected reports: {_fmt_reports(df.loc[non_whole.index, 'report_id'])}."
        )
    return None


def _check_grain_unique(df: pd.DataFrame) -> str | None:
    dupes = df[df.duplicated(subset=["report_id", "date"], keep=False)]
    if not dupes.empty:
        return (
            f"[7] {len(dupes)} duplicate (report_id, date) rows found. "
            f"Affected reports: {_fmt_reports(dupes['report_id'])}. "
            f"Sample dates: {_fmt_dates(dupes['date'])}."
        )
    return None


def _check_dates_sorted(df: pd.DataFrame) -> str | None:
    bad_reports = []
    for report_id, grp in df.groupby("report_id", sort=False):
        if not grp["date"].is_monotonic_increasing:
            bad_reports.append(str(report_id))
    if bad_reports:
        sample = bad_reports[:5]
        suffix = f" … (+{len(bad_reports) - 5} more)" if len(bad_reports) > 5 else ""
        return f"[8] Dates not sorted within report for: {', '.join(sample)}{suffix}."
    return None


def _check_dates_continuous(df: pd.DataFrame) -> str | None:
    """Each report's date sequence must have no gaps (diff == 1 day everywhere)."""
    gap_reports: list[tuple[str, str]] = []
    for report_id, grp in df.groupby("report_id", sort=False):
        sorted_dates = grp["date"].sort_values().reset_index(drop=True)
        diffs = sorted_dates.diff().dropna()
        gaps = diffs[diffs != pd.Timedelta("1D")]
        if not gaps.empty:
            first_gap_date = sorted_dates.iloc[gaps.index[0]]
            gap_reports.append((str(report_id), str(first_gap_date)[:10]))
    if gap_reports:
        details = "; ".join(f"{r} (first gap after {d})" for r, d in gap_reports[:5])
        suffix = f" … (+{len(gap_reports) - 5} more)" if len(gap_reports) > 5 else ""
        return f"[9] Date gaps found in {len(gap_reports)} report(s): {details}{suffix}."
    return None


def _check_imputed_zero_flag(df: pd.DataFrame) -> str | None:
    """is_imputed_zero must only be True when daily_views == 0."""
    bad = df[df["is_imputed_zero"] & (df["daily_views"] != 0)]
    if not bad.empty:
        return (
            f"[10] is_imputed_zero is True but daily_views != 0 in {len(bad)} row(s). "
            f"Affected reports: {_fmt_reports(bad['report_id'])}. "
            f"Sample dates: {_fmt_dates(bad['date'])}."
        )
    return None


def _check_indicator_mutual_exclusion(df: pd.DataFrame) -> str | None:
    """is_observed_day and is_imputed_zero cannot both be True."""
    bad = df[df["is_observed_day"] & df["is_imputed_zero"]]
    if not bad.empty:
        return (
            f"[11] is_observed_day and is_imputed_zero are both True in {len(bad)} row(s). "
            f"Affected reports: {_fmt_reports(bad['report_id'])}. "
            f"Sample dates: {_fmt_dates(bad['date'])}."
        )
    return None


def _check_each_report_has_rows(df: pd.DataFrame) -> str | None:
    """Every report_id present must contribute at least one row."""
    counts = df.groupby("report_id")["date"].count()
    empty = counts[counts == 0]
    if not empty.empty:
        return f"[12] Reports with zero rows: {sorted(empty.index.tolist())}."
    return None


def _check_row_count(df: pd.DataFrame, expected: int) -> str | None:
    actual = len(df)
    if actual != expected:
        return (
            f"[13] Row count mismatch: expected {expected}, got {actual} "
            f"(difference: {actual - expected:+d})."
        )
    return None


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def validate_report_daily_series(
    df: pd.DataFrame,
    expected_row_count: int | None = None,
) -> None:
    """Validate all structural and logical invariants of the canonical daily series.

    Parameters
    ----------
    df:
        Output of ``build_report_daily_series``.  Must contain at minimum the
        columns ``report_id``, ``date``, ``daily_views``, ``is_observed_day``,
        and ``is_imputed_zero``.
    expected_row_count:
        When supplied, the total number of rows is compared against this value
        (check 13).  Pass ``len(spine)`` from inside the builder so the
        post-join shape can be verified against the pre-join spine.

    Raises
    ------
    SeriesValidationError
        When one or more checks fail.  The message lists every failure with
        affected ``report_id`` values and sample dates.
    """
    failures: list[str] = []

    # Check 1 — required columns (must pass before any column-level check)
    col_msg = _check_required_columns(df)
    if col_msg:
        raise SeriesValidationError(col_msg)

    # Checks 2–4 — null checks (must pass before value-level checks)
    for fn in (_check_report_id_nulls, _check_date_nulls, _check_daily_views_nulls):
        msg = fn(df)
        if msg:
            failures.append(msg)

    if failures:
        # Cannot safely run value checks with nulls present
        raise SeriesValidationError(_format_failures(failures))

    # Checks 5–13 — value and structural checks
    failures += [
        m for m in (
            _check_daily_views_non_negative(df),
            _check_daily_views_integer(df),
            _check_grain_unique(df),
            _check_dates_sorted(df),
            _check_dates_continuous(df),
            _check_imputed_zero_flag(df),
            _check_indicator_mutual_exclusion(df),
            _check_each_report_has_rows(df),
        )
        if m is not None
    ]

    if expected_row_count is not None:
        msg = _check_row_count(df, expected_row_count)
        if msg:
            failures.append(msg)

    if failures:
        raise SeriesValidationError(_format_failures(failures))


def _format_failures(failures: list[str]) -> str:
    header = f"{len(failures)} validation check(s) failed on the daily series:\n"
    body = "\n".join(f"  • {f}" for f in failures)
    return header + body
