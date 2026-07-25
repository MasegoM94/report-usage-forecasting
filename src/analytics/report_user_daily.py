"""
Canonical privacy-safe report-user-day mart.

Source schema (fact_report_views.csv):
  date_key            — integer YYYYMMDD  (parsed via pd.to_datetime(..., format="%Y%m%d"))
  report_id           — string
  user_key            — pseudonymous surrogate key (e.g. UK_0001)
  consumption_method  — extra source column, retained through normalisation
  distribution_method — extra source column, retained through normalisation
  view_count          — integer / float

Date column handling:
  date_key is an 8-digit integer.  normalize_report_view_events parses it with
  format="%Y%m%d" and stores the result as datetime.date in usage_date.

Duplicate signature:
  fact_report_views has no event_id.  The duplicate signature is therefore the
  full source row (all columns after normalisation).  Two genuinely identical
  events on the same (user, report, date, view_count, consumption_method,
  distribution_method) are indistinguishable and are de-duplicated as one.
  This is a known limitation documented here.

Privacy:
  - dim_user is never joined.
  - user_id, unique_user, and all other PROHIBITED_OUTPUT_COLUMNS are never
    written to any output.
  - user_key is the only allowed user-level identifier.
"""

from __future__ import annotations

import re
import uuid
import warnings
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.analytics.privacy_policy import (
    EMAIL_PATTERN,
    PROHIBITED_OUTPUT_COLUMNS,
    detect_email_like_values,
    validate_no_direct_identifiers,
)

# ---------------------------------------------------------------------------
# Column-name constants
# ---------------------------------------------------------------------------

MART_REPORT_USER_DAILY_COLS: list[str] = [
    "analytics_run_id",
    "generated_at",
    "report_id",
    "user_key",
    "usage_date",
    "daily_views",
    "active_user_day",
    "source_event_count",
    "duplicate_event_count_removed",
    "first_report_use_date",
    "latest_report_use_date",
    "report_user_tenure_days",
    "first_use_flag",
    "lifetime_returned_flag",
    "user_identifier_status",
    "source_record_valid",
    "record_valid",
    "source_file",
    "source_row_count",
]

REPORT_USER_QUALITY_COLS: list[str] = [
    "analytics_run_id",
    "generated_at",
    "report_id",
    "report_name",
    "source_event_count",
    "valid_user_event_count",
    "valid_positive_usage_event_count",
    "exact_duplicate_event_count",
    "duplicate_event_count_removed",
    "missing_user_id_event_count",
    "invalid_user_id_event_count",
    "prohibited_identifier_event_count",
    "invalid_report_id_event_count",
    "invalid_date_event_count",
    "future_date_event_count",
    "non_finite_view_count_event_count",
    "zero_or_negative_view_event_count",
    "excluded_event_count",
    "excluded_user_event_share",
    "data_quality_status",
    "data_quality_reasons",
]

# Precedence-ordered exclusion reasons (first match wins)
_EXCLUSION_PRECEDENCE: list[str] = [
    "invalid_report_id",
    "missing_user_key",
    "prohibited_identifier",
    "invalid_date",
    "future_date",
    "non_finite_view_count",
    "zero_or_negative_view_count",
    "valid",
]


# ---------------------------------------------------------------------------
# Quality thresholds (configurable via dataclass)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class QualityThresholds:
    good_max_excluded_share: float = 0.05
    warning_max_excluded_share: float = 0.20


DEFAULT_THRESHOLDS = QualityThresholds()


# ---------------------------------------------------------------------------
# 1. normalize_report_view_events
# ---------------------------------------------------------------------------

def normalize_report_view_events(
    df: pd.DataFrame,
    as_of_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    Standardise column names and parse dates from raw fact_report_views DataFrame.

    Source column mapping:
      date_key  → usage_date   (integer YYYYMMDD → datetime.date)
      report_id → report_id    (kept as-is)
      user_key  → user_key     (kept as-is)
      view_count→ view_count   (kept as-is)
      All remaining columns are preserved unchanged.

    Rejects rows whose usage_date is in the future relative to as_of_date
    (or today if not provided) by recording them — the actual filter happens in
    classify_source_record_quality.

    Returns a DataFrame with columns:
      report_id, user_key, usage_date, view_count, <extra source columns>
    """
    df = df.copy()

    cutoff = as_of_date if as_of_date is not None else date.today()

    # ---- Map date column ----
    if "date_key" in df.columns:
        # Integer YYYYMMDD format
        parsed = pd.to_datetime(df["date_key"].astype(str), format="%Y%m%d", errors="coerce")
        df["usage_date"] = parsed.dt.date
    elif "date" in df.columns:
        parsed = pd.to_datetime(df["date"], errors="coerce")
        df["usage_date"] = parsed.dt.date
    else:
        raise ValueError(
            "normalize_report_view_events: source DataFrame has neither "
            "'date_key' nor 'date' column."
        )

    # Drop the original date column to avoid duplication (keep usage_date only)
    for col in ("date_key", "date"):
        if col in df.columns and col != "usage_date":
            df = df.drop(columns=[col])

    # ---- Ensure required columns present ----
    for col in ("report_id", "user_key", "view_count"):
        if col not in df.columns:
            raise ValueError(
                f"normalize_report_view_events: required column '{col}' missing."
            )

    # ---- Reorder: put canonical columns first ----
    extras = [c for c in df.columns if c not in ("report_id", "user_key", "usage_date", "view_count")]
    return df[["report_id", "user_key", "usage_date", "view_count"] + extras].reset_index(drop=True)


# ---------------------------------------------------------------------------
# 2. classify_source_record_quality
# ---------------------------------------------------------------------------

def classify_source_record_quality(
    df: pd.DataFrame,
    as_of_date: Optional[date] = None,
) -> pd.DataFrame:
    """
    Assign record_exclusion_reason and source_record_valid to every row.

    Precedence (first matching rule wins):
      1. invalid_report_id       — report_id is null or empty string
      2. missing_user_key        — user_key is null or empty string
      3. prohibited_identifier   — user_key contains an email-like pattern
      4. invalid_date            — usage_date is None / NaT / unparseable
      5. future_date             — usage_date > as_of_date
      6. non_finite_view_count   — view_count is NaN or ±inf
      7. zero_or_negative_view_count — view_count <= 0
      8. valid                   — all checks pass
    """
    df = df.copy()
    cutoff = as_of_date if as_of_date is not None else date.today()
    n = len(df)
    reasons = pd.array(["valid"] * n, dtype=object)

    # Work in reverse-precedence order so higher-precedence rules overwrite lower ones
    # 7. zero_or_negative_view_count
    mask_zero = pd.to_numeric(df["view_count"], errors="coerce").fillna(0) <= 0
    reasons[mask_zero.values] = "zero_or_negative_view_count"

    # 6. non_finite_view_count
    vc_num = pd.to_numeric(df["view_count"], errors="coerce")
    mask_nonfinite = ~np.isfinite(vc_num)
    reasons[mask_nonfinite.values] = "non_finite_view_count"

    # 5. future_date
    def _is_future(d: object) -> bool:
        if d is None or (isinstance(d, float) and np.isnan(d)):
            return False
        try:
            return d > cutoff
        except Exception:
            return False

    mask_future = df["usage_date"].apply(_is_future)
    reasons[mask_future.values] = "future_date"

    # 4. invalid_date
    def _is_invalid_date(d: object) -> bool:
        if d is None:
            return True
        if isinstance(d, float) and np.isnan(d):
            return True
        # pd.NaT is an instance of datetime.date but is not a real date
        if d is pd.NaT:
            return True
        try:
            # Must be a concrete date, not NaT
            _ = d.year  # real date.date has .year; NaT also has it so check above first
        except AttributeError:
            return True
        if isinstance(d, date) and not isinstance(d, type(pd.NaT)):
            return False
        return True

    mask_invalid_date = df["usage_date"].apply(_is_invalid_date)
    reasons[mask_invalid_date.values] = "invalid_date"

    # 3. prohibited_identifier
    def _is_email_like(val: object) -> bool:
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return False
        return bool(EMAIL_PATTERN.search(str(val)))

    mask_email = df["user_key"].apply(_is_email_like)
    reasons[mask_email.values] = "prohibited_identifier"

    # 2. missing_user_key
    mask_missing_user = df["user_key"].isna() | (df["user_key"].astype(str).str.strip() == "")
    reasons[mask_missing_user.values] = "missing_user_key"

    # 1. invalid_report_id
    mask_invalid_rid = df["report_id"].isna() | (df["report_id"].astype(str).str.strip() == "")
    reasons[mask_invalid_rid.values] = "invalid_report_id"

    df["record_exclusion_reason"] = reasons
    df["source_record_valid"] = df["record_exclusion_reason"] == "valid"
    return df


# ---------------------------------------------------------------------------
# 3. remove_exact_event_duplicates
# ---------------------------------------------------------------------------

def remove_exact_event_duplicates(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Remove exact duplicate rows from the classified DataFrame.

    Duplicate signature: all columns present in the DataFrame (the full row).
    Because fact_report_views has no event_id, two rows that are identical on
    every column are treated as duplicates.  This is a known limitation.

    Deduplication is applied across ALL rows (valid and invalid alike) before
    they enter the mart, so duplicate invalid events are also counted.

    Returns:
      (deduplicated_df, duplicate_counts_by_report_df)
      duplicate_counts_by_report_df columns: report_id, exact_duplicate_event_count
    """
    dedup = df.drop_duplicates(keep="first").copy()
    removed = df[df.duplicated(keep="first")].copy()

    if removed.empty:
        counts = pd.DataFrame({"report_id": pd.Series(dtype=str), "exact_duplicate_event_count": pd.Series(dtype=int)})
    else:
        counts = (
            removed.groupby("report_id", dropna=False)
            .size()
            .reset_index(name="exact_duplicate_event_count")
        )

    return dedup, counts


# ---------------------------------------------------------------------------
# 4. build_report_user_daily
# ---------------------------------------------------------------------------

def build_report_user_daily(
    valid_df: pd.DataFrame,
    analytics_run_id: str,
    dedup_counts_df: Optional[pd.DataFrame] = None,
    source_file: str = "",
    source_row_count: int = 0,
) -> pd.DataFrame:
    """
    Aggregate valid source events to (report_id, user_key, usage_date) grain.

    Columns produced:
      daily_views               — sum(view_count) per grain
      active_user_day           — True (all rows have positive views by construction)
      source_event_count        — number of source events contributing to this row
      duplicate_event_count_removed — from dedup_counts_df, 0 if none for this report
    """
    if valid_df.empty:
        mart = pd.DataFrame(columns=[
            "report_id", "user_key", "usage_date", "daily_views",
            "active_user_day", "source_event_count", "duplicate_event_count_removed",
        ])
        mart["analytics_run_id"] = analytics_run_id
        mart["generated_at"] = datetime.utcnow().isoformat()
        mart["user_identifier_status"] = "valid"
        mart["source_record_valid"] = True
        mart["record_valid"] = True
        mart["source_file"] = source_file
        mart["source_row_count"] = source_row_count
        return mart

    agg = (
        valid_df.groupby(["report_id", "user_key", "usage_date"], as_index=False)
        .agg(
            daily_views=("view_count", "sum"),
            source_event_count=("view_count", "count"),
        )
    )
    agg["active_user_day"] = True

    # Join duplicate counts
    if dedup_counts_df is not None and not dedup_counts_df.empty:
        agg = agg.merge(
            dedup_counts_df[["report_id", "exact_duplicate_event_count"]],
            on="report_id",
            how="left",
        )
        agg["duplicate_event_count_removed"] = agg["exact_duplicate_event_count"].fillna(0).astype(int)
        agg = agg.drop(columns=["exact_duplicate_event_count"])
    else:
        agg["duplicate_event_count_removed"] = 0

    agg["analytics_run_id"] = analytics_run_id
    agg["generated_at"] = datetime.utcnow().isoformat()
    agg["user_identifier_status"] = "valid"
    agg["source_record_valid"] = True
    agg["record_valid"] = True
    agg["source_file"] = source_file
    agg["source_row_count"] = source_row_count

    return agg


# ---------------------------------------------------------------------------
# 5. add_report_user_history_fields
# ---------------------------------------------------------------------------

def add_report_user_history_fields(mart_df: pd.DataFrame) -> pd.DataFrame:
    """
    Add per-(report_id, user_key) history fields:

      first_report_use_date     — min(usage_date)
      latest_report_use_date    — max(usage_date)
      report_user_tenure_days   — (usage_date - first_report_use_date).days  (0 on first day)
      first_use_flag            — True only on first_report_use_date
      lifetime_returned_flag    — True iff latest_report_use_date > first_report_use_date
    """
    if mart_df.empty:
        for col in (
            "first_report_use_date", "latest_report_use_date",
            "report_user_tenure_days", "first_use_flag", "lifetime_returned_flag",
        ):
            mart_df[col] = pd.Series(dtype=object)
        return mart_df

    df = mart_df.copy()

    pair_stats = (
        df.groupby(["report_id", "user_key"])["usage_date"]
        .agg(first_report_use_date="min", latest_report_use_date="max")
        .reset_index()
    )

    df = df.merge(pair_stats, on=["report_id", "user_key"], how="left")

    df["report_user_tenure_days"] = df.apply(
        lambda r: (r["usage_date"] - r["first_report_use_date"]).days
        if isinstance(r["usage_date"], date) and isinstance(r["first_report_use_date"], date)
        else None,
        axis=1,
    )

    df["first_use_flag"] = df["usage_date"] == df["first_report_use_date"]
    df["lifetime_returned_flag"] = df["latest_report_use_date"] > df["first_report_use_date"]

    return df


# ---------------------------------------------------------------------------
# 6. build_report_user_data_quality
# ---------------------------------------------------------------------------

def build_report_user_data_quality(
    classified_df: pd.DataFrame,
    dedup_counts_df: pd.DataFrame,
    analytics_run_id: str,
    report_meta_df: Optional[pd.DataFrame] = None,
    thresholds: QualityThresholds = DEFAULT_THRESHOLDS,
) -> pd.DataFrame:
    """
    Build the data-quality output: one row per report_id.

    report_meta_df (optional): must have report_id (unique) and report_name columns.
    Raises ValueError if report_meta_df has duplicate report_id values.
    """
    # Validate optional metadata
    if report_meta_df is not None and not report_meta_df.empty:
        if report_meta_df["report_id"].duplicated().any():
            raise ValueError(
                "build_report_user_data_quality: report_meta_df contains duplicate "
                "report_id values — join would duplicate mart rows."
            )

    generated_at = datetime.utcnow().isoformat()

    # Collect all report_ids (including those with only invalid rows).
    # Events with null/empty report_id are tallied under the sentinel "(null_report_id)".
    _NULL_SENTINEL = "(null_report_id)"
    work_df = classified_df.copy()
    null_rid_mask = work_df["report_id"].isna() | (work_df["report_id"].astype(str).str.strip() == "")
    work_df.loc[null_rid_mask, "report_id"] = _NULL_SENTINEL
    all_reports = sorted(work_df["report_id"].unique().tolist())

    # Per-report aggregations
    rows = []
    for rid in all_reports:
        sub = work_df[work_df["report_id"] == rid]
        reasons = sub["record_exclusion_reason"]

        source_event_count = len(sub)
        valid_positive = (reasons == "valid").sum()
        missing_user = (reasons == "missing_user_key").sum()
        prohibited = (reasons == "prohibited_identifier").sum()
        invalid_report_id = (reasons == "invalid_report_id").sum()
        invalid_date = (reasons == "invalid_date").sum()
        future_date = (reasons == "future_date").sum()
        non_finite = (reasons == "non_finite_view_count").sum()
        zero_neg = (reasons == "zero_or_negative_view_count").sum()

        # "invalid_user_id" = prohibited_identifier (email-like user key)
        invalid_user_id = prohibited

        # valid_user_event_count = events where user key is neither missing nor prohibited
        valid_user = source_event_count - missing_user - prohibited

        # Duplicate counts for this report (sentinel maps back to original null)
        dup_rid = rid if rid != _NULL_SENTINEL else None
        dup_sub = dedup_counts_df[dedup_counts_df["report_id"] == dup_rid] if dup_rid else pd.DataFrame()
        dup_count = int(dup_sub["exact_duplicate_event_count"].sum()) if not dup_sub.empty else 0

        excluded = source_event_count - valid_positive
        excluded_share = excluded / source_event_count if source_event_count > 0 else None

        # data_quality_status and reasons
        quality_reasons: list[str] = []
        if source_event_count == 0 or valid_positive == 0:
            status = "no_valid_user_data"
            quality_reasons.append("no_valid_positive_events")
        else:
            if excluded_share is not None:
                if excluded_share >= thresholds.warning_max_excluded_share:
                    status = "poor"
                    quality_reasons.append(f"high_exclusion_rate:{excluded_share:.2%}")
                elif excluded_share >= thresholds.good_max_excluded_share:
                    status = "warning"
                    quality_reasons.append(f"moderate_exclusion_rate:{excluded_share:.2%}")
                else:
                    status = "good"
            else:
                status = "good"

        if missing_user > 0:
            quality_reasons.append(f"missing_user_key:{missing_user}")
        if prohibited > 0:
            quality_reasons.append(f"prohibited_identifier:{prohibited}")
        if invalid_date > 0:
            quality_reasons.append(f"invalid_date:{invalid_date}")
        if future_date > 0:
            quality_reasons.append(f"future_date:{future_date}")
        if dup_count > 0:
            quality_reasons.append(f"duplicates_removed:{dup_count}")

        rows.append({
            "analytics_run_id": analytics_run_id,
            "generated_at": generated_at,
            "report_id": rid,
            "report_name": None,  # filled below from metadata
            "source_event_count": source_event_count,
            "valid_user_event_count": max(0, valid_user),
            "valid_positive_usage_event_count": int(valid_positive),
            "exact_duplicate_event_count": dup_count,
            "duplicate_event_count_removed": dup_count,
            "missing_user_id_event_count": int(missing_user),
            "invalid_user_id_event_count": int(invalid_user_id),
            "prohibited_identifier_event_count": int(prohibited),
            "invalid_report_id_event_count": int(invalid_report_id),
            "invalid_date_event_count": int(invalid_date),
            "future_date_event_count": int(future_date),
            "non_finite_view_count_event_count": int(non_finite),
            "zero_or_negative_view_event_count": int(zero_neg),
            "excluded_event_count": int(excluded),
            "excluded_user_event_share": excluded_share,
            "data_quality_status": status,
            "data_quality_reasons": ",".join(quality_reasons) if quality_reasons else "none",
        })

    quality_df = pd.DataFrame(rows)

    if quality_df.empty:
        quality_df = pd.DataFrame(columns=REPORT_USER_QUALITY_COLS)
        quality_df["analytics_run_id"] = analytics_run_id
        quality_df["generated_at"] = generated_at
        return quality_df

    # Join report_name from metadata
    if report_meta_df is not None and not report_meta_df.empty:
        meta = report_meta_df[["report_id", "report_name"]].drop_duplicates("report_id")
        before_len = len(quality_df)
        quality_df = quality_df.drop(columns=["report_name"]).merge(meta, on="report_id", how="left")
        assert len(quality_df) == before_len, (
            "report_meta_df join duplicated rows — duplicate report_id in metadata"
        )

    # Ensure column order
    for col in REPORT_USER_QUALITY_COLS:
        if col not in quality_df.columns:
            quality_df[col] = None
    return quality_df[REPORT_USER_QUALITY_COLS]


# ---------------------------------------------------------------------------
# 7. validate_report_user_daily
# ---------------------------------------------------------------------------

def validate_report_user_daily(
    df: pd.DataFrame,
    as_of_date: Optional[date] = None,
) -> None:
    """
    Validate all 20 contract points for the mart_report_user_daily output.

    Raises ValueError with a descriptive message on the first failure found.
    """
    cutoff = as_of_date if as_of_date is not None else date.today()

    # 1. Required columns present
    missing_cols = [c for c in MART_REPORT_USER_DAILY_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"validate_report_user_daily [1]: missing columns: {missing_cols}")

    # 2. Grain unique
    grain = ["report_id", "user_key", "usage_date"]
    if df.duplicated(subset=grain).any():
        raise ValueError("validate_report_user_daily [2]: duplicate (report_id, user_key, usage_date) rows found")

    # 3. No direct identifiers (column names)
    bad_cols = PROHIBITED_OUTPUT_COLUMNS & set(df.columns)
    if bad_cols:
        raise ValueError(f"validate_report_user_daily [3]: prohibited columns: {sorted(bad_cols)}")

    # 4. No email-like values
    detect_email_like_values(df, context="mart_report_user_daily")

    # 5. report_id non-null
    if df["report_id"].isna().any():
        raise ValueError("validate_report_user_daily [5]: report_id contains null values")

    # 6. user_key non-null
    if df["user_key"].isna().any():
        raise ValueError("validate_report_user_daily [6]: user_key contains null values")

    # 7. usage_date valid dates
    def _bad_date(d: object) -> bool:
        return not isinstance(d, date)

    if df["usage_date"].apply(_bad_date).any():
        raise ValueError("validate_report_user_daily [7]: usage_date contains invalid values")

    # 8. usage_date not in future
    if df["usage_date"].apply(lambda d: isinstance(d, date) and d > cutoff).any():
        raise ValueError("validate_report_user_daily [8]: usage_date contains future dates")

    # 9. daily_views finite and > 0
    dv = pd.to_numeric(df["daily_views"], errors="coerce")
    if (~np.isfinite(dv) | (dv <= 0)).any():
        raise ValueError("validate_report_user_daily [9]: daily_views must be finite and > 0")

    # 10. active_user_day is True for every row
    if not df["active_user_day"].all():
        raise ValueError("validate_report_user_daily [10]: active_user_day is not True for all rows")

    # 11. source_event_count > 0
    if (pd.to_numeric(df["source_event_count"], errors="coerce") <= 0).any():
        raise ValueError("validate_report_user_daily [11]: source_event_count must be > 0")

    # 12. duplicate_event_count_removed >= 0
    if (pd.to_numeric(df["duplicate_event_count_removed"], errors="coerce") < 0).any():
        raise ValueError("validate_report_user_daily [12]: duplicate_event_count_removed must be >= 0")

    # 13. first_report_use_date <= usage_date
    bad = df.apply(
        lambda r: isinstance(r["first_report_use_date"], date) and r["first_report_use_date"] > r["usage_date"],
        axis=1,
    )
    if bad.any():
        raise ValueError("validate_report_user_daily [13]: first_report_use_date > usage_date")

    # 14. latest_report_use_date >= usage_date
    bad = df.apply(
        lambda r: isinstance(r["latest_report_use_date"], date) and r["latest_report_use_date"] < r["usage_date"],
        axis=1,
    )
    if bad.any():
        raise ValueError("validate_report_user_daily [14]: latest_report_use_date < usage_date")

    # 15. first_report_use_date <= latest_report_use_date
    bad = df.apply(
        lambda r: isinstance(r["first_report_use_date"], date)
        and isinstance(r["latest_report_use_date"], date)
        and r["first_report_use_date"] > r["latest_report_use_date"],
        axis=1,
    )
    if bad.any():
        raise ValueError("validate_report_user_daily [15]: first_report_use_date > latest_report_use_date")

    # 16. report_user_tenure_days >= 0
    if (pd.to_numeric(df["report_user_tenure_days"], errors="coerce") < 0).any():
        raise ValueError("validate_report_user_daily [16]: report_user_tenure_days < 0")

    # 17. first_use_flag True only on first_report_use_date
    bad = df[df["first_use_flag"] == True].apply(
        lambda r: r["usage_date"] != r["first_report_use_date"], axis=1
    )
    if bad.any():
        raise ValueError("validate_report_user_daily [17]: first_use_flag=True on non-first date")

    # 18. Exactly one first_use_flag=True per (report_id, user_key)
    pair_counts = df.groupby(["report_id", "user_key"])["first_use_flag"].sum()
    if (pair_counts != 1).any():
        raise ValueError(
            "validate_report_user_daily [18]: not exactly one first_use_flag=True per (report_id, user_key)"
        )

    # 19. lifetime_returned_flag reconciles
    pair_info = df.groupby(["report_id", "user_key"]).agg(
        first_date=("first_report_use_date", "first"),
        latest_date=("latest_report_use_date", "first"),
        flag=("lifetime_returned_flag", "first"),
    )
    expected_flag = pair_info["latest_date"] > pair_info["first_date"]
    if (pair_info["flag"] != expected_flag).any():
        raise ValueError(
            "validate_report_user_daily [19]: lifetime_returned_flag inconsistent with first/latest dates"
        )

    # 20. Sorted by (report_id, user_key, usage_date) ascending
    sorted_df = df.sort_values(["report_id", "user_key", "usage_date"])
    if not (df[["report_id", "user_key", "usage_date"]].values == sorted_df[["report_id", "user_key", "usage_date"]].values).all():
        raise ValueError(
            "validate_report_user_daily [20]: output is not sorted by (report_id, user_key, usage_date)"
        )


# ---------------------------------------------------------------------------
# 8. validate_report_user_data_quality
# ---------------------------------------------------------------------------

def validate_report_user_data_quality(df: pd.DataFrame) -> None:
    """
    Validate all 8 contract points for the report_user_data_quality output.

    Raises ValueError with a descriptive message on the first failure found.
    """
    # 1. Required columns present
    missing_cols = [c for c in REPORT_USER_QUALITY_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"validate_report_user_data_quality [1]: missing columns: {missing_cols}")

    # 2. Grain unique: one row per report_id
    if df["report_id"].duplicated().any():
        raise ValueError("validate_report_user_data_quality [2]: duplicate report_id rows found")

    # 3. No prohibited columns
    bad_cols = PROHIBITED_OUTPUT_COLUMNS & set(df.columns)
    if bad_cols:
        raise ValueError(f"validate_report_user_data_quality [3]: prohibited columns: {sorted(bad_cols)}")

    # 4. No email-like values
    detect_email_like_values(df, context="report_user_data_quality")

    # 5. source_event_count >= 0
    if (pd.to_numeric(df["source_event_count"], errors="coerce") < 0).any():
        raise ValueError("validate_report_user_data_quality [5]: source_event_count < 0")

    # 6. data_quality_status is one of the allowed values
    allowed_statuses = {"good", "warning", "poor", "no_valid_user_data"}
    bad_statuses = set(df["data_quality_status"].dropna()) - allowed_statuses
    if bad_statuses:
        raise ValueError(
            f"validate_report_user_data_quality [6]: invalid data_quality_status values: {bad_statuses}"
        )

    # 7. Counts are non-negative integers
    count_cols = [
        "valid_user_event_count", "valid_positive_usage_event_count",
        "exact_duplicate_event_count", "duplicate_event_count_removed",
        "missing_user_id_event_count", "invalid_user_id_event_count",
        "prohibited_identifier_event_count", "invalid_report_id_event_count",
        "invalid_date_event_count", "future_date_event_count",
        "non_finite_view_count_event_count", "zero_or_negative_view_event_count",
        "excluded_event_count",
    ]
    for col in count_cols:
        if col in df.columns:
            if (pd.to_numeric(df[col], errors="coerce") < 0).any():
                raise ValueError(
                    f"validate_report_user_data_quality [7]: {col} contains negative values"
                )

    # 8. excluded_user_event_share between 0 and 1 (or null)
    share = pd.to_numeric(df["excluded_user_event_share"], errors="coerce")
    if ((share < 0) | (share > 1)).any():
        raise ValueError(
            "validate_report_user_data_quality [8]: excluded_user_event_share out of [0,1] range"
        )


# ---------------------------------------------------------------------------
# 9. persist_report_user_daily_outputs
# ---------------------------------------------------------------------------

def persist_report_user_daily_outputs(
    mart_df: pd.DataFrame,
    quality_df: pd.DataFrame,
    project_root: Path,
) -> dict[str, Path]:
    """
    Validate and write mart and quality DataFrames to outputs/analytics/.

    Validates both DataFrames before writing — raises ValueError if validation
    fails (no partial files are written).

    Returns dict with keys "mart" and "quality" pointing to written Paths.
    """
    validate_report_user_daily(mart_df)
    validate_report_user_data_quality(quality_df)

    analytics_dir = project_root / "outputs" / "analytics"
    analytics_dir.mkdir(parents=True, exist_ok=True)

    mart_path = analytics_dir / "mart_report_user_daily.csv"
    quality_path = analytics_dir / "report_user_data_quality.csv"

    mart_df.to_csv(mart_path, index=False)
    quality_df.to_csv(quality_path, index=False)

    return {"mart": mart_path, "quality": quality_path}


# ---------------------------------------------------------------------------
# Convenience: full pipeline in one call
# ---------------------------------------------------------------------------

def run_report_user_daily_pipeline(
    fact_df: pd.DataFrame,
    analytics_run_id: Optional[str] = None,
    as_of_date: Optional[date] = None,
    report_meta_df: Optional[pd.DataFrame] = None,
    source_file: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Orchestrate normalize → classify → dedup → build_quality → build_mart → add_history.

    Returns (mart_df, quality_df).
    """
    run_id = analytics_run_id or str(uuid.uuid4())

    normalized = normalize_report_view_events(fact_df, as_of_date=as_of_date)
    classified = classify_source_record_quality(normalized, as_of_date=as_of_date)
    deduped, dedup_counts = remove_exact_event_duplicates(classified)

    quality_df = build_report_user_data_quality(
        classified_df=deduped,
        dedup_counts_df=dedup_counts,
        analytics_run_id=run_id,
        report_meta_df=report_meta_df,
    )

    valid_df = deduped[deduped["source_record_valid"] == True].copy()
    mart_df = build_report_user_daily(
        valid_df=valid_df,
        analytics_run_id=run_id,
        dedup_counts_df=dedup_counts,
        source_file=source_file,
        source_row_count=len(fact_df),
    )
    mart_df = add_report_user_history_fields(mart_df)

    # Sort by grain
    if not mart_df.empty:
        mart_df = mart_df.sort_values(["report_id", "user_key", "usage_date"]).reset_index(drop=True)

    # Ensure all required columns present in correct order
    for col in MART_REPORT_USER_DAILY_COLS:
        if col not in mart_df.columns:
            mart_df[col] = None
    mart_df = mart_df[MART_REPORT_USER_DAILY_COLS]

    return mart_df, quality_df
