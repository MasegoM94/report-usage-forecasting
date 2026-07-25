"""
Tests for src/analytics/report_user_daily.py

All tests use mock DataFrames — no real files loaded from disk.
Source schema matches fact_report_views.csv:
  date_key (int YYYYMMDD), report_id, user_key, consumption_method,
  distribution_method, view_count
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.analytics.report_user_daily import (
    MART_REPORT_USER_DAILY_COLS,
    REPORT_USER_QUALITY_COLS,
    add_report_user_history_fields,
    build_report_user_daily,
    build_report_user_data_quality,
    classify_source_record_quality,
    normalize_report_view_events,
    persist_report_user_daily_outputs,
    remove_exact_event_duplicates,
    run_report_user_daily_pipeline,
    validate_report_user_daily,
    validate_report_user_data_quality,
)
from src.analytics.privacy_policy import validate_no_direct_identifiers


# ---------------------------------------------------------------------------
# Helper: build a minimal fact DataFrame matching source schema
# ---------------------------------------------------------------------------

def _make_fact_df(
    date_keys=None,
    report_ids=None,
    user_keys=None,
    view_counts=None,
    consumption_methods=None,
    distribution_methods=None,
    **extra_cols,
) -> pd.DataFrame:
    """Build a fact_report_views-style DataFrame from lists."""
    candidates = [x for x in [date_keys, report_ids, user_keys, view_counts] if x is not None]
    n = max(len(x) for x in candidates) if candidates else 1
    date_keys = date_keys or [20250101] * n
    report_ids = report_ids or ["R_001"] * n
    user_keys = user_keys or ["UK_0001"] * n
    view_counts = view_counts or [1] * n
    consumption_methods = consumption_methods or ["Web"] * n
    distribution_methods = distribution_methods or ["Direct"] * n

    data = {
        "date_key": date_keys,
        "report_id": report_ids,
        "user_key": user_keys,
        "consumption_method": consumption_methods,
        "distribution_method": distribution_methods,
        "view_count": view_counts,
    }
    for k, v in extra_cols.items():
        data[k] = v
    return pd.DataFrame(data)


def _pipeline(fact_df, as_of_date=None, report_meta_df=None):
    """Convenience: run full pipeline, return (mart, quality)."""
    return run_report_user_daily_pipeline(
        fact_df,
        analytics_run_id="test-run",
        as_of_date=as_of_date or date(2026, 7, 24),
        report_meta_df=report_meta_df,
        source_file="test",
    )


# ===========================================================================
# TestBasicAggregation
# ===========================================================================

class TestBasicAggregation:
    def test_single_event(self):
        """1 report, 1 user, 1 event → 1 mart row, daily_views correct."""
        fact = _make_fact_df(view_counts=[5])
        mart, _ = _pipeline(fact)
        assert len(mart) == 1
        assert mart.iloc[0]["daily_views"] == 5

    def test_multiple_events_same_day(self):
        """1 report, 1 user, 3 events same date → 1 mart row, daily_views = sum."""
        fact = _make_fact_df(
            date_keys=[20250101, 20250101, 20250101],
            view_counts=[2, 3, 4],
            consumption_methods=["Web", "Mobile", "Web"],
        )
        mart, _ = _pipeline(fact)
        assert len(mart) == 1
        assert mart.iloc[0]["daily_views"] == 9

    def test_multiple_dates(self):
        """1 report, 1 user, 3 different dates → 3 mart rows."""
        fact = _make_fact_df(
            date_keys=[20250101, 20250102, 20250103],
            view_counts=[1, 2, 3],
        )
        mart, _ = _pipeline(fact)
        assert len(mart) == 3

    def test_multiple_users(self):
        """1 report, 2 users → 2 mart rows (each with correct views)."""
        fact = _make_fact_df(
            user_keys=["UK_0001", "UK_0002"],
            view_counts=[10, 20],
        )
        mart, _ = _pipeline(fact)
        assert len(mart) == 2
        views_by_user = dict(zip(mart["user_key"], mart["daily_views"]))
        assert views_by_user["UK_0001"] == 10
        assert views_by_user["UK_0002"] == 20

    def test_multiple_reports(self):
        """2 reports, 1 user → 2 mart rows."""
        fact = _make_fact_df(
            report_ids=["R_001", "R_002"],
            view_counts=[5, 7],
        )
        mart, _ = _pipeline(fact)
        assert len(mart) == 2

    def test_daily_views_sums_view_count(self):
        """Explicit check: sum(view_count) == daily_views."""
        fact = _make_fact_df(
            date_keys=[20250101, 20250101],
            view_counts=[6, 4],
            consumption_methods=["Web", "Mobile"],
        )
        mart, _ = _pipeline(fact)
        assert mart.iloc[0]["daily_views"] == 10

    def test_no_zero_filled_dates(self):
        """Only dates with actual events appear in mart."""
        fact = _make_fact_df(
            date_keys=[20250101, 20250103],
            view_counts=[1, 1],
        )
        mart, _ = _pipeline(fact)
        assert len(mart) == 2
        dates = set(mart["usage_date"])
        assert date(2025, 1, 2) not in dates


# ===========================================================================
# TestHistoryFields
# ===========================================================================

class TestHistoryFields:
    def _three_date_fact(self):
        return _make_fact_df(
            date_keys=[20250101, 20250105, 20250110],
            view_counts=[1, 2, 3],
        )

    def test_first_use_date(self):
        """min(usage_date) per (report_id, user_key)."""
        mart, _ = _pipeline(self._three_date_fact())
        assert mart["first_report_use_date"].min() == date(2025, 1, 1)

    def test_latest_use_date(self):
        """max(usage_date) per (report_id, user_key)."""
        mart, _ = _pipeline(self._three_date_fact())
        assert mart["latest_report_use_date"].max() == date(2025, 1, 10)

    def test_tenure_zero_on_first_day(self):
        """Tenure = 0 on first_use_date."""
        mart, _ = _pipeline(self._three_date_fact())
        first_row = mart[mart["first_use_flag"] == True].iloc[0]
        assert first_row["report_user_tenure_days"] == 0

    def test_tenure_increasing(self):
        """Tenure grows with later dates."""
        mart, _ = _pipeline(self._three_date_fact())
        tenures = mart.sort_values("usage_date")["report_user_tenure_days"].tolist()
        assert tenures == sorted(tenures)

    def test_first_use_flag_exactly_once(self):
        """Only one row per user-report has first_use_flag=True."""
        fact = _make_fact_df(
            date_keys=[20250101, 20250102, 20250103],
            view_counts=[1, 1, 1],
        )
        mart, _ = _pipeline(fact)
        assert mart["first_use_flag"].sum() == 1

    def test_lifetime_returned_false_single_date(self):
        """Only 1 date: lifetime_returned_flag=False."""
        fact = _make_fact_df(view_counts=[5])
        mart, _ = _pipeline(fact)
        assert mart.iloc[0]["lifetime_returned_flag"] == False

    def test_lifetime_returned_true_multiple_dates(self):
        """2+ dates: lifetime_returned_flag=True."""
        fact = _make_fact_df(
            date_keys=[20250101, 20250102],
            view_counts=[1, 1],
        )
        mart, _ = _pipeline(fact)
        assert mart["lifetime_returned_flag"].all()


# ===========================================================================
# TestPrivacy
# ===========================================================================

class TestPrivacy:
    def test_email_column_rejected(self):
        """Fact df with user_key=email → excluded from mart (prohibited_identifier)."""
        fact = _make_fact_df(user_keys=["user001@masegoinc.com"])
        mart, quality = _pipeline(fact)
        assert len(mart) == 0
        assert quality.iloc[0]["prohibited_identifier_event_count"] == 1

    def test_email_in_renamed_column_rejected(self):
        """Column named 'identifier' with email → validate_no_direct_identifiers raises."""
        df = pd.DataFrame([{"identifier": "user001@masegoinc.com", "total_views": 1}])
        with pytest.raises(ValueError, match="email-like value"):
            validate_no_direct_identifiers(df)

    def test_user_key_accepted(self):
        """UK_0001 style key passes normalisation and classification."""
        fact = _make_fact_df(user_keys=["UK_0001"])
        mart, _ = _pipeline(fact)
        assert len(mart) == 1
        assert mart.iloc[0]["user_key"] == "UK_0001"

    def test_no_dim_user_join(self):
        """Mart output has no user_id, unique_user, display_name."""
        fact = _make_fact_df()
        mart, _ = _pipeline(fact)
        for col in ("user_id", "unique_user", "display_name"):
            assert col not in mart.columns

    def test_no_direct_identifiers_in_mart(self):
        """validate_no_direct_identifiers passes on mart output."""
        fact = _make_fact_df()
        mart, _ = _pipeline(fact)
        validate_no_direct_identifiers(mart)  # must not raise

    def test_no_direct_identifiers_in_quality(self):
        """validate_no_direct_identifiers passes on quality output."""
        fact = _make_fact_df()
        _, quality = _pipeline(fact)
        validate_no_direct_identifiers(quality)  # must not raise

    def test_missing_user_key_excluded(self):
        """Null user_key → excluded from mart, counted in quality."""
        fact = _make_fact_df(user_keys=[None])
        mart, quality = _pipeline(fact)
        assert len(mart) == 0
        assert quality.iloc[0]["missing_user_id_event_count"] == 1

    def test_empty_string_user_key_excluded(self):
        """Empty string user_key → excluded from mart."""
        fact = _make_fact_df(user_keys=[""])
        mart, quality = _pipeline(fact)
        assert len(mart) == 0
        assert quality.iloc[0]["missing_user_id_event_count"] == 1

    def test_no_shared_unknown_user_placeholder(self):
        """Missing user_key rows do not create an 'UNKNOWN' surrogate in mart."""
        fact = _make_fact_df(user_keys=[None])
        mart, _ = _pipeline(fact)
        assert "UNKNOWN" not in mart["user_key"].astype(str).tolist()


# ===========================================================================
# TestValidity
# ===========================================================================

class TestValidity:
    def test_missing_report_id_excluded(self):
        """Null report_id → excluded."""
        fact = _make_fact_df(report_ids=[None])
        mart, quality = _pipeline(fact)
        assert len(mart) == 0
        assert quality.iloc[0]["invalid_report_id_event_count"] == 1

    def test_invalid_date_excluded(self):
        """Unparseable date_key → usage_date = NaT → excluded."""
        fact = pd.DataFrame([{
            "date_key": "notadate",
            "report_id": "R_001",
            "user_key": "UK_0001",
            "consumption_method": "Web",
            "distribution_method": "Direct",
            "view_count": 1,
        }])
        mart, quality = _pipeline(fact)
        assert len(mart) == 0
        assert quality.iloc[0]["invalid_date_event_count"] == 1

    def test_future_date_excluded(self):
        """date > as_of_date → excluded."""
        fact = _make_fact_df(date_keys=[20991231])
        mart, quality = _pipeline(fact, as_of_date=date(2026, 7, 24))
        assert len(mart) == 0
        assert quality.iloc[0]["future_date_event_count"] == 1

    def test_non_finite_view_count_excluded(self):
        """NaN view_count → excluded."""
        fact = _make_fact_df(view_counts=[float("nan")])
        mart, quality = _pipeline(fact)
        assert len(mart) == 0
        assert quality.iloc[0]["non_finite_view_count_event_count"] == 1

    def test_zero_view_count_excluded(self):
        """view_count = 0 → excluded."""
        fact = _make_fact_df(view_counts=[0])
        mart, quality = _pipeline(fact)
        assert len(mart) == 0
        assert quality.iloc[0]["zero_or_negative_view_event_count"] == 1

    def test_negative_view_count_excluded(self):
        """Negative view_count → excluded."""
        fact = _make_fact_df(view_counts=[-3])
        mart, quality = _pipeline(fact)
        assert len(mart) == 0
        assert quality.iloc[0]["zero_or_negative_view_event_count"] == 1

    def test_valid_leap_day(self):
        """2024-02-29 is a valid date."""
        fact = _make_fact_df(date_keys=[20240229])
        mart, _ = _pipeline(fact, as_of_date=date(2026, 7, 24))
        assert len(mart) == 1
        assert mart.iloc[0]["usage_date"] == date(2024, 2, 29)

    def test_date_boundary(self):
        """Last day of month is valid."""
        fact = _make_fact_df(date_keys=[20250131])
        mart, _ = _pipeline(fact)
        assert len(mart) == 1
        assert mart.iloc[0]["usage_date"] == date(2025, 1, 31)


# ===========================================================================
# TestDuplicates
# ===========================================================================

class TestDuplicates:
    def test_exact_duplicate_removed(self):
        """2 identical rows → 1 event after dedup."""
        row = {
            "date_key": 20250101,
            "report_id": "R_001",
            "user_key": "UK_0001",
            "consumption_method": "Web",
            "distribution_method": "Direct",
            "view_count": 1,
        }
        fact = pd.DataFrame([row, row])
        mart, quality = _pipeline(fact)
        assert len(mart) == 1
        assert quality.iloc[0]["duplicate_event_count_removed"] >= 1

    def test_legitimate_repeated_events_retained(self):
        """2 rows same (report, user, date) but different fields → both retained."""
        fact = _make_fact_df(
            date_keys=[20250101, 20250101],
            view_counts=[1, 1],
            consumption_methods=["Web", "Mobile"],  # different field
        )
        mart, _ = _pipeline(fact)
        assert mart.iloc[0]["source_event_count"] == 2

    def test_duplicate_count_correct(self):
        """duplicate_event_count_removed reflects actual duplicates."""
        row = {
            "date_key": 20250101,
            "report_id": "R_001",
            "user_key": "UK_0001",
            "consumption_method": "Web",
            "distribution_method": "Direct",
            "view_count": 2,
        }
        fact = pd.DataFrame([row, row, row])
        _, quality = _pipeline(fact)
        assert quality.iloc[0]["duplicate_event_count_removed"] == 2

    def test_no_duplicate_grain_after_aggregation(self):
        """(report_id, user_key, usage_date) is unique in mart."""
        fact = _make_fact_df(
            date_keys=[20250101, 20250101],
            view_counts=[1, 2],
            consumption_methods=["Web", "Mobile"],
        )
        mart, _ = _pipeline(fact)
        assert not mart.duplicated(subset=["report_id", "user_key", "usage_date"]).any()

    def test_duplicate_signature_deterministic(self):
        """Same input → same dedup result (excluding generated_at timestamp)."""
        row = {
            "date_key": 20250101, "report_id": "R_001", "user_key": "UK_0001",
            "consumption_method": "Web", "distribution_method": "Direct", "view_count": 1,
        }
        fact = pd.DataFrame([row, row])
        mart1, _ = _pipeline(fact)
        mart2, _ = _pipeline(fact)
        # Exclude generated_at (timestamp differs between runs) from comparison
        cols = [c for c in mart1.columns if c != "generated_at"]
        pd.testing.assert_frame_equal(
            mart1[cols].reset_index(drop=True),
            mart2[cols].reset_index(drop=True),
        )


# ===========================================================================
# TestQualityOutput
# ===========================================================================

class TestQualityOutput:
    def test_counts_reconcile(self):
        """source_event_count = valid_positive + excluded."""
        fact = _make_fact_df(
            user_keys=["UK_0001", None],
            view_counts=[5, 1],
        )
        _, quality = _pipeline(fact)
        row = quality.iloc[0]
        assert row["source_event_count"] == row["valid_positive_usage_event_count"] + row["excluded_event_count"]

    def test_excluded_share_calculation(self):
        """excluded_user_event_share = excluded / source."""
        fact = _make_fact_df(
            user_keys=["UK_0001", "UK_0002", None, None],
            view_counts=[1, 1, 1, 1],
        )
        _, quality = _pipeline(fact)
        row = quality.iloc[0]
        expected = row["excluded_event_count"] / row["source_event_count"]
        assert abs(row["excluded_user_event_share"] - expected) < 1e-9

    def test_good_status(self):
        """All valid events → status = good."""
        fact = _make_fact_df(view_counts=[1, 2, 3])
        _, quality = _pipeline(fact)
        assert quality.iloc[0]["data_quality_status"] == "good"

    def test_warning_status(self):
        """~10% excluded → status = warning."""
        # 9 valid + 1 invalid = 10% exclusion
        valid_rows = [{"date_key": 20250101, "report_id": "R_001", "user_key": f"UK_{i:04d}",
                       "consumption_method": "Web", "distribution_method": "Direct", "view_count": 1}
                      for i in range(9)]
        invalid_row = {"date_key": 20250101, "report_id": "R_001", "user_key": None,
                       "consumption_method": "Web", "distribution_method": "Direct", "view_count": 1}
        fact = pd.DataFrame(valid_rows + [invalid_row])
        _, quality = _pipeline(fact)
        assert quality.iloc[0]["data_quality_status"] == "warning"

    def test_poor_status(self):
        """>20% excluded → status = poor."""
        valid_rows = [{"date_key": 20250101, "report_id": "R_001", "user_key": f"UK_{i:04d}",
                       "consumption_method": "Web", "distribution_method": "Direct", "view_count": 1}
                      for i in range(7)]
        # Use distinct date_keys so dedup keeps all 3 rows
        invalid_rows = [{"date_key": 20250101 + j, "report_id": "R_001", "user_key": None,
                         "consumption_method": "Web", "distribution_method": "Direct", "view_count": 1}
                        for j in range(3)]
        fact = pd.DataFrame(valid_rows + invalid_rows)
        _, quality = _pipeline(fact)
        assert quality.iloc[0]["data_quality_status"] == "poor"

    def test_no_valid_user_data_status(self):
        """All events have missing user_key → status = no_valid_user_data."""
        fact = _make_fact_df(user_keys=[None, None])
        _, quality = _pipeline(fact)
        assert quality.iloc[0]["data_quality_status"] == "no_valid_user_data"

    def test_deterministic_reason_ordering(self):
        """Same input → same data_quality_reasons string."""
        fact = _make_fact_df(
            user_keys=["UK_0001", None],
            view_counts=[1, 1],
        )
        _, q1 = _pipeline(fact)
        _, q2 = _pipeline(fact)
        assert q1.iloc[0]["data_quality_reasons"] == q2.iloc[0]["data_quality_reasons"]

    def test_report_with_only_invalid_users(self):
        """No valid rows in mart but counted in quality."""
        fact = _make_fact_df(user_keys=[None])
        mart, quality = _pipeline(fact)
        assert len(mart) == 0
        assert quality.iloc[0]["source_event_count"] == 1
        assert quality.iloc[0]["valid_positive_usage_event_count"] == 0

    def test_mixed_valid_invalid(self):
        """Some valid, some not → correct counts."""
        fact = _make_fact_df(
            user_keys=["UK_0001", "UK_0002", None],
            view_counts=[1, 2, 3],
        )
        mart, quality = _pipeline(fact)
        row = quality.iloc[0]
        assert row["source_event_count"] == 3
        assert row["valid_positive_usage_event_count"] == 2
        assert row["missing_user_id_event_count"] == 1


# ===========================================================================
# TestMetadata
# ===========================================================================

class TestMetadata:
    def test_report_name_joined_safely(self):
        """Metadata join works and populates report_name."""
        fact = _make_fact_df()
        meta = pd.DataFrame([{"report_id": "R_001", "report_name": "Finance Report"}])
        _, quality = _pipeline(fact, report_meta_df=meta)
        assert quality.iloc[0]["report_name"] == "Finance Report"

    def test_missing_report_metadata_does_not_drop_usage(self):
        """Mart rows unaffected when metadata is None."""
        fact = _make_fact_df()
        mart, _ = _pipeline(fact, report_meta_df=None)
        assert len(mart) == 1

    def test_duplicate_report_metadata_rejected(self):
        """Duplicate report_id in metadata → ValueError."""
        meta = pd.DataFrame([
            {"report_id": "R_001", "report_name": "Finance Report"},
            {"report_id": "R_001", "report_name": "Duplicate Name"},
        ])
        fact = _make_fact_df()
        normalized = normalize_report_view_events(fact, as_of_date=date(2026, 7, 24))
        classified = classify_source_record_quality(normalized, as_of_date=date(2026, 7, 24))
        deduped, dedup_counts = remove_exact_event_duplicates(classified)
        with pytest.raises(ValueError, match="duplicate"):
            build_report_user_data_quality(
                classified_df=deduped,
                dedup_counts_df=dedup_counts,
                analytics_run_id="test",
                report_meta_df=meta,
            )


# ===========================================================================
# TestPersistence
# ===========================================================================

class TestPersistence:
    def _run_and_persist(self, tmp_path: Path):
        fact = _make_fact_df(
            date_keys=[20250101, 20250102],
            view_counts=[1, 2],
        )
        mart, quality = _pipeline(fact)
        return persist_report_user_daily_outputs(mart, quality, tmp_path)

    def test_mart_file_created(self, tmp_path):
        """mart file is written to outputs/analytics/."""
        paths = self._run_and_persist(tmp_path)
        assert paths["mart"].exists()

    def test_quality_file_created(self, tmp_path):
        """quality file is written to outputs/analytics/."""
        paths = self._run_and_persist(tmp_path)
        assert paths["quality"].exists()

    def test_schemas_stable(self, tmp_path):
        """Output columns match MART_REPORT_USER_DAILY_COLS."""
        paths = self._run_and_persist(tmp_path)
        written = pd.read_csv(paths["mart"])
        for col in MART_REPORT_USER_DAILY_COLS:
            assert col in written.columns, f"Missing column: {col}"

    def test_deterministic_sorting(self, tmp_path):
        """Same input → same row order in output (excluding generated_at timestamp)."""
        fact = _make_fact_df(
            date_keys=[20250103, 20250101, 20250102],
            user_keys=["UK_0002", "UK_0001", "UK_0001"],
            view_counts=[1, 1, 1],
        )
        mart1, quality1 = _pipeline(fact)
        paths1 = persist_report_user_daily_outputs(mart1, quality1, tmp_path)
        df1 = pd.read_csv(paths1["mart"])

        mart2, quality2 = _pipeline(fact)
        paths2 = persist_report_user_daily_outputs(mart2, quality2, tmp_path)
        df2 = pd.read_csv(paths2["mart"])

        stable_cols = [c for c in df1.columns if c != "generated_at"]
        pd.testing.assert_frame_equal(
            df1[stable_cols].reset_index(drop=True),
            df2[stable_cols].reset_index(drop=True),
        )

    def test_latest_files_replaced(self, tmp_path):
        """Second call overwrites first call."""
        fact = _make_fact_df()
        mart, quality = _pipeline(fact)
        paths = persist_report_user_daily_outputs(mart, quality, tmp_path)
        first_mtime = paths["mart"].stat().st_mtime

        import time; time.sleep(0.05)

        paths2 = persist_report_user_daily_outputs(mart, quality, tmp_path)
        second_mtime = paths2["mart"].stat().st_mtime
        assert second_mtime >= first_mtime

    def test_source_fact_table_unchanged(self, tmp_path):
        """fact_report_views not modified by pipeline."""
        fact = _make_fact_df()
        original_cols = list(fact.columns)
        original_vals = fact.copy()
        _pipeline(fact)
        assert list(fact.columns) == original_cols
        pd.testing.assert_frame_equal(fact, original_vals)

    def test_invalid_output_rejected(self, tmp_path):
        """Validation failure → ValueError (no file written)."""
        # Build a mart with a duplicate grain row (invalid)
        fact = _make_fact_df()
        mart, quality = _pipeline(fact)
        # Duplicate a row to create invalid grain
        bad_mart = pd.concat([mart, mart], ignore_index=True)
        with pytest.raises(ValueError):
            persist_report_user_daily_outputs(bad_mart, quality, tmp_path)


# ===========================================================================
# TestValidation
# ===========================================================================

class TestValidation:
    def _valid_mart(self):
        fact = _make_fact_df(
            date_keys=[20250101, 20250102],
            view_counts=[1, 2],
        )
        mart, _ = _pipeline(fact)
        return mart

    def test_duplicate_grain_rejected(self):
        """Duplicate (report_id, user_key, usage_date) → raises."""
        mart = self._valid_mart()
        bad = pd.concat([mart, mart], ignore_index=True)
        with pytest.raises(ValueError, match=r"\[2\]"):
            validate_report_user_daily(bad)

    def test_negative_daily_views_rejected(self):
        """Negative daily_views → raises."""
        mart = self._valid_mart().copy()
        mart.loc[0, "daily_views"] = -1
        with pytest.raises(ValueError, match=r"\[9\]"):
            validate_report_user_daily(mart)

    def test_future_usage_date_rejected(self):
        """usage_date in future → raises."""
        mart = self._valid_mart().copy()
        mart.loc[0, "usage_date"] = date(2099, 1, 1)
        with pytest.raises(ValueError, match=r"\[8\]"):
            validate_report_user_daily(mart, as_of_date=date(2026, 7, 24))

    def test_first_use_date_after_usage_date_rejected(self):
        """first_report_use_date > usage_date → raises."""
        mart = self._valid_mart().copy()
        mart.loc[0, "first_report_use_date"] = date(2099, 1, 1)
        with pytest.raises(ValueError, match=r"\[13\]"):
            validate_report_user_daily(mart)

    def test_first_use_flag_multiple_times_rejected(self):
        """More than one first_use_flag=True per (report_id, user_key) → raises."""
        mart = self._valid_mart().copy()
        mart["first_use_flag"] = True  # both rows True → invalid (caught at [17] or [18])
        with pytest.raises(ValueError):
            validate_report_user_daily(mart)

    def test_lifetime_returned_flag_inconsistency_rejected(self):
        """lifetime_returned_flag inconsistent with dates → raises."""
        fact = _make_fact_df(
            date_keys=[20250101, 20250102],
            view_counts=[1, 1],
        )
        mart, _ = _pipeline(fact)
        mart = mart.copy()
        # Force wrong flag: both dates exist so should be True
        mart["lifetime_returned_flag"] = False
        with pytest.raises(ValueError, match=r"\[19\]"):
            validate_report_user_daily(mart)
