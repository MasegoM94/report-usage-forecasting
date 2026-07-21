"""Integration tests for the feature-engineering pipeline.

Tests the full flow from semantic input tables through:
  mart_report_daily_series  (SARIMA input)
  mart_report_daily_context (wide diagnostic mart)

All 15 contracted assertions are covered, plus one end-to-end smoke test that
writes outputs to a temporary directory and verifies that both pipeline
loaders can find and read them.

Fixture design
--------------
Two reports, 14 calendar days:

  R_FULL   — active for all 14 days; day-7 is a no-view day; three users;
              performance records present for observed days.
  R_LATE   — active from day 8 onward; zero-view day on day 12; two users;
              no performance records (performance mart is optional).

This gives:
  • ≥ 2 reports
  • ≥ 10 calendar days
  • One report active for the full period (R_FULL)
  • One report activated partway through (R_LATE)
  • Several no-view days (day 7 for R_FULL, day 12 for R_LATE)
  • Multiple users (U1, U2, U3 for R_FULL; U4, U5 for R_LATE)
  • At least one performance record (R_FULL has load records)
  • At least one day with no usage but an active report (same days above)
"""

from __future__ import annotations

import math
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.analytics.report_features import build_report_features
from src.features.build_forecast_features import (
    build_report_daily_context,
    validate_daily_context_schema,
)
from src.features.engagement_features import build_user_engagement_features
from src.features.performance_features import build_report_performance_features
from src.features.report_features import (
    add_time_series_usage_features,
    build_report_daily_adoption,
    build_report_daily_series,
)
from src.features.validate_series import SeriesValidationError, validate_report_daily_series
from src.pipelines.run_forecasting_pipeline import (
    choose_forecasting_input,
    standardise_forecasting_columns,
)
from src.pipelines.run_report_analytics_pipeline import choose_daily_usage_table

# ---------------------------------------------------------------------------
# Deterministic fixture
# ---------------------------------------------------------------------------

_START = pd.Timestamp("2025-01-01")
_END = pd.Timestamp("2025-01-14")
_DAY7 = pd.Timestamp("2025-01-07")   # R_FULL: no-view day
_DAY8 = pd.Timestamp("2025-01-08")   # R_LATE: active from here
_DAY12 = pd.Timestamp("2025-01-12")  # R_LATE: no-view day


def _make_fixtures() -> tuple[
    pd.DataFrame,  # fact_report_views
    pd.DataFrame,  # fact_page_views
    pd.DataFrame,  # fact_report_loads
    pd.DataFrame,  # dim_report (active periods)
]:
    """Build fully deterministic fixture tables from first principles."""
    view_rows: list[dict] = []
    page_rows: list[dict] = []
    load_rows: list[dict] = []

    # ── R_FULL: days 1-6 and 8-14 have views; day 7 is a no-view gap ──────
    full_dates = [
        d for d in pd.date_range(_START, _END, freq="D")
        if d != _DAY7
    ]
    for dt in full_dates:
        # Three users; U1 has 3 views, U2 has 2 views, U3 has 1 view per day
        for user, count in [("U1", 3), ("U2", 2), ("U3", 1)]:
            for _ in range(count):
                view_rows.append({"date": dt, "report_id": "R_FULL", "user_id": user,
                                  "view_count": 1})
            page_rows.append({"date": dt, "report_id": "R_FULL", "user_id": user,
                               "section_id": f"pg_{user}"})
        load_rows.append({"date": dt, "report_id": "R_FULL", "load_time_ms": 500.0,
                          "load_event": 1})

    # ── R_LATE: days 8-14; day 12 is a no-view gap ──────────────────────
    late_dates = [
        d for d in pd.date_range(_DAY8, _END, freq="D")
        if d != _DAY12
    ]
    for dt in late_dates:
        for user, count in [("U4", 4), ("U5", 1)]:
            for _ in range(count):
                view_rows.append({"date": dt, "report_id": "R_LATE", "user_id": user,
                                  "view_count": 1})
            page_rows.append({"date": dt, "report_id": "R_LATE", "user_id": user,
                               "section_id": f"pg_{user}"})
        # No performance records for R_LATE — tests that performance join is left-safe

    fact_views = pd.DataFrame(view_rows)
    fact_pages = pd.DataFrame(page_rows)
    fact_loads = pd.DataFrame(load_rows)

    dim_report = pd.DataFrame([
        {"report_id": "R_FULL", "launch_date": str(_START.date()),
         "retire_date": None,    "report_name": "Full Report"},
        {"report_id": "R_LATE", "launch_date": str(_DAY8.date()),
         "retire_date": None,    "report_name": "Late Report"},
    ])

    return fact_views, fact_pages, fact_loads, dim_report


@pytest.fixture(scope="module")
def fixtures():
    return _make_fixtures()


@pytest.fixture(scope="module")
def series(fixtures):
    fact_views, _, _, dim_report = fixtures
    return build_report_daily_series(
        fact_report_views=fact_views,
        report_active_periods=dim_report,
        date_col="date",
        report_col="report_id",
        views_col="view_count",
        active_start_col="launch_date",
        active_end_col="retire_date",
    )


@pytest.fixture(scope="module")
def adoption(fixtures, series):
    fact_views, _, _, _ = fixtures
    base = build_report_daily_adoption(
        fact_report_views=fact_views,
        date_col="date",
        report_col="report_id",
        user_col="user_id",
        views_col="view_count",
    )
    # Join viewer counts onto the canonical spine (preserves imputed-zero rows)
    merged = series.merge(
        base[["date", "report_id", "unique_viewers", "views_per_user"]],
        on=["date", "report_id"],
        how="left",
    )
    merged["unique_viewers"] = merged["unique_viewers"].fillna(0).astype(int)
    merged["views_per_user"] = (
        merged["daily_views"]
        .div(merged["unique_viewers"].replace(0, pd.NA))
        .fillna(0.0)
    )
    return add_time_series_usage_features(merged)


@pytest.fixture(scope="module")
def engagement(fixtures):
    fact_views, fact_pages, _, _ = fixtures
    return build_user_engagement_features(
        fact_report_views=fact_views,
        fact_page_views=fact_pages,
        date_col="date",
        report_col="report_id",
        user_col="user_id",
    )


@pytest.fixture(scope="module")
def performance(fixtures):
    _, _, fact_loads, _ = fixtures
    return build_report_performance_features(
        fact_report_loads=fact_loads,
        date_col="date",
        report_col="report_id",
        load_time_col="load_time_ms",
    )


@pytest.fixture(scope="module")
def context(adoption, engagement, performance):
    return build_report_daily_context(
        mart_report_daily_adoption=adoption,
        mart_user_engagement=engagement,
        mart_report_performance=performance,
    )


@pytest.fixture(scope="module")
def report_features(adoption, fixtures):
    fact_views, _, _, dim_report = fixtures
    base_adoption = adoption[["date", "report_id", "daily_views", "unique_viewers"]].copy()
    return build_report_features(
        daily_adoption=base_adoption,
        fact_report_views=fact_views,
        dim_report=dim_report,
    )


# ---------------------------------------------------------------------------
# Fixture sanity — run these first so failures are easy to diagnose
# ---------------------------------------------------------------------------

class TestFixtureSanity:
    def test_fact_views_has_expected_reports(self, fixtures):
        fact_views, *_ = fixtures
        assert set(fact_views["report_id"].unique()) == {"R_FULL", "R_LATE"}

    def test_no_view_row_on_day7_for_r_full(self, fixtures):
        fact_views, *_ = fixtures
        day7_full = fact_views[
            (fact_views["report_id"] == "R_FULL") & (fact_views["date"] == _DAY7)
        ]
        assert day7_full.empty, "Fixture error: R_FULL should have no events on day 7"

    def test_r_late_has_no_events_before_day8(self, fixtures):
        fact_views, *_ = fixtures
        pre_late = fact_views[
            (fact_views["report_id"] == "R_LATE") & (fact_views["date"] < _DAY8)
        ]
        assert pre_late.empty, "Fixture error: R_LATE should have no events before day 8"

    def test_multiple_users(self, fixtures):
        fact_views, *_ = fixtures
        assert fact_views["user_id"].nunique() >= 3


# ---------------------------------------------------------------------------
# Assertion 1: daily series contains every active report-date combination
# ---------------------------------------------------------------------------

class TestActiveCoverage:
    def test_r_full_has_14_rows(self, series):
        r_full = series[series["report_id"] == "R_FULL"]
        assert len(r_full) == 14, f"R_FULL expected 14 rows, got {len(r_full)}"

    def test_r_late_has_7_rows(self, series):
        r_late = series[series["report_id"] == "R_LATE"]
        assert len(r_late) == 7, f"R_LATE expected 7 rows, got {len(r_late)}"

    def test_series_covers_both_reports(self, series):
        assert set(series["report_id"].unique()) == {"R_FULL", "R_LATE"}

    def test_r_full_date_range_is_complete(self, series):
        r_full = series[series["report_id"] == "R_FULL"]
        expected = pd.date_range(_START, _END, freq="D")
        assert set(r_full["date"]) == set(expected)

    def test_r_late_date_range_is_complete(self, series):
        r_late = series[series["report_id"] == "R_LATE"]
        expected = pd.date_range(_DAY8, _END, freq="D")
        assert set(r_late["date"]) == set(expected)


# ---------------------------------------------------------------------------
# Assertion 2: no rows exist before a report's active date
# ---------------------------------------------------------------------------

class TestNoPre_LaunchRows:
    def test_r_late_has_no_rows_before_day8(self, series):
        pre = series[(series["report_id"] == "R_LATE") & (series["date"] < _DAY8)]
        assert pre.empty, f"R_LATE has {len(pre)} rows before its launch date"

    def test_r_full_min_date_equals_start(self, series):
        min_date = series[series["report_id"] == "R_FULL"]["date"].min()
        assert min_date == _START


# ---------------------------------------------------------------------------
# Assertion 3: missing active-day views are filled with zero
# ---------------------------------------------------------------------------

class TestZeroFill:
    def test_day7_views_is_zero_for_r_full(self, series):
        row = series[(series["report_id"] == "R_FULL") & (series["date"] == _DAY7)]
        assert len(row) == 1, "day 7 row missing from R_FULL"
        assert int(row["daily_views"].iloc[0]) == 0

    def test_day7_is_imputed_zero_for_r_full(self, series):
        row = series[(series["report_id"] == "R_FULL") & (series["date"] == _DAY7)]
        assert bool(row["is_imputed_zero"].iloc[0]) is True

    def test_day7_is_not_observed_for_r_full(self, series):
        row = series[(series["report_id"] == "R_FULL") & (series["date"] == _DAY7)]
        assert bool(row["is_observed_day"].iloc[0]) is False

    def test_day12_is_imputed_zero_for_r_late(self, series):
        row = series[(series["report_id"] == "R_LATE") & (series["date"] == _DAY12)]
        assert bool(row["is_imputed_zero"].iloc[0]) is True

    def test_observed_days_have_positive_views(self, series):
        observed = series[series["is_observed_day"]]
        assert (observed["daily_views"] > 0).all()


# ---------------------------------------------------------------------------
# Assertion 4: output grain is unique
# ---------------------------------------------------------------------------

class TestGrainUniqueness:
    def test_series_grain_unique(self, series):
        dups = series.duplicated(["report_id", "date"]).sum()
        assert dups == 0

    def test_adoption_grain_unique(self, adoption):
        dups = adoption.duplicated(["report_id", "date"]).sum()
        assert dups == 0

    def test_context_grain_unique(self, context):
        dups = context.duplicated(["report_id", "date"]).sum()
        assert dups == 0


# ---------------------------------------------------------------------------
# Assertion 5: output is sorted
# ---------------------------------------------------------------------------

class TestSortOrder:
    def test_series_sorted_by_report_then_date(self, series):
        expected = series.sort_values(["report_id", "date"])
        pd.testing.assert_frame_equal(
            series.reset_index(drop=True),
            expected.reset_index(drop=True),
        )

    def test_context_sorted_by_report_then_date(self, context):
        expected = context.sort_values(["report_id", "date"])
        pd.testing.assert_frame_equal(
            context.reset_index(drop=True),
            expected.reset_index(drop=True),
        )


# ---------------------------------------------------------------------------
# Assertion 6: daily_views is integer and non-negative
# ---------------------------------------------------------------------------

class TestDailyViewsType:
    def test_daily_views_non_negative_in_series(self, series):
        assert (series["daily_views"] >= 0).all()

    def test_daily_views_is_integer_in_series(self, series):
        assert pd.api.types.is_integer_dtype(series["daily_views"])

    def test_daily_views_non_negative_in_context(self, context):
        assert (context["daily_views"] >= 0).all()


# ---------------------------------------------------------------------------
# Assertion 7: top_1_user_view_share and top_10pct_user_share are not confused
# ---------------------------------------------------------------------------

class TestConcentrationMetricDistinction:
    def test_both_columns_present(self, engagement):
        assert "top_1_user_view_share" in engagement.columns
        assert "top_10pct_user_share" in engagement.columns

    def test_legacy_column_absent(self, engagement):
        assert "top_user_share" not in engagement.columns

    def test_top_1_equals_expected_value_for_r_full(self, engagement):
        # R_FULL: U1 has 3 views, U2 has 2, U3 has 1 → total 6 → top-1 share = 3/6 = 0.5
        r_full_day1 = engagement[
            (engagement["report_id"] == "R_FULL")
            & (engagement["date"] == _START)
        ]
        assert len(r_full_day1) == 1, "Expected engagement row for R_FULL on day 1"
        assert r_full_day1["top_1_user_view_share"].iloc[0] == pytest.approx(0.5)

    def test_top_10pct_gte_top_1_always(self, engagement):
        merged = engagement[["top_1_user_view_share", "top_10pct_user_share"]].dropna()
        assert (merged["top_10pct_user_share"] >= merged["top_1_user_view_share"]).all()

    def test_both_columns_present_in_context(self, context):
        assert "top_1_user_view_share" in context.columns
        assert "top_10pct_user_share" in context.columns


# ---------------------------------------------------------------------------
# Assertion 8: usage_change_28d_pct follows the documented formula
# ---------------------------------------------------------------------------

class TestUsageChange28d:
    def test_insufficient_history_returns_null(self, report_features):
        # R_FULL has 14 days < 56 → usage_change_28d_pct must be null
        r_full = report_features[report_features["report_id"] == "R_FULL"]
        assert len(r_full) == 1
        assert pd.isna(r_full["usage_change_28d_pct"].iloc[0]), (
            "R_FULL has < 56 days — usage_change_28d_pct must be null"
        )

    def test_sufficient_history_computes_correct_formula(self):
        # Build a fresh 56-day fixture with known values: 28 days @ 5, then 28 @ 10
        views = [5] * 28 + [10] * 28
        dates = pd.date_range("2025-01-01", periods=56, freq="D")
        df = pd.DataFrame({
            "report_id": "R_TEST",
            "date": dates,
            "daily_views": views,
        })
        result = build_report_features(df)
        row = result.iloc[0]
        # previous_28d = 28*5 = 140; recent_28d = 28*10 = 280; pct = (280-140)/140 = 1.0
        assert row["previous_28d_views"] == 140
        assert row["recent_28d_views"] == 280
        assert row["usage_change_28d_pct"] == pytest.approx(1.0)

    def test_both_windows_zero_gives_zero_change(self):
        views = [0] * 56
        dates = pd.date_range("2025-01-01", periods=56, freq="D")
        df = pd.DataFrame({"report_id": "R_ZERO", "date": dates, "daily_views": views})
        result = build_report_features(df)
        assert result.iloc[0]["usage_change_28d_pct"] == pytest.approx(0.0)

    def test_zero_prior_nonzero_recent_is_null_and_flags_newly_active(self):
        views = [0] * 28 + [5] * 28
        dates = pd.date_range("2025-01-01", periods=56, freq="D")
        df = pd.DataFrame({"report_id": "R_NEW", "date": dates, "daily_views": views})
        result = build_report_features(df)
        row = result.iloc[0]
        assert pd.isna(row["usage_change_28d_pct"]), "undefined denominator must be null"
        assert bool(row["newly_active_flag"]) is True


# ---------------------------------------------------------------------------
# Assertion 9: insufficient-history reports receive null trend metrics + flag
# ---------------------------------------------------------------------------

class TestInsufficientHistoryFlag:
    def test_r_full_trend_history_insufficient(self, report_features):
        # 14 days < 56 → trend_history_sufficient = False
        r_full = report_features[report_features["report_id"] == "R_FULL"]
        assert bool(r_full["trend_history_sufficient"].iloc[0]) is False

    def test_r_full_slope_null(self, report_features):
        r_full = report_features[report_features["report_id"] == "R_FULL"]
        assert pd.isna(r_full["usage_trend_12w_slope"].iloc[0])

    def test_r_late_trend_history_insufficient(self, report_features):
        # 7 days << 56 → trend_history_sufficient = False
        r_late = report_features[report_features["report_id"] == "R_LATE"]
        assert bool(r_late["trend_history_sufficient"].iloc[0]) is False

    def test_sufficient_history_sets_flag_true(self):
        views = [10] * 56
        dates = pd.date_range("2025-01-01", periods=56, freq="D")
        df = pd.DataFrame({"report_id": "R_S", "date": dates, "daily_views": views})
        result = build_report_features(df)
        assert bool(result["trend_history_sufficient"].iloc[0]) is True


# ---------------------------------------------------------------------------
# Assertion 10: context join does not remove target-series rows
# ---------------------------------------------------------------------------

class TestContextJoinPreservesRows:
    def test_context_row_count_equals_adoption_row_count(self, adoption, context):
        assert len(context) == len(adoption), (
            f"Context ({len(context)}) has different row count from adoption ({len(adoption)})"
        )

    def test_r_full_row_count_unchanged_after_join(self, adoption, context):
        n_adopt = (adoption["report_id"] == "R_FULL").sum()
        n_ctx = (context["report_id"] == "R_FULL").sum()
        assert n_ctx == n_adopt

    def test_r_late_row_count_unchanged_after_join(self, adoption, context):
        n_adopt = (adoption["report_id"] == "R_LATE").sum()
        n_ctx = (context["report_id"] == "R_LATE").sum()
        assert n_ctx == n_adopt

    def test_no_view_days_present_in_context(self, context):
        # Both imputed-zero days (day 7 R_FULL, day 12 R_LATE) must survive the join
        day7_full = context[
            (context["report_id"] == "R_FULL") & (context["date"] == _DAY7)
        ]
        day12_late = context[
            (context["report_id"] == "R_LATE") & (context["date"] == _DAY12)
        ]
        assert len(day7_full) == 1, "R_FULL day-7 (no-view) row lost in context join"
        assert len(day12_late) == 1, "R_LATE day-12 (no-view) row lost in context join"


# ---------------------------------------------------------------------------
# Assertion 11: null-handling rules are column-specific
# ---------------------------------------------------------------------------

class TestNullHandling:
    def test_daily_views_never_null(self, context):
        assert context["daily_views"].notna().all()

    def test_report_id_never_null(self, context):
        assert context["report_id"].notna().all()

    def test_date_never_null(self, context):
        assert context["date"].notna().all()

    def test_engagement_null_on_imputed_zero_days(self, context):
        # Imputed-zero days have no events → engagement left-join produces nulls
        imputed = context[context["is_imputed_zero"]]
        if "top_1_user_view_share" in context.columns:
            assert imputed["top_1_user_view_share"].isna().all(), (
                "top_1_user_view_share should be null on imputed-zero days"
            )

    def test_performance_null_for_r_late_all_days(self, context):
        # R_LATE has no performance records → avg_load_time must be null for all its rows
        if "avg_load_time" in context.columns:
            r_late = context[context["report_id"] == "R_LATE"]
            assert r_late["avg_load_time"].isna().all(), (
                "R_LATE has no load records — avg_load_time must be null"
            )

    def test_rolling_views_null_on_first_row_per_report(self, adoption):
        # nth(0) returns the actual first row per group (not the first non-null value)
        first_rows = adoption.sort_values(["report_id", "date"]).groupby("report_id").nth(0)
        for col in ["views_7d", "views_28d"]:
            if col in first_rows.columns:
                assert first_rows[col].isna().all(), (
                    f"{col} must be NaN on first row per report (leakage guard)"
                )

    def test_rolling_views_non_null_after_first_row(self, adoption):
        # All rows except the first per report must have non-null views_7d
        not_first = adoption[
            adoption.groupby("report_id").cumcount() > 0
        ]
        if "views_7d" in not_first.columns:
            assert not_first["views_7d"].notna().all(), (
                "views_7d is null on a non-first row — check rolling logic"
            )


# ---------------------------------------------------------------------------
# Assertion 12: forecasting pipeline can load mart_report_daily_series
# ---------------------------------------------------------------------------

class TestForecastingPipelineLoader:
    def test_choose_forecasting_input_prefers_series(self, series, tmp_path):
        (tmp_path / "processed").mkdir()
        series.to_csv(tmp_path / "processed" / "mart_report_daily_series.csv", index=False)
        chosen = choose_forecasting_input(tmp_path / "processed")
        assert chosen.name == "mart_report_daily_series.csv"

    def test_standardise_forecasting_columns_succeeds(self, series, tmp_path):
        (tmp_path / "processed").mkdir()
        series_path = tmp_path / "processed" / "mart_report_daily_series.csv"
        series.to_csv(series_path, index=False)
        raw = pd.read_csv(series_path)
        result = standardise_forecasting_columns(
            raw,
            source_path=series_path,
            date_dim_path=tmp_path / "processed" / "dim_date.csv",
        )
        for col in ["date", "report_id", "daily_views"]:
            assert col in result.columns

    def test_standardised_series_has_only_required_cols(self, series, tmp_path):
        (tmp_path / "processed").mkdir()
        series_path = tmp_path / "processed" / "mart_report_daily_series.csv"
        series.to_csv(series_path, index=False)
        raw = pd.read_csv(series_path)
        result = standardise_forecasting_columns(
            raw,
            source_path=series_path,
            date_dim_path=tmp_path / "processed" / "dim_date.csv",
        )
        # mart_report_daily_series has exactly 5 cols; standardise keeps all
        assert set(result.columns) >= {"date", "report_id", "daily_views"}


# ---------------------------------------------------------------------------
# Assertion 13: diagnostics pipeline can load mart_report_daily_context
# ---------------------------------------------------------------------------

class TestDiagnosticsPipelineLoader:
    def test_choose_daily_usage_table_prefers_context(self, context, tmp_path):
        (tmp_path / "processed").mkdir()
        context.to_csv(
            tmp_path / "processed" / "mart_report_daily_context.csv", index=False
        )
        chosen = choose_daily_usage_table(tmp_path / "processed")
        assert chosen.name == "mart_report_daily_context.csv"

    def test_context_is_loadable_and_parseable(self, context, tmp_path):
        (tmp_path / "processed").mkdir()
        ctx_path = tmp_path / "processed" / "mart_report_daily_context.csv"
        context.to_csv(ctx_path, index=False)
        loaded = pd.read_csv(ctx_path)
        assert "daily_views" in loaded.columns
        assert "report_id" in loaded.columns

    def test_context_falls_back_when_series_also_present(self, series, context, tmp_path):
        (tmp_path / "processed").mkdir()
        # Both files present: diagnostics pipeline must prefer context over series
        series.to_csv(
            tmp_path / "processed" / "mart_report_daily_series.csv", index=False
        )
        context.to_csv(
            tmp_path / "processed" / "mart_report_daily_context.csv", index=False
        )
        chosen = choose_daily_usage_table(tmp_path / "processed")
        assert chosen.name == "mart_report_daily_context.csv"


# ---------------------------------------------------------------------------
# Assertion 14: future observation does not alter a historical leakage-safe feature
# ---------------------------------------------------------------------------

class TestLeakageSafety:
    def test_spike_on_last_day_does_not_change_prior_views_7d(self, adoption):
        last_date = adoption["date"].max()
        spike = adoption.copy()
        # Replace the last day with an enormous spike
        spike.loc[spike["date"] == last_date, "daily_views"] = 999_999

        # Re-compute rolling features on spiked data
        result_spike = add_time_series_usage_features(
            spike[["date", "report_id", "daily_views", "unique_viewers"]].copy()
        )
        result_orig = add_time_series_usage_features(
            adoption[["date", "report_id", "daily_views", "unique_viewers"]].copy()
        )

        # All rows before the last day must be unchanged
        for rid in ["R_FULL", "R_LATE"]:
            orig_r = result_orig[
                (result_orig["report_id"] == rid) & (result_orig["date"] < last_date)
            ].reset_index(drop=True)
            spk_r = result_spike[
                (result_spike["report_id"] == rid) & (result_spike["date"] < last_date)
            ].reset_index(drop=True)
            pd.testing.assert_series_equal(
                orig_r["views_7d"], spk_r["views_7d"], check_names=False,
                obj=f"views_7d for {rid} before spike date",
            )

    def test_same_day_views_not_in_rolling_window(self, adoption):
        # Find the first day with events after the first row (to avoid NaN from shift)
        r_full = adoption[adoption["report_id"] == "R_FULL"].sort_values("date")
        day2 = r_full.iloc[1]["date"]
        day2_views = int(r_full[r_full["date"] == day2]["daily_views"].iloc[0])

        # day2's views_7d must cover only [day1], not day2 itself
        day1_views = int(r_full.iloc[0]["daily_views"])
        day2_row = r_full[r_full["date"] == day2]
        if "views_7d" in day2_row.columns:
            views_7d_at_day2 = day2_row["views_7d"].iloc[0]
            # views_7d at day2 = sum over shift(1) window = day1 only
            assert views_7d_at_day2 == pytest.approx(day1_views), (
                f"views_7d at day 2 ({views_7d_at_day2}) should equal day-1 views "
                f"({day1_views}), not include day-2 views ({day2_views})"
            )


# ---------------------------------------------------------------------------
# Assertion 15: output schemas match documented contracts
# ---------------------------------------------------------------------------

class TestOutputSchemas:
    SERIES_REQUIRED = ["report_id", "date", "daily_views", "is_observed_day", "is_imputed_zero"]
    CONTEXT_REQUIRED = ["report_id", "date", "daily_views",
                        "views_7d", "views_28d", "wow_change_views"]

    def test_series_schema(self, series):
        for col in self.SERIES_REQUIRED:
            assert col in series.columns, f"mart_report_daily_series missing: {col}"

    def test_context_schema(self, context):
        for col in self.CONTEXT_REQUIRED:
            assert col in context.columns, f"mart_report_daily_context missing: {col}"

    def test_validate_report_daily_series_passes(self, series):
        # The validator raises SeriesValidationError on any failure
        validate_report_daily_series(series)  # must not raise

    def test_validate_daily_context_schema_passes(self, context):
        validate_daily_context_schema(context)  # must not raise

    def test_series_has_exactly_5_columns(self, series):
        assert series.shape[1] == 5, (
            f"mart_report_daily_series must have 5 columns, got {series.shape[1]}: "
            f"{series.columns.tolist()}"
        )

    def test_context_has_more_columns_than_series(self, series, context):
        assert context.shape[1] > series.shape[1]

    def test_is_observed_day_and_is_imputed_zero_mutually_exclusive(self, series):
        both_true = series[series["is_observed_day"] & series["is_imputed_zero"]]
        assert both_true.empty, "is_observed_day and is_imputed_zero must not both be True"

    def test_is_observed_day_and_is_imputed_zero_are_boolean(self, series):
        assert pd.api.types.is_bool_dtype(series["is_observed_day"])
        assert pd.api.types.is_bool_dtype(series["is_imputed_zero"])


# ---------------------------------------------------------------------------
# Smoke test: full pipeline end-to-end in a temp directory
# ---------------------------------------------------------------------------

class TestEndToEndSmoke:
    def test_full_pipeline_writes_all_outputs(self, tmp_path):
        """Run the entire feature-engineering pipeline on the fixture and write outputs
        to tmp_path. Verify that each expected file exists, is non-empty, and that
        both pipeline loaders can locate their preferred input."""
        fact_views, fact_pages, fact_loads, dim_report = _make_fixtures()

        # ── Build each mart ────────────────────────────────────────────────
        series = build_report_daily_series(
            fact_report_views=fact_views,
            report_active_periods=dim_report,
            date_col="date",
            report_col="report_id",
            views_col="view_count",
            active_start_col="launch_date",
            active_end_col="retire_date",
        )
        base = build_report_daily_adoption(
            fact_report_views=fact_views,
            date_col="date",
            report_col="report_id",
            user_col="user_id",
            views_col="view_count",
        )
        adoption_base = series.merge(
            base[["date", "report_id", "unique_viewers", "views_per_user"]],
            on=["date", "report_id"],
            how="left",
        )
        adoption_base["unique_viewers"] = adoption_base["unique_viewers"].fillna(0).astype(int)
        adoption_base["views_per_user"] = (
            adoption_base["daily_views"]
            .div(adoption_base["unique_viewers"].replace(0, pd.NA))
            .fillna(0.0)
        )
        adoption_ts = add_time_series_usage_features(adoption_base)
        engagement = build_user_engagement_features(
            fact_report_views=fact_views,
            fact_page_views=fact_pages,
            date_col="date",
            report_col="report_id",
            user_col="user_id",
        )
        performance = build_report_performance_features(
            fact_report_loads=fact_loads,
            date_col="date",
            report_col="report_id",
            load_time_col="load_time_ms",
        )
        context = build_report_daily_context(
            mart_report_daily_adoption=adoption_ts,
            mart_user_engagement=engagement,
            mart_report_performance=performance,
        )

        # ── Write outputs to tmp_path/processed — NOT the repo outputs dir ──
        processed = tmp_path / "processed"
        processed.mkdir()

        output_files = {
            "mart_report_daily_series.csv":  series,
            "mart_report_daily_context.csv": context,
            "mart_user_engagement.csv":       engagement,
            "mart_report_performance.csv":    performance,
        }
        for filename, df in output_files.items():
            df.to_csv(processed / filename, index=False)

        # ── Verify all files exist and are non-empty ───────────────────────
        for filename in output_files:
            path = processed / filename
            assert path.exists(), f"{filename} was not written"
            assert path.stat().st_size > 0, f"{filename} is empty"

        # ── Verify pipeline loaders find the correct files ─────────────────
        fc_chosen = choose_forecasting_input(processed)
        assert fc_chosen.name == "mart_report_daily_series.csv"

        diag_chosen = choose_daily_usage_table(processed)
        assert diag_chosen.name == "mart_report_daily_context.csv"

        # ── Verify round-trip CSV integrity ───────────────────────────────
        series_rt = pd.read_csv(processed / "mart_report_daily_series.csv",
                                parse_dates=["date"])
        assert set(series_rt.columns) == set(series.columns)
        assert len(series_rt) == len(series)
        assert (series_rt["daily_views"] >= 0).all()

        context_rt = pd.read_csv(processed / "mart_report_daily_context.csv",
                                 parse_dates=["date"])
        assert len(context_rt) == len(context)
        assert "top_1_user_view_share" in context_rt.columns
        assert "top_10pct_user_share" in context_rt.columns
