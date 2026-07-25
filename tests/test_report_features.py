"""Tests for src.analytics.report_features — schema v2.0.0."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from datetime import date, timedelta

from src.analytics.report_features import (
    ReportFeaturesConfig,
    build_report_features,
    validate_report_features,
    persist_report_features,
    REPORT_FEATURES_COLS,
    REPORT_FEATURES_SCHEMA_VERSION,
)
from src.analytics.privacy_policy import PROHIBITED_OUTPUT_COLUMNS

AS_OF = date(2024, 3, 31)
RUN_ID = "test-rf-001"


def _make_daily_views(report_id, dates_views):
    rows = [{"report_id": report_id, "usage_date": d, "daily_views": v} for d, v in dates_views]
    return pd.DataFrame(rows)


def _make_dim(report_id="R001", name="Test Report", launch_date=None):
    return pd.DataFrame([{
        "report_id": report_id,
        "report_name": name,
        "launch_date": launch_date,
        "workspace_id": "WS001",
    }])


def _build(views_df, dim_df, as_of=AS_OF, run_id=RUN_ID, cfg=None):
    return build_report_features(views_df, dim_df, as_of, run_id, cfg or ReportFeaturesConfig())


class TestAsOfDate:
    def test_as_of_date_in_output(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 5) for i in range(30)])
        df = _build(views, _make_dim("R1"))
        assert df["analytics_as_of_date"].iloc[0] == str(AS_OF)

    def test_generated_at_is_separate(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 5) for i in range(30)])
        df = _build(views, _make_dim("R1"))
        gen = pd.to_datetime(df["generated_at"].iloc[0])
        as_of = pd.to_datetime(df["analytics_as_of_date"].iloc[0])
        assert gen >= as_of

    def test_no_future_dates_in_metrics(self):
        today = date.today()
        views = _make_daily_views("R1", [(today + timedelta(days=5), 10)])
        df = _build(views, _make_dim("R1"), as_of=date(2024, 3, 31))
        assert df["recent_28d_views"].iloc[0] == 0


class TestWindows:
    def test_7d_window_exact(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=6), 10)])
        assert _build(views, _make_dim("R1"))["recent_7d_views"].iloc[0] == 10

    def test_7d_window_excludes_day_7(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=7), 10)])
        assert _build(views, _make_dim("R1"))["recent_7d_views"].iloc[0] == 0

    def test_28d_window_exact(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=27), 5)])
        assert _build(views, _make_dim("R1"))["recent_28d_views"].iloc[0] == 5

    def test_previous_28d_window(self):
        # Need 56 days of calendar coverage (comparison_history_sufficient_28d)
        # Pad with zeros; set the boundary day to have 7 views
        zeros = [(AS_OF - timedelta(days=i), 0) for i in range(56)]
        zeros[28] = (AS_OF - timedelta(days=28), 7)
        views = _make_daily_views("R1", zeros)
        assert _build(views, _make_dim("R1"))["previous_28d_views"].iloc[0] == 7

    def test_windows_do_not_overlap(self):
        # Need 56 calendar days so comparison window is sufficient
        zeros = [(AS_OF - timedelta(days=i), 0) for i in range(56)]
        zeros[28] = (AS_OF - timedelta(days=28), 5)
        views = _make_daily_views("R1", zeros)
        df = _build(views, _make_dim("R1"))
        assert df["recent_28d_views"].iloc[0] == 0
        assert df["previous_28d_views"].iloc[0] == 5

    def test_90d_window(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=89), 3)])
        assert _build(views, _make_dim("R1"))["recent_90d_views"].iloc[0] == 3

    def test_month_boundary(self):
        as_of = date(2024, 3, 1)
        # as_of - 28d = Feb 2; need 56 calendar days of coverage
        zeros = [(as_of - timedelta(days=i), 0) for i in range(56)]
        zeros[28] = (date(2024, 2, 2), 10)  # day -28 from Mar 1
        views = _make_daily_views("R1", zeros)
        df = _build(views, _make_dim("R1"), as_of=as_of)
        assert df["previous_28d_views"].iloc[0] == 10

    def test_leap_day(self):
        as_of = date(2024, 3, 1)
        views = _make_daily_views("R1", [(date(2024, 2, 29), 5)])
        df = _build(views, _make_dim("R1"), as_of=as_of)
        assert df["recent_7d_views"].iloc[0] == 5


class TestLifecycle:
    def test_known_activation_date(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=10), 5)])
        dim = _make_dim("R1", launch_date=(AS_OF - timedelta(days=60)).isoformat())
        df = _build(views, dim)
        assert df["activation_date_status"].iloc[0] == "known"
        assert df["report_age_days"].iloc[0] == 60

    def test_missing_activation_fallback(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=10), 5)])
        df = _build(views, _make_dim("R1", launch_date=None))
        assert df["activation_date_status"].iloc[0] in ("inferred_from_source_coverage", "unavailable")

    def test_newly_launched_status(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 3) for i in range(10)])
        dim = _make_dim("R1", launch_date=(AS_OF - timedelta(days=9)).isoformat())
        df = _build(views, dim)
        assert df["adoption_maturity_status"].iloc[0] == "newly_launched"

    def test_maturing_status(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 3) for i in range(20)])
        dim = _make_dim("R1", launch_date=(AS_OF - timedelta(days=19)).isoformat())
        df = _build(views, dim)
        assert df["adoption_maturity_status"].iloc[0] == "maturing"

    def test_mature_status(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 3) for i in range(40)])
        dim = _make_dim("R1", launch_date=(AS_OF - timedelta(days=39)).isoformat())
        df = _build(views, dim)
        assert df["adoption_maturity_status"].iloc[0] == "mature"

    def test_days_since_last_use_today(self):
        views = _make_daily_views("R1", [(AS_OF, 5)])
        assert _build(views, _make_dim("R1"))["days_since_last_use"].iloc[0] == 0

    def test_days_since_last_use_yesterday(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=1), 5)])
        assert _build(views, _make_dim("R1"))["days_since_last_use"].iloc[0] == 1

    def test_activation_after_as_of(self):
        dim = _make_dim("R1", launch_date=(AS_OF + timedelta(days=5)).isoformat())
        views = pd.DataFrame(columns=["report_id", "usage_date", "daily_views"])
        df = _build(views, dim)
        assert df["report_lifecycle_status"].iloc[0] == "pre_activation"


class TestUsageTotals:
    def test_lifetime_views(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), i + 1) for i in range(100)])
        df = _build(views, _make_dim("R1"))
        assert df["lifetime_views"].iloc[0] == sum(range(1, 101))

    def test_average_daily_views(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 10) for i in range(28)])
        df = _build(views, _make_dim("R1"))
        assert abs(df["average_daily_views_28d"].iloc[0] - 10.0) < 0.1

    def test_previous_window_null_when_insufficient(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 5) for i in range(10)])
        df = _build(views, _make_dim("R1"))
        assert pd.isna(df["previous_28d_views"].iloc[0])

    def test_both_windows_zero_not_null(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 0) for i in range(56)])
        df = _build(views, _make_dim("R1"))
        assert df["recent_28d_views"].iloc[0] == 0
        assert df["previous_28d_views"].iloc[0] == 0


class TestUsageDirection:
    def test_growing(self):
        prev = [(AS_OF - timedelta(days=28 + i), 5) for i in range(28)]
        rec = [(AS_OF - timedelta(days=i), 20) for i in range(28)]
        df = _build(_make_daily_views("R1", prev + rec), _make_dim("R1"))
        assert df["usage_direction_28d"].iloc[0] == "growing"

    def test_declining(self):
        prev = [(AS_OF - timedelta(days=28 + i), 20) for i in range(28)]
        rec = [(AS_OF - timedelta(days=i), 5) for i in range(28)]
        df = _build(_make_daily_views("R1", prev + rec), _make_dim("R1"))
        assert df["usage_direction_28d"].iloc[0] == "declining"

    def test_stable(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 10) for i in range(56)])
        df = _build(views, _make_dim("R1"))
        assert df["usage_direction_28d"].iloc[0] == "stable"

    def test_inactive_direction(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 0) for i in range(56)])
        df = _build(views, _make_dim("R1"))
        assert df["usage_direction_28d"].iloc[0] == "inactive"


class TestTrend:
    def test_increasing_trend(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=89 - i), i * 2 + 1) for i in range(90)])
        df = _build(views, _make_dim("R1"))
        assert df["usage_trend_slope_90d"].iloc[0] > 0
        assert df["usage_trend_status"].iloc[0] in ("increasing", "strongly_increasing")

    def test_decreasing_trend(self):
        # oldest day (89d ago) has highest views (100), newest (today) has lowest (11)
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=89 - i), 100 - i) for i in range(90)])
        df = _build(views, _make_dim("R1"))
        assert df["usage_trend_slope_90d"].iloc[0] < 0
        assert df["usage_trend_status"].iloc[0] in ("decreasing", "strongly_decreasing")

    def test_stable_trend(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 10) for i in range(90)])
        df = _build(views, _make_dim("R1"))
        assert df["usage_trend_status"].iloc[0] == "stable"

    def test_inactive_trend(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 0) for i in range(90)])
        df = _build(views, _make_dim("R1"))
        assert df["usage_trend_status"].iloc[0] == "inactive"

    def test_insufficient_history_trend(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 5) for i in range(5)])
        df = _build(views, _make_dim("R1"))
        assert df["trend_evidence_status"].iloc[0] in ("insufficient", "partial")


class TestVolatility:
    def test_constant_low_volatility(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 10) for i in range(28)])
        df = _build(views, _make_dim("R1"))
        assert df["usage_cv_28d"].iloc[0] < 0.1
        assert df["usage_volatility_status"].iloc[0] == "low_volatility"

    def test_high_volatility(self):
        vals = [0] * 20 + [1000] * 8
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), vals[i]) for i in range(28)])
        df = _build(views, _make_dim("R1"))
        assert df["usage_volatility_status"].iloc[0] in ("high_volatility", "bursty")

    def test_zero_mean_cv_null(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 0) for i in range(28)])
        df = _build(views, _make_dim("R1"))
        assert pd.isna(df["usage_cv_28d"].iloc[0])

    def test_insufficient_history_null_volatility(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 10) for i in range(5)])
        df = _build(views, _make_dim("R1"))
        assert pd.isna(df["usage_cv_28d"].iloc[0])


class TestInactivity:
    def test_zero_streak_usage_today(self):
        views = _make_daily_views("R1", [(AS_OF, 5)])
        df = _build(views, _make_dim("R1"))
        assert df["current_zero_usage_streak_days"].iloc[0] == 0

    def test_one_day_zero_streak(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=1), 5)])
        df = _build(views, _make_dim("R1"))
        assert df["current_zero_usage_streak_days"].iloc[0] == 1

    def test_inactive_28d_with_complete_zero_window(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 0) for i in range(28)])
        dim = _make_dim("R1", launch_date=(AS_OF - timedelta(days=27)).isoformat())
        df = _build(views, dim)
        assert df["inactive_28d"].iloc[0] == True

    def test_no_valid_data_is_not_inactivity(self):
        views = pd.DataFrame(columns=["report_id", "usage_date", "daily_views"])
        df = _build(views, _make_dim("R1"))
        assert df["historical_usage_status"].iloc[0] == "no_valid_usage_data"


class TestPeaksAndAnomalies:
    def test_unique_peak(self):
        vals = [(AS_OF - timedelta(days=i), 10) for i in range(90)]
        vals[5] = (AS_OF - timedelta(days=5), 500)
        df = _build(_make_daily_views("R1", vals), _make_dim("R1"))
        assert df["peak_daily_views_90d"].iloc[0] == 500

    def test_tied_peak_earliest_date(self):
        base = [(AS_OF - timedelta(days=i), 5) for i in range(90)]
        base[80] = (AS_OF - timedelta(days=80), 100)
        base[10] = (AS_OF - timedelta(days=10), 100)
        df = _build(_make_daily_views("R1", base), _make_dim("R1"))
        peak_date = pd.to_datetime(df["peak_usage_date_90d"].iloc[0]).date()
        assert peak_date == AS_OF - timedelta(days=80)

    def test_anomaly_detected(self):
        base = [(AS_OF - timedelta(days=i), 10) for i in range(28)]
        base[3] = (AS_OF - timedelta(days=3), 500)
        df = _build(_make_daily_views("R1", base), _make_dim("R1"))
        assert df["usage_anomaly_count_28d"].iloc[0] >= 1

    def test_insufficient_history_anomaly(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 10) for i in range(5)])
        df = _build(views, _make_dim("R1"))
        assert df["anomaly_evidence_status"].iloc[0] in ("insufficient", "insufficient_history")


class TestHistoricalStatus:
    def test_growing_usage_status(self):
        prev = [(AS_OF - timedelta(days=28 + i), 5) for i in range(28)]
        rec = [(AS_OF - timedelta(days=i), 50) for i in range(28)]
        df = _build(_make_daily_views("R1", prev + rec), _make_dim("R1"))
        assert df["historical_usage_status"].iloc[0] == "growing_usage"

    def test_stable_regular_status(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 10) for i in range(90)])
        df = _build(views, _make_dim("R1"))
        assert df["historical_usage_status"].iloc[0] in (
            "stable_regular_usage", "stable_intermittent_usage"
        )

    def test_prolonged_inactivity_status(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 0) for i in range(90)])
        dim = _make_dim("R1", launch_date=(AS_OF - timedelta(days=89)).isoformat())
        df = _build(views, dim)
        assert df["historical_usage_status"].iloc[0] in (
            "prolonged_inactivity", "recently_inactive"
        )

    def test_no_valid_usage_data_status(self):
        views = pd.DataFrame(columns=["report_id", "usage_date", "daily_views"])
        df = _build(views, _make_dim("R1"))
        assert df["historical_usage_status"].iloc[0] == "no_valid_usage_data"

    def test_reasons_non_empty(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 10) for i in range(60)])
        df = _build(views, _make_dim("R1"))
        assert df["historical_usage_reasons"].iloc[0] is not None
        assert len(str(df["historical_usage_reasons"].iloc[0])) > 10


class TestSchema:
    def test_canonical_cols_all_present(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 5) for i in range(60)])
        df = _build(views, _make_dim("R1"))
        missing = [c for c in REPORT_FEATURES_COLS if c not in df.columns]
        assert not missing, f"Missing cols: {missing}"

    def test_deprecated_cols_absent(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 5) for i in range(60)])
        df = _build(views, _make_dim("R1"))
        deprecated = ["latest_views", "prior_views", "usage_change_pct", "top_user_concentration", "repeat_rate"]
        present = [c for c in deprecated if c in df.columns]
        assert not present, f"Deprecated cols still present: {present}"

    def test_no_user_identifier_columns(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 5) for i in range(60)])
        df = _build(views, _make_dim("R1"))
        bad = set(df.columns) & PROHIBITED_OUTPUT_COLUMNS
        assert not bad, f"Prohibited cols: {bad}"

    def test_schema_version_present(self):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 5) for i in range(60)])
        df = _build(views, _make_dim("R1"))
        assert df["schema_version"].iloc[0] == REPORT_FEATURES_SCHEMA_VERSION

    def test_validate_rejects_legacy_schema(self):
        df = pd.DataFrame([{"report_id": "R1", "latest_views": 100, "analytics_run_id": "x"}])
        with pytest.raises((ValueError, KeyError)):
            validate_report_features(df)

    def test_persistence_creates_file(self, tmp_path):
        views = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 5) for i in range(60)])
        df = _build(views, _make_dim("R1"))
        path = persist_report_features(df, tmp_path)
        assert path.exists()
        loaded = pd.read_csv(path)
        assert list(loaded.columns) == REPORT_FEATURES_COLS

    def test_multi_report_output(self):
        v1 = _make_daily_views("R1", [(AS_OF - timedelta(days=i), 5) for i in range(60)])
        v2 = _make_daily_views("R2", [(AS_OF - timedelta(days=i), 3) for i in range(60)])
        views = pd.concat([v1, v2], ignore_index=True)
        dim = pd.DataFrame([
            {"report_id": "R1", "report_name": "Report 1", "launch_date": None, "workspace_id": "WS1"},
            {"report_id": "R2", "report_name": "Report 2", "launch_date": None, "workspace_id": "WS1"},
        ])
        df = _build(views, dim)
        assert len(df) == 2
        assert set(df["report_id"]) == {"R1", "R2"}
