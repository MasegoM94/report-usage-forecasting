"""Tests for temporal leakage in rolling and lagged feature computations.

A feature computed for row t must depend only on observations from dates
strictly before t. These tests verify that invariant by:

1. Injecting a large spike at a future date and asserting that all past
   feature values are unchanged.
2. Checking that the rolling sum at date t does NOT include the same-day
   daily_views value.
3. Checking that the feature registry correctly classifies roles.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.features.feature_registry import (
    FEATURE_REGISTRY,
    get_diagnostic_features,
    get_predictor_features,
)
from src.features.report_features import add_time_series_usage_features


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ROLLING_COLS = ["views_7d", "views_28d", "viewers_7d", "viewers_28d", "wow_change_views"]


def _make_adoption(
    report_id: str = "R_001",
    start: str = "2025-01-01",
    n_days: int = 30,
    daily_views: list[int] | None = None,
    unique_viewers: list[int] | None = None,
) -> pd.DataFrame:
    """Build a minimal daily adoption mart for one report."""
    dates = pd.date_range(start, periods=n_days, freq="D")
    v = daily_views if daily_views is not None else [5] * n_days
    u = unique_viewers if unique_viewers is not None else [2] * n_days
    return pd.DataFrame({
        "date": dates,
        "report_id": report_id,
        "daily_views": v,
        "unique_viewers": u,
    })


def _run(df: pd.DataFrame) -> pd.DataFrame:
    return add_time_series_usage_features(df)


# ---------------------------------------------------------------------------
# 1. Future mutation does not change historical feature values
# ---------------------------------------------------------------------------

class TestFutureMutationIsolation:
    """Changing a future observation must never alter a past feature value."""

    def test_mutating_last_row_does_not_affect_prior_views_7d(self):
        df = _make_adoption(n_days=14)
        result_orig = _run(df)

        df_mut = df.copy()
        last_date = df_mut["date"].max()
        df_mut.loc[df_mut["date"] == last_date, "daily_views"] = 99_999
        result_mut = _run(df_mut)

        # All rows except the last must be identical
        mask = result_orig["date"] < last_date
        pd.testing.assert_series_equal(
            result_orig.loc[mask, "views_7d"].reset_index(drop=True),
            result_mut.loc[mask, "views_7d"].reset_index(drop=True),
            check_names=False,
        )

    def test_mutating_last_row_does_not_affect_prior_views_28d(self):
        df = _make_adoption(n_days=30)
        result_orig = _run(df)

        df_mut = df.copy()
        last_date = df_mut["date"].max()
        df_mut.loc[df_mut["date"] == last_date, "daily_views"] = 99_999
        result_mut = _run(df_mut)

        mask = result_orig["date"] < last_date
        pd.testing.assert_series_equal(
            result_orig.loc[mask, "views_28d"].reset_index(drop=True),
            result_mut.loc[mask, "views_28d"].reset_index(drop=True),
            check_names=False,
        )

    def test_mutating_middle_row_does_not_affect_earlier_rows(self):
        """Mutating row 10 must not change rows 1–9."""
        df = _make_adoption(n_days=20)
        result_orig = _run(df)

        pivot_date = pd.Timestamp("2025-01-11")  # row 11 (0-indexed: 10)
        df_mut = df.copy()
        df_mut.loc[df_mut["date"] == pivot_date, "daily_views"] = 50_000
        result_mut = _run(df_mut)

        mask = result_orig["date"] < pivot_date
        for col in ["views_7d", "views_28d", "viewers_7d", "viewers_28d"]:
            pd.testing.assert_series_equal(
                result_orig.loc[mask, col].reset_index(drop=True),
                result_mut.loc[mask, col].reset_index(drop=True),
                check_names=False,
                obj=f"{col} (before pivot_date)",
            )

    def test_wow_change_views_unaffected_by_future_spike(self):
        df = _make_adoption(n_days=20)
        result_orig = _run(df)

        last_date = df["date"].max()
        df_mut = df.copy()
        df_mut.loc[df_mut["date"] == last_date, "daily_views"] = 99_999
        result_mut = _run(df_mut)

        mask = result_orig["date"] < last_date
        pd.testing.assert_series_equal(
            result_orig.loc[mask, "wow_change_views"].reset_index(drop=True),
            result_mut.loc[mask, "wow_change_views"].reset_index(drop=True),
            check_names=False,
        )


# ---------------------------------------------------------------------------
# 2. Same-day target not included in rolling window
# ---------------------------------------------------------------------------

class TestSameDayExclusion:
    """The feature value at row t must not include daily_views from day t."""

    def test_views_7d_excludes_same_day_spike(self):
        """A spike on day 5 must not appear in views_7d at day 5."""
        # Days 1–4: views=1; day 5: views=9999; days 6–10: views=1
        views = [1, 1, 1, 1, 9999, 1, 1, 1, 1, 1]
        df = _make_adoption(n_days=10, daily_views=views)
        result = _run(df)

        spike_date = pd.Timestamp("2025-01-05")
        views_7d_at_spike = result.loc[result["date"] == spike_date, "views_7d"].values[0]

        # With shift(1) the window at day 5 covers days 1–4 → sum = 4
        assert views_7d_at_spike == pytest.approx(4.0), (
            f"views_7d at spike date includes same-day: {views_7d_at_spike}"
        )

    def test_views_28d_excludes_same_day_spike(self):
        """A spike on day 30 must not appear in views_28d at day 30."""
        views = [1] * 29 + [9999]
        df = _make_adoption(n_days=30, daily_views=views)
        result = _run(df)

        spike_date = pd.Timestamp("2025-01-30")
        views_28d_at_spike = result.loc[result["date"] == spike_date, "views_28d"].values[0]

        # With shift(1) the 28-day window at day 30 covers days 2–29 → sum = 28
        assert views_28d_at_spike == pytest.approx(28.0), (
            f"views_28d at spike date includes same-day: {views_28d_at_spike}"
        )

    def test_wow_change_views_uses_lag_1_not_lag_0(self):
        """wow_change_views at day t should reflect views_{t-1}, not views_t."""
        # All days: views=10 except day 9 (index 8) = 99 and day 10 (index 9) = 1
        views = [10] * 8 + [99, 1]
        df = _make_adoption(n_days=10, daily_views=views)
        result = _run(df)

        day10 = pd.Timestamp("2025-01-10")
        wow_day10 = result.loc[result["date"] == day10, "wow_change_views"].values[0]

        # lag_1 at day 10 = daily_views[day 9] = 99
        # lag_8 at day 10 = daily_views[day 2] = 10
        expected_wow = (99 - 10) / 10  # = 8.9
        assert wow_day10 == pytest.approx(expected_wow, rel=1e-6)

    def test_first_row_is_nan(self):
        """The first row per report has no prior data; rolling features must be NaN."""
        df = _make_adoption(n_days=10)
        result = _run(df)
        first_row = result.loc[result["date"] == result["date"].min()]
        for col in ["views_7d", "views_28d", "viewers_7d", "viewers_28d"]:
            assert pd.isna(first_row[col].values[0]), (
                f"{col} should be NaN on first row, got {first_row[col].values[0]}"
            )


# ---------------------------------------------------------------------------
# 3. Multiple reports — leakage does not cross report boundaries
# ---------------------------------------------------------------------------

class TestMultiReportIsolation:
    def test_spike_in_one_report_does_not_affect_another(self):
        """A large spike in R_002 must not change R_001's feature values."""
        df_r1 = _make_adoption("R_001", n_days=14)
        df_r2 = _make_adoption("R_002", n_days=14)
        df = pd.concat([df_r1, df_r2], ignore_index=True)
        result_orig = _run(df)

        df_mut = df.copy()
        df_mut.loc[df_mut["report_id"] == "R_002", "daily_views"] = 99_999
        result_mut = _run(df_mut)

        r1_orig = result_orig.loc[result_orig["report_id"] == "R_001"].reset_index(drop=True)
        r1_mut = result_mut.loc[result_mut["report_id"] == "R_001"].reset_index(drop=True)

        for col in ["views_7d", "views_28d"]:
            pd.testing.assert_series_equal(
                r1_orig[col], r1_mut[col], check_names=False, obj=col
            )


# ---------------------------------------------------------------------------
# 4. Feature registry integrity
# ---------------------------------------------------------------------------

class TestFeatureRegistry:
    def test_target_not_in_predictor_list(self):
        """daily_views must not appear in get_predictor_features()."""
        assert "daily_views" not in get_predictor_features()

    def test_target_not_available_at_forecast_time(self):
        """The target (daily_views) must be marked available_at_forecast_time=False."""
        targets = [e for e in FEATURE_REGISTRY if e["feature_role"] == "target"]
        assert targets, "No target feature found in registry"
        for entry in targets:
            assert entry["available_at_forecast_time"] is False, (
                f"{entry['feature_name']} is a target but available_at_forecast_time=True"
            )

    def test_rolling_features_are_historical_predictors(self):
        """views_7d, views_28d, viewers_7d, viewers_28d, wow_change_views must be
        classified as historical predictors."""
        registry_by_name = {e["feature_name"]: e for e in FEATURE_REGISTRY}
        for col in ["views_7d", "views_28d", "viewers_7d", "viewers_28d", "wow_change_views"]:
            assert col in registry_by_name, f"{col} missing from registry"
            entry = registry_by_name[col]
            assert entry["feature_role"] == "historical predictor", (
                f"{col} should be 'historical predictor', got {entry['feature_role']!r}"
            )
            assert entry["available_at_forecast_time"] is True

    def test_same_day_engagement_features_are_diagnostic(self):
        """Engagement features computed from same-day actuals must be diagnostic-only."""
        registry_by_name = {e["feature_name"]: e for e in FEATURE_REGISTRY}
        for col in ["repeat_user_rate", "top_1_user_view_share", "top_10pct_user_share",
                    "avg_pages_per_user"]:
            assert col in registry_by_name, f"{col} missing from registry"
            entry = registry_by_name[col]
            assert entry["feature_role"] == "diagnostic-only", (
                f"{col} should be 'diagnostic-only', got {entry['feature_role']!r}"
            )
            assert entry["available_at_forecast_time"] is False

    def test_same_day_performance_features_are_diagnostic(self):
        """Load-time features must be diagnostic-only."""
        registry_by_name = {e["feature_name"]: e for e in FEATURE_REGISTRY}
        for col in ["avg_load_time", "p90_load_time", "avg_load_time_7d", "load_time_wow_change"]:
            assert col in registry_by_name, f"{col} missing from registry"
            entry = registry_by_name[col]
            assert entry["feature_role"] == "diagnostic-only"
            assert entry["available_at_forecast_time"] is False

    def test_calendar_features_known_in_advance(self):
        """Calendar features must be marked known-in-advance and available."""
        registry_by_name = {e["feature_name"]: e for e in FEATURE_REGISTRY}
        for col in ["day_of_week", "is_weekend", "is_month_end", "is_quarter_end"]:
            assert col in registry_by_name, f"{col} missing from registry"
            entry = registry_by_name[col]
            assert entry["feature_role"] == "known-in-advance predictor"
            assert entry["available_at_forecast_time"] is True

    def test_all_entries_have_required_keys(self):
        """Every registry entry must contain all required metadata fields."""
        required_keys = {
            "feature_name",
            "description",
            "grain",
            "calculation_window",
            "feature_role",
            "available_at_forecast_time",
            "used_by_forecasting_model",
            "null_policy",
        }
        for entry in FEATURE_REGISTRY:
            missing = required_keys - set(entry.keys())
            assert not missing, (
                f"Entry for '{entry.get('feature_name', '?')}' missing keys: {missing}"
            )

    def test_no_diagnostic_feature_used_by_model(self):
        """Diagnostic-only features must have used_by_forecasting_model=False."""
        for entry in FEATURE_REGISTRY:
            if entry["feature_role"] == "diagnostic-only":
                assert entry["used_by_forecasting_model"] is False, (
                    f"Diagnostic feature '{entry['feature_name']}' has "
                    f"used_by_forecasting_model=True"
                )

    def test_get_predictor_features_returns_list_of_strings(self):
        predictors = get_predictor_features()
        assert isinstance(predictors, list)
        assert all(isinstance(p, str) for p in predictors)

    def test_get_diagnostic_features_returns_list_of_strings(self):
        diagnostics = get_diagnostic_features()
        assert isinstance(diagnostics, list)
        assert all(isinstance(d, str) for d in diagnostics)
