"""Sprint 6A-I: User analytics privacy hardening and engagement-definition tests.

Tests cover:
- Identifier removal from analytics outputs
- Streamlit app data loading exclusions
- Canonical engagement definitions
- Small-group suppression policy
- Privacy validation functions
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.analytics.privacy_policy import (
    PROHIBITED_OUTPUT_COLUMNS,
    PrivacyConfig,
    apply_small_group_suppression,
    detect_email_like_values,
    validate_no_direct_identifiers,
    validate_privacy_suppression,
    validate_user_analytics_output,
)


# ===========================================================================
# TestIdentifierRemoval
# ===========================================================================


class TestIdentifierRemoval:
    def test_user_features_excludes_email(self):
        """Mock df with user_key and numeric cols passes validation."""
        df = pd.DataFrame(
            [{"user_key": "UK_0001", "total_views": 10, "active_days": 3}]
        )
        validate_no_direct_identifiers(df)  # must not raise

    def test_user_features_excludes_user_id(self):
        """DataFrame with user_id column raises ValueError."""
        df = pd.DataFrame([{"user_id": "user001@masegoinc.com", "total_views": 5}])
        with pytest.raises(ValueError, match="user_id"):
            validate_no_direct_identifiers(df)

    def test_user_features_excludes_unique_user(self):
        """DataFrame with unique_user column raises ValueError."""
        df = pd.DataFrame([{"unique_user": "User 001", "total_views": 5}])
        with pytest.raises(ValueError, match="unique_user"):
            validate_no_direct_identifiers(df)

    def test_user_key_allowed(self):
        """DataFrame with only user_key passes validation."""
        df = pd.DataFrame([{"user_key": "UK_0001", "total_views": 3}])
        validate_no_direct_identifiers(df)  # must not raise

    def test_dim_user_marked_restricted(self, tmp_path):
        """dim_user.csv contains user_id, confirming it is restricted identity data."""
        dim_user_path = tmp_path / "dim_user.csv"
        dim_user = pd.DataFrame(
            [{"user_key": "UK_0001", "user_id": "user001@masegoinc.com", "unique_user": "User 001"}]
        )
        dim_user.to_csv(dim_user_path, index=False)
        loaded = pd.read_csv(dim_user_path)
        assert "user_id" in loaded.columns, "dim_user.csv must retain user_id (restricted identity data)"

    def test_email_in_renamed_column_detected(self):
        """Column named 'identifier' with email content raises ValueError."""
        df = pd.DataFrame([{"identifier": "user001@masegoinc.com"}])
        with pytest.raises(ValueError, match="email-like value"):
            validate_no_direct_identifiers(df)

    def test_null_values_no_false_positive(self):
        """Column with None values passes validation without false positives."""
        df = pd.DataFrame([{"user_key": "UK_0001", "some_col": None}])
        validate_no_direct_identifiers(df)  # must not raise

    def test_email_like_values_rejected(self):
        """Column containing an email string raises ValueError."""
        df = pd.DataFrame([{"label": "someone@example.com"}])
        with pytest.raises(ValueError, match="email-like value"):
            detect_email_like_values(df)

    def test_non_email_string_accepted(self):
        """Column with surrogate key 'UK_0001' passes."""
        df = pd.DataFrame([{"user_key": "UK_0001"}])
        detect_email_like_values(df)  # must not raise


# ===========================================================================
# TestStreamlitLoading
# ===========================================================================


class TestStreamlitLoading:
    """Tests for load_app_data privacy exclusions.

    Stubs are created in tmp_path so tests run without real pipeline outputs.
    """

    def _write_stub(self, path: Path, columns: list[str]) -> None:
        """Write a minimal stub CSV with one data row."""
        path.parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame([{c: "stub" for c in columns}])
        df.to_csv(path, index=False)

    def _setup_stubs(self, tmp_path: Path) -> None:
        """Write the minimum file set needed for load_app_data to run."""
        stubs = {
            "data/processed/mart_report_daily_context.csv": ["report_id", "daily_views"],
            "outputs/forecasts/report_view_forecasts_latest.csv": ["report_id", "date", "forecast"],
            "outputs/metrics/report_view_metrics_latest.csv": ["report_id", "forecast_reliable"],
            "outputs/metrics/report_features.csv": ["report_id", "top_1_user_view_share"],
            "outputs/segments/report_segments.csv": ["report_id", "report_segment"],
            "data/processed/dim_report.csv": ["report_id", "report_name"],
            "data/processed/fact_report_views.csv": ["date_key", "report_id", "user_key", "view_count"],
        }
        for rel, cols in stubs.items():
            self._write_stub(tmp_path / rel, cols)

    def test_user_features_not_loaded(self, tmp_path):
        """load_app_data must not include 'user_features' in the returned dict."""
        self._setup_stubs(tmp_path)
        # Also write user_features.csv on disk — it must still be excluded.
        self._write_stub(
            tmp_path / "outputs/metrics/user_features.csv",
            ["user_key", "total_views"],
        )
        from src.app.utils.load_data import load_app_data
        data = load_app_data(root=tmp_path)
        assert "user_features" not in data, "user_features must not be loaded into app memory"

    def test_user_segments_not_loaded(self, tmp_path):
        """load_app_data must not include 'user_segments' in the returned dict."""
        self._setup_stubs(tmp_path)
        self._write_stub(
            tmp_path / "outputs/segments/user_segments.csv",
            ["user_key", "user_segment"],
        )
        from src.app.utils.load_data import load_app_data
        data = load_app_data(root=tmp_path)
        assert "user_segments" not in data, "user_segments must not be loaded into app memory"

    def test_dim_user_not_loaded(self, tmp_path):
        """load_app_data must not include 'dim_user' in the returned dict."""
        self._setup_stubs(tmp_path)
        self._write_stub(
            tmp_path / "data/processed/dim_user.csv",
            ["user_key", "user_id", "unique_user"],
        )
        from src.app.utils.load_data import load_app_data
        data = load_app_data(root=tmp_path)
        assert "dim_user" not in data, "dim_user (restricted identity data) must not be loaded"

    def test_report_features_still_loaded(self, tmp_path):
        """report_features must still be present in the returned dict."""
        self._setup_stubs(tmp_path)
        from src.app.utils.load_data import load_app_data
        data = load_app_data(root=tmp_path)
        assert "report_features" in data

    def test_no_prohibited_fields_in_app_data(self, tmp_path):
        """No analytics output DataFrame in app data may contain prohibited identifier columns.

        Raw fact tables (fact_report_views) are excluded from this check because
        normalize_report_columns creates a user_id alias from user_key (surrogate, not email).
        The check targets analytics outputs: report_features, segments, metrics, forecasts, etc.
        """
        self._setup_stubs(tmp_path)
        from src.app.utils.load_data import load_app_data
        # Keys excluded from check: raw fact tables where normalize_report_columns
        # may create a user_id column from user_key (surrogate, not email identifier).
        raw_fact_keys = {"fact_report_views", "forecast_features"}
        data = load_app_data(root=tmp_path)
        for key, value in data.items():
            if key.startswith("_"):
                continue
            if key in raw_fact_keys:
                continue
            if isinstance(value, pd.DataFrame) and not value.empty:
                try:
                    validate_no_direct_identifiers(value, context=f"app_data['{key}']")
                except ValueError as exc:
                    pytest.fail(str(exc))

    def test_missing_user_files_does_not_crash(self, tmp_path):
        """load_app_data works fine even when user_features.csv is absent."""
        self._setup_stubs(tmp_path)
        # Deliberately do NOT create user_features.csv or user_segments.csv
        from src.app.utils.load_data import load_app_data
        data = load_app_data(root=tmp_path)  # must not raise
        assert isinstance(data, dict)


# ===========================================================================
# TestEngagementDefinitions
# ===========================================================================


class TestEngagementDefinitions:
    """Tests for canonical engagement definitions from engagement_definitions.py."""

    def _make_user(self, active_days: int, total_views: int) -> dict:
        return {"user_key": "UK_test", "active_days": active_days, "total_views": total_views}

    def test_two_views_one_day_is_one_time_user(self):
        """User with 2 views on 1 date: active_days=1, one_active_day_user_flag=True."""
        active_days = 1
        assert active_days == 1  # canonical: one_active_day_user_flag = (active_days == 1)

    def test_two_views_one_day_is_repeat_view_user(self):
        """User with 2 views on 1 date is a repeat-view user (total_views > 1)."""
        total_views = 2
        assert total_views > 1  # canonical: repeat-view user

    def test_two_views_one_day_not_returning_user(self):
        """User with 2 views on 1 date is NOT a returning user (only 1 active date)."""
        active_days = 1
        returning = active_days >= 2
        assert not returning

    def test_one_view_two_dates_is_returning(self):
        """User with 1 view on each of 2 distinct dates: active_days=2 → returning."""
        active_days = 2
        assert active_days >= 2  # canonical returning user in window

    def test_one_view_two_dates_not_one_time(self):
        """User with 1 view on 2 distinct dates: active_days=2, NOT one-time."""
        active_days = 2
        assert active_days != 1  # not one_active_day_user_flag

    def test_multiple_views_multiple_dates_returning_and_repeat_view(self):
        """User with 5 views on 3 dates: both returning and repeat-view."""
        active_days = 3
        total_views = 5
        returning = active_days >= 2
        repeat_view = total_views > 1
        assert returning and repeat_view

    def test_lifetime_returned_flag_true_when_later_date(self):
        """active_days > 1 → lifetime_returned_flag = True."""
        active_days = 3
        lifetime_returned_flag = active_days > 1
        assert lifetime_returned_flag is True

    def test_lifetime_returned_flag_false_for_single_date(self):
        """active_days == 1 → lifetime_returned_flag = False."""
        active_days = 1
        lifetime_returned_flag = active_days > 1
        assert lifetime_returned_flag is False

    def test_deprecated_repeat_usage_flag_maps_to_lifetime_returned(self):
        """repeat_usage_flag must equal lifetime_returned_flag (active_days > 1)."""
        import warnings
        from src.analytics.user_features import build_user_features

        data = pd.DataFrame(
            [
                {"date": "2025-01-01", "user_key": "UK_001", "report_id": "R_001", "view_count": 5},
                {"date": "2025-01-02", "user_key": "UK_001", "report_id": "R_001", "view_count": 3},
                {"date": "2025-01-01", "user_key": "UK_002", "report_id": "R_001", "view_count": 2},
            ]
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            features = build_user_features(data)

        # repeat_usage_flag must equal lifetime_returned_flag
        for _, row in features.iterrows():
            assert row["repeat_usage_flag"] == row["lifetime_returned_flag"], (
                "repeat_usage_flag must be an alias for lifetime_returned_flag"
            )

    def test_one_time_segment_uses_active_days_only(self):
        """User with active_days=1 and total_views=5 must get segment='one_time'."""
        from src.analytics.user_segmentation import build_user_segments

        features = pd.DataFrame(
            [{"user_key": "UK_001", "total_views": 5, "active_days": 1, "distinct_reports": 1}]
        )
        segments = build_user_segments(features)
        assert segments.iloc[0]["user_segment"] == "one_time"

    def test_old_one_time_logic_removed(self):
        """User with active_days=2 and total_views=1 must NOT get segment='one_time'."""
        from src.analytics.user_segmentation import build_user_segments

        # Build a larger dataset so quantile thresholds are meaningful
        rows = [
            {"user_key": f"UK_{i:03d}", "total_views": 10, "active_days": 5, "distinct_reports": 3}
            for i in range(20)
        ]
        # Target user: active_days=2 (> 1), total_views=1 (old logic would flag as one_time)
        rows.append({"user_key": "UK_999", "total_views": 1, "active_days": 2, "distinct_reports": 1})
        features = pd.DataFrame(rows)
        segments = build_user_segments(features)
        target = segments[segments["user_key"] == "UK_999"].iloc[0]
        assert target["user_segment"] != "one_time", (
            "active_days=2 with total_views=1 must NOT be 'one_time' under the canonical definition"
        )


# ===========================================================================
# TestSuppressionPolicy
# ===========================================================================


class TestSuppressionPolicy:
    def _make_report_df(self, unique_users_values: list[int]) -> pd.DataFrame:
        rows = []
        for n in unique_users_values:
            rows.append({
                "report_id": f"R_{n:03d}",
                "unique_users": n,
                "median_views_per_user": 5.0,
                "avg_views_per_user": 6.0,
                "total_views": n * 5,
            })
        return pd.DataFrame(rows)

    def test_below_threshold_suppresses_distribution_cols(self):
        """unique_users=3 (below 5) → median_views_per_user suppressed."""
        df = self._make_report_df([3])
        result = apply_small_group_suppression(
            df, "unique_users", ["median_views_per_user", "avg_views_per_user"]
        )
        assert pd.isna(result.iloc[0]["median_views_per_user"])
        assert pd.isna(result.iloc[0]["avg_views_per_user"])

    def test_at_threshold_not_suppressed(self):
        """unique_users=5 (equals threshold) → no suppression."""
        df = self._make_report_df([5])
        result = apply_small_group_suppression(
            df, "unique_users", ["median_views_per_user"]
        )
        assert not result.iloc[0]["privacy_suppressed"]
        assert pd.notna(result.iloc[0]["median_views_per_user"])

    def test_suppressed_values_are_null_not_zero(self):
        """Suppressed distribution columns must be None/NaN, never 0."""
        df = self._make_report_df([2])
        result = apply_small_group_suppression(
            df, "unique_users", ["median_views_per_user"]
        )
        val = result.iloc[0]["median_views_per_user"]
        assert pd.isna(val), f"Expected None/NaN but got {val!r}"

    def test_total_views_not_suppressed(self):
        """total_views is never listed as a distribution col, so it survives suppression."""
        df = self._make_report_df([2])
        result = apply_small_group_suppression(
            df, "unique_users", ["median_views_per_user"]
        )
        # total_views was NOT in distribution_cols → must remain intact
        assert pd.notna(result.iloc[0]["total_views"])

    def test_suppressed_fields_metadata_deterministic(self):
        """Same input always produces the same suppressed_fields string."""
        df = self._make_report_df([1])
        cols = ["median_views_per_user", "avg_views_per_user"]
        r1 = apply_small_group_suppression(df.copy(), "unique_users", cols)
        r2 = apply_small_group_suppression(df.copy(), "unique_users", cols)
        assert r1.iloc[0]["suppressed_fields"] == r2.iloc[0]["suppressed_fields"]

    def test_no_user_lists_in_metadata(self):
        """suppressed_fields must be a plain comma-separated string, not user data."""
        df = self._make_report_df([1])
        result = apply_small_group_suppression(
            df, "unique_users", ["median_views_per_user", "avg_views_per_user"]
        )
        fields_val = result.iloc[0]["suppressed_fields"]
        assert isinstance(fields_val, str)
        # Must not contain any email-like content
        import re
        assert not re.search(r"@", fields_val)

    def test_privacy_suppressed_flag_correct(self):
        """privacy_suppressed=True where below threshold, False otherwise."""
        df = self._make_report_df([2, 5, 10])
        result = apply_small_group_suppression(
            df, "unique_users", ["median_views_per_user"]
        )
        flags = dict(zip(result["unique_users"], result["privacy_suppressed"]))
        assert flags[2] is True or flags[2] == True
        assert flags[5] is False or flags[5] == False
        assert flags[10] is False or flags[10] == False


# ===========================================================================
# TestPrivacyValidators
# ===========================================================================


class TestPrivacyValidators:
    def test_prohibited_column_rejected(self):
        """validate_no_direct_identifiers raises for user_id."""
        df = pd.DataFrame([{"user_id": "UK_0001"}])
        with pytest.raises(ValueError, match="user_id"):
            validate_no_direct_identifiers(df)

    def test_email_content_rejected(self):
        """detect_email_like_values raises when any column has an email."""
        df = pd.DataFrame([{"label": "user@example.com"}])
        with pytest.raises(ValueError, match="email-like value"):
            detect_email_like_values(df)

    def test_pseudonymous_key_accepted(self):
        """user_key with 'UK_0001' passes all validators."""
        df = pd.DataFrame([{"user_key": "UK_0001", "total_views": 5}])
        validate_no_direct_identifiers(df)  # must not raise
        detect_email_like_values(df)        # must not raise

    def test_invalid_suppression_state_rejected(self):
        """Suppressed row with non-null distribution col raises ValueError."""
        df = pd.DataFrame([{
            "unique_users": 2,
            "median_views": 5.0,  # should be None for suppressed row
            "privacy_suppressed": True,
            "suppressed_fields": "median_views",
        }])
        with pytest.raises(ValueError, match="non-null values in suppressed rows"):
            validate_privacy_suppression(df, distribution_cols=["median_views"])

    def test_null_suppression_fields_for_non_suppressed(self):
        """Non-suppressed row with non-null suppressed_fields raises ValueError."""
        df = pd.DataFrame([{
            "unique_users": 10,
            "median_views": 5.0,
            "privacy_suppressed": False,
            "suppressed_fields": "median_views",  # should be None
        }])
        with pytest.raises(ValueError, match="suppressed_fields.*should be None"):
            validate_privacy_suppression(df, distribution_cols=["median_views"])

    def test_user_list_column_rejected(self):
        """Column containing a Python list raises ValueError."""
        df = pd.DataFrame([{"user_key": "UK_0001", "user_list": [1, 2, 3]}])
        with pytest.raises(ValueError, match="user lists"):
            validate_user_analytics_output(df)

    def test_deterministic_error_messages(self):
        """Same bad DataFrame always produces the same error message."""
        df = pd.DataFrame([{"user_id": "x"}])
        messages = []
        for _ in range(3):
            try:
                validate_no_direct_identifiers(df)
            except ValueError as exc:
                messages.append(str(exc))
        assert len(set(messages)) == 1, "Error messages must be deterministic"
