"""Tests for src.features.validate_series.validate_report_daily_series.

Each test targets one specific invariant so failures are easy to diagnose.
"""

import pandas as pd
import pytest

from src.features.validate_series import SeriesValidationError, validate_report_daily_series


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_series(
    report_ids: list[str] | None = None,
    start: str = "2025-01-01",
    n_days: int = 7,
    daily_views: list[int] | None = None,
) -> pd.DataFrame:
    """Return a structurally valid daily series for one or more reports.

    All rows satisfy every invariant by default — individual tests corrupt
    exactly the field they are testing.
    """
    if report_ids is None:
        report_ids = ["R_001"]

    frames = []
    for rid in report_ids:
        dates = pd.date_range(start, periods=n_days, freq="D")
        views = daily_views if daily_views is not None else ([10] * n_days)
        observed = [v > 0 for v in views]
        imputed = [not o for o in observed]
        frames.append(
            pd.DataFrame(
                {
                    "report_id": rid,
                    "date": dates,
                    "daily_views": views,
                    "is_observed_day": observed,
                    "is_imputed_zero": imputed,
                }
            )
        )

    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# 1. Normal continuous series — all checks must pass
# ---------------------------------------------------------------------------

class TestNormalSeries:
    def test_single_report_all_observed(self):
        df = _make_series(daily_views=[5, 3, 8, 2, 7, 1, 4])
        validate_report_daily_series(df)  # must not raise

    def test_multiple_reports(self):
        df = _make_series(report_ids=["R_001", "R_002", "R_003"])
        validate_report_daily_series(df)

    def test_expected_row_count_matches(self):
        df = _make_series(n_days=14)
        validate_report_daily_series(df, expected_row_count=14)

    def test_late_launch_report_valid(self):
        """Report that starts halfway through the portfolio calendar is valid."""
        df = _make_series(start="2025-07-07", n_days=10)
        validate_report_daily_series(df)


# ---------------------------------------------------------------------------
# 2. Missing day correctly filled with zero
# ---------------------------------------------------------------------------

class TestImputedZeroRows:
    def test_zero_days_accepted(self):
        """Rows where daily_views=0, is_observed_day=False, is_imputed_zero=True are valid."""
        views = [10, 0, 0, 5, 0, 3, 0]
        df = _make_series(daily_views=views)
        validate_report_daily_series(df)

    def test_all_zero_days_valid(self):
        """An entirely zero series is structurally valid (e.g. brand-new report)."""
        df = _make_series(n_days=5, daily_views=[0, 0, 0, 0, 0])
        validate_report_daily_series(df)


# ---------------------------------------------------------------------------
# 3. Duplicate report-date rows  →  check [7]
# ---------------------------------------------------------------------------

class TestDuplicateRows:
    def test_duplicate_raises(self):
        df = _make_series(n_days=5)
        duplicate_row = df.iloc[[0]].copy()
        df = pd.concat([df, duplicate_row], ignore_index=True)

        with pytest.raises(SeriesValidationError, match=r"\[7\]"):
            validate_report_daily_series(df)

    def test_error_names_report(self):
        df = _make_series(report_ids=["R_001"], n_days=3)
        df = pd.concat([df, df.iloc[[1]]], ignore_index=True)

        with pytest.raises(SeriesValidationError) as exc_info:
            validate_report_daily_series(df)
        assert "R_001" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 4. Negative daily views  →  check [5]
# ---------------------------------------------------------------------------

class TestNegativeViews:
    def test_negative_raises(self):
        df = _make_series(daily_views=[5, -1, 3, 0, 2, 4, 1])
        df.loc[df["daily_views"] < 0, "is_observed_day"] = True
        df.loc[df["daily_views"] < 0, "is_imputed_zero"] = False

        with pytest.raises(SeriesValidationError, match=r"\[5\]"):
            validate_report_daily_series(df)

    def test_error_names_report_and_date(self):
        df = _make_series(report_ids=["R_007"], n_days=5)
        df.loc[2, "daily_views"] = -99
        df.loc[2, "is_observed_day"] = True
        df.loc[2, "is_imputed_zero"] = False

        with pytest.raises(SeriesValidationError) as exc_info:
            validate_report_daily_series(df)
        msg = str(exc_info.value)
        assert "R_007" in msg
        assert "2025-01-03" in msg


# ---------------------------------------------------------------------------
# 5. Missing required column  →  check [1]
# ---------------------------------------------------------------------------

class TestMissingColumn:
    @pytest.mark.parametrize("col", [
        "report_id", "date", "daily_views", "is_observed_day", "is_imputed_zero"
    ])
    def test_each_required_column(self, col):
        df = _make_series().drop(columns=[col])
        with pytest.raises(SeriesValidationError, match=r"\[1\]"):
            validate_report_daily_series(df)

    def test_error_names_missing_column(self):
        df = _make_series().drop(columns=["daily_views"])
        with pytest.raises(SeriesValidationError) as exc_info:
            validate_report_daily_series(df)
        assert "daily_views" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 6. Report beginning halfway through the calendar  →  must pass
# ---------------------------------------------------------------------------

class TestLaunchMidCalendar:
    def test_late_start_valid(self):
        """Late-launch report (start 2025-07-07) with a complete spine is valid."""
        df = _make_series(start="2025-07-07", n_days=30)
        validate_report_daily_series(df)

    def test_two_reports_different_starts(self):
        """Portfolio with different launch dates must pass when each spine is gapless."""
        early = _make_series(report_ids=["R_001"], start="2025-01-01", n_days=20)
        late = _make_series(report_ids=["R_022"], start="2025-07-07", n_days=20)
        df = pd.concat([early, late], ignore_index=True)
        validate_report_daily_series(df)

    def test_gap_in_late_report_raises(self):
        """A gap in the late-launch report's spine must still fail check [9]."""
        df = _make_series(start="2025-07-07", n_days=10)
        # Remove day 5 to create a gap
        df = df.drop(index=4).reset_index(drop=True)
        with pytest.raises(SeriesValidationError, match=r"\[9\]"):
            validate_report_daily_series(df)


# ---------------------------------------------------------------------------
# 7. Incorrect is_imputed_zero flag  →  check [10]
# ---------------------------------------------------------------------------

class TestImputedZeroFlag:
    def test_imputed_zero_with_nonzero_views_raises(self):
        """is_imputed_zero=True when daily_views=5 is a contradiction."""
        df = _make_series(daily_views=[5, 3, 8, 2, 7, 1, 4])
        df.loc[0, "is_imputed_zero"] = True   # views=5, flag=True — wrong

        with pytest.raises(SeriesValidationError, match=r"\[10\]"):
            validate_report_daily_series(df)

    def test_error_names_report_and_date(self):
        df = _make_series(report_ids=["R_012"], n_days=5,
                          daily_views=[10, 8, 6, 4, 2])
        df.loc[2, "is_imputed_zero"] = True   # views=6, flag=True — wrong

        with pytest.raises(SeriesValidationError) as exc_info:
            validate_report_daily_series(df)
        assert "R_012" in str(exc_info.value)

    def test_both_indicators_true_raises(self):
        """is_observed_day=True and is_imputed_zero=True cannot coexist — check [11]."""
        df = _make_series(n_days=5, daily_views=[0, 0, 0, 0, 0])
        df.loc[1, "is_observed_day"] = True
        df.loc[1, "is_imputed_zero"] = True

        with pytest.raises(SeriesValidationError, match=r"\[11\]"):
            validate_report_daily_series(df)

    def test_zero_view_observed_day_false_valid(self):
        """is_observed_day=False, is_imputed_zero=True, daily_views=0 is the correct zero row."""
        df = _make_series(n_days=3, daily_views=[0, 0, 0])
        # All rows should be: observed=False, imputed=True, views=0
        validate_report_daily_series(df)  # must not raise


# ---------------------------------------------------------------------------
# 8. Edge-case and multi-failure tests
# ---------------------------------------------------------------------------

class TestMultipleFailures:
    def test_multiple_checks_all_reported(self):
        """All independent failures should appear in a single exception."""
        df = _make_series(n_days=5, daily_views=[10, 8, 6, 4, 2])
        # Introduce two independent failures
        df.loc[0, "daily_views"] = -3          # check [5]
        df.loc[0, "is_observed_day"] = True
        df.loc[0, "is_imputed_zero"] = False
        df.loc[1, "is_imputed_zero"] = True    # check [10]: views=8 but flag=True

        with pytest.raises(SeriesValidationError) as exc_info:
            validate_report_daily_series(df)

        msg = str(exc_info.value)
        assert "[5]" in msg
        assert "[10]" in msg

    def test_expected_row_count_mismatch_raises(self):
        df = _make_series(n_days=7)
        with pytest.raises(SeriesValidationError, match=r"\[13\]"):
            validate_report_daily_series(df, expected_row_count=999)

    def test_expected_row_count_correct_passes(self):
        df = _make_series(n_days=7)
        validate_report_daily_series(df, expected_row_count=7)

    def test_null_report_id_raises(self):
        df = _make_series(n_days=5)
        df.loc[2, "report_id"] = None
        with pytest.raises(SeriesValidationError, match=r"\[2\]"):
            validate_report_daily_series(df)

    def test_null_date_raises(self):
        df = _make_series(n_days=5)
        df.loc[2, "date"] = None
        with pytest.raises(SeriesValidationError, match=r"\[3\]"):
            validate_report_daily_series(df)

    def test_date_gap_raises(self):
        df = _make_series(n_days=10)
        df = df.drop(index=4).reset_index(drop=True)   # remove 2025-01-05
        with pytest.raises(SeriesValidationError, match=r"\[9\]"):
            validate_report_daily_series(df)

    def test_unsorted_dates_raises(self):
        df = _make_series(n_days=5)
        df = df.iloc[::-1].reset_index(drop=True)   # reverse order
        with pytest.raises(SeriesValidationError, match=r"\[8\]"):
            validate_report_daily_series(df)
