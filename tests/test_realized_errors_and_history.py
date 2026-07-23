"""Tests for production forecast realization and history persistence.

Covers:
- update_realized_errors  (join, zero-view, future-date, partial/full windows, dedup)
- append_forecasts_history (append, lineage, no-overwrite)
- append_metrics_history   (append, no-overwrite, no duplicate run)
- compute_daily_actuals    (zero-fill, groupby)
- schema validation        (missing columns raise clearly)

All file I/O uses tmp_path — no repository output directories are touched.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.pipelines.run_forecasting_pipeline import (
    append_forecasts_history,
    append_metrics_history,
    compute_daily_actuals,
    update_realized_errors,
)

# ── Column name constants (mirror the module) ─────────────────────────────────
_DATE_COL = "Date"
_REPORT_ID_COL = "Report Guid"
_REPORT_NAME_COL = "Report Name"
_VIEWS_COL = "Occurrences"

_HISTORY_COLS = [
    "run_id", "run_timestamp", "report_id", "report_name",
    "target_date", "horizon_days", "forecast_views",
    "lower_ci", "upper_ci", "model_name",
]
_REALIZED_COLS = [
    "run_id", "report_id", "report_name", "target_date",
    "horizon_days", "forecast_views", "actual_views",
    "error", "abs_error", "pct_error",
    "run_timestamp", "realized_at", "model_name",
]

# ── Shared fixtures ────────────────────────────────────────────────────────────

def _raw_df(
    report_id: str = "R001",
    report_name: str = "Report A",
    dates: list[pd.Timestamp] | None = None,
    views: list[int] | None = None,
) -> pd.DataFrame:
    """Return a minimal raw actuals DataFrame in the pipeline's native schema."""
    if views is None and dates is None:
        dates = pd.date_range("2024-01-01", periods=5, freq="D").tolist()
        views = [10] * len(dates)
    elif views is None:
        views = [10] * len(dates)
    elif dates is None:
        dates = pd.date_range("2024-01-01", periods=len(views), freq="D").tolist()
    return pd.DataFrame({
        _DATE_COL: dates,
        _REPORT_ID_COL: report_id,
        _REPORT_NAME_COL: report_name,
        _VIEWS_COL: views,
    })


def _history_row(
    run_id: str = "run_A",
    report_id: str = "R001",
    report_name: str = "Report A",
    target_date: str = "2024-01-01",
    horizon_days: int = 1,
    forecast_views: float = 8.0,
    model_name: str = "seasonal_naive_m7",
    lower_ci: float = 6.0,
    upper_ci: float = 10.0,
    run_timestamp: str = "2023-12-31 00:00:00",
) -> dict:
    return {
        "run_id": run_id,
        "run_timestamp": pd.Timestamp(run_timestamp),
        "report_id": report_id,
        "report_name": report_name,
        "target_date": pd.Timestamp(target_date),
        "horizon_days": horizon_days,
        "forecast_views": forecast_views,
        "lower_ci": lower_ci,
        "upper_ci": upper_ci,
        "model_name": model_name,
    }


def _write_history(tmp_path: Path, rows: list[dict]) -> Path:
    """Write rows to forecasts_history.csv under a tmp_path project root.

    Also pre-creates outputs/metrics/ so update_realized_errors can write there.
    """
    fc_dir = tmp_path / "outputs" / "forecasts"
    fc_dir.mkdir(parents=True)
    (tmp_path / "outputs" / "metrics").mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(fc_dir / "forecasts_history.csv", index=False)
    return tmp_path  # return the project_root


def _read_realized(tmp_path: Path) -> pd.DataFrame:
    path = tmp_path / "outputs" / "metrics" / "realized_errors_history.csv"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, parse_dates=["target_date", "run_timestamp", "realized_at"])


# ══════════════════════════════════════════════════════════════════════════════
# 1. compute_daily_actuals
# ══════════════════════════════════════════════════════════════════════════════

class TestComputeDailyActuals:

    def test_output_columns(self):
        df = _raw_df(views=[5, 10])
        result = compute_daily_actuals(df)
        assert set(result.columns) >= {"report_id", "report_name", "date", "actual_views"}

    def test_sums_multiple_rows_same_day(self):
        """Multiple events on the same day for the same report must be summed."""
        day = pd.Timestamp("2024-03-01")
        df = pd.DataFrame({
            _DATE_COL: [day, day, day],
            _REPORT_ID_COL: ["R1", "R1", "R1"],
            _REPORT_NAME_COL: ["Rep", "Rep", "Rep"],
            _VIEWS_COL: [3, 4, 5],
        })
        result = compute_daily_actuals(df)
        assert result["actual_views"].iloc[0] == 12

    def test_zero_views_preserved_not_dropped(self):
        """A day with zero Occurrences must appear in the output with actual_views=0."""
        df = _raw_df(views=[0, 5])
        result = compute_daily_actuals(df)
        assert 0 in result["actual_views"].values

    def test_null_occurrences_treated_as_zero(self):
        """NaN Occurrences must be coerced to 0, not dropped."""
        df = _raw_df(views=[None, 7])
        result = compute_daily_actuals(df)
        assert 0 in result["actual_views"].values

    def test_date_column_is_datetime(self):
        result = compute_daily_actuals(_raw_df())
        assert pd.api.types.is_datetime64_any_dtype(result["date"])

    def test_separate_reports_not_merged(self):
        """Rows from different reports on the same date must remain separate rows."""
        day = pd.Timestamp("2024-01-01")
        df = pd.DataFrame({
            _DATE_COL: [day, day],
            _REPORT_ID_COL: ["R1", "R2"],
            _REPORT_NAME_COL: ["Rep1", "Rep2"],
            _VIEWS_COL: [3, 7],
        })
        result = compute_daily_actuals(df)
        assert len(result) == 2


# ══════════════════════════════════════════════════════════════════════════════
# 2. update_realized_errors — join semantics
# ══════════════════════════════════════════════════════════════════════════════

class TestUpdateRealizedErrorsJoin:

    def test_joins_on_report_id_and_target_date(self, tmp_path):
        """Join must use report_id + target_date as the composite key."""
        project_root = _write_history(tmp_path, [
            _history_row(report_id="R001", target_date="2024-01-03"),
        ])
        raw = _raw_df(report_id="R001", dates=[pd.Timestamp("2024-01-03")], views=[20])
        result = update_realized_errors(raw, project_root)
        assert len(result) == 1
        assert result["report_id"].iloc[0] == "R001"
        assert result["actual_views"].iloc[0] == 20

    def test_wrong_report_id_does_not_match(self, tmp_path):
        """Forecast for R001 must not match actuals for R002."""
        project_root = _write_history(tmp_path, [
            _history_row(report_id="R001", target_date="2024-01-03"),
        ])
        raw = _raw_df(report_id="R002", dates=[pd.Timestamp("2024-01-03")], views=[20])
        result = update_realized_errors(raw, project_root)
        assert result.empty

    def test_wrong_date_does_not_match(self, tmp_path):
        """Forecast with target_date=Jan-03 must not match actuals for Jan-04."""
        project_root = _write_history(tmp_path, [
            _history_row(report_id="R001", target_date="2024-01-03"),
        ])
        raw = _raw_df(report_id="R001", dates=[pd.Timestamp("2024-01-04")], views=[15])
        result = update_realized_errors(raw, project_root)
        assert result.empty

    def test_output_contains_required_columns(self, tmp_path):
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01"),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[5])
        result = update_realized_errors(raw, project_root)
        assert set(_REALIZED_COLS).issubset(result.columns)

    def test_error_column_is_signed(self, tmp_path):
        """error = actual_views - forecast_views (signed)."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01", forecast_views=8.0),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[5])
        result = update_realized_errors(raw, project_root)
        assert result["error"].iloc[0] == pytest.approx(5 - 8)

    def test_abs_error_is_non_negative(self, tmp_path):
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01", forecast_views=8.0),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[5])
        result = update_realized_errors(raw, project_root)
        assert result["abs_error"].iloc[0] >= 0


# ══════════════════════════════════════════════════════════════════════════════
# 3. Zero-view actual days
# ══════════════════════════════════════════════════════════════════════════════

class TestZeroViewActuals:

    def test_zero_view_day_is_retained(self, tmp_path):
        """actual_views=0 must appear in realized errors (not filtered out)."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01", forecast_views=5.0),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[0])
        result = update_realized_errors(raw, project_root)
        assert len(result) == 1
        assert result["actual_views"].iloc[0] == 0

    def test_zero_view_error_is_signed_and_valid(self, tmp_path):
        """error = 0 - forecast_views; must be finite and correctly signed."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01", forecast_views=5.0),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[0])
        result = update_realized_errors(raw, project_root)
        assert result["error"].iloc[0] == pytest.approx(-5.0)

    def test_zero_view_abs_error_is_valid(self, tmp_path):
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01", forecast_views=5.0),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[0])
        result = update_realized_errors(raw, project_root)
        assert result["abs_error"].iloc[0] == pytest.approx(5.0)

    def test_zero_view_pct_error_is_nan_not_inf(self, tmp_path):
        """pct_error is undefined when actual=0; must be NaN, not inf or raise."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01", forecast_views=5.0),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[0])
        result = update_realized_errors(raw, project_root)
        assert np.isnan(result["pct_error"].iloc[0])
        assert not np.isinf(result["pct_error"].iloc[0])

    def test_zero_view_does_not_raise(self, tmp_path):
        """Processing a zero-view actual day must never raise an exception."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01", forecast_views=5.0),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[0])
        update_realized_errors(raw, project_root)  # must not raise


# ══════════════════════════════════════════════════════════════════════════════
# 4. Future dates — not marked realized
# ══════════════════════════════════════════════════════════════════════════════

class TestFutureDatesNotRealized:

    def test_future_forecast_not_matched(self, tmp_path):
        """A target_date with no actual row available must not appear in output."""
        future = pd.Timestamp("2099-01-01")
        project_root = _write_history(tmp_path, [
            _history_row(target_date=str(future.date())),
        ])
        # actuals contain only today, not the far future
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[10])
        result = update_realized_errors(raw, project_root)
        assert result.empty

    def test_mixed_past_and_future(self, tmp_path):
        """Only the past date should be realized; the future date should be excluded."""
        project_root = _write_history(tmp_path, [
            _history_row(run_id="run1", target_date="2024-01-01", forecast_views=5.0),
            _history_row(run_id="run1", target_date="2099-12-31", forecast_views=5.0),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[10])
        result = update_realized_errors(raw, project_root)
        assert len(result) == 1
        assert result["target_date"].iloc[0] == pd.Timestamp("2024-01-01")

    def test_no_actuals_at_all_returns_empty(self, tmp_path):
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2099-01-01"),
        ])
        raw = pd.DataFrame({
            _DATE_COL: pd.Series([], dtype="object"),
            _REPORT_ID_COL: pd.Series([], dtype="object"),
            _REPORT_NAME_COL: pd.Series([], dtype="object"),
            _VIEWS_COL: pd.Series([], dtype="float64"),
        })
        result = update_realized_errors(raw, project_root)
        assert result.empty


# ══════════════════════════════════════════════════════════════════════════════
# 5. Partial and full 28-day windows
# ══════════════════════════════════════════════════════════════════════════════

class TestPartialAndFullWindows:

    def _make_28day_history(self, run_id: str = "run1") -> list[dict]:
        """Return 28 forecast rows for a single report starting 2024-01-01."""
        return [
            _history_row(
                run_id=run_id,
                target_date=str((pd.Timestamp("2024-01-01") + pd.Timedelta(days=i)).date()),
                horizon_days=i + 1,
                forecast_views=float(10 + i),
            )
            for i in range(28)
        ]

    def test_partial_window_appends_only_available_dates(self, tmp_path):
        """If only 10 of 28 actual dates are available, exactly 10 rows are realized."""
        project_root = _write_history(tmp_path, self._make_28day_history())
        avail_dates = pd.date_range("2024-01-01", periods=10, freq="D").tolist()
        raw = _raw_df(
            dates=avail_dates,
            views=[5] * 10,
        )
        result = update_realized_errors(raw, project_root)
        assert len(result) == 10

    def test_full_window_appends_all_28_dates(self, tmp_path):
        """When all 28 actual dates are available, all 28 rows must be realized."""
        project_root = _write_history(tmp_path, self._make_28day_history())
        avail_dates = pd.date_range("2024-01-01", periods=28, freq="D").tolist()
        raw = _raw_df(
            dates=avail_dates,
            views=[5] * 28,
        )
        result = update_realized_errors(raw, project_root)
        assert len(result) == 28

    def test_partial_horizon_steps_are_contiguous(self, tmp_path):
        """Realized rows for a partial window must cover the first N horizon steps."""
        project_root = _write_history(tmp_path, self._make_28day_history())
        avail_dates = pd.date_range("2024-01-01", periods=7, freq="D").tolist()
        raw = _raw_df(dates=avail_dates, views=[5] * 7)
        result = update_realized_errors(raw, project_root)
        realized_dates = sorted(result["target_date"])
        expected = pd.date_range("2024-01-01", periods=7, freq="D").tolist()
        assert realized_dates == [pd.Timestamp(d) for d in expected]

    def test_full_window_all_actual_views_correct(self, tmp_path):
        """Each realized row must carry the actual_views from the actuals table."""
        project_root = _write_history(tmp_path, self._make_28day_history())
        views = list(range(100, 128))  # 100, 101, ..., 127
        avail_dates = pd.date_range("2024-01-01", periods=28, freq="D").tolist()
        raw = _raw_df(dates=avail_dates, views=views)
        result = update_realized_errors(raw, project_root)
        result_sorted = result.sort_values("target_date").reset_index(drop=True)
        assert list(result_sorted["actual_views"]) == views


# ══════════════════════════════════════════════════════════════════════════════
# 6 & 7. Deduplication — composite key (run_id, report_id, target_date)
# ══════════════════════════════════════════════════════════════════════════════

class TestDeduplication:

    def test_duplicate_key_not_written_on_second_call(self, tmp_path):
        """Running update_realized_errors twice with the same history must not
        create duplicate rows in realized_errors_history.csv."""
        project_root = _write_history(tmp_path, [
            _history_row(run_id="run1", target_date="2024-01-01"),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[10])

        update_realized_errors(raw, project_root)
        update_realized_errors(raw, project_root)

        result = _read_realized(tmp_path)
        assert len(result) == 1

    def test_second_call_returns_empty_when_all_already_written(self, tmp_path):
        """Second call must return an empty DataFrame when nothing new to append."""
        project_root = _write_history(tmp_path, [
            _history_row(run_id="run1", target_date="2024-01-01"),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[10])

        update_realized_errors(raw, project_root)
        second = update_realized_errors(raw, project_root)
        assert second.empty

    def test_different_run_ids_same_report_and_date_are_separate(self, tmp_path):
        """Two different forecast runs for the same (report_id, target_date) must
        both be retained — deduplication key includes run_id."""
        project_root = _write_history(tmp_path, [
            _history_row(run_id="run1", target_date="2024-01-01", forecast_views=8.0),
            _history_row(run_id="run2", target_date="2024-01-01", forecast_views=9.0),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[10])

        update_realized_errors(raw, project_root)
        result = _read_realized(tmp_path)
        assert len(result) == 2
        run_ids = set(result["run_id"])
        assert run_ids == {"run1", "run2"}

    def test_dedup_key_uses_all_three_components(self, tmp_path):
        """Rows that differ on only one key component must each be retained."""
        project_root = _write_history(tmp_path, [
            # Same run_id + report_id, different date
            _history_row(run_id="run1", report_id="R001", target_date="2024-01-01"),
            _history_row(run_id="run1", report_id="R001", target_date="2024-01-02"),
            # Same run_id + date, different report
            _history_row(run_id="run1", report_id="R002", target_date="2024-01-01"),
        ])
        raw = pd.DataFrame({
            _DATE_COL: [
                pd.Timestamp("2024-01-01"),
                pd.Timestamp("2024-01-02"),
                pd.Timestamp("2024-01-01"),
            ],
            _REPORT_ID_COL: ["R001", "R001", "R002"],
            _REPORT_NAME_COL: ["Rep1", "Rep1", "Rep2"],
            _VIEWS_COL: [10, 11, 12],
        })
        result = update_realized_errors(raw, project_root)
        assert len(result) == 3

    def test_repeated_calls_accumulate_new_rows_only(self, tmp_path):
        """If a new date becomes available on a second call, that row must be added
        while the already-written row is not duplicated."""
        # Start: one forecast row for Jan-01
        project_root = _write_history(tmp_path, [
            _history_row(run_id="run1", target_date="2024-01-01"),
            _history_row(run_id="run1", target_date="2024-01-02"),
        ])
        raw_day1 = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[10])
        raw_day1_and_2 = _raw_df(
            dates=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
            views=[10, 20],
        )

        update_realized_errors(raw_day1, project_root)        # writes Jan-01
        update_realized_errors(raw_day1_and_2, project_root)  # should add Jan-02 only

        result = _read_realized(tmp_path)
        assert len(result) == 2


# ══════════════════════════════════════════════════════════════════════════════
# 8. Multiple forecast runs — same report, same date
# ══════════════════════════════════════════════════════════════════════════════

class TestMultipleRunsSameReportDate:

    def test_two_runs_same_report_same_date_both_stored(self, tmp_path):
        """Forecasts from two different runs for the same report and date must
        both appear in realized_errors_history.csv."""
        project_root = _write_history(tmp_path, [
            _history_row(run_id="run_A", target_date="2024-06-01", forecast_views=100.0),
            _history_row(run_id="run_B", target_date="2024-06-01", forecast_views=120.0),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-06-01")], views=[110])
        update_realized_errors(raw, project_root)
        result = _read_realized(tmp_path)

        assert set(result["run_id"]) == {"run_A", "run_B"}
        # Errors differ because forecast_views differ
        row_a = result[result["run_id"] == "run_A"].iloc[0]
        row_b = result[result["run_id"] == "run_B"].iloc[0]
        assert row_a["error"] == pytest.approx(110 - 100)
        assert row_b["error"] == pytest.approx(110 - 120)

    def test_model_lineage_preserved_per_run(self, tmp_path):
        """model_name must be carried through per-run; different runs may use
        different models for the same report."""
        project_root = _write_history(tmp_path, [
            _history_row(run_id="run_A", target_date="2024-06-01",
                         model_name="seasonal_naive_m7"),
            _history_row(run_id="run_B", target_date="2024-06-01",
                         model_name="auto_arima_m7"),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-06-01")], views=[50])
        update_realized_errors(raw, project_root)
        result = _read_realized(tmp_path)

        models = set(result["model_name"])
        assert "seasonal_naive_m7" in models
        assert "auto_arima_m7" in models


# ══════════════════════════════════════════════════════════════════════════════
# 9. append_forecasts_history
# ══════════════════════════════════════════════════════════════════════════════

def _forecast_table(
    run_id: str = "run_A",
    run_timestamp: str = "2023-12-31 00:00:00",
    report_id: str = "R001",
    report_name: str = "Report A",
    n_forecast: int = 3,
    model_name: str = "seasonal_naive_m7",
    start_date: str = "2024-01-01",
) -> pd.DataFrame:
    """Build a minimal forecast_table as produced by run_forecasts_for_reports."""
    n_actual = 2
    dates = pd.date_range(start_date, periods=n_actual + n_forecast, freq="D")
    rows = []
    ts = pd.Timestamp(run_timestamp)
    for i, d in enumerate(dates):
        is_fc = 1 if i >= n_actual else 0
        rows.append({
            "run_id": run_id,
            "run_timestamp": ts,
            "ReportId": report_id,
            "ReportName": report_name,
            "Date": d,
            "ModelRunTimestamp": ts,
            "forecast": float(10 + i),
            "lower_ci": float(8 + i),
            "upper_ci": float(12 + i),
            "ModelName": model_name,
            "IsForecast": is_fc,
        })
    return pd.DataFrame(rows)


class TestAppendForecastsHistory:

    def test_first_run_creates_file(self, tmp_path):
        out_dir = tmp_path / "outputs" / "forecasts"
        out_dir.mkdir(parents=True)
        table = _forecast_table()
        append_forecasts_history(table, tmp_path)
        assert (out_dir / "forecasts_history.csv").exists()

    def test_first_run_writes_forecast_rows_only(self, tmp_path):
        (tmp_path / "outputs" / "forecasts").mkdir(parents=True)
        table = _forecast_table(n_forecast=3)  # 2 actuals + 3 forecasts in table
        append_forecasts_history(table, tmp_path)
        result = pd.read_csv(tmp_path / "outputs" / "forecasts" / "forecasts_history.csv")
        assert len(result) == 3  # only IsForecast==1 rows

    def test_second_run_appends_preserves_previous_rows(self, tmp_path):
        """A second call must append without deleting the first run's rows."""
        (tmp_path / "outputs" / "forecasts").mkdir(parents=True)
        t1 = _forecast_table(run_id="run1", start_date="2024-01-01", n_forecast=3)
        t2 = _forecast_table(run_id="run2", start_date="2024-01-10", n_forecast=3)
        append_forecasts_history(t1, tmp_path)
        append_forecasts_history(t2, tmp_path)
        result = pd.read_csv(tmp_path / "outputs" / "forecasts" / "forecasts_history.csv")
        assert len(result) == 6  # 3 + 3
        assert set(result["run_id"]) == {"run1", "run2"}

    def test_header_written_only_once(self, tmp_path):
        """After two appends the CSV header must appear exactly once."""
        (tmp_path / "outputs" / "forecasts").mkdir(parents=True)
        for i in range(2):
            t = _forecast_table(run_id=f"run{i}", start_date="2024-01-01")
            append_forecasts_history(t, tmp_path)
        path = tmp_path / "outputs" / "forecasts" / "forecasts_history.csv"
        with open(path) as f:
            lines = f.readlines()
        header_lines = [l for l in lines if l.startswith("run_id")]
        assert len(header_lines) == 1

    def test_model_name_preserved_in_history(self, tmp_path):
        """model_name (seasonal period lineage) must survive the rename/select step."""
        (tmp_path / "outputs" / "forecasts").mkdir(parents=True)
        table = _forecast_table(model_name="auto_arima_m30")
        append_forecasts_history(table, tmp_path)
        result = pd.read_csv(tmp_path / "outputs" / "forecasts" / "forecasts_history.csv")
        assert all(result["model_name"] == "auto_arima_m30")

    def test_run_id_and_run_timestamp_preserved(self, tmp_path):
        """run_id and run_timestamp must appear unchanged in history rows."""
        (tmp_path / "outputs" / "forecasts").mkdir(parents=True)
        ts = "2024-06-15 08:30:00"
        table = _forecast_table(run_id="run_XYZ", run_timestamp=ts)
        append_forecasts_history(table, tmp_path)
        result = pd.read_csv(tmp_path / "outputs" / "forecasts" / "forecasts_history.csv")
        assert all(result["run_id"] == "run_XYZ")
        assert all(pd.to_datetime(result["run_timestamp"]) == pd.Timestamp(ts))

    def test_lower_and_upper_ci_preserved(self, tmp_path):
        """Confidence-interval columns must survive into the history file."""
        (tmp_path / "outputs" / "forecasts").mkdir(parents=True)
        table = _forecast_table()
        append_forecasts_history(table, tmp_path)
        result = pd.read_csv(tmp_path / "outputs" / "forecasts" / "forecasts_history.csv")
        assert "lower_ci" in result.columns
        assert "upper_ci" in result.columns
        assert result["lower_ci"].notna().all()

    def test_empty_table_returns_none_and_no_file(self, tmp_path):
        """An empty forecast_table must not create the history file."""
        (tmp_path / "outputs" / "forecasts").mkdir(parents=True)
        result = append_forecasts_history(pd.DataFrame(), tmp_path)
        assert result is None
        assert not (tmp_path / "outputs" / "forecasts" / "forecasts_history.csv").exists()

    def test_no_is_forecast_rows_returns_none(self, tmp_path):
        """A table with IsForecast==0 only must write nothing and return None."""
        (tmp_path / "outputs" / "forecasts").mkdir(parents=True)
        table = _forecast_table(n_forecast=0)
        # All rows are actuals (IsForecast==0)
        assert (table["IsForecast"] == 0).all()
        result = append_forecasts_history(table, tmp_path)
        assert result is None

    def test_horizon_days_computed_as_target_minus_run(self, tmp_path):
        """horizon_days must equal calendar days between run date and target date."""
        (tmp_path / "outputs" / "forecasts").mkdir(parents=True)
        ts = "2024-01-01 00:00:00"
        # First forecast date is 2024-01-03 (2 actual rows before it)
        table = _forecast_table(run_timestamp=ts, start_date="2024-01-01", n_forecast=1)
        append_forecasts_history(table, tmp_path)
        result = pd.read_csv(tmp_path / "outputs" / "forecasts" / "forecasts_history.csv")
        assert result["horizon_days"].iloc[0] == 2  # Jan-03 - Jan-01 = 2 days


# ══════════════════════════════════════════════════════════════════════════════
# 10. append_metrics_history
# ══════════════════════════════════════════════════════════════════════════════

def _metrics_table(n: int = 3) -> pd.DataFrame:
    return pd.DataFrame({
        "report_id": [f"R{i:03d}" for i in range(n)],
        "model_name": ["naive"] * n,
        "wape": [0.2 + 0.01 * i for i in range(n)],
        "mae": [5.0 + i for i in range(n)],
    })


class TestAppendMetricsHistory:

    def test_first_run_creates_file(self, tmp_path):
        out_dir = tmp_path / "outputs" / "metrics"
        out_dir.mkdir(parents=True)
        append_metrics_history(_metrics_table(), tmp_path, "run1", pd.Timestamp("2024-01-01"))
        assert (out_dir / "metrics_history.csv").exists()

    def test_run_id_and_timestamp_injected(self, tmp_path):
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        ts = pd.Timestamp("2024-06-01 12:00:00")
        append_metrics_history(_metrics_table(1), tmp_path, "run_ABC", ts)
        result = pd.read_csv(tmp_path / "outputs" / "metrics" / "metrics_history.csv")
        assert result["run_id"].iloc[0] == "run_ABC"
        assert pd.Timestamp(result["run_timestamp"].iloc[0]) == ts

    def test_second_run_appends_preserves_first(self, tmp_path):
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        ts1 = pd.Timestamp("2024-01-01")
        ts2 = pd.Timestamp("2024-02-01")
        append_metrics_history(_metrics_table(2), tmp_path, "run1", ts1)
        append_metrics_history(_metrics_table(2), tmp_path, "run2", ts2)
        result = pd.read_csv(tmp_path / "outputs" / "metrics" / "metrics_history.csv")
        assert set(result["run_id"]) == {"run1", "run2"}
        assert len(result) == 4  # 2 reports × 2 runs

    def test_header_written_only_once(self, tmp_path):
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        for i in range(3):
            append_metrics_history(
                _metrics_table(1), tmp_path, f"run{i}", pd.Timestamp("2024-01-01")
            )
        path = tmp_path / "outputs" / "metrics" / "metrics_history.csv"
        with open(path) as f:
            content = f.read()
        assert content.count("run_id") == 1  # appears once in header

    def test_empty_metrics_returns_none(self, tmp_path):
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        result = append_metrics_history(
            pd.DataFrame(), tmp_path, "run1", pd.Timestamp("2024-01-01")
        )
        assert result is None
        assert not (tmp_path / "outputs" / "metrics" / "metrics_history.csv").exists()

    def test_duplicate_run_id_report_id_raises(self, tmp_path):
        """Appending the same (run_id, report_id) pair a second time must raise
        a clear ValueError — the grain of metrics_history is (run_id, report_id)."""
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        ts = pd.Timestamp("2024-01-01")
        append_metrics_history(_metrics_table(1), tmp_path, "run1", ts)
        with pytest.raises(ValueError, match="duplicate"):
            append_metrics_history(_metrics_table(1), tmp_path, "run1", ts)

    def test_different_run_ids_same_report_both_allowed(self, tmp_path):
        """Two different run_ids for the same report_id must both be accepted —
        they are distinct rows under the (run_id, report_id) grain."""
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        ts = pd.Timestamp("2024-01-01")
        t1 = _metrics_table(1)  # one row, report_id = R000
        t2 = _metrics_table(1)  # same report, different run
        append_metrics_history(t1, tmp_path, "run1", ts)
        append_metrics_history(t2, tmp_path, "run2", ts)  # must not raise
        result = pd.read_csv(tmp_path / "outputs" / "metrics" / "metrics_history.csv")
        assert len(result) == 2
        assert set(result["run_id"]) == {"run1", "run2"}

    def test_same_run_different_reports_all_accepted(self, tmp_path):
        """Within a single run, one row per report is the expected grain —
        all report rows in the same call must be accepted."""
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        ts = pd.Timestamp("2024-01-01")
        multi = _metrics_table(3)  # R000, R001, R002
        append_metrics_history(multi, tmp_path, "run1", ts)
        result = pd.read_csv(tmp_path / "outputs" / "metrics" / "metrics_history.csv")
        assert len(result) == 3

    def test_original_metric_columns_preserved(self, tmp_path):
        """Metric columns from the input table (wape, mae) must survive into history."""
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        append_metrics_history(
            _metrics_table(2), tmp_path, "run1", pd.Timestamp("2024-01-01")
        )
        result = pd.read_csv(tmp_path / "outputs" / "metrics" / "metrics_history.csv")
        assert "wape" in result.columns
        assert "mae" in result.columns


# ══════════════════════════════════════════════════════════════════════════════
# 11. update_realized_errors — edge cases and schema guards
# ══════════════════════════════════════════════════════════════════════════════

class TestSchemaAndEdgeCases:

    def test_no_forecasts_history_file_returns_empty(self, tmp_path):
        """If forecasts_history.csv does not exist, return empty DataFrame without error."""
        (tmp_path / "outputs" / "forecasts").mkdir(parents=True)
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        raw = _raw_df()
        result = update_realized_errors(raw, tmp_path)
        assert result.empty

    def test_empty_forecasts_history_file_returns_empty(self, tmp_path):
        """An empty (header-only) forecasts_history.csv must return an empty DataFrame."""
        out_dir = tmp_path / "outputs" / "forecasts"
        out_dir.mkdir(parents=True)
        header = ",".join(_HISTORY_COLS) + "\n"
        (out_dir / "forecasts_history.csv").write_text(header)
        raw = _raw_df()
        result = update_realized_errors(raw, tmp_path)
        assert result.empty

    def test_no_actuals_at_all_returns_empty(self, tmp_path):
        """If raw actuals DataFrame is empty, no realized errors can be produced."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01"),
        ])
        raw = pd.DataFrame(columns=[_DATE_COL, _REPORT_ID_COL, _REPORT_NAME_COL, _VIEWS_COL])
        result = update_realized_errors(raw, project_root)
        assert result.empty

    def test_realized_errors_file_created_on_first_call(self, tmp_path):
        """First successful call must create realized_errors_history.csv."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01"),
        ])
        (tmp_path / "outputs" / "metrics").mkdir(parents=True, exist_ok=True)
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[10])
        update_realized_errors(raw, project_root)
        assert (tmp_path / "outputs" / "metrics" / "realized_errors_history.csv").exists()

    def test_realized_errors_file_contains_all_required_columns(self, tmp_path):
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01"),
        ])
        (tmp_path / "outputs" / "metrics").mkdir(parents=True, exist_ok=True)
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[10])
        update_realized_errors(raw, project_root)
        result = pd.read_csv(
            tmp_path / "outputs" / "metrics" / "realized_errors_history.csv"
        )
        missing = set(_REALIZED_COLS) - set(result.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_positive_error_when_actual_exceeds_forecast(self, tmp_path):
        """error = actual - forecast; positive when actual > forecast."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01", forecast_views=5.0),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[10])
        result = update_realized_errors(raw, project_root)
        assert result["error"].iloc[0] > 0

    def test_negative_error_when_forecast_exceeds_actual(self, tmp_path):
        """error = actual - forecast; negative when forecast > actual."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01", forecast_views=20.0),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[5])
        result = update_realized_errors(raw, project_root)
        assert result["error"].iloc[0] < 0

    def test_pct_error_correct_for_nonzero_actual(self, tmp_path):
        """pct_error = |error| / actual × 100 when actual > 0."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01", forecast_views=8.0),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-01-01")], views=[10])
        result = update_realized_errors(raw, project_root)
        expected_pct = abs(10 - 8) / 10 * 100
        assert result["pct_error"].iloc[0] == pytest.approx(expected_pct)

    def test_multiple_reports_in_same_raw_df(self, tmp_path):
        """Actuals for multiple reports in one raw_df must each be matched correctly."""
        project_root = _write_history(tmp_path, [
            _history_row(report_id="R001", target_date="2024-01-01", forecast_views=5.0),
            _history_row(report_id="R002", target_date="2024-01-01", forecast_views=10.0),
        ])
        raw = pd.DataFrame({
            _DATE_COL: [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
            _REPORT_ID_COL: ["R001", "R002"],
            _REPORT_NAME_COL: ["Rep1", "Rep2"],
            _VIEWS_COL: [7, 15],
        })
        result = update_realized_errors(raw, project_root)
        assert len(result) == 2
        r1 = result[result["report_id"] == "R001"].iloc[0]
        r2 = result[result["report_id"] == "R002"].iloc[0]
        assert r1["actual_views"] == 7
        assert r2["actual_views"] == 15


# ══════════════════════════════════════════════════════════════════════════════
# 12. Canonical actuals source — raw_df must supply actuals
# ══════════════════════════════════════════════════════════════════════════════

class TestCanonicalActualsSource:

    def test_missing_occurrences_column_raises(self, tmp_path):
        """raw_df without the Occurrences column must raise (not silently produce zeros)."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01"),
        ])
        bad_df = pd.DataFrame({
            _DATE_COL: [pd.Timestamp("2024-01-01")],
            _REPORT_ID_COL: ["R001"],
            _REPORT_NAME_COL: ["Report A"],
            # _VIEWS_COL ("Occurrences") intentionally absent
        })
        with pytest.raises(KeyError):
            update_realized_errors(bad_df, project_root)

    def test_missing_date_column_raises(self, tmp_path):
        """raw_df without the Date column must raise (not silently match nothing)."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01"),
        ])
        bad_df = pd.DataFrame({
            # _DATE_COL ("Date") intentionally absent
            _REPORT_ID_COL: ["R001"],
            _REPORT_NAME_COL: ["Report A"],
            _VIEWS_COL: [10],
        })
        with pytest.raises(KeyError):
            update_realized_errors(bad_df, project_root)

    def test_missing_report_id_column_raises(self, tmp_path):
        """raw_df without the report-ID column must raise."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-01-01"),
        ])
        bad_df = pd.DataFrame({
            _DATE_COL: [pd.Timestamp("2024-01-01")],
            # _REPORT_ID_COL absent
            _REPORT_NAME_COL: ["Report A"],
            _VIEWS_COL: [10],
        })
        with pytest.raises(KeyError):
            update_realized_errors(bad_df, project_root)

    def test_alternative_actuals_source_not_used(self, tmp_path):
        """update_realized_errors must not silently fall back to any source other
        than the raw_df passed in; if the raw_df has no matching rows the result
        must be empty, not sourced from another file."""
        project_root = _write_history(tmp_path, [
            _history_row(report_id="R001", target_date="2024-01-01"),
        ])
        # Write a spurious actuals file next to the history — must be ignored
        spurious = tmp_path / "outputs" / "forecasts" / "spurious_actuals.csv"
        pd.DataFrame({
            "report_id": ["R001"], "date": ["2024-01-01"], "actual_views": [999]
        }).to_csv(spurious, index=False)

        raw = _raw_df(report_id="R999", dates=[pd.Timestamp("2024-01-01")], views=[50])
        result = update_realized_errors(raw, project_root)
        # R001 forecast cannot match R999 actuals; spurious file must be ignored
        assert result.empty


# ══════════════════════════════════════════════════════════════════════════════
# Realized-errors history validation (hardening tests)
# ══════════════════════════════════════════════════════════════════════════════

def _write_realized_errors(tmp_path: Path, rows: list[dict]) -> None:
    """Write rows directly to realized_errors_history.csv to simulate a
    pre-existing file in a given state (possibly corrupted)."""
    metrics_dir = tmp_path / "outputs" / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(
        metrics_dir / "realized_errors_history.csv", index=False
    )


def _realized_row(
    run_id: str = "run1",
    report_id: str = "R001",
    target_date: str = "2024-01-01",
    forecast_views: float = 8.0,
    actual_views: float = 10.0,
    error: float = 2.0,
    abs_error: float = 2.0,
    pct_error: float = 20.0,
    horizon_days: int = 1,
    report_name: str = "Report A",
    model_name: str = "seasonal_naive_m7",
    run_timestamp: str = "2023-12-31 00:00:00",
    realized_at: str = "2024-01-02 00:00:00",
) -> dict:
    return {
        "run_id": run_id,
        "report_id": report_id,
        "report_name": report_name,
        "target_date": target_date,
        "horizon_days": horizon_days,
        "forecast_views": forecast_views,
        "actual_views": actual_views,
        "error": error,
        "abs_error": abs_error,
        "pct_error": pct_error,
        "run_timestamp": run_timestamp,
        "realized_at": realized_at,
        "model_name": model_name,
    }


class TestRealizedErrorsHistoryValidation:
    """update_realized_errors must reject a corrupt history file with a clear
    ValueError rather than silently producing wrong deduplication behaviour."""

    def test_invalid_target_date_in_existing_history_raises(self, tmp_path):
        """A non-parseable date string in the existing file must raise ValueError."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-02-01"),
        ])
        # Write a pre-existing realized errors file with a bad date
        _write_realized_errors(tmp_path, [
            _realized_row(target_date="NOT_A_DATE"),
        ])
        raw = _raw_df(dates=[pd.Timestamp("2024-02-01")], views=[10])
        with pytest.raises(ValueError, match="target_date"):
            update_realized_errors(raw, project_root)

    def test_null_run_id_in_existing_history_raises(self, tmp_path):
        """A null run_id in the existing file must raise ValueError."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-02-01"),
        ])
        row = _realized_row()
        row["run_id"] = None  # inject null key
        _write_realized_errors(tmp_path, [row])
        raw = _raw_df(dates=[pd.Timestamp("2024-02-01")], views=[10])
        with pytest.raises(ValueError, match="null"):
            update_realized_errors(raw, project_root)

    def test_null_report_id_in_existing_history_raises(self, tmp_path):
        """A null report_id in the existing file must raise ValueError."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-02-01"),
        ])
        row = _realized_row()
        row["report_id"] = None
        _write_realized_errors(tmp_path, [row])
        raw = _raw_df(dates=[pd.Timestamp("2024-02-01")], views=[10])
        with pytest.raises(ValueError, match="null"):
            update_realized_errors(raw, project_root)

    def test_null_target_date_in_existing_history_raises(self, tmp_path):
        """A null target_date in the existing file must raise ValueError.
        (Distinct from unparseable: the value is genuinely absent/NaN.)"""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-02-01"),
        ])
        row = _realized_row()
        row["target_date"] = None  # will become NaT after parse
        _write_realized_errors(tmp_path, [row])
        raw = _raw_df(dates=[pd.Timestamp("2024-02-01")], views=[10])
        with pytest.raises(ValueError, match="null|target_date"):
            update_realized_errors(raw, project_root)

    def test_duplicate_existing_dedup_keys_raises(self, tmp_path):
        """If the existing file already contains duplicate (run_id, report_id,
        target_date) keys, update_realized_errors must raise ValueError rather
        than silently compounding the corruption."""
        project_root = _write_history(tmp_path, [
            _history_row(target_date="2024-02-01"),
        ])
        dup_row = _realized_row(
            run_id="run1", report_id="R001", target_date="2024-01-01"
        )
        _write_realized_errors(tmp_path, [dup_row, dup_row])  # same key twice
        raw = _raw_df(dates=[pd.Timestamp("2024-02-01")], views=[10])
        with pytest.raises(ValueError, match="duplicate"):
            update_realized_errors(raw, project_root)

    def test_valid_existing_history_proceeds_normally(self, tmp_path):
        """A clean existing file must not trigger any validation error."""
        project_root = _write_history(tmp_path, [
            _history_row(run_id="run1", target_date="2024-01-01"),
            _history_row(run_id="run1", target_date="2024-01-02"),
        ])
        # Write a clean pre-existing realized errors file
        _write_realized_errors(tmp_path, [
            _realized_row(run_id="run1", target_date="2024-01-01"),
        ])
        # Jan-02 is not yet in the realized file — should be appended without error
        raw = _raw_df(
            dates=[pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02")],
            views=[10, 20],
        )
        result = update_realized_errors(raw, project_root)
        assert len(result) == 1  # only Jan-02 is new
        assert result["target_date"].iloc[0] == pd.Timestamp("2024-01-02")


class TestMetricsHistoryValidation:
    """append_metrics_history must enforce (run_id, report_id) grain."""

    def test_duplicate_run_id_and_report_id_raises_value_error(self, tmp_path):
        """Re-appending the same (run_id, report_id) key must raise ValueError
        with a message mentioning 'duplicate'."""
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        ts = pd.Timestamp("2024-01-01")
        append_metrics_history(_metrics_table(1), tmp_path, "run_X", ts)
        with pytest.raises(ValueError, match="duplicate"):
            append_metrics_history(_metrics_table(1), tmp_path, "run_X", ts)

    def test_error_message_names_the_offending_key(self, tmp_path):
        """The ValueError message must identify the conflicting run_id."""
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        ts = pd.Timestamp("2024-01-01")
        append_metrics_history(_metrics_table(1), tmp_path, "run_CONFLICT", ts)
        with pytest.raises(ValueError, match="run_CONFLICT"):
            append_metrics_history(_metrics_table(1), tmp_path, "run_CONFLICT", ts)

    def test_valid_second_run_different_id_succeeds(self, tmp_path):
        """A second call with a fresh run_id must not raise."""
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        ts = pd.Timestamp("2024-01-01")
        append_metrics_history(_metrics_table(2), tmp_path, "run1", ts)
        append_metrics_history(_metrics_table(2), tmp_path, "run2", ts)  # fresh id
        result = pd.read_csv(tmp_path / "outputs" / "metrics" / "metrics_history.csv")
        assert len(result) == 4  # 2 reports × 2 runs

    def test_partial_overlap_raises(self, tmp_path):
        """If even one (run_id, report_id) key in the incoming batch conflicts,
        the entire call must be rejected."""
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        ts = pd.Timestamp("2024-01-01")
        # run1 has R000 and R001
        append_metrics_history(_metrics_table(2), tmp_path, "run1", ts)
        # Trying to append run1 again with 3 reports (R000–R002) — R000 and R001 conflict
        with pytest.raises(ValueError, match="duplicate"):
            append_metrics_history(_metrics_table(3), tmp_path, "run1", ts)

    def test_blank_run_id_raises(self, tmp_path):
        """A blank run_id string must raise ValueError immediately."""
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        with pytest.raises(ValueError, match="run_id"):
            append_metrics_history(_metrics_table(1), tmp_path, "", pd.Timestamp.now())

    def test_missing_report_id_column_raises(self, tmp_path):
        """A metrics_table without 'report_id' must raise ValueError."""
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        bad = pd.DataFrame({"model_name": ["naive"], "mae": [1.0]})
        with pytest.raises(ValueError, match="report_id"):
            append_metrics_history(bad, tmp_path, "run1", pd.Timestamp.now())
