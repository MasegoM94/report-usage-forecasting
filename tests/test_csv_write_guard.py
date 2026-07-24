"""Tests for the in-process CSV write guard and single-writer enforcement.

Covers:
  - lock released after successful write
  - lock released after write exception
  - two threads in one process do not corrupt the history file
  - repeated sequential calls remain idempotent (duplicate keys prevented)
  - duplicate keys remain prevented after concurrent access
  - legacy and current realized updaters are not both invoked in the same run
  - docstrings include the single-writer limitation
  - no claim of cross-process safety in docstrings
  - output remains append-only (no overwrites)

All file I/O uses temporary directories — no repository output files are touched.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.persistence._csv_lock import _lock_registry, csv_write_lock, log_write_plan


# ---------------------------------------------------------------------------
# Helpers shared across tests
# ---------------------------------------------------------------------------

_REALIZED_COLS_MIN = [
    "run_id", "selection_run_id", "generated_at", "training_cutoff",
    "realized_at", "report_id", "report_name",
    "selected_model_family", "selected_model_name", "selected_m",
    "forecast_date", "horizon_step",
    "forecast", "lower_bound", "upper_bound",
    "actual", "signed_error", "absolute_error", "squared_error",
    "inside_interval", "interval_width", "percentage_error",
    "lineage_complete", "lineage_missing_fields",
]


def _realized_row(
    *,
    run_id: str = "run_A",
    report_id: str = "r1",
    forecast_date: str = "2024-02-01",
    forecast: float = 110.0,
    actual: float = 100.0,
) -> dict:
    se = forecast - actual
    return {
        "run_id": run_id,
        "selection_run_id": run_id,
        "generated_at": pd.Timestamp("2024-01-31"),
        "training_cutoff": pd.Timestamp("2024-01-30"),
        "realized_at": pd.Timestamp("2024-02-02"),
        "report_id": report_id,
        "report_name": "Test Report",
        "selected_model_family": "seasonal_naive",
        "selected_model_name": "seasonal_naive_m7",
        "selected_m": 7,
        "forecast_date": pd.Timestamp(forecast_date),
        "horizon_step": 1,
        "forecast": forecast,
        "lower_bound": np.nan,
        "upper_bound": np.nan,
        "actual": actual,
        "signed_error": se,
        "absolute_error": abs(se),
        "squared_error": se ** 2,
        "inside_interval": np.nan,
        "interval_width": np.nan,
        "percentage_error": abs(se) / actual * 100 if actual != 0 else np.nan,
        "lineage_complete": None,
        "lineage_missing_fields": None,
    }


def _realized_df(*rows) -> pd.DataFrame:
    return pd.DataFrame(list(rows))


def _ensure_metrics_dir(root: Path) -> None:
    (root / "outputs" / "metrics").mkdir(parents=True, exist_ok=True)


def _canonical_path(root: Path) -> Path:
    return root / "outputs" / "metrics" / "realized_forecast_history.csv"


def _metrics_history_path(root: Path) -> Path:
    return root / "outputs" / "metrics" / "metrics_history.csv"


def _forecasts_history_path(root: Path) -> Path:
    return root / "outputs" / "forecasts" / "forecasts_history.csv"


# ---------------------------------------------------------------------------
# TestCsvWriteLock — unit tests for the lock primitive itself
# ---------------------------------------------------------------------------


class TestCsvWriteLock:

    def test_lock_released_after_successful_write(self, tmp_path):
        """Lock must not be held after the with-block completes normally."""
        target = tmp_path / "test.csv"
        with csv_write_lock(target):
            pass  # successful body
        # Lock must be free — a second acquisition must not block
        lock = _lock_registry.get(str(target.resolve()))
        if lock is not None:
            acquired = lock.acquire(blocking=False)
            assert acquired, "Lock was not released after successful write"
            lock.release()

    def test_lock_released_after_write_exception(self, tmp_path):
        """Lock must be released even when the with-block raises."""
        target = tmp_path / "test.csv"
        try:
            with csv_write_lock(target):
                raise RuntimeError("simulated write failure")
        except RuntimeError:
            pass

        lock = _lock_registry.get(str(target.resolve()))
        if lock is not None:
            acquired = lock.acquire(blocking=False)
            assert acquired, "Lock was not released after exception in write block"
            lock.release()

    def test_separate_paths_use_separate_locks(self, tmp_path):
        """Two different file paths must get independent locks."""
        path_a = tmp_path / "a.csv"
        path_b = tmp_path / "b.csv"
        with csv_write_lock(path_a):
            # Acquiring lock for path_b must succeed immediately (different lock)
            acquired = False
            with csv_write_lock(path_b):
                acquired = True
        assert acquired

    def test_log_write_plan_does_not_raise(self, tmp_path):
        """log_write_plan must emit without error for any valid inputs."""
        log_write_plan(
            target=tmp_path / "x.csv",
            candidate_rows=10,
            existing_rows=5,
            new_rows=7,
            skipped_rows=3,
        )


# ---------------------------------------------------------------------------
# TestThreadSafetyRealizedHistory — concurrent thread writes
# ---------------------------------------------------------------------------


class TestThreadSafetyRealizedHistory:

    def test_two_threads_do_not_corrupt_realized_history(self, tmp_path):
        """Two threads writing distinct run_ids must each append their rows exactly once."""
        from src.models.realized_forecast_history import write_realized_forecast_history

        _ensure_metrics_dir(tmp_path)

        row_a = _realized_row(run_id="thread_A", report_id="r1", forecast_date="2024-02-01")
        row_b = _realized_row(run_id="thread_B", report_id="r1", forecast_date="2024-02-01")
        df_a = _realized_df(row_a)
        df_b = _realized_df(row_b)

        errors: list[Exception] = []
        barrier = threading.Barrier(2)

        def write_a():
            try:
                barrier.wait()
                write_realized_forecast_history(df_a, tmp_path)
            except Exception as exc:
                errors.append(exc)

        def write_b():
            try:
                barrier.wait()
                write_realized_forecast_history(df_b, tmp_path)
            except Exception as exc:
                errors.append(exc)

        t1 = threading.Thread(target=write_a)
        t2 = threading.Thread(target=write_b)
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        assert not errors, f"Threads raised errors: {errors}"

        written = pd.read_csv(_canonical_path(tmp_path))
        assert len(written) == 2, f"Expected 2 rows, got {len(written)}"
        assert set(written["run_id"]) == {"thread_A", "thread_B"}

    def test_concurrent_duplicate_writes_deduplicated(self, tmp_path):
        """Two threads writing the same (run_id, report_id, forecast_date) must not duplicate."""
        from src.models.realized_forecast_history import write_realized_forecast_history

        _ensure_metrics_dir(tmp_path)
        row = _realized_row(run_id="run_X", report_id="r1", forecast_date="2024-02-01")
        df = _realized_df(row)

        errors: list[Exception] = []
        barrier = threading.Barrier(2)

        def write():
            try:
                barrier.wait()
                write_realized_forecast_history(df.copy(), tmp_path)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=write) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Threads raised: {errors}"

        written = pd.read_csv(_canonical_path(tmp_path))
        assert len(written) == 1, (
            f"Duplicate rows detected: expected 1, got {len(written)}"
        )

    def test_thread_safety_append_metrics_history(self, tmp_path):
        """Two threads writing distinct run_ids to metrics_history must not corrupt the file."""
        from src.pipelines.run_forecasting_pipeline import append_metrics_history

        (tmp_path / "outputs" / "metrics").mkdir(parents=True, exist_ok=True)

        metrics_a = pd.DataFrame([{"report_id": "r1", "mase": 0.9}])
        metrics_b = pd.DataFrame([{"report_id": "r1", "mase": 1.0}])
        ts = pd.Timestamp("2024-02-01")

        errors: list[Exception] = []
        barrier = threading.Barrier(2)

        def write_a():
            try:
                barrier.wait()
                append_metrics_history(metrics_a, tmp_path, "run_A", ts)
            except Exception as exc:
                errors.append(exc)

        def write_b():
            try:
                barrier.wait()
                append_metrics_history(metrics_b, tmp_path, "run_B", ts)
            except Exception as exc:
                errors.append(exc)

        t1 = threading.Thread(target=write_a)
        t2 = threading.Thread(target=write_b)
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)

        assert not errors, f"Threads raised: {errors}"

        written = pd.read_csv(_metrics_history_path(tmp_path))
        assert len(written) == 2
        assert set(written["run_id"]) == {"run_A", "run_B"}


# ---------------------------------------------------------------------------
# TestSequentialIdempotency — repeated sequential calls
# ---------------------------------------------------------------------------


class TestSequentialIdempotency:

    def test_sequential_writes_same_key_deduplicated(self, tmp_path):
        """Three sequential writes of the same row produce exactly one row."""
        from src.models.realized_forecast_history import write_realized_forecast_history

        _ensure_metrics_dir(tmp_path)
        row = _realized_row(run_id="run_A", report_id="r1", forecast_date="2024-02-01")
        df = _realized_df(row)

        write_realized_forecast_history(df.copy(), tmp_path)
        write_realized_forecast_history(df.copy(), tmp_path)
        write_realized_forecast_history(df.copy(), tmp_path)

        written = pd.read_csv(_canonical_path(tmp_path))
        assert len(written) == 1

    def test_sequential_writes_distinct_keys_accumulated(self, tmp_path):
        """Three sequential writes of distinct keys produce exactly three rows."""
        from src.models.realized_forecast_history import write_realized_forecast_history

        _ensure_metrics_dir(tmp_path)
        for i, date in enumerate(["2024-02-01", "2024-02-02", "2024-02-03"]):
            row = _realized_row(run_id="run_A", report_id="r1", forecast_date=date)
            write_realized_forecast_history(_realized_df(row), tmp_path)

        written = pd.read_csv(_canonical_path(tmp_path))
        assert len(written) == 3

    def test_append_metrics_history_sequential_distinct(self, tmp_path):
        """Two sequential append_metrics_history calls with different run_ids accumulate."""
        from src.pipelines.run_forecasting_pipeline import append_metrics_history

        (tmp_path / "outputs" / "metrics").mkdir(parents=True, exist_ok=True)
        ts = pd.Timestamp("2024-02-01")
        metrics = pd.DataFrame([{"report_id": "r1", "mase": 0.9}])

        append_metrics_history(metrics.copy(), tmp_path, "run_1", ts)
        append_metrics_history(metrics.copy(), tmp_path, "run_2", ts)

        written = pd.read_csv(_metrics_history_path(tmp_path))
        assert len(written) == 2

    def test_append_only_no_overwrite(self, tmp_path):
        """Output is strictly append-only: first row must still be present after later writes."""
        from src.models.realized_forecast_history import write_realized_forecast_history

        _ensure_metrics_dir(tmp_path)
        row1 = _realized_row(run_id="run_A", report_id="r1", forecast_date="2024-02-01",
                             forecast=100.0, actual=90.0)
        row2 = _realized_row(run_id="run_A", report_id="r1", forecast_date="2024-02-02",
                             forecast=200.0, actual=180.0)

        write_realized_forecast_history(_realized_df(row1), tmp_path)
        write_realized_forecast_history(_realized_df(row2), tmp_path)

        written = pd.read_csv(_canonical_path(tmp_path))
        assert len(written) == 2
        # First row's forecast must still be 100 (not overwritten)
        first = written[written["forecast_date"] == "2024-02-01"]
        assert first["forecast"].iloc[0] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# TestNoDualUpdaterInvocation — pipeline guard
# ---------------------------------------------------------------------------


class TestNoDualUpdaterInvocation:

    def test_run_production_pipeline_does_not_call_update_realized_errors(self):
        """run_production_pipeline must NOT invoke update_realized_errors.

        The legacy path (run_pipeline) still calls update_realized_errors, but
        run_production_pipeline uses update_realized_forecast_history exclusively.
        """
        import inspect
        import src.pipelines.run_forecasting_pipeline as pipe_mod

        source = inspect.getsource(pipe_mod.run_production_pipeline)
        assert "update_realized_errors(" not in source, (
            "run_production_pipeline must not call update_realized_errors(); "
            "use update_realized_forecast_history() instead."
        )

    def test_run_pipeline_does_not_call_update_realized_forecast_history(self):
        """The legacy run_pipeline path must NOT call update_realized_forecast_history.

        Calling both in the same orchestration session would double-count rows.
        """
        import inspect
        import src.pipelines.run_forecasting_pipeline as pipe_mod

        source = inspect.getsource(pipe_mod.run_pipeline)
        assert "update_realized_forecast_history" not in source, (
            "run_pipeline (legacy) must not call update_realized_forecast_history(); "
            "the two updaters must not run together in one session."
        )


# ---------------------------------------------------------------------------
# TestDocstringPolicy — documentation contract
# ---------------------------------------------------------------------------


class TestDocstringPolicy:

    def _get_source(self, module_path: str, func_name: str) -> str:
        import importlib
        import inspect
        mod = importlib.import_module(module_path)
        func = getattr(mod, func_name)
        return inspect.getdoc(func) or ""

    def test_append_forecasts_history_mentions_single_writer(self):
        doc = self._get_source(
            "src.pipelines.run_forecasting_pipeline", "append_forecasts_history"
        )
        assert "single active pipeline writer" in doc.lower() or \
               "single-writer" in doc.lower(), (
            "append_forecasts_history docstring must state the single-writer assumption"
        )

    def test_append_metrics_history_mentions_single_writer(self):
        doc = self._get_source(
            "src.pipelines.run_forecasting_pipeline", "append_metrics_history"
        )
        assert "single active pipeline writer" in doc.lower() or \
               "single-writer" in doc.lower(), (
            "append_metrics_history docstring must state the single-writer assumption"
        )

    def test_write_realized_forecast_history_mentions_single_writer(self):
        doc = self._get_source(
            "src.models.realized_forecast_history", "write_realized_forecast_history"
        )
        assert "single active pipeline writer" in doc.lower() or \
               "single-writer" in doc.lower(), (
            "write_realized_forecast_history docstring must state the single-writer assumption"
        )

    def test_migrate_legacy_mentions_single_writer(self):
        doc = self._get_source(
            "src.models.realized_forecast_history", "migrate_legacy_realized_errors"
        )
        assert "single active pipeline writer" in doc.lower() or \
               "single-writer" in doc.lower(), (
            "migrate_legacy_realized_errors docstring must state the single-writer assumption"
        )

    def test_no_claim_of_cross_process_safety_in_forecasts_history(self):
        doc = self._get_source(
            "src.pipelines.run_forecasting_pipeline", "append_forecasts_history"
        )
        # Must explicitly disclaim cross-process safety
        assert "not coordinated" in doc.lower() or \
               "separate os processes are not" in doc.lower() or \
               "separate" in doc.lower(), (
            "append_forecasts_history must not claim cross-process safety"
        )

    def test_no_claim_of_cross_process_safety_in_metrics_history(self):
        doc = self._get_source(
            "src.pipelines.run_forecasting_pipeline", "append_metrics_history"
        )
        assert "not coordinated" in doc.lower() or \
               "separate" in doc.lower(), (
            "append_metrics_history must not claim cross-process safety"
        )

    def test_no_claim_of_cross_process_safety_in_realized_history(self):
        doc = self._get_source(
            "src.models.realized_forecast_history", "write_realized_forecast_history"
        )
        assert "not coordinated" in doc.lower() or \
               "separate" in doc.lower(), (
            "write_realized_forecast_history must not claim cross-process safety"
        )

    def test_module_docstring_mentions_single_writer(self):
        import src.models.realized_forecast_history as mod
        doc = mod.__doc__ or ""
        assert "single active pipeline writer" in doc.lower() or \
               "single-writer" in doc.lower(), (
            "realized_forecast_history module docstring must state the single-writer assumption"
        )

    def test_csv_lock_module_docstring_mentions_not_cross_process(self):
        import src.persistence._csv_lock as mod
        doc = mod.__doc__ or ""
        assert "not" in doc.lower() and (
            "cross-process" in doc.lower() or "separate" in doc.lower()
        ), (
            "_csv_lock module docstring must clarify it does NOT coordinate across processes"
        )

    def test_readme_has_limitations_section(self):
        readme = Path(
            "/Users/masegomodibane/Documents/GitHub/Data Science Projects /"
            "Forecasting Report Usage/GitHub Final Version/"
            "report-usage-forecasting/README.md"
        )
        text = readme.read_text()
        assert "concurrency" in text.lower() or "limitations" in text.lower(), (
            "README must contain a concurrency or limitations section"
        )
        assert "not transactional" in text.lower() or "not coordinated" in text.lower(), (
            "README limitations section must state that CSV writes are not transactional "
            "or that separate processes are not coordinated"
        )
