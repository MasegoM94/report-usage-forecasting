"""Tests for migrate_legacy_realized_errors.

Covers all 9 required scenarios:
  1. Legacy file absent → early return, no files created
  2. Legacy file present, canonical absent → full migration
  3. Legacy file present, canonical absent, lineage missing → flags populated
  4. Legacy file present, canonical already populated → dedup, only new rows appended
  5. Duplicate rows skipped (row already in canonical)
  6. Missing lineage preserved as null, never invented
  7. Backup created (timestamped .bak alongside legacy file)
  8. Legacy file archived to outputs/archive/
  9. Second migration call makes no changes (idempotent)

Also verifies:
  - sign flip (legacy error = actual−forecast → canonical signed_error = forecast−actual)
  - absolute_error / squared_error recomputed after sign flip
  - lineage_complete=True only when all _LINEAGE_COLS non-null
  - lineage_missing_fields is JSON list of absent column names
  - canonical file schema matches REALIZED_FORECAST_HISTORY_COLS exactly
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.realized_forecast_history import (
    REALIZED_FORECAST_HISTORY_COLS,
    _LINEAGE_COLS,
    migrate_legacy_realized_errors,
)

# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

_LEGACY_RELPATH  = Path("outputs") / "metrics" / "realized_errors_history.csv"
_CANON_RELPATH   = Path("outputs") / "metrics" / "realized_forecast_history.csv"
_ARCHIVE_RELPATH = Path("outputs") / "archive"


def _legacy_path(root: Path) -> Path:
    return root / _LEGACY_RELPATH


def _canon_path(root: Path) -> Path:
    return root / _CANON_RELPATH


def _archive_dir(root: Path) -> Path:
    return root / _ARCHIVE_RELPATH


def _ensure_dirs(root: Path) -> None:
    (root / "outputs" / "metrics").mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------

def _legacy_row(
    *,
    run_id: str = "run_A",
    report_id: str = "r1",
    target_date: str = "2024-01-15",
    forecast_views: float = 120.0,
    actual_views: float = 100.0,
    lower_ci: float | None = None,
    upper_ci: float | None = None,
    model_name: str | None = None,
) -> dict:
    """Minimal legacy-schema row (as written by append_forecasts_history)."""
    # legacy error = actual − forecast
    error = actual_views - forecast_views
    return {
        "run_id": run_id,
        "report_id": report_id,
        "target_date": target_date,
        "forecast_views": forecast_views,
        "actual_views": actual_views,
        "error": error,
        "abs_error": abs(error),
        "lower_ci": lower_ci,
        "upper_ci": upper_ci,
        "model_name": model_name,
    }


def _write_legacy(root: Path, rows: list[dict]) -> Path:
    path = _legacy_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_canonical(root: Path, rows: list[dict]) -> Path:
    path = _canon_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    for col in REALIZED_FORECAST_HISTORY_COLS:
        if col not in df.columns:
            df[col] = np.nan
    df[REALIZED_FORECAST_HISTORY_COLS].to_csv(path, index=False)
    return path


def _canonical_row(
    *,
    run_id: str = "run_A",
    report_id: str = "r1",
    forecast_date: str = "2024-01-15",
    forecast: float = 120.0,
    actual: float = 100.0,
) -> dict:
    signed_error = forecast - actual
    return {
        "run_id": run_id,
        "selection_run_id": run_id,
        "generated_at": "2024-01-01",
        "training_cutoff": "2024-01-14",
        "realized_at": "2024-01-16",
        "report_id": report_id,
        "report_name": "Test Report",
        "selected_model_family": "seasonal_naive",
        "selected_model_name": "seasonal_naive_m7",
        "selected_m": 7,
        "forecast_date": forecast_date,
        "horizon_step": 1,
        "forecast": forecast,
        "lower_bound": np.nan,
        "upper_bound": np.nan,
        "actual": actual,
        "signed_error": signed_error,
        "absolute_error": abs(signed_error),
        "squared_error": signed_error ** 2,
        "inside_interval": np.nan,
        "interval_width": np.nan,
        "percentage_error": abs(signed_error) / actual * 100 if actual != 0 else np.nan,
        "lineage_complete": None,
        "lineage_missing_fields": None,
    }


# ---------------------------------------------------------------------------
# Scenario 1: Legacy file absent
# ---------------------------------------------------------------------------

class TestLegacyFileAbsent:

    def test_returns_no_legacy_file_status(self, tmp_path):
        _ensure_dirs(tmp_path)
        result = migrate_legacy_realized_errors(tmp_path)
        assert result["status"] == "no_legacy_file"

    def test_no_files_created(self, tmp_path):
        _ensure_dirs(tmp_path)
        migrate_legacy_realized_errors(tmp_path)
        assert not _canon_path(tmp_path).exists()
        assert not _archive_dir(tmp_path).exists()

    def test_zero_rows(self, tmp_path):
        result = migrate_legacy_realized_errors(tmp_path)
        assert result["rows_migrated"] == 0
        assert result["rows_skipped"] == 0
        assert result["backup_path"] is None
        assert result["archive_path"] is None


# ---------------------------------------------------------------------------
# Scenario 2: Legacy present, canonical absent → full migration
# ---------------------------------------------------------------------------

class TestFullMigration:

    def test_canonical_file_created(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row()])
        migrate_legacy_realized_errors(tmp_path)
        assert _canon_path(tmp_path).exists()

    def test_canonical_schema_matches(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row()])
        migrate_legacy_realized_errors(tmp_path)
        df = pd.read_csv(_canon_path(tmp_path))
        assert list(df.columns) == REALIZED_FORECAST_HISTORY_COLS

    def test_one_row_migrated(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row()])
        result = migrate_legacy_realized_errors(tmp_path)
        assert result["rows_migrated"] == 1
        assert result["rows_skipped"] == 0
        assert result["status"] == "migrated"

    def test_sign_flip_signed_error(self, tmp_path):
        """Legacy error = actual−forecast = −20. Canonical = forecast−actual = +20."""
        _write_legacy(tmp_path, [_legacy_row(forecast_views=120, actual_views=100)])
        migrate_legacy_realized_errors(tmp_path)
        df = pd.read_csv(_canon_path(tmp_path))
        assert df["signed_error"].iloc[0] == pytest.approx(20.0)

    def test_absolute_error_recomputed(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row(forecast_views=120, actual_views=100)])
        migrate_legacy_realized_errors(tmp_path)
        df = pd.read_csv(_canon_path(tmp_path))
        assert df["absolute_error"].iloc[0] == pytest.approx(20.0)

    def test_squared_error_recomputed(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row(forecast_views=120, actual_views=100)])
        migrate_legacy_realized_errors(tmp_path)
        df = pd.read_csv(_canon_path(tmp_path))
        assert df["squared_error"].iloc[0] == pytest.approx(400.0)

    def test_forecast_column_renamed(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row(forecast_views=120, actual_views=100)])
        migrate_legacy_realized_errors(tmp_path)
        df = pd.read_csv(_canon_path(tmp_path))
        assert df["forecast"].iloc[0] == pytest.approx(120.0)
        assert df["actual"].iloc[0] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Scenario 3: Missing lineage → lineage flags populated
# ---------------------------------------------------------------------------

class TestMissingLineagePreserved:

    def test_lineage_complete_false_when_run_id_missing(self, tmp_path):
        row = _legacy_row()
        row.pop("run_id")  # simulate missing run_id
        _write_legacy(tmp_path, [row])
        migrate_legacy_realized_errors(tmp_path)
        df = pd.read_csv(_canon_path(tmp_path))
        # lineage_complete should be False (stored as bool or 0)
        val = df["lineage_complete"].iloc[0]
        assert str(val).lower() in ("false", "0"), f"Expected False, got {val!r}"

    def test_lineage_missing_fields_lists_absent_columns(self, tmp_path):
        row = _legacy_row()
        row.pop("run_id")
        _write_legacy(tmp_path, [row])
        migrate_legacy_realized_errors(tmp_path)
        df = pd.read_csv(_canon_path(tmp_path))
        missing_json = df["lineage_missing_fields"].iloc[0]
        missing = json.loads(missing_json)
        assert "run_id" in missing

    def test_missing_lineage_not_invented(self, tmp_path):
        """run_id must remain null — must not be filled with a placeholder."""
        row = _legacy_row()
        row.pop("run_id")
        _write_legacy(tmp_path, [row])
        migrate_legacy_realized_errors(tmp_path)
        df = pd.read_csv(_canon_path(tmp_path))
        assert pd.isna(df["run_id"].iloc[0])

    def test_lineage_complete_true_when_all_present(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row(run_id="run_A", model_name="snaive_m7")])
        migrate_legacy_realized_errors(tmp_path)
        df = pd.read_csv(_canon_path(tmp_path))
        # run_id is present in legacy schema so it maps through; but
        # selected_model_family etc. are absent → lineage_complete=False
        val = df["lineage_complete"].iloc[0]
        # Legacy rows always lack selected_model_family, so should be False
        assert str(val).lower() in ("false", "0")

    def test_lineage_missing_fields_null_for_complete_rows(self, tmp_path):
        """A row complete in all _LINEAGE_COLS must have lineage_missing_fields=null."""
        # Build a row that has all _LINEAGE_COLS populated
        row = {col: f"val_{col}" for col in _LINEAGE_COLS}
        row["target_date"] = "2024-01-15"
        row["forecast_views"] = 100.0
        row["actual_views"] = 90.0
        row["error"] = -10.0
        row["abs_error"] = 10.0
        _write_legacy(tmp_path, [row])
        migrate_legacy_realized_errors(tmp_path)
        df = pd.read_csv(_canon_path(tmp_path))
        val = df["lineage_complete"].iloc[0]
        assert str(val).lower() in ("true", "1"), f"Expected True, got {val!r}"
        assert pd.isna(df["lineage_missing_fields"].iloc[0])


# ---------------------------------------------------------------------------
# Scenario 4 + 5: Canonical already populated → dedup
# ---------------------------------------------------------------------------

class TestDeduplicationIntoExistingCanonical:

    def test_existing_rows_not_duplicated(self, tmp_path):
        """Row already in canonical must not appear twice after migration."""
        legacy = _legacy_row(run_id="run_A", report_id="r1", target_date="2024-01-15")
        _write_legacy(tmp_path, [legacy])

        # Seed canonical with same run_id / report_id / forecast_date
        canon = _canonical_row(run_id="run_A", report_id="r1", forecast_date="2024-01-15")
        _write_canonical(tmp_path, [canon])

        result = migrate_legacy_realized_errors(tmp_path)
        df = pd.read_csv(_canon_path(tmp_path))
        assert len(df) == 1, f"Expected 1 row, got {len(df)}"
        assert result["rows_skipped"] == 1
        assert result["rows_migrated"] == 0

    def test_new_rows_appended_existing_kept(self, tmp_path):
        """A legacy row with a different date is appended; canonical row is kept."""
        legacy = _legacy_row(run_id="run_A", report_id="r1", target_date="2024-01-16")
        _write_legacy(tmp_path, [legacy])

        canon = _canonical_row(run_id="run_A", report_id="r1", forecast_date="2024-01-15")
        _write_canonical(tmp_path, [canon])

        result = migrate_legacy_realized_errors(tmp_path)
        df = pd.read_csv(_canon_path(tmp_path))
        assert len(df) == 2
        assert result["rows_migrated"] == 1
        assert result["rows_skipped"] == 0

    def test_multiple_rows_partial_overlap(self, tmp_path):
        """2 legacy rows, 1 already in canonical, 1 new → 1 appended, 1 skipped."""
        legacy_rows = [
            _legacy_row(run_id="run_A", report_id="r1", target_date="2024-01-15"),
            _legacy_row(run_id="run_A", report_id="r1", target_date="2024-01-16"),
        ]
        _write_legacy(tmp_path, legacy_rows)

        canon = _canonical_row(run_id="run_A", report_id="r1", forecast_date="2024-01-15")
        _write_canonical(tmp_path, [canon])

        result = migrate_legacy_realized_errors(tmp_path)
        df = pd.read_csv(_canon_path(tmp_path))
        assert len(df) == 2
        assert result["rows_migrated"] == 1
        assert result["rows_skipped"] == 1


# ---------------------------------------------------------------------------
# Scenario 7: Backup created
# ---------------------------------------------------------------------------

class TestBackupCreated:

    def test_backup_file_exists(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row()])
        result = migrate_legacy_realized_errors(tmp_path, backup=True)
        assert result["backup_path"] is not None
        assert result["backup_path"].exists()

    def test_backup_name_contains_timestamp(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row()])
        result = migrate_legacy_realized_errors(tmp_path, backup=True)
        name = result["backup_path"].name
        assert "realized_errors_history_" in name
        assert name.endswith(".csv.bak")

    def test_backup_contents_match_original(self, tmp_path):
        """Backup must be an exact copy of the original legacy file."""
        legacy_path = _write_legacy(tmp_path, [_legacy_row()])
        original_text = legacy_path.read_text()
        result = migrate_legacy_realized_errors(tmp_path, backup=True)
        # legacy is now archived, but backup still exists
        assert result["backup_path"].read_text() == original_text

    def test_no_backup_when_disabled(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row()])
        result = migrate_legacy_realized_errors(tmp_path, backup=False)
        assert result["backup_path"] is None


# ---------------------------------------------------------------------------
# Scenario 8: Legacy file archived
# ---------------------------------------------------------------------------

class TestLegacyFileArchived:

    def test_legacy_file_removed_from_metrics(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row()])
        migrate_legacy_realized_errors(tmp_path)
        assert not _legacy_path(tmp_path).exists()

    def test_archive_file_exists(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row()])
        result = migrate_legacy_realized_errors(tmp_path)
        assert result["archive_path"] is not None
        assert result["archive_path"].exists()

    def test_archive_path_in_outputs_archive(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row()])
        result = migrate_legacy_realized_errors(tmp_path)
        assert result["archive_path"].parent == _archive_dir(tmp_path)

    def test_archive_name_contains_timestamp(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row()])
        result = migrate_legacy_realized_errors(tmp_path)
        name = result["archive_path"].name
        assert "realized_errors_history_" in name
        assert name.endswith(".csv")


# ---------------------------------------------------------------------------
# Scenario 9: Second migration call makes no changes (idempotent)
# ---------------------------------------------------------------------------

class TestIdempotent:

    def test_second_call_returns_no_legacy_file(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row()])
        migrate_legacy_realized_errors(tmp_path)
        # Second call — legacy is archived, so should return no_legacy_file
        result2 = migrate_legacy_realized_errors(tmp_path)
        assert result2["status"] == "no_legacy_file"

    def test_second_call_does_not_change_canonical(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row()])
        migrate_legacy_realized_errors(tmp_path)
        df_after_first = pd.read_csv(_canon_path(tmp_path))

        # Canonical unchanged, legacy gone → second call is a no-op
        migrate_legacy_realized_errors(tmp_path)
        df_after_second = pd.read_csv(_canon_path(tmp_path))
        pd.testing.assert_frame_equal(df_after_first, df_after_second)

    def test_second_call_zero_rows(self, tmp_path):
        _write_legacy(tmp_path, [_legacy_row()])
        migrate_legacy_realized_errors(tmp_path)
        result2 = migrate_legacy_realized_errors(tmp_path)
        assert result2["rows_migrated"] == 0
        assert result2["rows_skipped"] == 0

    def test_multiple_legacy_rows_idempotent(self, tmp_path):
        """3 rows migrated on first call; zero on second."""
        rows = [
            _legacy_row(run_id="run_A", report_id="r1", target_date="2024-01-15"),
            _legacy_row(run_id="run_A", report_id="r1", target_date="2024-01-16"),
            _legacy_row(run_id="run_A", report_id="r2", target_date="2024-01-15"),
        ]
        _write_legacy(tmp_path, rows)
        result1 = migrate_legacy_realized_errors(tmp_path)
        assert result1["rows_migrated"] == 3

        result2 = migrate_legacy_realized_errors(tmp_path)
        assert result2["rows_migrated"] == 0
        assert result2["rows_skipped"] == 0
