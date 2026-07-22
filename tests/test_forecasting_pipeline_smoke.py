"""Smoke test: forecasting pipeline can consume mart_report_daily_series.

Requirement 12 — add a smoke test confirming the pipeline can forecast
from the canonical narrow daily series.

Uses a minimal deterministic fixture written to a tmp_path so no repo-level
processed CSV files are needed.
"""
from __future__ import annotations

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Minimal fixture
# ---------------------------------------------------------------------------

_START = pd.Timestamp("2023-01-01")
_END   = pd.Timestamp("2023-12-31")

def _make_daily_series(report_id: str, seed_views: int) -> pd.DataFrame:
    """Return a full-year contiguous zero-filled daily series for one report."""
    dates = pd.date_range(_START, _END, freq="D")
    import numpy as np
    rng = np.random.default_rng(abs(hash(report_id)) % (2 ** 32))
    views = (seed_views + rng.integers(-2, 3, size=len(dates))).clip(0)
    # Introduce a handful of zero-view days
    zero_idx = rng.choice(len(dates), size=max(1, len(dates) // 20), replace=False)
    views[zero_idx] = 0
    return pd.DataFrame({
        "report_id": report_id,
        "date": dates,
        "daily_views": views.astype(int),
        "is_observed_day": (views > 0).astype(int),
        "is_imputed_zero": (views == 0).astype(int),
    })


@pytest.fixture(scope="module")
def series_csv(tmp_path_factory):
    """Write mart_report_daily_series.csv to a tmp_path and return its directory."""
    d = tmp_path_factory.mktemp("processed")
    frames = [_make_daily_series(f"R_{i:03d}", seed_views=10 * (i + 1)) for i in range(3)]
    daily = pd.concat(frames, ignore_index=True)
    daily.to_csv(d / "mart_report_daily_series.csv", index=False)
    return d, daily


# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------

class TestForecastingPipelineSmoke:
    """Verifies the load → validate → adapt boundary with the canonical source."""

    def test_canonical_loader_finds_series_csv(self, series_csv):
        """load_canonical_daily_series must load mart_report_daily_series.csv."""
        from src.pipelines.run_forecasting_pipeline import load_canonical_daily_series
        d, _ = series_csv
        df = load_canonical_daily_series(d)
        assert set(["report_id", "date", "daily_views"]).issubset(df.columns)

    def test_canonical_loader_strips_to_core_columns(self, series_csv):
        """Canonical loader returns at least [date, report_id, daily_views]."""
        from src.pipelines.run_forecasting_pipeline import load_canonical_daily_series
        d, _ = series_csv
        df = load_canonical_daily_series(d)
        assert {"date", "report_id", "daily_views"}.issubset(set(df.columns))

    def test_validate_passes_on_clean_series(self, series_csv):
        """validate_forecasting_series_input should not raise on the clean fixture."""
        import pandas as pd
        from src.pipelines.run_forecasting_pipeline import (
            load_canonical_daily_series,
            validate_forecasting_series_input,
        )
        d, _ = series_csv
        df = load_canonical_daily_series(d)
        validate_forecasting_series_input(df)  # must not raise

    def test_validate_raises_on_null_target(self, series_csv, tmp_path):
        """validate_forecasting_series_input must raise on null daily_views."""
        import pandas as pd
        from src.pipelines.run_forecasting_pipeline import validate_forecasting_series_input
        d, daily = series_csv
        bad = daily[["report_id", "date", "daily_views"]].copy()
        bad.loc[bad.index[0], "daily_views"] = float("nan")
        with pytest.raises(ValueError, match="null daily_views"):
            validate_forecasting_series_input(bad)

    def test_validate_raises_on_duplicate_pairs(self, series_csv):
        """validate_forecasting_series_input must raise on duplicate (report_id, date)."""
        import pandas as pd
        from src.pipelines.run_forecasting_pipeline import validate_forecasting_series_input
        d, daily = series_csv
        dup = pd.concat([daily[["report_id", "date", "daily_views"]],
                         daily[["report_id", "date", "daily_views"]].head(1)],
                        ignore_index=True)
        with pytest.raises(ValueError, match="duplicate"):
            validate_forecasting_series_input(dup)

    def test_zero_view_days_preserved_by_loader(self, series_csv):
        """Zero-view days in the canonical file must be returned unchanged."""
        import pandas as pd
        from src.pipelines.run_forecasting_pipeline import load_canonical_daily_series
        d, daily = series_csv
        df = load_canonical_daily_series(d)
        zero_in_fixture = int((daily["daily_views"] == 0).sum())
        zero_in_loaded = int((df["daily_views"] == 0).sum())
        assert zero_in_loaded == zero_in_fixture, (
            f"Zero-view rows changed: fixture had {zero_in_fixture}, loader returned {zero_in_loaded}"
        )

    def test_adapt_produces_legacy_schema(self, series_csv):
        """adapt_to_forecasting_schema must return the internal legacy column set."""
        import pandas as pd
        from src.pipelines.run_forecasting_pipeline import (
            load_canonical_daily_series,
            adapt_to_forecasting_schema,
        )
        d, _ = series_csv
        df = load_canonical_daily_series(d)
        model_input = adapt_to_forecasting_schema(
            df[["date", "report_id", "daily_views"]], d / "dim_report.csv"
        )
        expected_cols = {"Date", "Report Guid", "Report Name", "User Id", "Occurrences"}
        assert expected_cols.issubset(set(model_input.columns))

    def test_no_engagement_columns_in_model_input(self, series_csv):
        """No engagement or rolling-feature columns must appear in adapt output."""
        import pandas as pd
        from src.pipelines.run_forecasting_pipeline import (
            load_canonical_daily_series,
            adapt_to_forecasting_schema,
        )
        d, _ = series_csv
        df = load_canonical_daily_series(d)
        model_input = adapt_to_forecasting_schema(
            df[["date", "report_id", "daily_views"]], d / "dim_report.csv"
        )
        forbidden = {
            "top_1_user_view_share", "top_10pct_user_share", "repeat_user_rate",
            "avg_load_time", "views_7d", "views_28d", "is_observed_day", "is_imputed_zero",
        }
        leaked = forbidden & set(model_input.columns)
        assert not leaked, f"Engagement/feature columns leaked into model_input: {leaked}"


# ---------------------------------------------------------------------------
# Sprint 1 fix — forecasting layer must not manufacture missing dates
# ---------------------------------------------------------------------------

def _make_adapted_input(
    report_id: str,
    dates: "list[pd.Timestamp]",
    views: "list[int]",
) -> "pd.DataFrame":
    """Build a minimal internal-schema frame (output of adapt_to_forecasting_schema)."""
    return pd.DataFrame({
        "Date": dates,
        "Report Guid": report_id,
        "Report Name": f"{report_id} name",
        "User Id": "PROCESSED_DAILY_TOTAL",
        "Occurrences": views,
    })


class TestBuildDailySeriesAdherence:
    """Prove build_daily_series_for_all_reports treats the input as authoritative."""

    def test_existing_zero_day_remains_zero(self):
        """A zero-view day in the source series must not be altered."""
        from src.pipelines.run_forecasting_pipeline import build_daily_series_for_all_reports
        dates = pd.date_range("2025-01-01", periods=5, freq="D").tolist()
        views = [10, 0, 8, 0, 12]   # two genuine zero days
        df = _make_adapted_input("R_ZERO", dates, views)
        series_dict, _, _ = build_daily_series_for_all_reports(df)
        s = series_dict["R_ZERO"]
        assert s.iloc[1] == 0, "zero on day 2 was altered"
        assert s.iloc[3] == 0, "zero on day 4 was altered"

    def test_missing_date_raises_error(self):
        """A gap in the standardised frame must raise ValueError before fitting."""
        from src.pipelines.run_forecasting_pipeline import validate_forecasting_series_input
        # Days 1, 2, 4, 5 — day 3 is missing
        dates = [
            pd.Timestamp("2025-01-01"),
            pd.Timestamp("2025-01-02"),
            pd.Timestamp("2025-01-04"),   # gap here
            pd.Timestamp("2025-01-05"),
        ]
        df = pd.DataFrame({
            "date": dates,
            "report_id": "R_GAP",
            "daily_views": [5, 3, 7, 2],
        })
        with pytest.raises(ValueError) as exc_info:
            validate_forecasting_series_input(df)
        msg = str(exc_info.value)
        assert "R_GAP" in msg, "error must identify the report"
        assert "2025-01-03" in msg, "error must state the expected missing date"
        assert "2025-01-02" in msg, "error must show the date before the gap"
        assert "2025-01-04" in msg, "error must show the date after the gap"

    def test_no_rows_added_by_forecasting_pipeline(self):
        """Row count out of build_daily_series_for_all_reports must equal row count in."""
        from src.pipelines.run_forecasting_pipeline import build_daily_series_for_all_reports
        dates = pd.date_range("2025-03-01", periods=7, freq="D").tolist()
        views = [4, 0, 6, 2, 0, 8, 3]
        df = _make_adapted_input("R_COUNT", dates, views)
        series_dict, _, provenance = build_daily_series_for_all_reports(df)
        assert len(series_dict["R_COUNT"]) == 7, (
            f"Expected 7 rows, got {len(series_dict['R_COUNT'])} — "
            "pipeline must not manufacture rows"
        )
        assert provenance["R_COUNT"]["n_obs"] == 7

    def test_valid_continuous_series_passes_unchanged(self):
        """A clean continuous series must survive validate + build with identical values."""
        from src.pipelines.run_forecasting_pipeline import (
            build_daily_series_for_all_reports,
            validate_forecasting_series_input,
        )
        dates = pd.date_range("2025-06-01", periods=10, freq="D")
        views_in = [5, 0, 3, 7, 0, 9, 1, 4, 0, 6]

        std = pd.DataFrame({
            "date": dates,
            "report_id": "R_VALID",
            "daily_views": views_in,
        })
        validate_forecasting_series_input(std)  # must not raise

        df_adapted = _make_adapted_input("R_VALID", dates.tolist(), views_in)
        series_dict, _, _ = build_daily_series_for_all_reports(df_adapted)
        s = series_dict["R_VALID"]
        assert list(s.values) == views_in, (
            f"Values changed in transit: {list(s.values)} != {views_in}"
        )
        assert len(s) == 10


# ---------------------------------------------------------------------------
# Strict data contract — load_canonical_daily_series
# ---------------------------------------------------------------------------

class TestStrictForecastingInput:
    """load_canonical_daily_series must accept only the one canonical file."""

    def test_canonical_file_loads_successfully(self, tmp_path):
        processed = tmp_path / "processed"
        processed.mkdir()
        pd.DataFrame({
            "report_id": ["R1", "R1"],
            "date": ["2025-01-01", "2025-01-02"],
            "daily_views": [5, 3],
        }).to_csv(processed / "mart_report_daily_series.csv", index=False)
        from src.pipelines.run_forecasting_pipeline import load_canonical_daily_series
        df = load_canonical_daily_series(processed)
        assert set(["report_id", "date", "daily_views"]).issubset(df.columns)
        assert len(df) == 2

    def test_absent_canonical_file_raises_file_not_found(self, tmp_path):
        processed = tmp_path / "processed"
        processed.mkdir()
        from src.pipelines.run_forecasting_pipeline import load_canonical_daily_series
        with pytest.raises(FileNotFoundError, match="mart_report_daily_series"):
            load_canonical_daily_series(processed)

    def test_alternate_fallback_file_is_ignored(self, tmp_path):
        """Canonical absent; only a fallback CSV exists — must still raise."""
        processed = tmp_path / "processed"
        processed.mkdir()
        pd.DataFrame({
            "report_id": ["R1"],
            "date": ["2025-01-01"],
            "daily_views": [5],
        }).to_csv(processed / "mart_report_daily_context.csv", index=False)
        from src.pipelines.run_forecasting_pipeline import load_canonical_daily_series
        with pytest.raises(FileNotFoundError, match="mart_report_daily_series"):
            load_canonical_daily_series(processed)

    def test_invalid_schema_raises_clear_error(self, tmp_path):
        processed = tmp_path / "processed"
        processed.mkdir()
        pd.DataFrame({"wrong_col": [1], "another": [2]}).to_csv(
            processed / "mart_report_daily_series.csv", index=False
        )
        from src.pipelines.run_forecasting_pipeline import load_canonical_daily_series
        with pytest.raises(ValueError, match="missing required columns"):
            load_canonical_daily_series(processed)
