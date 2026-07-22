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
    """Verifies the standardise → validate → adapt boundary with the new source."""

    def test_choose_forecasting_input_finds_series_csv(self, series_csv):
        """choose_forecasting_input should prefer mart_report_daily_series.csv."""
        from src.pipelines.run_forecasting_pipeline import choose_forecasting_input
        d, _ = series_csv
        chosen = choose_forecasting_input(d)
        assert chosen.name == "mart_report_daily_series.csv"

    def test_standardise_strips_to_three_columns(self, series_csv):
        """After standardise, only [date, report_id, daily_views] remain."""
        from src.pipelines.run_forecasting_pipeline import (
            choose_forecasting_input,
            standardise_forecasting_columns,
        )
        d, _ = series_csv
        raw = __import__("pandas").read_csv(choose_forecasting_input(d))
        std = standardise_forecasting_columns(raw, choose_forecasting_input(d), d / "dim_date.csv")
        assert set(std.columns) == {"date", "report_id", "daily_views"}

    def test_validate_passes_on_clean_series(self, series_csv):
        """validate_forecasting_series_input should not raise on the clean fixture."""
        import pandas as pd
        from src.pipelines.run_forecasting_pipeline import (
            choose_forecasting_input,
            standardise_forecasting_columns,
            validate_forecasting_series_input,
        )
        d, _ = series_csv
        raw = pd.read_csv(choose_forecasting_input(d))
        std = standardise_forecasting_columns(raw, choose_forecasting_input(d), d / "dim_date.csv")
        validate_forecasting_series_input(std)  # must not raise

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

    def test_zero_view_days_survive_standardise(self, series_csv):
        """Zero-view days in the fixture must not be dropped by standardise."""
        import pandas as pd
        from src.pipelines.run_forecasting_pipeline import (
            choose_forecasting_input,
            standardise_forecasting_columns,
        )
        d, daily = series_csv
        raw = pd.read_csv(choose_forecasting_input(d))
        std = standardise_forecasting_columns(raw, choose_forecasting_input(d), d / "dim_date.csv")
        zero_in_fixture = int((daily["daily_views"] == 0).sum())
        zero_in_std = int((std["daily_views"] == 0).sum())
        assert zero_in_std == zero_in_fixture, (
            f"Zero-view rows dropped: fixture had {zero_in_fixture}, standardised has {zero_in_std}"
        )

    def test_adapt_produces_legacy_schema(self, series_csv):
        """adapt_to_forecasting_schema must return the internal legacy column set."""
        import pandas as pd
        from src.pipelines.run_forecasting_pipeline import (
            choose_forecasting_input,
            standardise_forecasting_columns,
            adapt_to_forecasting_schema,
        )
        d, _ = series_csv
        raw = pd.read_csv(choose_forecasting_input(d))
        std = standardise_forecasting_columns(raw, choose_forecasting_input(d), d / "dim_date.csv")
        model_input = adapt_to_forecasting_schema(std, d / "dim_report.csv")
        expected_cols = {"Date", "Report Guid", "Report Name", "User Id", "Occurrences"}
        assert expected_cols.issubset(set(model_input.columns))

    def test_no_engagement_columns_in_model_input(self, series_csv):
        """No engagement or rolling-feature columns must appear in adapt output."""
        import pandas as pd
        from src.pipelines.run_forecasting_pipeline import (
            choose_forecasting_input,
            standardise_forecasting_columns,
            adapt_to_forecasting_schema,
        )
        d, _ = series_csv
        raw = pd.read_csv(choose_forecasting_input(d))
        std = standardise_forecasting_columns(raw, choose_forecasting_input(d), d / "dim_date.csv")
        model_input = adapt_to_forecasting_schema(std, d / "dim_report.csv")
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
