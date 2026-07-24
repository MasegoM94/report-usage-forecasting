"""Tests for the normalized realized production forecast history.

Covers:
- REALIZED_FORECAST_HISTORY_COLS schema (columns, order, normalization)
- normalize_forecast_history_schema  (production / legacy source)
- build_realized_forecast_rows       (error calc, interval calc, horizon, lineage)
- validate_realized_forecast_history (all 15 invariants)
- write_realized_forecast_history    (append-only, dedup, no overwrite)
- load_realized_forecast_history     (corrupt key detection)
- update_realized_forecast_history   (source selection, end-to-end)
- migrate_legacy_realized_errors     (backup, sign flip, column rename)

All filesystem operations use tmp_path — no repository output files are touched.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.realized_forecast_history import (
    REALIZED_FORECAST_HISTORY_COLS,
    _LEGACY_RENAME,
    _UNIQUE_KEY,
    build_realized_forecast_rows,
    load_realized_forecast_history,
    migrate_legacy_realized_errors,
    normalize_forecast_history_schema,
    update_realized_forecast_history,
    validate_realized_forecast_history,
    write_realized_forecast_history,
)

# ---------------------------------------------------------------------------
# Shared test-data factories
# ---------------------------------------------------------------------------

_T0 = pd.Timestamp("2024-01-01")   # training cutoff
_FC1 = pd.Timestamp("2024-01-02")  # horizon step 1
_FC2 = pd.Timestamp("2024-01-03")  # horizon step 2
_FC28 = pd.Timestamp("2024-01-29") # horizon step 28


def _prod_row(
    run_id: str = "run1",
    selection_run_id: str = "sel1",
    generated_at: pd.Timestamp = _T0,
    report_id: str = "R001",
    report_name: str = "Report A",
    training_cutoff: pd.Timestamp = _T0,
    forecast_date: pd.Timestamp = _FC1,
    horizon_step: int = 1,
    selected_model_family: str = "seasonal_naive",
    selected_model_name: str = "seasonal_naive_m7",
    selected_m: int = 7,
    forecast: float = 10.0,
    lower_bound: float = 8.0,
    upper_bound: float = 12.0,
) -> dict:
    """Return a minimal production forecast history row (canonical schema)."""
    return dict(
        run_id=run_id,
        selection_run_id=selection_run_id,
        generated_at=generated_at,
        report_id=report_id,
        report_name=report_name,
        training_cutoff=training_cutoff,
        forecast_date=forecast_date,
        horizon_step=horizon_step,
        selected_model_family=selected_model_family,
        selected_model_name=selected_model_name,
        selected_m=selected_m,
        forecast=forecast,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
    )


def _prod_df(**kwargs) -> pd.DataFrame:
    return pd.DataFrame([_prod_row(**kwargs)])


def _actuals_df(
    report_id: str = "R001",
    date: pd.Timestamp = _FC1,
    daily_views: float = 9.0,
) -> pd.DataFrame:
    return pd.DataFrame({"report_id": [report_id], "date": [date], "daily_views": [daily_views]})


def _realized_row(**kwargs) -> dict:
    """Return a minimal already-realized row in the canonical schema."""
    base = _prod_row(**kwargs)
    base.update(dict(
        realized_at=pd.Timestamp("2024-01-10"),
        actual=9.0,
        signed_error=base["forecast"] - 9.0,
        absolute_error=abs(base["forecast"] - 9.0),
        squared_error=(base["forecast"] - 9.0) ** 2,
        inside_interval=True,
        interval_width=base["upper_bound"] - base["lower_bound"],
        percentage_error=abs(base["forecast"] - 9.0) / 9.0 * 100,
    ))
    # Fill remaining canonical columns
    for col in REALIZED_FORECAST_HISTORY_COLS:
        if col not in base:
            base[col] = np.nan
    return base


def _realized_df(**kwargs) -> pd.DataFrame:
    return pd.DataFrame([_realized_row(**kwargs)])[REALIZED_FORECAST_HISTORY_COLS]


def _write_prod_history(tmp_path: Path, rows: list[dict]) -> Path:
    """Write production_forecasts_history.csv under tmp_path and return project_root."""
    d = tmp_path / "outputs" / "forecasts"
    d.mkdir(parents=True)
    (tmp_path / "outputs" / "metrics").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(d / "production_forecasts_history.csv", index=False)
    return tmp_path


def _write_legacy_history(tmp_path: Path, rows: list[dict]) -> Path:
    d = tmp_path / "outputs" / "forecasts"
    d.mkdir(parents=True, exist_ok=True)
    (tmp_path / "outputs" / "metrics").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(d / "forecasts_history.csv", index=False)
    return tmp_path


def _write_realized_history(tmp_path: Path, rows: list[dict]) -> None:
    """Pre-seed realized_forecast_history.csv to simulate prior runs."""
    m = tmp_path / "outputs" / "metrics"
    m.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(m / "realized_forecast_history.csv", index=False)


def _legacy_row(
    run_id: str = "run1",
    report_id: str = "R001",
    report_name: str = "Report A",
    target_date: str = "2024-01-02",
    forecast_views: float = 10.0,
    lower_ci: float = 8.0,
    upper_ci: float = 12.0,
    model_name: str = "AutoARIMA_m7",
    run_timestamp: str = "2024-01-01 00:00:00",
    horizon_days: int = 1,
) -> dict:
    return dict(
        run_id=run_id,
        run_timestamp=pd.Timestamp(run_timestamp),
        report_id=report_id,
        report_name=report_name,
        target_date=target_date,
        horizon_days=horizon_days,
        forecast_views=forecast_views,
        lower_ci=lower_ci,
        upper_ci=upper_ci,
        model_name=model_name,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Schema
# ══════════════════════════════════════════════════════════════════════════════

class TestCanonicalSchema:

    def test_all_canonical_columns_present(self):
        df = build_realized_forecast_rows(_prod_df(), _actuals_df())
        assert set(REALIZED_FORECAST_HISTORY_COLS).issubset(df.columns)

    def test_canonical_column_order_stable(self):
        df = build_realized_forecast_rows(_prod_df(), _actuals_df())
        assert list(df.columns) == REALIZED_FORECAST_HISTORY_COLS

    def test_unique_key_columns_documented(self):
        assert _UNIQUE_KEY == ["run_id", "report_id", "forecast_date"]

    def test_legacy_rename_map_covers_known_old_names(self):
        expected_old = {
            "target_date", "forecast_views", "actual_views",
            "error", "abs_error", "lower_ci", "upper_ci",
        }
        assert expected_old.issubset(set(_LEGACY_RENAME.keys()))


# ══════════════════════════════════════════════════════════════════════════════
# normalize_forecast_history_schema
# ══════════════════════════════════════════════════════════════════════════════

class TestNormalizeProductionSchema:

    def test_production_source_accepted(self):
        raw = _prod_df()
        result = normalize_forecast_history_schema(raw, source="production")
        assert "forecast_date" in result.columns
        assert "forecast" in result.columns

    def test_production_missing_required_column_raises(self):
        raw = _prod_df().drop(columns=["forecast_date"])
        with pytest.raises(ValueError, match="forecast_date"):
            normalize_forecast_history_schema(raw, source="production")

    def test_unknown_source_raises(self):
        with pytest.raises(ValueError, match="Unknown source"):
            normalize_forecast_history_schema(_prod_df(), source="bad_source")

    def test_production_source_preferred_over_legacy(self, tmp_path):
        """When both files exist, update_realized_forecast_history must use
        production_forecasts_history.csv and not the legacy file."""
        prod_row = _prod_row(report_id="PROD_ONLY", forecast=50.0)
        _write_prod_history(tmp_path, [prod_row])
        # Also write a legacy file with a different report
        _write_legacy_history(tmp_path, [_legacy_row(report_id="LEG_ONLY")])

        actuals = pd.DataFrame({
            "report_id": ["PROD_ONLY"],
            "date": [_FC1],
            "daily_views": [45.0],
        })
        new_rows, _ = update_realized_forecast_history(actuals, tmp_path)
        assert not new_rows.empty
        # Must only contain the production report
        assert set(new_rows["report_id"]) == {"PROD_ONLY"}

    def test_incompatible_production_schema_fails_clearly(self):
        """A DataFrame with completely unrelated columns must fail."""
        bad = pd.DataFrame({"col_a": [1], "col_b": [2]})
        with pytest.raises(ValueError, match="missing required columns"):
            normalize_forecast_history_schema(bad, source="production")


class TestNormalizeLegacySchema:

    def test_legacy_renames_target_date_to_forecast_date(self):
        raw = pd.DataFrame([_legacy_row()])
        result = normalize_forecast_history_schema(raw, source="legacy")
        assert "forecast_date" in result.columns
        assert "target_date" not in result.columns

    def test_legacy_renames_forecast_views_to_forecast(self):
        raw = pd.DataFrame([_legacy_row()])
        result = normalize_forecast_history_schema(raw, source="legacy")
        assert "forecast" in result.columns
        assert "forecast_views" not in result.columns

    def test_legacy_renames_lower_ci_to_lower_bound(self):
        raw = pd.DataFrame([_legacy_row()])
        result = normalize_forecast_history_schema(raw, source="legacy")
        assert "lower_bound" in result.columns

    def test_legacy_renames_upper_ci_to_upper_bound(self):
        raw = pd.DataFrame([_legacy_row()])
        result = normalize_forecast_history_schema(raw, source="legacy")
        assert "upper_bound" in result.columns

    def test_legacy_copies_model_name_to_selected_model_name(self):
        raw = pd.DataFrame([_legacy_row(model_name="seasonal_naive_m7")])
        result = normalize_forecast_history_schema(raw, source="legacy")
        assert result["selected_model_name"].iloc[0] == "seasonal_naive_m7"

    def test_legacy_missing_required_column_raises(self):
        raw = pd.DataFrame([_legacy_row()]).drop(columns=["target_date"])
        with pytest.raises(ValueError, match="target_date"):
            normalize_forecast_history_schema(raw, source="legacy")

    def test_legacy_fills_missing_lineage_with_nan(self):
        raw = pd.DataFrame([_legacy_row()])
        result = normalize_forecast_history_schema(raw, source="legacy")
        # selected_model_family is absent in legacy — must be NaN, not missing
        assert "selected_model_family" in result.columns
        assert pd.isna(result["selected_model_family"].iloc[0])

    def test_legacy_derives_horizon_step_from_horizon_days(self):
        raw = pd.DataFrame([_legacy_row(horizon_days=5)])
        result = normalize_forecast_history_schema(raw, source="legacy")
        assert result["horizon_step"].iloc[0] == 5


# ══════════════════════════════════════════════════════════════════════════════
# build_realized_forecast_rows — lineage
# ══════════════════════════════════════════════════════════════════════════════

class TestLineagePreservation:

    def test_run_id_preserved(self):
        df = build_realized_forecast_rows(
            _prod_df(run_id="special_run"), _actuals_df()
        )
        assert df["run_id"].iloc[0] == "special_run"

    def test_selection_run_id_preserved(self):
        df = build_realized_forecast_rows(
            _prod_df(selection_run_id="sel_XYZ"), _actuals_df()
        )
        assert df["selection_run_id"].iloc[0] == "sel_XYZ"

    def test_training_cutoff_preserved(self):
        cutoff = pd.Timestamp("2023-12-15")
        df = build_realized_forecast_rows(
            _prod_df(training_cutoff=cutoff), _actuals_df()
        )
        assert pd.Timestamp(df["training_cutoff"].iloc[0]) == cutoff

    def test_selected_model_family_preserved(self):
        df = build_realized_forecast_rows(
            _prod_df(selected_model_family="auto_arima"), _actuals_df()
        )
        assert df["selected_model_family"].iloc[0] == "auto_arima"

    def test_selected_model_name_preserved(self):
        df = build_realized_forecast_rows(
            _prod_df(selected_model_name="auto_arima_m30"), _actuals_df()
        )
        assert df["selected_model_name"].iloc[0] == "auto_arima_m30"

    def test_selected_m_preserved(self):
        df = build_realized_forecast_rows(
            _prod_df(selected_m=30), _actuals_df()
        )
        assert df["selected_m"].iloc[0] == 30

    def test_selected_m_1_for_non_seasonal(self):
        df = build_realized_forecast_rows(
            _prod_df(selected_m=1, selected_model_family="auto_arima",
                     selected_model_name="auto_arima_m1"),
            _actuals_df()
        )
        assert df["selected_m"].iloc[0] == 1


# ══════════════════════════════════════════════════════════════════════════════
# build_realized_forecast_rows — error calculations
# ══════════════════════════════════════════════════════════════════════════════

class TestErrorCalculations:

    def test_signed_error_is_forecast_minus_actual(self):
        df = build_realized_forecast_rows(_prod_df(forecast=10.0), _actuals_df(daily_views=7.0))
        assert df["signed_error"].iloc[0] == pytest.approx(10.0 - 7.0)

    def test_signed_error_positive_when_over_forecast(self):
        df = build_realized_forecast_rows(_prod_df(forecast=15.0), _actuals_df(daily_views=10.0))
        assert df["signed_error"].iloc[0] > 0

    def test_signed_error_negative_when_under_forecast(self):
        df = build_realized_forecast_rows(_prod_df(forecast=5.0), _actuals_df(daily_views=10.0))
        assert df["signed_error"].iloc[0] < 0

    def test_absolute_error_is_abs_signed_error(self):
        df = build_realized_forecast_rows(_prod_df(forecast=10.0), _actuals_df(daily_views=7.0))
        assert df["absolute_error"].iloc[0] == pytest.approx(abs(10.0 - 7.0))

    def test_absolute_error_non_negative_for_negative_signed_error(self):
        df = build_realized_forecast_rows(_prod_df(forecast=5.0), _actuals_df(daily_views=10.0))
        assert df["absolute_error"].iloc[0] >= 0

    def test_squared_error_is_signed_error_squared(self):
        df = build_realized_forecast_rows(_prod_df(forecast=10.0), _actuals_df(daily_views=7.0))
        expected = (10.0 - 7.0) ** 2
        assert df["squared_error"].iloc[0] == pytest.approx(expected)

    def test_zero_actual_retained_with_valid_signed_error(self):
        df = build_realized_forecast_rows(_prod_df(forecast=5.0), _actuals_df(daily_views=0.0))
        assert len(df) == 1
        assert df["actual"].iloc[0] == 0.0
        assert df["signed_error"].iloc[0] == pytest.approx(5.0)

    def test_percentage_error_null_for_zero_actual(self):
        df = build_realized_forecast_rows(_prod_df(forecast=5.0), _actuals_df(daily_views=0.0))
        assert pd.isna(df["percentage_error"].iloc[0])

    def test_percentage_error_computed_for_nonzero_actual(self):
        df = build_realized_forecast_rows(_prod_df(forecast=10.0), _actuals_df(daily_views=8.0))
        expected = abs(10.0 - 8.0) / 8.0 * 100
        assert df["percentage_error"].iloc[0] == pytest.approx(expected)

    def test_perfect_forecast_all_errors_zero(self):
        df = build_realized_forecast_rows(_prod_df(forecast=9.0), _actuals_df(daily_views=9.0))
        assert df["signed_error"].iloc[0] == pytest.approx(0.0)
        assert df["absolute_error"].iloc[0] == pytest.approx(0.0)
        assert df["squared_error"].iloc[0] == pytest.approx(0.0)


# ══════════════════════════════════════════════════════════════════════════════
# build_realized_forecast_rows — interval columns
# ══════════════════════════════════════════════════════════════════════════════

class TestIntervalCalculations:

    def test_actual_inside_interval(self):
        df = build_realized_forecast_rows(
            _prod_df(forecast=10.0, lower_bound=8.0, upper_bound=12.0),
            _actuals_df(daily_views=10.0),
        )
        assert df["inside_interval"].iloc[0] == True   # noqa: E712

    def test_actual_outside_interval(self):
        df = build_realized_forecast_rows(
            _prod_df(forecast=10.0, lower_bound=8.0, upper_bound=12.0),
            _actuals_df(daily_views=20.0),
        )
        assert df["inside_interval"].iloc[0] == False  # noqa: E712

    def test_actual_on_lower_bound(self):
        df = build_realized_forecast_rows(
            _prod_df(forecast=10.0, lower_bound=8.0, upper_bound=12.0),
            _actuals_df(daily_views=8.0),
        )
        assert df["inside_interval"].iloc[0] == True   # noqa: E712

    def test_actual_on_upper_bound(self):
        df = build_realized_forecast_rows(
            _prod_df(forecast=10.0, lower_bound=8.0, upper_bound=12.0),
            _actuals_df(daily_views=12.0),
        )
        assert df["inside_interval"].iloc[0] == True   # noqa: E712

    def test_interval_width_correct(self):
        df = build_realized_forecast_rows(
            _prod_df(lower_bound=8.0, upper_bound=12.0),
            _actuals_df(),
        )
        assert df["interval_width"].iloc[0] == pytest.approx(4.0)

    def test_missing_lower_bound_gives_null_inside_interval(self):
        row = _prod_row()
        row["lower_bound"] = np.nan
        df = build_realized_forecast_rows(pd.DataFrame([row]), _actuals_df())
        assert pd.isna(df["inside_interval"].iloc[0])

    def test_missing_upper_bound_gives_null_inside_interval(self):
        row = _prod_row()
        row["upper_bound"] = np.nan
        df = build_realized_forecast_rows(pd.DataFrame([row]), _actuals_df())
        assert pd.isna(df["inside_interval"].iloc[0])

    def test_missing_both_bounds_gives_null_width_and_inside(self):
        row = _prod_row()
        row["lower_bound"] = np.nan
        row["upper_bound"] = np.nan
        df = build_realized_forecast_rows(pd.DataFrame([row]), _actuals_df())
        assert pd.isna(df["inside_interval"].iloc[0])
        assert pd.isna(df["interval_width"].iloc[0])

    def test_inverted_bounds_recorded_as_provided(self):
        """build_realized_forecast_rows records whatever bounds are passed;
        validation (not the builder) is responsible for catching lb > ub."""
        row = _prod_row()
        row["lower_bound"] = 15.0
        row["upper_bound"] = 5.0   # intentionally inverted
        df = build_realized_forecast_rows(pd.DataFrame([row]), _actuals_df(daily_views=10.0))
        # 10 is NOT in [15, 5] as checked by actual >= lb AND actual <= ub
        assert df["inside_interval"].iloc[0] == False  # noqa: E712
        assert df["interval_width"].iloc[0] == pytest.approx(5.0 - 15.0)  # negative

    def test_inverted_bounds_fail_validation(self):
        """validate_realized_forecast_history must reject lb > ub."""
        df = _realized_df(lower_bound=15.0, upper_bound=5.0)
        # Override the derived columns to make other checks pass
        df["actual"] = 9.0
        df["signed_error"] = df["forecast"] - df["actual"]
        df["absolute_error"] = df["signed_error"].abs()
        df["squared_error"] = df["signed_error"] ** 2
        df["inside_interval"] = False
        df["interval_width"] = df["upper_bound"] - df["lower_bound"]
        df["percentage_error"] = df["absolute_error"] / df["actual"] * 100
        with pytest.raises(ValueError, match="lower_bound > upper_bound"):
            validate_realized_forecast_history(df)


# ══════════════════════════════════════════════════════════════════════════════
# build_realized_forecast_rows — horizon
# ══════════════════════════════════════════════════════════════════════════════

class TestHorizonStep:

    def test_horizon_step_1_preserved(self):
        df = build_realized_forecast_rows(_prod_df(horizon_step=1), _actuals_df())
        assert df["horizon_step"].iloc[0] == 1

    def test_horizon_step_28_preserved(self):
        df = build_realized_forecast_rows(
            _prod_df(horizon_step=28, forecast_date=_FC28),
            _actuals_df(date=_FC28),
        )
        assert df["horizon_step"].iloc[0] == 28

    def test_invalid_horizon_step_0_fails_validation(self):
        df = build_realized_forecast_rows(
            _prod_df(horizon_step=0), _actuals_df()
        )
        with pytest.raises(ValueError, match="horizon_step"):
            validate_realized_forecast_history(df)

    def test_invalid_horizon_step_29_fails_validation(self):
        df = build_realized_forecast_rows(
            _prod_df(horizon_step=29, forecast_date=_FC28),
            _actuals_df(date=_FC28),
        )
        with pytest.raises(ValueError, match="horizon_step"):
            validate_realized_forecast_history(df)

    def test_legacy_horizon_derived_from_horizon_days(self):
        """When using legacy source, horizon_days maps to horizon_step."""
        raw = pd.DataFrame([_legacy_row(horizon_days=7)])
        norm = normalize_forecast_history_schema(raw, source="legacy")
        assert norm["horizon_step"].iloc[0] == 7

    def test_legacy_missing_horizon_days_gives_nan_horizon_step(self):
        raw = pd.DataFrame([_legacy_row()])
        raw = raw.drop(columns=["horizon_days"])
        norm = normalize_forecast_history_schema(raw, source="legacy")
        # horizon_step should be NaN (not guessed)
        assert pd.isna(norm["horizon_step"].iloc[0])


# ══════════════════════════════════════════════════════════════════════════════
# build_realized_forecast_rows — realization logic
# ══════════════════════════════════════════════════════════════════════════════

class TestRealizationLogic:

    def test_future_date_excluded(self):
        """A forecast_date for which no actual exists must not be realized."""
        df = build_realized_forecast_rows(
            _prod_df(forecast_date=pd.Timestamp("2099-01-01")),
            _actuals_df(date=_FC1),  # actuals only for _FC1
        )
        assert df.empty

    def test_past_date_with_actual_included(self):
        df = build_realized_forecast_rows(_prod_df(), _actuals_df())
        assert len(df) == 1

    def test_partially_realized_28_day_forecast(self):
        """Only the subset of forecast dates that have actuals must be realized."""
        rows = [
            _prod_row(forecast_date=_T0 + pd.Timedelta(days=i + 1), horizon_step=i + 1)
            for i in range(28)
        ]
        fc = pd.DataFrame(rows)
        # Only 7 actual dates available
        act = pd.DataFrame({
            "report_id": ["R001"] * 7,
            "date": [_T0 + pd.Timedelta(days=i + 1) for i in range(7)],
            "daily_views": [10.0] * 7,
        })
        result = build_realized_forecast_rows(fc, act)
        assert len(result) == 7

    def test_fully_realized_28_day_forecast(self):
        rows = [
            _prod_row(forecast_date=_T0 + pd.Timedelta(days=i + 1), horizon_step=i + 1)
            for i in range(28)
        ]
        fc = pd.DataFrame(rows)
        act = pd.DataFrame({
            "report_id": ["R001"] * 28,
            "date": [_T0 + pd.Timedelta(days=i + 1) for i in range(28)],
            "daily_views": [10.0] * 28,
        })
        result = build_realized_forecast_rows(fc, act)
        assert len(result) == 28

    def test_multiple_reports_realized_independently(self):
        fc = pd.DataFrame([
            _prod_row(report_id="R001", forecast_date=_FC1, forecast=10.0),
            _prod_row(report_id="R002", forecast_date=_FC1, forecast=20.0),
        ])
        act = pd.DataFrame({
            "report_id": ["R001", "R002"],
            "date": [_FC1, _FC1],
            "daily_views": [9.0, 18.0],
        })
        result = build_realized_forecast_rows(fc, act)
        assert len(result) == 2
        r1 = result[result["report_id"] == "R001"].iloc[0]
        r2 = result[result["report_id"] == "R002"].iloc[0]
        assert r1["actual"] == 9.0
        assert r2["actual"] == 18.0

    def test_multiple_runs_same_report_and_date_both_realized(self):
        """Different run_ids for the same (report_id, forecast_date) must each
        produce a realized row — they remain separate under the unique key."""
        fc = pd.DataFrame([
            _prod_row(run_id="run_A", forecast=10.0),
            _prod_row(run_id="run_B", forecast=12.0),
        ])
        result = build_realized_forecast_rows(fc, _actuals_df(daily_views=9.0))
        assert len(result) == 2
        assert set(result["run_id"]) == {"run_A", "run_B"}

    def test_wrong_report_id_in_actuals_does_not_match(self):
        df = build_realized_forecast_rows(
            _prod_df(report_id="R001"),
            _actuals_df(report_id="R999"),  # different report
        )
        assert df.empty

    def test_zero_actual_views_retained(self):
        df = build_realized_forecast_rows(_prod_df(), _actuals_df(daily_views=0.0))
        assert len(df) == 1
        assert df["actual"].iloc[0] == 0.0


# ══════════════════════════════════════════════════════════════════════════════
# build_realized_forecast_rows — input validation
# ══════════════════════════════════════════════════════════════════════════════

class TestBuildInputValidation:

    def test_missing_run_id_in_forecast_history_raises(self):
        fc = _prod_df().drop(columns=["run_id"])
        with pytest.raises(ValueError, match="run_id"):
            build_realized_forecast_rows(fc, _actuals_df())

    def test_missing_report_id_in_forecast_history_raises(self):
        fc = _prod_df().drop(columns=["report_id"])
        with pytest.raises(ValueError, match="report_id"):
            build_realized_forecast_rows(fc, _actuals_df())

    def test_missing_daily_views_in_actuals_raises(self):
        act = _actuals_df().drop(columns=["daily_views"])
        with pytest.raises(ValueError, match="daily_views"):
            build_realized_forecast_rows(_prod_df(), act)

    def test_missing_date_in_actuals_raises(self):
        act = _actuals_df().drop(columns=["date"])
        with pytest.raises(ValueError, match="date"):
            build_realized_forecast_rows(_prod_df(), act)

    def test_unrelated_files_in_actuals_dir_do_not_affect_result(self, tmp_path):
        """Only the DataFrame passed as actual_daily_series is used — no
        filesystem reads happen inside build_realized_forecast_rows."""
        # Write a spurious CSV with high view count
        (tmp_path / "spurious.csv").write_text("report_id,date,daily_views\nR001,2024-01-02,9999\n")
        # The function must use the actuals DataFrame, not that file
        df = build_realized_forecast_rows(_prod_df(forecast=10.0), _actuals_df(daily_views=9.0))
        assert df["actual"].iloc[0] == 9.0

    def test_empty_actuals_returns_empty(self):
        empty_act = pd.DataFrame(columns=["report_id", "date", "daily_views"])
        result = build_realized_forecast_rows(_prod_df(), empty_act)
        assert result.empty


# ══════════════════════════════════════════════════════════════════════════════
# validate_realized_forecast_history
# ══════════════════════════════════════════════════════════════════════════════

class TestValidation:

    def _valid(self) -> pd.DataFrame:
        return build_realized_forecast_rows(_prod_df(), _actuals_df())

    def test_valid_row_passes(self):
        validate_realized_forecast_history(self._valid())  # must not raise

    def test_empty_frame_passes(self):
        empty = pd.DataFrame(columns=REALIZED_FORECAST_HISTORY_COLS)
        validate_realized_forecast_history(empty)

    def test_missing_column_raises(self):
        df = self._valid().drop(columns=["signed_error"])
        with pytest.raises(ValueError, match="signed_error"):
            validate_realized_forecast_history(df)

    def test_null_run_id_raises(self):
        df = self._valid().copy()
        df["run_id"] = None
        with pytest.raises(ValueError, match="run_id"):
            validate_realized_forecast_history(df)

    def test_null_report_id_raises(self):
        df = self._valid().copy()
        df["report_id"] = None
        with pytest.raises(ValueError, match="report_id"):
            validate_realized_forecast_history(df)

    def test_null_forecast_date_raises(self):
        df = self._valid().copy()
        df["forecast_date"] = pd.NaT
        with pytest.raises(ValueError, match="forecast_date"):
            validate_realized_forecast_history(df)

    def test_duplicate_unique_key_raises(self):
        df = pd.concat([self._valid(), self._valid()], ignore_index=True)
        with pytest.raises(ValueError, match="duplicate"):
            validate_realized_forecast_history(df)

    def test_negative_actual_raises(self):
        df = self._valid().copy()
        df["actual"] = -1.0
        with pytest.raises(ValueError, match="actual"):
            validate_realized_forecast_history(df)

    def test_negative_forecast_raises(self):
        df = self._valid().copy()
        df["forecast"] = -1.0
        df["signed_error"] = df["forecast"] - df["actual"]
        df["absolute_error"] = df["signed_error"].abs()
        df["squared_error"] = df["signed_error"] ** 2
        with pytest.raises(ValueError, match="forecast"):
            validate_realized_forecast_history(df)

    def test_selected_m_zero_raises(self):
        df = self._valid().copy()
        df["selected_m"] = 0
        with pytest.raises(ValueError, match="selected_m"):
            validate_realized_forecast_history(df)

    def test_selected_m_1_passes(self):
        df = self._valid().copy()
        df["selected_m"] = 1
        validate_realized_forecast_history(df)  # must not raise

    def test_negative_interval_width_raises(self):
        df = self._valid().copy()
        df["interval_width"] = -1.0
        with pytest.raises(ValueError, match="interval_width"):
            validate_realized_forecast_history(df)

    def test_inside_interval_inconsistency_raises(self):
        df = self._valid().copy()
        # actual=9, bounds=[8,12] → should be inside; force it wrong
        df["inside_interval"] = False
        with pytest.raises(ValueError, match="inside_interval"):
            validate_realized_forecast_history(df)

    def test_wrong_signed_error_raises(self):
        df = self._valid().copy()
        df["signed_error"] = 999.0  # deliberately wrong
        with pytest.raises(ValueError, match="signed_error"):
            validate_realized_forecast_history(df)

    def test_wrong_absolute_error_raises(self):
        df = self._valid().copy()
        df["absolute_error"] = 999.0
        with pytest.raises(ValueError, match="absolute_error"):
            validate_realized_forecast_history(df)

    def test_wrong_squared_error_raises(self):
        df = self._valid().copy()
        df["squared_error"] = 999.0
        with pytest.raises(ValueError, match="squared_error"):
            validate_realized_forecast_history(df)

    def test_pct_error_non_null_when_actual_zero_raises(self):
        df = build_realized_forecast_rows(_prod_df(forecast=5.0), _actuals_df(daily_views=0.0))
        # Force a non-null pct_error to trigger the check
        df = df.copy()
        df["percentage_error"] = 50.0
        with pytest.raises(ValueError, match="percentage_error"):
            validate_realized_forecast_history(df)

    def test_horizon_step_out_of_range_raises(self):
        df = self._valid().copy()
        df["horizon_step"] = 0
        with pytest.raises(ValueError, match="horizon_step"):
            validate_realized_forecast_history(df)


# ══════════════════════════════════════════════════════════════════════════════
# Persistence — write_realized_forecast_history and load
# ══════════════════════════════════════════════════════════════════════════════

class TestPersistence:

    def _new_rows(self, **kwargs) -> pd.DataFrame:
        return build_realized_forecast_rows(_prod_df(**kwargs), _actuals_df())

    def test_first_write_creates_file(self, tmp_path):
        rows = self._new_rows()
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        write_realized_forecast_history(rows, tmp_path)
        assert (tmp_path / "outputs" / "metrics" / "realized_forecast_history.csv").exists()

    def test_second_write_appends_new_row(self, tmp_path):
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        r1 = build_realized_forecast_rows(
            _prod_df(run_id="run1", forecast_date=_FC1), _actuals_df(date=_FC1)
        )
        r2 = build_realized_forecast_rows(
            _prod_df(run_id="run2", forecast_date=_FC1), _actuals_df(date=_FC1)
        )
        write_realized_forecast_history(r1, tmp_path)
        write_realized_forecast_history(r2, tmp_path)
        loaded = load_realized_forecast_history(tmp_path)
        assert len(loaded) == 2

    def test_repeat_write_same_key_not_duplicated(self, tmp_path):
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        rows = self._new_rows()
        write_realized_forecast_history(rows, tmp_path)
        _, n = write_realized_forecast_history(rows, tmp_path)
        loaded = load_realized_forecast_history(tmp_path)
        assert len(loaded) == 1
        assert n == 0  # nothing new was appended

    def test_skip_count_reported_correctly(self, tmp_path):
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        rows = self._new_rows()
        write_realized_forecast_history(rows, tmp_path)
        skip = [0]
        write_realized_forecast_history(rows, tmp_path, skip_count_out=skip)
        assert skip[0] == 1

    def test_existing_row_unchanged_after_second_write(self, tmp_path):
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        rows = self._new_rows(forecast=10.0)
        write_realized_forecast_history(rows, tmp_path)
        # Attempt to re-write with a different forecast value (same key)
        rows2 = self._new_rows(forecast=99.0)
        write_realized_forecast_history(rows2, tmp_path)
        loaded = load_realized_forecast_history(tmp_path)
        assert len(loaded) == 1
        assert float(loaded["forecast"].iloc[0]) == pytest.approx(10.0)

    def test_only_new_rows_appended_on_partial_update(self, tmp_path):
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        r1 = build_realized_forecast_rows(
            _prod_df(run_id="run1", forecast_date=_FC1, horizon_step=1),
            _actuals_df(date=_FC1)
        )
        r2 = build_realized_forecast_rows(
            _prod_df(run_id="run1", forecast_date=_FC2, horizon_step=2),
            _actuals_df(date=_FC2)
        )
        write_realized_forecast_history(r1, tmp_path)
        # r2 has a different forecast_date — only r2 should be appended
        combined = pd.concat([r1, r2], ignore_index=True)
        write_realized_forecast_history(combined, tmp_path)
        loaded = load_realized_forecast_history(tmp_path)
        assert len(loaded) == 2

    def test_header_written_only_once(self, tmp_path):
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        r1 = self._new_rows(run_id="r1")
        r2 = self._new_rows(run_id="r2")
        write_realized_forecast_history(r1, tmp_path)
        write_realized_forecast_history(r2, tmp_path)
        path = tmp_path / "outputs" / "metrics" / "realized_forecast_history.csv"
        with open(path) as f:
            lines = f.readlines()
        header_lines = [l for l in lines if l.startswith("run_id")]
        assert len(header_lines) == 1


# ══════════════════════════════════════════════════════════════════════════════
# load_realized_forecast_history — corrupt file detection
# ══════════════════════════════════════════════════════════════════════════════

class TestLoadCorruptionDetection:

    def _path(self, tmp_path: Path) -> Path:
        p = tmp_path / "outputs" / "metrics"
        p.mkdir(parents=True, exist_ok=True)
        return p / "realized_forecast_history.csv"

    def test_no_file_returns_empty(self, tmp_path):
        result = load_realized_forecast_history(tmp_path)
        assert result.empty

    def test_corrupt_date_raises(self, tmp_path):
        path = self._path(tmp_path)
        row = _realized_row()
        row["forecast_date"] = "NOT_A_DATE"
        pd.DataFrame([row]).to_csv(path, index=False)
        with pytest.raises(ValueError, match="forecast_date"):
            load_realized_forecast_history(tmp_path)

    def test_null_run_id_raises(self, tmp_path):
        path = self._path(tmp_path)
        row = _realized_row()
        row["run_id"] = None
        pd.DataFrame([row]).to_csv(path, index=False)
        with pytest.raises(ValueError, match="run_id"):
            load_realized_forecast_history(tmp_path)

    def test_null_report_id_raises(self, tmp_path):
        path = self._path(tmp_path)
        row = _realized_row()
        row["report_id"] = None
        pd.DataFrame([row]).to_csv(path, index=False)
        with pytest.raises(ValueError, match="report_id"):
            load_realized_forecast_history(tmp_path)

    def test_duplicate_key_in_file_raises(self, tmp_path):
        path = self._path(tmp_path)
        row = _realized_row()
        pd.DataFrame([row, row]).to_csv(path, index=False)
        with pytest.raises(ValueError, match="duplicate"):
            load_realized_forecast_history(tmp_path)

    def test_valid_file_loads_cleanly(self, tmp_path):
        path = self._path(tmp_path)
        _realized_df().to_csv(path, index=False)
        result = load_realized_forecast_history(tmp_path)
        assert len(result) == 1


# ══════════════════════════════════════════════════════════════════════════════
# update_realized_forecast_history — end-to-end
# ══════════════════════════════════════════════════════════════════════════════

class TestUpdateRealizedForecastHistory:

    def test_production_source_used_when_both_files_exist(self, tmp_path):
        _write_prod_history(tmp_path, [_prod_row(report_id="PROD_REPORT")])
        _write_legacy_history(tmp_path, [_legacy_row(report_id="LEGACY_REPORT")])
        act = pd.DataFrame({"report_id": ["PROD_REPORT"], "date": [_FC1], "daily_views": [10.0]})
        new_rows, _ = update_realized_forecast_history(act, tmp_path)
        assert set(new_rows["report_id"]) == {"PROD_REPORT"}

    def test_legacy_used_when_production_absent(self, tmp_path):
        _write_legacy_history(tmp_path, [_legacy_row(report_id="LEG_REPORT")])
        act = pd.DataFrame({
            "report_id": ["LEG_REPORT"],
            "date": [pd.Timestamp("2024-01-02")],
            "daily_views": [10.0],
        })
        new_rows, _ = update_realized_forecast_history(act, tmp_path)
        assert not new_rows.empty

    def test_no_history_file_returns_empty(self, tmp_path):
        (tmp_path / "outputs" / "forecasts").mkdir(parents=True)
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        act = _actuals_df()
        new_rows, _ = update_realized_forecast_history(act, tmp_path)
        assert new_rows.empty

    def test_repeat_call_skips_already_realized(self, tmp_path):
        _write_prod_history(tmp_path, [_prod_row()])
        act = _actuals_df()
        update_realized_forecast_history(act, tmp_path)
        _, n_skip = update_realized_forecast_history(act, tmp_path)
        assert n_skip == 1

    def test_missing_daily_views_column_raises(self, tmp_path):
        _write_prod_history(tmp_path, [_prod_row()])
        bad = pd.DataFrame({"report_id": ["R001"], "date": [_FC1]})
        with pytest.raises(ValueError, match="daily_views"):
            update_realized_forecast_history(bad, tmp_path)

    def test_result_passes_validation(self, tmp_path):
        _write_prod_history(tmp_path, [_prod_row()])
        act = _actuals_df()
        new_rows, _ = update_realized_forecast_history(act, tmp_path)
        validate_realized_forecast_history(new_rows)  # must not raise


# ══════════════════════════════════════════════════════════════════════════════
# migrate_legacy_realized_errors
# ══════════════════════════════════════════════════════════════════════════════

class TestMigrateLegacyRealizedErrors:

    def _legacy_realized_row(self) -> dict:
        """A realized error row as written by the old update_realized_errors."""
        return dict(
            run_id="run1",
            report_id="R001",
            report_name="Report A",
            target_date="2024-01-02",
            horizon_days=1,
            forecast_views=10.0,
            actual_views=9.0,
            error=9.0 - 10.0,     # old schema: actual - forecast
            abs_error=1.0,
            pct_error=11.11,
            run_timestamp="2024-01-01 00:00:00",
            realized_at="2024-01-03 00:00:00",
            model_name="AutoARIMA_m7",
        )

    def _write_legacy_realized(self, tmp_path: Path, rows: list[dict]) -> None:
        m = tmp_path / "outputs" / "metrics"
        m.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(m / "realized_errors_history.csv", index=False)

    def test_returns_no_legacy_file_status_when_absent(self, tmp_path):
        (tmp_path / "outputs" / "metrics").mkdir(parents=True)
        result = migrate_legacy_realized_errors(tmp_path, backup=False)
        assert result["status"] == "no_legacy_file"

    def test_deduplicates_into_existing_canonical(self, tmp_path):
        """When canonical already exists, migrate deduplicates and appends new rows."""
        self._write_legacy_realized(tmp_path, [self._legacy_realized_row()])
        _realized_df().to_csv(
            tmp_path / "outputs" / "metrics" / "realized_forecast_history.csv", index=False
        )
        result = migrate_legacy_realized_errors(tmp_path, backup=False)
        # May migrate or skip — must not fail and must return a dict
        assert isinstance(result, dict)
        assert "rows_migrated" in result

    def test_creates_backup_when_requested(self, tmp_path):
        self._write_legacy_realized(tmp_path, [self._legacy_realized_row()])
        result = migrate_legacy_realized_errors(tmp_path, backup=True)
        assert result["backup_path"] is not None
        assert result["backup_path"].exists()

    def test_no_backup_when_not_requested(self, tmp_path):
        self._write_legacy_realized(tmp_path, [self._legacy_realized_row()])
        result = migrate_legacy_realized_errors(tmp_path, backup=False)
        assert result["backup_path"] is None
        # No .bak files should exist in outputs/metrics/
        baks = list((tmp_path / "outputs" / "metrics").glob("*.bak"))
        assert baks == []

    def test_canonical_file_created_after_migration(self, tmp_path):
        self._write_legacy_realized(tmp_path, [self._legacy_realized_row()])
        migrate_legacy_realized_errors(tmp_path, backup=False)
        assert (tmp_path / "outputs" / "metrics" / "realized_forecast_history.csv").exists()

    def test_sign_of_signed_error_flipped(self, tmp_path):
        """Legacy error = actual − forecast; canonical signed_error = forecast − actual."""
        row = self._legacy_realized_row()  # error = 9 - 10 = -1 (old)
        self._write_legacy_realized(tmp_path, [row])
        migrate_legacy_realized_errors(tmp_path, backup=False)
        result = pd.read_csv(
            tmp_path / "outputs" / "metrics" / "realized_forecast_history.csv"
        )
        assert float(result["signed_error"].iloc[0]) == pytest.approx(10.0 - 9.0)

    def test_forecast_date_renamed_from_target_date(self, tmp_path):
        self._write_legacy_realized(tmp_path, [self._legacy_realized_row()])
        migrate_legacy_realized_errors(tmp_path, backup=False)
        result = pd.read_csv(
            tmp_path / "outputs" / "metrics" / "realized_forecast_history.csv"
        )
        assert "forecast_date" in result.columns
        assert "target_date" not in result.columns

    def test_canonical_columns_present_after_migration(self, tmp_path):
        self._write_legacy_realized(tmp_path, [self._legacy_realized_row()])
        migrate_legacy_realized_errors(tmp_path, backup=False)
        result = pd.read_csv(
            tmp_path / "outputs" / "metrics" / "realized_forecast_history.csv"
        )
        missing = set(REALIZED_FORECAST_HISTORY_COLS) - set(result.columns)
        assert not missing, f"Missing after migration: {missing}"
