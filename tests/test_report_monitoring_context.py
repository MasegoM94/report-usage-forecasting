"""Tests for src/monitoring/report_monitoring_context.py.

Seven scenarios are covered:
 1. Happy path — all sources present, output has one row per report_id.
 2. No perf_by_report — monitoring never ran; context_status = "no_actuals_yet".
 3. No deterioration table — deterioration fields are null, status is "ok"
    when other context is present.
 4. Missing feature_context — concentration fields null, status "missing_context"
    when all context sources absent.
 5. Production forecast only — all optional sources absent; identity columns
    populated, all metrics null.
 6. Multiple runs in forecast — each report takes its most-recent run.
 7. Schema validation — duplicate (run_id, report_id) raises ValueError.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.monitoring.report_monitoring_context import (
    OUTPUT_COLS,
    build_report_monitoring_context,
    validate_report_monitoring_context,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_forecast(
    run_id: str = "run_20240101_120000",
    report_ids: list[str] | None = None,
    n_horizon_steps: int = 28,
    generated_at: str = "2024-01-01T12:00:00",
) -> pd.DataFrame:
    """Minimal production forecast DataFrame (rows per report × horizon_step)."""
    if report_ids is None:
        report_ids = ["R1", "R2"]

    rows = []
    for rid in report_ids:
        for step in range(1, n_horizon_steps + 1):
            rows.append(
                {
                    "run_id": run_id,
                    "report_id": rid,
                    "generated_at": generated_at,
                    "selected_model_family": "SARIMA",
                    "selected_model_name": "SARIMA(1,1,1)(0,1,1,7)",
                    "selected_m": 7,
                    "forecast": float(step * 10),
                    "lower_bound": float(step * 8),
                    "upper_bound": float(step * 12),
                    "horizon_step": step,
                    "median_backtest_mase": 0.82,
                    "mean_backtest_wape": 0.15,
                    "mean_backtest_bias": -1.2,
                    "valid_backtest_folds": 4,
                    "fold_win_rate": 0.75,
                }
            )
    return pd.DataFrame(rows)


def _make_perf_by_report(
    report_ids: list[str] | None = None,
    monitoring_status: str = "ok",
) -> pd.DataFrame:
    if report_ids is None:
        report_ids = ["R1", "R2"]
    rows = [
        {
            "report_id": rid,
            "wape": 0.14,
            "bias": -0.5,
            "interval_coverage": 0.92,
            "realized_prediction_count": 56,
            "recent_wape": 0.14,
            "previous_wape": 0.12,
            "monitoring_status": monitoring_status,
        }
        for rid in report_ids
    ]
    return pd.DataFrame(rows)


def _make_deterioration(report_ids: list[str] | None = None) -> pd.DataFrame:
    if report_ids is None:
        report_ids = ["R1", "R2"]
    rows = [
        {
            "report_id": rid,
            "accuracy_deterioration_flag": False,
            "deterioration_reasons": "[]",
            "evidence_status": "ok",
        }
        for rid in report_ids
    ]
    return pd.DataFrame(rows)


def _make_report_features(report_ids: list[str] | None = None) -> pd.DataFrame:
    if report_ids is None:
        report_ids = ["R1", "R2"]
    rows = [
        {"report_id": rid, "usage_change_pct": 0.05, "report_name": f"Report {rid}"}
        for rid in report_ids
    ]
    return pd.DataFrame(rows)


def _make_feature_context(report_ids: list[str] | None = None) -> pd.DataFrame:
    if report_ids is None:
        report_ids = ["R1", "R2"]
    rows = [
        {
            "report_id": rid,
            "date": "2024-01-01",
            "top_1_user_view_share": 0.42,
            "top_10pct_user_share": 0.65,
        }
        for rid in report_ids
    ]
    return pd.DataFrame(rows)


def _make_segments(report_ids: list[str] | None = None) -> pd.DataFrame:
    if report_ids is None:
        report_ids = ["R1", "R2"]
    rows = [{"report_id": rid, "report_segment": "healthy"} for rid in report_ids]
    return pd.DataFrame(rows)


def _make_diagnostics(report_ids: list[str] | None = None) -> pd.DataFrame:
    if report_ids is None:
        report_ids = ["R1", "R2"]
    rows = [
        {
            "report_id": rid,
            "main_diagnostic": "stable_usage",
            "diagnostic_summary": "Usage is stable.",
        }
        for rid in report_ids
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Scenario 1 — happy path
# ---------------------------------------------------------------------------

class TestHappyPath:
    """All sources present; output has one row per report."""

    def _build(self):
        return build_report_monitoring_context(
            production_forecast_df=_make_forecast(),
            perf_by_report_df=_make_perf_by_report(),
            deterioration_df=_make_deterioration(),
            report_features_df=_make_report_features(),
            feature_context_df=_make_feature_context(),
            segments_df=_make_segments(),
            diagnostics_df=_make_diagnostics(),
        )

    def test_one_row_per_report(self):
        df = self._build()
        assert len(df) == 2
        assert sorted(df["report_id"].tolist()) == ["R1", "R2"]

    def test_output_columns_are_canonical(self):
        df = self._build()
        assert list(df.columns) == OUTPUT_COLS

    def test_monitoring_run_id_populated(self):
        df = self._build()
        assert (df["monitoring_run_id"] == "run_20240101_120000").all()

    def test_forecast_28d_views_is_sum(self):
        df = self._build()
        # steps 1–28: sum(1..28)*10 = 406*10 = 4060
        expected = sum(range(1, 29)) * 10
        for val in df["forecast_28d_views"]:
            assert val == pytest.approx(expected)

    def test_lower_upper_totals_populated(self):
        df = self._build()
        assert df["lower_28d_total"].notna().all()
        assert df["upper_28d_total"].notna().all()

    def test_production_wape_populated(self):
        df = self._build()
        assert df["production_wape"].notna().all()

    def test_concentration_fields_populated(self):
        df = self._build()
        assert df["top_1_user_view_share"].notna().all()
        assert df["top_10pct_user_share"].notna().all()

    def test_context_status_ok(self):
        df = self._build()
        assert (df["context_status"] == "ok").all()

    def test_passes_validation(self):
        df = self._build()
        validate_report_monitoring_context(df)  # must not raise

    def test_usage_change_aliased_from_usage_change_pct(self):
        """When report_features has usage_change_pct, it's mapped to usage_change_28d_pct."""
        df = self._build()
        assert df["usage_change_28d_pct"].notna().all()
        assert all(abs(v - 0.05) < 1e-9 for v in df["usage_change_28d_pct"])


# ---------------------------------------------------------------------------
# Scenario 2 — no perf_by_report
# ---------------------------------------------------------------------------

class TestNoPerfByReport:
    """When monitoring has never run, production metrics are null and
    context_status is no_actuals_yet."""

    def _build(self):
        return build_report_monitoring_context(
            production_forecast_df=_make_forecast(),
            perf_by_report_df=None,
            deterioration_df=_make_deterioration(),
            report_features_df=_make_report_features(),
            feature_context_df=_make_feature_context(),
            segments_df=_make_segments(),
            diagnostics_df=_make_diagnostics(),
        )

    def test_context_status_no_actuals_yet(self):
        df = self._build()
        assert (df["context_status"] == "no_actuals_yet").all()

    def test_production_wape_is_null(self):
        df = self._build()
        assert df["production_wape"].isna().all()

    def test_identity_columns_still_populated(self):
        df = self._build()
        assert df["monitoring_run_id"].notna().all()
        assert df["report_id"].notna().all()

    def test_forecast_totals_still_populated(self):
        df = self._build()
        assert df["forecast_28d_views"].notna().all()


# ---------------------------------------------------------------------------
# Scenario 3 — no deterioration table
# ---------------------------------------------------------------------------

class TestNoDeterioration:
    """When the deterioration CSV does not exist, the corresponding fields are
    null but the row is still present and context_status reflects remaining
    coverage."""

    def _build(self):
        return build_report_monitoring_context(
            production_forecast_df=_make_forecast(),
            perf_by_report_df=_make_perf_by_report(),
            deterioration_df=None,
            report_features_df=_make_report_features(),
            feature_context_df=_make_feature_context(),
            segments_df=_make_segments(),
            diagnostics_df=_make_diagnostics(),
        )

    def test_deterioration_flag_is_null(self):
        df = self._build()
        assert df["accuracy_deterioration_flag"].isna().all()

    def test_deterioration_reasons_is_null(self):
        df = self._build()
        assert df["deterioration_reasons"].isna().all()

    def test_row_count_unchanged(self):
        df = self._build()
        assert len(df) == 2

    def test_production_wape_still_populated(self):
        df = self._build()
        assert df["production_wape"].notna().all()


# ---------------------------------------------------------------------------
# Scenario 4 — all context sources absent
# ---------------------------------------------------------------------------

class TestAllContextSourcesAbsent:
    """When features, context mart, segments, and diagnostics are all absent,
    context_status = missing_context."""

    def _build(self):
        return build_report_monitoring_context(
            production_forecast_df=_make_forecast(),
            perf_by_report_df=_make_perf_by_report(),
            deterioration_df=None,
            report_features_df=None,
            feature_context_df=None,
            segments_df=None,
            diagnostics_df=None,
        )

    def test_context_status_missing_context(self):
        df = self._build()
        assert (df["context_status"] == "missing_context").all()

    def test_concentration_fields_null(self):
        df = self._build()
        assert df["top_1_user_view_share"].isna().all()
        assert df["top_10pct_user_share"].isna().all()

    def test_segment_null(self):
        df = self._build()
        assert df["report_segment"].isna().all()


# ---------------------------------------------------------------------------
# Scenario 5 — production forecast only
# ---------------------------------------------------------------------------

class TestProductionForecastOnly:
    """All optional sources absent; identity/forecast columns populated,
    all monitoring metrics null."""

    def _build(self):
        return build_report_monitoring_context(
            production_forecast_df=_make_forecast(report_ids=["R1"]),
        )

    def test_output_has_one_row(self):
        df = self._build()
        assert len(df) == 1

    def test_identity_populated(self):
        df = self._build()
        assert df.loc[0, "monitoring_run_id"] == "run_20240101_120000"
        assert df.loc[0, "report_id"] == "R1"

    def test_forecast_totals_populated(self):
        df = self._build()
        assert not pd.isna(df.loc[0, "forecast_28d_views"])

    def test_all_monitoring_metrics_null(self):
        df = self._build()
        for col in ["production_wape", "production_bias", "production_interval_coverage",
                    "accuracy_deterioration_flag", "top_1_user_view_share", "report_segment"]:
            assert pd.isna(df.loc[0, col]), f"Expected {col} to be null"

    def test_raises_when_forecast_empty(self):
        with pytest.raises(ValueError, match="production_forecast_df"):
            build_report_monitoring_context(production_forecast_df=pd.DataFrame())

    def test_raises_when_forecast_missing_required_col(self):
        df = _make_forecast(report_ids=["R1"])
        df = df.drop(columns=["run_id"])
        with pytest.raises(ValueError, match="run_id"):
            build_report_monitoring_context(production_forecast_df=df)


# ---------------------------------------------------------------------------
# Scenario 6 — multiple runs in forecast
# ---------------------------------------------------------------------------

class TestMultipleRunsInForecast:
    """When the production forecast file contains rows from multiple runs,
    each report takes its most-recent run_id."""

    def _make_multi_run_forecast(self) -> pd.DataFrame:
        old = _make_forecast(
            run_id="run_20230601_090000",
            report_ids=["R1"],
            n_horizon_steps=28,
        )
        new = _make_forecast(
            run_id="run_20240101_120000",
            report_ids=["R1", "R2"],
            n_horizon_steps=28,
        )
        return pd.concat([old, new], ignore_index=True)

    def test_most_recent_run_selected(self):
        df = build_report_monitoring_context(
            production_forecast_df=self._make_multi_run_forecast()
        )
        assert (df["monitoring_run_id"] == "run_20240101_120000").all()

    def test_one_row_per_report(self):
        df = build_report_monitoring_context(
            production_forecast_df=self._make_multi_run_forecast()
        )
        assert len(df) == 2  # R1 and R2, both from the newer run

    def test_old_run_not_duplicated(self):
        df = build_report_monitoring_context(
            production_forecast_df=self._make_multi_run_forecast()
        )
        assert len(df[df["report_id"] == "R1"]) == 1


# ---------------------------------------------------------------------------
# Scenario 7 — schema validation
# ---------------------------------------------------------------------------

class TestSchemaValidation:
    """validate_report_monitoring_context raises on cardinality violations."""

    def _valid_df(self) -> pd.DataFrame:
        return build_report_monitoring_context(
            production_forecast_df=_make_forecast()
        )

    def test_valid_df_passes(self):
        df = self._valid_df()
        validate_report_monitoring_context(df)  # must not raise

    def test_duplicate_key_raises(self):
        df = self._valid_df()
        # Inject a duplicate row for R1
        dupe = df[df["report_id"] == "R1"].copy()
        df = pd.concat([df, dupe], ignore_index=True)
        with pytest.raises(ValueError, match="duplicate"):
            validate_report_monitoring_context(df)

    def test_missing_monitoring_run_id_raises(self):
        df = self._valid_df().drop(columns=["monitoring_run_id"])
        with pytest.raises(ValueError, match="monitoring_run_id"):
            validate_report_monitoring_context(df)

    def test_null_report_id_raises(self):
        df = self._valid_df().copy()
        df.loc[0, "report_id"] = None
        with pytest.raises(ValueError, match="report_id"):
            validate_report_monitoring_context(df)

    def test_all_output_cols_present(self):
        """build always returns exactly OUTPUT_COLS, no more, no less."""
        df = self._valid_df()
        assert list(df.columns) == OUTPUT_COLS

    def test_usage_change_28d_pct_not_backfilled_with_zero(self):
        """Null usage_change_28d_pct must remain null, not be filled with 0."""
        df = build_report_monitoring_context(
            production_forecast_df=_make_forecast(),
            report_features_df=None,
        )
        # No features source → usage_change_28d_pct must be null, not 0
        assert df["usage_change_28d_pct"].isna().all()

    def test_feature_context_takes_latest_date(self):
        """When feature_context has multiple rows per report, the most-recent
        date row's concentration values are used."""
        context = pd.DataFrame([
            {"report_id": "R1", "date": "2023-06-01", "top_1_user_view_share": 0.90, "top_10pct_user_share": 0.95},
            {"report_id": "R1", "date": "2024-01-01", "top_1_user_view_share": 0.42, "top_10pct_user_share": 0.65},
            {"report_id": "R2", "date": "2024-01-01", "top_1_user_view_share": 0.30, "top_10pct_user_share": 0.55},
        ])
        df = build_report_monitoring_context(
            production_forecast_df=_make_forecast(),
            feature_context_df=context,
        )
        r1 = df[df["report_id"] == "R1"].iloc[0]
        assert r1["top_1_user_view_share"] == pytest.approx(0.42), (
            "Expected the 2024-01-01 row (most recent) to be used, not the 2023-06-01 row"
        )
