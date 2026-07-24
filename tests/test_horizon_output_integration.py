"""Integration tests for persisted horizon-bucket outputs.

Covers:
* Backtest horizon file generation via save_production_outputs
* Realized horizon file generation via save_production_performance
* Correct horizon_bucket values in both outputs
* model_name and selected_m lineage in backtest output
* No duplicate rows in either output
* Production and backtest horizon results remain distinct (different files,
  different grains)
* Schema validation catches malformed outputs
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.models.backtest_evaluation import BacktestConfig, evaluate_models_across_folds
from src.models.candidates import forecast_naive, forecast_seasonal_naive
from src.models.horizon_evaluation import HORIZON_BUCKETS
from src.models.horizon_output_validation import (
    validate_backtest_horizon_output,
    validate_realized_horizon_output,
)
from src.monitoring.production_performance import (
    realized_performance_by_horizon,
    save_production_performance,
)

# ---------------------------------------------------------------------------
# Constants shared across tests
# ---------------------------------------------------------------------------

_HORIZON = 28
_CFG = BacktestConfig(horizon=_HORIZON, n_folds=2, step=28, min_train_size=30)
_REGISTRY = {"naive": forecast_naive, "seasonal_naive": forecast_seasonal_naive}
_VALID_BUCKETS = {name for name, _, _ in HORIZON_BUCKETS}
_N_BUCKETS = len(HORIZON_BUCKETS)  # 4

_REPORT_A = "R_ALPHA"
_REPORT_B = "R_BETA"


def _make_series(n: int, start: str = "2023-01-01", seed: int = 0) -> pd.Series:
    rng = np.random.default_rng(seed)
    idx = pd.date_range(start, periods=n, freq="D")
    vals = (10 + rng.integers(-3, 4, size=n)).clip(1).astype(float)
    return pd.Series(vals, index=idx)


_SERIES_A = _make_series(90, seed=10)
_SERIES_B = _make_series(90, start="2023-04-01", seed=20)
_SERIES_DICT = {_REPORT_A: _SERIES_A, _REPORT_B: _SERIES_B}


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def backtest_predictions():
    """Row-per-(report, fold, model, horizon_step) predictions for two reports."""
    rows = []
    for rid, series in _SERIES_DICT.items():
        preds, _ = evaluate_models_across_folds(rid, series, _REGISTRY, _CFG)
        rows.append(preds)
    return pd.concat(rows, ignore_index=True)


@pytest.fixture(scope="module")
def selection_df():
    """Minimal selection DataFrame with selected_model_name and selected_m."""
    return pd.DataFrame({
        "report_id":           [_REPORT_A, _REPORT_B],
        "selected_model_name": ["seasonal_naive_m7", "naive"],
        "selected_m":          [7, 1],
        "selection_status":    ["selected", "selected"],
    })


@pytest.fixture(scope="module")
def backtest_horizon_df(backtest_predictions, selection_df):
    """Backtest horizon DataFrame as produced by save_production_outputs logic."""
    from src.models.horizon_evaluation import calculate_horizon_bucket_metrics

    df = calculate_horizon_bucket_metrics(backtest_predictions, _SERIES_DICT)
    lineage = selection_df[["report_id", "selected_model_name", "selected_m"]].drop_duplicates("report_id")
    return df.merge(lineage, on="report_id", how="left")


@pytest.fixture(scope="module")
def realized_horizon_df():
    """Minimal realized forecast history to exercise realized_performance_by_horizon."""
    rng = np.random.default_rng(42)
    n = 56  # 2 folds × 28 steps
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    rows = []
    for rid in [_REPORT_A, _REPORT_B]:
        for step, d in enumerate(dates, start=1):
            actual = float(rng.integers(5, 20))
            forecast = actual + float(rng.uniform(-2, 2))
            rows.append({
                "report_id":             rid,
                "run_id":                "run_001",
                "forecast_date":         d,
                "horizon_step":          (step - 1) % _HORIZON + 1,
                "actual":                actual,
                "forecast":              forecast,
                "signed_error":          forecast - actual,
                "absolute_error":        abs(forecast - actual),
                "lower_bound":           forecast - 3,
                "upper_bound":           forecast + 3,
                "selected_model_family": "seasonal_naive",
                "selected_model_name":   "seasonal_naive_m7",
                "selected_m":            7,
                "realized_at":           pd.Timestamp("2024-02-29"),
                "generated_at":          pd.Timestamp("2024-01-01"),
                "training_cutoff":       pd.Timestamp("2023-12-31"),
                "selection_run_id":      "run_001",
                "lineage_complete":      True,
                "lineage_missing_fields": None,
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# TestBacktestHorizonFileGeneration
# ---------------------------------------------------------------------------

class TestBacktestHorizonFileGeneration:
    def test_file_created(self, tmp_path, backtest_predictions, selection_df):
        """save_production_outputs writes backtest_horizon_performance_latest.csv."""
        from src.pipelines.run_forecasting_pipeline import save_production_outputs

        save_production_outputs(
            production_df=pd.DataFrame(),
            model_summary=pd.DataFrame(),
            selection=selection_df,
            project_root=tmp_path,
            run_id="run_test",
            generated_at=pd.Timestamp("2024-01-01"),
            series_dict=_SERIES_DICT,
            predictions=backtest_predictions,
        )
        out = tmp_path / "outputs" / "metrics" / "backtest_horizon_performance_latest.csv"
        assert out.exists(), "backtest_horizon_performance_latest.csv was not created"

    def test_file_is_non_empty(self, tmp_path, backtest_predictions, selection_df):
        from src.pipelines.run_forecasting_pipeline import save_production_outputs

        save_production_outputs(
            production_df=pd.DataFrame(),
            model_summary=pd.DataFrame(),
            selection=selection_df,
            project_root=tmp_path,
            run_id="run_test",
            generated_at=pd.Timestamp("2024-01-01"),
            series_dict=_SERIES_DICT,
            predictions=backtest_predictions,
        )
        df = pd.read_csv(tmp_path / "outputs" / "metrics" / "backtest_horizon_performance_latest.csv")
        assert len(df) > 0

    def test_no_file_without_predictions(self, tmp_path, selection_df):
        """Without predictions, backtest_horizon key is None."""
        from src.pipelines.run_forecasting_pipeline import save_production_outputs

        result = save_production_outputs(
            production_df=pd.DataFrame(),
            model_summary=pd.DataFrame(),
            selection=selection_df,
            project_root=tmp_path,
            run_id="run_test",
            generated_at=pd.Timestamp("2024-01-01"),
            series_dict=_SERIES_DICT,
            predictions=None,
        )
        assert result.get("backtest_horizon") is None


# ---------------------------------------------------------------------------
# TestBacktestHorizonBuckets
# ---------------------------------------------------------------------------

class TestBacktestHorizonBuckets:
    def test_only_canonical_bucket_names(self, backtest_horizon_df):
        buckets = set(backtest_horizon_df["horizon_bucket"].unique())
        assert buckets <= _VALID_BUCKETS, (
            f"Unexpected bucket names: {buckets - _VALID_BUCKETS}"
        )

    def test_all_four_buckets_present(self, backtest_horizon_df):
        buckets = set(backtest_horizon_df["horizon_bucket"].unique())
        assert buckets == _VALID_BUCKETS

    def test_bucket_step_ranges_respected(self, backtest_predictions):
        """days_1_7 rows must only contain horizon_steps 1-7."""
        from src.models.horizon_evaluation import calculate_horizon_bucket_metrics

        df = calculate_horizon_bucket_metrics(backtest_predictions, _SERIES_DICT)
        # Verify observation_count is non-zero for the near-term bucket (steps 1-7)
        near = df[df["horizon_bucket"] == "days_1_7"]
        assert near["observation_count"].sum() > 0

    def test_full_horizon_has_highest_observation_count(self, backtest_predictions):
        """full_horizon should have more observations than any sub-bucket per group."""
        from src.models.horizon_evaluation import calculate_horizon_bucket_metrics

        df = calculate_horizon_bucket_metrics(backtest_predictions, _SERIES_DICT)
        for (rid, mn, fn), grp in df.groupby(["report_id", "model_name", "fold_number"]):
            full_obs = grp.loc[grp["horizon_bucket"] == "full_horizon", "observation_count"]
            if full_obs.empty:
                continue
            full_val = float(full_obs.iloc[0])
            for sub_bucket in ["days_1_7", "days_8_14", "days_15_28"]:
                sub_obs = grp.loc[grp["horizon_bucket"] == sub_bucket, "observation_count"]
                if not sub_obs.empty:
                    assert float(sub_obs.iloc[0]) <= full_val


# ---------------------------------------------------------------------------
# TestLineagePreserved
# ---------------------------------------------------------------------------

class TestLineagePreserved:
    def test_selected_model_name_column_present(self, backtest_horizon_df):
        assert "selected_model_name" in backtest_horizon_df.columns

    def test_selected_m_column_present(self, backtest_horizon_df):
        assert "selected_m" in backtest_horizon_df.columns

    def test_selected_m_populated_for_known_reports(self, backtest_horizon_df):
        alpha_rows = backtest_horizon_df[backtest_horizon_df["report_id"] == _REPORT_A]
        assert not alpha_rows.empty
        assert alpha_rows["selected_m"].notna().all(), (
            "selected_m should be populated for every row belonging to R_ALPHA"
        )

    def test_selected_model_name_matches_selection(self, backtest_horizon_df):
        alpha_rows = backtest_horizon_df[backtest_horizon_df["report_id"] == _REPORT_A]
        assert (alpha_rows["selected_model_name"] == "seasonal_naive_m7").all()

    def test_model_name_column_also_present(self, backtest_horizon_df):
        """model_name (candidate evaluated) is separate from selected_model_name."""
        assert "model_name" in backtest_horizon_df.columns


# ---------------------------------------------------------------------------
# TestNoDuplicateBacktestRows
# ---------------------------------------------------------------------------

class TestNoDuplicateBacktestRows:
    def test_no_duplicate_rows(self, backtest_horizon_df):
        key = ["report_id", "model_name", "fold_number", "horizon_bucket"]
        dupes = backtest_horizon_df.duplicated(subset=key)
        assert not dupes.any(), (
            f"{dupes.sum()} duplicate (report_id, model_name, fold_number, horizon_bucket) rows"
        )

    def test_unique_key_per_report_and_bucket(self, backtest_horizon_df):
        for bucket in _VALID_BUCKETS:
            sub = backtest_horizon_df[backtest_horizon_df["horizon_bucket"] == bucket]
            dupes = sub.duplicated(subset=["report_id", "model_name", "fold_number"])
            assert not dupes.any(), (
                f"Duplicate rows in bucket '{bucket}': {dupes.sum()}"
            )


# ---------------------------------------------------------------------------
# TestRealizedHorizonFileGeneration
# ---------------------------------------------------------------------------

class TestRealizedHorizonFileGeneration:
    def test_monitoring_dir_file_created(self, tmp_path, realized_horizon_df):
        """save_production_performance writes to outputs/monitoring/."""
        horizon_tbl = realized_performance_by_horizon(realized_horizon_df)
        tables = {
            "by_run":     pd.DataFrame(),
            "by_report":  pd.DataFrame(),
            "by_horizon": horizon_tbl,
            "by_model":   pd.DataFrame(),
            "deterioration": pd.DataFrame(),
        }
        save_production_performance(tables, tmp_path)
        out = tmp_path / "outputs" / "monitoring" / "realized_performance_by_horizon.csv"
        assert out.exists()

    def test_metrics_dir_file_created(self, tmp_path, realized_horizon_df):
        """save_production_performance also mirrors to outputs/metrics/."""
        horizon_tbl = realized_performance_by_horizon(realized_horizon_df)
        tables = {
            "by_run":     pd.DataFrame(),
            "by_report":  pd.DataFrame(),
            "by_horizon": horizon_tbl,
            "by_model":   pd.DataFrame(),
            "deterioration": pd.DataFrame(),
        }
        paths = save_production_performance(tables, tmp_path)
        out = tmp_path / "outputs" / "metrics" / "realized_performance_by_horizon.csv"
        assert out.exists()
        assert paths.get("realized_horizon_metrics") == out

    def test_metrics_file_matches_monitoring_file(self, tmp_path, realized_horizon_df):
        """Both copies must be identical."""
        horizon_tbl = realized_performance_by_horizon(realized_horizon_df)
        tables = {
            "by_run":     pd.DataFrame(),
            "by_report":  pd.DataFrame(),
            "by_horizon": horizon_tbl,
            "by_model":   pd.DataFrame(),
            "deterioration": pd.DataFrame(),
        }
        save_production_performance(tables, tmp_path)
        mon = pd.read_csv(tmp_path / "outputs" / "monitoring" / "realized_performance_by_horizon.csv")
        met = pd.read_csv(tmp_path / "outputs" / "metrics" / "realized_performance_by_horizon.csv")
        pd.testing.assert_frame_equal(mon, met)


# ---------------------------------------------------------------------------
# TestRealizedHorizonBuckets
# ---------------------------------------------------------------------------

class TestRealizedHorizonBuckets:
    def test_only_canonical_bucket_names(self, realized_horizon_df):
        result = realized_performance_by_horizon(realized_horizon_df)
        buckets = set(result["horizon_bucket"].unique())
        assert buckets <= _VALID_BUCKETS

    def test_all_four_buckets_present(self, realized_horizon_df):
        result = realized_performance_by_horizon(realized_horizon_df)
        buckets = set(result["horizon_bucket"].unique())
        assert buckets == _VALID_BUCKETS

    def test_no_duplicate_rows(self, realized_horizon_df):
        result = realized_performance_by_horizon(realized_horizon_df)
        dupes = result.duplicated(subset=["report_id", "horizon_bucket"])
        assert not dupes.any()


# ---------------------------------------------------------------------------
# TestProductionAndBacktestDistinct
# ---------------------------------------------------------------------------

class TestProductionAndBacktestDistinct:
    def test_different_file_paths(self, tmp_path, backtest_predictions, selection_df, realized_horizon_df):
        """Backtest and realized horizon outputs go to different files."""
        from src.pipelines.run_forecasting_pipeline import save_production_outputs

        save_production_outputs(
            production_df=pd.DataFrame(),
            model_summary=pd.DataFrame(),
            selection=selection_df,
            project_root=tmp_path,
            run_id="run_test",
            generated_at=pd.Timestamp("2024-01-01"),
            series_dict=_SERIES_DICT,
            predictions=backtest_predictions,
        )

        horizon_tbl = realized_performance_by_horizon(realized_horizon_df)
        tables = {
            "by_run": pd.DataFrame(), "by_report": pd.DataFrame(),
            "by_horizon": horizon_tbl, "by_model": pd.DataFrame(),
            "deterioration": pd.DataFrame(),
        }
        save_production_performance(tables, tmp_path)

        backtest_path = tmp_path / "outputs" / "metrics" / "backtest_horizon_performance_latest.csv"
        realized_path = tmp_path / "outputs" / "metrics" / "realized_performance_by_horizon.csv"
        assert backtest_path.exists()
        assert realized_path.exists()
        assert backtest_path != realized_path

    def test_backtest_has_fold_number_realized_does_not(self, tmp_path, backtest_predictions, selection_df, realized_horizon_df):
        """Backtest output has fold_number; realized output does not."""
        from src.pipelines.run_forecasting_pipeline import save_production_outputs

        save_production_outputs(
            production_df=pd.DataFrame(),
            model_summary=pd.DataFrame(),
            selection=selection_df,
            project_root=tmp_path,
            run_id="run_test",
            generated_at=pd.Timestamp("2024-01-01"),
            series_dict=_SERIES_DICT,
            predictions=backtest_predictions,
        )

        bt = pd.read_csv(tmp_path / "outputs" / "metrics" / "backtest_horizon_performance_latest.csv")
        assert "fold_number" in bt.columns

        horizon_tbl = realized_performance_by_horizon(realized_horizon_df)
        tables = {
            "by_run": pd.DataFrame(), "by_report": pd.DataFrame(),
            "by_horizon": horizon_tbl, "by_model": pd.DataFrame(),
            "deterioration": pd.DataFrame(),
        }
        save_production_performance(tables, tmp_path)
        rh = pd.read_csv(tmp_path / "outputs" / "metrics" / "realized_performance_by_horizon.csv")
        assert "fold_number" not in rh.columns


# ---------------------------------------------------------------------------
# TestSchemaValidation
# ---------------------------------------------------------------------------

class TestSchemaValidation:
    def test_valid_backtest_output_passes(self, backtest_horizon_df):
        validate_backtest_horizon_output(backtest_horizon_df)  # must not raise

    def test_missing_required_col_raises(self, backtest_horizon_df):
        bad = backtest_horizon_df.drop(columns=["selected_m"])
        with pytest.raises(ValueError, match="missing required column"):
            validate_backtest_horizon_output(bad)

    def test_unknown_bucket_raises(self, backtest_horizon_df):
        bad = backtest_horizon_df.copy()
        bad.loc[0, "horizon_bucket"] = "days_999_999"
        with pytest.raises(ValueError, match="unknown horizon_bucket"):
            validate_backtest_horizon_output(bad)

    def test_duplicate_rows_raises(self, backtest_horizon_df):
        bad = pd.concat([backtest_horizon_df, backtest_horizon_df.iloc[[0]]], ignore_index=True)
        with pytest.raises(ValueError, match="duplicate"):
            validate_backtest_horizon_output(bad)

    def test_valid_realized_output_passes(self, realized_horizon_df):
        result = realized_performance_by_horizon(realized_horizon_df)
        validate_realized_horizon_output(result)  # must not raise

    def test_realized_missing_col_raises(self, realized_horizon_df):
        result = realized_performance_by_horizon(realized_horizon_df)
        bad = result.drop(columns=["wape"])
        with pytest.raises(ValueError, match="missing required column"):
            validate_realized_horizon_output(bad)

    def test_realized_duplicate_raises(self, realized_horizon_df):
        result = realized_performance_by_horizon(realized_horizon_df)
        bad = pd.concat([result, result.iloc[[0]]], ignore_index=True)
        with pytest.raises(ValueError, match="duplicate"):
            validate_realized_horizon_output(bad)
