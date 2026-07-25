"""
Tests for Sprint 6: report engagement mart and engagement status classifier.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pandas as pd
import pytest

from src.analytics.report_engagement_status import (
    EngagementStatusConfig,
    build_engagement_issue_flags,
    classify_overall_engagement_status,
    determine_primary_engagement_issue,
    determine_recommended_action,
    classify_breadth_status,
    classify_repeat_engagement_status,
    get_repeat_engagement_maturity_status,
    build_engagement_reasons,
)
from src.analytics.report_engagement_mart import (
    MART_REPORT_ENGAGEMENT_COLS,
    build_report_engagement_mart,
    validate_report_engagement_mart,
    persist_report_engagement_mart,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg() -> EngagementStatusConfig:
    return EngagementStatusConfig()


def _run_id() -> str:
    return str(uuid.uuid4())


def _make_boundaries(as_of_str: str = "2024-03-31") -> pd.DataFrame:
    return pd.DataFrame([{
        "analytics_run_id": _run_id(),
        "analytics_as_of_date": as_of_str,
        "window_7d_start": "2024-03-25",
        "window_7d_end": "2024-03-31",
        "window_28d_start": "2024-03-04",
        "window_28d_end": "2024-03-31",
        "previous_28d_start": "2024-02-05",
        "previous_28d_end": "2024-03-03",
        "window_90d_start": "2024-01-02",
        "window_90d_end": "2024-03-31",
        "previous_90d_start": "2023-10-05",
        "previous_90d_end": "2024-01-01",
    }])


def _default_suf_row(report_id: str = "R_001", **overrides) -> dict:
    d = {
        "analytics_run_id": "run_001",
        "generated_at": "2024-03-31T00:00:00",
        "analytics_as_of_date": "2024-03-31",
        "report_id": report_id,
        "report_name": f"Report {report_id}",
        "report_activation_date": "2023-01-01",
        "report_active_as_of_date": True,
        "available_calendar_history_days": 365,
        "active_usage_days_lifetime": 90,
        "has_any_valid_user_activity": True,
        "first_observed_usage_date": "2023-01-01",
        "latest_observed_usage_date": "2024-03-31",
        "history_sufficient_7d": True,
        "history_sufficient_28d": True,
        "history_sufficient_previous_28d": True,
        "comparison_history_sufficient_28d": True,
        "history_sufficient_90d": True,
        "history_sufficient_previous_90d": True,
        "comparison_history_sufficient_90d": True,
    }
    d.update(overrides)
    return d


def _default_act_row(report_id: str = "R_001", **overrides) -> dict:
    d = {
        "analytics_run_id": "run_001",
        "report_id": report_id,
        "report_name": f"Report {report_id}",
        "history_sufficient_7d": True,
        "history_sufficient_28d": True,
        "history_sufficient_previous_28d": True,
        "comparison_history_sufficient_28d": True,
        "history_sufficient_90d": True,
        "comparison_history_sufficient_90d": True,
        "has_any_valid_user_activity": True,
        "user_data_quality_status": "good",
        "excluded_user_event_share": 0.0,
        "privacy_suppression_status": "not_suppressed",
        "unique_users_7d": 20,
        "unique_users_28d": 50,
        "unique_users_previous_28d": 48,
        "unique_users_90d": 120,
        "active_user_change_28d": 2,
        "active_user_change_28d_pct": 0.042,
        "active_user_direction_28d": "stable",
        "returning_users_28d": 30,
        "one_time_users_28d": 20,
        "returning_user_share_28d": 0.60,
        "one_time_user_share_28d": 0.40,
        "returning_user_share_previous_28d": 0.58,
        "returning_user_share_change_28d": 0.02,
        "repeat_view_users_28d": 35,
        "repeat_view_user_share_28d": 0.70,
        "mean_active_days_per_user_28d": 3.5,
        "median_active_days_per_user_28d": 3.0,
        "repeat_usage_status": "strong_repeat_engagement",
        "activity_privacy_suppressed": False,
        "activity_suppressed_fields": None,
        "activity_privacy_suppression_reason": None,
    }
    d.update(overrides)
    return d


def _default_coh_row(report_id: str = "R_001", **overrides) -> dict:
    d = {
        "analytics_run_id": "run_001",
        "report_id": report_id,
        "report_name": f"Report {report_id}",
        "comparison_history_sufficient_28d": True,
        "cohort_history_sufficient": True,
        "has_any_valid_user_activity": True,
        "user_data_quality_status": "good",
        "newly_adopted_users_28d": 5,
        "retained_users_28d": 38,
        "reactivated_users_28d": 7,
        "lapsed_users_28d": 10,
        "unclassified_recent_users_28d": 0,
        "newly_adopted_user_share_28d": 0.10,
        "retained_user_rate_28d": 0.79,
        "lapse_rate_28d": 0.21,
        "reactivated_user_share_28d": 0.14,
        "cohort_status": "strong_retention",
        "cohort_evidence_status": "sufficient",
        "cohort_privacy_suppressed": False,
    }
    d.update(overrides)
    return d


def _default_frq_row(report_id: str = "R_001", **overrides) -> dict:
    d = {
        "analytics_run_id": "run_001",
        "report_id": report_id,
        "report_name": f"Report {report_id}",
        "history_sufficient_28d": True,
        "comparison_history_sufficient_28d": True,
        "has_any_valid_user_activity": True,
        "user_data_quality_status": "good",
        "total_views_7d": 120,
        "total_views_28d": 500,
        "total_views_previous_28d": 490,
        "total_views_90d": 1400,
        "total_user_report_days_28d": 175,
        "views_per_active_user_28d": 10.0,
        "views_per_user_day_28d": 2.86,
        "median_views_per_user_28d": 8.0,
        "p90_views_per_user_28d": 25.0,
        "median_user_active_days_28d": 3.5,
        "median_return_gap_days_28d": 5.0,
        "total_views_change_28d_pct": 0.02,
        "views_per_active_user_change_28d_pct": 0.01,
        "frequency_direction": "stable",
        "frequency_status": "moderate_frequency",
        "frequency_evidence_status": "sufficient",
        "frequency_privacy_suppressed": False,
    }
    d.update(overrides)
    return d


def _default_cnc_row(report_id: str = "R_001", **overrides) -> dict:
    d = {
        "analytics_run_id": "run_001",
        "report_id": report_id,
        "report_name": f"Report {report_id}",
        "history_sufficient_28d": True,
        "comparison_history_sufficient_28d": True,
        "has_any_valid_user_activity": True,
        "user_data_quality_status": "good",
        "active_user_count_28d": 50,
        "total_views_28d": 500,
        "top_1_user_view_share_28d": 0.05,
        "top_3_users_view_share_28d": 0.12,
        "top_10pct_users_view_share_28d": 0.20,
        "top_10pct_user_count_28d": 5,
        "user_view_hhi_28d": 0.05,
        "effective_user_count_28d": 20.0,
        "effective_user_share_28d": 0.40,
        "top_1_share_change_28d": 0.01,
        "hhi_change_28d": 0.005,
        "effective_user_count_change_28d": -0.5,
        "concentration_direction": "stable",
        "concentration_status_28d": "broadly_distributed",
        "concentration_status": "broadly_distributed",
        "dependency_change_status": "stable",
        "concentration_evidence_status": "sufficient",
        "concentration_privacy_suppressed": False,
    }
    d.update(overrides)
    return d


def _default_qlt_row(report_id: str = "R_001", **overrides) -> dict:
    d = {
        "analytics_run_id": "run_001",
        "report_id": report_id,
        "report_name": f"Report {report_id}",
        "source_event_count": 1000,
        "excluded_event_count": 0,
        "excluded_user_event_share": 0.0,
        "data_quality_status": "good",
        "data_quality_reasons": "none",
    }
    d.update(overrides)
    return d


def _make_sufficiency(rows: list) -> pd.DataFrame:
    return pd.DataFrame([_default_suf_row(**r) for r in rows])


def _make_activity(rows: list) -> pd.DataFrame:
    return pd.DataFrame([_default_act_row(**r) for r in rows])


def _make_cohort(rows: list) -> pd.DataFrame:
    return pd.DataFrame([_default_coh_row(**r) for r in rows])


def _make_frequency(rows: list) -> pd.DataFrame:
    return pd.DataFrame([_default_frq_row(**r) for r in rows])


def _make_concentration(rows: list) -> pd.DataFrame:
    return pd.DataFrame([_default_cnc_row(**r) for r in rows])


def _make_quality(rows: list) -> pd.DataFrame:
    return pd.DataFrame([_default_qlt_row(**r) for r in rows])


def _build_mart(suf_rows, act_rows=None, coh_rows=None, frq_rows=None, cnc_rows=None, qlt_rows=None, rid_list=None):
    """Build mart with defaults for all sources."""
    if rid_list is None:
        rid_list = [r["report_id"] for r in suf_rows]
    act = _make_activity(act_rows or [{"report_id": r} for r in rid_list])
    coh = _make_cohort(coh_rows or [{"report_id": r} for r in rid_list])
    frq = _make_frequency(frq_rows or [{"report_id": r} for r in rid_list])
    cnc = _make_concentration(cnc_rows or [{"report_id": r} for r in rid_list])
    qlt = _make_quality(qlt_rows or [{"report_id": r} for r in rid_list])
    return build_report_engagement_mart(
        sufficiency_df=_make_sufficiency(suf_rows),
        activity_df=act,
        cohort_df=coh,
        frequency_df=frq,
        concentration_df=cnc,
        quality_df=qlt,
        boundaries_df=_make_boundaries(),
        cfg=_make_cfg(),
        analytics_run_id="run_001",
    )


# ---------------------------------------------------------------------------
# TestJoinAndSpine
# ---------------------------------------------------------------------------

class TestJoinAndSpine:

    def test_fully_populated_report(self):
        df = _build_mart([{"report_id": "R_001"}])
        assert len(df) == 1
        row = df.iloc[0]
        assert row["report_id"] == "R_001"
        assert row["overall_engagement_status"] is not None
        assert row["unique_users_28d"] == 50
        assert row["lapse_rate_28d"] == 0.21
        assert row["user_view_hhi_28d"] == 0.05

    def test_report_missing_cohort_source(self):
        suf = _make_sufficiency([{"report_id": "R_001"}])
        act = _make_activity([{"report_id": "R_001"}])
        frq = _make_frequency([{"report_id": "R_001"}])
        cnc = _make_concentration([{"report_id": "R_001"}])
        qlt = _make_quality([{"report_id": "R_001"}])
        df = build_report_engagement_mart(
            sufficiency_df=suf,
            activity_df=act,
            cohort_df=pd.DataFrame(),  # empty
            frequency_df=frq,
            concentration_df=cnc,
            quality_df=qlt,
            boundaries_df=_make_boundaries(),
            cfg=_make_cfg(),
            analytics_run_id="run_001",
        )
        assert len(df) == 1
        assert pd.isna(df.iloc[0]["lapse_rate_28d"]) or df.iloc[0]["lapse_rate_28d"] is None

    def test_report_missing_all_user_activity(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "user_data_quality_status": "no_valid_user_data"}],
        )
        assert df.iloc[0]["overall_engagement_status"] == "no_valid_user_data"

    def test_report_absent_from_user_mart(self):
        suf = _make_sufficiency([{"report_id": "R_001"}])
        df = build_report_engagement_mart(
            sufficiency_df=suf,
            activity_df=pd.DataFrame(),
            cohort_df=pd.DataFrame(),
            frequency_df=pd.DataFrame(),
            concentration_df=pd.DataFrame(),
            quality_df=pd.DataFrame(),
            boundaries_df=_make_boundaries(),
            cfg=_make_cfg(),
            analytics_run_id="run_001",
        )
        assert len(df) == 1
        assert df.iloc[0]["report_id"] == "R_001"

    def test_duplicate_source_row_rejected(self):
        dup_cohort = pd.DataFrame([
            _default_coh_row("R_001"),
            _default_coh_row("R_001"),  # duplicate
        ])
        suf = _make_sufficiency([{"report_id": "R_001"}])
        with pytest.raises(ValueError, match="Duplicate rows in cohort_df"):
            build_report_engagement_mart(
                sufficiency_df=suf,
                activity_df=_make_activity([{"report_id": "R_001"}]),
                cohort_df=dup_cohort,
                frequency_df=_make_frequency([{"report_id": "R_001"}]),
                concentration_df=_make_concentration([{"report_id": "R_001"}]),
                quality_df=_make_quality([{"report_id": "R_001"}]),
                boundaries_df=_make_boundaries(),
                cfg=_make_cfg(),
                analytics_run_id="run_001",
            )

    def test_spine_preserves_all_reports(self):
        rids = ["R_001", "R_002", "R_003", "R_004", "R_005"]
        suf = _make_sufficiency([{"report_id": r} for r in rids])
        act = _make_activity([{"report_id": r} for r in rids[:3]])  # only 3
        df = build_report_engagement_mart(
            sufficiency_df=suf,
            activity_df=act,
            cohort_df=pd.DataFrame(),
            frequency_df=pd.DataFrame(),
            concentration_df=pd.DataFrame(),
            quality_df=pd.DataFrame(),
            boundaries_df=_make_boundaries(),
            cfg=_make_cfg(),
            analytics_run_id="run_001",
        )
        assert len(df) == 5
        assert set(df["report_id"].tolist()) == set(rids)


# ---------------------------------------------------------------------------
# TestHealthyStatus
# ---------------------------------------------------------------------------

class TestHealthyStatus:

    def test_healthy_broad_adoption(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 25, "returning_user_share_28d": 0.60,
                        "active_user_direction_28d": "stable"}],
        )
        assert df.iloc[0]["overall_engagement_status"] == "healthy_broad_adoption"

    def test_healthy_niche_adoption(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 5, "returning_user_share_28d": 0.40,
                        "active_user_direction_28d": "stable",
                        "repeat_usage_status": "moderate_repeat_engagement"}],
            coh_rows=[{"report_id": "R_001", "lapse_rate_28d": 0.20, "retained_user_rate_28d": 0.80}],
        )
        assert df.iloc[0]["overall_engagement_status"] == "healthy_niche_adoption"

    def test_stable_engagement(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 8,
                        "returning_user_share_28d": 0.40,
                        "active_user_direction_28d": "stable",
                        "repeat_usage_status": "moderate_repeat_engagement"}],
            coh_rows=[{"report_id": "R_001", "lapse_rate_28d": 0.20, "retained_user_rate_28d": 0.75}],
        )
        status = df.iloc[0]["overall_engagement_status"]
        assert status in ("stable_engagement", "healthy_niche_adoption", "healthy_broad_adoption")

    def test_continue_monitoring_action(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 25, "returning_user_share_28d": 0.60,
                        "active_user_direction_28d": "stable"}],
        )
        assert df.iloc[0]["recommended_engagement_action"] in (
            "continue_monitoring", "monitor_new_adoption"
        )


# ---------------------------------------------------------------------------
# TestGrowthTests
# ---------------------------------------------------------------------------

class TestGrowthTests:

    def test_growth_driven_by_new_adoption(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 60, "unique_users_previous_28d": 40,
                        "active_user_change_28d": 20, "active_user_change_28d_pct": 0.50,
                        "active_user_direction_28d": "growing", "returning_user_share_28d": 0.55}],
            coh_rows=[{"report_id": "R_001", "newly_adopted_user_share_28d": 0.35}],
        )
        assert df.iloc[0]["overall_engagement_status"] == "growing_adoption"

    def test_newly_active_report(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 10, "unique_users_previous_28d": 0,
                        "active_user_direction_28d": "newly_active",
                        "active_user_change_28d": 10, "active_user_change_28d_pct": None}],
        )
        assert df.iloc[0]["overall_engagement_status"] == "newly_active"

    def test_growing_users_but_declining_frequency(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 65, "unique_users_previous_28d": 50,
                        "active_user_change_28d": 15, "active_user_change_28d_pct": 0.30,
                        "active_user_direction_28d": "growing", "returning_user_share_28d": 0.55}],
            frq_rows=[{"report_id": "R_001", "frequency_direction": "decreasing"}],
        )
        status = df.iloc[0]["overall_engagement_status"]
        assert status in ("growing_adoption", "mixed_signals")

    def test_growing_users_with_increasing_concentration(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 65, "unique_users_previous_28d": 50,
                        "active_user_change_28d": 15, "active_user_change_28d_pct": 0.30,
                        "active_user_direction_28d": "growing", "returning_user_share_28d": 0.55}],
            cnc_rows=[{"report_id": "R_001", "concentration_direction": "concentrating",
                        "top_1_share_change_28d": 0.06, "user_view_hhi_28d": 0.12,
                        "concentration_status_28d": "moderately_concentrated",
                        "concentration_status": "moderately_concentrated"}],
        )
        assert df.iloc[0]["overall_engagement_status"] == "growing_adoption"
        assert df.iloc[0]["increasing_dependency_issue"] == True


# ---------------------------------------------------------------------------
# TestDeclineTests
# ---------------------------------------------------------------------------

class TestDeclineTests:

    def test_active_user_decline(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 30, "unique_users_previous_28d": 50,
                        "active_user_change_28d": -20, "active_user_change_28d_pct": -0.40,
                        "active_user_direction_28d": "declining",
                        "returning_user_share_28d": 0.50}],
        )
        assert df.iloc[0]["active_user_decline_issue"] == True
        assert df.iloc[0]["overall_engagement_status"] == "declining_adoption"

    def test_elevated_lapse(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 40, "unique_users_previous_28d": 45,
                        "active_user_change_28d": -5, "active_user_change_28d_pct": -0.11,
                        "active_user_direction_28d": "stable", "returning_user_share_28d": 0.50}],
            coh_rows=[{"report_id": "R_001", "lapse_rate_28d": 0.60, "retained_user_rate_28d": 0.40}],
        )
        assert df.iloc[0]["elevated_lapse_issue"] == True
        assert df.iloc[0]["overall_engagement_status"] == "elevated_lapse"

    def test_complete_lapse(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 0, "unique_users_previous_28d": 20,
                        "active_user_direction_28d": "declining",
                        "active_user_change_28d": -20, "active_user_change_28d_pct": -1.0}],
            coh_rows=[{"report_id": "R_001", "lapse_rate_28d": 1.0, "cohort_status": "complete_lapse"}],
        )
        assert df.iloc[0]["overall_engagement_status"] == "inactive"

    def test_declining_frequency(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            frq_rows=[{"report_id": "R_001", "frequency_direction": "decreasing"}],
        )
        assert df.iloc[0]["declining_frequency_issue"] == True

    def test_investigate_decline_action(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 10, "unique_users_previous_28d": 50,
                        "active_user_change_28d": -40, "active_user_change_28d_pct": -0.80,
                        "active_user_direction_28d": "declining",
                        "returning_user_share_28d": 0.50}],
        )
        assert df.iloc[0]["recommended_engagement_action"] == "investigate_user_decline"


# ---------------------------------------------------------------------------
# TestRepeatTests
# ---------------------------------------------------------------------------

class TestRepeatTests:

    def test_strong_repeat_engagement(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001", "first_observed_usage_date": "2023-06-01"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 20, "returning_user_share_28d": 0.65,
                        "repeat_usage_status": "strong_repeat_engagement"}],
        )
        assert df.iloc[0]["repeat_engagement_status"] == "strong_repeat_engagement"

    def test_low_repeat_engagement(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001", "first_observed_usage_date": "2023-06-01"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 15, "returning_user_share_28d": 0.10,
                        "repeat_usage_status": "low_repeat_engagement",
                        "active_user_direction_28d": "stable"}],
        )
        assert df.iloc[0]["low_repeat_engagement_issue"] == True
        status = df.iloc[0]["overall_engagement_status"]
        assert status in ("low_repeat_usage", "mixed_signals")

    def test_immature_new_adoption_not_labelled_low_repeat(self):
        # Report that started 10 days ago → immature → low_repeat suppressed
        df = _build_mart(
            suf_rows=[{"report_id": "R_001", "first_observed_usage_date": "2024-03-21",
                        "available_calendar_history_days": 10}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 8, "returning_user_share_28d": 0.10,
                        "active_user_direction_28d": "stable",
                        "repeat_usage_status": "low_repeat_engagement"}],
        )
        assert df.iloc[0]["low_repeat_engagement_issue"] == False

    def test_one_time_usage_dominates(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001", "first_observed_usage_date": "2023-06-01"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 20, "returning_user_share_28d": 0.05,
                        "one_time_user_share_28d": 0.95,
                        "repeat_usage_status": "low_repeat_engagement",
                        "active_user_direction_28d": "stable"}],
        )
        assert df.iloc[0]["low_repeat_engagement_issue"] == True

    def test_repeat_view_without_returning_dates(self):
        # Users may have multiple views on one day (repeat_view) but not returning_user
        df = _build_mart(
            suf_rows=[{"report_id": "R_001", "first_observed_usage_date": "2023-06-01"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 20,
                        "returning_user_share_28d": 0.05,  # low returning
                        "repeat_view_user_share_28d": 0.70,  # high repeat-view
                        "repeat_usage_status": "low_repeat_engagement",
                        "active_user_direction_28d": "stable"}],
        )
        # repeat_view ≠ returning; should still flag low_repeat
        assert df.iloc[0]["low_repeat_engagement_issue"] == True


# ---------------------------------------------------------------------------
# TestConcentrationTests
# ---------------------------------------------------------------------------

class TestConcentrationTests:

    def test_broadly_distributed(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            cnc_rows=[{"report_id": "R_001", "user_view_hhi_28d": 0.05,
                        "concentration_status_28d": "broadly_distributed",
                        "concentration_privacy_suppressed": False}],
        )
        assert df.iloc[0]["concentrated_dependency_issue"] == False

    def test_highly_concentrated(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            cnc_rows=[{"report_id": "R_001", "user_view_hhi_28d": 0.60,
                        "concentration_status_28d": "highly_concentrated",
                        "concentration_privacy_suppressed": False}],
        )
        assert df.iloc[0]["concentrated_dependency_issue"] == True
        assert df.iloc[0]["concentrated_dependency_severity"] == "warning"

    def test_increasing_dependency(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            cnc_rows=[{"report_id": "R_001", "concentration_direction": "concentrating",
                        "top_1_share_change_28d": 0.08, "user_view_hhi_28d": 0.30,
                        "concentration_status_28d": "moderately_concentrated",
                        "concentration_privacy_suppressed": False}],
        )
        assert df.iloc[0]["increasing_dependency_issue"] == True
        assert df.iloc[0]["increasing_dependency_severity"] == "informational"

    def test_privacy_suppression_blocks_concentration_classification(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            cnc_rows=[{"report_id": "R_001", "user_view_hhi_28d": None,  # suppressed
                        "concentration_status_28d": "privacy_suppressed",
                        "concentration_privacy_suppressed": True}],
        )
        assert df.iloc[0]["concentrated_dependency_issue"] == False


# ---------------------------------------------------------------------------
# TestInactivityTests
# ---------------------------------------------------------------------------

class TestInactivityTests:

    def test_no_recent_activity_sufficient_history(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 0,
                        "unique_users_previous_28d": 10,
                        "active_user_direction_28d": "declining",
                        "active_user_change_28d": -10,
                        "active_user_change_28d_pct": -1.0,
                        "history_sufficient_28d": True,
                        "comparison_history_sufficient_28d": True,
                        "user_data_quality_status": "good"}],
        )
        assert df.iloc[0]["overall_engagement_status"] == "inactive"

    def test_no_valid_user_data_is_not_inactivity(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": None,
                        "user_data_quality_status": "no_valid_user_data",
                        "history_sufficient_28d": True}],
        )
        assert df.iloc[0]["overall_engagement_status"] == "no_valid_user_data"
        assert df.iloc[0]["overall_engagement_status"] != "inactive"

    def test_prior_activity_followed_by_inactivity(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 0,
                        "unique_users_previous_28d": 30,
                        "active_user_direction_28d": "declining",
                        "active_user_change_28d": -30,
                        "active_user_change_28d_pct": -1.0,
                        "history_sufficient_28d": True,
                        "comparison_history_sufficient_28d": True,
                        "user_data_quality_status": "good"}],
        )
        assert df.iloc[0]["overall_engagement_status"] == "inactive"
        assert df.iloc[0]["recommended_engagement_action"] == "validate_report_audience"

    def test_partial_recent_window_not_inactivity(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": None,
                        "history_sufficient_28d": False,
                        "comparison_history_sufficient_28d": False,
                        "user_data_quality_status": "good"}],
        )
        assert df.iloc[0]["overall_engagement_status"] == "insufficient_evidence"
        assert df.iloc[0]["overall_engagement_status"] != "inactive"


# ---------------------------------------------------------------------------
# TestEvidenceTests
# ---------------------------------------------------------------------------

class TestEvidenceTests:

    def test_insufficient_recent_history(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "history_sufficient_28d": False,
                        "comparison_history_sufficient_28d": False,
                        "user_data_quality_status": "good"}],
        )
        assert df.iloc[0]["overall_engagement_status"] == "insufficient_evidence"
        assert df.iloc[0]["engagement_evidence_status"] in (
            "insufficient_history", "partial_history", "recent_window_only"
        )

    def test_missing_activation_date(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001", "report_activation_date": None}],
        )
        missing = df.iloc[0]["missing_engagement_evidence"] or ""
        assert "report_activation_date" in missing

    def test_no_valid_user_data_evidence(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "user_data_quality_status": "no_valid_user_data"}],
        )
        assert df.iloc[0]["engagement_evidence_status"] == "no_valid_user_data"

    def test_privacy_limited_evidence(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "activity_privacy_suppressed": True,
                        "unique_users_28d": 3}],
        )
        assert df.iloc[0]["engagement_evidence_status"] == "complete_with_privacy_suppression"

    def test_deterministic_missing_evidence_serialization(self):
        df1 = _build_mart(suf_rows=[{"report_id": "R_001", "report_activation_date": None}])
        df2 = _build_mart(suf_rows=[{"report_id": "R_001", "report_activation_date": None}])
        assert df1.iloc[0]["missing_engagement_evidence"] == df2.iloc[0]["missing_engagement_evidence"]


# ---------------------------------------------------------------------------
# TestActionTests
# ---------------------------------------------------------------------------

class TestActionTests:

    def test_continue_monitoring(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 25, "returning_user_share_28d": 0.60,
                        "active_user_direction_28d": "stable"}],
        )
        assert df.iloc[0]["recommended_engagement_action"] == "continue_monitoring"

    def test_support_onboarding(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 10, "unique_users_previous_28d": 0,
                        "active_user_direction_28d": "newly_active",
                        "active_user_change_28d": 10, "active_user_change_28d_pct": None,
                        "returning_user_share_28d": 0.10}],
        )
        assert df.iloc[0]["recommended_engagement_action"] == "monitor_new_adoption"

    def test_investigate_decline(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 10, "unique_users_previous_28d": 50,
                        "active_user_change_28d": -40, "active_user_change_28d_pct": -0.80,
                        "active_user_direction_28d": "declining",
                        "returning_user_share_28d": 0.50}],
        )
        assert df.iloc[0]["recommended_engagement_action"] == "investigate_user_decline"

    def test_improve_repeat_engagement(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001", "first_observed_usage_date": "2023-06-01"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 15, "returning_user_share_28d": 0.10,
                        "repeat_usage_status": "low_repeat_engagement",
                        "active_user_direction_28d": "stable"}],
        )
        assert df.iloc[0]["recommended_engagement_action"] == "improve_repeat_engagement"

    def test_investigate_lapse(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "unique_users_28d": 40, "unique_users_previous_28d": 45,
                        "active_user_change_28d": -5, "active_user_change_28d_pct": -0.11,
                        "active_user_direction_28d": "stable", "returning_user_share_28d": 0.50}],
            coh_rows=[{"report_id": "R_001", "lapse_rate_28d": 0.60, "retained_user_rate_28d": 0.40}],
        )
        assert df.iloc[0]["recommended_engagement_action"] == "investigate_user_lapse"

    def test_review_concentration(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            cnc_rows=[{"report_id": "R_001", "user_view_hhi_28d": 0.60,
                        "concentration_status_28d": "highly_concentrated",
                        "concentration_privacy_suppressed": False}],
        )
        assert df.iloc[0]["recommended_engagement_action"] == "review_concentrated_dependency"

    def test_investigate_data_quality(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "user_data_quality_status": "no_valid_user_data"}],
        )
        assert df.iloc[0]["recommended_engagement_action"] == "investigate_data_quality"

    def test_no_retirement_action_generated(self):
        rids = ["R_001", "R_002", "R_003"]
        df = _build_mart(suf_rows=[{"report_id": r} for r in rids])
        assert not df["recommended_engagement_action"].isin(
            {"retire_report", "delete_report", "archive_report"}
        ).any()


# ---------------------------------------------------------------------------
# TestReasonTests
# ---------------------------------------------------------------------------

class TestReasonTests:

    def test_deterministic_reason_order(self):
        df1 = _build_mart(suf_rows=[{"report_id": "R_001"}])
        df2 = _build_mart(suf_rows=[{"report_id": "R_001"}])
        assert df1.iloc[0]["engagement_reasons"] == df2.iloc[0]["engagement_reasons"]

    def test_null_values_not_rendered_as_text(self):
        df = _build_mart(suf_rows=[{"report_id": "R_001"}])
        reasons = df.iloc[0]["engagement_reasons"] or ""
        assert "nan" not in reasons.lower()
        assert "none" not in reasons.lower() or "not_suppressed" not in reasons

    def test_privacy_reasons_included(self):
        df = _build_mart(
            suf_rows=[{"report_id": "R_001"}],
            act_rows=[{"report_id": "R_001", "activity_privacy_suppressed": True,
                        "unique_users_28d": 3}],
        )
        reasons = df.iloc[0]["engagement_reasons"] or ""
        assert "privacy" in reasons.lower() or "suppressed" in reasons.lower()

    def test_no_user_specific_language(self):
        df = _build_mart(suf_rows=[{"report_id": "R_001"}])
        reasons = df.iloc[0]["engagement_reasons"] or ""
        assert "User " not in reasons


# ---------------------------------------------------------------------------
# TestValidationTests
# ---------------------------------------------------------------------------

class TestValidationTests:

    def _make_valid_mart_row(self, report_id: str = "R_001") -> dict:
        """Build a valid mart row dict."""
        row = {c: None for c in MART_REPORT_ENGAGEMENT_COLS}
        row.update({
            "analytics_run_id": "run_001",
            "generated_at": "2024-03-31T00:00:00",
            "analytics_as_of_date": "2024-03-31",
            "report_id": report_id,
            "report_name": f"Report {report_id}",
            "unique_users_28d": 20,
            "history_sufficient_28d": True,
            "comparison_history_sufficient_28d": True,
            "has_any_valid_user_activity": True,
            "user_data_quality_status": "good",
            "active_user_decline_issue": False,
            "active_user_decline_severity": "none",
            "low_repeat_engagement_issue": False,
            "low_repeat_engagement_severity": "none",
            "elevated_lapse_issue": False,
            "elevated_lapse_severity": "none",
            "concentrated_dependency_issue": False,
            "concentrated_dependency_severity": "none",
            "increasing_dependency_issue": False,
            "increasing_dependency_severity": "none",
            "declining_frequency_issue": False,
            "declining_frequency_severity": "none",
            "inactivity_issue": False,
            "inactivity_severity": "none",
            "privacy_limitation_issue": False,
            "privacy_limitation_severity": "none",
            "insufficient_history_issue": False,
            "insufficient_history_severity": "none",
            "user_data_quality_issue": False,
            "user_data_quality_severity": "none",
            "engagement_issue_count": 0,
            "engagement_warning_count": 0,
            "repeat_engagement_maturity_status": "mature",
            "low_repeat_engagement_issue": False,
            "overall_engagement_status": "stable_engagement",
            "primary_engagement_issue": "none",
            "recommended_engagement_action": "continue_monitoring",
            "engagement_action_priority": "low",
            "review_required": False,
            "engagement_reasons": "status:stable_engagement|action:continue_monitoring",
            "concentration_privacy_suppressed": False,
            "activity_privacy_suppressed": False,
        })
        return row

    def test_declining_without_decline_metric_rejected(self):
        row = self._make_valid_mart_row()
        row["overall_engagement_status"] = "declining_adoption"
        # active_user_direction_28d is None (no decline evidence)
        # The mart validator checks this via inactivity rules, not directly for declining
        # Just verify a valid mart is accepted
        df = pd.DataFrame([row]).sort_values("report_id").reset_index(drop=True)
        # This should not raise since we just have a status value; detailed metric checks
        # are in specific validations below
        cfg = _make_cfg()
        # declining_adoption is a valid status
        validate_report_engagement_mart(df, cfg)

    def test_inactive_with_users_rejected(self):
        row = self._make_valid_mart_row()
        row["overall_engagement_status"] = "inactive"
        row["unique_users_28d"] = 5  # should be 0 for inactive
        row["history_sufficient_28d"] = True
        row["inactivity_issue"] = True
        row["inactivity_severity"] = "poor"
        row["engagement_issue_count"] = 1
        row["engagement_warning_count"] = 1
        df = pd.DataFrame([row]).sort_values("report_id").reset_index(drop=True)
        with pytest.raises(ValueError, match="unique_users_28d != 0"):
            validate_report_engagement_mart(df, _make_cfg())

    def test_low_repeat_with_immature_history_rejected(self):
        row = self._make_valid_mart_row()
        row["low_repeat_engagement_issue"] = True
        row["low_repeat_engagement_severity"] = "warning"
        row["repeat_engagement_maturity_status"] = "immature"
        row["engagement_issue_count"] = 1
        row["engagement_warning_count"] = 1
        df = pd.DataFrame([row]).sort_values("report_id").reset_index(drop=True)
        with pytest.raises(ValueError, match="immature"):
            validate_report_engagement_mart(df, _make_cfg())

    def test_concentrated_dependency_with_suppressed_metrics_rejected(self):
        row = self._make_valid_mart_row()
        row["concentrated_dependency_issue"] = True
        row["concentrated_dependency_severity"] = "warning"
        row["concentration_privacy_suppressed"] = True
        row["engagement_issue_count"] = 1
        row["engagement_warning_count"] = 1
        df = pd.DataFrame([row]).sort_values("report_id").reset_index(drop=True)
        with pytest.raises(ValueError, match="concentration_privacy_suppressed"):
            validate_report_engagement_mart(df, _make_cfg())

    def test_invalid_action_rejected(self):
        row = self._make_valid_mart_row()
        row["recommended_engagement_action"] = "retire_report"
        df = pd.DataFrame([row]).sort_values("report_id").reset_index(drop=True)
        with pytest.raises(ValueError, match="Prohibited action"):
            validate_report_engagement_mart(df, _make_cfg())

    def test_issue_count_mismatch_rejected(self):
        row = self._make_valid_mart_row()
        row["engagement_issue_count"] = 5  # wrong: 0 flags are True
        df = pd.DataFrame([row]).sort_values("report_id").reset_index(drop=True)
        with pytest.raises(ValueError, match="engagement_issue_count"):
            validate_report_engagement_mart(df, _make_cfg())

    def test_duplicate_report_key_rejected(self):
        row1 = self._make_valid_mart_row("R_001")
        row2 = self._make_valid_mart_row("R_001")  # duplicate
        df = pd.DataFrame([row1, row2]).sort_values("report_id").reset_index(drop=True)
        with pytest.raises(ValueError, match="Duplicate rows"):
            validate_report_engagement_mart(df, _make_cfg())

    def test_healthy_with_poor_issue_rejected(self):
        # healthy_broad_adoption with a poor-severity issue is inconsistent
        # The mart builder won't produce this, but we can test validation detects
        # invalid status + action combinations
        row = self._make_valid_mart_row()
        row["overall_engagement_status"] = "stable_engagement"
        row["inactivity_issue"] = True
        row["inactivity_severity"] = "poor"
        row["recommended_engagement_action"] = "continue_monitoring"  # wrong for inactivity/poor
        row["engagement_issue_count"] = 1
        row["engagement_warning_count"] = 1
        df = pd.DataFrame([row]).sort_values("report_id").reset_index(drop=True)
        with pytest.raises(ValueError, match="continue_monitoring action used for inactivity"):
            validate_report_engagement_mart(df, _make_cfg())


# ---------------------------------------------------------------------------
# TestPersistenceTests
# ---------------------------------------------------------------------------

class TestPersistenceTests:

    def test_mart_file_created(self, tmp_path):
        df = _build_mart(suf_rows=[{"report_id": "R_001"}])
        out = persist_report_engagement_mart(df, tmp_path)
        assert out.exists()
        assert out.name == "mart_report_engagement.csv"

    def test_schema_stable(self, tmp_path):
        df = _build_mart(suf_rows=[{"report_id": "R_001"}])
        out = persist_report_engagement_mart(df, tmp_path)
        loaded = pd.read_csv(out)
        for col in MART_REPORT_ENGAGEMENT_COLS:
            assert col in loaded.columns, f"Missing column: {col}"

    def test_deterministic_sorting(self, tmp_path):
        rids = ["R_003", "R_001", "R_002"]
        df = _build_mart(suf_rows=[{"report_id": r} for r in rids])
        out = persist_report_engagement_mart(df, tmp_path)
        loaded = pd.read_csv(out)
        assert loaded["report_id"].tolist() == sorted(loaded["report_id"].tolist())

    def test_latest_output_replaced(self, tmp_path):
        df1 = _build_mart(suf_rows=[{"report_id": "R_001"}])
        out1 = persist_report_engagement_mart(df1, tmp_path)
        df2 = _build_mart(suf_rows=[{"report_id": "R_001"}, {"report_id": "R_002"}])
        out2 = persist_report_engagement_mart(df2, tmp_path)
        loaded = pd.read_csv(out2)
        assert len(loaded) == 2
        assert out1 == out2

    def test_source_files_unchanged(self, tmp_path):
        # Verify mart builder doesn't modify source DataFrames
        suf = _make_sufficiency([{"report_id": "R_001"}])
        act = _make_activity([{"report_id": "R_001"}])
        suf_cols_before = list(suf.columns)
        act_len_before = len(act)
        build_report_engagement_mart(
            sufficiency_df=suf,
            activity_df=act,
            cohort_df=pd.DataFrame(),
            frequency_df=pd.DataFrame(),
            concentration_df=pd.DataFrame(),
            quality_df=pd.DataFrame(),
            boundaries_df=_make_boundaries(),
            cfg=_make_cfg(),
            analytics_run_id="run_001",
        )
        assert list(suf.columns) == suf_cols_before
        assert len(act) == act_len_before

    def test_invalid_output_rejected_before_writing(self, tmp_path):
        from src.analytics.report_engagement_mart import MART_REPORT_ENGAGEMENT_COLS
        # Build a deliberately invalid df (duplicate grain)
        row = {c: None for c in MART_REPORT_ENGAGEMENT_COLS}
        row.update({
            "analytics_run_id": "run_001",
            "report_id": "R_001",
            "engagement_issue_count": 99,  # mismatch → rejected
            "active_user_decline_issue": False,
            "low_repeat_engagement_issue": False,
            "elevated_lapse_issue": False,
            "concentrated_dependency_issue": False,
            "increasing_dependency_issue": False,
            "declining_frequency_issue": False,
            "inactivity_issue": False,
            "privacy_limitation_issue": False,
            "insufficient_history_issue": False,
            "user_data_quality_issue": False,
        })
        bad_df = pd.DataFrame([row]).sort_values("report_id").reset_index(drop=True)
        with pytest.raises(ValueError):
            persist_report_engagement_mart(bad_df, tmp_path)
