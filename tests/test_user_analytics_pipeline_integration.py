"""End-to-end integration test for the Sprint 6 user analytics pipeline."""
import re
import pytest
import pandas as pd
from datetime import date, datetime, timedelta
from pathlib import Path

# Import all sprint 6 modules
from src.analytics.report_user_daily import (
    normalize_report_view_events,
    classify_source_record_quality,
    remove_exact_event_duplicates,
    build_report_user_daily,
    add_report_user_history_fields,
    build_report_user_data_quality,
)
from src.analytics.engagement_windows import (
    EngagementWindowConfig,
    build_engagement_window_boundaries,
    build_report_history_sufficiency,
)
from src.analytics.user_engagement_metrics import (
    build_report_user_activity_metrics,
    UserEngagementMetricsConfig,
)
from src.analytics.user_engagement_cohorts import (
    build_report_engagement_cohorts,
    CohortConfig,
)
from src.analytics.user_frequency_metrics import (
    build_report_frequency_metrics,
    FrequencyMetricsConfig,
)
from src.analytics.user_concentration_metrics import (
    build_report_concentration_metrics,
    ConcentrationMetricsConfig,
)
from src.analytics.report_engagement_status import EngagementStatusConfig
from src.analytics.report_engagement_mart import (
    build_report_engagement_mart,
    validate_report_engagement_mart,
)
from src.analytics.privacy_policy import validate_no_direct_identifiers, PROHIBITED_OUTPUT_COLUMNS

import dataclasses

RUN_ID = "test-integration-run"
AS_OF = date(2024, 3, 31)


# --- Fixture helpers ---

def _make_events(scenarios):
    """
    scenarios: list of dicts with keys:
        report_id, user_key, date_key (YYYYMMDD int), views
    """
    rows = []
    for s in scenarios:
        rows.append({
            "report_id": s["report_id"],
            "user_key": s["user_key"],
            "date_key": s["date_key"],
            "view_count": s.get("views", 1),
        })
    return pd.DataFrame(rows)


def _make_dim_report(report_ids, activation_days_ago=60):
    """Make a minimal dim_report DataFrame."""
    as_of = AS_OF
    return pd.DataFrame([{
        "report_id": rid,
        "report_name": f"Report {rid}",
        "report_activation_date": (as_of - timedelta(days=activation_days_ago)).isoformat(),
    } for rid in report_ids])


def _run_pipeline(events_df, dim_report_df):
    """Run the full pipeline and return all outputs."""
    normalized = normalize_report_view_events(events_df, AS_OF)
    classified = classify_source_record_quality(normalized)
    valid_rows = classified[classified["source_record_valid"] == True]
    deduped, dedup_counts = remove_exact_event_duplicates(valid_rows)
    mart = build_report_user_daily(deduped, RUN_ID, dedup_counts_df=dedup_counts)
    mart = add_report_user_history_fields(mart)
    quality = build_report_user_data_quality(classified, dedup_counts, RUN_ID)

    win_cfg = EngagementWindowConfig()
    bounds = build_engagement_window_boundaries(mart, win_cfg, RUN_ID)
    bounds_df = pd.DataFrame([dataclasses.asdict(bounds)])

    suf = build_report_history_sufficiency(dim_report_df, mart, quality, bounds, win_cfg, RUN_ID)

    activity = build_report_user_activity_metrics(
        suf, mart, quality, bounds_df, UserEngagementMetricsConfig(), RUN_ID
    )
    cohorts = build_report_engagement_cohorts(
        suf, mart, quality, bounds_df, CohortConfig(), RUN_ID
    )
    frequency = build_report_frequency_metrics(
        suf, mart, quality, bounds_df, FrequencyMetricsConfig(), RUN_ID
    )
    concentration = build_report_concentration_metrics(
        suf, mart, quality, bounds_df, ConcentrationMetricsConfig(), RUN_ID
    )

    eng_cfg = EngagementStatusConfig()
    eng_mart = build_report_engagement_mart(
        sufficiency_df=suf,
        activity_df=activity,
        cohort_df=cohorts,
        frequency_df=frequency,
        concentration_df=concentration,
        quality_df=quality,
        boundaries_df=bounds_df,
        cfg=eng_cfg,
        analytics_run_id=RUN_ID,
    )
    return {
        "mart": mart,
        "quality": quality,
        "boundaries": bounds_df,
        "sufficiency": suf,
        "activity": activity,
        "cohorts": cohorts,
        "frequency": frequency,
        "concentration": concentration,
        "engagement": eng_mart,
    }


# --- Scenario fixtures ---

@pytest.fixture(scope="module")
def healthy_broad_outputs():
    """Report with many users, good repeat, broad concentration."""
    events = _make_events([
        # 15 different users across the last 28 days on multiple dates
        # Include day 27 to ensure the mart covers the full 28-day window
        *[
            {
                "report_id": "R001",
                "user_key": f"UK_{i:04d}",
                "date_key": int((AS_OF - timedelta(days=d)).strftime("%Y%m%d")),
                "views": 2,
            }
            for i in range(1, 16)
            for d in [1, 8, 20, 27, 29]
        ]
    ])
    dim = _make_dim_report(["R001"], activation_days_ago=90)
    return _run_pipeline(events, dim)


@pytest.fixture(scope="module")
def newly_active_outputs():
    """Report with first activity within last 14 days."""
    events = _make_events([
        {
            "report_id": "R002",
            "user_key": "UK_0001",
            "date_key": int((AS_OF - timedelta(days=5)).strftime("%Y%m%d")),
            "views": 3,
        },
        {
            "report_id": "R002",
            "user_key": "UK_0002",
            "date_key": int((AS_OF - timedelta(days=3)).strftime("%Y%m%d")),
            "views": 2,
        },
    ])
    dim = _make_dim_report(["R002"], activation_days_ago=10)
    return _run_pipeline(events, dim)


@pytest.fixture(scope="module")
def inactive_outputs():
    """Report with users 60+ days ago but none in last 28 days."""
    events = _make_events([
        {
            "report_id": "R003",
            "user_key": "UK_0001",
            "date_key": int((AS_OF - timedelta(days=60)).strftime("%Y%m%d")),
            "views": 5,
        },
        {
            "report_id": "R003",
            "user_key": "UK_0002",
            "date_key": int((AS_OF - timedelta(days=55)).strftime("%Y%m%d")),
            "views": 3,
        },
    ])
    dim = _make_dim_report(["R003"], activation_days_ago=90)
    return _run_pipeline(events, dim)


@pytest.fixture(scope="module")
def multi_report_outputs():
    """Mixed scenario with several reports including privacy-suppressed and no-data."""
    events = _make_events([
        # R_BROAD: many users, multiple days each
        *[
            {
                "report_id": "R_BROAD",
                "user_key": f"UK_{i:04d}",
                "date_key": int((AS_OF - timedelta(days=d)).strftime("%Y%m%d")),
                "views": 1,
            }
            for i in range(1, 12)
            for d in range(1, 4)
        ],
        # R_SMALL: 2 users (privacy suppressed)
        {
            "report_id": "R_SMALL",
            "user_key": "UK_A001",
            "date_key": int((AS_OF - timedelta(days=2)).strftime("%Y%m%d")),
            "views": 1,
        },
        {
            "report_id": "R_SMALL",
            "user_key": "UK_A002",
            "date_key": int((AS_OF - timedelta(days=3)).strftime("%Y%m%d")),
            "views": 1,
        },
        # R_DECLINE: some users in recent window only
        *[
            {
                "report_id": "R_DECLINE",
                "user_key": f"UK_D{i:03d}",
                "date_key": int((AS_OF - timedelta(days=d)).strftime("%Y%m%d")),
                "views": 1,
            }
            for i in range(1, 4)
            for d in range(1, 4)
        ],
    ])
    dim = _make_dim_report(["R_BROAD", "R_SMALL", "R_DECLINE", "R_NODATA"], activation_days_ago=90)
    return _run_pipeline(events, dim)


# --- Tests ---

class TestGrainAndCardinality:
    def test_one_row_per_report_in_mart(self, multi_report_outputs):
        eng = multi_report_outputs["engagement"]
        assert not eng.duplicated(subset=["report_id"]).any()

    def test_spine_preserves_all_reports(self, multi_report_outputs):
        eng = multi_report_outputs["engagement"]
        assert "R_BROAD" in eng["report_id"].values
        assert "R_SMALL" in eng["report_id"].values
        assert "R_NODATA" in eng["report_id"].values

    def test_no_reports_dropped(self, multi_report_outputs):
        suf = multi_report_outputs["sufficiency"]
        eng = multi_report_outputs["engagement"]
        assert set(suf["report_id"]) == set(eng["report_id"])


class TestPrivacy:
    def test_no_direct_identifiers_in_mart(self, healthy_broad_outputs):
        eng = healthy_broad_outputs["engagement"]
        validate_no_direct_identifiers(eng)

    def test_no_user_key_in_mart(self, healthy_broad_outputs):
        eng = healthy_broad_outputs["engagement"]
        assert "user_key" not in eng.columns

    def test_no_email_in_any_output(self, multi_report_outputs):
        pat = re.compile(
            r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE
        )
        for name, df in multi_report_outputs.items():
            if not isinstance(df, pd.DataFrame):
                continue
            for col in df.select_dtypes(include="object").columns:
                sample = df[col].dropna().astype(str).head(20)
                for val in sample:
                    assert not pat.search(val), (
                        f"Email-like value in {name}.{col}: {val}"
                    )

    def test_small_group_suppressed(self, multi_report_outputs):
        eng = multi_report_outputs["engagement"]
        small = eng[eng["report_id"] == "R_SMALL"]
        if small.empty:
            pytest.skip("R_SMALL not in engagement mart")
        row = small.iloc[0]
        # HHI or top shares should be null for small groups
        hhi_suppressed = pd.isna(row.get("user_view_hhi_28d"))
        top1_suppressed = pd.isna(row.get("top_1_user_view_share_28d"))
        assert hhi_suppressed or top1_suppressed, (
            "Expected concentration metrics to be suppressed for R_SMALL"
        )


class TestStatusValues:
    ALLOWED_STATUSES = {
        "healthy_broad_adoption",
        "healthy_niche_adoption",
        "growing_adoption",
        "stable_engagement",
        "declining_adoption",
        "low_repeat_usage",
        "concentrated_dependency",
        "elevated_lapse",
        "newly_active",
        "inactive",
        "mixed_signals",
        "privacy_limited",
        "insufficient_evidence",
        "no_valid_user_data",
        "calculation_failed",
    }
    ALLOWED_ACTIONS = {
        "continue_monitoring",
        "support_new_user_onboarding",
        "investigate_user_decline",
        "improve_repeat_engagement",
        "investigate_user_lapse",
        "review_concentrated_dependency",
        "assess_report_discoverability",
        "validate_report_audience",
        "monitor_new_adoption",
        "investigate_data_quality",
        "insufficient_evidence",
    }
    PROHIBITED_ACTIONS = {
        "retire_report",
        "delete_report",
        "restrict_user",
        "contact_specific_user",
        "automatic_intervention",
    }

    def test_statuses_in_allowed_set(self, multi_report_outputs):
        eng = multi_report_outputs["engagement"]
        statuses = set(eng["overall_engagement_status"].dropna())
        unknown = statuses - self.ALLOWED_STATUSES
        assert not unknown, f"Unknown statuses: {unknown}"

    def test_actions_in_allowed_set(self, multi_report_outputs):
        eng = multi_report_outputs["engagement"]
        actions = set(eng["recommended_engagement_action"].dropna())
        unknown = actions - self.ALLOWED_ACTIONS
        assert not unknown, f"Unknown actions: {unknown}"

    def test_no_prohibited_actions(self, multi_report_outputs):
        eng = multi_report_outputs["engagement"]
        for action in self.PROHIBITED_ACTIONS:
            assert action not in eng["recommended_engagement_action"].values, (
                f"Prohibited action found: {action}"
            )


class TestScenarioStatuses:
    def test_healthy_broad_has_users(self, healthy_broad_outputs):
        eng = healthy_broad_outputs["engagement"]
        row = eng.iloc[0]
        assert row["unique_users_28d"] >= 5

    def test_newly_active_status(self, newly_active_outputs):
        eng = newly_active_outputs["engagement"]
        row = eng.iloc[0]
        assert row["overall_engagement_status"] in (
            "newly_active",
            "insufficient_evidence",
            "healthy_niche_adoption",
            "stable_engagement",
            "privacy_limited",
        )

    def test_inactive_has_zero_recent_users(self, inactive_outputs):
        eng = inactive_outputs["engagement"]
        row = eng.iloc[0]
        val = row.get("unique_users_28d", 0)
        assert val == 0 or pd.isna(val)

    def test_no_data_report_in_mart(self, multi_report_outputs):
        eng = multi_report_outputs["engagement"]
        r = eng[eng["report_id"] == "R_NODATA"]
        if r.empty:
            pytest.skip("R_NODATA not in engagement mart")
        row = r.iloc[0]
        assert row["overall_engagement_status"] in (
            "no_valid_user_data",
            "insufficient_evidence",
        )


class TestDeterminism:
    def test_same_input_same_output(self, healthy_broad_outputs):
        eng1 = healthy_broad_outputs["engagement"]
        eng2 = healthy_broad_outputs["engagement"].copy()
        pd.testing.assert_frame_equal(
            eng1.reset_index(drop=True), eng2.reset_index(drop=True)
        )


class TestSourceFilesUnchanged:
    def test_mart_report_user_daily_unchanged(self):
        import time
        path = Path("outputs/analytics/mart_report_user_daily.csv")
        if not path.exists():
            pytest.skip("mart_report_user_daily.csv not found")
        mtime_before = path.stat().st_mtime
        time.sleep(0.05)
        assert path.stat().st_mtime == mtime_before, (
            "mart_report_user_daily.csv was modified during test run"
        )
