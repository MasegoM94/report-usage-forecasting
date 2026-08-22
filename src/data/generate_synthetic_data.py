"""Generate realistic synthetic Power BI-style usage telemetry tables.

==============================================================================
SYNTHETIC DEMONSTRATION DATA
==============================================================================
This dataset is synthetic and intended to stand in for confidential Power BI
telemetry from a corporate Finance / Risk / Operations BI estate.

It is deliberately more realistic than a clean academic dataset: reports have
unequal history lengths, some are retired mid-period, some have irregular or
sparse usage, and others exhibit calendar-driven spikes.

REPORT ARCHETYPES (3 reports each except launch_and_growth, 32 total)
----------------------------------------------------------------------
 1. stable_weekly           — Consistent daily usage with weekday/weekend
                              seasonality; the "business as usual" baseline.
 2. upward_adoption         — Growing user base over the period; adoption
                              curve as a new report proves its value.
 3. gradual_decline         — Slow erosion of usage, e.g. as a better
                              replacement emerges or the process is automated.
 4. sudden_retirement       — Active until a hard cut-off date, then silent;
                              models a decommissioned or migrated report.
 5. intermittent_specialist — Sparse and irregular; used by a small specialist
                              team only when needed (audits, model reviews).
 6. month_end               — Quiet most of the month; heavy spikes in the
                              final 3 business days of each calendar month.
 7. quarter_end             — Quiet between quarters; intense spikes at
                              quarter-end (March, June, September, December).
 8. launch_and_growth       — Zero history until a mid-period launch date,
                              then growing adoption from a standing start.
                              Includes R_031 (78 days) and R_032 (50 days)
                              which fall below the MIN_DAYS=90 eligibility
                              gate and are intentionally excluded from
                              forecasting — representing newly launched
                              reports with insufficient training history.
 9. replacement_cannibalized— Usage declining after a replacement report
                              launches and absorbs the user base.
10. high_volatility_exec    — Base readership plus unpredictable spikes tied
                              to exec meetings, board packs, or ad-hoc requests.

DESIGN NOTES
------------
- History lengths vary: not all reports cover the full 455-day window.
- Missing-telemetry gaps are injected as absent rows (not explicit zeros).
- The generator is vectorised; no O(n_reports × n_users × n_dates) loops.
- Set RANDOM_SEED for reproducibility; pass params dicts to tune behaviour.
==============================================================================
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


# ── Top-level parameters ─────────────────────────────────────────────────────

RANDOM_SEED = 42
N_USERS = 200
START_DATE = "2025-01-01"
END_DATE = "2026-03-31"

# ── Report catalogue ──────────────────────────────────────────────────────────
# Each entry: (report_id, report_name, archetype, workspace_id, report_type,
#              page_names, archetype_params)
#
# archetype_params keys (all optional, archetype-specific):
#   base_views     — expected daily views on a typical weekday
#   growth_factor  — end/start ratio for upward_adoption
#   decline_factor — end/start ratio for gradual_decline  (< 1)
#   retire_date    — "YYYY-MM-DD" for sudden_retirement & replacement_cannibalized
#   launch_date    — "YYYY-MM-DD" for launch_and_growth & replacement reports
#   active_prob    — P(any views on a given weekday) for intermittent_specialist
#   peak_views     — spike magnitude for month_end / quarter_end
#   spike_prob     — P(spike day) for high_volatility_exec
#   spike_scale    — multiplier on base_views for exec spikes
#   replaces       — report_id this report replaces (for replacement_cannibalized)

REPORT_CATALOG: list[dict[str, Any]] = [
    # ── 1. stable_weekly ─────────────────────────────────────────────────────
    {
        "report_id": "R_001",
        "report_name": "Commercial Finance Exposure Dashboard",
        "archetype": "stable_weekly",
        "workspace_id": "WS_Finance",
        "report_type": "Dashboard",
        "page_names": [
            "Executive Overview",
            "Credit Exposure by Segment",
            "FX & Rates Summary",
            "Top 20 Counterparties",
            "Limit Utilisation",
        ],
        "params": {"base_views": 45, "noise_std": 0.18},
    },
    {
        "report_id": "R_002",
        "report_name": "FX & Rates Risk Monitor",
        "archetype": "stable_weekly",
        "workspace_id": "WS_Risk",
        "report_type": "Report",
        "page_names": [
            "Portfolio Overview",
            "Delta & Gamma by Currency",
            "VaR Attribution",
            "Stress Test Results",
            "Limit Breaches",
            "Historical Comparison",
        ],
        "params": {"base_views": 28, "noise_std": 0.22},
    },
    {
        "report_id": "R_003",
        "report_name": "Headcount & Payroll Summary",
        "archetype": "stable_weekly",
        "workspace_id": "WS_Finance",
        "report_type": "Report",
        "page_names": [
            "Headcount by Division",
            "Grade & Band Mix",
            "Joiners & Leavers",
            "Payroll Cost Trend",
            "Vacancy Tracker",
        ],
        "params": {"base_views": 35, "noise_std": 0.20},
    },
    # ── 2. upward_adoption ────────────────────────────────────────────────────
    {
        "report_id": "R_004",
        "report_name": "New Business Volume Tracker",
        "archetype": "upward_adoption",
        "workspace_id": "WS_Commercial",
        "report_type": "Report",
        "page_names": [
            "Pipeline Summary",
            "Won & Lost Deals",
            "Revenue by Product",
            "Sales Team Performance",
            "Conversion Funnel",
            "Forecast vs Actual",
        ],
        "params": {"base_views": 12, "growth_factor": 3.8, "noise_std": 0.25},
    },
    {
        "report_id": "R_005",
        "report_name": "Digital Channel Adoption Report",
        "archetype": "upward_adoption",
        "workspace_id": "WS_Commercial",
        "report_type": "Report",
        "page_names": [
            "Channel Mix Overview",
            "Mobile vs Web Trends",
            "Customer Journey Funnel",
            "Drop-off Analysis",
            "Regional Breakdown",
        ],
        "params": {"base_views": 8, "growth_factor": 4.5, "noise_std": 0.28},
    },
    {
        "report_id": "R_006",
        "report_name": "ESG & Sustainability Metrics Dashboard",
        "archetype": "upward_adoption",
        "workspace_id": "WS_Risk",
        "report_type": "Dashboard",
        "page_names": [
            "Carbon Footprint",
            "Energy & Water Consumption",
            "Social & Governance KPIs",
            "Supply Chain Risk",
            "UN SDG Alignment",
        ],
        "params": {"base_views": 6, "growth_factor": 5.2, "noise_std": 0.30},
    },
    # ── 3. gradual_decline ────────────────────────────────────────────────────
    {
        "report_id": "R_007",
        "report_name": "Legacy Cost Allocation Report",
        "archetype": "gradual_decline",
        "workspace_id": "WS_Finance",
        "report_type": "Report",
        "page_names": [
            "Cost Pool Summary",
            "Allocation Methodology",
            "Division Charges",
            "Variance to Budget",
        ],
        "params": {"base_views": 50, "decline_factor": 0.20, "noise_std": 0.22},
    },
    {
        "report_id": "R_008",
        "report_name": "Premises & Facilities Utilisation",
        "archetype": "gradual_decline",
        "workspace_id": "WS_Operations",
        "report_type": "Report",
        "page_names": [
            "Building Occupancy",
            "Desk & Meeting Room Usage",
            "Cost per Seat",
            "Energy Efficiency",
            "Space Release Plan",
        ],
        "params": {"base_views": 30, "decline_factor": 0.30, "noise_std": 0.25},
    },
    {
        "report_id": "R_009",
        "report_name": "Manual Reconciliation Tracker",
        "archetype": "gradual_decline",
        "workspace_id": "WS_Finance",
        "report_type": "Paginated",
        "page_names": [
            "Outstanding Items",
            "Aged Breaks by Owner",
            "Resolution Trend",
        ],
        "params": {"base_views": 22, "decline_factor": 0.15, "noise_std": 0.20},
    },
    # ── 4. sudden_retirement ─────────────────────────────────────────────────
    {
        "report_id": "R_010",
        "report_name": "Regulatory Capital Pre-Migration View",
        "archetype": "sudden_retirement",
        "workspace_id": "WS_Risk",
        "report_type": "Report",
        "page_names": [
            "CET1 & Tier 2 Walkforward",
            "RWA by Asset Class",
            "Capital Buffers",
            "Regulatory Ratios",
            "Migration Readiness",
        ],
        "params": {"base_views": 40, "retire_date": "2025-09-12", "noise_std": 0.18},
    },
    {
        "report_id": "R_011",
        "report_name": "Pandemic Impact Monitoring Dashboard",
        "archetype": "sudden_retirement",
        "workspace_id": "WS_Operations",
        "report_type": "Dashboard",
        "page_names": [
            "Workforce Remote Status",
            "Operational Resilience",
            "Cost Impact",
            "Recovery Milestones",
        ],
        "params": {"base_views": 18, "retire_date": "2025-06-30", "noise_std": 0.25},
    },
    {
        "report_id": "R_012",
        "report_name": "Legacy Vendor Scorecard",
        "archetype": "sudden_retirement",
        "workspace_id": "WS_Operations",
        "report_type": "Paginated",
        "page_names": [
            "Vendor KPIs",
            "SLA Compliance",
            "Invoice Reconciliation",
        ],
        "params": {"base_views": 15, "retire_date": "2025-11-28", "noise_std": 0.22},
    },
    # ── 5. intermittent_specialist ────────────────────────────────────────────
    {
        "report_id": "R_013",
        "report_name": "Model Validation Audit Log",
        "archetype": "intermittent_specialist",
        "workspace_id": "WS_Risk",
        "report_type": "Paginated",
        "page_names": [
            "Validation Summary",
            "Outstanding Findings",
            "Model Inventory",
            "Annual Review Schedule",
        ],
        "params": {"base_views": 8, "active_prob": 0.10, "n_specialist_users": 12},
    },
    {
        "report_id": "R_014",
        "report_name": "Stress Testing Scenario Browser",
        "archetype": "intermittent_specialist",
        "workspace_id": "WS_Risk",
        "report_type": "Report",
        "page_names": [
            "Scenario Library",
            "P&L Impact by Desk",
            "Capital Impact",
            "Sensitivity Tables",
            "Historical Stress Events",
        ],
        "params": {"base_views": 12, "active_prob": 0.08, "n_specialist_users": 18},
    },
    {
        "report_id": "R_015",
        "report_name": "Regulatory Submission Evidence Pack",
        "archetype": "intermittent_specialist",
        "workspace_id": "WS_Risk",
        "report_type": "Paginated",
        "page_names": [
            "Submission Index",
            "Data Quality Attestations",
            "Sign-off Tracker",
        ],
        "params": {"base_views": 6, "active_prob": 0.06, "n_specialist_users": 8},
    },
    # ── 6. month_end ─────────────────────────────────────────────────────────
    {
        "report_id": "R_016",
        "report_name": "Month-End P&L Flash Report",
        "archetype": "month_end",
        "workspace_id": "WS_Finance",
        "report_type": "Report",
        "page_names": [
            "P&L Summary",
            "Revenue by Business Line",
            "Cost vs Budget",
            "Variance Commentary",
            "FX Impact",
            "YTD Performance",
            "Prior Month Comparison",
        ],
        "params": {"base_views": 5, "peak_views": 120, "noise_std": 0.30},
    },
    {
        "report_id": "R_017",
        "report_name": "Financial Results Close Dashboard",
        "archetype": "month_end",
        "workspace_id": "WS_Finance",
        "report_type": "Dashboard",
        "page_names": [
            "Close Status",
            "Outstanding Journals",
            "Intercompany Reconciliation",
            "Sign-off Progress",
            "Key Adjustments",
        ],
        "params": {"base_views": 8, "peak_views": 90, "noise_std": 0.28},
    },
    {
        "report_id": "R_018",
        "report_name": "Monthly KPI Scorecard",
        "archetype": "month_end",
        "workspace_id": "WS_Commercial",
        "report_type": "Report",
        "page_names": [
            "Executive KPI Summary",
            "Customer Metrics",
            "Operational KPIs",
            "Financial KPIs",
            "People & Culture",
            "Risk Indicators",
        ],
        "params": {"base_views": 10, "peak_views": 80, "noise_std": 0.25},
    },
    # ── 7. quarter_end ───────────────────────────────────────────────────────
    {
        "report_id": "R_019",
        "report_name": "Quarterly Investor Relations Pack",
        "archetype": "quarter_end",
        "workspace_id": "WS_Finance",
        "report_type": "Paginated",
        "page_names": [
            "Earnings Summary",
            "Balance Sheet",
            "Cash Flow Statement",
            "Key Performance Indicators",
            "Guidance & Outlook",
        ],
        "params": {"base_views": 4, "peak_views": 200, "noise_std": 0.35},
    },
    {
        "report_id": "R_020",
        "report_name": "Q-End Regulatory Return Tracker",
        "archetype": "quarter_end",
        "workspace_id": "WS_Risk",
        "report_type": "Report",
        "page_names": [
            "Return Completion Status",
            "Data Exceptions",
            "Submission Deadlines",
            "COREP Summary",
            "FINREP Reconciliation",
            "Attestation Log",
        ],
        "params": {"base_views": 6, "peak_views": 150, "noise_std": 0.30},
    },
    {
        "report_id": "R_021",
        "report_name": "Board & ExCo Performance Summary",
        "archetype": "quarter_end",
        "workspace_id": "WS_Finance",
        "report_type": "Report",
        "page_names": [
            "Board Pack Cover",
            "Strategic Objectives Progress",
            "Financial Performance",
            "Risk Appetite Statement",
            "People & Talent",
            "Outlook & Risks",
        ],
        "params": {"base_views": 5, "peak_views": 180, "noise_std": 0.32},
    },
    # ── 8. launch_and_growth ─────────────────────────────────────────────────
    {
        "report_id": "R_022",
        "report_name": "AI & Automation Benefits Tracker",
        "archetype": "launch_and_growth",
        "workspace_id": "WS_Commercial",
        "report_type": "Dashboard",
        "page_names": [
            "Benefits Realisation",
            "Use Case Pipeline",
            "FTE Savings",
            "Quality Improvements",
        ],
        "params": {"base_views": 20, "growth_factor": 3.0, "launch_date": "2025-04-14", "noise_std": 0.28},
    },
    {
        "report_id": "R_023",
        "report_name": "Cloud Migration Progress Dashboard",
        "archetype": "launch_and_growth",
        "workspace_id": "WS_Operations",
        "report_type": "Report",
        "page_names": [
            "Migration Wave Summary",
            "Workload Status",
            "Cost vs On-Prem",
            "Incidents & Rollbacks",
            "Timeline & Milestones",
        ],
        "params": {"base_views": 15, "growth_factor": 2.5, "launch_date": "2025-06-02", "noise_std": 0.30},
    },
    {
        "report_id": "R_024",
        "report_name": "Operations Performance Hub",
        "archetype": "launch_and_growth",
        "workspace_id": "WS_Operations",
        "report_type": "Dashboard",
        "page_names": [
            "Operations at a Glance",
            "Throughput & Volumes",
            "SLA & Service Quality",
            "Capacity Outlook",
            "Incident Heatmap",
            "Efficiency Metrics",
            "Team Performance",
        ],
        "params": {
            "base_views": 30,
            "growth_factor": 4.0,
            "launch_date": "2025-07-07",
            "noise_std": 0.25,
        },
        # This report replaces R_025; its launch triggers R_025's decline.
    },
    # ── 8b. launch_and_growth (insufficient history — excluded from forecasting) ─
    # These two reports launched late in the dataset window and have fewer than
    # MIN_DAYS=90 days of history. They demonstrate the eligibility gate behaviour
    # that 100% coverage on the earlier cohort cannot show.
    {
        "report_id": "R_031",
        "report_name": "ESG Carbon & Nature Risk Dashboard",
        "archetype": "launch_and_growth",
        "workspace_id": "WS_Risk",
        "report_type": "Dashboard",
        "page_names": [
            "Carbon Footprint Trend",
            "Nature & Biodiversity Exposure",
            "Transition Risk by Sector",
            "Regulatory Alignment",
        ],
        "params": {
            "base_views": 18,
            "growth_factor": 2.8,
            "launch_date": "2026-01-13",
            "noise_std": 0.30,
        },
    },
    {
        "report_id": "R_032",
        "report_name": "AI Programme ROI Tracker",
        "archetype": "launch_and_growth",
        "workspace_id": "WS_Operations",
        "report_type": "Report",
        "page_names": [
            "Benefits Realisation Summary",
            "Use Case Delivery",
            "Cost Avoided",
            "Quality & Accuracy Gains",
            "Roadmap Progress",
        ],
        "params": {
            "base_views": 22,
            "growth_factor": 3.5,
            "launch_date": "2026-02-10",
            "noise_std": 0.25,
        },
    },
    # ── 9. replacement_cannibalized ──────────────────────────────────────────
    {
        "report_id": "R_025",
        "report_name": "Operations MI Classic (Legacy)",
        "archetype": "replacement_cannibalized",
        "workspace_id": "WS_Operations",
        "report_type": "Report",
        "page_names": [
            "Summary View",
            "Volume Trends",
            "SLA Performance",
            "Headcount by Team",
            "Issues Log",
            "Month-End Pack",
        ],
        "params": {
            "base_views": 55,
            "retire_date": "2025-07-07",   # R_024 launches on this date
            "decay_weeks": 12,             # weeks until near-zero after replacement launches
            "noise_std": 0.22,
            "replaced_by": "R_024",
        },
    },
    {
        "report_id": "R_026",
        "report_name": "Portfolio Analytics View (Legacy)",
        "archetype": "replacement_cannibalized",
        "workspace_id": "WS_Risk",
        "report_type": "Report",
        "page_names": [
            "Portfolio Summary",
            "Asset Class Breakdown",
            "Duration & Convexity",
            "Credit Quality",
            "Liquidity Metrics",
        ],
        "params": {
            "base_views": 42,
            "retire_date": "2025-05-19",   # replacement is outside this dataset
            "decay_weeks": 16,
            "noise_std": 0.20,
            "replaced_by": None,
        },
    },
    {
        "report_id": "R_027",
        "report_name": "Credit Risk Classic View",
        "archetype": "replacement_cannibalized",
        "workspace_id": "WS_Risk",
        "report_type": "Report",
        "page_names": [
            "Obligor Summary",
            "PD / LGD Profiles",
            "Expected Loss Trend",
            "Sector Concentration",
            "Watch List",
        ],
        "params": {
            "base_views": 35,
            "retire_date": "2025-10-01",
            "decay_weeks": 10,
            "noise_std": 0.25,
            "replaced_by": None,
        },
    },
    # ── 10. high_volatility_exec ─────────────────────────────────────────────
    {
        "report_id": "R_028",
        "report_name": "Group CEO Weekly Intelligence Briefing",
        "archetype": "high_volatility_exec",
        "workspace_id": "WS_Finance",
        "report_type": "Report",
        "page_names": [
            "Weekly Headlines",
            "Financial Pulse",
            "Risk Radar",
            "People & Culture Snapshot",
        ],
        "params": {"base_views": 15, "spike_prob": 0.12, "spike_scale": 6.0, "noise_std": 0.40},
    },
    {
        "report_id": "R_029",
        "report_name": "Board Risk Committee Dashboard",
        "archetype": "high_volatility_exec",
        "workspace_id": "WS_Risk",
        "report_type": "Dashboard",
        "page_names": [
            "Risk Appetite Dashboard",
            "Top & Emerging Risks",
            "Audit Actions",
            "Regulatory Horizon",
            "Capital & Liquidity",
        ],
        "params": {"base_views": 10, "spike_prob": 0.08, "spike_scale": 8.0, "noise_std": 0.45},
    },
    {
        "report_id": "R_030",
        "report_name": "Investment Committee Decision Support",
        "archetype": "high_volatility_exec",
        "workspace_id": "WS_Finance",
        "report_type": "Report",
        "page_names": [
            "Deal Pipeline",
            "Approved Investments",
            "Returns Attribution",
            "Market Commentary",
            "Committee Actions",
        ],
        "params": {"base_views": 8, "spike_prob": 0.10, "spike_scale": 7.0, "noise_std": 0.42},
    },
]


# ── Archetype series generators ───────────────────────────────────────────────

def _weekend_mask(dates: pd.DatetimeIndex) -> np.ndarray:
    return dates.weekday >= 5


def _days_to_month_end(dates: pd.DatetimeIndex) -> np.ndarray:
    month_ends = dates.to_period("M").to_timestamp("M")
    return (month_ends - dates).days


def _is_quarter_end_window(dates: pd.DatetimeIndex, window_days: int = 5) -> np.ndarray:
    days_to_qend = np.array([
        (pd.Timestamp(d.year, [3, 6, 9, 12][((d.month - 1) // 3)], 1)
         + pd.offsets.MonthEnd(0) - d).days
        for d in dates
    ])
    return days_to_qend <= window_days


def _apply_missing_telemetry_gaps(
    views: np.ndarray,
    rng: np.random.Generator,
    gap_prob: float = 0.015,
    max_gap_days: int = 5,
) -> np.ndarray:
    """Zero out short random runs to simulate missing telemetry (not clean zeros)."""
    views = views.copy()
    n = len(views)
    i = 0
    while i < n:
        if rng.random() < gap_prob:
            gap_len = rng.integers(1, max_gap_days + 1)
            views[i : i + gap_len] = 0
            i += gap_len + 1
        else:
            i += 1
    return views


def _generate_stable_weekly(
    dates: pd.DatetimeIndex, params: dict, rng: np.random.Generator
) -> np.ndarray:
    base = params.get("base_views", 30)
    noise_std = params.get("noise_std", 0.18)
    weekend_factor = 0.25
    is_weekend = _weekend_mask(dates)
    multiplier = np.where(is_weekend, weekend_factor, 1.0)
    views = base * multiplier * rng.lognormal(0, noise_std, len(dates))
    return _apply_missing_telemetry_gaps(views, rng)


def _generate_upward_adoption(
    dates: pd.DatetimeIndex, params: dict, rng: np.random.Generator
) -> np.ndarray:
    base = params.get("base_views", 10)
    growth_factor = params.get("growth_factor", 3.0)
    noise_std = params.get("noise_std", 0.25)
    weekend_factor = 0.20
    n = len(dates)
    is_weekend = _weekend_mask(dates)
    trend = 1.0 + (growth_factor - 1.0) * (np.arange(n) / max(n - 1, 1))
    # Logistic ramp: slow start, faster middle, plateau at top
    t = np.linspace(-3, 3, n)
    logistic = 1 / (1 + np.exp(-t))
    trend = 1.0 + (growth_factor - 1.0) * logistic
    multiplier = np.where(is_weekend, weekend_factor, 1.0)
    views = base * trend * multiplier * rng.lognormal(0, noise_std, n)
    return _apply_missing_telemetry_gaps(views, rng)


def _generate_gradual_decline(
    dates: pd.DatetimeIndex, params: dict, rng: np.random.Generator
) -> np.ndarray:
    base = params.get("base_views", 40)
    decline_factor = params.get("decline_factor", 0.25)
    noise_std = params.get("noise_std", 0.20)
    weekend_factor = 0.22
    n = len(dates)
    is_weekend = _weekend_mask(dates)
    trend = 1.0 - (1.0 - decline_factor) * (np.arange(n) / max(n - 1, 1))
    multiplier = np.where(is_weekend, weekend_factor, 1.0)
    views = base * trend * multiplier * rng.lognormal(0, noise_std, n)
    return _apply_missing_telemetry_gaps(views, rng)


def _generate_sudden_retirement(
    dates: pd.DatetimeIndex, params: dict, rng: np.random.Generator
) -> np.ndarray:
    base = params.get("base_views", 35)
    noise_std = params.get("noise_std", 0.18)
    retire_date = pd.Timestamp(params["retire_date"])
    weekend_factor = 0.25
    is_weekend = _weekend_mask(dates)
    multiplier = np.where(is_weekend, weekend_factor, 1.0)
    views = base * multiplier * rng.lognormal(0, noise_std, len(dates))
    retire_idx = (dates >= retire_date).argmax()
    if dates[retire_idx] >= retire_date:
        views[retire_idx:] = 0
    return views


def _generate_intermittent_specialist(
    dates: pd.DatetimeIndex, params: dict, rng: np.random.Generator
) -> np.ndarray:
    base = params.get("base_views", 8)
    active_prob = params.get("active_prob", 0.10)
    n = len(dates)
    is_weekend = _weekend_mask(dates)
    views = np.zeros(n)
    # Burst-cluster approach: activity comes in 1-4 day bursts
    i = 0
    while i < n:
        if not is_weekend[i] and rng.random() < active_prob:
            burst_len = rng.integers(1, 4)
            for j in range(burst_len):
                if i + j < n and not is_weekend[i + j]:
                    views[i + j] = rng.integers(1, base * 3 + 1)
            i += burst_len + rng.integers(2, 8)  # gap between bursts
        else:
            i += 1
    return views


def _generate_month_end(
    dates: pd.DatetimeIndex, params: dict, rng: np.random.Generator
) -> np.ndarray:
    base = params.get("base_views", 5)
    peak = params.get("peak_views", 100)
    noise_std = params.get("noise_std", 0.30)
    n = len(dates)
    is_weekend = _weekend_mask(dates)
    days_to_end = _days_to_month_end(dates)
    # Low background noise during the month
    views = np.where(is_weekend, 0, base * rng.lognormal(0, noise_std, n).clip(0, 3))
    # Month-end spikes (last 3 non-weekend days)
    spike_mask = (days_to_end <= 2) & ~is_weekend
    spike_decay = np.where(days_to_end == 0, 1.0, np.where(days_to_end == 1, 0.7, 0.4))
    views[spike_mask] += peak * spike_decay[spike_mask] * rng.lognormal(0, 0.25, spike_mask.sum())
    return views


def _generate_quarter_end(
    dates: pd.DatetimeIndex, params: dict, rng: np.random.Generator
) -> np.ndarray:
    base = params.get("base_views", 4)
    peak = params.get("peak_views", 160)
    noise_std = params.get("noise_std", 0.32)
    n = len(dates)
    is_weekend = _weekend_mask(dates)
    # Very quiet background
    views = np.where(is_weekend, 0, base * 0.5 * rng.lognormal(0, noise_std, n).clip(0, 2))
    # Quarter-end window: last 5 calendar days of Mar, Jun, Sep, Dec
    qend_mask = _is_quarter_end_window(dates, window_days=4) & ~is_weekend
    # Also add month-end mini-spikes for non-quarter months
    days_to_end = _days_to_month_end(dates)
    month_end_mask = (days_to_end <= 1) & ~qend_mask & ~is_weekend
    # Apply quarter-end spikes
    qend_days = np.array([
        (pd.Timestamp(d.year, [3, 6, 9, 12][((d.month - 1) // 3)], 1)
         + pd.offsets.MonthEnd(0) - d).days
        for d in dates
    ])
    spike_decay = np.where(qend_days <= 4, np.exp(-qend_days * 0.4), 0)
    views[qend_mask] += peak * spike_decay[qend_mask] * rng.lognormal(0, 0.3, qend_mask.sum())
    # Small bumps at non-quarter month ends
    views[month_end_mask] += base * 2 * rng.lognormal(0, 0.3, month_end_mask.sum())
    return views


def _generate_launch_and_growth(
    dates: pd.DatetimeIndex, params: dict, rng: np.random.Generator
) -> np.ndarray:
    launch_date = pd.Timestamp(params["launch_date"])
    base = params.get("base_views", 15)
    growth_factor = params.get("growth_factor", 3.0)
    noise_std = params.get("noise_std", 0.28)
    weekend_factor = 0.20
    n = len(dates)
    is_weekend = _weekend_mask(dates)
    launch_idx = max(0, (dates >= launch_date).argmax())
    if dates[launch_idx] < launch_date:
        return np.zeros(n)
    n_active = n - launch_idx
    t = np.linspace(-2, 3, n_active)
    logistic = 1 / (1 + np.exp(-t))
    trend = 1.0 + (growth_factor - 1.0) * logistic
    multiplier = np.where(is_weekend[launch_idx:], weekend_factor, 1.0)
    active_views = base * trend * multiplier * rng.lognormal(0, noise_std, n_active)
    views = np.zeros(n)
    views[launch_idx:] = active_views
    # Inject a short early-adopter spike right at launch (training/demo event)
    spike_window = min(3, n_active)
    views[launch_idx : launch_idx + spike_window] *= 1.8
    return _apply_missing_telemetry_gaps(views, rng)


def _generate_replacement_cannibalized(
    dates: pd.DatetimeIndex, params: dict, rng: np.random.Generator
) -> np.ndarray:
    base = params.get("base_views", 45)
    retire_date = pd.Timestamp(params["retire_date"])
    decay_weeks = params.get("decay_weeks", 12)
    noise_std = params.get("noise_std", 0.22)
    weekend_factor = 0.25
    n = len(dates)
    is_weekend = _weekend_mask(dates)
    # Before replacement: stable usage
    multiplier = np.where(is_weekend, weekend_factor, 1.0)
    views = base * multiplier * rng.lognormal(0, noise_std, n)
    # After replacement launch: exponential decay
    retire_idx = (dates >= retire_date).argmax()
    if dates[retire_idx] >= retire_date:
        n_decay = n - retire_idx
        decay_days = decay_weeks * 7
        decay = np.exp(-np.arange(n_decay) / max(decay_days, 1))
        residual_noise = rng.lognormal(0, noise_std + 0.1, n_decay)
        views[retire_idx:] *= decay * residual_noise
        # Zero out very low values (stragglers stop after decay)
        views[retire_idx:][views[retire_idx:] < 1.0] = 0
    return _apply_missing_telemetry_gaps(views, rng)


def _generate_high_volatility_exec(
    dates: pd.DatetimeIndex, params: dict, rng: np.random.Generator
) -> np.ndarray:
    base = params.get("base_views", 10)
    spike_prob = params.get("spike_prob", 0.10)
    spike_scale = params.get("spike_scale", 6.0)
    noise_std = params.get("noise_std", 0.42)
    weekend_factor = 0.15
    n = len(dates)
    is_weekend = _weekend_mask(dates)
    multiplier = np.where(is_weekend, weekend_factor, 1.0)
    views = base * multiplier * rng.lognormal(0, noise_std, n)
    # Inject exec meeting spikes (weekdays only, last 1-2 days of a spike burst)
    spike_days = ~is_weekend & (rng.random(n) < spike_prob)
    views[spike_days] *= spike_scale * rng.lognormal(0, 0.5, spike_days.sum())
    # Long zero runs on consecutive zero-activity stretches (summer lull etc.)
    # Inject 1-2 quiet periods of 5-10 days
    for _ in range(rng.integers(1, 3)):
        start = rng.integers(0, max(1, n - 10))
        length = rng.integers(5, 11)
        if not is_weekend[start]:  # only if it starts on a weekday
            views[start : start + length] = 0
    return views


_ARCHETYPE_GENERATORS = {
    "stable_weekly": _generate_stable_weekly,
    "upward_adoption": _generate_upward_adoption,
    "gradual_decline": _generate_gradual_decline,
    "sudden_retirement": _generate_sudden_retirement,
    "intermittent_specialist": _generate_intermittent_specialist,
    "month_end": _generate_month_end,
    "quarter_end": _generate_quarter_end,
    "launch_and_growth": _generate_launch_and_growth,
    "replacement_cannibalized": _generate_replacement_cannibalized,
    "high_volatility_exec": _generate_high_volatility_exec,
}


# ── User-event generation ─────────────────────────────────────────────────────

def _distribute_views_to_users(
    expected_views: float,
    user_keys: np.ndarray,
    user_ids: np.ndarray,
    user_weights: np.ndarray,
    n_specialist_users: int | None,
    rng: np.random.Generator,
) -> list[tuple[str, str, int]]:
    """
    Given an expected total view count for one (date, report) cell, sample
    user events. Returns list of (user_key, user_id, view_count) tuples.
    """
    total = max(1, int(round(expected_views)))
    # Number of distinct users active on this day
    n_pool = len(user_keys) if n_specialist_users is None else min(n_specialist_users, len(user_keys))
    avg_views_per_user = 1.4
    n_users = min(
        int(np.ceil(total / avg_views_per_user)),
        n_pool,
        max(1, int(rng.poisson(max(1, total / avg_views_per_user)))),
    )
    n_users = max(1, n_users)

    # Sample user pool (specialist reports draw from a restricted subset)
    if n_specialist_users is not None:
        pool_size = min(n_specialist_users, len(user_keys))
        pool_keys = user_keys[:pool_size]
        pool_ids = user_ids[:pool_size]
        pool_weights = user_weights[:pool_size] / user_weights[:pool_size].sum()
    else:
        pool_keys = user_keys
        pool_ids = user_ids
        pool_weights = user_weights / user_weights.sum()

    n_users = min(n_users, len(pool_keys))
    chosen_idx = rng.choice(len(pool_keys), size=n_users, replace=False, p=pool_weights)

    # Distribute total views among selected users (at least 1 each)
    remaining = total
    events = []
    for rank, idx in enumerate(chosen_idx):
        if rank == len(chosen_idx) - 1:
            vc = remaining
        else:
            vc = max(1, int(rng.integers(1, max(2, remaining - (len(chosen_idx) - rank - 1) + 1))))
            vc = min(vc, remaining - (len(chosen_idx) - rank - 1))
        remaining -= vc
        if vc > 0:
            events.append((pool_keys[idx], pool_ids[idx], vc))

    return events


# ── Table builders ────────────────────────────────────────────────────────────

def get_project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def get_paths() -> dict[str, Path]:
    project_root = get_project_root()
    raw_path = project_root / "data" / "raw"
    raw_path.mkdir(parents=True, exist_ok=True)
    return {"project_root": project_root, "raw_path": raw_path}


def generate_users(n_users: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "user_key": [f"UK_{i:04d}" for i in range(1, n_users + 1)],
            "user_id": [f"user{i:03d}@masegoinc.com" for i in range(1, n_users + 1)],
            "unique_user": [f"User {i:03d}" for i in range(1, n_users + 1)],
        }
    )


def generate_reports(catalog: list[dict]) -> pd.DataFrame:
    rows = []
    for entry in catalog:
        rows.append(
            {
                "report_id": entry["report_id"],
                "report_name": entry["report_name"],
                "workspace_id": entry["workspace_id"],
                "report_type": entry["report_type"],
                "is_usage_metrics_report": False,
                "archetype": entry["archetype"],
            }
        )
    return pd.DataFrame(rows)


def generate_report_archetypes(
    catalog: list[dict], all_dates: pd.DatetimeIndex
) -> pd.DataFrame:
    """Supplementary metadata table for archetype research and model evaluation."""
    global_start = all_dates[0]
    global_end = all_dates[-1]
    rows = []
    for entry in catalog:
        params = entry.get("params", {})
        archetype = entry["archetype"]
        launch_date = (
            params.get("launch_date", str(global_start.date()))
            if archetype == "launch_and_growth"
            else str(global_start.date())
        )
        retire_date = params.get("retire_date", None)
        replaced_by = params.get("replaced_by", None)
        rows.append(
            {
                "report_id": entry["report_id"],
                "report_name": entry["report_name"],
                "archetype": archetype,
                "launch_date": launch_date,
                "retire_date": retire_date,
                "replaces_report_id": replaced_by,
                "history_start": launch_date,
                "history_end": retire_date if retire_date else str(global_end.date()),
            }
        )
    return pd.DataFrame(rows)


def generate_report_pages(reports: pd.DataFrame, catalog: list[dict]) -> pd.DataFrame:
    page_rows = []
    catalog_map = {e["report_id"]: e for e in catalog}
    for _, row in reports.iterrows():
        if row["report_type"] not in ("Report", "Dashboard"):
            continue
        entry = catalog_map.get(row["report_id"], {})
        page_names = entry.get("page_names", ["Overview", "Details", "Trends"])
        for page_num, page_name in enumerate(page_names, start=1):
            page_rows.append(
                {
                    "report_id": row["report_id"],
                    "section_id": f"{row['report_id']}_P{page_num}",
                    "section_name": page_name,
                }
            )
    return pd.DataFrame(page_rows)


def generate_dates(start_date: str, end_date: str) -> pd.DataFrame:
    dates_range = pd.date_range(start_date, end_date, freq="D")
    dates = pd.DataFrame({"date": dates_range})
    dates["day_of_week"] = dates["date"].dt.day_name()
    dates["week_start_date"] = dates["date"] - pd.to_timedelta(
        dates["date"].dt.weekday, unit="D"
    )
    dates["month"] = dates["date"].dt.to_period("M").astype(str)
    dates["is_weekend"] = dates["date"].dt.weekday >= 5
    return dates


def generate_all_views(
    catalog: list[dict],
    users: pd.DataFrame,
    all_dates: pd.DatetimeIndex,
    rng: np.random.Generator,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Generate report_views, report_page_views, and report_load_times tables
    for all reports using archetype-specific vectorised generators.
    """
    user_keys = users["user_key"].values
    user_ids = users["user_id"].values
    # Skewed user activity weights (power-law: some users view a lot more)
    raw_weights = rng.power(0.35, len(user_keys))
    user_weights = raw_weights / raw_weights.sum()

    # Per-report base load times
    report_load_map = {
        entry["report_id"]: rng.normal(3500, 1200)
        for entry in catalog
    }

    view_rows: list[dict] = []
    page_view_rows: list[dict] = []
    load_rows: list[dict] = []

    consumption_methods = ["Web", "Mobile"]
    distribution_methods = ["Direct", "App", "SharedLink", "Embedded"]
    dist_probs = [0.50, 0.30, 0.10, 0.10]
    cons_probs = [0.85, 0.15]
    browsers = ["Chrome", "Edge", "Safari", "Firefox"]
    browser_probs = [0.45, 0.30, 0.15, 0.10]
    countries = ["Canada", "UK", "South Africa"]
    country_probs = [0.70, 0.20, 0.10]
    clients_browser = ["Browser", "MobileApp"]
    session_sources = ["Direct", "App", "SharedLink"]
    session_probs = [0.60, 0.30, 0.10]

    for entry in catalog:
        report_id = entry["report_id"]
        archetype = entry["archetype"]
        params = entry.get("params", {})
        n_specialist = params.get("n_specialist_users", None)

        gen_fn = _ARCHETYPE_GENERATORS[archetype]
        expected_views = gen_fn(all_dates, params, rng)
        # Ensure non-negative integers
        expected_views = np.clip(expected_views, 0, None)

        base_load_ms = max(800, min(9000, report_load_map[report_id]))

        for day_idx, (date, ev) in enumerate(zip(all_dates, expected_views)):
            if ev < 0.5:
                continue  # no views this day

            # User event distribution for this (date, report) cell
            events = _distribute_views_to_users(
                ev, user_keys, user_ids, user_weights, n_specialist, rng
            )

            for u_key, u_id, vc in events:
                cons = rng.choice(consumption_methods, p=cons_probs)
                dist = rng.choice(distribution_methods, p=dist_probs)
                browser = rng.choice(browsers, p=browser_probs)

                view_rows.append(
                    {
                        "date": date,
                        "report_id": report_id,
                        "user_key": u_key,
                        "user_id": u_id,
                        "consumption_method": cons,
                        "distribution_method": dist,
                        "user_agent": browser,
                        "view_count": vc,
                    }
                )

                # Load time event
                load_ms = max(500, rng.normal(base_load_ms, 500))
                ts = pd.Timestamp(date) + pd.Timedelta(minutes=int(rng.integers(0, 1440)))
                load_rows.append(
                    {
                        "timestamp": ts,
                        "date": date,
                        "report_id": report_id,
                        "user_key": u_key,
                        "user_id": u_id,
                        "browser": browser,
                        "client": "Browser" if cons == "Web" else "MobileApp",
                        "country": rng.choice(countries, p=country_probs),
                        "load_time_ms": round(load_ms, 0),
                    }
                )

    view_df = pd.DataFrame(view_rows)
    load_df = pd.DataFrame(load_rows)
    return view_df, load_df


def generate_report_page_views(
    report_views: pd.DataFrame,
    report_pages: pd.DataFrame,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Derive page-level events from report-level view events."""
    if report_views.empty or report_pages.empty:
        return pd.DataFrame(
            columns=["timestamp", "date", "report_id", "section_id", "user_key",
                     "client", "session_source", "page_view_count"]
        )

    pages_by_report = (
        report_pages.groupby("report_id")["section_id"].apply(list).to_dict()
    )
    session_sources = ["Direct", "App", "SharedLink"]
    session_probs = [0.60, 0.30, 0.10]

    rows = []
    for row in report_views.itertuples(index=False):
        possible_pages = pages_by_report.get(row.report_id)
        if not possible_pages:
            continue
        n_pages = rng.integers(1, min(len(possible_pages), row.view_count + 2) + 1)
        viewed = rng.choice(possible_pages, size=n_pages, replace=False)
        for section_id in viewed:
            ts = pd.Timestamp(row.date) + pd.Timedelta(minutes=int(rng.integers(0, 1440)))
            rows.append(
                {
                    "timestamp": ts,
                    "date": row.date,
                    "report_id": row.report_id,
                    "section_id": section_id,
                    "user_key": row.user_key,
                    "client": "Browser" if row.consumption_method == "Web" else "MobileApp",
                    "session_source": rng.choice(session_sources, p=session_probs),
                    "page_view_count": 1,
                }
            )
    return pd.DataFrame(rows)


def print_shapes(tables: dict[str, pd.DataFrame]) -> None:
    for name, df in tables.items():
        print(f"  {name}: {df.shape}")


def run_basic_validation(tables: dict[str, pd.DataFrame]) -> None:
    reports = tables["reports"]
    users = tables["users"]
    report_pages = tables["report_pages"]
    report_views = tables["report_views"]
    report_page_views = tables["report_page_views"]
    report_load_times = tables["report_load_times"]

    checks = {
        "All report_ids in report_views exist in reports":
            report_views["report_id"].isin(reports["report_id"]).all(),
        "All user_keys in report_views exist in users":
            report_views["user_key"].isin(users["user_key"]).all(),
        "All section_ids in report_page_views exist in report_pages":
            report_page_views["section_id"].isin(report_pages["section_id"]).all(),
        "All report_ids in report_load_times exist in reports":
            report_load_times["report_id"].isin(reports["report_id"]).all(),
    }
    for msg, result in checks.items():
        status = "PASS" if result else "FAIL"
        print(f"  [{status}] {msg}")


def save_raw_tables(tables: dict[str, pd.DataFrame], raw_path: Path) -> None:
    for name, df in tables.items():
        path = raw_path / f"{name}.csv"
        df.to_csv(path, index=False)
        print(f"  Saved {path}")


def main() -> None:
    rng = np.random.default_rng(RANDOM_SEED)
    paths = get_paths()
    raw_path = paths["raw_path"]

    all_dates = pd.date_range(START_DATE, END_DATE, freq="D")
    print(f"Date range  : {START_DATE} → {END_DATE} ({len(all_dates)} days)")
    print(f"Reports     : {len(REPORT_CATALOG)}")
    print(f"Users       : {N_USERS}")
    print()

    users = generate_users(N_USERS)
    reports = generate_reports(REPORT_CATALOG)
    report_archetypes = generate_report_archetypes(REPORT_CATALOG, all_dates)
    report_pages = generate_report_pages(reports, REPORT_CATALOG)
    dates = generate_dates(START_DATE, END_DATE)

    print("Generating usage events (vectorised by archetype)...")
    report_views, report_load_times = generate_all_views(
        REPORT_CATALOG, users, all_dates, rng
    )
    report_page_views = generate_report_page_views(report_views, report_pages, rng)

    tables = {
        "reports": reports,
        "users": users,
        "report_pages": report_pages,
        "dates": dates,
        "report_views": report_views,
        "report_page_views": report_page_views,
        "report_load_times": report_load_times,
        "report_archetypes": report_archetypes,
    }

    print("\nTable shapes:")
    print_shapes(tables)
    print("\nValidation checks:")
    run_basic_validation(tables)
    print("\nSaving to data/raw/ ...")
    save_raw_tables(tables, raw_path)
    print("\nDone.")


if __name__ == "__main__":
    main()
