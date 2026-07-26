# Deprecated Fields — Sprint 7

This document records field names that were deprecated in Sprint 7 and their replacements.
No Sprint 7 output file depends on any deprecated field.

---

## Field-Level Replacements

| Deprecated field | Replacement(s) | Reason |
|---|---|---|
| `latest_views` | `recent_28d_views` | Ambiguous window; 28d window is explicit |
| `prior_views` | `previous_28d_views` | Ambiguous window; 28d window is explicit |
| `usage_change_pct` | `usage_change_28d_pct` | Ambiguous window; 28d window is explicit |
| `repeat_rate` | `returning_user_share_28d` | "Rate" was ambiguous; share is explicit and window-specific |
| `top_user_concentration` | `top_1_user_view_share_28d` + `user_view_hhi_28d` | Single metric conflated share and HHI; now separate and privacy-gated |

---

## Segment-Level Replacements

| Deprecated segment value | Replacement(s) | Reason |
|---|---|---|
| `niche` (primary_report_segment) | `healthy_niche_adoption` (engagement_segment) + `concentrated_dependency` (dependency_segment) | "Niche" conflated small user base with high concentration — different risks |

The `niche` segment appeared in Sprint 6 as a combined label for reports with few users
AND/OR high concentration. Sprint 7 separates these:
- `niche_healthy_engagement` (in engagement_segment): small but stable, loyal user base — potentially healthy
- `concentrated_dependency` (in dependency_segment): at-risk pattern where one or few users dominate — requires review

Keeping them separate allows the diagnostic layer to raise the correct risk flag
(dependency risk vs. engagement quality) without conflating them.

---

## Verification

The following columns are confirmed absent from all Sprint 7 output files:

- `latest_views` — absent from report_features.csv, mart_report_analytics.csv
- `prior_views` — absent from report_features.csv, mart_report_analytics.csv
- `usage_change_pct` — absent from report_features.csv, mart_report_analytics.csv
- `repeat_rate` — absent from report_engagement_context.csv, mart_report_analytics.csv
- `top_user_concentration` — absent from report_engagement_context.csv, mart_report_analytics.csv

Verified by TestNoDeprecatedFields in tests/test_report_analytics_pipeline_integration.py.
