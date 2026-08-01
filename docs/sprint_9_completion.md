# Sprint 9 Completion Document

**Project:** Power BI Report Usage Forecasting — Streamlit Reviewer App  
**Sprint:** 9 (Steps 1–6)  
**Date:** 2026-08-01  
**Status:** COMPLETE WITH DOCUMENTED LIMITATIONS

---

## Scope Completed

### Step 1 — Audit (Sprint 9 prerequisite)
- Full audit of Streamlit entry point, all section renderers, sidebar, charts, filters, definitions, label mappings, data loaders, caching, tests, and documentation before each step began.

### Step 2 & 3 — Portfolio Overview (completed before this session)
- `render_overview()` reads from the canonical analytics mart (`mart_report_analytics.csv`).
- Headline metrics: total reports, recent usage, requiring review, high priority, privacy suppressed.
- Portfolio GenAI summary with full 6-state handling (valid, reused, rule_based, fallback, invalid, missing).
- Attention shortlist: deterministic sort by priority then report ID, capped at 5.
- Status distributions (historical usage, forecast outlook, engagement, review priority, action, model health).
- Evidence-maturity note for `insufficient_evidence` model health.
- Full portfolio note when mart is missing (degrades gracefully, no crash).

### Step 3 — Portfolio Overview (canonical mart integration)
- `available_reports()` prefers the canonical mart; legacy sources fill gaps only.
- Duplicate display names disambiguated by appending `(report_id)`.

### Step 4 — Report Explorer
- Consolidated three former tabs into one unified **Report Explorer** tab.
- 7-section layout: summary header → AI summary → historical usage → forecast → model health → engagement → diagnostics and decision → lineage expander.
- Sprint-8 GenAI schema fields: `executive_summary`, `usage_insight`, `engagement_insight`, `forecast_insight`, `model_confidence_note`, `recommended_action`, `evidence_limitations`.
- Legacy alias fallback for `forecast_summary`, `recommended_actions`, `hypotheses`.
- All 6 GenAI states rendered with accurate labels.
- Forecast chart uses **prediction interval** terminology (not confidence interval).
- Model health `insufficient_evidence` explained as evidence-maturity, not poor performance.
- Engagement suppression shown as `Suppressed (privacy)`, never as zero.
- Concentration described as dependency risk, not misuse.
- Low engagement: explicit note that it does not imply low business value.
- Pure-logic helpers in `src/app/utils/report_helpers.py` (no Streamlit dependency).

### Step 5 — Filters, Navigation, Centralised Definitions
- `src/app/utils/definitions.py`: 19 concept definitions + comprehensive `STATUS_LABELS` mapping + `status_label()`.
- `src/app/utils/filter_helpers.py`: `apply_filters`, `apply_attention_filter`, `search_reports`, `extract_filter_options`, `check_filter_availability`, `active_filter_summary`, `safe_session_report`, `default_filter_state`.
- Sidebar: search → portfolio filters expander → attention toggle → display controls → report selector → active filter summary.
- "Requires attention" = `overall_review_priority ∈ {high, critical}` OR `recommended_report_action ≠ continue_monitoring`.
- Filter version counter (`_sf_v`) for atomic clear-all.
- Portfolio distributions and "Showing X of Y" banner reflect the active filter.
- Portfolio AI summary and attention shortlist always reflect the **full portfolio** with explicit notes.
- Filter availability guard: fields with < 2 distinct non-null values are hidden.

### Step 6 — Accessibility, Empty States, Performance, Testing

#### 6a. Chart accessibility
- `usage_forecast_chart()`: renamed `"Confidence interval"` → `"Prediction interval"` (the only correct term).
- Historical actuals: solid blue line + circle markers.
- Forecast: dashed orange line + diamond markers.
- Both series are now distinguishable **without relying on colour alone**.
- Added `report_title` parameter; chart title includes report name for context.
- Added `xaxis_title="Date"` and `yaxis_title="Views"`.
- Invalid dates coerced rather than raising.
- Empty-figure annotation has `font.size=13` and hidden axes for clarity.

#### 6b. Freshness and load-state indicators
- Analytics `run_id` moved out of the inline freshness banner (now only in the lineage expander).
- Freshness banner shows: analytics as-of date + "Showing X of Y" when filtered.
- Sidebar footer shows: canonical mart load status, report AI insight record count, portfolio AI summary status.

#### 6c. Terminology corrections applied
| Context | Was | Now |
|---------|-----|-----|
| Chart legend | Confidence interval | Prediction interval |
| Forecast caption | (implicit) | "Prediction interval — not a confidence interval" |
| `insufficient_evidence` status | (risk of "unhealthy" confusion) | Explicitly documented as evidence-maturity |
| Low engagement display | (risk of "low value" confusion) | "Low engagement does not imply low business value" note |
| Concentration display | (risk of "misuse" confusion) | "High concentration is a dependency risk — it does not indicate misuse" |
| Recommended action | (risk of "automated" confusion) | "This recommendation … has not been executed" |

#### 6d. Smoke tests — `tests/test_app_smoke.py` (44 tests)
- All utility modules import without error.
- `available_reports()` handles: mart with data, empty mart, duplicate names, no data.
- `build_report_detail()` handles: complete data, empty mart row, missing engagement, invalid GenAI, missing GenAI.
- All 6 GenAI states classified correctly (valid, reused, rule_based, fallback, invalid, missing).
- Empty-state scenarios: no historical usage, no forecast, no model health, privacy-suppressed engagement, zero reports after filter.
- Chart smoke: complete data, empty data, forecast-only, actuals-only, prediction interval label.
- Schema validation: valid mart, empty mart, missing required columns, duplicate IDs.

#### 6e. Load-data integration tests — `tests/test_load_data_integration.py` (33 tests)
- Complete valid fixture: mart loaded, engagement loaded, insights loaded.
- Mart-only: missing optional files do not crash, portfolio insight status is `absent`.
- Missing optional files: forecasts absent, engagement absent, all files absent.
- Malformed JSON: insights malformed → empty frame, portfolio insight malformed → `malformed_json`, list structure → `unexpected_structure`, validation failed → `validation_failed`.
- Duplicate report IDs: raises `ValueError`.
- Invalid date values: coerced, no exception raised.
- Production forecast date normalization.
- Privacy suppression: suppressed unique_users_28d is null, not zero.
- Insight loading: valid insight, empty insights list, portfolio insight absent/valid/ok.
- Stale lineage mismatch: different run IDs between mart and insights does not crash.

#### 6f. Privacy and evidence state tests — `tests/test_privacy_evidence.py` (40 tests)
- Suppression never formatted as zero.
- Suppression with a numeric value still returns "Suppressed (privacy)".
- Engagement suppression flag propagated to `build_report_detail`.
- `insufficient_evidence` label does not contain "unhealthy" or "poor".
- Definition confirms evidence-maturity interpretation.
- Missing GenAI classified as "missing" (not "declining").
- Concentration definition does not equate with misuse.
- Low engagement label does not say "low value".
- Recommended action labels do not contain "execute" or "automated".
- Invalid GenAI state classified as `"invalid"`.
- Invalid state label communicates failure.
- All 6 GenAI state labels defined.
- `build_report_detail` does not expose `user_id` or `email`.
- Definitions cross-checked for terminology compliance.

#### 6g. Filter+selection integration tests — `tests/test_filter_selection_integration.py` (38 tests)
- Search: by name, by ID, blank returns all, no match returns empty.
- Filters: single field, two-field AND, empty filter, zero results, unknown field.
- Attention filter: high-priority included, low-priority+continue_monitoring excluded.
- Filtered portfolio distributions: headline metrics reflect subset, distribution table totals match.
- Attention shortlist: always full-mart (caller responsibility confirmed).
- Selected report resolution: preserved when in list, falls back to first when filtered out, empty list returns None, no current ID returns first.
- Report-detail assembly: valid mart → complete detail, missing mart → minimal identity, insight for correct report loaded, insights not mixed between reports.
- Duplicate display names: disambiguated, both resolvable.
- Unknown status categories: filter does not crash, extract options includes unknown values, unknown priority excluded from attention filter, clear-filter resets to defaults, filter availability suppresses single-value fields.

---

## Remaining Limitations

### Data
- **Synthetic source data only.** All 30 reports and their usage data were generated programmatically. The analytics are structurally valid but not grounded in real Power BI telemetry.
- **Model-health evidence maturity.** The `model_diagnostic_status` is `insufficient_evidence` for all 30 synthetic reports because no production forecast history has accumulated. This is expected and is correctly labelled as evidence-maturity, not model failure.

### Streamlit app
- **No live LLM calls in Streamlit.** The GenAI pipeline runs offline. The app displays persisted outputs only. No LLM call is initiated from the Streamlit session.
- **No filter-specific GenAI summaries.** The portfolio AI summary always reflects the full persisted portfolio. This is clearly communicated when filters are active.
- **Attention shortlist reflects full portfolio.** The deterministic shortlist is not re-ranked by the current filter selection. This is by design and is clearly communicated.
- **Smoke tests do not use Streamlit AppTest.** The `streamlit.testing.v1.AppTest` class requires an active event loop and is environment-dependent. All smoke tests target the pure-logic layer instead (import, render helpers, chart builders, schema validation).
- **Performance benchmarks are not strict thresholds.** Data loading is cached via `@st.cache_data` (no-op in tests). Filter operations on 30-row DataFrames are sub-millisecond. No strict timing assertions were added to avoid flaky tests.

---

## Deferred Work

| Item | Reason deferred |
|------|----------------|
| AppTest-based Streamlit smoke tests | Requires active Streamlit event loop; environment-dependent; pure-logic coverage provides equivalent assurance |
| Filter-specific GenAI summaries | Would require live LLM calls or a separate offline generation step; explicitly out of scope |
| Business-action automation | Out of scope by design — the app is read-only and advisory |
| Real Power BI telemetry | Out of scope for this portfolio project |
| Multi-report comparison view | Not planned for Sprint 9 |

---

## Sprint 9 Test Summary

| Test file | New tests | Total after sprint |
|-----------|-----------|-------------------|
| `test_report_helpers.py` (Step 4) | 81 | 81 |
| `test_definitions.py` (Step 5) | 33 | 33 |
| `test_filter_helpers.py` (Step 5) | 61 | 61 |
| `test_app_smoke.py` (Step 6) | 44 | 44 |
| `test_load_data_integration.py` (Step 6) | 33 | 33 |
| `test_privacy_evidence.py` (Step 6) | 40 | 40 |
| `test_filter_selection_integration.py` (Step 6) | 38 | 38 |
| **Sprint 9 total** | **330** | **330** |

**Full suite result (post Step 6):** 3580 passed, 101 skipped, 0 failures.

---

## Confirmation

- No new analytical features were added.
- No live LLM calls occur in the Streamlit app or in any test.
- No filter-specific GenAI summaries were generated.
- No upstream pipeline logic was recalculated in Streamlit.
- All new code is pure-Python and testable outside a Streamlit runtime.

---

## Sprint 9 Classification

**COMPLETE WITH DOCUMENTED LIMITATIONS**

All planned scope for Steps 1–6 is implemented and verified. Remaining limitations are documented above. They reflect the synthetic-data context of this portfolio project and explicit out-of-scope decisions, not incomplete implementation.
