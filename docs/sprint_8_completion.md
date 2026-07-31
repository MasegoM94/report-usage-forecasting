# Sprint 8 Completion Summary

**Sprint:** 8 — GenAI Insight Layer  
**Completion date:** 2026-07-30  
**Classification:** Complete — all ten steps delivered

---

## Scope Completed

### Step 1 — Schema and Contract Definition
Defined and implemented the full schema for report-level context inputs (34-field allowlist from 305-column mart), report insight outputs (7 narrative + lineage fields), portfolio context inputs (aggregates with sub-dicts), and portfolio insight outputs (8 narrative fields). Schema documented in `docs/genai_insight_schemas.md`.

### Step 2 — Privacy-Safe Context Construction
`build_mart_context()` applies the `INSIGHT_CONTEXT_ALLOWLIST` to every row of the mart. Privacy-suppressed engagement values are passed as `null` with explicit suppression annotations. No user-level data, no individual user identifiers, and no PII enter the LLM context.

### Step 3 — Versioned Prompt Design
Two versioned prompts (`report_insight_v1`, `portfolio_insight_v1`) in `src/genai/prompts.py`. Prompts enforce structured JSON output, numerical grounding constraints, direction consistency, prohibited action phrases, and evidence disclosure requirements. Version strings participate in the input hash so prompt changes automatically invalidate cached outputs.

### Step 4 — Report-Level Generation Pipeline
`generate_report_insights()` and `save_insights()` in `src/genai/insight_generator.py`. Pipeline: load mart → build contexts → hash-check each context → API call or reuse → validate → fallback if invalid → save. All generation paths produce the same schema.

### Step 5 — Portfolio-Level Generation Pipeline
`build_portfolio_context()` and `run_portfolio_pipeline()` in `src/genai/portfolio_insights.py`. Deterministic aggregates (usage distribution, forecast outlook, model health, decision support, attention shortlist) computed from the mart before any LLM call. Shortlist capped at 5, ranked deterministically. Portfolio hash excludes `attention_shortlist` to prevent spurious hash mismatches.

### Step 6 — Validation and Grounding
`src/genai/evaluation.py`: six hard validation dimensions (completeness, safety, directional consistency, numerical grounding, action alignment, evidence disclosure) plus two soft dimensions (readability, conciseness). Numerical grounding scans only the 7 narrative fields via `_collect_narrative_text()` — lineage fields (dates, UUIDs, timestamps) are excluded to prevent false failures.

### Step 7 — Rule-Based Fallback
`generate_rule_based_insight()`: deterministic, schema-valid fallback that does not require an API key. Used when no key is configured, when the API call fails, or when the LLM response fails validation. Fallback outputs always pass all validation checks.

### Step 8 — Hash Reuse and Cost Control
`_compute_context_hash()` (report-level) and `_compute_portfolio_hash()` (portfolio-level): SHA-256 over sorted context fields + prompt version + model name. Prior outputs with `generation_status in {success, reused}` are served unchanged. Rule-based outputs are regenerated each run (zero API cost, so no reuse needed).

### Step 9 — Evaluation Framework and Regression Tests
23 labelled fixture cases (`tests/fixtures/genai_evaluation_cases.json`), 10 golden configs (`tests/fixtures/genai_golden_outputs.json`), 79 deterministic tests in `tests/test_genai_evaluation.py` (all passing), human-review rubric in `docs/genai_evaluation_rubric.md`, and `run_regression_against_stored_outputs()` for end-to-end regression against persisted outputs. No LLM-as-judge — all checks are regex/value matching.

### Step 10 — Documentation and Notebook Cleanup
- `notebooks/08_genai_insights.ipynb`: full rewrite — 12 sections covering architecture, canonical input, report context (allowlist + privacy), report pipeline, portfolio context (aggregates + shortlist), portfolio output, 6 validation examples (no live API calls), evaluation framework, hash-reuse demonstration, limitations, and output file verification. No hardcoded paths. No API key exposure.
- `docs/genai_insight_schemas.md`: complete schema reference for all 5 types.
- `docs/genai_operations.md`: running commands, generation statuses, 11 failure modes, recovery, hash-reuse, cost controls, implemented vs. proposed monitoring metrics.
- `README.md`: stale legacy four-CSV text removed; canonical outputs section updated to include evaluation outputs; GenAI evaluation framework section added (Step 9).

---

## Security Constraints — Confirmed

All constraints were respected throughout the sprint:

| Constraint | Status |
|-----------|--------|
| No API key exposed in code or notebooks | Confirmed |
| No user-level data in GenAI context | Confirmed — only report-level aggregates |
| No individual user identifiers in LLM prompt | Confirmed — allowlist contains no user IDs |
| `automatic_retraining_triggered` always `False` | Confirmed |
| `retire_report` not a permitted LLM action | Confirmed — prohibited pattern |
| `delete_report` not a permitted LLM action | Confirmed — prohibited pattern |
| `restrict_user` not a permitted LLM action | Confirmed — prohibited pattern |
| `contact_specific_user` not a permitted LLM action | Confirmed — prohibited pattern |
| `automatic_intervention` not a permitted LLM action | Confirmed — prohibited pattern |
| `automatic_retraining` not a permitted LLM action | Confirmed — prohibited pattern |
| `change_selected_model` not a permitted LLM action | Confirmed — prohibited pattern |

---

## Confirmed Limitations

1. **All 30 synthetic mart reports have `model_diagnostic_status = insufficient_evidence`.** No production forecast backtests have been run; model-health commentary is limited to acknowledging the gap.

2. **All stored insights have `generation_status = rule_based`.** No API key is configured in the development environment. The LLM generation path has not been exercised on these outputs.

3. **Portfolio insights are not displayed in Streamlit.** `src/app/streamlit_app.py` reads and displays report-level insights only. Portfolio insights are persisted to JSON but not rendered. Deferred to Sprint 9.

4. **Streamlit reads legacy fields for display.** `render_ai_insights()` reads `forecast_summary`, `confidence`, `recommended_actions`, `hypotheses` — backward-compatibility fields populated from the current schema by `save_insights()`. No change required; these fields remain in the output schema for this reason.

5. **API latency, token usage, and cost are not instrumented.** These metrics are proposed but not yet implemented. See `docs/genai_operations.md` Section 8.

6. **Stakeholder usefulness experiment not conducted.** The human-review rubric is defined; no formal review session has been run.

---

## Deferred to Sprint 9

- Portfolio insight display in Streamlit
- API latency and token-usage instrumentation
- Stakeholder usefulness review (human-review rubric `docs/genai_evaluation_rubric.md`)
- Retry logic with instrumentation
- Grounding failure rate tracking per pipeline run

---

## File Inventory

### Source
- `src/genai/insight_generator.py` — report-level pipeline, allowlist, hash, validation
- `src/genai/portfolio_insights.py` — portfolio pipeline, aggregates, shortlist, hash
- `src/genai/prompts.py` — versioned prompts for report and portfolio
- `src/genai/evaluation.py` — all validation dimensions, regression runner

### Tests
- `tests/test_insight_generator.py`
- `tests/test_portfolio_insights.py`
- `tests/test_genai_evaluation.py` — 79 tests, all passing

### Fixtures
- `tests/fixtures/genai_evaluation_cases.json` — 23 labelled cases
- `tests/fixtures/genai_golden_outputs.json` — 10 golden configs

### Notebooks
- `notebooks/08_genai_insights.ipynb` — full 12-section demonstration notebook

### Documentation
- `docs/genai_insight_schemas.md`
- `docs/genai_operations.md`
- `docs/genai_evaluation_rubric.md`
- `docs/sprint_8_completion.md` (this file)

### Canonical Outputs
- `outputs/insights/report_ai_insights.json` — one insight per report
- `outputs/insights/portfolio_ai_insight.json` — one portfolio insight
- `outputs/evaluation/genai_evaluation_results.csv` — per-insight evaluation
- `outputs/evaluation/genai_evaluation_summary.json` — aggregate pass rates
