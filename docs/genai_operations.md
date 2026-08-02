# GenAI Insight Layer — Operations and Failure Guide

**Report-Usage Forecasting Project**

This guide covers running the GenAI pipeline, understanding generation statuses, handling failures, and operating cost controls. All commands are run from the repository root.

---

## 1. Running the Pipeline

### Prerequisites

The GenAI pipeline reads from the canonical analytics mart. Before running, confirm the mart exists:

```bash
ls outputs/analytics/mart_report_analytics.csv
```

### Report-level insight generation

```bash
python -m src.genai.insight_generator
```

Reads `mart_report_analytics.csv`, generates one insight per report, writes:
- `outputs/insights/report_ai_insights.json`
- `outputs/insights/report_ai_insights.md` (human-readable, optional)

### Portfolio insight generation

```bash
python -c "from src.genai.portfolio_insights import run_portfolio_pipeline; from pathlib import Path; run_portfolio_pipeline(project_root=Path('.'))"
```

Reads `mart_report_analytics.csv`, builds deterministic portfolio aggregates, generates one portfolio insight, writes:
- `outputs/insights/portfolio_ai_insight.json`
- `outputs/insights/portfolio_ai_insight.md` (human-readable, optional)

### Full GenAI pipeline (report + portfolio)

```bash
python -c "from src.genai.insight_generator import run_pipeline; from pathlib import Path; run_pipeline(project_root=Path('.'))"
```

Runs report-level generation then portfolio generation. If report-level fails, portfolio still runs. If portfolio fails, report outputs are not affected.

### Deterministic evaluation against stored outputs

```python
from pathlib import Path
from src.genai.evaluation import run_regression_against_stored_outputs

summary = run_regression_against_stored_outputs(project_root=Path("."))
print(summary["cases_evaluated"], summary["overall_pass_rate"])
```

Writes timestamped results to `outputs/evaluation/` and updates the `*_latest.*` files.

### Relevant tests

```bash
# Report-level pipeline tests
python -m pytest tests/test_insight_generator.py -v

# Portfolio insight tests
python -m pytest tests/test_portfolio_insights.py -v

# Evaluation framework tests
python -m pytest tests/test_genai_evaluation.py -v

# All GenAI tests
python -m pytest tests/test_insight_generator.py tests/test_portfolio_insights.py tests/test_genai_evaluation.py -v
```

---

## 2. Environment

### API key

The pipeline uses OpenAI's API. Set the key before running:

```bash
export OPENAI_API_KEY="sk-..."
```

Or add to a `.env` file in the repository root (loaded automatically via `python-dotenv`).

**If `OPENAI_API_KEY` is not set:** the pipeline falls back to deterministic rule-based generation for all reports and the portfolio. All outputs are tagged `generation_status: rule_based`. The pipeline does not fail or raise an error.

**Do not commit API keys to the repository.**

### Model configuration

The default model is controlled by `DEFAULT_MODEL` in `src/genai/insight_generator.py`:

```
DEFAULT_MODEL = "gpt-4.1-mini"
```

Pass a different model via the `model` parameter of `run_pipeline()` or `generate_report_insights()`.

### Prompt versions

| Pipeline | Prompt version constant | Current value |
|----------|------------------------|---------------|
| Report-level | `REPORT_INSIGHT_PROMPT_VERSION` in `src/genai/prompts.py` | `report_insight_v1` |
| Portfolio | `PORTFOLIO_INSIGHT_PROMPT_VERSION` in `src/genai/prompts.py` | `portfolio_insight_v1` |

Changing either version string changes the input hash and triggers regeneration for all affected outputs on the next run.

### Output length

The structured JSON output is constrained to `max_tokens=1200` per report (report-level API call). The portfolio call uses a higher limit. These values are set in `_call_openai_api()` and `_call_api_for_portfolio()` respectively.

---

## 3. Generation Statuses

| Status | Meaning | API call made? |
|--------|---------|----------------|
| `success` | LLM API returned valid, grounded JSON | Yes |
| `reused` | Input hash matched a prior `success` or `reused` output; content unchanged | No |
| `rule_based` | No API key configured; deterministic fallback used | No |
| `fallback_schema_invalid` | LLM response failed schema or grounding validation; deterministic fallback used | Yes |
| `fallback_api_error` | API call failed (timeout, rate limit, network error); deterministic fallback used | Yes (failed) |

The `validation_status` field records the schema check result: `valid` or `invalid`. A `valid` status on a `rule_based` insight means the deterministic fallback passed all schema checks.

---

## 4. Failure Handling

### Missing API key

**Behaviour:** pipeline uses deterministic rule-based generation for every report and the portfolio. No error is raised.  
**Output:** all insights have `generation_status: rule_based`, `api_attempts: 0`.  
**Action required:** none for rule-based operation; set `OPENAI_API_KEY` for LLM generation.

### API timeout or rate limit

**Behaviour:** the call is wrapped in a try/except. On failure, the report falls back to a deterministic insight with `generation_status: fallback_api_error` and the error message in `generation_error`.  
**Action required:** rerun the pipeline — unchanged reports are reused, only the failed one will retry.

### Malformed JSON response from LLM

**Behaviour:** `_parse_json_response()` extracts JSON from markdown code fences and normalises common formatting issues. If JSON cannot be parsed, schema validation fails and the fallback is used.  
**Output:** `generation_status: fallback_schema_invalid`.

### Schema validation failure

**Behaviour:** if the LLM response is valid JSON but fails `_validate_insight_schema()` (missing field, prohibited phrase, numerical grounding failure, directional conflict), the fallback is used.  
**Output:** `generation_status: fallback_schema_invalid`, `validation_status: invalid`.

### Numerical grounding failure

A percentage or count claim in the narrative is not within ±5 pp / ±5 units of any value in the context. Only narrative text fields are checked (lineage fields such as dates are excluded).  
**Output:** report `validation_status: invalid`, `generation_status: fallback_schema_invalid`.

### Directional conflict

The insight uses growing language (growing, increasing, expanding) when the context reports a declining status, or vice versa.  
**Output:** `validation_status: invalid`, `generation_status: fallback_schema_invalid`.

### Prohibited action phrase

The insight contains a word or phrase in the prohibited list (retire, delete, deleting, deletion, retrain, retraining, replace the model, model replacement, restrict user, contact user).  
**Output:** `validation_status: invalid`, `generation_status: fallback_schema_invalid`.

### Privacy issue

Privacy-suppressed fields are passed as `null` in the context with explicit notes. If the LLM ignores the suppression annotation and claims values for those fields, the grounding check fails on the fabricated numbers.  
**Note:** the pipeline does not detect "invented suppressed values" as a separate check — the grounding check catches them because invented values would not match the null context.

### One report failure

If a single report's LLM call fails or produces invalid output, it falls back to a deterministic insight. Other reports are not affected. The failed record is tagged in `generation_status` and `generation_error`.

### Portfolio failure

Portfolio failure is caught by a separate try/except in `run_pipeline()`. If the portfolio step fails, a warning is printed and report-level outputs are preserved. No report output is removed.

---

## 5. Recovery

### When a retry occurs

The pipeline does not automatically retry failed API calls within a single run. Retrying requires rerunning the pipeline. On rerun, hash-reuse logic skips unchanged successful outputs; only failed or changed records are regenerated.

### When a fallback occurs

Fallback occurs immediately when: (a) the API call fails, or (b) the response fails schema or grounding validation. The fallback is a deterministic rule-based summary built from context fields without any LLM call.

### Forcing regeneration

There is no force-regeneration flag. To regenerate a specific report: change the prompt version (triggers all reports) or delete the entry from the stored JSON (the pipeline will treat it as missing and regenerate).

### Identifying failed records

```python
import json
data = json.loads(open("outputs/insights/report_ai_insights.json").read())
failed = [e for e in data if e.get("generation_status") not in ("success", "reused", "rule_based")]
for f in failed:
    print(f["report_id"], f["generation_status"], f["generation_error"])
```

For the portfolio:
```python
import json
p = json.loads(open("outputs/insights/portfolio_ai_insight.json").read())
print(p["generation_status"], p.get("generation_error"))
```

### How prompt or model changes trigger regeneration

The input hash is computed from: `SHA-256(sorted context JSON + prompt_version + model_name)`. Changing either the prompt version constant or the model name changes every hash, so all outputs are regenerated on the next run.

---

## 6. Hash-Reuse Behaviour

Before each API call, the pipeline:
1. Computes `input_hash = SHA-256(sorted context + prompt_version + model_name)`.
2. Loads existing stored outputs.
3. If the hash matches a stored entry with `generation_status in {success, reused}`, the stored output is returned unchanged with `generation_status: reused`.
4. If the hash does not match, or the prior status was `rule_based` / `fallback_*`, generation proceeds.

**Portfolio hash:** excludes `attention_shortlist` from the context before hashing (the shortlist is derived from the aggregates; excluding it avoids trivial hash mismatches from equivalent shortlist content).

---

## 7. Cost Controls

| Control | Implementation |
|---------|---------------|
| Concise context | Only 34 allowlisted fields are passed; all other mart columns are excluded |
| Structured output | JSON schema enforced in the prompt; LLM is not asked for free-form prose |
| Batch execution | All reports in one pipeline run; not called per page load |
| Input hashing | Unchanged contexts are not re-sent to the API |
| Skip-unchanged | `reused` status means zero API calls for that report |
| Model selection | `gpt-4.1-mini` is the default; a smaller or cheaper model can be specified via the `model` parameter |
| Token limit | `max_tokens=1200` per report call limits output length and cost |

---

## 8. Monitoring

The following distinguishes **implemented** monitoring from **proposed** monitoring.

### Implemented (derivable from stored outputs)

| Metric | How to derive |
|--------|---------------|
| Generation status distribution | Count `generation_status` values in `report_ai_insights.json` and `portfolio_ai_insight.json` |
| Validation status distribution | Count `validation_status` values |
| Fallback rate | Count `rule_based + fallback_*` statuses ÷ total reports |
| Reuse rate | Count `reused` statuses ÷ total reports |
| Missing insights | Reports in mart not present in `report_ai_insights.json` |
| Prompt version | Read `prompt_version` from stored outputs |
| Model name | Read `model_name` from stored outputs |
| API attempts | Read `api_attempts` from stored outputs |
| Overall evaluation pass rate | Read `overall_pass_rate` from `outputs/evaluation/genai_evaluation_summary.json` |
| Failures by dimension | Read `failure_count_by_dimension` from evaluation summary |

### Proposed (not yet implemented)

The following require instrumentation changes and are not available in the current release:

| Metric | Notes |
|--------|-------|
| API latency (per call) | Requires timing code around `_call_openai_api()` |
| Token usage (per call) | Requires parsing the API response usage fields |
| Estimated cost | Derived from token usage × per-token pricing |
| Retry rate | Requires retry logic to be instrumented |
| Grounding failure rate (LLM outputs only) | Currently derivable from evaluation for stored outputs; not tracked per-run |

Human-review quality metrics (readability, stakeholder usefulness) require human input per the rubric in `docs/genai_evaluation_rubric.md` and are not automated.
