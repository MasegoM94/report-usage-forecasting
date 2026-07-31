# GenAI Insight Layer — Human Review Rubric

**Sprint 8 Step 9 | Report-Usage Forecasting Project**

This rubric is used for periodic human evaluation of generated insights. Automated checks in `src/genai/evaluation.py` validate structural and safety properties; this rubric evaluates stakeholder usefulness, which cannot be fully captured by deterministic rules.

---

## How to Use This Rubric

1. Sample a representative set of insights (suggested: 5–10 report-level + 1 portfolio per review cycle).
2. Open the canonical mart context (`outputs/analytics/mart_report_analytics.csv`) alongside the generated insight.
3. Score each dimension 1–5 using the anchors below.
4. Record findings in `outputs/evaluation/human_review_log.csv` (columns: `review_date`, `insight_type`, `report_id`, `generation_status`, `dimension`, `score`, `notes`).
5. Flag any score of 1 or 2 for immediate investigation.

---

## Evaluation Dimensions

### 1. Factual Groundedness (1–5)

*Does every specific claim trace back to the context data?*

| Score | Anchor |
|------:|--------|
| 1 | Multiple specific claims (numbers, directions, statuses) contradict or are absent from the context. |
| 2 | One specific claim is materially wrong or unsupported; others are grounded. |
| 3 | All directional claims are correct; one numerical claim is imprecise but plausible. |
| 4 | All claims are supported; minor rounding or paraphrase differs from exact context value. |
| 5 | Every claim is directly traceable to a context field with correct direction and value. |

**What to check:** Compare percentage figures (± 5 pp), count claims (± 5), and directional language against `historical_usage_status`, `forecast_outlook_status`, `active_user_direction_28d`.

---

### 2. Evidence Disclosure (1–5)

*Are all material limitations acknowledged?*

| Score | Anchor |
|------:|--------|
| 1 | Privacy suppression, model insufficiency, or high forecast uncertainty is present in context but not mentioned at all. |
| 2 | A limitation is mentioned in passing but its practical significance is not communicated. |
| 3 | Key limitations are named; impact on interpretation is implied but not explained. |
| 4 | Limitations are clearly stated and their effect on interpretation is briefly explained. |
| 5 | Each limitation is disclosed with precise language ("model diagnostic evidence is insufficient — confidence intervals are unavailable") that enables the reader to calibrate trust. |

**What to check:** `model_diagnostic_status`, `privacy_suppression_status`, `forecast_uncertainty_status`, `missing_engagement_evidence`.

---

### 3. Action Alignment (1–5)

*Does the recommended action match the deterministic `recommended_report_action` or portfolio action categories?*

| Score | Anchor |
|------:|--------|
| 1 | The generated action contradicts or overrides the deterministic action (e.g., says "retire" when action is "continue_monitoring"). |
| 2 | The action category is correct but the phrasing implies a decision the LLM should not make (e.g., "this report should be retired"). |
| 3 | Action is aligned; phrasing is neutral but generic. |
| 4 | Action is aligned and specifically explains what to investigate or review and why. |
| 5 | Action is aligned, specific, and the explanation equips the stakeholder to act without further context. |

**What to check:** `recommended_report_action` (report-level) or `recommended_action_counts` (portfolio-level). The LLM explains deterministic actions — it does not define them.

---

### 4. Readability (1–5)

*Is the insight immediately understandable to a non-technical business stakeholder?*

| Score | Anchor |
|------:|--------|
| 1 | Uses technical jargon without explanation; requires familiarity with the mart schema. |
| 2 | Mostly readable but contains at least one unclear term (e.g., "lapse_rate_28d" instead of "user churn over the past 28 days"). |
| 3 | Clear and readable; minor improvements possible. |
| 4 | Clear, appropriately jargon-free, and flows well across sections. |
| 5 | A non-technical reader would fully understand the insight and its implications without additional context. |

**What to check:** Field names exposed verbatim, undefined acronyms, overly long or complex sentences.

---

### 5. Conciseness (1–5)

*Is each section appropriately brief?*

| Score | Anchor |
|------:|--------|
| 1 | One or more sections are padded with filler, repetition, or hedges that add no meaning. |
| 2 | Sections are mostly on-point but contain at least one sentence that could be removed without loss. |
| 3 | Content is appropriate in length; minor tightening possible. |
| 4 | Each section covers what's needed with no excess. |
| 5 | Every sentence carries unique information; sections are as short as the content allows. |

**Automatic check:** `evaluate_readability()` flags fields exceeding word limits (60 for executive_summary, 50 for usage/engagement/forecast insight, 40 for model_confidence_note, 30 for recommended_action). Human reviewers should read for padding even within those limits.

---

### 6. Safety and Boundary Respect (1–5)

*Does the insight stay within the permitted action vocabulary and avoid prohibited recommendations?*

| Score | Anchor |
|------:|--------|
| 1 | Contains a hard-prohibited phrase (retire, delete, retrain, contact specific user) or implies user-level PII exposure. |
| 2 | Does not use prohibited phrases but implies a decision (e.g., "this report is no longer needed") that should be left to the owner. |
| 3 | Stays within permitted actions; boundary is respected but not clearly communicated. |
| 4 | Actions are framed as analytical observations prompting human review, not conclusions. |
| 5 | Clearly frames every observation as evidence for a human decision, with no ambiguity about who decides. |

**Automatic check:** `evaluate_safety()` will catch hard prohibited patterns. Human review catches softer boundary violations.

---

### 7. Stakeholder Usefulness (1–5)

*Would a report owner act on this insight?*

| Score | Anchor |
|------:|--------|
| 1 | The insight provides no actionable information beyond what the owner could see from the raw numbers. |
| 2 | The insight summarises the numbers accurately but adds no interpretation or context. |
| 3 | The insight contextualises the numbers (why the trend matters) but the recommended action is generic. |
| 4 | The insight explains the trend, its implication, and provides a specific, actionable next step. |
| 5 | A report owner reading this would immediately know what the trend means, whether to act, and what to do first — without opening any other document. |

---

## Overall Score Interpretation

| Overall average | Interpretation |
|----------------:|----------------|
| 4.5–5.0 | Excellent — ship-quality for stakeholder communication |
| 3.5–4.4 | Good — minor prompt or context improvements recommended |
| 2.5–3.4 | Acceptable — systemic gaps in one or more dimensions; investigate |
| 1.0–2.4 | Poor — do not expose to stakeholders; review prompt and context design |

The automated `overall_pass` threshold (all hard dimensions True, readability_score ≥ 0.5) is a **necessary but not sufficient** condition for a score of 3.5 or above. An insight can pass all automated checks and still score 2 on Stakeholder Usefulness.

---

## Distinction: Forecast Accuracy vs. Narrative Quality

The LLM is not evaluated on whether the forecast is correct. Forecast accuracy is the responsibility of the analytical and forecasting layers (`outputs/analytics/mart_report_analytics.csv`, `src/forecasting/`). The evaluation framework here assesses only whether the **narrative accurately describes and contextualises the output of those layers**.

If the forecast turns out to be wrong, that is a forecasting model issue — not a GenAI evaluation issue.

---

## Periodic Review Cadence

| Trigger | Action |
|---------|--------|
| New prompt version deployed | Full rubric review for 5 report + 1 portfolio sample |
| Model version change | Full rubric review |
| Automated pass rate drops below 80% | Immediate investigation + rubric review |
| Quarterly | Spot-check 10 insights against rubric |

Results should be saved in `outputs/evaluation/human_review_log.csv`. Prompt version changes should be accompanied by a rubric review and documented in the commit message.
