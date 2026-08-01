# Portfolio Audit Report

**Project:** Power BI Report Usage Forecasting — Streamlit Reviewer App  
**Audit date:** 2026-08-01  
**Auditor:** Claude Code (read-only; no files modified, no deployments, no live LLM calls)  
**Audit scope:** Packaging, reproducibility, deployment readiness, and portfolio presentation

---

## 1. Repository Structure Summary

The repository is a monorepo Python project with the following top-level layout:

```
report-usage-forecasting/
├── src/
│   ├── __init__.py
│   ├── analytics/        # Report features, user frequency, engagement metrics
│   ├── app/              # Streamlit app entry point + utils (no __init__.py)
│   │   └── utils/        # Filter, load, chart, report, portfolio helpers (no __init__.py)
│   ├── config/           # Constants (forecasting.py, config.py)
│   ├── features/         # Feature engineering
│   ├── genai/            # GenAI pipeline (insight_generator, portfolio_insights)
│   ├── models/           # Forecasting, diagnostics, calibration
│   ├── monitoring/       # Drift and monitoring
│   ├── persistence/      # Forecast history persistence
│   └── pipelines/        # run_forecasting_pipeline.py
├── notebooks/            # 10 notebooks (2 duplicate prefix collisions: 06_*, 08_*)
├── tests/                # 3 580 tests, 101 skipped
├── data/                 # .gitkeep only (runtime data excluded by .gitignore)
├── outputs/              # .gitkeep scaffolding + 1 sample CSV + 2 test fixtures
├── docs/                 # sprint completion docs, interview prep guides (untracked MDs)
├── requirements.txt      # 134 packages, 132 pinned, 2 unpinned (plotly, streamlit)
├── .gitignore
├── conftest.py
└── LICENSE               # MIT
```

**What is NOT present:** `.github/`, `.streamlit/`, `pyproject.toml`, `setup.py`, `Makefile`, `Dockerfile`, `.env.example`, `pytest.ini`, any linting config.

---

## 2. Files and Configurations Inspected

| File / Path | Inspected |
|---|---|
| `requirements.txt` | Full content (134 lines) |
| `.gitignore` | Full content |
| `conftest.py` | Full content |
| `src/app/streamlit_app.py` | Full content |
| `src/app/utils/load_data.py` | Full content |
| `src/app/utils/charts.py` | Full content |
| `src/app/utils/filter_helpers.py` | Full content |
| `src/app/utils/report_helpers.py` | Full content |
| `src/app/utils/portfolio_helpers.py` | Full content |
| `src/app/utils/definitions.py` | Full content |
| `src/config/forecasting.py` | Full content |
| `src/genai/insight_generator.py` | Full content |
| `src/genai/portfolio_insights.py` | Full content |
| `notebooks/` (all 10) | File listing; selected cells read |
| `tests/` (all test files) | Listing + key files full-read |
| `docs/sprint_9_completion.md` | Full content |
| `LICENSE` | Full content |
| `git ls-files` output | Full tracked-file listing |
| `git status --short` | Full untracked-file listing |

---

## 3. Dependency and Installation Findings

### requirements.txt

- **134 packages** listed as a flat pip freeze (runtime, dev, and notebook dependencies mixed).
- **132 packages are pinned** to exact versions (e.g., `pandas==2.3.1`, `scikit-learn==1.6.1`).
- **2 packages are unpinned:** `plotly` and `streamlit` appear without version specifiers on the final two lines. These are the two most critical runtime dependencies for the Streamlit application — pinning them is essential for reproducible deployments.
- No separation of runtime vs. development vs. test dependencies (no `requirements-dev.txt`, no optional dependency groups).
- No `pyproject.toml`, `setup.py`, or `setup.cfg` — the project cannot be installed as a package with `pip install -e .`.
- No `Makefile` or task runner to document standard install commands.

### Installation path

To run the application currently, a contributor must:
1. Infer the install command (`pip install -r requirements.txt`) — it is not documented beyond the README.
2. Manually set `OPENAI_API_KEY` in the environment (no `.env.example` or documented env-var list).
3. Run `streamlit run src/app/streamlit_app.py` from the repo root (documented in README).

---

## 4. Import and Package Findings

### sys.path manipulation

The project relies on manual `sys.path` manipulation instead of package installation in three places:

| Location | Manipulation |
|---|---|
| `conftest.py` | `sys.path.insert(0, str(Path(__file__).parent))` — adds repo root so `src.*` imports resolve in tests |
| `src/app/streamlit_app.py` | `sys.path.insert(0, str(Path(__file__).parent))` — adds `src/app/` so `utils.*` imports resolve in Streamlit runtime |
| Every notebook | Each adds `PROJECT_ROOT` independently |

### Missing `__init__.py`

The following directories contain Python modules but no `__init__.py`:

| Directory | Effect |
|---|---|
| `src/app/` | `src.app` is not a proper package; Streamlit resolves via sys.path instead |
| `src/app/utils/` | `src.app.utils` is not a proper package; resolves via sys.path |
| `src/data/` | Not a package (if it exists — directory not in tracked files) |

Directories **with** `__init__.py`: `src/`, `src/analytics/`, `src/config/`, `src/features/`, `src/genai/`, `src/models/`, `src/monitoring/`, `src/persistence/`, `src/pipelines/`.

### Dual status_label() functions

`portfolio_helpers.py` and `definitions.py` both define a `status_label()` function with overlapping but inconsistent label strings. `streamlit_app.py` imports both as `_status_label` and `_def_status_label`. Example discrepancy: `"growing_usage"` maps to `"Growing"` in `portfolio_helpers.py` and `"Growing usage"` in `definitions.py`.

### try/except import pattern

`filter_helpers.py` and `load_data.py` use a `try/except ModuleNotFoundError` pattern to handle two resolution contexts: Streamlit runtime (`utils.*`) and test context (`src.app.utils.*`). This is functional but fragile — a packaging approach would eliminate the need entirely.

---

## 5. Configuration Findings

| Config file | Status | Impact |
|---|---|---|
| `.streamlit/config.toml` | **Missing** | No theme, server port, or CORS settings documented |
| `.streamlit/secrets.toml` | **Missing** | No secrets management documented for Streamlit Cloud |
| `pyproject.toml` | **Missing** | No project metadata, build system, or tool config |
| `pytest.ini` / `pyproject.toml [tool.pytest]` | **Missing** | Test command not formally configured |
| `ruff.toml` / `.flake8` | **Missing** | No linting standard enforced |
| `.env.example` | **Missing** | `OPENAI_API_KEY` environment variable is undocumented |
| `Makefile` | **Missing** | No standard task shortcuts (install, test, lint, run) |

The only configuration that exists is `src/config/forecasting.py` (pipeline constants: `FORECAST_HORIZON_DAYS=28`, `MIN_TRAIN_DAYS=180`, etc.) and `src/config/config.py` (path constants).

---

## 6. Artifact Dependency Map

```
data/raw/reports.csv                  ← notebook 01 (generates synthetic data)
        ↓
data/processed/                       ← notebooks 02, 03, 04
        ↓
src/pipelines/run_forecasting_pipeline.py
        ├── src/models/ (forecasting, diagnostics, calibration)
        ├── src/analytics/ (report features, user frequency, engagement)
        ├── src/persistence/ (realized forecast history)
        └── src/genai/ (insight_generator, portfolio_insights)
                ↓
outputs/analytics/mart_report_analytics.csv        ← app primary source
outputs/analytics/mart_report_engagement.csv
outputs/analytics/mart_report_user_daily.csv
outputs/forecasts/report_view_forecasts_latest.csv
outputs/insights/report_ai_insights.json
outputs/insights/portfolio_ai_insight.json
        ↓
src/app/streamlit_app.py              ← reads all outputs/ files
src/app/utils/load_data.py            ← load_app_data(root=...)
src/app/utils/{charts, filter, report, portfolio, definitions}.py
```

All `outputs/` files are **runtime-generated artifacts** excluded from git (`.gitignore` is correctly configured). The only committed CSV is `outputs/forecasts/sample_baseline_forecasts.csv` (baseline reference, small).

---

## 7. Execution Commands and Manual Gaps

### Documented commands (from README)

| Task | Command |
|---|---|
| Install dependencies | `pip install -r requirements.txt` |
| Run Streamlit app | `streamlit run src/app/streamlit_app.py` |
| Run tests | `pytest` (inferred; not in a config file) |
| Run pipeline | `python src/pipelines/run_forecasting_pipeline.py` (inferred from README) |

### Manual gaps (undocumented steps)

1. **Environment variable setup** — `OPENAI_API_KEY` must be set before running the GenAI pipeline. No `.env.example` or documented step.
2. **Data generation** — synthetic data must be generated by running notebooks 01–04 in order before the pipeline runs. Not scripted; no `Makefile` target.
3. **Notebook execution order** — README lists notebooks but does not state they must be run sequentially to produce `data/processed/` inputs.
4. **Virtual environment** — not mentioned in any setup instructions.
5. **Python version** — not pinned in any config file (runtime behaviour depends on `pandas==2.3.1` which requires Python ≥ 3.9).

---

## 8. Reproducibility Findings

| Area | Finding | Severity |
|---|---|---|
| Dependency pinning | 132/134 packages pinned; `plotly` and `streamlit` unpinned | Medium |
| Python version | Not specified in any file | Medium |
| Data generation | Synthetic data pipeline runnable but not scripted | Low |
| Randomness | `np.random.seed` / `random.seed` not audited across all modules | Low |
| Notebook execution | Sequential dependency not documented or enforced | Low |
| Environment variables | `OPENAI_API_KEY` path undocumented | Low |
| Test reproducibility | All 3 580 tests pass deterministically (confirmed) | ✓ |
| Output artifacts | Excluded from git; regenerated by pipeline | ✓ |

The project is **reproducible with effort** — a motivated contributor can reconstruct everything by following the README, but there are manual gaps that create friction.

---

## 9. Security and Secret Findings

**No active credential exposure found.**

| Check | Finding |
|---|---|
| Hardcoded API keys | Not found in any source file |
| `OPENAI_API_KEY` handling | Read via `os.environ.get("OPENAI_API_KEY", "")` in `insight_generator.py` and `portfolio_insights.py` — correct |
| Committed `.env` file | Not found |
| Secrets in notebooks | Not found in inspected cells |
| `.streamlit/secrets.toml` | Not present (not a risk; also not a cloud deployment) |
| Word lock files (untracked) | `docs/~$terview_prep_genai_insight_layer.docx` and `docs/~$terview_prep_report_usage_forecasting.docx` are **untracked** — safe |

**Recommendation:** Add `.env.example` documenting `OPENAI_API_KEY=` (blank value) so contributors know the variable is required without accidentally committing a real value.

---

## 10. Git-Hygiene Findings

### Tracked files (210 total)

| Category | Count | Notes |
|---|---|---|
| Python source files | 166 | Clean |
| Markdown docs | 19 | Clean |
| Notebooks | 10 | 2 duplicate prefix collisions (see below) |
| `.gitkeep` files | 9 | Correct use for empty directory scaffolding |
| Test fixture JSON | 2 | `genai_evaluation_cases.json`, `genai_golden_outputs.json` — appropriate |
| Sample CSV | 1 | `outputs/forecasts/sample_baseline_forecasts.csv` — appropriate |
| `requirements.txt` | 1 | Clean |
| `.gitignore` | 1 | Clean |
| `LICENSE` | 1 | Clean |

### Untracked files (not committed — correct per .gitignore)

The `.gitignore` correctly excludes:
- `data/raw/` (synthetic source data, regenerable)
- `data/processed/` (intermediate processed data)
- `outputs/analytics/` (all mart CSVs including the 43MB `mart_report_user_daily.csv`)
- `outputs/insights/` (AI insight JSON files)
- `outputs/forecasts/` (except the committed sample file)

### Issues found

| Issue | File(s) | Severity |
|---|---|---|
| **Duplicate notebook prefix 06** | `notebooks/06_model_diagnostics.ipynb` and `notebooks/06_report_analytics.ipynb` | Medium — confuses sequential ordering |
| **Duplicate notebook prefix 08** | `notebooks/08_genai_insights.ipynb` and `notebooks/08_report_analytics.ipynb` | Medium — same issue |
| **Untracked Word lock files** | `docs/~$terview_prep_genai_insight_layer.docx`, `docs/~$terview_prep_report_usage_forecasting.docx` | Low — untracked but should be in `.gitignore` |
| **Untracked root scripts** | `CODEBASE_AUDIT.docx`, `build_audit_doc.py`, `build_interview_doc.py`, `build_interview_doc.js` | Low — tooling scripts that should be gitignored or committed intentionally |
| **Untracked interview prep MDs** | `docs/interview_prep_genai_insight_layer.md`, `docs/interview_prep_report_usage_forecasting.md` | Low — valuable docs that should be committed or explicitly gitignored |
| **No `.DS_Store` in .gitignore** (entries exist but files may still be untracked) | `.gitignore` | Low |

---

## 11. Test and Quality-Tool Findings

### Test suite

| Metric | Value |
|---|---|
| Total tests | 3 580 passed, 101 skipped, 0 failures |
| Sprint 9 new tests | 330 (smoke, load-data integration, privacy/evidence, filter+selection integration) |
| Test runner | pytest (no `pytest.ini` or `pyproject.toml [tool.pytest]`) |
| Test runtime | ~1 min 44 sec |
| Streamlit AppTest | Not used — all tests target pure-logic helpers (documented limitation) |

### Quality tooling

| Tool | Status |
|---|---|
| `ruff` / `flake8` | **Not configured** — no linting enforced |
| `black` / `isort` | **Not configured** — no formatting enforced |
| `mypy` / `pyright` | **Not configured** — no type checking |
| `bandit` | **Not configured** — no security linting |
| `coverage.py` | **Not configured** — no coverage measurement |

The absence of linting and type-checking config means code quality standards are maintained informally, not enforced by tooling.

---

## 12. CI Findings

**No CI exists.**

- `.github/` directory: **does not exist**
- No GitHub Actions workflows
- No CircleCI, Travis, or other CI configuration

Without CI, there is no automated verification that tests pass on push, no lint gate, and no dependency vulnerability scanning. For a portfolio project presented to employers, CI is a visible signal of engineering practice.

---

## 13. Streamlit Deployment Findings

| Area | Finding |
|---|---|
| `.streamlit/config.toml` | **Missing** — no theme, server port, or CORS documented |
| `.streamlit/secrets.toml` | **Missing** — no secret management for Streamlit Cloud |
| `requirements.txt` | Present and usable by Streamlit Cloud |
| Entry point | `src/app/streamlit_app.py` — requires root-relative `sys.path` manipulation |
| `plotly` version | **Unpinned** — Streamlit Cloud would install the latest, risking API breaks |
| `streamlit` version | **Unpinned** — same risk |
| Live demo URL | **None** — no deployed instance; no screenshot or demo GIF in README |
| Environment variable | `OPENAI_API_KEY` would need to be set in Streamlit Cloud secrets |

To deploy to Streamlit Cloud today: create `.streamlit/config.toml`, pin `plotly` and `streamlit` in `requirements.txt`, and set `OPENAI_API_KEY` as a Streamlit secret.

---

## 14. Container-Readiness Findings

| Area | Finding |
|---|---|
| `Dockerfile` | **Missing** |
| `.dockerignore` | **Missing** |
| Image base | Not specified anywhere |
| Port configuration | Not documented |
| Health check | Not defined |

The application is not container-ready. A minimal Dockerfile for Streamlit would be straightforward to add (FROM python:3.11-slim, COPY requirements.txt, RUN pip install, COPY src/, CMD streamlit run).

---

## 15. Documentation Findings

### Present and complete

| Document | Quality |
|---|---|
| `README.md` | Comprehensive — sprint-by-sprint feature log, setup instructions, test counts |
| `docs/sprint_9_completion.md` | Complete — all deliverables, limitations, deferred items |
| `src/config/forecasting.py` | Well-documented constants with inline explanations |
| Inline docstrings | Present in key utility functions |

### Missing or incomplete

| Gap | Impact |
|---|---|
| No `CONTRIBUTING.md` | Contributor onboarding is undocumented |
| No `CHANGELOG.md` | Release history is captured in README section headers (non-standard) |
| No architecture diagram in README | Reviewers cannot quickly grasp system shape |
| No `.env.example` | Environment setup requires reading source code |
| `docs/interview_prep_*.md` files are **untracked** | Valuable portfolio context not visible in the repo |
| No demo screenshot or GIF | Employers cannot see the application without running it |

---

## 16. Portfolio-Presentation Findings

### Strengths

- **Test coverage depth**: 3 580 tests across unit, integration, and smoke categories is strong for a solo portfolio project.
- **Documentation quality**: Sprint completion docs, terminology tables, and clear limitation disclosures show professional practice.
- **Sprint structure**: Progressive sprint log in README shows iterative delivery.
- **Privacy and terminology discipline**: Explicit test classes for suppression, misuse terminology, and automated-action risks demonstrate awareness of production concerns.
- **GenAI design**: Offline batch generation, grounding against deterministic analytics, structured output schema, and 6-state validation is a mature pattern.
- **MIT license**: Present and correctly formatted.

### Weaknesses

- **No live demo**: A deployed Streamlit app or GIF walkthrough is the single highest-impact missing portfolio element.
- **No CI badge**: A passing CI badge on the README is an immediate visual signal to technical reviewers.
- **Duplicate notebook numbers**: `06_*` and `08_*` collisions look like organizational noise during a repository walk.
- **Untracked interview prep docs**: `docs/interview_prep_*.md` files exist but are not committed — they add context but are invisible in GitHub.
- **`streamlit` and `plotly` unpinned**: A first-time reviewer running `pip install -r requirements.txt` could get a broken environment.
- **No architecture diagram**: Adds ~5 minutes of comprehension time for a technical reviewer.

---

## 17. Licensing Findings

- **License file:** `LICENSE` (MIT), present at repo root.
- **Copyright:** 2026 Masego Modibane — correct.
- **Third-party license audit:** Not performed (out of scope for this audit).
- **Status:** **COMPLIANT** — MIT license is appropriate for a portfolio project and imposes no restrictions on employer review.

---

## 18. Capability-Status Table

| Area | Status | Notes |
|---|---|---|
| Synthetic data generation | COMPLETE | Notebooks 01–04 produce 30-report dataset |
| Forecasting pipeline | COMPLETE | ETS/ARIMA candidates, backtest, production forecast |
| Model diagnostics | COMPLETE | Bias, stability, calibration, autocorrelation |
| Canonical analytics mart | COMPLETE | `mart_report_analytics.csv` with all status codes |
| Engagement analytics | COMPLETE | Privacy suppression, concentration, frequency |
| GenAI insight layer | COMPLETE | 6-state schema, batch offline generation |
| Portfolio GenAI summary | COMPLETE | With state-aware rendering |
| Streamlit reviewer app | COMPLETE | Portfolio + Explorer tabs, sidebar filters |
| Chart accessibility | COMPLETE | Dual encoding (colour + line style + markers), correct terminology |
| Filter system | COMPLETE | Search, multi-field AND, attention filter, clear-all |
| Definitions and terminology | COMPLETE | 19 definitions, `STATUS_LABELS`, status_label() |
| Test suite | COMPLETE | 3 580 passing, 101 skipped (all live-API), 0 failures |
| Privacy safeguards | COMPLETE | Suppression propagation, no user identifiers in app layer |
| CI / CD pipeline | MISSING | No `.github/workflows/` |
| Streamlit deployment config | MISSING | No `.streamlit/config.toml` |
| Container readiness | MISSING | No Dockerfile |
| Packaging (pyproject.toml) | MISSING | sys.path manipulation instead |
| Linting / type-checking config | MISSING | No ruff, flake8, mypy |
| Live demo | MISSING | No deployed URL or screenshot |
| `.env.example` | MISSING | `OPENAI_API_KEY` undocumented |
| `plotly` / `streamlit` pinned | DEFECTIVE | Unpinned in requirements.txt |
| Notebook numbering | DEFECTIVE | Duplicate 06_* and 08_* prefixes |
| Dual status_label() | DEFECTIVE | Inconsistent label strings across two modules |
| `src/app/` __init__.py | MISSING | Not a proper Python package |

---

## 19. Blocking Defects

These are defects that would cause a failure on first run or immediate confusion for a technical reviewer:

1. **`plotly` and `streamlit` unpinned in `requirements.txt`** — a fresh `pip install -r requirements.txt` may install incompatible versions. Risk is low today but increases over time. *Fix: append `==<current_version>` to both lines.*

2. **`sys.path` manipulation instead of package install** — the project cannot be imported as `from report_usage_forecasting import ...`. The `try/except ModuleNotFoundError` pattern in `filter_helpers.py` and `load_data.py` is a symptom. *Fix: add `pyproject.toml` with a minimal `[build-system]` and `[project]` section; add `__init__.py` to `src/app/` and `src/app/utils/`.*

3. **No `.env.example`** — contributors must read source code to discover `OPENAI_API_KEY` is required. *Fix: add `.env.example` with `OPENAI_API_KEY=` (blank).*

---

## 20. Highest-Priority Improvements

Ranked by impact on portfolio presentation and onboarding friction:

1. **Pin `plotly` and `streamlit`** — 5-minute fix, eliminates the most concrete reproducibility risk.
2. **Add a GitHub Actions CI workflow** — run `pytest` on push to `main`. Adds the CI badge to README and provides automated regression protection.
3. **Deploy to Streamlit Cloud** — add `.streamlit/config.toml`, set `OPENAI_API_KEY` in Streamlit secrets, get a shareable URL. Single highest-impact portfolio improvement.
4. **Commit the interview prep docs** (`docs/interview_prep_*.md`) — they are already written and on disk; committing them makes them visible in the repo.
5. **Add an architecture diagram to README** — even a simple Mermaid flowchart reduces reviewer orientation time significantly.
6. **Rename duplicate notebooks** — renumber `06_report_analytics.ipynb` → `07_report_analytics.ipynb` and `08_report_analytics.ipynb` → `09_report_analytics.ipynb`, updating all `07_user_analytics.ipynb` and downstream references accordingly.
7. **Add `pyproject.toml`** — minimal `[project]` section + `pip install -e .` eliminates `sys.path` manipulation across all entry points.
8. **Add `.env.example`** — documents `OPENAI_API_KEY` requirement.
9. **Resolve dual `status_label()` functions** — merge into a single canonical source in `definitions.py`; remove the copy from `portfolio_helpers.py`.
10. **Add a demo screenshot or GIF to README** — makes the application visible without running it.

---

## 21. Work That Should Be Deferred

| Item | Reason |
|---|---|
| Dockerfile and container build | Not needed for Streamlit Cloud deployment; adds complexity for no near-term gain |
| Type-checking with mypy | High annotation burden across 166 source files; would require significant annotation work before providing value |
| Coverage reporting | Useful but not blocking; add after CI is established |
| Full linting with ruff and pre-commit | Valuable but requires agreeing on rule set; defer until CI exists |
| Real Power BI telemetry | Out of scope for a portfolio project; synthetic data is intentional |
| Filter-specific GenAI summaries | Requires live LLM calls or separate offline generation; explicitly deferred per sprint constraints |
| AppTest-based Streamlit smoke tests | Requires active Streamlit event loop; environment-dependent; pure-logic coverage already provides equivalent assurance |
| Multi-report comparison view | Not planned; no user demand established |

---

## 22. Recommended Sprint 10 Implementation Order

**Week 1 — Immediate unblocking (1–2 days)**

1. Pin `plotly` and `streamlit` in `requirements.txt`.
2. Add `.env.example` with `OPENAI_API_KEY=`.
3. Commit `docs/interview_prep_*.md` files.
4. Add `docs/~$terview_prep_*.docx` and `CODEBASE_AUDIT.docx` to `.gitignore`.

**Week 2 — CI and deployment (2–3 days)**

5. Add `.github/workflows/ci.yml` running `pytest` on push/PR to `main`.
6. Add `.streamlit/config.toml` (theme, server config).
7. Deploy to Streamlit Community Cloud; add URL to README header.
8. Add CI badge and demo URL to README.

**Week 3 — Code quality (2–3 days)**

9. Add `pyproject.toml` with `[project]` metadata and `pip install -e .` support.
10. Add `__init__.py` to `src/app/` and `src/app/utils/`.
11. Remove `sys.path` manipulation from `streamlit_app.py` and `conftest.py` (replace with installed package imports).
12. Resolve dual `status_label()` — canonical version in `definitions.py` only.
13. Renumber conflicting notebook prefixes.

**Week 4 — Portfolio polish (1–2 days)**

14. Add a Mermaid architecture diagram to README.
15. Add a screenshot or GIF of the running application to README.
16. Review and tighten README for a first-time reader (current README is sprint-log heavy; add a "What this is" intro paragraph).

---

## 23. Release-Readiness Classification

**NOT READY**

The application is feature-complete and the test suite is strong, but the repository is missing infrastructure that an employer or collaborator would expect before treating a project as portfolio-ready:

| Blocker | Status |
|---|---|
| `plotly` and `streamlit` unpinned | ❌ Blocking |
| No CI | ❌ Blocking for portfolio signal |
| No live demo | ❌ Blocking for portfolio impact |
| No Streamlit deployment config | ❌ Blocking for deployment |
| `sys.path` manipulation instead of packaging | ⚠ Significant friction |
| Duplicate notebook numbers | ⚠ Organisational noise |
| Dual `status_label()` | ⚠ Maintainability risk |
| No `.env.example` | ⚠ Onboarding friction |

**Reclassification path:** The project would reach **READY WITH MINOR FIXES** after completing Sprint 10 Week 1–2 items (pin dependencies, add CI, deploy to Streamlit Cloud). The full Week 3–4 items bring it to **READY**.

The analytical work, GenAI design, privacy handling, and test discipline are portfolio-quality. The gaps are all infrastructure, not capability.
