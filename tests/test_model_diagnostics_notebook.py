"""Structural and static tests for notebooks/06_model_diagnostics.ipynb.

These tests do NOT execute the notebook.  They read the .ipynb JSON directly and
verify structure, imports, content policies, and Python syntax.
"""

import ast
import json
import re
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "06_model_diagnostics.ipynb"


def _load_notebook():
    with open(NOTEBOOK_PATH, encoding="utf-8") as fh:
        return json.load(fh)


def _cells(nb):
    return nb["cells"]


def _markdown_cells(nb):
    return [c for c in _cells(nb) if c["cell_type"] == "markdown"]


def _code_cells(nb):
    return [c for c in _cells(nb) if c["cell_type"] == "code"]


def _cell_source(cell):
    src = cell.get("source", "")
    if isinstance(src, list):
        return "".join(src)
    return src


def _all_markdown_source(nb):
    return "\n".join(_cell_source(c) for c in _markdown_cells(nb))


def _all_code_source(nb):
    return "\n".join(_cell_source(c) for c in _code_cells(nb))


def _extract_h2_headings(nb):
    """Return the set of ## heading texts from all markdown cells."""
    headings = set()
    for cell in _markdown_cells(nb):
        for line in _cell_source(cell).splitlines():
            m = re.match(r"^##\s+(.+)", line)
            if m:
                headings.add(m.group(1).strip())
    return headings


# ---------------------------------------------------------------------------
# Test 1: File existence
# ---------------------------------------------------------------------------


def test_notebook_file_exists():
    assert NOTEBOOK_PATH.exists(), (
        f"Notebook not found at {NOTEBOOK_PATH}. "
        "Run Task 1 to create notebooks/06_model_diagnostics.ipynb."
    )


# ---------------------------------------------------------------------------
# Test 2: Required section headings
# ---------------------------------------------------------------------------

REQUIRED_HEADINGS = [
    "Business Objective",
    "Diagnostic Architecture",
    "Residual Sign Conventions",
    "Load and Validate Diagnostic Outputs",
    "Representative Report Selection",
    "Training Residuals vs Forecast Errors",
    "Residual Time-Series Plots",
    "Autocorrelation Diagnostics",
    "Bias Diagnostics",
    "Residual Variance Stability",
    "Outlier Diagnostics",
    "Distribution Diagnostics",
    "Prediction Interval Calibration",
    "Report-Level Model-Health Summary",
    "Model-Health Classification",
    "Representative Model-Health Case Studies",
    "Relationship to Model Selection",
    "Relationship to Later Sprints",
    "Limitations",
]


def test_all_required_section_headings_present():
    nb = _load_notebook()
    found = _extract_h2_headings(nb)
    missing = [h for h in REQUIRED_HEADINGS if h not in found]
    assert not missing, (
        f"Missing {len(missing)} required ## headings:\n"
        + "\n".join(f"  - {h}" for h in missing)
        + f"\n\nHeadings found: {sorted(found)}"
    )


def test_heading_count_is_at_least_19():
    nb = _load_notebook()
    found = _extract_h2_headings(nb)
    assert len(found) >= 19, (
        f"Expected at least 19 ## headings, found {len(found)}: {sorted(found)}"
    )


# ---------------------------------------------------------------------------
# Test 3: Imports from src/
# ---------------------------------------------------------------------------

SRC_IMPORT_PATTERNS = [
    r"from src\.",
    r"import src\.",
]


def test_notebook_imports_from_src():
    nb = _load_notebook()
    code = _all_code_source(nb)
    found = any(re.search(pat, code) for pat in SRC_IMPORT_PATTERNS)
    assert found, (
        "Notebook does not import from src/ modules. "
        "Expected 'from src.' or 'import src.' in code cells."
    )


def test_imports_schema_constants_from_residual_datasets():
    nb = _load_notebook()
    code = _all_code_source(nb)
    assert "residual_datasets" in code, (
        "Notebook should import from src.models.residual_datasets."
    )


def test_imports_model_health_cols():
    nb = _load_notebook()
    code = _all_code_source(nb)
    assert "MODEL_HEALTH_COLS" in code, (
        "Notebook should import MODEL_HEALTH_COLS from src.models.model_health."
    )


# ---------------------------------------------------------------------------
# Test 4: No duplicate core formula definitions
# ---------------------------------------------------------------------------

DUPLICATE_FORMULA_PATTERNS = [
    # Would indicate a re-definition rather than importing the canonical function
    r"def\s+\w+.*actual\s*-\s*forecast",
    r"def\s+\w+.*forecast\s*-\s*actual",
]


def test_no_duplicate_core_formula_definitions():
    nb = _load_notebook()
    code = _all_code_source(nb)
    for pat in DUPLICATE_FORMULA_PATTERNS:
        assert not re.search(pat, code), (
            f"Notebook appears to redefine a core residual formula (pattern: {pat!r}). "
            "Import the canonical function from src/ instead."
        )


# ---------------------------------------------------------------------------
# Test 5: No live API calls
# ---------------------------------------------------------------------------

LIVE_API_PATTERNS = [
    r"requests\.get\(",
    r"requests\.post\(",
    r"openai\.",
    r"anthropic\.",
    r"boto3\.",
    r"httpx\.",
    r"urllib\.request\.",
]


def test_no_live_api_calls():
    nb = _load_notebook()
    code = _all_code_source(nb)
    for pat in LIVE_API_PATTERNS:
        assert not re.search(pat, code), (
            f"Notebook contains a live API call pattern: {pat!r}. "
            "Diagnostics notebooks must not call external APIs."
        )


# ---------------------------------------------------------------------------
# Test 6: No synthetic fallback data generation
# ---------------------------------------------------------------------------

SYNTHETIC_DATA_PATTERNS = [
    r"np\.random\.(rand|randn|randint|normal|uniform|choice)\(",
    r"random\.seed\(",
    r"np\.random\.seed\(",
]


def test_no_synthetic_fallback_data():
    nb = _load_notebook()
    code = _all_code_source(nb)
    for pat in SYNTHETIC_DATA_PATTERNS:
        assert not re.search(pat, code), (
            f"Notebook contains synthetic data generation: {pat!r}. "
            "Fallback must be an empty DataFrame, not synthetic data."
        )


# ---------------------------------------------------------------------------
# Test 7: No hardcoded absolute paths
# ---------------------------------------------------------------------------

ABSOLUTE_PATH_PATTERNS = [
    r"/Users/",
    r"/home/",
    r"C:\\\\Users\\\\",
    r"C:/Users/",
]


def test_no_hardcoded_absolute_paths():
    nb = _load_notebook()
    code = _all_code_source(nb)
    for pat in ABSOLUTE_PATH_PATTERNS:
        assert not re.search(pat, code), (
            f"Notebook contains a hardcoded absolute path: {pat!r}. "
            "Use repository-relative paths via Path.cwd() or ROOT detection."
        )


# ---------------------------------------------------------------------------
# Test 8: No writes to outputs/ (except reads are fine; writes to diagnostics/ only)
# ---------------------------------------------------------------------------

WRITE_TO_OUTPUTS_PATTERNS = [
    r"\.to_csv\(['\"].*outputs/(?!diagnostics/)",
    r"open\(['\"].*outputs/(?!diagnostics/).*['\"],\s*['\"]w",
]


def test_no_writes_to_non_diagnostics_outputs():
    nb = _load_notebook()
    code = _all_code_source(nb)
    for pat in WRITE_TO_OUTPUTS_PATTERNS:
        assert not re.search(pat, code), (
            f"Notebook writes to outputs/ in a non-diagnostics location: {pat!r}. "
            "This notebook must be read-only with respect to production outputs."
        )


# ---------------------------------------------------------------------------
# Test 9: selected_m appears in notebook
# ---------------------------------------------------------------------------


def test_selected_m_appears_in_notebook():
    nb = _load_notebook()
    full_text = _all_code_source(nb) + _all_markdown_source(nb)
    assert "selected_m" in full_text, (
        "The string 'selected_m' was not found in the notebook. "
        "Plots and labels should use selected_m rather than a hardcoded m=7."
    )


# ---------------------------------------------------------------------------
# Test 10: Evidence-status handling
# ---------------------------------------------------------------------------

EVIDENCE_STATUS_PATTERNS = [
    r"evidence_status",
    r"diagnostic_evidence_status",
]


def test_evidence_status_handling_exists():
    nb = _load_notebook()
    full_text = _all_code_source(nb) + _all_markdown_source(nb)
    found = any(re.search(pat, full_text) for pat in EVIDENCE_STATUS_PATTERNS)
    assert found, (
        "Notebook does not reference 'evidence_status' or 'diagnostic_evidence_status'. "
        "Downstream consumers rely on these fields to assess diagnostic reliability."
    )


# ---------------------------------------------------------------------------
# Test 11: Insufficient categories skipped gracefully
# ---------------------------------------------------------------------------

GRACEFUL_SKIP_PATTERNS = [
    r"no report currently meets",
    r"unavailable",
    r"no data available",
    r"not available",
    r"skipping",
    r"skip",
]


def test_insufficient_categories_skipped_gracefully():
    nb = _load_notebook()
    full_text = _all_code_source(nb).lower() + _all_markdown_source(nb).lower()
    found = any(re.search(pat, full_text) for pat in GRACEFUL_SKIP_PATTERNS)
    assert found, (
        "Notebook does not appear to handle missing/unavailable diagnostic categories "
        "gracefully. Add guard clauses that print a message and skip when data is absent."
    )


# ---------------------------------------------------------------------------
# Test 12: Python syntax validity for all code cells
# ---------------------------------------------------------------------------


def test_all_code_cells_have_valid_python_syntax():
    nb = _load_notebook()
    errors = []
    for i, cell in enumerate(_code_cells(nb)):
        src = _cell_source(cell)
        if not src.strip():
            continue
        try:
            compile(src, filename=f"<cell {i}>", mode="exec")
        except SyntaxError as exc:
            errors.append(f"Cell {i}: {exc}")
    assert not errors, (
        f"{len(errors)} code cell(s) have syntax errors:\n"
        + "\n".join(errors)
    )


# ---------------------------------------------------------------------------
# Test 13: No imports of non-standard packages beyond requirements.txt
# ---------------------------------------------------------------------------

REQUIREMENTS_PATH = REPO_ROOT / "requirements.txt"

ALLOWED_EXTRA_PACKAGES = {
    # Standard library modules commonly used in notebooks
    "pathlib", "json", "re", "os", "sys", "warnings", "ast", "itertools",
    "collections", "functools", "math", "datetime", "typing", "abc",
    "contextlib", "copy", "inspect", "io", "time", "uuid",
}


def _get_requirements_packages():
    if not REQUIREMENTS_PATH.exists():
        return set()
    pkgs = set()
    for line in REQUIREMENTS_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Strip version specifiers
        pkg = re.split(r"[>=<!;\[@ ]", line)[0].lower().replace("-", "_")
        pkgs.add(pkg)
    return pkgs


def _extract_imported_packages(code_src):
    """Return top-level package names imported in Python source."""
    packages = set()
    try:
        tree = ast.parse(code_src)
    except SyntaxError:
        return packages
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                packages.add(alias.name.split(".")[0].lower())
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                packages.add(node.module.split(".")[0].lower())
    return packages


def test_no_imports_beyond_requirements():
    nb = _load_notebook()
    code = _all_code_source(nb)
    imported = _extract_imported_packages(code)
    required = _get_requirements_packages()
    allowed = required | ALLOWED_EXTRA_PACKAGES | {"src"}  # src/ is the local package

    unknown = imported - allowed
    # Filter out common aliases that map to allowed packages (e.g. 'np' -> numpy)
    # Only flag truly unknown top-level packages
    assert not unknown, (
        f"Notebook imports packages not found in requirements.txt: {unknown}. "
        f"Add them to requirements.txt or remove the import."
    )
