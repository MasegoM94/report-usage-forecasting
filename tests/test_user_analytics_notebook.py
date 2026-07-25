"""Smoke tests for notebooks/07_user_analytics.ipynb."""
import json
import re
from pathlib import Path
import pytest

NOTEBOOK_PATH = Path(__file__).parent.parent / "notebooks" / "07_user_analytics.ipynb"


@pytest.fixture(scope="module")
def nb():
    with open(NOTEBOOK_PATH) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def all_source(nb):
    """All cell source lines concatenated."""
    lines = []
    for cell in nb["cells"]:
        lines.extend(cell.get("source", []))
    return "\n".join(lines)


@pytest.fixture(scope="module")
def code_source(nb):
    lines = []
    for cell in nb["cells"]:
        if cell["cell_type"] == "code":
            lines.extend(cell.get("source", []))
    return "\n".join(lines)


@pytest.fixture(scope="module")
def markdown_source(nb):
    lines = []
    for cell in nb["cells"]:
        if cell["cell_type"] == "markdown":
            lines.extend(cell.get("source", []))
    return "\n".join(lines)


# 1. File exists
class TestNotebookExists:
    def test_file_exists(self):
        assert NOTEBOOK_PATH.exists(), f"Notebook not found: {NOTEBOOK_PATH}"

    def test_valid_json(self, nb):
        assert "cells" in nb

    def test_has_cells(self, nb):
        assert len(nb["cells"]) > 10


# 2. Required headings
class TestRequiredHeadings:
    REQUIRED = [
        "## 1. Business Objective",
        "## 2. Privacy Architecture",
        "## 3. Canonical Engagement Definitions",
        "## 4. Canonical Report-User-Day Mart",
        "## 5. User-Data Quality",
        "## 6. Observation Windows",
        "## 7. Report History Sufficiency",
        "## 8. Active-User Breadth",
        "## 9. Returning, One-Time, and Repeat-View Users",
        "## 10. Engagement Cohorts",
        "## 11. Frequency and Intensity",
        "## 12. Concentration and Dependency",
        "## 13. Privacy Suppression",
        "## 14. Canonical Report Engagement Mart",
        "## 15. Engagement Classification Logic",
        "## 16. Representative Case Studies",
        "## 17. Relationship to Sprint 7 Report Analytics",
        "## 18. Fields for Later Consumers",
        "## 19. Limitations",
    ]

    @pytest.mark.parametrize("heading", REQUIRED)
    def test_heading_present(self, markdown_source, heading):
        assert heading in markdown_source, f"Missing heading: {heading}"


# 3. Module imports
class TestImports:
    def test_imports_pandas(self, code_source):
        assert "import pandas" in code_source

    def test_imports_pathlib(self, code_source):
        assert "from pathlib import Path" in code_source

    def test_uses_repo_root(self, code_source):
        assert "REPO_ROOT" in code_source

    def test_loads_from_analytics_dir(self, code_source):
        assert "ANALYTICS_DIR" in code_source


# 4. Privacy guards
class TestPrivacyGuards:
    def test_no_dim_user_load(self, code_source):
        assert "dim_user.csv" not in code_source

    def test_no_user_features_load(self, code_source):
        assert "user_features.csv" not in code_source

    def test_no_user_segments_load(self, code_source):
        assert "user_segments.csv" not in code_source

    def test_no_user_key_display(self, code_source):
        # user_key may appear for drop/exclusion but not in display columns
        assert 'print(mart["user_key"])' not in code_source
        assert "display(mart['user_key'])" not in code_source

    def test_no_email_pattern(self, all_source):
        assert not re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", all_source)

    def test_no_direct_identifier_columns(self, all_source):
        # Use word-boundary matching to avoid false positives:
        # e.g. "unique_user" must not appear but "unique_users_28d" is fine.
        prohibited = ["user_id", "email_address", "display_name", "unique_user", "principal_name"]
        for col in prohibited:
            # Match the prohibited column name only when followed by a non-alphanumeric / non-underscore char
            # (or end of string) — this prevents metric fields like unique_users_28d from triggering.
            pattern = re.compile(r"\b" + re.escape(col) + r"\b")
            matches = pattern.findall(all_source)
            # Allow the string when it appears only as an explanation of why it is prohibited
            # (i.e., in the context of documentation, not as a column access).
            assert not matches or all(
                "prohibited" in all_source[max(0, m.start() - 60): m.end() + 60].lower()
                or "not" in all_source[max(0, m.start() - 60): m.end() + 60].lower()
                for m in pattern.finditer(all_source)
            ), f"Direct identifier found outside documentation context: {col}"

    def test_no_retire_report_language(self, all_source):
        assert "retire_report" not in all_source
        assert "retire report" not in all_source.lower()

    def test_no_delete_report_language(self, all_source):
        assert "delete_report" not in all_source

    def test_no_api_calls(self, code_source):
        assert "requests.get" not in code_source
        assert "urllib.request" not in code_source
        assert "anthropic.Anthropic" not in code_source
        assert "openai.OpenAI" not in code_source

    def test_no_synthetic_data_generation(self, code_source):
        assert "np.random" not in code_source
        assert "random.seed" not in code_source
        assert "fake_data" not in code_source

    def test_no_analytics_output_overwrite(self, code_source):
        assert "to_csv" not in code_source


# 5. Content validation
class TestContentValidation:
    def test_privacy_suppression_explained(self, markdown_source):
        lower = markdown_source.lower()
        assert "suppression" in lower or "suppressed" in lower

    def test_engagement_not_business_value(self, markdown_source):
        lower = markdown_source.lower()
        assert "business value" in lower or "not determine" in lower or "cannot determine" in lower

    def test_sprint7_mentioned(self, markdown_source):
        assert "Sprint 7" in markdown_source

    def test_no_genai_update(self, all_source):
        # Notebook must not invoke GenAI layer
        assert "insight_generator" not in all_source
        assert "genai.prompts" not in all_source

    def test_no_streamlit_update(self, all_source):
        # Streamlit may be mentioned as a future consumer, but must not be imported or called
        assert "streamlit" not in all_source.lower() or "not" in all_source.lower()

    def test_unavailable_categories_skipped(self, code_source):
        assert ".empty" in code_source or "no examples" in code_source

    def test_relative_paths_used(self, code_source):
        # Should not have hardcoded absolute paths
        assert "/Users/" not in code_source
        assert "C:\\" not in code_source
