"""Smoke tests for notebooks/08_report_analytics.ipynb."""
import json
import re
from pathlib import Path
import pytest

NOTEBOOK_PATH = Path("notebooks/08_report_analytics.ipynb")


@pytest.fixture(scope="module")
def nb():
    assert NOTEBOOK_PATH.exists(), f"Notebook not found: {NOTEBOOK_PATH}"
    with open(NOTEBOOK_PATH) as f:
        return json.load(f)


@pytest.fixture(scope="module")
def all_source(nb):
    return "\n".join(
        "".join(c["source"]) for c in nb["cells"]
    )


@pytest.fixture(scope="module")
def md_source(nb):
    return "\n".join(
        "".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "markdown"
    )


@pytest.fixture(scope="module")
def code_source(nb):
    return "\n".join(
        "".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code"
    )


# --- Existence and structure ---

def test_notebook_exists():
    assert NOTEBOOK_PATH.exists()


def test_notebook_valid_json(nb):
    assert "cells" in nb
    assert len(nb["cells"]) > 0


def test_required_sections_present(md_source):
    required = [
        "Business Objective",
        "Architecture",
        "Historical Usage",
        "Forecast Outlook",
        "Model Health",
        "Engagement",
        "Metadata",
        "Diagnostics",
        "Segmentation",
        "Canonical Report Analytics Mart",
        "Case Studies",
        "Evidence",
        "Action Policy",
        "Limitations",
    ]
    for heading in required:
        assert heading in md_source, f"Missing section: {heading}"


# --- Data loading ---

def test_loads_report_features(code_source):
    assert "report_features.csv" in code_source


def test_loads_forecast_outlook(code_source):
    assert "report_forecast_outlook.csv" in code_source


def test_loads_model_health(code_source):
    assert "report_model_health_context.csv" in code_source


def test_loads_engagement(code_source):
    assert "report_engagement_context.csv" in code_source


def test_loads_metadata(code_source):
    assert "report_metadata_context.csv" in code_source


def test_loads_diagnostics(code_source):
    assert "report_diagnostics.csv" in code_source


def test_loads_segments(code_source):
    assert "report_segments.csv" in code_source


def test_loads_mart(code_source):
    assert "mart_report_analytics.csv" in code_source


# --- Reusable modules imported ---

def test_imports_diagnostics_module(code_source):
    assert "src.analytics.report_diagnostics" in code_source or "report_diagnostics" in code_source


def test_imports_mart_module(code_source):
    assert "src.analytics.report_analytics_mart" in code_source or "report_analytics_mart" in code_source


# --- Repository-relative paths ---

def test_uses_analytics_dir(code_source):
    assert "ANALYTICS_DIR" in code_source or "outputs/analytics" in code_source


def test_uses_metrics_dir(code_source):
    assert "METRICS_DIR" in code_source or "outputs/metrics" in code_source


def test_no_absolute_paths(code_source):
    assert "/Users/" not in code_source


# --- No prohibited content ---

def test_no_llm_calls(code_source):
    llm_patterns = ["openai", "anthropic", "ChatCompletion", "claude.complete", "llm.generate"]
    for pat in llm_patterns:
        assert pat not in code_source, f"LLM call found: {pat}"


def test_no_user_level_files(code_source):
    forbidden = ["user_features.csv", "user_segments.csv", "dim_user.csv"]
    for f in forbidden:
        assert f not in code_source, f"User-level file loaded: {f}"


def test_no_user_identifiers_displayed(code_source):
    assert "user_key" not in code_source


def test_no_to_csv_calls(code_source):
    assert ".to_csv(" not in code_source


def test_no_retirement_recommendation(all_source):
    assert "retire_report" not in all_source
    assert "delete_report" not in all_source


def test_no_automatic_retraining(all_source):
    assert "automatically_retrain" not in all_source


def test_no_synthetic_fallback(code_source):
    assert "pd.DataFrame({'report_id': range" not in code_source
    assert "np.random" not in code_source


# --- Business logic not duplicated ---

def test_no_inline_anomaly_formula(code_source):
    assert "IQR" not in code_source and "iqr" not in code_source.lower()


# --- Case study graceful skip ---

def test_case_studies_use_skip_guard(code_source):
    assert "skipped" in code_source or "skip" in code_source.lower()
    assert "_first_match" in code_source or "available_cases" in code_source


# --- Key concepts documented ---

def test_documents_null_not_zero(md_source):
    assert "null" in md_source.lower() or "null" in md_source


def test_documents_privacy_suppression(md_source):
    assert "privacy" in md_source.lower() and "suppressed" in md_source.lower()


def test_documents_no_inference_from_usage(md_source):
    assert "inferred" in md_source.lower() or "infer" in md_source.lower()


def test_documents_sprint8_relationship(md_source):
    assert "Sprint 8" in md_source or "sprint 8" in md_source.lower()
