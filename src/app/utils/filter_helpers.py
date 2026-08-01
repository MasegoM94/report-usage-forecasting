"""Pure-logic filter helpers for the Streamlit reviewer app sidebar.

These functions contain no Streamlit calls and can be tested independently.
All inputs and outputs are plain Python types (dicts, DataFrames, lists).

Attention-only logic
--------------------
"Requires attention" is defined as:
  - overall_review_priority in {"high", "critical"}  OR
  - recommended_report_action != "continue_monitoring"

This mapping is deterministic and based solely on upstream mart fields.
No score is computed in Streamlit; the mart values are used as-is.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

try:
    from utils.definitions import STATUS_LABELS, status_label
except ModuleNotFoundError:
    from src.app.utils.definitions import STATUS_LABELS, status_label  # type: ignore[no-redef]


# ---------------------------------------------------------------------------
# Filter field catalogue
# ---------------------------------------------------------------------------

# Ordered list of (mart_field, display_label) for the sidebar filter UI.
# Fields are shown only when they have at least 2 distinct non-null values.
FILTERABLE_FIELDS: list[tuple[str, str]] = [
    ("report_category",            "Report category"),
    ("historical_usage_status",    "Historical usage"),
    ("forecast_outlook_status",    "Forecast outlook"),
    ("overall_engagement_status",  "Engagement status"),
    ("model_diagnostic_status",    "Model health"),
    ("overall_report_status",      "Overall status"),
    ("overall_review_priority",    "Review priority"),
    ("recommended_report_action",  "Recommended action"),
    ("overall_evidence_status",    "Evidence status"),
    ("privacy_suppression_status", "Privacy suppression"),
    # Shown only when workspace / category metadata is populated:
    ("workspace_name",             "Workspace"),
    ("forecast_interpretation_status", "Forecast interpretation"),
]

FILTERABLE_FIELD_LABELS: dict[str, str] = dict(FILTERABLE_FIELDS)

# Fields that define "requires attention".
ATTENTION_PRIORITY_VALUES: frozenset[str] = frozenset({"high", "critical"})
ATTENTION_ACTION_EXCLUDE: frozenset[str] = frozenset({"continue_monitoring"})

# Fields to search against in report search.
SEARCH_FIELDS: list[str] = ["report_name", "report_id", "workspace_name"]


# ---------------------------------------------------------------------------
# Filter option extraction
# ---------------------------------------------------------------------------

def extract_filter_options(mart: pd.DataFrame, field: str) -> list[tuple[str, str]]:
    """Return sorted (display_label, internal_value) pairs for a filter field.

    Excludes null values and values that produce an empty display label.
    Returns an empty list when the field is absent or has no usable values.
    """
    if mart.empty or field not in mart.columns:
        return []
    values = (
        mart[field]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    # Exclude bare "nan" strings that survived the above
    values = [v for v in values if v.lower() not in ("nan", "none", "")]
    options = [(status_label(v), v) for v in sorted(values)]
    return options


def check_filter_availability(mart: pd.DataFrame) -> dict[str, bool]:
    """Return field → bool indicating whether a filter is usable.

    A filter is usable when:
    1. The field exists in the mart.
    2. At least 2 distinct non-null values are present (single-value filters
       have no discriminating power).
    """
    result: dict[str, bool] = {}
    for field, _ in FILTERABLE_FIELDS:
        if mart.empty or field not in mart.columns:
            result[field] = False
            continue
        n_distinct = (
            mart[field].dropna().astype(str).str.strip()
            .replace("", pd.NA).dropna().nunique()
        )
        result[field] = n_distinct >= 2
    return result


# ---------------------------------------------------------------------------
# Filter application
# ---------------------------------------------------------------------------

def apply_filters(mart: pd.DataFrame, filters: dict[str, list[str]]) -> pd.DataFrame:
    """Apply a dict of {field: [value, ...]} filters to the mart (AND logic).

    Returns the filtered mart.  Fields not present in the mart are silently
    ignored.  Empty value lists are treated as "no filter on this field".
    The original mart is never modified.
    """
    if mart.empty:
        return mart
    result = mart
    for field, values in filters.items():
        if not values:
            continue
        if field not in result.columns:
            continue
        result = result[result[field].isin(values)]
    return result


def apply_attention_filter(mart: pd.DataFrame) -> pd.DataFrame:
    """Return rows that 'require attention'.

    Attention = review_priority in {high, critical}
             OR recommended_report_action not in {continue_monitoring}.

    If both columns are absent, the mart is returned unchanged.
    """
    if mart.empty:
        return mart

    mask = pd.Series(False, index=mart.index)

    if "overall_review_priority" in mart.columns:
        mask = mask | mart["overall_review_priority"].isin(ATTENTION_PRIORITY_VALUES)

    if "recommended_report_action" in mart.columns:
        mask = mask | (~mart["recommended_report_action"].isin(ATTENTION_ACTION_EXCLUDE))

    return mart[mask]


# ---------------------------------------------------------------------------
# Report search
# ---------------------------------------------------------------------------

def search_reports(
    reports_df: pd.DataFrame,
    query: str,
    search_fields: list[str] | None = None,
) -> pd.DataFrame:
    """Return rows of *reports_df* that match *query* (case-insensitive).

    Matches against all columns listed in *search_fields* (default:
    SEARCH_FIELDS).  A row matches when any of its search-field values
    contains *query* as a substring.

    - Whitespace is stripped from *query* before matching.
    - Missing search-field columns are silently skipped.
    - Returns an empty DataFrame (preserving columns) when no rows match.
    - Returns *reports_df* unchanged when *query* is blank.
    """
    q = query.strip().lower()
    if not q:
        return reports_df

    if search_fields is None:
        search_fields = SEARCH_FIELDS

    if reports_df.empty:
        return reports_df

    mask = pd.Series(False, index=reports_df.index)
    for field in search_fields:
        if field not in reports_df.columns:
            continue
        col_str = reports_df[field].fillna("").astype(str).str.lower()
        mask = mask | col_str.str.contains(q, regex=False)

    return reports_df[mask]


# ---------------------------------------------------------------------------
# Active filter summary
# ---------------------------------------------------------------------------

def active_filter_summary(
    filters: dict[str, list[str]],
    search_query: str = "",
    attention_only: bool = False,
    label_fn: Any = None,
) -> list[str]:
    """Return a list of human-readable descriptions of currently active filters.

    *label_fn* maps internal values to display strings (defaults to
    status_label from definitions).
    """
    if label_fn is None:
        label_fn = status_label

    parts: list[str] = []

    if search_query.strip():
        parts.append(f"Search: \"{search_query.strip()}\"")

    if attention_only:
        parts.append(
            "Attention only (review priority high/critical or action ≠ continue monitoring)"
        )

    for field, values in filters.items():
        if not values:
            continue
        field_label = FILTERABLE_FIELD_LABELS.get(field, field.replace("_", " ").title())
        val_labels = [label_fn(v) for v in values]
        parts.append(f"{field_label}: {', '.join(val_labels)}")

    return parts


# ---------------------------------------------------------------------------
# Session-state helpers (pure logic — no st.session_state calls)
# ---------------------------------------------------------------------------

def safe_session_report(
    selectable_ids: list[str],
    current_id: str | None,
) -> str | None:
    """Return *current_id* if it is still selectable, otherwise first available.

    Returns None when *selectable_ids* is empty.
    """
    if not selectable_ids:
        return None
    if current_id and current_id in selectable_ids:
        return current_id
    return selectable_ids[0]


def default_filter_state() -> dict[str, Any]:
    """Return the default (reset) filter state as a plain dict.

    This is the pure-logic counterpart to clearing Streamlit session state.
    Keys returned:
        search_query   — empty string
        active_filters — empty dict
        attention_only — False
    """
    return {
        "search_query":   "",
        "active_filters": {},
        "attention_only": False,
    }
