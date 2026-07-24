"""
User analytics privacy policy for this repository.

Prohibited output fields (must never appear in analytics outputs):
- user_id  (contains raw email addresses, e.g. user001@masegoinc.com)
- email
- email_address
- user_name
- username
- display_name
- unique_user  (contains display name "User NNN" which is reversible via dim_user)
- principal_name
- directory_id

Allowed identifiers in analytics outputs:
- user_key  (surrogate key, e.g. UK_0001 — stable, pseudonymous, not directly identifiable)

Restricted identity layer:
- data/processed/dim_user.csv maps user_key -> user_id (email) -> unique_user
- dim_user must NOT be joined into any public analytics output
- dim_user must NOT be loaded into Streamlit

This module defines:
- PROHIBITED_OUTPUT_COLUMNS: frozenset of column names never allowed in analytics outputs
- ALLOWED_USER_IDENTIFIER: the single approved user-level key
- EMAIL_PATTERN: regex for detecting email-like values in any column
- PrivacyConfig: dataclass with suppression thresholds
"""

import re
import warnings
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd

PROHIBITED_OUTPUT_COLUMNS: frozenset = frozenset({
    "user_id",
    "email",
    "email_address",
    "user_name",
    "username",
    "display_name",
    "unique_user",
    "principal_name",
    "directory_id",
})

ALLOWED_USER_IDENTIFIER: str = "user_key"

EMAIL_PATTERN: re.Pattern = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class PrivacyConfig:
    MIN_USERS_FOR_DISTRIBUTION_METRICS: int = 5
    MIN_USERS_FOR_COHORT_BREAKDOWN: int = 5
    SUPPRESS_SMALL_GROUPS: bool = True
    SUPPRESSED_SENTINEL: None = None   # use None (null), never 0


PRIVACY_CONFIG = PrivacyConfig()


# ---------------------------------------------------------------------------
# Suppression utilities
# ---------------------------------------------------------------------------

def apply_small_group_suppression(
    df: pd.DataFrame,
    user_count_col: str,
    distribution_cols: List[str],
    config: PrivacyConfig = PRIVACY_CONFIG,
    suppression_reason: str = "unique_users_below_minimum",
) -> pd.DataFrame:
    """
    Set distribution-sensitive columns to None where user count < MIN_USERS_FOR_DISTRIBUTION_METRICS.

    Does NOT suppress: total_views, unique_users count itself.
    Does NOT replace with 0 — uses None (null).
    Adds: privacy_suppressed (bool), privacy_suppression_reason (str or None), suppressed_fields (str or None).
    Does NOT add user lists.
    """
    df = df.copy()
    below_threshold = df[user_count_col].lt(config.MIN_USERS_FOR_DISTRIBUTION_METRICS)

    # Add suppression metadata columns
    df["privacy_suppressed"] = below_threshold
    df["privacy_suppression_reason"] = None
    df["suppressed_fields"] = None

    if below_threshold.any():
        suppress_mask = below_threshold
        for col in distribution_cols:
            if col in df.columns:
                df.loc[suppress_mask, col] = None
        df.loc[suppress_mask, "privacy_suppression_reason"] = suppression_reason
        df.loc[suppress_mask, "suppressed_fields"] = ",".join(
            [c for c in distribution_cols if c in df.columns]
        )

    return df


# ---------------------------------------------------------------------------
# Privacy validation functions
# ---------------------------------------------------------------------------

def validate_no_direct_identifiers(
    df: pd.DataFrame,
    prohibited_columns: frozenset = PROHIBITED_OUTPUT_COLUMNS,
    context: str = "output",
) -> None:
    """
    Raise ValueError if df contains any prohibited column names.
    Also scans string/object columns for email-like content.
    """
    found_cols = prohibited_columns & set(df.columns)
    if found_cols:
        raise ValueError(
            f"Privacy violation in {context}: prohibited columns present: {sorted(found_cols)}"
        )
    detect_email_like_values(df, context=context)


def detect_email_like_values(
    df: pd.DataFrame,
    context: str = "output",
    sample_size: int = 100,
) -> None:
    """
    Scan object/string columns for email-like patterns.
    Raises ValueError if any are found.
    Skips null values (null does not produce false positive).
    """
    for col in df.select_dtypes(include=["object", "string"]).columns:
        sample = df[col].dropna().head(sample_size)
        for val in sample:
            if isinstance(val, str) and EMAIL_PATTERN.search(val):
                raise ValueError(
                    f"Privacy violation in {context}: column '{col}' contains email-like value: '{val[:30]}...'"
                )


def validate_user_analytics_output(
    df: pd.DataFrame,
    expected_user_key_col: str = "user_key",
    context: str = "user_analytics_output",
) -> None:
    """
    Full privacy check for user-level analytics output DataFrames.
    1. No prohibited columns.
    2. No email-like values.
    3. user_key present (if expected).
    4. No user lists (no list/array-type columns).
    """
    validate_no_direct_identifiers(df, context=context)
    if expected_user_key_col and expected_user_key_col not in df.columns:
        raise ValueError(
            f"Privacy validation in {context}: expected column '{expected_user_key_col}' not found."
        )
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, set, tuple))).any():
            raise ValueError(
                f"Privacy violation in {context}: column '{col}' contains user lists."
            )


def validate_privacy_suppression(
    df: pd.DataFrame,
    suppressed_col: str = "privacy_suppressed",
    suppressed_fields_col: str = "suppressed_fields",
    distribution_cols: Optional[List[str]] = None,
    config: PrivacyConfig = PRIVACY_CONFIG,
    context: str = "suppression_check",
) -> None:
    """
    Validate that suppression was applied correctly:
    1. Suppressed rows have None (not 0) for all distribution_cols.
    2. Non-suppressed rows have suppressed_fields = None.
    3. No user lists in suppressed_fields.
    """
    if suppressed_col not in df.columns:
        raise ValueError(f"Missing '{suppressed_col}' column in {context}")
    suppressed = df[df[suppressed_col] == True]
    if distribution_cols and not suppressed.empty:
        for col in distribution_cols:
            if col in df.columns:
                bad = suppressed[col].notna()
                if bad.any():
                    raise ValueError(
                        f"Suppression error in {context}: column '{col}' has non-null values in suppressed rows."
                    )
    not_suppressed = df[df[suppressed_col] == False]
    if suppressed_fields_col in df.columns and not not_suppressed.empty:
        bad = not_suppressed[suppressed_fields_col].notna()
        if bad.any():
            raise ValueError(
                f"Suppression error in {context}: '{suppressed_fields_col}' should be None for non-suppressed rows."
            )
