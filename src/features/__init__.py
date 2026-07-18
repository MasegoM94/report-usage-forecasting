"""Feature engineering modules."""

from src.features.build_forecast_features import (
    build_forecast_feature_table,
    build_report_daily_context,
    build_report_insight_context,
    validate_daily_context_schema,
    validate_insight_context_schema,
)
from src.features.engagement_features import build_user_engagement_features
from src.features.feature_registry import (
    FEATURE_REGISTRY,
    get_diagnostic_features,
    get_feature_registry,
    get_predictor_features,
)
from src.features.performance_features import build_report_performance_features
from src.features.report_features import (
    add_time_series_usage_features,
    build_report_daily_adoption,
    build_report_daily_series,
)
from src.features.validate_series import SeriesValidationError, validate_report_daily_series

__all__ = [
    "build_report_daily_series",
    "validate_report_daily_series",
    "SeriesValidationError",
    "build_report_daily_adoption",
    "add_time_series_usage_features",
    "build_user_engagement_features",
    "build_report_performance_features",
    "build_report_daily_context",
    "build_report_insight_context",
    "validate_daily_context_schema",
    "validate_insight_context_schema",
    "build_forecast_feature_table",
    "FEATURE_REGISTRY",
    "get_feature_registry",
    "get_predictor_features",
    "get_diagnostic_features",
]
