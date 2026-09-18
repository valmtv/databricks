"""
Air Quality Medallion Transformation Package
--------------------------------------------
Modular, testable, pure functions for Bronze -> Silver -> Gold transformations.
Decouples pure data manipulation from pipeline orchestration (DLT / Databricks runtime).
"""

from .cleansing import (
    clean_air_quality_records,
    validate_coordinate_bounds,
    cast_pollutant_metrics,
)
from .enrichment import (
    enrich_with_aqi_reference,
    classify_aqi_category,
    get_color_code_for_aqi,
)
from .quarantine import (
    evaluate_quarantine_conditions,
    split_valid_and_quarantined,
    QUARANTINE_RULES,
)
from .features import (
    calculate_surrogate_key,
    compute_who_pm25_exceedance,
    calculate_health_severity_score,
    add_hourly_surrogate_keys,
)
from .aggregations import (
    calculate_daily_aggregates,
    assign_compliance_grade,
    calculate_unhealthy_hours_percentage,
)
from .dimensions import (
    build_calendar_dimension_records,
    enrich_station_metadata,
)

__all__ = [
    "clean_air_quality_records",
    "validate_coordinate_bounds",
    "cast_pollutant_metrics",
    "enrich_with_aqi_reference",
    "classify_aqi_category",
    "get_color_code_for_aqi",
    "evaluate_quarantine_conditions",
    "split_valid_and_quarantined",
    "QUARANTINE_RULES",
    "calculate_surrogate_key",
    "compute_who_pm25_exceedance",
    "calculate_health_severity_score",
    "add_hourly_surrogate_keys",
    "calculate_daily_aggregates",
    "assign_compliance_grade",
    "calculate_unhealthy_hours_percentage",
    "build_calendar_dimension_records",
    "enrich_station_metadata",
]
