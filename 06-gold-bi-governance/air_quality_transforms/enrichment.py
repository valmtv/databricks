"""
Enrichment Transformations
--------------------------
EPA Air Quality Index category lookup, health advisory assignment,
and color coding.
"""

from typing import Tuple, Optional


EPA_AQI_TIERS = [
    (0, 50, "Good", "Green", "Air quality is satisfactory, and air pollution poses little or no risk."),
    (51, 100, "Moderate", "Yellow", "Air quality is acceptable. However, there may be a risk for some people."),
    (101, 150, "Unhealthy for Sensitive Groups", "Orange", "Members of sensitive groups may experience health effects."),
    (151, 200, "Unhealthy", "Red", "Some members of the general public may experience health effects."),
    (201, 300, "Very Unhealthy", "Purple", "Health alert: The risk of health effects is increased for everyone."),
    (301, 500, "Hazardous", "Maroon", "Health warning of emergency conditions: everyone is more likely to be affected.")
]


def classify_aqi_category(us_aqi: Optional[int]) -> str:
    """
    Classifies a numerical US AQI score into an EPA advisory category string.
    Returns 'Unknown' if the AQI is None, out of range, or unmetered.
    """
    if us_aqi is None:
        return "Unknown"
    
    try:
        score = int(us_aqi)
    except (ValueError, TypeError):
        return "Unknown"

    for low, high, category, _, _ in EPA_AQI_TIERS:
        if low <= score <= high:
            return category

    return "Unknown"


def get_color_code_for_aqi(us_aqi: Optional[int]) -> str:
    """
    Maps a US AQI value to its standardized EPA color code.
    Returns 'Gray' for unclassified or missing values.
    """
    if us_aqi is None:
        return "Gray"

    try:
        score = int(us_aqi)
    except (ValueError, TypeError):
        return "Gray"

    for low, high, _, color, _ in EPA_AQI_TIERS:
        if low <= score <= high:
            return color

    return "Gray"


def enrich_with_aqi_reference(df_cleaned, df_ref):
    """
    PySpark DataFrame transformer:
    Left joins cleansed observations with EPA reference table on AQI score intervals.
    Provides fallback defaults ('Unknown' / 'Gray') for unmatched records.
    """
    from pyspark.sql import functions as F

    return (
        df_cleaned.join(
            df_ref,
            (df_cleaned["us_aqi"] >= df_ref["aqi_min"]) & (df_cleaned["us_aqi"] <= df_ref["aqi_max"]),
            how="left"
        )
        .select(
            df_cleaned["event_id"],
            df_cleaned["station_id"],
            df_cleaned["city"],
            df_cleaned["country"],
            df_cleaned["latitude"],
            df_cleaned["longitude"],
            df_cleaned["recorded_at"],
            df_cleaned["pm2_5"],
            df_cleaned["pm10"],
            df_cleaned["carbon_monoxide"],
            df_cleaned["nitrogen_dioxide"],
            df_cleaned["sulphur_dioxide"],
            df_cleaned["ozone"],
            df_cleaned["us_aqi"],
            F.coalesce(df_ref["category"], F.lit("Unknown")).alias("aqi_category"),
            F.coalesce(df_ref["color_code"], F.lit("Gray")).alias("aqi_color_code"),
            df_ref["health_implication"],
            df_ref["cautionary_statement"],
            df_cleaned["_ingestion_timestamp"],
            F.current_timestamp().alias("_transformed_timestamp")
        )
    )
