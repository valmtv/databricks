"""
Cleansing & Schema Enforcement Transformations
-----------------------------------------------
Pure transformation functions for cleansing raw sensor telemetry,
enforcing physical boundaries, and casting types.
"""

from typing import Dict, Any, Optional
from datetime import datetime, timezone


def validate_coordinate_bounds(latitude: Optional[float], longitude: Optional[float]) -> bool:
    """
    Validates that geographic coordinates fall within physical Earth boundaries.
    Latitude: [-90.0, 90.0]
    Longitude: [-180.0, 180.0]
    """
    if latitude is None or longitude is None:
        return False
    try:
        lat = float(latitude)
        lon = float(longitude)
        return (-90.0 <= lat <= 90.0) and (-180.0 <= lon <= 180.0)
    except (ValueError, TypeError):
        return False


def validate_pollutant_ranges(
    pm2_5: Optional[float],
    pm10: Optional[float],
    us_aqi: Optional[int]
) -> bool:
    """
    Validates physical boundaries for pollutant concentrations and US AQI scale:
    - PM2.5: >= 0.0 (or None/null if unmetered)
    - PM10: >= 0.0 (or None/null if unmetered)
    - US AQI: [0, 500] (standard EPA scale)
    """
    if pm2_5 is not None:
        try:
            if float(pm2_5) < 0.0:
                return False
        except (ValueError, TypeError):
            return False

    if pm10 is not None:
        try:
            if float(pm10) < 0.0:
                return False
        except (ValueError, TypeError):
            return False

    if us_aqi is not None:
        try:
            aqi = int(us_aqi)
            if aqi < 0 or aqi > 500:
                return False
        except (ValueError, TypeError):
            return False

    return True


def validate_timestamp_freshness(
    recorded_at: Any,
    max_future_seconds: int = 300,
    reference_time: Optional[datetime] = None
) -> bool:
    """
    Ensures observations are not future-dated beyond an acceptable clock skew tolerance (default 5 min).
    """
    if recorded_at is None:
        return False

    ref_now = reference_time or datetime.now(timezone.utc)
    if ref_now.tzinfo is None:
        ref_now = ref_now.replace(tzinfo=timezone.utc)

    if isinstance(recorded_at, str):
        try:
            cleaned_str = recorded_at.replace("Z", "+00:00")
            dt = datetime.fromisoformat(cleaned_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            return False
    elif isinstance(recorded_at, datetime):
        dt = recorded_at if recorded_at.tzinfo else recorded_at.replace(tzinfo=timezone.utc)
    else:
        return False

    delta_seconds = (dt - ref_now).total_seconds()
    return delta_seconds <= max_future_seconds


def clean_air_quality_records(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pure Python record transformer: parses timestamps, casts numeric metrics,
    and strips whitespace from categorical keys.
    """
    cleaned = dict(record)
    
    # Normalize strings
    for k in ("event_id", "station_id", "city", "country"):
        if k in cleaned and cleaned[k] is not None:
            cleaned[k] = str(cleaned[k]).strip()

    # Safely cast numerics
    float_fields = ["latitude", "longitude", "pm2_5", "pm10", "carbon_monoxide",
                    "nitrogen_dioxide", "sulphur_dioxide", "ozone"]
    for field in float_fields:
        if field in cleaned and cleaned[field] is not None:
            try:
                cleaned[field] = float(cleaned[field])
            except (ValueError, TypeError):
                cleaned[field] = None

    if "us_aqi" in cleaned and cleaned["us_aqi"] is not None:
        try:
            cleaned["us_aqi"] = int(cleaned["us_aqi"])
        except (ValueError, TypeError):
            cleaned["us_aqi"] = None

    return cleaned


def cast_pollutant_metrics(record: Dict[str, Any]) -> Dict[str, Optional[float]]:
    """Extracts and casts pollutant metrics specifically."""
    pollutants = ["pm2_5", "pm10", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone"]
    result = {}
    for p in pollutants:
        val = record.get(p)
        try:
            result[p] = float(val) if val is not None else None
        except (ValueError, TypeError):
            result[p] = None
    return result


def clean_silver_air_quality_df(df_raw):
    """
    PySpark DataFrame transformer:
    Casts all columns to their target Silver schema types and selects standard columns.
    """
    from pyspark.sql import functions as F

    return (
        df_raw
        .withColumn("recorded_at", F.to_timestamp(F.col("timestamp")))
        .withColumn("pm2_5", F.col("pm2_5").cast("double"))
        .withColumn("pm10", F.col("pm10").cast("double"))
        .withColumn("carbon_monoxide", F.col("carbon_monoxide").cast("double"))
        .withColumn("nitrogen_dioxide", F.col("nitrogen_dioxide").cast("double"))
        .withColumn("sulphur_dioxide", F.col("sulphur_dioxide").cast("double"))
        .withColumn("ozone", F.col("ozone").cast("double"))
        .withColumn("us_aqi", F.col("us_aqi").cast("integer"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .select(
            "event_id",
            "station_id",
            "city",
            "country",
            "latitude",
            "longitude",
            "recorded_at",
            "pm2_5",
            "pm10",
            "carbon_monoxide",
            "nitrogen_dioxide",
            "sulphur_dioxide",
            "ozone",
            "us_aqi",
            "_ingestion_timestamp"
        )
    )
