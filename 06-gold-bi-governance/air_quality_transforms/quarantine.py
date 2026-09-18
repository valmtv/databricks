"""
Quarantine & Data Quality Routing Transformations
-------------------------------------------------
Implements the Enterprise Quarantine Table Pattern.
Prevents silent data loss by tagging and routing non-compliant records
into a dedicated quarantine table for engineering review and replay.
"""

from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, timezone


QUARANTINE_RULES = {
    "MISSING_MANDATORY_KEYS": {
        "description": "Primary identifier (event_id, station_id, or city) is null or empty",
        "sql_condition": "event_id IS NULL OR trim(event_id) = '' OR station_id IS NULL OR trim(station_id) = '' OR city IS NULL OR trim(city) = ''"
    },
    "INVALID_RECORDED_TIMESTAMP": {
        "description": "Timestamp is missing, unparseable, or future-dated",
        "sql_condition": "recorded_at IS NULL OR recorded_at > current_timestamp()"
    },
    "OUT_OF_BOUNDS_COORDINATES": {
        "description": "Latitude or longitude outside physical Earth boundaries [-90..90, -180..180]",
        "sql_condition": "latitude < -90.0 OR latitude > 90.0 OR longitude < -180.0 OR longitude > 180.0"
    },
    "NEGATIVE_POLLUTANT_VALUE": {
        "description": "Pollutant concentration cannot be physically negative",
        "sql_condition": "(pm2_5 IS NOT NULL AND pm2_5 < 0.0) OR (pm10 IS NOT NULL AND pm10 < 0.0)"
    },
    "OUT_OF_RANGE_AQI": {
        "description": "US EPA AQI index must be within 0 to 500",
        "sql_condition": "us_aqi IS NOT NULL AND (us_aqi < 0 OR us_aqi > 500)"
    }
}


def evaluate_quarantine_conditions(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Pure Python validator for a single record.
    Returns:
        (is_quarantined: bool, reasons: List[str])
    """
    reasons = []

    # Rule 1: Mandatory keys
    event_id = record.get("event_id")
    station_id = record.get("station_id")
    city = record.get("city")

    if not event_id or str(event_id).strip() == "":
        reasons.append("MISSING_EVENT_ID")
    if not station_id or str(station_id).strip() == "":
        reasons.append("MISSING_STATION_ID")
    if not city or str(city).strip() == "":
        reasons.append("MISSING_CITY")

    # Rule 2: Timestamp
    rec_at = record.get("recorded_at")
    now_utc = datetime.now(timezone.utc)
    if rec_at is None:
        reasons.append("NULL_RECORDED_TIMESTAMP")
    elif isinstance(rec_at, datetime):
        rec_dt = rec_at if rec_at.tzinfo else rec_at.replace(tzinfo=timezone.utc)
        if rec_dt > now_utc:
            reasons.append("FUTURE_RECORDED_TIMESTAMP")
    elif isinstance(rec_at, str):
        try:
            dt = datetime.fromisoformat(rec_at.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt > now_utc:
                reasons.append("FUTURE_RECORDED_TIMESTAMP")
        except Exception:
            reasons.append("UNPARSEABLE_TIMESTAMP")

    # Rule 3: Coordinates
    lat = record.get("latitude")
    lon = record.get("longitude")
    if lat is not None:
        try:
            flat = float(lat)
            if flat < -90.0 or flat > 90.0:
                reasons.append("LATITUDE_OUT_OF_BOUNDS")
        except (ValueError, TypeError):
            reasons.append("INVALID_LATITUDE_TYPE")

    if lon is not None:
        try:
            flon = float(lon)
            if flon < -180.0 or flon > 180.0:
                reasons.append("LONGITUDE_OUT_OF_BOUNDS")
        except (ValueError, TypeError):
            reasons.append("INVALID_LONGITUDE_TYPE")

    # Rule 4: Negative pollutants
    pm2_5 = record.get("pm2_5")
    if pm2_5 is not None:
        try:
            if float(pm2_5) < 0.0:
                reasons.append("NEGATIVE_PM25")
        except (ValueError, TypeError):
            reasons.append("INVALID_PM25_TYPE")

    pm10 = record.get("pm10")
    if pm10 is not None:
        try:
            if float(pm10) < 0.0:
                reasons.append("NEGATIVE_PM10")
        except (ValueError, TypeError):
            reasons.append("INVALID_PM10_TYPE")

    # Rule 5: AQI range
    aqi = record.get("us_aqi")
    if aqi is not None:
        try:
            iaqi = int(aqi)
            if iaqi < 0 or iaqi > 500:
                reasons.append("AQI_OUT_OF_BOUNDS")
        except (ValueError, TypeError):
            reasons.append("INVALID_AQI_TYPE")

    is_quarantined = len(reasons) > 0
    return is_quarantined, reasons


def split_valid_and_quarantined(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Splits a batch of dictionary records into valid and quarantined lists.
    Quarantined records receive additional metadata fields:
    - `quarantine_reason`: pipe-separated list of failed check codes
    - `quarantined_at`: UTC timestamp of quarantine action
    """
    valid = []
    quarantined = []

    for r in records:
        is_quarantine, reasons = evaluate_quarantine_conditions(r)
        if is_quarantine:
            qr = dict(r)
            qr["quarantine_reason"] = " | ".join(reasons)
            qr["quarantined_at"] = datetime.now(timezone.utc).isoformat()
            quarantined.append(qr)
        else:
            valid.append(dict(r))

    return valid, quarantined


def build_quarantine_predicate_spark():
    """
    Returns a PySpark Column expression that evaluates to TRUE if a record violates
    any DQ rule and must be routed to quarantine.
    """
    from pyspark.sql import functions as F

    return (
        # Missing keys
        F.col("event_id").isNull() | (F.trim(F.col("event_id")) == "") |
        F.col("station_id").isNull() | (F.trim(F.col("station_id")) == "") |
        F.col("city").isNull() | (F.trim(F.col("city")) == "") |
        
        # Invalid timestamps
        F.col("recorded_at").isNull() | (F.col("recorded_at") > F.current_timestamp()) |
        
        # Coordinate bounds
        (F.col("latitude").isNotNull() & ((F.col("latitude") < -90.0) | (F.col("latitude") > 90.0))) |
        (F.col("longitude").isNotNull() & ((F.col("longitude") < -180.0) | (F.col("longitude") > 180.0))) |
        
        # Negative pollutant values
        (F.col("pm2_5").isNotNull() & (F.col("pm2_5") < 0.0)) |
        (F.col("pm10").isNotNull() & (F.col("pm10") < 0.0)) |
        
        # Out-of-range AQI
        (F.col("us_aqi").isNotNull() & ((F.col("us_aqi") < 0) | (F.col("us_aqi") > 500)))
    )
