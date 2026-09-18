"""
Feature Engineering & Surrogate Key Transformations
---------------------------------------------------
Calculates analytical surrogate keys, WHO health threshold flags,
rolling moving averages, and health severity scores.
"""

import hashlib
from typing import Optional, Any


def calculate_surrogate_key(*values: Any) -> int:
    """
    Deterministically computes a positive 63-bit integer surrogate key
    from arbitrary string parts using MD5 hashing.
    Matches PySpark abs(hash(concat(...))) semantics across environments.
    """
    composite = "_".join("" if v is None else str(v) for v in values)
    digest = hashlib.md5(composite.encode("utf-8")).hexdigest()
    # Take first 15 hex digits to fit in positive signed 64-bit integer
    return int(digest[:15], 16) & 0x7FFFFFFFFFFFFFFF


def compute_who_pm25_exceedance(pm2_5: Optional[float], threshold: float = 15.0) -> bool:
    """
    Evaluates whether PM2.5 concentration exceeds the WHO 24-hour guideline (15.0 ug/m3).
    Returns False if PM2.5 is None or unmetered.
    """
    if pm2_5 is None:
        return False
    try:
        return float(pm2_5) > threshold
    except (ValueError, TypeError):
        return False


def calculate_health_severity_score(us_aqi: Optional[int]) -> float:
    """
    Assigns a continuous health severity metric [1.0 to 6.0] based on US AQI:
    - 1.0: Good (0-50)
    - 2.0: Moderate (51-100)
    - 3.0: Unhealthy for Sensitive Groups (101-150)
    - 4.0: Unhealthy (151-200)
    - 5.0: Very Unhealthy (201-300)
    - 6.0: Hazardous (301-500)
    """
    if us_aqi is None:
        return 0.0

    try:
        score = int(us_aqi)
    except (ValueError, TypeError):
        return 0.0

    if score <= 50:
        return 1.0
    elif score <= 100:
        return 2.0
    elif score <= 150:
        return 3.0
    elif score <= 200:
        return 4.0
    elif score <= 300:
        return 5.0
    else:
        return 6.0


def add_hourly_surrogate_keys(df_silver):
    """
    PySpark DataFrame transformer for fact_air_quality_hourly:
    Generates surrogate keys, rolling 24-hour averages, and WHO flags.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    window_24h = (
        Window
        .partitionBy("station_id")
        .orderBy("recorded_at")
        .rowsBetween(-23, 0)
    )

    return (
        df_silver
        .withColumn("station_sk", F.abs(F.hash(F.col("station_id"))))
        .withColumn("date_sk", F.date_format(F.to_date("recorded_at"), "yyyyMMdd").cast("integer"))
        .withColumn("category_sk", F.abs(F.hash(F.col("aqi_category"))))
        .withColumn("fact_sk", F.abs(F.hash(F.concat(F.col("station_id"), F.lit("_"), F.col("recorded_at")))))
        .withColumn("rolling_24h_avg_pm25", F.round(F.avg("pm2_5").over(window_24h), 2))
        .withColumn("is_who_pm25_exceeded", F.when(F.col("pm2_5") > 15.0, F.lit(True)).otherwise(F.lit(False)))
        .withColumn(
            "health_severity_score",
            F.when(F.col("us_aqi") <= 50, F.lit(1.0))
             .when(F.col("us_aqi") <= 100, F.lit(2.0))
             .when(F.col("us_aqi") <= 150, F.lit(3.0))
             .when(F.col("us_aqi") <= 200, F.lit(4.0))
             .when(F.col("us_aqi") <= 300, F.lit(5.0))
             .otherwise(F.lit(6.0))
        )
    )
