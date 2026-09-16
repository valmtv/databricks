"""
Aggregation & Summary Transformations
-------------------------------------
Aggregates hourly telemetry into daily city-level summaries,
computes air quality compliance grades, and evaluates threshold breaches.
"""

from typing import Optional, Dict, Any, List


def assign_compliance_grade(avg_us_aqi: Optional[float]) -> str:
    """
    Assigns an environmental compliance grade based on the 24-hour mean US AQI:
    - <= 50:  'Grade A (Clean)'
    - <= 100: 'Grade B (Acceptable)'
    - <= 150: 'Grade C (Warning)'
    - > 150:  'Grade D (Action Required)'
    """
    if avg_us_aqi is None:
        return "Grade D (Action Required)"
    
    try:
        score = float(avg_us_aqi)
    except (ValueError, TypeError):
        return "Grade D (Action Required)"

    if score <= 50.0:
        return "Grade A (Clean)"
    elif score <= 100.0:
        return "Grade B (Acceptable)"
    elif score <= 150.0:
        return "Grade C (Warning)"
    else:
        return "Grade D (Action Required)"


def calculate_unhealthy_hours_percentage(hours_unhealthy: int, total_observations: int) -> float:
    """
    Computes percentage of recorded hours where AQI exceeded the safe threshold (> 100).
    Guards against division by zero.
    """
    if total_observations <= 0:
        return 0.0
    return round((max(0, hours_unhealthy) * 100.0) / total_observations, 1)


def calculate_daily_aggregates(hourly_records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Pure Python aggregator for an array of hourly records belonging to 1 city/station and 1 day.
    """
    if not hourly_records:
        return {
            "observation_count": 0,
            "avg_us_aqi": None,
            "max_us_aqi": None,
            "min_us_aqi": None,
            "hours_safe": 0,
            "hours_moderate": 0,
            "hours_unhealthy": 0,
            "unhealthy_hours_pct": 0.0,
            "compliance_grade": "Grade D (Action Required)"
        }

    aqi_values = [r["us_aqi"] for r in hourly_records if r.get("us_aqi") is not None]
    count = len(hourly_records)

    avg_aqi = round(sum(aqi_values) / len(aqi_values), 1) if aqi_values else None
    max_aqi = max(aqi_values) if aqi_values else None
    min_aqi = min(aqi_values) if aqi_values else None

    hours_safe = sum(1 for aqi in aqi_values if aqi <= 50)
    hours_moderate = sum(1 for aqi in aqi_values if 50 < aqi <= 100)
    hours_unhealthy = sum(1 for aqi in aqi_values if aqi > 100)

    unhealthy_pct = calculate_unhealthy_hours_percentage(hours_unhealthy, count)
    compliance = assign_compliance_grade(avg_aqi)

    return {
        "observation_count": count,
        "avg_us_aqi": avg_aqi,
        "max_us_aqi": max_aqi,
        "min_us_aqi": min_aqi,
        "hours_safe": hours_safe,
        "hours_moderate": hours_moderate,
        "hours_unhealthy": hours_unhealthy,
        "unhealthy_hours_pct": unhealthy_pct,
        "compliance_grade": compliance
    }


def build_fact_city_daily_summary_df(df_hourly):
    """
    PySpark DataFrame transformer:
    Rolls up hourly observations to the daily station grain.
    """
    from pyspark.sql import functions as F

    return (
        df_hourly
        .withColumn("calendar_date", F.to_date("recorded_at"))
        .groupBy("station_sk", "date_sk", "city", "country", "calendar_date")
        .agg(
            F.count("*").alias("observation_count"),
            F.round(F.avg("us_aqi"), 1).alias("avg_us_aqi"),
            F.max("us_aqi").alias("max_us_aqi"),
            F.min("us_aqi").alias("min_us_aqi"),
            F.round(F.avg("pm2_5"), 2).alias("avg_pm2_5"),
            F.round(F.max("pm2_5"), 2).alias("max_pm2_5"),
            F.round(F.avg("pm10"), 2).alias("avg_pm10"),
            F.round(F.avg("ozone"), 2).alias("avg_ozone"),
            F.round(F.avg("nitrogen_dioxide"), 2).alias("avg_no2"),
            F.sum(F.when(F.col("us_aqi") <= 50, 1).otherwise(0)).alias("hours_safe"),
            F.sum(F.when((F.col("us_aqi") > 50) & (F.col("us_aqi") <= 100), 1).otherwise(0)).alias("hours_moderate"),
            F.sum(F.when(F.col("us_aqi") > 100, 1).otherwise(0)).alias("hours_unhealthy"),
            F.sum(F.when(F.col("is_who_pm25_exceeded") == True, 1).otherwise(0)).alias("hours_who_exceeded")
        )
        .withColumn("summary_sk", F.abs(F.hash(F.concat(F.col("city"), F.lit("_"), F.col("calendar_date")))))
        .withColumn(
            "unhealthy_hours_pct",
            F.round((F.col("hours_unhealthy") * 100.0) / F.col("observation_count"), 1)
        )
        .withColumn(
            "compliance_grade",
            F.when(F.col("avg_us_aqi") <= 50, F.lit("Grade A (Clean)"))
             .when(F.col("avg_us_aqi") <= 100, F.lit("Grade B (Acceptable)"))
             .when(F.col("avg_us_aqi") <= 150, F.lit("Grade C (Warning)"))
             .otherwise(F.lit("Grade D (Action Required)"))
        )
        .select(
            "summary_sk",
            "station_sk",
            "date_sk",
            "city",
            "country",
            "calendar_date",
            "observation_count",
            "avg_us_aqi",
            "max_us_aqi",
            "min_us_aqi",
            "avg_pm2_5",
            "max_pm2_5",
            "avg_pm10",
            "avg_ozone",
            "avg_no2",
            "hours_safe",
            "hours_moderate",
            "hours_unhealthy",
            "hours_who_exceeded",
            "unhealthy_hours_pct",
            "compliance_grade"
        )
    )
