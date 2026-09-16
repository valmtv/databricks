"""
Dimensional Modeling Transformations
------------------------------------
Conformed dimensions for the Gold Star Schema:
- dim_calendar_date: Temporal hierarchy with weekend indicators
- dim_city: Station geographic coordinates, region, and climate classifications
- dim_aqi_category: Standardized EPA air quality severity ranking
"""

from typing import Dict, Any, List
from datetime import date, datetime


def parse_date_hierarchy(cal_date: date) -> Dict[str, Any]:
    """
    Decomposes a calendar date object into dimensional attributes and a surrogate key.
    """
    date_sk = int(cal_date.strftime("%Y%m%d"))
    quarter = (cal_date.month - 1) // 3 + 1
    # Python weekday(): Monday is 0, Sunday is 6. ISO weekday(): Monday is 1, Sunday is 7.
    # Spark dayofweek(): Sunday is 1, Saturday is 7.
    spark_day_of_week = (cal_date.weekday() + 1) % 7 + 1
    is_weekend = spark_day_of_week in (1, 7)

    return {
        "date_sk": date_sk,
        "calendar_date": cal_date,
        "year": cal_date.year,
        "quarter": quarter,
        "month": cal_date.month,
        "month_name": cal_date.strftime("%B"),
        "day_of_month": cal_date.day,
        "day_of_week": spark_day_of_week,
        "day_name": cal_date.strftime("%A"),
        "is_weekend": is_weekend
    }


def build_calendar_dimension_records(dates: List[date]) -> List[Dict[str, Any]]:
    """Builds unique dimensional date records from a list of date objects."""
    unique_dates = sorted(set(dates))
    return [parse_date_hierarchy(d) for d in unique_dates]


def enrich_station_metadata(
    station_id: str,
    city: str,
    country: str,
    latitude: float,
    longitude: float,
    metadata_lookup: Dict[str, Dict[str, str]]
) -> Dict[str, Any]:
    """
    Enriches a station record with conformed regional attributes.
    """
    meta = metadata_lookup.get(station_id, {})
    return {
        "station_id": station_id,
        "city": city,
        "country": country,
        "latitude": latitude,
        "longitude": longitude,
        "region": meta.get("region", "Other"),
        "climate_zone": meta.get("climate_zone", "Temperate")
    }


def build_dim_calendar_date_df(df_silver):
    """
    PySpark DataFrame transformer:
    Builds conformed calendar date dimension from silver observations.
    """
    from pyspark.sql import functions as F

    return (
        df_silver
        .withColumn("calendar_date", F.to_date("recorded_at"))
        .select("calendar_date")
        .distinct()
        .withColumn("date_sk", F.date_format("calendar_date", "yyyyMMdd").cast("integer"))
        .withColumn("year", F.year("calendar_date"))
        .withColumn("quarter", F.quarter("calendar_date"))
        .withColumn("month", F.month("calendar_date"))
        .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
        .withColumn("day_of_month", F.dayofmonth("calendar_date"))
        .withColumn("day_of_week", F.dayofweek("calendar_date"))
        .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
        .withColumn("is_weekend", F.when(F.dayofweek("calendar_date").isin(1, 7), F.lit(True)).otherwise(F.lit(False)))
        .select(
            "date_sk",
            "calendar_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day_of_month",
            "day_of_week",
            "day_name",
            "is_weekend"
        )
    )


def build_dim_city_df(df_silver, meta_df):
    """
    PySpark DataFrame transformer:
    Builds conformed station & city dimension.
    """
    from pyspark.sql import functions as F

    distinct_stations = (
        df_silver
        .select("station_id", "city", "country", "latitude", "longitude")
        .distinct()
    )

    return (
        distinct_stations
        .join(meta_df, distinct_stations.station_id == meta_df.meta_station_id, "left")
        .withColumn("station_sk", F.abs(F.hash(F.col("station_id"))))
        .withColumn("region", F.coalesce(F.col("region"), F.lit("Other")))
        .withColumn("climate_zone", F.coalesce(F.col("climate_zone"), F.lit("Temperate")))
        .select(
            "station_sk",
            "station_id",
            "city",
            "country",
            "latitude",
            "longitude",
            "region",
            "climate_zone"
        )
    )


def build_dim_aqi_category_df(df_ref):
    """
    PySpark DataFrame transformer:
    Builds EPA AQI category dimension with severity ranks.
    """
    from pyspark.sql import functions as F

    return (
        df_ref
        .withColumn("category_sk", F.abs(F.hash(F.col("category"))))
        .withColumn(
            "severity_rank",
            F.when(F.col("category") == "Good", F.lit(1))
             .when(F.col("category") == "Moderate", F.lit(2))
             .when(F.col("category") == "Unhealthy for Sensitive Groups", F.lit(3))
             .when(F.col("category") == "Unhealthy", F.lit(4))
             .when(F.col("category") == "Very Unhealthy", F.lit(5))
             .when(F.col("category") == "Hazardous", F.lit(6))
             .otherwise(F.lit(0))
        )
        .select(
            "category_sk",
            F.col("category").alias("aqi_category"),
            "aqi_min",
            "aqi_max",
            "color_code",
            "health_implication",
            "cautionary_statement",
            "severity_rank"
        )
    )
