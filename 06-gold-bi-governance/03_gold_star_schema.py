"""
Gold Star Schema Layer Module (Lakeflow Declarative Pipeline)
-------------------------------------------------------------
Implements the Gold Star Schema dimensional model for Air Quality Business Intelligence:

STAR-SCHEMA BASICS:
- Star Schema: An industry-standard relational database schema optimized for OLAP data warehousing
  and fast business intelligence queries. It consists of centralized Fact tables surrounded by
  denormalized Dimension tables, resembling a star shape.
- Fact Tables: Contain quantitative measurements, numerical metrics, and foreign surrogate keys.
  Here: `fact_air_quality_hourly` (granular observation grain) and `fact_city_daily_summary` (aggregated grain).
- Dimension Tables: Contain rich descriptive context, attributes, and hierarchies used for filtering,
  grouping, and slicing. Here: `dim_city`, `dim_date`, and `dim_aqi_category`.
- Conformed Dimensions: Dimensions shared across multiple fact tables (`dim_city` and `dim_date`),
  allowing cross-grain drill-downs and consistent reporting across the enterprise.
- Surrogate Keys: Synthetic, system-generated integer or hash keys (e.g. `station_sk`, `date_sk`,
  `category_sk`) that decouple analytics from source system natural keys, protecting against schema changes.
- Star vs. Snowflake: Star schemas favor denormalized dimensions (fewer joins, superior query performance
  in modern columnar engines like Databricks Photon) over normalized Snowflake hierarchies.
"""

try:
    import dlt
except ImportError:
    import pyspark.pipelines as dlt

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# -----------------------------------------------------------------------------
# 1. Dimension Tables
# -----------------------------------------------------------------------------

CITY_METADATA = [
    ("US-NYC-001", "New York", "USA", 40.7128, -74.0060, "North America", "Humid Subtropical"),
    ("GB-LON-001", "London", "UK", 51.5074, -0.1278, "Western Europe", "Temperate Oceanic"),
    ("JP-TYO-001", "Tokyo", "Japan", 35.6762, 139.6503, "East Asia", "Humid Subtropical"),
    ("DE-BER-001", "Berlin", "Germany", 52.5200, 13.4050, "Central Europe", "Temperate Continental"),
    ("FR-PAR-001", "Paris", "France", 48.8566, 2.3522, "Western Europe", "Temperate Oceanic"),
]


@dlt.table(
    name="dim_city",
    comment="City and monitoring station dimension with geographic and regional classifications",
    table_properties={"quality": "gold"}
)
def dim_city():
    """
    Conformed city and station dimension table.
    Enriches sensor coordinates with regional classifications and generates surrogate keys.
    """
    df_silver = dlt.read("silver_air_quality_enriched")
    
    meta_df = spark.createDataFrame(
        CITY_METADATA,
        ["meta_station_id", "meta_city", "meta_country", "meta_lat", "meta_lon", "region", "climate_zone"]
    )
    
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


@dlt.table(
    name="dim_calendar_date",
    comment="Conformed calendar date dimension table for temporal rollups and slicing",
    table_properties={"quality": "gold"}
)
def dim_calendar_date():
    """
    Conformed date dimension providing calendar hierarchy (year, quarter, month, day of week).
    """
    df_silver = dlt.read("silver_air_quality_enriched")
    
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


@dlt.table(
    name="dim_aqi_category",
    comment="EPA Air Quality Index category dimension with health advisories and severity tiers",
    table_properties={"quality": "gold"}
)
def dim_aqi_category():
    """
    Standard EPA AQI category dimension with assigned numerical severity rank (1 to 6).
    """
    df_ref = dlt.read("bronze_aqi_reference")
    
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


# -----------------------------------------------------------------------------
# 2. Fact Tables
# -----------------------------------------------------------------------------

@dlt.table(
    name="fact_air_quality_hourly",
    comment="Hourly air quality observation fact table with rolling baselines and WHO threshold flags",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def fact_air_quality_hourly():
    """
    Core granular observation fact table.
    Grain: 1 record per monitoring station per recorded hour.
    Includes rolling 24h PM2.5 moving average and WHO guideline compliance indicator.
    """
    df_silver = dlt.read("silver_air_quality_enriched")
    
    # 24-hour rolling window per station
    window_24h = (
        Window
        .partitionBy("station_id")
        .orderBy("recorded_at")
        .rowsBetween(-23, 0)
    )
    
    with_keys = (
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
    
    return (
        with_keys
        .select(
            "fact_sk",
            "station_sk",
            "date_sk",
            "category_sk",
            "event_id",
            "station_id",
            "city",
            "country",
            "recorded_at",
            "pm2_5",
            "pm10",
            "carbon_monoxide",
            "nitrogen_dioxide",
            "sulphur_dioxide",
            "ozone",
            "us_aqi",
            "aqi_category",
            "aqi_color_code",
            "rolling_24h_avg_pm25",
            "is_who_pm25_exceeded",
            "health_severity_score",
            "_transformed_timestamp"
        )
    )


@dlt.table(
    name="fact_city_daily_summary",
    comment="Daily aggregated air quality business summary fact table for high-level executive KPIs",
    table_properties={
        "quality": "gold",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def fact_city_daily_summary():
    """
    Aggregated business fact table for daily executive monitoring.
    Grain: 1 record per city per calendar date.
    Tracks observation counts, safe vs unhealthy hours, and compliance rates.
    """
    df_hourly = dlt.read("fact_air_quality_hourly")
    
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
