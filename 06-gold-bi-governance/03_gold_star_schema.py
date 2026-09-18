"""
Gold Star Schema Layer Module (Lakeflow Declarative Pipeline)
-------------------------------------------------------------
Implements the Gold Star Schema dimensional model for Air Quality Business Intelligence.
Refactored to import pure transformation logic from `air_quality_transforms`.

STAR-SCHEMA ARCHITECTURE:
- Fact Tables:
  * `fact_air_quality_hourly`: Granular observation grain (station x hour) with rolling 24h average PM2.5.
  * `fact_city_daily_summary`: Aggregated executive grain (city x day) with compliance grading.
- Dimension Tables:
  * `dim_city`: Conformed station and regional geography.
  * `dim_calendar_date`: Conformed temporal hierarchy.
  * `dim_aqi_category`: Conformed EPA AQI severity classification.
"""

import sys
import os

# Ensure package is importable in both local IDE and DLT cluster execution
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    current_dir = os.getcwd()

if current_dir not in sys.path:
    sys.path.insert(0, current_dir)


try:
    import dlt
except ImportError:
    import pyspark.pipelines as dlt

from pyspark.sql import functions as F
from air_quality_transforms.dimensions import (
    build_dim_city_df,
    build_dim_calendar_date_df,
    build_dim_aqi_category_df,
)
from air_quality_transforms.features import add_hourly_surrogate_keys
from air_quality_transforms.aggregations import build_fact_city_daily_summary_df


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
    
    return build_dim_city_df(df_silver, meta_df)


# Note: dim_calendar_date is an enterprise conformed dimension generated once
# via recursive CTE in 00b_generate_dim_date.sql to minimize streaming pipeline
# shuffle, I/O, and compute overhead.


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
    return build_dim_aqi_category_df(df_ref)


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
    with_features = add_hourly_surrogate_keys(df_silver)
    
    return (
        with_features
        .dropDuplicates(["fact_sk"])
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
    return build_fact_city_daily_summary_df(df_hourly)
