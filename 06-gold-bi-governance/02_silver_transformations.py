"""
Silver Layer Transformation Module (Lakeflow Declarative Pipeline)
------------------------------------------------------------------
Cleanses streaming telemetry, routes invalid records to a quarantine table,
enriches valid telemetry with EPA reference standards, and tracks quality metrics.
Refactored to import pure transformation logic from `air_quality_transforms`.
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
from air_quality_transforms.cleansing import clean_silver_air_quality_df
from air_quality_transforms.enrichment import enrich_with_aqi_reference
from air_quality_transforms.quarantine import build_quarantine_predicate_spark


# -----------------------------------------------------------------------------
# 1. Silver Quarantine Table (Enterprise Zero-Silent-Loss Pattern)
# -----------------------------------------------------------------------------
@dlt.table(
    name="silver_air_quality_quarantine",
    comment="Quarantined air quality observations failing completeness, validity, or timeliness checks",
    table_properties={
        "quality": "silver_quarantine",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def silver_air_quality_quarantine():
    """
    Captures corrupted, out-of-bounds, or future-dated records without dropping them silently.
    Provides diagnostic error reasons and quarantine timestamps for data engineering review.
    """
    df_raw = dlt.read_stream("bronze_air_quality_raw")
    df_cleaned = clean_silver_air_quality_df(df_raw)
    is_quarantine_cond = build_quarantine_predicate_spark()

    return (
        df_cleaned
        .filter(is_quarantine_cond)
        .withColumn(
            "quarantine_reason",
            F.concat_ws(" | ",
                F.when(F.col("event_id").isNull() | (F.trim(F.col("event_id")) == ""), F.lit("MISSING_EVENT_ID")),
                F.when(F.col("station_id").isNull() | (F.trim(F.col("station_id")) == ""), F.lit("MISSING_STATION_ID")),
                F.when(F.col("city").isNull() | (F.trim(F.col("city")) == ""), F.lit("MISSING_CITY")),
                F.when(F.col("recorded_at").isNull(), F.lit("NULL_TIMESTAMP")),
                F.when(F.col("recorded_at") > F.current_timestamp(), F.lit("FUTURE_TIMESTAMP")),
                F.when((F.col("latitude") < -90.0) | (F.col("latitude") > 90.0), F.lit("LATITUDE_OUT_OF_BOUNDS")),
                F.when((F.col("longitude") < -180.0) | (F.col("longitude") > 180.0), F.lit("LONGITUDE_OUT_OF_BOUNDS")),
                F.when((F.col("pm2_5").isNotNull() & (F.col("pm2_5") < 0.0)), F.lit("NEGATIVE_PM25")),
                F.when((F.col("pm10").isNotNull() & (F.col("pm10") < 0.0)), F.lit("NEGATIVE_PM10")),
                F.when((F.col("us_aqi").isNotNull() & ((F.col("us_aqi") < 0) | (F.col("us_aqi") > 500))), F.lit("AQI_OUT_OF_BOUNDS"))
            )
        )
        .withColumn("quarantined_at", F.current_timestamp())
    )


# -----------------------------------------------------------------------------
# 2. Clean Enriched Silver Table (With Lakeflow Quality Expectations)
# -----------------------------------------------------------------------------
@dlt.table(
    name="silver_air_quality_enriched",
    comment="Cleansed, validated, and EPA-classified streaming air quality observations",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect("valid_observation_timestamp", "recorded_at IS NOT NULL AND recorded_at <= current_timestamp()")
@dlt.expect(
    "valid_pollutant_ranges",
    """
    (pm2_5 IS NULL OR pm2_5 >= 0.0) AND
    (pm10 IS NULL OR pm10 >= 0.0) AND
    (us_aqi IS NULL OR (us_aqi >= 0 AND us_aqi <= 500)) AND
    latitude BETWEEN -90.0 AND 90.0 AND
    longitude BETWEEN -180.0 AND 180.0
    """
)
@dlt.expect_or_fail("valid_primary_keys", "event_id IS NOT NULL AND station_id IS NOT NULL AND city IS NOT NULL")
def silver_air_quality_enriched():
    """
    Downstream-ready clean telemetry.
    Filters out quarantine candidates, deduplicates on event_id, and enriches with EPA advisory metadata.
    """
    df_raw = dlt.read_stream("bronze_air_quality_raw")
    df_ref = dlt.read("bronze_aqi_reference")

    df_cleaned = clean_silver_air_quality_df(df_raw)
    is_quarantine_cond = build_quarantine_predicate_spark()

    # Route only validated records to clean silver with streaming primary key deduplication
    df_valid = df_cleaned.filter(~is_quarantine_cond).dropDuplicates(["event_id"])
    return enrich_with_aqi_reference(df_valid, df_ref)
