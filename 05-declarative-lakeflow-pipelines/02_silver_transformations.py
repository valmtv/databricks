"""
Silver Layer Transformation Module
----------------------------------
Cleanses streaming telemetry, enriches with reference metadata, and enforces data quality rules.
"""

try:
    import dlt
except ImportError:
    import pyspark.pipelines as dlt

from pyspark.sql import functions as F


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
@dlt.expect_or_drop(
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
    df_raw = dlt.read_stream("bronze_air_quality_raw")
    df_ref = dlt.read("bronze_aqi_reference")

    df_cleaned = (
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

    df_enriched = (
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

    return df_enriched
