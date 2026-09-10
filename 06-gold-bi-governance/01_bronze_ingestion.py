"""
Bronze Layer Ingestion Module
-----------------------------
Ingests raw streaming air quality telemetry via Auto Loader and batch EPA AQI reference standards.
"""

try:
    import dlt
except ImportError:
    import pyspark.pipelines as dlt

from pyspark.sql import functions as F


def get_landing_base_path() -> str:
    """
    Dynamically resolves landing path from pipeline configurations.
    Defaults to the user's workspace path or shared landing path if not explicitly configured.
    """
    if spark.conf.get("pipeline.landing_path", None):
        return spark.conf.get("pipeline.landing_path")
    
    try:
        user_name = spark.sql("SELECT current_user()").collect()[0][0]
        user_path = f"/Workspace/Users/{user_name}/air_quality_landing/landing"
        return user_path
    except Exception:
        return "/Workspace/Shared/air_quality_landing/landing"


@dlt.table(
    name="bronze_air_quality_raw",
    comment="Raw streaming telemetry observations ingested from landing zone",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_air_quality_raw():
    """
    Streams raw telemetry JSON files using Auto Loader (cloudFiles).
    Auto Loader automatically manages file discovery state, schema inference,
    and checkpointing natively within the Lakeflow pipeline.
    """
    landing_base = get_landing_base_path()
    stream_source_path = f"{landing_base}/telemetry_stream"

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{stream_source_path}/_schema")
        .load(stream_source_path)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dlt.table(
    name="bronze_aqi_reference",
    comment="EPA Air Quality Index (AQI) classification standard thresholds and health advisories",
    table_properties={
        "quality": "bronze"
    }
)
def bronze_aqi_reference():
    """
    Ingests the static EPA AQI classification reference lookup table from CSV.
    """
    landing_base = get_landing_base_path()
    reference_path = f"{landing_base}/reference"

    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(reference_path)
        .withColumn("_reference_loaded_at", F.current_timestamp())
    )
