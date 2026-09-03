"""
Bronze Layer Ingestion Module (Lakeflow Declarative Pipeline)
-------------------------------------------------------------
Streams raw 5-minute stock prices and Finnhub news JSONs into Bronze Delta tables
using Auto Loader (cloudFiles) with explicit schemas to handle empty initial landing states.
"""

try:
    import dlt
except ImportError:
    import pyspark.pipelines as dlt

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Explicit schema prevents CF_EMPTY_DIR_FOR_SCHEMA_INFERENCE when landing zone is initially empty
PRICE_RAW_SCHEMA = StructType([
    StructField("PriceTimestamp", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", LongType(), True),
    StructField("Symbol", StringType(), True),
])

NEWS_RAW_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("datetime", LongType(), True),
    StructField("headline", StringType(), True),
    StructField("summary", StringType(), True),
    StructField("related", StringType(), True),
    StructField("source", StringType(), True),
    StructField("category", StringType(), True),
    StructField("url", StringType(), True),
    StructField("image", StringType(), True),
    StructField("_schema_phase", StringType(), True),
    StructField("index_tracker", StringType(), True),
    StructField("_landing_batch_id", IntegerType(), True),
    StructField("_ingested_at", StringType(), True),
])


def get_landing_paths():
    """
    Dynamically resolves landing paths for prices and news from pipeline configurations,
    with fallbacks to Unity Catalog volumes or workspace directory.
    """
    price_path = spark.conf.get("pipeline.landing_price_path", None)
    news_path = spark.conf.get("pipeline.landing_news_path", None)

    if not price_path or not news_path:
        try:
            user_name = spark.sql("SELECT current_user()").collect()[0][0]
            if not price_path:
                price_path = f"/Workspace/Users/{user_name}/nasdaq_landing/landing/nasdaq_price"
            if not news_path:
                news_path = f"/Workspace/Users/{user_name}/nasdaq_landing/landing/finnhub_news"
        except Exception:
            if not price_path:
                price_path = "/Volumes/dbr_dev/valeriimatviiv_bronze/market_radar_landing/landing/nasdaq_price"
            if not news_path:
                news_path = "/Volumes/dbr_dev/valeriimatviiv_bronze/market_radar_landing/landing/finnhub_news"

    return price_path, news_path


@dlt.table(
    name="bronze_nasdaq_price_raw",
    comment="Raw streaming 5-minute stock price ticks ingested from landing zone",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_nasdaq_price_raw():
    price_landing_path, _ = get_landing_paths()

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .schema(PRICE_RAW_SCHEMA)
        .option("cloudFiles.schemaLocation", f"{price_landing_path}/_schema")
        .load(price_landing_path)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dlt.table(
    name="bronze_finnhub_news_raw",
    comment="Raw streaming Finnhub market and company news JSONs ingested from landing zone",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def bronze_finnhub_news_raw():
    _, news_landing_path = get_landing_paths()

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(NEWS_RAW_SCHEMA)
        .option("cloudFiles.schemaLocation", f"{news_landing_path}/_schema")
        .load(news_landing_path)
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
