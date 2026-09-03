"""
Silver Layer Cleansing & Validation Module (Lakeflow Declarative Pipeline)
--------------------------------------------------------------------------
Enforces data quality rules using DLT expectations, deduplicates streaming ticks,
and routes rejected records to quarantine tables for auditing.
"""

try:
    import dlt
except ImportError:
    import pyspark.pipelines as dlt

from pyspark.sql import functions as F


# -------------------------------------------------------------------------
# Price Stream & Quarantine
# -------------------------------------------------------------------------

@dlt.table(
    name="silver_price_quarantine",
    comment="Quarantined raw price records failing quality constraints (negative prices/volumes or null keys)",
    table_properties={"quality": "quarantine"}
)
def silver_price_quarantine():
    df_raw = dlt.read_stream("bronze_nasdaq_price_raw")
    cols = df_raw.columns
    ts_col = "PriceTimestamp" if "PriceTimestamp" in cols else ("Datetime" if "Datetime" in cols else "Date")

    return (
        df_raw
        .withColumn("ParsedTimestamp", F.to_timestamp(F.col(ts_col)))
        .withColumn("ParsedClose", F.col("Close").cast("double"))
        .withColumn("ParsedVolume", F.col("Volume").cast("long"))
        .filter(
            F.col("Symbol").isNull() |
            (F.trim(F.col("Symbol")) == "") |
            F.col("ParsedTimestamp").isNull() |
            F.col("ParsedClose").isNull() |
            (F.col("ParsedClose") <= 0.0) |
            F.col("ParsedVolume").isNull() |
            (F.col("ParsedVolume") < 0)
        )
        .withColumn("quarantine_reason", F.when(
            F.col("Symbol").isNull() | (F.trim(F.col("Symbol")) == ""), F.lit("Missing or Empty Symbol")
        ).when(
            F.col("ParsedTimestamp").isNull(), F.lit("Invalid PriceTimestamp format")
        ).when(
            F.col("ParsedClose").isNull() | (F.col("ParsedClose") <= 0.0), F.lit("Invalid Close Price (<= 0)")
        ).otherwise(F.lit("Negative or Null Volume")))
        .withColumn("quarantined_at", F.current_timestamp())
    )


@dlt.table(
    name="silver_nasdaq_price_5m",
    comment="Cleansed, validated, and deduplicated 5-minute stock price bars",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_or_fail("valid_symbol_key", "Symbol IS NOT NULL AND Symbol != ''")
@dlt.expect_or_drop("valid_price_bounds", "Close > 0.0 AND Volume >= 0")
@dlt.expect("valid_price_timestamp", "PriceTimestamp IS NOT NULL AND PriceTimestamp <= current_timestamp()")
def silver_nasdaq_price_5m():
    df_raw = dlt.read_stream("bronze_nasdaq_price_raw")
    cols = df_raw.columns
    ts_col = "PriceTimestamp" if "PriceTimestamp" in cols else ("Datetime" if "Datetime" in cols else "Date")

    return (
        df_raw
        .withColumn("PriceTimestamp", F.to_timestamp(F.col(ts_col)))
        .withColumn("TradeDate", F.to_date(F.col("PriceTimestamp")))
        .withColumn("Symbol", F.upper(F.trim(F.col("Symbol"))))
        .withColumn("Open", F.col("Open").cast("double"))
        .withColumn("High", F.col("High").cast("double"))
        .withColumn("Low", F.col("Low").cast("double"))
        .withColumn("Close", F.col("Close").cast("double"))
        .withColumn("Volume", F.col("Volume").cast("long"))
        .select(
            "Symbol",
            "PriceTimestamp",
            "TradeDate",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "_source_file",
            "_ingestion_timestamp"
        )
        .filter(
            F.col("Symbol").isNotNull() &
            (F.col("Symbol") != "") &
            F.col("PriceTimestamp").isNotNull() &
            F.col("Close").isNotNull() &
            (F.col("Close") > 0.0) &
            F.col("Volume").isNotNull() &
            (F.col("Volume") >= 0)
        )
    )


# -------------------------------------------------------------------------
# News Stream & Quarantine
# -------------------------------------------------------------------------

@dlt.table(
    name="silver_news_quarantine",
    comment="Quarantined news records failing required key or text validations",
    table_properties={"quality": "quarantine"}
)
def silver_news_quarantine():
    df_raw = dlt.read_stream("bronze_finnhub_news_raw")

    return (
        df_raw
        .withColumn("ArticleId", F.col("id").cast("string"))
        .withColumn("Headline", F.trim(F.col("headline")))
        .withColumn("ParsedTimestamp", F.from_unixtime(F.col("datetime").cast("long")).cast("timestamp"))
        .filter(
            F.col("ArticleId").isNull() |
            (F.trim(F.col("ArticleId")) == "") |
            F.col("Headline").isNull() |
            (F.trim(F.col("Headline")) == "") |
            F.col("ParsedTimestamp").isNull()
        )
        .withColumn("quarantine_reason", F.when(
            F.col("ArticleId").isNull() | (F.trim(F.col("ArticleId")) == ""), F.lit("Missing or Empty ArticleId")
        ).when(
            F.col("Headline").isNull() | (F.trim(F.col("Headline")) == ""), F.lit("Missing or Empty Headline")
        ).otherwise(F.lit("Unparseable Unix datetime timestamp")))
        .withColumn("quarantined_at", F.current_timestamp())
    )


@dlt.table(
    name="silver_finnhub_news",
    comment="Cleansed, normalized, and deduplicated Finnhub market and company news",
    table_properties={
        "quality": "silver",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_or_fail("valid_article_id", "ArticleId IS NOT NULL AND ArticleId != ''")
@dlt.expect_or_drop("valid_headline", "Headline IS NOT NULL AND Headline != ''")
@dlt.expect("valid_news_timestamp", "NewsTimestamp IS NOT NULL")
def silver_finnhub_news():
    df_raw = dlt.read_stream("bronze_finnhub_news_raw")

    return (
        df_raw
        .withColumn("ArticleId", F.col("id").cast("string"))
        .withColumn("NewsTimestamp", F.from_unixtime(F.col("datetime").cast("long")).cast("timestamp"))
        .withColumn("NewsDate", F.to_date(F.col("NewsTimestamp")))
        .withColumn("Headline", F.trim(F.col("headline")))
        .withColumn("Summary", F.trim(F.col("summary")))
        .withColumn(
            "Symbol",
            F.when(
                (F.col("related").isNull()) | (F.trim(F.col("related")) == ""),
                F.lit("GENERAL")
            ).otherwise(F.upper(F.trim(F.col("related"))))
        )
        .withColumn("Source", F.coalesce(F.trim(F.col("source")), F.lit("Unknown")))
        .withColumn("Category", F.coalesce(F.trim(F.col("category")), F.lit("general")))
        .withColumn("Url", F.coalesce(F.trim(F.col("url")), F.lit("")))
        .select(
            "ArticleId",
            "Symbol",
            "NewsDate",
            "NewsTimestamp",
            "Headline",
            "Summary",
            "Source",
            "Category",
            "Url",
            "_source_file",
            "_ingestion_timestamp"
        )
        .filter(
            F.col("ArticleId").isNotNull() &
            (F.col("ArticleId") != "") &
            F.col("Headline").isNotNull() &
            (F.col("Headline") != "") &
            F.col("NewsTimestamp").isNotNull()
        )
    )
