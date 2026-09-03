"""
Star Schema Dimension Generators
--------------------------------
Generates conformed dimensions for the Gold Layer Star Schema:
- dim_company
- dim_date
- dim_news_source
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Reference company metadata lookup
COMPANY_METADATA = [
    ("AAPL", "Apple Inc.", "Technology", "Consumer Electronics", "NASDAQ"),
    ("NVDA", "NVIDIA Corporation", "Technology", "Semiconductors", "NASDAQ"),
    ("MSFT", "Microsoft Corporation", "Technology", "Software - Infrastructure", "NASDAQ"),
    ("AMZN", "Amazon.com Inc.", "Consumer Cyclical", "Internet Retail", "NASDAQ"),
    ("TSLA", "Tesla Inc.", "Consumer Cyclical", "Auto Manufacturers", "NASDAQ"),
    ("QQQ", "Invesco QQQ Trust", "Financial Services", "Exchange Traded Fund", "NASDAQ"),
]


def build_dim_company(df_tickers: DataFrame) -> DataFrame:
    """
    Builds dim_company dimension table enriched with sector and industry classifications.
    """
    spark = df_tickers.sql_ctx.sparkSession
    meta_df = spark.createDataFrame(
        COMPANY_METADATA,
        ["Symbol_ref", "CompanyName", "Sector", "Industry", "Exchange"]
    )

    distinct_symbols = (
        df_tickers
        .select(F.upper(F.trim(F.col("Symbol"))).alias("Symbol"))
        .filter((F.col("Symbol") != "GENERAL") & F.col("Symbol").isNotNull())
        .distinct()
    )

    dim_company = (
        distinct_symbols
        .join(meta_df, distinct_symbols.Symbol == meta_df.Symbol_ref, "left")
        .withColumn("company_sk", F.abs(F.hash(F.col("Symbol"))))
        .withColumn("CompanyName", F.coalesce(F.col("CompanyName"), F.col("Symbol")))
        .withColumn("Sector", F.coalesce(F.col("Sector"), F.lit("Unknown")))
        .withColumn("Industry", F.coalesce(F.col("Industry"), F.lit("Unknown")))
        .withColumn("Exchange", F.coalesce(F.col("Exchange"), F.lit("NASDAQ")))
        .select(
            "company_sk",
            "Symbol",
            "CompanyName",
            "Sector",
            "Industry",
            "Exchange"
        )
    )

    return dim_company


def build_dim_date(df_dates: DataFrame) -> DataFrame:
    """
    Builds conformed dim_date table from available trade and news dates.
    """
    distinct_dates = (
        df_dates
        .select(F.col("Date").cast("date").alias("CalendarDate"))
        .filter(F.col("CalendarDate").isNotNull())
        .distinct()
    )

    dim_date = (
        distinct_dates
        .withColumn("date_sk", F.date_format("CalendarDate", "yyyyMMdd").cast("integer"))
        .withColumn("Year", F.year("CalendarDate"))
        .withColumn("Quarter", F.quarter("CalendarDate"))
        .withColumn("Month", F.month("CalendarDate"))
        .withColumn("DayOfMonth", F.dayofmonth("CalendarDate"))
        .withColumn("DayOfWeek", F.dayofweek("CalendarDate"))
        .withColumn("DayName", F.date_format("CalendarDate", "EEEE"))
        .withColumn(
            "IsWeekend",
            F.when(F.dayofweek("CalendarDate").isin(1, 7), F.lit(True)).otherwise(F.lit(False))
        )
        .select(
            "date_sk",
            "CalendarDate",
            "Year",
            "Quarter",
            "Month",
            "DayOfMonth",
            "DayOfWeek",
            "DayName",
            "IsWeekend"
        )
        .orderBy("CalendarDate")
    )

    return dim_date


def build_dim_news_source(df_news: DataFrame) -> DataFrame:
    """
    Builds dim_news_source dimension table from news metadata.
    """
    distinct_sources = (
        df_news
        .select(
            F.coalesce(F.trim(F.col("Source")), F.lit("Unknown")).alias("SourceName"),
            F.coalesce(F.trim(F.col("Category")), F.lit("general")).alias("Category")
        )
        .distinct()
    )

    dim_source = (
        distinct_sources
        .withColumn("source_sk", F.abs(F.hash(F.concat(F.col("SourceName"), F.lit("_"), F.col("Category")))))
        .select(
            "source_sk",
            "SourceName",
            "Category"
        )
    )

    return dim_source
