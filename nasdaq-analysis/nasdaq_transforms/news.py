"""
News Transformations Module
---------------------------
Cleanses, normalizes, and deduplicates Finnhub news articles.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_news_articles(df: DataFrame) -> DataFrame:
    """
    Standardizes schema, converts Unix epoch datetime to timestamp,
    maps unassigned tickers to GENERAL, and deduplicates on ArticleId.
    """
    cleaned = (
        df
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
            "Url"
        )
        .filter(
            F.col("ArticleId").isNotNull() &
            (F.col("ArticleId") != "") &
            F.col("Headline").isNotNull() &
            (F.col("Headline") != "") &
            F.col("NewsTimestamp").isNotNull()
        )
        .dropDuplicates(["ArticleId"])
    )

    return cleaned


def extract_invalid_news_records(df: DataFrame) -> DataFrame:
    """
    Extracts malformed or un-parseable news records for routing to quarantine.
    """
    quarantine = (
        df
        .withColumn("ArticleId", F.col("id").cast("string"))
        .withColumn("NewsTimestamp", F.from_unixtime(F.col("datetime").cast("long")).cast("timestamp"))
        .withColumn("Headline", F.trim(F.col("headline")))
        .filter(
            F.col("ArticleId").isNull() |
            (F.col("ArticleId") == "") |
            F.col("Headline").isNull() |
            (F.col("Headline") == "") |
            F.col("NewsTimestamp").isNull()
        )
        .withColumn("quarantine_reason", F.when(
            F.col("ArticleId").isNull() | (F.col("ArticleId") == ""), F.lit("Missing or Empty ArticleId")
        ).when(
            F.col("Headline").isNull() | (F.col("Headline") == ""), F.lit("Missing or Empty Headline")
        ).otherwise(F.lit("Invalid NewsTimestamp / Datetime")))
        .withColumn("quarantined_at", F.current_timestamp())
    )

    return quarantine
