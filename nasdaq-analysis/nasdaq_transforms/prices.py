"""
Price Transformations Module
----------------------------
Cleanses, normalizes, and deduplicates 5-minute stock price ticks.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def clean_price_ticks(df: DataFrame) -> DataFrame:
    """
    Standardizes schema, parses timestamps, handles nulls, and deduplicates price ticks.
    Primary key: (Symbol, PriceTimestamp)
    """
    # Detect timestamp column (PriceTimestamp or Datetime or Date)
    cols = df.columns
    ts_col = "PriceTimestamp" if "PriceTimestamp" in cols else ("Datetime" if "Datetime" in cols else "Date")

    cleaned = (
        df
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
            "Volume"
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
        .dropDuplicates(["Symbol", "PriceTimestamp"])
    )

    return cleaned


def extract_invalid_price_records(df: DataFrame) -> DataFrame:
    """
    Extracts malformed or invalid records for routing to quarantine.
    """
    cols = df.columns
    ts_col = "PriceTimestamp" if "PriceTimestamp" in cols else ("Datetime" if "Datetime" in cols else "Date")

    quarantine = (
        df
        .withColumn("PriceTimestamp", F.to_timestamp(F.col(ts_col)))
        .withColumn("Symbol", F.upper(F.trim(F.col("Symbol"))))
        .withColumn("Close", F.col("Close").cast("double"))
        .withColumn("Volume", F.col("Volume").cast("long"))
        .filter(
            F.col("Symbol").isNull() |
            (F.col("Symbol") == "") |
            F.col("PriceTimestamp").isNull() |
            F.col("Close").isNull() |
            (F.col("Close") <= 0.0) |
            F.col("Volume").isNull() |
            (F.col("Volume") < 0)
        )
        .withColumn("quarantine_reason", F.when(
            F.col("Symbol").isNull() | (F.col("Symbol") == ""), F.lit("Missing or Empty Symbol")
        ).when(
            F.col("PriceTimestamp").isNull(), F.lit("Invalid PriceTimestamp")
        ).when(
            F.col("Close").isNull() | (F.col("Close") <= 0.0), F.lit("Invalid Close Price (<= 0)")
        ).otherwise(F.lit("Negative or Null Volume")))
        .withColumn("quarantined_at", F.current_timestamp())
    )

    return quarantine
