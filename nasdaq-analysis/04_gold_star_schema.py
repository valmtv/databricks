"""
Gold Star Schema Layer Module (Lakeflow Declarative Pipeline)
-------------------------------------------------------------
Constructs the Star Schema dimensional model:
- Dimensions: dim_company, dim_date, dim_news_source
- Facts: fact_stock_prices_5m, fact_news_impact (with alpha, volatility, surge, and trajectory)
"""

try:
    import dlt
except ImportError:
    import pyspark.pipelines as dlt

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# -------------------------------------------------------------------------
# Dimensions
# -------------------------------------------------------------------------

COMPANY_METADATA = [
    ("AAPL", "Apple Inc.", "Technology", "Consumer Electronics", "NASDAQ"),
    ("NVDA", "NVIDIA Corporation", "Technology", "Semiconductors", "NASDAQ"),
    ("MSFT", "Microsoft Corporation", "Technology", "Software - Infrastructure", "NASDAQ"),
    ("AMZN", "Amazon.com Inc.", "Consumer Cyclical", "Internet Retail", "NASDAQ"),
    ("TSLA", "Tesla Inc.", "Consumer Cyclical", "Auto Manufacturers", "NASDAQ"),
    ("QQQ", "Invesco QQQ Trust", "Financial Services", "Exchange Traded Fund", "NASDAQ"),
]


@dlt.table(
    name="dim_company",
    comment="Company dimension with industry classifications and ticker surrogate keys",
    table_properties={"quality": "gold"}
)
def dim_company():
    df_prices = dlt.read("silver_nasdaq_price_5m")
    meta_df = spark.createDataFrame(
        COMPANY_METADATA,
        ["Symbol_ref", "CompanyName", "Sector", "Industry", "Exchange"]
    )

    return (
        df_prices
        .select("Symbol")
        .distinct()
        .join(meta_df, F.col("Symbol") == meta_df.Symbol_ref, "left")
        .withColumn("company_sk", F.abs(F.hash(F.col("Symbol"))))
        .withColumn("CompanyName", F.coalesce(F.col("CompanyName"), F.col("Symbol")))
        .withColumn("Sector", F.coalesce(F.col("Sector"), F.lit("Unknown")))
        .withColumn("Industry", F.coalesce(F.col("Industry"), F.lit("Unknown")))
        .withColumn("Exchange", F.coalesce(F.col("Exchange"), F.lit("NASDAQ")))
        .select("company_sk", "Symbol", "CompanyName", "Sector", "Industry", "Exchange")
    )


@dlt.table(
    name="dim_date",
    comment="Conformed calendar date dimension table",
    table_properties={"quality": "gold"}
)
def dim_date():
    df_prices = dlt.read("silver_nasdaq_price_5m")

    return (
        df_prices
        .select("TradeDate")
        .distinct()
        .withColumn("date_sk", F.date_format("TradeDate", "yyyyMMdd").cast("integer"))
        .withColumn("Year", F.year("TradeDate"))
        .withColumn("Quarter", F.quarter("TradeDate"))
        .withColumn("Month", F.month("TradeDate"))
        .withColumn("DayOfMonth", F.dayofmonth("TradeDate"))
        .withColumn("DayOfWeek", F.dayofweek("TradeDate"))
        .withColumn("DayName", F.date_format("TradeDate", "EEEE"))
        .withColumn("IsWeekend", F.when(F.dayofweek("TradeDate").isin(1, 7), F.lit(True)).otherwise(F.lit(False)))
        .select("date_sk", F.col("TradeDate").alias("CalendarDate"), "Year", "Quarter", "Month", "DayOfMonth", "DayOfWeek", "DayName", "IsWeekend")
    )


@dlt.table(
    name="dim_news_source",
    comment="News source publisher and editorial category dimension",
    table_properties={"quality": "gold"}
)
def dim_news_source():
    df_news = dlt.read("silver_finnhub_news")

    return (
        df_news
        .select("Source", "Category")
        .distinct()
        .withColumn("source_sk", F.abs(F.hash(F.concat(F.col("Source"), F.lit("_"), F.col("Category")))))
        .select("source_sk", F.col("Source").alias("SourceName"), "Category")
    )


# -------------------------------------------------------------------------
# Facts
# -------------------------------------------------------------------------

@dlt.table(
    name="fact_stock_prices_5m",
    comment="Fact table containing 5-minute stock prices for charting and overlays",
    table_properties={"quality": "gold"}
)
def fact_stock_prices_5m():
    df_prices = dlt.read("silver_nasdaq_price_5m")

    return (
        df_prices
        .withColumn("company_sk", F.abs(F.hash(F.col("Symbol"))))
        .withColumn("date_sk", F.date_format("TradeDate", "yyyyMMdd").cast("integer"))
        .select(
            "company_sk",
            "date_sk",
            "Symbol",
            "PriceTimestamp",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume"
        )
    )


@dlt.table(
    name="fact_news_impact",
    comment="Gold fact table evaluating multi-dimensional stock market reactions to news events",
    table_properties={"quality": "gold"}
)
def fact_news_impact():
    df_news = dlt.read("silver_finnhub_news")
    df_prices = dlt.read("silver_nasdaq_price_5m")

    # 1. Filter company news
    company_news = df_news.filter(F.col("Symbol") != "GENERAL")

    # 2. Compute 20-period baseline volume
    window_baseline = Window.partitionBy("Symbol").orderBy("PriceTimestamp").rowsBetween(-20, -1)
    prices_with_baseline = (
        df_prices
        .withColumn("BaselineAvgVolume", F.coalesce(F.avg("Volume").over(window_baseline), F.lit(10000.0)))
    )

    # 3. Benchmark QQQ prices
    qqq_prices = (
        prices_with_baseline
        .filter(F.col("Symbol") == "QQQ")
        .select(
            F.col("PriceTimestamp").alias("QQQ_Timestamp"),
            F.col("Close").alias("QQQ_Close")
        )
    )

    # 4. Snap pre-event price (closest price bar on or after news)
    window_pre = Window.partitionBy("ArticleId").orderBy("PriceTimestamp")
    join_pre = (
        company_news
        .join(
            prices_with_baseline,
            (company_news.Symbol == prices_with_baseline.Symbol) &
            (prices_with_baseline.PriceTimestamp >= company_news.NewsTimestamp),
            "left"
        )
        .withColumn("pre_rank", F.row_number().over(window_pre))
        .filter((F.col("pre_rank") == 1) | F.col("pre_rank").isNull())
        .select(
            company_news["*"],
            F.col("PriceTimestamp").alias("Pre_Timestamp"),
            F.col("Open").alias("Pre_Open"),
            F.col("Close").alias("Pre_Close"),
            F.col("BaselineAvgVolume")
        )
    )

    # 5. Snap post-event window price (30 minutes after Pre_Timestamp)
    window_post = Window.partitionBy("ArticleId").orderBy("PriceTimestamp")
    join_post = (
        join_pre
        .join(
            prices_with_baseline,
            (join_pre.Symbol == prices_with_baseline.Symbol) &
            (prices_with_baseline.PriceTimestamp >= F.from_unixtime(
                F.unix_timestamp(join_pre.Pre_Timestamp) + 1800
            )),
            "left"
        )
        .withColumn("post_rank", F.row_number().over(window_post))
        .filter((F.col("post_rank") == 1) | F.col("post_rank").isNull())
        .select(
            join_pre["*"],
            F.col("PriceTimestamp").alias("Post_Timestamp"),
            F.col("Close").alias("Post_Close"),
            F.col("High").alias("Post_High"),
            F.col("Low").alias("Post_Low"),
            F.col("Volume").alias("Post_Volume")
        )
    )

    # 6. Join QQQ Pre & Post Close
    join_qqq = (
        join_post
        .join(
            qqq_prices.withColumnRenamed("QQQ_Close", "QQQ_Pre_Close"),
            join_post.Pre_Timestamp == F.col("QQQ_Timestamp"),
            "left"
        )
        .drop("QQQ_Timestamp")
        .join(
            qqq_prices.withColumnRenamed("QQQ_Close", "QQQ_Post_Close"),
            join_post.Post_Timestamp == F.col("QQQ_Timestamp"),
            "left"
        )
        .drop("QQQ_Timestamp")
    )

    # 7. Evaluate Impact Metrics
    time_diff_hours = (F.unix_timestamp("Post_Timestamp") - F.unix_timestamp("Pre_Timestamp")) / 3600.0
    is_completed = (
        F.col("Pre_Close").isNotNull() &
        F.col("Post_Close").isNotNull() &
        (time_diff_hours <= 24.0)
    )

    stock_return = F.round(((F.col("Post_Close") - F.col("Pre_Close")) / F.col("Pre_Close")) * 100.0, 4)
    qqq_return = F.when(
        F.col("QQQ_Pre_Close").isNotNull() & F.col("QQQ_Post_Close").isNotNull(),
        F.round(((F.col("QQQ_Post_Close") - F.col("QQQ_Pre_Close")) / F.col("QQQ_Pre_Close")) * 100.0, 4)
    ).otherwise(F.lit(0.0))

    alpha_return = F.round(stock_return - qqq_return, 4)
    intraday_volatility = F.round(((F.col("Post_High") - F.col("Post_Low")) / F.col("Pre_Close")) * 100.0, 4)
    volume_surge = F.round(F.col("Post_Volume") / F.greatest(F.col("BaselineAvgVolume"), F.lit(1.0)), 2)

    raw_score = (
        (alpha_return * 20.0) +
        F.when(alpha_return >= 0, (F.least(volume_surge, F.lit(5.0)) - 1.0) * 8.0)
        .otherwise(-((F.least(volume_surge, F.lit(5.0)) - 1.0) * 8.0)) +
        F.when(alpha_return >= 0, intraday_volatility * 10.0)
        .otherwise(-(intraday_volatility * 10.0))
    )
    composite_score = F.round(F.greatest(F.lit(-100.0), F.least(F.lit(100.0), raw_score)), 2)

    reaction_pattern = (
        F.when(F.abs(alpha_return) >= 0.4, F.lit("Sustained Move"))
        .when((intraday_volatility >= 0.5) & (F.abs(alpha_return) < 0.2), F.lit("Reversal/Fade"))
        .otherwise(F.lit("Flat Noise"))
    )

    return (
        join_qqq
        .withColumn("company_sk", F.abs(F.hash(F.col("Symbol"))))
        .withColumn("date_sk", F.date_format("NewsDate", "yyyyMMdd").cast("integer"))
        .withColumn("source_sk", F.abs(F.hash(F.concat(F.col("Source"), F.lit("_"), F.col("Category")))))
        .withColumn("ImpactStatus", F.when(is_completed, F.lit("COMPLETED")).otherwise(F.lit("PENDING_EVALUATION")))
        .withColumn("PrePrice", F.when(is_completed, F.col("Pre_Close")).otherwise(F.lit(None).cast("double")))
        .withColumn("PostPrice", F.when(is_completed, F.col("Post_Close")).otherwise(F.lit(None).cast("double")))
        .withColumn("StockReturnPct", F.when(is_completed, stock_return).otherwise(F.lit(None).cast("double")))
        .withColumn("QQQReturnPct", F.when(is_completed, qqq_return).otherwise(F.lit(None).cast("double")))
        .withColumn("AlphaReturnPct", F.when(is_completed, alpha_return).otherwise(F.lit(None).cast("double")))
        .withColumn("IntradayVolatilityPct", F.when(is_completed, intraday_volatility).otherwise(F.lit(None).cast("double")))
        .withColumn("VolumeSurgeRatio", F.when(is_completed, volume_surge).otherwise(F.lit(None).cast("double")))
        .withColumn("CompositeImpactScore", F.when(is_completed, composite_score).otherwise(F.lit(None).cast("double")))
        .withColumn("ReactionPattern", F.when(is_completed, reaction_pattern).otherwise(F.lit("Pending Window")))
        .select(
            "ArticleId",
            "company_sk",
            "date_sk",
            "source_sk",
            "Symbol",
            "NewsTimestamp",
            "Headline",
            "Summary",
            "ImpactStatus",
            "PrePrice",
            "PostPrice",
            "StockReturnPct",
            "QQQReturnPct",
            "AlphaReturnPct",
            "IntradayVolatilityPct",
            "VolumeSurgeRatio",
            "CompositeImpactScore",
            "ReactionPattern",
            "Url"
        )
    )
