"""
News Impact Microstructure Evaluation Module
---------------------------------------------
Evaluates the multi-dimensional impact of news articles on 5-minute stock prices:
- Event window return (Pre vs Post 30-minute / 60-minute window)
- QQQ Benchmark-adjusted Alpha return
- Intraday Volatility range (High - Low)
- Volume surge multiplier vs baseline
- Trajectory pattern classification ('Sustained Move', 'Reversal/Fade', 'Flat Noise')
- Indefinite / Pending evaluation state handling for off-hours and incomplete windows.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def evaluate_news_impact(
    df_news: DataFrame,
    df_prices: DataFrame,
    window_minutes: int = 30
) -> DataFrame:
    """
    Computes news impact metrics by joining news events with 5-minute price bars.
    News published off-hours or without elapsed post-window prices are tagged as PENDING_EVALUATION.
    """
    # 1. Filter out general market news (keep company-specific tickers)
    company_news = (
        df_news
        .filter((F.col("Symbol") != "GENERAL") & F.col("Symbol").isNotNull())
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
    )

    # 2. Compute 20-period baseline volume for each symbol
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

    # 4. Snap pre-event price: Find the closest price bar on or immediately after NewsTimestamp
    # (If news arrived at 10:12 AM, snap to 10:15 bar or 10:10 bar)
    # Using window forward snap to first available price bar
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

    # 5. Snap post-event window price: target timestamp = Pre_Timestamp + window_minutes
    # Target window is [Pre_Timestamp, Pre_Timestamp + window_minutes]
    # Find price at or after Pre_Timestamp + window_minutes
    window_post = Window.partitionBy("ArticleId").orderBy("PriceTimestamp")

    join_post = (
        join_pre
        .join(
            prices_with_baseline,
            (join_pre.Symbol == prices_with_baseline.Symbol) &
            (prices_with_baseline.PriceTimestamp >= F.from_unixtime(
                F.unix_timestamp(join_pre.Pre_Timestamp) + (window_minutes * 60)
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

    # 6. Join Benchmark QQQ Pre and Post Close
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

    # 7. Evaluate Impact Metrics or Mark as PENDING
    # Condition for completion: Both Pre_Close and Post_Close exist, and time difference is reasonable (< 24h)
    time_diff_hours = (F.unix_timestamp("Post_Timestamp") - F.unix_timestamp("Pre_Timestamp")) / 3600.0

    is_completed = (
        F.col("Pre_Close").isNotNull() &
        F.col("Post_Close").isNotNull() &
        (time_diff_hours <= 24.0)
    )

    # Return calculations
    stock_return = F.round(((F.col("Post_Close") - F.col("Pre_Close")) / F.col("Pre_Close")) * 100.0, 4)
    qqq_return = F.when(
        F.col("QQQ_Pre_Close").isNotNull() & F.col("QQQ_Post_Close").isNotNull(),
        F.round(((F.col("QQQ_Post_Close") - F.col("QQQ_Pre_Close")) / F.col("QQQ_Pre_Close")) * 100.0, 4)
    ).otherwise(F.lit(0.0))

    alpha_return = F.round(stock_return - qqq_return, 4)
    intraday_volatility = F.round(((F.col("Post_High") - F.col("Post_Low")) / F.col("Pre_Close")) * 100.0, 4)
    volume_surge = F.round(F.col("Post_Volume") / F.greatest(F.col("BaselineAvgVolume"), F.lit(1.0)), 2)

    # Composite impact score: -100 to +100
    raw_score = (
        (alpha_return * 20.0) +
        F.when(alpha_return >= 0, (F.least(volume_surge, F.lit(5.0)) - 1.0) * 8.0)
        .otherwise(-((F.least(volume_surge, F.lit(5.0)) - 1.0) * 8.0)) +
        F.when(alpha_return >= 0, intraday_volatility * 10.0)
        .otherwise(-(intraday_volatility * 10.0))
    )
    composite_score = F.round(F.greatest(F.lit(-100.0), F.least(F.lit(100.0), raw_score)), 2)

    # Reaction pattern classification
    reaction_pattern = (
        F.when(F.abs(alpha_return) >= 0.4, F.lit("Sustained Move"))
        .when((intraday_volatility >= 0.5) & (F.abs(alpha_return) < 0.2), F.lit("Reversal/Fade"))
        .otherwise(F.lit("Flat Noise"))
    )

    result = (
        join_qqq
        .withColumn("ImpactStatus", F.when(is_completed, F.lit("COMPLETED")).otherwise(F.lit("PENDING_EVALUATION")))
        .withColumn("EvaluationDate", F.coalesce(F.to_date(F.col("Post_Timestamp")), F.col("NewsDate")))
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
            "Symbol",
            "NewsDate",
            "NewsTimestamp",
            "Headline",
            "Summary",
            "Source",
            "Category",
            "Url",
            "ImpactStatus",
            "EvaluationDate",
            "PrePrice",
            "PostPrice",
            "StockReturnPct",
            "QQQReturnPct",
            "AlphaReturnPct",
            "IntradayVolatilityPct",
            "VolumeSurgeRatio",
            "CompositeImpactScore",
            "ReactionPattern"
        )
    )

    return result
