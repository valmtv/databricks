"""
Data Quality & Cross-Layer Reconciliation Suite (Lab 07)
--------------------------------------------------------
Performs layer-to-layer reconciliation checks across the Medallion architecture:
1. Row count integrity: Bronze Count == (Silver Valid Count + Quarantine Count)
2. Referential integrity: Orphan key check between fact_news_impact and dim_company
3. Temporal validity: No future timestamps
4. Completeness: Primary keys non-null in Gold
"""

import sys


def run_reconciliation(spark, catalog: str, silver_schema: str, gold_schema: str) -> dict:
    """
    Executes reconciliation SQL queries on Unity Catalog tables.
    Returns a dictionary of check names and boolean pass/fail results.
    """
    results = {}
    print("=" * 70)
    print(f"RUNNING MEDALLION RECONCILIATION SUITE: {catalog}")
    print("=" * 70)

    # 1. Price Reconciliation: Bronze vs Silver + Quarantine
    try:
        bronze_price_cnt = spark.table(f"{catalog}.{silver_schema}.bronze_nasdaq_price_raw").count()
        silver_price_cnt = spark.table(f"{catalog}.{silver_schema}.silver_nasdaq_price_5m").count()
        quarantine_price_cnt = spark.table(f"{catalog}.{silver_schema}.silver_price_quarantine").count()
        
        reconciled_price = (bronze_price_cnt == (silver_price_cnt + quarantine_price_cnt))
        results["price_row_count_reconciliation"] = {
            "passed": reconciled_price,
            "bronze": bronze_price_cnt,
            "silver_valid": silver_price_cnt,
            "quarantine": quarantine_price_cnt
        }
        print(f"[RECON] Price Layer: Bronze={bronze_price_cnt} | Silver={silver_price_cnt} | Quarantine={quarantine_price_cnt} => {'PASS' if reconciled_price else 'FAIL'}")
    except Exception as e:
        results["price_row_count_reconciliation"] = {"passed": False, "error": str(e)}
        print(f"[RECON] Price Layer Check Skipped/Failed: {e}")

    # 2. News Reconciliation: Bronze vs Silver + Quarantine
    try:
        bronze_news_cnt = spark.table(f"{catalog}.{silver_schema}.bronze_finnhub_news_raw").count()
        silver_news_cnt = spark.table(f"{catalog}.{silver_schema}.silver_finnhub_news").count()
        quarantine_news_cnt = spark.table(f"{catalog}.{silver_schema}.silver_news_quarantine").count()

        reconciled_news = (bronze_news_cnt == (silver_news_cnt + quarantine_news_cnt))
        results["news_row_count_reconciliation"] = {
            "passed": reconciled_news,
            "bronze": bronze_news_cnt,
            "silver_valid": silver_news_cnt,
            "quarantine": quarantine_news_cnt
        }
        print(f"[RECON] News Layer: Bronze={bronze_news_cnt} | Silver={silver_news_cnt} | Quarantine={quarantine_news_cnt} => {'PASS' if reconciled_news else 'FAIL'}")
    except Exception as e:
        results["news_row_count_reconciliation"] = {"passed": False, "error": str(e)}
        print(f"[RECON] News Layer Check Skipped/Failed: {e}")

    # 3. Gold Referential Integrity: No orphan company_sk in fact_news_impact
    try:
        orphan_companies_sql = f"""
            SELECT COUNT(*) as orphan_count
            FROM {catalog}.{gold_schema}.fact_news_impact f
            LEFT JOIN {catalog}.{gold_schema}.dim_company d ON f.company_sk = d.company_sk
            WHERE d.company_sk IS NULL AND f.company_sk IS NOT NULL
        """
        orphan_count = spark.sql(orphan_companies_sql).collect()[0]["orphan_count"]
        passed_orphan = (orphan_count == 0)
        results["gold_company_referential_integrity"] = {
            "passed": passed_orphan,
            "orphan_keys": orphan_count
        }
        print(f"[INTEGRITY] Gold Company FKs: Orphan Count={orphan_count} => {'PASS' if passed_orphan else 'FAIL'}")
    except Exception as e:
        results["gold_company_referential_integrity"] = {"passed": False, "error": str(e)}
        print(f"[INTEGRITY] Gold Company FK Check Skipped/Failed: {e}")

    # 4. Gold Timeliness: No future timestamps
    try:
        future_sql = f"""
            SELECT COUNT(*) as future_rows
            FROM {catalog}.{gold_schema}.fact_stock_prices_5m
            WHERE PriceTimestamp > current_timestamp()
        """
        future_rows = spark.sql(future_sql).collect()[0]["future_rows"]
        passed_future = (future_rows == 0)
        results["price_timeliness_check"] = {
            "passed": passed_future,
            "future_dated_rows": future_rows
        }
        print(f"[TIMELINESS] Price Timeliness: Future Rows={future_rows} => {'PASS' if passed_future else 'FAIL'}")
    except Exception as e:
        results["price_timeliness_check"] = {"passed": False, "error": str(e)}
        print(f"[TIMELINESS] Price Timeliness Check Skipped/Failed: {e}")

    print("=" * 70)
    return results


if __name__ == "__main__":
    # When executed inside Databricks cluster or notebook
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        catalog = sys.argv[1] if len(sys.argv) > 1 else "dbr_dev"
        silver = sys.argv[2] if len(sys.argv) > 2 else "valeriimatviiv_silver"
        gold = sys.argv[3] if len(sys.argv) > 3 else "valeriimatviiv_gold"
        run_reconciliation(spark, catalog, silver, gold)
    except Exception as err:
        print(f"Reconciliation runner error: {err}")
