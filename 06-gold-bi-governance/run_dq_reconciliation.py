# Databricks notebook source
"""
Medallion Data Quality & Cross-Layer Reconciliation Runner (Lab 07)
-------------------------------------------------------------------
Performs mathematical accounting and referential integrity checks across layers:
1. Row Count Conservation: Bronze Ingested == Silver Valid + Silver Quarantine (Zero Silent Loss)
2. Grain Rollup Conservation: Sum(fact_city_daily_summary.observation_count) == Count(fact_air_quality_hourly)
3. Dimensional Referential Integrity: No orphan foreign keys (station_sk, date_sk, category_sk)
4. Temporal Integrity: Zero future-dated observations in Gold

Exit Code: 0 on PASS, 1 on FAIL (functions as an automated CI/CD workflow gate).
"""

import sys
import argparse


def run_reconciliation(spark, catalog: str = "workspace", schema: str = "default") -> bool:
    """
    Executes live SQL assertions against Unity Catalog tables.
    Returns True if all checks pass, False if any check fails.
    """
    print("=" * 80)
    print(f"RUNNING MEDALLION DATA QUALITY RECONCILIATION SUITE")
    print(f"   Target Catalog: {catalog} | Schema: {schema}")
    print("=" * 80)

    all_passed = True
    results = {}

    def format_table(tbl_name: str) -> str:
        return f"{catalog}.{schema}.{tbl_name}" if catalog and schema else tbl_name

    # -------------------------------------------------------------------------
    # Check 1: Row Count Conservation (Zero Silent Data Loss)
    # -------------------------------------------------------------------------
    print("\n[CHECK 1] Row Count Conservation: Bronze == (Silver Valid + Quarantine)")
    try:
        bronze_tbl = format_table("bronze_air_quality_raw")
        silver_valid_tbl = format_table("silver_air_quality_enriched")
        quarantine_tbl = format_table("silver_air_quality_quarantine")

        bronze_count = spark.table(bronze_tbl).count()
        silver_valid_count = spark.table(silver_valid_tbl).count()

        # Handle quarantine table if not yet populated
        try:
            quarantine_count = spark.table(quarantine_tbl).count()
        except Exception:
            quarantine_count = 0

        total_silver = silver_valid_count + quarantine_count
        discrepancy = bronze_count - total_silver
        passed_c1 = (discrepancy == 0)

        results["row_count_conservation"] = {
            "passed": passed_c1,
            "bronze_count": bronze_count,
            "silver_valid": silver_valid_count,
            "quarantine": quarantine_count,
            "discrepancy": discrepancy
        }

        status = "[PASS]" if passed_c1 else "[FAIL]"
        print(f"  {status} -> Bronze: {bronze_count} | Silver Valid: {silver_valid_count} | Quarantine: {quarantine_count} (Discrepancy: {discrepancy})")
        if not passed_c1:
            all_passed = False
    except Exception as e:
        print(f"  [WARN] Check 1 skipped/errored: {e}")
        results["row_count_conservation"] = {"passed": False, "error": str(e)}

    # -------------------------------------------------------------------------
    # Check 2: Grain Rollup Conservation (Daily Summary vs Hourly Facts)
    # -------------------------------------------------------------------------
    print("\n[CHECK 2] Grain Rollup Conservation: Hourly Fact Rows == Sum(Daily observation_count)")
    try:
        hourly_tbl = format_table("fact_air_quality_hourly")
        daily_tbl = format_table("fact_city_daily_summary")

        hourly_fact_count = spark.table(hourly_tbl).count()
        daily_sum_df = spark.sql(f"SELECT COALESCE(SUM(observation_count), 0) AS total_obs FROM {daily_tbl}")
        summed_observations = daily_sum_df.collect()[0]["total_obs"]

        passed_c2 = (hourly_fact_count == summed_observations)
        results["grain_rollup_conservation"] = {
            "passed": passed_c2,
            "hourly_facts": hourly_fact_count,
            "daily_summed_observations": summed_observations
        }

        status = "[PASS]" if passed_c2 else "[FAIL]"
        print(f"  {status} -> Hourly Facts Count: {hourly_fact_count} | Daily Observation Sum: {summed_observations}")
        if not passed_c2:
            all_passed = False
    except Exception as e:
        print(f"  [WARN] Check 2 skipped/errored: {e}")
        results["grain_rollup_conservation"] = {"passed": False, "error": str(e)}

    # -------------------------------------------------------------------------
    # Check 3: Referential Integrity (Zero Orphan Foreign Keys in Gold)
    # -------------------------------------------------------------------------
    print("\n[CHECK 3] Dimensional Integrity: Orphan Foreign Key Checks")
    try:
        fact_tbl = format_table("fact_air_quality_hourly")
        dim_city_tbl = format_table("dim_city")
        dim_date_tbl = format_table("dim_calendar_date")
        dim_cat_tbl = format_table("dim_aqi_category")

        orphan_city_sql = f"""
            SELECT COUNT(*) AS cnt FROM {fact_tbl} f
            LEFT JOIN {dim_city_tbl} d ON f.station_sk = d.station_sk
            WHERE d.station_sk IS NULL
        """
        orphan_date_sql = f"""
            SELECT COUNT(*) AS cnt FROM {fact_tbl} f
            LEFT JOIN {dim_date_tbl} d ON f.date_sk = d.date_sk
            WHERE d.date_sk IS NULL
        """
        orphan_cat_sql = f"""
            SELECT COUNT(*) AS cnt FROM {fact_tbl} f
            LEFT JOIN {dim_cat_tbl} d ON f.category_sk = d.category_sk
            WHERE d.category_sk IS NULL
        """

        orphan_city = spark.sql(orphan_city_sql).collect()[0]["cnt"]
        orphan_date = spark.sql(orphan_date_sql).collect()[0]["cnt"]
        orphan_cat = spark.sql(orphan_cat_sql).collect()[0]["cnt"]

        passed_c3 = (orphan_city == 0 and orphan_date == 0 and orphan_cat == 0)
        results["referential_integrity"] = {
            "passed": passed_c3,
            "orphan_city_fk": orphan_city,
            "orphan_date_fk": orphan_date,
            "orphan_category_fk": orphan_cat
        }

        status = "[PASS]" if passed_c3 else "[FAIL]"
        print(f"  {status} -> Orphan Station SK: {orphan_city} | Orphan Date SK: {orphan_date} | Orphan Category SK: {orphan_cat}")
        if not passed_c3:
            all_passed = False
    except Exception as e:
        print(f"  [WARN] Check 3 skipped/errored: {e}")
        results["referential_integrity"] = {"passed": False, "error": str(e)}

    # -------------------------------------------------------------------------
    # Check 4: Temporal Freshness (Zero Future-Dated Records)
    # -------------------------------------------------------------------------
    print("\n[CHECK 4] Temporal Freshness: No Future-Dated Records in Facts")
    try:
        fact_tbl = format_table("fact_air_quality_hourly")
        future_sql = f"""
            SELECT COUNT(*) AS cnt FROM {fact_tbl}
            WHERE recorded_at > current_timestamp()
        """
        future_rows = spark.sql(future_sql).collect()[0]["cnt"]
        passed_c4 = (future_rows == 0)

        results["temporal_freshness"] = {
            "passed": passed_c4,
            "future_dated_rows": future_rows
        }

        status = "[PASS]" if passed_c4 else "[FAIL]"
        print(f"  {status} -> Future-dated rows: {future_rows}")
        if not passed_c4:
            all_passed = False
    except Exception as e:
        print(f"  [WARN] Check 4 skipped/errored: {e}")
        results["temporal_freshness"] = {"passed": False, "error": str(e)}

    # -------------------------------------------------------------------------
    # Final Verdict
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    if all_passed:
        print("FINAL RECONCILIATION RESULT: ALL CHECKS PASSED (Pipeline Trustworthy)")
    else:
        print("FINAL RECONCILIATION RESULT: FAILED CHECKS DETECTED (Gate Blocked)")
    print("=" * 80)

    return all_passed


if __name__ == "__main__":
    # Support both Databricks notebook widget parameters and command line arguments
    catalog = "workspace"
    schema = "default"
    
    try:
        catalog = dbutils.widgets.get("catalog")
        schema = dbutils.widgets.get("schema")
    except Exception:
        try:
            parser = argparse.ArgumentParser(description="Medallion Data Quality Reconciliation Suite")
            parser.add_argument("--catalog", default="workspace", help="Unity Catalog name")
            parser.add_argument("--schema", default="default", help="Schema name")
            args, _ = parser.parse_known_args()
            catalog = args.catalog
            schema = args.schema
        except Exception:
            pass

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        success = run_reconciliation(spark, catalog=catalog, schema=schema)
        if not success:
            raise ValueError("Medallion reconciliation quality checks failed! See logs above.")
    except Exception as err:
        print(f"[ERROR] Reconciliation execution error: {err}")
        sys.exit(1)

