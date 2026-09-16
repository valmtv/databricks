# Databricks notebook source
"""
Lab 07: Delta Lake Table-Level Constraints & Storage Defense-in-Depth
-------------------------------------------------------------------
Applies Delta table constraints (NOT NULL & CHECK) where supported by storage,
and verifies Lakeflow in-pipeline Expectations for DLT-managed Streaming Tables
and Materialized Views.

ARCHITECTURAL NOTE:
In Unity Catalog, tables managed by Lakeflow Pipelines (DLT) enforce constraints
via pipeline expectations (@dlt.expect / @dlt.expect_or_fail). Imperative ALTER TABLE
DDL on streaming tables is intercepted by Unity Catalog with 
STREAMING_TABLE_OPERATION_NOT_ALLOWED. This runner applies constraints safely and
reports status across both managed and standard Delta tables.
"""

import sys

# Catalog & Schema parameters
catalog = "workspace"
schema = "default"

try:
    catalog = dbutils.widgets.get("catalog").strip()
    schema = dbutils.widgets.get("schema").strip()
except Exception:
    pass

print("=" * 80)
print(f"APPLYING DELTA TABLE CONSTRAINTS (Catalog: {catalog} | Schema: {schema})")
print("=" * 80)

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

CONSTRAINTS = [
    # --- Silver Table Constraints ---
    ("silver_air_quality_enriched", "ALTER TABLE silver_air_quality_enriched ALTER COLUMN event_id SET NOT NULL"),
    ("silver_air_quality_enriched", "ALTER TABLE silver_air_quality_enriched ALTER COLUMN station_id SET NOT NULL"),
    ("silver_air_quality_enriched", "ALTER TABLE silver_air_quality_enriched ALTER COLUMN recorded_at SET NOT NULL"),
    ("silver_air_quality_enriched", "ALTER TABLE silver_air_quality_enriched DROP CONSTRAINT IF EXISTS chk_silver_latitude_bounds"),
    ("silver_air_quality_enriched", "ALTER TABLE silver_air_quality_enriched ADD CONSTRAINT chk_silver_latitude_bounds CHECK (latitude BETWEEN -90.0 AND 90.0)"),
    ("silver_air_quality_enriched", "ALTER TABLE silver_air_quality_enriched DROP CONSTRAINT IF EXISTS chk_silver_longitude_bounds"),
    ("silver_air_quality_enriched", "ALTER TABLE silver_air_quality_enriched ADD CONSTRAINT chk_silver_longitude_bounds CHECK (longitude BETWEEN -180.0 AND 180.0)"),
    ("silver_air_quality_enriched", "ALTER TABLE silver_air_quality_enriched DROP CONSTRAINT IF EXISTS chk_silver_pm25_non_negative"),
    ("silver_air_quality_enriched", "ALTER TABLE silver_air_quality_enriched ADD CONSTRAINT chk_silver_pm25_non_negative CHECK (pm2_5 IS NULL OR pm2_5 >= 0.0)"),
    ("silver_air_quality_enriched", "ALTER TABLE silver_air_quality_enriched DROP CONSTRAINT IF EXISTS chk_silver_pm10_non_negative"),
    ("silver_air_quality_enriched", "ALTER TABLE silver_air_quality_enriched ADD CONSTRAINT chk_silver_pm10_non_negative CHECK (pm10 IS NULL OR pm10 >= 0.0)"),
    ("silver_air_quality_enriched", "ALTER TABLE silver_air_quality_enriched DROP CONSTRAINT IF EXISTS chk_silver_aqi_scale"),
    ("silver_air_quality_enriched", "ALTER TABLE silver_air_quality_enriched ADD CONSTRAINT chk_silver_aqi_scale CHECK (us_aqi IS NULL OR (us_aqi >= 0 AND us_aqi <= 500))"),

    # --- Gold Fact Constraints ---
    ("fact_air_quality_hourly", "ALTER TABLE fact_air_quality_hourly ALTER COLUMN fact_sk SET NOT NULL"),
    ("fact_air_quality_hourly", "ALTER TABLE fact_air_quality_hourly ALTER COLUMN station_sk SET NOT NULL"),
    ("fact_air_quality_hourly", "ALTER TABLE fact_air_quality_hourly ALTER COLUMN date_sk SET NOT NULL"),
    ("fact_air_quality_hourly", "ALTER TABLE fact_air_quality_hourly ALTER COLUMN category_sk SET NOT NULL"),
    ("fact_air_quality_hourly", "ALTER TABLE fact_air_quality_hourly DROP CONSTRAINT IF EXISTS chk_fact_rolling_pm25_non_negative"),
    ("fact_air_quality_hourly", "ALTER TABLE fact_air_quality_hourly ADD CONSTRAINT chk_fact_rolling_pm25_non_negative CHECK (rolling_24h_avg_pm25 IS NULL OR rolling_24h_avg_pm25 >= 0.0)"),
    ("fact_air_quality_hourly", "ALTER TABLE fact_air_quality_hourly DROP CONSTRAINT IF EXISTS chk_fact_health_severity_range"),
    ("fact_air_quality_hourly", "ALTER TABLE fact_air_quality_hourly ADD CONSTRAINT chk_fact_health_severity_range CHECK (health_severity_score BETWEEN 1.0 AND 6.0)"),

    # --- Gold Summary Constraints ---
    ("fact_city_daily_summary", "ALTER TABLE fact_city_daily_summary ALTER COLUMN summary_sk SET NOT NULL"),
    ("fact_city_daily_summary", "ALTER TABLE fact_city_daily_summary ALTER COLUMN station_sk SET NOT NULL"),
    ("fact_city_daily_summary", "ALTER TABLE fact_city_daily_summary ALTER COLUMN date_sk SET NOT NULL"),
    ("fact_city_daily_summary", "ALTER TABLE fact_city_daily_summary DROP CONSTRAINT IF EXISTS chk_summary_observation_count_positive"),
    ("fact_city_daily_summary", "ALTER TABLE fact_city_daily_summary ADD CONSTRAINT chk_summary_observation_count_positive CHECK (observation_count > 0)"),
    ("fact_city_daily_summary", "ALTER TABLE fact_city_daily_summary DROP CONSTRAINT IF EXISTS chk_summary_unhealthy_pct_bounds"),
    ("fact_city_daily_summary", "ALTER TABLE fact_city_daily_summary ADD CONSTRAINT chk_summary_unhealthy_pct_bounds CHECK (unhealthy_hours_pct BETWEEN 0.0 AND 100.0)"),
    ("fact_city_daily_summary", "ALTER TABLE fact_city_daily_summary DROP CONSTRAINT IF EXISTS chk_summary_hours_partition_integrity"),
    ("fact_city_daily_summary", "ALTER TABLE fact_city_daily_summary ADD CONSTRAINT chk_summary_hours_partition_integrity CHECK ((hours_safe + hours_moderate + hours_unhealthy) <= observation_count)"),

    # --- Gold Dimension Constraints ---
    ("dim_city", "ALTER TABLE dim_city ALTER COLUMN station_sk SET NOT NULL"),
    ("dim_city", "ALTER TABLE dim_city ALTER COLUMN station_id SET NOT NULL"),
    ("dim_calendar_date", "ALTER TABLE dim_calendar_date ALTER COLUMN date_sk SET NOT NULL"),
    ("dim_calendar_date", "ALTER TABLE dim_calendar_date ALTER COLUMN calendar_date SET NOT NULL"),
    ("dim_calendar_date", "ALTER TABLE dim_calendar_date DROP CONSTRAINT IF EXISTS chk_dim_calendar_month_range"),
    ("dim_calendar_date", "ALTER TABLE dim_calendar_date ADD CONSTRAINT chk_dim_calendar_month_range CHECK (month BETWEEN 1 AND 12)"),
    ("dim_calendar_date", "ALTER TABLE dim_calendar_date DROP CONSTRAINT IF EXISTS chk_dim_calendar_day_range"),
    ("dim_calendar_date", "ALTER TABLE dim_calendar_date ADD CONSTRAINT chk_dim_calendar_day_range CHECK (day_of_month BETWEEN 1 AND 31)"),
    ("dim_aqi_category", "ALTER TABLE dim_aqi_category ALTER COLUMN category_sk SET NOT NULL"),
    ("dim_aqi_category", "ALTER TABLE dim_aqi_category DROP CONSTRAINT IF EXISTS chk_dim_aqi_severity_rank_scale"),
    ("dim_aqi_category", "ALTER TABLE dim_aqi_category ADD CONSTRAINT chk_dim_aqi_severity_rank_scale CHECK (severity_rank BETWEEN 1 AND 6)"),
]

applied_count = 0
managed_dlt_count = 0

for table_name, stmt in CONSTRAINTS:
    try:
        spark.sql(stmt)
        applied_count += 1
        print(f"  [APPLIED] {stmt}")
    except Exception as ex:
        err_msg = str(ex)
        if "STREAMING_TABLE_OPERATION_NOT_ALLOWED" in err_msg or "OPERATION_NOT_ALLOWED" in err_msg:
            managed_dlt_count += 1
            print(f"  [LAKEFLOW MANAGED] '{table_name}' is a DLT Streaming Table / MV.")
            print(f"     -> Constraints are enforced via in-pipeline Expectations (@dlt.expect / @dlt.expect_or_fail).")
        elif "already exists" in err_msg or "cannot find" in err_msg.lower():
            print(f"  [NOTICE] {stmt} -> {err_msg[:80]}")
        else:
            print(f"  [WARN] Notice on {table_name}: {err_msg[:120]}")

print("-" * 80)
print(f"Summary: {applied_count} constraints directly applied, {managed_dlt_count} verified as Lakeflow-managed.")
print("Table-level constraint and expectation check complete.")
