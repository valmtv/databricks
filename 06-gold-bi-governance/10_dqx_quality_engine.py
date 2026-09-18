# Databricks notebook source
# MAGIC %pip install -q -U "typing-extensions>=4.14.0" "protobuf>=4.25.8,<5" "databricks-labs-dqx>=0.8.0" pyyaml

# COMMAND ----------

"""
Databricks Labs DQX Quality Engine Runner (Lab 07)
--------------------------------------------------
Implements automated Data Quality profiling and rule execution via declarative 
Databricks Labs DQX (databricks-labs-dqx) syntax across all 5 Data Quality dimensions:
1. Completeness  (Missing event_id, station_id, city, recorded_at)
2. Validity      (Latitude [-90..90], Longitude [-180..180], non-negative pollutants, AQI [0..500])
3. Timeliness    (No future-dated timestamps)
4. Consistency   (Dimensional referential integrity FK & daily hours rollups)
5. Uniqueness    (Primary / surrogate key uniqueness on fact_sk)
"""

import os
import sys
import json
import traceback

# Prioritize notebook-installed packages over older Databricks system libraries
for p in list(sys.path):
    if "site-packages" in p and "databricks/python" not in p:
        if p in sys.path:
            sys.path.remove(p)
        sys.path.insert(0, p)

if "typing_extensions" in sys.modules:
    del sys.modules["typing_extensions"]

try:
    import yaml
except ImportError:
    yaml = None

IMPORT_ERROR = None
try:
    from databricks.sdk import WorkspaceClient
    from databricks.labs.dqx.engine import DQEngine
    from databricks.labs.dqx.metrics_observer import DQMetricsObserver
    from databricks.labs.dqx.rule import DQRowRule, DQDatasetRule
    from databricks.labs.dqx.check_funcs import (
        is_not_null,
        is_not_null_and_not_empty,
        is_in_range,
        is_not_in_future,
        is_unique,
        sql_expression,
    )
    NATIVE_DQX_AVAILABLE = True
except Exception as e:
    NATIVE_DQX_AVAILABLE = False
    IMPORT_ERROR = traceback.format_exc()

from pyspark.sql import functions as F

CREATE_WIDGETS = True
if CREATE_WIDGETS:
    try:
        dbutils.widgets.text("catalog", "workspace", "Catalog")
        dbutils.widgets.text("schema", "default", "Schema")
    except Exception:
        pass


def run_dqx_evaluation(spark, catalog: str = "workspace", schema: str = "default", rules_path: str = "dq_rules.yml") -> dict:
    """
    Executes Databricks Labs DQX quality suite against Silver and Gold Medallion tables.
    Uses native databricks.labs.dqx.engine.DQEngine when available, with resilient fallback.
    Output is concise and high-signal (no verbose console spam).
    """
    if NATIVE_DQX_AVAILABLE:
        print(f"[INFO] Initialized Databricks Labs DQX Engine (Native databricks.labs.dqx | Catalog: {catalog}.{schema})")
        return _run_native_dqx(spark, catalog, schema)
    else:
        print(f"[INFO] Initialized DQX Declarative Evaluator (Fallback mode | Catalog: {catalog}.{schema})")
        if IMPORT_ERROR:
            print(f"[WARN] Native DQX load error: {IMPORT_ERROR.splitlines()[-1] if IMPORT_ERROR else 'Unknown'}")
        return _run_fallback_dqx(spark, catalog, schema, rules_path)


def _run_native_dqx(spark, catalog: str, schema: str) -> dict:
    """Evaluates rules using native Databricks Labs DQX engine."""
    ws_client = WorkspaceClient()
    observer = DQMetricsObserver()
    engine = DQEngine(ws_client, spark=spark, observer=observer)

    # 1. Silver Layer Checks
    silver_table_name = f"{catalog}.{schema}.silver_air_quality_enriched"
    df_silver = spark.table(silver_table_name)
    silver_total = df_silver.count()

    silver_checks = [
        DQRowRule(name="completeness_event_id", criticality="error", check_func=is_not_null_and_not_empty, column="event_id"),
        DQRowRule(name="completeness_station_id", criticality="error", check_func=is_not_null_and_not_empty, column="station_id"),
        DQRowRule(name="completeness_city", criticality="error", check_func=is_not_null_and_not_empty, column="city"),
        DQRowRule(name="completeness_recorded_at", criticality="error", check_func=is_not_null, column="recorded_at"),
        DQRowRule(name="validity_latitude_bounds", criticality="error", check_func=is_in_range, column="latitude", check_func_kwargs={"min_limit": -90.0, "max_limit": 90.0}),
        DQRowRule(name="validity_longitude_bounds", criticality="error", check_func=is_in_range, column="longitude", check_func_kwargs={"min_limit": -180.0, "max_limit": 180.0}),
        DQRowRule(name="validity_pm25_non_negative", criticality="warn", check_func=sql_expression, check_func_kwargs={"expression": "pm2_5 IS NULL OR pm2_5 >= 0.0"}),
        DQRowRule(name="validity_pm10_non_negative", criticality="warn", check_func=sql_expression, check_func_kwargs={"expression": "pm10 IS NULL OR pm10 >= 0.0"}),
        DQRowRule(name="validity_us_aqi_scale", criticality="warn", check_func=sql_expression, check_func_kwargs={"expression": "us_aqi IS NULL OR (us_aqi >= 0 AND us_aqi <= 500)"}),
        DQRowRule(name="timeliness_future_dated_check", criticality="warn", check_func=is_not_in_future, column="recorded_at"),
    ]

    result_silver = engine.apply_checks(df_silver, silver_checks)
    val_silver_df = result_silver[0] if isinstance(result_silver, tuple) else result_silver

    silver_error_count = val_silver_df.filter(F.size(F.col("_errors")) > 0).count()
    silver_warn_count = val_silver_df.filter(F.size(F.col("_warnings")) > 0).count()

    print(f"[INFO] Silver ({silver_table_name}): {silver_total:,} rows evaluated across {len(silver_checks)} DQX rules -> {silver_error_count} errors, {silver_warn_count} warnings.")

    # 2. Gold Dimensional Checks (Uniqueness & Referential Integrity)
    gold_fact_name = f"{catalog}.{schema}.fact_air_quality_hourly"
    df_gold_fact = spark.table(gold_fact_name)
    gold_total = df_gold_fact.count()

    gold_checks = [
        DQDatasetRule(name="uniqueness_fact_sk", criticality="error", check_func=is_unique, columns=["fact_sk"]),
    ]
    result_gold = engine.apply_checks(df_gold_fact, gold_checks)
    val_gold_df = result_gold[0] if isinstance(result_gold, tuple) else result_gold
    gold_uniqueness_errors = val_gold_df.filter(F.size(F.col("_errors")) > 0).count()

    # Foreign Key integrity check
    dim_city_name = f"{catalog}.{schema}.dim_city"
    df_dim_city = spark.table(dim_city_name)
    orphan_stations = df_gold_fact.join(df_dim_city, "station_sk", "left_anti").count()

    # Rollup partition consistency on daily summary
    summary_name = f"{catalog}.{schema}.fact_city_daily_summary"
    df_summary = spark.table(summary_name)
    summary_total = df_summary.count()
    partition_violations = df_summary.filter(~F.expr("(hours_safe + hours_moderate + hours_unhealthy) <= observation_count")).count()

    print(f"[INFO] Gold ({gold_fact_name}): {gold_total:,} rows -> Uniqueness violations: {gold_uniqueness_errors}, Orphan FKs: {orphan_stations}.")
    print(f"[INFO] Gold Summary ({summary_name}): {summary_total:,} rows -> Partition integrity violations: {partition_violations}.")

    # Compute summary metrics table if running in notebook
    try:
        summary_df = engine.compute_summary_metrics(val_silver_df)
        if "display" in globals():
            display(summary_df)
    except Exception:
        pass

    total_critical_errors = silver_error_count + gold_uniqueness_errors + orphan_stations + partition_violations
    if total_critical_errors > 0:
        raise RuntimeError(
            f"DQX Quality Gate FAILED: {total_critical_errors} critical failure(s) detected "
            f"(Silver errors: {silver_error_count}, Gold duplicates: {gold_uniqueness_errors}, "
            f"Orphan FKs: {orphan_stations}, Summary partition violations: {partition_violations})."
        )

    print(f"[SUCCESS] All DQX quality gates passed (0 errors across 13 rules in 5 dimensions).")
    return {
        "status": "SUCCESS",
        "native_dqx_available": True,
        "import_error": None,
        "rules_evaluated": len(silver_checks) + 3,
        "quarantine_candidates": silver_warn_count,
        "critical_failures": 0,
    }


def _run_fallback_dqx(spark, catalog: str, schema: str, rules_path: str) -> dict:
    """Fallback evaluator when databricks.labs.dqx is not installed."""
    # Resilient fallback logic matching the declarative YAML rules
    silver_table_name = f"{catalog}.{schema}.silver_air_quality_enriched"
    gold_fact_name = f"{catalog}.{schema}.fact_air_quality_hourly"
    dim_city_name = f"{catalog}.{schema}.dim_city"
    summary_name = f"{catalog}.{schema}.fact_city_daily_summary"

    df_silver = spark.table(silver_table_name)
    df_gold_fact = spark.table(gold_fact_name)
    df_dim_city = spark.table(dim_city_name)
    df_summary = spark.table(summary_name)

    silver_total = df_silver.count()
    gold_total = df_gold_fact.count()

    # Silver completeness & validity
    silver_errors = df_silver.filter(
        F.expr("event_id IS NULL OR trim(event_id) = '' OR station_id IS NULL OR trim(station_id) = '' OR "
               "city IS NULL OR trim(city) = '' OR recorded_at IS NULL OR "
               "latitude < -90.0 OR latitude > 90.0 OR longitude < -180.0 OR longitude > 180.0")
    ).count()

    silver_warns = df_silver.filter(
        F.expr("(pm2_5 IS NOT NULL AND pm2_5 < 0.0) OR (pm10 IS NOT NULL AND pm10 < 0.0) OR "
               "(us_aqi IS NOT NULL AND (us_aqi < 0 OR us_aqi > 500)) OR recorded_at > current_timestamp()")
    ).count()

    # Gold uniqueness & referential integrity
    distinct_fact_sk = df_gold_fact.select("fact_sk").distinct().count()
    gold_duplicates = gold_total - distinct_fact_sk
    orphan_stations = df_gold_fact.join(df_dim_city, "station_sk", "left_anti").count()
    partition_violations = df_summary.filter(~F.expr("(hours_safe + hours_moderate + hours_unhealthy) <= observation_count")).count()

    print(f"[INFO] Silver ({silver_table_name}): {silver_total:,} rows -> {silver_errors} errors, {silver_warns} warnings.")
    print(f"[INFO] Gold ({gold_fact_name}): {gold_total:,} rows -> {gold_duplicates} duplicate keys, {orphan_stations} orphan FKs.")
    print(f"[INFO] Gold Summary ({summary_name}): {df_summary.count():,} rows -> {partition_violations} partition violations.")

    total_critical = silver_errors + gold_duplicates + orphan_stations + partition_violations
    if total_critical > 0:
        raise RuntimeError(f"DQX Quality Gate FAILED (Fallback): {total_critical} critical violation(s).")

    print(f"[SUCCESS] All DQX quality gates passed (0 errors across 13 rules in 5 dimensions).")
    return {
        "status": "SUCCESS",
        "native_dqx_available": False,
        "import_error": IMPORT_ERROR,
        "rules_evaluated": 13,
        "quarantine_candidates": silver_warns,
        "critical_failures": 0,
    }


if __name__ == "__main__":
    catalog = "workspace"
    schema = "default"
    try:
        catalog = dbutils.widgets.get("catalog").strip()
        schema = dbutils.widgets.get("schema").strip()
    except Exception:
        pass

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    eval_result = run_dqx_evaluation(spark, catalog=catalog, schema=schema)
    try:
        dbutils.notebook.exit(json.dumps(eval_result))
    except Exception:
        pass
