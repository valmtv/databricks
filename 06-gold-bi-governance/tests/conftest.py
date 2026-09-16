"""
Pytest and Test Configuration
-----------------------------
Supports:
1. Local lightweight execution (zero cloud cost, instant feedback).
2. Databricks Connect execution (remote execution against shared Azure or Serverless cluster).
"""

import os
import sys

# Add project root to sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def get_spark_session():
    """
    Returns an active SparkSession.
    Checks in order:
    1. Remote Databricks Connect session if DATABRICKS_CONNECT=true
    2. Existing PySpark session
    3. New local PySpark session
    Returns None if pyspark is not installed in the local environment.
    """
    if os.environ.get("DATABRICKS_CONNECT", "").lower() in ("true", "1"):
        try:
            from databricks.connect import DatabricksSession
            print("[INFO] Initializing Databricks Connect session to remote cluster...")
            return DatabricksSession.builder.getOrCreate()
        except ImportError:
            print("[WARN] databricks-connect not installed. Falling back to local PySpark.")

    try:
        from pyspark.sql import SparkSession
        return (
            SparkSession.builder
            .master("local[1]")
            .appName("air_quality_unit_tests")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
    except Exception as e:
        print(f"[NOTE] Local PySpark unavailable ({e}). Running in pure Python mode.")
        return None
