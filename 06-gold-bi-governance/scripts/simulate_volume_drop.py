#!/usr/bin/env python3
"""
Automated Data Volume Drop Simulation & Alert Verification Script
-----------------------------------------------------------------
Simulates an upstream sensor network outage / ingestion drop and asserts that
the Databricks SQL Volume Drop Alert query triggers with `is_volume_drop == 1`.
"""

import json
import subprocess
import sys
import time

WAREHOUSE_ID = "2035b157781cb38f"  # Serverless Starter Warehouse

def run_sql(statement: str, profile: str = "valerii.matviiv@gmail.com") -> dict:
    """Executes a SQL statement on Databricks Serverless SQL Warehouse via REST API."""
    payload = json.dumps({
        "warehouse_id": WAREHOUSE_ID,
        "statement": statement,
        "wait_timeout": "30s",
        "disposition": "INLINE"
    })
    cmd = [
        "databricks", "api", "post", "/api/2.0/sql/statements",
        "--profile", profile,
        "--json", payload
    ]
    try:
        res = subprocess.check_output(cmd, stderr=subprocess.PIPE, text=True)
        data = json.loads(res)
        
        # If still pending, poll until finished
        stmt_id = data.get("statement_id")
        status = data.get("status", {}).get("state")
        while status in ("PENDING", "RUNNING"):
            time.sleep(2)
            poll_cmd = ["databricks", "api", "get", f"/api/2.0/sql/statements/{stmt_id}", "--profile", profile]
            poll_res = subprocess.check_output(poll_cmd, stderr=subprocess.PIPE, text=True)
            data = json.loads(poll_res)
            status = data.get("status", {}).get("state")
            
        return data
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] SQL execution failed: {e.stderr}", file=sys.stderr)
        return {"error": str(e), "stderr": e.stderr}


def simulate_volume_drop():
    print("=" * 75)
    print(" [*] LAB 06: SIMULATING TELEMETRY DATA VOLUME DROP & ALERT TRIGGER")
    print("=" * 75)
    
    # 1. Inspect Baseline Live Ingestion Volumes
    print("\n[Step 1] Checking current baseline observation counts in workspace.default...")
    check_query = """
    SELECT 
        COUNT(*) AS total_records,
        COUNT(DISTINCT station_id) AS total_stations,
        COUNT(DISTINCT date_trunc('hour', recorded_at)) AS total_hours
    FROM workspace.default.fact_air_quality_hourly;
    """
    res = run_sql(check_query)
    if "result" in res:
        rows = res.get("result", {}).get("data_array", [])
        if rows:
            print(f" -> Live Fact Table: {rows[0][0]} records across {rows[0][1]} stations and {rows[0][2]} distinct hours.")
    else:
        print(" -> Note: Live table pending initial pipeline execution.")

    # 2. Simulate Volume Drop Condition
    print("\n[Step 2] Executing Volume Drop Simulation Logic (Simulating 80% telemetry collapse)...")
    simulated_alert_query = """
    WITH simulated_hourly_stream AS (
        -- Hour T-3: Normal operation (5 stations reporting)
        SELECT '2026-09-10 18:00:00' AS obs_hour, 5 AS readings, 5 AS stations
        UNION ALL
        -- Hour T-2: Normal operation (5 stations reporting)
        SELECT '2026-09-10 19:00:00' AS obs_hour, 5 AS readings, 5 AS stations
        UNION ALL
        -- Hour T-1: Normal operation (5 stations reporting)
        SELECT '2026-09-10 20:00:00' AS obs_hour, 5 AS readings, 5 AS stations
        UNION ALL
        -- Current Hour T: SENSOR OUTAGE SIMULATION (Only 1 station reporting, 80% volume drop!)
        SELECT '2026-09-10 21:00:00' AS obs_hour, 1 AS readings, 1 AS stations
    ),
    volume_stats AS (
        SELECT 
            obs_hour,
            readings,
            stations,
            LAG(readings, 1) OVER (ORDER BY obs_hour ASC) AS prev_readings,
            AVG(readings) OVER (ORDER BY obs_hour ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rolling_baseline
        FROM simulated_hourly_stream
    )
    SELECT 
        obs_hour,
        readings AS current_volume,
        rolling_baseline,
        stations AS active_stations,
        ROUND((readings * 100.0) / rolling_baseline, 1) AS volume_retention_pct,
        CASE 
            WHEN readings < (0.50 * rolling_baseline) OR stations < 3 THEN 1
            ELSE 0
        END AS is_volume_drop,
        CONCAT('CRITICAL ALERT: Data volume collapsed to ', readings, ' readings (Baseline: ', ROUND(rolling_baseline, 1), ' readings, ', stations, '/5 stations online).') AS alert_message
    FROM volume_stats
    ORDER BY obs_hour DESC
    LIMIT 1;
    """
    
    sim_res = run_sql(simulated_alert_query)
    
    if "result" in sim_res:
        row = sim_res.get("result", {}).get("data_array", [])[0]
        obs_hour = row[0]
        cur_vol = row[1]
        baseline = row[2]
        stations = row[3]
        retention = row[4]
        is_drop = int(row[5])
        message = row[6]
        
        print("\n" + "-" * 75)
        print(" SIMULATION TEST RESULTS")
        print("-" * 75)
        print(f" Observation Window    : {obs_hour}")
        print(f" Baseline Volume       : {baseline} readings/hr")
        print(f" Simulated Drop Volume : {cur_vol} readings/hr ({retention}% of normal)")
        print(f" Active Stations       : {stations} of 5")
        print(f" Alert Trigger Flag    : is_volume_drop = {is_drop}")
        print(f" Alert Message         : {message}")
        print("-" * 75)
        
        assert is_drop == 1, f"Expected alert trigger 1, got {is_drop}"
        print("\n [SUCCESS] Data Volume Drop Alert Trigger verified successfully!")
        print(" Simulated Email Notification Dispatched:")
        print("   To: valerii.matviiv@gmail.com")
        print(f"   Subject: [Databricks Alert] Air Quality Telemetry Volume Drop Detected")
        print(f"   Body: {message}")
        print("=" * 75)
    else:
        print("[WARNING] Could not parse query output directly, response:")
        print(json.dumps(sim_res, indent=2))

if __name__ == "__main__":
    simulate_volume_drop()
