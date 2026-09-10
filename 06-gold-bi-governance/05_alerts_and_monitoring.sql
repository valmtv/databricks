-- Databricks notebook source
-- =============================================================================
-- Automated Data Quality, Volume Drop & Environmental Hazard Alerts
-- =============================================================================

USE CATALOG workspace;
USE SCHEMA default;

-- -----------------------------------------------------------------------------
-- ALERT 1: Telemetry Data Volume Drop Alert (Ingestion Pipeline Health)
-- Business Objective: Immediately notify data engineering when sensor readings drop
-- by > 50% or if stations stop reporting (simulating network or upstream API failure).
-- Trigger Condition: is_volume_drop == 1
-- -----------------------------------------------------------------------------

WITH hourly_station_volume AS (
    SELECT 
        date_trunc('hour', recorded_at) AS observation_hour,
        COUNT(*) AS hourly_reading_count,
        COUNT(DISTINCT station_id) AS active_stations
    FROM fact_air_quality_hourly
    GROUP BY 1
),
volume_lagged AS (
    SELECT 
        observation_hour,
        hourly_reading_count,
        active_stations,
        LAG(hourly_reading_count, 1) OVER (ORDER BY observation_hour ASC) AS prev_hour_count,
        AVG(hourly_reading_count) OVER (ORDER BY observation_hour ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rolling_baseline_count
    FROM hourly_station_volume
)
SELECT 
    observation_hour,
    hourly_reading_count,
    active_stations,
    COALESCE(prev_hour_count, hourly_reading_count) AS prev_hour_count,
    ROUND(rolling_baseline_count, 1) AS rolling_baseline_count,
    CASE 
        -- Trigger when volume drops below 50% of rolling baseline OR when active stations < 3
        WHEN rolling_baseline_count IS NOT NULL AND hourly_reading_count < (0.50 * rolling_baseline_count) THEN 1
        WHEN active_stations < 3 THEN 1
        ELSE 0
    END AS is_volume_drop,
    CASE 
        WHEN rolling_baseline_count IS NOT NULL AND hourly_reading_count < (0.50 * rolling_baseline_count) 
            THEN CONCAT('ALERT: Data volume collapsed to ', hourly_reading_count, ' readings (Baseline: ', ROUND(rolling_baseline_count, 1), ')')
        WHEN active_stations < 3 
            THEN CONCAT('CRITICAL: Only ', active_stations, ' station(s) active out of 5 expected.')
        ELSE 'NORMAL: Ingestion volume within acceptable operational thresholds.'
    END AS alert_message
FROM volume_lagged
ORDER BY observation_hour DESC
LIMIT 1;


-- -----------------------------------------------------------------------------
-- ALERT 2: Severe Pollution & Public Health Hazard Alert
-- Business Objective: Trigger high-priority alert when any metropolitan area
-- enters 'Unhealthy' (AQI > 150) or PM2.5 exceeds EPA 24h hazard limits (35 ug/m3).
-- Trigger Condition: Row count > 0
-- -----------------------------------------------------------------------------

SELECT 
    city,
    country,
    recorded_at,
    us_aqi,
    aqi_category,
    pm2_5,
    rolling_24h_avg_pm25,
    ozone,
    CONCAT('HEALTH HAZARD: ', city, ' (', country, ') reached US AQI ', us_aqi, ' [', aqi_category, ']. 24h PM2.5: ', rolling_24h_avg_pm25, ' ug/m3.') AS hazard_notification
FROM fact_air_quality_hourly
WHERE us_aqi > 100 -- Warning/Unhealthy threshold
   OR pm2_5 > 35.0
ORDER BY us_aqi DESC, recorded_at DESC
LIMIT 5;


-- -----------------------------------------------------------------------------
-- ALERT 3: Medallion Layer Reconciliation Test (Bronze -> Silver -> Gold)
-- Business Objective: Ensure zero silent record loss across the Medallion flow.
-- -----------------------------------------------------------------------------

SELECT 
    (SELECT COUNT(*) FROM silver_air_quality_enriched) AS silver_count,
    (SELECT COUNT(*) FROM fact_air_quality_hourly) AS gold_fact_count,
    (SELECT COUNT(*) FROM silver_air_quality_enriched) - (SELECT COUNT(*) FROM fact_air_quality_hourly) AS record_discrepancy,
    CASE 
        WHEN (SELECT COUNT(*) FROM silver_air_quality_enriched) = (SELECT COUNT(*) FROM fact_air_quality_hourly)
        THEN 'RECONCILIATION PASSED: 100% data integrity between Silver and Gold'
        ELSE 'RECONCILIATION FAILED: Inconsistency detected between Silver and Gold'
    END AS audit_status;
