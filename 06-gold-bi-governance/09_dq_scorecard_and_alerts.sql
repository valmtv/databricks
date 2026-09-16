-- Databricks notebook source
-- ============================================================================
-- Lab 07: Data Quality Scorecard, Metrics & Automated Alerting Queries
-- ============================================================================
-- Catalog: workspace (dev_free)
-- Schema:  default
CREATE WIDGET TEXT catalog DEFAULT "workspace";
CREATE WIDGET TEXT schema DEFAULT "default";

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);


-- ----------------------------------------------------------------------------
-- 1. Data Quality Executive Scorecard View
-- Computes pass rates and compliance KPIs across the Medallion
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_data_quality_scorecard AS
WITH layer_metrics AS (
    SELECT 
        (SELECT COUNT(*) FROM bronze_air_quality_raw) AS bronze_total_records,
        (SELECT COUNT(*) FROM silver_air_quality_enriched) AS silver_clean_records,
        (SELECT COUNT(*) FROM silver_air_quality_quarantine) AS silver_quarantined_records,
        (SELECT COUNT(*) FROM fact_air_quality_hourly) AS gold_hourly_facts,
        (SELECT COALESCE(SUM(observation_count), 0) FROM fact_city_daily_summary) AS gold_daily_sum_observations
)
SELECT
    bronze_total_records,
    silver_clean_records,
    silver_quarantined_records,
    (silver_clean_records + silver_quarantined_records) AS total_processed_records,
    
    -- Invariant 1: Silent Loss Check
    (bronze_total_records - (silver_clean_records + silver_quarantined_records)) AS silent_data_loss_gap,
    
    -- Invariant 2: Grain Rollup Check
    (gold_hourly_facts - gold_daily_sum_observations) AS fact_to_summary_discrepancy,
    
    -- Data Quality KPI Scores (0.0% to 100.0%)
    ROUND((silver_clean_records * 100.0) / NULLIF(bronze_total_records, 0), 2) AS clean_data_ingestion_rate_pct,
    ROUND((silver_quarantined_records * 100.0) / NULLIF(bronze_total_records, 0), 2) AS quarantine_rate_pct,
    
    CASE 
        WHEN (bronze_total_records - (silver_clean_records + silver_quarantined_records)) = 0 
             AND (gold_hourly_facts - gold_daily_sum_observations) = 0 THEN 'HEALTHY (100% RECONCILED)'
        ELSE 'DEGRADED (DATA DRIFT DETECTED)'
    END AS overall_medallion_health_status,
    CURRENT_TIMESTAMP() AS evaluated_at
FROM layer_metrics;


-- ----------------------------------------------------------------------------
-- 2. Quarantine Diagnostic Breakdown View
-- Analyzes the root causes and error distributions of quarantined records
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_quarantine_diagnostics AS
SELECT 
    quarantine_reason,
    COUNT(*) AS rejected_record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS share_of_total_quarantined_pct,
    MIN(quarantined_at) AS first_detected_at,
    MAX(quarantined_at) AS last_detected_at
FROM silver_air_quality_quarantine
GROUP BY quarantine_reason
ORDER BY rejected_record_count DESC;


-- ----------------------------------------------------------------------------
-- 3. Databricks SQL Automated Alert Queries
-- ----------------------------------------------------------------------------

-- Alert Query 1: Quarantine Rate Breach Alert
-- Trigger Condition: quarantine_rate_pct > 2.0%
-- Destination: Slack / Email notification to Data Engineering Team
SELECT 
    quarantine_rate_pct,
    silver_quarantined_records,
    evaluated_at
FROM vw_data_quality_scorecard
WHERE quarantine_rate_pct > 2.0;

-- Alert Query 2: Silent Data Loss Discrepancy Alert
-- Trigger Condition: silent_data_loss_gap != 0
SELECT 
    silent_data_loss_gap,
    bronze_total_records,
    silver_clean_records,
    silver_quarantined_records,
    evaluated_at
FROM vw_data_quality_scorecard
WHERE silent_data_loss_gap != 0;

-- Alert Query 3: Referential Integrity Orphan Keys Alert
-- Trigger Condition: orphan_station_fks > 0
SELECT 
    COUNT(*) AS orphan_station_fks,
    CURRENT_TIMESTAMP() AS detected_at
FROM fact_air_quality_hourly f
LEFT JOIN dim_city d ON f.station_sk = d.station_sk
WHERE d.station_sk IS NULL;
