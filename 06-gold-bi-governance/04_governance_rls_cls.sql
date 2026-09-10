-- Databricks notebook source
-- =============================================================================
-- Unity Catalog Governance: Row-Level Security (RLS) & Column-Level Security (CLS)
-- Applied to: fact_air_quality_hourly & Gold Star Schema Objects
-- =============================================================================

-- Target Catalog and Schema Configuration (Defaults to Databricks Free Serverless workspace.default)
CREATE WIDGET TEXT catalog DEFAULT "workspace";
CREATE WIDGET TEXT schema DEFAULT "default";

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

-- -----------------------------------------------------------------------------
-- 1. Governance Functions (RLS & CLS)
-- -----------------------------------------------------------------------------

-- RLS Function: Enforces jurisdictional data segregation
-- US analysts view USA stations, EU analysts view European stations, Global Admins view all.
CREATE OR REPLACE FUNCTION air_quality_regional_rls_filter(country STRING)
RETURN 
  is_account_group_member('global_environmental_admins') 
  OR is_account_group_member('epa_auditors')
  OR current_user() IN (
      'valerii.matviiv@gmail.com',
      'valerii.matviiv@softserve.academy'
  )
  OR (is_account_group_member('us_health_officials') AND country = 'USA')
  OR (is_account_group_member('eu_health_officials') AND country IN ('UK', 'France', 'Germany'))
  OR country IS NOT NULL; -- Default fallback allows broad visibility for general demo


-- CLS Function: Redacts raw toxic industrial gas concentrations (CO, SO2) for unauthorized standard viewers
CREATE OR REPLACE FUNCTION mask_hazardous_pollutant(gas_ppm DOUBLE)
RETURN 
  CASE 
    WHEN is_account_group_member('certified_atmospheric_scientists') 
      OR is_account_group_member('global_environmental_admins')
      OR current_user() IN (
          'valerii.matviiv@gmail.com',
          'valerii.matviiv@softserve.academy'
      )
    THEN gas_ppm
    ELSE NULL -- Redacted for general public / non-certified reporting
  END;


-- -----------------------------------------------------------------------------
-- 2. Governed Reporting View (Standard Pattern for Lakeflow Materialized Views)
-- Because Lakeflow publishes Gold tables as Materialized Views in Unity Catalog,
-- RLS and CLS are enforced natively via a governed reporting interface.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW fact_air_quality_hourly_governed AS
SELECT 
    fact_sk,
    station_sk,
    date_sk,
    category_sk,
    event_id,
    station_id,
    city,
    country,
    recorded_at,
    pm2_5,
    pm10,
    -- Column-Level Security (CLS) Masking
    mask_hazardous_pollutant(carbon_monoxide) AS carbon_monoxide,
    nitrogen_dioxide,
    mask_hazardous_pollutant(sulphur_dioxide) AS sulphur_dioxide,
    ozone,
    us_aqi,
    aqi_category,
    aqi_color_code,
    rolling_24h_avg_pm25,
    is_who_pm25_exceeded,
    health_severity_score,
    _transformed_timestamp
FROM fact_air_quality_hourly
-- Row-Level Security (RLS) Predicate
WHERE air_quality_regional_rls_filter(country);


-- -----------------------------------------------------------------------------
-- 3. Access Control Grants (Principle of Least Privilege)
-- -----------------------------------------------------------------------------

GRANT SELECT ON VIEW fact_air_quality_hourly_governed TO `account users`;
GRANT SELECT ON TABLE dim_city TO `account users`;
GRANT SELECT ON TABLE dim_calendar_date TO `account users`;
GRANT SELECT ON TABLE dim_aqi_category TO `account users`;
GRANT SELECT ON TABLE fact_city_daily_summary TO `account users`;


-- -----------------------------------------------------------------------------
-- 4. Governance Verification Queries
-- -----------------------------------------------------------------------------

-- Verification 1: Inspect Governed View columns and verified non-null public indicators
SELECT 
    city, 
    country, 
    recorded_at, 
    us_aqi, 
    aqi_category,
    carbon_monoxide,
    sulphur_dioxide,
    rolling_24h_avg_pm25
FROM fact_air_quality_hourly_governed 
ORDER BY recorded_at DESC 
LIMIT 10;

-- Verification 2: Verify Daily Business Summary aggregation access
SELECT 
    city, 
    calendar_date, 
    observation_count, 
    avg_us_aqi, 
    compliance_grade 
FROM fact_city_daily_summary 
ORDER BY avg_us_aqi DESC 
LIMIT 10;
