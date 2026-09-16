-- ============================================================================
-- Lab 07: Delta Lake Table-Level Constraints (Storage Engine Defense-in-Depth)
-- ============================================================================
-- Catalog: workspace (or dbr_dev in Azure)
-- Schema:  default (or valeriimatviiv_gold)
--
-- PURPOSE:
-- While Lakeflow (DLT) expectations guard data entering through the automated
-- pipeline, Delta Constraints act as the final, immutable line of defense directly
-- in the Delta storage engine metadata (_delta_log).
-- Any write (ad-hoc SQL, Python MERGE, or batch backfill) that violates these
-- rules is rejected at the transaction commit level with InvariantViolationException.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Silver Layer Constraints (Clean Ingestion Boundaries)
-- ----------------------------------------------------------------------------

-- Enforce non-null primary keys on clean Silver table
ALTER TABLE silver_air_quality_enriched 
  ALTER COLUMN event_id SET NOT NULL;

ALTER TABLE silver_air_quality_enriched 
  ALTER COLUMN station_id SET NOT NULL;

ALTER TABLE silver_air_quality_enriched 
  ALTER COLUMN recorded_at SET NOT NULL;

-- Physical coordinate bounds
ALTER TABLE silver_air_quality_enriched 
  ADD CONSTRAINT chk_silver_latitude_bounds 
  CHECK (latitude BETWEEN -90.0 AND 90.0);

ALTER TABLE silver_air_quality_enriched 
  ADD CONSTRAINT chk_silver_longitude_bounds 
  CHECK (longitude BETWEEN -180.0 AND 180.0);

-- Non-negative pollutant concentrations
ALTER TABLE silver_air_quality_enriched 
  ADD CONSTRAINT chk_silver_pm25_non_negative 
  CHECK (pm2_5 IS NULL OR pm2_5 >= 0.0);

ALTER TABLE silver_air_quality_enriched 
  ADD CONSTRAINT chk_silver_pm10_non_negative 
  CHECK (pm10 IS NULL OR pm10 >= 0.0);

-- US EPA AQI index range [0..500]
ALTER TABLE silver_air_quality_enriched 
  ADD CONSTRAINT chk_silver_aqi_scale 
  CHECK (us_aqi IS NULL OR (us_aqi >= 0 AND us_aqi <= 500));


-- ----------------------------------------------------------------------------
-- 2. Gold Fact Tables Constraints (Analytical Invariants)
-- ----------------------------------------------------------------------------

-- fact_air_quality_hourly: Primary & Foreign Surrogate Keys MUST NOT be NULL
ALTER TABLE fact_air_quality_hourly 
  ALTER COLUMN fact_sk SET NOT NULL;

ALTER TABLE fact_air_quality_hourly 
  ALTER COLUMN station_sk SET NOT NULL;

ALTER TABLE fact_air_quality_hourly 
  ALTER COLUMN date_sk SET NOT NULL;

ALTER TABLE fact_air_quality_hourly 
  ALTER COLUMN category_sk SET NOT NULL;

-- Rolling 24h average PM2.5 must be non-negative
ALTER TABLE fact_air_quality_hourly 
  ADD CONSTRAINT chk_fact_rolling_pm25_non_negative 
  CHECK (rolling_24h_avg_pm25 IS NULL OR rolling_24h_avg_pm25 >= 0.0);

-- Health severity score must be within valid tier scale [1.0 .. 6.0]
ALTER TABLE fact_air_quality_hourly 
  ADD CONSTRAINT chk_fact_health_severity_range 
  CHECK (health_severity_score BETWEEN 1.0 AND 6.0);


-- fact_city_daily_summary: Aggregation Invariants
ALTER TABLE fact_city_daily_summary 
  ALTER COLUMN summary_sk SET NOT NULL;

ALTER TABLE fact_city_daily_summary 
  ALTER COLUMN station_sk SET NOT NULL;

ALTER TABLE fact_city_daily_summary 
  ALTER COLUMN date_sk SET NOT NULL;

-- Observation count must be strictly positive (> 0)
ALTER TABLE fact_city_daily_summary 
  ADD CONSTRAINT chk_summary_observation_count_positive 
  CHECK (observation_count > 0);

-- Unhealthy hours percentage must be between 0.0% and 100.0%
ALTER TABLE fact_city_daily_summary 
  ADD CONSTRAINT chk_summary_unhealthy_pct_bounds 
  CHECK (unhealthy_hours_pct BETWEEN 0.0 AND 100.0);

-- Partition invariant: Hours safe, moderate, and unhealthy cannot exceed total observations
ALTER TABLE fact_city_daily_summary 
  ADD CONSTRAINT chk_summary_hours_partition_integrity 
  CHECK ((hours_safe + hours_moderate + hours_unhealthy) <= observation_count);


-- ----------------------------------------------------------------------------
-- 3. Gold Dimension Tables Constraints (Entity Uniqueness)
-- ----------------------------------------------------------------------------

-- dim_city
ALTER TABLE dim_city 
  ALTER COLUMN station_sk SET NOT NULL;

ALTER TABLE dim_city 
  ALTER COLUMN station_id SET NOT NULL;

-- dim_calendar_date
ALTER TABLE dim_calendar_date 
  ALTER COLUMN date_sk SET NOT NULL;

ALTER TABLE dim_calendar_date 
  ALTER COLUMN calendar_date SET NOT NULL;

ALTER TABLE dim_calendar_date 
  ADD CONSTRAINT chk_dim_calendar_month_range 
  CHECK (month BETWEEN 1 AND 12);

ALTER TABLE dim_calendar_date 
  ADD CONSTRAINT chk_dim_calendar_day_range 
  CHECK (day_of_month BETWEEN 1 AND 31);

-- dim_aqi_category
ALTER TABLE dim_aqi_category 
  ALTER COLUMN category_sk SET NOT NULL;

ALTER TABLE dim_aqi_category 
  ADD CONSTRAINT chk_dim_aqi_severity_rank_scale 
  CHECK (severity_rank BETWEEN 1 AND 6);
