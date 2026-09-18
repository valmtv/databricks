-- Databricks notebook source
-- =============================================================================
-- Dimension: dim_calendar_date
-- Enterprise Temporal Hierarchy Generated via Databricks Recursive CTE
-- Reference: Generating a Date Dimension Table in Databricks Using Recursive CTEs
-- =============================================================================

CREATE WIDGET TEXT catalog DEFAULT "workspace";
CREATE WIDGET TEXT schema DEFAULT "default";

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

-- -----------------------------------------------------------------------------
-- 1. Generate Conformed Calendar Date Dimension Table
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE dim_calendar_date
CLUSTER BY (date_sk, calendar_date)
COMMENT "Conformed calendar date dimension table generated via recursive CTE for zero-shuffle temporal analytics"
TBLPROPERTIES (
  'quality' = 'gold',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
AS
WITH RECURSIVE Dates MAX RECURSION LEVEL 4500 AS (
  SELECT 
    cast('2020-01-01' as date) as calendar_date,
    dayname(cast('2020-01-01' as date)) as day_name,
    dayofmonth(cast('2020-01-01' as date)) as day_of_month,
    dayofweek(cast('2020-01-01' as date)) as day_of_week,
    dayofyear(cast('2020-01-01' as date)) as day_of_year,
    last_day(cast('2020-01-01' as date)) as last_day_of_month,
    quarter(cast('2020-01-01' as date)) as quarter,
    month(cast('2020-01-01' as date)) as month,
    date_format(cast('2020-01-01' as date), 'MMMM') as month_name,
    year(cast('2020-01-01' as date)) as year
  UNION ALL
  SELECT 
    cast(dateadd(day, 1, calendar_date) as date) as calendar_date,
    dayname(cast(dateadd(day, 1, calendar_date) as date)) as day_name,
    dayofmonth(cast(dateadd(day, 1, calendar_date) as date)) as day_of_month,
    dayofweek(cast(dateadd(day, 1, calendar_date) as date)) as day_of_week,
    dayofyear(cast(dateadd(day, 1, calendar_date) as date)) as day_of_year,
    last_day(cast(dateadd(day, 1, calendar_date) as date)) as last_day_of_month,
    quarter(cast(dateadd(day, 1, calendar_date) as date)) as quarter,
    month(cast(dateadd(day, 1, calendar_date) as date)) as month,
    date_format(cast(dateadd(day, 1, calendar_date) as date), 'MMMM') as month_name,
    year(cast(dateadd(day, 1, calendar_date) as date)) as year
  FROM Dates
  WHERE dateadd(day, 1, calendar_date) <= cast('2030-12-31' as date)
)
SELECT 
  cast(date_format(calendar_date, 'yyyyMMdd') as int) as date_sk,
  calendar_date,
  year,
  quarter,
  month,
  month_name,
  day_of_month,
  day_of_week,
  day_name,
  day_of_year,
  last_day_of_month,
  case when day_of_week in (1, 7) then true else false end as is_weekend
FROM Dates;
