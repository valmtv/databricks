-- Databricks notebook source
-- =============================================================================
-- Unity Catalog Governance: Row-Level Security (RLS) & Column-Level Security (CLS)
-- Applied to: fact_news_impact
-- =============================================================================

-- Target Catalog and Schema Configuration:
CREATE WIDGET TEXT catalog DEFAULT "dbr_dev";
CREATE WIDGET TEXT schema DEFAULT "valeriimatviiv_gold";

USE CATALOG IDENTIFIER(:catalog);
USE SCHEMA IDENTIFIER(:schema);

-- -----------------------------------------------------------------------------
-- 1. Governance Functions
-- -----------------------------------------------------------------------------

-- RLS Function: Hides unverified 'PENDING_EVALUATION' news from general users
CREATE OR REPLACE FUNCTION fact_news_impact_rls_filter(impact_status STRING)
RETURN 
  is_account_group_member('admin_analysts') 
  OR is_account_group_member('senior_traders')
  OR current_user() IN (
      'valerii.matviiv@softserve.academy',
      'valerii.matviiv@gmail.com'
  )
  OR impact_status = 'COMPLETED';

-- CLS Function: Redacts proprietary CompositeImpactScore for unauthorized users
CREATE OR REPLACE FUNCTION mask_composite_score(score DOUBLE)
RETURN 
  CASE 
    WHEN is_account_group_member('premium_traders') 
      OR is_account_group_member('admin_analysts')
      OR current_user() IN (
          'valerii.matviiv@softserve.academy',
          'valerii.matviiv@gmail.com'
      )
    THEN score
    ELSE NULL -- Masked for standard analysts
  END;


-- -----------------------------------------------------------------------------
-- 2. Governed Reporting View (Standard Pattern for Lakeflow Materialized Views)
-- Because Lakeflow publishes fact_news_impact as a Materialized View in Unity Catalog,
-- RLS and CLS are enforced seamlessly via a secure reporting interface.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW fact_news_impact_governed AS
SELECT 
    ArticleId,
    company_sk,
    date_sk,
    source_sk,
    Symbol,
    NewsTimestamp,
    Headline,
    Summary,
    ImpactStatus,
    PrePrice,
    PostPrice,
    StockReturnPct,
    QQQReturnPct,
    AlphaReturnPct,
    IntradayVolatilityPct,
    VolumeSurgeRatio,
    -- Column-Level Security (CLS)
    mask_composite_score(CompositeImpactScore) AS CompositeImpactScore,
    ReactionPattern,
    Url
FROM fact_news_impact
-- Row-Level Security (RLS)
WHERE fact_news_impact_rls_filter(ImpactStatus);


-- -----------------------------------------------------------------------------
-- 3. Access Control Grants (Least Privilege Model)
-- -----------------------------------------------------------------------------

GRANT SELECT ON VIEW fact_news_impact_governed TO `account users`;
GRANT SELECT ON TABLE dim_company TO `account users`;
GRANT SELECT ON TABLE dim_date TO `account users`;
GRANT SELECT ON TABLE dim_news_source TO `account users`;
GRANT SELECT ON TABLE fact_stock_prices_5m TO `account users`;


-- -----------------------------------------------------------------------------
-- 4. Governance Verification Queries
-- -----------------------------------------------------------------------------

-- Query 1: Query governed view as regular analyst (Pending news hidden & score masked)
SELECT Symbol, Headline, CompositeImpactScore, ImpactStatus 
FROM fact_news_impact_governed 
ORDER BY ImpactStatus;
