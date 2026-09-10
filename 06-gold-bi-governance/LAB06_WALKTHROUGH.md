# Lab 06: Gold Star Schema, BI, Alerts & Governance

Enterprise business presentation layer deployed natively on **Databricks Free Edition** (`workspace.default`).

---

## 1. Dimensional Architecture (Star Schema Basics)

- **Star Schema**: Optimized for OLAP and Photon; central Fact tables surrounded by denormalized Dimension tables to minimize joins.
- **Facts**: Quantitative numerical metrics (`fact_air_quality_hourly`, `fact_city_daily_summary`).
- **Dimensions**: Slicing attributes (`dim_city`, `dim_calendar_date`, `dim_aqi_category`).
- **Conformed Dimensions**: Shared across hourly and daily facts for unified rollups.
- **Surrogate Keys**: Synthetic integer hash keys (`*_sk`) isolating analytics from source key changes.

```
                      +-------------------+
                      | dim_calendar_date |
                      +-------------------+
                      | PK  date_sk       |
                      |     calendar_date |
                      |     year, month   |
                      |     day_of_week   |
                      +---------+---------+
                                |
             +------------------+------------------+
             |                                     |
             v                                     v
+-----------------------------+       +-----------------------------+
|  fact_air_quality_hourly    |       |   fact_city_daily_summary   |
+-----------------------------+       +-----------------------------+
| PK  fact_sk                 |       | PK  summary_sk              |
| FK  station_sk              |       | FK  station_sk              |
| FK  date_sk                 |       | FK  date_sk                 |
| FK  category_sk             |       |     city, country           |
|     recorded_at             |       |     calendar_date           |
|     pm2_5, pm10, o3, no2    |       |     observation_count       |
|     us_aqi                  |       |     avg_us_aqi, max_us_aqi  |
|     rolling_24h_avg_pm25    |       |     hours_safe              |
|     is_who_pm25_exceeded    |       |     hours_unhealthy         |
|     health_severity_score   |       |     compliance_grade        |
+--------------+--------------+       +--------------+--------------+
               |                                     |
       +-------+-------+                             |
       |               |                             |
       v               v                             v
+-------------+ +--------------------+         +-------------+
|  dim_city   | |  dim_aqi_category  |         |  dim_city   |
+-------------+ +--------------------+         +-------------+
| PK station_sk| | PK category_sk    |         | PK station_sk|
|    city     | |    aqi_category    |         |    city     |
|    country  | |    color_code      |         |    country  |
|    region   | |    severity_rank   |         |    region   |
+-------------+ +--------------------+         +-------------+
```

---

## 2. Medallion Flow

```
[ Open-Meteo API ] & [ EPA CSV ] -> Landing JSON/CSV
                   |
                   v
[ BRONZE ]  bronze_air_quality_raw (Streaming Table), bronze_aqi_reference (MV)
                   |
                   v
[ SILVER ]  silver_air_quality_enriched (Streaming Table, DLT Expectations)
                   |
                   v
[ GOLD ]    dim_city, dim_calendar_date, dim_aqi_category (Dimensions)
            fact_air_quality_hourly (1,260 rows, 24h rolling PM2.5, WHO flag)
            fact_city_daily_summary (35 rows, compliance grades)
                   |
                   v
[ BI & GOV] fact_air_quality_hourly_governed (RLS + CLS)
            AI/BI Dashboard, SQL Alert, Genie Space
```

---

## 3. Unity Catalog Governance (RLS & CLS)

Uses the **Governed Reporting View** pattern (`fact_air_quality_hourly_governed`):
- **Row-Level Security (RLS)**: `air_quality_regional_rls_filter(country)` isolates regional data (US vs EU health officials), granting full access to admins.
- **Column-Level Security (CLS)**: `mask_hazardous_pollutant(gas_ppm)` masks toxic industrial gases (`carbon_monoxide`, `sulphur_dioxide`) to `NULL` for unauthorized public viewers.
- **Grants**: Least-privilege `SELECT` granted to `account users`.

---

## 4. Live Workspace Objects & Verification

### A. Azure Dev/Prod Environment (`prod_azure`)
> **Deployment Policy**: Fully deployed with **zero runs executed**. All compute bound to existing cluster **`GP1`** (`0702-132442-toro5spu`) and non-serverless DLT (`Standard_D4ds_v5`, 1 worker, PRO edition). Alerts are created in `PAUSED` state.

| Asset | Type | ID / Target | Direct Link |
|---|---|---|---|
| **End-to-End Orchestrated Job** | Workflow Job (GP1 cluster) | `594339360399957` | [Open Job](https://adb-7405604503619901.1.azuredatabricks.net/jobs/594339360399957) |
| **AI/BI Executive Dashboard** | Published Lakeview Dashboard | `01f1ad5b7b691280b44f0cf37f0809e8` | [Open Dashboard](https://adb-7405604503619901.1.azuredatabricks.net/dashboardsv3/01f1ad5b7b691280b44f0cf37f0809e8/published) |
| **SQL Telemetry Alert** | Automated Volume Drop Alert | `4174171081479852` | [Open Alert](https://adb-7405604503619901.1.azuredatabricks.net/sql/alerts/4174171081479852) |
| **Genie Space** | Conversational Q&A Space | `01f1ad5ba90315768b2cbc6437e77a31` | [Open Genie Space](https://adb-7405604503619901.1.azuredatabricks.net/genie/spaces/01f1ad5ba90315768b2cbc6437e77a31) |

---

## 5. Deployment Commands (DABs)

```bash
# Deploy to Azure Dev/Prod (No execution)
databricks bundle deploy -t prod_azure

# Deploy to Databricks Free (Development)
databricks bundle deploy -t dev_free
```
