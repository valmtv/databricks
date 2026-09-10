# Databricks Genie Space: Natural-Language Q&A over the Gold Layer

An enterprise **Databricks Genie Space** configuration providing business executives, environmental researchers, and city health officials with conversational natural-language Q&A over the Air Quality Gold Star Schema (`workspace.default`).

---

## 1. Executive Summary & Value Proposition

Databricks Genie leverages Unity Catalog metadata, table comments, column descriptions, and curated semantic instructions to translate natural-language business questions directly into optimized ANSI SQL running on Databricks Photon and Serverless SQL Warehouses.

### Key Capabilities:
- **No SQL Required**: Business users can ask questions like *"Which city had the worst air quality yesterday?"* or *"Show the 24h rolling PM2.5 trend for London"*.
- **Governed by Unity Catalog**: Natural language queries automatically inherit Unity Catalog object permissions, Row-Level Security, and Column-Level Security.
- **Auditable SQL Execution**: Every answer includes the exact SQL query executed, execution metrics, and an interactive data visualization.

---

## 2. Genie Space Configuration Details

| Parameter | Recommended Value |
|---|---|
| **Space Title** | `Air Quality Environmental & Public Health Intelligence` |
| **Catalog** | `workspace` |
| **Schema** | `default` |
| **Compute / SQL Warehouse** | `Serverless Starter Warehouse` (`2035b157781cb38f`) |
| **Target Tables** | `fact_air_quality_hourly`, `fact_city_daily_summary`, `dim_city`, `dim_date`, `dim_aqi_category`, `fact_air_quality_hourly_governed` |

---

## 3. Curated Semantic Instructions (System Prompt for Genie)

Paste the following instructions into the **Instructions** tab of the Genie Space in Databricks:

```markdown
You are an expert environmental data analyst providing answers over the Air Quality Gold Star Schema in Unity Catalog (`workspace.default`).

### Table Relationships & Granularity:
1. `fact_air_quality_hourly`:
   - Grain: 1 row per monitoring station per recorded hour.
   - Primary metrics: `pm2_5`, `pm10`, `ozone`, `nitrogen_dioxide`, `sulphur_dioxide`, `carbon_monoxide`, `us_aqi`, `rolling_24h_avg_pm25`.
   - Dimension Foreign Keys: `station_sk` -> `dim_city.station_sk`, `date_sk` -> `dim_date.date_sk`, `category_sk` -> `dim_aqi_category.category_sk`.
2. `fact_city_daily_summary`:
   - Grain: 1 row per city per calendar date.
   - Use this table for high-level daily trends, compliance grades, and multi-day comparisons.
3. `dim_city`: Contains city names (`city`), countries (`country`), geographic coordinates (`latitude`, `longitude`), and macro `region`.
4. `dim_aqi_category`: Standard EPA categories (`Good`, `Moderate`, `Unhealthy for Sensitive Groups`, `Unhealthy`, `Very Unhealthy`, `Hazardous`).
5. `fact_air_quality_hourly_governed`: Use when addressing general reporting or external stakeholder requests where Column-Level Security (CLS) masking on toxic industrial gases (`carbon_monoxide`, `sulphur_dioxide`) is required.

### Domain Rules & Definitions:
- "Safe Air" or "Healthy Air": Refers to `us_aqi <= 50` or `aqi_category = 'Good'`.
- "WHO PM2.5 Guideline": World Health Organization 24-hour guideline is exceeded when `pm2_5 > 15.0 ug/m3` (`is_who_pm25_exceeded = true`).
- "Worst Air Quality": Sort by `us_aqi DESC` or `pm2_5 DESC`.
- "Best Air Quality": Sort by `us_aqi ASC` (lowest AQI).
- "Pollutant concentrations": Always report units — PM2.5, PM10, SO2, NO2 in ug/m3; CO in ug/m3; US AQI is unitless (0 to 500 scale).
```

---

## 4. Benchmark Sample Questions & Gold SQL Ground Truth

### Query 1: Which city recorded the highest air pollution (peak AQI)?
**User Prompt**: *"Which city had the worst air quality and what was the peak AQI?"*
```sql
SELECT 
    city,
    country,
    recorded_at,
    us_aqi,
    aqi_category,
    pm2_5
FROM workspace.default.fact_air_quality_hourly
ORDER BY us_aqi DESC
LIMIT 1;
```

---

### Query 2: Daily compliance scorecard for Paris
**User Prompt**: *"Show the daily compliance grade and healthy hours for Paris"*
```sql
SELECT 
    calendar_date,
    city,
    avg_us_aqi,
    compliance_grade,
    hours_safe,
    hours_unhealthy,
    unhealthy_hours_pct
FROM workspace.default.fact_city_daily_summary
WHERE LOWER(city) = 'paris'
ORDER BY calendar_date DESC;
```

---

### Query 3: What percentage of hours exceeded the WHO PM2.5 threshold per city?
**User Prompt**: *"What percentage of time did each city exceed the WHO PM2.5 safe limit?"*
```sql
SELECT 
    city,
    country,
    COUNT(*) AS total_hours,
    SUM(CASE WHEN is_who_pm25_exceeded THEN 1 ELSE 0 END) AS hours_exceeded,
    ROUND(100.0 * SUM(CASE WHEN is_who_pm25_exceeded THEN 1 ELSE 0 END) / COUNT(*), 1) AS exceedance_rate_pct
FROM workspace.default.fact_air_quality_hourly
GROUP BY city, country
ORDER BY exceedance_rate_pct DESC;
```

---

### Query 4: 24-Hour rolling PM2.5 trend for New York
**User Prompt**: *"Show the 24-hour rolling average PM2.5 for New York over time"*
```sql
SELECT 
    recorded_at,
    city,
    pm2_5,
    rolling_24h_avg_pm25,
    aqi_category
FROM workspace.default.fact_air_quality_hourly
WHERE LOWER(city) = 'new york'
ORDER BY recorded_at ASC;
```

---

### Query 5: Governed pollutant check for industrial gases (CO and SO2)
**User Prompt**: *"List latest carbon monoxide and sulphur dioxide levels across all cities using the governed view"*
```sql
SELECT 
    city,
    country,
    recorded_at,
    carbon_monoxide,
    sulphur_dioxide,
    us_aqi
FROM workspace.default.fact_air_quality_hourly_governed
ORDER BY recorded_at DESC
LIMIT 10;
```

---

## 5. UI Setup Steps in Databricks

1. In the Databricks left navigation menu, click **Genie** (or **New** → **Genie Space**).
2. Click **New Space** in the top right.
3. Name: **Air Quality Environmental & Public Health Intelligence**.
4. Select Default Warehouse: **Serverless Starter Warehouse** (`2035b157781cb38f`).
5. Select Tables:
   - Check `workspace.default.fact_air_quality_hourly`
   - Check `workspace.default.fact_city_daily_summary`
   - Check `workspace.default.dim_city`
   - Check `workspace.default.dim_calendar_date`
   - Check `workspace.default.dim_aqi_category`
   - Check `workspace.default.fact_air_quality_hourly_governed`
6. Click **Instructions** in the sidebar, and paste Section 3 above.
7. Click **Example Queries** and add the 5 sample questions from Section 4.
8. Ask a test question: *"Which city has the cleanest air on average?"* and confirm Genie returns a bar chart with the correct SQL query!
