# Lab 07: Unit Testing, Medallion Data Quality & CI/CD Gating

Enterprise data engineering quality framework: unit testing pure transformations, enforcing the 5 data quality dimensions, routing anomalies to quarantine tables, adding Delta storage constraints, and running automated cross-layer reconciliation gates.

---

## 1. Architecture & Testing Matrix

```
                             [ Bronze Ingestion ]
                            bronze_air_quality_raw
                                      |
               +----------------------+----------------------+
               |                                             |
   [ Clean / Valid Telemetry ]                    [ Non-Compliant / Corrupt ]
               v                                             v
  [ Silver Enriched Table ]                     [ Silver Quarantine Table ]
  silver_air_quality_enriched                   silver_air_quality_quarantine
  - EPA AQI tiers & advisories                  - Error tags (quarantine_reason)
  - DLT tracking expectations                   - Quarantined timestamp
               |                                             |
               v                                             v
        [ Gold Layer ]                              [ DQ Scorecard & Alerts ]
  dim_city, dim_calendar_date,                  vw_data_quality_scorecard,
  dim_aqi_category,                             vw_quarantine_diagnostics,
  fact_air_quality_hourly,                      Automated volume & drift alerts
  fact_city_daily_summary
               |
               v
   [ Cross-Layer Reconciliation Gate (run_dq_reconciliation.py) ]
   1. Row-Count Conservation: Bronze == Silver Valid + Quarantine
   2. Grain Conservation: Hourly Fact Count == Daily Summary observation_count Sum
   3. Referential Integrity: Zero orphan station_sk / date_sk / category_sk
   4. Timeliness: Zero future-dated observations (recorded_at <= current_timestamp)
```

---

## 2. Part A: Modular Transformations & Pure Unit Testing

### Why Modularization Matters
In traditional Databricks notebooks, transformation logic is coupled with Spark session initialization, widgets, and storage paths. This creates **test drift** where unit tests test fake copies instead of production code.

To eliminate test drift, all business logic is organized into the standalone, importable package **`air_quality_transforms/`**:
- [`cleansing.py`](file:///Users/omatviiv/learning/databricks/06-gold-bi-governance/air_quality_transforms/cleansing.py): Physical coordinate validation ($[-90..90], [-180..180]$), pollutant ranges, timestamp freshness, and type casting.
- [`enrichment.py`](file:///Users/omatviiv/learning/databricks/06-gold-bi-governance/air_quality_transforms/enrichment.py): Standard EPA AQI categories (Good $\rightarrow$ Hazardous), hex color codes, and fallback defaults.
- [`features.py`](file:///Users/omatviiv/learning/databricks/06-gold-bi-governance/air_quality_transforms/features.py): Deterministic surrogate key generation, WHO 24-hour PM2.5 exceedance check ($> 15.0\,\mu\text{g/m}^3$), health severity score (1.0 to 6.0).
- [`aggregations.py`](file:///Users/omatviiv/learning/databricks/06-gold-bi-governance/air_quality_transforms/aggregations.py): Daily rollups, observation partitioning ($\text{safe} + \text{moderate} + \text{unhealthy} = \text{total}$), compliance grades (Grades A/B/C/D).
- [`dimensions.py`](file:///Users/omatviiv/learning/databricks/06-gold-bi-governance/air_quality_transforms/dimensions.py): Calendar date hierarchy (year, quarter, month, day of week, weekend flag) and station metadata lookup.
- [`quarantine.py`](file:///Users/omatviiv/learning/databricks/06-gold-bi-governance/air_quality_transforms/quarantine.py): Rule definitions, quarantine predicate evaluation, and error reason tagging.

Both the production DLT scripts (`02_silver_transformations.py`, `03_gold_star_schema.py`) and the test suite import directly from this package. **A change in logic immediately updates both production and tests.**

### Running Unit Tests Locally
All 27 unit tests execute in **0.003 seconds** without requiring a cluster or internet connection:
```bash
python3 run_tests.py -v
```

### Running with Databricks Connect
To run the tests from your local IDE (VS Code, Cursor, PyCharm) against a remote cluster via Databricks Connect v2:
```bash
# 1. Install databricks-connect matching your DBR version
pip install "databricks-connect==17.3.*"

# 2. Run test runner with remote execution flag
python3 run_tests.py --connect
```

---

## 3. Part B: The 5 Dimensions of Medallion Data Quality

| Dimension | Description | Pipeline Implementation | Enforcement Mechanism |
|---|---|---|---|
| **1. Completeness** | Absence of null or blank primary keys | `event_id`, `station_id`, `city`, `recorded_at` cannot be null | DLT `@dlt.expect_or_fail` & Delta `NOT NULL` |
| **2. Uniqueness** | Primary key uniqueness and absence of duplicates | `fact_sk`, `summary_sk`, `(station_id, recorded_at)` | Deterministic surrogate key hashing |
| **3. Validity** | Compliance with physical bounds and ranges | Latitude $[-90..90]$, PM2.5 $\ge 0$, AQI $[0..500]$ | Lakeflow Quarantine routing & Delta `CHECK` constraints |
| **4. Consistency** | Referential integrity & cross-column partitioning | Foreign keys exist in dimensions; $\text{safe} + \text{moderate} + \text{unhealthy} = \text{total}$ | Medallion Reconciliation Runner (`run_dq_reconciliation.py`) |
| **5. Timeliness** | Data freshness and absence of future-dated records | $\text{recorded\_at} \le \text{current\_timestamp}()$; latency $< 6\,\text{hrs}$ | Quarantine routing & SQL Telemetry alerts |

---

## 4. The Enterprise Quarantine Table Pattern

### Why NOT `@dlt.expect_or_drop` or `@dlt.expect_or_fail`?
- `@dlt.expect_or_drop`: Silently deletes corrupted records. Causes **silent data loss** and invalidates financial/regulatory audits.
- `@dlt.expect_or_fail`: Crashes the entire pipeline on 1 corrupted record out of 10 million. Causes **pipeline fragility**.

### The Dual-Path Silver Routing Pattern
In [`02_silver_transformations.py`](file:///Users/omatviiv/learning/databricks/06-gold-bi-governance/02_silver_transformations.py):
1. **`silver_air_quality_enriched`**: Receives only records that pass all validation rules.
2. **`silver_air_quality_quarantine`**: Receives non-compliant records enriched with diagnostic metadata:
   - `quarantine_reason`: Pipe-separated error codes (e.g. `"LATITUDE_OUT_OF_BOUNDS | NEGATIVE_PM25"`).
   - `quarantined_at`: Timestamp when the record was quarantined.

**Invariant Guaranteed**:
$$\text{Count}(\text{Bronze}) = \text{Count}(\text{Silver Enriched}) + \text{Count}(\text{Silver Quarantine})$$
*(Zero silent data loss; complete audit trail).*

---

## 5. Delta Lake Storage Engine Constraints

While Lakeflow expectations enforce rules during pipeline execution, **Delta Table Constraints** enforce rules at the storage layer for **any** write (ad-hoc SQL, direct Spark scripts, or manual backfills).

Defined in [`08_delta_table_constraints.sql`](file:///Users/omatviiv/learning/databricks/06-gold-bi-governance/08_delta_table_constraints.sql):
```sql
-- Non-negative pollutant concentrations
ALTER TABLE silver_air_quality_enriched 
  ADD CONSTRAINT chk_silver_pm25_non_negative 
  CHECK (pm2_5 IS NULL OR pm2_5 >= 0.0);

-- US AQI range [0..500]
ALTER TABLE silver_air_quality_enriched 
  ADD CONSTRAINT chk_silver_aqi_scale 
  CHECK (us_aqi IS NULL OR (us_aqi >= 0 AND us_aqi <= 500));

-- Mandatory Foreign Keys
ALTER TABLE fact_air_quality_hourly 
  ALTER COLUMN station_sk SET NOT NULL;

-- Partition integrity on daily rollups
ALTER TABLE fact_city_daily_summary 
  ADD CONSTRAINT chk_summary_hours_partition_integrity 
  CHECK ((hours_safe + hours_moderate + hours_unhealthy) <= observation_count);
```

---

## 6. Cross-Layer Medallion Reconciliation Engine

The automated reconciliation runner [`run_dq_reconciliation.py`](file:///Users/omatviiv/learning/databricks/06-gold-bi-governance/run_dq_reconciliation.py) acts as a mathematical accounting gate:

1. **Row Count Conservation**:
   $$\text{Bronze Rows} = \text{Silver Valid Rows} + \text{Quarantined Rows}$$
2. **Grain Rollup Conservation**:
   $$\sum(\text{fact\_city\_daily\_summary.observation\_count}) = \text{Count}(\text{fact\_air\_quality\_hourly})$$
3. **Dimensional Referential Integrity**:
   Checks that 100% of `station_sk`, `date_sk`, and `category_sk` in fact tables resolve to dimension tables (zero orphan foreign keys).
4. **Temporal Integrity**:
   Checks that zero records have future-dated observation timestamps.

**Exit Code**: Returns `0` on PASS, `1` on FAIL (blocking downstream CI/CD deployment or triggering alerts).

---

## 7. CI/CD Gating & Pre-Commit Hook

### GitHub Actions CI Workflow (`.github/workflows/ci.yml`)
- Triggers on every pull request and push to `main`.
- Step 1: Runs the 27 unit tests unconditionally. Blocks PR merge if any test fails.
- Step 2: Validates the Databricks Asset Bundle (`databricks bundle validate -t dev_free`) if `DATABRICKS_DEV_FREE_TOKEN` is configured.

### Local Git Pre-Commit Hook
To block commits locally if tests fail:
```bash
chmod +x .git/hooks/pre-commit
```

---

## 8. Databricks Asset Bundle (DAB) Workflow Integration

The orchestrated workflow job in [`databricks.yml`](file:///Users/omatviiv/learning/databricks/06-gold-bi-governance/databricks.yml) chains all stages:

```
[ Task 1: Setup Landing Data ]
              |
              v
[ Task 2: Full Medallion Lakeflow Pipeline (Bronze -> Silver Valid/Quarantine -> Gold) ]
              |
              v
[ Task 3: Unity Catalog Governance (RLS / CLS) ]
              |
              v
[ Task 4: Automated Volume Drop Monitoring & Alerts ]
              |
              v
[ Task 5: Medallion Cross-Layer Reconciliation Gate (run_dq_reconciliation.py) ]
```

### Validate Bundle Configuration:
```bash
# Validate against Databricks Free trial target
databricks bundle validate -t dev_free
```
