# Lab 07: Unit Testing & Medallion Data Quality Framework

Automated data quality framework, pure transformation unit testing, enterprise quarantine routing, and cross-layer reconciliation for the Air Quality Medallion Lakehouse.

---

## Deployment Artifacts

| Asset | Environment | Identifier / Details | Reference |
|---|---|---|---|
| **Lakeflow Pipeline** | Prod (Azure Databricks) | `d4426afc-621d-4588-8af2-92563b8a0a3e` | Air Quality Full Medallion Pipeline |
| **Workflow Job** | Prod (Azure Databricks) | `594339360399957` | Air Quality Daily End-to-End Workflow |
| **SQL Telemetry Alert** | Prod (Azure Databricks) | `4174171081479852` | Air Quality Volume Drop & Anomaly Alert |
| **Workspace Target** | Prod (Azure Databricks) | `lab06_gold_bi_governance/prod_azure` | Unity Catalog Lakehouse |
| **GitHub Pull Request #3** | GitHub | Branch `lab7` | [Open PR #3](https://github.com/valmtv/databricks/pull/3) |
| **GitHub Repository** | GitHub | `06-gold-bi-governance` | [View Code](https://github.com/valmtv/databricks/tree/lab7/06-gold-bi-governance) |

## 1. Medallion Quality Flow & Quarantine Routing

```
[ Bronze Layer ]  bronze_air_quality_raw (Streaming Table)
                         |
                         +-----------------------------------+
                         | Valid Records                     | Quarantined Records
                         v                                   v
[ Silver Layer ]  silver_air_quality_enriched       silver_air_quality_quarantine
                  (DLT Expectations)                (Reason Codes + Ingestion Meta)
                         |
                         v
[ Gold Layer ]    dim_city, dim_calendar_date, dim_aqi_category (Dimensions)
                  fact_air_quality_hourly, fact_city_daily_summary (Facts)
                         |
                         v
[ Quality Gates ] vw_data_quality_scorecard (Executive KPI Scorecard)
                  vw_quarantine_diagnostics (Defect Pareto Breakdown)
                  run_dq_reconciliation.py (Mathematical Accounting & Foreign Key Audit)
```

---

## 2. Transformation Units & Pure Python Design

Business transformations are decoupled from Databricks runtime into pure Python functions under `air_quality_transforms/`:

| Module | Core Responsibility | Key Logic / Boundaries |
|---|---|---|
| `cleansing.py` | Data hygiene & coordinate bounds | Earth coordinate bounds ($[-90..90], [-180..180]$), non-negative pollutants, whitespace trimming |
| `enrichment.py` | EPA standard classifications | US EPA AQI category tiers (Good $\rightarrow$ Hazardous), hex color mappings |
| `features.py` | Analytics attributes & surrogate keys | Deterministic surrogate key hashing (`*_sk`), WHO 24h exceedance ($> 15\,\mu\text{g/m}^3$), severity scores ($1.0..6.0$) |
| `aggregations.py` | Fact table rollups | Daily grain aggregations, compliance grading (A/B/C/D), unhealthy hours percentage |
| `quarantine.py` | Defect tagging & routing | Zero-silent-loss evaluation, pipe-separated defect reason tagging |

### Unit Testing Suite
All **27 unit tests** execute locally in **0.003s** without cluster compute:
```bash
python3 run_tests.py -v
```
Automated pre-merge verification runs via GitHub Actions (`.github/workflows/ci.yml`).

---

## 3. Enterprise Quarantine Pattern (Zero Silent Data Loss)

Instead of silently dropping non-compliant records (`expect_or_drop`), records failing physical bounds or mandatory key rules route into `silver_air_quality_quarantine`:
- **`quarantine_reason`**: Pipe-delimited defect tags (e.g. `MISSING_MANDATORY_KEYS | NEGATIVE_PM25 | OUT_OF_BOUNDS_COORDINATES`).
- **`quarantined_at`**: Audit timestamp of rejection.

### Mathematical Accounting Invariant:
$$\text{Count}(\text{Bronze}) = \text{Count}(\text{Silver Clean}) + \text{Count}(\text{Silver Quarantine})$$

---

## 4. Cross-Layer Reconciliation Suite (`run_dq_reconciliation.py`)

Automated post-ingestion audit gate verifying mathematical and relational invariants:

1. **Row Count Conservation**: $\text{Bronze Ingested} - (\text{Silver Clean} + \text{Silver Quarantine}) = 0$.
2. **Grain Rollup Conservation**: $\sum(\text{Daily Observation Counts}) = \text{Count}(\text{Hourly Facts})$.
3. **Dimensional Referential Integrity**: Zero orphan foreign keys across `station_sk`, `date_sk`, and `category_sk`.
4. **Temporal Freshness**: Zero future-dated observations in Gold facts.

Exit Code: returns `0` on 100% pass, `1` on failure to block downstream consumers.

---

## 5. Data Quality Scorecard & Diagnostics (`09_dq_scorecard_and_alerts.sql`)

- **`vw_data_quality_scorecard`**: Computes live clean ingestion rate, quarantine percentage, silent loss gap, and overall health status (`HEALTHY (100% RECONCILED)` vs `DEGRADED`).
- **`vw_quarantine_diagnostics`**: Aggregates defect volume by `quarantine_reason` for Pareto root-cause analysis.
- **Automated Alerts**: Queries alerting when `quarantine_rate_pct > 2.0%` or silent data loss occurs.

---

## 6. Enterprise Date Dimension & Recursive CTE (`00b_generate_dim_date.sql`)

Rather than scanning and shuffling the streaming Silver table on every pipeline refresh, `dim_calendar_date` is decoupled from Lakeflow and generated as an enterprise conformed dimension using Databricks Recursive CTEs (`WITH RECURSIVE Dates MAX RECURSION LEVEL 4500`):
- Pre-seeds dates from 2020-01-01 to 2030-12-31 with full calendar attributes (`date_sk`, `year`, `quarter`, `month`, `day_of_month`, `day_of_week`, `day_name`, `day_of_year`, `last_day_of_month`, `is_weekend`).
- Employs Delta Lake Liquid Clustering (`CLUSTER BY (date_sk, calendar_date)`).
- **Resource Savings**: Drastically reduces Lakeflow CPU and I/O by eliminating redundant distributed `distinct()` operations on streaming data.

---

## 7. Delta Lake Storage Engine Constraints (`08_delta_table_constraints.py`)

Enforces defense-in-depth data integrity directly at the Delta storage layer:
- **Silver layer**: `CHECK` constraints on valid coordinate ranges ($[-90..90], [-180..180]$), non-negative PM2.5/PM10 concentrations, and US AQI scale ($[0..500]$).
- **Gold facts & summaries**: `NOT NULL` constraints on surrogate keys, positive observation counts, and partition bounds.
- **Lakeflow Awareness**: Distinguishes standard Delta tables from Lakeflow-managed streaming tables/materialized views, avoiding runtime `STREAMING_TABLE_OPERATION_NOT_ALLOWED` crashes.

---

## 8. Declarative Quality Suite with Databricks Labs DQX (`dq_rules.yml` & `10_dqx_quality_engine.py`)

Implements declarative rules across all 5 data quality dimensions:
- **Completeness**: Checks null-free mandatory keys.
- **Validity**: Evaluates coordinate bounds, non-negative pollutants, and AQI scale.
- **Timeliness**: Rejects future-dated timestamps.
- **Consistency**: Enforces relationship invariants.
- **Uniqueness**: Asserts uniqueness on composite business keys.

---

## 9. Orchestrated Pipeline Execution & Resource Optimization

### Pipeline Architecture & Orchestration

The project is structured into three decoupled Databricks Asset Bundle workflows to separate infrastructure DDL, ETL processing, and quality auditing:

```
1. Infrastructure & Governance Setup (Run-Once / DDL / Migrations)
   generate_dim_date ──> delta_table_constraints ──> governance_rls_cls ──> dq_scorecard_and_alerts

2. Data Loading Pipeline (Stateless ETL / Scheduled)
   setup_landing_data ──> air_quality_lakeflow_pipeline (Bronze -> Silver -> Gold)

3. Quality & Audit Suite (Verification & Reconciliation)
   dq_reconciliation_audit ──> dqx_quality_engine ──> alerts_monitoring
```

#### Workflow Specifications:
1. **Infrastructure & Governance (`air_quality_infrastructure_setup`)**:
   - Provisions static reference calendar dimensions (`00b_generate_dim_date.sql`), enforces Delta Lake table constraints (`08_delta_table_constraints.py`), applies Unity Catalog Row Filters & Column Masks (`04_governance_rls_cls.sql`), and deploys quality scorecard views (`09_dq_scorecard_and_alerts.sql`).
   - Run command: `databricks bundle run air_quality_infrastructure_setup`

2. **Data Pipeline (`air_quality_data_pipeline`)**:
   - Pure stateless ETL. Stages landing telemetry and triggers the declarative Medallion Lakeflow pipeline (`01_bronze_ingestion.py` $\to$ `02_silver_transformations.py` $\to$ `03_gold_star_schema.py`).
   - Run command: `databricks bundle run air_quality_data_pipeline`

3. **Quality & Audit Suite (`air_quality_quality_and_audit_suite`)**:
   - Independent verification suite. Executes cross-layer conservation reconciliation (`run_dq_reconciliation.py`), evaluates Databricks Labs DQX rule engine (`10_dqx_quality_engine.py`), and executes volume SLA alerts (`05_alerts_and_monitoring.sql`).
   - Run command: `databricks bundle run air_quality_quality_and_audit_suite`

### Target Environment Profiles:
- **`dev_free`**: Fully automated on Serverless compute with Unity Catalog.
- **`prod_azure`**: Reuses the active GP1 cluster (`0702-132442-toro5spu`) with classic cluster task libraries (`databricks-labs-dqx==0.8.0`), avoiding serverless or extra VM provisioning overhead.
