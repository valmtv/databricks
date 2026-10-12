# Platform Automation & Orchestration (Lab 9)

Platform operations tooling for Databricks compute lifecycle management, workflow execution, and declarative Lakeflow pipeline automation using the **Databricks Python SDK** (`databricks-sdk`) and **Databricks REST API 2.0 / 2.1**.

---

## 1. Compute & Environment Strategy

1. **Production Workspace (`prod_azure`)**:
   - Reuses the shared auto-terminating **`GP1`** cluster (**Cluster ID: `0702-132442-toro5spu`**, `Standard_F4`, 60-minute auto-termination).
   - Before running jobs, `ensure-compute` verifies `GP1` state and starts it if terminated.
   - When jobs complete, Databricks automatically terminates `GP1` after 60 minutes of inactivity, avoiding orphaned cloud compute.
2. **Development Workspace (`dev_free`)**:
   - Runs against Databricks Free Edition using serverless compute.
3. **Execution Engines**:
   - **Rich SDK Mode**: Uses `typer`, `rich`, and `databricks-sdk` for interactive CLI usage and formatted progress tracking.
   - **Portable Mode**: Automatic fallback using standard library HTTP/JSON with zero external dependencies.

---

## 2. Orchestrator CLI (`orchestrator.py`)

Primary automation interface:

```bash
# Ensure GP1 cluster is running on Azure PROD
python3 platform_ops/orchestrator.py ensure-compute

# Run a specific workflow job and stream task status
python3 platform_ops/orchestrator.py run --target prod_azure --job "[prod_azure] Air Quality Infrastructure & Governance Setup (DDL)"

# Trigger a Lakeflow declarative pipeline and monitor to completion
python3 platform_ops/orchestrator.py start-pipeline --target prod_azure --pipeline-name "[prod_azure] Air Quality Lakeflow Declarative Pipeline"
```

---

## 3. Operations CLI (`lakehouse_ops.py`)

Extended command-line tool for granular workspace inspection and management:

### Environment & Connectivity
```bash
python3 platform_ops/lakehouse_ops.py --target prod_azure check-env
python3 platform_ops/lakehouse_ops.py --target dev_free check-env
```

### Compute Operations
```bash
# List all workspace clusters
python3 platform_ops/lakehouse_ops.py --target prod_azure cluster list

# Check status of GP1
python3 platform_ops/lakehouse_ops.py --target prod_azure cluster status --cluster-id 0702-132442-toro5spu
```

### Workflow Jobs
```bash
# List jobs matching filter
python3 platform_ops/lakehouse_ops.py --target prod_azure job list --filter "Air Quality"

# Trigger a job and monitor execution
python3 platform_ops/lakehouse_ops.py --target prod_azure job trigger \
  --job-name "[prod_azure] Air Quality Infrastructure & Governance Setup (DDL)" \
  --monitor
```

### Lakeflow Pipelines
```bash
# List pipelines
python3 platform_ops/lakehouse_ops.py --target prod_azure pipeline list

# Start pipeline update and wait for completion
python3 platform_ops/lakehouse_ops.py --target prod_azure start-pipeline \
  --pipeline-name "[prod_azure] Air Quality Lakeflow Declarative Pipeline"
```

---

## 4. End-to-End Test Runner (`e2e_platform_run.py`)

Automates cluster provisioning, job execution, status monitoring, and cluster cleanup in a single end-to-end run:

```bash
python3 platform_ops/e2e_platform_run.py --target dev_free
```
