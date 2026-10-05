# Platform Operations CLI & Automation Suite (Lab 9)

Production-grade automation suite for platform operations, compute provisioning, job triggering, and pipeline orchestration using the **Databricks Python SDK** and **Databricks REST API 2.0 / 2.1**.

---

## 1. Key Capabilities & Design Philosophy

1. **Dual API Architecture**:
   - Uses `databricks-sdk` (`WorkspaceClient`) for modern, typed Python workflows.
   - Uses direct HTTP requests to Databricks REST API (`/api/2.0/clusters/create`, `/api/2.1/jobs/run-now`, `/api/2.0/pipelines`) for fine-grained control and portability.
2. **Enterprise Cost-Protection Guardrail**:
   - Built-in `SafetyGuardrailError` prevents accidental compute creation or job execution on `prod_azure`.
   - All live executions run on `dev_free` (Databricks Serverless / Free Tier) at $0 cost.
   - Any attempt to run jobs on `prod_azure` requires the explicit `--force-paid-run` flag.
3. **End-to-End Orchestrator**:
   - `e2e_platform_run.py` provisions minimal compute, discovers jobs, triggers execution, streams task-level telemetry, and cleans up compute automatically.

---

## 2. Installation & Prerequisites

From the repository root:
```bash
pip install -r platform_ops/requirements.txt
```

Verify your authentication tokens:
```bash
# For dev_free target:
export DATABRICKS_HOST="https://dbc-3d34d2aa-8db9.cloud.databricks.com"
export DATABRICKS_TOKEN="<your-dev-free-pat>"

# Or for prod_azure target:
export DATABRICKS_PROD_AZURE_TOKEN="<your-azure-pat>"
```

---

## 3. CLI Reference (`lakehouse_ops.py`)

### 1. Environment & Connectivity Check
```bash
# Verify connection to dev_free
python3 platform_ops/lakehouse_ops.py --target dev_free check-env

# Verify connection to prod_azure
python3 platform_ops/lakehouse_ops.py --target prod_azure check-env
```

### 2. Compute Lifecycle Management
```bash
# List all active clusters
python3 platform_ops/lakehouse_ops.py --target dev_free cluster list

# Provision a single-node on-demand cluster with 10-minute auto-termination
python3 platform_ops/lakehouse_ops.py --target dev_free cluster create \
  --name "test-worker-01" \
  --spark-version "15.4.x-scala2.12" \
  --autoterminate 10

# Check cluster state
python3 platform_ops/lakehouse_ops.py --target dev_free cluster status --cluster-id <cluster-id>

# Terminate a cluster
python3 platform_ops/lakehouse_ops.py --target dev_free cluster terminate --cluster-id <cluster-id>
```

> **Guardrail Verification**:
> If you run `python3 platform_ops/lakehouse_ops.py --target prod_azure cluster create --name "expensive"`, the CLI will halt and display:  
> `🛡️ [COST-PROTECTION GUARDRAIL BLOCKED] Action 'create_single_node_cluster' is strictly prohibited on 'prod_azure'.`

### 3. Workflow Jobs Management
```bash
# List all jobs
python3 platform_ops/lakehouse_ops.py --target dev_free job list

# Filter jobs by name
python3 platform_ops/lakehouse_ops.py --target dev_free job list --filter "Air Quality"

# Trigger a job and immediately monitor it
python3 platform_ops/lakehouse_ops.py --target dev_free job trigger \
  --job-name "[dev_free] Air Quality Quality & Reconciliation Audit Suite" \
  --monitor

# Check status of an existing run
python3 platform_ops/lakehouse_ops.py --target dev_free job status --run-id <run-id>
```

### 4. Declarative Lakeflow Pipelines
```bash
# List pipelines
python3 platform_ops/lakehouse_ops.py --target dev_free pipeline list

# Trigger pipeline update
python3 platform_ops/lakehouse_ops.py --target dev_free pipeline trigger \
  --pipeline-id <pipeline-id>
```

---

## 4. End-to-End Automated Platform Runner (`e2e_platform_run.py`)

Fulfills the Week 9 final completion criterion:
> *"Done when: a script provisions compute, runs a job, and reports its status end to end."*

Execute:
```bash
python3 platform_ops/e2e_platform_run.py --target dev_free
```

Execution steps handled automatically:
1. Validates connection to `dev_free`.
2. Provisions a temporary single-node cluster (`autotermination = 10m`).
3. Discovers the deployed Air Quality job.
4. Triggers execution and monitors task transitions (`PENDING` $\rightarrow$ `RUNNING` $\rightarrow$ `SUCCESS`).
5. Terminates the temporary cluster to guarantee zero lingering resource costs.
6. Emits exit code `0` on success.
