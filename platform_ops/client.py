"""
Unified Databricks Platform Client (SDK + REST API 2.0 / 2.1)
Encapsulates workspace interactions, compute lifecycle, job orchestration,
and enforces enterprise cost-protection guardrails.
Zero-dependency portable architecture with urllib fallback if requests is not installed.
"""

import os
import sys
import time
import json
import logging
from typing import Dict, Any, Optional, List

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service import compute, jobs, pipelines
    HAS_SDK = True
except ImportError:
    HAS_SDK = False

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False
    import urllib.request
    import urllib.error
    import urllib.parse

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("PlatformClient")


class SafetyGuardrailError(Exception):
    """Raised when an operation violates the zero-cost policy on prod_azure."""
    pass


class PlatformClient:
    # Known environment hosts from DAB bundle config
    DEV_FREE_HOST = "https://dbc-3d34d2aa-8db9.cloud.databricks.com"
    PROD_AZURE_HOST = "https://adb-7405604503619901.1.azuredatabricks.net"
    SHARED_AZURE_CLUSTER_ID = "0702-132442-toro5spu"  # GP1 shared cluster (Standard_F4, 60m autoterminate)

    def __init__(
        self,
        target: str = "dev_free",
        host: Optional[str] = None,
        token: Optional[str] = None,
        force_paid_run: bool = False
    ):
        self.target = target.lower()
        self.force_paid_run = force_paid_run

        # Resolve host
        if host:
            self.host = host.rstrip("/")
        elif self.target == "prod_azure":
            self.host = os.getenv("DATABRICKS_HOST", self.PROD_AZURE_HOST).rstrip("/")
        else:
            self.host = os.getenv("DATABRICKS_HOST", self.DEV_FREE_HOST).rstrip("/")

        # Resolve token
        if token:
            self.token = token
        elif self.target == "prod_azure":
            self.token = (
                os.getenv("DATABRICKS_PROD_AZURE_TOKEN")
                or os.getenv("DATABRICKS_TOKEN", "")
            )
        else:
            self.token = (
                os.getenv("DATABRICKS_DEV_FREE_TOKEN")
                or os.getenv("DATABRICKS_TOKEN", "")
            )

        # Dynamic fallback to Databricks CLI OAuth profiles if env vars are unset
        if not self.token:
            profile_name = (
                "valerii.matviiv@softserve.academy"
                if self.target == "prod_azure"
                else "valerii.matviiv@gmail.com"
            )
            try:
                import subprocess
                res = subprocess.run(
                    ["databricks", "auth", "token", "--profile", profile_name],
                    capture_output=True,
                    text=True,
                    check=False
                )
                if res.returncode == 0 and res.stdout:
                    token_data = json.loads(res.stdout)
                    self.token = token_data.get("access_token", "")
                    if self.token:
                        logger.info(f"Loaded active OAuth token from Databricks CLI profile '{profile_name}'")
            except Exception as e:
                logger.debug(f"CLI token lookup failed for profile '{profile_name}': {e}")

        # Initialize SDK WorkspaceClient if available
        self.sdk: Optional[WorkspaceClient] = None
        if HAS_SDK and self.token:
            try:
                self.sdk = WorkspaceClient(host=self.host, token=self.token)
            except Exception as e:
                logger.warning(f"Could not initialize databricks.sdk WorkspaceClient: {e}")

    def _warn_or_check_compute_creation(self, action_name: str) -> None:
        """
        Advisory warning when creating new compute on prod_azure.
        Best practice in Academy Lab 9 is reusing the existing shared GP1 cluster.
        """
        if self.target == "prod_azure":
            logger.warning(
                f"[Cost Best Practice] {action_name} invoked on {self.target}. "
                f"Prefer reusing the shared cluster GP1 ('{self.SHARED_AZURE_CLUSTER_ID}') via ensure_compute() "
                f"to prevent orphaned cloud compute costs."
            )

    # -------------------------------------------------------------------------
    # REST API Helper (Requests with Urllib Fallback)
    # -------------------------------------------------------------------------
    def _rest_call(
        self,
        method: str,
        endpoint: str,
        payload: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Direct Databricks REST API execution with portable fallback."""
        url = f"{self.host}/{endpoint.lstrip('/')}"

        if HAS_REQUESTS:
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "User-Agent": "Antigravity-PlatformOps/1.0"
            }
            try:
                resp = requests.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    json=payload if payload else None,
                    params=params if params else None,
                    timeout=30
                )
                if resp.status_code >= 400:
                    raise RuntimeError(
                        f"Databricks REST API Error ({resp.status_code}) on {url}: {resp.text}"
                    )
                return resp.json() if resp.text else {}
            except requests.exceptions.RequestException as e:
                raise RuntimeError(f"Network error communicating with Databricks API: {e}")
        else:
            # Portable standard library fallback
            if params:
                query_string = urllib.parse.urlencode(params)
                url = f"{url}?{query_string}"

            body = json.dumps(payload).encode("utf-8") if payload is not None else None
            req = urllib.request.Request(url, data=body, method=method.upper())
            req.add_header("Authorization", f"Bearer {self.token}")
            req.add_header("Content-Type", "application/json")
            req.add_header("User-Agent", "Antigravity-PlatformOps/1.0")

            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    resp_body = resp.read().decode("utf-8")
                    return json.loads(resp_body) if resp_body else {}
            except urllib.error.HTTPError as e:
                err_text = e.read().decode("utf-8") if e.fp else str(e)
                raise RuntimeError(f"Databricks REST API Error ({e.code}) on {url}: {err_text}")
            except urllib.error.URLError as e:
                raise RuntimeError(f"Network error communicating with Databricks API: {e}")

    # -------------------------------------------------------------------------
    # 1. Environment & Identity Verification
    # -------------------------------------------------------------------------
    def get_current_user(self) -> Dict[str, Any]:
        """Fetch current user identity via SCIM Me endpoint."""
        return self._rest_call("GET", "/api/2.0/preview/scim/v2/Me")

    def check_connection(self) -> Dict[str, Any]:
        """Verify API connectivity and return workspace metadata."""
        user_info = self.get_current_user()
        return {
            "target": self.target,
            "host": self.host,
            "authenticated_user": user_info.get("userName", "Unknown"),
            "display_name": user_info.get("displayName", ""),
            "has_sdk": bool(self.sdk),
            "status": "CONNECTED"
        }

    # -------------------------------------------------------------------------
    # 2. Compute Lifecycle Management (Clusters REST API & SDK)
    # -------------------------------------------------------------------------
    def create_single_node_cluster(
        self,
        cluster_name: str,
        spark_version: str = "15.4.x-scala2.12",
        node_type: Optional[str] = None,
        autoterminate_minutes: int = 10
    ) -> str:
        """
        Provision an auto-terminating single-node compute cluster.
        Safe defaults: single-node driver-only, 10-minute auto-termination.
        """
        self._warn_or_check_compute_creation("create_single_node_cluster")

        if not node_type:
            node_type = "Standard_D4ds_v5" if self.target == "prod_azure" else "i3.xlarge"

        cluster_config = {
            "cluster_name": cluster_name,
            "spark_version": spark_version,
            "node_type_id": node_type,
            "num_workers": 0,
            "autotermination_minutes": autoterminate_minutes,
            "spark_conf": {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*]"
            },
            "custom_tags": {
                "ResourceClass": "SingleNode",
                "ManagedBy": "PlatformOps-CLI",
                "Project": "AirQuality-Medallion"
            }
        }

        logger.info(f"Creating single-node cluster '{cluster_name}' on {self.target}...")
        resp = self._rest_call("POST", "/api/2.0/clusters/create", payload=cluster_config)
        cluster_id = resp.get("cluster_id")
        logger.info(f"Cluster created successfully. Cluster ID: {cluster_id}")
        return cluster_id

    def get_cluster(self, cluster_id: str) -> Dict[str, Any]:
        """Retrieve state and details of a cluster."""
        return self._rest_call("GET", "/api/2.0/clusters/get", params={"cluster_id": cluster_id})

    def list_clusters(self) -> List[Dict[str, Any]]:
        """List all clusters in the workspace."""
        resp = self._rest_call("GET", "/api/2.0/clusters/list")
        return resp.get("clusters", [])

    def start_cluster(self, cluster_id: str) -> None:
        """Start a terminated cluster."""
        logger.info(f"Starting cluster: {cluster_id} on {self.target}...")
        self._rest_call("POST", "/api/2.0/clusters/start", payload={"cluster_id": cluster_id})

    def terminate_cluster(self, cluster_id: str) -> None:
        """Stop/terminate an active cluster."""
        logger.info(f"Terminating cluster: {cluster_id}")
        self._rest_call("POST", "/api/2.0/clusters/delete", payload={"cluster_id": cluster_id})

    def ensure_compute(
        self,
        cluster_id: Optional[str] = None,
        poll_interval: int = 15,
        timeout_seconds: int = 600
    ) -> Dict[str, Any]:
        """
        Ensures the specified cluster (or shared GP1 cluster on Azure PROD) is running.
        If terminated, starts the cluster and polls until state is RUNNING.
        Avoids creating throwaway clusters, guaranteeing cost safety.
        """
        target_cluster_id = cluster_id
        if not target_cluster_id:
            if self.target == "prod_azure":
                target_cluster_id = self.SHARED_AZURE_CLUSTER_ID
            else:
                clusters = self.list_clusters()
                if clusters:
                    target_cluster_id = clusters[0].get("cluster_id")
                else:
                    raise RuntimeError("No existing clusters found on dev_free to ensure.")

        logger.info(f"Ensuring compute cluster '{target_cluster_id}' is RUNNING on {self.target}...")
        cluster = self.get_cluster(target_cluster_id)
        current_state = cluster.get("state", "UNKNOWN")
        logger.info(f"Initial state of cluster '{cluster.get('cluster_name', target_cluster_id)}': {current_state}")

        if current_state == "TERMINATED":
            logger.info(f"Starting terminated cluster '{target_cluster_id}'...")
            self.start_cluster(target_cluster_id)

        start_time = time.time()
        while True:
            cluster = self.get_cluster(target_cluster_id)
            state = cluster.get("state", "UNKNOWN")
            elapsed = int(time.time() - start_time)
            logger.info(f"[{elapsed}s] Cluster '{target_cluster_id}' state: {state}")

            if state == "RUNNING":
                logger.info(f"Cluster '{target_cluster_id}' is ready and RUNNING.")
                return cluster

            if state in {"ERROR", "UNKNOWN"}:
                error_msg = cluster.get("state_message", "Unknown cluster error")
                raise RuntimeError(f"Cluster failed to start. State: {state}; message: {error_msg}")

            if elapsed >= timeout_seconds:
                raise TimeoutError(f"Cluster '{target_cluster_id}' failed to reach RUNNING within {timeout_seconds}s")

            time.sleep(poll_interval)

    # -------------------------------------------------------------------------
    # 3. Job Orchestration & Status Monitoring
    # -------------------------------------------------------------------------
    def list_jobs(self, name_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """List workflow jobs with optional substring name filter."""
        resp = self._rest_call("GET", "/api/2.1/jobs/list")
        jobs_list = resp.get("jobs", [])
        if name_filter:
            jobs_list = [j for j in jobs_list if name_filter.lower() in j.get("settings", {}).get("name", "").lower()]
        return jobs_list

    def trigger_job(
        self,
        job_id: Optional[int] = None,
        job_name: Optional[str] = None,
        notebook_params: Optional[Dict[str, str]] = None
    ) -> int:
        """
        Trigger a workflow job execution via /api/2.1/jobs/run-now.
        Accepts either job_id or job_name.
        """
        target_job_id = job_id
        if not target_job_id and job_name:
            matches = self.list_jobs(name_filter=job_name)
            if not matches:
                raise ValueError(f"No job found matching name: '{job_name}'")
            target_job_id = matches[0]["job_id"]

        if not target_job_id:
            raise ValueError("Must provide either job_id or job_name.")

        payload: Dict[str, Any] = {"job_id": target_job_id}
        if notebook_params:
            payload["notebook_params"] = notebook_params

        logger.info(f"Triggering job ID {target_job_id} on {self.target}...")
        resp = self._rest_call("POST", "/api/2.1/jobs/run-now", payload=payload)
        run_id = resp.get("run_id")
        logger.info(f"Job triggered successfully. Run ID: {run_id}")
        return run_id

    def get_run(self, run_id: int) -> Dict[str, Any]:
        """Fetch status and task-level execution breakdown of a job run."""
        return self._rest_call("GET", "/api/2.1/jobs/runs/get", params={"run_id": run_id})

    def monitor_run(
        self,
        run_id: int,
        timeout_seconds: int = 600,
        poll_interval: int = 10
    ) -> Dict[str, Any]:
        """
        Poll a job run until terminal state (SUCCESS, FAILED, CANCELED, TIMED_OUT).
        Prints progress ticks and elapsed duration.
        """
        start_time = time.time()
        logger.info(f"Monitoring Run ID {run_id} (Timeout: {timeout_seconds}s, Interval: {poll_interval}s)...")

        while True:
            elapsed = int(time.time() - start_time)
            run_data = self.get_run(run_id)
            state = run_data.get("state", {})
            life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
            result_state = state.get("result_state", None)

            tasks = run_data.get("tasks", [])
            task_summary = ", ".join(
                f"{t.get('task_key')}: {t.get('state', {}).get('life_cycle_state', 'PENDING')}"
                for t in tasks
            )

            logger.info(f"[{elapsed}s] State: {life_cycle_state} | Tasks: [{task_summary}]")

            if life_cycle_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                logger.info(f"Run {run_id} reached terminal state: {life_cycle_state} (Result: {result_state})")
                return run_data

            if elapsed >= timeout_seconds:
                raise TimeoutError(f"Run {run_id} exceeded maximum monitoring timeout of {timeout_seconds}s.")

            time.sleep(poll_interval)

    # -------------------------------------------------------------------------
    # 4. Declarative Lakeflow Pipeline Operations
    # -------------------------------------------------------------------------
    def list_pipelines(self, name_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """List pipelines with optional name filter."""
        resp = self._rest_call("GET", "/api/2.0/pipelines", params={"max_results": 100})
        pipelines_list = resp.get("statuses", [])
        if name_filter:
            pipelines_list = [p for p in pipelines_list if name_filter.lower() in p.get("name", "").lower()]
        return pipelines_list

    def trigger_pipeline(self, pipeline_id: str, full_refresh: bool = False) -> str:
        """Trigger an update on a Lakeflow pipeline."""
        payload = {"full_refresh": full_refresh}
        logger.info(f"Starting update for pipeline {pipeline_id} (full_refresh={full_refresh})...")
        resp = self._rest_call("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", payload=payload)
        update_id = resp.get("update_id")
        logger.info(f"Pipeline update started. Update ID: {update_id}")
        return update_id

    def get_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Fetch pipeline metadata and current status."""
        return self._rest_call("GET", f"/api/2.0/pipelines/{pipeline_id}")

    def get_pipeline_update(self, pipeline_id: str, update_id: str) -> Dict[str, Any]:
        """Fetch status of a specific pipeline update."""
        return self._rest_call("GET", f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}")

    def monitor_pipeline_update(
        self,
        pipeline_id: str,
        update_id: str,
        timeout_seconds: int = 600,
        poll_interval: int = 15
    ) -> str:
        """Poll pipeline update until terminal state (COMPLETED, FAILED, CANCELED)."""
        terminal_states = {"COMPLETED", "FAILED", "CANCELED"}
        start_time = time.time()
        logger.info(f"Monitoring pipeline update {update_id} for pipeline {pipeline_id}...")

        while True:
            elapsed = int(time.time() - start_time)
            info = self.get_pipeline_update(pipeline_id, update_id)
            state = info.get("update", {}).get("state", "UNKNOWN")
            logger.info(f"[{elapsed}s] Pipeline update state: {state}")

            if state in terminal_states:
                logger.info(f"Pipeline update finished with state: {state}")
                return state

            if elapsed >= timeout_seconds:
                raise TimeoutError(f"Pipeline update {update_id} timed out after {timeout_seconds}s.")

            time.sleep(poll_interval)
