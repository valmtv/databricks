#!/usr/bin/env python3
"""
End-to-End Platform Operations Automation Script (Lab 9 Capstone)
Orchestrates:
  1. Safe Environment Discovery (targets dev_free with Zero-Cost Azure Guardrail)
  2. Compute Lifecycle (provisions/validates minimal single-node cluster with 10m auto-term)
  3. Job Triggering (executes deployed Medallion Lakehouse Job)
  4. Telemetry & Log Monitoring (tracks task-level execution until terminal state)
  5. Teardown / Cleanup (safely terminates provisioned cluster)
"""

import sys
import os
import time
import argparse
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from client import PlatformClient, SafetyGuardrailError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("E2EPlatformRunner")


def run_e2e(
    target: str = "dev_free",
    job_name_filter: str = "Air Quality Infrastructure & Governance Setup (DDL)",
    provision_compute: bool = True,
    autoterminate_minutes: int = 10,
    force_paid_run: bool = False
):
    logger.info("=================================================================")
    logger.info(" Starting Lab 9 End-to-End Databricks Platform Automation")
    logger.info(f" Target Workspace: {target}")
    logger.info("=================================================================")

    client = PlatformClient(target=target, force_paid_run=force_paid_run)

    # 0. Early Zero-Cost Guardrail Enforcement
    if target == "prod_azure" and not force_paid_run:
        logger.error("🛡️ [COST-PROTECTION GUARDRAIL BLOCKED] E2E automated execution is strictly prohibited on 'prod_azure'.")
        logger.error("   Per project guidelines, all compute provisioning and job runs must execute on 'dev_free'.")
        logger.error("   To bypass in an emergency, pass --force-paid-run.")
        return False

    # 1. Environment & Connectivity Verification
    logger.info("[Step 1/5] Verifying API authentication and connectivity...")
    env_info = client.check_connection()
    logger.info(f"Connected to {env_info['host']} as user '{env_info['authenticated_user']}'")

    created_cluster_id = None
    try:
        # 2. Compute Lifecycle Management
        if provision_compute:
            logger.info("[Step 2/5] Provisioning minimal single-node on-demand cluster...")
            cluster_name = f"platform-ops-e2e-{int(time.time())}"
            try:
                created_cluster_id = client.create_single_node_cluster(
                    cluster_name=cluster_name,
                    spark_version="15.4.x-scala2.12",
                    node_type=None,
                    autoterminate_minutes=autoterminate_minutes
                )
                logger.info(f"Created cluster {created_cluster_id}. Inspecting initial status...")
                c_info = client.get_cluster(created_cluster_id)
                logger.info(f"Cluster '{c_info.get('cluster_name')}' state: {c_info.get('state')}")
            except SafetyGuardrailError as sge:
                logger.warning(f"Guardrail triggered: {sge}")
                raise
        else:
            logger.info("[Step 2/5] Skipping cluster creation (serverless / pre-existing compute selected).")

        # 3. Discover Deployed Workflow Job
        logger.info(f"[Step 3/5] Locating deployed job matching '{job_name_filter}'...")
        jobs = client.list_jobs(name_filter=job_name_filter)
        if not jobs:
            # Fallback search for any air quality job in the workspace
            jobs = client.list_jobs(name_filter="Air Quality")

        if not jobs:
            logger.warning(f"No jobs matching '{job_name_filter}' found in workspace. Searching all jobs...")
            all_jobs = client.list_jobs()
            if not all_jobs:
                logger.error("No jobs exist in target workspace. Please deploy the bundle first via 'databricks bundle deploy'.")
                return False
            selected_job = all_jobs[0]
        else:
            selected_job = jobs[0]

        job_id = selected_job["job_id"]
        job_name = selected_job.get("settings", {}).get("name", "Unnamed")
        logger.info(f"Selected Job: '{job_name}' (ID: {job_id})")

        # 4. Trigger and Monitor Job Run
        logger.info(f"[Step 4/5] Triggering Job Run via Databricks REST API...")
        run_id = client.trigger_job(job_id=job_id)
        logger.info(f"Job triggered. Run ID: {run_id}. Beginning telemetry monitoring...")

        run_result = client.monitor_run(run_id=run_id, timeout_seconds=900, poll_interval=15)
        state = run_result.get("state", {})
        result_state = state.get("result_state", "UNKNOWN")
        life_cycle_state = state.get("life_cycle_state", "UNKNOWN")

        logger.info("-----------------------------------------------------------------")
        logger.info(f"Job Execution Finished: LifeCycle={life_cycle_state}, Result={result_state}")
        logger.info(f"Run Details URL: {run_result.get('run_page_url')}")
        logger.info("-----------------------------------------------------------------")

        tasks = run_result.get("tasks", [])
        for task in tasks:
            t_key = task.get("task_key")
            t_state = task.get("state", {}).get("result_state", "N/A")
            t_dur = task.get("execution_duration", 0) / 1000.0
            logger.info(f"  * Task '{t_key}': {t_state} (Duration: {t_dur:.1f}s)")

        return result_state == "SUCCESS"

    finally:
        # 5. Compute Teardown & Cleanup
        if created_cluster_id:
            logger.info(f"[Step 5/5] Tearing down temporary test cluster {created_cluster_id} to ensure zero cost...")
            try:
                client.terminate_cluster(created_cluster_id)
                logger.info("Cluster termination signal sent successfully.")
            except Exception as e:
                logger.error(f"Error terminating cluster: {e}")


def main():
    parser = argparse.ArgumentParser(description="Lab 9 Platform Operations E2E Runner")
    parser.add_argument("--target", default="dev_free", choices=["dev_free", "prod_azure"], help="Target Databricks environment")
    parser.add_argument("--job-filter", default="Air Quality Infrastructure & Governance Setup (DDL)", help="Job name filter")
    parser.add_argument("--no-cluster", action="store_true", help="Do not provision a temporary cluster")
    parser.add_argument("--force-paid-run", action="store_true", help="Bypass safety guardrails on prod_azure")
    args = parser.parse_args()

    success = run_e2e(
        target=args.target,
        job_name_filter=args.job_filter,
        provision_compute=not args.no_cluster,
        force_paid_run=args.force_paid_run
    )

    if success:
        logger.info("✅ End-to-End Platform Automation Run SUCCEEDED!")
        sys.exit(0)
    else:
        logger.error("❌ End-to-End Platform Automation Run FAILED or returned non-success state.")
        sys.exit(1)


if __name__ == "__main__":
    main()
