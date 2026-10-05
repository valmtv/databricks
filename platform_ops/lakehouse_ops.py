#!/usr/bin/env python3
"""
Lakehouse Ops CLI: Databricks Platform Automation Suite (Lab 9)
Supports Compute Lifecycle, Job Orchestration, Pipeline Tracking,
and Enforces Zero-Cost Guardrails on Azure PROD.
Zero-dependency architecture powered by standard library argparse.
"""

import sys
import os
import argparse
from typing import Optional, List

# Ensure local module is importable
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from client import PlatformClient, SafetyGuardrailError

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False


def print_table(headers: List[str], rows: List[List[str]]):
    """Pretty prints a table using tabulate or cleanly aligned standard output."""
    if HAS_TABULATE:
        print(tabulate(rows, headers=headers, tablefmt="github"))
    else:
        col_widths = [len(h) for h in headers]
        for row in rows:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)))

        header_str = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
        sep_str = "-+-".join("-" * col_widths[i] for i in range(len(headers)))
        print(header_str)
        print(sep_str)
        for row in rows:
            print(" | ".join(str(val).ljust(col_widths[i]) for i, val in enumerate(row)))


def cmd_check_env(client: PlatformClient, args):
    """Verify API authentication, host resolution, and workspace identity."""
    print(f"[*] Checking Databricks environment for target: {client.target}")
    try:
        info = client.check_connection()
        headers = ["Property", "Resolved Value"]
        rows = [
            ["Target Environment", info["target"]],
            ["Databricks Host", info["host"]],
            ["Authenticated User", info["authenticated_user"]],
            ["User Display Name", info["display_name"]],
            ["Databricks Python SDK", "Enabled" if info["has_sdk"] else "Disabled (REST fallback)"],
            ["API Connectivity", "SUCCESS (HTTP 200)"]
        ]
        print_table(headers, rows)
    except Exception as e:
        print(f"❌ Connection check failed: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_cluster_list(client: PlatformClient, args):
    """List compute clusters in the active workspace."""
    try:
        clusters = client.list_clusters()
        if not clusters:
            print("No compute clusters found.")
            return

        headers = ["Cluster ID", "Name", "State", "Cores / Workers", "Auto-Terminate"]
        rows = [
            [
                c.get("cluster_id"),
                c.get("cluster_name"),
                c.get("state"),
                f"{c.get('num_workers', 0)} workers",
                f"{c.get('autotermination_minutes', 0)} min"
            ]
            for c in clusters
        ]
        print_table(headers, rows)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_cluster_create(client: PlatformClient, args):
    """Provision a minimal on-demand single-node cluster (Zero-Cost on dev_free only)."""
    try:
        cluster_id = client.create_single_node_cluster(
            cluster_name=args.name,
            spark_version=args.spark_version,
            node_type=args.node_type,
            autoterminate_minutes=args.autoterminate
        )
        print(f"✅ Cluster successfully created! ID: {cluster_id}")
    except SafetyGuardrailError as sge:
        print(f"🛡️  {sge}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"❌ Failed to create cluster: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_cluster_status(client: PlatformClient, args):
    """Retrieve details and state of a specific cluster."""
    try:
        info = client.get_cluster(args.cluster_id)
        headers = ["Property", "Value"]
        rows = [
            ["Cluster ID", info.get("cluster_id")],
            ["Cluster Name", info.get("cluster_name")],
            ["State", info.get("state")],
            ["State Message", info.get("state_message", "N/A")],
            ["Runtime", info.get("spark_version")],
            ["Node Type", info.get("node_type_id")],
            ["Auto-Terminate", f"{info.get('autotermination_minutes')} min"]
        ]
        print_table(headers, rows)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_cluster_terminate(client: PlatformClient, args):
    """Terminate / stop a running cluster."""
    try:
        client.terminate_cluster(args.cluster_id)
        print(f"✅ Cluster {args.cluster_id} termination requested.")
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_job_list(client: PlatformClient, args):
    """List workflow jobs in the workspace."""
    try:
        jobs = client.list_jobs(name_filter=args.filter)
        if not jobs:
            print("No matching jobs found.")
            return

        headers = ["Job ID", "Name", "Creator", "Created Time"]
        rows = [
            [
                j.get("job_id"),
                j.get("settings", {}).get("name"),
                j.get("creator_user_name"),
                j.get("created_time")
            ]
            for j in jobs
        ]
        print_table(headers, rows)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_job_trigger(client: PlatformClient, args):
    """Trigger a workflow job run (Run-Now API)."""
    try:
        run_id = client.trigger_job(job_id=args.job_id, job_name=args.job_name)
        print(f"🚀 Job triggered successfully! Run ID: {run_id}")
        if args.monitor:
            client.monitor_run(run_id)
    except SafetyGuardrailError as sge:
        print(f"🛡️  {sge}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"❌ Failed to trigger job: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_job_status(client: PlatformClient, args):
    """Query execution status of a specific job run."""
    try:
        run_data = client.get_run(args.run_id)
        state = run_data.get("state", {})
        headers = ["Metric", "Value"]
        rows = [
            ["Run ID", run_data.get("run_id")],
            ["Job ID", run_data.get("job_id")],
            ["Run Name", run_data.get("run_name")],
            ["Life Cycle State", state.get("life_cycle_state")],
            ["Result State", state.get("result_state", "IN_PROGRESS")],
            ["Run Page URL", run_data.get("run_page_url", "N/A")]
        ]
        print_table(headers, rows)

        tasks = run_data.get("tasks", [])
        if tasks:
            print("\n--- Task Breakdown ---")
            task_headers = ["Task Key", "State", "Duration"]
            task_rows = [
                [
                    t.get("task_key"),
                    t.get("state", {}).get("life_cycle_state"),
                    f"{t.get('execution_duration', 0) / 1000:.1f}s"
                ]
                for t in tasks
            ]
            print_table(task_headers, task_rows)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_pipeline_list(client: PlatformClient, args):
    """List Lakeflow pipelines in the workspace."""
    try:
        pipelines = client.list_pipelines(name_filter=args.filter)
        if not pipelines:
            print("No matching pipelines found.")
            return

        headers = ["Pipeline ID", "Name", "State", "Edition"]
        rows = [
            [
                p.get("pipeline_id"),
                p.get("name"),
                p.get("state"),
                p.get("edition")
            ]
            for p in pipelines
        ]
        print_table(headers, rows)
    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_pipeline_trigger(client: PlatformClient, args):
    """Start an update run on a Declarative Lakeflow Pipeline."""
    try:
        update_id = client.trigger_pipeline(pipeline_id=args.pipeline_id, full_refresh=args.full_refresh)
        print(f"🚀 Pipeline update started! Update ID: {update_id}")
    except SafetyGuardrailError as sge:
        print(f"🛡️  {sge}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"❌ Failed to trigger pipeline: {e}", file=sys.stderr)
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lakehouse-ops",
        description="Databricks Platform Operations CLI (Lab 9 CI/CD & API Automation)"
    )
    parser.add_argument(
        "--target", "-t",
        choices=["dev_free", "prod_azure"],
        default="dev_free",
        help="Target Databricks environment (default: dev_free)"
    )
    parser.add_argument("--host", default=os.getenv("DATABRICKS_HOST"), help="Databricks workspace host URL")
    parser.add_argument("--token", default=os.getenv("DATABRICKS_TOKEN"), help="Databricks access token")
    parser.add_argument(
        "--force-paid-run",
        action="store_true",
        help="Emergency override to bypass zero-cost guardrails on prod_azure"
    )

    subparsers = parser.add_subparsers(dest="command", required=True, help="Sub-commands")

    # check-env
    subparsers.add_parser("check-env", help="Verify API authentication and workspace connectivity")

    # cluster
    cluster_parser = subparsers.add_parser("cluster", help="Manage cluster lifecycle")
    cluster_sub = cluster_parser.add_subparsers(dest="subcommand", required=True)

    cluster_sub.add_parser("list", help="List all clusters")

    create_p = cluster_sub.add_parser("create", help="Create a single-node cluster")
    create_p.add_argument("--name", required=True, help="Cluster name")
    create_p.add_argument("--spark-version", default="15.4.x-scala2.12", help="Spark runtime version")
    create_p.add_argument("--node-type", default="Standard_D4ds_v5", help="Node VM type")
    create_p.add_argument("--autoterminate", type=int, default=10, help="Auto-termination minutes")

    status_p = cluster_sub.add_parser("status", help="Get cluster status")
    status_p.add_argument("--cluster-id", required=True, help="Cluster ID")

    term_p = cluster_sub.add_parser("terminate", help="Terminate a cluster")
    term_p.add_argument("--cluster-id", required=True, help="Cluster ID")

    # job
    job_parser = subparsers.add_parser("job", help="Manage and trigger workflow jobs")
    job_sub = job_parser.add_subparsers(dest="subcommand", required=True)

    job_list_p = job_sub.add_parser("list", help="List jobs")
    job_list_p.add_argument("--filter", default=None, help="Filter by name")

    job_trig_p = job_sub.add_parser("trigger", help="Trigger a job run")
    job_trig_p.add_argument("--job-id", type=int, default=None, help="Job ID")
    job_trig_p.add_argument("--job-name", default=None, help="Job Name")
    job_trig_p.add_argument("--monitor", action="store_true", help="Wait and monitor run")

    job_stat_p = job_sub.add_parser("status", help="Query job run status")
    job_stat_p.add_argument("--run-id", type=int, required=True, help="Job Run ID")

    # pipeline
    pipe_parser = subparsers.add_parser("pipeline", help="Manage Lakeflow pipelines")
    pipe_sub = pipe_parser.add_subparsers(dest="subcommand", required=True)

    pipe_list_p = pipe_sub.add_parser("list", help="List pipelines")
    pipe_list_p.add_argument("--filter", default=None, help="Filter by name")

    pipe_trig_p = pipe_sub.add_parser("trigger", help="Trigger pipeline update")
    pipe_trig_p.add_argument("--pipeline-id", required=True, help="Pipeline ID")
    pipe_trig_p.add_argument("--full-refresh", action="store_true", help="Trigger full recomputation")

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    client = PlatformClient(
        target=args.target,
        host=args.host,
        token=args.token,
        force_paid_run=args.force_paid_run
    )

    if args.command == "check-env":
        cmd_check_env(client, args)
    elif args.command == "cluster":
        if args.subcommand == "list":
            cmd_cluster_list(client, args)
        elif args.subcommand == "create":
            cmd_cluster_create(client, args)
        elif args.subcommand == "status":
            cmd_cluster_status(client, args)
        elif args.subcommand == "terminate":
            cmd_cluster_terminate(client, args)
    elif args.command == "job":
        if args.subcommand == "list":
            cmd_job_list(client, args)
        elif args.subcommand == "trigger":
            cmd_job_trigger(client, args)
        elif args.subcommand == "status":
            cmd_job_status(client, args)
    elif args.command == "pipeline":
        if args.subcommand == "list":
            cmd_pipeline_list(client, args)
        elif args.subcommand == "trigger":
            cmd_pipeline_trigger(client, args)


if __name__ == "__main__":
    main()
