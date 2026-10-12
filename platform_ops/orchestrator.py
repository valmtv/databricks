#!/usr/bin/env python3
"""
Lab 9 Orchestrator CLI: Databricks REST API / SDK Platform Automation.
Implements the Databricks Academy Lab 9 reference interface:
- ensure-compute: verifies/starts shared GP1 cluster (0702-132442-toro5spu)
- run: runs a named job, polls task progress live, outputs report
- start-pipeline: triggers Lakeflow pipeline update and polls state

Supports native Typer + Rich + Databricks SDK when installed,
with an automatic fallback to portable standard library PlatformClient.
"""

import sys
import os
import datetime
import time

# Ensure local module directory is in sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from client import PlatformClient

# Check for Typer, Rich, and Databricks SDK
try:
    import typer
    from rich.console import Console
    from rich.table import Table
    from databricks.sdk import WorkspaceClient
    HAS_NATIVE_TYPER = True
except ImportError:
    HAS_NATIVE_TYPER = False


# =============================================================================
# Option A: Native Databricks Academy Reference (Typer + Rich + SDK)
# =============================================================================
if HAS_NATIVE_TYPER:
    app = typer.Typer(help="Databricks Academy Lab 9 Platform Orchestrator")
    console = Console()

    def get_sdk_client(target: str = "prod_azure") -> WorkspaceClient:
        client = PlatformClient(target=target)
        if client.sdk:
            return client.sdk
        return WorkspaceClient(host=client.host, token=client.token)

    @app.command()
    def ensure_compute(
        cluster_id: str = "0702-132442-toro5spu",
        target: str = typer.Option("prod_azure", help="Target environment (prod_azure or dev_free)")
    ):
        """Ensures that the specified Databricks cluster is running. If terminated, starts it and waits until RUNNING."""
        w = get_sdk_client(target)
        poll_interval = 20

        cluster = w.clusters.get(cluster_id)
        console.print(f"[bold blue]Initial cluster state:[/bold blue] {cluster.state}")

        if cluster.state.value == "TERMINATED":
            console.print(f"[yellow]Starting cluster {cluster_id}...[/yellow]")
            w.clusters.start(cluster_id)

        while True:
            cluster = w.clusters.get(cluster_id)
            state = cluster.state.value
            console.print(f"Cluster {cluster_id} state: [cyan]{state}[/cyan]")

            if state == "RUNNING":
                console.print("[bold green]Cluster is ready.[/bold green]")
                return cluster

            if state in {"ERROR", "UNKNOWN"}:
                error_message = getattr(cluster, "state_message", None)
                raise RuntimeError(
                    f"Cluster failed to start. State: {state}"
                    + (f"; message: {error_message}" if error_message else "")
                )

            time.sleep(poll_interval)

    def show_progress(run):
        """Displays progress of a running job by printing task lifecycle states."""
        for task in (run.tasks or []):
            state_val = task.state.life_cycle_state.value if task.state else "UNKNOWN"
            print(f"  Task '{task.task_key}' -> {state_val}")

    def format_timestamp(timestamp):
        """Formats epoch millisecond timestamp to readable UTC string."""
        if timestamp is None:
            return "n/a"
        return datetime.datetime.fromtimestamp(
            timestamp / 1000,
            tz=datetime.timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S UTC")

    def report(run, job_name):
        """Prints a Rich report table with execution details."""
        table = Table(title="Databricks Job Run Report")
        table.add_column("Field", style="cyan")
        table.add_column("Value", style="magenta")
        table.add_row("Job Name", job_name)
        table.add_row("Run ID", str(run.run_id))
        table.add_row("State", run.state.life_cycle_state.value if run.state else "UNKNOWN")
        table.add_row("Result", run.state.result_state.value if run.state and run.state.result_state else "IN_PROGRESS")
        table.add_row("Start Time", format_timestamp(run.start_time))
        table.add_row("End Time", format_timestamp(run.end_time))
        console.print(table)
        return run.state.result_state if run.state else None

    @app.command()
    def run(
        job: str = typer.Option(None, "--job", help="Job name to run; skips prompt (use in CI)"),
        target: str = typer.Option("prod_azure", help="Target environment"),
        cluster_id: str = "0702-132442-toro5spu"
    ):
        """Main flow: ensure compute is running, execute the job, and report results."""
        if target == "prod_azure":
            ensure_compute(cluster_id=cluster_id, target=target)

        w = get_sdk_client(target)
        jobs = list(w.jobs.list())
        names = [j.settings.name for j in jobs if j.settings and j.settings.name]

        job_name = job
        if not job_name:
            if not sys.stdin.isatty():
                console.print("[red]No --job provided and no interactive terminal (CI). Aborting.[/red]")
                raise typer.Exit(code=1)
            print("Available jobs:")
            for n in sorted(names):
                print("  -", n)
            job_name = input("Enter the job name to run: ").strip()

        matched_job = next((j for j in jobs if j.settings and j.settings.name == job_name), None)
        if not matched_job:
            console.print(f"[red]No job found named '{job_name}'. Available: {names}[/red]")
            raise typer.Exit(code=1)

        console.print(f"[bold green]Running job:[/bold green] {matched_job.settings.name} (ID: {matched_job.job_id})")
        run_obj = w.jobs.run_now(job_id=matched_job.job_id).result(
            timeout=datetime.timedelta(minutes=30),
            callback=show_progress
        )
        res = report(run_obj, job_name)
        ok = res is not None and res.value == "SUCCESS"
        raise typer.Exit(code=0 if ok else 1)

    @app.command()
    def start_pipeline(
        pipeline_name: str = typer.Option("[prod_azure] Air Quality Lakeflow Declarative Pipeline", "--pipeline-name"),
        target: str = typer.Option("prod_azure", help="Target environment")
    ):
        """Triggers a Lakeflow declarative pipeline by name and polls until terminal state."""
        w = get_sdk_client(target)
        pipelines = list(w.pipelines.list_pipelines())
        pipe = next((p for p in pipelines if p.name == pipeline_name), None)
        if pipe is None:
            names = [p.name for p in pipelines if p.name]
            console.print(f"[red]No pipeline named '{pipeline_name}'. Available: {names}[/red]")
            raise typer.Exit(code=1)

        update = w.pipelines.start_update(pipeline_id=pipe.pipeline_id)
        console.print(f"[bold green]Started update {update.update_id} on {pipe.name}[/bold green]")

        terminal = {"COMPLETED", "FAILED", "CANCELED"}
        while True:
            info = w.pipelines.get_update(
                pipeline_id=pipe.pipeline_id,
                update_id=update.update_id,
            ).update
            state = info.state.value
            console.print(f"Pipeline update state: [yellow]{state}[/yellow]")
            if state in terminal:
                break
            time.sleep(15)

        console.print(f"[bold]Pipeline finished:[/bold] {state}")
        raise typer.Exit(code=0 if state == "COMPLETED" else 1)


# =============================================================================
# Option B: Portable Standard Library CLI (No external dependencies required)
# =============================================================================
else:
    import argparse
    from lakehouse_ops import (
        cmd_ensure_compute,
        cmd_run,
        cmd_start_pipeline_workflow,
        cmd_check_env
    )

    def main():
        parser = argparse.ArgumentParser(
            prog="orchestrator",
            description="Databricks Academy Lab 9 Platform Orchestrator (Portable CLI)"
        )
        parser.add_argument(
            "--target", "-t",
            choices=["dev_free", "prod_azure"],
            default="prod_azure",
            help="Target Databricks environment (default: prod_azure)"
        )
        parser.add_argument("--host", default=os.getenv("DATABRICKS_HOST"), help="Workspace host URL")
        parser.add_argument("--token", default=os.getenv("DATABRICKS_TOKEN"), help="Databricks access token")
        parser.add_argument("--force-paid-run", action="store_true", help="Bypass guardrails")

        subparsers = parser.add_subparsers(dest="command", required=True, help="Sub-commands")

        # ensure-compute
        ec_p = subparsers.add_parser("ensure-compute", help="Ensure GP1 cluster is running")
        ec_p.add_argument("--cluster-id", default="0702-132442-toro5spu", help="Cluster ID")

        # run
        run_p = subparsers.add_parser("run", help="Ensure compute, run job, report")
        run_p.add_argument("--job", default=None, help="Job name")
        run_p.add_argument("--cluster-id", default="0702-132442-toro5spu", help="Cluster ID")
        run_p.add_argument("--timeout", type=int, default=1800, help="Timeout in seconds")

        # start-pipeline
        sp_p = subparsers.add_parser("start-pipeline", help="Start and monitor pipeline")
        sp_p.add_argument(
            "--pipeline-name",
            default="[prod_azure] Air Quality Lakeflow Declarative Pipeline",
            help="Pipeline name"
        )
        sp_p.add_argument("--pipeline-id", default=None, help="Pipeline ID")
        sp_p.add_argument("--full-refresh", action="store_true", help="Full refresh")
        sp_p.add_argument("--timeout", type=int, default=1800, help="Timeout in seconds")

        args = parser.parse_args()
        client = PlatformClient(
            target=args.target,
            host=args.host,
            token=args.token,
            force_paid_run=args.force_paid_run
        )

        if args.command == "ensure-compute":
            cmd_ensure_compute(client, args)
        elif args.command == "run":
            cmd_run(client, args)
        elif args.command == "start-pipeline":
            cmd_start_pipeline_workflow(client, args)


if __name__ == "__main__":
    if HAS_NATIVE_TYPER:
        app()
    else:
        main()
