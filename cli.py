#!/usr/bin/env python
# cli.py
"""
Tableau Multi-Agent System — CLI Control Layer
==============================================

Commands:
  run        Execute the full pipeline (or from a specific phase)
  validate   Run metadata validation only (no DB, no publish)
  status     Show current workflow state from SQLite store
  rollback   Rollback the last Tableau Cloud deployment (dry-run safe)

Usage examples:
  python cli.py run --env dev --dry-run
  python cli.py run --env prod --allow-overwrite
  python cli.py run --env dev --phase semantic
  python cli.py validate --csv-dir csv_inputs/
  python cli.py status
  python cli.py rollback
"""
from __future__ import annotations

import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn, Progress, SpinnerColumn,
    TaskProgressColumn, TextColumn, TimeElapsedColumn,
)
from rich.table import Table
from rich import box
from rich.text import Text

console = Console()


def _quiet_agent_logs():
    """Redirect structlog JSON output to logs/agents.log during CLI runs."""
    import logging
    import structlog

    Path("logs").mkdir(exist_ok=True)
    log_file = logging.FileHandler("logs/agents.log", encoding="utf-8")
    log_file.setLevel(logging.DEBUG)

    root = logging.getLogger()
    # Remove existing stream handlers that would print to stdout
    root.handlers = [h for h in root.handlers
                     if not isinstance(h, logging.StreamHandler)
                     or h.stream.name in ("<stderr>",)]
    root.addHandler(log_file)
    root.setLevel(logging.DEBUG)

    # Reconfigure structlog to use Python stdlib logging (goes to file)
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

# ── Phase ordering ─────────────────────────────────────────────────────────────
PHASES = [
    ("intake",       "Input Intake",          "01"),
    ("validation",   "Metadata Validation",   "02"),
    ("connectivity", "Connectivity Testing",  "03"),
    ("profiler",     "Source Schema Profiling","04"),
    ("conversion",   "Data Conversion",       "05"),
    ("semantic",     "Semantic Model (TDS)",  "06"),
    ("tableau_model","Worksheet Generation",  "08"),
    ("dashboard",    "Dashboard Layout",      "09"),
    ("assembly",     "TWB Assembly",          "11"),
    ("deployment",   "Publish to Tableau Cloud","13"),
]

PHASE_NAMES = [p[0] for p in PHASES]


# ── CLI group ──────────────────────────────────────────────────────────────────

@click.group()
@click.version_option("1.0.0", prog_name="Tableau Multi-Agent System")
def cli():
    """Tableau Multi-Agent System — automate MicroStrategy → Tableau Cloud migration."""
    pass


# ── run ───────────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--csv-dir",        default="csv_inputs/",  show_default=True,
              help="Directory containing the 15 CSV input files.")
@click.option("--env",            default="dev",
              type=click.Choice(["dev", "staging", "prod"]),
              show_default=True,  help="Target environment.")
@click.option("--dry-run",        is_flag=True,
              help="Validate and generate only — no DB connections, no publish.")
@click.option("--phase",          default=None,
              type=click.Choice(PHASE_NAMES),
              help="Start pipeline from this phase (skips earlier phases).")
@click.option("--allow-overwrite",is_flag=True,
              help="Overwrite existing workbook on Tableau Cloud.")
@click.option("--output-dir",     default="tableau/output/", show_default=True,
              help="Directory for generated .twb files.")
def run(csv_dir, env, dry_run, phase, allow_overwrite, output_dir):
    """Execute the full multi-agent pipeline."""

    _quiet_agent_logs()
    _print_header(env, dry_run)

    # ── Phase slice ───────────────────────────────────────────────────────
    start_idx = 0
    if phase:
        start_idx = next((i for i, p in enumerate(PHASES) if p[0] == phase), 0)
        if start_idx > 0:
            console.print(f"[yellow]⚡ Starting from phase [{phase}] — "
                          f"earlier outputs must already exist in state.[/]\n")

    phases_to_run = PHASES[start_idx:]

    # ── Build agent instances ─────────────────────────────────────────────
    from agents.base_agent import PhaseContext
    ctx = PhaseContext(
        project_id="cli_run",
        run_id=datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
        dry_run=dry_run,
    )

    agents = _build_agents(ctx, csv_dir=csv_dir,
                           output_dir=output_dir,
                           allow_overwrite=allow_overwrite)

    # ── Execute ───────────────────────────────────────────────────────────
    state: dict = {}
    run_start   = time.perf_counter()
    all_passed  = True

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=30),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        task = progress.add_task("Running pipeline…", total=len(phases_to_run))

        for phase_key, phase_label, phase_num in phases_to_run:
            agent = agents.get(phase_key)
            if agent is None:
                progress.console.print(
                    f"  [yellow]⚠  Phase {phase_num} [{phase_label}] — "
                    f"agent not registered, skipping.[/]"
                )
                progress.advance(task)
                continue

            progress.update(task,
                description=f"Phase {phase_num}: {phase_label}")

            result = _run_phase(agent, state, phase_key, phase_label,
                                phase_num, progress)

            if result is None:
                all_passed = False
                break

            state.update(result.output)

            if result.has_blocking_errors:
                _print_phase_failure(phase_num, phase_label, result)
                all_passed = False
                break
            else:
                _print_phase_success(phase_num, phase_label, result, progress)

            progress.advance(task)

    elapsed = time.perf_counter() - run_start
    _print_footer(all_passed, elapsed, dry_run, state)
    sys.exit(0 if all_passed else 1)


# ── validate ──────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--csv-dir", default="csv_inputs/", show_default=True,
              help="Directory containing the CSV input files.")
def validate(csv_dir):
    """Run intake + metadata validation only. No DB connections, no publish."""
    console.print(Panel(
        "[bold cyan]Metadata Validation[/]\n"
        f"CSV directory: [white]{csv_dir}[/]",
        title="Tableau Multi-Agent System", border_style="cyan"
    ))

    _quiet_agent_logs()
    from agents.base_agent import PhaseContext
    from agents.intake_agent import IntakeAgent
    from agents.validation_agent import ValidationAgent

    _quiet_agent_logs()  # re-apply after imports trigger structlog init
    ctx   = PhaseContext(project_id="validate", run_id="validate", dry_run=True)
    state = {}

    # Intake
    console.print("\n[bold]Phase 01:[/] Input Intake…", end=" ")
    intake_agent = IntakeAgent(config={"csv_dir": csv_dir})
    result = intake_agent.execute(state)
    state.update(result.output)

    if result.has_blocking_errors:
        console.print("[red]✗ FAILED[/]")
        _print_errors(result)
        sys.exit(1)
    console.print("[green]✓[/]")

    # Validation
    console.print("[bold]Phase 02:[/] Metadata Validation…", end=" ")
    val_agent = ValidationAgent()
    result = val_agent.execute(state)
    state.update(result.output)

    if result.has_blocking_errors:
        console.print("[red]✗ FAILED[/]")
        _print_errors(result)
        sys.exit(1)
    console.print("[green]✓[/]")

    # Summary
    report = state.get("validation_report", {})
    _print_validation_summary(report)
    sys.exit(0)


# ── status ────────────────────────────────────────────────────────────────────

@cli.command()
def status():
    """Show current workflow state from the SQLite state store."""
    db_path = Path("state/state.db")

    if not db_path.exists():
        console.print(
            "[yellow]No state database found.[/] "
            "Run [bold]python cli.py run[/] first."
        )
        sys.exit(0)

    try:
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        cur  = conn.cursor()

        # LangGraph checkpoints table
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = [r[0] for r in cur.fetchall()]

        console.print(Panel(
            f"[cyan]State database:[/] {db_path}\n"
            f"[cyan]Tables:[/] {', '.join(tables) or 'none'}",
            title="Workflow State", border_style="blue"
        ))

        if "checkpoints" in tables:
            cur.execute(
                "SELECT thread_id, checkpoint_id, created_at "
                "FROM checkpoints ORDER BY created_at DESC LIMIT 10"
            )
            rows = cur.fetchall()
            if rows:
                t = Table(title="Recent Checkpoints", box=box.SIMPLE)
                t.add_column("Thread ID", style="cyan")
                t.add_column("Checkpoint ID")
                t.add_column("Created At")
                for r in rows:
                    t.add_row(*[str(x) for x in r])
                console.print(t)
            else:
                console.print("[dim]No checkpoints recorded yet.[/]")

        conn.close()

    except Exception as e:
        console.print(f"[red]Error reading state DB:[/] {e}")
        sys.exit(1)


# ── rollback ──────────────────────────────────────────────────────────────────

@cli.command()
@click.option("--dry-run", is_flag=True,
              help="Show what would be rolled back without actually deleting.")
def rollback(dry_run):
    """Rollback the last Tableau Cloud deployment."""
    log_path = Path("deployment/rollback_log.json")

    if not log_path.exists():
        console.print(
            "[yellow]No rollback log found.[/] "
            "Nothing to roll back."
        )
        sys.exit(0)

    try:
        entries = json.loads(log_path.read_text())
    except Exception as e:
        console.print(f"[red]Cannot read rollback log:[/] {e}")
        sys.exit(1)

    if not entries:
        console.print("[yellow]Rollback log is empty.[/]")
        sys.exit(0)

    last = entries[-1]
    console.print(Panel(
        f"[cyan]Workbook ID:[/]   {last.get('workbook_id', 'unknown')}\n"
        f"[cyan]Workbook Name:[/] {last.get('workbook_name', 'unknown')}\n"
        f"[cyan]Published:[/]     {last.get('timestamp', 'unknown')}\n"
        f"[cyan]Server:[/]        {last.get('server_url', 'unknown')}",
        title="Last Deployment — Rollback Target", border_style="yellow"
    ))

    if dry_run:
        console.print("[yellow]Dry-run:[/] rollback skipped. "
                      "Remove --dry-run to execute.")
        sys.exit(0)

    workbook_id = last.get("workbook_id")
    if not workbook_id:
        console.print("[red]No workbook_id in rollback log — cannot proceed.[/]")
        sys.exit(1)

    try:
        import tableauserverclient as TSC
    except ImportError:
        console.print("[red]tableauserverclient not installed.[/]")
        sys.exit(1)

    # Resolve credentials
    pat_name   = os.environ.get(last.get("pat_name_env", ""), "")
    pat_secret = os.environ.get(last.get("pat_secret_env", ""), "")
    site_id    = last.get("site_id", "")
    server_url = last.get("server_url", "")

    if not pat_name or not pat_secret:
        console.print(
            "[red]PAT credentials not found in environment.[/] "
            "Set the env vars referenced in auth.csv."
        )
        sys.exit(1)

    console.print(f"\nDeleting workbook [bold]{workbook_id}[/] from Tableau Cloud…")
    try:
        auth   = TSC.PersonalAccessTokenAuth(pat_name, pat_secret, site_id=site_id)
        server = TSC.Server(server_url, use_server_version=True)
        with server.auth.sign_in(auth):
            server.workbooks.delete(workbook_id)
        console.print("[green]✓ Rollback complete.[/]")
    except Exception as e:
        console.print(f"[red]Rollback failed:[/] {e}")
        sys.exit(1)


# ── Agent factory ──────────────────────────────────────────────────────────────

def _build_agents(ctx, csv_dir: str, output_dir: str, allow_overwrite: bool) -> dict:
    """Instantiate all pipeline agents."""
    from agents.intake_agent        import IntakeAgent
    from agents.validation_agent    import ValidationAgent
    from agents.connectivity_agent  import ConnectivityAgent
    from agents.profiler_agent      import ProfilerAgent
    from agents.conversion_agent    import DataConversionAgent
    from agents.semantic_agent      import SemanticModelAgent
    from agents.tableau_model_agent import TableauModelAgent
    from agents.dashboard_agent     import DashboardGenAgent
    from agents.twb_assembly_agent  import TwbAssemblyAgent
    from agents.deployment_agent    import DeploymentAgent

    return {
        "intake":        IntakeAgent(config={"csv_dir": csv_dir}),
        "validation":    ValidationAgent(),
        "connectivity":  ConnectivityAgent(context=ctx),
        "profiler":      ProfilerAgent(context=ctx),
        "conversion":    DataConversionAgent(),
        "semantic":      SemanticModelAgent(),
        "tableau_model": TableauModelAgent(),
        "dashboard":     DashboardGenAgent(),
        "assembly":      TwbAssemblyAgent(
                             config={"output_dir": output_dir},
                             context=ctx),
        "deployment":    DeploymentAgent(
                             config={"allow_overwrite": allow_overwrite},
                             context=ctx),
    }


# ── Helpers ────────────────────────────────────────────────────────────────────

def _run_phase(agent, state, phase_key, phase_label, phase_num, progress):
    """Execute one agent phase, catching all exceptions."""
    try:
        result = agent.execute(state)
        return result
    except Exception as e:
        progress.console.print(
            f"\n[red]✗ Phase {phase_num} [{phase_label}] raised an "
            f"unhandled exception:[/]\n  {e}"
        )
        if os.environ.get("DEBUG"):
            progress.console.print(traceback.format_exc())
        return None


def _print_header(env: str, dry_run: bool):
    env_color = {"dev": "cyan", "staging": "yellow", "prod": "red"}.get(env, "white")
    mode_note = "  [yellow]DRY-RUN — no DB calls, no publish[/]" if dry_run else ""
    console.print(Panel(
        f"[bold]Environment:[/] [{env_color}]{env.upper()}[/]{mode_note}\n"
        f"[bold]Started:[/]     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        title="[bold cyan]Tableau Multi-Agent System[/]",
        border_style="cyan",
    ))
    console.print()


def _print_phase_success(num, label, result, progress):
    warn_note = f" [yellow]({len(result.warnings)} warning{'s' if len(result.warnings)!=1 else ''})[/]" \
                if result.warnings else ""
    progress.console.print(
        f"  [green]✓[/] Phase {num}: {label} "
        f"[dim]({result.duration_ms:.0f}ms)[/]{warn_note}"
    )


def _print_phase_failure(num, label, result):
    console.print(f"\n  [red]✗ Phase {num}: {label} FAILED[/]")
    for err in result.errors[:5]:
        console.print(f"    [red]•[/] {err.message}")


def _print_errors(result):
    for err in result.errors:
        console.print(f"  [red]•[/] {err.message}")


def _print_validation_summary(report: dict):
    findings = report.get("findings", [])
    totals   = report.get("totals", {})
    can_proceed = report.get("can_proceed", True)

    t = Table(title="Validation Summary", box=box.ROUNDED)
    t.add_column("Severity", style="bold")
    t.add_column("Count",    justify="right")

    critical = totals.get("critical", 0)
    high     = totals.get("high", 0)
    warnings = totals.get("warnings", 0)
    info     = totals.get("info", 0)

    if critical: t.add_row("[red]CRITICAL[/]", str(critical))
    if high:     t.add_row("[orange1]HIGH[/]",     str(high))
    if warnings: t.add_row("[yellow]WARNING[/]",   str(warnings))
    if info:     t.add_row("[dim]INFO[/]",          str(info))

    console.print()
    console.print(t)

    status_text = (
        "[green]✓ Validation passed — pipeline can proceed.[/]"
        if can_proceed else
        "[red]✗ Validation failed — fix CRITICAL errors before running.[/]"
    )
    console.print(f"\n{status_text}\n")

    # Show CRITICAL and HIGH details
    for f in findings:
        sev = f.get("severity", "").upper()
        if sev in ("CRITICAL", "HIGH"):
            color = "red" if sev == "CRITICAL" else "orange1"
            console.print(f"  [{color}]{sev}[/] {f.get('message', '')}")


def _print_footer(success: bool, elapsed: float, dry_run: bool, state: dict):
    console.print()
    if success:
        # Show publish result if available
        pr = state.get("publish_result", {})
        extra = ""
        if pr.get("status") == "success" and pr.get("workbook_id"):
            extra = (
                f"\n[bold]Workbook ID:[/]  {pr['workbook_id']}"
                f"\n[bold]URL:[/]          {pr.get('workbook_url', 'n/a')}"
            )
        elif pr.get("status") == "dry_run":
            twb_path = state.get("twb_path") or "n/a (dry-run)"
            extra = f"\n[bold]TWB File:[/]     {twb_path}"

        console.print(Panel(
            f"[green bold]✓ Pipeline complete[/]  "
            f"[dim]({elapsed:.1f}s)[/]{extra}",
            border_style="green",
        ))
    else:
        console.print(Panel(
            f"[red bold]✗ Pipeline failed[/]  [dim]({elapsed:.1f}s)[/]\n"
            "Check the errors above. Run with [bold]DEBUG=1[/] for stack traces.",
            border_style="red",
        ))


# ── Entrypoint ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    cli()
