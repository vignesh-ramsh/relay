"""relay.cli — `arc relay ...` commands.

Mounted via the `arc.plugins.cli` entry point, same mechanism as every
other plugin's CLI. `prune-job-log` does a real `arc.boot()` + opens
psqldb (same reason authn's admin CLI needs a real boot, §3.13) — it
exists specifically for the no-`lineup`-installed case, where relay's own
`@arc.relay.task(cron=...)` maintenance job (relay/_maintenance.py) has
no scheduler to fire it automatically; run this by hand or from an
external cron in that case. With `lineup` installed, the same function
already runs on its own schedule — this is a manual escape hatch, not the
primary path.
"""

from __future__ import annotations

import asyncio
import warnings

import arc
import typer
from rich.console import Console

from relay._maintenance import DEFAULT_RETENTION_DAYS, prune_job_log

app = typer.Typer(help="Commands for the relay provider.")
console = Console()
err_console = Console(stderr=True, style="bold red")


@app.command(name="prune-job-log")
def prune_job_log_cmd(
    older_than_days: int = typer.Option(
        DEFAULT_RETENTION_DAYS, "--older-than-days", help="Delete _job_log rows finished before this many days ago."
    ),
    yes: bool = typer.Option(False, "--yes", help="Skip the confirmation prompt."),
) -> None:
    """Permanently deletes old _job_log rows (a hard delete — it's a
    "system": true table, §3.9, no soft-delete/trash for it at all)."""
    if not yes:
        typer.confirm(
            f"Permanently delete every _job_log row finished more than {older_than_days} day(s) ago?",
            abort=True,
        )

    async def _run() -> int:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", arc.ArcAdvisory)
            arc.boot()
        await arc.psqldb.open()
        try:
            return await prune_job_log(older_than_days)
        finally:
            await arc.psqldb.close()

    deleted = asyncio.run(_run())
    console.print(f"[bold green]Deleted {deleted} row(s)[/bold green] from _job_log.")
