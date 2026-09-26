"""The `stepledger` command line."""

from __future__ import annotations

from typing import Any

import typer

from stepledger import __version__
from stepledger.config import resolve_dsn

app = typer.Typer(
    name="stepledger",
    help="Stepledger: a fenced, committed Postgres ledger for LangGraph nodes on Temporal.",
    no_args_is_help=True,
    add_completion=False,
)


def _version(value: bool) -> None:
    if value:
        typer.echo(f"stepledger {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False, "--version", callback=_version, is_eager=True, help="Print the version and exit."
    ),
) -> None:
    """Stepledger command line."""


DsnOption = typer.Option(None, "--dsn", envvar="STEPLEDGER_DSN", help="Postgres DSN.")


@app.command("init-db")
def init_db_cmd(dsn: str | None = DsnOption) -> None:
    """Create the Stepledger tables and views. Safe to run repeatedly."""
    from stepledger.ledger.store import init_db

    init_db(resolve_dsn(dsn))
    typer.echo("stepledger schema applied")


AddressOption = typer.Option("localhost:7233", "--address", envvar="TEMPORAL_ADDRESS")
NamespaceOption = typer.Option("default", "--namespace", "-n", envvar="TEMPORAL_NAMESPACE")


def _run(coro: Any) -> Any:
    import asyncio

    return asyncio.run(coro)


@app.command("reconcile")
def reconcile_cmd(
    workflow_id: str | None = typer.Argument(None, help="Reconcile every run of this workflow."),
    all_open: bool = typer.Option(False, "--all-open", help="Every run the ledger has not sealed."),
    dsn: str | None = DsnOption,
    address: str = AddressOption,
    namespace: str = NamespaceOption,
) -> None:
    """Repair the ledger from Temporal's history (terminations, timeouts, degraded runs)."""
    if not workflow_id and not all_open:
        raise typer.BadParameter("give a workflow id or --all-open")

    async def go() -> None:
        from temporalio.client import Client

        from stepledger.ledger.reconcile import reconcile
        from stepledger.ledger.store import LedgerStore

        client = await Client.connect(address, namespace=namespace)
        store = LedgerStore(resolve_dsn(dsn))
        try:
            reports = await reconcile(client, store, workflow_id, all_open=all_open)
        finally:
            await store.close()
        if not reports:
            typer.echo("nothing to reconcile")
        for r in reports:
            typer.echo(
                f"{r.workflow_id} {r.run_id} {r.run_status}: {r.action}"
                f" committed={r.committed} abandoned={r.abandoned} inserted={r.inserted}"
                f" divergence_repaired={r.divergence_repaired}"
            )

    _run(go())


@app.command("resolve")
def resolve_cmd(
    key: str = typer.Argument(..., help="The effect key from the UnknownEffectOutcome error."),
    outcome: str = typer.Option(..., "--outcome", help="done | not-done"),
    result: str | None = typer.Option(None, "--result", help="JSON result when done."),
    dsn: str | None = DsnOption,
) -> None:
    """Record a human's verdict on an effect whose outcome is unknown."""
    import json

    async def go() -> None:
        from stepledger.effects.once import resolve
        from stepledger.ledger.store import LedgerStore

        store = LedgerStore(resolve_dsn(dsn))
        try:
            state = await resolve(store, key, outcome, json.loads(result) if result else None)
        finally:
            await store.close()
        typer.echo(f"{key}: {state}")

    _run(go())


@app.command("ledger")
def ledger_cmd(
    workflow_id: str = typer.Argument(...),
    run_id: str | None = typer.Option(None, "--run-id"),
    dsn: str | None = DsnOption,
) -> None:
    """Print the run ledger: one line per node execution, with attempts, effects and waste."""
    from stepledger.read.ledger import run_ledger

    typer.echo(run_ledger(resolve_dsn(dsn), workflow_id, run_id))


@app.command("status")
def status_cmd(
    limit: int = typer.Option(20, "--limit"),
    dsn: str | None = DsnOption,
) -> None:
    """Recent runs in the ledger."""
    import psycopg

    with psycopg.connect(resolve_dsn(dsn)) as conn:
        rows = conn.execute(
            "SELECT workflow_id, left(run_id, 12), status, sealed_at IS NOT NULL, committed_count,"
            " abandoned_count, degraded FROM sl_runs ORDER BY first_seen_at DESC LIMIT %s",
            (limit,),
        ).fetchall()
    typer.echo(f"{'workflow_id':<36} {'run':<13} {'status':<17} sealed  committed abandoned")
    for wf, run, status, sealed, c, a, degraded in rows:
        flag = "  DEGRADED" if degraded else ""
        typer.echo(
            f"{wf:<36} {run:<13} {status:<17} {'yes' if sealed else 'no ':<6} {c or 0:>9}"
            f" {a or 0:>9}{flag}"
        )
