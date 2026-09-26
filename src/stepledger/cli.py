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
ConverterOption = typer.Option(
    None,
    "--data-converter",
    help="module:attr of the DataConverter your workers use (codecs, pydantic); Stepledger's"
    " storage driver is added to it when storage is enabled in stepledger.yaml.",
)


def build_data_converter(dsn: str, ref: str | None = None) -> Any:
    """The data converter a client needs to read this deployment's payloads: the user's own
    (`module:attr`) or the default, plus the dedup storage driver when storage is enabled.
    Every client that reads histories or results (starters, tooling, `reconcile`) needs it;
    a bare client raises TMPRL1105 on externally stored payloads."""
    import dataclasses
    import importlib
    import os
    import sys

    from temporalio.converter import DataConverter

    from stepledger.config import load_settings
    from stepledger.storage import make_external_storage

    base = DataConverter.default
    if ref:
        if os.getcwd() not in sys.path:
            sys.path.insert(0, os.getcwd())
        module, _, attr = ref.partition(":")
        base = getattr(importlib.import_module(module), attr)
    cfg = load_settings()
    if cfg.storage.enabled and base.external_storage is None:
        _, ext = make_external_storage(
            dsn,
            dedupe=cfg.storage.dedupe,
            payload_size_threshold=cfg.storage.payload_size_threshold,
            chunk_sizes=(cfg.storage.chunk.min, cfg.storage.chunk.avg, cfg.storage.chunk.max),
        )
        base = dataclasses.replace(base, external_storage=ext)
    return base


def _run(coro: Any) -> Any:
    import asyncio

    return asyncio.run(coro)


@app.command("reconcile")
def reconcile_cmd(
    workflow_id: str | None = typer.Argument(None, help="Reconcile every run of this workflow."),
    all_open: bool = typer.Option(False, "--all-open", help="Every run the ledger has not sealed."),
    include_open: bool = typer.Option(
        False, "--include-open", help="Also repair runs that are still running (racy)."
    ),
    cancellation_type: str = typer.Option(
        "TRY_CANCEL",
        "--cancellation-type",
        help="ActivityCancellationType your tracked nodes use; history does not record it.",
    ),
    dsn: str | None = DsnOption,
    address: str = AddressOption,
    namespace: str = NamespaceOption,
    data_converter: str | None = ConverterOption,
) -> None:
    """Repair the ledger from Temporal's history (terminations, timeouts, degraded runs)."""
    if not workflow_id and not all_open:
        raise typer.BadParameter("give a workflow id or --all-open")

    async def go() -> None:
        from temporalio.client import Client

        from stepledger.config import load_settings
        from stepledger.ledger.reconcile import reconcile
        from stepledger.ledger.store import LedgerStore

        resolved = resolve_dsn(dsn)
        client = await Client.connect(
            address,
            namespace=namespace,
            data_converter=build_data_converter(resolved, data_converter),
        )
        store = LedgerStore(resolved)
        try:
            reports = await reconcile(
                client,
                store,
                workflow_id,
                all_open=all_open,
                include_open=include_open,
                cancellation_type=cancellation_type,
                store_outputs=load_settings().ledger.store_outputs,
            )
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


@app.command("gc")
def gc_cmd(
    execute: bool = typer.Option(False, "--execute", help="Delete for real (default: dry run)."),
    retention_days: float | None = typer.Option(None, "--retention-days"),
    margin_days: float | None = typer.Option(None, "--margin-days"),
    grace_hours: float | None = typer.Option(None, "--grace-hours"),
    orphan_ref_days: float | None = typer.Option(None, "--orphan-ref-days"),
    dsn: str | None = DsnOption,
    address: str = AddressOption,
    namespace: str = NamespaceOption,
) -> None:
    """Mark and sweep the dedup store. Dry run unless --execute."""
    from datetime import timedelta

    from stepledger.config import load_settings

    cfg = load_settings().gc
    retention = timedelta(days=retention_days if retention_days is not None else cfg.retention_days)
    margin = timedelta(days=margin_days if margin_days is not None else cfg.margin_days)
    grace = timedelta(hours=grace_hours if grace_hours is not None else cfg.grace_hours)
    orphan = timedelta(days=orphan_ref_days if orphan_ref_days is not None else cfg.orphan_ref_days)

    async def go() -> None:
        from temporalio.client import Client

        from stepledger.storage.gc import check_retention, namespace_retention, summary, sweep

        client = await Client.connect(address, namespace=namespace)
        problem = check_retention(retention, await namespace_retention(client))
        if problem:
            typer.echo(f"refusing to run: {problem}", err=True)
            raise typer.Exit(2)
        report = await sweep(
            client,
            resolve_dsn(dsn),
            retention=retention,
            margin=margin,
            grace=grace,
            orphan_ref_age=orphan,
            dry_run=not execute,
        )
        for k, v in summary(report).items():
            if k != "details":
                typer.echo(f"{k}: {v}")

    _run(go())


@app.command("materialize")
def materialize_cmd(
    workflow_id: str = typer.Argument(...),
    graph: str = typer.Option(
        ..., "--graph", help="module:attr of a StateGraph, a compiled graph, or a factory for one"
    ),
    run_id: str | None = typer.Option(None, "--run-id"),
    chain: bool = typer.Option(False, "--chain", help="Fill task-cache gaps from earlier runs."),
    include_provisional: bool = typer.Option(False, "--include-provisional"),
    show_state: bool = typer.Option(False, "--state", help="Print the rebuilt state as JSON."),
    dsn: str | None = DsnOption,
) -> None:
    """Rebuild a run's graph state from the ledger and say whether it is EXACT."""
    import importlib
    import json
    import os
    import sys

    from stepledger.read.materialize import materialize

    if os.getcwd() not in sys.path:  # like uvicorn: --graph resolves from the working directory
        sys.path.insert(0, os.getcwd())
    module, _, attr = graph.partition(":")
    obj: Any = getattr(importlib.import_module(module), attr)
    if callable(obj) and not hasattr(obj, "compile") and not hasattr(obj, "channels"):
        obj = obj()
    compiled = obj.compile() if hasattr(obj, "compile") else obj
    result = _run(
        materialize(
            resolve_dsn(dsn),
            compiled,
            workflow_id,
            run_id,
            include_provisional=include_provisional,
            chain=chain,
        )
    )
    typer.echo(f"{result.completeness}: {result.reason}")
    typer.echo(f"rows used: {result.rows_used}  chain rows used: {result.chain_rows_used}")
    for p in result.positions:
        typer.echo(f"  gap: {p}")
    if show_state:
        typer.echo(json.dumps(result.state, indent=1, default=str))


@app.command("bench")
def bench_cmd(
    demo: str = typer.Argument(..., help="cliff | chaos | growth | materialize | cost"),
) -> None:
    """Run a demo from a repository checkout (the bench is not part of the wheel)."""
    import subprocess
    import sys
    from pathlib import Path

    if not Path("bench/demo.py").is_file():
        typer.echo("run this from a stepledger repository checkout (bench/ is not installed)")
        raise typer.Exit(2)
    raise typer.Exit(subprocess.call([sys.executable, "bench/demo.py", "--demo", demo]))


if __name__ == "__main__":
    app()
