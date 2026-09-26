"""The `stepledger` command line."""

from __future__ import annotations

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
