"""The `stepledger` command line."""

from __future__ import annotations

import typer

from stepledger import __version__

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
