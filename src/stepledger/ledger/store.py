"""LedgerStore: every Postgres statement the ledger runs."""

from __future__ import annotations

from importlib import resources

import psycopg

# Serializes concurrent `init-db` runs against one database.
_INIT_LOCK_KEY = 0x5E7E_1ED6


def _sql(package: str, name: str) -> str:
    return resources.files(package).joinpath(name).read_text(encoding="utf-8")


def schema_sql() -> str:
    return _sql("stepledger.ledger", "schema.sql")


def views_sql() -> str:
    return _sql("stepledger.read", "views.sql")


def init_db(dsn: str) -> None:
    """Apply schema.sql and views.sql. Idempotent: a second run changes nothing."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SELECT pg_advisory_lock(%s)", (_INIT_LOCK_KEY,))
        try:
            with conn.transaction():
                conn.execute(schema_sql().encode())
                conn.execute(views_sql().encode())
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (_INIT_LOCK_KEY,))
