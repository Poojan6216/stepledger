"""LedgerStore: every Postgres statement the ledger runs.

Each call site runs in one transaction (`async with store.tx() as tx`), so a node's fenced upsert,
the commits and abandons it carries, and its audit row land together or not at all.
"""

from __future__ import annotations

import os
import socket
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from importlib import resources
from typing import Any, Literal

import psycopg
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from stepledger.keys import Fence, LedgerKey, RunKey

# Serializes concurrent `init-db` runs against one database.
_INIT_LOCK_KEY = 0x5E7E_1ED6

RowStatus = Literal["PROVISIONAL", "COMMITTED", "ABANDONED"]
Kind = Literal["UPDATE", "COMMAND", "INTERRUPT"]
Outcome = Literal["WROTE", "FENCED_OUT", "DB_ERROR", "DIVERGENCE_REPAIRED"]
TERMINAL = ("COMPLETED", "FAILED", "CANCELLED", "TERMINATED", "TIMED_OUT", "CONTINUED_AS_NEW")

WORKER = f"{socket.gethostname()}:{os.getpid()}"


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


@dataclass(frozen=True)
class NodeWrite:
    """Everything a fenced upsert writes for one attempt of one node execution."""

    activity_id: str
    activity_type: str
    kind: Kind
    output_hash: str
    started_at: datetime
    finished_at: datetime
    output_json: Any = None  # plain JSON (json/plain encoding), or None
    output_bytes: bytes | None = None  # any other encoding
    output_encoding: str | None = None
    graph: str | None = None
    node: str | None = None
    lg_step: int | None = None
    lg_path: str | None = None
    lg_task_id: str | None = None
    checkpoint_ns: str | None = None
    input_snapshot: Any = None
    input_hash: str | None = None
    input_bytes: int | None = None
    tokens_in: int | None = None
    tokens_out: int | None = None
    cost_usd: Decimal | float | None = None


@dataclass(frozen=True)
class UpsertResult:
    wrote: bool
    row_status: RowStatus | None  # the row's status after the statement
    run_sealed: bool


@dataclass(frozen=True)
class SealResult:
    status: str
    node_count: int
    committed: int
    abandoned: int
    missing_seqs: list[int]
    already_sealed: bool


_UPSERT = """
INSERT INTO sl_nodes AS n (
  namespace, workflow_id, run_id, seq, activity_id, activity_type, graph, node,
  lg_step, lg_path, lg_task_id, checkpoint_ns, attempt, fence_scheduled_at, status, kind,
  output_json, output_bytes, output_encoding, output_hash, input_snapshot, input_hash, input_bytes,
  tokens_in, tokens_out, cost_usd, started_at, finished_at)
VALUES (
  %(namespace)s, %(workflow_id)s, %(run_id)s, %(seq)s, %(activity_id)s, %(activity_type)s,
  %(graph)s, %(node)s, %(lg_step)s, %(lg_path)s, %(lg_task_id)s, %(checkpoint_ns)s, %(attempt)s,
  %(fence_scheduled_at)s, 'PROVISIONAL', %(kind)s, %(output_json)s, %(output_bytes)s,
  %(output_encoding)s, %(output_hash)s, %(input_snapshot)s, %(input_hash)s, %(input_bytes)s,
  %(tokens_in)s, %(tokens_out)s, %(cost_usd)s, %(started_at)s, %(finished_at)s)
ON CONFLICT (namespace, workflow_id, run_id, seq) DO UPDATE SET
  activity_id = EXCLUDED.activity_id, attempt = EXCLUDED.attempt,
  fence_scheduled_at = EXCLUDED.fence_scheduled_at, kind = EXCLUDED.kind,
  output_json = EXCLUDED.output_json, output_bytes = EXCLUDED.output_bytes,
  output_encoding = EXCLUDED.output_encoding, output_hash = EXCLUDED.output_hash,
  input_snapshot = coalesce(EXCLUDED.input_snapshot, n.input_snapshot),
  input_hash = EXCLUDED.input_hash, input_bytes = EXCLUDED.input_bytes,
  tokens_in = EXCLUDED.tokens_in, tokens_out = EXCLUDED.tokens_out, cost_usd = EXCLUDED.cost_usd,
  started_at = EXCLUDED.started_at, finished_at = EXCLUDED.finished_at
WHERE n.status = 'PROVISIONAL'
  AND (n.fence_scheduled_at, n.attempt) <= (EXCLUDED.fence_scheduled_at, EXCLUDED.attempt)
RETURNING 1
"""


class LedgerTx:
    """One ledger transaction. Obtain it from `LedgerStore.tx()`."""

    def __init__(self, conn: psycopg.AsyncConnection[Any]) -> None:
        self.conn = conn

    async def ensure_run(self, run: RunKey, workflow_type: str | None = None) -> bool:
        """Create the run row if missing and lock it FOR SHARE; return whether it is sealed.

        The share lock serializes node writes against `seal_run`, which locks FOR UPDATE, so
        no write can slip into a run between its seal's reads and its commit."""
        await self.conn.execute(
            "INSERT INTO sl_runs (namespace, workflow_id, run_id, workflow_type)"
            " VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (run.namespace, run.workflow_id, run.run_id, workflow_type),
        )
        cur = await self.conn.execute(
            "SELECT sealed_at IS NOT NULL FROM sl_runs"
            " WHERE namespace = %s AND workflow_id = %s AND run_id = %s FOR SHARE",
            (run.namespace, run.workflow_id, run.run_id),
        )
        row = await cur.fetchone()
        return bool(row and row[0])

    async def fenced_upsert(self, key: LedgerKey, fence: Fence, w: NodeWrite) -> UpsertResult:
        sealed = await self.ensure_run(key.run)
        if sealed:
            # A sealed run's decisions are final; any write now comes from a stale attempt.
            return UpsertResult(False, await self.row_status(key), True)
        params = {
            "namespace": key.namespace,
            "workflow_id": key.workflow_id,
            "run_id": key.run_id,
            "seq": key.seq,
            "attempt": fence.attempt,
            "fence_scheduled_at": fence.scheduled_at,
            "activity_id": w.activity_id,
            "activity_type": w.activity_type,
            "graph": w.graph,
            "node": w.node,
            "lg_step": w.lg_step,
            "lg_path": w.lg_path,
            "lg_task_id": w.lg_task_id,
            "checkpoint_ns": w.checkpoint_ns,
            "kind": w.kind,
            "output_json": Jsonb(w.output_json) if w.output_json is not None else None,
            "output_bytes": w.output_bytes,
            "output_encoding": w.output_encoding,
            "output_hash": w.output_hash,
            "input_snapshot": Jsonb(w.input_snapshot) if w.input_snapshot is not None else None,
            "input_hash": w.input_hash,
            "input_bytes": w.input_bytes,
            "tokens_in": w.tokens_in,
            "tokens_out": w.tokens_out,
            "cost_usd": w.cost_usd,
            "started_at": w.started_at,
            "finished_at": w.finished_at,
        }
        cur = await self.conn.execute(_UPSERT, params)
        wrote = await cur.fetchone() is not None
        status = "PROVISIONAL" if wrote else await self.row_status(key)
        return UpsertResult(wrote, status, False)

    async def row_status(self, key: LedgerKey) -> RowStatus | None:
        cur = await self.conn.execute(
            "SELECT status FROM sl_nodes"
            " WHERE namespace = %s AND workflow_id = %s AND run_id = %s AND seq = %s",
            (key.namespace, key.workflow_id, key.run_id, key.seq),
        )
        row = await cur.fetchone()
        return row[0] if row else None

    async def commit(self, run: RunKey, seqs: Sequence[int]) -> int:
        """R4: PROVISIONAL -> COMMITTED for seqs the workflow saw succeed. Idempotent."""
        if not seqs:
            return 0
        cur = await self.conn.execute(
            "UPDATE sl_nodes SET status = 'COMMITTED', committed_at = now()"
            " WHERE namespace = %s AND workflow_id = %s AND run_id = %s AND seq = ANY(%s)"
            " AND status = 'PROVISIONAL'",
            (run.namespace, run.workflow_id, run.run_id, list(seqs)),
        )
        return cur.rowcount

    async def abandon(self, run: RunKey, seqs: Sequence[int]) -> int:
        """R5: PROVISIONAL -> ABANDONED for seqs the workflow saw fail or cancel. Idempotent."""
        if not seqs:
            return 0
        cur = await self.conn.execute(
            "UPDATE sl_nodes SET status = 'ABANDONED'"
            " WHERE namespace = %s AND workflow_id = %s AND run_id = %s AND seq = ANY(%s)"
            " AND status = 'PROVISIONAL'",
            (run.namespace, run.workflow_id, run.run_id, list(seqs)),
        )
        return cur.rowcount

    async def audit_attempt(
        self,
        key: LedgerKey,
        fence: Fence,
        outcome: Outcome,
        *,
        output_hash: str | None = None,
        tokens_in: int | None = None,
        tokens_out: int | None = None,
        cost_usd: Decimal | float | None = None,
        write_ms: float | None = None,
        note: str | None = None,
    ) -> None:
        await self.conn.execute(
            "INSERT INTO sl_node_attempts (namespace, workflow_id, run_id, seq, attempt,"
            " fence_scheduled_at, output_hash, outcome, tokens_in, tokens_out, cost_usd, write_ms,"
            " worker, note) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                key.namespace,
                key.workflow_id,
                key.run_id,
                key.seq,
                fence.attempt,
                fence.scheduled_at,
                output_hash,
                outcome,
                tokens_in,
                tokens_out,
                cost_usd,
                write_ms,
                WORKER,
                note,
            ),
        )

    async def seal_run(
        self,
        run: RunKey,
        *,
        status: str,
        node_count: int,
        commits: Sequence[int],
        abandons: Sequence[int],
        final_state_hash: str | None,
        workflow_type: str | None = None,
        sealed_by: Literal["seal", "reconcile"] = "seal",
    ) -> SealResult:
        """Close a run. Commits `commits`, then abandons every other PROVISIONAL row: at a real
        workflow exit, a result the workflow did not accept is by definition not accepted.
        Idempotent: sealing a sealed run changes nothing and reports the stored decision."""
        if status not in TERMINAL:
            raise ValueError(f"cannot seal with status {status!r}")
        await self.conn.execute(
            "INSERT INTO sl_runs (namespace, workflow_id, run_id, workflow_type)"
            " VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (run.namespace, run.workflow_id, run.run_id, workflow_type),
        )
        cur = await self.conn.execute(
            "SELECT sealed_at, status FROM sl_runs"
            " WHERE namespace = %s AND workflow_id = %s AND run_id = %s FOR UPDATE",
            (run.namespace, run.workflow_id, run.run_id),
        )
        row = await cur.fetchone()
        assert row is not None
        if row[0] is not None:
            counts = await self._counts(run)
            return SealResult(row[1], node_count, counts[0], counts[1], [], True)
        await self.commit(run, commits)
        await self.abandon(run, abandons)
        await self.conn.execute(
            "UPDATE sl_nodes SET status = 'ABANDONED'"
            " WHERE namespace = %s AND workflow_id = %s AND run_id = %s AND status = 'PROVISIONAL'",
            (run.namespace, run.workflow_id, run.run_id),
        )
        cur = await self.conn.execute(
            "SELECT s FROM generate_series(0, %s - 1) AS s WHERE NOT EXISTS ("
            " SELECT 1 FROM sl_nodes WHERE namespace = %s AND workflow_id = %s AND run_id = %s"
            " AND seq = s) ORDER BY s",
            (node_count, run.namespace, run.workflow_id, run.run_id),
        )
        missing = [r[0] for r in await cur.fetchall()]
        committed, abandoned = await self._counts(run)
        await self.conn.execute(
            "UPDATE sl_runs SET status = %s, node_count = %s, committed_count = %s,"
            " abandoned_count = %s, final_state_hash = %s, sealed_at = now(), sealed_by = %s,"
            " degraded = degraded OR %s, missing_seqs = %s,"
            " workflow_type = coalesce(workflow_type, %s)"
            " WHERE namespace = %s AND workflow_id = %s AND run_id = %s",
            (
                status,
                node_count,
                committed,
                abandoned,
                final_state_hash,
                sealed_by,
                bool(missing),
                missing or None,
                workflow_type,
                run.namespace,
                run.workflow_id,
                run.run_id,
            ),
        )
        return SealResult(status, node_count, committed, abandoned, missing, False)

    async def _counts(self, run: RunKey) -> tuple[int, int]:
        cur = await self.conn.execute(
            "SELECT count(*) FILTER (WHERE status = 'COMMITTED'),"
            " count(*) FILTER (WHERE status = 'ABANDONED') FROM sl_nodes"
            " WHERE namespace = %s AND workflow_id = %s AND run_id = %s",
            (run.namespace, run.workflow_id, run.run_id),
        )
        row = await cur.fetchone()
        assert row is not None
        return int(row[0]), int(row[1])

    async def mark_degraded(self, run: RunKey) -> None:
        await self.ensure_run(run)
        await self.conn.execute(
            "UPDATE sl_runs SET degraded = true"
            " WHERE namespace = %s AND workflow_id = %s AND run_id = %s",
            (run.namespace, run.workflow_id, run.run_id),
        )


class LedgerStore:
    """An async connection pool plus the ledger's transactions. One per worker process."""

    def __init__(self, dsn: str, *, min_size: int = 1, max_size: int = 10) -> None:
        self.dsn = dsn
        self._pool = AsyncConnectionPool(
            dsn, min_size=min_size, max_size=max_size, open=False, kwargs={"autocommit": False}
        )
        self._opened = False

    async def open(self) -> None:
        if not self._opened:
            await self._pool.open()
            self._opened = True

    async def close(self) -> None:
        if self._opened:
            await self._pool.close()
            self._opened = False

    @asynccontextmanager
    async def tx(self) -> AsyncIterator[LedgerTx]:
        await self.open()
        async with self._pool.connection() as conn, conn.transaction():
            yield LedgerTx(conn)

    @asynccontextmanager
    async def read(self) -> AsyncIterator[psycopg.AsyncConnection[Any]]:
        """A pooled connection for queries; use `conn.cursor(row_factory=dict_row)` for dicts."""
        await self.open()
        async with self._pool.connection() as conn:
            yield conn
