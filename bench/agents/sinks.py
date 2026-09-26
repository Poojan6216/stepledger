"""Postgres sinks used by the baselines and the fake effect targets.

B1 ("naive") inserts each node's delta; B1u ("naive_upsert") upserts on
(workflow_id, run_id, activity_id); B0 persists the final state once; effect targets count every
call so duplicate side effects can be measured. One pool per worker process.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from typing import Any

from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from temporalio import activity

from stepledger.config import resolve_dsn

SCHEMA = """
CREATE TABLE IF NOT EXISTS bench_naive_nodes (
  id bigserial PRIMARY KEY, workflow_id text, run_id text, activity_id text, node text,
  attempt int, output jsonb, output_hash text, at timestamptz DEFAULT now());
CREATE TABLE IF NOT EXISTS bench_naive_upsert (
  workflow_id text, run_id text, activity_id text, node text, attempt int, output jsonb,
  output_hash text, at timestamptz DEFAULT now(), PRIMARY KEY (workflow_id, run_id, activity_id));
CREATE TABLE IF NOT EXISTS bench_persist (
  id bigserial PRIMARY KEY, workflow_id text, run_id text, state jsonb,
  at timestamptz DEFAULT now());
CREATE TABLE IF NOT EXISTS bench_effects (
  id bigserial PRIMARY KEY, effect text, key text, request_hash text, workflow_id text,
  run_id text, attempt int, at timestamptz DEFAULT now());
"""

_pool: AsyncConnectionPool | None = None
_lock = asyncio.Lock()


async def pool() -> AsyncConnectionPool:
    global _pool
    async with _lock:
        if _pool is None:
            p = AsyncConnectionPool(resolve_dsn(os.environ.get("STEPLEDGER_DSN")), open=False)
            await p.open()
            async with p.connection() as conn:
                await conn.execute(SCHEMA.encode())
            _pool = p
    return _pool


def _where() -> tuple[str, str, str, int]:
    if not activity.in_activity():
        return "local", "local", "local", 1
    info = activity.info()
    return info.workflow_id or "", info.workflow_run_id or "", info.activity_id, info.attempt


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


async def naive_write(mode: str, node: str, delta: dict[str, Any]) -> None:
    wf, run, act, attempt = _where()
    p = await pool()
    async with p.connection() as conn:
        if mode == "naive":
            await conn.execute(
                "INSERT INTO bench_naive_nodes (workflow_id, run_id, activity_id, node, attempt,"
                " output, output_hash) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (wf, run, act, node, attempt, Jsonb(delta), _hash(delta)),
            )
        elif mode == "naive_upsert":
            await conn.execute(
                "INSERT INTO bench_naive_upsert (workflow_id, run_id, activity_id, node, attempt,"
                " output, output_hash) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                " ON CONFLICT (workflow_id, run_id, activity_id) DO UPDATE SET"
                " attempt = EXCLUDED.attempt, output = EXCLUDED.output,"
                " output_hash = EXCLUDED.output_hash, at = now()",
                (wf, run, act, node, attempt, Jsonb(delta), _hash(delta)),
            )
        else:
            raise ValueError(f"unknown persist_mode {mode!r}")


async def persist_all(state: dict[str, Any]) -> int:
    wf, run, _, _ = _where()
    p = await pool()
    async with p.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO bench_persist (workflow_id, run_id, state) VALUES (%s, %s, %s)"
            " RETURNING id",
            (wf, run, Jsonb(state)),
        )
        row = await cur.fetchone()
    return int(row[0]) if row else 0


async def effect(name: str, request: dict[str, Any], key: str | None) -> str:
    """The fake ticket / Slack target. Records every call; returns a deterministic receipt."""
    wf, run, _, attempt = _where()
    p = await pool()
    async with p.connection() as conn:
        await conn.execute(
            "INSERT INTO bench_effects (effect, key, request_hash, workflow_id, run_id, attempt)"
            " VALUES (%s, %s, %s, %s, %s, %s)",
            (name, key, _hash(request), wf, run, attempt),
        )
    return f"{name}-{_hash(request)[:8]}"
