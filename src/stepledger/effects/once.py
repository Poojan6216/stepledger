"""once(): at-least-once external effects with dedupe, never silently repeated.

    ticket = await once("open_ticket", lambda key: jira.create(req, idempotency_key=key),
                        request=req)

Inside a tracked node Activity, the key is sha256(namespace, workflow, run, seq, name, idx), where
idx counts once() calls with this name in this node execution, so it is stable across retries
of the node. `fn` receives the key so the tool can pass it upstream as its own idempotency key.

    no journal row      -> record STARTED, call fn(key), record DONE with the result
    DONE, same request  -> return the recorded result (a duplicate prevented, counted)
    other request       -> EffectDivergence (non-retryable)
    STARTED / UNKNOWN   -> the outcome is unknown: ask `reconcile(key)`. It returns the effect's
                           result if the tool shows it happened, NOT_DONE if the tool shows it did
                           not (then fn is called again), or None if it cannot tell: then mark
                           UNKNOWN and raise UnknownEffectOutcome, which waits (retrying slowly)
                           for `stepledger resolve <key> --outcome done|not-done`
    RESOLVED not-done   -> the effect did not happen: claim it again and call fn(key)

Results are stored as JSON, so fn must return a JSON-serializable value. This is at-least-once
delivery with dedupe, not exactly-once: an effect whose tool ignores the key can repeat if the
worker dies between the call and the DONE write, which is why that case stops for a human.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, TypeVar

import psycopg
import psycopg_pool
from psycopg.types.json import Jsonb

from stepledger._hooks import fault
from stepledger.canonical import chash
from stepledger.errors import EffectDivergence, NotInTrackedNode, UnknownEffectOutcome
from stepledger.ledger.context import current_node

T = TypeVar("T")


class _NotDone:
    def __repr__(self) -> str:
        return "NOT_DONE"


NOT_DONE: Any = _NotDone()
"""Returned by a reconcile callback when the tool shows the effect did not happen."""

UNKNOWN_RETRY_DELAY = timedelta(seconds=30)


async def once(
    name: str,
    fn: Callable[[str], Awaitable[T]],
    *,
    request: Any,
    reconcile: Callable[[str], Awaitable[T | None]] | None = None,
    not_sent: tuple[type[BaseException], ...] = (),
) -> T:
    """`not_sent` names exception types from `fn` that prove the effect did not happen (a 4xx
    validation error, say). Such a failure releases the claim so a retry calls `fn` again;
    any other exception leaves the outcome unknown, exactly like a crash, because it might
    have happened."""
    node = current_node()
    if node is None:
        raise NotInTrackedNode("once() must be called inside a node Activity tracked by Stepledger")
    idx = node.next_effect_idx(name)
    key = node.key.effect_key(name, idx)
    request_hash = chash(request)
    k = node.key

    async with node.store.tx() as tx:
        cur = await tx.conn.execute(
            "SELECT status, request_hash, result, resolution FROM sl_effects WHERE key = %s"
            " FOR UPDATE",
            (key,),
        )
        row = await cur.fetchone()
        if row is None:
            cur = await tx.conn.execute(
                "INSERT INTO sl_effects (key, namespace, workflow_id, run_id, seq, name, idx,"
                " request_hash, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'STARTED')"
                " ON CONFLICT (key) DO NOTHING RETURNING key",
                (key, k.namespace, k.workflow_id, k.run_id, k.seq, name, idx, request_hash),
            )
            if await cur.fetchone() is None:  # a concurrent attempt claimed it first
                cur = await tx.conn.execute(
                    "SELECT status, request_hash, result, resolution FROM sl_effects"
                    " WHERE key = %s FOR UPDATE",
                    (key,),
                )
                row = await cur.fetchone()
        if row is None:
            claimed = True
        else:
            status, stored_hash, result, resolution = row
            if stored_hash != request_hash:
                raise EffectDivergence(key, name)
            if status == "DONE":
                await tx.conn.execute(
                    "UPDATE sl_effects SET duplicates_prevented = duplicates_prevented + 1,"
                    " attempts = attempts + 1 WHERE key = %s",
                    (key,),
                )
                return result  # type: ignore[no-any-return]
            if status == "RESOLVED" and resolution in ("not-done", "not-sent"):
                await tx.conn.execute(
                    "UPDATE sl_effects SET status = 'STARTED', attempts = attempts + 1,"
                    " resolution = NULL WHERE key = %s",
                    (key,),
                )
                claimed = True
            else:
                claimed = False  # STARTED or UNKNOWN: an earlier attempt's outcome is unknown

    if claimed:
        try:
            result = await fn(key)
        except not_sent:
            await _release(node.store, key)  # provably not sent: the retry may call again
            raise
        await fault("FE", effect=name)  # chaos only: a crash between the call and its record
        await _done(node.store, key, result)
        return result

    if reconcile is not None:
        found = await reconcile(key)
        if found is NOT_DONE:
            result = await fn(key)
            await _done(node.store, key, result, resolution="reconciled:not-done")
            return result
        if found is not None:
            await _done(node.store, key, found, resolution="reconciled:done")
            return found
    async with node.store.tx() as tx:
        await tx.conn.execute(
            "UPDATE sl_effects SET status = 'UNKNOWN', attempts = attempts + 1 WHERE key = %s"
            " AND status IN ('STARTED', 'UNKNOWN')",
            (key,),
        )
    raise UnknownEffectOutcome(key, name, next_retry_delay=UNKNOWN_RETRY_DELAY)


async def _done(store: Any, key: str, result: Any, resolution: str | None = None) -> None:
    """Record the result. The effect has happened by now, so a transient database error here
    is retried briefly rather than turning a finished effect into an UNKNOWN one."""
    for attempt in range(4):
        try:
            async with store.tx() as tx:
                await tx.conn.execute(
                    "UPDATE sl_effects SET status = 'DONE', result = %s, done_at = now(),"
                    " resolution = coalesce(%s, resolution) WHERE key = %s AND status <> 'DONE'",
                    (Jsonb(result), resolution, key),
                )
            return
        except (psycopg.Error, psycopg_pool.PoolTimeout, OSError):
            if attempt == 3:
                raise
            await asyncio.sleep(0.2 * (attempt + 1))


async def _release(store: Any, key: str) -> None:
    async with store.tx() as tx:
        await tx.conn.execute(
            "UPDATE sl_effects SET status = 'RESOLVED', resolution = 'not-sent'"
            " WHERE key = %s AND status = 'STARTED'",
            (key,),
        )


async def resolve(store: Any, key: str, outcome: str, result: Any = None) -> str:
    """Record a human's verdict on an effect with an unknown outcome."""
    if outcome not in ("done", "not-done"):
        raise ValueError("outcome must be 'done' or 'not-done'")
    async with store.tx() as tx:
        cur = await tx.conn.execute(
            "SELECT status FROM sl_effects WHERE key = %s FOR UPDATE", (key,)
        )
        row = await cur.fetchone()
        if row is None:
            raise KeyError(f"no effect with key {key}")
        if row[0] == "DONE":
            return "already DONE"
        if outcome == "done":
            await tx.conn.execute(
                "UPDATE sl_effects SET status = 'DONE', result = %s, done_at = now(),"
                " resolution = 'manual:done' WHERE key = %s",
                (Jsonb(result), key),
            )
            return "DONE"
        await tx.conn.execute(
            "UPDATE sl_effects SET status = 'RESOLVED', resolution = 'not-done' WHERE key = %s",
            (key,),
        )
        return "RESOLVED not-done"
