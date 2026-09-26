"""Garbage collection for the dedup store: mark and sweep over references, never over claims.

A payload is shared by every run and workflow that stored the same bytes (that is what dedupe
means), so age alone never decides anything:

1. Expire references. A reference expires only when no run of its workflow id is open and the
   newest closed run closed longer ago than retention + margin, checked through Temporal's
   visibility (not only the ledger, which knows tracked runs only). Gating on the workflow id,
   not the run id, protects continue-as-new successors, whose input was stored under the
   previous run, and inputs a client stored before the run id existed. References with no
   workflow id (heartbeats, no store context) expire by age only (orphan_ref_days).
2. Delete manifests that no reference needs and that no store touched since
   `sweep_start - grace`.
3. Delete chunks that no manifest lists and that no store touched since `sweep_start - grace`.

A store that races the sweep either bumps last_ref_at first (the delete re-checks the row and
skips it) or finds the manifest gone and writes it again. `dry_run` reports without deleting.
"""

from __future__ import annotations

import contextlib
from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import psycopg
from temporalio.client import Client, WorkflowExecutionStatus

Hook = Callable[[str], Awaitable[None]]


@dataclass
class SweepReport:
    dry_run: bool
    sweep_start: datetime | None = None
    workflows_checked: int = 0
    workflows_kept_open: int = 0
    workflows_kept_retained: int = 0
    workflows_expired: int = 0
    refs_expired: int = 0
    orphan_refs_expired: int = 0
    manifests_deleted: int = 0
    chunks_deleted: int = 0
    chunk_bytes_deleted: int = 0
    other_namespaces_skipped: int = 0
    details: list[str] = field(default_factory=list)


async def namespace_retention(client: Client) -> timedelta:
    from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest

    resp = await client.workflow_service.describe_namespace(
        DescribeNamespaceRequest(namespace=client.namespace)
    )
    return resp.config.workflow_execution_retention_ttl.ToTimedelta()


async def _workflow_expired(client: Client, workflow_id: str, cutoff: datetime) -> str:
    """'open' | 'retained' | 'expired' for every run of this workflow id."""
    newest_close: datetime | None = None
    escaped = workflow_id.replace("'", "\\'")
    async for wf in client.list_workflows(f"WorkflowId = '{escaped}'"):
        if wf.status in (None, WorkflowExecutionStatus.RUNNING):
            return "open"
        if wf.close_time is not None and (newest_close is None or wf.close_time > newest_close):
            newest_close = wf.close_time
    if newest_close is not None and newest_close > cutoff:
        return "retained"
    return "expired"  # closed long enough ago, or no longer visible at all (past retention)


async def sweep(
    client: Client,
    dsn: str,
    *,
    retention: timedelta,
    margin: timedelta = timedelta(days=7),
    grace: timedelta = timedelta(hours=1),
    orphan_ref_age: timedelta = timedelta(days=90),
    dry_run: bool = True,
    batch: int = 500,
    only_workflows: Collection[str] | None = None,
    hook: Hook | None = None,
) -> SweepReport:
    """One mark-and-sweep pass. `only_workflows` limits which workflow ids' references may
    expire (manifests and chunks are still swept globally, but only what lost its last
    reference). `hook(phase)` is awaited between phases (tests use it)."""
    report = SweepReport(dry_run=dry_run)
    ns = client.namespace
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        cur = await conn.execute("SELECT now()")
        row = await cur.fetchone()
        assert row is not None
        sweep_start: datetime = row[0]
        report.sweep_start = sweep_start
        touched_before = sweep_start - grace
        cutoff = sweep_start - retention - margin

        # 1. Mark: which workflow ids can no longer need anything they stored.
        cur = await conn.execute(
            "SELECT DISTINCT namespace, workflow_id FROM sl_payload_refs WHERE workflow_id <> ''"
        )
        expired: list[str] = []
        for ref_ns, wf in await cur.fetchall():
            if only_workflows is not None and wf not in only_workflows:
                continue
            if ref_ns != ns:
                report.other_namespaces_skipped += 1
                continue
            report.workflows_checked += 1
            verdict = await _workflow_expired(client, wf, cutoff)
            if verdict == "open":
                report.workflows_kept_open += 1
            elif verdict == "retained":
                report.workflows_kept_retained += 1
            else:
                report.workflows_expired += 1
                expired.append(wf)
        if hook:
            await hook("marked")

        # A dry run does everything inside one transaction and rolls it back, so its counts are
        # exact; a real sweep commits each batch on its own (autocommit), holding no long locks.
        scope = conn.transaction(force_rollback=True) if dry_run else contextlib.nullcontext()
        async with scope:
            if expired:
                cur = await conn.execute(
                    "DELETE FROM sl_payload_refs WHERE namespace = %s AND workflow_id = ANY(%s)"
                    " AND last_ref_at < %s",
                    (ns, expired, touched_before),
                )
                report.refs_expired = cur.rowcount
            if only_workflows is None:
                cur = await conn.execute(
                    "DELETE FROM sl_payload_refs WHERE workflow_id = '' AND last_ref_at < %s",
                    (sweep_start - orphan_ref_age,),
                )
                report.orphan_refs_expired = cur.rowcount
            if hook:
                await hook("refs_expired")

            # 2. Manifests nobody references and nobody touched within the grace window.
            while True:
                cur = await conn.execute(
                    "DELETE FROM sl_payloads WHERE claim IN (SELECT p.claim FROM sl_payloads p"
                    " WHERE p.last_ref_at < %s AND NOT EXISTS (SELECT 1 FROM sl_payload_refs r"
                    " WHERE r.claim = p.claim) LIMIT %s FOR UPDATE SKIP LOCKED)"
                    " AND last_ref_at < %s AND NOT EXISTS (SELECT 1 FROM sl_payload_refs r"
                    " WHERE r.claim = sl_payloads.claim)",
                    (touched_before, batch, touched_before),
                )
                report.manifests_deleted += cur.rowcount
                if cur.rowcount < batch:
                    break
            if hook:
                await hook("manifests_deleted")

            # 3. Chunks no manifest lists and nobody touched within the grace window.
            while True:
                cur = await conn.execute(
                    "DELETE FROM sl_chunks WHERE hash IN (SELECT c.hash FROM sl_chunks c"
                    " WHERE c.last_ref_at < %s AND NOT EXISTS (SELECT 1 FROM sl_payloads p"
                    " WHERE p.chunks @> ARRAY[c.hash]) LIMIT %s FOR UPDATE SKIP LOCKED)"
                    " AND last_ref_at < %s AND NOT EXISTS (SELECT 1 FROM sl_payloads p"
                    " WHERE p.chunks @> ARRAY[sl_chunks.hash]) RETURNING size",
                    (touched_before, batch, touched_before),
                )
                sizes = [r[0] for r in await cur.fetchall()]
                report.chunks_deleted += len(sizes)
                report.chunk_bytes_deleted += sum(sizes)
                if len(sizes) < batch:
                    break
    return report


def check_retention(configured: timedelta, namespace: timedelta) -> str | None:
    """The CLI refuses to sweep with a retention shorter than the namespace's own."""
    if configured < namespace:
        return (
            f"gc retention {configured} is shorter than the namespace's workflow retention"
            f" {namespace}; a closed workflow could still be read after its payloads are gone"
        )
    return None


def summary(r: SweepReport) -> dict[str, Any]:
    d = dict(r.__dict__)
    d["sweep_start"] = r.sweep_start.isoformat() if r.sweep_start else None
    return d
