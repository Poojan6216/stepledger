"""Garbage collection for the dedup store: mark and sweep over references, never over claims.

A payload is shared by every run and workflow that stored the same bytes (that is what dedupe
means), so age alone never decides anything:

1. Expire references. A reference expires only when no run of its workflow id is open and the
   newest closed run closed longer ago than retention + margin, asked of Temporal's history
   service (`describe`, whose NOT_FOUND is authoritative), never of the ledger alone and never
   of visibility alone. Ids that a schedule starts are kept while the schedule exists, and a
   reference never expires before it is retention + margin old. Gating on the workflow id,
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

from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import psycopg
from temporalio.client import Client, ScheduleActionStartWorkflow, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

Hook = Callable[[str], Awaitable[None]]


@dataclass
class SweepReport:
    dry_run: bool
    sweep_start: datetime | None = None
    workflows_checked: int = 0
    workflows_kept_open: int = 0
    workflows_kept_retained: int = 0
    workflows_expired: int = 0
    schedule_actions_kept: int = 0
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


async def _schedule_action_ids(client: Client) -> set[str]:
    """Workflow ids that schedules start. Their start input is stored under that id when the
    schedule is created, and the firings run under timestamped ids, so the action id itself
    never appears as a workflow: its references must live as long as the schedule does."""
    ids: set[str] = set()
    async for entry in await client.list_schedules():
        try:
            desc = await client.get_schedule_handle(entry.id).describe()
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND:
                continue
            raise
        action = desc.schedule.action
        if isinstance(action, ScheduleActionStartWorkflow):
            ids.add(action.id)
    return ids


async def _workflow_expired(client: Client, workflow_id: str, cutoff: datetime) -> str:
    """'open' | 'retained' | 'expired' for the workflow id, from the history service (the
    authoritative answer: NOT_FOUND means every run is past retention), never from visibility
    alone. Schedule firings run under `<id>-<time>`; when the id itself is gone, a firing that is
    open or closed within retention still keeps it."""
    try:
        desc = await client.get_workflow_handle(workflow_id).describe()  # the latest run
    except RPCError as e:
        if e.status != RPCStatusCode.NOT_FOUND:
            raise
        prefix = workflow_id.replace("\\", "\\\\").replace("'", "\\'") + "-"
        async for wf in client.list_workflows(f"WorkflowId STARTS_WITH '{prefix}'"):
            if wf.status in (None, WorkflowExecutionStatus.RUNNING):
                return "open"
            if wf.close_time is not None and wf.close_time > cutoff:
                return "retained"
        return "expired"
    if desc.status in (None, WorkflowExecutionStatus.RUNNING):
        return "open"
    if desc.close_time is not None and desc.close_time > cutoff:
        return "retained"
    return "expired"


_DRY_RUN_COUNTS = """
WITH exp_refs AS (
  SELECT claim, namespace, workflow_id, run_id FROM sl_payload_refs
  WHERE (namespace = %(ns)s AND workflow_id = ANY(%(expired)s)
         AND last_ref_at < %(touched)s AND created_at < %(cutoff)s)
     OR (%(orphans)s AND workflow_id = '' AND last_ref_at < %(orphan_cutoff)s)),
live_refs AS (
  SELECT r.claim FROM sl_payload_refs r WHERE NOT EXISTS (
    SELECT 1 FROM exp_refs e WHERE e.claim = r.claim AND e.namespace = r.namespace
      AND e.workflow_id = r.workflow_id AND e.run_id = r.run_id)),
dead_manifests AS (
  SELECT p.claim, p.chunks FROM sl_payloads p WHERE p.last_ref_at < %(touched)s
    AND NOT EXISTS (SELECT 1 FROM live_refs l WHERE l.claim = p.claim)),
live_chunks AS (
  SELECT DISTINCT unnest(p.chunks) AS h FROM sl_payloads p
  WHERE NOT EXISTS (SELECT 1 FROM dead_manifests d WHERE d.claim = p.claim)),
dead_chunks AS (
  SELECT c.hash, c.size FROM sl_chunks c WHERE c.last_ref_at < %(touched)s
    AND NOT EXISTS (SELECT 1 FROM live_chunks l WHERE l.h = c.hash))
SELECT (SELECT count(*) FROM exp_refs WHERE workflow_id <> ''),
       (SELECT count(*) FROM exp_refs WHERE workflow_id = ''),
       (SELECT count(*) FROM dead_manifests),
       (SELECT count(*) FROM dead_chunks),
       (SELECT coalesce(sum(size), 0) FROM dead_chunks)
"""


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
    enforce_namespace_retention: bool = True,
    hook: Hook | None = None,
) -> SweepReport:
    """One mark-and-sweep pass.

    `only_workflows` limits which workflow ids' references may expire (manifests and chunks are
    still swept globally, but only what lost its last reference). `enforce_namespace_retention`
    refuses a `retention` shorter than the namespace's workflow retention. A dry run takes no
    locks: it counts what a real sweep would delete. `hook(phase)` is awaited between phases
    (tests use it)."""
    if enforce_namespace_retention:
        problem = check_retention(retention, await namespace_retention(client))
        if problem:
            raise ValueError(problem)
    report = SweepReport(dry_run=dry_run)
    ns = client.namespace
    schedule_ids = await _schedule_action_ids(client)
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
            if wf in schedule_ids:
                report.schedule_actions_kept += 1
                continue
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

        params = {
            "ns": ns,
            "expired": expired,
            "touched": touched_before,
            "cutoff": cutoff,  # a reference never expires before retention + margin, whatever
            "orphans": only_workflows is None,  # the history service says about its workflow
            "orphan_cutoff": sweep_start - orphan_ref_age,
        }
        if dry_run:
            cur = await conn.execute(_DRY_RUN_COUNTS, params)
            counts = await cur.fetchone()
            assert counts is not None
            (
                report.refs_expired,
                report.orphan_refs_expired,
                report.manifests_deleted,
                report.chunks_deleted,
                report.chunk_bytes_deleted,
            ) = (int(c) for c in counts)
            return report

        if expired:
            cur = await conn.execute(
                "DELETE FROM sl_payload_refs WHERE namespace = %(ns)s"
                " AND workflow_id = ANY(%(expired)s) AND last_ref_at < %(touched)s"
                " AND created_at < %(cutoff)s",
                params,
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

        # 2. Manifests nobody references and nobody touched within the grace window. Each batch
        #    is its own transaction (autocommit) and never waits for a lock.
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
