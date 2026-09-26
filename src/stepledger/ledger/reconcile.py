"""History-driven repair: `stepledger reconcile <workflow_id> | --all-open`.

For runs that never sealed (terminated, timed out, or a seal that could not run) Temporal's
history is the ground truth:

    R6   ActivityTaskCompleted for the seq, no earlier cancel request -> COMMITTED; if the row's
         output differs from history, rewrite it from history and audit DIVERGENCE_REPAIRED
         (this must never happen outside attacks); if the row is missing, insert it from history
    R6a  the cancel request came first -> ABANDONED (the workflow never used the result)
         the node failed, timed out, or never finished and the run is closed -> ABANDONED

Once the run is closed, reconcile seals it with the status Temporal reports (sealed_by =
'reconcile'). A sealed run's decisions are never changed; the only exception is a degraded run
(on_ledger_error="warn"), whose undecided PROVISIONAL rows reconcile settles from history.
Idempotent: a second pass changes nothing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from psycopg.types.json import Jsonb
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from stepledger.canonical import serialize
from stepledger.keys import Fence, LedgerKey, RunKey
from stepledger.ledger.history import HistoryNode, history_nodes
from stepledger.ledger.store import LedgerStore

_STATUS = {
    WorkflowExecutionStatus.COMPLETED: "COMPLETED",
    WorkflowExecutionStatus.FAILED: "FAILED",
    WorkflowExecutionStatus.CANCELED: "CANCELLED",
    WorkflowExecutionStatus.TERMINATED: "TERMINATED",
    WorkflowExecutionStatus.TIMED_OUT: "TIMED_OUT",
    WorkflowExecutionStatus.CONTINUED_AS_NEW: "CONTINUED_AS_NEW",
}


@dataclass
class ReconcileReport:
    workflow_id: str
    run_id: str
    run_status: str
    action: str = "none"  # none | repaired | sealed | skipped-sealed | open | history-gone
    committed: list[int] = field(default_factory=list)
    abandoned: list[int] = field(default_factory=list)
    inserted: list[int] = field(default_factory=list)
    divergence_repaired: list[int] = field(default_factory=list)

    @property
    def changed(self) -> bool:
        return bool(self.committed or self.abandoned or self.inserted or self.divergence_repaired)


async def _final_hash(client: Client, workflow_id: str, run_id: str) -> str | None:
    handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    history = await handle.fetch_history()
    attrs = history.events[-1].workflow_execution_completed_event_attributes
    if not attrs.result.payloads:
        return None
    (value,) = await client.data_converter.decode(list(attrs.result.payloads))
    return serialize(value, client.data_converter.payload_converter).hash


async def reconcile_run(
    client: Client,
    store: LedgerStore,
    workflow_id: str,
    run_id: str,
    *,
    include_open: bool = False,
    cancellation_type: str = "TRY_CANCEL",
    store_outputs: str = "full",
) -> ReconcileReport:
    """`include_open`: also repair a run that is still running (its own workflow is writing the
    ledger concurrently, so by default open runs are only reported). `cancellation_type`: the
    ActivityCancellationType the tracked nodes use; history does not record it, and it decides
    whether a completion after a cancel request reached the workflow."""
    handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    try:
        desc = await handle.describe()
    except RPCError as e:
        if e.status != RPCStatusCode.NOT_FOUND:
            raise
        # Past the namespace's retention there is no history to decide from; leave the rows as
        # they are and say so, rather than guess.
        return ReconcileReport(workflow_id, run_id, "UNKNOWN", action="history-gone")
    closed = desc.status not in (None, WorkflowExecutionStatus.RUNNING)
    status = _STATUS.get(desc.status, "RUNNING") if desc.status else "RUNNING"
    report = ReconcileReport(workflow_id, run_id, status)
    run = RunKey(client.namespace, workflow_id, run_id)

    async with store.read() as conn:
        cur = await conn.execute(
            "SELECT sealed_at IS NOT NULL, degraded FROM sl_runs"
            " WHERE namespace = %s AND workflow_id = %s AND run_id = %s",
            (run.namespace, run.workflow_id, run.run_id),
        )
        run_row = await cur.fetchone()
    sealed = bool(run_row and run_row[0])
    degraded = bool(run_row and run_row[1])
    if sealed and not degraded:
        report.action = "skipped-sealed"
        return report
    if not closed and not include_open:
        report.action = "open"
        return report

    nodes = {
        n.seq: n for n in await history_nodes(client, workflow_id, run_id) if n.seq is not None
    }
    async with store.tx() as tx:
        await tx.ensure_run(run, desc.workflow_type, lock=False)
        cur = await tx.conn.execute(
            "SELECT seq, status, output_hash FROM sl_nodes WHERE namespace = %s"
            " AND workflow_id = %s AND run_id = %s FOR UPDATE",
            (run.namespace, run.workflow_id, run.run_id),
        )
        rows = {r[0]: (r[1], r[2]) for r in await cur.fetchall()}
        for seq, node in sorted(nodes.items()):
            row = rows.get(seq)
            if node.accepted_under(cancellation_type):
                if row is None:
                    await _insert_from_history(tx.conn, run, node, store_outputs)
                    report.inserted.append(seq)
                    continue
                if row[0] == "PROVISIONAL":
                    if row[1] != node.result_hash:
                        await _rewrite_from_history(tx.conn, run, node, store_outputs)
                        report.divergence_repaired.append(seq)
                    await tx.commit(run, [seq])
                    report.committed.append(seq)
            elif row is not None and row[0] == "PROVISIONAL":
                finished = node.cancel_requested or node.status in (
                    "FAILED",
                    "TIMED_OUT",
                    "CANCELED",
                )
                if closed or finished:
                    await tx.abandon(run, [seq])
                    report.abandoned.append(seq)
        if closed and not sealed:
            accepted = [s for s, n in nodes.items() if n.accepted_under(cancellation_type)]
            final_hash = (
                await _final_hash(client, workflow_id, run_id) if status == "COMPLETED" else None
            )
            await tx.seal_run(
                run,
                status=status,
                node_count=len(nodes),
                accepted_count=len(accepted),
                commits=accepted,
                abandons=[s for s in nodes if s not in accepted],
                final_state_hash=final_hash,
                workflow_type=desc.workflow_type,
                sealed_by="reconcile",
            )
            report.action = "sealed"
        elif report.changed:
            report.action = "repaired"
        elif not closed:
            report.action = "open"
    return report


def _fence(node: HistoryNode) -> Fence:
    return Fence(node.started_at or datetime.now(UTC), node.attempt or 1)


def _output_columns(node: HistoryNode, store_outputs: str) -> tuple[Any, bytes | None, str]:
    ser = node.result_serialized
    if ser is None:
        return None, None, "json/plain"
    if store_outputs != "full":
        return None, None, ser.encoding
    return (
        (Jsonb(ser.plain) if ser.is_json else None),
        (None if ser.is_json else ser.data),
        ser.encoding,
    )


async def _rewrite_from_history(
    conn: Any, run: RunKey, node: HistoryNode, store_outputs: str
) -> None:
    assert node.seq is not None
    fence = _fence(node)
    output_json, output_bytes, encoding = _output_columns(node, store_outputs)
    await conn.execute(
        "UPDATE sl_nodes SET output_json = %s, output_bytes = %s, output_encoding = %s,"
        " output_hash = %s, attempt = %s WHERE namespace = %s AND workflow_id = %s"
        " AND run_id = %s AND seq = %s AND status = 'PROVISIONAL'",
        (
            output_json,
            output_bytes,
            encoding,
            node.result_hash,
            fence.attempt,
            run.namespace,
            run.workflow_id,
            run.run_id,
            node.seq,
        ),
    )
    await _audit(conn, run.node(node.seq), fence, node, "row differed from history; rewritten")


async def _insert_from_history(
    conn: Any, run: RunKey, node: HistoryNode, store_outputs: str
) -> None:
    assert node.seq is not None
    fence = _fence(node)
    output_json, output_bytes, encoding = _output_columns(node, store_outputs)
    graph = node.activity_type.rsplit(".", 1)[0] if "." in node.activity_type else None
    await conn.execute(
        "INSERT INTO sl_nodes (namespace, workflow_id, run_id, seq, activity_id, activity_type,"
        " graph, attempt, fence_scheduled_at, status, kind, output_json, output_bytes,"
        " output_encoding, output_hash, started_at, finished_at) VALUES (%s, %s, %s, %s, %s, %s,"
        " %s, %s, %s, 'PROVISIONAL', %s, %s, %s, %s, %s, %s, %s)",
        (
            run.namespace,
            run.workflow_id,
            run.run_id,
            node.seq,
            node.activity_id,
            node.activity_type,
            graph,
            fence.attempt,
            fence.scheduled_at,
            _kind(node.result),
            output_json,
            output_bytes,
            encoding,
            node.result_hash,
            fence.scheduled_at,
            node.completed_at or fence.scheduled_at,
        ),
    )
    await conn.execute(
        "UPDATE sl_nodes SET status = 'COMMITTED', committed_at = now() WHERE namespace = %s"
        " AND workflow_id = %s AND run_id = %s AND seq = %s",
        (run.namespace, run.workflow_id, run.run_id, node.seq),
    )
    await _audit(conn, run.node(node.seq), fence, node, "row missing; inserted from history")


def _kind(result: Any) -> str:
    if isinstance(result, dict):
        if result.get("langgraph_interrupts") is not None:
            return "INTERRUPT"
        if result.get("langgraph_command") is not None:
            return "COMMAND"
    return "UPDATE"


async def _audit(conn: Any, key: LedgerKey, fence: Fence, node: HistoryNode, note: str) -> None:
    await conn.execute(
        "INSERT INTO sl_node_attempts (namespace, workflow_id, run_id, seq, attempt,"
        " fence_scheduled_at, output_hash, outcome, worker, note)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, 'DIVERGENCE_REPAIRED', 'reconcile', %s)",
        (
            key.namespace,
            key.workflow_id,
            key.run_id,
            key.seq,
            fence.attempt,
            fence.scheduled_at,
            node.result_hash,
            note,
        ),
    )


async def reconcile(
    client: Client,
    store: LedgerStore,
    workflow_id: str | None = None,
    *,
    all_open: bool = False,
    include_open: bool = False,
    cancellation_type: str = "TRY_CANCEL",
    store_outputs: str = "full",
) -> list[ReconcileReport]:
    """Reconcile every run of `workflow_id`, or every run the ledger has not seen sealed."""
    targets: list[tuple[str, str]] = []
    async with store.read() as conn:
        if all_open:
            cur = await conn.execute(
                "SELECT workflow_id, run_id FROM sl_runs WHERE namespace = %s AND"
                " (sealed_at IS NULL OR (degraded AND EXISTS (SELECT 1 FROM sl_nodes n WHERE"
                " n.namespace = sl_runs.namespace AND n.workflow_id = sl_runs.workflow_id AND"
                " n.run_id = sl_runs.run_id AND n.status = 'PROVISIONAL')))"
                " ORDER BY first_seen_at",
                (client.namespace,),
            )
            targets = [(r[0], r[1]) for r in await cur.fetchall()]
        elif workflow_id:
            cur = await conn.execute(
                "SELECT workflow_id, run_id FROM sl_runs WHERE namespace = %s AND workflow_id = %s"
                " ORDER BY first_seen_at",
                (client.namespace, workflow_id),
            )
            targets = [(r[0], r[1]) for r in await cur.fetchall()]
    return [
        await reconcile_run(
            client,
            store,
            wf,
            run,
            include_open=include_open,
            cancellation_type=cancellation_type,
            store_outputs=store_outputs,
        )
        for wf, run in targets
    ]
