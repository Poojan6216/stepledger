"""The invariant checker: ledger rows versus Temporal's own history.

Maps each `stepledger-seq` header on `ActivityTaskScheduled` to that node's completion events,
decodes the result Temporal recorded with the client's data converter (so External Storage
references are followed), and counts what the ledger got wrong. Correctness is always judged
against history, never against the ledger's opinion of itself.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any

import psycopg
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client

from stepledger import headers
from stepledger.canonical import chash


@dataclass
class HistoryNode:
    scheduled_event_id: int
    activity_id: str
    activity_type: str
    seq: int | None
    header_commits: list[int]
    header_abandons: list[int]
    status: str = "SCHEDULED"  # SCHEDULED | COMPLETED | FAILED | TIMED_OUT | CANCELED
    cancel_requested: bool = False  # a cancel request preceded the completion
    result: Any = None  # the ActivityOutput Temporal recorded, as plain JSON
    result_hash: str | None = None
    attempt: int | None = None

    @property
    def accepted(self) -> bool:
        """The workflow received this result (R6 vs R6a)."""
        return self.status == "COMPLETED" and not self.cancel_requested


async def _decode_header(client: Client, p: Payload) -> Payload:
    if p.metadata.get("encoding") == b"json/plain" or client.data_converter.payload_codec is None:
        return p
    return (await client.data_converter.payload_codec.decode([p]))[0]


async def history_nodes(client: Client, workflow_id: str, run_id: str) -> list[HistoryNode]:
    handle = client.get_workflow_handle(workflow_id, run_id=run_id)
    nodes: dict[int, HistoryNode] = {}
    async for ev in handle.fetch_history_events():
        et = ev.event_type
        if et == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            a = ev.activity_task_scheduled_event_attributes
            fields = {k: await _decode_header(client, v) for k, v in a.header.fields.items()}
            seq, commits, abandons = headers.decode(fields)
            nodes[ev.event_id] = HistoryNode(
                ev.event_id, a.activity_id, a.activity_type.name, seq, commits, abandons
            )
        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED:
            sid = ev.activity_task_cancel_requested_event_attributes.scheduled_event_id
            if sid in nodes and nodes[sid].status == "SCHEDULED":
                nodes[sid].cancel_requested = True
        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
            st = ev.activity_task_started_event_attributes
            if st.scheduled_event_id in nodes:
                nodes[st.scheduled_event_id].attempt = st.attempt
        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
            c = ev.activity_task_completed_event_attributes
            node = nodes[c.scheduled_event_id]
            node.status = "COMPLETED"
            if c.result.payloads:
                (value,) = await client.data_converter.decode(list(c.result.payloads))
                node.result = json.loads(json.dumps(value))
                node.result_hash = chash(node.result)
        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED:
            nodes[ev.activity_task_failed_event_attributes.scheduled_event_id].status = "FAILED"
        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:
            sid = ev.activity_task_timed_out_event_attributes.scheduled_event_id
            nodes[sid].status = "TIMED_OUT"
        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
            nodes[ev.activity_task_canceled_event_attributes.scheduled_event_id].status = "CANCELED"
    return list(nodes.values())


@dataclass
class Counters:
    """The five invariant counters (build spec, Demo 2)."""

    rows: int = 0
    duplicate_rows: int = 0  # more than one row for one node Activity execution
    divergent_rows: int = 0  # row output differs from the result Temporal recorded
    lost_rows: int = 0  # accepted in history, no committed row
    orphan_rows: int = 0  # PROVISIONAL after the run sealed / closed
    duplicate_side_effects: int = 0  # extra calls that reached a fake effect sink
    wrongly_committed: int = 0  # COMMITTED although history shows it was not accepted
    details: list[str] = field(default_factory=list)

    def zero(self) -> bool:
        return not any(
            (
                self.duplicate_rows,
                self.divergent_rows,
                self.lost_rows,
                self.orphan_rows,
                self.duplicate_side_effects,
                self.wrongly_committed,
            )
        )

    def add(self, other: Counters) -> None:
        for k, v in asdict(other).items():
            if k == "details":
                self.details.extend(v)
            else:
                setattr(self, k, getattr(self, k) + v)

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("details")
        return d


def _effect_duplicates(conn: psycopg.Connection[Any], workflow_id: str) -> int:
    rows = conn.execute(
        "SELECT count(*) - 1 FROM bench_effects WHERE workflow_id = %s"
        " GROUP BY run_id, effect, request_hash HAVING count(*) > 1",
        (workflow_id,),
    ).fetchall()
    return sum(int(r[0]) for r in rows)


def _has_table(conn: psycopg.Connection[Any], name: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s) IS NOT NULL", (name,)).fetchone()
    return bool(row and row[0])


async def check_stepledger(
    client: Client, dsn: str, workflow_id: str, run_id: str, *, expect_sealed: bool = True
) -> Counters:
    hist = [n for n in await history_nodes(client, workflow_id, run_id) if n.seq is not None]
    by_seq: dict[int, list[HistoryNode]] = {}
    for n in hist:
        by_seq.setdefault(n.seq, []).append(n)  # type: ignore[arg-type]
    out = Counters()
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT seq, status, output_hash FROM sl_nodes WHERE workflow_id = %s AND run_id = %s",
            (workflow_id, run_id),
        ).fetchall()
        sealed = conn.execute(
            "SELECT sealed_at IS NOT NULL FROM sl_runs WHERE workflow_id = %s AND run_id = %s",
            (workflow_id, run_id),
        ).fetchone()
        if _has_table(conn, "bench_effects"):
            out.duplicate_side_effects = _effect_duplicates(conn, workflow_id)
    out.rows = len(rows)
    counts = Counter(r[0] for r in rows)
    out.duplicate_rows = sum(c - 1 for c in counts.values() if c > 1)
    out.duplicate_rows += sum(len(v) - 1 for v in by_seq.values() if len(v) > 1)  # seq reuse
    row_by_seq = {r[0]: r for r in rows}
    for seq, seq_nodes in by_seq.items():
        node = seq_nodes[0]
        row = row_by_seq.get(seq)
        if node.accepted:
            if row is None or row[1] != "COMMITTED":
                out.lost_rows += 1
                out.details.append(f"seq {seq}: accepted in history, row={row and row[1]}")
            elif row[2] != node.result_hash:
                out.divergent_rows += 1
                out.details.append(
                    f"seq {seq}: row hash {row[2][:8]} != history {node.result_hash}"
                )
        elif row is not None and row[1] == "COMMITTED":
            out.wrongly_committed += 1
            out.details.append(f"seq {seq}: COMMITTED but history status {node.status}")
    run_closed = bool(sealed and sealed[0]) if expect_sealed else True
    if run_closed:
        orphans = [r[0] for r in rows if r[1] == "PROVISIONAL"]
        out.orphan_rows = len(orphans)
        if orphans:
            out.details.append(f"PROVISIONAL after seal: {orphans}")
    if expect_sealed and not (sealed and sealed[0]):
        out.details.append("run not sealed")
    return out


async def check_naive(
    client: Client, dsn: str, workflow_id: str, run_id: str, *, table: str
) -> Counters:
    """The same five counters for the B1 / B1u baselines' own tables."""
    hist = {n.activity_id: n for n in await history_nodes(client, workflow_id, run_id)}
    out = Counters()
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            f"SELECT activity_id, output FROM {table} WHERE workflow_id = %s AND run_id = %s",
            (workflow_id, run_id),
        ).fetchall()
        if _has_table(conn, "bench_effects"):
            out.duplicate_side_effects = _effect_duplicates(conn, workflow_id)
    out.rows = len(rows)
    counts = Counter(r[0] for r in rows)
    out.duplicate_rows = sum(c - 1 for c in counts.values() if c > 1)
    seen: dict[str, list[Any]] = {}
    for act_id, output in rows:
        seen.setdefault(act_id, []).append(output)
    for act_id, node in hist.items():
        if node.activity_type.startswith(("bench.", "stepledger.")):
            continue
        outputs = seen.get(act_id, [])
        accepted = (node.result or {}).get("result") if node.accepted else None
        if node.accepted and not outputs:
            out.lost_rows += 1
        for o in outputs:
            if node.accepted and chash(o) != chash(accepted):
                out.divergent_rows += 1
                out.details.append(f"activity {act_id}: row differs from history")
        if not node.accepted and outputs:
            out.orphan_rows += len(outputs)
    return out
