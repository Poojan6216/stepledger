"""Run metrics read from Temporal: history size and events, largest payload, node timings.

History size is read two ways so they can be cross-checked: the server's own
`describe().raw_info.history_size_bytes`, and the sum of serialized events from
`fetch_history()`.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from typing import Any

from google.protobuf.message import Message
from temporalio.api.common.v1 import Payload
from temporalio.api.enums.v1 import EventType, WorkflowTaskFailedCause
from temporalio.api.history.v1 import HistoryEvent
from temporalio.client import WorkflowExecutionStatus, WorkflowHandle

_PAYLOAD = Payload.DESCRIPTOR.full_name
MB = 1024 * 1024


def iter_payloads(msg: Message) -> Iterator[Payload]:
    """Every Payload nested anywhere in a proto message (inputs, results, headers, memo...)."""
    if msg.DESCRIPTOR.full_name == _PAYLOAD:
        yield msg  # type: ignore[misc]
        return
    for fd, value in msg.ListFields():
        if fd.message_type is None:
            continue
        if fd.is_repeated:
            if fd.message_type.GetOptions().map_entry:
                for item in value.values():
                    if isinstance(item, Message):
                        yield from iter_payloads(item)
            else:
                for item in value:
                    yield from iter_payloads(item)
        else:
            yield from iter_payloads(value)


@dataclass
class NodeTiming:
    activity_id: str
    activity_type: str
    scheduled_event_id: int
    attempts: int
    schedule_to_close_ms: float | None
    input_bytes: int
    result_bytes: int | None


@dataclass
class RunMetrics:
    workflow_id: str
    run_id: str
    status: str
    history_size_bytes: int  # describe(), the server's number
    history_events: int  # describe()
    history_bytes_summed: int  # sum of serialized events from fetch_history()
    payload_bytes_total: int
    largest_payload_bytes: int
    largest_payload_event: str
    activities_scheduled: int
    activities_completed: int
    last_event: str
    workflow_task_failed_cause: str | None
    wall_clock_s: float | None
    nodes: list[NodeTiming] = field(default_factory=list)

    def to_json(self, *, with_nodes: bool = False) -> dict[str, Any]:
        out = asdict(self)
        if not with_nodes:
            out.pop("nodes")
        return out


async def collect(handle: WorkflowHandle[Any, Any]) -> RunMetrics:
    desc = await handle.describe()
    events: list[HistoryEvent] = [ev async for ev in handle.fetch_history_events()]
    summed = sum(ev.ByteSize() for ev in events)
    largest, largest_where, total = 0, "", 0
    scheduled: dict[int, NodeTiming] = {}
    scheduled_at: dict[int, Any] = {}
    completed = 0
    task_failed_cause: str | None = None
    for ev in events:
        for p in iter_payloads(ev):
            size = p.ByteSize()
            total += size
            if size > largest:
                largest, largest_where = size, f"{EventType.Name(ev.event_type)}#{ev.event_id}"
        if ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            a = ev.activity_task_scheduled_event_attributes
            scheduled[ev.event_id] = NodeTiming(
                activity_id=a.activity_id,
                activity_type=a.activity_type.name,
                scheduled_event_id=ev.event_id,
                attempts=0,
                schedule_to_close_ms=None,
                input_bytes=a.input.ByteSize(),
                result_bytes=None,
            )
            scheduled_at[ev.event_id] = ev.event_time.ToDatetime()
        elif ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
            a2 = ev.activity_task_started_event_attributes
            if a2.scheduled_event_id in scheduled:
                scheduled[a2.scheduled_event_id].attempts = a2.attempt
        elif ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
            a3 = ev.activity_task_completed_event_attributes
            node = scheduled.get(a3.scheduled_event_id)
            completed += 1
            if node is not None:
                node.result_bytes = a3.result.ByteSize()
                delta = ev.event_time.ToDatetime() - scheduled_at[a3.scheduled_event_id]
                node.schedule_to_close_ms = delta.total_seconds() * 1000
        elif ev.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED:
            cause = ev.workflow_task_failed_event_attributes.cause
            task_failed_cause = WorkflowTaskFailedCause.Name(cause)
    status = desc.status.name if isinstance(desc.status, WorkflowExecutionStatus) else "UNKNOWN"
    wall = None
    if desc.close_time is not None and desc.start_time is not None:
        wall = (desc.close_time - desc.start_time).total_seconds()
    return RunMetrics(
        workflow_id=handle.id,
        run_id=desc.run_id,
        status=status,
        history_size_bytes=desc.raw_info.history_size_bytes,
        history_events=desc.history_length,
        history_bytes_summed=summed,
        payload_bytes_total=total,
        largest_payload_bytes=largest,
        largest_payload_event=largest_where,
        activities_scheduled=len(scheduled),
        activities_completed=completed,
        last_event=EventType.Name(events[-1].event_type) if events else "",
        workflow_task_failed_cause=task_failed_cause,
        wall_clock_s=wall,
        nodes=list(scheduled.values()),
    )


async def store_bytes(dsn: str) -> dict[str, int]:
    """Bytes held by the dedup store: unique chunk bytes and manifest count."""
    import psycopg

    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cur = await conn.execute(
            "SELECT coalesce(sum(size), 0), count(*) FROM sl_chunks"
            " UNION ALL SELECT coalesce(sum(size), 0), count(*) FROM sl_payloads"
        )
        (chunk_bytes, chunks), (payload_bytes, payloads) = await cur.fetchall()
    return {
        "chunk_bytes": int(chunk_bytes),
        "chunks": int(chunks),
        "logical_payload_bytes": int(payload_bytes),
        "payloads": int(payloads),
    }
