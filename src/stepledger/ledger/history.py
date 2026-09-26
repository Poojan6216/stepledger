"""Read a run's node Activities back out of Temporal's history.

Maps each `stepledger-seq` header on `ActivityTaskScheduled` to that node's completion, cancel
request and attempt, and decodes the result Temporal recorded with the client's data converter
(so External Storage references are followed). Used by `reconcile` and by the test checker.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

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
    started_at: datetime | None = None
    completed_at: datetime | None = None

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
                nodes[st.scheduled_event_id].started_at = ev.event_time.ToDatetime()
        elif et == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
            c = ev.activity_task_completed_event_attributes
            node = nodes[c.scheduled_event_id]
            node.status = "COMPLETED"
            node.completed_at = ev.event_time.ToDatetime()
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
