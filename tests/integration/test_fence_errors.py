"""Hard Rules 3 and 4 by name: a planted row fences a live node out.

R2: the row is already COMMITTED (a zombie meeting a final row): the write is refused, audited
FENCED_OUT, the attempt raises non-retryable FencedOutFinal, and the row never changes.
R1: the row is PROVISIONAL with a newer fence: the attempt raises retryable FencedOut on every
attempt and the row's content never changes (the seal abandons it when the workflow ends).
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg
import pytest
from temporalio.api.enums.v1 import EventType
from temporalio.client import Client, WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.worker import Worker

from stepledger import StepledgerPlugin
from tests.integration import workflows as wfs
from tests.integration.conftest import Env

pytestmark = pytest.mark.integration


def plant(dsn: str, wid: str, run_id: str, seq: int, status: str, fence_at: datetime) -> None:
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "INSERT INTO sl_nodes (namespace, workflow_id, run_id, seq, activity_id, activity_type,"
            " node, attempt, fence_scheduled_at, status, kind, output_json, output_hash,"
            " started_at, finished_at) VALUES ('default', %s, %s, %s, 'planted', 'planted',"
            " 'slow', 1, %s, %s, 'UPDATE', '{\"planted\": true}', 'planted', now(), now())",
            (wid, run_id, seq, fence_at, status),
        )


def row(dsn: str, wid: str, seq: int) -> tuple[Any, ...] | None:
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT status, output_hash, attempt FROM sl_nodes WHERE workflow_id = %s AND seq = %s",
            (wid, seq),
        ).fetchone()


def audits(dsn: str, wid: str, seq: int) -> list[tuple[Any, ...]]:
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT attempt, outcome FROM sl_node_attempts WHERE workflow_id = %s AND seq = %s"
            " ORDER BY id",
            (wid, seq),
        ).fetchall()


async def failure_types(handle: Any) -> list[str]:
    out = []
    async for ev in handle.fetch_history_events():
        if ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_FAILED:
            f = ev.activity_task_failed_event_attributes.failure
            out.append(f.application_failure_info.type)
    return out


async def test_r2_write_on_a_committed_row_is_final(env: Env) -> None:
    """`slow` (seq 1) sleeps 3 s; meanwhile its row is planted COMMITTED with an older fence."""
    h = await env.client.start_workflow(
        wfs.LinearWorkflow.run,
        wfs.Args("slow_graph"),
        id=f"r2-{uuid.uuid4().hex[:8]}",
        task_queue=env.task_queue,
    )
    await asyncio.sleep(1.0)  # a (seq 0) is done, slow (seq 1) is running
    run_id = (await h.describe()).run_id
    plant(env.dsn, h.id, run_id, 1, "COMMITTED", datetime(2020, 1, 1, tzinfo=UTC))
    with pytest.raises(WorkflowFailureError):
        await h.result()
    assert row(env.dsn, h.id, 1) == ("COMMITTED", "planted", 1)  # untouched
    assert audits(env.dsn, h.id, 1) == [(1, "FENCED_OUT")]  # one attempt, no retry
    assert await failure_types(h) == ["StepledgerFencedOutFinal"]


async def test_r1_newer_provisional_fence_is_retryable(dsn: str) -> None:
    lg = LangGraphPlugin(
        graphs={"slow_graph": wfs.chain(wfs.a, wfs.slow)},
        default_activity_options={
            "start_to_close_timeout": timedelta(seconds=30),
            "retry_policy": RetryPolicy(
                initial_interval=timedelta(milliseconds=200), maximum_attempts=3
            ),
        },
    )
    sl = StepledgerPlugin(dsn, langgraph=lg, external_storage=False)
    client = await Client.connect("localhost:7233", plugins=[sl])
    tq = f"r1-{uuid.uuid4().hex[:8]}"
    async with Worker(client, task_queue=tq, workflows=[wfs.LinearWorkflow], plugins=[lg]):
        h = await client.start_workflow(
            wfs.LinearWorkflow.run,
            wfs.Args("slow_graph"),
            id=f"r1-{uuid.uuid4().hex[:8]}",
            task_queue=tq,
        )
        await asyncio.sleep(1.0)
        run_id = (await h.describe()).run_id
        future = datetime.now(UTC) + timedelta(hours=1)  # a fence no live attempt can beat
        plant(dsn, h.id, run_id, 1, "PROVISIONAL", future)
        with pytest.raises(WorkflowFailureError):
            await h.result()
    # The content never changed. The status did: once the activity failed for good the workflow
    # ended, and the seal abandoned the still-PROVISIONAL row (R5), which is what a workflow exit
    # must do with a result it never accepted.
    assert row(dsn, h.id, 1) == ("ABANDONED", "planted", 1)
    assert audits(dsn, h.id, 1) == [(1, "FENCED_OUT"), (2, "FENCED_OUT"), (3, "FENCED_OUT")]
    assert set(await failure_types(h)) == {"StepledgerFencedOut"}
    await sl.store.close()
