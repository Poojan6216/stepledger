"""Phase 2.3-2.5: headers, the write path, and the seal on every exit path."""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import psycopg
import pytest
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowFailureError, WorkflowHandle

from bench.common import RunConfig, run_in_process
from bench.metrics import collect
from stepledger import headers
from stepledger.canonical import canonical_json
from stepledger.testing.history import check_stepledger, history_nodes
from tests.integration import workflows as wfs
from tests.integration.conftest import INVESTIGATOR, Env

pytestmark = pytest.mark.integration


def _wid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def rows(dsn: str, wid: str, run_id: str | None = None) -> list[tuple[Any, ...]]:
    with psycopg.connect(dsn) as conn:
        q = "SELECT seq, node, status, kind, attempt FROM sl_nodes WHERE workflow_id = %s"
        params: list[Any] = [wid]
        if run_id:
            q += " AND run_id = %s"
            params.append(run_id)
        return conn.execute(q + " ORDER BY run_id, seq", params).fetchall()


def run_row(dsn: str, wid: str, run_id: str) -> dict[str, Any] | None:
    with psycopg.connect(dsn) as conn:
        cur = conn.execute(
            "SELECT status, node_count, committed_count, abandoned_count, sealed_at IS NOT NULL"
            " AS sealed, degraded, missing_seqs, final_state_hash FROM sl_runs"
            " WHERE workflow_id = %s AND run_id = %s",
            (wid, run_id),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return dict(zip([d.name for d in cur.description or []], row, strict=True))


async def seal_input(handle: WorkflowHandle[Any, Any]) -> dict[str, Any] | None:
    async for ev in handle.fetch_history_events():
        if ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            a = ev.activity_task_scheduled_event_attributes
            if a.activity_type.name == "stepledger.seal":
                assert not any(k.startswith("stepledger-") for k in a.header.fields)
                return json.loads(a.input.payloads[0].data)  # type: ignore[no-any-return]
    return None


async def test_headers_on_scheduled_events(env: Env) -> None:
    """2.3: seq 0..4 on five tracked nodes, each carrying the previous node's commit; the
    untracked Activity and the seal carry no Stepledger headers."""
    h = await env.client.start_workflow(
        wfs.LinearWorkflow.run,
        wfs.Args("lin5", untracked=True),
        id=_wid("hdr"),
        task_queue=env.task_queue,
    )
    await h.result()
    nodes = await history_nodes(env.client, h.id, h.result_run_id or "")
    tracked = [n for n in nodes if n.seq is not None]
    assert [n.seq for n in tracked] == [0, 1, 2, 3, 4]
    assert [n.header_commits for n in tracked] == [[], [0], [1], [2], [3]]
    assert all(n.header_abandons == [] for n in tracked)
    other = [n for n in nodes if n.seq is None]
    assert {n.activity_type for n in other} == {"tests.untracked", "stepledger.seal"}
    si = await seal_input(h)
    assert si is not None
    assert si["commits"] == [4] and si["abandons"] == [] and si["node_count"] == 5
    assert si["accepted_count"] == 5 and si["status"] == "COMPLETED"
    r = run_row(env.dsn, h.id, h.result_run_id or "")
    assert r and r["status"] == "COMPLETED" and r["committed_count"] == 5 and not r["degraded"]


async def test_thirty_node_agent_rows_equal_history(env: Env) -> None:
    """2.4: 30 rows, all COMMITTED after seal, each equal to the result in history."""
    cfg = RunConfig(shape=INVESTIGATOR, kb_per_node=1)
    h = await env.client.start_workflow(
        "Investigate", cfg.workflow_input(), id=_wid("inv"), task_queue=env.task_queue
    )
    result = await h.result()
    run_id = h.result_run_id or ""
    got = rows(env.dsn, h.id, run_id)
    assert len(got) == 30 and {r[2] for r in got} == {"COMMITTED"}
    counters = await check_stepledger(env.client, env.dsn, h.id, run_id)
    assert counters.zero(), counters.details
    assert counters.rows == 30
    # 1.1: the same graph and seed with no Temporal gives the same final state
    assert canonical_json(result) == canonical_json(await run_in_process(cfg))
    # 1.2: the server's history size and the sum of serialized events agree within 5%
    m = await collect(h)
    assert abs(m.history_size_bytes - m.history_bytes_summed) / m.history_size_bytes < 0.05
    # 5.3: the views count what the ledger holds
    with psycopg.connect(env.dsn) as conn:
        summary = conn.execute(
            "SELECT rows, committed, provisional, abandoned FROM sl_run_summary"
            " WHERE workflow_id = %s AND run_id = %s",
            (h.id, run_id),
        ).fetchone()
        timeline = conn.execute(
            "SELECT count(*) FROM sl_node_timeline WHERE workflow_id = %s AND run_id = %s",
            (h.id, run_id),
        ).fetchone()
        totals = conn.execute(
            "SELECT (SELECT count(*) FROM sl_run_summary) = (SELECT count(*) FROM sl_runs),"
            " (SELECT count(*) FROM sl_node_timeline) = (SELECT count(*) FROM sl_nodes)"
        ).fetchone()
    assert summary == (30, 30, 0, 0) and timeline == (30,) and totals == (True, True)


async def test_failed_carrier_keeps_its_commits_pending(env: Env) -> None:
    """2.3: the node that carried c's commit fails for good; the commit rides on the next
    carrier and nothing is left PROVISIONAL."""
    h = await env.client.start_workflow(
        wfs.CarrierFailWorkflow.run, id=_wid("carrier"), task_queue=env.task_queue
    )
    await h.result()
    run_id = h.result_run_id or ""
    tracked = [n for n in await history_nodes(env.client, h.id, run_id) if n.seq is not None]
    by_type = {n.activity_type.split(".")[-1]: n for n in tracked}
    assert by_type["fail_hard"].header_commits == [2]
    assert by_type["fail_hard"].status == "FAILED"
    assert 2 in by_type["e"].header_commits and 3 in by_type["e"].header_abandons
    got = {r[1]: r[2] for r in rows(env.dsn, h.id, run_id)}
    assert got == {"a": "COMMITTED", "b": "COMMITTED", "c": "COMMITTED", "e": "COMMITTED"}
    r = run_row(env.dsn, h.id, run_id)
    assert r and r["status"] == "COMPLETED" and not r["degraded"]


async def test_seal_failed(env: Env) -> None:
    h = await env.client.start_workflow(
        wfs.FailWorkflow.run, id=_wid("fail"), task_queue=env.task_queue
    )
    with pytest.raises(WorkflowFailureError):
        await h.result()
    desc = await h.describe()
    r = run_row(env.dsn, h.id, desc.run_id)
    assert r and r["status"] == "FAILED" and r["sealed"]
    assert {x[1]: x[2] for x in rows(env.dsn, h.id)} == {"a": "COMMITTED"}


async def test_seal_cancelled_from_activity_error(env: Env) -> None:
    """A client cancel surfaces in the workflow as ActivityError(cause=CancelledError) raised
    through LangGraph; it must seal CANCELLED, not FAILED."""
    h = await env.client.start_workflow(
        wfs.LinearWorkflow.run,
        wfs.Args("slow_graph"),
        id=_wid("cancel"),
        task_queue=env.task_queue,
    )
    await asyncio.sleep(1.0)
    await h.cancel()
    with pytest.raises(WorkflowFailureError):
        await h.result()
    desc = await h.describe()
    assert desc.status is not None and desc.status.name == "CANCELED"
    r = run_row(env.dsn, h.id, desc.run_id)
    assert r and r["status"] == "CANCELLED" and r["sealed"]
    await asyncio.sleep(3.5)  # let the cancelled `slow` attempt finish and try to write
    assert {x[2] for x in rows(env.dsn, h.id)} <= {"COMMITTED", "ABANDONED"}


async def test_inner_cancelled_error_seals_failed(env: Env) -> None:
    """asyncio.CancelledError with no cancel request fails the run in the SDK: seal FAILED."""
    h = await env.client.start_workflow(
        wfs.InnerCancelWorkflow.run, id=_wid("inner"), task_queue=env.task_queue
    )
    with pytest.raises(WorkflowFailureError):
        await h.result()
    desc = await h.describe()
    r = run_row(env.dsn, h.id, desc.run_id)
    assert r and r["status"] == "FAILED"


@pytest.mark.parametrize("wait", [4.5, 0.0], ids=["late-write-before-seal", "after-seal"])
async def test_completion_after_cancel_request_is_abandoned(env: Env, wait: float) -> None:
    """TRY_CANCEL: the Activity completes after the workflow requested its cancellation. The
    workflow never received that result, so its row ends ABANDONED (or is refused after the
    seal), never COMMITTED."""
    h = await env.client.start_workflow(
        wfs.CancelLateWorkflow.run, wait, id=_wid("late"), task_queue=env.task_queue
    )
    await h.result()
    run_id = h.result_run_id or ""
    await asyncio.sleep(4.0 if wait == 0 else 0.5)
    slow = [
        n for n in await history_nodes(env.client, h.id, run_id) if n.activity_type.endswith("slow")
    ]
    assert slow and slow[0].cancel_requested
    # Before the seal the late completion is recorded after the cancel request; once the run has
    # closed, Temporal cannot record it at all.
    assert slow[0].status == ("COMPLETED" if wait > 0 else "SCHEDULED")
    got = {r[1]: r[2] for r in rows(env.dsn, h.id, run_id)}
    assert got.get("a") == "COMMITTED"
    assert got.get("slow") in (None, "ABANDONED")
    if wait > 0:
        assert got.get("slow") == "ABANDONED"
    else:
        # The run sealed first; the zombie's write was refused and audited as such.
        assert "slow" not in got
        with psycopg.connect(env.dsn) as conn:
            audit = conn.execute(
                "SELECT outcome, note FROM sl_node_attempts WHERE workflow_id = %s AND seq = 1",
                (h.id,),
            ).fetchall()
        assert audit == [("FENCED_OUT", "run sealed")]
    counters = await check_stepledger(env.client, env.dsn, h.id, run_id)
    assert counters.zero(), counters.details


async def test_task_failure_does_not_seal(env: Env) -> None:
    """A plain RuntimeError in workflow code fails the workflow *task*: the run stays open and
    unsealed until the code recovers."""
    h = await env.client.start_workflow(
        wfs.TaskBugWorkflow.run, id=_wid("taskbug"), task_queue=env.task_queue
    )
    for _ in range(40):
        failed = [
            ev
            async for ev in h.fetch_history_events()
            if ev.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED
        ]
        if failed:
            break
        await asyncio.sleep(0.25)
    assert failed, "expected a workflow task failure"
    desc = await h.describe()
    r = run_row(env.dsn, h.id, desc.run_id)
    assert r is not None and r["status"] == "RUNNING" and not r["sealed"]
    await h.signal(wfs.TaskBugWorkflow.fix)
    await h.result()
    r = run_row(env.dsn, h.id, desc.run_id)
    assert r and r["status"] == "COMPLETED" and r["sealed"]


async def test_continue_as_new_seals_each_run(env: Env) -> None:
    h = await env.client.start_workflow(
        wfs.LinearWorkflow.run,
        wfs.Args("ab", continue_as_new=1),
        id=_wid("can"),
        task_queue=env.task_queue,
    )
    await h.result()
    with psycopg.connect(env.dsn) as conn:
        runs = conn.execute(
            "SELECT status, committed_count FROM sl_runs WHERE workflow_id = %s"
            " ORDER BY first_seen_at",
            (h.id,),
        ).fetchall()
    assert runs == [("CONTINUED_AS_NEW", 2), ("COMPLETED", 2)]
    assert {r[2] for r in rows(env.dsn, h.id)} == {"COMMITTED"}


def test_headers_module_constants() -> None:
    assert headers.ALL == ("stepledger-seq", "stepledger-commits", "stepledger-abandons")
