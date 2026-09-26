"""3.4: reconcile repairs a terminated run from history, idempotently, and leaves sealed runs."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import psycopg
import pytest
from temporalio.api.enums.v1 import EventType

from stepledger.ledger.reconcile import reconcile
from stepledger.ledger.store import LedgerStore
from stepledger.testing.history import check_stepledger
from tests.integration import workflows as wfs
from tests.integration.conftest import Env

pytestmark = pytest.mark.integration


def ledger(dsn: str, wid: str) -> tuple[Any, list[tuple[Any, ...]]]:
    with psycopg.connect(dsn) as conn:
        run = conn.execute(
            "SELECT status, sealed_at IS NOT NULL, sealed_by FROM sl_runs WHERE workflow_id = %s",
            (wid,),
        ).fetchone()
        rows = conn.execute(
            "SELECT seq, node, status FROM sl_nodes WHERE workflow_id = %s ORDER BY seq", (wid,)
        ).fetchall()
    return run, rows


async def _wait_for_task_failure(h: Any) -> None:
    for _ in range(40):
        async for ev in h.fetch_history_events():
            if ev.event_type == EventType.EVENT_TYPE_WORKFLOW_TASK_FAILED:
                return
        await asyncio.sleep(0.25)
    raise AssertionError("no workflow task failure")


async def _terminated_run(env: Env) -> Any:
    """A run stuck on a workflow-task bug, then terminated: never sealed."""
    h = await env.client.start_workflow(
        wfs.TaskBugWorkflow.run, id=f"term-{uuid.uuid4().hex[:8]}", task_queue=env.task_queue
    )
    await _wait_for_task_failure(h)
    await h.terminate(reason="operator gave up")
    return h


async def test_reconcile_terminated_run(env: Env) -> None:
    h = await _terminated_run(env)
    run, rows = ledger(env.dsn, h.id)
    assert run == ("RUNNING", False, None)
    assert [r[2] for r in rows] == ["COMMITTED", "PROVISIONAL"]  # b's commit had no carrier

    store = LedgerStore(env.dsn)
    first = await reconcile(env.client, store, h.id)
    assert [r.action for r in first] == ["sealed"] and first[0].committed == [1]
    run, rows = ledger(env.dsn, h.id)
    assert run == ("TERMINATED", True, "reconcile")
    assert [r[2] for r in rows] == ["COMMITTED", "COMMITTED"]
    desc = await h.describe()
    assert (await check_stepledger(env.client, env.dsn, h.id, desc.run_id)).zero()

    second = await reconcile(env.client, store, h.id)
    assert [r.action for r in second] == ["skipped-sealed"] and not second[0].changed
    assert ledger(env.dsn, h.id) == (run, rows)
    await store.close()


async def test_reconcile_never_changes_a_sealed_run(env: Env) -> None:
    h = await env.client.start_workflow(
        wfs.LinearWorkflow.run,
        wfs.Args("lin5"),
        id=f"sealed-{uuid.uuid4().hex[:8]}",
        task_queue=env.task_queue,
    )
    await h.result()
    before = ledger(env.dsn, h.id)
    store = LedgerStore(env.dsn)
    reports = await reconcile(env.client, store, h.id)
    await store.close()
    assert [r.action for r in reports] == ["skipped-sealed"]
    assert ledger(env.dsn, h.id) == before


async def test_reconcile_abandons_completion_after_cancel_request(env: Env) -> None:
    """R6a: slow completes after the workflow requested its cancellation; the run is then
    terminated before any seal. Reconcile must abandon slow, not commit it."""
    h = await env.client.start_workflow(
        wfs.CancelLateWorkflow.run,
        30.0,
        id=f"r6a-{uuid.uuid4().hex[:8]}",
        task_queue=env.task_queue,
    )
    await asyncio.sleep(5.0)  # slow completed (after its cancel request) and wrote PROVISIONAL
    await h.terminate(reason="stop")
    _, rows = ledger(env.dsn, h.id)
    assert dict((r[1], r[2]) for r in rows) == {"a": "COMMITTED", "slow": "PROVISIONAL"}
    store = LedgerStore(env.dsn)
    (report,) = await reconcile(env.client, store, h.id)
    await store.close()
    assert report.abandoned == [1] and report.committed == []
    _, rows = ledger(env.dsn, h.id)
    assert dict((r[1], r[2]) for r in rows) == {"a": "COMMITTED", "slow": "ABANDONED"}


async def test_reconcile_repairs_divergent_and_missing_rows(env: Env) -> None:
    """R6 on a hand-damaged unsealed run: a tampered PROVISIONAL row is rewritten from history
    (audited DIVERGENCE_REPAIRED) and a deleted row is re-inserted from history."""
    h = await _terminated_run(env)
    with psycopg.connect(env.dsn) as conn:
        conn.execute(
            "UPDATE sl_nodes SET output_hash = 'tampered', output_json = '{}' WHERE workflow_id"
            " = %s AND seq = 1",
            (h.id,),
        )
        conn.execute("DELETE FROM sl_nodes WHERE workflow_id = %s AND seq = 0", (h.id,))
    store = LedgerStore(env.dsn)
    (report,) = await reconcile(env.client, store, h.id)
    await store.close()
    assert report.divergence_repaired == [1] and report.inserted == [0]
    desc = await h.describe()
    assert (await check_stepledger(env.client, env.dsn, h.id, desc.run_id)).zero()
    with psycopg.connect(env.dsn) as conn:
        audits = conn.execute(
            "SELECT seq, outcome FROM sl_node_attempts WHERE workflow_id = %s AND worker ="
            " 'reconcile' ORDER BY seq",
            (h.id,),
        ).fetchall()
    assert audits == [(0, "DIVERGENCE_REPAIRED"), (1, "DIVERGENCE_REPAIRED")]
