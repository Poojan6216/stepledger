"""on_ledger_error="warn": a failed ledger write lets the node complete, is audited as DB_ERROR,
degrades the run at the seal, and is repaired by reconcile. No outage needed: the first ledger
transaction is made to fail."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

import psycopg
import pytest
from temporalio.client import Client
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.worker import Worker

from stepledger import StepledgerPlugin
from stepledger.ledger.reconcile import reconcile
from stepledger.testing.history import check_stepledger
from tests.integration import workflows as wfs

pytestmark = pytest.mark.integration


async def test_warn_mode_audits_degrades_and_reconciles(dsn: str) -> None:
    lg = LangGraphPlugin(
        graphs={"lin5": wfs.chain(wfs.a, wfs.b, wfs.c, wfs.d, wfs.e)},
        default_activity_options={"start_to_close_timeout": timedelta(seconds=30)},
    )
    sl = StepledgerPlugin(dsn, langgraph=lg, external_storage=False, on_ledger_error="warn")
    real_tx = sl.store.tx
    calls = {"n": 0}
    # tx calls in order: a (1, fails), a's degrade audit (2), b..e (3-6), the seal (7, fails)
    failing = {1, 7}

    @asynccontextmanager
    async def flaky_tx() -> AsyncIterator[Any]:
        calls["n"] += 1
        if calls["n"] in failing:
            raise psycopg.OperationalError("injected: database unreachable")
        async with real_tx() as tx:
            yield tx

    sl.store.tx = flaky_tx  # type: ignore[method-assign]
    client = await Client.connect("localhost:7233", plugins=[sl])
    tq = f"warn-{uuid.uuid4().hex[:8]}"
    async with Worker(client, task_queue=tq, workflows=[wfs.LinearWorkflow], plugins=[lg]):
        h = await client.start_workflow(
            wfs.LinearWorkflow.run,
            wfs.Args("lin5"),
            id=f"warn-{uuid.uuid4().hex[:8]}",
            task_queue=tq,
        )
        result = await h.result()
    assert result == {"log": ["a", "b", "c", "d", "e"]}  # the node completed despite the failure
    run_id = h.result_run_id or ""
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT seq, status FROM sl_nodes WHERE workflow_id = %s ORDER BY seq", (h.id,)
        ).fetchall()
        audit = conn.execute(
            "SELECT seq, outcome, note FROM sl_node_attempts WHERE workflow_id = %s"
            " AND outcome = 'DB_ERROR'",
            (h.id,),
        ).fetchall()
        run = conn.execute(
            "SELECT status, degraded, sealed_at IS NOT NULL FROM sl_runs WHERE workflow_id = %s",
            (h.id,),
        ).fetchone()
    assert [r[0] for r in rows] == [1, 2, 3, 4]
    assert [r[1] for r in rows] == ["COMMITTED", "COMMITTED", "COMMITTED", "PROVISIONAL"]
    assert len(audit) == 1 and audit[0][0] == 0 and "injected" in audit[0][2]
    assert run == ("RUNNING", True, False)  # the seal failed: left open, marked degraded
    before = await check_stepledger(client, dsn, h.id, run_id, expect_sealed=False)
    assert before.lost_rows == 1

    sl.store.tx = real_tx  # type: ignore[method-assign]
    (report,) = await reconcile(client, sl.store, h.id)
    await sl.store.close()
    assert report.action == "sealed" and report.inserted == [0] and report.committed == [4]
    after = await check_stepledger(client, dsn, h.id, run_id)
    assert after.zero(), after.details
