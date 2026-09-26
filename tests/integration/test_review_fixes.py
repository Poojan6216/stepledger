"""Behaviors added after the adversarial review: a cancel that lands during the seal, the CLI's
data converter, reconcile on open runs and under a non-JSON converter, once() hardening, the
multi-turn journal."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from langchain_core.messages import AIMessage, HumanMessage
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from bench.agents.messages_graph import MessagesWorkflow, build
from stepledger import StepledgerPlugin
from stepledger.cli import build_data_converter
from stepledger.effects.once import once
from stepledger.errors import UnknownEffectOutcome
from stepledger.keys import Fence, LedgerKey
from stepledger.ledger.context import NodeContext, _current
from stepledger.ledger.reconcile import reconcile
from stepledger.ledger.store import LedgerStore
from stepledger.llm.journal import JournaledChatModel
from stepledger.testing import faults
from stepledger.testing.fake_llm import FakeLLM
from stepledger.testing.history import check_stepledger
from tests.integration import workflows as wfs
from tests.integration.conftest import Env

pytestmark = pytest.mark.integration


def run_row(dsn: str, wid: str) -> tuple[object, ...] | None:
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT status, sealed_at IS NOT NULL, sealed_by, committed_count FROM sl_runs"
            " WHERE workflow_id = %s",
            (wid,),
        ).fetchone()


async def test_cancel_during_the_seal_does_not_change_the_outcome(env: Env) -> None:
    """The workflow returned; the seal is running (held for 2 s by a fault); a cancel arrives.
    The run must still COMPLETE with its result, and the ledger must agree."""
    wid = f"sealcancel-{uuid.uuid4().hex[:8]}"
    faults.install([faults.FaultSpec("F5", wf=wid, action="hang", hang=2.0)])
    try:
        h = await env.client.start_workflow(
            wfs.LinearWorkflow.run, wfs.Args("ab"), id=wid, task_queue=env.task_queue
        )
        await asyncio.sleep(1.0)  # a and b done; the seal is hanging
        await h.cancel()
        result = await h.result()  # no WorkflowFailureError: the cancel was too late
    finally:
        faults.uninstall()
    assert result == {"log": ["a", "b"]}
    assert (await h.describe()).status == WorkflowExecutionStatus.COMPLETED
    assert run_row(env.dsn, wid) == ("COMPLETED", True, "seal", 2)


async def test_reconcile_leaves_open_runs_alone_unless_told(env: Env) -> None:
    h = await env.client.start_workflow(
        wfs.LinearWorkflow.run,
        wfs.Args("slow_graph"),
        id=f"open-{uuid.uuid4().hex[:8]}",
        task_queue=env.task_queue,
    )
    await asyncio.sleep(0.5)  # a committed, slow running: the run is open
    store = LedgerStore(env.dsn)
    (report,) = await reconcile(env.client, store, h.id)
    assert report.action == "open" and not report.changed
    await store.close()
    await h.result()


async def test_reconcile_agrees_with_the_ledger_under_a_pydantic_converter(dsn: str) -> None:
    """Rows written under pydantic_data_converter hash the converter's bytes; reconcile must
    hash history the same way, or every row looks divergent."""
    lg = LangGraphPlugin(
        graphs={"msgs": build()},
        default_activity_options={"start_to_close_timeout": timedelta(seconds=30)},
    )
    sl = StepledgerPlugin(dsn, langgraph=lg, external_storage=False)
    client = await Client.connect(
        "localhost:7233", plugins=[sl], data_converter=pydantic_data_converter
    )
    tq = f"pyd-{uuid.uuid4().hex[:6]}"
    async with Worker(client, task_queue=tq, workflows=[MessagesWorkflow], plugins=[lg]):
        h = await client.start_workflow(
            MessagesWorkflow.run, id=f"pyd-{uuid.uuid4().hex[:8]}", task_queue=tq
        )
        await h.result()
    run_id = (await h.describe()).run_id
    with psycopg.connect(dsn) as conn:  # pretend the run was never sealed
        conn.execute(
            "UPDATE sl_runs SET sealed_at = NULL, status = 'RUNNING' WHERE workflow_id = %s",
            (h.id,),
        )
        conn.execute("UPDATE sl_nodes SET status = 'PROVISIONAL' WHERE workflow_id = %s", (h.id,))
    (report,) = await reconcile(client, sl.store, h.id)
    await sl.store.close()
    assert report.action == "sealed" and report.divergence_repaired == []
    assert sorted(report.committed) == [0, 1, 2]
    assert (await check_stepledger(client, dsn, h.id, run_id)).zero()


async def test_cli_data_converter_can_reconcile_externalized_runs(dsn: str) -> None:
    lg = LangGraphPlugin(
        graphs={"bigab": wfs.chain(wfs.big_a, wfs.big_b)},
        default_activity_options={"start_to_close_timeout": timedelta(seconds=30)},
    )
    sl = StepledgerPlugin(dsn, langgraph=lg, external_storage=True, payload_size_threshold=1024)
    client = await Client.connect("localhost:7233", plugins=[sl])
    tq = f"cli-{uuid.uuid4().hex[:6]}"
    async with Worker(client, task_queue=tq, workflows=[wfs.TaskBugWorkflow], plugins=[lg]):
        h = await client.start_workflow(
            wfs.TaskBugWorkflow.run, "bigab", id=f"cli-{uuid.uuid4().hex[:8]}", task_queue=tq
        )
        await asyncio.sleep(1.5)  # both nodes ran (results externalized); the task is failing
        await h.terminate(reason="stuck")
    await sl.store.close()

    bare = await Client.connect("localhost:7233")
    store = LedgerStore(dsn)
    with pytest.raises(RuntimeError, match="TMPRL1105"):
        await reconcile(bare, store, h.id)

    tooling = await Client.connect("localhost:7233", data_converter=build_data_converter(dsn))
    (report,) = await reconcile(tooling, store, h.id)
    await store.close()
    assert report.action == "sealed" and sorted(report.committed) == [1]
    assert run_row(dsn, h.id) == ("TERMINATED", True, "reconcile", 2)


T0 = datetime(2026, 9, 26, tzinfo=UTC)


@contextmanager
def attempt(store: LedgerStore, key: LedgerKey, n: int) -> Iterator[NodeContext]:
    ctx = NodeContext(key=key, fence=Fence(T0 + timedelta(seconds=n), n), store=store)
    token = _current.set(ctx)
    try:
        yield ctx
    finally:
        _current.reset(token)


async def test_once_not_sent_releases_the_claim(dsn: str) -> None:
    store = LedgerStore(dsn)
    key = LedgerKey("default", f"notsent-{uuid.uuid4().hex[:8]}", "run", 1)
    calls = 0

    async def flaky(k: str) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("400: bad request")  # the tool proves nothing was sent
        return "TCK-2"

    with attempt(store, key, 1), pytest.raises(ValueError):
        await once("open_ticket", flaky, request={"r": 1}, not_sent=(ValueError,))
    with attempt(store, key, 2):
        assert await once("open_ticket", flaky, request={"r": 1}, not_sent=(ValueError,)) == "TCK-2"
    assert calls == 2
    # without the classifier the same failure parks the effect for a person
    key2 = LedgerKey("default", f"notsent-{uuid.uuid4().hex[:8]}", "run", 1)
    calls = 0
    with attempt(store, key2, 1), pytest.raises(ValueError):
        await once("open_ticket", flaky, request={"r": 1})
    with attempt(store, key2, 2), pytest.raises(UnknownEffectOutcome):
        await once("open_ticket", flaky, request={"r": 1})
    assert calls == 1
    await store.close()


async def test_once_concurrent_claims_call_the_tool_once(dsn: str) -> None:
    store = LedgerStore(dsn)
    key = LedgerKey("default", f"race-{uuid.uuid4().hex[:8]}", "run", 2)
    calls = 0

    async def tool(k: str) -> str:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.3)
        return "TCK"

    async def one(n: int) -> object:
        with attempt(store, key, n):
            try:
                return await once("open_ticket", tool, request={"r": 1})
            except UnknownEffectOutcome as e:
                return e

    results = await asyncio.gather(one(1), one(2))
    assert calls == 1
    assert "TCK" in results and any(isinstance(r, UnknownEffectOutcome) for r in results)
    await store.close()


async def test_journal_replays_every_turn_of_a_multi_turn_node(dsn: str) -> None:
    """Call 2's context contains call 1's reply. On retry both must replay, so the replay
    flag and message ids must not leak into the request hash."""
    store = LedgerStore(dsn)
    key = LedgerKey("default", f"turns-{uuid.uuid4().hex[:8]}", "run", 4)
    billed: list[int] = []

    async def bill(model: str, t_in: int, t_out: int) -> None:
        billed.append(1)

    def model() -> JournaledChatModel:
        return JournaledChatModel(inner=FakeLLM(seed=3, vary_per_attempt=True, bill=bill))

    async def node(n: int) -> tuple[str, str]:
        with attempt(store, key, n):
            m = model()
            first = await m.ainvoke([HumanMessage("plan")])
            second = await m.ainvoke([HumanMessage("plan"), first, HumanMessage("act")])
            return str(first.content), str(second.content)

    a = await node(1)
    assert len(billed) == 2
    b = await node(2)
    assert b == a and len(billed) == 2  # both turns replayed, nothing billed
    with psycopg.connect(dsn) as conn:
        replays = conn.execute(
            "SELECT call_idx, replays FROM sl_llm_calls WHERE workflow_id = %s ORDER BY call_idx",
            (key.workflow_id,),
        ).fetchall()
    assert replays == [(0, 1), (1, 1)]
    assert isinstance(a, tuple) and AIMessage  # keep the import meaningful for readers
    await store.close()
