"""6.2: the LLM journal replays a crashed attempt's answer and never replays a changed request."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from langchain_core.messages import HumanMessage

from stepledger.keys import Fence, LedgerKey
from stepledger.ledger.context import NodeContext, _current
from stepledger.ledger.store import LedgerStore
from stepledger.llm.journal import JournaledChatModel, journaled_call
from stepledger.llm.meter import REPLAYED, CostMeter
from stepledger.testing import fake_llm
from stepledger.testing.fake_llm import FakeLLM

pytestmark = pytest.mark.integration

T0 = datetime(2026, 9, 26, tzinfo=UTC)


@contextmanager
def attempt(store: LedgerStore, key: LedgerKey, n: int) -> Iterator[NodeContext]:
    """One Activity attempt of a node: a fresh context (fresh call counters) for the same key,
    and the fake model sees attempt n (with vary_per_attempt it answers differently)."""
    ctx = NodeContext(key=key, fence=Fence(T0 + timedelta(seconds=n), n), store=store)
    token = _current.set(ctx)
    orig = fake_llm.current_attempt
    fake_llm.current_attempt = lambda: n  # type: ignore[assignment]
    try:
        yield ctx
    finally:
        fake_llm.current_attempt = orig  # type: ignore[assignment]
        _current.reset(token)


async def test_retry_replays_and_changed_request_does_not(dsn: str) -> None:
    store = LedgerStore(dsn)
    key = LedgerKey("default", f"journal-{uuid.uuid4().hex[:8]}", "run", 3)
    billed: list[int] = []

    async def bill(model: str, t_in: int, t_out: int) -> None:
        billed.append(t_in + t_out)

    meter = CostMeter({"claude-haiku-4-5": {"input": 1.0, "output": 5.0}})
    prompt = [HumanMessage("enrich_cve_3: investigate acct (call 0)")]

    def model() -> JournaledChatModel:
        inner = FakeLLM(
            seed=7,
            model_name="claude-haiku-4-5",
            tokens_per_call=500,
            vary_per_attempt=True,
            bill=bill,
        )
        return JournaledChatModel(inner=inner)

    with attempt(store, key, 1), meter.scope() as u1:
        first = await model().ainvoke(prompt)
    assert billed == [1000] and u1.calls == 1 and u1.cost is not None and u1.cost > 0

    # The worker died after the call (F2). Unjournaled, attempt 2 would answer differently:
    with attempt(store, key, 2):
        assert (
            await FakeLLM(
                seed=7, vary_per_attempt=True, model_name="claude-haiku-4-5", tokens_per_call=500
            ).ainvoke(prompt)
        ).content != first.content
    with attempt(store, key, 2), meter.scope() as u2:
        second = await model().ainvoke(prompt)
    assert second.content == first.content  # attempt 1's decision, replayed
    assert second.response_metadata.get(REPLAYED) is True
    assert billed == [1000]  # no new spend
    assert u2.calls == 0 and u2.replays == 1

    # A changed request is a different call: never replayed.
    with attempt(store, key, 3):
        third = await model().ainvoke([HumanMessage("a different prompt")])
    assert third.content != first.content and billed == [1000, 1000]

    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT call_idx, replays, first_attempt FROM sl_llm_calls WHERE workflow_id = %s"
            " ORDER BY first_attempt",
            (key.workflow_id,),
        ).fetchall()
    assert rows == [(0, 1, 1), (0, 0, 3)]
    await store.close()


async def test_outside_a_node_is_a_passthrough(dsn: str) -> None:
    billed: list[int] = []

    async def bill(model: str, t_in: int, t_out: int) -> None:
        billed.append(1)

    m = JournaledChatModel(inner=FakeLLM(bill=bill))
    await m.ainvoke([HumanMessage("x")])
    await m.ainvoke([HumanMessage("x")])
    assert billed == [1, 1]


async def test_journaled_call_for_raw_clients(dsn: str) -> None:
    store = LedgerStore(dsn)
    key = LedgerKey("default", f"raw-{uuid.uuid4().hex[:8]}", "run", 0)
    calls = 0

    async def sdk_call() -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {"text": f"answer {calls}", "usage": [10, 20]}

    with attempt(store, key, 1):
        a = await journaled_call(sdk_call, request={"prompt": "p"}, model="claude-haiku-4-5")
    with attempt(store, key, 2):
        b = await journaled_call(sdk_call, request={"prompt": "p"}, model="claude-haiku-4-5")
    assert a == b == {"text": "answer 1", "usage": [10, 20]} and calls == 1
    await store.close()
