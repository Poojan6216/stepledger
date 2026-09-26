"""6.1: once() never silently repeats an effect.

The state machine against real Postgres, one simulated Activity attempt at a time:
a retry after DONE returns the journaled result; a different request under the same identity is
refused; an effect STARTED by a crashed attempt goes to the tool's reconcile callback, or waits
for `stepledger resolve`, and is never simply called again.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

import psycopg
import pytest

from stepledger.canonical import chash
from stepledger.effects.once import NOT_DONE, once, resolve
from stepledger.errors import EffectDivergence, UnknownEffectOutcome
from stepledger.keys import Fence, LedgerKey
from stepledger.ledger.context import NodeContext, _current
from stepledger.ledger.store import LedgerStore

pytestmark = pytest.mark.integration

T0 = datetime(2026, 9, 26, tzinfo=UTC)


@contextmanager
def attempt(store: LedgerStore, key: LedgerKey, n: int) -> Iterator[None]:
    token = _current.set(
        NodeContext(key=key, fence=Fence(T0 + timedelta(seconds=n), n), store=store)
    )
    try:
        yield
    finally:
        _current.reset(token)


class Sink:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def open_ticket(self, key: str) -> str:
        self.calls.append(key)
        return f"TCK-{len(self.calls)}"


@pytest.fixture
def setup(dsn: str) -> tuple[LedgerStore, LedgerKey]:
    return LedgerStore(dsn), LedgerKey("default", f"once-{uuid.uuid4().hex[:8]}", "run", 27)


async def test_retry_after_done_returns_the_journaled_result(
    setup: tuple[LedgerStore, LedgerKey],
) -> None:
    store, key = setup
    sink = Sink()
    req = {"target": "acct", "risk": 71}
    with attempt(store, key, 1):
        first = await once("open_ticket", sink.open_ticket, request=req)
    # the worker died after the effect (F2/F3); Temporal retries the node
    with attempt(store, key, 2):
        second = await once("open_ticket", sink.open_ticket, request=req)
    assert first == second == "TCK-1" and len(sink.calls) == 1
    assert sink.calls[0] == key.effect_key("open_ticket", 0)  # the key went upstream
    with psycopg.connect(store.dsn) as conn:
        row = conn.execute(
            "SELECT status, duplicates_prevented FROM sl_effects WHERE workflow_id = %s",
            (key.workflow_id,),
        ).fetchone()
    assert row == ("DONE", 1)
    await store.close()


async def test_different_request_under_the_same_key_is_refused(
    setup: tuple[LedgerStore, LedgerKey],
) -> None:
    store, key = setup
    sink = Sink()
    with attempt(store, key, 1):
        await once("open_ticket", sink.open_ticket, request={"risk": 71})
    with attempt(store, key, 2), pytest.raises(EffectDivergence):
        await once("open_ticket", sink.open_ticket, request={"risk": 12})
    assert len(sink.calls) == 1
    await store.close()


async def _crashed_mid_effect(store: LedgerStore, key: LedgerKey) -> None:
    """Attempt 1 recorded STARTED, then died before it could record DONE."""
    ek = key.effect_key("open_ticket", 0)
    with psycopg.connect(store.dsn) as conn:
        conn.execute(
            "INSERT INTO sl_effects (key, namespace, workflow_id, run_id, seq, name, idx,"
            " request_hash, status) VALUES (%s, %s, %s, %s, %s, 'open_ticket', 0, %s, 'STARTED')",
            (ek, key.namespace, key.workflow_id, key.run_id, key.seq, chash({"risk": 71})),
        )


async def test_unknown_outcome_without_reconcile_waits_for_a_person(
    setup: tuple[LedgerStore, LedgerKey],
) -> None:
    store, key = setup
    sink = Sink()
    await _crashed_mid_effect(store, key)
    with attempt(store, key, 2), pytest.raises(UnknownEffectOutcome) as err:
        await once("open_ticket", sink.open_ticket, request={"risk": 71})
    assert sink.calls == []  # never a silent second call
    assert not err.value.non_retryable and err.value.next_retry_delay is not None
    ek = key.effect_key("open_ticket", 0)
    assert await resolve(store, ek, "done", "TCK-9") == "DONE"
    with attempt(store, key, 3):
        assert await once("open_ticket", sink.open_ticket, request={"risk": 71}) == "TCK-9"
    assert sink.calls == []
    await store.close()


async def test_resolved_not_done_calls_the_tool_once_more(
    setup: tuple[LedgerStore, LedgerKey],
) -> None:
    store, key = setup
    sink = Sink()
    await _crashed_mid_effect(store, key)
    await resolve(store, key.effect_key("open_ticket", 0), "not-done")
    with attempt(store, key, 2):
        assert await once("open_ticket", sink.open_ticket, request={"risk": 71}) == "TCK-1"
    assert len(sink.calls) == 1
    await store.close()


@pytest.mark.parametrize("answer", ["found", "not_done"])
async def test_unknown_outcome_asks_the_tool(
    setup: tuple[LedgerStore, LedgerKey], answer: str
) -> None:
    store, key = setup
    sink = Sink()
    await _crashed_mid_effect(store, key)

    async def reconcile(k: str) -> object:
        return "TCK-found" if answer == "found" else NOT_DONE

    with attempt(store, key, 2):
        got = await once("open_ticket", sink.open_ticket, request={"risk": 71}, reconcile=reconcile)
    if answer == "found":
        assert got == "TCK-found" and sink.calls == []
    else:
        assert got == "TCK-1" and len(sink.calls) == 1
    await store.close()
