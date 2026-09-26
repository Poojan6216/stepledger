"""2.2: the fenced upsert against real Postgres.

Random interleavings of writes (random fences), commits and abandons must always leave the row
holding the output of the highest fence written before it became final; final rows never change;
and the FENCED_OUT audit count equals the number of rejected writes.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from stepledger.keys import Fence, LedgerKey
from stepledger.ledger.store import LedgerStore, NodeWrite

pytestmark = pytest.mark.integration

T0 = datetime(2026, 9, 26, 12, 0, tzinfo=UTC)

write = st.tuples(
    st.just("w"), st.integers(0, 5), st.integers(1, 4), st.integers(0, 10**6)
)  # (op, schedule offset s, attempt, output tag)
final = st.sampled_from([("c",), ("a",)])
ops_strategy = st.lists(st.one_of(write, write, write, final), min_size=1, max_size=12)


@pytest.fixture(scope="module")
def env(dsn: str) -> Iterator[tuple[asyncio.AbstractEventLoop, LedgerStore]]:
    loop = asyncio.new_event_loop()
    store = LedgerStore(dsn, max_size=4)
    loop.run_until_complete(store.open())
    yield loop, store
    loop.run_until_complete(store.close())
    loop.close()


def _write(tag: int, fence: Fence) -> NodeWrite:
    return NodeWrite(
        activity_id="1",
        activity_type="g.n",
        kind="UPDATE",
        output_json={"tag": tag},
        output_hash=str(tag),
        started_at=fence.scheduled_at,
        finished_at=fence.scheduled_at,
    )


async def _apply(store: LedgerStore, key: LedgerKey, op: tuple[object, ...]) -> bool | None:
    async with store.tx() as tx:
        if op[0] == "w":
            _, off, attempt, tag = op
            fence = Fence(T0 + timedelta(seconds=int(off)), int(attempt))  # type: ignore[call-overload]
            r = await tx.fenced_upsert(key, fence, _write(int(tag), fence))  # type: ignore[call-overload]
            await tx.audit_attempt(key, fence, "WROTE" if r.wrote else "FENCED_OUT")
            return r.wrote
        if op[0] == "c":
            await tx.commit(key.run, [key.seq])
        else:
            await tx.abandon(key.run, [key.seq])
    return None


async def _row(store: LedgerStore, key: LedgerKey) -> tuple[str, int] | None:
    async with store.read() as conn:
        cur = await conn.execute(
            "SELECT status, (output_json->>'tag')::int FROM sl_nodes"
            " WHERE namespace = %s AND workflow_id = %s AND run_id = %s AND seq = %s",
            (key.namespace, key.workflow_id, key.run_id, key.seq),
        )
        row = await cur.fetchone()
    return (row[0], row[1]) if row else None


async def _fenced_out(store: LedgerStore, key: LedgerKey) -> int:
    async with store.read() as conn:
        cur = await conn.execute(
            "SELECT count(*) FROM sl_node_attempts WHERE namespace = %s AND workflow_id = %s"
            " AND run_id = %s AND seq = %s AND outcome = 'FENCED_OUT'",
            (key.namespace, key.workflow_id, key.run_id, key.seq),
        )
        row = await cur.fetchone()
    return int(row[0]) if row else 0


@settings(
    max_examples=300, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture]
)
@given(ops=ops_strategy)
def test_interleavings_match_the_model(
    env: tuple[asyncio.AbstractEventLoop, LedgerStore], ops: list[tuple[object, ...]]
) -> None:
    loop, store = env
    key = LedgerKey("default", "prop-" + uuid.uuid4().hex[:8], uuid.uuid4().hex, 0)

    model_fence: tuple[datetime, int] | None = None
    model_out: int | None = None
    model_status: str | None = None
    rejected = 0

    for op in ops:
        wrote = loop.run_until_complete(_apply(store, key, op))
        if op[0] == "w":
            _, off, attempt, tag = op
            fence = (T0 + timedelta(seconds=int(off)), int(attempt))  # type: ignore[call-overload]
            accept = model_status is None or (
                model_status == "PROVISIONAL" and model_fence is not None and model_fence <= fence
            )
            assert wrote == accept
            if accept:
                model_fence, model_out, model_status = fence, int(tag), "PROVISIONAL"  # type: ignore[call-overload]
            else:
                rejected += 1
        elif model_status == "PROVISIONAL":
            model_status = "COMMITTED" if op[0] == "c" else "ABANDONED"

        got = loop.run_until_complete(_row(store, key))
        expected = None if model_status is None else (model_status, model_out)
        assert got == expected

    assert loop.run_until_complete(_fenced_out(store, key)) == rejected


def test_concurrent_writers_leave_the_highest_fence(
    env: tuple[asyncio.AbstractEventLoop, LedgerStore],
) -> None:
    """Many attempts racing on one row: the highest fence wins regardless of arrival order."""
    loop, store = env
    key = LedgerKey("default", "race-" + uuid.uuid4().hex[:8], uuid.uuid4().hex, 3)
    offsets = list(range(20))

    async def one(off: int) -> None:
        fence = Fence(T0 + timedelta(seconds=off), 1)
        async with store.tx() as tx:
            r = await tx.fenced_upsert(key, fence, _write(off, fence))
            await tx.audit_attempt(key, fence, "WROTE" if r.wrote else "FENCED_OUT")

    async def race() -> None:
        import random

        random.Random(1).shuffle(offsets)
        await asyncio.gather(*(one(o) for o in offsets))

    loop.run_until_complete(race())
    assert loop.run_until_complete(_row(store, key)) == ("PROVISIONAL", 19)
