"""7.6 An effect whose tool ignores the idempotency key; a crash between the call and its record.

The worker fails right after the ticket was opened, before the effect is recorded DONE (fault
point FE inside once(); for the no-once() baseline, F2 right after the call). Three setups:

    direct         no once(): the retry opens a second ticket
    once, blind    once() with no way to ask the tool: the effect is UNKNOWN and the node waits
                   for a person (the bench acts as the operator: it checks the target and runs
                   `resolve --outcome done`)
    once, asks     once() with a reconcile callback that looks the key up in the target

Measures duplicate tickets reaching the target, and how many needed a person.
"""

from __future__ import annotations

import asyncio
import uuid

import psycopg

from bench.adversarial.common import AttackResult, stepledger_worker
from bench.agents import sinks
from bench.common import RunConfig, Shape, dsn, start
from stepledger.effects.once import resolve
from stepledger.ledger.store import LedgerStore
from stepledger.testing import faults

SHAPE = Shape(nodes=12, parallel_fanout=0, effects=True)
RUNS = 3


async def _operator(wids: list[str], stop: asyncio.Event, resolved: list[str]) -> None:
    """Acts as the person on call: resolves UNKNOWN effects after checking the target."""
    store = LedgerStore(dsn())
    try:
        while not stop.is_set():
            with psycopg.connect(dsn()) as conn:
                rows = conn.execute(
                    "SELECT key, name FROM sl_effects WHERE workflow_id = ANY(%s)"
                    " AND status = 'UNKNOWN'",
                    (wids,),
                ).fetchall()
            for key, name in rows:
                receipt = await sinks.lookup(name, key)  # the person looks it up by hand
                await resolve(store, key, "done", receipt)
                resolved.append(key)
            await asyncio.sleep(0.5)
    finally:
        await store.close()


async def one(mode: str) -> dict[str, object]:
    wids = [f"atk-effect-{mode}-{uuid.uuid4().hex[:6]}-{i}" for i in range(RUNS)]
    point = "F2" if mode == "direct" else "FE"
    plan = [
        faults.FaultSpec(point, wf=w, node="open_ticket" if point == "F2" else None, action="raise")
        for w in wids
    ]
    cfg = RunConfig(shape=SHAPE, kb_per_node=1, effects_mode=mode)
    stop, resolved = asyncio.Event(), []
    op = asyncio.create_task(_operator(wids, stop, resolved))
    try:
        async with stepledger_worker([SHAPE], plan=plan) as (client, tq):
            handles = [await start(client, tq, cfg, workflow_id=w) for w in wids]
            await asyncio.gather(*(asyncio.wait_for(h.result(), 180) for h in handles))
    finally:
        stop.set()
        await op
    with psycopg.connect(dsn()) as conn:
        calls = conn.execute(
            "SELECT count(*) FROM bench_effects WHERE workflow_id = ANY(%s)"
            " AND effect = 'open_ticket'",
            (wids,),
        ).fetchone()
    tickets = int(calls[0]) if calls else 0
    return {
        "mode": mode,
        "runs": RUNS,
        "tickets_opened": tickets,
        "duplicate_tickets": tickets - RUNS,
        "resolved_by_a_person": len(resolved),
    }


async def run() -> AttackResult:
    await sinks.pool()
    res = AttackResult(
        "7.6",
        "Effect whose tool ignores the key; crash before its record",
        "duplicates only without once(); blind once() stops for a person",
    )
    rows = [await one(m) for m in ("direct", "once_blind", "once")]
    res.measured = {r["mode"]: r for r in rows}  # type: ignore[misc]
    res.rate = ", ".join(
        f"{r['mode']}: {r['duplicate_tickets']} duplicate tickets"
        f" / {r['resolved_by_a_person']} needed a person"
        for r in rows
    )
    # once() stops the silent duplicate, but only a queryable tool (or a person) settles it
    res.holds = False
    return res
