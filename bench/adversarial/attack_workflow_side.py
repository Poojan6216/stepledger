"""7.3 Workflow-side nodes: a node with execute_in="workflow" runs in workflow code, which cannot
do I/O, so it gets no ledger row. materialize() must report GAP, never a wrong EXACT."""

from __future__ import annotations

import asyncio
import uuid

from bench.adversarial.common import AttackResult, stepledger_worker
from bench.common import RunConfig, Shape, dsn, start
from stepledger.canonical import canonical_json
from stepledger.read.materialize import materialize

RUNS = 10
SHAPES = [
    Shape(nodes=12, parallel_fanout=2, effects=False, execute_in=(("enrich_cve_0", "workflow"),)),
    Shape(
        nodes=16,
        parallel_fanout=0,
        effects=True,
        execute_in=(("enrich_cve_1", "workflow"), ("enrich_cve_3", "workflow")),
    ),
]


async def run() -> AttackResult:
    res = AttackResult("7.3", "Workflow-side nodes", "GAP every time; 0 wrong EXACT")
    counts = {
        "runs": RUNS,
        "gap": 0,
        "exact": 0,
        "wrong_exact": 0,
        "open": 0,
        "gap_positions_named": 0,
    }
    async with stepledger_worker(SHAPES) as (client, tq):
        for i in range(RUNS):
            shape = SHAPES[i % len(SHAPES)]
            cfg = RunConfig(shape=shape, seed=700 + i, kb_per_node=1, effects_mode="once")
            h = await start(client, tq, cfg, workflow_id=f"atk-wfside-{uuid.uuid4().hex[:8]}")
            result = await asyncio.wait_for(h.result(), 120)
            m = await materialize(dsn(), shape.build().compile(), h.id)
            if m.completeness == "EXACT":
                counts["exact"] += 1
                counts["wrong_exact"] += canonical_json(m.state) != canonical_json(result)
            elif m.completeness == "GAP":
                counts["gap"] += 1
                counts["gap_positions_named"] += bool(m.positions)
            else:
                counts["open"] += 1
    res.measured = counts
    res.rate = f"GAP {counts['gap']}/{RUNS}, wrong EXACT {counts['wrong_exact']}"
    res.holds = counts["wrong_exact"] == 0 and counts["gap"] == RUNS
    return res
