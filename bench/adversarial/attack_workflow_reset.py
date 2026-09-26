"""7.8 Workflow reset: `temporal workflow reset` starts a new run, and a new run ID means new
effect keys, so once() cannot recognise the effects the first run already performed. Measures the
duplicate effects that reach the target, and what a target that dedupes on its own business key
would have kept.
"""

from __future__ import annotations

import asyncio
import uuid

import psycopg

from bench.adversarial.common import AttackResult, stepledger_worker
from bench.agents import sinks
from bench.common import RunConfig, Shape, dsn, start

SHAPE = Shape(nodes=12, parallel_fanout=0, effects=True)
RUNS = 3


async def run() -> AttackResult:
    await sinks.pool()
    res = AttackResult(
        "7.8", "Workflow reset (new run ID)", "effects repeat unless the tool's own key dedupes"
    )
    wids = [f"atk-wfreset-{uuid.uuid4().hex[:6]}-{i}" for i in range(RUNS)]
    cfg = RunConfig(shape=SHAPE, kb_per_node=1, effects_mode="once")
    async with stepledger_worker([SHAPE]) as (client, tq):
        for w in wids:
            h = await start(client, tq, cfg, workflow_id=w)
            await asyncio.wait_for(h.result(), 120)
            proc = await asyncio.create_subprocess_exec(
                "temporal",
                "workflow",
                "reset",
                "--workflow-id",
                w,
                "--type",
                "FirstWorkflowTask",
                "--reason",
                "attack 7.8",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, err = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(f"workflow reset failed: {err.decode().strip()}")
            new = client.get_workflow_handle(w)  # latest run
            await asyncio.wait_for(new.result(), 120)
    with psycopg.connect(dsn()) as conn:
        calls, distinct_business, runs = conn.execute(
            "SELECT count(*), count(DISTINCT (workflow_id, effect, request_hash)),"
            " count(DISTINCT run_id) FROM bench_effects WHERE workflow_id = ANY(%s)",
            (wids,),
        ).fetchone() or (0, 0, 0)
    res.measured = {
        "workflows": RUNS,
        "runs_after_reset": runs,
        "effect_calls": calls,
        "duplicate_effect_calls": calls - distinct_business,
        "calls_a_business_key_dedupe_would_keep": distinct_business,
    }
    res.rate = f"{calls - distinct_business} duplicate effects across {RUNS} resets"
    res.holds = False  # documented limitation: once() keys include the run ID
    return res
