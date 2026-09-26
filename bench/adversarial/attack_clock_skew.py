"""7.2 Clock skew: two workers whose clocks are 5 minutes fast and 5 minutes slow.

Both poll one task queue; seeded F3 crashes (the worker dies after its ledger write) push retries
from one worker to the other. The fence uses the server's schedule time, so skew must not matter.
To show what a worker-clock fence would have done, every write records the worker's own clock
(sl_node_attempts.worker_time): an inversion is a retried node whose accepted write carries an
earlier worker time than a stale write it replaced.
"""

from __future__ import annotations

import asyncio
import random
import uuid

import psycopg

from bench.adversarial.common import WORK, AttackResult
from bench.chaos import SHAPE, STEP_NODES, SupervisedWorker
from bench.common import RunConfig, connect, dsn, start
from stepledger.testing import faults
from stepledger.testing.history import check_stepledger

RUNS = 8
SKEW_S = 300


async def run() -> AttackResult:
    res = AttackResult("7.2", "Worker clock skew (+/-5 min)", "0 divergent rows")
    WORK.mkdir(parents=True, exist_ok=True)
    session = uuid.uuid4().hex[:6]
    rnd = random.Random(72)
    wids = [f"atk-skew-{session}-{i}" for i in range(RUNS)]
    plan = [faults.FaultSpec("F3", wf=w, node=n) for w in wids for n in rnd.sample(STEP_NODES, 2)]
    plan_file = WORK / f"plan-skew-{session}.txt"
    plan_file.write_text("\n".join(faults.format_spec(p) for p in plan))
    env = {
        "STEPLEDGER_FAULTS": f"@{plan_file}",
        "STEPLEDGER_FAULT_LOG": str(WORK / f"faults-skew-{session}.log"),
    }
    tq = f"atk-skew-{session}"
    fast = SupervisedWorker("SL", tq, env, ["--clock-skew-s", str(SKEW_S)])
    slow = SupervisedWorker("SL", tq, env, ["--clock-skew-s", str(-SKEW_S)])
    await fast.start()
    await slow.start()
    client = await connect()
    cfg = RunConfig(shape=SHAPE, kb_per_node=1, vary_per_attempt=True, effects_mode="once")
    try:
        handles = [await start(client, tq, cfg, workflow_id=w) for w in wids]
        await asyncio.gather(*(asyncio.wait_for(h.result(), 300) for h in handles))
    finally:
        await fast.stop()
        await slow.stop()

    divergent = lost = 0
    for w in wids:
        desc = await client.get_workflow_handle(w).describe()
        c = await check_stepledger(client, dsn(), w, desc.run_id)
        divergent += c.divergent_rows
        lost += c.lost_rows
    with psycopg.connect(dsn()) as conn:
        retried = conn.execute(
            "SELECT a.workflow_id, a.seq, a.attempt, a.worker_time, a.worker,"
            " (a.fence_scheduled_at, a.attempt) = (n.fence_scheduled_at, n.attempt) AS accepted"
            " FROM sl_node_attempts a JOIN sl_nodes n USING (namespace, workflow_id, run_id, seq)"
            " WHERE a.workflow_id = ANY(%s) AND a.outcome = 'WROTE' AND (a.workflow_id, a.seq) IN"
            " (SELECT workflow_id, seq FROM sl_node_attempts WHERE workflow_id = ANY(%s)"
            "  AND outcome = 'WROTE' GROUP BY 1, 2 HAVING count(*) > 1)",
            (wids, wids),
        ).fetchall()
    by_node: dict[tuple[str, int], list[tuple[object, ...]]] = {}
    for r in retried:
        by_node.setdefault((r[0], r[1]), []).append(r)
    cross_worker = inversions = 0
    for rows in by_node.values():
        workers = {r[4] for r in rows}
        cross_worker += len(workers) > 1
        acc = [r for r in rows if r[5]]
        if acc and any(r[3] > acc[0][3] for r in rows if not r[5]):  # type: ignore[operator]
            inversions += 1
    res.measured = {
        "runs": RUNS,
        "f3_crashes_planned": len(plan),
        "worker_restarts": fast.restarts + slow.restarts,
        "retried_nodes": len(by_node),
        "retried_across_worker_processes": cross_worker,
        "divergent": divergent,
        "lost": lost,
        "worker_clock_fence_would_keep_stale_write": inversions,
    }
    res.rate = (
        f"{divergent} divergent rows; a worker-clock fence would keep the stale write for"
        f" {inversions} of {len(by_node)} retried nodes"
    )
    res.holds = divergent == 0 and lost == 0
    return res
