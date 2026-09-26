"""7.1 Activity reset: the attempt counter goes back to 1 mid-run.

A node's attempts 1, 2 and 3 each write their row and then fail (a retryable error right after
the ledger write, as if the completion report were lost). While the Activity waits for its
retry, `temporal activity reset` restarts it: the next attempt is attempt 1 again. That attempt
is the one Temporal accepts, so its write must replace attempt 3's. A fence on the attempt number
alone would reject it; the schedule-time fence must not.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import psycopg
from temporalio.common import RetryPolicy

from bench.adversarial.common import AttackResult, stepledger_worker
from bench.common import RunConfig, Shape, dsn, start
from stepledger.ledger.history import history_nodes
from stepledger.testing import faults
from stepledger.testing.history import check_stepledger

SHAPE = Shape(nodes=10, parallel_fanout=0, effects=False)
NODE = "enrich_cve_2"
RUNS = 5


async def _wait_for_attempt(wid: str, seq_node: str, attempt: int) -> int:
    for _ in range(200):
        with psycopg.connect(dsn()) as conn:
            row = conn.execute(
                "SELECT n.seq, n.activity_id FROM sl_nodes n WHERE n.workflow_id = %s"
                " AND n.node = %s AND n.attempt = %s",
                (wid, seq_node, attempt),
            ).fetchone()
        if row:
            return int(row[0])
        await asyncio.sleep(0.1)
    raise TimeoutError(f"{wid}: attempt {attempt} of {seq_node} never wrote")


async def run() -> AttackResult:
    res = AttackResult(
        "7.1", "Activity reset", "0 divergent rows; the accepted attempt 1 replaces attempt 3"
    )
    wids = [f"atk-reset-{uuid.uuid4().hex[:6]}-{i}" for i in range(RUNS)]
    plan = [
        faults.FaultSpec("F3", wf=w, node=NODE, attempt=a, action="raise")
        for w in wids
        for a in (1, 2, 3)
    ]
    retry = RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=4.0)
    cfg = RunConfig(shape=SHAPE, kb_per_node=1, vary_per_attempt=True)
    counters = {
        "runs": RUNS,
        "resets": 0,
        "divergent": 0,
        "lost": 0,
        "orphan": 0,
        "accepted_attempt_after_reset": [],
        "rows_replaced_by_lower_attempt": 0,
        "attempt_only_fence_would_reject_accepted": 0,
    }
    async with stepledger_worker([SHAPE], plan=plan, retry=retry) as (client, tq):
        for wid in wids:
            h = await start(client, tq, cfg, workflow_id=wid)
            await _wait_for_attempt(wid, NODE, 3)  # attempt 3 wrote, then failed
            act = next(
                n
                for n in await history_nodes(client, wid, h.first_execution_run_id or "")
                if n.activity_type.endswith(NODE)
            )
            proc = await asyncio.create_subprocess_exec(
                "temporal",
                "activity",
                "reset",
                "--workflow-id",
                wid,
                "--activity-id",
                act.activity_id,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _, err = await proc.communicate()
            if proc.returncode != 0:
                raise RuntimeError(f"activity reset failed: {err.decode().strip()}")
            counters["resets"] += 1
            await asyncio.wait_for(h.result(), 120)
            desc = await h.describe()
            c = await check_stepledger(client, dsn(), wid, desc.run_id)
            counters["divergent"] += c.divergent_rows
            counters["lost"] += c.lost_rows
            counters["orphan"] += c.orphan_rows
            with psycopg.connect(dsn()) as conn:
                row = conn.execute(
                    "SELECT attempt FROM sl_nodes WHERE workflow_id = %s AND node = %s",
                    (wid, NODE),
                ).fetchone()
                writes = conn.execute(
                    "SELECT a.attempt FROM sl_node_attempts a JOIN sl_nodes n USING"
                    " (namespace, workflow_id, run_id, seq) WHERE a.workflow_id = %s"
                    " AND n.node = %s AND a.outcome = 'WROTE' ORDER BY a.id",
                    (wid, NODE),
                ).fetchall()
            accepted = int(row[0]) if row else -1
            counters["accepted_attempt_after_reset"].append(accepted)
            attempts = [w[0] for w in writes]
            if len(attempts) >= 2 and attempts[-1] < max(attempts[:-1]):
                counters["rows_replaced_by_lower_attempt"] += 1
                counters["attempt_only_fence_would_reject_accepted"] += 1
    res.measured = counters
    res.rate = f"{counters['divergent']} divergent rows in {RUNS} reset runs"
    res.holds = counters["divergent"] == 0 and counters["lost"] == 0
    return res
