"""Phase 3.5: stop the ledger's Postgres for 20 s in the middle of a run.

    uv run python -m bench.outage

A 20-node run at 1.5 s per node, Postgres stopped from t=5 s to t=25 s (scripts/dev.sh pg-down /
pg-up; Temporal's dev server keeps running, it does not use Postgres).

    fail mode (default): ledger writes fail, the node Activities fail and Temporal retries them;
                         the run stalls, then completes once Postgres returns, invariants at 0.
    warn mode:           nodes complete without their rows; the seal marks the run DEGRADED and
                         lists the seqs with no row; `reconcile` then repairs them from history.
"""

from __future__ import annotations

import asyncio
import shlex
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

import psycopg
from temporalio.common import RetryPolicy

from bench.common import (
    RunConfig,
    Shape,
    connect,
    dsn,
    langgraph_plugin,
    running_worker,
    start,
    write_results,
)
from stepledger import StepledgerPlugin
from stepledger.ledger.history import history_nodes
from stepledger.ledger.reconcile import reconcile
from stepledger.ledger.store import LedgerStore
from stepledger.testing.history import check_stepledger

ROOT = Path(__file__).resolve().parent.parent
SHAPE = Shape(nodes=20, parallel_fanout=0, effects=False)
OUTAGE_START_S, OUTAGE_S = 5.0, 20.0


async def pg(action: str) -> None:
    proc = await asyncio.create_subprocess_exec(
        str(ROOT / "scripts" / "dev.sh"),
        f"pg-{action}",
        cwd=ROOT,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    if await proc.wait() != 0:
        raise RuntimeError(f"scripts/dev.sh pg-{action} failed")


async def scenario(mode: str) -> dict[str, Any]:
    cfg = RunConfig(shape=SHAPE, kb_per_node=1, node_delay_ms=1500)
    lg = langgraph_plugin(
        [SHAPE],
        start_to_close=timedelta(seconds=30),
        retry_policy=RetryPolicy(
            initial_interval=timedelta(milliseconds=500), maximum_interval=timedelta(seconds=2)
        ),
    )
    sl = StepledgerPlugin(dsn(), langgraph=lg, external_storage=False, on_ledger_error=mode)
    client = await connect([sl])
    async with running_worker(client, lg) as tq:
        t0 = time.monotonic()
        h = await start(client, tq, cfg, workflow_id=f"outage-{mode}-{int(time.time())}")
        await asyncio.sleep(OUTAGE_START_S)
        await pg("down")
        down_at = time.monotonic() - t0
        await asyncio.sleep(OUTAGE_S)
        await pg("up")
        up_at = time.monotonic() - t0
        await asyncio.wait_for(h.result(), timeout=300)
        wall = time.monotonic() - t0
    desc = await h.describe()
    nodes = await history_nodes(client, h.id, desc.run_id)
    retried = sorted(n.seq for n in nodes if n.seq is not None and (n.attempt or 1) > 1)
    with psycopg.connect(dsn()) as conn:
        run = conn.execute(
            "SELECT status, degraded, missing_seqs, committed_count FROM sl_runs"
            " WHERE workflow_id = %s",
            (h.id,),
        ).fetchone()
    before = await check_stepledger(client, dsn(), h.id, desc.run_id)
    out: dict[str, Any] = {
        "mode": mode,
        "workflow_id": h.id,
        "outage_window_s": [round(down_at, 1), round(up_at, 1)],
        "run_wall_clock_s": round(wall, 1),
        "workflow_status": desc.status.name if desc.status else None,
        "node_activities": len([n for n in nodes if n.seq is not None]),
        "seqs_retried_by_temporal": retried,
        "ledger_status": run[0] if run else None,
        "degraded": bool(run and run[1]),
        "missing_seqs": list(run[2] or []) if run else None,
        "committed_rows_before_reconcile": run[3] if run else None,
        "counters_before_reconcile": before.as_dict(),
    }
    if out["degraded"]:
        store = LedgerStore(dsn())
        (report,) = await reconcile(client, store, h.id)
        await store.close()
        after = await check_stepledger(client, dsn(), h.id, desc.run_id)
        out["reconcile"] = {
            "inserted": report.inserted,
            "committed": report.committed,
            "divergence_repaired": report.divergence_repaired,
        }
        out["counters_after_reconcile"] = after.as_dict()
    return out


async def main(argv: list[str]) -> None:
    results = []
    for mode in ("fail", "warn"):
        r = await scenario(mode)
        print({k: v for k, v in r.items() if not k.startswith("counters")}, flush=True)
        print("  counters:", r["counters_before_reconcile"], r.get("counters_after_reconcile", ""))
        results.append(r)
    path = write_results(
        "outage",
        "uv run python -m bench.outage " + shlex.join(argv),
        {"nodes": SHAPE.nodes, "node_delay_ms": 1500, "outage_s": OUTAGE_S, "scenarios": results},
    )
    print(f"wrote {path}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
