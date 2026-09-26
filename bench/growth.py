"""Demo 3, the quiet quadratic: history and storage growth across node counts and output sizes.

    uv run python -m bench.growth --configs B1 --out growth_problem          # Phase 1.4
    uv run python -m bench.growth --configs B2 B3 B4 --out growth             # Phase 4.6

Each run records history bytes and events, the largest payload, which wall stopped it
(a 2 MiB single payload or the 50 MB history limit), and, for Stepledger configs, the external
store bytes and ledger write latency. A run that hits the payload wall is declared STUCK as soon
as the PAYLOADS_TOO_LARGE task failure appears, then terminated.
"""

from __future__ import annotations

import argparse
import asyncio
import shlex
import sys
import time
from typing import Any

from bench.baselines import BASELINES, Baseline, client_plugins
from bench.common import (
    RunConfig,
    Shape,
    connect,
    dsn,
    langgraph_plugin,
    reset_store,
    running_worker,
    start,
    write_results,
)
from bench.metrics import collect, watch

NODES = [10, 20, 30, 40, 60, 80]
KBS = [20, 60, 100]


def which_wall(
    status: str, cause: str | None, terminated_by: str | None, reason: str | None
) -> str | None:
    if status == "COMPLETED":
        return None
    if cause == "WORKFLOW_TASK_FAILED_CAUSE_PAYLOADS_TOO_LARGE" or (
        reason and "exceeds size limit" in reason
    ):
        return "single payload 2 MB"
    if terminated_by == "history-service" and reason and "history" in reason.lower():
        return "history 50 MB"
    return f"other: {reason or cause or status}"


async def run_one(baseline: Baseline, nodes: int, kb: int, *, stuck_after: float) -> dict[str, Any]:
    cfg = baseline.run_config(RunConfig(shape=Shape(nodes=nodes), kb_per_node=kb))
    lg = langgraph_plugin([cfg.shape])
    client = await connect(client_plugins(baseline, lg, dsn()))
    t0 = time.monotonic()
    async with running_worker(client, lg) as tq:
        handle = await start(client, tq, cfg)
        o = await watch(handle, stuck_after=stuck_after, stop_on_payload_error=True)
    wall_clock = time.monotonic() - t0
    m = await collect(handle)
    node_acts = [n for n in m.nodes if not n.activity_type.startswith("bench.")]
    wall = which_wall(o.status, o.workflow_task_failed_cause, o.terminated_by, o.terminated_reason)
    row: dict[str, Any] = {
        "config": baseline.id,
        "nodes": nodes,
        "kb_per_node": kb,
        "status": o.status,
        "wall": wall,
        "stopped_at_node": None if o.status == "COMPLETED" else len(node_acts) + 1,
        "first_node_input_over_2mib": (
            len(node_acts) + 1 if wall == "single payload 2 MB" else None
        ),
        "node_activities_scheduled": len(node_acts),
        "history_size_bytes": m.history_size_bytes,
        "history_mib": round(m.history_size_bytes / 2**20, 2),
        "history_over_10mib_at_node": m.history_crossed_at_node["10MiB"],
        "history_over_50mib_at_node": m.history_crossed_at_node["50MiB"],
        "history_events": m.history_events,
        "largest_payload_bytes": m.largest_payload_bytes,
        "largest_node_input_bytes": max((n.input_bytes for n in node_acts), default=0),
        "wall_clock_s": round(wall_clock, 2),
        "terminated_by": o.terminated_by,
        "terminated_reason": o.terminated_reason,
        "workflow_id": m.workflow_id,
        "run_id": m.run_id,
    }
    if baseline.stepledger is not None:
        from bench.ledger_stats import run_stats

        row.update(await run_stats(dsn(), m.workflow_id, m.run_id))
    return row


async def main(argv: list[str]) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--configs", nargs="+", default=["B1"])
    ap.add_argument("--nodes", nargs="+", type=int, default=NODES)
    ap.add_argument("--kb", nargs="+", type=int, default=KBS)
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--out", default="growth_problem")
    args = ap.parse_args(argv)

    rows = []
    for cid in args.configs:
        if BASELINES[cid].stepledger is not None:
            await reset_store()  # this config's storage numbers are its own
        for kb in args.kb:
            for n in args.nodes:
                r = await run_one(BASELINES[cid], n, kb, stuck_after=args.timeout)
                print(
                    f"{cid} kb={kb:>3} n={n:>2}: {r['status']:<10} wall={r['wall']}"
                    f" history={r['history_mib']} MiB events={r['history_events']}"
                    f" largest={r['largest_payload_bytes']:,} wall={r['wall_clock_s']}s"
                    f" write_p50={r.get('ledger_write_ms_p50')} p95={r.get('ledger_write_ms_p95')}"
                    f" store={r.get('store_unique_chunk_bytes')}",
                    flush=True,
                )
                rows.append(r)
    path = write_results(
        args.out, "uv run python -m bench.growth " + shlex.join(argv), {"rows": rows}
    )
    print(f"wrote {path}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
