"""Phase 4.6: the External Storage threshold for B4 at 30 nodes x 60 KiB.

    uv run python -m bench.threshold_sweep

Runs B4 with payload_size_threshold 16, 64 and 256 KiB (three runs each) and records history
bytes, largest payload, externalized payloads, unique chunk bytes, wall clock and ledger write
latency. The default stays 64 KiB unless 256 KiB is better on every metric (build spec, section 5).
"""

from __future__ import annotations

import asyncio
import shlex
import statistics
import sys
import time
from dataclasses import replace
from typing import Any

from bench.baselines import BASELINES, client_plugins
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
from bench.ledger_stats import run_stats
from bench.metrics import collect

THRESHOLDS_KIB = [16, 64, 256]


async def one(threshold_kib: int, nodes: int, kb: int) -> dict[str, Any]:
    base = BASELINES["B4"]
    b = replace(
        base, stepledger={**(base.stepledger or {}), "payload_size_threshold": threshold_kib * 1024}
    )
    cfg = b.run_config(RunConfig(shape=Shape(nodes=nodes), kb_per_node=kb))
    lg = langgraph_plugin([cfg.shape])
    client = await connect(client_plugins(b, lg, dsn()))
    async with running_worker(client, lg) as tq:
        t0 = time.perf_counter()
        h = await start(client, tq, cfg)
        await h.result()
        wall = time.perf_counter() - t0
    m = await collect(h)
    stats = await run_stats(dsn(), m.workflow_id, m.run_id)
    return {
        "threshold_kib": threshold_kib,
        "history_size_bytes": m.history_size_bytes,
        "largest_payload_bytes": m.largest_payload_bytes,
        "wall_clock_s": round(wall, 3),
        **{
            k: stats[k]
            for k in (
                "externalized_payloads",
                "store_unique_chunk_bytes",
                "store_whole_blob_bytes",
                "ledger_write_ms_p50",
                "ledger_write_ms_p95",
            )
        },
    }


async def main(argv: list[str]) -> None:
    nodes, kb, repeats = 30, 60, 3
    rows = []
    for _ in range(repeats):
        for t in THRESHOLDS_KIB:
            rows.append(await one(t, nodes, kb))
    summary = {}
    for t in THRESHOLDS_KIB:
        rs = [r for r in rows if r["threshold_kib"] == t]
        summary[str(t)] = {
            k: statistics.median(r[k] for r in rs)
            for k in rs[0]
            if k != "threshold_kib" and rs[0][k] is not None
        }
        print(t, summary[str(t)])
    path = write_results(
        "threshold",
        "uv run python -m bench.threshold_sweep " + shlex.join(argv),
        {
            "nodes": nodes,
            "kb_per_node": kb,
            "repeats": repeats,
            "median_by_threshold_kib": summary,
            "runs": rows,
        },
    )
    print(f"wrote {path}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
