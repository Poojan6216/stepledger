"""Phase 2.7: ledger write latency and wall-clock overhead on the fake-LLM agent.

    uv run python -m bench.overhead --runs 10

Runs the 30-node investigator (1 KiB per node) `--runs` times without Stepledger and with it
(ledger only, no External Storage), alternating to spread noise. Reports per-node ledger write
latency (p50/p95 of sl_node_attempts.write_ms: the upsert, commits, abandons and audit insert in
one transaction, excluding the final COMMIT round trip) and per-run wall clock.
"""

from __future__ import annotations

import argparse
import asyncio
import shlex
import statistics
import sys
import time
from typing import Any

import psycopg

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
from bench.ledger_stats import percentile
from bench.metrics import collect
from stepledger import StepledgerPlugin


async def one_run(with_plugin: bool, cfg: RunConfig) -> dict[str, Any]:
    lg = langgraph_plugin([cfg.shape])
    plugins = [StepledgerPlugin(dsn(), langgraph=lg, external_storage=False)] if with_plugin else []
    client = await connect(plugins)
    async with running_worker(client, lg) as tq:
        t0 = time.perf_counter()
        h = await start(client, tq, cfg)
        await h.result()
        client_wall = time.perf_counter() - t0
    m = await collect(h)
    return {
        "with_plugin": with_plugin,
        "workflow_id": h.id,
        "run_id": m.run_id,
        "client_wall_s": round(client_wall, 4),
        "server_wall_s": m.wall_clock_s,
        "history_size_bytes": m.history_size_bytes,
        "history_events": m.history_events,
    }


async def main(argv: list[str]) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--nodes", type=int, default=30)
    ap.add_argument("--kb", type=int, default=1)
    args = ap.parse_args(argv)
    cfg = RunConfig(shape=Shape(nodes=args.nodes), kb_per_node=args.kb)

    await one_run(False, cfg)  # warm-up: imports, sandbox, pools
    await one_run(True, cfg)
    runs = []
    for i in range(args.runs):
        for with_plugin in (False, True) if i % 2 == 0 else (True, False):
            runs.append(await one_run(with_plugin, cfg))
    ids = [r["workflow_id"] for r in runs if r["with_plugin"]]
    with psycopg.connect(dsn()) as conn:
        writes = [
            float(w)
            for (w,) in conn.execute(
                "SELECT write_ms FROM sl_node_attempts WHERE workflow_id = ANY(%s)", (ids,)
            ).fetchall()
        ]

    def summary(with_plugin: bool, key: str) -> dict[str, float]:
        vals = [r[key] for r in runs if r["with_plugin"] == with_plugin]
        return {
            "median": round(statistics.median(vals), 4),
            "mean": round(statistics.mean(vals), 4),
        }

    base, plug = summary(False, "server_wall_s"), summary(True, "server_wall_s")
    out = {
        "nodes": args.nodes,
        "kb_per_node": args.kb,
        "runs_per_config": args.runs,
        "ledger_writes": len(writes),
        "ledger_write_ms_p50": percentile(writes, 50),
        "ledger_write_ms_p95": percentile(writes, 95),
        "ledger_write_ms_max": round(max(writes), 3),
        "wall_s_without_plugin": base,
        "wall_s_with_plugin": plug,
        "wall_overhead_median_s": round(plug["median"] - base["median"], 4),
        "wall_overhead_per_node_ms": round(
            (plug["median"] - base["median"]) / args.nodes * 1000, 3
        ),
        "history_bytes_without_plugin": summary(False, "history_size_bytes"),
        "history_bytes_with_plugin": summary(True, "history_size_bytes"),
        "runs": runs,
    }
    for k in (
        "ledger_write_ms_p50",
        "ledger_write_ms_p95",
        "wall_s_without_plugin",
        "wall_s_with_plugin",
        "wall_overhead_per_node_ms",
    ):
        print(f"{k}: {out[k]}")
    path = write_results("overhead", "uv run python -m bench.overhead " + shlex.join(argv), out)
    print(f"wrote {path}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
