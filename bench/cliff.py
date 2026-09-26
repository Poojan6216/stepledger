"""Demo 1, the cliff: reproduce #1894 and show which configurations get past it.

    uv run python -m bench.cliff                     # B0, B1 (problem side; Phase 1)
    uv run python -m bench.cliff --configs B0 B1 B2 B3 B4

B0 runs at the largest node count whose node inputs all stay under 2 MiB while its final state
exceeds it (measured by `preflight`), the reporter's exact shape. The others run at 40 nodes.
Each configuration runs twice: with the SDK default (the worker checks payload size and fails
the workflow task: STUCK) and with `disable_payload_error_limit=True` (the server rejects the
command).
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
    running_worker,
    start,
    write_results,
)
from bench.metrics import collect, watch

MiB = 1024 * 1024
LIMIT = 2 * MiB  # limit.blobSize.error on the dev server (scripts/dev.sh)


async def preflight(kb: int, llm_bytes: int, lo: int = 20, hi: int = 60) -> int:
    """Largest node count whose node inputs stay under LIMIT while the final state exceeds it.

    Serializes each node's ActivityInput exactly as the plugin builds it, with Temporal's
    default converter, in-process (no server).
    """
    from langgraph.checkpoint.memory import InMemorySaver
    from temporalio.contrib.langgraph._activity import ActivityInput
    from temporalio.converter import DataConverter

    from bench.agents.investigator import initial_state

    conv = DataConverter.default.payload_converter
    best = None
    for n in range(lo, hi + 1):
        cfg = RunConfig(shape=Shape(nodes=n), kb_per_node=kb, llm_bytes=llm_bytes)
        app = cfg.shape.build().compile(checkpointer=InMemorySaver())
        states: list[Any] = [initial_state()]
        async for s in app.astream(
            initial_state(),
            {"configurable": {"thread_id": "1"}},
            context=cfg.context(),
            stream_mode="values",
        ):
            states.append(s)
        # langgraph_config adds ~1 KB per input; use a conservative 2 KiB stand-in.
        pad = {"pad": "x" * 2048}
        inputs = [
            conv.to_payload(ActivityInput(args=(st,), kwargs={}, langgraph_config=pad)).ByteSize()
            for st in states[:-1]
        ]
        final = conv.to_payload(states[-1]).ByteSize()
        if max(inputs) >= LIMIT:
            break
        if final > LIMIT:
            best = n
    if best is None:
        raise RuntimeError(f"no node count in {lo}..{hi} has the B0 shape at {kb} KiB")
    return best


async def run_one(
    baseline: Baseline,
    nodes: int,
    kb: int,
    llm_bytes: int,
    *,
    disable_limit: bool,
    stuck_after: float,
) -> dict[str, Any]:
    cfg = baseline.run_config(
        RunConfig(shape=Shape(nodes=nodes), kb_per_node=kb, llm_bytes=llm_bytes)
    )
    lg = langgraph_plugin([cfg.shape])
    client = await connect(client_plugins(baseline, lg, dsn()))
    t0 = time.monotonic()
    async with running_worker(client, lg, disable_payload_error_limit=disable_limit) as tq:
        handle = await start(client, tq, cfg)
        o = await watch(handle, stuck_after=stuck_after)
    m = await collect(handle)
    node_activities = [n for n in m.nodes if not n.activity_type.startswith("bench.")]
    stopped_at = None
    if o.status != "COMPLETED":
        stopped_at = (
            "persist_all" if len(node_activities) == nodes else f"node {len(node_activities) + 1}"
        )
    return {
        "config": baseline.id,
        "label": baseline.label,
        "nodes": nodes,
        "kb_per_node": kb,
        "llm_bytes": llm_bytes,
        "payload_check": "disabled" if disable_limit else "sdk_default",
        "outcome": o.status,
        "workflow_task_failed_cause": o.workflow_task_failed_cause,
        "terminated_by": o.terminated_by,
        "terminated_reason": o.terminated_reason,
        "stopped_at": stopped_at,
        "node_activities_scheduled": len(node_activities),
        "largest_payload_bytes": m.largest_payload_bytes,
        "history_size_bytes": m.history_size_bytes,
        "history_events": m.history_events,
        "elapsed_s": round(time.monotonic() - t0, 2),
        "workflow_id": m.workflow_id,
    }


def fmt(rows: list[dict[str, Any]]) -> str:
    out = [
        f"{'config':<34} {'nodes':>5}  {'SDK default':<34} {'check disabled':<34} "
        f"{'largest payload':>15} {'history bytes':>13} {'events':>6}"
    ]
    by: dict[str, dict[str, dict[str, Any]]] = {}
    for r in rows:
        by.setdefault(r["config"], {})[r["payload_check"]] = r
    for cid, modes in by.items():
        d, x = modes.get("sdk_default", {}), modes.get("disabled", {})
        any_row = d or x

        def cell(r: dict[str, Any]) -> str:
            if not r:
                return "-"
            s = r["outcome"]
            if r.get("stopped_at"):
                s += f" at {r['stopped_at']}"
            if cause := r.get("workflow_task_failed_cause"):
                s += f" ({cause.removeprefix('WORKFLOW_TASK_FAILED_CAUSE_')})"
            return s

        name = f"{cid} {any_row['label']}"
        out.append(
            f"{name:<34} {any_row['nodes']:>5}  {cell(d):<34} {cell(x):<34} "
            f"{any_row['largest_payload_bytes']:>15,} {any_row['history_size_bytes']:>13,} "
            f"{any_row['history_events']:>6}"
        )
    return "\n".join(out)


async def main(argv: list[str]) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--configs", nargs="+", default=["B0", "B1"])
    ap.add_argument("--kb", type=int, default=60)
    ap.add_argument("--llm-bytes", type=int, default=1024)
    ap.add_argument("--nodes", type=int, default=40, help="node count for every config but B0")
    ap.add_argument("--stuck-after", type=float, default=60.0)
    ap.add_argument("--out", default="cliff")
    args = ap.parse_args(argv)

    b0_nodes = await preflight(args.kb, args.llm_bytes)
    print(f"preflight: B0 shape at {b0_nodes} nodes x {args.kb} KiB", flush=True)
    rows = []
    for cid in args.configs:
        b = BASELINES[cid]
        nodes = b0_nodes if cid == "B0" else args.nodes
        for disable in (False, True):
            r = await run_one(
                b,
                nodes,
                args.kb,
                args.llm_bytes,
                disable_limit=disable,
                stuck_after=args.stuck_after,
            )
            print(
                f"  {cid} {r['payload_check']}: {r['outcome']} {r['stopped_at'] or ''}", flush=True
            )
            rows.append(r)
    print(fmt(rows))
    path = write_results(
        args.out,
        "uv run python -m bench.cliff " + shlex.join(argv),
        {"b0_nodes": b0_nodes, "limit_bytes": LIMIT, "rows": rows},
    )
    print(f"wrote {path}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
