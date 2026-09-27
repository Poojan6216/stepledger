"""Hard Rule 11: nothing Stepledger adds to history grows with run length.

    uv run python -m bench.history_overhead

Runs the agent at 10..80 nodes (1 KiB per node, so no wall is hit) without Stepledger and with
it (ledger only), and measures the plugin's history footprint two ways:

1. Directly, from the with-plugin history: the bytes of the `stepledger-*` header fields on every
   `ActivityTaskScheduled` event, plus the bytes of the `stepledger.seal` Activity's own events.
   That is everything the plugin adds. Headers carry the seq and the commit and abandon ids no
   successful carrier has confirmed yet, so with no failed carrier each header is O(1) and the
   header bytes per node must be the same at every size, within the digits of the seq itself.
   The seal is one Activity per run, O(1).
2. As context, the total history size with the plugin minus without it. Each series is the
   minimum of `SAMPLES` runs, taken separately: workflow-task batching adds a few events'
   worth of noise to either run, and the minimum of each series is the quiet run.
"""

from __future__ import annotations

import asyncio
import itertools
import sys
from typing import Any

from temporalio.api.enums.v1 import EventType
from temporalio.api.history.v1 import HistoryEvent
from temporalio.client import WorkflowHandle

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
from bench.metrics import collect
from stepledger import StepledgerPlugin, headers
from stepledger.ledger.workflow_interceptor import SEAL_ACTIVITY

NODES = [10, 20, 40, 80]
SAMPLES = 3


def plugin_bytes_in(events: list[HistoryEvent]) -> tuple[int, int, int]:
    """(header bytes on node Activities, seal Activity event bytes, node Activities scheduled)."""
    header_bytes = seal_bytes = nodes = 0
    seal_ids: set[int] = set()
    for ev in events:
        if ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            a = ev.activity_task_scheduled_event_attributes
            if a.activity_type.name == SEAL_ACTIVITY:
                seal_ids.add(ev.event_id)
                seal_bytes += ev.ByteSize()
                continue
            fields = {k: p for k, p in a.header.fields.items() if k in headers.ALL}
            if fields:
                nodes += 1
                header_bytes += sum(len(k) + p.ByteSize() for k, p in fields.items())
        elif ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
            if ev.activity_task_started_event_attributes.scheduled_event_id in seal_ids:
                seal_bytes += ev.ByteSize()
        elif ev.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
            if ev.activity_task_completed_event_attributes.scheduled_event_id in seal_ids:
                seal_bytes += ev.ByteSize()
    return header_bytes, seal_bytes, nodes


async def one_run(nodes: int, with_plugin: bool) -> WorkflowHandle[Any, Any]:
    cfg = RunConfig(shape=Shape(nodes=nodes), kb_per_node=1)
    lg = langgraph_plugin([cfg.shape])
    plugins = [StepledgerPlugin(dsn(), langgraph=lg, external_storage=False)] if with_plugin else []
    client = await connect(plugins)
    async with running_worker(client, lg) as tq:
        h = await start(client, tq, cfg)
        await h.result()
    return h


async def measure(nodes: list[int] = NODES, samples: int = SAMPLES) -> dict[str, Any]:
    rows = []
    for n in nodes:
        without = []
        with_ = []
        header = scheduled = None
        seals: list[int] = []
        for _ in range(samples):
            without.append((await collect(await one_run(n, False))).history_size_bytes)
            h = await one_run(n, True)
            with_.append((await collect(h)).history_size_bytes)
            hb, sb, sched = plugin_bytes_in([ev async for ev in h.fetch_history_events()])
            # header bytes do not depend on batching: every sample agrees exactly. The seal's
            # events carry timestamps and a small result, so they vary by a few bytes: keep the min
            assert header in (None, hb) and scheduled in (None, sched), (n, header, hb, sched)
            header, scheduled = hb, sched
            seals.append(sb)
        assert header is not None and scheduled == n, (n, scheduled)
        seal = min(seals)
        rows.append(
            {
                "nodes": n,
                "header_bytes": header,
                "header_bytes_per_node": round(header / n, 2),
                "seal_event_bytes": seal,
                "history_without": min(without),
                "history_with": min(with_),
                "plugin_bytes": min(with_) - min(without),
                "samples_without": without,
                "samples_with": with_,
            }
        )
    marginal = [
        round((b["plugin_bytes"] - a["plugin_bytes"]) / (b["nodes"] - a["nodes"]), 2)
        for a, b in itertools.pairwise(rows)
    ]
    # The first node of a run carries no commits header (nothing is pending yet), so the header
    # total is a per-run constant plus a per-node cost; the per-node cost is the marginal one.
    marginal_header = [
        round((b["header_bytes"] - a["header_bytes"]) / (b["nodes"] - a["nodes"]), 2)
        for a, b in itertools.pairwise(rows)
    ]
    return {
        "samples_per_series": samples,
        "rows": rows,
        "header_bytes_per_node": [r["header_bytes_per_node"] for r in rows],
        "marginal_header_bytes_per_node": marginal_header,
        "seal_event_bytes": [r["seal_event_bytes"] for r in rows],
        "marginal_plugin_bytes_per_node": marginal,
    }


async def main(argv: list[str]) -> None:
    out = await measure()
    for r in out["rows"]:
        print(r)
    print("header bytes per node (average):", out["header_bytes_per_node"])
    print("header bytes per node (marginal):", out["marginal_header_bytes_per_node"])
    print("seal event bytes:", out["seal_event_bytes"])
    print(
        "marginal total plugin bytes per node (min of samples):",
        out["marginal_plugin_bytes_per_node"],
    )
    path = write_results("history_overhead", "uv run python -m bench.history_overhead", out)
    print(f"wrote {path}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
