"""Hard Rule 11: nothing Stepledger adds to history grows with run length.

    uv run python -m bench.history_overhead

Runs the agent at 10..80 nodes (1 KiB per node, so no wall is hit) without Stepledger and with
it (ledger only), and reports history bytes and the difference. Headers carry O(commits since
the last carrier) ids and the seal payload is O(1), so the difference must be linear in the node
count: a constant per-node header cost plus a constant for the seal. The marginal cost per node
between successive node counts must therefore be the same within noise.
"""

from __future__ import annotations

import asyncio
import itertools
import sys
from typing import Any

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
from stepledger import StepledgerPlugin

NODES = [10, 20, 40, 80]


async def history_bytes(nodes: int, with_plugin: bool) -> int:
    cfg = RunConfig(shape=Shape(nodes=nodes), kb_per_node=1)
    lg = langgraph_plugin([cfg.shape])
    plugins = [StepledgerPlugin(dsn(), langgraph=lg, external_storage=False)] if with_plugin else []
    client = await connect(plugins)
    async with running_worker(client, lg) as tq:
        h = await start(client, tq, cfg)
        await h.result()
    return (await collect(h)).history_size_bytes


async def measure(nodes: list[int] = NODES) -> dict[str, Any]:
    rows = []
    for n in nodes:
        plain, plugin = await history_bytes(n, False), await history_bytes(n, True)
        rows.append(
            {
                "nodes": n,
                "history_without": plain,
                "history_with": plugin,
                "plugin_bytes": plugin - plain,
            }
        )
    marginal = [
        round((b["plugin_bytes"] - a["plugin_bytes"]) / (b["nodes"] - a["nodes"]), 2)
        for a, b in itertools.pairwise(rows)
    ]
    return {"rows": rows, "marginal_plugin_bytes_per_node": marginal}


async def main(argv: list[str]) -> None:
    out = await measure()
    for r in out["rows"]:
        print(r)
    print("marginal plugin bytes per node:", out["marginal_plugin_bytes_per_node"])
    path = write_results("history_overhead", "uv run python -m bench.history_overhead", out)
    print(f"wrote {path}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
