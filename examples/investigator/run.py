"""Run one investigation under Stepledger and print its run ledger (needs the repo checkout)."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from bench.common import (
    RunConfig,
    Shape,
    connect,
    dsn,
    langgraph_plugin,
    running_worker,
    start,
)
from stepledger import StepledgerPlugin
from stepledger.read.ledger import run_ledger


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=30)
    ap.add_argument("--kb", type=int, default=1)
    ap.add_argument("--review", action="store_true", help="add an interrupt() for human review")
    args = ap.parse_args()
    shape = Shape(nodes=args.nodes, interrupt_at=0 if args.review else None)
    cfg = RunConfig(shape=shape, kb_per_node=args.kb, effects_mode="once")
    lg = langgraph_plugin([shape])
    client = await connect([StepledgerPlugin(dsn(), langgraph=lg)])
    async with running_worker(client, lg) as tq:
        handle = await start(client, tq, cfg)
        await handle.result()
    print(run_ledger(dsn(), handle.id))


if __name__ == "__main__":
    asyncio.run(main())
