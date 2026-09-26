"""A worker process for the chaos bench. The supervisor restarts it after os._exit(137).

    python -m bench.chaos_worker --config SL --task-queue chaos-x --nodes 30

Fault injection is armed from $STEPLEDGER_FAULTS / $STEPLEDGER_FAULT_LOG.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from datetime import timedelta

from temporalio.common import RetryPolicy
from temporalio.worker import Worker

from bench.agents.workflows import InvestigateWorkflow, persist_all
from bench.common import Shape, connect, dsn, langgraph_plugin
from stepledger.testing import faults

TIMEOUT = timedelta(seconds=5)


async def main(argv: list[str]) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", choices=["B1", "B1u", "SL"], required=True)
    ap.add_argument("--task-queue", required=True)
    ap.add_argument("--nodes", type=int, default=30)
    ap.add_argument("--on-ledger-error", default="fail")
    ap.add_argument("--clock-skew-s", type=float, default=0.0, help="attack 7.2")
    args = ap.parse_args(argv)
    if args.clock_skew_s:
        _skew_worker_clock(args.clock_skew_s)

    faults.install()
    lg = langgraph_plugin(
        [Shape(nodes=args.nodes)],
        start_to_close=TIMEOUT,
        retry_policy=RetryPolicy(initial_interval=timedelta(milliseconds=500)),
    )
    plugins = []
    if args.config == "SL":
        from stepledger import StepledgerPlugin

        plugins.append(
            StepledgerPlugin(
                dsn(),
                langgraph=lg,
                external_storage=False,
                seal_timeout=TIMEOUT,
                on_ledger_error=args.on_ledger_error,
            )
        )
    client = await connect(plugins)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    async with Worker(
        client,
        task_queue=args.task_queue,
        workflows=[InvestigateWorkflow],
        activities=[persist_all],
        plugins=[lg],
        max_concurrent_activities=50,
    ):
        print("READY", flush=True)
        await stop.wait()


def _skew_worker_clock(seconds: float) -> None:
    """Make every clock reading Stepledger takes on this worker wrong by `seconds`."""
    from datetime import datetime

    from stepledger.ledger import activity_interceptor

    class Skewed(datetime):
        @classmethod
        def now(cls, tz=None):  # type: ignore[no-untyped-def,override]
            return datetime.now(tz) + timedelta(seconds=seconds)

    activity_interceptor.datetime = Skewed  # type: ignore[misc]


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
