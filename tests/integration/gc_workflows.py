"""Workflows that carry large payloads through External Storage, for the GC tests."""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow


@workflow.defn(name="GcHold")
class HoldWorkflow:
    """Started by a client with a large input; waits until signalled."""

    def __init__(self) -> None:
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.run
    async def run(self, big: str) -> int:
        await workflow.wait_condition(lambda: self.done)
        return len(big)


@workflow.defn(name="GcChain")
class ChainWorkflow:
    """Continues as new once with a large argument, then waits until signalled."""

    def __init__(self) -> None:
        self.done = False

    @workflow.signal
    def finish(self) -> None:
        self.done = True

    @workflow.run
    async def run(self, big: str, hops: int) -> int:
        if hops > 0:
            workflow.continue_as_new(args=[big + "|next", hops - 1])
        await workflow.wait_condition(lambda: self.done, timeout=timedelta(minutes=10))
        return len(big)


@workflow.defn(name="GcDone")
class DoneWorkflow:
    @workflow.run
    async def run(self, big: str) -> int:
        return len(big)
