"""Small graphs and workflows for the ledger integration tests.

Node functions are distinct module-level functions (the LangGraph plugin rejects closures and
caches on function identity plus input).
"""

from __future__ import annotations

import asyncio
import contextlib
import operator
from dataclasses import dataclass
from datetime import timedelta
from typing import Annotated, Any, Literal, TypedDict

from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError

from bench.agents.variants import CacheHitWorkflow, cache_hit_graph

with workflow.unsafe.imports_passed_through():
    from langgraph.graph import END, START, StateGraph
    from langgraph.types import Command
    from temporalio.contrib.langgraph import graph as lg_graph


class S(TypedDict):
    log: Annotated[list[str], operator.add]


async def a(state: S) -> dict[str, Any]:
    return {"log": ["a"]}


async def b(state: S) -> dict[str, Any]:
    return {"log": ["b"]}


async def c(state: S) -> dict[str, Any]:
    return {"log": ["c"]}


async def d(state: S) -> dict[str, Any]:
    return {"log": ["d"]}


async def e(state: S) -> dict[str, Any]:
    return {"log": ["e"]}


async def fail_hard(state: S) -> dict[str, Any]:
    raise ApplicationError("node failed for good", non_retryable=True)


async def slow(state: S) -> dict[str, Any]:
    await asyncio.sleep(3.0)  # no heartbeat: never learns it was cancelled
    return {"log": ["slow"]}


def cmd_a(state: S) -> Command[Literal["cmd_b"]]:
    return Command(update={"log": ["a"]}, goto="cmd_b")


def cmd_b(state: S) -> Command[Literal["__end__"]]:
    return Command(update={"log": ["b"]}, goto="__end__")


def command_graph() -> StateGraph[Any, Any, Any, Any]:
    """Nodes that return Command(update=..., goto=...): COMMAND rows in the ledger."""
    g: StateGraph[Any, Any, Any, Any] = StateGraph(S)
    g.add_node("cmd_a", cmd_a, metadata={"execute_in": "activity"})
    g.add_node("cmd_b", cmd_b, metadata={"execute_in": "activity"})
    g.add_edge(START, "cmd_a")
    return g


async def big_a(state: S) -> dict[str, Any]:
    return {"log": ["a" * 5000]}  # over a 1 KiB storage threshold


async def big_b(state: S) -> dict[str, Any]:
    return {"log": ["b" * 5000]}


def chain(*fns: Any) -> StateGraph[Any, Any, Any, Any]:
    g: StateGraph[Any, Any, Any, Any] = StateGraph(S)
    prev = START
    for fn in fns:
        g.add_node(fn.__name__, fn, metadata={"execute_in": "activity"})
        g.add_edge(prev, fn.__name__)
        prev = fn.__name__
    g.add_edge(prev, END)
    return g


def graphs() -> dict[str, StateGraph[Any, Any, Any, Any]]:
    return {
        "lin5": chain(a, b, c, d, e),
        "until_fail": chain(a, b, c, fail_hard),
        "tail": chain(e),
        "fail_graph": chain(a, fail_hard),
        "slow_graph": chain(a, slow),
        "ab": chain(a, b),
        "cachehit": cache_hit_graph(),
        "commands": command_graph(),
        "bigab": chain(big_a, big_b),
    }


TIMEOUT = timedelta(seconds=30)


@activity.defn(name="tests.untracked")
async def untracked(x: int) -> int:
    return x + 1


@dataclass
class Args:
    graph: str = "lin5"
    untracked: bool = False
    continue_as_new: int = 0


@workflow.defn(name="Linear")
class LinearWorkflow:
    @workflow.run
    async def run(self, args: Args) -> dict[str, Any]:
        result: dict[str, Any] = await lg_graph(args.graph).compile().ainvoke({"log": []})
        if args.untracked:
            await workflow.execute_activity(untracked, 1, start_to_close_timeout=TIMEOUT)
        if args.continue_as_new > 0:
            workflow.continue_as_new(Args(args.graph, args.untracked, args.continue_as_new - 1))
        return result


@workflow.defn(name="CarrierFail")
class CarrierFailWorkflow:
    """Node `fail_hard` carries the commit for `c`, then fails for good. The workflow catches
    the failure and continues with another graph; `c`'s commit must still land."""

    @workflow.run
    async def run(self) -> dict[str, Any]:
        try:
            await lg_graph("until_fail").compile().ainvoke({"log": []})
        except Exception:
            workflow.logger.info("until_fail failed as planned")
        result: dict[str, Any] = await lg_graph("tail").compile().ainvoke({"log": []})
        return result


@workflow.defn(name="Fail")
class FailWorkflow:
    @workflow.run
    async def run(self) -> dict[str, Any]:
        result: dict[str, Any] = await lg_graph("fail_graph").compile().ainvoke({"log": []})
        return result


@workflow.defn(name="InnerCancel")
class InnerCancelWorkflow:
    """Raises asyncio.CancelledError with no cancel request: the SDK fails the run."""

    @workflow.run
    async def run(self) -> None:
        await lg_graph("ab").compile().ainvoke({"log": []})
        raise asyncio.CancelledError("internal")


@workflow.defn(name="CancelLate")
class CancelLateWorkflow:
    """Cancels the `slow` node's Activity (TRY_CANCEL) while it is running; the Activity keeps
    going and completes after the cancel request. The workflow never receives that result."""

    @workflow.run
    async def run(self, wait_after_cancel: float) -> list[str]:
        task = asyncio.create_task(lg_graph("slow_graph").compile().ainvoke({"log": []}))
        await workflow.sleep(1.0)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task  # the cancellation is the point
        if wait_after_cancel > 0:
            await workflow.sleep(wait_after_cancel)
        return ["done"]


@workflow.defn(name="TaskBug")
class TaskBugWorkflow:
    """Raises a plain RuntimeError (a workflow *task* failure) until signalled `fix`."""

    def __init__(self) -> None:
        self.fixed = False

    @workflow.signal
    def fix(self) -> None:
        self.fixed = True

    @workflow.run
    async def run(self, graph: str = "ab") -> dict[str, Any]:
        result: dict[str, Any] = await lg_graph(graph).compile().ainvoke({"log": []})
        if not self.fixed:
            raise RuntimeError("a bug in workflow code")
        return result


ALL_WORKFLOWS = [
    LinearWorkflow,
    CacheHitWorkflow,
    CarrierFailWorkflow,
    FailWorkflow,
    InnerCancelWorkflow,
    CancelLateWorkflow,
    TaskBugWorkflow,
]
