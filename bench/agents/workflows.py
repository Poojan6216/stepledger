"""The investigation workflow, plus the B0 bulk-persist activity."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from temporalio import activity, workflow

with workflow.unsafe.imports_passed_through():
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.types import Command
    from temporalio.contrib.langgraph import graph

    from bench.agents.investigator import initial_state


@dataclass
class InvestigateInput:
    graph: str
    context: dict[str, Any] = field(default_factory=dict)
    target: str = "acct-7f3a"
    persist: str = "none"  # "none" | "bulk" (B0: one persist_all activity at the end)
    review_decision: str | None = None  # resume value for an interrupt; None waits for a signal


@activity.defn(name="bench.persist_all")
async def persist_all(state: dict[str, Any]) -> int:
    from bench.agents import sinks

    return await sinks.persist_all(state)


@workflow.defn(name="Investigate")
class InvestigateWorkflow:
    def __init__(self) -> None:
        self._review: str | None = None

    @workflow.signal
    def review(self, decision: str) -> None:
        self._review = decision

    @workflow.run
    async def run(self, inp: InvestigateInput) -> dict[str, Any]:
        app = graph(inp.graph).compile(checkpointer=InMemorySaver())
        config: Any = {"configurable": {"thread_id": "1"}}
        result: dict[str, Any] = await app.ainvoke(
            initial_state(inp.target), config, context=inp.context
        )
        while "__interrupt__" in result:
            if inp.review_decision is None:
                await workflow.wait_condition(lambda: self._review is not None)
                decision, self._review = self._review, None
            else:
                decision = inp.review_decision
            result = await app.ainvoke(Command(resume=decision), config, context=inp.context)
        if inp.persist == "bulk":
            await workflow.execute_activity(
                persist_all, result, start_to_close_timeout=timedelta(minutes=1)
            )
        return result
