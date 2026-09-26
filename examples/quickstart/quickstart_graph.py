"""A three-node LangGraph graph and its workflow.

Node functions live in a named module: the LangGraph plugin identifies nodes by
module.qualname and rejects functions defined in __main__ or as closures.
"""

from __future__ import annotations

import operator
from typing import Annotated, Any, TypedDict

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from langgraph.graph import END, START, StateGraph
    from temporalio.contrib.langgraph import graph


class State(TypedDict):
    question: str
    notes: Annotated[list[str], operator.add]
    answer: str


async def research(state: State) -> dict[str, Any]:
    return {"notes": [f"looked up: {state['question']}"]}


async def analyze(state: State) -> dict[str, Any]:
    return {"notes": [f"{len(state['notes'])} note(s) analysed"]}


async def respond(state: State) -> dict[str, Any]:
    return {"answer": f"answer to {state['question']!r} from {len(state['notes'])} notes"}


def build_graph() -> StateGraph[Any, Any, Any, Any]:
    g: StateGraph[Any, Any, Any, Any] = StateGraph(State)
    for fn in (research, analyze, respond):
        g.add_node(fn.__name__, fn, metadata={"execute_in": "activity"})
    g.add_edge(START, "research")
    g.add_edge("research", "analyze")
    g.add_edge("analyze", "respond")
    g.add_edge("respond", END)
    return g


@workflow.defn(name="Quickstart")
class QuickstartWorkflow:
    @workflow.run
    async def run(self, question: str) -> dict[str, Any]:
        result: dict[str, Any] = (
            await graph("quickstart")
            .compile()
            .ainvoke({"question": question, "notes": [], "answer": ""})
        )
        return result
