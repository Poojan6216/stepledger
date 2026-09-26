"""Graph variants for the materialize equivalence corpus (Demo 4).

`cache_hit_graph`: two nodes run the same function on the same input subset, so the second is a
within-run task-cache hit: the LangGraph plugin replays the first result without scheduling an
Activity, so the ledger has no row for it and materialize must report GAP.
"""

from __future__ import annotations

import operator
from typing import Annotated, Any, TypedDict

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from langgraph.graph import END, START, StateGraph
    from temporalio.contrib.langgraph import graph as lg_graph


class CS(TypedDict):
    log: Annotated[list[str], operator.add]
    n: int


class NOnly(TypedDict):
    n: int


async def cs_a(state: CS) -> dict[str, Any]:
    return {"log": ["a"]}


async def tag(state: NOnly) -> dict[str, Any]:
    return {"log": [f"tag{state['n']}"]}


async def cs_c(state: CS) -> dict[str, Any]:
    return {"log": ["c"]}


async def cs_e(state: CS) -> dict[str, Any]:
    return {"log": ["e"]}


def cache_hit_graph() -> StateGraph[Any, Any, Any, Any]:
    """tag1 and tag2 run the same function on the same input subset ({n}), so tag2 is a
    within-run task-cache hit: the plugin replays tag1's result, no Activity, no row."""
    g: StateGraph[Any, Any, Any, Any] = StateGraph(CS)
    md = {"execute_in": "activity"}
    g.add_node("cs_a", cs_a, metadata=md)
    g.add_node("tag1", tag, metadata=md, input_schema=NOnly)
    g.add_node("cs_c", cs_c, metadata=md)
    g.add_node("tag2", tag, metadata=md, input_schema=NOnly)
    g.add_node("cs_e", cs_e, metadata=md)
    for x, y in [
        (START, "cs_a"),
        ("cs_a", "tag1"),
        ("tag1", "cs_c"),
        ("cs_c", "tag2"),
        ("tag2", "cs_e"),
        ("cs_e", END),
    ]:
        g.add_edge(x, y)
    return g


@workflow.defn(name="CacheHit")
class CacheHitWorkflow:
    @workflow.run
    async def run(self) -> dict[str, Any]:
        result: dict[str, Any] = await lg_graph("cachehit").compile().ainvoke({"log": [], "n": 0})
        return result
