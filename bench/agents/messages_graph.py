"""A message-state graph and workflow for attack 7.5 (kept free of bench imports so the workflow
sandbox can load it)."""

from __future__ import annotations

from typing import Annotated, Any, TypedDict

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from langchain_core.messages import AIMessage, AnyMessage, ToolMessage
    from langgraph.graph import END, START, StateGraph
    from langgraph.graph.message import add_messages
    from temporalio.contrib.langgraph import graph as lg_graph


class MsgState(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]
    turns: int


async def think(state: MsgState) -> dict[str, Any]:
    n = state["turns"]
    return {"messages": [AIMessage(content=f"plan step {n}", id=f"ai-{n}")], "turns": n + 1}


async def act(state: MsgState) -> dict[str, Any]:
    n = state["turns"]
    return {
        "messages": [
            ToolMessage(content=f"tool output {n}", tool_call_id=f"call-{n}", id=f"tool-{n}")
        ]
    }


async def conclude(state: MsgState) -> dict[str, Any]:
    return {"messages": [AIMessage(content=f"done after {len(state['messages'])}", id="ai-final")]}


def build() -> StateGraph[Any, Any, Any, Any]:
    g: StateGraph[Any, Any, Any, Any] = StateGraph(MsgState)
    for fn in (think, act, conclude):
        g.add_node(fn.__name__, fn, metadata={"execute_in": "activity"})
    g.add_edge(START, "think")
    g.add_edge("think", "act")
    g.add_edge("act", "conclude")
    g.add_edge("conclude", END)
    return g


@workflow.defn(name="MessagesAttack")
class MessagesWorkflow:
    @workflow.run
    async def run(self) -> dict[str, Any]:
        result: dict[str, Any] = (
            await lg_graph("msgs").compile().ainvoke({"messages": [], "turns": 0})
        )
        return result
