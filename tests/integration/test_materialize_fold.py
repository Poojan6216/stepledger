"""5.1: the fold, on the diamond graph from Appendix A.3, without a Temporal server.

a -> (b, c in parallel) -> d, with an operator.add list and a last-value channel. Each node
records what the Activity interceptor would (step, path, input, output); those become ledger
rows, and materialize must rebuild the graph's result exactly, or report GAP when a row is
missing or tampered with.
"""

from __future__ import annotations

import operator
import uuid
from datetime import UTC, datetime
from typing import Annotated, Any, TypedDict

import psycopg
import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph
from psycopg.types.json import Jsonb

from stepledger._compat import task_path_str
from stepledger.canonical import chash
from stepledger.read.materialize import materialize

pytestmark = pytest.mark.integration

CALLS: list[dict[str, Any]] = []


class D(TypedDict):
    items: Annotated[list[str], operator.add]
    last: str


def _record(name: str, state: D, config: RunnableConfig, out: dict[str, Any]) -> dict[str, Any]:
    md = config["metadata"]
    CALLS.append(
        {
            "node": name,
            "step": md["langgraph_step"],
            "path": task_path_str(md["langgraph_path"]),
            "input": dict(state),
            "output": out,
        }
    )
    return out


def a(state: D, config: RunnableConfig) -> dict[str, Any]:
    return _record("a", state, config, {"items": ["a"], "last": "a"})


def b(state: D, config: RunnableConfig) -> dict[str, Any]:
    return _record("b", state, config, {"items": ["b1", "b2"]})


def c(state: D, config: RunnableConfig) -> dict[str, Any]:
    return _record("c", state, config, {"items": ["c"]})


def d(state: D, config: RunnableConfig) -> dict[str, Any]:
    return _record("d", state, config, {"items": ["d"], "last": "d"})


def diamond() -> Any:
    g: StateGraph[Any, Any, Any, Any] = StateGraph(D)
    for fn in (a, b, c, d):
        g.add_node(fn.__name__, fn)
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("a", "c")
    g.add_edge(["b", "c"], "d")
    g.add_edge("d", END)
    return g.compile()


def _ledger(dsn: str, calls: list[dict[str, Any]], result: dict[str, Any]) -> str:
    wid = f"diamond-{uuid.uuid4().hex[:8]}"
    now = datetime.now(UTC)
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "INSERT INTO sl_runs (namespace, workflow_id, run_id, status, node_count,"
            " final_state_hash, sealed_at)"
            " VALUES ('default', %s, 'r1', 'COMPLETED', %s, %s, now())",
            (wid, len(calls), chash(result)),
        )
        for seq, call in enumerate(calls):
            out = {
                "result": call["output"],
                "langgraph_command": None,
                "langgraph_interrupts": None,
            }
            conn.execute(
                "INSERT INTO sl_nodes (namespace, workflow_id, run_id, seq, activity_id,"
                " activity_type, node, lg_step, lg_path, attempt, fence_scheduled_at, status,"
                " kind, output_json, output_hash, input_snapshot, input_hash, started_at,"
                " finished_at) VALUES ('default', %s, 'r1', %s, %s, %s, %s, %s, %s, 1, %s,"
                " 'COMMITTED', 'UPDATE', %s, %s, %s, %s, %s, %s)",
                (
                    wid,
                    seq,
                    str(seq + 1),
                    f"diamond.{call['node']}",
                    call["node"],
                    call["step"],
                    call["path"],
                    now,
                    Jsonb(out),
                    chash(out),
                    Jsonb(call["input"]) if seq == 0 else None,
                    chash(call["input"]),
                    now,
                    now,
                ),
            )
    return wid


def _run() -> tuple[Any, list[dict[str, Any]], dict[str, Any]]:
    CALLS.clear()
    graph = diamond()
    result = graph.invoke({"items": ["seed"], "last": ""})
    return graph, sorted(CALLS, key=lambda c: (c["step"], c["path"])), result


async def test_diamond_materializes_exactly(dsn: str) -> None:
    graph, calls, result = _run()
    assert [c["step"] for c in calls] == [1, 2, 2, 3]  # b and c share a superstep
    wid = _ledger(dsn, calls, result)
    m = await materialize(dsn, graph, wid)
    assert m.completeness == "EXACT", m.reason
    assert m.state == result
    assert m.rows_used == 4


async def test_parallel_order_is_langgraph_path_order(dsn: str) -> None:
    """Rows inserted in the 'wrong' seq order still fold in LangGraph's path order."""
    graph, calls, result = _run()
    swapped = [calls[0], calls[2], calls[1], calls[3]]
    wid = _ledger(dsn, swapped, result)
    m = await materialize(dsn, graph, wid)
    assert m.completeness == "EXACT" and m.state == result


async def test_missing_middle_step_is_a_gap_never_exact(dsn: str) -> None:
    graph, calls, result = _run()
    wid = _ledger(dsn, [calls[0], calls[3]], result)  # b and c have no rows
    m = await materialize(dsn, graph, wid)
    assert m.completeness == "GAP"
    assert m.state != result
    assert any("step 3" in p for p in m.positions), m.positions


async def test_tampered_output_is_a_gap(dsn: str) -> None:
    graph, calls, result = _run()
    bad = [dict(c) for c in calls]
    bad[1] = {**bad[1], "output": {"items": ["tampered"]}}
    wid = _ledger(dsn, bad, result)
    m = await materialize(dsn, graph, wid)
    assert m.completeness == "GAP"
