"""The demo agent: a cloud-security investigation (build spec, Appendix B).

Shape: list_assets -> scan_* (parallel superstep) -> enrich_cve_0..N (sequential) -> score_risk
-> [human_review, interrupt()] -> [open_ticket -> notify_slack] -> summarize.

Every node except the reviewer and the two effect nodes makes one FakeLLM call and appends a
tool transcript of `kb_per_node` KiB of non-repeating text to `messages`: the accumulating
state from issue #1894. All run parameters travel in LangGraph's runtime context, so the graph
code is identical under every baseline and under Stepledger.
"""

from __future__ import annotations

import hashlib
import itertools
import operator
from typing import Annotated, Any, TypedDict

from langchain_core.messages import HumanMessage
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime
from langgraph.types import interrupt

from stepledger.testing.fake_llm import FakeLLM, fake_bytes


class InvestigationState(TypedDict):
    target: str
    messages: Annotated[list[dict[str, Any]], operator.add]  # tool transcripts: the growing part
    findings: Annotated[list[dict[str, Any]], operator.add]
    risk_score: int  # LastValue
    ticket_id: str | None


class AgentContext(TypedDict, total=False):
    seed: int
    kb_per_node: int  # KiB of tool transcript per node
    llm_bytes: int  # bytes of FakeLLM "reasoning" per call
    llm_tokens: int  # tokens reported per call (input and output each)
    vary_per_attempt: bool  # FakeLLM answers differently on each Activity attempt
    persist_mode: str  # "none" | "naive" (B1) | "naive_upsert" (B1u)
    journal: bool  # wrap the model in stepledger's JournaledChatModel
    effects_mode: str  # "direct" | "once"


def initial_state(target: str = "acct-7f3a") -> InvestigationState:
    return {"target": target, "messages": [], "findings": [], "risk_score": 0, "ticket_id": None}


def _ctx(runtime: Runtime[AgentContext]) -> AgentContext:
    return runtime.context or {}


def make_llm(ctx: AgentContext) -> Any:
    llm = FakeLLM(
        seed=ctx.get("seed", 0),
        bytes_per_call=ctx.get("llm_bytes", 1024),
        tokens_per_call=ctx.get("llm_tokens", 100),
        vary_per_attempt=ctx.get("vary_per_attempt", False),
    )
    if ctx.get("journal"):
        from stepledger.llm.journal import JournaledChatModel

        return JournaledChatModel(inner=llm)
    return llm


async def _faults(point: str, node: str) -> None:
    from stepledger.testing import faults

    await faults.hit(point, node=node)


async def step(
    node: str, state: InvestigationState, runtime: Runtime[AgentContext]
) -> dict[str, Any]:
    """One tool-using node: an LLM call, a tool transcript, and a finding."""
    ctx = _ctx(runtime)
    await _faults("F1", node)
    reply = await make_llm(ctx).ainvoke(
        [HumanMessage(content=f"{node}: investigate {state['target']} (call 0)")]
    )
    await _faults("F2", node)
    reasoning = str(reply.content)
    transcript = fake_bytes(ctx.get("kb_per_node", 1) * 1024, ctx.get("seed", 0), "tool", node)
    delta: dict[str, Any] = {
        "messages": [
            {"role": "assistant", "name": node, "content": reasoning},
            {"role": "tool", "name": f"tool_{node}", "content": transcript},
        ],
        "findings": [{"node": node, "digest": hashlib.sha256(reasoning.encode()).hexdigest()[:12]}],
    }
    if node == "score_risk":
        delta["risk_score"] = int(hashlib.sha256(reasoning.encode()).hexdigest()[:4], 16) % 100
    await _naive_persist(ctx, node, delta)
    return delta


async def _naive_persist(ctx: AgentContext, node: str, delta: dict[str, Any]) -> None:
    mode = ctx.get("persist_mode", "none")
    if mode == "none":
        return
    from bench.agents import sinks

    await _faults("F6", node)  # a zombie hangs past its timeout, then writes late
    await sinks.naive_write(mode, node, delta)
    await _faults("F3", node)  # the worker dies after the write, before reporting completion


# --- fixed nodes (each a distinct module-level function; see Appendix B) --------------------


async def list_assets(state: InvestigationState, runtime: Runtime[AgentContext]) -> dict[str, Any]:
    return await step("list_assets", state, runtime)


async def scan_iam(state: InvestigationState, runtime: Runtime[AgentContext]) -> dict[str, Any]:
    return await step("scan_iam", state, runtime)


async def scan_network(state: InvestigationState, runtime: Runtime[AgentContext]) -> dict[str, Any]:
    return await step("scan_network", state, runtime)


async def scan_storage(state: InvestigationState, runtime: Runtime[AgentContext]) -> dict[str, Any]:
    return await step("scan_storage", state, runtime)


async def score_risk(state: InvestigationState, runtime: Runtime[AgentContext]) -> dict[str, Any]:
    return await step("score_risk", state, runtime)


async def summarize(state: InvestigationState, runtime: Runtime[AgentContext]) -> dict[str, Any]:
    return await step("summarize", state, runtime)


async def human_review(state: InvestigationState) -> dict[str, Any]:
    decision = interrupt({"risk_score": state["risk_score"], "findings": len(state["findings"])})
    return {"messages": [{"role": "human", "name": "reviewer", "content": str(decision)}]}


async def _effect(ctx: AgentContext, name: str, request: dict[str, Any]) -> str:
    from bench.agents import sinks

    if ctx.get("effects_mode", "direct") == "once":
        from stepledger import once

        return await once(name, lambda key: sinks.effect(name, request, key), request=request)
    return await sinks.effect(name, request, None)


async def open_ticket(state: InvestigationState, runtime: Runtime[AgentContext]) -> dict[str, Any]:
    request = {"target": state["target"], "risk": state["risk_score"]}
    ticket = await _effect(_ctx(runtime), "open_ticket", request)
    return {"ticket_id": ticket, "findings": [{"node": "open_ticket", "ticket": ticket}]}


async def notify_slack(state: InvestigationState, runtime: Runtime[AgentContext]) -> dict[str, Any]:
    request = {"channel": "#sec-alerts", "ticket": state["ticket_id"]}
    msg = await _effect(_ctx(runtime), "notify_slack", request)
    return {"findings": [{"node": "notify_slack", "message": msg}]}


SCANS = [scan_iam, scan_network, scan_storage]
FIXED_NODES = 3  # list_assets, score_risk, summarize


def enrich_count(
    nodes: int, *, parallel_fanout: int, interrupt_at: int | None, effects: bool
) -> int:
    fixed = FIXED_NODES + parallel_fanout + (interrupt_at is not None) + 2 * effects
    if nodes < fixed:
        raise ValueError(f"nodes={nodes} is below the {fixed} fixed nodes of this shape")
    return nodes - fixed


def graph_name(nodes: int, *, parallel_fanout: int, interrupt_at: int | None, effects: bool) -> str:
    review = "x" if interrupt_at is None else str(interrupt_at)
    return f"investigate-n{nodes}-f{parallel_fanout}-i{review}-e{int(effects)}"


def build_graph(
    nodes: int = 30,
    *,
    parallel_fanout: int = 3,
    interrupt_at: int | None = None,
    effects: bool = True,
    execute_in: dict[str, str] | None = None,
) -> StateGraph[Any, Any, Any, Any]:
    """A fresh StateGraph with exactly `nodes` nodes, all `execute_in="activity"` by default.

    `interrupt_at=k` inserts human_review after the k-th enrich node (k=0: before the first).
    `execute_in` overrides placement per node name (Phase 7.3 uses it for workflow-side nodes).
    LangGraphPlugin rewrites the nodes it is given, so build a new graph for every use.
    """
    from bench.agents._nodes_generated import ENRICH_NODES

    if not 0 <= parallel_fanout <= len(SCANS):
        raise ValueError(f"parallel_fanout must be 0..{len(SCANS)}")
    n_enrich = enrich_count(
        nodes, parallel_fanout=parallel_fanout, interrupt_at=interrupt_at, effects=effects
    )
    if n_enrich > len(ENRICH_NODES):
        raise ValueError(f"at most {len(ENRICH_NODES)} enrich nodes are generated")
    if interrupt_at is not None and not 0 <= interrupt_at <= n_enrich:
        raise ValueError(f"interrupt_at must be within 0..{n_enrich}")
    placement = execute_in or {}

    g: StateGraph[Any, Any, Any, Any] = StateGraph(InvestigationState, context_schema=AgentContext)

    def add(fn: Any) -> str:
        name = str(fn.__name__)
        g.add_node(name, fn, metadata={"execute_in": placement.get(name, "activity")})
        return name

    chain: list[str | list[str]] = [add(list_assets)]
    if parallel_fanout:
        chain.append([add(fn) for fn in SCANS[:parallel_fanout]])
    for i, fn in enumerate(ENRICH_NODES[:n_enrich]):
        if interrupt_at == i:
            chain.append(add(human_review))
        chain.append(add(fn))
    if interrupt_at == n_enrich:
        chain.append(add(human_review))
    chain.append(add(score_risk))
    if effects:
        chain += [add(open_ticket), add(notify_slack)]
    chain.append(add(summarize))

    g.add_edge(START, _first(chain[0]))
    for prev, nxt in itertools.pairwise(chain):
        if isinstance(nxt, list):
            for branch in nxt:
                g.add_edge(_last(prev), branch)
        else:
            g.add_edge(prev, nxt)
    g.add_edge(_last(chain[-1]), END)
    return g


def _first(item: str | list[str]) -> str:
    return item[0] if isinstance(item, list) else item


def _last(item: str | list[str]) -> str:
    if isinstance(item, list):
        raise ValueError("a parallel group must be followed by a single node")
    return item
