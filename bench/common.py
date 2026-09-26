"""Shared bench harness: plugins, workers and runs against the local dev server."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

from temporalio.client import Client, WorkflowHandle
from temporalio.common import RetryPolicy
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.worker import Worker

from bench.agents.investigator import build_graph, graph_name
from bench.agents.workflows import InvestigateInput, InvestigateWorkflow, persist_all
from stepledger.config import resolve_dsn

TEMPORAL_ADDRESS = "localhost:7233"


@dataclass(frozen=True)
class Shape:
    """One investigator graph shape; it becomes one registered graph."""

    nodes: int = 30
    parallel_fanout: int = 3
    interrupt_at: int | None = None
    effects: bool = True
    execute_in: tuple[tuple[str, str], ...] = ()

    @property
    def name(self) -> str:
        base = graph_name(
            self.nodes,
            parallel_fanout=self.parallel_fanout,
            interrupt_at=self.interrupt_at,
            effects=self.effects,
        )
        if self.execute_in:
            base += "-w" + "_".join(
                sorted(n for n, where in self.execute_in if where == "workflow")
            )
        return base

    def build(self) -> Any:
        return build_graph(
            self.nodes,
            parallel_fanout=self.parallel_fanout,
            interrupt_at=self.interrupt_at,
            effects=self.effects,
            execute_in=dict(self.execute_in) or None,
        )


@dataclass
class RunConfig:
    shape: Shape = field(default_factory=Shape)
    seed: int = 7
    kb_per_node: int = 1
    llm_bytes: int = 1024
    llm_tokens: int = 100
    vary_per_attempt: bool = False
    persist_mode: str = "none"  # none | naive | naive_upsert
    persist: str = "none"  # none | bulk
    journal: bool = False
    effects_mode: str = "direct"  # direct | once
    node_delay_ms: int = 0
    llm_model: str = "fake-llm"
    bill_llm: bool = False
    review_decision: str | None = "approve"

    def context(self) -> dict[str, Any]:
        return {
            "seed": self.seed,
            "kb_per_node": self.kb_per_node,
            "llm_bytes": self.llm_bytes,
            "llm_tokens": self.llm_tokens,
            "vary_per_attempt": self.vary_per_attempt,
            "persist_mode": self.persist_mode,
            "journal": self.journal,
            "effects_mode": self.effects_mode,
            "node_delay_ms": self.node_delay_ms,
            "llm_model": self.llm_model,
            "bill_llm": self.bill_llm,
        }

    def workflow_input(self) -> InvestigateInput:
        return InvestigateInput(
            graph=self.shape.name,
            context=self.context(),
            persist=self.persist,
            review_decision=self.review_decision,
        )


def langgraph_plugin(
    shapes: Sequence[Shape],
    *,
    start_to_close: timedelta = timedelta(minutes=2),
    retry_policy: RetryPolicy | None = None,
) -> LangGraphPlugin:
    opts: dict[str, Any] = {"start_to_close_timeout": start_to_close}
    if retry_policy is not None:
        opts["retry_policy"] = retry_policy
    return LangGraphPlugin(
        graphs={s.name: s.build() for s in dict.fromkeys(shapes)},
        default_activity_options=opts,
    )


async def connect(plugins: Sequence[Any] = ()) -> Client:
    return await Client.connect(TEMPORAL_ADDRESS, plugins=list(plugins))


@asynccontextmanager
async def running_worker(
    client: Client,
    lg: LangGraphPlugin,
    *,
    task_queue: str | None = None,
    disable_payload_error_limit: bool = False,
    **worker_kwargs: Any,
) -> AsyncIterator[str]:
    tq = task_queue or f"bench-{uuid.uuid4().hex[:8]}"
    async with Worker(
        client,
        task_queue=tq,
        workflows=[InvestigateWorkflow],
        activities=[persist_all],
        plugins=[lg],
        disable_payload_error_limit=disable_payload_error_limit,
        **worker_kwargs,
    ):
        yield tq


async def start(
    client: Client, task_queue: str, cfg: RunConfig, *, workflow_id: str | None = None
) -> WorkflowHandle[Any, Any]:
    return await client.start_workflow(
        InvestigateWorkflow.run,
        cfg.workflow_input(),
        id=workflow_id or f"investigate-{uuid.uuid4().hex[:10]}",
        task_queue=task_queue,
    )


async def run_in_process(cfg: RunConfig) -> dict[str, Any]:
    """The same graph and seed with no Temporal: the reference result."""
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.types import Command

    from bench.agents.investigator import initial_state

    app = cfg.shape.build().compile(checkpointer=InMemorySaver())
    config: Any = {"configurable": {"thread_id": "1"}}
    result: dict[str, Any] = await app.ainvoke(initial_state(), config, context=cfg.context())
    while "__interrupt__" in result:
        result = await app.ainvoke(
            Command(resume=cfg.review_decision), config, context=cfg.context()
        )
    return result


def dsn() -> str:
    return resolve_dsn()


RESULTS_DIR = Path(__file__).resolve().parent / "results"


def environment() -> dict[str, str]:
    import platform
    import subprocess
    from importlib.metadata import version

    try:
        server = subprocess.run(
            ["temporal", "--version"], capture_output=True, text=True, check=False
        ).stdout.strip()
    except OSError:
        server = "unknown"
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "temporalio": version("temporalio"),
        "langgraph": version("langgraph"),
        "fastcdc": version("fastcdc"),
        "psycopg": version("psycopg"),
        "temporal_cli": server,
    }


def write_results(name: str, command: str, data: dict[str, Any]) -> Any:
    """Write bench/results/<name>.json with the command that produced it."""
    import datetime
    import json

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    path = RESULTS_DIR / f"{name}.json"
    doc = {
        "command": command,
        "generated_at": datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
        "environment": environment(),
        **data,
    }
    path.write_text(json.dumps(doc, indent=1, sort_keys=False) + "\n")
    return path


async def reset_store() -> None:
    """Empty the dedup store before a storage config is measured.

    Payloads are shared by claim across every run in one database, so a B4 run that repeats a
    payload a B3 run stored whole-blob gets a dedupe hit on that whole-blob manifest. Emptying
    the store between configs keeps each config's storage numbers its own. Bench only: nothing
    else may be using the store while this runs.
    """
    import psycopg

    async with await psycopg.AsyncConnection.connect(dsn(), autocommit=True) as conn:
        await conn.execute("TRUNCATE sl_payload_refs, sl_payloads, sl_chunks")
