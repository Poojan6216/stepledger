"""Record workflow histories for tests/test_replay_determinism.py.

    scripts/dev.sh up && uv run python -m tests.histories.record

Writes tests/histories/with_plugin/*.json (runs under StepledgerPlugin) and
tests/histories/without_plugin/*.json (the same workflows with LangGraphPlugin only), so the
replay test proves both that plugin histories replay deterministically and that histories
recorded before the plugin existed still replay with it enabled.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

from temporalio.client import Client, WorkflowHandle
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.worker import Worker

from bench.agents.workflows import InvestigateWorkflow, persist_all
from bench.common import RunConfig, Shape
from stepledger import StepledgerPlugin
from stepledger.config import resolve_dsn
from tests.integration import workflows as wfs

HERE = Path(__file__).parent
SHAPES = [Shape(nodes=30), Shape(nodes=14, interrupt_at=2)]


def graphs() -> dict[str, Any]:
    return wfs.graphs() | {s.name: s.build() for s in SHAPES}


Starter = Callable[[Client, str], Awaitable[WorkflowHandle[Any, Any]]]


def _id(name: str) -> str:
    return f"hist-{name}-{uuid.uuid4().hex[:6]}"


async def _save(client: Client, handle: WorkflowHandle[Any, Any], out: Path, name: str) -> None:
    """Save every run of the workflow, following the continue-as-new chain through history."""
    run_id: str | None = handle.first_execution_run_id or handle.result_run_id
    runs = []
    while run_id:
        hist = await client.get_workflow_handle(handle.id, run_id=run_id).fetch_history()
        runs.append(hist)
        last = hist.events[-1]
        run_id = last.workflow_execution_continued_as_new_event_attributes.new_execution_run_id
    for i, hist in enumerate(runs):
        suffix = f"-run{i}" if len(runs) > 1 else ""
        (out / f"{name}{suffix}.json").write_text(hist.to_json())


async def _cancel_after(h: WorkflowHandle[Any, Any], delay: float) -> None:
    await asyncio.sleep(delay)
    await h.cancel()


async def record(variant: str, client: Client, lg: LangGraphPlugin) -> None:
    out = HERE / variant
    out.mkdir(parents=True, exist_ok=True)
    tq = f"hist-{uuid.uuid4().hex[:8]}"
    runs: dict[str, Starter] = {
        "lin5": lambda c, q: c.start_workflow(
            wfs.LinearWorkflow.run, wfs.Args("lin5", untracked=True), id=_id("lin5"), task_queue=q
        ),
        "carrier_fail": lambda c, q: c.start_workflow(
            wfs.CarrierFailWorkflow.run, id=_id("carrier"), task_queue=q
        ),
        "fail": lambda c, q: c.start_workflow(wfs.FailWorkflow.run, id=_id("fail"), task_queue=q),
        "inner_cancel": lambda c, q: c.start_workflow(
            wfs.InnerCancelWorkflow.run, id=_id("inner"), task_queue=q
        ),
        "cancel_late": lambda c, q: c.start_workflow(
            wfs.CancelLateWorkflow.run, 4.5, id=_id("late"), task_queue=q
        ),
        "continue_as_new": lambda c, q: c.start_workflow(
            wfs.LinearWorkflow.run, wfs.Args("ab", continue_as_new=1), id=_id("can"), task_queue=q
        ),
        "investigate30": lambda c, q: c.start_workflow(
            "Investigate",
            RunConfig(shape=SHAPES[0], llm_bytes=64).workflow_input(),
            id=_id("inv"),
            task_queue=q,
        ),
        "investigate_interrupt": lambda c, q: c.start_workflow(
            "Investigate",
            RunConfig(shape=SHAPES[1], llm_bytes=64).workflow_input(),
            id=_id("intr"),
            task_queue=q,
        ),
        "client_cancel": lambda c, q: c.start_workflow(
            wfs.LinearWorkflow.run, wfs.Args("slow_graph"), id=_id("cancel"), task_queue=q
        ),
    }
    async with Worker(
        client,
        task_queue=tq,
        workflows=[*wfs.ALL_WORKFLOWS, InvestigateWorkflow],
        activities=[wfs.untracked, persist_all],
        plugins=[lg],
    ):
        for name, start in runs.items():
            h = await start(client, tq)
            if name == "client_cancel":
                await _cancel_after(h, 1.0)
            try:
                await h.result()
            except Exception as e:  # failures and cancels are part of the corpus
                print(f"  {variant}/{name}: {type(e).__name__}")
            await _save(client, h, out, name)
            print(f"  {variant}/{name}: saved")


def plugins(with_stepledger: bool) -> tuple[LangGraphPlugin, list[Any]]:
    lg = LangGraphPlugin(
        graphs=graphs(), default_activity_options={"start_to_close_timeout": timedelta(seconds=30)}
    )
    client_plugins: list[Any] = []
    if with_stepledger:
        client_plugins.append(StepledgerPlugin(resolve_dsn(), langgraph=lg, external_storage=False))
    return lg, client_plugins


async def main() -> None:
    for variant, with_sl in (("with_plugin", True), ("without_plugin", False)):
        lg, cps = plugins(with_sl)
        client = await Client.connect("localhost:7233", plugins=cps)
        await record(variant, client, lg)


if __name__ == "__main__":
    asyncio.run(main())
