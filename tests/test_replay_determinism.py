"""Hard Rule 6: the plugin's workflow side is deterministic. Never skipped.

Replays every committed history in tests/histories/ through Temporal's Replayer with the
plugin enabled: histories recorded under the plugin, and histories recorded before it existed.
Temporal's own check compares commands but not headers, so this test also re-derives the
Stepledger headers and seal input during replay and compares them with what history recorded.
No server or database is needed.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from temporalio.api.enums.v1 import EventType
from temporalio.client import WorkflowHistory
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.worker import Replayer

from bench.agents.workflows import InvestigateWorkflow
from stepledger import StepledgerPlugin, headers
from stepledger.ledger import workflow_interceptor as wi
from tests.histories.record import graphs
from tests.integration import workflows as wfs

pytestmark = pytest.mark.never_skip

HISTORIES = Path(__file__).parent / "histories"
WORKFLOWS = [*wfs.ALL_WORKFLOWS, InvestigateWorkflow]


def history_files(variant: str) -> list[Path]:
    return sorted((HISTORIES / variant).glob("*.json"))


def load(path: Path) -> WorkflowHistory:
    doc = json.loads(path.read_text())
    started = doc["events"][0]["workflowExecutionStartedEventAttributes"]
    wid = started.get("workflowId") or path.stem
    return WorkflowHistory.from_json(wid, doc)


def recorded(history: WorkflowHistory) -> tuple[list[tuple[str, Any]], dict[str, Any] | None]:
    """(activity type, decoded stepledger headers) per scheduled event, and the seal input."""
    out, seal = [], None
    for ev in history.events:
        if ev.event_type != EventType.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
            continue
        a = ev.activity_task_scheduled_event_attributes
        if a.activity_type.name == wi.SEAL_ACTIVITY:
            seal = json.loads(a.input.payloads[0].data)
            continue
        out.append((a.activity_type.name, headers.decode(a.header.fields)))
    return out, seal


def replayer(order: str = "sl-first") -> Replayer:
    lg = LangGraphPlugin(
        graphs=graphs(), default_activity_options={"start_to_close_timeout": timedelta(seconds=30)}
    )
    sl = StepledgerPlugin(
        "postgresql://unused@localhost/unused", langgraph=lg, external_storage=False, prices={}
    )
    plugins: list[Any] = [sl, lg] if order == "sl-first" else [lg, sl]
    return Replayer(workflows=WORKFLOWS, plugins=plugins)


async def replay_and_compare(
    path: Path, monkeypatch: pytest.MonkeyPatch, order: str = "sl-first"
) -> list[str]:
    """Replay one history; return every mismatch between derived and recorded headers."""
    history = load(path)
    want_starts, want_seal = recorded(history)
    derived: list[tuple[str, Any]] = []
    seals: list[dict[str, Any]] = []
    orig_with = headers.with_headers
    orig_start = wi.LedgerOutbound.start_activity
    orig_seal_cls = wi.SealInput
    pending: list[Any] = []

    def with_headers(h: Any, seq: int, commits: Any, abandons: Any) -> Any:
        pending.append((seq, sorted(set(commits)), sorted(set(abandons))))
        return orig_with(h, seq, commits, abandons)

    def start_activity(self_: wi.LedgerOutbound, input: Any) -> Any:
        pending.clear()
        handle = orig_start(self_, input)
        if input.activity != wi.SEAL_ACTIVITY:  # the seal is compared by its input below
            derived.append((input.activity, pending[0] if pending else (None, [], [])))
        return handle

    def seal_input(*a: Any, **kw: Any) -> wi.SealInput:
        s = orig_seal_cls(*a, **kw)
        seals.append(asdict(s))
        return s

    with monkeypatch.context() as m:
        m.setattr(wi.headers, "with_headers", with_headers)
        m.setattr(wi.LedgerOutbound, "start_activity", start_activity)
        m.setattr(wi, "SealInput", seal_input)
        await replayer(order).replay_workflow(history)

    problems = []
    plugin_history = path.parent.name == "with_plugin"
    if plugin_history:
        if derived != want_starts:
            problems.append(
                f"{path.name}: headers differ\n derived {derived}\n recorded {want_starts}"
            )
        if want_seal is not None and (not seals or seals[-1] != want_seal):
            problems.append(f"{path.name}: seal input differs: {seals} vs {want_seal}")
        if want_seal is None and seals:
            problems.append(f"{path.name}: replay sealed but history did not")
    elif seals:
        problems.append(f"{path.name}: a pre-plugin history must not seal on replay")
    return problems


def _ids(paths: list[Path]) -> list[str]:
    return [f"{p.parent.name}/{p.stem}" for p in paths]


ALL = history_files("with_plugin") + history_files("without_plugin")


def test_corpus_present() -> None:
    assert len(history_files("with_plugin")) >= 10
    assert len(history_files("without_plugin")) >= 10


@pytest.mark.parametrize("path", ALL, ids=_ids(ALL))
async def test_replays_deterministically(path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert await replay_and_compare(path, monkeypatch) == []


@pytest.mark.parametrize(
    "path", history_files("with_plugin")[:3], ids=_ids(history_files("with_plugin")[:3])
)
async def test_plugin_order_does_not_matter(path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert await replay_and_compare(path, monkeypatch, order="lg-first") == []


async def test_planted_clock_read_is_caught(monkeypatch: pytest.MonkeyPatch) -> None:
    """A datetime.now() planted in the workflow interceptor must make replay fail."""
    orig = wi.RunState.assign

    def assign(self: wi.RunState) -> int:
        seq = orig(self)
        return seq + datetime.now(UTC).microsecond % 997 + 1  # nondeterministic

    monkeypatch.setattr(wi.RunState, "assign", assign)
    problems = await replay_and_compare(HISTORIES / "with_plugin" / "lin5.json", monkeypatch)
    assert problems and "headers differ" in problems[0]
