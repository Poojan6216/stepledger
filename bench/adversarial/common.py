"""Shared helpers for the attack strategies (Phase 7)."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

from temporalio.common import RetryPolicy

from bench.common import Shape, connect, dsn, langgraph_plugin, running_worker
from stepledger import StepledgerPlugin
from stepledger.testing import faults

ROOT = Path(__file__).resolve().parent.parent.parent
WORK = ROOT / ".temporal" / "attacks"


@dataclass
class AttackResult:
    id: str
    name: str
    expected: str
    measured: dict[str, Any] = field(default_factory=dict)
    rate: str = ""
    holds: bool | None = None
    error: str | None = None


@asynccontextmanager
async def stepledger_worker(
    shapes: list[Shape],
    *,
    plan: list[faults.FaultSpec] | None = None,
    retry: RetryPolicy | None = None,
    start_to_close: timedelta = timedelta(seconds=30),
    **plugin_kwargs: Any,
) -> AsyncIterator[tuple[Any, str]]:
    """An in-process worker under LangGraphPlugin + StepledgerPlugin, with faults armed."""
    WORK.mkdir(parents=True, exist_ok=True)
    injector = faults.install(plan or [], WORK / f"faults-{uuid.uuid4().hex[:6]}.log")
    lg = langgraph_plugin(shapes, start_to_close=start_to_close, retry_policy=retry)
    sl = StepledgerPlugin(dsn(), langgraph=lg, **{"external_storage": False, **plugin_kwargs})
    client = await connect([sl])
    try:
        async with running_worker(client, lg) as tq:
            yield client, tq
    finally:
        faults.uninstall()
        del injector
