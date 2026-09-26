"""A worker running every test graph under LangGraphPlugin + StepledgerPlugin."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import pytest
from temporalio.client import Client
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.worker import Worker

from bench.agents.workflows import InvestigateWorkflow, persist_all
from bench.common import Shape
from stepledger import StepledgerPlugin
from tests.integration import workflows as wfs

INVESTIGATOR = Shape(nodes=30)


@dataclass
class Env:
    client: Client
    task_queue: str
    dsn: str
    lg: LangGraphPlugin
    sl: Any


async def _client(plugins: list[Any]) -> Client:
    try:
        return await Client.connect("localhost:7233", plugins=plugins)
    except Exception as e:
        pytest.skip(f"Temporal dev server not reachable: run scripts/dev.sh up ({e})")


@pytest.fixture(scope="module")
async def env(dsn: str) -> AsyncIterator[Env]:
    graphs = wfs.graphs() | {INVESTIGATOR.name: INVESTIGATOR.build()}
    lg = LangGraphPlugin(
        graphs=graphs, default_activity_options={"start_to_close_timeout": timedelta(seconds=30)}
    )
    sl = StepledgerPlugin(dsn, langgraph=lg, external_storage=False)
    client = await _client([sl])
    tq = f"it-{uuid.uuid4().hex[:8]}"
    async with Worker(
        client,
        task_queue=tq,
        workflows=[*wfs.ALL_WORKFLOWS, InvestigateWorkflow],
        activities=[wfs.untracked, persist_all],
        plugins=[lg],
    ):
        yield Env(client, tq, dsn, lg, sl)
