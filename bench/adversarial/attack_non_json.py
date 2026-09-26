"""7.5 Non-JSON outputs: LangChain message objects in state, under Temporal's pydantic converter.

Row hashes and stored outputs must come from the worker's own payload converter, so the ledger
agrees with history whatever the converter. Measures ledger counters against history and whether
materialize (hashing through the same converter) can rebuild the state.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta
from typing import Any

from temporalio.client import Client
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from bench.adversarial.common import AttackResult
from bench.agents.messages_graph import MessagesWorkflow, build
from bench.common import dsn
from stepledger import StepledgerPlugin
from stepledger.read.materialize import materialize
from stepledger.testing.history import check_stepledger

RUNS = 5


async def run() -> AttackResult:
    res = AttackResult(
        "7.5",
        "Non-JSON outputs (pydantic converter, LangChain messages)",
        "0 ledger divergence; materialize EXACT through the same converter",
    )
    lg = LangGraphPlugin(
        graphs={"msgs": build()},
        default_activity_options={"start_to_close_timeout": timedelta(seconds=30)},
    )
    sl = StepledgerPlugin(dsn(), langgraph=lg)
    client = await Client.connect(
        "localhost:7233", plugins=[sl], data_converter=pydantic_data_converter
    )
    tq = f"atk-msgs-{uuid.uuid4().hex[:6]}"
    counts: dict[str, Any] = {
        "runs": RUNS,
        "divergent": 0,
        "lost": 0,
        "rows": 0,
        "exact": 0,
        "gap": 0,
        "materialize_errors": [],
    }
    async with Worker(client, task_queue=tq, workflows=[MessagesWorkflow], plugins=[lg]):
        for _ in range(RUNS):
            h = await client.start_workflow(
                MessagesWorkflow.run, id=f"atk-msgs-{uuid.uuid4().hex[:8]}", task_queue=tq
            )
            await asyncio.wait_for(h.result(), 60)
            desc = await h.describe()
            c = await check_stepledger(client, dsn(), h.id, desc.run_id)
            counts["divergent"] += c.divergent_rows
            counts["lost"] += c.lost_rows
            counts["rows"] += c.rows
            try:
                m = await materialize(
                    dsn(),
                    build().compile(),
                    h.id,
                    payload_converter=pydantic_data_converter.payload_converter,
                )
                counts["exact" if m.completeness == "EXACT" else "gap"] += 1
            except Exception as e:  # measured, not hidden
                counts["materialize_errors"].append(f"{type(e).__name__}: {e}"[:200])
    res.measured = counts
    res.rate = (
        f"{counts['divergent']} divergent rows in {counts['rows']}; materialize EXACT "
        f"{counts['exact']}/{RUNS}, errors {len(counts['materialize_errors'])}"
    )
    res.holds = counts["divergent"] == 0 and counts["lost"] == 0
    return res
