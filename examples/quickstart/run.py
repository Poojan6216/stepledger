"""Stepledger quickstart: run a three-node graph on Temporal and read its ledger back.

# a Temporal dev server on localhost:7233 and Postgres at $STEPLEDGER_DSN
stepledger init-db
python examples/quickstart/run.py
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import timedelta

from quickstart_graph import QuickstartWorkflow, build_graph
from temporalio.client import Client
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.worker import Worker

from stepledger import StepledgerPlugin, materialize
from stepledger.read.ledger import run_ledger

DSN = os.environ.get(
    "STEPLEDGER_DSN", "postgresql://stepledger:stepledger@localhost:5432/stepledger"
)


async def main() -> None:
    lg = LangGraphPlugin(
        graphs={"quickstart": build_graph()},
        default_activity_options={"start_to_close_timeout": timedelta(minutes=2)},
    )
    sl = StepledgerPlugin(dsn=DSN, langgraph=lg)
    client = await Client.connect("localhost:7233", plugins=[sl])
    async with Worker(
        client, task_queue="quickstart", workflows=[QuickstartWorkflow], plugins=[lg]
    ):
        wid = f"quickstart-{uuid.uuid4().hex[:8]}"
        result = await client.execute_workflow(
            QuickstartWorkflow.run, "why did the run stop?", id=wid, task_queue="quickstart"
        )
    print("workflow result:", result)
    print()
    print(run_ledger(DSN, wid))
    rebuilt = await materialize(DSN, build_graph().compile(), wid)
    print()
    print(f"materialize: {rebuilt.completeness}; equal to the result: {rebuilt.state == result}")


if __name__ == "__main__":
    asyncio.run(main())
