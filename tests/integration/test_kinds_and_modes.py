"""COMMAND rows (nodes returning Command(update=..., goto=...)) and store_outputs="hash_only"."""

from __future__ import annotations

import uuid
from datetime import timedelta

import psycopg
import pytest
from temporalio.client import Client
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.worker import Worker

from stepledger import StepledgerPlugin
from stepledger.read.materialize import materialize
from stepledger.testing.history import check_stepledger
from tests.integration import workflows as wfs
from tests.integration.conftest import Env

pytestmark = pytest.mark.integration


async def test_command_nodes_are_command_rows_and_materialize(env: Env) -> None:
    h = await env.client.start_workflow(
        wfs.LinearWorkflow.run,
        wfs.Args("commands"),
        id=f"cmd-{uuid.uuid4().hex[:8]}",
        task_queue=env.task_queue,
    )
    result = await h.result()
    assert result == {"log": ["a", "b"]}
    run_id = h.result_run_id or ""
    with psycopg.connect(env.dsn) as conn:
        rows = conn.execute(
            "SELECT node, kind, status, output_json->'langgraph_command'->'update' FROM sl_nodes"
            " WHERE workflow_id = %s ORDER BY seq",
            (h.id,),
        ).fetchall()
    assert [(r[0], r[1], r[2]) for r in rows] == [
        ("cmd_a", "COMMAND", "COMMITTED"),
        ("cmd_b", "COMMAND", "COMMITTED"),
    ]
    assert [r[3] for r in rows] == [{"log": ["a"]}, {"log": ["b"]}]
    assert (await check_stepledger(env.client, env.dsn, h.id, run_id)).zero()
    m = await materialize(env.dsn, wfs.command_graph().compile(), h.id, run_id)
    assert m.completeness == "EXACT" and m.state == result


async def test_hash_only_stores_no_output(dsn: str) -> None:
    lg = LangGraphPlugin(
        graphs={"lin5": wfs.chain(wfs.a, wfs.b, wfs.c, wfs.d, wfs.e)},
        default_activity_options={"start_to_close_timeout": timedelta(seconds=30)},
    )
    sl = StepledgerPlugin(dsn, langgraph=lg, external_storage=False, store_outputs="hash_only")
    client = await Client.connect("localhost:7233", plugins=[sl])
    tq = f"hashonly-{uuid.uuid4().hex[:8]}"
    async with Worker(client, task_queue=tq, workflows=[wfs.LinearWorkflow], plugins=[lg]):
        h = await client.start_workflow(
            wfs.LinearWorkflow.run,
            wfs.Args("lin5"),
            id=f"hashonly-{uuid.uuid4().hex[:8]}",
            task_queue=tq,
        )
        await h.result()
    run_id = h.result_run_id or ""
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT status, output_json, output_bytes, output_hash, input_snapshot FROM sl_nodes"
            " WHERE workflow_id = %s ORDER BY seq",
            (h.id,),
        ).fetchall()
    assert len(rows) == 5
    assert all(r[0] == "COMMITTED" and r[1] is None and r[2] is None for r in rows)
    assert all(len(r[3]) == 64 for r in rows)  # the hash is still there
    assert all(r[4] is None for r in rows)  # no input snapshot either
    # hashes still agree with history, so divergence is still detectable
    assert (await check_stepledger(client, dsn, h.id, run_id)).zero()
    # and materialize can say nothing more than GAP without outputs
    m = await materialize(dsn, wfs.chain(wfs.a, wfs.b, wfs.c, wfs.d, wfs.e).compile(), h.id, run_id)
    assert m.completeness == "GAP"
