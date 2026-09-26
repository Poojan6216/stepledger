"""3.2: the invariant checker flags a hand-corrupted ledger in each of the five ways."""

from __future__ import annotations

import uuid

import psycopg
import pytest

from bench.agents import sinks
from bench.common import RunConfig
from stepledger.testing.history import check_stepledger
from tests.integration.conftest import INVESTIGATOR, Env

pytestmark = pytest.mark.integration

CORRUPTIONS = {
    "duplicate_rows": [
        "INSERT INTO sl_nodes SELECT namespace, workflow_id, run_id, 1000, activity_id,"
        " activity_type, graph, node, lg_step, lg_path, lg_task_id, checkpoint_ns, attempt,"
        " fence_scheduled_at, status, kind, output_json, output_bytes, output_encoding,"
        " output_hash, input_snapshot, input_hash, input_bytes, tokens_in, tokens_out, cost_usd,"
        " started_at, finished_at, committed_at FROM sl_nodes"
        " WHERE workflow_id = %(wf)s AND run_id = %(run)s AND seq = 3"
    ],
    "divergent_rows": [
        "UPDATE sl_nodes SET output_hash = 'tampered'"
        " WHERE workflow_id = %(wf)s AND run_id = %(run)s AND seq = 5"
    ],
    "lost_rows": [
        "DELETE FROM sl_nodes WHERE workflow_id = %(wf)s AND run_id = %(run)s AND seq = 7"
    ],
    "orphan_rows": [
        "UPDATE sl_nodes SET status = 'PROVISIONAL'"
        " WHERE workflow_id = %(wf)s AND run_id = %(run)s AND seq = 9"
    ],
    "duplicate_side_effects": [
        "INSERT INTO bench_effects (effect, key, request_hash, workflow_id, run_id, attempt)"
        " SELECT effect, key, request_hash, workflow_id, run_id, attempt + 1 FROM bench_effects"
        " WHERE workflow_id = %(wf)s AND effect = 'open_ticket'"
    ],
}


@pytest.mark.parametrize("counter", list(CORRUPTIONS))
async def test_checker_flags_corruption(env: Env, counter: str) -> None:
    await sinks.pool()  # creates bench_effects
    cfg = RunConfig(shape=INVESTIGATOR, kb_per_node=1)
    h = await env.client.start_workflow(
        "Investigate",
        cfg.workflow_input(),
        id=f"corrupt-{counter}-{uuid.uuid4().hex[:6]}",
        task_queue=env.task_queue,
    )
    await h.result()
    run_id = h.result_run_id or ""
    clean = await check_stepledger(env.client, env.dsn, h.id, run_id)
    assert clean.zero(), clean.details

    with psycopg.connect(env.dsn) as conn:
        for sql in CORRUPTIONS[counter]:
            assert conn.execute(sql, {"wf": h.id, "run": run_id}).rowcount == 1
    dirty = await check_stepledger(env.client, env.dsn, h.id, run_id)
    assert getattr(dirty, counter) >= 1, dirty.details
    others = {k: v for k, v in dirty.as_dict().items() if k not in (counter, "rows")}
    assert set(others.values()) == {0}, others
