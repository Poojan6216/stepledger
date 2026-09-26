"""4.7: GC never deletes a payload a workflow can still need.

The three targeted tests (continue-as-new chain, client-stored start input, store/sweep race)
are never skipped. GC runs here with retention 0 and grace 0 so "expired" is immediate; the CLI
refuses such values (retention must cover the namespace's), the library allows them for tests.
Sweeps are scoped to this module's workflow ids so other data in the database is untouched.
"""

from __future__ import annotations

import asyncio
import base64
import random
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import psycopg
import pytest
from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.converter import (
    StorageDriverRetrieveContext,
    StorageDriverStoreContext,
    StorageDriverWorkflowInfo,
)
from temporalio.worker import Worker

from stepledger import StepledgerPlugin
from stepledger.storage import DedupStorageDriver, PostgresChunkBackend
from stepledger.storage import gc as gcmod
from stepledger.storage.gc import check_retention, namespace_retention, sweep
from tests.integration.gc_workflows import ChainWorkflow, DoneWorkflow, HoldWorkflow

pytestmark = pytest.mark.integration

ZERO = timedelta(0)


@dataclass
class GcEnv:
    client: Client
    tq: str
    dsn: str


def big(seed: int, n: int = 150_000) -> str:
    return base64.b64encode(random.Random(seed).randbytes(n)).decode()


@pytest.fixture(scope="module")
async def gc_env(dsn: str) -> AsyncIterator[GcEnv]:
    lg = LangGraphPlugin(graphs={})
    sl = StepledgerPlugin(dsn, langgraph=lg, external_storage=True, payload_size_threshold=1024)
    try:
        client = await Client.connect("localhost:7233", plugins=[sl])
    except Exception as e:
        pytest.skip(f"Temporal dev server not reachable: {e}")
    tq = f"gc-{uuid.uuid4().hex[:8]}"
    async with Worker(
        client, task_queue=tq, workflows=[HoldWorkflow, ChainWorkflow, DoneWorkflow], plugins=[lg]
    ):
        yield GcEnv(client, tq, dsn)


async def input_of_latest_run(client: Client, wid: str) -> Any:
    hist = await client.get_workflow_handle(wid).fetch_history()
    started = hist.events[0].workflow_execution_started_event_attributes
    return (await client.data_converter.decode(list(started.input.payloads)))[0]


async def gc(env: GcEnv, wids: list[str], **kw: Any) -> gcmod.SweepReport:
    opts: dict[str, Any] = {"retention": ZERO, "margin": ZERO, "grace": ZERO, "dry_run": False}
    opts.update(kw)
    return await sweep(env.client, env.dsn, only_workflows=wids, **opts)


async def _chain_waiting_in_run_two(env: GcEnv) -> tuple[str, str]:
    wid = f"gc-chain-{uuid.uuid4().hex[:8]}"
    first = big(1) + wid
    h = await env.client.start_workflow(
        ChainWorkflow.run, args=[first, 1], id=wid, task_queue=env.tq
    )
    for _ in range(50):
        if (await h.describe()).run_id != h.first_execution_run_id:
            break
        await asyncio.sleep(0.1)
    return wid, first + "|next"


@pytest.mark.never_skip
async def test_continue_as_new_successor_input_survives(gc_env: GcEnv) -> None:
    """(a) run 1 closed (CONTINUED_AS_NEW) and 'aged past retention'; run 2 is open. Run 2's
    input was stored under run 1; it must survive because the workflow id has an open run."""
    wid, successor_input = await _chain_waiting_in_run_two(gc_env)
    report = await gc(gc_env, [wid])
    assert report.workflows_kept_open == 1 and report.refs_expired == 0
    assert await input_of_latest_run(gc_env.client, wid) == successor_input
    await gc_env.client.get_workflow_handle(wid).signal(ChainWorkflow.finish)


@pytest.mark.never_skip
async def test_sweeping_by_age_instead_of_refs_loses_the_successor_input(
    gc_env: GcEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The planted bug: treat every workflow as expired by age. (a) must then fail."""
    wid, successor_input = await _chain_waiting_in_run_two(gc_env)

    async def by_age(client: Client, workflow_id: str, cutoff: Any) -> str:
        return "expired"

    monkeypatch.setattr(gcmod, "_workflow_expired", by_age)
    report = await gc(gc_env, [wid])
    assert report.manifests_deleted >= 1
    with pytest.raises(Exception):  # noqa: B017 - any decode failure proves the loss
        assert await input_of_latest_run(gc_env.client, wid) == successor_input
    await gc_env.client.get_workflow_handle(wid).terminate("test cleanup")


@pytest.mark.never_skip
async def test_client_stored_start_input_survives_while_open(gc_env: GcEnv) -> None:
    """(b) a large start input stored by the client (its ref has no run id) survives GC while
    the run is open."""
    wid = f"gc-hold-{uuid.uuid4().hex[:8]}"
    value = big(2) + wid
    h = await gc_env.client.start_workflow(HoldWorkflow.run, value, id=wid, task_queue=gc_env.tq)
    with psycopg.connect(gc_env.dsn) as conn:
        runs = conn.execute(
            "SELECT DISTINCT run_id FROM sl_payload_refs WHERE workflow_id = %s", (wid,)
        ).fetchall()
    assert ("",) in runs  # the client stored it before the run id existed
    report = await gc(gc_env, [wid])
    assert report.workflows_kept_open == 1 and report.refs_expired == 0
    assert await input_of_latest_run(gc_env.client, wid) == value
    await h.signal(HoldWorkflow.finish)
    assert await h.result() == len(value)


def _payload(seed: int) -> Payload:
    data = f'"{big(seed, 120_000)}-{uuid.uuid4()}"'.encode()
    return Payload(metadata={"encoding": b"json/plain"}, data=data)


def _ctx(wid: str) -> StorageDriverStoreContext:
    return StorageDriverStoreContext(target=StorageDriverWorkflowInfo(namespace="default", id=wid))


@pytest.mark.never_skip
@pytest.mark.parametrize("phase", ["refs_expired", "manifests_deleted"])
async def test_store_racing_the_sweep_is_never_lost(gc_env: GcEnv, phase: str) -> None:
    """(c) a store() that dedupes against a payload the sweep has just unreferenced runs while
    the sweep is paused at `phase`; afterwards the payload retrieves correctly."""
    driver = DedupStorageDriver(PostgresChunkBackend(gc_env.dsn))
    closed = f"gc-closed-{uuid.uuid4().hex[:8]}"
    h = await gc_env.client.start_workflow(DoneWorkflow.run, "x", id=closed, task_queue=gc_env.tq)
    await h.result()
    payload = _payload(3)
    (claim,) = await driver.store(_ctx(closed), [payload])
    with psycopg.connect(gc_env.dsn) as conn:  # everything about it last touched a day ago
        c = claim.claim_data["claim"]
        conn.execute(
            "UPDATE sl_payload_refs SET last_ref_at = now() - interval '1 day' WHERE claim = %s",
            (c,),
        )
        conn.execute(
            "UPDATE sl_chunks SET last_ref_at = now() - interval '1 day' WHERE hash = ANY("
            " SELECT unnest(chunks) FROM sl_payloads WHERE claim = %s)",
            (c,),
        )
        conn.execute(
            "UPDATE sl_payloads SET last_ref_at = now() - interval '1 day' WHERE claim = %s", (c,)
        )

    other = f"gc-racer-{uuid.uuid4().hex[:8]}"

    async def barrier(p: str) -> None:
        if p == phase:  # the sweep pauses here while a racing store lands
            (again,) = await driver.store(_ctx(other), [payload])
            assert again.claim_data == claim.claim_data

    await gc(gc_env, [closed], grace=timedelta(hours=1), hook=barrier)
    (back,) = await driver.retrieve(StorageDriverRetrieveContext(), [claim])
    assert back == payload
    await driver.close()


async def test_retention_rules(gc_env: GcEnv) -> None:
    """An open workflow and a closed-within-retention one keep their payloads; an expired closed
    workflow loses its unique chunks while chunks it shares with a live one survive."""
    shared = big(9, 400_000)
    live = f"gc-live-{uuid.uuid4().hex[:8]}"
    old = f"gc-old-{uuid.uuid4().hex[:8]}"
    recent = f"gc-recent-{uuid.uuid4().hex[:8]}"
    hl = await gc_env.client.start_workflow(
        HoldWorkflow.run, shared + big(10, 60_000), id=live, task_queue=gc_env.tq
    )
    old_input = shared + big(11, 60_000)
    await (
        await gc_env.client.start_workflow(
            DoneWorkflow.run, old_input, id=old, task_queue=gc_env.tq
        )
    ).result()
    await (
        await gc_env.client.start_workflow(
            DoneWorkflow.run, big(12), id=recent, task_queue=gc_env.tq
        )
    ).result()

    kept = await gc(gc_env, [recent], retention=timedelta(days=1))
    assert kept.workflows_kept_retained == 1 and kept.manifests_deleted == 0
    assert await input_of_latest_run(gc_env.client, recent) == big(12)

    def chunks_of(wid: str) -> set[bytes]:
        with psycopg.connect(gc_env.dsn) as conn:
            rows = conn.execute(
                "SELECT DISTINCT unnest(p.chunks) FROM sl_payloads p JOIN sl_payload_refs r"
                " USING (claim) WHERE r.workflow_id = %s",
                (wid,),
            ).fetchall()
        return {bytes(r[0]) for r in rows}

    live_chunks, old_chunks = chunks_of(live), chunks_of(old)
    assert live_chunks & old_chunks  # the shared prefix dedupes across the two workflows
    report = await gc(gc_env, [old, live])
    assert report.workflows_expired == 1 and report.workflows_kept_open == 1
    with psycopg.connect(gc_env.dsn) as conn:
        present = {
            bytes(r[0])
            for r in conn.execute(
                "SELECT hash FROM sl_chunks WHERE hash = ANY(%s)", (list(old_chunks),)
            ).fetchall()
        }
    assert present == old_chunks & live_chunks  # unique chunks gone, shared ones survive
    assert await input_of_latest_run(gc_env.client, live) == shared + big(10, 60_000)
    await hl.signal(HoldWorkflow.finish)


async def test_cli_refuses_retention_below_namespace(gc_env: GcEnv) -> None:
    ns = await namespace_retention(gc_env.client)
    assert ns > ZERO
    assert check_retention(ns - timedelta(hours=1), ns) is not None
    assert check_retention(ns, ns) is None
