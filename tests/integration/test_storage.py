"""4.2 / 4.3: the Postgres chunk backend and the dedup driver under Temporal's DataConverter."""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import random
import uuid
from collections.abc import AsyncIterator, Sequence
from typing import Any

import psycopg
import pytest
from temporalio.api.common.v1 import Payload
from temporalio.converter import (
    DataConverter,
    ExternalStorage,
    PayloadCodec,
    StorageDriverRetrieveContext,
    StorageDriverStoreContext,
    StorageDriverWorkflowInfo,
)

from stepledger._compat import ActivityInput
from stepledger.storage import DedupStorageDriver, IntegrityError, PostgresChunkBackend

pytestmark = pytest.mark.integration


@pytest.fixture
async def driver(dsn: str) -> AsyncIterator[DedupStorageDriver]:
    d = DedupStorageDriver(PostgresChunkBackend(dsn))
    yield d
    await d.close()


def converter(driver: DedupStorageDriver, codec: PayloadCodec | None = None) -> DataConverter:
    return dataclasses.replace(
        DataConverter.default,
        payload_codec=codec,
        external_storage=ExternalStorage(drivers=[driver], payload_size_threshold=1024),
    )


def blob(n: int, seed: int) -> str:
    return base64.b64encode(random.Random(seed).randbytes(n)).decode()


def ctx(wf: str, run: str = "") -> StorageDriverStoreContext:
    return StorageDriverStoreContext(
        target=StorageDriverWorkflowInfo(namespace="default", id=wf, run_id=run or None)
    )


async def test_concurrent_store_of_one_payload(dsn: str, driver: DedupStorageDriver) -> None:
    """10 tasks for 10 workflows store the same payload at once: one manifest, no duplicate
    chunks, 10 refs."""
    payload = Payload(
        metadata={"encoding": b"json/plain"}, data=f'"{blob(300_000, 1)}-{uuid.uuid4()}"'.encode()
    )
    wfs = [f"conc-{uuid.uuid4().hex[:6]}-{i}" for i in range(10)]
    backends = [DedupStorageDriver(PostgresChunkBackend(dsn)) for _ in wfs]
    try:
        claims = await asyncio.gather(
            *(d.store(ctx(w), [payload]) for d, w in zip(backends, wfs, strict=True))
        )
    finally:
        for d in backends:
            await d.close()
    claim = claims[0][0].claim_data["claim"]
    assert all(c[0].claim_data["claim"] == claim for c in claims)
    with psycopg.connect(dsn) as conn:
        manifests = conn.execute(
            "SELECT count(*), max(cardinality(chunks)) FROM sl_payloads WHERE claim = %s", (claim,)
        ).fetchone()
        refs = conn.execute(
            "SELECT count(*) FROM sl_payload_refs WHERE claim = %s", (claim,)
        ).fetchone()
        chunk_rows = conn.execute(
            "SELECT count(*), count(DISTINCT hash) FROM sl_chunks WHERE hash = ANY("
            " SELECT unnest(chunks) FROM sl_payloads WHERE claim = %s)",
            (claim,),
        ).fetchone()
    assert manifests is not None and manifests[0] == 1
    assert refs == (10,)
    assert chunk_rows is not None and chunk_rows[0] == chunk_rows[1]


VALUES: list[Any] = [
    {"messages": [{"role": "tool", "content": blob(90_000, 2)}], "target": "acct"},
    [blob(5_000, 3) for _ in range(40)],
    blob(200_000, 4),
    {"nested": {"k": [1, 2, 3], "big": blob(70_000, 5)}, "n": None, "f": 1.5},
]


@pytest.mark.parametrize("value", VALUES, ids=["state", "list", "str", "nested"])
async def test_round_trip_through_data_converter(driver: DedupStorageDriver, value: Any) -> None:
    dc = converter(driver)
    (encoded,) = await dc.encode([value])
    assert encoded.ByteSize() < 1024  # a reference, not the value
    (decoded,) = await dc.decode([encoded])
    assert decoded == value


async def test_round_trip_activity_input(driver: DedupStorageDriver) -> None:
    dc = converter(driver)
    inp = ActivityInput(args=(VALUES[0],), kwargs={}, langgraph_config={"metadata": {"x": 1}})
    (encoded,) = await dc.encode([inp])
    (decoded,) = await dc.decode([encoded], [ActivityInput])
    assert decoded == inp


async def test_successive_states_dedupe(driver: DedupStorageDriver) -> None:
    dc = converter(driver)
    msgs: list[dict[str, str]] = []
    before = dataclasses.replace(driver.metrics)
    for i in range(10):
        msgs.append({"role": "tool", "content": blob(60_000, 100 + i)})
        await dc.encode([{"messages": list(msgs), "tag": str(uuid.uuid4())}])
    logical = driver.metrics.logical_bytes - before.logical_bytes
    unique = driver.metrics.unique_bytes - before.unique_bytes
    assert unique < logical / 3  # 10 growing states, stored mostly once


async def test_flipped_byte_is_detected(dsn: str, driver: DedupStorageDriver) -> None:
    payload = Payload(
        metadata={"encoding": b"json/plain"}, data=f'"{blob(100_000, 6)}-{uuid.uuid4()}"'.encode()
    )
    (claim,) = await driver.store(ctx("flip"), [payload])
    with psycopg.connect(dsn) as conn:
        conn.execute(
            "UPDATE sl_chunks SET data = overlay(data placing '\\x00'::bytea from 100 for 1)"
            " WHERE hash = (SELECT chunks[2] FROM sl_payloads WHERE claim = %s)",
            (claim.claim_data["claim"],),
        )
    with pytest.raises(IntegrityError):
        await driver.retrieve(StorageDriverRetrieveContext(), [claim])


class XorCodec(PayloadCodec):
    """A stand-in encryption codec: ciphertext with a non-plaintext encoding."""

    KEY = 0x5A

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [
            Payload(
                metadata={"encoding": b"binary/encrypted"},
                data=bytes(b ^ self.KEY for b in p.SerializeToString()),
            )
            for p in payloads
        ]

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        return [Payload.FromString(bytes(b ^ self.KEY for b in p.data)) for p in payloads]


async def test_encrypted_payload_is_stored_whole_and_counted(
    dsn: str, driver: DedupStorageDriver
) -> None:
    dc = converter(driver, XorCodec())
    before = driver.metrics.opaque_payloads
    value = {"secret": blob(80_000, 7), "id": str(uuid.uuid4())}
    (encoded,) = await dc.encode([value])
    assert driver.metrics.opaque_payloads == before + 1
    (decoded,) = await dc.decode([encoded])
    assert decoded == value
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            "SELECT cardinality(chunks), deduped, encoding FROM sl_payloads"
            " ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    assert row == (1, False, "binary/encrypted")
