"""7.4 Encrypted payloads: codecs run before External Storage, so the driver sees ciphertext.

A stand-in cipher (a keystream seeded by a random per-payload nonce, as real AEAD modes use) makes
equal plaintexts encrypt differently, like a real encryption codec. The same 30 x 60 KiB run with
and without it: dedupe ratio (logical bytes / unique chunk bytes) and stored bytes.
"""

from __future__ import annotations

import asyncio
import dataclasses
import os
import random
import uuid
from collections.abc import Sequence

from temporalio.api.common.v1 import Payload
from temporalio.client import Client
from temporalio.converter import DataConverter, PayloadCodec

from bench.adversarial.common import AttackResult
from bench.common import RunConfig, Shape, dsn, langgraph_plugin, running_worker, start
from bench.ledger_stats import run_stats
from stepledger import StepledgerPlugin
from stepledger.testing.history import check_stepledger

SHAPE = Shape(nodes=30)


class NonceCipher(PayloadCodec):
    """Not real cryptography: randomized ciphertext, enough to measure what dedupe sees."""

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out = []
        for p in payloads:
            nonce = os.urandom(16)
            data = p.SerializeToString()
            ks = random.Random(nonce).randbytes(len(data))
            out.append(
                Payload(
                    metadata={"encoding": b"binary/encrypted"},
                    data=nonce + bytes(a ^ b for a, b in zip(data, ks, strict=True)),
                )
            )
        return out

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out = []
        for p in payloads:
            nonce, body = p.data[:16], p.data[16:]
            ks = random.Random(nonce).randbytes(len(body))
            out.append(Payload.FromString(bytes(a ^ b for a, b in zip(body, ks, strict=True))))
        return out


async def one(encrypted: bool) -> dict[str, object]:
    lg = langgraph_plugin([SHAPE])
    sl = StepledgerPlugin(dsn(), langgraph=lg)
    base = (
        dataclasses.replace(DataConverter.default, payload_codec=NonceCipher())
        if encrypted
        else DataConverter.default
    )
    client = await Client.connect("localhost:7233", plugins=[sl], data_converter=base)
    cfg = RunConfig(shape=SHAPE, kb_per_node=60, effects_mode="once")
    async with running_worker(client, lg) as tq:
        h = await start(client, tq, cfg, workflow_id=f"atk-enc-{uuid.uuid4().hex[:8]}")
        await asyncio.wait_for(h.result(), 300)
    desc = await h.describe()
    stats = await run_stats(dsn(), h.id, desc.run_id)
    c = await check_stepledger(client, dsn(), h.id, desc.run_id)
    m = sl.storage_driver.metrics
    unique = stats["store_unique_chunk_bytes"]
    return {
        "encrypted": encrypted,
        "externalized_payloads": stats["externalized_payloads"],
        "logical_bytes": stats["store_whole_blob_bytes"],
        "unique_chunk_bytes": unique,
        "dedupe_ratio": round(stats["store_whole_blob_bytes"] / unique, 2) if unique else None,
        "opaque_payloads_counted": m.opaque_payloads,
        "ledger_counters_zero": c.zero(),
    }


async def run() -> AttackResult:
    res = AttackResult(
        "7.4",
        "Encrypted payloads",
        "dedupe is defeated (about 1x); the ledger stays correct; payloads counted as opaque",
    )
    plain, enc = await one(False), await one(True)
    res.measured = {"plaintext": plain, "encrypted": enc}
    res.rate = f"dedupe {plain['dedupe_ratio']}x plaintext vs {enc['dedupe_ratio']}x encrypted"
    # The dedupe claim does not hold under encryption: this is a documented limitation.
    res.holds = False
    return res
