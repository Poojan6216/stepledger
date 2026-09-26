"""DedupStorageDriver: a Temporal External Storage driver that stores payloads as
content-defined chunks in Postgres (or any ChunkBackend).

Successive node inputs of an accumulating agent are almost the same bytes, so storing each
distinct chunk once turns storage that grows with the square of the run into storage that grows
linearly. Every retrieve reassembles the bytes and verifies SHA-256 against the claim; a
mismatch raises and never returns data.

Payload codecs run before External Storage, so an encrypted payload arrives as ciphertext that
cannot dedupe; anything not plainly encoded is stored whole and counted as opaque.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass

from temporalio.api.common.v1 import Payload
from temporalio.converter import (
    StorageDriver,
    StorageDriverClaim,
    StorageDriverRetrieveContext,
    StorageDriverStoreContext,
)

from stepledger.storage.backends import ChunkBackend, IntegrityError, Manifest, PayloadRef
from stepledger.storage.chunking import AVG_SIZE, MAX_SIZE, MIN_SIZE, chunk, chunk_id

PLAINTEXT_ENCODINGS = frozenset({"json/plain", "json/protobuf", "binary/plain"})
DRIVER_TYPE = "stepledger.dedup"


@dataclass
class DriverMetrics:
    stores: int = 0  # payloads handed to store()
    dedupe_hits: int = 0  # whole payload already stored (only a ref was written)
    logical_bytes: int = 0  # bytes handed to store()
    unique_bytes: int = 0  # chunk bytes actually written
    deduped_bytes: int = 0  # logical bytes that needed no new chunk bytes
    opaque_payloads: int = 0  # encrypted / unknown encodings, stored whole-blob
    retrieves: int = 0


class DedupStorageDriver(StorageDriver):
    def __init__(
        self,
        backend: ChunkBackend,
        *,
        name: str = "stepledger.dedup",
        dedupe: bool = True,
        max_payload_size: int = 256 * 1024 * 1024,
        chunk_sizes: tuple[int, int, int] = (MIN_SIZE, AVG_SIZE, MAX_SIZE),
    ) -> None:
        self.backend = backend
        self._name = name
        self.dedupe = dedupe
        self.max_payload_size = max_payload_size
        self.chunk_sizes = chunk_sizes  # (min, avg, max) bytes for FastCDC
        self.metrics = DriverMetrics()

    def name(self) -> str:
        return self._name

    def type(self) -> str:
        return DRIVER_TYPE

    async def store(
        self, context: StorageDriverStoreContext, payloads: Sequence[Payload]
    ) -> list[StorageDriverClaim]:
        ref = PayloadRef.from_context(context.target)
        claims = []
        for p in payloads:
            # deterministic=True: map fields (metadata) serialize in a fixed order, so equal
            # payloads built with different insertion orders still share one claim
            data = p.SerializeToString(deterministic=True)
            if len(data) > self.max_payload_size:
                raise ValueError(f"payload of {len(data)} bytes exceeds max_payload_size")
            claim = hashlib.sha256(data).hexdigest()
            self.metrics.stores += 1
            self.metrics.logical_bytes += len(data)
            if await self.backend.touch(claim, ref):
                self.metrics.dedupe_hits += 1
                self.metrics.deduped_bytes += len(data)
            else:
                encoding = p.metadata.get("encoding", b"").decode(errors="replace") or None
                plaintext = encoding in PLAINTEXT_ENCODINGS
                if not plaintext:
                    self.metrics.opaque_payloads += 1
                if self.dedupe and plaintext:
                    lo, avg, hi = self.chunk_sizes
                    parts = chunk(data, min_size=lo, avg_size=avg, max_size=hi)
                else:
                    parts = [data]
                ids = [chunk_id(c) for c in parts]
                manifest = Manifest(claim, ids, len(data), encoding, self.dedupe and plaintext)
                new = await self.backend.put(dict(zip(ids, parts, strict=True)), manifest, ref)
                self.metrics.unique_bytes += new
                self.metrics.deduped_bytes += len(data) - new
            claims.append(
                StorageDriverClaim(
                    claim_data={"claim": claim, "hash_algorithm": "sha256", "hash_value": claim}
                )
            )
        return claims

    async def retrieve(
        self, context: StorageDriverRetrieveContext, claims: Sequence[StorageDriverClaim]
    ) -> list[Payload]:
        out = []
        for c in claims:
            claim = c.claim_data["claim"]
            _, parts = await self.backend.get(claim)
            data = b"".join(parts)
            if hashlib.sha256(data).hexdigest() != claim:
                raise IntegrityError(f"payload for claim {claim[:16]} failed its SHA-256 check")
            self.metrics.retrieves += 1
            out.append(Payload.FromString(data))
        return out

    async def close(self) -> None:
        await self.backend.close()
