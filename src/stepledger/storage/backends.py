"""Where chunks, manifests and references live.

    sl_chunks        content-addressed chunk bytes (hash -> data), shared by everything
    sl_payloads      manifests: claim (sha256 of the whole serialized Payload) -> ordered chunks
    sl_payload_refs  who may still need a claim: one row per (claim, namespace, workflow, run),
                     written on every store, dedupe hits included

`last_ref_at` on manifests and chunks is bumped by every store that uses them; the sweep only
deletes what no reference needs *and* no store touched within a grace window, which closes the
race with a concurrent store. Chunk rows are always locked in hash order, so concurrent stores
that share chunks cannot deadlock.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from psycopg_pool import AsyncConnectionPool
from temporalio.converter import StorageDriverActivityInfo, StorageDriverWorkflowInfo


@dataclass(frozen=True)
class PayloadRef:
    namespace: str
    workflow_id: str
    run_id: str
    target_kind: str

    @staticmethod
    def from_context(
        target: StorageDriverActivityInfo | StorageDriverWorkflowInfo | None,
    ) -> PayloadRef:
        if isinstance(target, StorageDriverWorkflowInfo):
            return PayloadRef(target.namespace, target.id or "", target.run_id or "", "workflow")
        if isinstance(target, StorageDriverActivityInfo):
            # heartbeats and standalone activities: no workflow to tie the payload to
            return PayloadRef(target.namespace, "", "", "activity")
        return PayloadRef("", "", "", "none")


@dataclass(frozen=True)
class Manifest:
    claim: str
    chunks: list[bytes]  # ordered chunk ids
    size: int
    encoding: str | None
    deduped: bool


class ChunkBackend(Protocol):
    async def touch(self, claim: str, ref: PayloadRef) -> bool:
        """If the manifest exists: bump it and its chunks, upsert the ref, return True."""
        ...

    async def put(self, chunks: Mapping[bytes, bytes], manifest: Manifest, ref: PayloadRef) -> int:
        """Store missing chunks, the manifest and the ref in one transaction; return new bytes."""
        ...

    async def get(self, claim: str) -> tuple[Manifest, list[bytes]]: ...

    async def close(self) -> None: ...


class IntegrityError(Exception):
    """A retrieved payload does not hash to its claim. Never returned as data."""


class PostgresChunkBackend:
    def __init__(self, dsn: str, *, max_size: int = 10) -> None:
        self.dsn = dsn
        self._max = max_size
        self._pool: AsyncConnectionPool | None = None
        self._lock = asyncio.Lock()

    async def _open(self) -> AsyncConnectionPool:
        async with self._lock:
            if self._pool is None:
                pool = AsyncConnectionPool(
                    self.dsn,
                    min_size=1,
                    max_size=self._max,
                    open=False,
                    timeout=10.0,
                    check=AsyncConnectionPool.check_connection,
                    kwargs={"autocommit": False, "connect_timeout": 3},
                )
                await pool.open(wait=False)
                self._pool = pool
            return self._pool

    async def close(self) -> None:
        async with self._lock:
            if self._pool is not None:
                await self._pool.close()
                self._pool = None

    async def touch(self, claim: str, ref: PayloadRef) -> bool:
        pool = await self._open()
        async with pool.connection() as conn, conn.transaction():
            cur = await conn.execute(
                "UPDATE sl_payloads SET last_ref_at = now() WHERE claim = %s RETURNING chunks",
                (claim,),
            )
            row = await cur.fetchone()
            if row is None:
                return False
            await _bump_chunks(conn, sorted(set(row[0])))
            await _upsert_ref(conn, claim, ref)
            return True

    async def put(self, chunks: Mapping[bytes, bytes], manifest: Manifest, ref: PayloadRef) -> int:
        pool = await self._open()
        hashes = sorted(chunks)
        async with pool.connection() as conn, conn.transaction():
            # Lock existing chunks in hash order and bump them; only ship the missing bytes.
            existing = set(await _bump_chunks(conn, hashes))
            missing = [h for h in hashes if h not in existing]
            new_bytes = 0
            if missing:
                cur = await conn.execute(
                    "INSERT INTO sl_chunks (hash, data, size)"
                    " SELECT h, d, length(d) FROM unnest(%s::bytea[], %s::bytea[]) AS u(h, d)"
                    " ORDER BY h"
                    " ON CONFLICT (hash) DO UPDATE SET last_ref_at = now()"
                    " RETURNING (xmax = 0), size",
                    ([h for h in missing], [chunks[h] for h in missing]),
                )
                new_bytes = sum(size for inserted, size in await cur.fetchall() if inserted)
            await conn.execute(
                "INSERT INTO sl_payloads (claim, chunks, size, encoding, deduped)"
                " VALUES (%s, %s, %s, %s, %s)"
                " ON CONFLICT (claim) DO UPDATE SET last_ref_at = now()",
                (
                    manifest.claim,
                    manifest.chunks,
                    manifest.size,
                    manifest.encoding,
                    manifest.deduped,
                ),
            )
            await _upsert_ref(conn, manifest.claim, ref)
            return new_bytes

    async def get(self, claim: str) -> tuple[Manifest, list[bytes]]:
        pool = await self._open()
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT chunks, size, encoding, deduped FROM sl_payloads WHERE claim = %s",
                (claim,),
            )
            row = await cur.fetchone()
            if row is None:
                raise KeyError(f"no stored payload for claim {claim}")
            ids, size, encoding, deduped = row
            cur = await conn.execute(
                "SELECT u.i, c.data FROM unnest(%s::bytea[]) WITH ORDINALITY AS u(h, i)"
                " LEFT JOIN sl_chunks c ON c.hash = u.h ORDER BY u.i",
                (ids,),
            )
            rows = [d for _, d in await cur.fetchall()]
            await conn.rollback()
        missing = sum(d is None for d in rows)
        if missing:
            raise IntegrityError(f"claim {claim}: {missing} chunk(s) missing")
        parts = [bytes(d) for d in rows]
        return Manifest(claim, list(ids), size, encoding, deduped), parts


async def _bump_chunks(conn: Any, hashes: Sequence[bytes]) -> list[bytes]:
    if not hashes:
        return []
    cur = await conn.execute(
        "UPDATE sl_chunks SET last_ref_at = now() WHERE hash IN ("
        " SELECT hash FROM sl_chunks WHERE hash = ANY(%s) ORDER BY hash FOR UPDATE)"
        " RETURNING hash",
        (list(hashes),),
    )
    return [bytes(r[0]) for r in await cur.fetchall()]


async def _upsert_ref(conn: Any, claim: str, ref: PayloadRef) -> None:
    await conn.execute(
        "INSERT INTO sl_payload_refs (claim, namespace, workflow_id, run_id, target_kind)"
        " VALUES (%s, %s, %s, %s, %s) ON CONFLICT (claim, namespace, workflow_id, run_id)"
        " DO UPDATE SET last_ref_at = now()",
        (claim, ref.namespace, ref.workflow_id, ref.run_id, ref.target_kind),
    )


class FilesystemChunkBackend:
    """Chunks, manifests and refs as files, for tests. Not safe across processes."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        for sub in ("chunks", "manifests", "refs"):
            (self.root / sub).mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

    def _chunk(self, h: bytes) -> Path:
        return self.root / "chunks" / h.hex()

    async def close(self) -> None:
        return None

    async def touch(self, claim: str, ref: PayloadRef) -> bool:
        async with self._lock:
            m = self.root / "manifests" / f"{claim}.json"
            if not m.is_file():
                return False
            os.utime(m)
            self._write_ref(claim, ref)
            return True

    async def put(self, chunks: Mapping[bytes, bytes], manifest: Manifest, ref: PayloadRef) -> int:
        async with self._lock:
            new = 0
            for h, data in sorted(chunks.items()):
                p = self._chunk(h)
                if not p.exists():
                    p.write_bytes(data)
                    new += len(data)
            doc = {
                "chunks": [c.hex() for c in manifest.chunks],
                "size": manifest.size,
                "encoding": manifest.encoding,
                "deduped": manifest.deduped,
            }
            (self.root / "manifests" / f"{manifest.claim}.json").write_text(json.dumps(doc))
            self._write_ref(manifest.claim, ref)
            return new

    def _write_ref(self, claim: str, ref: PayloadRef) -> None:
        d = self.root / "refs" / claim
        d.mkdir(exist_ok=True)
        name = hashlib.sha256(
            f"{ref.namespace}|{ref.workflow_id}|{ref.run_id}".encode()
        ).hexdigest()
        (d / name).write_text(json.dumps(ref.__dict__))

    async def get(self, claim: str) -> tuple[Manifest, list[bytes]]:
        m = self.root / "manifests" / f"{claim}.json"
        if not m.is_file():
            raise KeyError(f"no stored payload for claim {claim}")
        doc = json.loads(m.read_text())
        ids = [bytes.fromhex(h) for h in doc["chunks"]]
        parts = []
        for h in ids:
            p = self._chunk(h)
            if not p.is_file():
                raise IntegrityError(f"claim {claim}: chunk {h.hex()[:12]} missing")
            parts.append(p.read_bytes())
        return Manifest(claim, ids, doc["size"], doc["encoding"], doc["deduped"]), parts
