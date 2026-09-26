"""Deduplicating External Storage: DedupStorageDriver over a ChunkBackend."""

from __future__ import annotations

from temporalio.converter import ExternalStorage

from stepledger.storage.backends import (
    ChunkBackend,
    FilesystemChunkBackend,
    IntegrityError,
    MissingPayloadError,
    PostgresChunkBackend,
)
from stepledger.storage.driver import DedupStorageDriver

__all__ = [
    "ChunkBackend",
    "DedupStorageDriver",
    "FilesystemChunkBackend",
    "IntegrityError",
    "MissingPayloadError",
    "PostgresChunkBackend",
    "make_external_storage",
]


def make_external_storage(
    dsn: str,
    *,
    dedupe: bool,
    payload_size_threshold: int,
    chunk_sizes: tuple[int, int, int] | None = None,
) -> tuple[DedupStorageDriver, ExternalStorage]:
    backend = PostgresChunkBackend(dsn)
    if chunk_sizes is None:
        driver = DedupStorageDriver(backend, dedupe=dedupe)
    else:
        driver = DedupStorageDriver(backend, dedupe=dedupe, chunk_sizes=chunk_sizes)
    return driver, ExternalStorage(drivers=[driver], payload_size_threshold=payload_size_threshold)
