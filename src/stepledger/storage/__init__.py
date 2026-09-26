"""Deduplicating External Storage: DedupStorageDriver over a ChunkBackend."""

from __future__ import annotations

from temporalio.converter import ExternalStorage

from stepledger.storage.backends import (
    ChunkBackend,
    FilesystemChunkBackend,
    IntegrityError,
    PostgresChunkBackend,
)
from stepledger.storage.driver import DedupStorageDriver

__all__ = [
    "ChunkBackend",
    "DedupStorageDriver",
    "FilesystemChunkBackend",
    "IntegrityError",
    "PostgresChunkBackend",
    "make_external_storage",
]


def make_external_storage(
    dsn: str, *, dedupe: bool, payload_size_threshold: int
) -> tuple[DedupStorageDriver, ExternalStorage]:
    driver = DedupStorageDriver(PostgresChunkBackend(dsn), dedupe=dedupe)
    return driver, ExternalStorage(drivers=[driver], payload_size_threshold=payload_size_threshold)
