"""Deduplicating External Storage (Phase 4)."""

from __future__ import annotations

from typing import Any


def make_external_storage(
    dsn: str, *, dedupe: bool, payload_size_threshold: int
) -> tuple[Any, Any]:
    raise NotImplementedError("dedup storage lands in Phase 4; pass external_storage=False")
