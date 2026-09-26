"""Row identity (LedgerKey), the attempt fence (Fence), and effect keys."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True, order=True)
class Fence:
    """(server schedule time of this attempt, attempt number). Later attempts compare greater.

    The schedule time is stamped by the Temporal server, so it survives an Activity reset
    (which restarts the attempt counter) and worker clock skew. The attempt number breaks ties.
    """

    scheduled_at: datetime
    attempt: int

    def __post_init__(self) -> None:
        if self.scheduled_at.tzinfo is None:
            object.__setattr__(self, "scheduled_at", self.scheduled_at.replace(tzinfo=UTC))


@dataclass(frozen=True)
class LedgerKey:
    namespace: str
    workflow_id: str
    run_id: str
    seq: int

    @property
    def run(self) -> RunKey:
        return RunKey(self.namespace, self.workflow_id, self.run_id)

    def effect_key(self, name: str, idx: int) -> str:
        """Stable across attempts of this node execution; unique per (name, idx) within it."""
        parts = (self.namespace, self.workflow_id, self.run_id, str(self.seq), name, str(idx))
        return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()

    def llm_key(self, call_idx: int, request_hash: str) -> str:
        parts = (self.namespace, self.workflow_id, self.run_id, str(self.seq), str(call_idx))
        return hashlib.sha256(("\x1f".join(parts) + "\x1f" + request_hash).encode()).hexdigest()


@dataclass(frozen=True)
class RunKey:
    namespace: str
    workflow_id: str
    run_id: str

    def node(self, seq: int) -> LedgerKey:
        return LedgerKey(self.namespace, self.workflow_id, self.run_id, seq)
