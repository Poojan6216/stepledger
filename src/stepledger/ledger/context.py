"""The ledger context of the running node Activity attempt, for once() and the journal."""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from stepledger.keys import Fence, LedgerKey

if TYPE_CHECKING:
    from stepledger.ledger.store import LedgerStore


@dataclass
class NodeContext:
    key: LedgerKey
    fence: Fence
    store: LedgerStore
    effect_counters: dict[str, int] = field(default_factory=dict)
    llm_calls: int = 0

    def next_effect_idx(self, name: str) -> int:
        idx = self.effect_counters.get(name, 0)
        self.effect_counters[name] = idx + 1
        return idx

    def next_llm_idx(self) -> int:
        idx = self.llm_calls
        self.llm_calls += 1
        return idx


_current: ContextVar[NodeContext | None] = ContextVar("stepledger_node", default=None)


def current_node() -> NodeContext | None:
    return _current.get()
