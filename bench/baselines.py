"""The six configurations every bench compares (build spec, section 12.1 of the design doc).

B0   bulk persist at end          the reporter's first attempt
B1   naive per-node write         the reporter's leading fix
B1u  naive per-node upsert        what a careful engineer adds next
B2   stepledger, no ext storage   correct persistence alone
B3   stepledger + whole-blob      one object per payload (the S3 driver's storage behavior)
B4   stepledger + dedup chunks    the full design
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

from bench.common import RunConfig


@dataclass(frozen=True)
class Baseline:
    id: str
    label: str
    persist_mode: str = "none"
    persist: str = "none"
    stepledger: dict[str, Any] | None = field(default=None, hash=False)

    def run_config(self, base: RunConfig) -> RunConfig:
        return replace(base, persist_mode=self.persist_mode, persist=self.persist)


BASELINES: dict[str, Baseline] = {
    b.id: b
    for b in [
        Baseline("B0", "bulk persist at end", persist="bulk"),
        Baseline("B1", "naive per-node write", persist_mode="naive"),
        Baseline("B1u", "naive upsert", persist_mode="naive_upsert"),
        Baseline("B2", "stepledger (no ext storage)", stepledger={"external_storage": False}),
        Baseline(
            "B3",
            "stepledger + whole-blob storage",
            stepledger={"external_storage": True, "dedupe": False},
        ),
        Baseline(
            "B4",
            "stepledger + dedup storage",
            stepledger={"external_storage": True, "dedupe": True},
        ),
    ]
}


def client_plugins(baseline: Baseline, langgraph: Any, dsn: str) -> list[Any]:
    """Client plugins for a baseline: none for B0-B1u, StepledgerPlugin for B2-B4."""
    if baseline.stepledger is None:
        return []
    from stepledger import StepledgerPlugin

    return [StepledgerPlugin(dsn=dsn, langgraph=langgraph, **baseline.stepledger)]
