"""Fault points F1..F6 for chaos tests and demos. Filled in by Phase 3."""

from __future__ import annotations

import os


async def hit(point: str, **where: object) -> None:
    """Fire fault `point` here if STEPLEDGER_FAULTS selects it; otherwise return at once."""
    if not os.environ.get("STEPLEDGER_FAULTS"):
        return
    raise NotImplementedError("fault injection lands in Phase 3")
