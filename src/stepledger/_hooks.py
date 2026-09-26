"""Fault-injection hook points inside Stepledger's own commit path (F3, F4, F5).

A no-op unless `stepledger.testing.faults.install()` registers an injector, which only chaos
workers do. Production code never imports stepledger.testing.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

Injector = Callable[..., Awaitable[None]]
_injector: Injector | None = None


def set_injector(fn: Injector | None) -> None:
    global _injector
    _injector = fn


async def fault(point: str, **where: object) -> None:
    if _injector is not None:
        await _injector(point, **where)
