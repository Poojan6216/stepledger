"""Stepledger: a fenced, committed Postgres ledger for LangGraph nodes on Temporal."""

from __future__ import annotations

from typing import Any

__version__ = "0.1.0"

__all__ = ["JournaledChatModel", "StepledgerPlugin", "__version__", "materialize", "once"]

_LAZY = {
    "StepledgerPlugin": ("stepledger.plugin", "StepledgerPlugin"),
    "once": ("stepledger.effects.once", "once"),
    "materialize": ("stepledger.read.materialize", "materialize"),
    "JournaledChatModel": ("stepledger.llm.journal", "JournaledChatModel"),
}


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        import importlib

        module, attr = _LAZY[name]
        return getattr(importlib.import_module(module), attr)
    raise AttributeError(f"module 'stepledger' has no attribute {name!r}")
