"""Every private Temporal SDK or LangGraph symbol Stepledger uses, in one place (Hard Rule 5, D2).

Pinned against temporalio 1.33.x and langgraph 1.2.x. tests/unit/test_compat.py fails loudly if
an upgrade moves or changes any of them. Nothing else in the package imports a private module.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import temporalio.activity
from langgraph._internal._typing import MISSING  # sentinel for channel.from_checkpoint
from langgraph.pregel._algo import task_path_str  # LangGraph's sortable task-path string
from temporalio.contrib.langgraph._activity import (  # the plugin's node Activity I/O
    ActivityInput,
    ActivityOutput,
)

__all__ = [
    "MISSING",
    "ActivityInput",
    "ActivityOutput",
    "activity_name",
    "plugin_activity_names",
    "task_path_str",
]


def activity_name(fn: Callable[..., Any]) -> str:
    """The registered name of an @activity.defn function."""
    name = temporalio.activity._Definition.must_from_callable(fn).name
    if name is None:
        raise ValueError(f"{fn!r} is a dynamic activity; it has no fixed name to track")
    return name


def plugin_activity_names(langgraph_plugin: Any) -> frozenset[str]:
    """Names of every Activity a LangGraphPlugin registered (its node and task Activities)."""
    return frozenset(activity_name(a) for a in langgraph_plugin.activities)
