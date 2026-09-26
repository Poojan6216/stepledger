"""D2: every private Temporal SDK / LangGraph symbol Stepledger uses lives in _compat.py and is
checked here, so an upgrade that moves or changes one fails loudly instead of at runtime."""

from __future__ import annotations

import dataclasses
import inspect
import re
from importlib.metadata import version

from packaging.version import Version

from stepledger import _compat

PINNED_TEMPORALIO = ("1.33", "1.34")  # [lower, upper) matching pyproject
PINNED_LANGGRAPH = ("1.2", "1.3")


def test_versions_are_inside_the_pinned_ranges() -> None:
    t, lg = Version(version("temporalio")), Version(version("langgraph"))
    assert Version(PINNED_TEMPORALIO[0]) <= t < Version(PINNED_TEMPORALIO[1]), t
    assert Version(PINNED_LANGGRAPH[0]) <= lg < Version(PINNED_LANGGRAPH[1]), lg


def test_no_private_imports_outside_compat() -> None:
    """Hard Rule 5: private modules (`._name`) are imported only in _compat.py."""
    import pathlib

    src = pathlib.Path(_compat.__file__).parent
    pattern = re.compile(r"^\s*(from|import)\s+(temporalio|langgraph)\S*\._", re.M)
    offenders = [
        str(p.relative_to(src))
        for p in src.rglob("*.py")
        if p.name != "_compat.py" and pattern.search(p.read_text())
    ]
    assert offenders == []


def test_activity_input_and_output_shapes() -> None:
    fields = {f.name for f in dataclasses.fields(_compat.ActivityInput)}
    assert fields == {"args", "kwargs", "langgraph_config"}
    out = {f.name for f in dataclasses.fields(_compat.ActivityOutput)}
    assert out == {"result", "langgraph_command", "langgraph_interrupts"}


def test_activity_name_reads_the_sdk_definition() -> None:
    from temporalio import activity

    @activity.defn(name="compat.test.activity")
    async def fn() -> None: ...

    @activity.defn
    async def default_name() -> None: ...

    assert _compat.activity_name(fn) == "compat.test.activity"
    assert _compat.activity_name(default_name) == "default_name"


def test_task_path_str_is_sortable_and_stable() -> None:
    assert _compat.task_path_str(("__pregel_pull", "b")) == "~__pregel_pull, b"
    assert _compat.task_path_str(("__pregel_push", 3)) == "~__pregel_push, 0000000003"
    assert _compat.task_path_str(("__pregel_push", 3)) < _compat.task_path_str(
        ("__pregel_push", 10)
    )


def test_missing_sentinel_seeds_an_empty_channel() -> None:
    from langgraph.channels import LastValue
    from langgraph.errors import EmptyChannelError

    ch = LastValue(int).from_checkpoint(_compat.MISSING)
    try:
        ch.get()
    except EmptyChannelError:
        pass
    else:
        raise AssertionError("MISSING must yield an empty channel")


def test_langgraph_used_in_this_run_reads_the_plugin_task_cache() -> None:
    from temporalio.contrib.langgraph._task_cache import set_task_cache

    assert _compat.langgraph_used_in_this_run() is False
    set_task_cache({})
    assert _compat.langgraph_used_in_this_run() is True
    set_task_cache(None)


def test_plugin_activity_names_signature() -> None:
    sig = inspect.signature(_compat.plugin_activity_names)
    assert list(sig.parameters) == ["langgraph_plugin"]
