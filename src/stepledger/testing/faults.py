"""Fault points F1..F6 for the chaos tests and demos (build spec, section 7).

    F1  before the node function runs                     worker dies (os._exit(137))
    F2  after the LLM call / external call, before return  worker dies
    F3  after the ledger write, before completion is reported
    F4  inside the ledger transaction (after upsert, before commit)
    F5  inside the seal Activity
    F6  zombie: the attempt hangs past its start-to-close timeout, ignoring the SDK's local
        timeout cancel, then carries on and writes

A plan is a list of specs, `F3:wf=<workflow id>:node=<name>:seq=<n>:attempt=<k>[:hang=<s>]`,
separated by commas or newlines, from $STEPLEDGER_FAULTS (or `@path` to read a file). Every
selector given must match. Each spec fires at most once: the firing is appended and fsynced to
$STEPLEDGER_FAULT_LOG before the process dies, and a restarted worker skips logged specs.

Only chaos workers call `install()`; production code reaches these points through the no-op
hook in `stepledger._hooks`.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from stepledger import _hooks

POINTS = ("F1", "F2", "F3", "F4", "F5", "F6")
DEFAULT_HANG_S = 12.0
EXIT_CODE = 137


@dataclass(frozen=True)
class FaultSpec:
    point: str
    wf: str | None = None
    node: str | None = None
    seq: int | None = None
    attempt: int | None = None
    hang: float | None = None

    @property
    def id(self) -> str:
        return format_spec(self)

    def matches(self, point: str, where: dict[str, Any]) -> bool:
        if point != self.point:
            return False
        for name in ("wf", "node", "seq", "attempt"):
            want = getattr(self, name)
            if want is not None and where.get(name) != want:
                return False
        return True


def parse(text: str) -> list[FaultSpec]:
    specs = []
    for raw in text.replace("\n", ",").split(","):
        raw = raw.strip()
        if not raw:
            continue
        point, *parts = raw.split(":")
        if point not in POINTS:
            raise ValueError(f"unknown fault point {point!r} in {raw!r}")
        kw: dict[str, Any] = {}
        for part in parts:
            k, _, v = part.partition("=")
            if k in ("seq", "attempt"):
                kw[k] = int(v)
            elif k == "hang":
                kw[k] = float(v)
            elif k in ("wf", "node"):
                kw[k] = v
            else:
                raise ValueError(f"unknown fault selector {k!r} in {raw!r}")
        specs.append(FaultSpec(point, **kw))
    return specs


def format_spec(s: FaultSpec) -> str:
    parts = [s.point]
    for name in ("wf", "node", "seq", "attempt", "hang"):
        v = getattr(s, name)
        if v is not None:
            parts.append(f"{name}={v}")
    return ":".join(parts)


def load_plan(value: str | None = None) -> list[FaultSpec]:
    value = value if value is not None else os.environ.get("STEPLEDGER_FAULTS", "")
    if value.startswith("@"):
        value = Path(value[1:]).read_text()
    return parse(value)


def read_log(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


class Injector:
    def __init__(self, plan: list[FaultSpec], log_path: Path) -> None:
        self.plan = plan
        self.log_path = log_path
        self.fired = {entry["spec"] for entry in read_log(log_path)}

    def _where(self, where: dict[str, Any]) -> dict[str, Any]:
        from temporalio import activity

        from stepledger.ledger.context import current_node

        out = dict(where)
        if activity.in_activity():
            info = activity.info()
            out.setdefault("wf", info.workflow_id)
            out.setdefault("attempt", info.attempt)
        node = current_node()
        if node is not None:
            out.setdefault("seq", node.key.seq)
        return out

    def _log(self, spec: FaultSpec, where: dict[str, Any]) -> None:
        entry = {
            "spec": spec.id,
            **asdict(spec),
            "where": where,
            "pid": os.getpid(),
            "at": time.time(),
        }
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with self.log_path.open("a") as f:
            f.write(json.dumps(entry, default=str) + "\n")
            f.flush()
            os.fsync(f.fileno())
        self.fired.add(spec.id)

    async def __call__(self, point: str, **where: Any) -> None:
        if not self.plan:
            return
        ctx = self._where(where)
        for spec in self.plan:
            if spec.id in self.fired or not spec.matches(point, ctx):
                continue
            self._log(spec, ctx)
            if point == "F6":
                await _zombie_hang(spec.hang if spec.hang is not None else DEFAULT_HANG_S)
                return
            os._exit(EXIT_CODE)


async def _zombie_hang(seconds: float) -> None:
    """Hang for `seconds`, ignoring cancellation, then carry on.

    The SDK core enforces start-to-close locally by cancelling the attempt (reason TIMED_OUT),
    so an async attempt that honors cancellation never writes late. Real zombies are code that
    does not respond: a sync node running on a thread, a swallowed CancelledError, a frozen
    process. This models them."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + seconds
    while (remaining := deadline - loop.time()) > 0:
        try:
            await asyncio.sleep(remaining)
        except asyncio.CancelledError:
            continue


_installed: Injector | None = None


def install(plan: list[FaultSpec] | None = None, log_path: str | Path | None = None) -> Injector:
    """Arm fault injection in this process (chaos workers only)."""
    global _installed
    log = Path(log_path or os.environ.get("STEPLEDGER_FAULT_LOG", ".temporal/faults.log"))
    injector = Injector(plan if plan is not None else load_plan(), log)
    _installed = injector
    _hooks.set_injector(injector)
    return injector


def uninstall() -> None:
    global _installed
    _installed = None
    _hooks.set_injector(None)


async def hit(point: str, **where: Any) -> None:
    """A fault point in user or bench code (F1, F2, and the baselines' F3/F6)."""
    if _installed is not None:
        await _installed(point, **where)
