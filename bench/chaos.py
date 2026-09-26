"""Demo 2, pull the plug: seeded worker crashes and zombies, checked against Temporal's history.

    uv run python -m bench.chaos --runs 20

The 30-node agent with a fake LLM that answers differently on every attempt (the worst case for
divergence). Each run gets one seeded fault. Stepledger (SL) gets all six points F1..F6; the
baselines get the four that exist in their code path (F1, F2, F3, F6). F1..F5 kill the worker
with os._exit(137) and a supervisor restarts it; F6 hangs an attempt past its 5 s start-to-close
timeout so it writes late, as a zombie. Runs with exit faults and runs with zombies go in
separate batches so a crash never cuts a zombie's hang short.

Every run is then checked against fetched history (stepledger.testing.history): duplicate rows,
divergent rows, lost rows, orphan rows, duplicate side effects.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import shlex
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from temporalio.client import Client

from bench.common import RunConfig, Shape, connect, dsn, start, write_results
from stepledger.testing import faults
from stepledger.testing.history import Counters, check_naive, check_stepledger

ROOT = Path(__file__).resolve().parent.parent
SHAPE = Shape(nodes=30)
STEP_NODES = [
    "list_assets",
    "scan_iam",
    "scan_network",
    "enrich_cve_3",
    "enrich_cve_11",
    "enrich_cve_17",
    "score_risk",
    "summarize",
]
EFFECT_NODES = ["open_ticket", "notify_slack"]
POINTS = {
    "SL": ["F1", "F2", "F3", "F4", "F5", "F6"],
    "B1": ["F1", "F2", "F3", "F6"],
    "B1u": ["F1", "F2", "F3", "F6"],
}
TABLES = {"B1": "bench_naive_nodes", "B1u": "bench_naive_upsert"}
# 5 s start-to-close + 0.5 s retry backoff + attempt 2 + its commit on the next node: a zombie of
# an early node wakes while the run is still going, so it meets a COMMITTED row (R2).
ZOMBIE_HANG_S = 6.5


@dataclass
class RunPlan:
    workflow_id: str
    faults: list[faults.FaultSpec]

    @property
    def zombie(self) -> bool:
        return any(f.point == "F6" for f in self.faults)


def make_plan(config: str, runs: int, seed: int, session: str) -> list[RunPlan]:
    rnd = random.Random(f"{seed}-{config}")
    points = POINTS[config]
    plans = []
    for i in range(runs):
        wid = f"chaos-{config}-{session}-{i:02d}"
        point = points[i % len(points)]
        if point == "F5":
            spec = faults.FaultSpec("F5", wf=wid)
        else:
            # half the effect-capable points land on effect nodes, where retries repeat effects
            pool = EFFECT_NODES if point in ("F2", "F3") and rnd.random() < 0.5 else STEP_NODES
            node = rnd.choice(pool)
            hang = ZOMBIE_HANG_S if point == "F6" else None
            spec = faults.FaultSpec(point, wf=wid, node=node, hang=hang)
        plans.append(RunPlan(wid, [spec]))
    return plans


class SupervisedWorker:
    """Runs bench.chaos_worker as a subprocess and restarts it whenever it dies."""

    def __init__(self, config: str, task_queue: str, env: dict[str, str], extra: list[str]) -> None:
        self.args = [
            sys.executable,
            "-m",
            "bench.chaos_worker",
            "--config",
            config,
            "--task-queue",
            task_queue,
            "--nodes",
            str(SHAPE.nodes),
            *extra,
        ]
        self.env = {**os.environ, **env, "PYTHONPATH": str(ROOT)}
        self.proc: asyncio.subprocess.Process | None = None
        self.restarts = 0
        self.exit_codes: list[int] = []
        self._stopping = False
        self._task: asyncio.Task[None] | None = None

    async def _spawn(self) -> None:
        self.proc = await asyncio.create_subprocess_exec(
            *self.args,
            cwd=ROOT,
            env=self.env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        assert self.proc.stdout is not None
        while True:
            line = await asyncio.wait_for(self.proc.stdout.readline(), timeout=60)
            if not line:
                raise RuntimeError("chaos worker exited before READY")
            if line.strip() == b"READY":
                break

    async def _supervise(self) -> None:
        while not self._stopping:
            assert self.proc is not None
            code = await self.proc.wait()
            if self._stopping:
                return
            self.exit_codes.append(code)
            self.restarts += 1
            await self._spawn()

    async def start(self) -> None:
        await self._spawn()
        self._task = asyncio.create_task(self._supervise())

    async def stop(self) -> None:
        self._stopping = True
        if self.proc and self.proc.returncode is None:
            self.proc.terminate()
            try:
                await asyncio.wait_for(self.proc.wait(), timeout=20)
            except TimeoutError:
                self.proc.kill()
        if self._task:
            self._task.cancel()


@dataclass
class ConfigResult:
    config: str
    runs: int
    fault_injections: int
    faults_declared: int
    faults_fired_once: bool
    worker_restarts: int
    completed: int
    counters: Counters
    per_run: list[dict[str, Any]] = field(default_factory=list)


async def _run_batch(client: Client, tq: str, cfg: RunConfig, plans: list[RunPlan]) -> list[Any]:
    handles = [await start(client, tq, cfg, workflow_id=p.workflow_id) for p in plans]

    async def result(h: Any) -> Any:
        try:
            await asyncio.wait_for(h.result(), timeout=300)
            return h
        except Exception as e:  # recorded, not raised: a run that does not complete is a finding
            return e

    return await asyncio.gather(*(result(h) for h in handles))


async def run_config(
    config: str, runs: int, seed: int, workdir: Path, *, worker_extra: list[str] | None = None
) -> ConfigResult:
    session = uuid.uuid4().hex[:6]
    plans = make_plan(config, runs, seed, session)
    plan_file = workdir / f"plan-{config}-{session}.txt"
    log_file = workdir / f"faults-{config}-{session}.log"
    plan_file.write_text("\n".join(faults.format_spec(f) for p in plans for f in p.faults))
    tq = f"chaos-{config}-{session}"
    worker = SupervisedWorker(
        config,
        tq,
        {"STEPLEDGER_FAULTS": f"@{plan_file}", "STEPLEDGER_FAULT_LOG": str(log_file)},
        worker_extra or [],
    )
    await worker.start()
    client = await connect()
    cfg = RunConfig(
        shape=SHAPE,
        kb_per_node=1,
        vary_per_attempt=True,
        persist_mode={"B1": "naive", "B1u": "naive_upsert"}.get(config, "none"),
        effects_mode="once" if config == "SL" else "direct",
    )
    try:
        outcomes = await _run_batch(client, tq, cfg, [p for p in plans if not p.zombie])
        outcomes += await _run_batch(client, tq, cfg, [p for p in plans if p.zombie])
        await asyncio.sleep(ZOMBIE_HANG_S + 3)  # let any zombie finish its late write
    finally:
        await worker.stop()

    total = Counters()
    per_run = []
    completed = 0
    for plan in plans:
        h = client.get_workflow_handle(plan.workflow_id)
        desc = await h.describe()
        status = desc.status.name if desc.status else "UNKNOWN"
        completed += status == "COMPLETED"
        if config == "SL":
            c = await check_stepledger(client, dsn(), plan.workflow_id, desc.run_id)
        else:
            c = await check_naive(
                client, dsn(), plan.workflow_id, desc.run_id, table=TABLES[config]
            )
        total.add(c)
        per_run.append(
            {
                "workflow_id": plan.workflow_id,
                "status": status,
                "faults": [faults.format_spec(f) for f in plan.faults],
                **c.as_dict(),
                "details": c.details[:5],
            }
        )
    log = faults.read_log(log_file)
    fired = [e["spec"] for e in log]
    declared = [faults.format_spec(f) for p in plans for f in p.faults]
    return ConfigResult(
        config=config,
        runs=runs,
        fault_injections=len(fired),
        faults_declared=len(declared),
        faults_fired_once=sorted(fired) == sorted(declared),
        worker_restarts=worker.restarts,
        completed=completed,
        counters=total,
        per_run=per_run,
    )


def fmt(results: list[ConfigResult]) -> str:
    head = (
        f"{'config':<22} {'runs':>4} {'faults':>6} {'restarts':>8} {'dup rows':>8} "
        f"{'divergent':>9} {'lost':>5} {'orphan':>6} {'dup effects':>11} {'completed':>9}"
    )
    labels = {"B1": "B1 naive per-node", "B1u": "B1u naive upsert", "SL": "stepledger"}
    lines = [head]
    for r in results:
        c = r.counters
        lines.append(
            f"{labels[r.config]:<22} {r.runs:>4} {r.fault_injections:>6} {r.worker_restarts:>8} "
            f"{c.duplicate_rows:>8} {c.divergent_rows:>9} {c.lost_rows:>5} {c.orphan_rows:>6} "
            f"{c.duplicate_side_effects:>11} {r.completed:>9}"
        )
    return "\n".join(lines)


def to_json(r: ConfigResult) -> dict[str, Any]:
    return {
        "config": r.config,
        "runs": r.runs,
        "fault_injections": r.fault_injections,
        "faults_declared": r.faults_declared,
        "faults_fired_once": r.faults_fired_once,
        "worker_restarts": r.worker_restarts,
        "completed": r.completed,
        **r.counters.as_dict(),
        "per_run": r.per_run,
    }


async def main(argv: list[str]) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--runs", type=int, default=20)
    ap.add_argument("--seed", type=int, default=1894)
    ap.add_argument("--configs", nargs="+", default=["B1", "B1u", "SL"])
    ap.add_argument("--out", default="chaos")
    args = ap.parse_args(argv)
    workdir = ROOT / ".temporal" / "chaos"
    workdir.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    results = []
    for config in args.configs:
        r = await run_config(config, args.runs, args.seed, workdir)
        print(
            f"  {config}: {r.fault_injections} faults, {r.worker_restarts} restarts, "
            f"{r.completed}/{r.runs} completed",
            flush=True,
        )
        results.append(r)
    print(fmt(results))
    path = write_results(
        args.out,
        "uv run python -m bench.chaos " + shlex.join(argv),
        {
            "seed": args.seed,
            "nodes": SHAPE.nodes,
            "start_to_close_s": 5,
            "zombie_hang_s": ZOMBIE_HANG_S,
            "elapsed_s": round(time.monotonic() - t0, 1),
            "configs": [to_json(r) for r in results],
        },
    )
    print(f"wrote {path}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
