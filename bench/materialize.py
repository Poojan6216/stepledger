"""Demo 4, the view equals the truth: materialize() against the workflow's own result.

    uv run python -m bench.materialize --runs 100

Seeded runs of the investigator: parallel supersteps (0-3 scans), interrupt() plus resume,
effects, and three declared-gap variants: a continue-as-new run served entirely from the
plugin's task cache (GAP alone, EXACT with chain=True), a graph with one execute_in="workflow"
node, and a within-run task-cache hit. For each run it checks
canonical(materialize(...)) == canonical(workflow result) and counts:

    equal     EXACT and equal to the result
    gap       GAP declared (a known unledgered write)
    unequal   EXACT but different from the result: must be 0
"""

from __future__ import annotations

import argparse
import asyncio
import random
import shlex
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.worker import Worker

from bench.agents.investigator import enrich_count
from bench.agents.variants import CacheHitWorkflow, cache_hit_graph
from bench.agents.workflows import InvestigateWorkflow, persist_all
from bench.common import RunConfig, Shape, connect, dsn, write_results
from stepledger import StepledgerPlugin
from stepledger.canonical import canonical_json
from stepledger.read.materialize import materialize


@dataclass
class Spec:
    kind: str  # plain | continue_as_new | workflow_side | cache_hit
    shape: Shape | None
    seed: int

    def config(self) -> RunConfig:
        assert self.shape is not None
        return RunConfig(
            shape=self.shape, seed=self.seed, kb_per_node=1, llm_bytes=128, effects_mode="once"
        )


def corpus(n: int, seed: int) -> list[Spec]:
    rnd = random.Random(seed)
    out: list[Spec] = []
    special = {
        "continue_as_new": max(1, n // 12),
        "workflow_side": max(1, n // 16),
        "cache_hit": max(1, n // 16),
    }
    for kind, count in special.items():
        for _ in range(count):
            if kind == "cache_hit":
                out.append(Spec(kind, None, rnd.randrange(10**6)))
                continue
            shape = _random_shape(rnd)
            if kind == "workflow_side":
                shape = Shape(
                    shape.nodes,
                    shape.parallel_fanout,
                    None,
                    shape.effects,
                    (("enrich_cve_0", "workflow"),),
                )
            out.append(Spec(kind, shape, rnd.randrange(10**6)))
    while len(out) < n:
        out.append(Spec("plain", _random_shape(rnd), rnd.randrange(10**6)))
    rnd.shuffle(out)
    return out


def _random_shape(rnd: random.Random) -> Shape:
    fanout = rnd.choice([0, 1, 2, 3])
    effects = rnd.random() < 0.5
    review = rnd.random() < 0.3
    fixed = 3 + fanout + (1 if review else 0) + 2 * effects
    nodes = rnd.randint(fixed + 1, 24)
    n_enrich = enrich_count(
        nodes, parallel_fanout=fanout, interrupt_at=0 if review else None, effects=effects
    )
    interrupt_at = rnd.randint(0, n_enrich) if review else None
    return Shape(nodes=nodes, parallel_fanout=fanout, interrupt_at=interrupt_at, effects=effects)


async def _start(client: Any, tq: str, spec: Spec) -> Any:
    wid = f"mat-{spec.kind}-{uuid.uuid4().hex[:8]}"
    if spec.kind == "cache_hit":
        return await client.start_workflow(CacheHitWorkflow.run, id=wid, task_queue=tq)
    inp = spec.config().workflow_input()
    inp.continue_as_new_cached = spec.kind == "continue_as_new"
    return await client.start_workflow(InvestigateWorkflow.run, inp, id=wid, task_queue=tq)


async def check(spec: Spec, handle: Any) -> dict[str, Any]:
    result = await handle.result()
    graph = (cache_hit_graph() if spec.shape is None else spec.shape.build()).compile()
    run_id = (await handle.describe()).run_id
    m = await materialize(dsn(), graph, handle.id, run_id)
    same = canonical_json(m.state) == canonical_json(result)
    row: dict[str, Any] = {
        "kind": spec.kind,
        "workflow_id": handle.id,
        "completeness": m.completeness,
        "equal": same,
        "positions": m.positions[:3],
        "reason": m.reason,
    }
    if spec.kind == "continue_as_new":
        mc = await materialize(dsn(), graph, handle.id, run_id, chain=True)
        row["chain_completeness"] = mc.completeness
        row["chain_equal"] = canonical_json(mc.state) == canonical_json(result)
        row["chain_rows_used"] = mc.chain_rows_used
    return row


def tally(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def verdicts(key_c: str, key_e: str) -> list[str]:
        out = []
        for r in rows:
            if key_c not in r:
                continue
            if r[key_c] == "EXACT":
                out.append("equal" if r[key_e] else "unequal")
            elif r[key_c] == "GAP":
                out.append("gap")
            else:
                out.append("open")
        return out

    v = verdicts("completeness", "equal")
    chain = verdicts("chain_completeness", "chain_equal")
    by_kind: dict[str, dict[str, int]] = {}
    for r, verdict in zip(rows, v, strict=True):
        by_kind.setdefault(r["kind"], {}).setdefault(verdict, 0)
        by_kind[r["kind"]][verdict] += 1
    return {
        "runs": len(rows),
        "equal": v.count("equal"),
        "gap": v.count("gap"),
        "unequal": v.count("unequal"),
        "open": v.count("open"),
        "by_kind": by_kind,
        "chain": {
            "equal": chain.count("equal"),
            "gap": chain.count("gap"),
            "unequal": chain.count("unequal"),
        },
    }


async def run_corpus(n: int, seed: int, concurrency: int = 10) -> dict[str, Any]:
    specs = corpus(n, seed)
    shapes = {s.shape for s in specs if s.shape is not None}
    lg_all = LangGraphPlugin(
        graphs={**{s.name: s.build() for s in shapes}, "cachehit": cache_hit_graph()},
        default_activity_options={"start_to_close_timeout": timedelta(minutes=2)},
    )
    sl = StepledgerPlugin(dsn(), langgraph=lg_all)
    client = await connect([sl])
    tq = f"mat-{uuid.uuid4().hex[:8]}"
    rows: list[dict[str, Any]] = []
    async with Worker(
        client,
        task_queue=tq,
        plugins=[lg_all],
        workflows=[InvestigateWorkflow, CacheHitWorkflow],
        activities=[persist_all],
    ):
        for i in range(0, len(specs), concurrency):
            batch = specs[i : i + concurrency]
            handles = [await _start(client, tq, s) for s in batch]
            rows += await asyncio.gather(
                *(check(s, h) for s, h in zip(batch, handles, strict=True))
            )
    return {"seed": seed, **tally(rows), "rows": rows}


async def main(argv: list[str]) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--runs", type=int, default=100)
    ap.add_argument("--seed", type=int, default=1894)
    args = ap.parse_args(argv)
    t0 = time.monotonic()
    out = await run_corpus(args.runs, args.seed)
    out["elapsed_s"] = round(time.monotonic() - t0, 1)
    print(
        f"runs={out['runs']} equal={out['equal']} gap={out['gap']} unequal={out['unequal']}"
        f" open={out['open']}  by kind: {out['by_kind']}  chain: {out['chain']}"
    )
    path = write_results(
        "materialize", "uv run python -m bench.materialize " + shlex.join(argv), out
    )
    print(f"wrote {path}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
