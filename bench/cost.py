"""Demo 5, the retry bill: tokens and dollars paid for attempts Temporal did not accept.

    uv run python -m bench.cost --runs 10

The 30-node agent with a priced fake LLM (it reports model claude-haiku-4-5 and records every
real call in a provider-style bill, bench_llm_bill). Each run gets three seeded F2 faults: the
worker dies right after a model call, before the node returns, so Temporal retries the node.
The same plan runs with the LLM journal off and on. Waste is read from the bill against
Temporal's history: a billed call was paid for nothing when its response never reached the
attempt Temporal accepted (it came from another attempt and was not the journaled response the
accepted attempt replayed). With the journal on, the retry replays the journaled response
instead of calling the model again, and the node's committed output is the first attempt's
decision.

Dollars use the published Anthropic rates for claude-haiku-4-5 (USD 1 / 5 per 1M input / output
tokens, https://platform.claude.com/docs/en/about-claude/pricing, checked 2026-09-26); the model
itself is the fake one, so the dollars are what those tokens would have cost.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import shlex
import sys
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg

from bench.chaos import ROOT, SHAPE, STEP_NODES, SupervisedWorker
from bench.common import RunConfig, connect, dsn, start, write_results
from stepledger.ledger.history import history_nodes
from stepledger.testing import faults

PRICE_IN, PRICE_OUT = Decimal("1.0"), Decimal("5.0")  # USD per 1M tokens, claude-haiku-4-5
MODEL = "claude-haiku-4-5"
TOKENS_PER_CALL = 2000


def dollars(t_in: int, t_out: int) -> Decimal:
    return (PRICE_IN * t_in + PRICE_OUT * t_out) / Decimal(1_000_000)


async def run_mode(journal: bool, runs: int, seed: int, workdir: Path) -> dict[str, Any]:
    session = uuid.uuid4().hex[:6]
    mode = "journal" if journal else "no-journal"
    rnd = random.Random(seed)
    plans: dict[str, list[faults.FaultSpec]] = {}
    for i in range(runs):
        wid = f"cost-{mode}-{session}-{i:02d}"
        plans[wid] = [faults.FaultSpec("F2", wf=wid, node=n) for n in rnd.sample(STEP_NODES, 3)]
    plan_file = workdir / f"plan-cost-{session}.txt"
    log_file = workdir / f"faults-cost-{session}.log"
    plan_file.write_text("\n".join(faults.format_spec(f) for fs in plans.values() for f in fs))
    tq = f"cost-{session}"
    worker = SupervisedWorker(
        "SL", tq, {"STEPLEDGER_FAULTS": f"@{plan_file}", "STEPLEDGER_FAULT_LOG": str(log_file)}, []
    )
    await worker.start()
    client = await connect()
    cfg = RunConfig(
        shape=SHAPE,
        kb_per_node=1,
        vary_per_attempt=True,
        journal=journal,
        llm_model=MODEL,
        llm_tokens=TOKENS_PER_CALL,
        bill_llm=True,
        effects_mode="once",
    )
    try:
        handles = [await start(client, tq, cfg, workflow_id=w) for w in plans]
        await asyncio.gather(*(asyncio.wait_for(h.result(), 300) for h in handles))
    finally:
        await worker.stop()

    accepted: dict[tuple[str, str], int] = {}
    for wid in plans:
        desc = await client.get_workflow_handle(wid).describe()
        for n in await history_nodes(client, wid, desc.run_id):
            if n.accepted and n.attempt is not None:
                accepted[(wid, n.activity_id)] = n.attempt
    with psycopg.connect(dsn()) as conn:
        bill = conn.execute(
            "SELECT workflow_id, activity_id, attempt, tokens_in, tokens_out FROM bench_llm_bill"
            " WHERE workflow_id = ANY(%s)",
            (list(plans),),
        ).fetchall()
        replays = conn.execute(
            "SELECT coalesce(sum(replays), 0) FROM sl_llm_calls WHERE workflow_id = ANY(%s)",
            (list(plans),),
        ).fetchone()
        ledger_waste = conn.execute(
            "SELECT coalesce(sum(wasted_tokens), 0) FROM sl_retry_waste"
            " WHERE workflow_id = ANY(%s)",
            (list(plans),),
        ).fetchone()
        # 6.2: every journaled node committed the first attempt's model response
        first_decision = conn.execute(
            "SELECT count(*), count(*) FILTER (WHERE n.output_json->'result'->'messages'->0->>"
            "'content' = l.response->'data'->>'content') FROM sl_llm_calls l JOIN sl_nodes n ON"
            " n.workflow_id = l.workflow_id AND n.run_id = l.run_id AND n.seq = l.seq"
            " WHERE l.workflow_id = ANY(%s) AND l.replays > 0",
            (list(plans),),
        ).fetchone()
        # which attempt first journaled each node's model call (each node makes one call)
        journaled = {
            (wf, act): first
            for wf, act, first in conn.execute(
                "SELECT n.workflow_id, n.activity_id, l.first_attempt FROM sl_llm_calls l"
                " JOIN sl_nodes n ON n.workflow_id = l.workflow_id AND n.run_id = l.run_id"
                " AND n.seq = l.seq WHERE l.workflow_id = ANY(%s) AND l.call_idx = 0",
                (list(plans),),
            ).fetchall()
        }
    total_in = sum(r[3] for r in bill)
    total_out = sum(r[4] for r in bill)
    # A billed call is wasted when its response never reached the accepted attempt: it came
    # from another attempt and was not the journaled response the accepted attempt replayed.
    wasted = [
        r
        for r in bill
        if accepted.get((r[0], r[1])) not in (None, r[2]) and journaled.get((r[0], r[1])) != r[2]
    ]
    replayed_not_wasted = sum(
        1
        for r in bill
        if accepted.get((r[0], r[1])) not in (None, r[2]) and journaled.get((r[0], r[1])) == r[2]
    )
    w_in, w_out = sum(r[3] for r in wasted), sum(r[4] for r in wasted)
    fired = len(faults.read_log(log_file))
    return {
        "mode": mode,
        "runs": runs,
        "f2_faults_fired": fired,
        "worker_restarts": worker.restarts,
        "model_calls_billed": len(bill),
        "tokens_billed": total_in + total_out,
        "usd_billed": float(round(dollars(total_in, total_out), 6)),
        "wasted_calls": len(wasted),
        "billed_in_a_failed_attempt_then_replayed": replayed_not_wasted,
        "wasted_tokens": w_in + w_out,
        "wasted_usd": float(round(dollars(w_in, w_out), 6)),
        "journal_replays": int(replays[0]) if replays else 0,
        "ledger_retry_waste_tokens": int(ledger_waste[0]) if ledger_waste else 0,
        "replayed_nodes_committing_first_decision": list(first_decision or (0, 0)),
    }


async def main(argv: list[str]) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--runs", type=int, default=10)
    ap.add_argument("--seed", type=int, default=1894)
    ap.add_argument("--real", default=None, help="run against a real model (needs a key)")
    args = ap.parse_args(argv)
    if args.real:
        raise SystemExit(
            "--real needs ANTHROPIC_API_KEY and STEPLEDGER_BENCH_BUDGET_USD; not run in this build"
        )
    workdir = ROOT / ".temporal" / "cost"
    workdir.mkdir(parents=True, exist_ok=True)
    rows = [await run_mode(j, args.runs, args.seed, workdir) for j in (False, True)]
    for r in rows:
        print(r)
    path = write_results(
        "cost",
        "uv run python -m bench.cost " + shlex.join(argv),
        {
            "model_priced_as": MODEL,
            "price_usd_per_mtok": {"input": 1.0, "output": 5.0},
            "tokens_per_call": TOKENS_PER_CALL,
            "modes": rows,
        },
    )
    print(f"wrote {path}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
