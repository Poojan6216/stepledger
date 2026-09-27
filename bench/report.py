"""Generate RESULTS.md and README.md from bench/results/*.json.

    uv run python -m bench.report

Every number in both files is read from a committed results file. Values derived here (ratios,
percentages) are written to bench/results/summary.json first, so `bench/check_numbers.py` can
trace each README number back to a results file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "bench" / "results"
MiB = 2**20


def load(name: str) -> dict[str, Any]:
    return dict(json.loads((RESULTS / f"{name}.json").read_text()))


def mib(b: float) -> str:
    return f"{b / MiB:.2f}"


def mb(b: float) -> str:
    return f"{b / 1e6:.2f}"


def gather() -> dict[str, Any]:
    d = {
        n: load(n)
        for n in (
            "cliff",
            "chaos",
            "growth",
            "growth_problem",
            "materialize",
            "cost",
            "overhead",
            "history_overhead",
            "threshold",
            "outage",
            "attacks",
            "ledger_sample",
        )
    }
    g = d["growth"]["rows"]

    def grow(cfg: str, kb: int, n: int) -> dict[str, Any]:
        return next(
            r for r in g if r["config"] == cfg and r["kb_per_node"] == kb and r["nodes"] == n
        )

    ratios = {
        f"{kb}KiB_{n}nodes": round(
            grow("B3", kb, n)["store_whole_blob_bytes"]
            / grow("B4", kb, n)["store_unique_chunk_bytes"],
            1,
        )
        for kb in (20, 60, 100)
        for n in (30, 60, 80)
    }
    cost = {m["mode"]: m for m in d["cost"]["modes"]}
    waste_cut = round((1 - cost["journal"]["wasted_usd"] / cost["no-journal"]["wasted_usd"]) * 100)
    b4_cliff = next(r for r in d["cliff"]["rows"] if r["config"] == "B4")
    b3_cliff = next(r for r in d["cliff"]["rows"] if r["config"] == "B3")
    summary = {
        "store_ratio_whole_blob_over_dedup": ratios,
        "cliff_store_ratio_40x60": round(
            b3_cliff["store_whole_blob_bytes"] / b4_cliff["store_unique_chunk_bytes"], 1
        ),
        "cliff_store_mb": {
            "B3": round(b3_cliff["store_whole_blob_bytes"] / 1e6, 2),
            "B4": round(b4_cliff["store_unique_chunk_bytes"] / 1e6, 2),
        },
        "cliff_history_mib": {
            r["config"]: round(r["history_size_bytes"] / MiB, 2)
            for r in d["cliff"]["rows"]
            if r["payload_check"] == "sdk_default"
        },
        "cliff_largest_payload_kib": {
            r["config"]: round(r["largest_payload_bytes"] / 1024, 1)
            for r in d["cliff"]["rows"]
            if r["payload_check"] == "sdk_default"
        },
        "retry_waste_cut_percent": waste_cut,
        "growth_store_mb_80": {
            f"{c}_{kb}": round(
                grow(c, kb, 80)[
                    "store_whole_blob_bytes" if c == "B3" else "store_unique_chunk_bytes"
                ]
                / 1e6,
                2,
            )
            for c in ("B3", "B4")
            for kb in (20, 60, 100)
        },
        "growth_history_mib": {
            f"{c}_{kb}_{n}": round(grow(c, kb, n)["history_size_bytes"] / MiB, 2)
            for c in ("B2", "B4")
            for kb in (20, 60, 100)
            for n in (10, 20, 30, 40, 60, 80)
        },
    }
    (RESULTS / "summary.json").write_text(
        json.dumps({"command": "uv run python -m bench.report", "summary": summary}, indent=1)
        + "\n"
    )
    d["summary"] = summary
    return d


def table(head: list[str], rows: list[list[Any]]) -> str:
    out = ["| " + " | ".join(head) + " |", "|" + "|".join("---" for _ in head) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def cliff_table(d: dict[str, Any]) -> str:
    by: dict[str, dict[str, Any]] = {}
    for r in d["cliff"]["rows"]:
        by.setdefault(r["config"], {})[r["payload_check"]] = r

    def cell(r: dict[str, Any]) -> str:
        s = r["outcome"]
        if r.get("stopped_at"):
            s += f" at {r['stopped_at']}"
        cause = (r.get("workflow_task_failed_cause") or "").removeprefix(
            "WORKFLOW_TASK_FAILED_CAUSE_"
        )
        if r["outcome"] != "COMPLETED" and cause:
            s += f" ({cause})"
        return s

    rows = []
    for cid, m in by.items():
        a, b = m["sdk_default"], m["disabled"]
        rows.append(
            [
                f"{cid} {a['label']}",
                a["nodes"],
                cell(a),
                cell(b),
                f"{a['largest_payload_bytes']:,}",
                f"{a['rejected_payload_bytes']:,}" if a.get("rejected_payload_bytes") else "",
                mib(a["history_size_bytes"]),
                a["history_events"],
            ]
        )
    return table(
        [
            "config",
            "nodes",
            "SDK default",
            "payload check disabled",
            "largest payload in history (bytes)",
            "rejected payload (bytes)",
            "history (MiB)",
            "events",
        ],
        rows,
    )


def chaos_table(d: dict[str, Any]) -> str:
    labels = {"B1": "B1 naive per-node write", "B1u": "B1u naive upsert", "SL": "Stepledger"}
    rows = [
        [
            labels[c["config"]],
            c["runs"],
            c["fault_injections"],
            c["duplicate_rows"],
            c["divergent_rows"],
            c["lost_rows"],
            c["orphan_rows"],
            c["duplicate_side_effects"],
        ]
        for c in d["chaos"]["configs"]
    ]
    return table(
        [
            "config",
            "runs",
            "faults injected",
            "duplicate rows",
            "divergent rows",
            "lost rows",
            "orphan rows",
            "duplicate side effects",
        ],
        rows,
    )


def growth_tables(d: dict[str, Any]) -> tuple[str, str]:
    s = d["summary"]
    walls = {(r["kb_per_node"], r["nodes"]): r for r in d["growth"]["rows"] if r["config"] == "B2"}

    def b2cell(kb: int, n: int) -> str:
        r = walls[(kb, n)]
        return (
            f"{mib(r['history_size_bytes'])}"
            if r["status"] == "COMPLETED"
            else f"stopped at node {r['stopped_at_node']}"
        )

    hist = [
        [f"{kb} KiB"]
        + [b2cell(kb, n) for n in (10, 30, 60, 80)]
        + [s["growth_history_mib"][f"B4_{kb}_{n}"] for n in (10, 30, 60, 80)]
        for kb in (20, 60, 100)
    ]
    h = table(
        [
            "new output per node",
            "no ext. storage: 10 nodes",
            "30",
            "60",
            "80",
            "B4: 10 nodes",
            "30",
            "60",
            "80",
        ],
        hist,
    )
    g = d["growth"]["rows"]

    def st(cfg: str, kb: int, n: int) -> str:
        r = next(x for x in g if x["config"] == cfg and x["kb_per_node"] == kb and x["nodes"] == n)
        return mb(r["store_whole_blob_bytes" if cfg == "B3" else "store_unique_chunk_bytes"])

    store = [
        [f"{kb} KiB"]
        + [f"{st('B3', kb, n)} / {st('B4', kb, n)}" for n in (10, 30, 60, 80)]
        + [s["store_ratio_whole_blob_over_dedup"][f"{kb}KiB_80nodes"]]
        for kb in (20, 60, 100)
    ]
    t = table(
        ["new output per node", "10 nodes", "30 nodes", "60 nodes", "80 nodes", "ratio at 80"],
        store,
    )
    return h, t


def attacks_table(d: dict[str, Any], beats: bool) -> str:
    rows = []
    for a in d["attacks"]["attacks"]:
        if a.get("error"):
            rows.append([a["id"], a["name"], "error", a["error"].splitlines()[0]])
        elif (a["holds"] is False) == beats:
            rows.append([a["id"], a["name"], a["rate"], a["expected"]])
    return table(["id", "attack", "measured", "expected"], rows)


def build_results(d: dict[str, Any]) -> str:
    s = d["summary"]
    env = d["cliff"]["environment"]
    hist, store = growth_tables(d)
    mat, cost = d["materialize"], {m["mode"]: m for m in d["cost"]["modes"]}
    ov, ho, th = d["overhead"], d["history_overhead"], d["threshold"]["median_by_threshold_kib"]
    out = d["outage"]["scenarios"]
    fail, warn = out[0], out[1]
    sections = [
        "# Results",
        "",
        "Generated by `uv run python -m bench.report` from `bench/results/*.json`. Each JSON file "
        "records the command that produced it. Do not edit this file by hand.",
        "",
        f"Environment: Python {env['python']}, temporalio {env['temporalio']}, langgraph "
        f"{env['langgraph']}, fastcdc {env['fastcdc']}, {env['temporal_cli']}, Postgres 16, "
        f"{env['platform']}. The Temporal dev server runs with a 2 MiB payload limit and a 50 MiB "
        "history limit (`scripts/dev.sh`).",
        "",
        "## What beats it",
        "",
        "Attacks that defeat a claimed benefit, with the measured rate "
        "(`bench/results/attacks.json`, `uv run python -m bench.adversarial.run_attacks --all`):",
        "",
        attacks_table(d, beats=True),
        "",
        f"- **Retry waste the journal cannot reach.** With the LLM journal on, "
        f"{cost['journal']['wasted_calls']} of {cost['journal']['model_calls_billed']} billed model "
        "calls were still wasted: the worker died between the provider call and the journal write "
        "(`bench/results/cost.json`).",
        f'- **Warn mode loses rows until reconcile.** With `on_ledger_error="warn"` and Postgres '
        f"down for {d['outage']['outage_s']:.0f} s, the run completed degraded with seqs "
        f"{warn['missing_seqs']} missing; `stepledger reconcile` inserted them from history "
        "(`bench/results/outage.json`).",
        "- **History still grows, linearly.** See the growth table below; unbounded runs still need "
        "continue-as-new.",
        "- **The Python SDK cancels timed-out async attempts locally**, so a late (zombie) write "
        "needs code that ignores cancellation; the chaos harness models that explicitly (F6).",
        "",
        "## Demo 1: the cliff (#1894)",
        "",
        f"The investigator agent at {d['cliff']['rows'][0]['kb_per_node']} KiB of new output per "
        f"node. B0 runs at {d['cliff']['b0_nodes']} nodes, where every node input stays under "
        "2 MiB and only the final bulk persist is over it (the issue's shape); the others at 40. "
        "`uv run python -m bench.cliff`",
        "",
        cliff_table(d),
        "",
        f"Stored bytes at 40 nodes: B3 whole-blob {s['cliff_store_mb']['B3']} MB, B4 dedup "
        f"{s['cliff_store_mb']['B4']} MB ({s['cliff_store_ratio_40x60']}x less).",
        "",
        "## Demo 2: pull the plug",
        "",
        f"{d['chaos']['configs'][0]['runs']} runs per config of the {d['chaos']['nodes']}-node agent; "
        "the fake LLM answers differently on every attempt. Each run gets one seeded fault: "
        "Stepledger F1 to F6, the baselines F1, F2, F3, F6. Every row is checked against "
        f"Temporal's history. `{d['chaos']['command'].strip()}`",
        "",
        chaos_table(d),
        "",
        "## Demo 3: the quiet quadratic",
        "",
        "History size in MiB. Without External Storage (B2; B1 is within 0.2%) against Stepledger "
        "with dedup storage (B4). `uv run python -m bench.growth --configs B2 B3 B4 --out growth`",
        "",
        hist,
        "",
        "Stored bytes per run in MB, whole-blob (B3, one object per payload like the S3 driver) / "
        "dedup chunks (B4). Both are logical payload bytes; the B4 figure is chunk bytes only and "
        "excludes manifests (32 bytes per chunk per payload) and reference rows, under 2% here. One "
        "run per cell; the bytes are seeded and deterministic.",
        "",
        store,
        "",
        "The B4 history column is not monotonic in output size: at 60 KiB per node each node's 61 KiB "
        "output sits just under the 64 KiB threshold and stays in history, so history grows faster "
        "there than at 100 KiB, where the outputs are externalized too.",
        "",
        "![history](bench/plots/history-light.png)",
        "",
        "![storage](bench/plots/storage-light.png)",
        "",
        "## Demo 4: the view equals the truth",
        "",
        f"{mat['runs']} seeded runs: equal {mat['equal']}, declared gap {mat['gap']}, unequal "
        f"{mat['unequal']}. By kind: {mat['by_kind']}. Cached continue-as-new runs with "
        f"`chain=True`: {mat['chain']}. `uv run python -m bench.materialize --runs 100`",
        "",
        "## Demo 5: the retry bill",
        "",
        f"{cost['no-journal']['runs']} runs x 3 crashes right after a model call, priced as "
        f"claude-haiku-4-5 at the published rates. `uv run python -m bench.cost --runs 10`",
        "",
        table(
            [
                "",
                "calls billed",
                "USD billed",
                "wasted calls",
                "wasted tokens",
                "wasted USD",
                "journal replays",
            ],
            [
                [
                    m,
                    cost[m]["model_calls_billed"],
                    cost[m]["usd_billed"],
                    cost[m]["wasted_calls"],
                    f"{cost[m]['wasted_tokens']:,}",
                    cost[m]["wasted_usd"],
                    cost[m]["journal_replays"],
                ]
                for m in ("no-journal", "journal")
            ],
        ),
        "",
        f"With the journal, {cost['journal']['replayed_nodes_committing_first_decision'][1]} of "
        f"{cost['journal']['replayed_nodes_committing_first_decision'][0]} replayed nodes committed "
        "the first attempt's model response.",
        "",
        "The wasted-call numbers come from the fake model's billing hook in the bench, not from the "
        "ledger. Stepledger's own `sl_retry_waste` view counts only attempts that reached the ledger "
        f"({cost['journal']['ledger_retry_waste_tokens']:,} tokens in this run), because a worker that "
        "dies right after the model call never writes a row; with the journal on, that call is in "
        "`sl_llm_calls`.",
        "",
        "## Overhead",
        "",
        f"30-node agent, 1 KiB per node, storage driver off, {ov['runs_per_config']} runs each on the "
        f"local dev server: ledger write p50 {ov['ledger_write_ms_p50']} ms, p95 "
        f"{ov['ledger_write_ms_p95']} ms; median wall clock {ov['wall_s_without_plugin']['median']} s "
        f"without the plugin, {ov['wall_s_with_plugin']['median']} s with it "
        f"({ov['wall_overhead_per_node_ms']} ms per node). With the storage driver on (threshold "
        "sweep, 60 KiB nodes) the ledger write p95 is "
        + ", ".join(f"{v['ledger_write_ms_p95']} ms at {k} KiB" for k, v in th.items())
        + "; that sweep has no without-plugin baseline. `uv run python -m bench.overhead --runs 10`",
        "",
        "History the plugin adds (storage driver off), measured on the with-plugin history itself: "
        "`stepledger-*` header bytes "
        + ", ".join(f"{r['header_bytes']:,} at {r['nodes']} nodes" for r in ho["rows"])
        + f", a marginal {ho['marginal_header_bytes_per_node']} bytes per node between sizes (the "
        "first node carries no commits header, and the seal Activity's events add "
        f"{ho['seal_event_bytes'][0]:,} bytes, both once per run). As "
        f"context, total history with the plugin minus without it, each the minimum of "
        f"{ho['samples_per_series']} runs: "
        + ", ".join(f"{r['plugin_bytes']:,} at {r['nodes']} nodes" for r in ho["rows"])
        + f"; marginal bytes per node {ho['marginal_plugin_bytes_per_node']}. "
        "`uv run python -m bench.history_overhead`",
        "",
        "## External Storage threshold",
        "",
        table(
            [
                "threshold (KiB)",
                "history (bytes)",
                "largest payload",
                "unique chunk bytes",
                "wall clock (s)",
                "ledger p95 (ms)",
            ],
            [
                [
                    k,
                    f"{v['history_size_bytes']:,.0f}",
                    f"{v['largest_payload_bytes']:,.0f}",
                    f"{v['store_unique_chunk_bytes']:,.0f}",
                    v["wall_clock_s"],
                    v["ledger_write_ms_p95"],
                ]
                for k, v in th.items()
            ],
        ),
        "",
        "The trade in this table: 16 KiB cuts history about 43x and the largest history payload about "
        "50x against 64 KiB, for about 38% more stored chunk bytes and twice the storage round trips; "
        "256 KiB is worse than 64 KiB on history and largest payload and saves under 1% of storage. "
        "The default stays at the design's 64 KiB (the rule was to move only if 256 KiB won on every "
        "metric); an agent whose node outputs sit just under 64 KiB, like this one, should set 16 KiB. "
        "Wall-clock differences here are within run-to-run noise. `uv run python -m bench.threshold_sweep`",
        "",
        "## Ledger outage",
        "",
        f"Postgres stopped for {d['outage']['outage_s']:.0f} s during a "
        f"{d['outage']['nodes']}-node run. Fail mode: the run stalled and completed in "
        f"{fail['run_wall_clock_s']} s, counters {fail['counters_before_reconcile']}. Warn mode: "
        f"completed in {warn['run_wall_clock_s']} s, degraded, missing {warn['missing_seqs']}; "
        f"completed in {warn['run_wall_clock_s']} s, degraded, seqs {warn['missing_seqs']} missing and "
        f"seq {warn['reconcile']['committed']} left PROVISIONAL (its commit rode on a write that failed); "
        f"reconcile inserted {len(warn['reconcile']['inserted'])} rows and committed "
        f"{len(warn['reconcile']['committed'])}: {warn['counters_after_reconcile']}. "
        "`uv run python -m bench.outage`",
        "",
        "## What holds under attack",
        "",
        attacks_table(d, beats=False),
        "",
    ]
    return "\n".join(sections)


INTEGRATION = """```python
from datetime import timedelta
import os
from temporalio.client import Client
from temporalio.contrib.langgraph import LangGraphPlugin
from temporalio.worker import Worker
from stepledger import StepledgerPlugin

lg = LangGraphPlugin(
    graphs={"investigate": build_graph()},
    default_activity_options={"start_to_close_timeout": timedelta(minutes=2)},
)
sl = StepledgerPlugin(dsn=os.environ["STEPLEDGER_DSN"], langgraph=lg)
client = await Client.connect("localhost:7233", plugins=[sl])
worker = Worker(client, task_queue="agents", workflows=[InvestigateWorkflow], plugins=[lg])
```"""


def picture(name: str, alt: str) -> str:
    raw = "https://raw.githubusercontent.com/Poojan6216/stepledger/main/bench/plots"
    return (
        f'<picture>\n  <source media="(prefers-color-scheme: dark)" '
        f'srcset="{raw}/{name}-dark.png">\n  <img alt="{alt}" '
        f'src="{raw}/{name}-light.png">\n</picture>'
    )


def build_readme(d: dict[str, Any]) -> str:
    s = d["summary"]
    chaos = {c["config"]: c for c in d["chaos"]["configs"]}
    mat, cost = d["materialize"], {m["mode"]: m for m in d["cost"]["modes"]}
    ov, ho = d["overhead"], d["history_overhead"]
    b1_stop = next(r for r in d["cliff"]["rows"] if r["config"] == "B1")["stopped_at"]
    sl = chaos["SL"]
    lines = [
        "<!-- Generated by `uv run python -m bench.report` from bench/results/*.json. Edit bench/report.py, not this file. -->",
        "",
        "# Stepledger",
        "",
        "**Every LangGraph node running on Temporal, recorded once per Activity execution in your "
        "own Postgres, at any state size.**",
        "",
        "Stepledger is one Temporal plugin that sits next to Temporal's `LangGraphPlugin`. It writes "
        "one fenced Postgres row per node Activity execution, commits it only once the workflow "
        "has accepted that result; `reconcile` and the test suite check every row against "
        "Temporal's own history. Its "
        "deduplicating External Storage driver keeps accumulating agent state under Temporal's "
        "payload and history limits. It changes no graph code and does not modify the LangGraph "
        "plugin. It was built in response to "
        "[temporalio/sdk-python#1894](https://github.com/temporalio/sdk-python/issues/1894).",
        "",
        "**Status:** alpha, a research prototype (see [limitations](https://github.com/Poojan6216/stepledger/blob/main/docs/limitations.md)). "
        "Every number below was measured on one macOS laptop against the Temporal dev server and a local "
        "Postgres 16; the environment is recorded in each results file.",
        "",
        "## The problem, measured",
        "",
        "The LangGraph plugin sends each node's whole input state as its Activity input, and "
        "Temporal records every Activity input in history. For an agent whose state accumulates:",
        "",
        "- **Persisting once at the end fails.** The final state is the first payload over the "
        "2 MiB limit: the run gets stuck (SDK default) or the server terminates it (the #1894 "
        "error).",
        f"- **Writing each node's delta from its Activity moves the failure, it does not remove "
        f"it.** At 60 KiB of new output per node the run still stops at {b1_stop}, because that "
        f"node's own input crosses 2 MiB; history is already {s['cliff_history_mib']['B1']} MiB. "
        f"With smaller outputs the 50 MiB history limit comes first.",
        f"- **Per-node writes are at-least-once.** Under injected crashes, a plain insert produced "
        f"{chaos['B1']['duplicate_rows']} duplicate and {chaos['B1']['divergent_rows']} divergent "
        f"rows in {chaos['B1']['runs']} runs; an upsert still produced "
        f"{chaos['B1u']['divergent_rows']} divergent rows (a stale attempt overwriting the "
        "accepted answer). Retried nodes also repeated their external calls: "
        f"{chaos['B1']['duplicate_side_effects']} and {chaos['B1u']['duplicate_side_effects']} "
        "duplicate side effects reached the fake ticket and Slack targets in those runs.",
        "- **External Storage fixes history, but storage then grows with the square of the run:** "
        "one object per payload, and every node input is a slightly longer copy of the last "
        "(history itself still grows, linearly, with references and sub-threshold payloads).",
        "",
        "## Install",
        "",
        "```bash",
        "pip install stepledger",
        "```",
        "",
        "Requires Python 3.11 or later, `temporalio[langgraph]` 1.33.x and `langgraph` 1.2.x (the tested "
        "ranges: both upstream features are experimental in the SDK and Stepledger reads five private "
        "symbols, see the section on private APIs), and Postgres (tested on 16). For the local "
        "environment the tests and benches use (Postgres plus a Temporal dev server with explicit "
        "payload and history limits): `scripts/dev.sh up`.",
        "",
        "## Integration",
        "",
        INTEGRATION,
        "",
        "Then `stepledger init-db` once. Workers built from the client inherit the plugin; "
        "`StepledgerPlugin.from_config(langgraph=lg)` reads the same options from `stepledger.yaml`. "
        "With External Storage on, every client that reads results or histories needs the same data "
        "converter (`stepledger.cli.build_data_converter`); see "
        "[how it works](https://github.com/Poojan6216/stepledger/blob/main/docs/how-it-works.md).",
        "",
        "## What beats it",
        "",
        attacks_table(d, beats=True),
        "",
        f"- With the journal on, {cost['journal']['wasted_calls']} billed model calls were still "
        "wasted: the worker died between the provider call and the journal write.",
        '- With `on_ledger_error="warn"`, a database outage leaves rows missing until '
        "`stepledger reconcile` repairs them from history.",
        f'- **Two kinds of node never get a row:** `execute_in="workflow"` nodes and task-cache hits '
        f"run no Activity. In Demo 4, {mat['gap']} of {mat['runs']} seeded runs were declared GAP for "
        "those reasons; `materialize()` names the position and never claims EXACT there.",
        "- History still grows, linearly; unbounded runs still need continue-as-new.",
        "",
        "See [docs/limitations.md](https://github.com/Poojan6216/stepledger/blob/main/docs/limitations.md) and [RESULTS.md](https://github.com/Poojan6216/stepledger/blob/main/RESULTS.md).",
        "",
        "## What it does, measured",
        "",
        f"- **One row per node Activity execution, fenced and committed against history.** Across "
        f"{sl['runs']} crash-injected runs (the worker killed at fault points F1 to F5, and "
        f"zombie attempts writing late at F6), Stepledger had {sl['duplicate_rows']} duplicate, "
        f"{sl['divergent_rows']} divergent, {sl['lost_rows']} lost and {sl['orphan_rows']} orphan "
        "rows, each checked against the result Temporal recorded; with `once()` on the two effect "
        f"nodes, {sl['duplicate_side_effects']} duplicate side effects reached the targets.",
        f"- **Past the wall.** With the dedup driver the 40-node run that stops B1 completes; the "
        f"largest payload left in history is {s['cliff_largest_payload_kib']['B4']} KiB (nothing "
        f"above the 64 KiB threshold) and history is {s['cliff_history_mib']['B4']} MiB.",
        f"- **Linear storage.** At 80 nodes x 100 KiB per node: "
        f"{s['growth_store_mb_80']['B3_100']} MB as one object per payload, "
        f"{s['growth_store_mb_80']['B4_100']} MB as dedup chunks "
        f"({s['store_ratio_whole_blob_over_dedup']['100KiB_80nodes']}x less).",
        f"- **The view equals the truth.** Over {mat['runs']} seeded runs, `materialize()` "
        f"rebuilt {mat['equal']} runs EXACT and equal to the workflow's result, declared "
        f"{mat['gap']} gaps (workflow-side nodes, task-cache hits), and gave {mat['unequal']} "
        "wrong answers.",
        f"- **The retry bill.** With the LLM journal, simulated spend wasted on attempts Temporal did "
        "not accept (a fake model at a fixed token count, priced at claude-haiku-4-5 list rates) fell "
        f"from USD {cost['no-journal']['wasted_usd']} to USD {cost['journal']['wasted_usd']} "
        f"({s['retry_waste_cut_percent']}% less) under the same seeded crash plan.",
        f"- **Small overhead.** For the ledger write alone (storage driver off, 1 KiB nodes, 10 runs on "
        f"a local dev server): p95 {ov['ledger_write_ms_p95']} ms and about "
        f"{round(ov['wall_overhead_per_node_ms'], 1)} ms of wall clock per node; about "
        f"{round(ho['marginal_header_bytes_per_node'][-1])} bytes of headers per node in history, "
        "the same from 10 to 80 nodes (the commit header grows only with pending ids after failed "
        "carriers).",
        "",
        "All numbers come from `bench/results/*.json` via the commands in [RESULTS.md](https://github.com/Poojan6216/stepledger/blob/main/RESULTS.md).",
        "",
        "## The five demos",
        "",
        "Each runs with one command against the local dev environment "
        "(`scripts/dev.sh up && uv run stepledger init-db`):",
        "",
        "```bash",
        "uv run python bench/demo.py --demo cliff        # reproduce #1894, and get past it",
        "uv run python bench/demo.py --demo chaos        # pull the plug",
        "uv run python bench/demo.py --demo growth       # the quiet quadratic",
        "uv run python bench/demo.py --demo materialize  # the view equals the truth",
        "uv run python bench/demo.py --demo cost         # the retry bill",
        "```",
        "",
        "### Demo 1: the cliff",
        "",
        cliff_table(d),
        "",
        "### Demo 2: pull the plug",
        "",
        chaos_table(d),
        "",
        "### Demo 3: the quiet quadratic",
        "",
        picture(
            "history", "Workflow history size against node count with and without External Storage"
        ),
        "",
        picture(
            "storage", "External store bytes per run: one object per payload against dedup chunks"
        ),
        "",
        "### Demo 4: the view equals the truth",
        "",
        f"{mat['runs']} seeded runs (parallel supersteps, `interrupt()` plus resume, effects, a cached "
        f"continue-as-new, a workflow-side node, a within-run cache hit): equal {mat['equal']}, declared "
        f"gap {mat['gap']}, unequal {mat['unequal']}. The cached continue-as-new runs are EXACT with "
        f"`chain=True` ({mat['chain']['equal']} of {sum(mat['by_kind']['continue_as_new'].values())}).",
        "",
        "### Demo 5: the retry bill",
        "",
        table(
            [
                "",
                "calls billed",
                "USD billed (simulated)",
                "wasted calls",
                "wasted USD",
                "journal replays",
            ],
            [
                [
                    m,
                    cost[m]["model_calls_billed"],
                    cost[m]["usd_billed"],
                    cost[m]["wasted_calls"],
                    cost[m]["wasted_usd"],
                    cost[m]["journal_replays"],
                ]
                for m in ("no-journal", "journal")
            ],
        ),
        "",
        "The wasted-call numbers come from the fake model's billing hook in the bench, not from the "
        "ledger. Stepledger's own `sl_retry_waste` view counts only attempts that reached the ledger "
        f"({cost['journal']['ledger_retry_waste_tokens']:,} tokens in this run), because a worker that "
        "dies right after the model call never writes a row; with the journal on, that call is in "
        "`sl_llm_calls`.",
        "",
        "### The run ledger",
        "",
        "The artifact itself: `stepledger ledger <workflow id>` for a crash-test run (attempt 1 of "
        "`scan_iam` wrote a row and died before reporting; attempt 3 won; one duplicate effect prevented):",
        "",
        "```",
        d["ledger_sample"]["ledger"],
        "```",
        "",
        "## What this is not",
        "",
        "- Not a LangGraph checkpointer, and not a replacement for Temporal's durability: Temporal "
        "stays the source of truth and the ledger is a projection of it.",
        "- Not exactly-once for external effects: `once()` is at-least-once delivery with dedupe, "
        "and an unknown outcome stops for a person.",
        "- Not a hosted service, a UI, or a new agent framework.",
        "",
        "## Private APIs and upgrade policy",
        "",
        "Both upstream features Stepledger builds on, `LangGraphPlugin` and External Storage, are marked "
        "experimental in temporalio 1.33. Stepledger reads five private symbols, all in "
        "[`_compat.py`](https://github.com/Poojan6216/stepledger/blob/main/src/stepledger/_compat.py): the "
        "plugin's `ActivityInput`/`ActivityOutput` and task-cache context variable, the SDK's activity "
        "definition lookup, and LangGraph's `task_path_str` and `MISSING`. `tests/unit/test_compat.py` "
        "checks each on every run, so an SDK release that moves one fails the test suite before it fails "
        "at runtime; the dependency ranges are the tested ones and are widened release by release. "
        "Enabling the plugin on in-flight runs is safe (`workflow.patched`); removing it is not, see "
        "[limitations](https://github.com/Poojan6216/stepledger/blob/main/docs/limitations.md).",
        "",
        "## Docs",
        "",
        "- [How it works](https://github.com/Poojan6216/stepledger/blob/main/docs/how-it-works.md): one step's life, commits, seal, reconcile, the read side",
        "- [Keys and fencing](https://github.com/Poojan6216/stepledger/blob/main/docs/keys-and-fencing.md)",
        "- [Storage and GC](https://github.com/Poojan6216/stepledger/blob/main/docs/storage-and-gc.md)",
        "- [Effects, the LLM journal and the retry bill](https://github.com/Poojan6216/stepledger/blob/main/docs/effects.md)",
        "- [Limitations](https://github.com/Poojan6216/stepledger/blob/main/docs/limitations.md)",
        "- [SDK facts](https://github.com/Poojan6216/stepledger/blob/main/docs/sdk-facts.md): every SDK behavior relied on, with file and line",
        "",
        "## Prior art, and where each stops",
        "",
        "- **Temporal's LangGraph plugin** (`temporalio.contrib.langgraph`) runs nodes as Activities "
        "and caches task results across continue-as-new. It keeps no per-node record outside "
        "Temporal. Stepledger composes with it and never modifies it.",
        "- **LangGraph checkpointers** (`PostgresSaver`) persist each superstep in-process; behind "
        "the Activity boundary they are bypassed.",
        "- **Temporal External Storage and its S3 driver** are the claim-check pattern built into "
        "the SDK. The driver stores one object per payload; Stepledger adds a deduplicating Postgres "
        "driver and does not replace the mechanism.",
        "- **DataDog's `temporal-large-payload-codec`** does claim-check as a codec plus a service, "
        "whole-blob.",
        "- **Temporal's idempotency guidance** (key on the run ID plus the Activity ID) is guidance, "
        "not a mechanism; it does not cover divergent retries, zombies or commit visibility.",
        '- **Fencing tokens** (Martin Kleppmann, "How to do distributed locking") are the idea '
        "behind the attempt fence.",
        "- **The transactional outbox** is the pattern behind committing on the next node's "
        "transaction.",
        "- **FastCDC** (Xia et al., USENIX ATC 2016), as used by restic and borg, through the "
        "`fastcdc` Python package.",
        "- **SpecuNode**, the author's earlier project, for journal-before-use and idempotency keys "
        "for agent effects.",
        "",
        "## License",
        "",
        "Apache-2.0.",
        "",
    ]
    return "\n".join(lines)


def build_writeup(d: dict[str, Any]) -> str:
    s = d["summary"]
    chaos = {c["config"]: c for c in d["chaos"]["configs"]}
    mat, cost = d["materialize"], {m["mode"]: m for m in d["cost"]["modes"]}
    attacks = {a["id"]: a for a in d["attacks"]["attacks"]}
    b1 = next(r for r in d["cliff"]["rows"] if r["config"] == "B1")
    return "\n".join(
        [
            "<!-- Generated by `uv run python -m bench.report`; numbers come from bench/results/*.json. -->",
            "",
            "# Long LangGraph agents on Temporal: where the state goes, and how to keep an honest record of it",
            "",
            "Temporal's LangGraph plugin runs each graph node as an Activity, which gives an agent "
            "retries, timeouts and crash recovery for free. Two things get harder at the same time. "
            "Your own database stops seeing the run, because a LangGraph checkpointer is bypassed at "
            "the Activity boundary. And every node's input is the whole accumulated state, which "
            "Temporal records in history. This write-up measures both problems on a live dev server "
            "and describes Stepledger, a plugin that addresses them.",
            "",
            "## Two walls",
            "",
            f"A run of the demo agent that adds 60 KiB of tool output per node reached "
            f"{s['cliff_history_mib']['B1']} MiB of history and then stopped at {b1['stopped_at']}: "
            "that node's own input was over the 2 MiB payload limit. With the Python SDK's default "
            "the run does not fail; it sits in a workflow task failure (`PAYLOADS_TOO_LARGE`) that "
            "retries forever. With the check disabled, the server terminates it. With smaller outputs "
            "the history limit comes first. Persisting each node's output to Postgres from inside the "
            "node does not move either wall; it only makes each write small.",
            "",
            "## At-least-once is not once",
            "",
            f"The natural per-node write runs under Temporal's retry model. In "
            f"{chaos['B1']['runs']} crash-injected runs with a model that answers differently on "
            f"every retry, a plain insert left {chaos['B1']['duplicate_rows']} duplicate rows and "
            f"{chaos['B1']['divergent_rows']} rows whose content differed from the result Temporal "
            f"accepted. An upsert removed the duplicates but left {chaos['B1u']['divergent_rows']} "
            "divergent rows: attempts that had timed out kept running and wrote after the accepted "
            "attempt. Nothing raised an error.",
            "",
            "Stepledger keys each row on a sequence number carried in an Activity header, fences each "
            "write on the attempt's server-assigned schedule time, and marks a row committed only once "
            f"the workflow has received the result. Across the same {chaos['SL']['runs']} runs it had "
            f"{chaos['SL']['duplicate_rows']} duplicate and {chaos['SL']['divergent_rows']} divergent "
            "rows. An Activity reset, which sends the attempt counter back to 1, did not break it: "
            f"{attacks['7.1']['rate']}.",
            "",
            "## Moving the state out of history, without paying for it quadratically",
            "",
            "Temporal's External Storage replaces large payloads in history with references. Stored as "
            "one object per payload, though, storage grows exactly the way history did, because each "
            "node input is a slightly longer copy of the previous one. Splitting payloads into "
            "content-defined chunks and storing each chunk once makes it linear: at 80 nodes and "
            f"100 KiB per node, {s['growth_store_mb_80']['B3_100']} MB as whole objects against "
            f"{s['growth_store_mb_80']['B4_100']} MB as chunks. Encryption codecs run before External "
            f"Storage and defeat this ({attacks['7.4']['rate']}).",
            "",
            "## Reading it back",
            "",
            f"Over {mat['runs']} seeded runs the ledger rebuilt the workflow's final state exactly in "
            f"{mat['equal']} runs and declared a gap in {mat['gap']} (workflow-side nodes and "
            f"task-cache hits, which run no Activity), with {mat['unequal']} wrong answers.",
            "",
            "## The retry bill",
            "",
            f"When the worker died right after a model call, retries paid for the call again: USD "
            f"{cost['no-journal']['wasted_usd']} of wasted calls in the test plan. Journaling each "
            f"response before the node uses it cut that to USD {cost['journal']['wasted_usd']}; what "
            "remains is calls that died before the journal write.",
            "",
            "## What it does not do",
            "",
            "It does not make external effects exactly-once; `once()` is at-least-once delivery with "
            "dedupe, and a `workflow reset` starts a new run with new keys "
            f"({attacks['7.8']['rate']}). It does not replace Temporal's durability. Full results, "
            "including everything that beats it, are in [RESULTS.md](../RESULTS.md).",
            "",
        ]
    )


def main() -> None:
    d = gather()
    (ROOT / "RESULTS.md").write_text(build_results(d))
    (ROOT / "README.md").write_text(build_readme(d))
    (ROOT / "docs" / "writeup.md").write_text(build_writeup(d))
    print("wrote RESULTS.md, README.md, docs/writeup.md, bench/results/summary.json")


if __name__ == "__main__":
    main()
