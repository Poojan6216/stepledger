"""Capture one real run ledger for the README: the chaos run (from bench/results/chaos.json)
with the most retries and prevented duplicate effects.

    uv run python -m bench.ledger_sample
"""

from __future__ import annotations

import json
import shlex
import sys

import psycopg

from bench.common import RESULTS_DIR, dsn, write_results
from stepledger.read.ledger import run_ledger


def pick(wids: list[str]) -> tuple[str, str] | None:
    with psycopg.connect(dsn()) as conn:
        row = conn.execute(
            "SELECT r.workflow_id, r.run_id,"
            " (SELECT count(*) FROM sl_node_attempts a WHERE a.workflow_id = r.workflow_id"
            "   AND a.run_id = r.run_id AND a.outcome <> 'WROTE')"
            " + (SELECT count(*) FROM sl_node_attempts a WHERE a.workflow_id = r.workflow_id"
            "   AND a.run_id = r.run_id AND a.outcome = 'WROTE' GROUP BY a.seq"
            "   HAVING count(*) > 1 LIMIT 1)"
            " + (SELECT coalesce(sum(duplicates_prevented), 0) FROM sl_effects e"
            "   WHERE e.workflow_id = r.workflow_id AND e.run_id = r.run_id) AS score"
            " FROM sl_runs r WHERE r.workflow_id = ANY(%s) AND r.sealed_at IS NOT NULL"
            " ORDER BY score DESC NULLS LAST LIMIT 1",
            (wids,),
        ).fetchone()
    return (row[0], row[1]) if row else None


def main(argv: list[str]) -> None:
    chaos = json.loads((RESULTS_DIR / "chaos.json").read_text())
    wids = [r["workflow_id"] for c in chaos["configs"] if c["config"] == "SL" for r in c["per_run"]]
    picked = pick(wids)
    if picked is None:
        raise SystemExit("no sealed chaos run found in the ledger; run bench.chaos first")
    wid, run_id = picked
    text = run_ledger(dsn(), wid, run_id)
    print(text)
    path = write_results(
        "ledger_sample",
        "uv run python -m bench.ledger_sample " + shlex.join(argv),
        {"workflow_id": wid, "run_id": run_id, "ledger": text},
    )
    print(f"wrote {path}")


if __name__ == "__main__":
    main(sys.argv[1:])
