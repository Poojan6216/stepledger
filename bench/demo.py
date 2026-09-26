"""The five demos, one command each.

    uv run python bench/demo.py --demo cliff        # Demo 1: reproduce #1894, and get past it
    uv run python bench/demo.py --demo chaos        # Demo 2: pull the plug
    uv run python bench/demo.py --demo growth       # Demo 3: the quiet quadratic
    uv run python bench/demo.py --demo materialize  # Demo 4: the view equals the truth
    uv run python bench/demo.py --demo cost         # Demo 5: the retry bill

Each writes its results to bench/results/<name>.json with the command that produced it. Needs
the dev environment: `scripts/dev.sh up && uv run stepledger init-db`.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEMOS = {
    "cliff": ("bench.cliff", []),
    "chaos": ("bench.chaos", ["--runs", "20"]),
    "growth": ("bench.growth", ["--configs", "B1", "B2", "B3", "B4", "--out", "growth_demo"]),
    "materialize": ("bench.materialize", ["--runs", "100"]),
    "cost": ("bench.cost", ["--runs", "10"]),
}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--demo", choices=sorted(DEMOS), required=True)
    ap.add_argument("extra", nargs=argparse.REMAINDER, help="passed to the demo")
    args = ap.parse_args()
    module_name, defaults = DEMOS[args.demo]
    import importlib

    module = importlib.import_module(module_name)
    asyncio.run(module.main(args.extra or defaults))
    if args.demo == "cost":
        _print_a_run_ledger()


def _print_a_run_ledger() -> None:
    """End with the run ledger of a journaled run that had a crash: the artifact itself."""
    import psycopg

    from stepledger.config import resolve_dsn
    from stepledger.read.ledger import run_ledger

    with psycopg.connect(resolve_dsn()) as conn:
        row = conn.execute(
            "SELECT workflow_id FROM sl_llm_calls WHERE workflow_id LIKE 'cost-journal-%'"
            " AND replays > 0 ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
    if row:
        print()
        print(run_ledger(resolve_dsn(), row[0]))


if __name__ == "__main__":
    main()
