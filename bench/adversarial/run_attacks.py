"""Phase 7: attack strategies. Each returns a measured rate; one that errors is an error row.

uv run python -m bench.adversarial.run_attacks --all
uv run python -m bench.adversarial.run_attacks 7.1 7.4
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import shlex
import sys
import traceback
from dataclasses import asdict

from bench.adversarial.common import AttackResult
from bench.common import write_results

ATTACKS = {
    "7.1": "bench.adversarial.attack_activity_reset",
    "7.2": "bench.adversarial.attack_clock_skew",
    "7.3": "bench.adversarial.attack_workflow_side",
    "7.4": "bench.adversarial.attack_encryption",
    "7.5": "bench.adversarial.attack_non_json",
    "7.6": "bench.adversarial.attack_effect_no_idempotency",
    "7.7": "bench.adversarial.attack_short_retention",
    "7.8": "bench.adversarial.attack_workflow_reset",
}


async def run_one(attack_id: str) -> AttackResult:
    module = ATTACKS[attack_id]
    try:
        result: AttackResult = await importlib.import_module(module).run()
        return result
    except Exception as e:  # an attack that errors is reported, never dropped
        return AttackResult(
            attack_id,
            module.rsplit(".", 1)[-1],
            "",
            error=(f"{type(e).__name__}: {e}\n" + "".join(traceback.format_exc(limit=3))),
        )


async def main(argv: list[str]) -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("ids", nargs="*")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--out", default="attacks")
    args = ap.parse_args(argv)
    ids = list(ATTACKS) if args.all else args.ids
    results = []
    for aid in ids:
        r = await run_one(aid)
        status = (
            "ERROR"
            if r.error
            else ("holds" if r.holds else "BEATS IT" if r.holds is False else "measured")
        )
        print(f"{r.id} {r.name}: {status}  {r.rate}", flush=True)
        if r.error:
            print("   ", r.error.splitlines()[0])
        results.append(asdict(r))
    path = write_results(
        args.out,
        "uv run python -m bench.adversarial.run_attacks " + shlex.join(argv),
        {"attacks": results},
    )
    print(f"wrote {path}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
