"""Hard Rules 1-4 under worker crashes and zombies. Never skipped.

Runs the Demo 2 chaos harness (bench/chaos.py) against the live dev server: 20 runs of the
30-node agent whose fake LLM answers differently on every attempt, one seeded fault per run
across F1..F6, with a supervised worker that os._exit(137)s and restarts. Every row is checked
against Temporal's own history. Stepledger must show zero on every counter.

    CHAOS_RUNS=20 (default)   CHAOS_BASELINES=1 also runs B1 and B1u and prints their counters
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from bench.chaos import ROOT, fmt, run_config

pytestmark = [pytest.mark.chaos, pytest.mark.integration]

RUNS = int(os.environ.get("CHAOS_RUNS", "20"))


@pytest.fixture(scope="module")
def workdir() -> Path:
    d = ROOT / ".temporal" / "chaos-test"
    d.mkdir(parents=True, exist_ok=True)
    return d


@pytest.mark.never_skip
@pytest.mark.timeout(1800)
async def test_stepledger_has_no_duplicates_or_divergence(dsn: str, workdir: Path) -> None:
    result = await run_config("SL", RUNS, seed=1894, workdir=workdir)
    print("\n" + fmt([result]))
    c = result.counters
    assert result.faults_fired_once, "every declared fault must fire exactly once"
    assert result.completed == RUNS, [r for r in result.per_run if r["status"] != "COMPLETED"]
    assert c.duplicate_rows == 0, c.details
    assert c.divergent_rows == 0, c.details
    assert c.lost_rows == 0, c.details
    assert c.orphan_rows == 0, c.details
    assert c.duplicate_side_effects == 0, c.details
    assert c.wrongly_committed == 0, c.details


@pytest.mark.timeout(1800)
@pytest.mark.skipif(not os.environ.get("CHAOS_BASELINES"), reason="baselines are results, opt-in")
async def test_baselines_are_recorded_not_asserted(dsn: str, workdir: Path) -> None:
    results = [await run_config(cfg, RUNS, seed=1894, workdir=workdir) for cfg in ("B1", "B1u")]
    print("\n" + fmt(results))
