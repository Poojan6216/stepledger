"""The view equals the truth. Never skipped.

Demo 4's corpus (bench/materialize.py): seeded investigator runs with parallel supersteps,
interrupt() plus resume and effects must materialize EXACT and equal to the workflow's result;
the declared-gap variants (workflow-side node, within-run cache hit, a continue-as-new run served
entirely from the task cache) must report GAP and never a wrong EXACT; the cached continue-as-new
run must be EXACT with chain=True.

    MATERIALIZE_RUNS=100 (default)
"""

from __future__ import annotations

import os

import pytest

from bench.materialize import run_corpus

pytestmark = [pytest.mark.never_skip, pytest.mark.integration]


@pytest.mark.timeout(1800)
async def test_materialize_equals_result(dsn: str) -> None:
    out = await run_corpus(int(os.environ.get("MATERIALIZE_RUNS", "100")), seed=1894)
    print({k: v for k, v in out.items() if k != "rows"})
    assert out["unequal"] == 0, [
        r for r in out["rows"] if r["completeness"] == "EXACT" and not r["equal"]
    ]
    assert out["chain"]["unequal"] == 0
    kinds = out["by_kind"]
    assert kinds["plain"] == {"equal": kinds["plain"].get("equal", 0)}, kinds["plain"]
    for declared in ("workflow_side", "cache_hit", "continue_as_new"):
        assert set(kinds[declared]) == {"gap"}, (declared, kinds[declared])
    assert out["chain"]["equal"] == sum(kinds["continue_as_new"].values())
    cache_hit = [r for r in out["rows"] if r["kind"] == "cache_hit"]
    assert all(r["positions"] for r in cache_hit), cache_hit  # the missing position is named
