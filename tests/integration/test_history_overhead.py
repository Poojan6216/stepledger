"""Hard Rule 11: the plugin's history cost per node is constant across 10 to 80 nodes.

Measured directly from the with-plugin history: the `stepledger-*` header bytes on every node
Activity's ActivityTaskScheduled event, plus the seal Activity's events. Headers only grow with
pending ids after failed carriers, and these runs have none, so the marginal header cost per node
between sizes may differ only by the digits of the seq and commit ids themselves. (The first node
of a run carries no commits header, a per-run constant like the seal, so the plain average per
node is not the right quantity; the marginal cost is.)
"""

from __future__ import annotations

import pytest

from bench.history_overhead import measure

pytestmark = pytest.mark.integration

TOLERANCE_BYTES = 4  # a two-digit seq and commit id against one-digit ones


async def test_plugin_history_cost_per_node_is_constant(dsn: str) -> None:
    out = await measure(samples=1)
    per_node = out["marginal_header_bytes_per_node"]
    assert all(p > 0 for p in per_node), out
    assert max(per_node) - min(per_node) <= TOLERANCE_BYTES, out
    # the seal is one Activity per run: the same size at every node count, give or take the
    # digits of node_count, accepted_count and the last commit id in its input and result
    seal = out["seal_event_bytes"]
    assert max(seal) - min(seal) <= 16, out
    # each row's direct measurement is bounded by the total the plugin added
    for r in out["rows"]:
        assert r["header_bytes"] + r["seal_event_bytes"] <= r["plugin_bytes"] + 2048, r
