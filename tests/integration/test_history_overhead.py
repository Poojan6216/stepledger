"""Hard Rule 11: the plugin's history cost per node is constant across 10 to 80 nodes."""

from __future__ import annotations

import pytest

from bench.history_overhead import measure

pytestmark = pytest.mark.integration


async def test_plugin_history_cost_per_node_is_constant(dsn: str) -> None:
    out = await measure()
    marginal = out["marginal_plugin_bytes_per_node"]
    mean = sum(marginal) / len(marginal)
    assert mean > 0
    # linear, not growing: every marginal per-node cost within 10% (or 16 bytes) of the mean
    assert all(abs(m - mean) <= max(16.0, 0.1 * mean) for m in marginal), out
