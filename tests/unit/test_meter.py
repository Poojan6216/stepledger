"""Prices match versioned model ids by prefix; unpriced models leave the cost empty."""

from __future__ import annotations

from stepledger.llm.meter import Usage, price_for

PRICES = {
    "claude-haiku-4-5": {"input": 1.0, "output": 5.0},
    "claude-haiku": {"input": 9, "output": 9},
}


def test_exact_then_longest_prefix() -> None:
    assert price_for("claude-haiku-4-5", PRICES) == PRICES["claude-haiku-4-5"]
    assert price_for("claude-haiku-4-5-20251001", PRICES) == PRICES["claude-haiku-4-5"]
    assert price_for("claude-haiku-3", PRICES) == PRICES["claude-haiku"]
    assert price_for("gpt-x", PRICES) is None and price_for(None, PRICES) is None


def test_unpriced_model_leaves_cost_empty() -> None:
    u = Usage()
    u.add("claude-haiku-4-5-20251001", 1_000_000, 0, PRICES)
    assert u.cost == 1.0
    u.add("unknown", 10, 10, PRICES)
    assert u.cost is None and u.tokens_in == 1_000_010
