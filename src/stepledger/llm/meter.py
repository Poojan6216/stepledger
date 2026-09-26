"""Per-attempt token and cost meter.

Inside `CostMeter.scope()`, every LangChain chat model call made by the node (directly, through
a chain, or on a worker thread) reports its `usage_metadata` here through a LangChain configure
hook, so no model needs wrapping. Replies replayed from the LLM journal are counted as replays,
not spend. Prices are USD per 1M tokens from `stepledger.yaml`.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult
from langchain_core.tracers.context import register_configure_hook

REPLAYED = "stepledger_replayed"  # response_metadata flag set by the journal on replays
_MILLION = Decimal(1_000_000)


@dataclass
class Usage:
    tokens_in: int = 0
    tokens_out: int = 0
    cost_usd: Decimal = Decimal(0)
    calls: int = 0
    replays: int = 0
    unpriced_models: set[str] = field(default_factory=set)

    def add(
        self, model: str | None, tokens_in: int, tokens_out: int, prices: Mapping[str, Any]
    ) -> None:
        self.calls += 1
        self.tokens_in += tokens_in
        self.tokens_out += tokens_out
        price = prices.get(model or "")
        if price is None:
            self.unpriced_models.add(model or "?")
            return
        pin = Decimal(str(_field(price, "input")))
        pout = Decimal(str(_field(price, "output")))
        self.cost_usd += (pin * tokens_in + pout * tokens_out) / _MILLION

    @property
    def cost(self) -> Decimal | None:
        """Total cost, or None when any call used a model with no configured price."""
        return None if self.unpriced_models else self.cost_usd


def _field(price: Any, name: str) -> float:
    return float(price[name] if isinstance(price, Mapping) else getattr(price, name))


def model_name(message: Any, llm_output: Mapping[str, Any] | None = None) -> str | None:
    meta = getattr(message, "response_metadata", None) or {}
    return meta.get("model_name") or meta.get("model") or (llm_output or {}).get("model_name")


class _UsageHandler(BaseCallbackHandler):
    def __init__(self, usage: Usage, prices: Mapping[str, Any]) -> None:
        self.usage = usage
        self.prices = prices

    def on_llm_end(self, response: LLMResult, **kwargs: Any) -> None:
        for generations in response.generations:
            for gen in generations:
                message = getattr(gen, "message", None)
                usage = getattr(message, "usage_metadata", None)
                if not usage:
                    continue
                if (getattr(message, "response_metadata", None) or {}).get(REPLAYED):
                    self.usage.replays += 1
                    continue
                self.usage.add(
                    model_name(message, response.llm_output),
                    int(usage.get("input_tokens", 0)),
                    int(usage.get("output_tokens", 0)),
                    self.prices,
                )


_current: ContextVar[_UsageHandler | None] = ContextVar("stepledger_meter", default=None)
register_configure_hook(_current, inheritable=True)


class CostMeter:
    def __init__(self, prices: Mapping[str, Any] | None = None) -> None:
        self.prices: Mapping[str, Any] = prices or {}

    @contextmanager
    def scope(self) -> Iterator[Usage]:
        """Collect usage for everything run inside the block (one node attempt)."""
        usage = Usage()
        token = _current.set(_UsageHandler(usage, self.prices))
        try:
            yield usage
        finally:
            _current.reset(token)


def record(model: str | None, tokens_in: int, tokens_out: int, *, replayed: bool = False) -> None:
    """Report a call made without LangChain (e.g. a raw SDK client) to the active scope."""
    handler = _current.get()
    if handler is None:
        return
    if replayed:
        handler.usage.replays += 1
    else:
        handler.usage.add(model, tokens_in, tokens_out, handler.prices)
