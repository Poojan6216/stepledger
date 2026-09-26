"""A deterministic LangChain chat model for tests and benches.

`FakeLLM(seed, bytes_per_call, tokens_per_call, vary_per_attempt)` answers each request with
text derived from `(seed, request)`. With `vary_per_attempt=True` the Activity attempt number
joins the seed, which models a real model answering differently when a node is retried.
Every reply carries `usage_metadata`, so the cost meter can price it.
"""

from __future__ import annotations

import base64
import hashlib
import random
from collections.abc import Sequence
from typing import Any

from langchain_core.callbacks import (
    AsyncCallbackManagerForLLMRun,
    CallbackManagerForLLMRun,
)
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult


def current_attempt() -> int:
    """The Activity attempt number, or 1 outside an Activity."""
    from temporalio import activity

    return activity.info().attempt if activity.in_activity() else 1


def fake_bytes(n: int, *key: object) -> str:
    """`n` bytes of non-repeating ASCII text, deterministic in `key` (base64 of seeded bytes)."""
    seed = hashlib.sha256("\x1f".join(map(str, key)).encode()).digest()
    raw = random.Random(seed).randbytes((n * 3 + 3) // 4)
    return base64.b64encode(raw).decode()[:n]


class FakeLLM(BaseChatModel):
    seed: int = 0
    bytes_per_call: int = 256
    tokens_per_call: int = 100
    vary_per_attempt: bool = False
    model_name: str = "fake-llm"

    @property
    def _llm_type(self) -> str:
        return "stepledger-fake"

    @property
    def _identifying_params(self) -> dict[str, Any]:
        return {"model_name": self.model_name, "seed": self.seed}

    def _reply(self, messages: Sequence[BaseMessage]) -> ChatResult:
        request = "\x1e".join(f"{m.type}:{m.content}" for m in messages)
        attempt = current_attempt() if self.vary_per_attempt else 1
        text = fake_bytes(self.bytes_per_call, self.seed, request, attempt)
        message = AIMessage(
            content=text,
            usage_metadata={
                "input_tokens": self.tokens_per_call,
                "output_tokens": self.tokens_per_call,
                "total_tokens": 2 * self.tokens_per_call,
            },
            response_metadata={"model_name": self.model_name},
        )
        return ChatResult(generations=[ChatGeneration(message=message)])

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        return self._reply(messages)

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        return self._reply(messages)
