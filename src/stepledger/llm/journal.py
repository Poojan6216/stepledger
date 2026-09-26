"""The LLM journal: a crashed attempt's model responses are replayed, not paid for again.

`JournaledChatModel(inner=model)` wraps any LangChain chat model. Inside a node Activity tracked
by Stepledger, each call is keyed by (the node's ledger key, the call's index in this node
execution, a hash of the request). A miss calls the inner model and writes the response to the
journal *before* returning it (journal before use); a hit returns the journaled response
(counted as a replay, no new spend). A changed request has a different hash, so it is never
replayed. Outside a tracked node it is a pure passthrough. `journaled_call` does the same for
raw SDK clients.

Opt-in per node: a team may prefer a fresh answer when the failure was the answer itself.
This module only forwards and caches; it never inspects or decides anything about a response.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from langchain_core.callbacks import (
    AsyncCallbackManagerForLLMRun,
    CallbackManagerForLLMRun,
)
from langchain_core.language_models.chat_models import BaseChatModel
from langchain_core.messages import BaseMessage, message_to_dict, messages_from_dict
from langchain_core.outputs import ChatGeneration, ChatResult
from psycopg.types.json import Jsonb

from stepledger.canonical import chash
from stepledger.ledger.context import current_node
from stepledger.llm.meter import REPLAYED, model_name, record


def request_hash(messages: list[BaseMessage], params: dict[str, Any]) -> str:
    return chash({"messages": [message_to_dict(m) for m in messages], "params": params})


async def _lookup(key: str) -> dict[str, Any] | None:
    node = current_node()
    assert node is not None
    async with node.store.tx() as tx:
        cur = await tx.conn.execute(
            "UPDATE sl_llm_calls SET replays = replays + 1 WHERE key = %s RETURNING response",
            (key,),
        )
        row = await cur.fetchone()
    return row[0] if row else None


async def _journal(
    key: str, rh: str, response: Any, call_idx: int, tokens: tuple[int, int], cost: Any
) -> None:
    node = current_node()
    assert node is not None
    k = node.key
    async with node.store.tx() as tx:
        await tx.conn.execute(
            "INSERT INTO sl_llm_calls (key, request_hash, response, namespace, workflow_id, run_id,"
            " seq, call_idx, tokens_in, tokens_out, cost_usd, first_attempt)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (key) DO NOTHING",
            (
                key,
                rh,
                Jsonb(response),
                k.namespace,
                k.workflow_id,
                k.run_id,
                k.seq,
                call_idx,
                tokens[0],
                tokens[1],
                cost,
                node.fence.attempt,
            ),
        )


class JournaledChatModel(BaseChatModel):
    inner: BaseChatModel

    @property
    def _llm_type(self) -> str:
        return f"journaled-{self.inner._llm_type}"

    @property
    def _identifying_params(self) -> dict[str, Any]:
        return {"inner": self.inner._identifying_params}

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        if current_node() is not None:
            raise RuntimeError("inside a tracked node, call the journaled model asynchronously")
        return self.inner._generate(messages, stop=stop, **kwargs)

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: AsyncCallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        node = current_node()
        if node is None:
            return await self.inner._agenerate(messages, stop=stop, **kwargs)
        call_idx = node.next_llm_idx()
        rh = request_hash(
            messages, {"inner": self.inner._identifying_params, "stop": stop, **kwargs}
        )
        key = node.key.llm_key(call_idx, rh)
        hit = await _lookup(key)
        if hit is not None:
            (message,) = messages_from_dict([hit])
            message.response_metadata = {**message.response_metadata, REPLAYED: True}
            return ChatResult(generations=[ChatGeneration(message=message)])
        # Calls the inner model directly (no nested callback run), so the usage is metered
        # once, through this model's own run.
        result = await self.inner._agenerate(messages, stop=stop, **kwargs)
        message = result.generations[0].message
        usage = getattr(message, "usage_metadata", None) or {}
        await _journal(
            key,
            rh,
            message_to_dict(message),
            call_idx,
            (int(usage.get("input_tokens", 0)), int(usage.get("output_tokens", 0))),
            None,
        )
        return result


async def journaled_call(
    fn: Callable[[], Awaitable[Any]],
    *,
    request: Any,
    model: str | None = None,
    usage: Callable[[Any], tuple[int, int]] | None = None,
) -> Any:
    """Journal a raw SDK call. `request` identifies the call (hashed); the response must be
    JSON-serializable. `usage(response)` returns (input_tokens, output_tokens) for the meter."""
    node = current_node()
    if node is None:
        return await fn()
    call_idx = node.next_llm_idx()
    rh = chash({"request": request, "model": model})
    key = node.key.llm_key(call_idx, rh)
    hit = await _lookup(key)
    if hit is not None:
        record(model, 0, 0, replayed=True)
        return hit
    response = await fn()
    t_in, t_out = usage(response) if usage else (0, 0)
    await _journal(key, rh, response, call_idx, (t_in, t_out), None)
    record(model, t_in, t_out)
    return response


__all__ = ["JournaledChatModel", "journaled_call", "model_name", "request_hash"]
