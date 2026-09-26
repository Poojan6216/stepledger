# Effects, the LLM journal, and the retry bill

A retried node repeats everything it did, including things outside Temporal. Stepledger stops the
repeats that matter and puts a number on the rest.

## `once()` for external side effects

```python
from stepledger import once

async def open_ticket(state, runtime):
    request = {"target": state["target"], "risk": state["risk_score"]}
    ticket = await once(
        "open_ticket",
        lambda key: jira.create(request, idempotency_key=key),   # pass the key upstream
        request=request,
        reconcile=lambda key: jira.find_by_key(key),             # optional, see below
    )
    return {"ticket_id": ticket}
```

The key is `sha256(namespace, workflow, run, seq, name, index)`, where `index` counts `once()`
calls with that name in this node execution. It is stable across retries of the node and is
handed to the function so the tool can use it as its own idempotency key where the API has one.

| Journal state | What happens |
|---|---|
| no row | record `STARTED` with a hash of the request, call the tool, record `DONE` with its result |
| `DONE`, same request | return the recorded result; a duplicate prevented, counted |
| any, different request | raise `EffectDivergence` (non-retryable): the retry is trying to do something else under the same identity |
| `STARTED` / `UNKNOWN` | an earlier attempt started it and its outcome is unknown: ask `reconcile(key)` |

`reconcile(key)` returns the effect's result if the tool shows it happened, `NOT_DONE` if the tool
shows it did not (the tool is then called again), or `None` if it cannot tell. With no answer the
effect is marked `UNKNOWN` and the node raises `UnknownEffectOutcome`, which waits, retrying every
30 seconds, until a person runs:

```bash
stepledger resolve <key> --outcome done --result '"TCK-123"'
stepledger resolve <key> --outcome not-done
```

The wait is a slow retry rather than a failed run because the key contains the run ID: a failed
run could never read the resolution.

This is at-least-once delivery with dedupe, not exactly-once. An effect whose tool ignores the key
can still repeat if the worker dies between the call and the `DONE` write; that is the case that
goes to `reconcile` or to a person. `once()` covers retries within one run; a `workflow reset`
creates a new run ID and so new keys, and only the tool's own upstream key can dedupe across it.
Results are stored as JSON, so the function must return a JSON-serializable value.

## The LLM journal

```python
from stepledger import JournaledChatModel
llm = JournaledChatModel(inner=ChatAnthropic(model="claude-haiku-4-5"))
```

Inside a tracked node, each call is keyed by (the node's ledger key, the call's index in the node
execution, a hash of the request and model parameters). A miss calls the model and writes the
response to `sl_llm_calls` before returning it (journal before use). A hit returns the journaled
response and counts a replay: no new spend, and the node continues with the first attempt's
answer, as if the crash never happened. A changed request has a different hash and is never
replayed. Outside a tracked node the wrapper is a passthrough. `journaled_call(fn, request=...)`
does the same for raw SDK clients.

It is opt-in per node, because a team may want a fresh answer when the failure was the answer.
The journal only forwards and caches; nothing in Stepledger's control path calls a model or
inspects a response (a test enforces this).

## The cost meter

Inside each node attempt, a LangChain configure hook collects `usage_metadata` from every chat
model call, whatever model it is and however it is called (directly, through a chain, or on a
worker thread). Tokens and cost land on the node's row and on its audit rows; journal replays are
counted as replays, not spend. `sl_retry_waste` sums tokens and dollars of attempts that reached
the ledger but were not the accepted attempt. An attempt that died right after its model call
never reached the ledger; with the journal on, that call is in `sl_llm_calls` and the retry
replays it instead of paying again.

Prices are USD per million tokens from `stepledger.yaml`:

```yaml
prices:
  claude-haiku-4-5: {input: 1.0, output: 5.0}
  claude-sonnet-5: {input: 2.0, output: 10.0}
```

The defaults are Anthropic's published first-party rates, checked on 2026-09-26 against
https://platform.claude.com/docs/en/about-claude/pricing. Prices change; set them for the models
you use. A call to a model with no configured price is counted in tokens and its cost is left
empty rather than guessed.
