# How Stepledger works

Stepledger is one Temporal plugin that sits next to `temporalio.contrib.langgraph.LangGraphPlugin`.
It changes no graph code and never touches `LangGraphPlugin`. It adds four things:

| Part | What it does | Where |
|---|---|---|
| Node ledger | One fenced Postgres row per node Activity execution, committed only once the workflow accepted it | `stepledger/ledger/` |
| Dedup External Storage driver | Large payloads leave history as small references; their bytes are stored once, as content-defined chunks | `stepledger/storage/` |
| Effects and the retry bill | `once()` for external side effects, an LLM journal, a per-attempt token and cost meter | `stepledger/effects/`, `stepledger/llm/` |
| Read side | `materialize()`, SQL views, the `stepledger` CLI | `stepledger/read/`, `stepledger/cli.py` |

## Wiring

```python
lg = LangGraphPlugin(
    graphs={"investigate": build_graph()},
    default_activity_options={"start_to_close_timeout": timedelta(minutes=2)},
)
sl = StepledgerPlugin(dsn=os.environ["STEPLEDGER_DSN"], langgraph=lg)
client = await Client.connect("localhost:7233", plugins=[sl])
worker = Worker(client, task_queue="agents", workflows=[InvestigateWorkflow], plugins=[lg])
```

`StepledgerPlugin` goes on the client. The SDK hands every client plugin that is also a worker
plugin to each worker built from that client, ahead of the worker's own plugins
(`temporalio/worker/_worker.py:397-411`), so the worker gets Stepledger's interceptor, its
`stepledger.seal` Activity, and the client's data converter with the storage driver. Putting it
on the worker instead would install the ledger but not the storage driver (worker plugins cannot
set a data converter). `StepledgerPlugin.from_config(langgraph=lg)` reads the same options from
`stepledger.yaml`. Run `stepledger init-db` once to create the tables and views.

### What runs where

External Storage lives in the data converter, so with it enabled every process that encodes or
decodes payloads talks to Postgres: the **workflow worker** stores each node's input and retrieves
each result during workflow tasks; the **activity worker** retrieves inputs and stores results;
and **every client** that starts workflows or reads results or histories (starters, tooling,
`stepledger reconcile`) needs the same converter, or it fails with `TMPRL1105`. Build such
clients with the plugin, or with `stepledger.cli.build_data_converter(dsn)`. The Temporal UI
shows externalized payloads as references. Replay (including the SDK's cache-eviction replays)
re-encodes commands and so re-runs `store()` for each externalized payload: a dedupe hit, three
statements and a commit each, and a Postgres outage fails replay too.

Per node, with the ledger alone: one Postgres transaction of about eight statements (advisory
locks, run row, upsert, carried commits and abandons, audit) on the activity worker. With the
storage driver: plus one store transaction per externalized payload (up to four statements) and
one retrieve (two statements) on each side.

### Slow or unavailable Postgres

The ledger pool waits up to 5 s for a connection (`connect_timeout` 3 s), the chunk store 10 s.
In fail mode a write that cannot get through fails the node Activity and Temporal retries it, so
the node re-executes (its model call too, unless journaled; the journal itself needs Postgres).
A `once()` whose `DONE` write fails after the tool call retries that write briefly, then leaves
the effect unknown for a person. Workflow tasks include the storage round trips, against the
default 10 s workflow task timeout.

### Removing the plugin

The seal is guarded by `workflow.patched("stepledger-seal-v1")`, which makes *enabling* the
plugin safe for runs already in flight. Disabling it (or `seal=False`) while runs that already
recorded the patch marker are in flight is the reverse case, which `patched()` does not cover:
finish or drain those runs first, or follow the SDK's `deprecate_patch` procedure.

## One step's life

1. **Schedule.** LangGraph asks the plugin to run node `enrich_cve_11`; the plugin calls
   `workflow.execute_activity`. Stepledger's outbound workflow interceptor sees the call for a
   tracked node Activity, takes the next number from a per-run counter (`seq = 11`), and adds
   the headers `stepledger-seq: 11` and `stepledger-commits: [10]`: the steps the workflow saw
   succeed that no successful Activity has confirmed yet. No I/O, no clock, no randomness.
2. **Offload.** The data converter serializes the Activity input. Above the threshold
   (64 KiB by default) the dedup driver stores it as chunks and history keeps a reference of a
   few hundred bytes.
3. **Execute.** A worker picks up the Activity. The converter fetches and verifies the chunks
   and rebuilds the input. The node runs, unchanged; the meter counts its model tokens.
4. **Write.** In one Postgres transaction: a fenced upsert of row `(namespace, workflow, run, 11)`
   as `PROVISIONAL`; `COMMITTED` for every seq in the commits header; an audit row for this
   attempt. If the fence rejects the write, the attempt raises instead of returning success.
5. **Complete.** The Activity returns; Temporal records `ActivityTaskCompleted`. The workflow
   interceptor notes that seq 11 succeeded; it rides on the next node's header and stays
   pending until an Activity that carried it completes.
6. **Seal.** When the workflow really ends, `stepledger.seal` commits the rest and records the
   run status and the hash of the final state. After a hard termination, `stepledger reconcile`
   does the same from Temporal's history.

## Committing: piggyback, seal, reconcile

A row becomes `COMMITTED` only when the workflow accepted the result: it was delivered to
workflow code. There are three ways that is recorded, and they cover every exit:

- **Piggyback.** The next node Activity's transaction commits what the workflow has seen
  succeed. No extra Activity, no extra round trip. Ids stay pending until a carrier succeeds, so
  a carrier that fails for good cannot lose a commit: the same ids ride on the next carrier or
  on the seal (the `UPDATE` is idempotent). The price is that a row stays `PROVISIONAL` until the
  next tracked node's ledger write or the seal: usually one step, but a run parked at an
  `interrupt()` waiting for a signal keeps its last row `PROVISIONAL` for the whole wait.
- **Seal.** At workflow exit. The workflow counts how many results it accepted (one integer).
  After the seal applies its commits, `COMMITTED` rows are exactly the accepted results that
  reached the ledger, so when that count matches, every remaining `PROVISIONAL` row is provably
  unaccepted and becomes `ABANDONED`. When it does not match (possible only with
  `on_ledger_error="warn"`), the run is marked degraded and undecided rows are left for
  `reconcile`. Writes to a sealed run are refused.
- **Reconcile.** `stepledger reconcile <workflow_id>` or `--all-open` reads history: a completed
  Activity with no earlier cancel request is committed; a completion after a cancel request is
  abandoned (the workflow never used it); a row that differs from history is rewritten from it
  and audited `DIVERGENCE_REPAIRED`; a missing row is inserted from history. Closed runs are
  sealed with the status Temporal reports. A sealed run's decisions are never changed.

## When the seal runs

The seal is classified the way the SDK ends a run (`worker/_workflow_instance.py:2748-2797`),
using public API only:

| Exception leaving the workflow | Run status | Seal? |
|---|---|---|
| `ContinueAsNewError` | `CONTINUED_AS_NEW` | yes |
| cancel requested and a cancellation error (including `ActivityError` caused by `CancelledError`) | `CANCELLED` | yes, scheduled so the cancellation cannot stop it |
| `asyncio.CancelledError` with no cancel request | `FAILED` | yes |
| `workflow.is_failure_exception(e)` | `FAILED` | yes |
| anything else | workflow **task** failure: Temporal retries the task from history | no |

The seal is gated by `workflow.patched("stepledger-seal-v1")`, so runs that started before the
plugin was enabled replay unchanged. A run that used LangGraph (`graph()` / `entrypoint()`) but
scheduled no tracked Activity, because every node came from the plugin's task cache, still
seals, so its final-state hash is recorded.

## Determinism

The workflow side keeps a counter and two sets of ids, all derived from workflow events.
Completion is observed with `add_done_callback` on the Activity handle, which runs on the
workflow's deterministic event loop. `tests/test_replay_determinism.py` replays committed
histories (recorded with and without the plugin) through Temporal's `Replayer` and also
re-derives the Stepledger headers and seal input during replay and compares them with the ones
history recorded, because Temporal's own nondeterminism check does not compare headers.

## Reading the ledger back

### `materialize()`

```python
from stepledger import materialize

result = await materialize(dsn, build_graph().compile(), "investigate-7f3a")
result.completeness  # "EXACT" | "GAP" | "OPEN"
result.state  # the rebuilt graph state
```

It takes fresh copies of the compiled graph's own channels, seeds them from the first node's
input snapshot, and applies each superstep's committed deltas in LangGraph's own task-path
order, so each channel's reducer does the folding. Every row carries the hash of the input its
node received; the fold checks it and names the position where some state change has no row.
`EXACT` only when the rebuilt state hashes to the final-state hash recorded at seal. A task-cache
hit or an `execute_in="workflow"` node has no Activity and so no row: the result is `GAP`, never
a wrong `EXACT`. `chain=True` fills positions a continue-as-new run served from the task cache
using earlier runs of the same workflow id.

### SQL views

```sql
-- recent runs, with spend, retry waste and prevented duplicate effects
SELECT workflow_id, status, sealed, committed, provisional, abandoned,
       tokens_in + tokens_out AS tokens, retry_waste_tokens, duplicate_effects_prevented
FROM sl_run_summary ORDER BY first_seen_at DESC LIMIT 20;

-- one run, step by step: timing, attempts, and how long COMMITTED lagged the node
SELECT seq, node, lg_step, status, attempt, write_attempts, duration_ms, commit_lag_ms
FROM sl_node_timeline WHERE workflow_id = 'investigate-7f3a' ORDER BY seq;

-- where retries cost money (attempts that reached the ledger but were not accepted)
SELECT workflow_id, node, wasted_attempts, wasted_tokens, wasted_cost_usd
FROM sl_retry_waste WHERE wasted_attempts > 0 ORDER BY wasted_tokens DESC;

-- nodes still in flight (PROVISIONAL rows are visible before they commit)
SELECT workflow_id, seq, node, attempt FROM sl_nodes WHERE status = 'PROVISIONAL';
```

### CLI

| Command | What it does |
|---|---|
| `stepledger init-db` | create tables and views (idempotent) |
| `stepledger ledger <workflow_id>` | the run ledger: one line per node execution, attempts, effects, waste |
| `stepledger status` | recent runs |
| `stepledger reconcile <workflow_id> \| --all-open` | repair from history |
| `stepledger resolve <effect key> --outcome done\|not-done` | a human's verdict on an unknown effect |
| `stepledger gc [--execute]` | mark and sweep the dedup store (dry run by default) |

## Tables

| Table | One row per |
|---|---|
| `sl_runs` | workflow run seen by the ledger (status, seal, final-state hash, degraded) |
| `sl_nodes` | node Activity execution (the ledger itself) |
| `sl_node_attempts` | write attempt, including fenced-out ones (append-only) |
| `sl_effects` | `once()` effect key |
| `sl_llm_calls` | journaled model call |
| `sl_payloads` / `sl_chunks` / `sl_payload_refs` | stored payload manifest / chunk / reference |
