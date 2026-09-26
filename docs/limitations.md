# Limitations

What Stepledger does not do, or does only under conditions. The measured side of several of these
is in `RESULTS.md` under "What beats it".

## The ledger

- **Workflow-side nodes get no row.** A node with `execute_in="workflow"` runs in workflow code,
  which cannot do I/O, so there is nothing to write from. `materialize()` reports `GAP` when such
  a node's writes matter.
- **Task-cache hits get no row.** The LangGraph plugin serves a task from its cache instead of
  scheduling an Activity after continue-as-new, and within a run when the same function gets equal
  input. No Activity, no row. "One row per node execution" means per Activity execution.
  `materialize()` reports `GAP`; `chain=True` fills continue-as-new gaps from earlier runs.
- **`COMMITTED` lags one step.** Commits ride on the next node's Activity, so `COMMITTED` trails
  the live run by up to one node. `PROVISIONAL` rows are visible at once.
- **The fence assumes one cluster's clock.** It relies on the server's schedule time moving
  forward per Activity. Multi-cluster replication and failover are out of scope.
- **`reconcile` assumes `TRY_CANCEL`.** History does not record an Activity's cancellation type.
  Under the default `TRY_CANCEL`, a completion after a cancel request never reaches the workflow
  and `reconcile` abandons it; under `WAIT_CANCELLATION_COMPLETED` it does reach the workflow, and
  under `ABANDON` no cancel request is recorded at all. Pass `--cancellation-type` if your nodes
  use another type. One case no history reading can settle: two parallel nodes where one fails and
  the other completes in the same workflow task; LangGraph cancels the completed one and the
  workflow never uses its result, but history shows only a completion. The live path abandons it
  correctly; `reconcile` on a run terminated at that moment would commit it.
- **`reconcile` leaves running runs alone** unless `--include-open`, because the workflow is
  writing the same rows; repairing a live run races its own commits.
- **Tracked Activities started from signal or update handlers** after the main workflow function
  returned are outside the seal's accounting: the seal abandons their rows.
- **Warn mode covers node writes and the seal, not effects or the journal.** `once()` and the LLM
  journal fail their node when Postgres is unreachable, by design.
- **Warn mode trades correctness for availability.** With `on_ledger_error="warn"` a node can
  complete without its row during a database outage; the run is marked degraded and
  `stepledger reconcile` repairs it from history afterwards.
- **Runs that started before the plugin was enabled** have no rows for the nodes they ran before
  it, and if they finish after it their seal lists those seqs as missing.
- **Outputs are stored in plaintext by default.** Use `store_outputs="hash_only"` and database
  encryption for sensitive data.
- **`EXACT` needs the workflow to return the graph's final state.** The seal records the hash of
  the workflow's return value; `materialize()` can only say `EXACT` when the rebuilt state hashes
  to it. A workflow that returns a projection of the state, or runs several graphs, gets `GAP`
  (pass `graph_name=` to fold one graph's rows). The first node's input snapshot seeds the fold,
  so a first node with a narrow `input_schema` leaves the other channels unseeded: `GAP` again,
  never a wrong `EXACT`.
- **Not exercised by the tests or bench:** LangGraph's Functional API (`@entrypoint`/`@task`),
  subgraphs, and `Send` fan-out. The ledger tracks any Activity the LangGraph plugin registers,
  so rows are written for those too, but their `materialize()` behavior and step/path metadata
  have not been checked.

## Storage

- **History still grows, linearly.** With the dedup driver, each node still adds its references and
  any payload under the threshold. Far from the limits, not flat. Unbounded runs still need
  continue-as-new.
- **Encryption defeats dedupe.** Payload codecs run before External Storage, so encrypted payloads
  are stored whole. Convergent encryption would restore dedupe but reveal which chunks are equal;
  it is not built.
- **Manifests still grow with the square of the run**, about 32 bytes per 16 KiB chunk per stored
  payload: several hundred times smaller than the data, but not linear.
- **One driver configuration per database.** A payload first stored whole (by `dedupe=False` or as
  opaque) stays whole when a deduplicating driver later stores the same bytes.
- **Postgres only.** The `ChunkBackend` protocol is the extension point for S3 and similar stores.
- **Private SDK surface.** Stepledger reads four private symbols, all isolated in
  `stepledger/_compat.py` and pinned by `tests/unit/test_compat.py`: the LangGraph plugin's
  `ActivityInput`/`ActivityOutput` and its task-cache context variable, and LangGraph's
  `task_path_str` and `MISSING`. An SDK release that moves one of them fails that test before it
  fails at runtime; the dependency ranges in `pyproject.toml` are the tested ones.
- **Interceptor classes are handed to the sandbox by reference.** The SDK re-imports only the
  workflow class inside the sandbox; `LedgerInbound` is the out-of-sandbox class object and does
  no I/O. Do not import `stepledger` from workflow code.
- **An existing External Storage on the client is not composed with.** The plugin raises rather
  than replace it; pass `external_storage=False` to keep yours.
- **Removing the plugin, or turning `seal` off, is not covered by `workflow.patched`** for runs
  that already recorded the patch marker; drain them first (see how-it-works.md).

## Effects

- **`once()` is at-least-once with dedupe, not exactly-once.** A tool that ignores the idempotency
  key can repeat its effect if the worker dies between the call and the journal write; that case
  goes to the tool's `reconcile` callback or waits for `stepledger resolve`.
- **`once()` covers retries of one run.** `temporal workflow reset` creates a new run ID and new
  keys; only the tool's own upstream idempotency key can dedupe across it.
- **Unknown outcomes stop for a person** when the tool offers no way to ask. That is deliberate.

## Zombies on the Python SDK

The SDK core cancels a timed-out attempt locally, so an async node that honors cancellation does
not write late. Code that ignores cancellation can: synchronous nodes (run on threads), swallowed
`CancelledError`, frozen processes. Stepledger fences those writes out; it cannot stop them from
running.

## Scope

A research prototype (PyPI classifier: Alpha): Postgres only; LangGraph's Graph API through
Temporal's plugin. Functional API tasks get ledger rows but are untested, and `materialize()`
does not apply to them. Subgraphs and `Send` fan-out are untested; `materialize()` folds the
top-level graph's channels only. One demo agent in the bench. Every number was measured on one
macOS laptop against the dev server (SQLite persistence) and a local Postgres; Temporal Cloud is
untested. LangGraph `Store` is not supported inside Activities by the plugin itself.
