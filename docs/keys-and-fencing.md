# Keys and fencing

Three words this document leans on:

- **accepted**: Temporal delivered the Activity's result to workflow code (the awaiting
  coroutine received it). A completion that arrives after the workflow cancelled that Activity
  is not delivered, so it is not accepted.
- **COMMITTED**: a row whose result was accepted, recorded by the next tracked Activity's
  transaction, by the seal, or by `reconcile` reading history. It never changes again.
- **fenced**: a write lands only if its attempt is at least as new as the one that wrote the
  row, judged by the server's schedule time for that attempt.

## Identity: a sequence number in a header

Every tracked node Activity gets `stepledger-seq`, a per-run counter assigned by the workflow
interceptor. The row key is `(namespace, workflow_id, run_id, seq)`.

Temporal's own guidance is to key idempotent writes on the run ID plus the Activity ID. The
Activity side can read its ID, but the workflow side also has to name steps (in the commit
header of the next call), and the default Activity ID is assigned inside the SDK after the
outbound interceptor runs and is not exposed on the returned handle
(`temporalio/worker/_workflow_instance.py:3391`). The interceptor could set its own Activity
IDs, but that would change IDs people see in the UI, match on in tooling, and depend on when
replaying runs in flight. A header adds identity and changes nothing. Because it is recorded in
`ActivityTaskScheduled`, `reconcile` and the test checker map every history event back to its row.
The Activity ID is still stored on the row for audit.

The counter restarts at 0 in each run (continue-as-new included); the key has the run ID in it.

## The fence

Each write carries `(current_attempt_scheduled_time, attempt)` from `activity.info()`. The
Temporal server stamps the schedule time when it schedules each attempt, so a later attempt
carries a later stamp even after an Activity reset (which restarts the attempt counter) and
regardless of worker clocks. The attempt number breaks ties. This is the fencing-token idea from
Martin Kleppmann's "How to do distributed locking" (2016).

```sql
INSERT INTO sl_nodes AS n (...) VALUES (...)
ON CONFLICT (namespace, workflow_id, run_id, seq) DO UPDATE SET output_json = EXCLUDED.output_json, ...
WHERE n.status = 'PROVISIONAL'
  AND (n.fence_scheduled_at, n.attempt) <= (EXCLUDED.fence_scheduled_at, EXCLUDED.attempt)
RETURNING 1;   -- nothing returned: this attempt was fenced out
```

Why it works: Temporal accepts a completion only from the current attempt, and a newer attempt
exists only if the current one failed or timed out. So the attempt whose result the workflow
receives is the one with the latest schedule time, and its write carries the highest fence.
Any stale attempt that writes later is rejected.

## Row status

First matching rule wins.

| Rule | Condition | Outcome |
|---|---|---|
| R1 | write with a lower fence, row `PROVISIONAL` | no change; audited `FENCED_OUT`; the attempt raises retryable `FencedOut` (a stale attempt's failure is discarded by Temporal; a wrong fence heals on retry) |
| R2 | write on a `COMMITTED` or `ABANDONED` row, or into a sealed run | no change; audited; raises non-retryable `FencedOutFinal` (only a stale attempt gets here) |
| R3 | write with a fence `>=` the stored one, row `PROVISIONAL` | overwrite; audited `WROTE` |
| R4 | the workflow saw the result (piggyback or seal) | `PROVISIONAL` becomes `COMMITTED` |
| R5 | the workflow saw a final failure or cancellation | `PROVISIONAL` becomes `ABANDONED` |
| R6a | reconcile: the cancel request came before the completion in history | `ABANDONED` |
| R6 | reconcile: completed in history, no earlier cancel request | `COMMITTED`; if the output differs, rewritten from history and audited `DIVERGENCE_REPAIRED` |

`COMMITTED` rows are never overwritten. A fenced-out attempt never returns success. A write into
a sealed run is refused the same way; the commit and abandon ids it carried are still applied,
which is idempotent.

The fence also assumes the server's schedule timestamps move forward for one Activity. Should a
history shard move between hosts with a backwards clock step, the newer attempt would be fenced
out, raise the retryable `FencedOut`, and heal on its next attempt; it cannot produce a wrong row.

## The two guards are redundant against zombies, on purpose

The upsert has two conditions: the row must be `PROVISIONAL`, and the fence must not go
backwards. Against a stale attempt either one alone suffices, because a stale attempt always
has the lower fence. The chaos test confirms this: dropping only the status condition does not
produce a divergent row; dropping both does (zombies then overwrite committed rows). The status
condition stays as defense in depth, and the property test in `tests/property/` catches its
removal directly.

## Where zombies come from on the Python SDK

The SDK core enforces `start_to_close_timeout` locally: when the deadline passes it cancels the
running attempt (`ActivityCancelReason.TIMED_OUT`; the enforcing code is in the Rust core, which
is not vendored here). An `async` node that honors cancellation therefore never writes after its
timeout. A synchronous node runs on a thread the cancel cannot interrupt, but the `await` around
it is cancelled, so its side effects run late while Stepledger's write does not. A late *row*
needs a swallowed cancel between the node returning and the transaction committing, or a frozen
process that wakes up; the chaos harness models exactly that (fault point F6 hangs while ignoring
the cancel, then writes).

## Failure policy

`on_ledger_error="fail"` (default): if the ledger write fails, the Activity fails and Temporal
retries it. The run stalls during a database outage and resumes intact.

`on_ledger_error="warn"`: a node whose ledger write fails returns its result without a row, and a
seal that cannot reach the database lets the workflow finish unsealed. The seal, when it can run,
notices that fewer rows were committed than the workflow accepted and marks the run degraded;
`stepledger reconcile` then repairs rows and the seal from history. `once()` and the LLM journal
have no warn path: an effect or a model call whose journal write fails still fails its node and
retries, because skipping those journals is what lets duplicates through. This trades ledger
completeness for availability during the outage, and says so.
