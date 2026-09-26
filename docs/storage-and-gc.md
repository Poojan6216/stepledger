# Storage and garbage collection

## Why

The LangGraph plugin sends each node's whole input state as its Activity input
(`ActivityInput(args=(state,), ...)`), and Temporal records every Activity input in history. For
an agent whose state accumulates, history grows with the square of the run and eventually hits
either the 2 MB single-payload limit or the 50 MB history limit.

Temporal's External Storage moves large payloads out of history and leaves a small reference.
The shipped S3 driver stores one object per distinct payload, keyed by SHA-256. Each node's
input is a slightly longer copy of the previous one, so storage then grows with the square of
the run instead.

## The driver

`DedupStorageDriver` is a `temporalio.converter.StorageDriver`:

1. `claim = sha256(payload.SerializeToString())`.
2. If a manifest for that claim exists, record a reference for this workflow and stop.
3. Otherwise split the bytes with FastCDC (Xia et al., USENIX ATC 2016; min 4 KiB, average
   16 KiB, max 64 KiB) and store only chunks not already stored, plus a manifest listing the
   chunk hashes in order, plus a reference.
4. On retrieve, reassemble and check SHA-256 against the claim. A mismatch raises; bytes that
   fail the check are never returned.

Content-defined boundaries follow the content, not fixed offsets, so appending to the state only
changes the chunk where the append lands; every earlier chunk is identical and already stored.

`StepledgerPlugin(external_storage=True, payload_size_threshold=64 * 1024)` wires it in as
`DataConverter.external_storage` on the client (workers inherit it). The threshold is 64 KiB
rather than the SDK's 256 KiB default; see `bench/results/threshold.json` for the sweep. For
agents whose node outputs sit just under 64 KiB, a lower threshold also moves those outputs out
of history.

### Tables

| Table | Holds |
|---|---|
| `sl_chunks(hash, data, size, last_ref_at)` | each distinct chunk once |
| `sl_payloads(claim, chunks[], size, encoding, deduped, last_ref_at)` | manifests, shared across runs and workflows |
| `sl_payload_refs(claim, namespace, workflow_id, run_id, target_kind)` | who may still need a claim; written on every store, dedupe hits included |

Chunk rows are locked and inserted in hash order, so concurrent stores that share chunks do not
deadlock, and only chunk bytes the database does not already have are sent.

### Encrypted payloads

Payload codecs run before External Storage (`temporalio/converter/_data_converter.py:263-286`),
so an encrypted payload reaches the driver as ciphertext, which cannot dedupe. Anything not
encoded `json/plain`, `json/protobuf` or `binary/plain` is stored whole and counted as opaque
(`driver.metrics.opaque_payloads`). Convergent encryption would restore dedupe but reveals which
chunks are equal; it is documented, not built.

### One driver configuration per database

A manifest is shared by claim. If a payload was first stored whole (by a driver with
`dedupe=False`, or because it was opaque), a later store of the same bytes by a deduplicating
driver is a dedupe hit on that whole manifest and stays whole. Keep one driver configuration per
database. The bench empties the store between configurations for the same reason.

## Garbage collection

Dedupe means one stored payload can belong to many runs and workflows, so GC never decides by a
claim's age. It is mark and sweep over references:

1. **References.** A reference expires only when no run of its workflow id is open and the newest
   closed run closed more than `retention_days + margin_days` ago (default 30 + 7), checked
   through Temporal's visibility, not only the ledger (which knows tracked runs only). Gating on
   the workflow id, not the run id, protects continue-as-new successors (their input was stored
   under the previous run) and inputs a client stored before the run id existed. References
   whose store context had no workflow id (heartbeat details, for example) expire by age
   (`orphan_ref_days`, default 90).
2. **Manifests** with no reference left and not touched by any store since
   `sweep_start - grace` (default 1 hour) are deleted.
3. **Chunks** that no manifest lists and that no store touched since `sweep_start - grace` are
   deleted.

The grace window closes the race with a concurrent store: every store bumps `last_ref_at` on the
manifest and chunks it uses, and each delete re-checks the row under its lock, so a store either
lands first (the row is skipped) or finds the manifest gone and writes it again.

```bash
stepledger gc                     # dry run: what would be deleted
stepledger gc --execute           # delete, one transaction per batch
```

`stepledger gc` refuses to run when `retention_days` is shorter than the namespace's workflow
retention (read from the server), because a closed workflow could still be read after its
payloads were gone. Tests (`tests/integration/test_gc.py`) cover a continue-as-new successor, a
client-stored start input, and a store racing the sweep; a planted "expire by age" bug makes the
first of those fail.
