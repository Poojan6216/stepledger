# Reply draft for temporalio/sdk-python#1894

Not posted. For the owner to post from their own account after reviewing. Every number below
comes from `bench/results/` in this repository (commands in `RESULTS.md`), measured on a local
Temporal dev server (CLI 1.9.1, server 1.32.0) with temporalio 1.33.0 and langgraph 1.2.12, and
the default 2 MiB payload and 50 MiB history limits.

---

Hi, I dug into this because it is the same shape of problem I have been working on. A few things
that might help, with numbers from a reproduction on a local dev server.

**1. Per-node persistence from inside the node's Activity is the right place**, since workflow
code can't do I/O and a separate persist Activity would carry the payload through history again.
Two things I would add. Make the write idempotent with a key that is stable across retries (the
docs suggest workflow run ID plus Activity ID). And fence it so an older attempt can't overwrite a
newer one. In a crash test (20 runs of a 30-node agent whose model answers differently on each
retry, worker killed at seeded points), a plain insert per node left 17 duplicate rows and 13 rows
that disagreed with the result Temporal actually accepted. An upsert on the Activity ID fixed the
duplicates but still left 5 disagreeing rows, from timed-out attempts that kept running and wrote
late, and 4 repeated side effects.

**2. Heads up on size.** The plugin sends each node's full input state as the Activity input
(`ActivityInput(args=(state,), ...)` in `contrib/langgraph/_activity.py`), and every Activity input
is stored in history, so history grows roughly with the square of the transcript. With 60 KiB of
new tool output per node, a 30-node run already had 29 MiB of history while every node input was
still under 2 MiB (largest about 1.7 MB). At that output size node 35's own input crosses 2 MiB,
so per-node persistence alone stops there. With smaller outputs the 50 MiB history limit comes
first: at 20 KiB per node the server terminated the run at node 71 with "Workflow history size
exceeds limit." Your end-of-run bulk persist was simply the first payload to cross 2 MiB; I could
reproduce your exact error that way at 36 nodes.

Small side note: with the Python SDK's default `disable_payload_error_limit=False`, the same
oversized payload shows up as a workflow task failure (`PAYLOADS_TOO_LARGE`) that keeps retrying,
so the run is stuck rather than terminated. With the check disabled you get the
`BadScheduleActivityAttributes ... Input exceeds size limit` termination from the issue. Worth
alerting on both.

**3. The Temporal-native answer for the size part is External Storage**
(`DataConverter(external_storage=ExternalStorage(drivers=[...]))`, Public Preview): large payloads
become small references in history. With it on, the 40-node run that got stuck above completed,
with no payload in history over 64 KB and about 2.5 MiB of history in total. Two caveats. With one
object per distinct payload (how the S3 driver stores them), storage grows the same way the
history did: about 28 MB for the 30-node run and 114 MB at 60 nodes, versus about 2.6 MB and
5.2 MB when the payloads are split into content-defined chunks stored once. And if you use an
encryption codec, it runs before External Storage, so the stored bytes can't be deduplicated.

I put this together as a small open-source plugin that does the fenced per-node ledger plus a
deduplicating Postgres storage driver, with crash tests checked against Temporal's own history:
[link]. Happy to answer questions or adapt anything useful into a sample here.
