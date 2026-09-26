# Reply draft for temporalio/sdk-python#1894

Not posted. For the owner to post from their own account after reviewing. Numbers come from
`bench/results/` in this repository (commands in `RESULTS.md`): one run per configuration on a
local dev server (CLI 1.9.1, server 1.32.0), temporalio 1.33.0, langgraph 1.2.12, the default
2 MiB payload and 50 MiB history limits.

---

A few things on the mechanics, from reproducing this on a local dev server.

**Which SDK version and worker settings were you on?** With the Python SDK's default
(`disable_payload_error_limit=False`) an oversized payload surfaces as a workflow task failure
(`PAYLOADS_TOO_LARGE`) that keeps retrying, so the run looks stuck rather than terminated. The
server-side `BadScheduleActivityAttributes ... Input exceeds size limit` termination in your
report is what I get with that check disabled. Worth alerting on both shapes.

**The size problem is larger than the one call that failed.** The plugin sends each node's full
input state as the Activity input (`ActivityInput(args=(state,), ...)` in
`contrib/langgraph/_activity.py`), and every Activity input is recorded in history, so history
grows roughly with the square of the transcript. With 60 KiB of new tool output per node, a
30-node run had 29 MiB of history while its largest node input was still 1.67 MiB, and node 35's
input crossed 2 MiB; with 20 KiB per node the 50 MiB history limit came first, at node 71. So
writing each node's delta to Postgres from inside the node keeps every write small but moves the
wall rather than removing it. Two things that write needs anyway: an idempotency key that is
stable across retries (run ID plus Activity ID, as the activity docs suggest) and a fence so a
stale attempt cannot overwrite a newer one; a retried LLM node can otherwise leave Postgres
holding attempt 1's answer while every later node used attempt 2's.

**The SDK-native answer for size is External Storage**
(`DataConverter(external_storage=ExternalStorage(drivers=[...]))`, marked experimental): large
payloads become small references in history. With it on and a 64 KiB threshold, the 40-node run
above completed with nothing above the threshold left in history and about 2.5 MiB of history in
total. Two caveats: stored as one object per distinct payload (how the S3 driver stores them) the
store grows the way history did, about 28 MB for the 30-node run and 114 MB at 60 nodes, against
about 2.6 MB and 5.2 MB when the payloads are split into content-defined chunks stored once; and
a payload codec runs before External Storage, so encrypted payloads cannot be deduplicated.

I put the measurements and a plugin that does the fenced per-node ledger plus a chunk-dedup
Postgres storage driver here: https://github.com/Poojan6216/stepledger. Happy to answer questions.
