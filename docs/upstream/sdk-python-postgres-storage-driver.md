# Draft issue for temporalio/sdk-python (do not open without the owner's OK)

**Title:** Proposal: a Postgres `StorageDriver` with content-defined chunk dedup in `temporalio.contrib`

**Body:**

External Storage ships with one driver, S3, which stores one object per distinct payload keyed by
SHA-256 (`contrib/aws/s3driver/_driver.py`). For workflows whose payloads are successive,
slightly longer copies of one growing value (LangGraph agents are the common case: the plugin
sends each node's whole state as its Activity input), that makes storage grow with the square of
the run, even though history is fixed.

I measured this on a local dev server (temporalio 1.33.0, langgraph 1.2.12) with a LangGraph agent
adding 60 KiB of tool output per node, External Storage at a 64 KiB threshold:

| nodes | one object per payload | content-defined chunks stored once |
|---|---|---|
| 30 | 28.5 MB | 2.6 MB |
| 60 | 114.0 MB | 5.2 MB |

(`bench/results/growth.json` in the linked repository; at 80 nodes x 100 KiB it is 343 MB against
12.2 MB.)

The driver I used for the right-hand column:

- splits each payload with FastCDC (4 / 16 / 64 KiB), stores each chunk once in Postgres, keeps a
  manifest per claim (`claim = sha256(serialized payload)`, the same claim shape as the S3 driver);
- verifies SHA-256 on every retrieve and raises on mismatch;
- detects non-plaintext encodings (codecs run before External Storage) and stores those whole;
- records a reference per `(claim, namespace, workflow_id, run_id)` on every store, including
  dedupe hits, so garbage collection can work on references instead of claim age. A reference
  expires only when no run of its workflow id is open and the newest closed run is past retention
  plus a margin (checked through visibility), which keeps continue-as-new successors and
  client-stored start inputs safe; a grace window closes the race with concurrent stores.

Would a driver like this be welcome in `temporalio.contrib` (or as a sample)? Postgres is a
common dependency for teams already running Temporal self-hosted, and the chunking only needs
`fastcdc`. I am happy to adapt the implementation to the contrib conventions, drop the parts that
are out of scope (the GC CLI, for example), and add tests in the SDK's style.

Implementation and measurements: [repository link]
