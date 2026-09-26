# Draft PR description for temporalio/samples-python (do not open without the owner's OK)

**Title:** `langgraph_plugin/graph_api/large_state`: External Storage plus an idempotent per-node write

**Body:**

A self-contained sample for agents whose LangGraph state accumulates, the situation in
temporalio/sdk-python#1894. It has no dependency outside the SDK, LangGraph and a database driver.

What it shows:

1. **Why the run fails.** The plugin sends each node's whole state as its Activity input, and
   history records every input, so history grows with the square of the run. The sample's README
   explains the two walls (a single payload over 2 MiB, or history over 50 MiB) and that the Python
   SDK's default reports an oversized payload as a retrying workflow task failure
   (`PAYLOADS_TOO_LARGE`) rather than a terminated run.
2. **External Storage** configured on the client's data converter, so node inputs and results
   above a threshold become references in history. The sample uses an in-memory or filesystem
   driver to stay self-contained, with a note on production drivers.
3. **An idempotent per-node write** from inside the node's Activity: keyed on the workflow run ID
   plus the Activity ID, and fenced on `activity.info().current_attempt_scheduled_time` so a
   timed-out attempt that keeps running cannot overwrite the attempt Temporal accepted. The sample
   notes that the SDK core cancels a timed-out async attempt locally, so late writes come from code
   that ignores cancellation (synchronous nodes on threads, for example).
4. **A test** that kills the worker after the write and checks the database row against the result
   recorded in history.

Measured motivation (local dev server, 60 KiB of new output per node): history reached 29 MiB at
30 nodes and the run stopped at node 35; with External Storage the same run completed with about
2.5 MiB of history at 40 nodes.
