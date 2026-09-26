# Demo video script (2 minutes)

Recorded against the local dev environment (`scripts/dev.sh up && uv run stepledger init-db`).
Numbers on screen come straight from the demo output; nothing is typed in by hand.

**0:00 to 0:20. The problem.**
On screen: the #1894 issue title, then `docs/how-it-works.md`'s "one step's life".
Voice: "Temporal's LangGraph plugin sends every node the whole agent state, and history keeps
every copy. For long agents that fails, and writing each step to Postgres from the node doesn't fix
it."

**0:20 to 0:50. Demo 1, the cliff.**
Run `uv run python bench/demo.py --demo cliff`. Show the table as it prints.
Voice: "Bulk persist at the end: stuck, or terminated with the exact error from the issue. Per-node
writes: stopped at node 35, because that node's own input is over two megabytes. With Stepledger
and the dedup storage driver, the same run completes, and nothing in history is over 64 KB."

**0:50 to 1:20. Demo 2, pull the plug.**
Run `uv run python bench/demo.py --demo chaos` (sped up). Show the worker being killed and
restarted in the log, then the table.
Voice: "Twenty runs per config, the worker killed at seeded points, a model that answers
differently on every retry. The naive write leaves duplicates and rows that disagree with what
Temporal accepted. Stepledger: zero on every counter, checked against Temporal's own history."

**1:20 to 1:40. Demo 3, the growth plot.**
Show `bench/plots/history-light.png`, then `storage-light.png`.
Voice: "History without External Storage grows with the square of the run and hits a wall. With
it, history stays small. Storage with one object per payload grows quadratically; stored as
chunks, it grows linearly."

**1:40 to 2:00. The ledger.**
Run `uv run stepledger ledger <workflow id>` on a chaos run with a crash.
Voice: "One row per node execution, which attempt won, what the retries wasted, and which
duplicate effects were prevented. What beats it, and why, is in RESULTS.md."
