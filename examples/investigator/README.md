# The investigator agent

The demo agent from the bench: a cloud-security investigation with a parallel scan superstep,
a chain of enrichment nodes producing tool transcripts, an optional `interrupt()` for human
review, and two external effects (`open_ticket`, `notify_slack`) through `once()`.

It lives in `bench/agents/` so the demos can parametrize it (node count, output size per node,
fan-out, interrupt, fault injection). To run one investigation against the dev environment:

```bash
scripts/dev.sh up && uv run stepledger init-db
uv run python examples/investigator/run.py --nodes 30 --kb 1
```
