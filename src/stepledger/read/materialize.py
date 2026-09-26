"""materialize(): rebuild a run's graph state from its committed deltas, the way LangGraph would.

1. Fresh copies of the compiled graph's own state channels (`channel.from_checkpoint(MISSING)`).
2. Seed them from the first tracked node's input snapshot.
3. For each superstep in order, apply every committed delta of that step to each channel in one
   `update()` call, ordered by LangGraph's sortable task path, so each channel's own reducer
   (operator.add, last value, custom reducers) does the folding.

Every row carries the hash of the input its node received. Before applying a step, the fold
recomputes each node's input from the rebuilt state (through the node's own input schema) and
compares; a mismatch names the position where some state change has no row.

The result is labelled:
    EXACT  only if the rebuilt state hashes to the final-state hash recorded at seal. Never
           inferred any other way.
    GAP    anything else, with the reason and the positions: a workflow-side node, a task-cache
           hit (no Activity, no row), or another write the ledger never saw.
    OPEN   the run has not sealed.

With `chain=True`, positions a continue-as-new run served from the plugin's task cache are
filled from earlier runs of the same workflow id: a COMMITTED row with the same node, step and
path whose input hash matches the rebuilt input. The result is still EXACT only if the hash
matches.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Literal

import psycopg
from langgraph.errors import EmptyChannelError
from temporalio.converter import DataConverter, PayloadConverter

from stepledger._compat import MISSING
from stepledger.canonical import serialize

Completeness = Literal["EXACT", "GAP", "OPEN"]


@dataclass
class MaterializeResult:
    state: dict[str, Any]
    completeness: Completeness
    workflow_id: str
    run_id: str
    reason: str = ""
    positions: list[str] = field(default_factory=list)  # where state changes have no row
    rows_used: int = 0
    chain_rows_used: int = 0
    state_hash: str = ""
    final_state_hash: str | None = None


@dataclass
class _Row:
    run_id: str
    seq: int
    node: str | None
    step: int
    path: str
    kind: str
    output: Any
    input_hash: str | None
    snapshot: Any


def _delta(row: _Row) -> Any:
    out = row.output if isinstance(row.output, dict) else {}
    if row.kind == "COMMAND":
        cmd = out.get("langgraph_command") or {}
        return cmd.get("update") if isinstance(cmd, dict) else None
    if row.kind == "UPDATE":
        return out.get("result")
    return None  # INTERRUPT rows change no state


def _writes(delta: Any) -> list[tuple[str, Any]]:
    if isinstance(delta, dict):
        return list(delta.items())
    if isinstance(delta, list | tuple):  # Command(update=[(key, value), ...])
        return [(k, v) for k, v in delta if isinstance(k, str)]
    return []


def _state_keys(graph: Any) -> list[str]:
    builder = getattr(graph, "builder", None)
    if builder is not None and getattr(builder, "channels", None):
        return list(builder.channels)
    return list(getattr(graph, "stream_channels_list", None) or graph.output_channels)


def _input_keys(graph: Any, node: str | None, state_keys: list[str]) -> list[str]:
    builder = getattr(graph, "builder", None)
    spec = getattr(builder, "nodes", {}).get(node) if builder is not None and node else None
    schema = getattr(spec, "input_schema", None)
    hints = getattr(schema, "__annotations__", None) if schema is not None else None
    if hints:
        return [k for k in hints if k in state_keys]
    return state_keys


def _values(channels: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    out = {}
    for k in keys:
        try:
            out[k] = channels[k].get()
        except EmptyChannelError:
            continue
    return out


async def _load(
    conn: Any, workflow_id: str, run_ids: list[str], statuses: list[str], graph: str | None
) -> list[_Row]:
    cur = await conn.execute(
        "SELECT run_id, seq, node, lg_step, lg_path, kind, output_json, input_hash, input_snapshot"
        " FROM sl_nodes WHERE workflow_id = %s AND run_id = ANY(%s) AND status = ANY(%s)"
        " AND lg_step IS NOT NULL AND (%s::text IS NULL OR graph = %s)"
        " ORDER BY lg_step, lg_path, seq",
        (workflow_id, run_ids, statuses, graph, graph),
    )
    return [_Row(*r) for r in await cur.fetchall()]


async def materialize(
    dsn: str,
    compiled_graph: Any,
    workflow_id: str,
    run_id: str | None = None,
    *,
    include_provisional: bool = False,
    chain: bool = False,
    payload_converter: PayloadConverter | None = None,
    graph_name: str | None = None,
) -> MaterializeResult:
    """`payload_converter` must be the workflow's (e.g. pydantic_data_converter's) when state
    holds non-JSON values such as LangChain messages; hashes go through it, as at seal.
    `graph_name` (the name the graph was registered under in LangGraphPlugin) restricts the fold
    to that graph's rows when one workflow invokes several graphs.

    EXACT compares the rebuilt state with the hash of the *workflow's return value* recorded at
    seal, so it is reachable only when the workflow returns the graph's final state (as the
    examples and bench do). A workflow that returns something else always gets GAP."""
    conv = payload_converter or DataConverter.default.payload_converter

    def state_hash(value: Any) -> str:
        return serialize(value, conv).hash

    statuses = ["COMMITTED", "PROVISIONAL"] if include_provisional else ["COMMITTED"]
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cur = await conn.execute(
            "SELECT run_id, sealed_at IS NOT NULL, final_state_hash, node_count FROM sl_runs"
            " WHERE workflow_id = %s ORDER BY first_seen_at",
            (workflow_id,),
        )
        runs = await cur.fetchall()
        if not runs:
            raise LookupError(f"no ledger runs for workflow {workflow_id}")
        ids = [r[0] for r in runs]
        target = run_id or ids[-1]
        if target not in ids:
            raise LookupError(f"run {target} of {workflow_id} is not in the ledger")
        idx = ids.index(target)
        _, sealed, final_hash, node_count = runs[idx]
        rows = await _load(conn, workflow_id, [target], statuses, graph_name)
        earlier = (
            await _load(conn, workflow_id, ids[:idx], ["COMMITTED"], graph_name) if chain else []
        )
        cur = await conn.execute(
            "SELECT count(*) FROM sl_nodes WHERE workflow_id = %s AND run_id = %s",
            (workflow_id, target),
        )
        row_count = (await cur.fetchone() or (0,))[0]

    keys = _state_keys(compiled_graph)
    channels = {k: compiled_graph.channels[k].from_checkpoint(MISSING) for k in keys}
    result = MaterializeResult({}, "OPEN", workflow_id, target, final_state_hash=final_hash)

    # Seed from the first tracked node's input snapshot (this run's, else an earlier run's).
    seed = next((r for r in rows if r.seq == 0 and r.snapshot is not None), None)
    if seed is None and chain:
        seed = next((r for r in earlier if r.seq == 0 and r.snapshot is not None), None)
    if seed is not None and isinstance(seed.snapshot, dict):
        for k, v in seed.snapshot.items():
            if k in channels:
                channels[k].update([v])
    first_step = seed.step if seed is not None else 0

    by_step: dict[int, dict[str, _Row]] = defaultdict(dict)
    for r in rows:
        if r.kind != "INTERRUPT" and r.step >= first_step:
            by_step[r.step][f"{r.path}#{r.seq}"] = r
    fill: dict[int, dict[str, _Row]] = defaultdict(dict)
    for r in earlier:  # later runs win for the same (step, path)
        if r.kind != "INTERRUPT" and r.step >= first_step:
            fill[r.step][r.path] = r

    last_step = None
    for step in sorted(set(by_step) | set(fill)):
        present = list(by_step.get(step, {}).values())
        paths = {r.path for r in present}
        candidates = [r for p, r in fill.get(step, {}).items() if p not in paths]
        applied: list[_Row] = []
        for r in present:
            expected = state_hash(_values(channels, _input_keys(compiled_graph, r.node, keys)))
            if r.input_hash is not None and r.input_hash != expected:
                result.positions.append(
                    f"before step {step}: input of {r.node} ({r.path}) does not match the fold"
                )
            applied.append(r)
        for r in candidates:
            expected = state_hash(_values(channels, _input_keys(compiled_graph, r.node, keys)))
            if r.input_hash == expected:
                applied.append(r)
                result.chain_rows_used += 1
        if not applied:
            continue
        if last_step is not None and step > last_step + 1 and not chain:
            result.positions.append(f"steps {last_step + 1}..{step - 1}: no rows")
        last_step = step
        writes: dict[str, list[Any]] = defaultdict(list)
        for r in sorted(applied, key=lambda r: (r.path, r.seq)):
            for k, v in _writes(_delta(r)):
                if k in channels:
                    writes[k].append(v)
        for k, vals in writes.items():
            channels[k].update(vals)
        result.rows_used += len([r for r in applied if r.run_id == target])

    result.state = _values(channels, keys)
    result.state_hash = state_hash(result.state)
    if not sealed:
        result.completeness, result.reason = "OPEN", "the run has not sealed"
    elif final_hash is not None and result.state_hash == final_hash:
        result.completeness = "EXACT"
        result.reason = "rebuilt state hashes to the final-state hash recorded at seal"
        result.positions = []
    else:
        result.completeness = "GAP"
        missing_rows = (node_count or 0) - row_count
        parts = []
        if final_hash is None:
            parts.append("no final-state hash was recorded (the run did not complete)")
        else:
            parts.append("rebuilt state differs from the final state recorded at seal")
        if not rows and not result.chain_rows_used:
            parts.append(
                "this run has no ledger rows (every node was served from the task cache"
                " or ran in the workflow)"
            )
        if missing_rows > 0:
            parts.append(f"{missing_rows} scheduled node(s) have no row")
        if not result.positions and last_step is not None:
            result.positions.append(f"after step {last_step}: a write with no row")
        result.reason = "; ".join(parts)
    return result
