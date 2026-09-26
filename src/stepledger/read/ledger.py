"""The run ledger: one run's rows, attempts, effects and retry waste, as a table."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg


@dataclass
class NodeLine:
    seq: int
    node: str | None
    step: int | None
    attempt: int
    status: str
    output_hash: str
    tokens: int
    retry_waste: int
    note: str


def _note(attempts: list[tuple[Any, ...]], final_attempt: int, final_hash: str) -> str:
    notes = []
    for attempt, outcome, h, note in attempts:
        if outcome == "WROTE" and attempt != final_attempt:
            diff = "divergent output discarded" if h != final_hash else "same output"
            notes.append(f"attempt {attempt}'s row overwritten by attempt {final_attempt}; {diff}")
        elif outcome == "FENCED_OUT":
            notes.append(f"stale attempt {attempt} fenced out" + (f" ({note})" if note else ""))
        elif outcome == "DIVERGENCE_REPAIRED":
            notes.append(f"reconcile: {note}")
    return "; ".join(notes)


def run_ledger(dsn: str, workflow_id: str, run_id: str | None = None) -> str:
    with psycopg.connect(dsn) as conn:
        if run_id is None:
            row = conn.execute(
                "SELECT run_id FROM sl_runs WHERE workflow_id = %s ORDER BY first_seen_at DESC"
                " LIMIT 1",
                (workflow_id,),
            ).fetchone()
            if row is None:
                return f"no ledger rows for workflow {workflow_id}"
            run_id = row[0]
        run = conn.execute(
            "SELECT status, sealed_at IS NOT NULL, sealed_by, node_count, committed_count,"
            " abandoned_count, degraded, missing_seqs FROM sl_runs"
            " WHERE workflow_id = %s AND run_id = %s",
            (workflow_id, run_id),
        ).fetchone()
        nodes = conn.execute(
            "SELECT seq, node, lg_step, attempt, status, output_hash,"
            " coalesce(tokens_in, 0) + coalesce(tokens_out, 0) FROM sl_nodes"
            " WHERE workflow_id = %s AND run_id = %s ORDER BY seq",
            (workflow_id, run_id),
        ).fetchall()
        attempts: dict[int, list[tuple[Any, ...]]] = {}
        waste: dict[int, int] = {}
        for seq, attempt, outcome, h, note, t_in, t_out, fence_attempt in conn.execute(
            "SELECT a.seq, a.attempt, a.outcome, a.output_hash, a.note, a.tokens_in, a.tokens_out,"
            " n.attempt FROM sl_node_attempts a JOIN sl_nodes n USING"
            " (namespace, workflow_id, run_id, seq) WHERE a.workflow_id = %s AND a.run_id = %s"
            " ORDER BY a.id",
            (workflow_id, run_id),
        ).fetchall():
            attempts.setdefault(seq, []).append((attempt, outcome, h, note))
            if attempt != fence_attempt:
                waste[seq] = waste.get(seq, 0) + (t_in or 0) + (t_out or 0)
        effects = conn.execute(
            "SELECT name, count(*), min(key), sum(duplicates_prevented), string_agg(DISTINCT"
            " status, ',') FROM sl_effects WHERE workflow_id = %s AND run_id = %s GROUP BY name"
            " ORDER BY min(seq)",
            (workflow_id, run_id),
        ).fetchall()

    lines = []
    if run is None:
        head = "not seen"
    else:
        status, sealed, sealed_by, n, c, a, degraded, missing = run
        head = (
            f"status={status}  {'sealed' if sealed else 'open'}"
            + (f" (by {sealed_by})" if sealed and sealed_by != "seal" else "")
            + f"  nodes={n if n is not None else len(nodes)} committed={c or 0} abandoned={a or 0}"
            + ("  DEGRADED" + (f" missing={missing}" if missing else "") if degraded else "")
        )
    lines.append(f"STEPLEDGER  wf={workflow_id}  run={run_id}  {head}")
    lines.append(
        f" {'seq':>3} {'node':<16} {'step':>4} {'attempt':>7} {'status':<11} {'output_hash':<12}"
        f" {'tokens':>7} {'retry_waste':>11}  note"
    )
    for seq, node, step, attempt, status, h, tokens in nodes:
        note = _note(attempts.get(seq, []), attempt, h)
        lines.append(
            f" {seq:>3} {(node or '?'):<16} {step if step is not None else '':>4} {attempt:>7}"
            f" {status:<11} {h[:8] + '...':<12} {tokens:>7,} {waste.get(seq, 0):>11,}  {note}"
        )
    if effects:
        parts = [f"{name} x{count} (key {key[:6]}...)" for name, count, key, _, _ in effects]
        prevented = sum(int(e[3] or 0) for e in effects)
        lines.append(f"effects: {', '.join(parts)}    duplicates prevented: {prevented}")
    return "\n".join(lines)
