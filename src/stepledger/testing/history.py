"""The invariant checker: ledger rows versus Temporal's own history.

Maps each `stepledger-seq` header on `ActivityTaskScheduled` to that node's completion events,
decodes the result Temporal recorded with the client's data converter (so External Storage
references are followed), and counts what the ledger got wrong. Correctness is always judged
against history, never against the ledger's opinion of itself.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any

import psycopg
from temporalio.client import Client

from stepledger.canonical import chash
from stepledger.ledger.history import HistoryNode, history_nodes

__all__ = ["Counters", "HistoryNode", "check_naive", "check_stepledger", "history_nodes"]


@dataclass
class Counters:
    """The five invariant counters (build spec, Demo 2)."""

    rows: int = 0
    duplicate_rows: int = 0  # more than one row for one node Activity execution
    divergent_rows: int = 0  # row output differs from the result Temporal recorded
    lost_rows: int = 0  # accepted in history, but no row or an ABANDONED row
    orphan_rows: int = 0  # PROVISIONAL after the run sealed / closed
    duplicate_side_effects: int = 0  # extra calls that reached a fake effect sink
    wrongly_committed: int = 0  # COMMITTED although history shows it was not accepted
    details: list[str] = field(default_factory=list)

    def zero(self) -> bool:
        return not any(
            (
                self.duplicate_rows,
                self.divergent_rows,
                self.lost_rows,
                self.orphan_rows,
                self.duplicate_side_effects,
                self.wrongly_committed,
            )
        )

    def add(self, other: Counters) -> None:
        for k, v in asdict(other).items():
            if k == "details":
                self.details.extend(v)
            else:
                setattr(self, k, getattr(self, k) + v)

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d.pop("details")
        return d


def _effect_duplicates(conn: psycopg.Connection[Any], workflow_id: str) -> int:
    rows = conn.execute(
        "SELECT count(*) - 1 FROM bench_effects WHERE workflow_id = %s"
        " GROUP BY run_id, effect, request_hash HAVING count(*) > 1",
        (workflow_id,),
    ).fetchall()
    return sum(int(r[0]) for r in rows)


def _has_table(conn: psycopg.Connection[Any], name: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s) IS NOT NULL", (name,)).fetchone()
    return bool(row and row[0])


async def check_stepledger(
    client: Client, dsn: str, workflow_id: str, run_id: str, *, expect_sealed: bool = True
) -> Counters:
    hist = [n for n in await history_nodes(client, workflow_id, run_id) if n.seq is not None]
    by_seq: dict[int, list[HistoryNode]] = {}
    for n in hist:
        by_seq.setdefault(n.seq, []).append(n)  # type: ignore[arg-type]
    out = Counters()
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            "SELECT seq, status, output_hash FROM sl_nodes WHERE workflow_id = %s AND run_id = %s",
            (workflow_id, run_id),
        ).fetchall()
        sealed = conn.execute(
            "SELECT sealed_at IS NOT NULL FROM sl_runs WHERE workflow_id = %s AND run_id = %s",
            (workflow_id, run_id),
        ).fetchone()
        if _has_table(conn, "bench_effects"):
            out.duplicate_side_effects = _effect_duplicates(conn, workflow_id)
    out.rows = len(rows)
    counts = Counter(r[0] for r in rows)
    out.duplicate_rows = sum(c - 1 for c in counts.values() if c > 1)
    out.duplicate_rows += sum(len(v) - 1 for v in by_seq.values() if len(v) > 1)  # seq reuse
    spurious = sorted(r[0] for r in rows if r[0] not in by_seq)  # no scheduled Activity has it
    out.duplicate_rows += len(spurious)
    if spurious:
        out.details.append(f"rows for seqs Temporal never scheduled: {spurious}")
    row_by_seq = {r[0]: r for r in rows}
    for seq, seq_nodes in by_seq.items():
        node = seq_nodes[0]
        row = row_by_seq.get(seq)
        if node.accepted:
            if row is None or row[1] == "ABANDONED":
                out.lost_rows += 1
                out.details.append(f"seq {seq}: accepted in history, row={row and row[1]}")
            elif row[1] == "PROVISIONAL":
                pass  # counted as an orphan once the run is sealed; still in flight otherwise
            elif row[2] != node.result_hash:
                out.divergent_rows += 1
                out.details.append(
                    f"seq {seq}: row hash {row[2][:8]} != history {node.result_hash}"
                )
        elif row is not None and row[1] == "COMMITTED":
            out.wrongly_committed += 1
            out.details.append(f"seq {seq}: COMMITTED but history status {node.status}")
    run_closed = bool(sealed and sealed[0]) if expect_sealed else True
    if run_closed:
        orphans = [r[0] for r in rows if r[1] == "PROVISIONAL"]
        out.orphan_rows = len(orphans)
        if orphans:
            out.details.append(f"PROVISIONAL after seal: {orphans}")
    if expect_sealed and not (sealed and sealed[0]):
        out.details.append("run not sealed")
    return out


async def check_naive(
    client: Client, dsn: str, workflow_id: str, run_id: str, *, table: str
) -> Counters:
    """The same five counters for the B1 / B1u baselines' own tables."""
    hist = {n.activity_id: n for n in await history_nodes(client, workflow_id, run_id)}
    out = Counters()
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(
            f"SELECT activity_id, output FROM {table} WHERE workflow_id = %s AND run_id = %s",
            (workflow_id, run_id),
        ).fetchall()
        if _has_table(conn, "bench_effects"):
            out.duplicate_side_effects = _effect_duplicates(conn, workflow_id)
    out.rows = len(rows)
    counts = Counter(r[0] for r in rows)
    out.duplicate_rows = sum(c - 1 for c in counts.values() if c > 1)
    seen: dict[str, list[Any]] = {}
    for act_id, output in rows:
        seen.setdefault(act_id, []).append(output)
    for act_id, node in hist.items():
        if node.activity_type.startswith(("bench.", "stepledger.")):
            continue
        outputs = seen.get(act_id, [])
        accepted = (node.result or {}).get("result") if node.accepted else None
        if node.accepted and not outputs:
            out.lost_rows += 1
        for o in outputs:
            if node.accepted and chash(o) != chash(accepted):
                out.divergent_rows += 1
                out.details.append(f"activity {act_id}: row differs from history")
        if not node.accepted and outputs:
            out.orphan_rows += len(outputs)
    return out
