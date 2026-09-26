"""Per-run numbers read back from the ledger: rows, statuses, write latency, store bytes."""

from __future__ import annotations

from typing import Any

import psycopg


def _pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, round(p / 100 * (len(s) - 1))))
    return round(s[k], 3)


async def run_stats(dsn: str, workflow_id: str, run_id: str) -> dict[str, Any]:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        cur = await conn.execute(
            "SELECT count(*), count(*) FILTER (WHERE status = 'COMMITTED'),"
            " count(*) FILTER (WHERE status = 'PROVISIONAL') FROM sl_nodes"
            " WHERE workflow_id = %s AND run_id = %s",
            (workflow_id, run_id),
        )
        rows, committed, provisional = await cur.fetchone() or (0, 0, 0)
        cur = await conn.execute(
            "SELECT write_ms FROM sl_node_attempts WHERE workflow_id = %s AND run_id = %s"
            " AND write_ms IS NOT NULL",
            (workflow_id, run_id),
        )
        writes = [float(r[0]) for r in await cur.fetchall()]
        cur = await conn.execute(
            "SELECT coalesce(sum(p.size), 0), count(*) FROM sl_payloads p WHERE p.claim IN"
            " (SELECT claim FROM sl_payload_refs WHERE workflow_id = %s AND run_id = %s)",
            (workflow_id, run_id),
        )
        logical, payloads = await cur.fetchone() or (0, 0)
    return {
        "ledger_rows": int(rows),
        "ledger_committed": int(committed),
        "ledger_provisional": int(provisional),
        "ledger_write_ms_p50": _pct(writes, 50),
        "ledger_write_ms_p95": _pct(writes, 95),
        "externalized_payloads": int(payloads),
        "externalized_logical_bytes": int(logical),
    }


def percentile(values: list[float], p: float) -> float | None:
    return _pct(values, p)
