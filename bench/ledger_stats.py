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
        # Storage this run needs: its manifests (refs carry its run id, or '' for the client's
        # start input), the whole-blob bytes of those payloads, and the distinct chunk bytes.
        runs = [run_id, ""]
        cur = await conn.execute(
            "WITH m AS (SELECT DISTINCT p.claim, p.size, p.chunks FROM sl_payloads p"
            " JOIN sl_payload_refs r USING (claim) WHERE r.workflow_id = %s AND r.run_id = ANY(%s))"
            " SELECT count(*), coalesce(sum(size), 0),"
            " (SELECT coalesce(sum(c.size), 0) FROM sl_chunks c"
            "  WHERE c.hash IN (SELECT unnest(chunks) FROM m)) FROM m",
            (workflow_id, runs),
        )
        payloads, logical, unique = await cur.fetchone() or (0, 0, 0)
    return {
        "ledger_rows": int(rows),
        "ledger_committed": int(committed),
        "ledger_provisional": int(provisional),
        "ledger_write_ms_p50": _pct(writes, 50),
        "ledger_write_ms_p95": _pct(writes, 95),
        "externalized_payloads": int(payloads),
        "store_whole_blob_bytes": int(logical),
        "store_unique_chunk_bytes": int(unique),
    }


def percentile(values: list[float], p: float) -> float | None:
    return _pct(values, p)
