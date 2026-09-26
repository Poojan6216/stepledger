-- Stepledger read-side views. Applied by `stepledger init-db`; every statement is idempotent.

-- Per node execution: tokens and dollars spent by attempts Temporal did not accept (overwritten
-- attempts and fenced-out zombies), from the audit trail. An attempt that died before its
-- ledger write (for example right after its model call) never reached the audit trail; the LLM
-- journal (sl_llm_calls) is where those calls show up.
CREATE OR REPLACE VIEW sl_retry_waste AS
SELECT
  a.namespace, a.workflow_id, a.run_id, a.seq, n.node,
  count(*) FILTER (WHERE (a.fence_scheduled_at, a.attempt)
                   IS DISTINCT FROM (n.fence_scheduled_at, n.attempt)) AS wasted_attempts,
  coalesce(sum(coalesce(a.tokens_in, 0) + coalesce(a.tokens_out, 0))
           FILTER (WHERE (a.fence_scheduled_at, a.attempt)
                   IS DISTINCT FROM (n.fence_scheduled_at, n.attempt)), 0) AS wasted_tokens,
  sum(a.cost_usd) FILTER (WHERE (a.fence_scheduled_at, a.attempt)
                          IS DISTINCT FROM (n.fence_scheduled_at, n.attempt)) AS wasted_cost_usd
FROM sl_node_attempts a
JOIN sl_nodes n USING (namespace, workflow_id, run_id, seq)
WHERE n.status = 'COMMITTED' AND a.outcome IN ('WROTE', 'FENCED_OUT')
GROUP BY a.namespace, a.workflow_id, a.run_id, a.seq, n.node;

-- One row per node execution, in order, with timing and how long COMMITTED lagged the node.
CREATE OR REPLACE VIEW sl_node_timeline AS
SELECT
  n.namespace, n.workflow_id, n.run_id, n.seq, n.node, n.lg_step, n.lg_path, n.kind, n.status,
  n.attempt,
  (SELECT count(*) FROM sl_node_attempts a
    WHERE a.namespace = n.namespace AND a.workflow_id = n.workflow_id AND a.run_id = n.run_id
      AND a.seq = n.seq) AS write_attempts,
  n.started_at, n.finished_at,
  round(extract(epoch FROM (n.finished_at - n.started_at)) * 1000) AS duration_ms,
  n.committed_at,
  round(extract(epoch FROM (n.committed_at - n.finished_at)) * 1000) AS commit_lag_ms,
  n.tokens_in, n.tokens_out, n.cost_usd, n.output_hash, n.input_bytes
FROM sl_nodes n;

-- One row per run: status, row counts, spend, retry waste, effects.
CREATE OR REPLACE VIEW sl_run_summary AS
SELECT
  r.namespace, r.workflow_id, r.run_id, r.workflow_type, r.status,
  r.sealed_at IS NOT NULL AS sealed, r.sealed_by, r.degraded, r.missing_seqs,
  r.node_count,
  count(n.seq) AS rows,
  count(n.seq) FILTER (WHERE n.status = 'COMMITTED') AS committed,
  count(n.seq) FILTER (WHERE n.status = 'PROVISIONAL') AS provisional,
  count(n.seq) FILTER (WHERE n.status = 'ABANDONED') AS abandoned,
  coalesce(sum(n.tokens_in), 0) AS tokens_in,
  coalesce(sum(n.tokens_out), 0) AS tokens_out,
  sum(n.cost_usd) AS cost_usd,
  (SELECT coalesce(sum(w.wasted_tokens), 0) FROM sl_retry_waste w
    WHERE w.namespace = r.namespace AND w.workflow_id = r.workflow_id AND w.run_id = r.run_id)
    AS retry_waste_tokens,
  (SELECT count(*) FROM sl_effects e
    WHERE e.namespace = r.namespace AND e.workflow_id = r.workflow_id AND e.run_id = r.run_id)
    AS effects,
  (SELECT coalesce(sum(e.duplicates_prevented), 0) FROM sl_effects e
    WHERE e.namespace = r.namespace AND e.workflow_id = r.workflow_id AND e.run_id = r.run_id)
    AS duplicate_effects_prevented,
  r.first_seen_at, r.sealed_at
FROM sl_runs r
LEFT JOIN sl_nodes n USING (namespace, workflow_id, run_id)
GROUP BY r.namespace, r.workflow_id, r.run_id;
