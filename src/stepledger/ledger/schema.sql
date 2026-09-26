-- Stepledger schema. Applied by `stepledger init-db`; every statement is idempotent.

CREATE TABLE IF NOT EXISTS sl_meta (
  key text PRIMARY KEY, value text NOT NULL);
INSERT INTO sl_meta (key, value) VALUES ('schema_version', '1') ON CONFLICT (key) DO NOTHING;

-- One row per workflow run the ledger has seen.
CREATE TABLE IF NOT EXISTS sl_runs (
  namespace text NOT NULL, workflow_id text NOT NULL, run_id text NOT NULL,
  workflow_type text, status text NOT NULL DEFAULT 'RUNNING'
    CHECK (status IN ('RUNNING','COMPLETED','FAILED','CANCELLED','TERMINATED','TIMED_OUT',
                      'CONTINUED_AS_NEW','DEGRADED')),
  node_count int, committed_count int, abandoned_count int,
  final_state_hash text, first_seen_at timestamptz NOT NULL DEFAULT now(), sealed_at timestamptz,
  sealed_by text CHECK (sealed_by IN ('seal','reconcile')),
  -- Set when on_ledger_error="warn" let a node complete without its row; missing_seqs lists them.
  degraded boolean NOT NULL DEFAULT false, missing_seqs int[],
  PRIMARY KEY (namespace, workflow_id, run_id));

-- One row per node Activity execution: PRIMARY KEY (namespace, workflow_id, run_id, seq).
CREATE TABLE IF NOT EXISTS sl_nodes (
  namespace text NOT NULL, workflow_id text NOT NULL, run_id text NOT NULL, seq int NOT NULL,
  activity_id text NOT NULL, activity_type text NOT NULL, graph text, node text,
  lg_step int, lg_path text, lg_task_id text, checkpoint_ns text,
  attempt int NOT NULL, fence_scheduled_at timestamptz NOT NULL,
  status text NOT NULL CHECK (status IN ('PROVISIONAL','COMMITTED','ABANDONED')),
  kind text NOT NULL CHECK (kind IN ('UPDATE','COMMAND','INTERRUPT')),
  output_json jsonb, output_bytes bytea, output_encoding text, output_hash text NOT NULL,
  input_snapshot jsonb, input_hash text, input_bytes int,
  tokens_in int, tokens_out int, cost_usd numeric(14,6),
  started_at timestamptz NOT NULL, finished_at timestamptz NOT NULL, committed_at timestamptz,
  PRIMARY KEY (namespace, workflow_id, run_id, seq));
CREATE INDEX IF NOT EXISTS sl_nodes_status ON sl_nodes (namespace, workflow_id, run_id, status);

-- Append-only forensics: every write attempt, including fenced-out ones.
CREATE TABLE IF NOT EXISTS sl_node_attempts (
  id bigserial PRIMARY KEY, namespace text, workflow_id text, run_id text, seq int,
  attempt int, fence_scheduled_at timestamptz, output_hash text,
  outcome text CHECK (outcome IN ('WROTE','FENCED_OUT','DB_ERROR','DIVERGENCE_REPAIRED')),
  tokens_in int, tokens_out int, cost_usd numeric(14,6),
  write_ms real,                                       -- ledger transaction time for this attempt
  worker text, note text, at timestamptz DEFAULT now());
CREATE INDEX IF NOT EXISTS sl_node_attempts_run ON sl_node_attempts (namespace, workflow_id, run_id, seq);
-- the writing worker's own clock at write time (forensics: clock skew never affects the fence)
ALTER TABLE sl_node_attempts ADD COLUMN IF NOT EXISTS worker_time timestamptz;

-- Effect journal for once().
CREATE TABLE IF NOT EXISTS sl_effects (
  key text PRIMARY KEY, namespace text, workflow_id text, run_id text, seq int, name text, idx int,
  request_hash text NOT NULL,
  status text NOT NULL CHECK (status IN ('STARTED','DONE','UNKNOWN','RESOLVED')),
  result jsonb, started_at timestamptz DEFAULT now(), done_at timestamptz,
  attempts int NOT NULL DEFAULT 1, duplicates_prevented int NOT NULL DEFAULT 0,
  resolution text);

-- LLM journal.
CREATE TABLE IF NOT EXISTS sl_llm_calls (
  key text PRIMARY KEY, request_hash text NOT NULL, response jsonb NOT NULL,
  namespace text, workflow_id text, run_id text, seq int, call_idx int,
  tokens_in int, tokens_out int, cost_usd numeric(14,6), first_attempt int,
  replays int NOT NULL DEFAULT 0, created_at timestamptz NOT NULL DEFAULT now());

-- Deduplicating External Storage: content-addressed chunks, manifests, and references.
CREATE TABLE IF NOT EXISTS sl_chunks (
  hash bytea PRIMARY KEY, data bytea NOT NULL, size int NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  last_ref_at timestamptz NOT NULL DEFAULT now());     -- touched by every store that uses the chunk
CREATE TABLE IF NOT EXISTS sl_payloads (               -- manifests; shared across runs and workflows
  claim text PRIMARY KEY,                              -- sha256 hex of the full serialized Payload
  chunks bytea[] NOT NULL, size bigint NOT NULL, encoding text, deduped boolean NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  last_ref_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS sl_payload_refs (           -- who may still need a claim; written on EVERY store
  claim text NOT NULL, namespace text NOT NULL,
  workflow_id text NOT NULL DEFAULT '',                -- '' when the store context has no target
  run_id text NOT NULL DEFAULT '',                     -- '' when unknown (client start, child start)
  target_kind text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
  last_ref_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (claim, namespace, workflow_id, run_id));
CREATE INDEX IF NOT EXISTS sl_payload_refs_wf ON sl_payload_refs (namespace, workflow_id);
-- GC asks "does any manifest still list this chunk?" per chunk; GIN answers without a scan.
CREATE INDEX IF NOT EXISTS sl_payloads_chunks ON sl_payloads USING gin (chunks);
-- GC scans candidates by age; without these every sweep batch reads the whole table.
CREATE INDEX IF NOT EXISTS sl_chunks_last_ref ON sl_chunks (last_ref_at);
CREATE INDEX IF NOT EXISTS sl_payloads_last_ref ON sl_payloads (last_ref_at);
