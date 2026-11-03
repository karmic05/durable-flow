export const SCHEMA = `
CREATE TABLE IF NOT EXISTS workflows (
  id text PRIMARY KEY,
  type text NOT NULL,
  status text NOT NULL DEFAULT 'running',
  input jsonb,
  result jsonb,
  error jsonb,
  lease_owner text,
  lease_expires_at timestamptz,
  lease_epoch bigint NOT NULL DEFAULT 0,
  next_wake_at timestamptz NOT NULL DEFAULT now(),
  next_seq int NOT NULL DEFAULT 1,
  parent_id text,
  parent_cmd int,
  root_id text NOT NULL,
  code_hash text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS workflows_claim_idx ON workflows (status, next_wake_at);
CREATE INDEX IF NOT EXISTS workflows_parent_idx ON workflows (parent_id);

CREATE TABLE IF NOT EXISTS events (
  id bigserial,
  workflow_id text NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
  seq int NOT NULL,
  type text NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}',
  ts timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (workflow_id, seq)
);
CREATE UNIQUE INDEX IF NOT EXISTS events_id_idx ON events (id);

CREATE TABLE IF NOT EXISTS workers (
  id text PRIMARY KEY,
  pid int,
  started_at timestamptz NOT NULL DEFAULT now(),
  last_seen timestamptz NOT NULL DEFAULT now(),
  status text NOT NULL DEFAULT 'alive',
  active jsonb NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS settings (key text PRIMARY KEY, value jsonb NOT NULL);

-- The simulated "outside world". Unique only when idempotency protection is ON (protected = true).
CREATE TABLE IF NOT EXISTS side_effects (
  id bigserial PRIMARY KEY,
  idempotency_key text NOT NULL,
  service text NOT NULL,
  action text NOT NULL,
  payload jsonb,
  protected boolean NOT NULL DEFAULT true,
  workflow_id text,
  ts timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS side_effects_protected_idx ON side_effects (idempotency_key) WHERE protected;
CREATE INDEX IF NOT EXISTS side_effects_key_idx ON side_effects (idempotency_key);

CREATE TABLE IF NOT EXISTS effect_calls (
  id bigserial PRIMARY KEY,
  idempotency_key text NOT NULL,
  service text NOT NULL,
  action text NOT NULL,
  deduped boolean NOT NULL,
  workflow_id text,
  ts timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS spans (
  id bigserial PRIMARY KEY,
  workflow_id text NOT NULL,
  trace_id text NOT NULL,
  span_id text NOT NULL,
  parent_span_id text,
  name text NOT NULL,
  start_ms double precision NOT NULL,
  end_ms double precision NOT NULL,
  status text NOT NULL DEFAULT 'ok',
  attributes jsonb NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS spans_wf_idx ON spans (workflow_id);
`;
