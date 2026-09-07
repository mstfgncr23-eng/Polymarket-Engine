-- PR18 integrated storage correctness. Additive and safe for existing installs.
CREATE TABLE IF NOT EXISTS durable_replay_keys (
  operation_key text PRIMARY KEY,
  type text NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_id text NOT NULL,
  payload_hash text NOT NULL,
  sequence bigint NOT NULL,
  occurred_at timestamptz NOT NULL,
  recorded_at timestamptz NOT NULL,
  expires_at timestamptz NOT NULL,
  lifecycle_state text NOT NULL DEFAULT 'closed'
);
CREATE INDEX IF NOT EXISTS durable_replay_keys_sequence_idx ON durable_replay_keys(sequence);
CREATE INDEX IF NOT EXISTS durable_replay_keys_expiry_idx ON durable_replay_keys(expires_at);
ALTER TABLE durable_replay_keys ENABLE ROW LEVEL SECURITY;
DO $$ DECLARE r text; BEGIN
  FOREACH r IN ARRAY ARRAY['anon','authenticated'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN EXECUTE format('REVOKE ALL ON TABLE durable_replay_keys FROM %I', r); END IF;
  END LOOP;
END $$;

ALTER TABLE experiment_report_rollups ADD COLUMN IF NOT EXISTS source_event_count integer NOT NULL DEFAULT 0;
ALTER TABLE experiment_report_rollups ADD COLUMN IF NOT EXISTS source_commit_seq bigint NOT NULL DEFAULT 0;
ALTER TABLE experiment_report_rollups ADD COLUMN IF NOT EXISTS source_recorded_at timestamptz;
ALTER TABLE experiment_report_rollups ADD COLUMN IF NOT EXISTS stable_read boolean NOT NULL DEFAULT false;
ALTER TABLE experiment_report_rollups ADD COLUMN IF NOT EXISTS previous_content_hash text;
ALTER TABLE experiment_report_rollups ADD COLUMN IF NOT EXISTS coverage_hash text;
