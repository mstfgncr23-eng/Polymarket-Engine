-- PR18 hot operational storage lifecycle. Additive only; 0000-0005 remain unchanged.
-- These compact singleton/rollup relations carry recovery authority after old
-- durable rows and canonical raw report input become eligible for cleanup.

CREATE TABLE IF NOT EXISTS durable_sequence_metadata (
  id integer PRIMARY KEY,
  high_water bigint NOT NULL DEFAULT 0,
  compacted_operation_keys jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO durable_sequence_metadata (id, high_water)
VALUES (1, COALESCE((SELECT max(sequence) FROM durable_transitions), 0))
ON CONFLICT (id) DO UPDATE SET high_water = GREATEST(durable_sequence_metadata.high_water, EXCLUDED.high_water);

CREATE TABLE IF NOT EXISTS experiment_report_rollups (
  experiment_id text NOT NULL,
  rollup_kind text NOT NULL,
  schema_version integer NOT NULL,
  source_cursor bigint NOT NULL,
  source_fence_id text,
  source_fence_occurred_at timestamptz,
  accumulator jsonb NOT NULL,
  content_hash text NOT NULL,
  verified boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (experiment_id, rollup_kind)
);

CREATE INDEX IF NOT EXISTS experiment_report_rollups_cursor_idx
  ON experiment_report_rollups (source_cursor);

ALTER TABLE durable_sequence_metadata ENABLE ROW LEVEL SECURITY;
ALTER TABLE experiment_report_rollups ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE r text;
BEGIN
  FOREACH r IN ARRAY ARRAY['anon', 'authenticated'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
      EXECUTE format('REVOKE ALL ON TABLE durable_sequence_metadata FROM %I', r);
      EXECUTE format('REVOKE ALL ON TABLE experiment_report_rollups FROM %I', r);
    END IF;
  END LOOP;
END $$;
