-- Replay-authoritative critical transitions.
-- Dedicated table is the single recovery source: journal_events stays the
-- concurrent audit/telemetry log (PR #12 admission unchanged). Sequences are
-- assigned only inside the durable-mutation advisory lock so a checkpoint
-- cursor cannot pass a lower-sequence uncommitted/unapplied transition.
--
-- Preserve RLS posture: engine PG access, RLS on, no public policies / no Data API.
-- Do not FORCE RLS. Do not add anon/authenticated policies.

CREATE TABLE IF NOT EXISTS durable_transitions (
  sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  operation_key text NOT NULL,
  type text NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_id text NOT NULL,
  payload_hash text NOT NULL,
  payload jsonb NOT NULL,
  occurred_at timestamptz NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT durable_transitions_operation_key_uidx UNIQUE (operation_key)
);

CREATE INDEX IF NOT EXISTS durable_transitions_sequence_idx ON durable_transitions (sequence);

ALTER TABLE durable_transitions ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE
  r text;
BEGIN
  FOREACH r IN ARRAY ARRAY['anon', 'authenticated'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
      EXECUTE format('REVOKE ALL ON TABLE durable_transitions FROM %I', r);
    END IF;
  END LOOP;
END $$;
