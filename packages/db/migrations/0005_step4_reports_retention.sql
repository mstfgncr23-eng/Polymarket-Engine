-- Step-4 durable experiment reports + keyset/retention indexes.
-- Additive only. Do not rewrite 0000–0004. Do not drop or rewrite existing relations.
-- Production currently lacks durable_transitions until 0004 is applied by
-- migrate-on-boot; this file must run after 0004. Schema verification fails
-- closed if durable_transitions or experiment_reports is absent.
--
-- Preserve RLS posture: ENABLE RLS, no policies, no FORCE, revoke
-- anon/authenticated only when those roles exist. Engine uses direct Postgres.

CREATE TABLE IF NOT EXISTS experiment_reports (
  experiment_id text NOT NULL,
  report_kind text NOT NULL,
  schema_version integer NOT NULL,
  generated_at timestamptz NOT NULL,
  complete boolean NOT NULL,
  truncated boolean NOT NULL,
  source_event_count integer NOT NULL,
  source_window_start timestamptz,
  source_window_end timestamptz,
  source_fence_occurred_at timestamptz,
  source_fence_id text,
  earliest_included_at timestamptz,
  latest_included_at timestamptz,
  canonical_summary jsonb NOT NULL,
  rendered_markdown text NOT NULL,
  content_hash text NOT NULL,
  persisted_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (experiment_id, report_kind)
);

CREATE INDEX IF NOT EXISTS experiment_reports_hash_idx
  ON experiment_reports (experiment_id, content_hash);
CREATE INDEX IF NOT EXISTS experiment_reports_complete_idx
  ON experiment_reports (complete, truncated, persisted_at);

-- Large journal_events keyset indexes are NOT created here. A transactional
-- CREATE INDEX on ~1M live rows can block writers. Fresh/local boot creates
-- them synchronously only when journal_events is small. Production must run
-- packages/db/operator/create-journal-keyset-indexes.concurrent.sql
-- (CREATE INDEX CONCURRENTLY) before readiness. Schema verification fails
-- closed if those indexes are missing or INVALID.

CREATE INDEX IF NOT EXISTS mcp_idempotency_created_at_idx
  ON mcp_idempotency_keys (created_at);

ALTER TABLE experiment_reports ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE
  r text;
BEGIN
  FOREACH r IN ARRAY ARRAY['anon', 'authenticated'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
      EXECUTE format('REVOKE ALL ON TABLE experiment_reports FROM %I', r);
    END IF;
  END LOOP;
END $$;
