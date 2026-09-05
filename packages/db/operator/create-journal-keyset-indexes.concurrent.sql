-- Operator / predeploy ONLY. Do not run inside a Drizzle migration transaction.
-- CREATE INDEX CONCURRENTLY cannot run in a transaction block.
--
-- Production sequence:
--   1. Apply 0000–0005 via engine boot or an explicit migrate window (tables only).
--   2. Run this file against the same database (autocommit / CONCURRENTLY).
--   3. Restart / wait for schema verification. Readiness refuses missing or INVALID indexes.
--
-- Interrupted CREATE INDEX CONCURRENTLY leaves an INVALID index. IF NOT EXISTS
-- will not rebuild an existing invalid index of the same name. Recovery:
--   SELECT c.relname, i.indisvalid, i.indisready
--     FROM pg_index i
--     JOIN pg_class c ON c.oid = i.indexrelid
--     JOIN pg_namespace n ON n.oid = c.relnamespace
--    WHERE n.nspname = 'public'
--      AND c.relname IN ('journal_occurred_id_idx', 'journal_type_occurred_id_idx');
--   DROP INDEX CONCURRENTLY IF EXISTS journal_occurred_id_idx;
--   DROP INDEX CONCURRENTLY IF EXISTS journal_type_occurred_id_idx;
--   then re-run this file and verify indisvalid before restart.
--
-- This PR does not execute this file against production.

CREATE INDEX CONCURRENTLY IF NOT EXISTS journal_occurred_id_idx
  ON journal_events (occurred_at, id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS journal_type_occurred_id_idx
  ON journal_events (type, occurred_at, id);
