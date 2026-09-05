-- Operator / predeploy ONLY. Do not run inside a Drizzle migration transaction.
-- CREATE INDEX CONCURRENTLY cannot run in a transaction block.
--
-- Production sequence:
--   1. Apply 0000–0005 via engine boot or an explicit migrate window (tables only).
--   2. Run this file against the same database (autocommit / CONCURRENTLY).
--   3. Restart / wait for schema verification. Readiness refuses missing or INVALID indexes.
--
-- This PR does not execute this file against production.

CREATE INDEX CONCURRENTLY IF NOT EXISTS journal_occurred_id_idx
  ON journal_events (occurred_at, id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS journal_type_occurred_id_idx
  ON journal_events (type, occurred_at, id);
