-- PR18 runtime closure: compact lifecycle scope for bounded economic replay.
-- Scope is metadata only; the authoritative payload remains in the original
-- durable transition/checkpoint and is never copied into replay keys.
ALTER TABLE durable_replay_keys ADD COLUMN IF NOT EXISTS scope_id text;
CREATE INDEX IF NOT EXISTS durable_replay_keys_scope_idx ON durable_replay_keys(scope_id, lifecycle_state);
