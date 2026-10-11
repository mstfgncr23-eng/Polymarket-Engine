-- G2H additive registration authority only.
-- CRITICAL: this table does NOT make automatic compaction safe.
-- Runtime guardianRecoveryHold MUST remain enabled until a real
-- economically verified durable receipt + atomic deletion fence exists.
CREATE TABLE IF NOT EXISTS guardian_recovery_roster (
  id smallint PRIMARY KEY CHECK (id=1),
  source_identity text NOT NULL CHECK (length(source_identity) BETWEEN 1 AND 256),
  sealed boolean NOT NULL DEFAULT false,
  member_count integer NOT NULL CHECK(member_count BETWEEN 1 AND 32),
  member_hash text NOT NULL CHECK(member_hash ~ '^[a-f0-9]{64}$'),
  revision bigint NOT NULL DEFAULT 1 CHECK (revision>=1),
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS guardian_recovery_pins (
  consumer_id text PRIMARY KEY CHECK (length(consumer_id) BETWEEN 1 AND 128),
  experiment_id text NOT NULL CHECK (length(experiment_id) BETWEEN 1 AND 128),
  checkpoint_id text NOT NULL REFERENCES engine_checkpoints(id) ON DELETE RESTRICT,
  checkpoint_hash text NOT NULL CHECK (checkpoint_hash ~ '^[a-f0-9]{64}$'),
  checkpoint_cursor bigint NOT NULL CHECK (checkpoint_cursor>=0),
  authority_epoch bigint NOT NULL CHECK (authority_epoch>=1),
  revision bigint NOT NULL DEFAULT 1 CHECK (revision>=1),
  required boolean NOT NULL DEFAULT true CHECK (required),
  enrolled_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS guardian_recovery_pins_checkpoint_idx
 ON guardian_recovery_pins(checkpoint_id);

ALTER TABLE guardian_recovery_roster ENABLE ROW LEVEL SECURITY;
ALTER TABLE guardian_recovery_pins ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON guardian_recovery_roster FROM PUBLIC;
REVOKE ALL ON guardian_recovery_pins FROM PUBLIC;
-- Never grant INSERT/UPDATE/DELETE to the Guardian SELECT-only role.
-- No anonymous/RPC membership discovery policy and NO table-driven
-- authorization to turn off the default retention HOLD.
