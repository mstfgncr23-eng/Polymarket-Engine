-- PR18 final storage closure: restart-safe journal admission fence.
-- The fence is advanced and the event row inserted in one transaction. A
-- repeatable-read snapshot therefore cannot accidentally claim an event that
-- commits after the rollup fence.
CREATE TABLE IF NOT EXISTS journal_commit_metadata (
  id integer PRIMARY KEY,
  high_water bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO journal_commit_metadata (id, high_water)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

ALTER TABLE journal_events
  ADD COLUMN IF NOT EXISTS commit_seq bigint;

-- Backfill legacy rows deterministically before making the column mandatory.
WITH ordered AS (
  SELECT id, row_number() OVER (ORDER BY recorded_at ASC, occurred_at ASC, id ASC) AS seq
  FROM journal_events
  WHERE commit_seq IS NULL OR commit_seq = 0
)
UPDATE journal_events e
SET commit_seq = ordered.seq
FROM ordered
WHERE e.id = ordered.id;

UPDATE journal_commit_metadata
SET high_water = GREATEST(high_water, COALESCE((SELECT max(commit_seq) FROM journal_events), 0)), updated_at = now()
WHERE id = 1;

ALTER TABLE journal_events ALTER COLUMN commit_seq SET DEFAULT 0;
ALTER TABLE journal_events ALTER COLUMN commit_seq SET NOT NULL;
CREATE INDEX IF NOT EXISTS journal_commit_seq_idx ON journal_events(commit_seq);

-- Cover maintenance/test writers that insert journal rows directly. The
-- application may supply a preallocated value; otherwise the trigger assigns
-- one while holding the metadata row lock in the same transaction.
CREATE OR REPLACE FUNCTION assign_journal_commit_seq() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE current_high bigint;
BEGIN
  INSERT INTO journal_commit_metadata (id, high_water) VALUES (1, 0)
  ON CONFLICT (id) DO NOTHING;
  SELECT high_water INTO current_high FROM journal_commit_metadata WHERE id = 1 FOR UPDATE;
  IF NEW.commit_seq IS NULL OR NEW.commit_seq <= 0 THEN
    NEW.commit_seq := current_high + 1;
    UPDATE journal_commit_metadata SET high_water = NEW.commit_seq, updated_at = now() WHERE id = 1;
  ELSIF NEW.commit_seq > current_high THEN
    UPDATE journal_commit_metadata SET high_water = NEW.commit_seq, updated_at = now() WHERE id = 1;
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS journal_events_commit_seq_trigger ON journal_events;
CREATE TRIGGER journal_events_commit_seq_trigger
BEFORE INSERT ON journal_events
FOR EACH ROW EXECUTE FUNCTION assign_journal_commit_seq();

ALTER TABLE journal_commit_metadata ENABLE ROW LEVEL SECURITY;
DO $$ DECLARE r text; BEGIN
  FOREACH r IN ARRAY ARRAY['anon','authenticated'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN EXECUTE format('REVOKE ALL ON TABLE journal_commit_metadata FROM %I', r); END IF;
  END LOOP;
END $$;
