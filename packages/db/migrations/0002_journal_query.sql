CREATE INDEX IF NOT EXISTS journal_type_time_idx ON journal_events(type, occurred_at);
CREATE INDEX IF NOT EXISTS journal_experiment_idx ON journal_events((payload->>'experimentId'));
