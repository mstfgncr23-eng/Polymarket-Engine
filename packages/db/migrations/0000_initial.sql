CREATE TABLE IF NOT EXISTS journal_events (
  id text PRIMARY KEY,
  trace_id text NOT NULL,
  correlation_id text NOT NULL,
  type text NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_id text NOT NULL,
  payload jsonb NOT NULL,
  occurred_at timestamptz NOT NULL,
  recorded_at timestamptz NOT NULL DEFAULT now(),
  source_event_id text
);
CREATE INDEX IF NOT EXISTS journal_trace_idx ON journal_events(trace_id);
CREATE INDEX IF NOT EXISTS journal_aggregate_idx ON journal_events(aggregate_type, aggregate_id, occurred_at);

CREATE TABLE IF NOT EXISTS engine_checkpoints (
  id text PRIMARY KEY,
  experiment_id text,
  sequence integer NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS checkpoint_sequence_idx ON engine_checkpoints(sequence);

CREATE TABLE IF NOT EXISTS markets (id text PRIMARY KEY, slug text NOT NULL UNIQUE, question text NOT NULL, condition_id text NOT NULL, active boolean NOT NULL, payload jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS market_snapshots (id text PRIMARY KEY, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL, market_id text NOT NULL, token_id text NOT NULL, source_timestamp timestamptz NOT NULL, sequence integer NOT NULL);
CREATE TABLE IF NOT EXISTS trigger_events (id text PRIMARY KEY, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL, market_id text, kind text NOT NULL, severity integer NOT NULL, dedupe_key text NOT NULL, expires_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS agent_invocations (id text PRIMARY KEY, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL, trigger_id text NOT NULL, agent_id text NOT NULL, model text NOT NULL, status text NOT NULL, duration_ms integer, estimated_cost_usd numeric(20,10));
CREATE TABLE IF NOT EXISTS agent_outputs (id text PRIMARY KEY, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL, invocation_id text NOT NULL UNIQUE, agent_id text NOT NULL, expires_at timestamptz);
CREATE TABLE IF NOT EXISTS consensus_decisions (id text PRIMARY KEY, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL, trigger_id text NOT NULL, direction text NOT NULL, confidence numeric(12,10) NOT NULL);
CREATE TABLE IF NOT EXISTS risk_evaluations (id text PRIMARY KEY, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL, accepted boolean NOT NULL, reason_codes jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS paper_orders (id text PRIMARY KEY, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL, trigger_id text NOT NULL, market_id text NOT NULL, token_id text NOT NULL, status text NOT NULL);
CREATE TABLE IF NOT EXISTS paper_fills (id text PRIMARY KEY, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL, order_id text NOT NULL, market_id text NOT NULL, source_market_event_id text NOT NULL, quantity numeric(38,18) NOT NULL, fill_price numeric(38,18) NOT NULL);
CREATE TABLE IF NOT EXISTS positions (id text PRIMARY KEY, trace_id text NOT NULL, market_id text NOT NULL UNIQUE, payload jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS hedges (id text PRIMARY KEY, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL, market_id text NOT NULL, first_leg_fill_id text NOT NULL, second_leg_fill_id text, status text NOT NULL);
CREATE TABLE IF NOT EXISTS pnl_snapshots (id text PRIMARY KEY, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL, experiment_id text NOT NULL, equity numeric(28,10) NOT NULL, drawdown numeric(28,10) NOT NULL);
CREATE TABLE IF NOT EXISTS ai_costs (id text PRIMARY KEY, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL, invocation_id text NOT NULL, agent_id text NOT NULL, model text NOT NULL, input_tokens integer NOT NULL, output_tokens integer NOT NULL, estimated_cost_usd numeric(20,10));
CREATE TABLE IF NOT EXISTS system_health_events (id text PRIMARY KEY, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL, component text NOT NULL, status text NOT NULL);
CREATE TABLE IF NOT EXISTS strategy_versions (id text PRIMARY KEY, version text NOT NULL UNIQUE, status text NOT NULL, configuration jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS proposed_strategy_changes (id text PRIMARY KEY, trace_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), payload jsonb NOT NULL, strategy_version_id text NOT NULL, status text NOT NULL DEFAULT 'PROPOSED', accepted_at timestamptz);
CREATE TABLE IF NOT EXISTS experiments (id text PRIMARY KEY, trace_id text NOT NULL, status text NOT NULL, started_at timestamptz NOT NULL, entry_closes_at timestamptz NOT NULL, completed_at timestamptz, payload jsonb NOT NULL);
