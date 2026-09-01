-- Engine RLS lockdown
--
-- Direct Postgres (Fly engine DATABASE_URL as table owner / postgres) bypasses RLS
-- unless FORCE ROW LEVEL SECURITY is set. Do not FORCE. Do not add anon/authenticated
-- policies — the engine does not use the Supabase Data API.
--
-- Idempotent: safe if RLS was already enabled during operator MCP apply.
-- REVOKE is skipped when anon/authenticated roles are absent (local docker-compose Postgres).

ALTER TABLE journal_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE engine_checkpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE markets ENABLE ROW LEVEL SECURITY;
ALTER TABLE market_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE trigger_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_invocations ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_outputs ENABLE ROW LEVEL SECURITY;
ALTER TABLE consensus_decisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE risk_evaluations ENABLE ROW LEVEL SECURITY;
ALTER TABLE paper_orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE paper_fills ENABLE ROW LEVEL SECURITY;
ALTER TABLE positions ENABLE ROW LEVEL SECURITY;
ALTER TABLE hedges ENABLE ROW LEVEL SECURITY;
ALTER TABLE pnl_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE ai_costs ENABLE ROW LEVEL SECURITY;
ALTER TABLE system_health_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE strategy_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE proposed_strategy_changes ENABLE ROW LEVEL SECURITY;
ALTER TABLE experiments ENABLE ROW LEVEL SECURITY;
ALTER TABLE mcp_idempotency_keys ENABLE ROW LEVEL SECURITY;

DO $$
DECLARE
  r text;
  t text;
  tables text[] := ARRAY[
    'journal_events', 'engine_checkpoints', 'markets', 'market_snapshots',
    'trigger_events', 'agent_invocations', 'agent_outputs', 'consensus_decisions',
    'risk_evaluations', 'paper_orders', 'paper_fills', 'positions', 'hedges',
    'pnl_snapshots', 'ai_costs', 'system_health_events', 'strategy_versions',
    'proposed_strategy_changes', 'experiments', 'mcp_idempotency_keys'
  ];
BEGIN
  FOREACH r IN ARRAY ARRAY['anon', 'authenticated'] LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
      FOREACH t IN ARRAY tables LOOP
        EXECUTE format('REVOKE ALL ON TABLE %I FROM %I', t, r);
      END LOOP;
    END IF;
  END LOOP;
END $$;
