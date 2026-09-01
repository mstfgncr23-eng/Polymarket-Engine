# Engine RLS lockdown
#
# Direct Postgres (Fly engine DATABASE_URL as table owner / postgres) bypasses RLS
# unless FORCE ROW LEVEL SECURITY is set. Do not FORCE. Do not add anon/authenticated
# policies — the engine does not use the Supabase Data API.
#
# Idempotent: safe if RLS was already enabled during operator MCP apply.

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

REVOKE ALL ON TABLE journal_events FROM anon, authenticated;
REVOKE ALL ON TABLE engine_checkpoints FROM anon, authenticated;
REVOKE ALL ON TABLE markets FROM anon, authenticated;
REVOKE ALL ON TABLE market_snapshots FROM anon, authenticated;
REVOKE ALL ON TABLE trigger_events FROM anon, authenticated;
REVOKE ALL ON TABLE agent_invocations FROM anon, authenticated;
REVOKE ALL ON TABLE agent_outputs FROM anon, authenticated;
REVOKE ALL ON TABLE consensus_decisions FROM anon, authenticated;
REVOKE ALL ON TABLE risk_evaluations FROM anon, authenticated;
REVOKE ALL ON TABLE paper_orders FROM anon, authenticated;
REVOKE ALL ON TABLE paper_fills FROM anon, authenticated;
REVOKE ALL ON TABLE positions FROM anon, authenticated;
REVOKE ALL ON TABLE hedges FROM anon, authenticated;
REVOKE ALL ON TABLE pnl_snapshots FROM anon, authenticated;
REVOKE ALL ON TABLE ai_costs FROM anon, authenticated;
REVOKE ALL ON TABLE system_health_events FROM anon, authenticated;
REVOKE ALL ON TABLE strategy_versions FROM anon, authenticated;
REVOKE ALL ON TABLE proposed_strategy_changes FROM anon, authenticated;
REVOKE ALL ON TABLE experiments FROM anon, authenticated;
REVOKE ALL ON TABLE mcp_idempotency_keys FROM anon, authenticated;
