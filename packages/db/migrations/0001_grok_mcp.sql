CREATE TABLE IF NOT EXISTS mcp_idempotency_keys (
  scope text NOT NULL,
  key text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS mcp_idempotency_scope_key_idx
  ON mcp_idempotency_keys(scope, key);
