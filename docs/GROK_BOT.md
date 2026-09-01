# Grok Bot operator setup

This engine hosts a Streamable HTTP MCP endpoint at `/mcp`. Grok Bots are **external** clients. The engine never calls an xAI inference API and never receives wallet or order-placement tools.

Official references used while implementing this path:

- MCP Streamable HTTP via `@modelcontextprotocol/sdk` 1.30 (negotiated protocol versions through 2025-11-25): POST-only MCP endpoint, Origin validation, JSON or SSE responses. The 2026-07-28 spec revision is documented; adopting it requires the MCP TypeScript SDK v2 opt-in, which is deferred so the existing Fastify transport is not rewritten.
- xAI Remote MCP Tools: `server_url` must be Streamable HTTP or SSE; optional `authorization` is sent as `Authorization`.
- Grok connectors: custom MCP connectors are added at [grok.com/connectors](https://grok.com/connectors).

## Token format

Engine secret `GROK_MCP_BEARER_TOKENS` is a comma-separated list of `clientId:token` pairs. Only the **token** goes in the HTTP `Authorization: Bearer <token>` header. The `clientId` is the audited identity.

Example:

```bash
openssl rand -hex 32
fly secrets set -a polymarket-engine \
  GROK_MCP_BEARER_TOKENS='market_microstructure_scanner:<hex>,crypto_market_scanner:<hex>,news_x_intelligence_scanner:<hex>,fair_probability_analyst:<hex>,risk_hedge_analyst:<hex>,strategy_supervisor:<hex>'
```

The dashboard must never receive this secret.

## Exact Grok Bot UI steps

Repeat for each of the six roles, or share one custom connector and pin the role in the bot instructions.

1. Deploy `polymarket-engine` so `https://<engine-app>.fly.dev/mcp` is reachable over public HTTPS. Localhost is not visible to Grok Bot.
2. Open [https://grok.com/connectors](https://grok.com/connectors).
3. Click **New Connector**, then select **Custom**.
4. Set the MCP server URL to `https://<engine-app>.fly.dev/mcp` (path must be `/mcp`; do not use `/health`, `/ready`, or `/sse`).
5. Complete authentication:
   - If the form has a bearer/authorization field, set `Authorization` to `Bearer <token>` using the token half of `clientId:token`.
   - If the form has a generic headers field, add header `Authorization` with value `Bearer <token>`.
   - If the UI only offers OAuth 2.1 and refuses static bearer tokens, stop and add OAuth protected-resource discovery later. This repository intentionally ships operator-managed bearer tokens first.
6. Confirm the connector lists only these tools (there must be **no** `place_order` / wallet tool):
   - Read: `get_markets`, `get_market_snapshot`, `get_orderbook`, `get_recent_triggers`, `get_positions`, `get_portfolio`, `get_strategy_metrics`, `get_recent_decisions`, `get_system_health`
   - Write: `submit_signal`, `submit_analysis`, `recommend_hedge`, `recommend_veto`
7. Create six Grok Bots (or six scheduled tasks) with the stable role IDs below. Each bot must send `agentId` and `role` equal to its ID, plus `timestamp`, `marketId`, `confidence`, `evidence`/`evidenceIds`, `expiresAt`, `reasonCodes`, and `traceId`/`correlationId`.
8. Restrict allowed tools per bot if the UI supports `allowed_tools`:
   - Scanners: read tools + `submit_signal`
   - Fair probability: read tools + `submit_analysis`
   - Risk & hedge: read tools + `recommend_hedge` + `recommend_veto`
   - Supervisor: read tools + `submit_signal`
9. Do **not** attach wallet, exchange, signing, or order connectors to these bots.

## Role IDs and instructions

| Role ID | Display name | Write tool |
|---|---|---|
| `market_microstructure_scanner` | Market Microstructure Scanner | `submit_signal` |
| `crypto_market_scanner` | Crypto Market Scanner | `submit_signal` |
| `news_x_intelligence_scanner` | News / X Intelligence Scanner | `submit_signal` |
| `fair_probability_analyst` | Market / Fair Probability Analyst | `submit_analysis` |
| `risk_hedge_analyst` | Risk & Hedge Analyst | `recommend_hedge` / `recommend_veto` |
| `strategy_supervisor` | Strategy Supervisor | `submit_signal` |

Paste the matching instructions from `packages/core/src/agents.ts`. Every bot must be told: submit structured research only; never place or approve an order; strategy changes remain `PROPOSED`.

## Runtime behavior

Triggers are always persisted. The engine never waits for Grok. Fresh, schema-valid signals are assembled into consensus when minimum role/evidence requirements are met. Stale, duplicate, malformed, unauthorized, and trade-tool calls are rejected and audited. If Grok Bots are down, monitoring continues and the dashboard shows idle/stale — it does not fake activity.
