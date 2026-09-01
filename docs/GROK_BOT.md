# Grok Bot operator setup

This engine hosts a Streamable HTTP MCP endpoint at `/mcp`. Grok Bots are **external** clients. The engine never calls an xAI inference API and never receives wallet or order-placement tools.

Official references used while implementing this path:

- MCP Streamable HTTP via `@modelcontextprotocol/sdk` 1.30 (negotiated protocol versions through 2025-11-25): POST-only MCP endpoint, Origin validation, JSON or SSE responses. The 2026-07-28 spec revision is documented; adopting it requires the MCP TypeScript SDK v2 opt-in, which is deferred so the existing Fastify transport is not rewritten.
- xAI Remote MCP Tools: `server_url` must be Streamable HTTP or SSE; optional `authorization` is sent as `Authorization`.
- Grok connectors: custom MCP connectors are added at [grok.com/connectors](https://grok.com/connectors).

## Token format and identity binding

Engine secret `GROK_MCP_BEARER_TOKENS` is a comma-separated list of `clientId:token` pairs. **`clientId` must be exactly one of the six role IDs.** Only the **token** goes in `Authorization: Bearer <token>`. The authenticated `clientId` is bound to writes:

`authenticated clientId === submission.agentId === submission.role`

A token issued for `market_microstructure_scanner` cannot submit as `fair_probability_analyst`, `risk_hedge_analyst`, `strategy_supervisor`, or any other role. Role-to-write-tool authorization is enforced on the engine, not only in Bot instructions:

| Authenticated `clientId` | Allowed write tools |
|---|---|
| `market_microstructure_scanner` | `submit_signal` |
| `crypto_market_scanner` | `submit_signal` |
| `news_x_intelligence_scanner` | `submit_signal` |
| `fair_probability_analyst` | `submit_analysis` |
| `risk_hedge_analyst` | `recommend_hedge`, `recommend_veto` |
| `strategy_supervisor` | `submit_signal` |

Do **not** share one bearer token across Bots. Give each role its own `clientId:token` pair.

Example:

```bash
openssl rand -hex 32
fly secrets set -a polymarket-engine \
  GROK_MCP_BEARER_TOKENS='market_microstructure_scanner:<hex>,crypto_market_scanner:<hex>,news_x_intelligence_scanner:<hex>,fair_probability_analyst:<hex>,risk_hedge_analyst:<hex>,strategy_supervisor:<hex>'
```

The dashboard must never receive this secret.

## Exact Grok Bot UI steps

Repeat for each of the six roles. Each Bot must use **its own** bearer token whose `clientId` is that role ID. Do not share one token.

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

Triggers are always persisted. The engine never waits for Grok. Fresh, schema-valid signals from the **authenticated** role are assembled into consensus when minimum role/evidence requirements are met. Stale, duplicate, malformed, unauthorized, impersonated, forbidden-tool, and trade-tool calls are rejected and audited. If Grok Bots are down, monitoring continues and the dashboard shows idle/stale — it does not fake activity.

## Event-driven activation: official limitation

An MCP server cannot wake a Grok Bot. Persisting `GROK_RESEARCH_AVAILABLE` and exposing `get_recent_triggers` is **not** event-driven activation; those are pull surfaces for a Bot that is already running.

Current official Grok Bot documentation (retrieved 2026-09-01) does **not** document a custom inbound webhook or HTTP event URL that this engine could POST to after a Polymarket trigger. No such wake-up path was implemented. This repository still does not call the xAI inference API.

Observed official activation mechanisms:

| Mechanism | What the docs say | Latency / limits | Source |
|---|---|---|---|
| Manual message | You work with a Bot by messaging it. | Immediate when a human/other Bot prompts it. | [Grok Bot overview](https://docs.x.ai/grok-bot/overview) |
| Scheduled routine | A routine runs a workflow on a schedule; background routines can run while the laptop is closed. A Bot can own up to 50 routines; 20 recent run records are kept. | Latency equals the schedule interval you choose (seconds are not documented as a supported grain; examples are weekday morning jobs). Overly frequent schedules consume weekly Grok Bot usage. | [Skills and routines](https://docs.x.ai/grok-bot/skills-routines-and-automations), [FAQ](https://docs.x.ai/grok-bot/faq) |
| Cursor account integration events | Routines can start from an event **where supported**, such as a Slack message or GitHub notification. These are Cursor account integrations, separate from Slack/GitHub plugins. Docs warn against broad listeners. | Latency is Slack/GitHub delivery plus routine start (not quantified). Not a custom Polymarket webhook. This engine does not post to Slack/GitHub. | [Skills and routines — Trigger work from an event](https://docs.x.ai/grok-bot/skills-routines-and-automations) |
| Custom MCP connector | Grok discovers tools on a public Streamable HTTP MCP URL and uses them **in conversations / while a routine is already running**. | Pull-only. No documented server-to-Bot push. | [Connectors](https://docs.x.ai/grok/connectors), [xAI remote MCP tools](https://docs.x.ai/developers/tools/remote-mcp) |
| Grok Build hooks | Grok **calls out** to command/HTTP hooks on *its* lifecycle events (`PreToolUse`, `Stop`, …). Direction is Bot → your URL, not engine → Bot. | Not an inbound Bot trigger. | [Hooks](https://docs.x.ai/build/features/hooks) |

### Operator subscription-only options (no engine wake-up code)

1. **Scheduled pull (viable, not event-driven).** Create six routines, one per role, that periodically: call `get_recent_triggers`, skip expired/already-handled IDs, read snapshots, submit through the role’s write tool. Expected latency ≈ routine interval. If the interval is longer than trigger TTL (`triggerTtlMs`, default 30s), most research windows will have expired before the Bot runs. A shorter interval burns Grok Bot weekly usage. Official examples are calendar-style (“every weekday at 8:00 AM”), not sub-second market events.
2. **Cursor Slack/GitHub event (operator-owned, not implemented here).** If you already connected Slack or GitHub in Cursor, you could *separately* notify that channel and teach a routine “when a message in #… contains `GROK_RESEARCH_AVAILABLE`, run the research skill.” This engine does not send those notifications; wiring them would be outside Grok Bot’s documented custom-webhook surface and would add unrelated chat credentials.
3. **On-demand message.** Mention/`@` the Bot when you want a one-off research pass.

Until xAI documents a custom inbound webhook for Grok Bot, Polymarket trigger → immediate Grok start is **not available** without using the xAI inference API (which this repo will not call).
