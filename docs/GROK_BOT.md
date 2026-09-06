# Grok Bot operator setup

This engine hosts a Streamable HTTP MCP endpoint at `/mcp`. Grok Bots are **external** clients. The engine never calls an xAI inference API and never receives wallet or order-placement tools.

**Product boundary (do not mix):** Grok Bot (desktop/iOS, Cursor sign-in) is not grok.com chat and is not the xAI inference API. This document covers Grok Bot only.

Official Grok Bot sources used for activation and MCP-during-work (retrieved 2026-09-01):

- [Skills and routines](https://docs.x.ai/grok-bot/skills-routines-and-automations) — schedules; event triggers only via Cursor-account integrations (examples: Slack message, GitHub notification); up to 50 routines per Bot; background work with laptop closed
- [Troubleshooting](https://docs.x.ai/grok-bot/troubleshooting) — event-triggered routines: confirm source channel, repository, and matching rule
- [Introducing Grok Bot](https://x.ai/news/introducing-grok-bot) — Bots have a cloud computer and keep working 24/7; you message them like a teammate
- [Message and collaborate](https://docs.x.ai/grok-bot/chat-and-collaboration) — user chat; a Bot-to-Bot message can wake the receiving Bot
- [Overview](https://docs.x.ai/grok-bot/overview) — work with a Bot by messaging it; connectors/MCP where available
- [Computer and apps](https://docs.x.ai/grok-bot/computer-and-apps) — Settings → Plugins for connectors
- [Teams and enterprises](https://docs.x.ai/grok-bot/teams-and-enterprises) — Grok Bot inherits Cursor MCP policy; MCP auth is shared across Cursor + Grok Bot

MCP protocol on this engine: Streamable HTTP via `@modelcontextprotocol/sdk` 1.30 (negotiated protocol versions through 2025-11-25). POST-only `/mcp`, Origin validation, JSON or SSE responses.

**Not Grok Bot (do not treat as a Bot wake path):** grok.com custom MCP connectors, Grok Build hooks (outbound lifecycle HTTP/command hooks), or any xAI inference API.

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

## MCP during Bot work (client-initiated; not a wake path)

MCP is **client-initiated while a Bot is already working**. An MCP server cannot wake a Bot. Exposing `get_recent_triggers` is a pull surface for a running Bot, not event-driven activation.

Official Grok Bot attachment of connectors/MCP:

1. Deploy `polymarket-engine` so `https://<engine-app>.fly.dev/mcp` is reachable over public HTTPS. Localhost is not visible to Grok Bot’s cloud computer.
2. In the Grok Bot app, open **Settings → Plugins**. Browse available connectors, choose **Add**, and complete authentication in the browser if requested. In chat, type `@` to attach a connector to the task. Installed connectors are account-wide, not isolated to one Bot. ([Computer and apps](https://docs.x.ai/grok-bot/computer-and-apps))
3. On Cursor Teams, Grok Bot follows existing Cursor plugin and MCP policy (allowlist/denylist, whether members can add their own servers). MCP authentication is shared across Cursor + Grok Bot. ([Teams and enterprises](https://docs.x.ai/grok-bot/teams-and-enterprises))
4. Each of the six role Bots must authenticate with **its own** bearer token whose `clientId` is that role ID. Send `Authorization: Bearer <token>` (token half only). Do not share one token.
5. The engine lists only these tools (there is **no** `place_order` / wallet tool):
   - Read: `get_markets`, `get_market_snapshot`, `get_orderbook`, `get_recent_triggers`, `get_positions`, `get_portfolio`, `get_strategy_metrics`, `get_recent_decisions`, `get_system_health`, `get_reference_context`
   - Write: `submit_signal`, `submit_analysis`, `recommend_hedge`, `recommend_veto`
6. Create six Grok Bots with the stable role IDs below. Each Bot must send `agentId` and `role` equal to its authenticated `clientId`, plus `timestamp`, `marketId`, `confidence`, `evidence`/`evidenceIds`, `expiresAt`, `reasonCodes`, and `traceId`/`correlationId`.
7. Restrict writes per role using the allow-list above (engine-enforced even if the UI cannot). Scanners + supervisor: `submit_signal`. Fair probability: `submit_analysis`. Risk & hedge: `recommend_hedge` + `recommend_veto`.
8. Do **not** attach wallet, exchange, signing, or order connectors to these Bots.

Do **not** configure this engine at [grok.com/connectors](https://grok.com/connectors) as if that were Grok Bot. That is grok.com chat custom MCP, a different product. It does not start a Grok Bot.

If a Grok Bot / Cursor MCP form only offers OAuth 2.1 and refuses static bearer tokens, stop and add OAuth protected-resource discovery later. This repository ships operator-managed bearer tokens first.

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

Triggers are always persisted. The engine never waits for Grok. Fresh, schema-valid signals from the **authenticated** role are assembled into consensus when minimum role/evidence requirements are met. Stale, duplicate, malformed, unauthorized, impersonated, forbidden-tool, and trade-tool calls are rejected and audited. If Grok Bots are down, monitoring continues and the dashboard shows idle/stale — it does not fake activity. Paper only: `TRADING_MODE=paper`; the live adapter always throws `LIVE_TRADING_DISABLED`.

## Issue #2 — Event-driven activation (re-verified 2026-09-01)

**Verdict:** Official Grok Bot docs still do **not** support `custom Polymarket event → Grok Bot actually starts work immediately`. There is **no custom inbound Grok webhook**.

Re-verified sources:

- [Skills and routines — Trigger work from an event](https://docs.x.ai/grok-bot/skills-routines-and-automations): Cursor account integrations can start a routine from an event, **such as a Slack message or a GitHub notification**. They are separate from Slack/GitHub plugins.
- [Troubleshooting — A routine did not run](https://docs.x.ai/grok-bot/troubleshooting): for an event-triggered routine, confirm the source channel, repository, and matching rule.
- [Message and collaborate](https://docs.x.ai/grok-bot/chat-and-collaboration): a Bot-to-Bot message can wake the receiving Bot. **MCP cannot send that message.**
- MCP is client-initiated while a Bot is already working. `get_recent_triggers` is a pull surface, not a wake path.
- Grok Build hooks and grok.com custom MCP are **not** Grok Bot wake paths.

### Architecture chosen

The engine defines `GrokActivationBridge`. Core trigger/consensus/risk code does not import Slack.

| Provider | When | What it does | What it does **not** mean |
|---|---|---|---|
| `NoopGrokActivationBridge` | `GROK_ACTIVATION_PROVIDER=noop` (default) | Records a skipped activation. Monitoring continues. | Not a fake Grok signal, consensus, or paper order. |
| `SlackGrokActivationBridge` | `GROK_ACTIVATION_PROVIDER=slack` plus engine-only `SLACK_BOT_TOKEN` and `SLACK_CHANNEL_ID` | Posts a **minimal wake-signal** per target role: `eventType, triggerId, traceId, marketId, asset, severity, triggerKind, createdAt, expiresAt, targetRoles`. Canonical research stays on MCP. | Adapter HTTP `ok` is **not** an observed Grok wake. Official docs do not prove that a programmatic Slack bot post (vs a human message) fires a Cursor-account routine. |

Bot-to-Bot fan-out is **not** used: MCP cannot send those messages, and a dispatcher Bot would add hops without validated product behavior. Role-specific Slack posts are used instead of waking all six Bots.

Activation is non-fatal: feed loops continue, the trigger remains available in live engine state, active experiment triggers are persisted, failures are journaled when applicable, and there is no invented research or paper order from missing Grok.

A future official Grok webhook can replace the Slack provider without rewriting the trigger engine.

### Operator steps (Slack wake transport)

1. In Cursor/Grok Bot, connect the **Cursor account Slack integration** (not only the Slack plugin).
2. Invite the integration to one dedicated channel (not a production chat).
3. For each of the six role Bots, create a **narrow** event routine, for example: *When a message in `#polymarket-wake` contains `GROK_RESEARCH_REQUEST` and my role id, call `get_recent_triggers` then inspect that `triggerId` via MCP. Submit only my allow-listed write tool. Never place an order.*
4. Set engine secrets only: `GROK_ACTIVATION_PROVIDER=slack`, `SLACK_BOT_TOKEN`, `SLACK_CHANNEL_ID`. Never put these on the dashboard, in MCP responses, journal payloads, or browser env.
5. Keep scheduled routines as a backup poll. If the cron interval is longer than `triggerTtlMs` (default 30s), most windows expire first. Do not silently raise TTL; measure `triggerCreatedAt → firstMcpRequestAt` (MCP is the documented proxy for Bot wake; the product exposes no wake timestamp).

Latency timestamps persisted: `triggerCreatedAt`, `activationRequestedAt`, `activationSentAt`, `firstMcpRequestAt`, `firstAcceptedSignalAt`. `firstMcpRequestAt` is a per-trigger proxy for wake, not a product-provided Bot-started-at field. It is assigned only when the MCP request can be deterministically associated with that `triggerId`. Generic `get_recent_triggers` polls without `triggerId` are recorded as unattributed MCP activity and do not stamp pending triggers.

### Role routing (do not wake all six)

| Trigger family | Roles |
|---|---|
| Microstructure / order book (`BOOK_IMBALANCE`, `SPREAD_CHANGE`, `DEPTH_CHANGE`, `REPRICING_LAG`, `PAIR_COST_OPPORTUNITY`, `EDGE_CHANGE`, `NEARING_EXPIRY`) | `market_microstructure_scanner`, `crypto_market_scanner`, `fair_probability_analyst` |
| Crypto reference / momentum (`BTC_RAPID_MOVE`, `CRYPTO_REFERENCE_*`) | `crypto_market_scanner`, `fair_probability_analyst` |
| News / information | `news_x_intelligence_scanner`, `fair_probability_analyst` |
| Hedge / exposure / risk | `risk_hedge_analyst` |
| Severe disagreement / regime / escalation (incl. severity ≥ 3) | `strategy_supervisor` |

Thousands of ticks are filtered deterministically. Only qualifying triggers request activation.

Do not mix grok.com custom MCP or Grok Build hooks (outbound) into this as if they wake Grok Bot.
