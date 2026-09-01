# Implementation Plan

## Grok Bot / remote MCP migration (2026-09-01)

### Current repository state

- The production-shaped paper engine, PostgreSQL journal/checkpoints, market adapters, trigger/consensus/risk/execution/accounting pipeline, REST/SSE API, dashboard, experiment/reporting, Docker/Fly files, and 20-test baseline already exist and pass locally.
- The component being replaced is the push-style OpenAI Agents SDK provider/orchestrator, its `OPENAI_API_KEY` runtime dependency, GPT-5.6 Luna/Terra/Sol identifiers, and OpenAI token-pricing accounting.
- Deterministic monitoring and all order/risk boundaries remain in place; the migration must not add any wallet, order, signer, or live-trading MCP capability.

### Target architecture

- Host a Streamable HTTP MCP endpoint inside `polymarket-engine`, sharing the existing journal/state while remaining behind bearer authentication and Fly TLS/private networking controls.
- External Grok Bot clients pull bounded market snapshots/order books, triggers, positions, and strategy performance, then push role-specific Zod-validated signals through one idempotent submission tool.
- Keep exactly six stable role IDs: `market_microstructure_scanner`, `crypto_market_scanner`, `news_x_intelligence_scanner`, `fair_probability_analyst`, `risk_hedge_analyst`, and `strategy_supervisor`.
- Replace synchronous model invocation with a deterministic signal inbox/window. Triggers are always persisted and monitoring never waits for Grok. Fresh submitted signals are assembled into the existing weighted consensus/risk/strategy path when minimum role/evidence requirements are met.
- Every MCP request records authentication outcome, client identity hash, tool, request/idempotency IDs, status, rate-limit result, trace/trigger IDs, and sanitized payload metadata in the append-only journal.

### Implementation phases

1. Verify current official MCP Streamable HTTP/auth/tool schemas and xAI/Grok remote-tool guidance.
2. Replace OpenAI-specific role/output/config contracts while preserving compatible journal/dashboard envelopes where practical.
3. Add the Grok signal inbox, freshness/idempotency rules, deterministic trigger-to-consensus evaluation, and unavailable-agent-safe behavior.
4. Add authenticated/rate-limited MCP tools for snapshots, books, triggers, positions, performance, and structured submission; expose no trade/order/wallet tool.
5. Update dashboard labels/activity/freshness/confidence, metrics, reports, environment and Fly/Docker documentation.
6. Add malformed, duplicate, stale, unavailable, unauthorized and regression tests; run typecheck, tests, build and smoke.

### Assumptions and open blockers

- Grok Bots are external MCP clients; this repository does not create or provision xAI credentials and does not call an xAI inference API.
- Initial machine-to-machine authentication uses operator-managed high-entropy bearer tokens supplied only to the engine as secrets. Token fingerprints—not raw tokens—are audited. OAuth-protected-resource discovery can be added later if the deployed Grok client requires delegated user authorization.
- A single Fly engine Machine remains the supported topology; in-memory rate/idempotency caches are backed by durable journal checks where required but horizontally shared rate limits need Redis/PostgreSQL advisory coordination before scaling.
- Docker/Fly CLIs and a real PostgreSQL instance are still unavailable locally; strict Fly, container, and real-DB fault tests remain explicit deployment gates.

### Test strategy

- Preserve all deterministic math, risk, execution, checkpoint/recovery, paper-only and dashboard build tests.
- Add MCP protocol/auth tests, per-client rate-limit behavior, tool input/output schemas, redacted audit events, malformed role output, duplicate idempotency key, stale signal rejection, unavailable-Grok monitoring continuity, and forbidden/unknown trade tool rejection.
- Acceptance remains: external text or malformed Grok data cannot become a paper order without schema validation, fresh consensus, deterministic calculations, and absolute risk approval.

### Fly deployment strategy

- Reuse the engine app/port and expose `/mcp` through the existing Fastify service; keep `/health`, `/ready`, `/metrics`, REST and SSE unchanged.
- Store `GROK_MCP_BEARER_TOKENS` only as an engine Fly secret. The dashboard never receives it.
- Keep engine autostop off, singleton deployment, SIGTERM checkpointing, `TRADING_MODE=paper`, and the intentionally disabled live adapter.
- Validate both configs with `fly config validate --strict` before any later deployment; this change does not authorize deployment.

## Current repository state

- Inspected on 2026-08-31 before any implementation work.
- The supplied workspace contains no existing application, package manifest, source code, tests, database schema, Docker configuration, or Fly.io configuration.
- Only the generated `outputs/` and `work/` directories were present, so there is no working implementation to preserve or overwrite.
- This project will be created at `outputs/polymarket-research-engine` as a new pnpm/TypeScript monorepo.
- Local tools observed: Node.js `v26.7.0` and pnpm `11.16.0`. Docker was not available on `PATH` during initial inspection. Fly CLI availability still needs an isolated check.
- Official-documentation verification completed against current OpenAI Agents/Responses/model, Polymarket unified CLOB v2/WebSocket/fee/rate-limit, and Fly Machines lifecycle documentation before the corresponding adapters/configuration were finalized.
- Implementation status (2026-09-01): strict typecheck passes, 20 tests pass, deterministic smoke passes, engine and standalone dashboard production builds pass. The public live smoke received 392 real BTC messages; Polymarket discovery timed out behind this host's TLS inspection/proxy, so no Polymarket stream result is claimed. Docker and Fly CLIs are not installed locally, so their validations remain operator steps.

## Architecture

The system will be a strict-TypeScript pnpm workspace with independently deployable engine and dashboard applications plus small domain-focused packages.

```text
Polymarket CLOB WebSocket/API ----\
                                  -> deterministic event pipeline -> journal/PostgreSQL
BTC WebSocket -------------------/             |
                                                +-> trigger engine (L0-L3)
                                                +-> six-agent orchestrator
                                                +-> deterministic consensus
                                                +-> deterministic risk veto
                                                +-> pluggable strategy
                                                +-> conservative paper execution
                                                +-> ledger/experiment/reporting
                                                +-> REST + SSE API
                                                           |
                                                           v
                                                separate Next.js dashboard
```

### Workspace shape

- `apps/engine`: persistent Fastify process, feed lifecycle, orchestration, REST/SSE, health/readiness/metrics, graceful shutdown, and experiment runner.
- `apps/dashboard`: separate Next.js application that consumes engine REST/SSE and contains no engine-only secrets.
- `packages/core`: domain/config schemas, decimal calculations, triggers, risk, paper execution, accounting, strategy, exactly-six agent orchestration, consensus, and experiment/report contracts.
- `packages/db`: Drizzle PostgreSQL schema/migrations, append-only journal, idempotent event writes, checkpoints, and recovery queries.
- `packages/market-data`: official unified Polymarket CLOB v2 REST/WebSocket adapter plus Binance BTC adapter, normalization, stale state, and reconnect/backoff/jitter.
- `apps/engine`: orchestration, observability, report aggregation/writing, health/readiness/metrics, REST/SSE, checkpoint recovery, and graceful shutdown.
- `apps/dashboard`: standalone Next.js server and private engine proxy with a reconnecting SSE terminal UI.
- `tests`: integration fixtures and cross-package acceptance tests.

### Runtime boundaries

- `polymarket-engine` is a persistent Fly Machine in configurable region `fra` by default. It never autostops and owns all feeds, decisions, paper execution, accounting, and journal writes.
- `polymarket-dashboard` is a separate Fly app. It reads only the engine's public dashboard API and may autostop independently.
- PostgreSQL is the durable source of truth. In-memory state is a projection rebuilt from persisted checkpoints plus journal events.
- Browser clients use SSE for live updates with event IDs and reconnect support. Historical chart data uses bounded REST queries.
- LLMs never receive order-submission tools and can never bypass schema validation, deterministic consensus, deterministic calculations, or risk vetoes.

## Implementation phases

1. **Repository and docs gate**
   - Confirm the empty starting state, create this plan first, and review current official OpenAI, Polymarket CLOB v2/WebSocket/fees/rate-limit, and Fly.io lifecycle documentation.
2. **Foundation and typed domain models**
   - Create workspace/tooling, strict TypeScript rules, shared identifiers, event envelopes, Zod schemas, result types, clocks, and configuration validation.
3. **Market-data adapters**
   - Implement official Polymarket market discovery/book/fee access and market-channel WebSocket normalization, plus a configurable public BTC WebSocket feed. Add heartbeat, stale detection, reconnect/backoff/jitter, deduplication, and timestamp-order safeguards.
4. **Event journal/database**
   - Define Drizzle schema/migrations for every required entity, append-only traceable decisions, health events, strategy versions, proposed changes, experiment manifests, and restart checkpoints.
5. **Trigger engine**
   - Aggregate high-rate ticks into bounded windows; calculate deterministic candidate features; apply thresholds, severity, deduplication, debounce, cooldowns, call-rate limits, and persistent trigger records.
6. **Deterministic calculations and risk**
   - Add decimal-safe book walking, VWAP, market-specific fees, measured/configured latency slippage, pair math, PnL, exposure, signal freshness, risk utilization, absolute vetoes, and circuit breakers.
7. **Paper execution and accounting**
   - Model taker fills from contemporaneous depth after simulated latency. Model maker orders as resting, conservatively queue-ahead, and fill them only from subsequent qualifying market events. Persist all assumptions and source-event links. Project fills into cash, reservations, positions, hedges, equity, gross/net PnL, AI cost, and infrastructure cost.
8. **Six agents and orchestrator**
   - Define exactly the requested agents and model assignments, structured outputs, event-triggered routing, fallback behavior, hard budgets, usage accounting, deterministic consensus, Sol escalation rules, and manual-only strategy activation.
9. **Experiment runner and API**
   - Implement the 48-hour immutable experiment manifest, end-of-window entry stop, settlement/close policy, full report generator, health/readiness/metrics, historical REST, and reconnectable SSE stream.
10. **Dashboard**
    - Build the dense dark paper-trading dashboard only after real engine endpoints exist. Every displayed value must originate from engine/database state. Keep permanent `PAPER MODE` labeling and expose all required sections responsively.
11. **Verification, containers, Fly, and documentation**
    - Complete mandatory unit/integration/acceptance tests, local end-to-end replay/live-feed smoke tests where credentials are not required, Docker builds where Docker is available, strict Fly validation where Fly CLI is available, Mermaid-rich README, and exact 48-hour start procedure.

## Assumptions

- TypeScript is appropriate because no pre-existing repository dictates another stack.
- Node.js 22 is the production baseline even though the local host is newer.
- PostgreSQL is available in deployed environments; tests may use repository fakes for pure-domain behavior and a real PostgreSQL test URL only when explicitly available.
- Polymarket public market/order-book data and a public BTC market-data feed require no trading credentials. The selected BTC provider will be configurable and documented.
- `TRADING_MODE` defaults to `paper`; no configuration, secret, feature flag, or cast can enable live submission in this milestone.
- OpenAI calls are optional at boot. Without `OPENAI_API_KEY`, deterministic monitoring, health, replay, dashboard, paper execution, and tests continue, while model invocations are recorded as unavailable/failed according to policy. No key will be requested or provisioned for this milestone.
- Monetary/probability calculations use decimal/fixed-point semantics rather than binary floating-point for authoritative accounting.
- The first strategy is Temporal Pair/Hedge behind a generic strategy interface; fills, not intentions, determine whether a hedge is complete.
- Conservative uncertainty is preferred: uncertain maker fills remain unfilled, stale depth cannot fill takers, and missing fee information vetoes rather than assuming zero.
- No deployment will be attempted. Deployment files and exact commands will be prepared and validated locally when tooling permits.

## Open blockers and validation risks

- Model identifiers/capabilities and pricing were confirmed from current official OpenAI documentation and captured in a replaceable, versioned catalog; they should still be rechecked before a future run because pricing changes.
- Current unified `@polymarket/client`, market-channel payloads, market fee schedule/formula, and published rate limits were confirmed and implemented without CLOB v1 examples.
- A BTC feed was not mandated. The implementation will default to a credential-free public exchange WebSocket and keep the adapter/provider configurable.
- Docker is not currently available on local `PATH`; Docker build verification may be blocked unless another bundled runtime provides it.
- Fly CLI availability and `fly config validate --strict` support need confirmation. Configuration will still be produced from current official docs if the CLI is absent.
- No `DATABASE_URL` or running PostgreSQL service has been supplied, so database-backed smoke/recovery tests may require a locally startable fixture or remain explicitly blocked.
- No `OPENAI_API_KEY` will be requested or provisioned. Live six-agent API calls and real token/cost observations will therefore be skipped unless a key is already intentionally supplied by the operator at run time.
- A real 48-hour run cannot be completed inside the build session; the runner, raw-data retention, and deterministic report generation will be verified on short/replay fixtures without fabricating a 48-hour outcome.

## Test strategy

### Unit tests

- Pair acquisition cost, locked edge, first-leg/unpaired exposure, settlement, realized/unrealized/gross/net/economic PnL.
- Fee lookup/formula/rounding, explicit unknown-fee failure, book walking, VWAP, latency slippage, and insufficient-depth behavior.
- Every risk limit and circuit breaker, stale feeds, timestamp disorder, signal expiration, duplicate prevention, strategy cooldown, call-rate and AI-budget limits.
- Weighted non-naive consensus, freshness/reliability weighting, disagreement/conflict reporting, malformed agent data, and malicious payload rejection.
- Taker fills tied to source snapshots and conservative maker queue/fill evolution.
- Cash reservation/release, fill projection, paired versus directional inventory, drawdown, and restart serialization.

### Integration tests

- Scripted WebSocket disconnect/heartbeat/reconnect/backoff and duplicate/out-of-order events.
- Temporary model failure, timeout, invalid JSON/structured output, budget exhaustion, and continued deterministic monitoring.
- Database disconnect/reconnect, idempotent journal writes, checkpoint/replay recovery, and reconciliation.
- Vetoed trade, first-leg fill plus failed hedge, successful pair, maker non-fill/fill, graceful SIGTERM drain/checkpoint, and restart recovery.
- SSE resume/reconnect and dashboard API contracts.

### Critical safety acceptance

- Feed a malformed, adversarial agent response that asks for a real order, falsifies confidence, and tries to override risk. Assert schema rejection or deterministic veto and assert that the live adapter throws `LIVE_TRADING_DISABLED`.

### Verification levels

- `pnpm lint`, `pnpm typecheck`, `pnpm test`, and `pnpm build` must pass.
- A fixture/replay smoke test must demonstrate event -> trigger -> validated signals -> consensus -> risk -> paper order/fill -> accounting -> SSE/report traceability.
- A credential-free live-feed smoke test should prove public Polymarket and BTC connectivity when network access permits; failures will be reported exactly.
- Docker image builds and `fly config validate --strict` will be run only if their CLIs are available; unavailable tooling is not reported as success.

## Fly.io deployment strategy

- Maintain two explicit deployment directories/configs or two clearly named Fly config files so the apps cannot share runtime fate.
- Engine defaults to primary region `fra`, exposes its HTTP health server, uses an always-on Machine/service configuration with autostop disabled and minimum running capacity, and has restart policy appropriate for a persistent process.
- Dashboard is a separate app with its own image/config and may use normal HTTP autostart/autostop behavior; it receives only the engine public URL and dashboard-safe settings.
- Both images run as non-root users, use Node.js 22, have deterministic build/start commands, and expose only required ports.
- Engine shutdown handles SIGTERM by marking readiness false, stopping new decisions/orders, canceling pending simulated decisions, draining/persisting journal state, checkpointing positions, closing feeds, stopping the HTTP server, and exiting before Fly's kill timeout.
- Engine boot loads/checks the latest experiment and strategy version, replays/reconciles positions and resting paper orders, starts readiness as false, reconnects both feeds, and becomes ready only after the DB and required fresh market state are available.
- Secrets are documented as Fly secrets. Dashboard configuration must never include OpenAI, wallet, signing, or Polymarket private credentials.
- Pre-deployment sequence will validate environment schemas, build/test both apps, build images, run `fly config validate --strict` for each config, inspect the rendered configuration, and only then document (not execute) `fly deploy` commands.

## Completion evidence to report

- Files created/changed and major architecture boundaries.
- Exact test, typecheck, build, replay smoke, live-feed smoke, Docker, and Fly validation commands with truthful results.
- Fly readiness and any validation/tooling blocker.
- Missing environment variable names only, never secret values.
- Known limitations and conservative modeling assumptions.
- Exact command to start a 48-hour `paper` experiment.
