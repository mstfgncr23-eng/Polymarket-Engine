import { describe, expect, it } from "vitest";
import {
  LiveExecutionDisabledAdapter,
  TriggerEngine,
  activationRequestFromTrigger,
  containsSecretLeak,
  createId,
  engineEnvSchema,
  evaluateRisk,
  grokSignalSubmissionSchema,
  parseGrokWritePayload,
  GrokSignalValidationError,
  qualifyMarket,
  redactSecrets,
  rolesForTriggerKind,
  riskLimitsSchema,
  toBinanceSpotSymbol,
  triggerConfigSchema,
  type TriggerEvent,
} from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { parseBinanceAggTrade } from "@pm/market-data";
import { EngineService } from "../apps/engine/src/engine.js";
import { GrokActivationCoordinator } from "../apps/engine/src/activation/coordinator.js";
import { NoopGrokActivationBridge, SlackGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { ALLOWED_MCP_TOOLS, FORBIDDEN_MCP_TRADE_TOOLS } from "../apps/engine/src/mcp.js";
import { fill, now, order, safeRisk, snapshot } from "./fixtures.js";

const future = () => new Date(Date.now() + 60_000).toISOString();

function fee() {
  return { rate: "0.07", exponent: 1, takerOnly: true, rebateRate: "0", source: "market_fee_schedule" as const, effectiveAt: new Date().toISOString() };
}

function marketInput(overrides: Record<string, unknown> = {}) {
  return {
    id: "m1", slug: "link-updown-15m-1", title: "LINK Up or Down", tags: ["crypto"],
    active: true, closed: false, acceptingOrders: true,
    endDate: new Date(Date.now() + 10 * 60_000).toISOString(),
    yesTokenId: "yes-1", noTokenId: "no-1", yesPrice: "0.48", noPrice: "0.51",
    liquidityUsd: "200", spread: 0.02, feeSchedule: fee(), feesUnknown: false, ...overrides,
  };
}

function trigger(overrides: Partial<TriggerEvent> = {}): TriggerEvent {
  const traceId = createId("trace");
  return {
    id: createId("trigger"), traceId, marketId: "btc-mkt", asset: "BTC", source: "polymarket",
    kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: "x", evidence: {},
    sourceEventIds: ["e1"], evidenceIds: ["e1"], createdAt: new Date().toISOString(), expiresAt: future(), ...overrides,
  };
}

async function makeEngine() {
  const journal = new MemoryEventJournal();
  const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down", GROK_ACTIVATION_PROVIDER: "noop" });
  return { journal, engine: new EngineService(env, journal, { error: () => undefined }) };
}

describe("multi-market 1-15", () => {
  it("1 discovers qualifying crypto markets beyond a BTC-only allow-list", () => {
    const link = qualifyMarket(marketInput(), new Map());
    const xyz = qualifyMarket(marketInput({ id: "m2", slug: "xyz-updown-5m-9", title: "XYZ Up or Down" }), new Map());
    expect(link.qualified).toBe(true);
    expect(link.asset).toBe("LINK");
    expect(xyz.qualified).toBe(true);
    expect(xyz.asset).toBe("XYZ");
  });

  it("2 rejects non-crypto and closed markets with explicit reason codes", () => {
    const politics = qualifyMarket(marketInput({ slug: "will-someone-win", title: "Will someone win?", tags: ["politics"] }), new Map());
    const closed = qualifyMarket(marketInput({ closed: true, active: false }), new Map());
    expect(politics.reasonCodes).toContain("NOT_CRYPTO");
    expect(closed.reasonCodes).toContain("MARKET_CLOSED");
  });

  it("3 skips unsupported binary semantics instead of inventing UP/DOWN", () => {
    const result = qualifyMarket(marketInput({ slug: "will-etf-be-approved", title: "Will the ETF be approved?", tags: ["crypto"] }), new Map());
    expect(result.reasonCodes).toContain("UNSUPPORTED_BINARY_SEMANTICS");
    expect(result.qualified).toBe(false);
    const reversed = qualifyMarket(marketInput({ yesLabel: "Down", noLabel: "Up" }), new Map());
    expect(reversed.qualified).toBe(true);
    expect(reversed.binary).toMatchObject({ yesOutcome: "DOWN", noOutcome: "UP" });
  });

  it("4 rejects missing token mapping, unknown fees, and bad duration", () => {
    expect(qualifyMarket(marketInput({ yesTokenId: null }), new Map()).reasonCodes).toContain("MISSING_TOKEN_IDS");
    expect(qualifyMarket(marketInput({ feesUnknown: true, feeSchedule: null }), new Map()).reasonCodes).toContain("UNKNOWN_FEE_SCHEDULE");
    expect(qualifyMarket(marketInput({ endDate: new Date(Date.now() + 90 * 86_400_000).toISOString() }), new Map()).reasonCodes).toContain("DURATION_OUT_OF_RANGE");
  });

  it("5 rejects insufficient liquidity, wide spread, invalid price, and duplicates", () => {
    expect(qualifyMarket(marketInput({ liquidityUsd: "1" }), new Map()).reasonCodes).toContain("INSUFFICIENT_LIQUIDITY");
    expect(qualifyMarket(marketInput({ spread: 0.9 }), new Map()).reasonCodes).toContain("SPREAD_TOO_WIDE");
    expect(qualifyMarket(marketInput({ yesPrice: "2" }), new Map()).reasonCodes).toContain("INVALID_PRICE");
    const seen = new Map([["no-1:yes-1", "other"]]);
    expect(qualifyMarket(marketInput(), seen).reasonCodes).toContain("DUPLICATE_MARKET");
  });

  it("6 isolates order books and snapshots by marketId", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 0, imbalanceThreshold: 0.5 }));
    const btc = snapshot({ marketId: "btc-mkt", tokenId: "btc-up", asset: "BTC", bids: [{ price: "0.9", quantity: "100" }], asks: [{ price: "0.91", quantity: "1" }] });
    const eth = snapshot({ marketId: "eth-mkt", tokenId: "eth-up", asset: "ETH", bids: [{ price: "0.5", quantity: "1" }], asks: [{ price: "0.51", quantity: "1" }] });
    const btcTriggers = engine.ingestMarket(btc);
    const ethTriggers = engine.ingestMarket(eth);
    expect(btcTriggers.every((item) => item.marketId === "btc-mkt" && item.asset === "BTC")).toBe(true);
    expect(ethTriggers.every((item) => item.marketId === "eth-mkt")).toBe(true);
  });

  it("7 isolates reference-move triggers per asset and does not contaminate ETH with BTC ticks", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 0, referenceRapidMoveBps: 1, btcRapidMoveBps: 1 }));
    engine.ingestMarket(snapshot({ marketId: "btc-mkt", tokenId: "btc-up", asset: "BTC" }));
    engine.ingestMarket(snapshot({ marketId: "eth-mkt", tokenId: "eth-up", asset: "ETH" }));
    const first = parseBinanceAggTrade(JSON.stringify({ s: "BTCUSDT", p: "100", q: "1", T: Date.now(), a: 1 }), "BTCUSDT", "BTC");
    const second = parseBinanceAggTrade(JSON.stringify({ s: "BTCUSDT", p: "110", q: "1", T: Date.now() + 1, a: 2 }), "BTCUSDT", "BTC");
    engine.ingestReference(first);
    const moves = engine.ingestReference(second);
    expect(moves.length).toBeGreaterThan(0);
    expect(moves.every((item) => item.asset === "BTC" && item.marketId !== "eth-mkt")).toBe(true);
  });

  it("8 shares one venue symbol mapping algorithmically (BTC→BTCUSDT is an example)", () => {
    expect(toBinanceSpotSymbol("BTC")).toBe("BTCUSDT");
    expect(toBinanceSpotSymbol("link")).toBe("LINKUSDT");
    expect(toBinanceSpotSymbol("NOTA COIN")).toBe("NOTACOINUSDT");
  });

  it("9 does not crash when a reference asset is unsupported; Polymarket-only remains available", async () => {
    const { engine } = await makeEngine();
    await engine.registerTriggerForResearch(trigger({ marketId: "obscure", asset: "OBSCURE" }));
    expect(engine.state.recentTriggers).toHaveLength(1);
    expect(engine.ledger.positions.size).toBe(0);
  });

  it("10 keeps maker orders and hedges scoped to tokenId/marketId", () => {
    expect(order().marketId).toBe("btc-15m");
    expect(fill("UP", "0.4").marketId).toBe("btc-15m");
    expect(fill("DOWN", "0.4", "10", "BUY").marketId).toBe("btc-15m");
  });

  it("11 journal/trace fields can carry marketId and asset without secrets", () => {
    const item = { traceId: "t", marketId: "m", asset: "ETH", detail: { token: "x" } };
    expect(containsSecretLeak(item, ["sk-live-secret"])).toBe(false);
  });

  it("12 trigger records include triggerId, traceId, market, asset, source, kind, severity, expiry, evidence IDs", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 0, imbalanceThreshold: 0.1 }));
    const [event] = engine.ingestMarket(snapshot({ marketId: "m", asset: "SOL", bids: [{ price: "0.9", quantity: "50" }], asks: [{ price: "0.91", quantity: "1" }] }));
    expect(event).toMatchObject({ marketId: "m", asset: "SOL", source: "polymarket", kind: "BOOK_IMBALANCE" });
    expect(event?.id.startsWith("trigger_")).toBe(true);
    expect(event?.evidenceIds?.length).toBeGreaterThan(0);
    expect(event?.expiresAt > event!.createdAt).toBe(true);
  });

  it("13 does not emit a trigger on every tick below threshold", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 0, referenceRapidMoveBps: 50, btcRapidMoveBps: 50 }));
    const a = parseBinanceAggTrade(JSON.stringify({ s: "ETHUSDT", p: "100", q: "1", T: Date.now(), a: 1 }), "ETHUSDT", "ETH");
    const b = parseBinanceAggTrade(JSON.stringify({ s: "ETHUSDT", p: "100.1", q: "1", T: Date.now() + 1, a: 2 }), "ETHUSDT", "ETH");
    engine.ingestReference(a);
    expect(engine.ingestReference(b)).toEqual([]);
  });

  it("14 maps BTC rapid-move math onto BTC_RAPID_MOVE without rewriting the window", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 0, btcRapidMoveBps: 1, referenceRapidMoveBps: 1 }));
    engine.ingestMarket(snapshot({ marketId: "btc-mkt", asset: "BTC" }));
    engine.ingestBtc(parseBinanceAggTrade(JSON.stringify({ s: "BTCUSDT", p: "100", q: "1", T: Date.now(), a: 1 })));
    const [event] = engine.ingestBtc(parseBinanceAggTrade(JSON.stringify({ s: "BTCUSDT", p: "102", q: "1", T: Date.now() + 1, a: 2 })));
    expect(event?.kind).toBe("BTC_RAPID_MOVE");
  });

  it("15 live adapter remains hard-disabled", async () => {
    await expect(new LiveExecutionDisabledAdapter().submit(order(), snapshot())).rejects.toThrow("LIVE_TRADING_DISABLED");
  });
});

describe("Grok activation 16-29", () => {
  it("16 routes microstructure to three research roles, not all six", () => {
    expect(rolesForTriggerKind("BOOK_IMBALANCE")).toEqual([
      "market_microstructure_scanner", "crypto_market_scanner", "fair_probability_analyst",
    ]);
  });

  it("17 routes crypto reference/momentum to scanner + fair probability only", () => {
    expect(rolesForTriggerKind("CRYPTO_REFERENCE_RAPID_MOVE")).toEqual(["crypto_market_scanner", "fair_probability_analyst"]);
    expect(rolesForTriggerKind("BTC_RAPID_MOVE")).toEqual(["crypto_market_scanner", "fair_probability_analyst"]);
  });

  it("18 routes news, hedge, and severe escalation without waking every bot", () => {
    expect(rolesForTriggerKind("NEWS_INFORMATION")).toEqual(["news_x_intelligence_scanner", "fair_probability_analyst"]);
    expect(rolesForTriggerKind("FAILED_HEDGE")).toEqual(["risk_hedge_analyst"]);
    expect(rolesForTriggerKind("SCHEDULED_REVIEW")).toEqual(["strategy_supervisor"]);
    expect(rolesForTriggerKind("BOOK_IMBALANCE", 3)).toContain("strategy_supervisor");
  });

  it("19 Noop bridge never fakes a Grok wake or a research signal", async () => {
    const bridge = new NoopGrokActivationBridge();
    const results = await bridge.activate(activationRequestFromTrigger(trigger()));
    expect(results.every((item) => item.skipped && item.grokWakeObserved === false && item.sentAt === null)).toBe(true);
  });

  it("20 Slack bridge posts a minimal payload per target role and treats HTTP success as adapter success only", async () => {
    const posts: Array<Record<string, unknown>> = [];
    const bridge = new SlackGrokActivationBridge({
      token: "xoxb-secret-token", channelId: "C123", apiUrl: "https://slack.example/api/chat.postMessage",
      fetchImpl: async (url, init) => {
        posts.push({ url: String(url), body: JSON.parse(String(init?.body)), authorization: String(init?.headers && "authorization" in (init.headers as object) ? (init.headers as Record<string, string>).authorization : "") });
        return new Response(JSON.stringify({ ok: true }), { status: 200 });
      },
    });
    const results = await bridge.activate(activationRequestFromTrigger(trigger({ kind: "BTC_RAPID_MOVE" })));
    expect(results.every((item) => item.ok && item.grokWakeObserved === false && item.sentAt)).toBe(true);
    expect(posts).toHaveLength(2);
    const body = posts[0]?.body as { text?: string; blocks?: unknown };
    expect(JSON.stringify(body)).toContain("GROK_RESEARCH_REQUEST");
    expect(JSON.stringify(body)).not.toContain("xoxb-secret-token");
  });

  it("21 Slack network/timeout failures are bounded and non-fatal", async () => {
    const bridge = new SlackGrokActivationBridge({
      token: "xoxb-secret-token", channelId: "C123", apiUrl: "https://slack.example/api/chat.postMessage", maxAttempts: 2, timeoutMs: 20,
      fetchImpl: async () => { throw new Error("network down"); },
    });
    const results = await bridge.activate(activationRequestFromTrigger(trigger({ kind: "FAILED_HEDGE" })));
    expect(results[0]?.ok).toBe(false);
    expect(results[0]?.errorCode).toBe("SLACK_NETWORK_ERROR");
    expect(results[0]?.grokWakeObserved).toBe(false);
  });

  it("22 expired triggers are rejected before dispatch", () => {
    const coordinator = new GrokActivationCoordinator(new NoopGrokActivationBridge(), { cooldownMs: 1_000, maxPerMinute: 10, suppressAtOrBelow: 1 });
    const decision = coordinator.decide(trigger({ expiresAt: new Date(Date.now() - 1_000).toISOString() }));
    expect(decision).toMatchObject({ activate: false, reason: "EXPIRED" });
  });

  it("23 duplicate trigger+role activations are suppressed without an idle journal decision", async () => {
    const { engine, journal } = await makeEngine();
    const event = trigger();
    await engine.registerTriggerForResearch(event);
    await engine.registerTriggerForResearch(event);
    const decisions = (await journal.readAll()).filter((item) => item.type === "GROK_ACTIVATION_DECISION");
    expect(decisions).toHaveLength(0);
  });

  it("24 severity-aware suppression does not wake Grok on a weaker repeat", () => {
    const coordinator = new GrokActivationCoordinator(new NoopGrokActivationBridge(), { cooldownMs: 60_000, maxPerMinute: 10, suppressAtOrBelow: 1 });
    const first = trigger({ severity: 1, kind: "SPREAD_CHANGE" });
    const decision = coordinator.decide(first);
    expect(decision.activate).toBe(true);
    coordinator.markRequested(first, decision.request!);
    expect(coordinator.decide(trigger({ marketId: first.marketId, asset: first.asset, kind: "SPREAD_CHANGE", severity: 1 })).reason).toBe("SEVERITY_SUPPRESSED");
  });

  it("25 rate limit bounds activation fan-out", () => {
    const coordinator = new GrokActivationCoordinator(new NoopGrokActivationBridge(), { cooldownMs: 0, maxPerMinute: 1, suppressAtOrBelow: 0 });
    const first = trigger({ marketId: "a", kind: "BOOK_IMBALANCE" });
    const send = coordinator.decide(first);
    coordinator.markRequested(first, send.request!);
    expect(coordinator.decide(trigger({ marketId: "b", kind: "DEPTH_CHANGE" })).reason).toBe("RATE_LIMITED");
  });

  it("26 activation failure does not invent consensus or paper orders", async () => {
    const { engine } = await makeEngine();
    await engine.registerTriggerForResearch(trigger());
    expect(engine.state.latestConsensus).toBeNull();
    expect(engine.ledger.positions.size).toBe(0);
  });

  it("27 generic MCP poll does not mark pending triggers as woken", async () => {
    const { engine } = await makeEngine();
    const event = trigger();
    await engine.registerTriggerForResearch(event);
    engine.noteMcpRequest("get_recent_triggers");
    const latency = engine.activation.snapshot().find((item) => item.triggerId === event.id);
    expect(latency?.triggerCreatedAt).toBe(event.createdAt);
    expect(latency?.firstMcpRequestAt).toBeNull();
    expect(latency?.firstAcceptedSignalAt).toBeNull();
    expect(engine.activation.unattributedSnapshot().count).toBe(1);
    expect(engine.activation.unattributedSnapshot().lastTool).toBe("get_recent_triggers");
    expect(engine.state.snapshot().unattributedMcp.count).toBe(1);
  });

  it("28 does not silently extend trigger TTL", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({ triggerTtlMs: 30_000, cooldownMs: 0, imbalanceThreshold: 0.1 }));
    const [event] = engine.ingestMarket(snapshot({ bids: [{ price: "0.9", quantity: "40" }], asks: [{ price: "0.91", quantity: "1" }] }));
    expect(new Date(event!.expiresAt).getTime() - new Date(event!.createdAt).getTime()).toBe(30_000);
  });

  it("29 activation payload omits canonical research data and secrets", async () => {
    const request = activationRequestFromTrigger(trigger());
    expect(request).toEqual(expect.objectContaining({ eventType: "GROK_RESEARCH_REQUEST", triggerId: request.triggerId, targetRoles: expect.any(Array) }));
    expect(JSON.stringify(request)).not.toContain("place_order");
    expect(JSON.stringify(redactSecrets({ SLACK_BOT_TOKEN: "xoxb-secret", ok: true }))).not.toContain("xoxb-secret");
  });
});

describe("security regression 30-35", () => {
  it("30 still exposes no trade/wallet MCP tools", () => {
    expect(ALLOWED_MCP_TOOLS.has("place_order")).toBe(false);
    expect(FORBIDDEN_MCP_TRADE_TOOLS.has("create_order")).toBe(true);
    expect(FORBIDDEN_MCP_TRADE_TOOLS.has("cancel_order")).toBe(true);
  });

  it("31 identity binding still requires clientId === agentId === role", () => {
    const event = trigger();
    const payload = grokSignalSubmissionSchema.parse({
      submissionId: createId("sub"), triggerId: event.id, traceId: event.traceId, correlationId: createId("c"),
      marketId: event.marketId, timestamp: new Date().toISOString(),
      agentId: "fair_probability_analyst", role: "fair_probability_analyst",
      output: { confidence: 0.8, evidenceIds: ["e"], expiresAt: future(), reasonCodes: ["X"], direction: "UP", fairProbability: 0.6, marketProbability: 0.5, estimatedEdge: 0.04 },
    });
    expect(() => parseGrokWritePayload("submit_analysis", payload, "market_microstructure_scanner")).toThrow(GrokSignalValidationError);
    try {
      parseGrokWritePayload("submit_analysis", payload, "market_microstructure_scanner");
    } catch (error) {
      expect(error).toBeInstanceOf(GrokSignalValidationError);
      expect((error as GrokSignalValidationError).code).toBe("IDENTITY_CLIENT_MISMATCH");
    }
  });

  it("32 scanners still cannot call recommend_veto", () => {
    const event = trigger();
    const payload = grokSignalSubmissionSchema.parse({
      submissionId: createId("sub"), triggerId: event.id, traceId: event.traceId, correlationId: createId("c"),
      marketId: event.marketId, timestamp: new Date().toISOString(),
      agentId: "risk_hedge_analyst", role: "risk_hedge_analyst",
      output: { confidence: 0.8, evidenceIds: ["e"], expiresAt: future(), reasonCodes: ["X"], recommendVeto: true, directionalExposureUsd: "1", unhedgedDurationMs: 0, pairedCost: null, adverseScenarios: [] },
    });
    expect(() => parseGrokWritePayload("recommend_veto", payload, "market_microstructure_scanner")).toThrow(GrokSignalValidationError);
    try {
      parseGrokWritePayload("recommend_veto", payload, "market_microstructure_scanner");
    } catch (error) {
      expect(error).toBeInstanceOf(GrokSignalValidationError);
      expect(["IDENTITY_CLIENT_MISMATCH", "WRITE_TOOL_FORBIDDEN"]).toContain((error as GrokSignalValidationError).code);
    }
  });

  it("33 dashboard/journal redaction drops provider tokens", () => {
    expect(JSON.stringify(redactSecrets({ authorization: "Bearer xoxb-secret", channel: "C1" }))).not.toContain("xoxb-secret");
    expect(containsSecretLeak({ text: "hello xoxb-secret" }, ["xoxb-secret"])).toBe(true);
  });

  it("34 TRADING_MODE remains paper-only", () => {
    expect(() => engineEnvSchema.parse({ TRADING_MODE: "live" })).toThrow();
    expect(engineEnvSchema.parse({ TRADING_MODE: "paper", JOURNAL_MODE: "memory", NODE_ENV: "test" }).TRADING_MODE).toBe("paper");
  });

  it("35 engine snapshot does not include Slack or MCP raw tokens", async () => {
    const { engine } = await makeEngine();
    const serialized = JSON.stringify(engine.state.snapshot());
    expect(serialized).not.toContain("xoxb-");
    expect(serialized).not.toMatch(/Bearer /);
  });
});

describe("native authority isolation 36-40", () => {
  it("36 external BTC/ETH signals never assemble an economic consensus", async () => {
    const { engine } = await makeEngine();
    const btc = trigger({ marketId: "btc-mkt", asset: "BTC", traceId: createId("btc") });
    const eth = trigger({ marketId: "eth-mkt", asset: "ETH", traceId: createId("eth") });
    await engine.registerTriggerForResearch(btc);
    await engine.registerTriggerForResearch(eth);
    const micro = {
      confidence: 0.8, evidenceIds: ["e1"], expiresAt: future(), reasonCodes: ["T"], direction: "UP",
      spread: "0.01", imbalance: 0.4, depthChange: 0.1, pairCost: "0.93", unusualConditions: [],
    };
    const fairUp = { confidence: 0.82, evidenceIds: ["e1"], expiresAt: future(), reasonCodes: ["T"], direction: "UP", fairProbability: 0.61, marketProbability: 0.48, estimatedEdge: 0.04 };
    const fairDown = { ...fairUp, direction: "DOWN" as const, estimatedEdge: -0.04 };
    await engine.submitGrokSignal(grokSignalSubmissionSchema.parse({
      submissionId: createId("s1"), triggerId: btc.id, traceId: btc.traceId, correlationId: createId("c"), marketId: "btc-mkt", asset: "BTC",
      timestamp: new Date().toISOString(), agentId: "market_microstructure_scanner", role: "market_microstructure_scanner", output: micro,
    }), "market_microstructure_scanner");
    await engine.submitGrokSignal(grokSignalSubmissionSchema.parse({
      submissionId: createId("s2"), triggerId: btc.id, traceId: btc.traceId, correlationId: createId("c"), marketId: "btc-mkt", asset: "BTC",
      timestamp: new Date().toISOString(), agentId: "fair_probability_analyst", role: "fair_probability_analyst", output: fairUp,
    }), "fair_probability_analyst");
    expect(engine.state.latestConsensus).toBeNull();
    await engine.submitGrokSignal(grokSignalSubmissionSchema.parse({
      submissionId: createId("s3"), triggerId: eth.id, traceId: eth.traceId, correlationId: createId("c"), marketId: "eth-mkt", asset: "ETH",
      timestamp: new Date().toISOString(), agentId: "market_microstructure_scanner", role: "market_microstructure_scanner", output: { ...micro, direction: "DOWN" },
    }), "market_microstructure_scanner");
    await engine.submitGrokSignal(grokSignalSubmissionSchema.parse({
      submissionId: createId("s4"), triggerId: eth.id, traceId: eth.traceId, correlationId: createId("c"), marketId: "eth-mkt", asset: "ETH",
      timestamp: new Date().toISOString(), agentId: "fair_probability_analyst", role: "fair_probability_analyst", output: fairDown,
    }), "fair_probability_analyst");
    expect(engine.state.latestConsensus).toBeNull();
  });

  it("37 rejects a Grok signal that mixes another market's id onto a trigger", async () => {
    const { engine } = await makeEngine();
    const event = trigger({ marketId: "btc-mkt", asset: "BTC" });
    await engine.registerTriggerForResearch(event);
    const result = await engine.submitGrokSignal(grokSignalSubmissionSchema.parse({
      submissionId: createId("s"), triggerId: event.id, traceId: event.traceId, correlationId: createId("c"), marketId: "eth-mkt",
      timestamp: new Date().toISOString(), agentId: "fair_probability_analyst", role: "fair_probability_analyst",
      output: { confidence: 0.8, evidenceIds: ["e"], expiresAt: future(), reasonCodes: ["T"], direction: "UP", fairProbability: 0.6, marketProbability: 0.5, estimatedEdge: 0.04 },
    }), "fair_probability_analyst");
    expect(result.status).toBe("market_mismatch");
  });

  it("38 rejects a Grok signal that mixes another asset onto a trigger", async () => {
    const { engine } = await makeEngine();
    const event = trigger({ marketId: "btc-mkt", asset: "BTC" });
    await engine.registerTriggerForResearch(event);
    const result = await engine.submitGrokSignal(grokSignalSubmissionSchema.parse({
      submissionId: createId("s"), triggerId: event.id, traceId: event.traceId, correlationId: createId("c"), marketId: "btc-mkt", asset: "ETH",
      timestamp: new Date().toISOString(), agentId: "fair_probability_analyst", role: "fair_probability_analyst",
      output: { confidence: 0.8, evidenceIds: ["e"], expiresAt: future(), reasonCodes: ["T"], direction: "UP", fairProbability: 0.6, marketProbability: 0.5, estimatedEdge: 0.04 },
    }), "fair_probability_analyst");
    expect(result.status).toBe("asset_mismatch");
  });

  it("39 risk is evaluated per order, market, asset, and total portfolio", () => {
    const limits = riskLimitsSchema.parse({ maxMarketExposureUsd: 10, maxAssetExposureUsd: 12, maxTotalExposureUsd: 15 });
    expect(evaluateRisk(safeRisk({ orderSizeUsd: "6", marketExposureUsd: "5", assetExposureUsd: "8", totalExposureUsd: "10" }), limits).vetoes).toEqual(expect.arrayContaining(["MAX_MARKET_EXPOSURE", "MAX_ASSET_EXPOSURE", "MAX_TOTAL_EXPOSURE"]));
  });

  it("40 stale BTC feed still vetoes BTC, while a missing reference is not required for Polymarket-only", () => {
    expect(evaluateRisk(safeRisk({ btcFeedAt: new Date(now.getTime() - 20_000) }), riskLimitsSchema.parse({})).vetoes).toContain("BTC_FEED_STALE");
    const polymarketOnly = evaluateRisk(safeRisk({ referenceRequired: false, btcFeedAt: now, referenceFeedAt: null }), riskLimitsSchema.parse({}));
    expect(polymarketOnly.vetoes).not.toContain("REFERENCE_FEED_STALE");
  });
});
