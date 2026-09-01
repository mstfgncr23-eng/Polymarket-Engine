import { describe, expect, it } from "vitest";
import {
  PaperLedger,
  activationRequestFromTrigger,
  buildConsensus,
  calculateFee,
  calculateSlippageUsd,
  conservativeRealizableMark,
  createId,
  decidePaperOrder,
  defaultConsensusConfig,
  engineEnvSchema,
  evaluateRisk,
  makerReserveNotional,
  parsePublicBinaryResolution,
  projectPaperExposures,
  riskLimitsSchema,
  type TriggerEvent,
} from "@pm/core";
import { MemoryEventJournal, createJournalEvent, queryEventsPaged } from "@pm/db";
import { EngineService, collectMarketFeedAt } from "../apps/engine/src/engine.js";
import { buildExperimentSummary } from "../apps/engine/src/report.js";
import { NoopGrokActivationBridge, SlackGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { ALLOWED_MCP_READ_TOOLS, ALLOWED_MCP_WRITE_TOOLS, FORBIDDEN_MCP_TRADE_TOOLS } from "../apps/engine/src/mcp.js";
import { feeSchedule, fill, now, order, safeRisk, snapshot } from "./fixtures.js";

const GROK_ROLES = [
  "market_microstructure_scanner", "crypto_market_scanner", "news_x_intelligence_scanner",
  "fair_probability_analyst", "risk_hedge_analyst", "strategy_supervisor",
] as const;

function grokPaperEnv() {
  return {
    GROK_ACTIVATION_PROVIDER: "slack",
    SLACK_BOT_TOKEN: "xoxb-test-token",
    SLACK_CHANNEL_ID: "C-TEST",
    GROK_MCP_BEARER_TOKENS: GROK_ROLES.map((role) => `${role}:tok-${role}`).join(","),
  };
}

function pendingFeed() {
  return {
    registerManualToken: () => undefined,
    setTokenIds: (tokenIds: readonly string[]) => ({ changed: false, previous: [...tokenIds], next: [...tokenIds] }),
    run: async (_ids: unknown, _handlers: unknown, signal: AbortSignal) => new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve(), { once: true })),
  };
}

function futureTrigger(overrides: Partial<TriggerEvent> = {}): TriggerEvent {
  const traceId = createId("trace");
  return {
    id: createId("trigger"), traceId, marketId: "btc-mkt", asset: "BTC", source: "polymarket",
    kind: "FAILED_HEDGE", severity: 2, score: 1, dedupeKey: "x", evidence: {},
    sourceEventIds: ["e1"], evidenceIds: ["e1"], createdAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + 60_000).toISOString(), ...overrides,
  };
}

function cashDecision(availableCashUsd: string, book = snapshot({
  asks: [{ price: "0.40", quantity: "20" }],
  bids: [{ price: "0.39", quantity: "20" }],
})) {
  const consensus = buildConsensus({
    traceId: "t", triggerId: "g", deterministicEdge: 0.2, now, config: defaultConsensusConfig,
    signals: [
      { agentId: "fair_probability_analyst", direction: "UP", confidence: 0.9, expiresAt: new Date(now.getTime() + 60_000).toISOString(), evidenceIds: ["e"] },
      { agentId: "market_microstructure_scanner", direction: "UP", confidence: 0.9, expiresAt: new Date(now.getTime() + 60_000).toISOString(), evidenceIds: ["e"] },
    ],
  });
  return decidePaperOrder({
    candidate: {
      strategyId: "temporal-pair-hedge", strategyVersion: "v1", marketId: book.marketId, tokenId: book.tokenId,
      outcome: "UP", side: "BUY", quantity: "10", limitPrice: "0.40", intent: "OPEN_FIRST_LEG", rawExpectedEdge: 0.2, reasonCodes: [],
    },
    consensus, snapshotAfterDecisionLatency: book,
    limits: riskLimitsSchema.parse({ minExpectedEdge: 0, minLiquidityUsd: 1, minSignalConfidence: 0, maxSpread: 1, staleFeedTimeoutMs: 60_000 }),
    triggerId: "g", signalExpiresAt: new Date(now.getTime() + 60_000),
    baseRisk: {
      now, marketFeedAt: now, btcFeedAt: now, databaseAvailable: true, timestampDisorder: false, riskStateAvailable: true,
      dailyPnlUsd: "0", drawdownUsd: "0", marketExposureUsd: "0", totalExposureUsd: "0", directionalExposureUsd: "0",
      unhedgedExposureUsd: "0", unhedgedDurationMs: 0, externalSignalsLastMinute: 0, lastStrategyTradeAt: null,
      duplicateTrade: false, experimentEntryOpen: true, executionErrorsLastHour: 0, externalRiskRecommendedVeto: false,
      shuttingDown: false, availableCashUsd,
    },
  });
}

describe("1 PnL / slippage double counting", () => {
  it("A net trading PnL is 1.80 after BUY 10@0.40 SELL 10@0.60 fees 0.20 regardless of slippage diagnostic", () => {
    const ledger = new PaperLedger(1000, now);
    ledger.applyFill({ ...fill("UP", "0.40"), feeUsd: "0.10", slippageUsd: "5.00" });
    ledger.applyFill({ ...fill("UP", "0.60", "10", "SELL"), feeUsd: "0.10", slippageUsd: "5.00" });
    const snap = ledger.snapshot();
    expect(Number(snap.realizedPnl)).toBeCloseTo(2.0, 8);
    expect(Number(snap.fees)).toBeCloseTo(0.2, 8);
    expect(Number(snap.slippage)).toBeCloseTo(10, 8);
    expect(Number(snap.netTradingPnl)).toBeCloseTo(1.8, 8);
  });

  it("B ledger PnL and report PnL reconcile exactly", () => {
    const ledger = new PaperLedger(1000, now);
    const buy = { ...fill("UP", "0.40"), feeUsd: "0.10", slippageUsd: "0.05" };
    const sell = { ...fill("UP", "0.60", "10", "SELL"), feeUsd: "0.10", slippageUsd: "0.05" };
    ledger.applyFill(buy);
    ledger.applyFill(sell);
    const events = [
      createJournalEvent({ traceId: buy.traceId, type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: buy.id, payload: { ...buy, asset: "BTC" } }),
      createJournalEvent({ traceId: sell.traceId, type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: sell.id, payload: { ...sell, asset: "BTC" } }),
      createJournalEvent({
        traceId: "pnl", type: "PNL_SNAPSHOT", aggregateType: "ledger", aggregateId: "exp",
        payload: { ...ledger.snapshot(), experimentId: "exp" },
      }),
    ];
    const summary = buildExperimentSummary(events, "exp", { reportKind: "final", metricsScope: "complete", complete: true });
    expect(summary.netTradingPnlUsd).toBeCloseTo(Number(ledger.snapshot().netTradingPnl), 8);
    expect(summary.pnlByMarket[buy.marketId]).toBeCloseTo(Number(ledger.snapshot().netTradingPnl), 8);
    expect(summary.slippageUsd).toBeCloseTo(0.1, 8);
  });

  it("C settled-position PnL and report PnL reconcile exactly", () => {
    const ledger = new PaperLedger(1000, now);
    const buy = { ...fill("UP", "0.40"), feeUsd: "0.20", slippageUsd: "1.00" };
    ledger.applyFill(buy);
    const settled = ledger.settlePosition(buy.marketId, "1", "0", now);
    const events = [
      createJournalEvent({ traceId: buy.traceId, type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: buy.id, payload: { ...buy, asset: "BTC" } }),
      createJournalEvent({
        traceId: buy.traceId, type: "POSITION_SETTLED", aggregateType: "position", aggregateId: settled!.position.id,
        payload: { marketId: buy.marketId, asset: "BTC", realizedDelta: settled!.realizedDelta },
      }),
      createJournalEvent({
        traceId: "pnl", type: "PNL_SNAPSHOT", aggregateType: "ledger", aggregateId: "exp",
        payload: { ...ledger.snapshot(), experimentId: "exp" },
      }),
    ];
    const summary = buildExperimentSummary(events, "exp", { reportKind: "final", complete: true });
    expect(summary.netTradingPnlUsd).toBeCloseTo(Number(ledger.snapshot().netTradingPnl), 8);
    expect(summary.pnlByMarket[buy.marketId]).toBeCloseTo(Number(ledger.snapshot().netTradingPnl), 8);
  });

  it("D favorable execution is not an adverse slippage cost and does not reduce realized PnL", () => {
    expect(calculateSlippageUsd("0.50", "0.40", "10", "BUY")).toBe("0");
    expect(calculateSlippageUsd("0.40", "0.50", "10", "SELL")).toBe("0");
    expect(calculateSlippageUsd("0.48", "0.49", "10", "BUY")).toBe("0.1");
    const ledger = new PaperLedger(1000, now);
    ledger.applyFill({ ...fill("UP", "0.40"), requestedPrice: "0.50", fillPrice: "0.40", feeUsd: "0.20", slippageUsd: calculateSlippageUsd("0.50", "0.40", "10", "BUY") });
    ledger.applyFill({ ...fill("UP", "0.60", "10", "SELL"), feeUsd: "0", slippageUsd: "0" });
    expect(Number(ledger.snapshot().slippage)).toBe(0);
    expect(Number(ledger.snapshot().netTradingPnl)).toBeCloseTo(1.8, 8);
  });
});

describe("2 quantity-based hedge projection", () => {
  it("10 UP @ 0.90 + 10 DOWN @ 0.10 has unpaired 0 even though hedge notional is $1", () => {
    const projected = projectPaperExposures({
      current: { market: "9", total: "9", directional: "9", unhedged: "9", unhedgedDurationMs: 1, asset: "9" },
      positions: [{ marketId: "m", outcome: "UP", quantity: "10", avgCost: "0.90", asset: "BTC" }],
      candidate: { marketId: "m", outcome: "DOWN", quantity: "10", price: "0.10", intent: "HEDGE_SECOND_LEG", asset: "BTC" },
    });
    expect(Number(projected.unhedged)).toBe(0);
    expect(Number(projected.directional)).toBe(0);
  });

  it("10 UP + 6 DOWN leaves 4 UP unpaired; first leg increases; partial reduces; oversized still vetoes", () => {
    const empty = { market: "0", total: "0", directional: "0", unhedged: "0", unhedgedDurationMs: 0, asset: "0" };
    const first = projectPaperExposures({
      current: empty, positions: [],
      candidate: { marketId: "m", outcome: "UP", quantity: "10", price: "0.90", intent: "OPEN_FIRST_LEG", asset: "BTC" },
    });
    expect(Number(first.unhedged)).toBeGreaterThan(0);
    const partial = projectPaperExposures({
      current: first,
      positions: [{ marketId: "m", outcome: "UP", quantity: "10", avgCost: "0.90", asset: "BTC" }],
      candidate: { marketId: "m", outcome: "DOWN", quantity: "6", price: "0.10", intent: "HEDGE_SECOND_LEG", asset: "BTC" },
    });
    expect(Number(partial.unhedged)).toBeCloseTo(3.6, 8);
    expect(Number(partial.unhedged)).toBeLessThan(Number(first.unhedged));
    const full = projectPaperExposures({
      current: first,
      positions: [{ marketId: "m", outcome: "UP", quantity: "10", avgCost: "0.90", asset: "BTC" }],
      candidate: { marketId: "m", outcome: "DOWN", quantity: "10", price: "0.10", intent: "HEDGE_SECOND_LEG", asset: "BTC" },
    });
    expect(Number(full.unhedged)).toBe(0);
    const unsafe = evaluateRisk(safeRisk({
      intent: "HEDGE_SECOND_LEG", unhedgedDurationMs: 60_000, unhedgedExposureUsd: "40",
      projectedUnhedgedExposureUsd: "30", projectedDirectionalExposureUsd: "30", projectedMarketExposureUsd: "50",
      projectedTotalExposureUsd: "50", projectedAssetExposureUsd: "50", orderSizeUsd: "10",
    }), riskLimitsSchema.parse({ maxUnhedgedExposureUsd: 10, maxUnhedgedDurationMs: 1_000 }));
    expect(unsafe.vetoes).toContain("MAX_UNHEDGED_EXPOSURE");
  });
});

describe("3 market-specific feed freshness", () => {
  it("stale market A is vetoed while fresh market B remains eligible; one outcome cannot freshen another market", () => {
    const limits = riskLimitsSchema.parse({ staleFeedTimeoutMs: 15_000, minExpectedEdge: 0, minLiquidityUsd: 1, minSignalConfidence: 0, maxSpread: 1 });
    const staleAt = new Date(now.getTime() - 20_000).toISOString();
    const freshAt = now.toISOString();
    const marketA = { UP: snapshot({ marketId: "mkt-a", tokenId: "a-up", receivedAt: staleAt, sourceTimestamp: staleAt }) };
    const marketB = { UP: snapshot({ marketId: "mkt-b", tokenId: "b-up", receivedAt: freshAt }) };
    const aFeed = collectMarketFeedAt(marketA, "UP", "OPEN_FIRST_LEG");
    const bFeed = collectMarketFeedAt(marketB, "UP", "OPEN_FIRST_LEG");
    expect(evaluateRisk(safeRisk({ marketFeedAt: aFeed, now }), limits).vetoes).toContain("MARKET_FEED_STALE");
    expect(evaluateRisk(safeRisk({ marketFeedAt: bFeed, now }), limits).vetoes).not.toContain("MARKET_FEED_STALE");
    expect(collectMarketFeedAt(marketA, "UP", "OPEN_FIRST_LEG", freshAt)?.toISOString()).toBe(staleAt);
  });
});

describe("4 final report pagination over >8000 events", () => {
  it("includes earliest and latest events and totals match the full dataset", async () => {
    const journal = new MemoryEventJournal();
    const start = new Date("2026-01-01T00:00:00.000Z");
    const events = Array.from({ length: 8_001 }, (_, index) => createJournalEvent({
      traceId: createId("trace"), type: "RISK_EVALUATION", aggregateType: "risk", aggregateId: `risk-${index}`,
      payload: { accepted: index % 2 === 0, marketId: "m", experimentId: "exp-full" },
      occurredAt: new Date(start.getTime() + index * 1_000),
    }));
    await journal.appendMany(events);
    const page = await journal.queryEvents({ types: ["RISK_EVALUATION"], limit: 8_000 });
    expect(page.length).toBe(8_000);
    const all = await queryEventsPaged(journal, { types: ["RISK_EVALUATION"] }, 500);
    expect(all.length).toBe(8_001);
    const summary = buildExperimentSummary(all, "exp-full", { reportKind: "final", metricsScope: "complete", complete: true });
    expect(summary.decisionAccountingEventCount).toBe(8_001);
    expect(summary.earliestOccurredAt).toBe(events[0]!.occurredAt);
    expect(summary.latestOccurredAt).toBe(events[8_000]!.occurredAt);
    expect(summary.totalOpportunities).toBe(8_001);
    expect(summary.complete).toBe(true);
  });
});

describe("5 experiment IDLE vs Grok paper activation policy", () => {
  it("Grok paper preflight blocks noop even without EXPERIMENT_REQUIRES_GROK", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down",
      GROK_ACTIVATION_PROVIDER: "noop",
    });
    const engine = new EngineService(env, journal, { error: () => undefined }, { market: pendingFeed() as never });
    await engine.start();
    try {
      const result = await engine.beginExperiment();
      expect(result.ok).toBe(false);
      expect(result.blockers).toEqual(expect.arrayContaining(["GROK_ACTIVATION_NOOP"]));
    } finally {
      await engine.stop();
    }
  });

  it("IDLE 10h accrues zero experiment infra cost and does not wake Grok; 1h RUNNING accrues one hour", async () => {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down",
      ...grokPaperEnv(), ESTIMATED_INFRA_COST_PER_HOUR_USD: "1",
    });
    const engine = new EngineService(env, journal, { error: () => undefined }, {
      market: pendingFeed() as never, now: () => new Date(nowMs), activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    try {
      expect(engine.experimentManifest()?.status).toBe("IDLE");
      await engine.registerTriggerForResearch(futureTrigger());
      nowMs += 10 * 3_600_000;
      await engine.tickMaintenance();
      expect(Number(engine.ledger.snapshot().infrastructureCost)).toBe(0);
      expect((await journal.queryEvents({ types: ["GROK_ACTIVATION_RESULT"] })).length).toBe(0);
      expect((await journal.queryEvents({ types: ["GROK_ACTIVATION_DECISION"] })).some((event) => (event.payload as { reason?: string }).reason === "EXPERIMENT_NOT_ACTIVE")).toBe(true);
      expect((await engine.beginExperiment()).ok).toBe(true);
      nowMs += 3_600_000;
      await engine.tickMaintenance();
      expect(Number(engine.ledger.snapshot().infrastructureCost)).toBeCloseTo(1, 8);
    } finally {
      await engine.stop();
    }
  });
});

describe("6 settlement mapping and conservative MTM", () => {
  it("ordinary YES=UP/NO=DOWN and reversed YES=DOWN/NO=UP, plus string/array labels", () => {
    const ordinary = parsePublicBinaryResolution({
      state: { closed: true }, resolution: { umaResolutionStatus: "resolved" },
      outcomes: { yes: { label: "Yes", price: "1" }, no: { label: "No", price: "0" } },
    }, "UP", "DOWN");
    expect(ordinary).toMatchObject({ status: "resolved", winner: "UP", upPayoff: "1", downPayoff: "0" });

    const reversed = parsePublicBinaryResolution({
      state: { closed: true }, resolution: { umaResolutionStatus: "resolved" },
      outcomes: { yes: { label: "Yes", price: "1" }, no: { label: "No", price: "0" } },
    }, "DOWN", "UP");
    expect(reversed).toMatchObject({ status: "resolved", winner: "DOWN", upPayoff: "0", downPayoff: "1" });

    const arrayLabels = parsePublicBinaryResolution({
      state: { closed: true }, resolution: { umaResolutionStatus: "resolved" },
      outcomes: ["Up", "Down"], outcomePrices: ["1", "0"],
    });
    expect(arrayLabels).toMatchObject({ status: "resolved", winner: "UP" });

    const objectArray = parsePublicBinaryResolution({
      state: { closed: true }, resolution: { umaResolutionStatus: "resolved" },
      outcomes: [{ label: "Down", price: "1" }, { label: "Up", price: "0" }],
    }, "UP", "DOWN");
    expect(objectArray).toMatchObject({ status: "resolved", winner: "DOWN" });

    expect(parsePublicBinaryResolution({
      state: { closed: true }, outcomes: { yes: { price: "0.7" }, no: { price: "0.3" } },
    }).status).toBe("unresolved");
  });

  it("values long inventory at best bid and leaves mark unavailable rather than zero", () => {
    expect(conservativeRealizableMark(snapshot({ bids: [{ price: "0.41", quantity: "10" }], asks: [{ price: "0.49", quantity: "10" }] }))).toBe("0.41");
    expect(conservativeRealizableMark({ bids: [], asks: [{ price: "0.49", quantity: "10" }] })).toBeNull();
  });
});

describe("7 fee-inclusive cash requirement", () => {
  it("vetoes when cash equals notional but is below notional+fee, and passes when fee-inclusive cash is available", () => {
    const book = snapshot({ asks: [{ price: "0.40", quantity: "20" }], bids: [{ price: "0.39", quantity: "20" }] });
    const notional = 4;
    const fee = Number(calculateFee("10", "0.40", "TAKER", book.feeSchedule!));
    expect(fee).toBeGreaterThan(0);
    const short = cashDecision(String(notional), book);
    expect(short.risk.vetoes).toContain("INSUFFICIENT_CASH");
    const enough = cashDecision(String(notional + fee + 0.01), book);
    expect(enough.risk.vetoes).not.toContain("INSUFFICIENT_CASH");
  });

  it("maker takerOnly does not reserve a fake maker fee; maker-fee schedule reserves fee; partial fills keep remaining reservation", async () => {
    const takerOnly = feeSchedule;
    expect(makerReserveNotional("10", "0.47", takerOnly)).toBe("4.7");
    const makerOn = { ...feeSchedule, takerOnly: false };
    const makerFee = calculateFee("10", "0.47", "MAKER", makerOn);
    expect(Number(makerFee)).toBeGreaterThan(0);
    expect(Number(makerReserveNotional("10", "0.47", makerOn))).toBeCloseTo(4.7 + Number(makerFee), 8);

    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down" });
    const engine = new EngineService(env, journal, { error: () => undefined }, { market: pendingFeed() as never });
    await engine.start();
    try {
      const book = snapshot({ feeSchedule: makerOn, bids: [{ price: "0.47", quantity: "10" }] });
      const makerOrder = order("MAKER", { quantity: "10", requestedPrice: "0.47" });
      await engine.restMakerForTest(makerOrder, book);
      const reserved = Number(engine.reservedCash());
      expect(reserved).toBeCloseTo(4.7 + Number(makerFee), 8);
      const partialFee = calculateFee("4", "0.47", "MAKER", makerOn);
      await engine.applyObservedFill({
        ...fill("UP", "0.47", "4"), orderId: makerOrder.id,
        liquidityRole: "MAKER", feeUsd: partialFee, fillPrice: "0.47", requestedPrice: "0.47",
      });
      expect(Number(engine.reservedCash())).toBeCloseTo(reserved - (4 * 0.47 + Number(partialFee)), 5);
    } finally {
      await engine.stop();
    }
  });
});

describe("8 reference MCP context and Slack expiry retry", () => {
  it("get_reference_context is a read-only tool with no trade capability", () => {
    expect(ALLOWED_MCP_READ_TOOLS).toContain("get_reference_context");
    expect(ALLOWED_MCP_WRITE_TOOLS).not.toContain("get_reference_context");
    expect(FORBIDDEN_MCP_TRADE_TOOLS.has("get_reference_context")).toBe(false);
  });

  it("TTL expiry during Retry-After does not send a second HTTP request and returns TRIGGER_EXPIRED", async () => {
    let clock = 1_000;
    let fetches = 0;
    const bridge = new SlackGrokActivationBridge({
      token: "xoxb-secret-token", channelId: "C123", apiUrl: "https://slack.example/api/chat.postMessage",
      maxAttempts: 3, now: () => clock,
      delayImpl: async (ms) => { clock += ms; },
      fetchImpl: async () => {
        fetches += 1;
        return new Response(JSON.stringify({ ok: false, error: "ratelimited" }), { status: 429, headers: { "retry-after": "1" } });
      },
    });
    const results = await bridge.activate(activationRequestFromTrigger(futureTrigger({
      createdAt: new Date(1_000).toISOString(),
      expiresAt: new Date(1_500).toISOString(),
    })));
    expect(fetches).toBe(1);
    expect(results.every((item) => item.errorCode === "TRIGGER_EXPIRED")).toBe(true);
    expect(results.every((item) => item.ok === false)).toBe(true);
  });
});
