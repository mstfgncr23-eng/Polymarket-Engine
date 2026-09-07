import { describe, expect, it } from "vitest";
import {
  PaperLedger,
  TriggerEngine,
  buildConsensus,
  classifySettledOutcome,
  createId,
  decidePaperOrder,
  defaultConsensusConfig,
  evaluateRisk,
  executableFairEdge,
  parsePublicBinaryResolution,
  projectPaperExposures,
  refreshExperiment,
  rolesForTriggerKind,
  shouldWriteFinalReport,
  startExperiment,
  triggerConfigSchema,
  type TriggerKind,
} from "@pm/core";
import { classifyBinanceExchangeInfo } from "@pm/market-data";
import { fill, now, safeRisk, snapshot } from "./fixtures.js";

describe("A/E trigger books are per outcome", () => {
  it("does not emit a fake SPREAD_CHANGE when UP is followed by DOWN", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 0, spreadChangeThreshold: 0.01, imbalanceThreshold: 1 }));
    const up = snapshot({ outcome: "UP", tokenId: "up", bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.42", quantity: "10" }] });
    const down = snapshot({
      outcome: "DOWN", tokenId: "down", marketId: up.marketId, eventId: createId("down"),
      bids: [{ price: "0.55", quantity: "10" }], asks: [{ price: "0.60", quantity: "10" }], sequence: 2,
    });
    engine.ingestMarket(up);
    const kinds = engine.ingestMarket(down).map((item) => item.kind);
    expect(kinds).not.toContain("SPREAD_CHANGE");
  });

  it("emits real SPREAD_CHANGE only against the same outcome book", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 0, spreadChangeThreshold: 0.01, imbalanceThreshold: 1 }));
    const up1 = snapshot({ outcome: "UP", tokenId: "up", bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.42", quantity: "10" }] });
    const down1 = snapshot({
      outcome: "DOWN", tokenId: "down", marketId: up1.marketId, eventId: createId("down"),
      bids: [{ price: "0.50", quantity: "10" }], asks: [{ price: "0.52", quantity: "10" }], sequence: 2,
    });
    const up2 = snapshot({
      outcome: "UP", tokenId: "up", marketId: up1.marketId, eventId: createId("up2"),
      bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.48", quantity: "10" }], sequence: 3,
    });
    const down2 = snapshot({
      outcome: "DOWN", tokenId: "down", marketId: up1.marketId, eventId: createId("down2"),
      bids: [{ price: "0.50", quantity: "10" }], asks: [{ price: "0.58", quantity: "10" }], sequence: 4,
    });
    engine.ingestMarket(up1);
    engine.ingestMarket(down1);
    expect(engine.ingestMarket(up2).some((item) => item.kind === "SPREAD_CHANGE" && item.evidence.outcome === "UP")).toBe(true);
    expect(engine.ingestMarket(down2).some((item) => item.kind === "SPREAD_CHANGE" && item.evidence.outcome === "DOWN")).toBe(true);
  });

  it("fans a reference move out once per market, not once per token", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 0, referenceRapidMoveBps: 10, btcRapidMoveBps: 10 }));
    const marketId = "btc-mkt";
    engine.ingestMarket(snapshot({ marketId, tokenId: "up", outcome: "UP", asset: "BTC" }));
    engine.ingestMarket(snapshot({ marketId, tokenId: "down", outcome: "DOWN", asset: "BTC", eventId: createId("d"), sequence: 2 }));
    const first = { eventId: "r1", traceId: "t", asset: "BTC", symbol: "BTCUSDT", venue: "binance", price: "100", quantity: "1", sourceTimestamp: now.toISOString(), receivedAt: now.toISOString(), tradeId: "1" };
    const second = { ...first, eventId: "r2", price: "101", tradeId: "2", sourceTimestamp: new Date(now.getTime() + 1_000).toISOString() };
    engine.ingestReference(first);
    const triggers = engine.ingestReference(second).filter((item) => item.kind === "BTC_RAPID_MOVE" || item.kind === "CRYPTO_REFERENCE_RAPID_MOVE");
    expect(triggers).toHaveLength(1);
    expect(triggers[0]?.marketId).toBe(marketId);
  });
});

describe("F/G trigger families and six-role reachability", () => {
  it("implements pair-cost, expiry, directional, failed-hedge, disagreement, and scheduled review", () => {
    let t = now.getTime();
    const engine = new TriggerEngine(triggerConfigSchema.parse({
      cooldownMs: 0, imbalanceThreshold: 1, pairCostThreshold: 0.99, expiryWarningMs: 120_000,
      failedHedgeDurationMs: 1_000, scheduledReviewIntervalMs: 1, disagreementAgreementMax: 0.9,
    }), () => new Date(t));
    const up = snapshot({ outcome: "UP", tokenId: "up", asks: [{ price: "0.48", quantity: "10" }], bids: [{ price: "0.47", quantity: "10" }] });
    const down = snapshot({
      outcome: "DOWN", tokenId: "down", marketId: up.marketId, eventId: createId("down"), sequence: 2,
      asks: [{ price: "0.49", quantity: "10" }], bids: [{ price: "0.48", quantity: "10" }],
    });
    engine.noteMarketWindow(up.marketId, "BTC", new Date(t + 60_000).toISOString());
    const pair = [...engine.ingestMarket(up), ...engine.ingestMarket(down)];
    expect(pair.some((item) => item.kind === "PAIR_COST_OPPORTUNITY")).toBe(true);
    expect(pair.some((item) => item.kind === "NEARING_EXPIRY")).toBe(true);

    const position = {
      id: "pos", marketId: up.marketId, upQuantity: "10", downQuantity: "0", averageUpCost: "0.48", averageDownCost: "0",
      pairedQuantity: "0", unpairedOutcome: "UP" as const, unpairedQuantity: "10", combinedPairCost: "0.48",
      totalFees: "0", totalSlippage: "0", openedAt: new Date(t - 5_000).toISOString(), updatedAt: new Date(t).toISOString(),
    };
    const directional = engine.ingestPosition(position, "trace", "BTC");
    expect(directional.some((item) => item.kind === "DIRECTIONAL_EXPOSURE")).toBe(true);
    t += 2_000;
    const failed = engine.ingestPosition(position, "trace2", "BTC");
    expect(failed.some((item) => item.kind === "FAILED_HEDGE")).toBe(true);

    const consensus = buildConsensus({
      traceId: "trace", triggerId: "trig", deterministicEdge: 0.02, now: new Date(t), config: defaultConsensusConfig,
      signals: [
        { agentId: "fair_probability_analyst", direction: "UP", confidence: 0.9, expiresAt: new Date(t + 60_000).toISOString(), evidenceIds: ["e1"] },
        { agentId: "crypto_market_scanner", direction: "DOWN", confidence: 0.8, expiresAt: new Date(t + 60_000).toISOString(), evidenceIds: ["e2"] },
      ],
    });
    expect(engine.ingestConsensus(consensus, up.marketId, "BTC").some((item) => item.kind === "SEVERE_DISAGREEMENT")).toBe(true);
    expect(engine.ingestScheduledReview("review").some((item) => item.kind === "SCHEDULED_REVIEW")).toBe(true);
  });

  it("gives each of the six Grok roles at least one runtime trigger path", () => {
    const kinds: TriggerKind[] = [
      "BOOK_IMBALANCE", "CRYPTO_REFERENCE_RAPID_MOVE", "NEWS_INFORMATION",
      "FAILED_HEDGE", "DIRECTIONAL_EXPOSURE", "SEVERE_DISAGREEMENT", "SCHEDULED_REVIEW",
    ];
    const reached = new Set(kinds.flatMap((kind) => rolesForTriggerKind(kind, kind === "SEVERE_DISAGREEMENT" ? 3 : 2)));
    expect(reached).toEqual(new Set([
      "market_microstructure_scanner", "crypto_market_scanner", "news_x_intelligence_scanner",
      "fair_probability_analyst", "risk_hedge_analyst", "strategy_supervisor",
    ]));
    expect(rolesForTriggerKind("BOOK_IMBALANCE", 1)).not.toContain("news_x_intelligence_scanner");
    expect(rolesForTriggerKind("BOOK_IMBALANCE", 1)).not.toContain("strategy_supervisor");
    expect(rolesForTriggerKind("FAILED_HEDGE")).toEqual(["risk_hedge_analyst"]);
  });
});

describe("H projected hedge risk", () => {
  it("projects unpaired quantity, not hedge notional dollars", () => {
    const empty = { market: "0", total: "0", directional: "0", unhedged: "0", unhedgedDurationMs: 0, asset: "0" };
    const full = projectPaperExposures({
      current: empty,
      positions: [{ marketId: "m", outcome: "UP", quantity: "10", avgCost: "0.90", asset: "BTC" }],
      candidate: { marketId: "m", outcome: "DOWN", quantity: "10", price: "0.10", intent: "HEDGE_SECOND_LEG", asset: "BTC" },
    });
    expect(Number(full.unhedged)).toBe(0);
    expect(Number(full.directional)).toBe(0);

    const partial = projectPaperExposures({
      current: empty,
      positions: [{ marketId: "m", outcome: "UP", quantity: "10", avgCost: "0.90", asset: "BTC" }],
      candidate: { marketId: "m", outcome: "DOWN", quantity: "6", price: "0.10", intent: "HEDGE_SECOND_LEG", asset: "BTC" },
    });
    expect(Number(partial.unhedged)).toBeCloseTo(4 * 0.90, 8);
    expect(partial.unhedged).toBe(String(Number(partial.unhedged)));

    const first = projectPaperExposures({
      current: empty,
      positions: [],
      candidate: { marketId: "m", outcome: "UP", quantity: "10", price: "0.90", intent: "OPEN_FIRST_LEG", asset: "BTC" },
    });
    expect(Number(first.unhedged)).toBeCloseTo(9, 8);
    expect(Number(first.directional)).toBeCloseTo(9, 8);
  });

  it("does not block a risk-reducing hedge solely because unhedged duration expired", () => {
    const riskLimits = { maxUnhedgedDurationMs: 1_000, maxUnhedgedExposureUsd: 80, maxDirectionalExposureUsd: 80, maxOrderSizeUsd: 25, maxMarketExposureUsd: 200, maxAssetExposureUsd: 200, maxTotalExposureUsd: 300, maxDailyPaperLossUsd: 50, maxDrawdownUsd: 100, minLiquidityUsd: 1, minExpectedEdge: 0, maxSpread: 1, minSignalConfidence: 0, staleFeedTimeoutMs: 15_000, maxExternalSignalsPerMinute: 30, strategyCooldownMs: 0, maxExecutionErrorsPerHour: 50 };
    const aged = safeRisk({
      intent: "HEDGE_SECOND_LEG", unhedgedDurationMs: 60_000, unhedgedExposureUsd: "40", directionalExposureUsd: "40",
      projectedUnhedgedExposureUsd: "20", projectedDirectionalExposureUsd: "20", projectedMarketExposureUsd: "50",
      projectedTotalExposureUsd: "50", projectedAssetExposureUsd: "50", orderSizeUsd: "20",
    });
    expect(evaluateRisk(aged, riskLimits).vetoes).not.toContain("MAX_UNHEDGED_DURATION");
    expect(evaluateRisk(aged, riskLimits).accepted).toBe(true);
  });

  it("still vetoes an unsafe hedge", () => {
    const riskLimits = { maxUnhedgedDurationMs: 1_000, maxUnhedgedExposureUsd: 10, maxDirectionalExposureUsd: 80, maxOrderSizeUsd: 25, maxMarketExposureUsd: 200, maxAssetExposureUsd: 200, maxTotalExposureUsd: 300, maxDailyPaperLossUsd: 50, maxDrawdownUsd: 100, minLiquidityUsd: 1, minExpectedEdge: 0, maxSpread: 1, minSignalConfidence: 0, staleFeedTimeoutMs: 15_000, maxExternalSignalsPerMinute: 30, strategyCooldownMs: 0, maxExecutionErrorsPerHour: 50 };
    const unsafe = safeRisk({
      intent: "HEDGE_SECOND_LEG", unhedgedDurationMs: 60_000, unhedgedExposureUsd: "40",
      projectedUnhedgedExposureUsd: "30", projectedDirectionalExposureUsd: "30", projectedMarketExposureUsd: "50",
      projectedTotalExposureUsd: "50", projectedAssetExposureUsd: "50", orderSizeUsd: "10",
    });
    expect(evaluateRisk(unsafe, riskLimits).vetoes).toContain("MAX_UNHEDGED_EXPOSURE");
  });
});

describe("I fair probability is diagnostic only", () => {
  it("ignores a malicious estimatedEdge when the live book only has 1% edge", () => {
    const book = snapshot({ outcome: "UP", asks: [{ price: "0.49", quantity: "20" }], bids: [{ price: "0.48", quantity: "20" }] });
    const result = executableFairEdge({
      fair: { direction: "UP", fairProbability: 0.50, marketProbability: 0.10, estimatedEdge: 0.90 },
      consensus: { consensusDirection: "UP" }, snapshot: book, quantity: "5",
    });
    expect(result?.rawEdge).toBeCloseTo(0.01, 8);
    expect(result?.rawEdgeUsdPerShare).toBeCloseTo(0.01, 8);
    expect(result?.fairProbabilityForTradedOutcome).toBeCloseTo(0.50, 8);
    expect(result?.usedComplement).toBe(false);
    expect(result?.estimatedEdgeIgnored).toBe(0.90);
    expect(result?.bookMarketProbability).toBe(0.49);
  });

  it("uses the binary complement when fair direction differs from consensus", () => {
    const book = snapshot({ outcome: "UP", asks: [{ price: "0.40", quantity: "20" }], bids: [{ price: "0.39", quantity: "20" }] });
    const result = executableFairEdge({
      fair: { direction: "DOWN", fairProbability: 0.30, estimatedEdge: 0.50 },
      consensus: { consensusDirection: "UP" }, snapshot: book, quantity: "5",
    });
    expect(result?.usedComplement).toBe(true);
    expect(result?.fairProbabilityForConsensus).toBeCloseTo(0.70, 8);
    expect(result?.fairProbabilityForTradedOutcome).toBeCloseTo(0.70, 8);
    expect(result?.rawEdgeUsdPerShare).toBeCloseTo(0.30, 8);
    expect(result?.rawEdge).toBeCloseTo(0.30, 8);
  });

  it("lets fee and slippage turn a positive raw edge into a veto", () => {
    const book = snapshot({
      outcome: "UP",
      asks: [{ price: "0.48", quantity: "5" }, { price: "0.52", quantity: "20" }],
      bids: [{ price: "0.47", quantity: "20" }],
    });
    const consensus = buildConsensus({
      traceId: "t", triggerId: "g", deterministicEdge: 0.04, now, config: defaultConsensusConfig,
      signals: [
        { agentId: "fair_probability_analyst", direction: "UP", confidence: 0.9, expiresAt: new Date(now.getTime() + 60_000).toISOString(), evidenceIds: ["e"] },
        { agentId: "market_microstructure_scanner", direction: "UP", confidence: 0.9, expiresAt: new Date(now.getTime() + 60_000).toISOString(), evidenceIds: ["e"] },
      ],
    });
    const decision = decidePaperOrder({
      candidate: {
        strategyId: "temporal-pair-hedge", strategyVersion: "v1", marketId: book.marketId, tokenId: book.tokenId,
        outcome: "UP", side: "BUY", quantity: "10", limitPrice: "0.52", intent: "OPEN_FIRST_LEG", rawExpectedEdge: 0.02, reasonCodes: [],
      },
      consensus, snapshotAfterDecisionLatency: book,
      limits: { maxOrderSizeUsd: 25, maxMarketExposureUsd: 200, maxAssetExposureUsd: 200, maxTotalExposureUsd: 300, maxDirectionalExposureUsd: 80, maxUnhedgedExposureUsd: 80, maxUnhedgedDurationMs: 60_000, maxDailyPaperLossUsd: 50, maxDrawdownUsd: 100, minLiquidityUsd: 1, minExpectedEdge: 0.015, maxSpread: 1, minSignalConfidence: 0.5, staleFeedTimeoutMs: 15_000, maxExternalSignalsPerMinute: 30, strategyCooldownMs: 0, maxExecutionErrorsPerHour: 50 },
      triggerId: "g", signalExpiresAt: new Date(now.getTime() + 60_000),
      baseRisk: {
        now, marketFeedAt: now, btcFeedAt: now, databaseAvailable: true, timestampDisorder: false, riskStateAvailable: true,
        dailyPnlUsd: "0", drawdownUsd: "0", marketExposureUsd: "0", totalExposureUsd: "0", directionalExposureUsd: "0",
        unhedgedExposureUsd: "0", unhedgedDurationMs: 0, externalSignalsLastMinute: 0, lastStrategyTradeAt: null,
        duplicateTrade: false, experimentEntryOpen: true, executionErrorsLastHour: 0, externalRiskRecommendedVeto: false, shuttingDown: false,
      },
    });
    expect(decision.rawEdge).toBe(0.02);
    expect(decision.rawEdgeUsdPerShare).toBe(0.02);
    expect(decision.quantity).toBe("10");
    expect(decision.executableNotionalUsd).toBe("5");
    expect(decision.feeUsdPerShare).not.toBeNull();
    expect(decision.costUsdPerShare).not.toBeNull();
    expect(decision.grossEvUsdTotal).toBe("0.2");
    expect(decision.netEvUsdTotal).not.toBeNull();
    expect(decision.expectedROI).toBeCloseTo(Number(decision.netEvUsdTotal) / Number(decision.executableNotionalUsd), 8);
    expect(Number(decision.netEvUsdTotal)).toBeCloseTo(decision.netEvUsdPerShare * 10, 8);
    expect(decision.netExpectedEdge).toBeLessThan(0.015);
    expect(decision.netEvUsdPerShare).toBe(decision.netExpectedEdge);
    expect(decision.risk.vetoes).toContain("MINIMUM_EXPECTED_EDGE");
  });
});

describe("B paper settlement from public resolution", () => {
  it("settles $1/$0 without guessing and is idempotent", () => {
    const parsed = parsePublicBinaryResolution({
      state: { closed: true }, resolution: { umaResolutionStatus: "resolved" },
      outcomes: { yes: { label: "Up", price: "1" }, no: { label: "Down", price: "0" } },
    });
    expect(parsed).toMatchObject({ status: "resolved", winner: "UP", upPayoff: "1", downPayoff: "0" });
    expect(parsePublicBinaryResolution({ state: { closed: true }, outcomes: { yes: { price: "0.7" }, no: { price: "0.3" } } }).status).toBe("unresolved");

    const ledger = new PaperLedger(1000, now);
    ledger.applyFill(fill("UP", "0.40", "10"));
    const first = ledger.settlePosition("btc-15m", "1", "0", now);
    expect(first?.outcome).toBe("POSITION_WIN");
    expect(classifySettledOutcome(first!.realizedDelta)).toBe("POSITION_WIN");
    expect(Number(ledger.snapshot().cash)).toBeCloseTo(1000 - 4 + 10, 8);
    expect(ledger.settlePosition("btc-15m", "1", "0", now)).toBeNull();
  });
});

describe("D experiment close policy", () => {
  it("waits for settlement under observe_until_settlement and MTM-completes without fabricating wins", () => {
    const started = startExperiment({
      durationHours: 48, strategyVersion: "v1", riskConfiguration: {}, modelConfiguration: {},
      startingBalance: "1000", systemVersion: "0.1.0", gitSha: "test", now,
      closePolicy: "observe_until_settlement",
    });
    const closed = refreshExperiment(started, new Date(now.getTime() + 48 * 3_600_000), { openPositions: true });
    expect(closed.status).toBe("ENTRY_CLOSED");
    expect(shouldWriteFinalReport(closed, true)).toBe(false);
    const stillOpen = refreshExperiment(closed, new Date(now.getTime() + 49 * 3_600_000), { openPositions: true });
    expect(stillOpen.status).toBe("ENTRY_CLOSED");
    const done = refreshExperiment(closed, new Date(now.getTime() + 50 * 3_600_000), { openPositions: false });
    expect(done.status).toBe("COMPLETED");

    const mtmStart = startExperiment({
      durationHours: 48, strategyVersion: "v1", riskConfiguration: {}, modelConfiguration: {},
      startingBalance: "1000", systemVersion: "0.1.0", gitSha: "test", now,
      closePolicy: "mark_to_market",
    });
    const mtmClosed = refreshExperiment(mtmStart, new Date(now.getTime() + 48 * 3_600_000), { openPositions: true, unresolvedCount: 2 });
    const mtmDone = refreshExperiment(mtmClosed, new Date(now.getTime() + 48 * 3_600_000), { openPositions: true, unresolvedCount: 2 });
    expect(mtmDone.status).toBe("COMPLETED");
    expect(mtmDone.unresolvedPositionCount).toBe(2);
    expect(mtmDone.markToMarketAt).toBeTruthy();
  });
});

describe("J maker cash reservation", () => {
  it("cannot spend the same cash on two resting makers", () => {
    const ledger = new PaperLedger(10, now);
    ledger.reserve("6", now);
    expect(ledger.snapshot().reservedCash).toBe("6");
    expect(() => ledger.reserve("5", now)).toThrow("INSUFFICIENT_CASH");
    ledger.release("6", now);
    ledger.reserve("5", now);
    expect(ledger.availableCash().toNumber()).toBe(5);
  });
});

describe("P provider/region blocks", () => {
  it("classifies HTTP 451 as a region block, not a tight transient", () => {
    expect(classifyBinanceExchangeInfo({ ok: false, status: 451, body: null, symbol: "ETHUSDT" })).toEqual({ kind: "provider_blocked", detail: "http_451" });
    expect(classifyBinanceExchangeInfo({ ok: false, status: 403, body: null, symbol: "ETHUSDT" })).toEqual({ kind: "provider_blocked", detail: "http_403" });
    expect(classifyBinanceExchangeInfo({ ok: false, status: 500, body: null, symbol: "ETHUSDT" }).kind).toBe("transient_failure");
  });
});
