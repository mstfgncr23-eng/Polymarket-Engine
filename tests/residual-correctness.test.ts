import { describe, expect, it } from "vitest";
import {
  PaperLedger,
  classifySettledOutcome,
  createId,
  engineEnvSchema,
  evaluateRisk,
  parsePublicBinaryResolution,
  riskLimitsSchema,
  type JournalEvent,
  type JournalKeysetQuery,
  type JournalQuery,
  type TriggerEvent,
} from "@pm/core";
import { MemoryEventJournal, createJournalEvent, queryEventsPaged } from "@pm/db";
import { EngineService, collectMarketFeedAt } from "../apps/engine/src/engine.js";
import { buildExperimentSummary, filterCurrentExperimentEvents } from "../apps/engine/src/report.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { fill, now, safeRisk, snapshot } from "./fixtures.js";
import {
  discoveryReport,
  grokPaperEnv,
  paperFill,
  qualified,
  qualifiedPaperEnv,
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
  startQualifiedEngine,
} from "./paper-engine-testkit.js";

class PostgresDescPageJournal {
  private readonly events: JournalEvent[] = [];

  async append(event: JournalEvent): Promise<void> {
    this.events.push(structuredClone(event));
  }
  async appendMany(events: readonly JournalEvent[]): Promise<void> {
    for (const event of events) await this.append(event);
  }
  async queryEvents(query: JournalQuery): Promise<JournalEvent[]> {
    let rows = this.events;
    if (query.types && query.types.length > 0) rows = rows.filter((event) => query.types!.includes(event.type));
    const newestFirst = [...rows].sort((a, b) => b.occurredAt.localeCompare(a.occurredAt) || b.id.localeCompare(a.id));
    const offset = query.offset ?? 0;
    const limit = query.limit ?? newestFirst.length;
    return newestFirst.slice(offset, offset + limit).reverse();
  }
}

function event(type: string, occurredAt: Date, payload: Record<string, unknown> = {}, aggregateId = type): JournalEvent {
  return createJournalEvent({
    traceId: createId("trace"), type, aggregateType: "test", aggregateId,
    payload, occurredAt,
  });
}

describe("postgres-style multi-page ordering", () => {
  it("first global event is earliest; last is latest; latest PNL and position are selected", async () => {
    const journal = new PostgresDescPageJournal();
    const start = new Date("2026-01-01T00:00:00.000Z");
    const events: JournalEvent[] = [];
    events.push(event("PNL_SNAPSHOT", start, { equity: "1000", netTradingPnl: "0", drawdown: "0" }, "ledger"));
    events.push(event("POSITION_UPDATED", new Date(start.getTime() + 1_000), {
      id: "pos-1", marketId: "m1", upQuantity: "10", downQuantity: "0", unpairedQuantity: "10", pairedQuantity: "0",
      averageUpCost: "0.40", averageDownCost: "0",
    }, "pos-1"));
    for (let index = 2; index < 8; index += 1) {
      events.push(event("RISK_EVALUATION", new Date(start.getTime() + index * 1_000), { accepted: true, i: index }, `risk-${index}`));
    }
    events.push(event("POSITION_UPDATED", new Date(start.getTime() + 8_000), {
      id: "pos-1", marketId: "m1", upQuantity: "10", downQuantity: "10", unpairedQuantity: "0", pairedQuantity: "10",
      averageUpCost: "0.40", averageDownCost: "0.55",
    }, "pos-1"));
    events.push(event("PNL_SNAPSHOT", new Date(start.getTime() + 9_000), { equity: "1012", netTradingPnl: "1.80", drawdown: "0" }, "ledger"));
    await journal.appendMany(events);

    const scan = await queryEventsPaged(journal, { types: events.map((item) => item.type) }, 3);
    expect(scan.complete).toBe(true);
    expect(scan.truncated).toBe(false);
    expect(scan.events[0]?.occurredAt).toBe(events[0]!.occurredAt);
    expect(scan.events.at(-1)?.occurredAt).toBe(events.at(-1)!.occurredAt);
    const summary = buildExperimentSummary(scan.events, "exp", { reportKind: "final", metricsScope: "complete" });
    expect(summary.netTradingPnlUsd).toBe(1.8);
    expect(summary.incompleteUnhedgedPositions).toBe(0);
    expect(summary.earliestOccurredAt).toBe(events[0]!.occurredAt);
    expect(summary.latestOccurredAt).toBe(events.at(-1)!.occurredAt);

    const ceiling = await queryEventsPaged(journal, {}, 3, 6);
    expect(ceiling.truncated).toBe(true);
    expect(ceiling.complete).toBe(false);
    expect(buildExperimentSummary(ceiling.events, "exp", {
      reportKind: "final", metricsScope: "complete", truncated: ceiling.truncated, complete: ceiling.complete,
    }).complete).toBe(false);
  });
});

describe("final-report retry", () => {
  it("does not set reportGenerated until the report actually succeeds, then retries", async () => {
    class FlakyJournal extends MemoryEventJournal {
      failures = 1;
      override async queryEventsKeyset(query: JournalKeysetQuery) {
        if (this.failures > 0) {
          this.failures -= 1;
          throw new Error("QUERY_FAILED");
        }
        return super.queryEventsKeyset(query);
      }
    }
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const adapter = new ScriptedMarketAdapter();
    adapter.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    const journal = new FlakyJournal();
    const engine = new EngineService(qualifiedPaperEnv({ EXPERIMENT_DURATION_HOURS: "0.001" }), journal, { error: () => undefined }, {
      market: adapter as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(),
      now: () => new Date(nowMs),
    });
    await engine.start();
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      nowMs += 10_000;
      await engine.tickMaintenance();
      expect(engine.experimentManifest()?.status).toBe("COMPLETED");
      expect((await journal.queryEvents({ types: ["EXPERIMENT_REPORT_GENERATED"] })).length).toBe(0);
      await engine.tickMaintenance();
      expect((await journal.queryEvents({ types: ["EXPERIMENT_REPORT_GENERATED"] })).length).toBe(1);
    } finally {
      await engine.stop();
    }
  });
});

describe("risk-reducing hedge freshness", () => {
  it("does not veto a fresh DOWN hedge because the UP inventory book is stale", () => {
    const limits = riskLimitsSchema.parse({ staleFeedTimeoutMs: 15_000, minExpectedEdge: 0, minLiquidityUsd: 1, minSignalConfidence: 0, maxSpread: 1 });
    const staleAt = new Date(now.getTime() - 20_000).toISOString();
    const freshAt = now.toISOString();
    const pair = {
      UP: snapshot({ marketId: "mkt", tokenId: "up", outcome: "UP", receivedAt: staleAt, sourceTimestamp: staleAt }),
      DOWN: snapshot({ marketId: "mkt", tokenId: "down", outcome: "DOWN", receivedAt: freshAt }),
    };
    const hedgeFeed = collectMarketFeedAt(pair, "DOWN", "HEDGE_SECOND_LEG");
    expect(evaluateRisk(safeRisk({ marketFeedAt: hedgeFeed, now, intent: "HEDGE_SECOND_LEG" }), limits).vetoes).not.toContain("MARKET_FEED_STALE");
    const staleHedge = collectMarketFeedAt({
      UP: pair.UP,
      DOWN: snapshot({ marketId: "mkt", tokenId: "down", outcome: "DOWN", receivedAt: staleAt, sourceTimestamp: staleAt }),
    }, "DOWN", "HEDGE_SECOND_LEG");
    expect(evaluateRisk(safeRisk({ marketFeedAt: staleHedge, now, intent: "HEDGE_SECOND_LEG" }), limits).vetoes).toContain("MARKET_FEED_STALE");
  });
});

describe("mixed maker/taker rate", () => {
  it("does not double-count maker orders and does not put makers in the taker denominator", () => {
    const t0 = new Date("2026-01-01T00:00:00.000Z");
    const events = [
      event("PAPER_ORDER_CREATED", t0, { orderType: "MAKER", id: "m1" }, "m1"),
      event("PAPER_MAKER_RESERVED", new Date(t0.getTime() + 1), { orderId: "m1" }, "m1"),
      event("PAPER_ORDER_CREATED", new Date(t0.getTime() + 2), { orderType: "TAKER", id: "t1" }, "t1"),
      event("PAPER_ORDER_CREATED", new Date(t0.getTime() + 3), { orderType: "TAKER", id: "t2" }, "t2"),
      event("PAPER_FILL", new Date(t0.getTime() + 4), { liquidityRole: "MAKER", marketId: "m", quantity: "1", fillPrice: "0.4", feeUsd: "0", slippageUsd: "0", outcome: "UP", side: "BUY" }, "f1"),
      event("PAPER_FILL", new Date(t0.getTime() + 5), { liquidityRole: "TAKER", marketId: "m", quantity: "1", fillPrice: "0.4", feeUsd: "0", slippageUsd: "0", outcome: "UP", side: "BUY" }, "f2"),
    ];
    const summary = buildExperimentSummary(events, "exp");
    expect(summary.makerFillRate).toBe(1);
    expect(summary.takerFillRate).toBe(0.5);
  });
});

describe("net-fee win/loss", () => {
  it("classifies a fee-flipped gross win as a net loss and exposes both labels", () => {
    const ledger = new PaperLedger(1000, now);
    ledger.applyFill({ ...fill("UP", "0.99", "1"), feeUsd: "0.02" });
    const settled = ledger.settlePosition("btc-15m", "1", "0", now);
    expect(Number(settled!.realizedDelta)).toBeCloseTo(0.01, 8);
    expect(settled!.grossOutcome).toBe("POSITION_WIN");
    expect(settled!.outcome).toBe("POSITION_LOSS");
    expect(classifySettledOutcome(settled!.realizedDelta, "0.02")).toBe("POSITION_LOSS");
    expect(classifySettledOutcome(settled!.realizedDelta, "0")).toBe("POSITION_WIN");

    const t0 = new Date(now);
    const events = [
      event("POSITION_SETTLED", t0, { marketId: "btc-15m", realizedDelta: settled!.realizedDelta, feesUsd: "0.02", grossOutcome: "POSITION_WIN", outcome: "POSITION_LOSS" }, "pos-fee"),
      event("POSITION_LOSS", new Date(t0.getTime() + 1), { realizedDelta: settled!.realizedDelta, grossOutcome: "POSITION_WIN" }, "pos-fee"),
      event("PAIR_REALIZED", new Date(t0.getTime() + 2), {
        pairedQuantity: "1", combinedPairCost: "0.99", grossLockedEdge: 0.01, netRealizedEdge: -0.01, realizedEdge: -0.01, fees: "0.02",
      }, "pos-fee"),
    ];
    const summary = buildExperimentSummary(events, "exp");
    expect(summary.winLossDistribution).toEqual({ wins: 0, losses: 1, breakeven: 0 });
    expect(summary.grossWinLossDistribution).toEqual({ wins: 1, losses: 0, breakeven: 0 });
    expect(summary.averageNetRealizedEdge).toBeCloseTo(-0.01, 8);
    expect(summary.averageRealizedEdge).toBeCloseTo(-0.01, 8);
    expect(summary.averageGrossLockedEdge).toBeCloseTo(0.01, 8);
  });
});

describe("real unhedged duration", () => {
  it("persists unhedgedDurationMs on HEDGE_COMPLETE so the average is not silently 0", async () => {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const { engine, journal } = await startQualifiedEngine({}, () => new Date(nowMs));
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill({ ...paperFill("btc-mkt", "UP", "0.40"), timestamp: new Date(nowMs).toISOString() });
      nowMs += 45_000;
      await engine.applyObservedFill({ ...paperFill("btc-mkt", "DOWN", "0.55"), timestamp: new Date(nowMs).toISOString() });
      const hedges = await journal.queryEvents({ types: ["HEDGE_COMPLETE"] });
      expect(hedges).toHaveLength(1);
      expect(Number((hedges[0]!.payload as { unhedgedDurationMs?: unknown }).unhedgedDurationMs)).toBe(45_000);
      const summary = buildExperimentSummary(hedges, "exp");
      expect(summary.averageUnhedgedDurationMs).toBe(45_000);
    } finally {
      await engine.stop();
    }
  });
});

describe("JSON-string settlement", () => {
  it("parses JSON-encoded outcome strings and never guesses on malformed JSON", () => {
    const ok = parsePublicBinaryResolution({
      state: { closed: true }, resolution: { umaResolutionStatus: "resolved" },
      outcomes: '["Up","Down"]',
      outcomePrices: '["1","0"]',
    });
    expect(ok).toMatchObject({ status: "resolved", winner: "UP", upPayoff: "1", downPayoff: "0" });

    const reversed = parsePublicBinaryResolution({
      state: { closed: true }, resolution: { umaResolutionStatus: "resolved" },
      outcomes: '["Yes","No"]',
      outcomePrices: '["1","0"]',
    }, "DOWN", "UP");
    expect(reversed).toMatchObject({ status: "resolved", winner: "DOWN" });

    const malformed = parsePublicBinaryResolution({
      state: { closed: true }, resolution: { umaResolutionStatus: "resolved" },
      outcomes: '["Up","Down"',
      outcomePrices: '["1","0"]',
    });
    expect(malformed.status).toBe("unresolved");
    expect(malformed.reason).toBe("MALFORMED_RESOLUTION_JSON");
    expect(malformed.winner).toBeNull();
  });
});

describe("missing-bid MTM", () => {
  it("keeps the experiment unresolved and does not treat acquisition cost as a mark", async () => {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const { engine, journal } = await startQualifiedEngine({
      EXPERIMENT_CLOSE_POLICY: "mark_to_market", EXPERIMENT_DURATION_HOURS: "1",
    }, () => new Date(nowMs));
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40"));
      const costEquity = engine.ledger.snapshot().equity;
      nowMs += 3_600_000 + 1;
      await engine.tickMaintenance();
      expect(engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
      expect(engine.experimentManifest()?.completedAt).toBeNull();
      const mtm = await journal.queryEvents({ types: ["PNL_SNAPSHOT"] });
      expect(mtm.some((item) => (item.payload as { markToMarket?: boolean }).markToMarket === true)).toBe(false);
      expect(engine.ledger.snapshot().equity).toBe(costEquity);
    } finally {
      await engine.stop();
    }
  });
});

describe("invalid manual-token preflight", () => {
  it("rejects POLYMARKET_TOKEN_IDS=up,down as insufficient for 48H_GROK_PAPER_EXPERIMENT", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down",
      ...grokPaperEnv(),
    });
    const engine = new EngineService(env, journal, { error: () => undefined }, {
      market: {
        registerManualToken: () => undefined,
        setTokenIds: (tokenIds: readonly string[]) => ({ changed: false, previous: [...tokenIds], next: [...tokenIds] }),
        run: async (_ids: unknown, _handlers: unknown, signal: AbortSignal) => new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve(), { once: true })),
      } as never,
      activation: new NoopGrokActivationBridge(),
    });
    await engine.start();
    try {
      const result = await engine.beginExperiment();
      expect(result.ok).toBe(false);
      expect(result.blockers).toEqual(expect.arrayContaining(["NO_ENTRY_QUALIFIED_MARKET"]));
    } finally {
      await engine.stop();
    }
  });
});

describe("complete current-experiment scoping", () => {
  it("excludes pre-experiment IDLE history from a complete summary", async () => {
    const { engine, journal } = await startQualifiedEngine();
    try {
      await engine.registerTriggerForResearch({
        id: createId("trigger"), traceId: createId("trace"), marketId: null, asset: "BTC", source: "system",
        kind: "SCHEDULED_REVIEW", severity: 1, score: 1, dedupeKey: "idle", evidence: {},
        sourceEventIds: ["e"], evidenceIds: ["e"], createdAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      } satisfies TriggerEvent);
      const idleEvents = await journal.readAll();
      expect(idleEvents.length).toBeGreaterThan(0);
      expect((await engine.beginExperiment()).ok).toBe(true);
      const started = engine.experimentManifest()!;
      await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40"));
      const all = await journal.readAll();
      const scoped = filterCurrentExperimentEvents(all, started);
      expect(scoped.every((event) => Date.parse(event.occurredAt) >= Date.parse(started.startedAt!))).toBe(true);
      expect(scoped.some((event) => event.type === "ENGINE_STARTED")).toBe(false);
      const complete = buildExperimentSummary(scoped, started.id, { metricsScope: "complete", complete: true, reportKind: "final" });
      const mixed = buildExperimentSummary(all, started.id, { metricsScope: "complete", complete: true, reportKind: "final" });
      expect(complete.decisionAccountingEventCount).toBeLessThan(mixed.decisionAccountingEventCount);
    } finally {
      await engine.stop();
    }
  });
});

describe("bounded-state pruning", () => {
  it("drops expired marketless triggers, stale signal times, errors, and recent trades", async () => {
    const { engine } = await startQualifiedEngine();
    try {
      const past = Date.now() - 120_000;
      engine.seedRuntimeBookkeepingForTest({
        signalReceivedTimes: [past, past - 1_000],
        executionErrors: [past - 4_000_000],
        recentTrades: [["mkt:UP:OPEN_FIRST_LEG", past]],
        trigger: {
          id: "expired-marketless", traceId: "t", marketId: null, asset: null, source: "system",
          kind: "SCHEDULED_REVIEW", severity: 1, score: 1, dedupeKey: "x", evidence: {},
          sourceEventIds: ["e"], evidenceIds: ["e"], createdAt: new Date(past).toISOString(),
          expiresAt: new Date(past + 1_000).toISOString(),
        },
      });
      expect(engine.runtimeBookkeepingForTest().activeTriggers).toBeGreaterThan(0);
      engine.pruneRuntimeBookkeepingForTest();
      const after = engine.runtimeBookkeepingForTest();
      expect(after.signalReceivedTimes).toBe(0);
      expect(after.executionErrors).toBe(0);
      expect(after.recentTrades).toBe(0);
      expect(after.activeTriggers).toBe(0);
      expect(after.signalsByTrigger).toBe(0);
      expect(after.lastEvaluatedSignalCount).toBe(0);
    } finally {
      await engine.stop();
    }
  });
});
