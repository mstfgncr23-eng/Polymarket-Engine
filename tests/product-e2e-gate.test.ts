import { describe, expect, it } from "vitest";
import {
  createId,
  grokSignalSubmissionSchema,
  NATIVE_DETERMINISTIC_MODEL_VERSION,
  triggerEventSchema,
  type Direction,
  type MarketSnapshot,
  type PaperFill,
  type ReferenceTick,
  type TriggerEvent,
} from "@pm/core";
import { hashExperimentReport, MemoryEventJournal, type EventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import {
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
  discoveryReport,
  qualified,
  qualifiedPaperEnv,
} from "./paper-engine-testkit.js";
import { snapshot } from "./fixtures.js";

const START_MS = Date.parse("2026-01-01T00:00:00.000Z");

type ScenarioClock = { nowMs: number };

async function drainRuntime(engine: EngineService): Promise<void> {
  const started = Date.now();
  while (engine.runtimeSupervisorSnapshot().unresolved !== 0) {
    if (Date.now() - started > 10_000) throw new Error("runtime drain timeout");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

async function waitFor<T>(read: () => T | Promise<T>, predicate: (value: T) => boolean, timeoutMs = 2_000): Promise<T> {
  const started = Date.now();
  while (true) {
    const value = await read();
    if (predicate(value)) return value;
    if (Date.now() - started > timeoutMs) throw new Error("test wait timeout");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

function iso(clock: ScenarioClock): string { return new Date(clock.nowMs).toISOString(); }

function marketBook(clock: ScenarioClock, outcome: "UP" | "DOWN", sequence: number): MarketSnapshot {
  const tokenId = outcome === "UP" ? "btc-up" : "btc-down";
  return snapshot({
    eventId: createId("market"),
    traceId: createId("trace"),
    marketId: "btc-mkt",
    tokenId,
    outcome,
    asset: "BTC",
    bids: [{ price: "0.39", quantity: "2000" }],
    asks: [{ price: "0.40", quantity: "300" }],
    sourceTimestamp: iso(clock),
    receivedAt: iso(clock),
    sequence,
  });
}

function referenceTick(clock: ScenarioClock, price: string): ReferenceTick {
  return {
    eventId: createId("reference"),
    traceId: createId("trace"),
    asset: "BTC",
    symbol: "BTCUSDT",
    venue: "deterministic-test",
    price,
    quantity: "1",
    sourceTimestamp: iso(clock),
    receivedAt: iso(clock),
    tradeId: createId("trade"),
  };
}

function outputFor(role: string, trigger: TriggerEvent, clock: ScenarioClock): Record<string, unknown> {
  const base = {
    confidence: 0.95,
    evidenceIds: trigger.sourceEventIds,
    expiresAt: trigger.expiresAt,
    reasonCodes: ["PRODUCT_E2E_GATE"],
    direction: "UP" as Direction,
  };
  if (role === "fair_probability_analyst") {
    return { ...base, fairProbability: 0.70, marketProbability: 0.40, estimatedEdge: 0.30 };
  }
  if (role === "market_microstructure_scanner") {
    return { ...base, spread: "0.01", imbalance: 0.80, depthChange: 0.10, pairCost: "0.80", unusualConditions: [] };
  }
  if (role === "crypto_market_scanner") {
    return { ...base, momentumBps: 20, volatilityBps: 8, volumeAnomalyScore: 0, accelerationBps: 2, regime: "TRENDING" };
  }
  if (role === "risk_hedge_analyst") {
    return { confidence: 0.95, evidenceIds: trigger.sourceEventIds, expiresAt: trigger.expiresAt, reasonCodes: ["PRODUCT_E2E_GATE"], recommendVeto: false, directionalExposureUsd: "0", unhedgedDurationMs: 0, pairedCost: "0.96", adverseScenarios: [] };
  }
  if (role === "strategy_supervisor") {
    return { confidence: 0.95, evidenceIds: trigger.sourceEventIds, expiresAt: trigger.expiresAt, reasonCodes: ["PRODUCT_E2E_GATE"], diagnosis: "deterministic product gate", regime: "TRENDING", proposedChange: null };
  }
  return { ...base, classification: "NEUTRAL", summary: "deterministic product gate", sources: [] };
}

function submission(
  trigger: TriggerEvent,
  role: "market_microstructure_scanner" | "crypto_market_scanner" | "news_x_intelligence_scanner" | "fair_probability_analyst" | "risk_hedge_analyst" | "strategy_supervisor",
  clock: ScenarioClock,
  suffix: string,
) {
  return grokSignalSubmissionSchema.parse({
    submissionId: createId(`sub-${suffix}`),
    triggerId: trigger.id,
    traceId: trigger.traceId,
    correlationId: createId("correlation"),
    marketId: trigger.marketId,
    asset: trigger.asset,
    timestamp: iso(clock),
    agentId: role,
    role,
    output: outputFor(role, trigger, clock),
  });
}

function unsynchronizedTrigger(clock: ScenarioClock): TriggerEvent {
  return triggerEventSchema.parse({
    id: createId("trigger"),
    traceId: createId("trace"),
    marketId: "btc-mkt",
    asset: "BTC",
    source: "system",
    kind: "BOOK_IMBALANCE",
    severity: 2,
    score: 1,
    dedupeKey: createId("dedupe"),
    evidence: {},
    sourceEventIds: [createId("source")],
    evidenceIds: [createId("evidence")],
    createdAt: iso(clock),
    expiresAt: new Date(clock.nowMs + 30_000).toISOString(),
  });
}

async function setupScenario(options: {
  journal?: EventJournal;
  clock?: ScenarioClock;
  resolutions?: Map<string, Record<string, unknown>>;
  env?: Record<string, string | undefined>;
} = {}) {
  const clock = options.clock ?? { nowMs: START_MS };
  const journal = options.journal ?? new MemoryEventJournal();
  const resolutions = options.resolutions ?? new Map<string, Record<string, unknown>>();
  const market = new ScriptedMarketAdapter();
  const endDate = new Date(START_MS + 10_000).toISOString();
  market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down", endDate)]));
  const env = qualifiedPaperEnv({
    EXPERIMENT_DURATION_HOURS: "0.001",
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    PAPER_EXECUTION_STYLE: "TAKER",
    PAPER_EXECUTION_LATENCY_MS: "0",
    ...options.env,
  });
  const engine = new EngineService(env, journal, { error: () => undefined }, {
    market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(),
    now: () => new Date(clock.nowMs),
    resolutionFetcher: async (id) => resolutions.get(id) ?? null,
  });
  await engine.start();
  await market.waitForHandlers();
  return { engine, journal, market, clock, resolutions };
}

async function beginAndGenerateTrigger(setup: Awaited<ReturnType<typeof setupScenario>>): Promise<TriggerEvent> {
  expect((await setup.engine.beginExperiment()).ok).toBe(true);
  await setup.market.emitSnapshot(marketBook(setup.clock, "UP", 1));
  await setup.market.emitSnapshot(marketBook(setup.clock, "DOWN", 1));
  await setup.engine.ingestReferenceTickForTest(referenceTick(setup.clock, "100"));
  setup.clock.nowMs = START_MS + 1_000;
  await setup.engine.ingestReferenceTickForTest(referenceTick(setup.clock, "102"));
  await drainRuntime(setup.engine);
  const trigger = setup.engine.state.recentTriggers.find((item) => item.marketId === "btc-mkt" && item.kind === "BOOK_IMBALANCE");
  expect(trigger).toBeTruthy();
  expect(setup.engine.state.recentTriggers.some((item) => item.kind === "BTC_RAPID_MOVE")).toBe(true);
  return trigger!;
}

function eventTypes(journal: EventJournal): Promise<string[]> {
  return journal.readAll().then((events) => events.map((event) => event.type));
}

describe("PR20 true product end-to-end gate", () => {
  it("Scenario A crosses one shared production chain from discovery through canonical report", async () => {
    const setup = await setupScenario();
    try {
      const cashBefore = setup.engine.ledger.snapshot().cash;
      await beginAndGenerateTrigger(setup);
      expect(setup.engine.state.latestConsensus?.consensusDirection).toBe("UP");
      expect(setup.engine.state.latestConsensus?.authority).toBe("NATIVE_DETERMINISTIC_V1");
      expect(setup.engine.ledger.snapshot().cash).not.toBe(cashBefore);

      // The books are immediately lockable as a profitable pair. A risk-reducing
      // HEDGE_SECOND_LEG must not wait for the strategy-wide fresh-entry cooldown.
      const paired = setup.engine.ledger.positions.get("btc-mkt");
      expect(paired?.pairedQuantity, JSON.stringify({
        position: paired,
        risk: setup.engine.state.latestRisk,
        events: await setup.journal.readAll(),
      })).not.toBe("0");
      expect(paired?.unpairedQuantity).toBe("0");

      setup.clock.nowMs = START_MS + 120_000;
      setup.resolutions.set("btc-mkt", {
        state: { closed: true },
        resolution: { umaResolutionStatus: "settled" },
        outcomes: { yes: { label: "Up", price: "1" }, no: { label: "Down", price: "0" } },
      });
      await setup.engine.tickMaintenance();
      await setup.engine.tickMaintenance();

      expect(setup.engine.experimentManifest()?.status).toBe("COMPLETED");
      const durable = await setup.journal.readDurableTransitionsAfter(0, 256);
      expect(durable.filter((row) => row.type === "PAPER_FILL")).toHaveLength(2);
      expect(durable.filter((row) => row.type === "POSITION_SETTLED")).toHaveLength(1);
      expect(new Set(durable.map((row) => row.operationKey)).size).toBe(durable.length);
      const ledger = setup.engine.ledger.snapshot();
      expect(Number(ledger.fees)).toBeGreaterThan(0);
      expect(Number(ledger.slippage)).toBeGreaterThanOrEqual(0);
      expect(Number(ledger.cash)).not.toBe(Number(cashBefore));

      const report = await setup.journal.loadExperimentReport(setup.engine.experimentManifest()!.id);
      expect(report).toBeTruthy();
      expect(report).toMatchObject({ complete: true, truncated: false });
      expect(hashExperimentReport(report!)).toBe(report!.contentHash);
      expect((report!.canonicalSummary as { totalFills?: number }).totalFills).toBe(2);
      expect((report!.canonicalSummary as { settlementCount?: number }).settlementCount).toBe(1);
      const types = await eventTypes(setup.journal);
      expect(types).toEqual(expect.arrayContaining([
        "EXPERIMENT_STARTED", "EXPERIMENT_START_VALIDATED", "TRIGGER_EVENT", "CONSENSUS_DECISION",
        "RISK_EVALUATION", "PAPER_ORDER_CREATED", "PAPER_FILL", "POSITION_SETTLED",
        "EXPERIMENT_COMPLETED", "EXPERIMENT_SOURCE_SEALED", "EXPERIMENT_REPORT_GENERATED",
      ]));
      expect(types).not.toContain("GROK_SIGNAL_ACCEPTED");
      const riskRows = await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] });
      const revalidatedFirstLeg = riskRows.find((row) => {
        const alpha = row.payload.alpha as { fairProvenance?: { revalidated?: boolean } } | undefined;
        return alpha?.fairProvenance?.revalidated === true;
      });
      expect(revalidatedFirstLeg?.payload.alpha).toMatchObject({
        fairProvenance: {
          revalidated: true,
          originalDirection: "UP",
          revalidatedDirection: "UP",
          up: { tokenId: "btc-up", sequence: expect.any(Number) },
          down: { tokenId: "btc-down", sequence: expect.any(Number) },
          reference: { venue: "deterministic-test", sourceTimestamp: expect.any(String) },
        },
      });
      const consensusRows = await setup.journal.queryEvents({ types: ["CONSENSUS_DECISION"] });
      expect(consensusRows[0]?.payload).toMatchObject({
        authority: "NATIVE_DETERMINISTIC_V1",
        featureVector: expect.any(Object),
        reference: { venue: "deterministic-test" },
      });
      expect(setup.engine.activationFootprint().sent).toBe(0);
      expect(setup.engine.experimentManifest()?.modelConfiguration).toMatchObject({
        provider: "native_deterministic", version: NATIVE_DETERMINISTIC_MODEL_VERSION, localExecution: true, externalInference: false,
      });
    } finally {
      await setup.engine.stop();
    }
  });

  it("Scenario B produces valid consensus but risk rejects without economic mutation", async () => {
    const setup = await setupScenario({ env: { EXPERIMENT_DURATION_HOURS: "1" } });
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(marketBook(setup.clock, "UP", 1));
      await setup.market.emitSnapshot(marketBook(setup.clock, "DOWN", 1));
      await setup.engine.ingestReferenceTickForTest(referenceTick(setup.clock, "100"));
      const before = setup.engine.ledger.snapshot();
      setup.clock.nowMs = START_MS + 16_000;
      await setup.engine.ingestReferenceTickForTest(referenceTick(setup.clock, "102"));
      await drainRuntime(setup.engine);
      expect(setup.engine.state.latestConsensus?.consensusDirection).toBe("UP");
      expect(setup.engine.state.latestRisk?.accepted, JSON.stringify({
        consensus: setup.engine.state.latestConsensus,
        events: await setup.journal.readAll(),
      })).toBe(false);
      expect(setup.engine.state.latestRisk?.vetoes).toContain("MARKET_FEED_STALE");
      expect(setup.engine.ledger.positions.size).toBe(0);
      expect(setup.engine.reservedCash()).toBe("0");
      expect(setup.engine.ledger.snapshot().cash).toBe(before.cash);
      expect(setup.engine.state.snapshot().decisionFeed.some((item) => item.type === "RISK_VETO")).toBe(true);
      expect((await setup.journal.readDurableTransitionsAfter(0, 256)).filter((row) => row.type === "PAPER_FILL")).toEqual([]);
    } finally {
      await setup.engine.stop();
    }
  });

  it("revalidates first-leg fair against the current book after consensus persistence", async () => {
    const journal = new MemoryEventJournal();
    const setup = await setupScenario({ journal, env: { EXPERIMENT_DURATION_HOURS: "1" } });
    let releaseHold: (() => void) | undefined;
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(marketBook(setup.clock, "UP", 1));
      await setup.market.emitSnapshot(marketBook(setup.clock, "DOWN", 1));
      await setup.engine.ingestReferenceTickForTest(referenceTick(setup.clock, "100"));

      const hold = new Promise<void>((resolve) => { releaseHold = resolve; });
      journal.setAppendInFlightHoldForTest(hold, ["CONSENSUS_DECISION"]);

      setup.clock.nowMs = START_MS + 1_000;
      await setup.engine.ingestReferenceTickForTest(referenceTick(setup.clock, "102"));
      await waitFor(() => journal.inFlightAppends, (count) => count > 0);

      // The original decision is UP. While its consensus row is durably
      // persisting, move the live pair into a state where fresh V1 evidence is
      // FLAT. The old UP fair must never be combined with this newer book.
      setup.clock.nowMs = START_MS + 1_100;
      await setup.market.emitSnapshot(snapshot({
        eventId: createId("market-revalidated-up"),
        traceId: createId("trace"),
        marketId: "btc-mkt",
        tokenId: "btc-up",
        outcome: "UP",
        asset: "BTC",
        bids: [{ price: "0.10", quantity: "1" }],
        asks: [{ price: "0.20", quantity: "2000" }],
        sourceTimestamp: iso(setup.clock),
        receivedAt: iso(setup.clock),
        sequence: 2,
      }));
      await setup.market.emitSnapshot(snapshot({
        eventId: createId("market-revalidated-down"),
        traceId: createId("trace"),
        marketId: "btc-mkt",
        tokenId: "btc-down",
        outcome: "DOWN",
        asset: "BTC",
        bids: [{ price: "0.80", quantity: "2000" }],
        asks: [{ price: "0.90", quantity: "1" }],
        sourceTimestamp: iso(setup.clock),
        receivedAt: iso(setup.clock),
        sequence: 2,
      }));

      releaseHold?.();
      releaseHold = undefined;
      journal.setAppendInFlightHoldForTest(null);
      await drainRuntime(setup.engine);

      expect(await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED", "PAPER_FILL"] })).toEqual([]);
      expect(setup.engine.ledger.positions.size).toBe(0);
      expect(setup.engine.state.snapshot().decisionFeed.some(
        (item) => item.type === "FIRST_LEG_REVALIDATION_REJECTED"
          && item.detail.reason === "DECISION_STATE_CHANGED",
      )).toBe(true);
    } finally {
      releaseHold?.();
      journal.setAppendInFlightHoldForTest(null);
      await setup.engine.stop();
    }
  });

  it("forces a bounded protective SELL after max unhedged duration and retries residual depth", async () => {
    const setup = await setupScenario({ env: { EXPERIMENT_DURATION_HOURS: "1", PAPER_EXECUTION_LATENCY_MS: "0" } });
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const upInitial = marketBook(setup.clock, "UP", 1);
      const downInitial = marketBook(setup.clock, "DOWN", 1);
      await setup.market.emitSnapshot(upInitial);
      await setup.market.emitSnapshot(downInitial);

      const firstLeg: PaperFill = {
        id: createId("fill-first"),
        orderId: createId("order-first"),
        traceId: createId("trace-first"),
        marketId: "btc-mkt",
        tokenId: "btc-up",
        outcome: "UP",
        side: "BUY",
        requestedPrice: "0.40",
        fillPrice: "0.40",
        quantity: "10",
        liquidityRole: "TAKER",
        latencyMs: 0,
        slippageUsd: "0",
        feeUsd: "0",
        timestamp: iso(setup.clock),
        sourceMarketEventId: upInitial.eventId,
        fillConfidence: 1,
        fillModel: "protective-derisk-test:first-leg",
        feeSchedule: upInitial.feeSchedule!,
      };
      await setup.engine.applyObservedFill(firstLeg);
      const opened = setup.engine.ledger.positions.get("btc-mkt");
      expect(opened?.unpairedQuantity).toBe("10");
      expect(opened?.unhedgedStartedAt).toBe(firstLeg.timestamp);

      // The hard timeout is five minutes. A fresh book one second before it
      // must not cause a protective order.
      setup.clock.nowMs = START_MS + 299_000;
      await setup.market.emitSnapshot(snapshot({
        ...marketBook(setup.clock, "UP", 2),
        bids: [{ price: "0.30", quantity: "4" }],
        asks: [{ price: "0.31", quantity: "100" }],
      }));
      await new Promise((resolve) => setTimeout(resolve, 5));
      expect((await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).filter(
        (row) => row.payload.intent === "PROTECTIVE_DE_RISK",
      )).toHaveLength(0);

      // After the timeout, only four shares are executable. The first de-risk
      // attempt must sell exactly those four and preserve the original episode
      // start for the six-share residual.
      setup.clock.nowMs = START_MS + 301_000;
      await setup.market.emitSnapshot(snapshot({
        ...marketBook(setup.clock, "UP", 3),
        bids: [{ price: "0.30", quantity: "4" }],
        asks: [{ price: "0.31", quantity: "100" }],
      }));
      await waitFor(
        () => setup.journal.queryEvents({ types: ["PAPER_FILL"] }),
        (rows) => rows.some((row) => row.payload.intent === "PROTECTIVE_DE_RISK"),
      );

      const residual = setup.engine.ledger.positions.get("btc-mkt");
      expect(residual?.unpairedOutcome).toBe("UP");
      expect(residual?.unpairedQuantity).toBe("6");
      expect(residual?.unhedgedStartedAt).toBe(firstLeg.timestamp);

      const firstProtectiveFill = (await setup.journal.queryEvents({ types: ["PAPER_FILL"] }))
        .find((row) => row.payload.intent === "PROTECTIVE_DE_RISK");
      expect(firstProtectiveFill?.payload).toMatchObject({
        side: "SELL",
        quantity: "4",
        intent: "PROTECTIVE_DE_RISK",
        deRisk: {
          deRiskReason: "MAX_UNHEDGED_DURATION",
          attemptedAction: "SELL_UNPAIRED",
          requestedQty: "10",
          executableQty: "4",
          residualQuantity: "6",
          finalState: "RESIDUAL_UNHEDGED",
        },
      });

      // New bid depth closes the residual on the next fresh book. A complete
      // directional unwind removes the zero-inventory position so the market
      // cannot remain stuck behind an empty Position object.
      setup.clock.nowMs = START_MS + 302_000;
      await setup.market.emitSnapshot(snapshot({
        ...marketBook(setup.clock, "UP", 4),
        bids: [{ price: "0.29", quantity: "6" }],
        asks: [{ price: "0.30", quantity: "100" }],
      }));
      await waitFor(
        () => setup.journal.queryEvents({ types: ["PAPER_FILL"] }),
        (rows) => rows.filter((row) => row.payload.intent === "PROTECTIVE_DE_RISK").length === 2,
      );
      expect(setup.engine.ledger.positions.has("btc-mkt")).toBe(false);

      const protectiveRisk = (await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] }))
        .filter((row) => Boolean(row.payload.deRisk));
      expect(protectiveRisk).toHaveLength(2);
      expect(protectiveRisk.every((row) => row.payload.accepted === true)).toBe(true);
      const protectiveFills = (await setup.journal.queryEvents({ types: ["PAPER_FILL"] }))
        .filter((row) => row.payload.intent === "PROTECTIVE_DE_RISK");
      expect(protectiveFills[1]?.payload).toMatchObject({
        side: "SELL",
        quantity: "6",
        deRisk: {
          retryCount: 2,
          residualQuantity: "0",
          residualExposure: "0",
          finalState: "RISK_NEUTRALIZED",
        },
      });
    } finally {
      await setup.engine.stop();
    }
  });

  it("Scenario C accepts one valid response but never creates consensus or a trade", async () => {
    const setup = await setupScenario();
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      const trigger = unsynchronizedTrigger(setup.clock);
      await setup.engine.registerTriggerForResearch(trigger);
      const before = setup.engine.ledger.snapshot();
      const result = await setup.engine.submitGrokSignal(submission(trigger, "fair_probability_analyst", setup.clock, "insufficient"), "fair_probability_analyst");
      expect(result.status).toBe("accepted");
      expect(setup.engine.state.latestConsensus).toBeNull();
      expect(setup.engine.ledger.positions.size).toBe(0);
      expect(setup.engine.ledger.snapshot().cash).toBe(before.cash);
      expect(await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED", "PAPER_FILL"] })).toEqual([]);
    } finally {
      await setup.engine.stop();
    }
  });

  it("Scenario E accepts legacy Grok research without crossing the native economic boundary", async () => {
    const setup = await setupScenario();
    try {
      await beginAndGenerateTrigger(setup);
      const nativeBefore = structuredClone(setup.engine.state.latestConsensus);
      const riskBefore = structuredClone(setup.engine.state.latestRisk);
      const positionsBefore = structuredClone([...setup.engine.ledger.positions.entries()]);
      const ledgerBefore = setup.engine.ledger.snapshot();
      const reservedBefore = setup.engine.reservedCash();
      const beforeCounts = {
        consensus: (await setup.journal.queryEvents({ types: ["CONSENSUS_DECISION"] })).length,
        risk: (await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] })).length,
        orders: (await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).length,
        fills: (await setup.journal.queryEvents({ types: ["PAPER_FILL"] })).length,
      };
      const nativeTrigger = setup.engine.state.recentTriggers.find((item) => item.id === nativeBefore?.triggerId);
      expect(nativeBefore).toBeTruthy();
      expect(nativeTrigger).toBeTruthy();

      const roles = [
        "market_microstructure_scanner", "crypto_market_scanner", "news_x_intelligence_scanner",
        "fair_probability_analyst", "risk_hedge_analyst", "strategy_supervisor",
      ] as const;
      for (const role of roles) {
        await expect(
          setup.engine.submitGrokSignal(submission(nativeTrigger!, role, setup.clock, `non-authority-${role}`), role),
        ).resolves.toMatchObject({ status: "accepted" });
      }

      const afterCounts = {
        consensus: (await setup.journal.queryEvents({ types: ["CONSENSUS_DECISION"] })).length,
        risk: (await setup.journal.queryEvents({ types: ["RISK_EVALUATION"] })).length,
        orders: (await setup.journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).length,
        fills: (await setup.journal.queryEvents({ types: ["PAPER_FILL"] })).length,
      };
      expect(setup.engine.state.latestConsensus).toEqual(nativeBefore);
      expect(setup.engine.state.latestRisk).toEqual(riskBefore);
      expect(afterCounts).toEqual(beforeCounts);
      expect(setup.engine.ledger.snapshot().cash).toBe(ledgerBefore.cash);
      expect([...setup.engine.ledger.positions.entries()]).toEqual(positionsBefore);
      expect(setup.engine.reservedCash()).toBe(reservedBefore);
      expect((await setup.journal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED"] })).length).toBe(roles.length);
    } finally {
      await setup.engine.stop();
    }
  });

  it("uses the TriggerEngine 60-second reference window instead of the last eight state prices", async () => {
    const setup = await setupScenario();
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(marketBook(setup.clock, "UP", 1));
      await setup.market.emitSnapshot(marketBook(setup.clock, "DOWN", 1));

      const prices = ["100", "100.13", "100.13", "100.13", "100.13", "100.13", "100.13", "100.13", "100.13", "100.29"];
      for (const [index, price] of prices.entries()) {
        setup.clock.nowMs = START_MS + index * 1_000;
        await setup.engine.ingestReferenceTickForTest(referenceTick(setup.clock, price));
      }
      await drainRuntime(setup.engine);

      const rapidMove = setup.engine.state.recentTriggers.find((item) => item.kind === "BTC_RAPID_MOVE");
      const rapidMoveBps = Number(rapidMove?.evidence.moveBps);
      const featureVector = setup.engine.state.latestConsensus?.featureVector as {
        referenceMoveBps?: number;
        referenceWindowMs?: number;
        referenceVenue?: string | null;
      } | undefined;
      expect(setup.engine.referenceWindowTotalEntriesForTest()).toBe(10);
      expect(rapidMove).toBeTruthy();
      expect(rapidMoveBps).toBeCloseTo(29, 8);
      expect(featureVector?.referenceMoveBps).toBeCloseTo(29, 8);
      expect(featureVector?.referenceWindowMs).toBe(9_000);
      expect(featureVector?.referenceVenue).toBe("deterministic-test");
    } finally {
      await setup.engine.stop();
    }
  });

  it("Scenario D replays a durable fill after crash and settles exactly once", async () => {
    const journal = new MemoryEventJournal();
    const clock = { nowMs: START_MS };
    const resolutions = new Map<string, Record<string, unknown>>();
    const setup = await setupScenario({ journal, clock, resolutions, env: { PAPER_EXECUTION_LATENCY_MS: "200" } });
    try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(marketBook(clock, "UP", 1));
      await setup.market.emitSnapshot(marketBook(clock, "DOWN", 1));
      await setup.engine.ingestReferenceTickForTest(referenceTick(clock, "100"));
      setup.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY");
      clock.nowMs = START_MS + 1_000;
      await setup.engine.ingestReferenceTickForTest(referenceTick(clock, "102"));
      await waitFor(() => journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] }), (events) => events.length === 1);
      await waitFor(() => journal.readDurableTransitionsAfter(0, 256), (rows) => rows.some((row) => row.type === "PAPER_FILL"));
      await drainRuntime(setup.engine);
      const trigger = setup.engine.state.recentTriggers.find((item) => item.kind === "BTC_RAPID_MOVE");
      expect(trigger).toBeTruthy();
      const durableFill = (await journal.readDurableTransitionsAfter(0, 256)).filter((row) => row.type === "PAPER_FILL");
      expect(durableFill).toHaveLength(1);
      expect(setup.engine.ledger.positions.size).toBe(0);
      await setup.engine.stop();
    } catch (error) {
      if (!setup.engine.state.draining) await setup.engine.stop();
      throw error;
    }

    const recovered = await setupScenario({ journal, clock, resolutions });
    try {
      const recoveredPosition = recovered.engine.ledger.positions.get("btc-mkt");
      expect(recoveredPosition?.unpairedQuantity).not.toBe("0");
      const recoveredFillTimestamp = ((durableFill[0]!.payload.fill as { timestamp?: string }).timestamp);
      expect(recoveredPosition?.unhedgedStartedAt).toBe(recoveredFillTimestamp);
      expect((await journal.queryEvents({ types: ["PAPER_FILL"] })).length).toBe(1);
      clock.nowMs = START_MS + 20_000;
      resolutions.set("btc-mkt", {
        state: { closed: true },
        resolution: { umaResolutionStatus: "settled" },
        outcomes: { yes: { label: "Up", price: "1" }, no: { label: "Down", price: "0" } },
      });
      await recovered.engine.tickMaintenance();
      await recovered.engine.tickMaintenance();
      expect((await journal.queryEvents({ types: ["PAPER_FILL"] })).length).toBe(1);
      expect((await journal.queryEvents({ types: ["POSITION_SETTLED"] })).length).toBe(1);
      const durableEconomic = await journal.queryDurableTransitions({ types: ["PAPER_FILL", "POSITION_SETTLED"], limit: 20 });
      expect(durableEconomic.filter((row) => row.type === "PAPER_FILL")).toHaveLength(1);
      expect(durableEconomic.filter((row) => row.type === "POSITION_SETTLED")).toHaveLength(1);
      expect(recovered.engine.ledger.positions.has("btc-mkt")).toBe(false);
      expect(recovered.engine.experimentManifest()?.status).toBe("COMPLETED");
    } finally {
      await recovered.engine.stop();
    }
  });
});
