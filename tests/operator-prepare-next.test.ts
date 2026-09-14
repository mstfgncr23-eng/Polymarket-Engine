import { MemoryEventJournal } from "@pm/db";
import { createId, type ReferenceTick, type TriggerEvent } from "@pm/core";
import { describe, expect, it } from "vitest";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { SseHub, createApiServer } from "../apps/engine/src/server.js";
import {
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
  discoveryReport,
  qualified,
  qualifiedPaperEnv,
} from "./paper-engine-testkit.js";
import { fill, snapshot } from "./fixtures.js";

const START_MS = Date.parse("2026-09-01T00:00:00.000Z");

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function p0Trigger(suffix: string, marketId = "btc-mkt"): TriggerEvent {
  const traceId = createId(`trace-${suffix}`);
  return {
    id: createId(`trigger-${suffix}`), traceId, marketId, asset: "BTC", source: "risk",
    kind: "SCHEDULED_REVIEW", severity: 0, score: 1, dedupeKey: `dedupe-${suffix}`, evidence: {},
    sourceEventIds: [createId(`source-${suffix}`)], evidenceIds: [],
    createdAt: new Date(START_MS).toISOString(), expiresAt: new Date(START_MS + 60_000).toISOString(),
  };
}

async function setup(input: { journal?: MemoryEventJournal; nowMs?: number; closePolicy?: "observe_until_settlement" | "mark_to_market" } = {}) {
  const clock = { nowMs: input.nowMs ?? START_MS };
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down", new Date(clock.nowMs + 60_000).toISOString())]));
  const journal = input.journal ?? new MemoryEventJournal();
  const env = qualifiedPaperEnv({
    ASTRA_ENABLED: "true",
    PAPER_START_BALANCE: "1250",
    EXPERIMENT_DURATION_HOURS: "48",
    EXPERIMENT_CLOSE_POLICY: input.closePolicy ?? "observe_until_settlement",
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    DISCOVERY_INTERVAL_MS: "3600000",
  });
  const engine = new EngineService(env, journal, { error: () => undefined }, {
    market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(),
    now: () => new Date(clock.nowMs),
  });
  await engine.start();
  return { clock, engine, env, journal, market };
}

async function operatorServer(engine: EngineService, journal: MemoryEventJournal) {
  return createApiServer({
    state: engine.state,
    journal,
    metrics: new EngineMetrics(),
    sse: new SseHub(),
    dashboardOrigin: "http://localhost:3000",
    logger: false,
    mcp: { bearerTokens: "", allowedOrigins: "https://grok.com", rateLimitPerMinute: 30 },
    submitGrokSignal: async () => ({ status: "ignored" }),
    prepareNextExperiment: () => engine.prepareNextExperiment(),
    operatorToken: "operator-token",
    nodeEnv: "production",
  });
}

describe("prepare-next paper experiment boundary", () => {
  it("rejects IDLE, RUNNING, and ENTRY_CLOSED without mutation", async () => {
    const test = await setup();
    try {
      expect((await test.engine.prepareNextExperiment()).blockers).toEqual(["EXPERIMENT_NOT_COMPLETED"]);
      expect((await test.engine.beginExperiment()).ok).toBe(true);
      expect((await test.engine.prepareNextExperiment()).blockers).toEqual(["EXPERIMENT_NOT_COMPLETED"]);
      await test.engine.closeExperimentEntries();
      expect((await test.engine.prepareNextExperiment()).blockers).toEqual(["EXPERIMENT_NOT_COMPLETED"]);
      expect(await test.engine.latestDurableSequenceForTest()).toBe(3);
    } finally {
      await test.engine.stop();
    }
  });

  it("requires the operator token and exposes blockers as HTTP 409", async () => {
    const test = await setup();
    const server = await operatorServer(test.engine, test.journal);
    try {
      expect((await server.inject({ method: "POST", url: "/api/operator/experiment/prepare-next" })).statusCode).toBe(401);
      const blocked = await server.inject({
        method: "POST", url: "/api/operator/experiment/prepare-next",
        headers: { authorization: "Bearer operator-token" },
      });
      expect(blocked.statusCode).toBe(409);
      expect(blocked.json()).toMatchObject({ ok: false, blockers: ["EXPERIMENT_NOT_COMPLETED"] });
    } finally {
      await server.close();
      await test.engine.stop();
    }
  });

  it("durably prepares a new empty IDLE experiment while preserving the old final report", async () => {
    const test = await setup();
    const server = await operatorServer(test.engine, test.journal);
    try {
      expect((await test.engine.beginExperiment()).ok).toBe(true);
      await test.market.emitSnapshot(snapshot({
        marketId: "btc-mkt", tokenId: "btc-up", outcome: "UP", asset: "BTC",
        receivedAt: new Date(test.clock.nowMs).toISOString(), sourceTimestamp: new Date(test.clock.nowMs).toISOString(),
      }));
      expect(test.engine.triggerDedupeEntryCount()).toBeGreaterThan(0);
      const warmReferenceTick: ReferenceTick = {
        eventId: "prepare-warm-reference", traceId: "prepare-warm-reference-trace", asset: "BTC", symbol: "BTCUSDT", venue: "test",
        price: "100", quantity: "1", sourceTimestamp: new Date(test.clock.nowMs).toISOString(), receivedAt: new Date(test.clock.nowMs).toISOString(), tradeId: "1",
      };
      await test.engine.ingestReferenceTickForTest(warmReferenceTick);
      const warmReferenceEntries = test.engine.referenceWindowTotalEntriesForTest();
      expect(warmReferenceEntries).toBeGreaterThan(0);
      await test.engine.completeExperimentForTest();
      expect((await test.engine.ensureSourceSealForTest()).sealed).toBe(true);
      await test.engine.writeFinalReportForTest();
      const previous = test.engine.experimentManifest()!;
      const oldReport = await test.journal.loadExperimentReport(previous.id, "final");
      expect(oldReport).toMatchObject({ experimentId: previous.id, reportKind: "final", complete: true, truncated: false });
      test.engine.seedRuntimeBookkeepingForTest({ signalReceivedTimes: [test.clock.nowMs], executionErrors: [test.clock.nowMs], recentTrades: [["old", test.clock.nowMs]] });

      const response = await server.inject({
        method: "POST", url: "/api/operator/experiment/prepare-next",
        headers: { authorization: "Bearer operator-token" },
      });
      expect(response.statusCode).toBe(200);
      const body = response.json();
      expect(body).toMatchObject({ ok: true, checkpoint: "ok", experiment: { status: "IDLE" } });

      const next = test.engine.experimentManifest()!;
      expect(next.id).not.toBe(previous.id);
      expect(next.status).toBe("IDLE");
      expect(next.startedAt).toBeNull();
      expect(next.entryClosesAt).toBeNull();
      expect(next.completedAt).toBeNull();
      expect(next.modelConfiguration.authority).toBe("ASTRA_BRAIN_V1");
      expect(next.startingBalance).toBe("1250");
      expect(test.engine.ledger.snapshot()).toMatchObject({
        startingBalance: "1250", cash: "1250", reservedCash: "0", positionValue: "0", equity: "1250",
        realizedPnl: "0", unrealizedPnl: "0", grossPnl: "0", fees: "0", slippage: "0", aiCost: "0",
        infrastructureCost: "0", netTradingPnl: "0", netEconomicPnl: "0", highWaterMark: "1250", drawdown: "0",
      });
      expect(test.engine.ledger.positions.size).toBe(0);
      expect(test.engine.restingOrdersForTest().size).toBe(0);
      expect(test.engine.makerReservationsForTest().size).toBe(0);
      expect(test.engine.astraExecutionCorrelationsForTest()).toEqual([]);
      expect(test.engine.settledMarketsForTest().size).toBe(0);
      expect(test.engine.runtimeBookkeepingForTest()).toMatchObject({ signalReceivedTimes: 0, executionErrors: 0, recentTrades: 0, activeTriggers: 0, signalsByTrigger: 0 });
      expect(test.engine.triggerDedupeEntryCount()).toBe(0);
      expect(test.engine.referenceWindowTotalEntriesForTest()).toBe(warmReferenceEntries);
      expect(test.engine.astraGovernorSnapshotForTest().dispatches).toHaveLength(0);
      expect(test.engine.reportGeneratedForTest()).toBe(false);
      expect(test.engine.sourceSealStateForTest()).toMatchObject({ sealing: false, sealed: false });

      const prepared = await test.engine.readDurableAfterForTest(0, 32);
      expect(prepared.filter((row) => row.type === "EXPERIMENT_PREPARED")).toHaveLength(1);
      const preparedRow = prepared.find((row) => row.type === "EXPERIMENT_PREPARED")!;
      expect(preparedRow.payload).toMatchObject({ previousExperimentId: previous.id, experiment: { id: next.id, status: "IDLE" }, ledger: { startingBalance: "1250" } });
      expect(await test.journal.loadExperimentReport(previous.id, "final")).toEqual(oldReport);
      expect(await test.journal.loadExperimentReport(next.id, "final")).toBeNull();

      const repeated = await server.inject({
        method: "POST", url: "/api/operator/experiment/prepare-next",
        headers: { authorization: "Bearer operator-token" },
      });
      expect(repeated.statusCode).toBe(409);
      expect((await test.engine.readDurableAfterForTest(0, 32)).filter((row) => row.type === "EXPERIMENT_PREPARED")).toHaveLength(1);
    } finally {
      await server.close();
      await test.engine.stop();
    }
  });

  it("recovers a prepared IDLE baseline after a crash between durable commit and apply", async () => {
    const journal = new MemoryEventJournal();
    const first = await setup({ journal });
    let recovered: EngineService | null = null;
    try {
      expect((await first.engine.beginExperiment()).ok).toBe(true);
      await first.engine.completeExperimentForTest();
      expect((await first.engine.ensureSourceSealForTest()).sealed).toBe(true);
      await first.engine.writeFinalReportForTest();
      const previous = first.engine.experimentManifest()!;
      const oldReport = await journal.loadExperimentReport(previous.id, "final");
      first.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY", { operationKey: `experiment:${previous.id}:prepared` });
      await expect(first.engine.prepareNextExperiment()).rejects.toThrow("CRASH_INJECTED");
      const preparedRows = (await journal.queryDurableTransitions({ types: ["EXPERIMENT_PREPARED"], limit: 8 }));
      expect(preparedRows).toHaveLength(1);
      expect(first.engine.experimentManifest()?.id).toBe(previous.id);
      await first.engine.stop();

      recovered = new EngineService(first.env, journal, { error: () => undefined }, {
        market: first.market as never,
        referenceRegistry: new ScriptedReferenceRegistry() as never,
        activation: new NoopGrokActivationBridge(),
        now: () => new Date(first.clock.nowMs),
      });
      first.market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down", new Date(first.clock.nowMs + 60_000).toISOString())]));
      await recovered.start();
      const next = recovered.experimentManifest()!;
      expect(next.id).toBe((preparedRows[0]!.payload.experiment as { id: string }).id);
      expect(next.status).toBe("IDLE");
      expect(recovered.ledger.snapshot()).toMatchObject({ startingBalance: "1250", cash: "1250", reservedCash: "0", equity: "1250" });
      expect(recovered.ledger.positions.size).toBe(0);
      expect(await journal.loadExperimentReport(previous.id, "final")).toEqual(oldReport);
      expect(await journal.loadExperimentReport(next.id, "final")).toBeNull();
      expect((await journal.queryDurableTransitions({ types: ["EXPERIMENT_PREPARED"], limit: 8 }))).toHaveLength(1);
      expect((await recovered.beginExperiment()).ok).toBe(true);
      expect(recovered.experimentManifest()?.id).toBe(next.id);
      expect(recovered.experimentManifest()?.status).toBe("RUNNING");
    } finally {
      if (recovered) await recovered.stop();
      else if (first.engine.lastShutdownResult().work === null) await first.engine.stop();
    }
  });

  it("rejects a completed mark-to-market experiment while its position obligation remains", async () => {
    const test = await setup({ closePolicy: "mark_to_market" });
    try {
      expect((await test.engine.beginExperiment()).ok).toBe(true);
      await test.engine.applyObservedFill({
        ...fill("UP", "0.40", "10"), id: "open-position-fill", orderId: "open-position-order", marketId: "btc-mkt",
        tokenId: "btc-up", timestamp: new Date(test.clock.nowMs).toISOString(),
      });
      await test.engine.completeExperimentForTest();
      expect((await test.engine.ensureSourceSealForTest()).sealed).toBe(true);
      await test.engine.writeFinalReportForTest();
      const result = await test.engine.prepareNextExperiment();
      expect(result.ok).toBe(false);
      expect(result.blockers).toContain("REPORT_HAS_UNRESOLVED_OBLIGATIONS");
      expect(test.engine.experimentManifest()?.status).toBe("COMPLETED");
      expect((await test.journal.queryDurableTransitions({ types: ["EXPERIMENT_PREPARED"], limit: 8 }))).toHaveLength(0);
    } finally {
      await test.engine.stop();
    }
  });

  it("freezes new P0 admission after the prepare drain point and preserves same-run hedging", async () => {
    const test = await setup();
    let releaseReportLoad!: () => void;
    let reportLoadStarted!: () => void;
    const reportLoadGate = new Promise<void>((resolve) => { releaseReportLoad = resolve; });
    const reportLoadStartedPromise = new Promise<void>((resolve) => { reportLoadStarted = resolve; });
    const originalLoadReport = test.journal.loadExperimentReport.bind(test.journal);
    let heldReportLoad = false;
    let stalePending: Promise<void> | undefined;
    const stale = p0Trigger("stale-after-drain");
    const sameRun = p0Trigger("same-run");

    try {
      expect((await test.engine.beginExperiment()).ok).toBe(true);
      const up = snapshot({
        marketId: "btc-mkt", tokenId: "btc-up", outcome: "UP", asset: "BTC",
        receivedAt: new Date(START_MS).toISOString(), sourceTimestamp: new Date(START_MS).toISOString(),
        asks: [{ price: "0.48", quantity: "300" }], bids: [{ price: "0.47", quantity: "300" }],
      });
      const down = snapshot({
        marketId: "btc-mkt", tokenId: "btc-down", outcome: "DOWN", asset: "BTC",
        receivedAt: new Date(START_MS).toISOString(), sourceTimestamp: new Date(START_MS).toISOString(),
        asks: [{ price: "0.51", quantity: "300" }], bids: [{ price: "0.50", quantity: "300" }],
      });
      await test.market.emitSnapshot(up);
      await test.market.emitSnapshot(down);
      await waitFor(() => test.engine.runtimeSupervisorSnapshot().unresolved === 0);
      await test.engine.completeExperimentForTest();
      expect((await test.engine.ensureSourceSealForTest()).sealed).toBe(true);
      await test.engine.writeFinalReportForTest();
      const previous = test.engine.experimentManifest()!;

      test.journal.loadExperimentReport = async (experimentId, reportKind = "final") => {
        if (!heldReportLoad) {
          heldReportLoad = true;
          reportLoadStarted();
          await reportLoadGate;
        }
        return originalLoadReport(experimentId, reportKind);
      };

      const preparePromise = test.engine.prepareNextExperiment();
      await reportLoadStartedPromise;

      // prepare-next has frozen trigger admission and already observed a stable
      // zero-unresolved supervisor snapshot. A P0 arriving now must never enter
      // the supervisor, so there is no await window that can cross the boundary.
      stalePending = test.engine.registerTriggerForResearch(stale);
      await stalePending;
      expect(test.engine.runtimeSupervisorSnapshot().unresolved).toBe(0);
      expect(await test.journal.readTrace(stale.traceId)).toEqual([]);

      releaseReportLoad();
      expect((await preparePromise).ok).toBe(true);
      const next = test.engine.experimentManifest()!;
      expect(next.id).not.toBe(previous.id);
      expect(next.status).toBe("IDLE");
      expect((await test.engine.beginExperiment()).ok).toBe(true);
      expect(test.engine.experimentManifest()?.status).toBe("RUNNING");

      await test.engine.applyObservedFill({
        ...fill("UP", "0.40", "10"), id: "fresh-run-first-leg", orderId: "fresh-run-first-leg-order",
        marketId: "btc-mkt", tokenId: "btc-up", timestamp: new Date(START_MS).toISOString(),
      });
      const beforeSameRun = test.engine.ledger.snapshot();
      expect(await test.journal.readTrace(stale.traceId)).toEqual([]);
      expect(test.engine.ledger.snapshot()).toEqual(beforeSameRun);

      // The fence is scoped only to prepare-next. Once the boundary has
      // completed, current-generation deterministic hedging remains immediate
      // and does not wait for Astra.
      await test.engine.registerTriggerForResearch(sameRun);
      const sameRunEvents = await test.journal.readTrace(sameRun.traceId);
      expect(sameRunEvents.some((event) => event.type === "ASTRA_DECISION" && event.payload.bypass === "DETERMINISTIC_HEDGE")).toBe(true);
      expect(sameRunEvents.some((event) => event.type === "RISK_EVALUATION")).toBe(true);
    } finally {
      releaseReportLoad();
      await stalePending?.catch(() => undefined);
      test.journal.loadExperimentReport = originalLoadReport;
      await test.engine.stop();
    }
  });

  it("blocks prepare on trigger work admitted before the freeze, then succeeds after that work drains", async () => {
    const test = await setup();
    let releaseTrigger!: () => void;
    const triggerHold = new Promise<void>((resolve) => { releaseTrigger = resolve; });
    let pending: Promise<void> | undefined;

    try {
      expect((await test.engine.beginExperiment()).ok).toBe(true);
      await test.engine.completeExperimentForTest();
      expect((await test.engine.ensureSourceSealForTest()).sealed).toBe(true);
      await test.engine.writeFinalReportForTest();

      test.engine.setProcessTriggerHoldForTest(triggerHold);
      pending = test.engine.registerTriggerForResearch(p0Trigger("pre-freeze-inflight"));
      await waitFor(() => test.engine.runtimeSupervisorSnapshot().lanes.p0.inFlight === 1);

      const blocked = await test.engine.prepareNextExperiment();
      expect(blocked).toMatchObject({ ok: false, blockers: ["RUNTIME_WORK_UNRESOLVED"] });
      expect((await test.journal.queryDurableTransitions({ types: ["EXPERIMENT_PREPARED"], limit: 8 }))).toHaveLength(0);

      test.engine.setProcessTriggerHoldForTest(null);
      releaseTrigger();
      await pending;
      await waitFor(() => test.engine.runtimeSupervisorSnapshot().unresolved === 0);

      const prepared = await test.engine.prepareNextExperiment();
      expect(prepared).toMatchObject({ ok: true, experiment: { status: "IDLE" } });
      expect((await test.journal.queryDurableTransitions({ types: ["EXPERIMENT_PREPARED"], limit: 8 }))).toHaveLength(1);
    } finally {
      test.engine.setProcessTriggerHoldForTest(null);
      releaseTrigger();
      await pending?.catch(() => undefined);
      await test.engine.stop();
    }
  });
});
