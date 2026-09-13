import { MemoryEventJournal } from "@pm/db";
import { createId, type TriggerEvent } from "@pm/core";
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
      await test.engine.completeExperimentForTest();
      expect((await test.engine.ensureSourceSealForTest()).sealed).toBe(true);
      await test.engine.writeFinalReportForTest();
      const previous = test.engine.experimentManifest()!;
      const oldReport = await test.journal.loadExperimentReport(previous.id, "final");
      expect(oldReport).toMatchObject({ experimentId: previous.id, reportKind: "final", complete: true, truncated: false });
      const budgetBefore = test.engine.astraBudgetSnapshotForTest();
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
      expect(test.engine.astraBudgetSnapshotForTest()).toEqual(budgetBefore);
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

  it("unconditionally fences an old-generation P0 trigger across preparation while preserving same-run hedging", async () => {
    const test = await setup();
    let releaseReportLoad!: () => void;
    let reportLoadStarted!: () => void;
    const reportLoadGate = new Promise<void>((resolve) => { releaseReportLoad = resolve; });
    const reportLoadStartedPromise = new Promise<void>((resolve) => { reportLoadStarted = resolve; });
    const originalLoadReport = test.journal.loadExperimentReport.bind(test.journal);
    let heldReportLoad = false;
    let releaseBlockers!: () => void;
    const blockerGate = new Promise<void>((resolve) => { releaseBlockers = resolve; });
    let blockerHoldsEntered!: () => void;
    const blockerHoldsEnteredPromise = new Promise<void>((resolve) => { blockerHoldsEntered = resolve; });
    let blockerHoldCount = 0;
    const blockerHold = {
      then: (onFulfilled: (value: void) => unknown) => {
        blockerHoldCount += 1;
        if (blockerHoldCount === 4) blockerHoldsEntered();
        return blockerGate.then(onFulfilled);
      },
    } as unknown as Promise<void>;
    let stalePending: Promise<void> | undefined;
    let blockerPendings: Promise<void>[] = [];
    const stale = p0Trigger("stale");
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

      test.engine.setProcessTriggerHoldForTest(blockerHold);
      const preparePromise = test.engine.prepareNextExperiment();
      await reportLoadStartedPromise;

      // The prepare operation has already passed its unresolved-work check,
      // but has not crossed the durable boundary yet. Fill every P0 worker
      // with old-generation work so the stale trigger remains queued until
      // the new generation is authoritative.
      blockerPendings = ["one", "two", "three", "four"].map((suffix) => test.engine.registerTriggerForResearch(p0Trigger(`blocker-${suffix}`, `blocker-${suffix}-mkt`)));
      await blockerHoldsEnteredPromise;
      stalePending = test.engine.registerTriggerForResearch(stale);
      await waitFor(() => test.engine.runtimeSupervisorSnapshot().lanes.p0.queued >= 1);
      releaseReportLoad();

      expect((await preparePromise).ok).toBe(true);
      const next = test.engine.experimentManifest()!;
      expect(next.id).not.toBe(previous.id);
      expect(next.status).toBe("IDLE");
      expect((await test.engine.beginExperiment()).ok).toBe(true);
      expect(test.engine.experimentManifest()?.status).toBe("RUNNING");

      // Put a fresh-run unpaired position on the same market. The stale P0
      // trigger must not be able to reinterpret it as its own hedge.
      await test.engine.applyObservedFill({
        ...fill("UP", "0.40", "10"), id: "fresh-run-first-leg", orderId: "fresh-run-first-leg-order",
        marketId: "btc-mkt", tokenId: "btc-up", timestamp: new Date(START_MS).toISOString(),
      });
      const beforeStaleRelease = test.engine.ledger.snapshot();
      test.engine.setProcessTriggerHoldForTest(null);
      releaseBlockers();
      await Promise.all([...blockerPendings, stalePending]);

      expect(test.engine.ledger.snapshot()).toEqual(beforeStaleRelease);
      expect(await test.journal.readTrace(stale.traceId)).toEqual([]);
      expect(await test.journal.queryEvents({ types: ["ASTRA_DECISION", "RISK_EVALUATION", "PAPER_ORDER_CREATED"], experimentId: next.id })).toEqual([]);

      // The same P0 hedge in the current generation remains ordinary behavior.
      await test.engine.registerTriggerForResearch(sameRun);
      const sameRunEvents = await test.journal.readTrace(sameRun.traceId);
      expect(sameRunEvents.some((event) => event.type === "ASTRA_DECISION" && event.payload.bypass === "DETERMINISTIC_HEDGE")).toBe(true);
      expect(sameRunEvents.some((event) => event.type === "RISK_EVALUATION")).toBe(true);
    } finally {
      test.engine.setProcessTriggerHoldForTest(null);
      releaseReportLoad();
      releaseBlockers();
      await Promise.all(blockerPendings.map((pending) => pending.catch(() => undefined)));
      await stalePending?.catch(() => undefined);
      test.journal.loadExperimentReport = originalLoadReport;
      await test.engine.stop();
    }
  });
});
