import { experimentAllowsFirstLeg, experimentAllowsHedge } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
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
import { fill } from "./fixtures.js";

const START_MS = Date.parse("2026-09-01T00:00:00.000Z");

function paperFill(marketId: string, outcome: "UP" | "DOWN", id: string, nowMs: number) {
  return {
    ...fill(outcome, outcome === "UP" ? "0.40" : "0.48", "10"),
    id,
    orderId: `${id}-order`,
    marketId,
    tokenId: `${marketId}-${outcome.toLowerCase()}`,
    timestamp: new Date(nowMs).toISOString(),
  };
}

async function setup(input: {
  journal?: MemoryEventJournal;
  nowMs?: number;
  endDate?: string;
  resolutions?: Map<string, Record<string, unknown>>;
} = {}) {
  const clock = { nowMs: input.nowMs ?? START_MS };
  const endDate = input.endDate ?? new Date(clock.nowMs + 60_000).toISOString();
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down", endDate)]));
  const journal = input.journal ?? new MemoryEventJournal();
  const resolutions = input.resolutions ?? new Map<string, Record<string, unknown>>();
  const env = qualifiedPaperEnv({
    EXPERIMENT_DURATION_HOURS: "48",
    EXPERIMENT_CLOSE_POLICY: "observe_until_settlement",
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    DISCOVERY_INTERVAL_MS: "3600000",
  });
  const engine = new EngineService(env, journal, { error: () => undefined }, {
    market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(),
    now: () => new Date(clock.nowMs),
    resolutionFetcher: async (id) => resolutions.get(id) ?? null,
  });
  await engine.start();
  return { clock, endDate, engine, env, journal, market, resolutions };
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
    closeExperimentEntries: () => engine.closeExperimentEntries(),
    operatorToken: "operator-token",
    nodeEnv: "production",
  });
}

describe("operator close-entries lifecycle action", () => {
  it("requires the existing operator token and rejects without a current experiment", async () => {
    const test = await setup();
    const server = await operatorServer(test.engine, test.journal);
    try {
      const unauthorized = await server.inject({
        method: "POST",
        url: "/api/operator/experiment/close-entries",
        headers: { authorization: "Bearer read-token" },
      });
      expect(unauthorized.statusCode).toBe(401);

      const noCurrentExperiment = await server.inject({
        method: "POST",
        url: "/api/operator/experiment/close-entries",
        headers: { authorization: "Bearer operator-token" },
      });
      expect(noCurrentExperiment.statusCode).toBe(409);
      expect(noCurrentExperiment.json()).toMatchObject({ ok: false, blockers: ["NO_CURRENT_EXPERIMENT"] });
    } finally {
      await server.close();
      await test.engine.stop();
    }
  });

  it("durably closes RUNNING entries once, preserves the manifest, and is idempotent", async () => {
    const test = await setup();
    const server = await operatorServer(test.engine, test.journal);
    try {
      expect((await test.engine.beginExperiment()).ok).toBe(true);
      const before = test.engine.experimentManifest()!;
      test.clock.nowMs += 5_000;
      const expectedEntryClosedAt = new Date(test.clock.nowMs).toISOString();

      const first = await server.inject({
        method: "POST",
        url: "/api/operator/experiment/close-entries",
        headers: { authorization: "Bearer operator-token" },
      });
      expect(first.statusCode).toBe(200);
      expect(first.json()).toMatchObject({ ok: true, experiment: { status: "ENTRY_CLOSED" } });

      const closed = test.engine.experimentManifest()!;
      expect(closed).toMatchObject({
        id: before.id,
        durationHours: before.durationHours,
        strategyVersion: before.strategyVersion,
        riskConfiguration: before.riskConfiguration,
        modelConfiguration: before.modelConfiguration,
        startingBalance: before.startingBalance,
        systemVersion: before.systemVersion,
        gitSha: before.gitSha,
        closePolicy: before.closePolicy,
        startedAt: before.startedAt,
        entryClosesAt: expectedEntryClosedAt,
        completedAt: null,
      });
      expect(closed.markToMarketAt).toBeNull();
      expect((await test.journal.queryDurableTransitions({ types: ["EXPERIMENT_ENTRY_CLOSED"], limit: 8 }))).toHaveLength(1);
      expect((await test.journal.queryEvents({ types: ["EXPERIMENT_REPORT_GENERATED"] }))).toHaveLength(0);

      const repeated = await server.inject({
        method: "POST",
        url: "/api/operator/experiment/close-entries",
        headers: { authorization: "Bearer operator-token" },
      });
      expect(repeated.statusCode).toBe(200);
      expect(repeated.json()).toMatchObject({ ok: true, experiment: { id: before.id, status: "ENTRY_CLOSED", entryClosesAt: expectedEntryClosedAt } });
      expect((await test.journal.queryDurableTransitions({ types: ["EXPERIMENT_ENTRY_CLOSED"], limit: 8 }))).toHaveLength(1);
      expect(test.engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
    } finally {
      await server.close();
      await test.engine.stop();
    }
  });

  it("keeps first-leg admission closed while allowing an existing hedge", async () => {
    const test = await setup();
    try {
      expect((await test.engine.beginExperiment()).ok).toBe(true);
      await test.engine.applyObservedFill(paperFill("btc-mkt", "UP", "before-close", test.clock.nowMs));
      await test.engine.closeExperimentEntries();
      const closed = test.engine.experimentManifest()!;
      expect(experimentAllowsFirstLeg(closed)).toBe(false);
      expect(experimentAllowsHedge(closed)).toBe(true);

      await test.engine.applyObservedFill(paperFill("btc-mkt", "DOWN", "after-close-hedge", test.clock.nowMs));
      expect((await test.journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 8 }))).toHaveLength(2);
      expect(test.engine.ledger.positions.get("btc-mkt")?.unpairedQuantity).toBe("0");
    } finally {
      await test.engine.stop();
    }
  });

  it("settles remaining positions before completing and generating the final report", async () => {
    const resolutions = new Map<string, Record<string, unknown>>();
    const endDate = new Date(START_MS + 10_000).toISOString();
    const test = await setup({ endDate, resolutions });
    try {
      expect((await test.engine.beginExperiment()).ok).toBe(true);
      await test.engine.applyObservedFill(paperFill("btc-mkt", "UP", "report-first-leg", test.clock.nowMs));
      test.clock.nowMs = START_MS + 1_000;
      await test.engine.closeExperimentEntries();
      expect(test.engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
      expect(await test.journal.loadExperimentReport(test.engine.experimentManifest()!.id)).toBeNull();
      expect((await test.journal.queryEvents({ types: ["EXPERIMENT_REPORT_GENERATED"] }))).toHaveLength(0);

      resolutions.set("btc-mkt", {
        state: { closed: true, endDate },
        resolution: { umaResolutionStatus: "settled" },
        outcomes: { yes: { label: "Up", price: "1" }, no: { label: "Down", price: "0" } },
      });
      test.clock.nowMs = START_MS + 11_000;
      await test.engine.tickMaintenance();
      await test.engine.tickMaintenance();

      expect(test.engine.experimentManifest()?.status).toBe("COMPLETED");
      expect((await test.journal.queryDurableTransitions({ types: ["EXPERIMENT_ENTRY_CLOSED"], limit: 8 }))).toHaveLength(1);
      expect((await test.journal.queryDurableTransitions({ types: ["EXPERIMENT_COMPLETED"], limit: 8 }))).toHaveLength(1);
      expect((await test.journal.queryEvents({ types: ["EXPERIMENT_REPORT_GENERATED"] }))).toHaveLength(1);
      expect(await test.journal.loadExperimentReport(test.engine.experimentManifest()!.id)).toMatchObject({ complete: true, truncated: false });
    } finally {
      await test.engine.stop();
    }
  });

  it("preserves ENTRY_CLOSED through restart and recovery", async () => {
    const journal = new MemoryEventJournal();
    const first = await setup({ journal });
    let recovered: EngineService | null = null;
    try {
      expect((await first.engine.beginExperiment()).ok).toBe(true);
      await first.engine.applyObservedFill(paperFill("btc-mkt", "UP", "recovery-first-leg", first.clock.nowMs));
      first.clock.nowMs += 5_000;
      await first.engine.closeExperimentEntries();
      const closed = first.engine.experimentManifest()!;
      await first.engine.stop();

      recovered = new EngineService(first.env, journal, { error: () => undefined }, {
        market: first.market as never,
        referenceRegistry: new ScriptedReferenceRegistry() as never,
        activation: new NoopGrokActivationBridge(),
        now: () => new Date(first.clock.nowMs),
      });
      await recovered.start();
      expect(recovered.experimentManifest()).toMatchObject({
        id: closed.id,
        status: "ENTRY_CLOSED",
        startedAt: closed.startedAt,
        entryClosesAt: closed.entryClosesAt,
      });
      await recovered.closeExperimentEntries();
      expect((await journal.queryDurableTransitions({ types: ["EXPERIMENT_ENTRY_CLOSED"], limit: 8 }))).toHaveLength(1);
    } finally {
      if (recovered) await recovered.stop();
      else if (first.engine.lastShutdownResult() === null) await first.engine.stop();
    }
  });
});
