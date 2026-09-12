import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  createId,
  type MarketSnapshot,
  type ReferenceTick,
} from "@pm/core";
import { hashExperimentReport, PostgresEventJournal, type EventJournal } from "@pm/db";
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

const databaseUrl = process.env.STEP4_TEST_DATABASE_URL;
const migrations = fileURLToPath(new URL("../packages/db/migrations", import.meta.url));
const START_MS = Date.parse("2026-01-01T00:00:00.000Z");

type ScenarioClock = { nowMs: number };
type PgScenario = {
  engine: EngineService;
  journal: EventJournal;
  market: ScriptedMarketAdapter;
  clock: ScenarioClock;
  resolutions: Map<string, Record<string, unknown>>;
  marketId: string;
};

function localPgUrl(): string {
  if (!databaseUrl) throw new Error("STEP4_TEST_DATABASE_URL_REQUIRED");
  const parsed = new URL(databaseUrl);
  // This test is intentionally limited to the disposable CI service. It must
  // never be pointed at a production Supabase endpoint by accident.
  expect(["localhost", "127.0.0.1"]).toContain(parsed.hostname);
  expect(parsed.pathname).toBe("/step4_test");
  return databaseUrl;
}

async function withIsolatedPgDatabase<T>(baseUrl: string, run: (database: string) => Promise<T>): Promise<T> {
  const dbName = `step4_pr20_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const adminUrl = new URL(baseUrl);
  adminUrl.pathname = "/postgres";
  const adminJournal = new PostgresEventJournal(adminUrl.toString(), migrations);
  const adminClient = Reflect.get(adminJournal, "client") as { unsafe(sql: string): Promise<unknown> };
  try {
    // The source URL is the disposable localhost PG17 service. A private
    // database keeps this lifecycle recovery test independent from the other
    // PG17 tests that share that service during the full CI run.
    await adminClient.unsafe(`CREATE DATABASE "${dbName}"`);
    const isolatedUrl = new URL(baseUrl);
    isolatedUrl.pathname = `/${dbName}`;
    return await run(isolatedUrl.toString());
  } finally {
    await adminClient.unsafe(`DROP DATABASE IF EXISTS "${dbName}" WITH (FORCE)`).catch(() => undefined);
    await adminJournal.close();
  }
}

async function drainRuntime(engine: EngineService): Promise<void> {
  const started = Date.now();
  while (engine.runtimeSupervisorSnapshot().unresolved !== 0) {
    if (Date.now() - started > 10_000) throw new Error("runtime drain timeout");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

async function waitFor<T>(read: () => T | Promise<T>, predicate: (value: T) => boolean, timeoutMs = 2_000): Promise<T> {
  const started = Date.now();
  for (;;) {
    const value = await read();
    if (predicate(value)) return value;
    if (Date.now() - started > timeoutMs) throw new Error("test wait timeout");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

function iso(clock: ScenarioClock): string { return new Date(clock.nowMs).toISOString(); }

function marketBook(clock: ScenarioClock, marketId: string, outcome: "UP" | "DOWN", sequence: number): MarketSnapshot {
  return snapshot({
    eventId: createId("market"),
    traceId: createId("trace"),
    marketId,
    tokenId: outcome === "UP" ? `${marketId}-up` : `${marketId}-down`,
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
    venue: "deterministic-pg17-test",
    price,
    quantity: "1",
    sourceTimestamp: iso(clock),
    receivedAt: iso(clock),
    tradeId: createId("trade"),
  };
}

async function setupPgScenario(journal: EventJournal, clock: ScenarioClock, resolutions: Map<string, Record<string, unknown>>, marketId: string): Promise<PgScenario> {
  const market = new ScriptedMarketAdapter();
  const endDate = new Date(START_MS + 10_000).toISOString();
  market.queue(discoveryReport([qualified(marketId, "BTC", `${marketId}-up`, `${marketId}-down`, endDate)]));
  const env = qualifiedPaperEnv({
    EXPERIMENT_DURATION_HOURS: "0.001",
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    PAPER_EXECUTION_STYLE: "TAKER",
    PAPER_EXECUTION_LATENCY_MS: "200",
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
  return { engine, journal, market, clock, resolutions, marketId };
}

function payloadObject(row: { payload: Record<string, unknown> }, key: string): Record<string, unknown> {
  const value = row.payload[key];
  return value && typeof value === "object" ? value as Record<string, unknown> : {};
}

describe("PR20 true product end-to-end gate on PostgreSQL 17", () => {
  it.skipIf(!databaseUrl)("persists fill, crash recovery, settlement, and canonical report exactly once", { timeout: 120_000 }, async () => {
    const baseUrl = localPgUrl();
    await withIsolatedPgDatabase(baseUrl, async (url) => {
      const clock = { nowMs: START_MS };
      const resolutions = new Map<string, Record<string, unknown>>();
      const marketId = `pg17-btc-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
      const journal = new PostgresEventJournal(url, migrations);
      const setup = await setupPgScenario(journal, clock, resolutions, marketId);
      let experimentId = "";
      let fillId = "";
      try {
      expect((await setup.engine.beginExperiment()).ok).toBe(true);
      await setup.market.emitSnapshot(marketBook(clock, marketId, "UP", 1));
      await setup.market.emitSnapshot(marketBook(clock, marketId, "DOWN", 1));
      await setup.engine.ingestReferenceTickForTest(referenceTick(clock, "100"));
      experimentId = setup.engine.experimentManifest()!.id;
      const startRows = await journal.queryDurableTransitions({ types: ["EXPERIMENT_STARTED"], limit: 20_000 });
      expect(startRows.filter((row) => row.aggregateId === experimentId)).toHaveLength(1);

      setup.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY");
      clock.nowMs = START_MS + 1_000;
      await setup.engine.ingestReferenceTickForTest(referenceTick(clock, "102"));
      await waitFor(() => journal.queryEvents({ types: ["PAPER_ORDER_CREATED"], limit: 20_000 }), (events) => events.some((event) => event.payload && typeof event.payload === "object" && event.payload.marketId === marketId));
      await waitFor(() => journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 20_000 }), (rows) => rows.length === 1);
      await drainRuntime(setup.engine);
      expect(setup.engine.state.latestConsensus?.authority).toBe("NATIVE_DETERMINISTIC_V1");

      const durableBeforeRestart = await journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 20_000 });
      const ownDurableFill = durableBeforeRestart.filter((row) => payloadObject(row, "fill").marketId === marketId);
      expect(ownDurableFill).toHaveLength(1);
      fillId = String(payloadObject(ownDurableFill[0]!, "fill").id);
      const fillEventsBeforeRestart = await journal.queryEvents({ types: ["PAPER_FILL"], limit: 20_000 });
      expect(fillEventsBeforeRestart.some((event) => event.aggregateId === fillId)).toBe(false);
      const settlementDurableBeforeRestart = await journal.queryDurableTransitions({ types: ["POSITION_SETTLED"], limit: 20_000 });
      expect(settlementDurableBeforeRestart.some((row) => row.aggregateId.includes(marketId))).toBe(false);
      await setup.engine.stop();
    } catch (error) {
      if (!setup.engine.state.draining) await setup.engine.stop();
      throw error;
    }

    const restartedJournal = new PostgresEventJournal(url, migrations);
    const recovered = await setupPgScenario(restartedJournal, clock, resolutions, marketId);
    try {
      const recoveredPosition = recovered.engine.ledger.positions.get(marketId);
      expect(recoveredPosition?.unpairedQuantity).not.toBe("0");
      const replayedEvents = await restartedJournal.queryEvents({ types: ["PAPER_FILL"], limit: 20_000 });
      expect(replayedEvents.filter((event) => event.aggregateId === fillId)).toHaveLength(1);

      const firstPositionId = recoveredPosition!.id;
      clock.nowMs = START_MS + 20_000;
      resolutions.set(marketId, {
        state: { closed: true },
        resolution: { umaResolutionStatus: "settled" },
        outcomes: { yes: { label: "Up", price: "1" }, no: { label: "Down", price: "0" } },
      });
      await recovered.engine.tickMaintenance();
      await recovered.engine.tickMaintenance();
      await recovered.engine.stop();

      const finalJournal = new PostgresEventJournal(url, migrations);
      await finalJournal.initialize();
      try {
        const fillEvents = (await finalJournal.queryEvents({ types: ["PAPER_FILL"], limit: 20_000 })).filter((event) => event.aggregateId === fillId);
        const settlementEvents = (await finalJournal.queryEvents({ types: ["POSITION_SETTLED"], limit: 20_000 })).filter((event) => event.aggregateId === firstPositionId);
        expect(fillEvents).toHaveLength(1);
        expect(settlementEvents).toHaveLength(1);
        const consensusEvents = await finalJournal.queryEvents({ types: ["CONSENSUS_DECISION"], limit: 20_000 });
        expect(consensusEvents.some((event) => event.payload && typeof event.payload === "object" && event.payload.authority === "NATIVE_DETERMINISTIC_V1")).toBe(true);
        expect((await finalJournal.queryEvents({ types: ["GROK_SIGNAL_ACCEPTED"], limit: 20_000 }))).toEqual([]);

        const durable = await finalJournal.queryDurableTransitions({ types: ["PAPER_FILL", "POSITION_SETTLED"], limit: 20_000 });
        expect(durable.filter((row) => row.type === "PAPER_FILL" && payloadObject(row, "fill").id === fillId)).toHaveLength(1);
        expect(durable.filter((row) => row.type === "POSITION_SETTLED" && row.aggregateId === firstPositionId)).toHaveLength(1);

        const finalExperiment = (await finalJournal.queryDurableTransitions({ types: ["EXPERIMENT_COMPLETED"], limit: 20_000 }))
          .find((row) => row.aggregateId === experimentId);
        expect(finalExperiment).toBeTruthy();
        const report = await finalJournal.loadExperimentReport(experimentId);
        expect(report).toMatchObject({ complete: true, truncated: false });
        expect(hashExperimentReport(report!)).toBe(report!.contentHash);

        const beforeRetry = await finalJournal.queryDurableTransitions({ types: ["PAPER_FILL", "POSITION_SETTLED"], limit: 20_000 });
        const retryJournal = new PostgresEventJournal(url, migrations);
        const retry = await setupPgScenario(retryJournal, clock, resolutions, marketId);
        try {
          expect(retry.engine.ledger.positions.has(marketId)).toBe(false);
          await retry.engine.tickMaintenance();
        } finally {
          await retry.engine.stop();
        }
        const afterRetry = await finalJournal.queryDurableTransitions({ types: ["PAPER_FILL", "POSITION_SETTLED"], limit: 20_000 });
        expect(afterRetry.filter((row) => row.type === "PAPER_FILL" && payloadObject(row, "fill").id === fillId)).toHaveLength(1);
        expect(afterRetry.filter((row) => row.type === "POSITION_SETTLED" && row.aggregateId === firstPositionId)).toHaveLength(1);
        expect(afterRetry.length).toBe(beforeRetry.length);
      } finally {
        await finalJournal.close();
      }
      } finally {
        if (!recovered.engine.state.draining) await recovered.engine.stop();
        await restartedJournal.close().catch(() => undefined);
      }
    });
  });
});
