import { describe, expect, it, beforeEach, afterEach } from "vitest";
import { type PaperOrder, type MarketSnapshot } from "@pm/core";
import { MemoryEventJournal, type EventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { qualifiedPaperEnv, qualified, discoveryReport, ScriptedMarketAdapter, ScriptedReferenceRegistry } from "./paper-engine-testkit.js";
import { snapshot } from "./fixtures.js";
import { p0PostgresFixture } from "./p0-postgres-fixture.js";

let pg: Awaited<ReturnType<typeof p0PostgresFixture>> | undefined;
beforeEach(async () => {
  if (process.env.P0_EXECUTION_TEST_DATABASE_URL) pg = await p0PostgresFixture(process.env.P0_EXECUTION_TEST_DATABASE_URL);
});
afterEach(async () => {
  if (pg) { try { await pg.close(); } finally { pg = undefined; } }
});
const T = Date.parse("2026-10-15T18:00:00Z"), M = "cost-bound-mkt", U = "cost-bound-up", D = "cost-bound-down";

async function setup(overrides: Record<string, string> = {}) {
  const market = new ScriptedMarketAdapter();
  const listing = qualified(M, "XRP", U, D, new Date(T + 900000).toISOString());
  listing.raw = { eventStartTime: new Date(T - 1000).toISOString() };
  market.queue(discoveryReport([listing]));
  const errors: unknown[] = [];
  const engine = new EngineService(
    qualifiedPaperEnv({
      PAPER_MAKER_FIRST_ENABLED: "true", PAPER_EXECUTION_LATENCY_MS: "150", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
      DISCOVERY_INTERVAL_MS: "3600000", ...overrides,
    }),
    pg?.journal ?? new MemoryEventJournal(),
    { error: (e) => errors.push(e) },
    { market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge(), now: () => new Date(T), resolutionFetcher: async () => null },
  );
  await engine.start();
  await market.waitForHandlers();
  if (engine.experimentManifest()?.status === "IDLE") expect((await engine.beginExperiment()).ok).toBe(true);
  const b = (outcome: "UP" | "DOWN", seq = 1): MarketSnapshot => snapshot({
    marketId: M, tokenId: outcome === "UP" ? U : D, outcome, asset: "XRP",
    bids: [{ price: outcome === "UP" ? "0.69" : "0.29", quantity: "2" }],
    asks: [{ price: outcome === "UP" ? "0.7" : "0.3", quantity: "1000" }],
    receivedAt: new Date(T).toISOString(), sourceTimestamp: new Date(T).toISOString(), sequence: seq,
  });
  await market.emitSnapshot(b("UP"));
  await market.emitSnapshot(b("DOWN"));
  await engine.ingestReferenceTickForTest({
    eventId: "cb-ref", traceId: "cb-ref", asset: "XRP", symbol: "XRPUSDT", venue: "test",
    price: "1", quantity: "1", sourceTimestamp: new Date(T).toISOString(), receivedAt: new Date(T).toISOString(), tradeId: "cb-ref",
  });
  const quote = (): PaperOrder => ({
    id: "cb-order", traceId: "cb-trace", triggerId: "cb-trigger", strategyId: "temporal-pair-hedge",
    marketId: M, tokenId: U, outcome: "UP", side: "BUY", orderType: "MAKER", requestedPrice: "0.69", quantity: "2",
    status: "PENDING", createdAt: new Date(T).toISOString(),
    entryAdmission: { fairProbability: 0.9, signalExpiresAt: new Date(T + 60000).toISOString() },
    makerFirst: { version: 1, role: "FIRST_LEG", postOnly: true, expiresAt: new Date(T + 2000).toISOString(), submissionLatencyMs: 150, tokenEpoch: 1, postingBookEventId: "cb-book", takerReferencePrice: "0.7" },
  });
  const intern = engine as unknown as {
    trySubmitDualMakerPair(o: PaperOrder, key: string, p: Record<"UP" | "DOWN", MarketSnapshot>): Promise<boolean>;
    restingOrders: Map<string, { order: PaperOrder }>;
  };
  return { engine, market, intern, quote, errors };
}

describe("engine cost-bound dual maker quoting (flag ON)", () => {
  it("prices both legs at the cost-bound affordable passive quotes (below the best bid)", async () => {
    const s = await setup({ PAPER_DUAL_SIDED_MAKER_ENABLED: "true", PAPER_DUAL_MAKER_COST_BOUND_ENABLED: "true" });
    try {
      const books = { UP: s.engine.state.marketSnapshots.get(U)!, DOWN: s.engine.state.marketSnapshots.get(D)! };
      // 0.69 UP / 0.29 DOWN joins leave the single-fill residual hedge uncovered.
      // The cost-bound prices each leg at the maximum passive quote that still
      // covers that hedge (UP 0.67, DOWN 0.27) - lower, safe, post-only quotes
      // that the corrected post-only admission accepts.
      expect(await s.intern.trySubmitDualMakerPair(s.quote(), "cb:plan", books)).toBe(true);
      expect(s.engine.restingOrderCount()).toBe(2);
      const q = [...s.intern.restingOrders.values()].map((v) => v.order);
      const upQuote = q.find((o) => o.outcome === "UP")!;
      const downQuote = q.find((o) => o.outcome === "DOWN")!;
      expect(upQuote.requestedPrice).toBe("0.67");
      expect(downQuote.requestedPrice).toBe("0.27");
      expect(upQuote.makerFirst?.postOnly).toBe(true);
      expect(downQuote.makerFirst?.postOnly).toBe(true);
      expect(new Set(q.map((o) => o.makerFirst?.dualGroupId)).size).toBe(1);
      expect(s.errors).toEqual([]);
    } finally { await s.engine.stop(); }
  });
  it("is default OFF: without the flag the historical join-the-bid quotes are unchanged", async () => {
    const s = await setup({ PAPER_DUAL_SIDED_MAKER_ENABLED: "true" });
    try {
      const books = { UP: s.engine.state.marketSnapshots.get(U)!, DOWN: s.engine.state.marketSnapshots.get(D)! };
      expect(await s.intern.trySubmitDualMakerPair(s.quote(), "cb:plan-off", books)).toBe(true);
      const q = [...s.intern.restingOrders.values()].map((v) => v.order);
      expect(q.find((o) => o.outcome === "UP")!.requestedPrice).toBe("0.69");
      expect(q.find((o) => o.outcome === "DOWN")!.requestedPrice).toBe("0.29");
    } finally { await s.engine.stop(); }
  });
});

