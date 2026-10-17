import { describe, expect, it, beforeEach, afterEach } from "vitest";
import { engineEnvSchema, planDualMakerQuotes, type PaperOrder, type MarketSnapshot } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
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
const T = Date.parse("2026-10-15T18:00:00Z"), M = "touch-mkt", U = "touch-up", D = "touch-down";
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
    eventId: "touch-ref", traceId: "touch-ref", asset: "XRP", symbol: "XRPUSDT", venue: "test",
    price: "1", quantity: "1", sourceTimestamp: new Date(T).toISOString(), receivedAt: new Date(T).toISOString(), tradeId: "touch-ref",
  });
  const quote = (): PaperOrder => ({
    id: "touch-order", traceId: "touch-trace", triggerId: "touch-trigger", strategyId: "temporal-pair-hedge",
    marketId: M, tokenId: U, outcome: "UP", side: "BUY", orderType: "MAKER", requestedPrice: "0.69", quantity: "2",
    status: "PENDING", createdAt: new Date(T).toISOString(),
    entryAdmission: { fairProbability: 0.9, signalExpiresAt: new Date(T + 60000).toISOString() },
    makerFirst: { version: 1, role: "FIRST_LEG", postOnly: true, expiresAt: new Date(T + 2000).toISOString(), submissionLatencyMs: 150, tokenEpoch: 1, postingBookEventId: "touch-book", takerReferencePrice: "0.7" },
  });
  const intern = engine as unknown as {
    trySubmitDualMakerPair(o: PaperOrder, key: string, p: Record<"UP" | "DOWN", MarketSnapshot>): Promise<boolean>;
    restingOrders: Map<string, { order: PaperOrder }>;
  };
  return { engine, market, intern, quote, errors };
}
function plan(overrides: Record<string, unknown> = {}) {
  const up = snapshot({
    marketId: "tp", tokenId: "tp-up", outcome: "UP", asset: "ETH",
    bids: [{ price: "0.95", quantity: "100" }], asks: [{ price: "0.96", quantity: "100" }],
    sourceTimestamp: new Date(T).toISOString(), receivedAt: new Date(T).toISOString(),
  });
  const down = snapshot({
    marketId: "tp", tokenId: "tp-down", outcome: "DOWN", asset: "ETH",
    bids: [{ price: "0.02", quantity: "100" }], asks: [{ price: "0.04", quantity: "100" }],
    sourceTimestamp: new Date(T).toISOString(), receivedAt: new Date(T).toISOString(),
  });
  return planDualMakerQuotes({
    up, down, quantity: "5", perSideRiskCapUsd: "5", availableCashUsd: "1000",
    minPairNetRoi: 0.015, nowMs: T + 100, maxBookAgeMs: 1000, ...overrides,
  });
}

describe("dual maker touch fill-zone pricing (flag ON, cost-bound stays ON)", () => {
  it("is default OFF and parses true", () => {
    expect(engineEnvSchema.parse({}).PAPER_DUAL_MAKER_TOUCH_PRICING_ENABLED).toBe(false);
    expect(engineEnvSchema.parse({ PAPER_DUAL_MAKER_TOUCH_PRICING_ENABLED: "true" }).PAPER_DUAL_MAKER_TOUCH_PRICING_ENABLED).toBe(true);
  });

  it("planner prices at the best-bid JOIN (fill zone) and reports the single-fill tail as uncovered AUDIT", () => {
    // Classic cost-bound on this book shaves UP from 0.95 to 0.94 so the
    // single-fill tail stays covered (the A3 paper behavior that never fills).
    const classic = plan({ costBound: true });
    expect(classic.plan).not.toBeNull();
    expect(classic.plan!.up.price).toBe("0.94");
    expect(classic.plan!.worstCaseSingleFill!.up.covered).toBe(true);
    // Touch pricing keeps the quote AT the 0.95 bid; the single-fill tail is
    // then an uncovered AUDIT (hedging the residual UP at the deep DOWN ask is
    // too expensive), governed by reservation/risk limits + hedge machinery.
    const r = plan({ costBound: true, touchPricing: true });
    expect(r.reason).toBeNull();
    if (!r.plan) throw new Error("missing plan");
    expect(r.plan.up.price).toBe("0.95");
    expect(r.plan.down.price).toBe("0.02");
    expect(r.plan.worstCaseSingleFill).toBeDefined();
    expect(r.plan.worstCaseSingleFill!.up.covered).toBe(false);
    // dual-fill pair still locks a positive edge (spread 0.03, no maker fee on
    // the taker-only schedule) at the 1.5% pair floor
    expect(Number(r.plan.netPairPnlUsd)).toBeGreaterThan(0);
    expect(r.plan.netPairRoi).toBeGreaterThanOrEqual(0.015);
  });
it("does not deviate from classic when the single-fill tail at the join is already covered", () => {
    const up = snapshot({
      marketId: "tp", tokenId: "tp-up", outcome: "UP", asset: "ETH",
      bids: [{ price: "0.70", quantity: "100" }], asks: [{ price: "0.71", quantity: "100" }],
      sourceTimestamp: new Date(T).toISOString(), receivedAt: new Date(T).toISOString(),
    });
    const down = snapshot({
      marketId: "tp", tokenId: "tp-down", outcome: "DOWN", asset: "ETH",
      bids: [{ price: "0.20", quantity: "100" }], asks: [{ price: "0.21", quantity: "100" }],
      sourceTimestamp: new Date(T).toISOString(), receivedAt: new Date(T).toISOString(),
    });
    const args = { up, down, quantity: "2", perSideRiskCapUsd: "5", availableCashUsd: "1000", minPairNetRoi: 0.015, nowMs: T + 100, maxBookAgeMs: 1000 };
    const c = planDualMakerQuotes({ ...args, costBound: true });
    const t = planDualMakerQuotes({ ...args, costBound: true, touchPricing: true });
    // When the join-price tail is covered the classic cap does NOT shave; touch
    // pricing is the join too. The distinguishing case is test above.
    expect(c.reason).toBeNull();
    expect(t.reason).toBeNull();
    expect(c.plan!.up.price).toBe("0.7");
    expect(t.plan!.up.price).toBe("0.7");
    expect(t.plan!.worstCaseSingleFill!.up.covered).toBe(true);
  });

  it("engine quotes at the JOIN best bid with all three flags and preserves post-only", async () => {
    const s = await setup({ PAPER_DUAL_SIDED_MAKER_ENABLED: "true", PAPER_DUAL_MAKER_COST_BOUND_ENABLED: "true", PAPER_DUAL_MAKER_TOUCH_PRICING_ENABLED: "true" });
    try {
      const books = { UP: s.engine.state.marketSnapshots.get(U)!, DOWN: s.engine.state.marketSnapshots.get(D)! };
      expect(await s.intern.trySubmitDualMakerPair(s.quote(), "touch:plan", books)).toBe(true);
      expect(s.engine.restingOrderCount()).toBe(2);
      const q = [...s.intern.restingOrders.values()].map((v) => v.order);
      expect(q.find((o) => o.outcome === "UP")!.requestedPrice).toBe("0.69");
      expect(q.find((o) => o.outcome === "DOWN")!.requestedPrice).toBe("0.29");
      expect(q.find((o) => o.outcome === "UP")!.makerFirst?.postOnly).toBe(true);
      expect(q.find((o) => o.outcome === "DOWN")!.makerFirst?.postOnly).toBe(true);
      expect(new Set(q.map((o) => o.makerFirst?.dualGroupId)).size).toBe(1);
      expect(s.errors).toEqual([]);
    } finally { await s.engine.stop(); }
  });

  it("engine with cost-bound only keeps the below-bid quotes (A3 baseline preserved)", async () => {
    const s = await setup({ PAPER_DUAL_SIDED_MAKER_ENABLED: "true", PAPER_DUAL_MAKER_COST_BOUND_ENABLED: "true" });
    try {
      const books = { UP: s.engine.state.marketSnapshots.get(U)!, DOWN: s.engine.state.marketSnapshots.get(D)! };
      expect(await s.intern.trySubmitDualMakerPair(s.quote(), "cb:baseline", books)).toBe(true);
      const q = [...s.intern.restingOrders.values()].map((v) => v.order);
      expect(q.find((o) => o.outcome === "UP")!.requestedPrice).toBe("0.67");
      expect(q.find((o) => o.outcome === "DOWN")!.requestedPrice).toBe("0.27");
    } finally { await s.engine.stop(); }
  });
});