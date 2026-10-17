import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { engineEnvSchema, type PaperOrder, type MarketSnapshot, type MarketTrade } from "@pm/core";
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
const T = Date.parse("2026-10-15T18:00:00Z"), M = "fz-mkt", U = "fz-up", D = "fz-down";
const FZ = { PAPER_DUAL_SIDED_MAKER_ENABLED: "true", PAPER_DUAL_MAKER_COST_BOUND_ENABLED: "true", PAPER_DUAL_MAKER_TOUCH_PRICING_ENABLED: "true", PAPER_DUAL_MAKER_FILL_ZONE_ADMISSION_ENABLED: "true" };

async function setup(overrides: Record<string, string> = {}, clock = { ms: T }) {
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
    { market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never, activation: new NoopGrokActivationBridge(), now: () => new Date(clock.ms), resolutionFetcher: async () => null },
  );
  await engine.start();
  await market.waitForHandlers();
  if (engine.experimentManifest()?.status === "IDLE") expect((await engine.beginExperiment()).ok).toBe(true);
  const b = (outcome: "UP" | "DOWN", seq = 1, upDepth = "2", downDepth = "2"): MarketSnapshot => snapshot({
    marketId: M, tokenId: outcome === "UP" ? U : D, outcome, asset: "XRP",
    bids: outcome === "UP"
      ? [{ price: "0.69", quantity: upDepth }, { price: "0.68", quantity: "0.4" }]
      : [{ price: "0.29", quantity: downDepth }, { price: "0.28", quantity: "0.4" }],
    asks: [{ price: outcome === "UP" ? "0.7" : "0.3", quantity: "1000" }],
    receivedAt: new Date(T).toISOString(), sourceTimestamp: new Date(T).toISOString(), sequence: seq,
  });
  await market.emitSnapshot(b("UP"));
  await market.emitSnapshot(b("DOWN"));
  await engine.ingestReferenceTickForTest({
    eventId: "fz-ref", traceId: "fz-ref", asset: "XRP", symbol: "XRPUSDT", venue: "test",
    price: "1", quantity: "1", sourceTimestamp: new Date(T).toISOString(), receivedAt: new Date(T).toISOString(), tradeId: "fz-ref",
  });
  const intern = engine as unknown as {
    trySubmitDualMakerPair(o: PaperOrder, key: string, p: Record<"UP" | "DOWN", MarketSnapshot>): Promise<boolean>;
    onMakerTrade(t: MarketTrade): Promise<void> | void;
    restingOrders: Map<string, { order: PaperOrder }>;
  };
  const quote = (): PaperOrder => ({
    id: "fz-order", traceId: "fz-trace", triggerId: "fz-trigger", strategyId: "temporal-pair-hedge",
    marketId: M, tokenId: U, outcome: "UP", side: "BUY", orderType: "MAKER", requestedPrice: "0.69", quantity: "2",
    status: "PENDING", createdAt: new Date(clock.ms).toISOString(),
    entryAdmission: { fairProbability: 0.9, signalExpiresAt: new Date(clock.ms + 60000).toISOString() },
    makerFirst: { version: 1, role: "FIRST_LEG", postOnly: true, expiresAt: new Date(clock.ms + 2000).toISOString(), submissionLatencyMs: 150, tokenEpoch: 1, postingBookEventId: "fz-book", takerReferencePrice: "0.7" },
  });
  const trade = (tokenId: string, outcome: "UP" | "DOWN", price: string, qty: string, n: number): MarketTrade => ({
    eventId: "trade-" + n, marketId: M, tokenId, outcome, price, quantity: qty, side: "SELL",
    sourceTimestamp: new Date(n).toISOString(), receivedAt: new Date(n).toISOString(),
    tokenEpoch: 1, transactionHash: "tx-" + n,
  });
  return { engine, market, clock, intern, b, quote, trade, errors };
}

describe("dual maker fill-zone admission (B3, opt-in)", () => {
  it("parses bounded fill-zone configuration and stays OFF by default", () => {
    const p = engineEnvSchema.parse({});
    expect(p.PAPER_DUAL_MAKER_FILL_ZONE_ADMISSION_ENABLED).toBe(false);
    expect(p.PAPER_DUAL_MAKER_FLOW_WINDOW_MS).toBe(30000);
    expect(p.PAPER_DUAL_MAKER_MIN_EXACT_FLOW_PER_WINDOW).toBe(2);
    expect(p.PAPER_DUAL_MAKER_MAX_QUOTE_TTL_MS).toBe(5000);
    const on = engineEnvSchema.parse({
      PAPER_DUAL_MAKER_FILL_ZONE_ADMISSION_ENABLED: "true",
      PAPER_DUAL_MAKER_FLOW_WINDOW_MS: "15000", PAPER_DUAL_MAKER_MIN_EXACT_FLOW_PER_WINDOW: "3", PAPER_DUAL_MAKER_MAX_QUOTE_TTL_MS: "3000",
    });
    expect(on.PAPER_DUAL_MAKER_FILL_ZONE_ADMISSION_ENABLED).toBe(true);
    expect(on.PAPER_DUAL_MAKER_FLOW_WINDOW_MS).toBe(15000);
    expect(on.PAPER_DUAL_MAKER_MIN_EXACT_FLOW_PER_WINDOW).toBe(3);
    expect(on.PAPER_DUAL_MAKER_MAX_QUOTE_TTL_MS).toBe(3000);
    expect(engineEnvSchema.safeParse({ PAPER_DUAL_MAKER_FLOW_WINDOW_MS: "1000" }).success).toBe(false);
    expect(engineEnvSchema.safeParse({ PAPER_DUAL_MAKER_MAX_QUOTE_TTL_MS: "6000" }).success).toBe(false);
  });

  it("keeps classic touch admission with base TTL when fill-zone is OFF", async () => {
    const s = await setup({ PAPER_DUAL_SIDED_MAKER_ENABLED: "true", PAPER_DUAL_MAKER_COST_BOUND_ENABLED: "true", PAPER_DUAL_MAKER_TOUCH_PRICING_ENABLED: "true" });
    try {
      const books = { UP: s.engine.state.marketSnapshots.get(U)!, DOWN: s.engine.state.marketSnapshots.get(D)! };
      expect(await s.intern.trySubmitDualMakerPair(s.quote(), "fz:off", books)).toBe(true);
      expect(s.engine.restingOrderCount()).toBe(2);
      const q = [...s.intern.restingOrders.values()].map((v) => v.order);
      expect(q.every((o) => Date.parse(o.makerFirst!.expiresAt) - Date.parse(o.createdAt) === 2000)).toBe(true);
    } finally { await s.engine.stop(); }
  });

  it("rejects the plan when no exact-price SELL flow at the touch exists in the window", async () => {
    const s = await setup(FZ);
    try {
      const books = { UP: s.engine.state.marketSnapshots.get(U)!, DOWN: s.engine.state.marketSnapshots.get(D)! };
      s.clock.ms = T + 3000;
      expect(await s.intern.trySubmitDualMakerPair(s.quote(), "fz:none", books)).toBe(false);
      expect(s.engine.restingOrderCount()).toBe(0);
    } finally { await s.engine.stop(); }
  });

  it("rejects when under MIN exact flow (single fluke trade)", async () => {
    const s = await setup(FZ);
    try {
      await s.intern.onMakerTrade(s.trade(U, "UP", "0.69", "3", T + 1000));
      await s.intern.onMakerTrade(s.trade(D, "DOWN", "0.29", "3", T + 2000));
      s.clock.ms = T + 3000;
      const books = { UP: s.engine.state.marketSnapshots.get(U)!, DOWN: s.engine.state.marketSnapshots.get(D)! };
      expect(await s.intern.trySubmitDualMakerPair(s.quote(), "fz:fluke", books)).toBe(false);
      expect(s.engine.restingOrderCount()).toBe(0);
    } finally { await s.engine.stop(); }
  });

  it("accepts flowing windows and scales the resting window by depth/flow within the bound", async () => {
    const s = await setup(FZ);
    try {
      // UP touch 0.69, DOWN touch 0.29. Seed 15 units of exact-price SELL flow per leg
      // across 30s -> 0.5 units/s; displayed depth 2 -> consumeMs = 2/0.5*1000 = 4000.
      for (const n of [0, 1, 2, 3, 4]) {
        await s.intern.onMakerTrade(s.trade(U, "UP", "0.69", "3", T + 1000 + n));
        await s.intern.onMakerTrade(s.trade(D, "DOWN", "0.29", "3", T + 1000 + n));
      }
      s.clock.ms = T + 6000;
      const books = { UP: s.engine.state.marketSnapshots.get(U)!, DOWN: s.engine.state.marketSnapshots.get(D)! };
      expect(await s.intern.trySubmitDualMakerPair(s.quote(), "fz:flow", books)).toBe(true);
      expect(s.engine.restingOrderCount()).toBe(2);
      const q = [...s.intern.restingOrders.values()].map((v) => v.order);
      expect(new Set(q.map((o) => o.makerFirst!.dualGroupId)).size).toBe(1);
      expect(q.find((o) => o.outcome === "UP")!.requestedPrice).toBe("0.69");
      expect(q.find((o) => o.outcome === "DOWN")!.requestedPrice).toBe("0.29");
      const ttls = q.map((o) => Date.parse(o.makerFirst!.expiresAt) - Date.parse(o.createdAt));
      expect(ttls.every((d) => d >= 4000 && d <= 5000)).toBe(true);
    } finally { await s.engine.stop(); }
  });

  it("rejects deep-queue windows whose displayed depth cannot be consumed inside the bound", async () => {
    const s = await setup(FZ);
    try {
      for (const n of [0, 1, 2, 3, 4]) {
        await s.intern.onMakerTrade(s.trade(U, "UP", "0.69", "3", T + 1000 + n));
        await s.intern.onMakerTrade(s.trade(D, "DOWN", "0.29", "3", T + 1000 + n));
      }
      s.clock.ms = T + 6000;
      const deep = { UP: s.b("UP", 2, "500", "2"), DOWN: s.b("DOWN", 2, "2", "500") };
      s.engine.state.marketSnapshots.set(U, deep.UP);
      s.engine.state.marketSnapshots.set(D, deep.DOWN);
      expect(await s.intern.trySubmitDualMakerPair(s.quote(), "fz:deep", deep as Record<"UP" | "DOWN", MarketSnapshot>)).toBe(false);
      expect(s.engine.restingOrderCount()).toBe(0);
    } finally { await s.engine.stop(); }
  });

  it("admitted fill-zone quote still fills from realized exact-price SELL flow", async () => {
    const s = await setup(FZ);
    try {
      for (const n of [0, 1, 2, 3, 4]) {
        await s.intern.onMakerTrade(s.trade(U, "UP", "0.69", "3", T + 1000 + n));
        await s.intern.onMakerTrade(s.trade(D, "DOWN", "0.29", "3", T + 1000 + n));
      }
      s.clock.ms = T + 6000;
      const books = { UP: s.engine.state.marketSnapshots.get(U)!, DOWN: s.engine.state.marketSnapshots.get(D)! };
      expect(await s.intern.trySubmitDualMakerPair(s.quote(), "fz:fill", books)).toBe(true);
      s.clock.ms = T + 6400; // past activation (6000 + 150)
      await s.intern.onMakerTrade(s.trade(U, "UP", "0.69", "3", T + 6200));
      expect(s.engine.ledger.positions.get(M)?.unpairedQuantity ?? "0").toBe("0.5");
    } finally { await s.engine.stop(); }
  });
});