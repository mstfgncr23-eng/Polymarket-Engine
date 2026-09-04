import { describe, expect, it } from "vitest";
import {
  AssetMarketIndex,
  ReferenceTickWindow,
  TriggerEngine,
  triggerConfigSchema,
  type ReferenceTick,
} from "@pm/core";
import { GrokActivationCoordinator } from "../apps/engine/src/activation/coordinator.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/noop.js";
import { RestingOrderTokenIndex } from "../apps/engine/src/resting-order-index.js";
import { createRestingMakerOrder } from "@pm/core";
import { order, snapshot } from "./fixtures.js";
import { startQualifiedEngine } from "./paper-engine-testkit.js";

function tick(i: number, start = Date.parse("2026-08-31T12:00:00.000Z"), dt = 10): ReferenceTick {
  const at = new Date(start + i * dt).toISOString();
  return {
    eventId: `r${i}`, traceId: "t", asset: "BTC", symbol: "BTCUSDT", venue: "binance",
    price: String(100 + i), quantity: "1", sourceTimestamp: at, receivedAt: at, tradeId: String(i),
  };
}

describe("Part 4 reference window O(1) deque", () => {
  it("keeps an exact 60s window without Array.shift and reports overflow at the finite cap", () => {
    const window = new ReferenceTickWindow(4, 60_000);
    const start = Date.parse("2026-08-31T12:00:00.000Z");
    expect(window.push(tick(0, start, 1_000)).accepted).toBe(true);
    expect(window.push(tick(10, start, 1_000)).accepted).toBe(true);
    expect(window.push(tick(20, start, 1_000)).accepted).toBe(true);
    expect(window.push(tick(30, start, 1_000)).accepted).toBe(true);
    const overflow = window.push(tick(40, start, 1_000));
    expect(overflow.accepted).toBe(false);
    expect(overflow.overflow).toBe(true);
    expect(window.size).toBe(4);
    expect(window.hasOverflowed).toBe(true);
    const later = new ReferenceTickWindow(4, 60_000);
    later.push(tick(0, start, 1_000));
    const expired = later.push(tick(70, start, 1_000));
    expect(expired.accepted).toBe(true);
    expect(later.size).toBe(1);
  });

  it("high tick-rate 60s ingest stays finite and surfaces overflow to TriggerEngine", () => {
    let overflow = 0;
    const observed = new TriggerEngine(
      triggerConfigSchema.parse({ referenceRapidMoveBps: 1_000_000 }),
      () => new Date(),
      {
        onCandidates: () => undefined,
        onEmitted: () => undefined,
        onSuppressed: () => undefined,
        onReferenceWindowOverflow: () => { overflow += 1; },
      },
    );
    const start = Date.parse("2026-08-31T12:00:00.000Z");
    const tight = new ReferenceTickWindow(8, 60_000);
    for (let i = 0; i < 20; i += 1) {
      const result = tight.push(tick(i, start, 10));
      if (result.overflow) overflow += 1;
    }
    expect(overflow).toBeGreaterThan(0);
    expect(tight.size).toBe(8);
    expect(observed.referenceWindowOverflowed("ETH")).toBe(false);
    for (let i = 0; i < 20; i += 1) observed.ingestReference(tick(i, start, 100));
    expect(observed.referenceWindowSize("BTC")).toBe(20);
  });
});

describe("Part 5 asset→market index", () => {
  it("is maintained on first snapshot and dropMarket/dropAsset", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 0, referenceRapidMoveBps: 10, btcRapidMoveBps: 10 }));
    engine.ingestMarket(snapshot({ marketId: "btc-mkt", tokenId: "up", outcome: "UP", asset: "BTC" }));
    engine.ingestMarket(snapshot({ marketId: "eth-mkt", tokenId: "eth-up", outcome: "UP", asset: "ETH", eventId: "e2", sequence: 2 }));
    expect(engine.hasManagedAsset("BTC")).toBe(true);
    expect(engine.marketsForAsset("BTC")).toEqual(["btc-mkt"]);
    engine.dropMarket("btc-mkt");
    expect(engine.hasManagedAsset("BTC")).toBe(false);
    engine.dropAsset("ETH");
    expect(engine.hasManagedAsset("ETH")).toBe(false);
    const index = new AssetMarketIndex();
    index.add("BTC", "m1");
    index.add("BTC", "m2");
    expect(index.entryCount()).toBe(2);
    index.removeMarket("m1");
    expect(index.marketIds("BTC")).toEqual(["m2"]);
  });
});

describe("Part 6 resting order token index", () => {
  it("indexes only the relevant token and cleans full fill / cancel", () => {
    const index = new RestingOrderTokenIndex();
    const up = createRestingMakerOrder(order("MAKER", { id: "o1", tokenId: "up" }), snapshot());
    const down = createRestingMakerOrder(order("MAKER", { id: "o2", tokenId: "down", marketId: "btc-15m" }), snapshot({ tokenId: "down" }));
    index.rebuild([up, down]);
    expect([...index.orderIdsFor("up")]).toEqual(["o1"]);
    expect([...index.orderIdsFor("other")]).toEqual([]);
    index.remove("o1");
    expect(index.entryCount()).toBe(1);
    index.rebuild([]);
    expect(index.entryCount()).toBe(0);
  });

  it("engine reserve rebuilds the derived index and cancel removes it", async () => {
    const { engine, market } = await startQualifiedEngine();
    const book = snapshot({
      marketId: "btc-mkt", tokenId: "btc-up",
      bids: [{ price: "0.47", quantity: "12" }], asks: [{ price: "0.48", quantity: "5" }],
    });
    await market.emitSnapshot(book);
    await engine.restMakerForTest(order("MAKER", { marketId: "btc-mkt", tokenId: "btc-up" }), book);
    expect(engine.restingOrderTokenIndexEntries()).toBe(1);
    const orderId = [...engine.restingOrdersForTest().keys()][0]!;
    await engine.cancelMakerForTest(orderId, "MARKET_REMOVED");
    expect(engine.restingOrderTokenIndexEntries()).toBe(0);
    await engine.stop();
  });
});

describe("Part 7 activation O(1) prune", () => {
  it("ensureLatency/snapshot/prune stay bounded at 500+ records without sorting the whole map each call", () => {
    const coordinator = new GrokActivationCoordinator(new NoopGrokActivationBridge(), {
      cooldownMs: 1, maxPerMinute: 10_000, suppressAtOrBelow: 0, memoryLimit: 500,
    }, () => new Date("2026-08-31T12:00:00.000Z"));
    const base = Date.parse("2026-08-31T12:00:00.000Z");
    for (let i = 0; i < 650; i += 1) {
      coordinator.ensureLatency({
        id: `trig_${i}`, traceId: "t", marketId: "m", asset: "BTC", source: "polymarket",
        kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: `trig_${i}`, evidence: {},
        sourceEventIds: ["e"], createdAt: new Date(base + i).toISOString(),
        expiresAt: "2026-08-31T13:00:00.000Z",
      });
    }
    expect(coordinator.footprint().latency).toBe(500);
    coordinator.restoreLatency([
      {
        triggerId: "old", triggerCreatedAt: "2026-08-31T11:00:00.000Z",
        activationRequestedAt: null, activationSentAt: null, firstMcpRequestAt: null, firstAcceptedSignalAt: null,
      },
      {
        triggerId: "new", triggerCreatedAt: "2026-08-31T12:00:00.000Z",
        activationRequestedAt: null, activationSentAt: null, firstMcpRequestAt: null, firstAcceptedSignalAt: null,
      },
    ]);
    expect(coordinator.snapshot().map((item) => item.triggerId)).toEqual(["old", "new"]);
  });
});
