import { describe, expect, it } from "vitest";
import {
  AssetMarketIndex,
  ReferenceTickWindow,
  TRIGGER_STABILITY_BOUNDS,
  TriggerEngine,
  triggerConfigSchema,
} from "@pm/core";
import { BoundedIngressQueue, MARKET_INGRESS_BOUNDS, TokenSyncIndex } from "@pm/market-data";
import { ACTIVATION_SCOPE_TTL_MS, GrokActivationCoordinator } from "../apps/engine/src/activation/coordinator.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/noop.js";
import { RestingOrderTokenIndex } from "../apps/engine/src/resting-order-index.js";
import { RUNTIME_BOUNDS } from "../apps/engine/src/runtime-supervisor.js";

describe("100k-event structure plateaus", () => {
  it("dedupe stays at the hard cap across 100k unique scopes", () => {
    let t = Date.parse("2026-08-31T12:00:00.000Z");
    const engine = new TriggerEngine(triggerConfigSchema.parse({
      cooldownMs: 0, imbalanceThreshold: 0.01,
    }), () => new Date(t));
    for (let i = 0; i < 100_000; i += 1) {
      t += 1;
      engine.ingestMarket({
        eventId: `e${i}`, traceId: "t", marketId: `m${i}`, tokenId: `tok${i}`,
        outcome: "UP", asset: "BTC",
        bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }],
        lastTradePrice: "0.48", tickSize: "0.01", minOrderSize: "1", feeSchedule: null,
        sourceTimestamp: new Date(t).toISOString(), receivedAt: new Date(t).toISOString(),
        sequence: i, hash: null,
      });
    }
    expect(engine.dedupeEntryCount()).toBeLessThanOrEqual(TRIGGER_STABILITY_BOUNDS.dedupeMaxEntries);
    expect(engine.dedupeEntryCount()).toBe(TRIGGER_STABILITY_BOUNDS.dedupeMaxEntries);
  });

  it("reference window never exceeds its finite ring", () => {
    const window = new ReferenceTickWindow();
    const start = Date.parse("2026-08-31T12:00:00.000Z");
    for (let i = 0; i < 100_000; i += 1) {
      window.push({
        eventId: `r${i}`, traceId: "t", asset: "BTC", symbol: "BTCUSDT", venue: "binance",
        price: "100", quantity: "1",
        sourceTimestamp: new Date(start + (i % 50_000)).toISOString(),
        receivedAt: new Date(start + (i % 50_000)).toISOString(),
        tradeId: String(i),
      });
    }
    expect(window.size).toBeLessThanOrEqual(TRIGGER_STABILITY_BOUNDS.referenceWindowMaxEntries);
  });

  it("ingress queue never exceeds count or byte bounds", () => {
    const queue = new BoundedIngressQueue();
    for (let i = 0; i < 100_000; i += 1) queue.tryEnqueue({ i, pad: "x".repeat(8) });
    expect(queue.depth).toBeLessThanOrEqual(MARKET_INGRESS_BOUNDS.maxEvents);
    expect(queue.queuedBytes).toBeLessThanOrEqual(MARKET_INGRESS_BOUNDS.maxBytes);
  });

  it("asset→market, token→resting, activation, and token sync stay finite", () => {
    const assets = new AssetMarketIndex();
    const resting = new RestingOrderTokenIndex();
    const sync = new TokenSyncIndex();
    for (let i = 0; i < 100_000; i += 1) {
      assets.add(`A${i % 40}`, `m${i % 200}`);
      resting.add(`o${i % 500}`, `tok${i % 80}`);
      sync.ensure(`tok${i % 80}`, 1);
    }
    expect(assets.entryCount()).toBeLessThanOrEqual(200);
    expect(resting.entryCount()).toBeLessThanOrEqual(500);
    expect(sync.entryCount()).toBeLessThanOrEqual(80);
    const coordinator = new GrokActivationCoordinator(new NoopGrokActivationBridge(), {
      cooldownMs: 1, maxPerMinute: 10_000, suppressAtOrBelow: 0, memoryLimit: 500,
    }, () => new Date("2026-08-31T12:00:00.000Z"));
    for (let i = 0; i < 100_000; i += 1) {
      coordinator.ensureLatency({
        id: `trig_${i}`, traceId: "t", marketId: "m", asset: "BTC", source: "system",
        kind: "SCHEDULED_REVIEW", severity: 1, score: 1, dedupeKey: `trig_${i}`, evidence: {},
        sourceEventIds: ["e"], createdAt: new Date(Date.parse("2026-08-31T12:00:00.000Z") + i).toISOString(),
        expiresAt: "2026-08-31T13:00:00.000Z",
      });
    }
    expect(coordinator.footprint().latency).toBeLessThanOrEqual(500);
  });

  it("activation TTL prune is not head-blocked by a refreshed scope (100k one-shots)", () => {
    let now = Date.parse("2026-08-31T12:00:00.000Z");
    const coordinator = new GrokActivationCoordinator(new NoopGrokActivationBridge(), {
      cooldownMs: 1, maxPerMinute: 10_000, suppressAtOrBelow: 0, memoryLimit: 500,
    }, () => new Date(now));
    coordinator.touchScopeForTest("keep", "BTC", "BOOK_IMBALANCE");
    for (let i = 0; i < 100_000; i += 1) {
      if (i % 64 === 0) coordinator.touchScopeForTest("keep", "BTC", "BOOK_IMBALANCE");
      coordinator.touchScopeForTest(`oneshot-${i}`, "BTC", "BOOK_IMBALANCE");
    }
    coordinator.touchScopeForTest("keep", "BTC", "BOOK_IMBALANCE");
    const beforeAdvance = coordinator.footprint();
    expect(beforeAdvance.lastByMarketAsset).toBeGreaterThan(1);
    now += ACTIVATION_SCOPE_TTL_MS + 1;
    coordinator.touchScopeForTest("keep", "BTC", "BOOK_IMBALANCE");
    const after = coordinator.footprint();
    expect(after.lastByMarketAsset).toBe(1);
    expect(after.sent).toBe(0);
    expect(after.latency).toBeLessThanOrEqual(500);
    now += ACTIVATION_SCOPE_TTL_MS + 1;
    coordinator.touchScopeForTest("keep", "BTC", "BOOK_IMBALANCE");
    expect(coordinator.footprint().lastByMarketAsset).toBe(1);
  });

  it("preserves PR #12 architectural bounds exactly", () => {
    expect(RUNTIME_BOUNDS.trigger.p0).toEqual({ concurrency: 4, queueLimit: 256, highWater: 192 });
    expect(RUNTIME_BOUNDS.trigger.p3).toEqual({ concurrency: 4, queueLimit: 1024, highWater: 768 });
    expect(RUNTIME_BOUNDS.journal.active).toBe(4);
    expect(RUNTIME_BOUNDS.journal.noncriticalActive).toBe(3);
    expect(RUNTIME_BOUNDS.journal.criticalWaiting).toBe(32);
    expect(RUNTIME_BOUNDS.journal.noncriticalWaiting).toBe(96);
    expect(RUNTIME_BOUNDS.criticalReserved).toEqual({ lane: "p0", queueLimit: 256, concurrency: 4 });
    expect(RUNTIME_BOUNDS.sse).toEqual({ byteThreshold: 262_144, persistentBackpressure: 8 });
  });
});
