import { describe, expect, it } from "vitest";
import {
  compactBookSidesForDerivation,
  deriveBook,
  TriggerEngine,
  triggerConfigSchema,
  type BookDerived,
  type MarketSnapshot,
  type TriggerEvent,
} from "@pm/core";
import { snapshot as fixtureSnapshot } from "./fixtures.js";
import { MarketDerivationWorkerService as EngineWorker } from "../apps/engine/src/market-derivation-service.js";

function book(overrides: Partial<MarketSnapshot> = {}): MarketSnapshot {
  return fixtureSnapshot(overrides);
}

function config(overrides: Record<string, unknown> = {}) {
  return triggerConfigSchema.parse({
    cooldownMs: 0,
    imbalanceThreshold: 0.5,
    spreadChangeThreshold: 0.01,
    depthChangeThreshold: 0.2,
    pairCostThreshold: 0.985,
    expiryWarningMs: 3_600_000,
    ...overrides,
  });
}

function comparable(event: TriggerEvent) {
  return {
    kind: event.kind,
    severity: event.severity,
    score: event.score,
    source: event.source,
    evidence: event.evidence,
    sourceEventIds: event.sourceEventIds,
    marketId: event.marketId,
    asset: event.asset,
  };
}

async function workerDerived(service: EngineWorker, snapshot: MarketSnapshot): Promise<BookDerived> {
  const result = await service.deriveSnapshot(snapshot);
  expect(result.status).toBe("success");
  expect(result.derived).toEqual(deriveBook(snapshot));
  return result.derived;
}

describe("PR52 trigger replay equivalence", () => {
  it("sync ingestMarket equals worker ingestMarketWithDerived for a deterministic stream", async () => {
    const service = new EngineWorker({ timeoutMs: 2_000 });
    expect(await service.start()).toBe(true);
    try {
      const now = () => new Date("2026-08-31T12:00:00.000Z");
      const syncEngine = new TriggerEngine(config(), now);
      const workerEngine = new TriggerEngine(config(), now);
      const stream: MarketSnapshot[] = [
        book({ bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }] }),
        book({
          eventId: "e2", sequence: 2,
          bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.48", quantity: "10" }],
        }),
        book({
          eventId: "e3", sequence: 3, tokenId: "down", outcome: "DOWN",
          bids: [{ price: "0.51", quantity: "20" }], asks: [{ price: "0.52", quantity: "8" }],
        }),
        book({
          eventId: "e4", sequence: 4, tokenId: "down", outcome: "DOWN",
          bids: [{ price: "0.10", quantity: "5" }], asks: [{ price: "0.90", quantity: "5" }],
        }),
        book({
          eventId: "e5", sequence: 5,
          bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }],
        }),
      ];
      const syncEvents = stream.flatMap((item) => syncEngine.ingestMarket(item));
      const workerEvents: TriggerEvent[] = [];
      for (const item of stream) {
        workerEvents.push(...workerEngine.ingestMarketWithDerived(item, await workerDerived(service, item)));
      }
      expect(workerEvents.map(comparable)).toEqual(syncEvents.map(comparable));
      expect(workerEvents.map((item) => item.kind)).toEqual(syncEvents.map((item) => item.kind));
    } finally {
      await service.stop();
    }
  });

  it("covers spread, depth, imbalance, pair cost, near expiry, repeats, and UP/DOWN alternation", async () => {
    const service = new EngineWorker({ timeoutMs: 2_000 });
    expect(await service.start()).toBe(true);
    try {
      let t = Date.parse("2026-08-31T12:00:00.000Z");
      const now = () => new Date(t);
      const shared = config({ cooldownMs: 0, expiryWarningMs: 120_000, pairCostThreshold: 0.99 });
      const syncEngine = new TriggerEngine(shared, now);
      const workerEngine = new TriggerEngine(shared, now);
      syncEngine.noteMarketWindow("btc-15m", "BTC", new Date(t + 60_000).toISOString());
      workerEngine.noteMarketWindow("btc-15m", "BTC", new Date(t + 60_000).toISOString());

      const stream: MarketSnapshot[] = [
        book({
          bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }],
        }),
        book({
          eventId: "spread", sequence: 2,
          bids: [{ price: "0.40", quantity: "100" }], asks: [{ price: "0.55", quantity: "1" }],
        }),
        book({
          eventId: "depth", sequence: 3,
          bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.55", quantity: "1" }],
        }),
        book({
          marketId: "btc-15m", tokenId: "down", outcome: "DOWN", eventId: "down-1", sequence: 4,
          bids: [{ price: "0.44", quantity: "8" }], asks: [{ price: "0.45", quantity: "8" }],
        }),
        book({
          eventId: "pair", sequence: 5,
          bids: [{ price: "0.48", quantity: "8" }], asks: [{ price: "0.49", quantity: "8" }],
        }),
        book({
          eventId: "repeat", sequence: 6,
          bids: [{ price: "0.48", quantity: "8" }], asks: [{ price: "0.49", quantity: "8" }],
        }),
        book({
          marketId: "btc-15m", tokenId: "down", outcome: "DOWN", eventId: "down-2", sequence: 7,
          bids: [{ price: "0.10", quantity: "50" }], asks: [{ price: "0.11", quantity: "1" }],
        }),
        book({
          eventId: "up-again", sequence: 8,
          bids: [{ price: "0.20", quantity: "4" }], asks: [{ price: "0.80", quantity: "4" }],
        }),
      ];

      const syncEvents = stream.flatMap((item) => syncEngine.ingestMarket(item));
      const workerEvents: TriggerEvent[] = [];
      for (const item of stream) {
        workerEvents.push(...workerEngine.ingestMarketWithDerived(item, await workerDerived(service, item)));
      }
      expect(workerEvents.map(comparable)).toEqual(syncEvents.map(comparable));
      const kinds = syncEvents.map((item) => item.kind);
      expect(kinds).toContain("BOOK_IMBALANCE");
      expect(kinds).toContain("SPREAD_CHANGE");
      expect(kinds).toContain("DEPTH_CHANGE");
      expect(kinds).toContain("PAIR_COST_OPPORTUNITY");
      expect(kinds).toContain("NEARING_EXPIRY");
    } finally {
      await service.stop();
    }
  });
});

describe("PR52 compact payload is derivation-only", () => {
  it("does not copy trace, journal, or market identity fields", () => {
    const snapshot = book({ eventId: "secret-event", traceId: "secret-trace", marketId: "secret-market" });
    const compact = compactBookSidesForDerivation(snapshot);
    expect(JSON.stringify(compact)).not.toContain("secret");
    expect(Object.keys(compact).sort()).toEqual(["asks", "bids"]);
  });
});
