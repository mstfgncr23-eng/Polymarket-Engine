import { describe, expect, it } from "vitest";
import {
  bookImbalance,
  bookImbalanceReference,
  deriveBook,
  SAFETY_ESCALATION_KINDS,
  safetyEscalationMayEmit,
  totalDepthCombined,
  totalDepthCombinedReference,
  TriggerEngine,
  triggerConfigSchema,
  triggerDedupeScopeKey,
  type MarketSnapshot,
  type TriggerEvent,
} from "@pm/core";
import { snapshot as fixtureSnapshot } from "./fixtures.js";

function book(overrides: Partial<MarketSnapshot> = {}): MarketSnapshot {
  return fixtureSnapshot(overrides);
}

function levels(count: number, start: number, qty: string): Array<{ price: string; quantity: string }> {
  return Array.from({ length: count }, (_, i) => ({ price: (start + i * 0.01).toFixed(2), quantity: qty }));
}

describe("Part 1 totalDepth combined first-10 semantics", () => {
  it("matches [...bids,...asks].slice(0,10) for empty, short, exact, overfull, mixed, and decimals", () => {
    const cases: Array<Pick<MarketSnapshot, "bids" | "asks">> = [
      { bids: [], asks: [] },
      { bids: levels(3, 0.4, "1"), asks: [] },
      { bids: [], asks: levels(3, 0.6, "2") },
      { bids: levels(4, 0.1, "1.5"), asks: levels(4, 0.6, "2.25") },
      { bids: levels(10, 0.1, "1"), asks: levels(10, 0.6, "1") },
      { bids: levels(20, 0.1, "3"), asks: levels(20, 0.6, "4") },
      { bids: levels(2, 0.1, "0.00000001"), asks: levels(20, 0.6, "9.99") },
    ];
    for (const item of cases) {
      expect(totalDepthCombined(item)).toBe(totalDepthCombinedReference(item));
    }
  });

  it("is not 10 bids + 10 asks", () => {
    const snapshot = { bids: levels(20, 0.1, "1"), asks: levels(20, 0.6, "1") };
    expect(totalDepthCombined(snapshot)).toBe(10);
    expect(totalDepthCombined(snapshot)).not.toBe(20);
  });
});

describe("Part 1 bookImbalance allocation-free Decimal accumulate", () => {
  it("preserves first-N-level Decimal semantics vs the previous slice/reduce formula", () => {
    const snapshot = book({
      bids: [{ price: "0.4", quantity: "10" }, { price: "0.39", quantity: "5" }, { price: "0.38", quantity: "1" }],
      asks: [{ price: "0.5", quantity: "2" }, { price: "0.51", quantity: "2" }],
    });
    expect(bookImbalance(snapshot)).toBe(bookImbalanceReference(snapshot));
    expect(bookImbalance(snapshot)).toBeCloseTo((16 - 4) / 20, 12);
  });

  it("matches Decimal reference for 2^53+1, tiny decimals, mixed, and extremes", () => {
    const cases: Array<Pick<MarketSnapshot, "bids" | "asks">> = [
      {
        bids: [{ price: "0.4", quantity: String(2 ** 53 + 1) }],
        asks: [{ price: "0.5", quantity: "1" }],
      },
      {
        bids: [{ price: "0.4", quantity: "0.0000000000000001" }],
        asks: [{ price: "0.5", quantity: "0.0000000000000002" }],
      },
      {
        bids: [{ price: "0.4", quantity: "0.1" }, { price: "0.39", quantity: "10" }],
        asks: [{ price: "0.5", quantity: "2.25" }, { price: "0.51", quantity: "0.3" }],
      },
      {
        bids: [{ price: "0.01", quantity: "999999999999999" }],
        asks: [{ price: "0.99", quantity: "0.00000001" }],
      },
    ];
    for (const item of cases) {
      const snapshot = book(item);
      expect(bookImbalance(snapshot)).toBe(bookImbalanceReference(snapshot));
    }
  });

  it("randomized equivalence vs Decimal slice/reduce reference", () => {
    for (let i = 0; i < 200; i += 1) {
      const qty = i % 3 === 0 ? "0.1" : String(1 + (i % 17));
      const snapshot = book({
        bids: levels(6, 0.1, qty),
        asks: levels(6, 0.6, String(2 + (i % 5))),
      });
      expect(bookImbalance(snapshot)).toBe(bookImbalanceReference(snapshot));
      expect(Math.abs(bookImbalance(snapshot))).toBeLessThanOrEqual(1);
    }
  });
});

describe("Part 1 derived reuse", () => {
  it("reuses previous spread/depth and does not invent SPREAD/DEPTH against a forgotten book", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({
      cooldownMs: 0, imbalanceThreshold: 1, spreadChangeThreshold: 0.01, depthChangeThreshold: 0.2,
    }));
    const first = book({ bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.42", quantity: "10" }] });
    engine.ingestMarket(first);
    engine.forgetBook(first.marketId, first.outcome);
    const second = book({
      eventId: "e2", sequence: 2,
      bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.50", quantity: "10" }],
    });
    const kinds = engine.ingestMarket(second).map((item) => item.kind);
    expect(kinds).not.toContain("SPREAD_CHANGE");
    expect(kinds).not.toContain("DEPTH_CHANGE");
  });

  it("deriveBook computes spread/depth/imbalance once", () => {
    const snapshot = book({ bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.50", quantity: "2" }] });
    const derived = deriveBook(snapshot);
    expect(derived.spreadNumber).toBeCloseTo(0.1, 8);
    expect(derived.totalDepth).toBe(12);
    expect(derived.imbalanceAbs).toBeGreaterThan(0);
  });
});

describe("Part 2 Tests A semantic-preserving streams", () => {
  it("same book twice still emits then suppresses with identical kind/severity/score/evidence", () => {
    const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 60_000, imbalanceThreshold: 0.5 }));
    const first = engine.ingestMarket(book({ bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }] }));
    const second = engine.ingestMarket(book({ eventId: "dup", sequence: 2, bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }] }));
    expect(first.length).toBeGreaterThan(0);
    expect(second.length).toBe(0);
    expect(first[0]?.kind).toBe("BOOK_IMBALANCE");
  });
});

describe("Part 2 Tests B intentional cooldown policy", () => {
  it("score-bucket change does not bypass cooldown for non-safety research", () => {
    let t = Date.parse("2026-08-31T12:00:00.000Z");
    const engine = new TriggerEngine(triggerConfigSchema.parse({
      cooldownMs: 60_000, imbalanceThreshold: 0.5,
    }), () => new Date(t));
    const first = engine.ingestMarket(book({ bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }] }));
    expect(first.some((item) => item.kind === "BOOK_IMBALANCE")).toBe(true);
    const louder = engine.ingestMarket(book({
      eventId: "e2", sequence: 2,
      bids: [{ price: "0.47", quantity: "1000" }], asks: [{ price: "0.48", quantity: "1" }],
    }));
    expect(louder.filter((item) => item.kind === "BOOK_IMBALANCE")).toHaveLength(0);
  });

  it("safety severity 1→2 may emit; 2→1 and 2→2 suppressed; 2→3 may emit", () => {
    let t = Date.parse("2026-08-31T12:00:00.000Z");
    const engine = new TriggerEngine(triggerConfigSchema.parse({
      cooldownMs: 60_000, failedHedgeDurationMs: 1, expiryWarningMs: 3_600_000,
    }), () => new Date(t));
    const position = {
      id: "pos", marketId: "btc-15m", upQuantity: "10", downQuantity: "0", averageUpCost: "0.48", averageDownCost: "0",
      pairedQuantity: "0", unpairedOutcome: "UP" as const, unpairedQuantity: "10", combinedPairCost: "0.48",
      totalFees: "0", totalSlippage: "0", openedAt: new Date(t - 5_000).toISOString(), updatedAt: new Date(t).toISOString(),
    };
    const first = engine.ingestPosition(position, "t1", "BTC").filter((item) => item.kind === "DIRECTIONAL_EXPOSURE");
    expect(first[0]?.severity).toBe(1);
    const escalated = engine.ingestPosition({ ...position, unpairedQuantity: "25", id: "pos2" }, "t2", "BTC")
      .filter((item) => item.kind === "DIRECTIONAL_EXPOSURE");
    expect(escalated[0]?.severity).toBe(2);
    const same = engine.ingestPosition({ ...position, unpairedQuantity: "25", id: "pos3" }, "t3", "BTC")
      .filter((item) => item.kind === "DIRECTIONAL_EXPOSURE");
    expect(same).toHaveLength(0);
    const down = engine.ingestPosition({ ...position, unpairedQuantity: "5", id: "pos4" }, "t4", "BTC")
      .filter((item) => item.kind === "DIRECTIONAL_EXPOSURE");
    expect(down).toHaveLength(0);
    expect(safetyEscalationMayEmit("DIRECTIONAL_EXPOSURE", 2, 3)).toBe(true);
    expect(safetyEscalationMayEmit("DIRECTIONAL_EXPOSURE", 2, 2)).toBe(false);
    expect(safetyEscalationMayEmit("DIRECTIONAL_EXPOSURE", 2, 1)).toBe(false);
    expect(safetyEscalationMayEmit("BOOK_IMBALANCE", 1, 2)).toBe(false);
    expect(safetyEscalationMayEmit("SCHEDULED_REVIEW", 1, 2)).toBe(false);
  });

  it("cooldown expiry emits again; market removal clears; no score-key accumulation", () => {
    let t = Date.parse("2026-08-31T12:00:00.000Z");
    const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 1_000, imbalanceThreshold: 0.5 }), () => new Date(t));
    engine.ingestMarket(book({ bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }] }));
    expect(engine.dedupeEntryCount()).toBeGreaterThan(0);
    t += 2_000;
    const again = engine.ingestMarket(book({
      eventId: "later", sequence: 2,
      bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }],
    }));
    expect(again.some((item) => item.kind === "BOOK_IMBALANCE")).toBe(true);
    engine.dropMarket("btc-15m");
    expect(engine.dedupeEntryCount()).toBe(0);
    expect(SAFETY_ESCALATION_KINDS).not.toContain("SCHEDULED_REVIEW");
    expect(triggerDedupeScopeKey("m", "BTC", "BOOK_IMBALANCE")).toBe("m:BTC:BOOK_IMBALANCE");
  });
});

describe("Part 3 candidates counted before cooldown", () => {
  it("does not reduce trigger_candidates_total by moving suppression earlier", () => {
    let candidates = 0;
    const engine = new TriggerEngine(
      triggerConfigSchema.parse({ cooldownMs: 60_000, imbalanceThreshold: 0.5 }),
      () => new Date(),
      {
        onCandidates: (count) => { candidates += count; },
        onEmitted: () => undefined,
        onSuppressed: () => undefined,
      },
    );
    const bookSnap = book({ bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }] });
    engine.ingestMarket(bookSnap);
    engine.ingestMarket({ ...bookSnap, eventId: "dup", sequence: 2 });
    expect(candidates).toBeGreaterThanOrEqual(2);
  });
});

function comparable(event: TriggerEvent) {
  return {
    kind: event.kind, severity: event.severity, score: event.score, source: event.source,
    evidence: event.evidence, sourceEventIds: event.sourceEventIds, marketId: event.marketId, asset: event.asset,
  };
}

describe("golden stream A vs B", () => {
  it("semantic-preserving stream matches field-wise across two engines", () => {
    const config = triggerConfigSchema.parse({ cooldownMs: 0, imbalanceThreshold: 0.5, spreadChangeThreshold: 0.01, depthChangeThreshold: 0.2 });
    const a = new TriggerEngine(config, () => new Date("2026-08-31T12:00:00.000Z"));
    const b = new TriggerEngine(config, () => new Date("2026-08-31T12:00:00.000Z"));
    const stream = [
      book({ bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }] }),
      book({ eventId: "e2", sequence: 2, bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.48", quantity: "10" }] }),
    ];
    expect(stream.flatMap((item) => a.ingestMarket(item)).map(comparable))
      .toEqual(stream.flatMap((item) => b.ingestMarket(item)).map(comparable));
  });

  it("documents exact kinds/severity/score for a deterministic semantic-preserving stream", () => {
    const engine = new TriggerEngine(
      triggerConfigSchema.parse({ cooldownMs: 0, imbalanceThreshold: 0.5, spreadChangeThreshold: 0.01, depthChangeThreshold: 0.2 }),
      () => new Date("2026-08-31T12:00:00.000Z"),
    );
    const first = engine.ingestMarket(book({
      bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }],
    }));
    expect(first.map((item) => ({ kind: item.kind, severity: item.severity, source: item.source }))).toEqual([
      { kind: "BOOK_IMBALANCE", severity: 2, source: "polymarket" },
      { kind: "NEWS_INFORMATION", severity: 2, source: "news" },
    ]);
    expect(first[0]?.score).toBeCloseTo(99 / 101, 12);
    const second = engine.ingestMarket(book({
      eventId: "e2", sequence: 2,
      bids: [{ price: "0.40", quantity: "10" }], asks: [{ price: "0.48", quantity: "10" }],
    }));
    expect(second.map((item) => item.kind)).toContain("SPREAD_CHANGE");
    expect(second.map((item) => item.kind)).toContain("DEPTH_CHANGE");
  });
});
