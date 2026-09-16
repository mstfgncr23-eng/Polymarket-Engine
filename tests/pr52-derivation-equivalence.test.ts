import { describe, expect, it } from "vitest";
import {
  compactBookSidesForDerivation,
  deriveBook,
  deriveBookFromSides,
  DERIVATION_COMPACT_ASK_LEVELS,
  DERIVATION_COMPACT_BID_LEVELS,
  TRIGGER_STABILITY_BOUNDS,
  type BookDerived,
  type MarketSnapshot,
} from "@pm/core";
import { snapshot as fixtureSnapshot } from "./fixtures.js";

function levels(count: number, start: number, qty: string | ((index: number) => string)): Array<{ price: string; quantity: string }> {
  return Array.from({ length: count }, (_, index) => ({
    price: (start + index * 0.001).toFixed(8),
    quantity: typeof qty === "function" ? qty(index) : qty,
  }));
}

function book(bids: Array<{ price: string; quantity: string }>, asks: Array<{ price: string; quantity: string }>): MarketSnapshot {
  return fixtureSnapshot({ bids, asks });
}

function expectDerivedEqual(actual: BookDerived, expected: BookDerived, label: string): void {
  expect({ ...actual, label }, label).toEqual({ ...expected, label });
}

const CORPUS: Array<{ name: string; bids: Array<{ price: string; quantity: string }>; asks: Array<{ price: string; quantity: string }> }> = [
  { name: "empty", bids: [], asks: [] },
  { name: "one-bid", bids: [{ price: "0.40", quantity: "1" }], asks: [] },
  { name: "one-ask", bids: [], asks: [{ price: "0.60", quantity: "1" }] },
  { name: "one-each", bids: [{ price: "0.47", quantity: "10" }], asks: [{ price: "0.48", quantity: "5" }] },
  { name: "5-levels", bids: levels(5, 0.40, "1"), asks: levels(5, 0.60, "2") },
  { name: "10-levels", bids: levels(10, 0.30, "1.5"), asks: levels(10, 0.55, "2.25") },
  { name: "45-levels", bids: levels(45, 0.10, (i) => String(1 + (i % 9))), asks: levels(45, 0.60, (i) => String(2 + (i % 7))) },
  { name: "100-levels", bids: levels(100, 0.01, (i) => (0.0001 * (i + 1)).toFixed(8)), asks: levels(100, 0.50, (i) => (0.0003 * (i + 1)).toFixed(8)) },
  { name: "asymmetric-short-bids", bids: levels(2, 0.40, "3"), asks: levels(40, 0.50, "1") },
  { name: "asymmetric-short-asks", bids: levels(40, 0.10, "1"), asks: levels(2, 0.80, "9") },
  { name: "tiny-decimals", bids: [{ price: "0.40000000", quantity: "0.00000001" }], asks: [{ price: "0.40000002", quantity: "0.00000002" }] },
  { name: "large-decimals", bids: [{ price: "0.01", quantity: "999999999999.12345678" }], asks: [{ price: "0.99", quantity: "888888888888.87654321" }] },
  { name: "varying-precision", bids: [{ price: "0.1", quantity: "1" }, { price: "0.12345678", quantity: "1.23456789" }], asks: [{ price: "0.2", quantity: "9.87654321" }, { price: "0.20000001", quantity: "0.00000010" }] },
  { name: "zero-quantities", bids: [{ price: "0.40", quantity: "0" }, { price: "0.39", quantity: "10" }], asks: [{ price: "0.50", quantity: "0" }, { price: "0.51", quantity: "1" }] },
  {
    name: "realistic-polymarket",
    bids: [
      { price: "0.48", quantity: "1250.5" },
      { price: "0.47", quantity: "800" },
      { price: "0.46", quantity: "410.25" },
      { price: "0.45", quantity: "90" },
      { price: "0.44", quantity: "12.5" },
      { price: "0.43", quantity: "3" },
    ],
    asks: [
      { price: "0.49", quantity: "980" },
      { price: "0.50", quantity: "640.75" },
      { price: "0.51", quantity: "220" },
      { price: "0.52", quantity: "15" },
      { price: "0.55", quantity: "1.01" },
    ],
  },
];

describe("PR52 derivation compact payload bounds", () => {
  it("derives compact lengths from TRIGGER_STABILITY_BOUNDS", () => {
    expect(DERIVATION_COMPACT_BID_LEVELS).toBe(TRIGGER_STABILITY_BOUNDS.totalDepthCombinedLevels);
    expect(DERIVATION_COMPACT_ASK_LEVELS).toBe(
      Math.max(TRIGGER_STABILITY_BOUNDS.imbalanceLevels, TRIGGER_STABILITY_BOUNDS.totalDepthCombinedLevels),
    );
    expect(DERIVATION_COMPACT_BID_LEVELS).toBe(10);
    expect(DERIVATION_COMPACT_ASK_LEVELS).toBe(10);
  });
});

describe("PR52 exact derivation equivalence", () => {
  it("deriveBook delegates to deriveBookFromSides", () => {
    const snapshot = book(levels(12, 0.2, "1"), levels(12, 0.6, "2"));
    expect(deriveBook(snapshot)).toEqual(deriveBookFromSides(snapshot));
  });

  it("compact sides preserve exact BookDerived output for the deterministic corpus", () => {
    for (const item of CORPUS) {
      const snapshot = book(item.bids, item.asks);
      const compact = compactBookSidesForDerivation(snapshot);
      expect(compact.bids.length).toBeLessThanOrEqual(DERIVATION_COMPACT_BID_LEVELS);
      expect(compact.asks.length).toBeLessThanOrEqual(DERIVATION_COMPACT_ASK_LEVELS);
      expectDerivedEqual(deriveBookFromSides(compact), deriveBook(snapshot), item.name);
    }
  });

  it("randomized corpus: sync derive === compact derive", () => {
    for (let i = 0; i < 250; i += 1) {
      const bidCount = i % 17 === 0 ? 0 : 1 + (i % 100);
      const askCount = i % 19 === 0 ? 0 : 1 + ((i * 3) % 100);
      const qty = i % 5 === 0 ? "0.00000001" : i % 7 === 0 ? "123456789.12345678" : String((i % 97) + (i % 10) / 10);
      const snapshot = book(
        levels(bidCount, 0.01, (index) => (index === 0 ? qty : String(1 + (index % 11)))),
        levels(askCount, 0.50, (index) => String(0.25 * (1 + (index % 8)))),
      );
      expect(deriveBookFromSides(compactBookSidesForDerivation(snapshot))).toEqual(deriveBook(snapshot));
    }
  });
});
