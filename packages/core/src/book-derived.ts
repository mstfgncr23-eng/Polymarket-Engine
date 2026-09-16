import { bookImbalance, spread } from "./calculations.js";
import { TRIGGER_STABILITY_BOUNDS } from "./stability-bounds.js";
import type { MarketSnapshot } from "./types-internal.js";

export interface BookDerived {
  spread: string | null;
  spreadNumber: number;
  totalDepth: number;
  imbalance: number;
  imbalanceAbs: number;
}

export type BookSides = Pick<MarketSnapshot, "bids" | "asks">;

export interface CompactBookLevel {
  price: string;
  quantity: string;
}

export interface CompactBookSides {
  bids: CompactBookLevel[];
  asks: CompactBookLevel[];
}

/** Leading bids needed for spread, combined depth, and imbalance. */
export const DERIVATION_COMPACT_BID_LEVELS = TRIGGER_STABILITY_BOUNDS.totalDepthCombinedLevels;

/**
 * Leading asks needed for spread, combined depth (when bids are short), and
 * first-N imbalance. Combined depth is bids-then-asks, not 10+10, but a short
 * bid side still consumes remaining depth slots from asks.
 */
export const DERIVATION_COMPACT_ASK_LEVELS = Math.max(
  TRIGGER_STABILITY_BOUNDS.imbalanceLevels,
  TRIGGER_STABILITY_BOUNDS.totalDepthCombinedLevels,
);

/**
 * Exact semantic equivalent of `[...bids, ...asks].slice(0, 10)` then
 * `Number(quantity)` left-to-right. Not 10 bids + 10 asks.
 */
export function totalDepthCombined(
  snapshot: BookSides,
  limit = TRIGGER_STABILITY_BOUNDS.totalDepthCombinedLevels,
): number {
  let sum = 0;
  let remaining = limit;
  const bids = snapshot.bids;
  for (let i = 0; i < bids.length && remaining > 0; i += 1, remaining -= 1) {
    sum += Number(bids[i]!.quantity);
  }
  const asks = snapshot.asks;
  for (let i = 0; i < asks.length && remaining > 0; i += 1, remaining -= 1) {
    sum += Number(asks[i]!.quantity);
  }
  return sum;
}

/** Reference implementation used only by equivalence tests. */
export function totalDepthCombinedReference(
  snapshot: BookSides,
  limit = TRIGGER_STABILITY_BOUNDS.totalDepthCombinedLevels,
): number {
  return [...snapshot.bids, ...snapshot.asks].slice(0, limit).reduce((sum, level) => sum + Number(level.quantity), 0);
}

/** Minimum immutable bid/ask strings required by the canonical derivation. */
export function compactBookSidesForDerivation(snapshot: BookSides): CompactBookSides {
  return {
    bids: snapshot.bids.slice(0, DERIVATION_COMPACT_BID_LEVELS).map((level) => ({
      price: level.price,
      quantity: level.quantity,
    })),
    asks: snapshot.asks.slice(0, DERIVATION_COMPACT_ASK_LEVELS).map((level) => ({
      price: level.price,
      quantity: level.quantity,
    })),
  };
}

export function deriveBookFromSides(sides: BookSides): BookDerived {
  const spreadText = spread(sides);
  const imbalance = bookImbalance(sides);
  return {
    spread: spreadText,
    spreadNumber: Number(spreadText ?? 0),
    totalDepth: totalDepthCombined(sides),
    imbalance,
    imbalanceAbs: Math.abs(imbalance),
  };
}

export function deriveBook(snapshot: MarketSnapshot): BookDerived {
  return deriveBookFromSides(snapshot);
}

export function isBookDerived(value: unknown): value is BookDerived {
  if (!value || typeof value !== "object") return false;
  const record = value as Record<string, unknown>;
  return (record.spread === null || typeof record.spread === "string")
    && typeof record.spreadNumber === "number"
    && Number.isFinite(record.spreadNumber)
    && typeof record.totalDepth === "number"
    && Number.isFinite(record.totalDepth)
    && typeof record.imbalance === "number"
    && Number.isFinite(record.imbalance)
    && typeof record.imbalanceAbs === "number"
    && Number.isFinite(record.imbalanceAbs);
}
