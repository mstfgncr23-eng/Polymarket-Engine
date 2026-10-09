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

/**
 * Exact semantic equivalent of `[...bids, ...asks].slice(0, 10)` then
 * `Number(quantity)` left-to-right. Not 10 bids + 10 asks.
 */
export function totalDepthCombined(
  snapshot: Pick<MarketSnapshot, "bids" | "asks">,
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
  snapshot: Pick<MarketSnapshot, "bids" | "asks">,
  limit = TRIGGER_STABILITY_BOUNDS.totalDepthCombinedLevels,
): number {
  return [...snapshot.bids, ...snapshot.asks].slice(0, limit).reduce((sum, level) => sum + Number(level.quantity), 0);
}

function sameQuantityPrefix(
  left: readonly { quantity: string }[],
  right: readonly { quantity: string }[],
  limit: number,
): boolean {
  const leftCount = Math.min(limit, left.length);
  const rightCount = Math.min(limit, right.length);
  if (leftCount != rightCount) return false;
  for (let i = 0; i < leftCount; i += 1) {
    if (left[i]!.quantity !== right[i]!.quantity) return false;
  }
  return true;
}

function combinedQuantityAt(snapshot: Pick<MarketSnapshot, "bids" | "asks">, index: number): string | undefined {
  if (index < snapshot.bids.length) return snapshot.bids[index]?.quantity;
  return snapshot.asks[index - snapshot.bids.length]?.quantity;
}

/**
 * Exact input-equivalence predicate for deriveBook().
 * It deliberately compares only the fields deriveBook reads:
 * best bid/ask prices, first-5 side quantities, and first-10 combined quantities.
 */
export function sameDerivedBookInputs(
  left: Pick<MarketSnapshot, "bids" | "asks">,
  right: Pick<MarketSnapshot, "bids" | "asks">,
): boolean {
  if ((left.bids[0]?.price ?? null) !== (right.bids[0]?.price ?? null)) return false;
  if ((left.asks[0]?.price ?? null) !== (right.asks[0]?.price ?? null)) return false;
  if (!sameQuantityPrefix(left.bids, right.bids, 5)) return false;
  if (!sameQuantityPrefix(left.asks, right.asks, 5)) return false;

  const limit = TRIGGER_STABILITY_BOUNDS.totalDepthCombinedLevels;
  const leftCount = Math.min(limit, left.bids.length + left.asks.length);
  const rightCount = Math.min(limit, right.bids.length + right.asks.length);
  if (leftCount !== rightCount) return false;
  for (let i = 0; i < leftCount; i += 1) {
    if (combinedQuantityAt(left, i) !== combinedQuantityAt(right, i)) return false;
  }
  return true;
}

export function deriveBook(snapshot: MarketSnapshot): BookDerived {
  const spreadText = spread(snapshot);
  const imbalance = bookImbalance(snapshot);
  return {
    spread: spreadText,
    spreadNumber: Number(spreadText ?? 0),
    totalDepth: totalDepthCombined(snapshot),
    imbalance,
    imbalanceAbs: Math.abs(imbalance),
  };
}
