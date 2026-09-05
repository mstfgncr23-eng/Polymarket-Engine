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
