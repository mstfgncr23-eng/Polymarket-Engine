import { bookImbalance, d, spread } from "./calculations.js";
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
  if (left === right) return true;
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

export interface DerivedBookState {
  snapshot: MarketSnapshot;
  derived: BookDerived;
  bidSum: ReturnType<typeof d>;
  askSum: ReturnType<typeof d>;
}

/** Exact component reuse: imbalance reads quantities only, spread reads best
 * prices only. Decimal sums retain the original left-to-right arithmetic. */
export function deriveBookState(snapshot: MarketSnapshot, previous?: DerivedBookState | null): DerivedBookState {
  const sum = (levels: MarketSnapshot["bids"]) => {
    let value = d(0);
    for (let i=0; i<Math.min(5,levels.length); i++) value=value.plus(levels[i]!.quantity);
    return value;
  };
  const bidSum = previous && sameQuantityPrefix(previous.snapshot.bids,snapshot.bids,5) ? previous.bidSum : sum(snapshot.bids);
  const askSum = previous && sameQuantityPrefix(previous.snapshot.asks,snapshot.asks,5) ? previous.askSum : sum(snapshot.asks);
  let imbalance: number;
  if (previous && bidSum===previous.bidSum && askSum===previous.askSum) imbalance=previous.derived.imbalance;
  else { const total=bidSum.plus(askSum);imbalance=total.eq(0)?0:bidSum.minus(askSum).div(total).toNumber(); }
  const unchangedPrices = previous && previous.snapshot.bids[0]?.price===snapshot.bids[0]?.price
    && previous.snapshot.asks[0]?.price===snapshot.asks[0]?.price;
  const spreadText = unchangedPrices ? previous.derived.spread : spread(snapshot);
  return {snapshot,bidSum,askSum,derived:{spread:spreadText,spreadNumber:Number(spreadText??0),
    totalDepth:totalDepthCombined(snapshot),imbalance,imbalanceAbs:Math.abs(imbalance)}};
}
