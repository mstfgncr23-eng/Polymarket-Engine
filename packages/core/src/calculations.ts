import { Decimal } from "decimal.js";
import type { BookLevel, FeeSchedule, MarketSnapshot, Position } from "./types-internal.js";

Decimal.set({ precision: 40, rounding: Decimal.ROUND_HALF_UP });

export function d(value: Decimal.Value): Decimal {
  return new Decimal(value);
}

export function decimal(value: Decimal.Value, places = 8): string {
  return d(value).toDecimalPlaces(places).toFixed();
}

export interface BookWalkResult {
  requestedQuantity: string;
  filledQuantity: string;
  notional: string;
  vwap: string;
  worstPrice: string | null;
  sufficientDepth: boolean;
  levelsConsumed: number;
}

export function walkBook(
  levels: readonly BookLevel[],
  requestedQuantity: Decimal.Value,
  limitPrice?: Decimal.Value,
): BookWalkResult {
  const requested = d(requestedQuantity);
  if (requested.lte(0)) throw new Error("INVALID_QUANTITY");
  let remaining = requested;
  let notional = d(0);
  let worstPrice: Decimal | null = null;
  let levelsConsumed = 0;

  for (const level of levels) {
    const price = d(level.price);
    const available = d(level.quantity);
    if (limitPrice !== undefined && price.gt(limitPrice)) break;
    if (available.lte(0)) continue;
    const take = Decimal.min(remaining, available);
    notional = notional.plus(take.mul(price));
    remaining = remaining.minus(take);
    worstPrice = price;
    levelsConsumed += 1;
    if (remaining.lte(0)) break;
  }

  const filled = requested.minus(remaining);
  return {
    requestedQuantity: decimal(requested),
    filledQuantity: decimal(filled),
    notional: decimal(notional),
    vwap: filled.gt(0) ? decimal(notional.div(filled)) : "0",
    worstPrice: worstPrice ? decimal(worstPrice) : null,
    sufficientDepth: remaining.eq(0),
    levelsConsumed,
  };
}

export function calculateFee(
  quantity: Decimal.Value,
  fillPrice: Decimal.Value,
  liquidityRole: "MAKER" | "TAKER",
  schedule: FeeSchedule,
): string {
  if (liquidityRole === "MAKER" && schedule.takerOnly) return "0";
  const p = d(fillPrice);
  const q = d(quantity);
  if (p.lt(0) || p.gt(1) || q.lt(0)) throw new Error("INVALID_FEE_INPUT");
  const priceComponent = p.mul(d(1).minus(p)).pow(schedule.exponent);
  const fee = q.mul(schedule.rate).mul(priceComponent);
  if (fee.lt("0.000005")) return "0";
  return fee.toDecimalPlaces(5, Decimal.ROUND_HALF_UP).toFixed(5);
}

/** Adverse execution cost only. Price improvement is 0, never an absolute "cost". */
export function calculateSlippageUsd(
  requestedPrice: Decimal.Value,
  fillPrice: Decimal.Value,
  quantity: Decimal.Value,
  side: "BUY" | "SELL" = "BUY",
): string {
  const delta = d(fillPrice).minus(requestedPrice);
  const adverse = side === "SELL" ? delta.negated() : delta;
  if (adverse.lte(0)) return decimal(0);
  return decimal(adverse.mul(quantity));
}

export function orderBookLiquidityUsd(snapshot: MarketSnapshot, side: "BUY" | "SELL"): string {
  const levels = side === "BUY" ? snapshot.asks : snapshot.bids;
  return decimal(levels.reduce((sum, level) => sum.plus(d(level.price).mul(level.quantity)), d(0)));
}

export function bestPrice(snapshot: MarketSnapshot, side: "BUY" | "SELL"): string | null {
  const levels = side === "BUY" ? snapshot.asks : snapshot.bids;
  return levels[0]?.price ?? null;
}

export function spread(snapshot: MarketSnapshot): string | null {
  const bid = snapshot.bids[0];
  const ask = snapshot.asks[0];
  return bid && ask ? decimal(d(ask.price).minus(bid.price)) : null;
}

const DECIMAL_QTY_RE = /^-?\d+(?:\.\d+)?$/;

/**
 * First-N-level imbalance without slice/reduce temps. Number fast path only
 * when every visited quantity is an exact IEEE value (Decimal fallback otherwise).
 */
export function bookImbalance(snapshot: MarketSnapshot, levels = 5): number {
  const bidN = Math.min(levels, snapshot.bids.length);
  const askN = Math.min(levels, snapshot.asks.length);
  const fast = tryNumberBookImbalance(snapshot, bidN, askN);
  if (fast !== null) return fast;
  let bid = d(0);
  let ask = d(0);
  for (let i = 0; i < bidN; i += 1) bid = bid.plus(snapshot.bids[i]!.quantity);
  for (let i = 0; i < askN; i += 1) ask = ask.plus(snapshot.asks[i]!.quantity);
  const total = bid.plus(ask);
  return total.eq(0) ? 0 : bid.minus(ask).div(total).toNumber();
}

export function exactNumberQuantity(value: string): number | null {
  if (!DECIMAL_QTY_RE.test(value)) return null;
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return null;
  // Decimal(number) uses Number#toString and hides binary error (0.1 → "0.1").
  // Compare the decimal literal to the true IEEE value (21-digit precision).
  const ieee = numeric.toPrecision(21);
  if (/e/i.test(ieee)) return d(value).eq(d(ieee)) ? numeric : null;
  return normalizeDecimalLiteral(value) === normalizeDecimalLiteral(ieee) ? numeric : null;
}

function normalizeDecimalLiteral(raw: string): string {
  let text = raw;
  let sign = "";
  if (text.startsWith("-")) {
    sign = "-";
    text = text.slice(1);
  }
  const dot = text.indexOf(".");
  let whole = dot === -1 ? text : text.slice(0, dot);
  let frac = dot === -1 ? "" : text.slice(dot + 1);
  whole = whole.replace(/^0+(?=\d)/, "");
  frac = frac.replace(/0+$/, "");
  return frac.length > 0 ? `${sign}${whole}.${frac}` : `${sign}${whole || "0"}`;
}

function tryNumberBookImbalance(snapshot: MarketSnapshot, bidN: number, askN: number): number | null {
  let bid = 0;
  let ask = 0;
  for (let i = 0; i < bidN; i += 1) {
    const quantity = exactNumberQuantity(snapshot.bids[i]!.quantity);
    if (quantity === null) return null;
    bid += quantity;
  }
  for (let i = 0; i < askN; i += 1) {
    const quantity = exactNumberQuantity(snapshot.asks[i]!.quantity);
    if (quantity === null) return null;
    ask += quantity;
  }
  const total = bid + ask;
  return total === 0 ? 0 : (bid - ask) / total;
}

export interface PairMetrics {
  pairedQuantity: string;
  unpairedQuantity: string;
  combinedAcquisitionCostPerPair: string;
  totalCombinedAcquisitionCost: string;
  settlementValue: string;
  fees: string;
  slippage: string;
  netPairEdge: string;
}

export function calculatePairMetrics(position: Position): PairMetrics {
  const paired = Decimal.min(position.upQuantity, position.downQuantity);
  const unpaired = d(position.upQuantity).minus(position.downQuantity).abs();
  const combinedPerPair = d(position.averageUpCost).plus(position.averageDownCost);
  const acquisition = combinedPerPair.mul(paired);
  const settlement = paired;
  const fees = d(position.totalFees);
  const slippage = d(position.totalSlippage);
  return {
    pairedQuantity: decimal(paired),
    unpairedQuantity: decimal(unpaired),
    combinedAcquisitionCostPerPair: decimal(combinedPerPair),
    totalCombinedAcquisitionCost: decimal(acquisition),
    settlementValue: decimal(settlement),
    fees: decimal(fees),
    slippage: decimal(slippage),
    netPairEdge: decimal(settlement.minus(acquisition).minus(fees)),
  };
}

export function calculateUnrealizedPnl(
  position: Position,
  currentUpPrice: Decimal.Value,
  currentDownPrice: Decimal.Value,
): string {
  const currentValue = d(position.upQuantity).mul(currentUpPrice)
    .plus(d(position.downQuantity).mul(currentDownPrice));
  const cost = d(position.upQuantity).mul(position.averageUpCost)
    .plus(d(position.downQuantity).mul(position.averageDownCost));
  return decimal(currentValue.minus(cost));
}
