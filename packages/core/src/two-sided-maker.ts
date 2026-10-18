import { Decimal } from "decimal.js";
import { calculateFee, d, walkBook } from "./calculations.js";
import type { MarketSnapshot } from "./domain.js";
import { passiveMakerBuyPrice, passiveMakerLimitPrice } from "./maker-first.js";

export type DualMakerRejection =
  | "INVALID_CONFIGURATION"
  | "MARKET_IDENTITY"
  | "STALE_OR_GAPPED_BOOK"
  | "NON_PASSIVE_QUOTE"
  | "MINIMUM_SIZE"
  | "SIDE_EXPOSURE_CAP"
  | "INSUFFICIENT_CASH"
  | "INSUFFICIENT_LOCKED_EDGE"
  | "HEDGE_COST_NOT_COVERED";

/** Why the cost-bound quote could not be derived for one side. */
export type CostBoundReason =
  | "NO_OPPOSITE_ASK"
  | "INSUFFICIENT_OPPOSITE_DEPTH"
  | "NO_OPPOSITE_FEE_SCHEDULE"
  | "NO_TICK_SIZE"
  | "ROI_UNREACHABLE";

/**
 * Worst-case single-fill economics for one quoted leg. Two resting maker
 * quotes never guarantee a pair: if only this leg fills as a maker while the
 * sibling maker quote does NOT fill, the residual must be hedged by TAKING the
 * opposite ASK. `netRoi` is the fee-adjusted ROI of that worst case on the full
 * reserved notional (this leg + the taker hedge). `covered` is true exactly
 * when that ROI clears the planner's minPairNetRoi floor.
 */
export interface SingleFillHedgeEconomics {
  covered: boolean;
  netRoi: number;
  residualNetUsd: string;
  hedgeCostUsd: string;
}

/**
 * Maximum affordable maker BUY price for one outcome such that, even in the
 * single-fill worst case (this leg fills as a maker quote; the sibling maker
 * quote does NOT fill, so the residual is hedged by TAKING the opposite ASK at
 * its actual fee-adjusted depth), the net still clears `minPairNetRoi` on the
 * full reserved notional.
 *
 * This is the corrected cost-bound quote ("maliyet sınırlı kotasyon"):
 *  - It uses the ACTUAL opposite-ASK depth walk (fee-adjusted), not a fitted
 *    quote, for the hedge cost.
 *  - It prices the leg at the MAXIMUM passive affordable price: any on-tick
 *    price strictly below the quoted side's best ASK (post-only) that still
 *    clears the ROI floor. This may be below the best bid (a less aggressive
 *    passive quote) or above it (a price-improving passive quote); both are
 *    valid post-only orders filled by exact-price SELL flow in evolveMakerTrade.
 *
 * Returns the floored-tick price, or a rejection reason when the opposite depth
 * cannot cover the residual at the ROI floor.
 */
export function costBoundMakerBuyPrice(input: {
  book: MarketSnapshot;
  opposite: MarketSnapshot;
  quantity: string;
  minPairNetRoi: number;
}): { price: string } | { reason: CostBoundReason } {
  const q = d(input.quantity);
  if (q.lte(0) || !Number.isFinite(input.minPairNetRoi) || input.minPairNetRoi < 0) return { reason: "ROI_UNREACHABLE" };
  const opposite = input.opposite;
  if (!opposite.asks.length) return { reason: "NO_OPPOSITE_ASK" };
  if (!opposite.feeSchedule) return { reason: "NO_OPPOSITE_FEE_SCHEDULE" };
  if (!input.book.tickSize || d(input.book.tickSize).lte(0) || !opposite.tickSize || d(opposite.tickSize).lte(0)) return { reason: "NO_TICK_SIZE" };
  const tick = d(input.book.tickSize);
  // Worst-case hedge: TAKER BUY of the full residual from the opposite ASK.
  const walk = walkBook(opposite.asks, q, undefined, "BUY");
  if (!walk.sufficientDepth || d(walk.filledQuantity).lt(q)) return { reason: "INSUFFICIENT_OPPOSITE_DEPTH" };
  const hedgeCost = d(walk.notional).plus(d(calculateFee(q, walk.vwap, "TAKER", opposite.feeSchedule)));
  // net(p) = q*(1-p) - makerFee(p) - hedgeCost ; reserve(p) = q*p + makerFee(p) + hedgeCost.
  // Require net >= R*reserve  =>  p <= (q - (1+R)*hedgeCost) / (q*(1+R)).
  // The quoted side's maker fee is 0 for takerOnly schedules; it is re-checked
  // with the actual fee at the candidate price below (fail-closed otherwise).
  const R = d(input.minPairNetRoi);
  const numerator = q.minus(d(1).plus(R).mul(hedgeCost));
  const denominator = q.mul(d(1).plus(R));
  const bound = numerator.div(denominator);
  if (bound.lte(0)) return { reason: "ROI_UNREACHABLE" };
  let price = bound.div(tick).floor().mul(tick);
  // Post-only passive bound on the quoted side: strictly below its best ASK
  // (at least one tick). No best-bid equality requirement.
  const ask = input.book.asks[0];
  if (ask) price = Decimal.min(price, d(ask.price).minus(tick));
  price = price.div(tick).floor().mul(tick);
  if (price.lte(0)) return { reason: "ROI_UNREACHABLE" };
  const makerFee = d(calculateFee(q, price, "MAKER", input.book.feeSchedule!));
  const net = q.minus(q.mul(price)).minus(makerFee).minus(hedgeCost);
  const reserve = q.mul(price).plus(makerFee).plus(hedgeCost);
  if (net.lte(0) || net.div(reserve).lt(R)) return { reason: "ROI_UNREACHABLE" };
  return { price: price.toFixed() };
}

/**
 * Worst-case single-fill economics for one leg after its final (possibly
 * cost-bound capped) quote price: this leg fills as a maker; the residual is
 * hedged by TAKING the opposite ASK at its actual fee-adjusted depth.
 */
export function singleFillHedgeEconomics(input: {
  book: MarketSnapshot;
  opposite: MarketSnapshot;
  quantity: string;
  price: string;
  makerFeeUsd: string;
  minPairNetRoi: number;
}): SingleFillHedgeEconomics {
  const q = d(input.quantity);
  const price = d(input.price);
  const makerFee = d(input.makerFeeUsd);
  const walk = walkBook(input.opposite.asks, q, undefined, "BUY");
  if (!input.opposite.feeSchedule || !walk.sufficientDepth || d(walk.filledQuantity).lt(q)) {
    // The residual cannot be fully hedged at the opposite ASK: the worst case is
    // an unhedged tail, which is never covered.
    return { covered: false, netRoi: Number.NEGATIVE_INFINITY, residualNetUsd: "0", hedgeCostUsd: "0" };
  }
  const hedgeCost = d(walk.notional).plus(
    d(input.opposite.feeSchedule ? calculateFee(q, walk.vwap, "TAKER", input.opposite.feeSchedule) : "0"),
  );
  const reserve = q.mul(price).plus(makerFee).plus(hedgeCost);
  const net = q.minus(q.mul(price)).minus(makerFee).minus(hedgeCost);
  const netRoi = reserve.gt(0) ? net.div(reserve).toNumber() : Number.NEGATIVE_INFINITY;
  return {
    covered: net.gt(0) && netRoi >= input.minPairNetRoi,
    netRoi,
    residualNetUsd: net.toFixed(),
    hedgeCostUsd: hedgeCost.toFixed(),
  };
}


export type DualMakerQuotePlan = {
  marketId: string;
  quantity: string;
  up: { tokenId: string; price: string; reserveUsd: string; makerFeeUsd: string };
  down: { tokenId: string; price: string; reserveUsd: string; makerFeeUsd: string };
  combinedReserveUsd: string;
  netPairPnlUsd: string;
  netPairRoi: number;
  maxUnpairedDownsideUsd: string;
  /** Present when costBound is enabled. Worst-case single-fill hedge
   *  coverage for each leg: always guaranteed by the cost-bound cap in
   *  classic mode; with touchPricing it is an AUDIT and may be uncovered
   *  (the tail is governed by reservation/risk limits + hedge machinery). */
  worstCaseSingleFill?: {
    up: SingleFillHedgeEconomics;
    down: SingleFillHedgeEconomics;
  } | undefined;
  /** Two resting first-leg quotes do not guarantee a pair. This is a conditional result. */
  fillAssumption: "BOTH_QUOTES_FILL_AT_LIMIT";
};

export function planDualMakerQuotes(input: {
  up: MarketSnapshot;
  down: MarketSnapshot;
  quantity: string;
  perSideRiskCapUsd: string;
  availableCashUsd: string;
  minPairNetRoi: number;
  nowMs: number;
  maxBookAgeMs: number;
  /** Enable cost-bound dual maker pricing: price each leg at the maximum passive
   *  affordable quote (below or above the best bid, still post-only) so the
   *  worst-case single-fill taker hedge at the opposite ASK is covered at
   *  minPairNetRoi. Default false preserves the historical join-the-bid price. */
  costBound?: boolean;
  /** Touch fill-zone pricing (opt-in, default false; requires costBound=true):
   *  keep each leg at the best-bid JOIN price (the zone where exact-price public
   *  SELL flow actually fills maker quotes) even when the worst-case single-fill
   *  taker hedge alone would not clear the ROI floor at that price. The
   *  single-fill tail is then an AUDIT (worstCaseSingleFill may be uncovered)
   *  bounded by the existing reservation/risk limits and the hedge machinery,
   *  instead of shaving the quote below the touch into the zero-fill zone. */
  touchPricing?: boolean;
}): { plan: DualMakerQuotePlan; reason: null } | { plan: null; reason: DualMakerRejection } {
  const { up, down } = input;
  const qty = d(input.quantity);
  const perSideCap = d(input.perSideRiskCapUsd);
  const available = d(input.availableCashUsd);
  if (qty.lte(0) || perSideCap.lte(0) || available.lte(0)
    || !Number.isFinite(input.minPairNetRoi) || input.minPairNetRoi < 0
    || !Number.isFinite(input.nowMs) || !Number.isFinite(input.maxBookAgeMs) || input.maxBookAgeMs <= 0) {
    return { plan: null, reason: "INVALID_CONFIGURATION" };
  }
  if (up.marketId !== down.marketId || up.outcome !== "UP" || down.outcome !== "DOWN"
    || up.tokenId === down.tokenId || up.asset !== down.asset) {
    return { plan: null, reason: "MARKET_IDENTITY" };
  }
  const fresh = (book: MarketSnapshot) => {
    const received = Date.parse(book.receivedAt);
    const source = Date.parse(book.sourceTimestamp);
    return Number.isFinite(received) && Number.isFinite(source)
      && Math.abs(received - source) <= input.maxBookAgeMs
      && received <= input.nowMs && source <= input.nowMs
      && input.nowMs - received <= input.maxBookAgeMs
      && input.nowMs - source <= input.maxBookAgeMs;
  };
  if (!fresh(up) || !fresh(down)) return { plan: null, reason: "STALE_OR_GAPPED_BOOK" };
  if (!up.minOrderSize || !down.minOrderSize || qty.lt(up.minOrderSize) || qty.lt(down.minOrderSize)) {
    return { plan: null, reason: "MINIMUM_SIZE" };
  }
  const upPrice = passiveMakerBuyPrice(up, input.quantity, up.bids[0]?.price ?? "0");
  const downPrice = passiveMakerBuyPrice(down, input.quantity, down.bids[0]?.price ?? "0");
  if (!upPrice || !downPrice) return { plan: null, reason: "NON_PASSIVE_QUOTE" };
  // Approach 3 (cost-bound pricing, opt-in): price each leg at the MAXIMUM
  // passive affordable quote - the highest on-tick price strictly below its own
  // best ASK (post-only) such that the worst-case single-fill residual can be
  // hedged by TAKING the opposite ASK at the ROI floor. This may be below the
  // best bid (a less aggressive passive quote) or above it (a price-improving
  // passive quote); both are valid post-only orders filled by exact-price SELL
  // flow. The Engine's post-only admission now accepts any such passive price
  // (passiveMakerLimitPrice), so the old best-bid-equality constraint no longer
  // forces this planner into a 100%-rejection gate.
  //
  // Touch fill-zone pricing (costBound && touchPricing): paper evidence showed
  // that quotes 1-6 ticks BELOW the best bid never fill (0 eligible exact-price
  // SELL trades over hundreds of 2s windows) while join-best-bid quotes fill at
  // ~49% of quotes. The single-fill tail hedge is therefore reported as an AUDIT
  // (worstCaseSingleFill, may be uncovered) and governed by the existing
  // reservation/risk limits and the hedge machinery, not by shaving the quote
  // below the touch where public flow does not exist.
  let costUp = upPrice;
  let costDown = downPrice;
  const touch = input.costBound && input.touchPricing === true;
  if (input.costBound) {
    if (!touch) {
      const upBound = costBoundMakerBuyPrice({ book: up, opposite: down, quantity: input.quantity, minPairNetRoi: input.minPairNetRoi });
      const downBound = costBoundMakerBuyPrice({ book: down, opposite: up, quantity: input.quantity, minPairNetRoi: input.minPairNetRoi });
      if ("reason" in upBound || "reason" in downBound) return { plan: null, reason: "HEDGE_COST_NOT_COVERED" };
      if ("price" in upBound) costUp = upBound.price;
      if ("price" in downBound) costDown = downBound.price;
    }
    // Double-check the produced prices are genuine post-only limits (non-crossing
    // and on-tick). Fail closed rather than trusting the solver.
    if (passiveMakerLimitPrice(up, costUp, input.quantity) === null
      || passiveMakerLimitPrice(down, costDown, input.quantity) === null) {
      return { plan: null, reason: "HEDGE_COST_NOT_COVERED" };
    }
  }
  const upCost = qty.mul(costUp);
  const downCost = qty.mul(costDown);
  if (upCost.gt(perSideCap) || downCost.gt(perSideCap)) return { plan: null, reason: "SIDE_EXPOSURE_CAP" };
  const feeUp = d(calculateFee(input.quantity, costUp, "MAKER", up.feeSchedule!));
  const feeDown = d(calculateFee(input.quantity, costDown, "MAKER", down.feeSchedule!));
  const reserveUp = upCost.plus(feeUp);
  const reserveDown = downCost.plus(feeDown);
  if (reserveUp.plus(reserveDown).gt(available)) return { plan: null, reason: "INSUFFICIENT_CASH" };
  const net = qty.minus(reserveUp).minus(reserveDown);
  const netRoi = net.div(reserveUp.plus(reserveDown)).toNumber();
  if (net.lte(0) || netRoi < input.minPairNetRoi) return { plan: null, reason: "INSUFFICIENT_LOCKED_EDGE" };
  const worstCaseSingleFill = input.costBound ? {
    up: singleFillHedgeEconomics({
      book: up, opposite: down, quantity: input.quantity, price: costUp,
      makerFeeUsd: feeUp.toFixed(), minPairNetRoi: input.minPairNetRoi,
    }),
    down: singleFillHedgeEconomics({
      book: down, opposite: up, quantity: input.quantity, price: costDown,
      makerFeeUsd: feeDown.toFixed(), minPairNetRoi: input.minPairNetRoi,
    }),
  } : undefined;
  // Binding single-fill ROI gate for classic cost-bound pricing. With touch
  // fill-zone pricing the tail is an AUDIT: rejecting here would push the quote
  // below the touch where no exact-price SELL flow exists (0 fills on paper),
  // which is economically worse than reporting the bounded tail.
  if (worstCaseSingleFill && !touch && (!worstCaseSingleFill.up.covered || !worstCaseSingleFill.down.covered)) {
    return { plan: null, reason: "HEDGE_COST_NOT_COVERED" };
  }
  return {
    plan: {
      marketId: up.marketId,
      quantity: qty.toFixed(),
      up: { tokenId: up.tokenId, price: costUp, reserveUsd: reserveUp.toFixed(), makerFeeUsd: feeUp.toFixed() },
      down: { tokenId: down.tokenId, price: costDown, reserveUsd: reserveDown.toFixed(), makerFeeUsd: feeDown.toFixed() },
      combinedReserveUsd: reserveUp.plus(reserveDown).toFixed(),
      netPairPnlUsd: net.toFixed(),
      netPairRoi: netRoi,
      maxUnpairedDownsideUsd: d(reserveUp).gt(reserveDown) ? reserveUp.toFixed() : reserveDown.toFixed(),
      worstCaseSingleFill,
      fillAssumption: "BOTH_QUOTES_FILL_AT_LIMIT",
    },
    reason: null,
  };
}
