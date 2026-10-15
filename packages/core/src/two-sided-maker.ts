import { calculateFee, d } from "./calculations.js";
import type { MarketSnapshot } from "./domain.js";
import { passiveMakerBuyPrice } from "./maker-first.js";

export type DualMakerRejection =
  | "INVALID_CONFIGURATION"
  | "MARKET_IDENTITY"
  | "STALE_OR_GAPPED_BOOK"
  | "NON_PASSIVE_QUOTE"
  | "MINIMUM_SIZE"
  | "SIDE_EXPOSURE_CAP"
  | "INSUFFICIENT_CASH"
  | "INSUFFICIENT_LOCKED_EDGE";

export type DualMakerQuotePlan = {
  marketId: string;
  quantity: string;
  up: { tokenId: string; price: string; reserveUsd: string; makerFeeUsd: string };
  down: { tokenId: string; price: string; reserveUsd: string; makerFeeUsd: string };
  combinedReserveUsd: string;
  netPairPnlUsd: string;
  netPairRoi: number;
  maxUnpairedDownsideUsd: string;
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
  const upCost = qty.mul(upPrice);
  const downCost = qty.mul(downPrice);
  if (upCost.gt(perSideCap) || downCost.gt(perSideCap)) return { plan: null, reason: "SIDE_EXPOSURE_CAP" };
  const feeUp = d(calculateFee(input.quantity, upPrice, "MAKER", up.feeSchedule!));
  const feeDown = d(calculateFee(input.quantity, downPrice, "MAKER", down.feeSchedule!));
  const reserveUp = upCost.plus(feeUp);
  const reserveDown = downCost.plus(feeDown);
  if (reserveUp.plus(reserveDown).gt(available)) return { plan: null, reason: "INSUFFICIENT_CASH" };
  const net = qty.minus(reserveUp).minus(reserveDown);
  const netRoi = net.div(reserveUp.plus(reserveDown)).toNumber();
  if (net.lte(0) || netRoi < input.minPairNetRoi) return { plan: null, reason: "INSUFFICIENT_LOCKED_EDGE" };
  return {
    plan: {
      marketId: up.marketId,
      quantity: qty.toFixed(),
      up: { tokenId: up.tokenId, price: upPrice, reserveUsd: reserveUp.toFixed(), makerFeeUsd: feeUp.toFixed() },
      down: { tokenId: down.tokenId, price: downPrice, reserveUsd: reserveDown.toFixed(), makerFeeUsd: feeDown.toFixed() },
      combinedReserveUsd: reserveUp.plus(reserveDown).toFixed(),
      netPairPnlUsd: net.toFixed(),
      netPairRoi: netRoi,
      maxUnpairedDownsideUsd: d(reserveUp).gt(reserveDown) ? reserveUp.toFixed() : reserveDown.toFixed(),
      fillAssumption: "BOTH_QUOTES_FILL_AT_LIMIT",
    },
    reason: null,
  };
}
