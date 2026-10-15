import { d, calculateFee } from "./calculations.js";
import type { MarketSnapshot } from "./domain.js";

/** Join the actual best bid. Unknown tick/minimum, crossed or off-tick books
 * do not create a passive quote. Both outcomes are never fresh entries. */
export function passiveMakerBuyPrice(book: MarketSnapshot, quantity: string, limit: string): string | null {
  const bid=book.bids[0], ask=book.asks[0];
  if(!bid || !ask || !book.tickSize || !book.minOrderSize || !book.feeSchedule)return null;
  const p=d(bid.price), tick=d(book.tickSize);
  if(tick.lte(0)||p.lte(0)||p.gte(1)||p.gte(ask.price)||p.gt(limit)
    ||!p.mod(tick).eq(0)||d(ask.price).minus(p).lt(tick)||d(quantity).lt(book.minOrderSize))return null;
  return p.toFixed();
}

export function makerPairNetPnl(input: { quantity:string; firstLegPrice:string; oppositeLegPrice:string; firstLegFees:string; oppositeFee:string; otherExecutionCosts?:string }) {
  return d(input.quantity).mul(d(1).minus(input.firstLegPrice).minus(input.oppositeLegPrice))
    .minus(input.firstLegFees).minus(input.oppositeFee).minus(input.otherExecutionCosts??0).toFixed();
}

export function expectedMakerFee(quantity:string,price:string,book:MarketSnapshot):string|null {
  return book.feeSchedule ? calculateFee(quantity,price,"MAKER",book.feeSchedule):null;
}
