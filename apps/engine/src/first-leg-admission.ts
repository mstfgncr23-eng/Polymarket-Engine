import { calculateFee, calculateSlippageUsd, d, walkBook, type MarketSnapshot, type PaperOrder } from "@pm/core";
import type { DiscoveredMarket } from "@pm/market-data";

export const FIRST_LEG_MIN_PRICE = 0.65;
export const FIRST_LEG_MIN_NET_ROI = 0.015;

export type EntryEconomicPolicy = { minPrice: number; minNetRoi: number; reserveExitFee?: boolean };
export function firstLegEntryFilterReason(price: string | number | null | undefined, expectedROI: number | null | undefined,
  policy: EntryEconomicPolicy = { minPrice: FIRST_LEG_MIN_PRICE, minNetRoi: FIRST_LEG_MIN_NET_ROI }) {
  try {
    if (price === null || price === undefined || !d(price).isFinite() || d(price).lte(0) || d(price).gte(1) || d(price).lt(policy.minPrice)) return "FIRST_LEG_PRICE_BELOW_FLOOR" as const;
  } catch { return "FIRST_LEG_PRICE_BELOW_FLOOR" as const; }
  return typeof expectedROI !== "number" || !Number.isFinite(expectedROI) || expectedROI < policy.minNetRoi
    ? "FIRST_LEG_NET_ROI_BELOW_FLOOR" as const : null;
}

/** Same net EV / executable notional semantics as decidePaperOrder. */
export function firstLegExecutionEconomics(order: PaperOrder, book: MarketSnapshot, nowMs: number, policy?: EntryEconomicPolicy) {
  const proof = order.entryAdmission;
  if (!proof || !Number.isFinite(proof.fairProbability) || !Number.isFinite(nowMs)
    || !Number.isFinite(Date.parse(proof.signalExpiresAt)) || nowMs >= Date.parse(proof.signalExpiresAt))
    return { reason: "ENTRY_ECONOMICS_UNAVAILABLE", price: null, expectedROI: null };
  const exitFeeReserve = policy?.reserveExitFee && book.feeSchedule
    ? calculateFee(order.quantity, "0.5", "TAKER", book.feeSchedule) : "0";
  const walk = walkBook(book.asks, order.quantity, order.requestedPrice, "BUY");
  if(order.makerFirst){
    const price=order.requestedPrice;
    if(order.orderType!=="MAKER"||!book.feeSchedule||!book.asks[0]||d(price).gte(book.asks[0].price))return {reason:"ENTRY_ECONOMICS_UNAVAILABLE",price,expectedROI:null};
    const fee=calculateFee(order.quantity,price,"MAKER",book.feeSchedule);
    const expectedROI=d(proof.fairProbability).minus(price).mul(order.quantity).minus(fee).minus(exitFeeReserve).div(d(order.quantity).mul(price)).toNumber();
    return {reason:firstLegEntryFilterReason(price,expectedROI,policy),price,expectedROI};
  }
  const price = order.orderType === "MAKER" ? order.requestedPrice : walk.sufficientDepth ? walk.vwap : null;
  if (!price || !walk.sufficientDepth || !book.feeSchedule) return { reason: "ENTRY_ECONOMICS_UNAVAILABLE", price, expectedROI: null };
  const fee = calculateFee(order.quantity, price, order.orderType, book.feeSchedule);
  const slippage = order.orderType === "MAKER" ? "0" : calculateSlippageUsd(order.requestedPrice, price, order.quantity, "BUY");
  const notional = d(order.quantity).mul(price);
  const roi = d(proof.fairProbability).minus(walk.vwap).mul(order.quantity).minus(fee).minus(exitFeeReserve).minus(slippage).div(notional).toNumber();
  return { reason: firstLegEntryFilterReason(price, roi, policy), price, expectedROI: roi };
}

/** Gamma eventStartTime is the fixing start; startDate is listing creation. */
export function officialMarketStartReason(market: Pick<DiscoveredMarket, "slug" | "endDate" | "raw"> | undefined, nowMs: number): string | null {
  if (!market) return "ENTRY_MARKET_START_MISSING";
  const raw = market.raw;
  const event = raw.event && typeof raw.event === "object" ? raw.event as Record<string, unknown> : {};
  const eventSchedule = event.schedule && typeof event.schedule === "object" ? event.schedule as Record<string, unknown> : {};
  const values = [raw.eventStartTime, event.eventStartTime, eventSchedule.startTime].filter(x => x !== undefined && x !== null);
  const parse = (x: unknown) => typeof x === "string" && /T.*(?:Z|[+-]\d\d:\d\d)$/.test(x) ? Date.parse(x) : NaN;
  const starts = values.map(parse);
  if (!starts.length || starts.some(x => !Number.isFinite(x))) return "ENTRY_MARKET_START_MISSING";
  const start = starts[0]!;
  if (starts.some(x => x !== start)) return "ENTRY_MARKET_START_CONFLICT";
  const end = market.endDate ? Date.parse(market.endDate) : NaN;
  if (!Number.isFinite(end) || end <= start) return "ENTRY_MARKET_START_CONFLICT";
  const slug = market.slug.match(/-updown-(\d+)([mh])-(\d{10})$/i);
  if (slug) {
    const slugStart = Number(slug[3]) * 1000;
    const duration = Number(slug[1]) * (slug[2]!.toLowerCase() === "h" ? 3600000 : 60000);
    if (start !== slugStart || end !== start + duration) return "ENTRY_MARKET_START_CONFLICT";
  }
  return Number.isFinite(nowMs) && nowMs >= start ? null : "ENTRY_MARKET_NOT_STARTED";
}

export function bookTiming(book: MarketSnapshot, nowMs: number) {
  const source = Date.parse(book.sourceTimestamp);
  const received = Date.parse(book.receivedAt);
  const ingress = book.wsCallbackAt === undefined ? null : Date.parse(book.wsCallbackAt);
  return { sourceAgeMs: nowMs - source, receivedAgeMs: nowMs - received,
    sourceToIngressMs: ingress === null ? null : ingress - source,
    ingressToReceivedMs: ingress === null ? null : received - ingress,
    receivedToDecisionMs: nowMs - received };
}

export function firstLegBookReason(input: {
  marketId: string; tokenId: string; outcome: "UP" | "DOWN";
  canonicalTokenId: string | null | undefined; synchronized: boolean;
  book: MarketSnapshot | null | undefined; expected?: MarketSnapshot;
  nowMs: number; maxAgeMs: number;
}): string | null {
  const b = input.book;
  if (!b || !input.canonicalTokenId || !input.synchronized) return "ENTRY_BOOK_UNAVAILABLE";
  if (b.marketId !== input.marketId || b.tokenId !== input.tokenId || b.outcome !== input.outcome
    || b.tokenId !== input.canonicalTokenId) return "ENTRY_BOOK_IDENTITY_CHANGED";
  if (!b.eventId || !Number.isInteger(b.sequence) || b.sequence < 0) return "ENTRY_BOOK_PROVENANCE_MISSING";
  if (input.expected && (b.eventId !== input.expected.eventId || b.sequence !== input.expected.sequence
    || b.hash !== input.expected.hash || b.sourceTimestamp !== input.expected.sourceTimestamp
    || b.receivedAt !== input.expected.receivedAt)) return "ENTRY_BOOK_CHANGED";
  const timing = bookTiming(b, input.nowMs);
  if (!Number.isFinite(input.nowMs) || !Number.isFinite(timing.sourceAgeMs)
    || !Number.isFinite(timing.receivedAgeMs)) return "ENTRY_BOOK_TIMESTAMP_INVALID";
  if (timing.sourceAgeMs < 0 || timing.receivedAgeMs < 0) return "ENTRY_BOOK_FUTURE";
  if (timing.sourceAgeMs > input.maxAgeMs) return "ENTRY_BOOK_SOURCE_STALE";
  if (timing.receivedAgeMs > input.maxAgeMs) return "ENTRY_BOOK_LOCAL_STALE";
  if (b.wsCallbackAt !== undefined && (!Number.isFinite(Date.parse(b.wsCallbackAt))
    || Date.parse(b.wsCallbackAt) > Date.parse(b.receivedAt)
    || input.nowMs - Date.parse(b.wsCallbackAt) > input.maxAgeMs)) return "ENTRY_BOOK_LOCAL_BACKLOG";
  return null;
}
