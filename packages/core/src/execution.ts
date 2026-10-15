import { z } from "zod";
import { calculateFee, calculateSlippageUsd, d, decimal, walkBook } from "./calculations.js";
import { createId, decimalStringSchema, paperOrderSchema, timestampSchema, type MarketSnapshot, type MarketTrade, type PaperFill, type PaperOrder } from "./domain.js";
import { passiveMakerBuyPrice } from "./maker-first.js";

export interface TakerExecutionInput {
  order: PaperOrder;
  snapshotAfterLatency: MarketSnapshot;
  measuredLatencyMs: number;
}

export function executePaperTaker(input: TakerExecutionInput): PaperFill {
  if (input.order.orderType !== "TAKER") throw new Error("ORDER_IS_NOT_TAKER");
  if (!input.snapshotAfterLatency.feeSchedule) throw new Error("UNKNOWN_FEE_SCHEDULE");
  const levels = input.order.side === "BUY" ? input.snapshotAfterLatency.asks : input.snapshotAfterLatency.bids;
  const walk = walkBook(levels, input.order.quantity, input.order.requestedPrice, input.order.side);
  if (!walk.sufficientDepth || d(walk.filledQuantity).lt(input.order.quantity)) throw new Error("INSUFFICIENT_DEPTH");
  const fee = calculateFee(walk.filledQuantity, walk.vwap, "TAKER", input.snapshotAfterLatency.feeSchedule);
  return {
    id: createId("fill"), orderId: input.order.id, traceId: input.order.traceId,
    marketId: input.order.marketId, tokenId: input.order.tokenId, outcome: input.order.outcome,
    side: input.order.side, requestedPrice: input.order.requestedPrice, fillPrice: walk.vwap,
    quantity: walk.filledQuantity, liquidityRole: "TAKER", latencyMs: input.measuredLatencyMs,
    slippageUsd: calculateSlippageUsd(input.order.requestedPrice, walk.vwap, walk.filledQuantity, input.order.side),
    feeUsd: fee, timestamp: input.snapshotAfterLatency.receivedAt,
    sourceMarketEventId: input.snapshotAfterLatency.eventId, fillConfidence: 1,
    fillModel: "paper-taker-v2:latency-snapshot-depth-walk-vwap",
    feeSchedule: input.snapshotAfterLatency.feeSchedule,
  };
}

export interface RestingPaperOrder {
  order: PaperOrder;
  queueAhead: string;
  remaining: string;
  observedAt: string;
  lastTradeAt?: string;
  /** Strategy lifecycle intent. Optional only for pre-PR1 checkpoints. */
  intent?: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG";
}

export const restingPaperOrderSchema = z.object({
  order: paperOrderSchema,
  queueAhead: decimalStringSchema,
  remaining: decimalStringSchema,
  observedAt: timestampSchema,
  lastTradeAt: timestampSchema.optional(),
  intent: z.enum(["OPEN_FIRST_LEG", "HEDGE_SECOND_LEG"]).optional(),
});

export function createRestingMakerOrder(
  order: PaperOrder,
  snapshot: MarketSnapshot,
  intent?: RestingPaperOrder["intent"],
): RestingPaperOrder {
  if (order.orderType !== "MAKER") throw new Error("ORDER_IS_NOT_MAKER");
  if (order.makerFirst && (order.side!=="BUY" || passiveMakerBuyPrice(snapshot,order.quantity,order.requestedPrice)!==order.requestedPrice)) throw new Error("MAKER_FIRST_POST_ONLY_INVALID");
  const sameSide = order.side === "BUY" ? snapshot.bids : snapshot.asks;
  const atPrice = sameSide.find((level) => d(level.price).eq(order.requestedPrice));
  return {
    order: { ...order, status: "RESTING" },
    queueAhead: atPrice?.quantity ?? "0",
    remaining: order.quantity,
    observedAt: snapshot.receivedAt,
    ...(intent ? { intent } : {}),
  };
}

export function evolveMakerOrder(
  resting: RestingPaperOrder,
  previous: MarketSnapshot,
  next: MarketSnapshot,
): { resting: RestingPaperOrder; fill: PaperFill | null } {
  // Hybrid quotes NEVER infer executions from cancellation/book-volume changes.
  if(resting.order.makerFirst)return {resting,fill:null};
  if (next.receivedAt <= resting.observedAt || next.sequence <= previous.sequence) return { resting, fill: null };
  const levelsBefore = resting.order.side === "BUY" ? previous.bids : previous.asks;
  const levelsAfter = resting.order.side === "BUY" ? next.bids : next.asks;
  const before = d(levelsBefore.find((level) => d(level.price).eq(resting.order.requestedPrice))?.quantity ?? 0);
  const after = d(levelsAfter.find((level) => d(level.price).eq(resting.order.requestedPrice))?.quantity ?? 0);
  const observedConsumption = DecimalMaxZero(before.minus(after));
  let queue = d(resting.queueAhead);
  const queueConsumed = DecimalMin(queue, observedConsumption);
  queue = queue.minus(queueConsumed);
  const eligible = observedConsumption.minus(queueConsumed);
  const fillQuantity = DecimalMin(d(resting.remaining), eligible.mul(0.5));
  const evolved: RestingPaperOrder = { ...resting, queueAhead: decimal(queue), remaining: decimal(d(resting.remaining).minus(fillQuantity)), observedAt: next.receivedAt };
  if (fillQuantity.lte(0) || !next.feeSchedule) return { resting: evolved, fill: null };
  const fill: PaperFill = {
    id: createId("fill"), orderId: resting.order.id, traceId: resting.order.traceId,
    marketId: resting.order.marketId, tokenId: resting.order.tokenId, outcome: resting.order.outcome,
    side: resting.order.side, requestedPrice: resting.order.requestedPrice,
    fillPrice: resting.order.requestedPrice, quantity: decimal(fillQuantity), liquidityRole: "MAKER",
    latencyMs: Math.max(0, new Date(next.receivedAt).getTime() - new Date(resting.observedAt).getTime()),
    slippageUsd: "0", feeUsd: calculateFee(fillQuantity, resting.order.requestedPrice, "MAKER", next.feeSchedule),
    timestamp: next.receivedAt, sourceMarketEventId: next.eventId, fillConfidence: 0.5,
    fillModel: "paper-maker-v2:queue-ahead-then-50pct-conservative-consumption",
    feeSchedule: next.feeSchedule,
  };
  return { resting: evolved, fill };
}

/** Conservative public-flow PAPER model, not proof of an account execution.
 * Exact-price aggressor flow consumes queue ahead; only 50% of excess can
 * fill us. Timestamp monotonicity deliberately undercounts ambiguous repeats.
 */
export function evolveMakerTrade(resting:RestingPaperOrder,trade:MarketTrade,book:MarketSnapshot,activatedAt:string,nowMs:number,maxFillQuantity?:string):{resting:RestingPaperOrder;fill:PaperFill|null}{
  const q=resting.order.makerFirst;
  if(!q || trade.tokenId!==resting.order.tokenId || trade.marketId!==resting.order.marketId
    || trade.outcome!==resting.order.outcome || trade.tokenEpoch!==q.tokenEpoch
    || trade.side!=="SELL" || !d(trade.price).eq(resting.order.requestedPrice)
    || d(trade.quantity).lte(0) || Date.parse(trade.sourceTimestamp)<=Date.parse(activatedAt)
    || Date.parse(trade.sourceTimestamp)<=Date.parse(resting.lastTradeAt??activatedAt)
    || Date.parse(trade.sourceTimestamp)>nowMs || Date.parse(trade.receivedAt)>nowMs
    || nowMs>=Date.parse(q.expiresAt) || !book.feeSchedule)return {resting,fill:null};
  const queue=d(resting.queueAhead),consumed=DecimalMin(queue,d(trade.quantity));
  const eligible=d(trade.quantity).minus(consumed).mul(.5);
  // A pre-posted dual sibling can be larger than the first actual partial fill.
  // Cap execution, not the public trade evidence or the durable order remainder.
  const capacity=maxFillQuantity===undefined?d(resting.remaining):DecimalMin(d(resting.remaining),DecimalMaxZero(d(maxFillQuantity)));
  const quantity=DecimalMin(capacity,eligible);
  const next={...resting,queueAhead:queue.minus(consumed).toFixed(),lastTradeAt:trade.sourceTimestamp,
    observedAt:trade.receivedAt,remaining:d(resting.remaining).minus(quantity).toFixed()};
  if(quantity.lte(0))return {resting:next,fill:null};
  return {resting:next,fill:{id:`maker-fill:${resting.order.id}:${trade.eventId}`,orderId:resting.order.id,traceId:resting.order.traceId,
    marketId:trade.marketId,tokenId:trade.tokenId,outcome:trade.outcome,side:"BUY",requestedPrice:resting.order.requestedPrice,
    fillPrice:resting.order.requestedPrice,quantity:quantity.toFixed(),liquidityRole:"MAKER",
    latencyMs:Math.max(0,Date.parse(trade.receivedAt)-Date.parse(activatedAt)),slippageUsd:"0",
    feeUsd:calculateFee(quantity,resting.order.requestedPrice,"MAKER",book.feeSchedule),timestamp:trade.receivedAt,
    sourceMarketEventId:trade.eventId,fillConfidence:.5,fillModel:"paper-maker-first-v1:public-trade-queue-50pct",
    feeSchedule:book.feeSchedule,makerEvidence:{trade,activatedAt,queueAheadBefore:resting.queueAhead,
      queueAheadAfter:next.queueAhead,participation:.5,quoteExpiresAt:q.expiresAt}}};
}

function DecimalMin(a: ReturnType<typeof d>, b: ReturnType<typeof d>) { return a.lt(b) ? a : b; }
function DecimalMaxZero(value: ReturnType<typeof d>) { return value.lt(0) ? d(0) : value; }

export interface ExecutionAdapter {
  submit(order: PaperOrder, snapshot: MarketSnapshot): Promise<PaperFill | RestingPaperOrder>;
}

export class LiveExecutionDisabledAdapter implements ExecutionAdapter {
  async submit(): Promise<never> {
    throw new Error("LIVE_TRADING_DISABLED");
  }
}
