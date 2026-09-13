import { z } from "zod";
import { calculateFee, calculateSlippageUsd, d, decimal, walkBook } from "./calculations.js";
import { createId, decimalStringSchema, paperOrderSchema, timestampSchema, type MarketSnapshot, type PaperFill, type PaperOrder } from "./domain.js";

export interface TakerExecutionInput {
  order: PaperOrder;
  snapshotAfterLatency: MarketSnapshot;
  measuredLatencyMs: number;
}

export function executePaperTaker(input: TakerExecutionInput): PaperFill {
  if (input.order.orderType !== "TAKER") throw new Error("ORDER_IS_NOT_TAKER");
  if (!input.snapshotAfterLatency.feeSchedule) throw new Error("UNKNOWN_FEE_SCHEDULE");
  const levels = input.order.side === "BUY" ? input.snapshotAfterLatency.asks : input.snapshotAfterLatency.bids;
  const walk = walkBook(levels, input.order.quantity, input.order.requestedPrice);
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
  astraFirstLegDecision?: AstraFirstLegDecision;
}

export const astraFirstLegDecisionSchema = z.object({
  decisionId: z.string().min(1).max(160),
  triggerId: z.string().min(1).max(160),
  fairProbabilityUp: z.number().min(0).max(1),
}).strict();
export type AstraFirstLegDecision = z.infer<typeof astraFirstLegDecisionSchema>;

export const restingPaperOrderSchema = z.object({
  order: paperOrderSchema,
  queueAhead: decimalStringSchema,
  remaining: decimalStringSchema,
  observedAt: timestampSchema,
  astraFirstLegDecision: astraFirstLegDecisionSchema.optional(),
});

export function createRestingMakerOrder(order: PaperOrder, snapshot: MarketSnapshot): RestingPaperOrder {
  if (order.orderType !== "MAKER") throw new Error("ORDER_IS_NOT_MAKER");
  const sameSide = order.side === "BUY" ? snapshot.bids : snapshot.asks;
  const atPrice = sameSide.find((level) => d(level.price).eq(order.requestedPrice));
  return { order: { ...order, status: "RESTING" }, queueAhead: atPrice?.quantity ?? "0", remaining: order.quantity, observedAt: snapshot.receivedAt };
}

export function evolveMakerOrder(
  resting: RestingPaperOrder,
  previous: MarketSnapshot,
  next: MarketSnapshot,
): { resting: RestingPaperOrder; fill: PaperFill | null } {
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
