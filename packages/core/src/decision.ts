import { calculateFee, calculateSlippageUsd, d, orderBookLiquidityUsd, walkBook } from "./calculations.js";
import { createId, type ConsensusDecision, type MarketSnapshot, type PaperOrder, type RiskEvaluation } from "./domain.js";
import type { RiskLimits } from "./config.js";
import { evaluateRisk, type RiskInput } from "./risk.js";
import type { StrategyCandidate } from "./strategy.js";

export interface PaperOrderDecision {
  accepted: boolean;
  order: PaperOrder | null;
  risk: RiskEvaluation;
  rawEdge: number;
  feeUsd: string | null;
  slippageUsd: string | null;
  netExpectedEdge: number;
  bookVwap: string | null;
}

export function decidePaperOrder(input: {
  candidate: StrategyCandidate;
  consensus: ConsensusDecision;
  snapshotAfterDecisionLatency: MarketSnapshot;
  baseRisk: Omit<RiskInput, "traceId" | "availableLiquidityUsd" | "expectedEdge" | "spread" | "signalConfidence" | "signalExpiresAt" | "feeScheduleKnown" | "orderSizeUsd">;
  limits: RiskLimits;
  triggerId: string;
  signalExpiresAt: Date;
  orderType?: "TAKER" | "MAKER";
}): PaperOrderDecision {
  const orderType = input.orderType ?? "TAKER";
  const bestBid = input.snapshotAfterDecisionLatency.bids[0];
  const bestAsk = input.snapshotAfterDecisionLatency.asks[0];
  const makerPrice = bestBid?.price ?? input.candidate.limitPrice;
  const requestedPrice = orderType === "MAKER" ? makerPrice : input.candidate.limitPrice;
  const walk = walkBook(input.snapshotAfterDecisionLatency.asks, input.candidate.quantity, input.candidate.limitPrice);
  const schedule = input.snapshotAfterDecisionLatency.feeSchedule;
  const executableNow = orderType === "MAKER" ? bestBid !== undefined : walk.sufficientDepth;
  const executionPrice = orderType === "MAKER" ? makerPrice : walk.vwap;
  const fee = executableNow && schedule ? calculateFee(input.candidate.quantity, executionPrice, orderType, schedule) : null;
  const slippage = executableNow ? orderType === "MAKER" ? "0" : calculateSlippageUsd(input.candidate.limitPrice, walk.vwap, walk.filledQuantity) : null;
  const notional = orderType === "MAKER" ? d(input.candidate.quantity).mul(makerPrice) : walk.sufficientDepth ? d(walk.notional) : d(input.candidate.quantity).mul(input.candidate.limitPrice);
  const netEdge = input.candidate.rawExpectedEdge
    - (fee ? d(fee).div(notional).toNumber() : 1)
    - (slippage ? d(slippage).div(notional).toNumber() : 1);
  const marketSpread = bestBid && bestAsk ? Number(d(bestAsk.price).minus(bestBid.price)) : Number.POSITIVE_INFINITY;
  const risk = evaluateRisk({
    ...input.baseRisk, traceId: input.consensus.traceId,
    availableLiquidityUsd: orderBookLiquidityUsd(input.snapshotAfterDecisionLatency, "BUY"),
    expectedEdge: netEdge, spread: marketSpread, signalConfidence: input.consensus.consensusConfidence,
    signalExpiresAt: input.signalExpiresAt,
    feeScheduleKnown: schedule !== null, orderSizeUsd: notional.toFixed(),
  }, input.limits);
  if (!executableNow && !risk.vetoes.includes("INSUFFICIENT_DEPTH")) {
    risk.vetoes.push("INSUFFICIENT_DEPTH");
    risk.accepted = false;
  }
  const order: PaperOrder | null = risk.accepted ? {
    id: createId("order"), traceId: input.consensus.traceId, triggerId: input.triggerId,
    strategyId: input.candidate.strategyId, marketId: input.candidate.marketId, tokenId: input.candidate.tokenId,
    outcome: input.candidate.outcome, side: input.candidate.side, orderType,
    requestedPrice, quantity: input.candidate.quantity, status: "PENDING",
    createdAt: input.baseRisk.now.toISOString(),
  } : null;
  return { accepted: risk.accepted, order, risk, rawEdge: input.candidate.rawExpectedEdge, feeUsd: fee, slippageUsd: slippage, netExpectedEdge: netEdge, bookVwap: orderType === "TAKER" && walk.sufficientDepth ? walk.vwap : null };
}
