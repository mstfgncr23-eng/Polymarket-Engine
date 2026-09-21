import { calculateFee, calculateSlippageUsd, d, orderBookLiquidityUsd, walkBook } from "./calculations.js";
import { createId, type ConsensusDecision, type MarketSnapshot, type PaperOrder, type RiskEvaluation } from "./domain.js";
import type { RiskLimits } from "./config.js";
import { evaluateRisk, type RiskInput } from "./risk.js";
import type { StrategyCandidate } from "./strategy.js";

export interface PaperOrderDecision {
  accepted: boolean;
  order: PaperOrder | null;
  risk: RiskEvaluation;
  /** Canonical expected-value units are USD/share for a $1 binary payout. */
  rawEdgeUsdPerShare: number;
  feeUsdPerShare: string | null;
  slippageUsdPerShare: string | null;
  costUsdPerShare: string | null;
  netEvUsdPerShare: number;
  grossEvUsdTotal: string;
  netEvUsdTotal: string | null;
  executableNotionalUsd: string;
  expectedROI: number | null;
  quantity: string;
  feeUsdTotal: string | null;
  slippageUsdTotal: string | null;
  /** @deprecated Compatibility aliases; all are in the units documented above. */
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
  baseRisk: Omit<RiskInput, "traceId" | "availableLiquidityUsd" | "expectedEdge" | "expectedROI" | "spread" | "signalConfidence" | "signalExpiresAt" | "feeScheduleKnown" | "orderSizeUsd">;
  limits: RiskLimits;
  triggerId: string;
  signalExpiresAt: Date;
  orderType?: "TAKER" | "MAKER";
}): PaperOrderDecision {
  const orderType = input.orderType ?? "TAKER";
  const bestBid = input.snapshotAfterDecisionLatency.bids[0];
  const bestAsk = input.snapshotAfterDecisionLatency.asks[0];
  const makerPrice = input.candidate.side === "BUY"
    ? (bestBid?.price ?? input.candidate.limitPrice)
    : (bestAsk?.price ?? input.candidate.limitPrice);
  const requestedPrice = orderType === "MAKER" ? makerPrice : input.candidate.limitPrice;
  const takerLevels = input.candidate.side === "BUY"
    ? input.snapshotAfterDecisionLatency.asks
    : input.snapshotAfterDecisionLatency.bids;
  const walk = walkBook(takerLevels, input.candidate.quantity, input.candidate.limitPrice, input.candidate.side);
  const schedule = input.snapshotAfterDecisionLatency.feeSchedule;
  const executableNow = orderType === "MAKER"
    ? (input.candidate.side === "BUY" ? bestBid !== undefined : bestAsk !== undefined)
    : walk.sufficientDepth;
  const executionPrice = orderType === "MAKER" ? makerPrice : walk.vwap;
  const fee = schedule ? calculateFee(input.candidate.quantity, executionPrice, orderType, schedule) : null;
  const slippage = executableNow
    ? orderType === "MAKER"
      ? "0"
      : calculateSlippageUsd(input.candidate.limitPrice, walk.vwap, walk.filledQuantity, input.candidate.side)
    : null;
  const notional = orderType === "MAKER" ? d(input.candidate.quantity).mul(makerPrice) : walk.sufficientDepth ? d(walk.notional) : d(input.candidate.quantity).mul(input.candidate.limitPrice);
  const quantity = d(input.candidate.quantity);
  const rawEdgeUsdPerShare = input.candidate.rawEdgeUsdPerShare ?? input.candidate.rawExpectedEdge;
  const feeUsdPerShare = fee ? d(fee).div(quantity).toFixed() : null;
  const slippageUsdPerShare = slippage ? d(slippage).div(quantity).toFixed() : null;
  const costUsdPerShare = feeUsdPerShare !== null && slippageUsdPerShare !== null
    ? d(feeUsdPerShare).plus(slippageUsdPerShare).toFixed()
    : null;
  // Unknown fee/depth inputs remain a hard risk veto. They must not be
  // represented as a notional-relative percentage or silently accepted.
  const netEvUsdPerShare = costUsdPerShare === null
    ? Number.NEGATIVE_INFINITY
    : d(rawEdgeUsdPerShare).minus(costUsdPerShare).toNumber();
  const grossEvUsdTotal = d(rawEdgeUsdPerShare).mul(quantity).toFixed();
  const netEvUsdTotal = Number.isFinite(netEvUsdPerShare) ? d(netEvUsdPerShare).mul(quantity).toFixed() : null;
  const expectedROI = netEvUsdTotal === null ? null : d(netEvUsdTotal).div(notional).toNumber();
  const requiredCashUsd = input.candidate.side === "BUY"
    ? d(notional).plus(fee ?? 0).toFixed()
    : "0";
  const marketSpread = bestBid && bestAsk ? Number(d(bestAsk.price).minus(bestBid.price)) : Number.POSITIVE_INFINITY;
  const risk = evaluateRisk({
    ...input.baseRisk, traceId: input.consensus.traceId,
    availableLiquidityUsd: orderBookLiquidityUsd(input.snapshotAfterDecisionLatency, input.candidate.side),
    expectedEdge: netEvUsdPerShare,
    expectedROI: expectedROI ?? Number.NEGATIVE_INFINITY,
    spread: marketSpread, signalConfidence: input.consensus.consensusConfidence,
    signalExpiresAt: input.signalExpiresAt,
    feeScheduleKnown: schedule !== null, orderSizeUsd: notional.toFixed(),
    requiredCashUsd,
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
  return {
    accepted: risk.accepted, order, risk,
    rawEdgeUsdPerShare, feeUsdPerShare, slippageUsdPerShare, costUsdPerShare,
    netEvUsdPerShare, grossEvUsdTotal, netEvUsdTotal,
    executableNotionalUsd: notional.toFixed(), expectedROI, quantity: quantity.toFixed(),
    feeUsdTotal: fee, slippageUsdTotal: slippage,
    rawEdge: rawEdgeUsdPerShare, feeUsd: fee, slippageUsd: slippage,
    netExpectedEdge: netEvUsdPerShare,
    bookVwap: orderType === "TAKER" && walk.sufficientDepth ? walk.vwap : null,
  };
}


/**
 * Deterministic risk-only admission for an existing long inventory unwind.
 * It deliberately does not manufacture an alpha signal: the protective intent
 * bypasses alpha gates inside evaluateRisk but still obeys hard feed/database/
 * execution safety and must monotonically reduce unhedged exposure.
 */
export function decideProtectivePaperOrder(input: {
  candidate: StrategyCandidate;
  snapshotAfterDecisionLatency: MarketSnapshot;
  baseRisk: Omit<
    RiskInput,
    | "traceId"
    | "availableLiquidityUsd"
    | "expectedEdge"
    | "expectedROI"
    | "spread"
    | "signalConfidence"
    | "signalExpiresAt"
    | "feeScheduleKnown"
    | "orderSizeUsd"
    | "requiredCashUsd"
    | "intent"
  >;
  limits: RiskLimits;
  traceId: string;
  triggerId: string;
}): PaperOrderDecision {
  if (input.candidate.intent !== "PROTECTIVE_DE_RISK" || input.candidate.side !== "SELL") {
    throw new Error("INVALID_PROTECTIVE_DE_RISK_CANDIDATE");
  }

  const snapshot = input.snapshotAfterDecisionLatency;
  const bestBid = snapshot.bids[0];
  const bestAsk = snapshot.asks[0];
  const walk = walkBook(snapshot.bids, input.candidate.quantity, input.candidate.limitPrice, "SELL");
  const schedule = snapshot.feeSchedule;
  const executableNow = walk.sufficientDepth && d(walk.filledQuantity).eq(input.candidate.quantity);
  const fee = schedule && executableNow
    ? calculateFee(input.candidate.quantity, walk.vwap, "TAKER", schedule)
    : null;
  const slippage = executableNow
    ? calculateSlippageUsd(input.candidate.limitPrice, walk.vwap, walk.filledQuantity, "SELL")
    : null;
  const notional = executableNow
    ? d(walk.notional)
    : d(input.candidate.quantity).mul(input.candidate.limitPrice);
  const quantity = d(input.candidate.quantity);
  const feeUsdPerShare = fee ? d(fee).div(quantity).toFixed() : null;
  const slippageUsdPerShare = slippage ? d(slippage).div(quantity).toFixed() : null;
  const costUsdPerShare = feeUsdPerShare !== null && slippageUsdPerShare !== null
    ? d(feeUsdPerShare).plus(slippageUsdPerShare).toFixed()
    : null;
  const netEvUsdPerShare = costUsdPerShare === null ? Number.NEGATIVE_INFINITY : d(costUsdPerShare).negated().toNumber();
  const netEvUsdTotal = Number.isFinite(netEvUsdPerShare)
    ? d(netEvUsdPerShare).mul(quantity).toFixed()
    : null;
  const spread = bestBid && bestAsk ? Number(d(bestAsk.price).minus(bestBid.price)) : Number.POSITIVE_INFINITY;

  const risk = evaluateRisk({
    ...input.baseRisk,
    traceId: input.traceId,
    intent: "PROTECTIVE_DE_RISK",
    availableLiquidityUsd: orderBookLiquidityUsd(snapshot, "SELL"),
    expectedEdge: 0,
    expectedROI: 0,
    spread,
    signalConfidence: 1,
    signalExpiresAt: new Date(input.baseRisk.now.getTime() + 60_000),
    feeScheduleKnown: schedule !== null,
    orderSizeUsd: notional.toFixed(),
    requiredCashUsd: "0",
  }, input.limits);

  if (!executableNow && !risk.vetoes.includes("INSUFFICIENT_DEPTH")) {
    risk.vetoes.push("INSUFFICIENT_DEPTH");
    risk.accepted = false;
  }

  const order: PaperOrder | null = risk.accepted ? {
    id: createId("order"),
    traceId: input.traceId,
    triggerId: input.triggerId,
    strategyId: input.candidate.strategyId,
    marketId: input.candidate.marketId,
    tokenId: input.candidate.tokenId,
    outcome: input.candidate.outcome,
    side: "SELL",
    orderType: "TAKER",
    requestedPrice: input.candidate.limitPrice,
    quantity: input.candidate.quantity,
    status: "PENDING",
    createdAt: input.baseRisk.now.toISOString(),
  } : null;

  return {
    accepted: risk.accepted,
    order,
    risk,
    rawEdgeUsdPerShare: 0,
    feeUsdPerShare,
    slippageUsdPerShare,
    costUsdPerShare,
    netEvUsdPerShare,
    grossEvUsdTotal: "0",
    netEvUsdTotal,
    executableNotionalUsd: notional.toFixed(),
    expectedROI: 0,
    quantity: quantity.toFixed(),
    feeUsdTotal: fee,
    slippageUsdTotal: slippage,
    rawEdge: 0,
    feeUsd: fee,
    slippageUsd: slippage,
    netExpectedEdge: netEvUsdPerShare,
    bookVwap: executableNow ? walk.vwap : null,
  };
}
