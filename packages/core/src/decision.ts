import { calculateFee, calculateSlippageUsd, d, orderBookLiquidityUsd, walkBook } from "./calculations.js";
import { createId, type ConsensusDecision, type MarketSnapshot, type PaperOrder, type RiskEvaluation } from "./domain.js";
import type { RiskLimits } from "./config.js";
import { evaluateRisk, type HedgePositionFacts, type RiskInput } from "./risk.js";
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
  /**
   * Post-latency executable price used for fresh first-leg regime selection
   * (full-quantity book-walk VWAP for TAKER; maker execution price for MAKER).
   */
  executablePrice: string | null;
  /**
   * Deterministic guaranteed terminal value of the executed action, excluding
   * any sunk entry cost and any second slippage subtraction: VWAP already
   * includes depth execution economics, so "slippage" stays telemetry-only.
   *  - PROTECTIVE_DE_RISK (SELL): q * sellVwap - fee
   *  - RESCUE_SECOND_LEG (BUY):  q * 1.00 - q * buyVwap - fee
   *  - other intents: null (not meaningful for fresh directional trades).
   */
  guaranteedSalvageUsd: string | null;
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
// Post-latency snapshot identity fence: the selected snapshot must belong to
  // the exact market/token/outcome being traded. A snapshot for another token
  // (or another market/outcome) must fail closed, never price the order.
  const snapshotMatchesCandidate = input.snapshotAfterDecisionLatency.marketId === input.candidate.marketId
    && input.snapshotAfterDecisionLatency.tokenId === input.candidate.tokenId
    && input.snapshotAfterDecisionLatency.outcome === input.candidate.outcome;
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
  // Price-regime input: the CURRENT post-latency executable price for the full
  // candidate quantity. TAKER uses the book-walk VWAP; MAKER uses the maker
  // execution price (existing decidePaperOrder semantics). Never the top ask
  // alone, never the stale decision-time limitPrice, and null when the full
  // quantity is not executable — the strict regime must fail closed.
  const executablePriceForRegime = orderType === "MAKER"
    ? (input.candidate.side === "BUY" ? (bestBid?.price ?? null) : (bestAsk?.price ?? null))
    : (walk.sufficientDepth ? walk.vwap : null);
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
    ...(input.candidate.intent === "OPEN_FIRST_LEG"
      ? { executablePrice: executablePriceForRegime }
      : {}),
    ...(input.candidate.intent === "HEDGE_SECOND_LEG"
      ? {
        hedgeCandidate: {
          marketId: input.candidate.marketId,
          tokenId: input.candidate.tokenId,
          outcome: input.candidate.outcome,
          side: input.candidate.side,
          quantity: input.candidate.quantity,
        },
      }
      : {}),
  }, input.limits);
  if (!snapshotMatchesCandidate && !risk.vetoes.includes("SNAPSHOT_TOKEN_MISMATCH")) {
    risk.vetoes.push("SNAPSHOT_TOKEN_MISMATCH");
    risk.accepted = false;
  }
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
    executablePrice: executablePriceForRegime,
    guaranteedSalvageUsd: null,
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
    executablePrice: null,
    guaranteedSalvageUsd: executableNow ? d(walk.notional).minus(fee ?? 0).toFixed() : null,
  };
}

/** Output of a controlled-loss rescue decision; adds fee-inclusive cash needs. */
export interface RescueOrderDecision extends PaperOrderDecision {
  /** Fee-inclusive cash required when the BUY is executable; "0" when not. */
  requiredCashUsd: string;
}

/**
 * Deterministic risk-only admission for a controlled-loss pair-lock
 * (RESCUE_SECOND_LEG): BUY the opposite outcome against the CURRENT unpaired
 * residual. TAKER-only, executes on the ask ladder, and may intentionally lock
 * a small loss (each paired share guarantees a $1 payout). It deliberately does
 * not manufacture an alpha signal — inside evaluateRisk the rescue intent
 * bypasses alpha/reference gates ONLY when verifiedRescuePairLock passes and
 * still obeys all hard feed/database/execution safety and exposure limits.
 */
export function decideRescueSecondLegOrder(input: {
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
  rescuePosition: HedgePositionFacts | null;
}): RescueOrderDecision {
  if (input.candidate.intent !== "RESCUE_SECOND_LEG" || input.candidate.side !== "BUY") {
    throw new Error("INVALID_RESCUE_SECOND_LEG_CANDIDATE");
  }

  const snapshot = input.snapshotAfterDecisionLatency;
  const bestBid = snapshot.bids[0];
  const bestAsk = snapshot.asks[0];
  const walk = walkBook(snapshot.asks, input.candidate.quantity, input.candidate.limitPrice, "BUY");
  const schedule = snapshot.feeSchedule;
  const executableNow = walk.sufficientDepth && d(walk.filledQuantity).eq(input.candidate.quantity);
  const fee = schedule && executableNow
    ? calculateFee(input.candidate.quantity, walk.vwap, "TAKER", schedule)
    : null;
  const slippage = executableNow
    ? calculateSlippageUsd(input.candidate.limitPrice, walk.vwap, walk.filledQuantity, "BUY")
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
  // Guaranteed terminal salvage per share: $1 payout - buy VWAP - fee/share.
  // VWAP already embeds real depth execution economics; slippage is telemetry.
  const salvagePerShare = executableNow
    ? d("1").minus(d(walk.vwap)).minus(fee ? d(fee).div(quantity) : d(0))
    : null;
  const netEvUsdPerShare = salvagePerShare ? salvagePerShare.toNumber() : Number.NEGATIVE_INFINITY;
  const netEvUsdTotal = Number.isFinite(netEvUsdPerShare) ? salvagePerShare!.mul(quantity).toFixed() : null;
  const requiredCashUsd = executableNow ? d(notional).plus(fee ?? 0).toFixed() : "0";
  const spread = bestBid && bestAsk ? Number(d(bestAsk.price).minus(bestBid.price)) : Number.POSITIVE_INFINITY;

  const risk = evaluateRisk({
    ...input.baseRisk,
    traceId: input.traceId,
    intent: "RESCUE_SECOND_LEG",
    availableLiquidityUsd: orderBookLiquidityUsd(snapshot, "BUY"),
    expectedEdge: netEvUsdPerShare,
    expectedROI: netEvUsdTotal === null ? Number.NEGATIVE_INFINITY : d(netEvUsdTotal).div(notional).toNumber(),
    spread,
    signalConfidence: 1,
    signalExpiresAt: new Date(input.baseRisk.now.getTime() + 60_000),
    feeScheduleKnown: schedule !== null,
    orderSizeUsd: notional.toFixed(),
    requiredCashUsd,
    rescueCandidate: {
      marketId: input.candidate.marketId,
      tokenId: input.candidate.tokenId,
      outcome: input.candidate.outcome,
      side: input.candidate.side,
      quantity: input.candidate.quantity,
    },
    rescuePosition: input.rescuePosition,
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
    side: "BUY",
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
    grossEvUsdTotal: quantity.toFixed(),
    netEvUsdTotal,
    executableNotionalUsd: notional.toFixed(),
    expectedROI: netEvUsdTotal === null ? null : d(netEvUsdTotal).div(notional).toNumber(),
    quantity: quantity.toFixed(),
    feeUsdTotal: fee,
    slippageUsdTotal: slippage,
    rawEdge: 0,
    feeUsd: fee,
    slippageUsd: slippage,
    netExpectedEdge: netEvUsdPerShare,
    bookVwap: executableNow ? walk.vwap : null,
    executablePrice: null,
    guaranteedSalvageUsd: executableNow ? d(quantity).minus(notional).minus(fee ?? 0).toFixed() : null,
    requiredCashUsd,
  };
}

/** Which risk-reduction action won the rescue arbitration. */
export type RiskReductionSelection = "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG" | "NONE";

export type RiskReductionSelectionReason =
  | "B_STRICTLY_BETTER"
  | "A_STRICTLY_BETTER"
  | "A_PREFERRED_TIE"
  | "A_ONLY"
  | "B_ONLY"
  | "NEITHER_AVAILABLE"
  | "EARLY_CATASTROPHIC_PAIR_LOCK";

/** Compact deterministic economics of one executable risk-reduction candidate. */
export interface RiskReductionCandidateEconomics {
  kind: "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG";
  accepted: boolean;
  rejectionReason: string | null;
  quantity: string | null;
  vwap: string | null;
  feeUsd: string | null;
  guaranteedSalvageUsd: string | null;
}

export interface RiskReductionArbitration {
  selection: RiskReductionSelection;
  selectionReason: RiskReductionSelectionReason;
  tieBreakUsed: boolean;
}

/**
 * Deterministic A/B selection for the rescue arbitration.
 *
 * Rule: greater guaranteed salvage wins (higher guaranteed terminal value =
 * smaller guaranteed damage). Exact tie prefers A / SELL because SELL releases
 * cash, consumes no cash, and adds no gross inventory. Never compares prices
 * against the sunk first-leg entry price; entry cost is common to both
 * alternatives and must not drive the decision.
 */
export function arbitrateRiskReduction(
  a: RiskReductionCandidateEconomics,
  b: RiskReductionCandidateEconomics,
): RiskReductionArbitration {
  const aUsd = a.guaranteedSalvageUsd == null || !a.accepted ? null : d(a.guaranteedSalvageUsd);
  const bUsd = b.guaranteedSalvageUsd == null || !b.accepted ? null : d(b.guaranteedSalvageUsd);
  if (aUsd != null && bUsd != null) {
    if (bUsd.gt(aUsd)) return { selection: "RESCUE_SECOND_LEG", selectionReason: "B_STRICTLY_BETTER", tieBreakUsed: false };
    if (aUsd.eq(bUsd)) return { selection: "PROTECTIVE_DE_RISK", selectionReason: "A_PREFERRED_TIE", tieBreakUsed: true };
    return { selection: "PROTECTIVE_DE_RISK", selectionReason: "A_STRICTLY_BETTER", tieBreakUsed: false };
  }
  if (aUsd != null) return { selection: "PROTECTIVE_DE_RISK", selectionReason: "A_ONLY", tieBreakUsed: false };
  if (bUsd != null) return { selection: "RESCUE_SECOND_LEG", selectionReason: "B_ONLY", tieBreakUsed: false };
  return { selection: "NONE", selectionReason: "NEITHER_AVAILABLE", tieBreakUsed: false };
}
