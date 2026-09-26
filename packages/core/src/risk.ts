import { d } from "./calculations.js";
import { createId, type Outcome, type RiskEvaluation, type RiskReasonCode, type Side } from "./domain.js";
import type { RiskLimits } from "./config.js";

/**
 * Candidate facts used to verify a HEDGE_SECOND_LEG admission. Populated by the
 * decision layer from the strategy candidate; never trusted from intent alone.
 */
export interface HedgeCandidateFacts {
  marketId: string;
  tokenId: string;
  outcome: Outcome;
  side: Side;
  quantity: string;
}

/**
 * Current durable position facts used to verify a HEDGE_SECOND_LEG admission.
 * Populated by the engine from the live ledger at decision time.
 */
export interface HedgePositionFacts {
  marketId: string;
  unpairedOutcome: Outcome | null;
  unpairedQuantity: string;
  /**
   * The EXPECTED opposite token id for the candidate outcome, resolved by the
   * caller from CURRENT synchronized market state for `marketId` and the
   * current `unpairedOutcome`. Never derived from the candidate. null when it
   * cannot be resolved — the verification must fail closed.
   */
  expectedOppositeTokenId: string | null;
}

/**
 * Deterministic verification that a HEDGE_SECOND_LEG candidate genuinely
 * reduces/completes the CURRENT durable unpaired residual. The confidence
 * exemption must never be granted from the intent label alone: every fact is
 * re-derived from the stated candidate and the current position state, and the
 * projected position must not increase directional or unhedged exposure.
 *
 * Returns false (fail closed) when any required fact is missing or unverifiable.
 */
export function verifiedReducingHedge(input: {
  hedgeIntent: boolean;
  candidate: HedgeCandidateFacts | null;
  position: HedgePositionFacts | null;
  projectedDirectionalExposureUsd: string | null | undefined;
  directionalExposureUsd: string;
  projectedUnhedgedExposureUsd: string | null | undefined;
  unhedgedExposureUsd: string;
}): boolean {
  const { candidate, position } = input;
  if (!input.hedgeIntent) return false;
  if (!candidate || !position) return false;
  // 1/3. Candidate reduces the position in the SAME market.
  if (candidate.marketId !== position.marketId) return false;
  // 2. A real unsettled position with a positive unpaired residual.
  if (!position.unpairedOutcome) return false;
  const residual = d(position.unpairedQuantity);
  if (!residual.isFinite() || residual.lte(0)) return false;
  // 5. The hedge is a BUY...
  if (candidate.side !== "BUY") return false;
  // 4. ...of the OPPOSITE outcome required to pair the residual.
  const requiredOutcome: Outcome = position.unpairedOutcome === "UP" ? "DOWN" : "UP";
  if (candidate.outcome !== requiredOutcome) return false;
  // 4b. The candidate must trade the EXPECTED opposite TOKEN for that outcome,
  // resolved from current market state — never trusted from the candidate.
  if (!position.expectedOppositeTokenId) return false;
  if (candidate.tokenId !== position.expectedOppositeTokenId) return false;
  // 3/6. Quantity is strictly positive and cannot exceed the residual.
  const quantity = d(candidate.quantity);
  if (!quantity.isFinite() || quantity.lte(0) || quantity.gt(residual)) return false;
  // 7. The projected position does not increase directional/unhedged exposure.
  if (
    input.projectedDirectionalExposureUsd === null
    || input.projectedDirectionalExposureUsd === undefined
    || input.projectedUnhedgedExposureUsd === null
    || input.projectedUnhedgedExposureUsd === undefined
  ) return false;
  if (d(input.projectedDirectionalExposureUsd).gt(input.directionalExposureUsd)) return false;
  if (d(input.projectedUnhedgedExposureUsd).gt(input.unhedgedExposureUsd)) return false;
  return true;
}

export interface RiskInput {
  traceId: string;
  now: Date;
  marketFeedAt: Date | null;
  btcFeedAt: Date | null;
  referenceFeedAt?: Date | null;
  referenceRequired?: boolean;
  asset?: string | null;
  assetExposureUsd?: string;
  databaseAvailable: boolean;
  timestampDisorder: boolean;
  riskStateAvailable: boolean;
  dailyPnlUsd: string;
  drawdownUsd: string;
  orderSizeUsd: string;
  marketExposureUsd: string;
  totalExposureUsd: string;
  directionalExposureUsd: string;
  unhedgedExposureUsd: string;
  unhedgedDurationMs: number;
  intent?: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK";
  projectedMarketExposureUsd?: string;
  projectedTotalExposureUsd?: string;
  projectedDirectionalExposureUsd?: string;
  projectedUnhedgedExposureUsd?: string;
  projectedAssetExposureUsd?: string;
  availableCashUsd?: string;
  /** Fee-inclusive cash required for a BUY. Defaults to orderSizeUsd when omitted. */
  requiredCashUsd?: string;
  referenceProviderBlocked?: boolean;
  marketEntryQualified?: boolean;
  availableLiquidityUsd: string;
  /** Net expected value in USD/share for a $1 binary payout. */
  expectedEdge: number;
  /** Fee/slippage-adjusted expected return divided by executable notional. */
  expectedROI: number;
  spread: number;
  signalConfidence: number;
  signalExpiresAt: Date;
  externalSignalsLastMinute: number;
  lastStrategyTradeAt: Date | null;
  duplicateTrade: boolean;
  feeScheduleKnown: boolean;
  experimentEntryOpen: boolean;
  executionErrorsLastHour: number;
  externalRiskRecommendedVeto: boolean;
  shuttingDown: boolean;
  /**
   * Candidate facts for HEDGE_SECOND_LEG verification. Populated by the
   * decision layer only for hedge candidates. The confidence exemption is
   * granted only when verifiedReducingHedge() passes against hedgePosition.
   */
  hedgeCandidate?: HedgeCandidateFacts | null;
  /**
   * Current durable position facts for HEDGE_SECOND_LEG verification.
   * Populated by the engine from the live ledger at decision time.
   */
  hedgePosition?: HedgePositionFacts | null;
}

export function evaluateRisk(input: RiskInput, limits: RiskLimits): RiskEvaluation {
  const vetoes: RiskReasonCode[] = [];
  const age = (value: Date | null) => value ? input.now.getTime() - value.getTime() : Number.POSITIVE_INFINITY;
  const hedge = input.intent === "HEDGE_SECOND_LEG";
  const protective = input.intent === "PROTECTIVE_DE_RISK";
  const riskReducingIntent = hedge || protective;
  // A hedge exempts the alpha-confidence gate only when it is VERIFIED against
  // the current durable position (the confidence gate must still trap a fresh
  // first-leg entry and a spoofed/stale hedge candidate).
  const verifiedHedge = verifiedReducingHedge({
    hedgeIntent: hedge,
    candidate: input.hedgeCandidate ?? null,
    position: input.hedgePosition ?? null,
    projectedDirectionalExposureUsd: input.projectedDirectionalExposureUsd,
    directionalExposureUsd: input.directionalExposureUsd,
    projectedUnhedgedExposureUsd: input.projectedUnhedgedExposureUsd,
    unhedgedExposureUsd: input.unhedgedExposureUsd,
  });
  const marketExposure = input.projectedMarketExposureUsd ?? (riskReducingIntent ? input.marketExposureUsd : d(input.marketExposureUsd).plus(input.orderSizeUsd).toFixed());
  const assetExposure = input.projectedAssetExposureUsd ?? (riskReducingIntent ? (input.assetExposureUsd ?? "0") : d(input.assetExposureUsd ?? "0").plus(input.orderSizeUsd).toFixed());
  const totalExposure = input.projectedTotalExposureUsd ?? (riskReducingIntent ? input.totalExposureUsd : d(input.totalExposureUsd).plus(input.orderSizeUsd).toFixed());
  const directionalExposure = input.projectedDirectionalExposureUsd ?? (riskReducingIntent ? input.directionalExposureUsd : d(input.directionalExposureUsd).plus(input.orderSizeUsd).toFixed());
  const unhedgedExposure = input.projectedUnhedgedExposureUsd ?? (riskReducingIntent ? input.unhedgedExposureUsd : d(input.unhedgedExposureUsd).plus(input.orderSizeUsd).toFixed());
  const protectiveReducesRisk = protective
    && d(marketExposure).lte(input.marketExposureUsd)
    && d(assetExposure).lte(input.assetExposureUsd ?? "0")
    && d(totalExposure).lte(input.totalExposureUsd)
    && d(directionalExposure).lte(input.directionalExposureUsd)
    && d(unhedgedExposure).lt(input.unhedgedExposureUsd);
  if (age(input.marketFeedAt) > limits.staleFeedTimeoutMs) vetoes.push("MARKET_FEED_STALE");
  if (!protective && input.referenceProviderBlocked === true) vetoes.push("REFERENCE_PROVIDER_BLOCKED");
  if (!protective && input.referenceRequired === true) {
    if (age(input.referenceFeedAt ?? null) > limits.staleFeedTimeoutMs) {
      vetoes.push("REFERENCE_FEED_STALE");
      if ((input.asset ?? "BTC") === "BTC") vetoes.push("BTC_FEED_STALE");
    }
  } else if (!protective && input.referenceRequired !== false && age(input.btcFeedAt) > limits.staleFeedTimeoutMs) {
    vetoes.push("BTC_FEED_STALE");
  }
  if (!input.databaseAvailable) vetoes.push("DATABASE_UNAVAILABLE");
  if (input.timestampDisorder) vetoes.push("EVENT_TIMESTAMP_DISORDER");
  if (!input.riskStateAvailable) vetoes.push("RISK_STATE_UNAVAILABLE");
  if (!protective && d(input.dailyPnlUsd).lte(d(limits.maxDailyPaperLossUsd).negated())) vetoes.push("DAILY_LOSS_LIMIT");
  if (!protective && d(input.drawdownUsd).gte(limits.maxDrawdownUsd)) vetoes.push("MAX_DRAWDOWN");
  if (d(input.orderSizeUsd).gt(limits.maxOrderSizeUsd)) vetoes.push("MAX_ORDER_SIZE");
  if (!protective && d(marketExposure).gt(limits.maxMarketExposureUsd)) vetoes.push("MAX_MARKET_EXPOSURE");
  if (!protective && d(assetExposure).gt(limits.maxAssetExposureUsd)) vetoes.push("MAX_ASSET_EXPOSURE");
  if (!protective && d(totalExposure).gt(limits.maxTotalExposureUsd)) vetoes.push("MAX_TOTAL_EXPOSURE");
  if (!protective && d(directionalExposure).gt(limits.maxDirectionalExposureUsd)) vetoes.push("MAX_DIRECTIONAL_EXPOSURE");
  if (!protective && d(unhedgedExposure).gt(limits.maxUnhedgedExposureUsd)) vetoes.push("MAX_UNHEDGED_EXPOSURE");
  if (protective && !protectiveReducesRisk) vetoes.push("PROTECTIVE_DE_RISK_NOT_REDUCING");
  if (!riskReducingIntent && input.unhedgedDurationMs > limits.maxUnhedgedDurationMs) vetoes.push("MAX_UNHEDGED_DURATION");
  if (!protective && d(input.availableLiquidityUsd).lt(limits.minLiquidityUsd)) vetoes.push("MINIMUM_LIQUIDITY");
  if (!protective && input.expectedEdge < limits.minExpectedEdge) vetoes.push("MINIMUM_EXPECTED_EDGE");
  if (!riskReducingIntent && (!Number.isFinite(input.expectedROI) || input.expectedROI < limits.minExpectedROI)) {
    vetoes.push("MINIMUM_EXPECTED_ROI");
  }
  if (!protective && input.spread > limits.maxSpread) vetoes.push("MAXIMUM_SPREAD");
  if (!protective && !verifiedHedge && input.signalConfidence < limits.minSignalConfidence) vetoes.push("MINIMUM_SIGNAL_CONFIDENCE");
  if (!protective && input.signalExpiresAt.getTime() <= input.now.getTime()) vetoes.push("SIGNAL_EXPIRED");
  if (!protective && input.externalSignalsLastMinute >= limits.maxExternalSignalsPerMinute) vetoes.push("MAXIMUM_EXTERNAL_SIGNAL_RATE");
  // Cooldown throttles fresh directional entries. It must never trap a risk-reducing action.
  if (!riskReducingIntent && input.lastStrategyTradeAt && input.now.getTime() - input.lastStrategyTradeAt.getTime() < limits.strategyCooldownMs) vetoes.push("STRATEGY_COOLDOWN");
  if (!protective && input.duplicateTrade) vetoes.push("DUPLICATE_TRADE");
  if (!input.feeScheduleKnown) vetoes.push("UNKNOWN_FEE_SCHEDULE");
  if (!protective && !input.experimentEntryOpen) vetoes.push("EXPERIMENT_ENTRY_CLOSED");
  if (input.marketEntryQualified === false && !riskReducingIntent) vetoes.push("MARKET_NOT_ENTRY_QUALIFIED");
  const requiredCash = input.requiredCashUsd ?? input.orderSizeUsd;
  if (input.availableCashUsd !== undefined && d(input.availableCashUsd).lt(requiredCash)) vetoes.push("INSUFFICIENT_CASH");
  if (input.executionErrorsLastHour >= limits.maxExecutionErrorsPerHour) vetoes.push("ABNORMAL_EXECUTION_ERRORS");
  if (!protective && input.externalRiskRecommendedVeto) vetoes.push("EXTERNAL_RISK_RECOMMENDED_VETO");
  if (input.shuttingDown) vetoes.push("SHUTTING_DOWN");

  const unique = [...new Set(vetoes)];
  const utilization = {
    orderSize: ratio(input.orderSizeUsd, limits.maxOrderSizeUsd),
    marketExposure: ratio(marketExposure, limits.maxMarketExposureUsd),
    assetExposure: ratio(assetExposure, limits.maxAssetExposureUsd),
    totalExposure: ratio(totalExposure, limits.maxTotalExposureUsd),
    directionalExposure: ratio(directionalExposure, limits.maxDirectionalExposureUsd),
    unhedgedExposure: ratio(unhedgedExposure, limits.maxUnhedgedExposureUsd),
    drawdown: ratio(input.drawdownUsd, limits.maxDrawdownUsd),
    externalSignalRate: input.externalSignalsLastMinute / limits.maxExternalSignalsPerMinute,
  };
  return { id: createId("risk"), traceId: input.traceId, accepted: unique.length === 0, vetoes: unique, utilization, evaluatedAt: input.now.toISOString() };
}

function ratio(value: Parameters<typeof d>[0], limit: number): number {
  return limit === 0 ? Number.POSITIVE_INFINITY : d(value).div(limit).toNumber();
}
