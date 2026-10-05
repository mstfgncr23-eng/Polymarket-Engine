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
 * Shared deterministic structural verification for a risk-reducing opposite
 * BUY (normal HEDGE_SECOND_LEG or controlled-loss RESCUE_SECOND_LEG): the
 * candidate must pair the SAME market's CURRENT positive unpaired residual by
 * BUYing the OPPOSITE outcome at the CURRENT canonical opposite token, in a
 * positive quantity that never exceeds the residual, without increasing
 * directional exposure. When `strictUnhedgedDecrease` is set (rescue), the
 * projected unhedged exposure must strictly decrease.
 *
 * Returns false (fail closed) when any required fact is missing or unverifiable.
 */
function verifiedOppositeRiskReductionBuy(input: {
  candidate: HedgeCandidateFacts | null;
  position: HedgePositionFacts | null;
  projectedDirectionalExposureUsd: string | null | undefined;
  directionalExposureUsd: string;
  projectedUnhedgedExposureUsd: string | null | undefined;
  unhedgedExposureUsd: string;
  strictUnhedgedDecrease: boolean;
}): boolean {
  const { candidate, position } = input;
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
  const projectedUnhedged = d(input.projectedUnhedgedExposureUsd);
  const unhedged = d(input.unhedgedExposureUsd);
  if (input.strictUnhedgedDecrease) {
    if (projectedUnhedged.gte(unhedged)) return false;
  } else if (projectedUnhedged.gt(unhedged)) {
    return false;
  }
  return true;
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
  if (!input.hedgeIntent) return false;
  return verifiedOppositeRiskReductionBuy({
    candidate: input.candidate,
    position: input.position,
    projectedDirectionalExposureUsd: input.projectedDirectionalExposureUsd,
    directionalExposureUsd: input.directionalExposureUsd,
    projectedUnhedgedExposureUsd: input.projectedUnhedgedExposureUsd,
    unhedgedExposureUsd: input.unhedgedExposureUsd,
    strictUnhedgedDecrease: false,
  });
}

/**
 * Deterministic structural verification for a controlled-loss rescue
 * (RESCUE_SECOND_LEG) — the same rigor as verifiedReducingHedge, with the
 * stricter requirement that the projected unhedged exposure STRICTLY
 * decreases (a rescue that does not strictly reduce the residual is not
 * risk-reducing and must fail closed). Grants no risk-gate exemption from the
 * intent label alone; every fact is re-derived from the candidate and the
 * current position state.
 */
export function verifiedRescuePairLock(input: {
  rescueIntent: boolean;
  candidate: HedgeCandidateFacts | null;
  position: HedgePositionFacts | null;
  projectedDirectionalExposureUsd: string | null | undefined;
  directionalExposureUsd: string;
  projectedUnhedgedExposureUsd: string | null | undefined;
  unhedgedExposureUsd: string;
}): boolean {
  if (!input.rescueIntent) return false;
  return verifiedOppositeRiskReductionBuy({
    candidate: input.candidate,
    position: input.position,
    projectedDirectionalExposureUsd: input.projectedDirectionalExposureUsd,
    directionalExposureUsd: input.directionalExposureUsd,
    projectedUnhedgedExposureUsd: input.projectedUnhedgedExposureUsd,
    unhedgedExposureUsd: input.unhedgedExposureUsd,
    strictUnhedgedDecrease: true,
  });
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
  intent?: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG" | "GUARDIAN_PROTECTIVE_SELL";
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
  /**
   * Current-book fee-inclusive terminal PnL for the experiment's profit-trailing
   * pair lock. This field alone grants no exemption: evaluateRisk additionally
   * requires verifiedReducingHedge() against the CURRENT durable residual.
   */
  profitTrailingLockedPnlUsd?: string;
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
   * Post-latency executable price for the candidate (decimal string): for
   * TAKER orders the full-quantity book-walk VWAP, for MAKER orders the maker
   * execution price. null/absent means it could not be established safely and
   * the STRICT first-leg regime must apply (never the relaxed one).
   */
  executablePrice?: string | null;
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
  /**
   * Candidate facts for RESCUE_SECOND_LEG structural verification. Populated
   * by the decision layer only for rescue candidates; verifiedRescuePairLock
   * must pass (fail closed) before any risk-gate exemption is granted.
   */
  rescueCandidate?: HedgeCandidateFacts | null;
  /**
   * Current durable position facts for RESCUE_SECOND_LEG verification.
   * Populated by the engine from the live ledger at decision time.
   */
  rescuePosition?: HedgePositionFacts | null;
}

export function evaluateRisk(input: RiskInput, limits: RiskLimits): RiskEvaluation {
  const vetoes: RiskReasonCode[] = [];
  const age = (value: Date | null) => value ? input.now.getTime() - value.getTime() : Number.POSITIVE_INFINITY;
  const hedge = input.intent === "HEDGE_SECOND_LEG";
  const protective = input.intent === "PROTECTIVE_DE_RISK" || input.intent === "GUARDIAN_PROTECTIVE_SELL";
  const rescue = input.intent === "RESCUE_SECOND_LEG";
  const riskReducingIntent = hedge || rescue || protective;
  // RESCUE_SECOND_LEG is a risk-only pair-lock: like the protective SELL it
  // must not be subject to directional alpha gates, but — unlike a normal
  // hedge — the pair BUY may intentionally lock a small loss, so the alpha
  // floors would be economically meaningless here. Exemptions are granted only
  // when the structural verification passes (fail closed otherwise).
  const verifiedHedge = verifiedReducingHedge({
    hedgeIntent: hedge,
    candidate: input.hedgeCandidate ?? null,
    position: input.hedgePosition ?? null,
    projectedDirectionalExposureUsd: input.projectedDirectionalExposureUsd,
    directionalExposureUsd: input.directionalExposureUsd,
    projectedUnhedgedExposureUsd: input.projectedUnhedgedExposureUsd,
    unhedgedExposureUsd: input.unhedgedExposureUsd,
  });
  const profitLockPnl = input.profitTrailingLockedPnlUsd !== undefined
    ? d(input.profitTrailingLockedPnlUsd) : null;
  const verifiedProfitableTrailingHedge = verifiedHedge
    && profitLockPnl !== null && profitLockPnl.isFinite() && profitLockPnl.gt(0);
  const verifiedRescue = verifiedRescuePairLock({
    rescueIntent: rescue,
    candidate: input.rescueCandidate ?? null,
    position: input.rescuePosition ?? null,
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
  // RESCUE_SECOND_LEG safety depends on the executable Polymarket book + binary
  // pair identity, NOT on directional fair-value prediction: the rescue BUY is
  // exempt from reference-provider availability/staleness gates (documented
  // policy). Every other hard safety gate below still applies.
  const rescueExemptFromReference = !protective && !rescue;
  if (rescueExemptFromReference && input.referenceProviderBlocked === true) vetoes.push("REFERENCE_PROVIDER_BLOCKED");
  if (rescueExemptFromReference && input.referenceRequired === true) {
    if (age(input.referenceFeedAt ?? null) > limits.staleFeedTimeoutMs) {
      vetoes.push("REFERENCE_FEED_STALE");
      if ((input.asset ?? "BTC") === "BTC") vetoes.push("BTC_FEED_STALE");
    }
  } else if (rescueExemptFromReference && input.referenceRequired !== false && age(input.btcFeedAt) > limits.staleFeedTimeoutMs) {
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
  // Fresh first-leg price-regime economic policy. ONLY an explicit OPEN_FIRST_LEG
  // intent with a safely established post-latency executable price strictly
  // below the boundary may use the NORMAL floors; every other intent (hedges,
  // protective, unknown) and every price at/above the boundary keeps the strict
  // floors. The regime is per-decision and never mutates the shared limits.
  const normalFirstLegRegime = input.intent === "OPEN_FIRST_LEG"
    && input.executablePrice !== undefined
    && input.executablePrice !== null
    && d(input.executablePrice).lt(limits.normalFirstLegMaxPriceExclusive);
  const firstLegMinExpectedEdge = normalFirstLegRegime ? limits.normalFirstLegMinExpectedEdge : limits.minExpectedEdge;
  const firstLegMinExpectedROI = normalFirstLegRegime ? limits.normalFirstLegMinExpectedROI : limits.minExpectedROI;
  // A rescue pair-lock may intentionally lock a small loss; the alpha floors
  // are skipped ONLY for structurally verified rescue candidates.
  if (!protective && !rescue && !verifiedProfitableTrailingHedge
    && input.expectedEdge < firstLegMinExpectedEdge) vetoes.push("MINIMUM_EXPECTED_EDGE");
  if (!riskReducingIntent && (!Number.isFinite(input.expectedROI) || input.expectedROI < firstLegMinExpectedROI)) {
    vetoes.push("MINIMUM_EXPECTED_ROI");
  }
  if (!protective && !rescue && !verifiedProfitableTrailingHedge
    && input.spread > limits.maxSpread) vetoes.push("MAXIMUM_SPREAD");
  if (!protective && !verifiedHedge && !verifiedRescue && input.signalConfidence < limits.minSignalConfidence) vetoes.push("MINIMUM_SIGNAL_CONFIDENCE");
  if (!protective && !verifiedHedge && !verifiedRescue && input.signalExpiresAt.getTime() <= input.now.getTime()) vetoes.push("SIGNAL_EXPIRED");
  if (!protective && !rescue && input.externalSignalsLastMinute >= limits.maxExternalSignalsPerMinute) vetoes.push("MAXIMUM_EXTERNAL_SIGNAL_RATE");
  // Cooldown throttles fresh directional entries. It must never trap a risk-reducing action.
  if (!riskReducingIntent && input.lastStrategyTradeAt && input.now.getTime() - input.lastStrategyTradeAt.getTime() < limits.strategyCooldownMs) vetoes.push("STRATEGY_COOLDOWN");
  if (!protective && !rescue && !verifiedProfitableTrailingHedge && input.duplicateTrade) vetoes.push("DUPLICATE_TRADE");
  if (!input.feeScheduleKnown) vetoes.push("UNKNOWN_FEE_SCHEDULE");
  if (!protective && !input.experimentEntryOpen) vetoes.push("EXPERIMENT_ENTRY_CLOSED");
  if (input.marketEntryQualified === false && !riskReducingIntent) vetoes.push("MARKET_NOT_ENTRY_QUALIFIED");
  // FAIL CLOSED: a rescue candidate that does not strictly reduce the durable
  // unpaired residual through the verified opposite-token pair-lock can never
  // receive the risk-reduction exemptions.
  if (rescue && !verifiedRescue) vetoes.push("RESCUE_NOT_VERIFIED_REDUCING");
  const requiredCash = input.requiredCashUsd ?? input.orderSizeUsd;
  if (input.availableCashUsd !== undefined && d(input.availableCashUsd).lt(requiredCash)) vetoes.push("INSUFFICIENT_CASH");
  // Guardian's emergency full SELL must not be trapped by unrelated hourly
  // errors. Its actual execution failures and hard authority fences remain in
  // the shared durable engine lifecycle; all other intents keep this veto.
  if (input.intent !== "GUARDIAN_PROTECTIVE_SELL"
    && input.executionErrorsLastHour >= limits.maxExecutionErrorsPerHour) vetoes.push("ABNORMAL_EXECUTION_ERRORS");
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
