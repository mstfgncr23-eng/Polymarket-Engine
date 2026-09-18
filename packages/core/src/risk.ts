import { d } from "./calculations.js";
import { createId, type RiskEvaluation, type RiskReasonCode } from "./domain.js";
import type { RiskLimits } from "./config.js";

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
}

export function evaluateRisk(input: RiskInput, limits: RiskLimits): RiskEvaluation {
  const vetoes: RiskReasonCode[] = [];
  const age = (value: Date | null) => value ? input.now.getTime() - value.getTime() : Number.POSITIVE_INFINITY;
  const hedge = input.intent === "HEDGE_SECOND_LEG";
  const protective = input.intent === "PROTECTIVE_DE_RISK";
  const riskReducingIntent = hedge || protective;
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
  if (!protective && input.signalConfidence < limits.minSignalConfidence) vetoes.push("MINIMUM_SIGNAL_CONFIDENCE");
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
