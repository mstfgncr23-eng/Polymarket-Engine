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
  intent?: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG";
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
  const marketExposure = input.projectedMarketExposureUsd ?? (hedge ? input.marketExposureUsd : d(input.marketExposureUsd).plus(input.orderSizeUsd).toFixed());
  const assetExposure = input.projectedAssetExposureUsd ?? (hedge ? (input.assetExposureUsd ?? "0") : d(input.assetExposureUsd ?? "0").plus(input.orderSizeUsd).toFixed());
  const totalExposure = input.projectedTotalExposureUsd ?? (hedge ? input.totalExposureUsd : d(input.totalExposureUsd).plus(input.orderSizeUsd).toFixed());
  const directionalExposure = input.projectedDirectionalExposureUsd ?? (hedge ? input.directionalExposureUsd : d(input.directionalExposureUsd).plus(input.orderSizeUsd).toFixed());
  const unhedgedExposure = input.projectedUnhedgedExposureUsd ?? (hedge ? input.unhedgedExposureUsd : d(input.unhedgedExposureUsd).plus(input.orderSizeUsd).toFixed());
  if (age(input.marketFeedAt) > limits.staleFeedTimeoutMs) vetoes.push("MARKET_FEED_STALE");
  if (input.referenceProviderBlocked === true) vetoes.push("REFERENCE_PROVIDER_BLOCKED");
  if (input.referenceRequired === true) {
    if (age(input.referenceFeedAt ?? null) > limits.staleFeedTimeoutMs) {
      vetoes.push("REFERENCE_FEED_STALE");
      if ((input.asset ?? "BTC") === "BTC") vetoes.push("BTC_FEED_STALE");
    }
  } else if (input.referenceRequired !== false && age(input.btcFeedAt) > limits.staleFeedTimeoutMs) {
    vetoes.push("BTC_FEED_STALE");
  }
  if (!input.databaseAvailable) vetoes.push("DATABASE_UNAVAILABLE");
  if (input.timestampDisorder) vetoes.push("EVENT_TIMESTAMP_DISORDER");
  if (!input.riskStateAvailable) vetoes.push("RISK_STATE_UNAVAILABLE");
  if (d(input.dailyPnlUsd).lte(d(limits.maxDailyPaperLossUsd).negated())) vetoes.push("DAILY_LOSS_LIMIT");
  if (d(input.drawdownUsd).gte(limits.maxDrawdownUsd)) vetoes.push("MAX_DRAWDOWN");
  if (d(input.orderSizeUsd).gt(limits.maxOrderSizeUsd)) vetoes.push("MAX_ORDER_SIZE");
  if (d(marketExposure).gt(limits.maxMarketExposureUsd)) vetoes.push("MAX_MARKET_EXPOSURE");
  if (d(assetExposure).gt(limits.maxAssetExposureUsd)) vetoes.push("MAX_ASSET_EXPOSURE");
  if (d(totalExposure).gt(limits.maxTotalExposureUsd)) vetoes.push("MAX_TOTAL_EXPOSURE");
  if (d(directionalExposure).gt(limits.maxDirectionalExposureUsd)) vetoes.push("MAX_DIRECTIONAL_EXPOSURE");
  if (d(unhedgedExposure).gt(limits.maxUnhedgedExposureUsd)) vetoes.push("MAX_UNHEDGED_EXPOSURE");
  if (!hedge && input.unhedgedDurationMs > limits.maxUnhedgedDurationMs) vetoes.push("MAX_UNHEDGED_DURATION");
  if (d(input.availableLiquidityUsd).lt(limits.minLiquidityUsd)) vetoes.push("MINIMUM_LIQUIDITY");
  if (input.expectedEdge < limits.minExpectedEdge) vetoes.push("MINIMUM_EXPECTED_EDGE");
  if (!hedge && (!Number.isFinite(input.expectedROI) || input.expectedROI < limits.minExpectedROI)) {
    vetoes.push("MINIMUM_EXPECTED_ROI");
  }
  if (input.spread > limits.maxSpread) vetoes.push("MAXIMUM_SPREAD");
  if (input.signalConfidence < limits.minSignalConfidence) vetoes.push("MINIMUM_SIGNAL_CONFIDENCE");
  if (input.signalExpiresAt.getTime() <= input.now.getTime()) vetoes.push("SIGNAL_EXPIRED");
  if (input.externalSignalsLastMinute >= limits.maxExternalSignalsPerMinute) vetoes.push("MAXIMUM_EXTERNAL_SIGNAL_RATE");
  // Cooldown throttles fresh directional entries. It must not delay a risk-reducing second leg.
  if (!hedge && input.lastStrategyTradeAt && input.now.getTime() - input.lastStrategyTradeAt.getTime() < limits.strategyCooldownMs) vetoes.push("STRATEGY_COOLDOWN");
  if (input.duplicateTrade) vetoes.push("DUPLICATE_TRADE");
  if (!input.feeScheduleKnown) vetoes.push("UNKNOWN_FEE_SCHEDULE");
  if (!input.experimentEntryOpen) vetoes.push("EXPERIMENT_ENTRY_CLOSED");
  if (input.marketEntryQualified === false && !hedge) vetoes.push("MARKET_NOT_ENTRY_QUALIFIED");
  const requiredCash = input.requiredCashUsd ?? input.orderSizeUsd;
  if (input.availableCashUsd !== undefined && d(input.availableCashUsd).lt(requiredCash)) vetoes.push("INSUFFICIENT_CASH");
  if (input.executionErrorsLastHour >= limits.maxExecutionErrorsPerHour) vetoes.push("ABNORMAL_EXECUTION_ERRORS");
  if (input.externalRiskRecommendedVeto) vetoes.push("EXTERNAL_RISK_RECOMMENDED_VETO");
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
