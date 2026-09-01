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
  availableLiquidityUsd: string;
  expectedEdge: number;
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
  if (age(input.marketFeedAt) > limits.staleFeedTimeoutMs) vetoes.push("MARKET_FEED_STALE");
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
  if (d(input.marketExposureUsd).plus(input.orderSizeUsd).gt(limits.maxMarketExposureUsd)) vetoes.push("MAX_MARKET_EXPOSURE");
  if (d(input.assetExposureUsd ?? "0").plus(input.orderSizeUsd).gt(limits.maxAssetExposureUsd)) vetoes.push("MAX_ASSET_EXPOSURE");
  if (d(input.totalExposureUsd).plus(input.orderSizeUsd).gt(limits.maxTotalExposureUsd)) vetoes.push("MAX_TOTAL_EXPOSURE");
  if (d(input.directionalExposureUsd).plus(input.orderSizeUsd).gt(limits.maxDirectionalExposureUsd)) vetoes.push("MAX_DIRECTIONAL_EXPOSURE");
  if (d(input.unhedgedExposureUsd).plus(input.orderSizeUsd).gt(limits.maxUnhedgedExposureUsd)) vetoes.push("MAX_UNHEDGED_EXPOSURE");
  if (input.unhedgedDurationMs > limits.maxUnhedgedDurationMs) vetoes.push("MAX_UNHEDGED_DURATION");
  if (d(input.availableLiquidityUsd).lt(limits.minLiquidityUsd)) vetoes.push("MINIMUM_LIQUIDITY");
  if (input.expectedEdge < limits.minExpectedEdge) vetoes.push("MINIMUM_EXPECTED_EDGE");
  if (input.spread > limits.maxSpread) vetoes.push("MAXIMUM_SPREAD");
  if (input.signalConfidence < limits.minSignalConfidence) vetoes.push("MINIMUM_SIGNAL_CONFIDENCE");
  if (input.signalExpiresAt.getTime() <= input.now.getTime()) vetoes.push("SIGNAL_EXPIRED");
  if (input.externalSignalsLastMinute >= limits.maxExternalSignalsPerMinute) vetoes.push("MAXIMUM_EXTERNAL_SIGNAL_RATE");
  if (input.lastStrategyTradeAt && input.now.getTime() - input.lastStrategyTradeAt.getTime() < limits.strategyCooldownMs) vetoes.push("STRATEGY_COOLDOWN");
  if (input.duplicateTrade) vetoes.push("DUPLICATE_TRADE");
  if (!input.feeScheduleKnown) vetoes.push("UNKNOWN_FEE_SCHEDULE");
  if (!input.experimentEntryOpen) vetoes.push("EXPERIMENT_ENTRY_CLOSED");
  if (input.executionErrorsLastHour >= limits.maxExecutionErrorsPerHour) vetoes.push("ABNORMAL_EXECUTION_ERRORS");
  if (input.externalRiskRecommendedVeto) vetoes.push("EXTERNAL_RISK_RECOMMENDED_VETO");
  if (input.shuttingDown) vetoes.push("SHUTTING_DOWN");

  const unique = [...new Set(vetoes)];
  const utilization = {
    orderSize: ratio(input.orderSizeUsd, limits.maxOrderSizeUsd),
    marketExposure: ratio(d(input.marketExposureUsd).plus(input.orderSizeUsd), limits.maxMarketExposureUsd),
    assetExposure: ratio(d(input.assetExposureUsd ?? "0").plus(input.orderSizeUsd), limits.maxAssetExposureUsd),
    totalExposure: ratio(d(input.totalExposureUsd).plus(input.orderSizeUsd), limits.maxTotalExposureUsd),
    directionalExposure: ratio(d(input.directionalExposureUsd).plus(input.orderSizeUsd), limits.maxDirectionalExposureUsd),
    unhedgedExposure: ratio(d(input.unhedgedExposureUsd).plus(input.orderSizeUsd), limits.maxUnhedgedExposureUsd),
    drawdown: ratio(input.drawdownUsd, limits.maxDrawdownUsd),
    externalSignalRate: input.externalSignalsLastMinute / limits.maxExternalSignalsPerMinute,
  };
  return { id: createId("risk"), traceId: input.traceId, accepted: unique.length === 0, vetoes: unique, utilization, evaluatedAt: input.now.toISOString() };
}

function ratio(value: Parameters<typeof d>[0], limit: number): number {
  return limit === 0 ? Number.POSITIVE_INFINITY : d(value).div(limit).toNumber();
}
