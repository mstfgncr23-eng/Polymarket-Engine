import type { LearningTrainingRowV1 } from "./learning-dataset.js";

export const LEARNING_ECONOMIC_EVAL_VERSION = "learning-economic-eval-v1" as const;

export interface LearningEconomicTradeHypothetical {
  sampleId: string;
  marketId: string;
  checkpointId: string;
  asset: string | null;
  side: "BUY_UP" | "SKIP";
  approximateAskUp: number;
  modelProbabilityUp: number;
  expectedGrossEdgePerShare: number | null;
  assumedFeePerShare: number | null;
  expectedNetEdgePerShare: number | null;
  realizedPnlPerShare: number | null;
}

export interface LearningEconomicEvaluation {
  version: typeof LEARNING_ECONOMIC_EVAL_VERSION;
  nRows: number;
  tradeCount: number;
  skipCount: number;
  meanExpectedGrossEdge: number | null;
  meanExpectedNetEdge: number | null;
  totalRealizedPnlPerShare: number | null;
  maxDrawdownPerShare: number | null;
  capabilities: {
    approximateExecutablePriceFromMidSpread: true;
    feeScheduleOnSample: false;
    vwapDepthWalkOnSample: false;
    counterfactualLedgerJoin: false;
    officialSettlementLabel: true;
  };
  unavailable: string[];
  byAsset: Record<string, { tradeCount: number; totalRealizedPnlPerShare: number | null }>;
  byCheckpoint: Record<string, { tradeCount: number; totalRealizedPnlPerShare: number | null }>;
  trades: LearningEconomicTradeHypothetical[];
}

export interface EvaluateLearningEconomicsOptions {
  minExpectedGrossEdge?: number;
  assumedFeePerShare?: number | null;
}

/**
 * Economic evaluation from fields available on finalized learning rows.
 *
 * CAN: approximate ask (mid+spread/2), expected gross edge, optional assumed fee,
 *      binary settlement PnL for hypothetical BUY UP, drawdown, asset/checkpoint breakdown.
 * CANNOT: true VWAP depth-walk, feeSchedule on sample, counterfactual ledger join.
 */
export function evaluateLearningEconomics(
  rows: readonly LearningTrainingRowV1[],
  modelProbabilityUp: readonly number[],
  options: EvaluateLearningEconomicsOptions = {},
): LearningEconomicEvaluation {
  if (rows.length !== modelProbabilityUp.length) {
    throw new Error(
      `LEARNING_ECONOMIC_LENGTH_MISMATCH: rows=${rows.length} probs=${modelProbabilityUp.length}`,
    );
  }
  const minEdge = options.minExpectedGrossEdge ?? 0.01;
  const assumedFee = options.assumedFeePerShare ?? null;

  const unavailable = [
    "feeSchedule not present on LearningSampleV1 — net edge requires assumedFeePerShare",
    "VWAP / depth-walk executable price not present on sample — using mid+spread/2 approximation",
    "Native V2 counterfactual ledger fills/PnL not joined in PR4 dataset builder",
  ];

  const trades: LearningEconomicTradeHypothetical[] = [];
  let tradeCount = 0;
  let skipCount = 0;
  let grossSum = 0;
  let grossN = 0;
  let netSum = 0;
  let netN = 0;
  let totalRealized = 0;
  let realizedN = 0;
  let equity = 0;
  let peak = 0;
  let maxDrawdown = 0;

  const byAsset: Record<string, { tradeCount: number; totalRealizedPnlPerShare: number }> = {};
  const byCheckpoint: Record<string, { tradeCount: number; totalRealizedPnlPerShare: number }> = {};

  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i]!;
    const p = modelProbabilityUp[i]!;
    const ask = row.approximateExecutableAskUp;
    const expectedGross = p - ask;
    const take = expectedGross >= minEdge;
    const expectedNet = assumedFee === null ? null : expectedGross - assumedFee;
    const realized = take
      ? (row.labelUp === 1 ? 1 - ask : 0 - ask) - (assumedFee ?? 0)
      : null;

    if (take && realized !== null) {
      tradeCount += 1;
      totalRealized += realized;
      realizedN += 1;
      equity += realized;
      peak = Math.max(peak, equity);
      maxDrawdown = Math.max(maxDrawdown, peak - equity);
      const assetKey = row.asset ?? "UNKNOWN";
      const asset = byAsset[assetKey] ?? { tradeCount: 0, totalRealizedPnlPerShare: 0 };
      asset.tradeCount += 1;
      asset.totalRealizedPnlPerShare += realized;
      byAsset[assetKey] = asset;
      const cp = byCheckpoint[row.checkpointId] ?? { tradeCount: 0, totalRealizedPnlPerShare: 0 };
      cp.tradeCount += 1;
      cp.totalRealizedPnlPerShare += realized;
      byCheckpoint[row.checkpointId] = cp;
    } else {
      skipCount += 1;
    }

    if (Number.isFinite(expectedGross)) {
      grossSum += expectedGross;
      grossN += 1;
    }
    if (expectedNet !== null && Number.isFinite(expectedNet)) {
      netSum += expectedNet;
      netN += 1;
    }

    trades.push({
      sampleId: row.sampleId,
      marketId: row.marketId,
      checkpointId: row.checkpointId,
      asset: row.asset,
      side: take ? "BUY_UP" : "SKIP",
      approximateAskUp: ask,
      modelProbabilityUp: p,
      expectedGrossEdgePerShare: Number.isFinite(expectedGross) ? round(expectedGross) : null,
      assumedFeePerShare: assumedFee,
      expectedNetEdgePerShare: expectedNet === null ? null : round(expectedNet),
      realizedPnlPerShare: realized === null ? null : round(realized),
    });
  }

  return {
    version: LEARNING_ECONOMIC_EVAL_VERSION,
    nRows: rows.length,
    tradeCount,
    skipCount,
    meanExpectedGrossEdge: grossN > 0 ? round(grossSum / grossN) : null,
    meanExpectedNetEdge: netN > 0 ? round(netSum / netN) : null,
    totalRealizedPnlPerShare: realizedN > 0 ? round(totalRealized) : null,
    maxDrawdownPerShare: realizedN > 0 ? round(maxDrawdown) : null,
    capabilities: {
      approximateExecutablePriceFromMidSpread: true,
      feeScheduleOnSample: false,
      vwapDepthWalkOnSample: false,
      counterfactualLedgerJoin: false,
      officialSettlementLabel: true,
    },
    unavailable,
    byAsset: Object.fromEntries(
      Object.entries(byAsset).map(([k, v]) => [
        k,
        { tradeCount: v.tradeCount, totalRealizedPnlPerShare: round(v.totalRealizedPnlPerShare) },
      ]),
    ),
    byCheckpoint: Object.fromEntries(
      Object.entries(byCheckpoint).map(([k, v]) => [
        k,
        { tradeCount: v.tradeCount, totalRealizedPnlPerShare: round(v.totalRealizedPnlPerShare) },
      ]),
    ),
    trades,
  };
}

function round(value: number): number {
  return Math.round(value * 1e10) / 1e10;
}
