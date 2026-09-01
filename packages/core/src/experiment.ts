import { createId, type ExperimentManifest } from "./domain.js";

export interface ExperimentStartInput {
  durationHours: number;
  strategyVersion: string;
  riskConfiguration: Record<string, unknown>;
  modelConfiguration: Record<string, unknown>;
  startingBalance: string;
  systemVersion: string;
  gitSha: string;
  now: Date;
}

export function startExperiment(input: ExperimentStartInput): ExperimentManifest {
  return {
    id: createId("experiment"), status: "RUNNING", durationHours: input.durationHours,
    strategyVersion: input.strategyVersion, riskConfiguration: input.riskConfiguration,
    modelConfiguration: input.modelConfiguration, startingBalance: input.startingBalance,
    systemVersion: input.systemVersion, gitSha: input.gitSha, startedAt: input.now.toISOString(),
    entryClosesAt: new Date(input.now.getTime() + input.durationHours * 3_600_000).toISOString(), completedAt: null,
  };
}

export function refreshExperiment(manifest: ExperimentManifest, now: Date): ExperimentManifest {
  if (manifest.status === "RUNNING" && now.getTime() >= new Date(manifest.entryClosesAt).getTime()) {
    return { ...manifest, status: "ENTRY_CLOSED" };
  }
  return manifest;
}

export interface ExperimentSummary {
  experimentId: string;
  generatedAt: string;
  totalOpportunities: number;
  acceptedOpportunities: number;
  rejectedOpportunities: number;
  totalPaperOrders: number;
  totalFills: number;
  makerFills: number;
  takerFills: number;
  completedPairs: number;
  incompleteUnhedgedPositions: number;
  winLossDistribution: { wins: number; losses: number; breakeven: number };
  grossPnlUsd: number;
  feesUsd: number;
  slippageUsd: number;
  netTradingPnlUsd: number;
  aiCostUsd: number;
  estimatedInfrastructureCostUsd: number;
  economicPnlUsd: number;
  maximumDrawdownUsd: number;
  maximumDirectionalExposureUsd: number;
  averageUnhedgedDurationMs: number;
  worstUnhedgedEvent: Record<string, unknown> | null;
  averagePairCost: number | null;
  averageLockedPairEdge: number | null;
  averageRealizedEdge: number | null;
  makerFillRate: number;
  takerFillRate: number;
  modelInvocationCounts: Record<string, number>;
  modelCostByAgentUsd: Record<string, number>;
  performanceByAgentSignal: Record<string, unknown>;
  performanceByMarketRegime: Record<string, unknown>;
  performanceByHour: Record<string, unknown>;
  rejectedTradesThatWouldHaveWon: number;
  acceptedTradesThatLost: number;
  failureStatistics: Record<string, number>;
  hedgeFailureRate: number;
  executableOpportunityRate: number;
}

export function renderExperimentSummaryMarkdown(summary: ExperimentSummary): string {
  return `# 48-Hour Paper Experiment Summary

Generated: ${summary.generatedAt}

## Decision outcome

- Gross PnL: $${summary.grossPnlUsd.toFixed(2)}
- Net trading PnL: $${summary.netTradingPnlUsd.toFixed(2)}
- AI cost: $${summary.aiCostUsd.toFixed(2)}
- Estimated infrastructure cost: $${summary.estimatedInfrastructureCostUsd.toFixed(2)}
- Economic PnL: $${summary.economicPnlUsd.toFixed(2)}
- Maximum drawdown: $${summary.maximumDrawdownUsd.toFixed(2)}
- Hedge failure rate: ${(summary.hedgeFailureRate * 100).toFixed(2)}%
- Executable opportunity rate: ${(summary.executableOpportunityRate * 100).toFixed(2)}%

## Execution

- Opportunities: ${summary.totalOpportunities} (${summary.acceptedOpportunities} accepted, ${summary.rejectedOpportunities} rejected)
- Orders / fills: ${summary.totalPaperOrders} / ${summary.totalFills}
- Maker / taker fills: ${summary.makerFills} / ${summary.takerFills}
- Completed pairs: ${summary.completedPairs}
- Incomplete or unhedged positions: ${summary.incompleteUnhedgedPositions}
- Fees / slippage: $${summary.feesUsd.toFixed(2)} / $${summary.slippageUsd.toFixed(2)}

## Risk and model evidence

- Maximum directional exposure: $${summary.maximumDirectionalExposureUsd.toFixed(2)}
- Average unhedged duration: ${summary.averageUnhedgedDurationMs.toFixed(0)} ms
- Model invocations: ${JSON.stringify(summary.modelInvocationCounts)}
- Model cost by agent: ${JSON.stringify(summary.modelCostByAgentUsd)}
- Failures: ${JSON.stringify(summary.failureStatistics)}

This report is computed from the append-only journal. Raw data is retained.
`;
}
