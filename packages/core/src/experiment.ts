import { createId, type ExperimentClosePolicy, type ExperimentManifest, type Position } from "./domain.js";

export interface ExperimentStartInput {
  durationHours: number;
  strategyVersion: string;
  riskConfiguration: Record<string, unknown>;
  modelConfiguration: Record<string, unknown>;
  startingBalance: string;
  systemVersion: string;
  gitSha: string;
  now: Date;
  closePolicy?: ExperimentClosePolicy;
  id?: string;
}

export function createIdleExperiment(input: Omit<ExperimentStartInput, "now"> & { now?: Date }): ExperimentManifest {
  return {
    id: input.id ?? createId("experiment"), status: "IDLE", durationHours: input.durationHours,
    strategyVersion: input.strategyVersion, riskConfiguration: input.riskConfiguration,
    modelConfiguration: input.modelConfiguration, startingBalance: input.startingBalance,
    systemVersion: input.systemVersion, gitSha: input.gitSha,
    closePolicy: input.closePolicy ?? "observe_until_settlement",
    startedAt: null, entryClosesAt: null, completedAt: null, unresolvedPositionCount: 0, markToMarketAt: null,
    startValidationPending: false, abortedAt: null, abortReason: null,
  };
}

export function startExperiment(input: ExperimentStartInput): ExperimentManifest {
  const startedAt = input.now.toISOString();
  return {
    id: input.id ?? createId("experiment"), status: "RUNNING", durationHours: input.durationHours,
    strategyVersion: input.strategyVersion, riskConfiguration: input.riskConfiguration,
    modelConfiguration: input.modelConfiguration, startingBalance: input.startingBalance,
    systemVersion: input.systemVersion, gitSha: input.gitSha,
    closePolicy: input.closePolicy ?? "observe_until_settlement",
    startedAt, entryClosesAt: new Date(input.now.getTime() + input.durationHours * 3_600_000).toISOString(),
    completedAt: null, unresolvedPositionCount: 0, markToMarketAt: null,
    startValidationPending: true, abortedAt: null, abortReason: null,
  };
}

export function openPositionsRemain(positions: Iterable<Position>): boolean {
  return [...positions].some((position) => Number(position.upQuantity) > 0 || Number(position.downQuantity) > 0);
}

export function refreshExperiment(
  manifest: ExperimentManifest,
  now: Date,
  options: { openPositions?: boolean; unresolvedCount?: number; markToMarketReady?: boolean } = {},
): ExperimentManifest {
  if (manifest.status === "IDLE" || manifest.status === "COMPLETED" || manifest.status === "FAILED" || manifest.status === "ABORTED_START") return manifest;
  if (manifest.status === "RUNNING" && manifest.entryClosesAt && now.getTime() >= new Date(manifest.entryClosesAt).getTime()) {
    return { ...manifest, status: "ENTRY_CLOSED" };
  }
  if (manifest.status === "ENTRY_CLOSED") {
    const policy = manifest.closePolicy ?? "observe_until_settlement";
    const open = options.openPositions ?? false;
    const unresolved = options.unresolvedCount ?? 0;
    if (policy === "observe_until_settlement" && !open) {
      return { ...manifest, status: "COMPLETED", completedAt: now.toISOString(), unresolvedPositionCount: 0 };
    }
    if (policy === "mark_to_market") {
      if (options.markToMarketReady === false) return manifest;
      return {
        ...manifest, status: "COMPLETED", completedAt: now.toISOString(),
        markToMarketAt: now.toISOString(), unresolvedPositionCount: unresolved,
      };
    }
  }
  return manifest;
}

export function experimentAllowsFirstLeg(manifest: ExperimentManifest | null): boolean {
  return manifest?.status === "RUNNING";
}

export function experimentAllowsHedge(manifest: ExperimentManifest | null): boolean {
  return manifest == null || manifest.status === "RUNNING" || manifest.status === "ENTRY_CLOSED" || manifest.status === "IDLE";
}

export function shouldWriteFinalReport(manifest: ExperimentManifest, openPositions: boolean): boolean {
  if (manifest.status === "COMPLETED") return true;
  if (manifest.status !== "ENTRY_CLOSED") return false;
  if ((manifest.closePolicy ?? "observe_until_settlement") === "observe_until_settlement" && openPositions) return false;
  return (manifest.closePolicy ?? "observe_until_settlement") === "mark_to_market";
}

export interface ExperimentSummary {
  experimentId: string;
  generatedAt: string;
  status: string;
  closePolicy: string;
  reportKind: "interim" | "final";
  unresolvedPositionCount: number;
  settlementCount: number;
  settlementPnlUsd: number;
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
  /** Fee-exclusive settlement classification. winLossDistribution is net of fees. */
  grossWinLossDistribution: { wins: number; losses: number; breakeven: number };
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
  /** Fee-inclusive realized edge per pair. Same as averageNetRealizedEdge. */
  averageRealizedEdge: number | null;
  averageGrossLockedEdge: number | null;
  averageNetRealizedEdge: number | null;
  makerFillRate: number;
  takerFillRate: number;
  modelInvocationCounts: Record<string, number>;
  modelCostByAgentUsd: Record<string, number>;
  performanceByAgentSignal: Record<string, unknown>;
  /** Real trading attribution from fills/risk — not journal event volume. */
  performanceByMarket: Record<string, MarketAssetAttribution>;
  performanceByAsset: Record<string, MarketAssetAttribution>;
  eventCountByMarket: Record<string, number>;
  eventCountByAsset: Record<string, number>;
  pnlByMarket: Record<string, number>;
  pnlByAsset: Record<string, number>;
  fillsByMarket: Record<string, number>;
  fillsByAsset: Record<string, number>;
  feesByMarket: Record<string, number>;
  feesByAsset: Record<string, number>;
  slippageByMarket: Record<string, number>;
  slippageByAsset: Record<string, number>;
  opportunitiesByMarket: Record<string, number>;
  opportunitiesByAsset: Record<string, number>;
  vetoesByMarket: Record<string, number>;
  vetoesByAsset: Record<string, number>;
  performanceByMarketRegime: Record<string, unknown>;
  performanceByHour: Record<string, unknown>;
  rejectedTradesThatWouldHaveWon: number;
  acceptedTradesThatLost: number;
  failureStatistics: Record<string, number>;
  hedgeFailureRate: number;
  executableOpportunityRate: number;
  metricsScope: "rolling" | "complete";
  complete: boolean;
  truncated: boolean;
  truncatedReason: string | null;
  earliestOccurredAt: string | null;
  latestOccurredAt: string | null;
  decisionAccountingEventCount: number;
  firstLegCount: number;
  hedgeCompleteCount: number;
  failedHedgeCount: number;
}

export interface MarketAssetAttribution {
  realizedPnlUsd: number;
  feesUsd: number;
  slippageUsd: number;
  settlementPnlUsd: number;
  netTradingPnlUsd: number;
  fills: number;
  opportunities: number;
  accepted: number;
  vetoes: number;
}

export function renderExperimentSummaryMarkdown(summary: ExperimentSummary): string {
  return `# 48-Hour Paper Experiment Summary

Generated: ${summary.generatedAt}
Status: ${summary.status}
Close policy: ${summary.closePolicy}
Report kind: ${summary.reportKind}

## Decision outcome

- Gross PnL: $${summary.grossPnlUsd.toFixed(2)}
- Net trading PnL (includes binary settlement, net of fees): $${summary.netTradingPnlUsd.toFixed(2)}
- Win/loss (net of fees): ${summary.winLossDistribution.wins} / ${summary.winLossDistribution.losses} / ${summary.winLossDistribution.breakeven}
- Win/loss (gross, fee-exclusive): ${summary.grossWinLossDistribution.wins} / ${summary.grossWinLossDistribution.losses} / ${summary.grossWinLossDistribution.breakeven}
- Settlement PnL: $${summary.settlementPnlUsd.toFixed(2)}
- Settlements: ${summary.settlementCount}
- Unresolved positions: ${summary.unresolvedPositionCount}
- AI cost (legacy ledger field, should remain $0 without OpenAI): $${summary.aiCostUsd.toFixed(2)}
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
- Average gross locked pair edge (fee-exclusive): ${summary.averageGrossLockedEdge ?? "n/a"}
- Average net realized edge (net of fees): ${summary.averageNetRealizedEdge ?? summary.averageRealizedEdge ?? "n/a"}
- Fees / slippage: $${summary.feesUsd.toFixed(2)} / $${summary.slippageUsd.toFixed(2)}

## Risk and model evidence

- Maximum directional exposure: $${summary.maximumDirectionalExposureUsd.toFixed(2)}
- Average unhedged duration: ${summary.averageUnhedgedDurationMs.toFixed(0)} ms
- Grok Bot signals: ${JSON.stringify(summary.modelInvocationCounts)}
- PnL by market: ${JSON.stringify(summary.pnlByMarket)}
- PnL by asset: ${JSON.stringify(summary.pnlByAsset)}
- Fills by market / asset: ${JSON.stringify(summary.fillsByMarket)} / ${JSON.stringify(summary.fillsByAsset)}
- Fees / slippage by market: ${JSON.stringify(summary.feesByMarket)} / ${JSON.stringify(summary.slippageByMarket)}
- Opportunities / vetoes by market: ${JSON.stringify(summary.opportunitiesByMarket)} / ${JSON.stringify(summary.vetoesByMarket)}
- Event counts (activity, not PnL) by market / asset: ${JSON.stringify(summary.eventCountByMarket)} / ${JSON.stringify(summary.eventCountByAsset)}
- OpenAI token cost: $0 (runtime inference is not billed here)
- Failures: ${JSON.stringify(summary.failureStatistics)}

This report is computed from the append-only decision/accounting journal. Raw high-frequency telemetry is sampled.
`;
}
