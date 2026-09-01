import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { renderExperimentSummaryMarkdown, type ExperimentSummary, type JournalEvent, type LedgerState, type PaperFill, type Position } from "@pm/core";

export function buildExperimentSummary(events: readonly JournalEvent[], experimentId = "unknown"): ExperimentSummary {
  const byType = (type: string) => events.filter((event) => event.type === type);
  const fills = byType("PAPER_FILL").map((event) => event.payload as unknown as PaperFill);
  const positions = latestByAggregate(byType("POSITION_UPDATED")).map((event) => event.payload as unknown as Position);
  const pnlEvents = byType("PNL_SNAPSHOT");
  const ledger = pnlEvents.at(-1)?.payload as unknown as LedgerState | undefined;
  const grokSignals = byType("GROK_SIGNAL_ACCEPTED");
  const agentOutputs = [...byType("AGENT_OUTPUT"), ...grokSignals];
  const risk = byType("RISK_EVALUATION");
  const accepted = risk.filter((event) => (event.payload as { accepted?: boolean }).accepted === true).length;
  const rejected = risk.length - accepted;
  const completedPairs = positions.filter((position) => Number(position.pairedQuantity) > 0).length;
  const unhedged = positions.filter((position) => Number(position.unpairedQuantity) > 0);
  const makerFills = fills.filter((fill) => fill.liquidityRole === "MAKER").length;
  const takerFills = fills.filter((fill) => fill.liquidityRole === "TAKER").length;
  const modelCounts: Record<string, number> = {};
  const modelCosts: Record<string, number> = {};
  for (const event of agentOutputs) {
    const output = event.payload as { agentId?: string; usage?: { estimatedCostUsd?: number | null } };
    if (!output.agentId) continue;
    modelCounts[output.agentId] = (modelCounts[output.agentId] ?? 0) + 1;
    modelCosts[output.agentId] = (modelCosts[output.agentId] ?? 0) + (output.usage?.estimatedCostUsd ?? 0);
  }
  const failureStatistics: Record<string, number> = {};
  for (const event of byType("AGENT_FAILURE")) {
    const code = String((event.payload as { code?: unknown }).code ?? "UNKNOWN");
    failureStatistics[code] = (failureStatistics[code] ?? 0) + 1;
  }
  for (const event of byType("MCP_INTERACTION")) {
    const status = String((event.payload as { status?: unknown }).status ?? "MCP");
    if (status !== "HANDLED") failureStatistics[`MCP_${status}`] = (failureStatistics[`MCP_${status}`] ?? 0) + 1;
  }
  for (const event of byType("PAPER_EXECUTION_ERROR")) failureStatistics.PAPER_EXECUTION_ERROR = (failureStatistics.PAPER_EXECUTION_ERROR ?? 0) + 1;
  const grossPnl = Number(ledger?.grossPnl ?? 0);
  const netTradingPnl = Number(ledger?.netTradingPnl ?? 0);
  const aiCost = Number(ledger?.aiCost ?? 0);
  const infraCost = Number(ledger?.infrastructureCost ?? 0);
  const pairCosts = positions.filter((position) => Number(position.pairedQuantity) > 0).map((position) => Number(position.combinedPairCost));
  return {
    experimentId, generatedAt: new Date().toISOString(), totalOpportunities: risk.length,
    acceptedOpportunities: accepted, rejectedOpportunities: rejected, totalPaperOrders: byType("PAPER_ORDER_CREATED").length,
    totalFills: fills.length, makerFills, takerFills, completedPairs, incompleteUnhedgedPositions: unhedged.length,
    winLossDistribution: { wins: byType("POSITION_WIN").length, losses: byType("POSITION_LOSS").length, breakeven: byType("POSITION_BREAKEVEN").length },
    grossPnlUsd: grossPnl, feesUsd: Number(ledger?.fees ?? 0), slippageUsd: Number(ledger?.slippage ?? 0),
    netTradingPnlUsd: netTradingPnl, aiCostUsd: aiCost, estimatedInfrastructureCostUsd: infraCost,
    economicPnlUsd: Number(ledger?.netEconomicPnl ?? netTradingPnl - aiCost - infraCost),
    maximumDrawdownUsd: Math.max(0, ...pnlEvents.map((event) => Number((event.payload as { drawdown?: unknown }).drawdown ?? 0))),
    maximumDirectionalExposureUsd: Math.max(0, ...positions.map((position) => Number(position.unpairedQuantity) * Number(position.unpairedOutcome === "UP" ? position.averageUpCost : position.averageDownCost))),
    averageUnhedgedDurationMs: average(byType("HEDGE_COMPLETE").map((event) => Number((event.payload as { unhedgedDurationMs?: unknown }).unhedgedDurationMs ?? 0))),
    worstUnhedgedEvent: byType("UNHEDGED_RISK_EVENT").toSorted((a, b) => Number((b.payload as { exposureUsd?: unknown }).exposureUsd ?? 0) - Number((a.payload as { exposureUsd?: unknown }).exposureUsd ?? 0))[0]?.payload ?? null,
    averagePairCost: pairCosts.length ? average(pairCosts) : null,
    averageLockedPairEdge: pairCosts.length ? average(pairCosts.map((cost) => 1 - cost)) : null,
    averageRealizedEdge: averageOrNull(byType("PAIR_REALIZED").map((event) => Number((event.payload as { realizedEdge?: unknown }).realizedEdge ?? 0))),
    makerFillRate: rate(makerFills, byType("PAPER_MAKER_ORDER_CREATED").length), takerFillRate: rate(takerFills, byType("PAPER_TAKER_ORDER_CREATED").length || byType("PAPER_ORDER_CREATED").length),
    modelInvocationCounts: modelCounts, modelCostByAgentUsd: modelCosts,
    performanceByAgentSignal: groupPayload(byType("AGENT_SIGNAL_ATTRIBUTION"), "agentId"),
    performanceByMarketRegime: groupPayload(byType("REGIME_PERFORMANCE"), "regime"),
    performanceByHour: groupPayload(byType("HOURLY_PERFORMANCE"), "hour"),
    rejectedTradesThatWouldHaveWon: byType("REJECTED_TRADE_WOULD_HAVE_WON").length,
    acceptedTradesThatLost: byType("ACCEPTED_TRADE_LOST").length, failureStatistics,
    hedgeFailureRate: rate(byType("FAILED_HEDGE").length, byType("FIRST_LEG_FILLED").length),
    executableOpportunityRate: rate(fills.length, risk.length),
  };
}

export async function writeExperimentSummary(summary: ExperimentSummary, reportDirectory: string): Promise<{ jsonPath: string; markdownPath: string }> {
  await mkdir(reportDirectory, { recursive: true });
  const jsonPath = resolve(reportDirectory, "48h-summary.json");
  const markdownPath = resolve(reportDirectory, "48h-summary.md");
  await writeFile(jsonPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  await writeFile(markdownPath, renderExperimentSummaryMarkdown(summary), "utf8");
  return { jsonPath, markdownPath };
}

function latestByAggregate(events: readonly JournalEvent[]): JournalEvent[] {
  const map = new Map<string, JournalEvent>();
  for (const event of events) map.set(event.aggregateId, event);
  return [...map.values()];
}
function average(values: readonly number[]): number { return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0; }
function averageOrNull(values: readonly number[]): number | null { return values.length ? average(values) : null; }
function rate(numerator: number, denominator: number): number { return denominator > 0 ? numerator / denominator : 0; }
function groupPayload(events: readonly JournalEvent[], key: string): Record<string, unknown> {
  return Object.fromEntries(events.map((event) => [String(event.payload[key] ?? "unknown"), event.payload]));
}
