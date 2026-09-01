import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { d, renderExperimentSummaryMarkdown, type ExperimentSummary, type JournalEvent, type LedgerState, type MarketAssetAttribution, type PaperFill, type Position } from "@pm/core";

export function filterCurrentExperimentEvents(
  events: readonly JournalEvent[],
  experiment: { id: string; startedAt?: string | null; completedAt?: string | null },
): JournalEvent[] {
  if (!experiment.startedAt) return [];
  const from = Date.parse(experiment.startedAt);
  const to = experiment.completedAt ? Date.parse(experiment.completedAt) : Number.POSITIVE_INFINITY;
  return events.filter((event) => {
    const at = Date.parse(event.occurredAt);
    if (!Number.isFinite(at) || at < from || at > to) return false;
    const payloadId = payloadString(event.payload, "experimentId");
    if (payloadId && payloadId !== experiment.id) return false;
    if (event.aggregateType === "experiment" && event.aggregateId !== experiment.id && event.aggregateId !== "preflight") return false;
    return true;
  });
}

export function buildExperimentSummary(
  events: readonly JournalEvent[],
  experimentId = "unknown",
  extras: Partial<Pick<ExperimentSummary, "status" | "closePolicy" | "reportKind" | "unresolvedPositionCount" | "metricsScope" | "complete" | "truncated" | "truncatedReason">> = {},
): ExperimentSummary {
  const byType = (type: string) => events.filter((event) => event.type === type);
  const fills = byType("PAPER_FILL").map((event) => event.payload as unknown as PaperFill & { asset?: string | null });
  const settlements = byType("POSITION_SETTLED");
  const settledMarketIds = new Set(settlements.map((event) => payloadString(event.payload, "marketId") ?? event.aggregateId));
  const settledPositionIds = new Set(settlements.map((event) => event.aggregateId));
  const positions = latestByAggregate(byType("POSITION_UPDATED"))
    .map((event) => event.payload as unknown as Position)
    .filter((position) => {
      if (settledMarketIds.has(position.marketId) || settledPositionIds.has(position.id)) return false;
      return Number(position.upQuantity) > 0 || Number(position.downQuantity) > 0;
    });
  const settlementPnlUsd = settlements.reduce((sum, event) => sum + Number((event.payload as { realizedDelta?: unknown }).realizedDelta ?? 0), 0);
  const pnlEvents = byType("PNL_SNAPSHOT");
  const ledger = pnlEvents.at(-1)?.payload as unknown as LedgerState | undefined;
  const grokSignals = byType("GROK_SIGNAL_ACCEPTED");
  const agentOutputs = [...byType("AGENT_OUTPUT"), ...grokSignals];
  const risk = byType("RISK_EVALUATION");
  const accepted = risk.filter((event) => (event.payload as { accepted?: boolean }).accepted === true).length;
  const rejected = risk.length - accepted;
  const completedPairs = positions.filter((position) => Number(position.pairedQuantity) > 0 && Number(position.unpairedQuantity) === 0).length
    + settlements.filter((event) => Number((event.payload as { upQuantity?: unknown }).upQuantity ?? 0) > 0 && Number((event.payload as { downQuantity?: unknown }).downQuantity ?? 0) > 0).length;
  const unhedged = positions.filter((position) => Number(position.unpairedQuantity) > 0);
  const makerFills = fills.filter((fill) => fill.liquidityRole === "MAKER").length;
  const createdOrders = byType("PAPER_ORDER_CREATED");
  const makerOrders = createdOrders.filter((event) => {
    const payload = event.payload as { orderType?: unknown; type?: unknown };
    return payload.orderType === "MAKER" || payload.type === "MAKER";
  }).length;
  const takerOrders = createdOrders.filter((event) => {
    const payload = event.payload as { orderType?: unknown; type?: unknown };
    return payload.orderType === "TAKER" || payload.type === "TAKER";
  }).length;
  const firstLegCount = byType("FIRST_LEG_FILLED").length;
  const hedgeCompleteCount = byType("HEDGE_COMPLETE").length;
  const failedHedgeCount = countFailedHedgeEpisodes(events);
  const occurred = events.map((event) => event.occurredAt).filter(Boolean).toSorted();
  const extrasScope = extras.metricsScope ?? (extras.reportKind === "final" ? "complete" : "rolling");
  const truncated = extras.truncated === true;
  const extrasComplete = extras.complete ?? (extrasScope === "complete" && !truncated);
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
  const eventCountByMarket = countBy(events, "marketId");
  const eventCountByAsset = countBy(events, "asset");
  const byMarket = attributeTrading(events, fills, risk, "marketId");
  const byAsset = attributeTrading(events, fills, risk, "asset");
  return {
    experimentId, generatedAt: new Date().toISOString(),
    status: extras.status ?? String((ledger as { experimentStatus?: unknown } | undefined)?.experimentStatus ?? "unknown"),
    closePolicy: extras.closePolicy ?? "observe_until_settlement",
    reportKind: extras.reportKind ?? "interim",
    unresolvedPositionCount: extras.unresolvedPositionCount ?? unhedged.length,
    settlementCount: settlements.length,
    settlementPnlUsd,
    totalOpportunities: risk.length,
    acceptedOpportunities: accepted, rejectedOpportunities: rejected, totalPaperOrders: byType("PAPER_ORDER_CREATED").length,
    totalFills: fills.length, makerFills, takerFills, completedPairs, incompleteUnhedgedPositions: unhedged.length,
    winLossDistribution: netWinLoss(events),
    grossWinLossDistribution: grossWinLoss(events),
    grossPnlUsd: grossPnl, feesUsd: Number(ledger?.fees ?? 0), slippageUsd: Number(ledger?.slippage ?? 0),
    netTradingPnlUsd: netTradingPnl, aiCostUsd: aiCost, estimatedInfrastructureCostUsd: infraCost,
    economicPnlUsd: Number(ledger?.netEconomicPnl ?? netTradingPnl - aiCost - infraCost),
    maximumDrawdownUsd: Math.max(0, ...pnlEvents.map((event) => Number((event.payload as { drawdown?: unknown }).drawdown ?? 0))),
    maximumDirectionalExposureUsd: Math.max(0, ...positions.map((position) => Number(position.unpairedQuantity) * Number(position.unpairedOutcome === "UP" ? position.averageUpCost : position.averageDownCost))),
    averageUnhedgedDurationMs: average(byType("HEDGE_COMPLETE").map((event) => Number((event.payload as { unhedgedDurationMs?: unknown }).unhedgedDurationMs)).filter((value) => Number.isFinite(value))),
    worstUnhedgedEvent: byType("UNHEDGED_RISK_EVENT").toSorted((a, b) => Number((b.payload as { exposureUsd?: unknown }).exposureUsd ?? 0) - Number((a.payload as { exposureUsd?: unknown }).exposureUsd ?? 0))[0]?.payload ?? null,
    averagePairCost: pairCosts.length ? average(pairCosts) : null,
    averageLockedPairEdge: pairCosts.length ? average(pairCosts.map((cost) => 1 - cost)) : null,
    averageRealizedEdge: averageOrNull(byType("PAIR_REALIZED").map((event) => netRealizedEdgeOf(event))),
    averageGrossLockedEdge: averageOrNull(byType("PAIR_REALIZED").map((event) => Number((event.payload as { grossLockedEdge?: unknown }).grossLockedEdge ?? (1 - Number((event.payload as { combinedPairCost?: unknown }).combinedPairCost ?? 0))))),
    averageNetRealizedEdge: averageOrNull(byType("PAIR_REALIZED").map((event) => netRealizedEdgeOf(event))),
    makerFillRate: rate(makerFills, makerOrders), takerFillRate: rate(takerFills, takerOrders),
    modelInvocationCounts: modelCounts, modelCostByAgentUsd: modelCosts,
    performanceByAgentSignal: groupPayload(byType("AGENT_SIGNAL_ATTRIBUTION"), "agentId"),
    performanceByMarket: byMarket, performanceByAsset: byAsset,
    eventCountByMarket, eventCountByAsset,
    pnlByMarket: pick(byMarket, "netTradingPnlUsd"), pnlByAsset: pick(byAsset, "netTradingPnlUsd"),
    fillsByMarket: pick(byMarket, "fills"), fillsByAsset: pick(byAsset, "fills"),
    feesByMarket: pick(byMarket, "feesUsd"), feesByAsset: pick(byAsset, "feesUsd"),
    slippageByMarket: pick(byMarket, "slippageUsd"), slippageByAsset: pick(byAsset, "slippageUsd"),
    opportunitiesByMarket: pick(byMarket, "opportunities"), opportunitiesByAsset: pick(byAsset, "opportunities"),
    vetoesByMarket: pick(byMarket, "vetoes"), vetoesByAsset: pick(byAsset, "vetoes"),
    performanceByMarketRegime: groupPayload(byType("REGIME_PERFORMANCE"), "regime"),
    performanceByHour: groupPayload(byType("HOURLY_PERFORMANCE"), "hour"),
    rejectedTradesThatWouldHaveWon: byType("REJECTED_TRADE_WOULD_HAVE_WON").length,
    acceptedTradesThatLost: byType("ACCEPTED_TRADE_LOST").length, failureStatistics,
    hedgeFailureRate: rate(failedHedgeCount, firstLegCount),
    executableOpportunityRate: rate(fills.length, risk.length),
    metricsScope: extrasScope,
    complete: extrasComplete && !truncated,
    truncated,
    truncatedReason: truncated ? (extras.truncatedReason ?? "SCAN_CEILING") : null,
    earliestOccurredAt: occurred[0] ?? null,
    latestOccurredAt: occurred.at(-1) ?? null,
    decisionAccountingEventCount: events.length,
    firstLegCount,
    hedgeCompleteCount,
    failedHedgeCount,
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

function attributeTrading(
  events: readonly JournalEvent[],
  fills: Array<PaperFill & { asset?: string | null }>,
  risk: readonly JournalEvent[],
  key: "marketId" | "asset",
): Record<string, MarketAssetAttribution> {
  const assetByMarket = inferAssetByMarket(events);
  const empty = (): MarketAssetAttribution => ({
    realizedPnlUsd: 0, feesUsd: 0, slippageUsd: 0, settlementPnlUsd: 0, netTradingPnlUsd: 0, fills: 0, opportunities: 0, accepted: 0, vetoes: 0,
  });
  const result: Record<string, MarketAssetAttribution> = {};
  const bucket = (id: string | null | undefined) => {
    if (!id) return null;
    const row = result[id] ?? empty();
    result[id] = row;
    return row;
  };
  const fillsByMarket = new Map<string, PaperFill[]>();
  for (const fill of fills) {
    const list = fillsByMarket.get(fill.marketId) ?? [];
    list.push(fill);
    fillsByMarket.set(fill.marketId, list);
  }
  for (const [marketId, marketFills] of fillsByMarket) {
    const pnl = replayFills(marketFills);
    const targets = key === "marketId" ? [marketId] : [assetByMarket.get(marketId) ?? payloadString(marketFills[0], "asset")].filter((item): item is string => Boolean(item));
    for (const id of targets) {
      const row = bucket(id);
      if (!row) continue;
      row.realizedPnlUsd += pnl.realizedPnlUsd;
      row.feesUsd += pnl.feesUsd;
      row.slippageUsd += pnl.slippageUsd;
      row.fills += marketFills.length;
    }
  }
  for (const event of events.filter((item) => item.type === "POSITION_SETTLED")) {
    const marketId = payloadString(event.payload, "marketId") ?? (event.aggregateType === "position" ? event.aggregateId : null);
    const realized = Number((event.payload as { realizedDelta?: unknown }).realizedDelta ?? 0);
    const targets = key === "marketId"
      ? [marketId]
      : [payloadString(event.payload, "asset") ?? (marketId ? assetByMarket.get(marketId) : null)].filter((item): item is string => Boolean(item));
    for (const id of targets) {
      const row = bucket(id);
      if (!row) continue;
      row.settlementPnlUsd += realized;
      row.realizedPnlUsd += realized;
    }
  }
  for (const event of risk) {
    const id = key === "marketId"
      ? payloadString(event.payload, "marketId") ?? (event.aggregateType === "risk" ? null : event.aggregateId)
      : payloadString(event.payload, "asset") ?? (payloadString(event.payload, "marketId") ? assetByMarket.get(String(event.payload.marketId)) ?? null : null);
    const row = bucket(id);
    if (!row) continue;
    row.opportunities += 1;
    if ((event.payload as { accepted?: boolean }).accepted === true) row.accepted += 1;
    else row.vetoes += 1;
  }
  for (const row of Object.values(result)) {
    row.netTradingPnlUsd = d(row.realizedPnlUsd).minus(row.feesUsd).toNumber();
  }
  return result;
}

function replayFills(fills: readonly PaperFill[]): { realizedPnlUsd: number; feesUsd: number; slippageUsd: number } {
  let upQty = d(0); let downQty = d(0); let upCost = d(0); let downCost = d(0);
  let realized = d(0); let fees = d(0); let slippage = d(0);
  for (const fill of fills) {
    const qty = d(fill.quantity);
    const price = d(fill.fillPrice);
    fees = fees.plus(fill.feeUsd);
    slippage = slippage.plus(fill.slippageUsd);
    if (fill.outcome === "UP") {
      if (fill.side === "BUY") {
        const next = upQty.plus(qty);
        upCost = next.gt(0) ? upQty.mul(upCost).plus(qty.mul(price)).div(next) : d(0);
        upQty = next;
      } else {
        realized = realized.plus(qty.mul(price.minus(upCost)));
        upQty = upQty.minus(qty);
        if (upQty.lte(0)) { upQty = d(0); upCost = d(0); }
      }
    } else if (fill.side === "BUY") {
      const next = downQty.plus(qty);
      downCost = next.gt(0) ? downQty.mul(downCost).plus(qty.mul(price)).div(next) : d(0);
      downQty = next;
    } else {
      realized = realized.plus(qty.mul(price.minus(downCost)));
      downQty = downQty.minus(qty);
      if (downQty.lte(0)) { downQty = d(0); downCost = d(0); }
    }
  }
  return { realizedPnlUsd: realized.toNumber(), feesUsd: fees.toNumber(), slippageUsd: slippage.toNumber() };
}

function inferAssetByMarket(events: readonly JournalEvent[]): Map<string, string> {
  const map = new Map<string, string>();
  for (const event of events) {
    const marketId = payloadString(event.payload, "marketId");
    const asset = payloadString(event.payload, "asset");
    if (marketId && asset) map.set(marketId, asset);
    const markets = event.payload.markets;
    if (Array.isArray(markets)) {
      for (const item of markets) {
        if (!item || typeof item !== "object") continue;
        const id = payloadString(item as Record<string, unknown>, "id") ?? payloadString(item as Record<string, unknown>, "marketId");
        const marketAsset = payloadString(item as Record<string, unknown>, "asset");
        if (id && marketAsset) map.set(id, marketAsset);
      }
    }
  }
  return map;
}

function payloadString(payload: unknown, key: string): string | null {
  if (!payload || typeof payload !== "object") return null;
  const value = (payload as Record<string, unknown>)[key];
  return typeof value === "string" && value.length > 0 ? value : null;
}

function pick(rows: Record<string, MarketAssetAttribution>, field: keyof MarketAssetAttribution): Record<string, number> {
  return Object.fromEntries(Object.entries(rows).map(([id, row]) => [id, row[field]]));
}

function latestByAggregate(events: readonly JournalEvent[]): JournalEvent[] {
  const map = new Map<string, JournalEvent>();
  for (const event of events) map.set(event.aggregateId, event);
  return [...map.values()];
}
function average(values: readonly number[]): number { return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0; }
function averageOrNull(values: readonly number[]): number | null {
  const finite = values.filter((value) => Number.isFinite(value));
  return finite.length ? average(finite) : null;
}
function netRealizedEdgeOf(event: JournalEvent): number {
  const payload = event.payload as { netRealizedEdge?: unknown; realizedEdge?: unknown };
  if (typeof payload.netRealizedEdge === "number") return payload.netRealizedEdge;
  return Number(payload.realizedEdge ?? 0);
}
function isFailedHedgeEvent(event: JournalEvent): boolean {
  if (event.type === "FAILED_HEDGE") return true;
  return event.type === "TRIGGER_EVENT" && (event.payload as { kind?: unknown }).kind === "FAILED_HEDGE";
}
function countFailedHedgeEpisodes(events: readonly JournalEvent[]): number {
  const open = new Set<string>();
  let count = 0;
  for (const event of events) {
    const marketId = payloadString(event.payload, "marketId")
      ?? (event.aggregateType === "position" || event.aggregateType === "trigger" ? event.aggregateId : null);
    if (isFailedHedgeEvent(event)) {
      const key = marketId ?? event.aggregateId;
      if (!open.has(key)) {
        open.add(key);
        count += 1;
      }
      continue;
    }
    if (event.type === "HEDGE_COMPLETE" || event.type === "POSITION_SETTLED") {
      if (marketId) open.delete(marketId);
      open.delete(event.aggregateId);
    }
  }
  return count;
}
function netWinLoss(events: readonly JournalEvent[]): { wins: number; losses: number; breakeven: number } {
  return {
    wins: events.filter((event) => event.type === "POSITION_WIN").length,
    losses: events.filter((event) => event.type === "POSITION_LOSS").length,
    breakeven: events.filter((event) => event.type === "POSITION_BREAKEVEN").length,
  };
}
function grossWinLoss(events: readonly JournalEvent[]): { wins: number; losses: number; breakeven: number } {
  const settled = events.filter((event) => event.type === "POSITION_SETTLED" || event.type === "POSITION_WIN" || event.type === "POSITION_LOSS" || event.type === "POSITION_BREAKEVEN");
  const counts = { wins: 0, losses: 0, breakeven: 0 };
  const seen = new Set<string>();
  for (const event of settled) {
    const key = event.aggregateId;
    if (event.type === "POSITION_SETTLED") {
      const gross = (event.payload as { grossOutcome?: unknown }).grossOutcome;
      if (gross === "POSITION_WIN" || gross === "POSITION_LOSS" || gross === "POSITION_BREAKEVEN") {
        if (seen.has(key)) continue;
        seen.add(key);
        if (gross === "POSITION_WIN") counts.wins += 1;
        else if (gross === "POSITION_LOSS") counts.losses += 1;
        else counts.breakeven += 1;
      }
    }
  }
  if (counts.wins + counts.losses + counts.breakeven === 0) return netWinLoss(events);
  return counts;
}
function rate(numerator: number, denominator: number): number { return denominator > 0 ? numerator / denominator : 0; }
function groupPayload(events: readonly JournalEvent[], key: string): Record<string, unknown> {
  return Object.fromEntries(events.map((event) => [String(event.payload[key] ?? "unknown"), event.payload]));
}
function countBy(events: readonly JournalEvent[], key: string): Record<string, number> {
  const counts: Record<string, number> = {};
  for (const event of events) {
    const payload = event.payload as Record<string, unknown>;
    const value = payload[key] ?? (event.aggregateType === "market" || event.aggregateType === "position" ? event.aggregateId : null);
    if (typeof value !== "string" || value.length === 0) continue;
    counts[value] = (counts[value] ?? 0) + 1;
  }
  return counts;
}
