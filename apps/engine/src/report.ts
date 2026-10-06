import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { d, renderExperimentSummaryMarkdown, type ExperimentSummary, type GuardianOutcomeCounts, type JournalEvent, type LedgerState, type MarketAssetAttribution, type PaperFill, type Position } from "@pm/core";

export function includeCurrentExperimentEvent(
  event: JournalEvent,
  experiment: { id: string; startedAt?: string | null; completedAt?: string | null },
): boolean {
  if (!experiment.startedAt) return false;
  const from = Date.parse(experiment.startedAt);
  const to = experiment.completedAt ? Date.parse(experiment.completedAt) : Number.POSITIVE_INFINITY;
  const at = Date.parse(event.occurredAt);
  if (!Number.isFinite(at) || at < from || at > to) return false;
  const payloadId = payloadString(event.payload, "experimentId");
  if (payloadId && payloadId !== experiment.id) return false;
  if (event.aggregateType === "experiment" && event.aggregateId !== experiment.id && event.aggregateId !== "preflight") return false;
  return true;
}

export function filterCurrentExperimentEvents(
  events: readonly JournalEvent[],
  experiment: { id: string; startedAt?: string | null; completedAt?: string | null },
): JournalEvent[] {
  return events.filter((event) => includeCurrentExperimentEvent(event, experiment));
}

export type ExperimentSummaryExtras = Partial<Pick<ExperimentSummary,
  "status" | "closePolicy" | "reportKind" | "unresolvedPositionCount" | "metricsScope" | "complete" | "truncated" | "truncatedReason" | "generatedAt"
>>;

export interface ExperimentSummaryAccumulatorSnapshot {
  guardianOutcomes?: GuardianOutcomeCounts;
  nakedDirectionalDiagnostics?: Array<[string, Record<string, unknown>]>;
  schemaVersion: number;
  experimentId: string;
  extras: ExperimentSummaryExtras;
  eventCount: number;
  earliestOccurredAt: string | null;
  latestOccurredAt: string | null;
  fillReplay: Array<{ marketId: string; upQty: string; downQty: string; upCost: string; downCost: string; realized: string; fees: string; slippage: string; fills: number }>;
  latestPositions: Array<[string, Position]>;
  settledMarketIds: string[];
  settledPositionIds: string[];
  settlementCount: number;
  settlementPnlUsd: number;
  settlementBothQty: number;
  lastLedger?: LedgerState;
  maxDrawdown: number;
  maxDirectionalExposureUsd?: number;
  riskCount: number;
  accepted: number;
  paperOrders: number;
  makerOrders: number;
  takerOrders: number;
  fillCount: number;
  makerFills: number;
  takerFills: number;
  firstLegCount: number;
  hedgeCompleteCount: number;
  hedgeDurationSum: number;
  hedgeDurationN: number;
  pairRealizedNetSum: number;
  pairRealizedNetN: number;
  pairRealizedGrossSum: number;
  pairRealizedGrossN: number;
  modelCounts: Record<string, number>;
  modelCosts: Record<string, number>;
  failureStatistics: Record<string, number>;
  eventCountByMarket: Record<string, number>;
  eventCountByAsset: Record<string, number>;
  assetByMarket: Array<[string, string]>;
  byMarket: Record<string, MarketAssetAttribution>;
  byAsset: Record<string, MarketAssetAttribution>;
  failedHedgeOpen: string[];
  failedHedgeCount: number;
  positionWins: number;
  positionLosses: number;
  positionBreakeven: number;
  grossWins: number;
  grossLosses: number;
  grossBreakeven: number;
  grossSeen: string[];
  rejectedWouldHaveWon: number;
  acceptedLost: number;
  worstUnhedgedEvent: Record<string, unknown> | null;
  worstUnhedgedUsd: number;
  agentSignals: Record<string, unknown>;
  regimes: Record<string, unknown>;
  hours: Record<string, unknown>;
}

export class ExperimentSummaryAccumulator {
  private guardianOutcomes: GuardianOutcomeCounts = { earlyExits: 0, lateExits: 0, pairedSettlements: 0, nakedSettlements: 0 };
  private readonly nakedDirectionalDiagnostics = new Map<string, Record<string, unknown>>();
  private readonly extras: ExperimentSummaryExtras;
  private readonly experimentId: string;
  private eventCount = 0;
  private earliestOccurredAt: string | null = null;
  private latestOccurredAt: string | null = null;
  private readonly fillReplay = new Map<string, ReturnType<typeof createFillReplay>>();
  private readonly latestPositions = new Map<string, Position>();
  private readonly settledMarketIds = new Set<string>();
  private readonly settledPositionIds = new Set<string>();
  private settlementCount = 0;
  private settlementPnlUsd = 0;
  private settlementBothQty = 0;
  private lastLedger: LedgerState | undefined;
  private maxDrawdown = 0;
  private maxDirectionalExposureUsd = 0;
  private riskCount = 0;
  private accepted = 0;
  private paperOrders = 0;
  private makerOrders = 0;
  private takerOrders = 0;
  private fillCount = 0;
  private makerFills = 0;
  private takerFills = 0;
  private firstLegCount = 0;
  private hedgeCompleteCount = 0;
  private hedgeDurationSum = 0;
  private hedgeDurationN = 0;
  private pairRealizedNetSum = 0;
  private pairRealizedNetN = 0;
  private pairRealizedGrossSum = 0;
  private pairRealizedGrossN = 0;
  private readonly modelCounts: Record<string, number> = {};
  private readonly modelCosts: Record<string, number> = {};
  private readonly failureStatistics: Record<string, number> = {};
  private readonly eventCountByMarket: Record<string, number> = {};
  private readonly eventCountByAsset: Record<string, number> = {};
  private readonly assetByMarket = new Map<string, string>();
  private readonly byMarket: Record<string, MarketAssetAttribution> = {};
  private readonly byAsset: Record<string, MarketAssetAttribution> = {};
  private readonly failedHedgeOpen = new Set<string>();
  private failedHedgeCount = 0;
  private positionWins = 0;
  private positionLosses = 0;
  private positionBreakeven = 0;
  private grossWins = 0;
  private grossLosses = 0;
  private grossBreakeven = 0;
  private readonly grossSeen = new Set<string>();
  private rejectedWouldHaveWon = 0;
  private acceptedLost = 0;
  private worstUnhedgedEvent: Record<string, unknown> | null = null;
  private worstUnhedgedUsd = Number.NEGATIVE_INFINITY;
  private readonly agentSignals: Record<string, unknown> = {};
  private readonly regimes: Record<string, unknown> = {};
  private readonly hours: Record<string, unknown> = {};
  private highWaterBytes = 0;

  constructor(experimentId: string, extras: ExperimentSummaryExtras = {}) {
    this.experimentId = experimentId;
    this.extras = extras;
  }

  get retainedEventCount(): number { return 0; }
  get sourceEventCount(): number { return this.eventCount; }
  /** Estimator of retained structure size — not a JS heap measurement. */
  get highWaterMarkBytes(): number { return this.highWaterBytes; }
  /** Structural cardinality of retained maps — not event count and not JS heap. */
  get retainedEntityCardinality(): number {
    return this.fillReplay.size + this.latestPositions.size + Object.keys(this.byMarket).length + Object.keys(this.byAsset).length;
  }
  get retainedMarketCardinality(): number {
    return new Set([...this.fillReplay.keys(), ...this.latestPositions.values()].map((item) => typeof item === "string" ? item : item.marketId)).size
      || Object.keys(this.byMarket).length;
  }

  /** JSON-safe bounded rollup state. No raw JournalEvent objects are retained. */
  serialize(): ExperimentSummaryAccumulatorSnapshot {
    return {
      guardianOutcomes: { ...this.guardianOutcomes }, schemaVersion: 1, experimentId: this.experimentId, extras: structuredClone(this.extras), eventCount: this.eventCount,
      ...(this.nakedDirectionalDiagnostics.size ? { nakedDirectionalDiagnostics: structuredClone([...this.nakedDirectionalDiagnostics]) } : {}),
      earliestOccurredAt: this.earliestOccurredAt, latestOccurredAt: this.latestOccurredAt,
      fillReplay: [...this.fillReplay.entries()].map(([marketId, s]) => ({ marketId, upQty: s.upQty.toString(), downQty: s.downQty.toString(), upCost: s.upCost.toString(), downCost: s.downCost.toString(), realized: s.realized.toString(), fees: s.fees.toString(), slippage: s.slippage.toString(), fills: s.fills })),
      latestPositions: [...this.latestPositions.entries()].map(([id, position]) => [id, structuredClone(position)]),
      settledMarketIds: [...this.settledMarketIds], settledPositionIds: [...this.settledPositionIds], settlementCount: this.settlementCount,
      settlementPnlUsd: this.settlementPnlUsd, settlementBothQty: this.settlementBothQty, lastLedger: this.lastLedger ? structuredClone(this.lastLedger) : undefined,
      maxDrawdown: this.maxDrawdown, maxDirectionalExposureUsd: this.maxDirectionalExposureUsd,
      riskCount: this.riskCount, accepted: this.accepted, paperOrders: this.paperOrders,
      makerOrders: this.makerOrders, takerOrders: this.takerOrders, fillCount: this.fillCount, makerFills: this.makerFills, takerFills: this.takerFills,
      firstLegCount: this.firstLegCount, hedgeCompleteCount: this.hedgeCompleteCount, hedgeDurationSum: this.hedgeDurationSum, hedgeDurationN: this.hedgeDurationN,
      pairRealizedNetSum: this.pairRealizedNetSum, pairRealizedNetN: this.pairRealizedNetN, pairRealizedGrossSum: this.pairRealizedGrossSum, pairRealizedGrossN: this.pairRealizedGrossN,
      modelCounts: structuredClone(this.modelCounts), modelCosts: structuredClone(this.modelCosts), failureStatistics: structuredClone(this.failureStatistics),
      eventCountByMarket: structuredClone(this.eventCountByMarket), eventCountByAsset: structuredClone(this.eventCountByAsset), assetByMarket: [...this.assetByMarket.entries()],
      byMarket: structuredClone(this.byMarket), byAsset: structuredClone(this.byAsset), failedHedgeOpen: [...this.failedHedgeOpen], failedHedgeCount: this.failedHedgeCount,
      positionWins: this.positionWins, positionLosses: this.positionLosses, positionBreakeven: this.positionBreakeven, grossWins: this.grossWins, grossLosses: this.grossLosses, grossBreakeven: this.grossBreakeven, grossSeen: [...this.grossSeen],
      rejectedWouldHaveWon: this.rejectedWouldHaveWon, acceptedLost: this.acceptedLost, worstUnhedgedEvent: this.worstUnhedgedEvent ? structuredClone(this.worstUnhedgedEvent) : null, worstUnhedgedUsd: this.worstUnhedgedUsd,
      agentSignals: structuredClone(this.agentSignals), regimes: structuredClone(this.regimes), hours: structuredClone(this.hours),
    };
  }
  snapshot(): ExperimentSummaryAccumulatorSnapshot { return this.serialize(); }
  restore(snapshot: ExperimentSummaryAccumulatorSnapshot): void {
    if (snapshot.schemaVersion !== 1 || snapshot.experimentId !== this.experimentId) throw new Error("ANALYTICS_ROLLUP_SCHEMA_MISMATCH");
    this.guardianOutcomes = { earlyExits: 0, lateExits: 0, pairedSettlements: 0, nakedSettlements: 0, ...snapshot.guardianOutcomes };
    this.nakedDirectionalDiagnostics.clear();
    for (const [id, diagnostic] of snapshot.nakedDirectionalDiagnostics ?? []) this.nakedDirectionalDiagnostics.set(id, structuredClone(diagnostic));
    const assign = (key: keyof ExperimentSummaryAccumulatorSnapshot, fallback: unknown = undefined) => (snapshot[key] ?? fallback) as never;
    this.eventCount = assign("eventCount", 0); this.earliestOccurredAt = assign("earliestOccurredAt", null); this.latestOccurredAt = assign("latestOccurredAt", null);
    this.fillReplay.clear(); for (const item of snapshot.fillReplay ?? []) { const s = createFillReplay(); s.upQty = d(item.upQty); s.downQty = d(item.downQty); s.upCost = d(item.upCost); s.downCost = d(item.downCost); s.realized = d(item.realized); s.fees = d(item.fees); s.slippage = d(item.slippage); s.fills = item.fills; this.fillReplay.set(item.marketId, s); }
    this.latestPositions.clear(); for (const [id, position] of snapshot.latestPositions ?? []) this.latestPositions.set(id, structuredClone(position));
    this.settledMarketIds.clear(); for (const id of snapshot.settledMarketIds ?? []) this.settledMarketIds.add(id); this.settledPositionIds.clear(); for (const id of snapshot.settledPositionIds ?? []) this.settledPositionIds.add(id);
    this.settlementCount = assign("settlementCount", 0); this.settlementPnlUsd = assign("settlementPnlUsd", 0); this.settlementBothQty = assign("settlementBothQty", 0); this.lastLedger = snapshot.lastLedger ? structuredClone(snapshot.lastLedger) : undefined;
    this.maxDirectionalExposureUsd = assign("maxDirectionalExposureUsd", 0);
    for (const key of ["maxDrawdown","riskCount","accepted","paperOrders","makerOrders","takerOrders","fillCount","makerFills","takerFills","firstLegCount","hedgeCompleteCount","hedgeDurationSum","hedgeDurationN","pairRealizedNetSum","pairRealizedNetN","pairRealizedGrossSum","pairRealizedGrossN","failedHedgeCount","positionWins","positionLosses","positionBreakeven","grossWins","grossLosses","grossBreakeven","rejectedWouldHaveWon","acceptedLost","worstUnhedgedUsd"] as const) (this as unknown as Record<string, unknown>)[key] = snapshot[key];
    const replaceRecord = (target: Record<string, unknown>, source: Record<string, unknown> | undefined) => { for (const key of Object.keys(target)) delete target[key]; Object.assign(target, structuredClone(source ?? {})); };
    replaceRecord(this.modelCounts, snapshot.modelCounts); replaceRecord(this.modelCosts, snapshot.modelCosts); replaceRecord(this.failureStatistics, snapshot.failureStatistics); replaceRecord(this.eventCountByMarket, snapshot.eventCountByMarket); replaceRecord(this.eventCountByAsset, snapshot.eventCountByAsset); replaceRecord(this.byMarket, snapshot.byMarket); replaceRecord(this.byAsset, snapshot.byAsset); replaceRecord(this.agentSignals, snapshot.agentSignals); replaceRecord(this.regimes, snapshot.regimes); replaceRecord(this.hours, snapshot.hours);
    this.assetByMarket.clear(); for (const [key, value] of snapshot.assetByMarket ?? []) this.assetByMarket.set(key, value); this.failedHedgeOpen.clear(); for (const key of snapshot.failedHedgeOpen ?? []) this.failedHedgeOpen.add(key); this.grossSeen.clear(); for (const key of snapshot.grossSeen ?? []) this.grossSeen.add(key); this.worstUnhedgedEvent = snapshot.worstUnhedgedEvent ? structuredClone(snapshot.worstUnhedgedEvent) : null; this.highWaterBytes = 0; this.refreshHighWater();
  }
  static fromSnapshot(snapshot: ExperimentSummaryAccumulatorSnapshot): ExperimentSummaryAccumulator { const acc = new ExperimentSummaryAccumulator(snapshot.experimentId, snapshot.extras); acc.restore(snapshot); return acc; }

  fold(event: JournalEvent): void {
    this.eventCount += 1;
    if (!this.earliestOccurredAt || event.occurredAt < this.earliestOccurredAt) this.earliestOccurredAt = event.occurredAt;
    if (!this.latestOccurredAt || event.occurredAt > this.latestOccurredAt) this.latestOccurredAt = event.occurredAt;
    this.noteAsset(event);
    this.bumpCount(event, "marketId", this.eventCountByMarket);
    this.bumpCount(event, "asset", this.eventCountByAsset);
    const type = event.type;
    if (type === "GUARDIAN_EXECUTION_COMPLETED") {
      if (event.payload.sensor === "EARLY") this.guardianOutcomes.earlyExits++;
      if (event.payload.sensor === "LATE") this.guardianOutcomes.lateExits++;
    } else if (type === "NAKED_DIRECTIONAL_SUMMARY") {
      this.nakedDirectionalDiagnostics.set(event.aggregateId, structuredClone(event.payload));
    } else if (type === "PAPER_FILL") {
      const fill = event.payload as unknown as PaperFill & { asset?: string | null };
      this.fillCount += 1;
      if (fill.liquidityRole === "MAKER") this.makerFills += 1;
      if (fill.liquidityRole === "TAKER") this.takerFills += 1;
      const replay = this.fillReplay.get(fill.marketId) ?? createFillReplay();
      foldFill(replay, fill);
      this.fillReplay.set(fill.marketId, replay);
      const fillAsset = payloadString(fill, "asset");
      if (fill.marketId && fillAsset) this.assetByMarket.set(fill.marketId, fillAsset);
    } else if (type === "POSITION_SETTLED") {
      const up = d(String(event.payload.upQuantity ?? 0)), down = d(String(event.payload.downQuantity ?? 0));
      if (up.eq(down) && up.gt(0)) this.guardianOutcomes.pairedSettlements++;
      else this.guardianOutcomes.nakedSettlements++;
      this.settlementCount += 1;
      this.settlementPnlUsd += Number((event.payload as { realizedDelta?: unknown }).realizedDelta ?? 0);
      this.settledMarketIds.add(payloadString(event.payload, "marketId") ?? event.aggregateId);
      this.settledPositionIds.add(event.aggregateId);
      if (Number((event.payload as { upQuantity?: unknown }).upQuantity ?? 0) > 0 && Number((event.payload as { downQuantity?: unknown }).downQuantity ?? 0) > 0) {
        this.settlementBothQty += 1;
      }
      const realized = Number((event.payload as { realizedDelta?: unknown }).realizedDelta ?? 0);
      this.addSettlementAttribution(event, realized);
      const gross = (event.payload as { grossOutcome?: unknown }).grossOutcome;
      if (gross === "POSITION_WIN" || gross === "POSITION_LOSS" || gross === "POSITION_BREAKEVEN") {
        if (!this.grossSeen.has(event.aggregateId)) {
          this.grossSeen.add(event.aggregateId);
          if (gross === "POSITION_WIN") this.grossWins += 1;
          else if (gross === "POSITION_LOSS") this.grossLosses += 1;
          else this.grossBreakeven += 1;
        }
      }
    } else if (type === "POSITION_UPDATED") {
      const position = event.payload as unknown as Position;
      const previous = this.latestPositions.get(event.aggregateId);
      // FIRST_LEG_FILLED counts the initial cycle. A later fully-paired -> unpaired
      // transition is a fresh opening cycle even though the Position object persists.
      if (previous && Number(previous.unpairedQuantity) <= 0 && Number(position.unpairedQuantity) > 0) {
        this.firstLegCount += 1;
      }
      this.maxDirectionalExposureUsd = Math.max(this.maxDirectionalExposureUsd, directionalExposureUsd(position));
      this.latestPositions.set(event.aggregateId, position);
    } else if (type === "PNL_SNAPSHOT") {
      this.lastLedger = event.payload as unknown as LedgerState;
      this.maxDrawdown = Math.max(this.maxDrawdown, Number((event.payload as { drawdown?: unknown }).drawdown ?? 0));
    } else if (type === "RISK_EVALUATION") {
      const payload = event.payload as { accepted?: boolean; intent?: unknown; deRisk?: unknown };
      const protective = payload.intent === "PROTECTIVE_DE_RISK" || payload.deRisk != null;
      if (!protective) {
        this.riskCount += 1;
        if (payload.accepted === true) this.accepted += 1;
        this.addRiskAttribution(event);
      }
    } else if (type === "PAPER_ORDER_CREATED") {
      this.paperOrders += 1;
      const payload = event.payload as { orderType?: unknown; type?: unknown };
      if (payload.orderType === "MAKER" || payload.type === "MAKER") this.makerOrders += 1;
      if (payload.orderType === "TAKER" || payload.type === "TAKER") this.takerOrders += 1;
    } else if (type === "FIRST_LEG_FILLED") this.firstLegCount += 1;
    else if (type === "HEDGE_COMPLETE") {
      const completionMode = (event.payload as { completionMode?: unknown }).completionMode;
      if (completionMode !== "PROTECTIVE_DE_RISK") {
        this.hedgeCompleteCount += 1;
        const duration = Number((event.payload as { unhedgedDurationMs?: unknown }).unhedgedDurationMs);
        if (Number.isFinite(duration)) {
          this.hedgeDurationSum += duration;
          this.hedgeDurationN += 1;
        }
      }
    } else if (type === "CONSENSUS_DECISION" && (event.payload as { authority?: unknown }).authority === "NATIVE_DETERMINISTIC_V1") {
      this.modelCounts.NATIVE_DETERMINISTIC_V1 = (this.modelCounts.NATIVE_DETERMINISTIC_V1 ?? 0) + 1;
      this.modelCosts.NATIVE_DETERMINISTIC_V1 = this.modelCosts.NATIVE_DETERMINISTIC_V1 ?? 0;
    } else if (type === "GROK_SIGNAL_ACCEPTED" || type === "AGENT_OUTPUT") {
      const output = event.payload as { agentId?: string; usage?: { estimatedCostUsd?: number | null } };
      if (output.agentId) {
        this.modelCounts[output.agentId] = (this.modelCounts[output.agentId] ?? 0) + 1;
        this.modelCosts[output.agentId] = (this.modelCosts[output.agentId] ?? 0) + (output.usage?.estimatedCostUsd ?? 0);
      }
    } else if (type === "AGENT_FAILURE") {
      const code = String((event.payload as { code?: unknown }).code ?? "UNKNOWN");
      this.failureStatistics[code] = (this.failureStatistics[code] ?? 0) + 1;
    } else if (type === "MCP_INTERACTION") {
      const status = String((event.payload as { status?: unknown }).status ?? "MCP");
      if (status !== "HANDLED") this.failureStatistics[`MCP_${status}`] = (this.failureStatistics[`MCP_${status}`] ?? 0) + 1;
    } else if (type === "PAPER_EXECUTION_ERROR") {
      if ((event.payload as { stateChanged?: unknown }).stateChanged !== true) {
        this.failureStatistics.PAPER_EXECUTION_ERROR = (this.failureStatistics.PAPER_EXECUTION_ERROR ?? 0) + 1;
      }
    } else if (type === "PAIR_REALIZED") {
      const net = netRealizedEdgeOf(event);
      if (Number.isFinite(net)) { this.pairRealizedNetSum += net; this.pairRealizedNetN += 1; }
      const gross = Number((event.payload as { grossLockedEdge?: unknown }).grossLockedEdge ?? (1 - Number((event.payload as { combinedPairCost?: unknown }).combinedPairCost ?? 0)));
      if (Number.isFinite(gross)) { this.pairRealizedGrossSum += gross; this.pairRealizedGrossN += 1; }
    } else if (type === "POSITION_WIN") this.positionWins += 1;
    else if (type === "POSITION_LOSS") this.positionLosses += 1;
    else if (type === "POSITION_BREAKEVEN") this.positionBreakeven += 1;
    else if (type === "REJECTED_TRADE_WOULD_HAVE_WON") this.rejectedWouldHaveWon += 1;
    else if (type === "ACCEPTED_TRADE_LOST") this.acceptedLost += 1;
    else if (type === "UNHEDGED_RISK_EVENT") {
      const exposure = Number((event.payload as { exposureUsd?: unknown }).exposureUsd ?? 0);
      if (exposure > this.worstUnhedgedUsd) {
        this.worstUnhedgedUsd = exposure;
        this.worstUnhedgedEvent = event.payload;
      }
    } else if (type === "AGENT_SIGNAL_ATTRIBUTION") {
      this.agentSignals[String(event.payload.agentId ?? "unknown")] = event.payload;
    } else if (type === "REGIME_PERFORMANCE") {
      this.regimes[String(event.payload.regime ?? "unknown")] = event.payload;
    } else if (type === "HOURLY_PERFORMANCE") {
      this.hours[String(event.payload.hour ?? "unknown")] = event.payload;
    }
    this.noteFailedHedge(event);
    if ((this.eventCount & 127) === 0) this.refreshHighWater();
  }

  finalize(): ExperimentSummary {
    // Attribution is derived from fillReplay for each projection. Keep the
    // accumulator authoritative pre-finalize so finalize/serialize/restore/
    // finalize is idempotent and cannot double count fills.
    const attributedByMarket = structuredClone(this.byMarket);
    const attributedByAsset = structuredClone(this.byAsset);
    this.applyFillAttribution(attributedByMarket, attributedByAsset);
    const extras = this.extras;
    const extrasScope = extras.metricsScope ?? (extras.reportKind === "final" ? "complete" : "rolling");
    const truncated = extras.truncated === true;
    const extrasComplete = extras.complete ?? (extrasScope === "complete" && !truncated);
    const positions = [...this.latestPositions.values()].filter((position) => {
      if (this.settledMarketIds.has(position.marketId) || this.settledPositionIds.has(position.id)) return false;
      return Number(position.upQuantity) > 0 || Number(position.downQuantity) > 0;
    });
    const completedPairs = positions.filter((position) => Number(position.pairedQuantity) > 0 && Number(position.unpairedQuantity) === 0).length
      + this.settlementBothQty;
    const unhedged = positions.filter((position) => Number(position.unpairedQuantity) > 0);
    const ledger = this.lastLedger;
    const grossPnl = Number(ledger?.grossPnl ?? 0);
    const netTradingPnl = Number(ledger?.netTradingPnl ?? 0);
    const aiCost = Number(ledger?.aiCost ?? 0);
    const infraCost = Number(ledger?.infrastructureCost ?? 0);
    const pairCosts = positions.filter((position) => Number(position.pairedQuantity) > 0).map((position) => Number(position.combinedPairCost));
    const netWl = { wins: this.positionWins, losses: this.positionLosses, breakeven: this.positionBreakeven };
    const grossWl = (this.grossWins + this.grossLosses + this.grossBreakeven) === 0
      ? netWl
      : { wins: this.grossWins, losses: this.grossLosses, breakeven: this.grossBreakeven };
    this.refreshHighWater();
    return {
      guardianOutcomes: { ...this.guardianOutcomes }, experimentId: this.experimentId, generatedAt: extras.generatedAt ?? new Date().toISOString(),
      ...(this.nakedDirectionalDiagnostics.size ? { nakedDirectionalDiagnostics: structuredClone([...this.nakedDirectionalDiagnostics.values()]) } : {}),
      status: extras.status ?? String((ledger as { experimentStatus?: unknown } | undefined)?.experimentStatus ?? "unknown"),
      closePolicy: extras.closePolicy ?? "observe_until_settlement",
      reportKind: extras.reportKind ?? "interim",
      unresolvedPositionCount: extras.unresolvedPositionCount ?? unhedged.length,
      settlementCount: this.settlementCount,
      settlementPnlUsd: this.settlementPnlUsd,
      totalOpportunities: this.riskCount,
      acceptedOpportunities: this.accepted, rejectedOpportunities: this.riskCount - this.accepted,
      totalPaperOrders: this.paperOrders,
      totalFills: this.fillCount, makerFills: this.makerFills, takerFills: this.takerFills,
      completedPairs, incompleteUnhedgedPositions: unhedged.length,
      winLossDistribution: netWl,
      grossWinLossDistribution: grossWl,
      grossPnlUsd: grossPnl, feesUsd: Number(ledger?.fees ?? 0), slippageUsd: Number(ledger?.slippage ?? 0),
      netTradingPnlUsd: netTradingPnl, aiCostUsd: aiCost, estimatedInfrastructureCostUsd: infraCost,
      economicPnlUsd: Number(ledger?.netEconomicPnl ?? netTradingPnl - aiCost - infraCost),
      maximumDrawdownUsd: Math.max(0, this.maxDrawdown),
      maximumDirectionalExposureUsd: Math.max(0, this.maxDirectionalExposureUsd),
      averageUnhedgedDurationMs: this.hedgeDurationN ? this.hedgeDurationSum / this.hedgeDurationN : 0,
      worstUnhedgedEvent: this.worstUnhedgedEvent,
      averagePairCost: pairCosts.length ? average(pairCosts) : null,
      averageLockedPairEdge: pairCosts.length ? average(pairCosts.map((cost) => 1 - cost)) : null,
      averageRealizedEdge: this.pairRealizedNetN ? this.pairRealizedNetSum / this.pairRealizedNetN : null,
      averageGrossLockedEdge: this.pairRealizedGrossN ? this.pairRealizedGrossSum / this.pairRealizedGrossN : null,
      averageNetRealizedEdge: this.pairRealizedNetN ? this.pairRealizedNetSum / this.pairRealizedNetN : null,
      makerFillRate: rate(this.makerFills, this.makerOrders), takerFillRate: rate(this.takerFills, this.takerOrders),
      modelInvocationCounts: this.modelCounts, modelCostByAgentUsd: this.modelCosts,
      performanceByAgentSignal: this.agentSignals,
      performanceByMarket: attributedByMarket, performanceByAsset: attributedByAsset,
      eventCountByMarket: this.eventCountByMarket, eventCountByAsset: this.eventCountByAsset,
      pnlByMarket: pick(attributedByMarket, "netTradingPnlUsd"), pnlByAsset: pick(attributedByAsset, "netTradingPnlUsd"),
      fillsByMarket: pick(attributedByMarket, "fills"), fillsByAsset: pick(attributedByAsset, "fills"),
      feesByMarket: pick(attributedByMarket, "feesUsd"), feesByAsset: pick(attributedByAsset, "feesUsd"),
      slippageByMarket: pick(attributedByMarket, "slippageUsd"), slippageByAsset: pick(attributedByAsset, "slippageUsd"),
      opportunitiesByMarket: pick(attributedByMarket, "opportunities"), opportunitiesByAsset: pick(attributedByAsset, "opportunities"),
      vetoesByMarket: pick(attributedByMarket, "vetoes"), vetoesByAsset: pick(attributedByAsset, "vetoes"),
      performanceByMarketRegime: this.regimes,
      performanceByHour: this.hours,
      rejectedTradesThatWouldHaveWon: this.rejectedWouldHaveWon,
      acceptedTradesThatLost: this.positionLosses, failureStatistics: this.failureStatistics,
      hedgeFailureRate: rate(this.failedHedgeCount, this.firstLegCount),
      executableOpportunityRate: rate(this.fillCount, this.riskCount),
      metricsScope: extrasScope,
      complete: extrasComplete && !truncated,
      truncated,
      truncatedReason: truncated ? (extras.truncatedReason ?? "SCAN_CEILING") : null,
      earliestOccurredAt: this.earliestOccurredAt,
      latestOccurredAt: this.latestOccurredAt,
      decisionAccountingEventCount: this.eventCount,
      firstLegCount: this.firstLegCount,
      hedgeCompleteCount: this.hedgeCompleteCount,
      failedHedgeCount: this.failedHedgeCount,
    };
  }

  private applyFillAttribution(byMarket: Record<string, MarketAssetAttribution>, byAsset: Record<string, MarketAssetAttribution>): void {
    for (const [marketId, pnl] of this.fillReplay) {
      const marketRow = bucket(byMarket, marketId);
      marketRow.realizedPnlUsd += pnl.realizedPnlUsd;
      marketRow.feesUsd += pnl.feesUsd;
      marketRow.slippageUsd += pnl.slippageUsd;
      marketRow.fills += pnl.fills;
      const asset = this.assetByMarket.get(marketId);
      if (asset) {
        const assetRow = bucket(byAsset, asset);
        assetRow.realizedPnlUsd += pnl.realizedPnlUsd;
        assetRow.feesUsd += pnl.feesUsd;
        assetRow.slippageUsd += pnl.slippageUsd;
        assetRow.fills += pnl.fills;
      }
    }
    for (const row of Object.values(byMarket)) row.netTradingPnlUsd = d(row.realizedPnlUsd).minus(row.feesUsd).toNumber();
    for (const row of Object.values(byAsset)) row.netTradingPnlUsd = d(row.realizedPnlUsd).minus(row.feesUsd).toNumber();
  }

  private addSettlementAttribution(event: JournalEvent, realized: number): void {
    const marketId = payloadString(event.payload, "marketId") ?? (event.aggregateType === "position" ? event.aggregateId : null);
    if (marketId) {
      const row = bucket(this.byMarket, marketId);
      row.settlementPnlUsd += realized;
      row.realizedPnlUsd += realized;
    }
    const asset = payloadString(event.payload, "asset") ?? (marketId ? this.assetByMarket.get(marketId) ?? null : null);
    if (asset) {
      const row = bucket(this.byAsset, asset);
      row.settlementPnlUsd += realized;
      row.realizedPnlUsd += realized;
    }
  }

  private addRiskAttribution(event: JournalEvent): void {
    const marketId = payloadString(event.payload, "marketId") ?? (event.aggregateType === "risk" ? null : event.aggregateId);
    if (marketId) this.bumpRisk(this.byMarket, marketId, event);
    const asset = payloadString(event.payload, "asset") ?? (payloadString(event.payload, "marketId") ? this.assetByMarket.get(String(event.payload.marketId)) ?? null : null);
    if (asset) this.bumpRisk(this.byAsset, asset, event);
  }

  private bumpRisk(target: Record<string, MarketAssetAttribution>, id: string, event: JournalEvent): void {
    const row = bucket(target, id);
    row.opportunities += 1;
    if ((event.payload as { accepted?: boolean }).accepted === true) row.accepted += 1;
    else row.vetoes += 1;
  }

  private noteFailedHedge(event: JournalEvent): void {
    const marketId = payloadString(event.payload, "marketId")
      ?? (event.aggregateType === "position" || event.aggregateType === "trigger" ? event.aggregateId : null);
    if (isFailedHedgeEvent(event)) {
      const key = marketId ?? event.aggregateId;
      if (!this.failedHedgeOpen.has(key)) {
        this.failedHedgeOpen.add(key);
        this.failedHedgeCount += 1;
      }
      return;
    }
    if (event.type === "HEDGE_COMPLETE" || event.type === "POSITION_SETTLED") {
      if (marketId) this.failedHedgeOpen.delete(marketId);
      this.failedHedgeOpen.delete(event.aggregateId);
    }
  }

  private noteAsset(event: JournalEvent): void {
    const marketId = payloadString(event.payload, "marketId");
    const asset = payloadString(event.payload, "asset");
    if (marketId && asset) this.assetByMarket.set(marketId, asset);
    const markets = event.payload.markets;
    if (Array.isArray(markets)) {
      for (const item of markets) {
        if (!item || typeof item !== "object") continue;
        const id = payloadString(item as Record<string, unknown>, "id") ?? payloadString(item as Record<string, unknown>, "marketId");
        const marketAsset = payloadString(item as Record<string, unknown>, "asset");
        if (id && marketAsset) this.assetByMarket.set(id, marketAsset);
      }
    }
  }

  private bumpCount(event: JournalEvent, key: string, counts: Record<string, number>): void {
    const payload = event.payload as Record<string, unknown>;
    const value = payload[key] ?? (event.aggregateType === "market" || event.aggregateType === "position" ? event.aggregateId : null);
    if (typeof value !== "string" || value.length === 0) return;
    counts[value] = (counts[value] ?? 0) + 1;
  }

  private refreshHighWater(): void {
    let bytes = 2_048;
    bytes += this.fillReplay.size * 192;
    bytes += this.latestPositions.size * 256;
    bytes += Object.keys(this.byMarket).length * 160 + Object.keys(this.byAsset).length * 160;
    bytes += (this.pairRealizedNetN + this.pairRealizedGrossN) * 8;
    this.highWaterBytes = Math.max(this.highWaterBytes, bytes);
  }
}

export function buildExperimentSummary(
  events: readonly JournalEvent[],
  experimentId = "unknown",
  extras: ExperimentSummaryExtras = {},
): ExperimentSummary {
  const acc = new ExperimentSummaryAccumulator(experimentId, extras);
  for (const event of events) acc.fold(event);
  return acc.finalize();
}

function emptyAttribution(): MarketAssetAttribution {
  return { realizedPnlUsd: 0, feesUsd: 0, slippageUsd: 0, settlementPnlUsd: 0, netTradingPnlUsd: 0, fills: 0, opportunities: 0, accepted: 0, vetoes: 0 };
}

function bucket(rows: Record<string, MarketAssetAttribution>, id: string): MarketAssetAttribution {
  const row = rows[id] ?? emptyAttribution();
  rows[id] = row;
  return row;
}

function createFillReplay() {
  return {
    upQty: d(0), downQty: d(0), upCost: d(0), downCost: d(0),
    realized: d(0), fees: d(0), slippage: d(0), fills: 0,
    get realizedPnlUsd() { return this.realized.toNumber(); },
    get feesUsd() { return this.fees.toNumber(); },
    get slippageUsd() { return this.slippage.toNumber(); },
  };
}

function foldFill(state: ReturnType<typeof createFillReplay>, fill: PaperFill): void {
  const qty = d(fill.quantity);
  const price = d(fill.fillPrice);
  state.fees = state.fees.plus(fill.feeUsd);
  state.slippage = state.slippage.plus(fill.slippageUsd);
  state.fills += 1;
  if (fill.outcome === "UP") {
    if (fill.side === "BUY") {
      const next = state.upQty.plus(qty);
      state.upCost = next.gt(0) ? state.upQty.mul(state.upCost).plus(qty.mul(price)).div(next) : d(0);
      state.upQty = next;
    } else {
      state.realized = state.realized.plus(qty.mul(price.minus(state.upCost)));
      state.upQty = state.upQty.minus(qty);
      if (state.upQty.lte(0)) { state.upQty = d(0); state.upCost = d(0); }
    }
  } else if (fill.side === "BUY") {
    const next = state.downQty.plus(qty);
    state.downCost = next.gt(0) ? state.downQty.mul(state.downCost).plus(qty.mul(price)).div(next) : d(0);
    state.downQty = next;
  } else {
    state.realized = state.realized.plus(qty.mul(price.minus(state.downCost)));
    state.downQty = state.downQty.minus(qty);
    if (state.downQty.lte(0)) { state.downQty = d(0); state.downCost = d(0); }
  }
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
function directionalExposureUsd(position: Position): number {
  const quantity = Number(position.unpairedQuantity);
  if (!Number.isFinite(quantity) || quantity <= 0 || !position.unpairedOutcome) return 0;
  // Keep the existing blended-side cost convention here. Residual-lot cost basis
  // is a separate risk/accounting correctness concern and is intentionally out of scope.
  const averageCost = Number(position.unpairedOutcome === "UP" ? position.averageUpCost : position.averageDownCost);
  if (!Number.isFinite(averageCost) || averageCost <= 0) return 0;
  return quantity * averageCost;
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
