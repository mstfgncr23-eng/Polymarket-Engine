import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { Buffer } from "node:buffer";
import { d, renderExperimentSummaryMarkdown, type AstraDecisionAuditDigest, type AstraDecisionAuditOutcome, type AstraDecisionAuditSource, type AstraDecisionAuditSummary, type ExperimentSummary, type JournalEvent, type LedgerState, type MarketAssetAttribution, type PaperFill, type Position } from "@pm/core";

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

const PROVIDER_DISPATCH_TRACKING_LIMIT = 10_000;
export const ASTRA_DECISION_AUDIT_MAX_FAST_DISPATCHES_PER_HOUR = 12;
export const ASTRA_DECISION_AUDIT_MAX_EXPERIMENT_HOURS = 48;
/** One provider digest represents the single FAST/DEEP economic decision. */
export const ASTRA_DECISION_AUDIT_MAX_PROVIDER_DIGESTS = ASTRA_DECISION_AUDIT_MAX_FAST_DISPATCHES_PER_HOUR * ASTRA_DECISION_AUDIT_MAX_EXPERIMENT_HOURS;
export const ASTRA_DECISION_AUDIT_MAX_DIGESTS = ASTRA_DECISION_AUDIT_MAX_PROVIDER_DIGESTS;
/** Derived from the legal envelope at a conservative 768-byte structural budget per digest. */
export const ASTRA_DECISION_AUDIT_MAX_BYTES = ASTRA_DECISION_AUDIT_MAX_PROVIDER_DIGESTS * 768;
const ASTRA_DECISION_REASON_LIMIT = 6;
const ASTRA_DECISION_REASON_MAX_LENGTH = 64;
const ASTRA_AUDIT_REASON_LIMIT = 8;
const PRE_PROVIDER_REASON_KEYS = [
  "GLOBAL_MIN_INTERVAL", "MARKET_COOLDOWN", "ROLLING_HOUR_CAP", "CLIENT_UNAVAILABLE",
  "FEATURE_EXTRACTION_FAILED", "STAGE_ADMISSION_CLOSED", "CONCURRENCY_SATURATED", "ENTRY_CLOSED",
  "FINALIZATION_PENDING", "SOURCE_SEALED", "DISPATCH_ALREADY_AUTHORIZED", "DISPATCH_NOT_STARTED",
  "EVIDENCE_PERSISTENCE_FAILED", "ACCOUNTING_FAILED",
] as const;
const PRE_PROVIDER_OTHER_REASON = "OTHER";
type ProviderDispatchTerminal = "KNOWN" | "UNKNOWN" | "NOT_DISPATCHED";
interface ProviderDispatchLifecycle {
  authorized: boolean;
  started: boolean;
  terminal: ProviderDispatchTerminal | null;
  invocationObserved: boolean;
  knownUsageFailureObserved: boolean;
  unknownFailureObserved: boolean;
  notDispatchedFailureObserved: boolean;
  usageObserved: boolean;
}

export interface DurableAccountingTransition {
  sequence: number;
  operationKey: string;
  type: string;
  aggregateId: string;
  occurredAt: string;
  payload: Record<string, unknown>;
}

interface AstraDecisionAuditCounters {
  decisionCount: number;
  providerDecisionCount: number;
  buyUpCount: number;
  buyDownCount: number;
  noTradeCount: number;
  directionUpCount: number;
  directionDownCount: number;
  directionFlatCount: number;
  fastOnlyCount: number;
  deepDecisionCount: number;
  failClosedCount: number;
  deterministicHedgeBypassCount: number;
  gateRejectedCount: number;
  preProviderRejectedCount: number;
  overflow: boolean;
  overflowCount: number;
  reasonEvidenceOverflowCount: number;
  preProviderRejectReasons: Record<string, number>;
  preProviderRejectReasonOverflow: boolean;
}

interface AnalyticsCoverage {
  version: 1;
  economicCostComplete: boolean;
  decisionAuditComplete: boolean;
  decisionAuditOverflow: boolean;
}

export interface ExperimentSummaryAccumulatorSnapshot {
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
  /** Durable INFRASTRUCTURE_COST_ACCRUED projection, independent of PNL_SNAPSHOT. */
  operatingAiCostUsd?: number;
  operatingInfrastructureCostUsd?: number;
  operatingCostTransitionCount?: number;
  operatingCostSourceCursor?: number;
  operatingCostCoverage?: "COMPLETE" | "UNKNOWN";
  operatingCostMalformed?: boolean;
  analyticsCoverage?: AnalyticsCoverage;
  astraDecisionAudit?: {
    counters: AstraDecisionAuditCounters;
    digests: AstraDecisionAuditDigest[];
  };
  maxDrawdown: number;
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
  providerDispatchStartedCount: number;
  providerDispatchAuthorizedCount?: number;
  providerDispatchLifecycle?: Array<[string, ProviderDispatchLifecycle]>;
  providerDispatchTrackingOverflow?: boolean;
  providerDispatchTrackingIncomplete?: boolean;
  providerKnownTerminalCount?: number;
  successfulModelInvocationCount: number;
  knownUsageFailureCount: number;
  dispatchedUnknownFailureCount: number;
  notDispatchedFailureCount: number;
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
  private operatingAiCostUsd = 0;
  private operatingInfrastructureCostUsd = 0;
  private operatingCostTransitionCount = 0;
  private operatingCostSourceCursor = 0;
  private operatingCostCoverage: "COMPLETE" | "UNKNOWN" = "UNKNOWN";
  private operatingCostMalformed = false;
  private decisionAuditCoverage: "COMPLETE" | "UNKNOWN" = "COMPLETE";
  private astraDecisionAuditCounters: AstraDecisionAuditCounters = emptyAstraDecisionAuditCounters();
  private readonly astraDecisionDigests = new Map<string, AstraDecisionAuditDigest>();
  private readonly astraDecisionByTrace = new Map<string, string>();
  private astraDecisionAuditBytes = 0;
  private maxDrawdown = 0;
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
  private readonly providerDispatches = new Map<string, ProviderDispatchLifecycle>();
  private providerDispatchAuthorizedCount = 0;
  private providerDispatchStartedCount = 0;
  private providerDispatchTrackingOverflow = false;
  private providerDispatchTrackingIncomplete = false;
  private providerKnownTerminalCount = 0;
  private successfulModelInvocationCount = 0;
  private knownUsageFailureCount = 0;
  private dispatchedUnknownFailureCount = 0;
  private notDispatchedFailureCount = 0;
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

  /** Fold one authoritative durable operating-cost transition. */
  foldDurableTransition(transition: DurableAccountingTransition): boolean {
    if (transition.type !== "INFRASTRUCTURE_COST_ACCRUED") return false;
    if (!Number.isInteger(transition.sequence) || transition.sequence <= 0) {
      this.operatingCostCoverage = "UNKNOWN";
      return false;
    }
    if (transition.sequence <= this.operatingCostSourceCursor) return false;
    const payload = transition.payload;
    const aiCost = finiteNonNegativeNumber(payload.aiCost);
    const infrastructureCost = finiteNonNegativeNumber(payload.infrastructureCost);
    if (aiCost == null || infrastructureCost == null) {
      this.operatingCostMalformed = true;
      this.operatingCostCoverage = "UNKNOWN";
      this.operatingCostSourceCursor = transition.sequence;
      return false;
    }
    this.operatingAiCostUsd += aiCost;
    this.operatingInfrastructureCostUsd += infrastructureCost;
    this.operatingCostTransitionCount += 1;
    this.operatingCostSourceCursor = transition.sequence;
    this.operatingCostCoverage = "COMPLETE";
    return true;
  }

  setOperatingCostSourceCursor(cursor: number, complete: boolean): void {
    if (Number.isInteger(cursor) && cursor >= this.operatingCostSourceCursor) this.operatingCostSourceCursor = cursor;
    if (!complete) this.operatingCostCoverage = "UNKNOWN";
    else if (!this.operatingCostMalformed) this.operatingCostCoverage = "COMPLETE";
  }

  setDecisionAuditCoverage(complete: boolean): void {
    if (!complete) this.decisionAuditCoverage = "UNKNOWN";
  }

  /** JSON-safe bounded rollup state. No raw JournalEvent objects are retained. */
  serialize(): ExperimentSummaryAccumulatorSnapshot {
    return {
      schemaVersion: 1, experimentId: this.experimentId, extras: structuredClone(this.extras), eventCount: this.eventCount,
      earliestOccurredAt: this.earliestOccurredAt, latestOccurredAt: this.latestOccurredAt,
      fillReplay: [...this.fillReplay.entries()].map(([marketId, s]) => ({ marketId, upQty: s.upQty.toString(), downQty: s.downQty.toString(), upCost: s.upCost.toString(), downCost: s.downCost.toString(), realized: s.realized.toString(), fees: s.fees.toString(), slippage: s.slippage.toString(), fills: s.fills })),
      latestPositions: [...this.latestPositions.entries()].map(([id, position]) => [id, structuredClone(position)]),
      settledMarketIds: [...this.settledMarketIds], settledPositionIds: [...this.settledPositionIds], settlementCount: this.settlementCount,
      settlementPnlUsd: this.settlementPnlUsd, settlementBothQty: this.settlementBothQty, lastLedger: this.lastLedger ? structuredClone(this.lastLedger) : undefined,
      operatingAiCostUsd: this.operatingAiCostUsd, operatingInfrastructureCostUsd: this.operatingInfrastructureCostUsd,
      operatingCostTransitionCount: this.operatingCostTransitionCount, operatingCostSourceCursor: this.operatingCostSourceCursor,
      operatingCostCoverage: this.operatingCostCoverage, operatingCostMalformed: this.operatingCostMalformed,
      analyticsCoverage: {
        version: 1,
        economicCostComplete: this.operatingCostCoverage === "COMPLETE",
        decisionAuditComplete: this.decisionAuditCoverage === "COMPLETE",
        decisionAuditOverflow: this.astraDecisionAuditCounters.overflow,
      },
      astraDecisionAudit: {
        counters: structuredClone(this.astraDecisionAuditCounters),
        digests: [...this.astraDecisionDigests.values()].map((digest) => structuredClone(digest)),
      },
      maxDrawdown: this.maxDrawdown, riskCount: this.riskCount, accepted: this.accepted, paperOrders: this.paperOrders,
      makerOrders: this.makerOrders, takerOrders: this.takerOrders, fillCount: this.fillCount, makerFills: this.makerFills, takerFills: this.takerFills,
      firstLegCount: this.firstLegCount, hedgeCompleteCount: this.hedgeCompleteCount, hedgeDurationSum: this.hedgeDurationSum, hedgeDurationN: this.hedgeDurationN,
      pairRealizedNetSum: this.pairRealizedNetSum, pairRealizedNetN: this.pairRealizedNetN, pairRealizedGrossSum: this.pairRealizedGrossSum, pairRealizedGrossN: this.pairRealizedGrossN,
      modelCounts: structuredClone(this.modelCounts), modelCosts: structuredClone(this.modelCosts),
      providerDispatchAuthorizedCount: this.providerDispatchAuthorizedCount,
      providerDispatchLifecycle: [...this.providerDispatches.entries()].map(([id, state]) => [id, structuredClone(state)]),
      providerDispatchTrackingOverflow: this.providerDispatchTrackingOverflow,
      providerDispatchTrackingIncomplete: this.providerDispatchTrackingIncomplete,
      providerKnownTerminalCount: this.providerKnownTerminalCount,
      providerDispatchStartedCount: this.providerDispatchStartedCount, successfulModelInvocationCount: this.successfulModelInvocationCount,
      knownUsageFailureCount: this.knownUsageFailureCount, dispatchedUnknownFailureCount: this.dispatchedUnknownFailureCount,
      notDispatchedFailureCount: this.notDispatchedFailureCount, failureStatistics: structuredClone(this.failureStatistics),
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
    const assign = (key: keyof ExperimentSummaryAccumulatorSnapshot, fallback: unknown = undefined) => (snapshot[key] ?? fallback) as never;
    this.eventCount = assign("eventCount", 0); this.earliestOccurredAt = assign("earliestOccurredAt", null); this.latestOccurredAt = assign("latestOccurredAt", null);
    this.fillReplay.clear(); for (const item of snapshot.fillReplay ?? []) { const s = createFillReplay(); s.upQty = d(item.upQty); s.downQty = d(item.downQty); s.upCost = d(item.upCost); s.downCost = d(item.downCost); s.realized = d(item.realized); s.fees = d(item.fees); s.slippage = d(item.slippage); s.fills = item.fills; this.fillReplay.set(item.marketId, s); }
    this.latestPositions.clear(); for (const [id, position] of snapshot.latestPositions ?? []) this.latestPositions.set(id, structuredClone(position));
    this.settledMarketIds.clear(); for (const id of snapshot.settledMarketIds ?? []) this.settledMarketIds.add(id); this.settledPositionIds.clear(); for (const id of snapshot.settledPositionIds ?? []) this.settledPositionIds.add(id);
    this.settlementCount = assign("settlementCount", 0); this.settlementPnlUsd = assign("settlementPnlUsd", 0); this.settlementBothQty = assign("settlementBothQty", 0); this.lastLedger = snapshot.lastLedger ? structuredClone(snapshot.lastLedger) : undefined;
    this.operatingAiCostUsd = finiteNonNegativeNumber(snapshot.operatingAiCostUsd) ?? 0;
    this.operatingInfrastructureCostUsd = finiteNonNegativeNumber(snapshot.operatingInfrastructureCostUsd) ?? 0;
    const transitionCount = snapshot.operatingCostTransitionCount;
    const sourceCursor = snapshot.operatingCostSourceCursor;
    this.operatingCostTransitionCount = typeof transitionCount === "number" && Number.isInteger(transitionCount) && transitionCount >= 0 ? transitionCount : 0;
    this.operatingCostSourceCursor = typeof sourceCursor === "number" && Number.isInteger(sourceCursor) && sourceCursor >= 0 ? sourceCursor : 0;
    this.operatingCostCoverage = snapshot.operatingCostCoverage === "COMPLETE" ? "COMPLETE" : "UNKNOWN";
    this.operatingCostMalformed = snapshot.operatingCostMalformed === true;
    if (this.operatingCostMalformed) this.operatingCostCoverage = "UNKNOWN";
    this.decisionAuditCoverage = snapshot.analyticsCoverage?.version === 1 && snapshot.analyticsCoverage.decisionAuditComplete === true ? "COMPLETE" : "UNKNOWN";
    this.astraDecisionAuditCounters = restoreAstraDecisionAuditCounters(snapshot.astraDecisionAudit?.counters);
    this.astraDecisionDigests.clear();
    this.astraDecisionByTrace.clear();
    this.astraDecisionAuditBytes = 0;
    const restoredDigests = Array.isArray(snapshot.astraDecisionAudit?.digests) ? snapshot.astraDecisionAudit!.digests : [];
    if (restoredDigests.length > ASTRA_DECISION_AUDIT_MAX_DIGESTS) this.astraDecisionAuditCounters.overflow = true;
    for (const digest of restoredDigests.slice(0, ASTRA_DECISION_AUDIT_MAX_DIGESTS)) {
      if (!isValidAstraDecisionDigest(digest)) {
        this.decisionAuditCoverage = "UNKNOWN";
        continue;
      }
      if (!this.retainAstraDecisionDigest(structuredClone(digest))) this.decisionAuditCoverage = "UNKNOWN";
    }
    for (const key of ["maxDrawdown","riskCount","accepted","paperOrders","makerOrders","takerOrders","fillCount","makerFills","takerFills","firstLegCount","hedgeCompleteCount","hedgeDurationSum","hedgeDurationN","pairRealizedNetSum","pairRealizedNetN","pairRealizedGrossSum","pairRealizedGrossN","failedHedgeCount","positionWins","positionLosses","positionBreakeven","grossWins","grossLosses","grossBreakeven","rejectedWouldHaveWon","acceptedLost","worstUnhedgedUsd"] as const) (this as unknown as Record<string, unknown>)[key] = snapshot[key];
    this.providerDispatchStartedCount = snapshot.providerDispatchStartedCount ?? 0;
    this.providerDispatchAuthorizedCount = snapshot.providerDispatchAuthorizedCount ?? 0;
    this.providerDispatchTrackingOverflow = snapshot.providerDispatchTrackingOverflow === true;
    this.providerDispatchTrackingIncomplete = snapshot.providerDispatchTrackingIncomplete === true
      || typeof snapshot.providerDispatchAuthorizedCount !== "number";
    this.providerKnownTerminalCount = snapshot.providerKnownTerminalCount
      ?? Math.max(0, (snapshot.successfulModelInvocationCount ?? 0) + (snapshot.knownUsageFailureCount ?? 0));
    this.providerDispatches.clear();
    if (Array.isArray(snapshot.providerDispatchLifecycle)) {
      if (snapshot.providerDispatchLifecycle.length > PROVIDER_DISPATCH_TRACKING_LIMIT) this.providerDispatchTrackingOverflow = true;
      for (const entry of snapshot.providerDispatchLifecycle.slice(0, PROVIDER_DISPATCH_TRACKING_LIMIT)) {
        if (!Array.isArray(entry) || typeof entry[0] !== "string" || !entry[0]) {
          this.providerDispatchTrackingIncomplete = true;
          continue;
        }
        const state = entry[1];
        if (!state || typeof state !== "object"
          || typeof state.authorized !== "boolean" || typeof state.started !== "boolean"
          || (state.terminal !== null && state.terminal !== "KNOWN" && state.terminal !== "UNKNOWN" && state.terminal !== "NOT_DISPATCHED")
          || typeof state.invocationObserved !== "boolean" || typeof state.knownUsageFailureObserved !== "boolean"
          || typeof state.unknownFailureObserved !== "boolean" || typeof state.notDispatchedFailureObserved !== "boolean"
          || typeof state.usageObserved !== "boolean") {
          this.providerDispatchTrackingIncomplete = true;
          continue;
        }
        this.providerDispatches.set(entry[0], structuredClone(state));
      }
    }
    this.successfulModelInvocationCount = snapshot.successfulModelInvocationCount ?? 0;
    this.knownUsageFailureCount = snapshot.knownUsageFailureCount ?? 0;
    this.dispatchedUnknownFailureCount = snapshot.dispatchedUnknownFailureCount ?? 0;
    this.notDispatchedFailureCount = snapshot.notDispatchedFailureCount ?? 0;
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
    if (type === "PAPER_FILL") {
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
      this.latestPositions.set(event.aggregateId, event.payload as unknown as Position);
    } else if (type === "PNL_SNAPSHOT") {
      this.lastLedger = event.payload as unknown as LedgerState;
      this.maxDrawdown = Math.max(this.maxDrawdown, Number((event.payload as { drawdown?: unknown }).drawdown ?? 0));
    } else if (type === "RISK_EVALUATION") {
      this.riskCount += 1;
      if ((event.payload as { accepted?: boolean }).accepted === true) this.accepted += 1;
      this.addRiskAttribution(event);
    } else if (type === "PAPER_ORDER_CREATED") {
      this.paperOrders += 1;
      const payload = event.payload as { orderType?: unknown; type?: unknown };
      if (payload.orderType === "MAKER" || payload.type === "MAKER") this.makerOrders += 1;
      if (payload.orderType === "TAKER" || payload.type === "TAKER") this.takerOrders += 1;
    } else if (type === "FIRST_LEG_FILLED") this.firstLegCount += 1;
    else if (type === "HEDGE_COMPLETE") {
      this.hedgeCompleteCount += 1;
      const duration = Number((event.payload as { unhedgedDurationMs?: unknown }).unhedgedDurationMs);
      if (Number.isFinite(duration)) {
        this.hedgeDurationSum += duration;
        this.hedgeDurationN += 1;
      }
    } else if (type === "ASTRA_PROVIDER_DISPATCH_AUTHORIZED" && (event.payload as { authority?: unknown }).authority === "ASTRA_BRAIN_V1") {
      this.noteProviderDispatchAuthorized(event);
    } else if (type === "ASTRA_PROVIDER_DISPATCH_STARTED" && (event.payload as { authority?: unknown }).authority === "ASTRA_BRAIN_V1") {
      this.noteProviderDispatchStarted(event);
    } else if (type === "ASTRA_MODEL_INVOCATION" && (event.payload as { authority?: unknown }).authority === "ASTRA_BRAIN_V1") {
      const lifecycle = this.providerDispatchLifecycleFor(event);
      if (!lifecycle || !lifecycle.invocationObserved) {
        if (lifecycle?.terminal === "NOT_DISPATCHED") this.providerDispatchTrackingIncomplete = true;
        if (lifecycle) lifecycle.invocationObserved = true;
        this.successfulModelInvocationCount += 1;
        this.noteKnownProviderTerminal(lifecycle);
        const stage = String((event.payload as { stage?: unknown }).stage ?? "DECISION");
        const usage = (event.payload as { usage?: { costUsd?: number } }).usage;
        this.modelCounts.ASTRA_BRAIN_V1 = (this.modelCounts.ASTRA_BRAIN_V1 ?? 0) + 1;
        this.modelCosts.ASTRA_BRAIN_V1 = (this.modelCosts.ASTRA_BRAIN_V1 ?? 0) + (usage?.costUsd ?? 0);
        const stageKey = `ASTRA_${stage}`;
        this.modelCounts[stageKey] = (this.modelCounts[stageKey] ?? 0) + 1;
        this.modelCosts[stageKey] = (this.modelCosts[stageKey] ?? 0) + (usage?.costUsd ?? 0);
        if (lifecycle) lifecycle.usageObserved = usage != null && typeof usage.costUsd === "number";
      }
    } else if (type === "ASTRA_DECISION" && (event.payload as { authority?: unknown }).authority === "ASTRA_BRAIN_V1") {
      this.noteAstraDecision(event);
      // Invocation events intentionally contain only bounded provider audit
      // data. The canonical decision is the single per-trigger source for
      // regime attribution; never manufacture an "unknown" regime for a
      // Fast/Deep or BATCH invocation.
      const payload = event.payload as { featurePacket?: { regime?: unknown }; stages?: unknown };
      const regime = payload.featurePacket?.regime;
      if (typeof regime === "string" && regime.length > 0 && regime !== "unknown") {
        const stages = Array.isArray(payload.stages) ? payload.stages.filter((stage): stage is string => typeof stage === "string") : [];
        this.regimes[regime] = { source: "ASTRA_DECISION", stage: stages.at(-1) ?? "DECISION" };
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
    } else if (type === "ASTRA_FAILURE" || type === "ASTRA_BUDGET_BLOCK") {
      const failurePayload = event.payload as { dispatchState?: unknown; knownUsage?: unknown; admissionReason?: unknown };
      const dispatchState = failurePayload.dispatchState;
      const lifecycle = type === "ASTRA_BUDGET_BLOCK" || (dispatchState === "NOT_DISPATCHED" && !providerDispatchIdentity(event))
        ? null
        : this.providerDispatchLifecycleFor(event);
      const knownUsage = dispatchState === "RESPONSE_RECEIVED" && failurePayload.knownUsage === true;
      const unknownDispatch = dispatchState === "DISPATCHED_UNKNOWN"
        || (dispatchState === "RESPONSE_RECEIVED" && !knownUsage);
      const provenNotDispatched = dispatchState === "NOT_DISPATCHED";
      let recordFailureStatistic = true;
      if (knownUsage) {
        const duplicate = lifecycle?.knownUsageFailureObserved === true;
        if (!duplicate) {
          if (lifecycle?.terminal === "NOT_DISPATCHED" || lifecycle?.terminal === "UNKNOWN") this.providerDispatchTrackingIncomplete = true;
          if (lifecycle) lifecycle.knownUsageFailureObserved = true;
          this.knownUsageFailureCount += 1;
          this.noteKnownProviderTerminal(lifecycle);
          this.noteProviderUsage(event, lifecycle);
        }
        recordFailureStatistic = !duplicate;
      } else if (unknownDispatch) {
        const duplicate = lifecycle?.unknownFailureObserved === true;
        if (!duplicate) {
          if (lifecycle?.terminal === "KNOWN" || lifecycle?.terminal === "NOT_DISPATCHED") this.providerDispatchTrackingIncomplete = true;
          if (lifecycle) lifecycle.unknownFailureObserved = true;
          this.dispatchedUnknownFailureCount += 1;
        }
        if (lifecycle) lifecycle.terminal = "UNKNOWN";
        recordFailureStatistic = !duplicate;
      } else {
        const duplicate = lifecycle?.notDispatchedFailureObserved === true;
        const duplicateAuthorizationRetry = provenNotDispatched
          && failurePayload.admissionReason === "DISPATCH_ALREADY_AUTHORIZED"
          && lifecycle?.authorized === true;
        if (!duplicate) {
          if (lifecycle?.terminal === "KNOWN" || lifecycle?.terminal === "UNKNOWN") this.providerDispatchTrackingIncomplete = true;
          if (lifecycle) lifecycle.notDispatchedFailureObserved = true;
          this.notDispatchedFailureCount += 1;
        }
        // Only an explicit NOT_DISPATCHED state closes an AUTHORIZED-only
        // lifecycle. Legacy failures without dispatchState remain unresolved.
        // A retry rejected by the one-shot authorization guard is not proof
        // that the original AUTHORIZED request failed to start. Keep that
        // original lifecycle unresolved after acknowledgement loss or crash.
        if (provenNotDispatched && lifecycle && lifecycle.terminal === null && !duplicateAuthorizationRetry) lifecycle.terminal = "NOT_DISPATCHED";
        recordFailureStatistic = !duplicate;
      }
      const code = String((event.payload as { reason?: unknown }).reason ?? type);
      if (recordFailureStatistic) this.failureStatistics[code] = (this.failureStatistics[code] ?? 0) + 1;
      if (dispatchState === "NOT_DISPATCHED" || type === "ASTRA_BUDGET_BLOCK") this.noteAstraPreProviderReject(event);
    } else if (type === "AGENT_FAILURE") {
      const code = String((event.payload as { code?: unknown }).code ?? "UNKNOWN");
      this.failureStatistics[code] = (this.failureStatistics[code] ?? 0) + 1;
    } else if (type === "MCP_INTERACTION") {
      const status = String((event.payload as { status?: unknown }).status ?? "MCP");
      if (status !== "HANDLED") this.failureStatistics[`MCP_${status}`] = (this.failureStatistics[`MCP_${status}`] ?? 0) + 1;
    } else if (type === "PAPER_EXECUTION_ERROR") {
      this.failureStatistics.PAPER_EXECUTION_ERROR = (this.failureStatistics.PAPER_EXECUTION_ERROR ?? 0) + 1;
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
    this.noteAstraFunnel(event);
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
    const ledgerAiCost = finiteNonNegativeNumber(ledger?.aiCost);
    const ledgerInfrastructureCost = finiteNonNegativeNumber(ledger?.infrastructureCost);
    const aiCost = this.operatingCostCoverage === "COMPLETE" ? this.operatingAiCostUsd : (ledgerAiCost ?? 0);
    const infraCost = this.operatingCostCoverage === "COMPLETE" ? this.operatingInfrastructureCostUsd : (ledgerInfrastructureCost ?? 0);
    const knownProviderOutcomes = this.providerKnownTerminalCount;
    const accountingFailure = (this.failureStatistics.ACCOUNTING_FAILED ?? 0) > 0;
    const unresolvedAuthorizationCount = this.providerDispatchUnresolvedAuthorizationCount();
    const providerCostCompleteness = accountingFailure || this.dispatchedUnknownFailureCount > 0 || unresolvedAuthorizationCount > 0
      || this.providerDispatchTrackingOverflow || this.providerDispatchTrackingIncomplete
      || knownProviderOutcomes < this.providerDispatchStartedCount
      ? (knownProviderOutcomes > 0 ? "PARTIAL" : "NONE")
      : "COMPLETE";
    // A PNL snapshot is only a safe fallback when provider accounting itself
    // is complete. If a known billable response lost its durable cost append,
    // the snapshot may predate that charge and must not make the report claim
    // complete economics.
    const economicCostCompleteness = providerCostCompleteness === "COMPLETE"
      && this.operatingCostCoverage === "COMPLETE" ? "COMPLETE" : "UNKNOWN";
    const pairCosts = positions.filter((position) => Number(position.pairedQuantity) > 0).map((position) => Number(position.combinedPairCost));
    const netWl = { wins: this.positionWins, losses: this.positionLosses, breakeven: this.positionBreakeven };
    const grossWl = (this.grossWins + this.grossLosses + this.grossBreakeven) === 0
      ? netWl
      : { wins: this.grossWins, losses: this.grossLosses, breakeven: this.grossBreakeven };
    this.refreshHighWater();
    return {
      experimentId: this.experimentId, generatedAt: extras.generatedAt ?? new Date().toISOString(),
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
      providerDispatchAuthorizedCount: this.providerDispatchAuthorizedCount,
      providerDispatchStartedCount: this.providerDispatchStartedCount,
      providerDispatchUnresolvedAuthorizationCount: unresolvedAuthorizationCount,
      providerDispatchTrackingOverflow: this.providerDispatchTrackingOverflow,
      providerDispatchTrackingIncomplete: this.providerDispatchTrackingIncomplete,
      successfulModelInvocationCount: this.successfulModelInvocationCount,
      knownUsageFailureCount: this.knownUsageFailureCount,
      dispatchedUnknownFailureCount: this.dispatchedUnknownFailureCount,
      notDispatchedFailureCount: this.notDispatchedFailureCount,
      providerCostCompleteness,
      economicCostCompleteness,
      economicPnlUsd: this.operatingCostCoverage === "COMPLETE" || ledger?.netEconomicPnl == null || providerCostCompleteness !== "COMPLETE"
        ? netTradingPnl - aiCost - infraCost
        : Number(ledger.netEconomicPnl),
      astraDecisionAudit: this.astraDecisionAuditSummary(),
      maximumDrawdownUsd: Math.max(0, this.maxDrawdown),
      maximumDirectionalExposureUsd: Math.max(0, ...positions.map((position) => Number(position.unpairedQuantity) * Number(position.unpairedOutcome === "UP" ? position.averageUpCost : position.averageDownCost))),
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
      acceptedTradesThatLost: this.acceptedLost, failureStatistics: this.failureStatistics,
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

  private noteAstraDecision(event: JournalEvent): void {
    if (this.astraDecisionDigests.has(event.id)) return;
    const payload = event.payload as Record<string, unknown>;
    const nested = isRecord(payload.decision) ? payload.decision : null;
    const action = asAstraAction(nested?.action ?? payload.action);
    const direction = asAstraDirection(nested?.direction ?? payload.direction);
    const stages = boundedStringArray(payload.stages, 2, 16);
    const source: AstraDecisionAuditSource = payload.bypass === "DETERMINISTIC_HEDGE"
      ? "DETERMINISTIC_HEDGE"
      : payload.decisionSource === "FAIL_CLOSED"
        ? "FAIL_CLOSED"
        : typeof payload.gateReason === "string"
          ? "GATE_REJECT"
          : "PROVIDER";
    const outcome: AstraDecisionAuditOutcome = source === "DETERMINISTIC_HEDGE"
      ? "DETERMINISTIC_HEDGE_BYPASS"
      : source === "FAIL_CLOSED"
        ? "SYNTHETIC_FAIL_CLOSED"
        : source === "GATE_REJECT"
          ? "GATE_REJECTED"
          : action === "NO_TRADE" ? "MODEL_NO_TRADE" : "DIRECTIONAL_DECISION";
    const reasonEvidence = collectAstraReasons(nested);
    const auditReasonEvidence = collectAstraAuditReasons(payload);
    const digest: AstraDecisionAuditDigest = {
      id: event.id,
      traceId: event.traceId,
      triggerId: payloadString(payload, "triggerId") ?? event.aggregateId,
      marketId: payloadString(payload, "marketId"),
      asset: payloadString(payload, "asset"),
      source,
      action,
      direction,
      stages,
      reasonCodes: reasonEvidence.values,
      auditReasonCodes: auditReasonEvidence.values,
      reasonEvidenceOverflowCount: reasonEvidence.overflowCount + auditReasonEvidence.overflowCount,
      outcome,
      riskOutcome: "NOT_EVALUATED",
      orderCreated: false,
      fillCount: 0,
    };
    // Gate, fail-closed, and deterministic-hedge outcomes are counted exactly
    // but do not consume provider forensic slots. This prevents cheap
    // pre-provider pressure from crowding out the legal 48h FAST envelope.
    const retained = source === "PROVIDER" ? this.retainAstraDecisionDigest(digest) : true;
    if (source === "GATE_REJECT") this.recordPreProviderRejectReason(payloadString(payload, "gateReason"));
    if (!retained) this.decisionAuditCoverage = "UNKNOWN";
    if (digest.reasonEvidenceOverflowCount > 0) this.decisionAuditCoverage = "UNKNOWN";
    this.bumpAstraDecisionCounters(digest);
  }

  private noteAstraPreProviderReject(event: JournalEvent): void {
    const payload = event.payload as Record<string, unknown>;
    if (payload.authority !== "ASTRA_BRAIN_V1") return;
    this.astraDecisionAuditCounters.preProviderRejectedCount += 1;
    this.recordPreProviderRejectReason(payloadString(payload, "reason")
      ?? payloadString(payload, "gateReason")
      ?? payloadString(payload, "failureReason"));
  }

  private recordPreProviderRejectReason(reason: string | null): void {
    const key = reason && (PRE_PROVIDER_REASON_KEYS as readonly string[]).includes(reason) ? reason : PRE_PROVIDER_OTHER_REASON;
    const reasons = this.astraDecisionAuditCounters.preProviderRejectReasons;
    reasons[key] = (reasons[key] ?? 0) + 1;
  }

  private retainAstraDecisionDigest(digest: AstraDecisionAuditDigest): boolean {
    if (this.astraDecisionDigests.has(digest.id)) return true;
    if (this.astraDecisionDigests.size >= ASTRA_DECISION_AUDIT_MAX_DIGESTS) {
      this.astraDecisionAuditCounters.overflow = true;
      this.astraDecisionAuditCounters.overflowCount += 1;
      return false;
    }
    const digestBytes = astraDecisionDigestBytes(digest);
    if (this.astraDecisionAuditBytes + digestBytes > ASTRA_DECISION_AUDIT_MAX_BYTES) {
      this.astraDecisionAuditCounters.overflow = true;
      this.astraDecisionAuditCounters.overflowCount += 1;
      return false;
    }
    this.astraDecisionDigests.set(digest.id, digest);
    this.astraDecisionAuditBytes += digestBytes;
    if (this.astraDecisionByTrace.size < ASTRA_DECISION_AUDIT_MAX_DIGESTS) this.astraDecisionByTrace.set(digest.traceId, digest.id);
    return true;
  }

  private bumpAstraDecisionCounters(digest: AstraDecisionAuditDigest): void {
    const counters = this.astraDecisionAuditCounters;
    if (digest.source !== "PRE_PROVIDER_REJECT") counters.decisionCount += 1;
    if (digest.source === "PROVIDER") counters.providerDecisionCount += 1;
    if (digest.source === "FAIL_CLOSED") counters.failClosedCount += 1;
    if (digest.source === "DETERMINISTIC_HEDGE") counters.deterministicHedgeBypassCount += 1;
    if (digest.source === "GATE_REJECT") counters.gateRejectedCount += 1;
    if (digest.source === "PRE_PROVIDER_REJECT") counters.preProviderRejectedCount += 1;
    if (digest.source === "PROVIDER") {
      if (digest.action === "BUY_UP") counters.buyUpCount += 1;
      if (digest.action === "BUY_DOWN") counters.buyDownCount += 1;
      if (digest.action === "NO_TRADE") counters.noTradeCount += 1;
      if (digest.direction === "UP") counters.directionUpCount += 1;
      if (digest.direction === "DOWN") counters.directionDownCount += 1;
      if (digest.direction === "FLAT") counters.directionFlatCount += 1;
      if (digest.stages.length === 1 && digest.stages[0] === "FAST") counters.fastOnlyCount += 1;
      if (digest.stages.includes("DEEP")) counters.deepDecisionCount += 1;
    }
    counters.reasonEvidenceOverflowCount += digest.reasonEvidenceOverflowCount;
  }

  private noteAstraFunnel(event: JournalEvent): void {
    if (event.type !== "RISK_EVALUATION" && event.type !== "BOOK_REVALIDATION_REJECTED" && event.type !== "PAPER_ORDER_CREATED" && event.type !== "PAPER_FILL" && event.type !== "PAPER_EXECUTION_ERROR" && event.type !== "PAPER_ORDER_CANCELLED" && event.type !== "ASTRA_COUNTERFACTUAL") return;
    const digestId = this.astraDecisionByTrace.get(event.traceId);
    const digest = digestId ? this.astraDecisionDigests.get(digestId) : undefined;
    if (!digest) return;
    if (event.type === "ASTRA_COUNTERFACTUAL") {
      const executionOutcome = (event.payload as { executionOutcome?: unknown }).executionOutcome;
      if (executionOutcome === "NO_EXECUTABLE_CANDIDATE" || executionOutcome === "EXECUTION_GATE_REJECTED" || executionOutcome === "ORDER_CREATED" || executionOutcome === "EXECUTION_ERROR" || executionOutcome === "CANCELLED") digest.outcome = executionOutcome;
      this.appendReasons(digest, boundedStringArray((event.payload as { reason?: unknown }).reason, 1, ASTRA_DECISION_REASON_MAX_LENGTH));
    } else if (event.type === "RISK_EVALUATION") {
      const accepted = (event.payload as { accepted?: unknown }).accepted === true;
      digest.riskOutcome = accepted ? "ACCEPT" : "VETO";
      digest.outcome = accepted ? "RISK_ACCEPTED" : "RISK_VETO";
      if (!accepted) {
        const reasons = boundedStringArray((event.payload as { vetoes?: unknown }).vetoes, ASTRA_DECISION_REASON_LIMIT, ASTRA_DECISION_REASON_MAX_LENGTH);
        this.appendReasons(digest, reasons);
      }
    } else if (event.type === "BOOK_REVALIDATION_REJECTED") {
      digest.outcome = "BOOK_REVALIDATION_REJECT";
      this.appendReasons(digest, boundedStringArray((event.payload as { reason?: unknown }).reason, 1, ASTRA_DECISION_REASON_MAX_LENGTH));
    } else if (event.type === "PAPER_ORDER_CREATED") {
      digest.orderCreated = true;
      digest.outcome = "ORDER_CREATED";
    } else if (event.type === "PAPER_FILL") {
      digest.fillCount += 1;
      digest.outcome = "FILLED";
    } else if (event.type === "PAPER_EXECUTION_ERROR") {
      if (digest.fillCount === 0) digest.outcome = "EXECUTION_ERROR";
      const payload = event.payload as Record<string, unknown>;
      this.appendReasons(digest, boundedStringArray(payload.reason ?? payload.message, 1, ASTRA_DECISION_REASON_MAX_LENGTH));
    } else if (event.type === "PAPER_ORDER_CANCELLED") {
      if (digest.fillCount === 0) digest.outcome = "CANCELLED";
      this.appendReasons(digest, boundedStringArray((event.payload as Record<string, unknown>).reason, 1, ASTRA_DECISION_REASON_MAX_LENGTH));
    }
  }

  private appendReasons(digest: AstraDecisionAuditDigest, reasons: string[]): void {
    for (const reason of reasons) {
      if (digest.auditReasonCodes.includes(reason)) continue;
      if (digest.auditReasonCodes.length >= ASTRA_AUDIT_REASON_LIMIT) {
        digest.reasonEvidenceOverflowCount += 1;
        this.astraDecisionAuditCounters.reasonEvidenceOverflowCount += 1;
        this.decisionAuditCoverage = "UNKNOWN";
        continue;
      }
      const beforeBytes = astraDecisionDigestBytes(digest);
      digest.auditReasonCodes.push(reason);
      const afterBytes = astraDecisionDigestBytes(digest);
      if (this.astraDecisionAuditBytes - beforeBytes + afterBytes > ASTRA_DECISION_AUDIT_MAX_BYTES) {
        digest.auditReasonCodes.pop();
        digest.reasonEvidenceOverflowCount += 1;
        this.astraDecisionAuditCounters.reasonEvidenceOverflowCount += 1;
        this.decisionAuditCoverage = "UNKNOWN";
        continue;
      }
      this.astraDecisionAuditBytes += afterBytes - beforeBytes;
    }
  }

  private astraDecisionAuditSummary(): AstraDecisionAuditSummary {
    const counters = this.astraDecisionAuditCounters;
    const coverage = this.decisionAuditCoverage === "UNKNOWN" || counters.overflow ? "UNKNOWN" : "COMPLETE";
    return {
      ...structuredClone(counters),
      maxDigestCount: ASTRA_DECISION_AUDIT_MAX_DIGESTS,
      maxBytes: ASTRA_DECISION_AUDIT_MAX_BYTES,
      coverage,
      digests: [...this.astraDecisionDigests.values()].map((digest) => structuredClone(digest)),
    };
  }

  private providerDispatchLifecycleFor(event: JournalEvent): ProviderDispatchLifecycle | null {
    const identity = providerDispatchIdentity(event);
    if (!identity) {
      this.providerDispatchTrackingIncomplete = true;
      return null;
    }
    const existing = this.providerDispatches.get(identity);
    if (existing) return existing;
    if (this.providerDispatches.size >= PROVIDER_DISPATCH_TRACKING_LIMIT) {
      this.providerDispatchTrackingOverflow = true;
      return null;
    }
    const lifecycle: ProviderDispatchLifecycle = {
      authorized: false,
      started: false,
      terminal: null,
      invocationObserved: false,
      knownUsageFailureObserved: false,
      unknownFailureObserved: false,
      notDispatchedFailureObserved: false,
      usageObserved: false,
    };
    this.providerDispatches.set(identity, lifecycle);
    return lifecycle;
  }

  private noteProviderDispatchAuthorized(event: JournalEvent): void {
    const lifecycle = this.providerDispatchLifecycleFor(event);
    if (!lifecycle) {
      this.providerDispatchAuthorizedCount += 1;
      return;
    }
    if (!lifecycle.authorized) {
      lifecycle.authorized = true;
      this.providerDispatchAuthorizedCount += 1;
    }
  }

  private noteProviderDispatchStarted(event: JournalEvent): void {
    const lifecycle = this.providerDispatchLifecycleFor(event);
    if (!lifecycle) {
      this.providerDispatchStartedCount += 1;
      return;
    }
    if (lifecycle.terminal !== null) this.providerDispatchTrackingIncomplete = true;
    if (!lifecycle.started) {
      lifecycle.started = true;
      this.providerDispatchStartedCount += 1;
    }
  }

  private noteKnownProviderTerminal(lifecycle: ProviderDispatchLifecycle | null): void {
    if (!lifecycle) {
      this.providerKnownTerminalCount += 1;
      return;
    }
    if (lifecycle.terminal !== "UNKNOWN" && lifecycle.terminal !== "KNOWN") {
      lifecycle.terminal = "KNOWN";
      this.providerKnownTerminalCount += 1;
    }
  }

  private noteProviderUsage(event: JournalEvent, lifecycle: ProviderDispatchLifecycle | null): void {
    const payload = event.payload as { stage?: unknown; usage?: { costUsd?: unknown } };
    const costUsd = payload.usage?.costUsd;
    if (typeof costUsd !== "number" || !Number.isFinite(costUsd)) return;
    if (lifecycle?.usageObserved === true) return;
    const stage = String(payload.stage ?? "DECISION");
    this.modelCosts.ASTRA_BRAIN_V1 = (this.modelCosts.ASTRA_BRAIN_V1 ?? 0) + costUsd;
    const stageKey = `ASTRA_${stage}`;
    this.modelCosts[stageKey] = (this.modelCosts[stageKey] ?? 0) + costUsd;
    if (lifecycle) lifecycle.usageObserved = true;
  }

  private providerDispatchUnresolvedAuthorizationCount(): number {
    let unresolved = 0;
    for (const lifecycle of this.providerDispatches.values()) {
      if (lifecycle.authorized && lifecycle.terminal === null) unresolved += 1;
    }
    return unresolved;
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
    bytes += this.providerDispatches.size * 160;
    bytes += this.astraDecisionAuditBytes;
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

function providerDispatchIdentity(event: JournalEvent): string | null {
  const payload = event.payload as { dispatchId?: unknown; stage?: unknown };
  if (typeof payload.dispatchId === "string" && payload.dispatchId.length > 0) return payload.dispatchId;
  const stage = typeof payload.stage === "string" && payload.stage.length > 0
    ? payload.stage
    : event.type === "ASTRA_MODEL_INVOCATION" ? "DECISION" : null;
  return stage && event.traceId ? `${event.traceId}:${stage}` : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function finiteNonNegativeNumber(value: unknown): number | null {
  const number = typeof value === "number" ? value : typeof value === "string" && value.trim() !== "" ? Number(value) : NaN;
  return Number.isFinite(number) && number >= 0 ? number : null;
}

function asAstraAction(value: unknown): AstraDecisionAuditDigest["action"] {
  return value === "BUY_UP" || value === "BUY_DOWN" || value === "NO_TRADE" ? value : null;
}

function asAstraDirection(value: unknown): AstraDecisionAuditDigest["direction"] {
  return value === "UP" || value === "DOWN" || value === "FLAT" ? value : null;
}

function boundedStringArray(value: unknown, limit: number, maxLength: number): string[] {
  const values = Array.isArray(value) ? value : typeof value === "string" ? [value] : [];
  return values.filter((item): item is string => typeof item === "string" && item.length > 0)
    .map((item) => item.slice(0, maxLength)).slice(0, limit);
}

function collectAstraReasons(decision?: Record<string, unknown> | null): { values: string[]; overflowCount: number } {
  const reasons = boundedStringArray(decision?.reasonCodes, ASTRA_DECISION_REASON_LIMIT, ASTRA_DECISION_REASON_MAX_LENGTH);
  const unique = [...new Set(reasons)];
  return { values: unique.slice(0, ASTRA_DECISION_REASON_LIMIT), overflowCount: Math.max(0, unique.length - ASTRA_DECISION_REASON_LIMIT) };
}

function collectAstraAuditReasons(payload: Record<string, unknown>): { values: string[]; overflowCount: number } {
  const reasons = [
    ...boundedStringArray(payload.gateReason, 1, ASTRA_DECISION_REASON_MAX_LENGTH),
    ...boundedStringArray(payload.failureReason, 1, ASTRA_DECISION_REASON_MAX_LENGTH),
    ...boundedStringArray(payload.reason, 1, ASTRA_DECISION_REASON_MAX_LENGTH),
  ];
  const unique = [...new Set(reasons)];
  return { values: unique.slice(0, ASTRA_AUDIT_REASON_LIMIT), overflowCount: Math.max(0, unique.length - ASTRA_AUDIT_REASON_LIMIT) };
}

function emptyAstraDecisionAuditCounters(): AstraDecisionAuditCounters {
  return {
    decisionCount: 0, providerDecisionCount: 0, buyUpCount: 0, buyDownCount: 0, noTradeCount: 0,
    directionUpCount: 0, directionDownCount: 0, directionFlatCount: 0, fastOnlyCount: 0, deepDecisionCount: 0,
    failClosedCount: 0, deterministicHedgeBypassCount: 0, gateRejectedCount: 0, preProviderRejectedCount: 0,
    overflow: false, overflowCount: 0, reasonEvidenceOverflowCount: 0,
    preProviderRejectReasons: Object.fromEntries([...PRE_PROVIDER_REASON_KEYS, PRE_PROVIDER_OTHER_REASON].map((key) => [key, 0])),
    preProviderRejectReasonOverflow: false,
  };
}

function restoreAstraDecisionAuditCounters(input: AstraDecisionAuditCounters | undefined): AstraDecisionAuditCounters {
  const defaults = emptyAstraDecisionAuditCounters();
  if (!input || typeof input !== "object") return defaults;
  const mutable = defaults as unknown as Record<string, unknown>;
  for (const key of Object.keys(defaults) as Array<keyof AstraDecisionAuditCounters>) {
    const value = input[key];
    if (key === "overflow") defaults[key] = value === true;
    else if (key === "preProviderRejectReasons" && value && typeof value === "object" && !Array.isArray(value)) {
      for (const reason of Object.keys(defaults.preProviderRejectReasons)) {
        const count = (value as Record<string, unknown>)[reason];
        if (typeof count === "number" && Number.isInteger(count) && count >= 0) defaults.preProviderRejectReasons[reason] = count;
      }
    } else if (key === "preProviderRejectReasonOverflow") defaults[key] = value === true;
    else if (typeof value === "number" && Number.isInteger(value) && value >= 0) mutable[key] = value;
  }
  return defaults;
}

function astraDecisionDigestBytes(digest: AstraDecisionAuditDigest): number {
  return Buffer.byteLength(JSON.stringify(digest), "utf8");
}

function isValidAstraDecisionDigest(value: unknown): value is AstraDecisionAuditDigest {
  if (!isRecord(value)) return false;
  return typeof value.id === "string" && value.id.length > 0
    && typeof value.traceId === "string" && value.traceId.length > 0
    && (value.triggerId === null || typeof value.triggerId === "string")
    && (value.marketId === null || typeof value.marketId === "string")
    && (value.asset === null || typeof value.asset === "string")
    && (value.source === "PROVIDER" || value.source === "FAIL_CLOSED" || value.source === "DETERMINISTIC_HEDGE" || value.source === "GATE_REJECT" || value.source === "PRE_PROVIDER_REJECT")
    && (value.action === null || value.action === "BUY_UP" || value.action === "BUY_DOWN" || value.action === "NO_TRADE")
    && (value.direction === null || value.direction === "UP" || value.direction === "DOWN" || value.direction === "FLAT")
    && Array.isArray(value.stages) && value.stages.length <= 2 && value.stages.every((stage) => typeof stage === "string" && stage.length <= 16)
    && Array.isArray(value.reasonCodes) && value.reasonCodes.length <= ASTRA_DECISION_REASON_LIMIT && value.reasonCodes.every((reason) => typeof reason === "string" && reason.length <= ASTRA_DECISION_REASON_MAX_LENGTH)
    && Array.isArray(value.auditReasonCodes) && value.auditReasonCodes.length <= ASTRA_AUDIT_REASON_LIMIT && value.auditReasonCodes.every((reason) => typeof reason === "string" && reason.length <= ASTRA_DECISION_REASON_MAX_LENGTH)
    && typeof value.reasonEvidenceOverflowCount === "number" && Number.isInteger(value.reasonEvidenceOverflowCount) && value.reasonEvidenceOverflowCount >= 0
    && (value.outcome === "MODEL_NO_TRADE" || value.outcome === "DIRECTIONAL_DECISION" || value.outcome === "SYNTHETIC_FAIL_CLOSED" || value.outcome === "DETERMINISTIC_HEDGE_BYPASS" || value.outcome === "GATE_REJECTED" || value.outcome === "PRE_PROVIDER_REJECTED" || value.outcome === "RISK_ACCEPTED" || value.outcome === "RISK_VETO" || value.outcome === "NO_EXECUTABLE_CANDIDATE" || value.outcome === "EXECUTION_GATE_REJECTED" || value.outcome === "BOOK_REVALIDATION_REJECT" || value.outcome === "ORDER_CREATED" || value.outcome === "FILLED" || value.outcome === "EXECUTION_ERROR" || value.outcome === "CANCELLED")
    && (value.riskOutcome === "NOT_EVALUATED" || value.riskOutcome === "ACCEPT" || value.riskOutcome === "VETO")
    && typeof value.orderCreated === "boolean"
    && typeof value.fillCount === "number" && Number.isInteger(value.fillCount) && value.fillCount >= 0;
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
