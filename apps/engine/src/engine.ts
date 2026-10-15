import { createHash } from "node:crypto";
import { experimentWsEventSchema, marketWsReason, type ExperimentWsEvent } from "@pm/core";
import { bookTiming, firstLegBookReason, officialMarketStartReason, firstLegEntryFilterReason, firstLegExecutionEconomics, FIRST_LEG_MIN_NET_ROI } from "./first-leg-admission.js";
import type { GuardianView, GuardianMarket, GuardianUpdate } from "./guardian-link.js";
import { currentGuardianConsensus } from "./guardian-link.js";
import { guardianV2StateSchema, type GuardianV2State } from "@pm/core";
import { evaluateGuardianV2, lockedGuardianGeneration, GUARDIAN_V2_CONFIG, isGuardianV2State, projectGuardianBookSignal, reversalProtectionStateSchema, sameReversalEpisode, type GuardianV2Event, type ReversalProtectionState, type ReversalEpisode } from "@pm/core";
import { nakedDirectionalDiagnosticSchema, observeNakedDirectionalPosition, noteNakedDirectionalAnomaly, compactNakedDirectionalObservation,
  nakedDirectionalSummary, SOAK_PERSIST_INTERVAL_MS, evaluateProfitTrailingPair, profitTrailingPairStateSchema,
  type NakedDirectionalDiagnostic, type ProfitTrailingPairState } from "@pm/core";
import {
  PaperLedger,
  TemporalPairHedgeStrategy,
  TriggerEngine,
  createId,
  decidePaperOrder,
  decideProtectivePaperOrder,
  decideRescueSecondLegOrder,
  arbitrateRiskReduction,
  evaluateProtectiveDeRiskCandidate,
  evaluateRescueSecondLegCandidate,
  evaluateEarlyCatastrophicPairLock,
  evaluateNormalHedgeCandidate,
  computeGuaranteedLockedPnl,
  walkBook,
  calculateFee,
  computeRealizableMarkLossUsd,
  type EarlyCatastrophicPairLockEvaluation,
  type EarlyCatastrophicRejectionReason,
  type ConsensusDecision,
  type RescueCandidateEvaluation,
  type RiskReductionCandidateEconomics,
  defaultNativeDecisionConfig,
  evaluateNativeDecision,
  NATIVE_DETERMINISTIC_AUTHORITY,
  parseEngineEnv,
  executePaperTaker,
  experimentManifestSchema,
  qualificationConfigSchema,
  type QualificationConfig,
  refreshExperiment,
  riskLimitsSchema,
  startExperiment,
  triggerConfigSchema,
  ledgerStateSchema,
  positionSchema,
  createRestingMakerOrder,
  restingPaperOrderSchema,
  evolveMakerOrder,
  evolveMakerTrade,
  passiveMakerBuyPrice,
  planDualMakerQuotes,
  type MarketTrade,
  grokSignalSubmissionSchema,
  grokSignalEnvelopeSchema,
  grokActivationRequestSchema,
  triggerEventSchema,
  calculatePairMetrics,
  computePaperExposures,
  projectPaperExposures,
  makerReserveNotional,
  executableFairEdge,
  parsePublicBinaryResolution,
  isAuthoritativeBinaryMapping,
  type BinarySettlementLookup,
  conservativeRealizableMark,
  classifySettledOutcome,
  settlementCashDelta,
  settlementRealizedDelta,
  createIdleExperiment,
  d,
  decimal,
  experimentAllowsFirstLeg,
  experimentAllowsHedge,
  openPositionsRemain,
  shouldWriteFinalReport,
  isReportInputEventType,
  type JournalEvent,
  type ExternalAgentRoleId,
  type Direction,
  type GrokActivationBridge,
  type GrokActivationRequest,
  type GrokSignalEnvelope,
  type GrokSignalSubmission,
  type EngineEnv,
  type ExperimentManifest,
  type MarketSnapshot,
  type LedgerState,
  type Outcome,
  type StrategyCandidate,
  type PaperOrder,
  type Position,
  type ReferenceTick,
  type RestingPaperOrder,
  type TriggerEvent,
  type ActivationLatency,
  AssetMarketIndex,
  classifyTriggerProvenance,
  isMarketDataDerivedTrigger,
  type TriggerProvenance,
  type NativeDecision,
  type NativeReferenceFeatures,
  type NativeDecisionConfig,
  NATIVE_DETERMINISTIC_MODEL_VERSION,
  rolesForTriggerKind,
  type RiskReductionSelection,
  type RiskReductionSelectionReason,
  type HedgePositionFacts,
} from "@pm/core";
import {
  assertJournalEventIdentity,
  JournalEventConflictError,
  PersistenceOutcomeUnknownError,
  retryPersistenceTransport,
  type JournalAppendOptions,
  QUARANTINE_TRANSITION, quarantineOperationKey, quarantinePayloadSchema, quarantineReceiptSchema,
  ExperimentQuarantineRejectedError, type QuarantineReceipt,
  CHECKPOINT_SCHEMA_VERSION,
  CheckpointFenceError,
  createJournalEvent,
  prepareJournalEvent,
  derivedAuditEventId,
  DurableCursorBehindError,
  IdempotencyConflictError,
  isCanonicalFinalReport,
  isJournalAppendAbortedError,
  hashCheckpointPayload,
  hashDurablePayload,
  type DurableCommitResult,
  type DurableTransition,
  type EventJournal,
} from "@pm/db";
import {
  BtcWebSocketAdapter,
  ClobMarketCloseError,
  PolymarketMarketDataAdapter,
  ReferenceFeedRegistry,
  createRestProbe,
  type DiscoveredMarket,
  type FeedConnectionEvent,
  type MinOrderSizeHydratedEvent,
  type PolymarketFeedAnomaly,
} from "@pm/market-data";
import { EngineMetrics, classifyJournalMetricClass } from "./metrics.js";
import { RestingOrderTokenIndex } from "./resting-order-index.js";
import { SseHub } from "./server.js";
import { EngineStateStore } from "./state.js";
import { ensureReportGeneratedAudit, persistCanonicalFinalReport, persistVerifiedAnalyticsRollup, streamExperimentSummary, summaryFromDurableReport } from "./final-report.js";
import { writeExperimentSummary } from "./report.js";
import { retentionConfigFromEngineEnv, RetentionManager } from "./retention-manager.js";
import { OvhArchiveReplicaVerifier } from "./ovh-archive-replica.js";
import {
  boundIntegrityFaults,
  createIntegrityFaultEvent,
  createIntegrityRepairEvent,
  integrityDurableOperationKey,
  INTEGRITY_OVERFLOW_EVENT_ID,
  isReportInputBoundaryType,
  loadIntegrityRepairIds,
  loadUnresolvedReportSourceIntegrity,
  overflowIntegrityFault,
  parseCheckpointIntegrityFaults,
  REPORT_SOURCE_INTEGRITY_HARD_CAP,
  REPORT_SOURCE_INTEGRITY_FAULT,
  shouldLatchReportSourceIntegrity,
  type ReportSourceIntegrityFault,
} from "./report-source-integrity.js";
import { GrokActivationCoordinator } from "./activation/coordinator.js";
import { createGrokActivationBridge } from "./activation/index.js";
import type { GrokRoleContext, GrokSubmissionMode, RoleRunOutcome } from "./activation/role-runner.js";
import { HotResearchCache, isScoutRole, SCOUT_ROLES } from "./activation/hot-research-cache.js";
import { BoundedJournalWriter } from "./journal-writer.js";
import { evaluateExperimentPreflight, isGenuineEntryQualifiedMarket } from "./preflight.js";
import { CrashInjectedError, CrashInjector, type CrashInjectionPoint } from "./crash-injection.js";
import { emptyResultAccounting, resultAccountingSchema, economicResults, tradingResults } from "./trading-results.js";
import { DurableMutationGate } from "./durable-gate.js";
import {
  applyDurableTransition,
  assessLegacyCheckpointSafety,
  cloneRecoverableState,
  durableIdentityConflicts,
  isLegacyCheckpointPayload,
  normalizeRecoveryError,
  parseCheckpointDerivedAuditCursor,
  parseCheckpointDurableCursor,
  parseDerivedAudit,
  plannedDurableTransition,
  RecoveryFailClosedError,
  stablePositionId,
  riskReductionOwnershipSchema,
  riskReductionExecutionFailureSchema,
  settlementLookupSchema,
  type RiskReductionExecutionFailure,
  unhedgedEpisode,
  validatePostReplayInvariants,
  type DerivedAuditRecord,
  type RecoverableEngineState,
  type RiskReductionOwnership,
} from "./recovery.js";
import {
  DirectJournalAdmission,
  FiniteSemaphore,
  JournalAdmissionRejected,
  OverloadLatch,
  RUNTIME_BOUNDS,
  RuntimeSupervisor,
  SingleFlight,
  SharedResultFlight,
  isCriticalPersistenceType,
  isRiskHedgeSettlementKind,
  remapTriggerRejection,
  triggerWorkKey,
  triggerWorkLane,
  type AdmitResult,
  type JoinResult,
} from "./runtime-supervisor.js";

class ExperimentStartGuardRejectedError extends Error {
  constructor() { super("EXPERIMENT_START_MARKET_TRUTH_CHANGED"); }
}

class SettlementObligationChangedError extends Error {
  constructor() { super("SETTLEMENT_OBLIGATION_CHANGED"); }
}

class FirstLegLifecycleGuardRejectedError extends Error {
  constructor(readonly marketId: string, readonly reason: string) {
    super(`FIRST_LEG_LIFECYCLE_REJECTED:${marketId}:${reason}`);
  }
}

class GuardianPersistenceGuardRejectedError extends Error {
  constructor(readonly marketId: string, readonly reason: "EPISODE_CHANGED" | "SUPERSEDED") {
    super(`GUARDIAN_PERSISTENCE_REJECTED:${marketId}:${reason}`);
  }
}

class RiskReductionLifecycleGuardRejectedError extends Error {
  constructor(
    readonly marketId: string,
    readonly intent: "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG" | "GUARDIAN_PROTECTIVE_SELL",
    readonly reason: string,
  ) {
    super(`RISK_REDUCTION_LIFECYCLE_REJECTED:${marketId}:${intent}:${reason}`);
  }
}

/**
 * Normal serialized-boundary rejection for a NEW TAKER BUY whose actual
 * fee-inclusive cash requirement exceeds CURRENT unreserved cash at the
 * durable mutation boundary. This is the concurrency safety-net behind the
 * earlier risk-admission cash check: multiple TAKER BUYs can be planned
 * against the same pre-fill cash, and only the serialized durable boundary
 * observes each preceding fill's committed effect. It is an expected
 * concurrent economic state-change outcome, never a durability/DB/integrity
 * failure and never execution corruption.
 */
export class TakerCashCommitGuardRejectedError extends Error {
  constructor(
    readonly fillId: string,
    readonly marketId: string,
    readonly requiredCash: string,
    readonly availableCash: string,
  ) {
    super(`TAKER_CASH_FENCE_REJECTED:${fillId}:${marketId}:required=${requiredCash}:available=${availableCash}`);
  }
}

/**
 * Normal serialized-boundary rejection for a NEW maker cash reservation whose
 * required reservation exceeds CURRENT unreserved cash at the durable mutation
 * boundary. The reserveMaker preflight is a cheap, non-serialized admission
 * read: two different-market maker admissions can be planned against the same
 * pre-reservation cash and both pass it. Only the serialized durable boundary
 * observes each preceding reservation's committed effect. It is an expected
 * concurrent economic state-change outcome, never a durability/DB/integrity
 * failure and never execution corruption. The rejection writes no
 * PAPER_MAKER_RESERVED, reserves no cash, and creates no resting order state.
 */
export class MakerReservationCashCommitGuardRejectedError extends Error {
  constructor(
    readonly orderId: string,
    readonly marketId: string,
    readonly requiredCash: string,
    readonly availableCash: string,
  ) {
    super(`MAKER_CASH_FENCE_REJECTED:${orderId}:${marketId}:required=${requiredCash}:available=${availableCash}`);
  }
}

const IDLE_GROK_IDEMPOTENCY_MAX = 4_096;
const IDLE_GROK_IDEMPOTENCY_TTL_MS = 5 * 60_000;

const GUARDIAN_OBSERVATION_CHECKPOINT_MS = 500;

export const GUARDIAN_PERSISTENCE_BOUNDS = Object.freeze({ perMarket: 64, total: 256, bytes: 4 * 1024 * 1024 });
export type GuardianPersistenceOutcome =
  | { status: "PERSISTED" | "SKIPPED_SUPERSEDED" | "SKIPPED_EPISODE_CHANGED"; sourceEventId: string | null }
  | { status: "FAILED"; error: unknown; sourceEventId: string | null };
type GuardianPersistenceRequest = {
  experimentId: string;
  generation: number;
  kind: "critical" | "checkpoint";
  state: ReversalProtectionState;
  events: GuardianV2Event[];
  sourceEvent?: JournalEvent;
  bytes: number;
  done: Promise<GuardianPersistenceOutcome>;
  resolve: (outcome: GuardianPersistenceOutcome) => void;
};

const SCOUT_REFRESH_STATE_TTL_MS = 60_000;
const SCOUT_REFRESH_CONTEXT_MAX = 128;
const SCOUT_REFRESH_RETRY_GAP_MS = 1_000;
const PROTECTIVE_DE_RISK_RETRY_MS = 5_000;
const PROTECTIVE_DE_RISK_VETO_RETRY_MS = 30_000;
const PROTECTIVE_DE_RISK_WAIT_SAMPLE_MS = 5_000;
/**
 * Pre-expiry safety window for the protective de-risk deadline, derived from
 * existing engine timing bounds (not an arbitrary buffer):
 *   - one maintenance cadence (60s) so at least one eligible tick can land
 *     inside the window even in the worst-case maintenance schedule;
 *   - the configured book freshness bound (riskLimits.staleFeedTimeoutMs, 15s
 *     default) so the last eligible decision can still observe a fresh
 *     synchronized book before the market ends;
 *   - one protective retry slot (PROTECTIVE_DE_RISK_RETRY_MS = 5s) for pacing.
 * Effective protective deadline =
 *   min(unhedgedStartedAt + maxUnhedgedDurationMs, marketEnd - this window),
 * with attempts additionally barred at/after marketEnd. This only bounds WHEN
 * the existing risk-reducing SELL may begin; it never opens or increases risk.
 */
const PROTECTIVE_DE_RISK_PRE_EXPIRY_SAFETY_MS = 80_000;
/**
 * Diagnostic-only bound for PROTECTIVE_DE_RISK_SUPPRESSED observability rows.
 * Ordinary polling emits at most one durable row per gate per 60s, aligned to
 * the maintenance cadence. Accepted-attempt terminal facts use a separate
 * attempt identity. Pure observability: never affects economic authority.
 */
const PROTECTIVE_DE_RISK_SUPPRESS_EMIT_MIN_MS = 60_000;

/**
 * Consensus stub used ONLY by the canonical normal-hedge admissibility check
 * (HEDGE priority). The TemporalPairHedgeStrategy hedge branch derives its
 * candidate purely from position + books; consensus direction is unused there.
 */
const HEDGE_CHECK_CONSENSUS: ConsensusDecision = {
  id: "early-catastrophic-hedge-check",
  traceId: "early-catastrophic-hedge-check",
  triggerId: "early-catastrophic-hedge-check",
  consensusDirection: "UP",
  consensusConfidence: 1,
  agentAgreement: 1,
  signalFreshness: 1,
  conflicts: [],
  estimatedEdge: 0,
  evidenceIds: ["early-catastrophic-hedge-check"],
  calculatedAt: new Date(0).toISOString(),
};
type IdleGrokSubmission = { observedAt: number; pending: boolean };
type RetainedBookkeepingCounts = {
  activeTriggers: number;
  triggerProvenance: number;
  provenanceInvalid: number;
  signalsByTrigger: number;
};
type RetainedCleanupCycle = { atMs: number; before: RetainedBookkeepingCounts; after: RetainedBookkeepingCounts };
type GrokSubmissionResult = {
  status: "accepted" | "duplicate" | "stale" | "unsupported_evidence" | "unknown_trigger" | "trace_mismatch" | "market_mismatch" | "asset_mismatch" | "replay_rejected" | "malformed" | "identity_mismatch";
  signalId?: string;
};

type RiskReductionUnhedgedReason = "REVERSAL_CONFIRMED" | "GUARDIAN_PROTECTIVE_SELL" | "MAX_UNHEDGED_DURATION" | "PRE_EXPIRY_SAFETY" | "EARLY_CATASTROPHIC_PAIR_LOCK" | "EARLY_HEDGE_ADOPTION";

type EarlyAdmissionAttempt = { riskId: string; admitted: boolean; reason: string };
type EarlyPairLockResult = { acted: boolean; acceptedWithoutAdmission?: boolean };

type ProtectiveDeRiskAudit = {
  deRiskReason: RiskReductionUnhedgedReason;
  positionId?: string;
  heldOutcome?: Outcome;
  guardianSensor?: "EARLY" | "LATE";
  /**
   * Observability improvement (Â§44): distinguishes which deadline bound made
   * the position protective-eligible â€” the hard unhedged duration
   * (MAX_UNHEDGED_DURATION) or the expiry safety window (PRE_EXPIRY_SAFETY).
   * Behavior is identical to the prior single label; this only removes the
   * diagnostic ambiguity.
   */
  eligibilityReason: RiskReductionUnhedgedReason;
  unhedgedStartedAt: string;
  unhedgedDurationMs: number;
  attemptedAction: "SELL_UNPAIRED" | "RESCUE_SECOND_LEG" | "HEDGE_SECOND_LEG" | "NONE";
  requestedQty: string;
  executableQty: string;
  executionPrice: string | null;
  residualQuantity: string;
  residualExposure: string;
  retryCount: number;
  finalState: "PRECHECK_ACCEPTED" | "ORDER_ADMITTED" | "RISK_VETO" | "RESIDUAL_UNHEDGED" | "RISK_NEUTRALIZED" | "EXECUTION_ERROR" | "STATE_CHANGED";
  /** Rescue arbitration observability (bounded; detailed economics in journal). */
  selection?: RiskReductionSelection;
  selectionReason?: RiskReductionSelectionReason;
  tieBreakUsed?: boolean;
  aAvailable?: boolean;
  aReason?: string | null;
  aQuantity?: string | null;
  aVwap?: string | null;
  aFeeUsd?: string | null;
  aGuaranteedSalvageUsd?: string | null;
  bAvailable?: boolean;
  bReason?: string | null;
  bQuantity?: string | null;
  bVwap?: string | null;
  bFeeUsd?: string | null;
  bRequiredCashUsd?: string | null;
  bGuaranteedSalvageUsd?: string | null;
  guardianReason?: string;
  rescueFailureOrderId?: string | null;
};


export class EngineService {
  readonly metrics = new EngineMetrics();
  readonly sse: SseHub;
  readonly ledger: PaperLedger;
  readonly state: EngineStateStore;
  readonly activation: GrokActivationCoordinator;
  private readonly abort = new AbortController();
  private readonly triggerEngine;
  private readonly riskLimits;
  private readonly triggerConfig;
  private readonly qualificationConfig: QualificationConfig;
  private readonly nativeDecisionConfig: NativeDecisionConfig;
  private readonly strategy = new TemporalPairHedgeStrategy();
  private readonly marketAdapter: PolymarketMarketDataAdapter;
  private readonly legacyBtcAdapter: BtcWebSocketAdapter | null;
  private readonly referenceRegistry: ReferenceFeedRegistry;
  private readonly writer: BoundedJournalWriter;
  private readonly nowFn: () => Date;
  private readonly resolutionFetcher: (idOrSlug: string) => Promise<Record<string, unknown> | null>;
  private readonly snapshots = new Map<string, Partial<Record<Outcome, MarketSnapshot>>>();
  private readonly restingOrders = new Map<string, RestingPaperOrder>();
  private readonly makerReservations = new Map<string, string>();
  private readonly makerActiveAfter = new Map<string, string>();
  private readonly makerQueuePrimed = new Set<string>();
  private readonly makerTradeTails = new Map<string,Promise<void>>();
  private makerQuoteTimer: ReturnType<typeof setTimeout> | null = null;
  private readonly recentTrades = new Map<string, number>();
  /** Ephemeral reservation for a not-yet-durable first-leg submission. */
  private readonly pendingFirstLegMarkets = new Set<string>();
  /**
   * ONE risk-reduction ownership flight per market, shared by normal
   * HEDGE_SECOND_LEG, controlled-loss RESCUE_SECOND_LEG and PROTECTIVE_DE_RISK
   * so the same unpaired residual can never admit two independent economic
   * mutations concurrently (PR2 single risk-reduction authority).
   */
  private readonly riskReductionInFlight = new Set<string>();
  private readonly riskReductionOwners = new Map<string, RiskReductionOwnership>();
  private readonly nakedDirectionalDiagnostics = new Map<string, NakedDirectionalDiagnostic>();
  private readonly reversalProtectionStates = new Map<string, ReversalProtectionState>();
  /** Per-tick Guardian sensor state. Raw market observations stay off the durable ingress path. */
  private readonly guardianRuntimeStates = new Map<string, ReversalProtectionState>();
  private readonly guardianConsensus = new Map<string, NonNullable<GuardianMarket["consensus"]>>();
  private readonly guardianAdmissions = new Map<string, { update: GuardianUpdate; done: Promise<GuardianPersistenceOutcome> }>();
  /** At most one coalescible ordinary checkpoint plus rare ordered critical transitions per market. */
  private readonly guardianPersistenceQueues = new Map<string, GuardianPersistenceRequest[]>();
  private guardianPersistenceBytes = 0;
  private readonly guardianShutdownAbort = new AbortController();
  private readonly guardianPersistenceWorkers = new Map<string, Promise<void>>();
  private readonly guardianCheckpointQueuedAt = new Map<string, number>();
  private readonly reversalProtectionInFlight = new Set<string>();
  private readonly reversalProtectionPending = new Set<string>();
  private readonly reversalProtectionSnapshotScheduled = new Set<string>();
  private readonly profitTrailingPairStates = new Map<string, ProfitTrailingPairState>();
  private readonly profitTrailingPairInFlight = new Set<string>();
  private readonly profitTrailingPairPending = new Set<string>();
  /** Only snapshot-originated legacy de-risk after duration; pair books get one event-loop turn to coalesce. */
  private readonly profitTrailingDeRiskSnapshotScheduled = new Set<string>();
  /** Vetoed post-duration PAIR evidence cannot suppress protective A/B on retry. */
  private readonly profitTrailingRejectedEvidence = new Map<string, string>();
  private readonly soakPersistedRevisions = new Map<string, number>();
  private readonly soakPersistedObservationCounts = new Map<string, number>();
  private lastSoakPersistAt = 0;
  private readonly riskReductionFailures = new Map<string, RiskReductionExecutionFailure>();
  private readonly protectiveDeRiskAttempts = new Map<string, number>();
  /** Earliest next durable de-risk evaluation per market. Ephemeral by design. */
  private readonly protectiveDeRiskNextAttemptAt = new Map<string, number>();
  /**
   * PR3 early-catastrophic retry/veto pacing mirrors PR82: a stable canonical
   * veto must not generate critical RISK_EVALUATION writes at snapshot rate.
   * Ephemeral by design; cleared on neutralization/settlement/market removal.
   */
  private readonly earlyCatastrophicNextAttemptAt = new Map<string, number>();
  /** Fingerprint of the last stable early veto (vetoes + material inputs). */
  private readonly earlyCatastrophicVetoFingerprint = new Map<string, string | null>();
  /** Instrumentation: number of canonical hedge-priority decisions evaluated by PR3. */
  private pr3HedgeDecisionCount = 0;
  /** Last PR3 hedge-check gating reason per market (instrumentation, bounded). */
  private readonly pr3HedgeDebug = new Map<string, { stage: string; vetoes: string[] | null }>();
  /** Last runtime-basis asset resolved by decideWithRuntimeBasis (parity test). */
  private lastRuntimeBasisAssetForTest: string | null = null;
  /** Identifies whether a hard veto is materially unchanged during backoff. */
  private readonly protectiveDeRiskVetoFingerprint = new Map<string, string>();
  /** Bounds non-durable waiting feed/SSE samples when no sell depth exists. */
  private readonly protectiveDeRiskLastWaitingAt = new Map<string, number>();
  /** Diagnostic-only: last PROTECTIVE_DE_RISK_SUPPRESSED emission per market+gate. */
  private readonly protectiveDeRiskLastSuppressEmitAt = new Map<string, number>();
  private readonly activeTriggers = new Map<string, TriggerEvent>();
  private readonly triggerProvenance = new Map<string, TriggerProvenance>();
  private readonly provenanceInvalid = new Set<string>();
  private readonly signalsByTrigger = new Map<string, Map<ExternalAgentRoleId, GrokSignalEnvelope>>();
  private readonly signalReceivedTimes: number[] = [];
  /** Bounded non-economic contexts used only while warming scout research. */
  private readonly scoutRefreshContexts = new Map<string, TriggerEvent>();
  private readonly scoutRefreshLastAt = new Map<string, number>();
  private readonly scoutRefreshInFlight = new Set<string>();
  private readonly scoutRefreshSubmissionKeys = new Map<string, number>();
  private readonly marketsById = new Map<string, DiscoveredMarket>();
  private readonly entryQualified = new Set<string>();
  private readonly managedMarketIds = new Set<string>();
  private readonly settledMarkets = new Set<string>();
  private readonly economicSettlementLookups = new Map<string, BinarySettlementLookup>();
  private readonly settlementBackoff = new Map<string, number>();
  private readonly assetByMarket = new Map<string, string | null>();
  private readonly assetIndex = new AssetMarketIndex();
  private readonly restingIndex = new RestingOrderTokenIndex();
  private readonly snapshotEpoch = new Map<string, number>();
  private readonly referenceUnreliable = new Set<string>();
  private readonly referenceEpoch = new Map<string, number>();
  private readonly referenceLogicalGapped = new Set<string>();
  private readonly referenceResyncInFlight = new Set<string>();
  private readonly referenceResyncRequested = new Set<string>();
  private readonly referenceRequired = new Map<string, boolean>();
  private readonly referenceBlocked = new Map<string, string>();
  private readonly referenceRetryTimers = new Map<string, NodeJS.Timeout>();
  private readonly referenceRetryDeadlines = new Map<string, number>();
  private tokenIds: string[] = [];
  private tokenIdSet = new Set<string>();
  private marketLoopStarted = false;
  private experiment: ExperimentManifest | null = null;
  /** Monotonic fence for feed/discovery invalidations racing experiment start. */
  private experimentFeedGeneration = 0;
  /** Keeps ingress out of the transient post-commit/start-validation window. */
  private experimentStartPending = false;
  private checkpointSequence = 0;
  private lastAppliedDurableCursor = 0;
  private derivedAuditCursor = 0;
  private readonly durableGate = new DurableMutationGate();
  /** Serializes the IDLE -> RUNNING boundary with idle Grok acceptance only. */
  private readonly experimentBoundaryGate = new DurableMutationGate();
  private readonly crashInjector = new CrashInjector();
  private recoveryPageSize = 256;
  private lastTradeAt: Date | null = null;
  private executionErrors: number[] = [];
  private timestampDisorder = false;
  private started = false;
  private bootstrapFailed = false;
  private maintenanceTimer: NodeJS.Timeout | null = null;
  private guardianDeadlineTimer: NodeJS.Timeout | null = null;
  private soakPersistenceTimer: NodeJS.Timeout | null = null;
  private readonly soakPersistenceFlight = new SharedResultFlight<void>();
  private discoveryTimer: NodeJS.Timeout | null = null;
  private lastInfrastructureAccrualAt = Date.now();
  private lastScheduledReviewAt = 0;
  private lastAnomalyJournalAt = 0;
  private reportGenerated = false;
  private readonly supervisor: RuntimeSupervisor;
  private readonly journalAdmission: DirectJournalAdmission;
  private readonly overload = new OverloadLatch();
  private readonly discoveryFlight: SingleFlight;
  private readonly maintenanceFlight: SingleFlight;
  private readonly heavyRead: FiniteSemaphore;
  private readonly retention: RetentionManager;
  private readonly shutdownJoinDeadlineMs: number;
  private lastShutdown: JoinResult | null = null;
  private lastGuardianShutdown: JoinResult | null = null;
  private lastJournalShutdown: JoinResult | null = null;
  private lastShutdownCheckpointed = false;
  private maintenanceHold: Promise<void> | null = null;
  private processTriggerHold: Promise<void> | null = null;
  private triggerEventPersistHold: Promise<void> | null = null;
  private reportQuarantine: QuarantineReceipt | null = null;
  private finalReportWrites = 0;
  private researchIngressGeneration = 0;
  private researchIngressInFlight = 0;
  private grokClaimHold: Promise<void> | null = null;
  private idleGrokAcceptanceHoldForTest: Promise<void> | null = null;
  /**
   * Idle monitoring deliberately has no durable canonical Grok fact. Keep a
   * bounded process-local claim set for exact live retries and concurrent
   * duplicate suppression; active experiment submissions use the journal.
   */
  private readonly idleGrokSubmissions = new Map<string, IdleGrokSubmission>();
  private readonly hotResearchCache: HotResearchCache;
  private readonly retainedCleanupCycles: RetainedCleanupCycle[] = [];
  private startPostCommitValidationHoldForTest: Promise<void> | null = null;
  private readonly reportSourceIntegrity = new Map<string, ReportSourceIntegrityFault>();
  private readonly persistedIntegrityFaults = new WeakSet<ReportSourceIntegrityFault>();
  private referenceTelemetryHoldForTest: Promise<void> | null = null;
  private lastAdmittedMarketEpochFence: ReadonlyMap<string, number> | null = null;
  private discoveryGenerationApplied = 0;

  constructor(
    readonly env: EngineEnv,
    private readonly journal: EventJournal,
    private readonly logger: { error(bindings: object, message: string): void; info?(bindings: object, message: string): void },
    adapters?: {
      market?: PolymarketMarketDataAdapter;
      btc?: BtcWebSocketAdapter;
      activation?: GrokActivationBridge;
      activationFetch?: typeof fetch;
      referenceRegistry?: ReferenceFeedRegistry;
      now?: () => Date;
      resolutionFetcher?: (idOrSlug: string) => Promise<Record<string, unknown> | null>;
      shutdownJoinDeadlineMs?: number;
      recoveryPageSize?: number;
    },
  ) {
    this.env = env;
    this.sse = new SseHub(env.DASHBOARD_SNAPSHOT_HZ, this.metrics);
    this.nowFn = adapters?.now ?? (() => new Date());
    this.ledger = new PaperLedger(env.PAPER_START_BALANCE, this.nowFn());
    this.state = new EngineStateStore(this.ledger.snapshot(), this.ledger.positions);
    this.triggerConfig = triggerConfigSchema.parse({ scheduledReviewIntervalMs: env.SCHEDULED_REVIEW_INTERVAL_MS });
    this.qualificationConfig = qualificationConfigSchema.parse({ maxQualifiedMarkets: env.MAX_QUALIFIED_MARKETS });
    this.triggerEngine = new TriggerEngine(this.triggerConfig, this.nowFn, this.metrics.triggerObserver());
    this.nativeDecisionConfig = {
      ...defaultNativeDecisionConfig,
      referenceRapidMoveBps: this.triggerConfig.referenceRapidMoveBps,
    };
    this.riskLimits = riskLimitsSchema.parse({
      maxExternalSignalsPerMinute: env.GROK_MCP_RATE_LIMIT_PER_MINUTE,
      normalHedgeMinLockedPairEdge: env.NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE,
    });
    if (env.PROFIT_TRAILING_PAIR_SOAK_ENABLED
      && (env.NAKED_DIRECTIONAL_SOAK_ENABLED
        || env.PAPER_EXECUTION_STYLE !== "TAKER"
        || env.PROFIT_TRAILING_PAIR_ARM_EDGE >= env.NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE
        || env.PROFIT_TRAILING_PAIR_RETRACE_EDGE >= env.NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE)) {
      throw new Error("INVALID_PROFIT_TRAILING_PAIR_CONFIG");
    }
    if(env.PAPER_DUAL_SIDED_MAKER_ENABLED&&!env.PAPER_MAKER_FIRST_ENABLED)throw new Error('DUAL_MAKER_REQUIRES_MAKER_FIRST');
    if(env.PAPER_MAKER_FIRST_ENABLED&&(env.PAPER_EXECUTION_STYLE!=='TAKER'
      ||env.PAPER_MAKER_FIRST_QUOTE_TTL_MS<=env.PAPER_EXECUTION_LATENCY_MS
      ||env.PAPER_MAKER_HEDGE_QUOTE_TTL_MS<=env.PAPER_EXECUTION_LATENCY_MS))throw new Error('INVALID_MAKER_FIRST_BASE_CONFIG');
    this.marketAdapter = adapters?.market ?? new PolymarketMarketDataAdapter(undefined, { hydrateMinOrderSizeFromRest: true });
    if (this.marketAdapter instanceof PolymarketMarketDataAdapter) {
      this.marketAdapter.setNormalizeObserver(this.metrics.normalizeObserver());
      this.marketAdapter.setMinOrderSizeHydrationObserver(this.metrics.minOrderSizeHydrationObserver());
      this.marketAdapter.setIngressObserver({
        onChange: (snapshot) => this.metrics.noteIngressSnapshot(snapshot),
      });
      this.marketAdapter.setShardObserver({ onChange: snapshots => this.metrics.noteShardSnapshots(snapshots) });
    }
    this.legacyBtcAdapter = adapters?.btc ?? null;
    this.referenceRegistry = adapters?.referenceRegistry ?? new ReferenceFeedRegistry(
      env.REFERENCE_WS_BASE_URL, env.REFERENCE_REST_BASE_URL, this.riskLimits.staleFeedTimeoutMs, undefined,
      createRestProbe(env.REFERENCE_REST_BASE_URL),
    );
    this.resolutionFetcher = adapters?.resolutionFetcher ?? ((id) => this.marketAdapter.fetchPublicMarket(id));
    this.hotResearchCache = new HotResearchCache({
      now: this.nowFn,
      maxEntries: env.XAI_SCOUT_CACHE_MAX_ENTRIES,
      roleTtlMs: {
        market_microstructure_scanner: env.XAI_SCOUT_MICRO_TTL_MS,
        crypto_market_scanner: env.XAI_SCOUT_CRYPTO_TTL_MS,
        news_x_intelligence_scanner: env.XAI_SCOUT_NEWS_TTL_MS,
      },
    });
    if (adapters?.recoveryPageSize) this.recoveryPageSize = adapters.recoveryPageSize;
    this.writer = new BoundedJournalWriter(journal, (up) => {
      if (up) this.state.database = "UP";
      else this.notePersistenceOutcome("EXPERIMENT_STARTED", false);
    }, logger, {
      queueLimit: env.TELEMETRY_QUEUE_LIMIT, batchSize: env.TELEMETRY_BATCH_SIZE, sampleEvery: 20, maxConcurrent: 2,
      onTelemetryDegraded: (degraded) => { this.state.telemetryDegraded = degraded; },
    }, this.metrics);
    this.shutdownJoinDeadlineMs = adapters?.shutdownJoinDeadlineMs ?? RUNTIME_BOUNDS.shutdownJoinDeadlineMs;
    this.supervisor = new RuntimeSupervisor(this.metrics.runtimeWorkMetrics(), () => this.syncRuntimeState(), () => this.nowFn().getTime());
    this.journalAdmission = new DirectJournalAdmission(this.metrics.journalAdmissionMetrics(), () => this.syncRuntimeState());
    this.heavyRead = new FiniteSemaphore(RUNTIME_BOUNDS.heavyRead.concurrency, RUNTIME_BOUNDS.heavyRead.waiting);
    this.discoveryFlight = new SingleFlight({
      onCoalesced: () => this.metrics.discoverySingleflightCoalesced.inc(),
      onDirty: (dirty) => this.metrics.discoverySingleflightDirty.set(dirty ? 1 : 0),
      onInFlight: (n) => this.metrics.discoveryRefreshInFlight.set(n),
    });
    this.maintenanceFlight = new SingleFlight({
      onCoalesced: () => this.metrics.maintenanceSingleflightCoalesced.inc(),
      onDirty: (dirty) => this.metrics.maintenanceSingleflightDirty.set(dirty ? 1 : 0),
    });
    this.retention = new RetentionManager(journal, retentionConfigFromEngineEnv(env), {
      noteDeleted: (cls, n) => { if (n > 0) this.metrics.retentionDeleted.inc({ retention_class: cls }, n); },
      noteFailure: () => this.metrics.retentionFailures.inc(),
      noteRun: () => this.metrics.retentionRuns.inc(),
      noteLastSuccess: (iso) => this.metrics.retentionLastSuccessUnix.set(Date.parse(iso) / 1000),
      noteBatch: (size, durationMs) => {
        this.metrics.retentionBatchSize.observe(size);
        this.metrics.retentionBatchDuration.observe(durationMs / 1000);
      },
    }, undefined, env.ARCHIVE_REMOTE_HOST ? OvhArchiveReplicaVerifier.forEngine(env.ARCHIVE_REMOTE_HOST) : undefined);
    this.activation = new GrokActivationCoordinator(
      adapters?.activation ?? createGrokActivationBridge(env, adapters?.activationFetch ?? fetch, {
        contextProvider: (request, role) => this.grokRoleContext(request, role, env.XAI_ALLOW_PORTFOLIO_CONTEXT),
        submitSignal: (submission, clientId, mode) => this.submitGrokSignal(submission, clientId, mode),
        onRoleResult: (request, outcome) => this.onGrokRoleResult(request, outcome),
        now: this.nowFn,
      }),
      {
        cooldownMs: this.triggerConfig.cooldownMs, maxPerMinute: this.triggerConfig.maxActivationsPerMinute,
        suppressAtOrBelow: this.triggerConfig.activationSeveritySuppression,
        memoryLimit: env.ACTIVATION_MEMORY_LIMIT, fanout: env.ACTIVATION_FANOUT,
      },
      this.nowFn,
    );
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    await this.initializeJournalWithRetry();
    await this.restoreAndRecover();
    await this.restoreReportSourceIntegrity();
    await this.recoverInterruptedFirstLegOrders();
    if(this.experiment&&['RUNNING','ENTRY_CLOSED'].includes(this.experiment.status)){
      const config=this.makerFirstConfig();
      if(config.enabled!==this.env.PAPER_MAKER_FIRST_ENABLED
        ||config.twoSidedFreshEntries!==this.env.PAPER_DUAL_SIDED_MAKER_ENABLED||(config.enabled
        &&(config.firstLegTtlMs!==this.env.PAPER_MAKER_FIRST_QUOTE_TTL_MS||config.hedgeTtlMs!==this.env.PAPER_MAKER_HEDGE_QUOTE_TTL_MS)))
        throw new RecoveryFailClosedError('invariant','maker-first active manifest/runtime configuration mismatch');
    }
    // A disconnected simulator cannot prove the intervening queue/trade flow.
    // Reconcile durable fills first, then cancel ONLY hybrid quote remainders.
    for(const resting of [...this.restingOrders.values()])if(resting.order.makerFirst)
      await this.releaseMaker(resting.order.id,{cancel:true,reason:"MAKER_FIRST_RESTART_RECONCILE"});
    if (this.experiment?.wsStats && ["RUNNING", "ENTRY_CLOSED"].includes(this.experiment.status))
      await this.persistExperimentWsEvent({ eventId: createId("ws_restart"), kind: "PROCESS_RESTART", shard: 0,
        retry: false, reason: "unknown", at: this.nowFn().toISOString() });
    // Pair the shutdown/recovered snapshot with a current Engine snapshot. This
    // avoids copying a multi-hour fallback replay tail for each small purge batch.
    if (this.env.GUARDIAN_SEPARATE_ENABLED && this.env.RETENTION_ENABLED && this.env.RETENTION_COMPACTION_ENABLED)
      await this.checkpoint();
    this.state.recoveryComplete = true;
    this.noteSoakAnomaly("RECOVERY_OBSERVATION_GAP", { reason: "No observations are fabricated during downtime" });
    const configuredTokens = splitList(this.env.POLYMARKET_TOKEN_IDS);
    if (configuredTokens.length > 0) {
      configuredTokens.forEach((tokenId, index) => this.marketAdapter.registerManualToken(tokenId, tokenId, index % 2 === 0 ? "UP" : "DOWN"));
      this.tokenIds = configuredTokens;
      this.tokenIdSet = new Set(configuredTokens);
      this.marketAdapter.setTokenIds(this.tokenIds);
      this.state.setDiscoveryStatus("MONITORING", this.tokenIds.length);
      this.state.setDiscoveryLoopActive(false);
    } else {
      this.state.setDiscoveryLoopActive(true);
      await this.runDiscovery();
    }
    this.startMarketLoop();
    if (this.legacyBtcAdapter) {
      void this.legacyBtcAdapter.run({
        onTick: (tick) => this.onReferenceTick(tick.asset ? tick : { ...tick, asset: "BTC" }),
        onConnection: (event) => this.onConnection({ ...event, feed: event.feed === "reference" ? "btc" : event.feed, asset: event.asset ?? "BTC" }),
        onError: (error) => this.onFeedError("btc", error),
      }, this.abort.signal);
    } else if (configuredTokens.length > 0) {
      await this.reconcileReferenceFeeds();
    }

    this.maintenanceTimer = setInterval(() => { void this.runMaintenance(); }, 60_000);
    this.maintenanceTimer.unref();
    this.scheduleGuardianDeadline();
    this.soakPersistenceTimer = setInterval(() => {
      void this.flushSoakDiagnostics().catch((error) => this.logger.error({ err: error }, "soak diagnostic persistence failed"));
    }, SOAK_PERSIST_INTERVAL_MS);
    this.soakPersistenceTimer.unref();
    if (configuredTokens.length === 0) {
      this.discoveryTimer = setInterval(() => { void this.runDiscovery().catch((error) => this.logger.error({ err: error }, "discovery refresh failed")); }, this.env.DISCOVERY_INTERVAL_MS);
      this.discoveryTimer.unref();
    }
    this.updateReady();
    this.record("ENGINE_STARTED", this.experiment?.id ?? createId("engine"), null, {
      tokenIds: this.tokenIds.length, mode: "paper", activationProvider: this.env.GROK_ACTIVATION_PROVIDER,
      discoveryStatus: this.state.discoveryStatus, qualifiedMarkets: this.marketsById.size,
      engineAlive: this.state.engineAlive(), marketMonitoring: this.state.marketMonitoring,
    }, null);
  }

  async stop(): Promise<void> {
    if (this.state.draining) return;
    this.state.draining = true;
    this.state.ready = false;
    this.lastShutdownCheckpointed = false;
    this.supervisor.closeAdmission();
    this.record("ENGINE_DRAINING", this.experiment?.id ?? "engine", null, {});
    this.abort.abort("SIGTERM");
    for (const timer of this.referenceRetryTimers.values()) clearTimeout(timer);
    this.referenceRetryTimers.clear();
    if (this.maintenanceTimer) clearInterval(this.maintenanceTimer);
    this.maintenanceTimer = null;
    if (this.guardianDeadlineTimer) clearTimeout(this.guardianDeadlineTimer);
    this.guardianDeadlineTimer = null;
    if(this.makerQuoteTimer)clearTimeout(this.makerQuoteTimer);
    this.makerQuoteTimer=null;
    if (this.soakPersistenceTimer) clearInterval(this.soakPersistenceTimer);
    this.soakPersistenceTimer = null;
    if (this.discoveryTimer) clearInterval(this.discoveryTimer);
    this.discoveryTimer = null;
    if (this.experiment?.status === "RUNNING" || this.experiment?.status === "ENTRY_CLOSED") {
      await this.accrueInfrastructureCost();
    }
    const deadline = this.shutdownJoinDeadlineMs;
    const started = Date.now();
    this.lastShutdown = await this.supervisor.join(deadline);
    this.flushGuardianRuntimeForShutdown();
    let remaining = Math.max(0, deadline - (Date.now() - started));
    this.lastGuardianShutdown = await this.joinGuardianPersistence(remaining);
    if (!this.lastGuardianShutdown.drained) this.guardianShutdownAbort.abort("GUARDIAN_SHUTDOWN_UNRESOLVED");
    let makerDrained=this.makerTradeTails.size===0;
    if(!makerDrained){
      remaining=Math.max(0,deadline-(Date.now()-started));
      makerDrained=await new Promise<boolean>(resolve=>{
        const timer=setTimeout(()=>resolve(false),remaining);
        void Promise.all([...this.makerTradeTails.values()]).then(()=>{clearTimeout(timer);resolve(true);});
      });
      if(!makerDrained)this.logger.error({unresolved:this.makerTradeTails.size},'maker trade shutdown unresolved; no clean checkpoint');
    }
    this.journalAdmission.closeAdmission();
    remaining = Math.max(0, deadline - (Date.now() - started));
    this.lastJournalShutdown = await this.journalAdmission.join(remaining);
    const latestDurable = this.lastGuardianShutdown.drained && this.lastShutdown.drained && this.lastJournalShutdown.drained
      ? await this.journal.latestDurableSequence() : null;
    const cleanDrain = Boolean(
      this.lastShutdown.drained
      && makerDrained
      && this.lastGuardianShutdown.drained
      && this.lastJournalShutdown.drained
      && !this.lastShutdown.timedOut
      && !this.lastGuardianShutdown.timedOut
      && !this.lastJournalShutdown.timedOut
      && this.state.database === "UP"
      && !this.overload.hasIntegrityFault
      && this.lastAppliedDurableCursor === latestDurable,
    );
    if (cleanDrain) {
      await this.checkpoint();
      this.lastShutdownCheckpointed = true;
    }
    this.sse.close();
    await this.journal.close();
    this.clearResearchActivationState();
    this.record("ENGINE_STOPPED", this.experiment?.id ?? "engine", null, {
      unresolvedOwnedWork: (this.lastShutdown.unresolved ?? 0)
        + (this.lastGuardianShutdown.unresolved ?? 0)
        + (this.lastJournalShutdown.unresolved ?? 0),
      shutdownTimedOut: this.lastShutdown.timedOut || this.lastGuardianShutdown.timedOut || this.lastJournalShutdown.timedOut,
      checkpointed: this.lastShutdownCheckpointed,
    });
  }

  noteMcpRequest(tool: string | null, triggerId?: string): void {
    if (triggerId) {
      // Attribute actual inbound research activity, not every Native trigger.
      const trigger = this.activeTriggers.get(triggerId);
      if (trigger) this.activation.ensureLatency(trigger);
      this.activation.noteMcp([triggerId]);
    } else this.activation.noteUnattributedMcp(tool);
    this.state.setActivationLatency(this.activation.snapshot());
    this.state.setUnattributedMcp(this.activation.unattributedSnapshot());
  }

  /** Public so tests and the discovery timer can reconcile live subscriptions. */
  async reconcileDiscovery(): Promise<void> { await this.runDiscovery(); }
  subscribedTokenIds(): string[] { return [...this.tokenIds]; }
  runningReferenceAssets(): string[] { return this.referenceRegistry.runningAssets(); }
  entryQualifiedIds(): string[] { return [...this.entryQualified]; }
  monitoredMarketIds(): string[] { return [...this.marketsById.keys()]; }
  telemetryStats() { return this.writer.stats(); }
  snapshotBroadcastCount() { return this.sse.snapshotBroadcastCount(); }
  experimentManifest() { return this.experiment; }
  activationFootprint() { return this.activation.footprint(); }
  hotResearchCacheForTest() { return this.hotResearchCache.snapshot(); }
  hotResearchFootprintForTest() { return this.hotResearchCache.footprint(); }
  /** Bounded Alpha-only MCP context. Fresh cache entries are derived from the
   * current actionable trigger and trusted synchronized engine state. */
  getAlphaResearchContextForMcp(triggerId: string): Record<string, unknown> | null {
    const trigger = this.activeTriggers.get(triggerId);
    if (!trigger || !this.isTriggerActionable(triggerId)) return null;
    const marketId = trigger.marketId ?? null;
    const synchronized = marketId ? this.isMarketSynchronized(marketId) : false;
    const requiredScoutRoles = rolesForTriggerKind(trigger.kind, trigger.severity).filter(isScoutRole);
    const research = marketId && synchronized
      ? this.hotResearchCache.freshFor(marketId, trigger.asset ?? null, requiredScoutRoles)
      : [];
    const snapshots = marketId && synchronized
      ? [...this.state.marketSnapshots.values()]
        .filter((item) => item.marketId === marketId)
        .slice(-2)
        .map((item) => boundedMarketSnapshot(item))
      : [];
    const reference = trigger.asset
      ? boundedReference(this.state.getReferenceContext(trigger.asset, this.nowFn().getTime(), this.riskLimits.staleFeedTimeoutMs))
      : null;
    const missingScoutRoles = requiredScoutRoles.filter((role) => !research.some((entry) => entry.role === role));
    return {
      trigger: boundedTrigger(trigger),
      market: { marketId, asset: trigger.asset ?? null, synchronized, snapshots, features: deterministicMarketFeatures(snapshots) },
      reference,
      research: research.slice(0, 3),
      requiredScoutRoles,
      missingScoutRoles,
      synchronized,
      actionable: true,
      readyForAlpha: synchronized && missingScoutRoles.length === 0,
      experiment: this.experiment ? {
        id: this.experiment.id, status: this.experiment.status, startedAt: this.experiment.startedAt,
        completedAt: this.experiment.completedAt,
      } : null,
      provenance: {
        triggerId: trigger.id,
        traceId: trigger.traceId,
        marketId: trigger.marketId ?? null,
        sourceEventIds: trigger.sourceEventIds.slice(0, 32),
        evidenceIds: (trigger.evidenceIds ?? []).slice(0, 32),
        scoutKeys: research.map((entry) => entry.key),
      },
    };
  }
  grokRuntimeMetricsForTest() { return this.activation.runtimeMetricsSnapshot(); }
  restingOrderCount() { return this.restingOrders.size; }
  reservedCash() { return this.ledger.snapshot().reservedCash; }
  async tickMaintenance(): Promise<void> { await this.runMaintenance(); }
  /** Test-only ingress hook that exercises the real reference precursor path. */
  async referenceTickForTest(tick: ReferenceTick): Promise<void> { await this.onReferenceTick(tick); }
  async applyObservedFill(
    fill: Parameters<PaperLedger["applyFill"]>[0],
    intent?: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG" | "GUARDIAN_PROTECTIVE_SELL",
  ): Promise<void> { await this.applyFill(fill, { intent }); }
  async restMakerForTest(
    order: PaperOrder,
    snapshot: MarketSnapshot,
    intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" = "OPEN_FIRST_LEG",
  ): Promise<void> {
    if (intent === "OPEN_FIRST_LEG" && this.experimentStartPending) return;
    await this.reserveMaker(order, snapshot, intent);
  }
  async cancelMakerForTest(orderId: string, reason = "TEST_RELEASE"): Promise<void> {
    await this.releaseMaker(orderId, { cancel: true, reason });
  }
  setCrashInjectionForTest(point: CrashInjectionPoint, options?: { times?: number; operationKey?: string }): void {
    this.crashInjector.arm(point, options);
  }
  setRecoveryPageSizeForTest(size: number): void {
    this.recoveryPageSize = Math.max(1, size);
  }
  lastAppliedDurableCursorForTest(): number { return this.lastAppliedDurableCursor; }
  derivedAuditCursorForTest(): number { return this.derivedAuditCursor; }
  restingOrdersForTest() { return new Map(this.restingOrders); }
  restingOrderTokenIndexEntries(): number { return this.restingIndex.entryCount(); }
  assetMarketIndexEntries(): number { return this.assetIndex.entryCount(); }
  isTokenSynchronized(tokenId: string): boolean { return this.adapterTokenSynchronized(tokenId); }
  isMarketSynchronized(marketId: string): boolean {
    const discovered = this.marketsById.get(marketId)?.tokens.map((token) => token.tokenId) ?? [];
    const pair = this.snapshots.get(marketId);
    const tokens = uniqueStrings([
      ...discovered,
      pair?.UP?.tokenId,
      pair?.DOWN?.tokenId,
      ...[...this.state.marketSnapshots.values()].filter((item) => item.marketId === marketId).map((item) => item.tokenId),
    ].filter((item): item is string => Boolean(item)));
    if (tokens.length === 0) return this.marketAdapter.unsynchronizedTokenCount?.() === 0;
    return tokens.every((tokenId) => this.adapterTokenSynchronized(tokenId));
  }
  unsynchronizedTokenCount(): number {
    return typeof this.marketAdapter.unsynchronizedTokenCount === "function"
      ? this.marketAdapter.unsynchronizedTokenCount()
      : 0;
  }
  triggerDedupeEntryCount(): number { return this.triggerEngine.dedupeEntryCount(); }
  makerReservationsForTest() { return new Map(this.makerReservations); }
  settledMarketsForTest() { return new Set(this.settledMarkets); }
  settlementBackoffCountForTest(): number { return this.settlementBackoff.size; }
  async latestDurableSequenceForTest(): Promise<number> { return this.journal.latestDurableSequence(); }
  async readDurableAfterForTest(sequence: number, limit: number) {
    return this.journal.readDurableTransitionsAfter(sequence, limit);
  }
  async commitDurableForTest(
    input: Parameters<EventJournal["commitDurableTransition"]>[0],
    expectedPreviousSequence?: number,
  ) {
    return this.journal.commitDurableTransition(input, expectedPreviousSequence ?? this.lastAppliedDurableCursor);
  }
  async checkpointForTest(): Promise<void> {
    await this.checkpoint();
  }
  recoveryFailClosedForTest(): boolean {
    return this.overload.hasIntegrityFault && this.overload.state === "FAIL_CLOSED";
  }

  private async readVerifiedQuarantine(experimentId: string): Promise<QuarantineReceipt | null> {
    try {
      return await retryPersistenceTransport(async () => {
        const row = await this.journal.readDurableByOperationKey(quarantineOperationKey(experimentId));
        if (!row) return null;
        const receipt = quarantineReceiptSchema.parse({ ...quarantinePayloadSchema.parse(row.payload), sequence: row.sequence });
        const highWater = await this.journal.latestDurableSequence();
        if (row.sequence > this.lastAppliedDurableCursor || row.sequence > highWater
          || row.type !== QUARANTINE_TRANSITION || row.aggregateType !== "experiment" || row.aggregateId !== experimentId
          || receipt.experimentId !== experimentId || receipt.durableCursorBefore + 1 !== row.sequence
          || row.payloadHash !== hashDurablePayload(row.payload)
          || Date.parse(row.occurredAt) !== Date.parse(receipt.quarantinedAt)) {
          throw new RecoveryFailClosedError("invariant", "invalid durable quarantine receipt");
        }
        if (isCanonicalFinalReport((await this.journal.loadExperimentReport(experimentId, "final")) ?? { reportKind: "final", complete: false, truncated: false })) {
          throw new RecoveryFailClosedError("invariant", "canonical report conflicts with quarantine");
        }
        return receipt;
      }, this.persistenceRetryOptions("integrity"));
    } catch (error) {
      this.failClosedIntegrity(error);
      throw error;
    }
  }

  experimentReportDisposition(): Record<string, unknown> {
    return { experimentId: this.experiment?.id ?? null, status: this.experiment?.status ?? null,
      reportDisposition: this.reportQuarantine?.disposition ?? null, canonical: this.reportQuarantine ? false : this.reportGenerated,
      integrityFaultPresent: Boolean(this.reportQuarantine || (this.experiment && this.unresolvedReportSourceIntegrity(this.experiment.id).length)),
      receipt: this.reportQuarantine ? structuredClone(this.reportQuarantine) : null };
  }

  private quarantineRuntimeDiagnostics() {
    const supervisor = this.supervisor.snapshot();
    let guardianQueued = 0;
    for (const queue of this.guardianPersistenceQueues.values()) guardianQueued += queue.length;
    return {
      supervisorUnresolved: supervisor.unresolved,
      guardianQueued,
      guardianWorkers: this.guardianPersistenceWorkers.size,
      reversalProtectionInFlight: this.reversalProtectionInFlight.size,
      reversalProtectionPending: this.reversalProtectionPending.size,
      reversalProtectionSnapshotScheduled: this.reversalProtectionSnapshotScheduled.size,
      journalActive: this.journalAdmission.activeCount,
      journalWaiting: this.journalAdmission.waitingCount,
      researchIngressInFlight: this.researchIngressInFlight,
      finalReportWrites: this.finalReportWrites,
      retentionInFlight: this.retention.inFlight,
    };
  }

  private quarantineRuntimeBlocker(): string | null {
    if (this.ledger.positions.size || this.restingOrders.size || this.makerReservations.size
      || !d(this.ledger.snapshot().reservedCash).eq(0) || this.riskReductionOwners.size
      || this.pendingFirstLegMarkets.size || this.riskReductionInFlight.size
      || this.profitTrailingPairInFlight.size || this.profitTrailingPairPending.size
      || this.reversalProtectionInFlight.size || this.reversalProtectionPending.size) return "QUARANTINE_ECONOMIC_AUTHORITY";
    if (this.supervisor.snapshot().unresolved || this.guardianPersistenceUnresolved()
      || this.journalAdmission.activeCount || this.journalAdmission.waitingCount
      || this.researchIngressInFlight || this.finalReportWrites || this.retention.inFlight) return "QUARANTINE_RUNTIME_NOT_DRAINED";
    return null;
  }

  private async assertQuarantineReady(experimentId: string): Promise<ReportSourceIntegrityFault[]> {
    if (this.experiment?.id !== experimentId) throw new ExperimentQuarantineRejectedError("QUARANTINE_EXPERIMENT_MISMATCH");
    if (this.experiment.status !== "COMPLETED") throw new ExperimentQuarantineRejectedError("QUARANTINE_COMPLETED_REQUIRED");
    const blocker = this.quarantineRuntimeBlocker();
    if (blocker) throw new ExperimentQuarantineRejectedError(blocker);
    const latest = await this.journal.latestDurableSequence();
    const invariant = validatePostReplayInvariants(this.recoverableBag(), this.lastAppliedDurableCursor, latest);
    if (!invariant.ok) throw new RecoveryFailClosedError("invariant", invariant.reasons.join(","));
    if (this.env.TRADING_MODE !== "paper" || this.state.database !== "UP" || this.state.journalDegraded
      || !this.state.recoveryComplete || this.overload.hasIntegrityFault) {
      throw new ExperimentQuarantineRejectedError("QUARANTINE_DATABASE_NOT_HEALTHY");
    }
    const canonical = await this.journal.loadExperimentReport(experimentId, "final");
    if (canonical && isCanonicalFinalReport(canonical)) throw new ExperimentQuarantineRejectedError("QUARANTINE_CANONICAL_FINAL_PRESENT");
    const faults = await loadUnresolvedReportSourceIntegrity(this.journal, experimentId);
    if (!faults.length) throw new ExperimentQuarantineRejectedError("QUARANTINE_UNRESOLVED_SOURCE_REQUIRED");
    // The loader can conservatively return an overflow sentinel. Accept only
    // evidence with a physically persisted fault marker, never a RAM-only fault.
    for (const fault of faults) {
      const key = integrityDurableOperationKey("fault", experimentId, fault.eventId);
      const event = await this.journal.readJournalEventById(key);
      const durable = event ? null : await this.journal.readDurableByOperationKey(key);
      const record = event ?? durable;
      if (!record || record.type !== "REPORT_SOURCE_INTEGRITY_FAULT"
        || record.payload.experimentId !== experimentId || record.payload.eventId !== fault.eventId) {
        throw new ExperimentQuarantineRejectedError("QUARANTINE_UNRESOLVED_SOURCE_REQUIRED");
      }
    }
    const raced = this.quarantineRuntimeBlocker();
    if (raced) throw new ExperimentQuarantineRejectedError(raced);
    return faults;
  }

  async quarantineExperiment(input: { experimentId: string; requestId: string; operatorId: string }): Promise<Record<string, unknown>> {
    return this.experimentBoundaryGate.run(() => this.durableGate.run(async () => {
      try {
        if (![input.experimentId, input.requestId, input.operatorId].every(v => typeof v === "string" && v.length > 0)
          || input.experimentId.length > 120 || input.requestId.length > 80 || input.operatorId.length > 80) {
          throw new ExperimentQuarantineRejectedError("QUARANTINE_INVALID_REQUEST");
        }
        const existing = await this.readVerifiedQuarantine(input.experimentId);
        if (existing) return { ok: true, existing: true, receipt: existing };
        const faults = await retryPersistenceTransport(
          () => this.assertQuarantineReady(input.experimentId), this.persistenceRetryOptions("integrity"),
        );
        const at = this.nowFn();
        const payload = quarantinePayloadSchema.parse({
          experimentId: input.experimentId, experimentStatus: "COMPLETED", disposition: "QUARANTINED_NON_FINAL",
          quarantinedAt: at.toISOString(), operatorId: input.operatorId, requestId: input.requestId,
          reasonCode: "CANONICAL_SOURCE_INTEGRITY_UNRESOLVED", canonicalFinalReportPresent: false,
          integrityFaultCount: faults.length,
          faultReferences: faults.map(f => ({ ...f, reason: f.reason.slice(0, 512) })).sort((a, b) => a.eventId.localeCompare(b.eventId)),
          durableCursorBefore: this.lastAppliedDurableCursor, validationAuthority: "DURABLE_REPLAY_VALID",
          economicTerminality: { openPositions: 0, restingOrders: 0, reservations: 0, reservedCash: "0", riskReductionOwners: 0, pendingEconomicWork: 0 },
        });
        const result = await this.commitAuthoritative({
          operationKey: quarantineOperationKey(input.experimentId), type: QUARANTINE_TRANSITION,
          aggregateType: "experiment", aggregateId: input.experimentId, occurredAt: at,
          buildPayload: () => ({ ...payload, derivedAudit: [{
            id: quarantineOperationKey(input.experimentId), type: QUARANTINE_TRANSITION,
            aggregateType: "experiment", aggregateId: input.experimentId, traceId: input.requestId,
            correlationId: input.requestId, occurredAt: at.toISOString(), recordedAt: at.toISOString(),
            payload: { ...payload, canonical: false },
          }] }),
          // commitAuthoritative already owns transport recovery here; do not nest retries.
          beforeCommit: async () => { await this.assertQuarantineReady(input.experimentId); },
          checkpoint: true,
        });
        return { ok: true, existing: result.existing, receipt: this.reportQuarantine };
      } catch (error) {
        if (error instanceof ExperimentQuarantineRejectedError) {
          return error.blocker === "QUARANTINE_RUNTIME_NOT_DRAINED"
            ? { ok: false, blockers: [error.blocker], runtimeDiagnostics: this.quarantineRuntimeDiagnostics() }
            : { ok: false, blockers: [error.blocker] };
        }
        this.failClosedIntegrity(error);
        throw error;
      }
    }));
  }

  async beginExperiment(): Promise<Record<string, unknown>> {
    return this.experimentBoundaryGate.run(async () => {
      if (this.experiment?.status === "RUNNING" || this.experiment?.status === "ENTRY_CLOSED") {
        return { ok: false, blockers: ["EXPERIMENT_ALREADY_ACTIVE"], experiment: this.experiment };
      }
      if (this.experiment?.status === "FAILED") {
        return { ok: false, blockers: ["EXPERIMENT_ALREADY_COMPLETED"], experiment: this.experiment };
      }
      // Historical report finalization is independent of this continuous PAPER
      // runtime. Keep old records intact; economic residue/integrity below still
      // prevents a new experiment from abandoning obligations or corrupt state.
      if (this.experiment?.status === "COMPLETED" && !this.env.GUARDIAN_SEPARATE_ENABLED) {
        const previousReport = await this.journal
          .loadExperimentReport(this.experiment.id, "final")
          .catch(() => null);
        if ((!previousReport || !isCanonicalFinalReport(previousReport))
          && !(await this.readVerifiedQuarantine(this.experiment.id))) {
          return {
            ok: false,
            blockers: ["PREVIOUS_EXPERIMENT_REPORT_NOT_FINALIZED"],
            experiment: this.experiment,
          };
        }
      }
      const startBlockers = [
        ...(this.state.journalDegraded || this.overload.hasIntegrityFault ? ["JOURNAL_NOT_HEALTHY"] : []),
        ...this.experimentEconomicResidueBlockers(),
      ];
      const entryBlocked = !this.state.newPaperEntryAllowed() || this.overload.state === "FAIL_CLOSED";
      if (entryBlocked || startBlockers.length > 0) {
        const preflight = this.preflight("48H_NATIVE_PAPER_EXPERIMENT");
        return {
          ...preflight,
          ok: false,
          blockers: [...new Set([
            ...preflight.blockers,
            ...startBlockers,
            ...(entryBlocked ? ["OVERLOAD_FAIL_CLOSED"] : []),
          ])],
        };
      }
      const preflight = this.preflight("48H_NATIVE_PAPER_EXPERIMENT");
      const feedBlockers = this.experimentFeedBlockers();
      const gatedPreflight = feedBlockers.length > 0
        ? { ...preflight, ok: false, blockers: [...new Set([...preflight.blockers, ...feedBlockers])] }
        : preflight;
      await this.safeAppend(createJournalEvent({
        traceId: createId("trace"), type: "EXPERIMENT_PREFLIGHT", aggregateType: "experiment", aggregateId: "preflight",
        payload: gatedPreflight as unknown as Record<string, unknown>,
      }));
      if (!gatedPreflight.ok) return { ...gatedPreflight, ok: false };
      const now = this.nowFn();
      if (this.env.REVERSAL_PROTECTION_ENABLED && (!this.env.PROFIT_TRAILING_PAIR_SOAK_ENABLED
        || this.env.TRADING_MODE !== "paper" || this.env.REVERSAL_PROTECTION_WINDOW_MS !== 180000
        || this.env.REVERSAL_PROTECTION_OPPOSITE_COUNT !== 3 || this.env.NAKED_DIRECTIONAL_SOAK_ENABLED
        || this.env.PAPER_EXECUTION_STYLE !== "TAKER" || this.riskLimits.normalHedgeMinLockedPairEdge !== 0.12
        || this.env.PROFIT_TRAILING_PAIR_ARM_EDGE !== 0.05 || this.env.PROFIT_TRAILING_PAIR_RETRACE_EDGE !== 0.02)) throw new Error("INVALID_REVERSAL_PROTECTION_V2_CONFIG");
      const next = startExperiment({
        id: undefined, durationHours: this.env.EXPERIMENT_DURATION_HOURS, strategyVersion: this.strategy.version,
        riskConfiguration: {
          ...this.riskLimits,
          nakedDirectionalSoakEnabled: this.env.NAKED_DIRECTIONAL_SOAK_ENABLED,
          profitTrailingPairSoakEnabled: this.env.PROFIT_TRAILING_PAIR_SOAK_ENABLED,
          profitTrailingPairTargetEdge: this.riskLimits.normalHedgeMinLockedPairEdge,
          profitTrailingPairArmEdge: this.env.PROFIT_TRAILING_PAIR_ARM_EDGE,
          profitTrailingPairRetraceEdge: this.env.PROFIT_TRAILING_PAIR_RETRACE_EDGE,
          profitTrailingPairExecutionStyle: "TAKER",
          makerFirst: {enabled:this.env.PAPER_MAKER_FIRST_ENABLED,firstLegTtlMs:this.env.PAPER_MAKER_FIRST_QUOTE_TTL_MS,
            hedgeTtlMs:this.env.PAPER_MAKER_HEDGE_QUOTE_TTL_MS,fillModel:'public-trade-queue-50pct',twoSidedFreshEntries:this.env.PAPER_DUAL_SIDED_MAKER_ENABLED},
          reversalProtectionEnabled: this.env.REVERSAL_PROTECTION_ENABLED,
          guardianVersion: 2, guardianFormatGeneration: 3, guardianV2Config: { ...GUARDIAN_V2_CONFIG },
          reversalProtectionWindowMs: this.env.REVERSAL_PROTECTION_WINDOW_MS,
          reversalProtectionOppositeCount: this.env.REVERSAL_PROTECTION_OPPOSITE_COUNT,
        },
        modelConfiguration: {
          provider: "native_deterministic", version: NATIVE_DETERMINISTIC_MODEL_VERSION,
          localExecution: true, externalInference: false, authority: NATIVE_DETERMINISTIC_AUTHORITY,
        },
        startingBalance: this.ledger.snapshot().startingBalance, systemVersion: "0.1.0",
        gitSha: process.env.GIT_SHA ?? process.env.FLY_IMAGE_REF ?? "unknown", now,
        closePolicy: (this.env.NAKED_DIRECTIONAL_SOAK_ENABLED || this.env.PROFIT_TRAILING_PAIR_SOAK_ENABLED)
          ? "observe_until_settlement" : this.env.EXPERIMENT_CLOSE_POLICY,
      });
      const startFeedGeneration = this.experimentFeedGeneration;
      this.experimentStartPending = true;
      try {
        await this.commitAuthoritative({
          operationKey: `experiment:${next.id}:started`,
          type: "EXPERIMENT_STARTED",
          aggregateType: "experiment",
          aggregateId: next.id,
          occurredAt: now,
          buildPayload: () => ({
            experiment: next,
            derivedAudit: [this.experimentDerivedAudit("EXPERIMENT_STARTED", next)],
          }),
          checkpoint: false,
          beforeCommit: () => {
            if (this.experimentFeedGeneration !== startFeedGeneration || this.experimentFeedBlockers().length > 0) {
              throw new ExperimentStartGuardRejectedError();
            }
          },
          afterApply: async () => {
            this.reportGenerated = false;
            this.protectiveDeRiskAttempts.clear();
            this.protectiveDeRiskNextAttemptAt.clear();
            this.protectiveDeRiskVetoFingerprint.clear();
            this.protectiveDeRiskLastWaitingAt.clear();
            this.protectiveDeRiskLastSuppressEmitAt.clear();
            this.researchIngressGeneration += 1;
            this.clearReportSourceIntegrity(next.id);
            this.record("EXPERIMENT_STARTED", next.id, null, { status: next.status });
          },
        });
        if (this.startPostCommitValidationHoldForTest) await this.startPostCommitValidationHoldForTest;
        const postCommitBlockers = this.experimentFeedBlockers();
        if (this.experimentFeedGeneration !== startFeedGeneration || postCommitBlockers.length > 0) {
          // The durable start may already have crossed the database boundary.
          // Keep the boundary gate and start-pending fence held, then commit a
          // terminal aborted-start state so no caller can observe an
          // entry-capable experiment from invalidated truth.
          await this.commitAbortedStart(next, "EXPERIMENT_FEED_INVALIDATED_DURING_START");
          return {
            ok: false,
            blockers: [...new Set([...postCommitBlockers, "EXPERIMENT_FEED_INVALIDATED_DURING_START"])],
            experiment: this.experiment,
          };
        }
        const validated: ExperimentManifest = { ...next, startValidationPending: false };
        await this.commitAuthoritative({
          operationKey: `experiment:${next.id}:start_validated`,
          type: "EXPERIMENT_START_VALIDATED",
          aggregateType: "experiment",
          aggregateId: next.id,
          buildPayload: () => ({ experiment: validated, derivedAudit: [this.experimentDerivedAudit("EXPERIMENT_START_VALIDATED", validated)] }),
          checkpoint: false,
          beforeCommit: () => {
            if (this.experiment?.id !== next.id || this.experiment.status !== "RUNNING" || !this.experiment.startValidationPending) {
              throw new RecoveryFailClosedError("invariant", "start validation state changed before durable validation");
            }
            if (this.experimentFeedGeneration !== startFeedGeneration || this.experimentFeedBlockers().length > 0) {
              throw new ExperimentStartGuardRejectedError();
            }
          },
          afterApply: async () => this.record("EXPERIMENT_START_VALIDATED", next.id, null, { status: "RUNNING" }),
        });
        // The final feed/generation check in the validation commit's
        // beforeCommit hook is the explicit linearization point.  Once
        // START_VALIDATED is durable, later feed loss is ordinary runtime
        // feed-loss semantics; there is no compensating transition to race
        // with recovery.
        this.crashInjector.maybeCrash("AFTER_START_VALIDATED_BEFORE_RUNTIME_ADMISSION", `experiment:${next.id}:start_validated`);
        this.syncExperimentWindow();
        try {
          await this.checkpoint();
        } catch (error) {
          // START_VALIDATED is authoritative.  A checkpoint failure is an
          // explicit degradation, not a failed start with a RUNNING identity.
          this.state.journalDegraded = true;
          this.record("EXPERIMENT_START_CHECKPOINT_DEGRADED", next.id, null, {
            status: "RUNNING", message: error instanceof Error ? error.message : String(error),
          });
          return {
            ok: true,
            experiment: this.experiment,
            preflight,
            checkpoint: "degraded",
            warnings: ["CHECKPOINT_DEGRADED"],
          };
        }
      } catch (error) {
        if (!(error instanceof ExperimentStartGuardRejectedError)) throw error;
        const currentExperiment = this.experiment;
        if (currentExperiment?.id === next.id && currentExperiment.status === "RUNNING" && currentExperiment.startValidationPending) {
          const blockers = this.experimentFeedBlockers();
          await this.commitAbortedStart(next, "EXPERIMENT_FEED_INVALIDATED_DURING_START");
          return {
            ok: false,
            blockers: [...new Set([...blockers, "EXPERIMENT_FEED_INVALIDATED_DURING_START"])],
            experiment: this.experiment,
          };
        }
        const current = this.preflight("48H_NATIVE_PAPER_EXPERIMENT");
        const blockers = [...new Set([...current.blockers, ...this.experimentFeedBlockers()])];
        return { ...current, ok: false, blockers };
      } finally {
        this.experimentStartPending = false;
      }
      return { ok: true, experiment: this.experiment, preflight };
    });
  }

  async closeExperimentEntries(): Promise<Record<string, unknown>> {
    return this.experimentBoundaryGate.run(async () => {
      const current = this.experiment;
      if (!current || current.status === "IDLE" || current.status === "CREATED") {
        return { ok: false, blockers: ["NO_CURRENT_EXPERIMENT"], experiment: current };
      }
      if (current.status === "ENTRY_CLOSED" || current.status === "COMPLETED") {
        return { ok: true, experiment: current };
      }
      if (current.status !== "RUNNING") {
        return { ok: false, blockers: ["EXPERIMENT_NOT_RUNNING"], experiment: current };
      }

      const closed: ExperimentManifest = {
        ...current,
        status: "ENTRY_CLOSED",
        entryClosesAt: this.nowFn().toISOString(),
        completedAt: null,
        markToMarketAt: null,
        // Manual/operator close-entries is another RUNNING â†’ ENTRY_CLOSED
        // boundary; the durable manifest must carry the actual unresolved
        // position count, exactly like the deadline and recovery paths.
        unresolvedPositionCount: this.countUnresolvedPositions(),
      };
      await this.commitExperimentStatus(closed);
      return { ok: true, experiment: this.experiment };
    });
  }
  /** Test-only view of current entry-feed eligibility. */
  experimentFeedBlockersForTest(): string[] { return this.experimentFeedBlockers(); }

  /**
   * A real experiment may only start with at least one currently trusted
   * market candidate. Synthetic/test adapters intentionally opt out because
   * they do not model the live transport trust state.
   */
  private experimentFeedBlockers(): string[] {
    if (this.state.discoveryStatus === "RECONCILING") return ["DISCOVERY_RECONCILING"];
    if (!(this.marketAdapter instanceof PolymarketMarketDataAdapter)) return [];
    if (this.state.connections.polymarket.state !== "CONNECTED") return ["MARKET_FEED_NOT_READY"];
    const candidates = [...this.entryQualified].map((marketId) => {
      const market = this.marketsById.get(marketId);
      const asset = this.assetByMarket.get(marketId) ?? market?.asset ?? null;
      const referenceReady = !asset || this.referenceRequired.get(marketId) !== true
        || (!this.referenceUnreliable.has(asset)
          && this.state.getReferenceContext(asset, this.nowFn().getTime(), this.riskLimits.staleFeedTimeoutMs).freshness === "fresh");
      const trusted = this.isMarketSynchronized(marketId);
      const marketFresh = this.marketBooksFresh(marketId);
      return { marketId, trusted: trusted && marketFresh, referenceReady };
    });
    if (candidates.some((candidate) => candidate.trusted && candidate.referenceReady)) return [];
    if (candidates.some((candidate) => candidate.trusted)) return ["REFERENCE_FEED_STALE"];
    return ["MARKET_BOOK_NOT_TRUSTED"];
  }

  private marketBooksFresh(marketId: string): boolean {
    const market = this.marketsById.get(marketId);
    if (!market) return false;
    const pair = this.snapshots.get(marketId);
    const nowMs = this.nowFn().getTime();
    const maxAge = this.riskLimits.staleFeedTimeoutMs;
    return (['UP', 'DOWN'] as const).every((outcome) => {
      const snapshot = pair?.[outcome];
      if (!snapshot) return false;
      const receivedAt = Date.parse(snapshot.receivedAt);
      const age = nowMs - receivedAt;
      return Number.isFinite(receivedAt) && age >= 0 && age <= maxAge;
    });
  }

  preflight(mode: "MONITOR_ONLY" | "48H_GROK_PAPER_EXPERIMENT" | "48H_NATIVE_PAPER_EXPERIMENT" = "MONITOR_ONLY") {
    const staticMcpRoles = this.env.GROK_MCP_BEARER_TOKENS.split(",").map((entry) => entry.trim().split(":")[0] ?? "").filter(Boolean);
    const oauthBootstrapRoles = this.env.GROK_MCP_OAUTH_ENABLED
      ? this.env.GROK_MCP_OAUTH_BOOTSTRAP_TOKENS.split(",").map((entry) => entry.trim().split(":")[0] ?? "").filter(Boolean)
      : [];
    return evaluateExperimentPreflight({
      env: this.env, databaseUp: this.state.database === "UP", discoveryStatus: this.state.discoveryStatus,
      qualifiedOrManaged: this.marketsById.size, bootstrapFailed: this.bootstrapFailed,
      tokenConfiguredPath: this.tokenIds.length > 0,
      mcpRoleTokens: [...new Set([...staticMcpRoles, ...oauthBootstrapRoles])],
      mode,
      overloadFailClosed: this.overload.state === "FAIL_CLOSED",
      entryMarkets: [...this.entryQualified].map((marketId) => {
        const market = this.marketsById.get(marketId);
        const asset = this.assetByMarket.get(marketId) ?? market?.asset ?? null;
        const blocked = asset ? this.referenceBlocked.get(asset) ?? null : null;
        const up = market?.tokens.find((token) => token.outcome === "UP")?.tokenId ?? null;
        const down = market?.tokens.find((token) => token.outcome === "DOWN")?.tokenId ?? null;
        const monitored = Boolean(up && down && this.tokenIds.includes(up) && this.tokenIds.includes(down));
        const referenceFresh = this.marketAdapter instanceof PolymarketMarketDataAdapter
          ? !this.referenceRequired.get(marketId) || this.state.getReferenceContext(asset ?? "", this.nowFn().getTime(), this.riskLimits.staleFeedTimeoutMs).freshness === "fresh"
          : undefined;
        return {
          marketId, asset, referenceRequired: this.referenceRequired.get(marketId) === true, blockedStatus: blocked,
          yesTokenId: up, noTokenId: down, endDate: market?.endDate ?? null,
          feeScheduleKnown: market?.feeSchedule != null,
          binarySafe: market?.qualification.binary.safe === true
            && market.qualification.binary.yesOutcome !== market.qualification.binary.noOutcome,
          monitored, referenceFresh,
        };
      }).filter((entry) => isGenuineEntryQualifiedMarket(entry) || this.entryQualified.has(entry.marketId)),
      reference: [...this.referenceRequired.keys()].map((marketId) => {
        const asset = this.assetByMarket.get(marketId);
        return { asset: asset ?? "unknown", status: this.referenceBlocked.get(asset ?? "") ?? (this.referenceRequired.get(marketId) ? "REQUIRED" : "OPTIONAL") };
      }),
    });
  }

  private startMarketLoop(): void {
    if (this.marketLoopStarted) return;
    this.marketLoopStarted = true;
    void this.marketAdapter.run(this.tokenIds, {
      onSnapshot: (snapshot) => this.onMarketSnapshot(snapshot),
      ...(this.env.PAPER_MAKER_FIRST_ENABLED ? {onTrade:(trade:MarketTrade)=>this.onMakerTrade(trade)}:{}),
      onMinOrderSizeHydrated: (event) => this.onMinOrderSizeHydrated(event),
      onConnection: (event) => this.onConnection(event),
      onShardConnection: event => this.persistExperimentWsEvent({ eventId: event.eventId, kind: event.state,
        shard: event.shard, retry: event.reconnecting ?? event.reconnectCount > 0, reason: marketWsReason(event.message), at: event.at }),
      onError: (error) => this.onFeedError("polymarket", error),
      onAnomaly: (anomaly) => this.onPolymarketAnomaly(anomaly),
      onGap: (event) => {
        this.noteSoakAnomaly(event.reason === "count_limit" ? "SLOW_CONSUMER" : "INGRESS_GAP", { ...event });
        this.noteExperimentFeedInvalidation();
        this.invalidateMarketProjections(event.tokenIds);
        this.metrics.noteIngressGap(event.reason);
        this.metrics.marketUnsynchronizedTokens.set(this.unsynchronizedTokenCount());
        this.syncIngressMetrics();
      },
      onResync: (event) => {
        this.metrics.noteIngressResync(event.reason);
        this.metrics.marketUnsynchronizedTokens.set(this.unsynchronizedTokenCount());
      },
    }, this.abort.signal).catch((error) => this.logger.error({ err: error }, "polymarket market loop stopped"));
  }

  private async runDiscovery(): Promise<void> {
    await this.discoveryFlight.trigger(async (generation) => {
      await this.refreshDiscovery(generation);
    });
  }

  private async runMaintenance(): Promise<void> {
    await this.maintenanceFlight.trigger(async () => {
      if (this.maintenanceHold) await this.maintenanceHold;
      // Hot-path gauges are O(1) snapshots of bounded state; publish them on the
      // existing maintenance cadence instead of mutating prom-client on every
      // market tick.
      this.metrics.triggerDedupeEntries.set(this.triggerEngine.dedupeEntryCount());
      this.metrics.assetMarketIndexEntries.set(this.assetIndex.entryCount());
      await this.maintainExperiment();
      const retentionResult = await this.retention.runCycle({
        experiment: this.experiment,
        now: this.nowFn(),
        degraded: this.state.journalDegraded || this.overload.state === "DEGRADED" || this.state.telemetryDegraded,
        failClosed: this.overload.state === "FAIL_CLOSED",
        databaseUp: this.state.database === "UP",
        hasEconomicObligations: [...this.ledger.positions.values()].some(p => d(p.upQuantity).gt(0) || d(p.downQuantity).gt(0))
          || this.restingOrders.size > 0 || this.riskReductionOwners.size > 0,
      });
      if (retentionResult.failures) this.logger.error({ reason: retentionResult.skipReason }, "retention deferred");
    });
  }

  private async refreshDiscovery(generation = this.discoveryFlight.currentGeneration): Promise<void> {
    this.metrics.discoveryRefreshAttempts.inc();
    const started = performance.now();
    const previousStatus = this.state.discoveryStatus;
    try {
    this.state.setDiscoveryStatus("RECONCILING", this.marketsById.size);
    const report = await this.marketAdapter.discoverCryptoMarkets(
      splitList(this.env.POLYMARKET_MARKET_SLUGS),
      this.env.MAX_QUALIFIED_MARKETS,
      this.qualificationConfig,
    );
    if (generation < this.discoveryGenerationApplied) return;
    const previousMarketIds = new Set(this.marketsById.keys());
    const previousAssets = new Set([...this.assetByMarket.values()].filter((asset): asset is string => Boolean(asset)));
    const previousTokens = [...this.tokenIds];
    let obligations = this.obligationMarketIds();
    const nextEntryQualified = new Set(report.qualified.map((market) => market.id));

    const nextMarkets = new Map<string, DiscoveredMarket>();
    for (const market of report.qualified) nextMarkets.set(market.id, market);
    const openPositionMarkets = this.openPositionMarketIds();
    for (const id of obligations) {
      const fresh = report.discovered.find((item) => item.id === id) ?? report.qualified.find((item) => item.id === id);
      const previous = this.marketsById.get(id);
      if (fresh) nextMarkets.set(id, fresh);
      // Keep vanished markets only while an open position still needs settlement.
      else if (previous && openPositionMarkets.has(id)) nextMarkets.set(id, previous);
    }

    const vanishedMakerMarkets = this.vanishedRestingMarketIds(nextMarkets);
    await this.cancelVanishedMakers(vanishedMakerMarkets);

    // Cancellation yields to fills and maker admissions. Reconcile CURRENT obligations at the
    // destructive boundary, with no await between this read and map replacement.
    obligations = this.obligationMarketIds();
    for (const id of nextMarkets.keys()) {
      if (!nextEntryQualified.has(id) && !obligations.has(id)) nextMarkets.delete(id);
    }
    for (const id of obligations) {
      const fresh = report.discovered.find((item) => item.id === id)
        ?? report.qualified.find((item) => item.id === id);
      const previous = this.marketsById.get(id);
      if (fresh) nextMarkets.set(id, fresh);
      // Every current obligation needs context, including makers admitted after
      // cancellation snapshotted its orders. Unobligated history is excluded.
      else if (previous) nextMarkets.set(id, previous);
    }

    this.entryQualified.clear();
    for (const id of nextEntryQualified) this.entryQualified.add(id);
    this.marketsById.clear();
    this.assetByMarket.clear();
    this.assetIndex.clear();
    this.managedMarketIds.clear();
    for (const [id, market] of nextMarkets) {
      this.marketsById.set(id, market);
      this.assetByMarket.set(id, market.asset);
      if (market.asset) this.assetIndex.add(market.asset, id);
      if (obligations.has(id) || this.entryQualified.has(id)) this.managedMarketIds.add(id);
      if (market.endDate) this.triggerEngine.noteMarketWindow(id, market.asset, market.endDate);
    }
    this.metrics.assetMarketIndexEntries.set(this.assetIndex.entryCount());
    const nextTokens = uniqueStrings([...this.marketsById.values()].flatMap((market) => market.tokens.map((token) => token.tokenId)));
    const addedMarkets = [...this.marketsById.keys()].filter((id) => !previousMarketIds.has(id));
    const removedMarkets = [...previousMarketIds].filter((id) => !this.marketsById.has(id));
    const unchangedMarkets = [...this.marketsById.keys()].filter((id) => previousMarketIds.has(id));
    const nextAssets = new Set([...this.assetByMarket.values()].filter((asset): asset is string => Boolean(asset)));
    const addedAssets = [...nextAssets].filter((asset) => !previousAssets.has(asset));
    const removedAssets = [...previousAssets].filter((asset) => !nextAssets.has(asset));

    this.tokenIds = nextTokens;
    this.tokenIdSet = new Set(nextTokens);
    this.marketAdapter.setTokenIds(nextTokens);
    this.noteExperimentFeedInvalidation();
    this.dropRemovedMarketState(removedMarkets, removedAssets);
    this.state.setDiscovery({
      qualified: report.qualified.map((market) => ({ marketId: market.id, slug: market.slug, asset: market.asset, tokens: market.tokens.map((token) => token.tokenId) })),
      rejected: report.rejected.slice(0, 100),
    });
    this.state.setDiscoveryStatus(nextTokens.length > 0 ? "MONITORING" : "ZERO_QUALIFIED", this.entryQualified.size);
    this.updateReady();

    await this.safeAppend(createJournalEvent({
      traceId: createId("trace"), type: "DISCOVERY_RECONCILED", aggregateType: "engine", aggregateId: "market-discovery",
      payload: {
        discovered: report.discovered.length, qualified: report.qualified.length,
        managed: [...this.managedMarketIds], entryQualified: [...this.entryQualified],
        addedMarkets, removedMarkets, unchangedMarkets, addedAssets, removedAssets,
        previousTokens, nextTokens,
        markets: [...this.marketsById.values()].map((market) => ({ id: market.id, slug: market.slug, asset: market.asset, tokens: market.tokens })),
        rejected: report.rejected.slice(0, 50),
      },
    }));
    this.record("DISCOVERY_RECONCILED", createId("trace"), null, {
      qualified: report.qualified.length, managed: this.managedMarketIds.size, addedMarkets, removedMarkets, addedAssets, removedAssets,
    });
    if (!this.legacyBtcAdapter) await this.reconcileReferenceFeeds();
    this.metrics.discoveryMarketsInspected.observe(report.discovered.length);
    this.metrics.discoveryQualified.observe(report.qualified.length);
    this.discoveryGenerationApplied = generation;
    } catch (error) {
      this.state.setDiscoveryStatus(previousStatus, this.entryQualified.size);
      throw error;
    } finally {
      this.metrics.discoveryRefreshDuration.observe((performance.now() - started) / 1000);
    }
  }

  private openPositionMarketIds(): Set<string> {
    const ids = new Set<string>();
    for (const position of this.ledger.positions.values()) {
      if (d(position.upQuantity).gt(0) || d(position.downQuantity).gt(0)) ids.add(position.marketId);
    }
    return ids;
  }

  private obligationMarketIds(): Set<string> {
    const ids = this.openPositionMarketIds();
    for (const resting of this.restingOrders.values()) {
      if (d(resting.remaining).gt(0)) ids.add(resting.order.marketId);
    }
    return ids;
  }

  private restingOrderIntent(resting: RestingPaperOrder): "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" {
    if (resting.order.makerFirst?.dualGroupId) {
      const position=this.ledger.positions.get(resting.order.marketId);
      if(position?.unpairedOutcome && resting.order.outcome!==position.unpairedOutcome)
        return "HEDGE_SECOND_LEG";
      return "OPEN_FIRST_LEG";
    }
    if (resting.intent) return resting.intent;
    // Pre-PR1 checkpoints did not embed intent in RestingPaperOrder. Infer only
    // enough to fail safe: an opposite-side order against current unpaired
    // inventory is a hedge; every other unknown order is treated as first-leg.
    const position = this.ledger.positions.get(resting.order.marketId);
    if (
      position?.unpairedOutcome
      && resting.order.outcome !== position.unpairedOutcome
    ) return "HEDGE_SECOND_LEG";
    return "OPEN_FIRST_LEG";
  }
/**
   * Resolve the CURRENT canonical tokenId for (marketId, outcome) from market
   * metadata (authoritative when present) and the live snapshot pair (current).
   * Returns null when the identity cannot be resolved â€” callers fail closed.
   */
  private currentTokenIdFor(marketId: string, outcome: Outcome): string | null {
    const market = this.marketsById.get(marketId);
    const canonical = market?.tokens?.find((token) => token.outcome === outcome)?.tokenId;
    if (canonical) return canonical;
    const snapshot = this.snapshots.get(marketId)?.[outcome];
    return snapshot?.tokenId ?? null;
  }

  private hasOutstandingFirstLegMaker(marketId: string): boolean {
    for (const resting of this.restingOrders.values()) {
      if (
        resting.order.marketId === marketId
        && d(resting.remaining).gt(0)
        && this.restingOrderIntent(resting) === "OPEN_FIRST_LEG"
      ) return true;
    }
    return false;
  }

  private firstLegLifecycleBlocked(marketId: string, ignoreOwnPending = false): boolean {
    return this.settledMarkets.has(marketId)
      || this.ledger.positions.has(marketId)
      || this.hasOutstandingFirstLegMaker(marketId)
      || (!ignoreOwnPending && this.pendingFirstLegMarkets.has(marketId));
  }

  private firstLegAdmissionReason(order: Pick<PaperOrder, "marketId" | "tokenId" | "outcome">, expected?: MarketSnapshot): string | null {
    return officialMarketStartReason(this.marketsById.get(order.marketId), this.nowFn().getTime()) ?? firstLegBookReason({ ...order,
      canonicalTokenId: this.currentTokenIdFor(order.marketId, order.outcome),
      synchronized: this.adapterTokenSynchronized(order.tokenId),
      book: this.state.marketSnapshots.get(order.tokenId), expected,
      nowMs: this.nowFn().getTime(), maxAgeMs: this.riskLimits.staleFeedTimeoutMs });
  }

  /** Validate a quote at execution time, before any persistence await. A later
   * feed update cannot revoke that historical observation. Economic ownership,
   * cash, cancellation and settlement are still fenced at commit time. */
  private firstLegExecutionReason(order: PaperOrder, book: MarketSnapshot, at: Date): string | null {
    if(order.makerFirst?.dualGroupId){
      return officialMarketStartReason(this.marketsById.get(order.marketId),at.getTime())
        ??(!this.firstLegExpiryAdmission(order.marketId,at).eligible?'DURATION_OUT_OF_RANGE':null)
        ??firstLegBookReason({...order,book,canonicalTokenId:this.currentTokenIdFor(order.marketId,order.outcome),
          synchronized:this.adapterTokenSynchronized(order.tokenId),nowMs:at.getTime(),maxAgeMs:this.riskLimits.staleFeedTimeoutMs});
    }
    return officialMarketStartReason(this.marketsById.get(order.marketId), at.getTime())
      ?? (!this.firstLegExpiryAdmission(order.marketId, at).eligible ? "DURATION_OUT_OF_RANGE" : null)
      ?? firstLegBookReason({ ...order, book,
        canonicalTokenId: this.currentTokenIdFor(order.marketId, order.outcome),
        synchronized: this.adapterTokenSynchronized(order.tokenId),
        nowMs: at.getTime(), maxAgeMs: this.riskLimits.staleFeedTimeoutMs })
      ?? firstLegExecutionEconomics(order, book, at.getTime()).reason;
  }

  private async rejectFirstLegOrder(order: Pick<PaperOrder, "id" | "marketId" | "traceId">, reason: string, stage: string): Promise<void> {
    const id = derivedAuditEventId("PAPER_ORDER_TERMINAL", order.id);
    if (await this.journal.readJournalEventById(id)) return;
    await this.persistReportInput(createJournalEvent({ id, traceId: order.traceId,
      type: "PAPER_ORDER_TERMINAL", aggregateType: "paper_order", aggregateId: order.id,
      payload: { orderId: order.id, marketId: order.marketId, intent: "OPEN_FIRST_LEG",
        status: "REJECTED", reason, stage, terminalAt: this.nowFn().toISOString() } }));
  }

  private async recoverInterruptedFirstLegOrders(): Promise<void> {
    // Recovery repairs committed fills (including their atomic terminal audit)
    // before this scan. An uncommitted TAKER quote is never executed on restart.
    // Versioning leaves completed historical experiments byte-for-byte intact.
    if (!this.experiment || !["RUNNING", "ENTRY_CLOSED"].includes(this.experiment.status)) return;
    let after: { occurredAt: string; id: string } | undefined;
    for (;;) {
      const page = await this.journal.queryEventsKeyset({ types: ["PAPER_ORDER_CREATED"], experimentId: this.experiment.id, after, limit: 200 });
      for (const row of page) {
        const p = row.payload;
        if (p.executionLifecycleVersion !== 2 || p.intent !== "OPEN_FIRST_LEG"
          || (p.orderType !== "TAKER" && !(p.orderType==='MAKER'&&p.makerFirst))) continue;
        const id = typeof p.id === "string" ? p.id : row.aggregateId;
        if (await this.journal.readJournalEventById(derivedAuditEventId("PAPER_ORDER_TERMINAL", id))) continue;
        if(p.orderType==='MAKER'&&this.restingOrders.has(id))continue;
        if (p.orderType==='TAKER' && await this.journal.readDurableByOperationKey(`fill:first-leg-fill:${id}`))
          throw new RecoveryFailClosedError("invariant", `missing terminal audit for committed first leg ${id}`);
        await this.rejectFirstLegOrder({ id, marketId: String(p.marketId), traceId: row.traceId }, p.orderType==='MAKER'?'PROCESS_RESTART_BEFORE_MAKER_RESERVATION':"PROCESS_RESTART_BEFORE_DURABLE_FILL", "RECOVERY");
      }
      if (page.length < 200) break;
      const last = page[page.length - 1]!; after = { occurredAt: last.occurredAt, id: last.id };
    }
  }

  private firstLegExpiryAdmission(marketId: string, now = this.nowFn()): {
    eligible: boolean;
    endDate: string | null;
    remainingDurationMs: number | null;
    minRemainingDurationMs: number;
  } {
    const endDate = this.marketsById.get(marketId)?.endDate ?? null;
    const marketEndMs = endDate ? Date.parse(endDate) : Number.NaN;
    const remainingDurationMs = Number.isFinite(marketEndMs) ? marketEndMs - now.getTime() : null;
    const minRemainingDurationMs = this.qualificationConfig.minRemainingDurationMs;
    return {
      eligible: remainingDurationMs !== null && remainingDurationMs >= minRemainingDurationMs,
      endDate,
      remainingDurationMs,
      minRemainingDurationMs,
    };
  }

  private tryReserveFirstLegAdmission(marketId: string): boolean {
    if (this.firstLegLifecycleBlocked(marketId)) return false;
    this.pendingFirstLegMarkets.add(marketId);
    return true;
  }

  private async cancelOutstandingFirstLegMakers(marketId: string, reason: string): Promise<void> {
    for (const [orderId, resting] of [...this.restingOrders.entries()]) {
      if (
        resting.order.marketId !== marketId
        || !d(resting.remaining).gt(0)
        || this.restingOrderIntent(resting) !== "OPEN_FIRST_LEG"
      ) continue;
      await this.releaseMaker(orderId, { cancel: true, reason });
    }
  }

  /**
   * Fence resting HEDGE_SECOND_LEG makers at the rescue boundary. A hedge maker
   * admitted BEFORE rescue ownership could fill behind the rescue action and
   * re-consume the same residual; cancelling it is itself risk-reducing. This
   * is proactive serialization, not a replacement for the final fill-time
   * residual/token guards (defense-in-depth stays).
   */
  private async cancelRestingHedgeMakers(marketId: string, reason: string): Promise<void> {
    for (const [orderId, resting] of [...this.restingOrders.entries()]) {
      if (
        resting.order.marketId !== marketId
        || !d(resting.remaining).gt(0)
        || this.restingOrderIntent(resting) !== "HEDGE_SECOND_LEG"
      ) continue;
      await this.releaseMaker(orderId, { cancel: true, reason });
    }
  }

  private experimentEconomicResidueBlockers(): string[] {
    const ledger = this.ledger.snapshot();
    const openPositions = [...this.ledger.positions.values()].filter((position) => d(position.upQuantity).gt(0) || d(position.downQuantity).gt(0));
    const restingOrders = [...this.restingOrders.values()].filter((order) => d(order.remaining).gt(0));
    const makerReservations = [...this.makerReservations.entries()].filter(([, amount]) => d(amount).gt(0));
    const blockers: string[] = [];
    if (openPositions.length > 0 || this.ledger.positions.size > 0) blockers.push("OPEN_POSITIONS_REMAIN");
    if (restingOrders.length > 0 || this.restingOrders.size > 0) blockers.push("RESTING_ORDERS_REMAIN");
    if (makerReservations.length > 0 || this.makerReservations.size > 0) blockers.push("MAKER_RESERVATIONS_REMAIN");
    if (!d(ledger.reservedCash).eq(0)) blockers.push("RESERVED_CASH_REMAIN");
    if (this.obligationMarketIds().size > 0) blockers.push("UNRESOLVED_ECONOMIC_OBLIGATIONS");
    return blockers;
  }

  private vanishedRestingMarketIds(nextMarkets: ReadonlyMap<string, DiscoveredMarket>): string[] {
    const vanished = new Set<string>();
    for (const resting of this.restingOrders.values()) {
      if (!d(resting.remaining).gt(0)) continue;
      const marketId = resting.order.marketId;
      if (!nextMarkets.has(marketId)) vanished.add(marketId);
    }
    return [...vanished];
  }

  private async cancelVanishedMakers(vanishedMarketIds: readonly string[]): Promise<void> {
    if (vanishedMarketIds.length === 0) return;
    const vanished = new Set(vanishedMarketIds);
    for (const [orderId, resting] of [...this.restingOrders.entries()]) {
      if (!vanished.has(resting.order.marketId)) continue;
      await this.releaseMaker(orderId, { cancel: true, reason: "MARKET_REMOVED" });
    }
  }

  private dropRemovedMarketState(removedMarkets: readonly string[], removedAssets: readonly string[]): void {
    for (const marketId of removedMarkets) {
      // POSITION_SETTLED is durable authority. The in-memory marker only
      // suppresses duplicate polling while the market remains in the live
      // obligation set; once discovery removes it, drop the marker so
      // historical turnover cannot accumulate in checkpoints/RAM.
      this.settledMarkets.delete(marketId);
      this.settlementBackoff.delete(marketId);
      this.earlyCatastrophicNextAttemptAt.delete(marketId);
      this.earlyCatastrophicVetoFingerprint.delete(marketId);
      this.snapshots.delete(marketId);
      this.triggerEngine.dropMarket(marketId);
      this.assetIndex.removeMarket(marketId);
      for (const [tokenId, snapshot] of [...this.state.marketSnapshots.entries()]) {
        if (snapshot.marketId === marketId) {
          this.state.marketSnapshots.delete(tokenId);
          this.snapshotEpoch.delete(tokenId);
        }
      }
    }
    for (const asset of removedAssets) {
      this.triggerEngine.dropAsset(asset);
      this.assetIndex.dropAsset(asset);
      this.referenceUnreliable.delete(asset);
      this.referenceEpoch.delete(asset);
      this.referenceLogicalGapped.delete(asset);
      this.referenceResyncRequested.delete(asset);
      this.referenceResyncInFlight.delete(asset);
      this.state.dropReference(asset);
      this.clearReferenceRetry(asset);
    }
    this.metrics.assetMarketIndexEntries.set(this.assetIndex.entryCount());
    for (const [triggerId, trigger] of [...this.activeTriggers]) {
      if (!trigger.marketId || !removedMarkets.includes(trigger.marketId)) continue;
      this.forgetTrigger(triggerId);
    }
  }

  private async reconcileReferenceFeeds(): Promise<void> {
    const needed = new Set([...this.assetByMarket.values()].filter((asset): asset is string => Boolean(asset)));
    for (const asset of this.referenceRegistry.runningAssets()) {
      if (needed.has(asset)) continue;
      this.referenceRegistry.stop(asset);
      this.state.dropReference(asset);
      this.clearReferenceRetry(asset);
      await this.safeAppend(createJournalEvent({
        traceId: createId("trace"), type: "REFERENCE_FEED_STOPPED", aggregateType: "reference", aggregateId: asset,
        payload: { asset, reason: "NO_ACTIVE_MARKET" },
      }));
      this.record("REFERENCE_FEED_STOPPED", createId("trace"), null, { asset }, asset);
    }
    for (const asset of needed) await this.reconcileReferenceFeed(asset);
  }

  private async reconcileReferenceFeed(asset: string): Promise<void> {
    if (![...this.assetByMarket.values()].includes(asset)) {
      this.clearReferenceRetry(asset);
      return;
    }
    const markets = [...this.assetByMarket.entries()].filter(([, value]) => value === asset).map(([marketId]) => marketId);
    const resolution = await this.referenceRegistry.resolve(asset);
    if (resolution.status === "UNSUPPORTED_DEFINITIVE") {
      for (const marketId of markets) this.referenceRequired.set(marketId, false);
      this.referenceBlocked.delete(asset);
      this.clearReferenceRetry(asset);
      await this.safeAppend(createJournalEvent({
        traceId: createId("trace"), type: "REFERENCE_UNSUPPORTED", aggregateType: "reference", aggregateId: asset,
        payload: { asset, reason: resolution.reason, status: resolution.status, detail: resolution.detail ?? null, choice: "POLYMARKET_ONLY_MONITOR" },
      }));
      this.record("REFERENCE_UNSUPPORTED", createId("trace"), null, { asset, choice: "POLYMARKET_ONLY_MONITOR" }, asset);
      return;
    }
    if (resolution.status === "PROVIDER_BLOCKED" || resolution.status === "REGION_BLOCKED") {
      for (const marketId of markets) this.referenceRequired.set(marketId, true);
      this.referenceBlocked.set(asset, resolution.status);
      await this.safeAppend(createJournalEvent({
        traceId: createId("trace"), type: "REFERENCE_PROVIDER_BLOCKED", aggregateType: "reference", aggregateId: asset,
        payload: { asset, status: resolution.status, detail: resolution.detail ?? null, retryAfterMs: resolution.retryAfterMs ?? 1_800_000 },
      }));
      this.record("REFERENCE_PROVIDER_BLOCKED", createId("trace"), null, { asset, status: resolution.status, detail: resolution.detail ?? null }, asset);
      this.scheduleReferenceRetry(asset, resolution.retryAfterMs ?? 1_800_000, 1_800_000);
      return;
    }
    if (resolution.status === "TRANSIENT_FAILURE") {
      for (const marketId of markets) this.referenceRequired.set(marketId, true);
      this.referenceBlocked.delete(asset);
      await this.safeAppend(createJournalEvent({
        traceId: createId("trace"), type: "REFERENCE_PROBE_TRANSIENT", aggregateType: "reference", aggregateId: asset,
        payload: { asset, reason: resolution.reason, status: resolution.status, detail: resolution.detail ?? null, retryAfterMs: resolution.retryAfterMs ?? null },
      }));
      this.record("REFERENCE_PROBE_TRANSIENT", createId("trace"), null, { asset, detail: resolution.detail ?? null }, asset);
      this.scheduleReferenceRetry(asset, resolution.retryAfterMs, 60_000);
      return;
    }
    this.clearReferenceRetry(asset);
    this.referenceBlocked.delete(asset);
    for (const marketId of markets) this.referenceRequired.set(marketId, true);
    const outcome = await this.referenceRegistry.ensureRunning(asset, {
      onTick: (tick) => this.onReferenceTick(tick),
      onConnection: (event) => this.onConnection(event),
      onError: (error) => this.onFeedError(`reference:${asset}`, error),
    }, this.abort.signal).catch((error) => {
      this.logger.error({ err: error, asset }, "reference feed failed to start");
      return "transient" as const;
    });
    if (outcome === "started" || outcome === "already") {
      if (outcome === "started") {
        await this.safeAppend(createJournalEvent({
          traceId: createId("trace"), type: "REFERENCE_FEED_STARTED", aggregateType: "reference", aggregateId: asset,
          payload: { asset, symbol: resolution.symbol, sharedByMarkets: markets },
        }));
        this.record("REFERENCE_FEED_STARTED", createId("trace"), null, { asset, markets }, asset);
      }
      if (this.referenceUnreliable.has(asset) && this.assetStillNeeded(asset)) {
        this.referenceResyncRequested.add(asset);
      } else if (!this.assetStillNeeded(asset)) {
        this.referenceResyncRequested.delete(asset);
        try { this.referenceRegistry.stop(asset); } catch { /* scripted */ }
      }
      return;
    }
    this.referenceResyncRequested.delete(asset);
    if (outcome === "unsupported") {
      return;
    }
    if ((outcome === "transient" || outcome === "stopped") && this.shouldRetryReferenceStart(asset)) {
      this.scheduleReferenceRetry(asset, 50, 60_000);
    }
  }

  private assetStillNeeded(asset: string): boolean {
    return [...this.assetByMarket.values()].includes(asset);
  }

  private shouldRetryReferenceStart(asset: string): boolean {
    return this.referenceUnreliable.has(asset)
      && this.assetStillNeeded(asset)
      && !this.abort.signal.aborted
      && !this.state.draining;
  }

  private scheduleReferenceRetry(asset: string, retryAfterMs: number | undefined, maxDelayMs: number): void {
    if (this.referenceRetryTimers.has(asset)) return;
    if (this.abort.signal.aborted) return;
    if (![...this.assetByMarket.values()].includes(asset)) return;
    const delayMs = Math.max(50, Math.min(retryAfterMs ?? 30_000, maxDelayMs));
    const deadline = this.nowFn().getTime() + delayMs;
    this.referenceRetryDeadlines.set(asset, deadline);
    const timer = setTimeout(() => { void this.retryTransientReference(asset); }, delayMs);
    timer.unref();
    this.referenceRetryTimers.set(asset, timer);
  }

  private clearReferenceRetry(asset: string): void {
    const timer = this.referenceRetryTimers.get(asset);
    if (!timer) return;
    clearTimeout(timer);
    this.referenceRetryTimers.delete(asset);
    this.referenceRetryDeadlines.delete(asset);
  }

  private async retryTransientReference(asset: string): Promise<void> {
    this.referenceRetryTimers.delete(asset);
    if (this.abort.signal.aborted || this.state.draining) return;
    if (![...this.assetByMarket.values()].includes(asset)) {
      this.clearReferenceRetry(asset);
      return;
    }
    await this.reconcileReferenceFeed(asset);
  }

  referenceRetryDeadline(asset: string): number | undefined { return this.referenceRetryDeadlines.get(asset); }

  private async initializeJournalWithRetry(): Promise<void> {
    let lastError: unknown;
    for (let attempt = 0; attempt < 5; attempt += 1) {
      try {
        await this.journal.initialize();
        this.state.database = "UP";
        return;
      } catch (error) {
        lastError = error;
        this.state.database = "DOWN";
        this.logger.error({ err: error, attempt }, "database initialization failed");
        await delay(Math.min(5_000, 250 * 2 ** attempt));
      }
    }
    throw lastError instanceof Error ? lastError : new Error("DATABASE_UNAVAILABLE");
  }

  private recoverableBag(): RecoverableEngineState {
    return {
      ledger: this.ledger,
      resultAccounting: this.state.resultAccounting,
      experiment: this.experiment,
      reportQuarantine: this.reportQuarantine,
      restingOrders: this.restingOrders,
      makerReservations: this.makerReservations,
      settledMarkets: this.settledMarkets,
      settlementLookups: this.economicSettlementLookups,
      lastInfrastructureAccrualAt: this.lastInfrastructureAccrualAt,
      riskReductionOwners: this.riskReductionOwners,
      riskReductionFailures: this.riskReductionFailures,
      nakedDirectionalDiagnostics: this.nakedDirectionalDiagnostics,
      profitTrailingPairStates: this.profitTrailingPairStates,
      reversalProtectionStates: this.reversalProtectionStates,
    };
  }

  private adoptRecoveredBag(bag: RecoverableEngineState): void {
    this.state.resultAccounting = bag.resultAccounting ?? emptyResultAccounting();
    this.experiment = bag.experiment;
    this.reportQuarantine = bag.reportQuarantine ?? null;
    this.lastInfrastructureAccrualAt = bag.lastInfrastructureAccrualAt;
    this.syncRecoveredProjection();
  }

  private syncRecoveredProjection(): void {
    for (const [marketId, runtime] of this.guardianRuntimeStates) {
      const episode = this.reversalEpisode(this.ledger.positions.get(marketId));
      if (!episode || !sameReversalEpisode(runtime, episode)) {
        this.guardianRuntimeStates.delete(marketId);
        this.guardianCheckpointQueuedAt.delete(marketId);
      }
    }
    this.state.setLedger(this.ledger.snapshot());
    this.syncExperimentWindow();
    this.metrics.openPositions.set(this.ledger.positions.size);
    this.metrics.unhedgedPositions.set([...this.ledger.positions.values()].filter((item) => Number(item.unpairedQuantity) > 0).length);
    this.metrics.durableTransitionLastAppliedSequence.set(this.lastAppliedDurableCursor);
  }

  private applyCommitted(transition: DurableTransition): boolean {
    if (transition.sequence <= this.lastAppliedDurableCursor) return false;
    if (transition.sequence !== this.lastAppliedDurableCursor + 1) {
      throw new RecoveryFailClosedError(
        "cursor_fence",
        `apply gap lastApplied=${this.lastAppliedDurableCursor} incoming=${transition.sequence}`,
      );
    }
    const bag = this.recoverableBag();
    applyDurableTransition(bag, transition);
    this.adoptRecoveredBag(bag);
    this.lastAppliedDurableCursor = transition.sequence;
    this.metrics.durableTransitionLastAppliedSequence.set(this.lastAppliedDurableCursor);
    return true;
  }

  private failClosedIntegrity(error: unknown): void {
    this.overload.markIntegrityFault();
    this.state.overloadState = "FAIL_CLOSED";
    this.updateReady();
    if (!(error instanceof IdempotencyConflictError) && !(error instanceof DurableCursorBehindError) && !(error instanceof RecoveryFailClosedError)) {
      this.state.database = "DOWN";
    }
  }

  private async commitAuthoritative(input: {
    operationKey: string;
    type: string;
    aggregateType: string;
    aggregateId: string;
    buildPayload: (occurredAt: Date, existing?: DurableTransition | null) => Record<string, unknown>;
    occurredAt?: Date;
    beforeCommit?: () => void | Promise<void>;
    afterApply?: (transition: DurableTransition) => Promise<void>;
    checkpoint?: boolean;
    signal?: AbortSignal;
    deadlineAt?: number;
  }): Promise<DurableCommitResult> {
    input = { ...input, occurredAt: input.occurredAt ? new Date(input.occurredAt.getTime()) : undefined };
    const requestedAt = performance.now();
    return this.durableGate.run(async () => {
      const gateWaitMs = performance.now() - requestedAt;
      let resolveMs = 0, postgresMs = 0;
      let receiptAt: string | null = null;
      let appliedAt: string | null = null;
      const occurredAt = input.occurredAt ?? this.nowFn();
      this.crashInjector.maybeCrash("BEFORE_DURABLE_COMMIT", input.operationKey);
      let commit: DurableCommitResult;
      try {
        // Build once before the first lookup. Both attempts retain the same
        // revision, audit records and authoritative business plan.
        // A queued duplicate SELL may have already closed inventory. Resolve
        // its identity inside this gate before attempting a NEW-fill projection.
        const existingFill = input.type === "PAPER_FILL" ? await this.journal.readDurableByOperationKey(input.operationKey) : undefined;
        const built = input.buildPayload(occurredAt, existingFill);
        const recordedAt = new Date().toISOString();
        // Complete current identities once, before either durable retry attempt.
        if (Array.isArray(built.derivedAudit)) built.derivedAudit = (built.derivedAudit as DerivedAuditRecord[]).map(r => ({
          ...r, identityGeneration: 1, occurredAt: r.occurredAt ?? occurredAt.toISOString(),
          recordedAt: r.recordedAt ?? recordedAt, sourceEventId: r.sourceEventId ?? null,
          correlationId: r.correlationId ?? r.traceId,
        }));
        const computed = structuredClone(built);
        let payload: Record<string, unknown> | undefined;
        const expectedPreviousSequence = this.lastAppliedDurableCursor;
        commit = await retryPersistenceTransport(async () => {
          const resolvingAt = performance.now();
          payload ??= await this.resolveDurablePayload(input, occurredAt, computed);
          resolveMs += performance.now() - resolvingAt;
          const postgresAt = performance.now();
          try { return await this.journal.commitDurableTransition({
            operationKey: input.operationKey,
            type: input.type,
            aggregateType: input.aggregateType,
            aggregateId: input.aggregateId,
            payload,
            occurredAt,
          }, expectedPreviousSequence, {
            signal: input.signal, deadlineAt: input.deadlineAt,
            beforeInsert: input.beforeCommit,
          }); } finally { postgresMs += performance.now() - postgresAt; }
        }, { ...this.persistenceRetryOptions("durable"), signal: input.signal, deadlineAt: input.deadlineAt });
        this.metrics.noteDurableCommit(input.type, commit.inserted);
        receiptAt = new Date().toISOString();
      } catch (error) {
        if (error instanceof CrashInjectedError) throw error;
        if (error instanceof ExperimentStartGuardRejectedError) throw error;
        if (error instanceof ExperimentQuarantineRejectedError) throw error;
        if (error instanceof FirstLegLifecycleGuardRejectedError) throw error;
        if (error instanceof GuardianPersistenceGuardRejectedError) throw error;
        if (error instanceof SettlementObligationChangedError) throw error;
        // A stale/raced hedge-vs-protective fill is an expected lifecycle
        // rejection, not durability corruption. It must rethrow as a normal
        // STATE_CHANGED rejection without latching the integrity fault; only
        // genuine durable/idempotency/cursor faults fail closed below.
        if (error instanceof RiskReductionLifecycleGuardRejectedError) throw error;
        // A NEW TAKER BUY that fails the serialized cash fence is the same
        // class of expected concurrent economic state-change rejection. It
        // must pass through without latching an integrity fault, setting the
        // database DOWN, or being classified as durable corruption.
        if (error instanceof TakerCashCommitGuardRejectedError) throw error;
        // A NEW maker cash reservation that fails the serialized cash fence
        // is the same class of expected concurrent economic state-change
        // rejection as the TAKER fence: no PAPER_MAKER_RESERVED was written,
        // no cash was reserved, and no resting order state was created.
        if (error instanceof MakerReservationCashCommitGuardRejectedError) throw error;
        if (error instanceof DurableCursorBehindError) {
          commit = await this.catchUpOrRejectStalePlan(input.operationKey, input.type, error);
        } else if (error instanceof IdempotencyConflictError) {
          this.metrics.noteDurableConflict(input.type);
          this.failClosedIntegrity(error);
          throw error;
        } else {
          this.metrics.noteDurableCommitFailure(input.type);
          this.failClosedIntegrity(error);
          throw error;
        }
      }
      this.crashInjector.maybeCrash("AFTER_DURABLE_COMMIT_BEFORE_APPLY", input.operationKey);
      const auditAt = performance.now();
      let applied: boolean;
      try {
        if (commit.transition.sequence > this.lastAppliedDurableCursor + 1) {
          await this.replayDurableTail(this.lastAppliedDurableCursor);
        }
        applied = this.applyCommitted(commit.transition);
        appliedAt = new Date().toISOString();
      } catch (error) {
        if (!(error instanceof CrashInjectedError)) this.failClosedIntegrity(error);
        throw error;
      }
      this.syncRestingIndexFromOrders();
      this.crashInjector.maybeCrash("AFTER_APPLY_BEFORE_DERIVED_AUDIT", input.operationKey);
      // Compacted replay identities intentionally carry no payload/derived
      // audit. The hash was already proven by resolveDurablePayload and this
      // existing sequence cannot be applied again.
      if (Object.keys(commit.transition.payload).length > 0) {
        await this.emitStoredDerivedAudit(commit.transition);
      }
      if (applied && input.afterApply) await input.afterApply(commit.transition);
      this.crashInjector.maybeCrash("AFTER_DERIVED_AUDIT_BEFORE_CHECKPOINT", input.operationKey);
      if (input.checkpoint) await this.checkpointInsideGate();
      this.crashInjector.maybeCrash("AFTER_CHECKPOINT", input.operationKey);
      const totalMs = performance.now() - requestedAt;
      const criticalTrace = input.type === "RISK_REDUCTION_ADMITTED" || input.type === "PAPER_FILL"
        || (input.type === "REVERSAL_PROTECTION_STATE_UPDATED" && parseDerivedAudit(commit.transition.payload).some(r => r.type.startsWith("GUARDIAN_")));
      if (criticalTrace || (this.env.GUARDIAN_SEPARATE_ENABLED && totalMs >= 1000)) this.logger.info?.({
        type: input.type, operationKey: input.operationKey, aggregateId: input.aggregateId, sequence: commit.sequence,
        occurredAt: occurredAt.toISOString(), receiptAt, appliedAt,
        gateWaitMs, resolveMs, postgresMs, applyAuditMs: performance.now() - auditAt, totalMs,
      }, "durable phase timing");
      return commit;
    });
  }

  private async catchUpOrRejectStalePlan(
    operationKey: string,
    type: string,
    error: DurableCursorBehindError,
  ): Promise<DurableCommitResult> {
    await this.replayDurableTail(this.lastAppliedDurableCursor);
    const latest = await this.journal.latestDurableSequence();
    const report = validatePostReplayInvariants(this.recoverableBag(), this.lastAppliedDurableCursor, latest);
    if (!report.ok) {
      const closed = new RecoveryFailClosedError("invariant", report.reasons.join(","));
      this.failClosedIntegrity(closed);
      throw closed;
    }
    const existing = await this.journal.readDurableByOperationKey(operationKey);
    if (existing) {
      this.metrics.noteDurableCommit(type, false);
      return {
        sequence: existing.sequence,
        operationKey,
        inserted: false,
        existing: true,
        transition: existing,
      };
    }
    const closed = new RecoveryFailClosedError(
      "cursor_fence",
      `stale_plan: ${operationKey} planned against cursor ${error.expectedPreviousSequence} after foreign commit to ${error.latestCommitted}`,
    );
    this.failClosedIntegrity(closed);
    throw closed;
  }

  private async resolveDurablePayload(
    input: {
      operationKey: string;
      type: string;
      aggregateType: string;
      aggregateId: string;
      buildPayload: (occurredAt: Date) => Record<string, unknown>;
    },
    occurredAt: Date,
    computed: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const existing = await this.journal.readDurableByOperationKey(input.operationKey);
    if (!existing) return computed;
    if (
      existing.type !== input.type
      || existing.aggregateType !== input.aggregateType
      || existing.aggregateId !== input.aggregateId
      || (Object.keys(existing.payload).length > 0 && durableIdentityConflicts(input.type, existing.payload, computed))
    ) {
      throw new IdempotencyConflictError(input.operationKey, existing.type, input.type);
    }
    // Compaction deliberately retains only the authoritative identity and its
    // payload hash. Rebuild the full payload from the current operation plan
    // after proving that it is byte-for-byte equivalent to that identity. The
    // caller will receive inserted=false and the original sequence, so the
    // economic transition is never applied again.
    if (Object.keys(existing.payload).length === 0) {
      if (existing.payloadHash !== hashDurablePayload(computed)) {
        throw new IdempotencyConflictError(input.operationKey, existing.type, input.type);
      }
      return computed;
    }
    return existing.payload;
  }

  private async restoreAndRecover(): Promise<void> {
    const started = performance.now();
    try {
      const verifiedCheckpoints = this.journal.listVerifiedCheckpoints ? await this.journal.listVerifiedCheckpoints() : [];
      const checkpoint = verifiedCheckpoints[0] ?? await this.journal.loadLatestCheckpoint();
      if (checkpoint) {
        const payload = checkpoint.payload as Record<string, unknown>;
        if (checkpoint.verified && checkpoint.contentHash && hashCheckpointPayload(payload) !== checkpoint.contentHash) {
          throw new RecoveryFailClosedError("malformed", "checkpoint content hash mismatch");
        }
        const restored = this.hydrateCheckpointPayload(payload);
        if (payload.resultAccounting === undefined) await this.restoreLegacyResults(parseCheckpointDurableCursor(payload) ?? 0);
        if (isLegacyCheckpointPayload(payload)) {
          const safety = assessLegacyCheckpointSafety({
            experiment: this.experiment,
            positions: [...this.ledger.positions.values()],
            restingOrders: [...this.restingOrders.values()],
            makerReservations: Object.fromEntries(this.makerReservations),
          });
          if (!safety.safe) {
            throw new RecoveryFailClosedError("legacy_unsafe", safety.detail);
          }
          this.lastAppliedDurableCursor = await this.journal.latestDurableSequence();
          this.checkpointSequence = checkpoint.sequence;
          await this.checkpointInsideGate();
        } else {
          const cursor = parseCheckpointDurableCursor(payload);
          if (cursor == null) throw new RecoveryFailClosedError("malformed", "checkpoint missing durable cursor");
          this.lastAppliedDurableCursor = cursor;
          this.derivedAuditCursor = parseCheckpointDerivedAuditCursor(payload);
          this.checkpointSequence = checkpoint.sequence;
        }
        await this.replayDurableTail(this.lastAppliedDurableCursor);
        await this.restoreRestingMakerIntentsFromDurable();
        await this.repairAllDerivedAudit();
        const latest = await this.journal.latestDurableSequence();
        const report = validatePostReplayInvariants(this.recoverableBag(), this.lastAppliedDurableCursor, latest);
        if (!report.ok) throw new RecoveryFailClosedError("invariant", report.reasons.join(","));
        this.syncRecoveredProjection();
        this.metrics.checkpointDurableCursor.set(this.lastAppliedDurableCursor);
        const recoveredId = this.experiment?.id ?? checkpoint.experimentId ?? createId("engine");
        await this.safeAppend(createJournalEvent({
          traceId: recoveredId, type: "STATE_RECOVERED", aggregateType: "engine", aggregateId: recoveredId,
          payload: {
            checkpointId: checkpoint.id, positions: restored.positions, restingOrders: restored.restingOrders,
            activeTriggers: restored.activeTriggers, grokSignals: restored.grokSignals,
            durableCursor: this.lastAppliedDurableCursor,
          },
        }));
        this.record("STATE_RECOVERED", recoveredId, null, {
          checkpointId: checkpoint.id, positions: restored.positions, restingOrders: restored.restingOrders,
          activeTriggers: restored.activeTriggers, grokSignals: restored.grokSignals,
          durableCursor: this.lastAppliedDurableCursor,
        });
      } else {
        this.experiment = createIdleExperiment({
          durationHours: this.env.EXPERIMENT_DURATION_HOURS, strategyVersion: this.strategy.version,
          riskConfiguration: this.riskLimits,
          modelConfiguration: {
            provider: "native_deterministic", version: NATIVE_DETERMINISTIC_MODEL_VERSION,
            localExecution: true, externalInference: false, authority: NATIVE_DETERMINISTIC_AUTHORITY,
          },
          startingBalance: this.ledger.snapshot().startingBalance, systemVersion: "0.1.0",
          gitSha: process.env.GIT_SHA ?? process.env.FLY_IMAGE_REF ?? "unknown",
        });
        this.lastAppliedDurableCursor = 0;
        await this.replayDurableTail(0);
        await this.restoreRestingMakerIntentsFromDurable();
        await this.repairAllDerivedAudit();
        const latest = await this.journal.latestDurableSequence();
        const report = validatePostReplayInvariants(this.recoverableBag(), this.lastAppliedDurableCursor, latest);
        if (!report.ok) throw new RecoveryFailClosedError("invariant", report.reasons.join(","));
        this.syncRecoveredProjection();
      }
      if (this.experiment?.id) {
        const verified = await this.readVerifiedQuarantine(this.experiment.id);
        if (this.reportQuarantine && (!verified || JSON.stringify(verified) !== JSON.stringify(this.reportQuarantine))) {
          throw new RecoveryFailClosedError("invariant", "checkpoint quarantine differs from durable receipt");
        }
        this.reportQuarantine = verified;
      }
      // A recovered order whose durable residual is already gone/changed
      // must terminate even if that market no longer receives book callbacks.
      for (const owner of [...this.riskReductionOwners.values()]) {
        const position = this.ledger.positions.get(owner.marketId);
        if (!position?.unpairedOutcome || !d(position.unpairedQuantity).gt(0)
          || unhedgedEpisode(position) !== owner.unhedgedStartedAt
          || !d(position.unpairedQuantity).eq(owner.admittedResidualQuantity)) {
          await this.failRiskReduction(owner, "RECOVERED_RESIDUAL_CHANGED");
        }
      }
      this.syncExperimentWindow();
      if (this.experiment?.status === "RUNNING" && this.experiment.startValidationPending) {
        await this.commitAbortedStart(this.experiment, "EXPERIMENT_START_VALIDATION_INCOMPLETE_RECOVERY");
      }
      this.syncExperimentWindow();
      if (this.experiment?.id) {
        const existing = await this.journal.loadExperimentReport(this.experiment.id, "final");
        if (existing && isCanonicalFinalReport(existing)) {
          this.reportGenerated = true;
          await ensureReportGeneratedAudit(this.journal, existing);
        }
      }
    } catch (error) {
      if (error instanceof CrashInjectedError) throw error;
      const normalized = normalizeRecoveryError(error);
      this.metrics.noteRecoveryFailure(normalized.reason);
      this.overload.markIntegrityFault();
      this.state.overloadState = "FAIL_CLOSED";
      this.updateReady();
      throw normalized;
    } finally {
      this.metrics.recoveryDuration.observe((performance.now() - started) / 1000);
    }
  }

  /** Upgrade old checkpoints only when the complete current experiment history proves the totals. */
  private async restoreLegacyResults(cursor: number): Promise<void> {
    const ledger = this.ledger.snapshot();
    if (!this.experiment?.startedAt && !this.ledger.positions.size && d(ledger.realizedPnl).eq(0) && d(ledger.fees).eq(0)) {
      this.state.resultAccounting = emptyResultAccounting();
      return;
    }
    const start = await this.journal.readDurableByOperationKey(`experiment:${this.experiment?.id}:started`);
    if (!start || start.sequence > cursor || !start.payload.experiment) return;
    const bag: RecoverableEngineState = {
      ledger: new PaperLedger(this.experiment!.startingBalance), experiment: null,
      restingOrders: new Map(), makerReservations: new Map(), settledMarkets: new Set(),
      lastInfrastructureAccrualAt: 0, resultAccounting: emptyResultAccounting(),
    };
    // Recovery-only journal reads. Neither dashboard nor /api/dashboard queries storage.
    try {
      applyDurableTransition(bag, start);
      let applied = start.sequence;
      while (applied < cursor) {
        const before = applied;
        const page = await this.journal.readDurableTransitionsAfter(applied, this.recoveryPageSize);
        if (!page.length) return;
        for (const row of page) {
          if (row.sequence > cursor) break;
          if (row.sequence !== applied + 1) return; // Retention removed required evidence.
          applyDurableTransition(bag, row);
          applied = row.sequence;
        }
        if (applied === before) return;
      }
      if (bag.experiment?.id !== this.experiment?.id || !d(bag.ledger.snapshot().realizedPnl).eq(ledger.realizedPnl)
        || !d(bag.ledger.snapshot().fees).eq(ledger.fees)) return;
      this.state.resultAccounting = bag.resultAccounting!;
    } catch {
      // Missing legacy evidence cannot change trading authority. Display unavailable until a new experiment.
    }
  }

  private async restoreRestingMakerIntentsFromDurable(): Promise<void> {
    for (const [orderId, resting] of this.restingOrders) {
      if (resting.intent) continue;
      const reserve = await this.journal.readDurableByOperationKey(`maker:${orderId}:reserve`);
      const intent = reserve?.payload?.intent;
      if (intent !== "OPEN_FIRST_LEG" && intent !== "HEDGE_SECOND_LEG") continue;
      this.restingOrders.set(orderId, { ...resting, intent });
    }
    this.syncRestingIndexFromOrders();
  }

  private hydrateCheckpointPayload(payload: Record<string, unknown>): {
    positions: number; restingOrders: number; activeTriggers: number; grokSignals: number;
  } {
    const ledger = ledgerStateSchema.parse(payload.ledger);
    const positions = Array.isArray(payload.positions) ? payload.positions.map((item) => positionSchema.parse(item)) : [];
    this.ledger.restore(ledger, positions);
    this.state.resultAccounting = payload.resultAccounting === undefined
      ? emptyResultAccounting(false) : resultAccountingSchema.parse(payload.resultAccounting);
    this.economicSettlementLookups.clear();
    for (const raw of (payload.settlementLookups ?? []) as unknown[]) {
      const lookup = settlementLookupSchema.parse(raw);
      if (this.openPositionMarketIds().has(lookup.marketId)) {
        this.economicSettlementLookups.set(lookup.marketId, lookup);
      }
    }
    this.state.setLedger(this.ledger.snapshot());
    this.experiment = payload.experiment ? experimentManifestSchema.parse(payload.experiment) : createIdleExperiment({
      durationHours: this.env.EXPERIMENT_DURATION_HOURS, strategyVersion: this.strategy.version,
      riskConfiguration: this.riskLimits,
      modelConfiguration: {
        provider: "native_deterministic", version: NATIVE_DETERMINISTIC_MODEL_VERSION,
        localExecution: true, externalInference: false, authority: NATIVE_DETERMINISTIC_AUTHORITY,
      },
      startingBalance: this.ledger.snapshot().startingBalance,
      systemVersion: "0.1.0", gitSha: "unknown",
    });
    const resting = Array.isArray(payload.restingOrders) ? payload.restingOrders.map((item) => restingPaperOrderSchema.parse(item)) : [];
    for (const item of resting) this.restingOrders.set(item.order.id, item);
    this.syncRestingIndexFromOrders();
    const reservations = payload.makerReservations && typeof payload.makerReservations === "object"
      ? payload.makerReservations as Record<string, string> : {};
    for (const [orderId, amount] of Object.entries(reservations)) this.makerReservations.set(orderId, amount);
    if (payload.riskReductionOwners !== undefined && !Array.isArray(payload.riskReductionOwners)) {
      throw new RecoveryFailClosedError("malformed", "pending takers must be an array");
    }
    for (const raw of (payload.riskReductionOwners ?? []) as unknown[]) {
      const owner = riskReductionOwnershipSchema.parse(raw);
      if (this.riskReductionOwners.has(owner.marketId)) throw new RecoveryFailClosedError("invariant", "duplicate pending taker");
      this.riskReductionOwners.set(owner.marketId, owner);
    }
    if (payload.riskReductionFailures !== undefined && !Array.isArray(payload.riskReductionFailures)) {
      throw new RecoveryFailClosedError("malformed", "taker failures must be an array");
    }
    for (const raw of (payload.riskReductionFailures ?? []) as unknown[]) {
      const failure = riskReductionExecutionFailureSchema.parse(raw);
      const marketId = failure.ownership.marketId;
      if (this.riskReductionFailures.has(marketId)) throw new RecoveryFailClosedError("invariant", "duplicate taker failure");
      this.riskReductionFailures.set(marketId, failure);
    }
    if (payload.nakedDirectionalDiagnostics !== undefined && !Array.isArray(payload.nakedDirectionalDiagnostics)) {
      throw new RecoveryFailClosedError("malformed", "soak diagnostics must be an array");
    }
    for (const raw of (payload.nakedDirectionalDiagnostics ?? []) as unknown[]) {
      const diagnostic = nakedDirectionalDiagnosticSchema.parse(raw);
      if (this.nakedDirectionalDiagnostics.has(diagnostic.marketId)) throw new RecoveryFailClosedError("invariant", "duplicate soak diagnostic");
      this.nakedDirectionalDiagnostics.set(diagnostic.marketId, diagnostic);
    }
    if (payload.reversalProtectionStates !== undefined && !Array.isArray(payload.reversalProtectionStates)) {
      throw new RecoveryFailClosedError("malformed", "invalid reversal protection checkpoint");
    }
    for (const raw of (payload.reversalProtectionStates ?? []) as unknown[]) {
      const reversal = reversalProtectionStateSchema.parse(raw);
      if (this.reversalProtectionStates.has(reversal.marketId)) throw new RecoveryFailClosedError("invariant", "duplicate reversal state");
      this.reversalProtectionStates.set(reversal.marketId, reversal);
    }
    if (payload.profitTrailingPairStates !== undefined && !Array.isArray(payload.profitTrailingPairStates)) {
      throw new RecoveryFailClosedError("malformed", "profit trailing pair states must be an array");
    }
    for (const raw of (payload.profitTrailingPairStates ?? []) as unknown[]) {
      const trailing = profitTrailingPairStateSchema.parse(raw);
      if (this.profitTrailingPairStates.has(trailing.marketId)) throw new RecoveryFailClosedError("invariant", "duplicate profit trailing pair state");
      this.profitTrailingPairStates.set(trailing.marketId, trailing);
    }
    const triggers = Array.isArray(payload.activeTriggers) ? payload.activeTriggers.map((item) => triggerEventSchema.parse(item)) : [];
    for (const trigger of triggers) {
      this.activeTriggers.set(trigger.id, trigger);
      this.state.addTrigger(trigger);
      const plane = classifyTriggerProvenance(trigger);
      if (plane === "MARKET" || plane === "REFERENCE") {
        this.triggerProvenance.set(trigger.id, plane === "MARKET"
          ? { kind: "MARKET", tokenEpochs: {} }
          : { kind: "REFERENCE", asset: trigger.asset ?? "", epoch: -1 });
        this.provenanceInvalid.add(trigger.id);
      } else {
        this.triggerProvenance.set(trigger.id, { kind: "NONE" });
      }
    }
    const signals = Array.isArray(payload.grokSignals) ? payload.grokSignals.map((item) => grokSignalEnvelopeSchema.parse(item)) : [];
    for (const signal of signals) {
      const byRole = this.signalsByTrigger.get(signal.triggerId) ?? new Map<ExternalAgentRoleId, GrokSignalEnvelope>();
      byRole.set(signal.agentId, signal); this.signalsByTrigger.set(signal.triggerId, byRole); this.state.addGrokSignal(signal);
    }
    if (Array.isArray(payload.activationLatency)) this.activation.restoreLatency(payload.activationLatency as ActivationLatency[]);
    this.state.setActivationLatency(this.activation.snapshot());
    if (Array.isArray(payload.marketMetadata)) {
      for (const raw of payload.marketMetadata as Array<Record<string, unknown>>) {
        if (!raw || typeof raw.id !== "string") continue;
        this.marketsById.set(raw.id, raw as unknown as DiscoveredMarket);
        this.assetByMarket.set(raw.id, typeof raw.asset === "string" ? raw.asset : null);
        if (typeof raw.asset === "string") this.assetIndex.add(raw.asset, raw.id);
        if (raw.endDate) this.triggerEngine.noteMarketWindow(raw.id, typeof raw.asset === "string" ? raw.asset : null, String(raw.endDate));
      }
    }
    if (Array.isArray(payload.managedMarkets)) {
      for (const item of payload.managedMarkets) {
        if (typeof item === "string") this.managedMarketIds.add(item);
        else if (item && typeof item === "object" && typeof (item as { id?: unknown }).id === "string") {
          this.managedMarketIds.add((item as { id: string }).id);
        }
      }
    }
    if (Array.isArray(payload.settledMarkets)) {
      for (const id of payload.settledMarkets as string[]) this.settledMarkets.add(id);
    }
    if (typeof payload.lastInfrastructureAccrualAt === "number") {
      this.lastInfrastructureAccrualAt = payload.lastInfrastructureAccrualAt;
    }
    if (typeof payload.derivedAuditCursor === "number") {
      this.derivedAuditCursor = parseCheckpointDerivedAuditCursor(payload);
    }
    this.reportQuarantine = payload.reportQuarantine == null ? null : quarantineReceiptSchema.parse(payload.reportQuarantine);
    for (const fault of parseCheckpointIntegrityFaults(payload)) {
      this.reportSourceIntegrity.set(`${fault.experimentId}:${fault.eventId}`, fault);
    }
    return { positions: positions.length, restingOrders: resting.length, activeTriggers: triggers.length, grokSignals: signals.length };
  }

  private async repairAllDerivedAudit(): Promise<void> {
    let cursor = this.derivedAuditCursor;
    for (;;) {
      const page = await this.journal.readDurableTransitionsAfter(cursor, this.recoveryPageSize);
      if (page.length === 0) break;
      for (const row of page) {
        if (row.sequence > this.lastAppliedDurableCursor) return;
        await this.emitStoredDerivedAudit(row);
        cursor = row.sequence;
      }
      if (page.length < this.recoveryPageSize) break;
    }
  }

  private async replayDurableTail(after: number): Promise<void> {
    let cursor = after;
    for (;;) {
      const page = await this.journal.readDurableTransitionsAfter(cursor, this.recoveryPageSize);
      if (page.length === 0) break;
      for (const row of page) {
        if (row.sequence <= cursor) {
          throw new RecoveryFailClosedError("cursor_fence", `non-monotonic replay ${row.sequence} <= ${cursor}`);
        }
        this.applyCommitted(row);
        this.metrics.noteRecoveryReplay(row.type);
        await this.emitStoredDerivedAudit(row);
        cursor = row.sequence;
      }
      if (page.length < this.recoveryPageSize) break;
    }
    this.syncRestingIndexFromOrders();
  }

  /** HEAD's verified maker producer omitted only direct experiment/deRisk/timestamps.
   * Missing canonical authority cannot be reconstructed from that reservation. */
  private async historicalMakerEvent(row: DurableTransition, record: DerivedAuditRecord): Promise<JournalEvent | null> {
    if (row.type !== "PAPER_MAKER_RESERVED" || record.type !== "PAPER_ORDER_CREATED"
      || record.identityGeneration != null || record.recordedAt != null || record.occurredAt != null
      || Object.hasOwn(record.payload, "experimentId") || Object.hasOwn(record.payload, "deRisk")) return null;
    const id = row.payload.orderId;
    const resting = row.payload.resting as { order?: Record<string, unknown> } | undefined;
    const order = resting?.order;
    const companion = parseDerivedAudit(row.payload).find(r => r.type === "PAPER_MAKER_RESERVED");
    const actual = await retryPersistenceTransport(() => this.journal.readJournalEventById(record.id), this.persistenceRetryOptions("audit"));
    const bad = () => { throw new JournalEventConflictError(record.id); };
    if (typeof id !== "string" || row.operationKey !== `maker:${id}:reserve`
      || row.aggregateType !== "paper_order" || row.aggregateId !== id
      || record.id !== derivedAuditEventId("PAPER_ORDER_CREATED", id)
      || record.aggregateType !== "paper_order" || record.aggregateId !== id
      || !order || order.id !== id || order.orderType !== "MAKER" || order.status !== "RESTING"
      || record.traceId !== order.traceId || record.sourceEventId != null || record.correlationId != null
      || !companion || companion.id !== derivedAuditEventId("PAPER_MAKER_RESERVED", id)
      || companion.aggregateType !== "paper_order" || companion.aggregateId !== id
      || companion.traceId !== order.traceId || companion.payload.orderId !== id
      || companion.payload.reserved !== row.payload.reserved
      || typeof companion.payload.experimentId !== "string" || !actual
      || actual.payload.experimentId !== companion.payload.experimentId
      || !d(String(order.quantity)).times(String(order.requestedPrice)).eq(String(row.payload.reserved))) return bad();
    const expectedPayload = { ...order, status: "PENDING", intent: row.payload.intent };
    // Check every old encoded field against the actual reservation order first.
    assertJournalEventIdentity({ ...actual, payload: expectedPayload }, { ...actual, payload: record.payload });
    const payload = { ...record.payload, experimentId: companion.payload.experimentId,
      ...(Object.hasOwn(actual.payload, "deRisk") ? { deRisk: actual.payload.deRisk } : {}) };
    assertJournalEventIdentity({ ...actual, id: record.id, type: record.type, aggregateType: record.aggregateType,
      aggregateId: record.aggregateId, traceId: record.traceId, sourceEventId: null,
      correlationId: record.traceId, payload }, actual);
    return actual;
  }

  private async emitStoredDerivedAudit(row: DurableTransition): Promise<void> {
    let records: DerivedAuditRecord[];
    try { records = parseDerivedAudit(row.payload); }
    catch (error) { this.failClosedIntegrity(error); throw error; }
    const occurredAt = new Date(row.occurredAt);
    for (const record of records) {
      const historical = row.type === "PAPER_MAKER_RESERVED" && record.type === "PAPER_ORDER_CREATED"
        && record.identityGeneration == null ? await this.historicalMakerEvent(row, record) : null;
      if (historical) { await this.appendAcknowledged(historical, undefined, false, true); continue; }
      const event = createJournalEvent({
        id: record.id,
        traceId: record.traceId,
        type: record.type,
        aggregateType: record.aggregateType,
        aggregateId: record.aggregateId,
        payload: record.payload,
        sourceEventId: record.sourceEventId,
        correlationId: record.correlationId,
        occurredAt: record.occurredAt ? new Date(record.occurredAt) : occurredAt,
      });
      if (record.recordedAt) event.recordedAt = record.recordedAt;
      await this.appendAcknowledged(event, undefined, record.identityGeneration == null && record.recordedAt == null, true);
    }
    this.derivedAuditCursor = Math.max(this.derivedAuditCursor, row.sequence);
  }

  private persistenceRetryOptions(operationClass: "append" | "durable" | "audit" | "integrity") {
    const labels = { operation_class: operationClass };
    return {
      onRetry: () => this.metrics.journalTransientRetry.inc(labels),
      onRecovered: () => this.metrics.journalTransientRetryRecovered.inc(labels),
      onExhausted: () => this.metrics.journalTransientRetryExhausted.inc(labels),
    };
  }

  private async appendWithTransportRecovery(
    event: JournalEvent, options: JournalAppendOptions = {},
    operationClass: "append" | "audit" | "integrity" = "append",
    allowStoredRecordedAt = false,
  ): Promise<void> {
    const frozen = prepareJournalEvent(event);
    await retryPersistenceTransport(() => this.journal.append(frozen, {
      ...options, ...(allowStoredRecordedAt ? { legacyRecordedAt: true } : {}),
    }), { ...this.persistenceRetryOptions(operationClass), ...options });
  }

  private async appendAcknowledged(event: ReturnType<typeof createJournalEvent>, signal?: AbortSignal, allowStoredRecordedAt = false, _ownedIdentity = false): Promise<void> {
    event = prepareJournalEvent(event);
    const journalClass = classifyJournalMetricClass(event.type);
    try {
      await this.journalAdmission.run(journalClass, true, async () => {
        if (signal?.aborted) throw new JournalAdmissionRejected(journalClass, true, "JOURNAL_ADMISSION_CLOSED");
        await this.metrics.observeDirectJournal(event.type, async () => {
          await this.appendWithTransportRecovery(event, { signal }, "audit", allowStoredRecordedAt);
          this.state.database = "UP";
        });
      }, signal, { reportInput: this.isCanonicalWindowAdmission(event.type) });
    } catch (error) {
      if (error instanceof CrashInjectedError) throw error;
      this.failClosedIntegrity(error);
      this.logger.error(
        { err: error, traceId: event.traceId, admitted: !(error instanceof JournalAdmissionRejected) },
        error instanceof JournalAdmissionRejected ? "derived audit admission rejected" : "derived audit append failed",
      );
      throw error;
    }
  }

  private experimentDerivedAudit(type: "EXPERIMENT_STARTED" | "EXPERIMENT_START_VALIDATED" | "EXPERIMENT_START_ABORTED" | "EXPERIMENT_ENTRY_CLOSED" | "EXPERIMENT_COMPLETED", experiment: ExperimentManifest): DerivedAuditRecord {
    return {
      id: derivedAuditEventId(type, experiment.id),
      type,
      aggregateType: "experiment",
      aggregateId: experiment.id,
      traceId: experiment.id,
      payload: { ...experiment, experimentId: experiment.id },
    };
  }

  private projectDerivedAudit(
    input: { operationKey: string; type: string; aggregateType: string; aggregateId: string; payload: Record<string, unknown> },
    occurredAt: Date,
    build: (projected: RecoverableEngineState) => DerivedAuditRecord[],
  ): DerivedAuditRecord[] {
    const projected = cloneRecoverableState(this.recoverableBag());
    applyDurableTransition(projected, plannedDurableTransition(input, occurredAt, this.lastAppliedDurableCursor + 1));
    return build(projected);
  }

  private async restoreExperimentState(): Promise<void> {
    await this.restoreAndRecover();
  }

  private onMarketSnapshot(snapshot: MarketSnapshot): Promise<void> | void {
    if (!this.state.recoveryComplete) return;
    if (!this.tokenIdSet.has(snapshot.tokenId)) return;
    const started = performance.now();
    this.metrics.marketMessages.inc();
    const timing = bookTiming(snapshot, this.nowFn().getTime());
    if (timing.sourceToIngressMs !== null && Number.isFinite(timing.sourceToIngressMs) && timing.sourceToIngressMs >= 0) this.metrics.bookSourceToIngress.observe(timing.sourceToIngressMs / 1000);
    if (timing.ingressToReceivedMs !== null && Number.isFinite(timing.ingressToReceivedMs) && timing.ingressToReceivedMs >= 0) this.metrics.bookIngressToReceived.observe(timing.ingressToReceivedMs / 1000);
    if (Number.isFinite(timing.receivedToDecisionMs) && timing.receivedToDecisionMs >= 0) this.metrics.bookReceivedToProcessing.observe(timing.receivedToDecisionMs / 1000);
    this.state.connections.polymarket.lastMessageAt = snapshot.receivedAt;
    const synchronized = this.adapterTokenSynchronized(snapshot.tokenId);
    if (!synchronized) {
      this.observeGuardianSnapshot(snapshot.marketId);
      this.updateReady();
      this.state.eventLatencyMs = performance.now() - started;
      this.metrics.eventLatency.observe((performance.now() - started) / 1000);
      return;
    }
    const epoch = this.adapterTokenEpoch(snapshot.tokenId);
    const previousEpoch = this.snapshotEpoch.get(snapshot.tokenId);
    const newTrustedEpoch = previousEpoch !== epoch;
    if (newTrustedEpoch) {
      const siblingId = this.siblingTokenId(snapshot.marketId, snapshot.outcome);
      const siblingEpoch = siblingId ? this.snapshotEpoch.get(siblingId) : undefined;
      if (siblingEpoch !== epoch) this.clearMarketPairProjections(snapshot.marketId);
      this.snapshotEpoch.set(snapshot.tokenId, epoch);
    }
    this.state.marketSnapshots.set(snapshot.tokenId, snapshot);
    const pair = this.snapshots.get(snapshot.marketId) ?? {};
    const previous = pair[snapshot.outcome];
    pair[snapshot.outcome] = snapshot;
    this.snapshots.set(snapshot.marketId, pair);
    this.observeGuardianSnapshot(snapshot.marketId);
    this.observeSoakPosition(snapshot.marketId);
    this.kickProfitTrailingPair(snapshot.marketId);
    this.scheduleSnapshotReversalProtection(snapshot.marketId);
    if (snapshot.asset && this.assetByMarket.get(snapshot.marketId) !== snapshot.asset) {
      this.assetByMarket.set(snapshot.marketId, snapshot.asset);
      this.assetIndex.add(snapshot.asset, snapshot.marketId);
      this.metrics.assetMarketIndexEntries.set(this.assetIndex.entryCount());
    }
    this.updateReady();
    // Market snapshots are live processing inputs. The current snapshot/state
    // maps above remain the source for trigger, maker, risk, and dashboard work;
    // raw observations are intentionally not permanent journal records.
    this.record("MARKET_EVENT", snapshot.traceId, snapshot.marketId, { tokenId: snapshot.tokenId, outcome: snapshot.outcome, sequence: snapshot.sequence }, snapshot.asset ?? null);

    // The usual TAKER feed callback is synchronous. Preserve the per-token
    // awaited maker lifecycle only when a resting order actually exists.
    if (previous && this.restingIndex.orderIdsFor(snapshot.tokenId).size > 0) {
      return this.evolveSnapshotMakers(snapshot, previous).then(() => this.finishMarketSnapshot(snapshot, started));
    }
    this.finishMarketSnapshot(snapshot, started);
  }

  private async evolveSnapshotMakers(snapshot: MarketSnapshot, previous: MarketSnapshot): Promise<void> {
    for (const orderId of this.restingIndex.orderIdsFor(snapshot.tokenId)) {
      const resting = this.restingOrders.get(orderId);
      if (!resting || resting.order.tokenId !== snapshot.tokenId) continue;
      if(resting.order.makerFirst && this.hybridQuoteInvalidReason(resting,snapshot)){
        await this.releaseMaker(orderId,{cancel:true,reason:this.hybridQuoteInvalidReason(resting,snapshot)!});continue;
      }
      if(resting.order.makerFirst&&!this.makerQueuePrimed.has(orderId)){
        const displayed=snapshot.bids.find(l=>d(l.price).eq(resting.order.requestedPrice))?.quantity??'0';
        if(d(displayed).gt(resting.queueAhead))resting.queueAhead=displayed;
      }
      const currentPosition = this.ledger.positions.get(resting.order.marketId);
      if (
        this.restingOrderIntent(resting) === "OPEN_FIRST_LEG"
        && currentPosition
        && d(currentPosition.pairedQuantity).gt(0)
      ) {
        await this.releaseMaker(orderId, {
          cancel: true,
          reason: "FIRST_LEG_LIFECYCLE_CLOSED",
        });
        continue;
      }
      const evolved = evolveMakerOrder(resting, previous, snapshot);
      if (evolved.fill) {
        // Capture the maker lifecycle intent synchronously here, BEFORE the
        // async fill path. Rescue/cancel of the resting order during the
        // applyFill boundary must not erase this identity: the PR2 fill-time
        // residual/token guards key off economicIntent. restingOrderIntent
        // keeps the legacy pre-PR1 inference, evaluated against the state
        // that derived the fill instead of re-read after an await.
        const restingIntent = this.restingOrderIntent(resting);
        await this.applyFill(evolved.fill, { restingAfter: evolved.resting, intent: restingIntent, executionSnapshot: snapshot });
      } else {
        this.restingOrders.set(orderId, evolved.resting);
        if (Number(evolved.resting.remaining) === 0) {
          this.restingOrders.delete(orderId);
          this.restingIndex.remove(orderId);
        }
      }
    }
  }

  /** Dual maker is a separate PAPER strategy. Neither quote is a guaranteed fill;
   * both share one economic group, but their cash reservations remain independent.
   * The low-priced quote has a pair-plan admission, not the directional 0.65 floor. */
  private async trySubmitDualMakerPair(directionOrder:PaperOrder, duplicateKey:string, pair:Record<Outcome,MarketSnapshot|null|undefined>):Promise<boolean>{
    if(!this.dualMakerEnabled()||!this.isExperimentWindowOpen()
      ||this.state.database!=='UP'||this.state.journalDegraded||this.overload.state==='FAIL_CLOSED')return false;
    const up=pair.UP,down=pair.DOWN,marketId=directionOrder.marketId;
    if(!up||!down||up.marketId!==marketId||down.marketId!==marketId
      ||!this.entryQualified.has(marketId)||this.firstLegLifecycleBlocked(marketId)
      ||!this.adapterTokenSynchronized(up.tokenId)||!this.adapterTokenSynchronized(down.tokenId)
      ||this.firstLegAdmissionReason({marketId,tokenId:up.tokenId,outcome:"UP"},up)
      ||this.firstLegAdmissionReason({marketId,tokenId:down.tokenId,outcome:"DOWN"},down)
      ||!this.firstLegExpiryAdmission(marketId).eligible)return false;
    const plan=planDualMakerQuotes({up,down,quantity:directionOrder.quantity,
      perSideRiskCapUsd:String(this.riskLimits.maxOrderSizeUsd),availableCashUsd:this.ledger.availableCash().toFixed(),
      minPairNetRoi:FIRST_LEG_MIN_NET_ROI,nowMs:this.nowFn().getTime(),maxBookAgeMs:this.riskLimits.staleFeedTimeoutMs});
    if(!plan.plan)return false;
    const group=createId('dual_pair'),ttl=this.makerFirstConfig().firstLegTtlMs;
    const expiry=this.marketsById.get(marketId)?.endDate;
    const end=Math.min(this.nowFn().getTime()+ttl,expiry?Date.parse(expiry)-PROTECTIVE_DE_RISK_PRE_EXPIRY_SAFETY_MS:Number.POSITIVE_INFINITY);
    if(!Number.isFinite(end)||end<=this.nowFn().getTime()+this.env.PAPER_EXECUTION_LATENCY_MS)return false;
    const build=(book:MarketSnapshot,price:string):PaperOrder=>({
      ...directionOrder,id:createId('order'),strategyId:'two-sided-maker-v1',side:'BUY',outcome:book.outcome,tokenId:book.tokenId,
      orderType:'MAKER',requestedPrice:price,status:'PENDING',createdAt:this.nowFn().toISOString(),entryAdmission:undefined,
      makerFirst:{version:1,role:'FIRST_LEG',postOnly:true,dualGroupId:group,expiresAt:new Date(end).toISOString(),
        submissionLatencyMs:this.env.PAPER_EXECUTION_LATENCY_MS,tokenEpoch:this.adapterTokenEpoch(book.tokenId),
        postingBookEventId:book.eventId,takerReferencePrice:book.asks[0]!.price,
        postingFeeKey:JSON.stringify([book.feeSchedule?.rate,book.feeSchedule?.exponent,book.feeSchedule?.takerOnly])},
    });
    const quotes=[build(up,plan.plan.up.price),build(down,plan.plan.down.price)];
    if(!this.tryReserveFirstLegAdmission(marketId))return false;
    try{
      // A second reservation is allowed ONLY as the sibling of the same plan.
      // Each remains within the original durable cash / exposure fences.
      for(const quote of quotes){
        if(this.ledger.positions.has(marketId)||!this.isExperimentWindowOpen()
          ||this.confirmedGuardianProtection(marketId))throw new Error('DUAL_PLAN_SUPERSEDED');
        await this.reserveMaker(quote,quote.outcome==='UP'?up:down,'OPEN_FIRST_LEG');
      }
      this.recentTrades.set(duplicateKey,Date.now());
      this.record('DUAL_MAKER_QUOTES_ADMITTED',group,marketId,{
        group,orderIds:quotes.map(q=>q.id),conditionalNetPairUsd:plan.plan.netPairPnlUsd,
        maxUnpairedDownsideUsd:plan.plan.maxUnpairedDownsideUsd});
      return true;
    }catch(error){
      for(const quote of quotes)if(this.restingOrders.has(quote.id))
        await this.releaseMaker(quote.id,{cancel:true,reason:'DUAL_PLAN_ROLLBACK'});
      // A durable fault or programmer error is NOT a normal no-quote result.
      // Preserve existing fail-closed behavior instead of silently trading TAKER.
      if(!(error instanceof FirstLegLifecycleGuardRejectedError)
        &&!(error instanceof MakerReservationCashCommitGuardRejectedError)
        &&!(error instanceof RiskReductionLifecycleGuardRejectedError))throw error;
      this.record('DUAL_MAKER_PLAN_NOT_ADMITTED',group,marketId,{group,reason:error.reason??error.message});
      // A first maker may have filled while its sibling was being reserved:
      // never fall back into another fresh directional first leg in that case.
      return this.ledger.positions.has(marketId);
    }finally{this.pendingFirstLegMarkets.delete(marketId);}
  }

  private dualMakerEnabled():boolean{
    return this.makerFirstConfig().twoSidedFreshEntries && this.makerFirstConfig().enabled;
  }

  private makerFirstConfig():{enabled:boolean;firstLegTtlMs:number;hedgeTtlMs:number;twoSidedFreshEntries:boolean}{
    const stored=this.experiment?.riskConfiguration.makerFirst as {enabled?:boolean;firstLegTtlMs?:number;hedgeTtlMs?:number;twoSidedFreshEntries?:boolean}|undefined;
    if(this.experiment && ['RUNNING','ENTRY_CLOSED'].includes(this.experiment.status))return {
      enabled:stored?.enabled===true,firstLegTtlMs:stored?.firstLegTtlMs??2000,hedgeTtlMs:stored?.hedgeTtlMs??750,twoSidedFreshEntries:stored?.twoSidedFreshEntries===true};
    return {enabled:this.env.PAPER_MAKER_FIRST_ENABLED,firstLegTtlMs:this.env.PAPER_MAKER_FIRST_QUOTE_TTL_MS,hedgeTtlMs:this.env.PAPER_MAKER_HEDGE_QUOTE_TTL_MS,twoSidedFreshEntries:this.env.PAPER_DUAL_SIDED_MAKER_ENABLED};
  }

  private entryProjectionLegs(excludeOrderId?:string):ReturnType<typeof projectableLegsFromPositions>{
    const legs=projectableLegsFromPositions(this.ledger.positions.values(),this.assetByMarket);
    for(const r of this.restingOrders.values())if(r.order.makerFirst&&r.order.id!==excludeOrderId
      &&this.restingOrderIntent(r)==='OPEN_FIRST_LEG'&&d(r.remaining).gt(0))legs.push({marketId:r.order.marketId,
        outcome:r.order.outcome,quantity:r.remaining,avgCost:r.order.requestedPrice,asset:this.assetByMarket.get(r.order.marketId)??null});
    return legs;
  }

  private pendingEntryExposureReason(order:Pick<PaperOrder,'id'|'marketId'|'outcome'|'quantity'|'requestedPrice'>):string|null{
    if(!this.makerFirstConfig().enabled)return null;
    const projected=projectPaperExposures({current:computePaperExposures(this.ledger.positions.values(),order.marketId,this.assetByMarket,this.nowFn().getTime()),
      positions:this.entryProjectionLegs(order.id),candidate:{...order,price:order.requestedPrice,intent:'OPEN_FIRST_LEG',asset:this.assetByMarket.get(order.marketId)??null},nowMs:this.nowFn().getTime()});
    for(const [key,limit] of [['market',this.riskLimits.maxMarketExposureUsd],['total',this.riskLimits.maxTotalExposureUsd],
      ['directional',this.riskLimits.maxDirectionalExposureUsd],['unhedged',this.riskLimits.maxUnhedgedExposureUsd],['asset',this.riskLimits.maxAssetExposureUsd]] as const)
      if(d(projected[key]).gt(limit))return `MAKER_FIRST_PENDING_${key.toUpperCase()}_EXPOSURE`;
    return null;
  }

  private hybridQuoteInvalidReason(resting:RestingPaperOrder,book:MarketSnapshot|undefined):string|null{
    const quote=resting.order.makerFirst;if(!quote)return null;
    if(this.nowFn().getTime()>=Date.parse(quote.expiresAt))return 'MAKER_FIRST_TTL_EXPIRED';
    if(this.restingOrderIntent(resting)==='OPEN_FIRST_LEG'&&!this.isExperimentWindowOpen())return 'MAKER_FIRST_ENTRIES_CLOSED';
    if(this.confirmedGuardianProtection(resting.order.marketId))return 'GUARDIAN_PRIORITY';
    if(!book || !this.freshEarlyCatastrophicSnapshot(book,this.nowFn().getTime())
      ||!this.adapterTokenSynchronized(resting.order.tokenId)||this.adapterTokenEpoch(resting.order.tokenId)!==quote.tokenEpoch)return 'MAKER_FIRST_BOOK_UNTRUSTED';
    if(!book.tickSize||!d(resting.order.requestedPrice).mod(book.tickSize).eq(0))return 'MAKER_FIRST_TICK_CHANGED';
    if(!book.asks[0]||d(book.asks[0].price).lte(resting.order.requestedPrice))return 'MAKER_FIRST_MARKET_MOVED';
    if(quote.postingFeeKey&&quote.postingFeeKey!==JSON.stringify([book.feeSchedule?.rate,book.feeSchedule?.exponent,book.feeSchedule?.takerOnly]))return 'MAKER_FIRST_FEES_CHANGED';
    return null;
  }

  private scheduleMakerQuoteExpiry():void{
    if(this.makerQuoteTimer)clearTimeout(this.makerQuoteTimer);this.makerQuoteTimer=null;
    if(this.state.draining||!this.state.recoveryComplete)return;
    const quotes=[...this.restingOrders.values()].filter(r=>r.order.makerFirst);
    if(!quotes.length)return;
    const delayMs=Math.max(1,Math.min(...quotes.map(r=>Date.parse(r.order.makerFirst!.expiresAt)))-this.nowFn().getTime());
    this.makerQuoteTimer=setTimeout(()=>{this.makerQuoteTimer=null;void this.expireMakerQuotes().catch(error=>this.logger.error({err:error},'maker quote expiry failed'));},delayMs);
    this.makerQuoteTimer.unref();
  }

  private async expireMakerQuotes():Promise<void>{
    try{for(const r of [...this.restingOrders.values()]){
      if(!r.order.makerFirst)continue;
      const reason=this.hybridQuoteInvalidReason(r,this.state.marketSnapshots.get(r.order.tokenId));
      if(reason)await this.releaseMaker(r.order.id,{cancel:true,reason});
    }}finally{this.scheduleMakerQuoteExpiry();}
  }

  private onMakerTrade(trade:MarketTrade):Promise<void>|void{
    if(this.state.draining||!this.state.recoveryComplete||!this.makerFirstConfig().enabled)return;
    this.metrics.makerPublicTrades.inc();
    if(!this.restingIndex.orderIdsFor(trade.tokenId).size)return;
    // A dual pair needs one cross-token lane: two opposite fills must not both be
    // classified as an empty-position FIRST_LEG across separate promise tails.
    const key=[...this.restingIndex.orderIdsFor(trade.tokenId)].some(id=>this.restingOrders.get(id)?.order.makerFirst?.dualGroupId)
      ? 'dual:'+trade.marketId:trade.tokenId;
    const previous=this.makerTradeTails.get(key);
    const work=(previous??Promise.resolve()).then(()=>this.applyMakerTrade(trade));
    const tail=work.then(()=>undefined,()=>undefined);this.makerTradeTails.set(key,tail);
    void tail.then(()=>{if(this.makerTradeTails.get(key)===tail)this.makerTradeTails.delete(key);});
    return work;
  }

  private async applyMakerTrade(trade:MarketTrade):Promise<void>{
    for(const orderId of this.restingIndex.orderIdsFor(trade.tokenId)){
      const r=this.restingOrders.get(orderId);if(!r?.order.makerFirst)continue;
      const book=this.state.marketSnapshots.get(trade.tokenId),reason=this.hybridQuoteInvalidReason(r,book);
      if(reason){await this.releaseMaker(orderId,{cancel:true,reason});continue;}
      const active=this.makerActiveAfter.get(orderId);if(!active||!book)continue;
      if(this.nowFn().getTime()-Date.parse(trade.sourceTimestamp)>this.riskLimits.staleFeedTimeoutMs)continue;
      if(!this.makerQueuePrimed.has(orderId)){
        const displayed=book.bids.find(l=>d(l.price).eq(r.order.requestedPrice))?.quantity??'0';
        if(d(displayed).gt(r.queueAhead))r.queueAhead=displayed;
        if(trade.side==='SELL'&&d(trade.price).eq(r.order.requestedPrice)&&Date.parse(trade.sourceTimestamp)>Date.parse(active)
          &&Date.parse(trade.sourceTimestamp)<=this.nowFn().getTime())this.makerQueuePrimed.add(orderId);
      }
      const evolved=evolveMakerTrade(r,trade,book,active,this.nowFn().getTime());
      if(evolved.fill){
        const applied=await this.applyFill(evolved.fill,{restingAfter:evolved.resting,intent:this.restingOrderIntent(r),executionSnapshot:book,executionOrder:r.order});
        if(applied&&r.order.makerFirst?.dualGroupId){
          // No same-side remainders may increase the original directional exposure.
          for(const sibling of [...this.restingOrders.values()]){
            if(sibling.order.makerFirst?.dualGroupId===r.order.makerFirst.dualGroupId
              &&sibling.order.outcome===r.order.outcome&&d(sibling.remaining).gt(0))
              await this.releaseMaker(sibling.order.id,{cancel:true,reason:'DUAL_FILLED_SIDE_REMAINDER'});
          }
        }
      }else if(this.restingOrders.get(orderId)===r)this.restingOrders.set(orderId,evolved.resting);
    }
    this.scheduleMakerQuoteExpiry();
  }

  private finishMarketSnapshot(snapshot: MarketSnapshot, started: number): void {
    this.metrics.restingOrderTokenIndexEntries.set(this.restingIndex.entryCount());
    // Deterministic market state is a precursor for bounded scout warming;
    // this admission is intentionally separate from economic triggers.
    this.requestScoutRefresh(snapshot, "market");
    const triggers = this.triggerEngine.ingestMarket(snapshot);
    for (const trigger of triggers) this.admitTrigger(trigger, this.provenanceForGenerated(trigger, "MARKET"));
    // Risk exits run outside the websocket consumer await-chain. The per-market
    // flight below prevents duplicate unwind orders while a prior attempt is
    // persisting or waiting through paper execution latency.
    this.scheduleSnapshotProtectiveDeRisk(snapshot.marketId);
    this.state.eventLatencyMs = performance.now() - started;
    this.metrics.eventLatency.observe((performance.now() - started) / 1000);
  }

  /**
   * Metadata-only minOrderSize hydration notification. This is NOT a market
   * event: it never consumes market sequence, never feeds TriggerEngine /
   * NativeV2 shadow / resting-maker evolution / scout refresh, and increments no
   * market-message telemetry. It patches only the cached snapshot's
   * minOrderSize (state.marketSnapshots + the engine pair projection) after
   * verifying the token is still managed, still synchronized, and at the same
   * adapter epoch. The only economic follow-up is the bounded per-market
   * protective de-risk check.
   */
  private onMinOrderSizeHydrated(event: MinOrderSizeHydratedEvent): void {
    if (!this.state.recoveryComplete) return;
    if (!event || !this.tokenIdSet.has(event.tokenId)) return;
    if (!this.adapterTokenSynchronized(event.tokenId)) return;
    if (this.adapterTokenEpoch(event.tokenId) !== event.tokenEpoch) return;
    const current = this.state.marketSnapshots.get(event.tokenId);
    if (!current) return; // never resurrect an old snapshot (post-gap/removal)
    if (current.minOrderSize === event.minOrderSize) return; // idempotent
    const patched: MarketSnapshot = { ...current, minOrderSize: event.minOrderSize };
    this.state.marketSnapshots.set(event.tokenId, patched);
    const pair = this.snapshots.get(current.marketId);
    if (pair && pair[current.outcome]?.tokenId === event.tokenId && pair[current.outcome]?.sequence === current.sequence) {
      pair[current.outcome] = patched;
      this.snapshots.set(current.marketId, pair);
    }
    // Hydration can turn the current opposite book into a genuinely executable
    // full-quantity pair without a new price tick.
    this.kickProfitTrailingPair(current.marketId);
    this.kickReversalProtection(current.marketId);
    // Only economic follow-up hydration may cause: bounded per-market de-risk.
    this.scheduleSnapshotProtectiveDeRisk(current.marketId);
  }

  private async onReferenceTick(tick: ReferenceTick): Promise<void> {
    if (!this.state.recoveryComplete) return;
    if (this.usesDiscovery() && !this.assetIndex.hasAsset(tick.asset)) return;
    this.metrics.referenceTicks.inc();
    const started = performance.now();
    const admission = this.triggerEngine.admitReferenceTick(tick);
    if (admission.overflow) {
      this.noteReferenceLogicalLoss(tick.asset);
      this.state.invalidateReferenceContext(tick.asset);
      this.requestReferenceResync(tick.asset);
      // Test-only processing hold; raw reference observations are never journaled.
      if (this.referenceTelemetryHoldForTest) await this.referenceTelemetryHoldForTest;
      this.metrics.referenceWindowEntries.set(this.triggerEngine.referenceWindowTotalEntries());
      this.metrics.referenceTickProcessingDuration.observe((performance.now() - started) / 1000);
      return;
    }
    if (!admission.accepted) {
      this.metrics.referenceWindowEntries.set(this.triggerEngine.referenceWindowTotalEntries());
      this.metrics.referenceTickProcessingDuration.observe((performance.now() - started) / 1000);
      return;
    }
    const wasUnreliable = this.referenceUnreliable.has(tick.asset);
    this.state.noteReferenceTick(tick.asset, tick.receivedAt, {
      symbol: tick.symbol, venue: tick.venue, price: tick.price,
    });
    if (tick.asset === "BTC") {
      this.state.lastBtcMessageAt = tick.receivedAt;
      this.state.connections.btc.lastMessageAt = tick.receivedAt;
    }
    this.updateReady();
    if (wasUnreliable) {
      this.referenceUnreliable.delete(tick.asset);
      this.referenceResyncRequested.delete(tick.asset);
    }
    this.metrics.referenceWindowEntries.set(this.triggerEngine.referenceWindowTotalEntries());
    // A fresh reference observation is another deterministic precursor. Warm
    // only markets that already have a trusted UP/DOWN pair; requestScoutRefresh
    // applies the per-scope cooldown and in-flight bounds.
    for (const marketId of this.assetIndex.marketIds(tick.asset)) {
      const pair = this.snapshots.get(marketId);
      const candidate = pair?.UP ?? pair?.DOWN;
      if (pair?.UP && pair?.DOWN && candidate) this.requestScoutRefresh(candidate, "reference");
    }
    for (const trigger of admission.triggers) this.admitTrigger(trigger, this.provenanceForGenerated(trigger, "REFERENCE", tick.asset));
    this.metrics.triggerDedupeEntries.set(this.triggerEngine.dedupeEntryCount());
    this.metrics.referenceTickProcessingDuration.observe((performance.now() - started) / 1000);
  }

  private admitTrigger(trigger: TriggerEvent, provenance?: TriggerProvenance): AdmitResult {
    if (this.started && !this.state.recoveryComplete) {
      return { admitted: false, reason: "fail_closed", done: Promise.resolve() };
    }
    const resolved = provenance ?? this.captureProvenance(trigger);
    const lane = triggerWorkLane(trigger.kind);
    const marketEpochFence = resolved.kind === "MARKET" && Object.keys(resolved.tokenEpochs).length > 0
      ? new Map(Object.entries(resolved.tokenEpochs))
      : this.captureMarketEpochFence(trigger);
    this.lastAdmittedMarketEpochFence = marketEpochFence;
    const result = this.supervisor.admit({
      id: trigger.id,
      lane,
      key: triggerWorkKey(trigger),
      estimatedBytes: RUNTIME_BOUNDS.triggerByteEstimate,
      critical: lane === "p0",
      expiresAtMs: Date.parse(trigger.expiresAt),
      marketEpochFence,
      provenance: { kind: resolved.kind },
      run: async (signal) => {
        if (signal.aborted) return;
        if (!this.isProvenanceCurrentlyValid(trigger.id, resolved)) {
          this.metrics.triggersSuppressed.inc({ kind: trigger.kind, severity: String(trigger.severity) });
          return;
        }
        await this.processTrigger(trigger, signal, marketEpochFence, resolved);
      },
    });
    if (!result.admitted) {
      const reason = remapTriggerRejection(result.reason, { lane, experimentStatus: this.experiment?.status });
      this.metrics.runtimeWorkRejected.inc({ lane, reason });
      this.syncRuntimeState();
      return { admitted: false, reason, done: result.done };
    }
    this.syncRuntimeState();
    return result;
  }

  private syncRuntimeState(): void {
    if (!this.supervisor || !this.journalAdmission) return;
    const snap = this.supervisor.snapshot();
    this.state.queueDepth = snap.totalQueued + snap.totalInFlight;
    this.metrics.queueDepth.set(this.state.queueDepth);
    this.state.overloadState = this.overload.recompute({
      p0Queued: snap.lanes.p0.queued,
      p3Queued: snap.lanes.p3.queued,
      journalWaiting: this.journalAdmission.waitingCount,
      p0HardCapHit: snap.lanes.p0.queued >= RUNTIME_BOUNDS.trigger.p0.queueLimit,
      p3HardCapDuringRunning: (this.experiment?.status === "RUNNING")
        && snap.lanes.p3.queued >= RUNTIME_BOUNDS.trigger.p3.queueLimit,
      reservedExhausted: !this.supervisor.criticalReservedAvailable()
        && snap.lanes.p0.queued >= RUNTIME_BOUNDS.trigger.p0.queueLimit,
    });
  }

  private notePersistenceOutcome(type: string, up: boolean): void {
    if (isCriticalPersistenceType(type)) {
      this.state.database = up ? "UP" : "DOWN";
      if (!up) this.updateReady();
    } else {
      this.state.journalDegraded = !up;
    }
    this.syncRuntimeState();
  }

  private async processTrigger(
    trigger: TriggerEvent,
    signal?: AbortSignal,
    marketEpochFence: ReadonlyMap<string, number> | null = null,
    provenance: TriggerProvenance = { kind: "NONE" },
  ): Promise<void> {
    this.metrics.noteProcessTriggerStart();
    const started = performance.now();
    try {
    if (signal?.aborted) return;
    if (this.processTriggerHold) await this.processTriggerHold;
    if (signal?.aborted) return;
    // A trigger emitted while a start is pending cannot begin an active
    // canonical research chain. Existing hedge/settlement obligations remain
    // governed by their normal lane.
    if (this.experimentStartPending && !isRiskHedgeSettlementTrigger(trigger.kind)) return;
    if (this.shouldSuppressStaleMarketTrigger(trigger, marketEpochFence, provenance)) {
      this.metrics.triggersSuppressed.inc({ kind: trigger.kind, severity: String(trigger.severity) });
      return;
    }
    this.metrics.triggers.inc({ kind: trigger.kind, severity: String(trigger.severity) });
    this.record(`TRIGGER_LEVEL_${trigger.severity}`, trigger.traceId, trigger.marketId, { triggerId: trigger.id, kind: trigger.kind, asset: trigger.asset ?? null }, trigger.asset ?? null);
    await this.persistReportInput(createJournalEvent({ traceId: trigger.traceId, type: "TRIGGER_EVENT", aggregateType: "trigger", aggregateId: trigger.id, payload: trigger, sourceEventId: trigger.sourceEventIds[0] ?? null }), signal);
    this.rememberProvenance(trigger.id, provenance);
    this.activeTriggers.set(trigger.id, trigger);
    if (!this.isProvenanceCurrentlyValid(trigger.id, provenance)) {
      this.markProvenanceInvalid(trigger.id);
      this.state.addTrigger(trigger);
      return;
    }
    this.state.addTrigger(trigger);
    await this.experimentBoundaryGate.run(async () => {
      this.record("NATIVE_DECISION_EVALUATION", trigger.traceId, trigger.marketId, {
        authority: NATIVE_DETERMINISTIC_AUTHORITY, triggerId: trigger.id, expiresAt: trigger.expiresAt,
      }, trigger.asset ?? null);
      await this.evaluateNativeDecisionForTrigger(trigger, provenance);
    });
    } finally {
      this.metrics.noteProcessTriggerEnd(started);
    }
  }

  /**
   * Number of ledger positions with positive up or down quantity. This is the
   * authoritative unresolved-position count used at the RUNNING â†’ ENTRY_CLOSED
   * boundary so the durable manifest reproduces the same value across restart/
   * replay.
   */
  private countUnresolvedPositions(): number {
    let count = 0;
    for (const position of this.ledger.positions.values()) {
      if (Number(position.upQuantity) > 0 || Number(position.downQuantity) > 0) count += 1;
    }
    return count;
  }

  /**
   * The entry deadline is an economic admission fence, not merely a
   * maintenance hint. Callers use this while holding experimentBoundaryGate.
   */
  private async ensureFirstLegEntryOpen(): Promise<boolean> {
    if (experimentAllowsFirstLeg(this.experiment, this.nowFn())) return true;
    const current = this.experiment;
    const now = this.nowFn();
    if (current?.status === "RUNNING" && current.entryClosesAt && now.getTime() >= new Date(current.entryClosesAt).getTime()) {
      await this.commitExperimentStatus({
        ...current, status: "ENTRY_CLOSED", completedAt: null, markToMarketAt: null,
        // The durable ENTRY_CLOSED manifest must carry the actual unresolved
        // position count at the boundary, including immediately after restart/
        // replay when the rehydrated ledger already holds open positions.
        unresolvedPositionCount: this.countUnresolvedPositions(),
      });
    }
    return false;
  }

  /**
   * Evaluate one trigger from the current trusted pair. The Engine owns the
   * synchronization, epoch, provenance, lifecycle, and persistence gates;
   * the core model only receives the already-admitted inputs.
   */
  private async evaluateNativeDecisionForTrigger(trigger: TriggerEvent, provenance: TriggerProvenance): Promise<void> {
    if (this.researchIngressBlocked() || !trigger.marketId || !this.isTriggerActionable(trigger.id)) return;
    if (!this.isMarketSynchronized(trigger.marketId)) return;
    const pair = this.snapshots.get(trigger.marketId);
    if (!pair?.UP || !pair.DOWN) return;
    const asset = trigger.asset ?? this.assetByMarket.get(trigger.marketId) ?? pair.UP.asset ?? pair.DOWN.asset ?? null;
    const referenceContext = asset
      ? this.state.getReferenceContext(asset, this.nowFn().getTime(), this.riskLimits.staleFeedTimeoutMs)
      : null;
    const referenceMovement = asset ? this.triggerEngine.referenceMovement(asset) : null;
    const reference: NativeReferenceFeatures | null = referenceContext ? {
      asset: referenceContext.asset,
      venue: referenceContext.venue,
      freshness: referenceContext.freshness,
      freshnessMs: referenceContext.freshnessMs,
      recentMoveBps: referenceMovement?.moveBps ?? null,
      recentVolatilityBps: referenceContext.recentVolatilityBps,
      referenceWindowMs: referenceMovement?.windowMs ?? null,
      referenceSourceTimestamp: referenceMovement?.sourceTimestamp ?? null,
    } : null;
    const native = evaluateNativeDecision({
      trigger, up: pair.UP, down: pair.DOWN, reference,
      referenceRequired: this.referenceRequired.get(trigger.marketId) === true,
      marketSynchronized: this.isMarketSynchronized(trigger.marketId),
      triggerProvenanceValid: this.isProvenanceCurrentlyValid(trigger.id, provenance),
      now: this.nowFn(), config: this.nativeDecisionConfig,
    });
    if (!native) return;

    const consensus = this.nativeCompatibilityDecision(trigger, native);
    this.state.latestConsensus = consensus;
    this.record("NATIVE_DECISION_COMPLETE", trigger.traceId, trigger.marketId, {
      authority: NATIVE_DETERMINISTIC_AUTHORITY,
      direction: native.direction, confidence: native.confidence,
      fairProbability: native.fairProbability, marketProbability: native.marketProbability,
      featureVector: native.featureVector, triggerId: trigger.id,
    }, asset);
    await this.persistReportInput(createJournalEvent({
      occurredAt: this.nowFn(), traceId: trigger.traceId, type: "CONSENSUS_DECISION", aggregateType: "consensus", aggregateId: consensus.id,
      payload: {
        ...consensus,
        authority: NATIVE_DETERMINISTIC_AUTHORITY,
        marketId: trigger.marketId, asset,
        fairProbability: native.fairProbability, marketProbability: native.marketProbability,
        expiresAt: native.expiresAt, featureVector: native.featureVector,
        reference: reference ? {
          asset: reference.asset, venue: reference.venue, freshness: reference.freshness,
          freshnessMs: reference.freshnessMs, recentMoveBps: reference.recentMoveBps,
          referenceWindowMs: reference.referenceWindowMs,
          referenceSourceTimestamp: reference.referenceSourceTimestamp,
          recentVolatilityBps: reference.recentVolatilityBps,
        } : null,
      },
    }));
    for (const disagreement of this.triggerEngine.ingestConsensus(consensus, trigger.marketId, asset)) this.admitTrigger(disagreement);

    await this.evaluateTrade(trigger, consensus, native);
  }

  private nativeCompatibilityDecision(trigger: TriggerEvent, native: NativeDecision): NonNullable<EngineStateStore["latestConsensus"]> {
    return {
      id: createId("native_decision"), traceId: trigger.traceId, triggerId: trigger.id,
      consensusDirection: native.direction, consensusConfidence: native.confidence,
      agentAgreement: 1, signalFreshness: 1, conflicts: [],
      estimatedEdge: native.fairProbability - native.marketProbability,
      evidenceIds: native.evidenceIds, calculatedAt: this.nowFn().toISOString(),
      authority: NATIVE_DETERMINISTIC_AUTHORITY,
      fairProbability: native.fairProbability, marketProbability: native.marketProbability,
      featureVector: { ...native.featureVector },
    };
  }

  /** Bounded, role-scoped context for the opt-in internal xAI runtime. */
  private grokRoleContext(request: GrokActivationRequest, role: ExternalAgentRoleId, allowPortfolioContext: boolean): GrokRoleContext {
    const trigger = this.activeTriggers.get(request.triggerId)
      ?? (request.researchRefresh ? this.scoutRefreshContexts.get(request.triggerId) : undefined);
    if (!trigger) throw new Error("RUNTIME_TRIGGER_UNAVAILABLE");
    const synchronized = request.marketId ? this.isMarketSynchronized(request.marketId) : true;
    // The adapter's token-sync index is the trusted-book authority. A raw
    // snapshot map is never sufficient after a gap or reconnect.
    const snapshots = request.marketId && synchronized
      ? [...this.state.marketSnapshots.values()]
        .filter((item) => item.marketId === request.marketId)
        .slice(-2)
        .map((item) => boundedMarketSnapshot(item))
      : [];
    const scoutResearch = request.marketId && role === "fair_probability_analyst"
      ? this.hotResearchCache.freshFor(request.marketId, request.asset ?? trigger.asset ?? null, SCOUT_ROLES)
      : [];
    const ledger = this.ledger.snapshot();
    const selectedPosition = request.marketId ? this.ledger.positions.get(request.marketId) : undefined;
    const scopedPositions = role === "risk_hedge_analyst" && allowPortfolioContext && selectedPosition
      ? [boundedPosition(selectedPosition)]
      : [];
    const exposure = request.marketId && role === "risk_hedge_analyst" && allowPortfolioContext
      ? computePaperExposures(this.ledger.positions.values(), request.marketId, this.assetByMarket, this.nowFn().getTime())
      : undefined;
    const hedgeState = selectedPosition && role === "risk_hedge_analyst" && allowPortfolioContext
      ? {
        marketId: selectedPosition.marketId,
        pairedQuantity: selectedPosition.pairedQuantity,
        unpairedQuantity: selectedPosition.unpairedQuantity,
        unpairedOutcome: selectedPosition.unpairedOutcome,
        combinedPairCost: selectedPosition.combinedPairCost,
        obligation: Number(selectedPosition.unpairedQuantity) > 0 ? "HEDGE_REQUIRED" : "PAIRED",
      }
      : null;
    const recentStrategyRisk = role === "strategy_supervisor" ? this.state.feed
      .filter((item) => item.type.startsWith("RISK_") || item.type.startsWith("CONSENSUS_") || item.type.startsWith("PAPER_"))
      .slice(0, 20)
      .map((item) => boundedStrategyRiskSummary(item)) : [];
    const experiment = this.experiment ? {
      id: this.experiment.id,
      status: this.experiment.status,
      startedAt: this.experiment.startedAt,
      completedAt: this.experiment.completedAt,
      startValidationPending: this.experiment.startValidationPending,
    } : null;
    return {
      trigger: boundedTrigger(trigger),
      market: request.marketId ? {
        marketId: request.marketId,
        asset: request.asset ?? trigger.asset ?? null,
        synchronized,
        snapshots,
        features: synchronized ? deterministicMarketFeatures(snapshots) : {},
      } : null,
      reference: request.asset && ["crypto_market_scanner", "news_x_intelligence_scanner", "fair_probability_analyst", "risk_hedge_analyst"].includes(role)
        ? boundedReference(this.state.getReferenceContext(request.asset, this.nowFn().getTime(), this.riskLimits.staleFeedTimeoutMs)) : null,
      portfolio: {
        cash: allowPortfolioContext && role === "risk_hedge_analyst" ? ledger.cash : "[omitted]",
        equity: allowPortfolioContext && role === "risk_hedge_analyst" ? ledger.equity : "[omitted]",
        reservedCash: allowPortfolioContext && role === "risk_hedge_analyst" ? ledger.reservedCash : "[omitted]",
        positions: scopedPositions,
        ...(exposure ? { exposure } : {}),
        hedgeState,
      },
      experiment,
      recentStrategyRisk,
      scoutResearch,
      provenance: role === "fair_probability_analyst" ? {
        triggerId: trigger.id,
        sourceEventIds: trigger.sourceEventIds.slice(0, 32),
        evidenceIds: (trigger.evidenceIds ?? []).slice(0, 32),
        scoutKeys: scoutResearch.map((entry) => entry.key),
        correlatedFeatureIds: [...new Set(scoutResearch.flatMap((entry) => Array.isArray(entry.featureIds) ? entry.featureIds : []))].slice(0, 32),
      } : undefined,
      researchToolsAvailable: false,
    };
  }

  private async onGrokRoleResult(request: GrokActivationRequest, outcome: RoleRunOutcome): Promise<void> {
    if (!isScoutRole(outcome.role) || !request.marketId) return;
    if (!outcome.submissionResult || !["accepted", "duplicate"].includes(outcome.submissionResult.status)) return;
    const trigger = this.activeTriggers.get(request.triggerId) ?? this.scoutRefreshContexts.get(request.triggerId);
    if (!trigger) return;
    const admission = this.admitScoutResearch(outcome.submission, trigger, request.researchRefresh === true);
    if (!admission.ok) return;
  }

  private scoutAdmissionStatus(submission: GrokSignalSubmission, trigger: TriggerEvent, allowNonActionable = false): "accepted" | "stale" | "unsupported_evidence" {
    if (!trigger.marketId || !rolesForTriggerKind(trigger.kind, trigger.severity).filter(isScoutRole).includes(submission.agentId)) return "stale";
    if (!this.entryQualified.has(trigger.marketId)) return "stale";
    if (!this.isMarketSynchronized(trigger.marketId)) return "stale";
    if (!allowNonActionable && !this.isTriggerActionable(trigger.id)) return "stale";
    const expiry = new Date(submission.output.expiresAt).getTime();
    if (!Number.isFinite(expiry) || expiry <= this.nowFn().getTime() || expiry > new Date(trigger.expiresAt).getTime()) return "stale";
    const suppliedEvidence = new Set<string>([
      ...(trigger.evidenceIds ?? []).map(String),
      ...trigger.sourceEventIds.map(String),
      ...[...this.state.marketSnapshots.values()]
        .filter((item) => item.marketId === trigger.marketId && this.isTokenSynchronized(item.tokenId))
        .flatMap((item) => [item.eventId, item.traceId].map(String)),
    ]);
    const evidenceIds = Array.isArray(submission.output.evidenceIds) ? submission.output.evidenceIds.map(String) : [];
    return evidenceIds.some((id) => !suppliedEvidence.has(id)) ? "unsupported_evidence" : "accepted";
  }

  private alphaAdmissionStatus(submission: GrokSignalSubmission, trigger: TriggerEvent): "accepted" | "stale" | "unsupported_evidence" {
    if (!trigger.marketId || !this.entryQualified.has(trigger.marketId) || !this.isMarketSynchronized(trigger.marketId) || !this.isTriggerActionable(trigger.id)) return "stale";
    const suppliedEvidence = new Set<string>([
      ...(trigger.evidenceIds ?? []).map(String),
      ...trigger.sourceEventIds.map(String),
      ...[...this.state.marketSnapshots.values()]
        .filter((item) => item.marketId === trigger.marketId && this.isTokenSynchronized(item.tokenId))
        .flatMap((item) => [item.eventId, item.traceId].map(String)),
    ]);
    const requiredScoutRoles = rolesForTriggerKind(trigger.kind, trigger.severity).filter(isScoutRole);
    const freshScout = this.hotResearchCache.freshFor(trigger.marketId, trigger.asset ?? null, requiredScoutRoles);
    if (freshScout.length !== requiredScoutRoles.length) return "stale";
    for (const entry of freshScout) {
      for (const id of [...entry.evidenceIds, ...entry.sourceEventIds, ...entry.featureIds, ...entry.derivedFrom]) suppliedEvidence.add(id);
    }
    const evidenceIds = Array.isArray(submission.output.evidenceIds) ? submission.output.evidenceIds.map(String) : [];
    return evidenceIds.some((id) => !suppliedEvidence.has(id)) ? "unsupported_evidence" : "accepted";
  }

  private admitScoutResearch(submission: GrokSignalSubmission, trigger: TriggerEvent, allowNonActionable = false): { ok: true } | { ok: false; status: "stale" | "unsupported_evidence" } {
    const status = this.scoutAdmissionStatus(submission, trigger, allowNonActionable);
    if (status !== "accepted") return { ok: false, status };
    const output = submission.output as Record<string, unknown>;
    const evidenceIds = Array.isArray(output.evidenceIds) ? output.evidenceIds.map(String) : [];
    const sourceEventIds = trigger.sourceEventIds.map(String);
    const snapshots = [...this.state.marketSnapshots.values()].filter((item) => item.marketId === trigger.marketId && this.isTokenSynchronized(item.tokenId));
    const reference = trigger.asset
      ? this.state.getReferenceContext(trigger.asset, this.nowFn().getTime(), this.riskLimits.staleFeedTimeoutMs)
      : null;
    this.hotResearchCache.put({
      role: submission.agentId,
      marketId: trigger.marketId!,
      asset: trigger.asset ?? submission.asset ?? null,
      triggerId: trigger.id,
      traceId: trigger.traceId,
      generatedAt: this.nowFn().toISOString(),
      expiresAt: String(output.expiresAt),
      evidenceIds,
      sourceEventIds,
      featureIds: [
        `feature:${String(trigger.kind)}:${trigger.marketId}`,
        ...evidenceIds.map((id) => `evidence:${id}`),
        ...sourceEventIds.map((id) => `source:${id}`),
      ],
      derivedFrom: [...sourceEventIds, ...evidenceIds],
      marketSequence: snapshots.length > 0 ? Math.max(...snapshots.map((item) => Number(item.sequence ?? 0))) : null,
      referenceTimestamp: reference?.lastMessageAt ?? null,
      output,
    });
    return { ok: true };
  }

  /** The only ingress for external Grok research; it cannot create orders directly. */
  async submitGrokSignal(raw: GrokSignalSubmission, clientId: string, mode: GrokSubmissionMode = "mcp"): Promise<GrokSubmissionResult> {
    this.assertResearchIntegrity();
    const researchRefresh = mode === "refresh";
    let submission: GrokSignalSubmission;
    try { submission = grokSignalSubmissionSchema.parse(raw); }
    catch {
      this.state.recordGrokRejection("rejected");
      this.metrics.grokSignals.inc({ agent: "unknown", status: "malformed" });
      return { status: "malformed" };
    }
    if (submission.agentId !== submission.role || clientId !== submission.agentId) {
      this.state.recordGrokRejection("rejected");
      this.metrics.grokSignals.inc({ agent: submission.agentId, status: "identity_mismatch" });
      return { status: "identity_mismatch" };
    }
    const rejectCompleted = (): { status: "stale" } => {
      this.state.recordGrokRejection("rejected");
      this.metrics.grokSignals.inc({ agent: submission.agentId, status: "stale" });
      return { status: "stale" };
    };
    const ingress = this.beginResearchIngress();
    try {
      if (this.researchIngressStale(ingress.generation)) return rejectCompleted();
      const now = this.nowFn();
      const trigger = this.activeTriggers.get(submission.triggerId)
        ?? (researchRefresh ? this.scoutRefreshContexts.get(submission.triggerId) : undefined);
      if (!trigger) { this.state.recordGrokRejection("rejected"); this.metrics.grokSignals.inc({ agent: submission.agentId, status: "unknown_trigger" }); return { status: "unknown_trigger" }; }
      if (trigger.traceId !== submission.traceId) { this.state.recordGrokRejection("rejected"); this.metrics.grokSignals.inc({ agent: submission.agentId, status: "trace_mismatch" }); return { status: "trace_mismatch" }; }
      if (trigger.marketId && trigger.marketId !== submission.marketId) {
        this.state.recordGrokRejection("rejected"); this.metrics.grokSignals.inc({ agent: submission.agentId, status: "market_mismatch" }); return { status: "market_mismatch" };
      }
      if (submission.asset && trigger.asset && submission.asset !== trigger.asset) {
        this.state.recordGrokRejection("rejected"); this.metrics.grokSignals.inc({ agent: submission.agentId, status: "asset_mismatch" }); return { status: "asset_mismatch" };
      }
      const submittedAt = new Date(submission.timestamp).getTime();
      const maxSkewMs = 5 * 60_000;
      if (!Number.isFinite(submittedAt) || submittedAt > now.getTime() + 60_000 || submittedAt < now.getTime() - maxSkewMs) {
        this.state.recordGrokRejection("rejected"); this.metrics.grokSignals.inc({ agent: submission.agentId, status: "replay_rejected" }); return { status: "replay_rejected" };
      }
      if (new Date(trigger.expiresAt).getTime() <= now.getTime() || new Date(submission.output.expiresAt).getTime() <= now.getTime()) {
        this.state.recordGrokRejection("rejected"); this.metrics.grokSignals.inc({ agent: submission.agentId, status: "stale" }); return { status: "stale" };
      }
      if (!researchRefresh && !this.isTriggerActionable(trigger.id)) {
        this.state.recordGrokRejection("rejected"); this.metrics.grokSignals.inc({ agent: submission.agentId, status: "stale" }); return { status: "stale" };
      }
      // Scout MCP ingress is admitted only from the trusted synchronized
      // trigger/cache domain. Reject before claiming or journaling anything
      // when the role, evidence, feed, or freshness contract is not met.
      if (mode === "scout") {
        const scoutAdmission = this.scoutAdmissionStatus(submission, trigger);
        if (scoutAdmission !== "accepted") {
          this.state.recordGrokRejection("rejected");
          this.metrics.grokSignals.inc({ agent: submission.agentId, status: scoutAdmission });
          return { status: scoutAdmission };
        }
      }
      if (mode === "alpha") {
        const alphaAdmission = this.alphaAdmissionStatus(submission, trigger);
        if (alphaAdmission !== "accepted") {
          this.state.recordGrokRejection("rejected");
          this.metrics.grokSignals.inc({ agent: submission.agentId, status: alphaAdmission });
          return { status: alphaAdmission };
        }
      }
      if (this.researchIngressStale(ingress.generation)) return rejectCompleted();
      if (this.grokClaimHold) await this.grokClaimHold;
      if (this.researchIngressStale(ingress.generation)) return rejectCompleted();
      const idempotencyKey = `${clientId}:${submission.submissionId}`;
      const idleAdmission = !this.isExperimentWindowOpen();
      let idleReservation = false;
      const duplicate = (): { status: "duplicate" } => {
        this.state.recordGrokRejection("duplicate");
        this.metrics.grokSignals.inc({ agent: submission.agentId, status: "duplicate" });
        return { status: "duplicate" };
      };
      if (researchRefresh) {
        if (idleAdmission || !isScoutRole(submission.agentId)) {
          this.state.recordGrokRejection("rejected");
          this.metrics.grokSignals.inc({ agent: submission.agentId, status: "stale" });
          return { status: "stale" };
        }
        this.pruneScoutRefreshState(now.getTime());
        if (this.scoutRefreshSubmissionKeys.has(idempotencyKey)) return duplicate();
        this.scoutRefreshSubmissionKeys.set(idempotencyKey, now.getTime());
      } else if (idleAdmission) {
        // Idle monitoring intentionally does not create durable idempotency
        // claims because its canonical accepted fact is not persisted. A
        // bounded live reservation still makes exact retries deterministic and
        // suppresses concurrent duplicates within this process.
        idleReservation = this.reserveIdleGrokSubmission(idempotencyKey, now.getTime());
        if (!idleReservation) return duplicate();
        this.assertResearchIntegrity();
      } else {
        // A completed idle acceptance remains a bounded process-local tombstone
        // across the lifecycle boundary. It has no durable canonical fact by
        // design, so do not let the first active retry create a second claim
        // and accepted fact for the same submission.
        this.pruneIdleGrokSubmissions(now.getTime());
        if (this.idleGrokSubmissions.has(idempotencyKey)) return duplicate();
        const claimed = await this.journal.claimIdempotency("grok_signal_submission", idempotencyKey);
        this.assertResearchIntegrity();
        if (!claimed) {
          const canonical = await this.proveGrokSubmissionCanonicalFacts(submission, clientId);
          if (mode === "scout") {
            const rehydrated = this.admitScoutResearch(canonical, trigger);
            void rehydrated;
          }
          return duplicate();
        }
      }

      const acceptSubmission = async (): Promise<GrokSubmissionResult> => {
        // An idle reservation may have waited while beginExperiment committed.
        // The boundary gate makes this recheck atomic with EXPERIMENT_STARTED:
        // either the idle submission accepts before the transition, or it is
        // promoted to the durable active claim before any canonical append.
        let promotedToActive = false;
        if (idleReservation && this.isExperimentWindowOpen()) {
          this.releaseIdleGrokSubmission(idempotencyKey);
          idleReservation = false;
          const promoted = await this.journal.claimIdempotency("grok_signal_submission", idempotencyKey);
          this.assertResearchIntegrity();
          if (!promoted) {
            const canonical = await this.proveGrokSubmissionCanonicalFacts(submission, clientId);
            if (mode === "scout") {
              const rehydrated = this.admitScoutResearch(canonical, trigger);
              void rehydrated;
            }
            return duplicate();
          }
          promotedToActive = true;
        }

        try {
        if (promotedToActive) {
          // beginExperiment advances the research-ingress generation. The promoted
          // submission is now active ingress for the new experiment; completion
          // and generation fences still reject stale work.
          ingress.generation = this.researchIngressGeneration;
        }
        // Recheck the experiment-scoped fence immediately before irreversible accept.
        if (this.researchIngressStale(ingress.generation) || (!researchRefresh && !this.isTriggerActionable(trigger.id))) {
          if (idleReservation) this.releaseIdleGrokSubmission(idempotencyKey);
          this.state.recordGrokRejection("rejected");
          this.metrics.grokSignals.inc({ agent: submission.agentId, status: "stale" });
          return { status: "stale" };
        }

        const signal: GrokSignalEnvelope = grokSignalEnvelopeSchema.parse({
          signalId: createId("grok_signal"), submissionId: submission.submissionId, traceId: submission.traceId,
          triggerId: submission.triggerId, correlationId: submission.correlationId, marketId: submission.marketId,
          asset: submission.asset ?? trigger.asset ?? null,
          agentId: submission.agentId, role: submission.role, provider: "grok_bot_mcp", clientId,
          receivedAt: now.toISOString(), timestamp: submission.timestamp, output: submission.output, resultingDecision: null,
        });
        if (this.researchIngressStale(ingress.generation)) {
          if (idleReservation) this.releaseIdleGrokSubmission(idempotencyKey);
          return rejectCompleted();
        }
        // This is the final admission linearization point for role-bound
        // research.  Once the canonical event is appended, a later feed loss
        // is ordinary runtime invalidation; it must not turn an accepted
        // journal fact into a stale response.
        if (mode === "scout") {
          const finalScoutAdmission = this.scoutAdmissionStatus(submission, trigger);
          if (finalScoutAdmission !== "accepted") {
            if (idleReservation) this.releaseIdleGrokSubmission(idempotencyKey);
            this.state.recordGrokRejection("rejected");
            this.metrics.grokSignals.inc({ agent: submission.agentId, status: finalScoutAdmission });
            return { status: finalScoutAdmission };
          }
        } else if (mode === "alpha") {
          const finalAlphaAdmission = this.alphaAdmissionStatus(submission, trigger);
          if (finalAlphaAdmission !== "accepted") {
            if (idleReservation) this.releaseIdleGrokSubmission(idempotencyKey);
            this.state.recordGrokRejection("rejected");
            this.metrics.grokSignals.inc({ agent: submission.agentId, status: finalAlphaAdmission });
            return { status: finalAlphaAdmission };
          }
        }
        if (researchRefresh) {
          // Scout refresh is a bounded in-memory research admission. It must
          // not create canonical report rows or an economic trigger.
          this.metrics.grokSignals.inc({ agent: signal.agentId, status: "accepted" });
          return { status: "accepted", signalId: signal.signalId };
        }
        await this.persistReportInput(createJournalEvent({ traceId: signal.traceId, type: "GROK_SIGNAL_ACCEPTED", aggregateType: "grok_signal", aggregateId: signal.signalId, payload: signal }));
        if (submission.agentId === "strategy_supervisor" && submission.output.proposedChange) {
          await this.persistReportInput(createJournalEvent({ traceId: signal.traceId, type: "STRATEGY_CHANGE_PROPOSED", aggregateType: "strategy_change", aggregateId: signal.signalId, payload: submission.output.proposedChange }));
        }
        this.assertResearchIntegrity();
        const byRole = this.signalsByTrigger.get(trigger.id) ?? new Map<ExternalAgentRoleId, GrokSignalEnvelope>();
        byRole.set(signal.agentId, signal);
        this.signalsByTrigger.set(trigger.id, byRole);
        this.signalReceivedTimes.push(now.getTime());
        this.state.addGrokSignal(signal);
        this.activation.ensureLatency(trigger);
        this.activation.noteSignal(trigger.id, now);
        this.state.setActivationLatency(this.activation.snapshot());
        this.metrics.grokSignals.inc({ agent: signal.agentId, status: "accepted" });
        this.record(`${signal.agentId.toUpperCase()}_COMPLETE`, signal.traceId, trigger.marketId, { signalId: signal.signalId, clientId, confidence: submission.output.confidence, expiresAt: submission.output.expiresAt }, trigger.asset ?? null);
        if (mode === "scout") {
          // Legacy scout admission remains available for diagnostics, but a
          // scout completion cannot wake an economic decision path.
          this.admitScoutResearch(submission, trigger);
        } else {
          // Legacy Alpha/MCP submissions are retained as auditable research
          // facts only. Native decisions are the sole economic authority.
        }
        if (idleReservation) this.completeIdleGrokSubmission(idempotencyKey, now.getTime());
        return { status: "accepted", signalId: signal.signalId };
      } catch (error) {
        if (idleReservation) this.releaseIdleGrokSubmission(idempotencyKey);
        throw error;
      }
      };
      if (idleReservation) {
        try {
          if (this.idleGrokAcceptanceHoldForTest) await this.idleGrokAcceptanceHoldForTest;
        } catch (error) {
          this.releaseIdleGrokSubmission(idempotencyKey);
          throw error;
        }
        return this.experimentBoundaryGate.run(acceptSubmission);
      }
      return acceptSubmission();
    } finally {
      this.endResearchIngress();
    }
  }

  private async proveGrokSubmissionCanonicalFacts(submission: GrokSignalSubmission, clientId: string): Promise<GrokSignalSubmission> {
    // A claim proves admission only, including across a crash before append.
    // Check every canonical fact required by this submission with bounded trace
    // pages (also works for old IDs). The proposal is linked to the accepted
    // signal by the same trace and aggregate identity.
    let acceptedEvent: JournalEvent | undefined;
    let canonicalSubmission: GrokSignalSubmission | undefined;
    let requiresStrategyProposal = false;
    const strategyProposalAggregateIds = new Set<string>();
    let after: Parameters<EventJournal["readTracePage"]>[1]["after"];
    do {
      const page = await this.journal.readTracePage(submission.traceId, { after, limit: 128 });
      for (const event of page.events) {
        if (event.type === "STRATEGY_CHANGE_PROPOSED" && event.traceId === submission.traceId) {
          strategyProposalAggregateIds.add(event.aggregateId);
        }
        if (!acceptedEvent && event.type === "GROK_SIGNAL_ACCEPTED"
          && event.traceId === submission.traceId
          && event.payload.clientId === clientId && event.payload.submissionId === submission.submissionId) {
          const candidate = this.canonicalGrokSubmission(event.payload);
          if (candidate) {
            acceptedEvent = event;
            canonicalSubmission = candidate;
            requiresStrategyProposal = candidate.agentId === "strategy_supervisor"
              && candidate.output.proposedChange !== null;
          }
        }
      }
      after = page.nextCursor ?? undefined;
      const proposalPersisted = acceptedEvent !== undefined && strategyProposalAggregateIds.has(acceptedEvent.aggregateId);
      if (acceptedEvent !== undefined && (!requiresStrategyProposal || proposalPersisted)) break;
    } while (after);
    this.assertResearchIntegrity();
    const proposalPersisted = acceptedEvent !== undefined && strategyProposalAggregateIds.has(acceptedEvent.aggregateId);
    if (acceptedEvent === undefined || canonicalSubmission === undefined || (requiresStrategyProposal && !proposalPersisted)) {
      // This can also be a concurrent claim whose first caller is still
      // persisting one of its canonical facts. Reject the unproven duplicate
      // without poisoning that caller. An actual total persistence failure
      // installs the sticky fail-closed state in safeAppend; restart still
      // cannot accept this claim.
      throw new Error("GROK_SUBMISSION_CANONICAL_FACT_MISSING");
    }
    return canonicalSubmission;
  }

  /** Reconstruct the immutable submission body from its accepted canonical fact. */
  private canonicalGrokSubmission(payload: Record<string, unknown>): GrokSignalSubmission | undefined {
    const parsed = grokSignalSubmissionSchema.safeParse({
      submissionId: payload.submissionId,
      triggerId: payload.triggerId,
      traceId: payload.traceId,
      correlationId: payload.correlationId,
      marketId: payload.marketId,
      asset: payload.asset,
      timestamp: payload.timestamp,
      agentId: payload.agentId,
      role: payload.role,
      output: payload.output,
    });
    return parsed.success ? parsed.data : undefined;
  }

  private assertResearchIntegrity(): void {
    if (this.overload.hasIntegrityFault) throw new Error("REPORT_SOURCE_INTEGRITY_FAIL_CLOSED");
  }

  /** Public for deterministic trigger ingestion and focused integration tests. */
  async registerTriggerForResearch(trigger: TriggerEvent): Promise<void> {
    const result = this.admitTrigger(triggerEventSchema.parse(trigger));
    if (result.admitted) await result.done;
  }

  private async evaluateTrade(trigger: TriggerEvent, consensus: NonNullable<EngineStateStore["latestConsensus"]>, native: NativeDecision): Promise<void> {
    if (this.researchIngressBlocked()) return;
    if (!this.isTriggerActionable(trigger.id)) return;
    let pair = this.snapshots.get(trigger.marketId ?? "");
    if (!pair) return;
    let position = this.ledger.positions.get(trigger.marketId ?? "") ?? null;
    if (this.experiment) {
      const next = refreshExperiment(this.soakLifecycleManifest(this.experiment), this.nowFn(), {
        openPositions: openPositionsRemain(this.ledger.positions.values()),
        unresolvedCount: this.countUnresolvedPositions(),
      });
      if (next.status !== this.experiment.status) await this.commitExperimentStatus(next);
    }

    // Experiment refresh is allowed to await. Re-read the economic state after
    // that boundary before deriving a candidate.
    pair = this.snapshots.get(trigger.marketId ?? "");
    if (!pair) return;
    position = this.ledger.positions.get(trigger.marketId ?? "") ?? null;

    let decisionConsensus = consensus;
    let decisionNative = native;
    let candidate = this.strategy.evaluate({
      consensus: decisionConsensus, snapshots: { UP: pair.UP ?? null, DOWN: pair.DOWN ?? null }, position,
      maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd, minimumLockedPairEdge: this.riskLimits.normalHedgeMinLockedPairEdge,
    });
    if (!candidate) return;
    if (candidate.intent === "HEDGE_SECOND_LEG" && this.soakBlocksPostEntry(candidate.marketId)) return;
    // During the profit-trailing soak, one controller owns ALL second-leg timing
    // so best-ask normal hedges cannot race the full-executable .12/trailing policy.
    if (candidate.intent === "HEDGE_SECOND_LEG" && this.profitTrailingPairSoakEnabled()) return;
    if (
      candidate.intent === "OPEN_FIRST_LEG"
      && this.firstLegLifecycleBlocked(candidate.marketId)
    ) return;
    // EXPERIMENT_STARTED is applied before its post-commit validation. Keep
    // first-leg admission fenced until the validation marker is durable.
    if (candidate.intent === "OPEN_FIRST_LEG" && this.experimentStartPending) return;
    if (!this.adapterTokenSynchronized(candidate.tokenId)) return;
    if (candidate.intent === "OPEN_FIRST_LEG" && this.overload.state === "FAIL_CLOSED") return;
    if (candidate.intent === "OPEN_FIRST_LEG" && !experimentAllowsFirstLeg(this.experiment, this.nowFn())) return;
    if (candidate.intent === "OPEN_FIRST_LEG" && !this.entryQualified.has(candidate.marketId)) return;
    if (candidate.intent === "OPEN_FIRST_LEG" && !this.isMarketSynchronized(candidate.marketId)) return;
    if (candidate.intent === "OPEN_FIRST_LEG" && assetNeedsReliableReference(this.referenceRequired.get(candidate.marketId), this.referenceUnreliable, this.assetByMarket.get(candidate.marketId))) return;
    if (candidate.intent === "HEDGE_SECOND_LEG" && !experimentAllowsHedge(this.experiment)) return;
    // RESCUE_EXCLUSIVE (PR2): once protective eligibility is reached, the
    // current unpaired residual has exactly ONE risk-reduction authority. The
    // normal profitable-hedge path stops admitting new competing BUYs and the
    // rescue arbiter alone decides between A (SELL) and B (pair-lock BUY) from
    // fresh state. This preserves the pre-eligibility V1 exact behavior.
    if (candidate.intent === "HEDGE_SECOND_LEG") {
      const hedgePositionNow = this.ledger.positions.get(candidate.marketId);
      if (hedgePositionNow?.unpairedOutcome) {
        const hedgeEligibility = this.protectiveDeRiskEligibility(
          candidate.marketId,
          hedgePositionNow.unhedgedStartedAt ?? hedgePositionNow.openedAt,
          this.nowFn().getTime(),
        );
        if (hedgeEligibility.eligible) {
          const suppressedMarketId = candidate.marketId;
          this.record("HEDGE_SUPPRESSED_RESCUE_EXCLUSIVE", trigger.traceId, suppressedMarketId, {
            unpairedOutcome: hedgePositionNow.unpairedOutcome,
            effectiveProtectiveDeadlineMs: hedgeEligibility.effectiveDeadlineMs,
            marketEndMs: hedgeEligibility.marketEndMs,
          }, this.assetByMarket.get(suppressedMarketId) ?? null);
          void this.maybeProtectiveDeRisk(suppressedMarketId).catch((error) => {
            this.logger.error({ err: error, marketId: suppressedMarketId }, "rescue arbitration attempt failed");
          });
          return;
        }
      }
    }

    // Database health is an async hard-safety prerequisite. Resolve it before
    // the final first-leg economic capture so a slow health check cannot leave
    // fair value tied to an older book.
    const databaseAvailable = await this.journal.healthCheck();

    let fairProvenance: Record<string, unknown> | null = null;
    if (candidate.intent === "OPEN_FIRST_LEG") {
      if (this.researchIngressBlocked() || !this.isTriggerActionable(trigger.id)) return;
      if (this.experimentStartPending || this.overload.state === "FAIL_CLOSED") return;
      if (!experimentAllowsFirstLeg(this.experiment, this.nowFn())) return;
      if (!this.entryQualified.has(candidate.marketId) || !this.isMarketSynchronized(candidate.marketId)) return;
      if (this.firstLegLifecycleBlocked(candidate.marketId)) return;

      const currentPair = this.snapshots.get(candidate.marketId);
      if (!currentPair?.UP || !currentPair.DOWN) return;
      const currentAsset = trigger.asset
        ?? this.assetByMarket.get(candidate.marketId)
        ?? currentPair.UP.asset
        ?? currentPair.DOWN.asset
        ?? null;
      if (assetNeedsReliableReference(this.referenceRequired.get(candidate.marketId), this.referenceUnreliable, currentAsset)) return;

      const referenceContext = currentAsset
        ? this.state.getReferenceContext(currentAsset, this.nowFn().getTime(), this.riskLimits.staleFeedTimeoutMs)
        : null;
      const referenceMovement = currentAsset ? this.triggerEngine.referenceMovement(currentAsset) : null;
      const currentReference: NativeReferenceFeatures | null = referenceContext ? {
        asset: referenceContext.asset,
        venue: referenceContext.venue,
        freshness: referenceContext.freshness,
        freshnessMs: referenceContext.freshnessMs,
        recentMoveBps: referenceMovement?.moveBps ?? null,
        recentVolatilityBps: referenceContext.recentVolatilityBps,
        referenceWindowMs: referenceMovement?.windowMs ?? null,
        referenceSourceTimestamp: referenceMovement?.sourceTimestamp ?? null,
      } : null;
      const currentProvenance = this.triggerProvenance.get(trigger.id);
      if (!currentProvenance || !this.isProvenanceCurrentlyValid(trigger.id, currentProvenance)) return;

      const revalidatedAt = this.nowFn();
      const revalidatedNative = evaluateNativeDecision({
        trigger,
        up: currentPair.UP,
        down: currentPair.DOWN,
        reference: currentReference,
        referenceRequired: this.referenceRequired.get(candidate.marketId) === true,
        marketSynchronized: true,
        triggerProvenanceValid: true,
        now: revalidatedAt,
        config: this.nativeDecisionConfig,
      });
      if (
        !revalidatedNative
        || (revalidatedNative.direction !== "UP" && revalidatedNative.direction !== "DOWN")
        || revalidatedNative.direction !== native.direction
      ) {
        this.record("FIRST_LEG_REVALIDATION_REJECTED", trigger.traceId, candidate.marketId, {
          triggerId: trigger.id,
          originalDirection: native.direction,
          revalidatedDirection: revalidatedNative?.direction ?? null,
          originalFairProbability: native.fairProbability,
          revalidatedFairProbability: revalidatedNative?.fairProbability ?? null,
          reason: "DECISION_STATE_CHANGED",
        }, currentAsset);
        return;
      }

      decisionNative = revalidatedNative;
      decisionConsensus = {
        ...consensus,
        consensusDirection: revalidatedNative.direction,
        consensusConfidence: revalidatedNative.confidence,
        estimatedEdge: revalidatedNative.fairProbability - revalidatedNative.marketProbability,
        fairProbability: revalidatedNative.fairProbability,
        marketProbability: revalidatedNative.marketProbability,
        featureVector: { ...revalidatedNative.featureVector },
        calculatedAt: revalidatedAt.toISOString(),
      };
      position = this.ledger.positions.get(candidate.marketId) ?? null;
      const revalidatedCandidate = this.strategy.evaluate({
        consensus: decisionConsensus,
        snapshots: { UP: currentPair.UP, DOWN: currentPair.DOWN },
        position,
        maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd,
        minimumLockedPairEdge: this.riskLimits.normalHedgeMinLockedPairEdge,
      });
      if (
        !revalidatedCandidate
        || revalidatedCandidate.intent !== "OPEN_FIRST_LEG"
        || revalidatedCandidate.outcome !== revalidatedNative.direction
      ) {
        this.record("FIRST_LEG_REVALIDATION_REJECTED", trigger.traceId, candidate.marketId, {
          triggerId: trigger.id,
          originalDirection: native.direction,
          revalidatedDirection: revalidatedNative.direction,
          reason: "NO_FRESH_FIRST_LEG_CANDIDATE",
        }, currentAsset);
        return;
      }
      candidate = revalidatedCandidate;
      pair = currentPair;
      if (!this.adapterTokenSynchronized(candidate.tokenId)) return;

      const snapshotProvenance = (snapshot: MarketSnapshot) => ({
        eventId: snapshot.eventId,
        tokenId: snapshot.tokenId,
        sequence: snapshot.sequence,
        hash: snapshot.hash,
        sourceTimestamp: snapshot.sourceTimestamp,
        receivedAt: snapshot.receivedAt,
        wsCallbackAt: snapshot.wsCallbackAt ?? null,
      });
      fairProvenance = {
        revalidated: true,
        revalidatedAt: revalidatedAt.toISOString(),
        originalDirection: native.direction,
        revalidatedDirection: revalidatedNative.direction,
        up: snapshotProvenance(currentPair.UP),
        down: snapshotProvenance(currentPair.DOWN),
        reference: {
          venue: currentReference?.venue ?? null,
          sourceTimestamp: currentReference?.referenceSourceTimestamp ?? null,
          receivedAt: currentAsset ? this.state.referenceLastMessageAt(currentAsset) : null,
        },
      };
    }

    const snapshot = candidate.intent === "OPEN_FIRST_LEG"
      ? pair[candidate.outcome]
      : this.state.marketSnapshots.get(candidate.tokenId);
    if (!snapshot || snapshot.tokenId !== candidate.tokenId) return;
    if (candidate.intent === "OPEN_FIRST_LEG") {
      const reason = this.firstLegAdmissionReason(candidate, snapshot);
      if (reason) {
        this.metrics.riskVetoes.inc({ reason });
        this.record("FIRST_LEG_REVALIDATION_REJECTED", trigger.traceId, candidate.marketId, { reason, timing: bookTiming(snapshot, this.nowFn().getTime()) });
        return;
      }
    }

    let executableFair: ReturnType<typeof executableFairEdge> = null;
    if (candidate.intent === "OPEN_FIRST_LEG" && (decisionNative.direction === "UP" || decisionNative.direction === "DOWN")) {
      executableFair = executableFairEdge({
        fair: {
          direction: decisionNative.direction,
          fairProbability: decisionNative.fairProbability,
          marketProbability: decisionNative.marketProbability,
        },
        consensus: decisionConsensus,
        snapshot,
        quantity: candidate.quantity,
        limitPrice: candidate.limitPrice,
      });
      if (!executableFair) return;
      candidate.rawEdgeUsdPerShare = executableFair.rawEdgeUsdPerShare;
      candidate.rawExpectedEdge = executableFair.rawEdgeUsdPerShare;
      decisionConsensus.estimatedEdge = executableFair.rawEdgeUsdPerShare;
    }

    const now = this.nowFn();
    const signalExpiry = new Date(decisionNative.expiresAt);
    const { decision, duplicateKey } = this.decideWithRuntimeBasis({
      candidate,
      consensus: decisionConsensus,
      triggerId: trigger.id,
      signalExpiresAt: signalExpiry,
      snapshot,
      pair,
      now,
      databaseAvailable,
      assetOverride: trigger.asset,
    });
    const asset = trigger.asset ?? this.assetByMarket.get(candidate.marketId) ?? snapshot.asset ?? null;
    if (candidate.intent === "OPEN_FIRST_LEG") {
      const entryReason = firstLegEntryFilterReason(decision.executablePrice, decision.expectedROI);
      if (entryReason) {
        decision.accepted = false; decision.order = null; decision.risk.accepted = false;
        if (!decision.risk.vetoes.includes(entryReason)) decision.risk.vetoes.push(entryReason);
      } else if (decision.order && executableFair) {
        decision.order.entryAdmission = { fairProbability: executableFair.fairProbabilityForTradedOutcome, signalExpiresAt: decisionNative.expiresAt };
      }
    }
    this.state.latestRisk = decision.risk;
    const executionStyle = decision.order?.orderType ?? this.env.PAPER_EXECUTION_STYLE;
    const calibration = executableFair ? {
      authority: NATIVE_DETERMINISTIC_AUTHORITY,
      /** Compatibility display fields; executable fields below are authoritative. */
      fairProbability: decisionNative.fairProbability,
      marketProbability: decisionNative.marketProbability,
      featureVector: decisionNative.featureVector,
      fairProbabilityForTradedOutcome: executableFair.fairProbabilityForTradedOutcome,
      tradedOutcome: executableFair.consensusOutcome,
      usedComplement: executableFair.usedComplement,
      executionStyle,
      ...(executionStyle === "MAKER"
        ? {
          makerLimitPrice: decision.order?.requestedPrice ?? snapshot.bids[0]?.price ?? candidate.limitPrice,
          takerReferenceVwap: String(executableFair.executablePrice),
        }
        : {
          takerExecutableVwap: String(executableFair.executablePrice),
          currentExecutablePrice: String(executableFair.executablePrice),
        }),
      rawEdgeUsdPerShare: executableFair.rawEdgeUsdPerShare,
      feeUsdTotal: decision.feeUsdTotal,
      slippageUsdTotal: decision.slippageUsdTotal,
      costUsdPerShare: decision.costUsdPerShare,
      netEvUsdPerShare: decision.netEvUsdPerShare,
      grossEvUsdTotal: decision.grossEvUsdTotal,
      netEvUsdTotal: decision.netEvUsdTotal,
      executableNotionalUsd: decision.executableNotionalUsd,
      expectedROI: decision.expectedROI,
      quantity: decision.quantity,
      decisionTimestamp: now.toISOString(),
      evidenceIds: decisionNative.evidenceIds,
      expiresAt: decisionNative.expiresAt,
      fairProvenance,
    } : {
      authority: NATIVE_DETERMINISTIC_AUTHORITY,
      fairProbability: decisionNative.fairProbability,
      marketProbability: decisionNative.marketProbability,
      featureVector: decisionNative.featureVector,
      evidenceIds: decisionNative.evidenceIds,
      expiresAt: decisionNative.expiresAt,
    };
    await this.persistReportInput(createJournalEvent({ traceId: trigger.traceId, type: "RISK_EVALUATION", aggregateType: "risk", aggregateId: decision.risk.id, payload: {
      ...decision.risk, marketId: trigger.marketId, asset,
      executablePrice: decision.executablePrice,
      ...(calibration ? { alpha: calibration } : {}),
    } }));
    if (!decision.accepted || !decision.order) {
      for (const reason of decision.risk.vetoes) this.metrics.riskVetoes.inc({ reason });
      this.record("RISK_VETO", trigger.traceId, trigger.marketId, { reasons: decision.risk.vetoes, netEvUsdPerShare: decision.netEvUsdPerShare }, asset);
      return;
    }
    this.record("RISK_ACCEPT", trigger.traceId, trigger.marketId, { orderId: decision.order.id, netEvUsdPerShare: decision.netEvUsdPerShare }, asset);
    if(candidate.intent==="OPEN_FIRST_LEG" && await this.trySubmitDualMakerPair(decision.order,duplicateKey,{UP:pair.UP,DOWN:pair.DOWN}))return;
    await this.submitPaperOrder(decision.order, snapshot, duplicateKey, candidate.intent);
  }

  private riskReductionBusy(marketId: string): boolean {
    const owner = this.riskReductionOwners.get(marketId);
    const position = this.ledger.positions.get(marketId);
    return this.riskReductionInFlight.has(marketId)
      || Boolean(owner && position && owner.unhedgedStartedAt === unhedgedEpisode(position));
  }

  private ownershipAudit(owner: RiskReductionOwnership, type: string, reason?: string): DerivedAuditRecord {
    return {
      id: derivedAuditEventId(type, owner.order.id), type,
      aggregateType: "paper_order", aggregateId: owner.order.id, traceId: owner.order.traceId,
      payload: { marketId: owner.marketId, orderId: owner.order.id, intent: owner.intent,
        unhedgedStartedAt: owner.unhedgedStartedAt, admittedResidualQuantity: owner.admittedResidualQuantity,
        state: type === "RISK_REDUCTION_ADMITTED" ? "ADMITTED" : type === "RISK_REDUCTION_COMPLETED" ? "COMPLETED" : "FAILED",
        experimentId: this.experiment?.id ?? null,
        ...(reason ? { reason } : {}) },
    };
  }

  private async failRiskReduction(owner: RiskReductionOwnership, reason: string, executionFailure = false): Promise<void> {
    const current = this.riskReductionOwners.get(owner.marketId);
    if (current?.order.id !== owner.order.id || current.unhedgedStartedAt !== owner.unhedgedStartedAt) return;
    await this.commitAuthoritative({
      operationKey: `risk-reduction:${owner.order.id}:failed`, type: "RISK_REDUCTION_FAILED",
      aggregateType: "paper_order", aggregateId: owner.order.id,
      buildPayload: () => ({ ownership: owner, reason, executionFailure,
        derivedAudit: [this.ownershipAudit(owner, "RISK_REDUCTION_FAILED", reason)] }),
    });
  }

  private async admitRiskReduction(owner: RiskReductionOwnership, attempt?: EarlyAdmissionAttempt, admissionGuard?: () => void): Promise<boolean> {
    if (this.soakBlocksPostEntry(owner.marketId)) return false;
    const old = this.riskReductionOwners.get(owner.marketId);
    if (old) {
      if (old.unhedgedStartedAt === owner.unhedgedStartedAt) {
        if (attempt) attempt.reason = "TAKER_ALREADY_OWNS_RESIDUAL";
        return false;
      }
      await this.failRiskReduction(old, "EPISODE_SUPERSEDED");
    }
    // A completed/failed action identity can never be re-admitted by a retry.
    if (await this.journal.readDurableByOperationKey(`risk-reduction:${owner.order.id}:admitted`)
      || await this.journal.readDurableByOperationKey(`fill:risk-reduction-fill:${owner.order.id}`)) {
      if (attempt) attempt.reason = "ACTION_IDENTITY_ALREADY_USED";
      return false;
    }
    try {
      await this.commitAuthoritative({
        operationKey: `risk-reduction:${owner.order.id}:admitted`, type: "RISK_REDUCTION_ADMITTED",
        aggregateType: "paper_order", aggregateId: owner.order.id,
        beforeCommit: () => {
          admissionGuard?.();
          if (owner.intent !== "GUARDIAN_PROTECTIVE_SELL" && this.confirmedGuardianProtection(owner.marketId)) {
            throw new RiskReductionLifecycleGuardRejectedError(owner.marketId, owner.intent, "GUARDIAN_PRIORITY");
          }
          const position = this.ledger.positions.get(owner.marketId);
          if (this.riskReductionOwners.has(owner.marketId)) throw new RiskReductionLifecycleGuardRejectedError(owner.marketId, owner.intent, "TAKER_ALREADY_OWNS_RESIDUAL");
          if (!position?.unpairedOutcome || this.settledMarkets.has(owner.marketId)
            || unhedgedEpisode(position) !== owner.unhedgedStartedAt
            || !d(position.unpairedQuantity).eq(owner.admittedResidualQuantity)
            || owner.order.outcome !== (owner.order.side === "SELL" ? position.unpairedOutcome : position.unpairedOutcome === "UP" ? "DOWN" : "UP")
            || owner.order.tokenId !== this.currentTokenIdFor(owner.marketId, owner.order.outcome)) {
            throw new RiskReductionLifecycleGuardRejectedError(owner.marketId, owner.intent, "ADMISSION_RESIDUAL_CHANGED");
          }
          const profitTrailingOwner = this.profitTrailingPairSoakEnabled()
            && owner.intent === "HEDGE_SECOND_LEG"
            && (owner.order.strategyId === "profit-trailing-pair-target"
              || owner.order.strategyId === "profit-trailing-pair-trail");
          if (profitTrailingOwner
            && !this.profitTrailingRetainsAuthority(owner.marketId, position, this.nowFn().getTime())) {
            throw new RiskReductionLifecycleGuardRejectedError(
              owner.marketId,
              owner.intent,
              "PROFIT_TRAILING_AUTHORITY_EXPIRED",
            );
          }
          if (owner.deRisk?.deRiskReason === "REVERSAL_CONFIRMED"
            && (!this.reversalOrderEligible(owner.deRisk, position, this.nowFn().getTime())
              || !d(owner.order.quantity).eq(position.unpairedQuantity))) {
            throw new RiskReductionLifecycleGuardRejectedError(owner.marketId, owner.intent, "REVERSAL_EPISODE_CHANGED");
          }
          if (owner.intent === "GUARDIAN_PROTECTIVE_SELL") {
            const evaluation = this.evaluateGuardian(owner.marketId, owner.order);
            if (!evaluation.decision?.accepted) throw new RiskReductionLifecycleGuardRejectedError(owner.marketId, owner.intent, evaluation.reason);
          }
          // The new durable boundary is another await after PR3's economic
          // fence. Repeat that fence here, inside the serialized mutation gate.
          if (owner.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK") {
            const oppositeOutcome: Outcome = position.unpairedOutcome === "UP" ? "DOWN" : "UP";
            const fence = this.finalEarlyEconomicFence(owner.marketId, position.unpairedOutcome, oppositeOutcome,
              { order: owner.order } as ReturnType<typeof decideRescueSecondLegOrder>, null, null);
            if (!fence.ok || fence.adoptHedge) throw new RiskReductionLifecycleGuardRejectedError(owner.marketId, owner.intent, fence.adoptHedge ? "NORMAL_HEDGE_PRIORITY" : fence.reason);
          }
        },
        buildPayload: () => ({ ownership: owner, derivedAudit: [
          this.ownershipAudit(owner, "RISK_REDUCTION_ADMITTED"),
          ...(owner.intent === "GUARDIAN_PROTECTIVE_SELL" && this.confirmedGuardianProtection(owner.marketId) ? [{
            id: derivedAuditEventId("GUARDIAN_EVALUATION", owner.order.id), type: "GUARDIAN_EVALUATION",
            aggregateType: "position", aggregateId: this.ledger.positions.get(owner.marketId)!.id, traceId: owner.order.traceId,
            payload: { marketId: owner.marketId, orderId: owner.order.id, intent: owner.intent, eligible: true,
              reason: owner.deRisk?.guardianReason, confirmedAt: this.confirmedGuardianProtection(owner.marketId)!.confirmedAt,
              executionStatus: "ADMITTED_PENDING_FILL", independentPriority: true },
          }] : []),
          { id: derivedAuditEventId("PAPER_ORDER_CREATED", owner.order.id),
            traceId: owner.order.traceId, type: "PAPER_ORDER_CREATED", aggregateType: "paper_order", aggregateId: owner.order.id,
            payload: { ...owner.order, intent: owner.intent, experimentId: this.experiment?.id ?? null,
              ...(owner.deRisk ? { deRisk: owner.deRisk } : {}) } },
        ] }),
      });
      if (attempt) attempt.admitted = true;
      if (owner.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK") {
        // The retry slot belongs to an actual durable action, including an
        // admitted action whose execution subsequently fails. Never preflight.
        this.earlyCatastrophicNextAttemptAt.set(owner.marketId, this.nowFn().getTime() + PROTECTIVE_DE_RISK_RETRY_MS);
        this.earlyCatastrophicVetoFingerprint.delete(owner.marketId);
      }
      return true;
    } catch (error) {
      if (!(error instanceof RiskReductionLifecycleGuardRejectedError)) throw error;
      if (attempt) attempt.reason = error.reason;
      this.record("RISK_REDUCTION_LIFECYCLE_BLOCKED", owner.order.traceId, owner.marketId, {
        orderId: owner.order.id, intent: owner.intent, reason: error.reason,
      });
      return false;
    }
  }

  /** Recovery waits for real fresh books and continues the original order.
   * No fresh decision/order identity is generated for a pending admission. */
  private async resumeRiskReduction(owner: RiskReductionOwnership): Promise<void> {
    if (this.soakBlocksPostEntry(owner.marketId)) {
      await this.failRiskReduction(owner, "NAKED_DIRECTIONAL_SOAK_DISABLED_AUTHORITY");
      return;
    }
    if (this.riskReductionInFlight.has(owner.marketId) || this.state.database !== "UP"
      || this.state.journalDegraded || this.overload.hasIntegrityFault || this.overload.state === "FAIL_CLOSED") return;
    this.riskReductionInFlight.add(owner.marketId);
    try {
      const position = this.ledger.positions.get(owner.marketId);
      if (!position?.unpairedOutcome || this.settledMarkets.has(owner.marketId)
        || unhedgedEpisode(position) !== owner.unhedgedStartedAt
        || !d(position.unpairedQuantity).eq(owner.admittedResidualQuantity)) {
        await this.failRiskReduction(owner, "RECOVERED_RESIDUAL_CHANGED");
        return;
      }
      const now = this.nowFn();
      const eligibility = this.protectiveDeRiskEligibility(owner.marketId, owner.unhedgedStartedAt, now.getTime());
      if (eligibility.marketEndMs != null && now.getTime() >= eligibility.marketEndMs) {
        await this.failRiskReduction(owner, "MARKET_EXPIRED");
        return;
      }
      const snapshot = this.freshEarlyCatastrophicSnapshot(this.snapshots.get(owner.marketId)?.[owner.order.outcome] ?? null, now.getTime());
      if (!snapshot || Date.parse(snapshot.receivedAt) < Date.parse(owner.decisionReceivedAt)) return;
      await this.submitPaperOrder(owner.order, snapshot, `${owner.marketId}:${owner.order.outcome}:${owner.intent}`,
        owner.intent, owner.deRisk as ProtectiveDeRiskAudit | undefined, { resume: true });
    } finally {
      this.riskReductionInFlight.delete(owner.marketId);
    }
  }

  private async submitPaperOrder(
    order: PaperOrder,
    decisionSnapshot: MarketSnapshot,
    duplicateKey: string,
    intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG" | "GUARDIAN_PROTECTIVE_SELL" = "OPEN_FIRST_LEG",
    deRisk?: ProtectiveDeRiskAudit,
    options?: { skipPreCreateFirstLegCancel?: boolean; resume?: boolean; earlyAttempt?: EarlyAdmissionAttempt; admissionGuard?: () => void },
  ): Promise<boolean> {
    if (intent === "OPEN_FIRST_LEG") return this.executeSubmittedPaperOrder(order, decisionSnapshot, duplicateKey, intent, deRisk, options);
    return this.durableGate.outside(() => this.executeSubmittedPaperOrder(order, decisionSnapshot, duplicateKey, intent, deRisk, options));
  }

  private async executeSubmittedPaperOrder(
    order: PaperOrder,
    decisionSnapshot: MarketSnapshot,
    duplicateKey: string,
    intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG" | "GUARDIAN_PROTECTIVE_SELL" = "OPEN_FIRST_LEG",
    deRisk?: ProtectiveDeRiskAudit,
    options?: { skipPreCreateFirstLegCancel?: boolean; resume?: boolean; earlyAttempt?: EarlyAdmissionAttempt; admissionGuard?: () => void },
  ): Promise<boolean> {
    if (intent === "OPEN_FIRST_LEG" && this.experimentStartPending) return false;
    if (this.soakBlocksPostEntry(order.marketId) && (intent !== "OPEN_FIRST_LEG" || order.side !== "BUY"
      || (this.ledger.positions.get(order.marketId)?.unpairedOutcome
        && this.ledger.positions.get(order.marketId)!.unpairedOutcome !== order.outcome))) return false;
    if ((intent === "PROTECTIVE_DE_RISK" || intent === "GUARDIAN_PROTECTIVE_SELL") && order.orderType !== "TAKER") {
      throw new Error("PROTECTIVE_DE_RISK_REQUIRES_TAKER");
    }
    if (intent === "RESCUE_SECOND_LEG" && order.orderType !== "TAKER") {
      throw new Error("RESCUE_SECOND_LEG_REQUIRES_TAKER");
    }
    const riskReductionOrder = intent === "HEDGE_SECOND_LEG"
      || intent === "RESCUE_SECOND_LEG"
      || intent === "PROTECTIVE_DE_RISK" || intent === "GUARDIAN_PROTECTIVE_SELL";
    // PR2 single risk-reduction authority: exactly one risk-reduction admission
    // may be in flight per market. RESCUE and PROTECTIVE submissions are
    // already serialized by the maybeProtectiveDeRisk per-market flight (the
    // shared set is held for their whole decision+execution lifetime), so only
    // normal HEDGE submissions acquire/check the slot here â€” a hedge must never
    // slip in behind rescue/protective ownership. A HEDGE MAKER admission holds
    // the slot through its durable PAPER_MAKER_RESERVED so rescue cannot start
    // A/B arbitration while a hedge maker is still being admitted/reserved; the
    // slot is released again once the resting order is authoritative. A resting
    // maker does NOT hold the slot for its resting lifetime.
    const ownsRiskReductionFlight = intent === "HEDGE_SECOND_LEG" && !options?.resume;
    if (ownsRiskReductionFlight) {
      if (this.riskReductionBusy(order.marketId)) {
        this.record("RISK_REDUCTION_FLIGHT_BUSY", order.traceId, order.marketId, {
          intent, orderId: order.id, reason: "RISK_REDUCTION_IN_FLIGHT",
        }, decisionSnapshot.asset ?? null);
        return false;
      }
      // Admission-boundary authority re-check after decision latency. Profit
      // trailing may bypass rescue exclusivity only while it still owns the
      // pre-deadline episode. Once the protective deadline is reached, stale
      // trailing orders are rejected and generic A/B protection owns.
      const profitTrailingOrder = this.profitTrailingPairSoakEnabled()
        && intent === "HEDGE_SECOND_LEG"
        && (order.strategyId === "profit-trailing-pair-target" || order.strategyId === "profit-trailing-pair-trail");
      const posAtAdmission = intent === "HEDGE_SECOND_LEG"
        ? this.ledger.positions.get(order.marketId)
        : undefined;
      const profitTrailingStillOwns = profitTrailingOrder
        && this.profitTrailingRetainsAuthority(order.marketId, posAtAdmission, this.nowFn().getTime());
      if (profitTrailingOrder && !profitTrailingStillOwns) {
        this.record("HEDGE_SUPPRESSED_RESCUE_EXCLUSIVE", order.traceId, order.marketId, {
          unpairedOutcome: posAtAdmission?.unpairedOutcome ?? null,
          reason: "PROFIT_TRAILING_AUTHORITY_EXPIRED",
        }, decisionSnapshot.asset ?? null);
        return false;
      }
      if (intent === "HEDGE_SECOND_LEG" && !profitTrailingOrder && posAtAdmission?.unpairedOutcome) {
        const admissionEligibility = this.protectiveDeRiskEligibility(
          order.marketId,
          posAtAdmission.unhedgedStartedAt ?? posAtAdmission.openedAt,
          this.nowFn().getTime(),
        );
        if (admissionEligibility.eligible) {
          this.record("HEDGE_SUPPRESSED_RESCUE_EXCLUSIVE", order.traceId, order.marketId, {
            unpairedOutcome: posAtAdmission.unpairedOutcome,
            reason: "ELIGIBILITY_REACHED_BEFORE_ADMISSION",
          }, decisionSnapshot.asset ?? null);
          return false;
        }
      }
      this.riskReductionInFlight.add(order.marketId);
    }
    const firstLegReserved = intent === "OPEN_FIRST_LEG"
      ? this.tryReserveFirstLegAdmission(order.marketId)
      : false;
    if (intent === "OPEN_FIRST_LEG" && !firstLegReserved) return false;
    const positionAtSubmission = this.ledger.positions.get(order.marketId);
    let durableOwner: RiskReductionOwnership | undefined;

    try {
      // Last economic admission fence immediately before PAPER_ORDER_CREATED.
      // close-entries and the native decision path are serialized by the same
      // experimentBoundaryGate, while deadline expiry closes durably here.
      if (intent === "OPEN_FIRST_LEG" && !(await this.ensureFirstLegEntryOpen())) return false;
      if (intent === "OPEN_FIRST_LEG") {
        const admissionBook = this.state.marketSnapshots.get(order.tokenId);
        const reason = admissionBook ? this.firstLegExecutionReason(order, admissionBook, this.nowFn()) : "ENTRY_BOOK_UNAVAILABLE";
        if (reason) {
          this.metrics.riskVetoes.inc({ reason });
          this.record("FIRST_LEG_REVALIDATION_REJECTED", order.traceId, order.marketId, { orderId: order.id, reason });
          return false;
        }
        const economics = firstLegExecutionEconomics(order, admissionBook!, this.nowFn().getTime());
        if (economics.reason) {
          this.metrics.riskVetoes.inc({ reason: economics.reason });
          this.record("FIRST_LEG_REVALIDATION_REJECTED", order.traceId, order.marketId, { orderId: order.id, ...economics });
          return false;
        }
        const expiry = this.firstLegExpiryAdmission(order.marketId);
        if (!expiry.eligible) {
          this.record("FIRST_LEG_REVALIDATION_REJECTED", order.traceId, order.marketId, {
            orderId: order.id,
            reason: "DURATION_OUT_OF_RANGE",
            endDate: expiry.endDate,
            remainingDurationMs: expiry.remainingDurationMs,
            minRemainingDurationMs: expiry.minRemainingDurationMs,
          }, decisionSnapshot.asset ?? null);
          return false;
        }
      }
      if (
        intent === "OPEN_FIRST_LEG"
        && this.firstLegLifecycleBlocked(order.marketId, true)
      ) return false;
      if (intent === "HEDGE_SECOND_LEG" || intent === "PROTECTIVE_DE_RISK" || intent === "RESCUE_SECOND_LEG" || intent === "GUARDIAN_PROTECTIVE_SELL") {
        if (deRisk?.deRiskReason === "REVERSAL_CONFIRMED") {
          // The reversal executor already fenced makers under the episode's
          // mutation gate. Never cancel a newer episode's entry remainder here.
          if (!this.reversalOrderEligible(deRisk, this.ledger.positions.get(order.marketId), this.nowFn().getTime())
            || this.hasOutstandingFirstLegMaker(order.marketId)) return false;
        } else if (options?.skipPreCreateFirstLegCancel) {
          // EARLY path: first-leg makers were durably fenced BEFORE the final
          // synchronous fence. Assert synchronously that none remain, and do NOT
          // enter an async cancellation boundary between fence and
          // PAPER_ORDER_CREATED (Fix 1). Generic HEDGE/PR82/PROTECTIVE behavior
          // is unchanged (no option passed).
          if (this.hasOutstandingFirstLegMaker(order.marketId)) {
            throw new Error("EARLY_FIRST_LEG_FENCE_VIOLATION");
          }
        } else {
          // Once risk reduction starts, an older first-leg maker remainder must
          // not be allowed to refill directional exposure behind the action.
          await this.cancelOutstandingFirstLegMakers(
            order.marketId,
            (intent === "PROTECTIVE_DE_RISK" || intent === "GUARDIAN_PROTECTIVE_SELL") ? `${intent}_ADMITTED`
              : intent === "RESCUE_SECOND_LEG" ? "RESCUE_ADMITTED"
              : "HEDGE_ADMITTED",
          );
        }
      }

      if (riskReductionOrder && order.orderType === "TAKER") {
        for(const resting of [...this.restingOrders.values()])if(resting.order.marketId===order.marketId
          &&resting.order.makerFirst&&this.restingOrderIntent(resting)==='HEDGE_SECOND_LEG')
          await this.releaseMaker(resting.order.id,{cancel:true,reason:'MAKER_FIRST_TAKER_HANDOFF'});
        if (options?.resume) {
          durableOwner = this.riskReductionOwners.get(order.marketId);
          if (durableOwner?.order.id !== order.id) return false;
        } else {
          if (!positionAtSubmission?.unpairedOutcome) return false;
          durableOwner = riskReductionOwnershipSchema.parse({ marketId: order.marketId,
            unhedgedStartedAt: deRisk?.unhedgedStartedAt ?? unhedgedEpisode(positionAtSubmission),
            intent, order, admittedResidualQuantity: positionAtSubmission.unpairedQuantity,
            decisionReceivedAt: decisionSnapshot.receivedAt, state: "ADMITTED", ...(deRisk ? { deRisk } : {}) });
          // ORDER_ADMITTED is emitted only by the durable admission's derived
          // order audit, never by the preceding RISK_EVALUATION/RISK_ACCEPT.
          if (deRisk) {
            deRisk = { ...deRisk, finalState: "ORDER_ADMITTED" };
            durableOwner.deRisk = deRisk;
          }
          if (!(await this.admitRiskReduction(durableOwner, options?.earlyAttempt, options?.admissionGuard))) return false;
        }
      }
      if (!options?.resume) this.metrics.paperOrders.inc({ type: order.orderType });
      if (!options?.resume) this.record("PAPER_ORDER_CREATED", order.traceId, order.marketId, {
        orderId: order.id, type: order.orderType, intent, ...(deRisk ? { deRisk } : {}),
      }, decisionSnapshot.asset ?? null);
      let orderCreatedEvent: JournalEvent | undefined;
      if (!durableOwner) {
        const makerExperimentId = order.orderType === "MAKER" && this.isExperimentWindowOpen()
          ? this.experiment?.id : undefined;
        orderCreatedEvent = createJournalEvent({
          id: derivedAuditEventId("PAPER_ORDER_CREATED", order.id),
          traceId: order.traceId, type: "PAPER_ORDER_CREATED", aggregateType: "paper_order", aggregateId: order.id,
          payload: { ...order, intent, ...(intent === "OPEN_FIRST_LEG" ? { executionLifecycleVersion: 2 } : {}), ...(deRisk ? { deRisk } : {}),
            ...(makerExperimentId ? { experimentId: makerExperimentId } : {}) },
        });
        // Freeze the final stamped maker event before the direct append can
        // await. Reservation repair must carry this identity, not recreate it.
        if (order.orderType === "MAKER") orderCreatedEvent = structuredClone(orderCreatedEvent);
        await this.persistReportInput(orderCreatedEvent);
      }
      this.recentTrades.set(duplicateKey, Date.now());
      try {
        if (order.orderType === "MAKER") {
          if (intent === "PROTECTIVE_DE_RISK" || intent === "GUARDIAN_PROTECTIVE_SELL") throw new Error("PROTECTIVE_DE_RISK_REQUIRES_TAKER");
          if (intent === "RESCUE_SECOND_LEG") throw new Error("RESCUE_SECOND_LEG_REQUIRES_TAKER");
          // Resting makers are only ever first-leg or hedge orders; rescue is
          // enforced TAKER-only above, so the cast is a pure TS narrowing aid.
          await this.reserveMaker(order, decisionSnapshot, intent === "OPEN_FIRST_LEG" ? "OPEN_FIRST_LEG" : "HEDGE_SECOND_LEG", orderCreatedEvent);
          return true;
        }
        await delay(this.env.PAPER_EXECUTION_LATENCY_MS);
        const current = this.state.marketSnapshots.get(order.tokenId);
        if (!current || new Date(current.receivedAt).getTime() < new Date(decisionSnapshot.receivedAt).getTime()) throw new Error("STALE_EXECUTION_SNAPSHOT");
        if (intent === "OPEN_FIRST_LEG") {
          const reason = this.firstLegAdmissionReason(order, current);
          if (reason) throw new FirstLegLifecycleGuardRejectedError(order.marketId, reason);
          const economics = firstLegExecutionEconomics(order, current, this.nowFn().getTime());
          if (economics.reason) throw new FirstLegLifecycleGuardRejectedError(order.marketId, economics.reason);
        }
        if (intent === "PROTECTIVE_DE_RISK" || intent === "GUARDIAN_PROTECTIVE_SELL") {
          const reason = this.protectiveFillHardSafetyReason(order);
          if (reason) {
            this.recordProtectiveDeRiskSuppressed(order.marketId, "FINAL_FENCE", reason, {
              snapshot: current, extra: { stage: "PRE_EXECUTION", orderId: order.id },
            });
            throw new RiskReductionLifecycleGuardRejectedError(order.marketId, intent, reason);
          }
        }
        if (riskReductionOrder && !this.freshEarlyCatastrophicSnapshot(current, this.nowFn().getTime())) {
          throw new RiskReductionLifecycleGuardRejectedError(order.marketId, intent, "GUARDIAN_EXECUTION_BOOK_UNAVAILABLE");
        }
        if (riskReductionOrder && intent !== "GUARDIAN_PROTECTIVE_SELL" && this.confirmedGuardianProtection(order.marketId)) {
          throw new RiskReductionLifecycleGuardRejectedError(order.marketId, intent, "GUARDIAN_PRIORITY");
        }
        if (riskReductionOrder) this.logger.info?.({ stage: "EXECUTION_STARTED", orderId: order.id, marketId: order.marketId,
          intent, sourceTimestamp: current.sourceTimestamp, receivedAt: current.receivedAt,
          residual: this.ledger.positions.get(order.marketId)?.unpairedQuantity, localAt: performance.now() }, "risk reduction milestone");
        if (durableOwner) {
          const residual = this.ledger.positions.get(order.marketId);
          if (this.riskReductionOwners.get(order.marketId)?.order.id !== order.id
            || !residual?.unpairedOutcome || unhedgedEpisode(residual) !== durableOwner.unhedgedStartedAt
            || !d(residual.unpairedQuantity).eq(durableOwner.admittedResidualQuantity)) {
            throw new RiskReductionLifecycleGuardRejectedError(order.marketId, durableOwner.intent, "ADMITTED_RESIDUAL_CHANGED");
          }
        }
        const takerFill = executePaperTaker({ order, snapshotAfterLatency: current, measuredLatencyMs: this.env.PAPER_EXECUTION_LATENCY_MS });
        if (durableOwner) takerFill.id = `risk-reduction-fill:${order.id}`;
        if (intent === "OPEN_FIRST_LEG") takerFill.id = `first-leg-fill:${order.id}`;
        const applied = await this.applyFill(
          takerFill,
          { intent, deRisk, riskReductionOwnership: durableOwner, executionSnapshot: current, executionOrder: order },
        );
        if (applied) this.lastTradeAt = this.nowFn();
        else if (durableOwner) await this.failRiskReduction(durableOwner, "FILL_NOT_APPLIED");
        if (!applied) {
          if (intent === "OPEN_FIRST_LEG") await this.rejectFirstLegOrder(order, "FILL_NOT_APPLIED", "COMMIT_FENCE");
          return false;
        }
      } catch (error) {
        if (this.overload.hasIntegrityFault || error instanceof IdempotencyConflictError || error instanceof CrashInjectedError || (error instanceof JournalAdmissionRejected && error.critical)) throw error;
        if (intent === "OPEN_FIRST_LEG") await this.rejectFirstLegOrder(order,
          error instanceof FirstLegLifecycleGuardRejectedError ? error.reason : error instanceof Error ? error.message : String(error),
          "EXECUTION_OR_COMMIT");
        if (error instanceof FirstLegLifecycleGuardRejectedError) {
          this.metrics.riskVetoes.inc({ reason: error.reason });
          this.record("FIRST_LEG_LIFECYCLE_BLOCKED", order.traceId, order.marketId, { orderId: order.id, reason: error.reason });
          return false;
        }
        if (durableOwner) await this.failRiskReduction(durableOwner, error instanceof RiskReductionLifecycleGuardRejectedError ? error.reason : error instanceof Error ? error.message : String(error),
          !(error instanceof RiskReductionLifecycleGuardRejectedError) && !(error instanceof TakerCashCommitGuardRejectedError) && !(error instanceof MakerReservationCashCommitGuardRejectedError));
        if (error instanceof RiskReductionLifecycleGuardRejectedError) {
          const stateChangedDeRisk = deRisk ? { ...deRisk, finalState: "STATE_CHANGED" as const } : undefined;
          this.record("RISK_REDUCTION_LIFECYCLE_BLOCKED", order.traceId, order.marketId, {
            orderId: order.id,
            intent: error.intent,
            reason: error.reason,
            ...(stateChangedDeRisk ? { deRisk: stateChangedDeRisk } : {}),
          }, decisionSnapshot.asset ?? null);
          await this.persistReportInput(createJournalEvent({
            traceId: order.traceId,
            type: "PAPER_EXECUTION_ERROR",
            aggregateType: "paper_order",
            aggregateId: order.id,
            payload: {
              orderId: order.id,
              marketId: order.marketId,
              intent: error.intent,
              message: error.message,
              stateChanged: true,
              ...(stateChangedDeRisk ? { deRisk: stateChangedDeRisk } : {}),
            },
          }));
          return false;
        }
        if (error instanceof TakerCashCommitGuardRejectedError) {
          // Normal concurrent cash race rejected at the serialized durable
          // boundary. The NEW fill produced no economic mutation. It is
          // bounded observability only and must not be counted as execution
          // corruption (executionErrors feeds the execution-error risk veto).
          this.record("TAKER_CASH_FENCE_REJECTED", order.traceId, order.marketId, {
            orderId: order.id,
            fillId: error.fillId,
            marketId: error.marketId,
            requiredCash: error.requiredCash,
            availableCash: error.availableCash,
            intent,
          });
          return false;
        }
        if (error instanceof MakerReservationCashCommitGuardRejectedError) {
          // Normal concurrent cash race rejected at the serialized durable
          // boundary. The NEW maker reservation produced no economic mutation
          // (no durable PAPER_MAKER_RESERVED, no reservedCash change, no
          // resting order state). Bounded observability only; it must not be
          // counted as execution corruption.
          this.record("MAKER_CASH_FENCE_REJECTED", order.traceId, order.marketId, {
            orderId: error.orderId,
            marketId: error.marketId,
            requiredCash: error.requiredCash,
            availableCash: error.availableCash,
            intent,
          });
          return false;
        }
        const message = error instanceof Error ? error.message : String(error);
        // Liquidity disappearance is an expected market condition, not an abnormal engine/exchange failure.
        // Keep audit and retry/failure semantics, but do not poison the global hourly breaker.
        if (message !== "INSUFFICIENT_DEPTH") this.executionErrors.push(Date.now());
        const failureDeRisk = deRisk ? { ...deRisk, finalState: "EXECUTION_ERROR" as const } : undefined;
        this.record("PAPER_EXECUTION_ERROR", order.traceId, order.marketId, {
          orderId: order.id, message, intent, ...(failureDeRisk ? { deRisk: failureDeRisk } : {}),
        });
        await this.persistReportInput(createJournalEvent({
          traceId: order.traceId,
          type: "PAPER_EXECUTION_ERROR",
          aggregateType: "paper_order",
          aggregateId: order.id,
          payload: { orderId: order.id, marketId: order.marketId, message, intent, ...(failureDeRisk ? { deRisk: failureDeRisk } : {}) },
        }));
        return false;
      }
    } finally {
      if (firstLegReserved) this.pendingFirstLegMarkets.delete(order.marketId);
      if (ownsRiskReductionFlight) this.riskReductionInFlight.delete(order.marketId);
      if (riskReductionOrder && intent !== "GUARDIAN_PROTECTIVE_SELL" && this.confirmedGuardianProtection(order.marketId)) {
        setImmediate(() => this.kickReversalProtection(order.marketId));
      }
    }
    return true;
  }

  private async reserveMaker(
    order: PaperOrder,
    snapshot: MarketSnapshot,
    intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" = "OPEN_FIRST_LEG",
    orderCreatedEvent?: JournalEvent,
  ): Promise<void> {
    if (this.soakBlocksPostEntry(order.marketId) && (intent !== "OPEN_FIRST_LEG" || order.side !== "BUY"
      || (this.ledger.positions.get(order.marketId)?.unpairedOutcome
        && this.ledger.positions.get(order.marketId)!.unpairedOutcome !== order.outcome))) return;
    const notional = makerReserveNotional(order.quantity, order.requestedPrice, snapshot.feeSchedule);
    const reservationBook=order.makerFirst?(this.state.marketSnapshots.get(order.tokenId)??snapshot):snapshot;
    const priorReserve = await this.journal.readDurableByOperationKey(`maker:${order.id}:reserve`);
    const compactedRetry = Boolean(priorReserve && Object.keys(priorReserve.payload).length === 0);
    if (this.ledger.availableCash().lt(notional) && !priorReserve) {
      if(order.makerFirst)throw new MakerReservationCashCommitGuardRejectedError(order.id,order.marketId,notional,this.ledger.availableCash().toFixed());
      throw new Error("INSUFFICIENT_CASH");
    }
    if(order.makerFirst&&passiveMakerBuyPrice(reservationBook,order.quantity,order.requestedPrice)!==order.requestedPrice){
      if(intent==='OPEN_FIRST_LEG')throw new FirstLegLifecycleGuardRejectedError(order.marketId,'MAKER_FIRST_POST_ONLY_INVALID');
      throw new RiskReductionLifecycleGuardRejectedError(order.marketId,'HEDGE_SECOND_LEG','MAKER_FIRST_POST_ONLY_INVALID');
    }
    const resting = compactedRetry
      ? (this.restingOrders.get(order.id) ?? createRestingMakerOrder(order, reservationBook, intent))
      : createRestingMakerOrder(order, reservationBook, intent);
    // Keep the durable resting identity byte-compatible with pre-PR1 rows.
    // intent has always been a top-level reserve fact; recovery reattaches it
    // to the in-memory resting order.
    const { intent: _runtimeIntent, ...durableResting } = resting;
    const auth = { orderId: order.id, reserved: notional, resting: durableResting, intent };
    await this.commitAuthoritative({
      operationKey: `maker:${order.id}:reserve`,
      type: "PAPER_MAKER_RESERVED",
      aggregateType: "paper_order",
      aggregateId: order.id,
      beforeCommit: async () => {
        // Authoritative maker cash fence at the serialized DurableMutationGate
        // boundary (the same gate that makes resolveDurablePayload and the
        // journal insert atomic). The reserveMaker preflight above is a
        // non-serialized admission read: two different-market maker plans can
        // both observe the same available cash and pass it. Re-fence CURRENT
        // unreserved cash here so a NEW PAPER_MAKER_RESERVED is never durably
        // written unless applying it would succeed against ledger.reserve().
        // A retry of the SAME reservation identity (one that resolved inside
        // resolveDurablePayload, or a compacted retry) must resolve as the
        // existing durable identity instead of being rejected or reserving
        // cash twice.
        const reserveAuthorityAtCommit = await this.journal.readDurableByOperationKey(`maker:${order.id}:reserve`);
        if (reserveAuthorityAtCommit) return;
        if(order.makerFirst){
          const current=this.state.marketSnapshots.get(order.tokenId);
          const reason=!current || passiveMakerBuyPrice(current,order.quantity,order.requestedPrice)!==order.requestedPrice
            ? 'MAKER_FIRST_POST_ONLY_INVALID':this.hybridQuoteInvalidReason(resting,current);
          if(reason){
            if(intent==='OPEN_FIRST_LEG')throw new FirstLegLifecycleGuardRejectedError(order.marketId,reason);
            throw new RiskReductionLifecycleGuardRejectedError(order.marketId,'HEDGE_SECOND_LEG',reason);
          }
          if(intent==='OPEN_FIRST_LEG'){
            const exposureReason=this.pendingEntryExposureReason(order);
            if(exposureReason)throw new FirstLegLifecycleGuardRejectedError(order.marketId,exposureReason);
          }
          if(intent==='HEDGE_SECOND_LEG'&&(this.riskReductionOwners.has(order.marketId)
            ||!this.ledger.positions.get(order.marketId)?.unpairedOutcome
            ||d(order.quantity).gt(this.ledger.positions.get(order.marketId)!.unpairedQuantity)))
            throw new RiskReductionLifecycleGuardRejectedError(order.marketId,'HEDGE_SECOND_LEG','MAKER_FIRST_RESIDUAL_CHANGED');
        }
        if (intent === "OPEN_FIRST_LEG" && orderCreatedEvent) {
          const current = this.state.marketSnapshots.get(order.tokenId);
          const reason = current ? this.firstLegExecutionReason(order, current, this.nowFn()) : "ENTRY_BOOK_UNAVAILABLE";
          if (reason) throw new FirstLegLifecycleGuardRejectedError(order.marketId, reason);
          const economics = firstLegExecutionEconomics(order, current!, this.nowFn().getTime());
          if (economics.reason) throw new FirstLegLifecycleGuardRejectedError(order.marketId, economics.reason);
        }
        if (this.ledger.availableCash().lt(notional)) {
          throw new MakerReservationCashCommitGuardRejectedError(
            order.id,
            order.marketId,
            notional,
            this.ledger.availableCash().toFixed(),
          );
        }
      },
      buildPayload: (occurredAt) => ({
        ...auth,
        derivedAudit: [
          orderCreatedEvent ? orderCreatedEvent : {
            id: derivedAuditEventId("PAPER_ORDER_CREATED", order.id),
            type: "PAPER_ORDER_CREATED",
            aggregateType: "paper_order",
            aggregateId: order.id,
            traceId: order.traceId,
            payload: { ...order, intent },
          },
          {
            id: derivedAuditEventId("PAPER_MAKER_RESERVED", order.id),
            type: "PAPER_MAKER_RESERVED",
            aggregateType: "paper_order",
            aggregateId: order.id,
            traceId: order.traceId,
            payload: { orderId: order.id, reserved: notional, experimentId: this.experiment?.id ?? null, updatedAt: occurredAt.toISOString() },
          },
        ],
      }),
      afterApply:async()=>{
        if(order.makerFirst){
          this.makerActiveAfter.set(order.id,new Date(this.nowFn().getTime()+order.makerFirst.submissionLatencyMs).toISOString());
          this.scheduleMakerQuoteExpiry();
        }
      },
    });
  }

  private async releaseMaker(orderId: string, options: { filledNotional?: string; cancel?: boolean; reason?: string } = {}): Promise<void> {
    const operationKey = options.cancel ? `maker:${orderId}:cancel` : `maker:${orderId}:release`;
    const reserved = this.makerReservations.get(orderId);
    if (!reserved && !options.cancel && !(await this.journal.readDurableByOperationKey(operationKey))) return;
    if (!reserved && options.cancel && !this.restingOrders.has(orderId) && !(await this.journal.readDurableByOperationKey(operationKey))) return;
    const leftover = reserved ? d(reserved).minus(options.filledNotional ?? 0) : d(0);
    const released = leftover.gt(0) ? decimal(leftover) : "0";
    const type = options.cancel ? "PAPER_ORDER_CANCELLED" : "PAPER_MAKER_RELEASED";
    const resting = this.restingOrders.get(orderId);
    await this.commitAuthoritative({
      operationKey,
      type,
      aggregateType: "paper_order",
      aggregateId: orderId,
      buildPayload: (occurredAt) => ({
        // The released amount is represented by the authoritative reservation
        // state and is kept in derived audit only, so compacted retries remain
        // hash-stable after the reservation has already been released.
        orderId, cancel: options.cancel === true, reason: options.reason,
        derivedAudit: [{
          id: derivedAuditEventId(type, orderId),
          type,
          aggregateType: "paper_order",
          aggregateId: orderId,
          traceId: orderId,
          payload: {
            orderId, released, reason: options.reason ?? null,
            experimentId: this.experiment?.id ?? null, updatedAt: occurredAt.toISOString(),
          },
        }, ...(options.cancel && resting && this.restingOrderIntent(resting) === "OPEN_FIRST_LEG" ? [{
          id: derivedAuditEventId("PAPER_ORDER_TERMINAL", orderId), type: "PAPER_ORDER_TERMINAL",
          aggregateType: "paper_order", aggregateId: orderId, traceId: resting.order.traceId,
          payload: { orderId, marketId: resting.order.marketId, intent: "OPEN_FIRST_LEG", status: "CANCELLED",
            reason: options.reason ?? "MAKER_CANCELLED", stage: "DURABLE_CANCEL", remainingQuantity: resting.remaining,
            filledQuantity: d(resting.order.quantity).minus(resting.remaining).toFixed(), terminalAt: occurredAt.toISOString(),
            experimentId: this.experiment?.id ?? null },
        }] : [])],
      }),
      afterApply: async () => {
        if (options.cancel) this.record("PAPER_ORDER_CANCELLED", orderId, null, { orderId, reason: options.reason ?? null });
        this.makerActiveAfter.delete(orderId);
        this.makerQueuePrimed.delete(orderId);
        if(resting?.order.makerFirst){
          this.recentTrades.delete(`${resting.order.marketId}:${resting.order.outcome}:${this.restingOrderIntent(resting)}`);
          this.scheduleMakerQuoteExpiry();
          if(!this.state.draining&&this.state.recoveryComplete)this.kickProfitTrailingPair(resting.order.marketId);
        }
      },
    });
  }

  private computeMakerRestingAfter(
    fill: Parameters<PaperLedger["applyFill"]>[0],
    extras: { restingAfter?: RestingPaperOrder | null },
  ): RestingPaperOrder | null {
    if (extras.restingAfter !== undefined) return extras.restingAfter;
    const current = this.restingOrders.get(fill.orderId) ?? null;
    if (!current || fill.liquidityRole !== "MAKER") return current;
    const remaining = d(current.remaining).minus(fill.quantity);
    return { ...current, remaining: remaining.gt(0) ? remaining.toFixed() : "0" };
  }

  private async applyFill(
    fill: Parameters<PaperLedger["applyFill"]>[0],
    extras: {
      restingAfter?: RestingPaperOrder | null;
      intent?: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG" | "GUARDIAN_PROTECTIVE_SELL";
      deRisk?: ProtectiveDeRiskAudit;
      riskReductionOwnership?: RiskReductionOwnership;
      executionSnapshot?: MarketSnapshot;
      executionOrder?: PaperOrder;
    } = {},
  ): Promise<boolean> {
    // Capture pricing, provenance and admission synchronously. PG identity reads
    // and queueing may admit later books; those are evidence for future orders.
    const executionAt = this.nowFn();
    const executionBook = extras.executionSnapshot ? structuredClone(extras.executionSnapshot) : undefined;
    const orderAtExecution = extras.executionOrder ?? this.restingOrders.get(fill.orderId)?.order;
    const executionOrder = orderAtExecution ? structuredClone(orderAtExecution) : undefined;
    const executionIntent = extras.intent ?? (this.restingOrders.get(fill.orderId)
      ? this.restingOrderIntent(this.restingOrders.get(fill.orderId)!) : undefined);
    const executionReason = executionIntent === "OPEN_FIRST_LEG" && executionBook
      ? executionOrder ? this.firstLegExecutionReason({ ...executionOrder, quantity: fill.quantity }, executionBook, executionAt)
        : "ENTRY_ECONOMICS_UNAVAILABLE"
      : null;
    // Capture before any asynchronous fill work can lose live discovery context.
    // The same lookup type used by learning/CF is persisted with economic authority.
    const settlementLookup = this.resolveSettlementLookup(fill.marketId, true);
    // A settlement is the terminal authority for this market. The live bit
    // is normally restored from the verified checkpoint; the durable lookup
    // covers a rediscovery/retry after bounded replay-key pressure evicted the
    // settlement identity. Never let an old fill recreate a settled position.
    if (this.settledMarkets.has(fill.marketId)) return false;
    const settlementAuthority = await this.journal.readDurableByOperationKey(`settlement:${fill.marketId}:${this.experiment?.id ?? "paper"}`);
    if (settlementAuthority?.type === "POSITION_SETTLED") {
      this.settledMarkets.add(fill.marketId);
      return false;
    }
    const restingAtAdmission = fill.liquidityRole === "MAKER"
      ? (this.restingOrders.get(fill.orderId) ?? null)
      : null;
    const economicIntent = extras.intent
      ?? (restingAtAdmission ? this.restingOrderIntent(restingAtAdmission) : undefined);
    if (this.soakBlocksPostEntry(fill.marketId) && (fill.side !== "BUY"
      || (economicIntent !== undefined && economicIntent !== "OPEN_FIRST_LEG")
      || (this.ledger.positions.get(fill.marketId)?.unpairedOutcome
        && this.ledger.positions.get(fill.marketId)!.unpairedOutcome !== fill.outcome))) return false;
    const priorFillAuthority = await this.journal.readDurableByOperationKey(`fill:${fill.id}`);
    const compactedRetry = Boolean(priorFillAuthority && Object.keys(priorFillAuthority.payload).length === 0);
    // A fill with no existing position is a first leg. The post-commit start
    // validation fence must reject it while the experiment identity is still
    // pending; hedges for pre-existing positions remain admissible.
    if (this.experimentStartPending && !this.ledger.positions.has(fill.marketId)) return false;
    const reserved = this.makerReservations.get(fill.orderId);
    let reservationAfter: string | null = reserved ?? null;
    if (reserved && fill.liquidityRole === "MAKER" && !compactedRetry) {
      const fillCost = d(fill.quantity).mul(fill.fillPrice).plus(fill.feeUsd);
      const next = d(reserved).minus(fillCost);
      reservationAfter = next.gt(0) ? decimal(next) : null;
    }
    const previous = this.ledger.positions.get(fill.marketId);
    const positionId = previous?.id ?? stablePositionId(fill.marketId);
    const restingAfter = compactedRetry
      ? (this.restingOrders.get(fill.orderId) ?? null)
      : this.computeMakerRestingAfter(fill, extras);
    try {
      await this.commitAuthoritative({
        operationKey: `fill:${fill.id}`,
        type: "PAPER_FILL",
        aggregateType: "paper_fill",
        aggregateId: fill.id,
        beforeCommit: async () => {
          if (priorFillAuthority) return;
          if (this.settledMarkets.has(fill.marketId)) {
            if (economicIntent === "OPEN_FIRST_LEG") throw new FirstLegLifecycleGuardRejectedError(fill.marketId, "MARKET_ALREADY_SETTLED");
            throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, economicIntent ?? "HEDGE_SECOND_LEG", "MARKET_ALREADY_SETTLED");
          }
          // NEW-fill TAKER BUY cash linearization at the serialized durable
          // boundary. The admission-time availableCash read is stale while this
          // call waits on DurableMutationGate: two TAKER BUYs on different
          // markets can be planned against the same pre-fill cash and both
          // pass earlier risk admission. Re-read the durable fill identity
          // first so a concurrently-committed identical retry resolves as the
          // existing idempotent result rather than an insufficient-cash
          // failure, then fence only the genuinely NEW economic mutation
          // against CURRENT unreserved cash.
          if (fill.side === "BUY" && fill.liquidityRole === "TAKER") {
            const fillAuthorityAtCommit = await this.journal.readDurableByOperationKey(`fill:${fill.id}`);
            if (fillAuthorityAtCommit) return;
            const actualRequiredCash = d(fill.quantity).mul(fill.fillPrice).plus(fill.feeUsd);
            if (this.ledger.availableCash().lt(actualRequiredCash)) {
              throw new TakerCashCommitGuardRejectedError(
                fill.id,
                fill.marketId,
                actualRequiredCash.toFixed(),
                this.ledger.availableCash().toFixed(),
              );
            }
          }
          const currentPosition = this.ledger.positions.get(fill.marketId);
          if(economicIntent==='OPEN_FIRST_LEG'&&this.makerFirstConfig().enabled){
            const live=this.restingOrders.get(fill.orderId);
            const exposureReason=this.pendingEntryExposureReason({id:fill.orderId,marketId:fill.marketId,outcome:fill.outcome,
              quantity:live?.remaining??fill.quantity,requestedPrice:fill.fillPrice});
            if(exposureReason)throw new FirstLegLifecycleGuardRejectedError(fill.marketId,exposureReason);
          }

          if(fill.makerEvidence){
            const live=this.restingOrders.get(fill.orderId);
            const reason=!live?'MAKER_FIRST_NOT_ACTIVE'
              :economicIntent==='OPEN_FIRST_LEG'&&!this.isExperimentWindowOpen()?'MAKER_FIRST_ENTRIES_CLOSED'
              :this.adapterTokenEpoch(fill.tokenId)!==live.order.makerFirst?.tokenEpoch?'MAKER_FIRST_EPOCH_CHANGED':null;
            if(reason||!live||d(fill.quantity).gt(live.remaining)
              ||Date.parse(fill.makerEvidence.trade.sourceTimestamp)<=Date.parse(this.makerActiveAfter.get(fill.orderId)??'9999-01-01T00:00:00Z')){
              if(economicIntent==='OPEN_FIRST_LEG')throw new FirstLegLifecycleGuardRejectedError(fill.marketId,reason??'MAKER_FIRST_QUANTITY_OR_LATENCY');
              throw new RiskReductionLifecycleGuardRejectedError(fill.marketId,'HEDGE_SECOND_LEG',reason??'MAKER_FIRST_QUANTITY_OR_LATENCY');
            }
          }

          const owner = this.riskReductionOwners.get(fill.marketId);
          if (extras.riskReductionOwnership && owner?.order.id !== extras.riskReductionOwnership.order.id) {
            throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, extras.riskReductionOwnership.intent, "TAKER_AUTHORITY_RELEASED");
          }
          if (owner) {
            const sameEpisode = currentPosition && unhedgedEpisode(currentPosition) === owner.unhedgedStartedAt;
            if (sameEpisode && owner.order.id !== fill.orderId) {
              throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, owner.intent, "RESIDUAL_OWNED_BY_ANOTHER_TAKER");
            }
            if (owner.order.id === fill.orderId && (!sameEpisode
              || !currentPosition?.unpairedOutcome
              || !d(currentPosition.unpairedQuantity).eq(owner.admittedResidualQuantity)
              || fill.tokenId !== owner.order.tokenId || fill.outcome !== owner.order.outcome
              || fill.tokenId !== this.currentTokenIdFor(fill.marketId, fill.outcome)
              || fill.side !== owner.order.side || !d(fill.quantity).eq(owner.order.quantity))) {
              throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, owner.intent, "ADMITTED_RESIDUAL_CHANGED");
            }
          }

          if (economicIntent === "HEDGE_SECOND_LEG" || economicIntent === "PROTECTIVE_DE_RISK" || economicIntent === "RESCUE_SECOND_LEG" || economicIntent === "GUARDIAN_PROTECTIVE_SELL") {
            if (economicIntent !== "GUARDIAN_PROTECTIVE_SELL" && this.confirmedGuardianProtection(fill.marketId)) {
              throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, economicIntent, "GUARDIAN_PRIORITY");
            }
            // DURABLE CANCEL WINS over an older in-flight HEDGE maker fill. A
            // hedge maker fill derived before rescue ownership must not commit
            // once the rescue durably cancelled the resting order: its
            // `restingAfter` payload would resurrect the cancelled order
            // without its reservation (resting_without_reservation invariant).
            if (economicIntent === "HEDGE_SECOND_LEG" && fill.liquidityRole === "MAKER") {
              const activeHedgeResting = this.restingOrders.get(fill.orderId);
              if (
                !activeHedgeResting
                || !d(activeHedgeResting.remaining).gt(0)
                || this.restingOrderIntent(activeHedgeResting) !== "HEDGE_SECOND_LEG"
              ) {
                throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, "HEDGE_SECOND_LEG", "HEDGE_MAKER_NOT_ACTIVE");
              }
            }
            // Protected actions (protective SELL and rescue pair-lock BUY)
            // re-validate hard safety immediately before the durable economic
            // mutation. A flip since risk evaluation makes this a state-changed
            // rejection (bounded, deterministic), never a durability fault.
            if (economicIntent === "PROTECTIVE_DE_RISK" || economicIntent === "RESCUE_SECOND_LEG" || economicIntent === "GUARDIAN_PROTECTIVE_SELL") {
              const hardSafetyReason = this.protectiveFillHardSafetyReason(fill);
              if (hardSafetyReason) {
                this.recordProtectiveDeRiskSuppressed(
                  fill.marketId, "FINAL_FENCE", hardSafetyReason,
                  { snapshot: this.snapshots.get(fill.marketId)?.[fill.outcome] ?? null },
                );
                throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, economicIntent, hardSafetyReason);
              }
            }
            if (!currentPosition?.unpairedOutcome || !d(currentPosition.unpairedQuantity).gt(0)) {
              throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, economicIntent, "NO_UNPAIRED_RESIDUAL");
            }
            if (d(fill.quantity).gt(currentPosition.unpairedQuantity)) {
              throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, economicIntent, "QUANTITY_EXCEEDS_RESIDUAL");
            }
            if (economicIntent === "HEDGE_SECOND_LEG" || economicIntent === "RESCUE_SECOND_LEG") {
              if (fill.side !== "BUY") {
                throw new RiskReductionLifecycleGuardRejectedError(
                  fill.marketId, economicIntent,
                  economicIntent === "HEDGE_SECOND_LEG" ? "HEDGE_SIDE_CHANGED" : "RESCUE_SIDE_CHANGED",
                );
              }
              if (fill.outcome === currentPosition.unpairedOutcome) {
                throw new RiskReductionLifecycleGuardRejectedError(
                  fill.marketId, economicIntent,
                  economicIntent === "HEDGE_SECOND_LEG" ? "HEDGE_OUTCOME_CHANGED" : "RESCUE_OUTCOME_CHANGED",
                );
              }
              // Token identity revalidation immediately before the durable
              // economic mutation: the fill token must still be the canonical /
              // current token for the required opposite outcome in this market.
              const expectedHedgeToken = this.currentTokenIdFor(fill.marketId, fill.outcome);
              if (!expectedHedgeToken || expectedHedgeToken !== fill.tokenId) {
                throw new RiskReductionLifecycleGuardRejectedError(
                  fill.marketId,
                  economicIntent,
                  expectedHedgeToken
                    ? (economicIntent === "RESCUE_SECOND_LEG" ? "RESCUE_TOKEN_MISMATCH" : "HEDGE_TOKEN_MISMATCH")
                    : (economicIntent === "RESCUE_SECOND_LEG" ? "RESCUE_TOKEN_UNRESOLVABLE" : "HEDGE_TOKEN_UNRESOLVABLE"),
                );
              }
            } else {
              if (fill.side !== "SELL") {
                throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, economicIntent, "DE_RISK_SIDE_CHANGED");
              }
              if (fill.outcome !== currentPosition.unpairedOutcome) {
                throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, economicIntent, "DE_RISK_OUTCOME_CHANGED");
              }
            }
            if (economicIntent === "GUARDIAN_PROTECTIVE_SELL" && (!owner || !d(fill.quantity).eq(currentPosition.unpairedQuantity))) {
              throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, economicIntent, "GUARDIAN_FULL_RESIDUAL_REQUIRED");
            }
            if (economicIntent === "GUARDIAN_PROTECTIVE_SELL" && !this.freshEarlyCatastrophicSnapshot(
              this.state.marketSnapshots.get(fill.tokenId) ?? null, this.nowFn().getTime())) {
              throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, economicIntent, "GUARDIAN_EXECUTION_BOOK_UNAVAILABLE");
            }
            if (extras.deRisk?.deRiskReason === "REVERSAL_CONFIRMED"
              && (!owner || !this.reversalOrderEligible(extras.deRisk, currentPosition, this.nowFn().getTime())
                || !d(fill.quantity).eq(currentPosition.unpairedQuantity))) {
              throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, economicIntent, "REVERSAL_EPISODE_CHANGED");
            }
            // PR3 EARLY catastrophic pair-lock final fence: decided-state is not
            // enough â€” revalidate immediately before the durable mutation that
            // the fill still covers the ENTIRE current residual and that the
            // ACTUAL fee-inclusive locked PnL (from the executed fill economics)
            // stays within the configured loss cap. Any violation is a normal
            // state-change rejection, never a durability fault.
            if (economicIntent === "RESCUE_SECOND_LEG" && extras.deRisk?.deRiskReason === "EARLY_CATASTROPHIC_PAIR_LOCK") {
              if (!d(fill.quantity).eq(d(currentPosition.unpairedQuantity))) {
                throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, "RESCUE_SECOND_LEG", "EARLY_CATASTROPHIC_PARTIAL_FILL");
              }
              // Pre-deadline ownership is part of the accounting invariant: an
              // early pair-lock decided before the deadline must never commit
              // once the old protective deadline owns the residual.
              if (this.protectiveDeRiskEligibility(
                fill.marketId,
                currentPosition.unhedgedStartedAt ?? currentPosition.openedAt,
                this.nowFn().getTime(),
              ).eligible) {
                throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, "RESCUE_SECOND_LEG", "EARLY_CATASTROPHIC_DEADLINE_OWNERSHIP");
              }
              const earlyLockedPnl = computeGuaranteedLockedPnl({
                position: currentPosition,
                rescueQuantity: fill.quantity,
                rescueVwap: fill.fillPrice,
                rescueTakerFeeUsd: fill.feeUsd,
              });
              if (d(earlyLockedPnl).lt(d(-this.riskLimits.catastrophicPairLockMaxLockedLossUsd))) {
                this.recordProtectiveDeRiskSuppressed(
                  fill.marketId, "FINAL_FENCE", "EARLY_CATASTROPHIC_LOSS_CAP_EXCEEDED",
                  { snapshot: this.snapshots.get(fill.marketId)?.[fill.outcome] ?? null },
                );
                throw new RiskReductionLifecycleGuardRejectedError(fill.marketId, "RESCUE_SECOND_LEG", "EARLY_CATASTROPHIC_LOSS_CAP_EXCEEDED");
              }
            }
            return;
          }

          if (economicIntent !== "OPEN_FIRST_LEG") return;
          // Production fills carry the execution book; observed-fill is a
          // test-only economic fixture injection, not order execution.
          if (executionReason) throw new FirstLegLifecycleGuardRejectedError(fill.marketId, executionReason);
          if (executionBook && this.currentTokenIdFor(fill.marketId, fill.outcome) !== fill.tokenId)
            throw new FirstLegLifecycleGuardRejectedError(fill.marketId, "ENTRY_BOOK_IDENTITY_CHANGED");
          if (fill.liquidityRole === "TAKER") {
            if (currentPosition) {
              throw new FirstLegLifecycleGuardRejectedError(fill.marketId, "POSITION_ALREADY_EXISTS");
            }
            return;
          }
          const currentResting = this.restingOrders.get(fill.orderId);
          if (
            !currentResting
            || this.restingOrderIntent(currentResting) !== "OPEN_FIRST_LEG"
          ) {
            throw new FirstLegLifecycleGuardRejectedError(fill.marketId, "MAKER_NOT_ACTIVE");
          }
          if (currentPosition && (d(currentPosition.pairedQuantity).gt(0)
            || (currentResting.order.makerFirst?.dualGroupId && d(currentPosition.unpairedQuantity).gt(0)))) {
            throw new FirstLegLifecycleGuardRejectedError(fill.marketId, "PAIRING_ALREADY_STARTED");
          }
        },
        buildPayload: (occurredAt, existingFill) => {
          const previousAtCommit = this.ledger.positions.get(fill.marketId);
          const wasEmptyAtCommit = !previousAtCommit
            || (!d(previousAtCommit.upQuantity).gt(0) && !d(previousAtCommit.downQuantity).gt(0));
          // wasEmpty only drives derived audit classification and is deliberately
          // excluded from the authoritative transition payload. Keeping the
          // replay payload to stable operation facts lets a compacted retry
          // reconstruct the same hash after the live projection has advanced.
          const unhedgedStartedAtBefore = previousAtCommit?.unhedgedStartedAt ?? previousAtCommit?.openedAt ?? null;
          const auth = {
            fill, positionId, reservationAfter, restingAfter,
            ...(this.nakedDirectionalSoakEnabled() && fill.side === "BUY"
              && (economicIntent === "OPEN_FIRST_LEG" || (!economicIntent && wasEmptyAtCommit))
              ? { nakedDirectionalSoak: true } : {}),
            ...(settlementLookup ? { settlementLookup } : {}),
            ...(economicIntent ? { intent: economicIntent } : {}),
            ...(extras.deRisk ? { deRisk: extras.deRisk } : {}),
          };
          return {
            ...auth,
            derivedAudit: existingFill ? [] : this.projectDerivedAudit(
              { operationKey: `fill:${fill.id}`, type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: fill.id, payload: auth },
              occurredAt,
              (projected) => this.buildFillDerivedAudit(
                fill,
                wasEmptyAtCommit,
                projected,
                occurredAt,
                economicIntent,
                extras.deRisk,
                unhedgedStartedAtBefore,
                executionBook && executionOrder ? {
                  ...(executionOrder.makerFirst?.dualGroupId
                    ? { entryPolicy:"TWO_SIDED_MAKER",dualGroupId:executionOrder.makerFirst.dualGroupId,
                        executablePrice:fill.fillPrice,quantity:fill.quantity,reason:null,conditionalPairOnly:true }
                    : firstLegExecutionEconomics({ ...executionOrder, quantity: fill.quantity }, executionBook, executionAt.getTime())),
                  checkedAt: executionAt.toISOString(), book: { marketId: executionBook.marketId, tokenId: executionBook.tokenId,
                    outcome: executionBook.outcome, eventId: executionBook.eventId, sequence: executionBook.sequence, hash: executionBook.hash,
                    sourceTimestamp: executionBook.sourceTimestamp, receivedAt: executionBook.receivedAt, wsCallbackAt: executionBook.wsCallbackAt ?? null },
                  timing: bookTiming(executionBook, executionAt.getTime()), maxBookAgeMs: this.riskLimits.staleFeedTimeoutMs,
                  marketSlug: this.marketsById.get(fill.marketId)?.slug ?? null,
                  startVerified: officialMarketStartReason(this.marketsById.get(fill.marketId), executionAt.getTime()) === null,
                } : undefined,
              ),
            ),
          };
        },
        afterApply: async () => {
          this.observeSoakPosition(fill.marketId);
          if(fill.makerEvidence){
            if(!this.restingOrders.has(fill.orderId)){this.makerActiveAfter.delete(fill.orderId);this.makerQueuePrimed.delete(fill.orderId);}
            this.scheduleMakerQuoteExpiry();
          }
          this.record("PAPER_FILL", fill.traceId, fill.marketId, { fillId: fill.id, orderId: fill.orderId, price: fill.fillPrice, quantity: fill.quantity, role: fill.liquidityRole,
            ...(economicIntent ? { intent: economicIntent } : {}) });
          this.metrics.paperFills.inc({ role: fill.liquidityRole });
          const position = this.ledger.positions.get(fill.marketId);
          if (position) {
            for (const trigger of this.triggerEngine.ingestPosition(position, fill.traceId, this.assetByMarket.get(fill.marketId) ?? null)) {
              this.admitTrigger(trigger);
            }
          }
        },
      });
    } catch (error) {
      if (error instanceof RiskReductionLifecycleGuardRejectedError && fill.liquidityRole === "MAKER") {
        // A resting hedge maker that raced a protective SELL (or another hedge)
        // is a normal state change. Cancel the stale remainder so it cannot
        // refill directional exposure behind the newer authority, audit it, and
        // reject without an economic mutation. TAKER fills propagate to the
        // caller's STATE_CHANGED audit instead.
        this.record("RISK_REDUCTION_LIFECYCLE_BLOCKED", fill.traceId, fill.marketId, {
          fillId: fill.id, orderId: fill.orderId, intent: error.intent, reason: error.reason,
        });
        if (this.restingOrders.has(fill.orderId)) {
          await this.releaseMaker(fill.orderId, { cancel: true, reason: "RISK_REDUCTION_LIFECYCLE_CLOSED" });
        }
        return false;
      }
      if (!(error instanceof FirstLegLifecycleGuardRejectedError)) throw error;
      if (fill.liquidityRole === "TAKER") await this.rejectFirstLegOrder({ id: fill.orderId, marketId: fill.marketId, traceId: fill.traceId }, error.reason, "COMMIT_FENCE");
      this.record("FIRST_LEG_LIFECYCLE_BLOCKED", fill.traceId, fill.marketId, {
        fillId: fill.id,
        orderId: fill.orderId,
        reason: error.reason,
      });
      if (fill.liquidityRole === "MAKER" && this.restingOrders.has(fill.orderId)) {
        await this.releaseMaker(fill.orderId, {
          cancel: true,
          reason: "FIRST_LEG_LIFECYCLE_CLOSED",
        });
      }
      return false;
    }
    if (economicIntent === "HEDGE_SECOND_LEG" || economicIntent === "PROTECTIVE_DE_RISK" || economicIntent === "RESCUE_SECOND_LEG" || economicIntent === "GUARDIAN_PROTECTIVE_SELL") {
      await this.cancelOutstandingFirstLegMakers(
        fill.marketId,
        (economicIntent === "PROTECTIVE_DE_RISK" || economicIntent === "GUARDIAN_PROTECTIVE_SELL") ? `${economicIntent}_FILLED`
          : economicIntent === "RESCUE_SECOND_LEG" ? "RESCUE_FILLED"
          : "HEDGE_FILLED",
      );
    }
    return true;
  }

  private buildFillDerivedAudit(
    fill: Parameters<PaperLedger["applyFill"]>[0],
    wasEmpty: boolean,
    projected: RecoverableEngineState,
    occurredAt: Date,
    intent?: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG" | "GUARDIAN_PROTECTIVE_SELL",
    deRisk?: ProtectiveDeRiskAudit,
    unhedgedStartedAtBefore: string | null = null,
    entryAdmission?: Record<string, unknown>,
  ): DerivedAuditRecord[] {
    const position = projected.ledger.positions.get(fill.marketId);
    const experimentId = projected.experiment?.id ?? this.experiment?.id ?? null;
    const records: DerivedAuditRecord[] = [{
      id: derivedAuditEventId("PAPER_FILL", fill.id),
      type: "PAPER_FILL",
      aggregateType: "paper_fill",
      aggregateId: fill.id,
      traceId: fill.traceId,
      payload: {
        ...fill,
        asset: this.assetByMarket.get(fill.marketId) ?? null,
        ...(intent ? { intent } : {}),
        ...(intent === "OPEN_FIRST_LEG" && entryAdmission ? { entryAdmission } : {}),
        ...(deRisk ? {
          deRisk: this.finalizeProtectiveDeRiskAudit(deRisk, fill, position),
        } : {}),
      },
      sourceEventId: fill.sourceMarketEventId,
    }];
    if (intent === "OPEN_FIRST_LEG" && (fill.liquidityRole === "TAKER" || !projected.restingOrders.has(fill.orderId))) {
      records.push({ id: derivedAuditEventId("PAPER_ORDER_TERMINAL", fill.orderId), type: "PAPER_ORDER_TERMINAL",
        aggregateType: "paper_order", aggregateId: fill.orderId, traceId: fill.traceId,
        payload: { orderId: fill.orderId, marketId: fill.marketId, intent, status: "FILLED", reason: "EXECUTED",
          stage: "DURABLE_FILL", fillId: fill.id, terminalAt: occurredAt.toISOString(), experimentId } });
    }
    if (deRisk?.guardianSensor && (!position?.unpairedOutcome || d(position.unpairedQuantity).eq(0))) {
      records.push({ id: derivedAuditEventId("GUARDIAN_EXECUTION_COMPLETED", fill.id), type: "GUARDIAN_EXECUTION_COMPLETED",
        aggregateType: "position", aggregateId: deRisk.positionId ?? fill.marketId, traceId: fill.traceId,
        payload: { marketId: fill.marketId, positionId: deRisk.positionId, experimentId,
          sensor: deRisk.guardianSensor, reason: deRisk.guardianReason, selection: deRisk.selection,
          unhedgedStartedAt: deRisk.unhedgedStartedAt, orderId: fill.orderId, fillId: fill.id } });
    }
    if (wasEmpty && fill.side === "BUY" && position) {
      records.push({
        id: derivedAuditEventId("FIRST_LEG_FILLED", fill.id),
        type: "FIRST_LEG_FILLED",
        aggregateType: "position",
        aggregateId: position.id,
        traceId: fill.traceId,
        payload: {
          positionId: position.id,
          marketId: fill.marketId,
          unpairedQuantity: position.unpairedQuantity,
          unhedgedStartedAt: position.unhedgedStartedAt ?? position.openedAt,
          experimentId,
        },
      });
    }
    if (position && Number(position.pairedQuantity) > 0 && Number(position.unpairedQuantity) === 0) {
      const episodeStart = unhedgedStartedAtBefore ?? position.openedAt;
      const unhedgedDurationMs = Math.max(0, occurredAt.getTime() - new Date(episodeStart).getTime());
      records.push({
        id: derivedAuditEventId("HEDGE_COMPLETE", fill.id),
        type: "HEDGE_COMPLETE",
        aggregateType: "position",
        aggregateId: position.id,
        traceId: fill.traceId,
        payload: {
          positionId: position.id,
          marketId: fill.marketId,
          pairedQuantity: position.pairedQuantity,
          unhedgedDurationMs,
          completionMode: intent === "PROTECTIVE_DE_RISK" ? "PROTECTIVE_DE_RISK" : "PAIR_HEDGE",
          ...(intent ? { intent } : {}),
          ...(deRisk ? { deRisk: this.finalizeProtectiveDeRiskAudit(deRisk, fill, position) } : {}),
          experimentId,
        },
      });
    }
    if (position) {
      records.push({
        id: derivedAuditEventId("POSITION_UPDATED", fill.id),
        type: "POSITION_UPDATED",
        aggregateType: "position",
        aggregateId: position.id,
        traceId: fill.traceId,
        payload: {
          ...structuredClone(position),
          ...(deRisk ? { deRisk: this.finalizeProtectiveDeRiskAudit(deRisk, fill, position) } : {}),
        },
        sourceEventId: fill.id,
      });
    } else if (intent === "GUARDIAN_PROTECTIVE_SELL") {
      // A full SELL removes an empty position from PaperLedger. Report that
      // actual terminal result in the same fill commit; never recreate it in
      // the ledger merely to produce a POSITION_UPDATED audit.
      const previous = this.ledger.positions.get(fill.marketId)!;
      records.push({
        id: derivedAuditEventId("POSITION_UPDATED", fill.id), type: "POSITION_UPDATED",
        aggregateType: "position", aggregateId: previous.id, traceId: fill.traceId,
        sourceEventId: fill.id,
        payload: { ...structuredClone(previous), upQuantity: "0", downQuantity: "0",
          pairedQuantity: "0", unpairedQuantity: "0", unpairedOutcome: null, unhedgedStartedAt: null,
          totalFees: d(previous.totalFees).plus(fill.feeUsd).toFixed(),
          totalSlippage: d(previous.totalSlippage).plus(fill.slippageUsd).toFixed(),
          updatedAt: fill.timestamp, closed: true, intent, experimentId,
          ...(deRisk ? { deRisk: this.finalizeProtectiveDeRiskAudit(deRisk, fill, undefined) } : {}) },
      });
    }
    records.push({
      id: derivedAuditEventId("PNL_SNAPSHOT", fill.id),
      type: "PNL_SNAPSHOT",
      aggregateType: "ledger",
      aggregateId: experimentId ?? "paper",
      traceId: fill.traceId,
      payload: { ...projected.ledger.snapshot(), experimentId, source: "FILL", fillId: fill.id,
        economicResults: projected.resultAccounting ? economicResults(projected.resultAccounting) : null,
        tradingResults: projected.resultAccounting ? tradingResults(projected.resultAccounting) : null },
    });
    const completedOwner = this.riskReductionOwners.get(fill.marketId);
    if (completedOwner?.order.id === fill.orderId) {
      records.push(this.ownershipAudit(completedOwner, "RISK_REDUCTION_COMPLETED"));
    }
    return records;
  }

  private finalizeProtectiveDeRiskAudit(
    audit: ProtectiveDeRiskAudit,
    fill: Parameters<PaperLedger["applyFill"]>[0],
    position: Position | undefined,
  ): ProtectiveDeRiskAudit {
    const residualQuantity = position?.unpairedQuantity ?? "0";
    const residualExposure = position?.unpairedOutcome
      ? d(position.unpairedQuantity)
        .mul(position.unpairedOutcome === "UP" ? position.averageUpCost : position.averageDownCost)
        .toFixed()
      : "0";
    return {
      ...audit,
      executionPrice: fill.fillPrice,
      residualQuantity,
      residualExposure,
      finalState: d(residualQuantity).gt(0) ? "RESIDUAL_UNHEDGED" : "RISK_NEUTRALIZED",
    };
  }

  private async persistExperimentWsEvent(input: ExperimentWsEvent): Promise<void> {
    const event = experimentWsEventSchema.parse(input), experimentId = this.experiment?.id;
    if (!experimentId || event.kind === "STOPPED" || event.kind === "STALE"
      || ((event.kind === "CONNECTED" || event.kind === "CONNECTING") && !event.retry)) return;
    // Enqueue at callback arrival, on the same authority as entry-close and
    // completion. No polling, per-book writes, or process-counter subtraction.
    await this.durableGate.run(async () => {
      const current = this.experiment;
      if (!current || current.id !== experimentId || !["RUNNING", "ENTRY_CLOSED"].includes(current.status)
        || !current.startedAt || Date.parse(event.at) < Date.parse(current.startedAt)) return;
      await this.commitAuthoritative({ operationKey: `experiment-ws:${experimentId}:${event.eventId}`,
        type: "EXPERIMENT_WS_EVENT", aggregateType: "experiment", aggregateId: experimentId,
        buildPayload: () => ({ experimentId, event, derivedAudit: [{
          id: derivedAuditEventId("EXPERIMENT_WS_EVENT", event.eventId), type: "EXPERIMENT_WS_EVENT",
          aggregateType: "experiment", aggregateId: experimentId, traceId: experimentId,
          payload: { experimentId, event },
        }] }),
      });
    });
  }

  private async onConnection(event: FeedConnectionEvent): Promise<void> {
    this.noteSoakAnomaly(event.reconnectCount > 0 ? "RECONNECT" : `FEED_${event.state}`, { ...event }, undefined, event.asset ?? undefined);
    if (event.feed === "polymarket") {
      if (event.state !== "CONNECTED") this.noteExperimentFeedInvalidation();
      const connection = this.state.connections.polymarket;
      if (event.reconnectCount > connection.reconnects) this.metrics.wsReconnects.inc({ feed: "polymarket" }, event.reconnectCount - connection.reconnects);
      connection.state = event.state;
      connection.reconnects = event.reconnectCount;
    } else {
      const asset = event.asset ?? (event.feed === "btc" ? "BTC" : "unknown");
      this.state.noteReferenceConnection(asset, event.state, event.reconnectCount);
      if (asset === "BTC") {
        const connection = this.state.connections.btc;
        if (event.reconnectCount > connection.reconnects) this.metrics.wsReconnects.inc({ feed: "btc" }, event.reconnectCount - connection.reconnects);
        connection.state = event.state;
        connection.reconnects = event.reconnectCount;
      }
      if (event.state === "STALE") {
        this.noteExperimentFeedInvalidation();
        this.noteReferenceLogicalLoss(asset);
        this.state.invalidateReferenceContext(asset);
        this.triggerEngine.resetReferenceAfterReconnect(asset);
        if (event.message?.includes("REFERENCE_INGRESS_OVERFLOW")) this.requestReferenceResync(asset);
      } else if (event.state === "DISCONNECTED") {
        this.noteExperimentFeedInvalidation();
        const kind = /kind=([a-z_]+)/.exec(event.message ?? "")?.[1] ?? "unknown";
        this.metrics.referenceCloses.inc({ kind });
        if (event.reconnectCount > 0) this.metrics.wsReconnects.inc({ feed: "reference" });
        this.noteReferenceLogicalLoss(asset);
        this.state.invalidateReferenceContext(asset);
        this.triggerEngine.resetReferenceAfterReconnect(asset);
      } else if (event.state === "CONNECTED") {
        this.referenceLogicalGapped.delete(asset);
        if (this.referenceUnreliable.has(asset) || this.triggerEngine.referenceWindowOverflowed(asset)) {
          this.triggerEngine.resetReferenceAfterReconnect(asset);
        }
      }
    }
    this.updateReady();
    this.record(`WS_${event.state}`, createId("trace"), null, { feed: event.feed, asset: event.asset ?? null, reconnects: event.reconnectCount, message: event.message }, event.asset ?? null);
  }

  private onPolymarketAnomaly(anomaly: PolymarketFeedAnomaly): void {
    this.noteSoakAnomaly(`BOOK_${anomaly.kind.toUpperCase()}`, { ...anomaly },
      anomaly.tokenId ? this.state.marketSnapshots.get(anomaly.tokenId)?.marketId : undefined);
    if (anomaly.kind === "stale") {
      this.metrics.staleEventsDropped.inc();
      if (anomaly.regressionMs != null) this.metrics.staleEventRegression.observe(anomaly.regressionMs);
    } else {
      this.metrics.malformedMarketEvents.inc();
    }
    if (!anomaly.sampled) return;
    const now = Date.now();
    if (now - this.lastAnomalyJournalAt < 5_000) return;
    this.lastAnomalyJournalAt = now;
    this.logger.error({
      tokenId: anomaly.tokenId, kind: anomaly.kind, regressionMs: anomaly.regressionMs,
      incomingMs: anomaly.incomingMs, lastAcceptedMs: anomaly.lastAcceptedMs, totalDropped: anomaly.totalDropped,
    }, "polymarket event dropped without reconnect");
    void this.safeAppend(createJournalEvent({
      traceId: createId("trace"), type: "SYSTEM_HEALTH_EVENT", aggregateType: "feed", aggregateId: "polymarket",
      payload: {
        status: "TIMESTAMP_ANOMALY", kind: anomaly.kind, tokenId: anomaly.tokenId,
        regressionMs: anomaly.regressionMs, totalDropped: anomaly.totalDropped, reconnect: false,
      },
    }));
  }

  private async onFeedError(feed: string, error: Error): Promise<void> {
    this.noteSoakAnomaly(/slow.consumer|overflow/i.test(error.message) ? "SLOW_CONSUMER" : "FEED_ERROR", { feed, message: error.message.slice(0, 512) });
    const close = error instanceof ClobMarketCloseError ? { close: error.diagnostic } : {};
    this.logger.error({ err: { name: error.name, message: error.message }, feed, ...close }, "market feed error");
    await this.safeAppend(createJournalEvent({
      traceId: createId("trace"), type: "SYSTEM_HEALTH_EVENT", aggregateType: "feed", aggregateId: feed,
      payload: { status: "ERROR", message: error.message, ...close },
    }));
  }

  private updateReady(): void {
    this.state.ready = this.state.engineAlive();
    this.state.setMarketMonitoring(this.tokenIds.length > 0 && this.state.connections.polymarket.state === "CONNECTED");
  }

  private usesDiscovery(): boolean {
    return splitList(this.env.POLYMARKET_TOKEN_IDS).length === 0;
  }

  private record(type: string, traceId: string, marketId: string | null, detail: Record<string, unknown>, asset: string | null = null): void {
    const telemetry = type === "MARKET_EVENT" || type === "REFERENCE_TICK";
    if (telemetry) {
      // Raw feed observations are intentionally not retained. Avoid allocating
      // an ID/timestamp/feed item, and do not churn the zero-subscriber SSE
      // counter millions of times. A connected SSE client still gets the same
      // coalesced authoritative snapshot.
      if (this.sse.hasSubscribers()) this.sse.publish("snapshot", () => this.state.snapshot());
      return;
    }
    const item = { id: createId("feed"), at: new Date().toISOString(), type, traceId, marketId, asset, detail };
    this.state.record(item);
    this.sse.publish("decision", item);
    this.sse.publish("snapshot", () => this.state.snapshot());
  }

  private isExperimentWindowOpen(): boolean {
    if (this.experimentStartPending || this.experiment?.startValidationPending === true) return false;
    const status = this.experiment?.status;
    return status === "RUNNING" || status === "ENTRY_CLOSED";
  }

  /**
   * Admit a bounded, non-economic scout refresh from trusted live state.
   * Refresh contexts never enter activeTriggers, trigger admission, or the
   * durable journal. They exist only until the asynchronous role fanout has
   * returned, with role TTL/headroom, a small retry gap, and the shared
   * actual-attempt budget preventing a queue per tick.
   */
  private requestScoutRefresh(snapshot: MarketSnapshot, source: "market" | "reference" | "news"): void {
    // Background research is an entry-path concern. ENTRY_CLOSED may still
    // process hedge/settlement work, but it must not keep paid scouts warm.
    if (this.env.GROK_ACTIVATION_PROVIDER !== "xai"
      || this.experiment?.modelConfiguration.authority === NATIVE_DETERMINISTIC_AUTHORITY
      || this.experiment?.status !== "RUNNING"
      || !experimentAllowsFirstLeg(this.experiment, this.nowFn())
      || (this.experiment.entryClosesAt !== null && this.nowFn().getTime() >= new Date(this.experiment.entryClosesAt).getTime())) return;
    // No bounded news context is produced by ordinary market/reference
    // ingress. Never spend a call to manufacture a "no news" answer.
    if (source === "news") {
      this.metrics.scoutRefreshSkipped.inc({ reason: "NO_NEWS_CONTEXT" });
      return;
    }
    const pair = this.snapshots.get(snapshot.marketId);
    if (!pair?.UP || !pair.DOWN) return;
    const now = this.nowFn();
    const nowMs = now.getTime();
    this.pruneScoutRefreshState(nowMs);
    const scope = `${snapshot.marketId}:${snapshot.asset ?? this.assetByMarket.get(snapshot.marketId) ?? "none"}`;
    const ownedRole: ExternalAgentRoleId = source === "market"
      ? "market_microstructure_scanner"
      : "crypto_market_scanner";
    const dueRoles = this.hotResearchCache.refreshDueFor(snapshot.marketId, snapshot.asset ?? null, [ownedRole]);
    if (dueRoles.length === 0 || this.scoutRefreshInFlight.has(scope)) return;
    const roleKey = `${scope}:${ownedRole}`;
    const lastAt = this.scoutRefreshLastAt.get(roleKey);
    // This is a research retry gap, deliberately independent of the
    // economic trigger cooldown. The usable TTL/headroom decision above is
    // role-specific; this gap only prevents a failed call from hot-looping.
    if (lastAt !== undefined && nowMs - lastAt < SCOUT_REFRESH_RETRY_GAP_MS) return;
    const budget = this.activation.runtimeBudgetSnapshot();
    if (budget && budget.remaining <= 0) {
      this.metrics.scoutRefreshSkipped.inc({ reason: "BUDGET_EXHAUSTED" });
      return;
    }

    const triggerId = `research_refresh_${snapshot.marketId}_${nowMs}`;
    const expiresAt = new Date(nowMs + Math.min(this.triggerConfig.triggerTtlMs, 60_000)).toISOString();
    const trigger = triggerEventSchema.parse({
      id: triggerId,
      traceId: snapshot.traceId,
      marketId: snapshot.marketId,
      asset: snapshot.asset ?? this.assetByMarket.get(snapshot.marketId) ?? null,
      source: "polymarket",
      kind: "BOOK_IMBALANCE",
      severity: 1,
      score: 0,
      dedupeKey: `research_refresh:${scope}`,
      evidence: { source: "deterministic_market_state", refresh: true, sequence: snapshot.sequence },
      sourceEventIds: [snapshot.eventId],
      evidenceIds: [snapshot.eventId],
      createdAt: now.toISOString(),
      expiresAt,
    });
    const request = grokActivationRequestSchema.parse({
      eventType: "GROK_RESEARCH_REQUEST",
      triggerId,
      traceId: snapshot.traceId,
      marketId: snapshot.marketId,
      asset: trigger.asset ?? null,
      severity: 1,
      triggerKind: trigger.kind,
      createdAt: trigger.createdAt,
      expiresAt: trigger.expiresAt,
      targetRoles: [ownedRole],
      researchRefresh: true,
    });
    this.scoutRefreshContexts.set(triggerId, trigger);
    this.scoutRefreshLastAt.set(roleKey, nowMs);
    this.scoutRefreshInFlight.add(scope);
    this.pruneScoutRefreshState(nowMs);
    void this.activation.dispatch(request)
      .then((results) => {
        for (const result of results) this.metrics.activations.inc({ provider: result.provider, status: result.ok ? "ok" : "failed" });
      })
      .catch((error) => this.logger.error({ err: error, marketId: snapshot.marketId }, "bounded scout refresh failed"))
      .finally(() => {
        this.scoutRefreshInFlight.delete(scope);
        this.scoutRefreshContexts.delete(triggerId);
      });
  }

  private pruneScoutRefreshState(nowMs: number): void {
    for (const [triggerId, trigger] of this.scoutRefreshContexts) {
      if (Date.parse(trigger.expiresAt) <= nowMs) this.scoutRefreshContexts.delete(triggerId);
    }
    while (this.scoutRefreshContexts.size > SCOUT_REFRESH_CONTEXT_MAX) {
      const oldest = this.scoutRefreshContexts.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.scoutRefreshContexts.delete(oldest);
    }
    for (const [scope, at] of this.scoutRefreshLastAt) {
      if (nowMs - at >= SCOUT_REFRESH_STATE_TTL_MS) this.scoutRefreshLastAt.delete(scope);
    }
    for (const [key, at] of this.scoutRefreshSubmissionKeys) {
      if (nowMs - at >= SCOUT_REFRESH_STATE_TTL_MS) this.scoutRefreshSubmissionKeys.delete(key);
    }
    while (this.scoutRefreshSubmissionKeys.size > SCOUT_REFRESH_CONTEXT_MAX * 2) {
      const oldest = this.scoutRefreshSubmissionKeys.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.scoutRefreshSubmissionKeys.delete(oldest);
    }
  }

  private noteExperimentFeedInvalidation(): void {
    this.experimentFeedGeneration += 1;
  }

  private pruneIdleGrokSubmissions(nowMs: number): void {
    for (const [key, entry] of this.idleGrokSubmissions) {
      if (nowMs - entry.observedAt > IDLE_GROK_IDEMPOTENCY_TTL_MS) this.idleGrokSubmissions.delete(key);
    }
    while (this.idleGrokSubmissions.size > IDLE_GROK_IDEMPOTENCY_MAX) {
      const oldest = this.idleGrokSubmissions.keys().next().value as string | undefined;
      if (oldest === undefined) break;
      this.idleGrokSubmissions.delete(oldest);
    }
  }

  private reserveIdleGrokSubmission(key: string, nowMs: number): boolean {
    this.pruneIdleGrokSubmissions(nowMs);
    if (this.idleGrokSubmissions.has(key)) return false;
    this.idleGrokSubmissions.set(key, { observedAt: nowMs, pending: true });
    this.pruneIdleGrokSubmissions(nowMs);
    return this.idleGrokSubmissions.has(key);
  }

  private completeIdleGrokSubmission(key: string, nowMs: number): void {
    const entry = this.idleGrokSubmissions.get(key);
    if (entry?.pending) {
      entry.pending = false;
      entry.observedAt = nowMs;
    }
  }

  private releaseIdleGrokSubmission(key: string): void {
    if (this.idleGrokSubmissions.get(key)?.pending) this.idleGrokSubmissions.delete(key);
  }

  private isCanonicalWindowAdmission(type: string): boolean {
    return isReportInputEventType(type) && !isReportInputBoundaryType(type) && this.isExperimentWindowOpen();
  }

  /**
   * Canonical report inputs are durable only while an experiment window is
   * open. Lifecycle exceptions remain durable at the boundaries; raw market
   * and reference observations never come through this path.
   */
  private isReportInputPersistenceAdmitted(type: string): boolean {
    // These are live processing inputs, never durable facts. Keep this guard
    // here as a second line of defense if a future producer reaches the
    // report-input helper directly.
    if (type === "MARKET_SNAPSHOT" || type === "REFERENCE_TICK") return false;
    // MCP_INTERACTION is an always-on security/audit trail. It is classified
    // as a report input for source-integrity accounting, but it must remain
    // durable for authenticated and rejected calls while the engine monitors.
    return type === "MCP_INTERACTION"
      || !isReportInputEventType(type)
      || isReportInputBoundaryType(type)
      || this.isExperimentWindowOpen();
  }

  async persistReportInput(event: JournalEvent, signal?: AbortSignal, extra?: { deadlineAt?: number }): Promise<void> {
    if (!this.isReportInputPersistenceAdmitted(event.type)) return;
    const experimentId = this.experiment?.id;
    const stamped = this.isExperimentWindowOpen() && experimentId && typeof event.payload.experimentId !== "string"
      ? { ...event, payload: { ...event.payload, experimentId } }
      : event;
    if (stamped.type === "CONSENSUS_DECISION") {
      try {
        if (await this.persistReversalConsensus(stamped)) return;
      } catch (error) {
        if (error instanceof CrashInjectedError) throw error;
        if (shouldLatchReportSourceIntegrity(stamped, this.experiment)) await this.noteReportSourceIntegrityFault(stamped, error);
        this.failClosedIntegrity(error);
        throw error;
      }
    }
    await this.safeAppend(stamped, signal, extra);
  }

  private async safeAppend(event: ReturnType<typeof createJournalEvent>, signal?: AbortSignal, extra?: { deadlineAt?: number }): Promise<void> {
    const critical = isCriticalPersistenceType(event.type);
    const journalClass = classifyJournalMetricClass(event.type);
    const reportInput = this.isCanonicalWindowAdmission(event.type);
    try {
      await this.journalAdmission.run(journalClass, critical, async () => {
        if (signal?.aborted) throw new JournalAdmissionRejected(journalClass, critical, "JOURNAL_ADMISSION_CLOSED");
        if (event.type === "TRIGGER_EVENT" && this.triggerEventPersistHold) await this.triggerEventPersistHold;
        await this.metrics.observeDirectJournal(event.type, async () => {
          await this.appendWithTransportRecovery(event, { signal, deadlineAt: extra?.deadlineAt });
          if (critical) this.state.database = "UP";
        });
      }, signal, { reportInput });
      if (!critical) this.state.journalDegraded = false;
    } catch (error) {
      if (isJournalAppendAbortedError(error)) throw error;
      if (shouldLatchReportSourceIntegrity(event, this.experiment)) {
        const durableFault = await this.noteReportSourceIntegrityFault(event, error);
        if (!durableFault) {
          this.failClosedIntegrity(error);
          this.syncRuntimeState();
          throw error;
        }
      }
      if (critical || error instanceof JournalEventConflictError || (reportInput && error instanceof PersistenceOutcomeUnknownError)) {
        this.overload.markIntegrityFault();
        this.state.overloadState = "FAIL_CLOSED";
        if (!(error instanceof JournalAdmissionRejected)) this.state.database = "DOWN";
        this.updateReady();
        this.syncRuntimeState();
        this.logger.error(
          { err: error, traceId: event.traceId, admitted: !(error instanceof JournalAdmissionRejected) },
          error instanceof JournalAdmissionRejected ? "critical journal admission rejected" : "critical journal append failed",
        );
        throw error;
      }
      this.notePersistenceOutcome(event.type, false);
      this.updateReady();
      this.logger.error(
        { err: error, traceId: event.traceId, admitted: !(error instanceof JournalAdmissionRejected) },
        error instanceof JournalAdmissionRejected ? "journal admission rejected" : "journal append failed",
      );
    }
  }

  private async checkpoint(): Promise<void> {
    if (!this.experiment || this.state.database !== "UP") return;
    await this.durableGate.run(() => this.checkpointInsideGate());
  }

  private async checkpointInsideGate(): Promise<void> {
    if (!this.experiment || this.state.database !== "UP") return;
    const latest = await this.journal.latestDurableSequence();
    if (this.lastAppliedDurableCursor !== latest) {
      this.metrics.checkpointWrites.inc({ status: "fence" });
      throw new CheckpointFenceError(this.lastAppliedDurableCursor, latest);
    }
    try {
      await this.journal.saveCheckpoint({
        experimentId: this.experiment.id, sequence: ++this.checkpointSequence,
        payload: {
          schemaVersion: CHECKPOINT_SCHEMA_VERSION,
          durableCursor: this.lastAppliedDurableCursor,
          ledger: this.ledger.snapshot(), positions: [...this.ledger.positions.values()], experiment: this.experiment,
          resultAccounting: structuredClone(this.state.resultAccounting),
          restingOrders: [...this.restingOrders.values()], activeTriggers: [...this.activeTriggers.values()],
          grokSignals: [...this.signalsByTrigger.values()].flatMap((signals) => [...signals.values()]),
          activationLatency: this.activation.snapshot(),
          makerReservations: Object.fromEntries(this.makerReservations),
          riskReductionOwners: [...this.riskReductionOwners.values()],
          riskReductionFailures: [...this.riskReductionFailures.values()],
          nakedDirectionalDiagnostics: [...this.nakedDirectionalDiagnostics.values()],
          profitTrailingPairStates: [...this.profitTrailingPairStates.values()],
          reversalProtectionStates: [...this.reversalProtectionStates.values()],
          managedMarkets: [...this.obligationMarketIds()],
          settledMarkets: [...this.settledMarkets],
          settlementLookups: [...this.economicSettlementLookups.values()],
          marketMetadata: [...this.marketsById.values()].map((market) => ({ ...market, raw: {} })),
          lastInfrastructureAccrualAt: this.lastInfrastructureAccrualAt,
          derivedAuditCursor: this.derivedAuditCursor,
          reportQuarantine: this.reportQuarantine,
          reportSourceIntegrity: boundIntegrityFaults(
            [...this.reportSourceIntegrity.values()].filter((fault) => fault.experimentId === this.experiment!.id),
            this.experiment.id,
          ),
        },
      });
      this.metrics.checkpointWrites.inc({ status: "success" });
      this.metrics.checkpointDurableCursor.set(this.lastAppliedDurableCursor);
    } catch (error) {
      if (error instanceof CheckpointFenceError) {
        this.metrics.checkpointWrites.inc({ status: "fence" });
        throw error;
      }
      this.metrics.checkpointWrites.inc({ status: "failed" });
      throw error;
    }
  }

  private async accrueInfrastructureCost(now = Date.now()): Promise<void> {
    const elapsedHours = Math.max(0, now - this.lastInfrastructureAccrualAt) / 3_600_000;
    const amount = elapsedHours * this.env.ESTIMATED_INFRA_COST_PER_HOUR_USD;
    if (elapsedHours <= 0) return;
    if (amount <= 0) {
      this.lastInfrastructureAccrualAt = now;
      return;
    }
    const occurredAt = new Date(now);
    const accruedAt = occurredAt.toISOString();
    const aggregateId = this.experiment?.id ?? "paper";
    const previousAccrualKey = this.lastInfrastructureAccrualAt;
    const auth = {
      aiCost: "0",
      infrastructureCost: String(amount),
      accruedAt,
      accrualKey: now,
    };
    await this.commitAuthoritative({
      operationKey: `infra:${aggregateId}:${previousAccrualKey}`,
      type: "INFRASTRUCTURE_COST_ACCRUED",
      aggregateType: "ledger",
      aggregateId,
      occurredAt,
      buildPayload: (at) => ({
        ...auth,
        derivedAudit: [{
          id: derivedAuditEventId("INFRASTRUCTURE_COST_ACCRUED", `${aggregateId}:${auth.accrualKey}`),
          type: "INFRASTRUCTURE_COST_ACCRUED",
          aggregateType: "ledger",
          aggregateId,
          traceId: aggregateId,
          payload: { ...auth, experimentId: this.experiment?.id ?? null },
        }, ...this.projectDerivedAudit({ operationKey: `infra:${aggregateId}:${previousAccrualKey}`,
          type: "INFRASTRUCTURE_COST_ACCRUED", aggregateType: "ledger", aggregateId, payload: auth }, at,
          (projected) => [{ id: derivedAuditEventId("PNL_SNAPSHOT", `infra:${aggregateId}:${auth.accrualKey}`),
            type: "PNL_SNAPSHOT", aggregateType: "ledger", aggregateId, traceId: aggregateId,
            payload: { ...projected.ledger.snapshot(), experimentId: this.experiment?.id ?? null,
              source: "OPERATING_COST", economicResults: projected.resultAccounting ? economicResults(projected.resultAccounting) : null,
              tradingResults: projected.resultAccounting ? tradingResults(projected.resultAccounting) : null } }])],
      }),
    });
  }

  private async maintainExperiment(): Promise<void> {
    for (const marketId of this.nakedDirectionalDiagnostics.keys()) this.observeSoakPosition(marketId);
    await this.flushSoakDiagnostics();
    if (!this.experiment) return;
    if (this.experiment.status === "RUNNING" || this.experiment.status === "ENTRY_CLOSED") {
      await this.accrueInfrastructureCost(this.nowFn().getTime());
    }
    this.pruneRuntimeBookkeeping();
    await this.pollFailedHedges();
    await this.maybeScheduledReview();
    await this.pollSettlements();
    const previous = this.experiment.status;
    const open = openPositionsRemain(this.ledger.positions.values());
    const unresolved = [...this.ledger.positions.values()].filter((position) => Number(position.upQuantity) > 0 || Number(position.downQuantity) > 0).length;
    let next = refreshExperiment(this.soakLifecycleManifest(this.experiment), this.nowFn(), { openPositions: open, unresolvedCount: unresolved });
    if (next.status === "ENTRY_CLOSED" && (next.closePolicy ?? "observe_until_settlement") === "mark_to_market") {
      const marked = await this.markToMarketClose(unresolved);
      next = refreshExperiment(next, this.nowFn(), {
        openPositions: open, unresolvedCount: unresolved, markToMarketReady: marked,
      });
    }
    if (next.status === "ENTRY_CLOSED") {
      const policy = next.closePolicy ?? "observe_until_settlement";
      next = refreshExperiment(next, this.nowFn(), {
        openPositions: openPositionsRemain(this.ledger.positions.values()),
        unresolvedCount: [...this.ledger.positions.values()].filter((position) => Number(position.upQuantity) > 0 || Number(position.downQuantity) > 0).length,
        markToMarketReady: policy === "mark_to_market" ? false : undefined,
      });
    }
    if (previous === "RUNNING" && (next.status === "ENTRY_CLOSED" || next.status === "COMPLETED")) {
      const closed: ExperimentManifest = {
        ...next, status: "ENTRY_CLOSED", completedAt: null, markToMarketAt: null,
      };
      await this.experimentBoundaryGate.run(async () => {
        if (this.experiment?.status === "RUNNING") await this.commitExperimentStatus(closed);
      });
    }
    if (next.status === "COMPLETED" && this.experiment?.status !== "COMPLETED") {
      await this.experimentBoundaryGate.run(async () => {
        if (this.experiment?.status !== "COMPLETED") await this.commitExperimentStatus(next);
      });
    } else if (next.status === "ENTRY_CLOSED" && this.experiment?.status !== "ENTRY_CLOSED" && this.experiment?.status !== "COMPLETED") {
      await this.experimentBoundaryGate.run(async () => {
        if (this.experiment?.status !== "ENTRY_CLOSED" && this.experiment?.status !== "COMPLETED") await this.commitExperimentStatus(next);
      });
    }
    this.syncExperimentWindow();
    const stillOpen = openPositionsRemain(this.ledger.positions.values());
    if (shouldWriteFinalReport(this.experiment, stillOpen) && !this.reportGenerated) {
      await this.writeFinalReport();
    }
  }

  private nakedDirectionalSoakEnabled(): boolean {
    // Runtime configuration only opts in the NEXT experiment at beginExperiment.
    // Existing authority (including legacy manifests without the flag) is durable.
    return this.experiment?.riskConfiguration.nakedDirectionalSoakEnabled === true;
  }

  private soakLifecycleManifest(manifest: ExperimentManifest): ExperimentManifest {
    return manifest.riskConfiguration.nakedDirectionalSoakEnabled === true
      || manifest.riskConfiguration.profitTrailingPairSoakEnabled === true
      ? { ...manifest, closePolicy: "observe_until_settlement" } : manifest;
  }

  private soakBlocksPostEntry(marketId: string): boolean {
    return this.nakedDirectionalSoakEnabled() || this.nakedDirectionalDiagnostics.has(marketId);
  }

  private profitTrailingPairSoakEnabled(): boolean {
    return this.experiment?.riskConfiguration.profitTrailingPairSoakEnabled === true;
  }

  private reversalProtectionEnabled(): boolean {
    const risk = this.experiment?.riskConfiguration;
    return this.env.TRADING_MODE === "paper" && this.profitTrailingPairSoakEnabled()
      && risk?.reversalProtectionEnabled === true && risk.reversalProtectionWindowMs === 180000
      && lockedGuardianGeneration(risk) !== null;
  }

  private reversalEpisode(position: Position | undefined): ReversalEpisode | null {
    return position?.unpairedOutcome && d(position.unpairedQuantity).gt(0)
      ? { marketId: position.marketId, positionId: position.id,
        unhedgedStartedAt: unhedgedEpisode(position), heldOutcome: position.unpairedOutcome } : null;
  }

  /** Fresh, synchronized, current-token books; same UP-positive pressure definition as Native V1. */
  private guardianObservation(marketId: string, heldOutcome: Outcome, nowMs: number) {
    if (!this.isMarketSynchronized(marketId)) return null;
    const pair = this.snapshots.get(marketId);
    const up = this.freshEarlyCatastrophicSnapshot(pair?.UP ?? null, nowMs);
    const down = this.freshEarlyCatastrophicSnapshot(pair?.DOWN ?? null, nowMs);
    if (!up || !down || up.tokenId !== this.currentTokenIdFor(marketId, "UP")
      || down.tokenId !== this.currentTokenIdFor(marketId, "DOWN")) return null;
    if ([up, down].some(book => !Number.isFinite(Date.parse(book.sourceTimestamp))
      || Date.parse(book.sourceTimestamp) > nowMs || nowMs - Date.parse(book.sourceTimestamp) > this.riskLimits.staleFeedTimeoutMs)) return null;
    return projectGuardianBookSignal(up, down, heldOutcome);
  }

  private evaluateGuardianRuntime(marketId: string, event?: JournalEvent): {
    previous: ReversalProtectionState | null;
    nextState: ReversalProtectionState;
    events: GuardianV2Event[];
  } | null {
    if (this.env.GUARDIAN_SEPARATE_ENABLED) return null;
    if (!this.reversalProtectionEnabled() || !experimentAllowsHedge(this.experiment) || this.state.draining
      || this.state.database !== "UP" || this.state.journalDegraded || this.overload.hasIntegrityFault) return null;
    const episode = this.reversalEpisode(this.ledger.positions.get(marketId));
    if (!episode || this.settledMarkets.has(marketId)) {
      this.guardianRuntimeStates.delete(marketId);
      this.guardianCheckpointQueuedAt.delete(marketId);
      return null;
    }
    const position = this.ledger.positions.get(marketId);
    if (!position || !sameReversalEpisode(this.reversalEpisode(position), episode)) return null;
    const now = event ? new Date(event.occurredAt) : this.nowFn();
    if (now.getTime() > this.nowFn().getTime()) return null;

    let raw = this.guardianRuntimeStates.get(marketId) ?? null;
    if (raw && !sameReversalEpisode(raw, episode)) {
      this.guardianRuntimeStates.delete(marketId);
      raw = null;
    }
    if (!raw) {
      const durable = this.reversalProtectionStates.get(marketId) ?? null;
      if (durable && !isGuardianV2State(durable)) throw new RecoveryFailClosedError("invariant", "active legacy Guardian state");
      if (durable && sameReversalEpisode(durable, episode)) {
        raw = structuredClone(durable);
        this.guardianRuntimeStates.set(marketId, raw);
      }
    }
    if (raw && !isGuardianV2State(raw)) throw new RecoveryFailClosedError("invariant", "active legacy Guardian runtime state");

    const observation = this.guardianObservation(marketId, episode.heldOutcome, now.getTime());
    // A restart first rebuilds the pair from its two subscriptions. Missing books
    // during that bootstrap are not evidence of a price excursion. EARLY's
    // strict deadline still advances independently of book availability.
    const pair = this.snapshots.get(marketId);
    if ((!pair?.UP || !pair.DOWN) && !observation
      && !(raw?.phase === "EARLY_RECOVERY_WATCH" && now.getTime() >= Date.parse(raw.deadlineAt!))) return null;
    const direction = event?.payload.consensusDirection;
    const consensus = event && (direction === "UP" || direction === "DOWN" || direction === "FLAT") ? {
      eventId: event.id,
      direction: direction as Direction,
      confidence: Number(event.payload.consensusConfidence),
      estimatedEdge: Number(event.payload.estimatedEdge),
      calculatedAt: typeof event.payload.calculatedAt === "string" ? event.payload.calculatedAt : event.occurredAt,
      ...(typeof event.payload.expiresAt === "string" ? { expiresAt: event.payload.expiresAt } : {}),
    } : undefined;
    const evaluation = evaluateGuardianV2({
      generation: lockedGuardianGeneration(this.experiment!.riskConfiguration)!,
      state: raw ?? null,
      episode,
      entryPrice: episode.heldOutcome === "UP" ? position.averageUpCost : position.averageDownCost,
      now,
      observation,
      observationGap: !!raw?.lastObservationAt
        && now.getTime() - Date.parse(raw.lastObservationAt) > this.riskLimits.staleFeedTimeoutMs,
      consensus: currentGuardianConsensus(consensus, this.nowFn().getTime(), this.riskLimits.staleFeedTimeoutMs),
    });
    if (!evaluation.stateChanged || !evaluation.nextState) return null;
    this.guardianRuntimeStates.set(marketId, evaluation.nextState);
    return { previous: raw, nextState: evaluation.nextState, events: evaluation.events };
  }

  private guardianPersistenceKind(
    marketId: string,
    previous: ReversalProtectionState | null,
    next: ReversalProtectionState,
    events: GuardianV2Event[],
    sourceEvent?: JournalEvent,
  ): "critical" | "checkpoint" | null {
    if (sourceEvent || events.length > 0) return "critical";
    if (previous && isGuardianV2State(previous) && isGuardianV2State(next)) {
      const semanticStateChanged = previous.phase !== next.phase
        || previous.sensor !== next.sensor
        || previous.armedAt !== next.armedAt
        || previous.deadlineAt !== next.deadlineAt
        || previous.confirmedAt !== next.confirmedAt
        || previous.protectionReason !== next.protectionReason;
      const observationGapReset = previous.phase === "OBSERVING" && next.phase === "OBSERVING"
        && ((previous.badRegionSince !== null && next.badRegionSince === null)
          || (previous.history.length > 0 && next.history.length === 0));
      if (semanticStateChanged || observationGapReset) return "critical";
    }
    const nowMs = Date.parse(next.updatedAt);
    const durable = this.reversalProtectionStates.get(marketId);
    const lastQueuedAt = this.guardianCheckpointQueuedAt.get(marketId)
      ?? (durable && isGuardianV2State(durable) ? Date.parse(durable.updatedAt) : Number.NEGATIVE_INFINITY);
    return nowMs - lastQueuedAt >= GUARDIAN_OBSERVATION_CHECKPOINT_MS ? "checkpoint" : null;
  }

  private enqueueGuardianPersistence(
    state: ReversalProtectionState, events: GuardianV2Event[], kind: "critical" | "checkpoint",
    sourceEvent?: JournalEvent, shutdownFlush = false,
  ): Promise<GuardianPersistenceOutcome> {
    if (this.state.draining && !shutdownFlush) return Promise.reject(new Error("GUARDIAN_ADMISSION_CLOSED"));
    const marketId = state.marketId, experimentId = this.experiment!.id;
    const generation = isGuardianV2State(state) ? state.formatGeneration ?? 2 : 1;
    const queue = this.guardianPersistenceQueues.get(marketId) ?? [];
    const last = queue.at(-1);
    // Initial durable revision is an authority boundary, never replaceable.
    const replace = kind === "checkpoint" && last?.kind === "checkpoint" && !last.sourceEvent && !sourceEvent
      && last.state.revision !== 1 && state.revision !== 1
      && last.experimentId === experimentId && last.generation === generation
      && sameReversalEpisode(last.state, state) && last.state.revision < state.revision;
    const bytes = Buffer.byteLength(JSON.stringify({ state, events, sourceEvent }));
    const pending = [...this.guardianPersistenceQueues.values()].reduce((n, q) => n + q.length, 0)
      + this.guardianPersistenceWorkers.size;
    if ((queue.length + (this.guardianPersistenceWorkers.has(marketId) ? 1 : 0) >= GUARDIAN_PERSISTENCE_BOUNDS.perMarket && !replace)
      || (pending >= GUARDIAN_PERSISTENCE_BOUNDS.total && !replace)
      || this.guardianPersistenceBytes - (replace ? last.bytes : 0) + bytes > GUARDIAN_PERSISTENCE_BOUNDS.bytes) {
      const error = new Error("GUARDIAN_CRITICAL_BACKPRESSURE");
      this.failClosedIntegrity(error);
      return Promise.resolve({ status: "FAILED", error, sourceEventId: sourceEvent?.id ?? null });
    }
    if (replace) {
      queue.pop();
      this.guardianPersistenceBytes -= last.bytes;
      last.resolve({ status: "SKIPPED_SUPERSEDED", sourceEventId: null });
    }
    let resolve!: (outcome: GuardianPersistenceOutcome) => void;
    const done = new Promise<GuardianPersistenceOutcome>(res => { resolve = res; });
    queue.push({ experimentId, generation, kind, state: structuredClone(state), events: [...events],
      ...(sourceEvent ? { sourceEvent: structuredClone(sourceEvent) } : {}), bytes, done, resolve });
    this.guardianPersistenceBytes += bytes;
    this.guardianPersistenceQueues.set(marketId, queue);
    this.guardianCheckpointQueuedAt.set(marketId, Date.parse(state.updatedAt));
    this.startGuardianPersistenceWorker(marketId);
    return done;
  }

  private startGuardianPersistenceWorker(marketId: string): void {
    if (this.guardianPersistenceWorkers.has(marketId)) return;
    const worker = this.durableGate.outside(async () => {
      while (true) {
        const queue = this.guardianPersistenceQueues.get(marketId);
        const request = queue?.shift();
        if (!request) break;
        try {
          request.resolve(await this.persistGuardianState(request));
        } catch (error) {
          request.resolve({ status: "FAILED", error, sourceEventId: request.sourceEvent?.id ?? null });
          for (const pending of queue ?? []) {
            this.guardianPersistenceBytes -= pending.bytes;
            pending.resolve({ status: "FAILED", error, sourceEventId: pending.sourceEvent?.id ?? null });
          }
          queue?.splice(0);
          // This worker owns authoritative durable mutations. A post-commit
          // crash/fault cannot be reduced to logging: the durable cursor may
          // already be ahead of in-memory state. Latch fail-closed so no later
          // economic mutation or clean checkpoint can cross that boundary.
          this.failClosedIntegrity(error);
          throw error;
        } finally {
          this.guardianPersistenceBytes -= request.bytes;
        }
      }
    });
    this.guardianPersistenceWorkers.set(marketId, worker);
    void worker.catch((error) => {
      this.logger.error({ err: error, marketId }, "guardian persistence worker failed");
    }).finally(() => {
      if (this.guardianPersistenceWorkers.get(marketId) === worker) this.guardianPersistenceWorkers.delete(marketId);
      const queue = this.guardianPersistenceQueues.get(marketId);
      if (queue?.length) this.startGuardianPersistenceWorker(marketId);
      else this.guardianPersistenceQueues.delete(marketId);
    });
  }

  private flushGuardianRuntimeForShutdown(): void {
    for (const runtime of this.guardianRuntimeStates.values()) {
      const durable = this.reversalProtectionStates.get(runtime.marketId);
      if (durable && isGuardianV2State(durable) && durable.revision >= runtime.revision) continue;
      void this.enqueueGuardianPersistence(runtime, [], "checkpoint", undefined, true).then(outcome => {
        if (outcome.status === "FAILED") throw outcome.error;
      }).catch((error) => {
        this.logger.error({ err: error, marketId: runtime.marketId }, "guardian shutdown flush failed");
      });
    }
  }

  private async awaitGuardianPersistence(marketId: string): Promise<void> {
    while (true) {
      const worker = this.guardianPersistenceWorkers.get(marketId);
      if (!worker) return;
      await worker;
    }
  }

  private guardianPersistenceUnresolved(): number {
    let queued = 0;
    for (const queue of this.guardianPersistenceQueues.values()) queued += queue.length;
    return queued
      + this.guardianPersistenceWorkers.size
      + this.reversalProtectionInFlight.size
      + this.reversalProtectionPending.size
      + this.reversalProtectionSnapshotScheduled.size;
  }

  private async joinGuardianPersistence(deadlineMs: number): Promise<JoinResult> {
    const started = Date.now();
    while (this.guardianPersistenceUnresolved() > 0) {
      if (Date.now() - started >= deadlineMs) {
        return { drained: false, unresolved: this.guardianPersistenceUnresolved(), timedOut: true };
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    return { drained: true, unresolved: 0, timedOut: false };
  }

  private async persistGuardianState(request: GuardianPersistenceRequest): Promise<GuardianPersistenceOutcome> {
    const next = request.state;
    const key = `${next.positionId}:${next.unhedgedStartedAt}:v2${request.generation === 3 ? "g3" : ""}:${next.revision}`;
    const auditKey = createHash("sha256").update(key).digest("hex");
    const payload = {
      ...next,
      experimentId: request.experimentId,
      sourceConsensusEventId: request.sourceEvent?.id ?? null,
    };
    const audit: DerivedAuditRecord[] = request.sourceEvent ? [{
      id: request.sourceEvent.id,
      type: request.sourceEvent.type,
      aggregateType: request.sourceEvent.aggregateType,
      aggregateId: request.sourceEvent.aggregateId,
      traceId: request.sourceEvent.traceId,
      payload: request.sourceEvent.payload,
      sourceEventId: request.sourceEvent.sourceEventId,
      correlationId: request.sourceEvent.correlationId,
      occurredAt: request.sourceEvent.occurredAt,
      recordedAt: request.sourceEvent.recordedAt,
    }] : [];
    audit.push({
      id: derivedAuditEventId("REVERSAL_PROTECTION_STATE_UPDATED", auditKey),
      type: "REVERSAL_PROTECTION_STATE_UPDATED",
      aggregateType: "position",
      aggregateId: next.positionId,
      traceId: next.positionId,
      payload,
    });
    for (const reason of request.events) {
      audit.push({
        id: derivedAuditEventId(`GUARDIAN_${reason}`, auditKey),
        type: `GUARDIAN_${reason}`,
        aggregateType: "position",
        aggregateId: next.positionId,
        traceId: next.positionId,
        payload,
      });
    }
    if (next.confirmedAt) {
      audit.push({
        id: derivedAuditEventId("REVERSAL_PROTECTION_CONFIRMED", auditKey),
        type: "REVERSAL_PROTECTION_CONFIRMED",
        aggregateType: "position",
        aggregateId: next.positionId,
        traceId: next.positionId,
        payload,
      });
    }

    try {
      const receipt = await this.commitAuthoritative({
        operationKey: `reversal:${key}`,
        type: "REVERSAL_PROTECTION_STATE_UPDATED",
        aggregateType: "position",
        aggregateId: next.positionId,
        occurredAt: new Date(next.updatedAt),
        signal: this.guardianShutdownAbort.signal,
        beforeCommit: () => {
          const position = this.ledger.positions.get(next.marketId);
          if (this.experiment?.id !== request.experimentId || !position || this.settledMarkets.has(next.marketId)
            || !sameReversalEpisode(this.reversalEpisode(position), next)) {
            throw new GuardianPersistenceGuardRejectedError(next.marketId, "EPISODE_CHANGED");
          }
          const current = this.reversalProtectionStates.get(next.marketId);
          if (current && !isGuardianV2State(current)) {
            throw new RecoveryFailClosedError("invariant", "active legacy Guardian state");
          }
          if (current && sameReversalEpisode(current, next) && current.revision >= next.revision) {
            throw new GuardianPersistenceGuardRejectedError(next.marketId, "SUPERSEDED");
          }
        },
        buildPayload: () => ({ state: next, derivedAudit: audit }),
      });
      if (request.sourceEvent) {
        const owned = parseDerivedAudit(receipt.transition.payload).find(r => r.id === request.sourceEvent!.id);
        if (!owned) return { status: "SKIPPED_SUPERSEDED", sourceEventId: null };
        assertJournalEventIdentity(request.sourceEvent, {
          ...request.sourceEvent, ...owned, occurredAt: owned.occurredAt ?? receipt.transition.occurredAt,
          recordedAt: owned.recordedAt ?? receipt.transition.recordedAt,
        });
      }
    } catch (error) {
      if (error instanceof GuardianPersistenceGuardRejectedError) return {
        status: error.reason === "SUPERSEDED" ? "SKIPPED_SUPERSEDED" : "SKIPPED_EPISODE_CHANGED",
        sourceEventId: null,
      };
      throw error;
    }
    this.scheduleGuardianDeadline();
    return { status: "PERSISTED", sourceEventId: request.sourceEvent?.id ?? null };
  }

  private async persistGuardianObservation(marketId: string, event?: JournalEvent): Promise<boolean> {
    const evaluation = this.evaluateGuardianRuntime(marketId, event);
    if (!evaluation) return false;
    const kind = this.guardianPersistenceKind(
      marketId,
      evaluation.previous,
      evaluation.nextState,
      evaluation.events,
      event,
    );
    if (!kind) return false;
    const outcome = await this.enqueueGuardianPersistence(evaluation.nextState, evaluation.events, kind, event);
    if (outcome.status === "FAILED") throw outcome.error;
    return outcome.status === "PERSISTED";
  }

  private observeGuardianSnapshot(marketId: string): void {
    const evaluation = this.evaluateGuardianRuntime(marketId);
    if (!evaluation) return;
    const kind = this.guardianPersistenceKind(
      marketId,
      evaluation.previous,
      evaluation.nextState,
      evaluation.events,
    );
    if (!kind) return;
    void this.enqueueGuardianPersistence(evaluation.nextState, evaluation.events, kind).then(outcome => {
      if (outcome.status === "FAILED") throw outcome.error;
    }).catch((error) => {
      this.logger.error({ err: error, marketId }, "guardian snapshot persistence failed");
    });
  }

  private async persistReversalConsensus(event: JournalEvent): Promise<boolean> {
    if (event.payload.authority !== NATIVE_DETERMINISTIC_AUTHORITY || typeof event.payload.marketId !== "string") return false;
    if (this.env.GUARDIAN_SEPARATE_ENABLED) {
      const direction = event.payload.consensusDirection;
      if (direction === "UP" || direction === "DOWN" || direction === "FLAT") {
        this.guardianConsensus.set(event.payload.marketId, { eventId: event.id, direction,
          confidence: Number(event.payload.consensusConfidence), estimatedEdge: Number(event.payload.estimatedEdge),
          calculatedAt: typeof event.payload.calculatedAt === "string" ? event.payload.calculatedAt : event.occurredAt,
          ...(typeof event.payload.expiresAt === "string" ? { expiresAt: event.payload.expiresAt } : {}) });
      }
      return false;
    }
    const handled = await this.persistGuardianObservation(event.payload.marketId, event);
    if (handled) await this.durableGate.outside(() => this.maybeReversalProtection(event.payload.marketId as string));
    return handled;
  }

  private reversalEligibility(reversal: ReversalProtectionState, position: Position | undefined, nowMs: number): {
    eligible: boolean; effectiveDeadlineMs: number | null; marketEndMs: number | null;
  } {
    const deadline = this.protectiveDeRiskEligibility(reversal.marketId, reversal.unhedgedStartedAt, nowMs);
    const current = this.reversalProtectionStates.get(reversal.marketId);
    return { ...deadline, eligible: this.reversalProtectionEnabled() && isGuardianV2State(current) && !!current.confirmedAt
      && sameReversalEpisode(current, reversal) && sameReversalEpisode(this.reversalEpisode(position), reversal)
      && !this.settledMarkets.has(reversal.marketId) && (deadline.marketEndMs === null || nowMs < deadline.marketEndMs) };
  }

  private reversalOrderEligible(audit: Record<string, unknown> | ProtectiveDeRiskAudit, position: Position | undefined, nowMs: number): boolean {
    const current = position && this.reversalProtectionStates.get(position.marketId);
    return !!current && current.positionId === audit.positionId && current.heldOutcome === audit.heldOutcome
      && current.unhedgedStartedAt === audit.unhedgedStartedAt && this.reversalEligibility(current, position, nowMs).eligible;
  }

  /** Persisted Guardian confirmation reserves risk priority for this episode.
   * Unconfirmed observations never preempt the normal hedge controller. */
  private confirmedGuardianProtection(marketId: string): GuardianV2State | null {
    const state = this.reversalProtectionStates.get(marketId);
    return state && isGuardianV2State(state) && state.confirmedAt
      && this.reversalEligibility(state, this.ledger.positions.get(marketId), this.nowFn().getTime()).eligible ? state : null;
  }

  /**
   * Coalesce all normalized snapshots from the current transport/event-loop turn
   * before risk-reduction arbitration. This replaces the accidental pair
   * coalescing that used to come from awaiting Guardian DB writes in the socket
   * hot path.
   */
  private scheduleSnapshotReversalProtection(marketId: string): void {
    if (this.experiment?.status === "COMPLETED") return;
    if (this.reversalProtectionSnapshotScheduled.has(marketId)) return;
    this.reversalProtectionSnapshotScheduled.add(marketId);
    const immediate = setImmediate(() => {
      this.reversalProtectionSnapshotScheduled.delete(marketId);
      if (!this.state.draining) this.kickReversalProtection(marketId);
    });
    immediate.unref();
  }

  private kickReversalProtection(marketId: string): void {
    if (!this.reversalProtectionEnabled() || !this.reversalEpisode(this.ledger.positions.get(marketId))) return;
    if (this.reversalProtectionInFlight.has(marketId)) { this.reversalProtectionPending.add(marketId); return; }
    this.reversalProtectionInFlight.add(marketId);
    void this.maybeReversalProtection(marketId)
      .catch((error) => this.logger.error({ err: error, marketId }, "reversal protection attempt failed"))
      .finally(() => {
        this.reversalProtectionInFlight.delete(marketId);
        if (this.reversalProtectionPending.delete(marketId)) this.kickReversalProtection(marketId);
      });
  }

  /** One wakeup reconstructed from journal state; snapshots and maintenance use
   * the same classifier. A timer is a scheduling hint, never economic authority. */
  private scheduleGuardianDeadline(): void {
    if (this.guardianDeadlineTimer) clearTimeout(this.guardianDeadlineTimer);
    this.guardianDeadlineTimer = null;
    if (this.env.GUARDIAN_SEPARATE_ENABLED) return;
    if (this.state.draining || !this.reversalProtectionEnabled()) return;
    const watches = [...this.reversalProtectionStates.values()].filter(state => isGuardianV2State(state)
      && !state.confirmedAt && state.deadlineAt && this.reversalEpisode(this.ledger.positions.get(state.marketId)));
    if (!watches.length) return;
    const now = this.nowFn().getTime();
    const next = Math.min(...watches.map(state => Date.parse(isGuardianV2State(state) ? state.deadlineAt! : "")));
    // An overdue LATE watch may be waiting for subscription bootstrap; avoid a busy loop.
    this.guardianDeadlineTimer = setTimeout(() => {
      this.guardianDeadlineTimer = null;
      const currentNow = this.nowFn().getTime();
      for (const state of this.reversalProtectionStates.values()) {
        if (isGuardianV2State(state) && !state.confirmedAt && state.deadlineAt
          && currentNow >= Date.parse(state.deadlineAt)) this.kickReversalProtection(state.marketId);
      }
      this.scheduleGuardianDeadline();
    }, next > now ? next - now : 1000);
    this.guardianDeadlineTimer.unref();
  }

  private async maybeReversalProtection(marketId: string): Promise<void> {
    await this.persistGuardianObservation(marketId);
    // Economic execution is allowed to wait; the WebSocket ingress path is not.
    // This guarantees arm/release/protect state is durable before arbitration.
    await this.awaitGuardianPersistence(marketId);
    const reversal = this.reversalProtectionStates.get(marketId);
    if (!reversal?.confirmedAt || !this.reversalEligibility(reversal, this.ledger.positions.get(marketId), this.nowFn().getTime()).eligible
      || !experimentAllowsHedge(this.experiment) || this.state.draining || this.state.database !== "UP"
      || this.state.journalDegraded || this.overload.hasIntegrityFault || this.overload.state === "FAIL_CLOSED") return;
    const owner = this.riskReductionOwners.get(marketId);
    if (owner) {
      if (owner.intent !== "GUARDIAN_PROTECTIVE_SELL" && !this.riskReductionInFlight.has(marketId)) {
        // A recovered/admitted but non-running PAPER taker has not executed.
        // Revoke its durable authority before requiring its opposite book;
        // a missing hedge book cannot trap a valid held-token emergency SELL.
        await this.durableGate.run(async () => {
          if (this.confirmedGuardianProtection(marketId)
            && this.riskReductionOwners.get(marketId)?.order.id === owner.order.id
            && !this.riskReductionInFlight.has(marketId)) await this.failRiskReduction(owner, "GUARDIAN_PRIORITY");
        });
      } else await this.resumeRiskReduction(owner);
      if (this.riskReductionOwners.has(marketId)) return;
    }
    if (this.riskReductionBusy(marketId)) {
      this.record("GUARDIAN_BLOCKED", marketId, marketId, { reason: "EXISTING_EXECUTION_MUST_TERMINATE", confirmedAt: reversal.confirmedAt });
      return;
    }
    const heldBook = this.snapshots.get(marketId)?.[reversal.heldOutcome];
    const retryFacts = JSON.stringify({ eventId: heldBook?.eventId, sequence: heldBook?.sequence,
      tokenId: heldBook?.tokenId, sourceTimestamp: heldBook?.sourceTimestamp,
      synchronized: heldBook ? this.adapterTokenSynchronized(heldBook.tokenId) : false,
      database: this.state.database, journalDegraded: this.state.journalDegraded });
    if (this.protectiveDeRiskVetoFingerprint.get(marketId) === retryFacts
      && this.nowFn().getTime() < (this.protectiveDeRiskNextAttemptAt.get(marketId) ?? 0)) return;
    // Guardian owns the emergency decision. Reuse the full-residual SELL
    // executor and its durable fences; normal A/B profitability is irrelevant.
    for (let attempt = 0; attempt < 3; attempt++) {
      if (await this.maybeGuardianProtectiveSell(marketId)) return;
      if (!this.confirmedGuardianProtection(marketId) || this.riskReductionBusy(marketId)
        || this.state.database !== "UP" || this.state.journalDegraded || this.overload.hasIntegrityFault) return;
    }
    this.record("GUARDIAN_EXECUTION_BLOCKED", marketId, marketId, { reason: "BOUNDED_ATTEMPTS_EXHAUSTED", confirmedAt: reversal.confirmedAt, pending: true });
    this.protectiveDeRiskNextAttemptAt.set(marketId, this.nowFn().getTime() + 1000);
    this.protectiveDeRiskVetoFingerprint.set(marketId, retryFacts);
    const blocked = this.evaluateGuardian(marketId);
    await this.persistReportInput(createJournalEvent({ traceId: reversal.positionId, type: "GUARDIAN_EVALUATION",
      aggregateType: "position", aggregateId: reversal.positionId, payload: { marketId, eligible: false,
        reason: blocked.reason, independentPriority: true, executionStatus: "BLOCKED_PENDING_RETRY",
        attempts: 3, confirmedAt: reversal.confirmedAt, residualQuantity: this.ledger.positions.get(marketId)?.unpairedQuantity ?? "0" } }));
  }

  /** Current PG-recovered economic projection and Engine's existing public feed.
   * Guardian never needs a second feed, DB writer, checkpoint issuer or wallet. */
  guardianView(): GuardianView {
    const now = this.nowFn();
    const markets: GuardianMarket[] = [];
    const open = new Set<string>();
    for (const position of this.ledger.positions.values()) {
      if (!this.reversalEpisode(position) || this.settledMarkets.has(position.marketId)) continue;
      open.add(position.marketId);
      const pair = this.snapshots.get(position.marketId);
      const trim = (book: MarketSnapshot | undefined, outcome: Outcome): MarketSnapshot | null =>
        book && book.tokenId === this.currentTokenIdFor(position.marketId, outcome)
          ? { ...book, bids: book.bids.slice(0, 1), asks: book.asks.slice(0, 1) } : null;
      const stored = this.reversalProtectionStates.get(position.marketId);
      const consensus = this.guardianConsensus.get(position.marketId);
      const admission = [...this.guardianAdmissions.values()].filter(a => a.update.state.marketId === position.marketId
        && sameReversalEpisode(this.reversalEpisode(position), a.update.state)).at(-1)?.update.state;
      markets.push({ position: structuredClone(position),
        state: stored && isGuardianV2State(stored) ? structuredClone(stored) : null,
        up: trim(pair?.UP, "UP"), down: trim(pair?.DOWN, "DOWN"),
        synchronized: this.isMarketSynchronized(position.marketId), ...(consensus ? { consensus } : {}),
        ...(admission ? { admission: structuredClone(admission) } : {}) });
    }
    for (const key of this.guardianConsensus.keys()) if (!open.has(key)) this.guardianConsensus.delete(key);
    return { mode: "paper", enginePid: process.pid, engineStartedAt: this.state.startedAt.toISOString(),
      experimentId: this.experiment?.id ?? null, now: now.toISOString(),
      enabled: this.env.GUARDIAN_SEPARATE_ENABLED && this.reversalProtectionEnabled()
        && experimentAllowsHedge(this.experiment) && this.state.database === "UP" && this.state.recoveryComplete
        && !this.state.draining && !this.state.journalDegraded && !this.overload.hasIntegrityFault,
      generation: this.experiment ? lockedGuardianGeneration(this.experiment.riskConfiguration) : null,
      staleMs: this.riskLimits.staleFeedTimeoutMs, feed: { ...this.state.connections.polymarket }, markets };
  }

  /** Persist through the existing revision/episode/idempotency boundary, then
   * use the existing PR3/hedge/SELL arbitration. Only Engine writes orders. */
  async acceptGuardianUpdate(input: unknown, waitForCheckpoint = true): Promise<{ status: string; reason?: string }> {
    const raw = input as { experimentId?: unknown; state?: unknown; events?: unknown } | null;
    const parsed = guardianV2StateSchema.safeParse(raw?.state);
    if (!parsed.success || !Array.isArray(raw?.events)) return { status: "REJECTED", reason: "INVALID_STATE" };
    const next = parsed.data;
    const validEvents: GuardianV2Event[] = ["EARLY_DETERIORATION_ARMED", "EARLY_TRUE_RECOVERY", "EARLY_EMERGENCY_PROTECT",
      "EARLY_TIMEOUT_PROTECT", "LATE_SEVERITY_ARMED", "LATE_CONFIRMATION_RELEASED", "LATE_CONFIRMATION_ACCEPTED", "LATE_EMERGENCY_PROTECT"];
    if (raw.events.some(event => !validEvents.includes(event))) return { status: "REJECTED", reason: "INVALID_EVENTS" };
    const current = this.reversalProtectionStates.get(next.marketId);
    if (!this.env.GUARDIAN_SEPARATE_ENABLED || raw.experimentId !== this.experiment?.id
      || !this.reversalProtectionEnabled() || !experimentAllowsHedge(this.experiment)
      || this.state.draining || this.state.database !== "UP" || this.state.journalDegraded || this.overload.hasIntegrityFault)
      return { status: "REJECTED", reason: "ENGINE_UNAVAILABLE" };
    if (!sameReversalEpisode(this.reversalEpisode(this.ledger.positions.get(next.marketId)), next))
      return { status: "REJECTED", reason: "EPISODE_CHANGED" };
    if (current && sameReversalEpisode(current, next) && current.revision >= next.revision) {
      if (current.revision === next.revision && JSON.stringify(current) !== JSON.stringify(next))
        return { status: "REJECTED", reason: "REVISION_CONFLICT" };
      if (current.confirmedAt) this.kickReversalProtection(next.marketId);
      return { status: "REPLAY" };
    }
    const admissionKey = `${next.positionId}:${next.unhedgedStartedAt}:${next.revision}`;
    const pending = this.guardianAdmissions.get(admissionKey);
    // A restarted observer may probe the same admitted state without its lost
    // semantic event list. It cannot replace the original queued events.
    if (pending) return JSON.stringify(pending.update.state) === JSON.stringify(next)
      && (!raw.events.length || JSON.stringify(pending.update.events) === JSON.stringify(raw.events))
      ? { status: "IN_FLIGHT" } : { status: "REJECTED", reason: "REVISION_CONFLICT" };
    const now = this.nowFn().getTime();
    if (Date.parse(next.updatedAt) > now || now - Date.parse(next.updatedAt) > this.riskLimits.staleFeedTimeoutMs)
      return { status: "REJECTED", reason: "STATE_STALE_OR_FUTURE" };
    if (!this.guardianObservation(next.marketId, next.heldOutcome, now))
      return { status: "REJECTED", reason: "BOOK_UNAVAILABLE" };
    const critical = next.revision === 1 || raw.events.length > 0 || !!next.confirmedAt
      || (!!current && isGuardianV2State(current) && (current.phase !== next.phase || (current.badRegionSince !== null && next.badRegionSince === null)));
    const persistence = this.enqueueGuardianPersistence(next, raw.events, critical ? "critical" : "checkpoint")
      .catch((error): GuardianPersistenceOutcome => {
        // Workers normally resolve FAILED. Also settle a rejected admission
        // without leaving an orphan/unchecked child promise. No rejected state
        // becomes authority, and an ambiguous durable failure stays fail closed.
        this.failClosedIntegrity(error);
        return { status: "FAILED", error, sourceEventId: null };
      });
    const admitted = { update: { experimentId: this.experiment!.id, state: next, events: raw.events as GuardianV2Event[] }, done: persistence };
    this.guardianAdmissions.set(admissionKey, admitted);
    void persistence.then(() => {
      if (this.guardianAdmissions.get(admissionKey) === admitted) this.guardianAdmissions.delete(admissionKey);
    });
    if (critical) this.logger.info?.({ stage: "STATE_LANE_QUEUED", marketId: next.marketId, positionId: next.positionId,
      unhedgedStartedAt: next.unhedgedStartedAt, revision: next.revision, updatedAt: next.updatedAt,
      lastObservationAt: next.lastObservationAt, localAt: performance.now() }, "guardian critical milestone");
    // Routine observation checkpoints already have a bounded, coalescing Engine
    // persistence lane. Do not stall independent monitoring behind its durable
    // gate. QUEUED is admission only; critical watches/decisions still await commit.
    if (!critical && !waitForCheckpoint) {
      void persistence.then(outcome => {
        if (outcome.status === "FAILED") this.logger.error({ err: outcome.error, marketId: next.marketId }, "guardian checkpoint failed");
      });
      return { status: "QUEUED" };
    }
    const outcome = await persistence;
    if (outcome.status === "FAILED") throw outcome.error;
    if (next.confirmedAt && outcome.status === "PERSISTED") this.kickReversalProtection(next.marketId);
    return { status: outcome.status === "PERSISTED" ? "PERSISTED" : "REPLAY" };
  }

  private profitTrailingPairConfig(): { target: number; arm: number; retrace: number } | null {
    if (!this.profitTrailingPairSoakEnabled()) return null;
    const risk = this.experiment!.riskConfiguration;
    const target = Number(risk.profitTrailingPairTargetEdge ?? risk.normalHedgeMinLockedPairEdge);
    const arm = Number(risk.profitTrailingPairArmEdge);
    const retrace = Number(risk.profitTrailingPairRetraceEdge);
    if (!Number.isFinite(target) || !Number.isFinite(arm) || !Number.isFinite(retrace)
      || target <= 0 || arm < 0 || retrace <= 0 || arm >= target || retrace >= target) return null;
    return { target, arm, retrace };
  }

  private profitTrailingPairOpportunity(marketId: string, position: Position, now: Date): {
    pair: Record<Outcome, MarketSnapshot>;
    opposite: MarketSnapshot;
    grossEdge: string;
    lockedPnl: string;
    worstPrice: string;
  } | null {
    if (!position.unpairedOutcome || d(position.unpairedQuantity).lte(0)
      || this.state.connections.polymarket.state !== "CONNECTED") return null;
    const raw = this.snapshots.get(marketId);
    const up = this.freshEarlyCatastrophicSnapshot(raw?.UP ?? null, now.getTime());
    const down = this.freshEarlyCatastrophicSnapshot(raw?.DOWN ?? null, now.getTime());
    if (!up || !down) return null;
    const opposite = position.unpairedOutcome === "UP" ? down : up;
    if (!opposite.feeSchedule || !opposite.minOrderSize) return null;
    const quantity = d(position.unpairedQuantity);
    if (quantity.lt(opposite.minOrderSize)) return null;
    const walk = walkBook(opposite.asks, quantity, "1", "BUY");
    if (!walk.sufficientDepth || !walk.worstPrice || !d(walk.filledQuantity).eq(quantity)) return null;
    const firstAverage = position.unpairedOutcome === "UP" ? position.averageUpCost : position.averageDownCost;
    const grossEdge = d(1).minus(firstAverage).minus(walk.vwap).toFixed();
    const fee = calculateFee(quantity, walk.vwap, "TAKER", opposite.feeSchedule);
    const lockedPnl = quantity.mul(grossEdge).minus(position.totalFees).minus(fee).toFixed();
    return { pair: { UP: up, DOWN: down }, opposite, grossEdge, lockedPnl, worstPrice: walk.worstPrice };
  }

  private kickProfitTrailingPair(marketId: string): void {
    if (!this.profitTrailingPairSoakEnabled()) return;
    if (!this.ledger.positions.get(marketId)?.unpairedOutcome) return;
    if (this.profitTrailingPairInFlight.has(marketId)) {
      this.profitTrailingPairPending.add(marketId);
      return;
    }
    this.profitTrailingPairInFlight.add(marketId);
    void this.maybeProfitTrailingPair(marketId)
      .catch((error) => this.logger.error({ err: error, marketId }, "profit trailing pair attempt failed"))
      .finally(() => {
        this.profitTrailingPairInFlight.delete(marketId);
        if (this.profitTrailingPairPending.delete(marketId)) {
          this.kickProfitTrailingPair(marketId);
        } else if (!this.profitTrailingRetainsAuthority(marketId)) {
          this.scheduleSnapshotProtectiveDeRisk(marketId);
        }
      });
  }

  private async maybeProfitTrailingPair(marketId: string): Promise<void> {
    // Existing Maker-Hedge still controls its fallback. A pre-posted dual sibling,
    // however, yields to an actionable Profit-Trailing target/retrace below.
    if([...this.restingOrders.values()].some(r=>r.order.marketId===marketId&&r.order.makerFirst
      &&!r.order.makerFirst.dualGroupId&&this.restingOrderIntent(r)==='HEDGE_SECOND_LEG'))return;
    const config = this.profitTrailingPairConfig();
    if (!config || this.soakBlocksPostEntry(marketId) || !experimentAllowsHedge(this.experiment)
      || this.state.database !== "UP" || this.state.journalDegraded || this.overload.hasIntegrityFault
      || this.overload.state === "FAIL_CLOSED" || this.settledMarkets.has(marketId) || this.riskReductionBusy(marketId)) return;

    for (let pass = 0; pass < 3; pass += 1) {
      const position = this.ledger.positions.get(marketId);
      if (!position?.unpairedOutcome || d(position.unpairedQuantity).lte(0)) return;
      const now = this.nowFn();
      if (!this.profitTrailingRetainsAuthority(marketId, position, now.getTime())) return;
      const opportunity = this.profitTrailingPairOpportunity(marketId, position, now);
      if (!opportunity) return;
      const evaluation = evaluateProfitTrailingPair({
        state: this.profitTrailingPairStates.get(marketId) ?? null,
        marketId,
        positionId: position.id,
        unhedgedStartedAt: unhedgedEpisode(position),
        now,
        currentGrossEdge: opportunity.grossEdge,
        currentLockedPnl: opportunity.lockedPnl,
        targetEdge: config.target,
        armEdge: config.arm,
        retraceEdge: config.retrace,
      });

      if (evaluation.stateChanged && evaluation.nextState) {
        const trailing = evaluation.nextState;
        await this.commitAuthoritative({
          operationKey: `profit-trailing:${trailing.positionId}:${trailing.unhedgedStartedAt}:${trailing.revision}`,
          type: "PROFIT_TRAILING_PAIR_STATE_UPDATED",
          aggregateType: "position",
          aggregateId: trailing.positionId,
          occurredAt: now,
          buildPayload: () => ({
            state: trailing,
            derivedAudit: [{
              id: derivedAuditEventId("PROFIT_TRAILING_PAIR_STATE_UPDATED", `${trailing.positionId}:${trailing.unhedgedStartedAt}:${trailing.revision}`),
              type: "PROFIT_TRAILING_PAIR_STATE_UPDATED",
              aggregateType: "position",
              aggregateId: trailing.positionId,
              traceId: trailing.positionId,
              payload: {
                marketId, revision: trailing.revision, highWaterEdge: trailing.highWaterEdge,
                armedAt: trailing.armedAt, targetEdge: config.target, armEdge: config.arm,
                retraceEdge: config.retrace, experimentId: this.experiment?.id ?? null,
              },
            }],
          }),
        });
        // Re-read both durable position state and the latest books after the DB
        // boundary. Never act on the snapshot that preceded persistence.
        continue;
      }

      if (evaluation.action === "NONE") return;
      if (this.riskReductionBusy(marketId)) return;

      const latestPosition = this.ledger.positions.get(marketId);
      if (!latestPosition?.unpairedOutcome || latestPosition.id !== position.id
        || unhedgedEpisode(latestPosition) !== unhedgedEpisode(position)
        || !d(latestPosition.unpairedQuantity).eq(position.unpairedQuantity)) continue;
      const latestNow = this.nowFn();
      const latest = this.profitTrailingPairOpportunity(marketId, latestPosition, latestNow);
      if (!latest) return;
      const latestEvaluation = evaluateProfitTrailingPair({
        state: this.profitTrailingPairStates.get(marketId) ?? null,
        marketId,
        positionId: latestPosition.id,
        unhedgedStartedAt: unhedgedEpisode(latestPosition),
        now: latestNow,
        currentGrossEdge: latest.grossEdge,
        currentLockedPnl: latest.lockedPnl,
        targetEdge: config.target,
        armEdge: config.arm,
        retraceEdge: config.retrace,
      });
      if (latestEvaluation.stateChanged) continue;
      if (latestEvaluation.action === "NONE") return;

      const baseCandidate = evaluateNormalHedgeCandidate({
        position: latestPosition,
        snapshots: latest.pair,
        maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd,
        minimumLockedPairEdge: 0,
      });
      if (!baseCandidate || !d(baseCandidate.quantity).eq(latestPosition.unpairedQuantity)) {
        this.noteProfitTrailingRejectedEvidence(marketId, latestPosition, latestNow.getTime());
        return;
      }
      // An actionable target/retrace outranks a speculative resting dual quote.
      const dualSibling=[...this.restingOrders.values()].filter(r=>r.order.marketId===marketId
        &&r.order.makerFirst?.dualGroupId&&this.restingOrderIntent(r)==='HEDGE_SECOND_LEG');
      for(const resting of dualSibling)
        await this.releaseMaker(resting.order.id,{cancel:true,reason:'PROFIT_TRAILING_PRIORITY'});
      const candidate: StrategyCandidate = {
        ...baseCandidate,
        strategyId: latestEvaluation.action === "TARGET" ? "profit-trailing-pair-target" : "profit-trailing-pair-trail",
        strategyVersion: "profit-trailing-pair-v1",
        limitPrice: latest.worstPrice,
        rawEdgeUsdPerShare: Number(latest.grossEdge),
        rawExpectedEdge: Number(latest.grossEdge),
        reasonCodes: [...baseCandidate.reasonCodes,
          latestEvaluation.action === "TARGET" ? "PROFIT_PAIR_TARGET" : "PROFIT_PAIR_RETRACE"],
      };
      const { decision, duplicateKey } = this.decideWithRuntimeBasis({
        candidate,
        consensus: HEDGE_CHECK_CONSENSUS,
        triggerId: latestEvaluation.action === "TARGET" ? "profit-pair-target" : "profit-pair-retrace",
        signalExpiresAt: new Date(latestNow.getTime() + 60_000),
        snapshot: latest.opposite,
        pair: latest.pair,
        now: latestNow,
        databaseAvailable: this.state.database === "UP",
        orderTypeOverride: "TAKER",
        allowMakerFirstHedge:!dualSibling.length&&!this.profitTrailingPairStates.get(marketId)?.makerAttemptedAt,
        profitTrailingLockedPnlUsd: latest.lockedPnl,
      });
      this.record("PROFIT_TRAILING_PAIR_EVALUATION", latestPosition.id, marketId, {
        action: latestEvaluation.action, accepted: decision.accepted, vetoes: decision.risk.vetoes,
        grossEdge: latest.grossEdge, lockedPnl: latest.lockedPnl,
        highWaterEdge: this.profitTrailingPairStates.get(marketId)?.highWaterEdge ?? null,
        targetEdge: config.target, armEdge: config.arm, retraceEdge: config.retrace,
      }, this.assetByMarket.get(marketId) ?? null);
      if (!decision.accepted || !decision.order) {
        this.noteProfitTrailingRejectedEvidence(marketId, latestPosition, latestNow.getTime());
        return;
      }
      if(decision.order.makerFirst){
        const current=this.profitTrailingPairStates.get(marketId);
        if(!current||current.makerAttemptedAt)return;
        const attempted={...current,makerAttemptedAt:latestNow.toISOString(),revision:current.revision+1,updatedAt:latestNow.toISOString()};
        await this.commitAuthoritative({operationKey:`profit-trailing:${attempted.positionId}:${attempted.unhedgedStartedAt}:${attempted.revision}`,
          type:'PROFIT_TRAILING_PAIR_STATE_UPDATED',aggregateType:'position',aggregateId:attempted.positionId,
          buildPayload:()=>({state:attempted,derivedAudit:[{id:derivedAuditEventId('PROFIT_TRAILING_PAIR_STATE_UPDATED',`${attempted.positionId}:${attempted.unhedgedStartedAt}:${attempted.revision}`),
            type:'PROFIT_TRAILING_PAIR_STATE_UPDATED',aggregateType:'position',aggregateId:attempted.positionId,traceId:attempted.positionId,
            payload:{...attempted,experimentId:this.experiment?.id??null}}]})});
      }
      const submitted = await this.submitPaperOrder(decision.order, latest.opposite, duplicateKey, "HEDGE_SECOND_LEG");
      if(decision.order.makerFirst&&!submitted)this.profitTrailingPairPending.add(marketId);
      if (!submitted) this.noteProfitTrailingRejectedEvidence(marketId, latestPosition, this.nowFn().getTime());
      else this.profitTrailingRejectedEvidence.delete(marketId);
      return;
    }
    this.profitTrailingPairPending.add(marketId);
  }

  private observeSoakPosition(marketId: string): void {
    const diagnostic = this.nakedDirectionalDiagnostics.get(marketId);
    const position = this.ledger.positions.get(marketId);
    if (!diagnostic || !position) return;
    const pair = this.snapshots.get(marketId);
    const held = pair?.[diagnostic.entryDirection] ?? null;
    const opposite = pair?.[diagnostic.entryDirection === "UP" ? "DOWN" : "UP"] ?? null;
    const previous = diagnostic.current;
    observeNakedDirectionalPosition(diagnostic, position, {
      held, opposite, heldSynchronized: !!held && this.adapterTokenSynchronized(held.tokenId),
      oppositeSynchronized: !!opposite && this.adapterTokenSynchronized(opposite.tokenId),
      now: this.nowFn(), staleTimeoutMs: this.riskLimits.staleFeedTimeoutMs,
      feedState: this.state.connections.polymarket.state,
    });
    for (const side of ["heldFreshness", "oppositeFreshness"] as const) {
      const freshness = diagnostic.current![side];
      if (freshness !== "FRESH" && previous?.[side] !== freshness) {
        noteNakedDirectionalAnomaly(diagnostic, this.nowFn(), `BOOK_${freshness}`, { side });
      }
    }
  }

  private noteSoakAnomaly(kind: string, detail: Record<string, unknown>, marketId?: string, asset?: string): void {
    for (const diagnostic of this.nakedDirectionalDiagnostics.values()) {
      if (marketId && diagnostic.marketId !== marketId) continue;
      if (asset && this.assetByMarket.get(diagnostic.marketId) !== asset) continue;
      const tokenIds = Array.isArray(detail.tokenIds) ? detail.tokenIds : typeof detail.tokenId === "string" ? [detail.tokenId] : null;
      if (tokenIds && !tokenIds.some((tokenId) => tokenId === this.currentTokenIdFor(diagnostic.marketId, "UP")
        || tokenId === this.currentTokenIdFor(diagnostic.marketId, "DOWN"))) continue;
      const bounded = { ...detail, ...(tokenIds ? { tokenIds: tokenIds.filter((id) =>
        id === this.currentTokenIdFor(diagnostic.marketId, "UP") || id === this.currentTokenIdFor(diagnostic.marketId, "DOWN")) } : {}) };
      noteNakedDirectionalAnomaly(diagnostic, this.nowFn(), kind, bounded);
    }
  }

  /** Low-frequency durable batch. No database operation runs in the book observer. */
  private async flushSoakDiagnostics(): Promise<void> {
    // Timer and maintenance share one bounded write. Journal admission owns the
    // entire flight, including waiting for the durable gate, so shutdown joins it.
    await this.soakPersistenceFlight.run(async () => {
      const nowMs = this.nowFn().getTime();
      if (this.state.draining || nowMs - this.lastSoakPersistAt < SOAK_PERSIST_INTERVAL_MS || this.nakedDirectionalDiagnostics.size === 0) return;
      if (this.state.database !== "UP" || this.state.journalDegraded || this.overload.hasIntegrityFault) return;
      if (![...this.nakedDirectionalDiagnostics.values()].some((item) => this.ledger.positions.has(item.marketId)
        && item.revision > (this.soakPersistedRevisions.get(item.marketId) ?? -1))) return;
      await this.journalAdmission.run("audit", true, async () => {
        await this.commitAuthoritative({
          operationKey: `soak:${createId("observation")}`, type: "NAKED_DIRECTIONAL_OBSERVATION",
          aggregateType: "experiment", aggregateId: this.experiment?.id ?? "paper",
          buildPayload: () => {
            const diagnostics = [...this.nakedDirectionalDiagnostics.values()]
              .filter((item) => this.ledger.positions.has(item.marketId)
                && item.revision > (this.soakPersistedRevisions.get(item.marketId) ?? -1))
              .map((item) => compactNakedDirectionalObservation(item, this.soakPersistedObservationCounts.get(item.marketId) ?? 0));
            return { diagnostics, derivedAudit: [] };
          },
          afterApply: async (transition) => {
            for (const diagnostic of transition.payload.diagnostics as NakedDirectionalDiagnostic[]) {
              this.soakPersistedRevisions.set(diagnostic.marketId, diagnostic.revision);
              this.soakPersistedObservationCounts.set(diagnostic.marketId, diagnostic.observationCount);
            }
            this.lastSoakPersistAt = nowMs;
          },
        });
      });
    });
  }

  private async maybeScheduledReview(): Promise<void> {
    const status = this.experiment?.status;
    if (status === "IDLE" || status === "COMPLETED" || status === "FAILED" || status === "ABORTED_START" || status == null) return;
    const triggers = this.triggerEngine.ingestScheduledReview(this.experiment?.id ?? createId("review"));
    for (const trigger of triggers) this.admitTrigger(trigger);
  }

  private async pollFailedHedges(): Promise<void> {
    for (const position of [...this.ledger.positions.values()]) {
      if (Number(position.unpairedQuantity) <= 0) continue;
      // Maintenance is a low-frequency fallback. Fresh synchronized snapshots
      // normally trigger the same keyed de-risk flight sooner. A genuine
      // durable fault here has already latched the fail-closed state; log it
      // like the snapshot-triggered flight instead of letting an unhandled
      // rejection escape the maintenance timer loop.
      try {
        await this.maybeReversalProtection(position.marketId);
        await this.maybeProtectiveDeRisk(position.marketId);
      } catch (error) {
        this.logger.error({ err: error, marketId: position.marketId }, "maintenance protective de-risk attempt failed");
      }
      const current = this.ledger.positions.get(position.marketId);
      if (!current || Number(current.unpairedQuantity) <= 0) continue;
      for (const trigger of this.triggerEngine.ingestPosition(current, current.id, this.assetByMarket.get(current.marketId) ?? null)) {
        this.admitTrigger(trigger);
      }
    }
  }

  /**
   * Expiry-aware protective de-risk eligibility.
   *
   * Effective protective deadline =
   *   min(unhedgedStartedAt + maxUnhedgedDurationMs, marketEnd - safetyWindow)
   * Attempts are additionally barred at/after marketEnd: once the market's
   * usable trading lifetime is over, settlement authority owns the position.
   * When the authoritative market endDate is unknown (e.g. no discovery
   * metadata for a recovered obligation), the pure duration deadline governs.
   */
  private protectiveDeRiskEligibility(
    marketId: string,
    unhedgedStartedAt: string,
    nowMs: number,
  ): { eligible: boolean; effectiveDeadlineMs: number | null; marketEndMs: number | null } {
    const startedMs = Date.parse(unhedgedStartedAt);
    const durationDeadlineMs = Number.isFinite(startedMs) ? startedMs + this.riskLimits.maxUnhedgedDurationMs : null;
    const marketEndValue = this.marketsById.get(marketId)?.endDate ?? null;
    const parsedEnd = marketEndValue ? Date.parse(marketEndValue) : Number.NaN;
    const marketEndMs = Number.isFinite(parsedEnd) ? parsedEnd : null;
    if (marketEndMs != null) {
      const expiryDeadlineMs = marketEndMs - PROTECTIVE_DE_RISK_PRE_EXPIRY_SAFETY_MS;
      const effectiveDeadlineMs = durationDeadlineMs == null ? expiryDeadlineMs : Math.min(durationDeadlineMs, expiryDeadlineMs);
      if (nowMs >= marketEndMs) return { eligible: false, effectiveDeadlineMs, marketEndMs };
      return { eligible: nowMs >= effectiveDeadlineMs, effectiveDeadlineMs, marketEndMs };
    }
    return {
      eligible: durationDeadlineMs != null && nowMs >= durationDeadlineMs,
      effectiveDeadlineMs: durationDeadlineMs,
      marketEndMs: null,
    };
  }

  /** Stable episode + two-side source evidence; a rejected order cannot monopolize a later deadline attempt. */
  private profitTrailingEvidenceKey(marketId: string, position: Position): string {
    const pair = this.snapshots.get(marketId);
    return JSON.stringify([position.id, unhedgedEpisode(position), pair?.UP?.eventId ?? null, pair?.DOWN?.eventId ?? null]);
  }

  private noteProfitTrailingRejectedEvidence(marketId: string, position: Position, nowMs: number): void {
    const deadline = this.protectiveDeRiskEligibility(
      marketId, position.unhedgedStartedAt ?? position.openedAt, nowMs,
    );
    if (deadline.eligible) this.profitTrailingRejectedEvidence.set(marketId, this.profitTrailingEvidenceKey(marketId, position));
  }

  /**
   * Only the duration deadline may yield to an *immediately actionable*,
   * fee-positive full PAIR. A non-actionable or stale book goes straight to
   * the existing protective A/B fallback. Market end - 80s never yields.
   * This guard is re-evaluated at durable admission and fill boundaries.
   */
  private profitTrailingRetainsAuthority(
    marketId: string,
    position: Position | null | undefined = this.ledger.positions.get(marketId),
    nowMs = this.nowFn().getTime(),
  ): boolean {
    if (!this.profitTrailingPairSoakEnabled()) return false;
    if (!position?.unpairedOutcome || d(position.unpairedQuantity).lte(0)) return false;
    const eligibility = this.protectiveDeRiskEligibility(
      marketId,
      position.unhedgedStartedAt ?? position.openedAt,
      nowMs,
    );
    if (eligibility.marketEndMs != null
      && nowMs >= eligibility.marketEndMs - PROTECTIVE_DE_RISK_PRE_EXPIRY_SAFETY_MS) return false;
    if (!eligibility.eligible) return true;
    if (this.profitTrailingRejectedEvidence.get(marketId) === this.profitTrailingEvidenceKey(marketId, position)) return false;

    const config = this.profitTrailingPairConfig();
    if (!config) return false;
    const now = new Date(nowMs);
    const opportunity = this.profitTrailingPairOpportunity(marketId, position, now);
    if (!opportunity || d(opportunity.lockedPnl).lte(0)) return false;
    const evaluation = evaluateProfitTrailingPair({
      state: this.profitTrailingPairStates.get(marketId) ?? null,
      marketId,
      positionId: position.id,
      unhedgedStartedAt: unhedgedEpisode(position),
      now,
      currentGrossEdge: opportunity.grossEdge,
      currentLockedPnl: opportunity.lockedPnl,
      targetEdge: config.target,
      armEdge: config.arm,
      retraceEdge: config.retrace,
    });
    return evaluation.action !== "NONE";
  }

  /**
   * Only snapshot/hydration-originated duration fallback is deferred by ONE
   * event-loop turn, so a paired UP/DOWN update can be evaluated together.
   * No timer grace is added to the 300s limit; maintenance fallback is unchanged.
   * Pre-expiry protection also remains immediate.
   */
  private scheduleSnapshotProtectiveDeRisk(marketId: string): void {
    // No asynchronous risk work exists for an empty/paired market. Pending
    // durable ownership still gets its recovery pass even if inventory closed.
    if (!this.ledger.positions.get(marketId)?.unpairedOutcome
      && !this.riskReductionOwners.has(marketId) && !this.riskReductionInFlight.has(marketId)) {
      this.clearProtectiveDeRiskState(marketId);
      return;
    }
    const run = () => {
      if (!this.state.draining) void this.maybeProtectiveDeRisk(marketId).catch((error) => {
        this.logger.error({ err: error, marketId }, "protective de-risk attempt failed");
      });
    };
    const position = this.ledger.positions.get(marketId);
    if (!this.profitTrailingPairSoakEnabled() || !position?.unpairedOutcome) {
      run();
      return;
    }
    const nowMs = this.nowFn().getTime();
    const deadline = this.protectiveDeRiskEligibility(
      marketId, position.unhedgedStartedAt ?? position.openedAt, nowMs,
    );
    if (!deadline.eligible || (deadline.marketEndMs != null
      && nowMs >= deadline.marketEndMs - PROTECTIVE_DE_RISK_PRE_EXPIRY_SAFETY_MS)) {
      run();
      return;
    }
    if (this.profitTrailingDeRiskSnapshotScheduled.has(marketId)) return;
    this.profitTrailingDeRiskSnapshotScheduled.add(marketId);
    const immediate = setImmediate(() => {
      this.profitTrailingDeRiskSnapshotScheduled.delete(marketId);
      run();
    });
    immediate.unref();
  }

  /**
   * PR3: early catastrophic full pair-lock.
   *
   * Runs ONLY BEFORE the old protective deadline (pre-deadline ownership),
   * continuously on every fresh synchronized market state. B-only: buys the
   * ENTIRE remaining residual of the opposite outcome. It enters the SAME
   * canonical deterministic rescue risk admission as PR82
   * (decideRescueSecondLegOrder / evaluateRisk) with current durable position,
   * projected exposure, available cash, database/risk state, execution errors
   * and rescue structural verification. A currently admissible normal
   * profitable hedge (canonical decidePaperOrder semantics on FRESH books)
   * always takes priority. Returns true when this pass CONSUMED the shared
   * risk-reduction decision (so the old deadline fallback must not also act).
   */
  private async maybeEarlyCatastrophicPairLock(marketId: string): Promise<EarlyPairLockResult> {
    if (this.soakBlocksPostEntry(marketId) || this.profitTrailingPairSoakEnabled()) return { acted: false };
    if (!this.earlyAuthority(marketId)) return { acted: false };
    const position = this.ledger.positions.get(marketId);
    if (!position?.unpairedOutcome || d(position.unpairedQuantity).lte(0)) return { acted: false };
    if (this.settledMarkets.has(marketId)) return { acted: false };
    if (this.riskReductionBusy(marketId)) return { acted: false };
    const now = this.nowFn();
    const pair = this.snapshots.get(marketId);
    const oppositeOutcome: Outcome = position.unpairedOutcome === "UP" ? "DOWN" : "UP";
    const held = this.freshEarlyCatastrophicSnapshot(position.unpairedOutcome === "UP" ? (pair?.UP ?? null) : (pair?.DOWN ?? null), now.getTime());
    const opposite = this.freshEarlyCatastrophicSnapshot(oppositeOutcome === "UP" ? (pair?.UP ?? null) : (pair?.DOWN ?? null), now.getTime());
    if (this.rescueExecutionFailure(position)) {
      // The failure override skips a repeated PR3 attempt, never NORMAL.
      const hedge = this.normalHedgeDecision(marketId, position, {
        UP: this.freshEarlyCatastrophicSnapshot(pair?.UP ?? null, now.getTime()),
        DOWN: this.freshEarlyCatastrophicSnapshot(pair?.DOWN ?? null, now.getTime()),
      }, now);
      if (!hedge?.accepted || !hedge.order || !opposite) return { acted: false };
      this.riskReductionInFlight.add(marketId);
      try { return await this.adoptHedgeFromEarlyFlight(marketId, hedge, opposite, now); }
      finally { this.riskReductionInFlight.delete(marketId); }
    }
    // CHEAP stage ONLY on the per-snapshot hot path (Section 8): real residual +
    // min age + mark-loss trigger + full opposite depth + loss cap. No canonical
    // hedge decision / risk projection runs unless the early action is otherwise
    // actionable.
    const cheap = this.earlyCheapGate({ position, held, opposite, nowMs: now.getTime() });
    if (!cheap.ok) {
      this.recordEarlyCatastrophicSuppressed(marketId, {
        eligible: false, reason: cheap.reason, candidate: cheap.candidate,
        markLossUsd: cheap.markLossUsd, guaranteedLockedPnl: cheap.guaranteedLockedPnl,
        rescueNotionalUsd: cheap.rescueNotionalUsd, rescueVwap: cheap.rescueVwap,
        rescueFeeUsd: cheap.rescueFeeUsd, hedgeAvailable: false,
      }, now);
      return { acted: false };
    }
    // Section 7 pacing: a stable canonical veto must not regenerate critical
    // RISK_EVALUATION writes at snapshot rate.
    if (this.earlyAttemptPaced(marketId, position, opposite, now)) {
      this.recordProtectiveDeRiskSuppressed(marketId, "EARLY_CATASTROPHIC_PAIR_LOCK", "RETRY_BACKOFF", {
        position, extra: { retryNextAttemptAtMs: this.earlyCatastrophicNextAttemptAt.get(marketId) ?? null },
      });
      return { acted: false };
    }
    this.riskReductionInFlight.add(marketId);
    try {
      return await this.executeEarlyCatastrophicPairLock(marketId, position.unpairedOutcome, oppositeOutcome, now);
    } finally {
      this.riskReductionInFlight.delete(marketId);
      const current = this.ledger.positions.get(marketId);
      if (!current || d(current.unpairedQuantity).lte(0)) this.clearProtectiveDeRiskState(marketId);
    }
  }

  /**
   * PR3 early ownership: the early catastrophic path is allowed only while the
   * old protective deadline has NOT yet been reached AND marketEnd has NOT been
   * reached (at/after marketEnd, settlement/expiry authority is exclusive).
   */
  private earlyAuthority(marketId: string): boolean {
    const position = this.ledger.positions.get(marketId);
    if (!position?.unpairedOutcome || d(position.unpairedQuantity).lte(0)) return false;
    const now = this.nowFn();
    const eligibility = this.protectiveDeRiskEligibility(
      marketId,
      position.unhedgedStartedAt ?? position.openedAt,
      now.getTime(),
    );
    if (eligibility.marketEndMs != null && now.getTime() >= eligibility.marketEndMs) return false;
    if (eligibility.eligible) return false;
    return true;
  }

  /**
   * Freshness/synchronization gate for one early-catastrophic book: the book
   * must be present, adapter-synchronized and within the stale-feed timeout.
   * Anything else yields null (fail closed; never price from stale data).
   */
  private freshEarlyCatastrophicSnapshot(snapshot: MarketSnapshot | null, nowMs: number): MarketSnapshot | null {
    if (!snapshot) return null;
    if (!this.adapterTokenSynchronized(snapshot.tokenId)) return null;
    const receivedAtMs = Date.parse(snapshot.receivedAt);
    // Future-dated books must fail closed: a negative age must never authorize
    // an early action or suppress it via a "fresh but impossible" hedge check.
    if (!Number.isFinite(receivedAtMs) || receivedAtMs > nowMs) return null;
    if (nowMs - receivedAtMs > this.riskLimits.staleFeedTimeoutMs) return null;
    return snapshot;
  }

  /**
   * Pure PR3 eligibility with the CANONICAL normal-hedge admissibility computed
   * from the SAME fresh/synchronized books (never from raw/stale snapshots) and
   * with the same decidePaperOrder/evaluateRisk semantics as the running hedge.
   */
  private evaluateEarlyCatastrophic(
    marketId: string,
    position: Position,
    held: MarketSnapshot | null,
    opposite: MarketSnapshot | null,
    now: Date,
    pair: Partial<Record<Outcome, MarketSnapshot>> | undefined,
  ): { evaluation: EarlyCatastrophicPairLockEvaluation; held: MarketSnapshot | null; opposite: MarketSnapshot | null } {
    // STAGED evaluation (Section 8): all cheap catastrophic gates run BEFORE any
    // canonical normal-hedge decision / risk projection. The expensive
    // hedge-priority work happens only when the early action is actionable.
    const cheap = this.earlyCheapGate({ position, held, opposite, nowMs: now.getTime() });
    if (!cheap.ok) {
      return {
        evaluation: {
          eligible: false, reason: cheap.reason, candidate: cheap.candidate,
          markLossUsd: cheap.markLossUsd, guaranteedLockedPnl: cheap.guaranteedLockedPnl,
          rescueNotionalUsd: cheap.rescueNotionalUsd, rescueVwap: cheap.rescueVwap,
          rescueFeeUsd: cheap.rescueFeeUsd, hedgeAvailable: false,
        },
        held,
        opposite,
      };
    }
    const hedgeAdmissible = this.normalHedgeAdmissible(marketId, position, {
      UP: this.freshEarlyCatastrophicSnapshot(pair?.UP ?? null, now.getTime()),
      DOWN: this.freshEarlyCatastrophicSnapshot(pair?.DOWN ?? null, now.getTime()),
    }, now);
    return {
      evaluation: evaluateEarlyCatastrophicPairLock({
        position,
        heldSnapshot: held,
        oppositeSnapshot: opposite,
        nowMs: now.getTime(),
        maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd,
        minUnhedgedMs: this.riskLimits.catastrophicPairLockMinUnhedgedMs,
        triggerLossUsd: this.riskLimits.catastrophicPairLockTriggerLossUsd,
        maxLockedLossUsd: this.riskLimits.catastrophicPairLockMaxLockedLossUsd,
        hedgeAdmissible,
      }),
      held,
      opposite,
    };
  }

/**
   * SHARED normal decision + risk basis (Section 2/10 of the pre-merge pass).
   *
   * This is the ONE semantic source for `decidePaperOrder` inputs used by BOTH
   * the live `evaluateTrade` normal-hedge/first-leg path and the PR3 hedge
   * priority check. It consumes the same externally material state as the live
   * path: reference-required/blocked/feed freshness, last BTC message,
   * PAPER_EXECUTION_STYLE, current database health, exposure/projection,
   * cooldown, experiment hedge gate and execution-error count.
   */
  private decideWithRuntimeBasis(input: {
    candidate: StrategyCandidate;
    consensus: ConsensusDecision;
    triggerId: string;
    signalExpiresAt: Date;
    snapshot: MarketSnapshot;
    pair: Partial<Record<Outcome, MarketSnapshot>>;
    now: Date;
    databaseAvailable: boolean;
    /** Live trigger asset override (trigger.asset) — preserves pre-PR3 semantics. */
    assetOverride?: string | null;
    /** Read-only observers set false so test/debug instrumentation is not mutated. */
    instrument?: boolean;
    /** Experiment-scoped execution style override; ordinary paths use env. */
    orderTypeOverride?: "TAKER" | "MAKER";
    /** Only the profit-trailing controller supplies this current-book proof. */
    profitTrailingLockedPnlUsd?: string;
    allowMakerFirstHedge?: boolean;
  }): { decision: ReturnType<typeof decidePaperOrder>; duplicateKey: string } {
    const { candidate, consensus, triggerId, signalExpiresAt, snapshot, pair, now, databaseAvailable, assetOverride } = input;
    const duplicateKey = `${candidate.marketId}:${candidate.outcome}:${candidate.intent}`;
    const asset = assetOverride ?? this.assetByMarket.get(candidate.marketId) ?? snapshot.asset ?? null;
    if (input.instrument !== false) this.lastRuntimeBasisAssetForTest = asset;
    const exposure = computePaperExposures(this.ledger.positions.values(), candidate.marketId, this.assetByMarket, now.getTime());
    const projected = projectPaperExposures({
      current: exposure,
      positions: candidate.intent==='OPEN_FIRST_LEG'&&this.makerFirstConfig().enabled
        ?this.entryProjectionLegs():projectableLegsFromPositions(this.ledger.positions.values(), this.assetByMarket),
      candidate: {
        marketId: candidate.marketId, outcome: candidate.outcome, quantity: candidate.quantity,
        price: candidate.limitPrice, side: candidate.side, intent: candidate.intent, asset,
      },
      nowMs: now.getTime(),
    });
    const referenceRequired = this.referenceRequired.get(candidate.marketId) === true;
    const referenceFeedAt = asset ? dateOrNull(this.state.referenceLastMessageAt(asset)) : null;
    const blocked = asset ? this.referenceBlocked.get(asset) : undefined;
    const marketFeedAt = collectMarketFeedAt(pair, candidate.outcome, candidate.intent, snapshot.receivedAt);
    const paperInput:Parameters<typeof decidePaperOrder>[0]={
      candidate,
      consensus,
      snapshotAfterDecisionLatency: snapshot,
      limits: this.riskLimits,
      triggerId,
      signalExpiresAt,
      orderType: input.orderTypeOverride ?? this.env.PAPER_EXECUTION_STYLE,
      baseRisk: {
        now, marketFeedAt,
        btcFeedAt: asset === "BTC" ? referenceFeedAt ?? dateOrNull(this.state.lastBtcMessageAt) : now,
        referenceFeedAt, referenceRequired, asset, assetExposureUsd: exposure.asset,
        databaseAvailable, timestampDisorder: this.timestampDisorder, riskStateAvailable: true,
        dailyPnlUsd: this.ledger.snapshot().netTradingPnl, drawdownUsd: this.ledger.snapshot().drawdown,
        marketExposureUsd: exposure.market, totalExposureUsd: exposure.total, directionalExposureUsd: exposure.directional,
        unhedgedExposureUsd: exposure.unhedged, unhedgedDurationMs: exposure.unhedgedDurationMs,
        intent: candidate.intent,
        ...(input.profitTrailingLockedPnlUsd !== undefined
          ? { profitTrailingLockedPnlUsd: input.profitTrailingLockedPnlUsd }
          : {}),
        ...(candidate.intent === "HEDGE_SECOND_LEG"
          ? {
            hedgePosition: (() => {
              const currentPosition = this.ledger.positions.get(candidate.marketId);
              if (!currentPosition?.unpairedOutcome) return null;
              const requiredOutcome: Outcome = currentPosition.unpairedOutcome === "UP" ? "DOWN" : "UP";
              return {
                marketId: currentPosition.marketId,
                unpairedOutcome: currentPosition.unpairedOutcome,
                unpairedQuantity: currentPosition.unpairedQuantity,
                expectedOppositeTokenId: this.currentTokenIdFor(currentPosition.marketId, requiredOutcome),
              };
            })(),
          }
          : {}),
        projectedMarketExposureUsd: projected.market, projectedTotalExposureUsd: projected.total,
        projectedDirectionalExposureUsd: projected.directional, projectedUnhedgedExposureUsd: projected.unhedged,
        projectedAssetExposureUsd: projected.asset, availableCashUsd: this.ledger.availableCash().toFixed(),
        referenceProviderBlocked: Boolean(blocked),
        marketEntryQualified: this.entryQualified.has(candidate.marketId),
        externalSignalsLastMinute: 0,
        lastStrategyTradeAt: this.lastTradeAt,
        duplicateTrade: (this.recentTrades.get(duplicateKey) ?? 0) > now.getTime() - this.riskLimits.strategyCooldownMs,
        experimentEntryOpen: candidate.intent === "HEDGE_SECOND_LEG"
          ? experimentAllowsHedge(this.experiment)
          : experimentAllowsFirstLeg(this.experiment, now),
        executionErrorsLastHour: this.executionErrors.filter((at) => now.getTime() - at < 3_600_000).length,
        externalRiskRecommendedVeto: false, shuttingDown: this.state.draining,
      },
    };
    let decision=decidePaperOrder(paperInput);
    const makerConfig=this.makerFirstConfig();
    const role=candidate.intent==='OPEN_FIRST_LEG'?'FIRST_LEG':candidate.intent==='HEDGE_SECOND_LEG'&&input.allowMakerFirstHedge?'HEDGE':null;
    if(makerConfig.enabled&&role&&decision.accepted&&decision.order?.orderType==='TAKER'
      &&!this.confirmedGuardianProtection(candidate.marketId)){
      const price=passiveMakerBuyPrice(snapshot,candidate.quantity,candidate.limitPrice);
      if(price&&(role!=='FIRST_LEG'||!firstLegEntryFilterReason(price,decision.expectedROI))&&decision.bookVwap){
        const improved=d(candidate.rawEdgeUsdPerShare??candidate.rawExpectedEdge).plus(decision.bookVwap).minus(price).toNumber();
        const maker=decidePaperOrder({...paperInput,orderType:'MAKER',candidate:{...candidate,rawEdgeUsdPerShare:improved,rawExpectedEdge:improved}});
        if(maker.accepted&&maker.order&&maker.netEvUsdTotal!==null&&decision.netEvUsdTotal!==null&&d(maker.netEvUsdTotal).gt(decision.netEvUsdTotal)){
          const ttl=role==='FIRST_LEG'?makerConfig.firstLegTtlMs:makerConfig.hedgeTtlMs;
          const end=this.marketsById.get(candidate.marketId)?.endDate;
          const expiresAt=Math.min(now.getTime()+ttl,signalExpiresAt.getTime(),end?Date.parse(end)-PROTECTIVE_DE_RISK_PRE_EXPIRY_SAFETY_MS:Number.POSITIVE_INFINITY);
          if(!Number.isFinite(expiresAt)||expiresAt<=now.getTime()+this.env.PAPER_EXECUTION_LATENCY_MS)return {decision,duplicateKey};
          maker.order.makerFirst={version:1,role,postOnly:true,expiresAt:new Date(expiresAt).toISOString(),
            submissionLatencyMs:this.env.PAPER_EXECUTION_LATENCY_MS,tokenEpoch:this.adapterTokenEpoch(snapshot.tokenId),
            postingBookEventId:snapshot.eventId,takerReferencePrice:decision.bookVwap};
          maker.order.makerFirst.postingFeeKey=JSON.stringify([snapshot.feeSchedule?.rate,snapshot.feeSchedule?.exponent,snapshot.feeSchedule?.takerOnly]);
          decision=maker;
        }
      }
    }
    return { decision, duplicateKey };
  }

  /** Instrumentation: canonical hedge-priority decisions evaluated by PR3. */
  pr3HedgeDecisionCountForTest(): number {
    return this.pr3HedgeDecisionCount;
  }

  /** Test hook: seed the strategy cooldown so a hedge decision vetoes via DUPLICATE_TRADE. */
  seedRecentTradeForTest(marketId: string, outcome: Outcome): void {
    this.recentTrades.set(`${marketId}:${outcome}:HEDGE_SECOND_LEG`, this.nowFn().getTime());
  }

  /** Test hook: expose the shared normal-hedge decision (Sections 2/10). */
  normalHedgeDecisionForTest(marketId: string, position: Position, pair: Record<Outcome, MarketSnapshot | null>, now: Date): ReturnType<typeof decidePaperOrder> | null {
    return this.normalHedgeDecision(marketId, position, pair, now);
  }
/**
   * Canonical normal-hedge decision for PR3 hedge priority. Uses the SAME
   * decideWithRuntimeBasis as the live hedge path (Section 2): candidate from
   * FRESH synchronized books, real reference/execution-state semantics and the
   * configured PAPER_EXECUTION_STYLE. Returns null when the hedge is not
   * available (candidate absent, books stale/unsynced/future, or decision
   * rejected). Counted for hot-path instrumentation (Section 8).
   */
  private normalHedgeDecision(
    marketId: string,
    position: Position,
    pair: Record<Outcome, MarketSnapshot | null>,
    now: Date,
  ): ReturnType<typeof decidePaperOrder> | null {
    if (!position.unpairedOutcome || d(position.unpairedQuantity).lte(0)) {
      this.pr3HedgeDebug.set(marketId, { stage: "NO_UNPAIRED", vetoes: null });
      return null;
    }
    const freshUp = this.freshEarlyCatastrophicSnapshot(pair.UP, now.getTime());
    const freshDown = this.freshEarlyCatastrophicSnapshot(pair.DOWN, now.getTime());
    if (!freshUp || !freshDown) {
      this.pr3HedgeDebug.set(marketId, { stage: `BOOK_UNAVAILABLE_${freshUp ? "DOWN" : freshDown ? "UP" : "BOTH"}`, vetoes: null });
      return null;
    }
    const candidate = evaluateNormalHedgeCandidate({
      position,
      snapshots: { UP: freshUp, DOWN: freshDown },
      maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd,
      minimumLockedPairEdge: this.riskLimits.normalHedgeMinLockedPairEdge,
    });
    if (!candidate) {
      this.pr3HedgeDebug.set(marketId, { stage: "NO_CANDIDATE", vetoes: null });
      return null;
    }
    const snapshot = candidate.outcome === "UP" ? freshUp : freshDown;
    if (snapshot.tokenId !== candidate.tokenId) {
      this.pr3HedgeDebug.set(marketId, { stage: `TOKEN_MISMATCH_${candidate.tokenId}`, vetoes: null });
      return null;
    }
    this.pr3HedgeDecisionCount += 1;
    const decision = this.decideWithRuntimeBasis({
      candidate,
      consensus: HEDGE_CHECK_CONSENSUS,
      triggerId: "early-catastrophic-hedge-check",
      signalExpiresAt: new Date(now.getTime() + 60_000),
      snapshot,
      pair: { UP: freshUp, DOWN: freshDown },
      now,
      databaseAvailable: this.state.database === "UP",
    }).decision;
    this.pr3HedgeDebug.set(marketId, { stage: "DECIDED", vetoes: decision.risk.vetoes });
    return decision;
  }

  /**
   * Side-effect-free canonical NORMAL hedge decision for the external Guardian
   * observer. This deliberately shares the live decision/risk basis but skips
   * PR3 debug counters/maps and test instrumentation.
   */
  private normalHedgeDecisionReadOnly(
    marketId: string,
    position: Position,
    pair: Record<Outcome, MarketSnapshot | null>,
    now: Date,
  ): ReturnType<typeof decidePaperOrder> | null {
    if (!position.unpairedOutcome || d(position.unpairedQuantity).lte(0)) return null;
    const freshUp = this.freshEarlyCatastrophicSnapshot(pair.UP, now.getTime());
    const freshDown = this.freshEarlyCatastrophicSnapshot(pair.DOWN, now.getTime());
    if (!freshUp || !freshDown) return null;
    const candidate = evaluateNormalHedgeCandidate({
      position,
      snapshots: { UP: freshUp, DOWN: freshDown },
      maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd,
      minimumLockedPairEdge: this.riskLimits.normalHedgeMinLockedPairEdge,
    });
    if (!candidate) return null;
    const snapshot = candidate.outcome === "UP" ? freshUp : freshDown;
    if (snapshot.tokenId !== candidate.tokenId) return null;
    return this.decideWithRuntimeBasis({
      candidate,
      consensus: HEDGE_CHECK_CONSENSUS,
      triggerId: "guardian-shadow-normal-check",
      signalExpiresAt: new Date(now.getTime() + 60_000),
      snapshot,
      pair: { UP: freshUp, DOWN: freshDown },
      now,
      databaseAvailable: this.state.database === "UP",
      instrument: false,
    }).decision;
  }

  /**
   * Observation-only authority surface for the external two-stage Guardian
   * forward soak. No durable state, order state, counters, debug maps, or
   * economic state are mutated here.
   */
  guardianShadowReadState(): Record<string, unknown> {
    const now = this.nowFn();
    const positions = [...this.ledger.positions.values()]
      .filter((position) => position.unpairedOutcome && d(position.unpairedQuantity).gt(0))
      .map((position) => {
        const pair = this.snapshots.get(position.marketId);
        const normalDecision = this.normalHedgeDecisionReadOnly(position.marketId, position, {
          UP: pair?.UP ?? null,
          DOWN: pair?.DOWN ?? null,
        }, now);
        const oppositeOutcome: Outcome = position.unpairedOutcome === "UP" ? "DOWN" : "UP";
        const held = position.unpairedOutcome === "UP" ? pair?.UP ?? null : pair?.DOWN ?? null;
        const opposite = oppositeOutcome === "UP" ? pair?.UP ?? null : pair?.DOWN ?? null;
        return {
          position: structuredClone(position),
          normalHedgeAdmissible: normalDecision?.accepted === true && normalDecision.order != null,
          normalHedgeVetoes: normalDecision?.risk.vetoes ?? [],
          normalHedgeOrder: normalDecision?.order ? {
            tokenId: normalDecision.order.tokenId,
            outcome: normalDecision.order.outcome,
            quantity: normalDecision.order.quantity,
            requestedPrice: normalDecision.order.requestedPrice,
          } : null,
          hedgeMakerOwnsResidual: this.activeHedgeMakerOwnsResidual(position.marketId, oppositeOutcome),
          riskReductionInFlight: this.riskReductionBusy(position.marketId),
          heldSnapshot: held ? structuredClone(held) : null,
          oppositeSnapshot: opposite ? structuredClone(opposite) : null,
        };
      });

    const tokenMap: Array<Record<string, unknown>> = [];
    for (const market of this.marketsById.values()) {
      for (const token of market.tokens) {
        if (!token.tokenId || !this.tokenIdSet.has(token.tokenId)) continue;
        const snapshot = this.state.marketSnapshots.get(token.tokenId) ?? null;
        tokenMap.push({
          marketId: market.id,
          tokenId: token.tokenId,
          outcome: token.outcome,
          asset: market.asset ?? null,
          minOrderSize: snapshot?.minOrderSize ?? null,
          tickSize: snapshot?.tickSize ?? null,
          feeSchedule: snapshot?.feeSchedule
            ? structuredClone(snapshot.feeSchedule)
            : market.feeSchedule
              ? structuredClone(market.feeSchedule)
              : null,
          sourceTimestamp: snapshot?.sourceTimestamp ?? null,
          receivedAt: snapshot?.receivedAt ?? null,
          wsCallbackAt: snapshot?.wsCallbackAt ?? null,
        });
      }
    }

    return {
      paperMode: true,
      generatedAt: now.toISOString(),
      experiment: this.experiment ? structuredClone(this.experiment) : null,
      riskConfiguration: {
        maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd,
        minExpectedEdge: this.riskLimits.minExpectedEdge,
        normalHedgeMinLockedPairEdge: this.riskLimits.normalHedgeMinLockedPairEdge,
        staleFeedTimeoutMs: this.riskLimits.staleFeedTimeoutMs,
        catastrophicPairLockMinUnhedgedMs: this.riskLimits.catastrophicPairLockMinUnhedgedMs,
        catastrophicPairLockTriggerLossUsd: this.riskLimits.catastrophicPairLockTriggerLossUsd,
        catastrophicPairLockMaxLockedLossUsd: this.riskLimits.catastrophicPairLockMaxLockedLossUsd,
      },
      positions,
      tokenMap,
      recentRiskEvents: this.state.decisionFeedForObservation(500).filter((item) =>
        item.type === "PAPER_EXECUTION_ERROR"
        || item.type === "RISK_REDUCTION_LIFECYCLE_BLOCKED"
        || item.type === "PAPER_ORDER_CREATED"
        || item.type === "PAPER_FILL"
        || item.type === "HEDGE_COMPLETE"
        || item.type === "RISK_ACCEPT"
        || item.type === "RISK_VETO"
      ).slice(0, 200),
    };
  }

  /** Test hook: last PR3 hedge-check gating reason per market. */
  pr3HedgeDebugForTest(marketId: string): { stage: string; vetoes: string[] | null } | null {
    return this.pr3HedgeDebug.get(marketId) ?? null;
  }

  /** Test hook: last asset resolved by the shared runtime decision basis. */
  runtimeBasisAssetForTest(): string | null {
    return this.lastRuntimeBasisAssetForTest;
  }

  /** Test hook: drive the shared runtime decision basis with an explicit asset override. */
  decideWithRuntimeBasisForTest(
    candidate: StrategyCandidate,
    pair: Record<Outcome, MarketSnapshot>,
    now: Date,
    assetOverride: string | null,
  ): ReturnType<typeof decidePaperOrder> {
    const snapshot = candidate.outcome === "UP" ? pair.UP : pair.DOWN;
    return this.decideWithRuntimeBasis({
      candidate,
      consensus: HEDGE_CHECK_CONSENSUS,
      triggerId: "runtime-basis-parity-test",
      signalExpiresAt: new Date(now.getTime() + 60_000),
      snapshot,
      pair,
      now,
      databaseAvailable: this.state.database === "UP",
      assetOverride,
    }).decision;
  }
  /** Canonical normal-hedge admissibility: the decision is actually accepted. */
  private normalHedgeAdmissible(
    marketId: string,
    position: Position,
    pair: Record<Outcome, MarketSnapshot | null>,
    now: Date,
  ): boolean {
    const decision = this.normalHedgeDecision(marketId, position, pair, now);
    return decision?.accepted === true && decision.order != null;
  }

/**
   * The in-flight half of the PR3 early catastrophic pair-lock (pre-merge
   * pass Sections 1-9, 11). Runs while the shared risk-reduction flight is
   * HELD. All durable/awaiting work (hedge-maker fencing, same-market
   * first-leg cancellation, canonical RISK_EVALUATION persistence) happens
   * BEFORE the final SYNCHRONOUS economic fence; between that fence and
   * PAPER_ORDER_CREATED no state-changing await remains. If a normal hedge is
   * canonically admissible at any decision point it is ADOPTED from this same
   * flight (actual execution, never a hypothetical priority). Returns
   * { acted } — true only when a durable economic order was created (hedge
   * adoption or early rescue).
   */
  private async executeEarlyCatastrophicPairLock(
    marketId: string,
    unpairedOutcome: Outcome,
    oppositeOutcome: Outcome,
    now: Date,
  ): Promise<EarlyPairLockResult> {
    return this.earlyCatastrophicInFlight(marketId, unpairedOutcome, oppositeOutcome, now);
  }

  /**
   * Full canonical early-catastrophic in-flight lifecycle (Sections 1, 3, 5,
   * 6, 9). Returns { acted } — true only when a durable economic order was
   * created (hedge adoption or early rescue).
   */
  private async earlyCatastrophicInFlight(
    marketId: string,
    unpairedOutcome: Outcome,
    oppositeOutcome: Outcome,
    now: Date,
  ): Promise<EarlyPairLockResult> {
    if (!this.earlyAuthority(marketId)) {
      this.recordProtectiveDeRiskSuppressed(marketId, "EARLY_CATASTROPHIC_PAIR_LOCK", "DEADLINE_OWNERSHIP", { position: this.ledger.positions.get(marketId) ?? null });
      return { acted: false };
    }
    const position0 = this.ledger.positions.get(marketId);
    if (!position0?.unpairedOutcome || position0.unpairedOutcome !== unpairedOutcome || d(position0.unpairedQuantity).lte(0)) {
      this.recordProtectiveDeRiskSuppressed(marketId, "EARLY_CATASTROPHIC_PAIR_LOCK", "POSITION_CHANGED", { position: position0 ?? null });
      return { acted: false };
    }
    if (this.settledMarkets.has(marketId)) return { acted: false };

    // (1) VALID resting HEDGE maker owns the residual -> EARLY yields (Fix 3).
    if (this.activeHedgeMakerOwnsResidual(marketId, oppositeOutcome)) {
      const p0 = position0;
      this.recordProtectiveDeRiskSuppressed(marketId, "EARLY_CATASTROPHIC_PAIR_LOCK", "HEDGE_MAKER_OWNS_RESIDUAL", {
        position: p0, snapshot: null, unhedgedDurationMs: Math.max(0, this.nowFn().getTime() - Date.parse(p0.unhedgedStartedAt ?? p0.openedAt)),
      });
      return { acted: false };
    }
    // Fence only OBSOLETE/STALE hedge makers (valid ones already yielded above).
    await this.fenceObsoleteHedgeMakers(marketId);

    // (2) Cheap + canonical evaluation on FRESH state (post hedge-maker cancel).
    let nowCurrent = this.nowFn();
    let fresh = this.evaluateEarlyCatastrophicWithBooks(marketId, position0, unpairedOutcome, oppositeOutcome, nowCurrent, this.snapshots.get(marketId));
    if (fresh.evaluation.hedgeAvailable) {
      // Section 3: a canonically admissible normal hedge is ADOPTED from this
      // same flight (actual execution), never left to a hypothetical trigger.
      const hedgeDecision = this.normalHedgeDecision(marketId, position0, {
        UP: this.freshEarlyCatastrophicSnapshot(this.snapshots.get(marketId)?.UP ?? null, nowCurrent.getTime()),
        DOWN: this.freshEarlyCatastrophicSnapshot(this.snapshots.get(marketId)?.DOWN ?? null, nowCurrent.getTime()),
      }, nowCurrent);
      if (hedgeDecision?.accepted && hedgeDecision.order && fresh.opposite) {
        return await this.adoptHedgeFromEarlyFlight(marketId, hedgeDecision, fresh.opposite, nowCurrent);
      }
      this.recordEarlyCatastrophicSuppressed(marketId, fresh.evaluation, nowCurrent);
      return { acted: false };
    }
    if (!fresh.evaluation.eligible || !fresh.evaluation.candidate || !fresh.opposite) {
      this.recordEarlyCatastrophicSuppressed(marketId, fresh.evaluation, nowCurrent);
      return { acted: false };
    }
    const expectedOppositeTokenId = this.currentTokenIdFor(marketId, oppositeOutcome);
    if (!expectedOppositeTokenId || fresh.evaluation.candidate.tokenId !== expectedOppositeTokenId) {
      this.recordEarlyCatastrophicSuppressed(marketId, fresh.evaluation, nowCurrent, { reason: "OPPOSITE_TOKEN_MISMATCH" });
      return { acted: false };
    }
    let asset = this.assetByMarket.get(marketId) ?? fresh.held?.asset ?? null;
// (3) PREFLIGHT canonical admission. Only an INSUFFICIENT_CASH-only veto
    //     with releasable same-market first-leg reservations triggers durable
    //     cancellation; any other veto is persisted (paced, Section 7).
    let admission = this.admitEarlyRescue(marketId, position0, oppositeOutcome, fresh.held, fresh.opposite, fresh.evaluation.candidate, nowCurrent, asset);
    if (admission && !admission.decision.accepted) {
      if (this.earlySameMarketFirstLegReservationCoverage(marketId, admission.decision)) {
        await this.cancelOutstandingFirstLegMakers(marketId, "EARLY_CATASTROPHIC_PAIR_LOCK");
        nowCurrent = this.nowFn();
        fresh = this.evaluateEarlyCatastrophicWithBooks(marketId, position0, unpairedOutcome, oppositeOutcome, nowCurrent, this.snapshots.get(marketId));
        if (fresh.evaluation.hedgeAvailable) {
          const hedgeDecision = this.normalHedgeDecision(marketId, position0, {
            UP: this.freshEarlyCatastrophicSnapshot(this.snapshots.get(marketId)?.UP ?? null, nowCurrent.getTime()),
            DOWN: this.freshEarlyCatastrophicSnapshot(this.snapshots.get(marketId)?.DOWN ?? null, nowCurrent.getTime()),
          }, nowCurrent);
          if (hedgeDecision?.accepted && hedgeDecision.order && fresh.opposite) {
            return await this.adoptHedgeFromEarlyFlight(marketId, hedgeDecision, fresh.opposite, nowCurrent);
          }
          this.recordEarlyCatastrophicSuppressed(marketId, fresh.evaluation, nowCurrent);
          return { acted: false };
        }
        if (!fresh.evaluation.eligible || !fresh.evaluation.candidate || !fresh.opposite) {
          this.recordEarlyCatastrophicSuppressed(marketId, fresh.evaluation, nowCurrent);
          return { acted: false };
        }
        asset = this.assetByMarket.get(marketId) ?? fresh.held?.asset ?? null;
        admission = this.admitEarlyRescue(marketId, position0, oppositeOutcome, fresh.held, fresh.opposite, fresh.evaluation.candidate, nowCurrent, asset);
      } else {
        await this.persistEarlyEvaluation(marketId, admission.decision, fresh.evaluation, nowCurrent, asset, false);
        this.noteEarlyRiskVetoes(admission.decision);
        this.recordEarlyCatastrophicSuppressed(marketId, fresh.evaluation, nowCurrent, {
          reason: admission.decision.risk.vetoes[0] ?? "RISK_VETO",
          vetoes: admission.decision.risk.vetoes,
        });
        this.paceEarlyAttempt(marketId, admission.decision, fresh, nowCurrent, false);
        return { acted: false };
      }
    }
    if (!admission) return { acted: false };
    if (!admission.decision.accepted) {
      await this.persistEarlyEvaluation(marketId, admission.decision, fresh.evaluation, nowCurrent, asset, false);
      this.noteEarlyRiskVetoes(admission.decision);
      this.recordEarlyCatastrophicSuppressed(marketId, fresh.evaluation, nowCurrent, {
        reason: admission.decision.risk.vetoes[0] ?? "RISK_VETO",
        vetoes: admission.decision.risk.vetoes,
      });
      this.paceEarlyAttempt(marketId, admission.decision, fresh, nowCurrent, false);
      return { acted: false };
    }
// (4) Durable same-market first-leg cancellation MUST precede the final
    //     admission so submission's internal cancelOutstandingFirstLegMakers is
    //     a NO-OP and the final fence truly linearizes (Section 1 / RACE-B).
    await this.cancelOutstandingFirstLegMakers(marketId, "EARLY_CATASTROPHIC_PAIR_LOCK");

    // (5) FINAL canonical admission (post all reservations released).
    const finalDecisionAttempt = this.admitEarlyRescue(marketId, position0, oppositeOutcome, fresh.held, fresh.opposite, fresh.evaluation.candidate, this.nowFn(), asset);
    if (!finalDecisionAttempt || !finalDecisionAttempt.decision.accepted) {
      if (finalDecisionAttempt) {
        await this.persistEarlyEvaluation(marketId, finalDecisionAttempt.decision, fresh.evaluation, this.nowFn(), asset, false);
        this.noteEarlyRiskVetoes(finalDecisionAttempt.decision);
        this.recordEarlyCatastrophicSuppressed(marketId, fresh.evaluation, this.nowFn(), {
          reason: finalDecisionAttempt.decision.risk.vetoes[0] ?? "RISK_VETO",
          vetoes: finalDecisionAttempt.decision.risk.vetoes,
        });
        this.paceEarlyAttempt(marketId, finalDecisionAttempt.decision, fresh, this.nowFn(), false);
      }
      return { acted: false };
    }
    const decision = finalDecisionAttempt.decision;

    // (6) Persist the accepted canonical RISK_EVALUATION (await boundary).
    await this.persistEarlyEvaluation(marketId, decision, fresh.evaluation, this.nowFn(), asset, true);
    this.paceEarlyAttempt(marketId, decision, fresh, this.nowFn(), true);

    const attempt: EarlyAdmissionAttempt = { riskId: decision.risk.id, admitted: false, reason: "SUBMISSION_NOT_ADMITTED" };
    try {
      // (7) FINAL SYNCHRONOUS FENCE — validates the PERSISTED decision whose
      //     risk/order identity is fixed (Fix 2). This is the last sync point
      //     before EARLY submit: no machine-await follows (Fix 1).
      const fence = this.finalEarlyEconomicFence(marketId, unpairedOutcome, oppositeOutcome, decision, fresh.opposite, asset);
      if (fence.adoptHedge) {
        attempt.reason = "NORMAL_HEDGE_PRIORITY";
        const hedgeDecision = this.normalHedgeDecision(marketId, position0, {
          UP: this.freshEarlyCatastrophicSnapshot(this.snapshots.get(marketId)?.UP ?? null, this.nowFn().getTime()),
          DOWN: this.freshEarlyCatastrophicSnapshot(this.snapshots.get(marketId)?.DOWN ?? null, this.nowFn().getTime()),
        }, this.nowFn());
        if (hedgeDecision?.accepted && hedgeDecision.order && fence.opposite) {
          return await this.adoptHedgeFromEarlyFlight(marketId, hedgeDecision, fence.opposite, this.nowFn());
        }
        this.recordProtectiveDeRiskSuppressed(marketId, "EARLY_CATASTROPHIC_PAIR_LOCK", "HEDGE_ADOPTION_REGRESSED", { position: position0, snapshot: fence.opposite ?? null });
        return { acted: false, acceptedWithoutAdmission: true };
      }
      if (!fence.ok) {
        attempt.reason = `FINAL_FENCE_${fence.reason}`;
        this.recordProtectiveDeRiskSuppressed(marketId, "EARLY_CATASTROPHIC_PAIR_LOCK", fence.reason, { position: position0, snapshot: fence.opposite ?? null });
        return { acted: false, acceptedWithoutAdmission: true };
      }
      const audit = this.buildEarlyAudit(marketId, position0, decision, fresh.evaluation, this.nowFn(), true);
      this.state.latestRisk = decision.risk;
      this.record("RISK_ACCEPT", decision.risk.traceId, marketId, {
        orderId: decision.order!.id,
        intent: "RESCUE_SECOND_LEG",
        deRisk: audit,
      }, asset);
      const created = await this.submitPaperOrder(
        decision.order!,
        fence.opposite!,
        `${marketId}:${oppositeOutcome}:EARLY_CATASTROPHIC_PAIR_LOCK`,
        "RESCUE_SECOND_LEG",
        audit,
        { skipPreCreateFirstLegCancel: true, earlyAttempt: attempt },
      );
      return { acted: created, acceptedWithoutAdmission: !attempt.admitted };
    } catch (error) {
      attempt.reason = `PRE_ADMISSION_ERROR:${error instanceof Error ? error.message : String(error)}`;
      throw error;
    } finally {
      // Terminal observability must never hold the economic flight/handoff.
      // Consume unexpected rejections as well as the helper's I/O failures.
      if (!attempt.admitted) void this.recordEarlyAttemptTerminated(marketId, decision.order!, attempt).catch((error) => {
        this.logger.error({ error, marketId, orderId: decision.order!.id, reason: attempt.reason }, "PR3 terminal diagnostic failed");
      });
    }
  }

  /** One operational fact per accepted attempt, independent of the ordinary
   * polling suppression gate. No economic transition or report hash input. */
  private async recordEarlyAttemptTerminated(marketId: string, order: PaperOrder, attempt: EarlyAdmissionAttempt): Promise<void> {
    // Freeze termination facts before detached I/O: current economic authority
    // may consume or replace the residual while the lookup/append is pending.
    const position = this.ledger.positions.get(marketId);
    const facts = {
      gate: "EARLY_ACCEPTED_ATTEMPT_TERMINATED", reason: attempt.reason,
      marketId, orderId: order.id, riskEvaluationId: attempt.riskId,
      intent: "RESCUE_SECOND_LEG", precheckAccepted: true, durableAdmission: false,
      finalState: "STATE_CHANGED", experimentId: this.experiment?.id ?? null,
      unhedgedStartedAt: position ? unhedgedEpisode(position) : null,
      residualQuantity: position?.unpairedQuantity ?? "0",
      competingOwnerOrderId: this.riskReductionOwners.get(marketId)?.order.id ?? null,
    };
    try {
      if (this.abort.signal.aborted) return;
      // A post-commit error can precede the local admitted flag. Preserve the
      // durable check, but only on this detached operational path.
      if (await this.journal.readDurableByOperationKey(`risk-reduction:${order.id}:admitted`)) return;
      // stop() need not wait for an uncancellable diagnostic lookup, and its
      // late result must never start an append against the closed journal.
      if (this.abort.signal.aborted) return;
      this.record("PROTECTIVE_DE_RISK_SUPPRESSED", order.traceId, marketId, facts);
      await this.journal.append(createJournalEvent({
        id: derivedAuditEventId("EARLY_ACCEPTED_ATTEMPT_TERMINATED", order.id),
        traceId: order.traceId, type: "PROTECTIVE_DE_RISK_SUPPRESSED",
        aggregateType: "paper_order", aggregateId: order.id, payload: facts,
      }), { signal: this.abort.signal });
    } catch (error) {
      // Diagnostic write failures must not change economic authority/health.
      this.logger.error({ error, marketId, orderId: order.id, reason: attempt.reason }, "PR3 terminal diagnostic persistence failed");
    }
  }
/** Selects FRESH canonical held/opposite books and runs the staged PR3 predicate. */
  private evaluateEarlyCatastrophicWithBooks(
    marketId: string,
    position: Position,
    unpairedOutcome: Outcome,
    oppositeOutcome: Outcome,
    now: Date,
    pair: Partial<Record<Outcome, MarketSnapshot>> | undefined,
  ): { evaluation: EarlyCatastrophicPairLockEvaluation; held: MarketSnapshot | null; opposite: MarketSnapshot | null } {
    const held = this.freshEarlyCatastrophicSnapshot(unpairedOutcome === "UP" ? (pair?.UP ?? null) : (pair?.DOWN ?? null), now.getTime());
    const opposite = this.freshEarlyCatastrophicSnapshot(oppositeOutcome === "UP" ? (pair?.UP ?? null) : (pair?.DOWN ?? null), now.getTime());
    const evaluation = this.evaluateEarlyCatastrophic(marketId, position, held, opposite, now, pair);
    return { evaluation: evaluation.evaluation, held, opposite };
  }

  /**
   * True when a VALID resting HEDGE_SECOND_LEG maker already owns the required
   * opposite residual (current token, remaining > 0). Such a maker is actual
   * hedge authority — EARLY yields instead of cancelling it (Fix 3).
   */
  private activeHedgeMakerOwnsResidual(marketId: string, oppositeOutcome: Outcome): boolean {
    const expectedTokenId = this.currentTokenIdFor(marketId, oppositeOutcome);
    if (!expectedTokenId) return false;
    for (const resting of this.restingOrders.values()) {
      if (resting.order.marketId !== marketId) continue;
      if (!d(resting.remaining).gt(0)) continue;
      if (this.restingOrderIntent(resting) !== "HEDGE_SECOND_LEG") continue;
      if (resting.order.tokenId !== expectedTokenId) continue;
      return true;
    }
    return false;
  }

  /**
   * Fence only OBSOLETE/STALE resting hedge makers (token no longer matches the
   * current opposite outcome). VALID makers already yielded in
   * activeHedgeMakerOwnsResidual, so no live hedge authority is destroyed before
   * rescue owns the residual (Fix 3).
   */
  private async fenceObsoleteHedgeMakers(marketId: string): Promise<void> {
    const position = this.ledger.positions.get(marketId);
    if (!position?.unpairedOutcome) return;
    const oppositeOutcome: Outcome = position.unpairedOutcome === "UP" ? "DOWN" : "UP";
    const expectedTokenId = this.currentTokenIdFor(marketId, oppositeOutcome);
    for (const [orderId, resting] of [...this.restingOrders.entries()]) {
      if (resting.order.marketId !== marketId) continue;
      if (!d(resting.remaining).gt(0)) continue;
      if (this.restingOrderIntent(resting) !== "HEDGE_SECOND_LEG") continue;
      if (expectedTokenId && resting.order.tokenId === expectedTokenId) continue;
      await this.releaseMaker(orderId, { cancel: true, reason: "EARLY_CATASTROPHIC_PAIR_LOCK_STALE_MAKER" });
    }
  }


  /**
   * CHEAP catastrophic gate (Section 8): pure decimal/book math only —
   * no canonical hedge decision, no decidePaperOrder/evaluateRisk — so the
   * non-actionable snapshot hot path does NO expensive canonical work. A full
   * residual + loss cap are prerequisites before any hedge/risk admission runs.
   */
  private earlyCheapGate(input: {
    position: Position | null;
    held: MarketSnapshot | null;
    opposite: MarketSnapshot | null;
    nowMs: number;
  }): {
    ok: boolean;
    reason: EarlyCatastrophicRejectionReason | null;
    markLossUsd: string | null;
    candidate: StrategyCandidate | null;
    rescueNotionalUsd: string | null;
    rescueVwap: string | null;
    rescueFeeUsd: string | null;
    guaranteedLockedPnl: string | null;
  } {
    const empty = {
      ok: false as const, reason: "NO_UNPAIRED_RESIDUAL", markLossUsd: null as string | null,
      candidate: null as StrategyCandidate | null, rescueNotionalUsd: null as string | null,
      rescueVwap: null as string | null, rescueFeeUsd: null as string | null, guaranteedLockedPnl: null as string | null,
    };
    const { position, held, opposite, nowMs } = input;
    if (!position?.unpairedOutcome || d(position.unpairedQuantity).lte(0)) return { ...empty, reason: "NO_UNPAIRED_RESIDUAL" };
    const startedAt = position.unhedgedStartedAt ?? position.openedAt;
    const startedMs = Date.parse(startedAt);
    const unhedgedAgeMs = Number.isFinite(startedMs) ? Math.max(0, nowMs - startedMs) : Number.POSITIVE_INFINITY;
    if (!Number.isFinite(unhedgedAgeMs) || unhedgedAgeMs < this.riskLimits.catastrophicPairLockMinUnhedgedMs) {
      return { ...empty, reason: "UNHEDGED_AGE_NOT_REACHED" };
    }
    if (!held) return { ...empty, reason: "MARK_UNAVAILABLE" };
    const markLossUsd = computeRealizableMarkLossUsd(position, held);
    if (markLossUsd === null) return { ...empty, reason: "MARK_UNAVAILABLE" };
    if (d(markLossUsd).lt(this.riskLimits.catastrophicPairLockTriggerLossUsd)) {
      return { ...empty, markLossUsd: String(markLossUsd), reason: "MARK_LOSS_NOT_REACHED" };
    }
    if (!opposite) return { ...empty, markLossUsd: String(markLossUsd), reason: "OPPOSITE_ASK_UNAVAILABLE" };
    const candidateEvaluation = evaluateRescueSecondLegCandidate({ position, snapshot: opposite, maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd });
    const candidate = candidateEvaluation.candidate;
    if (!candidate) return { ...empty, markLossUsd: String(markLossUsd), reason: "OPPOSITE_ASK_UNAVAILABLE" };
    if (!d(candidate.quantity).eq(d(position.unpairedQuantity))) {
      return { ...empty, markLossUsd: String(markLossUsd), candidate, reason: "PARTIAL_OPPOSITE_DEPTH" };
    }
    if (!opposite.feeSchedule || !opposite.minOrderSize) {
      return { ...empty, markLossUsd: String(markLossUsd), candidate, reason: "OPPOSITE_ASK_UNAVAILABLE" };
    }
    const walk = walkBook(opposite.asks, candidate.quantity, candidate.limitPrice, "BUY");
    if (!walk.sufficientDepth || !d(walk.filledQuantity).eq(candidate.quantity)) {
      return { ...empty, markLossUsd: String(markLossUsd), candidate, reason: "PARTIAL_OPPOSITE_DEPTH" };
    }
    const rescueFeeUsd = calculateFee(candidate.quantity, walk.vwap, "TAKER", opposite.feeSchedule);
    const guaranteedLockedPnl = computeGuaranteedLockedPnl({
      position, rescueQuantity: candidate.quantity, rescueVwap: walk.vwap, rescueTakerFeeUsd: rescueFeeUsd,
    });
    if (d(guaranteedLockedPnl).lt(d(-this.riskLimits.catastrophicPairLockMaxLockedLossUsd))) {
      return {
        ...empty, markLossUsd: String(markLossUsd), candidate, guaranteedLockedPnl,
        rescueNotionalUsd: walk.notional, rescueVwap: walk.vwap, rescueFeeUsd, reason: "LOCKED_LOSS_EXCEEDS_CAP",
      };
    }
    return {
      ok: true, reason: null, markLossUsd: String(markLossUsd), candidate,
      rescueNotionalUsd: walk.notional, rescueVwap: walk.vwap, rescueFeeUsd, guaranteedLockedPnl,
    };
  }
/**
   * True when a vetoed rescue admission is blocked ONLY by INSUFFICIENT_CASH
   * and the SAME-MARKET first-leg maker reservations would cover the deficit
   * (Section 6). Unrelated markets are never counted as releasable cash.
   */
  private earlySameMarketFirstLegReservationCoverage(
    marketId: string,
    decision: ReturnType<typeof decideRescueSecondLegOrder>,
  ): boolean {
    const vetoes = decision.risk.vetoes;
    if (vetoes.length !== 1 || vetoes[0] !== "INSUFFICIENT_CASH") return false;
    const requiredCash = decision.requiredCashUsd ?? "0";
    const deficit = d(requiredCash).minus(this.ledger.availableCash());
    if (deficit.lte(0)) return false;
    let releasable = d(0);
    for (const [orderId, reserved] of this.makerReservations.entries()) {
      const resting = this.restingOrders.get(orderId);
      if (!resting || resting.order.marketId !== marketId) continue;
      if (this.restingOrderIntent(resting) !== "OPEN_FIRST_LEG") continue;
      releasable = releasable.plus(reserved);
    }
    return releasable.gte(deficit);
  }

  /** Section 7: stable-veto pacing for early RISK_EVALUATION writes. */
  private earlyAttemptFingerprint(position: Position, opposite: MarketSnapshot | null): string {
    const topAsk = opposite?.asks[0] ?? null;
    return JSON.stringify([
      position.marketId,
      position.unpairedQuantity,
      this.ledger.availableCash().toFixed(),
      topAsk?.price ?? null,
      topAsk?.quantity ?? null,
      position.unhedgedStartedAt ?? position.openedAt,
    ]);
  }

  private paceEarlyAttempt(
    marketId: string,
    decision: ReturnType<typeof decideRescueSecondLegOrder>,
    fresh: { held: MarketSnapshot | null; opposite: MarketSnapshot | null },
    now: Date,
    accepted: boolean,
  ): void {
    this.protectiveDeRiskAttempts.set(marketId, (this.protectiveDeRiskAttempts.get(marketId) ?? 0) + 1);
    if (accepted) {
      // Acceptance retires a previous veto, but consumes no action retry slot.
      // admitRiskReduction installs the 5s slot only after durable admission.
      this.earlyCatastrophicNextAttemptAt.delete(marketId);
      this.earlyCatastrophicVetoFingerprint.delete(marketId);
      return;
    }
    const position = this.ledger.positions.get(marketId);
    const fingerprint = `${decision.risk.vetoes.join("|")}|${position ? this.earlyAttemptFingerprint(position, fresh.opposite) : "none"}`;
    this.earlyCatastrophicVetoFingerprint.set(marketId, fingerprint);
    this.earlyCatastrophicNextAttemptAt.set(marketId, now.getTime() + PROTECTIVE_DE_RISK_VETO_RETRY_MS);
  }

  private earlyAttemptPaced(marketId: string, position: Position, opposite: MarketSnapshot | null, now: Date): boolean {
    const nextAttemptAt = this.earlyCatastrophicNextAttemptAt.get(marketId) ?? 0;
    if (now.getTime() < nextAttemptAt) {
      const stored = this.earlyCatastrophicVetoFingerprint.get(marketId);
      if (stored != null) {
        const current = this.earlyAttemptFingerprint(position, opposite);
        if (stored.endsWith(`|${current}`)) return true;
      } else {
        return true;
      }
    }
    return false;
  }

  private noteEarlyRiskVetoes(decision: ReturnType<typeof decideRescueSecondLegOrder>): void {
    for (const reason of decision.risk.vetoes) this.metrics.riskVetoes.inc({ reason });
    this.state.latestRisk = decision.risk;
  }
/**
   * FINAL SYNCHRONOUS ECONOMIC FENCE (Section 1). Runs at the last possible
   * synchronous point before an EARLY PAPER_ORDER_CREATED, after every
   * pre-admission await. Proves: durable position residual/outcome unchanged,
   * market not settled/expired, early pre-deadline authority still owned
   * (never at/after marketEnd), canonical books fresh/synchronized and not
   * future-dated, full residual still executable, loss cap still passes and a
   * canonical rescue decision against current cash/exposure/risk state is still
   * acceptable. Also re-evaluates normal-hedge admissibility so a hedge that
   * became authoritative during the last async boundary wins (ADOPTED).
   * No state-changing await exists between this fence and EARLY submit.
   */
private finalEarlyEconomicFence(
    marketId: string,
    unpairedOutcome: Outcome,
    oppositeOutcome: Outcome,
    persistedDecision: ReturnType<typeof decideRescueSecondLegOrder>,
    oppositeAtDecision: MarketSnapshot | null,
    assetAtDecision: string | null,
  ): {
    ok: boolean;
    reason: string;
    adoptHedge: boolean;
    opposite?: MarketSnapshot | null;
  } {
    const now = this.nowFn();
    const position = this.ledger.positions.get(marketId);
    if (!position?.unpairedOutcome || position.unpairedOutcome !== unpairedOutcome || d(position.unpairedQuantity).lte(0)) {
      return { ok: false, reason: "POSITION_CHANGED", adoptHedge: false };
    }
    if (this.settledMarkets.has(marketId)) return { ok: false, reason: "SETTLED_MARKET", adoptHedge: false };
    // Fix 6: pre-deadline ownership must hold at the fence instant too.
    if (!this.earlyAuthority(marketId)) return { ok: false, reason: "DEADLINE_OWNERSHIP", adoptHedge: false };
    const pair = this.snapshots.get(marketId);
    const held = this.freshEarlyCatastrophicSnapshot(unpairedOutcome === "UP" ? (pair?.UP ?? null) : (pair?.DOWN ?? null), now.getTime());
    const opposite = this.freshEarlyCatastrophicSnapshot(oppositeOutcome === "UP" ? (pair?.UP ?? null) : (pair?.DOWN ?? null), now.getTime());
    if (!held || !opposite) return { ok: false, reason: "BOOK_UNAVAILABLE_OR_FUTURE_STALE", adoptHedge: false };
    // Persisted decision identity must still match the CURRENT token/residual.
    const persistedOrder = persistedDecision.order;
    if (!persistedOrder) return { ok: false, reason: "ORDER_MISSING", adoptHedge: false };
    const expectedTokenId = this.currentTokenIdFor(marketId, oppositeOutcome);
    if (!expectedTokenId || persistedOrder.tokenId !== expectedTokenId) {
      return { ok: false, reason: "OPPOSITE_TOKEN_MISMATCH", adoptHedge: false };
    }
    if (!d(persistedOrder.quantity).eq(d(position.unpairedQuantity))) {
      return { ok: false, reason: "RESIDUAL_CHANGED", adoptHedge: false };
    }
    // A normal hedge that became canonically admissible is ADOPTED instead of
    // the persisted rescue (only valid while pre-deadline; earlyAuthority above).
    const hedgeDecision = this.normalHedgeDecision(marketId, position, {
      UP: this.freshEarlyCatastrophicSnapshot(pair?.UP ?? null, now.getTime()),
      DOWN: this.freshEarlyCatastrophicSnapshot(pair?.DOWN ?? null, now.getTime()),
    }, now);
    if (hedgeDecision?.accepted && hedgeDecision.order) {
      return { ok: true, reason: "HEDGE_ADOPTED", adoptHedge: true, opposite };
    }
    // The held mark can improve during either pre-admission await. Reapply
    // the current PR3 predicate as well as the persisted-order execution fences.
    const currentGate = this.earlyCheapGate({ position, held, opposite, nowMs: now.getTime() });
    if (!currentGate.ok) return { ok: false, reason: currentGate.reason ?? "ADMISSION_REGRESSED", adoptHedge: false };
    // VALIDATION-ONLY canonical admission (Fix 2): never persisted, never
    // submitted. Only used to prove the PERSISTED decision is still executable.
    const candidateValidation = evaluateRescueSecondLegCandidate({ position, snapshot: opposite, maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd }).candidate;
    if (!candidateValidation || !d(candidateValidation.quantity).eq(d(position.unpairedQuantity))) {
      return { ok: false, reason: "PARTIAL_OPPOSITE_DEPTH", adoptHedge: false };
    }
    if (!opposite.feeSchedule || !opposite.minOrderSize) {
      return { ok: false, reason: "BOOK_UNAVAILABLE_OR_FUTURE_STALE", adoptHedge: false };
    }
    const validation = this.admitEarlyRescue(marketId, position, oppositeOutcome, held, opposite, candidateValidation, now, assetAtDecision ?? held.asset ?? null);
    if (!validation || !validation.decision.accepted || !validation.decision.order) {
      return { ok: false, reason: validation?.decision.risk.vetoes.length
        ? `RISK_VETO:${validation.decision.risk.vetoes.join("|")}` : "ADMISSION_REGRESSED", adoptHedge: false };
    }
    const vOrder = validation.decision.order;
    if (vOrder.tokenId !== persistedOrder.tokenId || !d(vOrder.quantity).eq(d(persistedOrder.quantity))) {
      return { ok: false, reason: "STATE_CHANGED", adoptHedge: false };
    }
    // Current-walk economics: actual fill (PAPER) at the CURRENT executable book
    // must still satisfy the configured loss cap for the PERSISTED order.
    const walk = walkBook(opposite.asks, persistedOrder.quantity, persistedOrder.requestedPrice, "BUY");
    if (!walk.sufficientDepth || !d(walk.filledQuantity).eq(persistedOrder.quantity)) {
      return { ok: false, reason: "PARTIAL_OPPOSITE_DEPTH", adoptHedge: false };
    }
    const feeUsd = calculateFee(persistedOrder.quantity, walk.vwap, "TAKER", opposite.feeSchedule);
    const lockedPnl = computeGuaranteedLockedPnl({ position, rescueQuantity: persistedOrder.quantity, rescueVwap: walk.vwap, rescueTakerFeeUsd: feeUsd });
    if (d(lockedPnl).lt(d(-this.riskLimits.catastrophicPairLockMaxLockedLossUsd))) {
      return { ok: false, reason: "LOSS_CAP_EXCEEDED", adoptHedge: false };
    }
    return { ok: true, reason: "OK", adoptHedge: false, opposite };
  }
  /**
   * Section 3: ADOPT the normal hedge from the SAME risk-reduction flight. The
   * early flight is released and the live HEDGE_SECOND_LEG submission re-acquires
   * it in the same synchronous tick (exactly-one risk-reduction authority
   * preserved, no interleaving). The residual is never left naked to a
   * hypothetical hedge.
   */
  private async adoptHedgeFromEarlyFlight(
    marketId: string,
    hedgeDecision: ReturnType<typeof decidePaperOrder>,
    oppositeSnapshot: MarketSnapshot,
    now: Date,
  ): Promise<{ acted: boolean }> {
    // Fix 6: never adopt a normal hedge as a NEW pre-deadline decision once the
    // protective deadline (or marketEnd) owns the position. Yield to the
    // caller's same-pass PR82 handoff instead — no extra snapshot required.
    if (!this.earlyAuthority(marketId)) {
      this.recordProtectiveDeRiskSuppressed(marketId, "EARLY_CATASTROPHIC_PAIR_LOCK", "DEADLINE_OWNERSHIP", { position: this.ledger.positions.get(marketId) ?? null });
      return { acted: false };
    }
    const order = hedgeDecision.order!;
    const snapshot = order.tokenId === oppositeSnapshot.tokenId
      ? oppositeSnapshot
      : (this.state.marketSnapshots.get(order.tokenId) ?? oppositeSnapshot);
    const asset = this.assetByMarket.get(marketId) ?? snapshot.asset ?? null;
    const audit = this.buildHedgeAdoptionAudit(marketId, order, now);
    this.record("RISK_ACCEPT", order.traceId, marketId, { orderId: order.id, intent: "HEDGE_SECOND_LEG", deRisk: audit }, asset);
    this.record("NORMAL_HEDGE_ADOPTED_UNDER_EARLY_FLIGHT", order.traceId, marketId, {
      orderId: order.id, reason: "EARLY_HEDGE_PRIORITY_ADOPTION",
    }, asset);
    this.riskReductionInFlight.delete(marketId);
    try {
      const created = await this.submitPaperOrder(
        order,
        snapshot,
        `${order.marketId}:${order.outcome}:HEDGE_SECOND_LEG`,
        "HEDGE_SECOND_LEG",
        audit,
      );
      return { acted: created };
    } finally {
      // NOTE: the HEDGE submission owns/releases the flight itself; if it was
      // suppressed (deadline reached) the caller's same-pass handoff covers it.
      this.riskReductionInFlight.delete(marketId);
    }
  }

  private buildHedgeAdoptionAudit(marketId: string, order: PaperOrder, now: Date): ProtectiveDeRiskAudit {
    const position = this.ledger.positions.get(marketId);
    const unhedgedStartedAt = position?.unhedgedStartedAt ?? position?.openedAt ?? now.toISOString();
    return {
      deRiskReason: "EARLY_HEDGE_ADOPTION",
      eligibilityReason: "EARLY_HEDGE_ADOPTION",
      unhedgedStartedAt,
      unhedgedDurationMs: Math.max(0, now.getTime() - Date.parse(unhedgedStartedAt)),
      attemptedAction: "HEDGE_SECOND_LEG",
      requestedQty: position?.unpairedQuantity ?? "0",
      executableQty: order.quantity,
      executionPrice: null,
      residualQuantity: "0",
      residualExposure: "0",
      retryCount: (this.protectiveDeRiskAttempts.get(marketId) ?? 0) + 1,
      finalState: "ORDER_ADMITTED",
      selection: "NONE",
      tieBreakUsed: false,
      aAvailable: false,
      bAvailable: false,
      bReason: null,
      bQuantity: null, bVwap: null, bFeeUsd: null, bRequiredCashUsd: null, bGuaranteedSalvageUsd: null,
    };
  }
/**
   * Canonical deterministic rescue risk admission (Section 1/10): the ONLY
   * source of accepted/vetoes and of the PaperOrder identity is
   * decideRescueSecondLegOrder (which runs evaluateRisk with rescue structural
   * verification) against the current durable position, projected exposure,
   * available cash, database/risk state, execution-error count and canonical
   * opposite token. PR3-only gates stay outside this canonical rescue policy.
   */
  private admitEarlyRescue(
    marketId: string,
    position: Position,
    oppositeOutcome: Outcome,
    held: MarketSnapshot | null,
    opposite: MarketSnapshot | null,
    candidate: StrategyCandidate,
    now: Date,
    asset: string | null,
  ): { decision: ReturnType<typeof decideRescueSecondLegOrder> } | null {
    if (!opposite || !held) return null;
    const executionErrorsLastHour = this.executionErrors.filter((at) => now.getTime() - at < 3_600_000).length;
    const exposure = computePaperExposures(this.ledger.positions.values(), marketId, this.assetByMarket, now.getTime());
    const basis = this.buildRescueRiskBasis({
      marketId, position, oppositeOutcome, heldSnapshot: held, oppositeSnapshot: opposite,
      candidate, exposure, now, asset, executionErrorsLastHour,
    });
    if (!basis) return null;
    return {
      decision: decideRescueSecondLegOrder({
        candidate,
        snapshotAfterDecisionLatency: opposite,
        limits: this.riskLimits,
        traceId: createId("derisk"),
        triggerId: createId("derisk-trigger"),
        baseRisk: basis.baseRiskB,
        rescuePosition: basis.rescuePosition,
      }),
    };
  }

  /**
   * Canonical RESCUE risk basis constructed from the SAME facts the PR82
   * deadline fallback uses for its B decision (exposures, ledger,
   * availableCash, feed/database/risk state). PR3-only gates are NOT here;
   * parity with PR82's inline construction is proven by test.
   */
  private buildRescueRiskBasis(input: {
    marketId: string;
    position: Position;
    oppositeOutcome: Outcome;
    heldSnapshot: MarketSnapshot | null;
    oppositeSnapshot: MarketSnapshot | null;
    candidate: StrategyCandidate;
    exposure: ReturnType<typeof computePaperExposures>;
    now: Date;
    asset: string | null;
    executionErrorsLastHour: number;
  }): { baseRiskB: Parameters<typeof decideRescueSecondLegOrder>[0]["baseRisk"]; rescuePosition: HedgePositionFacts } | null {
    if (!input.oppositeSnapshot) return null;
    const projectedB = projectPaperExposures({
      current: input.exposure,
      positions: projectableLegsFromPositions(this.ledger.positions.values(), this.assetByMarket),
      candidate: {
        marketId: input.marketId, outcome: input.oppositeOutcome, quantity: input.candidate.quantity,
        price: input.candidate.limitPrice, side: "BUY", intent: "RESCUE_SECOND_LEG", asset: input.asset,
      },
      nowMs: input.now.getTime(),
    });
    if (!projectedB) return null;
    const ledger = this.ledger.snapshot();
    const rescuePosition: HedgePositionFacts = {
      marketId: input.position.marketId,
      unpairedOutcome: input.position.unpairedOutcome,
      unpairedQuantity: input.position.unpairedQuantity,
      expectedOppositeTokenId: this.currentTokenIdFor(input.marketId, input.oppositeOutcome),
    };
    const baseRiskCommon = {
      now: input.now,
      marketFeedAt: new Date(input.heldSnapshot?.receivedAt ?? input.now.toISOString()),
      btcFeedAt: input.now,
      referenceFeedAt: null,
      referenceRequired: false,
      asset: input.asset,
      assetExposureUsd: input.exposure.asset,
      databaseAvailable: this.state.database === "UP",
      timestampDisorder: this.timestampDisorder,
      riskStateAvailable: this.protectiveRiskStateAvailable(),
      dailyPnlUsd: ledger.netTradingPnl,
      drawdownUsd: ledger.drawdown,
      marketExposureUsd: input.exposure.market,
      totalExposureUsd: input.exposure.total,
      directionalExposureUsd: input.exposure.directional,
      unhedgedExposureUsd: input.exposure.unhedged,
      unhedgedDurationMs: input.exposure.unhedgedDurationMs,
      availableCashUsd: this.ledger.availableCash().toFixed(),
      referenceProviderBlocked: false,
      marketEntryQualified: this.entryQualified.has(input.marketId),
      externalSignalsLastMinute: 0,
      lastStrategyTradeAt: this.lastTradeAt,
      duplicateTrade: false,
      experimentEntryOpen: experimentAllowsHedge(this.experiment),
      executionErrorsLastHour: input.executionErrorsLastHour,
      externalRiskRecommendedVeto: false,
      shuttingDown: this.state.draining,
    };
    return {
      baseRiskB: {
        ...baseRiskCommon,
        projectedMarketExposureUsd: projectedB.market,
        projectedTotalExposureUsd: projectedB.total,
        projectedDirectionalExposureUsd: projectedB.directional,
        projectedUnhedgedExposureUsd: projectedB.unhedged,
        projectedAssetExposureUsd: projectedB.asset,
      },
      rescuePosition,
    };
  }
private buildEarlyAudit(
    marketId: string,
    position: Position,
    decision: ReturnType<typeof decideRescueSecondLegOrder>,
    evaluation: EarlyCatastrophicPairLockEvaluation,
    now: Date,
    accepted: boolean,
  ): ProtectiveDeRiskAudit {
    const unhedgedStartedAt = position.unhedgedStartedAt ?? position.openedAt;
    const unhedgedDurationMs = Math.max(0, now.getTime() - Date.parse(unhedgedStartedAt));
    // residualExposure is a DOLLAR value (market-price exposure), not shares.
    const averageHeldCost = position.unpairedOutcome === "UP" ? position.averageUpCost : position.averageDownCost;
    const residualExposureUsd = d(position.unpairedQuantity).mul(averageHeldCost).toFixed();
    return {
      deRiskReason: "EARLY_CATASTROPHIC_PAIR_LOCK",
      eligibilityReason: "EARLY_CATASTROPHIC_PAIR_LOCK",
      unhedgedStartedAt,
      unhedgedDurationMs,
      attemptedAction: accepted ? "RESCUE_SECOND_LEG" : "NONE",
      requestedQty: position.unpairedQuantity,
      executableQty: accepted ? (decision.order?.quantity ?? "0") : "0",
      executionPrice: accepted ? (decision.bookVwap ?? null) : null,
      residualQuantity: position.unpairedQuantity,
      residualExposure: position.unpairedOutcome ? residualExposureUsd : "0",
      retryCount: (this.protectiveDeRiskAttempts.get(marketId) ?? 0) + 1,
      finalState: accepted ? "PRECHECK_ACCEPTED" : "RISK_VETO",
      selection: "RESCUE_SECOND_LEG",
      selectionReason: "EARLY_CATASTROPHIC_PAIR_LOCK",
      tieBreakUsed: false,
      aAvailable: false,
      bAvailable: accepted,
      bReason: accepted ? null : (decision.risk.vetoes[0] ?? "RISK_VETO"),
      bQuantity: accepted ? (decision.order?.quantity ?? null) : null,
      bVwap: accepted ? decision.bookVwap : null,
      bFeeUsd: accepted ? decision.feeUsdTotal : null,
      bRequiredCashUsd: accepted ? (decision.requiredCashUsd ?? null) : null,
      bGuaranteedSalvageUsd: accepted ? decision.guaranteedSalvageUsd : null,
    };
  }

  private async persistEarlyEvaluation(
    marketId: string,
    decision: ReturnType<typeof decideRescueSecondLegOrder>,
    evaluation: EarlyCatastrophicPairLockEvaluation,
    now: Date,
    asset: string | null,
    accepted: boolean,
  ): Promise<void> {
    const audit = this.buildEarlyAudit(marketId, this.ledger.positions.get(marketId)!, decision, evaluation, now, accepted);
    await this.persistReportInput(createJournalEvent({
      traceId: decision.risk.traceId,
      type: "RISK_EVALUATION",
      aggregateType: "risk",
      aggregateId: decision.risk.id,
      payload: {
        ...decision.risk,
        marketId,
        asset,
        intent: "RESCUE_SECOND_LEG",
        earlyCatastrophic: {
          markLossUsd: evaluation.markLossUsd,
          guaranteedLockedPnl: evaluation.guaranteedLockedPnl,
          rescueNotionalUsd: evaluation.rescueNotionalUsd,
          rescueVwap: evaluation.rescueVwap,
        },
        deRisk: audit,
      },
    }));
  }

  private recordEarlyCatastrophicSuppressed(
    marketId: string,
    evaluation: EarlyCatastrophicPairLockEvaluation,
    now: Date,
    extra: Record<string, unknown> = {},
  ): void {
    if (!evaluation.reason) return;
    this.recordProtectiveDeRiskSuppressed(marketId, "EARLY_CATASTROPHIC_PAIR_LOCK", evaluation.reason, {
      position: this.ledger.positions.get(marketId) ?? null,
      unhedgedDurationMs: null,
      extra: {
        markLossUsd: evaluation.markLossUsd,
        guaranteedLockedPnl: evaluation.guaranteedLockedPnl,
        rescueNotionalUsd: evaluation.rescueNotionalUsd,
        rescueVwap: evaluation.rescueVwap,
        hedgeAvailable: evaluation.hedgeAvailable,
        ...extra,
      },
    });
  }
  
  /** Only a durable, actual rescue execution failure for this exact residual
   * episode can override an otherwise affordable PR3 opportunity. */
  private rescueExecutionFailure(position: Position): RiskReductionExecutionFailure | null {
    const failure = this.riskReductionFailures.get(position.marketId);
    const owner = failure?.ownership;
    return owner?.intent === "RESCUE_SECOND_LEG"
      && owner.marketId === position.marketId
      && owner.unhedgedStartedAt === unhedgedEpisode(position)
      && d(owner.admittedResidualQuantity).eq(position.unpairedQuantity)
      ? failure! : null;
  }

  /** The sole Guardian policy. Synchronous and validation-only; also called
   * inside PR87's serialized admission boundary after all decision awaits. */
  private evaluateGuardian(marketId: string, persistedOrder?: PaperOrder): {
    reason: string; markLossUsd: string | null; rescueFailureOrderId: string | null;
    decision: ReturnType<typeof decideProtectivePaperOrder> | null;
    held: MarketSnapshot | null; position: Position | null;
  } {
    const position = this.ledger.positions.get(marketId) ?? null;
    const result = { reason: "NO_UNPAIRED_RESIDUAL", markLossUsd: null as string | null,
      rescueFailureOrderId: null as string | null, decision: null as ReturnType<typeof decideProtectivePaperOrder> | null,
      held: null as MarketSnapshot | null, position };
    if (!position?.unpairedOutcome || !d(position.unpairedQuantity).gt(0)) return result;
    if (this.env.TRADING_MODE !== "paper" || !experimentAllowsHedge(this.experiment)
      || this.state.database !== "UP" || this.state.journalDegraded || this.overload.hasIntegrityFault
      || this.overload.state === "FAIL_CLOSED" || this.state.draining) return { ...result, reason: "HARD_AUTHORITY_UNAVAILABLE" };
    const confirmed = this.confirmedGuardianProtection(marketId);
    if (this.settledMarkets.has(marketId) || (!confirmed && !this.earlyAuthority(marketId))) return { ...result, reason: "DEADLINE_OR_SETTLEMENT_AUTHORITY" };
    if (this.riskReductionOwners.has(marketId)) return { ...result, reason: "TAKER_ALREADY_OWNS_RESIDUAL" };
    const now = this.nowFn();
    if (!confirmed && now.getTime() - Date.parse(unhedgedEpisode(position)) < this.riskLimits.catastrophicPairLockMinUnhedgedMs) {
      return { ...result, reason: "UNHEDGED_AGE_NOT_REACHED" };
    }
    const pair = this.snapshots.get(marketId);
    const fresh = { UP: this.freshEarlyCatastrophicSnapshot(pair?.UP ?? null, now.getTime()),
      DOWN: this.freshEarlyCatastrophicSnapshot(pair?.DOWN ?? null, now.getTime()) };
    const held = fresh[position.unpairedOutcome];
    const oppositeOutcome: Outcome = position.unpairedOutcome === "UP" ? "DOWN" : "UP";
    const opposite = fresh[oppositeOutcome];
    if (!held || held.tokenId !== this.currentTokenIdFor(marketId, position.unpairedOutcome)) return { ...result, reason: "HELD_BOOK_UNAVAILABLE" };
    result.held = held;
    result.markLossUsd = computeRealizableMarkLossUsd(position, held);
    if (!confirmed && (result.markLossUsd === null || d(result.markLossUsd).lt(this.riskLimits.catastrophicPairLockTriggerLossUsd))) {
      return { ...result, reason: "MARK_LOSS_NOT_REACHED" };
    }
    if (!confirmed && this.activeHedgeMakerOwnsResidual(marketId, oppositeOutcome)) return { ...result, reason: "HEDGE_MAKER_PRIORITY" };
    const normal = confirmed ? null : this.normalHedgeDecisionReadOnly(marketId, position, fresh, now);
    if (normal?.accepted && normal.order) return { ...result, reason: "NORMAL_HEDGE_PRIORITY" };
    const failure = this.rescueExecutionFailure(position);
    let reason: string;
    if (confirmed) {
      reason = `GUARDIAN_CONFIRMED_${confirmed.protectionReason}`;
    } else if (failure) {
      reason = "SAME_EPISODE_RESCUE_EXECUTION_FAILED";
      result.rescueFailureOrderId = failure.ownership.order.id;
    } else {
      // Unknown/stale opposite data is not proof of missing depth. A trusted
      // empty/partial ladder is. Price the FULL residual rather than PR82's
      // intentionally partial/max-notional candidate.
      if (!opposite || opposite.tokenId !== this.currentTokenIdFor(marketId, oppositeOutcome)
        || !opposite.feeSchedule || !opposite.minOrderSize) return { ...result, reason: "PR3_BOOK_UNAVAILABLE" };
      const lockWalk = walkBook(opposite.asks, position.unpairedQuantity, "1", "BUY");
      if (!lockWalk.sufficientDepth || !d(lockWalk.filledQuantity).eq(position.unpairedQuantity)) reason = "PR3_FULL_DEPTH_UNAVAILABLE";
      else {
        const pnl = computeGuaranteedLockedPnl({ position, rescueQuantity: position.unpairedQuantity,
          rescueVwap: lockWalk.vwap, rescueTakerFeeUsd: calculateFee(position.unpairedQuantity, lockWalk.vwap, "TAKER", opposite.feeSchedule) });
        if (d(pnl).gte(-this.riskLimits.catastrophicPairLockMaxLockedLossUsd)) return { ...result, reason: "PR3_PRIORITY" };
        reason = "PR3_LOCKED_LOSS_EXCEEDS_CAP";
      }
    }
    const candidate = evaluateProtectiveDeRiskCandidate({ position, snapshot: held,
      maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd }).candidate;
    if (!candidate || !d(candidate.quantity).eq(position.unpairedQuantity)) return { ...result, reason: "FULL_SELL_UNAVAILABLE" };
    candidate.intent = "GUARDIAN_PROTECTIVE_SELL";
    candidate.strategyId = "engine-guardian";
    candidate.strategyVersion = "engine-guardian-v1";
    candidate.reasonCodes = [reason, "FULL_HELD_RESIDUAL_SELL"];
    if (persistedOrder) {
      if (persistedOrder.tokenId !== held.tokenId || persistedOrder.outcome !== position.unpairedOutcome
        || persistedOrder.side !== "SELL" || persistedOrder.orderType !== "TAKER"
        || !d(persistedOrder.quantity).eq(position.unpairedQuantity)) return { ...result, reason: "GUARDIAN_RESIDUAL_CHANGED" };
      candidate.limitPrice = persistedOrder.requestedPrice;
    }
    const asset = this.assetByMarket.get(marketId) ?? held.asset ?? null;
    const exposure = computePaperExposures(this.ledger.positions.values(), marketId, this.assetByMarket, now.getTime());
    const projected = projectPaperExposures({ current: exposure,
      positions: projectableLegsFromPositions(this.ledger.positions.values(), this.assetByMarket),
      candidate: { marketId, outcome: candidate.outcome, quantity: candidate.quantity, price: candidate.limitPrice,
        side: "SELL", intent: candidate.intent, asset }, nowMs: now.getTime() });
    const ledger = this.ledger.snapshot();
    const decision = decideProtectivePaperOrder({ candidate, snapshotAfterDecisionLatency: held,
      limits: this.riskLimits, traceId: persistedOrder?.traceId ?? position.id,
      triggerId: persistedOrder?.triggerId ?? position.id,
      baseRisk: {
        now, marketFeedAt: new Date(held.receivedAt), btcFeedAt: now, referenceRequired: false,
        asset, assetExposureUsd: exposure.asset, databaseAvailable: this.state.database === "UP",
        timestampDisorder: this.timestampDisorder, riskStateAvailable: this.protectiveRiskStateAvailable(),
        dailyPnlUsd: ledger.netTradingPnl, drawdownUsd: ledger.drawdown,
        marketExposureUsd: exposure.market, totalExposureUsd: exposure.total,
        directionalExposureUsd: exposure.directional, unhedgedExposureUsd: exposure.unhedged,
        unhedgedDurationMs: exposure.unhedgedDurationMs,
        projectedMarketExposureUsd: projected.market, projectedTotalExposureUsd: projected.total,
        projectedDirectionalExposureUsd: projected.directional, projectedUnhedgedExposureUsd: projected.unhedged,
        projectedAssetExposureUsd: projected.asset, availableCashUsd: this.ledger.availableCash().toFixed(),
        externalSignalsLastMinute: 0, lastStrategyTradeAt: this.lastTradeAt, duplicateTrade: false,
        experimentEntryOpen: experimentAllowsHedge(this.experiment),
        executionErrorsLastHour: this.executionErrors.filter((at) => now.getTime() - at < 3_600_000).length,
        externalRiskRecommendedVeto: false, shuttingDown: this.state.draining,
      } });
    return { ...result, reason: decision.accepted ? reason : `RISK_VETO:${decision.risk.vetoes.join("|")}`, decision };
  }

  private async maybeGuardianProtectiveSell(marketId: string): Promise<boolean> {
    const confirmed = this.confirmedGuardianProtection(marketId);
    if (this.soakBlocksPostEntry(marketId) || (this.profitTrailingPairSoakEnabled() && !confirmed)) return false;
    if (this.riskReductionBusy(marketId)) {
      this.record("GUARDIAN_BLOCKED", marketId, marketId, { reason: "RISK_REDUCTION_IN_FLIGHT" });
      return false;
    }
    let evaluation = this.evaluateGuardian(marketId);
    let { decision, position, held } = evaluation;
    const evidence = { intent: "GUARDIAN_PROTECTIVE_SELL", eligible: decision?.accepted === true,
      reason: evaluation.reason, marketId, unhedgedStartedAt: position ? unhedgedEpisode(position) : null,
      heldTokenId: held?.tokenId ?? null, quantity: position?.unpairedQuantity ?? "0",
      markLossUsd: evaluation.markLossUsd, rescueFailureOrderId: evaluation.rescueFailureOrderId,
      orderId: decision?.order?.id ?? null };
    this.record("GUARDIAN_EVALUATION", position?.id ?? marketId, marketId, evidence);
    if (!decision?.accepted || !decision.order || !position || !held) {
      return false;
    }
    this.riskReductionInFlight.add(marketId);
    try {
      if (confirmed) {
        const started = performance.now();
        const fenced = await this.durableGate.run(async () => {
          if (!this.confirmedGuardianProtection(marketId)) return false;
          await this.cancelOutstandingFirstLegMakers(marketId, "GUARDIAN_PRIORITY");
          await this.cancelRestingHedgeMakers(marketId, "GUARDIAN_PRIORITY");
          return true;
        });
        this.logger.info?.({ stage: "GUARDIAN_MAKERS_FENCED", marketId, durationMs: performance.now() - started,
          confirmedAt: confirmed.confirmedAt }, "risk reduction milestone");
        if (!fenced) return false;
        evaluation = this.evaluateGuardian(marketId);
        ({ decision, position, held } = evaluation);
        if (!decision?.accepted || !decision.order || !position || !held) return false;
      }
      // Freeze decision episode facts before any await: PaperLedger may update
      // the existing Position object in place while diagnostic persistence waits.
      const deRisk: ProtectiveDeRiskAudit = {
        deRiskReason: confirmed ? "REVERSAL_CONFIRMED" : "GUARDIAN_PROTECTIVE_SELL", eligibilityReason: "GUARDIAN_PROTECTIVE_SELL",
        ...(confirmed ? { positionId: confirmed.positionId, heldOutcome: confirmed.heldOutcome,
          guardianSensor: confirmed.sensor!, selection: "PROTECTIVE_DE_RISK" as const, tieBreakUsed: false } : {}),
        unhedgedStartedAt: unhedgedEpisode(position), unhedgedDurationMs: this.nowFn().getTime() - Date.parse(unhedgedEpisode(position)),
        attemptedAction: "SELL_UNPAIRED", requestedQty: position.unpairedQuantity, executableQty: decision.order.quantity,
        executionPrice: decision.bookVwap, residualQuantity: position.unpairedQuantity,
        residualExposure: computePaperExposures(this.ledger.positions.values(), marketId, this.assetByMarket).unhedged,
        retryCount: 0, finalState: "PRECHECK_ACCEPTED", guardianReason: confirmed?.protectionReason ?? evaluation.reason,
        rescueFailureOrderId: evaluation.rescueFailureOrderId,
      };
      if (!confirmed) await this.persistReportInput(createJournalEvent({ traceId: decision.order.traceId,
        type: "GUARDIAN_EVALUATION", aggregateType: "position", aggregateId: position.id, payload: evidence }));
      const result = await this.submitPaperOrder(decision.order, held, `guardian:${decision.order.id}`, "GUARDIAN_PROTECTIVE_SELL", deRisk);
      this.logger.info?.({ stage: "GUARDIAN_EXECUTION_RESULT", marketId, orderId: decision.order.id, filled: result,
        remaining: this.ledger.positions.get(marketId)?.unpairedQuantity ?? "0", confirmedAt: confirmed?.confirmedAt ?? null,
        decisionToResultMs: confirmed?.confirmedAt ? this.nowFn().getTime() - Date.parse(confirmed.confirmedAt) : null }, "risk reduction milestone");
      return result;
    } finally {
      this.riskReductionInFlight.delete(marketId);
      const remaining = this.ledger.positions.get(marketId);
      if (!remaining?.unpairedOutcome || d(remaining.unpairedQuantity).lte(0)) this.clearProtectiveDeRiskState(marketId);
    }
  }

  private async maybeProtectiveDeRisk(marketId: string): Promise<void> {
    if (this.soakBlocksPostEntry(marketId)) return;
    if (this.confirmedGuardianProtection(marketId)) { await this.maybeReversalProtection(marketId); return; }
    const pending = this.riskReductionOwners.get(marketId);
    if (pending) {
      await this.resumeRiskReduction(pending);
      // A resumed order can be durably released by a deadline/state fence.
      // Callbacks received while it owned the episode already yielded; hand
      // the still-open residual to current authority in this same pass.
      if (this.riskReductionOwners.has(marketId) || this.riskReductionInFlight.has(marketId)) return;
    }
    if (this.riskReductionInFlight.has(marketId)) {
      this.recordProtectiveDeRiskSuppressed(marketId, "GATE_IN_FLIGHT", "IN_FLIGHT");
      return;
    }
    let initial = this.ledger.positions.get(marketId);
    if (!initial?.unpairedOutcome || d(initial.unpairedQuantity).lte(0)) {
      this.clearProtectiveDeRiskState(marketId);
      return;
    }
    if (this.profitTrailingRetainsAuthority(marketId, initial, this.nowFn().getTime())) return;
    if (!experimentAllowsHedge(this.experiment)) {
      this.recordProtectiveDeRiskSuppressed(marketId, "GATE_HEDGE_UNAVAILABLE", "HEDGE_UNAVAILABLE", { position: initial });
      return;
    }
    if (this.settledMarkets.has(marketId)) {
      this.recordProtectiveDeRiskSuppressed(marketId, "GATE_SETTLED_MARKET", "SETTLED_MARKET", { position: initial });
      return;
    }
    // Economic risk reduction still requires healthy durability authority.
    // Fail-closed/integrity/journal faults must never be bypassed by calling an
    // exit "protective".
    if (this.state.database !== "UP") {
      this.recordProtectiveDeRiskSuppressed(marketId, "GATE_HARD_AUTHORITY", "DATABASE_UNAVAILABLE", { position: initial });
      return;
    }
    if (this.state.journalDegraded) {
      this.recordProtectiveDeRiskSuppressed(marketId, "GATE_HARD_AUTHORITY", "JOURNAL_DEGRADED", { position: initial });
      return;
    }
    if (this.overload.hasIntegrityFault || this.overload.state === "FAIL_CLOSED") {
      this.recordProtectiveDeRiskSuppressed(marketId, "GATE_HARD_AUTHORITY", "INTEGRITY_OR_FAIL_CLOSED", { position: initial });
      return;
    }

    // PR3 PRE-DEADLINE ownership: the early catastrophic pair-lock runs ONLY
    // BEFORE the old protective deadline. At/after the deadline the PR82 A/B
    // fallback exclusively owns the position (B-only never steals the
    // fallback). Pre-deadline priority: normal profitable hedge -> early
    // catastrophic full pair-lock -> Guardian full held SELL -> WAIT (deadline fallback later).
    let initialNow = this.nowFn();
    let initialStartedAt = initial.unhedgedStartedAt ?? initial.openedAt;
    let initialDurationMs = Math.max(0, initialNow.getTime() - Date.parse(initialStartedAt));
    let initialEligibility = this.protectiveDeRiskEligibility(marketId, initialStartedAt, initialNow.getTime());
    if (!initialEligibility.eligible) {
      const residualBeforeEarly = initial.unpairedQuantity;
      const earlyResult = await this.maybeEarlyCatastrophicPairLock(marketId);
      if (earlyResult.acted) {
        const remaining = this.ledger.positions.get(marketId);
        if (!remaining?.unpairedOutcome || this.riskReductionOwners.has(marketId)
          || !d(remaining.unpairedQuantity).eq(residualBeforeEarly)) return;
        // Order creation did not consume the residual: execution/fence failure
        // released its shared owner. Revalidate Guardian or deadline authority
        // in this same pass, without waiting for another book callback.
      }
      if (await this.maybeGuardianProtectiveSell(marketId)) return;
      // A book callback arriving during Guardian's admission await could make
      // NORMAL/PR3 eligible while that callback yields to the shared flight.
      // Recheck priority once in this same pass, avoiding a lost wake-up.
      const priorityRecheck = await this.maybeEarlyCatastrophicPairLock(marketId);
      const afterPriority = this.ledger.positions.get(marketId);
      if (priorityRecheck.acted || this.riskReductionBusy(marketId) || !afterPriority?.unpairedOutcome
        || !d(afterPriority.unpairedQuantity).eq(residualBeforeEarly)) return;
      // This bounded retry can itself lose admission while callbacks yield to
      // its flight. Give current Guardian authority one final same-pass check.
      if (priorityRecheck.acceptedWithoutAdmission && await this.maybeGuardianProtectiveSell(marketId)) return;
      // Re-read durable state AFTER the early phase: the deadline may have been
      // crossed during an early await. Authority handoff must occur in the SAME
      // pass (Section 4) — no extra snapshot/tick required.
      const afterEarly = this.ledger.positions.get(marketId);
      if (!afterEarly?.unpairedOutcome || d(afterEarly.unpairedQuantity).lte(0)) {
        this.clearProtectiveDeRiskState(marketId);
        return;
      }
      if (!experimentAllowsHedge(this.experiment)) {
        this.recordProtectiveDeRiskSuppressed(marketId, "GATE_HEDGE_UNAVAILABLE", "HEDGE_UNAVAILABLE", { position: afterEarly });
        return;
      }
      if (this.settledMarkets.has(marketId)) {
        this.recordProtectiveDeRiskSuppressed(marketId, "GATE_SETTLED_MARKET", "SETTLED_MARKET", { position: afterEarly });
        return;
      }
      initial = afterEarly;
      initialNow = this.nowFn();
      initialStartedAt = initial.unhedgedStartedAt ?? initial.openedAt;
      initialDurationMs = Math.max(0, initialNow.getTime() - Date.parse(initialStartedAt));
      initialEligibility = this.protectiveDeRiskEligibility(marketId, initialStartedAt, initialNow.getTime());
      if (!initialEligibility.eligible) {
        const initialMarketExpired = initialEligibility.marketEndMs != null && initialNow.getTime() >= initialEligibility.marketEndMs;
        this.recordProtectiveDeRiskSuppressed(
          marketId, "GATE_DURATION", initialMarketExpired ? "MARKET_EXPIRED" : "UNHEDGED_DURATION_NOT_EXCEEDED",
          {
            position: initial,
            unhedgedDurationMs: Number.isFinite(initialDurationMs) ? initialDurationMs : null,
            extra: {
              effectiveProtectiveDeadlineMs: initialEligibility.effectiveDeadlineMs,
              marketEndMs: initialEligibility.marketEndMs,
            },
          },
        );
        return;
      }
      // Deadline crossed during the early phase -> PR82 fallback owns the
      // position in this same pass. Early backoff must never delay it.
      this.earlyCatastrophicNextAttemptAt.delete(marketId);
      this.earlyCatastrophicVetoFingerprint.delete(marketId);
    }

    await this.executeRiskReductionArbitration(marketId);
  }

  /** Shared PR2 executor. Reversal is an explicit episode-scoped authority. */
  private async executeRiskReductionArbitration(marketId: string, reversal?: ReversalProtectionState): Promise<void> {
    if (this.riskReductionBusy(marketId)) return;
    if (reversal && this.nowFn().getTime() < (this.protectiveDeRiskNextAttemptAt.get(marketId) ?? 0)) return;
    const reason = reversal ? "REVERSAL_CONFIRMED" : "MAX_UNHEDGED_DURATION";
    this.riskReductionInFlight.add(marketId);
    try {
      // Quote/risk plans are immutable after durable ownership. Before admission
      // only, retry at most twice when an async audit/gate invalidated the plan.
      for (let quotePass = 0; quotePass < 3; quotePass++) {
      // Fence older entry/hedge makers before A/B. For reversal, cancellation
      // and episode validation share the mutation gate, so an old confirmation
      // cannot cancel orders belonging to a replacement episode.
      if (reversal) {
        const fenced = await this.durableGate.run(async () => {
          if (!this.reversalEligibility(reversal, this.ledger.positions.get(marketId), this.nowFn().getTime()).eligible) return false;
          await this.cancelOutstandingFirstLegMakers(marketId, reason);
          await this.cancelRestingHedgeMakers(marketId, reason);
          return true;
        });
        if (!fenced) return;
      } else {
        await this.cancelOutstandingFirstLegMakers(marketId, reason);
        await this.cancelRestingHedgeMakers(marketId, "RESCUE_ELIGIBLE");
      }

      const position = this.ledger.positions.get(marketId);
      if (!position?.unpairedOutcome || d(position.unpairedQuantity).lte(0)) {
        this.clearProtectiveDeRiskState(marketId);
        this.recordProtectiveDeRiskSuppressed(marketId, "GATE_POSITION_CHANGED", "POSITION_CHANGED");
        return;
      }
      if (!experimentAllowsHedge(this.experiment)) {
        this.recordProtectiveDeRiskSuppressed(marketId, "GATE_HEDGE_UNAVAILABLE_AFTER_CANCEL", "HEDGE_UNAVAILABLE", { position });
        return;
      }
      if (this.settledMarkets.has(marketId)) {
        this.recordProtectiveDeRiskSuppressed(marketId, "GATE_SETTLED_AFTER_CANCEL", "SETTLED_MARKET", { position });
        return;
      }
      if (this.state.draining) {
        this.recordProtectiveDeRiskSuppressed(marketId, "GATE_DRAINING", "DRAINING", { position });
        return;
      }

      const now = this.nowFn();
      const unhedgedStartedAt = position.unhedgedStartedAt ?? position.openedAt;
      const unhedgedDurationMs = Math.max(0, now.getTime() - Date.parse(unhedgedStartedAt));
      const eligibility = reversal
        ? this.reversalEligibility(reversal, position, now.getTime())
        : this.protectiveDeRiskEligibility(marketId, unhedgedStartedAt, now.getTime());
      if (!eligibility.eligible) {
        const marketExpired = eligibility.marketEndMs != null && now.getTime() >= eligibility.marketEndMs;
        this.recordProtectiveDeRiskSuppressed(
          marketId, "GATE_DURATION_CHANGED", marketExpired ? "MARKET_EXPIRED" : "UNHEDGED_DURATION_NOT_EXCEEDED",
          {
            position, unhedgedDurationMs: Number.isFinite(unhedgedDurationMs) ? unhedgedDurationMs : null,
            extra: { effectiveProtectiveDeadlineMs: eligibility.effectiveDeadlineMs, marketEndMs: eligibility.marketEndMs },
          },
        );
        return;
      }

      const pair = this.snapshots.get(marketId);
      const rawHeld = pair?.[position.unpairedOutcome]
        ?? [...this.state.marketSnapshots.values()].find((item) => item.marketId === marketId && item.outcome === position.unpairedOutcome)
        ?? null;
      const snapshot = reversal ? this.freshEarlyCatastrophicSnapshot(rawHeld, now.getTime()) : rawHeld;
      if (!snapshot || (reversal && snapshot.tokenId !== this.currentTokenIdFor(marketId, position.unpairedOutcome))) {
        this.recordProtectiveDeRiskSuppressed(marketId, "GATE_SNAPSHOT_MISSING", "SNAPSHOT_MISSING", { position });
        return;
      }
      if (!this.adapterTokenSynchronized(snapshot.tokenId)) {
        this.recordProtectiveDeRiskSuppressed(marketId, "GATE_TOKEN_UNSYNCHRONIZED", "TOKEN_UNSYNCHRONIZED", { position, snapshot });
        return;
      }

      const receivedAtMs = Date.parse(snapshot.receivedAt);
      if (!Number.isFinite(receivedAtMs) || now.getTime() - receivedAtMs > this.riskLimits.staleFeedTimeoutMs) {
        this.recordProtectiveDeRiskSuppressed(marketId, "GATE_SNAPSHOT_STALE", "SNAPSHOT_STALE", { position, snapshot, unhedgedDurationMs });
        return;
      }

      // ---- PR2 rescue arbitration: build BOTH candidates from the SAME
      // rescue observation state (A = protective SELL, B = pair-lock BUY). ----
      const oppositeOutcome: Outcome = position.unpairedOutcome === "UP" ? "DOWN" : "UP";
      let oppositeSnapshot = this.snapshots.get(marketId)?.[oppositeOutcome]
        ?? [...this.state.marketSnapshots.values()].find((item) => item.marketId === marketId && item.outcome === oppositeOutcome)
        ?? null;
      if (oppositeSnapshot && !this.adapterTokenSynchronized(oppositeSnapshot.tokenId)) {
        // B is unavailable from an unsynchronized book; the arbiter may still
        // choose A. Never price B from a stale/unsynced opposite book.
        oppositeSnapshot = null;
      }
      if (oppositeSnapshot) {
        // Coherent comparison (Â§16): B must be priced from a FRESH opposite
        // book from the SAME observation window. A stale/absent/timestamp-
        // disordered opposite ladder makes B unavailable (fail closed) instead
        // of comparing A@fresh against B@stale.
        const oppositeReceivedAtMs = Date.parse(oppositeSnapshot.receivedAt);
        const oppositeAgeMs = now.getTime() - oppositeReceivedAtMs;
        if (!Number.isFinite(oppositeReceivedAtMs) || oppositeAgeMs > this.riskLimits.staleFeedTimeoutMs) {
          oppositeSnapshot = null;
        }
      }
      const aEval = evaluateProtectiveDeRiskCandidate({
        position,
        snapshot,
        maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd,
      });
      const bEval: RescueCandidateEvaluation = oppositeSnapshot
        ? evaluateRescueSecondLegCandidate({
          position,
          snapshot: oppositeSnapshot,
          maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd,
        })
        : { candidate: null, reason: "NO_EXECUTABLE_ASK_DEPTH" };
      // V1 only neutralizes the full residual; canonical partial candidates wait.
      let aReason: string | null = aEval.reason;
      let bReason: string | null = bEval.reason;
      if (reversal) {
        oppositeSnapshot = this.freshEarlyCatastrophicSnapshot(oppositeSnapshot, now.getTime());
        for (const evaluation of [aEval, bEval]) {
          if (evaluation.candidate && !d(evaluation.candidate.quantity).eq(position.unpairedQuantity)) {
            if (evaluation === aEval) aReason = "FULL_RESIDUAL_UNAVAILABLE";
            else bReason = "FULL_RESIDUAL_UNAVAILABLE";
            evaluation.candidate = null;
          }
          if (evaluation.candidate) evaluation.candidate.reasonCodes = evaluation.candidate.reasonCodes
            .map((code) => code === "MAX_UNHEDGED_DURATION" ? reason : code);
        }
        if (!oppositeSnapshot) { bEval.candidate = null; bReason = "OPPOSITE_BOOK_UNAVAILABLE"; }
      }

      if (!aEval.candidate && !bEval.candidate) {
        // A-oriented diagnostic contract is preserved: when both are null the
        // suppression reason keeps the unpaired-book reason (the old single
        // candidate semantics); the opposite-book reason stays observable in
        // extras for the rescue arbiter.
        const rejectionReason = aReason ?? bReason ?? "NO_EXECUTABLE_DEPTH";
        if (reversal) {
          this.protectiveDeRiskNextAttemptAt.set(marketId, now.getTime() + PROTECTIVE_DE_RISK_RETRY_MS);
          await this.persistReportInput(createJournalEvent({ traceId: position.id, type: "RISK_EVALUATION",
            aggregateType: "risk", aggregateId: createId("risk"), occurredAt: now,
            payload: { marketId, accepted: false, vetoes: [rejectionReason], intent: "NONE",
              deRisk: { deRiskReason: reason, positionId: reversal.positionId, unhedgedStartedAt,
                heldOutcome: reversal.heldOutcome,
                ...(isGuardianV2State(reversal) ? { guardianSensor: reversal.sensor, guardianReason: reversal.protectionReason } : {}), requestedQty: position.unpairedQuantity, executableQty: "0",
                selection: "NONE", selectionReason: "NEITHER_ACCEPTED", tieBreakUsed: false,
                aAvailable: false, aReason, bAvailable: false, bReason } } }));
        }
        const lastWaitingAt = this.protectiveDeRiskLastWaitingAt.get(marketId) ?? Number.NEGATIVE_INFINITY;
        if (now.getTime() - lastWaitingAt >= PROTECTIVE_DE_RISK_WAIT_SAMPLE_MS) {
          this.protectiveDeRiskLastWaitingAt.set(marketId, now.getTime());
          this.record("PROTECTIVE_DE_RISK_WAITING", position.id, marketId, {
            deRiskReason: reason,
            unhedgedStartedAt,
            unhedgedDurationMs,
            attemptedAction: "NONE",
            requestedQty: position.unpairedQuantity,
            executableQty: "0",
            finalState: rejectionReason,
            selection: "NONE",
            aReason: aEval.reason,
            bReason: bEval.reason,
          }, this.assetByMarket.get(marketId) ?? snapshot.asset ?? null);
        }
        this.recordProtectiveDeRiskSuppressed(
          marketId, "GATE_CANDIDATE_NULL", rejectionReason,
          { position, snapshot, unhedgedDurationMs, extra: { aReason: aEval.reason, bReason: bEval.reason } },
        );
        return;
      }

      const executionErrorsLastHour = this.executionErrors.filter((at) => now.getTime() - at < 3_600_000).length;
      const hardStateFingerprint = [
        snapshot.feeSchedule ? "fee:known" : "fee:unknown",
        this.timestampDisorder ? "timestamp:disordered" : "timestamp:ordered",
        executionErrorsLastHour >= this.riskLimits.maxExecutionErrorsPerHour ? "execution:blocked" : "execution:healthy",
      ].join("|");
      const nextAttemptAt = this.protectiveDeRiskNextAttemptAt.get(marketId) ?? 0;
      if (quotePass === 0 && now.getTime() < nextAttemptAt) {
        const vetoFingerprint = this.protectiveDeRiskVetoFingerprint.get(marketId);
        // A normal execution retry keeps its short cooldown. A hard-veto
        // backoff may be bypassed only when the blocker state materially changed.
        if (vetoFingerprint == null || vetoFingerprint === hardStateFingerprint) {
          this.recordProtectiveDeRiskSuppressed(
            marketId, "GATE_RETRY_BACKOFF", "RETRY_BACKOFF",
            {
              position, snapshot, candidate: aEval.candidate, unhedgedDurationMs,
              extra: { retryNextAttemptAtMs: nextAttemptAt, vetoFingerprint },
            },
          );
          return;
        }
        this.protectiveDeRiskVetoFingerprint.delete(marketId);
      }
      // Reserve the retry slot before any journal await so fresh snapshot
      // bursts cannot generate duplicate durable evaluations behind this flight.
      this.protectiveDeRiskNextAttemptAt.set(marketId, now.getTime() + PROTECTIVE_DE_RISK_RETRY_MS);

      const asset = this.assetByMarket.get(marketId) ?? snapshot.asset ?? null;
      const exposure = computePaperExposures(this.ledger.positions.values(), marketId, this.assetByMarket, now.getTime());
      const projectedA = aEval.candidate
        ? projectPaperExposures({
          current: exposure,
          positions: projectableLegsFromPositions(this.ledger.positions.values(), this.assetByMarket),
          candidate: {
            marketId,
            outcome: snapshot.outcome,
            quantity: aEval.candidate.quantity,
            price: aEval.candidate.limitPrice,
            side: "SELL",
            intent: "PROTECTIVE_DE_RISK",
            asset,
          },
          nowMs: now.getTime(),
        })
        : null;
      const projectedB = bEval.candidate && oppositeSnapshot
        ? projectPaperExposures({
          current: exposure,
          positions: projectableLegsFromPositions(this.ledger.positions.values(), this.assetByMarket),
          candidate: {
            marketId,
            outcome: oppositeOutcome,
            quantity: bEval.candidate.quantity,
            price: bEval.candidate.limitPrice,
            side: "BUY",
            intent: "RESCUE_SECOND_LEG",
            asset,
          },
          nowMs: now.getTime(),
        })
        : null;
      const ledger = this.ledger.snapshot();
      const bookFacts = (book: MarketSnapshot | undefined) => book ? {
        marketId: book.marketId, tokenId: book.tokenId, outcome: book.outcome,
        bids: book.bids, asks: book.asks, tickSize: book.tickSize, minOrderSize: book.minOrderSize, feeSchedule: book.feeSchedule,
        fresh: !!this.freshEarlyCatastrophicSnapshot(book, this.nowFn().getTime())
          && Number.isFinite(Date.parse(book.sourceTimestamp)) && Date.parse(book.sourceTimestamp) <= this.nowFn().getTime()
          && this.nowFn().getTime() - Date.parse(book.sourceTimestamp) <= this.riskLimits.staleFeedTimeoutMs,
      } : null;
      // Transport heartbeats/sequence changes with identical economic books
      // must not exhaust retries. Freshness availability remains part of facts.
      const riskFacts = () => {
        const currentNow = this.nowFn().getTime(), currentLedger = this.ledger.snapshot();
        const currentPosition = this.ledger.positions.get(marketId);
        const currentExposure = computePaperExposures(this.ledger.positions.values(), marketId, this.assetByMarket, currentNow);
        return JSON.stringify({ pair: {
          UP: bookFacts(this.snapshots.get(marketId)?.UP), DOWN: bookFacts(this.snapshots.get(marketId)?.DOWN),
        }, position: currentPosition ? { ...currentPosition, updatedAt: undefined } : null,
          // Global quantity/cost exposures and available cash can change the
          // two candidates' risk verdicts. Other markets' marks/audit metadata
          // cannot, unless an existing risk threshold is actually crossed.
          exposure: { ...currentExposure, unhedgedDurationMs: undefined },
          cash: this.ledger.availableCash().toFixed(),
          dailyLossBlocked: d(currentLedger.netTradingPnl).lte(d(this.riskLimits.maxDailyPaperLossUsd).negated()),
          drawdownBlocked: d(currentLedger.drawdown).gte(this.riskLimits.maxDrawdownUsd),
          executionBlocked: this.executionErrors.filter(at => currentNow - at < 3_600_000).length >= this.riskLimits.maxExecutionErrorsPerHour,
          database: this.state.database, draining: this.state.draining, integrity: this.protectiveRiskStateAvailable(),
          timestampDisorder: this.timestampDisorder, synchronized: this.isMarketSynchronized(marketId),
          upToken: this.currentTokenIdFor(marketId, "UP"), downToken: this.currentTokenIdFor(marketId, "DOWN"),
          experiment: this.experiment?.status });
      };
      const quotedFacts = riskFacts();
      const quoteCurrent = () => {
        const currentNow = this.nowFn().getTime();
        const currentPair = this.snapshots.get(marketId);
        return quotedFacts === riskFacts() && bookFacts(currentPair?.[position.unpairedOutcome!])?.fresh === true
          && (!oppositeSnapshot || bookFacts(currentPair?.[oppositeOutcome])?.fresh === true)
          && (eligibility.marketEndMs === null || currentNow < eligibility.marketEndMs);
      };
      const rescuePosition: HedgePositionFacts = {
        marketId: position.marketId,
        unpairedOutcome: position.unpairedOutcome,
        unpairedQuantity: position.unpairedQuantity,
        expectedOppositeTokenId: oppositeSnapshot ? this.currentTokenIdFor(marketId, oppositeOutcome) : null,
      };
      // Both candidates are priced from the SAME observation state: same now,
      // same exposure snapshot, same ledger facts. Only the projected exposure
      // differs per candidate (SELL vs BUY for the correct projected position).
      const baseRiskCommon = {
        now,
        marketFeedAt: new Date(snapshot.receivedAt),
        btcFeedAt: now,
        referenceFeedAt: null,
        referenceRequired: false,
        asset,
        assetExposureUsd: exposure.asset,
        databaseAvailable: this.state.database === "UP",
        timestampDisorder: this.timestampDisorder,
        riskStateAvailable: this.protectiveRiskStateAvailable(),
        dailyPnlUsd: ledger.netTradingPnl,
        drawdownUsd: ledger.drawdown,
        marketExposureUsd: exposure.market,
        totalExposureUsd: exposure.total,
        directionalExposureUsd: exposure.directional,
        unhedgedExposureUsd: exposure.unhedged,
        unhedgedDurationMs: exposure.unhedgedDurationMs,
        availableCashUsd: this.ledger.availableCash().toFixed(),
        referenceProviderBlocked: false,
        marketEntryQualified: this.entryQualified.has(marketId),
        externalSignalsLastMinute: 0,
        lastStrategyTradeAt: this.lastTradeAt,
        duplicateTrade: false,
        experimentEntryOpen: experimentAllowsHedge(this.experiment),
        executionErrorsLastHour,
        externalRiskRecommendedVeto: false,
        shuttingDown: this.state.draining,
      };
      const baseRiskA = {
        ...baseRiskCommon,
        projectedMarketExposureUsd: projectedA?.market,
        projectedTotalExposureUsd: projectedA?.total,
        projectedDirectionalExposureUsd: projectedA?.directional,
        projectedUnhedgedExposureUsd: projectedA?.unhedged,
        projectedAssetExposureUsd: projectedA?.asset,
      };
        const baseRiskB = {
        ...baseRiskCommon,
        projectedMarketExposureUsd: projectedB?.market,
        projectedTotalExposureUsd: projectedB?.total,
        projectedDirectionalExposureUsd: projectedB?.directional,
        projectedUnhedgedExposureUsd: projectedB?.unhedged,
        projectedAssetExposureUsd: projectedB?.asset,
      };
      const retryCount = (this.protectiveDeRiskAttempts.get(marketId) ?? 0) + 1;
      this.protectiveDeRiskAttempts.set(marketId, retryCount);
      const traceId = createId("derisk");
      const triggerId = createId("derisk-trigger");

      // Evaluate A and B from the SAME rescue observation state (no compare
      // across different book timestamps). VWAP-derived guaranteed salvage is
      // the only economic comparison input â€” the first-leg entry price is sunk.
      let aDecision: ReturnType<typeof decideProtectivePaperOrder> | null = null;
      let bDecision: ReturnType<typeof decideRescueSecondLegOrder> | null = null;
      let aDecisionReason: string | null = aReason;
      let bDecisionReason: string | null = bReason;
      if (aEval.candidate && projectedA) {
        // The evaluated decision (with its risk/veto details) is kept even when
        // rejected so the RISK_EVALUATION payload preserves the old protective
        // veto observability contract (accepted=false, vetoes=...).
        const evaluated = decideProtectivePaperOrder({
          candidate: aEval.candidate,
          snapshotAfterDecisionLatency: snapshot,
          limits: this.riskLimits,
          traceId,
          triggerId,
          baseRisk: baseRiskA,
        });
        aDecision = evaluated;
        if (!evaluated.accepted) aDecisionReason = evaluated.risk.vetoes[0] ?? "RISK_VETO";
      } else {
        aDecisionReason = aReason ?? "NO_EXECUTABLE_BID_DEPTH";
      }
      if (bEval.candidate && oppositeSnapshot && projectedB) {
        if (!rescuePosition.expectedOppositeTokenId || bEval.candidate.tokenId !== rescuePosition.expectedOppositeTokenId) {
          bDecisionReason = "OPPOSITE_TOKEN_MISMATCH";
        } else {
          const evaluated = decideRescueSecondLegOrder({
            candidate: bEval.candidate,
            snapshotAfterDecisionLatency: oppositeSnapshot,
            limits: this.riskLimits,
            traceId,
            triggerId,
            baseRisk: baseRiskB,
            rescuePosition,
          });
          bDecision = evaluated;
          if (!evaluated.accepted) bDecisionReason = evaluated.risk.vetoes[0] ?? "RISK_VETO";
        }
      } else {
        bDecisionReason = bReason ?? "NO_EXECUTABLE_ASK_DEPTH";
      }

      const aEconomics: RiskReductionCandidateEconomics = {
        kind: "PROTECTIVE_DE_RISK",
        accepted: aDecision?.accepted === true,
        rejectionReason: aDecisionReason,
        quantity: aDecision?.order?.quantity ?? null,
        vwap: aDecision?.bookVwap ?? null,
        feeUsd: aDecision?.feeUsdTotal ?? null,
        guaranteedSalvageUsd: aDecision?.guaranteedSalvageUsd ?? null,
      };
      const bEconomics: RiskReductionCandidateEconomics = {
        kind: "RESCUE_SECOND_LEG",
        accepted: bDecision?.accepted === true,
        rejectionReason: bDecisionReason,
        quantity: bDecision?.order?.quantity ?? null,
        vwap: bDecision?.bookVwap ?? null,
        feeUsd: bDecision?.feeUsdTotal ?? null,
        guaranteedSalvageUsd: bDecision?.guaranteedSalvageUsd ?? null,
      };
      const arbitration = arbitrateRiskReduction(aEconomics, bEconomics);
      const selectedIntent: "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG" | null =
        arbitration.selection === "NONE" ? null : arbitration.selection;
      const selectedDecision = selectedIntent === "RESCUE_SECOND_LEG" ? bDecision : aDecision;
      this.state.latestRisk = selectedDecision?.risk ?? null;
      const eligibilityReason: RiskReductionUnhedgedReason = reversal ? "REVERSAL_CONFIRMED" : this.deRiskEligibilityReason(eligibility, unhedgedStartedAt);

      const residualQuantity = selectedDecision?.order
        ? d(position.unpairedQuantity).minus(selectedDecision.order.quantity)
        : d(position.unpairedQuantity);
      const audit: ProtectiveDeRiskAudit = {
        deRiskReason: reason,
        ...(reversal ? { positionId: reversal.positionId, heldOutcome: reversal.heldOutcome,
          ...(isGuardianV2State(reversal) ? { guardianSensor: reversal.sensor!, guardianReason: reversal.protectionReason! } : {}) } : {}),
        eligibilityReason,
        unhedgedStartedAt,
        unhedgedDurationMs,
        attemptedAction: selectedIntent === "RESCUE_SECOND_LEG" ? "RESCUE_SECOND_LEG" : selectedIntent === "PROTECTIVE_DE_RISK" ? "SELL_UNPAIRED" : "NONE",
        requestedQty: position.unpairedQuantity,
        executableQty: selectedDecision?.order?.quantity ?? "0",
        executionPrice: selectedDecision?.bookVwap ?? null,
        residualQuantity: residualQuantity.gt(0) ? residualQuantity.toFixed() : "0",
        residualExposure: (selectedIntent === "RESCUE_SECOND_LEG" ? projectedB?.unhedged : projectedA?.unhedged) ?? exposure.unhedged,
        retryCount,
        finalState: selectedDecision?.order ? "PRECHECK_ACCEPTED" : "RISK_VETO",
        selection: arbitration.selection,
        selectionReason: arbitration.selectionReason,
        tieBreakUsed: arbitration.tieBreakUsed,
        aAvailable: aDecision?.accepted === true,
        aReason: aDecisionReason,
        aQuantity: aDecision?.order?.quantity ?? null,
        aVwap: aDecision?.bookVwap ?? null,
        aFeeUsd: aDecision?.feeUsdTotal ?? null,
        aGuaranteedSalvageUsd: aDecision?.guaranteedSalvageUsd ?? null,
        bAvailable: bDecision?.accepted === true,
        bReason: bDecisionReason,
        bQuantity: bDecision?.order?.quantity ?? null,
        bVwap: bDecision?.bookVwap ?? null,
        bFeeUsd: bDecision?.feeUsdTotal ?? null,
        bRequiredCashUsd: bDecision?.requiredCashUsd ?? null,
        bGuaranteedSalvageUsd: bDecision?.guaranteedSalvageUsd ?? null,
      };

      if (selectedDecision?.order) this.logger.info?.({ stage: "ACTION_SELECTED", marketId, orderId: selectedDecision.order.id,
        positionId: position.id, unhedgedStartedAt, revision: reversal?.revision, selection: selectedIntent,
        createdAt: selectedDecision.order.createdAt, sourceTimestamp: snapshot.sourceTimestamp, receivedAt: snapshot.receivedAt,
        residual: position.unpairedQuantity, quotePass, localAt: performance.now() }, "risk reduction milestone");

      await this.persistReportInput(createJournalEvent({
        traceId,
        type: "RISK_EVALUATION",
        aggregateType: "risk",
        aggregateId: selectedDecision?.risk.id ?? (aDecision?.risk.id ?? bDecision?.risk.id ?? createId("risk")),
        payload: {
          ...(selectedDecision?.risk ?? aDecision?.risk ?? bDecision?.risk ?? {}),
          marketId,
          asset,
          intent: selectedIntent
            ?? (aDecision ? "PROTECTIVE_DE_RISK" : (bDecision ? "RESCUE_SECOND_LEG" : "NONE")),
          deRisk: audit,
        },
      }));
      if (!selectedDecision || !selectedDecision.order) {
        this.protectiveDeRiskVetoFingerprint.set(marketId, hardStateFingerprint);
        this.protectiveDeRiskNextAttemptAt.set(marketId, now.getTime() + PROTECTIVE_DE_RISK_VETO_RETRY_MS);
        // Preserve the old protective veto observability: prefer the protective
        // (A) veto list, fall back to rescue (B), then the arbitration reason.
        const vetoMetrics = aDecision?.risk.vetoes ?? bDecision?.risk.vetoes ?? [arbitration.selectionReason];
        for (const reason of vetoMetrics) this.metrics.riskVetoes.inc({ reason });
        this.record("RISK_VETO", traceId, marketId, {
          reasons: vetoMetrics,
          intent: selectedIntent ?? "NONE",
          deRisk: audit,
        }, asset);
        return;
      }

      if (!quoteCurrent()) {
        this.logger.info?.({ stage: "REQUOTE", marketId, quotePass, reason: "ASYNC_RISK_FACTS_CHANGED" }, "risk reduction milestone");
        continue;
      }

      this.protectiveDeRiskVetoFingerprint.delete(marketId);
      this.record("RISK_ACCEPT", traceId, marketId, {
        orderId: selectedDecision.order.id,
        intent: selectedIntent,
        deRisk: audit,
      }, asset);
      if (reversal && !this.reversalEligibility(reversal, this.ledger.positions.get(marketId), this.nowFn().getTime()).eligible) return;
      const unpairedBeforeSubmit = d(position.unpairedQuantity);
      await this.submitPaperOrder(
        selectedDecision.order,
        selectedIntent === "RESCUE_SECOND_LEG" ? oppositeSnapshot! : snapshot,
        `${marketId}:${selectedIntent === "RESCUE_SECOND_LEG" ? oppositeOutcome : snapshot.outcome}:${selectedIntent}`,
        selectedIntent!,
        audit,
        { admissionGuard: () => {
          if (selectedIntent === "PROTECTIVE_DE_RISK") {
            const reason = this.protectiveFillHardSafetyReason(selectedDecision.order!);
            if (reason) throw new RiskReductionLifecycleGuardRejectedError(marketId, selectedIntent, reason);
          }
          if (!quoteCurrent()) throw new RiskReductionLifecycleGuardRejectedError(marketId, selectedIntent!, "QUOTE_RISK_CHANGED");
        } },
      );
      const afterSubmit = this.ledger.positions.get(marketId);
      const unpairedAfterSubmit = afterSubmit?.unpairedOutcome
        ? d(afterSubmit.unpairedQuantity)
        : d(0);
      if (unpairedAfterSubmit.lt(unpairedBeforeSubmit)) {
        // A successful reduction earns an immediate residual retry on the next
        // fresh book instead of waiting out the failure backoff.
        this.protectiveDeRiskNextAttemptAt.delete(marketId);
      }
      if (!this.riskReductionOwners.has(marketId) && unpairedAfterSubmit.eq(unpairedBeforeSubmit) && !quoteCurrent()) continue;
      return;
      }
    } finally {
      this.riskReductionInFlight.delete(marketId);
      const current = this.ledger.positions.get(marketId);
      if (!current || d(current.unpairedQuantity).lte(0)) this.clearProtectiveDeRiskState(marketId);
    }
  }

  /**
   * Distinguishes which deadline bound made the position protective-eligible:
   * the hard unhedged duration or the expiry safety window (Â§44). Pure
   * observability; the deadline math never changes.
   */
  private deRiskEligibilityReason(
    eligibility: { effectiveDeadlineMs: number | null; marketEndMs: number | null },
    unhedgedStartedAt: string,
  ): RiskReductionUnhedgedReason {
    const startedMs = Date.parse(unhedgedStartedAt);
    const durationDeadlineMs = Number.isFinite(startedMs) ? startedMs + this.riskLimits.maxUnhedgedDurationMs : null;
    if (eligibility.marketEndMs != null && durationDeadlineMs != null) {
      const expiryDeadlineMs = eligibility.marketEndMs - PROTECTIVE_DE_RISK_PRE_EXPIRY_SAFETY_MS;
      if (eligibility.effectiveDeadlineMs != null && eligibility.effectiveDeadlineMs === expiryDeadlineMs && expiryDeadlineMs < durationDeadlineMs) {
        return "PRE_EXPIRY_SAFETY";
      }
    }
    return "MAX_UNHEDGED_DURATION";
  }

  private protectiveRiskStateAvailable(): boolean {
    return !this.state.journalDegraded
      && !this.overload.hasIntegrityFault
      && this.overload.state !== "FAIL_CLOSED";
  }

  /**
   * Final mutation-boundary hard-safety check for a protective SELL fill. This
   * runs inside the PAPER_FILL beforeCommit, after every await between risk
   * evaluation and the decisive commit (PAPER_ORDER_CREATED persistence,
   * PAPER_EXECUTION_LATENCY_MS, settlement lookups). Alpha gates remain
   * bypassed for PROTECTIVE_DE_RISK, but a transition to DB unavailable,
   * journal degraded, overload integrity fault / FAIL_CLOSED, timestamp
   * disorder, shutdown, a feed gap (no live book), token desynchronization,
   * or stale/absent/future required market execution state must prevent a new
   * protective economic mutation â€” rejected as a normal state change, never
   * as an integrity fault.
   */
  private protectiveFillHardSafetyReason(fill: Pick<Parameters<PaperLedger["applyFill"]>[0], "marketId" | "tokenId" | "outcome">): string | null {
    if (this.state.database !== "UP") return "DATABASE_UNAVAILABLE";
    if (this.state.journalDegraded || this.overload.hasIntegrityFault || this.overload.state === "FAIL_CLOSED") {
      return "RISK_STATE_UNAVAILABLE";
    }
    if (this.timestampDisorder) return "EVENT_TIMESTAMP_DISORDER";
    if (this.state.draining) return "SHUTTING_DOWN";

    // A protective SELL after the market's usable trading lifetime is over is
    // illegal: settlement authority owns the position from marketEnd onward.
    // This is the durable final fence for a candidate prepared before expiry
    // whose commit lands after it (feed queued / maintenance lag).
    const filledMarketEnd = this.marketsById.get(fill.marketId)?.endDate ?? null;
    const filledMarketEndMs = filledMarketEnd ? Date.parse(filledMarketEnd) : Number.NaN;
    const fenceNowMs = this.nowFn().getTime();
    if (Number.isFinite(filledMarketEndMs) && Number.isFinite(fenceNowMs) && fenceNowMs >= filledMarketEndMs) return "MARKET_EXPIRED";

    // The protective SELL executes against the live trusted book. If the book
    // vanished (feed gap/drop or pair-projection clearing during a resync),
    // its market/outcome/token identity no longer matches the fill, or the
    // token left the synchronized set, the admission-era execution snapshot is
    // no longer authoritative and this mutation must not commit.
    const executionBook = this.snapshots.get(fill.marketId)?.[fill.outcome] ?? null;
    if (!executionBook) return "MARKET_BOOK_UNAVAILABLE";
    if (
      executionBook.marketId !== fill.marketId
      || executionBook.outcome !== fill.outcome
      || executionBook.tokenId !== fill.tokenId
    ) {
      return "MARKET_BOOK_IDENTITY_MISMATCH";
    }
    if (!this.adapterTokenSynchronized(fill.tokenId)) return "MARKET_TOKEN_UNSYNCHRONIZED";

    // Book time must be valid, not from the future, and within the configured
    // freshness window at the commit boundary.
    const receivedAtMs = Date.parse(executionBook.receivedAt);
    const nowMs = this.nowFn().getTime();
    if (!Number.isFinite(receivedAtMs) || !Number.isFinite(nowMs)) return "MARKET_FEED_STALE";
    if (receivedAtMs > nowMs) return "MARKET_FEED_FUTURE";
    if (nowMs - receivedAtMs > this.riskLimits.staleFeedTimeoutMs) return "MARKET_FEED_STALE";
    return null;
  }

  /**
   * DIAGNOSTIC ONLY (short soak). Records why a protective de-risk attempt was
   * suppressed, without altering any decision, timing, or paper behavior.
   * Rate-limited per (market, gate): a repeated identical gate emits at most
   * once per PROTECTIVE_DE_RISK_SUPPRESS_EMIT_MIN_MS, while a transition to a
   * different gate for the same market emits immediately, so a pre-timeout or
   * in-flight observation can never hide the actual post-timeout gate. Each
   * emission is one structured log line, one SSE/decision-feed record, and one
   * durable journal_events row of type PROTECTIVE_DE_RISK_SUPPRESSED
   * (operational report class, monitor_control journal class: not canonical,
   * not hashed and has no canonical integrity effect).
   */
  private recordProtectiveDeRiskSuppressed(
    marketId: string,
    gate: string,
    reason: string,
    context: {
      position?: Position | null;
      snapshot?: MarketSnapshot | null;
      candidate?: StrategyCandidate | null;
      unhedgedDurationMs?: number | null;
      extra?: Record<string, unknown>;
    } = {},
  ): void {
    const now = this.nowFn();
    const key = `${marketId}\u0000${gate}`;
    const lastAt = this.protectiveDeRiskLastSuppressEmitAt.get(key) ?? Number.NEGATIVE_INFINITY;
    if (now.getTime() - lastAt < PROTECTIVE_DE_RISK_SUPPRESS_EMIT_MIN_MS) return;
    this.protectiveDeRiskLastSuppressEmitAt.set(key, now.getTime());

    const position = context.position;
    const snapshot = context.snapshot;
    let bestBid: string | null = null;
    let bidDepth = "0";
    if (snapshot) {
      let depth = d(0);
      for (const level of snapshot.bids) {
        const price = d(level.price);
        const quantity = d(level.quantity);
        if (price.lte(0) || quantity.lte(0)) continue;
        if (bestBid == null || price.gt(bestBid)) bestBid = decimal(price);
        depth = depth.plus(quantity);
      }
      bidDepth = decimal(depth);
    }
    const unhedgedDurationMs = context.unhedgedDurationMs != null
      ? context.unhedgedDurationMs
      : position
        ? Math.max(0, now.getTime() - Date.parse(position.unhedgedStartedAt ?? position.openedAt))
        : null;
    const facts: Record<string, unknown> = {
      gate,
      reason,
      experimentId: this.experiment?.id ?? null,
      experimentStatus: this.experiment?.status ?? null,
      marketId,
      tokenId: snapshot?.tokenId ?? null,
      outcome: position?.unpairedOutcome ?? null,
      at: now.toISOString(),
      unhedgedDurationMs,
      unpairedQuantity: position?.unpairedQuantity ?? null,
      snapshotPresent: snapshot != null,
      snapshotReceivedAt: snapshot?.receivedAt ?? null,
      snapshotAgeMs: snapshot ? Math.max(0, now.getTime() - Date.parse(snapshot.receivedAt)) : null,
      tokenSynchronized: snapshot ? this.adapterTokenSynchronized(snapshot.tokenId) : null,
      tokenEpoch: snapshot ? this.adapterTokenEpoch(snapshot.tokenId) : null,
      bidsPresent: snapshot ? snapshot.bids.length > 0 : null,
      bestBid,
      bidDepth,
      asksPresent: snapshot ? snapshot.asks.length > 0 : null,
      minOrderSize: snapshot?.minOrderSize ?? null,
      feeScheduleKnown: snapshot ? snapshot.feeSchedule != null : null,
      candidateQuantity: context.candidate?.quantity ?? null,
      candidateLimitPrice: context.candidate?.limitPrice ?? null,
      database: this.state.database,
      journalDegraded: this.state.journalDegraded,
      overloadState: this.overload.state,
      integrityFault: this.overload.hasIntegrityFault,
      timestampDisorder: this.timestampDisorder,
      draining: this.state.draining,
      ...(context.extra ?? {}),
    };
    const traceId = createId("derisk-obs");
    this.record("PROTECTIVE_DE_RISK_SUPPRESSED", traceId, marketId, facts, this.assetByMarket.get(marketId) ?? snapshot?.asset ?? null);
    (this.logger as { warn?: (obj: unknown, msg?: string) => void }).warn?.({ marketId, gate, reason, benign: true }, "protective de-risk suppressed (diagnostic)");
    void this.persistProtectiveDeRiskSuppressed(traceId, marketId, facts).catch(() => undefined);
  }

  private async persistProtectiveDeRiskSuppressed(traceId: string, marketId: string, facts: Record<string, unknown>): Promise<void> {
    try {
      // Diagnostic-only row. It intentionally bypasses safeAppend: a benign
      // observation must never mutate live health flags (safeAppend clears
      // journalDegraded on any non-critical success) or take fail-closed
      // semantics. Raw append is rate-bounded (one per market per 60s).
      await this.journal.append(createJournalEvent({
        traceId,
        type: "PROTECTIVE_DE_RISK_SUPPRESSED",
        aggregateType: "market",
        aggregateId: marketId,
        payload: { ...facts, experimentId: this.experiment?.id ?? null },
      }), {});
    } catch {
      // Observability must never affect trading decisions, timing, or health.
    }
  }

  private clearProtectiveDeRiskState(marketId: string): void {
    this.profitTrailingRejectedEvidence.delete(marketId);
    this.protectiveDeRiskAttempts.delete(marketId);
    this.protectiveDeRiskNextAttemptAt.delete(marketId);
    this.protectiveDeRiskVetoFingerprint.delete(marketId);
    this.protectiveDeRiskLastWaitingAt.delete(marketId);
    this.earlyCatastrophicNextAttemptAt.delete(marketId);
    this.earlyCatastrophicVetoFingerprint.delete(marketId);
    // Diagnostic suppression timestamps are keyed per (market, gate).
    for (const key of [...this.protectiveDeRiskLastSuppressEmitAt.keys()]) {
      if (key.startsWith(`${marketId}\u0000`)) this.protectiveDeRiskLastSuppressEmitAt.delete(key);
    }
  }

  private async pollSettlements(): Promise<void> {
    const now = this.nowFn();
    for (const [orderId, resting] of [...this.restingOrders.entries()]) {
      const market = this.marketsById.get(resting.order.marketId);
      if (!market?.endDate || new Date(market.endDate).getTime() > now.getTime()) continue;
      await this.releaseMaker(orderId, { cancel: true, reason: "MARKET_EXPIRED" });
    }
    for (const position of [...this.ledger.positions.values()]) {
      if (this.settledMarkets.has(position.marketId)) continue;
      if (Number(position.upQuantity) === 0 && Number(position.downQuantity) === 0) continue;
      const lookup = this.resolveSettlementLookup(position.marketId, true);
      if (!lookup || !Number.isFinite(Date.parse(lookup.endDate))
        || Date.parse(lookup.endDate) > now.getTime()
        || !isAuthoritativeBinaryMapping(lookup.yesMapsTo, lookup.noMapsTo)) continue;
      const waitUntil = this.settlementBackoff.get(position.marketId) ?? 0;
      if (now.getTime() < waitUntil) continue;
      try {
        await this.settleMarket(position.marketId, lookup);
      } catch (error) {
        if (!(error instanceof SettlementObligationChangedError)) throw error;
        // Inventory was closed while the resolution request/commit was in flight.
      }
    }
  }

  /**
   * Non-economic learning settlement poll. Labels durable learning samples via
   * the same public resolution fetch used by economic settlement, without
   * calling settleMarket / commitAuthoritative POSITION_SETTLED / ledger mutation.
   */

  private settlementLookupFromMarket(market: DiscoveredMarket): BinarySettlementLookup | null {
    if (!market.endDate || !Number.isFinite(Date.parse(market.endDate))) return null;
    const yes = market.qualification.binary.yesOutcome;
    const no = market.qualification.binary.noOutcome;
    if (!isAuthoritativeBinaryMapping(yes, no)) return null;
    return {
      marketId: market.id,
      slug: market.slug ?? null,
      endDate: market.endDate,
      yesMapsTo: yes as "UP" | "DOWN",
      noMapsTo: no as "UP" | "DOWN",
    };
  }

  private resolveSettlementLookup(marketId: string, economic = false): BinarySettlementLookup | null {
    // Economic inventory retains its own original capture; learning/CF callers
    // keep their existing decision-time lookup precedence.
    const fromEconomic = economic ? this.economicSettlementLookups.get(marketId) : null;
    if (fromEconomic) return fromEconomic;
    const live = this.marketsById.get(marketId);
    if (live) {
      const fromLive = this.settlementLookupFromMarket(live);
      if (fromLive) return fromLive;
    }
    return null;
  }

  private async settleMarket(marketId: string, lookup: BinarySettlementLookup): Promise<void> {
    if (this.settledMarkets.has(marketId)) return;
    // There is no economic obligation to settle when no position remains. In
    // particular this prevents an old rediscovered market from creating a new
    // durable settlement transition after its bounded replay identity expires.
    if (!this.ledger.positions.has(marketId)) return;
    const operationKey = `settlement:${marketId}:${this.experiment?.id ?? "paper"}`;
    const durableSettlement = await this.journal.readDurableByOperationKey(operationKey);
    if (durableSettlement?.type === "POSITION_SETTLED") {
      // Rediscovery may occur after the live marker was evicted. Rehydrate
      // the short-lived suppression bit from durable authority without
      // replaying cash or settlement effects.
      this.settledMarkets.add(marketId);
      return;
    }
    const raw = await this.resolutionFetcher(lookup.marketId) ?? (lookup.slug ? await this.resolutionFetcher(lookup.slug) : null);
    if (!raw) {
      this.bumpSettlementBackoff(marketId);
      return;
    }
    const parsed = parsePublicBinaryResolution(
      raw,
      lookup.yesMapsTo,
      lookup.noMapsTo,
    );
    if (parsed.status !== "resolved" || parsed.upPayoff === null || parsed.downPayoff === null || !parsed.winner) {
      this.bumpSettlementBackoff(marketId);
      return;
    }
    const settledAt = this.nowFn();
    let positionBefore = this.ledger.positions.get(marketId);
    if (!positionBefore || (!d(positionBefore.upQuantity).gt(0) && !d(positionBefore.downQuantity).gt(0))) return;
    const upPayoff = parsed.upPayoff;
    const downPayoff = parsed.downPayoff;
    const asset = this.assetByMarket.get(marketId) ?? null;
    const experimentId = this.experiment?.id ?? null;
    await this.commitAuthoritative({
      operationKey,
      type: "POSITION_SETTLED",
      aggregateType: "position",
      aggregateId: positionBefore?.id ?? marketId,
      occurredAt: settledAt,
      buildPayload: (occurredAt) => {
        // Build economics from inventory at the serialized mutation boundary.
        positionBefore = this.ledger.positions.get(marketId);
        if (!positionBefore || (!d(positionBefore.upQuantity).gt(0) && !d(positionBefore.downQuantity).gt(0))) {
          throw new SettlementObligationChangedError();
        }
        const auth = {
          marketId,
          upPayoff,
          downPayoff,
          settledAt: occurredAt.toISOString(),
          winner: parsed.winner,
          slug: lookup.slug,
          resolution: parsed,
        };
        return {
          ...auth,
          derivedAudit: [...this.buildSettlementDerivedAudit({
            marketId, asset, experimentId, parsed, positionBefore, upPayoff, downPayoff, occurredAt, auth,
          }), ...(this.nakedDirectionalDiagnostics.has(marketId) ? [{
            id: derivedAuditEventId("NAKED_DIRECTIONAL_SUMMARY", positionBefore.id),
            type: "NAKED_DIRECTIONAL_SUMMARY", aggregateType: "position", aggregateId: positionBefore.id, traceId: positionBefore.id,
            payload: { ...nakedDirectionalSummary(this.nakedDirectionalDiagnostics.get(marketId)!, parsed.winner!, occurredAt,
              settlementRealizedDelta(positionBefore, upPayoff, downPayoff)), experimentId, asset },
          }] : []), ...(this.riskReductionOwners.has(marketId)
            ? [this.ownershipAudit(this.riskReductionOwners.get(marketId)!, "RISK_REDUCTION_FAILED", "SETTLEMENT_SUPERSEDED")]
            : [])],
        };
      },
      afterApply: async () => {
        this.settledMarkets.add(marketId);
        this.soakPersistedRevisions.delete(marketId);
        this.soakPersistedObservationCounts.delete(marketId);
        this.settlementBackoff.delete(marketId);
        this.clearProtectiveDeRiskState(marketId);
        if (!positionBefore) return;
        const realizedDelta = settlementRealizedDelta(positionBefore, upPayoff, downPayoff);
        const outcome = classifySettledOutcome(realizedDelta, positionBefore.totalFees);
        this.record("POSITION_SETTLED", positionBefore.id, marketId, { outcome, realizedDelta }, asset);
        this.metrics.openPositions.set(this.ledger.positions.size);
      },
    });
  }

  private buildSettlementDerivedAudit(input: {
    marketId: string;
    asset: string | null | undefined;
    experimentId: string | null;
    parsed: ReturnType<typeof parsePublicBinaryResolution>;
    positionBefore: Position | undefined;
    upPayoff: "0" | "1";
    downPayoff: "0" | "1";
    occurredAt: Date;
    auth: Record<string, unknown>;
  }): DerivedAuditRecord[] {
    const { marketId, asset, experimentId, parsed, positionBefore, upPayoff, downPayoff, occurredAt, auth } = input;
    const operationKey = `settlement:${marketId}:${experimentId ?? "paper"}`;
    const records: DerivedAuditRecord[] = [{
      id: derivedAuditEventId("MARKET_RESOLUTION_OBSERVED", marketId),
      type: "MARKET_RESOLUTION_OBSERVED",
      aggregateType: "market",
      aggregateId: marketId,
      traceId: experimentId ?? marketId,
      payload: { ...parsed, marketId, slug: auth.slug, experimentId },
    }];
    const projected = this.projectDerivedAudit(
      {
        operationKey,
        type: "POSITION_SETTLED",
        aggregateType: "position",
        aggregateId: positionBefore?.id ?? marketId,
        payload: auth,
      },
      occurredAt,
      (state) => {
        const extra: DerivedAuditRecord[] = [];
        if (positionBefore) {
          const cashDelta = settlementCashDelta(positionBefore, upPayoff, downPayoff);
          const realizedDelta = settlementRealizedDelta(positionBefore, upPayoff, downPayoff);
          const priorRealized = this.state.resultAccounting.episodes[marketId]?.realized ?? "0";
          const wholeEpisodeRealized = d(realizedDelta).plus(priorRealized).toFixed();
          const outcome = classifySettledOutcome(wholeEpisodeRealized, positionBefore.totalFees);
          const grossOutcome = classifySettledOutcome(realizedDelta, "0");
          extra.push({
            id: derivedAuditEventId("POSITION_SETTLED", marketId),
            type: "POSITION_SETTLED",
            aggregateType: "position",
            aggregateId: positionBefore.id,
            traceId: positionBefore.id,
            payload: {
              marketId, asset, cashDelta, realizedDelta, episodeNetPnl: d(wholeEpisodeRealized).minus(positionBefore.totalFees).toFixed(),
              upPayoff, downPayoff, winner: parsed.winner,
              upQuantity: positionBefore.upQuantity, downQuantity: positionBefore.downQuantity,
              feesUsd: positionBefore.totalFees, grossOutcome, outcome, experimentId,
            },
          });
          extra.push({
            id: derivedAuditEventId(outcome, marketId),
            type: outcome,
            aggregateType: "position",
            aggregateId: positionBefore.id,
            traceId: positionBefore.id,
            payload: { marketId, realizedDelta, feesUsd: positionBefore.totalFees, grossOutcome, experimentId },
          });
          if (Number(positionBefore.pairedQuantity) > 0) {
            const metrics = calculatePairMetrics(positionBefore);
            const paired = Number(positionBefore.pairedQuantity);
            const grossLockedEdge = 1 - Number(positionBefore.combinedPairCost);
            const netRealizedEdge = paired > 0 ? Number(metrics.netPairEdge) / paired : Number(metrics.netPairEdge);
            extra.push({
              id: derivedAuditEventId("PAIR_REALIZED", marketId),
              type: "PAIR_REALIZED",
              aggregateType: "position",
              aggregateId: positionBefore.id,
              traceId: positionBefore.id,
              payload: {
                marketId, pairedQuantity: positionBefore.pairedQuantity, combinedPairCost: positionBefore.combinedPairCost,
                grossLockedEdge, netRealizedEdge, realizedEdge: netRealizedEdge,
                fees: metrics.fees, experimentId,
              },
            });
          }
        }
        extra.push({
          id: derivedAuditEventId("PNL_SNAPSHOT", `settlement:${marketId}`),
          type: "PNL_SNAPSHOT",
          aggregateType: "ledger",
          aggregateId: experimentId ?? "paper",
          traceId: experimentId ?? "paper",
          payload: { ...state.ledger.snapshot(), experimentId, source: "SETTLEMENT", marketId,
            economicResults: state.resultAccounting ? economicResults(state.resultAccounting) : null,
            tradingResults: state.resultAccounting ? tradingResults(state.resultAccounting) : null },
        });
        return extra;
      },
    );
    return [...records, ...projected];
  }

  private bumpSettlementBackoff(marketId: string): void {
    const now = this.nowFn().getTime();
    const current = this.settlementBackoff.get(marketId);
    const lastDelay = current && current > now ? current - now : this.env.SETTLEMENT_POLL_MS;
    const nextDelay = Math.min(900_000, Math.max(this.env.SETTLEMENT_POLL_MS, lastDelay * 2));
    this.settlementBackoff.set(marketId, now + nextDelay);
  }

  private async markToMarketClose(unresolvedCount: number): Promise<boolean> {
    if (this.nakedDirectionalSoakEnabled() || this.nakedDirectionalDiagnostics.size > 0) return false;
    const prices: Record<string, { up: string; down: string }> = {};
    for (const position of this.ledger.positions.values()) {
      const pair = this.snapshots.get(position.marketId);
      const upQty = Number(position.upQuantity);
      const downQty = Number(position.downQuantity);
      if (upQty <= 0 && downQty <= 0) continue;
      const upMark = upQty > 0 ? conservativeRealizableMark(pair?.UP) : position.averageUpCost;
      const downMark = downQty > 0 ? conservativeRealizableMark(pair?.DOWN) : position.averageDownCost;
      if ((upQty > 0 && upMark === null) || (downQty > 0 && downMark === null)) {
        return false;
      }
      prices[position.marketId] = { up: upMark ?? position.averageUpCost, down: downMark ?? position.averageDownCost };
    }
    const occurredAt = this.nowFn();
    const aggregateId = this.experiment?.id ?? "paper";
    await this.commitAuthoritative({
      operationKey: `mtm:${aggregateId}`,
      type: "LEDGER_MARK_TO_MARKET",
      aggregateType: "ledger",
      aggregateId,
      occurredAt,
      buildPayload: (at) => {
        const auth = { prices, markedAt: at.toISOString(), unresolvedPositionCount: unresolvedCount };
        return {
          ...auth,
          derivedAudit: this.projectDerivedAudit(
            { operationKey: `mtm:${aggregateId}`, type: "LEDGER_MARK_TO_MARKET", aggregateType: "ledger", aggregateId, payload: auth },
            at,
            (projected) => [
              {
                id: derivedAuditEventId("LEDGER_MARK_TO_MARKET", aggregateId),
                type: "LEDGER_MARK_TO_MARKET",
                aggregateType: "ledger",
                aggregateId,
                traceId: aggregateId,
                payload: { ...auth, experimentId: this.experiment?.id ?? null },
              },
              {
                id: derivedAuditEventId("PNL_SNAPSHOT", `mtm:${aggregateId}`),
                type: "PNL_SNAPSHOT",
                aggregateType: "ledger",
                aggregateId,
                traceId: aggregateId,
                payload: {
                  ...projected.ledger.snapshot(), experimentId: this.experiment?.id ?? null,
                  source: "MARK_TO_MARKET", unresolvedPositionCount: unresolvedCount, markToMarket: true,
                  economicResults: projected.resultAccounting ? economicResults(projected.resultAccounting) : null,
                  tradingResults: projected.resultAccounting ? tradingResults(projected.resultAccounting) : null,
                },
              },
            ],
          ),
        };
      },
    });
    return true;
  }

  private async commitAbortedStart(start: ExperimentManifest, reason: string): Promise<void> {
    const aborted: ExperimentManifest = {
      ...start,
      status: "ABORTED_START",
      startValidationPending: false,
      abortedAt: this.nowFn().toISOString(),
      abortReason: reason,
      completedAt: null,
      markToMarketAt: null,
    };
    await this.commitAuthoritative({
      operationKey: `experiment:${start.id}:start_aborted`,
      type: "EXPERIMENT_START_ABORTED",
      aggregateType: "experiment",
      aggregateId: start.id,
      buildPayload: () => ({ experiment: aborted, reason, derivedAudit: [this.experimentDerivedAudit("EXPERIMENT_START_ABORTED", aborted)] }),
      checkpoint: true,
    });
  }

  private async commitExperimentStatus(next: ExperimentManifest): Promise<void> {
    const type = next.status === "ENTRY_CLOSED" ? "EXPERIMENT_ENTRY_CLOSED"
      : next.status === "COMPLETED" ? "EXPERIMENT_COMPLETED"
      : null;
    if (!type) {
      this.experiment = next;
      this.syncExperimentWindow();
      return;
    }
    await this.commitAuthoritative({
      operationKey: `experiment:${next.id}:${next.status === "ENTRY_CLOSED" ? "entry_closed" : "completed"}`,
      type,
      aggregateType: "experiment",
      aggregateId: next.id,
      buildPayload: (at) => ({
        experiment: next,
        derivedAudit: [this.experimentDerivedAudit(type, next), ...(next.status === "COMPLETED"
          ? this.projectDerivedAudit({ operationKey: `experiment:${next.id}:completed`, type,
            aggregateType: "experiment", aggregateId: next.id, payload: { experiment: next } }, at,
            (projected) => [{ id: derivedAuditEventId("PNL_SNAPSHOT", `experiment:${next.id}:completed`),
              type: "PNL_SNAPSHOT", aggregateType: "ledger", aggregateId: next.id, traceId: next.id,
              payload: { ...projected.ledger.snapshot(), experimentId: next.id, source: "EXPERIMENT_COMPLETED",
                economicResults: projected.resultAccounting ? economicResults(projected.resultAccounting) : null,
                tradingResults: projected.resultAccounting ? tradingResults(projected.resultAccounting) : null } }]) : [])],
      }),
      // Keep the durable completion event inside the manifest's authoritative
      // report window.  The manifest timestamp is captured before the durable
      // append; taking a second clock reading here can otherwise place the
      // event just beyond completedAt on a slow PG17 runner.
      occurredAt: next.status === "COMPLETED" && next.completedAt ? new Date(next.completedAt) : undefined,
      checkpoint: true,
    });
    if(next.status==='ENTRY_CLOSED')for(const r of [...this.restingOrders.values()])
      if(r.order.makerFirst&&this.restingOrderIntent(r)==='OPEN_FIRST_LEG')
        await this.releaseMaker(r.order.id,{cancel:true,reason:'MAKER_FIRST_ENTRIES_CLOSED'});
    if (next.status === "COMPLETED") {
      this.researchIngressGeneration += 1;
      this.clearResearchActivationState();
    }
  }

  private researchIngressBlocked(): boolean {
    return this.experiment?.status === "COMPLETED";
  }

  private beginResearchIngress(): { generation: number } {
    this.researchIngressInFlight += 1;
    return { generation: this.researchIngressGeneration };
  }

  private endResearchIngress(): void {
    this.researchIngressInFlight = Math.max(0, this.researchIngressInFlight - 1);
  }

  private researchIngressStale(generation: number): boolean {
    return this.researchIngressBlocked() || generation !== this.researchIngressGeneration;
  }

  private installIntegrityFault(fault: ReportSourceIntegrityFault): void {
    const overflowKey = `${fault.experimentId}:${INTEGRITY_OVERFLOW_EVENT_ID}`;
    if (this.reportSourceIntegrity.has(overflowKey)) return;
    const key = `${fault.experimentId}:${fault.eventId}`;
    if (this.reportSourceIntegrity.has(key)) return;
    const count = [...this.reportSourceIntegrity.values()].filter((item) => item.experimentId === fault.experimentId).length;
    if (fault.eventId === INTEGRITY_OVERFLOW_EVENT_ID || count >= REPORT_SOURCE_INTEGRITY_HARD_CAP) {
      for (const [existing, item] of this.reportSourceIntegrity) {
        if (item.experimentId === fault.experimentId) this.reportSourceIntegrity.delete(existing);
      }
      this.reportSourceIntegrity.set(overflowKey, overflowIntegrityFault(fault.experimentId, fault.at));
      return;
    }
    this.reportSourceIntegrity.set(key, fault);
  }

  private async restoreReportSourceIntegrity(): Promise<void> {
    const experimentId = this.experiment?.id;
    if (!experimentId) return;
    if (this.experiment?.status === "IDLE") return;
    try {
      const unresolved = await loadUnresolvedReportSourceIntegrity(this.journal, experimentId);
      const repairs = await loadIntegrityRepairIds(this.journal, experimentId);
      if (repairs.overflow) {
        this.clearExperimentIntegrity(experimentId);
        this.installIntegrityFault(overflowIntegrityFault(experimentId, this.nowFn().toISOString()));
        return;
      }
      for (const [key, fault] of [...this.reportSourceIntegrity.entries()]) {
        if (fault.experimentId !== experimentId) continue;
        if (repairs.ids.has(fault.eventId) && fault.eventId !== INTEGRITY_OVERFLOW_EVENT_ID) {
          this.reportSourceIntegrity.delete(key);
        }
      }
      for (const fault of unresolved) this.installIntegrityFault(fault);
      const remaining = [...this.reportSourceIntegrity.values()].filter((fault) => fault.experimentId === experimentId);
      this.clearExperimentIntegrity(experimentId);
      for (const fault of boundIntegrityFaults(remaining, experimentId)) this.installIntegrityFault(fault);
    } catch {
      this.clearExperimentIntegrity(experimentId);
      this.installIntegrityFault(overflowIntegrityFault(experimentId, this.nowFn().toISOString()));
    }
  }

  private clearExperimentIntegrity(experimentId: string): void {
    for (const key of [...this.reportSourceIntegrity.keys()]) {
      if (key.startsWith(`${experimentId}:`)) this.reportSourceIntegrity.delete(key);
    }
  }

  private clearReportSourceIntegrity(keepExperimentId?: string): void {
    if (!keepExperimentId) {
      this.reportSourceIntegrity.clear();
      return;
    }
    for (const key of [...this.reportSourceIntegrity.keys()]) {
      if (!key.startsWith(`${keepExperimentId}:`)) this.reportSourceIntegrity.delete(key);
    }
  }

  private async persistIntegrityFaultMarker(fault: ReportSourceIntegrityFault): Promise<boolean> {
    if (this.persistedIntegrityFaults.has(fault)) return true;
    try {
      await this.appendWithTransportRecovery(createIntegrityFaultEvent(fault), {}, "integrity");
    } catch {
      try {
        await this.commitAuthoritative({
          operationKey: integrityDurableOperationKey("fault", fault.experimentId, fault.eventId),
          type: REPORT_SOURCE_INTEGRITY_FAULT,
          aggregateType: "experiment",
          aggregateId: fault.experimentId,
          buildPayload: () => ({
            eventId: fault.eventId,
            eventType: fault.eventType,
            reason: fault.reason,
            experimentId: fault.experimentId,
            at: fault.at,
            derivedAudit: [],
          }),
          checkpoint: false,
        });
      } catch (error) {
        this.logger.error({
          err: error,
          eventId: fault.eventId,
        }, "report_source_integrity_durable_marker_failed");
        return false;
      }
    }
    this.persistedIntegrityFaults.add(fault);
    return true;
  }

  private async noteReportSourceIntegrityFault(event: JournalEvent, error: unknown): Promise<boolean> {
    const experimentId = this.experiment?.id;
    if (!experimentId) return false;
    const overflowKey = `${experimentId}:${INTEGRITY_OVERFLOW_EVENT_ID}`;
    const overflow = this.reportSourceIntegrity.get(overflowKey);
    if (overflow) return this.persistIntegrityFaultMarker(overflow);
    const fault: ReportSourceIntegrityFault = {
      experimentId,
      eventType: event.type,
      eventId: event.id,
      reason: error instanceof Error ? error.message : String(error),
      at: this.nowFn().toISOString(),
    };
    const existing = this.reportSourceIntegrity.get(`${experimentId}:${event.id}`);
    if (existing) return this.persistIntegrityFaultMarker(existing);
    this.installIntegrityFault(fault);
    return this.persistIntegrityFaultMarker(this.reportSourceIntegrity.get(overflowKey) ?? fault);
  }

  private unresolvedReportSourceIntegrity(experimentId: string): ReportSourceIntegrityFault[] {
    return [...this.reportSourceIntegrity.values()].filter((fault) => fault.experimentId === experimentId);
  }

  async repairReportSourceIntegrityForTest(event: JournalEvent): Promise<boolean> {
    await this.journal.append(event);
    const experimentId = this.experiment?.id;
    if (!experimentId) return false;
    if (this.reportSourceIntegrity.has(`${experimentId}:${INTEGRITY_OVERFLOW_EVENT_ID}`)) return false;
    const key = `${experimentId}:${event.id}`;
    const fault = this.reportSourceIntegrity.get(key);
    if (!fault) return false;
    const verified = await this.journal.readJournalEventById(event.id);
    if (!verified || verified.type !== event.type) return false;
    await this.journal.append(createIntegrityRepairEvent(fault));
    this.reportSourceIntegrity.delete(key);
    return true;
  }

  private finalReportRuntimeUnresolved() {
    return {
      reportInput: this.journalAdmission.reportInputUnresolved,
      researchIngress: this.researchIngressInFlight,
      guardianPersistence: this.guardianPersistenceUnresolved(),
    };
  }

  private finalReportRuntimeDrained(): boolean {
    const unresolved = this.finalReportRuntimeUnresolved();
    return unresolved.reportInput === 0
      && unresolved.researchIngress === 0
      && unresolved.guardianPersistence === 0;
  }

  private async writeFinalReport(): Promise<void> {
    this.finalReportWrites += 1;
    try { await this.writeFinalReportAttempt(); }
    finally { this.finalReportWrites -= 1; }
  }

  private async writeFinalReportAttempt(): Promise<void> {
    if (!this.experiment || this.reportGenerated) return;
    const experiment = this.experiment;
    try {
      if (await this.readVerifiedQuarantine(experiment.id)) return;
      if (experiment.status === "COMPLETED") {
        if (!this.finalReportRuntimeDrained()) {
          this.reportGenerated = false;
          this.logger.error({
            experimentId: experiment.id,
            unresolved: this.finalReportRuntimeUnresolved(),
          }, "final experiment report deferred: canonical runtime work still in flight");
          return;
        }
        const integrityFaults = this.unresolvedReportSourceIntegrity(experiment.id);
        if (integrityFaults.length > 0) {
          this.reportGenerated = false;
          this.logger.error({
            experimentId: experiment.id,
            integrityFaultCount: integrityFaults.length,
            integrityFaults: integrityFaults.slice(0, 8),
          }, "final experiment report blocked by unresolved source integrity");
          return;
        }
      }
      const existing = await this.journal.loadExperimentReport(experiment.id, "final");
      if (existing && isCanonicalFinalReport(existing)) {
        await ensureReportGeneratedAudit(this.journal, existing, { reportDirectory: process.env.REPORT_DIRECTORY ?? "reports" });
        this.reportGenerated = true;
        try {
          await writeExperimentSummary(summaryFromDurableReport(existing), process.env.REPORT_DIRECTORY ?? "reports");
        } catch {
          this.metrics.finalReportFsExportFailures.inc();
        }
        return;
      }
      const unresolved = [...this.ledger.positions.values()].filter((position) => Number(position.upQuantity) > 0 || Number(position.downQuantity) > 0).length;
      const streamed = await this.heavyRead.run(() => streamExperimentSummary(this.journal, experiment, {
        status: experiment.status,
        closePolicy: experiment.closePolicy ?? "observe_until_settlement",
        reportKind: "final",
        unresolvedPositionCount: unresolved,
        metricsScope: "complete",
        complete: true,
        truncated: false,
      }));
      this.metrics.reportScanPages.observe(streamed.scan.pages);
      this.metrics.reportScanEvents.observe(streamed.scan.scanned);
      this.metrics.reportScanDuration.observe(streamed.scan.durationMs / 1000);
      this.metrics.reportAccumulatorHighWaterBytes.set(streamed.accumulatorHighWaterBytes);
      streamed.summary.complete = streamed.scan.complete && !streamed.scan.truncated && !streamed.scan.interrupted;
      streamed.summary.truncated = streamed.scan.truncated || streamed.scan.interrupted;
      streamed.summary.truncatedReason = streamed.summary.truncated ? (streamed.scan.truncatedReason ?? "SCAN_CEILING") : null;
      if (!streamed.summary.complete || streamed.summary.truncated) {
        this.reportGenerated = false;
        return;
      }
      if (experiment.status === "COMPLETED") {
        if (!this.finalReportRuntimeDrained()) {
          this.reportGenerated = false;
          this.logger.error({
            experimentId: experiment.id,
            unresolved: this.finalReportRuntimeUnresolved(),
          }, "final experiment report deferred: canonical runtime work changed during scan");
          return;
        }
        const integrityFaults = this.unresolvedReportSourceIntegrity(experiment.id);
        if (integrityFaults.length > 0) {
          this.reportGenerated = false;
          this.logger.error({
            experimentId: experiment.id,
            integrityFaultCount: integrityFaults.length,
            integrityFaults: integrityFaults.slice(0, 8),
          }, "final experiment report blocked by source integrity change during scan");
          return;
        }
      }
      if (streamed.accumulatorSnapshot) {
        await persistVerifiedAnalyticsRollup(this.journal, {
          experimentId: experiment.id,
          sourceCursor: await this.journal.latestDurableSequence(),
          sourceFenceId: streamed.scan.fence.through?.id ?? null,
          sourceFenceOccurredAt: streamed.scan.fence.through?.occurredAt ?? null,
          sourceEventCount: streamed.accumulatorSnapshot.eventCount,
          sourceCommitSeq: streamed.scan.fence.commitSeqTo,
          sourceRecordedAt: streamed.scan.fence.recordedAtTo.toISOString(),
          stableRead: streamed.scan.fence.stable,
          sourceCoverageComplete: streamed.scan.complete && !streamed.scan.truncated && !streamed.scan.interrupted,
          baseContentHash: streamed.baseRollupContentHash,
          accumulator: streamed.accumulatorSnapshot as unknown as Record<string, unknown>,
        });
      }
      let persisted;
      try {
        persisted = await persistCanonicalFinalReport(this.journal, {
          summary: streamed.summary, scan: streamed.scan, experimentStatus: experiment.status,
        });
      } catch (error) {
        this.metrics.finalReportPersistFailures.inc();
        throw error;
      }
      if (!persisted) {
        this.reportGenerated = false;
        this.logger.error({
          experimentId: experiment.id,
          scanComplete: streamed.scan.complete,
          scanInterrupted: streamed.scan.interrupted,
          scanFenceStable: streamed.scan.fence.stable,
        }, "canonical final report persist refused");
        return;
      }
      try {
        await writeExperimentSummary(streamed.summary, process.env.REPORT_DIRECTORY ?? "reports");
      } catch {
        this.metrics.finalReportFsExportFailures.inc();
      }
      await ensureReportGeneratedAudit(this.journal, persisted.report, {
        reportDirectory: process.env.REPORT_DIRECTORY ?? "reports",
        settlementPnlUsd: streamed.summary.settlementPnlUsd,
        unresolvedPositionCount: unresolved,
      });
      this.reportGenerated = true;
      this.record("EXPERIMENT_REPORT_GENERATED", experiment.id, null, {
        reportKind: "final",
        settlementPnlUsd: streamed.summary.settlementPnlUsd,
        contentHash: persisted.report.contentHash,
      });
    } catch (error) {
      this.reportGenerated = false;
      this.logger.error({ err: error }, "final experiment report generation failed");
    }
  }

  private syncExperimentWindow(): void {
    this.state.experimentId = this.experiment?.id ?? null;
    this.state.experimentStatus = this.experiment?.status ?? null;
    this.state.experimentStartedAt = this.experiment?.startedAt ?? null;
    this.state.experimentCompletedAt = this.experiment?.completedAt ?? null;
    this.state.experimentWs = this.experiment?.wsStats ? { experimentId: this.experiment.id,
      startedAt: this.experiment.startedAt, entryClosedAt: this.experiment.status === "RUNNING" ? null : this.experiment.entryClosesAt,
      completedAt: this.experiment.completedAt, stats: structuredClone(this.experiment.wsStats) } : null;
  }

  private pruneRuntimeBookkeeping(now = this.nowFn()): void {
    const nowMs = now.getTime();
    const before = this.retainedBookkeepingSnapshot();
    const signalCutoff = nowMs - 60_000;
    while (this.signalReceivedTimes[0] !== undefined && this.signalReceivedTimes[0] < signalCutoff) this.signalReceivedTimes.shift();
    this.executionErrors = this.executionErrors.filter((at) => nowMs - at < 3_600_000);
    const cooldown = this.riskLimits.strategyCooldownMs;
    for (const [key, at] of this.recentTrades) {
      if (nowMs - at > cooldown) this.recentTrades.delete(key);
    }
    for (const [triggerId, trigger] of [...this.activeTriggers]) {
      if (new Date(trigger.expiresAt).getTime() > nowMs) continue;
      this.forgetTrigger(triggerId);
    }
    const after = this.retainedBookkeepingSnapshot();
    this.retainedCleanupCycles.push({ atMs: nowMs, before, after });
    if (this.retainedCleanupCycles.length > 32) this.retainedCleanupCycles.splice(0, this.retainedCleanupCycles.length - 32);
  }

  private retainedBookkeepingSnapshot(): RetainedBookkeepingCounts {
    return {
      activeTriggers: this.activeTriggers.size,
      triggerProvenance: this.triggerProvenance.size,
      provenanceInvalid: this.provenanceInvalid.size,
      signalsByTrigger: this.signalsByTrigger.size,
    };
  }

  runtimeBookkeepingForTest() {
    return {
      signalReceivedTimes: this.signalReceivedTimes.length,
      executionErrors: this.executionErrors.length,
      recentTrades: this.recentTrades.size,
      activeTriggers: this.activeTriggers.size,
      triggerProvenance: this.triggerProvenance.size,
      provenanceInvalid: this.provenanceInvalid.size,
      signalsByTrigger: this.signalsByTrigger.size,
    };
  }
  retainedCleanupCyclesForTest(): RetainedCleanupCycle[] {
    return this.retainedCleanupCycles.map((cycle) => ({
      atMs: cycle.atMs,
      before: { ...cycle.before },
      after: { ...cycle.after },
    }));
  }

  seedRuntimeBookkeepingForTest(input: {
    signalReceivedTimes?: number[];
    executionErrors?: number[];
    recentTrades?: Array<[string, number]>;
    trigger?: TriggerEvent;
  }): void {
    if (input.signalReceivedTimes) this.signalReceivedTimes.push(...input.signalReceivedTimes);
    if (input.executionErrors) this.executionErrors.push(...input.executionErrors);
    if (input.recentTrades) for (const [key, at] of input.recentTrades) this.recentTrades.set(key, at);
    if (input.trigger) {
      this.activeTriggers.set(input.trigger.id, input.trigger);
      this.rememberProvenance(input.trigger.id, this.captureProvenance(input.trigger));
      this.signalsByTrigger.set(input.trigger.id, new Map());
    }
  }

  pruneRuntimeBookkeepingForTest(): void { this.pruneRuntimeBookkeeping(); }

  runtimeSupervisorSnapshot() { return this.supervisor.snapshot(); }
  journalAdmissionSnapshot() {
    return {
      active: this.journalAdmission.activeCount,
      activeCritical: this.journalAdmission.activeCriticalCount,
      activeNoncritical: this.journalAdmission.activeNoncriticalCount,
      waiting: this.journalAdmission.waitingCount,
      criticalWaiting: this.journalAdmission.criticalWaitingCount,
      noncriticalWaiting: this.journalAdmission.noncriticalWaitingCount,
      closed: this.journalAdmission.admissionClosed,
      criticalRejects: this.journalAdmission.criticalRejectionCount,
    };
  }
  overloadState() { return this.overload.state; }
  integrityFault() { return this.overload.hasIntegrityFault; }
  lastShutdownResult() {
    return { work: this.lastShutdown, journal: this.lastJournalShutdown, checkpointed: this.lastShutdownCheckpointed };
  }
  discoveryFlightSnapshot() {
    const coalescing = this.discoveryFlight.coalescingState();
    return {
      inFlight: this.discoveryFlight.inFlight,
      dirty: this.discoveryFlight.dirty,
      coalesced: this.discoveryFlight.coalesced,
      attempts: this.discoveryFlight.attempts,
      committed: this.discoveryFlight.committed,
      retainedSharedCycles: coalescing.retainedSharedCycles,
      retainedWaiters: coalescing.retainedWaiters,
    };
  }
  maintenanceFlightSnapshot() {
    const coalescing = this.maintenanceFlight.coalescingState();
    return {
      inFlight: this.maintenanceFlight.inFlight,
      dirty: this.maintenanceFlight.dirty,
      coalesced: this.maintenanceFlight.coalesced,
      attempts: this.maintenanceFlight.attempts,
      retainedSharedCycles: coalescing.retainedSharedCycles,
      retainedWaiters: coalescing.retainedWaiters,
    };
  }
  setMaintenanceHoldForTest(hold: Promise<void> | null): void { this.maintenanceHold = hold; }
  setProcessTriggerHoldForTest(hold: Promise<void> | null): void { this.processTriggerHold = hold; }
  /** Test helper: simulate discovery removing a market from live maps after decision-time capture. */
  dropLiveMarketForTest(marketId: string): void {
    this.marketsById.delete(marketId);
    this.entryQualified.delete(marketId);
    this.managedMarketIds.delete(marketId);
    this.assetByMarket.delete(marketId);
  }
  setTriggerEventPersistHoldForTest(hold: Promise<void> | null): void { this.triggerEventPersistHold = hold; }
  setGrokClaimHoldForTest(hold: Promise<void> | null): void { this.grokClaimHold = hold; }
  setIdleGrokAcceptanceHoldForTest(hold: Promise<void> | null): void { this.idleGrokAcceptanceHoldForTest = hold; }
  setStartPostCommitValidationHoldForTest(hold: Promise<void> | null): void { this.startPostCommitValidationHoldForTest = hold; }
  researchIngressInFlightForTest() { return this.researchIngressInFlight; }
  idleGrokSubmissionCountForTest(): number { return this.idleGrokSubmissions.size; }
  async completeExperimentForTest(): Promise<void> {
    await this.experimentBoundaryGate.run(async () => {
      if (!this.experiment) throw new Error("NO_EXPERIMENT");
      await this.commitExperimentStatus({
        ...this.experiment,
        status: "COMPLETED",
        completedAt: this.nowFn().toISOString(),
      });
    });
  }
  setReferenceTelemetryHoldForTest(hold: Promise<void> | null): void { this.referenceTelemetryHoldForTest = hold; }
  isTriggerActionable(triggerId: string): boolean {
    const trigger = this.activeTriggers.get(triggerId);
    const provenance = this.triggerProvenance.get(triggerId);
    if (this.provenanceInvalid.has(triggerId)) return false;
    if (!provenance) {
      if (!trigger) return false;
      return classifyTriggerProvenance(trigger) === "NONE";
    }
    return this.isProvenanceCurrentlyValid(triggerId, provenance);
  }
  triggerProvenanceForTest(triggerId: string): TriggerProvenance | undefined {
    return this.triggerProvenance.get(triggerId);
  }
  referenceEpochForTest(asset: string): number { return this.referenceEpoch.get(asset) ?? 0; }
  async flushReferenceRetryForTest(asset: string): Promise<void> {
    this.clearReferenceRetry(asset);
    await this.retryTransientReference(asset);
  }
  referenceRetryTimerCountForTest(): number { return this.referenceRetryTimers.size; }
  referenceResyncRequestedForTest(asset: string): boolean { return this.referenceResyncRequested.has(asset); }
  triggerDedupeScanCallsForTest(): number { return this.triggerEngine.dedupeScanCallsForTest(); }
  lastAdmittedMarketEpochFenceForTest(): Record<string, number> | null {
    if (!this.lastAdmittedMarketEpochFence || this.lastAdmittedMarketEpochFence.size === 0) return null;
    return Object.fromEntries(this.lastAdmittedMarketEpochFence);
  }
  applyIngressGapForTest(tokenIds: readonly string[], reason: "explicit_resubscribe" | "reconnect" | "count_limit" = "explicit_resubscribe"): void {
    this.invalidateMarketProjections(tokenIds);
    this.metrics.noteIngressGap(reason);
  }
  async ingestReferenceTickForTest(tick: ReferenceTick): Promise<void> {
    await this.onReferenceTick(tick);
  }
  referenceUnreliableForTest(asset: string): boolean { return this.referenceUnreliable.has(asset); }
  applyReferenceConnectionForTest(asset: string, state: FeedConnectionEvent["state"], reconnectCount = 1): Promise<void> {
    return this.onConnection({ feed: "reference", asset, state, at: new Date().toISOString(), reconnectCount });
  }
  activationCoordinatorForTest() { return this.activation; }
  abandonWithoutCheckpointForTest(): void { this.overload.markIntegrityFault(); }
  restingIndexOrderIdsForTest(tokenId: string): string[] { return [...this.restingIndex.orderIdsFor(tokenId)]; }
  attachReferenceWindowForTest(asset: string, maxEntries: number): void {
    this.triggerEngine.attachReferenceWindowForTest(asset, maxEntries);
  }
  referenceWindowOverflowedForTest(asset: string): boolean {
    return this.triggerEngine.referenceWindowOverflowed(asset);
  }
  referenceWindowTotalEntriesForTest(): number {
    return this.triggerEngine.referenceWindowTotalEntries();
  }
  openFirstLegBlockedByUnreliableReferenceForTest(marketId: string): boolean {
    return assetNeedsReliableReference(
      this.referenceRequired.get(marketId),
      this.referenceUnreliable,
      this.assetByMarket.get(marketId),
    );
  }
  async persistEventForTest(event: ReturnType<typeof createJournalEvent>): Promise<void> {
    // Test hook for direct journal/integrity failure scenarios. Production
    // canonical producers use persistReportInput, which applies lifecycle
    // admission before reaching safeAppend.
    await this.safeAppend(event);
  }
  reportSourceIntegrityForTest() { return [...this.reportSourceIntegrity.values()]; }
  enqueueTriggerStormForTest(triggers: TriggerEvent[]): { admitted: number; rejected: number; reasons: Record<string, number> } {
    let admitted = 0;
    let rejected = 0;
    const reasons: Record<string, number> = {};
    for (const trigger of triggers) {
      const result = this.admitTrigger(trigger);
      if (result.admitted) admitted += 1;
      else {
        rejected += 1;
        const reason = result.reason ?? "queue_full";
        reasons[reason] = (reasons[reason] ?? 0) + 1;
      }
    }
    return { admitted, rejected, reasons };
  }
  criticalReservedAvailable(): boolean { return this.supervisor.criticalReservedAvailable(); }
  heavyReadStats() { return { active: this.heavyRead.activeCount, waiting: this.heavyRead.waitingCount }; }
  retentionManagerForTest() { return this.retention; }
  async writeFinalReportForTest() { await this.writeFinalReport(); }
  reportGeneratedForTest() { return this.reportGenerated; }
  async runRetentionCycleForTest() {
    return this.retention.runCycle({
      experiment: this.experiment,
      now: this.nowFn(),
      degraded: this.state.journalDegraded || this.overload.state === "DEGRADED",
      failClosed: this.overload.state === "FAIL_CLOSED",
      databaseUp: this.state.database === "UP",
    });
  }

  private adapterTokenSynchronized(tokenId: string): boolean {
    const adapter = this.marketAdapter as PolymarketMarketDataAdapter & { isTokenSynchronized?(id: string): boolean };
    if (typeof adapter.isTokenSynchronized === "function") return adapter.isTokenSynchronized(tokenId);
    return true;
  }

  private adapterTokenEpoch(tokenId: string): number {
    const adapter = this.marketAdapter as PolymarketMarketDataAdapter & { tokenEpoch?(id: string): number };
    return typeof adapter.tokenEpoch === "function" ? adapter.tokenEpoch(tokenId) : 1;
  }

  private syncRestingIndexFromOrders(): void {
    this.restingIndex.rebuild(this.restingOrders.values());
    this.metrics.restingOrderTokenIndexEntries.set(this.restingIndex.entryCount());
  }

  private shouldSuppressStaleMarketTrigger(
    trigger: TriggerEvent,
    fence: ReadonlyMap<string, number> | null = null,
    provenance: TriggerProvenance | null = null,
  ): boolean {
    if (provenance) {
      if (provenance.kind === "NONE") return false;
      if (!this.isProvenanceCurrentlyValid(trigger.id, provenance)) return true;
    }
    if (!isMarketDataDerivedTrigger(trigger) && classifyTriggerProvenance(trigger) === "NONE") return false;
    if (trigger.marketId && this.marketHasGappedToken(trigger.marketId)) return true;
    return this.marketEpochFenceStale(fence);
  }

  private provenanceForGenerated(
    trigger: TriggerEvent,
    plane: "MARKET" | "REFERENCE",
    asset?: string,
  ): TriggerProvenance {
    const classified = classifyTriggerProvenance(trigger);
    if (classified === "NONE") return { kind: "NONE" };
    if (classified === "REFERENCE" || plane === "REFERENCE") {
      const resolved = asset ?? trigger.asset;
      if (!resolved) return { kind: "NONE" };
      return { kind: "REFERENCE", asset: resolved, epoch: this.referenceEpoch.get(resolved) ?? 0 };
    }
    const fence = this.captureMarketEpochFence({ ...trigger, source: "polymarket", kind: trigger.kind === "NEWS_INFORMATION" ? "BOOK_IMBALANCE" : trigger.kind });
    return { kind: "MARKET", tokenEpochs: fence ? Object.fromEntries(fence) : {} };
  }

  private captureProvenance(trigger: TriggerEvent): TriggerProvenance {
    const plane = classifyTriggerProvenance(trigger);
    if (plane === "NONE") return { kind: "NONE" };
    if (plane === "REFERENCE") {
      const asset = trigger.asset;
      if (!asset) return { kind: "NONE" };
      return { kind: "REFERENCE", asset, epoch: this.referenceEpoch.get(asset) ?? 0 };
    }
    const fence = this.captureMarketEpochFence({ ...trigger, source: "polymarket" });
    return { kind: "MARKET", tokenEpochs: fence ? Object.fromEntries(fence) : {} };
  }

  private rememberProvenance(triggerId: string, provenance: TriggerProvenance): void {
    this.triggerProvenance.set(triggerId, provenance);
  }

  private markProvenanceInvalid(triggerId: string): void {
    this.provenanceInvalid.add(triggerId);
    // An invalidated trigger can no longer receive research wakeups. Release
    // its bounded reservations while retaining the trigger record so stale
    // submissions still receive the explicit provenance-stale response.
    this.activation.forgetTrigger(triggerId);
  }

  private forgetTrigger(triggerId: string): void {
    this.activation.forgetTrigger(triggerId);
    this.activeTriggers.delete(triggerId);
    this.triggerProvenance.delete(triggerId);
    this.provenanceInvalid.delete(triggerId);
    this.signalsByTrigger.delete(triggerId);
  }

  private clearResearchActivationState(): void {
    for (const triggerId of [...this.activeTriggers.keys()]) this.forgetTrigger(triggerId);
    this.hotResearchCache.clear();
    this.activation.clearAll();
  }

  private isProvenanceCurrentlyValid(triggerId: string, provenance: TriggerProvenance): boolean {
    if (this.provenanceInvalid.has(triggerId)) return false;
    if (provenance.kind === "NONE") return true;
    if (provenance.kind === "MARKET") {
      const tokens = Object.entries(provenance.tokenEpochs);
      if (tokens.length === 0) return true;
      for (const [tokenId, epoch] of tokens) {
        if (!this.adapterTokenSynchronized(tokenId) || this.adapterTokenEpoch(tokenId) !== epoch) {
          this.markProvenanceInvalid(triggerId);
          return false;
        }
      }
      return true;
    }
    if (this.referenceUnreliable.has(provenance.asset) || (this.referenceEpoch.get(provenance.asset) ?? 0) !== provenance.epoch) {
      this.markProvenanceInvalid(triggerId);
      return false;
    }
    return true;
  }

  private noteReferenceLogicalLoss(asset: string): void {
    this.referenceUnreliable.add(asset);
    if (this.referenceLogicalGapped.has(asset)) return;
    this.referenceLogicalGapped.add(asset);
    this.referenceEpoch.set(asset, (this.referenceEpoch.get(asset) ?? 0) + 1);
  }

  /**
   * Market-data research is fenced by the trusted tokens that exist at admission.
   * A sibling still AWAITING_BOOK does not drop the fence. At execution every
   * captured token must still be SYNCED at that exact epoch. Risk/system/
   * settlement stay unfenced. If no token is trusted yet, fence is null and
   * MCP/research registration without books remains possible; that work is not
   * treated as originating from trusted market data.
   */
  private captureMarketEpochFence(trigger: TriggerEvent): Map<string, number> | null {
    if (!isMarketDataDerivedTrigger(trigger) || !trigger.marketId) return null;
    const tokens = this.tokensForMarket(trigger.marketId);
    if (tokens.length === 0) return null;
    const fence = new Map<string, number>();
    for (const tokenId of tokens) {
      if (!this.adapterTokenSynchronized(tokenId)) continue;
      fence.set(tokenId, this.adapterTokenEpoch(tokenId));
    }
    return fence.size > 0 ? fence : null;
  }

  private marketEpochFenceStale(fence: ReadonlyMap<string, number> | null): boolean {
    if (!fence || fence.size === 0) return false;
    for (const [tokenId, epoch] of fence) {
      if (!this.adapterTokenSynchronized(tokenId) || this.adapterTokenEpoch(tokenId) !== epoch) {
        return true;
      }
    }
    return false;
  }

  private tokensForMarket(marketId: string): string[] {
    const discovered = this.marketsById.get(marketId)?.tokens.map((token) => token.tokenId) ?? [];
    const pair = this.snapshots.get(marketId);
    return uniqueStrings([
      ...discovered,
      pair?.UP?.tokenId,
      pair?.DOWN?.tokenId,
      ...[...this.state.marketSnapshots.values()].filter((item) => item.marketId === marketId).map((item) => item.tokenId),
    ].filter((item): item is string => Boolean(item)));
  }

  private marketHasGappedToken(marketId: string): boolean {
    const adapter = this.marketAdapter as PolymarketMarketDataAdapter & {
      tokenSyncState?(id: string): string;
    };
    if (typeof adapter.tokenSyncState !== "function") return false;
    const discovered = this.marketsById.get(marketId)?.tokens.map((token) => token.tokenId) ?? [];
    const pair = this.snapshots.get(marketId);
    const tokens = uniqueStrings([
      ...discovered,
      pair?.UP?.tokenId,
      pair?.DOWN?.tokenId,
      ...[...this.state.marketSnapshots.values()].filter((item) => item.marketId === marketId).map((item) => item.tokenId),
    ].filter((item): item is string => Boolean(item)));
    return tokens.some((tokenId) => adapter.tokenSyncState!(tokenId) === "GAPPED");
  }

  private tokenLocation(tokenId: string): { marketId: string; outcome: Outcome } | null {
    const snapshot = this.state.marketSnapshots.get(tokenId);
    if (snapshot) return { marketId: snapshot.marketId, outcome: snapshot.outcome };
    for (const market of this.marketsById.values()) {
      const token = market.tokens.find((item) => item.tokenId === tokenId);
      if (token) return { marketId: market.id, outcome: token.outcome };
    }
    return null;
  }

  private clearMarketPairProjections(marketId: string): void {
    this.triggerEngine.forgetMarketBooks(marketId);
    this.snapshots.delete(marketId);
    for (const [tokenId, snapshot] of [...this.state.marketSnapshots.entries()]) {
      if (snapshot.marketId !== marketId) continue;
      this.state.marketSnapshots.delete(tokenId);
      this.snapshotEpoch.delete(tokenId);
    }
  }

  private invalidateMarketProjections(tokenIds: readonly string[]): void {
    const markets = new Set<string>();
    for (const tokenId of tokenIds) {
      const loc = this.tokenLocation(tokenId);
      this.state.marketSnapshots.delete(tokenId);
      this.snapshotEpoch.delete(tokenId);
      if (loc) markets.add(loc.marketId);
    }
    for (const marketId of markets) this.clearMarketPairProjections(marketId);
    this.updateReady();
  }

  private markReferenceUnreliable(asset: string): void {
    this.referenceUnreliable.add(asset);
  }

  private requestReferenceResync(asset: string): void {
    if (this.referenceResyncInFlight.has(asset)) return;
    if (this.referenceRetryTimers.has(asset)) return;
    if (this.referenceResyncRequested.has(asset)) return;
    this.referenceResyncInFlight.add(asset);
    this.state.invalidateReferenceContext(asset);
    try {
      this.referenceRegistry.stop(asset);
    } catch { /* scripted registries may no-op */ }
    void this.reconcileReferenceFeed(asset).catch((error) => {
      this.logger.error({ err: error, asset }, "reference overflow resync failed");
      this.referenceResyncRequested.delete(asset);
      if (this.shouldRetryReferenceStart(asset)) this.scheduleReferenceRetry(asset, 50, 60_000);
    }).finally(() => {
      this.referenceResyncInFlight.delete(asset);
    });
  }

  private siblingTokenId(marketId: string, outcome: Outcome): string | undefined {
    const sibling: Outcome = outcome === "UP" ? "DOWN" : "UP";
    const pair = this.snapshots.get(marketId);
    if (pair?.[sibling]?.tokenId) return pair[sibling]!.tokenId;
    return this.marketsById.get(marketId)?.tokens.find((token) => token.outcome === sibling)?.tokenId;
  }

  private syncIngressMetrics(): void {
    const adapter = this.marketAdapter as PolymarketMarketDataAdapter & { ingressSnapshot?(): { depth: number; bytes: number; highWaterEvents: number } };
    if (typeof adapter.ingressSnapshot !== "function") return;
    this.metrics.noteIngressSnapshot(adapter.ingressSnapshot());
    this.metrics.marketUnsynchronizedTokens.set(this.unsynchronizedTokenCount());
  }
}

export function parseEnv(env: NodeJS.ProcessEnv): EngineEnv { return parseEngineEnv(env); }
export { RUNTIME_BOUNDS } from "./runtime-supervisor.js";
function splitList(value: string): string[] { return value.split(",").map((item) => item.trim()).filter(Boolean); }
function uniqueStrings(values: readonly string[]): string[] { return [...new Set(values.filter(Boolean))]; }
function dateOrNull(value: string | null | undefined): Date | null { return value ? new Date(value) : null; }
function delay(ms: number): Promise<void> { return new Promise((resolve) => setTimeout(resolve, ms)); }

function boundedTrigger(trigger: TriggerEvent): Record<string, unknown> {
  return {
    id: trigger.id,
    traceId: trigger.traceId,
    marketId: trigger.marketId,
    asset: trigger.asset ?? null,
    kind: trigger.kind,
    severity: trigger.severity,
    score: trigger.score,
    evidenceIds: (trigger.evidenceIds ?? []).slice(0, 32),
    sourceEventIds: trigger.sourceEventIds.slice(0, 32),
    createdAt: trigger.createdAt,
    expiresAt: trigger.expiresAt,
  };
}

function boundedMarketSnapshot(snapshot: MarketSnapshot): Record<string, unknown> {
  return {
    eventId: snapshot.eventId,
    traceId: snapshot.traceId,
    marketId: snapshot.marketId,
    tokenId: snapshot.tokenId,
    outcome: snapshot.outcome,
    asset: snapshot.asset ?? null,
    bids: snapshot.bids.slice(0, 20).map((level) => ({ price: level.price, quantity: level.quantity })),
    asks: snapshot.asks.slice(0, 20).map((level) => ({ price: level.price, quantity: level.quantity })),
    lastTradePrice: snapshot.lastTradePrice,
    tickSize: snapshot.tickSize,
    minOrderSize: snapshot.minOrderSize,
    sourceTimestamp: snapshot.sourceTimestamp,
    receivedAt: snapshot.receivedAt,
    sequence: snapshot.sequence,
    hash: snapshot.hash,
  };
}

function deterministicMarketFeatures(snapshots: readonly Record<string, unknown>[]): Record<string, unknown> {
  const levels = snapshots.flatMap((snapshot) => {
    const bids = Array.isArray(snapshot.bids) ? snapshot.bids as Array<{ price?: unknown; quantity?: unknown }> : [];
    const asks = Array.isArray(snapshot.asks) ? snapshot.asks as Array<{ price?: unknown; quantity?: unknown }> : [];
    const bid = bids[0] ? Number(bids[0].price) : Number.NaN;
    const ask = asks[0] ? Number(asks[0].price) : Number.NaN;
    const bidDepth = bids.reduce((sum, level) => sum + (Number(level.quantity) || 0), 0);
    const askDepth = asks.reduce((sum, level) => sum + (Number(level.quantity) || 0), 0);
    return [{ bid, ask, bidDepth, askDepth }];
  });
  const valid = levels.filter((item) => Number.isFinite(item.bid) && Number.isFinite(item.ask));
  const bestBid = valid.length > 0 ? Math.max(...valid.map((item) => item.bid)) : null;
  const bestAsk = valid.length > 0 ? Math.min(...valid.map((item) => item.ask)) : null;
  const bidDepth = levels.reduce((sum, item) => sum + item.bidDepth, 0);
  const askDepth = levels.reduce((sum, item) => sum + item.askDepth, 0);
  return {
    bestBid, bestAsk, spread: bestBid !== null && bestAsk !== null ? bestAsk - bestBid : null,
    mid: bestBid !== null && bestAsk !== null ? (bestAsk + bestBid) / 2 : null,
    bidDepth, askDepth, imbalance: bidDepth + askDepth > 0 ? (bidDepth - askDepth) / (bidDepth + askDepth) : null,
  };
}

function boundedPosition(position: Position): Record<string, unknown> {
  return {
    marketId: position.marketId,
    upQuantity: position.upQuantity,
    downQuantity: position.downQuantity,
    averageUpCost: position.averageUpCost,
    averageDownCost: position.averageDownCost,
    pairedQuantity: position.pairedQuantity,
    unpairedOutcome: position.unpairedOutcome,
    unpairedQuantity: position.unpairedQuantity,
    combinedPairCost: position.combinedPairCost,
    openedAt: position.openedAt,
    updatedAt: position.updatedAt,
  };
}

function boundedReference(reference: {
  asset: string; symbol: string | null; venue: string | null; latestPrice: string | null;
  lastMessageAt: string | null; freshness: string; freshnessMs: number | null;
  recentMoveBps: number | null; recentVolatilityBps: number | null;
}): Record<string, unknown> {
  return {
    asset: reference.asset,
    symbol: reference.symbol,
    venue: reference.venue,
    latestPrice: reference.latestPrice,
    lastMessageAt: reference.lastMessageAt,
    freshness: reference.freshness,
    freshnessMs: reference.freshnessMs,
    recentMoveBps: reference.recentMoveBps,
    recentVolatilityBps: reference.recentVolatilityBps,
  };
}

function boundedStrategyRiskSummary(item: {
  type: string; at: string; traceId: string; marketId: string | null; asset?: string | null; detail: Record<string, unknown>;
}): Record<string, unknown> {
  const allowed = [
    "direction", "confidence", "estimatedEdge", "consensusDirection", "consensusConfidence", "agentAgreement",
    "vetoes", "orderSizeUsd", "marketExposureUsd", "assetExposureUsd", "totalExposureUsd", "unhedgedExposureUsd",
    "reason", "status", "intent", "outcome", "pairedQuantity", "unpairedQuantity",
  ];
  const detail: Record<string, unknown> = {};
  for (const key of allowed) {
    const value = item.detail[key];
    if (typeof value === "string" || typeof value === "number" || typeof value === "boolean" || value === null) detail[key] = value;
    else if (Array.isArray(value)) detail[key] = value.slice(0, 8).filter((entry) => typeof entry === "string" || typeof entry === "number" || typeof entry === "boolean");
  }
  return { type: item.type, at: item.at, traceId: item.traceId, marketId: item.marketId, asset: item.asset ?? null, detail };
}

export function isRiskHedgeSettlementTrigger(kind: string): boolean {
  return isRiskHedgeSettlementKind(kind);
}

function assetNeedsReliableReference(
  required: boolean | undefined,
  unreliable: ReadonlySet<string>,
  asset: string | null | undefined,
): boolean {
  return required === true && Boolean(asset) && unreliable.has(asset!);
}

export function collectMarketFeedAt(
  pair: Partial<Record<Outcome, MarketSnapshot>> | undefined,
  outcome: Outcome,
  intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG" | "GUARDIAN_PROTECTIVE_SELL",
  fallbackReceivedAt?: string | Date | null,
): Date | null {
  const required = pair?.[outcome];
  if (required?.receivedAt) return new Date(required.receivedAt);
  if (intent === "HEDGE_SECOND_LEG" && fallbackReceivedAt) return new Date(fallbackReceivedAt);
  if (fallbackReceivedAt) return new Date(fallbackReceivedAt);
  return null;
}

export function projectableLegsFromPositions(
  positions: Iterable<Position>,
  assetByMarket: ReadonlyMap<string, string | null>,
): Array<{ marketId: string; outcome: Outcome; quantity: string; avgCost: string; asset: string | null }> {
  const legs: Array<{ marketId: string; outcome: Outcome; quantity: string; avgCost: string; asset: string | null }> = [];
  for (const position of positions) {
    const asset = assetByMarket.get(position.marketId) ?? null;
    if (Number(position.upQuantity) > 0) {
      legs.push({ marketId: position.marketId, outcome: "UP", quantity: position.upQuantity, avgCost: position.averageUpCost, asset });
    }
    if (Number(position.downQuantity) > 0) {
      legs.push({ marketId: position.marketId, outcome: "DOWN", quantity: position.downQuantity, avgCost: position.averageDownCost, asset });
    }
  }
  return legs;
}
