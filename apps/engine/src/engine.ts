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
  type RescueCandidateEvaluation,
  type RiskReductionCandidateEconomics,
  defaultNativeDecisionConfig,
  evaluateNativeDecision,
  NATIVE_DETERMINISTIC_AUTHORITY,
  parseEngineEnv,
  executePaperTaker,
  experimentManifestSchema,
  qualificationConfigSchema,
  refreshExperiment,
  riskLimitsSchema,
  startExperiment,
  triggerConfigSchema,
  ledgerStateSchema,
  positionSchema,
  createRestingMakerOrder,
  restingPaperOrderSchema,
  evolveMakerOrder,
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
  NativeV2ShadowFeatureExtractor,
  evaluateNativeV2ShadowFairProbability,
  LearningSampleStore,
  tryObserveLearningSample,
  LearningRuntime,
  type LearnedV2ShadowEvaluation,
  NativeV2CounterfactualStore,
  counterfactualDataDir,
  nativeV2CounterfactualActionKey,
  NATIVE_V2_COUNTERFACTUAL_STATE_BASIS,
  NativeV2ShadowOpportunityRanker,
  evaluateNativeV2ShadowMarketOpportunity,
  buildNativeV2ShadowExecutionIntent,
  NATIVE_V2_SHADOW_EXECUTION_VERSION,
  type NativeV2ShadowFairProbability,
  type NativeV2ShadowFeatureVector,
  type NativeV2ShadowMarketOpportunity,
  type NativeV2ShadowOpportunityCandidate,
  type NativeV2ShadowOpportunityRanking,
  DEFAULT_FIRST_LEG_RISK_CAP_USD,
  rolesForTriggerKind,
  type RiskReductionSelection,
  type RiskReductionSelectionReason,
  type HedgePositionFacts,
} from "@pm/core";
import {
  CHECKPOINT_SCHEMA_VERSION,
  CheckpointFenceError,
  createJournalEvent,
  derivedAuditEventId,
  DurableCursorBehindError,
  IdempotencyConflictError,
  isCanonicalFinalReport,
  JournalAppendAbortedError,
  isJournalAppendAbortedError,
  isJournalAppendDeadlineError,
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
import { NativeV2ShadowStore } from "./native-v2-shadow-store.js";
import { ensureReportGeneratedAudit, persistCanonicalFinalReport, persistVerifiedAnalyticsRollup, streamExperimentSummary, summaryFromDurableReport } from "./final-report.js";
import { writeExperimentSummary } from "./report.js";
import { retentionConfigFromEngineEnv, RetentionManager } from "./retention-manager.js";
import { createSourceSealEvent, loadSourceSeal, remainingSealMs, SOURCE_SEAL_TIMEOUT_MS, isSourceSealExemptType } from "./source-seal.js";
import {
  boundIntegrityFaults,
  createIntegrityFaultEvent,
  createIntegrityRepairEvent,
  integrityDurableOperationKey,
  INTEGRITY_OVERFLOW_EVENT_ID,
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
  validatePostReplayInvariants,
  type DerivedAuditRecord,
  type RecoverableEngineState,
} from "./recovery.js";
import {
  DirectJournalAdmission,
  FiniteSemaphore,
  JournalAdmissionRejected,
  OverloadLatch,
  RUNTIME_BOUNDS,
  RuntimeSupervisor,
  SingleFlight,
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

class FirstLegLifecycleGuardRejectedError extends Error {
  constructor(readonly marketId: string, readonly reason: string) {
    super(`FIRST_LEG_LIFECYCLE_REJECTED:${marketId}:${reason}`);
  }
}

class RiskReductionLifecycleGuardRejectedError extends Error {
  constructor(
    readonly marketId: string,
    readonly intent: "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG",
    readonly reason: string,
  ) {
    super(`RISK_REDUCTION_LIFECYCLE_REJECTED:${marketId}:${intent}:${reason}`);
  }
}

function isJournalAdmissionRejectedError(error: unknown): boolean {
  return error instanceof JournalAdmissionRejected
    || (Boolean(error) && typeof error === "object" && (error as { name?: string }).name === "JournalAdmissionRejected");
}

const IDLE_GROK_IDEMPOTENCY_MAX = 4_096;
const IDLE_GROK_IDEMPOTENCY_TTL_MS = 5 * 60_000;
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
 * At most one durable row per market per 60s, aligned to the maintenance
 * cadence. Pure observability: never affects trading decisions or timing.
 */
const PROTECTIVE_DE_RISK_SUPPRESS_EMIT_MIN_MS = 60_000;
type IdleGrokSubmission = { observedAt: number; pending: boolean };
type RetainedBookkeepingCounts = {
  activeTriggers: number;
  triggerProvenance: number;
  provenanceInvalid: number;
  signalsByTrigger: number;
};
type RetainedCleanupCycle = { atMs: number; before: RetainedBookkeepingCounts; after: RetainedBookkeepingCounts };
type GrokSubmissionResult = {
  status: "accepted" | "duplicate" | "stale" | "unsupported_evidence" | "unknown_trigger" | "trace_mismatch" | "market_mismatch" | "asset_mismatch" | "replay_rejected" | "malformed" | "identity_mismatch" | "source_sealed";
  signalId?: string;
};

type RiskReductionUnhedgedReason = "MAX_UNHEDGED_DURATION" | "PRE_EXPIRY_SAFETY";

type ProtectiveDeRiskAudit = {
  deRiskReason: RiskReductionUnhedgedReason;
  /**
   * Observability improvement (§44): distinguishes which deadline bound made
   * the position protective-eligible — the hard unhedged duration
   * (MAX_UNHEDGED_DURATION) or the expiry safety window (PRE_EXPIRY_SAFETY).
   * Behavior is identical to the prior single label; this only removes the
   * diagnostic ambiguity.
   */
  eligibilityReason: RiskReductionUnhedgedReason;
  unhedgedStartedAt: string;
  unhedgedDurationMs: number;
  attemptedAction: "SELL_UNPAIRED" | "RESCUE_SECOND_LEG" | "NONE";
  requestedQty: string;
  executableQty: string;
  executionPrice: string | null;
  residualQuantity: string;
  residualExposure: string;
  retryCount: number;
  finalState: "ORDER_ADMITTED" | "RISK_VETO" | "RESIDUAL_UNHEDGED" | "RISK_NEUTRALIZED" | "EXECUTION_ERROR" | "STATE_CHANGED";
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
};

type NativeV2ShadowExecutionCapture = {
  trigger: TriggerEvent;
  featureVector: NativeV2ShadowFeatureVector;
  fair: NativeV2ShadowFairProbability;
  opportunity: NativeV2ShadowMarketOpportunity;
  ranking: NativeV2ShadowOpportunityRanking;
  native: NativeDecision;
  capturedAt: string;
  experimentId: string | null;
  experimentStatus: string | null;
  gitSha: string | null;
  asset: string | null;
  currentSnapshot: MarketSnapshot | null;
  selected: NativeV2ShadowOpportunityCandidate | null;
  evidenceIds: string[];
  preflightBlockers: string[];
  positions: Position[];
  assetByMarketEntries: Array<[string, string | null]>;
  ledger: LedgerState;
  availableCashUsd: string;
  marketFeedAt: string | null;
  referenceFeedAt: string | null;
  btcFeedAt: string | null;
  referenceRequired: boolean;
  databaseAvailable: boolean;
  timestampDisorder: boolean;
  referenceProviderBlocked: boolean;
  marketEntryQualified: boolean;
  lastStrategyTradeAt: string | null;
  duplicateTrade: boolean;
  experimentEntryOpen: boolean;
  executionErrorsLastHour: number;
  shuttingDown: boolean;
  /**
   * Authoritative YES/NO mapping captured at decision-time reserve.
   * evaluateNativeV2ShadowExecution must not re-resolve from live marketsById.
   */
  settlementLookup: BinarySettlementLookup | null;
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
  private readonly nativeDecisionConfig: NativeDecisionConfig;
  private readonly nativeV2Shadow = new NativeV2ShadowFeatureExtractor();
  private readonly nativeV2OpportunityRanker = new NativeV2ShadowOpportunityRanker();
  private readonly nativeV2ShadowStore = new NativeV2ShadowStore();
  private readonly learningStore: LearningSampleStore | null;
  /** PR5 learned V2 shadow runtime — advisory only; never V1 authority. */
  private readonly learningRuntime: LearningRuntime;
  /** Isolated V2 shadow portfolio — never mutates V1 ledger/cash/positions. */
  private readonly counterfactualStore: NativeV2CounterfactualStore;
  private nativeV2ShadowExecutionBusy = false;
  private lastLearningSettlementPollAt = 0;
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
  private readonly protectiveDeRiskAttempts = new Map<string, number>();
  /** Earliest next durable de-risk evaluation per market. Ephemeral by design. */
  private readonly protectiveDeRiskNextAttemptAt = new Map<string, number>();
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
  private lastJournalShutdown: JoinResult | null = null;
  private lastShutdownCheckpointed = false;
  private maintenanceHold: Promise<void> | null = null;
  private processTriggerHold: Promise<void> | null = null;
  private nativeV2ShadowEvalHoldForTest: Promise<void> | null = null;
  private triggerEventPersistHold: Promise<void> | null = null;
  private sourceSealing = false;
  private sourceSealed = false;
  private sourceSealTimeoutMs = SOURCE_SEAL_TIMEOUT_MS;
  private sourceSealGeneration = 0;
  private sourceSealDeadlineAt = 0;
  private sourceSealPersistCancelled = false;
  private sourceSealPersistAttempt = 0;
  private sourceSealPersistHold: Promise<void> | null = null;
  private sourceSealAbort: AbortController | null = null;
  private sourceSealPersistInFlight = false;
  private sourceSealFlight: Promise<{ sealed: boolean; reason: string | null }> | null = null;
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
    private readonly logger: { error(bindings: object, message: string): void },
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
    this.triggerEngine = new TriggerEngine(this.triggerConfig, this.nowFn, this.metrics.triggerObserver());
    this.nativeDecisionConfig = {
      ...defaultNativeDecisionConfig,
      referenceRapidMoveBps: this.triggerConfig.referenceRapidMoveBps,
    };
    this.riskLimits = riskLimitsSchema.parse({ maxExternalSignalsPerMinute: env.GROK_MCP_RATE_LIMIT_PER_MINUTE });
    this.marketAdapter = adapters?.market ?? new PolymarketMarketDataAdapter(undefined, { hydrateMinOrderSizeFromRest: true });
    if (this.marketAdapter instanceof PolymarketMarketDataAdapter) {
      this.marketAdapter.setNormalizeObserver(this.metrics.normalizeObserver());
      this.marketAdapter.setMinOrderSizeHydrationObserver(this.metrics.minOrderSizeHydrationObserver());
      this.marketAdapter.setIngressObserver({
        onChange: (snapshot) => this.metrics.noteIngressSnapshot(snapshot),
      });
    }
    this.legacyBtcAdapter = adapters?.btc ?? null;
    this.referenceRegistry = adapters?.referenceRegistry ?? new ReferenceFeedRegistry(
      env.REFERENCE_WS_BASE_URL, env.REFERENCE_REST_BASE_URL, this.riskLimits.staleFeedTimeoutMs, undefined,
      createRestProbe(env.REFERENCE_REST_BASE_URL),
    );
    this.resolutionFetcher = adapters?.resolutionFetcher ?? ((id) => this.marketAdapter.fetchPublicMarket(id));
    this.learningStore = env.LEARNING_ENABLED
      ? new LearningSampleStore({
        enabled: true,
        dataDir: env.LEARNING_DATA_DIR,
        maxBytes: env.LEARNING_MAX_BYTES,
        checkpointSeconds: env.LEARNING_CHECKPOINT_SECONDS,
        gitSha: process.env.GIT_SHA ?? process.env.FLY_IMAGE_REF ?? null,
      })
      : null;
    try {
      this.learningStore?.init();
    } catch {
      // Learning storage must never block engine startup (V1/V2/risk/execution).
    }
    this.learningRuntime = new LearningRuntime({
      shadowEnabled: env.LEARNING_SHADOW_ENABLED,
      learningDataDir: env.LEARNING_DATA_DIR,
      modelArtifactPath: env.LEARNING_SHADOW_MODEL_PATH ?? null,
      modelId: env.LEARNING_SHADOW_MODEL_ID ?? null,
      paperAuthority: {
        paperAuthorityEnabled: env.LEARNING_PAPER_AUTHORITY_ENABLED,
        approvedModelId: env.LEARNING_PAPER_AUTHORITY_MODEL_ID ?? null,
      },
    });
    // Defense in depth: LearningRuntime.init() is fail-soft, and the engine
    // boundary also refuses to let optional learned state block V1 startup.
    try {
      this.learningRuntime.init();
    } catch {
      // Learned shadow remains unavailable; V1/V2/risk/execution continue.
    }
    // V2 counterfactual portfolio: durable under LEARNING_DATA_DIR/counterfactual.
    // Always constructed for Phase-4 shadow statefulness; isolated from V1.
    this.counterfactualStore = new NativeV2CounterfactualStore({
      enabled: true,
      durable: true,
      dataDir: counterfactualDataDir(env.LEARNING_DATA_DIR),
      startingBalance: env.PAPER_START_BALANCE,
      // CF has its own bounded log under LEARNING_DATA_DIR/counterfactual (always durable).
      // Cap matches the learning volume contract; CF failures never affect V1 authority.
      maxBytes: env.LEARNING_MAX_BYTES,
    });
    try {
      this.counterfactualStore.init();
    } catch {
      // Counterfactual storage must never block engine startup (V1/V2/risk/execution).
    }
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
    });
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
    await this.restoreSourceSealState();
    await this.restoreReportSourceIntegrity();
    this.state.recoveryComplete = true;
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
    if (this.discoveryTimer) clearInterval(this.discoveryTimer);
    this.discoveryTimer = null;
    if (this.experiment?.status === "RUNNING" || this.experiment?.status === "ENTRY_CLOSED") {
      await this.accrueInfrastructureCost();
    }
    const deadline = this.shutdownJoinDeadlineMs;
    const started = Date.now();
    this.lastShutdown = await this.supervisor.join(deadline);
    this.journalAdmission.closeAdmission();
    const remaining = Math.max(0, deadline - (Date.now() - started));
    this.lastJournalShutdown = await this.journalAdmission.join(remaining);
    const latestDurable = await this.journal.latestDurableSequence();
    const cleanDrain = Boolean(
      this.lastShutdown.drained
      && this.lastJournalShutdown.drained
      && !this.lastShutdown.timedOut
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
      unresolvedOwnedWork: (this.lastShutdown.unresolved ?? 0) + (this.lastJournalShutdown.unresolved ?? 0),
      shutdownTimedOut: this.lastShutdown.timedOut || this.lastJournalShutdown.timedOut,
      checkpointed: this.lastShutdownCheckpointed,
    });
  }

  noteMcpRequest(tool: string | null, triggerId?: string): void {
    if (triggerId) this.activation.noteMcp([triggerId]);
    else this.activation.noteUnattributedMcp(tool);
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
    intent?: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG",
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

  async beginExperiment(): Promise<Record<string, unknown>> {
    return this.experimentBoundaryGate.run(async () => {
      if (this.experiment?.status === "RUNNING" || this.experiment?.status === "ENTRY_CLOSED") {
        return { ok: false, blockers: ["EXPERIMENT_ALREADY_ACTIVE"], experiment: this.experiment };
      }
      if (this.experiment?.status === "FAILED") {
        return { ok: false, blockers: ["EXPERIMENT_ALREADY_COMPLETED"], experiment: this.experiment };
      }
      if (this.experiment?.status === "COMPLETED") {
        const previousReport = await this.journal
          .loadExperimentReport(this.experiment.id, "final")
          .catch(() => null);
        if (!previousReport || !isCanonicalFinalReport(previousReport)) {
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
      const next = startExperiment({
        id: undefined, durationHours: this.env.EXPERIMENT_DURATION_HOURS, strategyVersion: this.strategy.version,
        riskConfiguration: this.riskLimits,
        modelConfiguration: {
          provider: "native_deterministic", version: NATIVE_DETERMINISTIC_MODEL_VERSION,
          localExecution: true, externalInference: false, authority: NATIVE_DETERMINISTIC_AUTHORITY,
        },
        startingBalance: this.ledger.snapshot().startingBalance, systemVersion: "0.1.0",
        gitSha: process.env.GIT_SHA ?? process.env.FLY_IMAGE_REF ?? "unknown", now,
        closePolicy: this.env.EXPERIMENT_CLOSE_POLICY,
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
            this.abortSourceSealPersist();
            this.sourceSealed = false;
            this.sourceSealing = false;
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
        this.nativeV2ShadowStore.reset();
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
        // Manual/operator close-entries is another RUNNING → ENTRY_CLOSED
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
      onMinOrderSizeHydrated: (event) => this.onMinOrderSizeHydrated(event),
      onConnection: (event) => this.onConnection(event),
      onError: (error) => this.onFeedError("polymarket", error),
      onAnomaly: (anomaly) => this.onPolymarketAnomaly(anomaly),
      onGap: (event) => {
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
      await this.maintainExperiment();
      await this.retention.runCycle({
        experiment: this.experiment,
        now: this.nowFn(),
        degraded: this.state.journalDegraded || this.overload.state === "DEGRADED" || this.state.telemetryDegraded,
        failClosed: this.overload.state === "FAIL_CLOSED",
        databaseUp: this.state.database === "UP",
      });
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
      qualificationConfigSchema.parse({ maxQualifiedMarkets: this.env.MAX_QUALIFIED_MARKETS }),
    );
    if (generation < this.discoveryGenerationApplied) return;
    const previousMarketIds = new Set(this.marketsById.keys());
    const previousAssets = new Set([...this.assetByMarket.values()].filter((asset): asset is string => Boolean(asset)));
    const previousTokens = [...this.tokenIds];
    const obligations = this.obligationMarketIds();
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
   * Returns null when the identity cannot be resolved — callers fail closed.
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
      this.snapshots.delete(marketId);
      this.triggerEngine.dropMarket(marketId);
      this.nativeV2Shadow.dropMarket(marketId);
      this.nativeV2OpportunityRanker.dropMarket(marketId);
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
      this.nativeV2Shadow.dropAsset(asset);
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
      experiment: this.experiment,
      restingOrders: this.restingOrders,
      makerReservations: this.makerReservations,
      settledMarkets: this.settledMarkets,
      lastInfrastructureAccrualAt: this.lastInfrastructureAccrualAt,
    };
  }

  private adoptRecoveredBag(bag: RecoverableEngineState): void {
    this.experiment = bag.experiment;
    this.lastInfrastructureAccrualAt = bag.lastInfrastructureAccrualAt;
    this.syncRecoveredProjection();
  }

  private syncRecoveredProjection(): void {
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
    buildPayload: (occurredAt: Date) => Record<string, unknown>;
    occurredAt?: Date;
    beforeCommit?: () => void;
    afterApply?: (transition: DurableTransition) => Promise<void>;
    checkpoint?: boolean;
  }): Promise<DurableCommitResult> {
    return this.durableGate.run(async () => {
      const occurredAt = input.occurredAt ?? this.nowFn();
      this.crashInjector.maybeCrash("BEFORE_DURABLE_COMMIT", input.operationKey);
      let commit: DurableCommitResult;
      try {
        const payload = await this.resolveDurablePayload(input, occurredAt);
        input.beforeCommit?.();
        commit = await this.journal.commitDurableTransition({
          operationKey: input.operationKey,
          type: input.type,
          aggregateType: input.aggregateType,
          aggregateId: input.aggregateId,
          payload,
          occurredAt,
        }, this.lastAppliedDurableCursor);
        this.metrics.noteDurableCommit(input.type, commit.inserted);
      } catch (error) {
        if (error instanceof CrashInjectedError) throw error;
        if (error instanceof ExperimentStartGuardRejectedError) throw error;
        if (error instanceof FirstLegLifecycleGuardRejectedError) throw error;
        // A stale/raced hedge-vs-protective fill is an expected lifecycle
        // rejection, not durability corruption. It must rethrow as a normal
        // STATE_CHANGED rejection without latching the integrity fault; only
        // genuine durable/idempotency/cursor faults fail closed below.
        if (error instanceof RiskReductionLifecycleGuardRejectedError) throw error;
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
      if (commit.transition.sequence > this.lastAppliedDurableCursor + 1) {
        await this.replayDurableTail(this.lastAppliedDurableCursor);
      }
      const applied = this.applyCommitted(commit.transition);
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
  ): Promise<Record<string, unknown>> {
    const computed = input.buildPayload(occurredAt);
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

  private async emitStoredDerivedAudit(row: DurableTransition): Promise<void> {
    const records = parseDerivedAudit(row.payload);
    const occurredAt = new Date(row.occurredAt);
    for (const record of records) {
      await this.appendAcknowledged(createJournalEvent({
        id: record.id,
        traceId: record.traceId,
        type: record.type,
        aggregateType: record.aggregateType,
        aggregateId: record.aggregateId,
        payload: record.payload,
        sourceEventId: record.sourceEventId,
        occurredAt,
      }));
    }
    this.derivedAuditCursor = Math.max(this.derivedAuditCursor, row.sequence);
  }

  private async appendAcknowledged(event: ReturnType<typeof createJournalEvent>, signal?: AbortSignal): Promise<void> {
    const journalClass = classifyJournalMetricClass(event.type);
    try {
      await this.journalAdmission.run(journalClass, true, async () => {
        if (signal?.aborted) throw new JournalAdmissionRejected(journalClass, true, "JOURNAL_ADMISSION_CLOSED");
        await this.metrics.observeDirectJournal(event.type, async () => {
          await this.journal.append(event);
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

  private async onMarketSnapshot(snapshot: MarketSnapshot): Promise<void> {
    if (!this.state.recoveryComplete) return;
    if (!this.tokenIdSet.has(snapshot.tokenId)) return;
    const started = performance.now();
    this.metrics.marketMessages.inc();
    this.state.connections.polymarket.lastMessageAt = snapshot.receivedAt;
    const synchronized = this.adapterTokenSynchronized(snapshot.tokenId);
    if (!synchronized) {
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
    this.nativeV2Shadow.ingestMarket(snapshot);
    if (snapshot.asset) {
      this.assetByMarket.set(snapshot.marketId, snapshot.asset);
      this.assetIndex.add(snapshot.asset, snapshot.marketId);
      this.metrics.assetMarketIndexEntries.set(this.assetIndex.entryCount());
    }
    this.updateReady();
    // Market snapshots are live processing inputs. The current snapshot/state
    // maps above remain the source for trigger, maker, risk, and dashboard work;
    // raw observations are intentionally not permanent journal records.
    this.record("MARKET_EVENT", snapshot.traceId, snapshot.marketId, { tokenId: snapshot.tokenId, outcome: snapshot.outcome, sequence: snapshot.sequence }, snapshot.asset ?? null);

    if (synchronized && previous) {
      for (const orderId of this.restingIndex.orderIdsFor(snapshot.tokenId)) {
        const resting = this.restingOrders.get(orderId);
        if (!resting || resting.order.tokenId !== snapshot.tokenId) continue;
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
          await this.applyFill(evolved.fill, { restingAfter: evolved.resting, intent: restingIntent });
        } else {
          this.restingOrders.set(orderId, evolved.resting);
          if (Number(evolved.resting.remaining) === 0) {
            this.restingOrders.delete(orderId);
            this.restingIndex.remove(orderId);
          }
        }
      }
      this.metrics.restingOrderTokenIndexEntries.set(this.restingIndex.entryCount());
    }

    if (synchronized) {
      // Deterministic market state is a precursor for bounded scout warming;
      // this admission is intentionally separate from economic triggers.
      this.requestScoutRefresh(snapshot, "market");
      const triggers = this.triggerEngine.ingestMarket(snapshot);
      for (const trigger of triggers) this.admitTrigger(trigger, this.provenanceForGenerated(trigger, "MARKET"));
      // Risk exits run outside the websocket consumer await-chain. The per-market
      // flight below prevents duplicate unwind orders while a prior attempt is
      // persisting or waiting through paper execution latency.
      void this.maybeProtectiveDeRisk(snapshot.marketId).catch((error) => {
        this.logger.error({ err: error, marketId: snapshot.marketId }, "protective de-risk attempt failed");
      });
    }
    const window = this.marketsById.get(snapshot.marketId);
    if (window?.endDate) this.triggerEngine.noteMarketWindow(snapshot.marketId, snapshot.asset ?? window.asset, window.endDate);
    this.metrics.triggerDedupeEntries.set(this.triggerEngine.dedupeEntryCount());
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
    // Only economic follow-up hydration may cause: bounded per-market de-risk.
    void this.maybeProtectiveDeRisk(current.marketId).catch((error) => {
      this.logger.error({ err: error, marketId: current.marketId }, "protective de-risk attempt failed");
    });
  }

  private async onReferenceTick(tick: ReferenceTick): Promise<void> {
    if (!this.state.recoveryComplete) return;
    if (this.usesDiscovery() && !this.assetIndex.hasAsset(tick.asset)) return;
    this.metrics.referenceTicks.inc();
    const started = performance.now();
    const admission = this.triggerEngine.admitReferenceTick(tick);
    if (admission.overflow) {
      this.nativeV2Shadow.resetReference(tick.asset);
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
    this.nativeV2Shadow.ingestReference(tick);
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
    if (this.sourceSealBlocksAdmission()) {
      return { admitted: false, reason: "source_sealed", done: Promise.resolve() };
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
      const nativeBookkeepingStarted = performance.now();
      this.activation.ensureLatency(trigger);
      this.state.setActivationLatency(this.activation.snapshot());
      await this.persistReportInput(createJournalEvent({
        traceId: trigger.traceId, type: "GROK_ACTIVATION_DECISION", aggregateType: "activation", aggregateId: trigger.id,
        payload: {
          triggerId: trigger.id, marketId: trigger.marketId, asset: trigger.asset ?? null,
          reason: "NATIVE_DETERMINISTIC_ONLY", activate: false, authority: NATIVE_DETERMINISTIC_AUTHORITY,
        },
      }));
      this.state.addActivation({
        triggerId: trigger.id, reason: "NATIVE_DETERMINISTIC_ONLY", at: this.nowFn().toISOString(),
        provider: "native_deterministic", grokWakeObserved: false,
      });
      this.metrics.idleActivationBookkeepingDuration.observe((performance.now() - nativeBookkeepingStarted) / 1000);
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
   * authoritative unresolved-position count used at the RUNNING → ENTRY_CLOSED
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

    const shadowV2FeatureVector = this.nativeV2Shadow.extract({
      marketId: trigger.marketId,
      asset,
      now: this.nowFn(),
      endDate: this.marketsById.get(trigger.marketId)?.endDate ?? null,
      marketSynchronized: true,
      referenceFreshness: referenceContext?.freshness ?? "unknown",
      referenceFreshnessMs: referenceContext?.freshnessMs ?? null,
    });
    const shadowV2FairProbability = shadowV2FeatureVector
      ? evaluateNativeV2ShadowFairProbability(shadowV2FeatureVector)
      : null;
    // PR1 learning foundation: durable samples after V2 fair succeeds, before
    // opportunity/ranking. Fail-soft — never throw into the trading path.
    if (this.learningStore && shadowV2FeatureVector && shadowV2FairProbability) {
      try {
        {
          const discovered = this.marketsById.get(trigger.marketId);
          tryObserveLearningSample(this.learningStore, {
            marketId: trigger.marketId,
            asset,
            now: this.nowFn(),
            featureVector: shadowV2FeatureVector,
            v2FairProbability: shadowV2FairProbability,
            native,
            experimentId: this.experiment?.id ?? null,
            gitSha: process.env.GIT_SHA ?? process.env.FLY_IMAGE_REF ?? null,
            settlementLookup: discovered ? (this.settlementLookupFromMarket(discovered) ?? undefined) : undefined,
          });
        }
      } catch {
        // Learning must never affect V1/V2/risk/execution authority.
      }
    }
    // PR5 learned V2 shadow — advisory only. Fail-soft; never mutates V1.
    let learnedV2Shadow: LearnedV2ShadowEvaluation | null = null;
    try {
      learnedV2Shadow = this.learningRuntime.evaluateShadow({
        featureVector: shadowV2FeatureVector,
        deterministicV2: shadowV2FairProbability,
        marketId: trigger.marketId,
        asset,
        experimentId: this.experiment?.id ?? null,
        triggerId: trigger.id,
        traceId: trigger.traceId,
        now: this.nowFn(),
      });
    } catch {
      learnedV2Shadow = null;
    }
    const shadowNow = this.nowFn();
    const shadowV2MarketOpportunity = shadowV2FairProbability
      ? evaluateNativeV2ShadowMarketOpportunity({
        fair: shadowV2FairProbability,
        snapshots: { UP: pair.UP ?? null, DOWN: pair.DOWN ?? null },
        now: shadowNow,
        targetNotionalUsd: Math.min(this.riskLimits.maxOrderSizeUsd, DEFAULT_FIRST_LEG_RISK_CAP_USD),
        minimumNetEvUsdPerShare: this.riskLimits.minExpectedEdge,
      })
      : null;
    const shadowV2OpportunityRanking = shadowV2MarketOpportunity
      ? this.nativeV2OpportunityRanker.upsertAndRank(
        shadowV2MarketOpportunity,
        shadowNow,
        this.riskLimits.staleFeedTimeoutMs,
      )
      : null;
    if (!shadowV2MarketOpportunity) this.nativeV2OpportunityRanker.dropMarket(trigger.marketId);

    const consensus = this.nativeCompatibilityDecision(trigger, native);
    this.state.latestConsensus = consensus;
    this.record("NATIVE_DECISION_COMPLETE", trigger.traceId, trigger.marketId, {
      authority: NATIVE_DETERMINISTIC_AUTHORITY,
      direction: native.direction, confidence: native.confidence,
      fairProbability: native.fairProbability, marketProbability: native.marketProbability,
      featureVector: native.featureVector, shadowV2FeatureVector, shadowV2FairProbability, shadowV2OpportunityRanking, learnedV2Shadow, triggerId: trigger.id,
    }, asset);
    await this.persistReportInput(createJournalEvent({
      traceId: trigger.traceId, type: "CONSENSUS_DECISION", aggregateType: "consensus", aggregateId: consensus.id,
      payload: {
        ...consensus,
        authority: NATIVE_DETERMINISTIC_AUTHORITY,
        marketId: trigger.marketId, asset,
        fairProbability: native.fairProbability, marketProbability: native.marketProbability,
        expiresAt: native.expiresAt, featureVector: native.featureVector, shadowV2FeatureVector, shadowV2FairProbability, shadowV2OpportunityRanking, learnedV2Shadow,
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

    const shadowV2ExecutionCapture = shadowV2FeatureVector && shadowV2FairProbability && shadowV2MarketOpportunity && shadowV2OpportunityRanking
      ? this.reserveNativeV2ShadowExecution({
        trigger, native, featureVector: shadowV2FeatureVector, fair: shadowV2FairProbability,
        opportunity: shadowV2MarketOpportunity, ranking: shadowV2OpportunityRanking,
      })
      : null;
    try {
      await this.evaluateTrade(trigger, consensus, native);
    } catch (error) {
      if (shadowV2ExecutionCapture) this.nativeV2ShadowExecutionBusy = false;
      throw error;
    }
    if (shadowV2ExecutionCapture) this.dispatchNativeV2ShadowExecution(shadowV2ExecutionCapture);
  }

  private reserveNativeV2ShadowExecution(input: {
    trigger: TriggerEvent;
    native: NativeDecision;
    featureVector: NativeV2ShadowFeatureVector;
    fair: NativeV2ShadowFairProbability;
    opportunity: NativeV2ShadowMarketOpportunity;
    ranking: NativeV2ShadowOpportunityRanking;
  }): NativeV2ShadowExecutionCapture | null {
    const selectedAtDecision = input.opportunity.best;
    if (input.ranking.marketRank !== 1 || !selectedAtDecision?.actionable) return null;

    const now = this.nowFn();
    // Phase-4 data is scoped to a real entry-capable experiment. Do not fill
    // the bounded RAM export with idle or post-entry-close observations.
    if (!experimentAllowsFirstLeg(this.experiment, now)) return null;
    this.nativeV2ShadowStore.noteObserved();
    if (this.nativeV2ShadowExecutionBusy) {
      this.nativeV2ShadowStore.noteDroppedBusy();
      return null;
    }

    const marketId = input.opportunity.marketId;
    const currentPair = this.snapshots.get(marketId);
    const currentSnapshot = currentPair?.[selectedAtDecision.outcome] ?? null;
    const targetNotionalUsd = Math.min(this.riskLimits.maxOrderSizeUsd, DEFAULT_FIRST_LEG_RISK_CAP_USD);
    const refreshedOpportunity = currentPair
      ? evaluateNativeV2ShadowMarketOpportunity({
        fair: input.fair,
        snapshots: { UP: currentPair.UP ?? null, DOWN: currentPair.DOWN ?? null },
        now,
        targetNotionalUsd,
        minimumNetEvUsdPerShare: this.riskLimits.minExpectedEdge,
      })
      : null;
    const selected = refreshedOpportunity?.candidates[selectedAtDecision.outcome] ?? null;
    const asset = input.trigger.asset ?? this.assetByMarket.get(marketId) ?? currentSnapshot?.asset ?? null;
    const blockers: string[] = [];

    if (this.env.PAPER_EXECUTION_STYLE !== "TAKER") blockers.push("SHADOW_TAKER_MODEL_REQUIRED");
    if (this.researchIngressBlocked()) blockers.push("RESEARCH_INGRESS_BLOCKED");
    if (!this.isTriggerActionable(input.trigger.id)) blockers.push("TRIGGER_NOT_ACTIONABLE");
    if (this.experimentStartPending) blockers.push("EXPERIMENT_START_PENDING");
    if (this.overload.state === "FAIL_CLOSED") blockers.push("OVERLOAD_FAIL_CLOSED");
    if (!experimentAllowsFirstLeg(this.experiment, now)) blockers.push("EXPERIMENT_ENTRY_CLOSED");
    if (!this.entryQualified.has(marketId)) blockers.push("MARKET_NOT_ENTRY_QUALIFIED");
    if (!this.isMarketSynchronized(marketId)) blockers.push("MARKET_UNSYNCHRONIZED");
    if (assetNeedsReliableReference(this.referenceRequired.get(marketId), this.referenceUnreliable, asset)) blockers.push("REFERENCE_PROVIDER_BLOCKED");
    if (!this.counterfactualStore.isRecoveryReady()) blockers.push("CF_RECOVERY_UNAVAILABLE");
    if (this.counterfactualStore.hasSettledMarket(marketId)) blockers.push("CF_MARKET_SETTLED");
    if (this.counterfactualStore.hasOpenPosition(marketId)) blockers.push("EXISTING_OPEN_POSITION");
    if (!currentSnapshot || !selected) blockers.push("CURRENT_BOOK_NOT_EXECUTABLE");
    if (currentSnapshot && !this.adapterTokenSynchronized(currentSnapshot.tokenId)) blockers.push("TOKEN_UNSYNCHRONIZED");

    const referenceRequired = this.referenceRequired.get(marketId) === true;
    const referenceFeedAt = asset ? dateOrNull(this.state.referenceLastMessageAt(asset)) : null;
    const btcFeedAt = asset === "BTC" ? referenceFeedAt ?? dateOrNull(this.state.lastBtcMessageAt) : now;
    const marketFeedAt = currentSnapshot
      ? collectMarketFeedAt(currentPair, selectedAtDecision.outcome, "OPEN_FIRST_LEG", currentSnapshot.receivedAt)
      : null;
    const duplicateKey = nativeV2CounterfactualActionKey(marketId, selectedAtDecision.outcome, "OPEN_FIRST_LEG");
    const cfState = this.counterfactualStore.decisionState();
    const blocked = asset ? this.referenceBlocked.get(asset) : undefined;

    const discoveredAtDecision = this.marketsById.get(marketId);
    const settlementLookupAtDecision = discoveredAtDecision
      ? this.settlementLookupFromMarket(discoveredAtDecision)
      : null;

    const capture: NativeV2ShadowExecutionCapture = {
      trigger: structuredClone(input.trigger),
      featureVector: structuredClone(input.featureVector),
      fair: structuredClone(input.fair),
      opportunity: structuredClone(input.opportunity),
      ranking: structuredClone(input.ranking),
      native: structuredClone(input.native),
      capturedAt: now.toISOString(),
      experimentId: this.experiment?.id ?? null,
      experimentStatus: this.experiment?.status ?? null,
      gitSha: process.env.GIT_SHA ?? null,
      asset,
      currentSnapshot: currentSnapshot ? structuredClone(currentSnapshot) : null,
      selected: selected ? structuredClone(selected) : null,
      settlementLookup: settlementLookupAtDecision ? structuredClone(settlementLookupAtDecision) : null,
      evidenceIds: [...new Set([
        ...input.trigger.sourceEventIds,
        ...(input.trigger.evidenceIds ?? []),
        currentPair?.UP?.eventId ?? "",
        currentPair?.DOWN?.eventId ?? "",
      ].filter(Boolean))],
      preflightBlockers: [...new Set(blockers)],
      // V2-only counterfactual portfolio — never the authoritative V1 paper ledger.
      positions: cfState.positions,
      assetByMarketEntries: [...this.assetByMarket.entries()],
      ledger: cfState.ledger,
      availableCashUsd: cfState.availableCashUsd,
      marketFeedAt: marketFeedAt?.toISOString() ?? null,
      referenceFeedAt: referenceFeedAt?.toISOString() ?? null,
      btcFeedAt: btcFeedAt?.toISOString() ?? null,
      referenceRequired,
      databaseAvailable: this.state.database === "UP",
      timestampDisorder: this.timestampDisorder,
      referenceProviderBlocked: Boolean(blocked),
      marketEntryQualified: this.entryQualified.has(marketId),
      lastStrategyTradeAt: cfState.lastStrategyTradeAt,
      duplicateTrade: this.counterfactualStore.hasAction(duplicateKey),
      experimentEntryOpen: experimentAllowsFirstLeg(this.experiment, now),
      executionErrorsLastHour: this.executionErrors.filter((at) => now.getTime() - at < 3_600_000).length,
      shuttingDown: this.state.draining,
    };
    this.nativeV2ShadowExecutionBusy = true;
    return capture;
  }

  private dispatchNativeV2ShadowExecution(capture: NativeV2ShadowExecutionCapture): void {
    const handle = setImmediate(() => {
      void this.runNativeV2ShadowExecution(capture);
    });
    handle.unref?.();
  }

  private async runNativeV2ShadowExecution(capture: NativeV2ShadowExecutionCapture): Promise<void> {
    try {
      if (this.nativeV2ShadowEvalHoldForTest) await this.nativeV2ShadowEvalHoldForTest;
      try {
        const execution = this.evaluateNativeV2ShadowExecution(capture);
        const selectedAtDecision = capture.opportunity.best;
        if (!selectedAtDecision) return;
        this.nativeV2ShadowStore.append({
          recordedAt: this.nowFn().toISOString(),
          experimentId: capture.experimentId,
          experimentStatus: capture.experimentStatus,
          gitSha: capture.gitSha,
          triggerId: capture.trigger.id,
          traceId: capture.trigger.traceId,
          marketId: capture.opportunity.marketId,
          asset: capture.asset,
          outcome: selectedAtDecision.outcome,
          v1: {
            direction: capture.native.direction,
            confidence: capture.native.confidence,
            fairProbability: capture.native.fairProbability,
            marketProbability: capture.native.marketProbability,
          },
          featureVector: capture.featureVector,
          fairProbability: capture.fair,
          opportunity: {
            marketRank: capture.ranking.marketRank ?? 1,
            rankedMarketCount: capture.ranking.rankedMarketCount,
            actionableMarketCount: capture.ranking.actionableMarketCount,
            selected: selectedAtDecision,
            globalTop: capture.ranking.top,
          },
          execution,
        });
      } catch (error) {
        const selectedAtDecision = capture.opportunity.best;
        this.logger.error({ err: error, marketId: capture.opportunity.marketId }, "native v2 shadow execution evaluation failed");
        if (selectedAtDecision) {
          this.nativeV2ShadowStore.append({
            recordedAt: this.nowFn().toISOString(),
            experimentId: capture.experimentId, experimentStatus: capture.experimentStatus, gitSha: capture.gitSha,
            triggerId: capture.trigger.id, traceId: capture.trigger.traceId, marketId: capture.opportunity.marketId,
            asset: capture.asset, outcome: selectedAtDecision.outcome,
            v1: {
              direction: capture.native.direction, confidence: capture.native.confidence,
              fairProbability: capture.native.fairProbability, marketProbability: capture.native.marketProbability,
            },
            featureVector: capture.featureVector, fairProbability: capture.fair,
            opportunity: {
              marketRank: capture.ranking.marketRank ?? 1, rankedMarketCount: capture.ranking.rankedMarketCount,
              actionableMarketCount: capture.ranking.actionableMarketCount, selected: selectedAtDecision, globalTop: capture.ranking.top,
            },
            execution: {
              version: NATIVE_V2_SHADOW_EXECUTION_VERSION, evaluatedAt: capture.capturedAt,
              preflightBlockers: ["SHADOW_EVALUATION_ERROR"], riskVetoes: [], wouldSubmitOrder: false,
              error: error instanceof Error ? error.message : String(error),
            },
          });
        }
      } finally {
        this.nativeV2ShadowExecutionBusy = false;
      }
    } catch (error) {
      this.nativeV2ShadowExecutionBusy = false;
      this.logger.error({ err: error, marketId: capture.opportunity.marketId }, "native v2 shadow execution dispatch failed");
    }
  }

  private evaluateNativeV2ShadowExecution(input: NativeV2ShadowExecutionCapture): Record<string, unknown> {
    const selectedAtDecision = input.opportunity.best!;
    const now = new Date(input.capturedAt);
    const marketId = input.opportunity.marketId;
    const selected = input.selected;
    const compactBase = {
      version: NATIVE_V2_SHADOW_EXECUTION_VERSION,
      stateBasis: NATIVE_V2_COUNTERFACTUAL_STATE_BASIS,
      evaluatedAt: input.capturedAt,
      marketId,
      outcome: selectedAtDecision.outcome,
      marketRank: input.ranking.marketRank,
      selectedFairProbability: selected?.fairProbability ?? selectedAtDecision.fairProbability,
      currentExecutableVwap: selected?.executableVwap ?? null,
      currentGrossEdgeUsdPerShare: selected?.grossEdgeUsdPerShare ?? null,
      preflightBlockers: input.preflightBlockers,
    };
    if (input.preflightBlockers.length > 0 || !input.currentSnapshot || !selected) {
      return {
        ...compactBase, executionConfidence: null, evidenceCoverage: null, riskAccepted: null,
        riskVetoes: [], wouldSubmitOrder: false, order: null, economics: null,
      };
    }

    const intent = buildNativeV2ShadowExecutionIntent({
      triggerId: input.trigger.id,
      traceId: input.trigger.traceId,
      expiresAt: input.trigger.expiresAt,
      evidenceIds: input.evidenceIds,
      fair: input.fair,
      selected,
      snapshot: input.currentSnapshot,
      now,
    });
    if (!intent) {
      return {
        ...compactBase, preflightBlockers: ["INVALID_SHADOW_EXECUTION_INTENT"],
        executionConfidence: null, evidenceCoverage: null, riskAccepted: null,
        riskVetoes: [], wouldSubmitOrder: false, order: null, economics: null,
      };
    }

    const assetByMarket = new Map(input.assetByMarketEntries);
    const candidate = intent.candidate;
    const exposure = computePaperExposures(input.positions, marketId, assetByMarket, now.getTime());
    const projected = projectPaperExposures({
      current: exposure,
      positions: projectableLegsFromPositions(input.positions, assetByMarket),
      candidate: {
        marketId: candidate.marketId, outcome: candidate.outcome, quantity: candidate.quantity,
        price: candidate.limitPrice, intent: candidate.intent, asset: input.asset,
      },
      nowMs: now.getTime(),
    });
    const decision = decidePaperOrder({
      candidate,
      consensus: intent.consensus,
      snapshotAfterDecisionLatency: input.currentSnapshot,
      limits: this.riskLimits,
      triggerId: input.trigger.id,
      signalExpiresAt: new Date(input.trigger.expiresAt),
      orderType: "TAKER",
      baseRisk: {
        now,
        marketFeedAt: dateOrNull(input.marketFeedAt),
        btcFeedAt: dateOrNull(input.btcFeedAt),
        referenceFeedAt: dateOrNull(input.referenceFeedAt),
        referenceRequired: input.referenceRequired,
        asset: input.asset,
        assetExposureUsd: exposure.asset,
        databaseAvailable: input.databaseAvailable,
        timestampDisorder: input.timestampDisorder,
        riskStateAvailable: true,
        dailyPnlUsd: input.ledger.netTradingPnl,
        drawdownUsd: input.ledger.drawdown,
        marketExposureUsd: exposure.market,
        totalExposureUsd: exposure.total,
        directionalExposureUsd: exposure.directional,
        unhedgedExposureUsd: exposure.unhedged,
        unhedgedDurationMs: exposure.unhedgedDurationMs,
        intent: candidate.intent,
        projectedMarketExposureUsd: projected.market,
        projectedTotalExposureUsd: projected.total,
        projectedDirectionalExposureUsd: projected.directional,
        projectedUnhedgedExposureUsd: projected.unhedged,
        projectedAssetExposureUsd: projected.asset,
        availableCashUsd: input.availableCashUsd,
        referenceProviderBlocked: input.referenceProviderBlocked,
        marketEntryQualified: input.marketEntryQualified,
        externalSignalsLastMinute: 0,
        lastStrategyTradeAt: dateOrNull(input.lastStrategyTradeAt),
        duplicateTrade: input.duplicateTrade,
        experimentEntryOpen: input.experimentEntryOpen,
        executionErrorsLastHour: input.executionErrorsLastHour,
        externalRiskRecommendedVeto: false,
        shuttingDown: input.shuttingDown,
      },
    });

    const wouldSubmitOrder = decision.accepted && decision.order !== null;
    let counterfactual: Record<string, unknown> | null = null;
    if (wouldSubmitOrder && decision.order && this.counterfactualStore.isRecoveryReady()) {
      const actionKey = nativeV2CounterfactualActionKey(marketId, selectedAtDecision.outcome, "OPEN_FIRST_LEG");
      // Decision-time capture only — discovery refresh after reserve must not change historical settlement semantics.
      const capturedLookup = input.settlementLookup;
      if (
        !capturedLookup
        || capturedLookup.marketId !== marketId
        || !isAuthoritativeBinaryMapping(capturedLookup.yesMapsTo, capturedLookup.noMapsTo)
      ) {
        // Production-shaped qualified actionable CF fill without restart-safe lookup must not durable-accept.
        counterfactual = {
          actionKey,
          applied: false,
          reason: "MISSING_SETTLEMENT_LOOKUP",
          fillId: null,
          summary: this.counterfactualStore.summary(),
        };
      } else {
        const commit = this.counterfactualStore.tryCommitWouldSubmit({
          actionKey,
          order: decision.order,
          snapshot: input.currentSnapshot,
          measuredLatencyMs: 0,
          at: now,
          settlementLookup: structuredClone(capturedLookup),
        });
        counterfactual = {
          actionKey,
          applied: commit.applied,
          reason: commit.applied ? null : commit.reason,
          fillId: commit.applied ? commit.fill.id : null,
          summary: this.counterfactualStore.summary(),
        };
      }
    }

    return {
      ...compactBase,
      executionConfidence: intent.confidence,
      evidenceCoverage: intent.evidenceCoverage,
      riskAccepted: decision.risk.accepted,
      riskVetoes: decision.risk.vetoes,
      wouldSubmitOrder,
      order: decision.order ? {
        orderType: decision.order.orderType, requestedPrice: decision.order.requestedPrice, quantity: decision.order.quantity,
      } : null,
      economics: {
        rawEdgeUsdPerShare: decision.rawEdgeUsdPerShare, feeUsdPerShare: decision.feeUsdPerShare,
        slippageUsdPerShare: decision.slippageUsdPerShare, costUsdPerShare: decision.costUsdPerShare,
        netEvUsdPerShare: decision.netEvUsdPerShare, grossEvUsdTotal: decision.grossEvUsdTotal,
        netEvUsdTotal: decision.netEvUsdTotal, executableNotionalUsd: decision.executableNotionalUsd,
        expectedROI: decision.expectedROI, bookVwap: decision.bookVwap,
      },
      counterfactual,
    };
  }

  nativeV2ShadowSnapshot() {
    return this.nativeV2ShadowStore.snapshot();
  }

  learningSummary() {
    return this.learningStore?.summary() ?? { enabled: false };
  }

  learningRuntimeSummary() {
    return this.learningRuntime.summary();
  }

  counterfactualSummary() {
    return this.counterfactualStore.summary();
  }

  nativeV2ShadowExport() {
    return this.nativeV2ShadowStore.export({
      generatedAt: this.nowFn().toISOString(),
      experimentId: this.experiment?.id ?? null,
      experimentStatus: this.experiment?.status ?? null,
      gitSha: process.env.GIT_SHA ?? null,
    });
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
    const rejectSealed = (): { status: "source_sealed" } => {
      this.state.recordGrokRejection("rejected");
      this.metrics.grokSignals.inc({ agent: submission.agentId, status: "source_sealed" });
      return { status: "source_sealed" };
    };
    const ingress = this.beginResearchIngress();
    try {
      if (this.researchIngressStale(ingress.generation)) return rejectSealed();
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
      if (this.researchIngressStale(ingress.generation)) return rejectSealed();
      if (this.grokClaimHold) await this.grokClaimHold;
      if (this.researchIngressStale(ingress.generation)) return rejectSealed();
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
          // beginExperiment aborts any stale source-seal persistence and bumps
          // that generation. The promoted submission is now an active ingress;
          // continue from the post-transition generation under the boundary
          // gate, while completion/source-seal fences still reject it.
          ingress.generation = this.sourceSealGeneration;
        }
        // Recheck the experiment-scoped fence immediately before irreversible accept.
        if (this.researchIngressStale(ingress.generation) || (!researchRefresh && !this.isTriggerActionable(trigger.id))) {
          if (idleReservation) this.releaseIdleGrokSubmission(idempotencyKey);
          this.state.recordGrokRejection("rejected");
          this.metrics.grokSignals.inc({ agent: submission.agentId, status: this.researchIngressStale(ingress.generation) ? "source_sealed" : "stale" });
          return { status: this.researchIngressStale(ingress.generation) ? "source_sealed" : "stale" };
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
          return rejectSealed();
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
      const next = refreshExperiment(this.experiment, this.nowFn(), {
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
      maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd, minimumLockedPairEdge: this.riskLimits.minExpectedEdge,
    });
    if (!candidate) return;
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
        minimumLockedPairEdge: this.riskLimits.minExpectedEdge,
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

    const duplicateKey = `${candidate.marketId}:${candidate.outcome}:${candidate.intent}`;
    const now = this.nowFn();
    const signalExpiry = new Date(decisionNative.expiresAt);
    const asset = trigger.asset ?? this.assetByMarket.get(candidate.marketId) ?? snapshot.asset ?? null;
    const exposure = computePaperExposures(this.ledger.positions.values(), candidate.marketId, this.assetByMarket, now.getTime());
    const projected = projectPaperExposures({
      current: exposure,
      positions: projectableLegsFromPositions(this.ledger.positions.values(), this.assetByMarket),
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
    const decision = decidePaperOrder({
      candidate,
      consensus: decisionConsensus,
      snapshotAfterDecisionLatency: snapshot,
      limits: this.riskLimits,
      triggerId: trigger.id,
      signalExpiresAt: signalExpiry,
      orderType: this.env.PAPER_EXECUTION_STYLE,
      baseRisk: {
        now, marketFeedAt,
        btcFeedAt: asset === "BTC" ? referenceFeedAt ?? dateOrNull(this.state.lastBtcMessageAt) : now,
        referenceFeedAt, referenceRequired, asset, assetExposureUsd: exposure.asset,
        databaseAvailable, timestampDisorder: this.timestampDisorder, riskStateAvailable: true,
        dailyPnlUsd: this.ledger.snapshot().netTradingPnl, drawdownUsd: this.ledger.snapshot().drawdown,
        marketExposureUsd: exposure.market, totalExposureUsd: exposure.total, directionalExposureUsd: exposure.directional,
        unhedgedExposureUsd: exposure.unhedged, unhedgedDurationMs: exposure.unhedgedDurationMs,
        intent: candidate.intent,
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
    });
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
    await this.submitPaperOrder(decision.order, snapshot, duplicateKey, candidate.intent);
  }

  private async submitPaperOrder(
    order: PaperOrder,
    decisionSnapshot: MarketSnapshot,
    duplicateKey: string,
    intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG" = "OPEN_FIRST_LEG",
    deRisk?: ProtectiveDeRiskAudit,
  ): Promise<void> {
    if (intent === "OPEN_FIRST_LEG" && this.experimentStartPending) return;
    if (intent === "PROTECTIVE_DE_RISK" && order.orderType !== "TAKER") {
      throw new Error("PROTECTIVE_DE_RISK_REQUIRES_TAKER");
    }
    if (intent === "RESCUE_SECOND_LEG" && order.orderType !== "TAKER") {
      throw new Error("RESCUE_SECOND_LEG_REQUIRES_TAKER");
    }
    const riskReductionOrder = intent === "HEDGE_SECOND_LEG"
      || intent === "RESCUE_SECOND_LEG"
      || intent === "PROTECTIVE_DE_RISK";
    // PR2 single risk-reduction authority: exactly one risk-reduction admission
    // may be in flight per market. RESCUE and PROTECTIVE submissions are
    // already serialized by the maybeProtectiveDeRisk per-market flight (the
    // shared set is held for their whole decision+execution lifetime), so only
    // normal HEDGE submissions acquire/check the slot here — a hedge must never
    // slip in behind rescue/protective ownership. A HEDGE MAKER admission holds
    // the slot through its durable PAPER_MAKER_RESERVED so rescue cannot start
    // A/B arbitration while a hedge maker is still being admitted/reserved; the
    // slot is released again once the resting order is authoritative. A resting
    // maker does NOT hold the slot for its resting lifetime.
    const ownsRiskReductionFlight = intent === "HEDGE_SECOND_LEG";
    if (ownsRiskReductionFlight) {
      if (this.riskReductionInFlight.has(order.marketId)) {
        this.record("RISK_REDUCTION_FLIGHT_BUSY", order.traceId, order.marketId, {
          intent, orderId: order.id, reason: "RISK_REDUCTION_IN_FLIGHT",
        }, decisionSnapshot.asset ?? null);
        return;
      }
      // Admission-boundary eligibility re-check (post decision latency): a
      // normal hedge must not slip in behind rescue ownership once protective
      // eligibility became true.
      if (intent === "HEDGE_SECOND_LEG") {
        const posAtAdmission = this.ledger.positions.get(order.marketId);
        if (posAtAdmission?.unpairedOutcome) {
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
            return;
          }
        }
      }
      this.riskReductionInFlight.add(order.marketId);
    }
    const firstLegReserved = intent === "OPEN_FIRST_LEG"
      ? this.tryReserveFirstLegAdmission(order.marketId)
      : false;
    if (intent === "OPEN_FIRST_LEG" && !firstLegReserved) return;

    try {
      // Last economic admission fence immediately before PAPER_ORDER_CREATED.
      // close-entries and the native decision path are serialized by the same
      // experimentBoundaryGate, while deadline expiry closes durably here.
      if (intent === "OPEN_FIRST_LEG" && !(await this.ensureFirstLegEntryOpen())) return;
      if (
        intent === "OPEN_FIRST_LEG"
        && this.firstLegLifecycleBlocked(order.marketId, true)
      ) return;
      if (intent === "HEDGE_SECOND_LEG" || intent === "PROTECTIVE_DE_RISK" || intent === "RESCUE_SECOND_LEG") {
        // Once risk reduction starts, an older first-leg maker remainder must
        // not be allowed to refill directional exposure behind the action.
        await this.cancelOutstandingFirstLegMakers(
          order.marketId,
          intent === "PROTECTIVE_DE_RISK" ? "PROTECTIVE_DE_RISK_ADMITTED"
            : intent === "RESCUE_SECOND_LEG" ? "RESCUE_ADMITTED"
            : "HEDGE_ADMITTED",
        );
      }

      this.metrics.paperOrders.inc({ type: order.orderType });
      this.record("PAPER_ORDER_CREATED", order.traceId, order.marketId, {
        orderId: order.id, type: order.orderType, intent, ...(deRisk ? { deRisk } : {}),
      }, decisionSnapshot.asset ?? null);
      await this.persistReportInput(createJournalEvent({
        id: derivedAuditEventId("PAPER_ORDER_CREATED", order.id),
        traceId: order.traceId, type: "PAPER_ORDER_CREATED", aggregateType: "paper_order", aggregateId: order.id,
        payload: { ...order, intent, ...(deRisk ? { deRisk } : {}) },
      }));
      this.recentTrades.set(duplicateKey, Date.now());
      try {
        if (order.orderType === "MAKER") {
          if (intent === "PROTECTIVE_DE_RISK") throw new Error("PROTECTIVE_DE_RISK_REQUIRES_TAKER");
          if (intent === "RESCUE_SECOND_LEG") throw new Error("RESCUE_SECOND_LEG_REQUIRES_TAKER");
          // Resting makers are only ever first-leg or hedge orders; rescue is
          // enforced TAKER-only above, so the cast is a pure TS narrowing aid.
          await this.reserveMaker(order, decisionSnapshot, intent === "OPEN_FIRST_LEG" ? "OPEN_FIRST_LEG" : "HEDGE_SECOND_LEG");
          return;
        }
        await delay(this.env.PAPER_EXECUTION_LATENCY_MS);
        const current = this.state.marketSnapshots.get(order.tokenId);
        if (!current || new Date(current.receivedAt).getTime() < new Date(decisionSnapshot.receivedAt).getTime()) throw new Error("STALE_EXECUTION_SNAPSHOT");
        const applied = await this.applyFill(
          executePaperTaker({ order, snapshotAfterLatency: current, measuredLatencyMs: this.env.PAPER_EXECUTION_LATENCY_MS }),
          { intent, deRisk },
        );
        if (applied) this.lastTradeAt = this.nowFn();
      } catch (error) {
        if (this.overload.hasIntegrityFault || error instanceof IdempotencyConflictError || error instanceof CrashInjectedError || (error instanceof JournalAdmissionRejected && error.critical)) throw error;
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
          return;
        }
        this.executionErrors.push(Date.now());
        const message = error instanceof Error ? error.message : String(error);
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
      }
    } finally {
      if (firstLegReserved) this.pendingFirstLegMarkets.delete(order.marketId);
      if (ownsRiskReductionFlight) this.riskReductionInFlight.delete(order.marketId);
    }
  }

  private async reserveMaker(
    order: PaperOrder,
    snapshot: MarketSnapshot,
    intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" = "OPEN_FIRST_LEG",
  ): Promise<void> {
    const notional = makerReserveNotional(order.quantity, order.requestedPrice, snapshot.feeSchedule);
    const priorReserve = await this.journal.readDurableByOperationKey(`maker:${order.id}:reserve`);
    const compactedRetry = Boolean(priorReserve && Object.keys(priorReserve.payload).length === 0);
    if (this.ledger.availableCash().lt(notional) && !priorReserve) {
      throw new Error("INSUFFICIENT_CASH");
    }
    const resting = compactedRetry
      ? (this.restingOrders.get(order.id) ?? createRestingMakerOrder(order, snapshot, intent))
      : createRestingMakerOrder(order, snapshot, intent);
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
      buildPayload: (occurredAt) => ({
        ...auth,
        derivedAudit: [
          {
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
        }],
      }),
      afterApply: async () => {
        if (options.cancel) this.record("PAPER_ORDER_CANCELLED", orderId, null, { orderId, reason: options.reason ?? null });
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
      intent?: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG";
      deRisk?: ProtectiveDeRiskAudit;
    } = {},
  ): Promise<boolean> {
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
        beforeCommit: () => {
          if (priorFillAuthority) return;
          const currentPosition = this.ledger.positions.get(fill.marketId);

          if (economicIntent === "HEDGE_SECOND_LEG" || economicIntent === "PROTECTIVE_DE_RISK" || economicIntent === "RESCUE_SECOND_LEG") {
            // Protected actions (protective SELL and rescue pair-lock BUY)
            // re-validate hard safety immediately before the durable economic
            // mutation. A flip since risk evaluation makes this a state-changed
            // rejection (bounded, deterministic), never a durability fault.
            if (economicIntent === "PROTECTIVE_DE_RISK" || economicIntent === "RESCUE_SECOND_LEG") {
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
            return;
          }

          if (economicIntent !== "OPEN_FIRST_LEG") return;
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
          if (currentPosition && d(currentPosition.pairedQuantity).gt(0)) {
            throw new FirstLegLifecycleGuardRejectedError(fill.marketId, "PAIRING_ALREADY_STARTED");
          }
        },
        buildPayload: (occurredAt) => {
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
            ...(economicIntent ? { intent: economicIntent } : {}),
            ...(extras.deRisk ? { deRisk: extras.deRisk } : {}),
          };
          return {
            ...auth,
            derivedAudit: this.projectDerivedAudit(
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
              ),
            ),
          };
        },
        afterApply: async () => {
          this.record("PAPER_FILL", fill.traceId, fill.marketId, { fillId: fill.id, orderId: fill.orderId, price: fill.fillPrice, quantity: fill.quantity, role: fill.liquidityRole });
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
    if (economicIntent === "HEDGE_SECOND_LEG" || economicIntent === "PROTECTIVE_DE_RISK" || economicIntent === "RESCUE_SECOND_LEG") {
      await this.cancelOutstandingFirstLegMakers(
        fill.marketId,
        economicIntent === "PROTECTIVE_DE_RISK" ? "PROTECTIVE_DE_RISK_FILLED"
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
    intent?: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG",
    deRisk?: ProtectiveDeRiskAudit,
    unhedgedStartedAtBefore: string | null = null,
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
        ...(deRisk ? {
          deRisk: this.finalizeProtectiveDeRiskAudit(deRisk, fill, position),
        } : {}),
      },
      sourceEventId: fill.sourceMarketEventId,
    }];
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
    }
    records.push({
      id: derivedAuditEventId("PNL_SNAPSHOT", fill.id),
      type: "PNL_SNAPSHOT",
      aggregateType: "ledger",
      aggregateId: experimentId ?? "paper",
      traceId: fill.traceId,
      payload: { ...projected.ledger.snapshot(), experimentId, source: "FILL", fillId: fill.id },
    });
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

  private async onConnection(event: FeedConnectionEvent): Promise<void> {
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
        this.nativeV2Shadow.resetReference(asset);
        if (event.message?.includes("REFERENCE_INGRESS_OVERFLOW")) this.requestReferenceResync(asset);
      } else if (event.state === "DISCONNECTED") {
        this.noteExperimentFeedInvalidation();
        const kind = /kind=([a-z_]+)/.exec(event.message ?? "")?.[1] ?? "unknown";
        this.metrics.referenceCloses.inc({ kind });
        if (event.reconnectCount > 0) this.metrics.wsReconnects.inc({ feed: "reference" });
        this.noteReferenceLogicalLoss(asset);
        this.state.invalidateReferenceContext(asset);
        this.triggerEngine.resetReferenceAfterReconnect(asset);
        this.nativeV2Shadow.resetReference(asset);
      } else if (event.state === "CONNECTED") {
        this.referenceLogicalGapped.delete(asset);
        if (this.referenceUnreliable.has(asset) || this.triggerEngine.referenceWindowOverflowed(asset)) {
          this.triggerEngine.resetReferenceAfterReconnect(asset);
          this.nativeV2Shadow.resetReference(asset);
        }
      }
    }
    this.updateReady();
    this.record(`WS_${event.state}`, createId("trace"), null, { feed: event.feed, asset: event.asset ?? null, reconnects: event.reconnectCount, message: event.message }, event.asset ?? null);
  }

  private onPolymarketAnomaly(anomaly: PolymarketFeedAnomaly): void {
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
    const item = { id: createId("feed"), at: new Date().toISOString(), type, traceId, marketId, asset, detail };
    const telemetry = type === "MARKET_EVENT" || type === "REFERENCE_TICK";
    if (!telemetry) {
      this.state.record(item);
      this.sse.publish("decision", item);
    }
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
    return isReportInputEventType(type) && !isSourceSealExemptType(type) && this.isExperimentWindowOpen();
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
      || isSourceSealExemptType(type)
      || this.isExperimentWindowOpen();
  }

  async persistReportInput(event: JournalEvent, signal?: AbortSignal, extra?: { deadlineAt?: number }): Promise<void> {
    if (!this.isReportInputPersistenceAdmitted(event.type)) return;
    const experimentId = this.experiment?.id;
    const stamped = this.isExperimentWindowOpen() && experimentId && typeof event.payload.experimentId !== "string"
      ? { ...event, payload: { ...event.payload, experimentId } }
      : event;
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
          await this.journal.append(event, { signal, deadlineAt: extra?.deadlineAt });
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
      if (critical) {
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
          restingOrders: [...this.restingOrders.values()], activeTriggers: [...this.activeTriggers.values()],
          grokSignals: [...this.signalsByTrigger.values()].flatMap((signals) => [...signals.values()]),
          activationLatency: this.activation.snapshot(),
          makerReservations: Object.fromEntries(this.makerReservations),
          managedMarkets: [...this.obligationMarketIds()],
          settledMarkets: [...this.settledMarkets],
          marketMetadata: [...this.marketsById.values()].map((market) => ({ ...market, raw: {} })),
          lastInfrastructureAccrualAt: this.lastInfrastructureAccrualAt,
          derivedAuditCursor: this.derivedAuditCursor,
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
      buildPayload: () => ({
        ...auth,
        derivedAudit: [{
          id: derivedAuditEventId("INFRASTRUCTURE_COST_ACCRUED", `${aggregateId}:${auth.accrualKey}`),
          type: "INFRASTRUCTURE_COST_ACCRUED",
          aggregateType: "ledger",
          aggregateId,
          traceId: aggregateId,
          payload: { ...auth, experimentId: this.experiment?.id ?? null },
        }],
      }),
    });
  }

  private async maintainExperiment(): Promise<void> {
    // CF + learning settlement obligations are restart-durable and must poll even when
    // no experiment is active (expired markets may be absent from discovery).
    await this.pollLearningSettlements();
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
    let next = refreshExperiment(this.experiment, this.nowFn(), { openPositions: open, unresolvedCount: unresolved });
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

  private async maybeProtectiveDeRisk(marketId: string): Promise<void> {
    if (this.riskReductionInFlight.has(marketId)) {
      this.recordProtectiveDeRiskSuppressed(marketId, "GATE_IN_FLIGHT", "IN_FLIGHT");
      return;
    }
    const initial = this.ledger.positions.get(marketId);
    if (!initial?.unpairedOutcome || d(initial.unpairedQuantity).lte(0)) {
      this.clearProtectiveDeRiskState(marketId);
      return;
    }
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

    const initialNow = this.nowFn();
    const initialStartedAt = initial.unhedgedStartedAt ?? initial.openedAt;
    const initialDurationMs = Math.max(0, initialNow.getTime() - Date.parse(initialStartedAt));
    const initialEligibility = this.protectiveDeRiskEligibility(marketId, initialStartedAt, initialNow.getTime());
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

    this.riskReductionInFlight.add(marketId);
    try {
      // Once the hard unhedged timeout is breached, no stale entry remainder
      // may keep increasing directional inventory while an exit is pending or
      // temporarily unexecutable. Cancelling it is itself risk-reducing.
      await this.cancelOutstandingFirstLegMakers(marketId, "MAX_UNHEDGED_DURATION");

      // RESCUE boundary: a resting HEDGE_SECOND_LEG maker admitted BEFORE the
      // rescue ownership must not stay economically live while the arbiter
      // decides between A (SELL) and B (pair-lock). Cancel/fence it first, then
      // re-read position and books from FRESH state.
      await this.cancelRestingHedgeMakers(marketId, "RESCUE_ELIGIBLE");

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
      const eligibility = this.protectiveDeRiskEligibility(marketId, unhedgedStartedAt, now.getTime());
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
      const snapshot = pair?.[position.unpairedOutcome]
        ?? [...this.state.marketSnapshots.values()].find((item) => item.marketId === marketId && item.outcome === position.unpairedOutcome)
        ?? null;
      if (!snapshot) {
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
        // Coherent comparison (§16): B must be priced from a FRESH opposite
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
      const aReason: string | null = aEval.reason;
      const bReason: string | null = bEval.reason;
      if (!aEval.candidate && !bEval.candidate) {
        // A-oriented diagnostic contract is preserved: when both are null the
        // suppression reason keeps the unpaired-book reason (the old single
        // candidate semantics); the opposite-book reason stays observable in
        // extras for the rescue arbiter.
        const rejectionReason = aEval.reason ?? bEval.reason ?? "NO_EXECUTABLE_DEPTH";
        const lastWaitingAt = this.protectiveDeRiskLastWaitingAt.get(marketId) ?? Number.NEGATIVE_INFINITY;
        if (now.getTime() - lastWaitingAt >= PROTECTIVE_DE_RISK_WAIT_SAMPLE_MS) {
          this.protectiveDeRiskLastWaitingAt.set(marketId, now.getTime());
          this.record("PROTECTIVE_DE_RISK_WAITING", position.id, marketId, {
            deRiskReason: "MAX_UNHEDGED_DURATION",
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
      if (now.getTime() < nextAttemptAt) {
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
      // the only economic comparison input — the first-leg entry price is sunk.
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
      const eligibilityReason: RiskReductionUnhedgedReason = this.deRiskEligibilityReason(eligibility, unhedgedStartedAt);

      const residualQuantity = selectedDecision?.order
        ? d(position.unpairedQuantity).minus(selectedDecision.order.quantity)
        : d(position.unpairedQuantity);
      const audit: ProtectiveDeRiskAudit = {
        deRiskReason: "MAX_UNHEDGED_DURATION",
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
        finalState: selectedDecision?.order ? "ORDER_ADMITTED" : "RISK_VETO",
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

      this.protectiveDeRiskVetoFingerprint.delete(marketId);
      this.record("RISK_ACCEPT", traceId, marketId, {
        orderId: selectedDecision.order.id,
        intent: selectedIntent,
        deRisk: audit,
      }, asset);
      const unpairedBeforeSubmit = d(position.unpairedQuantity);
      await this.submitPaperOrder(
        selectedDecision.order,
        selectedIntent === "RESCUE_SECOND_LEG" ? oppositeSnapshot! : snapshot,
        `${marketId}:${selectedIntent === "RESCUE_SECOND_LEG" ? oppositeOutcome : snapshot.outcome}:${selectedIntent}`,
        selectedIntent!,
        audit,
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
    } finally {
      this.riskReductionInFlight.delete(marketId);
      const current = this.ledger.positions.get(marketId);
      if (!current || d(current.unpairedQuantity).lte(0)) this.clearProtectiveDeRiskState(marketId);
    }
  }

  /**
   * Distinguishes which deadline bound made the position protective-eligible:
   * the hard unhedged duration or the expiry safety window (§44). Pure
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
   * protective economic mutation — rejected as a normal state change, never
   * as an integrity fault.
   */
  private protectiveFillHardSafetyReason(fill: Parameters<PaperLedger["applyFill"]>[0]): string | null {
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
   * not hashed, not source-sealed, no integrity effect).
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
    this.protectiveDeRiskAttempts.delete(marketId);
    this.protectiveDeRiskNextAttemptAt.delete(marketId);
    this.protectiveDeRiskVetoFingerprint.delete(marketId);
    this.protectiveDeRiskLastWaitingAt.delete(marketId);
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
      const market = this.marketsById.get(position.marketId);
      if (!market?.endDate || new Date(market.endDate).getTime() > now.getTime()) continue;
      const waitUntil = this.settlementBackoff.get(position.marketId) ?? 0;
      if (now.getTime() < waitUntil) continue;
      await this.settleMarket(position.marketId, market);
    }
  }

  /**
   * Non-economic learning settlement poll. Labels durable learning samples via
   * the same public resolution fetch used by economic settlement, without
   * calling settleMarket / commitAuthoritative POSITION_SETTLED / ledger mutation.
   */

  private settlementLookupFromMarket(market: DiscoveredMarket): BinarySettlementLookup | null {
    if (!market.endDate) return null;
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

  private resolveSettlementLookup(marketId: string): BinarySettlementLookup | null {
    // Prefer decision/accept-time durable captures over live discovery so a later
    // re-discovery cannot rewrite historical YES/NO settlement semantics.
    const fromCf = this.counterfactualStore.settlementLookupFor(marketId);
    if (fromCf) return fromCf;
    const fromLearning = this.learningStore?.settlementLookupFor(marketId) ?? null;
    if (fromLearning) return fromLearning;
    const live = this.marketsById.get(marketId);
    if (live) {
      const fromLive = this.settlementLookupFromMarket(live);
      if (fromLive) return fromLive;
    }
    return null;
  }

  private async pollLearningSettlements(): Promise<void> {
    const now = this.nowFn();
    const intervalMs = this.env.LEARNING_SETTLEMENT_POLL_MS ?? this.env.SETTLEMENT_POLL_MS;
    if (now.getTime() - this.lastLearningSettlementPollAt < intervalMs) return;
    this.lastLearningSettlementPollAt = now.getTime();

    if (this.learningStore) {
      for (const marketId of this.learningStore.unresolvedLearningMarketIds()) {
        const lookup = this.resolveSettlementLookup(marketId);
        // Without authoritative endDate + YES/NO mapping, do not guess after discovery drops the market.
        if (!lookup?.endDate || new Date(lookup.endDate).getTime() > now.getTime()) continue;
        if (!isAuthoritativeBinaryMapping(lookup.yesMapsTo, lookup.noMapsTo)) continue;
        try {
          const raw = await this.resolutionFetcher(lookup.marketId)
            ?? (lookup.slug ? await this.resolutionFetcher(lookup.slug) : null);
          if (!raw) continue;
          this.learningStore.labelMarket(marketId, raw, lookup.yesMapsTo, lookup.noMapsTo, now);
          // CF settlement uses the same authoritative public resolution parse — never invents winners.
          const parsed = parsePublicBinaryResolution(raw, lookup.yesMapsTo, lookup.noMapsTo);
          if (parsed.status === "resolved" && parsed.upPayoff && parsed.downPayoff
            && this.counterfactualStore.isRecoveryReady()) {
            this.counterfactualStore.trySettleMarket(marketId, parsed.upPayoff, parsed.downPayoff, {
              at: now,
              source: parsed.source,
              reason: parsed.reason,
            });
          }
        } catch {
          // Learning labeling failures must not affect economic settlement.
        }
      }
    }

    // Settle open CF positions even when no learning samples exist for the market.
    if (!this.counterfactualStore.isRecoveryReady()) return;
    for (const marketId of this.counterfactualStore.openPositionMarketIds()) {
      const lookup = this.resolveSettlementLookup(marketId);
      if (!lookup?.endDate || new Date(lookup.endDate).getTime() > now.getTime()) continue;
      if (!isAuthoritativeBinaryMapping(lookup.yesMapsTo, lookup.noMapsTo)) continue;
      try {
        const raw = await this.resolutionFetcher(lookup.marketId)
          ?? (lookup.slug ? await this.resolutionFetcher(lookup.slug) : null);
        if (!raw) continue;
        const parsed = parsePublicBinaryResolution(raw, lookup.yesMapsTo, lookup.noMapsTo);
        if (parsed.status === "resolved" && parsed.upPayoff && parsed.downPayoff) {
          this.counterfactualStore.trySettleMarket(marketId, parsed.upPayoff, parsed.downPayoff, {
            at: now,
            source: parsed.source,
            reason: parsed.reason,
          });
        }
      } catch {
        // CF settlement failures must not affect V1 economic settlement.
      }
    }
  }

  private async settleMarket(marketId: string, market: DiscoveredMarket): Promise<void> {
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
    const raw = await this.resolutionFetcher(market.id) ?? (market.slug ? await this.resolutionFetcher(market.slug) : null);
    if (!raw) {
      this.bumpSettlementBackoff(marketId);
      return;
    }
    const binary = market.qualification.binary;
    const parsed = parsePublicBinaryResolution(
      raw,
      binary.yesOutcome === "DOWN" || binary.yesOutcome === "UP" ? binary.yesOutcome : "UP",
      binary.noOutcome === "DOWN" || binary.noOutcome === "UP" ? binary.noOutcome : "DOWN",
    );
    if (parsed.status !== "resolved" || parsed.upPayoff === null || parsed.downPayoff === null || !parsed.winner) {
      this.bumpSettlementBackoff(marketId);
      return;
    }
    const settledAt = this.nowFn();
    const positionBefore = this.ledger.positions.get(marketId);
    const upPayoff = parsed.upPayoff;
    const downPayoff = parsed.downPayoff;
    const asset = this.assetByMarket.get(marketId) ?? market.asset;
    const experimentId = this.experiment?.id ?? null;
    await this.commitAuthoritative({
      operationKey,
      type: "POSITION_SETTLED",
      aggregateType: "position",
      aggregateId: positionBefore?.id ?? marketId,
      occurredAt: settledAt,
      buildPayload: (occurredAt) => {
        const auth = {
          marketId,
          upPayoff,
          downPayoff,
          settledAt: occurredAt.toISOString(),
          winner: parsed.winner,
          slug: market.slug,
          resolution: parsed,
        };
        return {
          ...auth,
          derivedAudit: this.buildSettlementDerivedAudit({
            marketId, asset, experimentId, parsed, positionBefore, upPayoff, downPayoff, occurredAt, auth,
          }),
        };
      },
      afterApply: async () => {
        this.settledMarkets.add(marketId);
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
          const outcome = classifySettledOutcome(realizedDelta, positionBefore.totalFees);
          const grossOutcome = classifySettledOutcome(realizedDelta, "0");
          extra.push({
            id: derivedAuditEventId("POSITION_SETTLED", marketId),
            type: "POSITION_SETTLED",
            aggregateType: "position",
            aggregateId: positionBefore.id,
            traceId: positionBefore.id,
            payload: {
              marketId, asset, cashDelta, realizedDelta,
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
          payload: { ...state.ledger.snapshot(), experimentId, source: "SETTLEMENT", marketId },
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
      buildPayload: () => ({
        experiment: next,
        derivedAudit: [this.experimentDerivedAudit(type, next)],
      }),
      // Keep the durable completion event inside the manifest's authoritative
      // report window.  The manifest timestamp is captured before the durable
      // append; taking a second clock reading here can otherwise place the
      // event just beyond completedAt on a slow PG17 runner.
      occurredAt: next.status === "COMPLETED" && next.completedAt ? new Date(next.completedAt) : undefined,
      checkpoint: true,
    });
    if (next.status === "COMPLETED") {
      this.freezeExperimentResearchIngress();
      this.clearResearchActivationState();
    }
  }

  private sourceSealBlocksAdmission(): boolean {
    return this.sourceSealing && !this.sourceSealed;
  }

  private researchIngressBlocked(): boolean {
    return this.sourceSealing || this.sourceSealed || this.experiment?.status === "COMPLETED";
  }

  private freezeExperimentResearchIngress(): void {
    this.sourceSealing = true;
    this.sourceSealGeneration += 1;
  }

  private beginResearchIngress(): { generation: number } {
    this.researchIngressInFlight += 1;
    return { generation: this.sourceSealGeneration };
  }

  private endResearchIngress(): void {
    this.researchIngressInFlight = Math.max(0, this.researchIngressInFlight - 1);
  }

  private researchIngressStale(generation: number): boolean {
    return this.researchIngressBlocked() || generation !== this.sourceSealGeneration;
  }

  private async waitResearchIngress(deadlineMs: number): Promise<JoinResult> {
    const started = Date.now();
    while (this.researchIngressInFlight > 0) {
      if (Date.now() - started >= deadlineMs) {
        return { drained: false, unresolved: this.researchIngressInFlight, timedOut: true };
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    return { drained: true, unresolved: 0, timedOut: false };
  }

  private async restoreSourceSealState(): Promise<void> {
    const experiment = this.experiment;
    if (!experiment || experiment.status !== "COMPLETED") return;
    const existing = await loadSourceSeal(this.journal, experiment.id);
    if (existing) {
      this.sourceSealed = true;
      this.sourceSealing = false;
      return;
    }
    this.sourceSealing = true;
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
      await this.journal.append(createIntegrityFaultEvent(fault));
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

  ensureSourceSeal(experiment: { id: string; startedAt?: string | null; completedAt?: string | null } = this.experiment!): Promise<{ sealed: boolean; reason: string | null }> {
    // Keep concurrent callers/retries on the same transaction outcome.
    if (this.sourceSealFlight) return this.sourceSealFlight;
    this.sourceSealFlight = this.ensureSourceSealAttempt(experiment).finally(() => { this.sourceSealFlight = null; });
    return this.sourceSealFlight;
  }

  private async ensureSourceSealAttempt(experiment: { id: string; startedAt?: string | null; completedAt?: string | null }): Promise<{ sealed: boolean; reason: string | null }> {
    if (this.unresolvedReportSourceIntegrity(experiment.id).length > 0) {
      return { sealed: false, reason: "REPORT_SOURCE_INTEGRITY_UNRESOLVED" };
    }
    const existing = await loadSourceSeal(this.journal, experiment.id);
    if (existing) {
      this.sourceSealed = true;
      this.sourceSealing = false;
      return { sealed: true, reason: null };
    }
    this.freezeExperimentResearchIngress();
    this.sourceSealPersistCancelled = false;
    this.sourceSealDeadlineAt = Date.now() + this.sourceSealTimeoutMs;
    this.sourceSealAbort = new AbortController();
    const generation = this.sourceSealGeneration;
    const persistAttempt = ++this.sourceSealPersistAttempt;
    const remaining = () => remainingSealMs(this.sourceSealDeadlineAt);
    const work = await this.supervisor.waitUnresolved(remaining());
    const research = await this.waitResearchIngress(remaining());
    const journal = await this.journalAdmission.waitReportInput(remaining());
    if (!work.drained || !research.drained || !journal.drained || work.timedOut || research.timedOut || journal.timedOut || remaining() <= 0) {
      return { sealed: false, reason: "SOURCE_SEAL_TIMEOUT" };
    }
    if (this.unresolvedReportSourceIntegrity(experiment.id).length > 0) {
      return { sealed: false, reason: "REPORT_SOURCE_INTEGRITY_UNRESOLVED" };
    }
    const persist = this.persistSourceSeal(experiment, generation, persistAttempt, this.sourceSealAbort.signal);
    let timeoutId: ReturnType<typeof setTimeout> | undefined;
    const outcome = await Promise.race([
      persist.then((ok) => ({ kind: "done" as const, ok }), (error) =>
        isJournalAppendDeadlineError(error)
          ? { kind: "deadline" as const }
          : { kind: "failed" as const, error }),
      new Promise<{ kind: "timeout" }>((resolve) => {
        timeoutId = setTimeout(() => {
          this.sourceSealAbort?.abort();
          this.abortSourceSealPersist();
          resolve({ kind: "timeout" });
        }, remaining() || 0);
      }),
    ]);
    if (timeoutId) clearTimeout(timeoutId);
    const sealedIfPresent = async (): Promise<{ sealed: true; reason: null } | null> => {
      const already = await loadSourceSeal(this.journal, experiment.id);
      if (!already) return null;
      this.sourceSealed = true;
      this.sourceSealing = false;
      return { sealed: true, reason: null };
    };
    const abortedWithoutCommit = (error: unknown) => (
      isJournalAppendAbortedError(error)
      || isJournalAdmissionRejectedError(error)
    );
    if (outcome.kind === "timeout") {
      this.sourceSealAbort?.abort();
      this.abortSourceSealPersist();
      if (this.sourceSealPersistInFlight) {
        // Abort requests do not establish a transaction outcome. PG17 disables
        // transaction_timeout in the irreversible commit phase: await COMMIT or
        // confirmed abort, even when the wall-clock budget has already elapsed.
        try { await persist; }
        catch (error) {
          const committed = await sealedIfPresent();
          if (committed) return committed;
          if (abortedWithoutCommit(error)) return { sealed: false, reason: "SOURCE_SEAL_TIMEOUT" };
          return { sealed: false, reason: "SOURCE_SEAL_PERSIST_FAILED" };
        }
      }
      return (await sealedIfPresent()) ?? { sealed: false, reason: "SOURCE_SEAL_TIMEOUT" };
    }
    if (outcome.kind === "deadline") {
      return (await sealedIfPresent()) ?? { sealed: false, reason: "SOURCE_SEAL_TIMEOUT" };
    }
    if (outcome.kind === "failed") {
      const committed = await sealedIfPresent();
      if (committed) return committed;
      if (abortedWithoutCommit(outcome.error)) return { sealed: false, reason: "SOURCE_SEAL_TIMEOUT" };
      return { sealed: false, reason: "SOURCE_SEAL_PERSIST_FAILED" };
    }
    if (!outcome.ok) {
      return (await sealedIfPresent()) ?? {
        sealed: false,
        reason: remaining() <= 0 || this.sourceSealPersistCancelled ? "SOURCE_SEAL_TIMEOUT" : "SOURCE_SEAL_PERSIST_FAILED",
      };
    }
    const persisted = await loadSourceSeal(this.journal, experiment.id);
    if (!persisted) return { sealed: false, reason: "SOURCE_SEAL_PERSIST_FAILED" };
    this.sourceSealed = true;
    this.sourceSealing = false;
    return { sealed: true, reason: null };
  }

  private abortSourceSealPersist(): void {
    this.sourceSealPersistCancelled = true;
    this.sourceSealPersistAttempt += 1;
    this.sourceSealGeneration += 1;
  }

  private sourceSealPersistStillValid(generation: number, persistAttempt: number): boolean {
    return !this.sourceSealPersistCancelled
      && generation === this.sourceSealGeneration
      && persistAttempt === this.sourceSealPersistAttempt;
  }

  private async persistSourceSeal(
    experiment: { id: string; startedAt?: string | null; completedAt?: string | null },
    generation: number,
    persistAttempt: number,
    signal?: AbortSignal,
  ): Promise<boolean> {
    if (this.sourceSealPersistHold) await this.sourceSealPersistHold;
    if (!this.sourceSealPersistStillValid(generation, persistAttempt) || signal?.aborted) return false;
    if (remainingSealMs(this.sourceSealDeadlineAt) <= 0) {
      this.abortSourceSealPersist();
      return false;
    }
    if (!this.sourceSealPersistStillValid(generation, persistAttempt) || signal?.aborted) return false;
    if (this.unresolvedReportSourceIntegrity(experiment.id).length > 0) return false;
    const event = createSourceSealEvent({
      experimentId: experiment.id,
      startedAt: experiment.startedAt,
      completedAt: experiment.completedAt,
      persistAttempt,
    });
    this.sourceSealPersistInFlight = true;
    try {
      await this.journalAdmission.run("audit", true, async () => {
        if (signal?.aborted) throw new JournalAppendAbortedError("aborted");
        await this.journal.append(event, { signal, deadlineAt: this.sourceSealDeadlineAt });
      }, signal, { reportInput: false });
    } catch (error) {
      if (isJournalAppendDeadlineError(error)) throw error;
      if (isJournalAppendAbortedError(error) || isJournalAdmissionRejectedError(error)) return false;
      throw error;
    } finally {
      this.sourceSealPersistInFlight = false;
    }
    if (!this.sourceSealPersistStillValid(generation, persistAttempt) || signal?.aborted) return false;
    return Boolean(await loadSourceSeal(this.journal, experiment.id));
  }

  private async writeFinalReport(): Promise<void> {
    if (!this.experiment || this.reportGenerated) return;
    const experiment = this.experiment;
    try {
      if (experiment.status === "COMPLETED") {
        if (this.unresolvedReportSourceIntegrity(experiment.id).length > 0) {
          this.reportGenerated = false;
          return;
        }
        const seal = await this.ensureSourceSeal(experiment);
        if (!seal.sealed) {
          this.reportGenerated = false;
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
        if (this.unresolvedReportSourceIntegrity(experiment.id).length > 0) {
          this.reportGenerated = false;
          return;
        }
        if (!(await loadSourceSeal(this.journal, experiment.id))) {
          this.reportGenerated = false;
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
  setNativeV2ShadowEvalHoldForTest(hold: Promise<void> | null): void { this.nativeV2ShadowEvalHoldForTest = hold; }
  /** Test helper: simulate discovery removing a market from live maps after decision-time capture. */
  dropLiveMarketForTest(marketId: string): void {
    this.marketsById.delete(marketId);
    this.entryQualified.delete(marketId);
    this.managedMarketIds.delete(marketId);
    this.assetByMarket.delete(marketId);
  }
  setTriggerEventPersistHoldForTest(hold: Promise<void> | null): void { this.triggerEventPersistHold = hold; }
  setSourceSealTimeoutForTest(ms: number): void { this.sourceSealTimeoutMs = Math.max(1, ms); }
  setSourceSealPersistHoldForTest(hold: Promise<void> | null): void { this.sourceSealPersistHold = hold; }
  sourceSealPersistInFlightForTest() { return this.sourceSealPersistInFlight; }
  setGrokClaimHoldForTest(hold: Promise<void> | null): void { this.grokClaimHold = hold; }
  setIdleGrokAcceptanceHoldForTest(hold: Promise<void> | null): void { this.idleGrokAcceptanceHoldForTest = hold; }
  setStartPostCommitValidationHoldForTest(hold: Promise<void> | null): void { this.startPostCommitValidationHoldForTest = hold; }
  researchIngressInFlightForTest() { return this.researchIngressInFlight; }
  idleGrokSubmissionCountForTest(): number { return this.idleGrokSubmissions.size; }
  sourceSealStateForTest() { return { sealing: this.sourceSealing, sealed: this.sourceSealed, generation: this.sourceSealGeneration }; }
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
  async ensureSourceSealForTest() {
    if (!this.experiment) throw new Error("NO_EXPERIMENT");
    return this.ensureSourceSeal(this.experiment);
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
    this.nativeV2Shadow.dropMarket(marketId);
    this.nativeV2OpportunityRanker.dropMarket(marketId);
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
  intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK" | "RESCUE_SECOND_LEG",
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
