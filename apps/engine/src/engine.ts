import {
  PaperLedger,
  TemporalPairHedgeStrategy,
  TriggerEngine,
  buildConsensus,
  createId,
  decidePaperOrder,
  defaultConsensusConfig,
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
  triggerEventSchema,
  calculatePairMetrics,
  computePaperExposures,
  projectPaperExposures,
  makerReserveNotional,
  executableFairEdge,
  parsePublicBinaryResolution,
  conservativeRealizableMark,
  classifySettledOutcome,
  settlementCashDelta,
  settlementRealizedDelta,
  DECISION_EVENT_TYPES,
  createIdleExperiment,
  d,
  decimal,
  experimentAllowsFirstLeg,
  experimentAllowsHedge,
  openPositionsRemain,
  shouldWriteFinalReport,
  type ExternalAgentRoleId,
  type Direction,
  type GrokActivationBridge,
  type GrokSignalEnvelope,
  type GrokSignalSubmission,
  type EngineEnv,
  type ExperimentManifest,
  type MarketSnapshot,
  type Outcome,
  type PaperOrder,
  type Position,
  type ReferenceTick,
  type RestingPaperOrder,
  type TriggerEvent,
  type ActivationLatency,
  AssetMarketIndex,
  isMarketDataDerivedTrigger,
} from "@pm/core";
import {
  CHECKPOINT_SCHEMA_VERSION,
  CheckpointFenceError,
  createJournalEvent,
  derivedAuditEventId,
  DurableCursorBehindError,
  IdempotencyConflictError,
  queryEventsPaged,
  type DurableCommitResult,
  type DurableTransition,
  type EventJournal,
} from "@pm/db";
import {
  BtcWebSocketAdapter,
  PolymarketMarketDataAdapter,
  ReferenceFeedRegistry,
  createRestProbe,
  type DiscoveredMarket,
  type FeedConnectionEvent,
  type PolymarketFeedAnomaly,
} from "@pm/market-data";
import { EngineMetrics, classifyJournalMetricClass } from "./metrics.js";
import { RestingOrderTokenIndex } from "./resting-order-index.js";
import { SseHub } from "./server.js";
import { EngineStateStore } from "./state.js";
import { buildExperimentSummary, filterCurrentExperimentEvents, writeExperimentSummary } from "./report.js";
import { GrokActivationCoordinator, persistableActivation } from "./activation/coordinator.js";
import { createGrokActivationBridge } from "./activation/index.js";
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
  private readonly activeTriggers = new Map<string, TriggerEvent>();
  private readonly signalsByTrigger = new Map<string, Map<ExternalAgentRoleId, GrokSignalEnvelope>>();
  private readonly signalReceivedTimes: number[] = [];
  private readonly lastEvaluatedSignalCount = new Map<string, number>();
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
  private readonly referenceResyncInFlight = new Set<string>();
  private readonly referenceResyncRequested = new Set<string>();
  private readonly referenceRequired = new Map<string, boolean>();
  private readonly referenceBlocked = new Map<string, string>();
  private readonly referenceRetryTimers = new Map<string, NodeJS.Timeout>();
  private readonly referenceRetryDeadlines = new Map<string, number>();
  private tokenIds: string[] = [];
  private marketLoopStarted = false;
  private experiment: ExperimentManifest | null = null;
  private checkpointSequence = 0;
  private lastAppliedDurableCursor = 0;
  private derivedAuditCursor = 0;
  private readonly durableGate = new DurableMutationGate();
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
  private readonly shutdownJoinDeadlineMs: number;
  private lastShutdown: JoinResult | null = null;
  private lastJournalShutdown: JoinResult | null = null;
  private lastShutdownCheckpointed = false;
  private maintenanceHold: Promise<void> | null = null;
  private processTriggerHold: Promise<void> | null = null;
  private discoveryGenerationApplied = 0;

  constructor(
    readonly env: EngineEnv,
    private readonly journal: EventJournal,
    private readonly logger: { error(bindings: object, message: string): void },
    adapters?: {
      market?: PolymarketMarketDataAdapter;
      btc?: BtcWebSocketAdapter;
      activation?: GrokActivationBridge;
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
    this.riskLimits = riskLimitsSchema.parse({ maxExternalSignalsPerMinute: env.GROK_MCP_RATE_LIMIT_PER_MINUTE });
    this.marketAdapter = adapters?.market ?? new PolymarketMarketDataAdapter();
    if (this.marketAdapter instanceof PolymarketMarketDataAdapter) {
      this.marketAdapter.setNormalizeObserver(this.metrics.normalizeObserver());
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
    if (adapters?.recoveryPageSize) this.recoveryPageSize = adapters.recoveryPageSize;
    this.writer = new BoundedJournalWriter(journal, (up) => {
      if (up) this.state.database = "UP";
      else this.notePersistenceOutcome("EXPERIMENT_STARTED", false);
    }, logger, {
      queueLimit: env.TELEMETRY_QUEUE_LIMIT, batchSize: env.TELEMETRY_BATCH_SIZE, sampleEvery: 20, maxConcurrent: 2,
      onTelemetryDegraded: (degraded) => { this.state.telemetryDegraded = degraded; },
    }, this.metrics);
    this.shutdownJoinDeadlineMs = adapters?.shutdownJoinDeadlineMs ?? RUNTIME_BOUNDS.shutdownJoinDeadlineMs;
    this.supervisor = new RuntimeSupervisor(this.metrics.runtimeWorkMetrics(), () => this.syncRuntimeState());
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
    this.activation = new GrokActivationCoordinator(
      adapters?.activation ?? createGrokActivationBridge(env),
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
    this.state.recoveryComplete = true;
    const configuredTokens = splitList(this.env.POLYMARKET_TOKEN_IDS);
    if (configuredTokens.length > 0) {
      configuredTokens.forEach((tokenId, index) => this.marketAdapter.registerManualToken(tokenId, tokenId, index % 2 === 0 ? "UP" : "DOWN"));
      this.tokenIds = configuredTokens;
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
  restingOrderCount() { return this.restingOrders.size; }
  reservedCash() { return this.ledger.snapshot().reservedCash; }
  async tickMaintenance(): Promise<void> { await this.runMaintenance(); }
  async applyObservedFill(fill: Parameters<PaperLedger["applyFill"]>[0]): Promise<void> { await this.applyFill(fill); }
  async restMakerForTest(
    order: PaperOrder,
    snapshot: MarketSnapshot,
    intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" = "OPEN_FIRST_LEG",
  ): Promise<void> {
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
    if (this.experiment?.status === "RUNNING" || this.experiment?.status === "ENTRY_CLOSED") {
      return { ok: false, blockers: ["EXPERIMENT_ALREADY_ACTIVE"], experiment: this.experiment };
    }
    if (this.experiment?.status === "COMPLETED" || this.experiment?.status === "FAILED") {
      return { ok: false, blockers: ["EXPERIMENT_ALREADY_COMPLETED"], experiment: this.experiment };
    }
    if (!this.state.newPaperEntryAllowed() || this.overload.state === "FAIL_CLOSED") {
      const preflight = this.preflight("48H_GROK_PAPER_EXPERIMENT");
      return { ...preflight, ok: false, blockers: [...new Set([...preflight.blockers, "OVERLOAD_FAIL_CLOSED"])] };
    }
    const preflight = this.preflight("48H_GROK_PAPER_EXPERIMENT");
    await this.safeAppend(createJournalEvent({
      traceId: createId("trace"), type: "EXPERIMENT_PREFLIGHT", aggregateType: "experiment", aggregateId: "preflight",
      payload: preflight as unknown as Record<string, unknown>,
    }));
    if (!preflight.ok) return { ...preflight, ok: false };
    const now = this.nowFn();
    const next = startExperiment({
      id: this.experiment?.id, durationHours: this.env.EXPERIMENT_DURATION_HOURS, strategyVersion: this.strategy.version,
      riskConfiguration: this.riskLimits, modelConfiguration: { provider: "external_grok_bot_mcp", logicalRoles: 6, localModelExecution: false },
      startingBalance: this.ledger.snapshot().startingBalance, systemVersion: "0.1.0",
      gitSha: process.env.GIT_SHA ?? process.env.FLY_IMAGE_REF ?? "unknown", now,
      closePolicy: this.env.EXPERIMENT_CLOSE_POLICY,
    });
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
      checkpoint: true,
      afterApply: async () => {
        this.reportGenerated = false;
        this.record("EXPERIMENT_STARTED", next.id, null, { status: next.status });
      },
    });
    return { ok: true, experiment: this.experiment, preflight };
  }

  preflight(mode: "MONITOR_ONLY" | "48H_GROK_PAPER_EXPERIMENT" = "MONITOR_ONLY") {
    return evaluateExperimentPreflight({
      env: this.env, databaseUp: this.state.database === "UP", discoveryStatus: this.state.discoveryStatus,
      qualifiedOrManaged: this.marketsById.size, bootstrapFailed: this.bootstrapFailed,
      tokenConfiguredPath: this.tokenIds.length > 0,
      mcpRoleTokens: this.env.GROK_MCP_BEARER_TOKENS.split(",").map((entry) => entry.trim().split(":")[0] ?? "").filter(Boolean),
      mode,
      overloadFailClosed: this.overload.state === "FAIL_CLOSED",
      entryMarkets: [...this.entryQualified].map((marketId) => {
        const market = this.marketsById.get(marketId);
        const asset = this.assetByMarket.get(marketId) ?? market?.asset ?? null;
        const blocked = asset ? this.referenceBlocked.get(asset) ?? null : null;
        const up = market?.tokens.find((token) => token.outcome === "UP")?.tokenId ?? null;
        const down = market?.tokens.find((token) => token.outcome === "DOWN")?.tokenId ?? null;
        const monitored = Boolean(up && down && this.tokenIds.includes(up) && this.tokenIds.includes(down));
        return {
          marketId, asset, referenceRequired: this.referenceRequired.get(marketId) === true, blockedStatus: blocked,
          yesTokenId: up, noTokenId: down, endDate: market?.endDate ?? null,
          feeScheduleKnown: market?.feeSchedule != null,
          binarySafe: market?.qualification.binary.safe === true
            && market.qualification.binary.yesOutcome !== market.qualification.binary.noOutcome,
          monitored,
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
      onConnection: (event) => this.onConnection(event),
      onError: (error) => this.onFeedError("polymarket", error),
      onAnomaly: (anomaly) => this.onPolymarketAnomaly(anomaly),
      onGap: (event) => {
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
    this.marketAdapter.setTokenIds(nextTokens);
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
      this.referenceResyncRequested.delete(asset);
      this.referenceResyncInFlight.delete(asset);
      this.state.dropReference(asset);
      this.clearReferenceRetry(asset);
    }
    this.metrics.assetMarketIndexEntries.set(this.assetIndex.entryCount());
    for (const [triggerId, trigger] of [...this.activeTriggers]) {
      if (!trigger.marketId || !removedMarkets.includes(trigger.marketId)) continue;
      this.activeTriggers.delete(triggerId);
      this.signalsByTrigger.delete(triggerId);
      this.lastEvaluatedSignalCount.delete(triggerId);
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
    if (outcome === "started") {
      await this.safeAppend(createJournalEvent({
        traceId: createId("trace"), type: "REFERENCE_FEED_STARTED", aggregateType: "reference", aggregateId: asset,
        payload: { asset, symbol: resolution.symbol, sharedByMarkets: markets },
      }));
      this.record("REFERENCE_FEED_STARTED", createId("trace"), null, { asset, markets }, asset);
    }
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
    afterApply?: (transition: DurableTransition) => Promise<void>;
    checkpoint?: boolean;
  }): Promise<DurableCommitResult> {
    return this.durableGate.run(async () => {
      const occurredAt = input.occurredAt ?? this.nowFn();
      this.crashInjector.maybeCrash("BEFORE_DURABLE_COMMIT", input.operationKey);
      let commit: DurableCommitResult;
      try {
        const payload = await this.resolveDurablePayload(input, occurredAt);
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
      await this.emitStoredDerivedAudit(commit.transition);
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
      || durableIdentityConflicts(input.type, existing.payload, computed)
    ) {
      throw new IdempotencyConflictError(input.operationKey, existing.type, input.type);
    }
    return existing.payload;
  }

  private async restoreAndRecover(): Promise<void> {
    const started = performance.now();
    try {
      const checkpoint = await this.journal.loadLatestCheckpoint();
      if (checkpoint) {
        const restored = this.hydrateCheckpointPayload(checkpoint.payload as Record<string, unknown>);
        const payload = checkpoint.payload as Record<string, unknown>;
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
          riskConfiguration: this.riskLimits, modelConfiguration: { provider: "external_grok_bot_mcp", logicalRoles: 6, localModelExecution: false },
          startingBalance: this.ledger.snapshot().startingBalance, systemVersion: "0.1.0",
          gitSha: process.env.GIT_SHA ?? process.env.FLY_IMAGE_REF ?? "unknown",
        });
        this.lastAppliedDurableCursor = 0;
        await this.replayDurableTail(0);
        await this.repairAllDerivedAudit();
        const latest = await this.journal.latestDurableSequence();
        const report = validatePostReplayInvariants(this.recoverableBag(), this.lastAppliedDurableCursor, latest);
        if (!report.ok) throw new RecoveryFailClosedError("invariant", report.reasons.join(","));
        this.syncRecoveredProjection();
      }
      this.syncExperimentWindow();
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

  private hydrateCheckpointPayload(payload: Record<string, unknown>): {
    positions: number; restingOrders: number; activeTriggers: number; grokSignals: number;
  } {
    const ledger = ledgerStateSchema.parse(payload.ledger);
    const positions = Array.isArray(payload.positions) ? payload.positions.map((item) => positionSchema.parse(item)) : [];
    this.ledger.restore(ledger, positions);
    this.state.setLedger(this.ledger.snapshot());
    this.experiment = payload.experiment ? experimentManifestSchema.parse(payload.experiment) : createIdleExperiment({
      durationHours: this.env.EXPERIMENT_DURATION_HOURS, strategyVersion: this.strategy.version,
      riskConfiguration: this.riskLimits, modelConfiguration: {}, startingBalance: this.ledger.snapshot().startingBalance,
      systemVersion: "0.1.0", gitSha: "unknown",
    });
    const resting = Array.isArray(payload.restingOrders) ? payload.restingOrders.map((item) => restingPaperOrderSchema.parse(item)) : [];
    for (const item of resting) this.restingOrders.set(item.order.id, item);
    this.syncRestingIndexFromOrders();
    const reservations = payload.makerReservations && typeof payload.makerReservations === "object"
      ? payload.makerReservations as Record<string, string> : {};
    for (const [orderId, amount] of Object.entries(reservations)) this.makerReservations.set(orderId, amount);
    const triggers = Array.isArray(payload.activeTriggers) ? payload.activeTriggers.map((item) => triggerEventSchema.parse(item)) : [];
    for (const trigger of triggers) { this.activeTriggers.set(trigger.id, trigger); this.state.addTrigger(trigger); }
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
      }, signal);
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

  private experimentDerivedAudit(type: "EXPERIMENT_STARTED" | "EXPERIMENT_ENTRY_CLOSED" | "EXPERIMENT_COMPLETED", experiment: ExperimentManifest): DerivedAuditRecord {
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
    if (!this.tokenIds.includes(snapshot.tokenId)) return;
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
    if (snapshot.asset) {
      this.assetByMarket.set(snapshot.marketId, snapshot.asset);
      this.assetIndex.add(snapshot.asset, snapshot.marketId);
      this.metrics.assetMarketIndexEntries.set(this.assetIndex.entryCount());
    }
    this.updateReady();
    await this.writer.persist(createJournalEvent({ traceId: snapshot.traceId, type: "MARKET_SNAPSHOT", aggregateType: "market", aggregateId: snapshot.marketId, payload: snapshot, occurredAt: new Date(snapshot.sourceTimestamp), sourceEventId: snapshot.eventId }));
    this.record("MARKET_EVENT", snapshot.traceId, snapshot.marketId, { tokenId: snapshot.tokenId, outcome: snapshot.outcome, sequence: snapshot.sequence }, snapshot.asset ?? null);

    if (synchronized && previous) {
      for (const orderId of this.restingIndex.orderIdsFor(snapshot.tokenId)) {
        const resting = this.restingOrders.get(orderId);
        if (!resting || resting.order.tokenId !== snapshot.tokenId) continue;
        const evolved = evolveMakerOrder(resting, previous, snapshot);
        if (evolved.fill) {
          await this.applyFill(evolved.fill, { restingAfter: evolved.resting });
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
      const triggers = this.triggerEngine.ingestMarket(snapshot);
      for (const trigger of triggers) this.admitTrigger(trigger);
    }
    const window = this.marketsById.get(snapshot.marketId);
    if (window?.endDate) this.triggerEngine.noteMarketWindow(snapshot.marketId, snapshot.asset ?? window.asset, window.endDate);
    this.metrics.triggerDedupeEntries.set(this.triggerEngine.dedupeEntryCount());
    this.state.eventLatencyMs = performance.now() - started;
    this.metrics.eventLatency.observe((performance.now() - started) / 1000);
  }

  private async onReferenceTick(tick: ReferenceTick): Promise<void> {
    if (!this.state.recoveryComplete) return;
    if (this.usesDiscovery() && !this.assetIndex.hasAsset(tick.asset)) return;
    this.metrics.referenceTicks.inc();
    const started = performance.now();
    if (this.triggerEngine.referenceWindowOverflowed(tick.asset)) {
      this.markReferenceUnreliable(tick.asset);
      this.state.invalidateReferenceContext(tick.asset);
      this.requestReferenceResync(tick.asset);
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
    await this.writer.persist(createJournalEvent({ traceId: tick.traceId, type: "REFERENCE_TICK", aggregateType: "reference", aggregateId: tick.asset, payload: tick, occurredAt: new Date(tick.sourceTimestamp), sourceEventId: tick.eventId }));
    const triggers = this.triggerEngine.ingestReference(tick);
    if (this.triggerEngine.referenceWindowOverflowed(tick.asset)) {
      this.markReferenceUnreliable(tick.asset);
      this.state.invalidateReferenceContext(tick.asset);
      this.requestReferenceResync(tick.asset);
      this.metrics.referenceWindowEntries.set(this.triggerEngine.referenceWindowTotalEntries());
      this.metrics.referenceTickProcessingDuration.observe((performance.now() - started) / 1000);
      return;
    }
    if (wasUnreliable) {
      this.referenceUnreliable.delete(tick.asset);
      this.referenceResyncRequested.delete(tick.asset);
    }
    this.metrics.referenceWindowEntries.set(this.triggerEngine.referenceWindowTotalEntries());
    for (const trigger of triggers) this.admitTrigger(trigger);
    this.metrics.triggerDedupeEntries.set(this.triggerEngine.dedupeEntryCount());
    this.metrics.referenceTickProcessingDuration.observe((performance.now() - started) / 1000);
  }

  private admitTrigger(trigger: TriggerEvent): AdmitResult {
    if (this.started && !this.state.recoveryComplete) {
      return { admitted: false, reason: "fail_closed", done: Promise.resolve() };
    }
    const lane = triggerWorkLane(trigger.kind);
    const result = this.supervisor.admit({
      id: trigger.id,
      lane,
      key: triggerWorkKey(trigger),
      estimatedBytes: RUNTIME_BOUNDS.triggerByteEstimate,
      critical: lane === "p0",
      run: async (signal) => {
        if (signal.aborted) return;
        await this.processTrigger(trigger, signal);
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

  private async processTrigger(trigger: TriggerEvent, signal?: AbortSignal): Promise<void> {
    this.metrics.noteProcessTriggerStart();
    const started = performance.now();
    try {
    if (signal?.aborted) return;
    if (this.processTriggerHold) await this.processTriggerHold;
    if (signal?.aborted) return;
    if (this.shouldSuppressStaleMarketTrigger(trigger)) {
      this.metrics.triggersSuppressed.inc({ kind: trigger.kind, severity: String(trigger.severity) });
      return;
    }
    this.metrics.triggers.inc({ kind: trigger.kind, severity: String(trigger.severity) });
    this.record(`TRIGGER_LEVEL_${trigger.severity}`, trigger.traceId, trigger.marketId, { triggerId: trigger.id, kind: trigger.kind, asset: trigger.asset ?? null }, trigger.asset ?? null);
    await this.safeAppend(createJournalEvent({ traceId: trigger.traceId, type: "TRIGGER_EVENT", aggregateType: "trigger", aggregateId: trigger.id, payload: trigger, sourceEventId: trigger.sourceEventIds[0] ?? null }), signal);
    this.activeTriggers.set(trigger.id, trigger);
    this.state.addTrigger(trigger);
    this.record("GROK_RESEARCH_AVAILABLE", trigger.traceId, trigger.marketId, { triggerId: trigger.id, expiresAt: trigger.expiresAt }, trigger.asset ?? null);
    await this.requestActivation(trigger);
    } finally {
      this.metrics.noteProcessTriggerEnd(started);
    }
  }

  private async requestActivation(trigger: TriggerEvent): Promise<void> {
    const status = this.experiment?.status ?? "IDLE";
    if (status === "IDLE" || status === "COMPLETED" || status === "FAILED") {
      const bookkeepingStarted = performance.now();
      this.activation.ensureLatency(trigger);
      this.state.setActivationLatency(this.activation.snapshot());
      await this.safeAppend(createJournalEvent({
        traceId: trigger.traceId, type: "GROK_ACTIVATION_DECISION", aggregateType: "activation", aggregateId: trigger.id,
        payload: { triggerId: trigger.id, marketId: trigger.marketId, asset: trigger.asset ?? null, reason: "EXPERIMENT_NOT_ACTIVE", activate: false },
      }));
      this.state.addActivation({ triggerId: trigger.id, reason: "EXPERIMENT_NOT_ACTIVE", at: this.nowFn().toISOString(), provider: this.env.GROK_ACTIVATION_PROVIDER, grokWakeObserved: false });
      this.metrics.idleActivationBookkeepingDuration.observe((performance.now() - bookkeepingStarted) / 1000);
      return;
    }
    if (status === "ENTRY_CLOSED" && !isRiskHedgeSettlementTrigger(trigger.kind)) {
      await this.safeAppend(createJournalEvent({
        traceId: trigger.traceId, type: "GROK_ACTIVATION_DECISION", aggregateType: "activation", aggregateId: trigger.id,
        payload: { triggerId: trigger.id, marketId: trigger.marketId, asset: trigger.asset ?? null, reason: "ENTRY_CLOSED", activate: false },
      }));
      this.state.addActivation({ triggerId: trigger.id, reason: "ENTRY_CLOSED", at: this.nowFn().toISOString(), provider: this.env.GROK_ACTIVATION_PROVIDER, grokWakeObserved: false });
      return;
    }
    const decision = this.activation.decide(trigger);
    await this.safeAppend(createJournalEvent({
      traceId: trigger.traceId, type: "GROK_ACTIVATION_DECISION", aggregateType: "activation", aggregateId: trigger.id,
      payload: { triggerId: trigger.id, marketId: trigger.marketId, asset: trigger.asset ?? null, reason: decision.reason, activate: decision.activate },
    }));
    if (!decision.activate || !decision.request) {
      this.state.addActivation({ triggerId: trigger.id, reason: decision.reason, at: new Date().toISOString(), provider: this.env.GROK_ACTIVATION_PROVIDER, grokWakeObserved: false });
      return;
    }
    this.activation.markRequested(trigger, decision.request);
    this.state.setActivationLatency(this.activation.snapshot());
    const results = await this.activation.dispatch(decision.request);
    for (const result of results) {
      this.metrics.activations.inc({ provider: result.provider, status: result.ok ? "ok" : "failed" });
      await this.safeAppend(createJournalEvent({
        traceId: trigger.traceId, type: "GROK_ACTIVATION_RESULT", aggregateType: "activation", aggregateId: trigger.id,
        payload: persistableActivation(result, trigger.id, decision.reason),
      }));
      this.state.addActivation({
        triggerId: trigger.id, role: result.targetRole, provider: result.provider, ok: result.ok, skipped: result.skipped,
        errorCode: result.errorCode, grokWakeObserved: false, at: result.sentAt ?? result.requestedAt,
      });
    }
    this.state.setActivationLatency(this.activation.snapshot());
  }

  /** The only ingress for external Grok research; it cannot create orders directly. */
  async submitGrokSignal(raw: GrokSignalSubmission, clientId: string): Promise<{ status: "accepted" | "duplicate" | "stale" | "unknown_trigger" | "trace_mismatch" | "market_mismatch" | "asset_mismatch" | "replay_rejected" | "malformed" | "identity_mismatch"; signalId?: string }> {
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
    const now = new Date();
    const trigger = this.activeTriggers.get(submission.triggerId);
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
    const claimed = await this.journal.claimIdempotency("grok_signal_submission", `${clientId}:${submission.submissionId}`);
    if (!claimed) { this.state.recordGrokRejection("duplicate"); this.metrics.grokSignals.inc({ agent: submission.agentId, status: "duplicate" }); return { status: "duplicate" }; }

    const signal: GrokSignalEnvelope = grokSignalEnvelopeSchema.parse({
      signalId: createId("grok_signal"), submissionId: submission.submissionId, traceId: submission.traceId,
      triggerId: submission.triggerId, correlationId: submission.correlationId, marketId: submission.marketId,
      asset: submission.asset ?? trigger.asset ?? null,
      agentId: submission.agentId, role: submission.role, provider: "grok_bot_mcp", clientId,
      receivedAt: now.toISOString(), timestamp: submission.timestamp, output: submission.output, resultingDecision: null,
    });
    const byRole = this.signalsByTrigger.get(trigger.id) ?? new Map<ExternalAgentRoleId, GrokSignalEnvelope>();
    byRole.set(signal.agentId, signal);
    this.signalsByTrigger.set(trigger.id, byRole);
    this.signalReceivedTimes.push(now.getTime());
    this.state.addGrokSignal(signal);
    this.activation.noteSignal(trigger.id, now);
    this.state.setActivationLatency(this.activation.snapshot());
    this.metrics.grokSignals.inc({ agent: signal.agentId, status: "accepted" });
    this.record(`${signal.agentId.toUpperCase()}_COMPLETE`, signal.traceId, trigger.marketId, { signalId: signal.signalId, clientId, confidence: submission.output.confidence, expiresAt: submission.output.expiresAt }, trigger.asset ?? null);
    await this.safeAppend(createJournalEvent({ traceId: signal.traceId, type: "GROK_SIGNAL_ACCEPTED", aggregateType: "grok_signal", aggregateId: signal.signalId, payload: signal }));
    if (submission.agentId === "strategy_supervisor" && submission.output.proposedChange) {
      await this.safeAppend(createJournalEvent({ traceId: signal.traceId, type: "STRATEGY_CHANGE_PROPOSED", aggregateType: "strategy_change", aggregateId: signal.signalId, payload: submission.output.proposedChange }));
    }
    await this.evaluateExternalConsensus(trigger);
    return { status: "accepted", signalId: signal.signalId };
  }

  /** Public for deterministic trigger ingestion and focused integration tests. */
  async registerTriggerForResearch(trigger: TriggerEvent): Promise<void> {
    const result = this.admitTrigger(triggerEventSchema.parse(trigger));
    if (result.admitted) await result.done;
  }

  private async evaluateExternalConsensus(trigger: TriggerEvent): Promise<void> {
    const outputs = [...(this.signalsByTrigger.get(trigger.id)?.values() ?? [])];
    const fair = outputs.find((item) => item.agentId === "fair_probability_analyst")?.output as { estimatedEdge?: number; fairProbability?: number; direction?: Direction; marketProbability?: number } | undefined;
    const directional = outputs.flatMap((item) => {
      const output = item.output as { direction?: unknown; confidence?: unknown; expiresAt?: unknown; evidenceIds?: unknown };
      return (output.direction === "UP" || output.direction === "DOWN" || output.direction === "FLAT") && typeof output.confidence === "number"
        && typeof output.expiresAt === "string" && Array.isArray(output.evidenceIds) && new Date(output.expiresAt).getTime() > this.nowFn().getTime()
        ? [{ agentId: item.agentId, direction: output.direction as Direction, confidence: output.confidence, expiresAt: output.expiresAt, evidenceIds: output.evidenceIds.map(String) }]
        : [];
    });
    if (!fair || typeof fair.fairProbability !== "number" || directional.length < 2 || this.lastEvaluatedSignalCount.get(trigger.id) === outputs.length) return;
    this.lastEvaluatedSignalCount.set(trigger.id, outputs.length);
    const consensus = buildConsensus({ traceId: trigger.traceId, triggerId: trigger.id, signals: directional, deterministicEdge: 0, now: this.nowFn(), config: defaultConsensusConfig });
    this.state.latestConsensus = consensus;
    this.record("CONSENSUS_COMPLETE", trigger.traceId, trigger.marketId, { direction: consensus.consensusDirection, confidence: consensus.consensusConfidence, conflicts: consensus.conflicts, triggerId: trigger.id, estimatedEdgeIgnored: fair.estimatedEdge ?? null }, trigger.asset ?? null);
    await this.safeAppend(createJournalEvent({ traceId: trigger.traceId, type: "CONSENSUS_DECISION", aggregateType: "consensus", aggregateId: consensus.id, payload: { ...consensus, marketId: trigger.marketId, asset: trigger.asset ?? null } }));
    for (const disagreement of this.triggerEngine.ingestConsensus(consensus, trigger.marketId, trigger.asset ?? null)) this.admitTrigger(disagreement);
    if (trigger.marketId) await this.evaluateTrade(trigger, consensus, outputs);
  }

  private async evaluateTrade(trigger: TriggerEvent, consensus: NonNullable<EngineStateStore["latestConsensus"]>, outputs: GrokSignalEnvelope[]): Promise<void> {
    const pair = this.snapshots.get(trigger.marketId ?? "");
    if (!pair) return;
    const position = this.ledger.positions.get(trigger.marketId ?? "") ?? null;
    if (this.experiment) {
      const next = refreshExperiment(this.experiment, this.nowFn(), { openPositions: openPositionsRemain(this.ledger.positions.values()) });
      if (next.status !== this.experiment.status) await this.commitExperimentStatus(next);
    }
    const candidate = this.strategy.evaluate({
      consensus, snapshots: { UP: pair.UP ?? null, DOWN: pair.DOWN ?? null }, position,
      maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd, minimumLockedPairEdge: this.riskLimits.minExpectedEdge,
    });
    if (!candidate) return;
    if (!this.adapterTokenSynchronized(candidate.tokenId)) return;
    if (candidate.intent === "OPEN_FIRST_LEG" && this.overload.state === "FAIL_CLOSED") return;
    if (candidate.intent === "OPEN_FIRST_LEG" && !experimentAllowsFirstLeg(this.experiment)) return;
    if (candidate.intent === "OPEN_FIRST_LEG" && !this.entryQualified.has(candidate.marketId)) return;
    if (candidate.intent === "OPEN_FIRST_LEG" && !this.isMarketSynchronized(candidate.marketId)) return;
    if (candidate.intent === "OPEN_FIRST_LEG" && assetNeedsReliableReference(this.referenceRequired.get(candidate.marketId), this.referenceUnreliable, this.assetByMarket.get(candidate.marketId))) return;
    if (candidate.intent === "HEDGE_SECOND_LEG" && !experimentAllowsHedge(this.experiment)) return;
    const snapshot = this.state.marketSnapshots.get(candidate.tokenId);
    if (!snapshot) return;
    const fair = outputs.find((item) => item.agentId === "fair_probability_analyst")?.output as { direction?: Direction; fairProbability?: number; marketProbability?: number; estimatedEdge?: number } | undefined;
    if (candidate.intent === "OPEN_FIRST_LEG" && fair && typeof fair.fairProbability === "number" && (fair.direction === "UP" || fair.direction === "DOWN")) {
      const executable = executableFairEdge({
        fair: { direction: fair.direction, fairProbability: fair.fairProbability, marketProbability: fair.marketProbability, estimatedEdge: fair.estimatedEdge },
        consensus, snapshot, quantity: candidate.quantity, limitPrice: candidate.limitPrice,
      });
      if (!executable) return;
      candidate.rawExpectedEdge = executable.rawEdge;
      consensus.estimatedEdge = executable.rawEdge;
    }
    const duplicateKey = `${candidate.marketId}:${candidate.outcome}:${candidate.intent}`;
    const now = this.nowFn();
    const externalRisk = outputs.find((item) => item.agentId === "risk_hedge_analyst")?.output as { recommendVeto?: boolean } | undefined;
    const earliestSignalExpiry = outputs.reduce((earliest, item) => {
      const expiresAt = (item.output as { expiresAt?: string }).expiresAt;
      return expiresAt && new Date(expiresAt).getTime() < earliest.getTime() ? new Date(expiresAt) : earliest;
    }, new Date(trigger.expiresAt));
    const asset = trigger.asset ?? this.assetByMarket.get(candidate.marketId) ?? snapshot.asset ?? null;
    const exposure = computePaperExposures(this.ledger.positions.values(), candidate.marketId, this.assetByMarket, now.getTime());
    const projected = projectPaperExposures({
      current: exposure,
      positions: projectableLegsFromPositions(this.ledger.positions.values(), this.assetByMarket),
      candidate: {
        marketId: candidate.marketId, outcome: candidate.outcome, quantity: candidate.quantity,
        price: candidate.limitPrice, intent: candidate.intent, asset,
      },
      nowMs: now.getTime(),
    });
    const referenceRequired = this.referenceRequired.get(candidate.marketId) === true;
    const referenceFeedAt = asset ? dateOrNull(this.state.referenceLastMessageAt(asset)) : null;
    const blocked = asset ? this.referenceBlocked.get(asset) : undefined;
    const marketFeedAt = collectMarketFeedAt(this.snapshots.get(candidate.marketId), candidate.outcome, candidate.intent, snapshot.receivedAt);
    const decision = decidePaperOrder({
      candidate, consensus, snapshotAfterDecisionLatency: snapshot, limits: this.riskLimits, triggerId: trigger.id,
      signalExpiresAt: earliestSignalExpiry, orderType: this.env.PAPER_EXECUTION_STYLE,
      baseRisk: {
        now, marketFeedAt,
        btcFeedAt: asset === "BTC" ? referenceFeedAt ?? dateOrNull(this.state.lastBtcMessageAt) : now,
        referenceFeedAt, referenceRequired, asset, assetExposureUsd: exposure.asset,
        databaseAvailable: await this.journal.healthCheck(), timestampDisorder: this.timestampDisorder, riskStateAvailable: true,
        dailyPnlUsd: this.ledger.snapshot().netTradingPnl, drawdownUsd: this.ledger.snapshot().drawdown,
        marketExposureUsd: exposure.market, totalExposureUsd: exposure.total, directionalExposureUsd: exposure.directional,
        unhedgedExposureUsd: exposure.unhedged, unhedgedDurationMs: exposure.unhedgedDurationMs,
        intent: candidate.intent,
        projectedMarketExposureUsd: projected.market, projectedTotalExposureUsd: projected.total,
        projectedDirectionalExposureUsd: projected.directional, projectedUnhedgedExposureUsd: projected.unhedged,
        projectedAssetExposureUsd: projected.asset, availableCashUsd: this.ledger.availableCash().toFixed(),
        referenceProviderBlocked: Boolean(blocked),
        marketEntryQualified: this.entryQualified.has(candidate.marketId),
        externalSignalsLastMinute: this.signalReceivedTimes.filter((at) => now.getTime() - at < 60_000).length,
        lastStrategyTradeAt: this.lastTradeAt,
        duplicateTrade: (this.recentTrades.get(duplicateKey) ?? 0) > now.getTime() - this.riskLimits.strategyCooldownMs,
        experimentEntryOpen: candidate.intent === "HEDGE_SECOND_LEG" ? experimentAllowsHedge(this.experiment) : experimentAllowsFirstLeg(this.experiment),
        executionErrorsLastHour: this.executionErrors.filter((at) => now.getTime() - at < 3_600_000).length,
        externalRiskRecommendedVeto: externalRisk?.recommendVeto === true, shuttingDown: this.state.draining,
      },
    });
    this.state.latestRisk = decision.risk;
    await this.safeAppend(createJournalEvent({ traceId: trigger.traceId, type: "RISK_EVALUATION", aggregateType: "risk", aggregateId: decision.risk.id, payload: { ...decision.risk, marketId: trigger.marketId, asset } }));
    if (!decision.accepted || !decision.order) {
      for (const reason of decision.risk.vetoes) this.metrics.riskVetoes.inc({ reason });
      this.record("RISK_VETO", trigger.traceId, trigger.marketId, { reasons: decision.risk.vetoes, netExpectedEdge: decision.netExpectedEdge }, asset);
      return;
    }
    this.record("RISK_ACCEPT", trigger.traceId, trigger.marketId, { orderId: decision.order.id, netExpectedEdge: decision.netExpectedEdge }, asset);
    await this.submitPaperOrder(decision.order, snapshot, duplicateKey, candidate.intent);
  }

  private async submitPaperOrder(order: PaperOrder, decisionSnapshot: MarketSnapshot, duplicateKey: string, intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" = "OPEN_FIRST_LEG"): Promise<void> {
    this.metrics.paperOrders.inc({ type: order.orderType });
    this.record("PAPER_ORDER_CREATED", order.traceId, order.marketId, { orderId: order.id, type: order.orderType, intent }, decisionSnapshot.asset ?? null);
    await this.safeAppend(createJournalEvent({
      id: derivedAuditEventId("PAPER_ORDER_CREATED", order.id),
      traceId: order.traceId, type: "PAPER_ORDER_CREATED", aggregateType: "paper_order", aggregateId: order.id,
      payload: { ...order, intent },
    }));
    this.recentTrades.set(duplicateKey, Date.now());
    try {
      if (order.orderType === "MAKER") {
        await this.reserveMaker(order, decisionSnapshot, intent);
        return;
      }
      await delay(this.env.PAPER_EXECUTION_LATENCY_MS);
      const current = this.state.marketSnapshots.get(order.tokenId);
      if (!current || new Date(current.receivedAt).getTime() < new Date(decisionSnapshot.receivedAt).getTime()) throw new Error("STALE_EXECUTION_SNAPSHOT");
      await this.applyFill(executePaperTaker({ order, snapshotAfterLatency: current, measuredLatencyMs: this.env.PAPER_EXECUTION_LATENCY_MS }));
      this.lastTradeAt = this.nowFn();
    } catch (error) {
      if (this.overload.hasIntegrityFault || error instanceof IdempotencyConflictError || error instanceof CrashInjectedError || (error instanceof JournalAdmissionRejected && error.critical)) throw error;
      this.executionErrors.push(Date.now());
      this.record("PAPER_EXECUTION_ERROR", order.traceId, order.marketId, { orderId: order.id, message: error instanceof Error ? error.message : String(error) });
      await this.safeAppend(createJournalEvent({ traceId: order.traceId, type: "PAPER_EXECUTION_ERROR", aggregateType: "paper_order", aggregateId: order.id, payload: { message: error instanceof Error ? error.message : String(error) } }));
    }
  }

  private async reserveMaker(
    order: PaperOrder,
    snapshot: MarketSnapshot,
    intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" = "OPEN_FIRST_LEG",
  ): Promise<void> {
    const notional = makerReserveNotional(order.quantity, order.requestedPrice, snapshot.feeSchedule);
    if (this.ledger.availableCash().lt(notional) && !(await this.journal.readDurableByOperationKey(`maker:${order.id}:reserve`))) {
      throw new Error("INSUFFICIENT_CASH");
    }
    const resting = createRestingMakerOrder(order, snapshot);
    const auth = { orderId: order.id, reserved: notional, resting, intent };
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
        orderId, released, cancel: options.cancel === true, reason: options.reason,
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

  private async applyFill(fill: Parameters<PaperLedger["applyFill"]>[0], extras: { restingAfter?: RestingPaperOrder | null } = {}): Promise<void> {
    const reserved = this.makerReservations.get(fill.orderId);
    let reservationAfter: string | null = reserved ?? null;
    if (reserved && fill.liquidityRole === "MAKER") {
      const fillCost = d(fill.quantity).mul(fill.fillPrice).plus(fill.feeUsd);
      const next = d(reserved).minus(fillCost);
      reservationAfter = next.gt(0) ? decimal(next) : null;
    }
    const previous = this.ledger.positions.get(fill.marketId);
    const wasEmpty = !previous || (!d(previous.upQuantity).gt(0) && !d(previous.downQuantity).gt(0));
    const positionId = previous?.id ?? stablePositionId(fill.marketId);
    const restingAfter = this.computeMakerRestingAfter(fill, extras);
    await this.commitAuthoritative({
      operationKey: `fill:${fill.id}`,
      type: "PAPER_FILL",
      aggregateType: "paper_fill",
      aggregateId: fill.id,
      buildPayload: (occurredAt) => {
        const auth = { fill, positionId, reservationAfter, restingAfter, wasEmpty };
        return {
          ...auth,
          derivedAudit: this.projectDerivedAudit(
            { operationKey: `fill:${fill.id}`, type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: fill.id, payload: auth },
            occurredAt,
            (projected) => this.buildFillDerivedAudit(fill, wasEmpty, projected, occurredAt),
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
  }

  private buildFillDerivedAudit(
    fill: Parameters<PaperLedger["applyFill"]>[0],
    wasEmpty: boolean,
    projected: RecoverableEngineState,
    occurredAt: Date,
  ): DerivedAuditRecord[] {
    const position = projected.ledger.positions.get(fill.marketId);
    const experimentId = projected.experiment?.id ?? this.experiment?.id ?? null;
    const records: DerivedAuditRecord[] = [{
      id: derivedAuditEventId("PAPER_FILL", fill.id),
      type: "PAPER_FILL",
      aggregateType: "paper_fill",
      aggregateId: fill.id,
      traceId: fill.traceId,
      payload: { ...fill, asset: this.assetByMarket.get(fill.marketId) ?? null },
      sourceEventId: fill.sourceMarketEventId,
    }];
    if (wasEmpty && fill.side === "BUY" && position) {
      records.push({
        id: derivedAuditEventId("FIRST_LEG_FILLED", fill.id),
        type: "FIRST_LEG_FILLED",
        aggregateType: "position",
        aggregateId: position.id,
        traceId: fill.traceId,
        payload: { positionId: position.id, marketId: fill.marketId, unpairedQuantity: position.unpairedQuantity, experimentId },
      });
    }
    if (position && Number(position.pairedQuantity) > 0 && Number(position.unpairedQuantity) === 0) {
      const unhedgedDurationMs = Math.max(0, occurredAt.getTime() - new Date(position.openedAt).getTime());
      records.push({
        id: derivedAuditEventId("HEDGE_COMPLETE", fill.id),
        type: "HEDGE_COMPLETE",
        aggregateType: "position",
        aggregateId: position.id,
        traceId: fill.traceId,
        payload: {
          positionId: position.id, marketId: fill.marketId, pairedQuantity: position.pairedQuantity,
          unhedgedDurationMs, experimentId,
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
        payload: structuredClone(position),
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

  private async onConnection(event: FeedConnectionEvent): Promise<void> {
    if (event.feed === "polymarket") {
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
      if (event.state === "DISCONNECTED") {
        const kind = /kind=([a-z_]+)/.exec(event.message ?? "")?.[1] ?? "unknown";
        this.metrics.referenceCloses.inc({ kind });
        if (event.reconnectCount > 0) this.metrics.wsReconnects.inc({ feed: "reference" });
        this.markReferenceUnreliable(asset);
        this.state.invalidateReferenceContext(asset);
        this.triggerEngine.resetReferenceAfterReconnect(asset);
      } else if (event.state === "CONNECTED") {
        if (this.referenceUnreliable.has(asset) || this.triggerEngine.referenceWindowOverflowed(asset)) {
          this.triggerEngine.resetReferenceAfterReconnect(asset);
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
    this.logger.error({ err: { name: error.name, message: error.message }, feed }, "market feed error");
    await this.safeAppend(createJournalEvent({ traceId: createId("trace"), type: "SYSTEM_HEALTH_EVENT", aggregateType: "feed", aggregateId: feed, payload: { status: "ERROR", message: error.message } }));
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

  private async safeAppend(event: ReturnType<typeof createJournalEvent>, signal?: AbortSignal): Promise<void> {
    const critical = isCriticalPersistenceType(event.type);
    const journalClass = classifyJournalMetricClass(event.type);
    try {
      await this.journalAdmission.run(journalClass, critical, async () => {
        if (signal?.aborted) throw new JournalAdmissionRejected(journalClass, critical, "JOURNAL_ADMISSION_CLOSED");
        await this.metrics.observeDirectJournal(event.type, async () => {
          await this.journal.append(event);
          if (critical) this.state.database = "UP";
        });
      }, signal);
      if (!critical) this.state.journalDegraded = false;
    } catch (error) {
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
      if (this.experiment.status !== "ENTRY_CLOSED") await this.commitExperimentStatus(closed);
    }
    if (next.status === "COMPLETED" && this.experiment?.status !== "COMPLETED") {
      await this.commitExperimentStatus(next);
    } else if (next.status === "ENTRY_CLOSED" && this.experiment?.status !== "ENTRY_CLOSED" && this.experiment?.status !== "COMPLETED") {
      await this.commitExperimentStatus(next);
    }
    this.syncExperimentWindow();
    const stillOpen = openPositionsRemain(this.ledger.positions.values());
    if (shouldWriteFinalReport(this.experiment, stillOpen) && !this.reportGenerated) {
      await this.writeFinalReport();
    }
  }

  private async maybeScheduledReview(): Promise<void> {
    const status = this.experiment?.status;
    if (status === "IDLE" || status === "COMPLETED" || status === "FAILED" || status == null) return;
    const triggers = this.triggerEngine.ingestScheduledReview(this.experiment?.id ?? createId("review"));
    for (const trigger of triggers) this.admitTrigger(trigger);
  }

  private async pollFailedHedges(): Promise<void> {
    for (const position of this.ledger.positions.values()) {
      if (Number(position.unpairedQuantity) <= 0) continue;
      for (const trigger of this.triggerEngine.ingestPosition(position, position.id, this.assetByMarket.get(position.marketId) ?? null)) {
        this.admitTrigger(trigger);
      }
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

  private async settleMarket(marketId: string, market: DiscoveredMarket): Promise<void> {
    if (this.settledMarkets.has(marketId)) return;
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
      operationKey: `settlement:${marketId}:${this.experiment?.id ?? "paper"}`,
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
        this.settlementBackoff.delete(marketId);
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
        operationKey: `settlement:${marketId}:${experimentId ?? "paper"}`,
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
      checkpoint: true,
    });
  }

  private async writeFinalReport(): Promise<void> {
    if (!this.experiment || this.reportGenerated) return;
    const experiment = this.experiment;
    try {
      const scan = await this.heavyRead.run(() => queryEventsPaged(this.journal, {
        types: [...DECISION_EVENT_TYPES],
        from: experiment.startedAt ? new Date(experiment.startedAt) : undefined,
        to: experiment.completedAt ? new Date(experiment.completedAt) : this.nowFn(),
      }));
      const events = filterCurrentExperimentEvents(scan.events, experiment);
      const unresolved = [...this.ledger.positions.values()].filter((position) => Number(position.upQuantity) > 0 || Number(position.downQuantity) > 0).length;
      const summary = buildExperimentSummary(events, this.experiment.id, {
        status: this.experiment.status,
        closePolicy: this.experiment.closePolicy ?? "observe_until_settlement",
        reportKind: "final",
        unresolvedPositionCount: unresolved,
        metricsScope: "complete",
        complete: scan.complete && !scan.truncated,
        truncated: scan.truncated,
        truncatedReason: scan.truncated ? "SCAN_CEILING" : null,
      });
      await writeExperimentSummary(summary, process.env.REPORT_DIRECTORY ?? "reports");
      await this.journal.append(createJournalEvent({
        traceId: this.experiment.id, type: "EXPERIMENT_REPORT_GENERATED", aggregateType: "experiment", aggregateId: this.experiment.id,
        payload: {
          reportDirectory: process.env.REPORT_DIRECTORY ?? "reports", reportKind: "final",
          settlementPnlUsd: summary.settlementPnlUsd, unresolvedPositionCount: unresolved,
          experimentId: this.experiment.id, complete: summary.complete, truncated: summary.truncated,
        },
      }));
      this.reportGenerated = true;
      this.record("EXPERIMENT_REPORT_GENERATED", this.experiment.id, null, { reportKind: "final", settlementPnlUsd: summary.settlementPnlUsd });
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
    const signalCutoff = nowMs - 60_000;
    while (this.signalReceivedTimes[0] !== undefined && this.signalReceivedTimes[0] < signalCutoff) this.signalReceivedTimes.shift();
    this.executionErrors = this.executionErrors.filter((at) => nowMs - at < 3_600_000);
    const cooldown = this.riskLimits.strategyCooldownMs;
    for (const [key, at] of this.recentTrades) {
      if (nowMs - at > cooldown) this.recentTrades.delete(key);
    }
    for (const [triggerId, trigger] of [...this.activeTriggers]) {
      if (new Date(trigger.expiresAt).getTime() > nowMs) continue;
      this.activeTriggers.delete(triggerId);
      this.signalsByTrigger.delete(triggerId);
      this.lastEvaluatedSignalCount.delete(triggerId);
    }
  }

  runtimeBookkeepingForTest() {
    return {
      signalReceivedTimes: this.signalReceivedTimes.length,
      executionErrors: this.executionErrors.length,
      recentTrades: this.recentTrades.size,
      activeTriggers: this.activeTriggers.size,
      signalsByTrigger: this.signalsByTrigger.size,
      lastEvaluatedSignalCount: this.lastEvaluatedSignalCount.size,
    };
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
      this.signalsByTrigger.set(input.trigger.id, new Map());
      this.lastEvaluatedSignalCount.set(input.trigger.id, 1);
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
    await this.safeAppend(event);
  }
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

  private shouldSuppressStaleMarketTrigger(trigger: TriggerEvent): boolean {
    if (!isMarketDataDerivedTrigger(trigger)) return false;
    if (trigger.marketId && !this.isMarketSynchronized(trigger.marketId)) return true;
    return false;
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
    if (this.referenceResyncRequested.has(asset) || this.referenceResyncInFlight.has(asset)) return;
    this.referenceResyncRequested.add(asset);
    this.referenceResyncInFlight.add(asset);
    this.state.invalidateReferenceContext(asset);
    try {
      this.referenceRegistry.stop(asset);
    } catch { /* scripted registries may no-op */ }
    void this.referenceRegistry.ensureRunning(asset, {
      onTick: (tick) => this.onReferenceTick(tick),
      onConnection: (event) => this.onConnection(event),
      onError: (error) => this.onFeedError(`reference:${asset}`, error),
    }, this.abort.signal).catch((error) => {
      this.logger.error({ err: error, asset }, "reference overflow resync failed");
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
  intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG",
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
