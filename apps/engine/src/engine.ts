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
  DECISION_EVENT_TYPES,
  createIdleExperiment,
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
} from "@pm/core";
import { createJournalEvent, queryEventsPaged, type EventJournal } from "@pm/db";
import {
  BtcWebSocketAdapter,
  PolymarketMarketDataAdapter,
  ReferenceFeedRegistry,
  createRestProbe,
  type DiscoveredMarket,
  type FeedConnectionEvent,
} from "@pm/market-data";
import { EngineMetrics } from "./metrics.js";
import { SseHub } from "./server.js";
import { EngineStateStore } from "./state.js";
import { buildExperimentSummary, filterCurrentExperimentEvents, writeExperimentSummary } from "./report.js";
import { GrokActivationCoordinator, persistableActivation } from "./activation/coordinator.js";
import { createGrokActivationBridge } from "./activation/index.js";
import { BoundedJournalWriter } from "./journal-writer.js";
import { evaluateExperimentPreflight, isGenuineEntryQualifiedMarket } from "./preflight.js";

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
  private readonly referenceRequired = new Map<string, boolean>();
  private readonly referenceBlocked = new Map<string, string>();
  private readonly referenceRetryTimers = new Map<string, NodeJS.Timeout>();
  private readonly referenceRetryDeadlines = new Map<string, number>();
  private tokenIds: string[] = [];
  private marketLoopStarted = false;
  private experiment: ExperimentManifest | null = null;
  private checkpointSequence = 0;
  private lastTradeAt: Date | null = null;
  private executionErrors: number[] = [];
  private timestampDisorder = false;
  private started = false;
  private bootstrapFailed = false;
  private maintenanceTimer: NodeJS.Timeout | null = null;
  private discoveryTimer: NodeJS.Timeout | null = null;
  private lastInfrastructureAccrualAt = Date.now();
  private lastScheduledReviewAt = 0;
  private reportGenerated = false;

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
    },
  ) {
    this.env = env;
    this.sse = new SseHub(env.DASHBOARD_SNAPSHOT_HZ);
    this.nowFn = adapters?.now ?? (() => new Date());
    this.ledger = new PaperLedger(env.PAPER_START_BALANCE, this.nowFn());
    this.state = new EngineStateStore(this.ledger.snapshot(), this.ledger.positions);
    this.triggerConfig = triggerConfigSchema.parse({ scheduledReviewIntervalMs: env.SCHEDULED_REVIEW_INTERVAL_MS });
    this.triggerEngine = new TriggerEngine(this.triggerConfig, this.nowFn);
    this.riskLimits = riskLimitsSchema.parse({ maxExternalSignalsPerMinute: env.GROK_MCP_RATE_LIMIT_PER_MINUTE });
    this.marketAdapter = adapters?.market ?? new PolymarketMarketDataAdapter();
    this.legacyBtcAdapter = adapters?.btc ?? null;
    this.referenceRegistry = adapters?.referenceRegistry ?? new ReferenceFeedRegistry(
      env.REFERENCE_WS_BASE_URL, env.REFERENCE_REST_BASE_URL, this.riskLimits.staleFeedTimeoutMs, undefined,
      createRestProbe(env.REFERENCE_REST_BASE_URL),
    );
    this.resolutionFetcher = adapters?.resolutionFetcher ?? ((id) => this.marketAdapter.fetchPublicMarket(id));
    this.writer = new BoundedJournalWriter(journal, (up) => { this.state.database = up ? "UP" : "DOWN"; }, logger, {
      queueLimit: env.TELEMETRY_QUEUE_LIMIT, batchSize: env.TELEMETRY_BATCH_SIZE, sampleEvery: 20, maxConcurrent: 2,
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
    await this.restoreExperimentState();
    const configuredTokens = splitList(this.env.POLYMARKET_TOKEN_IDS);
    if (configuredTokens.length > 0) {
      configuredTokens.forEach((tokenId, index) => this.marketAdapter.registerManualToken(tokenId, tokenId, index % 2 === 0 ? "UP" : "DOWN"));
      this.tokenIds = configuredTokens;
      this.marketAdapter.setTokenIds(this.tokenIds);
      this.state.setDiscoveryStatus("MONITORING", this.tokenIds.length);
      this.state.setDiscoveryLoopActive(false);
    } else {
      this.state.setDiscoveryLoopActive(true);
      await this.refreshDiscovery();
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

    this.maintenanceTimer = setInterval(() => { void this.maintainExperiment(); }, 60_000);
    this.maintenanceTimer.unref();
    if (configuredTokens.length === 0) {
      this.discoveryTimer = setInterval(() => { void this.refreshDiscovery().catch((error) => this.logger.error({ err: error }, "discovery refresh failed")); }, this.env.DISCOVERY_INTERVAL_MS);
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
    this.record("ENGINE_DRAINING", this.experiment?.id ?? "engine", null, {});
    this.abort.abort("SIGTERM");
    for (const timer of this.referenceRetryTimers.values()) clearTimeout(timer);
    this.referenceRetryTimers.clear();
    if (this.maintenanceTimer) clearInterval(this.maintenanceTimer);
    if (this.discoveryTimer) clearInterval(this.discoveryTimer);
    if (this.experiment?.status === "RUNNING" || this.experiment?.status === "ENTRY_CLOSED") {
      this.accrueInfrastructureCost();
    }
    await this.checkpoint();
    this.sse.close();
    await this.journal.close();
    this.record("ENGINE_STOPPED", this.experiment?.id ?? "engine", null, {});
  }

  noteMcpRequest(tool: string | null, triggerId?: string): void {
    if (triggerId) this.activation.noteMcp([triggerId]);
    else this.activation.noteUnattributedMcp(tool);
    this.state.setActivationLatency(this.activation.snapshot());
    this.state.setUnattributedMcp(this.activation.unattributedSnapshot());
  }

  /** Public so tests and the discovery timer can reconcile live subscriptions. */
  async reconcileDiscovery(): Promise<void> { await this.refreshDiscovery(); }
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
  async tickMaintenance(): Promise<void> { await this.maintainExperiment(); }
  async applyObservedFill(fill: Parameters<PaperLedger["applyFill"]>[0]): Promise<void> { await this.applyFill(fill); }
  async restMakerForTest(order: PaperOrder, snapshot: MarketSnapshot): Promise<void> {
    const notional = makerReserveNotional(order.quantity, order.requestedPrice, snapshot.feeSchedule);
    this.ledger.reserve(notional, this.nowFn());
    this.makerReservations.set(order.id, notional);
    this.restingOrders.set(order.id, createRestingMakerOrder(order, snapshot));
    this.state.setLedger(this.ledger.snapshot());
    await this.safeAppend(createJournalEvent({
      traceId: order.traceId, type: "PAPER_MAKER_RESERVED", aggregateType: "paper_order", aggregateId: order.id,
      payload: { orderId: order.id, reserved: notional, experimentId: this.experiment?.id ?? null },
    }));
  }

  async beginExperiment(): Promise<Record<string, unknown>> {
    if (this.experiment?.status === "RUNNING" || this.experiment?.status === "ENTRY_CLOSED") {
      return { ok: false, blockers: ["EXPERIMENT_ALREADY_ACTIVE"], experiment: this.experiment };
    }
    if (this.experiment?.status === "COMPLETED" || this.experiment?.status === "FAILED") {
      return { ok: false, blockers: ["EXPERIMENT_ALREADY_COMPLETED"], experiment: this.experiment };
    }
    const preflight = this.preflight("48H_GROK_PAPER_EXPERIMENT");
    await this.safeAppend(createJournalEvent({
      traceId: createId("trace"), type: "EXPERIMENT_PREFLIGHT", aggregateType: "experiment", aggregateId: "preflight",
      payload: preflight as unknown as Record<string, unknown>,
    }));
    if (!preflight.ok) return { ...preflight, ok: false };
    const now = this.nowFn();
    this.ledger.resetOperatingCostBaseline(now);
    this.lastInfrastructureAccrualAt = now.getTime();
    this.experiment = startExperiment({
      id: this.experiment?.id, durationHours: this.env.EXPERIMENT_DURATION_HOURS, strategyVersion: this.strategy.version,
      riskConfiguration: this.riskLimits, modelConfiguration: { provider: "external_grok_bot_mcp", logicalRoles: 6, localModelExecution: false },
      startingBalance: this.ledger.snapshot().startingBalance, systemVersion: "0.1.0",
      gitSha: process.env.GIT_SHA ?? process.env.FLY_IMAGE_REF ?? "unknown", now,
      closePolicy: this.env.EXPERIMENT_CLOSE_POLICY,
    });
    this.state.experimentId = this.experiment.id;
    this.state.experimentStatus = this.experiment.status;
    this.state.experimentStartedAt = this.experiment.startedAt;
    this.state.experimentCompletedAt = this.experiment.completedAt;
    this.reportGenerated = false;
    this.state.setLedger(this.ledger.snapshot());
    await this.safeAppend(createJournalEvent({ traceId: createId("trace"), type: "EXPERIMENT_STARTED", aggregateType: "experiment", aggregateId: this.experiment.id, payload: this.experiment }));
    await this.checkpoint();
    this.record("EXPERIMENT_STARTED", this.experiment.id, null, { status: this.experiment.status });
    return { ok: true, experiment: this.experiment, preflight };
  }

  preflight(mode: "MONITOR_ONLY" | "48H_GROK_PAPER_EXPERIMENT" = "MONITOR_ONLY") {
    return evaluateExperimentPreflight({
      env: this.env, databaseUp: this.state.database === "UP", discoveryStatus: this.state.discoveryStatus,
      qualifiedOrManaged: this.marketsById.size, bootstrapFailed: this.bootstrapFailed,
      tokenConfiguredPath: this.tokenIds.length > 0,
      mcpRoleTokens: this.env.GROK_MCP_BEARER_TOKENS.split(",").map((entry) => entry.trim().split(":")[0] ?? "").filter(Boolean),
      mode,
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
    }, this.abort.signal).catch((error) => this.logger.error({ err: error }, "polymarket market loop stopped"));
  }

  private async refreshDiscovery(): Promise<void> {
    this.state.setDiscoveryStatus("RECONCILING", this.marketsById.size);
    const report = await this.marketAdapter.discoverCryptoMarkets(
      splitList(this.env.POLYMARKET_MARKET_SLUGS),
      this.env.MAX_QUALIFIED_MARKETS,
      qualificationConfigSchema.parse({ maxQualifiedMarkets: this.env.MAX_QUALIFIED_MARKETS }),
    );
    const previousMarketIds = new Set(this.marketsById.keys());
    const previousAssets = new Set([...this.assetByMarket.values()].filter((asset): asset is string => Boolean(asset)));
    const previousTokens = [...this.tokenIds];
    const obligations = this.obligationMarketIds();
    this.entryQualified.clear();
    for (const market of report.qualified) this.entryQualified.add(market.id);

    const nextMarkets = new Map<string, DiscoveredMarket>();
    for (const market of report.qualified) nextMarkets.set(market.id, market);
    for (const id of obligations) {
      const fresh = report.discovered.find((item) => item.id === id) ?? report.qualified.find((item) => item.id === id);
      const previous = this.marketsById.get(id);
      if (fresh) nextMarkets.set(id, fresh);
      else if (previous) nextMarkets.set(id, previous);
    }

    this.marketsById.clear();
    this.assetByMarket.clear();
    this.managedMarketIds.clear();
    for (const [id, market] of nextMarkets) {
      this.marketsById.set(id, market);
      this.assetByMarket.set(id, market.asset);
      if (obligations.has(id) || this.entryQualified.has(id)) this.managedMarketIds.add(id);
      if (market.endDate) this.triggerEngine.noteMarketWindow(id, market.asset, market.endDate);
    }
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
  }

  private obligationMarketIds(): Set<string> {
    const ids = new Set<string>();
    for (const position of this.ledger.positions.values()) {
      if (Number(position.upQuantity) > 0 || Number(position.downQuantity) > 0) ids.add(position.marketId);
    }
    for (const resting of this.restingOrders.values()) {
      if (Number(resting.remaining) > 0) ids.add(resting.order.marketId);
    }
    return ids;
  }

  private dropRemovedMarketState(removedMarkets: readonly string[], removedAssets: readonly string[]): void {
    for (const marketId of removedMarkets) {
      this.snapshots.delete(marketId);
      this.triggerEngine.dropMarket(marketId);
      for (const [tokenId, snapshot] of [...this.state.marketSnapshots.entries()]) {
        if (snapshot.marketId === marketId) this.state.marketSnapshots.delete(tokenId);
      }
      for (const [orderId, resting] of [...this.restingOrders.entries()]) {
        if (resting.order.marketId !== marketId) continue;
        this.releaseMaker(orderId);
        this.restingOrders.delete(orderId);
        this.record("PAPER_ORDER_CANCELLED", resting.order.traceId, marketId, { orderId, reason: "MARKET_REMOVED" });
      }
    }
    for (const asset of removedAssets) {
      this.triggerEngine.dropAsset(asset);
      this.state.dropReference(asset);
      this.clearReferenceRetry(asset);
    }
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

  private async restoreExperimentState(): Promise<void> {
    const checkpoint = await this.journal.loadLatestCheckpoint();
    if (checkpoint) {
      const payload = checkpoint.payload as Record<string, unknown>;
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
      this.checkpointSequence = checkpoint.sequence;
      this.record("STATE_RECOVERED", this.experiment.id, null, { checkpointId: checkpoint.id, positions: positions.length, restingOrders: resting.length, activeTriggers: triggers.length, grokSignals: signals.length });
    } else {
      this.experiment = createIdleExperiment({
        durationHours: this.env.EXPERIMENT_DURATION_HOURS, strategyVersion: this.strategy.version,
        riskConfiguration: this.riskLimits, modelConfiguration: { provider: "external_grok_bot_mcp", logicalRoles: 6, localModelExecution: false },
        startingBalance: this.ledger.snapshot().startingBalance, systemVersion: "0.1.0",
        gitSha: process.env.GIT_SHA ?? process.env.FLY_IMAGE_REF ?? "unknown",
      });
    }
    this.syncExperimentWindow();
  }

  private async onMarketSnapshot(snapshot: MarketSnapshot): Promise<void> {
    if (!this.tokenIds.includes(snapshot.tokenId)) return;
    const started = performance.now();
    this.metrics.marketMessages.inc();
    this.state.connections.polymarket.lastMessageAt = snapshot.receivedAt;
    this.state.marketSnapshots.set(snapshot.tokenId, snapshot);
    const pair = this.snapshots.get(snapshot.marketId) ?? {};
    const previous = pair[snapshot.outcome];
    pair[snapshot.outcome] = snapshot;
    this.snapshots.set(snapshot.marketId, pair);
    if (snapshot.asset) this.assetByMarket.set(snapshot.marketId, snapshot.asset);
    this.updateReady();
    await this.writer.persist(createJournalEvent({ traceId: snapshot.traceId, type: "MARKET_SNAPSHOT", aggregateType: "market", aggregateId: snapshot.marketId, payload: snapshot, occurredAt: new Date(snapshot.sourceTimestamp), sourceEventId: snapshot.eventId }));
    this.record("MARKET_EVENT", snapshot.traceId, snapshot.marketId, { tokenId: snapshot.tokenId, outcome: snapshot.outcome, sequence: snapshot.sequence }, snapshot.asset ?? null);

    for (const [orderId, resting] of this.restingOrders) {
      if (resting.order.tokenId !== snapshot.tokenId || !previous) continue;
      const evolved = evolveMakerOrder(resting, previous, snapshot);
      this.restingOrders.set(orderId, evolved.resting);
      if (evolved.fill) await this.applyFill(evolved.fill);
      if (Number(evolved.resting.remaining) === 0) this.restingOrders.delete(orderId);
    }

    const triggers = this.triggerEngine.ingestMarket(snapshot);
    for (const trigger of triggers) void this.processTrigger(trigger);
    const window = this.marketsById.get(snapshot.marketId);
    if (window?.endDate) this.triggerEngine.noteMarketWindow(snapshot.marketId, snapshot.asset ?? window.asset, window.endDate);
    this.state.eventLatencyMs = performance.now() - started;
    this.metrics.eventLatency.observe((performance.now() - started) / 1000);
  }

  private async onReferenceTick(tick: ReferenceTick): Promise<void> {
    if (this.usesDiscovery() && ![...this.assetByMarket.values()].includes(tick.asset)) return;
    this.state.noteReferenceTick(tick.asset, tick.receivedAt, {
      symbol: tick.symbol, venue: tick.venue, price: tick.price,
    });
    if (tick.asset === "BTC") {
      this.state.lastBtcMessageAt = tick.receivedAt;
      this.state.connections.btc.lastMessageAt = tick.receivedAt;
    }
    this.updateReady();
    await this.writer.persist(createJournalEvent({ traceId: tick.traceId, type: "REFERENCE_TICK", aggregateType: "reference", aggregateId: tick.asset, payload: tick, occurredAt: new Date(tick.sourceTimestamp), sourceEventId: tick.eventId }));
    for (const trigger of this.triggerEngine.ingestReference(tick)) void this.processTrigger(trigger);
  }

  private async processTrigger(trigger: TriggerEvent): Promise<void> {
    if (this.state.draining) return;
    this.state.queueDepth += 1;
    this.metrics.queueDepth.set(this.state.queueDepth);
    this.metrics.triggers.inc({ kind: trigger.kind, severity: String(trigger.severity) });
    this.record(`TRIGGER_LEVEL_${trigger.severity}`, trigger.traceId, trigger.marketId, { triggerId: trigger.id, kind: trigger.kind, asset: trigger.asset ?? null }, trigger.asset ?? null);
    await this.safeAppend(createJournalEvent({ traceId: trigger.traceId, type: "TRIGGER_EVENT", aggregateType: "trigger", aggregateId: trigger.id, payload: trigger, sourceEventId: trigger.sourceEventIds[0] ?? null }));
    try {
      this.activeTriggers.set(trigger.id, trigger);
      this.state.addTrigger(trigger);
      this.record("GROK_RESEARCH_AVAILABLE", trigger.traceId, trigger.marketId, { triggerId: trigger.id, expiresAt: trigger.expiresAt }, trigger.asset ?? null);
      await this.requestActivation(trigger);
    } finally {
      this.state.queueDepth -= 1;
      this.metrics.queueDepth.set(this.state.queueDepth);
    }
  }

  private async requestActivation(trigger: TriggerEvent): Promise<void> {
    const status = this.experiment?.status ?? "IDLE";
    if (status === "IDLE" || status === "COMPLETED" || status === "FAILED") {
      this.activation.ensureLatency(trigger);
      this.state.setActivationLatency(this.activation.snapshot());
      await this.safeAppend(createJournalEvent({
        traceId: trigger.traceId, type: "GROK_ACTIVATION_DECISION", aggregateType: "activation", aggregateId: trigger.id,
        payload: { triggerId: trigger.id, marketId: trigger.marketId, asset: trigger.asset ?? null, reason: "EXPERIMENT_NOT_ACTIVE", activate: false },
      }));
      this.state.addActivation({ triggerId: trigger.id, reason: "EXPERIMENT_NOT_ACTIVE", at: this.nowFn().toISOString(), provider: this.env.GROK_ACTIVATION_PROVIDER, grokWakeObserved: false });
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
  async registerTriggerForResearch(trigger: TriggerEvent): Promise<void> { await this.processTrigger(triggerEventSchema.parse(trigger)); }

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
    for (const disagreement of this.triggerEngine.ingestConsensus(consensus, trigger.marketId, trigger.asset ?? null)) void this.processTrigger(disagreement);
    if (trigger.marketId) await this.evaluateTrade(trigger, consensus, outputs);
  }

  private async evaluateTrade(trigger: TriggerEvent, consensus: NonNullable<EngineStateStore["latestConsensus"]>, outputs: GrokSignalEnvelope[]): Promise<void> {
    const pair = this.snapshots.get(trigger.marketId ?? "");
    if (!pair) return;
    const position = this.ledger.positions.get(trigger.marketId ?? "") ?? null;
    this.experiment = this.experiment ? refreshExperiment(this.experiment, this.nowFn(), { openPositions: openPositionsRemain(this.ledger.positions.values()) }) : null;
    if (this.experiment) this.state.experimentStatus = this.experiment.status;
    const candidate = this.strategy.evaluate({
      consensus, snapshots: { UP: pair.UP ?? null, DOWN: pair.DOWN ?? null }, position,
      maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd, minimumLockedPairEdge: this.riskLimits.minExpectedEdge,
    });
    if (!candidate) return;
    if (candidate.intent === "OPEN_FIRST_LEG" && !experimentAllowsFirstLeg(this.experiment)) return;
    if (candidate.intent === "OPEN_FIRST_LEG" && !this.entryQualified.has(candidate.marketId)) return;
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
    await this.safeAppend(createJournalEvent({ traceId: order.traceId, type: "PAPER_ORDER_CREATED", aggregateType: "paper_order", aggregateId: order.id, payload: { ...order, intent } }));
    this.recentTrades.set(duplicateKey, Date.now());
    try {
      if (order.orderType === "MAKER") {
        const notional = makerReserveNotional(order.quantity, order.requestedPrice, decisionSnapshot.feeSchedule);
        this.ledger.reserve(notional, this.nowFn());
        this.makerReservations.set(order.id, notional);
        this.state.setLedger(this.ledger.snapshot());
        this.restingOrders.set(order.id, createRestingMakerOrder(order, decisionSnapshot));
        await this.safeAppend(createJournalEvent({ traceId: order.traceId, type: "PAPER_MAKER_RESERVED", aggregateType: "paper_order", aggregateId: order.id, payload: { orderId: order.id, reserved: notional } }));
        return;
      }
      await delay(this.env.PAPER_EXECUTION_LATENCY_MS);
      const current = this.state.marketSnapshots.get(order.tokenId);
      if (!current || new Date(current.receivedAt).getTime() < new Date(decisionSnapshot.receivedAt).getTime()) throw new Error("STALE_EXECUTION_SNAPSHOT");
      await this.applyFill(executePaperTaker({ order, snapshotAfterLatency: current, measuredLatencyMs: this.env.PAPER_EXECUTION_LATENCY_MS }));
      this.lastTradeAt = this.nowFn();
    } catch (error) {
      this.executionErrors.push(Date.now());
      this.record("PAPER_EXECUTION_ERROR", order.traceId, order.marketId, { orderId: order.id, message: error instanceof Error ? error.message : String(error) });
      await this.safeAppend(createJournalEvent({ traceId: order.traceId, type: "PAPER_EXECUTION_ERROR", aggregateType: "paper_order", aggregateId: order.id, payload: { message: error instanceof Error ? error.message : String(error) } }));
    }
  }

  private releaseMaker(orderId: string, filledNotional = "0"): void {
    const reserved = this.makerReservations.get(orderId);
    if (!reserved) return;
    const remaining = Math.max(0, Number(reserved) - Number(filledNotional));
    if (remaining > 0) this.ledger.release(remaining, this.nowFn());
    this.makerReservations.delete(orderId);
    this.state.setLedger(this.ledger.snapshot());
    void this.safeAppend(createJournalEvent({
      traceId: orderId, type: "PAPER_MAKER_RELEASED", aggregateType: "paper_order", aggregateId: orderId,
      payload: { orderId, released: String(remaining), experimentId: this.experiment?.id ?? null },
    }));
  }

  private async applyFill(fill: Parameters<PaperLedger["applyFill"]>[0]): Promise<void> {
    const reserved = this.makerReservations.get(fill.orderId);
    if (reserved && fill.liquidityRole === "MAKER") {
      const fillCost = Number(fill.quantity) * Number(fill.fillPrice) + Number(fill.feeUsd);
      const next = Math.max(0, Number(reserved) - fillCost);
      this.ledger.release(Math.min(Number(reserved), fillCost), this.nowFn());
      if (next > 0) this.makerReservations.set(fill.orderId, String(next));
      else this.makerReservations.delete(fill.orderId);
    }
    const previous = this.ledger.positions.get(fill.marketId);
    const wasEmpty = !previous || (Number(previous.upQuantity) === 0 && Number(previous.downQuantity) === 0);
    const position = this.ledger.applyFill(fill);
    this.state.setLedger(this.ledger.snapshot());
    this.metrics.paperFills.inc({ role: fill.liquidityRole });
    this.metrics.openPositions.set(this.ledger.positions.size);
    this.metrics.unhedgedPositions.set([...this.ledger.positions.values()].filter((item) => Number(item.unpairedQuantity) > 0).length);
    this.record("PAPER_FILL", fill.traceId, fill.marketId, { fillId: fill.id, orderId: fill.orderId, price: fill.fillPrice, quantity: fill.quantity, role: fill.liquidityRole });
    if (wasEmpty && fill.side === "BUY") {
      this.record("FIRST_LEG_FILLED", fill.traceId, fill.marketId, { positionId: position.id, unpairedQuantity: position.unpairedQuantity });
      await this.safeAppend(createJournalEvent({
        traceId: fill.traceId, type: "FIRST_LEG_FILLED", aggregateType: "position", aggregateId: position.id,
        payload: { positionId: position.id, marketId: fill.marketId, unpairedQuantity: position.unpairedQuantity, experimentId: this.experiment?.id ?? null },
      }));
    }
    if (Number(position.pairedQuantity) > 0 && Number(position.unpairedQuantity) === 0) {
      const unhedgedDurationMs = Math.max(0, this.nowFn().getTime() - new Date(position.openedAt).getTime());
      this.record("HEDGE_COMPLETE", fill.traceId, fill.marketId, { positionId: position.id, pairedQuantity: position.pairedQuantity, unhedgedDurationMs });
      await this.safeAppend(createJournalEvent({
        traceId: fill.traceId, type: "HEDGE_COMPLETE", aggregateType: "position", aggregateId: position.id,
        payload: {
          positionId: position.id, marketId: fill.marketId, pairedQuantity: position.pairedQuantity,
          unhedgedDurationMs, experimentId: this.experiment?.id ?? null,
        },
      }));
    }
    await this.safeAppend(createJournalEvent({
      traceId: fill.traceId, type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: fill.id,
      payload: { ...fill, asset: this.assetByMarket.get(fill.marketId) ?? null }, sourceEventId: fill.sourceMarketEventId,
    }));
    await this.safeAppend(createJournalEvent({ traceId: fill.traceId, type: "POSITION_UPDATED", aggregateType: "position", aggregateId: position.id, payload: position, sourceEventId: fill.id }));
    await this.safeAppend(createJournalEvent({ traceId: fill.traceId, type: "PNL_SNAPSHOT", aggregateType: "ledger", aggregateId: this.experiment?.id ?? "paper", payload: { ...this.ledger.snapshot(), experimentId: this.experiment?.id ?? null } }));
    for (const trigger of this.triggerEngine.ingestPosition(position, fill.traceId, this.assetByMarket.get(fill.marketId) ?? null)) {
      void this.processTrigger(trigger);
    }
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
      this.metrics.wsReconnects.inc({ feed: "reference" }, 0);
    }
    this.updateReady();
    this.record(`WS_${event.state}`, createId("trace"), null, { feed: event.feed, asset: event.asset ?? null, reconnects: event.reconnectCount, message: event.message }, event.asset ?? null);
  }

  private async onFeedError(feed: string, error: Error): Promise<void> {
    if (error.message.includes("EVENT_TIMESTAMP_DISORDER")) this.timestampDisorder = true;
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
    this.sse.publish("snapshot", this.state.snapshot());
  }

  private async safeAppend(event: ReturnType<typeof createJournalEvent>): Promise<void> {
    try { await this.journal.append(event); this.state.database = "UP"; }
    catch (error) { this.state.database = "DOWN"; this.updateReady(); this.logger.error({ err: error, traceId: event.traceId }, "journal append failed"); }
  }

  private async checkpoint(): Promise<void> {
    if (!this.experiment || this.state.database !== "UP") return;
    await this.journal.saveCheckpoint({
      experimentId: this.experiment.id, sequence: ++this.checkpointSequence,
      payload: {
        ledger: this.ledger.snapshot(), positions: [...this.ledger.positions.values()], experiment: this.experiment,
        restingOrders: [...this.restingOrders.values()], activeTriggers: [...this.activeTriggers.values()],
        grokSignals: [...this.signalsByTrigger.values()].flatMap((signals) => [...signals.values()]),
        activationLatency: this.activation.snapshot(),
        makerReservations: Object.fromEntries(this.makerReservations),
        managedMarkets: [...this.obligationMarketIds()],
        settledMarkets: [...this.settledMarkets],
        marketMetadata: [...this.marketsById.values()].map((market) => ({ ...market, raw: {} })),
      },
    });
  }

  private accrueInfrastructureCost(now = Date.now()): void {
    const elapsedHours = Math.max(0, now - this.lastInfrastructureAccrualAt) / 3_600_000;
    if (elapsedHours > 0) this.ledger.addOperatingCosts(0, elapsedHours * this.env.ESTIMATED_INFRA_COST_PER_HOUR_USD, new Date(now));
    this.lastInfrastructureAccrualAt = now;
    this.state.setLedger(this.ledger.snapshot());
  }

  private async maintainExperiment(): Promise<void> {
    if (!this.experiment) return;
    if (this.experiment.status === "RUNNING" || this.experiment.status === "ENTRY_CLOSED") {
      this.accrueInfrastructureCost(this.nowFn().getTime());
    }
    this.pruneRuntimeBookkeeping();
    await this.pollFailedHedges();
    await this.maybeScheduledReview();
    await this.pollSettlements();
    const previous = this.experiment.status;
    const open = openPositionsRemain(this.ledger.positions.values());
    const unresolved = [...this.ledger.positions.values()].filter((position) => Number(position.upQuantity) > 0 || Number(position.downQuantity) > 0).length;
    this.experiment = refreshExperiment(this.experiment, this.nowFn(), { openPositions: open, unresolvedCount: unresolved });
    if (this.experiment.status === "ENTRY_CLOSED" && (this.experiment.closePolicy ?? "observe_until_settlement") === "mark_to_market") {
      const marked = await this.markToMarketClose(unresolved);
      this.experiment = refreshExperiment(this.experiment, this.nowFn(), {
        openPositions: open, unresolvedCount: unresolved, markToMarketReady: marked,
      });
    }
    if (this.experiment.status === "ENTRY_CLOSED") {
      const policy = this.experiment.closePolicy ?? "observe_until_settlement";
      this.experiment = refreshExperiment(this.experiment, this.nowFn(), {
        openPositions: openPositionsRemain(this.ledger.positions.values()),
        unresolvedCount: [...this.ledger.positions.values()].filter((position) => Number(position.upQuantity) > 0 || Number(position.downQuantity) > 0).length,
        markToMarketReady: policy === "mark_to_market" ? false : undefined,
      });
    }
    this.state.experimentStatus = this.experiment.status;
    this.syncExperimentWindow();
    if (previous === "RUNNING" && this.experiment.status === "ENTRY_CLOSED") {
      await this.safeAppend(createJournalEvent({
        traceId: this.experiment.id, type: "EXPERIMENT_ENTRY_CLOSED", aggregateType: "experiment", aggregateId: this.experiment.id,
        payload: { ...this.experiment, experimentId: this.experiment.id },
      }));
      await this.checkpoint();
    }
    if (previous !== "COMPLETED" && this.experiment.status === "COMPLETED") {
      await this.safeAppend(createJournalEvent({
        traceId: this.experiment.id, type: "EXPERIMENT_COMPLETED", aggregateType: "experiment", aggregateId: this.experiment.id,
        payload: { ...this.experiment, experimentId: this.experiment.id },
      }));
      await this.checkpoint();
    }
    const stillOpen = openPositionsRemain(this.ledger.positions.values());
    if (shouldWriteFinalReport(this.experiment, stillOpen) && !this.reportGenerated) {
      await this.writeFinalReport();
    }
  }

  private async maybeScheduledReview(): Promise<void> {
    const status = this.experiment?.status;
    if (status === "IDLE" || status === "COMPLETED" || status === "FAILED" || status == null) return;
    const triggers = this.triggerEngine.ingestScheduledReview(this.experiment?.id ?? createId("review"));
    for (const trigger of triggers) void this.processTrigger(trigger);
  }

  private async pollFailedHedges(): Promise<void> {
    for (const position of this.ledger.positions.values()) {
      if (Number(position.unpairedQuantity) <= 0) continue;
      for (const trigger of this.triggerEngine.ingestPosition(position, position.id, this.assetByMarket.get(position.marketId) ?? null)) {
        void this.processTrigger(trigger);
      }
    }
  }

  private async pollSettlements(): Promise<void> {
    const now = this.nowFn();
    for (const [orderId, resting] of [...this.restingOrders.entries()]) {
      const market = this.marketsById.get(resting.order.marketId);
      if (!market?.endDate || new Date(market.endDate).getTime() > now.getTime()) continue;
      this.releaseMaker(orderId);
      this.restingOrders.delete(orderId);
      this.record("PAPER_ORDER_CANCELLED", resting.order.traceId, resting.order.marketId, { orderId, reason: "MARKET_EXPIRED" });
      await this.safeAppend(createJournalEvent({
        traceId: resting.order.traceId, type: "PAPER_ORDER_CANCELLED", aggregateType: "paper_order", aggregateId: orderId,
        payload: { orderId, reason: "MARKET_EXPIRED", experimentId: this.experiment?.id ?? null },
      }));
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
    await this.safeAppend(createJournalEvent({
      traceId: this.experiment?.id ?? createId("trace"), type: "MARKET_RESOLUTION_OBSERVED", aggregateType: "market", aggregateId: marketId,
      payload: { ...parsed, marketId, slug: market.slug, experimentId: this.experiment?.id ?? null },
    }));
    const settled = this.ledger.settlePosition(marketId, parsed.upPayoff, parsed.downPayoff, this.nowFn());
    this.settledMarkets.add(marketId);
    this.settlementBackoff.delete(marketId);
    this.state.setLedger(this.ledger.snapshot());
    if (!settled) return;
    const asset = this.assetByMarket.get(marketId) ?? market.asset;
    await this.safeAppend(createJournalEvent({
      traceId: settled.position.id, type: "POSITION_SETTLED", aggregateType: "position", aggregateId: settled.position.id,
      payload: {
        marketId, asset, cashDelta: settled.cashDelta, realizedDelta: settled.realizedDelta,
        upPayoff: parsed.upPayoff, downPayoff: parsed.downPayoff, winner: parsed.winner,
        upQuantity: settled.position.upQuantity, downQuantity: settled.position.downQuantity,
        feesUsd: settled.position.totalFees, grossOutcome: settled.grossOutcome, outcome: settled.outcome,
        experimentId: this.experiment?.id ?? null,
      },
    }));
    await this.safeAppend(createJournalEvent({
      traceId: settled.position.id, type: settled.outcome, aggregateType: "position", aggregateId: settled.position.id,
      payload: { marketId, realizedDelta: settled.realizedDelta, feesUsd: settled.position.totalFees, grossOutcome: settled.grossOutcome, experimentId: this.experiment?.id ?? null },
    }));
    if (Number(settled.position.pairedQuantity) > 0) {
      const metrics = calculatePairMetrics(settled.position);
      const paired = Number(settled.position.pairedQuantity);
      const grossLockedEdge = 1 - Number(settled.position.combinedPairCost);
      const netRealizedEdge = paired > 0 ? Number(metrics.netPairEdge) / paired : Number(metrics.netPairEdge);
      await this.safeAppend(createJournalEvent({
        traceId: settled.position.id, type: "PAIR_REALIZED", aggregateType: "position", aggregateId: settled.position.id,
        payload: {
          marketId, pairedQuantity: settled.position.pairedQuantity, combinedPairCost: settled.position.combinedPairCost,
          grossLockedEdge, netRealizedEdge, realizedEdge: netRealizedEdge,
          fees: metrics.fees, experimentId: this.experiment?.id ?? null,
        },
      }));
    }
    await this.safeAppend(createJournalEvent({
      traceId: this.experiment?.id ?? "paper", type: "PNL_SNAPSHOT", aggregateType: "ledger",
      aggregateId: this.experiment?.id ?? "paper",
      payload: { ...this.ledger.snapshot(), experimentId: this.experiment?.id ?? null, source: "SETTLEMENT" },
    }));
    this.record("POSITION_SETTLED", settled.position.id, marketId, { outcome: settled.outcome, realizedDelta: settled.realizedDelta }, asset);
    this.metrics.openPositions.set(this.ledger.positions.size);
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
    this.ledger.revalue(prices, this.nowFn().toISOString());
    this.state.setLedger(this.ledger.snapshot());
    await this.safeAppend(createJournalEvent({
      traceId: this.experiment?.id ?? "paper", type: "PNL_SNAPSHOT", aggregateType: "ledger",
      aggregateId: this.experiment?.id ?? "paper",
      payload: {
        ...this.ledger.snapshot(), experimentId: this.experiment?.id ?? null,
        source: "MARK_TO_MARKET", unresolvedPositionCount: unresolvedCount, markToMarket: true,
      },
    }));
    return true;
  }

  private async writeFinalReport(): Promise<void> {
    if (!this.experiment || this.reportGenerated) return;
    try {
      const scan = await queryEventsPaged(this.journal, {
        types: [...DECISION_EVENT_TYPES],
        from: this.experiment.startedAt ? new Date(this.experiment.startedAt) : undefined,
        to: this.experiment.completedAt ? new Date(this.experiment.completedAt) : this.nowFn(),
      });
      const events = filterCurrentExperimentEvents(scan.events, this.experiment);
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
}

export function parseEnv(env: NodeJS.ProcessEnv): EngineEnv { return parseEngineEnv(env); }
function splitList(value: string): string[] { return value.split(",").map((item) => item.trim()).filter(Boolean); }
function uniqueStrings(values: readonly string[]): string[] { return [...new Set(values.filter(Boolean))]; }
function dateOrNull(value: string | null | undefined): Date | null { return value ? new Date(value) : null; }
function delay(ms: number): Promise<void> { return new Promise((resolve) => setTimeout(resolve, ms)); }

const RISK_HEDGE_SETTLEMENT_KINDS = new Set([
  "FAILED_HEDGE", "DIRECTIONAL_EXPOSURE", "NEARING_EXPIRY", "SCHEDULED_REVIEW",
]);

export function isRiskHedgeSettlementTrigger(kind: string): boolean {
  return RISK_HEDGE_SETTLEMENT_KINDS.has(kind);
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
