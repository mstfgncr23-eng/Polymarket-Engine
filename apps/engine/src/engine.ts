import {
  PaperLedger,
  TemporalPairHedgeStrategy,
  TriggerEngine,
  buildConsensus,
  createId,
  decidePaperOrder,
  defaultConsensusConfig,
  engineEnvSchema,
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
  computePaperExposures,
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
} from "@pm/core";
import { createJournalEvent, type EventJournal } from "@pm/db";
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
import { buildExperimentSummary, writeExperimentSummary } from "./report.js";
import { GrokActivationCoordinator, persistableActivation } from "./activation/coordinator.js";
import { createGrokActivationBridge } from "./activation/index.js";

export class EngineService {
  readonly metrics = new EngineMetrics();
  readonly sse = new SseHub();
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
  private readonly snapshots = new Map<string, Partial<Record<Outcome, MarketSnapshot>>>();
  private readonly restingOrders = new Map<string, RestingPaperOrder>();
  private readonly recentTrades = new Map<string, number>();
  private readonly activeTriggers = new Map<string, TriggerEvent>();
  private readonly signalsByTrigger = new Map<string, Map<ExternalAgentRoleId, GrokSignalEnvelope>>();
  private readonly signalReceivedTimes: number[] = [];
  private readonly lastEvaluatedSignalCount = new Map<string, number>();
  private readonly marketsById = new Map<string, DiscoveredMarket>();
  private readonly assetByMarket = new Map<string, string | null>();
  private readonly referenceRequired = new Map<string, boolean>();
  private readonly referenceRetryTimers = new Map<string, NodeJS.Timeout>();
  private tokenIds: string[] = [];
  private marketLoopStarted = false;
  private experiment: ExperimentManifest | null = null;
  private checkpointSequence = 0;
  private lastTradeAt: Date | null = null;
  private executionErrors: number[] = [];
  private timestampDisorder = false;
  private started = false;
  private maintenanceTimer: NodeJS.Timeout | null = null;
  private discoveryTimer: NodeJS.Timeout | null = null;
  private lastInfrastructureAccrualAt = Date.now();
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
    },
  ) {
    this.ledger = new PaperLedger(env.PAPER_START_BALANCE);
    this.state = new EngineStateStore(this.ledger.snapshot(), this.ledger.positions);
    this.triggerConfig = triggerConfigSchema.parse({});
    this.triggerEngine = new TriggerEngine(this.triggerConfig);
    this.riskLimits = riskLimitsSchema.parse({ maxExternalSignalsPerMinute: env.GROK_MCP_RATE_LIMIT_PER_MINUTE });
    this.marketAdapter = adapters?.market ?? new PolymarketMarketDataAdapter();
    this.legacyBtcAdapter = adapters?.btc ?? null;
    this.referenceRegistry = adapters?.referenceRegistry ?? new ReferenceFeedRegistry(
      env.REFERENCE_WS_BASE_URL, env.REFERENCE_REST_BASE_URL, this.riskLimits.staleFeedTimeoutMs, undefined,
      createRestProbe(env.REFERENCE_REST_BASE_URL),
    );
    this.activation = new GrokActivationCoordinator(
      adapters?.activation ?? createGrokActivationBridge(env),
      { cooldownMs: this.triggerConfig.cooldownMs, maxPerMinute: this.triggerConfig.maxActivationsPerMinute, suppressAtOrBelow: this.triggerConfig.activationSeveritySuppression },
    );
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    await this.initializeJournalWithRetry();
    await this.restoreOrStartExperiment();
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
    this.accrueInfrastructureCost();
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

    this.marketsById.clear();
    this.assetByMarket.clear();
    this.referenceRequired.clear();
    for (const market of report.qualified) {
      this.marketsById.set(market.id, market);
      this.assetByMarket.set(market.id, market.asset);
    }
    const nextTokens = uniqueStrings(report.qualified.flatMap((market) => market.tokens.map((token) => token.tokenId)));
    const addedMarkets = report.qualified.filter((market) => !previousMarketIds.has(market.id)).map((market) => market.id);
    const removedMarkets = [...previousMarketIds].filter((id) => !this.marketsById.has(id));
    const unchangedMarkets = report.qualified.filter((market) => previousMarketIds.has(market.id)).map((market) => market.id);
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
    this.state.setDiscoveryStatus(nextTokens.length > 0 ? "MONITORING" : "ZERO_QUALIFIED", this.marketsById.size);
    this.updateReady();

    await this.safeAppend(createJournalEvent({
      traceId: createId("trace"), type: "DISCOVERY_RECONCILED", aggregateType: "engine", aggregateId: "market-discovery",
      payload: {
        discovered: report.discovered.length, qualified: report.qualified.length,
        addedMarkets, removedMarkets, unchangedMarkets, addedAssets, removedAssets,
        previousTokens, nextTokens,
        markets: report.qualified.map((market) => ({ id: market.id, slug: market.slug, asset: market.asset, tokens: market.tokens })),
        rejected: report.rejected.slice(0, 50),
      },
    }));
    this.record("DISCOVERY_RECONCILED", createId("trace"), null, {
      qualified: report.qualified.length, addedMarkets, removedMarkets, addedAssets, removedAssets,
    });
    if (!this.legacyBtcAdapter) await this.reconcileReferenceFeeds();
  }

  private dropRemovedMarketState(removedMarkets: readonly string[], removedAssets: readonly string[]): void {
    for (const marketId of removedMarkets) {
      this.snapshots.delete(marketId);
      this.triggerEngine.dropMarket(marketId);
      for (const [tokenId, snapshot] of [...this.state.marketSnapshots.entries()]) {
        if (snapshot.marketId === marketId) this.state.marketSnapshots.delete(tokenId);
      }
      for (const [orderId, resting] of [...this.restingOrders.entries()]) {
        if (resting.order.marketId === marketId) this.restingOrders.delete(orderId);
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
      await this.safeAppend(createJournalEvent({
        traceId: createId("trace"), type: "REFERENCE_FEED_STOPPED", aggregateType: "reference", aggregateId: asset,
        payload: { asset, reason: "NO_ACTIVE_MARKET" },
      }));
      this.record("REFERENCE_FEED_STOPPED", createId("trace"), null, { asset }, asset);
    }
    for (const asset of needed) {
      const markets = [...this.assetByMarket.entries()].filter(([, value]) => value === asset).map(([marketId]) => marketId);
      const resolution = await this.referenceRegistry.resolve(asset);
      if (resolution.status === "UNSUPPORTED_DEFINITIVE") {
        for (const marketId of markets) this.referenceRequired.set(marketId, false);
        await this.safeAppend(createJournalEvent({
          traceId: createId("trace"), type: "REFERENCE_UNSUPPORTED", aggregateType: "reference", aggregateId: asset,
          payload: { asset, reason: resolution.reason, status: resolution.status, detail: resolution.detail ?? null, choice: "POLYMARKET_ONLY_MONITOR" },
        }));
        this.record("REFERENCE_UNSUPPORTED", createId("trace"), null, { asset, choice: "POLYMARKET_ONLY_MONITOR" }, asset);
        continue;
      }
      if (resolution.status === "TRANSIENT_FAILURE") {
        for (const marketId of markets) this.referenceRequired.set(marketId, true);
        await this.safeAppend(createJournalEvent({
          traceId: createId("trace"), type: "REFERENCE_PROBE_TRANSIENT", aggregateType: "reference", aggregateId: asset,
          payload: { asset, reason: resolution.reason, status: resolution.status, detail: resolution.detail ?? null, retryAfterMs: resolution.retryAfterMs ?? null },
        }));
        this.record("REFERENCE_PROBE_TRANSIENT", createId("trace"), null, { asset, detail: resolution.detail ?? null }, asset);
        this.scheduleReferenceRetry(asset, resolution.retryAfterMs);
        continue;
      }
      this.clearReferenceRetry(asset);
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
  }

  private scheduleReferenceRetry(asset: string, retryAfterMs: number | undefined): void {
    this.clearReferenceRetry(asset);
    if (this.abort.signal.aborted) return;
    if (![...this.assetByMarket.values()].includes(asset)) return;
    const delayMs = Math.max(50, Math.min(retryAfterMs ?? 30_000, 60_000));
    const timer = setTimeout(() => { void this.retryTransientReference(asset); }, delayMs);
    timer.unref();
    this.referenceRetryTimers.set(asset, timer);
  }

  private clearReferenceRetry(asset: string): void {
    const timer = this.referenceRetryTimers.get(asset);
    if (!timer) return;
    clearTimeout(timer);
    this.referenceRetryTimers.delete(asset);
  }

  private async retryTransientReference(asset: string): Promise<void> {
    this.referenceRetryTimers.delete(asset);
    if (this.abort.signal.aborted || this.state.draining) return;
    if (![...this.assetByMarket.values()].includes(asset)) return;
    await this.reconcileReferenceFeeds();
  }

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

  private async restoreOrStartExperiment(): Promise<void> {
    const checkpoint = await this.journal.loadLatestCheckpoint();
    if (checkpoint) {
      const payload = checkpoint.payload as Record<string, unknown>;
      const ledger = ledgerStateSchema.parse(payload.ledger);
      const positions = Array.isArray(payload.positions) ? payload.positions.map((item) => positionSchema.parse(item)) : [];
      this.ledger.restore(ledger, positions);
      this.state.setLedger(this.ledger.snapshot());
      this.experiment = experimentManifestSchema.parse(payload.experiment);
      const resting = Array.isArray(payload.restingOrders) ? payload.restingOrders.map((item) => restingPaperOrderSchema.parse(item)) : [];
      for (const item of resting) this.restingOrders.set(item.order.id, item);
      const triggers = Array.isArray(payload.activeTriggers) ? payload.activeTriggers.map((item) => triggerEventSchema.parse(item)) : [];
      for (const trigger of triggers) { this.activeTriggers.set(trigger.id, trigger); this.state.addTrigger(trigger); }
      const signals = Array.isArray(payload.grokSignals) ? payload.grokSignals.map((item) => grokSignalEnvelopeSchema.parse(item)) : [];
      for (const signal of signals) {
        const byRole = this.signalsByTrigger.get(signal.triggerId) ?? new Map<ExternalAgentRoleId, GrokSignalEnvelope>();
        byRole.set(signal.agentId, signal); this.signalsByTrigger.set(signal.triggerId, byRole); this.state.addGrokSignal(signal);
      }
      this.checkpointSequence = checkpoint.sequence;
      this.record("STATE_RECOVERED", this.experiment.id, null, { checkpointId: checkpoint.id, positions: positions.length, restingOrders: resting.length, activeTriggers: triggers.length, grokSignals: signals.length });
    } else {
      this.experiment = startExperiment({
        durationHours: this.env.EXPERIMENT_DURATION_HOURS, strategyVersion: this.strategy.version,
        riskConfiguration: this.riskLimits, modelConfiguration: { provider: "external_grok_bot_mcp", logicalRoles: 6, localModelExecution: false },
        startingBalance: this.ledger.snapshot().startingBalance, systemVersion: "0.1.0",
        gitSha: process.env.GIT_SHA ?? process.env.FLY_IMAGE_REF ?? "unknown", now: new Date(),
      });
      await this.safeAppend(createJournalEvent({ traceId: createId("trace"), type: "EXPERIMENT_STARTED", aggregateType: "experiment", aggregateId: this.experiment.id, payload: this.experiment }));
      await this.checkpoint();
    }
    this.state.experimentId = this.experiment.id;
    this.state.experimentStatus = this.experiment.status;
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
    await this.safeAppend(createJournalEvent({ traceId: snapshot.traceId, type: "MARKET_SNAPSHOT", aggregateType: "market", aggregateId: snapshot.marketId, payload: snapshot, occurredAt: new Date(snapshot.sourceTimestamp), sourceEventId: snapshot.eventId }));
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
    this.state.eventLatencyMs = performance.now() - started;
    this.metrics.eventLatency.observe((performance.now() - started) / 1000);
  }

  private async onReferenceTick(tick: ReferenceTick): Promise<void> {
    if (this.usesDiscovery() && ![...this.assetByMarket.values()].includes(tick.asset)) return;
    this.state.noteReferenceTick(tick.asset, tick.receivedAt);
    if (tick.asset === "BTC") {
      this.state.lastBtcMessageAt = tick.receivedAt;
      this.state.connections.btc.lastMessageAt = tick.receivedAt;
    }
    this.updateReady();
    await this.safeAppend(createJournalEvent({ traceId: tick.traceId, type: "REFERENCE_TICK", aggregateType: "reference", aggregateId: tick.asset, payload: tick, occurredAt: new Date(tick.sourceTimestamp), sourceEventId: tick.eventId }));
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
    const fair = outputs.find((item) => item.agentId === "fair_probability_analyst")?.output as { estimatedEdge?: number } | undefined;
    const directional = outputs.flatMap((item) => {
      const output = item.output as { direction?: unknown; confidence?: unknown; expiresAt?: unknown; evidenceIds?: unknown };
      return (output.direction === "UP" || output.direction === "DOWN" || output.direction === "FLAT") && typeof output.confidence === "number"
        && typeof output.expiresAt === "string" && Array.isArray(output.evidenceIds) && new Date(output.expiresAt).getTime() > Date.now()
        ? [{ agentId: item.agentId, direction: output.direction as Direction, confidence: output.confidence, expiresAt: output.expiresAt, evidenceIds: output.evidenceIds.map(String) }]
        : [];
    });
    if (!fair || typeof fair.estimatedEdge !== "number" || directional.length < 2 || this.lastEvaluatedSignalCount.get(trigger.id) === outputs.length) return;
    this.lastEvaluatedSignalCount.set(trigger.id, outputs.length);
    const consensus = buildConsensus({ traceId: trigger.traceId, triggerId: trigger.id, signals: directional, deterministicEdge: fair.estimatedEdge, now: new Date(), config: defaultConsensusConfig });
    this.state.latestConsensus = consensus;
    this.record("CONSENSUS_COMPLETE", trigger.traceId, trigger.marketId, { direction: consensus.consensusDirection, confidence: consensus.consensusConfidence, conflicts: consensus.conflicts, triggerId: trigger.id }, trigger.asset ?? null);
    await this.safeAppend(createJournalEvent({ traceId: trigger.traceId, type: "CONSENSUS_DECISION", aggregateType: "consensus", aggregateId: consensus.id, payload: { ...consensus, marketId: trigger.marketId, asset: trigger.asset ?? null } }));
    if (trigger.marketId) await this.evaluateTrade(trigger, consensus, outputs);
  }

  private async evaluateTrade(trigger: TriggerEvent, consensus: NonNullable<EngineStateStore["latestConsensus"]>, outputs: GrokSignalEnvelope[]): Promise<void> {
    const pair = this.snapshots.get(trigger.marketId ?? "");
    if (!pair) return;
    const position = this.ledger.positions.get(trigger.marketId ?? "") ?? null;
    this.experiment = this.experiment ? refreshExperiment(this.experiment, new Date()) : null;
    if (this.experiment) this.state.experimentStatus = this.experiment.status;
    const candidate = this.strategy.evaluate({
      consensus, snapshots: { UP: pair.UP ?? null, DOWN: pair.DOWN ?? null }, position,
      maxOrderSizeUsd: this.riskLimits.maxOrderSizeUsd, minimumLockedPairEdge: this.riskLimits.minExpectedEdge,
    });
    if (!candidate) return;
    const snapshot = this.state.marketSnapshots.get(candidate.tokenId);
    if (!snapshot) return;
    const duplicateKey = `${candidate.marketId}:${candidate.outcome}:${candidate.intent}`;
    const now = new Date();
    const externalRisk = outputs.find((item) => item.agentId === "risk_hedge_analyst")?.output as { recommendVeto?: boolean } | undefined;
    const earliestSignalExpiry = outputs.reduce((earliest, item) => {
      const expiresAt = (item.output as { expiresAt?: string }).expiresAt;
      return expiresAt && new Date(expiresAt).getTime() < earliest.getTime() ? new Date(expiresAt) : earliest;
    }, new Date(trigger.expiresAt));
    const asset = trigger.asset ?? this.assetByMarket.get(candidate.marketId) ?? snapshot.asset ?? null;
    const exposure = computePaperExposures(this.ledger.positions.values(), candidate.marketId, this.assetByMarket);
    const referenceRequired = this.referenceRequired.get(candidate.marketId) === true;
    const referenceFeedAt = asset ? dateOrNull(this.state.referenceLastMessageAt(asset)) : null;
    const decision = decidePaperOrder({
      candidate, consensus, snapshotAfterDecisionLatency: snapshot, limits: this.riskLimits, triggerId: trigger.id,
      signalExpiresAt: earliestSignalExpiry, orderType: this.env.PAPER_EXECUTION_STYLE,
      baseRisk: {
        now, marketFeedAt: dateOrNull(this.state.connections.polymarket.lastMessageAt),
        btcFeedAt: asset === "BTC" ? referenceFeedAt ?? dateOrNull(this.state.lastBtcMessageAt) : now,
        referenceFeedAt, referenceRequired, asset, assetExposureUsd: exposure.asset,
        databaseAvailable: await this.journal.healthCheck(), timestampDisorder: this.timestampDisorder, riskStateAvailable: true,
        dailyPnlUsd: this.ledger.snapshot().netTradingPnl, drawdownUsd: this.ledger.snapshot().drawdown,
        marketExposureUsd: exposure.market, totalExposureUsd: exposure.total, directionalExposureUsd: exposure.directional,
        unhedgedExposureUsd: exposure.unhedged, unhedgedDurationMs: exposure.unhedgedDurationMs,
        externalSignalsLastMinute: this.signalReceivedTimes.filter((at) => now.getTime() - at < 60_000).length,
        lastStrategyTradeAt: this.lastTradeAt,
        duplicateTrade: (this.recentTrades.get(duplicateKey) ?? 0) > now.getTime() - this.riskLimits.strategyCooldownMs,
        experimentEntryOpen: candidate.intent === "HEDGE_SECOND_LEG" || this.experiment?.status === "RUNNING",
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
    await this.submitPaperOrder(decision.order, snapshot, duplicateKey);
  }

  private async submitPaperOrder(order: PaperOrder, decisionSnapshot: MarketSnapshot, duplicateKey: string): Promise<void> {
    this.metrics.paperOrders.inc({ type: order.orderType });
    this.record("PAPER_ORDER_CREATED", order.traceId, order.marketId, { orderId: order.id, type: order.orderType }, decisionSnapshot.asset ?? null);
    await this.safeAppend(createJournalEvent({ traceId: order.traceId, type: "PAPER_ORDER_CREATED", aggregateType: "paper_order", aggregateId: order.id, payload: order }));
    this.recentTrades.set(duplicateKey, Date.now());
    try {
      if (order.orderType === "MAKER") {
        this.restingOrders.set(order.id, createRestingMakerOrder(order, decisionSnapshot));
        return;
      }
      await delay(this.env.PAPER_EXECUTION_LATENCY_MS);
      const current = this.state.marketSnapshots.get(order.tokenId);
      if (!current || new Date(current.receivedAt).getTime() < new Date(decisionSnapshot.receivedAt).getTime()) throw new Error("STALE_EXECUTION_SNAPSHOT");
      await this.applyFill(executePaperTaker({ order, snapshotAfterLatency: current, measuredLatencyMs: this.env.PAPER_EXECUTION_LATENCY_MS }));
      this.lastTradeAt = new Date();
    } catch (error) {
      this.executionErrors.push(Date.now());
      this.record("PAPER_EXECUTION_ERROR", order.traceId, order.marketId, { orderId: order.id, message: error instanceof Error ? error.message : String(error) });
      await this.safeAppend(createJournalEvent({ traceId: order.traceId, type: "PAPER_EXECUTION_ERROR", aggregateType: "paper_order", aggregateId: order.id, payload: { message: error instanceof Error ? error.message : String(error) } }));
    }
  }

  private async applyFill(fill: Parameters<PaperLedger["applyFill"]>[0]): Promise<void> {
    const position = this.ledger.applyFill(fill);
    this.state.setLedger(this.ledger.snapshot());
    this.metrics.paperFills.inc({ role: fill.liquidityRole });
    this.metrics.openPositions.set(this.ledger.positions.size);
    this.metrics.unhedgedPositions.set([...this.ledger.positions.values()].filter((item) => Number(item.unpairedQuantity) > 0).length);
    this.record("PAPER_FILL", fill.traceId, fill.marketId, { fillId: fill.id, orderId: fill.orderId, price: fill.fillPrice, quantity: fill.quantity, role: fill.liquidityRole });
    if (Number(position.pairedQuantity) > 0 && Number(position.unpairedQuantity) === 0) this.record("HEDGE_COMPLETE", fill.traceId, fill.marketId, { positionId: position.id, pairedQuantity: position.pairedQuantity });
    await this.safeAppend(createJournalEvent({
      traceId: fill.traceId, type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: fill.id,
      payload: { ...fill, asset: this.assetByMarket.get(fill.marketId) ?? null }, sourceEventId: fill.sourceMarketEventId,
    }));
    await this.safeAppend(createJournalEvent({ traceId: fill.traceId, type: "POSITION_UPDATED", aggregateType: "position", aggregateId: position.id, payload: position, sourceEventId: fill.id }));
    await this.safeAppend(createJournalEvent({ traceId: fill.traceId, type: "PNL_SNAPSHOT", aggregateType: "ledger", aggregateId: this.experiment?.id ?? "paper", payload: this.ledger.snapshot(), sourceEventId: fill.id }));
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
    this.state.record(item);
    this.sse.publish("decision", item);
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
    this.accrueInfrastructureCost();
    const previous = this.experiment.status;
    this.experiment = refreshExperiment(this.experiment, new Date());
    this.state.experimentStatus = this.experiment.status;
    if (previous !== this.experiment.status) {
      await this.safeAppend(createJournalEvent({ traceId: this.experiment.id, type: "EXPERIMENT_ENTRY_CLOSED", aggregateType: "experiment", aggregateId: this.experiment.id, payload: this.experiment }));
      await this.checkpoint();
    }
    if (this.experiment.status === "ENTRY_CLOSED" && !this.reportGenerated) {
      this.reportGenerated = true;
      const summary = buildExperimentSummary(await this.journal.readAll(), this.experiment.id);
      await writeExperimentSummary(summary, process.env.REPORT_DIRECTORY ?? "reports");
      this.record("EXPERIMENT_REPORT_GENERATED", this.experiment.id, null, { reportDirectory: process.env.REPORT_DIRECTORY ?? "reports" });
    }
  }
}

export function parseEnv(env: NodeJS.ProcessEnv): EngineEnv { return engineEnvSchema.parse(env); }
function splitList(value: string): string[] { return value.split(",").map((item) => item.trim()).filter(Boolean); }
function uniqueStrings(values: readonly string[]): string[] { return [...new Set(values.filter(Boolean))]; }
function dateOrNull(value: string | null | undefined): Date | null { return value ? new Date(value) : null; }
function delay(ms: number): Promise<void> { return new Promise((resolve) => setTimeout(resolve, ms)); }
