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
  type ExternalAgentRoleId,
  type Direction,
  type GrokSignalEnvelope,
  type GrokSignalSubmission,
  type EngineEnv,
  type ExperimentManifest,
  type MarketSnapshot,
  type Outcome,
  type PaperOrder,
  type Position,
  type RestingPaperOrder,
  type TriggerEvent,
} from "@pm/core";
import { createJournalEvent, type EventJournal } from "@pm/db";
import { BtcWebSocketAdapter, PolymarketMarketDataAdapter, type FeedConnectionEvent } from "@pm/market-data";
import { EngineMetrics } from "./metrics.js";
import { SseHub } from "./server.js";
import { EngineStateStore } from "./state.js";
import { buildExperimentSummary, writeExperimentSummary } from "./report.js";

export class EngineService {
  readonly metrics = new EngineMetrics();
  readonly sse = new SseHub();
  readonly ledger: PaperLedger;
  readonly state: EngineStateStore;
  private readonly abort = new AbortController();
  private readonly triggerEngine;
  private readonly riskLimits;
  private readonly strategy = new TemporalPairHedgeStrategy();
  private readonly marketAdapter: PolymarketMarketDataAdapter;
  private readonly btcAdapter: BtcWebSocketAdapter;
  private readonly snapshots = new Map<string, Partial<Record<Outcome, MarketSnapshot>>>();
  private readonly restingOrders = new Map<string, RestingPaperOrder>();
  private readonly recentTrades = new Map<string, number>();
  private readonly activeTriggers = new Map<string, TriggerEvent>();
  private readonly signalsByTrigger = new Map<string, Map<ExternalAgentRoleId, GrokSignalEnvelope>>();
  private readonly signalReceivedTimes: number[] = [];
  private readonly lastEvaluatedSignalCount = new Map<string, number>();
  private experiment: ExperimentManifest | null = null;
  private checkpointSequence = 0;
  private lastTradeAt: Date | null = null;
  private executionErrors: number[] = [];
  private timestampDisorder = false;
  private started = false;
  private maintenanceTimer: NodeJS.Timeout | null = null;
  private lastInfrastructureAccrualAt = Date.now();
  private reportGenerated = false;

  constructor(
    readonly env: EngineEnv,
    private readonly journal: EventJournal,
    private readonly logger: { error(bindings: object, message: string): void },
    adapters?: { market?: PolymarketMarketDataAdapter; btc?: BtcWebSocketAdapter },
  ) {
    this.ledger = new PaperLedger(env.PAPER_START_BALANCE);
    this.state = new EngineStateStore(this.ledger.snapshot(), this.ledger.positions);
    this.triggerEngine = new TriggerEngine(triggerConfigSchema.parse({}));
    this.riskLimits = riskLimitsSchema.parse({ maxExternalSignalsPerMinute: env.GROK_MCP_RATE_LIMIT_PER_MINUTE });
    this.marketAdapter = adapters?.market ?? new PolymarketMarketDataAdapter();
    this.btcAdapter = adapters?.btc ?? new BtcWebSocketAdapter(env.BTC_WS_URL, env.BTC_SYMBOL, this.riskLimits.staleFeedTimeoutMs);
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    await this.initializeJournalWithRetry();
    await this.restoreOrStartExperiment();
    const configuredTokens = splitList(this.env.POLYMARKET_TOKEN_IDS);
    let tokenIds = configuredTokens;
    if (tokenIds.length > 0) tokenIds.forEach((tokenId, index) => this.marketAdapter.registerManualToken(tokenId, tokenId, index % 2 === 0 ? "UP" : "DOWN"));
    else {
      const markets = await this.marketAdapter.discoverBitcoinMarkets(splitList(this.env.POLYMARKET_MARKET_SLUGS));
      tokenIds = markets.flatMap((market) => market.tokens.map((token) => token.tokenId));
      await this.safeAppend(createJournalEvent({
        traceId: createId("trace"), type: "MARKETS_DISCOVERED", aggregateType: "engine", aggregateId: "market-discovery",
        payload: { count: markets.length, markets: markets.map((market) => ({ id: market.id, slug: market.slug, tokens: market.tokens })) },
      }));
    }
    if (tokenIds.length === 0) throw new Error("NO_ACTIVE_BITCOIN_MARKETS_FOUND");

    void this.marketAdapter.run(tokenIds, {
      onSnapshot: (snapshot) => this.onMarketSnapshot(snapshot),
      onConnection: (event) => this.onConnection(event),
      onError: (error) => this.onFeedError("polymarket", error),
    }, this.abort.signal);
    void this.btcAdapter.run({
      onTick: (tick) => this.onBtcTick(tick),
      onConnection: (event) => this.onConnection(event),
      onError: (error) => this.onFeedError("btc", error),
    }, this.abort.signal);
    this.maintenanceTimer = setInterval(() => { void this.maintainExperiment(); }, 60_000);
    this.maintenanceTimer.unref();
    this.record("ENGINE_STARTED", this.experiment?.id ?? createId("engine"), null, { tokenIds: tokenIds.length, mode: "paper" });
  }

  async stop(): Promise<void> {
    if (this.state.draining) return;
    this.state.draining = true;
    this.state.ready = false;
    this.record("ENGINE_DRAINING", this.experiment?.id ?? "engine", null, {});
    this.abort.abort("SIGTERM");
    if (this.maintenanceTimer) clearInterval(this.maintenanceTimer);
    this.accrueInfrastructureCost();
    await this.checkpoint();
    this.sse.close();
    await this.journal.close();
    this.record("ENGINE_STOPPED", this.experiment?.id ?? "engine", null, {});
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
    const started = performance.now();
    this.metrics.marketMessages.inc();
    this.state.connections.polymarket.lastMessageAt = snapshot.receivedAt;
    this.state.marketSnapshots.set(snapshot.tokenId, snapshot);
    const pair = this.snapshots.get(snapshot.marketId) ?? {};
    const previous = pair[snapshot.outcome];
    pair[snapshot.outcome] = snapshot;
    this.snapshots.set(snapshot.marketId, pair);
    this.updateReady();
    await this.safeAppend(createJournalEvent({ traceId: snapshot.traceId, type: "MARKET_SNAPSHOT", aggregateType: "market", aggregateId: snapshot.marketId, payload: snapshot, occurredAt: new Date(snapshot.sourceTimestamp), sourceEventId: snapshot.eventId }));
    this.record("MARKET_EVENT", snapshot.traceId, snapshot.marketId, { tokenId: snapshot.tokenId, outcome: snapshot.outcome, sequence: snapshot.sequence });

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

  private async onBtcTick(tick: Parameters<TriggerEngine["ingestBtc"]>[0]): Promise<void> {
    this.state.lastBtcMessageAt = tick.receivedAt;
    this.state.connections.btc.lastMessageAt = tick.receivedAt;
    this.updateReady();
    await this.safeAppend(createJournalEvent({ traceId: tick.traceId, type: "BTC_TICK", aggregateType: "btc", aggregateId: tick.symbol, payload: tick, occurredAt: new Date(tick.sourceTimestamp), sourceEventId: tick.eventId }));
    for (const trigger of this.triggerEngine.ingestBtc(tick)) void this.processTrigger(trigger);
  }

  private async processTrigger(trigger: TriggerEvent): Promise<void> {
    if (this.state.draining) return;
    this.state.queueDepth += 1;
    this.metrics.queueDepth.set(this.state.queueDepth);
    this.metrics.triggers.inc({ kind: trigger.kind, severity: String(trigger.severity) });
    this.record(`TRIGGER_LEVEL_${trigger.severity}`, trigger.traceId, trigger.marketId, { triggerId: trigger.id, kind: trigger.kind });
    await this.safeAppend(createJournalEvent({ traceId: trigger.traceId, type: "TRIGGER_EVENT", aggregateType: "trigger", aggregateId: trigger.id, payload: trigger, sourceEventId: trigger.sourceEventIds[0] ?? null }));
    try {
      this.activeTriggers.set(trigger.id, trigger);
      this.state.addTrigger(trigger);
      this.record("GROK_RESEARCH_AVAILABLE", trigger.traceId, trigger.marketId, { triggerId: trigger.id, expiresAt: trigger.expiresAt });
    } finally {
      this.state.queueDepth -= 1;
      this.metrics.queueDepth.set(this.state.queueDepth);
    }
  }

  /** The only ingress for external Grok research; it cannot create orders directly. */
  async submitGrokSignal(raw: GrokSignalSubmission, clientId: string): Promise<{ status: "accepted" | "duplicate" | "stale" | "unknown_trigger" | "trace_mismatch" | "market_mismatch" | "replay_rejected" | "malformed" | "identity_mismatch"; signalId?: string }> {
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
      agentId: submission.agentId, role: submission.role, provider: "grok_bot_mcp", clientId,
      receivedAt: now.toISOString(), timestamp: submission.timestamp, output: submission.output, resultingDecision: null,
    });
    const byRole = this.signalsByTrigger.get(trigger.id) ?? new Map<ExternalAgentRoleId, GrokSignalEnvelope>();
    byRole.set(signal.agentId, signal);
    this.signalsByTrigger.set(trigger.id, byRole);
    this.signalReceivedTimes.push(now.getTime());
    this.state.addGrokSignal(signal);
    this.metrics.grokSignals.inc({ agent: signal.agentId, status: "accepted" });
    this.record(`${signal.agentId.toUpperCase()}_COMPLETE`, signal.traceId, trigger.marketId, { signalId: signal.signalId, clientId, confidence: submission.output.confidence, expiresAt: submission.output.expiresAt });
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
    this.record("CONSENSUS_COMPLETE", trigger.traceId, trigger.marketId, { direction: consensus.consensusDirection, confidence: consensus.consensusConfidence, conflicts: consensus.conflicts });
    await this.safeAppend(createJournalEvent({ traceId: trigger.traceId, type: "CONSENSUS_DECISION", aggregateType: "consensus", aggregateId: consensus.id, payload: consensus }));
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
    const exposure = this.exposure(candidate.marketId);
    const decision = decidePaperOrder({
      candidate, consensus, snapshotAfterDecisionLatency: snapshot, limits: this.riskLimits, triggerId: trigger.id,
      signalExpiresAt: earliestSignalExpiry, orderType: this.env.PAPER_EXECUTION_STYLE,
      baseRisk: {
        now, marketFeedAt: dateOrNull(this.state.connections.polymarket.lastMessageAt), btcFeedAt: dateOrNull(this.state.lastBtcMessageAt),
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
    await this.safeAppend(createJournalEvent({ traceId: trigger.traceId, type: "RISK_EVALUATION", aggregateType: "risk", aggregateId: decision.risk.id, payload: decision.risk }));
    if (!decision.accepted || !decision.order) {
      for (const reason of decision.risk.vetoes) this.metrics.riskVetoes.inc({ reason });
      this.record("RISK_VETO", trigger.traceId, trigger.marketId, { reasons: decision.risk.vetoes, netExpectedEdge: decision.netExpectedEdge });
      return;
    }
    this.record("RISK_ACCEPT", trigger.traceId, trigger.marketId, { orderId: decision.order.id, netExpectedEdge: decision.netExpectedEdge });
    await this.submitPaperOrder(decision.order, snapshot, duplicateKey);
  }

  private async submitPaperOrder(order: PaperOrder, decisionSnapshot: MarketSnapshot, duplicateKey: string): Promise<void> {
    this.metrics.paperOrders.inc({ type: order.orderType });
    this.record("PAPER_ORDER_CREATED", order.traceId, order.marketId, { orderId: order.id, type: order.orderType });
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
    await this.safeAppend(createJournalEvent({ traceId: fill.traceId, type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: fill.id, payload: fill, sourceEventId: fill.sourceMarketEventId }));
    await this.safeAppend(createJournalEvent({ traceId: fill.traceId, type: "POSITION_UPDATED", aggregateType: "position", aggregateId: position.id, payload: position, sourceEventId: fill.id }));
    await this.safeAppend(createJournalEvent({ traceId: fill.traceId, type: "PNL_SNAPSHOT", aggregateType: "ledger", aggregateId: this.experiment?.id ?? "paper", payload: this.ledger.snapshot(), sourceEventId: fill.id }));
  }

  private exposure(marketId: string): { market: string; total: string; directional: string; unhedged: string; unhedgedDurationMs: number } {
    let total = 0; let directional = 0; let market = 0; let duration = 0;
    for (const position of this.ledger.positions.values()) {
      const value = Number(position.upQuantity) * Number(position.averageUpCost) + Number(position.downQuantity) * Number(position.averageDownCost);
      const unhedged = Number(position.unpairedQuantity) * Number(position.unpairedOutcome === "UP" ? position.averageUpCost : position.averageDownCost);
      total += value; directional += unhedged;
      if (position.marketId === marketId) { market = value; if (unhedged > 0) duration = Date.now() - new Date(position.openedAt).getTime(); }
    }
    const selected = this.ledger.positions.get(marketId);
    const unhedged = selected ? Number(selected.unpairedQuantity) * Number(selected.unpairedOutcome === "UP" ? selected.averageUpCost : selected.averageDownCost) : 0;
    return { market: String(market), total: String(total), directional: String(directional), unhedged: String(unhedged), unhedgedDurationMs: Math.max(0, duration) };
  }

  private async onConnection(event: FeedConnectionEvent): Promise<void> {
    const connection = this.state.connections[event.feed];
    if (event.reconnectCount > connection.reconnects) this.metrics.wsReconnects.inc({ feed: event.feed }, event.reconnectCount - connection.reconnects);
    connection.state = event.state;
    connection.reconnects = event.reconnectCount;
    this.updateReady();
    this.record(`WS_${event.state}`, createId("trace"), null, { feed: event.feed, reconnects: event.reconnectCount, message: event.message });
  }

  private async onFeedError(feed: string, error: Error): Promise<void> {
    if (error.message.includes("EVENT_TIMESTAMP_DISORDER")) this.timestampDisorder = true;
    this.logger.error({ err: error, feed }, "market feed error");
    await this.safeAppend(createJournalEvent({ traceId: createId("trace"), type: "SYSTEM_HEALTH_EVENT", aggregateType: "feed", aggregateId: feed, payload: { status: "ERROR", message: error.message } }));
  }

  private updateReady(): void {
    this.state.ready = !this.state.draining && this.state.database === "UP"
      && this.state.connections.polymarket.state === "CONNECTED" && this.state.connections.btc.state === "CONNECTED"
      && this.state.connections.polymarket.lastMessageAt !== null && this.state.lastBtcMessageAt !== null;
  }

  private record(type: string, traceId: string, marketId: string | null, detail: Record<string, unknown>): void {
    const item = { id: createId("feed"), at: new Date().toISOString(), type, traceId, marketId, detail };
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
function dateOrNull(value: string | null): Date | null { return value ? new Date(value) : null; }
function delay(ms: number): Promise<void> { return new Promise((resolve) => setTimeout(resolve, ms)); }
