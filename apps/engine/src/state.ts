import type { ActivationLatency, ConsensusDecision, ExternalAgentRoleId, GrokSignalEnvelope, LedgerState, MarketSnapshot, Position, QualificationReasonCode, RiskEvaluation, TriggerEvent } from "@pm/core";

export interface DecisionFeedItem {
  id: string;
  at: string;
  type: string;
  traceId: string;
  marketId: string | null;
  asset?: string | null;
  detail: Record<string, unknown>;
}

export interface ConnectionState {
  state: "CONNECTING" | "CONNECTED" | "DISCONNECTED" | "STALE" | "STOPPED";
  lastMessageAt: string | null;
  reconnects: number;
}

export interface ActivationRecord {
  triggerId: string;
  role?: string;
  provider: string;
  ok?: boolean;
  skipped?: boolean;
  errorCode?: string | null;
  reason?: string;
  grokWakeObserved: false;
  at: string;
}

export type DiscoveryStatus = "ZERO_QUALIFIED" | "MONITORING" | "RECONCILING";

export interface UnattributedMcpActivity {
  lastAt: string | null;
  count: number;
  lastTool: string | null;
}

export type OverloadState = "NORMAL" | "DEGRADED" | "FAIL_CLOSED";

export interface EngineReadiness {
  status: "ready" | "not_ready";
  mode: "paper";
  engineAlive: boolean;
  draining: boolean;
  database: "UP" | "DOWN";
  discoveryStatus: DiscoveryStatus;
  discoveryLoopActive: boolean;
  marketMonitoring: boolean;
  qualifiedMarketCount: number;
  polymarketWs: ConnectionState["state"];
  overloadState: OverloadState;
  journalDegraded: boolean;
  telemetryDegraded: boolean;
  newPaperEntryAllowed: boolean;
}

export interface DashboardSnapshot {
  paperMode: true;
  generatedAt: string;
  engine: {
    ready: boolean;
    draining: boolean;
    alive: boolean;
    uptimeSeconds: number;
    queueDepth: number;
    experimentId: string | null;
    experimentStatus: string | null;
    discoveryStatus: DiscoveryStatus;
    discoveryLoopActive: boolean;
    marketMonitoring: boolean;
    qualifiedMarketCount: number;
    overloadState: OverloadState;
    journalDegraded: boolean;
    telemetryDegraded: boolean;
    newPaperEntryAllowed: boolean;
  };
  balance: LedgerState;
  positions: Position[];
  latestAgents: Partial<Record<ExternalAgentRoleId, GrokSignalEnvelope>>;
  consensus: ConsensusDecision | null;
  risk: RiskEvaluation | null;
  scanner: Array<{ marketId: string; asset: string | null; outcome: string; bid: string | null; ask: string | null; spread: number | null; depthUsd: number; lastMessageAt: string }>;
  decisionFeed: DecisionFeedItem[];
  grokActivity: { acceptedSignals: number; rejectedSignals: number; duplicates: number; unauthorizedCalls: number; rateLimitedCalls: number; lastSeenAt: string | null; byAgent: Record<string, number> };
  recentTriggers: TriggerEvent[];
  activations: ActivationRecord[];
  activationLatency: ActivationLatency[];
  unattributedMcp: UnattributedMcpActivity;
  discovery: { qualified: Array<{ marketId: string; slug: string; asset: string | null; tokens: string[] }>; rejected: Array<{ id: string; slug: string; title: string; reasonCodes: QualificationReasonCode[] }> };
  health: {
    engineAlive: boolean;
    discoveryStatus: DiscoveryStatus;
    discoveryLoopActive: boolean;
    marketMonitoring: boolean;
    qualifiedMarketCount: number;
    polymarketWs: ConnectionState;
    btcWs: ConnectionState;
    referenceFeeds: Array<{ asset: string; state: ConnectionState["state"]; lastMessageAt: string | null; reconnects: number }>;
    database: "UP" | "DOWN";
    journalDegraded: boolean;
    telemetryDegraded: boolean;
    overloadState: OverloadState;
    eventLatencyMs: number | null;
    lastBtcMessageAt: string | null;
    unattributedMcp: UnattributedMcpActivity;
  };
  equityHistory: Array<{ at: string; equity: string; drawdown: string }>;
}

export interface ReferenceContext {
  asset: string;
  symbol: string | null;
  venue: string | null;
  latestPrice: string | null;
  lastMessageAt: string | null;
  freshness: "fresh" | "stale" | "unknown";
  freshnessMs: number | null;
  recentMoveBps: number | null;
  recentVolatilityBps: number | null;
}

export class EngineStateStore {
  readonly startedAt = new Date();
  ready = false;
  draining = false;
  queueDepth = 0;
  experimentId: string | null = null;
  experimentStatus: string | null = null;
  experimentStartedAt: string | null = null;
  experimentCompletedAt: string | null = null;
  database: "UP" | "DOWN" = "DOWN";
  journalDegraded = false;
  recoveryComplete = false;
  telemetryDegraded = false;
  overloadState: OverloadState = "NORMAL";
  lastBtcMessageAt: string | null = null;
  eventLatencyMs: number | null = null;
  discoveryStatus: DiscoveryStatus = "ZERO_QUALIFIED";
  discoveryLoopActive = false;
  marketMonitoring = false;
  qualifiedMarketCount = 0;
  readonly connections: Record<"polymarket" | "btc", ConnectionState> = {
    polymarket: { state: "STOPPED", lastMessageAt: null, reconnects: 0 },
    btc: { state: "STOPPED", lastMessageAt: null, reconnects: 0 },
  };
  readonly marketSnapshots = new Map<string, MarketSnapshot>();
  readonly latestAgents = new Map<ExternalAgentRoleId, GrokSignalEnvelope>();
  readonly recentTriggers: TriggerEvent[] = [];
  latestConsensus: ConsensusDecision | null = null;
  latestRisk: RiskEvaluation | null = null;
  readonly feed: DecisionFeedItem[] = [];
  readonly equityHistory: Array<{ at: string; equity: string; drawdown: string }> = [];
  grokActivity = { acceptedSignals: 0, rejectedSignals: 0, duplicates: 0, unauthorizedCalls: 0, rateLimitedCalls: 0, lastSeenAt: null as string | null, byAgent: {} as Record<string, number> };
  private readonly reference = new Map<string, ConnectionState>();
  private discovery: DashboardSnapshot["discovery"] = { qualified: [], rejected: [] };
  private readonly activations: ActivationRecord[] = [];
  private activationLatency: ActivationLatency[] = [];
  private unattributedMcp: UnattributedMcpActivity = { lastAt: null, count: 0, lastTool: null };
  private readonly referenceContext = new Map<string, {
    symbol: string | null;
    venue: string | null;
    latestPrice: string | null;
    lastMessageAt: string | null;
    prices: Array<{ price: number; at: number }>;
  }>();

  constructor(private ledger: LedgerState, private readonly positions: Map<string, Position>) {}

  setLedger(ledger: LedgerState): void {
    this.ledger = structuredClone(ledger);
    this.equityHistory.push({ at: ledger.updatedAt, equity: ledger.equity, drawdown: ledger.drawdown });
    if (this.equityHistory.length > 2_000) this.equityHistory.splice(0, this.equityHistory.length - 2_000);
  }

  addGrokSignal(signal: GrokSignalEnvelope): void {
    this.latestAgents.set(signal.agentId, signal);
    this.grokActivity.acceptedSignals += 1;
    this.grokActivity.lastSeenAt = signal.receivedAt;
    this.grokActivity.byAgent[signal.agentId] = (this.grokActivity.byAgent[signal.agentId] ?? 0) + 1;
  }

  addTrigger(trigger: TriggerEvent): void {
    this.recentTriggers.unshift(structuredClone(trigger));
    if (this.recentTriggers.length > 500) this.recentTriggers.length = 500;
  }

  addActivation(record: ActivationRecord): void {
    this.activations.unshift(record);
    if (this.activations.length > 200) this.activations.length = 200;
  }

  setActivationLatency(items: ActivationLatency[]): void { this.activationLatency = structuredClone(items); }
  setDiscovery(discovery: DashboardSnapshot["discovery"]): void { this.discovery = structuredClone(discovery); }
  setDiscoveryStatus(status: DiscoveryStatus, qualifiedMarketCount: number): void {
    this.discoveryStatus = status;
    this.qualifiedMarketCount = qualifiedMarketCount;
  }
  setDiscoveryLoopActive(active: boolean): void { this.discoveryLoopActive = active; }
  setMarketMonitoring(monitoring: boolean): void { this.marketMonitoring = monitoring; }
  setUnattributedMcp(activity: UnattributedMcpActivity): void { this.unattributedMcp = structuredClone(activity); }
  dropReference(asset: string): void {
    this.reference.delete(asset);
    this.invalidateReferenceContext(asset);
  }

  invalidateReferenceContext(asset: string): void {
    this.referenceContext.delete(asset);
    this.referenceContext.delete(asset.trim().toUpperCase());
    const current = this.reference.get(asset);
    if (current) current.lastMessageAt = null;
    const keyed = this.reference.get(asset.trim().toUpperCase());
    if (keyed) keyed.lastMessageAt = null;
    if (asset === "BTC" || asset.trim().toUpperCase() === "BTC") this.lastBtcMessageAt = null;
  }
  engineAlive(): boolean { return !this.draining && this.database === "UP"; }
  newPaperEntryAllowed(): boolean {
    return this.engineAlive() && this.overloadState !== "FAIL_CLOSED" && this.recoveryComplete;
  }

  readiness(): EngineReadiness {
    const alive = this.engineAlive();
    return {
      status: alive ? "ready" : "not_ready",
      mode: "paper",
      engineAlive: alive,
      draining: this.draining,
      database: this.database,
      discoveryStatus: this.discoveryStatus,
      discoveryLoopActive: this.discoveryLoopActive,
      marketMonitoring: this.marketMonitoring,
      qualifiedMarketCount: this.qualifiedMarketCount,
      polymarketWs: this.connections.polymarket.state,
      overloadState: this.overloadState,
      journalDegraded: this.journalDegraded,
      telemetryDegraded: this.telemetryDegraded,
      newPaperEntryAllowed: this.newPaperEntryAllowed(),
    };
  }

  noteReferenceTick(asset: string, at: string, meta?: { symbol?: string; venue?: string; price?: string }): void {
    const current = this.reference.get(asset) ?? { state: "CONNECTED" as const, lastMessageAt: null, reconnects: 0 };
    current.lastMessageAt = at;
    this.reference.set(asset, current);
    const bounded = this.referenceContext.get(asset) ?? {
      symbol: null, venue: null, latestPrice: null, lastMessageAt: null, prices: [] as Array<{ price: number; at: number }>,
    };
    if (meta?.symbol) bounded.symbol = meta.symbol;
    if (meta?.venue) bounded.venue = meta.venue;
    if (meta?.price) bounded.latestPrice = meta.price;
    bounded.lastMessageAt = at;
    const price = meta?.price !== undefined ? Number(meta.price) : Number.NaN;
    if (Number.isFinite(price)) {
      bounded.prices.push({ price, at: new Date(at).getTime() });
      if (bounded.prices.length > 8) bounded.prices.splice(0, bounded.prices.length - 8);
    }
    this.referenceContext.set(asset, bounded);
  }

  getReferenceContext(asset: string, nowMs = Date.now(), staleMs = 15_000): ReferenceContext {
    const key = asset.trim().toUpperCase();
    const bounded = this.referenceContext.get(key) ?? this.referenceContext.get(asset);
    const lastMessageAt = bounded?.lastMessageAt ?? this.referenceLastMessageAt(key) ?? this.referenceLastMessageAt(asset);
    const freshnessMs = lastMessageAt ? Math.max(0, nowMs - new Date(lastMessageAt).getTime()) : null;
    const prices = bounded?.prices ?? [];
    const first = prices[0];
    const last = prices.at(-1);
    const recentMoveBps = first && last && first.price !== 0
      ? ((last.price - first.price) / first.price) * 10_000
      : null;
    let recentVolatilityBps: number | null = null;
    if (prices.length >= 2) {
      const mean = prices.reduce((sum, item) => sum + item.price, 0) / prices.length;
      if (mean !== 0) {
        const variance = prices.reduce((sum, item) => sum + (item.price - mean) ** 2, 0) / prices.length;
        recentVolatilityBps = (Math.sqrt(variance) / mean) * 10_000;
      }
    }
    return {
      asset: key || asset,
      symbol: bounded?.symbol ?? null,
      venue: bounded?.venue ?? null,
      latestPrice: bounded?.latestPrice ?? null,
      lastMessageAt: lastMessageAt ?? null,
      freshness: freshnessMs === null ? "unknown" : freshnessMs > staleMs ? "stale" : "fresh",
      freshnessMs,
      recentMoveBps,
      recentVolatilityBps,
    };
  }

  noteReferenceConnection(asset: string, state: ConnectionState["state"], reconnects: number): void {
    const current = this.reference.get(asset) ?? { state: "STOPPED", lastMessageAt: null, reconnects: 0 };
    current.state = state;
    current.reconnects = reconnects;
    this.reference.set(asset, current);
  }

  referenceLastMessageAt(asset: string): string | null {
    return this.reference.get(asset)?.lastMessageAt ?? (asset === "BTC" ? this.lastBtcMessageAt : null);
  }

  recordGrokRejection(kind: "rejected" | "duplicate" | "unauthorized" | "rate_limited"): void {
    if (kind === "rejected") this.grokActivity.rejectedSignals += 1;
    if (kind === "duplicate") this.grokActivity.duplicates += 1;
    if (kind === "unauthorized") this.grokActivity.unauthorizedCalls += 1;
    if (kind === "rate_limited") this.grokActivity.rateLimitedCalls += 1;
  }

  record(item: DecisionFeedItem): void {
    this.feed.unshift(item);
    if (this.feed.length > 500) this.feed.length = 500;
  }

  /** Bounded read-only feed used by observation sidecars; never exposes a live array. */
  decisionFeedForObservation(limit = 200): DecisionFeedItem[] {
    return structuredClone(this.feed.slice(0, Math.max(0, Math.min(limit, 500))));
  }

  snapshot(): DashboardSnapshot {
    const alive = this.engineAlive();
    return {
      paperMode: true, generatedAt: new Date().toISOString(),
      engine: {
        ready: this.ready, draining: this.draining, alive,
        uptimeSeconds: Math.floor((Date.now() - this.startedAt.getTime()) / 1000), queueDepth: this.queueDepth,
        experimentId: this.experimentId, experimentStatus: this.experimentStatus,
        discoveryStatus: this.discoveryStatus, discoveryLoopActive: this.discoveryLoopActive,
        marketMonitoring: this.marketMonitoring, qualifiedMarketCount: this.qualifiedMarketCount,
        overloadState: this.overloadState, journalDegraded: this.journalDegraded,
        telemetryDegraded: this.telemetryDegraded, newPaperEntryAllowed: this.newPaperEntryAllowed(),
      },
      balance: structuredClone(this.ledger), positions: structuredClone([...this.positions.values()]),
      latestAgents: Object.fromEntries(this.latestAgents), consensus: structuredClone(this.latestConsensus), risk: structuredClone(this.latestRisk),
      scanner: [...this.marketSnapshots.values()].map((snapshot) => ({
        marketId: snapshot.marketId, asset: snapshot.asset ?? null, outcome: snapshot.outcome, bid: snapshot.bids[0]?.price ?? null,
        ask: snapshot.asks[0]?.price ?? null,
        spread: snapshot.bids[0] && snapshot.asks[0] ? Number(snapshot.asks[0].price) - Number(snapshot.bids[0].price) : null,
        depthUsd: [...snapshot.bids, ...snapshot.asks].reduce((sum, level) => sum + Number(level.price) * Number(level.quantity), 0),
        lastMessageAt: snapshot.receivedAt,
      })),
      decisionFeed: structuredClone(this.feed), grokActivity: structuredClone(this.grokActivity), recentTriggers: structuredClone(this.recentTriggers),
      activations: structuredClone(this.activations), activationLatency: structuredClone(this.activationLatency),
      unattributedMcp: structuredClone(this.unattributedMcp), discovery: structuredClone(this.discovery),
      health: {
        engineAlive: alive, discoveryStatus: this.discoveryStatus, discoveryLoopActive: this.discoveryLoopActive,
        marketMonitoring: this.marketMonitoring, qualifiedMarketCount: this.qualifiedMarketCount,
        polymarketWs: structuredClone(this.connections.polymarket),
        btcWs: structuredClone(this.connections.btc),
        referenceFeeds: [...this.reference.entries()].map(([asset, connection]) => ({ asset, state: connection.state, lastMessageAt: connection.lastMessageAt, reconnects: connection.reconnects })),
        database: this.database,
        journalDegraded: this.journalDegraded,
        telemetryDegraded: this.telemetryDegraded,
        overloadState: this.overloadState,
        eventLatencyMs: this.eventLatencyMs, lastBtcMessageAt: this.lastBtcMessageAt,
        unattributedMcp: structuredClone(this.unattributedMcp),
      },
      equityHistory: structuredClone(this.equityHistory),
    };
  }
}
