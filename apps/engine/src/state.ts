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
    eventLatencyMs: number | null;
    lastBtcMessageAt: string | null;
    unattributedMcp: UnattributedMcpActivity;
  };
  equityHistory: Array<{ at: string; equity: string; drawdown: string }>;
}

export class EngineStateStore {
  readonly startedAt = new Date();
  ready = false;
  draining = false;
  queueDepth = 0;
  experimentId: string | null = null;
  experimentStatus: string | null = null;
  database: "UP" | "DOWN" = "DOWN";
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
  dropReference(asset: string): void { this.reference.delete(asset); }
  engineAlive(): boolean { return !this.draining && this.database === "UP"; }

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
    };
  }

  noteReferenceTick(asset: string, at: string): void {
    const current = this.reference.get(asset) ?? { state: "CONNECTED", lastMessageAt: null, reconnects: 0 };
    current.lastMessageAt = at;
    this.reference.set(asset, current);
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
        database: this.database, eventLatencyMs: this.eventLatencyMs, lastBtcMessageAt: this.lastBtcMessageAt,
        unattributedMcp: structuredClone(this.unattributedMcp),
      },
      equityHistory: structuredClone(this.equityHistory),
    };
  }
}
