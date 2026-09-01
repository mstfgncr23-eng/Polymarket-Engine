import type { ConsensusDecision, ExternalAgentRoleId, GrokSignalEnvelope, LedgerState, MarketSnapshot, Position, RiskEvaluation, TriggerEvent } from "@pm/core";

export interface DecisionFeedItem {
  id: string;
  at: string;
  type: string;
  traceId: string;
  marketId: string | null;
  detail: Record<string, unknown>;
}

export interface ConnectionState {
  state: "CONNECTING" | "CONNECTED" | "DISCONNECTED" | "STALE" | "STOPPED";
  lastMessageAt: string | null;
  reconnects: number;
}

export interface DashboardSnapshot {
  paperMode: true;
  generatedAt: string;
  engine: { ready: boolean; draining: boolean; uptimeSeconds: number; queueDepth: number; experimentId: string | null; experimentStatus: string | null };
  balance: LedgerState;
  positions: Position[];
  latestAgents: Partial<Record<ExternalAgentRoleId, GrokSignalEnvelope>>;
  consensus: ConsensusDecision | null;
  risk: RiskEvaluation | null;
  scanner: Array<{ marketId: string; outcome: string; bid: string | null; ask: string | null; spread: number | null; depthUsd: number; lastMessageAt: string }>;
  decisionFeed: DecisionFeedItem[];
  grokActivity: { acceptedSignals: number; rejectedSignals: number; duplicates: number; unauthorizedCalls: number; rateLimitedCalls: number; lastSeenAt: string | null; byAgent: Record<string, number> };
  recentTriggers: TriggerEvent[];
  health: { polymarketWs: ConnectionState; btcWs: ConnectionState; database: "UP" | "DOWN"; eventLatencyMs: number | null; lastBtcMessageAt: string | null };
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
    return {
      paperMode: true, generatedAt: new Date().toISOString(),
      engine: { ready: this.ready, draining: this.draining, uptimeSeconds: Math.floor((Date.now() - this.startedAt.getTime()) / 1000), queueDepth: this.queueDepth, experimentId: this.experimentId, experimentStatus: this.experimentStatus },
      balance: structuredClone(this.ledger), positions: structuredClone([...this.positions.values()]),
      latestAgents: Object.fromEntries(this.latestAgents), consensus: structuredClone(this.latestConsensus), risk: structuredClone(this.latestRisk),
      scanner: [...this.marketSnapshots.values()].map((snapshot) => ({
        marketId: snapshot.marketId, outcome: snapshot.outcome, bid: snapshot.bids[0]?.price ?? null,
        ask: snapshot.asks[0]?.price ?? null,
        spread: snapshot.bids[0] && snapshot.asks[0] ? Number(snapshot.asks[0].price) - Number(snapshot.bids[0].price) : null,
        depthUsd: [...snapshot.bids, ...snapshot.asks].reduce((sum, level) => sum + Number(level.price) * Number(level.quantity), 0),
        lastMessageAt: snapshot.receivedAt,
      })),
      decisionFeed: structuredClone(this.feed), grokActivity: structuredClone(this.grokActivity), recentTriggers: structuredClone(this.recentTriggers),
      health: { polymarketWs: structuredClone(this.connections.polymarket), btcWs: structuredClone(this.connections.btc), database: this.database, eventLatencyMs: this.eventLatencyMs, lastBtcMessageAt: this.lastBtcMessageAt },
      equityHistory: structuredClone(this.equityHistory),
    };
  }
}
