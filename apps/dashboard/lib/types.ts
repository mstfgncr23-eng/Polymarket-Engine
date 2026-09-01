export interface DashboardSnapshot {
  paperMode: true;
  generatedAt: string;
  engine: {
    ready: boolean;
    draining: boolean;
    alive?: boolean;
    uptimeSeconds: number;
    queueDepth: number;
    experimentId: string | null;
    experimentStatus: string | null;
    discoveryStatus?: "ZERO_QUALIFIED" | "MONITORING" | "RECONCILING";
    discoveryLoopActive?: boolean;
    marketMonitoring?: boolean;
    qualifiedMarketCount?: number;
  };
  balance: { cash: string; reservedCash: string; realizedPnl: string; unrealizedPnl: string; grossPnl: string; fees: string; slippage: string; aiCost: string; infrastructureCost: string; netTradingPnl: string; netEconomicPnl: string; equity: string; drawdown: string; updatedAt: string };
  positions: Array<{ id: string; marketId: string; upQuantity: string; downQuantity: string; pairedQuantity: string; unpairedQuantity: string; unpairedOutcome: string | null; averageUpCost: string; averageDownCost: string; openedAt: string; updatedAt: string }>;
  latestAgents: Partial<Record<string, GrokAgentSignal>>;
  consensus: { consensusDirection: string; consensusConfidence: number; agentAgreement: number; signalFreshness: number; conflicts: string[]; estimatedEdge: number; calculatedAt: string } | null;
  risk: { accepted: boolean; vetoes: string[]; utilization: Record<string, number>; evaluatedAt: string } | null;
  scanner: Array<{ marketId: string; asset?: string | null; outcome: string; bid: string | null; ask: string | null; spread: number | null; depthUsd: number; lastMessageAt: string }>;
  decisionFeed: Array<{ id: string; at: string; type: string; traceId: string; marketId: string | null; asset?: string | null; detail: Record<string, unknown> }>;
  grokActivity: { acceptedSignals: number; rejectedSignals: number; duplicates: number; unauthorizedCalls: number; rateLimitedCalls: number; lastSeenAt: string | null; byAgent: Record<string, number> };
  recentTriggers: Array<{ id: string; kind: string; severity: number; marketId: string | null; asset?: string | null; createdAt: string; expiresAt: string }>;
  activations?: Array<{ triggerId: string; role?: string; provider: string; ok?: boolean; skipped?: boolean; errorCode?: string | null; reason?: string; grokWakeObserved: false; at: string }>;
  activationLatency?: Array<{ triggerId: string; triggerCreatedAt: string; activationRequestedAt: string | null; activationSentAt: string | null; firstMcpRequestAt: string | null; firstAcceptedSignalAt: string | null }>;
  unattributedMcp?: { lastAt: string | null; count: number; lastTool: string | null };
  discovery?: { qualified: Array<{ marketId: string; slug: string; asset: string | null; tokens: string[] }>; rejected: Array<{ id: string; slug: string; title: string; reasonCodes: string[] }> };
  health: {
    engineAlive?: boolean;
    discoveryStatus?: string;
    discoveryLoopActive?: boolean;
    marketMonitoring?: boolean;
    qualifiedMarketCount?: number;
    polymarketWs: Connection;
    btcWs: Connection;
    referenceFeeds?: Array<{ asset: string; state: string; lastMessageAt: string | null; reconnects: number }>;
    database: "UP" | "DOWN";
    eventLatencyMs: number | null;
    lastBtcMessageAt: string | null;
    unattributedMcp?: { lastAt: string | null; count: number; lastTool: string | null };
  };
  equityHistory: Array<{ at: string; equity: string; drawdown: string }>;
}

export interface GrokAgentSignal {
  signalId: string;
  agentId: string;
  role?: string;
  receivedAt: string;
  timestamp?: string;
  output: Record<string, unknown> | null;
  resultingDecision: string | null;
}

interface Connection { state: string; lastMessageAt: string | null; reconnects: number }