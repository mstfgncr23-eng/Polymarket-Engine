export interface DashboardSnapshot {
  paperMode: true;
  generatedAt: string;
  engine: { ready: boolean; draining: boolean; uptimeSeconds: number; queueDepth: number; experimentId: string | null; experimentStatus: string | null };
  balance: { cash: string; reservedCash: string; realizedPnl: string; unrealizedPnl: string; grossPnl: string; fees: string; slippage: string; aiCost: string; infrastructureCost: string; netTradingPnl: string; netEconomicPnl: string; equity: string; drawdown: string; updatedAt: string };
  positions: Array<{ id: string; marketId: string; upQuantity: string; downQuantity: string; pairedQuantity: string; unpairedQuantity: string; unpairedOutcome: string | null; averageUpCost: string; averageDownCost: string; realizedPnl: string; unrealizedPnl: string; openedAt: string; updatedAt: string }>;
  latestAgents: Record<string, { agentId: string; model: string; durationMs: number; usage: { inputTokens: number; outputTokens: number; estimatedCostUsd: number | null }; output: Record<string, unknown> | null }>;
  consensus: { consensusDirection: string; consensusConfidence: number; agentAgreement: number; signalFreshness: number; conflicts: string[]; estimatedEdge: number; calculatedAt: string } | null;
  risk: { accepted: boolean; vetoes: string[]; utilization: Record<string, number>; evaluatedAt: string } | null;
  scanner: Array<{ marketId: string; outcome: string; bid: string | null; ask: string | null; spread: number | null; depthUsd: number; lastMessageAt: string }>;
  decisionFeed: Array<{ id: string; at: string; type: string; traceId: string; marketId: string | null; detail: Record<string, unknown> }>;
  aiUsage: { calls: number; inputTokens: number; outputTokens: number; costTodayUsd: number; costByAgent: Record<string, number> };
  health: { polymarketWs: Connection; btcWs: Connection; database: "UP" | "DOWN"; eventLatencyMs: number | null; lastBtcMessageAt: string | null };
  equityHistory: Array<{ at: string; equity: string; drawdown: string }>;
}

interface Connection { state: string; lastMessageAt: string | null; reconnects: number }
