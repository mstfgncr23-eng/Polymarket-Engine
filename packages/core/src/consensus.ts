import { createId, type ConsensusDecision, type Direction } from "./domain.js";

export interface ConsensusSignal {
  agentId: string;
  direction: Direction;
  confidence: number;
  expiresAt: string;
  evidenceIds: string[];
}

export interface ConsensusConfig {
  weights: Record<string, number>;
  reliability: Record<string, number>;
  minimumFreshSignals: number;
  conflictThreshold: number;
}

export function buildConsensus(input: {
  traceId: string;
  triggerId: string;
  signals: readonly ConsensusSignal[];
  deterministicEdge: number;
  now: Date;
  config: ConsensusConfig;
}): ConsensusDecision {
  const fresh = input.signals.filter((signal) => new Date(signal.expiresAt).getTime() > input.now.getTime());
  let signedScore = 0;
  let totalWeight = 0;
  let agreeingWeight = 0;
  const conflicts: string[] = [];
  const directions = new Set(fresh.filter((s) => s.direction !== "FLAT").map((s) => s.direction));
  if (directions.size > 1) conflicts.push("DIRECTION_DISAGREEMENT");
  if (fresh.length < input.config.minimumFreshSignals) conflicts.push("INSUFFICIENT_FRESH_SIGNALS");

  for (const signal of fresh) {
    const configured = input.config.weights[signal.agentId] ?? 0;
    const reliability = input.config.reliability[signal.agentId] ?? 0.5;
    const weight = configured * reliability * signal.confidence;
    const sign = signal.direction === "UP" ? 1 : signal.direction === "DOWN" ? -1 : 0;
    signedScore += sign * weight;
    totalWeight += weight;
  }
  const normalized = totalWeight === 0 ? 0 : signedScore / totalWeight;
  const direction: Direction = normalized > input.config.conflictThreshold ? "UP"
    : normalized < -input.config.conflictThreshold ? "DOWN" : "FLAT";
  for (const signal of fresh) if (signal.direction === direction) {
    agreeingWeight += (input.config.weights[signal.agentId] ?? 0) * (input.config.reliability[signal.agentId] ?? 0.5) * signal.confidence;
  }
  const freshness = input.signals.length === 0 ? 0 : fresh.length / input.signals.length;
  const agreement = totalWeight === 0 ? 0 : agreeingWeight / totalWeight;
  return {
    id: createId("consensus"), traceId: input.traceId, triggerId: input.triggerId,
    consensusDirection: direction, consensusConfidence: Math.min(1, Math.abs(normalized)),
    agentAgreement: Math.min(1, agreement), signalFreshness: Math.min(1, freshness), conflicts,
    estimatedEdge: input.deterministicEdge,
    evidenceIds: [...new Set(fresh.flatMap((signal) => signal.evidenceIds))],
    calculatedAt: input.now.toISOString(),
  };
}

export const defaultConsensusConfig: ConsensusConfig = {
  weights: {
    market_microstructure_scanner: 0.2,
    crypto_market_scanner: 0.15,
    news_x_intelligence_scanner: 0.1,
    fair_probability_analyst: 0.4,
    risk_hedge_analyst: 0.15,
    strategy_supervisor: 0,
  },
  reliability: {
    market_microstructure_scanner: 0.5,
    crypto_market_scanner: 0.5,
    news_x_intelligence_scanner: 0.5,
    fair_probability_analyst: 0.5,
    risk_hedge_analyst: 0.5,
    strategy_supervisor: 0.5,
  },
  minimumFreshSignals: 2,
  conflictThreshold: 0.15,
};
