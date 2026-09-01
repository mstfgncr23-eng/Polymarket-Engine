import type { ConsensusDecision, Direction, MarketSnapshot, Outcome } from "./domain.js";
import { d, walkBook } from "./calculations.js";

export interface FairProbabilityEvidence {
  direction: Direction;
  fairProbability: number;
  marketProbability?: number;
  estimatedEdge?: number;
}

export interface ExecutableFairEdge {
  consensusOutcome: Outcome;
  fairProbabilityForConsensus: number;
  executablePrice: number;
  rawEdge: number;
  usedComplement: boolean;
  estimatedEdgeIgnored: number | null;
  bookMarketProbability: number;
}

/**
 * Recompute executable raw edge from fair probability, consensus outcome, and the
 * CURRENT same-market book. Grok estimatedEdge is diagnostic only.
 * fairProbability refers to the submitted fair direction. If that direction is the
 * binary opposite of consensus, use 1 - p (explicit complement). Never reuse an
 * edge number for the opposite outcome without that transform.
 */
export function executableFairEdge(input: {
  fair: FairProbabilityEvidence;
  consensus: Pick<ConsensusDecision, "consensusDirection">;
  snapshot: MarketSnapshot;
  quantity: string;
  limitPrice?: string;
}): ExecutableFairEdge | null {
  const consensus = input.consensus.consensusDirection;
  if (consensus !== "UP" && consensus !== "DOWN") return null;
  if (input.snapshot.outcome !== consensus) return null;
  let fairForConsensus = input.fair.fairProbability;
  let usedComplement = false;
  if (input.fair.direction === consensus) {
    fairForConsensus = input.fair.fairProbability;
  } else if ((input.fair.direction === "UP" || input.fair.direction === "DOWN") && input.fair.direction !== consensus) {
    fairForConsensus = 1 - input.fair.fairProbability;
    usedComplement = true;
  } else {
    return null;
  }
  const walk = walkBook(input.snapshot.asks, input.quantity, input.limitPrice ?? input.snapshot.asks[0]?.price);
  if (!walk.sufficientDepth) return null;
  const executablePrice = Number(walk.vwap);
  if (!Number.isFinite(executablePrice)) return null;
  return {
    consensusOutcome: consensus,
    fairProbabilityForConsensus: fairForConsensus,
    executablePrice,
    rawEdge: fairForConsensus - executablePrice,
    usedComplement,
    estimatedEdgeIgnored: typeof input.fair.estimatedEdge === "number" ? input.fair.estimatedEdge : null,
    bookMarketProbability: executablePrice,
  };
}

export function bookAskProbability(snapshot: MarketSnapshot): number | null {
  const ask = snapshot.asks[0]?.price;
  if (!ask) return null;
  const value = d(ask).toNumber();
  return Number.isFinite(value) ? value : null;
}
