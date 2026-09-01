import { d, decimal } from "./calculations.js";
import type { ConsensusDecision, MarketSnapshot, Outcome, Position } from "./domain.js";

export interface StrategyCandidate {
  strategyId: string;
  strategyVersion: string;
  marketId: string;
  tokenId: string;
  outcome: Outcome;
  side: "BUY";
  quantity: string;
  limitPrice: string;
  intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG";
  rawExpectedEdge: number;
  reasonCodes: string[];
}

export interface StrategyContext {
  consensus: ConsensusDecision;
  snapshots: Record<Outcome, MarketSnapshot | null>;
  position: Position | null;
  maxOrderSizeUsd: number;
  minimumLockedPairEdge: number;
}

export interface Strategy {
  readonly id: string;
  readonly version: string;
  evaluate(context: StrategyContext): StrategyCandidate | null;
}

export class TemporalPairHedgeStrategy implements Strategy {
  readonly id = "temporal-pair-hedge";
  constructor(readonly version = "temporal-pair-hedge-v1") {}

  evaluate(context: StrategyContext): StrategyCandidate | null {
    const position = context.position;
    if (position?.unpairedOutcome) {
      const hedgeOutcome: Outcome = position.unpairedOutcome === "UP" ? "DOWN" : "UP";
      const snapshot = context.snapshots[hedgeOutcome];
      const ask = snapshot?.asks[0];
      if (!snapshot || !ask) return null;
      const firstAverage = position.unpairedOutcome === "UP" ? position.averageUpCost : position.averageDownCost;
      const pairCost = d(firstAverage).plus(ask.price);
      const lockedEdge = d(1).minus(pairCost).toNumber();
      if (lockedEdge < context.minimumLockedPairEdge) return null;
      const quantity = decimal(Math.min(Number(position.unpairedQuantity), context.maxOrderSizeUsd / Number(ask.price)));
      if (d(quantity).lte(0)) return null;
      return {
        strategyId: this.id, strategyVersion: this.version, marketId: snapshot.marketId,
        tokenId: snapshot.tokenId, outcome: hedgeOutcome, side: "BUY", quantity, limitPrice: ask.price,
        intent: "HEDGE_SECOND_LEG", rawExpectedEdge: lockedEdge,
        reasonCodes: ["UNPAIRED_INVENTORY", "LOCKABLE_PAIR_EDGE"],
      };
    }

    if (context.consensus.consensusDirection === "FLAT") return null;
    const outcome: Outcome = context.consensus.consensusDirection;
    const snapshot = context.snapshots[outcome];
    const ask = snapshot?.asks[0];
    if (!snapshot || !ask) return null;
    const quantity = decimal(context.maxOrderSizeUsd / Number(ask.price));
    return {
      strategyId: this.id, strategyVersion: this.version, marketId: snapshot.marketId,
      tokenId: snapshot.tokenId, outcome, side: "BUY", quantity, limitPrice: ask.price,
      intent: "OPEN_FIRST_LEG", rawExpectedEdge: context.consensus.estimatedEdge,
      reasonCodes: ["CONSENSUS_DIRECTION", "TEMPORAL_FIRST_LEG"],
    };
  }
}
