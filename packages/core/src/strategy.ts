import { d, decimal } from "./calculations.js";
import type { ConsensusDecision, MarketSnapshot, Outcome, Position } from "./domain.js";

/**
 * Safety cap for the initially unhedged leg. This is deliberately smaller than
 * maxOrderSizeUsd so a single failed hedge cannot consume the full per-order
 * risk budget. It applies only while opening a fresh directional leg; the
 * second-leg hedge may still use the full maxOrderSizeUsd to neutralize risk.
 */
export const DEFAULT_FIRST_LEG_RISK_CAP_USD = 5;

export interface StrategyCandidate {
  strategyId: string;
  strategyVersion: string;
  marketId: string;
  tokenId: string;
  outcome: Outcome;
  side: "BUY" | "SELL";
  quantity: string;
  limitPrice: string;
  intent: "OPEN_FIRST_LEG" | "HEDGE_SECOND_LEG" | "PROTECTIVE_DE_RISK";
  /** Raw expected value in USD/share for a $1 binary payout. */
  rawEdgeUsdPerShare?: number;
  /** @deprecated Compatibility alias for rawEdgeUsdPerShare. */
  rawExpectedEdge: number;
  reasonCodes: string[];
}

export interface StrategyContext {
  consensus: ConsensusDecision;
  snapshots: Record<Outcome, MarketSnapshot | null>;
  position: Position | null;
  maxOrderSizeUsd: number;
  /** Optional override used by tests/future explicit config. */
  firstLegRiskCapUsd?: number;
  /** Minimum locked pair value in USD/share for a $1 binary payout. */
  minimumLockedPairEdge: number;
}

export interface Strategy {
  readonly id: string;
  readonly version: string;
  evaluate(context: StrategyContext): StrategyCandidate | null;
}

export class TemporalPairHedgeStrategy implements Strategy {
  readonly id = "temporal-pair-hedge";
  constructor(readonly version = "temporal-pair-hedge-v1.2") {}

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
        intent: "HEDGE_SECOND_LEG", rawEdgeUsdPerShare: lockedEdge, rawExpectedEdge: lockedEdge,
        reasonCodes: ["UNPAIRED_INVENTORY", "LOCKABLE_PAIR_EDGE"],
      };
    }

    // Any unsettled position owns this market lifecycle. An unpaired position
    // may only hedge above; a fully paired position is absorbing until settlement.
    if (position) return null;

    if (context.consensus.consensusDirection === "FLAT") return null;
    const winnerProbability = context.consensus.fairProbability;
    // Fresh entry requires explicit winner-probability semantics. Historical
    // consensus rows may remain readable without this field, but they are not
    // executable first-leg authority.
    if (
      winnerProbability === undefined
      || !Number.isFinite(winnerProbability)
      || winnerProbability <= 0.5
    ) return null;
    const outcome: Outcome = context.consensus.consensusDirection;
    const snapshot = context.snapshots[outcome];
    const ask = snapshot?.asks[0];
    if (!snapshot || !ask) return null;
    const askPrice = Number(ask.price);
    const firstLegRiskCapUsd = Math.min(context.maxOrderSizeUsd, context.firstLegRiskCapUsd ?? DEFAULT_FIRST_LEG_RISK_CAP_USD);
    if (!Number.isFinite(askPrice) || askPrice <= 0 || !Number.isFinite(firstLegRiskCapUsd) || firstLegRiskCapUsd <= 0) return null;
    const quantity = decimal(firstLegRiskCapUsd / askPrice);
    return {
      strategyId: this.id, strategyVersion: this.version, marketId: snapshot.marketId,
      tokenId: snapshot.tokenId, outcome, side: "BUY", quantity, limitPrice: ask.price,
      intent: "OPEN_FIRST_LEG", rawEdgeUsdPerShare: context.consensus.estimatedEdge, rawExpectedEdge: context.consensus.estimatedEdge,
      reasonCodes: ["CONSENSUS_DIRECTION", "TEMPORAL_FIRST_LEG", "FIRST_LEG_RISK_CAPPED"],
    };
  }
}
