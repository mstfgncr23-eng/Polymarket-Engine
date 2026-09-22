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

/**
 * Why a protective de-risk candidate could not be built from the current
 * position and live book. Diagnostic classification only — it never loosens
 * the protective SELL bounds.
 *
 * - NO_EXECUTABLE_BID_DEPTH: the unpaired token book is absent for the position
 *   outcome, or the walk over executable bid levels produced zero quantity.
 * - MIN_ORDER_SIZE_UNKNOWN: the authoritative minimum order size is not known
 *   for this token. The CLOB full-book event carries `min_order_size`; until
 *   the adapter has committed one such full book (and carried the value
 *   forward), the protective SELL fails safely rather than inventing a size.
 * - QUANTITY_BELOW_MIN_ORDER_SIZE: executable bid quantity exists but is below
 *   the known minimum order size for the token.
 */
export type ProtectiveCandidateRejection =
  | "NO_EXECUTABLE_BID_DEPTH"
  | "MIN_ORDER_SIZE_UNKNOWN"
  | "QUANTITY_BELOW_MIN_ORDER_SIZE";

export interface ProtectiveCandidateEvaluation {
  candidate: StrategyCandidate | null;
  /** Non-null exactly when candidate is null on the executable-quantity paths. */
  reason: ProtectiveCandidateRejection | null;
}

export function evaluateProtectiveDeRiskCandidate(input: {
  position: Position;
  snapshot: MarketSnapshot;
  maxOrderSizeUsd: number;
}): ProtectiveCandidateEvaluation {
  const { position, snapshot } = input;
  if (!position.unpairedOutcome || position.unpairedOutcome !== snapshot.outcome) return { candidate: null, reason: null };
  const unpaired = d(position.unpairedQuantity);
  const maxNotional = d(input.maxOrderSizeUsd);
  if (unpaired.lte(0) || maxNotional.lte(0) || snapshot.bids.length === 0) {
    return { candidate: null, reason: "NO_EXECUTABLE_BID_DEPTH" };
  }

  let quantity = d(0);
  let notional = d(0);
  let worstPrice: string | null = null;
  for (const level of snapshot.bids) {
    const price = d(level.price);
    const available = d(level.quantity);
    if (price.lte(0) || available.lte(0)) continue;
    const remainingInventory = unpaired.minus(quantity);
    const remainingNotional = maxNotional.minus(notional);
    if (remainingInventory.lte(0) || remainingNotional.lte(0)) break;
    const capQuantity = remainingNotional.div(price);
    let take = available;
    if (take.gt(remainingInventory)) take = remainingInventory;
    if (take.gt(capQuantity)) take = capQuantity;
    if (take.lte(0)) break;
    quantity = quantity.plus(take);
    notional = notional.plus(take.mul(price));
    worstPrice = level.price;
  }

  if (quantity.lte(0) || !worstPrice) return { candidate: null, reason: "NO_EXECUTABLE_BID_DEPTH" };
  if (!snapshot.minOrderSize) return { candidate: null, reason: "MIN_ORDER_SIZE_UNKNOWN" };
  if (quantity.lt(snapshot.minOrderSize)) return { candidate: null, reason: "QUANTITY_BELOW_MIN_ORDER_SIZE" };
  return {
    candidate: {
      strategyId: "protective-de-risk",
      strategyVersion: "protective-de-risk-v1",
      marketId: snapshot.marketId,
      tokenId: snapshot.tokenId,
      outcome: snapshot.outcome,
      side: "SELL",
      quantity: decimal(quantity),
      limitPrice: worstPrice,
      intent: "PROTECTIVE_DE_RISK",
      rawEdgeUsdPerShare: 0,
      rawExpectedEdge: 0,
      reasonCodes: ["MAX_UNHEDGED_DURATION", "REDUCE_UNPAIRED_INVENTORY"],
    },
    reason: null,
  };
}

export function buildProtectiveDeRiskCandidate(input: {
  position: Position;
  snapshot: MarketSnapshot;
  maxOrderSizeUsd: number;
}): StrategyCandidate | null {
  return evaluateProtectiveDeRiskCandidate(input).candidate;
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
