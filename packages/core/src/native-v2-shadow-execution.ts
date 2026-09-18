import { decimal } from "./calculations.js";
import type { ConsensusDecision, MarketSnapshot } from "./domain.js";
import type { NativeV2ShadowFairProbability } from "./native-v2-shadow-fair-probability.js";
import type { NativeV2ShadowOpportunityCandidate } from "./native-v2-shadow-opportunity-ranking.js";
import type { StrategyCandidate } from "./strategy.js";

export const NATIVE_V2_SHADOW_EXECUTION_VERSION = "native-v2-shadow-execution-v1" as const;

export interface NativeV2ShadowExecutionIntent {
  version: typeof NATIVE_V2_SHADOW_EXECUTION_VERSION;
  confidence: number;
  evidenceCoverage: number;
  consensus: ConsensusDecision;
  candidate: StrategyCandidate;
}

/**
 * Conservative confidence proxy for Phase-4 shadow risk evaluation.
 *
 * It is intentionally NOT presented as a calibrated probability. Confidence
 * starts at 0.5, rises only when directional evidence is strong, and is
 * discounted when fewer than the three V2 evidence families are available.
 */
export function nativeV2ShadowExecutionConfidence(fair: NativeV2ShadowFairProbability): {
  confidence: number;
  evidenceCoverage: number;
} {
  const evidenceCoverage = clamp(fair.evidenceFamilyCount / 3, 0, 1);
  const directionalStrength = clamp(Math.abs(fair.directionalEvidenceScore), 0, 1);
  return {
    evidenceCoverage,
    confidence: clamp(0.5 + 0.5 * directionalStrength * evidenceCoverage, 0, 1),
  };
}

/**
 * Convert a CURRENT-book V2 opportunity into the same candidate + consensus
 * contracts consumed by the deterministic paper risk/execution evaluator.
 *
 * This function has no I/O and performs no ledger/order mutation. The caller
 * must re-read the current snapshot before invoking it.
 */
export function buildNativeV2ShadowExecutionIntent(input: {
  triggerId: string;
  traceId: string;
  expiresAt: string;
  evidenceIds: readonly string[];
  fair: NativeV2ShadowFairProbability;
  selected: NativeV2ShadowOpportunityCandidate;
  snapshot: MarketSnapshot;
  now: Date;
}): NativeV2ShadowExecutionIntent | null {
  if (input.snapshot.marketId !== input.selected.marketId) return null;
  if (input.snapshot.outcome !== input.selected.outcome) return null;
  if (!Number.isFinite(input.now.getTime()) || input.now.getTime() >= Date.parse(input.expiresAt)) return null;
  if (!Number.isFinite(input.selected.fairProbability) || input.selected.fairProbability < 0 || input.selected.fairProbability > 1) return null;
  const winnerOutcome = input.fair.fairProbabilityUp > input.fair.fairProbabilityDown
    ? "UP"
    : input.fair.fairProbabilityDown > input.fair.fairProbabilityUp ? "DOWN" : null;
  if (!winnerOutcome || input.selected.outcome !== winnerOutcome || !input.selected.actionable) return null;
  const winnerProbability = winnerOutcome === "UP"
    ? input.fair.fairProbabilityUp
    : input.fair.fairProbabilityDown;
  if (
    winnerProbability <= 0.5
    || Math.abs(input.selected.fairProbability - winnerProbability) > 1e-9
  ) return null;
  if (!Number.isFinite(input.selected.executableVwap) || input.selected.executableVwap <= 0 || input.selected.executableVwap > 1) return null;
  if (!Number.isFinite(input.selected.worstPrice) || input.selected.worstPrice <= 0 || input.selected.worstPrice > 1) return null;

  const { confidence, evidenceCoverage } = nativeV2ShadowExecutionConfidence(input.fair);
  const evidenceIds = [...new Set(input.evidenceIds)];
  const consensus: ConsensusDecision = {
    id: `native_v2_shadow_consensus:${input.triggerId}`,
    traceId: input.traceId,
    triggerId: input.triggerId,
    consensusDirection: input.selected.outcome,
    consensusConfidence: confidence,
    agentAgreement: evidenceCoverage,
    signalFreshness: 1,
    conflicts: [],
    estimatedEdge: clamp(input.selected.grossEdgeUsdPerShare, -1, 1),
    evidenceIds,
    calculatedAt: input.now.toISOString(),
    fairProbability: input.selected.fairProbability,
    marketProbability: input.selected.executableVwap,
    featureVector: {
      nativeV2ShadowExecutionVersion: NATIVE_V2_SHADOW_EXECUTION_VERSION,
      fairProbabilityVersion: input.fair.version,
      calibrationStatus: input.fair.calibrationStatus,
      evidenceCoverage,
      directionalEvidenceScore: input.fair.directionalEvidenceScore,
    },
  };

  const candidate: StrategyCandidate = {
    strategyId: "native-v2-shadow-opportunity",
    strategyVersion: NATIVE_V2_SHADOW_EXECUTION_VERSION,
    marketId: input.selected.marketId,
    tokenId: input.snapshot.tokenId,
    outcome: input.selected.outcome,
    side: "BUY",
    quantity: input.selected.quantity,
    // The selected opportunity already walked the current book. The worst
    // consumed price is the economically valid BUY limit for that exact walk.
    limitPrice: decimal(input.selected.worstPrice),
    intent: "OPEN_FIRST_LEG",
    rawEdgeUsdPerShare: input.selected.grossEdgeUsdPerShare,
    rawExpectedEdge: input.selected.grossEdgeUsdPerShare,
    reasonCodes: [
      "NATIVE_V2_SHADOW_GLOBAL_TOP",
      "CURRENT_BOOK_REVALIDATED",
      "FIRST_LEG_RISK_CAPPED",
    ],
  };

  return {
    version: NATIVE_V2_SHADOW_EXECUTION_VERSION,
    confidence,
    evidenceCoverage,
    consensus,
    candidate,
  };
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}
