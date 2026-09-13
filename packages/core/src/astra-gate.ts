import type { AstraFeaturePacket } from "./astra.js";

export type AstraGateReason = "ACCEPTED" | "STALE" | "UNSYNCHRONIZED" | "INVALID_PROVENANCE" | "NO_LIQUIDITY" | "SPREAD_TOO_WIDE" | "COOLDOWN";

export interface AstraOpportunityGateInput {
  featurePacket: AstraFeaturePacket;
  now: Date;
  marketSynchronized: boolean;
  triggerProvenanceValid: boolean;
  referenceRequired?: boolean;
  minDepthUsd?: number;
  maxSpreadBps?: number;
  cooldownUntilMs?: number;
}

export interface AstraOpportunityGateResult {
  accepted: boolean;
  reason: AstraGateReason;
  deepReviewSuggested: boolean;
}

/** Pure, deterministic admission gate. It never chooses direction or price. */
export function evaluateAstraOpportunityGate(input: AstraOpportunityGateInput): AstraOpportunityGateResult {
  if (!input.marketSynchronized) return rejected("UNSYNCHRONIZED");
  if (!input.triggerProvenanceValid) return rejected("INVALID_PROVENANCE");
  if (Date.parse(input.featurePacket.expiresAt) <= input.now.getTime()) return rejected("STALE");
  if ((input.referenceRequired ?? input.featurePacket.referenceRequired) && input.featurePacket.referenceFreshness !== "fresh") return rejected("STALE");
  if (input.cooldownUntilMs !== undefined && input.now.getTime() < input.cooldownUntilMs) return rejected("COOLDOWN");
  const minDepth = input.minDepthUsd ?? 25;
  const maxSpread = input.maxSpreadBps ?? 1_200;
  if (Math.min(input.featurePacket.upDepthUsd, input.featurePacket.downDepthUsd) < minDepth) return rejected("NO_LIQUIDITY");
  if (Math.max(input.featurePacket.upSpreadBps, input.featurePacket.downSpreadBps) > maxSpread) return rejected("SPREAD_TOO_WIDE");
  return { accepted: true, reason: "ACCEPTED", deepReviewSuggested: input.featurePacket.regime === "VOLATILE" || input.featurePacket.regime === "DISLOCATED" };
}

function rejected(reason: Exclude<AstraGateReason, "ACCEPTED">): AstraOpportunityGateResult {
  return { accepted: false, reason, deepReviewSuggested: false };
}
