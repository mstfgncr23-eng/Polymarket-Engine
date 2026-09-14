import type { TriggerKind } from "./domain.js";

/**
 * Paid first-leg admission is deliberately an allowlist.  A trigger being
 * useful to the deterministic runtime does not imply that it is worth buying
 * provider inference for it.
 */
export type AstraFirstLegCandidateReason =
  | "DIRECT_ECONOMIC_CANDIDATE"
  | "STRONG_INFORMATIONAL_CANDIDATE"
  | "NON_SPECULATIVE_TRIGGER"
  | "SEVERITY_BELOW_THRESHOLD"
  | "UNCLASSIFIED_TRIGGER";

export interface AstraFirstLegTriggerClassification {
  eligible: boolean;
  reason: AstraFirstLegCandidateReason;
}

const DIRECT_ECONOMIC_KINDS: ReadonlySet<TriggerKind> = new Set([
  "PAIR_COST_OPPORTUNITY",
  "REPRICING_LAG",
  "EDGE_CHANGE",
]);

const STRONG_INFORMATIONAL_KINDS: ReadonlySet<TriggerKind> = new Set([
  "BOOK_IMBALANCE",
  "SPREAD_CHANGE",
  "DEPTH_CHANGE",
  "BTC_RAPID_MOVE",
  "BTC_VOLATILITY_SPIKE",
  "BTC_VOLUME_SPIKE",
  "CRYPTO_REFERENCE_RAPID_MOVE",
  "CRYPTO_REFERENCE_VOLATILITY_SPIKE",
  "CRYPTO_REFERENCE_VOLUME_SPIKE",
  "NEWS_INFORMATION",
  "REGIME_ESCALATION",
]);

const NON_SPECULATIVE_KINDS: ReadonlySet<TriggerKind> = new Set([
  "NEARING_EXPIRY",
  "FAILED_HEDGE",
  "DIRECTIONAL_EXPOSURE",
  "LOSS_CLUSTER",
  "SEVERE_DISAGREEMENT",
  "SCHEDULED_REVIEW",
]);

export function classifyAstraFirstLegTrigger(input: { kind: string; severity: number }): AstraFirstLegTriggerClassification {
  if (DIRECT_ECONOMIC_KINDS.has(input.kind as TriggerKind)) {
    return { eligible: true, reason: "DIRECT_ECONOMIC_CANDIDATE" };
  }
  if (STRONG_INFORMATIONAL_KINDS.has(input.kind as TriggerKind)) {
    return input.severity >= 2
      ? { eligible: true, reason: "STRONG_INFORMATIONAL_CANDIDATE" }
      : { eligible: false, reason: "SEVERITY_BELOW_THRESHOLD" };
  }
  if (NON_SPECULATIVE_KINDS.has(input.kind as TriggerKind)) {
    return { eligible: false, reason: "NON_SPECULATIVE_TRIGGER" };
  }
  return { eligible: false, reason: "UNCLASSIFIED_TRIGGER" };
}
