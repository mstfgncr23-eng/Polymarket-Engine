import type { TriggerKind } from "./domain.js";

/** Finite architectural retention for trigger-owned structures. Not a CPU claim. */
export const TRIGGER_STABILITY_BOUNDS = {
  totalDepthCombinedLevels: 10,
  imbalanceLevels: 5,
  referenceWindowMs: 60_000,
  referenceWindowMaxEntries: 16_384,
  dedupeRetentionMs: 3_600_000,
  dedupeMaxEntries: 8_192,
} as const;

/**
 * Safety-critical kinds that MAY bypass cooldown on severity escalation.
 * SCHEDULED_REVIEW is P0 for admission but is a periodic review, not a
 * safety-escalation exception. Non-safety research never bypasses on score
 * or severity alone.
 */
export const SAFETY_ESCALATION_KINDS = [
  "FAILED_HEDGE",
  "DIRECTIONAL_EXPOSURE",
  "NEARING_EXPIRY",
] as const satisfies readonly TriggerKind[];

export type SafetyEscalationKind = (typeof SAFETY_ESCALATION_KINDS)[number];

const SAFETY_SET = new Set<string>(SAFETY_ESCALATION_KINDS);

export function isSafetyEscalationKind(kind: string): kind is SafetyEscalationKind {
  return SAFETY_SET.has(kind);
}

/** Safety-critical kinds MAY emit on a strict severity increase inside cooldown. */
export function safetyEscalationMayEmit(
  kind: string,
  previousSeverity: 1 | 2 | 3,
  nextSeverity: 1 | 2 | 3,
): boolean {
  return isSafetyEscalationKind(kind) && nextSeverity > previousSeverity;
}

export const MARKET_DATA_TRIGGER_KINDS = [
  "BOOK_IMBALANCE",
  "SPREAD_CHANGE",
  "DEPTH_CHANGE",
  "PAIR_COST_OPPORTUNITY",
  "REPRICING_LAG",
  "EDGE_CHANGE",
] as const satisfies readonly TriggerKind[];

/** Risk / system / settlement kinds that must still run after a market-data gap. */
export const NON_SUPPRESSIBLE_ON_MARKET_GAP_KINDS = [
  "DIRECTIONAL_EXPOSURE",
  "FAILED_HEDGE",
  "NEARING_EXPIRY",
  "SEVERE_DISAGREEMENT",
  "LOSS_CLUSTER",
  "SCHEDULED_REVIEW",
] as const satisfies readonly TriggerKind[];

const MARKET_DATA_SET = new Set<string>(MARKET_DATA_TRIGGER_KINDS);
const NON_SUPPRESSIBLE_SET = new Set<string>(NON_SUPPRESSIBLE_ON_MARKET_GAP_KINDS);

export function isMarketDataDerivedTrigger(trigger: { kind: string; source?: string | null }): boolean {
  if (NON_SUPPRESSIBLE_SET.has(trigger.kind)) return false;
  if (MARKET_DATA_SET.has(trigger.kind)) return true;
  return trigger.source === "polymarket";
}

/** Reference-feed research kinds. Volume/volatility variants are classified explicitly even if unused. */
export const REFERENCE_DATA_TRIGGER_KINDS = [
  "BTC_RAPID_MOVE",
  "BTC_VOLATILITY_SPIKE",
  "BTC_VOLUME_SPIKE",
  "CRYPTO_REFERENCE_RAPID_MOVE",
  "CRYPTO_REFERENCE_VOLATILITY_SPIKE",
  "CRYPTO_REFERENCE_VOLUME_SPIKE",
] as const satisfies readonly TriggerKind[];

const REFERENCE_DATA_SET = new Set<string>(REFERENCE_DATA_TRIGGER_KINDS);

export type TriggerProvenance =
  | { kind: "MARKET"; tokenEpochs: Readonly<Record<string, number>> }
  | { kind: "REFERENCE"; asset: string; epoch: number }
  | { kind: "NONE" };

export type TriggerProvenanceKind = TriggerProvenance["kind"];

/**
 * Classify from structured kind + evidence.because — never from `source` strings.
 * NEWS_INFORMATION inherits the because-kind's plane (BOOK_IMBALANCE → MARKET,
 * BTC_RAPID_MOVE → REFERENCE). Safety/risk/settlement and NEARING_EXPIRY are NONE.
 */
export function classifyTriggerProvenance(trigger: {
  kind: string;
  evidence?: Record<string, unknown> | null;
}): TriggerProvenanceKind {
  if (NON_SUPPRESSIBLE_SET.has(trigger.kind)) return "NONE";
  if (MARKET_DATA_SET.has(trigger.kind)) return "MARKET";
  if (REFERENCE_DATA_SET.has(trigger.kind)) return "REFERENCE";
  if (trigger.kind === "NEWS_INFORMATION") {
    const because = typeof trigger.evidence?.because === "string" ? trigger.evidence.because : "";
    if (MARKET_DATA_SET.has(because) || because === "BOOK_IMBALANCE") return "MARKET";
    if (REFERENCE_DATA_SET.has(because)) return "REFERENCE";
    return "NONE";
  }
  return "NONE";
}

export function triggerDedupeScopeKey(
  marketId: string | null,
  asset: string | null,
  kind: string,
): string {
  return `${marketId ?? "none"}:${asset ?? "none"}:${kind}`;
}
