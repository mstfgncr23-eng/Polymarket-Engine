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

export function triggerDedupeScopeKey(
  marketId: string | null,
  asset: string | null,
  kind: string,
): string {
  return `${marketId ?? "none"}:${asset ?? "none"}:${kind}`;
}
