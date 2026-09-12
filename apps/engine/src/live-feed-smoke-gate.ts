const BLOCKING_REFERENCE_STATUSES = new Set(["TRANSIENT_FAILURE", "PROVIDER_BLOCKED", "REGION_BLOCKED"]);

export interface LiveFeedSmokeGateInput {
  polymarketMessages: number;
  referenceMessages: Readonly<Record<string, number>>;
  referenceStatuses: readonly string[];
}

export interface LiveFeedSmokeGateStatus {
  referenceMessageCount: number;
  referenceExpected: boolean;
  referenceResolutionBlocked: boolean;
  settled: boolean;
  ok: boolean;
}

/** Pure completion gate for the live feed diagnostic. */
export function evaluateLiveFeedSmokeGate(input: LiveFeedSmokeGateInput): LiveFeedSmokeGateStatus {
  const referenceMessageCount = Object.values(input.referenceMessages).reduce((sum, value) => sum + value, 0);
  const referenceExpected = Object.keys(input.referenceMessages).length > 0;
  const referenceResolutionBlocked = input.referenceStatuses.some((status) => BLOCKING_REFERENCE_STATUSES.has(status));
  const referenceSatisfied = !referenceExpected || referenceMessageCount > 0;
  const ok = input.polymarketMessages > 0 && !referenceResolutionBlocked && referenceSatisfied;
  const settled = ok || (input.polymarketMessages > 0 && referenceResolutionBlocked);
  return { referenceMessageCount, referenceExpected, referenceResolutionBlocked, settled, ok };
}
