import type { ExternalAgentRoleId } from "@pm/core";

export const SCOUT_ROLES: readonly ExternalAgentRoleId[] = [
  "market_microstructure_scanner",
  "crypto_market_scanner",
  "news_x_intelligence_scanner",
];

export type HotResearchEntry = {
  key: string;
  role: ExternalAgentRoleId;
  marketId: string;
  asset: string | null;
  triggerId: string;
  traceId: string;
  generatedAt: string;
  expiresAt: string;
  evidenceIds: string[];
  sourceEventIds: string[];
  featureIds: string[];
  derivedFrom: string[];
  marketSequence: number | null;
  referenceTimestamp: string | null;
  output: Record<string, unknown>;
};

export type HotResearchPolicy = {
  maxEntries?: number;
  roleTtlMs?: Partial<Record<ExternalAgentRoleId, number>>;
  now?: () => Date;
};

const DEFAULT_TTL_MS: Record<ExternalAgentRoleId, number> = {
  market_microstructure_scanner: 5_000,
  crypto_market_scanner: 15_000,
  news_x_intelligence_scanner: 60_000,
  fair_probability_analyst: 5_000,
  risk_hedge_analyst: 30_000,
  strategy_supervisor: 15 * 60_000,
};

/** Bounded process-local research working set. It never writes to the journal. */
export class HotResearchCache {
  private readonly entries = new Map<string, HotResearchEntry>();
  private readonly maxEntries: number;
  private readonly roleTtlMs: Record<ExternalAgentRoleId, number>;
  private readonly now: () => Date;

  constructor(policy: HotResearchPolicy = {}) {
    this.maxEntries = Math.max(1, Math.min(policy.maxEntries ?? 128, 2_048));
    this.roleTtlMs = { ...DEFAULT_TTL_MS, ...(policy.roleTtlMs ?? {}) };
    this.now = policy.now ?? (() => new Date());
  }

  put(input: Omit<HotResearchEntry, "key">): HotResearchEntry {
    this.prune();
    const key = HotResearchCache.key(input.marketId, input.asset, input.role);
    const generatedMs = new Date(input.generatedAt).getTime();
    const configuredExpiry = new Date(input.expiresAt).getTime();
    const ttlExpiry = Number.isFinite(generatedMs)
      ? generatedMs + Math.max(1, this.roleTtlMs[input.role] ?? DEFAULT_TTL_MS[input.role])
      : this.now().getTime();
    const expiresAt = new Date(Math.min(configuredExpiry, ttlExpiry)).toISOString();
    const entry: HotResearchEntry = {
      ...structuredClone(input),
      key,
      expiresAt,
      evidenceIds: [...new Set(input.evidenceIds)].slice(0, 32),
      sourceEventIds: [...new Set(input.sourceEventIds)].slice(0, 32),
      featureIds: [...new Set(input.featureIds)].slice(0, 32),
      derivedFrom: [...new Set(input.derivedFrom)].slice(0, 32),
    };
    this.entries.delete(key);
    this.entries.set(key, entry);
    while (this.entries.size > this.maxEntries) this.entries.delete(this.entries.keys().next().value!);
    return structuredClone(entry);
  }

  get(marketId: string, asset: string | null, role: ExternalAgentRoleId): HotResearchEntry | null {
    this.prune();
    const entry = this.entries.get(HotResearchCache.key(marketId, asset, role));
    return entry ? structuredClone(entry) : null;
  }

  freshFor(marketId: string, asset: string | null, roles: readonly ExternalAgentRoleId[]): HotResearchEntry[] {
    this.prune();
    return roles.flatMap((role) => {
      const entry = this.entries.get(HotResearchCache.key(marketId, asset, role));
      return entry ? [structuredClone(entry)] : [];
    });
  }

  /** A role becomes refresh-due before its usable TTL expires. */
  refreshDueFor(marketId: string, asset: string | null, roles: readonly ExternalAgentRoleId[]): ExternalAgentRoleId[] {
    this.prune();
    const nowMs = this.now().getTime();
    return roles.filter((role) => {
      const entry = this.entries.get(HotResearchCache.key(marketId, asset, role));
      if (!entry) return true;
      const generatedMs = new Date(entry.generatedAt).getTime();
      const expiresMs = new Date(entry.expiresAt).getTime();
      if (!Number.isFinite(generatedMs) || !Number.isFinite(expiresMs)) return true;
      const ttlMs = Math.max(1, expiresMs - generatedMs);
      const headroomMs = Math.min(2_000, Math.max(250, Math.floor(ttlMs / 3)));
      return nowMs >= expiresMs - headroomMs;
    });
  }

  snapshot(): HotResearchEntry[] {
    this.prune();
    return [...this.entries.values()].map((entry) => structuredClone(entry));
  }

  footprint(): { entries: number; bytes: number } {
    const snapshot = this.snapshot();
    return { entries: snapshot.length, bytes: Buffer.byteLength(JSON.stringify(snapshot), "utf8") };
  }

  clear(): void { this.entries.clear(); }

  static key(marketId: string, asset: string | null, role: ExternalAgentRoleId): string {
    return `${marketId}|${asset ?? "none"}|${role}`;
  }

  private prune(): void {
    const now = this.now().getTime();
    for (const [key, entry] of this.entries) {
      if (new Date(entry.expiresAt).getTime() <= now) this.entries.delete(key);
    }
  }
}

export function isScoutRole(role: ExternalAgentRoleId): boolean {
  return (SCOUT_ROLES as readonly string[]).includes(role);
}
