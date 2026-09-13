import { z } from "zod";

export const astraMemoryEntrySchema = z.object({
  id: z.string().min(1).max(160),
  marketId: z.string().min(1).max(160).optional(),
  asset: z.string().min(1).max(32).optional(),
  regime: z.enum(["CALM", "TRENDING", "VOLATILE", "DISLOCATED", "UNKNOWN"]).optional(),
  kind: z.enum(["DECISION", "SETTLEMENT", "LESSON"]),
  createdAt: z.string().datetime({ offset: true }),
  expiresAt: z.string().datetime({ offset: true }).nullable(),
  summary: z.string().min(1).max(512),
  features: z.record(z.string(), z.number().finite()).default({}),
  outcome: z.enum(["WIN", "LOSS", "BREAKEVEN", "OPEN"]).optional(),
}).strict();
export type AstraMemoryEntry = z.infer<typeof astraMemoryEntrySchema>;

export const astraMemoryStateSchema = z.object({
  entries: z.array(astraMemoryEntrySchema),
  lessons: z.array(astraMemoryEntrySchema),
}).strict();
export type AstraMemoryState = z.infer<typeof astraMemoryStateSchema>;

export class AstraMemoryStore {
  private entries: AstraMemoryEntry[] = [];
  private lessons: AstraMemoryEntry[] = [];
  constructor(private readonly limits: { maxEntries: number; maxLessons: number; maxBytes: number }) {}
  add(entry: AstraMemoryEntry): void {
    const parsed = astraMemoryEntrySchema.parse(entry);
    const target = parsed.kind === "LESSON" ? this.lessons : this.entries;
    target.push(parsed);
    this.trim();
  }
  relevant(limit = 8, match?: { marketId?: string | null; asset?: string | null; regime?: string | null }): AstraMemoryEntry[] {
    const boundedLimit = Math.max(0, Math.min(limit, 32));
    if (!match) return [...this.entries, ...this.lessons].slice(-boundedLimit).map((entry) => structuredClone(entry));
    const matching = [...this.entries, ...this.lessons].filter((entry) => {
      const marketMatch = !match.marketId || !entry.marketId || entry.marketId === match.marketId;
      const assetMatch = !match.asset || !entry.asset || entry.asset === match.asset;
      const regimeMatch = !match.regime || !entry.regime || entry.regime === match.regime;
      return marketMatch && assetMatch && regimeMatch;
    });
    return matching.slice(-boundedLimit).map((entry) => structuredClone(entry));
  }
  latestDecisionForMarket(marketId: string): AstraMemoryEntry | null {
    const entry = [...this.entries].reverse().find((item) => item.kind === "DECISION" && item.marketId === marketId);
    return entry ? structuredClone(entry) : null;
  }
  snapshot(): AstraMemoryState { return { entries: structuredClone(this.entries), lessons: structuredClone(this.lessons) }; }
  restore(state: AstraMemoryState): void {
    const parsed = astraMemoryStateSchema.parse(state);
    this.entries = parsed.entries.map((entry) => structuredClone(entry));
    this.lessons = parsed.lessons.map((entry) => structuredClone(entry));
    this.trim();
  }
  counts(): { entries: number; lessons: number; bytes: number } { return { entries: this.entries.length, lessons: this.lessons.length, bytes: Buffer.byteLength(JSON.stringify(this.snapshot())) }; }
  private trim(): void {
    while (this.entries.length > this.limits.maxEntries) this.entries.shift();
    while (this.lessons.length > this.limits.maxLessons) this.lessons.shift();
    while (Buffer.byteLength(JSON.stringify(this.snapshot())) > this.limits.maxBytes && (this.entries.length > 0 || this.lessons.length > 0)) {
      if (this.entries.length > 0) this.entries.shift(); else this.lessons.shift();
    }
  }
}

export const astraBudgetStateSchema = z.object({
  day: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).or(z.literal("")),
  spentUsd: z.number().finite().nonnegative(),
  calls: z.number().int().nonnegative(),
}).strict();
export type AstraBudgetState = z.infer<typeof astraBudgetStateSchema>;

export class AstraDailyBudget {
  private state: AstraBudgetState = { day: "", spentUsd: 0, calls: 0 };
  constructor(private readonly maxUsd: number) {}
  canSpend(amountUsd: number, now: Date): boolean { this.roll(now); return Number.isFinite(amountUsd) && amountUsd >= 0 && this.state.spentUsd + amountUsd <= this.maxUsd + 1e-9; }
  spend(amountUsd: number, now: Date): boolean { if (!this.canSpend(amountUsd, now)) return false; this.state.spentUsd += amountUsd; this.state.calls += 1; return true; }
  reserve(amountUsd: number, now: Date): boolean { return this.spend(amountUsd, now); }
  settle(reservedUsd: number, actualUsd: number, now: Date): void {
    this.roll(now);
    const refund = Math.max(0, reservedUsd - Math.max(0, actualUsd));
    this.state.spentUsd = Math.max(0, this.state.spentUsd - refund);
  }
  snapshot(now = new Date()): AstraBudgetState { this.roll(now); return { ...this.state }; }
  restore(state: AstraBudgetState): void { this.state = astraBudgetStateSchema.parse(state); }
  private roll(now: Date): void { const day = now.toISOString().slice(0, 10); if (this.state.day !== day) this.state = { day, spentUsd: 0, calls: 0 }; }
}
