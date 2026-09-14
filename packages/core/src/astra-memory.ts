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
  constructor(
    private readonly limits: { maxEntries: number; maxLessons: number; maxBytes: number },
    private readonly nowFn: () => Date = () => new Date(),
  ) {}
  add(entry: AstraMemoryEntry): void {
    const parsed = astraMemoryEntrySchema.parse(entry);
    const target = parsed.kind === "LESSON" ? this.lessons : this.entries;
    target.push(parsed);
    this.trim();
  }
  relevant(limit = 8, match?: { marketId?: string | null; asset?: string | null; regime?: string | null }): AstraMemoryEntry[] {
    const boundedLimit = Math.max(0, Math.min(limit, 32));
    const nowMs = this.nowFn().getTime();
    const active = [...this.entries, ...this.lessons].filter((entry) => entry.expiresAt === null || new Date(entry.expiresAt).getTime() > nowMs);
    if (!match) return active.slice(-boundedLimit).map((entry) => structuredClone(entry));
    const matching = active.filter((entry) => {
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
  snapshot(): AstraMemoryState {
    return { entries: structuredClone(this.entries), lessons: structuredClone(this.lessons) };
  }
  restore(state: AstraMemoryState): void {
    const parsed = astraMemoryStateSchema.parse(state);
    this.entries = parsed.entries.map((entry) => structuredClone(entry));
    this.lessons = parsed.lessons.map((entry) => structuredClone(entry));
    this.trim();
  }
  counts(): { entries: number; lessons: number; bytes: number } {
    return {
      entries: this.entries.length,
      lessons: this.lessons.length,
      bytes: Buffer.byteLength(JSON.stringify(this.snapshot())),
    };
  }
  private trim(): void {
    while (this.entries.length > this.limits.maxEntries) this.entries.shift();
    while (this.lessons.length > this.limits.maxLessons) this.lessons.shift();
    while (Buffer.byteLength(JSON.stringify(this.snapshot())) > this.limits.maxBytes && (this.entries.length > 0 || this.lessons.length > 0)) {
      if (this.entries.length > 0) this.entries.shift();
      else this.lessons.shift();
    }
  }
}
