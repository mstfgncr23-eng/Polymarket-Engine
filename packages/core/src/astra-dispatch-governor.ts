import { z } from "zod";

export const ASTRA_FAST_MAX_DISPATCHES_PER_HOUR = 12;
export const ASTRA_FAST_MAX_DISPATCHES_PER_HOUR_LIMIT = 10_000;
export const ASTRA_FAST_MAX_MARKETS = 256;
export const ASTRA_FAST_MIN_DISPATCH_INTERVAL_MS = 5_000;
export const ASTRA_FAST_MARKET_COOLDOWN_MS = 60_000;

export const astraDispatchGovernorStateSchema = z.object({
  dispatches: z.array(z.object({ atMs: z.number().int().nonnegative() })).max(ASTRA_FAST_MAX_DISPATCHES_PER_HOUR_LIMIT),
  lastDispatchAtMs: z.number().int().nonnegative().nullable(),
  marketCooldowns: z.array(z.object({ marketId: z.string().min(1).max(160), untilMs: z.number().int().nonnegative() })).max(256),
}).strict();
export type AstraDispatchGovernorState = z.infer<typeof astraDispatchGovernorStateSchema>;

export type AstraDispatchGovernorBlockReason =
  | "GLOBAL_MIN_INTERVAL"
  | "MARKET_COOLDOWN"
  | "ROLLING_HOUR_CAP";

export type AstraDispatchGovernorDecision =
  | { allowed: true; state: AstraDispatchGovernorState }
  | { allowed: false; reason: AstraDispatchGovernorBlockReason; state: AstraDispatchGovernorState };

/**
 * A bounded, deterministic FAST dispatch governor.  It is intentionally
 * independent of wall-clock calendar boundaries; recovery restores the small
 * evidence window so a restart cannot create a fresh burst.
 */
export class AstraDispatchGovernor {
  private dispatches: number[] = [];
  private lastDispatchAtMs: number | null = null;
  private readonly marketCooldowns = new Map<string, number>();

  constructor(private readonly options: {
    maxDispatchesPerHour?: number;
    minDispatchIntervalMs?: number;
    marketCooldownMs?: number;
    maxMarkets?: number;
  } = {}) {}

  tryAdmit(marketId: string, nowMs: number): AstraDispatchGovernorDecision {
    this.prune(nowMs);
    const state = this.snapshot(nowMs);
    if (this.lastDispatchAtMs != null && nowMs - this.lastDispatchAtMs < this.minIntervalMs()) {
      return { allowed: false, reason: "GLOBAL_MIN_INTERVAL", state };
    }
    const marketUntil = this.marketCooldowns.get(marketId) ?? 0;
    if (nowMs < marketUntil) return { allowed: false, reason: "MARKET_COOLDOWN", state };
    if (this.dispatches.length >= this.maxDispatches()) {
      return { allowed: false, reason: "ROLLING_HOUR_CAP", state };
    }
    return { allowed: true, state };
  }

  consume(marketId: string, nowMs: number): void {
    this.prune(nowMs);
    const decision = this.tryAdmit(marketId, nowMs);
    if (!decision.allowed) throw new Error(`ASTRA_DISPATCH_GOVERNOR:${decision.reason}`);
    this.dispatches.push(nowMs);
    this.lastDispatchAtMs = nowMs;
    this.marketCooldowns.set(marketId, nowMs + this.marketCooldownMs());
    this.trimMarkets();
  }

  snapshot(nowMs = Date.now()): AstraDispatchGovernorState {
    this.prune(nowMs);
    return {
      dispatches: this.dispatches.slice(-this.maxDispatches()).map((atMs) => ({ atMs })),
      lastDispatchAtMs: this.lastDispatchAtMs,
      marketCooldowns: [...this.marketCooldowns.entries()].map(([marketId, untilMs]) => ({ marketId, untilMs })).slice(-this.maxMarkets()),
    };
  }

  restore(state: AstraDispatchGovernorState, nowMs = Date.now()): void {
    const parsed = astraDispatchGovernorStateSchema.parse(state);
    this.dispatches = parsed.dispatches.map((item) => item.atMs).slice(-this.maxDispatches());
    this.lastDispatchAtMs = parsed.lastDispatchAtMs;
    this.marketCooldowns.clear();
    for (const item of parsed.marketCooldowns.slice(-this.maxMarkets())) this.marketCooldowns.set(item.marketId, item.untilMs);
    this.prune(nowMs);
  }

  /** Starts a new experiment scope without carrying the prior experiment's allowance. */
  reset(): void {
    this.dispatches = [];
    this.lastDispatchAtMs = null;
    this.marketCooldowns.clear();
  }

  /** Rebuilds the governor from bounded canonical dispatch-start evidence. */
  restoreFromEvidence(events: readonly { marketId?: unknown; occurredAt: string }[], nowMs = Date.now()): void {
    this.dispatches = [];
    this.lastDispatchAtMs = null;
    this.marketCooldowns.clear();
    const ordered = [...events]
      .filter((event) => typeof event.occurredAt === "string")
      .sort((a, b) => a.occurredAt.localeCompare(b.occurredAt))
      .slice(-this.maxDispatches());
    for (const event of ordered) {
      const atMs = Date.parse(event.occurredAt);
      if (!Number.isFinite(atMs)) continue;
      this.dispatches.push(atMs);
      this.lastDispatchAtMs = atMs;
      if (typeof event.marketId === "string" && event.marketId.length > 0) this.marketCooldowns.set(event.marketId, atMs + this.marketCooldownMs());
    }
    this.trimMarkets();
    this.prune(nowMs);
  }

  private maxDispatches(): number {
    return Math.min(ASTRA_FAST_MAX_DISPATCHES_PER_HOUR, Math.max(1, Math.floor(this.options.maxDispatchesPerHour ?? ASTRA_FAST_MAX_DISPATCHES_PER_HOUR)));
  }
  private minIntervalMs(): number { return Math.max(0, Math.floor(this.options.minDispatchIntervalMs ?? ASTRA_FAST_MIN_DISPATCH_INTERVAL_MS)); }
  private marketCooldownMs(): number { return Math.max(0, Math.floor(this.options.marketCooldownMs ?? ASTRA_FAST_MARKET_COOLDOWN_MS)); }
  private maxMarkets(): number {
    return Math.min(ASTRA_FAST_MAX_MARKETS, Math.max(1, Math.floor(this.options.maxMarkets ?? ASTRA_FAST_MAX_MARKETS)));
  }

  private prune(nowMs: number): void {
    const cutoff = nowMs - 3_600_000;
    this.dispatches = this.dispatches.filter((atMs) => atMs >= cutoff).slice(-this.maxDispatches());
    if (this.lastDispatchAtMs != null && this.lastDispatchAtMs < cutoff) this.lastDispatchAtMs = this.dispatches.at(-1) ?? null;
    for (const [marketId, untilMs] of this.marketCooldowns) {
      if (untilMs <= nowMs) this.marketCooldowns.delete(marketId);
    }
    this.trimMarkets();
  }

  private trimMarkets(): void {
    while (this.marketCooldowns.size > this.maxMarkets()) {
      const oldest = this.marketCooldowns.keys().next().value as string | undefined;
      if (!oldest) break;
      this.marketCooldowns.delete(oldest);
    }
  }
}
