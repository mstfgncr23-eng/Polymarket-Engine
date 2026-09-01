import { bookImbalance, d, spread } from "./calculations.js";
import { createId, type BtcTick, type MarketSnapshot, type TriggerEvent, type TriggerKind } from "./trigger-types.js";
import type { TriggerConfig } from "./config.js";

interface MarketState { snapshot: MarketSnapshot; emitted: Map<string, number>; }

export class TriggerEngine {
  private readonly markets = new Map<string, MarketState>();
  private readonly btcTicks: BtcTick[] = [];

  constructor(private readonly config: TriggerConfig, private readonly now: () => Date = () => new Date()) {}

  ingestMarket(snapshot: MarketSnapshot): TriggerEvent[] {
    const previous = this.markets.get(snapshot.marketId);
    const state: MarketState = previous ?? { snapshot, emitted: new Map() };
    const candidates: Array<{ kind: TriggerKind; score: number; severity: 1 | 2 | 3; evidence: Record<string, unknown> }> = [];
    const imbalance = Math.abs(bookImbalance(snapshot));
    if (imbalance >= this.config.imbalanceThreshold) candidates.push({ kind: "BOOK_IMBALANCE", score: imbalance, severity: imbalance > 0.85 ? 2 : 1, evidence: { imbalance } });
    const currentSpread = Number(spread(snapshot) ?? 0);
    const previousSpread = previous ? Number(spread(previous.snapshot) ?? 0) : currentSpread;
    const spreadDelta = Math.abs(currentSpread - previousSpread);
    if (spreadDelta >= this.config.spreadChangeThreshold) candidates.push({ kind: "SPREAD_CHANGE", score: spreadDelta, severity: currentSpread > 0.1 ? 2 : 1, evidence: { currentSpread, previousSpread } });
    if (previous) {
      const depth = totalDepth(snapshot);
      const oldDepth = totalDepth(previous.snapshot);
      const change = oldDepth === 0 ? 0 : Math.abs(depth - oldDepth) / oldDepth;
      if (change >= this.config.depthChangeThreshold) candidates.push({ kind: "DEPTH_CHANGE", score: change, severity: change > 0.75 ? 2 : 1, evidence: { depth, oldDepth, change } });
    }
    state.snapshot = snapshot;
    this.markets.set(snapshot.marketId, state);
    return this.emit(snapshot.traceId, snapshot.marketId, snapshot.eventId, candidates, state.emitted);
  }

  ingestBtc(tick: BtcTick): TriggerEvent[] {
    this.btcTicks.push(tick);
    const cutoff = new Date(tick.sourceTimestamp).getTime() - 60_000;
    while (this.btcTicks[0] && new Date(this.btcTicks[0].sourceTimestamp).getTime() < cutoff) this.btcTicks.shift();
    const first = this.btcTicks[0];
    if (!first) return [];
    const moveBps = d(tick.price).minus(first.price).div(first.price).mul(10_000).toNumber();
    if (Math.abs(moveBps) < this.config.btcRapidMoveBps) return [];
    return this.emit(tick.traceId, null, tick.eventId, [{
      kind: "BTC_RAPID_MOVE", score: Math.abs(moveBps), severity: Math.abs(moveBps) >= this.config.volatilitySpikeBps ? 2 : 1,
      evidence: { moveBps, windowMs: new Date(tick.sourceTimestamp).getTime() - new Date(first.sourceTimestamp).getTime() },
    }], new Map());
  }

  private emit(
    traceId: string, marketId: string | null, sourceEventId: string,
    candidates: Array<{ kind: TriggerKind; score: number; severity: 1 | 2 | 3; evidence: Record<string, unknown> }>,
    emitted: Map<string, number>,
  ): TriggerEvent[] {
    const now = this.now();
    const result: TriggerEvent[] = [];
    for (const candidate of candidates) {
      const dedupeKey = `${marketId ?? "btc"}:${candidate.kind}:${Math.floor(candidate.score * 10)}`;
      const last = emitted.get(dedupeKey) ?? 0;
      if (now.getTime() - last < this.config.cooldownMs) continue;
      emitted.set(dedupeKey, now.getTime());
      result.push({
        id: createId("trigger"), traceId, marketId, kind: candidate.kind, severity: candidate.severity,
        score: candidate.score, dedupeKey, evidence: candidate.evidence, sourceEventIds: [sourceEventId],
        createdAt: now.toISOString(), expiresAt: new Date(now.getTime() + this.config.triggerTtlMs).toISOString(),
      });
    }
    return result;
  }
}

function totalDepth(snapshot: MarketSnapshot): number {
  return [...snapshot.bids, ...snapshot.asks].slice(0, 10).reduce((sum, level) => sum + Number(level.quantity), 0);
}
