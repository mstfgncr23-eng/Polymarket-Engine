import { bookImbalance, d, spread } from "./calculations.js";
import { createId, type MarketSnapshot, type ReferenceTick, type TriggerEvent, type TriggerKind, type TriggerSource } from "./trigger-types.js";
import type { TriggerConfig } from "./config.js";

interface MarketState { snapshot: MarketSnapshot; emitted: Map<string, number>; }

export class TriggerEngine {
  private readonly markets = new Map<string, MarketState>();
  private readonly ticksByAsset = new Map<string, ReferenceTick[]>();

  constructor(private readonly config: TriggerConfig, private readonly now: () => Date = () => new Date()) {}

  ingestMarket(snapshot: MarketSnapshot): TriggerEvent[] {
    const previous = this.markets.get(snapshot.marketId);
    const state: MarketState = previous ?? { snapshot, emitted: new Map() };
    const candidates: Array<{ kind: TriggerKind; score: number; severity: 1 | 2 | 3; evidence: Record<string, unknown>; source: TriggerSource }> = [];
    const imbalance = Math.abs(bookImbalance(snapshot));
    if (imbalance >= this.config.imbalanceThreshold) candidates.push({ kind: "BOOK_IMBALANCE", score: imbalance, severity: imbalance > 0.85 ? 2 : 1, evidence: { imbalance }, source: "polymarket" });
    const currentSpread = Number(spread(snapshot) ?? 0);
    const previousSpread = previous ? Number(spread(previous.snapshot) ?? 0) : currentSpread;
    const spreadDelta = Math.abs(currentSpread - previousSpread);
    if (spreadDelta >= this.config.spreadChangeThreshold) candidates.push({ kind: "SPREAD_CHANGE", score: spreadDelta, severity: currentSpread > 0.1 ? 2 : 1, evidence: { currentSpread, previousSpread }, source: "polymarket" });
    if (previous) {
      const depth = totalDepth(snapshot);
      const oldDepth = totalDepth(previous.snapshot);
      const change = oldDepth === 0 ? 0 : Math.abs(depth - oldDepth) / oldDepth;
      if (change >= this.config.depthChangeThreshold) candidates.push({ kind: "DEPTH_CHANGE", score: change, severity: change > 0.75 ? 2 : 1, evidence: { depth, oldDepth, change }, source: "polymarket" });
    }
    state.snapshot = snapshot;
    this.markets.set(snapshot.marketId, state);
    return this.emit(snapshot.traceId, snapshot.marketId, snapshot.asset ?? null, snapshot.eventId, candidates, state.emitted);
  }

  ingestReference(tick: ReferenceTick): TriggerEvent[] {
    const asset = tick.asset;
    const ticks = this.ticksByAsset.get(asset) ?? [];
    ticks.push(tick);
    const cutoff = new Date(tick.sourceTimestamp).getTime() - 60_000;
    while (ticks[0] && new Date(ticks[0].sourceTimestamp).getTime() < cutoff) ticks.shift();
    this.ticksByAsset.set(asset, ticks);
    const first = ticks[0];
    if (!first) return [];
    const moveBps = d(tick.price).minus(first.price).div(first.price).mul(10_000).toNumber();
    const threshold = this.config.referenceRapidMoveBps ?? this.config.btcRapidMoveBps;
    if (Math.abs(moveBps) < threshold) return [];
    const kind: TriggerKind = asset === "BTC" ? "BTC_RAPID_MOVE" : "CRYPTO_REFERENCE_RAPID_MOVE";
    const candidate = {
      kind,
      score: Math.abs(moveBps),
      severity: (Math.abs(moveBps) >= this.config.volatilitySpikeBps ? 2 : 1) as 1 | 2 | 3,
      evidence: { moveBps, windowMs: new Date(tick.sourceTimestamp).getTime() - new Date(first.sourceTimestamp).getTime(), asset, symbol: tick.symbol },
      source: "reference" as const,
    };
    const markets = [...this.markets.values()].filter((state) => (state.snapshot.asset ?? null) === asset);
    if (markets.length === 0) {
      return this.emit(tick.traceId, null, asset, tick.eventId, [candidate], new Map());
    }
    const emitted: TriggerEvent[] = [];
    for (const state of markets) {
      emitted.push(...this.emit(tick.traceId, state.snapshot.marketId, asset, tick.eventId, [candidate], state.emitted));
    }
    return emitted;
  }

  /** @deprecated Use ingestReference. BTC ticks are asset-isolated reference ticks. */
  ingestBtc(tick: ReferenceTick): TriggerEvent[] {
    return this.ingestReference(tick.asset ? tick : { ...tick, asset: "BTC" });
  }

  dropMarket(marketId: string): void {
    this.markets.delete(marketId);
  }

  dropAsset(asset: string): void {
    this.ticksByAsset.delete(asset);
  }

  private emit(
    traceId: string, marketId: string | null, asset: string | null, sourceEventId: string,
    candidates: Array<{ kind: TriggerKind; score: number; severity: 1 | 2 | 3; evidence: Record<string, unknown>; source: TriggerSource }>,
    emitted: Map<string, number>,
  ): TriggerEvent[] {
    const now = this.now();
    const result: TriggerEvent[] = [];
    for (const candidate of candidates) {
      const dedupeKey = `${marketId ?? "none"}:${asset ?? "none"}:${candidate.kind}:${Math.floor(candidate.score * 10)}`;
      const last = emitted.get(dedupeKey) ?? 0;
      if (now.getTime() - last < this.config.cooldownMs) continue;
      emitted.set(dedupeKey, now.getTime());
      result.push({
        id: createId("trigger"), traceId, marketId, asset, source: candidate.source, kind: candidate.kind, severity: candidate.severity,
        score: candidate.score, dedupeKey, evidence: candidate.evidence, sourceEventIds: [sourceEventId], evidenceIds: [sourceEventId],
        createdAt: now.toISOString(), expiresAt: new Date(now.getTime() + this.config.triggerTtlMs).toISOString(),
      });
    }
    return result;
  }
}

function totalDepth(snapshot: MarketSnapshot): number {
  return [...snapshot.bids, ...snapshot.asks].slice(0, 10).reduce((sum, level) => sum + Number(level.quantity), 0);
}
