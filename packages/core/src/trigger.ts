import { bookImbalance, d, spread } from "./calculations.js";
import { createId, type ConsensusDecision, type MarketSnapshot, type Outcome, type Position, type ReferenceTick, type TriggerEvent, type TriggerKind, type TriggerSource } from "./domain.js";
import type { TriggerConfig } from "./config.js";

interface BookState { snapshot: MarketSnapshot; emitted: Map<string, number>; }

export class TriggerEngine {
  private readonly books = new Map<string, BookState>();
  private readonly pair = new Map<string, Partial<Record<Outcome, MarketSnapshot>>>();
  private readonly ticksByAsset = new Map<string, ReferenceTick[]>();
  private readonly windows = new Map<string, { asset: string | null; endDate: string | null }>();
  private readonly emittedByMarket = new Map<string, Map<string, number>>();
  private lastScheduledReviewAt = 0;

  constructor(private readonly config: TriggerConfig, private readonly now: () => Date = () => new Date()) {}

  noteMarketWindow(marketId: string, asset: string | null, endDate: string | null): void {
    this.windows.set(marketId, { asset, endDate });
  }

  ingestMarket(snapshot: MarketSnapshot): TriggerEvent[] {
    const key = bookKey(snapshot.marketId, snapshot.outcome);
    const previous = this.books.get(key);
    const state: BookState = previous ?? { snapshot, emitted: new Map() };
    const candidates: Array<{ kind: TriggerKind; score: number; severity: 1 | 2 | 3; evidence: Record<string, unknown>; source: TriggerSource }> = [];
    const imbalance = Math.abs(bookImbalance(snapshot));
    if (imbalance >= this.config.imbalanceThreshold) candidates.push({ kind: "BOOK_IMBALANCE", score: imbalance, severity: imbalance > 0.85 ? 2 : 1, evidence: { imbalance, outcome: snapshot.outcome }, source: "polymarket" });
    const currentSpread = Number(spread(snapshot) ?? 0);
    const previousSameOutcome = previous && previous.snapshot.outcome === snapshot.outcome && previous.snapshot.tokenId === snapshot.tokenId ? previous : null;
    const previousSpread = previousSameOutcome ? Number(spread(previousSameOutcome.snapshot) ?? 0) : currentSpread;
    const spreadDelta = Math.abs(currentSpread - previousSpread);
    if (previousSameOutcome && spreadDelta >= this.config.spreadChangeThreshold) {
      candidates.push({ kind: "SPREAD_CHANGE", score: spreadDelta, severity: currentSpread > 0.1 ? 2 : 1, evidence: { currentSpread, previousSpread, outcome: snapshot.outcome, tokenId: snapshot.tokenId }, source: "polymarket" });
    }
    if (previousSameOutcome) {
      const depth = totalDepth(snapshot);
      const oldDepth = totalDepth(previousSameOutcome.snapshot);
      const change = oldDepth === 0 ? 0 : Math.abs(depth - oldDepth) / oldDepth;
      if (change >= this.config.depthChangeThreshold) candidates.push({ kind: "DEPTH_CHANGE", score: change, severity: change > 0.75 ? 2 : 1, evidence: { depth, oldDepth, change, outcome: snapshot.outcome, tokenId: snapshot.tokenId }, source: "polymarket" });
    }
    const pair = this.pair.get(snapshot.marketId) ?? {};
    pair[snapshot.outcome] = snapshot;
    this.pair.set(snapshot.marketId, pair);
    if (pair.UP && pair.DOWN) {
      const upAsk = Number(pair.UP.asks[0]?.price ?? Number.NaN);
      const downAsk = Number(pair.DOWN.asks[0]?.price ?? Number.NaN);
      if (Number.isFinite(upAsk) && Number.isFinite(downAsk)) {
        const pairCost = upAsk + downAsk;
        if (pairCost <= this.config.pairCostThreshold) {
          candidates.push({ kind: "PAIR_COST_OPPORTUNITY", score: Math.max(0, 1 - pairCost), severity: pairCost < 0.96 ? 2 : 1, evidence: { pairCost, upAsk, downAsk }, source: "polymarket" });
        }
      }
    }
    const window = this.windows.get(snapshot.marketId);
    if (window?.endDate) {
      const remaining = new Date(window.endDate).getTime() - this.now().getTime();
      if (remaining > 0 && remaining <= this.config.expiryWarningMs) {
        candidates.push({ kind: "NEARING_EXPIRY", score: 1 / Math.max(1, remaining), severity: remaining < 60_000 ? 2 : 1, evidence: { endDate: window.endDate, remainingMs: remaining }, source: "system" });
      }
    }
    const newsWorthy = candidates.filter((item) => item.kind === "BOOK_IMBALANCE" && item.severity >= (this.config.newsSeverityThreshold ?? 2));
    if (newsWorthy.length > 0) {
      candidates.push({ kind: "NEWS_INFORMATION", score: Math.max(...newsWorthy.map((item) => item.score)), severity: 2, evidence: { because: "BOOK_IMBALANCE" }, source: "news" });
    }
    state.snapshot = snapshot;
    this.books.set(key, state);
    return this.emit(snapshot.traceId, snapshot.marketId, snapshot.asset ?? window?.asset ?? null, snapshot.eventId, candidates, this.marketEmitted(snapshot.marketId));
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
    const severity = (Math.abs(moveBps) >= this.config.volatilitySpikeBps ? 2 : 1) as 1 | 2 | 3;
    const candidate = {
      kind, score: Math.abs(moveBps), severity,
      evidence: { moveBps, windowMs: new Date(tick.sourceTimestamp).getTime() - new Date(first.sourceTimestamp).getTime(), asset, symbol: tick.symbol },
      source: "reference" as const,
    };
    const extras = severity >= (this.config.newsSeverityThreshold ?? 2)
      ? [{ kind: "NEWS_INFORMATION" as const, score: candidate.score, severity: 2 as const, evidence: { because: kind, asset }, source: "news" as const }]
      : [];
    const marketIds = uniqueMarketIdsForAsset(this.books, asset);
    if (marketIds.length === 0) {
      return this.emit(tick.traceId, null, asset, tick.eventId, [candidate, ...extras], new Map());
    }
    const emitted: TriggerEvent[] = [];
    for (const marketId of marketIds) {
      emitted.push(...this.emit(tick.traceId, marketId, asset, tick.eventId, [candidate, ...extras], this.marketEmitted(marketId)));
    }
    return emitted;
  }

  ingestPosition(position: Position, traceId: string, asset: string | null): TriggerEvent[] {
    const unpaired = Number(position.unpairedQuantity);
    if (unpaired <= 0) return [];
    const candidates: Array<{ kind: TriggerKind; score: number; severity: 1 | 2 | 3; evidence: Record<string, unknown>; source: TriggerSource }> = [
      { kind: "DIRECTIONAL_EXPOSURE", score: unpaired, severity: unpaired > 20 ? 2 : 1, evidence: { unpairedQuantity: position.unpairedQuantity, unpairedOutcome: position.unpairedOutcome }, source: "risk" },
    ];
    const age = this.now().getTime() - new Date(position.openedAt).getTime();
    if (age >= (this.config.failedHedgeDurationMs ?? 300_000)) {
      candidates.push({ kind: "FAILED_HEDGE", score: age, severity: 2, evidence: { unpairedQuantity: position.unpairedQuantity, ageMs: age }, source: "risk" });
    }
    return this.emit(traceId, position.marketId, asset, position.id, candidates, this.marketEmitted(position.marketId));
  }

  ingestConsensus(consensus: Pick<ConsensusDecision, "id" | "traceId" | "conflicts" | "agentAgreement">, marketId: string | null, asset: string | null): TriggerEvent[] {
    if (!consensus.conflicts.includes("DIRECTION_DISAGREEMENT")) return [];
    if (consensus.agentAgreement > (this.config.disagreementAgreementMax ?? 0.45)) return [];
    return this.emit(consensus.traceId, marketId, asset, consensus.id, [{
      kind: "SEVERE_DISAGREEMENT", score: 1 - consensus.agentAgreement, severity: 3,
      evidence: { conflicts: consensus.conflicts, agentAgreement: consensus.agentAgreement }, source: "system",
    }], this.marketEmitted(marketId ?? "none"));
  }

  ingestScheduledReview(traceId: string, marketId: string | null = null, asset: string | null = null): TriggerEvent[] {
    const now = this.now().getTime();
    if (now - this.lastScheduledReviewAt < this.config.scheduledReviewIntervalMs) return [];
    this.lastScheduledReviewAt = now;
    return this.emit(traceId, marketId, asset, traceId, [{
      kind: "SCHEDULED_REVIEW", score: 1, severity: 1, evidence: { intervalMs: this.config.scheduledReviewIntervalMs }, source: "system",
    }], this.marketEmitted(marketId ?? "supervisor"));
  }

  /** @deprecated Use ingestReference. BTC ticks are asset-isolated reference ticks. */
  ingestBtc(tick: ReferenceTick): TriggerEvent[] {
    return this.ingestReference(tick.asset ? tick : { ...tick, asset: "BTC" });
  }

  dropMarket(marketId: string): void {
    for (const key of [...this.books.keys()]) if (key.startsWith(`${marketId}:`)) this.books.delete(key);
    this.pair.delete(marketId);
    this.windows.delete(marketId);
    this.emittedByMarket.delete(marketId);
  }

  dropAsset(asset: string): void {
    this.ticksByAsset.delete(asset);
  }

  private marketEmitted(marketId: string): Map<string, number> {
    const existing = this.emittedByMarket.get(marketId);
    if (existing) return existing;
    const created = new Map<string, number>();
    this.emittedByMarket.set(marketId, created);
    return created;
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

function bookKey(marketId: string, outcome: Outcome): string {
  return `${marketId}:${outcome}`;
}

function uniqueMarketIdsForAsset(books: Map<string, BookState>, asset: string): string[] {
  const ids = new Set<string>();
  for (const state of books.values()) {
    if ((state.snapshot.asset ?? null) === asset) ids.add(state.snapshot.marketId);
  }
  return [...ids];
}
