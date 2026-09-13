import { d } from "./calculations.js";
import { AssetMarketIndex } from "./asset-market-index.js";
import { deriveBook, type BookDerived } from "./book-derived.js";
import { createId, type ConsensusDecision, type MarketSnapshot, type Outcome, type Position, type ReferenceTick, type TriggerEvent, type TriggerKind, type TriggerSource } from "./domain.js";
import type { TriggerConfig } from "./config.js";
import { ReferenceTickWindow } from "./reference-window.js";
import {
  safetyEscalationMayEmit,
  TRIGGER_STABILITY_BOUNDS,
  triggerDedupeScopeKey,
} from "./stability-bounds.js";

interface BookState {
  snapshot: MarketSnapshot;
  derived: BookDerived;
}

interface MarketWindow {
  asset: string | null;
  endDate: string | null;
  endDateMs: number | null;
}

interface DedupeEntry {
  at: number;
  severity: 1 | 2 | 3;
}

interface Candidate {
  kind: TriggerKind;
  score: number;
  severity: 1 | 2 | 3;
  evidence: Record<string, unknown>;
  source: TriggerSource;
}

/** Optional emit observer. Must not change cooldown, debounce, or emit semantics. */
export interface TriggerEmitObserver {
  onCandidates(count: number): void;
  onEmitted(kind: TriggerKind, severity: 0 | 1 | 2 | 3): void;
  onSuppressed(kind: TriggerKind, severity: 0 | 1 | 2 | 3): void;
  onReferenceWindow?(length: number): void;
  onMarketIngest?(durationSeconds: number): void;
  onReferenceIngest?(durationSeconds: number): void;
  onReferenceWindowOverflow?(): void;
}

export class TriggerEngine {
  private readonly books = new Map<string, BookState>();
  private readonly pair = new Map<string, Partial<Record<Outcome, MarketSnapshot>>>();
  private readonly ticksByAsset = new Map<string, ReferenceTickWindow>();
  private readonly windows = new Map<string, MarketWindow>();
  private readonly emittedByMarket = new Map<string, Map<string, DedupeEntry>>();
  private readonly dedupeOrder: string[] = [];
  private dedupeOrderHead = 0;
  private dedupeCount = 0;
  private dedupeLiveEntries = 0;
  private dedupeScanCalls = 0;
  private readonly assetIndex = new AssetMarketIndex();
  private lastScheduledReviewAt = 0;

  constructor(
    private readonly config: TriggerConfig,
    private readonly now: () => Date = () => new Date(),
    private readonly observer?: TriggerEmitObserver,
  ) {}

  /**
   * Clear only suppression/timing state owned by the completed experiment.
   * Live books, pair projections, reference windows, market windows, and the
   * asset index intentionally survive the experiment boundary.
   */
  resetExperimentBoundaryState(): void {
    this.emittedByMarket.clear();
    this.dedupeOrder.length = 0;
    this.dedupeOrderHead = 0;
    this.dedupeCount = 0;
    this.dedupeLiveEntries = 0;
    this.lastScheduledReviewAt = 0;
  }

  noteMarketWindow(marketId: string, asset: string | null, endDate: string | null): void {
    const parsed = endDate ? Date.parse(endDate) : Number.NaN;
    this.windows.set(marketId, {
      asset,
      endDate,
      endDateMs: Number.isFinite(parsed) ? parsed : null,
    });
  }

  forgetBook(marketId: string, outcome: Outcome): void {
    this.books.delete(bookKey(marketId, outcome));
    this.pair.delete(marketId);
  }

  /** Clears both pair sides so pair-cost cannot mix epochs. */
  forgetMarketBooks(marketId: string): void {
    this.books.delete(bookKey(marketId, "UP"));
    this.books.delete(bookKey(marketId, "DOWN"));
    this.pair.delete(marketId);
  }

  ingestMarket(snapshot: MarketSnapshot): TriggerEvent[] {
    const started = performance.now();
    try {
      const key = bookKey(snapshot.marketId, snapshot.outcome);
      const previous = this.books.get(key);
      const derived = deriveBook(snapshot);
      const candidates: Candidate[] = [];
      let newsScore = -1;
      const imbalance = derived.imbalanceAbs;
      if (imbalance >= this.config.imbalanceThreshold) {
        const severity = (imbalance > 0.85 ? 2 : 1) as 1 | 2;
        candidates.push({ kind: "BOOK_IMBALANCE", score: imbalance, severity, evidence: { imbalance, outcome: snapshot.outcome }, source: "polymarket" });
        if (severity >= (this.config.newsSeverityThreshold ?? 2) && imbalance > newsScore) newsScore = imbalance;
      }
      const currentSpread = derived.spreadNumber;
      const previousSameOutcome = previous && previous.snapshot.outcome === snapshot.outcome && previous.snapshot.tokenId === snapshot.tokenId ? previous : null;
      const previousSpread = previousSameOutcome ? previousSameOutcome.derived.spreadNumber : currentSpread;
      const spreadDelta = Math.abs(currentSpread - previousSpread);
      if (previousSameOutcome && spreadDelta >= this.config.spreadChangeThreshold) {
        candidates.push({
          kind: "SPREAD_CHANGE",
          score: spreadDelta,
          severity: currentSpread > 0.1 ? 2 : 1,
          evidence: { currentSpread, previousSpread, outcome: snapshot.outcome, tokenId: snapshot.tokenId },
          source: "polymarket",
        });
      }
      if (previousSameOutcome) {
        const depth = derived.totalDepth;
        const oldDepth = previousSameOutcome.derived.totalDepth;
        const change = oldDepth === 0 ? 0 : Math.abs(depth - oldDepth) / oldDepth;
        if (change >= this.config.depthChangeThreshold) {
          candidates.push({
            kind: "DEPTH_CHANGE",
            score: change,
            severity: change > 0.75 ? 2 : 1,
            evidence: { depth, oldDepth, change, outcome: snapshot.outcome, tokenId: snapshot.tokenId },
            source: "polymarket",
          });
        }
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
      if (window?.endDateMs != null) {
        const remaining = window.endDateMs - this.now().getTime();
        if (remaining > 0 && remaining <= this.config.expiryWarningMs) {
          candidates.push({
            kind: "NEARING_EXPIRY",
            score: 1 / Math.max(1, remaining),
            severity: remaining < 60_000 ? 2 : 1,
            evidence: { endDate: window.endDate, remainingMs: remaining },
            source: "system",
          });
        }
      }
      if (newsScore >= 0) {
        candidates.push({ kind: "NEWS_INFORMATION", score: newsScore, severity: 2, evidence: { because: "BOOK_IMBALANCE" }, source: "news" });
      }
      this.books.set(key, { snapshot, derived });
      if (snapshot.asset) this.assetIndex.add(snapshot.asset, snapshot.marketId);
      return this.emit(snapshot.traceId, snapshot.marketId, snapshot.asset ?? window?.asset ?? null, snapshot.eventId, candidates, this.marketEmitted(snapshot.marketId));
    } finally {
      this.observer?.onMarketIngest?.((performance.now() - started) / 1000);
    }
  }

  admitReferenceTick(tick: ReferenceTick): { overflow: boolean; accepted: boolean; triggers: TriggerEvent[] } {
    const started = performance.now();
    try {
      const asset = tick.asset;
      const window = this.ticksByAsset.get(asset) ?? new ReferenceTickWindow();
      this.ticksByAsset.set(asset, window);
      if (window.hasOverflowed) {
        this.observer?.onReferenceWindow?.(window.size);
        return { overflow: true, accepted: false, triggers: [] };
      }
      const sourceMs = Date.parse(tick.sourceTimestamp);
      const pushed = window.push(tick, sourceMs);
      this.observer?.onReferenceWindow?.(window.size);
      if (pushed.overflow) {
        this.observer?.onReferenceWindowOverflow?.();
        return { overflow: true, accepted: false, triggers: [] };
      }
      if (!pushed.accepted) return { overflow: false, accepted: false, triggers: [] };
      const first = window.first();
      if (!first) return { overflow: false, accepted: true, triggers: [] };
      const moveBps = d(tick.price).minus(first.tick.price).div(first.tick.price).mul(10_000).toNumber();
      const threshold = this.config.referenceRapidMoveBps ?? this.config.btcRapidMoveBps;
      if (Math.abs(moveBps) < threshold) return { overflow: false, accepted: true, triggers: [] };
      const kind: TriggerKind = asset === "BTC" ? "BTC_RAPID_MOVE" : "CRYPTO_REFERENCE_RAPID_MOVE";
      const severity = (Math.abs(moveBps) >= this.config.volatilitySpikeBps ? 2 : 1) as 1 | 2 | 3;
      const candidate: Candidate = {
        kind, score: Math.abs(moveBps), severity,
        evidence: { moveBps, windowMs: sourceMs - first.sourceMs, asset, symbol: tick.symbol },
        source: "reference",
      };
      const extras: Candidate[] = severity >= (this.config.newsSeverityThreshold ?? 2)
        ? [{ kind: "NEWS_INFORMATION", score: candidate.score, severity: 2, evidence: { because: kind, asset }, source: "news" }]
        : [];
      const marketIds = this.assetIndex.marketIds(asset);
      if (marketIds.length === 0) {
        return { overflow: false, accepted: true, triggers: this.emit(tick.traceId, null, asset, tick.eventId, [candidate, ...extras], new Map()) };
      }
      const emitted: TriggerEvent[] = [];
      for (const marketId of marketIds) {
        emitted.push(...this.emit(tick.traceId, marketId, asset, tick.eventId, [candidate, ...extras], this.marketEmitted(marketId)));
      }
      return { overflow: false, accepted: true, triggers: emitted };
    } finally {
      this.observer?.onReferenceIngest?.((performance.now() - started) / 1000);
    }
  }

  ingestReference(tick: ReferenceTick): TriggerEvent[] {
    return this.admitReferenceTick(tick).triggers;
  }

  /**
   * Returns the directional move across the same bounded time window used by
   * admitReferenceTick(). This is intentionally O(1) and returns null for an
   * empty or overflowed window so native decisions fail closed.
   */
  referenceMovement(asset: string): {
    moveBps: number;
    windowMs: number;
    sourceTimestamp: string;
  } | null {
    const window = this.ticksByAsset.get(asset);
    if (!window || window.hasOverflowed) return null;
    const first = window.first();
    const last = window.last();
    if (!first || !last || first.sourceMs > last.sourceMs) return null;
    try {
      const moveBps = d(last.tick.price).minus(first.tick.price).div(first.tick.price).mul(10_000).toNumber();
      if (!Number.isFinite(moveBps)) return null;
      return {
        moveBps,
        windowMs: last.sourceMs - first.sourceMs,
        sourceTimestamp: last.tick.sourceTimestamp,
      };
    } catch {
      return null;
    }
  }

  ingestPosition(position: Position, traceId: string, asset: string | null): TriggerEvent[] {
    const unpaired = Number(position.unpairedQuantity);
    if (unpaired <= 0) return [];
    const candidates: Candidate[] = [
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
    const bucket = this.emittedByMarket.get(marketId);
    if (bucket) this.dedupeLiveEntries = Math.max(0, this.dedupeLiveEntries - bucket.size);
    this.emittedByMarket.delete(marketId);
    this.assetIndex.removeMarket(marketId);
  }

  dropAsset(asset: string): void {
    this.ticksByAsset.get(asset)?.clear();
    this.ticksByAsset.delete(asset);
    this.assetIndex.dropAsset(asset);
  }

  resetReferenceAfterReconnect(asset: string): void {
    this.ticksByAsset.get(asset)?.resetAfterReconnect();
  }

  attachReferenceWindowForTest(asset: string, maxEntries: number): void {
    this.ticksByAsset.set(asset, new ReferenceTickWindow(maxEntries));
  }

  referenceWindowSize(asset: string): number {
    return this.ticksByAsset.get(asset)?.size ?? 0;
  }

  referenceWindowTotalEntries(): number {
    let total = 0;
    for (const window of this.ticksByAsset.values()) total += window.size;
    return total;
  }

  referenceWindowOverflowed(asset: string): boolean {
    return this.ticksByAsset.get(asset)?.hasOverflowed ?? false;
  }

  dedupeEntryCount(): number {
    return this.dedupeLiveEntries;
  }

  dedupeEntryCountScanForTest(): number {
    this.dedupeScanCalls += 1;
    let count = 0;
    for (const bucket of this.emittedByMarket.values()) count += bucket.size;
    return count;
  }

  dedupeScanCallsForTest(): number {
    return this.dedupeScanCalls;
  }

  assetMarketIndexEntries(): number {
    return this.assetIndex.entryCount();
  }

  hasManagedAsset(asset: string): boolean {
    return this.assetIndex.hasAsset(asset);
  }

  marketsForAsset(asset: string): string[] {
    return this.assetIndex.marketIds(asset);
  }

  private marketEmitted(marketId: string): Map<string, DedupeEntry> {
    const existing = this.emittedByMarket.get(marketId);
    if (existing) return existing;
    const created = new Map<string, DedupeEntry>();
    this.emittedByMarket.set(marketId, created);
    return created;
  }

  private emit(
    traceId: string, marketId: string | null, asset: string | null, sourceEventId: string,
    candidates: Candidate[],
    emitted: Map<string, DedupeEntry>,
  ): TriggerEvent[] {
    const now = this.now();
    const nowMs = now.getTime();
    const result: TriggerEvent[] = [];
    if (candidates.length > 0) this.observer?.onCandidates(candidates.length);
    this.pruneDedupe(nowMs);
    for (const candidate of candidates) {
      const dedupeKey = triggerDedupeScopeKey(marketId, asset, candidate.kind);
      const last = emitted.get(dedupeKey);
      if (last && nowMs - last.at < this.config.cooldownMs) {
        const escalate = safetyEscalationMayEmit(candidate.kind, last.severity, candidate.severity);
        if (!escalate) {
          this.observer?.onSuppressed(candidate.kind, candidate.severity);
          continue;
        }
      }
      const existed = emitted.has(dedupeKey);
      emitted.set(dedupeKey, { at: nowMs, severity: candidate.severity });
      if (!existed) this.dedupeLiveEntries += 1;
      this.noteDedupe(marketId ?? "none", dedupeKey, nowMs);
      result.push({
        id: createId("trigger"), traceId, marketId, asset, source: candidate.source, kind: candidate.kind, severity: candidate.severity,
        score: candidate.score, dedupeKey, evidence: candidate.evidence, sourceEventIds: [sourceEventId], evidenceIds: [sourceEventId],
        createdAt: now.toISOString(), expiresAt: new Date(nowMs + this.config.triggerTtlMs).toISOString(),
      });
      this.observer?.onEmitted(candidate.kind, candidate.severity);
    }
    return result;
  }

  private noteDedupe(marketId: string, dedupeKey: string, at: number): void {
    this.dedupeOrder.push(`${marketId}\0${dedupeKey}\0${at}`);
    this.dedupeCount += 1;
    this.enforceDedupeCap();
  }

  private pruneDedupe(nowMs: number): void {
    const cutoff = nowMs - TRIGGER_STABILITY_BOUNDS.dedupeRetentionMs;
    while (this.dedupeOrderHead < this.dedupeOrder.length) {
      const raw = this.dedupeOrder[this.dedupeOrderHead];
      if (!raw) break;
      const parts = raw.split("\0");
      const at = Number(parts[2]);
      if (at >= cutoff) break;
      this.removeDedupeRecord(parts[0]!, parts[1]!, at);
      this.dedupeOrder[this.dedupeOrderHead] = undefined as unknown as string;
      this.dedupeOrderHead += 1;
      this.dedupeCount = Math.max(0, this.dedupeCount - 1);
    }
    this.compactDedupeOrder();
  }

  private enforceDedupeCap(): void {
    while (this.dedupeCount > TRIGGER_STABILITY_BOUNDS.dedupeMaxEntries && this.dedupeOrderHead < this.dedupeOrder.length) {
      const raw = this.dedupeOrder[this.dedupeOrderHead];
      this.dedupeOrder[this.dedupeOrderHead] = undefined as unknown as string;
      this.dedupeOrderHead += 1;
      this.dedupeCount = Math.max(0, this.dedupeCount - 1);
      if (!raw) continue;
      const parts = raw.split("\0");
      this.removeDedupeRecord(parts[0]!, parts[1]!, Number(parts[2]));
    }
    this.compactDedupeOrder();
  }

  private removeDedupeRecord(marketId: string, dedupeKey: string, at: number): void {
    const bucket = this.emittedByMarket.get(marketId);
    const current = bucket?.get(dedupeKey);
    if (current && current.at === at) {
      bucket!.delete(dedupeKey);
      this.dedupeLiveEntries = Math.max(0, this.dedupeLiveEntries - 1);
    }
    if (bucket && bucket.size === 0) this.emittedByMarket.delete(marketId);
  }

  private compactDedupeOrder(): void {
    if (this.dedupeOrderHead < 1_024) return;
    this.dedupeOrder.splice(0, this.dedupeOrderHead);
    this.dedupeOrderHead = 0;
  }
}

function bookKey(marketId: string, outcome: Outcome): string {
  return `${marketId}:${outcome}`;
}
