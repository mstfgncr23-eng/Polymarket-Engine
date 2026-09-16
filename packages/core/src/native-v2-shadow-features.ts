import type { MarketSnapshot, Outcome, ReferenceTick } from "./domain.js";

export const NATIVE_V2_SHADOW_FEATURE_VERSION = "native-v2-shadow-features-v1" as const;

const BOOK_LEVELS = 5;
const REFERENCE_WINDOW_MS = 60_000;
const REFERENCE_SHORT_MS = 5_000;
const REFERENCE_MEDIUM_MS = 30_000;
const REFERENCE_BUCKET_MS = 1_000;
const REFERENCE_MAX_BUCKETS = 64;

export interface NativeV2ShadowBookFeatures {
  mid: number;
  spread: number;
  spreadBps: number;
  microprice: number;
  micropriceOffsetBps: number;
  imbalanceL1: number;
  imbalanceL3: number;
  imbalanceL5: number;
  bidDepthUsdL5: number;
  askDepthUsdL5: number;
  depthAsymmetryL5: number;
  bidDepthSlope: number | null;
  askDepthSlope: number | null;
  depthSlopeAsymmetry: number | null;
  bidLiquidityConcentrationL1InL5: number;
  askLiquidityConcentrationL1InL5: number;
  spreadVelocityBpsPerSecond: number | null;
  midVelocityBpsPerSecond: number | null;
  sourceTimestamp: string;
  receivedAt: string;
  sequence: number;
}

export interface NativeV2ShadowFeatureVector {
  version: typeof NATIVE_V2_SHADOW_FEATURE_VERSION;
  marketId: string;
  asset: string | null;
  observedAt: string;
  up: NativeV2ShadowBookFeatures;
  down: NativeV2ShadowBookFeatures;
  pair: {
    marketPriorUp: number;
    pairAskCost: number;
    binaryComplementBasis: number;
    bookPressureL1: number;
    bookPressureL3: number;
    bookPressureL5: number;
    orderPressureVelocityPerSecond: number | null;
    signalDirection: -1 | 0 | 1;
    signalPersistenceMs: number;
    signalPersistenceSamples: number;
  };
  reference: {
    latestPrice: number | null;
    shortMomentumBps: number | null;
    mediumMomentumBps: number | null;
    realizedVolatilityBps: number | null;
    volatilityNormalizedMovement: number | null;
    sourceTimestamp: string | null;
    sourceAgeMs: number | null;
    marketReferenceSourceSkewMs: number | null;
  };
  timing: {
    timeToExpiryMs: number | null;
    upSourceAgeMs: number;
    downSourceAgeMs: number;
    pairSourceSkewMs: number;
  };
  synchronization: {
    marketSynchronized: boolean;
    referenceFreshness: "fresh" | "stale" | "unknown";
    referenceFreshnessMs: number | null;
  };
}

interface StoredBook {
  snapshot: MarketSnapshot;
  sourceMs: number;
  features: NativeV2ShadowBookFeatures;
}

interface PairSignalState {
  key: string;
  pressure: number;
  observedMs: number;
  direction: -1 | 0 | 1;
  sinceMs: number;
  samples: number;
  velocityPerSecond: number | null;
}

interface ReferenceBucket {
  bucket: number;
  sourceMs: number;
  price: number;
  sourceTimestamp: string;
}

class BoundedReferenceBuckets {
  private items: ReferenceBucket[] = [];
  private head = 0;

  push(tick: ReferenceTick): boolean {
    const sourceMs = Date.parse(tick.sourceTimestamp);
    const price = Number(tick.price);
    if (!Number.isFinite(sourceMs) || !Number.isFinite(price) || price <= 0) return false;
    const last = this.last();
    if (last && sourceMs < last.sourceMs) return false;

    const bucket = Math.floor(sourceMs / REFERENCE_BUCKET_MS);
    if (last?.bucket === bucket) {
      this.items[this.items.length - 1] = { bucket, sourceMs, price, sourceTimestamp: tick.sourceTimestamp };
    } else {
      this.items.push({ bucket, sourceMs, price, sourceTimestamp: tick.sourceTimestamp });
    }
    this.evict(sourceMs - REFERENCE_WINDOW_MS);
    this.enforceBound();
    return true;
  }

  clear(): void {
    this.items = [];
    this.head = 0;
  }

  size(): number { return this.items.length - this.head; }

  latest(): ReferenceBucket | null { return this.last() ?? null; }

  momentumBps(horizonMs: number): number | null {
    const last = this.last();
    if (!last) return null;
    const base = this.atOrBefore(last.sourceMs - horizonMs);
    if (!base || base.price <= 0) return null;
    return ((last.price - base.price) / base.price) * 10_000;
  }

  realizedVolatilityBps(horizonMs: number): number | null {
    const last = this.last();
    if (!last) return null;
    const cutoff = last.sourceMs - horizonMs;
    const start = this.indexAtOrBefore(cutoff);
    if (start == null || this.items.length - start < 2) return null;
    let sumSquares = 0;
    let observations = 0;
    let previous = this.items[start]!;
    for (let i = start + 1; i < this.items.length; i += 1) {
      const current = this.items[i]!;
      if (current.price > 0 && previous.price > 0) {
        const r = (current.price - previous.price) / previous.price;
        if (Number.isFinite(r)) {
          sumSquares += r * r;
          observations += 1;
        }
      }
      previous = current;
    }
    return observations > 0 ? Math.sqrt(sumSquares) * 10_000 : null;
  }

  private last(): ReferenceBucket | undefined {
    return this.items.length > this.head ? this.items[this.items.length - 1] : undefined;
  }

  private atOrBefore(targetMs: number): ReferenceBucket | null {
    const index = this.indexAtOrBefore(targetMs);
    return index == null ? null : this.items[index]!;
  }

  private indexAtOrBefore(targetMs: number): number | null {
    for (let i = this.items.length - 1; i >= this.head; i -= 1) {
      if (this.items[i]!.sourceMs <= targetMs) return i;
    }
    return null;
  }

  private evict(cutoffMs: number): void {
    while (this.head < this.items.length && this.items[this.head]!.sourceMs < cutoffMs) this.head += 1;
    if (this.head > 32) {
      this.items = this.items.slice(this.head);
      this.head = 0;
    }
  }

  private enforceBound(): void {
    const live = this.size();
    if (live <= REFERENCE_MAX_BUCKETS) return;
    this.head += live - REFERENCE_MAX_BUCKETS;
    if (this.head > 32) {
      this.items = this.items.slice(this.head);
      this.head = 0;
    }
  }
}

export class NativeV2ShadowFeatureExtractor {
  private readonly books = new Map<string, Partial<Record<Outcome, StoredBook>>>();
  private readonly pairSignals = new Map<string, PairSignalState>();
  private readonly references = new Map<string, BoundedReferenceBuckets>();

  ingestMarket(snapshot: MarketSnapshot): boolean {
    const sourceMs = Date.parse(snapshot.sourceTimestamp);
    if (!Number.isFinite(sourceMs)) return false;
    const pair = this.books.get(snapshot.marketId) ?? {};
    const previous = pair[snapshot.outcome];
    if (previous && snapshot.sequence <= previous.snapshot.sequence) return false;
    const features = deriveBookFeatures(snapshot, previous ?? null);
    if (!features) return false;
    pair[snapshot.outcome] = { snapshot, sourceMs, features };
    this.books.set(snapshot.marketId, pair);
    this.updatePairSignal(snapshot.marketId, pair);
    return true;
  }

  ingestReference(tick: ReferenceTick): boolean {
    const window = this.references.get(tick.asset) ?? new BoundedReferenceBuckets();
    this.references.set(tick.asset, window);
    return window.push(tick);
  }

  extract(input: {
    marketId: string;
    asset: string | null;
    now: Date;
    endDate: string | null;
    marketSynchronized: boolean;
    referenceFreshness: "fresh" | "stale" | "unknown";
    referenceFreshnessMs: number | null;
  }): NativeV2ShadowFeatureVector | null {
    const nowMs = input.now.getTime();
    if (!Number.isFinite(nowMs)) return null;
    const pair = this.books.get(input.marketId);
    const up = pair?.UP;
    const down = pair?.DOWN;
    if (!up || !down) return null;

    const marketPriorUp = clamp((up.features.mid + (1 - down.features.mid)) / 2, 0, 1);
    const bookPressureL1 = clamp((up.features.imbalanceL1 - down.features.imbalanceL1) / 2, -1, 1);
    const bookPressureL3 = clamp((up.features.imbalanceL3 - down.features.imbalanceL3) / 2, -1, 1);
    const bookPressureL5 = clamp((up.features.imbalanceL5 - down.features.imbalanceL5) / 2, -1, 1);
    const pairSignal = this.pairSignals.get(input.marketId);
    const pairObservedMs = Math.max(up.sourceMs, down.sourceMs);

    const referenceWindow = input.asset ? this.references.get(input.asset) : undefined;
    const referenceLatest = referenceWindow?.latest() ?? null;
    const shortMomentumBps = referenceWindow?.momentumBps(REFERENCE_SHORT_MS) ?? null;
    const mediumMomentumBps = referenceWindow?.momentumBps(REFERENCE_MEDIUM_MS) ?? null;
    const realizedVolatilityBps = referenceWindow?.realizedVolatilityBps(REFERENCE_MEDIUM_MS) ?? null;
    const volatilityNormalizedMovement = mediumMomentumBps !== null && realizedVolatilityBps !== null && realizedVolatilityBps > 0
      ? mediumMomentumBps / realizedVolatilityBps
      : null;
    const referenceSourceAgeMs = referenceLatest ? Math.max(0, nowMs - referenceLatest.sourceMs) : null;
    const endMs = input.endDate ? Date.parse(input.endDate) : Number.NaN;

    return {
      version: NATIVE_V2_SHADOW_FEATURE_VERSION,
      marketId: input.marketId,
      asset: input.asset,
      observedAt: input.now.toISOString(),
      up: { ...up.features },
      down: { ...down.features },
      pair: {
        marketPriorUp,
        pairAskCost: Number(up.snapshot.asks[0]!.price) + Number(down.snapshot.asks[0]!.price),
        binaryComplementBasis: up.features.mid + down.features.mid - 1,
        bookPressureL1,
        bookPressureL3,
        bookPressureL5,
        orderPressureVelocityPerSecond: pairSignal?.velocityPerSecond ?? null,
        signalDirection: pairSignal?.direction ?? directionOf(bookPressureL5),
        signalPersistenceMs: pairSignal ? Math.max(0, pairSignal.observedMs - pairSignal.sinceMs) : 0,
        signalPersistenceSamples: pairSignal?.samples ?? 1,
      },
      reference: {
        latestPrice: referenceLatest?.price ?? null,
        shortMomentumBps,
        mediumMomentumBps,
        realizedVolatilityBps,
        volatilityNormalizedMovement,
        sourceTimestamp: referenceLatest?.sourceTimestamp ?? null,
        sourceAgeMs: referenceSourceAgeMs,
        marketReferenceSourceSkewMs: referenceLatest ? pairObservedMs - referenceLatest.sourceMs : null,
      },
      timing: {
        timeToExpiryMs: Number.isFinite(endMs) ? endMs - nowMs : null,
        upSourceAgeMs: Math.max(0, nowMs - up.sourceMs),
        downSourceAgeMs: Math.max(0, nowMs - down.sourceMs),
        pairSourceSkewMs: Math.abs(up.sourceMs - down.sourceMs),
      },
      synchronization: {
        marketSynchronized: input.marketSynchronized,
        referenceFreshness: input.referenceFreshness,
        referenceFreshnessMs: input.referenceFreshnessMs,
      },
    };
  }

  dropMarket(marketId: string): void {
    this.books.delete(marketId);
    this.pairSignals.delete(marketId);
  }

  dropAsset(asset: string): void { this.references.delete(asset); }

  resetReference(asset: string): void { this.references.get(asset)?.clear(); }

  footprint(): { markets: number; pairSignals: number; referenceAssets: number; referenceBuckets: number } {
    let referenceBuckets = 0;
    for (const window of this.references.values()) referenceBuckets += window.size();
    return {
      markets: this.books.size,
      pairSignals: this.pairSignals.size,
      referenceAssets: this.references.size,
      referenceBuckets,
    };
  }

  private updatePairSignal(marketId: string, pair: Partial<Record<Outcome, StoredBook>>): void {
    const up = pair.UP;
    const down = pair.DOWN;
    if (!up || !down) return;
    const key = `${up.snapshot.sequence}:${down.snapshot.sequence}`;
    const previous = this.pairSignals.get(marketId);
    if (previous?.key === key) return;
    const observedMs = Math.max(up.sourceMs, down.sourceMs);
    if (previous && observedMs < previous.observedMs) return;
    const pressure = clamp((up.features.imbalanceL5 - down.features.imbalanceL5) / 2, -1, 1);
    const direction = directionOf(pressure);
    const deltaSeconds = previous && observedMs > previous.observedMs ? (observedMs - previous.observedMs) / 1_000 : null;
    const velocityPerSecond = deltaSeconds && deltaSeconds > 0 ? (pressure - previous!.pressure) / deltaSeconds : null;
    const sameDirection = previous && direction !== 0 && direction === previous.direction;
    this.pairSignals.set(marketId, {
      key,
      pressure,
      observedMs,
      direction,
      sinceMs: sameDirection ? previous.sinceMs : observedMs,
      samples: sameDirection ? previous.samples + 1 : 1,
      velocityPerSecond: finiteOrNull(velocityPerSecond),
    });
  }
}

function deriveBookFeatures(snapshot: MarketSnapshot, previous: StoredBook | null): NativeV2ShadowBookFeatures | null {
  const bestBid = snapshot.bids[0];
  const bestAsk = snapshot.asks[0];
  if (!bestBid || !bestAsk) return null;
  const bid = Number(bestBid.price);
  const ask = Number(bestAsk.price);
  const bidQty = Number(bestBid.quantity);
  const askQty = Number(bestAsk.quantity);
  if (![bid, ask, bidQty, askQty].every(Number.isFinite)) return null;
  if (bid < 0 || ask > 1 || ask < bid || bidQty < 0 || askQty < 0 || bidQty + askQty <= 0) return null;
  const mid = (bid + ask) / 2;
  if (!(mid > 0)) return null;
  const spread = ask - bid;
  const spreadBps = (spread / mid) * 10_000;
  const microprice = (bid * askQty + ask * bidQty) / (bidQty + askQty);
  const micropriceOffsetBps = ((microprice - mid) / mid) * 10_000;
  const imbalanceL1 = quantityImbalance(snapshot, 1);
  const imbalanceL3 = quantityImbalance(snapshot, 3);
  const imbalanceL5 = quantityImbalance(snapshot, BOOK_LEVELS);
  const bidDepthUsdL5 = depthNotional(snapshot.bids, BOOK_LEVELS);
  const askDepthUsdL5 = depthNotional(snapshot.asks, BOOK_LEVELS);
  const depthTotal = bidDepthUsdL5 + askDepthUsdL5;
  const depthAsymmetryL5 = depthTotal > 0 ? (bidDepthUsdL5 - askDepthUsdL5) / depthTotal : 0;
  const bidDepthSlope = normalizedQuantitySlope(snapshot.bids, BOOK_LEVELS);
  const askDepthSlope = normalizedQuantitySlope(snapshot.asks, BOOK_LEVELS);
  const depthSlopeAsymmetry = bidDepthSlope !== null && askDepthSlope !== null
    ? (bidDepthSlope - askDepthSlope) / 2
    : null;
  const sourceMs = Date.parse(snapshot.sourceTimestamp);
  const deltaSeconds = previous && sourceMs > previous.sourceMs ? (sourceMs - previous.sourceMs) / 1_000 : null;
  const spreadVelocityBpsPerSecond = deltaSeconds && deltaSeconds > 0
    ? (spreadBps - previous.features.spreadBps) / deltaSeconds
    : null;
  const midVelocityBpsPerSecond = deltaSeconds && deltaSeconds > 0 && previous.features.mid > 0
    ? (((mid / previous.features.mid) - 1) * 10_000) / deltaSeconds
    : null;

  return {
    mid,
    spread,
    spreadBps,
    microprice,
    micropriceOffsetBps,
    imbalanceL1,
    imbalanceL3,
    imbalanceL5,
    bidDepthUsdL5,
    askDepthUsdL5,
    depthAsymmetryL5,
    bidDepthSlope,
    askDepthSlope,
    depthSlopeAsymmetry,
    bidLiquidityConcentrationL1InL5: bidDepthUsdL5 > 0 ? Number(bestBid.price) * Number(bestBid.quantity) / bidDepthUsdL5 : 0,
    askLiquidityConcentrationL1InL5: askDepthUsdL5 > 0 ? Number(bestAsk.price) * Number(bestAsk.quantity) / askDepthUsdL5 : 0,
    spreadVelocityBpsPerSecond: finiteOrNull(spreadVelocityBpsPerSecond),
    midVelocityBpsPerSecond: finiteOrNull(midVelocityBpsPerSecond),
    sourceTimestamp: snapshot.sourceTimestamp,
    receivedAt: snapshot.receivedAt,
    sequence: snapshot.sequence,
  };
}

function quantityImbalance(snapshot: MarketSnapshot, levels: number): number {
  let bid = 0;
  let ask = 0;
  for (let i = 0; i < Math.min(levels, snapshot.bids.length); i += 1) bid += Number(snapshot.bids[i]!.quantity);
  for (let i = 0; i < Math.min(levels, snapshot.asks.length); i += 1) ask += Number(snapshot.asks[i]!.quantity);
  const total = bid + ask;
  return total > 0 && Number.isFinite(total) ? clamp((bid - ask) / total, -1, 1) : 0;
}

function depthNotional(levels: readonly { price: string; quantity: string }[], limit: number): number {
  let total = 0;
  for (let i = 0; i < Math.min(limit, levels.length); i += 1) {
    const price = Number(levels[i]!.price);
    const quantity = Number(levels[i]!.quantity);
    if (Number.isFinite(price) && Number.isFinite(quantity) && price >= 0 && quantity >= 0) total += price * quantity;
  }
  return total;
}

function normalizedQuantitySlope(levels: readonly { quantity: string }[], limit: number): number | null {
  const values: number[] = [];
  for (let i = 0; i < Math.min(limit, levels.length); i += 1) {
    const value = Number(levels[i]!.quantity);
    if (!Number.isFinite(value) || value < 0) return null;
    values.push(value);
  }
  if (values.length < 2) return null;
  const xMean = (values.length - 1) / 2;
  const yMean = values.reduce((sum, value) => sum + value, 0) / values.length;
  if (!(yMean > 0)) return 0;
  let numerator = 0;
  let denominator = 0;
  for (let i = 0; i < values.length; i += 1) {
    numerator += (i - xMean) * (values[i]! - yMean);
    denominator += (i - xMean) ** 2;
  }
  return denominator > 0 ? (numerator / denominator) / yMean : 0;
}

function directionOf(value: number): -1 | 0 | 1 { return value > 0 ? 1 : value < 0 ? -1 : 0; }
function finiteOrNull(value: number | null): number | null { return value !== null && Number.isFinite(value) ? value : null; }
function clamp(value: number, min: number, max: number): number { return Math.min(max, Math.max(min, value)); }
