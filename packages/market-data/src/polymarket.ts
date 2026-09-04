import { createPublicClient } from "@polymarket/client";
import {
  bookLevelSchema,
  createId,
  decimalStringSchema,
  feeScheduleSchema,
  impliedSpread,
  isDirectionalListing,
  qualifyMarket,
  qualificationConfigSchema,
  type BinaryNormalization,
  type MarketSnapshot,
  type QualificationConfig,
  type QualificationReasonCode,
  type QualificationResult,
} from "@pm/core";
import { abortableDelay, ExponentialBackoff } from "./reconnect.js";

export interface DiscoveredMarket {
  id: string;
  slug: string;
  question: string;
  title: string;
  asset: string | null;
  endDate: string | null;
  endDateSource?: string;
  remainingDurationMs?: number | null;
  active: boolean;
  tokens: Array<{ tokenId: string; outcome: "UP" | "DOWN"; price: string | null }>;
  feeSchedule: MarketSnapshot["feeSchedule"];
  liquidityUsd: string | null;
  spread: number | null;
  qualification: QualificationResult;
  raw: Record<string, unknown>;
}

export interface DirectionalMarketDiagnostic {
  slug: string;
  asset: string | null;
  parsedEndDate: string | null;
  endDateSource: string;
  remainingDurationMs: number | null;
  active: boolean;
  closed: boolean;
  acceptingOrders: boolean;
  binary: BinaryNormalization;
  liquidityUsd: string | null;
  spread: number | null;
  feeStatus: "known" | "unknown" | "zero";
  reasonCodes: QualificationReasonCode[];
}

export interface FeedConnectionEvent {
  feed: "polymarket" | "btc" | "reference";
  asset?: string;
  state: "CONNECTING" | "CONNECTED" | "DISCONNECTED" | "STALE" | "STOPPED";
  at: string;
  reconnectCount: number;
  message?: string;
}

export interface PolymarketFeedAnomaly {
  kind: "stale" | "malformed";
  tokenId: string;
  incomingMs: number | null;
  lastAcceptedMs: number | null;
  regressionMs: number | null;
  totalDropped: number;
  sampled: boolean;
  at: string;
}

export interface PolymarketTimestampStats {
  staleDropped: number;
  malformedDropped: number;
  diagnosticsEmitted: number;
  lastAnomaly: PolymarketFeedAnomaly | null;
}

export interface PolymarketFeedHandlers {
  onSnapshot(snapshot: MarketSnapshot): Promise<void> | void;
  onConnection(event: FeedConnectionEvent): Promise<void> | void;
  onError(error: Error): Promise<void> | void;
  onAnomaly?(anomaly: PolymarketFeedAnomaly): void;
}

/** Optional normalize observer. Must not change book/price_change semantics. */
export interface MarketNormalizeObserver {
  onNormalizedSnapshot(input: { durationSeconds: number; bidLevels: number; askLevels: number }): void;
  onPriceChangeEvent(entriesInPayload: number, processed: number): void;
}

export interface DiscoveryReport {
  discovered: DiscoveredMarket[];
  qualified: DiscoveredMarket[];
  rejected: Array<{ id: string; slug: string; title: string; reasonCodes: QualificationReasonCode[] }>;
}

type PublicClient = ReturnType<typeof createPublicClient>;

interface BookLevel {
  price: string;
  quantity: string;
}

interface BookState {
  marketId: string;
  bids: Map<string, string>;
  asks: Map<string, string>;
  bidLevels: BookLevel[];
  askLevels: BookLevel[];
  publishedBids: readonly BookLevel[] | null;
  publishedAsks: readonly BookLevel[] | null;
  bidsDirty: boolean;
  asksDirty: boolean;
  levelsValid: boolean;
  lastTradePrice: string | null;
  timestamp: string;
  hash: string | null;
}

export class PolymarketMarketDataAdapter {
  private readonly client: PublicClient;
  private readonly marketsByToken = new Map<string, DiscoveredMarket>();
  private readonly books = new Map<string, BookState>();
  private sequence = 0;
  private lastTimestampByToken = new Map<string, number>();
  private tokenIds: string[] = [];
  private closeCurrentStream: (() => void) | null = null;
  private tokenWaiters: Array<() => void> = [];
  private streamGeneration = 0;
  private activeStreams = 0;
  private readonly anomalySampler = new BoundedAnomalySampler();
  private lastAnomaly: PolymarketFeedAnomaly | null = null;
  private normalizeObserver: MarketNormalizeObserver | null = null;

  constructor(client: PublicClient = createPublicClient()) { this.client = client; }

  setNormalizeObserver(observer: MarketNormalizeObserver | null): void { this.normalizeObserver = observer; }

  getMarket(tokenId: string): DiscoveredMarket | undefined { return this.marketsByToken.get(tokenId); }
  getMarkets(): DiscoveredMarket[] { return uniqueMarkets([...this.marketsByToken.values()]); }
  timestampStats(): PolymarketTimestampStats {
    return {
      staleDropped: this.anomalySampler.staleDropped,
      malformedDropped: this.anomalySampler.malformedDropped,
      diagnosticsEmitted: this.anomalySampler.diagnosticsEmitted,
      lastAnomaly: this.lastAnomaly,
    };
  }
  lastAcceptedTimestamp(tokenId: string): number | undefined { return this.lastTimestampByToken.get(tokenId); }
  bookState(tokenId: string): { bids: Map<string, string>; asks: Map<string, string>; timestamp: string } | undefined {
    const state = this.books.get(tokenId);
    return state ? { bids: new Map(state.bids), asks: new Map(state.asks), timestamp: state.timestamp } : undefined;
  }
  activeStreamCount(): number { return this.activeStreams; }

  /** Same normalize path as the live feed. Intended for replay/equivalence tests. */
  normalizeSourceEvent(event: unknown, handlers: Pick<PolymarketFeedHandlers, "onAnomaly"> = {}): MarketSnapshot[] {
    return this.normalizeEvent(event, handlers);
  }

  async discoverCryptoMarkets(
    slugs: readonly string[] = [],
    maxMarkets = 40,
    qualification: Partial<QualificationConfig> = {},
    now = new Date(),
  ): Promise<DiscoveryReport> {
    const config = qualificationConfigSchema.parse({ ...qualification, maxQualifiedMarkets: maxMarkets });
    const rawMarkets: unknown[] = [];
    if (slugs.length > 0) {
      for (const slug of slugs) rawMarkets.push(await this.client.fetchMarket({ slug }));
    } else {
      rawMarkets.push(...await this.listCryptoEventMarkets(now, config));
    }
    const seen = new Map<string, string>();
    const discovered: DiscoveredMarket[] = [];
    const rejected: DiscoveryReport["rejected"] = [];
    for (const raw of rawMarkets) {
      const candidate = toQualificationInput(raw);
      if (!candidate) {
        rejected.push({ id: "unknown", slug: "unknown", title: "unknown", reasonCodes: ["UNSUPPORTED_STRUCTURE"] });
        continue;
      }
      const result = qualifyMarket(candidate.input, seen, now, config);
      const market = toDiscoveredMarket(candidate, result, now);
      discovered.push(market);
      if (result.qualified) {
        const tokenKey = market.tokens.map((token) => token.tokenId).sort().join(":");
        seen.set(tokenKey, market.id);
        seen.set(market.id, market.id);
        for (const token of market.tokens) this.marketsByToken.set(token.tokenId, market);
      } else {
        rejected.push({ id: market.id, slug: market.slug, title: market.title, reasonCodes: result.reasonCodes });
      }
    }
    discovered.sort((left, right) => (left.remainingDurationMs ?? Number.POSITIVE_INFINITY) - (right.remainingDurationMs ?? Number.POSITIVE_INFINITY));
    const qualified = discovered.filter((market) => market.qualification.qualified).slice(0, config.maxQualifiedMarkets);
    return { discovered, qualified, rejected };
  }

  async fetchPublicMarket(idOrSlug: string): Promise<Record<string, unknown> | null> {
    try {
      const raw = /^\d+$/.test(idOrSlug)
        ? await this.client.fetchMarket({ id: idOrSlug })
        : await this.client.fetchMarket({ slug: idOrSlug });
      return raw as Record<string, unknown>;
    } catch {
      return null;
    }
  }

  closestDirectional(report: DiscoveryReport, limit = 20): DirectionalMarketDiagnostic[] {
    return closestDirectionalDiagnostics(report, limit);
  }

  /** @deprecated Use discoverCryptoMarkets. */
  async discoverBitcoinMarkets(slugs: readonly string[] = [], maxMarkets = 20): Promise<DiscoveredMarket[]> {
    const report = await this.discoverCryptoMarkets(slugs, maxMarkets);
    return report.qualified;
  }

  private async listCryptoEventMarkets(now: Date, config: QualificationConfig): Promise<unknown[]> {
    const seen = new Set<string>();
    const markets: unknown[] = [];
    const paginator = this.client.listEvents({
      closed: false,
      tagSlug: "crypto",
      relatedTags: true,
      pageSize: 50,
      order: "endDate",
      ascending: true,
      endDateMin: new Date(now.getTime() - Math.min(60_000, config.minRemainingDurationMs)),
      endDateMax: new Date(now.getTime() + config.maxRemainingDurationMs),
    });
    let pages = 0;
    for await (const page of paginator) {
      for (const event of page.items as Array<Record<string, unknown>>) {
        const nested = Array.isArray(event.markets) ? event.markets as Array<Record<string, unknown>> : [];
        for (const market of nested) {
          const id = String(market.id ?? market.slug ?? "");
          if (!id || seen.has(id)) continue;
          seen.add(id);
          markets.push({ ...market, event, tags: [...asArray(market.tags), ...asArray(event.tags)] });
        }
      }
      pages += 1;
      if (pages >= 24 || !page.hasMore || markets.length >= 800) break;
    }
    return markets;
  }

  registerManualToken(tokenId: string, marketId = tokenId, outcome: "UP" | "DOWN" = "UP", asset: string | null = null): void {
    this.marketsByToken.set(tokenId, {
      id: marketId, slug: marketId, question: marketId, title: marketId, asset, endDate: null, endDateSource: "manual", remainingDurationMs: null, active: true,
      tokens: [{ tokenId, outcome, price: null }], feeSchedule: null, liquidityUsd: null, spread: null,
      qualification: {
        qualified: true, reasonCodes: ["QUALIFIED"], asset, crypto: true,
        binary: { safe: true, yesOutcome: "UP", noOutcome: "DOWN", reason: "QUALIFIED" }, duplicateOf: null,
      },
      raw: {},
    });
  }

  setTokenIds(tokenIds: readonly string[]): { changed: boolean; previous: string[]; next: string[] } {
    const previous = [...this.tokenIds];
    const next = uniqueStrings(tokenIds);
    const changed = !sameStringSet(previous, next);
    this.tokenIds = next;
    if (next.length > 0) {
      const waiters = this.tokenWaiters.splice(0);
      for (const waiter of waiters) waiter();
    }
    if (changed) {
      for (const tokenId of previous) {
        if (next.includes(tokenId)) continue;
        this.books.delete(tokenId);
        this.lastTimestampByToken.delete(tokenId);
        this.anomalySampler.forget(tokenId);
      }
      this.closeCurrentStream?.();
    }
    return { changed, previous, next };
  }

  async run(tokenIds: readonly string[], handlers: PolymarketFeedHandlers, signal: AbortSignal): Promise<void> {
    this.setTokenIds(this.tokenIds.length > 0 ? this.tokenIds : tokenIds);
    const backoff = new ExponentialBackoff({ initialDelayMs: 500, maxDelayMs: 30_000, multiplier: 2, jitterRatio: 0.2 });
    let reconnectCount = 0;
    const onAbort = () => { this.closeCurrentStream?.(); };
    if (signal.aborted) {
      await handlers.onConnection({ feed: "polymarket", state: "STOPPED", at: new Date().toISOString(), reconnectCount });
      return;
    }
    signal.addEventListener("abort", onAbort);
    try {
      while (!signal.aborted) {
        if (this.tokenIds.length === 0) {
          await handlers.onConnection({ feed: "polymarket", state: "STOPPED", at: new Date().toISOString(), reconnectCount, message: "NO_QUALIFIED_TOKENS" });
          try { await this.waitForTokens(signal); } catch { break; }
          continue;
        }
        await handlers.onConnection({ feed: "polymarket", state: "CONNECTING", at: new Date().toISOString(), reconnectCount });
        let resubscribed = false;
        const generation = ++this.streamGeneration;
        try {
          const current = [...this.tokenIds];
          if (current.length === 0) continue;
          const stream = await this.client.subscribe([{ topic: "market", tokenIds: current, customFeatureEnabled: true }]);
          this.activeStreams += 1;
          this.closeCurrentStream = () => {
            resubscribed = true;
            void stream.close();
          };
          if (signal.aborted) this.closeCurrentStream();
          try {
            backoff.reset();
            await handlers.onConnection({ feed: "polymarket", state: "CONNECTED", at: new Date().toISOString(), reconnectCount });
            for await (const event of stream) {
              if (signal.aborted) { await stream.close(); break; }
              if (resubscribed || generation !== this.streamGeneration) break;
              try {
                for (const snapshot of this.normalizeEvent(event, handlers)) {
                  if (resubscribed || generation !== this.streamGeneration) break;
                  await handlers.onSnapshot(snapshot);
                }
              } catch (error) {
                const normalized = error instanceof Error ? error : new Error(String(error));
                if (isTimestampDisorderError(normalized)) {
                  this.noteAnomaly(handlers, timestampDisorderAnomaly(normalized));
                  continue;
                }
                await handlers.onError(normalized);
              }
            }
          } finally {
            this.closeCurrentStream = null;
            this.activeStreams = Math.max(0, this.activeStreams - 1);
          }
          if (signal.aborted) break;
          if (resubscribed || this.tokenIds.length === 0 || generation !== this.streamGeneration) continue;
          throw new Error("POLYMARKET_STREAM_ENDED");
        } catch (error) {
          this.closeCurrentStream = null;
          if (signal.aborted) break;
          if (resubscribed || generation !== this.streamGeneration) continue;
          const normalized = error instanceof Error ? error : new Error(String(error));
          if (isTimestampDisorderError(normalized)) {
            this.noteAnomaly(handlers, timestampDisorderAnomaly(normalized));
            continue;
          }
          reconnectCount += 1;
          await handlers.onError(normalized);
          await handlers.onConnection({ feed: "polymarket", state: "DISCONNECTED", at: new Date().toISOString(), reconnectCount, message: normalized.message });
          await abortableDelay(backoff.nextDelay(), signal).catch(() => undefined);
        }
      }
      this.closeCurrentStream = null;
      this.activeStreams = 0;
      await this.client.closeSubscriptions();
      await handlers.onConnection({ feed: "polymarket", state: "STOPPED", at: new Date().toISOString(), reconnectCount });
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
  }

  private waitForTokens(signal: AbortSignal): Promise<void> {
    if (this.tokenIds.length > 0) return Promise.resolve();
    if (signal.aborted) return Promise.reject(signal.reason);
    return new Promise((resolve, reject) => {
      const finish = () => { cleanup(); resolve(); };
      const onAbort = () => { cleanup(); reject(signal.reason); };
      const cleanup = () => {
        signal.removeEventListener("abort", onAbort);
        this.tokenWaiters = this.tokenWaiters.filter((waiter) => waiter !== finish);
      };
      signal.addEventListener("abort", onAbort, { once: true });
      this.tokenWaiters.push(finish);
    });
  }

  private normalizeEvent(event: unknown, handlers: Pick<PolymarketFeedHandlers, "onAnomaly">): MarketSnapshot[] {
    const value = event as { type?: string; payload?: Record<string, unknown> };
    const payload = value.payload ?? {};
    if (value.type === "book") {
      const tokenId = String(payload.tokenId ?? "");
      const market = this.marketsByToken.get(tokenId);
      if (!tokenId || !market || !this.tokenIds.includes(tokenId)) return [];
      const sourceMs = parseSourceTimestamp(payload.timestamp);
      if (!this.acceptTimestamp(tokenId, sourceMs, handlers)) return [];
      const acceptedMs = sourceMs as number;
      const bids = levelsToMap(payload.bids);
      const asks = levelsToMap(payload.asks);
      const started = performance.now();
      const state: BookState = {
        marketId: String(payload.market ?? market.id),
        bids,
        asks,
        bidLevels: sortedFrozenLevels(bids, "desc"),
        askLevels: sortedFrozenLevels(asks, "asc"),
        publishedBids: null,
        publishedAsks: null,
        bidsDirty: true,
        asksDirty: true,
        levelsValid: false,
        lastTradePrice: nullableString(payload.lastTradePrice),
        timestamp: new Date(acceptedMs).toISOString(),
        hash: nullableString(payload.hash),
      };
      this.books.set(tokenId, state);
      return [this.toSnapshot(tokenId, state, payload, undefined, started)];
    }
    if (value.type === "price_change") {
      const changes = Array.isArray(payload.priceChanges) ? payload.priceChanges as Array<Record<string, unknown>> : [];
      const snapshots: MarketSnapshot[] = [];
      for (const change of changes) {
        const tokenId = String(change.tokenId ?? "");
        const state = this.books.get(tokenId);
        if (!state || !this.tokenIds.includes(tokenId)) continue;
        const sourceMs = parseSourceTimestamp(payload.timestamp);
        if (!this.acceptTimestamp(tokenId, sourceMs, handlers)) continue;
        const acceptedMs = sourceMs as number;
        const side = String(change.side);
        const price = String(change.price ?? "");
        const size = String(change.size ?? "0");
        const started = performance.now();
        const changed = side === "BUY"
          ? applyLevelChange(state.bids, state.bidLevels, price, size, "desc")
          : applyLevelChange(state.asks, state.askLevels, price, size, "asc");
        if (changed) {
          if (side === "BUY") {
            state.bidsDirty = true;
          } else {
            state.asksDirty = true;
          }
        }
        state.timestamp = new Date(acceptedMs).toISOString();
        state.hash = nullableString(change.hash) ?? state.hash;
        const changedLevel = Number(size) <= 0 ? null : { price, quantity: size };
        snapshots.push(this.toSnapshot(tokenId, state, payload, changedLevel, started));
      }
      this.normalizeObserver?.onPriceChangeEvent(changes.length, snapshots.length);
      return snapshots;
    }
    if (value.type === "last_trade_price") {
      const tokenId = String(payload.tokenId ?? "");
      const state = this.books.get(tokenId);
      if (state) state.lastTradePrice = nullableString(payload.price);
    }
    return [];
  }

  private acceptTimestamp(tokenId: string, sourceMs: number | null, handlers: Pick<PolymarketFeedHandlers, "onAnomaly">): boolean {
    if (sourceMs === null) {
      this.noteAnomaly(handlers, {
        kind: "malformed", tokenId, incomingMs: null, lastAcceptedMs: this.lastTimestampByToken.get(tokenId) ?? null,
        regressionMs: null,
      });
      return false;
    }
    const previous = this.lastTimestampByToken.get(tokenId);
    if (previous !== undefined && sourceMs < previous) {
      this.noteAnomaly(handlers, {
        kind: "stale", tokenId, incomingMs: sourceMs, lastAcceptedMs: previous, regressionMs: previous - sourceMs,
      });
      return false;
    }
    this.lastTimestampByToken.set(tokenId, sourceMs);
    return true;
  }

  private noteAnomaly(handlers: Pick<PolymarketFeedHandlers, "onAnomaly">, partial: Omit<PolymarketFeedAnomaly, "sampled" | "totalDropped" | "at">): void {
    const sampled = this.anomalySampler.observe(partial);
    const anomaly: PolymarketFeedAnomaly = {
      ...partial,
      sampled,
      totalDropped: this.anomalySampler.totalDropped,
      at: new Date().toISOString(),
    };
    this.lastAnomaly = anomaly;
    handlers.onAnomaly?.(anomaly);
  }

  private toSnapshot(
    tokenId: string,
    state: BookState,
    raw: Record<string, unknown>,
    changedLevel?: BookLevel | null,
    started = performance.now(),
  ): MarketSnapshot {
    const market = this.marketsByToken.get(tokenId);
    if (!market) throw new Error("UNKNOWN_TOKEN");
    const token = market.tokens.find((item) => item.tokenId === tokenId);
    const bids = publishSide(state, "bids");
    const asks = publishSide(state, "asks");
    const tickSize = nullableString(raw.tickSize);
    const minOrderSize = nullableString(raw.minOrderSize);
    let feeSchedule: MarketSnapshot["feeSchedule"] = null;
    try {
      validateNormalizedBook(state, changedLevel);
      assertDecimalString(state.lastTradePrice);
      assertDecimalString(tickSize);
      assertDecimalString(minOrderSize);
      feeSchedule = market.feeSchedule == null ? null : feeScheduleSchema.parse(market.feeSchedule);
      state.levelsValid = true;
    } catch (error) {
      state.levelsValid = false;
      throw error;
    }
    const snapshot: MarketSnapshot = {
      eventId: createId("poly"),
      traceId: createId("trace"),
      marketId: market.id,
      tokenId,
      outcome: token?.outcome ?? "UP",
      asset: market.asset,
      bids,
      asks,
      lastTradePrice: state.lastTradePrice,
      tickSize,
      minOrderSize,
      feeSchedule,
      sourceTimestamp: state.timestamp,
      receivedAt: new Date().toISOString(),
      sequence: ++this.sequence,
      hash: state.hash,
    };
    this.normalizeObserver?.onNormalizedSnapshot({
      durationSeconds: (performance.now() - started) / 1000,
      bidLevels: state.bids.size,
      askLevels: state.asks.size,
    });
    return snapshot;
  }
}

interface ParsedCandidate {
  input: Parameters<typeof qualifyMarket>[0];
  feeSchedule: MarketSnapshot["feeSchedule"];
  raw: Record<string, unknown>;
  endDateSource: string;
}

function toQualificationInput(raw: unknown): ParsedCandidate | null {
  const value = raw as Record<string, unknown>;
  const outcomes = value.outcomes as { yes?: Record<string, unknown>; no?: Record<string, unknown> } | undefined;
  const yesToken = nullableString(outcomes?.yes?.tokenId);
  const noToken = nullableString(outcomes?.no?.tokenId);
  if (!yesToken || !noToken) {
    const clob = Array.isArray(value.clobTokenIds) ? value.clobTokenIds.map(String) : [];
    if (clob.length !== 2) return null;
  }
  const state = value.state as Record<string, unknown> | undefined;
  const trading = value.trading as Record<string, unknown> | undefined;
  const metrics = value.metrics as Record<string, unknown> | undefined;
  const prices = value.prices as Record<string, unknown> | undefined;
  const rawFee = (trading?.feeSchedule ?? value.feeSchedule) as Record<string, unknown> | null | undefined;
  const feesEnabled = trading?.feesEnabled === true || value.feesEnabled === true;
  const feeSchedule = feesEnabled && rawFee ? {
    rate: String(rawFee.rate), exponent: Number(rawFee.exponent), takerOnly: Boolean(rawFee.takerOnly),
    rebateRate: String(rawFee.rebateRate ?? 0), source: "market_fee_schedule" as const, effectiveAt: new Date().toISOString(),
  } : feesEnabled && !rawFee ? null : {
    rate: "0", exponent: 1, takerOnly: true, rebateRate: "0", source: "market_fee_schedule" as const, effectiveAt: new Date().toISOString(),
  };
  const tags = collectTags(value);
  const configAsset = cryptoConfigAsset(value);
  if (configAsset) tags.push(configAsset);
  const yesPrice = nullableString(outcomes?.yes?.price) ?? firstPrice(value, 0);
  const noPrice = nullableString(outcomes?.no?.price) ?? firstPrice(value, 1);
  const liquidity = nullableString(metrics?.liquidityNum ?? metrics?.liquidity ?? trading?.liquidity ?? value.liquidityNum ?? value.liquidity);
  const acceptingOrders = state?.acceptingOrders !== false && value.acceptingOrders !== false && state?.enableOrderBook !== false;
  const resolvedEnd = resolveMarketEndDate(value);
  return {
    feeSchedule,
    raw: value,
    input: {
      id: String(value.id ?? value.conditionId ?? value.slug),
      slug: String(value.slug ?? value.id ?? ""),
      title: String(value.question ?? value.title ?? value.slug ?? ""),
      tags,
      active: state?.active === true || value.active === true,
      closed: state?.closed === true || value.closed === true,
      acceptingOrders,
      endDate: resolvedEnd.iso,
      yesTokenId: yesToken ?? (Array.isArray(value.clobTokenIds) ? String((value.clobTokenIds as unknown[])[0]) : null),
      noTokenId: noToken ?? (Array.isArray(value.clobTokenIds) ? String((value.clobTokenIds as unknown[])[1]) : null),
      yesPrice, noPrice, liquidityUsd: liquidity, spread: numberOrNull(prices?.spread) ?? impliedSpread(yesPrice, noPrice) ?? numberOrNull(value.spread),
      feeSchedule, feesUnknown: feesEnabled && rawFee == null,
      yesLabel: outcomeLabel(value, 0, outcomes?.yes), noLabel: outcomeLabel(value, 1, outcomes?.no),
    },
    endDateSource: resolvedEnd.source,
  };
}

function toDiscoveredMarket(candidate: ParsedCandidate, qualification: QualificationResult, now = new Date()): DiscoveredMarket {
  const input = candidate.input;
  const yesOutcome = qualification.binary.yesOutcome ?? "UP";
  const noOutcome = qualification.binary.noOutcome ?? "DOWN";
  const remaining = input.endDate ? new Date(input.endDate).getTime() - now.getTime() : null;
  return {
    id: input.id, slug: input.slug, question: input.title, title: input.title, asset: qualification.asset,
    endDate: input.endDate, endDateSource: candidate.endDateSource,
    remainingDurationMs: remaining !== null && Number.isFinite(remaining) ? remaining : null,
    active: input.active && !input.closed,
    tokens: [
      { tokenId: input.yesTokenId ?? "", outcome: yesOutcome, price: input.yesPrice },
      { tokenId: input.noTokenId ?? "", outcome: noOutcome, price: input.noPrice },
    ],
    feeSchedule: candidate.feeSchedule, liquidityUsd: input.liquidityUsd, spread: input.spread,
    qualification, raw: candidate.raw,
  };
}

function collectTags(value: Record<string, unknown>): string[] {
  const tags: string[] = [];
  const push = (item: unknown) => {
    if (typeof item === "string") tags.push(item);
    else if (item && typeof item === "object") {
      const record = item as Record<string, unknown>;
      if (typeof record.slug === "string") tags.push(record.slug);
      if (typeof record.label === "string") tags.push(record.label);
    }
  };
  if (Array.isArray(value.tags)) value.tags.forEach(push);
  if (Array.isArray(value.events)) {
    for (const event of value.events as Array<Record<string, unknown>>) {
      if (Array.isArray(event.tags)) event.tags.forEach(push);
    }
  }
  const event = value.event as Record<string, unknown> | undefined;
  if (event && Array.isArray(event.tags)) event.tags.forEach(push);
  return tags;
}

function uniqueStrings(values: readonly string[]): string[] {
  return [...new Set(values.filter(Boolean))];
}
function sameStringSet(left: readonly string[], right: readonly string[]): boolean {
  if (left.length !== right.length) return false;
  const set = new Set(left);
  return right.every((value) => set.has(value));
}
function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}
function uniqueMarkets(markets: DiscoveredMarket[]): DiscoveredMarket[] {
  const map = new Map<string, DiscoveredMarket>();
  for (const market of markets) map.set(market.id, market);
  return [...map.values()];
}

function nullableString(value: unknown): string | null { return value === undefined || value === null || value === "" ? null : String(value); }
function numberOrNull(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}
function outcomeLabel(value: Record<string, unknown>, index: number, named: Record<string, unknown> | undefined): string | null {
  const fromNamed = nullableString(named?.name ?? named?.outcome ?? named?.label);
  if (fromNamed) return fromNamed;
  const outcomes = value.outcomes;
  if (Array.isArray(outcomes) && typeof outcomes[index] === "string") return outcomes[index];
  if (typeof outcomes === "string") {
    try {
      const parsed = JSON.parse(outcomes) as unknown;
      if (Array.isArray(parsed) && typeof parsed[index] === "string") return parsed[index];
    } catch { return null; }
  }
  return null;
}
function firstPrice(value: Record<string, unknown>, index: number): string | null {
  const prices = value.outcomePrices;
  if (Array.isArray(prices) && prices[index] !== undefined) return String(prices[index]);
  if (typeof prices === "string") {
    try {
      const parsed = JSON.parse(prices) as unknown;
      if (Array.isArray(parsed) && parsed[index] !== undefined) return String(parsed[index]);
    } catch { return null; }
  }
  return null;
}
function parseSourceTimestamp(value: unknown): number | null {
  if (value === undefined || value === null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

const TIMESTAMP_DISORDER_RE = /^EVENT_TIMESTAMP_DISORDER:([^:]+):(\d+)<(\d+)$/;

function isTimestampDisorderError(error: Error): boolean {
  return error.message.startsWith("EVENT_TIMESTAMP_DISORDER:");
}

function timestampDisorderAnomaly(error: Error): Omit<PolymarketFeedAnomaly, "sampled" | "totalDropped" | "at"> {
  const match = error.message.match(TIMESTAMP_DISORDER_RE);
  const tokenId = match?.[1] ?? "unknown";
  const incomingMs = match ? Number(match[2]) : null;
  const lastAcceptedMs = match ? Number(match[3]) : null;
  return {
    kind: "stale",
    tokenId,
    incomingMs: Number.isFinite(incomingMs) ? incomingMs : null,
    lastAcceptedMs: Number.isFinite(lastAcceptedMs) ? lastAcceptedMs : null,
    regressionMs: incomingMs != null && lastAcceptedMs != null && Number.isFinite(incomingMs) && Number.isFinite(lastAcceptedMs)
      ? lastAcceptedMs - incomingMs
      : null,
  };
}

class BoundedAnomalySampler {
  staleDropped = 0;
  malformedDropped = 0;
  diagnosticsEmitted = 0;
  private readonly perToken = new Map<string, number>();
  private lastEmitAt = 0;
  private readonly firstN = 3;
  private readonly everyN = 250;
  private readonly minIntervalMs = 5_000;
  private readonly maxTokenKeys = 256;

  get totalDropped(): number { return this.staleDropped + this.malformedDropped; }

  observe(partial: Omit<PolymarketFeedAnomaly, "sampled" | "totalDropped" | "at">): boolean {
    if (partial.kind === "stale") this.staleDropped += 1;
    else this.malformedDropped += 1;
    const count = (this.perToken.get(partial.tokenId) ?? 0) + 1;
    if (this.perToken.size >= this.maxTokenKeys && !this.perToken.has(partial.tokenId)) {
      const first = this.perToken.keys().next().value;
      if (first !== undefined) this.perToken.delete(first);
    }
    this.perToken.set(partial.tokenId, count);
    const large = (partial.regressionMs ?? 0) >= 1_000 || partial.kind === "malformed";
    const now = Date.now();
    const emit = count <= this.firstN
      || this.totalDropped % this.everyN === 0
      || (large && now - this.lastEmitAt >= this.minIntervalMs)
      || now - this.lastEmitAt >= this.minIntervalMs * 6;
    if (!emit) return false;
    this.lastEmitAt = now;
    this.diagnosticsEmitted += 1;
    return true;
  }

  forget(tokenId: string): void { this.perToken.delete(tokenId); }
}
function levelsToMap(value: unknown): Map<string, string> {
  const result = new Map<string, string>();
  if (!Array.isArray(value)) return result;
  for (const level of value as Array<Record<string, unknown>>) result.set(String(level.price), String(level.size ?? level.quantity));
  return result;
}

function freezeLevel(price: string, quantity: string): BookLevel {
  return Object.freeze({ price, quantity });
}

function sortedFrozenLevels(map: Map<string, string>, order: "asc" | "desc"): BookLevel[] {
  return [...map.entries()]
    .sort(([left], [right]) => order === "asc" ? Number(left) - Number(right) : Number(right) - Number(left))
    .map(([price, quantity]) => freezeLevel(price, quantity));
}

function findExactPriceIndex(levels: readonly BookLevel[], price: string, order: "asc" | "desc"): number {
  const numeric = Number(price);
  let lo = 0;
  let hi = levels.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    const midPrice = Number(levels[mid]!.price);
    if (order === "desc" ? midPrice > numeric : midPrice < numeric) lo = mid + 1;
    else hi = mid;
  }
  for (let index = lo; index < levels.length && Number(levels[index]!.price) === numeric; index += 1) {
    if (levels[index]!.price === price) return index;
  }
  return -1;
}

function insertIndex(levels: readonly BookLevel[], price: string, order: "asc" | "desc"): number {
  const numeric = Number(price);
  let lo = 0;
  let hi = levels.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    const midPrice = Number(levels[mid]!.price);
    if (order === "desc" ? midPrice >= numeric : midPrice <= numeric) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

function applyLevelChange(
  map: Map<string, string>,
  levels: BookLevel[],
  price: string,
  size: string,
  order: "asc" | "desc",
): boolean {
  if (Number(size) <= 0) {
    if (!map.has(price)) return false;
    map.delete(price);
    const index = findExactPriceIndex(levels, price, order);
    if (index >= 0) levels.splice(index, 1);
    return true;
  }
  const existing = findExactPriceIndex(levels, price, order);
  if (existing >= 0) {
    map.set(price, size);
    levels[existing] = freezeLevel(price, size);
    return true;
  }
  map.set(price, size);
  levels.splice(insertIndex(levels, price, order), 0, freezeLevel(price, size));
  return true;
}

function publishSide(state: BookState, side: "bids" | "asks"): BookLevel[] {
  if (side === "bids") {
    if (!state.bidsDirty && state.publishedBids) return state.publishedBids as BookLevel[];
    const published = Object.freeze(state.bidLevels.slice()) as BookLevel[];
    state.publishedBids = published;
    state.bidsDirty = false;
    return published;
  }
  if (!state.asksDirty && state.publishedAsks) return state.publishedAsks as BookLevel[];
  const published = Object.freeze(state.askLevels.slice()) as BookLevel[];
  state.publishedAsks = published;
  state.asksDirty = false;
  return published;
}

const DECIMAL_RE = /^-?\d+(?:\.\d+)?$/;

function assertBookLevel(level: BookLevel): void {
  if (DECIMAL_RE.test(level.price) && DECIMAL_RE.test(level.quantity) && Number(level.quantity) >= 0) return;
  bookLevelSchema.parse(level);
}

function assertDecimalString(value: string | null): void {
  if (value == null || DECIMAL_RE.test(value)) return;
  decimalStringSchema.parse(value);
}

function validateNormalizedBook(state: BookState, changedLevel?: BookLevel | null): void {
  if (!state.levelsValid) {
    for (const level of state.bidLevels) assertBookLevel(level);
    for (const level of state.askLevels) assertBookLevel(level);
    return;
  }
  if (changedLevel) assertBookLevel(changedLevel);
}

const SLUG_WINDOW = /(?:^|-)([a-z0-9]{2,16})-updown-(\d+)([mh])-(\d{10,})(?:-|$)/i;

export function resolveMarketEndDate(value: Record<string, unknown>): { iso: string | null; source: string } {
  const event = value.event && typeof value.event === "object" ? value.event as Record<string, unknown> : undefined;
  const state = value.state && typeof value.state === "object" ? value.state as Record<string, unknown> : undefined;
  const eventState = event?.state && typeof event.state === "object" ? event.state as Record<string, unknown> : undefined;
  const schedule = value.schedule && typeof value.schedule === "object" ? value.schedule as Record<string, unknown> : undefined;
  const eventSchedule = event?.schedule && typeof event.schedule === "object" ? event.schedule as Record<string, unknown> : undefined;
  const candidates: Array<[unknown, string]> = [
    [state?.endDate, "state.endDate"],
    [value.endDate, "endDate"],
    [schedule?.endDate, "schedule.endDate"],
    [eventState?.endDate, "event.state.endDate"],
    [event?.endDate, "event.endDate"],
    [eventSchedule?.endDate, "event.schedule.endDate"],
  ];
  for (const [raw, source] of candidates) {
    const iso = coerceDateTime(raw);
    if (iso) return { iso, source };
  }
  const slug = String(value.slug ?? event?.slug ?? "");
  const fromSlug = endDateFromDirectionalSlug(slug);
  if (fromSlug) return fromSlug;
  return { iso: null, source: "missing" };
}

function coerceDateTime(value: unknown): string | null {
  if (value instanceof Date && Number.isFinite(value.getTime())) return value.toISOString();
  if (typeof value !== "string" || value.trim().length === 0) return null;
  const text = value.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return null;
  const parsed = Date.parse(text);
  if (!Number.isFinite(parsed)) return null;
  return new Date(parsed).toISOString();
}

export function endDateFromDirectionalSlug(slug: string): { iso: string; source: "slug_window" } | null {
  const match = slug.match(SLUG_WINDOW);
  if (!match) return null;
  const amount = Number(match[2]);
  const unit = match[3]?.toLowerCase();
  const startUnix = Number(match[4]);
  if (!unit || !Number.isFinite(amount) || amount <= 0 || !Number.isFinite(startUnix)) return null;
  const durationSeconds = unit === "h" ? amount * 3_600 : amount * 60;
  if (durationSeconds <= 0 || startUnix % durationSeconds !== 0) return null;
  return { iso: new Date((startUnix + durationSeconds) * 1_000).toISOString(), source: "slug_window" };
}

function cryptoConfigAsset(value: Record<string, unknown>): string | null {
  const direct = value.cryptoMarketConfig;
  const nested = value.event && typeof value.event === "object" ? (value.event as Record<string, unknown>).cryptoMarketConfig : undefined;
  for (const raw of [direct, nested]) {
    if (!raw || typeof raw !== "object") continue;
    const asset = nullableString((raw as Record<string, unknown>).asset);
    if (asset) return asset.toUpperCase();
  }
  return null;
}

export function closestDirectionalDiagnostics(report: DiscoveryReport, limit = 20): DirectionalMarketDiagnostic[] {
  return report.discovered
    .filter((market) => isDirectionalListing(market.slug, market.title) || isDirectionalListing(market.slug, market.question))
    .toSorted((left, right) => (left.remainingDurationMs ?? Number.POSITIVE_INFINITY) - (right.remainingDurationMs ?? Number.POSITIVE_INFINITY))
    .slice(0, limit)
    .map((market) => ({
      slug: market.slug,
      asset: market.asset,
      parsedEndDate: market.endDate,
      endDateSource: market.endDateSource ?? "unknown",
      remainingDurationMs: market.remainingDurationMs ?? null,
      active: market.active,
      closed: market.qualification.reasonCodes.includes("MARKET_CLOSED"),
      acceptingOrders: market.active && !market.qualification.reasonCodes.includes("MARKET_INACTIVE"),
      binary: market.qualification.binary,
      liquidityUsd: market.liquidityUsd,
      spread: market.spread,
      feeStatus: market.qualification.reasonCodes.includes("UNKNOWN_FEE_SCHEDULE")
        ? "unknown"
        : market.feeSchedule && Number(market.feeSchedule.rate) === 0 ? "zero" : "known",
      reasonCodes: market.qualification.reasonCodes,
    }));
}
