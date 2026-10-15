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
import { BoundedClobMarketTransport, ClobMarketCloseError } from "./bounded-clob-market.js";
import { BoundedIngressQueue, IngressOverflowError, type IngressQueueObserver } from "./bounded-ingress.js";
import {
  CLOB_MARKET_WS_URL,
  MARKET_CONNECT_BOUNDS,
  MARKET_DRAIN_BOUNDS,
  type IngressGapReason,
  type IngressQueueSnapshot,
  type TokenSyncState,
} from "./ingress-bounds.js";

export class MarketIngressMalformedError extends Error {
  readonly reason = "malformed" as const;
  constructor(readonly tokenIds: readonly string[] = []) {
    super("MARKET_INGRESS_MALFORMED");
    this.name = "MarketIngressMalformedError";
  }
}
import { isBoundedMarketStreamTransport, type MarketStreamTransport } from "./market-stream-transport.js";
import { abortableDelay, ExponentialBackoff } from "./reconnect.js";
import { TokenSyncIndex } from "./token-sync.js";

/**
 * Resolves the authoritative minimum order size for a token, or null/undefined
 * when unknown or unavailable.
 *
 * Production source: the CLOB REST order book (PublicClient.fetchOrderBook),
 * whose official OrderBook schema REQUIRES `min_order_size` — unlike the WS
 * MarketBookEventSchema, where the field is OPTIONAL. Never a per-price_change
 * call: hydration is triggered only once per token when a trusted WS full book
 * omits the field, with a bounded backoff and per-epoch attempt cap.
 */
export type MinOrderSizeFetcher = (tokenId: string) => Promise<string | null> | string | null;

/**
 * Per-token minOrderSize hydration lifecycle state. The map of these objects
 * is bounded by currently managed tokens; entries are deleted on token removal
 * (stale in-flight requests hold only captured scalar identity — lifecycle,
 * epoch, flight nonce — so a deleted/new state can never be confused). The
 * lifecycle version is advanced by every gap/malformed/reconnect invalidation;
 * a fresh epoch request therefore always carries a different identity.
 */
interface TokenMinOrderSizeHydrationState {
  lifecycle: number;
  /** Global nonce of the currently running physical request (0 when idle). */
  flightId: number;
  /** Failed attempts within the current lifecycle. */
  attempts: number;
  /** Earliest autonomous retry timestamp (minOrderSizeClock ms). */
  retryAt: number;
  /** True while the token is removed but a stale flight still holds identity. */
  removed: boolean;
}

/**
 * Scalar identity captured by a physical hydration request. Stale requests hold
 * these values (never map entries), so lifecycle/epoch invalidation or token
 * removal can never be confused with a newer request.
 */
interface MinOrderSizeHydrationIdentity {
  tokenId: string;
  lifecycle: number;
  epoch: number;
  flightId: number;
}

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
  /** This attempt follows an unexpected stream failure, not discovery resubscription. */
  reconnecting?: boolean;
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

export interface MarketIngressGapEvent {
  reason: IngressGapReason;
  tokenCount: number;
  epoch: number;
  /** Affected tokens for engine invalidation. Never a metric label. */
  tokenIds: string[];
}

export interface MarketIngressResyncEvent {
  reason: IngressGapReason | "full_book";
  tokenId: string;
  epoch: number;
}

export interface MinOrderSizeHydratedEvent {
  tokenId: string;
  minOrderSize: string;
  tokenEpoch: number;
}

export interface PolymarketFeedHandlers {
  onSnapshot(snapshot: MarketSnapshot): Promise<void> | void;
  onConnection(event: FeedConnectionEvent): Promise<void> | void;
  /** Raw owned-shard lifecycle; independent of aggregate UI connection state. */
  onShardConnection?(event: FeedConnectionEvent & { shard: number; eventId: string }): Promise<void> | void;
  onError(error: Error): Promise<void> | void;
  onAnomaly?(anomaly: PolymarketFeedAnomaly): void;
  onGap?(event: MarketIngressGapEvent): void;
  onResync?(event: MarketIngressResyncEvent): void;
  /**
   * Narrow metadata-only notification when the authoritative min order size for
   * a token becomes known via REST hydration. NOT a market event: consumers
   * must not treat it as a new book (no sequence, no triggers, no maker/shadow
   * ingest). Consumers patch only the cached snapshot's minOrderSize.
   */
  onMinOrderSizeHydrated?(event: MinOrderSizeHydratedEvent): Promise<void> | void;
}

/**
 * Bounded, label-free observability for REST minOrderSize hydration. Counters
 * are low-cardinality (no tokenId/marketId labels) so the metrics registry can
 * prove production hydration occurred without per-book spam.
 */
export interface MinOrderSizeHydrationObserver {
  onHydrationAttempt(): void;
  onHydrationSuccess(): void;
  onHydrationFailure(): void;
  onHydrationStale(): void;
  onHydrationApplied(): void;
  onHydrationInFlightChanged(count: number): void;
}

/** Optional normalize observer. Must not change book/price_change semantics. */
export interface MarketNormalizeObserver {
  onNormalizedSnapshot(input: { durationSeconds: number; bidLevels: number; askLevels: number }): void;
  onPriceChangeEvent(entriesInPayload: number, processed: number): void;
  onSourceEventNormalized?(durationSeconds: number): void;
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

/**
 * REST min_order_size hydration bounds:
 * - at most one in-flight request per token,
 * - at most MAX_ATTEMPTS requests per stream epoch (a reconnect/resync resets),
 * - at least RETRY_MS between failed attempts (no unbounded retries),
 * - never triggered by price_change events (only by trusted full-book commits
 *   that omitted the field).
 */
const MIN_ORDER_SIZE_REST_MAX_ATTEMPTS = 5;
const MIN_ORDER_SIZE_REST_RETRY_MS = 60_000;

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
  /** Working maps/arrays are unique to this object and safe to mutate. */
  bidsOwned: boolean;
  asksOwned: boolean;
  levelsValid: boolean;
  lastTradePrice: string | null;
  /**
   * Authoritative minimum order size for the token. Preferred sources, in
   * order: the CLOB full-book event's `min_order_size` (WS MarketBookEventSchema
   * makes it optional), then a bounded REST order-book hydration when a trusted
   * full book omits it (REST OrderBookSchema requires the field). Price-change
   * events never carry it, so the committed value is carried forward in BookState.
   */
  minOrderSize: string | null;
  timestamp: string;
  hash: string | null;
}

export class PolymarketMarketDataAdapter {
  private readonly client: PublicClient;
  private readonly marketsByToken = new Map<string, DiscoveredMarket>();
  private readonly books = new Map<string, BookState>();
  /**
   * Token metadata cache intentionally outlives book/stream epochs. A stream
   * gap invalidates price/depth trust, but it does not change token identity.
   * Full books may omit min_order_size, so deleting BookState must not erase a
   * value already learned authoritatively from WS or REST for the same token.
   */
  private readonly minOrderSizeByToken = new Map<string, string>();
  private readonly minOrderSizeFetcher: MinOrderSizeFetcher | null;
  private readonly minOrderSizeClock: () => number;
  private readonly minOrderSizeRetryIntervalMs: number;
  private readonly minOrderSizeMaxAttempts: number;
  /**
   * Bounded per-current-token hydration lifecycle state (one physical in-flight
   * request per token at any instant). Deleted on token removal; stale request
   * identities hold scalars, never map entries.
   */
  private readonly minOrderSizeHydrationState = new Map<string, TokenMinOrderSizeHydrationState>();
  /** Global monotonic request identity nonce; never reused. */
  private minOrderSizeHydrationFlightSeq = 0;
  private minOrderSizeHydrationRetryTimer: ReturnType<typeof setInterval> | null = null;
  private minOrderSizeHydrationShutdown = false;
  private minOrderSizeFetcherCalls = 0;
  private minOrderSizeHydrationInFlightCount = 0;
  private minOrderSizeHydrationObserver: MinOrderSizeHydrationObserver | null = null;
  /** Metadata-only consumer attached by the current run() loop; cleared at exit. */
  private streamOnMinOrderSizeHydrated: PolymarketFeedHandlers["onMinOrderSizeHydrated"] | undefined;
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
  private readonly sync = new TokenSyncIndex();
  readonly ingress: BoundedIngressQueue;
  private readonly transport: MarketStreamTransport;
  /** Trusted full book observed on the CURRENT stream attempt only. */
  private streamHadTrustedBooks = false;
  /** First trusted canonical book on the CURRENT stream attempt. Not connect time. */
  private firstTrustedBookAtMs = 0;
  private logicalLossGapped = false;
  private tokenIdSet = new Set<string>();
  private pendingEpochReason: IngressGapReason | null = null;
  private readonly stableAfterMs: number;
  lastReconnectDelayMs = 0;
  readonly reconnectDelays: number[] = [];

  constructor(
    client?: PublicClient,
    options: {
      transport?: MarketStreamTransport;
      marketWsUrl?: string;
      ingress?: BoundedIngressQueue;
      stableAfterMs?: number;
      /**
       * Enable authoritative min_order_size hydration from the REST order-book
       * path (PublicClient.fetchOrderBook) when a trusted WS full book omits
       * min_order_size. The official REST OrderBook schema requires the field;
       * the WS MarketBookEventSchema makes it optional. Off by default so
       * adapter unit tests never perform network I/O; the engine enables it.
       */
      hydrateMinOrderSizeFromRest?: boolean;
      /** Override fetcher for deterministic tests and venue-specific wiring. */
      minOrderSizeFetcher?: MinOrderSizeFetcher;
      /** Injectable time source for autonomous retry scheduling (default Date.now). */
      minOrderSizeClock?: () => number;
      /** Failure retry interval in ms (default MIN_ORDER_SIZE_REST_RETRY_MS). */
      minOrderSizeRetryIntervalMs?: number;
      /** Attempt cap per lifecycle (default MIN_ORDER_SIZE_REST_MAX_ATTEMPTS). */
      minOrderSizeMaxAttempts?: number;
    } = {},
  ) {
    this.client = client ?? createPublicClient();
    if (options.transport && !isBoundedMarketStreamTransport(options.transport)) {
      throw new Error("MARKET_TRANSPORT_MUST_BE_BOUNDED");
    }
    this.ingress = options.ingress ?? options.transport?.queue ?? new BoundedIngressQueue();
    this.transport = options.transport ?? new BoundedClobMarketTransport({
      url: options.marketWsUrl ?? CLOB_MARKET_WS_URL,
      queue: this.ingress,
    });
    this.stableAfterMs = options.stableAfterMs ?? MARKET_CONNECT_BOUNDS.stableAfterMs;
    this.minOrderSizeClock = options.minOrderSizeClock ?? (() => Date.now());
    this.minOrderSizeRetryIntervalMs = options.minOrderSizeRetryIntervalMs ?? MIN_ORDER_SIZE_REST_RETRY_MS;
    this.minOrderSizeMaxAttempts = options.minOrderSizeMaxAttempts ?? MIN_ORDER_SIZE_REST_MAX_ATTEMPTS;

    let minOrderSizeFetcher = options.minOrderSizeFetcher ?? null;
    if (minOrderSizeFetcher == null && options.hydrateMinOrderSizeFromRest === true) {
      // Nearest existing REST path: the same PublicClient that serves discovery
      // and fetchPublicMarket exposes fetchOrderBook (OrderBook.minOrderSize).
      const orderBookClient = this.client as unknown as {
        fetchOrderBook?: (request: { assetId: string }) => Promise<{ minOrderSize?: string | null }>;
      };
      const fetchOrderBook = orderBookClient.fetchOrderBook;
      if (typeof fetchOrderBook === "function") {
        minOrderSizeFetcher = async (tokenId) => {
          const book = await fetchOrderBook({ assetId: tokenId });
          return book?.minOrderSize ?? null;
        };
      }
    }
    this.minOrderSizeFetcher = minOrderSizeFetcher;
  }

  setMinOrderSizeHydrationObserver(observer: MinOrderSizeHydrationObserver | null): void {
    this.minOrderSizeHydrationObserver = observer;
  }

  lastReconnectDelayForTest(): number { return this.lastReconnectDelayMs; }

  setNormalizeObserver(observer: MarketNormalizeObserver | null): void { this.normalizeObserver = observer; }

  isTokenSynchronized(tokenId: string): boolean { return this.sync.isSynchronized(tokenId); }
  tokenSyncState(tokenId: string): TokenSyncState { return this.sync.stateOf(tokenId); }
  tokenEpoch(tokenId: string): number { return this.sync.epochOf(tokenId); }
  unsynchronizedTokenCount(): number { return this.sync.unsynchronizedCount(); }
  tokenSyncEntryCount(): number { return this.sync.entryCount(); }
  ingressSnapshot(): IngressQueueSnapshot { return this.ingress.snapshot(); }
  usesBoundedMarketTransport(): boolean { return this.transport.bounded === true; }
  /** @deprecated Use usesBoundedMarketTransport. Always true — SDK subscribe is not a runtime path. */
  usesOwnedBoundedTransport(): boolean { return this.usesBoundedMarketTransport(); }
  setIngressObserver(observer: IngressQueueObserver | null): void { this.ingress.setObserver(observer); }

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
    this.tokenIdSet = new Set(next);
    if (changed) {
      this.pendingEpochReason = this.streamGeneration > 0 ? "generation_change" : "explicit_resubscribe";
    }
    if (next.length > 0) {
      const waiters = this.tokenWaiters.splice(0);
      for (const waiter of waiters) waiter();
    }
    if (changed) {
      for (const tokenId of previous) {
        if (next.includes(tokenId)) continue;
        this.books.delete(tokenId);
        this.minOrderSizeByToken.delete(tokenId);
        this.lastTimestampByToken.delete(tokenId);
        this.anomalySampler.forget(tokenId);
        this.sync.drop(tokenId);
        this.removeMinOrderSizeHydration(tokenId);
      }
      this.sync.retain(next);
      this.closeCurrentStream?.();
    } else {
      this.sync.retain(next);
    }
    return { changed, previous, next };
  }

  async run(tokenIds: readonly string[], handlers: PolymarketFeedHandlers, signal: AbortSignal): Promise<void> {
    this.setTokenIds(this.tokenIds.length > 0 ? this.tokenIds : tokenIds);
    const backoff = new ExponentialBackoff({ initialDelayMs: 500, maxDelayMs: 30_000, multiplier: 2, jitterRatio: 0.2 });
    let reconnectCount = 0;
    let reconnectPending = false;
    const onAbort = () => { this.closeCurrentStream?.(); };
    if (signal.aborted) {
      await handlers.onConnection({ feed: "polymarket", state: "STOPPED", at: new Date().toISOString(), reconnectCount });
      return;
    }
    signal.addEventListener("abort", onAbort);
    this.streamOnMinOrderSizeHydrated = handlers?.onMinOrderSizeHydrated;
    this.minOrderSizeHydrationShutdown = false;
    this.clearMinOrderSizeHydrationLifecycle();
    try {
      while (!signal.aborted) {
        if (this.tokenIds.length === 0) {
          await handlers.onConnection({ feed: "polymarket", state: "STOPPED", at: new Date().toISOString(), reconnectCount, message: "NO_QUALIFIED_TOKENS" });
          try { await this.waitForTokens(signal); } catch { break; }
          continue;
        }
        await handlers.onConnection({ feed: "polymarket", state: "CONNECTING", at: new Date().toISOString(), reconnectCount, reconnecting: reconnectPending });
        let resubscribed = false;
        const generation = ++this.streamGeneration;
        try {
          const current = [...this.tokenIds];
          if (current.length === 0) continue;
          const generationRestart = generation > 1 && reconnectCount === 0;
          const previousStreamTrusted = this.streamHadTrustedBooks;
          this.streamHadTrustedBooks = false;
          this.firstTrustedBookAtMs = 0;
          const reason = this.pendingEpochReason
            ?? (reconnectCount > 0 ? "reconnect" : generationRestart ? "generation_change" : "explicit_resubscribe");
          const emitGap = reason === "generation_change"
            || generationRestart
            || (reconnectCount > 0 && !this.logicalLossGapped && previousStreamTrusted);
          this.pendingEpochReason = null;
          this.beginStreamEpoch(current, reason, handlers, { emitGap });
          this.logicalLossGapped = false;
          const connectAbort = new AbortController();
          const onParentAbort = () => connectAbort.abort(signal.reason);
          if (signal.aborted) connectAbort.abort(signal.reason);
          else signal.addEventListener("abort", onParentAbort, { once: true });
          this.closeCurrentStream = () => {
            resubscribed = true;
            connectAbort.abort("MARKET_STREAM_RESUBSCRIBE");
          };
          let stream: Awaited<ReturnType<MarketStreamTransport["subscribe"]>>;
          try {
            stream = await this.transport.subscribe(current, false, connectAbort.signal);
          } finally {
            signal.removeEventListener("abort", onParentAbort);
          }
          this.activeStreams += 1;
          this.closeCurrentStream = () => {
            resubscribed = true;
            void stream.close("generation_change");
          };
          if (signal.aborted || connectAbort.signal.aborted) this.closeCurrentStream();
          try {
            await handlers.onConnection({ feed: "polymarket", state: "CONNECTED", at: new Date().toISOString(), reconnectCount, reconnecting: reconnectPending });
            reconnectPending = false;
            let turnStarted = performance.now(), turnEvents = 0;
            for await (const event of stream) {
              if (signal.aborted) { await stream.close(); break; }
              if (resubscribed || generation !== this.streamGeneration) break;
              try {
                for (const snapshot of this.normalizeEvent(event, handlers)) {
                  if (resubscribed || generation !== this.streamGeneration) break;
                  await handlers.onSnapshot(snapshot);
                }
                // Every source event and snapshot is still processed in order.
                // A bounded batch amortizes scheduling without starving IO.
                if (++turnEvents >= MARKET_DRAIN_BOUNDS.maxEventsPerTurn || performance.now() - turnStarted >= MARKET_DRAIN_BOUNDS.maxTurnMs) {
                  await new Promise<void>(resolve => setImmediate(resolve));
                  turnEvents = 0; turnStarted = performance.now();
                }
              } catch (error) {
                const normalized = error instanceof Error ? error : new Error(String(error));
                if (normalized instanceof IngressOverflowError) {
                  this.noteUnexpectedStreamLoss(normalized.reason, handlers);
                  await stream.close();
                  break;
                }
                if (normalized instanceof MarketIngressMalformedError) {
                  if (this.streamHadTrustedBooks) this.noteUnexpectedStreamLoss("malformed", handlers);
                  await stream.close();
                  break;
                }
                if (isTimestampDisorderError(normalized)) {
                  this.noteAnomaly(handlers, timestampDisorderAnomaly(normalized));
                  continue;
                }
                await handlers.onError(normalized);
              }
            }
          } finally {
            if (
              this.streamHadTrustedBooks
              && this.firstTrustedBookAtMs > 0
              && Date.now() - this.firstTrustedBookAtMs >= this.stableAfterMs
            ) {
              backoff.reset();
            }
            this.closeCurrentStream = null;
            this.activeStreams = Math.max(0, this.activeStreams - 1);
          }
          if (signal.aborted) break;
          if (resubscribed || this.tokenIds.length === 0 || generation !== this.streamGeneration) continue;
          const closeDiagnostic = stream.closeDiagnostic?.();
          if (closeDiagnostic && closeDiagnostic.kind !== "operator_shutdown" && closeDiagnostic.kind !== "generation_change") {
            throw new ClobMarketCloseError("POLYMARKET_STREAM_ENDED", closeDiagnostic);
          }
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
          this.noteUnexpectedStreamLoss(unexpectedLossReason(normalized), handlers);
          reconnectCount += 1;
          reconnectPending = true;
          if (!(normalized instanceof IngressOverflowError)) await handlers.onError(normalized);
          await handlers.onConnection({ feed: "polymarket", state: "DISCONNECTED", at: new Date().toISOString(), reconnectCount, message: normalized.message });
          const delay = backoff.nextDelay();
          this.lastReconnectDelayMs = delay;
          this.reconnectDelays.push(delay);
          await abortableDelay(delay, signal).catch(() => undefined);
        }
      }
      this.closeCurrentStream = null;
      this.activeStreams = 0;
      await this.client.closeSubscriptions();
      await handlers.onConnection({ feed: "polymarket", state: "STOPPED", at: new Date().toISOString(), reconnectCount });
    } finally {
      signal.removeEventListener("abort", onAbort);
      this.streamOnMinOrderSizeHydrated = undefined;
      // Run shutdown: no autonomous retries may start afterwards, and any
      // pending retry timer/state must be cleared.
      this.minOrderSizeHydrationShutdown = true;
      this.clearMinOrderSizeHydrationLifecycle();
    }
  }

  private clearMinOrderSizeHydrationLifecycle(): void {
    if (this.minOrderSizeHydrationRetryTimer != null) {
      clearInterval(this.minOrderSizeHydrationRetryTimer);
      this.minOrderSizeHydrationRetryTimer = null;
    }
    this.minOrderSizeHydrationState.clear();
    this.noteHydrationInFlightChanged();
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

  beginStreamEpoch(
    tokenIds: readonly string[],
    reason: IngressGapReason,
    handlers?: Pick<PolymarketFeedHandlers, "onGap">,
    options: { emitGap?: boolean } = {},
  ): void {
    const epoch = this.sync.beginEpoch(tokenIds, reason);
    for (const tokenId of tokenIds) {
      this.books.delete(tokenId);
      this.lastTimestampByToken.delete(tokenId);
      // A new stream epoch supersedes any in-flight hydration: stale completions
      // must neither block nor apply to the new epoch's books.
      this.invalidateMinOrderSizeHydration(tokenId);
    }
    this.ingress.clear();
    if (options.emitGap !== false && (options.emitGap === true || reason !== "explicit_resubscribe")) {
      handlers?.onGap?.({ reason, tokenCount: tokenIds.length, epoch, tokenIds: [...tokenIds] });
    }
  }

  gapAll(reason: IngressGapReason, handlers?: Pick<PolymarketFeedHandlers, "onGap">): void {
    const tokenIds = [...this.tokenIds];
    for (const tokenId of tokenIds) {
      this.sync.markGapped(tokenId, reason);
      this.books.delete(tokenId);
      this.lastTimestampByToken.delete(tokenId);
      this.invalidateMinOrderSizeHydration(tokenId);
    }
    this.ingress.clear();
    this.logicalLossGapped = true;
    handlers?.onGap?.({
      reason,
      tokenCount: tokenIds.length,
      epoch: tokenIds[0] ? this.sync.epochOf(tokenIds[0]) : 0,
      tokenIds,
    });
  }

  private noteUnexpectedStreamLoss(reason: IngressGapReason, handlers?: Pick<PolymarketFeedHandlers, "onGap">): void {
    if (this.logicalLossGapped || !this.streamHadTrustedBooks) return;
    this.gapAll(reason, handlers);
  }

  markTokenGappedForTest(tokenId: string, reason: IngressGapReason = "explicit_resubscribe"): void {
    this.sync.markGapped(tokenId, reason);
    this.books.delete(tokenId);
    this.invalidateMinOrderSizeHydration(tokenId);
  }

  private normalizeEvent(event: unknown, handlers: Pick<PolymarketFeedHandlers, "onAnomaly" | "onResync" | "onGap">): MarketSnapshot[] {
    const started = performance.now();
    try {
    const value = event as { type?: string; payload?: Record<string, unknown> };
    const payload = value.payload ?? {};
    if (value.type === "book") {
      return this.commitFullBook(payload, handlers);
    }
    if (value.type === "price_change") {
      return this.commitPriceChange(payload, handlers);
    }
    if (value.type === "last_trade_price") {
      const tokenId = String(payload.tokenId ?? "");
      const state = this.books.get(tokenId);
      if (state) state.lastTradePrice = nullableString(payload.price);
    }
    return [];
    } finally { this.normalizeObserver?.onSourceEventNormalized?.((performance.now() - started) / 1000); }
  }

  /**
   * Parse → validate every trusted field → stage → atomically publish.
   * A malformed canonical book never becomes SYNCED / adapter / MCP state.
   */
  private commitFullBook(
    payload: Record<string, unknown>,
    handlers: Pick<PolymarketFeedHandlers, "onAnomaly" | "onResync" | "onGap">,
  ): MarketSnapshot[] {
    const tokenId = String(payload.tokenId ?? "");
    const market = this.marketsByToken.get(tokenId);
    if (!tokenId || !market || !this.tokenIdSet.has(tokenId)) return [];
    if (this.sync.stateOf(tokenId) !== "SYNCED") this.lastTimestampByToken.delete(tokenId);
    const sourceMs = parseSourceTimestamp(payload.timestamp);
    if (sourceMs === null) {
      this.noteAnomaly(handlers, {
        kind: "malformed", tokenId, incomingMs: null, lastAcceptedMs: this.lastTimestampByToken.get(tokenId) ?? null,
        regressionMs: null,
      });
      throw new MarketIngressMalformedError([tokenId]);
    }
    const previous = this.lastTimestampByToken.get(tokenId);
    if (previous !== undefined && sourceMs < previous) {
      this.noteAnomaly(handlers, {
        kind: "stale", tokenId, incomingMs: sourceMs, lastAcceptedMs: previous, regressionMs: previous - sourceMs,
      });
      return [];
    }
    const bids = levelsToMap(payload.bids);
    const asks = levelsToMap(payload.asks);
    const started = performance.now();
    const staged: BookState = {
      marketId: String(payload.market ?? market.id),
      bids,
      asks,
      bidLevels: sortedFrozenLevels(bids, "desc"),
      askLevels: sortedFrozenLevels(asks, "asc"),
      publishedBids: null,
      publishedAsks: null,
      bidsDirty: true,
      asksDirty: true,
      bidsOwned: true,
      asksOwned: true,
      levelsValid: false,
      lastTradePrice: nullableString(payload.lastTradePrice),
      // Preserve the last known authoritative value across full books that
      // omit min_order_size (WS field is optional), so a committed REST-hydrated
      // or previously-supplied value is not lost until a book actively changes it.
      minOrderSize: nullableString(payload.minOrderSize) ?? this.books.get(tokenId)?.minOrderSize ?? this.minOrderSizeByToken.get(tokenId) ?? null,
      timestamp: new Date(sourceMs).toISOString(),
      hash: nullableString(payload.hash),
    };
    try {
      validateNormalizedBook(staged);
      assertDecimalString(staged.lastTradePrice);
      assertDecimalString(nullableString(payload.tickSize));
      assertDecimalString(nullableString(payload.minOrderSize));
      if (market.feeSchedule != null) feeScheduleSchema.parse(market.feeSchedule);
    } catch {
      this.noteAnomaly(handlers, {
        kind: "malformed", tokenId, incomingMs: sourceMs, lastAcceptedMs: previous ?? null, regressionMs: null,
      });
      throw new MarketIngressMalformedError([tokenId]);
    }
    staged.levelsValid = true;
    this.lastTimestampByToken.set(tokenId, sourceMs);
    this.books.set(tokenId, staged);
    if (staged.minOrderSize != null) this.minOrderSizeByToken.set(tokenId, staged.minOrderSize);
    const synced = this.sync.markSyncedFromBook(tokenId);
    if (staged.minOrderSize == null) this.requestMinOrderSizeHydration(tokenId);
    if (this.firstTrustedBookAtMs === 0) this.firstTrustedBookAtMs = Date.now();
    this.streamHadTrustedBooks = true;
    if (synced.wasResync) {
      handlers.onResync?.({ reason: "full_book", tokenId, epoch: synced.epoch });
    }
    return [this.toSnapshot(tokenId, staged, payload, undefined, started)];
  }

  /**
   * Parse and validate every relevant price_change entry, then atomically
   * publish staged copies. One malformed entry leaves ZERO trusted mutation
   * and forces a resync. Stale/out-of-order timestamps remain a safe-drop.
   *
   * Each accepted token is staged once per source payload. Later entries for
   * the same token mutate that working book in place after copy-on-write of
   * the written side. Independent frozen arrays are captured per accepted
   * entry so snapshot count and source order stay unchanged.
   */
  private commitPriceChange(
    payload: Record<string, unknown>,
    handlers: Pick<PolymarketFeedHandlers, "onAnomaly" | "onResync" | "onGap">,
  ): MarketSnapshot[] {
    const changes = Array.isArray(payload.priceChanges) ? payload.priceChanges as Array<Record<string, unknown>> : [];
    const sourceMs = parseSourceTimestamp(payload.timestamp);
    const parsed: Array<{ tokenId: string; side: "BUY" | "SELL"; price: string; size: string; hash: unknown; state: BookState }> = [];
    const mentioned = new Set<string>();
    for (const change of changes) {
      const tokenId = String(change.tokenId ?? "");
      if (!tokenId || !this.tokenIdSet.has(tokenId)) continue;
      if (this.sync.stateOf(tokenId) !== "SYNCED") continue;
      const state = this.books.get(tokenId);
      if (!state) continue;
      mentioned.add(tokenId);
      const sideRaw = String(change.side ?? "").toUpperCase();
      const price = String(change.price ?? "");
      const size = String(change.size ?? "0");
      if ((sideRaw !== "BUY" && sideRaw !== "SELL") || !DECIMAL_RE.test(price) || !DECIMAL_RE.test(size) || Number(size) < 0) {
        this.noteAnomaly(handlers, {
          kind: "malformed", tokenId, incomingMs: sourceMs, lastAcceptedMs: this.lastTimestampByToken.get(tokenId) ?? null,
          regressionMs: null,
        });
        this.failIncrementalTrust([...mentioned], handlers);
        throw new MarketIngressMalformedError([...mentioned]);
      }
      parsed.push({ tokenId, side: sideRaw, price, size, hash: change.hash, state });
    }
    if (parsed.length === 0) {
      this.normalizeObserver?.onPriceChangeEvent(changes.length, 0);
      return [];
    }
    if (sourceMs === null) {
      const tokens = [...mentioned];
      this.noteAnomaly(handlers, {
        kind: "malformed", tokenId: tokens[0] ?? "unknown", incomingMs: null,
        lastAcceptedMs: tokens[0] ? this.lastTimestampByToken.get(tokens[0]) ?? null : null,
        regressionMs: null,
      });
      this.failIncrementalTrust(tokens, handlers);
      throw new MarketIngressMalformedError(tokens);
    }

    const accepted: typeof parsed = [];
    for (const item of parsed) {
      const previous = this.lastTimestampByToken.get(item.tokenId);
      if (previous !== undefined && sourceMs < previous) {
        this.noteAnomaly(handlers, {
          kind: "stale", tokenId: item.tokenId, incomingMs: sourceMs, lastAcceptedMs: previous,
          regressionMs: previous - sourceMs,
        });
        continue;
      }
      accepted.push(item);
    }
    if (accepted.length === 0) {
      this.normalizeObserver?.onPriceChangeEvent(changes.length, 0);
      return [];
    }

    const working = new Map<string, BookState>();
    const commits: Array<{ tokenId: string; state: BookState; changedLevel: BookLevel | null }> = [];
    for (const item of accepted) {
      let copy = working.get(item.tokenId);
      if (!copy) {
        copy = cloneBookState(item.state);
        working.set(item.tokenId, copy);
      }
      const sideMap = item.side === "BUY" ? copy.bids : copy.asks;
      // An unchanged level is still a distinct source observation, with its
      // timestamp/hash/sequence and callback. Only redundant copies are skipped.
      if (Number(item.size) <= 0 ? sideMap.has(item.price) : sideMap.get(item.price) !== item.size) {
        ownBookSide(copy, item.side);
        if (item.side === "BUY") applyLevelChange(copy.bids, copy.bidLevels, item.price, item.size, "desc");
        else applyLevelChange(copy.asks, copy.askLevels, item.price, item.size, "asc");
      }
      copy.timestamp = new Date(sourceMs).toISOString();
      copy.hash = nullableString(item.hash) ?? copy.hash;
      const changedLevel = Number(item.size) <= 0 ? null : { price: item.price, quantity: item.size };
      try {
        this.assertSnapshotPublishable(item.tokenId, copy, payload, changedLevel);
      } catch {
        this.noteAnomaly(handlers, {
          kind: "malformed", tokenId: item.tokenId, incomingMs: sourceMs,
          lastAcceptedMs: this.lastTimestampByToken.get(item.tokenId) ?? null, regressionMs: null,
        });
        this.failIncrementalTrust([...new Set(accepted.map((entry) => entry.tokenId))], handlers);
        throw new MarketIngressMalformedError([...new Set(accepted.map((entry) => entry.tokenId))]);
      }
      copy.levelsValid = true;
      commits.push({ tokenId: item.tokenId, state: captureStagedSnapshot(copy), changedLevel });
    }

    for (const [tokenId, state] of working) {
      publishSide(state, "bids");
      publishSide(state, "asks");
      this.books.set(tokenId, state);
      this.lastTimestampByToken.set(tokenId, sourceMs);
    }

    const snapshots: MarketSnapshot[] = [];
    for (const commit of commits) {
      const started = performance.now();
      snapshots.push(this.toSnapshot(commit.tokenId, commit.state, payload, commit.changedLevel, started));
    }
    this.normalizeObserver?.onPriceChangeEvent(changes.length, snapshots.length);
    return snapshots;
  }

  /**
   * Every check toSnapshot needs before a source event may mutate trusted state.
   * Does not increment sequence or publish sides.
   */
  private assertSnapshotPublishable(
    tokenId: string,
    state: BookState,
    raw: Record<string, unknown>,
    changedLevel?: BookLevel | null,
  ): void {
    const market = this.marketsByToken.get(tokenId);
    if (!market) throw new Error("UNKNOWN_TOKEN");
    validateNormalizedBook(state, changedLevel);
    assertDecimalString(state.lastTradePrice);
    assertDecimalString(nullableString(raw.lastTradePrice));
    assertDecimalString(nullableString(raw.tickSize));
    assertDecimalString(nullableString(raw.minOrderSize));
    if (market.feeSchedule != null) feeScheduleSchema.parse(market.feeSchedule);
  }

  private failIncrementalTrust(
    tokenIds: readonly string[],
    handlers: Pick<PolymarketFeedHandlers, "onGap">,
  ): void {
    const affected = [...new Set(tokenIds.filter(Boolean))];
    if (affected.length === 0) return;
    for (const tokenId of affected) {
      this.sync.markGapped(tokenId, "malformed");
      this.books.delete(tokenId);
      this.lastTimestampByToken.delete(tokenId);
      this.invalidateMinOrderSizeHydration(tokenId);
    }
    if (!this.streamHadTrustedBooks) return;
    this.logicalLossGapped = true;
    handlers.onGap?.({
      reason: "malformed",
      tokenCount: affected.length,
      epoch: this.sync.epochOf(affected[0]!),
      tokenIds: affected,
    });
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

  /**
   * Enqueue or autonomously resume a bounded authoritative min_order_size
   * hydration for a token whose trusted WS full book omitted the field. Bounds:
   * exactly one physical in-flight request per token at any instant, a
   * per-lifecycle attempt cap, a failure backoff, and autonomous retry that
   * does not require another WS event. Never invoked from price_change
   * processing.
   */
  private requestMinOrderSizeHydration(tokenId: string): void {
    const fetcher = this.minOrderSizeFetcher;
    if (!fetcher) return;
    if (this.minOrderSizeHydrationShutdown) return;
    if (!this.tokenIdSet.has(tokenId)) return;
    let hydration = this.minOrderSizeHydrationState.get(tokenId);
    if (!hydration) {
      hydration = { lifecycle: 0, flightId: 0, attempts: 0, retryAt: 0, removed: false };
      this.minOrderSizeHydrationState.set(tokenId, hydration);
    }
    // A re-added token reuses its bumped-lifecycle entry (removal invalidated it).
    if (hydration.removed) hydration.removed = false;
    if (hydration.flightId !== 0) return; // one physical flight per token
    const book = this.books.get(tokenId);
    if (!book || book.minOrderSize != null) return;
    const now = this.minOrderSizeClock();
    if (now < hydration.retryAt) return; // failure backoff
    if (hydration.attempts >= this.minOrderSizeMaxAttempts) return;
    hydration.attempts += 1;
    const flightId = ++this.minOrderSizeHydrationFlightSeq;
    hydration.flightId = flightId;
    const identity: MinOrderSizeHydrationIdentity = {
      tokenId, lifecycle: hydration.lifecycle, epoch: this.sync.epochOf(tokenId), flightId,
    };
    this.ensureMinOrderSizeHydrationRetryTimer();
    this.noteHydrationInFlightChanged();
    this.minOrderSizeFetcherCalls += 1;
    this.minOrderSizeHydrationObserver?.onHydrationAttempt?.();
    const settle = (value: string | null): void => {
      const current = this.minOrderSizeHydrationState.get(tokenId);
      // A completion is stale when the lifecycle advanced (gap/malformed/epoch/
      // reconnect) or this flight is no longer the registered one. Stale
      // completions never apply and never touch newer retry/attempt state.
      if (!current || current.lifecycle !== identity.lifecycle || current.flightId !== flightId) {
        this.minOrderSizeHydrationObserver?.onHydrationStale?.();
        return;
      }
      if (value == null) {
        this.minOrderSizeHydrationObserver?.onHydrationFailure?.();
        current.retryAt = this.minOrderSizeClock() + this.minOrderSizeRetryIntervalMs;
        return;
      }
      this.minOrderSizeHydrationObserver?.onHydrationSuccess?.();
      this.applyHydratedMinOrderSize(tokenId, identity, value);
    };
    void Promise.resolve()
      .then(() => fetcher(tokenId))
      .then((raw) => {
        // Accept only a valid decimal string; anything else stays unknown so the
        // protective path fails safely (MIN_ORDER_SIZE_UNKNOWN).
        const value = raw == null ? null : decimalStringSchema.safeParse(String(raw)).success ? String(raw) : null;
        settle(value);
      })
      .catch(() => settle(null))
      .finally(() => {
        const current = this.minOrderSizeHydrationState.get(tokenId);
        if (current && current.flightId === flightId) {
          current.flightId = 0;
          this.noteHydrationInFlightChanged();
          // Removal during flight defers deletion until the stale flight settles
          // (identity stays valid to block re-add reuse); then release it.
          if (current.removed) {
            this.minOrderSizeHydrationState.delete(tokenId);
            this.noteHydrationInFlightChanged();
            return;
          }
          // After settling, if the current lifecycle still needs hydration,
          // resume autonomously (failure backoff still applies).
          this.requestMinOrderSizeHydration(tokenId);
        }
      });
  }

  /**
   * Apply the hydrated value only when the token is still managed, at the same
   * stream epoch AND the same hydration lifecycle identity (no reconnect/
   * resync/gap/token-replacement in between), and its live book still lacks an
   * authoritative value. A WS-supplied min_order_size that arrives first always
   * wins. After apply, consumers are notified through the metadata-only channel
   * (never a market event).
   */
  private applyHydratedMinOrderSize(tokenId: string, identity: MinOrderSizeHydrationIdentity, value: string): void {
    if (!this.tokenIdSet.has(tokenId)) return;
    if (this.sync.epochOf(tokenId) !== identity.epoch) return;
    const current = this.minOrderSizeHydrationState.get(tokenId);
    if (!current || current.lifecycle !== identity.lifecycle) return;
    const book = this.books.get(tokenId);
    if (!book || book.minOrderSize != null) return;
    book.minOrderSize = value;
    this.minOrderSizeByToken.set(tokenId, value);
    this.minOrderSizeHydrationObserver?.onHydrationApplied?.();
    this.emitMinOrderSizeHydrated(tokenId, value, identity.epoch);
  }

  private emitMinOrderSizeHydrated(tokenId: string, minOrderSize: string, tokenEpoch: number): void {
    const handler = this.streamOnMinOrderSizeHydrated;
    if (!handler) return;
    void Promise.resolve(handler({ tokenId, minOrderSize, tokenEpoch })).catch(() => undefined);
  }

  /**
   * Advance the hydration lifecycle for a token whose book/token lifecycle
   * transitioned (gap, malformed-fail, epoch change). In-flight requests become
   * stale: they can neither apply nor mutate retry/backoff state, and the next
   * launch receives a fresh identity. The state entry itself is kept (bounded
   * per currently managed token); token removal deletes it entirely.
   */
  private invalidateMinOrderSizeHydration(tokenId: string): void {
    const hydration = this.minOrderSizeHydrationState.get(tokenId);
    if (!hydration) return;
    hydration.lifecycle += 1;
    hydration.attempts = 0;
    hydration.retryAt = 0;
  }

  private removeMinOrderSizeHydration(tokenId: string): void {
    const hydration = this.minOrderSizeHydrationState.get(tokenId);
    if (hydration && hydration.flightId !== 0) {
      // Defer deletion until the physical stale flight settles; invalidate now
      // so the stale result can never apply to a same-token re-add.
      hydration.lifecycle += 1;
      hydration.attempts = 0;
      hydration.retryAt = 0;
      hydration.removed = true;
      return;
    }
    this.minOrderSizeHydrationState.delete(tokenId);
    this.noteHydrationInFlightChanged();
  }

  private ensureMinOrderSizeHydrationRetryTimer(): void {
    if (this.minOrderSizeHydrationRetryTimer != null || this.minOrderSizeHydrationShutdown) return;
    const timer = setInterval(() => this.tickMinOrderSizeHydrationRetries(), 1_000);
    if (typeof (timer as { unref?: () => void }).unref === "function") (timer as { unref?: () => void }).unref?.();
    this.minOrderSizeHydrationRetryTimer = timer;
  }

  /** One per-adapter bounded scan; requestMinOrderSizeHydration enforces every guard. */
  private tickMinOrderSizeHydrationRetries(): void {
    if (this.minOrderSizeHydrationShutdown) return;
    for (const tokenId of [...this.minOrderSizeHydrationState.keys()]) {
      this.requestMinOrderSizeHydration(tokenId);
    }
  }

  private noteHydrationInFlightChanged(): void {
    let count = 0;
    for (const hydration of this.minOrderSizeHydrationState.values()) {
      if (hydration.flightId !== 0) count += 1;
    }
    if (count !== this.minOrderSizeHydrationInFlightCount) {
      this.minOrderSizeHydrationInFlightCount = count;
      this.minOrderSizeHydrationObserver?.onHydrationInFlightChanged?.(count);
    }
  }

  /** Test-only: authoritative min order size currently held for a token. */
  hydratedMinOrderSizeForTest(tokenId: string): string | null {
    return this.books.get(tokenId)?.minOrderSize ?? null;
  }

  /** Test-only: how many times the min-order fetcher was invoked. */
  minOrderSizeFetcherCallCountForTest(): number {
    return this.minOrderSizeFetcherCalls;
  }

  /** Test-only: run one autonomous-retry scan synchronously (used with minOrderSizeClock). */
  flushMinOrderSizeHydrationRetriesForTest(): void {
    this.tickMinOrderSizeHydrationRetries();
  }

  /** Test-only: number of hydration lifecycle states (should stay bounded). */
  minOrderSizeHydrationStateCountForTest(): number {
    return this.minOrderSizeHydrationState.size;
  }

  /** Test-only: count of physically in-flight hydration requests. */
  minOrderSizeHydrationInFlightForTest(): number {
    return this.minOrderSizeHydrationInFlightCount;
  }

  /** Test-only: force a stream-epoch advance (reconnect/resync surrogate). */
  bumpTokenEpochForTest(tokenId: string, reason: IngressGapReason = "explicit_resubscribe"): void {
    this.sync.beginEpoch([tokenId], reason);
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
    // Carry forward the last committed full-book min_order_size so a
    // price-change-derived snapshot keeps reporting the authoritative minimum
    // order size instead of silently nulling it.
    const minOrderSize = state.minOrderSize ?? nullableString(raw.minOrderSize);
    let feeSchedule: MarketSnapshot["feeSchedule"] = null;
    try {
      // Publication safety is proven before trusted mutation. Repeating
      // full-book validation here would redo O(n) level walks already done
      // by commitFullBook / commitPriceChange. Event-level fields and the
      // changed level stay checked on those commit paths.
      if (!state.levelsValid) this.assertSnapshotPublishable(tokenId, state, raw, changedLevel);
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
      ...(typeof raw.__wsCallbackAt === "string" && Number.isFinite(Date.parse(raw.__wsCallbackAt))
        ? { wsCallbackAt: raw.__wsCallbackAt }
        : {}),
      sequence: ++this.sequence,
      hash: state.hash,
    };
    this.normalizeObserver?.onNormalizedSnapshot({
      durationSeconds: (performance.now() - started) / 1000,
      bidLevels: state.bidLevels.length,
      askLevels: state.askLevels.length,
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

/**
 * Stage a price_change onto a trusted BookState without exposing partial
 * mutation. Copy-on-write clones only the side that will be mutated.
 *
 * Invariants that make sharing the unchanged side safe:
 * - Trusted books in `this.books` are only installed after validation.
 * - applyLevelChange mutates only the maps/arrays passed to it (the cloned
 *   side). The opposite side is never written.
 * - Published snapshot arrays are frozen slices, not the working arrays.
 * - A later mutation always forks the side it writes before splicing, so an
 *   already-published snapshot and a previous staged state cannot change.
 * - levelsValid is preserved: previously validated unchanged levels stay
 *   trusted; only the inserted/replaced level is rechecked.
 */
function cloneBookState(state: BookState): BookState {
  return { ...state, bidsOwned: false, asksOwned: false };
}

function ownBookSide(state: BookState, side: "BUY" | "SELL"): void {
  if (side === "BUY") {
    if (state.bidsOwned) {
      state.publishedBids = null;
      state.bidsDirty = true;
      return;
    }
    state.bids = new Map(state.bids);
    state.bidLevels = state.bidLevels.slice();
    state.publishedBids = null;
    state.bidsDirty = true;
    state.bidsOwned = true;
    return;
  }
  if (state.asksOwned) {
    state.publishedAsks = null;
    state.asksDirty = true;
    return;
  }
  state.asks = new Map(state.asks);
  state.askLevels = state.askLevels.slice();
  state.publishedAsks = null;
  state.asksDirty = true;
  state.asksOwned = true;
}

/**
 * Freeze independent published arrays for one accepted entry while the
 * per-token working state continues to accept later entries in the same
 * payload. Maps may still be shared with the working state; toSnapshot
 * and observers read the captured arrays, not those maps.
 */
function captureStagedSnapshot(state: BookState): BookState {
  const bids = !state.bidsDirty && state.publishedBids
    ? state.publishedBids
    : Object.freeze(state.bidLevels.slice()) as BookLevel[];
  const asks = !state.asksDirty && state.publishedAsks
    ? state.publishedAsks
    : Object.freeze(state.askLevels.slice()) as BookLevel[];
  return {
    ...state,
    bidLevels: bids as BookLevel[],
    askLevels: asks as BookLevel[],
    publishedBids: bids,
    publishedAsks: asks,
    bidsDirty: false,
    asksDirty: false,
    bidsOwned: false,
    asksOwned: false,
  };
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

/**
 * Full books always walk every level (`levelsValid === false`).
 *
 * Incremental updates may keep `levelsValid` from a previously trusted
 * BookState. That is safe because:
 * 1. commitFullBook / a prior successful incremental commit only install
 *    books after every level passed assertBookLevel.
 * 2. cloneBookState copies the mutated side and shares the other; apply
 *    cannot rewrite a previously validated unchanged level object (levels
 *    are frozen; replacements assign a new frozen object).
 * 3. Insert/replace still validate `changedLevel` (price/size decimals and
 *    non-negative size). Deletion (`changedLevel == null`) only removes a
 *    previously validated level, so the remainder stays valid.
 * 4. Event-level fields (timestamp, tickSize, minOrderSize, lastTradePrice,
 *    fee schedule, side) are still checked on the commit path.
 *
 * Malformed incremental data therefore cannot skip the checks that would
 * have rejected it before; it only skips re-walking unchanged levels.
 */
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

function unexpectedLossReason(error: Error): IngressGapReason {
  if (error instanceof IngressOverflowError) return error.reason;
  if (error instanceof MarketIngressMalformedError) return "malformed";
  if (error.message.includes("MALFORMED") || error.message === "MARKET_INGRESS_MALFORMED") return "malformed";
  return "reconnect";
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
