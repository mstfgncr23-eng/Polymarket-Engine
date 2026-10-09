import type { MarketSnapshot } from "@pm/core";
import { type IngressQueueObserver } from "./bounded-ingress.js";
import type { IngressGapReason, IngressQueueSnapshot, TokenSyncState } from "./ingress-bounds.js";
import { isBoundedMarketStreamTransport, type MarketStreamTransport } from "./market-stream-transport.js";
import {
  PolymarketMarketDataAdapter as SinglePolymarketMarketDataAdapter,
  type DiscoveryReport,
  type FeedConnectionEvent,
  type MarketIngressGapEvent,
  type MarketIngressResyncEvent,
  type MinOrderSizeHydratedEvent,
  type MinOrderSizeHydrationObserver,
  type PolymarketFeedHandlers,
  type PolymarketFeedAnomaly,
  type PolymarketTimestampStats,
  type MarketNormalizeObserver,
} from "./polymarket.js";

export const DEFAULT_POLYMARKET_MARKET_SHARD_COUNT = 4;
type SingleOptions = NonNullable<ConstructorParameters<typeof SinglePolymarketMarketDataAdapter>[1]>;
type SingleClient = ConstructorParameters<typeof SinglePolymarketMarketDataAdapter>[0];

type ConnectionState = FeedConnectionEvent["state"];
type TokenGroup = { key: string; tokenIds: string[] };

export interface ShardedPolymarketAdapterOptions extends SingleOptions {
  /**
   * Test seam only. Production constructs DEFAULT_POLYMARKET_MARKET_SHARD_COUNT
   * independent owned transports. Supplying transports makes their positive
   * length the shard count for deterministic tests.
   */
  transports?: readonly MarketStreamTransport[];
}

export interface PolymarketMarketShardSnapshot {
  shard: number;
  state: ConnectionState;
  reconnectCount: number;
  tokenCount: number;
  unsynchronizedTokens: number;
  ingress: IngressQueueSnapshot;
}

export interface PolymarketMarketShardObserver {
  onChange?(snapshots: readonly PolymarketMarketShardSnapshot[]): void;
  onError?(input: { shard: number; error: Error }): void;
  onGap?(input: { shard: number; event: MarketIngressGapEvent }): void;
  onResync?(input: { shard: number; event: MarketIngressResyncEvent; remainingUnsynchronizedTokens: number }): void;
}

/**
 * Production facade for independent Polymarket market WebSocket shards.
 *
 * Why composition rather than teaching the existing adapter about multiple
 * sockets? The single-socket adapter already has correct, tested reconnect,
 * bounded-ingress and book-trust semantics. Keeping one complete adapter per
 * shard makes queue, generation, reconnect and resync state physically
 * independent. A loss on one shard therefore cannot clear or gap another.
 *
 * The facade preserves the Engine-facing contract:
 * - both outcomes of a discovered market stay on the same shard;
 * - snapshots remain serialized within each owned shard; independent shards may overlap across async Engine awaits;
 * - one global facade sequence is assigned in dispatch order;
 * - connection and ingress telemetry are aggregated;
 * - no raw market event is dropped, coalesced, or sampled.
 *
 * Explicit client/transport/ingress injection keeps legacy single-shard
 * behavior for existing tests and custom callers. Before run() starts, direct
 * normalizeSourceEvent/test usage also remains backed by the inherited
 * single-adapter state. The production zero-argument live run activates the
 * four-shard production default measured by the production-shaped soak.
 */
export class ShardedPolymarketMarketDataAdapter extends SinglePolymarketMarketDataAdapter {
  private readonly sharded: boolean;
  private readonly shardCount: number;
  private readonly shards: readonly SinglePolymarketMarketDataAdapter[];
  private shardedRunStarted = false;
  private readonly ownerByToken = new Map<string, number>();
  private readonly ownerByGroup = new Map<string, number>();
  private currentTokenIds: string[] = [];
  private shardTokenIds: string[][];
  private globalSequence = 0;

  private aggregateIngressObserver: IngressQueueObserver | null = null;
  private shardObserver: PolymarketMarketShardObserver | null = null;
  private aggregateIngressHighWaterEvents = 0;
  private aggregateIngressHighWaterBytes = 0;

  private connectionStates: ConnectionState[];
  private connectionReconnects: number[];
  private lastConnectionSignature = "";
  private connectionTail: Promise<void> = Promise.resolve();
  /** One ordered callback lane per owned socket. Cross-shard async work must not head-of-line block healthy shards. */
  private snapshotTails: Promise<void>[] = [];

  constructor(client?: SingleClient, options: ShardedPolymarketAdapterOptions = {}) {
    const { transports, ...singleOptions } = options;
    super(client, singleOptions);

    if (transports && transports.length === 0) {
      throw new Error("MARKET_SHARD_TRANSPORT_COUNT:0");
    }
    if (transports?.some((transport) => !isBoundedMarketStreamTransport(transport))) {
      throw new Error("MARKET_TRANSPORT_MUST_BE_BOUNDED");
    }

    this.shardCount = transports?.length ?? DEFAULT_POLYMARKET_MARKET_SHARD_COUNT;
    this.shardTokenIds = this.emptyShardTokenIds();
    this.snapshotTails = Array.from({ length: this.shardCount }, () => Promise.resolve());
    this.connectionStates = this.stoppedConnectionStates();
    this.connectionReconnects = this.zeroReconnectCounts();

    // Preserve every explicit legacy injection path. Engine production uses
    // the zero-argument constructor and therefore gets the production default.
    this.sharded = Boolean(transports) || (!client && !singleOptions.transport && !singleOptions.ingress);
    if (!this.sharded) {
      this.shards = [];
      return;
    }

    this.shards = Array.from({ length: this.shardCount }, (_, index) => {
      const transport = transports?.[index];
      const childOptions: SingleOptions = {};
      if (transport) childOptions.transport = transport;
      if (singleOptions.marketWsUrl !== undefined) childOptions.marketWsUrl = singleOptions.marketWsUrl;
      if (transport?.queue) childOptions.ingress = transport.queue;
      if (singleOptions.stableAfterMs !== undefined) childOptions.stableAfterMs = singleOptions.stableAfterMs;
      if (singleOptions.hydrateMinOrderSizeFromRest !== undefined) childOptions.hydrateMinOrderSizeFromRest = singleOptions.hydrateMinOrderSizeFromRest;
      if (singleOptions.minOrderSizeFetcher !== undefined) childOptions.minOrderSizeFetcher = singleOptions.minOrderSizeFetcher;
      if (singleOptions.minOrderSizeClock !== undefined) childOptions.minOrderSizeClock = singleOptions.minOrderSizeClock;
      if (singleOptions.minOrderSizeRetryIntervalMs !== undefined) childOptions.minOrderSizeRetryIntervalMs = singleOptions.minOrderSizeRetryIntervalMs;
      if (singleOptions.minOrderSizeMaxAttempts !== undefined) childOptions.minOrderSizeMaxAttempts = singleOptions.minOrderSizeMaxAttempts;
      return new SinglePolymarketMarketDataAdapter(client, childOptions);
    });
  }

  marketShardCount(): number { return this.sharded ? this.shardCount : 1; }

  shardSnapshots(): readonly PolymarketMarketShardSnapshot[] {
    if (!this.sharded) return [];
    return this.shards.map((shard, index) => ({
      shard: index + 1,
      state: this.connectionStates[index] ?? "STOPPED",
      reconnectCount: this.connectionReconnects[index] ?? 0,
      tokenCount: this.shardTokenIds[index]?.length ?? 0,
      unsynchronizedTokens: shard.unsynchronizedTokenCount(),
      ingress: shard.ingressSnapshot(),
    }));
  }

  setShardObserver(observer: PolymarketMarketShardObserver | null): void {
    this.shardObserver = observer;
    this.publishShardSnapshots();
  }

  override setNormalizeObserver(observer: MarketNormalizeObserver | null): void {
    // Keep the inherited direct-normalization API observable for replay/unit
    // callers that intentionally use normalizeSourceEvent() without run().
    super.setNormalizeObserver(observer);
    if (!this.sharded) return;
    for (const shard of this.shards) shard.setNormalizeObserver(observer);
  }

  override setMinOrderSizeHydrationObserver(observer: MinOrderSizeHydrationObserver | null): void {
    super.setMinOrderSizeHydrationObserver(observer);
    if (!this.sharded) return;
    for (const shard of this.shards) shard.setMinOrderSizeHydrationObserver(observer);
  }

  override setIngressObserver(observer: IngressQueueObserver | null): void {
    if (!this.sharded) {
      super.setIngressObserver(observer);
      return;
    }
    this.aggregateIngressObserver = observer;
    for (const shard of this.shards) {
      shard.setIngressObserver(observer || this.shardObserver ? {
        onChange: () => {
          if (this.aggregateIngressObserver) this.publishAggregateIngress();
          this.publishShardSnapshots();
        },
      } : null);
    }
    if (observer) this.publishAggregateIngress();
    this.publishShardSnapshots();
  }

  override ingressSnapshot(): IngressQueueSnapshot {
    if (!this.sharded) return super.ingressSnapshot();
    const snapshots = this.shards.map((shard) => shard.ingressSnapshot());
    const depth = snapshots.reduce((sum, item) => sum + item.depth, 0);
    const bytes = snapshots.reduce((sum, item) => sum + item.bytes, 0);
    this.aggregateIngressHighWaterEvents = Math.max(this.aggregateIngressHighWaterEvents, depth);
    this.aggregateIngressHighWaterBytes = Math.max(this.aggregateIngressHighWaterBytes, bytes);
    return {
      depth,
      bytes,
      highWaterEvents: this.aggregateIngressHighWaterEvents,
      highWaterBytes: this.aggregateIngressHighWaterBytes,
      accepted: snapshots.reduce((sum, item) => sum + item.accepted, 0),
      processed: snapshots.reduce((sum, item) => sum + item.processed, 0),
      gapTotal: snapshots.reduce((sum, item) => sum + item.gapTotal, 0),
    };
  }

  override usesBoundedMarketTransport(): boolean {
    return this.sharded
      ? this.shards.every((shard) => shard.usesBoundedMarketTransport())
      : super.usesBoundedMarketTransport();
  }

  override usesOwnedBoundedTransport(): boolean { return this.usesBoundedMarketTransport(); }

  override async discoverCryptoMarkets(
    ...args: Parameters<SinglePolymarketMarketDataAdapter["discoverCryptoMarkets"]>
  ): Promise<DiscoveryReport> {
    const report = await super.discoverCryptoMarkets(...args);
    if (!this.sharded) return report;

    // Child adapters need normalization metadata, but discovery/REST remains
    // centralized in the facade. Rich catalog fields are restored onto each
    // emitted snapshot at the facade boundary.
    for (const market of report.qualified) {
      for (const token of market.tokens) {
        for (const shard of this.shards) {
          shard.registerManualToken(token.tokenId, market.id, token.outcome, market.asset);
        }
      }
    }
    return report;
  }

  override registerManualToken(
    ...args: Parameters<SinglePolymarketMarketDataAdapter["registerManualToken"]>
  ): void {
    super.registerManualToken(...args);
    if (!this.sharded) return;
    for (const shard of this.shards) shard.registerManualToken(...args);
  }

  override setTokenIds(
    tokenIds: readonly string[],
  ): ReturnType<SinglePolymarketMarketDataAdapter["setTokenIds"]> {
    const result = super.setTokenIds(tokenIds);
    if (!this.sharded) return result;

    this.currentTokenIds = [...result.next];
    this.shardTokenIds = this.partitionPairAware(result.next);
    this.ownerByToken.clear();
    for (let shardIndex = 0; shardIndex < this.shardTokenIds.length; shardIndex += 1) {
      for (const tokenId of this.shardTokenIds[shardIndex]!) this.ownerByToken.set(tokenId, shardIndex);
      this.shards[shardIndex]!.setTokenIds(this.shardTokenIds[shardIndex]!);
    }
    this.publishShardSnapshots();
    return result;
  }

  override async run(
    tokenIds: readonly string[],
    handlers: PolymarketFeedHandlers,
    signal: AbortSignal,
  ): Promise<void> {
    if (!this.sharded) return super.run(tokenIds, handlers, signal);

    this.shardedRunStarted = true;
    this.setTokenIds(this.currentTokenIds.length > 0 ? this.currentTokenIds : tokenIds);
    this.connectionStates = this.stoppedConnectionStates();
    this.connectionReconnects = this.zeroReconnectCounts();
    this.lastConnectionSignature = "";
    this.connectionTail = Promise.resolve();
    this.snapshotTails = Array.from({ length: this.shardCount }, () => Promise.resolve());
    this.publishShardSnapshots();

    await Promise.all(this.shards.map((shard, shardIndex) => shard.run(
      this.shardTokenIds[shardIndex] ?? [],
      this.handlersForShard(shardIndex, handlers),
      signal,
    )));
  }

  override isTokenSynchronized(tokenId: string): boolean {
    if (!this.sharded || !this.shardedRunStarted) return super.isTokenSynchronized(tokenId);
    return this.shardForToken(tokenId)?.isTokenSynchronized(tokenId) ?? false;
  }

  override tokenSyncState(tokenId: string): TokenSyncState {
    if (!this.sharded || !this.shardedRunStarted) return super.tokenSyncState(tokenId);
    return this.shardForToken(tokenId)?.tokenSyncState(tokenId) ?? "AWAITING_BOOK";
  }

  override tokenEpoch(tokenId: string): number {
    if (!this.sharded || !this.shardedRunStarted) return super.tokenEpoch(tokenId);
    return this.shardForToken(tokenId)?.tokenEpoch(tokenId) ?? 0;
  }

  override unsynchronizedTokenCount(): number {
    if (!this.sharded || !this.shardedRunStarted) return super.unsynchronizedTokenCount();
    return this.shards.reduce((sum, shard) => sum + shard.unsynchronizedTokenCount(), 0);
  }

  override tokenSyncEntryCount(): number {
    if (!this.sharded || !this.shardedRunStarted) return super.tokenSyncEntryCount();
    return this.shards.reduce((sum, shard) => sum + shard.tokenSyncEntryCount(), 0);
  }

  override activeStreamCount(): number {
    if (!this.sharded) return super.activeStreamCount();
    return this.shards.reduce((sum, shard) => sum + shard.activeStreamCount(), 0);
  }

  override lastAcceptedTimestamp(tokenId: string): number | undefined {
    if (!this.sharded || !this.shardedRunStarted) return super.lastAcceptedTimestamp(tokenId);
    return this.shardForToken(tokenId)?.lastAcceptedTimestamp(tokenId);
  }

  override bookState(tokenId: string): ReturnType<SinglePolymarketMarketDataAdapter["bookState"]> {
    if (!this.sharded || !this.shardedRunStarted) return super.bookState(tokenId);
    return this.shardForToken(tokenId)?.bookState(tokenId);
  }

  override timestampStats(): PolymarketTimestampStats {
    if (!this.sharded || !this.shardedRunStarted) return super.timestampStats();
    const stats = this.shards.map((shard) => shard.timestampStats());
    const anomalies = stats
      .map((item) => item.lastAnomaly)
      .filter((item): item is PolymarketFeedAnomaly => item !== null)
      .sort((left, right) => Date.parse(left.at) - Date.parse(right.at));
    return {
      staleDropped: stats.reduce((sum, item) => sum + item.staleDropped, 0),
      malformedDropped: stats.reduce((sum, item) => sum + item.malformedDropped, 0),
      diagnosticsEmitted: stats.reduce((sum, item) => sum + item.diagnosticsEmitted, 0),
      lastAnomaly: anomalies.at(-1) ?? null,
    };
  }

  override markTokenGappedForTest(tokenId: string, reason: IngressGapReason = "explicit_resubscribe"): void {
    if (!this.sharded || !this.shardedRunStarted) {
      super.markTokenGappedForTest(tokenId, reason);
      return;
    }
    this.shardForToken(tokenId)?.markTokenGappedForTest(tokenId, reason);
    this.publishShardSnapshots();
  }

  override hydratedMinOrderSizeForTest(tokenId: string): string | null {
    if (!this.sharded || !this.shardedRunStarted) return super.hydratedMinOrderSizeForTest(tokenId);
    return this.shardForToken(tokenId)?.hydratedMinOrderSizeForTest(tokenId) ?? null;
  }

  override minOrderSizeFetcherCallCountForTest(): number {
    if (!this.sharded || !this.shardedRunStarted) return super.minOrderSizeFetcherCallCountForTest();
    return this.shards.reduce((sum, shard) => sum + shard.minOrderSizeFetcherCallCountForTest(), 0);
  }

  override minOrderSizeHydrationStateCountForTest(): number {
    if (!this.sharded || !this.shardedRunStarted) return super.minOrderSizeHydrationStateCountForTest();
    return this.shards.reduce((sum, shard) => sum + shard.minOrderSizeHydrationStateCountForTest(), 0);
  }

  override minOrderSizeHydrationInFlightForTest(): number {
    if (!this.sharded || !this.shardedRunStarted) return super.minOrderSizeHydrationInFlightForTest();
    return this.shards.reduce((sum, shard) => sum + shard.minOrderSizeHydrationInFlightForTest(), 0);
  }

  override flushMinOrderSizeHydrationRetriesForTest(): void {
    if (!this.sharded || !this.shardedRunStarted) {
      super.flushMinOrderSizeHydrationRetriesForTest();
      return;
    }
    for (const shard of this.shards) shard.flushMinOrderSizeHydrationRetriesForTest();
  }

  override bumpTokenEpochForTest(tokenId: string, reason: IngressGapReason = "explicit_resubscribe"): void {
    if (!this.sharded || !this.shardedRunStarted) {
      super.bumpTokenEpochForTest(tokenId, reason);
      return;
    }
    this.shardForToken(tokenId)?.bumpTokenEpochForTest(tokenId, reason);
  }

  override beginStreamEpoch(
    tokenIds: readonly string[],
    reason: IngressGapReason,
    handlers?: Pick<PolymarketFeedHandlers, "onGap">,
    options: { emitGap?: boolean } = {},
  ): void {
    if (!this.sharded || !this.shardedRunStarted) {
      super.beginStreamEpoch(tokenIds, reason, handlers, options);
      return;
    }
    const byShard = this.emptyShardTokenIds();
    for (const tokenId of tokenIds) {
      const owner = this.ownerByToken.get(tokenId);
      if (owner !== undefined) byShard[owner]!.push(tokenId);
    }
    for (let index = 0; index < this.shards.length; index += 1) {
      if (byShard[index]!.length > 0) this.shards[index]!.beginStreamEpoch(byShard[index]!, reason, handlers, options);
    }
    this.publishShardSnapshots();
  }

  override gapAll(reason: IngressGapReason, handlers?: Pick<PolymarketFeedHandlers, "onGap">): void {
    if (!this.sharded || !this.shardedRunStarted) {
      super.gapAll(reason, handlers);
      return;
    }
    for (const shard of this.shards) shard.gapAll(reason, handlers);
    this.publishShardSnapshots();
  }

  private handlersForShard(shardIndex: number, handlers: PolymarketFeedHandlers): PolymarketFeedHandlers {
    const result: PolymarketFeedHandlers = {
      onSnapshot: (snapshot) => this.dispatchSnapshot(shardIndex, snapshot, handlers),
      onConnection: (event) => this.reportShardConnection(shardIndex, event, handlers),
      onError: (error) => {
        this.notifyShardObserver((observer) => observer.onError?.({ shard: shardIndex + 1, error }));
        return handlers.onError(error);
      },
    };
    // Metadata-only channel: forwarded WITHOUT dispatchSnapshot/globalSequence so
    // hydration never manufactures a market observation or advances market sequence.
    if (handlers.onMinOrderSizeHydrated) {
      result.onMinOrderSizeHydrated = (event) => this.dispatchMinOrderSizeHydrated(shardIndex, event, handlers);
    }
    if (handlers.onAnomaly) result.onAnomaly = handlers.onAnomaly;
    if (handlers.onGap) result.onGap = (event: MarketIngressGapEvent) => {
      this.notifyShardObserver((observer) => observer.onGap?.({ shard: shardIndex + 1, event }));
      this.publishShardSnapshots();
      handlers.onGap!(event);
    };
    if (handlers.onResync) result.onResync = (event: MarketIngressResyncEvent) => {
      const remainingUnsynchronizedTokens = this.shards[shardIndex]!.unsynchronizedTokenCount();
      this.notifyShardObserver((observer) => observer.onResync?.({
        shard: shardIndex + 1,
        event,
        remainingUnsynchronizedTokens,
      }));
      this.publishShardSnapshots();
      handlers.onResync!(event);
    };
    return result;
  }

  private dispatchSnapshot(
    shardIndex: number,
    snapshot: MarketSnapshot,
    handlers: PolymarketFeedHandlers,
  ): Promise<void> {
    const market = super.getMarket(snapshot.tokenId);
    const token = market?.tokens.find((item) => item.tokenId === snapshot.tokenId);
    // Assignment is synchronous on the owning JS thread, so sequence remains
    // globally unique/monotonic by facade dispatch even though different shard
    // callbacks may overlap across awaits.
    const patched: MarketSnapshot = {
      ...snapshot,
      sequence: ++this.globalSequence,
      marketId: market?.id ?? snapshot.marketId,
      outcome: token?.outcome ?? snapshot.outcome,
      asset: market?.asset ?? snapshot.asset,
      feeSchedule: market ? market.feeSchedule : snapshot.feeSchedule,
    };
    const previous = this.snapshotTails[shardIndex] ?? Promise.resolve();
    const work = previous.then(async () => {
      await handlers.onSnapshot(patched);
    });
    this.snapshotTails[shardIndex] = work.then(() => undefined, () => undefined);
    return work;
  }

  /**
   * Metadata for a token shares its shard callback lane with price snapshots,
   * preserving token/epoch ordering without globally blocking the other three
   * independent sockets. It never advances globalSequence.
   */
  private dispatchMinOrderSizeHydrated(
    shardIndex: number,
    event: MinOrderSizeHydratedEvent,
    handlers: PolymarketFeedHandlers,
  ): Promise<void> {
    const previous = this.snapshotTails[shardIndex] ?? Promise.resolve();
    const work = previous.then(async () => {
      await handlers.onMinOrderSizeHydrated?.(event);
    });
    this.snapshotTails[shardIndex] = work.then(() => undefined, () => undefined);
    return work;
  }

  private reportShardConnection(
    shardIndex: number,
    event: FeedConnectionEvent,
    handlers: PolymarketFeedHandlers,
  ): Promise<void> {
    const work = this.connectionTail.then(async () => {
      this.connectionStates[shardIndex] = event.state;
      this.connectionReconnects[shardIndex] = event.reconnectCount;
      this.publishShardSnapshots();
      const aggregate = this.aggregateConnectionState();
      const reconnectCount = this.connectionReconnects.reduce((sum, value) => sum + value, 0);
      const message = event.message ? `shard=${shardIndex + 1}:${event.message}` : undefined;
      const signature = `${aggregate}:${reconnectCount}:${message ?? ""}`;
      if (signature === this.lastConnectionSignature) return;
      this.lastConnectionSignature = signature;
      await handlers.onConnection({
        feed: "polymarket",
        state: aggregate,
        at: event.at,
        reconnectCount,
        ...(message ? { message } : {}),
      });
    });
    this.connectionTail = work.then(() => undefined, () => undefined);
    return work;
  }

  private aggregateConnectionState(): ConnectionState {
    const active = this.connectionStates.filter((_, index) => (this.shardTokenIds[index]?.length ?? 0) > 0);
    if (active.length === 0 || active.every((state) => state === "STOPPED")) return "STOPPED";
    if (active.some((state) => state === "STALE")) return "STALE";
    if (active.some((state) => state === "DISCONNECTED")) return "DISCONNECTED";
    if (active.every((state) => state === "CONNECTED")) return "CONNECTED";
    return "CONNECTING";
  }

  private publishAggregateIngress(): void {
    this.aggregateIngressObserver?.onChange(this.ingressSnapshot());
  }

  private publishShardSnapshots(): void {
    if (!this.shardObserver?.onChange) return;
    const snapshots = this.shardSnapshots();
    this.notifyShardObserver((observer) => observer.onChange?.(snapshots));
  }

  private notifyShardObserver(work: (observer: PolymarketMarketShardObserver) => void): void {
    const observer = this.shardObserver;
    if (!observer) return;
    try { work(observer); } catch { /* observability must never change feed semantics */ }
  }

  private shardForToken(tokenId: string): SinglePolymarketMarketDataAdapter | undefined {
    const owner = this.ownerByToken.get(tokenId);
    return owner === undefined ? undefined : this.shards[owner];
  }

  private emptyShardTokenIds(): string[][] {
    return Array.from({ length: this.shardCount }, () => []);
  }

  private stoppedConnectionStates(): ConnectionState[] {
    return Array.from({ length: this.shardCount }, () => "STOPPED" as ConnectionState);
  }

  private zeroReconnectCounts(): number[] {
    return Array.from({ length: this.shardCount }, () => 0);
  }

  private partitionPairAware(tokenIds: readonly string[]): string[][] {
    const requested = new Set(tokenIds);
    const consumed = new Set<string>();
    const groups: TokenGroup[] = [];
    const singletons: TokenGroup[] = [];

    for (const tokenId of tokenIds) {
      if (consumed.has(tokenId)) continue;
      const market = super.getMarket(tokenId);
      const marketTokens = market
        ? market.tokens.map((token) => token.tokenId).filter((id) => requested.has(id) && !consumed.has(id))
        : [];
      if (market && marketTokens.length > 1) {
        for (const id of marketTokens) consumed.add(id);
        groups.push({ key: `market:${market.id}`, tokenIds: marketTokens });
        continue;
      }
      consumed.add(tokenId);
      singletons.push({ key: `single:${tokenId}`, tokenIds: [tokenId] });
    }

    // POLYMARKET_TOKEN_IDS legacy configuration carries UP/DOWN as adjacent
    // entries but does not provide a market id. Co-locate adjacent orphan
    // singletons rather than risk splitting a configured pair.
    for (let index = 0; index < singletons.length; index += 2) {
      const pair = singletons.slice(index, index + 2).flatMap((item) => item.tokenIds);
      const key = `fallback:${[...pair].sort().join(":")}`;
      groups.push({ key, tokenIds: pair });
    }

    const liveKeys = new Set(groups.map((group) => group.key));
    for (const key of [...this.ownerByGroup.keys()]) {
      if (!liveKeys.has(key)) this.ownerByGroup.delete(key);
    }

    const load = Array.from({ length: this.shardCount }, () => 0);
    for (const group of groups) {
      const owner = this.ownerByGroup.get(group.key);
      if (owner !== undefined) load[owner] = (load[owner] ?? 0) + group.tokenIds.length;
    }

    for (const group of groups) {
      if (this.ownerByGroup.has(group.key)) continue;
      let owner = 0;
      for (let index = 1; index < this.shardCount; index += 1) {
        if ((load[index] ?? 0) < (load[owner] ?? 0)) owner = index;
      }
      this.ownerByGroup.set(group.key, owner);
      load[owner] = (load[owner] ?? 0) + group.tokenIds.length;
    }

    const partitions = this.emptyShardTokenIds();
    for (const group of groups) {
      const owner = this.ownerByGroup.get(group.key)!;
      partitions[owner]!.push(...group.tokenIds);
    }
    return partitions;
  }
}
