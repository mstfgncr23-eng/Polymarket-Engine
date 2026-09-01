import { createPublicClient } from "@polymarket/client";
import { createId, marketSnapshotSchema, type MarketSnapshot } from "@pm/core";
import { abortableDelay, ExponentialBackoff } from "./reconnect.js";

export interface DiscoveredMarket {
  id: string;
  slug: string;
  question: string;
  endDate: string | null;
  active: boolean;
  tokens: Array<{ tokenId: string; outcome: "UP" | "DOWN"; price: string | null }>;
  feeSchedule: MarketSnapshot["feeSchedule"];
  raw: Record<string, unknown>;
}

export interface FeedConnectionEvent {
  feed: "polymarket" | "btc";
  state: "CONNECTING" | "CONNECTED" | "DISCONNECTED" | "STALE" | "STOPPED";
  at: string;
  reconnectCount: number;
  message?: string;
}

export interface PolymarketFeedHandlers {
  onSnapshot(snapshot: MarketSnapshot): Promise<void> | void;
  onConnection(event: FeedConnectionEvent): Promise<void> | void;
  onError(error: Error): Promise<void> | void;
}

type PublicClient = ReturnType<typeof createPublicClient>;

export class PolymarketMarketDataAdapter {
  private readonly client: PublicClient;
  private readonly marketsByToken = new Map<string, DiscoveredMarket>();
  private readonly books = new Map<string, { marketId: string; bids: Map<string, string>; asks: Map<string, string>; lastTradePrice: string | null; timestamp: string; hash: string | null }>();
  private sequence = 0;
  private lastTimestampByToken = new Map<string, number>();

  constructor(client: PublicClient = createPublicClient()) { this.client = client; }

  async discoverBitcoinMarkets(slugs: readonly string[] = [], maxMarkets = 20): Promise<DiscoveredMarket[]> {
    const rawMarkets: unknown[] = [];
    if (slugs.length > 0) {
      for (const slug of slugs) rawMarkets.push(await this.client.fetchMarket({ slug }));
    } else {
      const page = await this.client.listMarkets({ closed: false, pageSize: 100 }).firstPage();
      rawMarkets.push(...page.items);
    }
    const normalized = rawMarkets.map(normalizeMarket).filter((market): market is DiscoveredMarket => market !== null)
      .filter((market) => slugs.length > 0 || /\b(bitcoin|btc)\b/i.test(`${market.question} ${market.slug}`))
      .filter((market) => market.active && market.tokens.length === 2)
      .slice(0, maxMarkets);
    for (const market of normalized) for (const token of market.tokens) this.marketsByToken.set(token.tokenId, market);
    return normalized;
  }

  registerManualToken(tokenId: string, marketId = tokenId, outcome: "UP" | "DOWN" = "UP"): void {
    this.marketsByToken.set(tokenId, {
      id: marketId, slug: marketId, question: marketId, endDate: null, active: true,
      tokens: [{ tokenId, outcome, price: null }], feeSchedule: null, raw: {},
    });
  }

  async run(tokenIds: readonly string[], handlers: PolymarketFeedHandlers, signal: AbortSignal): Promise<void> {
    const backoff = new ExponentialBackoff({ initialDelayMs: 500, maxDelayMs: 30_000, multiplier: 2, jitterRatio: 0.2 });
    let reconnectCount = 0;
    while (!signal.aborted) {
      await handlers.onConnection({ feed: "polymarket", state: "CONNECTING", at: new Date().toISOString(), reconnectCount });
      try {
        const stream = await this.client.subscribe([{ topic: "market", tokenIds: [...tokenIds], customFeatureEnabled: true }]);
        backoff.reset();
        await handlers.onConnection({ feed: "polymarket", state: "CONNECTED", at: new Date().toISOString(), reconnectCount });
        for await (const event of stream) {
          if (signal.aborted) { await stream.close(); break; }
          for (const snapshot of this.normalizeEvent(event)) await handlers.onSnapshot(snapshot);
        }
        if (!signal.aborted) throw new Error("POLYMARKET_STREAM_ENDED");
      } catch (error) {
        if (signal.aborted) break;
        reconnectCount += 1;
        const normalized = error instanceof Error ? error : new Error(String(error));
        await handlers.onError(normalized);
        await handlers.onConnection({ feed: "polymarket", state: "DISCONNECTED", at: new Date().toISOString(), reconnectCount, message: normalized.message });
        await abortableDelay(backoff.nextDelay(), signal).catch(() => undefined);
      }
    }
    await this.client.closeSubscriptions();
    await handlers.onConnection({ feed: "polymarket", state: "STOPPED", at: new Date().toISOString(), reconnectCount });
  }

  private normalizeEvent(event: unknown): MarketSnapshot[] {
    const value = event as { type?: string; payload?: Record<string, unknown> };
    const payload = value.payload ?? {};
    if (value.type === "book") {
      const tokenId = String(payload.tokenId ?? "");
      const market = this.marketsByToken.get(tokenId);
      if (!tokenId || !market) return [];
      const sourceMs = parseSourceTimestamp(payload.timestamp);
      this.assertTimestampOrder(tokenId, sourceMs);
      const state = {
        marketId: String(payload.market ?? market.id), bids: levelsToMap(payload.bids), asks: levelsToMap(payload.asks),
        lastTradePrice: nullableString(payload.lastTradePrice), timestamp: new Date(sourceMs).toISOString(), hash: nullableString(payload.hash),
      };
      this.books.set(tokenId, state);
      return [this.toSnapshot(tokenId, state, payload)];
    }
    if (value.type === "price_change") {
      const changes = Array.isArray(payload.priceChanges) ? payload.priceChanges as Array<Record<string, unknown>> : [];
      const snapshots: MarketSnapshot[] = [];
      for (const change of changes) {
        const tokenId = String(change.tokenId ?? "");
        const state = this.books.get(tokenId);
        if (!state) continue;
        const sourceMs = parseSourceTimestamp(payload.timestamp);
        this.assertTimestampOrder(tokenId, sourceMs);
        const side = String(change.side);
        const target = side === "BUY" ? state.bids : state.asks;
        const price = String(change.price ?? "");
        const size = String(change.size ?? "0");
        if (Number(size) <= 0) target.delete(price); else target.set(price, size);
        state.timestamp = new Date(sourceMs).toISOString();
        state.hash = nullableString(change.hash) ?? state.hash;
        snapshots.push(this.toSnapshot(tokenId, state, payload));
      }
      return snapshots;
    }
    if (value.type === "last_trade_price") {
      const tokenId = String(payload.tokenId ?? "");
      const state = this.books.get(tokenId);
      if (state) state.lastTradePrice = nullableString(payload.price);
    }
    return [];
  }

  private assertTimestampOrder(tokenId: string, sourceMs: number): void {
    const previous = this.lastTimestampByToken.get(tokenId);
    if (previous !== undefined && sourceMs < previous) throw new Error(`EVENT_TIMESTAMP_DISORDER:${tokenId}:${sourceMs}<${previous}`);
    this.lastTimestampByToken.set(tokenId, sourceMs);
  }

  private toSnapshot(tokenId: string, state: NonNullable<ReturnType<PolymarketMarketDataAdapter["books"]["get"]>>, raw: Record<string, unknown>): MarketSnapshot {
    const market = this.marketsByToken.get(tokenId);
    if (!market) throw new Error("UNKNOWN_TOKEN");
    const token = market.tokens.find((item) => item.tokenId === tokenId);
    return marketSnapshotSchema.parse({
      eventId: createId("poly"), traceId: createId("trace"), marketId: market.id, tokenId,
      outcome: token?.outcome ?? "UP", bids: sortedLevels(state.bids, "desc"), asks: sortedLevels(state.asks, "asc"),
      lastTradePrice: state.lastTradePrice, tickSize: nullableString(raw.tickSize), minOrderSize: nullableString(raw.minOrderSize),
      feeSchedule: market.feeSchedule, sourceTimestamp: state.timestamp, receivedAt: new Date().toISOString(),
      sequence: ++this.sequence, hash: state.hash,
    });
  }
}

function normalizeMarket(raw: unknown): DiscoveredMarket | null {
  const value = raw as Record<string, unknown>;
  const outcomes = value.outcomes as { yes?: Record<string, unknown>; no?: Record<string, unknown> } | undefined;
  const yesToken = nullableString(outcomes?.yes?.tokenId);
  const noToken = nullableString(outcomes?.no?.tokenId);
  const state = value.state as Record<string, unknown> | undefined;
  const trading = value.trading as Record<string, unknown> | undefined;
  const rawFee = trading?.feeSchedule as Record<string, unknown> | null | undefined;
  const feesEnabled = trading?.feesEnabled === true;
  const feeSchedule = feesEnabled && rawFee ? {
    rate: String(rawFee.rate), exponent: Number(rawFee.exponent), takerOnly: Boolean(rawFee.takerOnly),
    rebateRate: String(rawFee.rebateRate ?? 0), source: "market_fee_schedule" as const, effectiveAt: new Date().toISOString(),
  } : feesEnabled ? null : {
    rate: "0", exponent: 1, takerOnly: true, rebateRate: "0", source: "market_fee_schedule" as const, effectiveAt: new Date().toISOString(),
  };
  if (!yesToken || !noToken) return null;
  return {
    id: String(value.id ?? value.conditionId ?? value.slug), slug: String(value.slug ?? value.id),
    question: String(value.question ?? value.title ?? value.slug), endDate: nullableString(state?.endDate),
    active: state?.active === true && state.closed !== true && state.acceptingOrders !== false,
    tokens: [
      { tokenId: yesToken, outcome: "UP", price: nullableString(outcomes?.yes?.price) },
      { tokenId: noToken, outcome: "DOWN", price: nullableString(outcomes?.no?.price) },
    ], feeSchedule, raw: value,
  };
}

function nullableString(value: unknown): string | null { return value === undefined || value === null || value === "" ? null : String(value); }
function parseSourceTimestamp(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : Date.now();
}
function levelsToMap(value: unknown): Map<string, string> {
  const result = new Map<string, string>();
  if (!Array.isArray(value)) return result;
  for (const level of value as Array<Record<string, unknown>>) result.set(String(level.price), String(level.size ?? level.quantity));
  return result;
}
function sortedLevels(map: Map<string, string>, order: "asc" | "desc") {
  return [...map.entries()].sort(([a], [b]) => order === "asc" ? Number(a) - Number(b) : Number(b) - Number(a))
    .map(([price, quantity]) => ({ price, quantity }));
}
