import { createPublicClient } from "@polymarket/client";
import {
  createId,
  impliedSpread,
  marketSnapshotSchema,
  qualifyMarket,
  qualificationConfigSchema,
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
  active: boolean;
  tokens: Array<{ tokenId: string; outcome: "UP" | "DOWN"; price: string | null }>;
  feeSchedule: MarketSnapshot["feeSchedule"];
  liquidityUsd: string | null;
  spread: number | null;
  qualification: QualificationResult;
  raw: Record<string, unknown>;
}

export interface FeedConnectionEvent {
  feed: "polymarket" | "btc" | "reference";
  asset?: string;
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

export interface DiscoveryReport {
  discovered: DiscoveredMarket[];
  qualified: DiscoveredMarket[];
  rejected: Array<{ id: string; slug: string; title: string; reasonCodes: QualificationReasonCode[] }>;
}

type PublicClient = ReturnType<typeof createPublicClient>;

export class PolymarketMarketDataAdapter {
  private readonly client: PublicClient;
  private readonly marketsByToken = new Map<string, DiscoveredMarket>();
  private readonly books = new Map<string, { marketId: string; bids: Map<string, string>; asks: Map<string, string>; lastTradePrice: string | null; timestamp: string; hash: string | null }>();
  private sequence = 0;
  private lastTimestampByToken = new Map<string, number>();
  private tokenIds: string[] = [];
  private closeCurrentStream: (() => void) | null = null;
  private tokenWaiters: Array<() => void> = [];

  constructor(client: PublicClient = createPublicClient()) { this.client = client; }

  getMarket(tokenId: string): DiscoveredMarket | undefined { return this.marketsByToken.get(tokenId); }
  getMarkets(): DiscoveredMarket[] { return uniqueMarkets([...this.marketsByToken.values()]); }

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
      rawMarkets.push(...await this.listCryptoEventMarkets());
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
      const market = toDiscoveredMarket(candidate, result);
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
    const qualified = discovered.filter((market) => market.qualification.qualified).slice(0, config.maxQualifiedMarkets);
    return { discovered, qualified, rejected };
  }

  /** @deprecated Use discoverCryptoMarkets. */
  async discoverBitcoinMarkets(slugs: readonly string[] = [], maxMarkets = 20): Promise<DiscoveredMarket[]> {
    const report = await this.discoverCryptoMarkets(slugs, maxMarkets);
    return report.qualified;
  }

  private async listCryptoEventMarkets(): Promise<unknown[]> {
    const seen = new Set<string>();
    const markets: unknown[] = [];
    const paginator = this.client.listEvents({ closed: false, tagSlug: "crypto", pageSize: 50, relatedTags: true });
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
      if (pages >= 8 || !page.hasMore || markets.length >= 400) break;
    }
    return markets;
  }

  registerManualToken(tokenId: string, marketId = tokenId, outcome: "UP" | "DOWN" = "UP", asset: string | null = null): void {
    this.marketsByToken.set(tokenId, {
      id: marketId, slug: marketId, question: marketId, title: marketId, asset, endDate: null, active: true,
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
      }
      this.closeCurrentStream?.();
    }
    return { changed, previous, next };
  }

  async run(tokenIds: readonly string[], handlers: PolymarketFeedHandlers, signal: AbortSignal): Promise<void> {
    this.setTokenIds(this.tokenIds.length > 0 ? this.tokenIds : tokenIds);
    const backoff = new ExponentialBackoff({ initialDelayMs: 500, maxDelayMs: 30_000, multiplier: 2, jitterRatio: 0.2 });
    let reconnectCount = 0;
    while (!signal.aborted) {
      if (this.tokenIds.length === 0) {
        await handlers.onConnection({ feed: "polymarket", state: "STOPPED", at: new Date().toISOString(), reconnectCount, message: "NO_QUALIFIED_TOKENS" });
        try { await this.waitForTokens(signal); } catch { break; }
        continue;
      }
      await handlers.onConnection({ feed: "polymarket", state: "CONNECTING", at: new Date().toISOString(), reconnectCount });
      let resubscribed = false;
      try {
        const current = [...this.tokenIds];
        if (current.length === 0) continue;
        const stream = await this.client.subscribe([{ topic: "market", tokenIds: current, customFeatureEnabled: true }]);
        this.closeCurrentStream = () => { resubscribed = true; void stream.close(); };
        backoff.reset();
        await handlers.onConnection({ feed: "polymarket", state: "CONNECTED", at: new Date().toISOString(), reconnectCount });
        for await (const event of stream) {
          if (signal.aborted) { await stream.close(); break; }
          for (const snapshot of this.normalizeEvent(event)) await handlers.onSnapshot(snapshot);
        }
        this.closeCurrentStream = null;
        if (signal.aborted) break;
        if (resubscribed || this.tokenIds.length === 0) continue;
        throw new Error("POLYMARKET_STREAM_ENDED");
      } catch (error) {
        this.closeCurrentStream = null;
        if (signal.aborted) break;
        if (resubscribed) continue;
        reconnectCount += 1;
        const normalized = error instanceof Error ? error : new Error(String(error));
        await handlers.onError(normalized);
        await handlers.onConnection({ feed: "polymarket", state: "DISCONNECTED", at: new Date().toISOString(), reconnectCount, message: normalized.message });
        await abortableDelay(backoff.nextDelay(), signal).catch(() => undefined);
      }
    }
    this.closeCurrentStream = null;
    await this.client.closeSubscriptions();
    await handlers.onConnection({ feed: "polymarket", state: "STOPPED", at: new Date().toISOString(), reconnectCount });
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

  private normalizeEvent(event: unknown): MarketSnapshot[] {
    const value = event as { type?: string; payload?: Record<string, unknown> };
    const payload = value.payload ?? {};
    if (value.type === "book") {
      const tokenId = String(payload.tokenId ?? "");
      const market = this.marketsByToken.get(tokenId);
      if (!tokenId || !market || !this.tokenIds.includes(tokenId)) return [];
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
        if (!state || !this.tokenIds.includes(tokenId)) continue;
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
      outcome: token?.outcome ?? "UP", asset: market.asset, bids: sortedLevels(state.bids, "desc"), asks: sortedLevels(state.asks, "asc"),
      lastTradePrice: state.lastTradePrice, tickSize: nullableString(raw.tickSize), minOrderSize: nullableString(raw.minOrderSize),
      feeSchedule: market.feeSchedule, sourceTimestamp: state.timestamp, receivedAt: new Date().toISOString(),
      sequence: ++this.sequence, hash: state.hash,
    });
  }
}

interface ParsedCandidate {
  input: Parameters<typeof qualifyMarket>[0];
  feeSchedule: MarketSnapshot["feeSchedule"];
  raw: Record<string, unknown>;
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
  const rawFee = trading?.feeSchedule as Record<string, unknown> | null | undefined;
  const feesEnabled = trading?.feesEnabled === true || value.feesEnabled === true;
  const feeSchedule = feesEnabled && rawFee ? {
    rate: String(rawFee.rate), exponent: Number(rawFee.exponent), takerOnly: Boolean(rawFee.takerOnly),
    rebateRate: String(rawFee.rebateRate ?? 0), source: "market_fee_schedule" as const, effectiveAt: new Date().toISOString(),
  } : feesEnabled && !rawFee ? null : {
    rate: "0", exponent: 1, takerOnly: true, rebateRate: "0", source: "market_fee_schedule" as const, effectiveAt: new Date().toISOString(),
  };
  const tags = collectTags(value);
  const yesPrice = nullableString(outcomes?.yes?.price) ?? firstPrice(value, 0);
  const noPrice = nullableString(outcomes?.no?.price) ?? firstPrice(value, 1);
  const liquidity = nullableString(metrics?.liquidityNum ?? metrics?.liquidity ?? trading?.liquidity ?? value.liquidityNum ?? value.liquidity);
  const acceptingOrders = state?.acceptingOrders !== false && value.acceptingOrders !== false && state?.enableOrderBook !== false;
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
      endDate: nullableString(state?.endDate ?? value.endDateIso ?? value.endDate),
      yesTokenId: yesToken ?? (Array.isArray(value.clobTokenIds) ? String((value.clobTokenIds as unknown[])[0]) : null),
      noTokenId: noToken ?? (Array.isArray(value.clobTokenIds) ? String((value.clobTokenIds as unknown[])[1]) : null),
      yesPrice, noPrice, liquidityUsd: liquidity, spread: numberOrNull(prices?.spread) ?? impliedSpread(yesPrice, noPrice) ?? numberOrNull(value.spread),
      feeSchedule, feesUnknown: feesEnabled && rawFee == null,
      yesLabel: outcomeLabel(value, 0, outcomes?.yes), noLabel: outcomeLabel(value, 1, outcomes?.no),
    },
  };
}

function toDiscoveredMarket(candidate: ParsedCandidate, qualification: QualificationResult): DiscoveredMarket {
  const input = candidate.input;
  const yesOutcome = qualification.binary.yesOutcome ?? "UP";
  const noOutcome = qualification.binary.noOutcome ?? "DOWN";
  return {
    id: input.id, slug: input.slug, question: input.title, title: input.title, asset: qualification.asset,
    endDate: input.endDate, active: input.active && !input.closed,
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
