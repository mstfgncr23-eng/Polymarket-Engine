import {
  createId,
  marketSnapshotSchema,
  type MarketSnapshot,
} from "@pm/core";
import type { DiscoveredMarket, PolymarketFeedAnomaly, PolymarketFeedHandlers } from "@pm/market-data";

/**
 * Frozen copy of main's pre-optimization normalize semantics:
 * mutate one Map level, convert+sort both sides, Zod-validate the complete snapshot.
 * Used only as a replay oracle. Not imported by production code.
 */
export class ReferencePolymarketNormalizer {
  private readonly marketsByToken = new Map<string, DiscoveredMarket>();
  private readonly books = new Map<string, {
    marketId: string;
    bids: Map<string, string>;
    asks: Map<string, string>;
    lastTradePrice: string | null;
    minOrderSize: string | null;
    timestamp: string;
    hash: string | null;
  }>();
  private sequence = 0;
  private lastTimestampByToken = new Map<string, number>();
  private tokenIds: string[] = [];

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

  getMarket(tokenId: string) { return this.marketsByToken.get(tokenId); }

  setTokenIds(tokenIds: readonly string[]): void {
    this.tokenIds = [...new Set(tokenIds.filter(Boolean))];
  }

  lastAcceptedTimestamp(tokenId: string): number | undefined {
    return this.lastTimestampByToken.get(tokenId);
  }

  normalizeSourceEvent(event: unknown, handlers: Pick<PolymarketFeedHandlers, "onAnomaly"> = {}): MarketSnapshot[] {
    const value = event as { type?: string; payload?: Record<string, unknown> };
    const payload = value.payload ?? {};
    if (value.type === "book") {
      const tokenId = String(payload.tokenId ?? "");
      const market = this.marketsByToken.get(tokenId);
      if (!tokenId || !market || !this.tokenIds.includes(tokenId)) return [];
      const sourceMs = parseSourceTimestamp(payload.timestamp);
      if (!this.acceptTimestamp(tokenId, sourceMs, handlers)) return [];
      const acceptedMs = sourceMs as number;
      const state = {
        marketId: String(payload.market ?? market.id),
        bids: levelsToMap(payload.bids),
        asks: levelsToMap(payload.asks),
        lastTradePrice: nullableString(payload.lastTradePrice),
        // Mirror production: preserve the last known value when a full book omits it.
        minOrderSize: nullableString(payload.minOrderSize) ?? this.books.get(tokenId)?.minOrderSize ?? null,
        timestamp: new Date(acceptedMs).toISOString(),
        hash: nullableString(payload.hash),
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
        if (!this.acceptTimestamp(tokenId, sourceMs, handlers)) continue;
        const acceptedMs = sourceMs as number;
        const side = String(change.side);
        const target = side === "BUY" ? state.bids : state.asks;
        const price = String(change.price ?? "");
        const size = String(change.size ?? "0");
        if (Number(size) <= 0) target.delete(price); else target.set(price, size);
        state.timestamp = new Date(acceptedMs).toISOString();
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

  private acceptTimestamp(tokenId: string, sourceMs: number | null, handlers: Pick<PolymarketFeedHandlers, "onAnomaly">): boolean {
    if (sourceMs === null) {
      handlers.onAnomaly?.({
        kind: "malformed", tokenId, incomingMs: null, lastAcceptedMs: this.lastTimestampByToken.get(tokenId) ?? null,
        regressionMs: null, totalDropped: 0, sampled: true, at: new Date().toISOString(),
      } satisfies PolymarketFeedAnomaly);
      return false;
    }
    const previous = this.lastTimestampByToken.get(tokenId);
    if (previous !== undefined && sourceMs < previous) {
      handlers.onAnomaly?.({
        kind: "stale", tokenId, incomingMs: sourceMs, lastAcceptedMs: previous, regressionMs: previous - sourceMs,
        totalDropped: 0, sampled: true, at: new Date().toISOString(),
      } satisfies PolymarketFeedAnomaly);
      return false;
    }
    this.lastTimestampByToken.set(tokenId, sourceMs);
    return true;
  }

  private toSnapshot(
    tokenId: string,
    state: { bids: Map<string, string>; asks: Map<string, string>; lastTradePrice: string | null; minOrderSize: string | null; timestamp: string; hash: string | null },
    raw: Record<string, unknown>,
  ): MarketSnapshot {
    const market = this.marketsByToken.get(tokenId);
    if (!market) throw new Error("UNKNOWN_TOKEN");
    const token = market.tokens.find((item) => item.tokenId === tokenId);
    return marketSnapshotSchema.parse({
      eventId: createId("poly"),
      traceId: createId("trace"),
      marketId: market.id,
      tokenId,
      outcome: token?.outcome ?? "UP",
      asset: market.asset,
      bids: sortedLevels(state.bids, "desc"),
      asks: sortedLevels(state.asks, "asc"),
      lastTradePrice: state.lastTradePrice,
      tickSize: nullableString(raw.tickSize),
      // Mirror the production carry-forward: the last committed full-book
      // min_order_size remains authoritative for price_change-derived snapshots.
      minOrderSize: state.minOrderSize ?? nullableString(raw.minOrderSize),
      feeSchedule: market.feeSchedule,
      sourceTimestamp: state.timestamp,
      receivedAt: new Date().toISOString(),
      sequence: ++this.sequence,
      hash: state.hash,
    });
  }
}

function nullableString(value: unknown): string | null {
  return value === undefined || value === null || value === "" ? null : String(value);
}

function parseSourceTimestamp(value: unknown): number | null {
  if (value === undefined || value === null || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function levelsToMap(value: unknown): Map<string, string> {
  const result = new Map<string, string>();
  if (!Array.isArray(value)) return result;
  for (const level of value as Array<Record<string, unknown>>) result.set(String(level.price), String(level.size ?? level.quantity));
  return result;
}

function sortedLevels(map: Map<string, string>, order: "asc" | "desc") {
  return [...map.entries()]
    .sort(([left], [right]) => order === "asc" ? Number(left) - Number(right) : Number(right) - Number(left))
    .map(([price, quantity]) => ({ price, quantity }));
}
