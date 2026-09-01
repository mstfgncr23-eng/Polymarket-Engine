import { describe, expect, it } from "vitest";
import { engineEnvSchema, qualifyMarket } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import {
  PolymarketMarketDataAdapter,
  ReferenceFeedRegistry,
  endDateFromDirectionalSlug,
  resolveMarketEndDate,
  type DiscoveredMarket,
  type DiscoveryReport,
  type PolymarketFeedHandlers,
  type WebSocketLike,
} from "@pm/market-data";
import { EngineService } from "../apps/engine/src/engine.js";

function dummySocket(): WebSocketLike {
  const socket = new EventTarget() as EventTarget & { readyState: number; close: (code?: number, reason?: string) => void };
  socket.readyState = 0;
  socket.close = () => { socket.dispatchEvent(new Event("close")); };
  return socket;
}

function clientMarket(slug: string, endDate: string, extra: Record<string, unknown> = {}) {
  return {
    id: slug, slug, question: slug.replace(/-/g, " "),
    state: { active: true, closed: false, acceptingOrders: true, enableOrderBook: true, endDate },
    outcomes: { yes: { label: "Up", tokenId: `${slug}-up`, price: "0.48" }, no: { label: "Down", tokenId: `${slug}-down`, price: "0.51" } },
    metrics: { liquidityNum: "200", liquidity: "200" },
    prices: { spread: 0.02 },
    trading: { feesEnabled: true, feeSchedule: { exponent: 1, rate: "0.07", takerOnly: true, rebateRate: "0.2" } },
    tags: [],
    ...extra,
  };
}

function clientEvent(market: ReturnType<typeof clientMarket>, tags = ["crypto", "up-or-down"]) {
  return {
    slug: market.slug, title: market.question,
    state: { endDate: market.state.endDate },
    tags: tags.map((slug) => ({ slug, label: slug })),
    markets: [market],
  };
}

class RecordingEventsClient {
  lastRequest: Record<string, unknown> | null = null;
  constructor(private readonly events: unknown[]) {}
  listEvents(request: Record<string, unknown>) {
    this.lastRequest = request;
    const items = this.events;
    return {
      async *[Symbol.asyncIterator]() {
        yield { items, hasMore: false };
      },
    };
  }
  async fetchMarket(): Promise<never> { throw new Error("fetchMarket unused"); }
  async closeSubscriptions(): Promise<void> {}
}

class CountingMarketAdapter {
  discoverCalls = 0;
  tokenIds: string[] = [];
  constructor(private readonly report: DiscoveryReport) {}
  registerManualToken(): void { /* unused */ }
  async discoverCryptoMarkets(): Promise<DiscoveryReport> {
    this.discoverCalls += 1;
    return this.report;
  }
  setTokenIds(tokenIds: readonly string[]): { changed: boolean; previous: string[]; next: string[] } {
    const previous = [...this.tokenIds];
    this.tokenIds = [...tokenIds];
    return { changed: true, previous, next: this.tokenIds };
  }
  async run(_tokenIds: readonly string[], handlers: PolymarketFeedHandlers, signal: AbortSignal): Promise<void> {
    await handlers.onConnection({ feed: "polymarket", state: "CONNECTED", at: new Date().toISOString(), reconnectCount: 0 });
    await new Promise<void>((resolve) => {
      if (signal.aborted) { resolve(); return; }
      signal.addEventListener("abort", () => resolve(), { once: true });
    });
  }
}

function qualifiedReport(id: string, asset: string): DiscoveryReport {
  const market: DiscoveredMarket = {
    id, slug: `${asset.toLowerCase()}-updown-5m-1`, question: `${asset} Up or Down`, title: `${asset} Up or Down`,
    asset, endDate: new Date(Date.now() + 240_000).toISOString(), endDateSource: "state.endDate", remainingDurationMs: 240_000,
    active: true,
    tokens: [{ tokenId: `${id}-up`, outcome: "UP", price: "0.48" }, { tokenId: `${id}-down`, outcome: "DOWN", price: "0.51" }],
    feeSchedule: { rate: "0.07", exponent: 1, takerOnly: true, rebateRate: "0.2", source: "market_fee_schedule", effectiveAt: new Date().toISOString() },
    liquidityUsd: "200", spread: 0.02,
    qualification: { qualified: true, reasonCodes: ["QUALIFIED"], asset, crypto: true, binary: { safe: true, yesOutcome: "UP", noOutcome: "DOWN", reason: "QUALIFIED" }, duplicateOf: null },
    raw: {},
  };
  return { discovered: [market], qualified: [market], rejected: [] };
}

describe("short-duration discovery listing and expiry parsing", () => {
  it("uses official listEvents endDate window rather than unbounded crypto catalog order", async () => {
    const now = new Date("2026-09-01T15:58:00.000Z");
    const start = 1788278100;
    const current = clientMarket(`btc-updown-5m-${start}`, new Date(now.getTime() + 120_000).toISOString());
    const client = new RecordingEventsClient([clientEvent(current)]);
    const adapter = new PolymarketMarketDataAdapter(client as never);
    await adapter.discoverCryptoMarkets([], 20, {}, now);
    expect(client.lastRequest).toMatchObject({ closed: false, tagSlug: "crypto", relatedTags: true, order: "endDate", ascending: true });
    const min = client.lastRequest?.endDateMin as Date;
    const max = client.lastRequest?.endDateMax as Date;
    expect(min.getTime()).toBeLessThanOrEqual(now.getTime());
    expect(max.getTime()).toBeGreaterThan(now.getTime() + 3_600_000);
  });

  it("parses window expiry from state.endDate and ignores date-only endDateIso", () => {
    const resolved = resolveMarketEndDate({
      slug: "btc-updown-5m-1788278100",
      endDateIso: "2026-09-01",
      endDate: "2026-09-01T16:00:00Z",
      state: { endDate: "2026-09-01T16:05:00Z", endDateIso: "2026-09-01" },
    });
    expect(resolved.source).toBe("state.endDate");
    expect(resolved.iso).toBe("2026-09-01T16:05:00.000Z");
    expect(resolveMarketEndDate({ endDateIso: "2026-09-01" }).source).toBe("missing");
  });

  it("uses aligned slug-window unix as fallback expiry only", () => {
    const fromSlug = endDateFromDirectionalSlug("btc-updown-5m-1788278100");
    expect(fromSlug?.source).toBe("slug_window");
    expect(fromSlug?.iso).toBe("2026-09-01T16:00:00.000Z");
    expect(endDateFromDirectionalSlug("btc-updown-5m-1788278101")).toBeNull();
    expect(resolveMarketEndDate({ slug: "btc-updown-5m-1788278100" })).toEqual({
      iso: "2026-09-01T16:00:00.000Z", source: "slug_window",
    });
  });

  it("qualifies a current 5m Up/Down window and does not mark it DURATION_OUT_OF_RANGE", async () => {
    const now = new Date("2026-09-01T15:58:00.000Z");
    const current = clientMarket("btc-updown-5m-1788278100", "2026-09-01T16:02:00.000Z");
    const longDated = clientMarket("kraken-ipo-in-2025", "2027-01-01T05:00:00.000Z", {
      outcomes: { yes: { label: "Yes", tokenId: "yes-k", price: "0.4" }, no: { label: "No", tokenId: "no-k", price: "0.6" } },
    });
    const client = new RecordingEventsClient([clientEvent(current), clientEvent(longDated, ["crypto"])]);
    const adapter = new PolymarketMarketDataAdapter(client as never);
    const report = await adapter.discoverCryptoMarkets([], 20, {}, now);
    const btc = report.discovered.find((item) => item.slug.startsWith("btc-updown-5m"));
    const ipo = report.discovered.find((item) => item.slug === "kraken-ipo-in-2025");
    expect(btc?.endDateSource).toBe("state.endDate");
    expect(btc?.qualification.reasonCodes).toEqual(["QUALIFIED"]);
    expect(btc?.qualification.qualified).toBe(true);
    expect(btc?.asset).toBe("BTC");
    expect(btc?.tokens.map((token) => token.outcome)).toEqual(["UP", "DOWN"]);
    expect(ipo?.qualification.reasonCodes).toContain("DURATION_OUT_OF_RANGE");
    expect(report.qualified.some((item) => item.slug.startsWith("btc-updown-5m"))).toBe(true);
    const closest = adapter.closestDirectional(report, 20);
    expect(closest[0]?.slug).toContain("updown");
    expect(closest[0]?.endDateSource).toBe("state.endDate");
  });

  it("reports liquidity/spread/fee as the reject reason when duration is valid", async () => {
    const now = new Date("2026-09-01T15:58:00.000Z");
    const thin = clientMarket("eth-updown-5m-1788278100", "2026-09-01T16:02:00.000Z", {
      metrics: { liquidityNum: "1", liquidity: "1" },
      prices: { spread: 0.9 },
    });
    const client = new RecordingEventsClient([clientEvent(thin)]);
    const report = await new PolymarketMarketDataAdapter(client as never).discoverCryptoMarkets([], 20, {}, now);
    const eth = report.discovered[0];
    expect(eth?.qualification.reasonCodes).not.toContain("DURATION_OUT_OF_RANGE");
    expect(eth?.qualification.reasonCodes).toEqual(expect.arrayContaining(["INSUFFICIENT_LIQUIDITY", "SPREAD_TOO_WIDE"]));
  });
});

describe("transient reference recovery does not wait for discovery", () => {
  it("transient probe failure then automatic recovery starts the feed without rediscovery", async () => {
    let probes = 0;
    const feeds = new ReferenceFeedRegistry(
      "wss://example.invalid/ws",
      "https://example.invalid",
      15_000,
      dummySocket,
      async () => {
        probes += 1;
        if (probes === 1) return { kind: "transient_failure", detail: "timeout" };
        return { kind: "supported", detail: "trading" };
      },
      { negativeCacheTtlMs: 0 },
    );
    const market = new CountingMarketAdapter(qualifiedReport("eth-mkt", "ETH"));
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", GROK_ACTIVATION_PROVIDER: "noop",
      DISCOVERY_INTERVAL_MS: "3600000",
    });
    const engine = new EngineService(env, journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: feeds,
    });
    await engine.start();
    try {
      expect(market.discoverCalls).toBe(1);
      const first = await journal.readAll();
      expect(first.some((event) => event.type === "REFERENCE_PROBE_TRANSIENT")).toBe(true);
      expect(first.some((event) => event.type === "REFERENCE_FEED_STARTED")).toBe(false);
      expect(engine.runningReferenceAssets()).toEqual([]);
      await new Promise((resolve) => setTimeout(resolve, 120));
      expect(market.discoverCalls).toBe(1);
      const second = await journal.readAll();
      expect(second.some((event) => event.type === "REFERENCE_FEED_STARTED")).toBe(true);
      expect(engine.runningReferenceAssets()).toEqual(["ETH"]);
      expect(probes).toBeGreaterThanOrEqual(2);
    } finally {
      await engine.stop();
    }
  });
});

describe("slug window helper does not invent duration for unaligned timestamps", () => {
  it("keeps unknown-asset qualification rejection independent of expiry parsing", () => {
    const result = qualifyMarket({
      id: "x", slug: "up-or-down-now", title: "Up or Down", tags: ["crypto"],
      active: true, closed: false, acceptingOrders: true,
      endDate: new Date(Date.now() + 180_000).toISOString(),
      yesTokenId: "y", noTokenId: "n", yesPrice: "0.5", noPrice: "0.5",
      liquidityUsd: "200", spread: 0.02,
      feeSchedule: { rate: "0.07", exponent: 1, takerOnly: true, rebateRate: "0", source: "market_fee_schedule", effectiveAt: new Date().toISOString() },
      feesUnknown: false,
    }, new Map());
    expect(result.reasonCodes).toContain("UNKNOWN_UNDERLYING_ASSET");
    expect(result.reasonCodes).not.toContain("DURATION_OUT_OF_RANGE");
  });
});
