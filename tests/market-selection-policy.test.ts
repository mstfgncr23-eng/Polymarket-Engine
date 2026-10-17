import { describe, expect, it } from "vitest";
import { engineEnvSchema, qualificationConfigSchema } from "@pm/core";
import { PolymarketMarketDataAdapter, marketSelectionTier } from "@pm/market-data";
import { officialMarketStartReason } from "../apps/engine/src/first-leg-admission.js";

const NOW = Date.parse("2026-10-17T00:30:00.000Z");
const WINDOW_5M = 5 * 60_000;
const WINDOW_15M = 15 * 60_000;

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

/** Window market with an aligned slug; startDeltaMs relative to NOW (negative = already started). */
function windowMarket(ticker: string, windowMs: number, startDeltaMs: number, extra: Record<string, unknown> = {}) {
  const startMs = NOW + startDeltaMs;
  const endMs = startMs + windowMs;
  const minutes = Math.round(windowMs / 60_000);
  const slug = `${ticker}-updown-${minutes}m-${Math.floor(startMs / 1000)}`;
  return clientMarket(slug, new Date(endMs).toISOString(), { eventStartTime: new Date(startMs).toISOString(), ...extra });
}

/** Structurally qualified but missing start metadata (guard permanently rejects). */
function unknownStartMarket(ticker: string, endMs: number, extra: Record<string, unknown> = {}) {
  return clientMarket(`${ticker}-other-market`, new Date(endMs).toISOString(), { ...extra });
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

function clientEvent(market: ReturnType<typeof clientMarket>, tags = ["crypto", "up-or-down"]) {
  return {
    slug: market.slug, title: market.question,
    state: { endDate: market.state.endDate },
    tags: tags.map((slug) => ({ slug, label: slug })),
    markets: [market],
  };
}

async function discover(events: unknown[], maxMarkets: number, selectionPolicy: "duration" | "readiness_economic") {
  const client = new RecordingEventsClient(events);
  const adapter = new PolymarketMarketDataAdapter(client as never);
  return adapter.discoverCryptoMarkets([], maxMarkets, { selectionPolicy }, new Date(NOW));
}

describe("marketSelectionTier (mirrors officialMarketStartReason start metadata)", () => {
  it("classifies missing / listing-only / conflicting start metadata as never-tradable (-1)", () => {
    const slug = `btc-updown-5m-${Math.floor(NOW / 1000)}`;
    const end = new Date(NOW + WINDOW_5M).toISOString();
    expect(marketSelectionTier(undefined, end, slug, NOW)).toBe(-1);
    expect(marketSelectionTier({}, end, slug, NOW)).toBe(-1);
    expect(marketSelectionTier({ startDate: new Date(NOW - 100_000).toISOString() }, end, slug, NOW)).toBe(-1);
    expect(marketSelectionTier({ eventStartTime: "2026-10-15T02:00:00" }, end, slug, NOW)).toBe(-1);
    expect(marketSelectionTier({
      eventStartTime: new Date(NOW).toISOString(),
      event: { eventStartTime: new Date(NOW + 1000).toISOString() },
    }, end, slug, NOW)).toBe(-1);
  });

  it("uses the official fixing start, never listing startDate, and accepts timezones", () => {
    const slug = `btc-updown-5m-${Math.floor(NOW / 1000)}`;
    const end = new Date(NOW + WINDOW_5M).toISOString();
    expect(marketSelectionTier({ event: { schedule: { startTime: new Date(NOW).toISOString() } } }, end, slug, NOW)).toBe(0);
    expect(marketSelectionTier({ eventStartTime: "2026-10-17T05:30:00+05:00" }, end, slug, NOW)).toBe(0);
  });

  it("rejects slug/window misalignment (the guard also rejects it)", () => {
    const misalignedSlug = `btc-updown-5m-${Math.floor((NOW - 60_000) / 1000)}`;
    const end = new Date(NOW + WINDOW_5M).toISOString();
    expect(marketSelectionTier({ eventStartTime: new Date(NOW).toISOString() }, end, misalignedSlug, NOW)).toBe(-1);
  });

  it("ranks started=0, starting <=15m=1, <=60m=2, later=3", () => {
    const slug = (startMs: number) => `btc-updown-5m-${Math.floor(startMs / 1000)}`;
    const end = (startMs: number) => new Date(startMs + WINDOW_5M).toISOString();
    expect(marketSelectionTier({ eventStartTime: new Date(NOW).toISOString() }, end(NOW), slug(NOW), NOW)).toBe(0);
    expect(marketSelectionTier({ eventStartTime: new Date(NOW + 10_000).toISOString() }, end(NOW + 10_000), slug(NOW + 10_000), NOW)).toBe(1);
    expect(marketSelectionTier({ eventStartTime: new Date(NOW + 20 * 60_000).toISOString() }, end(NOW + 20 * 60_000), slug(NOW + 20 * 60_000), NOW)).toBe(2);
    expect(marketSelectionTier({ eventStartTime: new Date(NOW + 2 * 3_600_000).toISOString() }, end(NOW + 2 * 3_600_000), slug(NOW + 2 * 3_600_000), NOW)).toBe(3);
  });

  it("parity: tier -1 <=> guard returns MISSING; tier >= 0 <=> guard allows the window", () => {
    const slug = (startMs: number) => `btc-updown-5m-${Math.floor(startMs / 1000)}`;
    const end = (startMs: number) => new Date(startMs + WINDOW_5M).toISOString();
    const fixtures = [
      { raw: {}, endDate: end(NOW), slug: slug(NOW) },
      { raw: { startDate: new Date(NOW - 100_000).toISOString() }, endDate: end(NOW), slug: slug(NOW) },
      { raw: { eventStartTime: new Date(NOW).toISOString() }, endDate: end(NOW), slug: slug(NOW) },
      { raw: { eventStartTime: new Date(NOW + 10_000).toISOString() }, endDate: end(NOW + 10_000), slug: slug(NOW + 10_000) },
      { raw: { eventStartTime: new Date(NOW).toISOString() }, endDate: end(NOW), slug: slug(NOW - 60_000) },
      { raw: { eventStartTime: new Date(NOW).toISOString(), event: { eventStartTime: new Date(NOW + 1).toISOString() } }, endDate: end(NOW), slug: slug(NOW) },
    ];
    for (const fixture of fixtures) {
      const tier = marketSelectionTier(fixture.raw, fixture.endDate, fixture.slug, NOW);
      const reason = officialMarketStartReason({ slug: fixture.slug, endDate: fixture.endDate, raw: fixture.raw }, NOW);
      if (tier === -1) {
        expect(["ENTRY_MARKET_START_MISSING", "ENTRY_MARKET_START_CONFLICT"]).toContain(reason);
      } else {
        expect(reason === null || reason === "ENTRY_MARKET_NOT_STARTED").toBe(true);
      }
    }
  });
});

describe("selection policy: 40-cap ranking", () => {
  it("config defaults preserve the historical duration-only policy", () => {
    expect(qualificationConfigSchema.parse({}).selectionPolicy).toBe("duration");
    expect(qualificationConfigSchema.parse({ selectionPolicy: "readiness_economic" }).selectionPolicy).toBe("readiness_economic");
    expect(engineEnvSchema.parse({}).MARKET_SELECTION_POLICY).toBe("duration");
  });

  it("duration policy puts nearest expiry first even when that market has not started", async () => {
    // future5: 5m window starting in 4m -> remaining 9m (tier 1)
    const future5 = windowMarket("eth", WINDOW_5M, 4 * 60_000);
    // started15: 15m window started 5m ago -> remaining 10m (tier 0)
    const started15 = windowMarket("btc", WINDOW_15M, -5 * 60_000);
    const report = await discover([clientEvent(started15), clientEvent(future5)], 40, "duration");
    expect(report.qualified.map((m) => m.slug)).toEqual([future5.slug, started15.slug]);
  });

  it("readiness_economic ranks started-now before not-yet-started, preserving the 40 cap", async () => {
    const future5 = windowMarket("eth", WINDOW_5M, 4 * 60_000);
    const started15 = windowMarket("btc", WINDOW_15M, -5 * 60_000);
    const report = await discover([clientEvent(started15), clientEvent(future5)], 40, "readiness_economic");
    expect(report.qualified.map((m) => m.slug)).toEqual([started15.slug, future5.slug]);
  });

  it("never-tradable (missing start metadata) markets rank last and cannot crowd out tradable slots", async () => {
    const startedA = windowMarket("btc", WINDOW_5M, -60_000);
    const futureB = windowMarket("eth", WINDOW_5M, 2 * 60_000);
    const neverTradable = unknownStartMarket("doge", NOW + 30 * 60_000, {
      prices: { spread: 0.01 }, metrics: { liquidityNum: "500000", liquidity: "500000" },
    });
    const report = await discover([clientEvent(startedA), clientEvent(futureB), clientEvent(neverTradable)], 2, "readiness_economic");
    expect(report.discovered.map((m) => m.slug)).toContain(neverTradable.slug);
    expect(report.qualified.map((m) => m.slug)).toEqual([startedA.slug, futureB.slug]);
  });

  it("inside the same readiness tier it prefers tick-floor spread, then liquidity", async () => {
    const wide = windowMarket("btc", WINDOW_5M, -60_000, { prices: { spread: 0.02 }, metrics: { liquidityNum: "9000", liquidity: "9000" } });
    const tight = windowMarket("eth", WINDOW_5M, -2 * 60_000, { prices: { spread: 0.01 }, metrics: { liquidityNum: "200", liquidity: "200" } });
    const deep = windowMarket("sol", WINDOW_5M, -3 * 60_000, { prices: { spread: 0.02 }, metrics: { liquidityNum: "100", liquidity: "100" } });
    const report = await discover([clientEvent(wide), clientEvent(tight), clientEvent(deep)], 2, "readiness_economic");
    expect(report.qualified.map((m) => m.slug)).toEqual([tight.slug, wide.slug]);
  });

  it("duration policy keeps the historical order byte-identical for the same input", async () => {
    const later = windowMarket("eth", WINDOW_5M, 3 * 60_000);
    const sooner = windowMarket("btc", WINDOW_5M, 2 * 60_000);
    const report = await discover([clientEvent(later), clientEvent(sooner)], 40, "duration");
    expect(report.qualified.map((m) => m.slug)).toEqual([sooner.slug, later.slug]);
  });
});


