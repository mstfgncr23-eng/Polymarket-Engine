import { describe, expect, it } from "vitest";
import { bookTiming, firstLegBookReason, officialMarketStartReason, firstLegEntryFilterReason, firstLegExecutionEconomics } from "../apps/engine/src/first-leg-admission.js";
import { EngineService } from "../apps/engine/src/engine.js";
import { MemoryEventJournal } from "@pm/db";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv, paperFill } from "./paper-engine-testkit.js";
import { snapshot, order } from "./fixtures.js";
import type { MarketSnapshot, PaperOrder } from "@pm/core";

const NOW = Date.parse("2026-10-15T02:00:00Z");
const book = (extra: Partial<MarketSnapshot> = {}) => snapshot({ marketId: "entry", tokenId: "entry-up", outcome: "UP", asset: "XRP",
  sourceTimestamp: new Date(NOW - 100).toISOString(), receivedAt: new Date(NOW - 10).toISOString(),
  wsCallbackAt: new Date(NOW - 20).toISOString(), bids: [{ price: "0.69", quantity: "100" }], asks: [{ price: "0.70", quantity: "100" }], ...extra });
const reason = (b: MarketSnapshot | null, extra = {}) => firstLegBookReason({ marketId: "entry", tokenId: "entry-up", outcome: "UP",
  canonicalTokenId: "entry-up", synchronized: true, book: b, nowMs: NOW, maxAgeMs: 1000, ...extra });

describe("P0-06 first-leg book authority", () => {
  it("accepts current book and separates source, queue, and decision latency", () => {
    expect(reason(book())).toBe(null);
    expect(bookTiming(book(), NOW)).toEqual({ sourceAgeMs: 100, receivedAgeMs: 10, sourceToIngressMs: 80, ingressToReceivedMs: 10, receivedToDecisionMs: 10 });
    expect(bookTiming(book({ wsCallbackAt: undefined }), NOW).sourceToIngressMs).toBe(null);
  });
  it.each([
    ["sourceTimestamp", new Date(NOW - 1001).toISOString(), "ENTRY_BOOK_SOURCE_STALE"],
    ["receivedAt", new Date(NOW - 1001).toISOString(), "ENTRY_BOOK_LOCAL_STALE"],
    ["sourceTimestamp", new Date(NOW + 1).toISOString(), "ENTRY_BOOK_FUTURE"],
    ["sourceTimestamp", "invalid", "ENTRY_BOOK_TIMESTAMP_INVALID"],
    ["tokenId", "other", "ENTRY_BOOK_IDENTITY_CHANGED"],
    ["marketId", "other", "ENTRY_BOOK_IDENTITY_CHANGED"],
    ["outcome", "DOWN", "ENTRY_BOOK_IDENTITY_CHANGED"],
    ["eventId", "", "ENTRY_BOOK_PROVENANCE_MISSING"],
    ["wsCallbackAt", new Date(NOW - 1001).toISOString(), "ENTRY_BOOK_LOCAL_BACKLOG"],
  ])("fails closed for %s=%s", (key, value, expected) => expect(reason({ ...book(), [key]: value } as MarketSnapshot)).toBe(expected));
  it("rejects missing, unsynchronized, replaced and canonical-token-changed data", () => {
    expect(reason(null)).toBe("ENTRY_BOOK_UNAVAILABLE");
    expect(reason(book(), { synchronized: false })).toBe("ENTRY_BOOK_UNAVAILABLE");
    expect(reason(book(), { canonicalTokenId: "new" })).toBe("ENTRY_BOOK_IDENTITY_CHANGED");
    expect(reason(book(), { expected: book({ sequence: 9 }) })).toBe("ENTRY_BOOK_CHANGED");
  });
  it.each(["fresh", "stale", "async-replaced", "async-stale", "before-start", "missing-start", "low-price", "low-roi", "boundary"])("guards the actual durable first BUY with %s and leaves cash intact on veto", async scenario => {
    const adapter = new ScriptedMarketAdapter();
    const market = qualified("entry", "XRP", "entry-up", "entry-down", new Date(NOW + 900000).toISOString());
    market.raw = { eventStartTime: new Date(NOW - 1).toISOString() };
    if (scenario === "before-start") market.raw = { eventStartTime: new Date(NOW + 1).toISOString() };
    if (scenario === "missing-start") market.raw = {};
    adapter.queue(discoveryReport([market]));
    const journal = new MemoryEventJournal();
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: adapter as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(), now: () => new Date(NOW) });
    await engine.start();
    try {
      const b = book(scenario === "stale" ? { sourceTimestamp: new Date(NOW - 60000).toISOString() } : {});
      if (scenario === "low-price") b.asks[0]!.price = "0.6499";
      if (scenario === "boundary") { b.asks[0]!.price = "0.65"; b.feeSchedule = { ...b.feeSchedule!, rate: "0" }; }
      adapter.markTokenSync(b.tokenId, "SYNCED"); engine.state.marketSnapshots.set(b.tokenId, b);
      const originalRead = journal.readDurableByOperationKey.bind(journal);
      let reads = 0;
      journal.readDurableByOperationKey = async key => {
        const result = await originalRead(key);
        if (key.startsWith("fill:") && ++reads === 2 && scenario.startsWith("async")) {
          engine.state.marketSnapshots.set(b.tokenId, { ...b, eventId: "new-book", sequence: 2,
            ...(scenario === "async-stale" ? { sourceTimestamp: new Date(NOW - 60000).toISOString() } : {}) });
        }
        return result;
      };
      const before = engine.ledger.snapshot().cash;
      const executionOrder = order("TAKER", { marketId: "entry", tokenId: b.tokenId, quantity: "2", requestedPrice: b.asks[0]!.price,
        entryAdmission: { fairProbability: scenario === "low-roi" ? 0.71 : scenario === "boundary" ? 0.65975 : 0.9, signalExpiresAt: new Date(NOW + 60000).toISOString() } });
      await (engine as unknown as { applyFill: (f: ReturnType<typeof paperFill>, e: { intent: "OPEN_FIRST_LEG"; executionSnapshot: MarketSnapshot; executionOrder: PaperOrder }) => Promise<boolean> })
        .applyFill(paperFill("entry", "UP", "0.70", { tokenId: b.tokenId, quantity: "2" }), { intent: "OPEN_FIRST_LEG", executionSnapshot: b, executionOrder });
      const rows = await journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 10 });
      const accepted = scenario === "fresh" || scenario === "boundary";
      expect(rows).toHaveLength(accepted ? 1 : 0);
      if (!accepted) expect(engine.ledger.snapshot().cash).toBe(before);
      expect(engine.integrityFault()).toBe(false);
    } finally { await engine.stop(); }
  });
});

describe("direct first-entry price and net ROI policy", () => {
  it.each([0.6499, 0.6500, 0.6501].flatMap(p => [0.01499, 0.01500, 0.01501].map(roi => [p, roi])))
    ("price=%s ROI=%s", (price, roi) => expect(firstLegEntryFilterReason(price, roi)).toBe(price! < 0.65 ? "FIRST_LEG_PRICE_BELOW_FLOOR" : roi! < 0.015 ? "FIRST_LEG_NET_ROI_BELOW_FLOOR" : null));
  it("rejects missing, NaN and infinite inputs", () => {
    for (const price of [undefined, null, NaN, Infinity, "garbage"]) expect(firstLegEntryFilterReason(price, 1)).not.toBe(null);
    for (const roi of [undefined, null, NaN, Infinity]) expect(firstLegEntryFilterReason("0.65", roi)).not.toBe(null);
  });
  it("re-walks full executable depth, preserves fees and rejects a late price or ROI deterioration", () => {
    const o = order("TAKER", { requestedPrice: "0.90", quantity: "10", entryAdmission: { fairProbability: 0.8, signalExpiresAt: new Date(NOW + 1000).toISOString() } });
    expect(firstLegExecutionEconomics(o, book(), NOW).reason).toBe(null);
    expect(firstLegExecutionEconomics(o, book({ asks: [{ price: "0.64", quantity: "100" }] }), NOW).reason).toBe("FIRST_LEG_PRICE_BELOW_FLOOR");
    expect(firstLegExecutionEconomics(o, book({ asks: [{ price: "0.79", quantity: "100" }] }), NOW).reason).toBe("FIRST_LEG_NET_ROI_BELOW_FLOOR");
    expect(firstLegExecutionEconomics(o, book({ asks: [{ price: "0.70", quantity: "2" }] }), NOW).reason).toBe("ENTRY_ECONOMICS_UNAVAILABLE");
    expect(firstLegExecutionEconomics(o, book({ feeSchedule: null }), NOW).reason).toBe("ENTRY_ECONOMICS_UNAVAILABLE");
    expect(firstLegExecutionEconomics(o, book(), NOW + 1001).reason).toBe("ENTRY_ECONOMICS_UNAVAILABLE");
  });
});

describe("P0-01 official fixing start", () => {
  const market = { slug: `xrp-updown-15m-${NOW / 1000}`, endDate: new Date(NOW + 900000).toISOString(), raw: { eventStartTime: new Date(NOW).toISOString() } };
  it.each([[-1, "ENTRY_MARKET_NOT_STARTED"], [0, null], [1, null]])("start boundary %s ms", (delta, result) => {
    expect(officialMarketStartReason(market, NOW + delta!)).toBe(result);
  });
  it("rejects missing, listing-only, ambiguous timezone and conflicting fixing metadata", () => {
    expect(officialMarketStartReason(undefined, NOW)).toBe("ENTRY_MARKET_START_MISSING");
    for (const raw of [{}, { startDate: new Date(NOW - 100000).toISOString() }, { eventStartTime: "2026-10-15T02:00:00" }])
      expect(officialMarketStartReason({ ...market, raw }, NOW)).toBe("ENTRY_MARKET_START_MISSING");
    expect(officialMarketStartReason({ ...market, raw: { eventStartTime: new Date(NOW + 1).toISOString() } }, NOW)).toBe("ENTRY_MARKET_START_CONFLICT");
  });
  it("uses epoch time across timezones", () => {
    expect(officialMarketStartReason({ ...market, raw: { eventStartTime: "2026-10-15T05:00:00+03:00" } }, NOW)).toBe(null);
  });
  it("accepts the official SDK event schedule fixing time, never its listing startDate", () => {
    expect(officialMarketStartReason({ ...market, raw: { event: { schedule: { startTime: new Date(NOW).toISOString() } } } }, NOW)).toBe(null);
    expect(officialMarketStartReason({ ...market, raw: { event: { schedule: { startDate: new Date(NOW).toISOString() } } } }, NOW)).toBe("ENTRY_MARKET_START_MISSING");
  });
});
