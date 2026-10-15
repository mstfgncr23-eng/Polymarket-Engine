import { describe, expect, it } from "vitest";
import { bookTiming, firstLegBookReason } from "../apps/engine/src/first-leg-admission.js";
import { EngineService } from "../apps/engine/src/engine.js";
import { MemoryEventJournal } from "@pm/db";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv, paperFill } from "./paper-engine-testkit.js";
import { snapshot } from "./fixtures.js";
import type { MarketSnapshot } from "@pm/core";

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
  it.each(["fresh", "stale", "async-replaced", "async-stale"])("guards the actual durable first BUY with %s and leaves cash intact on veto", async scenario => {
    const adapter = new ScriptedMarketAdapter();
    adapter.queue(discoveryReport([qualified("entry", "XRP", "entry-up", "entry-down", new Date(NOW + 900000).toISOString())]));
    const journal = new MemoryEventJournal();
    const engine = new EngineService(qualifiedPaperEnv(), journal, { error: () => undefined }, {
      market: adapter as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(), now: () => new Date(NOW) });
    await engine.start();
    try {
      const b = book(scenario === "stale" ? { sourceTimestamp: new Date(NOW - 60000).toISOString() } : {});
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
      await (engine as unknown as { applyFill: (f: ReturnType<typeof paperFill>, e: { intent: "OPEN_FIRST_LEG"; executionSnapshot: MarketSnapshot }) => Promise<boolean> })
        .applyFill(paperFill("entry", "UP", "0.70", { tokenId: b.tokenId, quantity: "2" }), { intent: "OPEN_FIRST_LEG", executionSnapshot: b });
      const rows = await journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 10 });
      expect(rows).toHaveLength(scenario === "fresh" ? 1 : 0);
      if (scenario !== "fresh") expect(engine.ledger.snapshot().cash).toBe(before);
      expect(engine.integrityFault()).toBe(false);
    } finally { await engine.stop(); }
  });
});
