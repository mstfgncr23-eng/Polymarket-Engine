import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MemoryEventJournal, type EventJournal } from "@pm/db";
import { p0PostgresFixture } from "./p0-postgres-fixture.js";
import type { MarketSnapshot, PaperOrder } from "@pm/core";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { discoveryReport, qualified, qualifiedPaperEnv, ScriptedMarketAdapter, ScriptedReferenceRegistry } from "./paper-engine-testkit.js";
import { order, snapshot } from "./fixtures.js";

const NOW = Date.parse("2026-10-15T12:00:00Z");
const makeBook = (sequence = 1, extra: Partial<MarketSnapshot> = {}) => snapshot({
  marketId: "execution-mkt", tokenId: "execution-up", outcome: "UP", asset: "XRP",
  eventId: `execution-book-${sequence}`, sequence, hash: `hash-${sequence}`,
  receivedAt: new Date(NOW - 10 + sequence).toISOString(), sourceTimestamp: new Date(NOW - 20 + sequence).toISOString(),
  bids: [{ price: "0.69", quantity: "100" }], asks: [{ price: "0.70", quantity: "100" }],
  ...extra,
});
type Submitter = { submitPaperOrder(o: PaperOrder, b: MarketSnapshot, key: string, intent: "OPEN_FIRST_LEG"): Promise<boolean> };

let pg: Awaited<ReturnType<typeof p0PostgresFixture>> | undefined;
beforeEach(async () => { if (process.env.P0_EXECUTION_TEST_DATABASE_URL) pg = await p0PostgresFixture(process.env.P0_EXECUTION_TEST_DATABASE_URL); });
afterEach(async () => { if (pg) { await pg.close(); pg = undefined; } });

async function setup(journal: EventJournal = pg?.journal ?? new MemoryEventJournal(), begin = true) {
  const adapter = new ScriptedMarketAdapter();
  const market = qualified("execution-mkt", "XRP", "execution-up", "execution-down", new Date(NOW + 900000).toISOString());
  market.raw = { eventStartTime: new Date(NOW - 1000).toISOString() };
  adapter.queue(discoveryReport([market]));
  const env = qualifiedPaperEnv({ PAPER_EXECUTION_STYLE: "TAKER", PAPER_EXECUTION_LATENCY_MS: "0", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0" });
  const engine = new EngineService(env, journal, { error: () => undefined }, {
    market: adapter as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(), now: () => new Date(NOW),
  });
  await engine.start(); await adapter.waitForHandlers();
  if (begin) expect((await engine.beginExperiment()).ok).toBe(true);
  adapter.markTokenSync("execution-up", "SYNCED");
  const a = makeBook(); engine.state.marketSnapshots.set(a.tokenId, a);
  const o = order("TAKER", { marketId: a.marketId, tokenId: a.tokenId, quantity: "2", requestedPrice: "0.70",
    entryAdmission: { fairProbability: 0.90, signalExpiresAt: new Date(NOW + 60000).toISOString() } });
  const submit = (candidate = o, decision = a) => (engine as unknown as Submitter).submitPaperOrder(candidate, decision, `entry:${candidate.id}`, "OPEN_FIRST_LEG");
  return { engine, journal, adapter, a, o, submit };
}

describe("first-leg finite execution lifecycle", () => {
  it("accepts a trusted newer book at submission when the original limit and net ROI remain valid", async () => {
    const s = await setup();
    try {
      s.engine.state.marketSnapshots.set(s.a.tokenId, makeBook(2));
      expect(await s.submit()).toBe(true);
      const fills = await s.journal.queryEvents({ types: ["PAPER_FILL"], limit: 10 });
      expect(fills).toHaveLength(1);
      expect(fills[0]!.payload.sourceMarketEventId).toBe("execution-book-2");
      expect((await s.journal.queryEvents({ types: ["PAPER_ORDER_TERMINAL"], limit: 10 }))[0]!.payload.status).toBe("FILLED");
    } finally { await s.engine.stop(); }
  });

  it.each(["same-economics", "stale-next-book", "expensive-next-book"])("a later %s book during PostgreSQL identity reads cannot revoke an already validated execution snapshot", async scenario => {
    const s = await setup();
    try {
      const read = s.journal.readDurableByOperationKey.bind(s.journal);
      s.journal.readDurableByOperationKey = async key => {
        const value = await read(key);
        if (key.startsWith("fill:")) s.engine.state.marketSnapshots.set(s.a.tokenId, makeBook(3, {
          ...(scenario === "stale-next-book" ? { sourceTimestamp: new Date(NOW - 60000).toISOString() } : {}),
          ...(scenario === "expensive-next-book" ? { asks: [{ price: "0.99", quantity: "100" }] } : {}),
        }));
        return value;
      };
      expect(await s.submit()).toBe(true);
      const fills = await s.journal.queryEvents({ types: ["PAPER_FILL"], limit: 10 });
      expect(fills).toHaveLength(1);
      expect(fills[0]!.payload.sourceMarketEventId).toBe(s.a.eventId);
      expect(fills[0]!.payload.fillPrice).toBe("0.7");
      expect(s.engine.ledger.positions.get(s.a.marketId)?.upQuantity).toBe("2");
      expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });

  it.each(["price", "roi", "depth", "stale", "unsynchronized"])("rejects a genuinely invalid %s execution book and records a terminal order result", async scenario => {
    const s = await setup();
    try {
      const append = s.journal.append.bind(s.journal);
      s.journal.append = async (event, options) => {
        await append(event, options);
        if (event.type === "PAPER_ORDER_CREATED") {
          s.engine.state.marketSnapshots.set(s.a.tokenId, makeBook(2, {
            ...(scenario === "price" ? { asks: [{ price: "0.71", quantity: "100" }] } : {}),
            ...(scenario === "depth" ? { asks: [{ price: "0.70", quantity: "1" }] } : {}),
            ...(scenario === "stale" ? { sourceTimestamp: new Date(NOW - 60000).toISOString() } : {}),
          }));
          if (scenario === "unsynchronized") s.adapter.markTokenSync(s.a.tokenId, "GAPPED");
        }
      };
      if (scenario === "roi") s.o.entryAdmission!.fairProbability = 0.71;
      // ROI must deteriorate after order creation, not fail the initial admission.
      if (scenario === "roi") {
        s.o.entryAdmission!.fairProbability = 0.90;
        const original = s.journal.append;
        s.journal.append = async (event, options) => { await original(event, options); if (event.type === "PAPER_ORDER_CREATED") s.o.entryAdmission!.fairProbability = 0.71; };
      }
      const cash = s.engine.ledger.snapshot().cash;
      expect(await s.submit()).toBe(false);
      expect(await s.journal.queryEvents({ types: ["PAPER_FILL"], limit: 10 })).toHaveLength(0);
      const terminal = await s.journal.queryEvents({ types: ["PAPER_ORDER_TERMINAL"], limit: 10 });
      expect(terminal).toHaveLength(1);
      expect(terminal[0]!.payload).toMatchObject({ orderId: s.o.id, status: "REJECTED" });
      expect(terminal[0]!.payload.reason).toBeTruthy();
      expect(s.engine.ledger.snapshot().cash).toBe(cash);
      expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });

  it("serializes concurrent first-leg plans for one market to a single economic BUY", async () => {
    const s = await setup();
    try {
      const other = { ...s.o, id: "second-concurrent-first-leg" };
      const results = await Promise.all([s.submit(), s.submit(other)]);
      expect(results.filter(Boolean)).toHaveLength(1);
      expect(await s.journal.queryEvents({ types: ["PAPER_FILL"], limit: 10 })).toHaveLength(1);
      expect(s.engine.ledger.positions.get(s.a.marketId)?.upQuantity).toBe("2");
    } finally { await s.engine.stop(); }
  });

  it.each(["BEFORE_DURABLE_COMMIT", "AFTER_DURABLE_COMMIT_BEFORE_APPLY", "AFTER_APPLY_BEFORE_DERIVED_AUDIT"] as const)("recovers %s with one terminal result and at most one economic fill", async point => {
    let s = await setup();
    const key = `fill:first-leg-fill:${s.o.id}`;
    try {
      s.engine.setCrashInjectionForTest(point, { operationKey: key });
      await expect(s.submit()).rejects.toThrow("CRASH_INJECTED");
      const journal = s.journal;
      await s.engine.stop();
      s = await setup(journal, false);
      const terminals = await journal.queryEvents({ types: ["PAPER_ORDER_TERMINAL"], limit: 10 });
      expect(terminals).toHaveLength(1);
      const committed = point !== "BEFORE_DURABLE_COMMIT";
      expect(terminals[0]!.payload.status).toBe(committed ? "FILLED" : "REJECTED");
      expect(await journal.queryDurableTransitions({ types: ["PAPER_FILL"], limit: 10 })).toHaveLength(committed ? 1 : 0);
      expect(s.engine.ledger.positions.size).toBe(committed ? 1 : 0);
      expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });
});
