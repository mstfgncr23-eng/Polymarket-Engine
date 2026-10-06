import { describe, expect, it } from "vitest";
import { type MarketSnapshot, type ProfitTrailingPairState } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv, paperFill } from "./paper-engine-testkit.js";
import { snapshot } from "./fixtures.js";

const START = Date.parse("2026-10-05T00:00:00.000Z");
const MARKET = "trail-mkt";
const UP = "trail-up";
const DOWN = "trail-down";

type Internals = {
  experiment: { status: string; riskConfiguration: Record<string, unknown> } | null;
  profitTrailingPairStates: Map<string, ProfitTrailingPairState>;
  snapshots: Map<string, Partial<Record<"UP" | "DOWN", MarketSnapshot>>>;
  maybeProtectiveDeRisk(marketId: string): Promise<void>;
};
const internal = (engine: EngineService) => engine as unknown as Internals;

async function start(journal = new MemoryEventJournal(), clock = { ms: START }) {
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified(MARKET, "SOL", UP, DOWN, new Date(START + 3_600_000).toISOString())]));
  const errors: Array<{ detail: unknown; message?: string }> = [];
  const engine = new EngineService(qualifiedPaperEnv({
    DISCOVERY_INTERVAL_MS: "3600000",
    ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    PAPER_EXECUTION_STYLE: "TAKER",
    PAPER_EXECUTION_LATENCY_MS: "0",
    NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE: "0.12",
    PROFIT_TRAILING_PAIR_SOAK_ENABLED: "true",
    PROFIT_TRAILING_PAIR_ARM_EDGE: "0.05",
    PROFIT_TRAILING_PAIR_RETRACE_EDGE: "0.02",
  }), journal, { error: (detail: unknown, message?: string) => { errors.push({ detail, ...(message ? { message } : {}) }); } }, {
    market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(),
    now: () => new Date(clock.ms),
    resolutionFetcher: async () => null,
  });
  await engine.start();
  await market.waitForHandlers();
  return { engine, journal, market, clock, errors };
}
type Scenario = Awaited<ReturnType<typeof start>>;

function book(s: Scenario, outcome: "UP" | "DOWN", ask: string, sequence: number): MarketSnapshot {
  const askNumber = Number(ask);
  return snapshot({
    marketId: MARKET,
    tokenId: outcome === "UP" ? UP : DOWN,
    outcome,
    asset: "SOL",
    // Canonical risk keeps minLiquidityUsd as a hard safety gate. Give the
    // fixture ample book depth so these tests exercise trailing authority,
    // not an intentionally unrelated liquidity veto.
    bids: [{ price: Math.max(0.01, askNumber - 0.01).toFixed(2), quantity: "10000" }],
    asks: [{ price: ask, quantity: "10000" }],
    lastTradePrice: ask,
    sourceTimestamp: new Date(s.clock.ms).toISOString(),
    receivedAt: new Date(s.clock.ms).toISOString(),
    sequence,
    hash: `${outcome}-${sequence}`,
  });
}

async function publish(s: Scenario, downAsk: string, sequence: number) {
  await s.market.emitSnapshot(book(s, "UP", "0.81", sequence));
  await s.market.emitSnapshot(book(s, "DOWN", downAsk, sequence));
}

async function seedFirstLeg(s: Scenario) {
  const preflight = await s.engine.beginExperiment();
  expect(preflight.ok).toBe(true);
  await s.engine.applyObservedFill(paperFill(MARKET, "UP", "0.80", {
    tokenId: UP,
    quantity: "5",
    timestamp: new Date(s.clock.ms).toISOString(),
    feeUsd: "0",
  }));
}

async function waitFor(s: Scenario, label: string, predicate: () => boolean | Promise<boolean>) {
  const deadline = Date.now() + 1000;
  while (!(await predicate())) {
    if (Date.now() > deadline) {
      const position = s.engine.ledger.positions.get(MARKET) ?? null;
      const pair = internal(s.engine).snapshots.get(MARKET) ?? null;
      const trailing = await s.journal.queryDurableTransitions({ types: ["PROFIT_TRAILING_PAIR_STATE_UPDATED"], limit: 20 });
      throw new Error(`profit trailing lifecycle did not complete: ${label} :: ${JSON.stringify({
        experiment: internal(s.engine).experiment,
        database: s.engine.state.database,
        journalDegraded: s.engine.state.journalDegraded,
        polymarket: s.engine.state.connections.polymarket,
        position,
        pair,
        trailingState: internal(s.engine).profitTrailingPairStates.get(MARKET) ?? null,
        durableTrailing: trailing,
        errors: s.errors,
      })}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

async function restart(s: Scenario) {
  await s.engine.stop();
  return start(s.journal, s.clock);
}

describe("profit trailing pair PAPER authority", () => {
  it("persists high-water across compacted restart and trails profit after legacy deadline", async () => {
    let s = await start();
    try {
      await seedFirstLeg(s);

      await publish(s, "0.15", 1); // full executable gross edge .05 => arm
      await waitFor(s, "arm-.05", () => internal(s.engine).profitTrailingPairStates.get(MARKET)?.highWaterEdge === "0.05");

      s.clock.ms += 1_000;
      await publish(s, "0.10", 2); // gross edge .10 => high-water
      await waitFor(s, "high-water-.10", () => internal(s.engine).profitTrailingPairStates.get(MARKET)?.highWaterEdge === "0.1");

      // Cross the old 300s protection deadline. The trailing soak must not let
      // PR3 / Guardian / deadline fallback consume the still-profitable episode.
      s.clock.ms += 301_000;
      await internal(s.engine).maybeProtectiveDeRisk(MARKET);
      expect(s.engine.ledger.positions.get(MARKET)?.unpairedQuantity).toBe("5");
      expect(await s.journal.queryDurableTransitions({ types: ["RISK_REDUCTION_ADMITTED"], limit: 20 })).toHaveLength(0);

      await s.engine.checkpointForTest();
      const cursor = s.engine.lastAppliedDurableCursorForTest();
      await s.journal.compactDurableTransitionsThrough(cursor);
      s = await restart(s);
      expect(internal(s.engine).profitTrailingPairStates.get(MARKET)?.highWaterEdge).toBe("0.1");

      s.clock.ms += 1_000;
      await publish(s, "0.12", 3); // current edge .08 => .02 retrace from .10
      await waitFor(s, "pair-complete", () => s.engine.ledger.positions.get(MARKET)?.unpairedQuantity === "0");
      expect(internal(s.engine).profitTrailingPairStates.has(MARKET)).toBe(false);

      const fills = await s.journal.queryEvents({ types: ["PAPER_FILL"] });
      expect(fills.filter((row) => row.payload.intent === "HEDGE_SECOND_LEG")).toHaveLength(1);
      expect(fills.filter((row) => row.payload.intent === "PROTECTIVE_DE_RISK"
        || row.payload.intent === "RESCUE_SECOND_LEG"
        || row.payload.intent === "GUARDIAN_PROTECTIVE_SELL")).toHaveLength(0);
    } finally { await s.engine.stop(); }
  });

  it("locks a still-positive .02 gross retrace instead of being trapped by the normal alpha floor", async () => {
    const s = await start();
    try {
      await seedFirstLeg(s);
      await publish(s, "0.15", 1); // arm at .05 gross
      await waitFor(s, "arm-.05", () => internal(s.engine).profitTrailingPairStates.get(MARKET)?.highWaterEdge === "0.05");

      s.clock.ms += 1_000;
      await publish(s, "0.18", 2); // .02 gross: fee-inclusive total lock is still positive
      await waitFor(s, "pair-complete", () => s.engine.ledger.positions.get(MARKET)?.unpairedQuantity === "0");

      const orders = await s.journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] });
      expect(orders.some((row) => row.payload.strategyId === "profit-trailing-pair-trail")).toBe(true);
      const fills = await s.journal.queryEvents({ types: ["PAPER_FILL"] });
      expect(fills.filter((row) => row.payload.intent === "HEDGE_SECOND_LEG")).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });

  it("takes the full-executable .12 target without waiting for a retrace", async () => {
    const s = await start();
    try {
      await seedFirstLeg(s);
      await publish(s, "0.08", 1); // .80 first leg + .08 opposite = .12 gross pair edge
      await waitFor(s, "pair-complete", () => s.engine.ledger.positions.get(MARKET)?.unpairedQuantity === "0");
      const fills = await s.journal.queryEvents({ types: ["PAPER_FILL"] });
      expect(fills.filter((row) => row.payload.intent === "HEDGE_SECOND_LEG")).toHaveLength(1);
    } finally { await s.engine.stop(); }
  });
});
