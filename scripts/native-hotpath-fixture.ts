import assert from "node:assert/strict";
import { performance } from "node:perf_hooks";
import { type ActivationLatency, type MarketSnapshot, type TriggerEvent } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv } from "../tests/paper-engine-testkit.js";
import { snapshot } from "../tests/fixtures.js";

export const FIXTURE_START = Date.parse("2026-10-05T00:00:00.000Z");

export async function fixture(markets = 1, journal = new MemoryEventJournal(), begin = true, clock = { ms: FIXTURE_START }) {
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport(Array.from({ length: markets }, (_, i) =>
    qualified(`native-${i}`, "BTC", `native-${i}-up`, `native-${i}-down`, new Date(FIXTURE_START + 3_600_000).toISOString()))));
  const errors: unknown[] = [];
  const engine = new EngineService(qualifiedPaperEnv({
    GROK_ACTIVATION_PROVIDER: "noop", PAPER_EXECUTION_STYLE: "TAKER", PAPER_EXECUTION_LATENCY_MS: "0",
    EXPERIMENT_DURATION_HOURS: "1", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    NAKED_DIRECTIONAL_SOAK_ENABLED: "false", NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE: "0.12",
    PROFIT_TRAILING_PAIR_SOAK_ENABLED: "true", PROFIT_TRAILING_PAIR_ARM_EDGE: "0.05",
    PROFIT_TRAILING_PAIR_RETRACE_EDGE: "0.02", RETENTION_ENABLED: "true",
    RETENTION_COMPACTION_ENABLED: "false", RETENTION_ANALYTICS_ROLLUP_ENABLED: "false",
  }), journal, { error: (error: unknown) => { errors.push(error); } }, {
    market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
    activation: new NoopGrokActivationBridge(), now: () => new Date(clock.ms), resolutionFetcher: async () => null,
  });
  await engine.start();
  await market.waitForHandlers();
  if (begin) assert.equal((await engine.beginExperiment()).ok, true);
  async function reference(price: string, sequence: number) {
    const at = new Date(clock.ms).toISOString();
    await engine.ingestReferenceTickForTest({ eventId: `ref-${sequence}`, traceId: `ref-trace-${sequence}`,
      asset: "BTC", symbol: "BTCUSDT", venue: "deterministic-test", price, quantity: "1",
      sourceTimestamp: at, receivedAt: at, tradeId: `trade-${sequence}` });
  }
  await reference("100000", 1);
  clock.ms += 1000;
  await reference("100000", 2);
  await drain(engine);
  return { engine, journal, market, clock, errors, reference };
}

export type Fixture = Awaited<ReturnType<typeof fixture>>;

export function book(s: Fixture, marketId: string, outcome: "UP" | "DOWN", sequence: number,
  ask = "0.51", directional = false): MarketSnapshot {
  return snapshot({ eventId: `book-${marketId}-${outcome}-${sequence}`, traceId: `book-trace-${marketId}-${outcome}-${sequence}`,
    marketId, tokenId: `${marketId}-${outcome.toLowerCase()}`, outcome, asset: "BTC",
    bids: [{ price: (Number(ask) - 0.01).toFixed(2), quantity: directional && outcome === "DOWN" ? "1000" : "10000" }],
    asks: [{ price: ask, quantity: directional && outcome === "UP" ? "1000" : "10000" }],
    lastTradePrice: ask, sourceTimestamp: new Date(s.clock.ms).toISOString(),
    receivedAt: new Date(s.clock.ms).toISOString(), sequence, hash: `hash-${marketId}-${outcome}-${sequence}` });
}

export function trigger(s: Fixture, i: number, markets = 1): TriggerEvent {
  const marketId = `native-${i % markets}`;
  return { id: `fixture-trigger-${i}`, traceId: `fixture-trace-${i}`, marketId, asset: "BTC",
    source: "polymarket", kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: `fixture-${i}`,
    evidence: {}, sourceEventIds: [`source-${i}`], evidenceIds: [`source-${i}`],
    createdAt: new Date(s.clock.ms).toISOString(), expiresAt: new Date(s.clock.ms + 60_000).toISOString() };
}

export async function drain(engine: EngineService): Promise<void> {
  const deadline = performance.now() + 10_000;
  while (engine.runtimeSupervisorSnapshot().unresolved !== 0) {
    if (performance.now() > deadline) throw new Error("fixture runtime did not drain");
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

// Strip only generated identities/wall-clock audit metadata; retain all business values.
export function semantics(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(semantics);
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value)
    .filter(([key]) => !["id", "orderId", "positionId", "experimentId", "traceId", "triggerId", "calculatedAt", "recordedAt", "at", "auditKey", "operationKey"].includes(key))
    .map(([key, item]) => [key, semantics(item)]));
  return value;
}

export async function economicFixture() {
  const s = await fixture();
  try {
    await s.reference("102000", 3);
    await s.market.emitSnapshot(book(s, "native-0", "UP", 1, "0.60", true));
    await s.market.emitSnapshot(book(s, "native-0", "DOWN", 1, "0.41", true));
    await drain(s.engine);
    // Exercise actual Native first-leg admission and the durable trailing controller.
    assert.ok((await s.journal.queryEvents({ types: ["PAPER_FILL"] })).some((e) => e.payload.intent === "OPEN_FIRST_LEG"), "fixture must create a Native first leg");
    for (const [sequence, ask] of [[2, "0.35"], [3, "0.30"], [4, "0.32"]] as const) {
      s.clock.ms += 1000;
      await s.market.emitSnapshot(book(s, "native-0", "DOWN", sequence, ask, true));
      await drain(s.engine);
      // The pair controller is detached from trigger work; wait for its durable gate.
      const deadline = performance.now() + 2000;
      const expected = sequence === 2 ? "0.05" : "0.1";
      while (sequence < 4
        ? (s.engine as unknown as { profitTrailingPairStates: Map<string, { highWaterEdge: string }> }).profitTrailingPairStates.get("native-0")?.highWaterEdge !== expected
        : s.engine.ledger.positions.get("native-0")?.unpairedQuantity !== "0") {
        if (performance.now() > deadline) throw new Error(`trailing fixture did not reach stage ${sequence}`);
        await new Promise((resolve) => setTimeout(resolve, 1));
      }
    }
    const types = ["CONSENSUS_DECISION", "RISK_EVALUATION", "PAPER_ORDER_CREATED", "PAPER_FILL"];
    const rows = await s.journal.queryEvents({ types });
    assert.ok(rows.some((e) => e.type === "PAPER_FILL" && e.payload.intent === "HEDGE_SECOND_LEG"));
    const trailing = await s.journal.queryDurableTransitions({ types: ["PROFIT_TRAILING_PAIR_STATE_UPDATED"], limit: 100 });
    return semantics({ events: rows.map((row) => ({ type: row.type, payload: row.payload })),
      trailing: trailing.map((row) => row.payload), ledger: s.engine.ledger.snapshot(),
      positions: [...s.engine.ledger.positions.values()] });
  } finally { await s.engine.stop(); }
}

export async function benchmark(eventCount = 2048, markets = 8, journalDelayMs = 0) {
  const s = await fixture(markets);
  try {
    for (let i = 0; i < markets; i++) {
      await s.market.emitSnapshot(book(s, `native-${i}`, "UP", 1));
      await s.market.emitSnapshot(book(s, `native-${i}`, "DOWN", 1));
    }
    await drain(s.engine);
    // Warm both the model and a full bounded 500-entry legacy attribution history.
    for (let i = 0; i < 512; i++) await s.engine.registerTriggerForResearch(trigger(s, i, markets));
    let activationMs = 0, activationLatencyUpdates = 0, activationRecords = 0;
    const ensure = s.engine.activation.ensureLatency.bind(s.engine.activation);
    s.engine.activation.ensureLatency = (...args) => { const start = performance.now(); ensure(...args); activationMs += performance.now() - start; };
    const snap = s.engine.activation.snapshot.bind(s.engine.activation);
    s.engine.activation.snapshot = () => { const start = performance.now(); const result = snap(); activationMs += performance.now() - start; return result; };
    const latency = s.engine.state.setActivationLatency.bind(s.engine.state);
    s.engine.state.setActivationLatency = (items: ActivationLatency[]) => { const start = performance.now(); latency(items); activationLatencyUpdates++; activationMs += performance.now() - start; };
    const legacyState = s.engine.state as unknown as { addActivation?: (record: unknown) => void };
    if (legacyState.addActivation) {
      const add = legacyState.addActivation.bind(s.engine.state);
      legacyState.addActivation = (record) => { const start = performance.now(); add(record); activationRecords++; activationMs += performance.now() - start; };
    }
    const journalWrites: Record<string, number> = {}, journalWallMs: Record<string, number> = {};
    const append = s.journal.append.bind(s.journal);
    s.journal.append = async (event) => {
      const start = performance.now();
      if (journalDelayMs) await new Promise((resolve) => setTimeout(resolve, journalDelayMs));
      const result = await append(event);
      journalWrites[event.type] = (journalWrites[event.type] ?? 0) + 1;
      journalWallMs[event.type] = (journalWallMs[event.type] ?? 0) + performance.now() - start;
      return result;
    };
    let runtimeQueuePeak = 0, rssPeak = process.memoryUsage().rss;
    const cpu = process.cpuUsage(), rssBefore = process.memoryUsage().rss, start = performance.now();
    for (let i = 0; i < eventCount; i += 64) {
      const work = Array.from({ length: Math.min(64, eventCount - i) }, (_, j) =>
        s.engine.registerTriggerForResearch(trigger(s, 512 + i + j, markets)));
      runtimeQueuePeak = Math.max(runtimeQueuePeak, s.engine.runtimeSupervisorSnapshot().totalQueued);
      await Promise.all(work);
      rssPeak = Math.max(rssPeak, process.memoryUsage().rss);
    }
    const wallMs = performance.now() - start, usage = process.cpuUsage(cpu);
    assert.equal(journalWrites.TRIGGER_EVENT, eventCount);
    assert.equal(journalWrites.CONSENSUS_DECISION, eventCount);
    assert.equal(s.errors.length, 0);
    return { node: process.version, fixture: "native-flat-8-market-64-trigger-bursts-v1", markets, eventCount, journalDelayMs,
      journalBackend: "MemoryEventJournal", wallMs, throughputPerSecond: eventCount * 1000 / wallMs,
      cpuMs: (usage.user + usage.system) / 1000, runtimeQueuePeak, rssBefore, rssAfter: process.memoryUsage().rss, rssPeak,
      activationBookkeepingMs: activationMs, activationLatencyUpdates, activationRecords, journalWrites, journalWallMs };
  } finally { await s.engine.stop(); }
}
