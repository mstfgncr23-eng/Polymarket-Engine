/**
 * Deterministic local benchmark for the Polymarket incremental price_change path.
 *
 * Workload: ~45 levels/side, multi-entry events, mixed insert/replace/delete.
 * Measures the production adapter (not the historical Zod reference).
 *
 *   pnpm exec tsx scripts/price-change-incremental-bench.ts
 */
import { writeFileSync } from "node:fs";
import { cpus } from "node:os";
import { PolymarketMarketDataAdapter } from "../packages/market-data/src/polymarket.ts";

const LEVELS_PER_SIDE = 45;
const EVENTS = 8_000;
const ENTRIES_PER_EVENT = 4;
const WARMUP_EVENTS = 200;

function createAdapter() {
  const adapter = new PolymarketMarketDataAdapter({
    subscribe: async () => ({ close: async () => undefined, [Symbol.asyncIterator]: async function* () {} }),
    closeSubscriptions: async () => undefined,
    fetchMarket: async () => ({}),
    listEvents: async function* () {},
  } as never);
  adapter.registerManualToken("tok-up", "mkt", "UP", "BTC");
  adapter.registerManualToken("tok-down", "mkt", "DOWN", "BTC");
  adapter.setTokenIds(["tok-up", "tok-down"]);
  const up = adapter.getMarket("tok-up");
  const down = adapter.getMarket("tok-down");
  const fee = {
    rate: "0.07",
    exponent: 1,
    takerOnly: true,
    rebateRate: "0.2",
    source: "configured_test_fixture" as const,
    effectiveAt: "2026-08-31T12:00:00.000Z",
  };
  if (up) up.feeSchedule = fee;
  if (down) down.feeSchedule = { ...fee };
  return adapter;
}

function levels(count: number, start: number, step: number): Array<{ price: string; size: string }> {
  return Array.from({ length: count }, (_, index) => ({
    price: (start + index * step).toFixed(4),
    size: "1",
  }));
}

function bookEvent(tokenId: string, timestamp: number) {
  const start = tokenId === "tok-up" ? 0.1 : 0.2;
  const askStart = tokenId === "tok-up" ? 0.6 : 0.7;
  return {
    type: "book",
    payload: {
      tokenId,
      market: "mkt",
      timestamp,
      bids: levels(LEVELS_PER_SIDE, start, 0.001).reverse(),
      asks: levels(LEVELS_PER_SIDE, askStart, 0.001),
      lastTradePrice: "0.50",
      hash: "h0",
      tickSize: "0.01",
      minOrderSize: "1",
    },
  };
}

function priceChangeEvent(timestamp: number, index: number) {
  const changes: Array<Record<string, unknown>> = [];
  for (let entry = 0; entry < ENTRIES_PER_EVENT; entry += 1) {
    const n = index * ENTRIES_PER_EVENT + entry;
    const tokenId = n % 5 === 0 ? "tok-down" : "tok-up";
    const buy = n % 2 === 0;
    const offset = n % LEVELS_PER_SIDE;
    const base = tokenId === "tok-up" ? (buy ? 0.1 : 0.6) : (buy ? 0.2 : 0.7);
    const op = n % 7;
    const price = (base + offset * 0.001).toFixed(4);
    const size = op === 6 ? "0" : String(1 + (n % 9));
    const insertPrice = (base + (LEVELS_PER_SIDE + (n % 5)) * 0.001).toFixed(4);
    changes.push({
      tokenId,
      side: buy ? "BUY" : "SELL",
      price: op === 3 ? insertPrice : price,
      size,
      hash: `h${n}`,
    });
  }
  return {
    type: "price_change",
    payload: {
      timestamp,
      tickSize: "0.01",
      minOrderSize: "1",
      priceChanges: changes,
    },
  };
}

function cpuMs(usage: NodeJS.CpuUsage): number {
  return (usage.user + usage.system) / 1000;
}

function runWorkload(adapter: PolymarketMarketDataAdapter, eventCount: number): { events: number; snapshots: number } {
  let snapshots = 0;
  for (let index = 0; index < eventCount; index += 1) {
    snapshots += adapter.normalizeSourceEvent(priceChangeEvent(2_000 + index, index)).length;
  }
  return { events: eventCount, snapshots };
}

function measure() {
  const adapter = createAdapter();
  adapter.normalizeSourceEvent(bookEvent("tok-up", 1_000));
  adapter.normalizeSourceEvent(bookEvent("tok-down", 1_000));
  runWorkload(adapter, WARMUP_EVENTS);
  if (typeof globalThis.gc === "function") globalThis.gc();

  const heapBefore = process.memoryUsage().heapUsed;
  const cpuBefore = process.cpuUsage();
  const started = process.hrtime.bigint();
  const result = runWorkload(adapter, EVENTS);
  const elapsedNs = process.hrtime.bigint() - started;
  const cpu = process.cpuUsage(cpuBefore);
  const heapAfter = process.memoryUsage().heapUsed;
  const wallMs = Number(elapsedNs) / 1_000_000;
  const cpuUsedMs = cpuMs(cpu);

  return {
    events: result.events,
    snapshots: result.snapshots,
    entries: result.events * ENTRIES_PER_EVENT,
    levelsPerSide: LEVELS_PER_SIDE,
    entriesPerEvent: ENTRIES_PER_EVENT,
    wallMs,
    cpuMs: cpuUsedMs,
    heapDeltaMb: (heapAfter - heapBefore) / (1024 * 1024),
    eventsPerSec: result.events / (wallMs / 1000),
    snapshotsPerSec: result.snapshots / (wallMs / 1000),
    usPerEvent: (wallMs * 1000) / result.events,
    usPerSnapshot: (wallMs * 1000) / result.snapshots,
  };
}

const first = measure();
const second = measure();
const report = {
  measuredAt: new Date().toISOString(),
  node: process.version,
  cpuCount: cpus().length,
  cpuModel: cpus()[0]?.model ?? "unknown",
  gcAvailable: typeof globalThis.gc === "function",
  label: process.env.PR50_BENCH_LABEL ?? "current",
  warmup: first,
  measured: second,
};

writeFileSync("reports/pr50-price-change-bench.json", `${JSON.stringify(report, null, 2)}\n`);
const row = report.measured;
console.log(JSON.stringify({
  label: report.label,
  events: row.events,
  snapshots: row.snapshots,
  wallMs: Number(row.wallMs.toFixed(3)),
  cpuMs: Number(row.cpuMs.toFixed(3)),
  eventsPerSec: Math.round(row.eventsPerSec),
  snapshotsPerSec: Math.round(row.snapshotsPerSec),
  usPerEvent: Number(row.usPerEvent.toFixed(3)),
  usPerSnapshot: Number(row.usPerSnapshot.toFixed(3)),
  heapDeltaMb: Number(row.heapDeltaMb.toFixed(3)),
}, null, 2));
