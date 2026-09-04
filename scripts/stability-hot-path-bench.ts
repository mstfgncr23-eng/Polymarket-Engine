/**
 * Separate from normal CI. Measures trigger / activation / reference hot-path
 * CPU. Does not claim production CPU is fixed.
 *
 *   pnpm exec tsx scripts/stability-hot-path-bench.ts
 */
import { writeFileSync } from "node:fs";
import { cpus } from "node:os";
import { performance } from "node:perf_hooks";
import { GrokActivationCoordinator } from "../apps/engine/src/activation/coordinator.ts";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/noop.ts";
import { bookImbalance, spread, TriggerEngine, triggerConfigSchema, type MarketSnapshot, type ReferenceTick, type TriggerEvent } from "../packages/core/src/index.ts";

interface Row {
  case: string;
  iterations: number;
  wallMs: number;
  cpuMs: number;
  usPerOp: number;
  opsPerSec: number;
}

function cpuMs(): number {
  const usage = process.cpuUsage();
  return (usage.user + usage.system) / 1000;
}

function levels(count: number, start: number, step: number, qty = "1"): Array<{ price: string; quantity: string }> {
  return Array.from({ length: count }, (_, i) => ({ price: (start + i * step).toFixed(4), quantity: qty }));
}

function book(overrides: Partial<MarketSnapshot> = {}): MarketSnapshot {
  return {
    eventId: "e",
    traceId: "t",
    marketId: "m",
    tokenId: "up",
    outcome: "UP",
    asset: "BTC",
    bids: [{ price: "0.47", quantity: "100" }],
    asks: [{ price: "0.48", quantity: "5" }],
    lastTradePrice: "0.48",
    tickSize: "0.01",
    minOrderSize: "1",
    feeSchedule: null,
    sourceTimestamp: "2026-08-31T12:00:00.000Z",
    receivedAt: "2026-08-31T12:00:00.000Z",
    sequence: 1,
    hash: "h",
    ...overrides,
  };
}

function measure(name: string, iterations: number, fn: () => void): Row {
  fn();
  const wall0 = performance.now();
  const cpu0 = cpuMs();
  fn();
  const wallMs = performance.now() - wall0;
  const cpu = cpuMs() - cpu0;
  return {
    case: name,
    iterations,
    wallMs: Number(wallMs.toFixed(3)),
    cpuMs: Number(cpu.toFixed(3)),
    usPerOp: Number(((wallMs * 1000) / iterations).toFixed(3)),
    opsPerSec: Math.round(iterations / (wallMs / 1000)),
  };
}

function trigger(id: string, createdAt: string): TriggerEvent {
  return {
    id,
    traceId: "t",
    marketId: "m",
    asset: "BTC",
    source: "polymarket",
    kind: "BOOK_IMBALANCE",
    severity: 2,
    score: 1,
    dedupeKey: id,
    evidence: {},
    sourceEventIds: ["e"],
    createdAt,
    expiresAt: "2026-08-31T13:00:00.000Z",
  };
}

const rows: Row[] = [];

const deep = book({
  bids: levels(100, 0.01, 0.001, "3.25"),
  asks: levels(100, 0.6, 0.001, "1.5"),
});

rows.push(measure("bookImbalance+spread+100-level x1000", 1000, () => {
  for (let i = 0; i < 1000; i += 1) {
    bookImbalance(deep);
    spread(deep);
  }
}));

rows.push(measure("bookImbalance+spread+100-level x5000", 5000, () => {
  for (let i = 0; i < 5000; i += 1) {
    bookImbalance(deep);
    spread(deep);
  }
}));

rows.push(measure("ingestMarket 100-level x1000", 1000, () => {
  const engine = new TriggerEngine(triggerConfigSchema.parse({ cooldownMs: 0, imbalanceThreshold: 0.01, spreadChangeThreshold: 0.0001, depthChangeThreshold: 0.01 }));
  for (let i = 0; i < 1000; i += 1) {
    engine.ingestMarket({
      ...deep,
      eventId: `e${i}`,
      sequence: i + 1,
      bids: levels(100, 0.01, 0.001, String(1 + (i % 9))),
      asks: levels(100, 0.6, 0.001, String(1 + ((i + 3) % 7))),
    });
  }
}));

rows.push(measure("ingestMarket mixed mostly-suppressed x5000", 5000, () => {
  const engine = new TriggerEngine(triggerConfigSchema.parse({
    cooldownMs: 60_000,
    imbalanceThreshold: 0.65,
    spreadChangeThreshold: 0.02,
    depthChangeThreshold: 0.4,
    pairCostThreshold: 0.985,
  }));
  const up = book({ bids: [{ price: "0.47", quantity: "10" }], asks: [{ price: "0.48", quantity: "10" }] });
  const down = book({
    marketId: "m", tokenId: "down", outcome: "DOWN", eventId: "d",
    bids: [{ price: "0.51", quantity: "10" }], asks: [{ price: "0.52", quantity: "10" }],
  });
  engine.ingestMarket(up);
  engine.ingestMarket(down);
  for (let i = 0; i < 5000; i += 1) {
    engine.ingestMarket({ ...up, eventId: `u${i}`, sequence: i + 2, bids: [{ price: "0.47", quantity: String(10 + (i % 3)) }] });
  }
}));

rows.push(measure("ingestReference 60s high-freq x10000", 10_000, () => {
  const engine = new TriggerEngine(triggerConfigSchema.parse({ referenceRapidMoveBps: 15, btcRapidMoveBps: 15 }));
  const start = Date.parse("2026-08-31T12:00:00.000Z");
  for (let i = 0; i < 10_000; i += 1) {
    const at = new Date(start + i * 5).toISOString();
    const tick: ReferenceTick = {
      eventId: `r${i}`, traceId: "t", asset: "BTC", symbol: "BTCUSDT", venue: "binance",
      price: String(100 + (i % 50) * 0.01), quantity: "1", sourceTimestamp: at, receivedAt: at, tradeId: String(i),
    };
    engine.ingestReference(tick);
  }
}));

rows.push(measure("activation ensureLatency+snapshot+prune 500", 500, () => {
  const coordinator = new GrokActivationCoordinator(new NoopGrokActivationBridge(), {
    cooldownMs: 1_000, maxPerMinute: 10_000, suppressAtOrBelow: 0, memoryLimit: 500,
  }, () => new Date("2026-08-31T12:00:00.000Z"));
  for (let i = 0; i < 500; i += 1) {
    coordinator.ensureLatency(trigger(`trig_${i}`, new Date(Date.parse("2026-08-31T12:00:00.000Z") + i).toISOString()));
    if (i % 17 === 0) coordinator.snapshot();
  }
  coordinator.snapshot();
}));

const report = {
  capturedAt: new Date().toISOString(),
  cpus: cpus().length,
  node: process.version,
  rows,
};

const out = `/tmp/stability-hot-path-bench-${Date.now()}.json`;
writeFileSync(out, JSON.stringify(report, null, 2));
console.log(JSON.stringify(report, null, 2));
console.log(`wrote ${out}`);
