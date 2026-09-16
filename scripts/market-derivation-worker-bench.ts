/**
 * Deterministic benchmark for the PR52 BookDerived worker boundary.
 *
 * Compares the synchronous derive path against one worker thread using the
 * SAME generated snapshots. The goal is to move derivation CPU off the main
 * event loop, not necessarily to reduce total process CPU.
 *
 *   pnpm bench:market-derivation-worker
 */
import { performance, monitorEventLoopDelay } from "node:perf_hooks";
import { cpus } from "node:os";
import { writeFileSync } from "node:fs";
import {
  compactBookSidesForDerivation,
  deriveBook,
  deriveBookFromSides,
  marketSnapshotSchema,
  TriggerEngine,
  triggerConfigSchema,
  type BookDerived,
  type MarketSnapshot,
} from "../packages/core/src/index.ts";
import { MarketDerivationWorkerService } from "../apps/engine/src/market-derivation-service.ts";

const ITERATIONS = 8_000;
const WARMUP = 100;
const LEVELS = 45;

function levels(count: number, start: number, seed: number): Array<{ price: string; quantity: string }> {
  return Array.from({ length: count }, (_, index) => ({
    price: (start + index * 0.001).toFixed(4),
    quantity: String(1 + ((seed + index) % 9) + index / 100),
  }));
}

const NOW = "2026-08-31T12:00:00.000Z";
const FEE = {
  rate: "0.07",
  exponent: 1,
  takerOnly: true,
  rebateRate: "0.2",
  source: "configured_test_fixture" as const,
  effectiveAt: NOW,
};

function makeSnapshot(index: number): MarketSnapshot {
  return marketSnapshotSchema.parse({
    eventId: `m-${index}`,
    traceId: `t-${index}`,
    marketId: "btc-15m",
    tokenId: index % 2 === 0 ? "up" : "down",
    outcome: index % 2 === 0 ? "UP" : "DOWN",
    bids: levels(LEVELS, 0.10, index).map((level, levelIndex) => (
      levelIndex < 5 ? { ...level, quantity: String(200 + (index % 17)) } : level
    )),
    asks: levels(LEVELS, 0.55, index * 3).map((level, levelIndex) => (
      levelIndex < 5 ? { ...level, quantity: String(1 + (index % 3)) } : level
    )),
    lastTradePrice: "0.48",
    tickSize: "0.01",
    minOrderSize: "1",
    feeSchedule: FEE,
    sourceTimestamp: NOW,
    receivedAt: NOW,
    sequence: index + 1,
    hash: `h-${index}`,
  });
}

function checksum(items: BookDerived[]): string {
  let hash = 0;
  for (const item of items) {
    const text = `${item.spread}|${item.spreadNumber}|${item.totalDepth}|${item.imbalance}|${item.imbalanceAbs}`;
    for (let i = 0; i < text.length; i += 1) hash = Math.imul(hash, 31) + text.charCodeAt(i) | 0;
  }
  return String(hash);
}

function cpuMs(start: NodeJS.CpuUsage): number {
  const used = process.cpuUsage(start);
  return (used.user + used.system) / 1000;
}

/**
 * Valid main-thread stall measurement.
 *
 * Arm monitorEventLoopDelay until it has sampled, then schedule a 5ms timer
 * immediately before work. After work, yield once so both the timer and the
 * delay histogram can observe a synchronous stall. Do not put this setup on
 * the wall/cpu clock — that would pollute ops/s.
 *
 * A setInterval wrapped around a blocking loop and cleared in the same turn
 * never fires and must not be reported as 0ms responsiveness.
 */
async function armResponsivenessProbe(): Promise<{
  histogram: ReturnType<typeof monitorEventLoopDelay>;
  timer: NodeJS.Timeout;
  scheduledAt: number;
  expectedDelayMs: number;
  firedAt: { current: number | null };
}> {
  const histogram = monitorEventLoopDelay({ resolution: 1 });
  histogram.enable();
  const armedAt = Date.now();
  while (histogram.count === 0 && Date.now() - armedAt < 50) {
    await new Promise<void>((resolve) => setTimeout(resolve, 1));
  }
  const expectedDelayMs = 5;
  const firedAt = { current: null as number | null };
  const scheduledAt = performance.now();
  const timer = setTimeout(() => {
    firedAt.current = performance.now();
  }, expectedDelayMs);
  timer.unref();
  return { histogram, timer, scheduledAt, expectedDelayMs, firedAt };
}

async function finishResponsivenessProbe(probe: Awaited<ReturnType<typeof armResponsivenessProbe>>): Promise<{
  mainThreadTimerDelayMs: number;
  eventLoopDelayMaxMs: number;
}> {
  await new Promise<void>((resolve) => setImmediate(resolve));
  if (probe.firedAt.current == null) {
    await new Promise<void>((resolve) => setTimeout(resolve, probe.expectedDelayMs + 1));
  }
  probe.histogram.disable();
  clearTimeout(probe.timer);
  const fired = probe.firedAt.current;
  if (fired == null) throw new Error("MAIN_THREAD_TIMER_PROBE_DID_NOT_FIRE");
  return {
    mainThreadTimerDelayMs: Number(Math.max(0, fired - probe.scheduledAt - probe.expectedDelayMs).toFixed(3)),
    eventLoopDelayMaxMs: Number((probe.histogram.max / 1e6).toFixed(3)),
  };
}

function triggerConfig() {
  return triggerConfigSchema.parse({
    cooldownMs: 0, imbalanceThreshold: 0.65, spreadChangeThreshold: 0.02, depthChangeThreshold: 0.4,
  });
}

const snapshots = Array.from({ length: ITERATIONS }, (_, index) => makeSnapshot(index));
const warmup = snapshots.slice(0, WARMUP);
const measured = snapshots.slice(WARMUP);

for (const item of warmup) deriveBook(item);

const syncEngine = new TriggerEngine(triggerConfig(), () => new Date("2026-08-31T12:00:00.000Z"));
for (const item of warmup) syncEngine.ingestMarket(item);

const service = new MarketDerivationWorkerService({ timeoutMs: 2_000 });
if (!await service.start()) throw new Error("WORKER_START_FAILED");
const workerEngine = new TriggerEngine(triggerConfig(), () => new Date("2026-08-31T12:00:00.000Z"));
for (const item of warmup) {
  const { derived } = await service.deriveSnapshot(item);
  workerEngine.ingestMarketWithDerived(item, derived);
}

const syncProbeArm = await armResponsivenessProbe();
const syncEluStart = performance.eventLoopUtilization();
const syncCpu = process.cpuUsage();
const syncWallStart = performance.now();
const syncDerived: BookDerived[] = [];
let syncTriggers = 0;
for (const item of measured) {
  const next = deriveBook(item);
  syncDerived.push(next);
  syncTriggers += syncEngine.ingestMarketWithDerived(item, next).length;
}
const syncWallMs = performance.now() - syncWallStart;
const syncCpuMs = cpuMs(syncCpu);
const syncElu = performance.eventLoopUtilization(syncEluStart).utilization;
const syncProbe = await finishResponsivenessProbe(syncProbeArm);

const workerProbeArm = await armResponsivenessProbe();
const workerEluStart = performance.eventLoopUtilization();
const workerCpu = process.cpuUsage();
const workerWallStart = performance.now();
const workerDerived: BookDerived[] = [];
let workerTriggers = 0;
let workerSuccess = 0;
let roundTripSum = 0;
let roundTripMax = 0;
for (const item of measured) {
  const started = performance.now();
  const result = await service.deriveSnapshot(item);
  const rtt = performance.now() - started;
  roundTripSum += rtt;
  roundTripMax = Math.max(roundTripMax, rtt);
  if (result.status === "success") workerSuccess += 1;
  workerDerived.push(result.derived);
  workerTriggers += workerEngine.ingestMarketWithDerived(item, result.derived).length;
}
const workerWallMs = performance.now() - workerWallStart;
const workerCpuMs = cpuMs(workerCpu);
const workerElu = performance.eventLoopUtilization(workerEluStart).utilization;
const workerProbe = await finishResponsivenessProbe(workerProbeArm);

await service.stop();

const syncChecksum = checksum(syncDerived);
const workerChecksum = checksum(workerDerived);
const compactChecksum = checksum(measured.map((item) => deriveBookFromSides(compactBookSidesForDerivation(item))));
if (syncChecksum !== workerChecksum || syncChecksum !== compactChecksum) {
  throw new Error(`CHECKSUM_MISMATCH sync=${syncChecksum} worker=${workerChecksum} compact=${compactChecksum}`);
}
if (syncTriggers !== workerTriggers) {
  throw new Error(`TRIGGER_COUNT_MISMATCH sync=${syncTriggers} worker=${workerTriggers}`);
}

const iterations = measured.length;
const report = {
  iterations,
  warmup: WARMUP,
  levelsPerSide: LEVELS,
  cpus: cpus().length,
  checksum: syncChecksum,
  triggerCount: syncTriggers,
  equivalent: true,
  sync: {
    wallMs: Number(syncWallMs.toFixed(3)),
    cpuMs: Number(syncCpuMs.toFixed(3)),
    opsPerSec: Number((iterations / (syncWallMs / 1000)).toFixed(1)),
    eventLoopUtilization: Number(syncElu.toFixed(4)),
    mainThreadTimerDelayMs: syncProbe.mainThreadTimerDelayMs,
    eventLoopDelayMaxMs: syncProbe.eventLoopDelayMaxMs,
  },
  worker: {
    wallMs: Number(workerWallMs.toFixed(3)),
    cpuMs: Number(workerCpuMs.toFixed(3)),
    opsPerSec: Number((iterations / (workerWallMs / 1000)).toFixed(1)),
    eventLoopUtilization: Number(workerElu.toFixed(4)),
    mainThreadTimerDelayMs: workerProbe.mainThreadTimerDelayMs,
    eventLoopDelayMaxMs: workerProbe.eventLoopDelayMaxMs,
    success: workerSuccess,
    roundTripAvgMs: Number((roundTripSum / iterations).toFixed(3)),
    roundTripMaxMs: Number(roundTripMax.toFixed(3)),
  },
  ipcCostMsAvg: Number(((workerWallMs - syncWallMs) / iterations).toFixed(4)),
};

console.log(JSON.stringify(report, null, 2));
writeFileSync("/tmp/pr52-market-derivation-worker-bench.json", `${JSON.stringify(report, null, 2)}\n`);
