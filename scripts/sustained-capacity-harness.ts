/**
 * Production-shaped, local/synthetic sustained-load harness.
 *
 * This deliberately drives EngineService through its market adapter, bounded
 * transport, reference ingress, supervisor, journal admission, and SSE hub.
 * It is a convergence probe, not a Fly capacity claim.
 *
 *   pnpm exec tsx scripts/sustained-capacity-harness.ts --duration-ms 10000
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { monitorEventLoopDelay, performance } from "node:perf_hooks";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createId, engineEnvSchema, feeScheduleSchema, type EngineEnv, type ReferenceTick } from "../packages/core/src/index.ts";
import { MemoryEventJournal } from "../packages/db/src/index.ts";
import {
  PolymarketMarketDataAdapter,
  ScriptedBoundedMarketTransport,
  type DiscoveryReport,
  type DiscoveredMarket,
  type ReferenceFeedHandlers,
  type ReferenceResolution,
} from "../packages/market-data/src/index.ts";
import { EngineService } from "../apps/engine/src/engine.ts";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/noop.ts";

export type DashboardMode = "disconnected" | "connected" | "slow";
export type DatabaseMode = "healthy" | "delayed" | "unavailable";

export interface CapacityHarnessOptions {
  durationMs?: number;
  warmupMs?: number;
  sampleIntervalMs?: number;
  markets?: number;
  marketRatePerSec?: number;
  referenceRatePerSec?: number;
  reconnectStorm?: boolean;
  discoveryRefresh?: boolean;
  dashboard?: DashboardMode;
  database?: DatabaseMode;
  databaseDelayMs?: number;
}

export interface CapacitySample {
  elapsedMs: number;
  phase: "warmup" | "steady" | "burst" | "recovery";
  memory: { rss: number; heapUsed: number; heapTotal: number; external: number; arrayBuffers: number };
  eventLoop: { delayP99Ms: number; delayMaxMs: number; utilization: number };
  cpu: { processMs: number; userMs: number; systemMs: number };
  trigger: { queued: number; inFlight: number; active: number; provenance: number; dedupe: number };
  journal: { active: number; waiting: number; criticalWaiting: number; noncriticalWaiting: number };
  ingress: { depth: number; bytes: number; highWaterEvents: number; highWaterBytes: number; accepted: number; processed: number; gaps: number };
  reference: { accepted: number; windowEntries: number; retryTimers: number };
  supervisor: { queued: number; inFlight: number; busyKeys: number; failures: number };
  sse: { subscribers: number; tracked: number; bufferedBytes: number };
  state: { marketSnapshots: number; marketCache: number; settledMarkets: number; activations: number; grokSignals: number; idleTombstones: number };
}

export interface CapacityReport {
  schemaVersion: 1;
  capturedAt: string;
  locality: "local-synthetic";
  target: "Fly shared-cpu-2x 512MB FRA singleton";
  options: Required<CapacityHarnessOptions>;
  workload: { sourceEventsSent: number; sourceEventsRejected: number; reconnectsRequested: number; referenceTicksSent: number; burstEvents: number; discoveryRefreshes: number };
  samples: CapacitySample[];
  plateau: {
    warmupSamples: number;
    steadySamples: number;
    finalWindowSamples: number;
    metrics: Record<string, { warmupHighWater: number; plateauHighWater: number; finalSlopePerSecond: number; classification: "plateau" | "monotonic_growth" | "insufficient_samples" }>;
  };
  invariants: {
    boundedIngress: boolean;
    boundedSupervisor: boolean;
    boundedJournalAdmission: boolean;
    boundedReferenceWindow: boolean;
    boundedSse: boolean;
    boundedRuntimeBookkeeping: boolean;
    rawMarketAndReferenceEventsPersisted: number;
    durableTransitions: number;
    dashboardSlowClientDisconnected: boolean;
  };
  cpuModel: {
    measuredLocal: { processCpuMs: number; elapsedMs: number };
    syntheticEstimate: string;
    flySoakRequired: string[];
  };
}

const DEFAULTS: Required<CapacityHarnessOptions> = {
  durationMs: 5_000,
  warmupMs: 750,
  sampleIntervalMs: 100,
  markets: 4,
  marketRatePerSec: 30,
  referenceRatePerSec: 12,
  reconnectStorm: true,
  discoveryRefresh: true,
  dashboard: "slow",
  database: "healthy",
  databaseDelayMs: 8,
};

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function levelRows(count: number, base: number, direction: 1 | -1): Array<{ price: string; quantity: string }> {
  return Array.from({ length: count }, (_, index) => ({ price: (base + direction * index * 0.001).toFixed(4), quantity: String(4 + (index % 3)) }));
}

function rawBook(tokenId: string, marketId: string, timestamp: number, levels = 5): Record<string, unknown> {
  return {
    type: "book",
    payload: {
      tokenId, market: marketId, timestamp,
      bids: levelRows(levels, 0.47, -1).map((item) => ({ price: item.price, size: item.quantity })),
      asks: levelRows(levels, 0.48, 1).map((item) => ({ price: item.price, size: item.quantity })),
      lastTradePrice: "0.48",
    },
  };
}

function syntheticQualified(id: string, asset: string, up: string, down: string): DiscoveredMarket {
  const endDate = new Date(Date.now() + 15 * 60_000).toISOString();
  const feeSchedule = feeScheduleSchema.parse({ rate: "0.07", exponent: 1, takerOnly: true, rebateRate: "0.2", source: "configured_test_fixture", effectiveAt: new Date().toISOString() });
  return {
    id, slug: `${asset.toLowerCase()}-updown-15m`, question: `${asset} Up or Down`, title: `${asset} Up or Down`, asset, endDate, active: true,
    tokens: [{ tokenId: up, outcome: "UP", price: "0.48" }, { tokenId: down, outcome: "DOWN", price: "0.51" }],
    feeSchedule, liquidityUsd: "200", spread: 0.02,
    qualification: { qualified: true, reasonCodes: ["QUALIFIED"], asset, crypto: true, binary: { safe: true, yesOutcome: "UP", noOutcome: "DOWN", reason: "QUALIFIED" }, duplicateOf: null }, raw: {},
  };
}

function syntheticDiscovery(markets: DiscoveredMarket[]): DiscoveryReport {
  return { discovered: markets, qualified: markets, rejected: [] };
}

function syntheticEnv(overrides: Record<string, string | undefined> = {}): EngineEnv {
  return engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", GROK_ACTIVATION_PROVIDER: "noop", DISCOVERY_INTERVAL_MS: "3600000", ...overrides });
}

class SyntheticReferenceRegistry {
  private readonly running = new Map<string, ReferenceFeedHandlers>();
  async resolve(asset: string): Promise<ReferenceResolution> {
    return { asset, supported: true, symbol: `${asset}USDT`, url: `wss://synthetic/${asset.toLowerCase()}`, reason: "RESOLVED", status: "SUPPORTED" };
  }
  adapterFor(): null { return null; }
  async ensureRunning(asset: string, handlers: ReferenceFeedHandlers): Promise<"started" | "already"> {
    if (this.running.has(asset)) return "already";
    this.running.set(asset, handlers);
    return "started";
  }
  stop(asset: string): boolean { return this.running.delete(asset); }
  runningAssets(): string[] { return [...this.running.keys()]; }
}

function rawPriceChange(tokens: string[], timestamp: number, index: number): Record<string, unknown> {
  return {
    type: "price_change",
    payload: {
      timestamp,
      priceChanges: tokens.flatMap((tokenId, tokenIndex) => [
        { tokenId, side: "BUY", price: (0.46 + ((index + tokenIndex) % 4) * 0.002).toFixed(4), size: String(3 + (index % 4)) },
        { tokenId, side: "SELL", price: (0.50 + ((index + tokenIndex) % 4) * 0.002).toFixed(4), size: String(2 + (index % 3)) },
      ]),
    },
  };
}

function referenceTick(asset: string, sequence: number): ReferenceTick {
  const at = new Date().toISOString();
  return {
    eventId: createId("reference"), traceId: createId("trace"), asset, symbol: `${asset}USDT`, venue: "synthetic",
    price: (100 + (sequence % 17) * 0.01).toFixed(4), quantity: "1", sourceTimestamp: at, receivedAt: at, tradeId: String(sequence),
  };
}

function numericSlope(values: number[], elapsedMs: number[]): number {
  if (values.length < 3) return 0;
  const x0 = elapsedMs[0] ?? 0;
  const x = elapsedMs.map((item) => (item - x0) / 1000);
  const meanX = x.reduce((sum, item) => sum + item, 0) / x.length;
  const meanY = values.reduce((sum, item) => sum + item, 0) / values.length;
  const denominator = x.reduce((sum, item) => sum + (item - meanX) ** 2, 0);
  if (denominator === 0) return 0;
  return x.reduce((sum, item, index) => sum + (item - meanX) * ((values[index] ?? meanY) - meanY), 0) / denominator;
}

function classifyMetric(samples: CapacitySample[], key: string, value: (sample: CapacitySample) => number, warmupSamples: number): CapacityReport["plateau"]["metrics"][string] {
  const steady = samples.slice(warmupSamples);
  const finalWindow = steady.slice(-Math.max(3, Math.floor(steady.length / 2)));
  if (steady.length < 3 || finalWindow.length < 3) return { warmupHighWater: Math.max(0, ...samples.slice(0, warmupSamples).map(value)), plateauHighWater: Math.max(0, ...steady.map(value)), finalSlopePerSecond: 0, classification: "insufficient_samples" };
  const slope = numericSlope(finalWindow.map(value), finalWindow.map((sample) => sample.elapsedMs));
  const highWater = Math.max(0, ...steady.map(value));
  const baseline = Math.max(1, Math.max(...finalWindow.map(value)));
  const classification = slope > Math.max(0.5, baseline * 0.05) ? "monotonic_growth" : "plateau";
  return { warmupHighWater: Math.max(0, ...samples.slice(0, warmupSamples).map(value)), plateauHighWater: highWater, finalSlopePerSecond: Number(slope.toFixed(4)), classification };
}

export async function runSustainedCapacityHarness(input: CapacityHarnessOptions = {}): Promise<CapacityReport> {
  const options: Required<CapacityHarnessOptions> = { ...DEFAULTS, ...input };
  const transport = new ScriptedBoundedMarketTransport();
  const adapter = new PolymarketMarketDataAdapter(undefined, { transport });
  const markets = Array.from({ length: Math.max(1, Math.min(20, Math.floor(options.markets))) }, (_, index) => {
    const asset = ["BTC", "ETH", "SOL", "XRP", "DOGE", "ADA"][index % 6]!;
    return syntheticQualified(`synthetic-${asset.toLowerCase()}-${index}`, asset, `synthetic-${index}-up`, `synthetic-${index}-down`);
  });
  const report: DiscoveryReport = syntheticDiscovery(markets);
  const tokenIds = markets.flatMap((market) => market.tokens.map((token) => token.tokenId));
  for (const market of markets) for (const token of market.tokens) adapter.registerManualToken(token.tokenId, market.id, token.outcome, market.asset);
  (adapter as PolymarketMarketDataAdapter & { discoverCryptoMarkets: () => Promise<DiscoveryReport> }).discoverCryptoMarkets = async () => report;
  const journal = new MemoryEventJournal();
  const referenceRegistry = new SyntheticReferenceRegistry();
  const env = syntheticEnv({
    GROK_ACTIVATION_PROVIDER: "noop",
    MAX_QUALIFIED_MARKETS: String(markets.length),
    TELEMETRY_QUEUE_LIMIT: "256",
    TELEMETRY_BATCH_SIZE: "25",
    DASHBOARD_SNAPSHOT_HZ: "20",
  });
  if (options.database === "delayed") {
    const original = journal.append.bind(journal);
    journal.append = async (...args: Parameters<MemoryEventJournal["append"]>) => { await sleep(Math.max(0, options.databaseDelayMs)); return original(...args); };
  }
  const engine = new EngineService(env, journal, { error: () => undefined }, {
    market: adapter,
    referenceRegistry: referenceRegistry as never,
    activation: new NoopGrokActivationBridge(),
  });
  const workload = { sourceEventsSent: 0, sourceEventsRejected: 0, reconnectsRequested: 0, referenceTicksSent: 0, burstEvents: 0, discoveryRefreshes: 0 };
  const samples: CapacitySample[] = [];
  const eventLoop = monitorEventLoopDelay({ resolution: 10 });
  eventLoop.enable();
  let previousUtilization = performance.eventLoopUtilization();
  let previousCpu = process.cpuUsage();
  let fastBytes = 0;
  let slowEnded = false;
  if (options.dashboard !== "disconnected") {
    engine.sse.add({ write: (chunk) => { fastBytes += chunk.length; return true; }, end: () => undefined });
    if (options.dashboard === "slow") engine.sse.add({ write: () => false, end: () => { slowEnded = true; } });
  }
  await engine.start();
  const startedAt = Date.now();
  const endAt = startedAt + Math.max(200, options.durationMs);
  let phase: CapacitySample["phase"] = "warmup";
  let sequence = 1;
  const firstBooks = new Set<string>();
  const sample = () => {
    const ingress = adapter.ingressSnapshot();
    const supervisor = engine.runtimeSupervisorSnapshot();
    const admission = engine.journalAdmissionSnapshot();
    const utilization = performance.eventLoopUtilization(previousUtilization);
    previousUtilization = utilization;
    const cpu = process.cpuUsage(previousCpu);
    previousCpu = process.cpuUsage();
    const memory = process.memoryUsage();
    const sse = engine.sse.statsForTest();
    const bookkeeping = engine.runtimeBookkeepingForTest();
    samples.push({
      elapsedMs: Date.now() - startedAt, phase,
      memory: { rss: memory.rss, heapUsed: memory.heapUsed, heapTotal: memory.heapTotal, external: memory.external, arrayBuffers: memory.arrayBuffers ?? 0 },
      eventLoop: { delayP99Ms: Number((eventLoop.percentile(99) / 1e6).toFixed(3)), delayMaxMs: Number((eventLoop.max / 1e6).toFixed(3)), utilization: Number(utilization.utilization.toFixed(5)) },
      cpu: { processMs: (cpu.user + cpu.system) / 1000, userMs: cpu.user / 1000, systemMs: cpu.system / 1000 },
      trigger: { queued: supervisor.totalQueued, inFlight: supervisor.totalInFlight, active: bookkeeping.activeTriggers, provenance: bookkeeping.triggerProvenance, dedupe: engine.triggerDedupeEntryCount() },
      journal: { active: admission.active, waiting: admission.waiting, criticalWaiting: admission.criticalWaiting, noncriticalWaiting: admission.noncriticalWaiting },
      ingress: { depth: ingress.depth, bytes: ingress.bytes, highWaterEvents: ingress.highWaterEvents, highWaterBytes: ingress.highWaterBytes, accepted: ingress.accepted, processed: ingress.processed, gaps: ingress.gapTotal },
      reference: { accepted: workload.referenceTicksSent, windowEntries: engine.referenceWindowTotalEntriesForTest(), retryTimers: engine.referenceRetryTimerCountForTest() },
      supervisor: { queued: supervisor.totalQueued, inFlight: supervisor.totalInFlight, busyKeys: supervisor.busyKeys, failures: supervisor.failureCount },
      sse: { subscribers: sse.subscribers, tracked: sse.tracked, bufferedBytes: sse.bufferedBytes },
      state: { marketSnapshots: engine.state.marketSnapshots.size, marketCache: engine.monitoredMarketIds().length + engine.assetMarketIndexEntries(), settledMarkets: engine.settledMarketsForTest().size, activations: engine.activationFootprint().latency, grokSignals: engine.state.grokActivity.acceptedSignals, idleTombstones: engine.idleGrokSubmissionCountForTest() },
    });
  };
  const sampler = (async () => {
    while (Date.now() < endAt) { sample(); await sleep(Math.max(10, options.sampleIntervalMs)); }
    sample();
  })();
  const producer = (async () => {
    let nextMarketAt = Date.now();
    let nextReferenceAt = Date.now();
    let didBurst = false;
    let didReconnect = false;
    let didDiscovery = false;
    while (Date.now() < endAt) {
      const now = Date.now();
      if (now - startedAt >= options.warmupMs) phase = "steady";
      if (!didBurst && now - startedAt >= Math.max(options.warmupMs, Math.floor(options.durationMs * 0.35))) {
        didBurst = true; phase = "burst";
        const stream = transport.stream;
        if (stream) {
          for (let burst = 0; burst < Math.max(10, options.markets * 8); burst += 1) {
            for (const market of markets) {
              const outcomeTokens = market.tokens.map((token) => token.tokenId);
              const result = transport.push(rawPriceChange(outcomeTokens, Date.now() + sequence, sequence));
              if (result.ok) { workload.sourceEventsSent += 1; workload.burstEvents += 1; } else workload.sourceEventsRejected += 1;
              sequence += 1;
            }
          }
        }
        phase = "steady";
      }
      if (options.reconnectStorm && !didReconnect && now - startedAt >= Math.floor(options.durationMs * 0.55)) {
        didReconnect = true; workload.reconnectsRequested += 1; transport.stream?.endRemote();
      }
      if (options.discoveryRefresh && !didDiscovery && now - startedAt >= Math.floor(options.durationMs * 0.7)) {
        didDiscovery = true; workload.discoveryRefreshes += 1; await engine.reconcileDiscovery();
      }
      if (now >= nextMarketAt) {
        const multiplier = phase === "burst" ? 3 : 1;
        for (const market of markets) {
          const outcomeTokens = market.tokens.map((token) => token.tokenId);
          if (!firstBooks.has(market.id)) {
            for (const token of market.tokens) {
              const result = transport.stream ? transport.push(rawBook(token.tokenId, market.id, Date.now() + sequence, 5)) : { ok: false as const, reason: "count_limit" as const };
              if (result.ok) workload.sourceEventsSent += 1; else workload.sourceEventsRejected += 1;
              sequence += 1;
            }
            firstBooks.add(market.id);
          } else {
            const result = transport.stream ? transport.push(rawPriceChange(outcomeTokens, Date.now() + sequence, sequence)) : { ok: false as const, reason: "count_limit" as const };
            if (result.ok) workload.sourceEventsSent += 1; else workload.sourceEventsRejected += 1;
            sequence += 1;
          }
        }
        nextMarketAt = now + Math.max(1, Math.floor(1000 / Math.max(1, options.marketRatePerSec * multiplier)));
      }
      if (now >= nextReferenceAt) {
        const asset = markets[(workload.referenceTicksSent + sequence) % markets.length]?.asset ?? "BTC";
        await engine.ingestReferenceTickForTest(referenceTick(asset, workload.referenceTicksSent));
        workload.referenceTicksSent += 1;
        nextReferenceAt = now + Math.max(1, Math.floor(1000 / Math.max(1, options.referenceRatePerSec)));
      }
      if (options.database === "unavailable" && now - startedAt >= options.warmupMs && now - startedAt < options.warmupMs + Math.max(100, options.durationMs * 0.25)) journal.available = false;
      else if (options.database === "unavailable") journal.available = true;
      await sleep(1);
    }
    phase = "recovery";
    await sleep(Math.max(options.sampleIntervalMs * 2, 100));
  })();
  await Promise.all([producer, sampler]);
  await sleep(Math.max(options.sampleIntervalMs * 2, 100));
  sample();
  eventLoop.disable();
  const rawEvents = await journal.queryEvents({ types: ["MARKET_SNAPSHOT", "REFERENCE_TICK"] });
  const durable = await journal.queryDurableTransitions({ limit: 10_000 });
  const warmupSamples = samples.filter((item) => item.phase === "warmup").length;
  const steadySamples = samples.filter((item) => item.phase !== "warmup").length;
  const finalWindowSamples = Math.max(0, Math.floor(steadySamples / 2));
  const metrics: CapacityReport["plateau"]["metrics"] = {
    rss: classifyMetric(samples, "rss", (item) => item.memory.rss, warmupSamples),
    heapUsed: classifyMetric(samples, "heapUsed", (item) => item.memory.heapUsed, warmupSamples),
    triggerQueue: classifyMetric(samples, "triggerQueue", (item) => item.trigger.queued, warmupSamples),
    journalWaiting: classifyMetric(samples, "journalWaiting", (item) => item.journal.waiting, warmupSamples),
    ingressDepth: classifyMetric(samples, "ingressDepth", (item) => item.ingress.depth, warmupSamples),
    sseBufferedBytes: classifyMetric(samples, "sseBufferedBytes", (item) => item.sse.bufferedBytes, warmupSamples),
    retryTimers: classifyMetric(samples, "retryTimers", (item) => item.reference.retryTimers, warmupSamples),
  };
  const last = samples.at(-1)!;
  const max = (selector: (item: CapacitySample) => number) => Math.max(0, ...samples.map(selector));
  const result: CapacityReport = {
    schemaVersion: 1, capturedAt: new Date().toISOString(), locality: "local-synthetic", target: "Fly shared-cpu-2x 512MB FRA singleton", options,
    workload, samples,
    plateau: { warmupSamples, steadySamples, finalWindowSamples, metrics },
    invariants: {
      boundedIngress: max((item) => item.ingress.depth) <= 10_000 && max((item) => item.ingress.bytes) <= 16_000_000,
      boundedSupervisor: max((item) => item.supervisor.queued) <= 1_280,
      boundedJournalAdmission: max((item) => item.journal.waiting) <= 128,
      boundedReferenceWindow: max((item) => item.reference.windowEntries) <= 16_384,
      boundedSse: max((item) => item.sse.bufferedBytes) <= 262_144 * Math.max(1, options.dashboard === "slow" ? 2 : 1),
      boundedRuntimeBookkeeping: max((item) => item.state.marketSnapshots) <= Math.max(2, options.markets * 2),
      rawMarketAndReferenceEventsPersisted: rawEvents.length,
      durableTransitions: durable.length,
      dashboardSlowClientDisconnected: options.dashboard !== "slow" || slowEnded,
    },
    cpuModel: {
      measuredLocal: { processCpuMs: samples.reduce((sum, item) => sum + item.cpu.processMs, 0), elapsedMs: last.elapsedMs },
      syntheticEstimate: "Measured on the local host only; rates and payloads are synthetic and are not a Fly CPU percentage.",
      flySoakRequired: ["shared-cpu-2x CPU utilization and steal", "RSS/heap/event-loop delay over 48h", "DB latency and pool waiters", "market/reference ingress and reconnect rate", "SSE client count and writable buffers"],
    },
  };
  await engine.stop();
  return result;
}

function parseArgs(argv: string[]): CapacityHarnessOptions & { json?: string } {
  const result: CapacityHarnessOptions & { json?: string } = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index] ?? "";
    const value = argv[index + 1];
    if (!arg.startsWith("--") || value === undefined) continue;
    const key = arg.slice(2);
    if (key === "json") result.json = value;
    else if (key === "duration-ms") result.durationMs = Number(value);
    else if (key === "warmup-ms") result.warmupMs = Number(value);
    else if (key === "sample-ms") result.sampleIntervalMs = Number(value);
    else if (key === "markets") result.markets = Number(value);
    else if (key === "market-rate") result.marketRatePerSec = Number(value);
    else if (key === "reference-rate") result.referenceRatePerSec = Number(value);
    else if (key === "dashboard") result.dashboard = value as DashboardMode;
    else if (key === "database") result.database = value as DatabaseMode;
    else if (key === "database-delay-ms") result.databaseDelayMs = Number(value);
    else if (key === "reconnect-storm") result.reconnectStorm = value !== "false";
    else if (key === "discovery-refresh") result.discoveryRefresh = value !== "false";
  }
  return result;
}

if (process.argv[1] && fileURLToPath(import.meta.url).toLowerCase() === process.argv[1].toLowerCase()) {
  const options = parseArgs(process.argv.slice(2));
  const output = await runSustainedCapacityHarness(options);
  const outDir = join(tmpdir(), "polymarket-engine-capacity");
  mkdirSync(outDir, { recursive: true });
  const out = options.json ? join(process.cwd(), options.json) : join(outDir, `sustained-capacity-${Date.now()}.json`);
  writeFileSync(out, JSON.stringify(output, null, 2));
  console.log(JSON.stringify(output, null, 2));
  console.log(`wrote ${out}`);
}
