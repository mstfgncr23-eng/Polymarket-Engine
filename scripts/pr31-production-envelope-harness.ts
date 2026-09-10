import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { monitorEventLoopDelay, performance } from "node:perf_hooks";
import { engineEnvSchema, type MarketSnapshot } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { EngineService } from "../apps/engine/src/engine.js";
import {
  BoundedClobMarketTransport,
  INGRESS_GAP_REASONS,
  PolymarketMarketDataAdapter,
  type BoundedMarketStream,
  type MarketStreamTransport,
} from "../packages/market-data/src/index.js";

export const PR31_CANDIDATE = {
  maxDeliveryEventsPerSlice: 128,
  maxDeliverySliceMs: 0.5,
} as const;

export const TOKEN_COUNT = 80;
export const MARKET_COUNT = 40;
export const INITIAL_BOOKS = TOKEN_COUNT;
export const MAX_CATCH_UP_EVENTS_PER_TURN = 64;
export const PRODUCER_SCHEDULING_TOLERANCE_MS = 100;
export const PRODUCER_GRACE_MS = 2_000;

export type EnvelopeScenarioName = "A" | "B" | "C" | "probe";
export type SourceEventKind = "book" | "price_change" | "last_trade_price";
export type EventPhase = "initial" | "workload";

export interface RateSegment {
  label: string;
  durationMs: number;
  ratePerSec: number;
}

export interface EnvelopeScenario {
  name: EnvelopeScenarioName;
  description: string;
  observed: boolean;
  syntheticSafetyMargin: boolean;
  segments: readonly RateSegment[];
}

export const ENVELOPE_SCENARIOS: readonly EnvelopeScenario[] = [
  {
    name: "A",
    description: "Stage0 average source-event load",
    observed: true,
    syntheticSafetyMargin: false,
    segments: [{ label: "average", durationMs: 30_000, ratePerSec: 1_714 }],
  },
  {
    name: "B",
    description: "Stage0 observed approximately six-second peak",
    observed: true,
    syntheticSafetyMargin: false,
    segments: [
      { label: "average-before-peak", durationMs: 10_000, ratePerSec: 1_714 },
      { label: "observed-peak", durationMs: 6_000, ratePerSec: 3_650 },
      { label: "average-after-peak", durationMs: 10_000, ratePerSec: 1_714 },
    ],
  },
  {
    name: "C",
    description: "Synthetic safety margin above the observed 3,641/s peak",
    observed: false,
    syntheticSafetyMargin: true,
    segments: [{ label: "synthetic-safety", durationMs: 10_000, ratePerSec: 4_500 }],
  },
] as const;

export interface LatencySummary {
  count: number;
  p50Ms: number | null;
  p95Ms: number | null;
  p99Ms: number | null;
  maxMs: number | null;
}

export interface DurationSummary {
  count: number;
  totalMs: number;
  meanMs: number | null;
}

export interface SourceCounts {
  book: number;
  price_change: number;
  last_trade_price: number;
  total: number;
}

export interface EnvelopeRunResult {
  scenario: EnvelopeScenarioName | "probe";
  repeat: number;
  pass: boolean;
  maxEvents: number;
  maxSliceMs: number;
  initialBooksExpected: number;
  initialBooksAttempted: number;
  initialBooksAdmitted: number;
  initialBooksDelivered: number;
  initialBooksProcessed: number;
  workloadEventsScheduled: number;
  workloadEventsExpected: number;
  workloadEventsAttempted: number;
  workloadEventsAdmitted: number;
  workloadEventsDelivered: number;
  workloadEventsProcessed: number;
  producerDueEventsAtStop: number;
  actualProducerRatePerSec: number;
  workloadDeliveredRatePerSec: number;
  producerSchedulingLag: LatencySummary;
  workloadSourceCountsAttempted: SourceCounts;
  workloadSourceCountsAdmitted: SourceCounts;
  workloadSourceCountsDelivered: SourceCounts;
  workloadPriceChangeEntriesAttempted: number;
  workloadPriceChangeEntriesDelivered: number;
  workloadNormalizedSnapshotsExpected: number;
  workloadNormalizedSnapshots: number;
  initialNormalizedSnapshots: number;
  allInitialBooksProcessed: boolean;
  loss: number;
  duplicateEvents: number;
  fifoViolations: number;
  perTokenOrderingViolations: number;
  gapsByReason: Record<string, number>;
  resyncs: number;
  unsynchronizedTokens: number;
  queueFinalDepth: number;
  queueFinalBytes: number;
  queueDepthAtProducerStop: number;
  queueHighWaterEvents: number;
  queueHighWaterBytes: number;
  countLimitGaps: number;
  byteLimitGaps: number;
  transportDeliveryLatency: LatencySummary;
  snapshotCompletionLatency: LatencySummary;
  eventLoopUtilization: number;
  eventLoopDelayP50Ms: number;
  eventLoopDelayP95Ms: number;
  eventLoopDelayP99Ms: number;
  eventLoopDelayMaxMs: number;
  fairnessProbeCallbacks: number;
  fairnessProbeCallbacksWhileBacklogged: number;
  fairnessProbeMaxDriftMs: number;
  processCpuMs: number;
  wallMs: number;
  engineOnMarketSnapshot: DurationSummary;
  triggerEngineIngestMarket: DurationSummary;
  adapterNormalization: DurationSummary;
  drainCompletionMs: number | null;
  drainTimedOut: boolean;
  terminalError: string | null;
  adapterErrors: number;
  reconnects: number;
  connectionGeneration: number;
  notes: string[];
}

interface TokenSpec {
  tokenId: string;
  marketId: string;
  outcome: "UP" | "DOWN";
}

interface SequenceTag {
  sequence: number;
  sentAtMs: number;
}

class EnvelopeWebSocket extends EventTarget {
  static instances: EnvelopeWebSocket[] = [];
  readyState = 0;

  constructor(readonly url: string) {
    super();
    EnvelopeWebSocket.instances.push(this);
  }

  send(_data: string): void {}

  close(): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatchEvent(Object.assign(new Event("close"), { code: 1000, reason: "", wasClean: true }));
  }

  open(): void {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }

  message(data: string): void {
    if (this.readyState !== 1) return;
    this.dispatchEvent(new MessageEvent("message", { data }));
  }
}

class Samples {
  private readonly values: number[] = [];
  private maximum: number | null = null;

  add(value: number): void {
    if (!Number.isFinite(value) || value < 0) return;
    this.values.push(value);
    this.maximum = this.maximum === null ? value : Math.max(this.maximum, value);
  }

  summary(): LatencySummary {
    if (this.values.length === 0) return { count: 0, p50Ms: null, p95Ms: null, p99Ms: null, maxMs: null };
    const sorted = [...this.values].sort((left, right) => left - right);
    const quantile = (fraction: number) => sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * fraction))]!;
    return {
      count: this.values.length,
      p50Ms: quantile(0.5),
      p95Ms: quantile(0.95),
      p99Ms: quantile(0.99),
      maxMs: this.maximum,
    };
  }
}

function durationSummary(count: number, totalSeconds: number): DurationSummary {
  const totalMs = totalSeconds * 1_000;
  return { count, totalMs, meanMs: count > 0 ? totalMs / count : null };
}

function emptySourceCounts(): SourceCounts {
  return { book: 0, price_change: 0, last_trade_price: 0, total: 0 };
}

function addSourceCount(counts: SourceCounts, kind: SourceEventKind): void {
  counts[kind] += 1;
  counts.total += 1;
}

function tokens(): TokenSpec[] {
  return Array.from({ length: MARKET_COUNT }, (_, marketIndex) => {
    const marketId = `pr31-market-${String(marketIndex).padStart(2, "0")}`;
    return [
      { tokenId: `${marketId}-up`, marketId, outcome: "UP" as const },
      { tokenId: `${marketId}-down`, marketId, outcome: "DOWN" as const },
    ];
  }).flat();
}

const TOKEN_SPECS = tokens();
const TOKEN_BY_ID = new Map(TOKEN_SPECS.map((token) => [token.tokenId, token]));

function sourceKind(index: number): SourceEventKind {
  const bucket = index % 1_000;
  if (bucket < 981) return "price_change";
  if (bucket < 994) return "book";
  return "last_trade_price";
}

function sourceTag(sequence: number, sentAtMs: number): string {
  return `pr31-${sequence}-${sentAtMs.toFixed(3)}`;
}

function tagFromEvent(event: unknown): SequenceTag | null {
  const value = event as { type?: string; payload?: Record<string, unknown> };
  const payload = value.payload ?? {};
  let candidate: unknown = payload.hash;
  if (value.type === "price_change" && Array.isArray(payload.priceChanges)) {
    candidate = (payload.priceChanges[0] as Record<string, unknown> | undefined)?.hash;
  }
  if (value.type === "last_trade_price") candidate = payload.transactionHash;
  if (typeof candidate !== "string") return null;
  const match = /^pr31-(\d+)-([0-9]+(?:\.[0-9]+)?)$/.exec(candidate);
  return match ? { sequence: Number(match[1]), sentAtMs: Number(match[2]) } : null;
}

function tagFromSnapshot(snapshot: MarketSnapshot): SequenceTag | null {
  if (typeof snapshot.hash !== "string") return null;
  const match = /^pr31-(\d+)-([0-9]+(?:\.[0-9]+)?)$/.exec(snapshot.hash);
  return match ? { sequence: Number(match[1]), sentAtMs: Number(match[2]) } : null;
}

function timestampFor(sequence: number): number {
  return 1_700_000_000_000 + sequence;
}

function sourceFrame(kind: SourceEventKind, sequence: number, sentAtMs: number): string {
  const tag = sourceTag(sequence, sentAtMs);
  if (kind === "price_change") {
    const pair = TOKEN_SPECS[sequence % MARKET_COUNT * 2]!;
    const down = TOKEN_SPECS[sequence % MARKET_COUNT * 2 + 1]!;
    return JSON.stringify({
      event_type: "price_change",
      market: pair.marketId,
      price_changes: [
        { asset_id: pair.tokenId, price: "0.50", size: "1", side: "BUY", hash: tag },
        { asset_id: down.tokenId, price: "0.50", size: "1", side: "BUY", hash: tag },
      ],
      timestamp: timestampFor(sequence),
    });
  }
  if (kind === "book") {
    const token = TOKEN_SPECS[(sequence * 2) % TOKEN_COUNT]!;
    return JSON.stringify({
      event_type: "book",
      market: token.marketId,
      asset_id: token.tokenId,
      bids: [{ price: "0.50", size: "1" }],
      asks: [{ price: "0.51", size: "1" }],
      hash: tag,
      timestamp: timestampFor(sequence),
    });
  }
  const token = TOKEN_SPECS[(sequence * 3 + 1) % TOKEN_COUNT]!;
  return JSON.stringify({
    event_type: "last_trade_price",
    market: token.marketId,
    asset_id: token.tokenId,
    price: "0.50",
    size: "1",
    fee_rate_bps: "0",
    side: "BUY",
    transaction_hash: tag,
    timestamp: timestampFor(sequence),
  });
}

function initialBookFrame(token: TokenSpec, sequence: number, sentAtMs: number): string {
  return JSON.stringify({
    event_type: "book",
    market: token.marketId,
    asset_id: token.tokenId,
    bids: [{ price: "0.50", size: "1" }],
    asks: [{ price: "0.51", size: "1" }],
    hash: sourceTag(sequence, sentAtMs),
    timestamp: timestampFor(sequence),
  });
}

function observedStream(stream: BoundedMarketStream, onEvent: (event: unknown) => void): BoundedMarketStream {
  const iterator = stream[Symbol.asyncIterator]();
  return {
    close: (intent) => stream.close(intent),
    closeDiagnostic: () => stream.closeDiagnostic?.() ?? null,
    pendingWaiterCount: () => stream.pendingWaiterCount(),
    pingTimerActive: () => stream.pingTimerActive(),
    streamGeneration: () => stream.streamGeneration(),
    [Symbol.asyncIterator]: () => ({
      next: async () => {
        const result = await iterator.next();
        if (!result.done) onEvent(result.value);
        return result;
      },
    }),
  };
}

function observedTransport(
  transport: BoundedClobMarketTransport,
  onEvent: (event: unknown) => void,
): MarketStreamTransport {
  return {
    bounded: true,
    queue: transport.queue,
    subscribe: async (tokenIds, customFeatureEnabled, signal) => observedStream(
      await transport.subscribe(tokenIds, customFeatureEnabled, signal),
      onEvent,
    ),
  };
}

function expectedWorkloadEvents(scenario: EnvelopeScenario): number {
  return scenario.segments.reduce((sum, segment) => sum + Math.floor(segment.durationMs * segment.ratePerSec / 1_000), 0);
}

function scenarioDurationMs(scenario: EnvelopeScenario): number {
  return scenario.segments.reduce((sum, segment) => sum + segment.durationMs, 0);
}

function expectedAt(scenario: EnvelopeScenario, elapsedMs: number): number {
  let remaining = Math.max(0, elapsedMs);
  let total = 0;
  for (const segment of scenario.segments) {
    const inside = Math.min(segment.durationMs, remaining);
    total += Math.floor(inside * segment.ratePerSec / 1_000);
    remaining -= inside;
    if (remaining <= 0) break;
  }
  return Math.min(expectedWorkloadEvents(scenario), total);
}

function dueAtForOrdinal(scenario: EnvelopeScenario, ordinal: number): number {
  let offset = 0;
  let remaining = ordinal;
  for (const segment of scenario.segments) {
    const capacity = Math.floor(segment.durationMs * segment.ratePerSec / 1_000);
    if (remaining < capacity) return offset + ((remaining + 1) * 1_000) / segment.ratePerSec;
    remaining -= capacity;
    offset += segment.durationMs;
  }
  return offset;
}

async function waitFor(predicate: () => boolean, timeoutMs: number, message: string): Promise<void> {
  const started = performance.now();
  while (!predicate()) {
    if (performance.now() - started > timeoutMs) throw new Error(message);
    await new Promise<void>((resolve) => setTimeout(resolve, 1));
  }
}

async function metricPoints(metrics: EngineService["metrics"], name: string): Promise<Array<{ metricName?: string; value: number; labels: Record<string, string> }>> {
  const metric = metrics.registry.getSingleMetric(name);
  if (!metric) return [];
  const data = await metric.get();
  return data.values.map((point) => {
    const named = point as unknown as { metricName?: string; value: number; labels?: Record<string, string | number> };
    return {
      metricName: named.metricName,
      value: Number(named.value),
      labels: Object.fromEntries(Object.entries(named.labels ?? {}).map(([key, value]) => [key, String(value)])),
    };
  });
}

async function counterValue(metrics: EngineService["metrics"], name: string): Promise<number> {
  const points = await metricPoints(metrics, name);
  return points.filter((point) => point.metricName === name).reduce((sum, point) => sum + point.value, 0);
}

async function labeledCounterValues(metrics: EngineService["metrics"], name: string, label: string): Promise<Record<string, number>> {
  const values: Record<string, number> = {};
  for (const point of await metricPoints(metrics, name)) {
    if (point.metricName !== name) continue;
    const key = point.labels[label];
    if (key) values[key] = (values[key] ?? 0) + point.value;
  }
  return values;
}

async function histogramSummary(metrics: EngineService["metrics"], name: string): Promise<DurationSummary> {
  const points = await metricPoints(metrics, name);
  const count = points.filter((point) => point.metricName === `${name}_count`).reduce((sum, point) => sum + point.value, 0);
  const totalSeconds = points.filter((point) => point.metricName === `${name}_sum`).reduce((sum, point) => sum + point.value, 0);
  return durationSummary(count, totalSeconds);
}

function eventLoopMetric(delay: ReturnType<typeof monitorEventLoopDelay>, percentile: number): number {
  const value = delay.percentile(percentile) / 1_000_000;
  return Number.isFinite(value) ? value : 0;
}

function normalizeScenario(input: EnvelopeScenario | { name?: "probe"; segments: readonly RateSegment[] }): EnvelopeScenario {
  if ("description" in input) return input;
  return {
    name: "probe",
    description: "focused deterministic production-path probe",
    observed: false,
    syntheticSafetyMargin: false,
    segments: input.segments,
  };
}

export interface EnvelopeRunOptions {
  scenario: EnvelopeScenario | { name?: "probe"; segments: readonly RateSegment[] };
  repeat?: number;
}

export async function runEnvelopeOnce(options: EnvelopeRunOptions): Promise<EnvelopeRunResult> {
  const scenario = normalizeScenario(options.scenario);
  const repeat = options.repeat ?? 1;
  const expected = expectedWorkloadEvents(scenario);
  const durationMs = scenarioDurationMs(scenario);
  EnvelopeWebSocket.instances = [];

  const transportBase = new BoundedClobMarketTransport({
    webSocket: EnvelopeWebSocket as unknown as typeof WebSocket,
    maxDeliveryEventsPerSlice: PR31_CANDIDATE.maxDeliveryEventsPerSlice,
    maxDeliverySliceMs: PR31_CANDIDATE.maxDeliverySliceMs,
    pingIntervalMs: 60_000,
    staleMs: 120_000,
    connectTimeoutMs: 2_000,
  });

  let sourceFramesAttempted = 0;
  let nextSequence = 1;
  let initialBooksAttempted = 0;
  let initialBooksAdmitted = 0;
  let initialBooksDelivered = 0;
  let workloadEventsAttempted = 0;
  let workloadEventsAdmitted = 0;
  let workloadEventsDelivered = 0;
  let initialNormalizedSnapshots = 0;
  let workloadNormalizedSnapshots = 0;
  let workloadPriceChangeEntriesDelivered = 0;
  let initialPriceChangeEntries = 0;
  let producerDueEventsAtStop = 0;
  let workloadSourceCountsAttempted = emptySourceCounts();
  let workloadSourceCountsAdmitted = emptySourceCounts();
  let workloadSourceCountsDelivered = emptySourceCounts();
  let workloadPriceChangeEntriesAttempted = 0;
  let initialQueueAccepted = 0;
  let initialQueueProcessed = 0;
  let workloadQueueAccepted = 0;
  let workloadQueueProcessed = 0;
  let snapshotHandlerCount = 0;
  let adapterErrors = 0;
  let terminalError: string | null = null;
  let currentPhase: EventPhase = "initial";
  const phaseBySequence = new Map<number, EventPhase>();
  const kindBySequence = new Map<number, SourceEventKind>();
  const transportLatency = new Samples();
  const snapshotLatency = new Samples();
  const producerLag = new Samples();
  const seenSequences = new Set<number>();
  const lastTokenSequence = new Map<string, number>();
  let duplicateEvents = 0;
  let fifoViolations = 0;
  let perTokenOrderingViolations = 0;
  let lastTransportSequence = 0;

  const onTransportEvent = (event: unknown): void => {
    const tag = tagFromEvent(event);
    if (!tag) {
      fifoViolations += 1;
      return;
    }
    if (seenSequences.has(tag.sequence)) duplicateEvents += 1;
    seenSequences.add(tag.sequence);
    if (tag.sequence !== lastTransportSequence + 1) fifoViolations += 1;
    lastTransportSequence = tag.sequence;
    transportLatency.add(Math.max(0, performance.now() - tag.sentAtMs));
    const phase = phaseBySequence.get(tag.sequence);
    if (phase === "initial") initialBooksDelivered += 1;
    if (phase !== "workload") return;
    workloadEventsDelivered += 1;
    const kind = kindBySequence.get(tag.sequence);
    if (kind) addSourceCount(workloadSourceCountsDelivered, kind);
    if (kind === "price_change") workloadPriceChangeEntriesDelivered += 2;
  };

  const transport = observedTransport(transportBase, onTransportEvent);
  const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transportBase.queue });
  const env = engineEnvSchema.parse({
    NODE_ENV: "test",
    TRADING_MODE: "paper",
    JOURNAL_MODE: "memory",
    GROK_ACTIVATION_PROVIDER: "noop",
    POLYMARKET_TOKEN_IDS: TOKEN_SPECS.map((token) => token.tokenId).join(","),
    DISCOVERY_INTERVAL_MS: "3600000",
    RETENTION_ENABLED: "false",
    DASHBOARD_SNAPSHOT_HZ: "1",
  });
  const journal = new MemoryEventJournal();
  const loggerErrors: string[] = [];
  const engine = new EngineService(env, journal, {
    error: (_bindings, message) => {
      loggerErrors.push(message);
      terminalError ??= message;
    },
  }, {
    market: adapter,
    activation: new NoopGrokActivationBridge(),
    shutdownJoinDeadlineMs: 5_000,
  });

  const innerNormalizeObserver = engine.metrics.normalizeObserver();
  adapter.setNormalizeObserver({
    onNormalizedSnapshot: (input) => {
      if (currentPhase === "initial") initialNormalizedSnapshots += 1;
      else workloadNormalizedSnapshots += 1;
      innerNormalizeObserver.onNormalizedSnapshot(input);
    },
    onPriceChangeEvent: (entries, processed) => {
      if (currentPhase === "initial") initialPriceChangeEntries += entries;
      else if (processed > 0) workloadPriceChangeEntriesDelivered += 0;
      innerNormalizeObserver.onPriceChangeEvent(entries, processed);
    },
  });

  const engineInternals = engine as unknown as {
    onMarketSnapshot: (snapshot: MarketSnapshot) => Promise<void>;
  };
  const realOnMarketSnapshot = engineInternals.onMarketSnapshot.bind(engine);
  engineInternals.onMarketSnapshot = async (snapshot) => {
    const tag = tagFromSnapshot(snapshot);
    await realOnMarketSnapshot(snapshot);
    snapshotHandlerCount += 1;
    if (tag && phaseBySequence.get(tag.sequence) === "workload") {
      snapshotLatency.add(Math.max(0, performance.now() - tag.sentAtMs));
      const previous = lastTokenSequence.get(snapshot.tokenId);
      if (previous !== undefined && tag.sequence <= previous) perTokenOrderingViolations += 1;
      lastTokenSequence.set(snapshot.tokenId, tag.sequence);
    }
  };

  const delay = monitorEventLoopDelay({ resolution: 10 });
  const fairnessIntervalMs = 10;
  let fairnessProbeCallbacks = 0;
  let fairnessProbeCallbacksWhileBacklogged = 0;
  let fairnessProbeMaxDriftMs = 0;
  let startElu = performance.eventLoopUtilization();
  let cpuStart = process.cpuUsage();
  let wallStart = performance.now();
  let previousFairnessAt = wallStart;
  let fairnessTimer: ReturnType<typeof setInterval> | null = null;

  const send = (raw: string, kind: SourceEventKind, sequence: number, phase: EventPhase): boolean => {
    const socket = EnvelopeWebSocket.instances[0];
    if (!socket || socket.readyState !== 1) return false;
    const before = transportBase.queue.accepted;
    sourceFramesAttempted += 1;
    phaseBySequence.set(sequence, phase);
    kindBySequence.set(sequence, kind);
    socket.message(raw);
    const admitted = transportBase.queue.accepted - before;
    if (phase === "initial") {
      initialBooksAttempted += 1;
      initialBooksAdmitted += admitted;
    } else {
      workloadEventsAttempted += 1;
      workloadEventsAdmitted += admitted;
      addSourceCount(workloadSourceCountsAttempted, kind);
      if (admitted > 0) addSourceCount(workloadSourceCountsAdmitted, kind);
      if (kind === "price_change") {
        workloadPriceChangeEntriesAttempted += 2;
      }
    }
    return true;
  };

  let producerStart = wallStart;
  let producerStop = wallStart;
  let drainCompletionMs: number | null = null;
  let drainTimedOut = false;

  try {
    await engine.start();
    for (const token of TOKEN_SPECS) adapter.registerManualToken(token.tokenId, token.marketId, token.outcome, "BTC");
    await waitFor(() => EnvelopeWebSocket.instances.length === 1, 5_000, "envelope WebSocket was not constructed");
    const socket = EnvelopeWebSocket.instances[0]!;
    socket.open();
    await waitFor(() => engine.state.connections.polymarket.state === "CONNECTED", 5_000, "real Engine market loop did not connect");

    currentPhase = "initial";
    for (const token of TOKEN_SPECS) {
      const sequence = nextSequence++;
      const sentAtMs = performance.now();
      send(initialBookFrame(token, sequence, sentAtMs), "book", sequence, "initial");
    }
    initialQueueAccepted = transportBase.queue.accepted;
    await waitFor(
      () => adapter.unsynchronizedTokenCount() === 0
        && initialBooksDelivered === INITIAL_BOOKS
        && transportBase.queue.depth === 0
        && transportBase.queue.processed >= INITIAL_BOOKS,
      15_000,
      "initial trusted books did not fully drain",
    );
    initialQueueProcessed = transportBase.queue.processed;

    currentPhase = "workload";
    startElu = performance.eventLoopUtilization();
    delay.enable();
    cpuStart = process.cpuUsage();
    wallStart = performance.now();
    previousFairnessAt = wallStart;
    fairnessTimer = setInterval(() => {
      const now = performance.now();
      fairnessProbeCallbacks += 1;
      if (transportBase.queue.depth > 0) fairnessProbeCallbacksWhileBacklogged += 1;
      fairnessProbeMaxDriftMs = Math.max(fairnessProbeMaxDriftMs, Math.max(0, now - previousFairnessAt - fairnessIntervalMs));
      previousFairnessAt = now;
    }, fairnessIntervalMs);
    fairnessTimer.unref?.();
    producerStart = performance.now();
    const producerDeadline = producerStart + durationMs + PRODUCER_GRACE_MS;
    let ordinal = 0;
    while (ordinal < expected && performance.now() <= producerDeadline) {
      const now = performance.now();
      const elapsed = now - producerStart;
      producerDueEventsAtStop = expectedAt(scenario, elapsed);
      const available = producerDueEventsAtStop - ordinal;
      if (available > 0) {
        const batch = Math.min(available, MAX_CATCH_UP_EVENTS_PER_TURN);
        for (let i = 0; i < batch && ordinal < expected; i += 1) {
          const dueAt = dueAtForOrdinal(scenario, ordinal);
          const sentAtMs = performance.now();
          producerLag.add(Math.max(0, sentAtMs - (producerStart + dueAt)));
          const sequence = nextSequence++;
          const kind = sourceKind(ordinal);
          if (!send(sourceFrame(kind, sequence, sentAtMs), kind, sequence, "workload")) {
            ordinal = expected;
            break;
          }
          ordinal += 1;
        }
        await new Promise<void>((resolve) => setImmediate(resolve));
        continue;
      }
      const nextDue = producerStart + dueAtForOrdinal(scenario, ordinal);
      const waitMs = nextDue - performance.now();
      if (waitMs > 0.5) {
        await new Promise<void>((resolve) => setTimeout(resolve, Math.min(5, waitMs)));
      } else {
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
    }
    producerStop = performance.now();
    if (ordinal === expected) producerDueEventsAtStop = expected;
    workloadQueueAccepted = transportBase.queue.accepted - initialQueueAccepted;
    const producerStopDepth = transportBase.queue.depth;

    try {
      await waitFor(
        () => workloadEventsDelivered === expected
          && transportBase.queue.processed - initialQueueProcessed >= expected
          && transportBase.queue.depth === 0
          && workloadNormalizedSnapshots >= workloadNormalizedSnapshotExpectation(scenario, expected),
        20_000,
        "workload did not complete and drain",
      );
      drainCompletionMs = performance.now() - producerStop;
    } catch {
      drainTimedOut = true;
    }

    const queueAtEnd = transportBase.queue.snapshot();
    const deliveredAfterInitial = workloadEventsDelivered;
    const workloadProcessed = queueAtEnd.processed - initialQueueProcessed;
    workloadQueueProcessed = Math.max(0, workloadProcessed);
    const gapsByReason = await labeledCounterValues(engine.metrics, "market_ingress_gap_total", "reason");
    const resyncs = await counterValue(engine.metrics, "market_ingress_resync_total");
    const cpu = process.cpuUsage(cpuStart);
    const wallMs = performance.now() - wallStart;
    delay.disable();
    const elu = performance.eventLoopUtilization(startElu).utilization;
    const expectedSnapshots = workloadNormalizedSnapshotExpectation(scenario, expected);
    const loss = Math.max(0, expected - deliveredAfterInitial);
    const queueCountLimit = gapsByReason.count_limit ?? 0;
    const queueByteLimit = gapsByReason.byte_limit ?? 0;
    const producerLagSummary = producerLag.summary();
    const pass = ordinal === expected
      && workloadEventsAttempted === expected
      && workloadEventsAdmitted === expected
      && workloadEventsDelivered === expected
      && workloadQueueProcessed === expected
      && workloadNormalizedSnapshots === expectedSnapshots
      && queueCountLimit === 0
      && queueByteLimit === 0
      && loss === 0
      && duplicateEvents === 0
      && fifoViolations === 0
      && perTokenOrderingViolations === 0
      && Object.values(gapsByReason).every((value) => value === 0)
      && resyncs === 0
      && adapter.unsynchronizedTokenCount() === 0
      && queueAtEnd.depth === 0
      && queueAtEnd.bytes === 0
      && !drainTimedOut
      && terminalError === null
      && fairnessProbeCallbacksWhileBacklogged > 0
      && (producerLagSummary.maxMs ?? 0) <= PRODUCER_SCHEDULING_TOLERANCE_MS;

    const result: EnvelopeRunResult = {
      scenario: scenario.name,
      repeat,
      pass,
      maxEvents: PR31_CANDIDATE.maxDeliveryEventsPerSlice,
      maxSliceMs: PR31_CANDIDATE.maxDeliverySliceMs,
      initialBooksExpected: INITIAL_BOOKS,
      initialBooksAttempted,
      initialBooksAdmitted,
      initialBooksDelivered,
      initialBooksProcessed: Math.max(0, initialQueueProcessed),
      workloadEventsScheduled: expected,
      workloadEventsExpected: expected,
      workloadEventsAttempted,
      workloadEventsAdmitted,
      workloadEventsDelivered,
      workloadEventsProcessed: workloadQueueProcessed,
      producerDueEventsAtStop,
      actualProducerRatePerSec: workloadEventsAttempted / Math.max(0.001, (producerStop - producerStart) / 1_000),
      workloadDeliveredRatePerSec: workloadEventsDelivered / Math.max(0.001, (performance.now() - producerStart) / 1_000),
      producerSchedulingLag: producerLagSummary,
      workloadSourceCountsAttempted,
      workloadSourceCountsAdmitted,
      workloadSourceCountsDelivered,
      workloadPriceChangeEntriesAttempted,
      workloadPriceChangeEntriesDelivered,
      workloadNormalizedSnapshotsExpected: expectedSnapshots,
      workloadNormalizedSnapshots,
      initialNormalizedSnapshots,
      allInitialBooksProcessed: initialBooksAttempted === INITIAL_BOOKS
        && initialBooksAdmitted === INITIAL_BOOKS
        && initialBooksDelivered === INITIAL_BOOKS
        && initialNormalizedSnapshots === INITIAL_BOOKS
        && adapter.unsynchronizedTokenCount() === 0,
      loss,
      duplicateEvents,
      fifoViolations,
      perTokenOrderingViolations,
      gapsByReason: Object.fromEntries(INGRESS_GAP_REASONS.map((reason) => [reason, gapsByReason[reason] ?? 0])),
      resyncs,
      unsynchronizedTokens: adapter.unsynchronizedTokenCount(),
      queueFinalDepth: queueAtEnd.depth,
      queueFinalBytes: queueAtEnd.bytes,
      queueDepthAtProducerStop: producerStopDepth,
      queueHighWaterEvents: queueAtEnd.highWaterEvents,
      queueHighWaterBytes: queueAtEnd.highWaterBytes,
      countLimitGaps: queueCountLimit,
      byteLimitGaps: queueByteLimit,
      transportDeliveryLatency: transportLatency.summary(),
      snapshotCompletionLatency: snapshotLatency.summary(),
      eventLoopUtilization: Number.isFinite(elu) ? elu : 0,
      eventLoopDelayP50Ms: eventLoopMetric(delay, 50),
      eventLoopDelayP95Ms: eventLoopMetric(delay, 95),
      eventLoopDelayP99Ms: eventLoopMetric(delay, 99),
      eventLoopDelayMaxMs: Number.isFinite(delay.max / 1_000_000) ? delay.max / 1_000_000 : 0,
      fairnessProbeCallbacks,
      fairnessProbeCallbacksWhileBacklogged,
      fairnessProbeMaxDriftMs,
      processCpuMs: (cpu.user + cpu.system) / 1_000,
      wallMs,
      engineOnMarketSnapshot: await histogramSummary(engine.metrics, "event_processing_latency"),
      triggerEngineIngestMarket: await histogramSummary(engine.metrics, "trigger_market_ingest_duration_seconds"),
      adapterNormalization: await histogramSummary(engine.metrics, "market_normalization_duration_seconds"),
      drainCompletionMs,
      drainTimedOut,
      terminalError,
      adapterErrors,
      reconnects: engine.state.connections.polymarket.reconnects,
      connectionGeneration: transportBase.currentGeneration(),
      notes: [
        "Source-event schedule used real-time pacing with a maximum synchronous catch-up batch of 64 events per turn.",
        "Fairness maximum drift is per 10 ms timer interval (actual interval minus 10 ms), not cumulative timer phase drift.",
        `Producer scheduling lag tolerance was ${PRODUCER_SCHEDULING_TOLERANCE_MS} ms; this is a harness pass criterion, not a production SLO.`,
        "processed is the bounded queue processed counter after iterator delivery; normalized snapshots are measured separately by the real adapter observer.",
        "Engine instrumentation wraps and calls the real onMarketSnapshot implementation; wrapper overhead is not production code.",
        scenario.syntheticSafetyMargin ? "Scenario C is synthetic safety margin above the observed Stage0 peak, not an observed production rate." : "Scenario rate is drawn from the Stage0 observed envelope.",
      ],
    };
    return result;
  } catch (error) {
    terminalError ??= error instanceof Error ? error.message : String(error);
    throw error;
  } finally {
    if (fairnessTimer) clearInterval(fairnessTimer);
    await engine.stop().catch((error) => {
      adapterErrors += 1;
      terminalError ??= error instanceof Error ? error.message : String(error);
    });
    adapterErrors += loggerErrors.filter((message) => message.includes("market feed error")).length;
  }
}

function workloadNormalizedSnapshotExpectation(scenario: EnvelopeScenario, expected: number): number {
  let book = 0;
  let priceChange = 0;
  for (let index = 0; index < expected; index += 1) {
    const kind = sourceKind(index);
    if (kind === "book") book += 1;
    if (kind === "price_change") priceChange += 2;
  }
  return book + priceChange;
}

function scenarioSummary(results: EnvelopeRunResult[]): Record<string, unknown> {
  const values = (selector: (result: EnvelopeRunResult) => number): { min: number; max: number; range: number } | null => {
    if (results.length === 0) return null;
    const selected = results.map(selector);
    const min = Math.min(...selected);
    const max = Math.max(...selected);
    return { min, max, range: max - min };
  };
  return {
    repeats: results.length,
    passingRuns: results.filter((result) => result.pass).length,
    allRunsPass: results.length === 3 && results.every((result) => result.pass),
    repeatsDetail: results,
    variance: {
      workloadEventsAttempted: values((result) => result.workloadEventsAttempted),
      workloadEventsDelivered: values((result) => result.workloadEventsDelivered),
      producerSchedulingLagMaxMs: values((result) => result.producerSchedulingLag.maxMs ?? 0),
      queueDepthAtProducerStop: values((result) => result.queueDepthAtProducerStop),
      queueHighWaterEvents: values((result) => result.queueHighWaterEvents),
      queueHighWaterBytes: values((result) => result.queueHighWaterBytes),
      transportDeliveryP99Ms: values((result) => result.transportDeliveryLatency.p99Ms ?? 0),
      snapshotCompletionP99Ms: values((result) => result.snapshotCompletionLatency.p99Ms ?? 0),
      eventLoopUtilization: values((result) => result.eventLoopUtilization),
      eventLoopDelayP99Ms: values((result) => result.eventLoopDelayP99Ms),
      fairnessMaxDriftMs: values((result) => result.fairnessProbeMaxDriftMs),
      processCpuMs: values((result) => result.processCpuMs),
      wallMs: values((result) => result.wallMs),
    },
  };
}

function bottleneckAttribution(results: EnvelopeRunResult[]): string {
  const first = results.find((result) => !result.pass);
  if (!first) return "No local bottleneck was exposed by the required production-shaped scenarios; this does not establish production safety.";
  if (first.workloadEventsAttempted < first.workloadEventsExpected || (first.producerSchedulingLag.maxMs ?? 0) > PRODUCER_SCHEDULING_TOLERANCE_MS) {
    return "Producer pacing/scheduling is the first failing boundary; transport and downstream attribution is not valid until intended source events are actually injected on schedule.";
  }
  if (first.workloadEventsAdmitted < first.workloadEventsAttempted || first.countLimitGaps > 0 || first.byteLimitGaps > 0) {
    return "Bounded ingress admission is the first failing boundary; this is queue/transport pressure, not evidence that downstream handlers caused the failure.";
  }
  if (first.workloadEventsDelivered < first.workloadEventsAdmitted || first.fifoViolations > 0 || first.duplicateEvents > 0) {
    return "Transport delivery/order is the first failing boundary.";
  }
  if (first.workloadNormalizedSnapshots < first.workloadNormalizedSnapshotsExpected) {
    return "Adapter normalization is the first failing boundary after complete source-event admission and delivery.";
  }
  if (first.drainTimedOut || first.queueFinalDepth > 0) {
    return "The real serial adapter snapshot callback / Engine hot path is the remaining failing boundary after complete transport delivery and normalization.";
  }
  return "A required integrity or readiness criterion failed after the primary counters matched; inspect the per-repeat record before attributing the next bottleneck.";
}

function markdownReport(report: Record<string, any>): string {
  const scenarioRows = (report.scenarios as Record<string, any>);
  const lines = [
    "# PR31 production-envelope falsification",
    "",
    "This is an experimental falsification report for the sole PR31 candidate: `maxDeliveryEventsPerSlice=128`, `maxDeliverySliceMs=0.5`. It is not a production scheduler selection, an OVH soak, a 1013 fix claim, or a live-trading validation.",
    "",
    `Decision: **${report.decision}**`,
    "",
    "| Scenario | Workload | Repeats passed | Required result |",
    "|---|---:|---:|---|",
    ...(["A", "B", "C"] as const).map((name) => {
      const value = scenarioRows[name];
      const first = value.repeatsDetail[0];
      const workload = `${first.workloadEventsExpected} source events`;
      return `| ${name} | ${workload} | ${value.passingRuns}/3 | ${value.allRunsPass ? "PASS" : "FAIL"} |`;
    }),
    "",
    `Bottleneck attribution: ${report.bottleneckAttribution}`,
    "",
    "## Scope classification",
    "",
    "- FULL REAL: BoundedClobMarketTransport, BoundedIngressQueue, candidate delivery scheduler, PolymarketMarketDataAdapter parsing/normalization, EngineService.onMarketSnapshot implementation, and TriggerEngine.ingestMarket implementation.",
    "- PARTIAL REAL: Engine lifecycle uses a memory journal and deterministic configured token metadata; the harness wraps the real snapshot handler only to measure completion and source correlation.",
    "- STUBBED: external reference/economic side effects are not required by the neutral, IDLE paper-mode workload; activation is the production no-op bridge.",
    "- NOT EXERCISED: live OVH network behavior, actual server-side WebSocket send-buffer thresholds, reconnect/initial_dump recovery, dynamic subscription, active triggers, Grok/network calls, resting orders, fills, risk, and execution.",
    "",
    "The source workload preserves approximately 98.1% `price_change`, 1.3% `book`, and 0.6% `last_trade_price`; every price-change source event carries two entries. Scenario C is explicitly synthetic safety margin above the observed Stage0 peak, not an observed production rate. Source events/sec and normalized snapshots/sec remain distinct units.",
    "",
    "### KNOWN FROM STAGE0",
    "",
    "Stage0 observed 1,713.75 expanded source events/sec average, approximately 3,641.33 source events/sec at the maximum sample interval, approximately 3,384.91 normalized snapshots/sec average, and 7,190.33 normalized snapshots/sec at that interval. The queue lifetime HWM was 104 with no ingress overflow, while 21 of 22 provider closes were 1013 slow-consumer closes. The server-side threshold is undocumented.",
    "",
    "### WHAT THIS HARNESS ACTUALLY EXERCISES",
    "",
    "A real in-process transport-to-Engine path with the PR31 candidate override, deterministic 40-market/80-token ordering, trusted initial books, the Stage0 event mix, two-entry price-change expansion, bounded real-time producer catch-up, exact source admission/delivery accounting, FIFO/per-token checks, queue bounds, event-loop/fairness/CPU measurements, and the real Engine/Trigger hot path.",
    "",
    "### WHAT IT DOES NOT EXERCISE",
    "",
    "It does not reproduce the remote Polymarket server, prove the cause of close code 1013, exercise reconnect initial dumps, or validate sharding, production persistence, reference feeds, triggers, risk, execution, or live trading.",
    "",
    "### RESULTS A / B / C",
    "",
  ];
  for (const name of ["A", "B", "C"] as const) {
    const value = scenarioRows[name];
    lines.push(`#### ${name}`);
    lines.push("");
    for (const result of value.repeatsDetail as EnvelopeRunResult[]) {
      lines.push(`- repeat ${result.repeat}: **${result.pass ? "PASS" : "FAIL"}**; attempted/admitted/delivered/processed=${result.workloadEventsAttempted}/${result.workloadEventsAdmitted}/${result.workloadEventsDelivered}/${result.workloadEventsProcessed}; normalized=${result.workloadNormalizedSnapshots}/${result.workloadNormalizedSnapshotsExpected}; queue HWM=${result.queueHighWaterEvents} events / ${result.queueHighWaterBytes} bytes; producer lag max=${result.producerSchedulingLag.maxMs ?? "n/a"} ms; drain=${result.drainCompletionMs ?? "timeout"} ms.`);
    }
    lines.push("");
    lines.push(`Variance: ${JSON.stringify(value.variance)}.`);
    lines.push("");
  }
  lines.push(
    "### BOTTLENECK ATTRIBUTION",
    "",
    String(report.bottleneckAttribution),
    "",
    "### FALSIFIED HYPOTHESES",
    "",
    report.decision === "PASS_TO_OVH_COMPARISON"
      ? "Within this local production-shaped envelope, the candidate did not fail through intended producer under-injection, bounded ingress overflow, transport reordering, normalization loss, or the measured serial Engine/Trigger path. This falsifies only those local failure modes for these scenarios; it does not falsify remote server send-buffer pressure."
      : "The candidate was not shown to tolerate every required local production-shaped scenario. No claim is made that the remote 1013 cause has been isolated until the first failing boundary is reviewed.",
    "",
    "### REMAINING HYPOTHESES",
    "",
    "Remote per-connection burst/send-buffer pressure, reconnect initial dumps, differences between the synthetic transport and the Polymarket server, and any production-only scheduling or persistence interaction remain unresolved.",
    "",
    "### DECISION",
    "",
    `**${report.decision}** — this result is local falsification evidence only. It does not select a production scheduler, claim to fix 1013, approve merge, approve deploy, or approve live trading.`,
    "",
  );
  return lines.join("\n");
}

export async function runProductionEnvelopeCalibration(): Promise<Record<string, unknown>> {
  const scenarios: Record<string, unknown> = {};
  const allResults: EnvelopeRunResult[] = [];
  for (const scenario of ENVELOPE_SCENARIOS) {
    const results: EnvelopeRunResult[] = [];
    for (let repeat = 1; repeat <= 3; repeat += 1) {
      const result = await runEnvelopeOnce({ scenario, repeat });
      results.push(result);
      allResults.push(result);
    }
    scenarios[scenario.name] = {
      name: scenario.name,
      description: scenario.description,
      observed: scenario.observed,
      syntheticSafetyMargin: scenario.syntheticSafetyMargin,
      segments: scenario.segments,
      ...scenarioSummary(results),
    };
  }
  const everyScenarioPassed = ["A", "B", "C"].every((name) => {
    const value = scenarios[name] as { allRunsPass: boolean };
    return value.allRunsPass;
  });
  const report = {
    report: "PR31 production-envelope falsification",
    generatedAt: new Date().toISOString(),
    candidate: PR31_CANDIDATE,
    queueBounds: { maxEvents: 4_096, maxBytes: 4 * 1024 * 1024 },
    tokenModel: { markets: MARKET_COUNT, tokens: TOKEN_COUNT, outcomesPerMarket: 2, ordering: "deterministic UP then DOWN" },
    sourceMix: { price_change: 0.981, book: 0.013, last_trade_price: 0.006, priceChangeEntriesPerPayload: 2 },
    producer: {
      maxCatchUpEventsPerTurn: MAX_CATCH_UP_EVENTS_PER_TURN,
      graceMs: PRODUCER_GRACE_MS,
      schedulingLagToleranceMs: PRODUCER_SCHEDULING_TOLERANCE_MS,
      pacing: "real-time due-time pacing with bounded catch-up; no giant synchronous catch-up batch",
    },
    stage0ObservedEnvelope: {
      durationSec: 901,
      rawFrames: 1_542_116,
      rawFramesPerSec: 1_711.56,
      expandedSourceEvents: 1_544_091,
      sourceEventsPerSec: 1_713.75,
      averageNormalizedSnapshotsPerSec: 3_384.91,
      observedPeakSourceEventsPerSec: 3_641.33,
      observedPeakNormalizedSnapshotsPerSec: 7_190.33,
      queueHighWaterEvents: 104,
      ingressOverflow: 0,
      provider1013Closes: 21,
    },
    scope: {
      fullReal: ["BoundedClobMarketTransport", "BoundedIngressQueue", "delivery scheduler", "PolymarketMarketDataAdapter", "EngineService.onMarketSnapshot", "TriggerEngine.ingestMarket"],
      partialReal: ["in-process deterministic WebSocket boundary", "memory journal", "configured token metadata", "measurement wrappers around real snapshot handler"],
      stubbed: ["external reference/economic side effects not required by neutral IDLE paper-mode workload", "NoopGrokActivationBridge"],
      notExercised: ["remote Polymarket server", "server-side send-buffer threshold", "reconnect initial_dump", "dynamic subscription", "active triggers", "Grok/network", "resting orders", "fills", "risk", "execution", "live trading"],
    },
    scenarios,
    bottleneckAttribution: bottleneckAttribution(allResults),
    decision: everyScenarioPassed ? "PASS_TO_OVH_COMPARISON" : "FAIL_ARCH_REVIEW",
    claimsNotMade: ["1013 fixed", "architecture fixed", "production scheduler selected", "safe for merge", "safe for deploy", "safe for live trading"],
  };
  return report;
}

export async function writeProductionEnvelopeReports(report: Record<string, unknown>): Promise<void> {
  const jsonPath = "reports/pr31-production-envelope.json";
  const markdownPath = "reports/pr31-production-envelope.md";
  await mkdir(dirname(jsonPath), { recursive: true });
  await writeFile(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  await writeFile(markdownPath, markdownReport(report as Record<string, any>), "utf8");
}

async function main(): Promise<void> {
  const report = await runProductionEnvelopeCalibration();
  await writeProductionEnvelopeReports(report);
  console.log(JSON.stringify({ decision: report.decision, jsonPath: "reports/pr31-production-envelope.json", markdownPath: "reports/pr31-production-envelope.md" }, null, 2));
}

const isMain = process.argv[1]?.replaceAll("\\", "/").endsWith("/pr31-production-envelope-harness.ts") === true;
if (isMain) await main();
