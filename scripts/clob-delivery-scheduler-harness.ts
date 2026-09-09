import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { monitorEventLoopDelay, performance } from "node:perf_hooks";
import {
  BoundedClobMarketTransport,
  PolymarketMarketDataAdapter,
  type BoundedMarketStream,
  type MarketStreamTransport,
} from "../packages/market-data/src/index.ts";

const TOKEN_COUNT = 80;
const TOKENS = Array.from({ length: TOKEN_COUNT }, (_, index) => `tok-${index}`);
const STEADY_RATES = [2_000, 4_000, 6_000, 8_000, 10_000] as const;
const POLICY_SWEEP = [
  { maxEvents: 1, maxSliceMs: 0.25 },
  { maxEvents: 1, maxSliceMs: 0.5 },
  { maxEvents: 1, maxSliceMs: 1 },
  { maxEvents: 1, maxSliceMs: 2 },
  { maxEvents: 4, maxSliceMs: 0.25 },
  { maxEvents: 4, maxSliceMs: 0.5 },
  { maxEvents: 4, maxSliceMs: 1 },
  { maxEvents: 4, maxSliceMs: 2 },
  { maxEvents: 8, maxSliceMs: 0.25 },
  { maxEvents: 8, maxSliceMs: 0.5 },
  { maxEvents: 8, maxSliceMs: 1 },
  { maxEvents: 8, maxSliceMs: 2 },
  { maxEvents: 16, maxSliceMs: 0.25 },
  { maxEvents: 16, maxSliceMs: 0.5 },
  { maxEvents: 16, maxSliceMs: 1 },
  { maxEvents: 16, maxSliceMs: 2 },
  { maxEvents: 32, maxSliceMs: 0.25 },
  { maxEvents: 32, maxSliceMs: 0.5 },
  { maxEvents: 32, maxSliceMs: 1 },
  { maxEvents: 32, maxSliceMs: 2 },
  { maxEvents: 64, maxSliceMs: 0.25 },
  { maxEvents: 64, maxSliceMs: 0.5 },
  { maxEvents: 64, maxSliceMs: 1 },
  { maxEvents: 64, maxSliceMs: 2 },
] as const;
const ADAPTER_POLICIES = [
  { maxEvents: 1, maxSliceMs: 1 },
  { maxEvents: 8, maxSliceMs: 0.5 },
  { maxEvents: 16, maxSliceMs: 1 },
  { maxEvents: 32, maxSliceMs: 1 },
  { maxEvents: 64, maxSliceMs: 1 },
] as const;

type HarnessMode = "direct" | "adapter";
type WorkloadKind =
  | "initial_book_burst"
  | "steady_book"
  | "steady_price_change_single"
  | "steady_price_change_multi"
  | "burst_above"
  | "partial_queue_burst";

interface Workload {
  kind: WorkloadKind;
  targetRate: number;
  durationMs: number;
  multiEntries: number;
  prefillEvents: number;
}

interface HarnessOptions {
  maxEvents: number;
  maxSliceMs: number;
  workload: Workload;
  mode: HarnessMode;
  durationMs: number;
}

interface SequenceTag {
  sequence: number;
  sentAtMs: number;
}

interface LatencySummary {
  count: number;
  p50Ms: number | null;
  p95Ms: number | null;
  p99Ms: number | null;
  maxMs: number | null;
}

interface HarnessRunResult {
  mode: HarnessMode;
  maxEvents: number;
  maxSliceMs: number;
  workload: WorkloadKind;
  targetEventRate: number;
  workloadDurationMs: number;
  sourceFramesSent: number;
  sourceEventsAttempted: number;
  sourceEventsAdmitted: number;
  sourceEventsRejected: number;
  eventsDelivered: number;
  deliveredPerSec: number;
  snapshotsProduced: number;
  queueFinalDepth: number;
  queueHighWaterEvents: number;
  queueHighWaterBytes: number;
  countLimitGaps: number;
  byteLimitGaps: number;
  fifoViolations: number;
  lostEvents: number;
  undeliveredAtObservationEnd: number;
  duplicateEvents: number;
  latency: LatencySummary;
  eventLoopUtilization: number;
  eventLoopDelayP99Ms: number;
  eventLoopDelayMaxMs: number;
  processCpuMs: number;
  wallMs: number;
  fairnessProbeCallbacks: number;
  fairnessProbeCallbacksWhileBacklogged: number;
  fairnessProbeMaxDriftMs: number;
  drainCompletionMs: number | null;
  drainTimedOut: boolean;
  streamTerminalError: string | null;
  adapterErrors: number;
  adapterObservedConnections: number;
  notes: string[];
}

class HarnessWebSocket extends EventTarget {
  static instances: HarnessWebSocket[] = [];
  readyState = 0;

  constructor(readonly url: string) {
    super();
    HarnessWebSocket.instances.push(this);
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

class BoundedLatencySample {
  private readonly values = new Float64Array(4_096);
  private count = 0;
  private randomState = 0x9e3779b9;
  private maximum: number | null = null;

  add(value: number): void {
    if (!Number.isFinite(value) || value < 0) return;
    this.count += 1;
    if (this.maximum === null || value > this.maximum) this.maximum = value;
    if (this.count <= this.values.length) {
      this.values[this.count - 1] = value;
      return;
    }
    this.randomState = (Math.imul(this.randomState, 1_664_525) + 1_013_904_223) >>> 0;
    const replacement = this.randomState % this.count;
    if (replacement < this.values.length) this.values[replacement] = value;
  }

  summary(): LatencySummary {
    if (this.count === 0) return { count: 0, p50Ms: null, p95Ms: null, p99Ms: null, maxMs: null };
    const size = Math.min(this.count, this.values.length);
    const sorted = Array.from(this.values.subarray(0, size)).sort((left, right) => left - right);
    const quantile = (fraction: number) => sorted[Math.min(size - 1, Math.floor((size - 1) * fraction))]!;
    return { count: this.count, p50Ms: quantile(0.5), p95Ms: quantile(0.95), p99Ms: quantile(0.99), maxMs: this.maximum };
  }
}

class BoundedSequenceTracker {
  private readonly seen: Uint8Array;
  private lastSequence = 0;
  unique = 0;
  duplicates = 0;
  fifoViolations = 0;

  constructor(maxSequence: number) {
    this.seen = new Uint8Array(Math.ceil((maxSequence + 1) / 8));
  }

  add(sequence: number): void {
    if (!Number.isInteger(sequence) || sequence < 1) {
      this.fifoViolations += 1;
      return;
    }
    const byte = sequence >> 3;
    const bit = 1 << (sequence & 7);
    if (byte >= this.seen.length) {
      this.fifoViolations += 1;
      return;
    }
    if ((this.seen[byte]! & bit) !== 0) {
      this.duplicates += 1;
      return;
    }
    this.seen[byte] = (this.seen[byte] ?? 0) | bit;
    this.unique += 1;
    if (sequence !== this.lastSequence + 1) this.fifoViolations += 1;
    this.lastSequence = sequence;
  }
}

function parseNumber(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function getArg(name: string): string | undefined {
  const prefix = `--${name}=`;
  return process.argv.find((argument) => argument.startsWith(prefix))?.slice(prefix.length);
}

function parseMode(): "direct" | "adapter" | "both" {
  const value = getArg("mode") ?? "both";
  if (value === "direct" || value === "adapter" || value === "both") return value;
  throw new Error(`Unsupported --mode=${value}`);
}

function streamEventTag(event: unknown): SequenceTag | null {
  const value = event as { type?: string; payload?: Record<string, unknown> };
  const payload = value.payload ?? {};
  let hash = payload.hash;
  if (value.type === "price_change" && Array.isArray(payload.priceChanges)) {
    hash = (payload.priceChanges[0] as Record<string, unknown> | undefined)?.hash;
  }
  const match = typeof hash === "string" ? /^pr30b-(\d+)-([0-9]+(?:\.[0-9]+)?)$/.exec(hash) : null;
  if (!match) return null;
  return { sequence: Number(match[1]), sentAtMs: Number(match[2]) };
}

function snapshotHashTag(sequence: number, sentAtMs: number): string {
  return `pr30b-${sequence}-${sentAtMs.toFixed(3)}`;
}

function bookFrame(sequence: number, tokenId: string, sentAtMs: number): string {
  return JSON.stringify({
    event_type: "book",
    market: "mkt",
    asset_id: tokenId,
    bids: [{ price: "0.50", size: "1" }],
    asks: [{ price: "0.51", size: "1" }],
    hash: snapshotHashTag(sequence, sentAtMs),
    timestamp: Date.now() + sequence,
  });
}

function priceChangeFrame(sequence: number, tokenIds: readonly string[], sentAtMs: number): string {
  const tag = snapshotHashTag(sequence, sentAtMs);
  return JSON.stringify({
    event_type: "price_change",
    market: "mkt",
    price_changes: tokenIds.map((tokenId) => ({
      asset_id: tokenId,
      price: "0.50",
      size: "1",
      side: "BUY",
      hash: tag,
    })),
    timestamp: Date.now() + sequence,
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
    subscribe: async (tokenIds, customFeatureEnabled, signal) => {
      const stream = await transport.subscribe(tokenIds, customFeatureEnabled, signal);
      return observedStream(stream, onEvent);
    },
  };
}

async function waitFor(predicate: () => boolean, timeoutMs = 5_000): Promise<void> {
  const started = performance.now();
  while (!predicate()) {
    if (performance.now() - started > timeoutMs) throw new Error("harness waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}

function createWorkload(kind: WorkloadKind, durationMs: number, targetRate = 6_000): Workload {
  switch (kind) {
    case "initial_book_burst": return { kind, targetRate: 0, durationMs: 0, multiEntries: 1, prefillEvents: 0 };
    case "burst_above": return { kind, targetRate: 12_000, durationMs: Math.min(150, durationMs), multiEntries: 1, prefillEvents: 0 };
    case "partial_queue_burst": return { kind, targetRate, durationMs: Math.min(150, durationMs), multiEntries: 1, prefillEvents: 512 };
    case "steady_price_change_multi": return { kind, targetRate, durationMs, multiEntries: 4, prefillEvents: 0 };
    default: return { kind, targetRate, durationMs, multiEntries: 1, prefillEvents: 0 };
  }
}

function representativeWorkloads(durationMs: number): Workload[] {
  return [
    createWorkload("initial_book_burst", durationMs),
    createWorkload("steady_price_change_single", durationMs),
    createWorkload("steady_price_change_multi", durationMs),
    createWorkload("burst_above", durationMs),
    createWorkload("partial_queue_burst", durationMs),
  ];
}

function directSteadyWorkloads(durationMs: number): Workload[] {
  return STEADY_RATES.map((rate) => createWorkload("steady_book", durationMs, rate));
}

function expectedSequenceCapacity(workload: Workload): number {
  return TOKEN_COUNT + workload.prefillEvents + Math.ceil((workload.targetRate * Math.max(1, workload.durationMs)) / 1_000) + 2_048;
}

async function runOnce(options: HarnessOptions): Promise<HarnessRunResult> {
  HarnessWebSocket.instances = [];
  const baseTransport = new BoundedClobMarketTransport({
    webSocket: HarnessWebSocket as unknown as typeof WebSocket,
    maxDeliveryEventsPerSlice: options.maxEvents,
    maxDeliverySliceMs: options.maxSliceMs,
    pingIntervalMs: 60_000,
    staleMs: 120_000,
    connectTimeoutMs: 2_000,
  });
  const sequenceTracker = new BoundedSequenceTracker(expectedSequenceCapacity(options.workload));
  const latency = new BoundedLatencySample();
  let sourceFramesSent = 0;
  let sourceEventsAttempted = 0;
  let snapshotsProduced = 0;
  let countLimitGaps = 0;
  let byteLimitGaps = 0;
  let streamTerminalError: string | null = null;
  let adapterErrors = 0;
  let adapterObservedConnections = 0;
  let nextSequence = 1;
  const onTransportEvent = (event: unknown) => {
    const tag = streamEventTag(event);
    if (!tag) return;
    sequenceTracker.add(tag.sequence);
    latency.add(Math.max(0, performance.now() - tag.sentAtMs));
  };
  const transport = options.mode === "adapter" ? observedTransport(baseTransport, onTransportEvent) : baseTransport;
  const socketPending = options.mode === "adapter"
    ? null
    : baseTransport.subscribe(TOKENS, true);

  let stream: BoundedMarketStream | null = null;
  let directConsumer: Promise<void> | null = null;
  let adapterAbort: AbortController | null = null;
  let adapterRun: Promise<void> | null = null;
  let connectionState = "";

  if (options.mode === "direct") {
    await waitFor(() => HarnessWebSocket.instances.length === 1);
    const socket = HarnessWebSocket.instances[0]!;
    socket.open();
    stream = observedStream(await socketPending!, onTransportEvent);
  } else {
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: baseTransport.queue });
    for (const [index, tokenId] of TOKENS.entries()) {
      adapter.registerManualToken(tokenId, "mkt", index % 2 === 0 ? "UP" : "DOWN", "BTC");
    }
    adapter.setTokenIds(TOKENS);
    adapterAbort = new AbortController();
    adapterRun = adapter.run(TOKENS, {
      onSnapshot: async () => {
        snapshotsProduced += 1;
        await Promise.resolve();
      },
      onConnection: (event) => {
        connectionState = event.state;
        if (event.state === "CONNECTED") adapterObservedConnections += 1;
      },
      onError: () => { adapterErrors += 1; },
      onGap: (event) => {
        if (event.reason === "count_limit") countLimitGaps += 1;
        if (event.reason === "byte_limit") byteLimitGaps += 1;
      },
    }, adapterAbort.signal);
    await waitFor(() => HarnessWebSocket.instances.length === 1);
    HarnessWebSocket.instances[0]!.open();
    await waitFor(() => connectionState === "CONNECTED");
  }

  const socket = HarnessWebSocket.instances[0]!;
  const initialQueueAccepted = baseTransport.queue.accepted;
  const initialQueueProcessed = baseTransport.queue.processed;
  const startElu = performance.eventLoopUtilization();
  const delay = monitorEventLoopDelay({ resolution: 10 });
  delay.enable();
  const cpuStart = process.cpuUsage();
  const wallStart = performance.now();
  const fairnessIntervalMs = 10;
  let fairnessCallbacks = 0;
  let fairnessWhileBacklogged = 0;
  let fairnessMaxDriftMs = 0;
  let nextFairnessAt = wallStart + fairnessIntervalMs;
  const fairnessTimer = setInterval(() => {
    const now = performance.now();
    fairnessCallbacks += 1;
    if (baseTransport.queue.depth > 0) fairnessWhileBacklogged += 1;
    fairnessMaxDriftMs = Math.max(fairnessMaxDriftMs, Math.max(0, now - nextFairnessAt));
    nextFairnessAt += fairnessIntervalMs;
  }, fairnessIntervalMs);
  fairnessTimer.unref?.();

  const send = (raw: string, eventCount: number) => {
    if (socket.readyState !== 1) return;
    sourceFramesSent += 1;
    sourceEventsAttempted += eventCount;
    socket.message(raw);
  };
  const sendBook = (tokenIndex: number) => {
    const sentAtMs = performance.now();
    const sequence = nextSequence++;
    send(bookFrame(sequence, TOKENS[tokenIndex % TOKEN_COUNT]!, sentAtMs), 1);
  };
  const sendPriceChange = (entries: number) => {
    const sentAtMs = performance.now();
    const sequence = nextSequence++;
    const startToken = sequence % TOKEN_COUNT;
    const tokenIds = Array.from({ length: entries }, (_, offset) => TOKENS[(startToken + offset) % TOKEN_COUNT]!);
    send(priceChangeFrame(sequence, tokenIds, sentAtMs), 1);
  };

  const sendInitialBooks = () => {
    for (let index = 0; index < TOKEN_COUNT; index += 1) sendBook(index);
  };

  const startDirectConsumer = () => {
    if (directConsumer || options.mode !== "direct") return;
    const directStream = stream!;
    const iterator = directStream[Symbol.asyncIterator]();
    directConsumer = (async () => {
      try {
        while (true) {
          const result = await iterator.next();
          if (result.done) return;
        }
      } catch (error) {
        const candidate = error as { reason?: string; message?: string };
        streamTerminalError = candidate.message ?? String(error);
        if (candidate.reason === "count_limit") countLimitGaps += 1;
        if (candidate.reason === "byte_limit") byteLimitGaps += 1;
      }
    })();
  };

  if (options.mode === "direct" && options.workload.kind !== "partial_queue_burst") {
    startDirectConsumer();
  }

  const preloadBeforeConsumer = options.workload.kind === "partial_queue_burst" && options.mode === "direct";
  if (preloadBeforeConsumer) {
    sendInitialBooks();
    for (let index = 0; index < options.workload.prefillEvents; index += 1) sendBook(index);
    startDirectConsumer();
  } else {
    sendInitialBooks();
    if (options.workload.kind === "partial_queue_burst") {
      for (let index = 0; index < options.workload.prefillEvents; index += 1) sendBook(index);
    }
  }

  if (options.workload.kind === "initial_book_burst") {
    // The initial book burst is the workload; no extra steady producer is used.
  } else {
    const workloadStarted = performance.now();
    const workloadDuration = options.workload.durationMs;
    await new Promise<void>((resolve) => {
      const timer = setInterval(() => {
        const elapsed = performance.now() - workloadStarted;
        const desired = Math.floor(Math.min(elapsed, workloadDuration) * options.workload.targetRate / 1_000);
        const prefillApplied = options.workload.kind === "partial_queue_burst" ? options.workload.prefillEvents : 0;
        const workloadEventsSent = nextSequence - 1 - TOKEN_COUNT - prefillApplied;
        const toSend = Math.min(512, Math.max(0, desired - workloadEventsSent));
        for (let i = 0; i < toSend; i += 1) {
          if (options.workload.kind === "steady_price_change_single") sendPriceChange(1);
          else if (options.workload.kind === "steady_price_change_multi") sendPriceChange(options.workload.multiEntries);
          else sendBook(nextSequence);
        }
        if (elapsed >= workloadDuration) {
          clearInterval(timer);
          resolve();
        }
      }, 1);
      timer.unref?.();
    });
  }

  const admittedAtProducerStop = baseTransport.queue.accepted - initialQueueAccepted;
  const drainStartedAt = performance.now();
  let drainCompletionMs: number | null = null;
  const drainDeadline = drainStartedAt + Math.max(1_000, Math.min(10_000, options.durationMs * 8 + 1_000));
  while (performance.now() < drainDeadline) {
    if (baseTransport.queue.depth === 0 && baseTransport.queue.processed - initialQueueProcessed >= admittedAtProducerStop) {
      drainCompletionMs = performance.now() - drainStartedAt;
      break;
    }
    if (socket.readyState === 3 && baseTransport.queue.depth > 0) break;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }

  clearInterval(fairnessTimer);
  const wallMs = performance.now() - wallStart;
  const elu = performance.eventLoopUtilization(startElu);
  const cpu = process.cpuUsage(cpuStart);
  const snapshot = baseTransport.queue.snapshot();
  const sourceEventsAdmitted = snapshot.accepted - initialQueueAccepted;
  const eventsDelivered = baseTransport.queue.processed - initialQueueProcessed;
  const undeliveredAtObservationEnd = Math.max(0, sourceEventsAdmitted - sequenceTracker.unique);
  const terminalizedWithQueuedEvents = (streamTerminalError !== null || countLimitGaps > 0 || byteLimitGaps > 0)
    && snapshot.depth > 0;
  if (options.mode === "direct") {
    await stream!.close();
    await directConsumer;
  } else {
    adapterAbort!.abort("harness_complete");
    await adapterRun;
  }
  delay.disable();

  const result: HarnessRunResult = {
    mode: options.mode,
    maxEvents: options.maxEvents,
    maxSliceMs: options.maxSliceMs,
    workload: options.workload.kind,
    targetEventRate: options.workload.targetRate,
    workloadDurationMs: options.workload.durationMs,
    sourceFramesSent,
    sourceEventsAttempted,
    sourceEventsAdmitted,
    sourceEventsRejected: Math.max(0, sourceEventsAttempted - sourceEventsAdmitted),
    eventsDelivered,
    deliveredPerSec: eventsDelivered / Math.max(0.001, options.workload.durationMs / 1_000),
    snapshotsProduced,
    queueFinalDepth: snapshot.depth,
    queueHighWaterEvents: snapshot.highWaterEvents,
    queueHighWaterBytes: snapshot.highWaterBytes,
    countLimitGaps,
    byteLimitGaps,
    fifoViolations: sequenceTracker.fifoViolations,
    lostEvents: terminalizedWithQueuedEvents ? undeliveredAtObservationEnd : 0,
    undeliveredAtObservationEnd,
    duplicateEvents: sequenceTracker.duplicates,
    latency: latency.summary(),
    eventLoopUtilization: elu.utilization,
    eventLoopDelayP99Ms: delay.percentile(99) / 1e6,
    eventLoopDelayMaxMs: delay.max / 1e6,
    processCpuMs: (cpu.user + cpu.system) / 1_000,
    wallMs,
    fairnessProbeCallbacks: fairnessCallbacks,
    fairnessProbeCallbacksWhileBacklogged: fairnessWhileBacklogged,
    fairnessProbeMaxDriftMs: fairnessMaxDriftMs,
    drainCompletionMs,
    drainTimedOut: drainCompletionMs === null,
    streamTerminalError,
    adapterErrors,
    adapterObservedConnections,
    notes: [
      ...(preloadBeforeConsumer ? ["partial_queue_prefill was applied before the direct consumer; adapter mode applies it after connection"] : []),
      "fairnessProbe is local timer progress only and is not proof of provider socket fairness",
      ...(admittedAtProducerStop !== eventsDelivered ? ["producer stop snapshot preceded complete drain"] : []),
    ],
  };
  return result;
}

function resultPassesSixK(result: HarnessRunResult): boolean {
  return result.mode === "direct"
    && result.workload === "steady_book"
    && result.targetEventRate === 6_000
    && result.countLimitGaps === 0
    && result.byteLimitGaps === 0
    && result.sourceEventsAdmitted === result.eventsDelivered
    && result.lostEvents === 0
    && result.duplicateEvents === 0
    && result.fifoViolations === 0
    && result.fairnessProbeCallbacksWhileBacklogged > 0
    && !result.drainTimedOut;
}

async function main(): Promise<void> {
  const mode = parseMode();
  const durationMs = Math.max(50, parseNumber(getArg("duration-ms"), 250));
  const runLimit = Math.max(0, Math.floor(parseNumber(getArg("limit"), Number.POSITIVE_INFINITY)));
  const output = getArg("output") ?? "reports/pr30b-clob-delivery-scheduler.json";
  const plans: HarnessOptions[] = [];
  if (mode === "direct" || mode === "both") {
    for (const policy of POLICY_SWEEP) {
      for (const workload of directSteadyWorkloads(durationMs)) plans.push({ ...policy, workload, mode: "direct", durationMs });
    }
    for (const policy of [POLICY_SWEEP[0]!, POLICY_SWEEP[10]!, POLICY_SWEEP[14]!, POLICY_SWEEP[18]!, POLICY_SWEEP[22]!, POLICY_SWEEP[23]!]) {
      for (const workload of representativeWorkloads(durationMs)) plans.push({ ...policy, workload, mode: "direct", durationMs });
    }
  }
  if (mode === "adapter" || mode === "both") {
    for (const policy of ADAPTER_POLICIES) {
      for (const workload of representativeWorkloads(durationMs)) plans.push({ ...policy, workload, mode: "adapter", durationMs });
    }
  }

  const selectedPlans = Number.isFinite(runLimit) ? plans.slice(0, runLimit) : plans;
  const results: HarnessRunResult[] = [];
  for (const [index, plan] of selectedPlans.entries()) {
    const result = await runOnce(plan);
    results.push(result);
    console.log(`${index + 1}/${selectedPlans.length} ${plan.mode} ${plan.maxEvents}/${plan.maxSliceMs} ${plan.workload.kind}@${plan.workload.targetRate}: delivered=${result.eventsDelivered} admitted=${result.sourceEventsAdmitted} gaps=${result.countLimitGaps}/${result.byteLimitGaps} hwm=${result.queueHighWaterEvents}`);
  }

  const candidateSummary = POLICY_SWEEP.map((policy) => {
    const result = results.find((candidate) => candidate.mode === "direct"
      && candidate.workload === "steady_book"
      && candidate.targetEventRate === 6_000
      && candidate.maxEvents === policy.maxEvents
      && candidate.maxSliceMs === policy.maxSliceMs);
    return {
      ...policy,
      sixKPass: result ? resultPassesSixK(result) : false,
      zeroCountLimit: result ? result.countLimitGaps === 0 : false,
      zeroByteLimit: result ? result.byteLimitGaps === 0 : false,
      everyAdmittedDeliveredExactlyOnce: result
        ? result.sourceEventsAdmitted === result.eventsDelivered
          && result.lostEvents === 0
          && result.duplicateEvents === 0
        : false,
      fifoPreserved: result ? result.fifoViolations === 0 : false,
      fairnessProbeProgress: result ? result.fairnessProbeCallbacksWhileBacklogged > 0 : false,
      drainCompleted: result ? !result.drainTimedOut : false,
      queueHighWaterEvents: result?.queueHighWaterEvents ?? null,
      queueHighWaterBytes: result?.queueHighWaterBytes ?? null,
      eventLoopUtilization: result?.eventLoopUtilization ?? null,
      eventLoopDelayP99Ms: result?.eventLoopDelayP99Ms ?? null,
      eventLoopDelayMaxMs: result?.eventLoopDelayMaxMs ?? null,
      fairnessProbeCallbacksWhileBacklogged: result?.fairnessProbeCallbacksWhileBacklogged ?? null,
      fairnessProbeMaxDriftMs: result?.fairnessProbeMaxDriftMs ?? null,
      drainCompletionMs: result?.drainCompletionMs ?? null,
    };
  });
  const report = {
    generatedAt: new Date().toISOString(),
    defaultsPreserved: {
      maxDeliveryEventsPerSlice: 1,
      maxDeliverySliceMs: 1,
      note: "All matrix values are explicit calibration overrides; no production default is selected by this harness.",
    },
    queueBounds: { maxEvents: 4_096, maxBytes: 4 * 1024 * 1024, maxRawFrameBytes: 256 * 1024 },
    workloadPlan: {
      steadyRates: STEADY_RATES,
      policySweep: POLICY_SWEEP,
      adapterPolicies: ADAPTER_POLICIES,
      directSteadyRuns: POLICY_SWEEP.length * STEADY_RATES.length,
      representativeWorkloads: representativeWorkloads(durationMs).map((workload) => workload.kind),
      durationMs,
    },
    candidateSummary,
    results,
    interpretation: {
      sixKPassDefinition: "zero count/byte gaps, every admitted source event delivered exactly once, FIFO, fairness progress, and drain completion",
      fairnessLimitation: "The local periodic timer detects event-loop starvation only; OVH production soak remains the network fairness proof.",
      sustainedHarnessLimitation: "scripts/sustained-capacity-harness.ts is not used as scheduler-capacity evidence because it uses ScriptedBoundedMarketTransport.",
      recommendation: null,
    },
  };
  await mkdir(dirname(output), { recursive: true });
  await writeFile(output, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  console.log(`raw report: ${output}`);
  console.log(`six-k candidates: ${candidateSummary.filter((candidate) => candidate.sixKPass).map((candidate) => `${candidate.maxEvents}/${candidate.maxSliceMs}`).join(", ") || "none"}`);
}

await main();
