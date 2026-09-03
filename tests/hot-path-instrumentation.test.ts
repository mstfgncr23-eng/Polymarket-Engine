import { describe, expect, it } from "vitest";
import {
  TriggerEngine,
  createId,
  executePaperTaker,
  triggerConfigSchema,
  triggerKindSchema,
  type TriggerEvent,
} from "@pm/core";
import { MemoryEventJournal, createJournalEvent } from "@pm/db";
import { PolymarketMarketDataAdapter } from "@pm/market-data";
import {
  EngineMetrics,
  FORBIDDEN_METRIC_LABEL_NAMES,
  JOURNAL_METRIC_CLASSES,
  TRIGGER_KIND_VALUES,
  TRIGGER_SEVERITY_VALUES,
  classifyJournalMetricClass,
} from "../apps/engine/src/metrics.js";
import { BoundedJournalWriter } from "../apps/engine/src/journal-writer.js";
import { SseHub } from "../apps/engine/src/server.js";
import { EngineService } from "../apps/engine/src/engine.js";
import {
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
  discoveryReport,
  qualified,
  qualifiedPaperEnv,
  startQualifiedEngine,
} from "./paper-engine-testkit.js";
import { order, snapshot } from "./fixtures.js";

async function metricValue(metrics: EngineMetrics, name: string, labels?: Record<string, string>): Promise<number> {
  const item = metrics.registry.getSingleMetric(name);
  if (!item) return 0;
  const data = await item.get();
  const points = data.values.filter((point) => {
    if (point.metricName?.endsWith("_bucket") || point.metricName?.endsWith("_sum")) return false;
    if (labels) return Object.entries(labels).every(([key, value]) => point.labels[key] === value);
    return true;
  });
  if (labels) return points[0]?.value ?? 0;
  return points.reduce((sum, point) => {
    if (point.metricName?.endsWith("_count")) return sum + point.value;
    if (!point.metricName || point.metricName === name) return sum + point.value;
    return sum;
  }, 0);
}

async function histogramCount(metrics: EngineMetrics, name: string): Promise<number> {
  const item = metrics.registry.getSingleMetric(name);
  if (!item) return 0;
  const data = await item.get();
  return data.values.filter((point) => point.metricName === `${name}_count`).reduce((sum, point) => sum + point.value, 0);
}

function expositionLabelNames(text: string): string[] {
  const names = new Set<string>();
  for (const line of text.split("\n")) {
    if (line.startsWith("#") || !line.includes("{")) continue;
    const match = /\{([^}]+)\}/.exec(line);
    if (!match?.[1]) continue;
    for (const part of match[1].split(",")) {
      const key = part.split("=")[0]?.trim();
      if (key) names.add(key);
    }
  }
  return [...names];
}

function expositionLabelValues(text: string, label: string): string[] {
  const values = new Set<string>();
  const re = new RegExp(`${label}="([^"]*)"`, "g");
  for (const match of text.matchAll(re)) {
    if (match[1] !== undefined) values.add(match[1]);
  }
  return [...values];
}

function researchTrigger(overrides: Partial<TriggerEvent> = {}): TriggerEvent {
  const traceId = createId("trace");
  return {
    id: createId("trigger"), traceId, marketId: "btc-mkt", asset: "BTC", source: "polymarket",
    kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: "x", evidence: {},
    sourceEventIds: ["e1"], evidenceIds: ["e1"],
    createdAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 30_000).toISOString(),
    ...overrides,
  };
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 1_500): Promise<void> {
  const started = Date.now();
  while (!(await predicate())) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

class ControllableMarketStream {
  private queue: unknown[] = [];
  private waiters: Array<(result: IteratorResult<unknown>) => void> = [];
  closed = false;
  push(event: unknown): void {
    const waiter = this.waiters.shift();
    if (waiter) waiter({ value: event, done: false });
    else this.queue.push(event);
  }
  async close(): Promise<void> {
    this.closed = true;
    const waiter = this.waiters.shift();
    if (waiter) waiter({ value: undefined, done: true });
  }
  async *[Symbol.asyncIterator](): AsyncGenerator<unknown> {
    while (true) {
      if (this.queue.length > 0) {
        yield this.queue.shift();
        continue;
      }
      if (this.closed) return;
      const result = await new Promise<IteratorResult<unknown>>((resolve) => this.waiters.push(resolve));
      if (result.done) return;
      yield result.value;
    }
  }
}

class FakePolymarketClient {
  readonly stream = new ControllableMarketStream();
  async subscribe(): Promise<ControllableMarketStream> { return this.stream; }
  async closeSubscriptions(): Promise<void> {}
}

class DelayedDiscoveryAdapter extends ScriptedMarketAdapter {
  hold: Promise<void> = Promise.resolve();
  inFlight = 0;
  completed = 0;
  async discoverCryptoMarkets() {
    this.inFlight += 1;
    await this.hold;
    this.inFlight -= 1;
    this.completed += 1;
    return super.discoverCryptoMarkets();
  }
}

describe("hot-path instrumentation labels", () => {
  it("keeps metric labels bounded and never uses market/token/trace/slug identifiers", async () => {
    const metrics = new EngineMetrics();
    metrics.triggers.inc({ kind: "BOOK_IMBALANCE", severity: "2" });
    metrics.triggersEmitted.inc({ kind: "BOOK_IMBALANCE", severity: "2" });
    metrics.triggersSuppressed.inc({ kind: "SPREAD_CHANGE", severity: "1" });
    metrics.journalDirectWrites.inc({ class: "trigger" });
    metrics.journalDirectFailures.inc({ class: "system" });
    metrics.journalDirectLatency.observe({ class: "activation" }, 0.01);
    metrics.wsReconnects.inc({ feed: "polymarket" });
    metrics.referenceCloses.inc({ kind: "network_error" });
    metrics.activations.inc({ provider: "noop", status: "ok" });
    metrics.grokSignals.inc({ agent: "fair_probability_analyst", status: "accepted" });
    metrics.mcpInteractions.inc({ status: "handled", method: "GET" });
    metrics.paperOrders.inc({ type: "TAKER" });
    metrics.paperFills.inc({ role: "TAKER" });
    metrics.riskVetoes.inc({ reason: "MAX_ORDER_SIZE" });

    const text = await metrics.registry.metrics();
    const labelNames = expositionLabelNames(text);
    for (const forbidden of FORBIDDEN_METRIC_LABEL_NAMES) {
      expect(labelNames, forbidden).not.toContain(forbidden);
    }
    expect(text).not.toMatch(/marketId=/);
    expect(text).not.toMatch(/tokenId=/);
    expect(text).not.toMatch(/traceId=/);
    expect(text).not.toMatch(/slug=/);

    for (const kind of expositionLabelValues(text, "kind")) {
      const allowed = new Set<string>([
        ...TRIGGER_KIND_VALUES,
        "network_error", "operator_shutdown", "asset_unneeded", "clean_remote_close", "stale_feed", "websocket_error", "unknown",
      ]);
      expect(allowed.has(kind), kind).toBe(true);
    }
    for (const severity of expositionLabelValues(text, "severity")) {
      expect(TRIGGER_SEVERITY_VALUES.includes(severity as typeof TRIGGER_SEVERITY_VALUES[number])).toBe(true);
    }
    for (const journalClass of expositionLabelValues(text, "class")) {
      expect(JOURNAL_METRIC_CLASSES.includes(journalClass as typeof JOURNAL_METRIC_CLASSES[number])).toBe(true);
    }
    expect(classifyJournalMetricClass("TRIGGER_EVENT")).toBe("trigger");
    expect(classifyJournalMetricClass("GROK_ACTIVATION_DECISION")).toBe("activation");
    expect(classifyJournalMetricClass("CONSENSUS_DECISION")).toBe("consensus");
    expect(classifyJournalMetricClass("PAPER_FILL")).toBe("execution");
    expect(classifyJournalMetricClass("DISCOVERY_RECONCILED")).toBe("system");
    expect(triggerKindSchema.options.length).toBeLessThanOrEqual(32);
  });

  it("exposes existing prom-client process/event-loop defaults without duplicating lag", async () => {
    const text = await new EngineMetrics().registry.metrics();
    expect(text).toMatch(/polymarket_nodejs_eventloop_lag_seconds/);
    expect(text).toMatch(/polymarket_process_cpu_user_seconds_total/);
    expect(text).toMatch(/polymarket_process_resident_memory_bytes/);
    expect(text).toMatch(/nodejs_eventloop_utilization/);
    expect(text.match(/nodejs_eventloop_utilization/g)?.length).toBeLessThanOrEqual(4);
  });
});

describe("SSE instrumentation does not change snapshot behavior", () => {
  it("increments and decrements the subscriber gauge", async () => {
    const metrics = new EngineMetrics();
    const hub = new SseHub(20, metrics);
    expect(await metricValue(metrics, "sse_subscribers")).toBe(0);
    const removeA = hub.add({ write: () => true, end: () => undefined });
    const removeB = hub.add({ write: () => true, end: () => undefined });
    expect(await metricValue(metrics, "sse_subscribers")).toBe(2);
    removeA();
    expect(await metricValue(metrics, "sse_subscribers")).toBe(1);
    removeB();
    expect(await metricValue(metrics, "sse_subscribers")).toBe(0);
  });

  it("counts zero-subscriber snapshot scheduling but still builds and serializes", async () => {
    const metrics = new EngineMetrics();
    const hub = new SseHub(20, metrics);
    let built = 0;
    hub.publish("snapshot", () => {
      built += 1;
      return { paperMode: true, queueDepth: 0 };
    });
    expect(await metricValue(metrics, "sse_snapshots_scheduled_zero_subscribers_total")).toBe(1);
    await waitFor(() => built === 1);
    expect(built).toBe(1);
    expect(await metricValue(metrics, "sse_snapshot_broadcasts_total")).toBe(1);
    expect(await histogramCount(metrics, "sse_snapshot_build_duration_seconds")).toBe(1);
    expect(await histogramCount(metrics, "sse_snapshot_serialize_duration_seconds")).toBe(1);
    expect(await histogramCount(metrics, "sse_snapshot_payload_bytes")).toBe(1);
  });

  it("counts write(false) backpressure without changing the frame or handling it", async () => {
    const metrics = new EngineMetrics();
    const hub = new SseHub(20, metrics);
    const writes: string[] = [];
    hub.add({
      write: (chunk) => {
        writes.push(chunk);
        return false;
      },
      end: () => undefined,
    });
    hub.publish("decision", { type: "TRIGGER_LEVEL_2", marketId: "must-not-become-a-label" });
    expect(writes).toHaveLength(1);
    expect(writes[0]).toMatch(/^id: 1\nevent: decision\ndata: /);
    expect(writes[0]).toContain("must-not-become-a-label");
    expect(await metricValue(metrics, "sse_subscriber_backpressure_total")).toBe(1);
    expect(await metrics.registry.metrics()).not.toMatch(/marketId="/);
  });
});

describe("processTrigger and journal in-flight gauges return to zero", () => {
  it("returns processTrigger in-flight to zero after success and after journal failure", async () => {
    const journal = new MemoryEventJournal();
    let gate: Promise<void> = Promise.resolve();
    const original = journal.append.bind(journal);
    journal.append = async (event) => {
      if (event.type === "TRIGGER_EVENT") await gate;
      return original(event);
    };
    const env = qualifiedPaperEnv({ GROK_ACTIVATION_PROVIDER: "noop" });
    const market = new ScriptedMarketAdapter();
    market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    const engine = new EngineService(env, journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
    });
    await engine.start();
    try {
      expect(engine.experimentManifest()?.status).toBe("IDLE");
      let release!: () => void;
      gate = new Promise((resolve) => { release = resolve; });
      const pending = engine.registerTriggerForResearch(researchTrigger());
      await waitFor(async () => (await metricValue(engine.metrics, "process_trigger_in_flight")) === 1);
      expect(await metricValue(engine.metrics, "process_trigger_in_flight_peak")).toBeGreaterThanOrEqual(1);
      release();
      await pending;
      expect(await metricValue(engine.metrics, "process_trigger_in_flight")).toBe(0);
      expect(await histogramCount(engine.metrics, "process_trigger_duration_seconds")).toBeGreaterThanOrEqual(1);
      expect(await histogramCount(engine.metrics, "idle_activation_latency_bookkeeping_duration_seconds")).toBeGreaterThanOrEqual(1);

      journal.append = async () => { throw new Error("JOURNAL_DOWN"); };
      await engine.registerTriggerForResearch(researchTrigger({ id: createId("trigger") }));
      expect(await metricValue(engine.metrics, "process_trigger_in_flight")).toBe(0);
      expect(await metricValue(engine.metrics, "journal_direct_in_flight")).toBe(0);
      expect(await metricValue(engine.metrics, "journal_direct_failures_total", { class: "trigger" })).toBeGreaterThanOrEqual(1);
    } finally {
      await engine.stop();
    }
  });

  it("returns journal in-flight to zero after success and failure without retries", async () => {
    const metrics = new EngineMetrics();
    const journal = new MemoryEventJournal();
    let appends = 0;
    const original = journal.append.bind(journal);
    journal.append = async (event) => {
      appends += 1;
      return original(event);
    };
    const writer = new BoundedJournalWriter(journal, () => undefined, { error: () => undefined }, undefined, metrics);
    await writer.persist(createJournalEvent({
      traceId: createId("trace"), type: "TRIGGER_EVENT", aggregateType: "trigger", aggregateId: "t1",
      payload: { ok: true },
    }));
    expect(await metricValue(metrics, "journal_direct_in_flight")).toBe(0);
    expect(await metricValue(metrics, "journal_direct_writes_total", { class: "trigger" })).toBe(1);
    expect(appends).toBe(1);

    journal.append = async () => {
      appends += 1;
      throw new Error("JOURNAL_DOWN");
    };
    const before = appends;
    await writer.persist(createJournalEvent({
      traceId: createId("trace"), type: "TRIGGER_EVENT", aggregateType: "trigger", aggregateId: "t2",
      payload: { ok: false },
    }));
    expect(appends).toBe(before + 1);
    expect(await metricValue(metrics, "journal_direct_in_flight")).toBe(0);
    expect(await metricValue(metrics, "journal_direct_failures_total", { class: "trigger" })).toBe(1);
  });
});

describe("discovery overlap is observed and not prevented", () => {
  it("increments overlap while both refreshes complete", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const market = new DelayedDiscoveryAdapter();
    market.hold = hold;
    market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
    market.queue(discoveryReport([qualified("eth-mkt", "ETH", "eth-up", "eth-down")]));
    const journal = new MemoryEventJournal();
    const env = qualifiedPaperEnv({ GROK_ACTIVATION_PROVIDER: "noop" });
    const engine = new EngineService(env, journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
    });
    const starting = engine.start();
    await waitFor(async () => (await metricValue(engine.metrics, "discovery_refresh_in_flight")) === 1);
    const second = engine.reconcileDiscovery();
    await waitFor(async () => (await metricValue(engine.metrics, "discovery_refresh_overlap_total")) >= 1);
    expect(await metricValue(engine.metrics, "discovery_refresh_in_flight")).toBe(2);
    release();
    await Promise.all([starting, second]);
    expect(market.completed).toBe(2);
    expect(await metricValue(engine.metrics, "discovery_refresh_in_flight")).toBe(0);
    expect(await metricValue(engine.metrics, "discovery_refresh_attempts_total")).toBe(2);
    expect(await histogramCount(engine.metrics, "discovery_refresh_duration_seconds")).toBe(2);
    await engine.stop();
  });
});

describe("existing paper / SSE / discovery / journal semantics remain unchanged", () => {
  it("keeps paper trading and IDLE experiment behavior", async () => {
    const { engine, journal } = await startQualifiedEngine({ GROK_ACTIVATION_PROVIDER: "noop" });
    try {
      expect(engine.experimentManifest()?.status).toBe("IDLE");
      await engine.registerTriggerForResearch(researchTrigger());
      const events = await journal.queryEvents({ types: ["TRIGGER_EVENT", "GROK_ACTIVATION_DECISION", "PAPER_ORDER_CREATED"] });
      expect(events.some((event) => event.type === "TRIGGER_EVENT")).toBe(true);
      expect(events.some((event) => event.type === "GROK_ACTIVATION_DECISION" && (event.payload as { reason?: string }).reason === "EXPERIMENT_NOT_ACTIVE")).toBe(true);
      expect(events.some((event) => event.type === "PAPER_ORDER_CREATED")).toBe(false);
      const taker = executePaperTaker({ order: order(), snapshotAfterLatency: snapshot(), measuredLatencyMs: 150 });
      expect(taker.fillPrice).toBe("0.49");
    } finally {
      await engine.stop();
    }
  });

  it("keeps SSE frame format and still performs snapshot work with zero subscribers", async () => {
    const hub = new SseHub(20);
    const writes: string[] = [];
    hub.add({ write: (chunk) => { writes.push(chunk); return true; }, end: () => undefined });
    hub.publish("decision", { hello: "world" });
    expect(writes[0]).toBe(`id: 1\nevent: decision\ndata: ${JSON.stringify({ hello: "world" })}\n\n`);
    let built = 0;
    const empty = new SseHub(20);
    empty.publish("snapshot", () => {
      built += 1;
      return { n: 1 };
    });
    await waitFor(() => built === 1);
    expect(built).toBe(1);
    expect(empty.snapshotBroadcastCount()).toBe(1);
  });

  it("keeps discovery applying the latest report when refreshes overlap", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const market = new DelayedDiscoveryAdapter();
    market.hold = hold;
    const first = qualified("btc-mkt", "BTC", "btc-up", "btc-down");
    const second = qualified("eth-mkt", "ETH", "eth-up", "eth-down");
    market.queue(discoveryReport([first]));
    market.queue(discoveryReport([second]));
    const journal = new MemoryEventJournal();
    const engine = new EngineService(qualifiedPaperEnv({ GROK_ACTIVATION_PROVIDER: "noop" }), journal, { error: () => undefined }, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
    });
    const starting = engine.start();
    const overlapping = engine.reconcileDiscovery();
    release();
    await Promise.all([starting, overlapping]);
    expect(market.completed).toBe(2);
    expect(engine.monitoredMarketIds().length).toBeGreaterThan(0);
    await engine.stop();
  });
});

describe("market normalization and trigger/reference/telemetry metrics", () => {
  it("records snapshot, price-change, and book-level metrics without extra collection scans", async () => {
    const metrics = new EngineMetrics();
    const client = new FakePolymarketClient();
    const adapter = new PolymarketMarketDataAdapter(client as never);
    adapter.setNormalizeObserver(metrics.normalizeObserver());
    adapter.registerManualToken("tok-up", "mkt", "UP", "BTC");
    adapter.setTokenIds(["tok-up"]);
    const snapshots: unknown[] = [];
    const controller = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: (snapshot) => { snapshots.push(snapshot); },
      onConnection: () => undefined,
      onError: () => undefined,
    }, controller.signal);
    await waitFor(() => adapter.activeStreamCount() === 1);
    client.stream.push({
      type: "book",
      payload: {
        tokenId: "tok-up", market: "mkt", timestamp: 1_000,
        bids: [{ price: "0.40", size: "1" }, { price: "0.39", size: "2" }],
        asks: [{ price: "0.60", size: "3" }],
      },
    });
    await waitFor(() => snapshots.length === 1);
    client.stream.push({
      type: "price_change",
      payload: {
        timestamp: 1_001,
        priceChanges: [
          { tokenId: "tok-up", side: "BUY", price: "0.41", size: "4" },
          { tokenId: "tok-up", side: "SELL", price: "0.59", size: "1" },
        ],
      },
    });
    await waitFor(() => snapshots.length === 3);
    expect(await metricValue(metrics, "normalized_market_snapshots_total")).toBe(3);
    expect(await metricValue(metrics, "price_change_entries_total")).toBe(2);
    expect(await histogramCount(metrics, "price_change_entries_per_event")).toBe(1);
    expect(await histogramCount(metrics, "market_book_bid_levels")).toBe(3);
    expect(await histogramCount(metrics, "market_book_ask_levels")).toBe(3);
    expect(await histogramCount(metrics, "market_normalization_duration_seconds")).toBe(3);
    controller.abort();
    await running;
  });

  it("records trigger candidates, emissions, suppressions, and reference window length", async () => {
    const metrics = new EngineMetrics();
    const engine = new TriggerEngine(
      triggerConfigSchema.parse({ cooldownMs: 60_000, imbalanceThreshold: 0.5, referenceRapidMoveBps: 1, btcRapidMoveBps: 1 }),
      () => new Date(),
      metrics.triggerObserver(),
    );
    const book = snapshot({ bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "1" }] });
    expect(engine.ingestMarket(book).length).toBeGreaterThan(0);
    expect(engine.ingestMarket({ ...book, eventId: createId("market"), sequence: 2 }).length).toBe(0);
    expect(await metricValue(metrics, "trigger_candidates_total")).toBeGreaterThan(0);
    expect(await metricValue(metrics, "triggers_emitted_total")).toBeGreaterThan(0);
    expect(await metricValue(metrics, "triggers_suppressed_total")).toBeGreaterThan(0);
    engine.ingestReference({
      eventId: createId("ref"), traceId: createId("trace"), asset: "BTC", symbol: "BTCUSDT", venue: "binance",
      price: "100", quantity: "1", sourceTimestamp: new Date().toISOString(), receivedAt: new Date().toISOString(), tradeId: "1",
    });
    expect(await histogramCount(metrics, "reference_window_length")).toBe(1);
  });

  it("records telemetry queue depth, drops, flush duration, failures, and batch size", async () => {
    const metrics = new EngineMetrics();
    const journal = new MemoryEventJournal();
    journal.appendMany = async () => { throw new Error("TELEMETRY_DOWN"); };
    const writer = new BoundedJournalWriter(journal, () => undefined, { error: () => undefined }, {
      queueLimit: 2, batchSize: 2, sampleEvery: 1, maxConcurrent: 2,
    }, metrics);
    await writer.persist(createJournalEvent({ traceId: createId("trace"), type: "MARKET_SNAPSHOT", aggregateType: "market", aggregateId: "m", payload: {} }));
    await writer.persist(createJournalEvent({ traceId: createId("trace"), type: "MARKET_SNAPSHOT", aggregateType: "market", aggregateId: "m", payload: {} }));
    await writer.persist(createJournalEvent({ traceId: createId("trace"), type: "MARKET_SNAPSHOT", aggregateType: "market", aggregateId: "m", payload: {} }));
    await writer.flush();
    expect(await metricValue(metrics, "telemetry_events_dropped_total")).toBeGreaterThan(0);
    expect(await metricValue(metrics, "telemetry_batch_failures_total")).toBeGreaterThan(0);
    expect(await histogramCount(metrics, "telemetry_batch_size")).toBeGreaterThan(0);
    expect(await histogramCount(metrics, "telemetry_batch_flush_duration_seconds")).toBeGreaterThan(0);
    expect(await metricValue(metrics, "telemetry_queue_depth")).toBeGreaterThanOrEqual(0);
  });
});
