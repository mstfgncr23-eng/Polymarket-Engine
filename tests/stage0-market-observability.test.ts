import { describe, expect, it } from "vitest";
import {
  BoundedClobMarketTransport,
  PolymarketMarketDataAdapter,
  ScriptedBoundedMarketTransport,
} from "@pm/market-data";
import { EngineMetrics, type MarketGenerationSummary } from "../apps/engine/src/metrics.js";
import { book, startQualifiedEngine } from "./paper-engine-testkit.js";

async function metricValue(metrics: EngineMetrics, name: string, labels?: Record<string, string>): Promise<number> {
  const metric = metrics.registry.getSingleMetric(name);
  if (!metric) return 0;
  const data = await metric.get();
  const values = data.values.filter((point) => {
    if (point.metricName?.endsWith("_bucket") || point.metricName?.endsWith("_sum")) return false;
    return !labels || Object.entries(labels).every(([key, value]) => point.labels[key] === value);
  });
  if (labels) return values[0]?.value ?? 0;
  return values.reduce((sum, point) => {
    if (point.metricName?.endsWith("_count")) return sum + point.value;
    if (!point.metricName || point.metricName === name) return sum + point.value;
    return sum;
  }, 0);
}

async function histogramCount(metrics: EngineMetrics, name: string): Promise<number> {
  const metric = metrics.registry.getSingleMetric(name);
  if (!metric) return 0;
  const data = await metric.get();
  return data.values
    .filter((point) => point.metricName === `${name}_count`)
    .reduce((sum, point) => sum + point.value, 0);
}

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function closeEvent(code: number, reason: string, wasClean = false): Event {
  return Object.assign(new Event("close"), { code, reason, wasClean });
}

class Stage0WebSocket extends EventTarget {
  static instances: Stage0WebSocket[] = [];
  readyState = 0;
  readonly sent: string[] = [];

  constructor(readonly url: string) {
    super();
    Stage0WebSocket.instances.push(this);
  }

  send(data: string): void { this.sent.push(data); }
  open(): void { this.readyState = 1; this.dispatchEvent(new Event("open")); }
  message(data: string): void { this.dispatchEvent(new MessageEvent("message", { data })); }
  remoteClose(code: number, reason: string): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatchEvent(closeEvent(code, reason));
  }
  close(): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatchEvent(closeEvent(1000, "", true));
  }
}

function rawBook(timestamp: number) {
  return {
    event_type: "book",
    market: "mkt",
    asset_id: "tok-up",
    timestamp: String(timestamp),
    hash: `book-${timestamp}`,
    bids: [{ price: "0.40", size: "10" }],
    asks: [{ price: "0.60", size: "10" }],
  };
}

function rawPriceChange(timestamp: number) {
  return {
    event_type: "price_change",
    market: "mkt",
    timestamp: String(timestamp),
    price_changes: [
      { asset_id: "tok-up", price: "0.41", size: "4", side: "BUY", hash: "delta-buy" },
      { asset_id: "tok-up", price: "0.59", size: "3", side: "SELL", hash: "delta-sell" },
    ],
  };
}

describe("Stage 0 market observability", () => {
  it("counts raw frames, control frames, expanded source events, and price-change entries", async () => {
    Stage0WebSocket.instances = [];
    const metrics = new EngineMetrics();
    const transport = new BoundedClobMarketTransport({
      webSocket: Stage0WebSocket as unknown as typeof WebSocket,
      observability: metrics.marketObservability(),
    });
    const pending = transport.subscribe(["tok-up"], false);
    await waitFor(() => Stage0WebSocket.instances.length === 1);
    const socket = Stage0WebSocket.instances[0]!;
    socket.open();
    const stream = await pending;

    socket.message(JSON.stringify(rawBook(1_000)));
    socket.message(JSON.stringify([rawPriceChange(1_001), { event_type: "last_trade_price", asset_id: "tok-up", price: "0.50" }]));
    socket.message("PONG");

    expect(await metricValue(metrics, "polymarket_clob_raw_frames_total", { frame_type: "market" })).toBe(2);
    expect(await metricValue(metrics, "polymarket_clob_raw_frames_total", { frame_type: "control" })).toBe(1);
    expect(await metricValue(metrics, "polymarket_clob_raw_frame_bytes_total", { frame_type: "market" })).toBeGreaterThan(0);
    expect(await metricValue(metrics, "polymarket_clob_source_events_total", { type: "book" })).toBe(1);
    expect(await metricValue(metrics, "polymarket_clob_source_events_total", { type: "price_change" })).toBe(1);
    expect(await metricValue(metrics, "polymarket_clob_source_events_total", { type: "last_trade_price" })).toBe(1);
    expect(await metricValue(metrics, "polymarket_clob_price_change_payloads_total")).toBe(1);
    expect(await metricValue(metrics, "polymarket_clob_price_change_entries_total")).toBe(2);
    expect(await histogramCount(metrics, "polymarket_clob_source_events_per_frame")).toBe(2);
    expect(await histogramCount(metrics, "polymarket_clob_parse_duration_seconds")).toBe(2);
    expect(await histogramCount(metrics, "polymarket_clob_pre_admission_duration_seconds")).toBe(2);
    expect(await histogramCount(metrics, "polymarket_clob_callback_to_admission_duration_seconds")).toBe(2);

    await stream.close();
  });

  it("summarizes one generation without misclassifying a provider 1013 close", async () => {
    Stage0WebSocket.instances = [];
    const metrics = new EngineMetrics();
    const summaries: MarketGenerationSummary[] = [];
    const transport = new BoundedClobMarketTransport({
      webSocket: Stage0WebSocket as unknown as typeof WebSocket,
    });
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport });
    adapter.setNormalizeObserver(metrics.normalizeObserver());
    adapter.setObservability(metrics.marketObservability((summary) => summaries.push(summary)));
    adapter.registerManualToken("tok-up", "mkt", "UP", "BTC");
    adapter.setTokenIds(["tok-up"]);
    const controller = new AbortController();
    const snapshots: unknown[] = [];
    const running = adapter.run(["tok-up"], {
      onSnapshot: (snapshot) => { snapshots.push(snapshot); },
      onConnection: () => undefined,
      onError: () => undefined,
    }, controller.signal);
    await waitFor(() => Stage0WebSocket.instances.length === 1);
    const socket = Stage0WebSocket.instances[0]!;
    socket.open();
    await waitFor(() => adapter.activeStreamCount() === 1);
    socket.message(JSON.stringify(rawBook(1_000)));
    socket.message(JSON.stringify(rawPriceChange(1_001)));
    await waitFor(() => snapshots.length === 3);
    socket.remoteClose(1013, "slow consumer: send buffer full");
    await waitFor(() => summaries.length === 1);
    controller.abort();
    await running;

    const [summary] = summaries;
    expect(summary).toMatchObject({
      tokenCount: 1,
      rawFrames: 2,
      marketFrames: 2,
      sourceEvents: 2,
      eventCounts: { book: 1, price_change: 1, last_trade_price: 0, other: 0 },
      bookEvents: 1,
      priceChangePayloads: 1,
      priceChangeEntries: 2,
      normalizedSnapshots: 3,
      firstTrustedBooks: 1,
      uniqueTrustedTokens: 1,
      closeCode: 1013,
      closeReason: "slow consumer: send buffer full",
      endReason: "provider_close",
    });
    expect(summary!.queueHwmEvents).toBeGreaterThanOrEqual(0);
    expect(await metricValue(metrics, "polymarket_clob_source_events_total", { type: "price_change" })).toBe(1);
    expect(await histogramCount(metrics, "polymarket_market_normalize_event_duration_seconds")).toBe(2);
    expect(await histogramCount(metrics, "polymarket_market_snapshot_handler_total_duration_seconds")).toBe(3);
    expect(await metricValue(metrics, "polymarket_market_generation_ended_total", { reason: "provider_close" })).toBe(1);
  });

  it("resets generation-local aggregates and keeps operator changes separate", () => {
    const metrics = new EngineMetrics();
    const summaries: MarketGenerationSummary[] = [];
    const observer = metrics.marketObservability((summary) => summaries.push(summary));
    observer.onGenerationStart({ generation: 1, tokenCount: 2, startedAtMs: 1_000, queueHighWaterEvents: 0, queueHighWaterBytes: 0 });
    observer.onRawFrame({ kind: "market", bytes: 10 });
    observer.onSourceFrame(1);
    observer.onSourceEvent({ type: "book" });
    observer.onTrustedFullBook();
    observer.onGenerationEnd({ generation: 1, endedAtMs: 1_100, durationMs: 100, queueHighWaterEvents: 1, queueHighWaterBytes: 10, closeCode: 1013, closeReason: "provider", endReason: "provider_close" });
    observer.onGenerationStart({ generation: 2, tokenCount: 2, startedAtMs: 2_000, queueHighWaterEvents: 1, queueHighWaterBytes: 10 });
    observer.onGenerationEnd({ generation: 2, endedAtMs: 2_050, durationMs: 50, queueHighWaterEvents: 0, queueHighWaterBytes: 0, closeCode: null, closeReason: "", endReason: "operator_generation_change" });

    expect(summaries).toHaveLength(2);
    expect(summaries[0]).toMatchObject({ generation: 1, sourceEvents: 1, firstTrustedBooks: 1, endReason: "provider_close", closeCode: 1013 });
    expect(summaries[1]).toMatchObject({ generation: 2, sourceEvents: 0, firstTrustedBooks: 0, endReason: "operator_generation_change", closeCode: null });
  });

  it("records adapter normalization and Engine snapshot timing without changing snapshot delivery", async () => {
    const { engine, market } = await startQualifiedEngine({ GROK_ACTIVATION_PROVIDER: "noop" });
    await market.waitForHandlers();
    await market.emitSnapshot(book({ marketId: "btc-mkt", tokenId: "btc-up", asset: "BTC", outcome: "UP" }));
    expect(await histogramCount(engine.metrics, "polymarket_market_snapshot_processing_duration_seconds")).toBe(1);
    await engine.stop();
  });

  it("keeps the scripted transport compatible with adapter observability without inventing raw-frame counts", async () => {
    const metrics = new EngineMetrics();
    const summaries: MarketGenerationSummary[] = [];
    const transport = new ScriptedBoundedMarketTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport });
    adapter.setObservability(metrics.marketObservability((summary) => summaries.push(summary)));
    adapter.registerManualToken("tok-up", "mkt", "UP", "BTC");
    const controller = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
    }, controller.signal);
    await waitFor(() => adapter.activeStreamCount() === 1);
    transport.push({ type: "book", payload: { tokenId: "tok-up", market: "mkt", timestamp: 1_000, bids: [{ price: "0.40", size: "1" }], asks: [{ price: "0.60", size: "1" }] } });
    await waitFor(() => summaries.length === 0 && adapter.isTokenSynchronized("tok-up"));
    controller.abort();
    await running;
    expect(summaries).toHaveLength(1);
    expect(summaries[0]).toMatchObject({ rawFrames: 0, sourceEvents: 0, normalizedSnapshots: 1, firstTrustedBooks: 1 });
  });
});
