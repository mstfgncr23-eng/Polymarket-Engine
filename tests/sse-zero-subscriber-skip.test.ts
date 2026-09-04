import { describe, expect, it } from "vitest";
import { PaperLedger } from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { SseHub, createApiServer } from "../apps/engine/src/server.js";
import { EngineStateStore } from "../apps/engine/src/state.js";

const PR9_SSE_METRIC_NAMES = [
  "sse_subscribers",
  "sse_snapshot_build_duration_seconds",
  "sse_snapshot_serialize_duration_seconds",
  "sse_snapshot_payload_bytes",
  "sse_snapshot_broadcasts_total",
  "sse_snapshots_scheduled_zero_subscribers_total",
  "sse_subscriber_backpressure_total",
] as const;

async function metricValue(metrics: EngineMetrics, name: string): Promise<number> {
  const item = metrics.registry.getSingleMetric(name);
  if (!item) return 0;
  const data = await item.get();
  const points = data.values.filter((point) => {
    if (point.metricName?.endsWith("_bucket") || point.metricName?.endsWith("_sum")) return false;
    return true;
  });
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

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 1_500): Promise<void> {
  const started = Date.now();
  while (!(await predicate())) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function snapshotDelayMs(hz: number): number {
  return Math.max(50, Math.floor(1000 / Math.max(0.5, hz)));
}

async function listenEventsServer(input: {
  state: EngineStateStore;
  sse: SseHub;
  metrics: EngineMetrics;
}): Promise<{ server: Awaited<ReturnType<typeof createApiServer>>; url: string }> {
  const server = await createApiServer({
    state: input.state,
    journal: new MemoryEventJournal(),
    metrics: input.metrics,
    sse: input.sse,
    dashboardOrigin: "http://localhost:3000",
    logger: false,
    mcp: { bearerTokens: "", allowedOrigins: "https://grok.com", rateLimitPerMinute: 30 },
    submitGrokSignal: async () => ({ status: "ignored" }),
  });
  await server.listen({ host: "127.0.0.1", port: 0 });
  const address = server.server.address();
  const port = typeof address === "object" && address ? address.port : 0;
  return { server, url: `http://127.0.0.1:${port}` };
}

async function readFirstSseEvent(url: string, signal: AbortSignal): Promise<string> {
  const response = await fetch(url, { headers: { accept: "text/event-stream" }, signal });
  if (!response.ok || !response.body) throw new Error(`events status ${response.status}`);
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let text = "";
  while (!text.includes("\n\n")) {
    const { value, done } = await reader.read();
    if (done) break;
    text += decoder.decode(value, { stream: true });
  }
  return text;
}

describe("SSE snapshot skip when there are zero subscribers", () => {
  it("A zero subscribers + lazy snapshot: builder invocation count remains 0", async () => {
    const hub = new SseHub(20);
    let built = 0;
    hub.publish("snapshot", () => {
      built += 1;
      return { n: 1 };
    });
    expect(built).toBe(0);
    await new Promise((resolve) => setTimeout(resolve, snapshotDelayMs(20) + 30));
    expect(built).toBe(0);
  });

  it("B many zero-subscriber publishes: no lazy snapshot even after waiting past throttle", async () => {
    const hub = new SseHub(20);
    let built = 0;
    for (let i = 0; i < 250; i += 1) {
      hub.publish("snapshot", () => {
        built += 1;
        return { i };
      });
    }
    await new Promise((resolve) => setTimeout(resolve, snapshotDelayMs(20) * 3));
    expect(built).toBe(0);
    expect(hub.snapshotBroadcastCount()).toBe(0);
  });

  it("C zero subscribers: snapshotBroadcastCount does not increase", async () => {
    const hub = new SseHub(20);
    hub.publish("snapshot", () => ({ n: 1 }));
    hub.publish("snapshot", { already: "built" });
    await new Promise((resolve) => setTimeout(resolve, snapshotDelayMs(20) + 30));
    expect(hub.snapshotBroadcastCount()).toBe(0);
  });

  it("D subscriber connected: snapshot coalescing remains one timer / one broadcast per window", async () => {
    const hub = new SseHub(20);
    const writes: string[] = [];
    hub.add({ write: (chunk) => { writes.push(chunk); return true; }, end: () => undefined });
    let built = 0;
    hub.publish("snapshot", () => {
      built += 1;
      return { n: 1 };
    });
    hub.publish("snapshot", () => {
      built += 1;
      return { n: 2 };
    });
    await waitFor(() => writes.length === 1);
    expect(built).toBe(1);
    expect(hub.snapshotBroadcastCount()).toBe(1);
    expect(writes[0]).toMatch(/^id: 1\nevent: snapshot\ndata: /);
    expect(writes[0]).toContain('"n":2');

    hub.publish("snapshot", () => {
      built += 1;
      return { n: 3 };
    });
    await waitFor(() => writes.length === 2);
    expect(built).toBe(2);
    expect(hub.snapshotBroadcastCount()).toBe(2);
    expect(writes[1]).toMatch(/^id: 2\nevent: snapshot\ndata: /);
    expect(writes[1]).toContain('"n":3');
  });

  it("E multiple snapshot publishes during throttle window: latest pending snapshot is broadcast", async () => {
    const hub = new SseHub(20);
    const writes: string[] = [];
    hub.add({ write: (chunk) => { writes.push(chunk); return true; }, end: () => undefined });
    const built: number[] = [];
    hub.publish("snapshot", () => { built.push(1); return { n: 1 }; });
    hub.publish("snapshot", () => { built.push(2); return { n: 2 }; });
    hub.publish("snapshot", () => { built.push(3); return { n: 3 }; });
    hub.publish("snapshot", () => { built.push(4); return { n: 4 }; });
    await waitFor(() => writes.length === 1);
    expect(built).toEqual([4]);
    expect(JSON.parse(writes[0]!.split("data: ")[1]!.trim())).toEqual({ n: 4 });
    expect(hub.snapshotBroadcastCount()).toBe(1);
  });

  it("F final subscriber disconnects while timer pending: timer cancelled and lazy snapshot never invoked", async () => {
    const hub = new SseHub(2);
    let built = 0;
    const remove = hub.add({ write: () => true, end: () => undefined });
    hub.publish("snapshot", () => {
      built += 1;
      return { n: 1 };
    });
    remove();
    await new Promise((resolve) => setTimeout(resolve, snapshotDelayMs(2) + 80));
    expect(built).toBe(0);
    expect(hub.snapshotBroadcastCount()).toBe(0);
  });

  it("G later new subscriber: /events still sends a fresh initial snapshot", async () => {
    const metrics = new EngineMetrics();
    const sse = new SseHub(20, metrics);
    const ledger = new PaperLedger(1_000);
    const state = new EngineStateStore(ledger.snapshot(), ledger.positions);
    state.database = "UP";
    state.experimentStatus = "IDLE";
    state.queueDepth = 7;
    let hubBuilt = 0;
    for (let i = 0; i < 20; i += 1) {
      sse.publish("snapshot", () => {
        hubBuilt += 1;
        return { stale: true, n: i };
      });
    }
    state.queueDepth = 42;
    const { server, url } = await listenEventsServer({ state, sse, metrics });
    const controller = new AbortController();
    try {
      const text = await readFirstSseEvent(`${url}/events`, controller.signal);
      expect(hubBuilt).toBe(0);
      expect(text).toMatch(/^event: snapshot\ndata: /);
      expect(text).not.toMatch(/^id: /m);
      const jsonLine = text.split("\n").find((line) => line.startsWith("data: "));
      expect(jsonLine).toBeTruthy();
      const snapshot = JSON.parse(jsonLine!.slice(6)) as {
        paperMode: boolean;
        engine: { queueDepth: number; experimentStatus: string | null };
      };
      expect(snapshot.paperMode).toBe(true);
      expect(snapshot.engine.experimentStatus).toBe("IDLE");
      expect(snapshot.engine.queueDepth).toBe(42);
      expect(snapshot.engine.queueDepth).not.toBe(7);
      expect(text).not.toContain("stale");
      await new Promise((resolve) => setTimeout(resolve, snapshotDelayMs(20) + 40));
      expect(hubBuilt).toBe(0);
      expect(sse.snapshotBroadcastCount()).toBe(0);
    } finally {
      controller.abort();
      await server.close();
    }
  });

  it("H backpressure metric behavior for a connected subscriber remains unchanged", async () => {
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

    hub.publish("snapshot", () => ({ paperMode: true, n: 1 }));
    await waitFor(() => writes.length === 2);
    expect(writes[1]).toMatch(/^id: 2\nevent: snapshot\ndata: /);
    expect(await metricValue(metrics, "sse_subscriber_backpressure_total")).toBe(2);
    expect(await metrics.registry.metrics()).not.toMatch(/marketId="/);
  });

  it("I non-snapshot SSE events remain unchanged with and without subscribers", async () => {
    const connected = new SseHub(20);
    const writes: string[] = [];
    connected.add({ write: (chunk) => { writes.push(chunk); return true; }, end: () => undefined });
    connected.publish("decision", { hello: "world" });
    expect(writes[0]).toBe(`id: 1\nevent: decision\ndata: ${JSON.stringify({ hello: "world" })}\n\n`);
    connected.publish("decision", { hello: "again" });
    expect(writes[1]).toBe(`id: 2\nevent: decision\ndata: ${JSON.stringify({ hello: "again" })}\n\n`);

    const empty = new SseHub(20);
    empty.publish("decision", { orphan: true });
    empty.publish("snapshot", () => ({ n: 1 }));
    await new Promise((resolve) => setTimeout(resolve, snapshotDelayMs(20) + 30));
    expect(empty.snapshotBroadcastCount()).toBe(0);
  });

  it("J skipped counter increments while build/serialize/broadcast do not", async () => {
    const metrics = new EngineMetrics();
    const hub = new SseHub(20, metrics);
    for (const name of PR9_SSE_METRIC_NAMES) {
      expect(metrics.registry.getSingleMetric(name), name).toBeTruthy();
    }
    expect(metrics.registry.getSingleMetric("sse_snapshots_skipped_zero_subscribers_total")).toBeTruthy();

    let built = 0;
    for (let i = 0; i < 17; i += 1) {
      hub.publish("snapshot", () => {
        built += 1;
        return { n: i };
      });
    }
    await new Promise((resolve) => setTimeout(resolve, snapshotDelayMs(20) + 30));
    expect(built).toBe(0);
    expect(await metricValue(metrics, "sse_snapshots_skipped_zero_subscribers_total")).toBe(17);
    expect(await metricValue(metrics, "sse_snapshots_scheduled_zero_subscribers_total")).toBe(0);
    expect(await metricValue(metrics, "sse_snapshot_broadcasts_total")).toBe(0);
    expect(await histogramCount(metrics, "sse_snapshot_build_duration_seconds")).toBe(0);
    expect(await histogramCount(metrics, "sse_snapshot_serialize_duration_seconds")).toBe(0);
    expect(await histogramCount(metrics, "sse_snapshot_payload_bytes")).toBe(0);

    const text = await metrics.registry.metrics();
    expect(text).toMatch(/sse_snapshots_skipped_zero_subscribers_total/);
    for (const name of PR9_SSE_METRIC_NAMES) {
      expect(text).toMatch(new RegExp(name));
    }
    expect(text).not.toMatch(/marketId="/);
    expect(text).not.toMatch(/tokenId="/);
    expect(text).not.toMatch(/traceId="/);
  });

  it("repeated zero-subscriber snapshot publishes never call the expensive builder", () => {
    const hub = new SseHub(4);
    let built = 0;
    const expensive = () => {
      built += 1;
      let acc = 0;
      for (let i = 0; i < 50_000; i += 1) acc += i;
      return { n: built, acc };
    };
    const started = performance.now();
    for (let i = 0; i < 10_000; i += 1) hub.publish("snapshot", expensive);
    const elapsedMs = performance.now() - started;
    expect(built).toBe(0);
    expect(hub.snapshotBroadcastCount()).toBe(0);
    expect(elapsedMs).toBeLessThan(250);
  });
});
