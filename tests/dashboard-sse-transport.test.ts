import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { GET as eventsProxy } from "../apps/dashboard/app/api/engine/events/route.js";
import { GET as snapshotProxy } from "../apps/dashboard/app/api/engine/snapshot/route.js";
import { startEngineLiveTransport, type EngineEventSource, type StreamStatus } from "../apps/dashboard/lib/live-transport.js";
import { DEFAULT_SSE_PROXY_MAX_MS, SSE_PROXY_HARD_CAP_MS, proxyEngineSse, resolveSseProxyMaxMs } from "../apps/dashboard/lib/sse-proxy.js";
import type { DashboardSnapshot } from "../apps/dashboard/lib/types.js";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const TOKEN = "proxy-read-token-secret";
const encoder = new TextEncoder();
const decoder = new TextDecoder();

afterEach(() => {
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

function snapshotFixture(equity: string): DashboardSnapshot {
  return {
    paperMode: true,
    generatedAt: "2026-09-03T00:00:00.000Z",
    engine: { ready: true, draining: false, alive: true, uptimeSeconds: 12, queueDepth: 0, experimentId: null, experimentStatus: "IDLE" },
    balance: {
      cash: "1000", reservedCash: "0", realizedPnl: "0", unrealizedPnl: "0", grossPnl: "0", fees: "0", slippage: "0",
      aiCost: "0", infrastructureCost: "0", netTradingPnl: "0", netEconomicPnl: "0", equity, drawdown: "0",
      updatedAt: "2026-09-03T00:00:00.000Z",
    },
    positions: [],
    latestAgents: {},
    consensus: null,
    risk: null,
    scanner: [],
    decisionFeed: [],
    grokActivity: { acceptedSignals: 0, rejectedSignals: 0, duplicates: 0, unauthorizedCalls: 0, rateLimitedCalls: 0, lastSeenAt: null, byAgent: {} },
    recentTriggers: [],
    health: { polymarketWs: { state: "CONNECTED", lastMessageAt: null, reconnects: 0 }, btcWs: { state: "CONNECTED", lastMessageAt: null, reconnects: 0 }, database: "UP", eventLatencyMs: 4, lastBtcMessageAt: null },
    equityHistory: [],
  };
}

class MockEventSource implements EngineEventSource {
  static instances: MockEventSource[] = [];
  onopen: ((ev: Event) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  closed = false;
  readonly url: string;
  private readonly listeners = new Map<string, Array<(event: MessageEvent<string>) => void>>();

  constructor(url: string) {
    this.url = url;
    MockEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: (event: MessageEvent<string>) => void): void {
    const list = this.listeners.get(type) ?? [];
    list.push(listener);
    this.listeners.set(type, list);
  }

  close(): void {
    this.closed = true;
  }

  emitOpen(): void {
    this.onopen?.(new Event("open"));
  }

  emitError(): void {
    this.onerror?.(new Event("error"));
  }

  emitSnapshot(data: unknown): void {
    const event = { data: JSON.stringify(data) } as MessageEvent<string>;
    for (const listener of this.listeners.get("snapshot") ?? []) listener(event);
  }
}

async function drainStream(body: ReadableStream<Uint8Array> | null, timeoutMs = 2_000): Promise<string> {
  if (!body) throw new Error("missing body");
  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const remaining = Math.max(1, deadline - Date.now());
    const result = await Promise.race([
      reader.read(),
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("drain timeout")), remaining)),
    ]);
    if (result.done) break;
    if (result.value) chunks.push(result.value);
  }
  return decoder.decode(Buffer.concat(chunks.map((c) => Buffer.from(c))));
}

function hangingUpstream(onCancel: () => void, fetchSignal?: AbortSignal): ReadableStream<Uint8Array> {
  let notified = false;
  const notify = () => {
    if (notified) return;
    notified = true;
    onCancel();
  };
  return new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(encoder.encode("event: snapshot\ndata: {\"paperMode\":true}\n\n"));
      fetchSignal?.addEventListener("abort", () => {
        notify();
        try {
          controller.error(new DOMException("Aborted", "AbortError"));
        } catch {
          // already closed
        }
      });
    },
    cancel() {
      notify();
    },
  });
}

describe("dashboard SSE transport hardening", () => {
  it("A SSE proxy establishes an authenticated upstream Fly stream", async () => {
    vi.stubEnv("ENGINE_INTERNAL_URL", "http://engine.internal:8080");
    vi.stubEnv("ENGINE_READ_TOKEN", TOKEN);
    const fetchImpl = vi.fn(async () => new Response(hangingUpstream(() => undefined), {
      status: 200,
      headers: { "content-type": "text/event-stream" },
    }));
    const response = await proxyEngineSse(new Request("http://dashboard.local/api/engine/events", {
      headers: { "last-event-id": "42" },
    }), { maxMs: 5_000, fetchImpl: fetchImpl as unknown as typeof fetch });
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("text/event-stream");
    expect(fetchImpl).toHaveBeenCalledOnce();
    const [url, init] = fetchImpl.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("http://engine.internal:8080/events");
    const headers = new Headers(init.headers);
    expect(headers.get("authorization")).toBe(`Bearer ${TOKEN}`);
    expect(headers.get("accept")).toBe("text/event-stream");
    expect(headers.get("last-event-id")).toBe("42");
    await response.body?.cancel();
  });

  it("B controlled lifetime closes downstream before the 300s runtime kill", async () => {
    vi.stubEnv("ENGINE_INTERNAL_URL", "http://engine.internal:8080");
    vi.stubEnv("ENGINE_READ_TOKEN", TOKEN);
    vi.stubEnv("SSE_PROXY_MAX_MS", "40");
    expect(resolveSseProxyMaxMs()).toBe(40);
    expect(DEFAULT_SSE_PROXY_MAX_MS).toBe(240_000);
    expect(SSE_PROXY_HARD_CAP_MS).toBe(270_000);
    expect(SSE_PROXY_HARD_CAP_MS).toBeLessThan(300_000);
    expect(resolveSseProxyMaxMs({ SSE_PROXY_MAX_MS: "400000" })).toBe(SSE_PROXY_HARD_CAP_MS);
    expect(resolveSseProxyMaxMs({}, 500_000)).toBe(SSE_PROXY_HARD_CAP_MS);

    let cancelled = false;
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => new Response(
      hangingUpstream(() => { cancelled = true; }, init?.signal ?? undefined),
      { status: 200, headers: { "content-type": "text/event-stream" } },
    ));
    const started = Date.now();
    const response = await proxyEngineSse(new Request("http://dashboard.local/api/engine/events"), {
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    expect(response.headers.get("x-sse-proxy-max-ms")).toBe("40");
    const text = await drainStream(response.body, 1_000);
    const elapsed = Date.now() - started;
    expect(elapsed).toBeGreaterThanOrEqual(25);
    expect(elapsed).toBeLessThan(300);
    expect(text).toMatch(/sse-proxy-rotation/);
    expect(response.headers.get("content-type")).toBe("text/event-stream");
    expect(cancelled).toBe(true);
  });

  it("C upstream reader is cancelled after controlled rotation", async () => {
    vi.stubEnv("ENGINE_INTERNAL_URL", "http://engine.internal:8080");
    vi.stubEnv("ENGINE_READ_TOKEN", TOKEN);
    let cancelled = false;
    let fetchSignal: AbortSignal | undefined;
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => {
      fetchSignal = init?.signal ?? undefined;
      return new Response(hangingUpstream(() => { cancelled = true; }, fetchSignal), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      });
    });
    const response = await proxyEngineSse(new Request("http://dashboard.local/api/engine/events"), {
      maxMs: 30,
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    await drainStream(response.body, 1_000);
    expect(cancelled).toBe(true);
    expect(fetchSignal?.aborted).toBe(true);
  });

  it("D client abort cleans up the upstream stream immediately", async () => {
    vi.stubEnv("ENGINE_INTERNAL_URL", "http://engine.internal:8080");
    vi.stubEnv("ENGINE_READ_TOKEN", TOKEN);
    let cancelled = false;
    let fetchSignal: AbortSignal | undefined;
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => {
      fetchSignal = init?.signal ?? undefined;
      return new Response(hangingUpstream(() => { cancelled = true; }, fetchSignal), {
        status: 200,
        headers: { "content-type": "text/event-stream" },
      });
    });
    const client = new AbortController();
    const response = await proxyEngineSse(new Request("http://dashboard.local/api/engine/events", { signal: client.signal }), {
      maxMs: 10_000,
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    const reader = response.body!.getReader();
    const first = await reader.read();
    expect(first.done).toBe(false);
    const started = Date.now();
    client.abort();
    while (!cancelled && Date.now() - started < 500) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    expect(cancelled).toBe(true);
    expect(fetchSignal?.aborted).toBe(true);
    expect(Date.now() - started).toBeLessThan(400);
  });

  it("E EventSource client reconnects after stream termination without a second instance", async () => {
    MockEventSource.instances = [];
    const statuses: StreamStatus[] = [];
    const transport = startEngineLiveTransport({
      onStatus: (status) => statuses.push(status),
      onSnapshot: () => undefined,
    }, { eventSourceCtor: MockEventSource, fetchImpl: vi.fn(async () => new Response(null, { status: 503 })) as unknown as typeof fetch });
    expect(MockEventSource.instances).toHaveLength(1);
    const source = MockEventSource.instances[0]!;
    source.emitOpen();
    source.emitError();
    source.emitOpen();
    expect(MockEventSource.instances).toHaveLength(1);
    expect(source.closed).toBe(false);
    expect(statuses).toEqual(["LIVE", "DEGRADED", "LIVE"]);
    transport.close();
    expect(source.closed).toBe(true);
    expect(readFileSync(join(repoRoot, "apps/dashboard/app/dashboard.tsx"), "utf8")).not.toMatch(/new EventSource/);
    expect(readFileSync(join(repoRoot, "apps/dashboard/lib/live-transport.ts"), "utf8")).not.toMatch(/setInterval/);
  });

  it("F dashboard retains the previous snapshot during a temporary disconnect", async () => {
    MockEventSource.instances = [];
    const snapshots: DashboardSnapshot[] = [];
    const first = snapshotFixture("1000");
    startEngineLiveTransport({
      onStatus: () => undefined,
      onSnapshot: (snapshot) => snapshots.push(snapshot),
    }, {
      eventSourceCtor: MockEventSource,
      fetchImpl: vi.fn(async () => { throw new Error("snapshot unavailable"); }) as unknown as typeof fetch,
    });
    const source = MockEventSource.instances[0]!;
    source.emitSnapshot(first);
    expect(snapshots).toEqual([first]);
    source.emitError();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(snapshots).toEqual([first]);
  });

  it("G recovery snapshot updates state after SSE failure", async () => {
    MockEventSource.instances = [];
    const snapshots: DashboardSnapshot[] = [];
    const first = snapshotFixture("1000");
    const recovered = snapshotFixture("1100");
    let inflight = 0;
    let fetches = 0;
    const fetchImpl = vi.fn(async (url: string, init?: RequestInit) => {
      expect(url).toBe("/api/engine/snapshot");
      expect(new Headers(init?.headers).get("authorization")).toBeNull();
      fetches += 1;
      inflight += 1;
      expect(inflight).toBe(1);
      await new Promise((resolve) => setTimeout(resolve, 30));
      inflight -= 1;
      return Response.json(recovered);
    });
    startEngineLiveTransport({
      onStatus: () => undefined,
      onSnapshot: (snapshot) => snapshots.push(snapshot),
    }, { eventSourceCtor: MockEventSource, fetchImpl: fetchImpl as unknown as typeof fetch });
    const source = MockEventSource.instances[0]!;
    source.emitSnapshot(first);
    source.emitError();
    source.emitError();
    await new Promise((resolve) => setTimeout(resolve, 80));
    expect(fetches).toBe(1);
    expect(snapshots).toEqual([first, recovered]);
  });

  it("H ENGINE_READ_TOKEN is never returned to the browser or client payload", async () => {
    vi.stubEnv("ENGINE_INTERNAL_URL", "http://engine.internal:8080");
    vi.stubEnv("ENGINE_READ_TOKEN", TOKEN);
    const fetchImpl = vi.fn(async () => new Response(new ReadableStream({
      start(controller) {
        controller.enqueue(encoder.encode("event: snapshot\ndata: {\"paperMode\":true}\n\n"));
        controller.close();
      },
    }), { status: 200, headers: { "content-type": "text/event-stream" } }));
    const response = await proxyEngineSse(new Request("http://dashboard.local/api/engine/events"), {
      maxMs: 5_000,
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    const text = await drainStream(response.body, 1_000);
    expect(text).not.toContain(TOKEN);
    expect(text).not.toMatch(/authorization/i);
    for (const [name, value] of response.headers) {
      expect(name).not.toMatch(/authorization/i);
      expect(value).not.toContain(TOKEN);
    }

    MockEventSource.instances = [];
    const recovered = snapshotFixture("1200");
    const clientFetch = vi.fn(async (url: string, init?: RequestInit) => {
      expect(String(url)).toBe("/api/engine/snapshot");
      expect(JSON.stringify(init ?? {})).not.toContain(TOKEN);
      expect(new Headers(init?.headers).get("authorization")).toBeNull();
      return Response.json(recovered);
    });
    const seen: DashboardSnapshot[] = [];
    startEngineLiveTransport({
      onStatus: () => undefined,
      onSnapshot: (snapshot) => seen.push(snapshot),
    }, { eventSourceCtor: MockEventSource, fetchImpl: clientFetch as unknown as typeof fetch });
    MockEventSource.instances[0]!.emitError();
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(JSON.stringify(seen)).not.toContain(TOKEN);
    expect(JSON.stringify(seen)).not.toMatch(/NEXT_PUBLIC_/);
    expect(readFileSync(join(repoRoot, "apps/dashboard/lib/live-transport.ts"), "utf8")).not.toMatch(/ENGINE_READ_TOKEN/);
    expect(readFileSync(join(repoRoot, "apps/dashboard/lib/live-transport.ts"), "utf8")).not.toMatch(/NEXT_PUBLIC_/);
    expect(readFileSync(join(repoRoot, ".env.example"), "utf8")).not.toMatch(/^NEXT_PUBLIC_ENGINE_READ_TOKEN=/m);
  });

  it("I existing snapshot proxy behavior remains unchanged", async () => {
    vi.stubEnv("ENGINE_INTERNAL_URL", "http://engine.internal:8080");
    vi.stubEnv("ENGINE_READ_TOKEN", TOKEN);
    const payload = snapshotFixture("1000");
    const fetchMock = vi.fn(async (url: string, init?: RequestInit) => {
      expect(url).toBe("http://engine.internal:8080/api/dashboard");
      expect(new Headers(init?.headers).get("authorization")).toBe(`Bearer ${TOKEN}`);
      return Response.json(payload);
    });
    vi.stubGlobal("fetch", fetchMock);
    const ok = await snapshotProxy();
    expect(ok.status).toBe(200);
    expect(ok.headers.get("content-type")).toMatch(/application\/json/);
    expect(ok.headers.get("cache-control")).toBe("no-store");
    expect(ok.headers.get("authorization")).toBeNull();
    const body = await ok.json() as DashboardSnapshot;
    expect(body.paperMode).toBe(true);
    expect(body.engine.experimentStatus).toBe("IDLE");
    expect(JSON.stringify(body)).not.toContain(TOKEN);

    vi.stubGlobal("fetch", vi.fn(async () => { throw new Error("ECONNREFUSED"); }));
    const down = await snapshotProxy();
    expect(down.status).toBe(503);
    expect(await down.json()).toEqual({ error: "ENGINE_UNREACHABLE" });

    const eventsRoute = readFileSync(join(repoRoot, "apps/dashboard/app/api/engine/events/route.ts"), "utf8");
    expect(eventsRoute).not.toMatch(/maxDuration/);
    expect(existsSync(join(repoRoot, "fly.engine.toml"))).toBe(true);
  });

  it("upstream fetch stall before headers is aborted by the pre-fetch deadline", async () => {
    vi.stubEnv("ENGINE_INTERNAL_URL", "http://engine.internal:8080");
    vi.stubEnv("ENGINE_READ_TOKEN", TOKEN);
    let fetchSignal: AbortSignal | undefined;
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => {
      fetchSignal = init?.signal ?? undefined;
      await new Promise<Response>((_resolve, reject) => {
        const fail = () => reject(new DOMException("Aborted", "AbortError"));
        if (fetchSignal?.aborted) fail();
        else fetchSignal?.addEventListener("abort", fail, { once: true });
      });
    });
    const started = Date.now();
    const response = await proxyEngineSse(new Request("http://dashboard.local/api/engine/events"), {
      maxMs: 40,
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    const elapsed = Date.now() - started;
    expect(response.status).toBe(503);
    expect(await response.json()).toEqual({ error: "ENGINE_UNREACHABLE" });
    expect(fetchSignal?.aborted).toBe(true);
    expect(elapsed).toBeGreaterThanOrEqual(25);
    expect(elapsed).toBeLessThan(300);
    expect(elapsed).toBeLessThan(SSE_PROXY_HARD_CAP_MS);
  });

  it("stale snapshot recovery does not overwrite a newer SSE snapshot after reconnect", async () => {
    MockEventSource.instances = [];
    const snapshots: DashboardSnapshot[] = [];
    const stale = snapshotFixture("1500");
    const newer = snapshotFixture("2000");
    const recovery = Promise.withResolvers<Response>();
    let fetches = 0;
    const fetchImpl = vi.fn(async () => {
      fetches += 1;
      return recovery.promise;
    });
    const transport = startEngineLiveTransport({
      onStatus: () => undefined,
      onSnapshot: (snapshot) => snapshots.push(snapshot),
    }, { eventSourceCtor: MockEventSource, fetchImpl: fetchImpl as unknown as typeof fetch });
    const source = MockEventSource.instances[0]!;
    source.emitError();
    source.emitError();
    await Promise.resolve();
    expect(fetches).toBe(1);
    source.emitOpen();
    source.emitSnapshot(newer);
    recovery.resolve(Response.json(stale));
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(snapshots).toEqual([newer]);
    expect(snapshots.map((row) => row.balance.equity)).not.toContain("1500");
    transport.close();
  });

  it("slow downstream does not eagerly drain the upstream SSE", async () => {
    vi.stubEnv("ENGINE_INTERNAL_URL", "http://engine.internal:8080");
    vi.stubEnv("ENGINE_READ_TOKEN", TOKEN);
    let upstreamPulls = 0;
    const fetchImpl = vi.fn(async (_url: string, init?: RequestInit) => {
      const stream = new ReadableStream<Uint8Array>({
        pull(controller) {
          upstreamPulls += 1;
          if (upstreamPulls > 40) {
            controller.close();
            return;
          }
          controller.enqueue(encoder.encode(`data: ${upstreamPulls}\n\n`));
        },
      });
      return new Response(stream, { status: 200, headers: { "content-type": "text/event-stream" } });
    });
    const response = await proxyEngineSse(new Request("http://dashboard.local/api/engine/events"), {
      maxMs: 2_000,
      fetchImpl: fetchImpl as unknown as typeof fetch,
    });
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(upstreamPulls).toBeGreaterThanOrEqual(1);
    expect(upstreamPulls).toBeLessThanOrEqual(2);
    const reader = response.body!.getReader();
    const first = await reader.read();
    expect(first.done).toBe(false);
    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(upstreamPulls).toBeLessThanOrEqual(3);
    await reader.cancel();
  });
});
