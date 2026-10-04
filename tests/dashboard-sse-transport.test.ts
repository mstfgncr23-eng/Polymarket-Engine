import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";
import { startEngineLiveTransport, type EngineEventSource, type StreamStatus } from "../apps/dashboard/lib/live-transport.js";
import type { DashboardSnapshot } from "../apps/dashboard/lib/types.js";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

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

describe("dashboard local browser transport", () => {
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


});
