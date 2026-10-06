import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import type { ReactNode } from "../apps/dashboard/node_modules/@types/react/index.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { PaperLedger } from "@pm/core";
import { EngineStateStore } from "../apps/engine/src/state.js";
import { dashboardCache, initialSnapshot, SnapshotCache, stopDashboardCache, pollConfig } from "../apps/dashboard/lib/snapshot-cache.js";
import { GET as snapshotRoute } from "../apps/dashboard/app/api/engine/snapshot/route.js";
import { GET as eventsRoute } from "../apps/dashboard/app/api/engine/events/route.js";
import { GET as readyRoute } from "../apps/dashboard/app/ready/route.js";
import { localSnapshotSse } from "../apps/dashboard/lib/local-sse.js";
import { PortfolioPanel } from "../apps/dashboard/app/components/portfolio-panel.js";
import { CacheStatus } from "../apps/dashboard/app/components/cache-status.js";
import type { DashboardSnapshot } from "../apps/dashboard/lib/types.js";

const require = createRequire(import.meta.url);
const { createElement } = require("../apps/dashboard/node_modules/react/index.js") as typeof import("../apps/dashboard/node_modules/@types/react/index.js");
const { renderToStaticMarkup } = require("../apps/dashboard/node_modules/react-dom/server.node.js") as { renderToStaticMarkup(node: ReactNode): string };

function fixture(status = "RUNNING"): DashboardSnapshot {
  const ledger = new PaperLedger("1000");
  const state = new EngineStateStore(ledger.snapshot(), ledger.positions);
  state.database = "UP";
  state.ready = true;
  state.experimentStatus = status;
  return state.snapshot() as unknown as DashboardSnapshot;
}

const caches: SnapshotCache[] = [];
function cache(fetcher: typeof fetch, env: NodeJS.ProcessEnv = {}) {
  vi.useFakeTimers();
  const value = new SnapshotCache(fetcher, env);
  caches.push(value);
  return value;
}
afterEach(() => {
  for (const c of caches.splice(0)) c.stop();
  stopDashboardCache();
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe("process-wide dashboard cache", () => {
  it.each([["RUNNING", 60_000, "LOW_IMPACT"], ["ENTRY_CLOSED", 60_000, "LOW_IMPACT"], ["IDLE", 10_000, "IDLE"], ["COMPLETED", 10_000, "IDLE"]])("%s polls at %s ms", async (status, interval, mode) => {
    const fetcher = vi.fn(async () => Response.json(fixture(String(status))));
    const c = cache(fetcher);
    await c.read();
    expect(c.metadata()).toMatchObject({ mode, pollIntervalMs: interval, stale: false });
    await vi.advanceTimersByTimeAsync(Number(interval) - 1);
    expect(fetcher).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it.each(["overload", "database", "journal", "alive", "ready"])("%s degradation overrides active cadence", async (reason) => {
    const s = fixture();
    if (reason === "overload") s.engine.overloadState = "FAIL_CLOSED";
    if (reason === "database") s.health.database = "DOWN";
    if (reason === "journal") s.health.journalDegraded = true;
    if (reason === "alive") s.engine.alive = false;
    if (reason === "ready") s.engine.ready = false;
    const fetcher = vi.fn(async () => Response.json(s));
    const c = cache(fetcher);
    await c.read();
    expect(c.metadata()).toMatchObject({ mode: "DEGRADED", pollIntervalMs: 120_000 });
    await vi.advanceTimersByTimeAsync(119_999);
    expect(fetcher).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("respects configured intervals and bounds invalid config", async () => {
    const env = { DASHBOARD_POLL_IDLE_MS: "200", DASHBOARD_POLL_ACTIVE_MS: "400", DASHBOARD_POLL_DEGRADED_MS: "800", DASHBOARD_UPSTREAM_TIMEOUT_MS: "100" };
    expect(pollConfig(env)).toEqual({ idle: 200, active: 400, degraded: 800, timeout: 100 });
    expect(pollConfig({ DASHBOARD_POLL_IDLE_MS: "0", DASHBOARD_POLL_ACTIVE_MS: "NaN", DASHBOARD_POLL_DEGRADED_MS: "-1" })).toEqual({ idle: 10_000, active: 60_000, degraded: 120_000, timeout: 3_000 });
    expect(pollConfig({ DASHBOARD_POLL_DEGRADED_MS: "2147483648" }).degraded).toBe(120_000);
    const fetcher = vi.fn(async () => Response.json(fixture()));
    const c = cache(fetcher, env);
    await c.read();
    await vi.advanceTimersByTimeAsync(400);
    expect(fetcher).toHaveBeenCalledTimes(2);
  });

  it("concurrent API reads, SSR and separate bundles share one authenticated RAM endpoint", async () => {
    vi.useFakeTimers();
    vi.stubEnv("ENGINE_INTERNAL_URL", "http://private.local:18080");
    vi.stubEnv("ENGINE_READ_TOKEN", "server-secret");
    const fetcher = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
      expect(url).toBe("http://private.local:18080/api/dashboard");
      expect(init?.method).toBe("GET");
      expect(new Headers(init?.headers).get("authorization")).toBe("Bearer server-secret");
      return Response.json(fixture());
    });
    vi.stubGlobal("fetch", fetcher);
    const [ssr, ...responses] = await Promise.all([initialSnapshot(), ...Array.from({ length: 20 }, () => snapshotRoute())]);
    expect(ssr?.cache?.mode).toBe("LOW_IMPACT");
    for (const response of responses) {
      expect((response as Response).status).toBe(200);
      expect(await (response as Response).text()).not.toContain("server-secret");
    }
    const original = dashboardCache();
    vi.resetModules();
    const otherBundle = await import("../apps/dashboard/lib/snapshot-cache.js");
    expect(otherBundle.dashboardCache()).toBe(original);
    await otherBundle.initialSnapshot();
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(readFileSync(new URL("../apps/dashboard/app/page.tsx", import.meta.url), "utf8")).toContain("initialSnapshot");
    expect(readFileSync(new URL("../apps/dashboard/app/page.tsx", import.meta.url), "utf8")).not.toContain("fetch(");
  });

  it("slow fetch, including an abort-ignoring adapter, never overlaps or spins", async () => {
    const pending = Promise.withResolvers<Response>();
    const fetcher = vi.fn(() => pending.promise);
    const c = cache(fetcher);
    const initial = c.read();
    await vi.advanceTimersByTimeAsync(3_000);
    expect(await initial).toBeNull();
    await vi.advanceTimersByTimeAsync(360_000);
    await Promise.all(Array.from({ length: 20 }, () => c.read()));
    expect(fetcher).toHaveBeenCalledTimes(1);
    pending.resolve(Response.json(fixture()));
    await vi.advanceTimersByTimeAsync(0);
    expect(c.peek()).toBeNull(); // Late timed-out response cannot become fresh.
  });

  it("timeout preserves stale cache; recovery notifies each subscriber once", async () => {
    let calls = 0;
    const fetcher = vi.fn(async (_url: string | URL | Request, init?: RequestInit) => {
      calls += 1;
      if (calls === 2) return new Promise<Response>((_, reject) => init!.signal!.addEventListener("abort", () => reject(new Error("timeout")), { once: true }));
      return Response.json(fixture());
    });
    const c = cache(fetcher);
    const a = vi.fn(), b = vi.fn();
    const unsubscribe = c.subscribe(a);
    c.subscribe(b);
    await c.read();
    expect(a).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(63_000);
    expect(c.peek()?.balance.equity).toBe("1000");
    expect(c.metadata()).toMatchObject({ stale: true, upstreamReachable: false, mode: "DEGRADED" });
    expect(a).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(120_000);
    expect(a).toHaveBeenCalledTimes(2);
    expect(b).toHaveBeenCalledTimes(2);
    expect(c.metadata().stale).toBe(false);
    unsubscribe();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(a).toHaveBeenCalledTimes(2);
    expect(b).toHaveBeenCalledTimes(3);
  });

  it("unavailable first snapshot uses bounded recovery; malformed response does not replace cache", async () => {
    let calls = 0;
    const fetcher = vi.fn(async () => {
      calls += 1;
      if (calls === 1) throw new Error("offline");
      return Response.json(calls === 3 ? { error: "bad" } : fixture());
    });
    const c = cache(fetcher);
    expect(await c.read()).toBeNull();
    await vi.advanceTimersByTimeAsync(119_999);
    expect(fetcher).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    expect(c.peek()).not.toBeNull();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(c.peek()?.balance.equity).toBe("1000");
    expect(c.metadata().stale).toBe(true);
  });

  it("events route and browser reconnects have only local subscriptions, no upstream /events", async () => {
    vi.useFakeTimers();
    const fetcher = vi.fn(async (url: string | URL | Request) => {
      expect(String(url)).toMatch(/\/api\/dashboard$/);
      expect(String(url)).not.toMatch(/\/events/);
      return Response.json(fixture());
    });
    vi.stubGlobal("fetch", fetcher);
    for (let reconnect = 0; reconnect < 3; reconnect += 1) {
      const controllers = Array.from({ length: 10 }, () => new AbortController());
      const responses = await Promise.all(controllers.map((abort) => eventsRoute(new Request("http://local/api/engine/events", { signal: abort.signal }))));
      await initialSnapshot();
      for (const response of responses) {
        const reader = response.body!.getReader();
        const text = new TextDecoder().decode((await reader.read()).value);
        expect(text).toContain("event: snapshot");
        expect(text).toContain('"mode":"LOW_IMPACT"');
        await reader.cancel();
      }
      controllers.forEach((abort) => abort.abort());
    }
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("local metadata marks failure without a snapshot notification and bounds slow client buffering", async () => {
    let calls = 0;
    const c = cache(vi.fn(async () => { if (++calls > 1) throw new Error("offline"); return Response.json(fixture()); }));
    await c.read();
    const abort = new AbortController();
    const response = localSnapshotSse(new Request("http://local", { signal: abort.signal }), c);
    const reader = response.body!.getReader();
    await reader.read();
    const read = reader.read();
    await vi.advanceTimersByTimeAsync(5_000);
    expect(new TextDecoder().decode((await read).value)).toContain("event: cache");
    await vi.advanceTimersByTimeAsync(60_000); // Stalled reader is disconnected after bounded queue.
    await reader.read();
    await reader.read();
    expect((await reader.read()).done).toBe(true);
    expect(c.metadata().stale).toBe(true);
    abort.abort();
  });

  it("readiness uses cached data and stopping prevents further polls", async () => {
    vi.useFakeTimers();
    const fetcher = vi.fn(async () => Response.json(fixture()));
    vi.stubGlobal("fetch", fetcher);
    expect((await readyRoute()).status).toBe(200);
    expect((await readyRoute()).status).toBe(200);
    expect(fetcher).toHaveBeenCalledTimes(1);
    stopDashboardCache();
    await vi.advanceTimersByTimeAsync(300_000);
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("snapshot API serves stale values and connected SSE reports actual failure metadata", async () => {
    vi.useFakeTimers();
    let calls = 0;
    const fetcher = vi.fn(async () => { if (++calls > 1) throw new Error("offline"); return Response.json(fixture()); });
    vi.stubGlobal("fetch", fetcher);
    const response = await eventsRoute(new Request("http://local/api/engine/events"));
    const reader = response.body!.getReader();
    await reader.read();
    for (let tick = 0; tick < 13; tick += 1) {
      const pending = reader.read();
      await vi.advanceTimersByTimeAsync(5_000);
      const text = new TextDecoder().decode((await pending).value);
      if (tick === 12) {
        expect(text).toContain('"upstreamReachable":false');
        expect(text).toContain('"stale":true');
        expect(text).toContain('"mode":"DEGRADED"');
      }
    }
    const stale = await snapshotRoute();
    expect(stale.status).toBe(200);
    expect(await stale.json()).toMatchObject({ balance: { equity: "1000" }, cache: { stale: true } });
    expect((await readyRoute()).status).toBe(503);
    expect(fetcher).toHaveBeenCalledTimes(2);
    await reader.cancel();
  });
});

describe("cached trading dashboard rendering", () => {
  it("removes cost/economic metrics and renders canonical current-experiment counters", () => {
    const s = fixture();
    s.tradingResults = { completed: 9, wins: 5, losses: 3, breakeven: 1, winRate: 5 / 9 };
    const html = renderToStaticMarkup(createElement(PortfolioPanel, { snapshot: s }));
    expect(html).not.toMatch(/AI cost|Net economic|Infrastructure cost/i);
    for (const text of ["Completed", "Wins", "Losses", "Win Rate", "55.6%", "Breakeven", "Gross PnL", "Net Trading PnL", "Fees"]) expect(html).toContain(text);
    expect(html).toContain(">9<");
    s.tradingResults = null;
    expect(renderToStaticMarkup(createElement(PortfolioPanel, { snapshot: s }))).toContain("unavailable");
  });

  it("renders low-impact freshness from real metadata, including failure and elapsed age", async () => {
    const c = cache(vi.fn(async () => Response.json(fixture())));
    await c.read();
    const metadata = c.metadata();
    const html = renderToStaticMarkup(createElement(CacheStatus, { cache: metadata, now: Date.now() + 34_000 }));
    for (const text of ["LOW IMPACT", "34s ago", "Poll: 60", metadata.sourceGeneratedAt!, "reachable", "CACHED"]) expect(html).toContain(text);
    expect(html).not.toContain("STALE");
    const stale = renderToStaticMarkup(createElement(CacheStatus, { cache: { ...metadata, stale: true, upstreamReachable: false }, now: Date.now() + 64_000 }));
    expect(stale).toContain("STALE");
    expect(stale).toContain("unreachable");
  });
});
