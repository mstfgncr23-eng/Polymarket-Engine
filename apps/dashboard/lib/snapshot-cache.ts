import { engineInternalUrl, engineReadHeaders } from "./engine";
import type { CacheMetadata, DashboardSnapshot } from "./types";

export function pollConfig(env: NodeJS.ProcessEnv = process.env) {
  const interval = (key: string, fallback: number) => {
    const value = Number(env[key]);
    return Number.isSafeInteger(value) && value >= 100 && value <= 2_147_483_647 ? value : fallback;
  };
  return {
    idle: interval("DASHBOARD_POLL_IDLE_MS", 10_000),
    active: interval("DASHBOARD_POLL_ACTIVE_MS", 60_000),
    degraded: interval("DASHBOARD_POLL_DEGRADED_MS", 120_000),
    timeout: interval("DASHBOARD_UPSTREAM_TIMEOUT_MS", 3_000),
  };
}

// Reject errors/partial payloads rather than replacing usable data with invented values.
export function validSnapshot(value: unknown): value is DashboardSnapshot {
  if (!value || typeof value !== "object") return false;
  const s = value as DashboardSnapshot;
  return s.paperMode === true && Number.isFinite(Date.parse(s.generatedAt))
    && !!s.engine && typeof s.engine.ready === "boolean" && typeof s.engine.draining === "boolean"
    && !!s.health && ["UP", "DOWN"].includes(s.health.database)
    && !!s.balance && typeof s.balance.equity === "string"
    && Array.isArray(s.positions) && Array.isArray(s.decisionFeed) && Array.isArray(s.equityHistory)
    && Array.isArray(s.scanner) && Array.isArray(s.recentTriggers)
    && !!s.latestAgents && !!s.grokActivity && !!s.health.polymarketWs && !!s.health.btcWs;
}

export class SnapshotCache {
  private snapshot: DashboardSnapshot | null = null;
  private syncedAt: number | null = null;
  private reachable = false;
  private started = false;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private abort: AbortController | undefined;
  private inFlight: Promise<void> | undefined;
  private initial: Promise<void> = Promise.resolve();
  private readonly subscribers = new Set<(snapshot: DashboardSnapshot) => void>();
  private readonly config;

  constructor(private readonly fetchImpl: typeof fetch = fetch, env: NodeJS.ProcessEnv = process.env) {
    this.config = pollConfig(env);
  }

  metadata(): CacheMetadata {
    const s = this.snapshot;
    const degraded = !this.reachable || !s || s.engine.draining || !s.engine.ready
      || s.engine.alive === false || s.health.engineAlive === false || s.health.database === "DOWN"
      || s.engine.journalDegraded || s.health.journalDegraded
      || (s.engine.overloadState !== undefined && s.engine.overloadState !== "NORMAL")
      || (s.health.overloadState !== undefined && s.health.overloadState !== "NORMAL");
    const mode = degraded ? "DEGRADED" : ["RUNNING", "ENTRY_CLOSED"].includes(s.engine.experimentStatus ?? "") ? "LOW_IMPACT" : "IDLE";
    const pollIntervalMs = mode === "DEGRADED" ? this.config.degraded : mode === "LOW_IMPACT" ? this.config.active : this.config.idle;
    const cacheAgeMs = this.syncedAt === null ? null : Math.max(0, Date.now() - this.syncedAt);
    return {
      lastUpstreamSync: this.syncedAt === null ? null : new Date(this.syncedAt).toISOString(),
      sourceGeneratedAt: s?.generatedAt ?? null, cacheAgeMs, mode, pollIntervalMs,
      staleAfterMs: pollIntervalMs + this.config.timeout,
      upstreamReachable: this.reachable,
      stale: !this.reachable || cacheAgeMs === null || cacheAgeMs > pollIntervalMs + this.config.timeout,
    };
  }

  peek(): DashboardSnapshot | null {
    return this.snapshot ? { ...this.snapshot, cache: this.metadata() } : null;
  }

  start(): void {
    if (this.started) return;
    this.started = true;
    this.initial = this.poll();
  }

  async read(): Promise<DashboardSnapshot | null> {
    this.start();
    if (!this.snapshot) await this.initial;
    return this.peek();
  }

  subscribe(listener: (snapshot: DashboardSnapshot) => void): () => void {
    this.subscribers.add(listener);
    this.start();
    return () => { this.subscribers.delete(listener); };
  }

  stop(): void {
    this.started = false;
    clearTimeout(this.timer);
    this.abort?.abort();
    this.subscribers.clear();
  }

  private poll(): Promise<void> {
    if (this.inFlight) return this.inFlight;
    const abort = new AbortController();
    this.abort = abort;
    let deadline: ReturnType<typeof setTimeout>;
    const timeout = new Promise<void>((resolve) => {
      deadline = setTimeout(() => {
        this.reachable = false;
        abort.abort();
        resolve();
      }, this.config.timeout);
    });
    // Hold the flight until fetch/body consumption settles, even if a broken adapter ignores abort.
    const flight = Promise.resolve().then(async () => {
      try {
        const response = await this.fetchImpl(`${engineInternalUrl().replace(/\/$/, "")}/api/dashboard`, {
          method: "GET", headers: engineReadHeaders(), cache: "no-store", signal: abort.signal,
          redirect: "error",
        });
        if (!response.ok) throw new Error("upstream_unavailable");
        const value: unknown = await response.json();
        if (!validSnapshot(value)) throw new Error("invalid_snapshot");
        if (abort.signal.aborted || !this.started) return;
        this.snapshot = value;
        this.syncedAt = Date.now();
        this.reachable = true;
        for (const listener of this.subscribers) {
          try { listener(this.peek()!); } catch { /* Local browser cannot disrupt polling. */ }
        }
      } catch {
        this.reachable = false;
      } finally {
        clearTimeout(deadline!);
        this.inFlight = undefined;
        if (this.started) {
          this.timer = setTimeout(() => { void this.poll(); }, this.metadata().pollIntervalMs);
          this.timer.unref?.();
        }
      }
    });
    this.inFlight = flight;
    return Promise.race([flight, timeout]);
  }
}

const key = Symbol.for("pm.dashboard.snapshot-cache.v1");
const processCache = globalThis as typeof globalThis & { [key]?: SnapshotCache };

export function dashboardCache(): SnapshotCache {
  return processCache[key] ??= new SnapshotCache();
}

export function stopDashboardCache(): void {
  processCache[key]?.stop();
  delete processCache[key];
}

export async function initialSnapshot(): Promise<DashboardSnapshot | null> {
  return dashboardCache().read();
}
