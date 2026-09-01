import { describe, expect, it } from "vitest";
import {
  createId,
  engineEnvSchema,
  evaluateRisk,
  type MarketSnapshot,
  type PaperFill,
  type TriggerEvent,
} from "@pm/core";
import { MemoryEventJournal } from "@pm/db";
import {
  type DiscoveryReport,
  type DiscoveredMarket,
  type PolymarketFeedHandlers,
  type ReferenceFeedHandlers,
  type ReferenceResolution,
} from "@pm/market-data";
import { EngineService } from "../apps/engine/src/engine.js";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { SseHub, createApiServer } from "../apps/engine/src/server.js";
import { feeSchedule, fill, order, safeRisk, snapshot } from "./fixtures.js";

function future() { return new Date(Date.now() + 60_000).toISOString(); }

function qualified(id: string, asset: string, up: string, down: string, endDate = new Date(Date.now() + 15 * 60_000).toISOString()): DiscoveredMarket {
  return {
    id, slug: `${asset.toLowerCase()}-updown-15m`, question: `${asset} Up or Down`, title: `${asset} Up or Down`,
    asset, endDate, active: true,
    tokens: [{ tokenId: up, outcome: "UP", price: "0.48" }, { tokenId: down, outcome: "DOWN", price: "0.51" }],
    feeSchedule, liquidityUsd: "200", spread: 0.02,
    qualification: { qualified: true, reasonCodes: ["QUALIFIED"], asset, crypto: true, binary: { safe: true, yesOutcome: "UP", noOutcome: "DOWN", reason: "QUALIFIED" }, duplicateOf: null },
    raw: {},
  };
}

function asRejected(market: DiscoveredMarket, reasonCodes: DiscoveredMarket["qualification"]["reasonCodes"]): DiscoveredMarket {
  return { ...market, qualification: { ...market.qualification, qualified: false, reasonCodes }, spread: 0.4, liquidityUsd: "1" };
}

function report(markets: DiscoveredMarket[]): DiscoveryReport {
  return {
    discovered: markets,
    qualified: markets.filter((item) => item.qualification.qualified),
    rejected: markets.filter((item) => !item.qualification.qualified).map((item) => ({
      id: item.id, slug: item.slug, title: item.title, reasonCodes: item.qualification.reasonCodes,
    })),
  };
}

class ScriptedMarketAdapter {
  readonly reports: DiscoveryReport[] = [];
  tokenIds: string[] = [];
  handlers: PolymarketFeedHandlers | null = null;
  private tokenWaiters: Array<() => void> = [];
  private streamClosed: (() => void) | null = null;
  queue(next: DiscoveryReport): void { this.reports.push(next); }
  registerManualToken(): void {}
  async discoverCryptoMarkets(): Promise<DiscoveryReport> {
    return this.reports.shift() ?? { discovered: [], qualified: [], rejected: [] };
  }
  setTokenIds(tokenIds: readonly string[]): { changed: boolean; previous: string[]; next: string[] } {
    const previous = [...this.tokenIds];
    const next = [...new Set(tokenIds.filter(Boolean))];
    const changed = previous.length !== next.length || previous.some((id) => !next.includes(id));
    this.tokenIds = next;
    if (next.length > 0) {
      const waiters = this.tokenWaiters.splice(0);
      for (const waiter of waiters) waiter();
    }
    if (changed) this.streamClosed?.();
    return { changed, previous, next };
  }
  async run(_tokenIds: readonly string[], handlers: PolymarketFeedHandlers, signal: AbortSignal): Promise<void> {
    this.handlers = handlers;
    while (!signal.aborted) {
      if (this.tokenIds.length === 0) {
        await handlers.onConnection({ feed: "polymarket", state: "STOPPED", at: new Date().toISOString(), reconnectCount: 0, message: "NO_QUALIFIED_TOKENS" });
        try {
          await new Promise<void>((resolve, reject) => {
            const finish = () => { cleanup(); resolve(); };
            const onAbort = () => { cleanup(); reject(signal.reason); };
            const cleanup = () => {
              signal.removeEventListener("abort", onAbort);
              this.tokenWaiters = this.tokenWaiters.filter((waiter) => waiter !== finish);
            };
            if (this.tokenIds.length > 0) { resolve(); return; }
            signal.addEventListener("abort", onAbort, { once: true });
            this.tokenWaiters.push(finish);
          });
        } catch { break; }
        continue;
      }
      await handlers.onConnection({ feed: "polymarket", state: "CONNECTED", at: new Date().toISOString(), reconnectCount: 0 });
      await new Promise<void>((resolve) => {
        this.streamClosed = () => resolve();
        signal.addEventListener("abort", () => resolve(), { once: true });
      });
      this.streamClosed = null;
    }
  }
  async emitSnapshot(next: MarketSnapshot): Promise<void> { await this.handlers?.onSnapshot(next); }
}

class ScriptedReferenceRegistry {
  readonly startCounts = new Map<string, number>();
  private readonly running = new Map<string, ReferenceFeedHandlers>();
  async resolve(asset: string): Promise<ReferenceResolution> {
    return { asset, supported: true, symbol: `${asset}USDT`, url: `wss://example/${asset.toLowerCase()}`, reason: "RESOLVED", status: "SUPPORTED" };
  }
  adapterFor(): null { return null; }
  async ensureRunning(asset: string, handlers: ReferenceFeedHandlers): Promise<"started" | "already"> {
    if (this.running.has(asset)) return "already";
    this.running.set(asset, handlers);
    this.startCounts.set(asset, (this.startCounts.get(asset) ?? 0) + 1);
    return "started";
  }
  stop(asset: string): boolean { return this.running.delete(asset); }
  runningAssets(): string[] { return [...this.running.keys()]; }
}

function paperFill(marketId: string, outcome: "UP" | "DOWN", price = "0.40"): PaperFill {
  return { ...fill(outcome, price, "10"), marketId, tokenId: `${marketId}-${outcome.toLowerCase()}` };
}

function pendingFeed() {
  return {
    registerManualToken: () => undefined,
    setTokenIds: (tokenIds: readonly string[]) => ({ changed: false, previous: [...tokenIds], next: [...tokenIds] }),
    run: async (_ids: unknown, _handlers: unknown, signal: AbortSignal) => new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve(), { once: true })),
  };
}

const logger = { error: () => undefined };
const looseLimits = {
  maxOrderSizeUsd: 25, maxMarketExposureUsd: 200, maxAssetExposureUsd: 200, maxTotalExposureUsd: 300,
  maxDirectionalExposureUsd: 80, maxUnhedgedExposureUsd: 80, maxUnhedgedDurationMs: 60_000, maxDailyPaperLossUsd: 50,
  maxDrawdownUsd: 100, minLiquidityUsd: 1, minExpectedEdge: 0, maxSpread: 1, minSignalConfidence: 0,
  staleFeedTimeoutMs: 15_000, maxExternalSignalsPerMinute: 30, strategyCooldownMs: 0, maxExecutionErrorsPerHour: 50,
};

describe("C experiment clock is explicit", () => {
  it("boots IDLE and does not auto-start the 48h experiment", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down" });
    const engine = new EngineService(env, journal, logger, { market: pendingFeed() as never });
    await engine.start();
    try {
      expect(engine.experimentManifest()?.status).toBe("IDLE");
      expect((await journal.queryEvents({ types: ["EXPERIMENT_STARTED"] })).length).toBe(0);
      expect((await engine.beginExperiment()).ok).toBe(true);
      expect(engine.experimentManifest()?.status).toBe("RUNNING");
    } finally {
      await engine.stop();
    }
  });

  it("preflight blocks a Grok-required start when the activation provider is noop", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down",
      GROK_ACTIVATION_PROVIDER: "noop", EXPERIMENT_REQUIRES_GROK: true,
    });
    const engine = new EngineService(env, journal, logger, { market: pendingFeed() as never });
    await engine.start();
    try {
      const result = await engine.beginExperiment();
      expect(result.ok).toBe(false);
      expect(result.blockers).toEqual(expect.arrayContaining(["GROK_ACTIVATION_NOOP"]));
      expect(engine.experimentManifest()?.status).toBe("IDLE");
    } finally {
      await engine.stop();
    }
  });
});

describe("A managed vs entry-qualified markets", () => {
  it("keeps an open-position market monitored after spread/liquidity disqualification and rotation", async () => {
    const market = new ScriptedMarketAdapter();
    const btc = qualified("btc-mkt", "BTC", "btc-up", "btc-down");
    market.queue(report([btc]));
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", GROK_ACTIVATION_PROVIDER: "noop",
      DISCOVERY_INTERVAL_MS: "3600000", MAX_QUALIFIED_MARKETS: "1",
    });
    const engine = new EngineService(env, journal, logger, { market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never });
    await engine.start();
    try {
      expect(engine.entryQualifiedIds()).toContain("btc-mkt");
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill(paperFill("btc-mkt", "UP"));
      market.queue({
        discovered: [asRejected(btc, ["SPREAD_TOO_WIDE", "INSUFFICIENT_LIQUIDITY"])],
        qualified: [],
        rejected: [{ id: btc.id, slug: btc.slug, title: btc.title, reasonCodes: ["SPREAD_TOO_WIDE", "INSUFFICIENT_LIQUIDITY"] }],
      });
      await engine.reconcileDiscovery();
      expect(engine.entryQualifiedIds()).not.toContain("btc-mkt");
      expect(engine.monitoredMarketIds()).toContain("btc-mkt");
      expect(engine.subscribedTokenIds()).toEqual(expect.arrayContaining(["btc-up", "btc-down"]));
      await market.emitSnapshot(snapshot({ marketId: "btc-mkt", tokenId: "btc-up", asset: "BTC" }));
      expect(engine.state.marketSnapshots.has("btc-up")).toBe(true);
      expect(evaluateRisk(safeRisk({ marketEntryQualified: false, intent: "OPEN_FIRST_LEG" }), looseLimits).vetoes).toContain("MARKET_NOT_ENTRY_QUALIFIED");
      expect(evaluateRisk(safeRisk({
        marketEntryQualified: false, intent: "HEDGE_SECOND_LEG", projectedUnhedgedExposureUsd: "0",
        projectedDirectionalExposureUsd: "0", projectedMarketExposureUsd: "8", projectedTotalExposureUsd: "8", projectedAssetExposureUsd: "8",
      }), looseLimits).vetoes).not.toContain("MARKET_NOT_ENTRY_QUALIFIED");

      const eth = qualified("eth-mkt", "ETH", "eth-up", "eth-down");
      market.queue({
        discovered: [asRejected(btc, ["SPREAD_TOO_WIDE"]), eth],
        qualified: [eth],
        rejected: [{ id: btc.id, slug: btc.slug, title: btc.title, reasonCodes: ["SPREAD_TOO_WIDE"] }],
      });
      await engine.reconcileDiscovery();
      expect(engine.monitoredMarketIds().sort()).toEqual(["btc-mkt", "eth-mkt"]);
      expect(engine.entryQualifiedIds()).toEqual(["eth-mkt"]);
      engine.ledger.positions.clear();
      market.queue(report([]));
      await engine.reconcileDiscovery();
      expect(engine.monitoredMarketIds()).not.toContain("btc-mkt");
    } finally {
      await engine.stop();
    }
  });
});

describe("B/D/R/T settlement, close policy, recovery, accelerated lifecycle", () => {
  it("settles from a public resolution fixture, waits at ENTRY_CLOSED, then completes", async () => {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const resolutions = new Map<string, Record<string, unknown>>();
    const market = new ScriptedMarketAdapter();
    const btc = qualified("btc-mkt", "BTC", "btc-up", "btc-down", new Date(nowMs + 5 * 60_000).toISOString());
    const eth = qualified("eth-mkt", "ETH", "eth-up", "eth-down", new Date(nowMs + 15 * 60_000).toISOString());
    market.queue(report([btc, eth]));
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", GROK_ACTIVATION_PROVIDER: "noop",
      EXPERIMENT_DURATION_HOURS: "48", EXPERIMENT_CLOSE_POLICY: "observe_until_settlement",
      SETTLEMENT_POLL_MS: "1", TELEMETRY_QUEUE_LIMIT: "8", DASHBOARD_SNAPSHOT_HZ: "4", ACTIVATION_MEMORY_LIMIT: "20",
      DISCOVERY_INTERVAL_MS: "3600000",
    });
    const engine = new EngineService(env, journal, logger, {
      market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
      now: () => new Date(nowMs),
      resolutionFetcher: async (id) => resolutions.get(id) ?? null,
    });
    await engine.start();
    try {
      expect(engine.experimentManifest()?.status).toBe("IDLE");
      expect((await engine.beginExperiment()).ok).toBe(true);
      await engine.applyObservedFill(paperFill("btc-mkt", "UP", "0.40"));
      await engine.restMakerForTest(order("MAKER", { marketId: "eth-mkt", tokenId: "eth-up" }), snapshot({ marketId: "eth-mkt", tokenId: "eth-up", asset: "ETH" }));
      expect(Number(engine.reservedCash())).toBeGreaterThan(0);

      for (let i = 0; i < 80; i += 1) {
        await market.emitSnapshot(snapshot({ marketId: "btc-mkt", tokenId: "btc-up", asset: "BTC", sequence: i + 1, eventId: createId("book") }));
      }
      expect(engine.telemetryStats().queued).toBeLessThanOrEqual(8);
      await new Promise((resolve) => setTimeout(resolve, 280));
      expect(engine.snapshotBroadcastCount()).toBeLessThan(10);

      market.queue({
        discovered: [asRejected(btc, ["SPREAD_TOO_WIDE"]), eth],
        qualified: [eth],
        rejected: [{ id: btc.id, slug: btc.slug, title: btc.title, reasonCodes: ["SPREAD_TOO_WIDE"] }],
      });
      await engine.reconcileDiscovery();
      expect(engine.monitoredMarketIds()).toContain("btc-mkt");

      nowMs += 6 * 60_000;
      resolutions.set("btc-mkt", {
        state: { closed: true, endDate: btc.endDate },
        resolution: { umaResolutionStatus: "resolved" },
        outcomes: { yes: { label: "Up", price: "1" }, no: { label: "Down", price: "0" } },
      });
      await engine.tickMaintenance();
      const settled = await journal.queryEvents({ types: ["POSITION_SETTLED", "POSITION_WIN", "MARKET_RESOLUTION_OBSERVED"] });
      expect(settled.some((event) => event.type === "MARKET_RESOLUTION_OBSERVED")).toBe(true);
      expect(settled.some((event) => event.type === "POSITION_SETTLED")).toBe(true);
      expect(settled.some((event) => event.type === "POSITION_WIN")).toBe(true);
      expect(engine.ledger.positions.has("btc-mkt")).toBe(false);

      await engine.applyObservedFill(paperFill("eth-mkt", "UP", "0.45"));
      nowMs += 48 * 3_600_000;
      await engine.tickMaintenance();
      expect(engine.experimentManifest()?.status).toBe("ENTRY_CLOSED");
      expect((await journal.queryEvents({ types: ["EXPERIMENT_REPORT_GENERATED"] })).length).toBe(0);

      nowMs += 16 * 60_000;
      resolutions.set("eth-mkt", {
        state: { closed: true },
        resolution: { umaResolutionStatus: "settled" },
        outcomes: { yes: { label: "Up", price: "0" }, no: { label: "Down", price: "1" } },
      });
      await engine.tickMaintenance();
      expect(engine.experimentManifest()?.status).toBe("COMPLETED");
      const reports = await journal.queryEvents({ types: ["EXPERIMENT_REPORT_GENERATED", "EXPERIMENT_COMPLETED"] });
      expect(reports.some((event) => event.type === "EXPERIMENT_COMPLETED")).toBe(true);
      expect(reports.some((event) => event.type === "EXPERIMENT_REPORT_GENERATED")).toBe(true);
      expect(engine.activationFootprint().latency).toBeLessThanOrEqual(20);
    } finally {
      await engine.stop();
    }
  });

  it("mark_to_market completes with unresolved positions and does not fabricate wins", async () => {
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down",
      EXPERIMENT_CLOSE_POLICY: "mark_to_market", EXPERIMENT_DURATION_HOURS: "48",
    });
    const engine = new EngineService(env, journal, logger, { market: pendingFeed() as never, now: () => new Date(nowMs) });
    await engine.start();
    try {
      await engine.beginExperiment();
      await engine.applyObservedFill(paperFill("btc-15m", "UP", "0.40"));
      nowMs += 48 * 3_600_000 + 1;
      await engine.tickMaintenance();
      expect(engine.experimentManifest()?.status).toBe("COMPLETED");
      expect(engine.experimentManifest()?.unresolvedPositionCount).toBeGreaterThan(0);
      expect((await journal.queryEvents({ types: ["POSITION_WIN"] })).length).toBe(0);
      const mtm = await journal.queryEvents({ types: ["PNL_SNAPSHOT"] });
      expect(mtm.some((event) => (event.payload as { markToMarket?: boolean }).markToMarket === true)).toBe(true);
    } finally {
      await engine.stop();
    }
  });

  it("restores open position, resting maker, ENTRY_CLOSED experiment, and activation latency", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down",
      EXPERIMENT_DURATION_HOURS: "48", EXPERIMENT_CLOSE_POLICY: "observe_until_settlement",
    });
    let nowMs = Date.parse("2026-09-01T00:00:00.000Z");
    const first = new EngineService(env, journal, logger, { market: pendingFeed() as never, now: () => new Date(nowMs) });
    await first.start();
    await first.beginExperiment();
    await first.applyObservedFill(paperFill("btc-15m", "UP", "0.40"));
    await first.restMakerForTest(order("MAKER"), snapshot());
    const trigger: TriggerEvent = {
      id: createId("trigger"), traceId: createId("trace"), marketId: "btc-15m", asset: "BTC", source: "polymarket",
      kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: "x", evidence: {}, sourceEventIds: ["e"], evidenceIds: ["e"],
      createdAt: new Date(nowMs).toISOString(), expiresAt: new Date(nowMs + 60_000).toISOString(),
    };
    await first.registerTriggerForResearch(trigger);
    nowMs += 48 * 3_600_000 + 1;
    await first.tickMaintenance();
    expect(first.experimentManifest()?.status).toBe("ENTRY_CLOSED");
    const reserved = first.reservedCash();
    const experimentId = first.experimentManifest()?.id;
    await first.stop();

    const second = new EngineService(env, journal, logger, { market: pendingFeed() as never, now: () => new Date(nowMs) });
    await second.start();
    try {
      expect(second.experimentManifest()?.id).toBe(experimentId);
      expect(second.experimentManifest()?.status).toBe("ENTRY_CLOSED");
      expect(second.ledger.positions.get("btc-15m")?.upQuantity).toBe("10");
      expect(second.restingOrderCount()).toBeGreaterThan(0);
      expect(second.reservedCash()).toBe(reserved);
      expect(second.state.activationLatency.length).toBeGreaterThan(0);
    } finally {
      await second.stop();
    }
  });
});

describe("J maker reservation through the engine", () => {
  it("reserves cash for a maker and does not double-reserve after recovery", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down" });
    const first = new EngineService(env, journal, logger, { market: pendingFeed() as never });
    await first.start();
    await first.restMakerForTest(order("MAKER", { quantity: "10", requestedPrice: "0.47" }), snapshot());
    expect(Number(first.reservedCash())).toBeCloseTo(4.7, 8);
    await expect(first.restMakerForTest(order("MAKER", { quantity: "2000", requestedPrice: "0.90" }), snapshot())).rejects.toThrow("INSUFFICIENT_CASH");
    await first.stop();
    const second = new EngineService(env, journal, logger, { market: pendingFeed() as never });
    await second.start();
    try {
      expect(Number(second.reservedCash())).toBeCloseTo(4.7, 8);
      await expect(second.restMakerForTest(order("MAKER", { quantity: "2000", requestedPrice: "0.90" }), snapshot())).rejects.toThrow("INSUFFICIENT_CASH");
    } finally {
      await second.stop();
    }
  });
});

describe("O per-asset reference retry isolation", () => {
  it("lets ETH recover without postponing SOL, and removing SOL cancels only SOL", async () => {
    const probes: Record<string, number> = { ETH: 0, SOL: 0 };
    const running = new Set<string>();
    const adapters = new Set<string>();
    const registry = {
      async resolve(asset: string): Promise<ReferenceResolution> {
        probes[asset] = (probes[asset] ?? 0) + 1;
        if (asset === "ETH" && probes.ETH >= 2) {
          return { asset, supported: true, status: "SUPPORTED", symbol: "ETHUSDT", url: "wss://example/eth", reason: "RESOLVED" };
        }
        if (asset === "SOL" && probes.SOL >= 4) {
          return { asset, supported: true, status: "SUPPORTED", symbol: "SOLUSDT", url: "wss://example/sol", reason: "RESOLVED" };
        }
        return {
          asset, supported: false, status: "TRANSIENT_FAILURE", symbol: `${asset}USDT`, url: null,
          reason: "REFERENCE_PROBE_TRANSIENT", detail: "timeout", retryAfterMs: asset === "ETH" ? 50 : 5_000,
        };
      },
      adapterFor(asset: string) { return adapters.has(asset) ? {} : null; },
      async ensureRunning(asset: string) {
        if (running.has(asset)) return "already" as const;
        adapters.add(asset);
        running.add(asset);
        return "started" as const;
      },
      stop(asset: string) { running.delete(asset); adapters.delete(asset); return true; },
      runningAssets: () => [...running],
      assets: () => [...adapters],
    };
    const market = new ScriptedMarketAdapter();
    market.queue(report([qualified("eth-mkt", "ETH", "eth-up", "eth-down"), qualified("sol-mkt", "SOL", "sol-up", "sol-down")]));
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", DISCOVERY_INTERVAL_MS: "3600000" });
    const engine = new EngineService(env, journal, logger, { market: market as never, referenceRegistry: registry as never });
    await engine.start();
    try {
      const solDeadline = engine.referenceRetryDeadline("SOL");
      expect(solDeadline).toBeTruthy();
      await new Promise((resolve) => setTimeout(resolve, 120));
      expect(engine.runningReferenceAssets()).toEqual(["ETH"]);
      expect(engine.referenceRetryDeadline("SOL")).toBe(solDeadline);
      expect(registry.assets()).toEqual(["ETH"]);
      market.queue(report([qualified("eth-mkt", "ETH", "eth-up", "eth-down")]));
      await engine.reconcileDiscovery();
      expect(engine.referenceRetryDeadline("SOL")).toBeUndefined();
      expect(engine.runningReferenceAssets()).toEqual(["ETH"]);
    } finally {
      await engine.stop();
    }
  });
});

describe("Q engine read APIs are credentialed", () => {
  it("keeps /health public and requires the engine read token for dashboard/history/events", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({ NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down" });
    const engine = new EngineService(env, journal, logger, { market: pendingFeed() as never });
    const server = await createApiServer({
      state: engine.state, journal, metrics: new EngineMetrics(), sse: new SseHub(),
      dashboardOrigin: "http://localhost:3000", logger: false,
      mcp: { bearerTokens: "fair_probability_analyst:test-token", allowedOrigins: "https://grok.com", rateLimitPerMinute: 30 },
      submitGrokSignal: async () => ({ status: "ignored" }),
      engineReadToken: "read-secret-token",
      nodeEnv: "test",
    });
    try {
      expect((await server.inject({ method: "GET", url: "/health" })).statusCode).toBe(200);
      expect((await server.inject({ method: "GET", url: "/api/dashboard" })).statusCode).toBe(401);
      expect((await server.inject({ method: "GET", url: "/events" })).statusCode).toBe(401);
      expect((await server.inject({ method: "GET", url: "/api/dashboard", headers: { authorization: "Bearer read-secret-token" } })).statusCode).toBe(200);
    } finally {
      await server.close();
    }
  });
});

describe("N activation memory is bounded", () => {
  it("prunes activation latency to the configured window", async () => {
    const journal = new MemoryEventJournal();
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", POLYMARKET_TOKEN_IDS: "up,down",
      ACTIVATION_MEMORY_LIMIT: "3",
    });
    const engine = new EngineService(env, journal, logger, { market: pendingFeed() as never });
    await engine.start();
    try {
      for (let i = 0; i < 8; i += 1) {
        await engine.registerTriggerForResearch({
          id: createId("trigger"), traceId: createId("trace"), marketId: `m-${i}`, asset: "BTC", source: "polymarket",
          kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: `d-${i}`, evidence: {}, sourceEventIds: ["e"], evidenceIds: ["e"],
          createdAt: new Date().toISOString(), expiresAt: future(),
        });
      }
      expect(engine.activationFootprint().latency).toBeLessThanOrEqual(3);
    } finally {
      await engine.stop();
    }
  });
});
