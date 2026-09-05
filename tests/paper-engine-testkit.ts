import {
  createId,
  engineEnvSchema,
  type EngineEnv,
  type MarketSnapshot,
  type PaperFill,
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
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { feeSchedule, fill, snapshot } from "./fixtures.js";

export const GROK_ROLES = [
  "market_microstructure_scanner", "crypto_market_scanner", "news_x_intelligence_scanner",
  "fair_probability_analyst", "risk_hedge_analyst", "strategy_supervisor",
] as const;

export function grokPaperEnv() {
  return {
    GROK_ACTIVATION_PROVIDER: "slack" as const,
    SLACK_BOT_TOKEN: "xoxb-test-token",
    SLACK_CHANNEL_ID: "C-TEST",
    GROK_MCP_BEARER_TOKENS: GROK_ROLES.map((role) => `${role}:tok-${role}`).join(","),
  };
}

export function qualified(
  id: string,
  asset: string,
  up: string,
  down: string,
  endDate = new Date(Date.now() + 15 * 60_000).toISOString(),
): DiscoveredMarket {
  return {
    id, slug: `${asset.toLowerCase()}-updown-15m`, question: `${asset} Up or Down`, title: `${asset} Up or Down`,
    asset, endDate, active: true,
    tokens: [{ tokenId: up, outcome: "UP", price: "0.48" }, { tokenId: down, outcome: "DOWN", price: "0.51" }],
    feeSchedule, liquidityUsd: "200", spread: 0.02,
    qualification: { qualified: true, reasonCodes: ["QUALIFIED"], asset, crypto: true, binary: { safe: true, yesOutcome: "UP", noOutcome: "DOWN", reason: "QUALIFIED" }, duplicateOf: null },
    raw: {},
  };
}

export function discoveryReport(markets: DiscoveredMarket[]): DiscoveryReport {
  return {
    discovered: markets,
    qualified: markets.filter((item) => item.qualification.qualified),
    rejected: markets.filter((item) => !item.qualification.qualified).map((item) => ({
      id: item.id, slug: item.slug, title: item.title, reasonCodes: item.qualification.reasonCodes,
    })),
  };
}

export class ScriptedMarketAdapter {
  readonly reports: DiscoveryReport[] = [];
  tokenIds: string[] = [];
  handlers: PolymarketFeedHandlers | null = null;
  private readonly sync = new Map<string, { state: "AWAITING_BOOK" | "SYNCED" | "GAPPED"; epoch: number }>();
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
  async emitSnapshot(next: MarketSnapshot): Promise<void> {
    if (!this.sync.has(next.tokenId)) this.sync.set(next.tokenId, { state: "SYNCED", epoch: 1 });
    await this.handlers?.onSnapshot(next);
  }
  isTokenSynchronized(tokenId: string): boolean {
    return (this.sync.get(tokenId)?.state ?? "SYNCED") === "SYNCED";
  }
  tokenEpoch(tokenId: string): number { return this.sync.get(tokenId)?.epoch ?? 1; }
  tokenSyncState(tokenId: string): "AWAITING_BOOK" | "SYNCED" | "GAPPED" {
    return this.sync.get(tokenId)?.state ?? "SYNCED";
  }
  unsynchronizedTokenCount(): number {
    return [...this.sync.values()].filter((item) => item.state !== "SYNCED").length;
  }
  markTokenSync(tokenId: string, state: "AWAITING_BOOK" | "SYNCED" | "GAPPED", epoch?: number): void {
    const current = this.sync.get(tokenId);
    this.sync.set(tokenId, { state, epoch: epoch ?? (current?.epoch ?? 1) + (state === "GAPPED" || state === "AWAITING_BOOK" ? 1 : 0) });
    if (state === "GAPPED") {
      void this.handlers?.onGap?.({
        reason: "explicit_resubscribe",
        tokenCount: 1,
        epoch: this.sync.get(tokenId)?.epoch ?? 1,
        tokenIds: [tokenId],
      });
    }
  }

  async waitForHandlers(timeoutMs = 1_500): Promise<void> {
    const started = Date.now();
    while (!this.handlers) {
      if (Date.now() - started > timeoutMs) throw new Error("scripted market handlers not attached");
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
  }

  emitGap(tokenIds: readonly string[], reason: "explicit_resubscribe" | "reconnect" | "count_limit" = "explicit_resubscribe"): void {
    for (const tokenId of tokenIds) {
      const current = this.sync.get(tokenId);
      this.sync.set(tokenId, { state: "GAPPED", epoch: (current?.epoch ?? 1) + 1 });
    }
    void this.handlers?.onGap?.({
      reason,
      tokenCount: tokenIds.length,
      epoch: tokenIds[0] ? (this.sync.get(tokenIds[0])?.epoch ?? 1) : 0,
      tokenIds: [...tokenIds],
    });
  }
}

export type ScriptedEnsureOutcome = "started" | "already" | "transient" | "unsupported" | "stopped" | "throw";

export class ScriptedReferenceRegistry {
  readonly startCounts = new Map<string, number>();
  readonly ensureCalls = new Map<string, number>();
  readonly maxConcurrentEnsure = { value: 0 };
  private ensureInFlight = 0;
  private readonly queuedOutcomes = new Map<string, ScriptedEnsureOutcome[]>();
  private readonly running = new Map<string, ReferenceFeedHandlers>();
  async resolve(asset: string): Promise<ReferenceResolution> {
    return { asset, supported: true, symbol: `${asset}USDT`, url: `wss://example/${asset.toLowerCase()}`, reason: "RESOLVED", status: "SUPPORTED" };
  }
  adapterFor(): null { return null; }
  queueEnsureOutcome(asset: string, outcome: ScriptedEnsureOutcome): void {
    const queued = this.queuedOutcomes.get(asset) ?? [];
    queued.push(outcome);
    this.queuedOutcomes.set(asset, queued);
  }
  async ensureRunning(
    asset: string,
    handlers: ReferenceFeedHandlers,
  ): Promise<"started" | "already" | "transient" | "unsupported" | "stopped"> {
    this.ensureInFlight += 1;
    this.maxConcurrentEnsure.value = Math.max(this.maxConcurrentEnsure.value, this.ensureInFlight);
    this.ensureCalls.set(asset, (this.ensureCalls.get(asset) ?? 0) + 1);
    try {
      const queued = this.queuedOutcomes.get(asset) ?? [];
      const next = queued.shift() ?? (this.running.has(asset) ? "already" : "started");
      if (next === "throw") throw new Error("REFERENCE_ENSURE_THROW");
      if (next === "started") {
        this.running.set(asset, handlers);
        this.startCounts.set(asset, (this.startCounts.get(asset) ?? 0) + 1);
      } else if (next === "already" && !this.running.has(asset)) {
        this.running.set(asset, handlers);
      }
      return next;
    } finally {
      this.ensureInFlight = Math.max(0, this.ensureInFlight - 1);
    }
  }
  readonly stopCounts = new Map<string, number>();
  stop(asset: string): boolean {
    this.stopCounts.set(asset, (this.stopCounts.get(asset) ?? 0) + 1);
    return this.running.delete(asset);
  }
  runningAssets(): string[] { return [...this.running.keys()]; }
}

export function paperFill(marketId: string, outcome: "UP" | "DOWN", price = "0.40", extra: Partial<PaperFill> = {}): PaperFill {
  return { ...fill(outcome, price, "10"), marketId, tokenId: `${marketId}-${outcome.toLowerCase()}`, ...extra };
}

export function qualifiedPaperEnv(overrides: Record<string, string | undefined> = {}): EngineEnv {
  return engineEnvSchema.parse({
    NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper",
    ...grokPaperEnv(),
    DISCOVERY_INTERVAL_MS: "3600000",
    ...overrides,
  });
}

export async function startQualifiedEngine(overrides: Record<string, string | undefined> = {}, now?: () => Date) {
  const market = new ScriptedMarketAdapter();
  const btc = qualified("btc-mkt", "BTC", "btc-up", "btc-down");
  market.queue(discoveryReport([btc]));
  const journal = new MemoryEventJournal();
  const env = qualifiedPaperEnv(overrides);
  const referenceRegistry = new ScriptedReferenceRegistry();
  const engine = new EngineService(env, journal, { error: () => undefined }, {
    market: market as never,
    referenceRegistry: referenceRegistry as never,
    activation: new NoopGrokActivationBridge(),
    now,
  });
  await engine.start();
  return { engine, journal, market, env, marketId: btc.id, referenceRegistry };
}

export function book(overrides: Partial<MarketSnapshot> = {}): MarketSnapshot {
  return snapshot(overrides);
}

export { createId };
