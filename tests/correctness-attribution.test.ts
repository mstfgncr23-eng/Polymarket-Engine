import { describe, expect, it } from "vitest";
import {
  computePaperExposures,
  createId,
  engineEnvSchema,
  evaluateRisk,
  positionSchema,
  qualifyMarket,
  renderExperimentSummaryMarkdown,
  riskLimitsSchema,
  type Position,
} from "@pm/core";
import { MemoryEventJournal, createJournalEvent } from "@pm/db";
import {
  ReferenceFeedRegistry,
  classifyBinanceExchangeInfo,
  type DiscoveredMarket,
  type DiscoveryReport,
  type PolymarketFeedHandlers,
  type WebSocketLike,
} from "@pm/market-data";
import { EngineService } from "../apps/engine/src/engine.js";
import { buildExperimentSummary } from "../apps/engine/src/report.js";
import { fill, now, safeRisk } from "./fixtures.js";

function fee() {
  return { rate: "0.07", exponent: 1, takerOnly: true, rebateRate: "0", source: "market_fee_schedule" as const, effectiveAt: new Date().toISOString() };
}

function marketInput(overrides: Record<string, unknown> = {}) {
  return {
    id: "m1", slug: "link-updown-15m-1", title: "LINK Up or Down", tags: ["crypto"],
    active: true, closed: false, acceptingOrders: true,
    endDate: new Date(Date.now() + 10 * 60_000).toISOString(),
    yesTokenId: "yes-1", noTokenId: "no-1", yesPrice: "0.48", noPrice: "0.51",
    liquidityUsd: "200", spread: 0.02, feeSchedule: fee(), feesUnknown: false, ...overrides,
  };
}

function position(marketId: string, notionalUsd: string): Position {
  return positionSchema.parse({
    id: createId("pos"), marketId, upQuantity: "10", downQuantity: "0",
    averageUpCost: String(Number(notionalUsd) / 10), averageDownCost: "0",
    pairedQuantity: "0", unpairedOutcome: "UP", unpairedQuantity: "10",
    combinedPairCost: "0", totalFees: "0", totalSlippage: "0",
    openedAt: now.toISOString(), updatedAt: now.toISOString(),
  });
}

function dummySocket(): WebSocketLike {
  const socket = new EventTarget() as EventTarget & { readyState: number; close: (code?: number, reason?: string) => void };
  socket.readyState = 0;
  socket.close = () => { socket.dispatchEvent(new Event("close")); };
  return socket;
}

function registry(
  probe: (symbol: string) => Promise<{ kind: "supported" | "unsupported_definitive" | "transient_failure"; detail: string }>,
  negativeCacheTtlMs = 0,
) {
  return new ReferenceFeedRegistry(
    "wss://example.invalid/ws",
    "https://example.invalid",
    15_000,
    dummySocket,
    probe,
    { negativeCacheTtlMs },
  );
}

function qualified(id: string, asset: string, up: string, down: string): DiscoveredMarket {
  return {
    id, slug: `${asset.toLowerCase()}-updown-15m`, question: `${asset} Up or Down`, title: `${asset} Up or Down`,
    asset, endDate: new Date(Date.now() + 15 * 60_000).toISOString(), active: true,
    tokens: [{ tokenId: up, outcome: "UP", price: "0.48" }, { tokenId: down, outcome: "DOWN", price: "0.51" }],
    feeSchedule: fee(), liquidityUsd: "200", spread: 0.02,
    qualification: { qualified: true, reasonCodes: ["QUALIFIED"], asset, crypto: true, binary: { safe: true, yesOutcome: "UP", noOutcome: "DOWN", reason: "QUALIFIED" }, duplicateOf: null },
    raw: {},
  };
}

class OneShotMarketAdapter {
  tokenIds: string[] = [];
  constructor(private readonly report: DiscoveryReport) {}
  registerManualToken(): void { /* unused */ }
  async discoverCryptoMarkets(): Promise<DiscoveryReport> { return this.report; }
  setTokenIds(tokenIds: readonly string[]): { changed: boolean; previous: string[]; next: string[] } {
    const previous = [...this.tokenIds];
    const next = [...tokenIds];
    this.tokenIds = next;
    return { changed: true, previous, next };
  }
  async run(_tokenIds: readonly string[], handlers: PolymarketFeedHandlers, signal: AbortSignal): Promise<void> {
    await handlers.onConnection({ feed: "polymarket", state: "CONNECTED", at: new Date().toISOString(), reconnectCount: 0 });
    await new Promise<void>((resolve) => {
      if (signal.aborted) { resolve(); return; }
      signal.addEventListener("abort", () => resolve(), { once: true });
    });
  }
}

describe("transient reference probe must not become permanently unsupported", () => {
  it("temporary probe timeout then recovery", async () => {
    const calls: string[] = [];
    const feeds = registry(async (symbol) => {
      calls.push(symbol);
      if (calls.length === 1) return { kind: "transient_failure", detail: "timeout" };
      return { kind: "supported", detail: "trading" };
    });
    const first = await feeds.resolve("ETH");
    expect(first.status).toBe("TRANSIENT_FAILURE");
    expect(first.supported).toBe(false);
    expect(first.reason).toBe("REFERENCE_PROBE_TRANSIENT");
    expect(first.detail).toBe("timeout");
    const second = await feeds.resolve("ETH");
    expect(second.status).toBe("SUPPORTED");
    expect(second.supported).toBe(true);
    expect(second.symbol).toBe("ETHUSDT");
    expect(calls).toEqual(["ETHUSDT", "ETHUSDT"]);
  });

  it("temporary HTTP 5xx then recovery", async () => {
    const sequence = [
      classifyBinanceExchangeInfo({ ok: false, status: 503, body: null, symbol: "ETHUSDT" }),
      classifyBinanceExchangeInfo({ ok: true, status: 200, body: { symbols: [{ symbol: "ETHUSDT", status: "TRADING" }] }, symbol: "ETHUSDT" }),
    ];
    expect(sequence[0]).toMatchObject({ kind: "transient_failure", detail: "http_503" });
    const feeds = registry(async () => sequence.shift() ?? { kind: "supported", detail: "trading" });
    expect((await feeds.resolve("ETH")).status).toBe("TRANSIENT_FAILURE");
    expect((await feeds.resolve("ETH")).status).toBe("SUPPORTED");
  });

  it("true unsupported symbol remains unsupported", async () => {
    let probes = 0;
    const feeds = registry(async () => {
      probes += 1;
      return { kind: "unsupported_definitive", detail: "symbol_not_listed" };
    });
    const first = await feeds.resolve("NOTAREAL");
    const second = await feeds.resolve("NOTAREAL");
    expect(first.status).toBe("UNSUPPORTED_DEFINITIVE");
    expect(second.status).toBe("UNSUPPORTED_DEFINITIVE");
    expect(second.reason).toBe("REFERENCE_UNSUPPORTED");
    expect(probes).toBe(1);
    expect(classifyBinanceExchangeInfo({
      ok: false, status: 400, body: { code: -1121 }, symbol: "ZZZZUSDT",
    }).kind).toBe("unsupported_definitive");
    expect(classifyBinanceExchangeInfo({
      ok: true, status: 200, body: { symbols: [] }, symbol: "ZZZZUSDT",
    }).kind).toBe("unsupported_definitive");
  });

  it("transient failure never permits reference-dependent paper execution", async () => {
    const blocked = evaluateRisk(safeRisk({
      referenceRequired: true, referenceFeedAt: null, asset: "ETH", btcFeedAt: now,
    }), riskLimitsSchema.parse({}));
    expect(blocked.accepted).toBe(false);
    expect(blocked.vetoes).toContain("REFERENCE_FEED_STALE");
    expect(blocked.vetoes).not.toContain("BTC_FEED_STALE");

    const recovered = evaluateRisk(safeRisk({
      referenceRequired: true, referenceFeedAt: now, asset: "ETH", btcFeedAt: now,
    }), riskLimitsSchema.parse({}));
    expect(recovered.vetoes).not.toContain("REFERENCE_FEED_STALE");

    const journal = new MemoryEventJournal();
    const market = new OneShotMarketAdapter({
      discovered: [qualified("eth-mkt", "ETH", "eth-up", "eth-down")],
      qualified: [qualified("eth-mkt", "ETH", "eth-up", "eth-down")],
      rejected: [],
    });
    const feeds = registry(async () => ({ kind: "transient_failure", detail: "timeout" }));
    const env = engineEnvSchema.parse({
      NODE_ENV: "test", JOURNAL_MODE: "memory", TRADING_MODE: "paper", GROK_ACTIVATION_PROVIDER: "noop",
      DISCOVERY_INTERVAL_MS: "3600000",
    });
    const engine = new EngineService(env, journal, { error: () => undefined }, {
      market: market as never, referenceRegistry: feeds,
    });
    await engine.start();
    try {
      const events = await journal.readAll();
      expect(events.some((event) => event.type === "REFERENCE_PROBE_TRANSIENT")).toBe(true);
      expect(events.some((event) => event.type === "REFERENCE_UNSUPPORTED")).toBe(false);
      expect(events.some((event) => event.type === "REFERENCE_FEED_STARTED")).toBe(false);
      expect(events.some((event) => event.type === "PAPER_FILL")).toBe(false);
      expect(engine.runningReferenceAssets()).toEqual([]);
      expect(engine.state.ready).toBe(true);
    } finally {
      await engine.stop();
    }
  });
});

describe("unknown underlying asset must not qualify in dynamic discovery", () => {
  it("directional crypto market with unknown asset is rejected", () => {
    const result = qualifyMarket(marketInput({
      slug: "up-or-down-now", title: "Up or Down", tags: ["crypto"],
    }), new Map());
    expect(result.crypto).toBe(true);
    expect(result.asset).toBeNull();
    expect(result.qualified).toBe(false);
    expect(result.reasonCodes).toContain("UNKNOWN_UNDERLYING_ASSET");
  });

  it("known dynamic asset still qualifies", () => {
    const link = qualifyMarket(marketInput(), new Map());
    expect(link.qualified).toBe(true);
    expect(link.asset).toBe("LINK");
    expect(link.reasonCodes).toEqual(["QUALIFIED"]);
  });

  it("two markets of the same known asset aggregate under the same asset risk", () => {
    const positions = [position("btc-a", "50"), position("btc-b", "80")];
    const known = new Map<string, string | null>([["btc-a", "BTC"], ["btc-b", "BTC"]]);
    const combined = computePaperExposures(positions, "btc-a", known);
    expect(combined.market).toBe("50");
    expect(combined.asset).toBe("130");

    const unknown = computePaperExposures(positions, "btc-a", new Map());
    expect(unknown.market).toBe("50");
    expect(unknown.asset).toBe("0");

    const limits = riskLimitsSchema.parse({ maxAssetExposureUsd: 100, maxMarketExposureUsd: 100, maxOrderSizeUsd: 30 });
    const aggregated = evaluateRisk(safeRisk({
      asset: "BTC", assetExposureUsd: "80", marketExposureUsd: "40", orderSizeUsd: "25",
    }), limits);
    expect(aggregated.vetoes).toContain("MAX_ASSET_EXPOSURE");
    expect(aggregated.vetoes).not.toContain("MAX_MARKET_EXPOSURE");
    const marketOnlyWouldPass = evaluateRisk(safeRisk({
      asset: "BTC", assetExposureUsd: "40", marketExposureUsd: "40", orderSizeUsd: "25",
    }), limits);
    expect(marketOnlyWouldPass.vetoes).not.toContain("MAX_ASSET_EXPOSURE");
  });
});

describe("performance report fields are trading attribution, not event counts", () => {
  it("event volume and PnL attribution are distinct", () => {
    const noisy = Array.from({ length: 40 }, (_, index) => createJournalEvent({
      traceId: createId("trace"), type: "MARKET_SNAPSHOT", aggregateType: "market", aggregateId: "noisy-mkt",
      payload: { marketId: "noisy-mkt", asset: "ETH", sequence: index },
    }));
    const quietBuy = {
      ...fill("UP", "0.40"), marketId: "quiet-mkt", feeUsd: "0.10", slippageUsd: "0.05",
    };
    const quietSell = {
      ...fill("UP", "0.60", "10", "SELL"), marketId: "quiet-mkt", feeUsd: "0.10", slippageUsd: "0.05",
    };
    const events = [
      createJournalEvent({
        traceId: createId("trace"), type: "DISCOVERY_RECONCILED", aggregateType: "engine", aggregateId: "market-discovery",
        payload: { markets: [{ id: "quiet-mkt", asset: "BTC" }, { id: "noisy-mkt", asset: "ETH" }] },
      }),
      ...noisy,
      createJournalEvent({
        traceId: createId("trace"), type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: quietBuy.id,
        payload: { ...quietBuy, asset: "BTC" },
      }),
      createJournalEvent({
        traceId: createId("trace"), type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: quietSell.id,
        payload: { ...quietSell, asset: "BTC" },
      }),
      createJournalEvent({
        traceId: createId("trace"), type: "RISK_EVALUATION", aggregateType: "risk", aggregateId: createId("risk"),
        payload: { accepted: true, marketId: "noisy-mkt", asset: "ETH" },
      }),
      createJournalEvent({
        traceId: createId("trace"), type: "RISK_EVALUATION", aggregateType: "risk", aggregateId: createId("risk"),
        payload: { accepted: true, marketId: "quiet-mkt", asset: "BTC" },
      }),
    ];
    const summary = buildExperimentSummary(events, "exp-attribution");
    expect(summary.eventCountByMarket["noisy-mkt"] ?? 0).toBeGreaterThan(summary.eventCountByMarket["quiet-mkt"] ?? 0);
    expect(summary.pnlByMarket["quiet-mkt"]).toBeCloseTo(1.8);
    expect(summary.pnlByMarket["noisy-mkt"] ?? 0).toBe(0);
    expect(summary.fillsByMarket["quiet-mkt"]).toBe(2);
    expect(summary.fillsByMarket["noisy-mkt"] ?? 0).toBe(0);
    expect(summary.feesByMarket["quiet-mkt"]).toBeCloseTo(0.2);
    expect(summary.slippageByMarket["quiet-mkt"]).toBeCloseTo(0.1);
    expect(summary.pnlByAsset["BTC"]).toBeCloseTo(1.8);
    expect(summary.pnlByAsset["ETH"] ?? 0).toBe(0);
    expect(summary.fillsByAsset["BTC"]).toBe(2);
    expect(summary.opportunitiesByMarket["noisy-mkt"]).toBe(1);
    expect(summary.opportunitiesByMarket["quiet-mkt"]).toBe(1);
    expect(summary.performanceByMarket["quiet-mkt"]?.netTradingPnlUsd).toBeCloseTo(1.8);
    expect(typeof summary.performanceByMarket["quiet-mkt"]).toBe("object");
    const markdown = renderExperimentSummaryMarkdown(summary);
    expect(markdown).toContain("activity, not PnL");
    expect(markdown).toContain("PnL by market");
    expect(markdown).not.toMatch(/performance by market: \{[^}]*noisy-mkt":4/);
  });
});
