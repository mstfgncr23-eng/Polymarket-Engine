import { resolve } from "node:path";
import {
  PaperLedger,
  TemporalPairHedgeStrategy,
  buildConsensus,
  createId,
  defaultConsensusConfig,
  executePaperTaker,
  feeScheduleSchema,
  marketSnapshotSchema,
  paperOrderSchema,
} from "@pm/core";
import { MemoryEventJournal, PostgresEventJournal } from "@pm/db";
import { PolymarketMarketDataAdapter, ReferenceFeedRegistry, createRestProbe } from "@pm/market-data";
import { buildExperimentSummary, writeExperimentSummary } from "./report.js";
import { startRuntime } from "./runtime.js";

const command = process.argv[2] ?? "help";

if (command === "experiment:start") {
  if (process.env.TRADING_MODE && process.env.TRADING_MODE !== "paper") throw new Error("LIVE_TRADING_DISABLED");
  process.env.TRADING_MODE = "paper";
  await startRuntime();
  console.log(JSON.stringify({ status: "running", mode: "paper", durationHours: Number(process.env.EXPERIMENT_DURATION_HOURS ?? 48) }));
} else if (command === "report") {
  const journal = process.env.DATABASE_URL
    ? new PostgresEventJournal(process.env.DATABASE_URL, process.env.DB_MIGRATIONS_DIR ?? resolve(process.cwd(), "packages/db/migrations"))
    : new MemoryEventJournal();
  await journal.initialize();
  const summary = buildExperimentSummary(await journal.readAll(), process.env.EXPERIMENT_ID ?? "no-persisted-memory-experiment");
  const paths = await writeExperimentSummary(summary, resolve(process.cwd(), "reports"));
  await journal.close();
  console.log(JSON.stringify(paths));
} else if (command === "smoke") {
  const now = new Date();
  const traceId = createId("trace");
  const feeSchedule = feeScheduleSchema.parse({ rate: "0.07", exponent: 1, takerOnly: true, rebateRate: "0.20", source: "configured_test_fixture", effectiveAt: now.toISOString() });
  const snapshot = marketSnapshotSchema.parse({
    eventId: createId("market"), traceId, marketId: "btc-up-down", tokenId: "up-token", outcome: "UP", asset: "BTC",
    bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "30" }, { price: "0.49", quantity: "100" }],
    lastTradePrice: "0.48", tickSize: "0.01", minOrderSize: "5", feeSchedule,
    sourceTimestamp: now.toISOString(), receivedAt: now.toISOString(), sequence: 1, hash: "fixture",
  });
  const consensus = buildConsensus({
    traceId, triggerId: createId("trigger"), deterministicEdge: 0.04, now,
    signals: [
      { agentId: "market_microstructure_scanner", direction: "UP", confidence: 0.8, expiresAt: new Date(now.getTime() + 30_000).toISOString(), evidenceIds: [snapshot.eventId] },
      { agentId: "fair_probability_analyst", direction: "UP", confidence: 0.82, expiresAt: new Date(now.getTime() + 30_000).toISOString(), evidenceIds: [snapshot.eventId] },
    ], config: defaultConsensusConfig,
  });
  const strategy = new TemporalPairHedgeStrategy();
  const candidate = strategy.evaluate({ consensus, snapshots: { UP: snapshot, DOWN: null }, position: null, maxOrderSizeUsd: 10, minimumLockedPairEdge: 0.01 });
  if (!candidate) throw new Error("SMOKE_NO_CANDIDATE");
  const order = paperOrderSchema.parse({
    id: createId("order"), traceId, triggerId: consensus.triggerId, strategyId: strategy.id,
    marketId: snapshot.marketId, tokenId: snapshot.tokenId, outcome: "UP", side: "BUY", orderType: "TAKER",
    requestedPrice: "0.49", quantity: candidate.quantity, status: "PENDING", createdAt: now.toISOString(),
  });
  const fill = executePaperTaker({ order, snapshotAfterLatency: snapshot, measuredLatencyMs: 150 });
  const ledger = new PaperLedger(1000, now);
  const position = ledger.applyFill(fill);
  console.log(JSON.stringify({ ok: true, traceId, consensus: consensus.consensusDirection, fillPrice: fill.fillPrice, feeUsd: fill.feeUsd, cash: ledger.snapshot().cash, position }, null, 2));
} else if (command === "live-feed-smoke") {
  const abort = new AbortController();
  const market = new PolymarketMarketDataAdapter();
  const configuredSlugs = (process.env.POLYMARKET_MARKET_SLUGS ?? "").split(",").map((value) => value.trim()).filter(Boolean);
  const failures: string[] = [];
  const report = await market.discoverCryptoMarkets(configuredSlugs, 20)
    .catch((error) => { failures.push(`polymarket-discovery:${error instanceof Error ? error.message : String(error)}`); return { discovered: [], qualified: [], rejected: [] }; });
  const tokenIds = report.qualified.flatMap((item) => item.tokens.map((token) => token.tokenId));
  const assets = [...new Set(report.qualified.map((item) => item.asset).filter((item): item is string => Boolean(item)))];
  let polymarketMessages = 0;
  const referenceMessages: Record<string, number> = {};
  let finish!: () => void;
  const complete = new Promise<void>((resolve) => { finish = resolve; });
  const check = () => {
    const refCount = Object.values(referenceMessages).reduce((sum, value) => sum + value, 0);
    if (polymarketMessages > 0 && (assets.length === 0 || refCount > 0)) finish();
  };
  if (tokenIds.length > 0) void market.run(tokenIds, { onSnapshot: () => { polymarketMessages += 1; check(); }, onConnection: () => undefined, onError: (error) => { failures.push(`polymarket:${error.message}`); } }, abort.signal);
  const restBase = process.env.REFERENCE_REST_BASE_URL ?? "https://api.binance.com";
  const wsBase = process.env.REFERENCE_WS_BASE_URL ?? "wss://data-stream.binance.vision/ws";
  const registry = new ReferenceFeedRegistry(wsBase, restBase, 15_000, undefined, createRestProbe(restBase));
  const referenceResolutions: Array<{ asset: string; supported: boolean; symbol: string | null }> = [];
  for (const asset of assets) {
    const resolution = await registry.resolve(asset);
    referenceResolutions.push({ asset, supported: resolution.supported, symbol: resolution.symbol });
    const adapter = registry.adapterFor(asset);
    if (!adapter) {
      failures.push(`reference-unsupported:${asset}`);
      continue;
    }
    referenceMessages[asset] = 0;
    void adapter.run({
      onTick: () => { referenceMessages[asset] = (referenceMessages[asset] ?? 0) + 1; check(); },
      onConnection: () => undefined,
      onError: (error) => { failures.push(`reference:${asset}:${error.message}`); },
    }, abort.signal);
  }
  await Promise.race([complete, new Promise<void>((resolve) => setTimeout(resolve, 25_000))]);
  abort.abort("smoke complete");
  const refCount = Object.values(referenceMessages).reduce((sum, value) => sum + value, 0);
  const result = {
    ok: polymarketMessages > 0,
    discoveredMarkets: report.discovered.length,
    qualifiedMarkets: report.qualified.map((market) => ({ id: market.id, slug: market.slug, asset: market.asset, tokens: market.tokens.length })),
    rejectedMarkets: report.rejected.slice(0, 25),
    assets,
    tokenSubscriptions: tokenIds.length,
    referenceFeeds: referenceResolutions,
    polymarketMessages,
    referenceMessages,
    referenceMessageCount: refCount,
    failures,
  };
  console.log(JSON.stringify(result, null, 2));
  if (!result.ok) process.exitCode = 1;
} else {
  console.log("Usage: tsx src/cli.ts <smoke|live-feed-smoke|experiment:start|report>");
}
