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
import { BtcWebSocketAdapter, PolymarketMarketDataAdapter } from "@pm/market-data";
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
    eventId: createId("market"), traceId, marketId: "btc-up-down", tokenId: "up-token", outcome: "UP",
    bids: [{ price: "0.47", quantity: "100" }], asks: [{ price: "0.48", quantity: "30" }, { price: "0.49", quantity: "100" }],
    lastTradePrice: "0.48", tickSize: "0.01", minOrderSize: "5", feeSchedule,
    sourceTimestamp: now.toISOString(), receivedAt: now.toISOString(), sequence: 1, hash: "fixture",
  });
  const consensus = buildConsensus({
    traceId, triggerId: createId("trigger"), deterministicEdge: 0.04, now,
    signals: [
      { agentId: "luna_market_microstructure", direction: "UP", confidence: 0.8, expiresAt: new Date(now.getTime() + 30_000).toISOString(), evidenceIds: [snapshot.eventId] },
      { agentId: "terra_fair_probability", direction: "UP", confidence: 0.82, expiresAt: new Date(now.getTime() + 30_000).toISOString(), evidenceIds: [snapshot.eventId] },
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
  const currentWindow = Math.floor(Date.now() / 900_000) * 900;
  const failures: string[] = [];
  const discovered = await market.discoverBitcoinMarkets(configuredSlugs.length ? configuredSlugs : [`btc-updown-15m-${currentWindow}`], 5)
    .catch((error) => { failures.push(`polymarket-discovery:${error instanceof Error ? error.message : String(error)}`); return []; });
  const tokenIds = discovered.flatMap((item) => item.tokens.map((token) => token.tokenId));
  let polymarketMessages = 0;
  let btcMessages = 0;
  let finish!: () => void;
  const complete = new Promise<void>((resolve) => { finish = resolve; });
  const check = () => { if (polymarketMessages > 0 && btcMessages > 0) finish(); };
  if (tokenIds.length > 0) void market.run(tokenIds, { onSnapshot: () => { polymarketMessages += 1; check(); }, onConnection: () => undefined, onError: (error) => { failures.push(`polymarket:${error.message}`); } }, abort.signal);
  const btc = new BtcWebSocketAdapter(process.env.BTC_WS_URL ?? "wss://stream.binance.com:9443/ws/btcusdt@aggTrade", process.env.BTC_SYMBOL ?? "BTCUSDT");
  void btc.run({ onTick: () => { btcMessages += 1; check(); }, onConnection: () => undefined, onError: (error) => { failures.push(`btc:${error.message}`); } }, abort.signal);
  await Promise.race([complete, new Promise<void>((resolve) => setTimeout(resolve, 25_000))]);
  abort.abort("smoke complete");
  const result = { ok: polymarketMessages > 0 && btcMessages > 0, discoveredMarkets: discovered.length, polymarketMessages, btcMessages, failures };
  console.log(JSON.stringify(result, null, 2));
  if (!result.ok) process.exitCode = 1;
} else {
  console.log("Usage: tsx src/cli.ts <smoke|live-feed-smoke|experiment:start|report>");
}
