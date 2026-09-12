import { resolve } from "node:path";
import {
  PaperLedger,
  TemporalPairHedgeStrategy,
  createId,
  evaluateNativeDecision,
  executePaperTaker,
  feeScheduleSchema,
  marketSnapshotSchema,
  paperOrderSchema,
  triggerEventSchema,
  NATIVE_DETERMINISTIC_AUTHORITY,
} from "@pm/core";
import { MemoryEventJournal, PostgresEventJournal, type JournalAccessMode } from "@pm/db";
import { applyJournalCleanup, dryRunJournalCleanup } from "./journal-cleanup.js";
import { resolveCleanupLifecycle } from "./cleanup-lifecycle.js";
import { generateOperatorReport } from "./operator-report.js";
import { retentionConfigFromEnv } from "./retention-manager.js";
import { PolymarketMarketDataAdapter, ReferenceFeedRegistry, createRestProbe } from "@pm/market-data";
import { startRuntime } from "./runtime.js";
import { exportExperimentArchive } from "./archive.js";

const command = process.argv[2] ?? "help";

if (command === "experiment:start") {
  if (process.env.TRADING_MODE && process.env.TRADING_MODE !== "paper") throw new Error("LIVE_TRADING_DISABLED");
  process.env.TRADING_MODE = "paper";
  const runtime = await startRuntime();
  const result = await runtime.engine.beginExperiment();
  console.log(JSON.stringify({ status: result.ok ? "running" : "blocked", mode: "paper", durationHours: Number(process.env.EXPERIMENT_DURATION_HOURS ?? 48), ...result }));
} else if (command === "report") {
  const persistCanonical = process.env.REPORT_PERSIST === "1" || process.env.REPORT_PERSIST === "true";
  const mode: JournalAccessMode = persistCanonical ? "report-write" : "readonly";
  const journal = process.env.DATABASE_URL
    ? new PostgresEventJournal(process.env.DATABASE_URL, process.env.DB_MIGRATIONS_DIR ?? resolve(process.cwd(), "packages/db/migrations"), { access: mode })
    : new MemoryEventJournal();
  if (journal.connectWithoutMigrate) await journal.connectWithoutMigrate(mode);
  if (journal instanceof MemoryEventJournal) journal.access = persistCanonical ? "report-write" : "readonly";
  try {
    const result = await generateOperatorReport(journal, {
      experimentId: process.env.EXPERIMENT_ID,
      reportDirectory: resolve(process.cwd(), "reports"),
      allowPersist: persistCanonical,
    });
    console.log(JSON.stringify(result));
  } finally {
    await journal.close();
  }
} else if (command === "journal-cleanup") {
  const apply = process.argv.includes("--apply");
  const confirm = process.argv.includes("--i-understand-destructive");
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL_REQUIRED_FOR_JOURNAL_CLEANUP");
  const mode: JournalAccessMode = apply ? "maintenance" : "readonly";
  const journal = new PostgresEventJournal(process.env.DATABASE_URL, process.env.DB_MIGRATIONS_DIR ?? resolve(process.cwd(), "packages/db/migrations"), { access: mode });
  await journal.connectWithoutMigrate(mode);
  const lifecycle = await resolveCleanupLifecycle(journal, {
    experimentId: process.env.EXPERIMENT_ID,
    status: process.env.EXPERIMENT_STATUS,
    startedAt: process.env.EXPERIMENT_STARTED_AT,
  });
  const experiment = lifecycle.experiment;
  const retention = retentionConfigFromEnv(process.env);
  try {
    if (!apply) {
      console.log(JSON.stringify(await dryRunJournalCleanup(journal, new Date(), experiment, retention, {}, lifecycle), null, 2));
    } else {
      if (!confirm) throw new Error("JOURNAL_CLEANUP_APPLY_REQUIRES_--i-understand-destructive");
      console.log(JSON.stringify(await applyJournalCleanup(journal, {
        experiment, confirm: "I_UNDERSTAND_DESTRUCTIVE_CLEANUP", config: retention, lifecycle,
      }), null, 2));
    }
  } finally {
    await journal.close();
  }
} else if (command === "archive:export" || command === "archive") {
  if (!process.env.DATABASE_URL) throw new Error("DATABASE_URL_REQUIRED_FOR_ARCHIVE_EXPORT");
  const experimentId = process.env.EXPERIMENT_ID;
  if (!experimentId) throw new Error("EXPERIMENT_ID_REQUIRED_FOR_ARCHIVE_EXPORT");
  const journal = new PostgresEventJournal(process.env.DATABASE_URL, process.env.DB_MIGRATIONS_DIR ?? resolve(process.cwd(), "packages/db/migrations"), { access: "readonly" });
  await journal.connectWithoutMigrate("readonly");
  try {
    const directory = process.env.ARCHIVE_DIRECTORY ?? resolve(process.cwd(), "reports", "archive", experimentId);
    console.log(JSON.stringify(await exportExperimentArchive(journal, { experimentId, directory }), null, 2));
  } finally {
    await journal.close();
  }
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
  const downSnapshot = marketSnapshotSchema.parse({
    ...snapshot, eventId: createId("market"), tokenId: "down-token", outcome: "DOWN",
    bids: [{ price: "0.50", quantity: "80" }], asks: [{ price: "0.51", quantity: "30" }],
  });
  const trigger = triggerEventSchema.parse({
    id: createId("trigger"), traceId, marketId: snapshot.marketId, asset: "BTC", source: "polymarket",
    kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: createId("dedupe"), evidence: { source: "smoke" },
    sourceEventIds: [snapshot.eventId, downSnapshot.eventId], evidenceIds: [snapshot.eventId, downSnapshot.eventId],
    createdAt: now.toISOString(), expiresAt: new Date(now.getTime() + 30_000).toISOString(),
  });
  const native = evaluateNativeDecision({
    trigger, up: snapshot, down: downSnapshot,
    reference: { asset: "BTC", venue: "binance", freshness: "fresh", freshnessMs: 0, recentMoveBps: 30, recentVolatilityBps: 0 },
    referenceRequired: true, marketSynchronized: true, triggerProvenanceValid: true, now,
  });
  if (!native) throw new Error("SMOKE_NATIVE_DECISION_UNAVAILABLE");
  const consensus = {
    id: createId("consensus"), traceId, triggerId: trigger.id,
    consensusDirection: native.direction, consensusConfidence: native.confidence,
    agentAgreement: 1, signalFreshness: 1, conflicts: [],
    estimatedEdge: native.fairProbability - native.marketProbability,
    evidenceIds: native.evidenceIds, calculatedAt: now.toISOString(), authority: NATIVE_DETERMINISTIC_AUTHORITY,
    fairProbability: native.fairProbability, marketProbability: native.marketProbability, featureVector: { ...native.featureVector },
  };
  const strategy = new TemporalPairHedgeStrategy();
  const candidate = strategy.evaluate({ consensus, snapshots: { UP: snapshot, DOWN: downSnapshot }, position: null, maxOrderSizeUsd: 10, minimumLockedPairEdge: 0.01 });
  if (!candidate) throw new Error("SMOKE_NO_CANDIDATE");
  const order = paperOrderSchema.parse({
    id: createId("order"), traceId, triggerId: trigger.id, strategyId: strategy.id,
    marketId: snapshot.marketId, tokenId: snapshot.tokenId, outcome: "UP", side: "BUY", orderType: "TAKER",
    requestedPrice: "0.49", quantity: candidate.quantity, status: "PENDING", createdAt: now.toISOString(),
  });
  const fill = executePaperTaker({ order, snapshotAfterLatency: snapshot, measuredLatencyMs: 150 });
  const ledger = new PaperLedger(1000, now);
  const position = ledger.applyFill(fill);
  console.log(JSON.stringify({ ok: true, traceId, consensus: consensus.consensusDirection, fillPrice: fill.fillPrice, feeUsd: fill.feeUsd, cash: ledger.snapshot().cash, position }, null, 2));
} else if (command === "live-discovery-diag") {
  const market = new PolymarketMarketDataAdapter();
  const report = await market.discoverCryptoMarkets([], 40);
  const closestDirectional = market.closestDirectional(report, 20);
  console.log(JSON.stringify({
    discoveredMarkets: report.discovered.length,
    qualifiedMarkets: report.qualified.map((item) => ({
      id: item.id, slug: item.slug, asset: item.asset, endDate: item.endDate, endDateSource: item.endDateSource,
      remainingDurationMs: item.remainingDurationMs, tokens: item.tokens,
    })),
    closestDirectional,
    rejectedReasonCounts: Object.fromEntries(report.rejected.flatMap((item) => item.reasonCodes).reduce((map, code) => {
      map.set(code, (map.get(code) ?? 0) + 1);
      return map;
    }, new Map<string, number>())),
  }, null, 2));
} else if (command === "live-feed-smoke") {
  const abort = new AbortController();
  const market = new PolymarketMarketDataAdapter();
  const configuredSlugs = (process.env.POLYMARKET_MARKET_SLUGS ?? "").split(",").map((value) => value.trim()).filter(Boolean);
  const failures: string[] = [];
  const report = await market.discoverCryptoMarkets(configuredSlugs, 20)
    .catch((error) => { failures.push(`polymarket-discovery:${error instanceof Error ? error.message : String(error)}`); return { discovered: [], qualified: [], rejected: [] }; });
  const qualifiedTokenIds = report.qualified.flatMap((item) => item.tokens.map((token) => token.tokenId));
  let connectivityProbe = false;
  let subscribeTokenIds = qualifiedTokenIds;
  if (subscribeTokenIds.length === 0) {
    const probe = report.discovered.filter((item) => item.active && item.tokens.every((token) => token.tokenId.length > 0)).slice(0, 6);
    for (const item of probe) {
      for (const token of item.tokens) market.registerManualToken(token.tokenId, item.id, token.outcome, item.asset);
    }
    subscribeTokenIds = probe.flatMap((item) => item.tokens.map((token) => token.tokenId));
    connectivityProbe = subscribeTokenIds.length > 0;
  }
  const assets = [...new Set(report.qualified.map((item) => item.asset).filter((item): item is string => Boolean(item)))];
  let polymarketMessages = 0;
  const referenceMessages: Record<string, number> = {};
  let finish!: () => void;
  const complete = new Promise<void>((resolve) => { finish = resolve; });
  const check = () => {
    const refCount = Object.values(referenceMessages).reduce((sum, value) => sum + value, 0);
    const waitingForReference = Object.keys(referenceMessages).length > 0;
    if (polymarketMessages > 0 && (!waitingForReference || refCount > 0)) finish();
  };
  if (subscribeTokenIds.length > 0) void market.run(subscribeTokenIds, { onSnapshot: () => { polymarketMessages += 1; check(); }, onConnection: () => undefined, onError: (error) => { failures.push(`polymarket:${error.message}`); } }, abort.signal);
  const restBase = process.env.REFERENCE_REST_BASE_URL ?? "https://api.binance.com";
  const wsBase = process.env.REFERENCE_WS_BASE_URL ?? "wss://data-stream.binance.vision/ws";
  const registry = new ReferenceFeedRegistry(wsBase, restBase, 15_000, undefined, createRestProbe(restBase));
  const referenceResolutions: Array<{ asset: string; supported: boolean; status: string; symbol: string | null; reason: string; detail?: string }> = [];
  for (const asset of assets) {
    const resolution = await registry.resolve(asset);
    referenceResolutions.push({
      asset, supported: resolution.supported, status: resolution.status, symbol: resolution.symbol,
      reason: resolution.reason, ...(resolution.detail ? { detail: resolution.detail } : {}),
    });
    const adapter = registry.adapterFor(asset);
    if (!adapter) {
      if (resolution.status === "PROVIDER_BLOCKED" || resolution.status === "REGION_BLOCKED") {
        failures.push(`reference-blocked:${asset}:${resolution.detail ?? resolution.reason}`);
      } else if (resolution.status === "TRANSIENT_FAILURE") {
        failures.push(`reference-transient:${asset}:${resolution.detail ?? resolution.reason}`);
      } else {
        failures.push(`reference-unsupported:${asset}`);
      }
      continue;
    }
    referenceMessages[asset] = 0;
    void adapter.run({
      onTick: () => { referenceMessages[asset] = (referenceMessages[asset] ?? 0) + 1; check(); },
      onConnection: () => undefined,
      onError: (error) => { failures.push(`reference:${asset}:${error.message}`); },
    }, abort.signal);
  }
  check();
  await Promise.race([complete, new Promise<void>((resolve) => setTimeout(resolve, 25_000))]);
  abort.abort("smoke complete");
  const refCount = Object.values(referenceMessages).reduce((sum, value) => sum + value, 0);
  const rejectReasons: Record<string, number> = {};
  for (const item of report.rejected) {
    for (const code of item.reasonCodes) rejectReasons[code] = (rejectReasons[code] ?? 0) + 1;
  }
  const result = {
    ok: polymarketMessages > 0,
    discoveredMarkets: report.discovered.length,
    qualifiedMarkets: report.qualified.map((market) => ({ id: market.id, slug: market.slug, asset: market.asset, tokens: market.tokens.length })),
    rejectedReasonCounts: rejectReasons,
    rejectedMarkets: report.rejected.slice(0, 25),
    assets,
    tokenSubscriptions: subscribeTokenIds.length,
    connectivityProbe,
    closestDirectional: market.closestDirectional(report, 20),
    referenceFeeds: referenceResolutions,
    polymarketMessages,
    referenceMessages,
    referenceMessageCount: refCount,
    failures,
  };
  console.log(JSON.stringify(result, null, 2));
  if (!result.ok) process.exitCode = 1;
} else {
  console.log("Usage: tsx src/cli.ts <smoke|live-feed-smoke|live-discovery-diag|experiment:start|report|journal-cleanup|archive:export>");
}
