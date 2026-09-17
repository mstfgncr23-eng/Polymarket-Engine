import { appendFileSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  NATIVE_V2_COUNTERFACTUAL_STATE_BASIS,
  MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES,
  buildBoundedCounterfactualSettlementRow,
  clampJsonStringUtf8Bytes,
  MAX_COUNTERFACTUAL_SETTLEMENT_SOURCE_JSON_BYTES,
  counterfactualSettlementObligationBytes,
  NativeV2CounterfactualStore,
  PaperLedger,
  counterfactualDataDir,
  createId,
  nativeV2CounterfactualActionKey,
  type MarketSnapshot,
  type PaperOrder,
} from "@pm/core";

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), "pm-cf-"));
  dirs.push(dir);
  return dir;
}

const NOW = new Date("2026-09-17T08:00:00.000Z");

function snapshot(marketId = "m1"): MarketSnapshot {
  const iso = NOW.toISOString();
  return {
    eventId: "event-m1-up",
    traceId: "trace-1",
    marketId,
    tokenId: "token-up",
    outcome: "UP",
    asset: "BTC",
    bids: [{ price: "0.53", quantity: "200" }],
    asks: [{ price: "0.54", quantity: "4" }, { price: "0.56", quantity: "200" }],
    lastTradePrice: null,
    tickSize: "0.01",
    minOrderSize: "1",
    feeSchedule: {
      rate: "0",
      exponent: 1,
      takerOnly: true,
      rebateRate: "0",
      source: "configured_test_fixture",
      effectiveAt: iso,
    },
    sourceTimestamp: iso,
    receivedAt: iso,
    sequence: 1,
    hash: null,
  };
}

function settlementLookup(marketId = "m1") {
  return {
    marketId,
    slug: `${marketId}-slug`,
    endDate: "2026-09-17T09:00:00.000Z",
    yesMapsTo: "UP" as const,
    noMapsTo: "DOWN" as const,
  };
}

function order(marketId = "m1"): PaperOrder {
  return {
    id: createId("order"),
    traceId: "trace-1",
    triggerId: "trigger-1",
    strategyId: "native-v2-shadow-opportunity",
    marketId,
    tokenId: "token-up",
    outcome: "UP",
    side: "BUY",
    orderType: "TAKER",
    requestedPrice: "0.56",
    quantity: "5",
    status: "PENDING",
    createdAt: NOW.toISOString(),
  };
}

describe("Native V2 counterfactual state", () => {
  it("places durable files under LEARNING_DATA_DIR/counterfactual", () => {
    // path.join is platform-specific; assert the CF subdir contract, not POSIX separators.
    expect(counterfactualDataDir("/data/learning")).toBe(join("/data/learning", "counterfactual"));
  });

  it("first hypothetical would-submit changes V2-only state; V1 ledger unchanged", () => {
    const dataDir = tempDir();
    const v1 = new PaperLedger(1000, NOW);
    const v1Before = v1.snapshot();
    const store = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    store.init();

    const actionKey = nativeV2CounterfactualActionKey("m1", "UP");
    const first = store.tryCommitWouldSubmit({
      actionKey,
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    });
    expect(first.applied).toBe(true);
    if (!first.applied) throw new Error("expected apply");

    const after = store.decisionState();
    expect(after.stateBasis).toBe(NATIVE_V2_COUNTERFACTUAL_STATE_BASIS);
    expect(after.positions).toHaveLength(1);
    expect(after.positions[0]?.marketId).toBe("m1");
    expect(Number(after.positions[0]?.upQuantity)).toBeGreaterThan(0);
    expect(Number(after.availableCashUsd)).toBeLessThan(1000);
    expect(store.summary().fillCount).toBe(1);

    // V1 untouched
    expect(v1.snapshot()).toEqual(v1Before);
    expect(v1.positions.size).toBe(0);
    expect(v1.availableCash().toFixed()).toBe("1000");
  });

  it("subsequent evaluation sees updated V2 state (unpaired + lower cash)", () => {
    const store = new NativeV2CounterfactualStore({ dataDir: tempDir(), startingBalance: 1000 });
    store.init();
    const actionKey = nativeV2CounterfactualActionKey("m1", "UP");
    expect(store.tryCommitWouldSubmit({
      actionKey,
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    }).applied).toBe(true);

    const state = store.decisionState();
    expect(store.hasUnpairedPosition("m1")).toBe(true);
    expect(store.hasAction(actionKey)).toBe(true);
    expect(Number(state.ledger.cash)).toBeLessThan(1000);
    expect(Number(state.positions[0]?.unpairedQuantity)).toBeGreaterThan(0);
  });

  it("duplicate action is idempotent", () => {
    const store = new NativeV2CounterfactualStore({ dataDir: tempDir(), startingBalance: 1000 });
    store.init();
    const actionKey = nativeV2CounterfactualActionKey("m1", "UP");
    const input = { actionKey, order: order(), snapshot: snapshot(), at: NOW, settlementLookup: settlementLookup("m1") };
    expect(store.tryCommitWouldSubmit(input).applied).toBe(true);
    const mid = store.summary();
    const dup = store.tryCommitWouldSubmit({
      ...input,
      order: order(), // new order id, same action key,
      settlementLookup: settlementLookup("m1"),
    });
    expect(dup.applied).toBe(false);
    if (dup.applied) throw new Error("expected duplicate");
    expect(dup.reason).toBe("DUPLICATE_ACTION");
    expect(store.summary().fillCount).toBe(mid.fillCount);
    expect(store.summary().availableCashUsd).toBe(mid.availableCashUsd);
  });

  it("settlement realizes counterfactual PnL from authoritative labels", () => {
    const store = new NativeV2CounterfactualStore({ dataDir: tempDir(), startingBalance: 1000 });
    store.init();
    expect(store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    }).applied).toBe(true);

    const beforeSettle = store.summary();
    const settled = store.trySettleFromLearningLabel("m1", {
      status: "resolved",
      outcome: "UP",
      labeledAt: NOW.toISOString(),
      source: "public_polymarket",
      reason: "RESOLVED",
    }, NOW);
    expect(settled.applied).toBe(true);
    if (!settled.applied) throw new Error("expected settle");
    expect(Number(settled.realizedDelta)).not.toBeNaN();
    expect(store.openPositionMarketIds()).toEqual([]);
    expect(store.summary().settlementCount).toBe(1);
    expect(Number(store.summary().realizedPnl)).toBe(Number(beforeSettle.realizedPnl) + Number(settled.realizedDelta));
    // Terminal tombstone: settled market must never reopen (V1-style).
    expect(store.hasAction(nativeV2CounterfactualActionKey("m1", "UP"))).toBe(false);
    expect(store.hasSettledMarket("m1")).toBe(true);
    const reopen = store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    });
    expect(reopen.applied).toBe(false);
    if (reopen.applied) throw new Error("expected reject");
    expect(reopen.reason).toBe("MARKET_SETTLED");
  });

  it("restart/recovery replays durable events into equivalent CF state", () => {
    const dataDir = tempDir();
    const first = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    first.init();
    expect(first.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    }).applied).toBe(true);
    expect(first.trySettleMarket("m1", "1", "0", {
      at: new Date("2026-09-17T09:00:00.000Z"),
      source: "public_polymarket",
      reason: "RESOLVED",
    }).applied).toBe(true);
    const expected = first.summary();

    const recovered = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    recovered.init();
    const got = recovered.summary();
    expect(got.fillCount).toBe(expected.fillCount);
    expect(got.settlementCount).toBe(expected.settlementCount);
    expect(got.availableCashUsd).toBe(expected.availableCashUsd);
    expect(got.realizedPnl).toBe(expected.realizedPnl);
    expect(got.fees).toBe(expected.fees);
    expect(recovered.openPositionMarketIds()).toEqual([]);
    expect(recovered.decisionState().stateBasis).toBe(NATIVE_V2_COUNTERFACTUAL_STATE_BASIS);
  });

  it("unresolved learning labels do not invent CF settlement", () => {
    const store = new NativeV2CounterfactualStore({ dataDir: tempDir(), startingBalance: 1000 });
    store.init();
    expect(store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    }).applied).toBe(true);
    const rejected = store.trySettleFromLearningLabel("m1", {
      status: "unresolved",
      outcome: null,
      labeledAt: null,
      source: null,
      reason: "UNRESOLVED",
    });
    expect(rejected.applied).toBe(false);
    if (rejected.applied) throw new Error("expected reject");
    expect(rejected.reason).toBe("UNRESOLVED_LABEL");
    expect(store.openPositionMarketIds()).toEqual(["m1"]);
  });

  it("fill durable append failure fail-closes CF for subsequent evaluations; restart recovers prior log", () => {
    const dataDir = tempDir();
    const store = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    store.init();
    const before = store.summary();
    store.faultNextAppendForTest(new Error("FORCED_FILL_APPEND_FAILURE"));
    const result = store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    });
    expect(result.applied).toBe(false);
    if (result.applied) throw new Error("expected failure");
    expect(result.reason).toBe("EXECUTION_ERROR");
    expect(store.summary().fillCount).toBe(before.fillCount);
    expect(store.summary().availableCashUsd).toBe(before.availableCashUsd);
    expect(store.openPositionMarketIds()).toEqual([]);
    expect(store.summary().degraded).toBe(true);
    expect(store.isRecoveryReady()).toBe(false);
    expect(store.decisionState().recoveryReady).toBe(false);
    // Subsequent evaluation attempt must not treat CF as authoritative.
    const again = store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m2", "UP"),
      order: order("m2"),
      snapshot: snapshot("m2"),
      at: NOW,
      settlementLookup: settlementLookup("m2"),
    });
    expect(again.applied).toBe(false);
    if (again.applied) throw new Error("expected reject");
    expect(again.reason).toBe("RECOVERY_UNAVAILABLE");

    // Durable log was never mutated — clean restart is recoveryReady again.
    const recovered = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    recovered.init();
    expect(recovered.isRecoveryReady()).toBe(true);
    expect(recovered.summary().fillCount).toBe(0);
    expect(recovered.openPositionMarketIds()).toEqual([]);
    expect(recovered.summary().availableCashUsd).toBe(before.availableCashUsd);
  });

  it("settlement durable append failure leaves memory and restart state unchanged", () => {
    const dataDir = tempDir();
    const store = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    store.init();
    expect(store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    }).applied).toBe(true);
    const before = store.summary();
    expect(store.openPositionMarketIds()).toEqual(["m1"]);

    store.faultNextAppendForTest(new Error("FORCED_SETTLEMENT_APPEND_FAILURE"));
    const settled = store.trySettleMarket("m1", "1", "0", {
      at: new Date("2026-09-17T09:00:00.000Z"),
      source: "public_polymarket",
      reason: "RESOLVED",
    });
    expect(settled.applied).toBe(false);
    if (settled.applied) throw new Error("expected failure");
    expect(settled.reason).toBe("EXECUTION_ERROR");
    expect(store.summary().settlementCount).toBe(before.settlementCount);
    expect(store.openPositionMarketIds()).toEqual(["m1"]);
    expect(store.summary().availableCashUsd).toBe(before.availableCashUsd);
    expect(store.summary().degraded).toBe(true);

    const recovered = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    recovered.init();
    expect(recovered.openPositionMarketIds()).toEqual(["m1"]);
    expect(recovered.summary().settlementCount).toBe(0);
    expect(recovered.summary().fillCount).toBe(1);
    expect(recovered.summary().availableCashUsd).toBe(before.availableCashUsd);
  });

  it("CF portfolio is continuous across experiment boundaries (matches V1 PaperLedger lifetime)", () => {
    // KNOWN: V1 ledger is process-lifetime (PAPER_START_BALANCE once); beginExperiment
    // carries snapshot().startingBalance and only residue-gates open exposure.
    // Shadow store resets on START_VALIDATED; CF must NOT invent a per-experiment wipe.
    const dataDir = tempDir();
    const store = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    store.init();
    expect(store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order("m1"),
      snapshot: snapshot("m1"),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    }).applied).toBe(true);
    expect(store.trySettleMarket("m1", "1", "0", { at: NOW }).applied).toBe(true);
    const afterExperimentA = store.summary();
    expect(afterExperimentA.settlementCount).toBe(1);
    expect(Number(afterExperimentA.availableCashUsd)).not.toBe(1000);

    // Simulated experiment B boundary: no CF reset; cash/PnL accumulate.
    expect(store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m2", "UP"),
      order: order("m2"),
      snapshot: snapshot("m2"),
      at: NOW,
      settlementLookup: settlementLookup("m2"),
    }).applied).toBe(true);
    const afterExperimentB = store.summary();
    expect(afterExperimentB.fillCount).toBe(2);
    expect(afterExperimentB.settlementCount).toBe(1);
    expect(Number(afterExperimentB.availableCashUsd)).toBeLessThan(Number(afterExperimentA.availableCashUsd));

    const recovered = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    recovered.init();
    expect(recovered.summary().fillCount).toBe(2);
    expect(recovered.summary().settlementCount).toBe(1);
    expect(recovered.summary().availableCashUsd).toBe(afterExperimentB.availableCashUsd);
  });

  it("total maxBytes bound rejects fills without mutating CF or V1-visible authority", () => {
    const dataDir = tempDir();
    const store = new NativeV2CounterfactualStore({
      dataDir,
      startingBalance: 1000,
      // Must fit one fill + one settlement reservation; settle-as-you-go keeps open count <= 1.
      maxBytes: 20_000,
      segmentMaxBytes: 8_000,
    });
    store.init();
    // First fill may succeed; keep committing until capacity rejects.
    let rejected = null;
    for (let i = 0; i < 200; i += 1) {
      const marketId = `m${i}`;
      const result = store.tryCommitWouldSubmit({
        actionKey: nativeV2CounterfactualActionKey(marketId, "UP"),
        order: order(marketId),
        snapshot: snapshot(marketId),
        at: NOW,
      settlementLookup: settlementLookup(marketId),
    });
      if (!result.applied) {
        rejected = result;
        break;
      }
      // Settle to free action keys / keep writing more fill events.
      store.trySettleMarket(marketId, "1", "0", { at: NOW });
    }
    expect(rejected).not.toBeNull();
    expect(rejected?.reason).toBe("CAPACITY_EXCEEDED");
    expect(store.summary().degraded).toBe(true);
    expect(store.isRecoveryReady()).toBe(false);
    expect(store.decisionState().recoveryReady).toBe(false);

    const recovered = new NativeV2CounterfactualStore({
      dataDir,
      startingBalance: 1000,
      maxBytes: 20_000,
      segmentMaxBytes: 8_000,
    });
    recovered.init();
    expect(recovered.isRecoveryReady()).toBe(true);
    expect(recovered.summary().fillCount).toBeGreaterThan(0);
    expect(recovered.summary().bytes).toBeLessThanOrEqual(recovered.summary().maxBytes);
  });

  it("accepted open CF positions retain guaranteed settlement capacity (incl. restart)", () => {
    const dataDir = tempDir();
    const maxBytes = 25_000;
    const store = new NativeV2CounterfactualStore({
      dataDir,
      startingBalance: 10_000,
      maxBytes,
      segmentMaxBytes: 8_000,
    });
    store.init();

    const acceptedMarketIds = [];
    for (let i = 0; i < 80; i += 1) {
      const marketId = `open-${i}`;
      const result = store.tryCommitWouldSubmit({
        actionKey: nativeV2CounterfactualActionKey(marketId, "UP"),
        order: order(marketId),
        snapshot: snapshot(marketId),
        at: NOW,
      settlementLookup: settlementLookup(marketId),
    });
      expect(store.summary().bytes).toBeLessThanOrEqual(maxBytes);
      if (!result.applied) {
        expect(result.reason).toBe("CAPACITY_EXCEEDED");
        break;
      }
      acceptedMarketIds.push(marketId);
    }

    expect(acceptedMarketIds.length).toBeGreaterThan(3);
    // Capacity rejection fail-closes the live instance; durable opens are recovered on restart.
    const mid = new NativeV2CounterfactualStore({
      dataDir,
      startingBalance: 10_000,
      maxBytes,
      segmentMaxBytes: 8_000,
    });
    mid.init();
    expect(mid.openPositionMarketIds().sort()).toEqual([...acceptedMarketIds].sort());
    const midReserved = mid.openPositionMarketIds()
      .reduce((sum, id) => sum + counterfactualSettlementObligationBytes(id), 0);
    expect(mid.summary().bytes + midReserved).toBeLessThanOrEqual(maxBytes);
    expect(mid.summary().bytes).toBeLessThanOrEqual(maxBytes);

    for (const marketId of acceptedMarketIds) {
      const settled = mid.trySettleMarket(marketId, "1", "0", {
        at: NOW,
        source: "x".repeat(200),
        reason: "y".repeat(200),
      });
      expect(settled.applied).toBe(true);
      expect(mid.summary().bytes).toBeLessThanOrEqual(maxBytes);
    }
    expect(mid.openPositionMarketIds()).toEqual([]);
    expect(mid.summary().settlementCount).toBe(acceptedMarketIds.length);

    const finalStore = new NativeV2CounterfactualStore({
      dataDir,
      startingBalance: 10_000,
      maxBytes,
      segmentMaxBytes: 8_000,
    });
    finalStore.init();
    expect(finalStore.openPositionMarketIds()).toEqual([]);
    expect(finalStore.summary().settlementCount).toBe(acceptedMarketIds.length);
    expect(finalStore.summary().bytes).toBeLessThanOrEqual(maxBytes);
  });
  it("settlement preserves exact legal 160-char marketId across restart (no resurrection)", () => {
    const dataDir = tempDir();
    const marketId = "m".repeat(160);
    expect(marketId.length).toBe(160);
    const store = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    store.init();

    const filled = store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey(marketId, "UP"),
      order: order(marketId),
      snapshot: snapshot(marketId),
      at: NOW,
      settlementLookup: settlementLookup(marketId),
    });
    expect(filled.applied).toBe(true);
    expect(store.openPositionMarketIds()).toEqual([marketId]);

    const settled = store.trySettleMarket(marketId, "1", "0", {
      at: new Date("2026-09-17T09:00:00.000Z"),
      source: "x".repeat(200),
      reason: "y".repeat(200),
    });
    expect(settled.applied).toBe(true);
    if (!settled.applied) throw new Error("expected settle");
    expect(settled.marketId).toBe(marketId);
    expect(store.openPositionMarketIds()).toEqual([]);
    const after = store.summary();
    expect(after.settlementCount).toBe(1);
    expect(after.bytes).toBeLessThanOrEqual(after.maxBytes);

    // Durable row must carry the exact identity (not a truncated prefix).
    const segmentsDir = join(dataDir, "segments");
    const segmentFiles = readdirSync(segmentsDir).filter((n) => n.endsWith(".jsonl"));
    expect(segmentFiles.length).toBeGreaterThan(0);
    const text = segmentFiles.map((n) => readFileSync(join(segmentsDir, n), "utf8")).join("");
    const settlementLines = text.split("\n").filter((line) => line.includes('"kind":"settlement"'));
    expect(settlementLines.length).toBe(1);
    const row = JSON.parse(settlementLines[0]!);
    expect(row.marketId).toBe(marketId);
    expect(row.marketId.length).toBe(160);
    expect(Buffer.byteLength(settlementLines[0]! + "\n", "utf8")).toBeLessThanOrEqual(MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES);

    // Unit: builder never truncates a legal 160-char id even with huge metadata.
    const bounded = buildBoundedCounterfactualSettlementRow({
      marketId,
      upPayoff: "1",
      downPayoff: "0",
      cashDelta: "1.25",
      realizedDelta: "0.5",
      at: NOW.toISOString(),
      source: "z".repeat(500),
      reason: "w".repeat(500),
    });
    expect(bounded.marketId).toBe(marketId);
    expect(Buffer.byteLength(JSON.stringify(bounded) + "\n", "utf8")).toBeLessThanOrEqual(MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES);

    const recovered = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    recovered.init();
    expect(recovered.openPositionMarketIds()).toEqual([]);
    expect(recovered.summary().settlementCount).toBe(1);
    expect(recovered.summary().fillCount).toBe(1);
    expect(recovered.summary().availableCashUsd).toBe(after.availableCashUsd);
    expect(recovered.summary().realizedPnl).toBe(after.realizedPnl);
    expect(recovered.summary().fees).toBe(after.fees);
    expect(recovered.summary().bytes).toBeLessThanOrEqual(recovered.summary().maxBytes);
  });
  it("corrupt recovery fail-closes CF and rejects shadow commits until restart stays closed", () => {
    const dataDir = tempDir();
    const segments = join(dataDir, "segments");
    mkdirSync(segments, { recursive: true });
    writeFileSync(join(dataDir, "manifest.json"), JSON.stringify({
      manifestVersion: 1,
      createdAt: NOW.toISOString(),
      updatedAt: NOW.toISOString(),
      startingBalance: "1000",
      fillCount: 1,
      settlementCount: 0,
      openPositionCount: 1,
      realizedPnl: "0",
      fees: "0",
      segmentCount: 1,
      bytes: 100,
      maxBytes: 256000000,
      degraded: false,
      lastError: null,
      lastErrorAt: null,
    }, null, 2) + "\n", "utf8");
    // Mid-file corrupt economic row (not a torn tail) — must fail closed.
    writeFileSync(join(segments, "segment-00001.jsonl"), "{\"kind\":\"fill\",\"not\":\"valid-economic-row\"}\n", "utf8");

    const store = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    store.init();
    expect(store.isRecoveryReady()).toBe(false);
    expect(store.summary().recoveryStatus).toBe("unavailable");
    expect(store.decisionState().recoveryReady).toBe(false);
    const rejected = store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    });
    expect(rejected.applied).toBe(false);
    if (rejected.applied) throw new Error("expected reject");
    expect(rejected.reason).toBe("RECOVERY_UNAVAILABLE");

    const restarted = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    restarted.init();
    expect(restarted.isRecoveryReady()).toBe(false);
    expect(restarted.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    }).applied).toBe(false);
  });

  it("torn truncated tail is physically repaired so later appends stay newline-safe", () => {
    const dataDir = tempDir();
    const good = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    good.init();
    expect(good.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    }).applied).toBe(true);
    const segment = join(dataDir, "segments", "segment-00001.jsonl");
    const beforeTorn = readFileSync(segment, "utf8");
    expect(beforeTorn.endsWith("\n")).toBe(true);
    appendFileSync(segment, '{"kind":"fill","torn":', "utf8"); // no trailing newline
    expect(readFileSync(segment, "utf8").endsWith("\n")).toBe(false);

    const recovered = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    recovered.init();
    expect(recovered.isRecoveryReady()).toBe(true);
    expect(recovered.summary().fillCount).toBe(1);
    expect(recovered.openPositionMarketIds()).toEqual(["m1"]);
    // Physical repair: torn bytes removed; file ends at last committed newline.
    const repaired = readFileSync(segment, "utf8");
    expect(repaired).toBe(beforeTorn);
    expect(repaired.endsWith("\n")).toBe(true);

    expect(recovered.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m2", "UP"),
      order: order("m2"),
      snapshot: snapshot("m2"),
      at: NOW,
      settlementLookup: settlementLookup("m2"),
    }).applied).toBe(true);

    const again = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    again.init();
    expect(again.isRecoveryReady()).toBe(true);
    expect(again.summary().fillCount).toBe(2);
    expect(again.openPositionMarketIds().sort()).toEqual(["m1", "m2"]);
    // Every durable line remains independently parseable (no concatenated torn+row).
    const text = readFileSync(segment, "utf8");
    for (const line of text.split("\n").filter(Boolean)) {
      expect(() => JSON.parse(line)).not.toThrow();
    }
  });

  it("persisted startingBalance mismatch makes CF recovery unavailable (A→B restart)", () => {
    const dataDir = tempDir();
    const a = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    a.init();
    expect(a.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    }).applied).toBe(true);
    const cashA = a.summary().availableCashUsd;

    const b = new NativeV2CounterfactualStore({ dataDir, startingBalance: 2000 });
    b.init();
    expect(b.isRecoveryReady()).toBe(false);
    expect(b.summary().recoveryStatus).toBe("unavailable");
    expect(b.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m2", "UP"),
      order: order("m2"),
      snapshot: snapshot("m2"),
      at: NOW,
      settlementLookup: settlementLookup("m2"),
    }).applied).toBe(false);

    const ok = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    ok.init();
    expect(ok.isRecoveryReady()).toBe(true);
    expect(ok.summary().availableCashUsd).toBe(cashA);
    expect(ok.openPositionMarketIds()).toEqual(["m1"]);
  });

  it("unsafe maxBytes decrease is rejected; safe increase is allowed", () => {
    const dataDir = tempDir();
    const a = new NativeV2CounterfactualStore({
      dataDir,
      startingBalance: 10_000,
      maxBytes: 50_000,
      segmentMaxBytes: 8_000,
    });
    a.init();
    for (let i = 0; i < 5; i += 1) {
      const marketId = `open-${i}`;
      expect(a.tryCommitWouldSubmit({
        actionKey: nativeV2CounterfactualActionKey(marketId, "UP"),
        order: order(marketId),
        snapshot: snapshot(marketId),
        at: NOW,
      settlementLookup: settlementLookup(marketId),
    }).applied).toBe(true);
    }
    const need = a.summary().bytes + a.openPositionMarketIds()
      .reduce((sum, id) => sum + counterfactualSettlementObligationBytes(id), 0);

    const unsafe = new NativeV2CounterfactualStore({
      dataDir,
      startingBalance: 10_000,
      maxBytes: Math.max(1, need - 1),
      segmentMaxBytes: 8_000,
    });
    unsafe.init();
    expect(unsafe.isRecoveryReady()).toBe(false);

    const increased = new NativeV2CounterfactualStore({
      dataDir,
      startingBalance: 10_000,
      maxBytes: 80_000,
      segmentMaxBytes: 8_000,
    });
    increased.init();
    expect(increased.isRecoveryReady()).toBe(true);
    expect(increased.openPositionMarketIds().length).toBe(5);
    expect(increased.maxBytes).toBe(80_000);
  });

  it("settled-market tombstone survives restart and rejects same-market commit", () => {
    const dataDir = tempDir();
    const store = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    store.init();
    expect(store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    }).applied).toBe(true);
    expect(store.trySettleMarket("m1", "1", "0", {
      at: new Date("2026-09-17T09:00:00.000Z"),
      source: "public_polymarket",
      reason: "RESOLVED",
    }).applied).toBe(true);

    const again = store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "DOWN"),
      order: { ...order(), outcome: "DOWN", tokenId: "token-down" },
      snapshot: { ...snapshot(), outcome: "DOWN", tokenId: "token-down" },
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    });
    expect(again.applied).toBe(false);
    if (again.applied) throw new Error("expected reject");
    expect(again.reason).toBe("MARKET_SETTLED");

    const recovered = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    recovered.init();
    expect(recovered.hasSettledMarket("m1")).toBe(true);
    const afterRestart = recovered.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: settlementLookup("m1"),
    });
    expect(afterRestart.applied).toBe(false);
    if (afterRestart.applied) throw new Error("expected reject");
    expect(afterRestart.reason).toBe("MARKET_SETTLED");
  });

  it("exact JSON-escaped identity obligation covers worst-case legal idSchema marketId", () => {
    const dataDir = tempDir();
    const marketId = String.fromCharCode(1).repeat(160);
    expect(marketId.length).toBe(160);
    const obligation = counterfactualSettlementObligationBytes(marketId);
    expect(obligation).toBeGreaterThan(MAX_COUNTERFACTUAL_SETTLEMENT_ROW_BYTES);

    const maxBytes = 20_000;
    const store = new NativeV2CounterfactualStore({
      dataDir,
      startingBalance: 1000,
      maxBytes,
      segmentMaxBytes: 8_000,
    });
    store.init();
    const filled = store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey(marketId, "UP"),
      order: order(marketId),
      snapshot: snapshot(marketId),
      at: NOW,
      settlementLookup: settlementLookup(marketId),
    });
    expect(filled.applied).toBe(true);
    expect(store.summary().bytes + obligation).toBeLessThanOrEqual(maxBytes);

    const settled = store.trySettleMarket(marketId, "1", "0", {
      at: NOW,
      source: "x".repeat(200),
      reason: "y".repeat(200),
    });
    expect(settled.applied).toBe(true);
    expect(store.summary().bytes).toBeLessThanOrEqual(maxBytes);

    const recovered = new NativeV2CounterfactualStore({
      dataDir,
      startingBalance: 1000,
      maxBytes,
      segmentMaxBytes: 8_000,
    });
    recovered.init();
    expect(recovered.openPositionMarketIds()).toEqual([]);
    expect(recovered.hasSettledMarket(marketId)).toBe(true);
    expect(recovered.summary().settlementCount).toBe(1);
    expect(recovered.summary().bytes).toBeLessThanOrEqual(maxBytes);
  });


  it("capacity exhaustion on would-submit fail-closes CF for subsequent evaluations", () => {
    const dataDir = tempDir();
    const store = new NativeV2CounterfactualStore({
      dataDir,
      startingBalance: 1000,
      maxBytes: 400,
      segmentMaxBytes: 8_000,
    });
    store.init();
    let rejected = null;
    for (let i = 0; i < 50; i += 1) {
      const marketId = `cap-${i}`;
      const result = store.tryCommitWouldSubmit({
        actionKey: nativeV2CounterfactualActionKey(marketId, "UP"),
        order: order(marketId),
        snapshot: snapshot(marketId),
        at: NOW,
      settlementLookup: settlementLookup(marketId),
    });
      if (!result.applied) {
        rejected = result;
        break;
      }
    }
    expect(rejected).not.toBeNull();
    expect(rejected?.reason).toBe("CAPACITY_EXCEEDED");
    expect(store.isRecoveryReady()).toBe(false);
    expect(store.decisionState().recoveryReady).toBe(false);
    const followUp = store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("follow", "UP"),
      order: order("follow"),
      snapshot: snapshot("follow"),
      at: NOW,
      settlementLookup: settlementLookup("follow"),
    });
    expect(followUp.applied).toBe(false);
    if (followUp.applied) throw new Error("expected reject");
    expect(followUp.reason).toBe("RECOVERY_UNAVAILABLE");
  });

  it("settlement source/reason clamps are UTF-8 JSON byte-safe (not char-count)", () => {
    const multibyte = "€".repeat(80); // 3 bytes each in UTF-8
    const bounded = buildBoundedCounterfactualSettlementRow({
      marketId: "m1",
      upPayoff: "1",
      downPayoff: "0",
      cashDelta: "1",
      realizedDelta: "1",
      at: NOW.toISOString(),
      source: multibyte,
      reason: multibyte,
    });
    expect(Buffer.byteLength(JSON.stringify(bounded.source), "utf8")).toBeLessThanOrEqual(
      MAX_COUNTERFACTUAL_SETTLEMENT_SOURCE_JSON_BYTES,
    );
    expect(Buffer.byteLength(JSON.stringify(bounded.reason), "utf8")).toBeLessThanOrEqual(
      MAX_COUNTERFACTUAL_SETTLEMENT_SOURCE_JSON_BYTES,
    );
    // Escape-aware: quotes/backslashes expand under JSON.stringify
    const escaped = clampJsonStringUtf8Bytes('x"\\'.repeat(40), MAX_COUNTERFACTUAL_SETTLEMENT_SOURCE_JSON_BYTES);
    expect(Buffer.byteLength(JSON.stringify(escaped), "utf8")).toBeLessThanOrEqual(
      MAX_COUNTERFACTUAL_SETTLEMENT_SOURCE_JSON_BYTES,
    );
    const obligation = counterfactualSettlementObligationBytes("m1");
    const worst = buildBoundedCounterfactualSettlementRow({
      marketId: "m1",
      upPayoff: "1",
      downPayoff: "0",
      cashDelta: "9".repeat(256),
      realizedDelta: "9".repeat(256),
      at: "2".repeat(64),
      source: multibyte,
      reason: multibyte,
    });
    expect(Buffer.byteLength(`${JSON.stringify(worst)}\n`, "utf8")).toBeLessThanOrEqual(obligation);
  });

  it("persists settlement lookup on CF fill and recovers it after restart", () => {
    const dataDir = tempDir();
    const lookup = {
      marketId: "m1",
      slug: "btc-updown-15m",
      endDate: "2026-09-17T09:00:00.000Z",
      yesMapsTo: "DOWN" as const,
      noMapsTo: "UP" as const,
    };
    const store = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    store.init();
    expect(store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
      settlementLookup: lookup,
    }).applied).toBe(true);
    expect(store.settlementLookupFor("m1")).toEqual(lookup);

    const recovered = new NativeV2CounterfactualStore({ dataDir, startingBalance: 1000 });
    recovered.init();
    expect(recovered.isRecoveryReady()).toBe(true);
    expect(recovered.settlementLookupFor("m1")).toEqual(lookup);
    expect(recovered.openSettlementLookups()).toEqual([lookup]);
  });

  it("rejects durable would-submit without authoritative settlementLookup", () => {
    const store = new NativeV2CounterfactualStore({ dataDir: tempDir(), startingBalance: 1000 });
    store.init();
    const result = store.tryCommitWouldSubmit({
      actionKey: nativeV2CounterfactualActionKey("m1", "UP"),
      order: order(),
      snapshot: snapshot(),
      at: NOW,
    });
    expect(result.applied).toBe(false);
    if (result.applied) throw new Error("expected reject");
    expect(result.reason).toBe("MISSING_SETTLEMENT_LOOKUP");
    expect(store.summary().fillCount).toBe(0);
    expect(store.settlementLookupFor("m1")).toBeNull();
  });


});
