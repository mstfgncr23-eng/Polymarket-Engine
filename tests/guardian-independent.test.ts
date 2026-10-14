import { describe, it, expect } from "vitest";
import { spawn } from "node:child_process";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { randomUUID } from "node:crypto";
import { MemoryEventJournal, type EventJournal } from "@pm/db";
import { isolatedPostgres } from "./simple-postgres-fixture.js";
import { evaluateGuardianV2 } from "@pm/core";
import { EngineService } from "../apps/engine/src/engine.js";
import { GuardianMonitor, GuardianUpdateLane, runGuardian } from "../apps/engine/src/guardian-worker.js";
import { guardianRpc, startGuardianSocket, type GuardianView } from "../apps/engine/src/guardian-link.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv, paperFill } from "./paper-engine-testkit.js";
import { snapshot } from "./fixtures.js";

const START = Date.parse("2026-10-05T00:00:00Z");
async function setup(journal: EventJournal = new MemoryEventJournal(), clock: { ms: number; live?: boolean } = { ms: START },
  marketIds = ["first", "second"], asset: "SOL" | "ETH" = "SOL", timings: Array<Record<string, unknown>> = []) {
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport(marketIds.map(id => qualified(id, asset, id + "-up", id + "-down", new Date(clock.ms + 3600000).toISOString()))));
  const engine = new EngineService(qualifiedPaperEnv({ DISCOVERY_INTERVAL_MS: "3600000", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    PAPER_EXECUTION_STYLE: "TAKER", PAPER_EXECUTION_LATENCY_MS: "0", NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE: "0.12",
    PROFIT_TRAILING_PAIR_SOAK_ENABLED: "true", REVERSAL_PROTECTION_ENABLED: "true", GUARDIAN_SEPARATE_ENABLED: "true" }),
    journal, { error: () => undefined, info: (row: Record<string, unknown>) => { timings.push(row); } }, { market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(), now: () => new Date(clock.live ? Date.now() : clock.ms), resolutionFetcher: async () => null });
  await engine.start(); await market.waitForHandlers();
  return { engine, journal, clock, market, asset };
}
type Scenario = Awaited<ReturnType<typeof setup>>;
async function seed(s: Scenario, id: string) {
  await s.engine.applyObservedFill(paperFill(id, "UP", "0.80", { tokenId: id + "-up", quantity: "5", timestamp: new Date(s.clock.ms).toISOString() }));
}
function books(s: Scenario, id: string, ratio = .75) {
  const internals = s.engine as unknown as { snapshots: Map<string, object> };
  const position = s.engine.ledger.positions.get(id)!;
  const held = position.unpairedOutcome!;
  const entry = Number(held === "UP" ? position.averageUpCost : position.averageDownCost);
  const pair = Object.fromEntries((["UP", "DOWN"] as const).map(outcome => {
    const mid = outcome === held ? ratio * entry : .71;
    const book = snapshot({ marketId: id, tokenId: id + "-" + outcome.toLowerCase(), outcome, asset: s.asset,
      bids: [{ price: String(mid - .005), quantity: outcome === held ? "10000" : "30000" }],
      asks: [{ price: String(mid + .005), quantity: outcome === held ? "30000" : "10000" }],
      receivedAt: new Date(s.clock.ms).toISOString(), sourceTimestamp: new Date(s.clock.ms).toISOString() });
    s.market.markTokenSync(book.tokenId, "SYNCED"); s.engine.state.marketSnapshots.set(book.tokenId, book);
    return [outcome, book];
  }));
  internals.snapshots.set(id, pair);
}
async function waitFor(check: () => boolean | Promise<boolean>, ms = 4000) {
  const deadline = Date.now() + ms;
  while (!await check()) { if (Date.now() > deadline) throw new Error("condition timeout"); await new Promise(r => setTimeout(r, 10)); }
}

describe("simple independent Guardian", () => {
  it("R2: measured seven-market gate/CPU benchmark keeps each rescue owner and fill unique", async () => {
    const ids = Array.from({ length: 7 }, (_, i) => `bench-${i}`), timings: Array<Record<string, unknown>> = [];
    const s = await setup(undefined, undefined, ids, "SOL", timings);
    try {
      await s.engine.beginExperiment(); for (const id of ids) { await seed(s, id); books(s, id); }
      const monitor = new GuardianMonitor();
      for (const [age, ratio] of [[0, .75], [5000, .70], [10000, .70]] as const) {
        s.clock.ms = START + age; for (const id of ids) books(s, id, ratio);
        const replies = await Promise.all(monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates.map(u => s.engine.acceptGuardianUpdate(u)));
        expect(replies.every(r => r.status === "PERSISTED")).toBe(true);
      }
      const cpu = process.cpuUsage(), started = performance.now(); timings.splice(0);
      s.clock.ms = START + 70000; for (const id of ids) books(s, id, .70);
      await Promise.all(monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates.map(u => s.engine.acceptGuardianUpdate(u)));
      // Concurrent economic fills really change cash/exposure. Bounded plans
      // can require another existing retry slot; this is not a false invalidation.
      for (let attempt = 0; attempt < 8 && s.engine.guardianView().markets.length; attempt++) {
        await new Promise(resolve => setTimeout(resolve, 20));
        s.clock.ms += 5001; for (const id of ids) if (s.engine.ledger.positions.has(id)) books(s, id, .70);
        const execute = s.engine as unknown as { maybeReversalProtection(id: string): Promise<void> };
        await Promise.all(ids.map(id => execute.maybeReversalProtection(id)));
      }
      const elapsed = performance.now() - started, used = process.cpuUsage(cpu);
      const fills = (await s.journal.queryEvents({ types: ["PAPER_FILL"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK");
      expect(fills).toHaveLength(7); expect(new Set(fills.map(e => e.payload.orderId)).size).toBe(7);
      expect(await s.journal.queryEvents({ types: ["RISK_REDUCTION_ADMITTED"] })).toHaveLength(7);
      expect(s.engine.guardianView().markets).toHaveLength(0);
      const waits = timings.filter(t => typeof t.gateWaitMs === "number").map(t => Number(t.gateWaitMs)).sort((a, b) => a - b);
      const percentile = (p: number) => waits[Math.min(waits.length - 1, Math.floor(waits.length * p))];
      expect(waits.length).toBeGreaterThanOrEqual(21);
      console.log(JSON.stringify({ evidence: "R2_SEVEN_MARKET_BENCH", elapsedMs: elapsed, cpuMs: (used.user + used.system) / 1000,
        gateSamples: waits.length, gateWaitP50Ms: percentile(.5), gateWaitP95Ms: percentile(.95), gateWaitMaxMs: waits.at(-1),
        owners: 7, fills: fills.length, duplicates: 0, integrityFault: s.engine.integrityFault() }));
      expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });

  it("R3: production loop/socket measures real monitoring cadence with nine IN_FLIGHT markets", async () => {
    const s = await setup(undefined, { ms: Date.now(), live: true }, Array.from({ length: 9 }, (_, i) => `rpc-${i}`));
    const path = process.platform === "win32" ? "\\\\.\\pipe\\guardian-receipts-" + randomUUID() : join(tmpdir(), randomUUID() + ".sock");
    const controller = new AbortController(); const counts = new Map<string, number>(), views: number[] = [];
    let server: Awaited<ReturnType<typeof startGuardianSocket>> | undefined;
    try {
      await s.engine.beginExperiment(); for (const id of s.engine.ledger.positions.keys()) books(s, id);
      for (let i = 0; i < 9; i++) { await seed(s, `rpc-${i}`); books(s, `rpc-${i}`); }
      server = await startGuardianSocket(path, async input => {
        const message = input as { kind: string; state: { marketId: string } };
        if (message.kind === "VIEW") { views.push(performance.now()); return s.engine.guardianView(); }
        const id = message.state.marketId; counts.set(id, (counts.get(id) ?? 0) + 1); return { status: "IN_FLIGHT" };
      });
      const running = runGuardian(path, controller.signal);
      await new Promise(resolve => setTimeout(resolve, 1750)); controller.abort(); await running;
      expect(views.length).toBeGreaterThanOrEqual(5); expect(counts.size).toBe(9);
      expect(Math.max(...counts.values())).toBeLessThanOrEqual(3);
      const gaps = views.slice(1).map((t, i) => t - views[i]!).sort((a, b) => a - b);
      console.log(JSON.stringify({ evidence: "R3_REAL_LOOP", views: views.length, markets: counts.size,
        requestsPerMarket: [...counts.values()], viewGapMedianMs: gaps[Math.floor(gaps.length / 2)], viewGapMaxMs: gaps.at(-1) }));
    } finally { controller.abort(); await server?.stop(); await s.engine.stop(); }
  }, 10000);

  it.runIf(process.env.SIMPLE_RETENTION_POSTGRES_TEST === "1").each(["constraint", "timeout", "after_commit"] as const)(
    "R1: isolated real PG %s failure has no orphan; same-revision restart follows only durable authority", async mode => {
      const pg = await isolatedPostgres(); let s = await setup(pg.journal);
      const original = pg.journal.commitDurableTransition.bind(pg.journal);
      const unhandled: unknown[] = [], listener = (e: unknown) => { unhandled.push(e); };
      process.on("unhandledRejection", listener);
      try {
        await s.engine.beginExperiment(); await seed(s, "first"); books(s, "first");
        const update = new GuardianMonitor().evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
        let pgCode: string | null = null;
        if (mode === "constraint") await pg.sql`ALTER TABLE durable_transitions ADD CONSTRAINT injected_guardian_failure CHECK (type <> 'REVERSAL_PROTECTION_STATE_UPDATED')`;
        if (mode === "timeout") pg.journal.commitDurableTransition = (input, cursor, options) => original(input, cursor, {
          ...options, beforeInsert: async () => {
            if (input.type === "REVERSAL_PROTECTION_STATE_UPDATED") {
              try {
                await pg.sql.begin(async (tx: typeof pg.sql) => {
                  await tx`SET LOCAL statement_timeout = '20ms'`;
                  await tx`SELECT pg_sleep(0.2)`;
                });
              } catch (error) { pgCode = (error as { code: string }).code; throw error; }
            }
            await options?.beforeInsert?.();
          },
        });
        if (mode === "after_commit") s.engine.setCrashInjectionForTest("AFTER_DURABLE_COMMIT_BEFORE_APPLY", {
          operationKey: `reversal:${update.state.positionId}:${update.state.unhedgedStartedAt}:v2g3:${update.state.revision}`,
        });
        await expect(s.engine.acceptGuardianUpdate(update)).rejects.toThrow();
        await new Promise(resolve => setImmediate(resolve));
        expect((s.engine as unknown as { guardianAdmissions: Map<string, unknown> }).guardianAdmissions.size).toBe(0);
        expect(unhandled).toHaveLength(0); expect(s.engine.integrityFault()).toBe(true);
        expect((await s.engine.acceptGuardianUpdate(update)).reason).toBe("ENGINE_UNAVAILABLE");
        expect(s.engine.guardianView().markets[0]!.state).toBeNull();
        if (mode === "timeout") expect(pgCode).toBe("57014");
        const committed = (await pg.sql`SELECT count(*) AS n FROM durable_transitions WHERE type = 'REVERSAL_PROTECTION_STATE_UPDATED'`)[0].n;
        expect(Number(committed)).toBe(mode === "after_commit" ? 1 : 0);
        pg.journal.commitDurableTransition = original;
        if (mode === "constraint") await pg.sql`ALTER TABLE durable_transitions DROP CONSTRAINT injected_guardian_failure`;
        await s.engine.stop(); s = await setup(pg.journal, s.clock); books(s, "first");
        expect((await s.engine.acceptGuardianUpdate(update)).status).toBe(mode === "after_commit" ? "REPLAY" : "PERSISTED");
        const monitor = new GuardianMonitor();
        for (const [age, ratio] of [[5000, .70], [10000, .70], [70000, .70]] as const) {
          s.clock.ms = START + age; books(s, "first", ratio);
          await s.engine.acceptGuardianUpdate(monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!);
        }
        await waitFor(async () => (await s.journal.queryEvents({ types: ["PAPER_FILL"] })).some(e => e.payload.intent === "PROTECTIVE_DE_RISK"));
        const ledger = s.engine.ledger.snapshot(); await s.engine.stop(); s = await setup(pg.journal, s.clock);
        expect(s.engine.ledger.snapshot().cash).toBe(ledger.cash); expect(s.engine.ledger.snapshot().fees).toBe(ledger.fees);
        const fills = (await s.journal.queryEvents({ types: ["PAPER_FILL"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK");
        expect(fills).toHaveLength(1); expect(s.engine.integrityFault()).toBe(false);
        console.log(JSON.stringify({ evidence: "R1_REAL_PG_FAILURE_RECOVERY", mode, pgCode, committedBeforeRestart: Number(committed),
          orphans: 0, unhandled: unhandled.length, fills: fills.length, cashParity: true, feeParity: true }));
      } finally {
        pg.journal.commitDurableTransition = original; process.off("unhandledRejection", listener);
        await s.engine.stop(); await pg.close();
      }
    }, 20000);

  it.each(["admission", "owner"] as const)("R4: recovery during held %s preserves the original confirmed decision and late receipt/restart gives one fill", async boundary => {
    const journal = new MemoryEventJournal(); let s = await setup(journal);
    let release!: () => void; const held = new Promise<void>(r => { release = r; });
    const original = journal.commitDurableTransition.bind(journal); let started = false;
    try {
      await s.engine.beginExperiment(); await seed(s, "first"); const monitor = new GuardianMonitor();
      for (const [age, ratio] of [[0, .75], [5000, .70], [10000, .70]] as const) {
        s.clock.ms = START + age; books(s, "first", ratio);
        await s.engine.acceptGuardianUpdate(monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!);
      }
      if (boundary === "admission") journal.commitDurableTransition = async (input, cursor, options) => {
        if (input.type === "REVERSAL_PROTECTION_STATE_UPDATED") { started = true; await held; }
        return original(input, cursor, options);
      };
      else journal.setAppendInFlightHoldForTest(held, ["PAPER_ORDER_CREATED"]);
      s.clock.ms = START + 70000; books(s, "first", .70);
      const update = monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
      const receipt = s.engine.acceptGuardianUpdate(update);
      await waitFor(() => boundary === "admission" ? started : journal.inFlightAppends > 0);
      s.clock.ms = START + 90000; books(s, "first", .96);
      const probe = new GuardianMonitor().evaluate(s.engine.guardianView(), s.clock.ms);
      expect(probe.markets[0]!.phase).toBe("PROTECTION_PENDING");
      expect((await s.engine.acceptGuardianUpdate(update)).status).toBe(boundary === "admission" ? "IN_FLIGHT" : "REPLAY");
      release(); journal.setAppendInFlightHoldForTest(null); await receipt;
      await waitFor(() => (s.engine.ledger.positions.get("first")?.unpairedQuantity ?? "0") === "0");
      const fills = (await journal.queryEvents({ types: ["PAPER_FILL"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK");
      expect(fills).toHaveLength(1);
      expect((fills[0]!.payload.deRisk as { guardianReason: string }).guardianReason).toBe("EARLY_TIMEOUT");
      expect(await journal.queryEvents({ types: ["RISK_REDUCTION_ADMITTED"] })).toHaveLength(1);
      const order = fills[0]!.payload.orderId; await s.engine.stop(); s = await setup(journal, s.clock);
      expect((await journal.queryEvents({ types: ["PAPER_FILL"] })).filter(e => e.payload.orderId === order)).toHaveLength(1);
      expect((await s.engine.acceptGuardianUpdate(update)).reason).toBe("EPISODE_CHANGED");
      console.log(JSON.stringify({ evidence: "R4_HELD_RECOVERY", boundary, phase: "PROTECTION_PENDING", fills: 1, orderId: order, restartDuplicates: 0 }));
    } finally { release?.(); journal.setAppendInFlightHoldForTest(null); journal.commitDurableTransition = original; await s.engine.stop(); }
  });

  it("R1: rejected admission promise settles without an orphan or unhandled rejection and fails closed", async () => {
    const s = await setup(); const unhandled: unknown[] = [];
    const listener = (error: unknown) => { unhandled.push(error); };
    process.on("unhandledRejection", listener);
    try {
      await s.engine.beginExperiment(); await seed(s, "first"); books(s, "first");
      const update = new GuardianMonitor().evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
      const internals = s.engine as unknown as { guardianAdmissions: Map<string, unknown>;
        enqueueGuardianPersistence(): Promise<never> };
      internals.enqueueGuardianPersistence = () => Promise.reject(new Error("INJECTED_ADMISSION_REJECTION"));
      await expect(s.engine.acceptGuardianUpdate(update)).rejects.toThrow("INJECTED_ADMISSION_REJECTION");
      await new Promise(resolve => setImmediate(resolve));
      expect(internals.guardianAdmissions.size).toBe(0);
      expect(unhandled).toHaveLength(0); expect(s.engine.integrityFault()).toBe(true);
      expect(s.engine.guardianView().markets[0]!.state).toBeNull();
      expect((await s.engine.acceptGuardianUpdate(update)).reason).toBe("ENGINE_UNAVAILABLE");
      expect((await s.journal.queryEvents({ types: ["PAPER_FILL"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK")).toHaveLength(0);
    } finally { process.off("unhandledRejection", listener); await s.engine.stop(); }
  });

  it.each([5000, 60000])("R3: %ims IN_FLIGHT is bounded while eight markets keep monitoring; late VIEW receipt drops the request", async duration => {
    const ids = Array.from({ length: 8 }, (_, i) => `receipt-${i}`), s = await setup(undefined, undefined, ids);
    try {
      await s.engine.beginExperiment(); for (const id of ids) { await seed(s, id); books(s, id); }
      const monitor = new GuardianMonitor(), view = s.engine.guardianView();
      const updates = monitor.evaluate(view, s.clock.ms).updates;
      let elapsed = 0, cycles = 0; const counts = new Map<string, number>();
      const lane = new GuardianUpdateLane(async u => {
        counts.set(u.state.marketId, (counts.get(u.state.marketId) ?? 0) + 1);
        return { status: "IN_FLIGHT" };
      }, monitor, () => undefined, () => elapsed);
      for (; elapsed < duration; elapsed += 250) {
        lane.reconcile(view); monitor.evaluate(view, s.clock.ms); cycles++;
        for (const update of updates) lane.offer(update);
        lane.pump(); await lane.drain();
      }
      expect(cycles).toBe(duration / 250); expect(counts.size).toBe(8);
      expect(Math.max(...counts.values())).toBeLessThanOrEqual(duration === 5000 ? 4 : 15);
      // Receipt reconciliation is immediate, even before the next probe is due.
      for (const market of view.markets) market.state = updates.find(u => u.state.marketId === market.position.marketId)!.state;
      lane.reconcile(view); elapsed += 5000; lane.pump(); await lane.drain();
      const final = [...counts.values()]; expect(final.every(n => n === final[0])).toBe(true);
      console.log(JSON.stringify({ evidence: "R3_BOUNDED_RECEIPTS", durationMs: duration, cycles, requestsPerMarket: final }));
    } finally { await s.engine.stop(); }
  });

  it.each([-1, 0, 1])("R4: recovery at deadline offset %ims retains real core semantics after a stale rejection", async offset => {
    const s = await setup();
    try {
      await s.engine.beginExperiment(); await seed(s, "first"); const monitor = new GuardianMonitor();
      for (const [t, ratio] of [[0, .75], [5000, .70], [10000, .70]] as const) {
        s.clock.ms = START + t; books(s, "first", ratio);
        await s.engine.acceptGuardianUpdate(monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!);
      }
      const watch = s.engine.guardianView().markets[0]!.state!;
      const deadline = Date.parse(watch.deadlineAt!), age = deadline - START + offset;
      // A pre-deadline emergency confirmation is never admitted, then ages out.
      s.clock.ms = deadline - 17000; books(s, "first", .15);
      const old = monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
      expect(old.state.protectionReason).toBe("EARLY_EMERGENCY");
      s.clock.ms = START + age; books(s, "first", .96);
      const rejected = await s.engine.acceptGuardianUpdate(old);
      expect(rejected.reason).toBe("STATE_STALE_OR_FUTURE"); monitor.rejected(old, rejected.reason!);
      const next = monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
      expect(next.state.deadlineAt).toBe(offset < 0 ? null : watch.deadlineAt);
      expect(next.state.phase).toBe(offset < 0 ? "OBSERVING" : "PROTECTION_PENDING");
      expect(next.state.protectionReason).toBe(offset < 0 ? null : "EARLY_TIMEOUT");
      expect((await s.engine.acceptGuardianUpdate(next)).status).toBe("PERSISTED");
      if (offset >= 0) await waitFor(() => (s.engine.ledger.positions.get("first")?.unpairedQuantity ?? "0") === "0");
      const fills = (await s.journal.queryEvents({ types: ["PAPER_FILL"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK");
      expect(fills).toHaveLength(offset < 0 ? 0 : 1);
      console.log(JSON.stringify({ evidence: "R4_RECOVERY_DEADLINE", age, offset, phase: next.state.phase, fills: fills.length, unchangedDeadline: offset >= 0 }));
    } finally { await s.engine.stop(); }
  });

  it("R2: seven-market mark/audit/error activity cannot exhaust the target's rescue quote budget", async () => {
    const ids = ["first", ...Array.from({ length: 6 }, (_, i) => `other-${i}`)], s = await setup(undefined, undefined, ids);
    try {
      await s.engine.beginExperiment(); for (const id of ids) { await seed(s, id); books(s, id); }
      const monitor = new GuardianMonitor();
      for (const [age, ratio] of [[0, .75], [5000, .70], [10000, .70]] as const) {
        s.clock.ms = START + age; books(s, "first", ratio);
        await s.engine.acceptGuardianUpdate(monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates.find(u => u.state.marketId === "first")!);
      }
      let plans = 0; const original = s.engine.persistReportInput.bind(s.engine);
      s.engine.persistReportInput = async input => {
        if (input.type === "RISK_EVALUATION" && input.payload.marketId === "first") {
          plans++;
          for (const id of ids.slice(1)) books(s, id, .75 + plans / 10000);
          s.engine.ledger.revalue(Object.fromEntries(ids.slice(1).map(id => [id, { up: String(.8 + plans / 10000), down: ".71" }])), new Date(s.clock.ms).toISOString());
          (s.engine as unknown as { executionErrors: number[] }).executionErrors.push(s.clock.ms - 3600001);
        }
        return original(input);
      };
      s.clock.ms = START + 70000; books(s, "first", .70);
      const confirm = monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates.find(u => u.state.marketId === "first")!;
      await s.engine.acceptGuardianUpdate(confirm);
      await waitFor(() => plans > 0);
      await waitFor(async () => !(s.engine as unknown as { riskReductionInFlight: Set<string> }).riskReductionInFlight.has("first"));
      expect(plans).toBe(1);
      const fills = (await s.journal.queryEvents({ types: ["PAPER_FILL"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK");
      expect(fills).toHaveLength(1); expect(fills[0]!.payload.quantity).toBe("5");
      expect(s.engine.ledger.positions.get("first")?.unpairedQuantity ?? "0").toBe("0");
      expect((await s.journal.queryEvents({ types: ["RISK_REDUCTION_ADMITTED"] }))).toHaveLength(1);
      expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });

  it.runIf(process.env.SIMPLE_RETENTION_POSTGRES_TEST === "1")("isolated existing LOCAL PostgreSQL: protection receipt, fill and restart ledger parity", async () => {
    const pg = await isolatedPostgres(); let s = await setup(pg.journal);
    try {
      await s.engine.beginExperiment(); await seed(s, "first"); const monitor = new GuardianMonitor();
      for (const [age, ratio] of [[0, .75], [5000, .70], [10000, .70], [70000, .70]] as const) {
        s.clock.ms = START + age; books(s, "first", ratio);
        expect((await s.engine.acceptGuardianUpdate(monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!)).status).toBe("PERSISTED");
      }
      await waitFor(async () => (await s.journal.queryEvents({ types: ["PAPER_FILL"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK").length === 1);
      const ledger = s.engine.ledger.snapshot();
      await s.engine.stop(); s = await setup(pg.journal, s.clock);
      expect(s.engine.ledger.snapshot().cash).toBe(ledger.cash);
      expect(s.engine.ledger.snapshot().fees).toBe(ledger.fees);
      expect(s.engine.guardianView().markets).toHaveLength(0);
      expect((await s.journal.queryEvents({ types: ["PAPER_FILL"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK")).toHaveLength(1);
      expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); await pg.close(); }
  }, 15000);
  it("documents the retained missing-book deadline semantic difference without changing policy", async () => {
    const s = await setup();
    try {
      await s.engine.beginExperiment(); await seed(s, "first"); const monitor = new GuardianMonitor();
      for (const [age, ratio] of [[0, .75], [5000, .70], [10000, .70]] as const) {
        s.clock.ms = START + age; books(s, "first", ratio);
        await s.engine.acceptGuardianUpdate(monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!);
      }
      const view = s.engine.guardianView(), watch = view.markets[0]!.state!;
      s.clock.ms = START + 70000; view.markets[0]!.down = null;
      expect(monitor.evaluate(view, s.clock.ms).updates).toHaveLength(0);
      const inline = evaluateGuardianV2({ generation: view.generation!, state: watch, episode: watch,
        entryPrice: watch.entryPrice, now: new Date(s.clock.ms), observation: null });
      expect(inline.nextState!.protectionReason).toBe("EARLY_TIMEOUT");
    } finally { await s.engine.stop(); }
  });

  it.each(["old", "future"])("Engine rejects a %s source timestamp even when receivedAt is current", async mode => {
    const s = await setup();
    try {
      await s.engine.beginExperiment(); await seed(s, "first"); books(s, "first");
      const update = new GuardianMonitor().evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
      const pair = (s.engine as unknown as { snapshots: Map<string, { UP: { sourceTimestamp: string } }> }).snapshots.get("first")!;
      pair.UP.sourceTimestamp = new Date(s.clock.ms + (mode === "future" ? 1 : -16000)).toISOString();
      expect((await s.engine.acceptGuardianUpdate(update)).reason).toBe("BOOK_UNAVAILABLE");
      expect(s.engine.guardianView().markets[0]!.state).toBeNull();
    } finally { await s.engine.stop(); }
  });
  it("a real 5s socket timeout does not cancel accepted work: IN_FLIGHT, late receipt and restart produce one fill", async () => {
    let s = await setup();
    const path = process.platform === "win32" ? "\\\\.\\pipe\\guardian-late-" + randomUUID() : join(tmpdir(), randomUUID() + ".sock");
    let release!: () => void; const held = new Promise<void>(r => release = r);
    let server: Awaited<ReturnType<typeof startGuardianSocket>> | undefined;
    try {
      await s.engine.beginExperiment(); await seed(s, "first"); const monitor = new GuardianMonitor();
      for (const [age, ratio] of [[0, .75], [5000, .70], [10000, .70]] as const) {
        s.clock.ms = START + age; books(s, "first", ratio);
        await s.engine.acceptGuardianUpdate(monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!);
      }
      const original = s.journal.commitDurableTransition.bind(s.journal);
      s.journal.commitDurableTransition = async (input, cursor, options) => {
        if (input.type === "REVERSAL_PROTECTION_STATE_UPDATED" && (input.payload.state as { confirmedAt?: string })?.confirmedAt) await held;
        return original(input, cursor, options);
      };
      s.clock.ms = START + 70000; books(s, "first", .7);
      const update = monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
      server = await startGuardianSocket(path, input => s.engine.acceptGuardianUpdate(input));
      await expect(guardianRpc(path, { kind: "UPDATE", ...update })).rejects.toThrow("GUARDIAN_SOCKET_TIMEOUT");
      s.clock.ms += 16000; books(s, "first", .7);
      expect((await s.engine.acceptGuardianUpdate(update)).status).toBe("IN_FLIGHT");
      expect(s.engine.guardianView().markets[0]!.state!.confirmedAt).toBeNull();
      expect(s.engine.guardianView().markets[0]!.admission!.revision).toBe(update.state.revision);
      const restartedMonitor = new GuardianMonitor();
      const probe = restartedMonitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
      expect(probe.state).toEqual(update.state); expect(probe.events).toHaveLength(0);
      expect((await s.engine.acceptGuardianUpdate(probe)).status).toBe("IN_FLIGHT");
      release();
      await waitFor(async () => (await s.journal.queryEvents({ types: ["PAPER_FILL"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK").length === 1);
      await server.stop(); server = undefined; await s.engine.stop(); s = await setup(s.journal, s.clock);
      expect(s.engine.guardianView().markets).toHaveLength(0);
      expect((await s.journal.queryEvents({ types: ["PAPER_FILL"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK")).toHaveLength(1);
      expect(s.engine.integrityFault()).toBe(false);
    } finally { release(); await server?.stop(); await s.engine.stop(); }
  }, 12000);

  it("episode replacement and Engine restart discard rejected local watch context", async () => {
    const s = await setup();
    try {
      await s.engine.beginExperiment(); await seed(s, "first"); books(s, "first"); const monitor = new GuardianMonitor();
      const oldView = s.engine.guardianView(); monitor.evaluate(oldView, s.clock.ms);
      const replacement = structuredClone(oldView);
      replacement.markets[0]!.position.id += "-new";
      const update = monitor.evaluate(replacement, s.clock.ms).updates[0]!;
      expect(update.state.positionId).toBe(replacement.markets[0]!.position.id); expect(update.state.revision).toBe(1);
      replacement.engineStartedAt = new Date(s.clock.ms + 1).toISOString();
      expect(monitor.evaluate(replacement, s.clock.ms).updates[0]!.state.revision).toBe(1);
    } finally { await s.engine.stop(); }
  });

  it("LATE uses fresh, timestamped consensus; expired/missing/future consensus cannot arm", async () => {
    const s = await setup();
    try {
      await s.engine.beginExperiment(); await seed(s, "first"); s.clock.ms += 200000; books(s, "first", .6);
      const view = s.engine.guardianView();
      for (const kind of ["fresh", "expired", "future", "missing"] as const) {
        const copy = structuredClone(view); copy.markets[0]!.consensus = { eventId: kind, direction: "DOWN", confidence: .7, estimatedEdge: .02,
          ...(kind !== "missing" ? { calculatedAt: new Date(s.clock.ms + (kind === "future" ? 1 : 0)).toISOString(),
            expiresAt: new Date(s.clock.ms + (kind === "expired" ? -1 : 5000)).toISOString() } : {}) };
        const result = new GuardianMonitor().evaluate(copy, s.clock.ms);
        expect(result.markets[0]!.phase).toBe(kind === "fresh" ? "LATE_CONFIRMATION" : null);
      }
    } finally { await s.engine.stop(); }
  });

  it("transport admission is bounded even when every market ACK is stalled", async () => {
    const s = await setup(); let release!: () => void;
    try {
      await s.engine.beginExperiment(); await seed(s, "first"); books(s, "first");
      const monitor = new GuardianMonitor(), view = s.engine.guardianView();
      const update = monitor.evaluate(view, s.clock.ms).updates[0]!;
      const held = new Promise<{ status: string }>(r => { release = () => r({ status: "PERSISTED" }); });
      let active = 0; const lane = new GuardianUpdateLane(async () => { active++; return held; }, monitor, () => undefined);
      lane.reconcile(view);
      for (let i = 0; i < 64; i++) { const u = structuredClone(update); u.state.marketId = `bounded-${i}`; lane.offer(u); }
      const extra = structuredClone(update); extra.state.marketId = "overflow";
      expect(() => lane.offer(extra)).toThrow("GUARDIAN_UPDATE_BACKPRESSURE");
      lane.pump(); expect(active).toBe(8); release(); await lane.drain();
    } finally { release?.(); await s.engine.stop(); }
  });
  it("5588451: definitively rejected stale confirmation is re-evaluated from its watch, not cosmetically restamped", async () => {
    const incidentStart = Date.parse("2026-10-12T22:01:20.894Z"), id = "5588451";
    const s = await setup(new MemoryEventJournal(), { ms: incidentStart }, [id], "ETH");
    try {
      await s.engine.beginExperiment();
      await s.engine.applyObservedFill(paperFill(id, "DOWN", "0.63", { asset: "ETH", tokenId: id + "-down",
        quantity: "7.93650794", feeUsd: "0.12950", timestamp: new Date(incidentStart).toISOString() }));
      const monitor = new GuardianMonitor();
      for (const [age, ratio] of [[0, .75], [5000, .70], [10000, .70]] as const) {
        s.clock.ms = incidentStart + age; books(s, id, ratio);
        const u = monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
        if (!age) await s.engine.acceptGuardianUpdate(u); // only rev1 durable, as in the incident
      }
      for (let age = 11000; age <= 18000; age += 1000) {
        s.clock.ms = incidentStart + age; books(s, id, .70); monitor.evaluate(s.engine.guardianView(), s.clock.ms);
      }
      s.clock.ms = incidentStart + 70000; books(s, id, .70);
      const old = monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
      expect(old.state.revision).toBe(12);
      s.clock.ms = incidentStart + 111729; books(s, id, .70);
      let reply = { status: "", reason: "" as string | undefined };
      for (let i = 0; i < 89; i++) {
        s.clock.ms = incidentStart + 111729 + Math.round(i * 32600 / 88); books(s, id, .70);
        const retry = monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
        expect(retry.state.updatedAt).toBe(old.state.updatedAt);
        reply = await s.engine.acceptGuardianUpdate(retry);
        expect(reply.reason).toBe("STATE_STALE_OR_FUTURE");
      }
      // Production receipt processing; absent in the starting release.
      monitor.rejected(old, reply.reason!);
      const fresh = monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
      expect(fresh.state.updatedAt).toBe(new Date(s.clock.ms).toISOString());
      expect(fresh.state.confirmedAt).toBe(fresh.state.updatedAt);
      expect(fresh.state.armedAt).toBe(old.state.armedAt);
      expect(fresh.state.deadlineAt).toBe(old.state.deadlineAt);
      expect(fresh.events).toContain("EARLY_TIMEOUT_PROTECT");
      expect((await s.engine.acceptGuardianUpdate(fresh)).status).toBe("PERSISTED");
      await waitFor(async () => (await s.journal.queryEvents({ types: ["PAPER_FILL"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK").length === 1);
      console.log(JSON.stringify({ evidence: "5588451_RECOVERY", staleRetries: 89, durableBefore: 1, requestedRevision: old.state.revision,
        marketId: id, heldOutcome: "DOWN", entry: "0.63", quantity: "7.93650794", retryWindow: "22:03:12.623Z..22:03:45.223Z",
        acceptedAfterReevaluation: 1, protectiveFills: 1, syntheticQuoteFixture: true }));
    } finally { await s.engine.stop(); }
  });

  it("a held 5s ACK in one of seven markets does not stop the other six", async () => {
    const s = await setup(); const abort = new AbortController();
    const path = process.platform === "win32" ? "\\\\.\\pipe\\guardian-convoy-" + randomUUID() : join(tmpdir(), randomUUID() + ".sock");
    let release!: () => void; const held = new Promise<void>(r => release = r);
    let server: Awaited<ReturnType<typeof startGuardianSocket>> | undefined;
    let running: Promise<void> | undefined;
    try {
      await s.engine.beginExperiment(); await seed(s, "first"); books(s, "first");
      const view = s.engine.guardianView(); view.now = new Date().toISOString();
      view.markets = Array.from({ length: 7 }, (_, i) => {
        const m = structuredClone(view.markets[0]!); const id = `convoy-${i}`;
        m.position.marketId = id; m.position.id = id; m.position.openedAt = view.now; m.position.unhedgedStartedAt = view.now;
        for (const b of [m.up!, m.down!]) { b.marketId = id; b.sourceTimestamp = view.now; b.receivedAt = view.now; }
        return m;
      });
      const seen = new Set<string>(), counts = new Map<string, number>(); let views = 0;
      server = await startGuardianSocket(path, async input => {
        const r = input as { kind: string; state: { marketId: string } };
        if (r.kind === "VIEW") { views++; return { ...view, now: new Date().toISOString() }; }
        seen.add(r.state.marketId);
        counts.set(r.state.marketId, (counts.get(r.state.marketId) ?? 0) + 1);
        if (r.state.marketId === "convoy-0") await held;
        return { status: "PERSISTED" };
      });
      running = runGuardian(path, abort.signal);
      await new Promise(r => setTimeout(r, 600));
      expect(seen.size).toBe(7);
      // Keep the first server handler stalled through the real RPC timeout.
      await new Promise(r => setTimeout(r, 4950));
      expect(views).toBeGreaterThanOrEqual(15);
      for (let i = 1; i < 7; i++) expect(counts.get(`convoy-${i}`)).toBeGreaterThanOrEqual(4);
      console.log(JSON.stringify({ evidence: "SEVEN_MARKET_ACK_ISOLATION", progressedBeforeHeldAck: seen.size,
        budgetMs: 600, heldThroughMs: 5550, completedViews: views, healthyMarketRequests: Object.fromEntries(counts) }));
    } finally { release(); abort.abort(); await running; await server?.stop(); await s.engine.stop(); }
  }, 10000);
  it("acknowledges routine checkpoint admission without claiming durability or waiting behind a held write", async () => {
    const s = await setup(); let release = () => undefined;
    try {
      expect((await s.engine.beginExperiment()).ok).toBe(true); await seed(s, "first"); books(s, "first");
      const monitor = new GuardianMonitor();
      await s.engine.acceptGuardianUpdate(monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!);
      const hold = new Promise<void>(resolve => { release = resolve; });
      const original = s.journal.commitDurableTransition.bind(s.journal);
      s.journal.commitDurableTransition = async (input, cursor, options) => { await hold; return original(input, cursor, options); };
      s.clock.ms += 1000; books(s, "first", .76);
      const update = monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
      expect((await s.engine.acceptGuardianUpdate(update, false)).status).toBe("QUEUED");
      expect(s.engine.guardianView().markets[0]!.state!.revision).toBe(1);
      release();
      await waitFor(() => s.engine.guardianView().markets[0]!.state!.revision === update.state.revision);
      expect(s.engine.integrityFault()).toBe(false);
    } finally { release(); await s.engine.stop(); }
  });
  it("discovers the second position dynamically; one bad/future/gapped book leaves the other monitoring", async () => {
    const s = await setup();
    try {
      expect((await s.engine.beginExperiment()).ok).toBe(true);
      const monitor = new GuardianMonitor();
      await seed(s, "first"); books(s, "first");
      expect(monitor.evaluate(s.engine.guardianView(), s.clock.ms).markets).toHaveLength(1);
      await seed(s, "second"); books(s, "second");
      const view = s.engine.guardianView();
      expect(monitor.evaluate(view, s.clock.ms).markets).toHaveLength(2);
      for (const broken of ["missing", "future", "gapped", "invalid"] as const) {
        const copy = structuredClone(view);
        if (broken === "missing") copy.markets[0]!.down = null;
        if (broken === "future") copy.markets[0]!.up!.sourceTimestamp = new Date(s.clock.ms + 1).toISOString();
        if (broken === "gapped") copy.markets[0]!.synchronized = false;
        if (broken === "invalid") copy.markets[0]!.up!.bids[0]!.price = "nan";
        const result = monitor.evaluate(copy, s.clock.ms);
        expect(result.markets[0]!.reason).not.toBe("OBSERVING");
        expect(result.markets[1]!.reason).toBe("OBSERVING");
        expect(result.updates.some(u => u.state.marketId === "first")).toBe(false);
      }
      const skew = structuredClone(view); skew.markets[0]!.down!.sourceTimestamp = new Date(s.clock.ms - 3000).toISOString();
      expect(monitor.evaluate(skew, s.clock.ms).markets[0]!.reason).toBe("OBSERVING");
      expect((s.engine as unknown as { guardianRuntimeStates: Map<string, unknown> }).guardianRuntimeStates.size).toBe(0);
    } finally { await s.engine.stop(); }
  });

  it("existing policy persists, recovers, executes one actual PAPER protection and does not duplicate on concurrent retries or Engine restart", async () => {
    let s = await setup();
    try {
      expect((await s.engine.beginExperiment()).ok).toBe(true);
      await seed(s, "first");
      let monitor = new GuardianMonitor();
      for (const [age, ratio] of [[0, .75], [5000, .70], [10000, .70]] as const) {
        s.clock.ms = START + age; books(s, "first", ratio);
        const update = monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
        expect((await s.engine.acceptGuardianUpdate(update)).status).toBe("PERSISTED");
      }
      expect(s.engine.guardianView().markets[0]!.state?.phase).toBe("EARLY_RECOVERY_WATCH");
      await s.engine.stop(); s = await setup(s.journal, s.clock);
      books(s, "first", .7);
      monitor = new GuardianMonitor();
      expect(monitor.evaluate(s.engine.guardianView(), s.clock.ms).markets[0]!.phase).toBe("EARLY_RECOVERY_WATCH");
      s.clock.ms = START + 70000; books(s, "first", .7);
      s.engine.state.noteReferenceTick("SOL", new Date(s.clock.ms).toISOString());
      const update = monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!;
      expect(update.state.protectionReason).toBe("EARLY_TIMEOUT");
      const replies = await Promise.all([s.engine.acceptGuardianUpdate(update), s.engine.acceptGuardianUpdate(update)]);
      expect(replies.every(r => ["PERSISTED", "REPLAY", "IN_FLIGHT"].includes(r.status))).toBe(true);
      await waitFor(async () => (await s.journal.queryEvents({ types: ["PAPER_FILL"] })).some(e => e.payload.intent === "PROTECTIVE_DE_RISK"));
      const fills = await s.journal.queryEvents({ types: ["PAPER_FILL"] });
      expect(fills.filter(e => e.payload.intent === "PROTECTIVE_DE_RISK")).toHaveLength(1);
      expect((await s.engine.acceptGuardianUpdate(update)).reason).toBe("EPISODE_CHANGED");
      await s.engine.stop(); s = await setup(s.journal, s.clock);
      expect(s.engine.guardianView().markets).toHaveLength(0);
      expect((await s.journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK")).toHaveLength(1);
      expect(s.engine.integrityFault()).toBe(false);
    } finally { await s.engine.stop(); }
  });

  it("runs the production Guardian in a separate PID and reconnects to the same socket", async () => {
    const path = process.platform === "win32" ? "\\\\.\\pipe\\guardian-" + randomUUID() : join(tmpdir(), "guardian-" + randomUUID() + ".sock");
    const view: GuardianView = { mode: "paper", enginePid: process.pid, engineStartedAt: new Date().toISOString(), experimentId: null,
      enabled: false, generation: null, now: new Date().toISOString(), staleMs: 15000,
      feed: { state: "DISCONNECTED", lastMessageAt: null, reconnects: 0 }, markets: [] };
    let requests = 0;
    const handler = async () => { requests++; return { ...view, now: new Date().toISOString() }; };
    let server = await startGuardianSocket(path, handler);
    const child = spawn(process.execPath, ["apps/engine/dist/guardian-main.js"], {
      env: { ...process.env, TRADING_MODE: "paper", GUARDIAN_SOCKET_PATH: path }, stdio: ["ignore", "pipe", "pipe"] });
    let output = ""; child.stdout.on("data", chunk => { output += chunk; });
    try {
      expect(child.pid).not.toBe(process.pid);
      await waitFor(() => output.includes('"event":"CYCLE"'));
      await server.stop();
      const before = requests;
      await new Promise(r => setTimeout(r, 700));
      expect(child.exitCode).toBeNull();
      server = await startGuardianSocket(path, handler);
      await waitFor(() => requests > before + 2);
      expect((await guardianRpc<GuardianView>(path, { kind: "VIEW" })).enginePid).toBe(process.pid);
    } finally {
      child.kill(); await server.stop();
      await new Promise<void>(resolve => { if (child.exitCode !== null) resolve(); else child.once("close", () => resolve()); });
    }
  }, 15000);

  it("production Guardian PID requests a real Engine PAPER fill over the socket and retry cannot produce another order", async () => {
    const entered = Date.now() - 70000;
    const s = await setup(new MemoryEventJournal(), { ms: entered });
    const path = process.platform === "win32" ? "\\\\.\\pipe\\guardian-e2e-" + randomUUID() : join(tmpdir(), "guardian-e2e-" + randomUUID() + ".sock");
    let child: ReturnType<typeof spawn> | undefined;
    let server: Awaited<ReturnType<typeof startGuardianSocket>> | undefined;
    let protection: unknown;
    try {
      expect((await s.engine.beginExperiment()).ok).toBe(true);
      await seed(s, "first");
      // Restore an actual policy watch from its prior observations; no fabricated
      // confirmation. The production worker must itself expire it and request execution.
      const monitor = new GuardianMonitor();
      for (const [age, ratio] of [[0, .75], [5000, .70], [10000, .70]] as const) {
        s.clock.ms = entered + age; books(s, "first", ratio);
        expect((await s.engine.acceptGuardianUpdate(monitor.evaluate(s.engine.guardianView(), s.clock.ms).updates[0]!)).status).toBe("PERSISTED");
      }
      s.clock.ms = Date.now(); books(s, "first", .7); s.clock.live = true;
      s.engine.state.noteReferenceTick("SOL", new Date().toISOString());
      server = await startGuardianSocket(path, async input => {
        const message = input as { kind: string; state?: { confirmedAt?: string } };
        if (message.kind === "VIEW") return s.engine.guardianView();
        if (message.state?.confirmedAt) protection = input;
        return s.engine.acceptGuardianUpdate(input);
      });
      child = spawn(process.execPath, ["apps/engine/dist/guardian-main.js"], {
        env: { ...process.env, TRADING_MODE: "paper", GUARDIAN_SOCKET_PATH: path }, stdio: ["ignore", "pipe", "pipe"] });
      let output = ""; child.stdout!.on("data", chunk => { output += chunk; });
      await waitFor(async () => (await s.journal.queryEvents({ types: ["PAPER_FILL"] })).some(e => e.payload.intent === "PROTECTIVE_DE_RISK"));
      expect(protection).toBeTruthy();
      expect((await guardianRpc<{ status: string; reason: string }>(path, protection)).reason).toBe("EPISODE_CHANGED");
      const fills = (await s.journal.queryEvents({ types: ["PAPER_FILL"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK");
      const orders = (await s.journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).filter(e => e.payload.intent === "PROTECTIVE_DE_RISK");
      expect(fills).toHaveLength(1); expect(orders).toHaveLength(1); expect(child.pid).not.toBe(process.pid);
      console.log(JSON.stringify({ evidence: "CONTROLLED_PAPER_SOCKET_PROTECTION", enginePid: process.pid, guardianPid: child.pid,
        orderId: fills[0]!.payload.orderId, intent: fills[0]!.payload.intent, side: fills[0]!.payload.side,
        quantity: fills[0]!.payload.quantity, fillPrice: fills[0]!.payload.fillPrice,
        guardianReason: (fills[0]!.payload.deRisk as { guardianReason?: string })?.guardianReason,
        orderCount: orders.length, fillCount: fills.length, retry: "EPISODE_CHANGED", guardianLog: output.trim() }));
    } finally {
      child?.kill(); await server?.stop();
      if (child && child.exitCode === null) await new Promise<void>(resolve => child!.once("close", () => resolve()));
      await s.engine.stop();
    }
  }, 15000);
});
