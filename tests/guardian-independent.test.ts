import { describe, it, expect } from "vitest";
import { spawn } from "node:child_process";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { randomUUID } from "node:crypto";
import { MemoryEventJournal } from "@pm/db";
import { EngineService } from "../apps/engine/src/engine.js";
import { GuardianMonitor } from "../apps/engine/src/guardian-worker.js";
import { guardianRpc, startGuardianSocket, type GuardianView } from "../apps/engine/src/guardian-link.js";
import { NoopGrokActivationBridge } from "../apps/engine/src/activation/index.js";
import { ScriptedMarketAdapter, ScriptedReferenceRegistry, discoveryReport, qualified, qualifiedPaperEnv, paperFill } from "./paper-engine-testkit.js";
import { snapshot } from "./fixtures.js";

const START = Date.parse("2026-10-05T00:00:00Z");
async function setup(journal = new MemoryEventJournal(), clock = { ms: START }) {
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport(["first", "second"].map(id => qualified(id, "SOL", id + "-up", id + "-down", new Date(START + 3600000).toISOString()))));
  const engine = new EngineService(qualifiedPaperEnv({ DISCOVERY_INTERVAL_MS: "3600000", ESTIMATED_INFRA_COST_PER_HOUR_USD: "0",
    PAPER_EXECUTION_STYLE: "TAKER", PAPER_EXECUTION_LATENCY_MS: "0", NORMAL_HEDGE_MIN_LOCKED_PAIR_EDGE: "0.12",
    PROFIT_TRAILING_PAIR_SOAK_ENABLED: "true", REVERSAL_PROTECTION_ENABLED: "true", GUARDIAN_SEPARATE_ENABLED: "true" }),
    journal, { error: () => undefined }, { market: market as never, referenceRegistry: new ScriptedReferenceRegistry() as never,
      activation: new NoopGrokActivationBridge(), now: () => new Date(clock.ms), resolutionFetcher: async () => null });
  await engine.start(); await market.waitForHandlers();
  return { engine, journal, clock, market };
}
type Scenario = Awaited<ReturnType<typeof setup>>;
async function seed(s: Scenario, id: string) {
  await s.engine.applyObservedFill(paperFill(id, "UP", "0.80", { tokenId: id + "-up", quantity: "5", timestamp: new Date(s.clock.ms).toISOString() }));
}
function books(s: Scenario, id: string, ratio = .75) {
  const internals = s.engine as unknown as { snapshots: Map<string, object> };
  const pair = Object.fromEntries((["UP", "DOWN"] as const).map(outcome => {
    const mid = outcome === "UP" ? ratio * .8 : .71;
    const book = snapshot({ marketId: id, tokenId: id + "-" + outcome.toLowerCase(), outcome, asset: "SOL",
      bids: [{ price: String(mid - .005), quantity: outcome === "UP" ? "10000" : "30000" }],
      asks: [{ price: String(mid + .005), quantity: outcome === "UP" ? "30000" : "10000" }],
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
      expect(replies.every(r => ["PERSISTED", "REPLAY"].includes(r.status))).toBe(true);
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
});
