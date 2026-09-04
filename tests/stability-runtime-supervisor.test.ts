import { describe, expect, it } from "vitest";
import { PaperLedger, createId, type TriggerEvent } from "@pm/core";
import { MemoryEventJournal, createJournalEvent } from "@pm/db";
import { EngineService, RUNTIME_BOUNDS } from "../apps/engine/src/engine.js";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import {
  DirectJournalAdmission,
  FiniteSemaphore,
  JournalAdmissionRejected,
  OverloadLatch,
  RuntimeSupervisor,
  SingleFlight,
  remapTriggerRejection,
} from "../apps/engine/src/runtime-supervisor.js";
import { BoundedJournalWriter } from "../apps/engine/src/journal-writer.js";
import { SseHub, createApiServer } from "../apps/engine/src/server.js";
import { EngineStateStore } from "../apps/engine/src/state.js";
import { order, snapshot } from "./fixtures.js";
import {
  ScriptedMarketAdapter,
  ScriptedReferenceRegistry,
  discoveryReport,
  paperFill,
  qualified,
  qualifiedPaperEnv,
  startQualifiedEngine,
} from "./paper-engine-testkit.js";

async function metricValue(metrics: EngineMetrics, name: string, labels?: Record<string, string>): Promise<number> {
  const item = metrics.registry.getSingleMetric(name);
  if (!item) return 0;
  const data = await item.get();
  const points = data.values.filter((point) => {
    if (point.metricName?.endsWith("_bucket") || point.metricName?.endsWith("_sum")) return false;
    if (labels) return Object.entries(labels).every(([key, value]) => point.labels[key] === value);
    return true;
  });
  if (labels) return points[0]?.value ?? 0;
  return points.reduce((sum, point) => {
    if (point.metricName?.endsWith("_count")) return sum + point.value;
    if (!point.metricName || point.metricName === name) return sum + point.value;
    return sum;
  }, 0);
}

async function waitFor(predicate: () => boolean | Promise<boolean>, timeoutMs = 2_000): Promise<void> {
  const started = Date.now();
  while (!(await predicate())) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function researchTrigger(overrides: Partial<TriggerEvent> = {}): TriggerEvent {
  const traceId = createId("trace");
  return {
    id: createId("trigger"), traceId, marketId: "btc-mkt", asset: "BTC", source: "polymarket",
    kind: "BOOK_IMBALANCE", severity: 2, score: 1, dedupeKey: "x", evidence: {},
    sourceEventIds: ["e1"], evidenceIds: ["e1"],
    createdAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 30_000).toISOString(),
    ...overrides,
  };
}

function riskTrigger(overrides: Partial<TriggerEvent> = {}): TriggerEvent {
  return researchTrigger({ kind: "FAILED_HEDGE", severity: 0, ...overrides });
}

class DelayedDiscoveryAdapter extends ScriptedMarketAdapter {
  hold: Promise<void> = Promise.resolve();
  inFlight = 0;
  completed = 0;
  peak = 0;
  async discoverCryptoMarkets() {
    this.inFlight += 1;
    this.peak = Math.max(this.peak, this.inFlight);
    await this.hold;
    this.inFlight -= 1;
    this.completed += 1;
    return super.discoverCryptoMarkets();
  }
}

async function startNoopEngine(opts?: {
  holdAppend?: Promise<void>;
  appendHook?: (event: { type: string; payload: Record<string, unknown> }) => void | Promise<void>;
  shutdownJoinDeadlineMs?: number;
  onAppendStart?: () => void;
}) {
  const journal = new MemoryEventJournal();
  const original = journal.append.bind(journal);
  let appendActive = 0;
  let appendPeak = 0;
  let appendCalls = 0;
  journal.append = async (event) => {
    appendCalls += 1;
    appendActive += 1;
    appendPeak = Math.max(appendPeak, appendActive);
    opts?.onAppendStart?.();
    await opts?.appendHook?.(event);
    if (opts?.holdAppend && event.type === "TRIGGER_EVENT") await opts.holdAppend;
    try {
      return await original(event);
    } finally {
      appendActive -= 1;
    }
  };
  const market = new ScriptedMarketAdapter();
  market.queue(discoveryReport([qualified("btc-mkt", "BTC", "btc-up", "btc-down")]));
  const engine = new EngineService(qualifiedPaperEnv({ GROK_ACTIVATION_PROVIDER: "noop" }), journal, { error: () => undefined }, {
    market: market as never,
    referenceRegistry: new ScriptedReferenceRegistry() as never,
    shutdownJoinDeadlineMs: opts?.shutdownJoinDeadlineMs,
  });
  await engine.start();
  return { engine, journal, market, appendPeak: () => appendPeak, appendCalls: () => appendCalls, appendActive: () => appendActive };
}

describe("RuntimeSupervisor unit bounds", () => {
  it("never exceeds lane caps and serializes the same key", async () => {
    const running: string[] = [];
    const order: string[] = [];
    let peak = 0;
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const supervisor = new RuntimeSupervisor();
    const make = (id: string, lane: "p0" | "p3", key: string) => supervisor.admit({
      id, lane, key, estimatedBytes: 10, critical: lane === "p0",
      run: async () => {
        running.push(id);
        peak = Math.max(peak, running.length);
        order.push(id);
        await hold;
        running.splice(running.indexOf(id), 1);
      },
    });
    const a1 = make("a1", "p3", "market:m1");
    const a2 = make("a2", "p3", "market:m1");
    const b1 = make("b1", "p3", "market:m2");
    expect(a1.admitted && a2.admitted && b1.admitted).toBe(true);
    await waitFor(() => order.includes("a1") && order.includes("b1"));
    expect(order).not.toContain("a2");
    expect(supervisor.snapshot().lanes.p3.inFlight).toBeLessThanOrEqual(RUNTIME_BOUNDS.trigger.p3.concurrency);
    release();
    await Promise.all([a1.done, a2.done, b1.done]);
    expect(order.indexOf("a1")).toBeLessThan(order.indexOf("a2"));
    expect(peak).toBeLessThanOrEqual(RUNTIME_BOUNDS.trigger.p3.concurrency);
  });

  it("rejects at the hard cap without creating extra workers", async () => {
    const supervisor = new RuntimeSupervisor();
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    let admitted = 0;
    let rejected = 0;
    const dones: Array<Promise<void>> = [];
    for (let i = 0; i < 5_000; i += 1) {
      const result = supervisor.admit({
        id: `t${i}`, lane: "p3", key: `market:${i % 50}`, estimatedBytes: 8, critical: false,
        run: () => hold,
      });
      if (result.admitted) {
        admitted += 1;
        dones.push(result.done);
      } else rejected += 1;
    }
    const snap = supervisor.snapshot();
    expect(snap.lanes.p3.queued).toBeLessThanOrEqual(RUNTIME_BOUNDS.trigger.p3.queueLimit);
    expect(snap.lanes.p3.inFlight).toBeLessThanOrEqual(RUNTIME_BOUNDS.trigger.p3.concurrency);
    expect(admitted).toBe(snap.totalQueued + snap.totalInFlight);
    expect(rejected).toBe(5_000 - admitted);
    expect(supervisor.criticalReservedAvailable()).toBe(true);
    release();
    await Promise.all(dones);
  });

  it("DirectJournalAdmission caps active and waiting without recursive retry", async () => {
    const gate = new DirectJournalAdmission();
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    let started = 0;
    const work = () => { started += 1; return hold; };
    const running = Array.from({ length: 8 }, () => gate.run("trigger", false, work).catch(() => "rejected"));
    await waitFor(() => gate.activeCount === RUNTIME_BOUNDS.journal.active);
    expect(gate.waitingCount).toBeLessThanOrEqual(RUNTIME_BOUNDS.journal.waiting);
    expect(started).toBe(RUNTIME_BOUNDS.journal.active);
    const overflow: Array<Promise<"ok" | "rejected">> = [];
    for (let i = 0; i < 200; i += 1) {
      overflow.push(gate.run("trigger", false, work).then(() => "ok" as const, () => "rejected" as const));
    }
    await waitFor(() => gate.noncriticalWaitingCount === RUNTIME_BOUNDS.journal.noncriticalWaiting);
    expect(gate.activeCount).toBe(RUNTIME_BOUNDS.journal.active);
    release();
    const overflowResults = await Promise.all(overflow);
    expect(overflowResults.filter((item) => item === "rejected").length).toBeGreaterThan(0);
    await Promise.all(running);
  });

  it("SingleFlight collapses concurrent calls to one run plus at most one dirty rerun", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    let runs = 0;
    let peak = 0;
    let inFlight = 0;
    const flight = new SingleFlight();
    const task = async () => {
      inFlight += 1;
      peak = Math.max(peak, inFlight);
      runs += 1;
      await hold;
      inFlight -= 1;
    };
    const first = flight.trigger(task);
    await waitFor(() => flight.inFlight);
    const rest = Array.from({ length: 12 }, () => flight.trigger(task));
    expect(flight.dirty).toBe(true);
    expect(flight.coalesced).toBe(12);
    expect(peak).toBe(1);
    release();
    await Promise.all([first, ...rest]);
    expect(runs).toBe(2);
    expect(peak).toBe(1);
  });

  it("remapTriggerRejection fail-closes P0 and RUNNING P3 overflow", () => {
    expect(remapTriggerRejection("queue_full", { lane: "p0", experimentStatus: "IDLE" })).toBe("fail_closed");
    expect(remapTriggerRejection("queue_full", { lane: "p3", experimentStatus: "RUNNING" })).toBe("fail_closed");
    expect(remapTriggerRejection("queue_full", { lane: "p3", experimentStatus: "IDLE" })).toBe("overload_shed");
    expect(remapTriggerRejection("admission_closed", { lane: "p3", experimentStatus: "IDLE" })).toBe("admission_closed");
  });

  it("FiniteSemaphore rejects above waiting cap", async () => {
    const sem = new FiniteSemaphore(1, 2, "HEAVY_READ_BUSY");
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const first = sem.run(() => hold);
    const waiting = [sem.run(() => Promise.resolve("a")), sem.run(() => Promise.resolve("b"))];
    await expect(sem.run(() => Promise.resolve("c"))).rejects.toThrow("HEAVY_READ_BUSY");
    release();
    await first;
    await Promise.all(waiting);
  });

  it("overload latch uses hysteresis from transient FAIL_CLOSED back to NORMAL", () => {
    const latch = new OverloadLatch();
    expect(latch.recompute({
      p0Queued: 256, p3Queued: 0, journalWaiting: 0,
      p0HardCapHit: true, p3HardCapDuringRunning: false, reservedExhausted: true,
    })).toBe("FAIL_CLOSED");
    expect(latch.recompute({
      p0Queued: 200, p3Queued: 0, journalWaiting: 0,
      p0HardCapHit: false, p3HardCapDuringRunning: false, reservedExhausted: false,
    })).toBe("DEGRADED");
    expect(latch.recompute({
      p0Queued: 10, p3Queued: 10, journalWaiting: 0,
      p0HardCapHit: false, p3HardCapDuringRunning: false, reservedExhausted: false,
    })).toBe("NORMAL");
  });

  it("critical integrity fault stays FAIL_CLOSED after queues drain", () => {
    const latch = new OverloadLatch();
    latch.markIntegrityFault();
    expect(latch.state).toBe("FAIL_CLOSED");
    expect(latch.hasIntegrityFault).toBe(true);
    expect(latch.recompute({
      p0Queued: 0, p3Queued: 0, journalWaiting: 0,
      p0HardCapHit: false, p3HardCapDuringRunning: false, reservedExhausted: false,
    })).toBe("FAIL_CLOSED");
    latch.force("NORMAL");
    expect(latch.state).toBe("FAIL_CLOSED");
  });
});

describe("A blocked journal + 10k trigger storm", () => {
  it("keeps workers/queue/journal within caps and does not fan out 10k PG calls", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const { engine, journal, appendPeak, appendCalls } = await startNoopEngine({ holdAppend: hold });
    const started = performance.now();
    try {
      const cashBefore = engine.ledger.snapshot().cash;
      const storm = Array.from({ length: 10_000 }, (_, i) => researchTrigger({
        id: createId("trigger"), marketId: `mkt-${i % 40}`, dedupeKey: `d-${i}`,
      }));
      const result = engine.enqueueTriggerStormForTest(storm);
      const snap = engine.runtimeSupervisorSnapshot();
      const journalSnap = engine.journalAdmissionSnapshot();
      expect(snap.lanes.p3.inFlight).toBeLessThanOrEqual(RUNTIME_BOUNDS.trigger.p3.concurrency);
      expect(snap.lanes.p0.inFlight).toBeLessThanOrEqual(RUNTIME_BOUNDS.trigger.p0.concurrency);
      expect(snap.lanes.p3.queued).toBeLessThanOrEqual(RUNTIME_BOUNDS.trigger.p3.queueLimit);
      expect(snap.totalInFlight + snap.totalQueued).toBeLessThanOrEqual(
        RUNTIME_BOUNDS.trigger.p0.concurrency + RUNTIME_BOUNDS.trigger.p0.queueLimit
        + RUNTIME_BOUNDS.trigger.p3.concurrency + RUNTIME_BOUNDS.trigger.p3.queueLimit,
      );
      expect(journalSnap.active).toBeLessThanOrEqual(RUNTIME_BOUNDS.journal.active);
      expect(journalSnap.waiting).toBeLessThanOrEqual(RUNTIME_BOUNDS.journal.waiting);
      expect(appendPeak()).toBeLessThanOrEqual(RUNTIME_BOUNDS.journal.active);
      expect(appendCalls()).toBeLessThan(10_000);
      expect(result.admitted + result.rejected).toBe(10_000);
      expect(result.rejected).toBeGreaterThan(0);
      expect(engine.criticalReservedAvailable()).toBe(true);
      expect(await metricValue(engine.metrics, "process_trigger_in_flight")).toBeLessThanOrEqual(
        RUNTIME_BOUNDS.trigger.p0.concurrency + RUNTIME_BOUNDS.trigger.p3.concurrency,
      );
      expect(await metricValue(engine.metrics, "runtime_work_rejected_total", { lane: "p3", reason: "overload_shed" })).toBe(result.reasons.overload_shed ?? 0);
      expect(engine.ledger.snapshot().cash).toBe(cashBefore);
      expect((await journal.queryEvents({ types: ["PAPER_ORDER_CREATED", "PAPER_FILL"] })).length).toBe(0);
      console.log(JSON.stringify({
        test: "A",
        durationMs: Math.round(performance.now() - started),
        maxWorkers: snap.totalInFlight,
        maxQueue: snap.totalQueued,
        journalActive: journalSnap.active,
        journalWaiting: journalSnap.waiting,
        appendPeak: appendPeak(),
        admitted: result.admitted,
        rejected: result.rejected,
      }));
    } finally {
      release();
      await engine.stop();
    }
  }, 20_000);
});

describe("B queue drain after release", () => {
  it("returns depth/in-flight to 0, recovers overload, and does not double-execute", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const executed: string[] = [];
    const { engine } = await startNoopEngine({
      holdAppend: hold,
      appendHook: (event) => { if (event.type === "TRIGGER_EVENT") executed.push(event.type); },
    });
    try {
      const storm = Array.from({ length: 1_500 }, (_, i) => researchTrigger({ id: createId("trigger"), marketId: `mkt-${i % 10}` }));
      const result = engine.enqueueTriggerStormForTest(storm);
      expect(engine.runtimeSupervisorSnapshot().unresolved).toBeGreaterThan(0);
      release();
      await waitFor(() => engine.runtimeSupervisorSnapshot().unresolved === 0, 8_000);
      expect(await metricValue(engine.metrics, "process_trigger_in_flight")).toBe(0);
      expect(await metricValue(engine.metrics, "journal_direct_in_flight")).toBe(0);
      expect(engine.overloadState()).toBe("NORMAL");
      expect(executed.length).toBe(result.admitted);
    } finally {
      release();
      await engine.stop();
    }
  }, 20_000);
});

describe("C same-market ordering", () => {
  it("preserves same-market P3 FIFO and never runs two same-key tasks concurrently", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
      const seen: string[] = [];
    const { engine } = await startNoopEngine({
      holdAppend: hold,
      appendHook: (event) => {
        if (event.type === "TRIGGER_EVENT" && typeof event.payload.id === "string") seen.push(event.payload.id);
      },
    });
    try {
      const ids = ["t-a", "t-b", "t-c"];
      engine.enqueueTriggerStormForTest(ids.map((id) => researchTrigger({ id, marketId: "same-mkt", traceId: createId("trace") })));
      await waitFor(() => engine.runtimeSupervisorSnapshot().totalInFlight >= 1);
      expect(engine.runtimeSupervisorSnapshot().lanes.p3.inFlight).toBe(1);
      release();
      await waitFor(() => engine.runtimeSupervisorSnapshot().unresolved === 0, 5_000);
      await waitFor(() => seen.length === 3);
      expect(seen).toEqual(ids);
    } finally {
      release();
      await engine.stop();
    }
  }, 15_000);
});

describe("D IDLE+noop overload shedding", () => {
  it("rejects noncritical work without PAPER_* or cash/position changes and leaves P0 reserved", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const { engine, journal } = await startNoopEngine({ holdAppend: hold });
    try {
      const cash = engine.ledger.snapshot().cash;
      const storm = Array.from({ length: 5_000 }, () => researchTrigger({ id: createId("trigger"), marketId: `mkt-${Math.random()}` }));
      const result = engine.enqueueTriggerStormForTest(storm);
      expect(result.rejected).toBeGreaterThan(0);
      expect(result.reasons.overload_shed).toBeGreaterThan(0);
      expect(engine.criticalReservedAvailable()).toBe(true);
      const p0 = engine.enqueueTriggerStormForTest([riskTrigger({ id: createId("trigger"), marketId: "reserved-mkt" })]);
      expect(p0.admitted).toBe(1);
      expect(engine.ledger.snapshot().cash).toBe(cash);
      expect(engine.ledger.positions.size).toBe(0);
      expect(Number(engine.reservedCash())).toBe(0);
      expect((await journal.queryEvents({ types: ["PAPER_ORDER_CREATED", "PAPER_FILL", "PAPER_MAKER_RESERVED"] })).length).toBe(0);
      expect(await metricValue(engine.metrics, "runtime_work_rejected_total", { lane: "p3", reason: "overload_shed" })).toBeGreaterThan(0);
    } finally {
      release();
      await engine.stop();
    }
  }, 15_000);
});

describe("E RUNNING overload fail-closed", () => {
  it("blocks new paper entry when P0 capacity is exhausted and does not drop queued P0 work", async () => {
    const { engine, journal } = await startQualifiedEngine();
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const original = journal.append.bind(journal);
    journal.append = async (event) => {
      if (event.type === "TRIGGER_EVENT") await hold;
      return original(event);
    };
    try {
      expect((await engine.beginExperiment()).ok).toBe(true);
      const storm = Array.from({ length: 400 }, (_, i) => riskTrigger({ id: createId("trigger"), marketId: `risk-${i}` }));
      const result = engine.enqueueTriggerStormForTest(storm);
      const snap = engine.runtimeSupervisorSnapshot();
      expect(snap.lanes.p0.queued).toBeLessThanOrEqual(RUNTIME_BOUNDS.trigger.p0.queueLimit);
      expect(result.rejected).toBeGreaterThan(0);
      expect(engine.overloadState()).toBe("FAIL_CLOSED");
      expect(engine.state.newPaperEntryAllowed()).toBe(false);
      expect(engine.preflight("48H_GROK_PAPER_EXPERIMENT").blockers).toContain("OVERLOAD_FAIL_CLOSED");
      expect(snap.unresolved).toBe(result.admitted);
      expect((await journal.queryEvents({ types: ["PAPER_ORDER_CREATED"] })).length).toBe(0);
    } finally {
      release();
      await engine.stop();
    }
  });
});

describe("F discovery single-flight", () => {
  it("collapses 10+ concurrent reconciles to one active run and at most one dirty rerun", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const market = new DelayedDiscoveryAdapter();
    market.hold = hold;
    const first = qualified("btc-mkt", "BTC", "btc-up", "btc-down");
    const second = qualified("eth-mkt", "ETH", "eth-up", "eth-down");
    market.queue(discoveryReport([first]));
    market.queue(discoveryReport([second]));
    const engine = new EngineService(qualifiedPaperEnv({ GROK_ACTIVATION_PROVIDER: "noop" }), new MemoryEventJournal(), { error: () => undefined }, {
      market: market as never,
      referenceRegistry: new ScriptedReferenceRegistry() as never,
    });
    const starting = engine.start();
    await waitFor(() => market.inFlight === 1);
    const extras = Array.from({ length: 12 }, () => engine.reconcileDiscovery());
    await waitFor(() => engine.discoveryFlightSnapshot().coalesced >= 12);
    expect(market.peak).toBe(1);
    expect(engine.discoveryFlightSnapshot().dirty).toBe(true);
    expect(await metricValue(engine.metrics, "discovery_refresh_in_flight")).toBe(1);
    expect(await metricValue(engine.metrics, "discovery_refresh_overlap_total")).toBe(0);
    release();
    await Promise.all([starting, ...extras]);
    expect(market.completed).toBe(2);
    expect(engine.monitoredMarketIds()).toContain("eth-mkt");
    expect(engine.monitoredMarketIds()).not.toContain("btc-mkt");
    await engine.stop();
  });
});

describe("G maintenance single-flight", () => {
  it("collapses overlapping ticks to one active run and at most one dirty rerun", async () => {
    const { engine } = await startQualifiedEngine({ GROK_ACTIVATION_PROVIDER: "noop" });
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    try {
      engine.setMaintenanceHoldForTest(hold);
      const first = engine.tickMaintenance();
      await waitFor(() => engine.maintenanceFlightSnapshot().inFlight);
      const rest = Array.from({ length: 8 }, () => engine.tickMaintenance());
      await waitFor(() => engine.maintenanceFlightSnapshot().coalesced >= 8);
      expect(engine.maintenanceFlightSnapshot().dirty).toBe(true);
      engine.setMaintenanceHoldForTest(null);
      release();
      await Promise.all([first, ...rest]);
      expect(engine.maintenanceFlightSnapshot().attempts).toBe(2);
      expect(await metricValue(engine.metrics, "maintenance_singleflight_coalesced_total")).toBeGreaterThanOrEqual(8);
    } finally {
      engine.setMaintenanceHoldForTest(null);
      release();
      await engine.stop();
    }
  });
});

describe("H shutdown mid-work", () => {
  it("closes admission, joins with a finite deadline, and does not hang", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const { engine } = await startNoopEngine({ holdAppend: hold, shutdownJoinDeadlineMs: 80 });
    engine.enqueueTriggerStormForTest([researchTrigger({ id: createId("trigger") })]);
    await waitFor(() => engine.runtimeSupervisorSnapshot().totalInFlight >= 1);
    const started = Date.now();
    await engine.stop();
    expect(Date.now() - started).toBeLessThan(2_000);
    const shutdown = engine.lastShutdownResult();
    expect(shutdown.work?.timedOut).toBe(true);
    expect(shutdown.work?.drained).toBe(false);
    expect(shutdown.checkpointed).toBe(false);
    expect((shutdown.work?.unresolved ?? 0) + (shutdown.journal?.unresolved ?? 0)).toBeGreaterThan(0);
    expect(engine.runtimeSupervisorSnapshot().closed).toBe(true);
    const after = engine.enqueueTriggerStormForTest([researchTrigger({ id: createId("trigger") })]);
    expect(after.admitted).toBe(0);
    expect(after.reasons.admission_closed).toBe(1);
    release();
  }, 15_000);
});

describe("I SSE slow-client bound", () => {
  it("disconnects a non-draining client at the byte threshold and keeps the engine going", async () => {
    const metrics = new EngineMetrics();
    const hub = new SseHub(20, metrics);
    let ended = 0;
    const writes: string[] = [];
    hub.add({
      write: (chunk) => { writes.push(chunk); return false; },
      end: () => { ended += 1; },
      writableLength: RUNTIME_BOUNDS.sse.byteThreshold + 10,
    });
    const healthy: string[] = [];
    hub.add({ write: (chunk) => { healthy.push(chunk); return true; }, end: () => undefined });
    hub.publish("decision", { n: 1 });
    expect(ended).toBe(1);
    expect(await metricValue(metrics, "sse_slow_client_disconnects_total", { reason: "byte_threshold" })).toBe(1);
    expect(healthy).toHaveLength(1);
    hub.publish("decision", { n: 2 });
    expect(healthy).toHaveLength(2);

    const ledger = new PaperLedger(1_000);
    const state = new EngineStateStore(ledger.snapshot(), ledger.positions);
    state.database = "UP";
    const server = await createApiServer({
      state, journal: new MemoryEventJournal(), metrics, sse: hub,
      dashboardOrigin: "http://localhost:3000", logger: false,
      mcp: { bearerTokens: "", allowedOrigins: "https://grok.com", rateLimitPerMinute: 30 },
      submitGrokSignal: async () => ({ status: "ignored" }),
    });
    await server.listen({ host: "127.0.0.1", port: 0 });
    const address = server.server.address();
    const port = typeof address === "object" && address ? address.port : 0;
    const controller = new AbortController();
    try {
      const response = await fetch(`http://127.0.0.1:${port}/events`, { headers: { accept: "text/event-stream" }, signal: controller.signal });
      const reader = response.body!.getReader();
      const decoder = new TextDecoder();
      let text = "";
      while (!text.includes("\n\n")) {
        const { value, done } = await reader.read();
        if (done) break;
        text += decoder.decode(value, { stream: true });
      }
      expect(text).toMatch(/^event: snapshot\ndata: /);
      expect(text).toContain("\"paperMode\":true");
    } finally {
      controller.abort();
      await server.close();
    }
  });
});

describe("J readiness distinction", () => {
  it("does not treat telemetry failure as machine death, and fail-closed blocks new entry", async () => {
    const { engine, journal } = await startQualifiedEngine({ GROK_ACTIVATION_PROVIDER: "noop" });
    try {
      journal.appendMany = async () => { throw new Error("TELEMETRY_DOWN"); };
      const writer = new BoundedJournalWriter(journal, () => { throw new Error("must-not-mark-database-down"); }, { error: () => undefined }, {
        queueLimit: 2, batchSize: 1, sampleEvery: 1, maxConcurrent: 1,
        onTelemetryDegraded: (degraded) => { engine.state.telemetryDegraded = degraded; },
      }, engine.metrics);
      await writer.persist(createJournalEvent({
        traceId: createId("trace"), type: "MARKET_SNAPSHOT", aggregateType: "market", aggregateId: "m", payload: {},
      }));
      await writer.flush();
      expect(engine.state.telemetryDegraded).toBe(true);
      expect(engine.state.database).toBe("UP");
      expect(engine.state.engineAlive()).toBe(true);

      const original = journal.append.bind(journal);
      journal.append = async (event) => {
        if (event.type.startsWith("EXPERIMENT_") || event.type.startsWith("PAPER_")) throw new Error("CRITICAL_DOWN");
        return original(event);
      };
      await expect(engine.restMakerForTest(
        order("MAKER", { marketId: "btc-mkt", tokenId: "btc-up" }),
        snapshot({ marketId: "btc-mkt", tokenId: "btc-up" }),
      )).rejects.toThrow("CRITICAL_DOWN");
      expect(engine.state.database).toBe("DOWN");
      expect(engine.integrityFault()).toBe(true);
      expect(engine.overloadState()).toBe("FAIL_CLOSED");
      expect(engine.state.engineAlive()).toBe(false);
      const blocked = await engine.beginExperiment();
      expect(blocked.ok).toBe(false);
      expect(blocked.blockers).toEqual(expect.arrayContaining(["POSTGRES_UNHEALTHY"]));
    } finally {
      await engine.stop();
    }
  });

  it("critical persistence down makes /ready not_ready while /health stays cheap", async () => {
    const ledger = new PaperLedger(1000);
    const state = new EngineStateStore(ledger.snapshot(), ledger.positions);
    state.database = "DOWN";
    state.telemetryDegraded = true;
    expect(state.engineAlive()).toBe(false);
    expect(state.readiness()).toMatchObject({ status: "not_ready", database: "DOWN", telemetryDegraded: true, newPaperEntryAllowed: false });
    const server = await createApiServer({
      state, journal: new MemoryEventJournal(), metrics: new EngineMetrics(), sse: new SseHub(),
      dashboardOrigin: "http://localhost:3000", logger: false,
      mcp: { bearerTokens: "", allowedOrigins: "https://grok.com", rateLimitPerMinute: 30 },
      submitGrokSignal: async () => ({ status: "ignored" }),
    });
    try {
      const ready = await server.inject({ method: "GET", url: "/ready" });
      const health = await server.inject({ method: "GET", url: "/health" });
      expect(ready.statusCode).toBe(503);
      expect(health.statusCode).toBe(200);
      expect(health.json()).toMatchObject({ status: "ok", mode: "paper" });
    } finally {
      await server.close();
    }
  });
});

describe("normal-load trigger semantics remain journaled", () => {
  it("still writes TRIGGER_EVENT and IDLE activation decision under the supervisor", async () => {
    const { engine, journal } = await startQualifiedEngine({ GROK_ACTIVATION_PROVIDER: "noop" });
    try {
      await engine.registerTriggerForResearch(researchTrigger());
      const events = await journal.queryEvents({ types: ["TRIGGER_EVENT", "GROK_ACTIVATION_DECISION", "PAPER_ORDER_CREATED"] });
      expect(events.some((event) => event.type === "TRIGGER_EVENT")).toBe(true);
      expect(events.some((event) => event.type === "GROK_ACTIVATION_DECISION")).toBe(true);
      expect(events.some((event) => event.type === "PAPER_ORDER_CREATED")).toBe(false);
    } finally {
      await engine.stop();
    }
  });
});

describe("blocker 1 bounded failure history", () => {
  it("keeps retained failure history at a fixed cap after 100k task failures", async () => {
    const metrics = new EngineMetrics();
    const supervisor = new RuntimeSupervisor(metrics.runtimeWorkMetrics());
    const target = 100_000;
    let submitted = 0;
    while (submitted < target) {
      const dones: Array<Promise<void>> = [];
      while (submitted < target) {
        const result = supervisor.admit({
          id: `f${submitted}`, lane: "p3", key: `k${submitted % 8}`, estimatedBytes: 4, critical: false,
          run: async () => { throw new Error("task_failed"); },
        });
        if (!result.admitted) break;
        dones.push(result.done);
        submitted += 1;
      }
      await Promise.all(dones);
    }
    expect(supervisor.totalFailureCount).toBe(target);
    expect(supervisor.snapshot().retainedFailures).toBe(RUNTIME_BOUNDS.failureHistory);
    expect(supervisor.observedFailures()).toHaveLength(RUNTIME_BOUNDS.failureHistory);
    expect(supervisor.observedFailures().every((item) => item.message === "task_failed" && item.lane === "p3")).toBe(true);
    expect(await metricValue(metrics, "runtime_work_failures_total", { lane: "p3" })).toBe(target);
    const metric = metrics.registry.getSingleMetric("runtime_work_failures_total");
    const data = await metric!.get();
    for (const point of data.values) {
      expect(Object.keys(point.labels).every((key) => key === "lane")).toBe(true);
      expect(point.labels.lane === "p0" || point.labels.lane === "p3").toBe(true);
    }
  }, 20_000);
});

describe("blocker 2 SingleFlight O(1) coalescing", () => {
  it("keeps internal coalescing state O(1) under a 100k concurrent trigger storm", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    let runs = 0;
    let peak = 0;
    let inFlight = 0;
    const flight = new SingleFlight();
    const task = async () => {
      inFlight += 1;
      peak = Math.max(peak, inFlight);
      runs += 1;
      await hold;
      inFlight -= 1;
    };
    const first = flight.trigger(task);
    await waitFor(() => flight.inFlight);
    const rest = Array.from({ length: 100_000 }, () => flight.trigger(task));
    const state = flight.coalescingState();
    expect(state.retainedWaiters).toBe(0);
    expect(state.retainedSharedCycles).toBe(1);
    expect(state.dirty).toBe(true);
    expect(flight.coalesced).toBe(100_000);
    expect(peak).toBe(1);
    release();
    await Promise.all([first, ...rest]);
    expect(runs).toBeLessThanOrEqual(2);
    expect(peak).toBe(1);
    expect(flight.coalescingState()).toEqual({ running: false, dirty: false, retainedSharedCycles: 0, retainedWaiters: 0 });
  }, 20_000);
});

describe("blocker 3 same-key P0 vs P3", () => {
  it("A: same-market P0 is not forced to wait for an entire queued P3 backlog", async () => {
    const started: string[] = [];
    let releaseFirst!: () => void;
    const holdFirst = new Promise<void>((resolve) => { releaseFirst = resolve; });
    const supervisor = new RuntimeSupervisor();
    const make = (id: string, lane: "p0" | "p3") => supervisor.admit({
      id, lane, key: "market:same", estimatedBytes: 8, critical: lane === "p0",
      run: async () => {
        started.push(id);
        if (started.length === 1) await holdFirst;
      },
    });
    const p3 = Array.from({ length: 80 }, (_, i) => make(`p3-${i}`, "p3"));
    await waitFor(() => started[0] === "p3-0");
    expect(started).toEqual(["p3-0"]);
    const p0 = make("p0-hedge", "p0");
    expect(p0.admitted).toBe(true);
    releaseFirst();
    await p0.done;
    expect(started.indexOf("p0-hedge")).toBe(1);
    expect(started.indexOf("p0-hedge")).toBeLessThan(started.indexOf("p3-1"));
    await Promise.all(p3.map((item) => item.done));
  });

  it("B: same-key P0 and P3 never run concurrently", async () => {
    const supervisor = new RuntimeSupervisor();
    let inflight = 0;
    let peak = 0;
    let releaseP3!: () => void;
    const holdP3 = new Promise<void>((resolve) => { releaseP3 = resolve; });
    const p3 = supervisor.admit({
      id: "p3", lane: "p3", key: "market:k", estimatedBytes: 8, critical: false,
      run: async () => {
        inflight += 1;
        peak = Math.max(peak, inflight);
        await holdP3;
        inflight -= 1;
      },
    });
    await waitFor(() => inflight === 1);
    const p0 = supervisor.admit({
      id: "p0", lane: "p0", key: "market:k", estimatedBytes: 8, critical: true,
      run: async () => {
        inflight += 1;
        peak = Math.max(peak, inflight);
        inflight -= 1;
      },
    });
    await Promise.resolve();
    expect(peak).toBe(1);
    expect(inflight).toBe(1);
    expect(supervisor.snapshot().lanes.p0.inFlight).toBe(0);
    expect(supervisor.snapshot().lanes.p0.queued).toBe(1);
    releaseP3();
    await Promise.all([p3.done, p0.done]);
    expect(peak).toBe(1);
  });

  it("C: different-market P0 uses reserved capacity during a P3 storm", async () => {
    const supervisor = new RuntimeSupervisor();
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const p3 = Array.from({ length: 200 }, (_, i) => supervisor.admit({
      id: `p3-${i}`, lane: "p3", key: `market:p3-${i}`, estimatedBytes: 8, critical: false,
      run: () => hold,
    }));
    await waitFor(() => supervisor.snapshot().lanes.p3.inFlight === RUNTIME_BOUNDS.trigger.p3.concurrency);
    expect(supervisor.criticalReservedAvailable()).toBe(true);
    const p0 = supervisor.admit({
      id: "p0-other", lane: "p0", key: "market:p0-other", estimatedBytes: 8, critical: true,
      run: () => hold,
    });
    await waitFor(() => supervisor.snapshot().lanes.p0.inFlight === 1);
    expect(p0.admitted).toBe(true);
    expect(supervisor.snapshot().lanes.p3.inFlight).toBe(RUNTIME_BOUNDS.trigger.p3.concurrency);
    expect(supervisor.snapshot().lanes.p0.inFlight).toBe(1);
    release();
    await Promise.all([...p3.map((item) => item.done), p0.done]);
  });
});

describe("blocker 4-6 journal reservation, integrity, shutdown", () => {
  it("noncritical journal saturation still admits reserved critical work", async () => {
    const gate = new DirectJournalAdmission();
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const work = () => hold;
    const noncritical: Array<Promise<unknown>> = [];
    for (let i = 0; i < RUNTIME_BOUNDS.journal.active + RUNTIME_BOUNDS.journal.noncriticalWaiting + 10; i += 1) {
      noncritical.push(gate.run("trigger", false, work).catch((error) => error));
    }
    await waitFor(() => gate.noncriticalWaitingCount === RUNTIME_BOUNDS.journal.noncriticalWaiting);
    expect(gate.activeCount).toBe(RUNTIME_BOUNDS.journal.active);
    await expect(gate.run("trigger", false, work)).rejects.toBeInstanceOf(JournalAdmissionRejected);
    const critical = gate.run("execution", true, work);
    await waitFor(() => gate.criticalWaitingCount === 1);
    expect(gate.criticalRejectionCount).toBe(0);
    release();
    await critical;
    await Promise.all(noncritical);
  });

  it("engine noncritical backlog cannot reject a later critical fill", async () => {
    const { engine, journal } = await startNoopEngine();
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const original = journal.append.bind(journal);
    const pending: Array<Promise<unknown>> = [];
    try {
      journal.append = async (event) => {
        await hold;
        return original(event);
      };
      for (let i = 0; i < RUNTIME_BOUNDS.journal.active + RUNTIME_BOUNDS.journal.noncriticalWaiting; i += 1) {
        pending.push(engine.persistEventForTest(createJournalEvent({
          traceId: createId("trace"), type: "TRIGGER_EVENT", aggregateType: "trigger", aggregateId: createId("trigger"), payload: {},
        })).catch((error) => error));
      }
      await waitFor(() => engine.journalAdmissionSnapshot().noncriticalWaiting === RUNTIME_BOUNDS.journal.noncriticalWaiting);
      expect(engine.journalAdmissionSnapshot().active).toBe(RUNTIME_BOUNDS.journal.active);
      const critical = engine.persistEventForTest(createJournalEvent({
        traceId: createId("trace"), type: "PAPER_FILL", aggregateType: "paper_fill", aggregateId: createId("fill"), payload: {},
      }));
      await waitFor(() => engine.journalAdmissionSnapshot().criticalWaiting === 1);
      expect(engine.journalAdmissionSnapshot().criticalRejects).toBe(0);
      release();
      await critical;
      await Promise.all(pending);
      expect(engine.integrityFault()).toBe(false);
    } finally {
      release();
      await engine.stop();
    }
  }, 15_000);

  it("fill/risk/settlement critical admission rejection propagates and stays FAIL_CLOSED after drain", async () => {
    const { engine, journal } = await startNoopEngine();
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const original = journal.append.bind(journal);
    const pending: Array<Promise<unknown>> = [];
    try {
      journal.append = async (event) => {
        await hold;
        return original(event);
      };
      for (let i = 0; i < RUNTIME_BOUNDS.journal.active + RUNTIME_BOUNDS.journal.criticalWaiting; i += 1) {
        pending.push(engine.persistEventForTest(createJournalEvent({
          traceId: createId("trace"), type: "RISK_EVALUATION", aggregateType: "risk", aggregateId: createId("risk"), payload: {},
        })).catch((error) => error));
      }
      await waitFor(() => engine.journalAdmissionSnapshot().criticalWaiting === RUNTIME_BOUNDS.journal.criticalWaiting);
      await expect(engine.applyObservedFill(paperFill("btc-mkt", "UP"))).rejects.toBeInstanceOf(JournalAdmissionRejected);
      await expect(engine.persistEventForTest(createJournalEvent({
        traceId: createId("trace"), type: "RISK_EVALUATION", aggregateType: "risk", aggregateId: createId("risk"), payload: {},
      }))).rejects.toBeInstanceOf(JournalAdmissionRejected);
      await expect(engine.persistEventForTest(createJournalEvent({
        traceId: createId("trace"), type: "POSITION_SETTLED", aggregateType: "position", aggregateId: createId("pos"), payload: {},
      }))).rejects.toBeInstanceOf(JournalAdmissionRejected);
      expect(engine.integrityFault()).toBe(true);
      expect(engine.overloadState()).toBe("FAIL_CLOSED");
      expect(engine.state.newPaperEntryAllowed()).toBe(false);
      release();
      await Promise.all(pending);
      await waitFor(() => engine.journalAdmissionSnapshot().active === 0 && engine.journalAdmissionSnapshot().waiting === 0);
      expect(engine.integrityFault()).toBe(true);
      expect(engine.overloadState()).toBe("FAIL_CLOSED");
      expect(engine.state.newPaperEntryAllowed()).toBe(false);
      const blocked = await engine.beginExperiment();
      expect(blocked.ok).toBe(false);
      expect(blocked.blockers).toContain("OVERLOAD_FAIL_CLOSED");
    } finally {
      release();
      await engine.stop();
    }
  }, 15_000);

  it("ordinary telemetry/research degradation does not create a sticky integrity fault", async () => {
    const { engine, journal } = await startNoopEngine();
    const original = journal.append.bind(journal);
    try {
      journal.append = async (event) => {
        if (event.type === "TRIGGER_EVENT" || event.type === "MARKET_SNAPSHOT") throw new Error("RESEARCH_DOWN");
        return original(event);
      };
      await engine.persistEventForTest(createJournalEvent({
        traceId: createId("trace"), type: "TRIGGER_EVENT", aggregateType: "trigger", aggregateId: createId("trigger"), payload: {},
      }));
      expect(engine.integrityFault()).toBe(false);
      expect(engine.state.journalDegraded).toBe(true);
      expect(engine.overloadState()).not.toBe("FAIL_CLOSED");
      expect(engine.state.database).toBe("UP");
      expect(engine.state.engineAlive()).toBe(true);
    } finally {
      await engine.stop();
    }
  });

  it("admitted trigger can still journal after shutdown begins", async () => {
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const { engine, journal } = await startNoopEngine();
    try {
      engine.setProcessTriggerHoldForTest(hold);
      const trigger = researchTrigger({ id: "drain-t", marketId: "drain-mkt" });
      engine.enqueueTriggerStormForTest([trigger]);
      await waitFor(() => engine.runtimeSupervisorSnapshot().totalInFlight >= 1);
      expect(engine.journalAdmissionSnapshot().closed).toBe(false);
      const stopping = engine.stop();
      await waitFor(() => engine.state.draining);
      expect(engine.journalAdmissionSnapshot().closed).toBe(false);
      engine.setProcessTriggerHoldForTest(null);
      release();
      await stopping;
      const events = await journal.queryEvents({ types: ["TRIGGER_EVENT"] });
      expect(events.some((event) => event.payload.id === "drain-t")).toBe(true);
      const shutdown = engine.lastShutdownResult();
      expect(shutdown.work?.drained).toBe(true);
      expect(shutdown.journal?.drained).toBe(true);
      expect(shutdown.work?.timedOut).toBe(false);
      expect(engine.journalAdmissionSnapshot().closed).toBe(true);
    } finally {
      engine.setProcessTriggerHoldForTest(null);
      release();
    }
  }, 15_000);

  it("normal graceful shutdown drains runtime then journal then checkpoints", async () => {
    const { engine } = await startNoopEngine();
    await engine.registerTriggerForResearch(researchTrigger());
    await engine.stop();
    const shutdown = engine.lastShutdownResult();
    expect(shutdown.work?.drained).toBe(true);
    expect(shutdown.journal?.drained).toBe(true);
    expect(shutdown.work?.timedOut).toBe(false);
    expect(shutdown.journal?.timedOut).toBe(false);
    expect(shutdown.checkpointed).toBe(true);
    expect(engine.journalAdmissionSnapshot().closed).toBe(true);
    expect(engine.runtimeSupervisorSnapshot().closed).toBe(true);
  });
});
