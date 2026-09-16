import { describe, expect, it } from "vitest";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { RUNTIME_BOUNDS, RuntimeSupervisor } from "../apps/engine/src/runtime-supervisor.js";

async function waitFor(predicate: () => boolean, timeoutMs = 1_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
}

async function metricValue(metrics: EngineMetrics, lane: "p0" | "p3"): Promise<number> {
  const metric = metrics.registry.getSingleMetric("runtime_work_expired_total");
  if (!metric) return 0;
  const data = await metric.get();
  return data.values.find((point) => point.labels.lane === lane)?.value ?? 0;
}

describe("RuntimeSupervisor economic expiry", () => {
  it("drops queued expired P3 work without running it and resolves done", async () => {
    let nowMs = 1_000;
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const metrics = new EngineMetrics();
    const supervisor = new RuntimeSupervisor(metrics.runtimeWorkMetrics(), undefined, () => nowMs);
    const ran: string[] = [];

    const first = supervisor.admit({
      id: "first", lane: "p3", key: "market:m1", estimatedBytes: 10, critical: false,
      expiresAtMs: 10_000, run: async () => { ran.push("first"); await hold; },
    });
    const stale = supervisor.admit({
      id: "stale", lane: "p3", key: "market:m1", estimatedBytes: 10, critical: false,
      expiresAtMs: 2_000, run: async () => { ran.push("stale"); },
    });

    await waitFor(() => ran.includes("first"));
    expect(supervisor.snapshot().lanes.p3.queued).toBe(1);
    nowMs = 3_000;
    release();
    await Promise.all([first.done, stale.done]);

    expect(ran).toEqual(["first"]);
    expect(supervisor.snapshot().lanes.p3.queued).toBe(0);
    expect(await metricValue(metrics, "p3")).toBe(1);
  });

  it("drops an expired P0 head while preserving fresh P0-before-P3 ordering", async () => {
    let nowMs = 1_000;
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const metrics = new EngineMetrics();
    const supervisor = new RuntimeSupervisor(metrics.runtimeWorkMetrics(), undefined, () => nowMs);
    const ran: string[] = [];

    const blocker = supervisor.admit({
      id: "blocker", lane: "p3", key: "market:m1", estimatedBytes: 10, critical: false,
      expiresAtMs: 10_000, run: async () => { ran.push("blocker"); await hold; },
    });
    const staleP0 = supervisor.admit({
      id: "stale-p0", lane: "p0", key: "market:m1", estimatedBytes: 10, critical: true,
      expiresAtMs: 2_000, run: async () => { ran.push("stale-p0"); },
    });
    const freshP0 = supervisor.admit({
      id: "fresh-p0", lane: "p0", key: "market:m1", estimatedBytes: 10, critical: true,
      expiresAtMs: 8_000, run: async () => { ran.push("fresh-p0"); },
    });
    const laterP3 = supervisor.admit({
      id: "later-p3", lane: "p3", key: "market:m1", estimatedBytes: 10, critical: false,
      expiresAtMs: 8_000, run: async () => { ran.push("later-p3"); },
    });

    await waitFor(() => ran.includes("blocker"));
    nowMs = 3_000;
    release();
    await Promise.all([blocker.done, staleP0.done, freshP0.done, laterP3.done]);

    expect(ran).toEqual(["blocker", "fresh-p0", "later-p3"]);
    expect(await metricValue(metrics, "p0")).toBe(1);
    expect(supervisor.snapshot().totalQueued).toBe(0);
  });

  it("prunes an expired full P3 queue before hard-cap checking fresh work", async () => {
    let nowMs = 1_000;
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const metrics = new EngineMetrics();
    const supervisor = new RuntimeSupervisor(metrics.runtimeWorkMetrics(), undefined, () => nowMs);

    const blocker = supervisor.admit({
      id: "blocker", lane: "p3", key: "market:blocked", estimatedBytes: 1, critical: false,
      expiresAtMs: 10_000, run: () => hold,
    });
    const staleDones: Array<Promise<void>> = [];
    for (let i = 0; i < RUNTIME_BOUNDS.trigger.p3.queueLimit; i += 1) {
      const stale = supervisor.admit({
        id: `stale-${i}`, lane: "p3", key: "market:blocked", estimatedBytes: 1, critical: false,
        expiresAtMs: 2_000, run: async () => { throw new Error("stale work must not run"); },
      });
      expect(stale.admitted).toBe(true);
      staleDones.push(stale.done);
    }
    expect(supervisor.snapshot().lanes.p3.queued).toBe(RUNTIME_BOUNDS.trigger.p3.queueLimit);

    nowMs = 3_000;
    const fresh = supervisor.admit({
      id: "fresh", lane: "p3", key: "market:fresh", estimatedBytes: 1, critical: false,
      expiresAtMs: 9_000, run: async () => undefined,
    });

    expect(fresh.admitted).toBe(true);
    await Promise.all([...staleDones, fresh.done]);
    expect(await metricValue(metrics, "p3")).toBe(RUNTIME_BOUNDS.trigger.p3.queueLimit);
    release();
    await blocker.done;
    expect(supervisor.snapshot().totalQueued).toBe(0);
  });
});
