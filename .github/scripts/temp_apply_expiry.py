from pathlib import Path

# RuntimeSupervisor: carry TriggerEvent deadline and discard obsolete queued work.
path = Path("apps/engine/src/runtime-supervisor.ts")
text = path.read_text()
replacements = [
    (
        "  critical: boolean;\n  run: (signal: AbortSignal) => Promise<void>;",
        "  critical: boolean;\n  /** Economic deadline copied from TriggerEvent.expiresAt. Expired queued work is obsolete and must not consume a worker. */\n  expiresAtMs?: number;\n  run: (signal: AbortSignal) => Promise<void>;",
    ),
    (
        "  incRejected(lane: WorkLane, reason: RejectionReason): void;\n  incFailed(lane: WorkLane): void;",
        "  incRejected(lane: WorkLane, reason: RejectionReason): void;\n  incFailed(lane: WorkLane): void;\n  incExpired(lane: WorkLane): void;",
    ),
    (
        "  constructor(\n    private readonly metrics?: RuntimeWorkMetrics,\n    private readonly onChange?: () => void,\n  ) {}",
        "  constructor(\n    private readonly metrics?: RuntimeWorkMetrics,\n    private readonly onChange?: () => void,\n    private readonly nowMs: () => number = () => Date.now(),\n  ) {}",
    ),
    (
        "  admit(work: RuntimeWork): AdmitResult {\n    if (this.closed) {\n      this.publish();\n      return { admitted: false, reason: \"admission_closed\", done: Promise.resolve() };\n    }\n    const limit = RUNTIME_BOUNDS.trigger[work.lane].queueLimit;",
        "  admit(work: RuntimeWork): AdmitResult {\n    if (this.closed) {\n      this.publish();\n      return { admitted: false, reason: \"admission_closed\", done: Promise.resolve() };\n    }\n    // Obsolete trading decisions cannot occupy capacity ahead of a fresh trigger.\n    if (this.pruneExpiredQueued(this.nowMs())) this.publish();\n    const limit = RUNTIME_BOUNDS.trigger[work.lane].queueLimit;",
    ),
    (
        "  private pump(): void {\n    for (;;) {\n      const next = this.pickNext();",
        "  private pump(): void {\n    if (this.pruneExpiredQueued(this.nowMs())) this.publish();\n    for (;;) {\n      const next = this.pickNext();",
    ),
]
for old, new in replacements:
    assert old in text, old
    text = text.replace(old, new, 1)

marker = "  private unresolved(): number {\n    return this.queuedCount.p0 + this.queuedCount.p3 + this.inFlightCount.p0 + this.inFlightCount.p3;\n  }"
replacement = """  private pruneExpiredQueued(nowMs: number): boolean {
    let p0Changed = false;
    let p3Changed = false;
    for (const [key, queues] of this.queuedByKey) {
      for (const lane of WORK_LANES) {
        const queue = queues[lane];
        // Same key/lane is FIFO and trigger deadlines are created in admission
        // order. Work without a deadline keeps the legacy supervisor contract.
        while (queue.length > 0) {
          const item = queue[0]!;
          const expiresAtMs = item.work.expiresAtMs;
          if (expiresAtMs == null || !Number.isFinite(expiresAtMs) || expiresAtMs > nowMs) break;
          queue.shift();
          this.queuedCount[lane] -= 1;
          this.queueBytes[lane] -= item.work.estimatedBytes;
          this.metrics?.incExpired(lane);
          item.resolve();
          if (lane === \"p0\") p0Changed = true;
          else p3Changed = true;
        }
      }
      if (queues.p0.length === 0 && queues.p3.length === 0) this.queuedByKey.delete(key);
    }
    if (p0Changed) this.refreshOldest(\"p0\");
    if (p3Changed) this.refreshOldest(\"p3\");
    return p0Changed || p3Changed;
  }

  private unresolved(): number {
    return this.queuedCount.p0 + this.queuedCount.p3 + this.inFlightCount.p0 + this.inFlightCount.p3;
  }"""
assert marker in text
path.write_text(text.replace(marker, replacement, 1))

# Engine: pass exact TriggerEvent expiry and the injected Engine clock.
path = Path("apps/engine/src/engine.ts")
text = path.read_text()
old = "    this.supervisor = new RuntimeSupervisor(this.metrics.runtimeWorkMetrics(), () => this.syncRuntimeState());"
new = "    this.supervisor = new RuntimeSupervisor(this.metrics.runtimeWorkMetrics(), () => this.syncRuntimeState(), () => this.nowFn().getTime());"
assert old in text
text = text.replace(old, new, 1)
old = "      estimatedBytes: RUNTIME_BOUNDS.triggerByteEstimate,\n      critical: lane === \"p0\",\n      marketEpochFence,"
new = "      estimatedBytes: RUNTIME_BOUNDS.triggerByteEstimate,\n      critical: lane === \"p0\",\n      expiresAtMs: Date.parse(trigger.expiresAt),\n      marketEpochFence,"
assert old in text
path.write_text(text.replace(old, new, 1))

# Separate bounded metric for admitted work that became economically obsolete.
path = Path("apps/engine/src/metrics.ts")
text = path.read_text()
old = """  readonly runtimeWorkFailures = new Counter({
    name: \"runtime_work_failures_total\",
    help: \"RuntimeSupervisor task failures by lane (no error text in labels)\",
    labelNames: [\"lane\"],
    registers: [this.registry],
  });"""
new = old + """
  readonly runtimeWorkExpired = new Counter({
    name: \"runtime_work_expired_total\",
    help: \"Admitted trigger work discarded from the queue after its economic deadline\",
    labelNames: [\"lane\"],
    registers: [this.registry],
  });"""
assert old in text
text = text.replace(old, new, 1)
old = "      this.runtimeWorkFailures.inc({ lane }, 0);\n      for (const reason of REJECTION_REASONS) this.runtimeWorkRejected.inc({ lane, reason }, 0);"
new = "      this.runtimeWorkFailures.inc({ lane }, 0);\n      this.runtimeWorkExpired.inc({ lane }, 0);\n      for (const reason of REJECTION_REASONS) this.runtimeWorkRejected.inc({ lane, reason }, 0);"
assert old in text
text = text.replace(old, new, 1)
old = "      incRejected: (lane: WorkLane, reason: RejectionReason) => this.runtimeWorkRejected.inc({ lane, reason }),\n      incFailed: (lane: WorkLane) => this.runtimeWorkFailures.inc({ lane }),"
new = "      incRejected: (lane: WorkLane, reason: RejectionReason) => this.runtimeWorkRejected.inc({ lane, reason }),\n      incFailed: (lane: WorkLane) => this.runtimeWorkFailures.inc({ lane }),\n      incExpired: (lane: WorkLane) => this.runtimeWorkExpired.inc({ lane }),"
assert old in text
path.write_text(text.replace(old, new, 1))

# Focused regression tests.
Path("tests/runtime-expired-work.test.ts").write_text(r'''import { describe, expect, it } from "vitest";
import { EngineMetrics } from "../apps/engine/src/metrics.js";
import { RuntimeSupervisor } from "../apps/engine/src/runtime-supervisor.js";

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

  it("prunes stale queued capacity before admitting fresh work", async () => {
    let nowMs = 1_000;
    let release!: () => void;
    const hold = new Promise<void>((resolve) => { release = resolve; });
    const supervisor = new RuntimeSupervisor(undefined, undefined, () => nowMs);

    const blocker = supervisor.admit({
      id: "blocker", lane: "p3", key: "market:blocked", estimatedBytes: 1, critical: false,
      expiresAtMs: 10_000, run: () => hold,
    });
    const stale = supervisor.admit({
      id: "stale", lane: "p3", key: "market:blocked", estimatedBytes: 1, critical: false,
      expiresAtMs: 2_000, run: async () => { throw new Error("stale work must not run"); },
    });

    nowMs = 3_000;
    const fresh = supervisor.admit({
      id: "fresh", lane: "p3", key: "market:fresh", estimatedBytes: 1, critical: false,
      expiresAtMs: 9_000, run: async () => undefined,
    });

    expect(fresh.admitted).toBe(true);
    await Promise.all([fresh.done, stale.done]);
    release();
    await blocker.done;
    expect(supervisor.snapshot().totalQueued).toBe(0);
  });
});
''')
