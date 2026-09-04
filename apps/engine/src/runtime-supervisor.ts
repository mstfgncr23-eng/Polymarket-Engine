/**
 * Engine-specific owner for async background work.
 *
 * Not a generic job framework. It exists to replace structurally unbounded
 * `void this.processTrigger(...)` / overlapping discovery+maintenance / unbounded
 * direct-journal PG waiters with explicit admission, finite queues, keyed
 * serialization, and a joinable shutdown.
 *
 * Caps are architectural (finite active+queued work), not a CPU claim.
 *
 * Journal active concurrency is 4, below postgres.js pool max=10. Remaining
 * connections stay available for telemetry (maxConcurrent=2), healthCheck,
 * checkpoint, MCP/report reads (semaphore 2), and recovery. Direct journal
 * admission happens in-process BEFORE a waiter is created on the PG pool.
 */

export const WORK_LANES = ["p0", "p3"] as const;
export type WorkLane = (typeof WORK_LANES)[number];

export const REJECTION_REASONS = [
  "queue_full",
  "admission_closed",
  "overload_shed",
  "fail_closed",
  "reserved_capacity",
] as const;
export type RejectionReason = (typeof REJECTION_REASONS)[number];

export type OverloadState = "NORMAL" | "DEGRADED" | "FAIL_CLOSED";
export type ExperimentWorkStatus = "IDLE" | "RUNNING" | "ENTRY_CLOSED" | "COMPLETED" | "FAILED" | string;

export const RUNTIME_BOUNDS = {
  trigger: {
    p0: { concurrency: 4, queueLimit: 256, highWater: 192 },
    p3: { concurrency: 4, queueLimit: 1024, highWater: 768 },
  },
  /** Entire P0 lane is reserved critical capacity; P3 cannot consume these slots. */
  criticalReserved: { lane: "p0" as const, queueLimit: 256, concurrency: 4 },
  journal: { active: 4, waiting: 128, highWater: 96 },
  sse: { byteThreshold: 262_144, persistentBackpressure: 8 },
  shutdownJoinDeadlineMs: 5_000,
  heavyRead: { concurrency: 2, waiting: 16 },
  triggerByteEstimate: 2_048,
} as const;

export interface RuntimeWork {
  id: string;
  lane: WorkLane;
  key: string;
  estimatedBytes: number;
  critical: boolean;
  run: (signal: AbortSignal) => Promise<void>;
}

export interface AdmitResult {
  admitted: boolean;
  reason?: RejectionReason;
  done: Promise<void>;
}

export interface LaneSnapshot {
  lane: WorkLane;
  queued: number;
  inFlight: number;
  queueBytes: number;
  oldestAgeSeconds: number;
  concurrency: number;
  queueLimit: number;
  highWater: number;
}

export interface SupervisorSnapshot {
  closed: boolean;
  lanes: Record<WorkLane, LaneSnapshot>;
  busyKeys: number;
  totalQueued: number;
  totalInFlight: number;
  unresolved: number;
}

export interface JoinResult {
  drained: boolean;
  unresolved: number;
  timedOut: boolean;
}

export interface RuntimeWorkMetrics {
  setQueueDepth(lane: WorkLane, value: number): void;
  setInFlight(lane: WorkLane, value: number): void;
  setQueueBytes(lane: WorkLane, value: number): void;
  setOldestAge(lane: WorkLane, seconds: number): void;
  incRejected(lane: WorkLane, reason: RejectionReason): void;
}

interface QueuedWork {
  work: RuntimeWork;
  enqueuedAt: number;
  abort: AbortController;
  resolve: () => void;
  reject: (error: unknown) => void;
}

const RISK_HEDGE_SETTLEMENT_KINDS = new Set([
  "FAILED_HEDGE", "DIRECTIONAL_EXPOSURE", "NEARING_EXPIRY", "SCHEDULED_REVIEW",
]);

export function isRiskHedgeSettlementKind(kind: string): boolean {
  return RISK_HEDGE_SETTLEMENT_KINDS.has(kind);
}

export function triggerWorkLane(kind: string): WorkLane {
  return isRiskHedgeSettlementKind(kind) ? "p0" : "p3";
}

export function triggerWorkKey(input: { marketId?: string | null; asset?: string | null }): string {
  if (input.marketId) return `market:${input.marketId}`;
  if (input.asset) return `asset:${input.asset}`;
  return "global";
}

export function isIdleLikeStatus(status: ExperimentWorkStatus | null | undefined): boolean {
  return status == null || status === "IDLE" || status === "COMPLETED" || status === "FAILED";
}

/** Persistence whose failure must not be treated as machine death. */
export function isCriticalPersistenceType(type: string): boolean {
  return type.startsWith("PAPER_")
    || type.startsWith("POSITION_")
    || type.startsWith("RISK_")
    || type.startsWith("EXPERIMENT_")
    || type === "PNL_SNAPSHOT"
    || type === "HEDGE_COMPLETE"
    || type === "FAILED_HEDGE"
    || type === "FIRST_LEG_FILLED"
    || type === "PAIR_REALIZED"
    || type === "MARKET_RESOLUTION_OBSERVED"
    || type === "STATE_RECOVERED";
}

export class OverloadLatch {
  private current: OverloadState = "NORMAL";

  get state(): OverloadState { return this.current; }

  recompute(input: {
    p0Queued: number;
    p3Queued: number;
    journalWaiting: number;
    journalAdmissionRejectedCritical: boolean;
    p0HardCapHit: boolean;
    p3HardCapDuringRunning: boolean;
    reservedExhausted: boolean;
  }): OverloadState {
    if (
      input.p0HardCapHit
      || input.reservedExhausted
      || input.journalAdmissionRejectedCritical
      || input.p3HardCapDuringRunning
    ) {
      this.current = "FAIL_CLOSED";
      return this.current;
    }
    const p0High = input.p0Queued >= RUNTIME_BOUNDS.trigger.p0.highWater;
    const p3High = input.p3Queued >= RUNTIME_BOUNDS.trigger.p3.highWater;
    const journalHigh = input.journalWaiting >= RUNTIME_BOUNDS.journal.highWater;
    if (p0High || p3High || journalHigh) {
      this.current = "DEGRADED";
      return this.current;
    }
    const p0Recover = input.p0Queued <= Math.floor(RUNTIME_BOUNDS.trigger.p0.highWater / 2);
    const p3Recover = input.p3Queued <= Math.floor(RUNTIME_BOUNDS.trigger.p3.highWater / 2);
    const journalRecover = input.journalWaiting <= Math.floor(RUNTIME_BOUNDS.journal.highWater / 2);
    if (this.current !== "NORMAL" && p0Recover && p3Recover && journalRecover) {
      this.current = "NORMAL";
    }
    return this.current;
  }

  force(state: OverloadState): void { this.current = state; }
}

/**
 * Bounded lanes + per-key FIFO serialization + global worker caps.
 * Unrelated keys run in parallel up to lane concurrency. Same key never
 * has two in-flight tasks (across P0 and P3), so admitted same-market
 * order is preserved.
 */
export class RuntimeSupervisor {
  private closed = false;
  private readonly queuedByKey = new Map<string, QueuedWork[]>();
  private readonly busyKeys = new Set<string>();
  private readonly inflight = new Set<QueuedWork>();
  private readonly queuedCount: Record<WorkLane, number> = { p0: 0, p3: 0 };
  private readonly inFlightCount: Record<WorkLane, number> = { p0: 0, p3: 0 };
  private readonly queueBytes: Record<WorkLane, number> = { p0: 0, p3: 0 };
  private readonly oldestAt: Record<WorkLane, number | null> = { p0: null, p3: null };
  private readonly failures: unknown[] = [];
  private lastP0HardCap = false;
  private lastP3HardCap = false;

  constructor(
    private readonly metrics?: RuntimeWorkMetrics,
    private readonly onChange?: () => void,
  ) {
    this.publish();
  }

  get admissionClosed(): boolean { return this.closed; }
  get lastHardCap(): { p0: boolean; p3: boolean } { return { p0: this.lastP0HardCap, p3: this.lastP3HardCap }; }

  snapshot(): SupervisorSnapshot {
    return {
      closed: this.closed,
      lanes: {
        p0: this.laneSnapshot("p0"),
        p3: this.laneSnapshot("p3"),
      },
      busyKeys: this.busyKeys.size,
      totalQueued: this.queuedCount.p0 + this.queuedCount.p3,
      totalInFlight: this.inFlightCount.p0 + this.inFlightCount.p3,
      unresolved: this.queuedCount.p0 + this.queuedCount.p3 + this.inFlightCount.p0 + this.inFlightCount.p3,
    };
  }

  criticalReservedAvailable(): boolean {
    return this.queuedCount.p0 < RUNTIME_BOUNDS.trigger.p0.queueLimit
      && this.inFlightCount.p0 < RUNTIME_BOUNDS.trigger.p0.concurrency;
  }

  admit(work: RuntimeWork): AdmitResult {
    if (this.closed) {
      this.publish();
      return { admitted: false, reason: "admission_closed", done: Promise.resolve() };
    }
    const limit = RUNTIME_BOUNDS.trigger[work.lane].queueLimit;
    if (this.queuedCount[work.lane] >= limit) {
      if (work.lane === "p0") this.lastP0HardCap = true;
      else this.lastP3HardCap = true;
      this.publish();
      return { admitted: false, reason: "queue_full", done: Promise.resolve() };
    }
    if (work.lane === "p0") this.lastP0HardCap = false;
    else this.lastP3HardCap = false;

    let resolve!: () => void;
    let reject!: (error: unknown) => void;
    const done = new Promise<void>((res, rej) => { resolve = res; reject = rej; });
    const item: QueuedWork = {
      work, enqueuedAt: Date.now(), abort: new AbortController(), resolve, reject,
    };
    const queue = this.queuedByKey.get(work.key) ?? [];
    queue.push(item);
    this.queuedByKey.set(work.key, queue);
    this.queuedCount[work.lane] += 1;
    this.queueBytes[work.lane] += work.estimatedBytes;
    if (this.oldestAt[work.lane] == null) this.oldestAt[work.lane] = item.enqueuedAt;
    this.publish();
    this.pump();
    return { admitted: true, done };
  }

  closeAdmission(): void {
    this.closed = true;
    this.publish();
  }

  async join(deadlineMs: number): Promise<JoinResult> {
    const started = Date.now();
    while (this.unresolved() > 0) {
      if (Date.now() - started >= deadlineMs) {
        this.cancelRemaining();
        this.publish();
        return { drained: false, unresolved: this.unresolved(), timedOut: true };
      }
      await delay(5);
    }
    this.publish();
    return { drained: true, unresolved: 0, timedOut: false };
  }

  observedFailures(): unknown[] { return [...this.failures]; }

  private unresolved(): number {
    return this.queuedCount.p0 + this.queuedCount.p3 + this.inFlightCount.p0 + this.inFlightCount.p3;
  }

  private cancelRemaining(): void {
    for (const item of this.inflight) {
      item.abort.abort("shutdown_deadline");
    }
    for (const [, queue] of this.queuedByKey) {
      for (const item of queue) {
        this.queuedCount[item.work.lane] -= 1;
        this.queueBytes[item.work.lane] -= item.work.estimatedBytes;
        item.resolve();
      }
    }
    this.queuedByKey.clear();
    this.oldestAt.p0 = null;
    this.oldestAt.p3 = null;
  }

  private pump(): void {
    for (;;) {
      const next = this.pickNext();
      if (!next) return;
      void this.runItem(next);
    }
  }

  private pickNext(): QueuedWork | null {
    const preferP0 = this.inFlightCount.p0 < RUNTIME_BOUNDS.trigger.p0.concurrency;
    const preferP3 = this.inFlightCount.p3 < RUNTIME_BOUNDS.trigger.p3.concurrency;
    if (!preferP0 && !preferP3) return null;

    let fallback: { key: string; item: QueuedWork } | null = null;
    for (const [key, queue] of this.queuedByKey) {
      if (this.busyKeys.has(key)) continue;
      const item = queue[0];
      if (!item) continue;
      const lane = item.work.lane;
      if (lane === "p0" && !preferP0) continue;
      if (lane === "p3" && !preferP3) continue;
      if (lane === "p0") {
        this.dequeueHead(key);
        return item;
      }
      fallback ??= { key, item };
    }
    if (fallback) {
      this.dequeueHead(fallback.key);
      return fallback.item;
    }
    return null;
  }

  private dequeueHead(key: string): void {
    const queue = this.queuedByKey.get(key);
    if (!queue) return;
    const item = queue.shift();
    if (!item) return;
    this.queuedCount[item.work.lane] -= 1;
    this.queueBytes[item.work.lane] -= item.work.estimatedBytes;
    if (queue.length === 0) this.queuedByKey.delete(key);
    this.refreshOldest(item.work.lane);
  }

  private refreshOldest(lane: WorkLane): void {
    let oldest: number | null = null;
    for (const queue of this.queuedByKey.values()) {
      for (const item of queue) {
        if (item.work.lane !== lane) continue;
        if (oldest == null || item.enqueuedAt < oldest) oldest = item.enqueuedAt;
      }
    }
    this.oldestAt[lane] = oldest;
  }

  private async runItem(item: QueuedWork): Promise<void> {
    this.busyKeys.add(item.work.key);
    this.inflight.add(item);
    this.inFlightCount[item.work.lane] += 1;
    this.publish();
    try {
      await item.work.run(item.abort.signal);
    } catch (error) {
      this.failures.push(error);
    } finally {
      this.inflight.delete(item);
      this.busyKeys.delete(item.work.key);
      this.inFlightCount[item.work.lane] -= 1;
      item.resolve();
      this.publish();
      this.pump();
    }
  }

  private laneSnapshot(lane: WorkLane): LaneSnapshot {
    const oldest = this.oldestAt[lane];
    return {
      lane,
      queued: this.queuedCount[lane],
      inFlight: this.inFlightCount[lane],
      queueBytes: this.queueBytes[lane],
      oldestAgeSeconds: oldest == null ? 0 : Math.max(0, (Date.now() - oldest) / 1000),
      concurrency: RUNTIME_BOUNDS.trigger[lane].concurrency,
      queueLimit: RUNTIME_BOUNDS.trigger[lane].queueLimit,
      highWater: RUNTIME_BOUNDS.trigger[lane].highWater,
    };
  }

  private publish(): void {
    for (const lane of WORK_LANES) {
      const snap = this.laneSnapshot(lane);
      this.metrics?.setQueueDepth(lane, snap.queued);
      this.metrics?.setInFlight(lane, snap.inFlight);
      this.metrics?.setQueueBytes(lane, snap.queueBytes);
      this.metrics?.setOldestAge(lane, snap.oldestAgeSeconds);
    }
    this.onChange?.();
  }
}

export interface JournalAdmissionMetrics {
  setWaiting(value: number): void;
  setActive(value: number): void;
  setOldestAge(seconds: number): void;
  incRejected(journalClass: string): void;
}

export class JournalAdmissionRejected extends Error {
  readonly journalClass: string;
  readonly critical: boolean;
  constructor(journalClass: string, critical: boolean, message = "JOURNAL_ADMISSION_REJECTED") {
    super(message);
    this.name = "JournalAdmissionRejected";
    this.journalClass = journalClass;
    this.critical = critical;
  }
}

interface JournalWaiter<T> {
  journalClass: string;
  critical: boolean;
  enqueuedAt: number;
  run: () => Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
}

/**
 * Application-level direct-journal gate. Waiters here are JS promises, not
 * postgres.js pool waiters. No recursive retry.
 */
export class DirectJournalAdmission {
  private closed = false;
  private active = 0;
  private readonly waiting: Array<JournalWaiter<unknown>> = [];
  private criticalRejects = 0;
  private oldestAt: number | null = null;

  constructor(private readonly metrics?: JournalAdmissionMetrics, private readonly onChange?: () => void) {
    this.publish();
  }

  get waitingCount(): number { return this.waiting.length; }
  get activeCount(): number { return this.active; }
  get admissionClosed(): boolean { return this.closed; }
  get criticalRejectionCount(): number { return this.criticalRejects; }

  closeAdmission(): void { this.closed = true; }

  async run<T>(journalClass: string, critical: boolean, work: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    if (this.closed || signal?.aborted) {
      this.metrics?.incRejected(journalClass);
      if (critical) this.criticalRejects += 1;
      this.onChange?.();
      throw new JournalAdmissionRejected(journalClass, critical, "JOURNAL_ADMISSION_CLOSED");
    }
    if (this.active < RUNTIME_BOUNDS.journal.active) {
      return this.execute(work);
    }
    if (this.waiting.length >= RUNTIME_BOUNDS.journal.waiting) {
      this.metrics?.incRejected(journalClass);
      if (critical) this.criticalRejects += 1;
      this.publish();
      throw new JournalAdmissionRejected(journalClass, critical);
    }
    return new Promise<T>((resolve, reject) => {
      const waiter: JournalWaiter<T> = {
        journalClass, critical, enqueuedAt: Date.now(), run: work, resolve, reject,
      };
      this.waiting.push(waiter as JournalWaiter<unknown>);
      if (this.oldestAt == null) this.oldestAt = waiter.enqueuedAt;
      this.publish();
    });
  }

  async join(deadlineMs: number): Promise<JoinResult> {
    const started = Date.now();
    while (this.active + this.waiting.length > 0) {
      if (Date.now() - started >= deadlineMs) {
        this.rejectWaiting("JOURNAL_ADMISSION_CLOSED");
        return { drained: this.active === 0, unresolved: this.active + this.waiting.length, timedOut: true };
      }
      await delay(5);
    }
    return { drained: true, unresolved: 0, timedOut: false };
  }

  private async execute<T>(work: () => Promise<T>): Promise<T> {
    this.active += 1;
    this.publish();
    try {
      return await work();
    } finally {
      this.active -= 1;
      this.publish();
      this.pump();
    }
  }

  private pump(): void {
    while (this.active < RUNTIME_BOUNDS.journal.active && this.waiting.length > 0) {
      if (this.closed) {
        this.rejectWaiting("JOURNAL_ADMISSION_CLOSED");
        return;
      }
      const next = this.waiting.shift();
      if (!next) return;
      this.refreshOldest();
      void this.execute(next.run).then(next.resolve, next.reject);
    }
    this.refreshOldest();
    this.publish();
  }

  private rejectWaiting(message: string): void {
    const leftover = this.waiting.splice(0);
    this.oldestAt = null;
    for (const waiter of leftover) {
      this.metrics?.incRejected(waiter.journalClass);
      if (waiter.critical) this.criticalRejects += 1;
      waiter.reject(new JournalAdmissionRejected(waiter.journalClass, waiter.critical, message));
    }
    this.publish();
  }

  private refreshOldest(): void {
    this.oldestAt = this.waiting[0]?.enqueuedAt ?? null;
  }

  private publish(): void {
    this.metrics?.setWaiting(this.waiting.length);
    this.metrics?.setActive(this.active);
    this.metrics?.setOldestAge(this.oldestAt == null ? 0 : Math.max(0, (Date.now() - this.oldestAt) / 1000));
    this.onChange?.();
  }
}

export class SingleFlight {
  private running = false;
  private generation = 0;
  private committedGeneration = 0;
  private waiters: Array<{ resolve: () => void; reject: (error: unknown) => void }> = [];
  dirty = false;
  coalesced = 0;
  attempts = 0;

  constructor(
    private readonly hooks?: {
      onCoalesced?(): void;
      onDirty?(dirty: boolean): void;
      onInFlight?(n: number): void;
      onAttempt?(): void;
    },
  ) {}

  get committed(): number { return this.committedGeneration; }
  get currentGeneration(): number { return this.generation; }
  get inFlight(): boolean { return this.running; }

  async trigger(task: (generation: number) => Promise<void>): Promise<void> {
    if (this.running) {
      this.dirty = true;
      this.coalesced += 1;
      this.hooks?.onCoalesced?.();
      this.hooks?.onDirty?.(true);
      return new Promise<void>((resolve, reject) => this.waiters.push({ resolve, reject }));
    }
    return this.lead(task);
  }

  private async lead(task: (generation: number) => Promise<void>): Promise<void> {
    this.running = true;
    let lastError: unknown;
    try {
      for (;;) {
        this.dirty = false;
        this.hooks?.onDirty?.(false);
        const generation = ++this.generation;
        this.attempts += 1;
        this.hooks?.onAttempt?.();
        this.hooks?.onInFlight?.(1);
        try {
          await task(generation);
          this.committedGeneration = generation;
          lastError = undefined;
        } catch (error) {
          lastError = error;
          if (!this.dirty) throw error;
        } finally {
          this.hooks?.onInFlight?.(0);
        }
        if (!this.dirty) break;
      }
    } finally {
      this.running = false;
      const waiters = this.waiters.splice(0);
      if (this.dirty) {
        const next = this.lead(task);
        for (const waiter of waiters) void next.then(waiter.resolve, waiter.reject);
      } else {
        this.hooks?.onDirty?.(false);
        for (const waiter of waiters) {
          if (lastError !== undefined) waiter.reject(lastError);
          else waiter.resolve();
        }
      }
    }
  }
}

export class FiniteSemaphore {
  private active = 0;
  private readonly waiters: Array<{ resolve: () => void; reject: (error: unknown) => void }> = [];

  constructor(
    readonly concurrency: number,
    readonly waitingLimit: number,
    private readonly busyError = "HEAVY_READ_BUSY",
  ) {}

  get activeCount(): number { return this.active; }
  get waitingCount(): number { return this.waiters.length; }

  async run<T>(work: () => Promise<T>): Promise<T> {
    if (this.active >= this.concurrency) {
      if (this.waiters.length >= this.waitingLimit) throw new Error(this.busyError);
      await new Promise<void>((resolve, reject) => this.waiters.push({ resolve, reject }));
    }
    this.active += 1;
    try {
      return await work();
    } finally {
      this.active -= 1;
      this.waiters.shift()?.resolve();
    }
  }
}

export function remapTriggerRejection(
  reason: RejectionReason | undefined,
  input: { lane: WorkLane; experimentStatus: ExperimentWorkStatus | null | undefined },
): RejectionReason {
  if (reason !== "queue_full") return reason ?? "queue_full";
  if (input.lane === "p0") return "fail_closed";
  if (input.experimentStatus === "RUNNING") return "fail_closed";
  return "overload_shed";
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
