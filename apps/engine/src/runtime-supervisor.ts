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
 * Cross-lane same-key ordering (blocker 3):
 * - At most one task per key is in-flight (P0 and P3 share the key lock).
 * - Same-lane same-key work stays FIFO.
 * - Per key, P0 and P3 wait on separate bounded queues. A later P0 is picked
 *   before not-yet-started P3 on that key. Queued P3 has not mutated state, so
 *   running a later P0 first is equivalent to P0 arriving before those P3 items
 *   were admitted. Already-started same-key P3 always finishes first.
 * - P3 is ordinary research (TRIGGER_EVENT / activation bookkeeping). This PR
 *   does not change TriggerEngine cooldown/dedupe.
 * - Unrelated keys are not serialized. Different-market P0 uses reserved P0
 *   workers during a P3 storm.
 *
 * Direct journal admission (blocker 4 + active reserve):
 * - Shared active concurrency remains 4, below postgres.js pool max=10.
 * - Noncritical active max is 3, so one execution slot is reserved for
 *   critical persistence. Critical may use all 4. Already-running DB work
 *   is never preempted.
 * - Critical and noncritical have separate bounded waiting queues.
 * - Free active slots prefer critical waiters; noncritical backlog cannot
 *   consume the critical waiting cap or the reserved active slot.
 *
 * Shutdown (blocker 6): close NEW runtime-work admission first; keep journal
 * admission OPEN until RuntimeSupervisor has drained or the join deadline is
 * reached; then close NEW journal admission and drain journal work. Timeout
 * does not claim a clean drain or checkpoint.
 *
 * Overload: transient pressure (queue high-water / hard-cap) may recover via
 * hysteresis. Critical persistence failure is a sticky integrity fault and
 * cannot auto-clear when queues drain.
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
export type ExperimentWorkStatus = "IDLE" | "RUNNING" | "ENTRY_CLOSED" | "COMPLETED" | "FAILED" | "ABORTED_START" | string;

export const RUNTIME_BOUNDS = {
  trigger: {
    p0: { concurrency: 4, queueLimit: 256, highWater: 192 },
    p3: { concurrency: 4, queueLimit: 1024, highWater: 768 },
  },
  /** Entire P0 lane is reserved critical capacity; P3 cannot consume these slots. */
  criticalReserved: { lane: "p0" as const, queueLimit: 256, concurrency: 4 },
  journal: {
    active: 4,
    /** Leaves ≥1 of the 4 active slots for critical persistence. */
    noncriticalActive: 3,
    waiting: 128,
    highWater: 96,
    criticalWaiting: 32,
    noncriticalWaiting: 96,
  },
  sse: { byteThreshold: 262_144, persistentBackpressure: 8 },
  shutdownJoinDeadlineMs: 5_000,
  heavyRead: { concurrency: 2, waiting: 16 },
  triggerByteEstimate: 2_048,
  /** Hard cap on retained RuntimeSupervisor failure records (not a metric label). */
  failureHistory: 32,
} as const;

/** Finite per-work market-epoch snapshot. Not durable and not public strategy metadata. */
export type MarketEpochFence = ReadonlyMap<string, number>;

export interface RuntimeWork {
  id: string;
  lane: WorkLane;
  key: string;
  estimatedBytes: number;
  critical: boolean;
  /** Economic deadline copied from TriggerEvent.expiresAt. Expired queued work is obsolete and must not consume a worker. */
  expiresAtMs?: number;
  run: (signal: AbortSignal) => Promise<void>;
  /** Trusted token epochs at admission; stale if the live market epoch advanced. */
  marketEpochFence?: MarketEpochFence | null;
  /** Explicit source-data provenance. Not inferred later from `source` strings. */
  provenance?: { kind: "MARKET" | "REFERENCE" | "NONE" } | null;
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
  failureCount: number;
  retainedFailures: number;
}

export interface JoinResult {
  drained: boolean;
  unresolved: number;
  timedOut: boolean;
}

export interface ObservedFailure {
  at: number;
  lane: WorkLane;
  key: string;
  message: string;
}

export interface RuntimeWorkMetrics {
  setQueueDepth(lane: WorkLane, value: number): void;
  setInFlight(lane: WorkLane, value: number): void;
  setQueueBytes(lane: WorkLane, value: number): void;
  setOldestAge(lane: WorkLane, seconds: number): void;
  incRejected(lane: WorkLane, reason: RejectionReason): void;
  incFailed(lane: WorkLane): void;
  incExpired(lane: WorkLane): void;
}

interface QueuedWork {
  work: RuntimeWork;
  enqueuedAt: number;
  abort: AbortController;
  resolve: () => void;
  reject: (error: unknown) => void;
}

interface KeyQueues {
  p0: QueuedWork[];
  p3: QueuedWork[];
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
  return status == null || status === "IDLE" || status === "COMPLETED" || status === "FAILED" || status === "ABORTED_START";
}

/** Persistence whose failure must not be treated as a successful mutation. */
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
    || type === "LEDGER_MARK_TO_MARKET"
    || type === "INFRASTRUCTURE_COST_ACCRUED"
    || type === "STATE_RECOVERED";
}

export class OverloadLatch {
  private current: OverloadState = "NORMAL";
  private integrityFault = false;

  get state(): OverloadState { return this.current; }
  get hasIntegrityFault(): boolean { return this.integrityFault; }

  /**
   * Sticky FAIL_CLOSED for critical persistence loss. Does not auto-clear when
   * queue depth falls. Only an explicit process recovery/reset clears it.
   */
  markIntegrityFault(): void {
    this.integrityFault = true;
    this.current = "FAIL_CLOSED";
  }

  recompute(input: {
    p0Queued: number;
    p3Queued: number;
    journalWaiting: number;
    p0HardCapHit: boolean;
    p3HardCapDuringRunning: boolean;
    reservedExhausted: boolean;
  }): OverloadState {
    if (this.integrityFault) {
      this.current = "FAIL_CLOSED";
      return this.current;
    }
    if (input.p0HardCapHit || input.reservedExhausted || input.p3HardCapDuringRunning) {
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

  force(state: OverloadState): void {
    if (this.integrityFault) {
      this.current = "FAIL_CLOSED";
      return;
    }
    this.current = state;
  }
}

/**
 * Bounded lanes + per-key serialization + global worker caps.
 * Unrelated keys run in parallel up to lane concurrency. Same key never
 * has two in-flight tasks (across P0 and P3).
 *
 * Failure observability is a cumulative counter plus a hard-capped ring.
 * No structure grows with process lifetime.
 */
export class RuntimeSupervisor {
  private closed = false;
  private readonly queuedByKey = new Map<string, KeyQueues>();
  private readonly busyKeys = new Set<string>();
  private readonly inflight = new Set<QueuedWork>();
  private readonly queuedCount: Record<WorkLane, number> = { p0: 0, p3: 0 };
  private readonly inFlightCount: Record<WorkLane, number> = { p0: 0, p3: 0 };
  private readonly queueBytes: Record<WorkLane, number> = { p0: 0, p3: 0 };
  private readonly oldestAt: Record<WorkLane, number | null> = { p0: null, p3: null };
  private readonly recentFailures: Array<ObservedFailure | undefined> = Array.from(
    { length: RUNTIME_BOUNDS.failureHistory },
  );
  private failureWrite = 0;
  private failureRetained = 0;
  private failureCount = 0;
  private lastP0HardCap = false;
  private lastP3HardCap = false;

  constructor(
    private readonly metrics?: RuntimeWorkMetrics,
    private readonly onChange?: () => void,
    private readonly nowMs: () => number = () => Date.now(),
  ) {}

  get admissionClosed(): boolean { return this.closed; }
  get lastHardCap(): { p0: boolean; p3: boolean } { return { p0: this.lastP0HardCap, p3: this.lastP3HardCap }; }
  get totalFailureCount(): number { return this.failureCount; }

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
      failureCount: this.failureCount,
      retainedFailures: this.failureRetained,
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
    // Obsolete trading decisions cannot occupy capacity ahead of a fresh trigger.
    if (this.pruneExpiredQueued(this.nowMs())) this.publish();
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
    const queues = this.queuedByKey.get(work.key) ?? { p0: [], p3: [] };
    queues[work.lane].push(item);
    this.queuedByKey.set(work.key, queues);
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

  /** Wait for in-flight/queued work without cancelling on timeout. */
  async waitUnresolved(deadlineMs: number): Promise<JoinResult> {
    const started = Date.now();
    while (this.unresolved() > 0) {
      if (Date.now() - started >= deadlineMs) {
        return { drained: false, unresolved: this.unresolved(), timedOut: true };
      }
      await delay(5);
    }
    return { drained: true, unresolved: 0, timedOut: false };
  }

  observedFailures(): ObservedFailure[] {
    const cap = RUNTIME_BOUNDS.failureHistory;
    const out: ObservedFailure[] = [];
    const start = this.failureCount > cap ? this.failureWrite % cap : 0;
    for (let i = 0; i < this.failureRetained; i += 1) {
      const item = this.recentFailures[(start + i) % cap];
      if (item) out.push({ ...item });
    }
    return out;
  }

  private pruneExpiredQueued(nowMs: number): boolean {
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
          if (lane === "p0") p0Changed = true;
          else p3Changed = true;
        }
      }
      if (queues.p0.length === 0 && queues.p3.length === 0) this.queuedByKey.delete(key);
    }
    if (p0Changed) this.refreshOldest("p0");
    if (p3Changed) this.refreshOldest("p3");
    return p0Changed || p3Changed;
  }

  private unresolved(): number {
    return this.queuedCount.p0 + this.queuedCount.p3 + this.inFlightCount.p0 + this.inFlightCount.p3;
  }

  private cancelRemaining(): void {
    for (const item of this.inflight) {
      item.abort.abort("shutdown_deadline");
    }
    for (const queues of this.queuedByKey.values()) {
      for (const item of [...queues.p0, ...queues.p3]) {
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
    if (this.pruneExpiredQueued(this.nowMs())) this.publish();
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

    let p3Fallback: { key: string; lane: "p3" } | null = null;
    for (const [key, queues] of this.queuedByKey) {
      if (this.busyKeys.has(key)) continue;
      if (preferP0 && queues.p0.length > 0) return this.dequeue(key, "p0");
      if (preferP3 && queues.p3.length > 0 && !p3Fallback) p3Fallback = { key, lane: "p3" };
    }
    if (p3Fallback) return this.dequeue(p3Fallback.key, p3Fallback.lane);
    return null;
  }

  private dequeue(key: string, lane: WorkLane): QueuedWork | null {
    const queues = this.queuedByKey.get(key);
    if (!queues) return null;
    const item = queues[lane].shift();
    if (!item) return null;
    this.queuedCount[lane] -= 1;
    this.queueBytes[lane] -= item.work.estimatedBytes;
    if (queues.p0.length === 0 && queues.p3.length === 0) this.queuedByKey.delete(key);
    this.refreshOldest(lane);
    return item;
  }

  private refreshOldest(lane: WorkLane): void {
    let oldest: number | null = null;
    for (const queues of this.queuedByKey.values()) {
      for (const item of queues[lane]) {
        if (oldest == null || item.enqueuedAt < oldest) oldest = item.enqueuedAt;
      }
    }
    this.oldestAt[lane] = oldest;
  }

  private noteFailure(item: QueuedWork, error: unknown): void {
    this.failureCount += 1;
    this.metrics?.incFailed(item.work.lane);
    const raw = error instanceof Error ? error.message : String(error);
    const record: ObservedFailure = {
      at: Date.now(),
      lane: item.work.lane,
      key: item.work.key,
      message: raw.slice(0, 200),
    };
    const cap = RUNTIME_BOUNDS.failureHistory;
    this.recentFailures[this.failureWrite % cap] = record;
    this.failureWrite += 1;
    this.failureRetained = Math.min(cap, this.failureRetained + 1);
  }

  private async runItem(item: QueuedWork): Promise<void> {
    this.busyKeys.add(item.work.key);
    this.inflight.add(item);
    this.inFlightCount[item.work.lane] += 1;
    this.publish();
    try {
      await item.work.run(item.abort.signal);
    } catch (error) {
      this.noteFailure(item, error);
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
  setActiveCritical(value: number): void;
  setActiveNoncritical(value: number): void;
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
  reportInput: boolean;
  enqueuedAt: number;
  run: () => Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
}

/**
 * Application-level direct-journal gate. Waiters here are JS promises, not
 * postgres.js pool waiters. No recursive retry. Already-running work is never
 * preempted.
 *
 * Shared active workers (4) + noncritical active cap (3) so one slot stays
 * available for critical persistence. Critical may use all 4. Separate bounded
 * waiting queues (32 / 96). Pump prefers critical waiters. Total waiting ≤ 128;
 * total active ≤ 4; noncritical active ≤ 3.
 */
export class DirectJournalAdmission {
  private closed = false;
  private active = 0;
  private activeCritical = 0;
  private activeNoncritical = 0;
  private reportInputActive = 0;
  private reportInputWaiting = 0;
  private readonly criticalWaiting: Array<JournalWaiter<unknown>> = [];
  private readonly noncriticalWaiting: Array<JournalWaiter<unknown>> = [];
  private criticalRejects = 0;
  private oldestAt: number | null = null;

  constructor(private readonly metrics?: JournalAdmissionMetrics, private readonly onChange?: () => void) {}

  get waitingCount(): number { return this.criticalWaiting.length + this.noncriticalWaiting.length; }
  get criticalWaitingCount(): number { return this.criticalWaiting.length; }
  get noncriticalWaitingCount(): number { return this.noncriticalWaiting.length; }
  get activeCount(): number { return this.active; }
  get activeCriticalCount(): number { return this.activeCritical; }
  get activeNoncriticalCount(): number { return this.activeNoncritical; }
  get admissionClosed(): boolean { return this.closed; }
  get criticalRejectionCount(): number { return this.criticalRejects; }
  get reportInputUnresolved(): number { return this.reportInputActive + this.reportInputWaiting; }

  closeAdmission(): void { this.closed = true; }

  async run<T>(journalClass: string, critical: boolean, work: () => Promise<T>, signal?: AbortSignal, flags?: { reportInput?: boolean }): Promise<T> {
    const reportInput = Boolean(flags?.reportInput);
    if (this.closed || signal?.aborted) {
      this.rejectNow(journalClass, critical, "JOURNAL_ADMISSION_CLOSED");
    }
    if (this.canStartNow(critical)) {
      return this.execute(work, critical, reportInput);
    }
    const queue = critical ? this.criticalWaiting : this.noncriticalWaiting;
    const limit = critical ? RUNTIME_BOUNDS.journal.criticalWaiting : RUNTIME_BOUNDS.journal.noncriticalWaiting;
    if (queue.length >= limit) {
      this.rejectNow(journalClass, critical);
    }
    return new Promise<T>((resolve, reject) => {
      const waiter: JournalWaiter<T> = {
        journalClass, critical, reportInput, enqueuedAt: Date.now(), run: work, resolve, reject,
      };
      queue.push(waiter as JournalWaiter<unknown>);
      if (reportInput) this.reportInputWaiting += 1;
      if (this.oldestAt == null) this.oldestAt = waiter.enqueuedAt;
      this.publish();
      this.pump();
    });
  }

  async join(deadlineMs: number): Promise<JoinResult> {
    const started = Date.now();
    while (this.active + this.waitingCount > 0) {
      if (Date.now() - started >= deadlineMs) {
        this.rejectWaiting("JOURNAL_ADMISSION_CLOSED");
        return { drained: this.active === 0, unresolved: this.active + this.waitingCount, timedOut: true };
      }
      await delay(5);
    }
    return { drained: true, unresolved: 0, timedOut: false };
  }

  /** Wait only for report-input journal writes. Telemetry may continue. Does not reject on timeout. */
  async waitReportInput(deadlineMs: number): Promise<JoinResult> {
    const started = Date.now();
    while (this.reportInputUnresolved > 0) {
      if (Date.now() - started >= deadlineMs) {
        return { drained: false, unresolved: this.reportInputUnresolved, timedOut: true };
      }
      await delay(5);
    }
    return { drained: true, unresolved: 0, timedOut: false };
  }

  private hasCapacity(critical: boolean): boolean {
    if (this.active >= RUNTIME_BOUNDS.journal.active) return false;
    if (critical) return true;
    return this.activeNoncritical < RUNTIME_BOUNDS.journal.noncriticalActive;
  }

  private canStartNow(critical: boolean): boolean {
    if (!this.hasCapacity(critical)) return false;
    if (this.criticalWaiting.length > 0) return false;
    if (critical) return true;
    return this.noncriticalWaiting.length === 0;
  }

  private rejectNow(journalClass: string, critical: boolean, message?: string): never {
    this.metrics?.incRejected(journalClass);
    if (critical) this.criticalRejects += 1;
    this.onChange?.();
    throw new JournalAdmissionRejected(journalClass, critical, message);
  }

  private async execute<T>(work: () => Promise<T>, critical: boolean, reportInput = false): Promise<T> {
    this.active += 1;
    if (critical) this.activeCritical += 1;
    else this.activeNoncritical += 1;
    if (reportInput) this.reportInputActive += 1;
    this.publish();
    try {
      return await work();
    } finally {
      this.active -= 1;
      if (critical) this.activeCritical -= 1;
      else this.activeNoncritical -= 1;
      if (reportInput) this.reportInputActive -= 1;
      this.publish();
      this.pump();
    }
  }

  private pump(): void {
    for (;;) {
      if (this.closed) {
        this.rejectWaiting("JOURNAL_ADMISSION_CLOSED");
        return;
      }
      if (this.criticalWaiting.length > 0 && this.hasCapacity(true)) {
        const next = this.criticalWaiting.shift();
        if (!next) break;
        this.refreshOldest();
        if (next.reportInput) this.reportInputWaiting = Math.max(0, this.reportInputWaiting - 1);
        void this.execute(next.run, true, next.reportInput).then(next.resolve, next.reject);
        continue;
      }
      if (this.noncriticalWaiting.length > 0 && this.hasCapacity(false)) {
        const next = this.noncriticalWaiting.shift();
        if (!next) break;
        this.refreshOldest();
        if (next.reportInput) this.reportInputWaiting = Math.max(0, this.reportInputWaiting - 1);
        void this.execute(next.run, false, next.reportInput).then(next.resolve, next.reject);
        continue;
      }
      break;
    }
    this.refreshOldest();
    this.publish();
  }

  private rejectWaiting(message: string): void {
    const leftover = [...this.criticalWaiting.splice(0), ...this.noncriticalWaiting.splice(0)];
    this.oldestAt = null;
    for (const waiter of leftover) {
      if (waiter.reportInput) this.reportInputWaiting = Math.max(0, this.reportInputWaiting - 1);
      this.metrics?.incRejected(waiter.journalClass);
      if (waiter.critical) this.criticalRejects += 1;
      waiter.reject(new JournalAdmissionRejected(waiter.journalClass, waiter.critical, message));
    }
    this.publish();
  }

  private refreshOldest(): void {
    const criticalOldest = this.criticalWaiting[0]?.enqueuedAt;
    const noncriticalOldest = this.noncriticalWaiting[0]?.enqueuedAt;
    if (criticalOldest == null) this.oldestAt = noncriticalOldest ?? null;
    else if (noncriticalOldest == null) this.oldestAt = criticalOldest;
    else this.oldestAt = Math.min(criticalOldest, noncriticalOldest);
  }

  private publish(): void {
    this.metrics?.setWaiting(this.waitingCount);
    this.metrics?.setActive(this.active);
    this.metrics?.setActiveCritical(this.activeCritical);
    this.metrics?.setActiveNoncritical(this.activeNoncritical);
    this.metrics?.setOldestAge(this.oldestAt == null ? 0 : Math.max(0, (Date.now() - this.oldestAt) / 1000));
    this.onChange?.();
  }
}

export interface SingleFlightCoalescingState {
  running: boolean;
  dirty: boolean;
  retainedSharedCycles: number;
  retainedWaiters: number;
}

/**
 * One active execution + one dirty bit + one shared completion promise.
 * Overlapping callers do not allocate an invocation-sized waiter array inside
 * SingleFlight. They share the in-flight cycle promise (O(1) retained state).
 */
export class SingleFlight {
  private running = false;
  private generation = 0;
  private committedGeneration = 0;
  private cycle: Promise<void> | null = null;
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

  coalescingState(): SingleFlightCoalescingState {
    return {
      running: this.running,
      dirty: this.dirty,
      retainedSharedCycles: this.cycle == null ? 0 : 1,
      retainedWaiters: 0,
    };
  }

  async trigger(task: (generation: number) => Promise<void>): Promise<void> {
    if (this.cycle) {
      this.dirty = true;
      this.coalesced += 1;
      this.hooks?.onCoalesced?.();
      this.hooks?.onDirty?.(true);
      return this.cycle;
    }
    const cycle = this.execute(task);
    this.cycle = cycle;
    return cycle;
  }

  private async execute(task: (generation: number) => Promise<void>): Promise<void> {
    this.running = true;
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
        } catch (error) {
          if (!this.dirty) throw error;
        } finally {
          this.hooks?.onInFlight?.(0);
        }
        if (!this.dirty) break;
      }
    } finally {
      this.running = false;
      this.cycle = null;
      this.hooks?.onDirty?.(false);
    }
  }
}

/** Coalesce concurrent callers onto one in-flight promise; all waiters share the result. */
export class SharedResultFlight<T> {
  private inflight: Promise<T> | null = null;
  coalesced = 0;

  async run(work: () => Promise<T>): Promise<T> {
    if (this.inflight) {
      this.coalesced += 1;
      return this.inflight;
    }
    const cycle = work().finally(() => { this.inflight = null; });
    this.inflight = cycle;
    return cycle;
  }

  get inFlight(): boolean { return this.inflight != null; }
}

/** Coalesce by key so experiment A in flight cannot serve experiment B. */
export class KeyedResultFlight<T> {
  private readonly flights = new Map<string, SharedResultFlight<T>>();

  async run(key: string, work: () => Promise<T>): Promise<T> {
    let flight = this.flights.get(key);
    if (!flight) {
      flight = new SharedResultFlight<T>();
      this.flights.set(key, flight);
    }
    if (flight.inFlight) this.coalesced += 1;
    try {
      return await flight.run(work);
    } finally {
      if (!flight.inFlight) this.flights.delete(key);
    }
  }

  inFlight(key: string): boolean {
    return this.flights.get(key)?.inFlight ?? false;
  }

  clear(): void {
    this.flights.clear();
    this.coalesced = 0;
  }

  coalesced = 0;
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
