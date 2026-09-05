import {
  INGRESS_OBSERVER_BOUNDS,
  MARKET_INGRESS_BOUNDS,
  type IngressGapReason,
  type IngressQueueSnapshot,
} from "./ingress-bounds.js";

export class IngressOverflowError extends Error {
  readonly reason: IngressGapReason;
  constructor(reason: Extract<IngressGapReason, "count_limit" | "byte_limit">) {
    super(`MARKET_INGRESS_OVERFLOW:${reason}`);
    this.name = "IngressOverflowError";
    this.reason = reason;
  }
}

interface QueuedIngress {
  event: unknown;
  bytes: number;
}

function approximateEventBytes(event: unknown): number {
  if (typeof event === "string") return event.length;
  if (event && typeof event === "object") {
    try { return JSON.stringify(event).length; } catch { return 256; }
  }
  return 16;
}

/**
 * Dual-bound (event-count + approximate bytes) FIFO. Overflow never drops
 * oldest/newest/sampled events to continue — the caller must GAP + resync.
 */
export interface IngressQueueObserver {
  onChange(snapshot: IngressQueueSnapshot): void;
}

export class BoundedIngressQueue {
  private readonly items: QueuedIngress[] = [];
  private head = 0;
  private bytes = 0;
  private highWaterEvents = 0;
  private highWaterBytes = 0;
  accepted = 0;
  processed = 0;
  gapTotal = 0;
  lastGapReason: IngressGapReason | null = null;
  observerPublishCount = 0;
  private observer: IngressQueueObserver | null = null;
  private mutationsSincePublish = 0;
  private lastPublishAt = 0;
  private flushTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    readonly maxEvents: number = MARKET_INGRESS_BOUNDS.maxEvents,
    readonly maxBytes: number = MARKET_INGRESS_BOUNDS.maxBytes,
  ) {}

  get depth(): number { return this.items.length - this.head; }
  get queuedBytes(): number { return this.bytes; }

  setObserver(observer: IngressQueueObserver | null): void { this.observer = observer; }

  tryEnqueue(event: unknown, precomputedBytes?: number): { ok: true } | { ok: false; reason: Extract<IngressGapReason, "count_limit" | "byte_limit"> } {
    return this.tryEnqueueWeighted(event, precomputedBytes ?? approximateEventBytes(event));
  }

  tryEnqueueWeighted(
    event: unknown,
    bytes: number,
  ): { ok: true } | { ok: false; reason: Extract<IngressGapReason, "count_limit" | "byte_limit"> } {
    const size = Number.isFinite(bytes) && bytes >= 0 ? bytes : 16;
    if (this.depth >= this.maxEvents) {
      this.noteGap("count_limit");
      return { ok: false, reason: "count_limit" };
    }
    if (this.bytes + size > this.maxBytes) {
      this.noteGap("byte_limit");
      return { ok: false, reason: "byte_limit" };
    }
    this.items.push({ event, bytes: size });
    this.bytes += size;
    this.accepted += 1;
    if (this.depth > this.highWaterEvents) this.highWaterEvents = this.depth;
    if (this.bytes > this.highWaterBytes) this.highWaterBytes = this.bytes;
    this.publish(false);
    return { ok: true };
  }

  canAdmit(count: number, bytes: number): { ok: true } | { ok: false; reason: Extract<IngressGapReason, "count_limit" | "byte_limit"> } {
    if (this.depth + count > this.maxEvents) return { ok: false, reason: "count_limit" };
    if (this.bytes + bytes > this.maxBytes) return { ok: false, reason: "byte_limit" };
    return { ok: true };
  }

  /** Count one logical overflow without enqueueing. Idempotent callers must invoke this once. */
  noteOverflow(reason: Extract<IngressGapReason, "count_limit" | "byte_limit">): void {
    this.noteGap(reason);
  }

  dequeue(): unknown | undefined {
    if (this.head >= this.items.length) return undefined;
    const item = this.items[this.head]!;
    this.items[this.head] = undefined as unknown as QueuedIngress;
    this.head += 1;
    this.bytes -= item.bytes;
    this.processed += 1;
    if (this.head >= 1_024) {
      this.items.splice(0, this.head);
      this.head = 0;
    }
    this.publish(false);
    return item.event;
  }

  /** Drain queued events. Does not increment gapTotal — overflow already counted at tryEnqueue. */
  clear(): void {
    this.items.length = 0;
    this.head = 0;
    this.bytes = 0;
    this.publish(false);
  }

  flushObserver(): void {
    this.publish(true);
  }

  snapshot(): IngressQueueSnapshot {
    return {
      depth: this.depth,
      bytes: this.bytes,
      highWaterEvents: this.highWaterEvents,
      highWaterBytes: this.highWaterBytes,
      accepted: this.accepted,
      processed: this.processed,
      gapTotal: this.gapTotal,
    };
  }

  private noteGap(reason: IngressGapReason): void {
    this.gapTotal += 1;
    this.lastGapReason = reason;
    this.publish(true);
  }

  private publish(force: boolean): void {
    this.mutationsSincePublish += 1;
    const now = Date.now();
    if (
      force
      || this.mutationsSincePublish >= INGRESS_OBSERVER_BOUNDS.coalesceEveryMutations
      || now - this.lastPublishAt >= INGRESS_OBSERVER_BOUNDS.coalesceMinIntervalMs
    ) {
      this.emitNow();
      return;
    }
    this.scheduleFlush();
  }

  /** At most one unref'd timer per queue. Additional mutations reuse it. */
  private scheduleFlush(): void {
    if (this.flushTimer) return;
    const elapsed = Date.now() - this.lastPublishAt;
    const waitMs = Math.max(0, INGRESS_OBSERVER_BOUNDS.coalesceMinIntervalMs - elapsed);
    this.flushTimer = setTimeout(() => {
      this.flushTimer = null;
      this.emitNow();
    }, waitMs);
    this.flushTimer.unref?.();
  }

  private clearFlushTimer(): void {
    if (!this.flushTimer) return;
    clearTimeout(this.flushTimer);
    this.flushTimer = null;
  }

  private emitNow(): void {
    this.clearFlushTimer();
    this.mutationsSincePublish = 0;
    this.lastPublishAt = Date.now();
    this.observerPublishCount += 1;
    this.observer?.onChange(this.snapshot());
  }
}
