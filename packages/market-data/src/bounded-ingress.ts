import {
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
  private observer: IngressQueueObserver | null = null;

  constructor(
    readonly maxEvents: number = MARKET_INGRESS_BOUNDS.maxEvents,
    readonly maxBytes: number = MARKET_INGRESS_BOUNDS.maxBytes,
  ) {}

  get depth(): number { return this.items.length - this.head; }
  get queuedBytes(): number { return this.bytes; }

  setObserver(observer: IngressQueueObserver | null): void { this.observer = observer; }

  tryEnqueue(event: unknown): { ok: true } | { ok: false; reason: Extract<IngressGapReason, "count_limit" | "byte_limit"> } {
    const size = approximateEventBytes(event);
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
    this.publish();
    return { ok: true };
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
    this.publish();
    return item.event;
  }

  /** Drain queued events. Does not increment gapTotal — overflow already counted at tryEnqueue. */
  clear(): void {
    this.items.length = 0;
    this.head = 0;
    this.bytes = 0;
    this.publish();
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
    this.publish();
  }

  private publish(): void {
    this.observer?.onChange(this.snapshot());
  }
}
