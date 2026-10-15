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

/** Redistributes the existing aggregate budget across owned socket FIFOs.
 * Idle-shard capacity may absorb a hot shard's burst; every other shard keeps
 * a reserved admission allowance. Total event/byte limits never increase. */
export class SharedIngressBudget {
  private readonly queues = new Set<BoundedIngressQueue>();
  private events = 0;
  private bytes = 0;
  private highWaterEvents = 0;
  private highWaterBytes = 0;
  constructor(readonly maxQueues: number, readonly maxEvents: number, readonly maxBytes: number,
    readonly reserveEvents = 1024, readonly reserveBytes = 512 * 1024) {
    if (![maxQueues,maxEvents,maxBytes,reserveEvents,reserveBytes].every(x=>Number.isInteger(x)&&x>0)
      || reserveEvents*maxQueues>maxEvents || reserveBytes*maxQueues>maxBytes) throw Error("INVALID_SHARED_INGRESS_BUDGET");
  }
  register(queue: BoundedIngressQueue): void {
    if (this.queues.has(queue) || this.queues.size >= this.maxQueues || queue.depth || queue.accepted)
      throw Error("SHARED_INGRESS_REGISTRATION_INVALID");
    this.queues.add(queue);
  }
  canAdmit(queue: BoundedIngressQueue, count: number, bytes: number): {ok:true}|{ok:false;reason:"count_limit"|"byte_limit"} {
    let reservedEvents=(this.maxQueues-this.queues.size)*this.reserveEvents;
    let reservedBytes=(this.maxQueues-this.queues.size)*this.reserveBytes;
    for (const other of this.queues) if (other!==queue) {
      reservedEvents+=Math.max(0,Math.min(this.reserveEvents,other.maxEvents)-other.depth);
      reservedBytes+=Math.max(0,Math.min(this.reserveBytes,other.maxBytes)-other.queuedBytes);
    }
    if(this.events+count+reservedEvents>this.maxEvents)return {ok:false,reason:"count_limit"};
    if(this.bytes+bytes+reservedBytes>this.maxBytes)return {ok:false,reason:"byte_limit"};
    return {ok:true};
  }
  recordEnqueue(bytes: number): void {
    this.events++;this.bytes+=bytes;
    this.highWaterEvents=Math.max(this.highWaterEvents,this.events);
    this.highWaterBytes=Math.max(this.highWaterBytes,this.bytes);
  }
  release(count: number, bytes: number): void {
    this.events-=count;this.bytes-=bytes;
    if(this.events<0||this.bytes < -1e-7)throw Error("SHARED_INGRESS_ACCOUNTING_INVALID");
    if(Math.abs(this.bytes)<1e-7)this.bytes=0;
  }
  snapshot() {return {depth:this.events,bytes:this.bytes,highWaterEvents:this.highWaterEvents,highWaterBytes:this.highWaterBytes,
    poolMaxEvents:this.maxEvents,poolMaxBytes:this.maxBytes};}
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

  private sharedBudget: SharedIngressBudget | null = null;
  attachSharedBudget(budget: SharedIngressBudget): void {
    if(this.sharedBudget)throw Error("SHARED_INGRESS_ALREADY_ATTACHED");
    budget.register(this);this.sharedBudget=budget;
  }

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
    const admission=this.canAdmit(1,size);
    if(!admission.ok){this.noteGap(admission.reason);return admission;}
    this.items.push({ event, bytes: size });
    this.bytes += size;
    this.accepted += 1;
    this.sharedBudget?.recordEnqueue(size);
    if (this.depth > this.highWaterEvents) this.highWaterEvents = this.depth;
    if (this.bytes > this.highWaterBytes) this.highWaterBytes = this.bytes;
    this.publish(false);
    return { ok: true };
  }

  canAdmit(count: number, bytes: number): { ok: true } | { ok: false; reason: Extract<IngressGapReason, "count_limit" | "byte_limit"> } {
    if(this.sharedBudget)return this.sharedBudget.canAdmit(this,count,bytes);
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
    this.sharedBudget?.release(1,item.bytes);
    if (this.head >= 1_024) {
      this.items.splice(0, this.head);
      this.head = 0;
    }
    this.publish(false);
    return item.event;
  }

  /** Drain queued events. Does not increment gapTotal — overflow already counted at tryEnqueue. */
  clear(): void {
    const count=this.depth,bytes=this.bytes;
    this.items.length = 0;
    this.head = 0;
    this.bytes = 0;
    this.sharedBudget?.release(count,bytes);
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
      ...(this.sharedBudget ? {borrowedEvents:Math.max(0,this.depth-this.maxEvents),borrowedBytes:Math.max(0,this.bytes-this.maxBytes),
        poolMaxEvents:this.sharedBudget.maxEvents,poolMaxBytes:this.sharedBudget.maxBytes} : {}),
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
