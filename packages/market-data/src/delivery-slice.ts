/**
 * Internal delivery-slice accounting. The transport owns when a slice starts;
 * this helper only answers whether another queued item may be delivered.
 */
export type MonotonicClock = () => number;

export interface DeliverySlicePolicyOptions {
  maxEvents: number;
  maxSliceMs: number;
  now?: MonotonicClock;
}

function positiveInteger(value: number, fallback: number): number {
  return Number.isFinite(value) && value >= 1 ? Math.floor(value) : fallback;
}

function positiveMilliseconds(value: number, fallback: number): number {
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

export class DeliverySlicePolicy {
  readonly maxEvents: number;
  readonly maxSliceMs: number;
  private readonly now: MonotonicClock;
  private startedAtMs: number | null = null;
  private delivered = 0;

  constructor(options: DeliverySlicePolicyOptions) {
    this.maxEvents = positiveInteger(options.maxEvents, 1);
    this.maxSliceMs = positiveMilliseconds(options.maxSliceMs, 1);
    this.now = options.now ?? (() => performance.now());
  }

  begin(): void {
    this.startedAtMs = this.now();
    this.delivered = 0;
  }

  reset(): void {
    this.startedAtMs = null;
    this.delivered = 0;
  }

  get active(): boolean { return this.startedAtMs !== null; }
  get hasDelivered(): boolean { return this.delivered > 0; }
  get deliveredInSlice(): number { return this.delivered; }

  canDeliver(): boolean {
    if (this.startedAtMs === null) return false;
    const elapsedMs = Math.max(0, this.now() - this.startedAtMs);
    return this.delivered < this.maxEvents && elapsedMs < this.maxSliceMs;
  }

  recordDelivery(): void {
    this.delivered += 1;
  }
}
