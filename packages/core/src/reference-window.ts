import { TRIGGER_STABILITY_BOUNDS } from "./stability-bounds.js";
import type { ReferenceTick } from "./domain.js";

export interface StoredReferenceTick {
  tick: ReferenceTick;
  sourceMs: number;
}

export interface ReferenceWindowPushResult {
  accepted: boolean;
  overflow: boolean;
  size: number;
}

/**
 * O(1) ring for the exact 60s reference window. Never uses Array.shift.
 * If the finite cap is reached while every retained tick is still in-window,
 * the push is refused and overflow is reported — in-window data is not dropped
 * to make room.
 */
export class ReferenceTickWindow {
  private readonly slots: Array<StoredReferenceTick | undefined>;
  private head = 0;
  private count = 0;
  private overflowed = false;
  readonly maxEntries: number;
  readonly windowMs: number;

  constructor(
    maxEntries: number = TRIGGER_STABILITY_BOUNDS.referenceWindowMaxEntries,
    windowMs: number = TRIGGER_STABILITY_BOUNDS.referenceWindowMs,
  ) {
    this.maxEntries = maxEntries;
    this.windowMs = windowMs;
    this.slots = new Array<StoredReferenceTick | undefined>(maxEntries);
  }

  get size(): number { return this.count; }
  get hasOverflowed(): boolean { return this.overflowed; }

  first(): StoredReferenceTick | undefined {
    return this.count > 0 ? this.slots[this.head] : undefined;
  }

  push(tick: ReferenceTick, sourceMs = Date.parse(tick.sourceTimestamp)): ReferenceWindowPushResult {
    if (this.overflowed) {
      return { accepted: false, overflow: true, size: this.count };
    }
    if (!Number.isFinite(sourceMs)) {
      return { accepted: false, overflow: false, size: this.count };
    }
    const cutoff = sourceMs - this.windowMs;
    this.evictExpired(cutoff);
    if (this.count >= this.maxEntries) {
      this.overflowed = true;
      return { accepted: false, overflow: true, size: this.count };
    }
    const index = (this.head + this.count) % this.maxEntries;
    this.slots[index] = { tick, sourceMs };
    this.count += 1;
    return { accepted: true, overflow: false, size: this.count };
  }

  clear(): void {
    for (let i = 0; i < this.count; i += 1) this.slots[(this.head + i) % this.maxEntries] = undefined;
    this.head = 0;
    this.count = 0;
    this.overflowed = false;
  }

  resetAfterReconnect(): void {
    this.clear();
  }

  private evictExpired(cutoff: number): void {
    while (this.count > 0) {
      const first = this.slots[this.head];
      if (!first || first.sourceMs >= cutoff) break;
      this.slots[this.head] = undefined;
      this.head = (this.head + 1) % this.maxEntries;
      this.count -= 1;
    }
  }
}
