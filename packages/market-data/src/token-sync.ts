import type { IngressGapReason, TokenSyncSnapshot, TokenSyncState } from "./ingress-bounds.js";

interface TokenSyncRecord {
  state: TokenSyncState;
  epoch: number;
  gapReason: IngressGapReason | null;
}

export class TokenSyncIndex {
  private readonly tokens = new Map<string, TokenSyncRecord>();

  ensure(tokenId: string, epoch = 1): TokenSyncRecord {
    const existing = this.tokens.get(tokenId);
    if (existing) return existing;
    const created: TokenSyncRecord = { state: "AWAITING_BOOK", epoch, gapReason: null };
    this.tokens.set(tokenId, created);
    return created;
  }

  beginEpoch(tokenIds: readonly string[], reason: IngressGapReason): number {
    let epoch = 1;
    for (const tokenId of tokenIds) {
      const current = this.tokens.get(tokenId);
      if (current && current.epoch + 1 > epoch) epoch = current.epoch + 1;
    }
    for (const tokenId of tokenIds) {
      const previous = this.tokens.get(tokenId);
      const nextEpoch = previous ? previous.epoch + 1 : epoch;
      this.tokens.set(tokenId, {
        state: reason === "reconnect" || reason === "generation_change" || reason === "explicit_resubscribe"
          ? "AWAITING_BOOK"
          : "GAPPED",
        epoch: nextEpoch,
        gapReason: reason,
      });
    }
    return epoch;
  }

  markGapped(tokenId: string, reason: IngressGapReason): void {
    const current = this.ensure(tokenId);
    current.state = "GAPPED";
    current.gapReason = reason;
  }

  markAwaiting(tokenId: string): void {
    const current = this.ensure(tokenId);
    current.state = "AWAITING_BOOK";
  }

  markSyncedFromBook(tokenId: string): { epoch: number; wasResync: boolean } {
    const current = this.ensure(tokenId);
    const wasResync = current.state === "GAPPED" || current.state === "AWAITING_BOOK";
    current.state = "SYNCED";
    current.gapReason = null;
    return { epoch: current.epoch, wasResync };
  }

  isSynchronized(tokenId: string): boolean {
    return this.tokens.get(tokenId)?.state === "SYNCED";
  }

  stateOf(tokenId: string): TokenSyncState {
    return this.tokens.get(tokenId)?.state ?? "AWAITING_BOOK";
  }

  epochOf(tokenId: string): number {
    return this.tokens.get(tokenId)?.epoch ?? 0;
  }

  gapReason(tokenId: string): IngressGapReason | null {
    return this.tokens.get(tokenId)?.gapReason ?? null;
  }

  unsynchronizedCount(): number {
    let count = 0;
    for (const record of this.tokens.values()) {
      if (record.state !== "SYNCED") count += 1;
    }
    return count;
  }

  drop(tokenId: string): void {
    this.tokens.delete(tokenId);
  }

  retain(tokenIds: readonly string[]): void {
    const keep = new Set(tokenIds);
    for (const tokenId of [...this.tokens.keys()]) {
      if (!keep.has(tokenId)) this.tokens.delete(tokenId);
    }
    for (const tokenId of tokenIds) this.ensure(tokenId, this.tokens.get(tokenId)?.epoch ?? 1);
  }

  snapshots(): TokenSyncSnapshot[] {
    return [...this.tokens.entries()].map(([tokenId, record]) => ({
      tokenId,
      state: record.state,
      epoch: record.epoch,
      gapReason: record.gapReason,
    }));
  }

  entryCount(): number {
    return this.tokens.size;
  }
}
