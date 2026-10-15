import type { MarketSnapshot } from "@pm/core";

export function bookTiming(book: MarketSnapshot, nowMs: number) {
  const source = Date.parse(book.sourceTimestamp);
  const received = Date.parse(book.receivedAt);
  const ingress = book.wsCallbackAt === undefined ? null : Date.parse(book.wsCallbackAt);
  return { sourceAgeMs: nowMs - source, receivedAgeMs: nowMs - received,
    sourceToIngressMs: ingress === null ? null : ingress - source,
    ingressToReceivedMs: ingress === null ? null : received - ingress,
    receivedToDecisionMs: nowMs - received };
}

export function firstLegBookReason(input: {
  marketId: string; tokenId: string; outcome: "UP" | "DOWN";
  canonicalTokenId: string | null | undefined; synchronized: boolean;
  book: MarketSnapshot | null | undefined; expected?: MarketSnapshot;
  nowMs: number; maxAgeMs: number;
}): string | null {
  const b = input.book;
  if (!b || !input.canonicalTokenId || !input.synchronized) return "ENTRY_BOOK_UNAVAILABLE";
  if (b.marketId !== input.marketId || b.tokenId !== input.tokenId || b.outcome !== input.outcome
    || b.tokenId !== input.canonicalTokenId) return "ENTRY_BOOK_IDENTITY_CHANGED";
  if (!b.eventId || !Number.isInteger(b.sequence) || b.sequence < 0) return "ENTRY_BOOK_PROVENANCE_MISSING";
  if (input.expected && (b.eventId !== input.expected.eventId || b.sequence !== input.expected.sequence
    || b.hash !== input.expected.hash || b.sourceTimestamp !== input.expected.sourceTimestamp
    || b.receivedAt !== input.expected.receivedAt)) return "ENTRY_BOOK_CHANGED";
  const timing = bookTiming(b, input.nowMs);
  if (!Number.isFinite(input.nowMs) || !Number.isFinite(timing.sourceAgeMs)
    || !Number.isFinite(timing.receivedAgeMs)) return "ENTRY_BOOK_TIMESTAMP_INVALID";
  if (timing.sourceAgeMs < 0 || timing.receivedAgeMs < 0) return "ENTRY_BOOK_FUTURE";
  if (timing.sourceAgeMs > input.maxAgeMs) return "ENTRY_BOOK_SOURCE_STALE";
  if (timing.receivedAgeMs > input.maxAgeMs) return "ENTRY_BOOK_LOCAL_STALE";
  if (b.wsCallbackAt !== undefined && (!Number.isFinite(Date.parse(b.wsCallbackAt))
    || Date.parse(b.wsCallbackAt) > Date.parse(b.receivedAt)
    || input.nowMs - Date.parse(b.wsCallbackAt) > input.maxAgeMs)) return "ENTRY_BOOK_LOCAL_BACKLOG";
  return null;
}
