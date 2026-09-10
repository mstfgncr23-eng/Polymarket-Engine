export const MARKET_RAW_FRAME_KINDS = ["market", "control"] as const;
export type MarketRawFrameKind = (typeof MARKET_RAW_FRAME_KINDS)[number];

export const MARKET_SOURCE_EVENT_TYPES = ["book", "price_change", "last_trade_price", "other"] as const;
export type MarketSourceEventType = (typeof MARKET_SOURCE_EVENT_TYPES)[number];

export const MARKET_GENERATION_END_REASONS = [
  "provider_close",
  "operator_generation_change",
  "overflow",
  "malformed",
  "stale",
  "abort_shutdown",
  "other",
] as const;
export type MarketGenerationEndReason = (typeof MARKET_GENERATION_END_REASONS)[number];

/**
 * Optional, bounded market-feed observability. Implementations must be
 * synchronous and must not throw into the transport or adapter hot paths.
 */
export interface MarketDataObservability {
  onGenerationStart(input: {
    generation: number;
    tokenCount: number;
    startedAtMs: number;
    queueHighWaterEvents: number;
    queueHighWaterBytes: number;
  }): void;
  onRawFrame(input: { kind: MarketRawFrameKind; bytes: number }): void;
  onSourceFrame(eventCount: number): void;
  onSourceEvent(input: { type: MarketSourceEventType; priceChangeEntries?: number }): void;
  onReceiveTiming(input: {
    parseDurationSeconds: number;
    preAdmissionDurationSeconds?: number;
    callbackToAdmissionDurationSeconds?: number;
  }): void;
  onNormalizeEvent(input: { durationSeconds: number; snapshotsProduced: number }): void;
  onSnapshotCallback(input: {
    synchronousDurationSeconds: number;
    totalDurationSeconds: number;
    awaitedDurationSeconds: number;
  }): void;
  onTrustedFullBook(): void;
  onGenerationEnd(input: {
    generation: number;
    endedAtMs: number;
    durationMs: number;
    queueHighWaterEvents: number;
    queueHighWaterBytes: number;
    closeCode: number | null;
    closeReason: string;
    endReason: MarketGenerationEndReason;
  }): void;
}
