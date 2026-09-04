/**
 * Architectural constants for owned market-ingress queues.
 * One place for count + approximate-byte bounds. Not a production-CPU claim.
 */

export const INGRESS_GAP_REASONS = [
  "count_limit",
  "byte_limit",
  "reconnect",
  "generation_change",
  "reference_window_limit",
  "malformed",
  "explicit_resubscribe",
] as const;

export type IngressGapReason = (typeof INGRESS_GAP_REASONS)[number];

export const MARKET_INGRESS_BOUNDS = {
  maxEvents: 4_096,
  maxBytes: 4 * 1024 * 1024,
} as const;

export const CLOB_MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

export const TOKEN_SYNC_STATES = ["AWAITING_BOOK", "SYNCED", "GAPPED"] as const;
export type TokenSyncState = (typeof TOKEN_SYNC_STATES)[number];

export interface IngressQueueSnapshot {
  depth: number;
  bytes: number;
  highWaterEvents: number;
  highWaterBytes: number;
  accepted: number;
  processed: number;
  gapTotal: number;
}

export interface TokenSyncSnapshot {
  tokenId: string;
  state: TokenSyncState;
  epoch: number;
  gapReason: IngressGapReason | null;
}

/**
 * Installed @polymarket/client@0.8.1 subscribe findings (source maps + it-pushable@3.2.4).
 *
 * - subscribe() → ClobMarketWebSocketManager.subscribe → SubscriptionRegistry.add
 *   creates `pushable<TEvent>({ objectMode: true })` (websockets/registry.ts).
 * - it-pushable options are only `objectMode` and `onEnd`. There is no
 *   highWaterMark, maxSize, or backpressure hook.
 * - WS onmessage parses then `registry.dispatch` → `queue.push` immediately,
 *   independent of consumer pull rate. FIFO, unbounded.
 * - SubscriptionHandle exposes only `close()` + AsyncIterator. `readableLength`
 *   exists on the Pushable but is not forwarded.
 * - Slow consumer: the SDK queue grows without bound. No observable depth API.
 * - SDK reconnect reuses the same Pushable and does not signal a gap.
 * - Therefore SDK subscribe is NOT a hard-bounded ingress. This package owns a
 *   bounded CLOB market transport at the WebSocket layer we control.
 */
export const POLYMARKET_CLIENT_SUBSCRIBE_FINDINGS = {
  package: "@polymarket/client@0.8.1",
  queue: "it-pushable objectMode FIFO",
  bounded: false,
  configurableBound: false,
  observableDepth: false,
  slowConsumer: "unbounded_buffer_then_yield",
  sdkReconnectSignalsGap: false,
} as const;
