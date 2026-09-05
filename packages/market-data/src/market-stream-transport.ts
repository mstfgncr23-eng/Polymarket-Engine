import type { BoundedMarketStream } from "./bounded-clob-market.js";
import type { BoundedIngressQueue } from "./bounded-ingress.js";

/**
 * Market WebSocket streaming contract. Every runtime implementation must be
 * hard-bounded (`bounded: true`). PublicClient.subscribe is not a valid
 * implementation — it is an unbounded it-pushable FIFO.
 */
export interface MarketStreamTransport {
  readonly bounded: true;
  readonly queue?: BoundedIngressQueue;
  subscribe(tokenIds: readonly string[], customFeatureEnabled?: boolean): Promise<BoundedMarketStream>;
}

export function isBoundedMarketStreamTransport(value: unknown): value is MarketStreamTransport {
  if (!value || typeof value !== "object") return false;
  const transport = value as MarketStreamTransport;
  return transport.bounded === true && typeof transport.subscribe === "function";
}
