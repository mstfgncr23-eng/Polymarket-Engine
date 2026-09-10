import { Buffer } from "node:buffer";
import { performance } from "node:perf_hooks";
import { BoundedIngressQueue, IngressOverflowError } from "./bounded-ingress.js";
import { DeliverySlicePolicy, type MonotonicClock } from "./delivery-slice.js";
import { CLOB_MARKET_WS_URL, MARKET_CONNECT_BOUNDS, MARKET_INGRESS_BOUNDS } from "./ingress-bounds.js";
import type { MarketDataObservability, MarketSourceEventType } from "./market-observability.js";
import type { MarketStreamTransport } from "./market-stream-transport.js";

export interface BoundedMarketStream {
  close(intent?: BoundedMarketCloseIntent): Promise<void>;
  closeDiagnostic?(): ClobMarketCloseDiagnostic | null;
  pendingWaiterCount(): number;
  pingTimerActive(): boolean;
  streamGeneration(): number;
  [Symbol.asyncIterator](): AsyncIterator<unknown>;
}

export type BoundedMarketCloseIntent = "operator_shutdown" | "generation_change";

export type ClobMarketCloseKind =
  | BoundedMarketCloseIntent
  | "clean_remote_close"
  | "abnormal_remote_close"
  | "socket_error"
  | "stale_heartbeat"
  | "connect_failure";

export interface ClobMarketCloseDiagnostic {
  kind: ClobMarketCloseKind;
  code: number | null;
  reason: string;
  wasClean: boolean;
}

export class ClobMarketCloseError extends Error {
  constructor(
    readonly baseMessage: string,
    readonly diagnostic: ClobMarketCloseDiagnostic,
  ) {
    super(`${baseMessage}: ${formatClobMarketClose(diagnostic)}`);
    this.name = "ClobMarketCloseError";
  }
}

export function formatClobMarketClose(diagnostic: ClobMarketCloseDiagnostic): string {
  return `CLOB_MARKET_CLOSE:kind=${diagnostic.kind}:code=${diagnostic.code ?? "none"}:wasClean=${diagnostic.wasClean}:reason=${diagnostic.reason || ""}`;
}

export function classifyClobRemoteClose(input: Pick<ClobMarketCloseDiagnostic, "code" | "wasClean">):
  | "clean_remote_close"
  | "abnormal_remote_close" {
  return input.wasClean && (input.code === 1000 || input.code === 1001)
    ? "clean_remote_close"
    : "abnormal_remote_close";
}

export interface BoundedMarketTransportOptions {
  url?: string;
  maxEvents?: number;
  maxBytes?: number;
  maxRawFrameBytes?: number;
  queue?: BoundedIngressQueue;
  pingIntervalMs?: number;
  staleMs?: number;
  connectTimeoutMs?: number;
  webSocket?: typeof WebSocket;
  observability?: MarketDataObservability;
  /** Experimental comparison-candidate delivery count override. */
  maxDeliveryEventsPerSlice?: number;
  /** Experimental comparison-candidate monotonic time bound override. */
  maxDeliverySliceMs?: number;
  /** Internal deterministic-clock hook for scheduler tests. */
  monotonicNow?: MonotonicClock;
}

export const CLOB_MARKET_CONNECT_TIMEOUT = "CLOB_MARKET_WS_CONNECT_TIMEOUT";
export const CLOB_MARKET_CONNECT_ABORTED = "CLOB_MARKET_WS_CONNECT_ABORTED";
export const CLOB_MARKET_CLOSED_BEFORE_OPEN = "CLOB_MARKET_WS_CLOSED_BEFORE_OPEN";
export const CLOB_MARKET_WS_ERROR = "CLOB_MARKET_WS_ERROR";
export const CLOB_MARKET_WS_STALE = "CLOB_MARKET_WS_STALE";
export const CLOB_MARKET_WS_OPEN_FAILED = "CLOB_MARKET_WS_OPEN_FAILED";
export const MARKET_INGRESS_MALFORMED = "MARKET_INGRESS_MALFORMED";

const DEFAULT_DELIVERY_EVENTS_PER_SLICE = 128;
const DEFAULT_DELIVERY_SLICE_MS = 0.5;

interface StreamWaiter {
  resolve: (result: IteratorResult<unknown>) => void;
  reject: (error: Error) => void;
}

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function canonicalMarketEvent(type: string, payload: Record<string, unknown>) {
  return { topic: "market", type, payload };
}

/**
 * The owned transport speaks directly to the public CLOB websocket, whose
 * wire payload uses event_type / asset_id / snake_case. The rest of the
 * market-data package intentionally consumes the PublicClient-style
 * type/payload/camelCase event shape. Normalize only at this boundary.
 */
export function normalizeRawClobMarketEvent(event: unknown): unknown {
  const value = record(event);
  if (!value) return event;
  if (typeof value.type === "string" && record(value.payload)) return event;

  const eventType = typeof value.event_type === "string" ? value.event_type : null;
  if (!eventType) return event;

  if (eventType === "book") {
    return canonicalMarketEvent("book", {
      market: value.market,
      tokenId: value.asset_id,
      bids: value.bids,
      asks: value.asks,
      hash: value.hash,
      timestamp: value.timestamp,
      minOrderSize: value.min_order_size,
      tickSize: value.tick_size,
      negRisk: value.neg_risk,
      lastTradePrice: value.last_trade_price,
    });
  }

  if (eventType === "price_change") {
    const priceChanges = Array.isArray(value.price_changes)
      ? value.price_changes.map((item) => {
        const change = record(item) ?? {};
        return {
          tokenId: change.asset_id,
          price: change.price,
          size: change.size,
          side: change.side,
          hash: change.hash,
          bestBid: change.best_bid,
          bestAsk: change.best_ask,
        };
      })
      : value.price_changes;
    return canonicalMarketEvent("price_change", {
      market: value.market,
      priceChanges,
      timestamp: value.timestamp,
    });
  }

  if (eventType === "last_trade_price") {
    return canonicalMarketEvent("last_trade_price", {
      market: value.market,
      tokenId: value.asset_id,
      price: value.price,
      size: value.size,
      feeRateBps: value.fee_rate_bps,
      side: value.side,
      timestamp: value.timestamp,
      transactionHash: value.transaction_hash,
    });
  }

  return event;
}

function observeSafely(observer: MarketDataObservability | null, callback: () => void): void {
  if (!observer) return;
  try { callback(); } catch { /* observability must not change feed semantics */ }
}

function sourceEventObservation(event: unknown): { type: MarketSourceEventType; priceChangeEntries?: number } {
  const value = record(event);
  const rawType = typeof value?.type === "string"
    ? value.type
    : typeof value?.event_type === "string" ? value.event_type : "";
  const type: MarketSourceEventType = rawType === "book"
    || rawType === "price_change"
    || rawType === "last_trade_price"
    ? rawType
    : "other";
  if (type !== "price_change") return { type };
  const payload = record(value?.payload);
  return {
    type,
    priceChangeEntries: Array.isArray(payload?.priceChanges) ? payload.priceChanges.length : 0,
  };
}

/**
 * Owned CLOB market WebSocket. Nearest controlled layer in front of the
 * unbounded @polymarket/client it-pushable registry.
 *
 * Subscribe frame matches the official SDK (`type: "market"`, `assets_ids`,
 * `custom_feature_enabled`). Fresh subscribe is how the CLOB emits canonical
 * `book` snapshots; this transport does not invent books.
 *
 * Terminalization is total and idempotent: every pending iterator.next()
 * settles on malformed ingress, stale socket, runtime error, remote close,
 * overflow, explicit close, or abort. Error terminals reject; explicit close
 * and remote close complete cleanly. Ping/listeners are detached on any
 * terminal. A dead generation never enqueues into a later stream's queue.
 */
export class BoundedClobMarketTransport implements MarketStreamTransport {
  readonly bounded = true as const;
  readonly queue: BoundedIngressQueue;
  private readonly url: string;
  private readonly pingIntervalMs: number;
  private readonly staleMs: number;
  private readonly connectTimeoutMs: number;
  private readonly maxRawFrameBytes: number;
  private readonly WebSocketImpl: typeof WebSocket;
  private observability: MarketDataObservability | null;
  private readonly maxDeliveryEventsPerSlice: number;
  private readonly maxDeliverySliceMs: number;
  private readonly monotonicNow: MonotonicClock;
  private generation = 0;

  constructor(options: BoundedMarketTransportOptions = {}) {
    this.url = options.url ?? CLOB_MARKET_WS_URL;
    this.queue = options.queue ?? new BoundedIngressQueue(
      options.maxEvents ?? MARKET_INGRESS_BOUNDS.maxEvents,
      options.maxBytes ?? MARKET_INGRESS_BOUNDS.maxBytes,
    );
    this.pingIntervalMs = options.pingIntervalMs ?? 10_000;
    this.staleMs = options.staleMs ?? 30_000;
    this.connectTimeoutMs = options.connectTimeoutMs ?? MARKET_CONNECT_BOUNDS.connectTimeoutMs;
    this.maxRawFrameBytes = options.maxRawFrameBytes ?? MARKET_INGRESS_BOUNDS.maxRawFrameBytes;
    this.WebSocketImpl = options.webSocket ?? WebSocket;
    this.observability = options.observability ?? null;
    this.maxDeliveryEventsPerSlice = options.maxDeliveryEventsPerSlice ?? DEFAULT_DELIVERY_EVENTS_PER_SLICE;
    this.maxDeliverySliceMs = options.maxDeliverySliceMs ?? DEFAULT_DELIVERY_SLICE_MS;
    this.monotonicNow = options.monotonicNow ?? (() => performance.now());
  }

  currentGeneration(): number { return this.generation; }

  setObservability(observer: MarketDataObservability | null): void { this.observability = observer; }

  async subscribe(
    tokenIds: readonly string[],
    customFeatureEnabled = true,
    signal?: AbortSignal,
  ): Promise<BoundedMarketStream> {
    const generation = ++this.generation;
    const observability = this.observability;
    const queue = this.queue;
    queue.clear();
    const socket = new this.WebSocketImpl(this.url);
    const waiters: StreamWaiter[] = [];
    const listeners: Array<{ type: string; fn: EventListenerOrEventListenerObject }> = [];
    let pendingError: Error | null = null;
    let closeDiagnostic: ClobMarketCloseDiagnostic | null = null;
    let ended = false;
    let finalized = false;
    let opened = false;
    let lastPong = Date.now();
    let pingTimer: ReturnType<typeof setInterval> | undefined;
    let deliveryHandle: ReturnType<typeof setImmediate> | undefined;
    let deliveryScheduled = false;
    const deliverySlice = new DeliverySlicePolicy({
      maxEvents: this.maxDeliveryEventsPerSlice,
      maxSliceMs: this.maxDeliverySliceMs,
      now: this.monotonicNow,
    });

    const attach = (type: string, fn: EventListenerOrEventListenerObject, options?: AddEventListenerOptions) => {
      socket.addEventListener(type, fn, options);
      listeners.push({ type, fn });
    };

    const detachAll = () => {
      for (const { type, fn } of listeners) socket.removeEventListener(type, fn);
      listeners.length = 0;
      if (pingTimer) {
        clearInterval(pingTimer);
        pingTimer = undefined;
      }
    };

    const settleWaiters = () => {
      while (waiters.length > 0) {
        const waiter = waiters.shift()!;
        if (pendingError) waiter.reject(pendingError);
        else waiter.resolve({ value: undefined, done: true });
      }
    };

    const isCurrentGeneration = () => generation === this.generation;

    const deliverAvailable = () => {
      while (waiters.length > 0 && queue.depth > 0 && deliverySlice.canDeliver()) {
        if (pendingError || ended || !isCurrentGeneration()) {
          settleWaiters();
          return;
        }
        const item = queue.dequeue();
        if (item === undefined) return;
        if (pendingError || ended || !isCurrentGeneration()) {
          settleWaiters();
          return;
        }
        deliverySlice.recordDelivery();
        waiters.shift()!.resolve({ value: item, done: false });
      }

      if (pendingError || ended || !isCurrentGeneration()) {
        settleWaiters();
        return;
      }
      if (waiters.length > 0 && queue.depth > 0) schedulePump();
    };

    const pump = () => {
      if (pendingError || ended || !isCurrentGeneration()) {
        settleWaiters();
        return;
      }
      deliverySlice.begin();
      deliverAvailable();
    };

    const cancelScheduledPump = () => {
      if (deliveryHandle !== undefined) {
        clearImmediate(deliveryHandle);
        deliveryHandle = undefined;
      }
      deliveryScheduled = false;
    };

    const schedulePump = () => {
      if (deliveryScheduled || finalized) return;
      deliveryScheduled = true;
      deliveryHandle = setImmediate(() => {
        deliveryHandle = undefined;
        deliveryScheduled = false;
        pump();
      });
      deliveryHandle.unref?.();
    };

    const finalize = (error: Error | null, diagnostic?: ClobMarketCloseDiagnostic) => {
      if (finalized) return;
      finalized = true;
      ended = true;
      closeDiagnostic = diagnostic ?? (error instanceof ClobMarketCloseError ? error.diagnostic : closeDiagnostic);
      if (error && !pendingError) pendingError = error;
      detachAll();
      cancelScheduledPump();
      deliverySlice.reset();
      signal?.removeEventListener("abort", onAbort);
      try { socket.close(); } catch { /* already closed */ }
      settleWaiters();
    };

    const diagnosticError = (baseMessage: string, diagnostic: ClobMarketCloseDiagnostic): ClobMarketCloseError => {
      return new ClobMarketCloseError(baseMessage, diagnostic);
    };

    const abortError = (): ClobMarketCloseError => {
      const intent: BoundedMarketCloseIntent = signal?.reason === "MARKET_STREAM_RESUBSCRIBE"
        ? "generation_change"
        : "operator_shutdown";
      return diagnosticError(CLOB_MARKET_CONNECT_ABORTED, {
        kind: intent, code: null, reason: String(signal?.reason ?? ""), wasClean: true,
      });
    };

    const closeMetadata = (event: Event): Pick<ClobMarketCloseDiagnostic, "code" | "reason" | "wasClean"> => {
      const close = event as Partial<CloseEvent>;
      return {
        code: typeof close.code === "number" && Number.isFinite(close.code) ? close.code : null,
        reason: typeof close.reason === "string" ? close.reason : "",
        wasClean: close.wasClean === true,
      };
    };

    let finishConnect: ((error?: Error) => void) | null = null;
    const onAbort = () => {
      if (finishConnect) {
        finishConnect(abortError());
        return;
      }
      finalize(abortError());
    };

    const enqueueFrame = (raw: string, callbackStarted: number) => {
      if (finalized || generation !== this.generation) return;
      if (raw.length > this.maxRawFrameBytes) {
        queue.noteOverflow("byte_limit");
        finalize(new IngressOverflowError("byte_limit"));
        return;
      }
      let parsed: unknown;
      const parseStarted = performance.now();
      try { parsed = JSON.parse(raw) as unknown; } catch {
        observeSafely(observability, () => observability?.onReceiveTiming({
          parseDurationSeconds: (performance.now() - parseStarted) / 1000,
        }));
        finalize(new Error(MARKET_INGRESS_MALFORMED));
        return;
      }
      const parseDurationSeconds = (performance.now() - parseStarted) / 1000;
      const expansionStarted = performance.now();
      const events = Array.isArray(parsed) ? parsed : [parsed];
      const expansionDurationSeconds = (performance.now() - expansionStarted) / 1000;
      observeSafely(observability, () => observability?.onSourceFrame(events.length));
      const admitStarted = performance.now();
      const admit = queue.canAdmit(events.length, raw.length);
      const admissionCheckDurationSeconds = (performance.now() - admitStarted) / 1000;
      if (!admit.ok) {
        observeSafely(observability, () => observability?.onReceiveTiming({
          parseDurationSeconds,
          preAdmissionDurationSeconds: parseDurationSeconds + expansionDurationSeconds + admissionCheckDurationSeconds,
        }));
        queue.noteOverflow(admit.reason);
        finalize(new IngressOverflowError(admit.reason));
        return;
      }
      const n = events.length;
      let remaining = raw.length;
      let normalizationDurationSeconds = 0;
      for (let i = 0; i < n; i += 1) {
        if (finalized || generation !== this.generation) return;
        const share = i === n - 1 ? remaining : Math.floor(raw.length / n);
        remaining -= share;
        const normalizationStarted = performance.now();
        const normalized = normalizeRawClobMarketEvent(events[i]);
        normalizationDurationSeconds += (performance.now() - normalizationStarted) / 1000;
        const source = sourceEventObservation(normalized);
        observeSafely(observability, () => observability?.onSourceEvent(source));
        const result = queue.tryEnqueueWeighted(normalized, share);
        if (!result.ok) {
          finalize(new IngressOverflowError(result.reason));
          return;
        }
      }
      observeSafely(observability, () => observability?.onReceiveTiming({
        parseDurationSeconds,
        preAdmissionDurationSeconds: parseDurationSeconds + expansionDurationSeconds + admissionCheckDurationSeconds + normalizationDurationSeconds,
        callbackToAdmissionDurationSeconds: (performance.now() - callbackStarted) / 1000,
      }));
      schedulePump();
    };

    const onMessage = (message: Event) => {
      const callbackStarted = performance.now();
      if (finalized || generation !== this.generation) return;
      const data = (message as MessageEvent).data;
      const raw = typeof data === "string" ? data : String(data ?? "");
      const bytes = Buffer.byteLength(raw, "utf8");
      const kind = raw === "PONG" ? "control" : "market";
      observeSafely(observability, () => observability?.onRawFrame({ kind, bytes }));
      if (raw === "PONG") {
        lastPong = Date.now();
        return;
      }
      enqueueFrame(raw, callbackStarted);
    };

    const startPing = () => {
      lastPong = Date.now();
      pingTimer = setInterval(() => {
        if (finalized || generation !== this.generation) {
          if (pingTimer) {
            clearInterval(pingTimer);
            pingTimer = undefined;
          }
          return;
        }
        if (Date.now() - lastPong > this.staleMs) {
          finalize(diagnosticError(CLOB_MARKET_WS_STALE, {
            kind: "stale_heartbeat", code: null, reason: CLOB_MARKET_WS_STALE, wasClean: false,
          }));
          return;
        }
        try { socket.send("PING"); } catch {
          finalize(diagnosticError(CLOB_MARKET_WS_ERROR, {
            kind: "socket_error", code: null, reason: CLOB_MARKET_WS_ERROR, wasClean: false,
          }));
        }
      }, this.pingIntervalMs);
    };

    const onRuntimeError = () => finalize(diagnosticError(CLOB_MARKET_WS_ERROR, {
      kind: "socket_error", code: null, reason: CLOB_MARKET_WS_ERROR, wasClean: false,
    }));
    const onRemoteClose = (event: Event) => {
      const metadata = closeMetadata(event);
      finalize(null, { ...metadata, kind: classifyClobRemoteClose(metadata) });
    };

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const connectTimer = setTimeout(() => {
        finish(diagnosticError(CLOB_MARKET_CONNECT_TIMEOUT, {
          kind: "connect_failure", code: null, reason: CLOB_MARKET_CONNECT_TIMEOUT, wasClean: false,
        }));
      }, this.connectTimeoutMs);
      connectTimer.unref?.();

      const onConnectError = () => finish(diagnosticError(CLOB_MARKET_WS_ERROR, {
        kind: "connect_failure", code: null, reason: CLOB_MARKET_WS_ERROR, wasClean: false,
      }));
      const onCloseDuringConnect = (event: Event) => {
        if (!opened) finish(diagnosticError(CLOB_MARKET_CLOSED_BEFORE_OPEN, {
          kind: "connect_failure", ...closeMetadata(event),
        }));
      };
      const onOpened = () => {
        opened = true;
        try {
          socket.send(JSON.stringify({
            assets_ids: [...tokenIds],
            custom_feature_enabled: customFeatureEnabled,
            type: "market",
          }));
          startPing();
        } catch (error) {
          const baseMessage = error instanceof Error ? error.message : CLOB_MARKET_WS_OPEN_FAILED;
          finish(diagnosticError(baseMessage, {
            kind: "connect_failure", code: null, reason: baseMessage, wasClean: false,
          }));
          return;
        }
        finish();
      };

      const finish = (error?: Error) => {
        if (settled) return;
        settled = true;
        finishConnect = null;
        clearTimeout(connectTimer);
        socket.removeEventListener("open", onOpened);
        socket.removeEventListener("error", onConnectError);
        socket.removeEventListener("close", onCloseDuringConnect);
        if (!error) {
          attach("close", onRemoteClose);
          attach("error", onRuntimeError);
          if (signal?.aborted) {
            finalize(abortError());
            resolve();
            return;
          }
          resolve();
          return;
        }
        const normalized = error instanceof ClobMarketCloseError
          ? error
          : diagnosticError(error.message, {
            kind: "connect_failure", code: null, reason: error.message, wasClean: false,
          });
        finalize(normalized);
        reject(normalized);
      };
      finishConnect = finish;

      attach("message", onMessage as EventListener);
      socket.addEventListener("open", onOpened, { once: true });
      socket.addEventListener("error", onConnectError, { once: true });
      socket.addEventListener("close", onCloseDuringConnect);
      if (signal?.aborted) {
        finish(abortError());
        return;
      }
      signal?.addEventListener("abort", onAbort);
    });

    return {
      close: async (intent = "operator_shutdown") => {
        finalize(null, { kind: intent, code: null, reason: "", wasClean: true });
      },
      closeDiagnostic: () => closeDiagnostic,
      pendingWaiterCount: () => waiters.length,
      pingTimerActive: () => pingTimer != null,
      streamGeneration: () => generation,
      [Symbol.asyncIterator]: () => ({
        next: async () => {
          if (pendingError) throw pendingError;
          if (ended || !isCurrentGeneration()) {
            if (pendingError) throw pendingError;
            return { value: undefined, done: true };
          }
          return await new Promise<IteratorResult<unknown>>((resolve, reject) => {
            waiters.push({ resolve, reject });
            if (deliverySlice.active && deliverySlice.hasDelivered && deliverySlice.canDeliver()) {
              deliverAvailable();
            } else {
              schedulePump();
            }
          });
        },
      }),
    };
  }
}
