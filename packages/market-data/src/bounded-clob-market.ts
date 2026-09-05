import { BoundedIngressQueue, IngressOverflowError } from "./bounded-ingress.js";
import { CLOB_MARKET_WS_URL, MARKET_CONNECT_BOUNDS, MARKET_INGRESS_BOUNDS } from "./ingress-bounds.js";
import type { MarketStreamTransport } from "./market-stream-transport.js";

export interface BoundedMarketStream {
  close(): Promise<void>;
  [Symbol.asyncIterator](): AsyncIterator<unknown>;
}

export interface BoundedMarketTransportOptions {
  url?: string;
  maxEvents?: number;
  maxBytes?: number;
  queue?: BoundedIngressQueue;
  pingIntervalMs?: number;
  staleMs?: number;
  connectTimeoutMs?: number;
  webSocket?: typeof WebSocket;
}

export const CLOB_MARKET_CONNECT_TIMEOUT = "CLOB_MARKET_WS_CONNECT_TIMEOUT";
export const CLOB_MARKET_CONNECT_ABORTED = "CLOB_MARKET_WS_CONNECT_ABORTED";
export const CLOB_MARKET_CLOSED_BEFORE_OPEN = "CLOB_MARKET_WS_CLOSED_BEFORE_OPEN";

/**
 * Owned CLOB market WebSocket. Nearest controlled layer in front of the
 * unbounded @polymarket/client it-pushable registry.
 *
 * Subscribe frame matches the official SDK (`type: "market"`, `assets_ids`,
 * `custom_feature_enabled`). Fresh subscribe is how the CLOB emits canonical
 * `book` snapshots; this transport does not invent books.
 *
 * Connect is finitely bounded: timeout, error, close-before-open, and AbortSignal
 * all reject the subscribe promise. Listeners/timers are cleaned on settle.
 */
export class BoundedClobMarketTransport implements MarketStreamTransport {
  readonly bounded = true as const;
  readonly queue: BoundedIngressQueue;
  private readonly url: string;
  private readonly pingIntervalMs: number;
  private readonly staleMs: number;
  private readonly connectTimeoutMs: number;
  private readonly WebSocketImpl: typeof WebSocket;

  constructor(options: BoundedMarketTransportOptions = {}) {
    this.url = options.url ?? CLOB_MARKET_WS_URL;
    this.queue = options.queue ?? new BoundedIngressQueue(
      options.maxEvents ?? MARKET_INGRESS_BOUNDS.maxEvents,
      options.maxBytes ?? MARKET_INGRESS_BOUNDS.maxBytes,
    );
    this.pingIntervalMs = options.pingIntervalMs ?? 10_000;
    this.staleMs = options.staleMs ?? 30_000;
    this.connectTimeoutMs = options.connectTimeoutMs ?? MARKET_CONNECT_BOUNDS.connectTimeoutMs;
    this.WebSocketImpl = options.webSocket ?? WebSocket;
  }

  async subscribe(
    tokenIds: readonly string[],
    customFeatureEnabled = true,
    signal?: AbortSignal,
  ): Promise<BoundedMarketStream> {
    const queue = this.queue;
    queue.clear();
    const socket = new this.WebSocketImpl(this.url);
    let closed = false;
    let lastPong = Date.now();
    let pingTimer: ReturnType<typeof setInterval> | undefined;
    const waiters: Array<(result: IteratorResult<unknown>) => void> = [];
    let pendingError: Error | null = null;
    let ended = false;
    let opened = false;

    const wake = () => {
      while (waiters.length > 0) {
        if (pendingError) {
          waiters.shift();
          continue;
        }
        const item = queue.dequeue();
        if (item !== undefined) {
          waiters.shift()!({ value: item, done: false });
          continue;
        }
        if (ended) {
          waiters.shift()!({ value: undefined, done: true });
          continue;
        }
        break;
      }
    };

    const fail = (error: Error) => {
      pendingError = error;
      ended = true;
      while (waiters.length > 0) {
        waiters.shift()!({ value: undefined, done: true });
      }
    };

    const enqueue = (event: unknown) => {
      const result = queue.tryEnqueue(event);
      if (!result.ok) {
        fail(new IngressOverflowError(result.reason));
        void closeSocket();
        return;
      }
      wake();
    };

    const closeSocket = async () => {
      if (closed) return;
      closed = true;
      if (pingTimer) clearInterval(pingTimer);
      ended = true;
      try { socket.close(); } catch { /* already closed */ }
      wake();
    };

    const onOpen = () => {
      socket.send(JSON.stringify({
        assets_ids: [...tokenIds],
        custom_feature_enabled: customFeatureEnabled,
        type: "market",
      }));
      lastPong = Date.now();
      pingTimer = setInterval(() => {
        if (Date.now() - lastPong > this.staleMs) {
          fail(new Error("CLOB_MARKET_WS_STALE"));
          void closeSocket();
          return;
        }
        try { socket.send("PING"); } catch { /* closed */ }
      }, this.pingIntervalMs);
    };

    const onMessage = (message: { data?: unknown }) => {
      const data = typeof message.data === "string" ? message.data : String(message.data);
      if (data === "PONG") {
        lastPong = Date.now();
        return;
      }
      let parsed: unknown;
      try { parsed = JSON.parse(data) as unknown; } catch {
        fail(new Error("MARKET_INGRESS_MALFORMED"));
        void closeSocket();
        return;
      }
      const events = Array.isArray(parsed) ? parsed : [parsed];
      for (const event of events) enqueue(event);
    };

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const connectTimer = setTimeout(() => {
        finish(new Error(CLOB_MARKET_CONNECT_TIMEOUT));
      }, this.connectTimeoutMs);
      connectTimer.unref?.();

      const onAbort = () => finish(new Error(CLOB_MARKET_CONNECT_ABORTED));
      const onError = () => finish(new Error("CLOB_MARKET_WS_ERROR"));
      const onClose = () => {
        if (!opened) {
          finish(new Error(CLOB_MARKET_CLOSED_BEFORE_OPEN));
          return;
        }
        ended = true;
        wake();
      };
      const onOpened = () => {
        opened = true;
        onOpen();
        finish();
      };

      const cleanup = () => {
        clearTimeout(connectTimer);
        signal?.removeEventListener("abort", onAbort);
        socket.removeEventListener("open", onOpened);
        socket.removeEventListener("error", onError);
        socket.removeEventListener("close", onCloseDuringConnect);
      };

      const onCloseDuringConnect = () => onClose();

      const finish = (error?: Error) => {
        if (settled) return;
        settled = true;
        cleanup();
        if (!error) {
          socket.addEventListener("close", () => {
            ended = true;
            wake();
          });
          resolve();
          return;
        }
        ended = true;
        void closeSocket();
        reject(error);
      };

      socket.addEventListener("open", onOpened, { once: true });
      socket.addEventListener("error", onError, { once: true });
      socket.addEventListener("close", onCloseDuringConnect);
      socket.addEventListener("message", onMessage);
      if (signal?.aborted) {
        finish(new Error(CLOB_MARKET_CONNECT_ABORTED));
        return;
      }
      signal?.addEventListener("abort", onAbort);
    });

    return {
      close: closeSocket,
      [Symbol.asyncIterator]: () => ({
        next: async () => {
          if (pendingError) throw pendingError;
          const existing = queue.dequeue();
          if (existing !== undefined) return { value: existing, done: false };
          if (ended) {
            if (pendingError) throw pendingError;
            return { value: undefined, done: true };
          }
          return await new Promise<IteratorResult<unknown>>((resolve) => {
            waiters.push(resolve);
            wake();
          }).then((result) => {
            if (pendingError) throw pendingError;
            return result;
          });
        },
      }),
    };
  }
}
