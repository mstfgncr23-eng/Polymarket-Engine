import { btcTickSchema, createId, type BtcTick } from "@pm/core";
import { abortableDelay, ExponentialBackoff } from "./reconnect.js";
import type { FeedConnectionEvent } from "./polymarket.js";

export interface BtcFeedHandlers {
  onTick(tick: BtcTick): Promise<void> | void;
  onConnection(event: FeedConnectionEvent): Promise<void> | void;
  onError(error: Error): Promise<void> | void;
}

export interface WebSocketLike extends EventTarget {
  readyState: number;
  close(code?: number, reason?: string): void;
}

export type WebSocketFactory = (url: string) => WebSocketLike;

export class BtcWebSocketAdapter {
  constructor(
    private readonly url: string,
    private readonly symbol: string,
    private readonly staleTimeoutMs = 15_000,
    private readonly factory: WebSocketFactory = (url) => new WebSocket(url),
  ) {}

  async run(handlers: BtcFeedHandlers, signal: AbortSignal): Promise<void> {
    const backoff = new ExponentialBackoff({ initialDelayMs: 500, maxDelayMs: 30_000, multiplier: 2, jitterRatio: 0.2 });
    let reconnectCount = 0;
    while (!signal.aborted) {
      await handlers.onConnection({ feed: "btc", state: "CONNECTING", at: new Date().toISOString(), reconnectCount });
      try {
        await this.runConnection(handlers, signal, reconnectCount);
        if (!signal.aborted) throw new Error("BTC_STREAM_ENDED");
      } catch (error) {
        if (signal.aborted) break;
        reconnectCount += 1;
        const normalized = error instanceof Error ? error : new Error(String(error));
        await handlers.onError(normalized);
        await handlers.onConnection({ feed: "btc", state: "DISCONNECTED", at: new Date().toISOString(), reconnectCount, message: normalized.message });
        await abortableDelay(backoff.nextDelay(), signal).catch(() => undefined);
      }
    }
    await handlers.onConnection({ feed: "btc", state: "STOPPED", at: new Date().toISOString(), reconnectCount });
  }

  private runConnection(handlers: BtcFeedHandlers, signal: AbortSignal, reconnectCount: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const socket = this.factory(this.url);
      let lastMessageAt = Date.now();
      let connected = false;
      const staleTimer = setInterval(() => {
        if (connected && Date.now() - lastMessageAt > this.staleTimeoutMs) {
          void handlers.onConnection({ feed: "btc", state: "STALE", at: new Date().toISOString(), reconnectCount });
          socket.close(4000, "stale feed");
        }
      }, Math.max(500, Math.floor(this.staleTimeoutMs / 2)));
      const cleanup = () => clearInterval(staleTimer);
      socket.addEventListener("open", () => {
        connected = true;
        lastMessageAt = Date.now();
        void handlers.onConnection({ feed: "btc", state: "CONNECTED", at: new Date().toISOString(), reconnectCount });
      });
      socket.addEventListener("message", (event) => {
        lastMessageAt = Date.now();
        try { void handlers.onTick(parseBinanceAggTrade((event as MessageEvent).data, this.symbol)); }
        catch (error) { void handlers.onError(error instanceof Error ? error : new Error(String(error))); }
      });
      socket.addEventListener("error", () => { cleanup(); reject(new Error("BTC_WEBSOCKET_ERROR")); }, { once: true });
      socket.addEventListener("close", () => { cleanup(); resolve(); }, { once: true });
      signal.addEventListener("abort", () => { cleanup(); socket.close(1000, "shutdown"); resolve(); }, { once: true });
    });
  }
}

export function parseBinanceAggTrade(raw: unknown, symbol = "BTCUSDT"): BtcTick {
  const text = typeof raw === "string" ? raw : raw instanceof ArrayBuffer ? new TextDecoder().decode(raw) : String(raw);
  const value = JSON.parse(text) as Record<string, unknown>;
  const timestamp = new Date(Number(value.T ?? value.E));
  return btcTickSchema.parse({
    eventId: createId("btc"), traceId: createId("trace"), symbol: String(value.s ?? symbol),
    price: String(value.p), quantity: String(value.q), sourceTimestamp: timestamp.toISOString(),
    receivedAt: new Date().toISOString(), tradeId: String(value.a ?? value.t),
  });
}
