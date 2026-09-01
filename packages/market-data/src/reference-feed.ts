import { createId, referenceStreamUrl, referenceTickSchema, toBinanceSpotSymbol, type ReferenceTick } from "@pm/core";
import { abortableDelay, ExponentialBackoff } from "./reconnect.js";
import type { FeedConnectionEvent } from "./polymarket.js";

export interface ReferenceFeedHandlers {
  onTick(tick: ReferenceTick): Promise<void> | void;
  onConnection(event: FeedConnectionEvent): Promise<void> | void;
  onError(error: Error): Promise<void> | void;
}

export interface WebSocketLike extends EventTarget {
  readyState: number;
  close(code?: number, reason?: string): void;
}

export type WebSocketFactory = (url: string) => WebSocketLike;

export interface ReferenceFeedOptions {
  asset: string;
  symbol: string;
  url: string;
  venue?: string;
  staleTimeoutMs?: number;
  factory?: WebSocketFactory;
}

export class ReferencePriceFeedAdapter {
  private readonly factory: WebSocketFactory;
  private readonly staleTimeoutMs: number;
  private readonly venue: string;

  constructor(private readonly options: ReferenceFeedOptions) {
    this.factory = options.factory ?? ((url) => new WebSocket(url));
    this.staleTimeoutMs = options.staleTimeoutMs ?? 15_000;
    this.venue = options.venue ?? "binance";
  }

  get asset(): string { return this.options.asset; }
  get symbol(): string { return this.options.symbol; }

  async run(handlers: ReferenceFeedHandlers, signal: AbortSignal): Promise<void> {
    const backoff = new ExponentialBackoff({ initialDelayMs: 500, maxDelayMs: 30_000, multiplier: 2, jitterRatio: 0.2 });
    let reconnectCount = 0;
    while (!signal.aborted) {
      await handlers.onConnection(this.connection("CONNECTING", reconnectCount));
      try {
        await this.runConnection(handlers, signal, reconnectCount);
        if (!signal.aborted) throw new Error("REFERENCE_STREAM_ENDED");
      } catch (error) {
        if (signal.aborted) break;
        reconnectCount += 1;
        const normalized = error instanceof Error ? error : new Error(String(error));
        await handlers.onError(normalized);
        await handlers.onConnection(this.connection("DISCONNECTED", reconnectCount, normalized.message));
        await abortableDelay(backoff.nextDelay(), signal).catch(() => undefined);
      }
    }
    await handlers.onConnection(this.connection("STOPPED", reconnectCount));
  }

  private connection(state: FeedConnectionEvent["state"], reconnectCount: number, message?: string): FeedConnectionEvent {
    return message
      ? { feed: "reference", asset: this.options.asset, state, at: new Date().toISOString(), reconnectCount, message }
      : { feed: "reference", asset: this.options.asset, state, at: new Date().toISOString(), reconnectCount };
  }

  private runConnection(handlers: ReferenceFeedHandlers, signal: AbortSignal, reconnectCount: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const socket = this.factory(this.options.url);
      let lastMessageAt = Date.now();
      let connected = false;
      const staleTimer = setInterval(() => {
        if (connected && Date.now() - lastMessageAt > this.staleTimeoutMs) {
          void handlers.onConnection(this.connection("STALE", reconnectCount));
          socket.close(4000, "stale feed");
        }
      }, Math.max(500, Math.floor(this.staleTimeoutMs / 2)));
      const cleanup = () => clearInterval(staleTimer);
      socket.addEventListener("open", () => {
        connected = true;
        lastMessageAt = Date.now();
        void handlers.onConnection(this.connection("CONNECTED", reconnectCount));
      });
      socket.addEventListener("message", (event) => {
        lastMessageAt = Date.now();
        try { void handlers.onTick(parseBinanceAggTrade((event as MessageEvent).data, this.options.symbol, this.options.asset, this.venue)); }
        catch (error) { void handlers.onError(error instanceof Error ? error : new Error(String(error))); }
      });
      socket.addEventListener("error", () => { cleanup(); reject(new Error("REFERENCE_WEBSOCKET_ERROR")); }, { once: true });
      socket.addEventListener("close", () => { cleanup(); resolve(); }, { once: true });
      signal.addEventListener("abort", () => { cleanup(); socket.close(1000, "shutdown"); resolve(); }, { once: true });
    });
  }
}

/** @deprecated Use ReferencePriceFeedAdapter. Thin BTC-named wrapper for existing tests/callers. */
export class BtcWebSocketAdapter extends ReferencePriceFeedAdapter {
  constructor(url: string, symbol: string, staleTimeoutMs = 15_000, factory: WebSocketFactory = (url) => new WebSocket(url)) {
    super({ url, symbol, asset: symbol.replace(/USDT$/i, "").toUpperCase() || "BTC", staleTimeoutMs, factory });
  }

  override async run(handlers: ReferenceFeedHandlers, signal: AbortSignal): Promise<void> {
    const wrapped: ReferenceFeedHandlers = {
      onTick: handlers.onTick,
      onError: handlers.onError,
      onConnection: async (event) => handlers.onConnection({ ...event, feed: event.feed === "reference" ? "btc" : event.feed }),
    };
    await super.run(wrapped, signal);
  }
}

export function parseBinanceAggTrade(raw: unknown, symbol = "BTCUSDT", asset?: string, venue = "binance"): ReferenceTick {
  const text = typeof raw === "string" ? raw : raw instanceof ArrayBuffer ? new TextDecoder().decode(raw) : String(raw);
  const value = JSON.parse(text) as Record<string, unknown>;
  const timestamp = new Date(Number(value.T ?? value.E));
  const resolvedSymbol = String(value.s ?? symbol);
  return referenceTickSchema.parse({
    eventId: createId("ref"), traceId: createId("trace"),
    asset: (asset ?? resolvedSymbol.replace(/USDT$/i, "")).toUpperCase(),
    symbol: resolvedSymbol, venue,
    price: String(value.p), quantity: String(value.q), sourceTimestamp: timestamp.toISOString(),
    receivedAt: new Date().toISOString(), tradeId: String(value.a ?? value.t),
  });
}

export interface ReferenceResolution {
  asset: string;
  supported: boolean;
  symbol: string | null;
  url: string | null;
  reason: "RESOLVED" | "REFERENCE_UNSUPPORTED";
}

export class ReferenceFeedRegistry {
  private readonly adapters = new Map<string, ReferencePriceFeedAdapter>();
  private readonly supported = new Map<string, ReferenceResolution>();
  private readonly running = new Map<string, AbortController>();

  constructor(
    private readonly baseWsUrl: string,
    private readonly restBaseUrl: string,
    private readonly staleTimeoutMs = 15_000,
    private readonly factory?: WebSocketFactory,
    private readonly probe: (symbol: string) => Promise<boolean> = (symbol) => defaultBinanceSpotProbe(symbol, restBaseUrl),
  ) {}

  async resolve(asset: string): Promise<ReferenceResolution> {
    const existing = this.supported.get(asset);
    if (existing) return existing;
    let symbol: string;
    try { symbol = toBinanceSpotSymbol(asset); }
    catch {
      const failed = { asset, supported: false, symbol: null, url: null, reason: "REFERENCE_UNSUPPORTED" as const };
      this.supported.set(asset, failed);
      return failed;
    }
    const ok = await this.probe(symbol);
    const url = ok ? referenceStreamUrl(this.baseWsUrl, symbol) : null;
    const resolved: ReferenceResolution = ok
      ? { asset, supported: true, symbol, url, reason: "RESOLVED" }
      : { asset, supported: false, symbol, url: null, reason: "REFERENCE_UNSUPPORTED" };
    this.supported.set(asset, resolved);
    return resolved;
  }

  adapterFor(asset: string): ReferencePriceFeedAdapter | null {
    const resolution = this.supported.get(asset);
    if (!resolution?.supported || !resolution.url || !resolution.symbol) return null;
    const existing = this.adapters.get(asset);
    if (existing) return existing;
    const adapter = new ReferencePriceFeedAdapter({
      asset, symbol: resolution.symbol, url: resolution.url, staleTimeoutMs: this.staleTimeoutMs,
      ...(this.factory ? { factory: this.factory } : {}),
    });
    this.adapters.set(asset, adapter);
    return adapter;
  }

  async ensureRunning(
    asset: string,
    handlers: ReferenceFeedHandlers,
    parentSignal: AbortSignal,
  ): Promise<"started" | "already" | "unsupported"> {
    if (this.running.has(asset)) return "already";
    const resolution = await this.resolve(asset);
    if (!resolution.supported) return "unsupported";
    const adapter = this.adapterFor(asset);
    if (!adapter) return "unsupported";
    const child = new AbortController();
    const onParentAbort = () => child.abort(parentSignal.reason);
    if (parentSignal.aborted) {
      child.abort(parentSignal.reason);
      return "unsupported";
    }
    parentSignal.addEventListener("abort", onParentAbort, { once: true });
    this.running.set(asset, child);
    void adapter.run(handlers, child.signal).catch(() => undefined).finally(() => {
      parentSignal.removeEventListener("abort", onParentAbort);
      if (this.running.get(asset) === child) this.running.delete(asset);
    });
    return "started";
  }

  stop(asset: string): boolean {
    const child = this.running.get(asset);
    if (!child) return false;
    this.running.delete(asset);
    this.adapters.delete(asset);
    child.abort("REFERENCE_ASSET_UNNEEDED");
    return true;
  }

  runningAssets(): string[] { return [...this.running.keys()]; }
  assets(): string[] { return [...this.adapters.keys()]; }
}

async function defaultBinanceSpotProbe(symbol: string, restBaseUrl = "https://api.binance.com"): Promise<boolean> {
  try {
    const response = await fetch(`${restBaseUrl.replace(/\/$/, "")}/api/v3/exchangeInfo?symbol=${encodeURIComponent(symbol)}`, {
      signal: AbortSignal.timeout(5_000),
    });
    if (!response.ok) return false;
    const body = await response.json() as { symbols?: Array<{ status?: string; symbol?: string }> };
    const match = body.symbols?.find((item) => item.symbol === symbol);
    return match?.status === "TRADING";
  } catch {
    return false;
  }
}

export function createRestProbe(restBaseUrl: string): (symbol: string) => Promise<boolean> {
  return (symbol) => defaultBinanceSpotProbe(symbol, restBaseUrl);
}
