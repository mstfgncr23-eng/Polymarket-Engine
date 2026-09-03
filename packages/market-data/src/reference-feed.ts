import { createId, referenceStreamUrl, referenceTickSchema, toBinanceSpotSymbol, type ReferenceTick } from "@pm/core";
import { abortableDelay, ExponentialBackoff, type ReconnectOptions } from "./reconnect.js";
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

export type ReferenceCloseKind =
  | "operator_shutdown"
  | "asset_unneeded"
  | "clean_remote_close"
  | "network_error"
  | "stale_feed"
  | "websocket_error";

export interface ReferenceCloseEvent {
  kind: ReferenceCloseKind;
  code: number | null;
  reason: string;
  wasClean: boolean;
  abortInitiated: boolean;
  reconnectCount: number;
  asset: string;
}

export interface ReferenceFeedOptions {
  asset: string;
  symbol: string;
  url: string;
  venue?: string;
  staleTimeoutMs?: number;
  factory?: WebSocketFactory;
  reconnect?: Partial<ReconnectOptions>;
}

const DEFAULT_RECONNECT: ReconnectOptions = { initialDelayMs: 500, maxDelayMs: 30_000, multiplier: 2, jitterRatio: 0.2 };

export function formatReferenceClose(closed: Omit<ReferenceCloseEvent, "reconnectCount" | "asset"> & { reconnectCount?: number; asset?: string }): string {
  return `REFERENCE_CLOSE:kind=${closed.kind}:code=${closed.code ?? "none"}:wasClean=${closed.wasClean}:abort=${closed.abortInitiated}:reason=${closed.reason || ""}`;
}

export function classifyReferenceClose(input: {
  abortInitiated: boolean;
  abortReason: unknown;
  code: number | null;
  reason: string;
  wasClean: boolean;
  sawError: boolean;
  staleForced: boolean;
}): ReferenceCloseKind {
  if (input.abortInitiated) {
    const reason = `${input.abortReason ?? ""} ${input.reason}`.trim();
    if (reason.includes("REFERENCE_ASSET_UNNEEDED")) return "asset_unneeded";
    return "operator_shutdown";
  }
  if (input.staleForced || input.code === 4000 || input.reason === "stale feed") return "stale_feed";
  if (input.sawError) return "websocket_error";
  if (input.wasClean && (input.code === 1000 || input.code === 1001)) return "clean_remote_close";
  return "network_error";
}

export class ReferencePriceFeedAdapter {
  private readonly factory: WebSocketFactory;
  private readonly staleTimeoutMs: number;
  private readonly venue: string;
  private readonly reconnect: ReconnectOptions;
  private inFlight = false;

  constructor(private readonly options: ReferenceFeedOptions) {
    this.factory = options.factory ?? ((url) => new WebSocket(url));
    this.staleTimeoutMs = options.staleTimeoutMs ?? 15_000;
    this.venue = options.venue ?? "binance";
    this.reconnect = {
      initialDelayMs: options.reconnect?.initialDelayMs ?? DEFAULT_RECONNECT.initialDelayMs,
      maxDelayMs: options.reconnect?.maxDelayMs ?? DEFAULT_RECONNECT.maxDelayMs,
      multiplier: options.reconnect?.multiplier ?? DEFAULT_RECONNECT.multiplier,
      jitterRatio: options.reconnect?.jitterRatio ?? DEFAULT_RECONNECT.jitterRatio,
    };
  }

  get asset(): string { return this.options.asset; }
  get symbol(): string { return this.options.symbol; }
  get running(): boolean { return this.inFlight; }

  async run(handlers: ReferenceFeedHandlers, signal: AbortSignal): Promise<void> {
    if (this.inFlight) throw new Error(`REFERENCE_FEED_ALREADY_RUNNING:${this.options.asset}`);
    this.inFlight = true;
    const backoff = new ExponentialBackoff(this.reconnect);
    let reconnectCount = 0;
    try {
      while (!signal.aborted) {
        await handlers.onConnection(this.connection("CONNECTING", reconnectCount));
        const closed = await this.runConnection(handlers, signal, reconnectCount, backoff);
        if (signal.aborted || closed.kind === "operator_shutdown" || closed.kind === "asset_unneeded") break;
        reconnectCount += 1;
        const message = formatReferenceClose({ ...closed, reconnectCount, asset: this.options.asset });
        if (closed.kind === "network_error" || closed.kind === "websocket_error" || closed.kind === "stale_feed") {
          await handlers.onError(new Error(message));
        }
        await handlers.onConnection(this.connection("DISCONNECTED", reconnectCount, message));
        await abortableDelay(backoff.nextDelay(), signal).catch(() => undefined);
      }
      await handlers.onConnection(this.connection("STOPPED", reconnectCount));
    } finally {
      this.inFlight = false;
    }
  }

  private connection(state: FeedConnectionEvent["state"], reconnectCount: number, message?: string): FeedConnectionEvent {
    return message
      ? { feed: "reference", asset: this.options.asset, state, at: new Date().toISOString(), reconnectCount, message }
      : { feed: "reference", asset: this.options.asset, state, at: new Date().toISOString(), reconnectCount };
  }

  private runConnection(
    handlers: ReferenceFeedHandlers,
    signal: AbortSignal,
    reconnectCount: number,
    backoff: ExponentialBackoff,
  ): Promise<Omit<ReferenceCloseEvent, "reconnectCount" | "asset"> & { reconnectCount: number; asset: string }> {
    return new Promise((resolve) => {
      const socket = this.factory(this.options.url);
      let lastMessageAt = Date.now();
      let connected = false;
      let settled = false;
      let abortInitiated = false;
      let sawError = false;
      let staleForced = false;
      const staleTimer = setInterval(() => {
        if (connected && Date.now() - lastMessageAt > this.staleTimeoutMs) {
          staleForced = true;
          void handlers.onConnection(this.connection("STALE", reconnectCount));
          try { socket.close(4000, "stale feed"); } catch { /* ignore */ }
        }
      }, Math.max(500, Math.floor(this.staleTimeoutMs / 2)));
      const finish = (input: { code: number | null; reason: string; wasClean: boolean }) => {
        if (settled) return;
        settled = true;
        clearInterval(staleTimer);
        signal.removeEventListener("abort", onAbort);
        const kind = classifyReferenceClose({
          abortInitiated, abortReason: signal.reason, code: input.code, reason: input.reason,
          wasClean: input.wasClean, sawError, staleForced,
        });
        resolve({
          kind, code: input.code, reason: input.reason, wasClean: input.wasClean, abortInitiated,
          reconnectCount, asset: this.options.asset,
        });
      };
      const onAbort = () => {
        abortInitiated = true;
        try { socket.close(1000, "shutdown"); } catch { /* close adapters may be test doubles */ }
        finish({ code: 1000, reason: "shutdown", wasClean: true });
      };
      socket.addEventListener("open", () => {
        connected = true;
        lastMessageAt = Date.now();
        backoff.reset();
        void handlers.onConnection(this.connection("CONNECTED", reconnectCount));
      });
      socket.addEventListener("message", (event) => {
        lastMessageAt = Date.now();
        try { void handlers.onTick(parseBinanceAggTrade((event as MessageEvent).data, this.options.symbol, this.options.asset, this.venue)); }
        catch (error) { void handlers.onError(error instanceof Error ? error : new Error(String(error))); }
      });
      socket.addEventListener("error", () => {
        sawError = true;
        finish({ code: null, reason: "REFERENCE_WEBSOCKET_ERROR", wasClean: false });
      }, { once: true });
      socket.addEventListener("close", (event) => {
        const close = event as Partial<CloseEvent>;
        const code = typeof close.code === "number" ? close.code : null;
        const reason = typeof close.reason === "string" ? close.reason : "";
        const wasClean = close.wasClean === true;
        finish({ code, reason, wasClean });
      }, { once: true });
      if (signal.aborted) onAbort();
      else signal.addEventListener("abort", onAbort, { once: true });
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

export type ReferenceSupportStatus = "SUPPORTED" | "UNSUPPORTED_DEFINITIVE" | "TRANSIENT_FAILURE" | "PROVIDER_BLOCKED" | "REGION_BLOCKED";

export type ReferenceProbeOutcome = {
  kind: "supported" | "unsupported_definitive" | "transient_failure" | "provider_blocked";
  detail: string;
};

export interface ReferenceResolution {
  asset: string;
  supported: boolean;
  status: ReferenceSupportStatus;
  symbol: string | null;
  url: string | null;
  reason: "RESOLVED" | "REFERENCE_UNSUPPORTED" | "REFERENCE_PROBE_TRANSIENT" | "REFERENCE_PROVIDER_BLOCKED";
  detail?: string;
  retryAfterMs?: number;
}

interface CachedResolution {
  resolution: ReferenceResolution;
  retryAfter: number;
}

export interface ReferenceRegistryOptions {
  negativeCacheTtlMs?: number;
  now?: () => number;
}

interface AssetSession {
  controller: AbortController;
  done: Promise<void>;
}

export class ReferenceFeedRegistry {
  private readonly adapters = new Map<string, ReferencePriceFeedAdapter>();
  private readonly cache = new Map<string, CachedResolution>();
  private readonly sessions = new Map<string, AssetSession>();
  private readonly chains = new Map<string, Promise<unknown>>();
  private readonly transientBackoff = new Map<string, ExponentialBackoff>();
  private readonly now: () => number;
  private readonly negativeCacheTtlMs: number;

  constructor(
    private readonly baseWsUrl: string,
    private readonly restBaseUrl: string,
    private readonly staleTimeoutMs = 15_000,
    private readonly factory?: WebSocketFactory,
    private readonly probe: (symbol: string) => Promise<ReferenceProbeOutcome> = (symbol) => defaultBinanceSpotProbe(symbol, restBaseUrl),
    options: ReferenceRegistryOptions = {},
  ) {
    this.now = options.now ?? Date.now;
    this.negativeCacheTtlMs = options.negativeCacheTtlMs ?? 30_000;
  }

  async resolve(asset: string): Promise<ReferenceResolution> {
    const cached = this.cache.get(asset);
    if (cached && this.isReusable(cached)) return cached.resolution;
    let symbol: string;
    try { symbol = toBinanceSpotSymbol(asset); }
    catch {
      return this.remember(asset, {
        asset, supported: false, status: "UNSUPPORTED_DEFINITIVE", symbol: null, url: null,
        reason: "REFERENCE_UNSUPPORTED", detail: "invalid_asset",
      });
    }
    const outcome = await this.probe(symbol);
    if (outcome.kind === "supported") {
      this.transientBackoff.get(asset)?.reset();
      return this.remember(asset, {
        asset, supported: true, status: "SUPPORTED", symbol, url: referenceStreamUrl(this.baseWsUrl, symbol),
        reason: "RESOLVED", detail: outcome.detail,
      });
    }
    if (outcome.kind === "unsupported_definitive") {
      return this.remember(asset, {
        asset, supported: false, status: "UNSUPPORTED_DEFINITIVE", symbol, url: null,
        reason: "REFERENCE_UNSUPPORTED", detail: outcome.detail,
      });
    }
    if (outcome.kind === "provider_blocked") {
      const region = /451/.test(outcome.detail);
      return this.remember(asset, {
        asset, supported: false, status: region ? "REGION_BLOCKED" : "PROVIDER_BLOCKED", symbol, url: null,
        reason: "REFERENCE_PROVIDER_BLOCKED", detail: outcome.detail, retryAfterMs: 1_800_000,
      }, this.now() + 1_800_000);
    }
    const backoff = this.transientBackoff.get(asset) ?? new ExponentialBackoff({ initialDelayMs: 1_000, maxDelayMs: 60_000, multiplier: 2, jitterRatio: 0 });
    this.transientBackoff.set(asset, backoff);
    const retryAfterMs = this.negativeCacheTtlMs <= 0 ? 0 : Math.max(this.negativeCacheTtlMs, backoff.nextDelay());
    return this.remember(asset, {
      asset, supported: false, status: "TRANSIENT_FAILURE", symbol, url: null,
      reason: "REFERENCE_PROBE_TRANSIENT", detail: outcome.detail, retryAfterMs,
    }, this.now() + retryAfterMs);
  }

  adapterFor(asset: string): ReferencePriceFeedAdapter | null {
    const resolution = this.cache.get(asset)?.resolution;
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
  ): Promise<"started" | "already" | "unsupported" | "transient"> {
    return this.enqueue(asset, async () => {
      const existing = this.sessions.get(asset);
      if (existing && !existing.controller.signal.aborted) return "already";
      if (existing) await existing.done;
      const raced = this.sessions.get(asset);
      if (raced && !raced.controller.signal.aborted) return "already";
      const resolution = await this.resolve(asset);
      if (resolution.status === "TRANSIENT_FAILURE") return "transient";
      if (resolution.status !== "SUPPORTED") return "unsupported";
      const adapter = this.adapterFor(asset);
      if (!adapter) return "unsupported";
      if (adapter.running) return "already";
      const child = new AbortController();
      const onParentAbort = () => child.abort(parentSignal.reason);
      if (parentSignal.aborted) {
        child.abort(parentSignal.reason);
        return "transient";
      }
      parentSignal.addEventListener("abort", onParentAbort, { once: true });
      let release!: () => void;
      const done = new Promise<void>((resolve) => { release = resolve; });
      const session: AssetSession = { controller: child, done };
      this.sessions.set(asset, session);
      void adapter.run(handlers, child.signal).catch(() => undefined).finally(() => {
        parentSignal.removeEventListener("abort", onParentAbort);
        if (this.sessions.get(asset) === session) this.sessions.delete(asset);
        release();
      });
      return "started";
    });
  }

  stop(asset: string): boolean {
    const session = this.sessions.get(asset);
    if (!session || session.controller.signal.aborted) return false;
    this.adapters.delete(asset);
    session.controller.abort("REFERENCE_ASSET_UNNEEDED");
    return true;
  }

  runningAssets(): string[] {
    return [...this.sessions.entries()].filter(([, session]) => !session.controller.signal.aborted).map(([asset]) => asset);
  }
  sessionAssets(): string[] { return [...this.sessions.keys()]; }
  assets(): string[] { return [...this.adapters.keys()]; }

  private enqueue<T>(asset: string, fn: () => Promise<T>): Promise<T> {
    const previous = this.chains.get(asset) ?? Promise.resolve();
    const next = previous.then(fn, fn);
    this.chains.set(asset, next.then(() => undefined, () => undefined));
    return next;
  }

  private isReusable(entry: CachedResolution): boolean {
    if (entry.resolution.status === "SUPPORTED" || entry.resolution.status === "UNSUPPORTED_DEFINITIVE") return true;
    return this.now() < entry.retryAfter;
  }

  private remember(asset: string, resolution: ReferenceResolution, retryAfter = Number.POSITIVE_INFINITY): ReferenceResolution {
    this.cache.set(asset, { resolution, retryAfter });
    return resolution;
  }
}

export function classifyBinanceExchangeInfo(input: { ok: boolean; status: number; body: unknown; symbol?: string }): ReferenceProbeOutcome {
  if (input.status === 451 || input.status === 403) {
    return { kind: "provider_blocked", detail: `http_${input.status}` };
  }
  if (input.status === 408 || input.status === 429 || input.status >= 500) {
    return { kind: "transient_failure", detail: `http_${input.status}` };
  }
  if (!input.ok) {
    const code = input.body && typeof input.body === "object" ? (input.body as { code?: unknown }).code : undefined;
    if (input.status === 400 && (code === -1121 || code === -1122)) {
      return { kind: "unsupported_definitive", detail: "invalid_symbol" };
    }
    return { kind: "transient_failure", detail: `http_${input.status}` };
  }
  const body = input.body as { symbols?: Array<{ status?: string; symbol?: string }> } | null;
  const match = input.symbol
    ? body?.symbols?.find((item) => item.symbol === input.symbol)
    : body?.symbols?.[0];
  if (!match) return { kind: "unsupported_definitive", detail: "symbol_not_listed" };
  if (match.status === "TRADING") return { kind: "supported", detail: "trading" };
  return { kind: "transient_failure", detail: `status_${match.status ?? "unknown"}` };
}

async function defaultBinanceSpotProbe(symbol: string, restBaseUrl = "https://api.binance.com"): Promise<ReferenceProbeOutcome> {
  try {
    const response = await fetch(`${restBaseUrl.replace(/\/$/, "")}/api/v3/exchangeInfo?symbol=${encodeURIComponent(symbol)}`, {
      signal: AbortSignal.timeout(5_000),
    });
    let body: unknown = null;
    try { body = await response.json(); } catch { body = null; }
    return classifyBinanceExchangeInfo({ ok: response.ok, status: response.status, body, symbol });
  } catch (error) {
    const name = error instanceof Error ? error.name : "";
    const message = error instanceof Error ? error.message : String(error);
    if (name === "TimeoutError" || name === "AbortError" || /timeout/i.test(message)) {
      return { kind: "transient_failure", detail: "timeout" };
    }
    return { kind: "transient_failure", detail: "network" };
  }
}

export function createRestProbe(restBaseUrl: string): (symbol: string) => Promise<ReferenceProbeOutcome> {
  return (symbol) => defaultBinanceSpotProbe(symbol, restBaseUrl);
}
