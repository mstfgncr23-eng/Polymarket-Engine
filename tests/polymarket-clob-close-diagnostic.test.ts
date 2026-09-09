import { describe, expect, it } from "vitest";
import {
  BoundedClobMarketTransport,
  ClobMarketCloseError,
  PolymarketMarketDataAdapter,
  ScriptedBoundedMarketTransport,
  classifyClobRemoteClose,
  CLOB_MARKET_CONNECT_TIMEOUT,
} from "@pm/market-data";

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function closeEvent(code: number, reason: string, wasClean: boolean): Event {
  return Object.assign(new Event("close"), { code, reason, wasClean });
}

class DiagnosticWebSocket extends EventTarget {
  static instances: DiagnosticWebSocket[] = [];
  readyState = 0;
  readonly sent: string[] = [];

  constructor(readonly url: string) {
    super();
    DiagnosticWebSocket.instances.push(this);
  }

  send(data: string): void { this.sent.push(data); }

  close(): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatchEvent(closeEvent(1000, "", true));
  }

  open(): void {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }

  remoteClose(code: number, reason: string, wasClean: boolean): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatchEvent(closeEvent(code, reason, wasClean));
  }

  error(): void {
    this.dispatchEvent(new Event("error"));
  }
}

class RecordingScriptedTransport extends ScriptedBoundedMarketTransport {
  readonly customFeatureEnabled: boolean[] = [];

  override async subscribe(tokenIds: readonly string[], customFeatureEnabled = true, signal?: AbortSignal) {
    this.customFeatureEnabled.push(customFeatureEnabled);
    return super.subscribe(tokenIds, customFeatureEnabled, signal);
  }
}

async function openStream(options: ConstructorParameters<typeof BoundedClobMarketTransport>[0] = {}) {
  DiagnosticWebSocket.instances = [];
  const transport = new BoundedClobMarketTransport({
    webSocket: DiagnosticWebSocket as unknown as typeof WebSocket,
    pingIntervalMs: 20,
    staleMs: 40,
    ...options,
  });
  const pending = transport.subscribe(["tok-up"]);
  await waitFor(() => DiagnosticWebSocket.instances.length === 1);
  const socket = DiagnosticWebSocket.instances[0]!;
  socket.open();
  return { transport, stream: await pending, socket };
}

describe("Polymarket CLOB remote close diagnostics", () => {
  it("passes customFeatureEnabled=false on the live adapter subscription path", async () => {
    const transport = new RecordingScriptedTransport();
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport });
    adapter.registerManualToken("tok-up", "tok-up", "UP", "BTC");
    const abort = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: () => undefined,
      onError: () => undefined,
    }, abort.signal);

    await waitFor(() => transport.customFeatureEnabled.length === 1);
    expect(transport.customFeatureEnabled).toEqual([false]);
    abort.abort("test");
    await running;
  });

  it("writes custom_feature_enabled=false in the owned WebSocket subscribe frame", async () => {
    DiagnosticWebSocket.instances = [];
    const transport = new BoundedClobMarketTransport({
      webSocket: DiagnosticWebSocket as unknown as typeof WebSocket,
      connectTimeoutMs: 1_000,
    });
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue });
    adapter.registerManualToken("tok-up", "tok-up", "UP", "BTC");
    const connections: Array<{ state: string }> = [];
    const abort = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: (event) => { connections.push(event); },
      onError: () => undefined,
    }, abort.signal);

    await waitFor(() => DiagnosticWebSocket.instances.length === 1);
    const socket = DiagnosticWebSocket.instances[0]!;
    socket.open();
    await waitFor(() => connections.some((event) => event.state === "CONNECTED"));

    expect(JSON.parse(socket.sent[0]!)).toEqual({
      assets_ids: ["tok-up"],
      custom_feature_enabled: false,
      type: "market",
    });
    abort.abort("test");
    await running;
  });

  it("preserves code, reason, and wasClean on a remote close", async () => {
    const { stream, socket } = await openStream();
    const next = stream[Symbol.asyncIterator]().next();

    socket.remoteClose(1008, "server maintenance", true);

    await expect(next).resolves.toEqual({ value: undefined, done: true });
    expect(stream.closeDiagnostic?.()).toEqual({
      kind: "abnormal_remote_close",
      code: 1008,
      reason: "server maintenance",
      wasClean: true,
    });
  });

  it("distinguishes clean and abnormal remote closes", async () => {
    const clean = await openStream();
    const cleanNext = clean.stream[Symbol.asyncIterator]().next();
    clean.socket.remoteClose(1000, "", true);
    await expect(cleanNext).resolves.toEqual({ value: undefined, done: true });
    expect(clean.stream.closeDiagnostic?.()?.kind).toBe("clean_remote_close");

    const abnormal = await openStream();
    const abnormalNext = abnormal.stream[Symbol.asyncIterator]().next();
    abnormal.socket.remoteClose(1006, "network reset", false);
    await expect(abnormalNext).resolves.toEqual({ value: undefined, done: true });
    expect(abnormal.stream.closeDiagnostic?.()?.kind).toBe("abnormal_remote_close");

    expect(classifyClobRemoteClose({ code: 1000, wasClean: true })).toBe("clean_remote_close");
    expect(classifyClobRemoteClose({ code: 1001, wasClean: true })).toBe("clean_remote_close");
    expect(classifyClobRemoteClose({ code: 1006, wasClean: false })).toBe("abnormal_remote_close");
    expect(classifyClobRemoteClose({ code: 1000, wasClean: false })).toBe("abnormal_remote_close");
  });

  it("propagates abnormal remote close diagnostics and keeps the existing reconnect delay path", async () => {
    DiagnosticWebSocket.instances = [];
    const transport = new BoundedClobMarketTransport({
      webSocket: DiagnosticWebSocket as unknown as typeof WebSocket,
      connectTimeoutMs: 1_000,
    });
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue });
    adapter.registerManualToken("tok-up", "tok-up", "UP", "BTC");
    const connections: Array<{ state: string; message?: string }> = [];
    const errors: Error[] = [];
    const abort = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: (event) => { connections.push(event); },
      onError: (error) => { errors.push(error); },
    }, abort.signal);

    await waitFor(() => DiagnosticWebSocket.instances.length === 1);
    DiagnosticWebSocket.instances[0]!.open();
    await waitFor(() => connections.some((event) => event.state === "CONNECTED"));
    const closedAt = Date.now();
    DiagnosticWebSocket.instances[0]!.remoteClose(1006, "upstream reset", false);
    await waitFor(() => errors.length === 1 && connections.some((event) => event.state === "DISCONNECTED"));

    expect(errors[0]).toBeInstanceOf(ClobMarketCloseError);
    expect((errors[0] as ClobMarketCloseError).diagnostic).toEqual({
      kind: "abnormal_remote_close",
      code: 1006,
      reason: "upstream reset",
      wasClean: false,
    });
    expect(errors[0]!.message).toContain("CLOB_MARKET_CLOSE:kind=abnormal_remote_close:code=1006:wasClean=false:reason=upstream reset");
    expect(connections.find((event) => event.state === "DISCONNECTED")?.message).toContain("kind=abnormal_remote_close");

    await waitFor(() => DiagnosticWebSocket.instances.length === 2);
    expect(adapter.reconnectDelays).toHaveLength(1);
    expect(adapter.reconnectDelays[0]).toBeGreaterThanOrEqual(400);
    expect(adapter.reconnectDelays[0]).toBeLessThanOrEqual(600);
    expect(Date.now() - closedAt).toBeGreaterThanOrEqual(350);

    DiagnosticWebSocket.instances[1]!.open();
    await waitFor(() => connections.filter((event) => event.state === "CONNECTED").length === 2);
    abort.abort("test");
    await running;
  });

  it("labels runtime socket errors and stale heartbeats separately", async () => {
    const runtime = await openStream();
    const runtimeNext = runtime.stream[Symbol.asyncIterator]().next();
    runtime.socket.error();
    await expect(runtimeNext).rejects.toMatchObject({
      diagnostic: { kind: "socket_error", code: null, wasClean: false },
    });

    const stale = await openStream({ pingIntervalMs: 10, staleMs: 25 });
    const staleNext = stale.stream[Symbol.asyncIterator]().next();
    await expect(staleNext).rejects.toMatchObject({
      diagnostic: { kind: "stale_heartbeat", code: null, wasClean: false },
    });
  });

  it("does not turn generation-change close into a remote failure", async () => {
    DiagnosticWebSocket.instances = [];
    const transport = new BoundedClobMarketTransport({
      webSocket: DiagnosticWebSocket as unknown as typeof WebSocket,
      connectTimeoutMs: 1_000,
    });
    const adapter = new PolymarketMarketDataAdapter(undefined, { transport, ingress: transport.queue });
    adapter.registerManualToken("tok-up", "tok-up", "UP", "BTC");
    adapter.registerManualToken("tok-down", "tok-down", "DOWN", "BTC");
    const connections: Array<{ state: string; message?: string }> = [];
    const errors: Error[] = [];
    const abort = new AbortController();
    const running = adapter.run(["tok-up"], {
      onSnapshot: () => undefined,
      onConnection: (event) => { connections.push(event); },
      onError: (error) => { errors.push(error); },
    }, abort.signal);

    await waitFor(() => DiagnosticWebSocket.instances.length === 1);
    DiagnosticWebSocket.instances[0]!.open();
    await waitFor(() => connections.some((event) => event.state === "CONNECTED"));
    adapter.setTokenIds(["tok-down"]);
    await waitFor(() => DiagnosticWebSocket.instances.length === 2);
    DiagnosticWebSocket.instances[1]!.open();
    await waitFor(() => connections.filter((event) => event.state === "CONNECTED").length === 2);

    expect(errors).toEqual([]);
    expect(connections.some((event) => event.state === "DISCONNECTED")).toBe(false);
    expect(adapter.reconnectDelays).toEqual([]);
    abort.abort("test");
    await running;
  });

  it("classifies a bounded connect timeout separately", async () => {
    class NeverOpenWebSocket extends DiagnosticWebSocket {
      static override instances: NeverOpenWebSocket[] = [];
      constructor(url: string) {
        super(url);
        NeverOpenWebSocket.instances.push(this);
      }
    }
    const transport = new BoundedClobMarketTransport({
      webSocket: NeverOpenWebSocket as unknown as typeof WebSocket,
      connectTimeoutMs: 20,
    });

    await expect(transport.subscribe(["tok-up"])).rejects.toSatisfy((error: unknown) => {
      expect(error).toBeInstanceOf(ClobMarketCloseError);
      expect(error).toMatchObject({
        baseMessage: CLOB_MARKET_CONNECT_TIMEOUT,
        diagnostic: { kind: "connect_failure", code: null, wasClean: false },
      });
      return true;
    });
  });
});
