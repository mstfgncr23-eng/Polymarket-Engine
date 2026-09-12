import { afterEach, describe, expect, it, vi } from "vitest";
import {
  SSE_RECONNECT_GRACE_MS,
  startEngineLiveTransport,
  type EngineEventSource,
  type StreamStatus,
} from "../apps/dashboard/lib/live-transport.js";

class ReconnectingEventSource implements EngineEventSource {
  static instance: ReconnectingEventSource | undefined;

  onopen: ((ev: Event) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  readyState = 0;
  closed = false;

  constructor(_url: string) {
    ReconnectingEventSource.instance = this;
  }

  addEventListener(_type: string, _listener: (event: MessageEvent<string>) => void): void {}

  close(): void {
    this.closed = true;
    this.readyState = 2;
  }

  emitOpen(): void {
    this.readyState = 1;
    this.onopen?.(new Event("open"));
  }

  emitTransientError(): void {
    this.readyState = 0;
    this.onerror?.(new Event("error"));
  }

  emitClosedError(): void {
    this.readyState = 2;
    this.onerror?.(new Event("error"));
  }
}

function unavailableSnapshotFetch(): typeof fetch {
  return vi.fn(async () => new Response(null, { status: 503 })) as unknown as typeof fetch;
}

afterEach(() => {
  ReconnectingEventSource.instance = undefined;
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("dashboard SSE reconnect grace", () => {
  it("keeps the transport LIVE when native EventSource reconnects within the grace window", async () => {
    vi.useFakeTimers();
    const statuses: StreamStatus[] = [];
    const transport = startEngineLiveTransport({
      onStatus: (status) => statuses.push(status),
      onSnapshot: () => undefined,
    }, {
      eventSourceCtor: ReconnectingEventSource,
      fetchImpl: unavailableSnapshotFetch(),
    });
    const source = ReconnectingEventSource.instance!;

    source.emitOpen();
    source.emitTransientError();
    await vi.advanceTimersByTimeAsync(SSE_RECONNECT_GRACE_MS - 1);
    expect(statuses).toEqual(["LIVE"]);

    source.emitOpen();
    await vi.advanceTimersByTimeAsync(1);
    expect(statuses).toEqual(["LIVE", "LIVE"]);
    expect(statuses).not.toContain("DEGRADED");

    transport.close();
  });

  it("marks the transport DEGRADED when reconnect does not recover before the grace deadline", async () => {
    vi.useFakeTimers();
    const statuses: StreamStatus[] = [];
    const transport = startEngineLiveTransport({
      onStatus: (status) => statuses.push(status),
      onSnapshot: () => undefined,
    }, {
      eventSourceCtor: ReconnectingEventSource,
      fetchImpl: unavailableSnapshotFetch(),
    });
    const source = ReconnectingEventSource.instance!;

    source.emitOpen();
    source.emitTransientError();
    await vi.advanceTimersByTimeAsync(SSE_RECONNECT_GRACE_MS - 1);
    expect(statuses).toEqual(["LIVE"]);

    await vi.advanceTimersByTimeAsync(1);
    expect(statuses).toEqual(["LIVE", "DEGRADED"]);

    transport.close();
  });

  it("marks a terminal EventSource error DEGRADED immediately", () => {
    const statuses: StreamStatus[] = [];
    const transport = startEngineLiveTransport({
      onStatus: (status) => statuses.push(status),
      onSnapshot: () => undefined,
    }, {
      eventSourceCtor: ReconnectingEventSource,
      fetchImpl: unavailableSnapshotFetch(),
    });
    const source = ReconnectingEventSource.instance!;

    source.emitOpen();
    source.emitClosedError();
    expect(statuses).toEqual(["LIVE", "DEGRADED"]);

    transport.close();
  });
});
