import { describe, expect, it } from "vitest";
import {
  BoundedClobMarketTransport,
  CLOB_MARKET_CONNECT_ABORTED,
} from "@pm/market-data";

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const started = Date.now();
  while (!predicate()) {
    if (Date.now() - started > timeoutMs) throw new Error("waitFor timeout");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function flushImmediate(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}

function rawBook(sequence: number): string {
  return JSON.stringify({
    type: "book",
    payload: { tokenId: "tok-up", sequence },
  });
}

class SchedulingWebSocket extends EventTarget {
  static instances: SchedulingWebSocket[] = [];
  readyState = 0;
  readonly sent: string[] = [];

  constructor(readonly url: string) {
    super();
    SchedulingWebSocket.instances.push(this);
  }

  send(data: string): void { this.sent.push(data); }

  close(): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatchEvent(Object.assign(new Event("close"), {
      code: 1000,
      reason: "",
      wasClean: true,
    }));
  }

  open(): void {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }

  message(data: string): void {
    this.dispatchEvent(new MessageEvent("message", { data }));
  }

  error(): void {
    this.dispatchEvent(new Event("error"));
  }
}

async function openStream(options: ConstructorParameters<typeof BoundedClobMarketTransport>[0] = {}, signal?: AbortSignal) {
  SchedulingWebSocket.instances = [];
  const transport = new BoundedClobMarketTransport({
    webSocket: SchedulingWebSocket as unknown as typeof WebSocket,
    connectTimeoutMs: 1_000,
    pingIntervalMs: 1_000,
    staleMs: 10_000,
    ...options,
  });
  const pending = transport.subscribe(["tok-up"], false, signal);
  await waitFor(() => SchedulingWebSocket.instances.length === 1);
  const socket = SchedulingWebSocket.instances[0]!;
  socket.open();
  return { transport, stream: await pending, socket };
}

describe("PR30A — CLOB receive delivery scheduling", () => {
  it("A: a synchronous message burst admits within bounds without waking the consumer in the receive callback", async () => {
    const { transport, stream, socket } = await openStream();
    try {
      const iterator = stream[Symbol.asyncIterator]();
      const delivered: number[] = [];
      const first = iterator.next().then((result) => {
        delivered.push((result.value as { payload: { sequence: number } }).payload.sequence);
        return result;
      });

      socket.message(rawBook(1));
      socket.message(rawBook(2));
      socket.message(rawBook(3));

      expect(delivered).toEqual([]);
      await Promise.resolve();
      expect(delivered).toEqual([]);
      expect(transport.queue.depth).toBe(3);
      expect(transport.queue.snapshot().gapTotal).toBe(0);

      await expect(first).resolves.toMatchObject({ done: false });
      expect(delivered).toEqual([1]);
      expect(transport.queue.depth).toBe(2);

      await expect(iterator.next()).resolves.toMatchObject({ done: false });
      await expect(iterator.next()).resolves.toMatchObject({ done: false });
      expect(transport.queue.depth).toBe(0);
      expect(transport.queue.snapshot().gapTotal).toBe(0);
    } finally {
      await stream.close();
    }
  });

  it("B: a prefilled queue cannot drain through an already-resolved Promise microtask chain", async () => {
    const { transport, stream, socket } = await openStream();
    try {
      socket.message(rawBook(1));
      socket.message(rawBook(2));
      socket.message(rawBook(3));
      expect(transport.queue.depth).toBe(3);

      const iterator = stream[Symbol.asyncIterator]();
      const delivered: number[] = [];
      const first = iterator.next().then((result) => {
        delivered.push((result.value as { payload: { sequence: number } }).payload.sequence);
        return result;
      });
      await Promise.resolve();
      expect(delivered).toEqual([]);
      await expect(first).resolves.toMatchObject({ done: false });
      expect(delivered).toEqual([1]);

      const second = iterator.next().then((result) => {
        delivered.push((result.value as { payload: { sequence: number } }).payload.sequence);
        return result;
      });
      await Promise.resolve();
      expect(delivered).toEqual([1]);
      await expect(second).resolves.toMatchObject({ done: false });
      expect(delivered).toEqual([1, 2]);

      await expect(iterator.next()).resolves.toMatchObject({ done: false });
      expect(transport.queue.depth).toBe(0);
    } finally {
      await stream.close();
    }
  });

  it("C: slow consumption keeps the queue bounded and turns overflow into a terminal gap", async () => {
    const { transport, stream, socket } = await openStream({ maxEvents: 2, maxBytes: 4_096 });
    try {
      socket.message(JSON.stringify([JSON.parse(rawBook(1)), JSON.parse(rawBook(2))]));
      expect(transport.queue.depth).toBe(2);
      expect(transport.queue.snapshot().highWaterEvents).toBe(2);

      const pending = stream[Symbol.asyncIterator]().next();
      socket.message(rawBook(3));

      await expect(pending).rejects.toThrow(/MARKET_INGRESS_OVERFLOW/);
      expect(transport.queue.depth).toBe(2);
      expect(transport.queue.snapshot().gapTotal).toBe(1);
      expect(transport.queue.lastGapReason).toBe("count_limit");
      await flushImmediate();
      expect(transport.queue.depth).toBe(2);
    } finally {
      await stream.close();
    }
  });

  it("D: scheduled delivery is generation-fenced and cancellation preserves the new stream's queue", async () => {
    const { transport, stream: oldStream, socket: oldSocket } = await openStream();
    try {
      const oldIterator = oldStream[Symbol.asyncIterator]();
      const oldNext = oldIterator.next();
      oldSocket.message(rawBook(1));

      await oldStream.close("generation_change");
      await expect(oldNext).resolves.toEqual({ value: undefined, done: true });
      await flushImmediate();
      expect(oldStream.pendingWaiterCount()).toBe(0);

      const pending = transport.subscribe(["tok-up"], false);
      await waitFor(() => SchedulingWebSocket.instances.length === 2);
      const newSocket = SchedulingWebSocket.instances[1]!;
      newSocket.open();
      const newStream = await pending;
      const newIterator = newStream[Symbol.asyncIterator]();

      oldSocket.message(rawBook(99));
      expect(transport.queue.depth).toBe(0);
      newSocket.message(rawBook(2));
      const newNext = await newIterator.next();
      expect(newNext.done).toBe(false);
      expect((newNext.value as { payload: { sequence: number } }).payload.sequence).toBe(2);
      expect(transport.queue.depth).toBe(0);
      await newStream.close();
    } finally {
      await oldStream.close();
    }
  });

  it("E: abort cancels scheduled delivery, settles the waiter, and clears heartbeat state", async () => {
    const abort = new AbortController();
    const { stream, socket } = await openStream({}, abort.signal);
    try {
      const pending = stream[Symbol.asyncIterator]().next();
      socket.message(rawBook(1));
      abort.abort("test");

      await expect(pending).rejects.toThrow(CLOB_MARKET_CONNECT_ABORTED);
      await flushImmediate();
      expect(stream.pendingWaiterCount()).toBe(0);
      expect(stream.pingTimerActive()).toBe(false);
    } finally {
      await stream.close();
    }
  });
});
