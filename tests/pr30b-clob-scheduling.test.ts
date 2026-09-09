import { describe, expect, it } from "vitest";
import { BoundedClobMarketTransport, CLOB_MARKET_CONNECT_ABORTED } from "@pm/market-data";

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
  return JSON.stringify({ type: "book", payload: { tokenId: "tok-up", sequence } });
}

function sequenceOf(result: IteratorResult<unknown>): number {
  return (result.value as { payload: { sequence: number } }).payload.sequence;
}

class SchedulerWebSocket extends EventTarget {
  static instances: SchedulerWebSocket[] = [];
  readyState = 0;
  readonly sent: string[] = [];

  constructor(readonly url: string) {
    super();
    SchedulerWebSocket.instances.push(this);
  }

  send(data: string): void { this.sent.push(data); }

  close(): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatchEvent(Object.assign(new Event("close"), { code: 1000, reason: "", wasClean: true }));
  }

  open(): void {
    this.readyState = 1;
    this.dispatchEvent(new Event("open"));
  }

  message(data: string): void {
    this.dispatchEvent(new MessageEvent("message", { data }));
  }

  remoteClose(code: number, reason: string, wasClean: boolean): void {
    if (this.readyState === 3) return;
    this.readyState = 3;
    this.dispatchEvent(Object.assign(new Event("close"), { code, reason, wasClean }));
  }
}

async function openStream(
  options: ConstructorParameters<typeof BoundedClobMarketTransport>[0] = {},
  signal?: AbortSignal,
) {
  SchedulerWebSocket.instances = [];
  const transport = new BoundedClobMarketTransport({
    webSocket: SchedulerWebSocket as unknown as typeof WebSocket,
    connectTimeoutMs: 1_000,
    pingIntervalMs: 1_000,
    staleMs: 10_000,
    ...options,
  });
  const pending = transport.subscribe(["tok-up"], false, signal);
  await waitFor(() => SchedulerWebSocket.instances.length === 1);
  const socket = SchedulerWebSocket.instances[0]!;
  socket.open();
  return { transport, stream: await pending, socket };
}

describe("PR30B — bounded CLOB delivery slices", () => {
  it("F: isolates a burst from the receive callback even when a slice admits multiple events", async () => {
    const { transport, stream, socket } = await openStream({
      maxDeliveryEventsPerSlice: 4,
      maxDeliverySliceMs: 10,
    });
    try {
      const iterator = stream[Symbol.asyncIterator]();
      let delivered = 0;
      const first = iterator.next().then((result) => {
        delivered += 1;
        return result;
      });

      socket.message(rawBook(1));
      socket.message(rawBook(2));
      socket.message(rawBook(3));

      expect(delivered).toBe(0);
      await Promise.resolve();
      expect(delivered).toBe(0);
      expect(transport.queue.depth).toBe(3);

      await expect(first).resolves.toMatchObject({ done: false });
      expect(delivered).toBe(1);
    } finally {
      await stream.close();
    }
  });

  it("G: stops a prefilled slice at the count budget and resumes on a later macrotask", async () => {
    const { transport, stream, socket } = await openStream({
      maxDeliveryEventsPerSlice: 3,
      maxDeliverySliceMs: 100,
      monotonicNow: () => 0,
    });
    try {
      socket.message(rawBook(1));
      socket.message(rawBook(2));
      socket.message(rawBook(3));
      socket.message(rawBook(4));

      const iterator = stream[Symbol.asyncIterator]();
      const first = await iterator.next();
      expect(sequenceOf(first)).toBe(1);

      const second = await iterator.next();
      const third = await iterator.next();
      expect([sequenceOf(second), sequenceOf(third)]).toEqual([2, 3]);
      expect(transport.queue.depth).toBe(1);

      let fourthSettled = false;
      const fourth = iterator.next().then((result) => {
        fourthSettled = true;
        return result;
      });
      await Promise.resolve();
      expect(fourthSettled).toBe(false);
      expect(transport.queue.depth).toBe(1);

      await flushImmediate();
      await expect(fourth).resolves.toMatchObject({ done: false });
      expect(sequenceOf(await fourth)).toBe(4);
    } finally {
      await stream.close();
    }
  });

  it("H: requires a new macrotask after the monotonic time budget expires", async () => {
    let now = 0;
    const { stream, socket } = await openStream({
      maxDeliveryEventsPerSlice: 8,
      maxDeliverySliceMs: 5,
      monotonicNow: () => now,
    });
    try {
      socket.message(rawBook(1));
      socket.message(rawBook(2));
      const iterator = stream[Symbol.asyncIterator]();
      await expect(iterator.next()).resolves.toSatisfy((result: IteratorResult<unknown>) => sequenceOf(result) === 1);

      now = 6;
      let settled = false;
      const second = iterator.next().then((result) => {
        settled = true;
        return result;
      });
      await Promise.resolve();
      expect(settled).toBe(false);
      await flushImmediate();
      await expect(second).resolves.toSatisfy((result: IteratorResult<unknown>) => sequenceOf(result) === 2);
    } finally {
      await stream.close();
    }
  });

  it("I: preserves FIFO order and delivers every accepted event exactly once across slices", async () => {
    const { transport, stream, socket } = await openStream({
      maxDeliveryEventsPerSlice: 2,
      maxDeliverySliceMs: 100,
      monotonicNow: () => 0,
    });
    try {
      for (let sequence = 1; sequence <= 9; sequence += 1) socket.message(rawBook(sequence));
      const iterator = stream[Symbol.asyncIterator]();
      const delivered: number[] = [];
      for (let i = 0; i < 9; i += 1) delivered.push(sequenceOf(await iterator.next()));
      expect(delivered).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9]);
      expect(new Set(delivered).size).toBe(transport.queue.snapshot().accepted);
      expect(transport.queue.snapshot().processed).toBe(9);
      expect(transport.queue.snapshot().gapTotal).toBe(0);
    } finally {
      await stream.close();
    }
  });

  it("J: keeps count overflow terminal and does not dequeue or silently drop queued events", async () => {
    const { transport, stream, socket } = await openStream({
      maxEvents: 2,
      maxBytes: 4_096,
      maxDeliveryEventsPerSlice: 4,
      maxDeliverySliceMs: 100,
    });
    try {
      socket.message(rawBook(1));
      socket.message(rawBook(2));
      socket.message(rawBook(3));

      await expect(stream[Symbol.asyncIterator]().next()).rejects.toThrow("MARKET_INGRESS_OVERFLOW:count_limit");
      expect(transport.queue.depth).toBe(2);
      expect(transport.queue.snapshot().processed).toBe(0);
      expect(transport.queue.snapshot().gapTotal).toBe(1);
    } finally {
      await stream.close();
    }
  });

  it("K: cancels scheduled delivery across generation change, close, abort, and remote terminalization", async () => {
    const old = await openStream({ maxDeliveryEventsPerSlice: 4, maxDeliverySliceMs: 100 });
    const oldIterator = old.stream[Symbol.asyncIterator]();
    const oldNext = oldIterator.next();
    old.socket.message(rawBook(1));
    await old.stream.close("generation_change");
    await expect(oldNext).resolves.toEqual({ value: undefined, done: true });
    const replacementPending = old.transport.subscribe(["tok-up"], false);
    await waitFor(() => SchedulerWebSocket.instances.length === 2);
    const replacementSocket = SchedulerWebSocket.instances[1]!;
    replacementSocket.open();
    const replacement = await replacementPending;
    await flushImmediate();
    expect(old.transport.queue.depth).toBe(0);
    replacementSocket.message(rawBook(2));
    await expect(replacement[Symbol.asyncIterator]().next()).resolves.toSatisfy(
      (result: IteratorResult<unknown>) => sequenceOf(result) === 2,
    );
    await replacement.close();

    const explicit = await openStream({ maxDeliveryEventsPerSlice: 4, maxDeliverySliceMs: 100 });
    const explicitNext = explicit.stream[Symbol.asyncIterator]().next();
    explicit.socket.message(rawBook(2));
    await explicit.stream.close();
    await expect(explicitNext).resolves.toEqual({ value: undefined, done: true });
    await flushImmediate();
    expect(explicit.transport.queue.depth).toBe(1);

    const abort = new AbortController();
    const aborted = await openStream({ maxDeliveryEventsPerSlice: 4, maxDeliverySliceMs: 100 }, abort.signal);
    const abortedNext = aborted.stream[Symbol.asyncIterator]().next();
    aborted.socket.message(rawBook(3));
    abort.abort("test");
    await expect(abortedNext).rejects.toThrow(CLOB_MARKET_CONNECT_ABORTED);
    await flushImmediate();
    expect(aborted.stream.pendingWaiterCount()).toBe(0);
    expect(aborted.transport.queue.depth).toBe(1);

    const remote = await openStream({ maxDeliveryEventsPerSlice: 4, maxDeliverySliceMs: 100 });
    const remoteNext = remote.stream[Symbol.asyncIterator]().next();
    remote.socket.message(rawBook(4));
    remote.socket.remoteClose(1013, "slow consumer: send buffer full", false);
    await expect(remoteNext).resolves.toEqual({ value: undefined, done: true });
    await flushImmediate();
    expect(remote.stream.pendingWaiterCount()).toBe(0);
    expect(remote.transport.queue.depth).toBe(1);

    await replacement.close();
    await explicit.stream.close();
    await aborted.stream.close();
    await remote.stream.close();
  });

  it("L: settles multiple waiters FIFO while enforcing the slice limit", async () => {
    const { stream, socket } = await openStream({
      maxDeliveryEventsPerSlice: 2,
      maxDeliverySliceMs: 100,
      monotonicNow: () => 0,
    });
    try {
      for (let sequence = 1; sequence <= 4; sequence += 1) socket.message(rawBook(sequence));
      const iterator = stream[Symbol.asyncIterator]();
      const settled: number[] = [];
      const pending = [1, 2, 3, 4].map(() => iterator.next().then((result) => {
        settled.push(sequenceOf(result));
        return result;
      }));

      await flushImmediate();
      await Promise.resolve();
      expect(settled).toEqual([1, 2]);

      const results = await Promise.all(pending);
      expect(results.map(sequenceOf)).toEqual([1, 2, 3, 4]);
      expect(new Set(settled).size).toBe(4);
    } finally {
      await stream.close();
    }
  });
});
