import { BoundedIngressQueue, IngressOverflowError } from "./bounded-ingress.js";
import type { BoundedMarketStream } from "./bounded-clob-market.js";
import type { MarketStreamTransport } from "./market-stream-transport.js";

/**
 * Bounded-compatible fake for tests. Events enter only through the owned
 * BoundedIngressQueue — never an unbounded SDK FIFO.
 */
export class ScriptedBoundedMarketTransport implements MarketStreamTransport {
  readonly bounded = true as const;
  readonly queue: BoundedIngressQueue;
  readonly streams: ScriptedBoundedStream[] = [];
  subscribeCalls = 0;
  concurrent = 0;
  maxConcurrent = 0;
  failNextSubscribeCount = 0;
  failNextSubscribeError = new Error("CLOB_MARKET_WS_ERROR");
  private subscribeGate: Promise<void> | null = null;

  constructor(queue?: BoundedIngressQueue) {
    this.queue = queue ?? new BoundedIngressQueue();
  }

  get stream(): ScriptedBoundedStream | undefined { return this.streams.at(-1); }

  current(): ScriptedBoundedStream { return this.streams.at(-1)!; }

  holdNextSubscribe(): { release: () => void } {
    let release!: () => void;
    this.subscribeGate = new Promise<void>((resolve) => { release = resolve; });
    return { release };
  }

  async subscribe(
    _tokenIds: readonly string[] = [],
    _customFeatureEnabled = true,
    signal?: AbortSignal,
  ): Promise<ScriptedBoundedStream> {
    this.subscribeCalls += 1;
    if (signal?.aborted) throw abortConnectError(signal.reason);
    if (this.subscribeGate) {
      const gate = this.subscribeGate;
      this.subscribeGate = null;
      await Promise.race([
        gate,
        abortConnectPromise(signal),
      ]);
    }
    if (signal?.aborted) throw abortConnectError(signal.reason);
    if (this.failNextSubscribeCount > 0) {
      this.failNextSubscribeCount -= 1;
      throw this.failNextSubscribeError;
    }
    this.concurrent += 1;
    this.maxConcurrent = Math.max(this.maxConcurrent, this.concurrent);
    this.queue.clear();
    const stream = new ScriptedBoundedStream(this.queue, () => {
      this.concurrent = Math.max(0, this.concurrent - 1);
    });
    this.streams.push(stream);
    return stream;
  }

  push(event: unknown): { ok: true } | { ok: false; reason: "count_limit" | "byte_limit" } {
    return this.current().push(event);
  }
}

interface ScriptedWaiter {
  resolve: (result: IteratorResult<unknown>) => void;
  reject: (error: Error) => void;
}

export class ScriptedBoundedStream implements BoundedMarketStream {
  private readonly waiters: ScriptedWaiter[] = [];
  private pendingError: Error | null = null;
  private ended = false;
  private finalized = false;
  closed = false;
  closeCount = 0;
  private readonly generation = 1;

  constructor(
    private readonly queue: BoundedIngressQueue,
    private readonly onClosed?: () => void,
  ) {}

  pendingWaiterCount(): number { return this.waiters.length; }
  pingTimerActive(): boolean { return false; }
  streamGeneration(): number { return this.generation; }

  push(event: unknown): { ok: true } | { ok: false; reason: "count_limit" | "byte_limit" } {
    if (this.closed || this.finalized) return { ok: true };
    const result = this.queue.tryEnqueue(event);
    if (!result.ok) {
      this.fail(new IngressOverflowError(result.reason));
      return result;
    }
    this.wake();
    return { ok: true };
  }

  /** Remote unexpected close without going through adapter closeCurrentStream. */
  endRemote(): void {
    this.finalize(null);
  }

  fail(error: Error): void {
    this.finalize(error);
  }

  async close(): Promise<void> {
    if (this.closed) {
      this.finalize(null);
      return;
    }
    this.closed = true;
    this.closeCount += 1;
    this.onClosed?.();
    this.finalize(null);
  }

  [Symbol.asyncIterator](): AsyncIterator<unknown> {
    return {
      next: async () => {
        if (this.pendingError) throw this.pendingError;
        if (this.ended) {
          return { value: undefined, done: true };
        }
        const existing = this.queue.dequeue();
        if (existing !== undefined) return { value: existing, done: false };
        if (this.ended) {
          if (this.pendingError) throw this.pendingError;
          return { value: undefined, done: true };
        }
        return await new Promise<IteratorResult<unknown>>((resolve, reject) => {
          this.waiters.push({ resolve, reject });
          this.wake();
        });
      },
    };
  }

  private finalize(error: Error | null): void {
    if (this.finalized) return;
    this.finalized = true;
    this.ended = true;
    if (error && !this.pendingError) this.pendingError = error;
    while (this.waiters.length > 0) {
      const waiter = this.waiters.shift()!;
      if (this.pendingError) waiter.reject(this.pendingError);
      else waiter.resolve({ value: undefined, done: true });
    }
  }

  private wake(): void {
    while (this.waiters.length > 0) {
      if (this.pendingError) {
        this.waiters.shift()!.reject(this.pendingError);
        continue;
      }
      if (this.ended) {
        this.waiters.shift()!.resolve({ value: undefined, done: true });
        continue;
      }
      const item = this.queue.dequeue();
      if (item !== undefined) {
        this.waiters.shift()!.resolve({ value: item, done: false });
        continue;
      }
      if (this.ended) {
        this.waiters.shift()!.resolve({ value: undefined, done: true });
        continue;
      }
      break;
    }
  }
}

function abortConnectError(reason: unknown): Error {
  return reason instanceof Error ? reason : new Error("CLOB_MARKET_WS_CONNECT_ABORTED");
}

function abortConnectPromise(signal?: AbortSignal): Promise<never> {
  if (!signal) return new Promise(() => undefined);
  if (signal.aborted) return Promise.reject(abortConnectError(signal.reason));
  return new Promise((_, reject) => {
    signal.addEventListener("abort", () => reject(abortConnectError(signal.reason)), { once: true });
  });
}
