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

export class ScriptedBoundedStream implements BoundedMarketStream {
  private readonly waiters: Array<(result: IteratorResult<unknown>) => void> = [];
  private pendingError: Error | null = null;
  private ended = false;
  closed = false;
  closeCount = 0;

  constructor(
    private readonly queue: BoundedIngressQueue,
    private readonly onClosed?: () => void,
  ) {}

  push(event: unknown): { ok: true } | { ok: false; reason: "count_limit" | "byte_limit" } {
    if (this.closed) return { ok: true };
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
    this.ended = true;
    this.wake();
  }

  fail(error: Error): void {
    this.pendingError = error;
    this.ended = true;
    while (this.waiters.length > 0) {
      this.waiters.shift()!({ value: undefined, done: true });
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.closeCount += 1;
    this.ended = true;
    this.onClosed?.();
    this.wake();
  }

  [Symbol.asyncIterator](): AsyncIterator<unknown> {
    return {
      next: async () => {
        if (this.pendingError) throw this.pendingError;
        const existing = this.queue.dequeue();
        if (existing !== undefined) return { value: existing, done: false };
        if (this.ended) {
          if (this.pendingError) throw this.pendingError;
          return { value: undefined, done: true };
        }
        return await new Promise<IteratorResult<unknown>>((resolve) => {
          this.waiters.push(resolve);
          this.wake();
        }).then((result) => {
          if (this.pendingError) throw this.pendingError;
          return result;
        });
      },
    };
  }

  private wake(): void {
    while (this.waiters.length > 0) {
      if (this.pendingError) {
        this.waiters.shift()!({ value: undefined, done: true });
        continue;
      }
      const item = this.queue.dequeue();
      if (item !== undefined) {
        this.waiters.shift()!({ value: item, done: false });
        continue;
      }
      if (this.ended) {
        this.waiters.shift()!({ value: undefined, done: true });
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
