import { AsyncLocalStorage } from "node:async_hooks";

/**
 * Serializes replay-authoritative state transitions and checkpoint fencing.
 * Ordinary journal admission (PR #12) stays concurrent for telemetry/research.
 * Re-entrant for the same async context so commit+apply+checkpoint can nest.
 */
export class DurableMutationGate {
  private chain = Promise.resolve();
  private readonly held = new AsyncLocalStorage<symbol | null>();
  /**
   * AsyncLocalStorage contexts are inherited by timers/promises spawned while
   * the gate is held. A detached callback may run after the original owner has
   * released the gate, so "context exists" is not enough to prove re-entrancy.
   * Only the currently-active owner token may bypass serialization.
   */
  private activeOwner: symbol | null = null;

  /** A trading submission may be spawned by a fill's afterApply callback.
   * It is independent work, not a reentrant mutation of that fill. Inheriting
   * the callback's lock context would bypass serialization during/after it. */
  outside<T>(work: () => Promise<T>): Promise<T> {
    return this.held.run(null, work);
  }

  async run<T>(work: () => Promise<T>): Promise<T> {
    const inheritedOwner = this.held.getStore();
    if (inheritedOwner !== null && inheritedOwner !== undefined && inheritedOwner === this.activeOwner) return work();
    const previous = this.chain;
    let release: () => void = () => undefined;
    this.chain = new Promise<void>((resolve) => { release = resolve; });
    await previous;
    const owner = Symbol("durable-mutation-owner");
    this.activeOwner = owner;
    try {
      return await this.held.run(owner, work);
    } finally {
      if (this.activeOwner === owner) this.activeOwner = null;
      release();
    }
  }
}
